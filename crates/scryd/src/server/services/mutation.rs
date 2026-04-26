use crate::server::prelude::*;

use crate::server::auth::*;
use crate::server::config::AuthConfig;
use crate::server::rpc::*;
use crate::server::state::AppState;

pub(crate) struct MutationSvc {
    pub(crate) state: Arc<AppState>,
    pub(crate) auth: Arc<AuthConfig>,
}

#[tonic::async_trait]
impl Mutation for MutationSvc {
    async fn create(
        &self,
        request: Request<CreateRequest>,
    ) -> Result<Response<CreateResponse>, Status> {
        ensure_workspace_auth(&request, &self.auth, request.get_ref().workspace_id.trim())?;
        let req = request.into_inner();
        if req.parent_node_id.trim().is_empty() {
            return Err(Status::invalid_argument("parent_node_id cannot be empty"));
        }
        if req.name.trim().is_empty() {
            return Err(Status::invalid_argument("name cannot be empty"));
        }
        let kind = from_proto_kind(req.kind).map_err(Status::invalid_argument)?;
        let mode = if req.mode == 0 {
            match kind {
                NodeKind::File => 0o644,
                NodeKind::Dir => 0o755,
            }
        } else {
            req.mode
        };
        let (node, event_id) = self
            .state
            .index_backend
            .create_node(
                &req.workspace_id,
                &req.parent_node_id,
                &req.name,
                kind,
                mode,
                req.exclusive,
            )
            .map_err(index_status)?;
        if let Some(id) = event_id {
            self.state
                .publish_event_for_workspace_by_id(&req.workspace_id, id);
        }
        if node.attrs.kind == NodeKind::File {
            let file_path = self
                .state
                .content_store
                .workspace_root(&req.workspace_id)
                .map_err(internal_status)?
                .join(&node.path);
            if let Some(parent) = file_path.parent() {
                tokio::fs::create_dir_all(parent).await.map_err(|err| {
                    Status::internal(format!(
                        "failed creating parent directory {}: {err}",
                        parent.display()
                    ))
                })?;
            }
            tokio::fs::write(&file_path, b"").await.map_err(|err| {
                Status::internal(format!(
                    "failed creating content file {}: {err}",
                    file_path.display()
                ))
            })?;
        } else {
            let dir_path = self
                .state
                .content_store
                .workspace_root(&req.workspace_id)
                .map_err(internal_status)?
                .join(&node.path);
            tokio::fs::create_dir_all(&dir_path).await.map_err(|err| {
                Status::internal(format!(
                    "failed creating content directory {}: {err}",
                    dir_path.display()
                ))
            })?;
        }
        Ok(Response::new(CreateResponse {
            node_id: node.node_id,
            attrs: Some(to_proto_attrs(&node.attrs)),
        }))
    }

    async fn unlink(
        &self,
        request: Request<UnlinkRequest>,
    ) -> Result<Response<UnlinkResponse>, Status> {
        ensure_workspace_auth(&request, &self.auth, request.get_ref().workspace_id.trim())?;
        let req = request.into_inner();
        if req.parent_node_id.trim().is_empty() {
            return Err(Status::invalid_argument("parent_node_id cannot be empty"));
        }
        if req.name.trim().is_empty() {
            return Err(Status::invalid_argument("name cannot be empty"));
        }
        let target = self
            .state
            .index_backend
            .lookup(&req.workspace_id, &req.parent_node_id, &req.name)
            .map_err(index_status)?;
        let if_version = if req.if_version == 0 {
            None
        } else {
            Some(req.if_version)
        };
        let event_id = self
            .state
            .index_backend
            .unlink(
                &req.workspace_id,
                &req.parent_node_id,
                &req.name,
                if_version,
            )
            .map_err(index_status)?;
        self.state
            .publish_event_for_workspace_by_id(&req.workspace_id, event_id);
        let fs_path = self
            .state
            .content_store
            .workspace_root(&req.workspace_id)
            .map_err(internal_status)?
            .join(&target.path);
        if target.attrs.kind == NodeKind::Dir {
            if tokio::fs::metadata(&fs_path).await.is_ok() {
                tokio::fs::remove_dir(&fs_path).await.map_err(|err| {
                    Status::internal(format!(
                        "failed removing content directory {}: {err}",
                        fs_path.display()
                    ))
                })?;
            }
        } else if tokio::fs::metadata(&fs_path).await.is_ok() {
            tokio::fs::remove_file(&fs_path).await.map_err(|err| {
                Status::internal(format!(
                    "failed removing content file {}: {err}",
                    fs_path.display()
                ))
            })?;
        }
        Ok(Response::new(UnlinkResponse {}))
    }

    async fn rename(
        &self,
        request: Request<RenameRequest>,
    ) -> Result<Response<RenameResponse>, Status> {
        ensure_workspace_auth(&request, &self.auth, request.get_ref().workspace_id.trim())?;
        let req = request.into_inner();
        if req.from_parent_node_id.trim().is_empty() || req.to_parent_node_id.trim().is_empty() {
            return Err(Status::invalid_argument("parent_node_id cannot be empty"));
        }
        if req.from_name.trim().is_empty() || req.to_name.trim().is_empty() {
            return Err(Status::invalid_argument("name cannot be empty"));
        }
        let from_node = self
            .state
            .index_backend
            .lookup(&req.workspace_id, &req.from_parent_node_id, &req.from_name)
            .map_err(index_status)?;
        let existing_dest = self
            .state
            .index_backend
            .lookup(&req.workspace_id, &req.to_parent_node_id, &req.to_name)
            .ok();
        let if_version = if req.if_version == 0 {
            None
        } else {
            Some(req.if_version)
        };
        let ((node_id, version), event_id) = self
            .state
            .index_backend
            .rename(
                &req.workspace_id,
                &req.from_parent_node_id,
                &req.from_name,
                &req.to_parent_node_id,
                &req.to_name,
                req.overwrite,
                if_version,
            )
            .map_err(index_status)?;
        self.state
            .publish_event_for_workspace_by_id(&req.workspace_id, event_id);
        let root = self
            .state
            .content_store
            .workspace_root(&req.workspace_id)
            .map_err(internal_status)?;
        let renamed = self
            .state
            .index_backend
            .lookup(&req.workspace_id, &req.to_parent_node_id, &req.to_name)
            .map_err(index_status)?;
        let from_fs = root.join(&from_node.path);
        let to_fs = root.join(&renamed.path);
        if let Some(existing) = existing_dest {
            let existing_path = root.join(existing.path);
            if tokio::fs::metadata(&existing_path).await.is_ok() {
                if existing.attrs.kind == NodeKind::Dir {
                    tokio::fs::remove_dir(&existing_path).await.map_err(|err| {
                        Status::internal(format!(
                            "failed removing overwrite directory {}: {err}",
                            existing_path.display()
                        ))
                    })?;
                } else {
                    tokio::fs::remove_file(&existing_path)
                        .await
                        .map_err(|err| {
                            Status::internal(format!(
                                "failed removing overwrite file {}: {err}",
                                existing_path.display()
                            ))
                        })?;
                }
            }
        }
        if let Some(parent) = to_fs.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|err| {
                Status::internal(format!(
                    "failed creating parent directory {}: {err}",
                    parent.display()
                ))
            })?;
        }
        tokio::fs::rename(&from_fs, &to_fs).await.map_err(|err| {
            Status::internal(format!(
                "failed renaming content {} -> {}: {err}",
                from_fs.display(),
                to_fs.display()
            ))
        })?;
        Ok(Response::new(RenameResponse { node_id, version }))
    }

    async fn set_attrs(
        &self,
        request: Request<SetAttrsRequest>,
    ) -> Result<Response<SetAttrsResponse>, Status> {
        ensure_workspace_auth(&request, &self.auth, request.get_ref().workspace_id.trim())?;
        let req = request.into_inner();
        if req.node_id.trim().is_empty() {
            return Err(Status::invalid_argument("node_id cannot be empty"));
        }
        let patch = req.patch.unwrap_or_default();
        let (attrs, event_id) = self
            .state
            .index_backend
            .set_attrs(
                &req.workspace_id,
                &req.node_id,
                AttrsPatch {
                    mode: if patch.has_mode {
                        Some(patch.mode)
                    } else {
                        None
                    },
                    mtime_unix_nano: if patch.has_mtime_unix_nano {
                        Some(patch.mtime_unix_nano)
                    } else {
                        None
                    },
                },
            )
            .map_err(index_status)?;
        self.state
            .publish_event_for_workspace_by_id(&req.workspace_id, event_id);
        Ok(Response::new(SetAttrsResponse {
            attrs: Some(to_proto_attrs(&attrs)),
        }))
    }
}
