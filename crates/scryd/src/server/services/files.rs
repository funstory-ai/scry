use crate::server::prelude::*;

use crate::server::auth::*;
use crate::server::config::AuthConfig;
use crate::server::rpc::*;
use crate::server::state::AppState;

pub(crate) struct FilesSvc {
    pub(crate) state: Arc<AppState>,
    pub(crate) auth: Arc<AuthConfig>,
    pub(crate) io_stream_chunk_bytes: usize,
}

impl FilesSvc {
    fn resolve_put_target_path(
        state: &AppState,
        workspace_id: &str,
        req: &PutFileRequest,
    ) -> Result<String, Box<Status>> {
        if let Some(target) = &req.target {
            match target {
                put_file_request::Target::NodeId(node_id) => {
                    if node_id.trim().is_empty() {
                        return Err(Box::new(Status::invalid_argument(
                            "node_id cannot be empty",
                        )));
                    }
                    let resolved = state
                        .index_backend
                        .resolve_ref(workspace_id, node_id.trim())
                        .map_err(|e| Box::new(index_status(e)))?
                        .ok_or_else(|| Box::new(Status::not_found("node_id not found")))?;
                    Ok(resolved)
                }
                put_file_request::Target::PutFilePath(path_target) => {
                    if path_target.parent_node_id.trim().is_empty() {
                        return Err(Box::new(Status::invalid_argument(
                            "parent_node_id cannot be empty",
                        )));
                    }
                    if path_target.name.trim().is_empty() {
                        return Err(Box::new(Status::invalid_argument("name cannot be empty")));
                    }
                    let parent = state
                        .index_backend
                        .get_node(workspace_id, path_target.parent_node_id.trim())
                        .map_err(|e| Box::new(index_status(e)))?;
                    if parent.attrs.kind != NodeKind::Dir {
                        return Err(Box::new(Status::failed_precondition(
                            "parent_node_id does not reference a directory",
                        )));
                    }
                    let name = path_target.name.trim();
                    if name.contains('/') || name == "." || name == ".." {
                        return Err(Box::new(Status::invalid_argument(
                            "invalid target file name",
                        )));
                    }
                    if parent.path.is_empty() {
                        Ok(name.to_string())
                    } else {
                        Ok(format!("{}/{}", parent.path, name))
                    }
                }
            }
        } else {
            #[allow(deprecated)]
            let compat_path = req.path.trim();
            if compat_path.is_empty() {
                Err(Box::new(Status::invalid_argument(
                    "put target is required (target oneof or deprecated path)",
                )))
            } else {
                Ok(compat_path.to_string())
            }
        }
    }
}

type GetFileStream =
    Pin<Box<dyn futures_core::Stream<Item = Result<GetFileChunk, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl Files for FilesSvc {
    type GetFileStream = GetFileStream;

    async fn put_file(
        &self,
        request: Request<PutFileRequest>,
    ) -> Result<Response<PutFileResponse>, Status> {
        ensure_workspace_auth(&request, &self.auth, request.get_ref().workspace_id.trim())?;
        let req = request.into_inner();
        let resolved_path =
            Self::resolve_put_target_path(&self.state, &req.workspace_id, &req).map_err(|e| *e)?;

        let if_version = if req.if_version == 0 {
            None
        } else {
            Some(req.if_version)
        };
        let rel = resolved_path.trim_start_matches('/');
        self.state
            .write_content_file_atomic(&req.workspace_id, rel, &req.content)
            .await?;
        let (record, event_id) = self
            .state
            .index_backend
            .put_file(&req.workspace_id, &resolved_path, &req.content, if_version)
            .map_err(index_status)?;
        self.state
            .publish_event_for_workspace_by_id(&req.workspace_id, event_id);

        self.state
            .enqueue_index_job(&req.workspace_id, &record.node_id, record.version);

        Ok(Response::new(PutFileResponse {
            node_id: record.node_id,
            version: record.version,
            size: record.size,
        }))
    }

    async fn get_file(
        &self,
        request: Request<GetFileRequest>,
    ) -> Result<Response<Self::GetFileStream>, Status> {
        ensure_workspace_auth(&request, &self.auth, request.get_ref().workspace_id.trim())?;
        let req = request.into_inner();
        if req.path.trim().is_empty() {
            return Err(Status::invalid_argument("path cannot be empty"));
        }

        let node_id = self
            .state
            .index_backend
            .resolve_path(&req.workspace_id, &req.path)
            .map_err(index_status)?
            .ok_or_else(|| Status::not_found("path not found"))?;
        let node = self
            .state
            .index_backend
            .get_node(&req.workspace_id, &node_id)
            .map_err(index_status)?;
        if node.attrs.kind != NodeKind::File {
            return Err(Status::failed_precondition(
                "path must reference a file node",
            ));
        }
        let content_path = self
            .state
            .content_store
            .workspace_root(&req.workspace_id)
            .map_err(internal_status)?
            .join(node.path.trim_start_matches('/'));
        let chunk_bytes = self.io_stream_chunk_bytes.max(1);
        let outbound = try_stream! {
            let mut file = File::open(&content_path).await.map_err(|err| {
                if err.kind() == std::io::ErrorKind::NotFound {
                    Status::not_found(format!("content file not found for {}", node.path))
                } else {
                    Status::internal(format!(
                        "failed opening content file {}: {err}",
                        content_path.display()
                    ))
                }
            })?;

            let mut buf = vec![0u8; chunk_bytes];
            let mut next_read = file.read(&mut buf).await.map_err(|err| {
                Status::internal(format!(
                    "failed reading content file {}: {err}",
                    content_path.display()
                ))
            })?;
            if next_read == 0 {
                yield GetFileChunk {
                    data: Vec::new(),
                    eof: true,
                };
            } else {
                loop {
                    let current_read = next_read;
                    let data = buf[..current_read].to_vec();
                    next_read = file.read(&mut buf).await.map_err(|err| {
                        Status::internal(format!(
                            "failed reading content file {}: {err}",
                            content_path.display()
                        ))
                    })?;
                    let eof = next_read == 0;
                    yield GetFileChunk {
                        data,
                        eof,
                    };
                    if eof {
                        break;
                    }
                }
            }
        };
        Ok(Response::new(Box::pin(outbound)))
    }
}
