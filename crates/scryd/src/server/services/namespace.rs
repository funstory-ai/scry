use crate::server::prelude::*;

use crate::server::auth::*;
use crate::server::config::AuthConfig;
use crate::server::rpc::*;
use crate::server::state::AppState;

pub(crate) struct NamespaceSvc {
    pub(crate) state: Arc<AppState>,
    pub(crate) auth: Arc<AuthConfig>,
}

#[tonic::async_trait]
impl Namespace for NamespaceSvc {
    async fn lookup(
        &self,
        request: Request<LookupRequest>,
    ) -> Result<Response<LookupResponse>, Status> {
        ensure_workspace_auth(&request, &self.auth, request.get_ref().workspace_id.trim())?;
        let req = request.into_inner();
        if req.parent_node_id.trim().is_empty() {
            return Err(Status::invalid_argument("parent_node_id cannot be empty"));
        }
        if req.name.trim().is_empty() {
            return Err(Status::invalid_argument("name cannot be empty"));
        }
        let node = self
            .state
            .index_backend
            .lookup(&req.workspace_id, &req.parent_node_id, &req.name)
            .map_err(index_status)?;
        Ok(Response::new(LookupResponse {
            node_id: node.node_id,
            attrs: Some(to_proto_attrs(&node.attrs)),
        }))
    }

    async fn get_attrs(
        &self,
        request: Request<GetAttrsRequest>,
    ) -> Result<Response<GetAttrsResponse>, Status> {
        ensure_workspace_auth(&request, &self.auth, request.get_ref().workspace_id.trim())?;
        let req = request.into_inner();
        if req.node_id.trim().is_empty() {
            return Err(Status::invalid_argument("node_id cannot be empty"));
        }
        let attrs = self
            .state
            .index_backend
            .get_attrs(&req.workspace_id, &req.node_id)
            .map_err(index_status)?;
        Ok(Response::new(GetAttrsResponse {
            attrs: Some(to_proto_attrs(&attrs)),
        }))
    }

    async fn get_attrs_if_changed(
        &self,
        request: Request<GetAttrsIfChangedRequest>,
    ) -> Result<Response<GetAttrsIfChangedResponse>, Status> {
        ensure_workspace_auth(&request, &self.auth, request.get_ref().workspace_id.trim())?;
        let req = request.into_inner();
        if req.node_id.trim().is_empty() {
            return Err(Status::invalid_argument("node_id cannot be empty"));
        }
        let attrs = self
            .state
            .index_backend
            .get_attrs_if_changed(&req.workspace_id, &req.node_id, req.known_version)
            .map_err(index_status)?;
        Ok(Response::new(GetAttrsIfChangedResponse {
            changed: attrs.is_some(),
            attrs: attrs.as_ref().map(to_proto_attrs),
        }))
    }

    async fn read_dir(
        &self,
        request: Request<ReadDirRequest>,
    ) -> Result<Response<ReadDirResponse>, Status> {
        ensure_workspace_auth(&request, &self.auth, request.get_ref().workspace_id.trim())?;
        let req = request.into_inner();
        if req.node_id.trim().is_empty() {
            return Err(Status::invalid_argument("node_id cannot be empty"));
        }
        let cursor_opt = if req.cursor.is_empty() {
            None
        } else {
            Some(req.cursor.as_str())
        };
        let (entries, next_cursor) = self
            .state
            .index_backend
            .read_dir(&req.workspace_id, &req.node_id, cursor_opt, req.limit)
            .map_err(index_status)?;
        let mapped = entries.into_iter().map(to_dir_entry).collect();
        Ok(Response::new(ReadDirResponse {
            entries: mapped,
            next_cursor: next_cursor.unwrap_or_default(),
        }))
    }

    async fn resolve_ref(
        &self,
        request: Request<ResolveRefRequest>,
    ) -> Result<Response<ResolveRefResponse>, Status> {
        ensure_workspace_auth(&request, &self.auth, request.get_ref().workspace_id.trim())?;
        let req = request.into_inner();
        if req.node_id.trim().is_empty() {
            return Err(Status::invalid_argument("node_id cannot be empty"));
        }
        let resolved = self
            .state
            .index_backend
            .resolve_ref_with_redirect(&req.workspace_id, &req.node_id)
            .map_err(index_status)?;
        Ok(Response::new(ResolveRefResponse {
            path: resolved.path.clone().unwrap_or_default(),
            exists: resolved.path.is_some(),
            redirect_to_node_id: resolved.redirect_to_node_id.unwrap_or_default(),
        }))
    }

    async fn resolve_path(
        &self,
        request: Request<ResolvePathRequest>,
    ) -> Result<Response<ResolvePathResponse>, Status> {
        ensure_workspace_auth(&request, &self.auth, request.get_ref().workspace_id.trim())?;
        let req = request.into_inner();
        let node_id = self
            .state
            .index_backend
            .resolve_path(&req.workspace_id, &req.path)
            .map_err(index_status)?;
        Ok(Response::new(ResolvePathResponse {
            node_id: node_id.clone().unwrap_or_default(),
            exists: node_id.is_some(),
        }))
    }
}
