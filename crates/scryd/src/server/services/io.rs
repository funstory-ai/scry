use crate::server::prelude::*;

use crate::server::auth::*;
use crate::server::config::AuthConfig;
use crate::server::handle_storage::HandleBody;
use crate::server::rpc::*;
use crate::server::state::{AppState, HandleState};

pub(crate) struct IoSvc {
    pub(crate) state: Arc<AppState>,
    pub(crate) auth: Arc<AuthConfig>,
}

#[tonic::async_trait]
impl Io for IoSvc {
    async fn open(&self, request: Request<OpenRequest>) -> Result<Response<OpenResponse>, Status> {
        ensure_workspace_auth(&request, &self.auth, request.get_ref().workspace_id.trim())?;
        self.state.cleanup_idle_handles();
        let req = request.into_inner();
        if req.node_id.trim().is_empty() {
            return Err(Status::invalid_argument("node_id cannot be empty"));
        }
        let mode = to_index_handle_mode(req.mode);
        let node = self
            .state
            .index_backend
            .node_record_by_id(&req.workspace_id, &req.node_id)
            .map_err(index_status)?;
        let mut body = HandleBody::empty();
        if mode.allows_read() {
            let path = self
                .state
                .content_store
                .workspace_root(&req.workspace_id)
                .map_err(internal_status)?
                .join(node.path.trim_start_matches('/'));
            let content = tokio::fs::read(&path).await.map_err(|err| {
                if err.kind() == std::io::ErrorKind::NotFound {
                    Status::not_found(format!("content file not found for {}", node.path))
                } else {
                    Status::internal(format!(
                        "failed reading content file {}: {err}",
                        path.display()
                    ))
                }
            })?;
            body = HandleBody::from_bytes(content, self.state.io_handle_memory_limit).map_err(
                |err| {
                    Status::internal(format!("failed initializing handle buffer (spill?): {err}"))
                },
            )?;
        }
        let handle_id = Uuid::now_v7().to_string();
        self.state
            .handles
            .lock()
            .map_err(|_| Status::internal("failed to lock handle map"))?
            .insert(
                handle_id.clone(),
                HandleState {
                    workspace_id: req.workspace_id,
                    node_id: req.node_id,
                    mode,
                    snapshot_version: node.attrs.version,
                    body,
                    dirty: false,
                    last_touched: Instant::now(),
                    closing: false,
                },
            );
        Ok(Response::new(OpenResponse {
            handle_id,
            snapshot_version: node.attrs.version,
            attrs: Some(to_proto_attrs(&node.attrs)),
        }))
    }

    async fn read(&self, request: Request<ReadRequest>) -> Result<Response<ReadResponse>, Status> {
        ensure_workspace_auth(&request, &self.auth, request.get_ref().workspace_id.trim())?;
        self.state.cleanup_idle_handles();
        let req = request.into_inner();
        let (handle_id, mut body) = {
            let mut guard = self
                .state
                .handles
                .lock()
                .map_err(|_| Status::internal("failed to lock handle map"))?;
            let state = guard
                .get_mut(&req.handle_id)
                .ok_or_else(|| Status::not_found("handle_id not found"))?;
            if state.workspace_id != req.workspace_id {
                return Err(Status::permission_denied(
                    "handle does not belong to workspace",
                ));
            }
            if state.closing {
                return Err(Status::failed_precondition("handle is currently closing"));
            }
            if !state.mode.allows_read() {
                return Err(Status::failed_precondition("handle is write-only"));
            }
            let body = std::mem::take(&mut state.body);
            (req.handle_id.clone(), body)
        };

        let read_result = body
            .read_window(req.offset, req.length)
            .await
            .map_err(|err| Status::internal(format!("handle read failed: {err}")));

        let mut guard = self
            .state
            .handles
            .lock()
            .map_err(|_| Status::internal("failed to lock handle map"))?;
        let state = guard
            .get_mut(&handle_id)
            .ok_or_else(|| Status::not_found("handle_id not found"))?;
        state.body = body;
        if read_result.is_ok() {
            state.last_touched = Instant::now();
        }
        drop(guard);

        let (data, eof) = read_result?;
        Ok(Response::new(ReadResponse {
            data: data.to_vec(),
            eof,
        }))
    }

    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        ensure_workspace_auth(&request, &self.auth, request.get_ref().workspace_id.trim())?;
        self.state.cleanup_idle_handles();
        let req = request.into_inner();
        let (handle_id, workspace_id, node_id, mut body, mem_limit) = {
            let mut guard = self
                .state
                .handles
                .lock()
                .map_err(|_| Status::internal("failed to lock handle map"))?;
            let state = guard
                .get_mut(&req.handle_id)
                .ok_or_else(|| Status::not_found("handle_id not found"))?;
            if state.workspace_id != req.workspace_id {
                return Err(Status::permission_denied(
                    "handle does not belong to workspace",
                ));
            }
            if state.closing {
                return Err(Status::failed_precondition("handle is currently closing"));
            }
            if !state.mode.allows_write() {
                return Err(Status::failed_precondition("handle is read-only"));
            }
            let body = std::mem::take(&mut state.body);
            (
                req.handle_id.clone(),
                req.workspace_id.clone(),
                state.node_id.clone(),
                body,
                self.state.io_handle_memory_limit,
            )
        };

        let _ = self
            .state
            .index_backend
            .node_record_by_id(&workspace_id, &node_id)
            .map_err(index_status)?;
        let write_result = body
            .write_at(req.offset, &req.data, mem_limit)
            .await
            .map_err(|err| Status::internal(format!("handle write failed: {err}")));

        let mut guard = self
            .state
            .handles
            .lock()
            .map_err(|_| Status::internal("failed to lock handle map"))?;
        if let Some(state) = guard.get_mut(&handle_id) {
            state.body = body;
            if write_result.is_ok() {
                state.dirty = true;
                state.last_touched = Instant::now();
            }
        }
        drop(guard);

        write_result?;
        Ok(Response::new(WriteResponse {
            bytes_written: u64::try_from(req.data.len())
                .map_err(|_| Status::internal("data length conversion failed"))?,
        }))
    }

    async fn truncate(
        &self,
        request: Request<TruncateRequest>,
    ) -> Result<Response<TruncateResponse>, Status> {
        ensure_workspace_auth(&request, &self.auth, request.get_ref().workspace_id.trim())?;
        self.state.cleanup_idle_handles();
        let req = request.into_inner();
        let (handle_id, workspace_id, node_id, mut body, mem_limit) = {
            let mut guard = self
                .state
                .handles
                .lock()
                .map_err(|_| Status::internal("failed to lock handle map"))?;
            let state = guard
                .get_mut(&req.handle_id)
                .ok_or_else(|| Status::not_found("handle_id not found"))?;
            if state.workspace_id != req.workspace_id {
                return Err(Status::permission_denied(
                    "handle does not belong to workspace",
                ));
            }
            if state.closing {
                return Err(Status::failed_precondition("handle is currently closing"));
            }
            if !state.mode.allows_write() {
                return Err(Status::failed_precondition("handle is read-only"));
            }
            let body = std::mem::take(&mut state.body);
            (
                req.handle_id.clone(),
                req.workspace_id.clone(),
                state.node_id.clone(),
                body,
                self.state.io_handle_memory_limit,
            )
        };

        let _ = self
            .state
            .index_backend
            .node_record_by_id(&workspace_id, &node_id)
            .map_err(index_status)?;
        let trunc_result = body
            .truncate(req.size, mem_limit)
            .await
            .map_err(|err| Status::internal(format!("handle truncate failed: {err}")));

        let mut guard = self
            .state
            .handles
            .lock()
            .map_err(|_| Status::internal("failed to lock handle map"))?;
        if let Some(state) = guard.get_mut(&handle_id) {
            state.body = body;
            if trunc_result.is_ok() {
                state.dirty = true;
                state.last_touched = Instant::now();
            }
        }
        drop(guard);

        trunc_result?;
        Ok(Response::new(TruncateResponse {}))
    }

    async fn flush(
        &self,
        request: Request<FlushRequest>,
    ) -> Result<Response<FlushResponse>, Status> {
        ensure_workspace_auth(&request, &self.auth, request.get_ref().workspace_id.trim())?;
        self.state.cleanup_idle_handles();
        let req = request.into_inner();
        let (workspace_id, node_id, mode, snapshot_version, maybe_body) = {
            let mut guard = self
                .state
                .handles
                .lock()
                .map_err(|_| Status::internal("failed to lock handle map"))?;
            let state = guard
                .get_mut(&req.handle_id)
                .ok_or_else(|| Status::not_found("handle_id not found"))?;
            if state.workspace_id != req.workspace_id {
                return Err(Status::permission_denied(
                    "handle does not belong to workspace",
                ));
            }
            if state.closing {
                return Err(Status::failed_precondition("handle is currently closing"));
            }
            if !state.dirty {
                state.last_touched = Instant::now();
                let attrs = self
                    .state
                    .index_backend
                    .get_attrs(&state.workspace_id, &state.node_id)
                    .map_err(index_status)?;
                return Ok(Response::new(FlushResponse {
                    version: attrs.version,
                    attrs: Some(to_proto_attrs(&attrs)),
                }));
            }
            state.closing = true;
            let taken = std::mem::take(&mut state.body);
            (
                state.workspace_id.clone(),
                state.node_id.clone(),
                state.mode,
                state.snapshot_version,
                Some(taken),
            )
        };

        let body = maybe_body.expect("dirty flush must have taken body");
        let flush_result = self
            .state
            .flush_handle_from_body(&workspace_id, &node_id, mode, body, Some(snapshot_version))
            .await;

        match flush_result {
            Ok((flushed, new_body)) => {
                let mut guard = self
                    .state
                    .handles
                    .lock()
                    .map_err(|_| Status::internal("failed to lock handle map"))?;
                if let Some(state) = guard.get_mut(&req.handle_id) {
                    state.last_touched = Instant::now();
                    state.snapshot_version = flushed.version;
                    state.dirty = false;
                    state.body = new_body;
                    state.closing = false;
                }
                Ok(Response::new(flushed))
            }
            Err((err, restored)) => {
                let mut guard = self
                    .state
                    .handles
                    .lock()
                    .map_err(|_| Status::internal("failed to lock handle map"))?;
                if let Some(state) = guard.get_mut(&req.handle_id) {
                    state.last_touched = Instant::now();
                    state.body = restored;
                    state.closing = false;
                }
                Err(err)
            }
        }
    }

    async fn release(
        &self,
        request: Request<ReleaseRequest>,
    ) -> Result<Response<ReleaseResponse>, Status> {
        ensure_workspace_auth(&request, &self.auth, request.get_ref().workspace_id.trim())?;
        self.state.cleanup_idle_handles();
        let req = request.into_inner();
        let (workspace_id, node_id, mode, dirty, snapshot_version, body_opt) = {
            let mut guard = self
                .state
                .handles
                .lock()
                .map_err(|_| Status::internal("failed to lock handle map"))?;
            let state = guard
                .get_mut(&req.handle_id)
                .ok_or_else(|| Status::not_found("handle_id not found"))?;
            if state.workspace_id != req.workspace_id {
                return Err(Status::permission_denied(
                    "handle does not belong to workspace",
                ));
            }
            if state.closing {
                return Err(Status::failed_precondition("handle is currently closing"));
            }
            state.closing = true;
            state.last_touched = Instant::now();
            let dirty = state.dirty;
            let snap = state.snapshot_version;
            let body = if dirty {
                Some(std::mem::take(&mut state.body))
            } else {
                None
            };
            (
                state.workspace_id.clone(),
                state.node_id.clone(),
                state.mode,
                dirty,
                snap,
                body,
            )
        };

        if dirty {
            let body = body_opt.expect("dirty release must hold body");
            let flush_result = self
                .state
                .flush_handle_from_body(&workspace_id, &node_id, mode, body, Some(snapshot_version))
                .await;
            match flush_result {
                Ok((flushed, _new_body)) => {
                    self.state
                        .handles
                        .lock()
                        .map_err(|_| Status::internal("failed to lock handle map"))?
                        .remove(&req.handle_id);
                    return Ok(Response::new(ReleaseResponse {
                        version: flushed.version,
                    }));
                }
                Err((err, restored)) => {
                    if let Some(state) = self
                        .state
                        .handles
                        .lock()
                        .map_err(|_| Status::internal("failed to lock handle map"))?
                        .get_mut(&req.handle_id)
                    {
                        state.body = restored;
                        state.closing = false;
                        state.last_touched = Instant::now();
                    }
                    return Err(err);
                }
            }
        }

        let removed = self
            .state
            .handles
            .lock()
            .map_err(|_| Status::internal("failed to lock handle map"))?
            .remove(&req.handle_id)
            .ok_or_else(|| Status::not_found("handle_id not found"))?;
        Ok(Response::new(ReleaseResponse {
            version: removed.snapshot_version.max(snapshot_version),
        }))
    }
}
