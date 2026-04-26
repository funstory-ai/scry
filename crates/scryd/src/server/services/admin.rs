use crate::server::prelude::*;

use crate::server::auth::*;
use crate::server::config::AuthConfig;
use crate::server::rpc::*;
use crate::server::state::AppState;

#[derive(Clone)]
pub(crate) struct AdminSvc {
    pub(crate) state: Arc<AppState>,
    pub(crate) auth: Arc<AuthConfig>,
}

#[tonic::async_trait]
impl Admin for AdminSvc {
    async fn create_workspace(
        &self,
        request: Request<CreateWorkspaceRequest>,
    ) -> Result<Response<CreateWorkspaceResponse>, Status> {
        ensure_admin_auth_scoped(&request, &self.auth, "admin.workspace.create")?;
        let req = request.into_inner();
        let workspace_name = req.name.trim();
        if workspace_name.is_empty() {
            return Err(Status::invalid_argument("workspace name cannot be empty"));
        }

        let workspace_id = Uuid::now_v7().to_string();
        let root_path = self
            .state
            .content_store
            .workspace_root(&workspace_id)
            .map_err(internal_status)?;
        tokio::fs::create_dir_all(&root_path).await.map_err(|err| {
            Status::internal(format!(
                "failed to create content workspace directory {}: {err}",
                root_path.display()
            ))
        })?;

        let root_node_id = self
            .state
            .index_backend
            .create_workspace(&workspace_id, workspace_name)
            .map_err(internal_status)?;
        if let Err(err) = self
            .state
            .catalog
            .register_workspace(&workspace_id, workspace_name, &root_node_id)
            .await
        {
            let _ = self.state.index_backend.delete_workspace(&workspace_id);
            return Err(Status::internal(format!(
                "failed to register workspace in catalog: {err}"
            )));
        }

        Ok(Response::new(CreateWorkspaceResponse {
            workspace_id: workspace_id.clone(),
            root_path: root_path.to_string_lossy().to_string(),
            metadata_db_path: self
                .state
                .index_backend
                .db_path_for_workspace(&workspace_id)
                .map_err(index_status)?
                .to_string_lossy()
                .to_string(),
            root_node_id,
        }))
    }

    async fn delete_workspace(
        &self,
        request: Request<DeleteWorkspaceRequest>,
    ) -> Result<Response<DeleteWorkspaceResponse>, Status> {
        ensure_admin_auth_scoped(&request, &self.auth, "admin.workspace.delete")?;
        let req = request.into_inner();
        let workspace_id = req.workspace_id.trim();
        if workspace_id.is_empty() {
            return Err(Status::invalid_argument("workspace_id cannot be empty"));
        }
        let root_path = self
            .state
            .content_store
            .workspace_root(workspace_id)
            .map_err(internal_status)?;
        self.state
            .index_backend
            .delete_workspace(workspace_id)
            .map_err(index_status)?;
        if let Err(err) = self.state.catalog.delete_workspace(workspace_id).await {
            warn!(
                workspace_id = workspace_id,
                error = %err,
                "workspace deleted from index store but failed to delete from catalog"
            );
        }
        if tokio::fs::metadata(&root_path).await.is_ok() {
            tokio::fs::remove_dir_all(&root_path).await.map_err(|err| {
                Status::internal(format!(
                    "failed deleting content workspace directory {}: {err}",
                    root_path.display()
                ))
            })?;
        }
        Ok(Response::new(DeleteWorkspaceResponse {}))
    }

    async fn get_workspace_stats(
        &self,
        request: Request<GetWorkspaceStatsRequest>,
    ) -> Result<Response<GetWorkspaceStatsResponse>, Status> {
        ensure_admin_auth_scoped(&request, &self.auth, "admin.workspace.stats")?;
        let req = request.into_inner();
        let workspace_id = req.workspace_id.trim();
        if workspace_id.is_empty() {
            return Err(Status::invalid_argument("workspace_id cannot be empty"));
        }
        let stats = self
            .state
            .index_backend
            .get_workspace_stats(workspace_id)
            .map_err(index_status)?;
        Ok(Response::new(GetWorkspaceStatsResponse {
            file_count: stats.file_count,
            dir_count: stats.dir_count,
            chunk_count: stats.chunk_count,
            event_count: stats.event_count,
            total_content_bytes: stats.total_content_bytes,
            queued_jobs: stats.queued_jobs,
            running_jobs: stats.running_jobs,
            completed_jobs: stats.completed_jobs,
            failed_jobs: stats.failed_jobs,
            indexing_policy_full_files: stats.indexing_policy_full_files,
            indexing_policy_headline_only_files: stats.indexing_policy_headline_only_files,
            indexing_policy_skipped_files: stats.indexing_policy_skipped_files,
        }))
    }

    async fn reindex_workspace(
        &self,
        request: Request<ReindexWorkspaceRequest>,
    ) -> Result<Response<ReindexWorkspaceResponse>, Status> {
        ensure_admin_auth_scoped(&request, &self.auth, "admin.workspace.reindex")?;
        let req = request.into_inner();
        let workspace_id = req.workspace_id.trim();
        if workspace_id.is_empty() {
            return Err(Status::invalid_argument("workspace_id cannot be empty"));
        }
        let enqueued_jobs = self
            .state
            .index_backend
            .reindex_workspace(workspace_id)
            .map_err(index_status)?;
        Ok(Response::new(ReindexWorkspaceResponse { enqueued_jobs }))
    }

    async fn list_workspaces(
        &self,
        request: Request<ListWorkspacesRequest>,
    ) -> Result<Response<ListWorkspacesResponse>, Status> {
        ensure_admin_auth_scoped(&request, &self.auth, "admin.workspace.list")?;
        let req = request.into_inner();
        let limit = if req.limit == 0 {
            100
        } else {
            req.limit.clamp(1, 1000)
        };
        let cursor = req.cursor.trim();
        let cursor_opt = if cursor.is_empty() {
            None
        } else {
            Some(cursor)
        };
        let rows = self
            .state
            .catalog
            .list_workspaces(cursor_opt, limit)
            .await
            .map_err(catalog_status)?;
        let next_cursor = if rows.len() == usize::try_from(limit).unwrap_or(usize::MAX) {
            rows.last()
                .map(|row| row.workspace_id.clone())
                .unwrap_or_default()
        } else {
            String::new()
        };
        let workspaces = rows
            .into_iter()
            .map(|row| WorkspaceSummary {
                workspace_id: row.workspace_id,
                name: row.name,
                root_node_id: row.root_node_id,
                created_at_unix_nano: row.created_at_unix_nano,
            })
            .collect();
        Ok(Response::new(ListWorkspacesResponse {
            workspaces,
            next_cursor,
        }))
    }
}
