use crate::server::prelude::*;

use crate::server::auth::*;
use crate::server::config::AuthConfig;
use crate::server::rpc::*;
use crate::server::state::AppState;

pub(crate) struct SearchSvc {
    pub(crate) state: Arc<AppState>,
    pub(crate) auth: Arc<AuthConfig>,
}

#[tonic::async_trait]
impl Search for SearchSvc {
    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        ensure_workspace_auth(&request, &self.auth, request.get_ref().workspace_id.trim())?;
        let req = request.into_inner();
        if req.query.trim().is_empty() {
            return Err(Status::invalid_argument("query cannot be empty"));
        }
        let fts_query = normalize_fts_query(&req.query);
        let mode = to_index_search_mode(req.mode);
        let result_set = match mode {
            scry_index::SearchMode::Fts => self
                .state
                .index_backend
                .search_fts(&req.workspace_id, &fts_query, req.limit)
                .map_err(index_status)?,
            scry_index::SearchMode::Vector => {
                let query_outcome = self
                    .state
                    .embedding_provider
                    .embed_query(&req.query)
                    .await
                    .map_err(embedding_status)?;
                let query_embedding = query_outcome.value;
                let result_set = self
                    .state
                    .index_backend
                    .search_vector_with_embedding(&req.workspace_id, &query_embedding, req.limit)
                    .map_err(index_status)?;
                self.state.record_embedding_charge(&query_outcome.charge);
                result_set
            }
            scry_index::SearchMode::Hybrid => {
                let query_outcome = self
                    .state
                    .embedding_provider
                    .embed_query(&req.query)
                    .await
                    .map_err(embedding_status)?;
                let query_embedding = query_outcome.value;
                let result_set = self
                    .state
                    .index_backend
                    .search_hybrid_with_embedding(
                        &req.workspace_id,
                        &fts_query,
                        &query_embedding,
                        req.limit,
                    )
                    .map_err(index_status)?;
                self.state.record_embedding_charge(&query_outcome.charge);
                result_set
            }
        };
        let mapped = result_set
            .hits
            .into_iter()
            .map(|hit| scry_proto::scry::v1::SearchHit {
                node_id: hit.node_id,
                path: hit.path.clone(),
                start_line: hit.start_line,
                end_line: hit.end_line,
                score: hit.score,
                snippet: hit.snippet,
                context_path: hit.context_path,
                degraded_indexing: hit.headline_only,
            })
            .collect();
        Ok(Response::new(SearchResponse {
            hits: mapped,
            total_hits: result_set.total_hits,
        }))
    }
}

fn normalize_fts_query(raw: &str) -> String {
    let trimmed = raw.trim();
    if let Some(expr) = trimmed.strip_prefix("raw:") {
        let passthrough = expr.trim();
        if !passthrough.is_empty() {
            return passthrough.to_string();
        }
    }
    // Default to literal phrase search so special characters (e.g. '-') do not break MATCH parsing.
    format!("\"{}\"", trimmed.replace('"', "\"\""))
}
