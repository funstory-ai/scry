use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[cfg(test)]
use std::path::Path;

use anyhow::Context;
use scry_index::{
    AttrsPatch, EventRecord, FileRecord, FlushResult, HandleMode, IndexError, IndexStore,
    IndexingJobRecord, IndexingMetricsSnapshot, NodeAttrs, NodeKind, NodeRecord,
    PreparedFileIndexing, ResolveRefResult, SearchResultSet, WorkspaceStats,
};

/// Per-workspace index backend.
///
/// Each workspace has its own SQLite file at `{root}/{workspace_id}/index.db`.
/// There is no shared layout; everything routes through `store_for_workspace`.
#[derive(Clone)]
pub(crate) struct IndexBackend {
    root: PathBuf,
    stores: Arc<Mutex<HashMap<String, IndexStore>>>,
}

impl IndexBackend {
    /// Open the per-workspace backend rooted at `SCRYD_INDEX_ROOT`.
    #[cfg(test)]
    pub(crate) fn from_env() -> anyhow::Result<Self> {
        let root = std::env::var("SCRYD_INDEX_ROOT").context(
            "SCRYD_INDEX_ROOT is required (per-workspace layout is the only supported layout)",
        )?;
        Self::open(PathBuf::from(root))
    }

    /// Open or create the per-workspace backend rooted at `root`.
    pub(crate) fn open(root: PathBuf) -> anyhow::Result<Self> {
        fs::create_dir_all(&root).with_context(|| {
            format!(
                "failed to create per-workspace index root {}",
                root.display()
            )
        })?;
        Ok(Self {
            root,
            stores: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub(crate) fn layout_label(&self) -> &'static str {
        "per-workspace"
    }

    pub(crate) fn open_store_count(&self) -> usize {
        self.stores.lock().map(|g| g.len()).unwrap_or(0)
    }

    #[cfg(test)]
    pub(crate) fn root_path(&self) -> &Path {
        &self.root
    }

    pub(crate) fn db_path_for_workspace(&self, workspace_id: &str) -> scry_index::Result<PathBuf> {
        let workspace = workspace_id.trim();
        if workspace.is_empty() {
            return Err(IndexError::InvalidPath(
                "workspace_id cannot be empty".to_string(),
            ));
        }
        Ok(self.root.join(workspace).join("index.db"))
    }

    fn store_for_workspace(
        &self,
        workspace_id: &str,
        create_if_missing: bool,
    ) -> scry_index::Result<IndexStore> {
        let workspace = workspace_id.trim();
        if workspace.is_empty() {
            return Err(IndexError::InvalidPath(
                "workspace_id cannot be empty".to_string(),
            ));
        }
        if let Ok(guard) = self.stores.lock() {
            if let Some(store) = guard.get(workspace) {
                return Ok(store.clone());
            }
        }
        let db_path = self.db_path_for_workspace(workspace)?;
        if !create_if_missing && !db_path.is_file() {
            return Err(IndexError::WorkspaceNotFound(workspace.to_string()));
        }
        if let Some(parent) = db_path.parent() {
            fs::create_dir_all(parent).map_err(|err| {
                IndexError::InvalidPath(format!(
                    "failed to create index directory {}: {err}",
                    parent.display()
                ))
            })?;
        }
        let store = IndexStore::open(&db_path)?;
        let mut guard = self.stores.lock().map_err(|_| {
            IndexError::ConnectionPoolPoisoned("per-workspace index store map poisoned".to_string())
        })?;
        guard.insert(workspace.to_string(), store.clone());
        Ok(store)
    }

    fn list_workspace_ids(&self) -> scry_index::Result<Vec<String>> {
        let mut out = HashSet::<String>::new();
        let entries = fs::read_dir(&self.root).map_err(|err| {
            IndexError::InvalidPath(format!(
                "failed to read per-workspace index root {}: {err}",
                self.root.display()
            ))
        })?;
        for entry in entries {
            let entry = entry.map_err(|err| {
                IndexError::InvalidPath(format!(
                    "failed to inspect per-workspace index root {}: {err}",
                    self.root.display()
                ))
            })?;
            if !entry.file_type().map(|ty| ty.is_dir()).unwrap_or(false) {
                continue;
            }
            let workspace = entry.file_name().to_string_lossy().to_string();
            if workspace.trim().is_empty() {
                continue;
            }
            let db_path = entry.path().join("index.db");
            if db_path.is_file() {
                out.insert(workspace);
            }
        }
        if let Ok(guard) = self.stores.lock() {
            out.extend(guard.keys().cloned());
        }
        let mut ids = out.into_iter().collect::<Vec<_>>();
        ids.sort();
        Ok(ids)
    }

    fn aggregate_over_stores<T>(
        &self,
        mut f: impl FnMut(&IndexStore) -> scry_index::Result<T>,
    ) -> scry_index::Result<Vec<T>> {
        let ids = self.list_workspace_ids()?;
        let mut out = Vec::with_capacity(ids.len());
        for workspace_id in ids {
            let store = self.store_for_workspace(&workspace_id, false)?;
            out.push(f(&store)?);
        }
        Ok(out)
    }

    pub(crate) fn create_workspace(
        &self,
        workspace_id: &str,
        name: &str,
    ) -> scry_index::Result<String> {
        let store = self.store_for_workspace(workspace_id, true)?;
        store.create_workspace(workspace_id, name)
    }

    pub(crate) fn delete_workspace(&self, workspace_id: &str) -> scry_index::Result<()> {
        let store = self.store_for_workspace(workspace_id, false)?;
        store.delete_workspace(workspace_id)?;
        if let Ok(mut guard) = self.stores.lock() {
            guard.remove(workspace_id);
        }
        let db_path = self.db_path_for_workspace(workspace_id)?;
        if db_path.is_file() {
            fs::remove_file(&db_path).map_err(|err| {
                IndexError::InvalidPath(format!(
                    "failed removing workspace index db {}: {err}",
                    db_path.display()
                ))
            })?;
        }
        // Best-effort: clean up the now-empty parent directory; ignore if non-empty.
        if let Some(parent) = db_path.parent() {
            let _ = fs::remove_dir(parent);
        }
        Ok(())
    }

    /// Aggregate all per-workspace summaries (workspace_id, name, root_node_id) for catalog reconcile.
    pub(crate) fn list_all_workspace_summaries(
        &self,
    ) -> scry_index::Result<Vec<(String, String, String)>> {
        let ids = self.list_workspace_ids()?;
        let mut out = Vec::with_capacity(ids.len());
        for workspace_id in ids {
            let store = self.store_for_workspace(&workspace_id, false)?;
            for row in store.list_workspace_summaries()? {
                out.push(row);
            }
        }
        Ok(out)
    }

    pub(crate) fn get_workspace_stats(
        &self,
        workspace_id: &str,
    ) -> scry_index::Result<WorkspaceStats> {
        self.store_for_workspace(workspace_id, false)?
            .get_workspace_stats(workspace_id)
    }

    pub(crate) fn reindex_workspace(&self, workspace_id: &str) -> scry_index::Result<u32> {
        self.store_for_workspace(workspace_id, false)?
            .reindex_workspace(workspace_id)
    }

    pub(crate) fn put_file(
        &self,
        workspace_id: &str,
        path: &str,
        content: &[u8],
        if_version: Option<u64>,
    ) -> scry_index::Result<(FileRecord, i64)> {
        self.store_for_workspace(workspace_id, false)?.put_file(
            workspace_id,
            path,
            content,
            if_version,
        )
    }

    pub(crate) fn flush_handle_with_prepared(
        &self,
        workspace_id: &str,
        node_id: &str,
        mode: HandleMode,
        size: u64,
        prepared: PreparedFileIndexing,
        if_version: Option<u64>,
    ) -> scry_index::Result<FlushResult> {
        self.store_for_workspace(workspace_id, false)?
            .flush_handle_with_prepared(workspace_id, node_id, mode, size, prepared, if_version)
    }

    pub(crate) fn node_record_by_id(
        &self,
        workspace_id: &str,
        node_id: &str,
    ) -> scry_index::Result<NodeRecord> {
        self.store_for_workspace(workspace_id, false)?
            .node_record_by_id(workspace_id, node_id)
    }

    pub(crate) fn get_node(
        &self,
        workspace_id: &str,
        node_id: &str,
    ) -> scry_index::Result<NodeRecord> {
        self.store_for_workspace(workspace_id, false)?
            .get_node(workspace_id, node_id)
    }

    pub(crate) fn resolve_ref(
        &self,
        workspace_id: &str,
        node_id: &str,
    ) -> scry_index::Result<Option<String>> {
        self.store_for_workspace(workspace_id, false)?
            .resolve_ref(workspace_id, node_id)
    }

    pub(crate) fn resolve_ref_with_redirect(
        &self,
        workspace_id: &str,
        node_id: &str,
    ) -> scry_index::Result<ResolveRefResult> {
        self.store_for_workspace(workspace_id, false)?
            .resolve_ref_with_redirect(workspace_id, node_id)
    }

    pub(crate) fn resolve_path(
        &self,
        workspace_id: &str,
        path: &str,
    ) -> scry_index::Result<Option<String>> {
        self.store_for_workspace(workspace_id, false)?
            .resolve_path(workspace_id, path)
    }

    pub(crate) fn lookup(
        &self,
        workspace_id: &str,
        parent_node_id: &str,
        name: &str,
    ) -> scry_index::Result<NodeRecord> {
        self.store_for_workspace(workspace_id, false)?
            .lookup(workspace_id, parent_node_id, name)
    }

    pub(crate) fn get_attrs(
        &self,
        workspace_id: &str,
        node_id: &str,
    ) -> scry_index::Result<NodeAttrs> {
        self.store_for_workspace(workspace_id, false)?
            .get_attrs(workspace_id, node_id)
    }

    pub(crate) fn get_attrs_if_changed(
        &self,
        workspace_id: &str,
        node_id: &str,
        known_version: u64,
    ) -> scry_index::Result<Option<NodeAttrs>> {
        self.store_for_workspace(workspace_id, false)?
            .get_attrs_if_changed(workspace_id, node_id, known_version)
    }

    pub(crate) fn read_dir(
        &self,
        workspace_id: &str,
        node_id: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> scry_index::Result<(Vec<NodeRecord>, Option<String>)> {
        self.store_for_workspace(workspace_id, false)?.read_dir(
            workspace_id,
            node_id,
            cursor,
            limit,
        )
    }

    pub(crate) fn create_node(
        &self,
        workspace_id: &str,
        parent_node_id: &str,
        name: &str,
        kind: NodeKind,
        mode: u32,
        exclusive: bool,
    ) -> scry_index::Result<(NodeRecord, Option<i64>)> {
        self.store_for_workspace(workspace_id, false)?.create_node(
            workspace_id,
            parent_node_id,
            name,
            kind,
            mode,
            exclusive,
        )
    }

    pub(crate) fn unlink(
        &self,
        workspace_id: &str,
        parent_node_id: &str,
        name: &str,
        if_version: Option<u64>,
    ) -> scry_index::Result<i64> {
        self.store_for_workspace(workspace_id, false)?.unlink(
            workspace_id,
            parent_node_id,
            name,
            if_version,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn rename(
        &self,
        workspace_id: &str,
        from_parent_node_id: &str,
        from_name: &str,
        to_parent_node_id: &str,
        to_name: &str,
        overwrite: bool,
        if_version: Option<u64>,
    ) -> scry_index::Result<((String, u64), i64)> {
        self.store_for_workspace(workspace_id, false)?.rename(
            workspace_id,
            from_parent_node_id,
            from_name,
            to_parent_node_id,
            to_name,
            overwrite,
            if_version,
        )
    }

    pub(crate) fn set_attrs(
        &self,
        workspace_id: &str,
        node_id: &str,
        patch: AttrsPatch,
    ) -> scry_index::Result<(NodeAttrs, i64)> {
        self.store_for_workspace(workspace_id, false)?
            .set_attrs(workspace_id, node_id, patch)
    }

    pub(crate) fn search_fts(
        &self,
        workspace_id: &str,
        query: &str,
        limit: u32,
    ) -> scry_index::Result<SearchResultSet> {
        self.store_for_workspace(workspace_id, false)?
            .search_fts(workspace_id, query, limit)
    }

    pub(crate) fn search_vector_with_embedding(
        &self,
        workspace_id: &str,
        query_embedding: &[f32],
        limit: u32,
    ) -> scry_index::Result<SearchResultSet> {
        self.store_for_workspace(workspace_id, false)?
            .search_vector_with_embedding(workspace_id, query_embedding, limit)
    }

    pub(crate) fn search_hybrid_with_embedding(
        &self,
        workspace_id: &str,
        query: &str,
        query_embedding: &[f32],
        limit: u32,
    ) -> scry_index::Result<SearchResultSet> {
        self.store_for_workspace(workspace_id, false)?
            .search_hybrid_with_embedding(workspace_id, query, query_embedding, limit)
    }

    pub(crate) fn get_event_by_id(
        &self,
        workspace_id: &str,
        event_id: i64,
    ) -> scry_index::Result<EventRecord> {
        self.store_for_workspace(workspace_id, false)?
            .get_event_by_id(workspace_id, event_id)
    }

    pub(crate) fn list_events_since(
        &self,
        workspace_id: &str,
        since_cursor: Option<u64>,
        limit: u32,
    ) -> scry_index::Result<Vec<EventRecord>> {
        self.store_for_workspace(workspace_id, false)?
            .list_events_since(workspace_id, since_cursor, limit)
    }

    pub(crate) fn acknowledge_cursor(
        &self,
        workspace_id: &str,
        subscriber_id: &str,
        cursor: &str,
    ) -> scry_index::Result<()> {
        self.store_for_workspace(workspace_id, false)?
            .acknowledge_cursor(workspace_id, subscriber_id, cursor)
    }

    pub(crate) fn latest_event_cursor(
        &self,
        workspace_id: &str,
    ) -> scry_index::Result<Option<u64>> {
        self.store_for_workspace(workspace_id, false)?
            .latest_event_cursor(workspace_id)
    }

    pub(crate) fn subscriber_cursor(
        &self,
        workspace_id: &str,
        subscriber_id: &str,
    ) -> scry_index::Result<Option<u64>> {
        self.store_for_workspace(workspace_id, false)?
            .subscriber_cursor(workspace_id, subscriber_id)
    }

    pub(crate) fn enqueue_indexing_job(
        &self,
        workspace_id: &str,
        node_id: &str,
        node_version: u64,
        priority: u8,
    ) -> scry_index::Result<()> {
        self.store_for_workspace(workspace_id, false)?
            .enqueue_indexing_job(workspace_id, node_id, node_version, priority)
    }

    pub(crate) fn take_indexing_jobs(
        &self,
        limit: u32,
    ) -> scry_index::Result<Vec<IndexingJobRecord>> {
        let mut jobs = Vec::<IndexingJobRecord>::new();
        for workspace_id in self.list_workspace_ids()? {
            let remaining = limit.saturating_sub(jobs.len() as u32);
            if remaining == 0 {
                break;
            }
            let store = self.store_for_workspace(&workspace_id, false)?;
            jobs.extend(store.take_indexing_jobs(remaining)?);
        }
        Ok(jobs)
    }

    pub(crate) fn complete_indexing_job(
        &self,
        workspace_id: &str,
        job_id: i64,
    ) -> scry_index::Result<()> {
        self.store_for_workspace(workspace_id, false)?
            .complete_indexing_job(job_id)
    }

    pub(crate) fn fail_indexing_job(
        &self,
        workspace_id: &str,
        job_id: i64,
        error_message: &str,
        retry_delay_ms: u64,
    ) -> scry_index::Result<()> {
        self.store_for_workspace(workspace_id, false)?
            .fail_indexing_job(job_id, error_message, retry_delay_ms)
    }

    #[allow(dead_code)] // retained for callers that still need all chunks at once.
    pub(crate) fn list_chunks_for_node(
        &self,
        workspace_id: &str,
        node_id: &str,
    ) -> scry_index::Result<Vec<scry_index::ChunkContent>> {
        self.store_for_workspace(workspace_id, false)?
            .list_chunks_for_node(workspace_id, node_id)
    }

    pub(crate) fn list_chunks_for_node_after(
        &self,
        workspace_id: &str,
        node_id: &str,
        after_id: i64,
        limit: usize,
    ) -> scry_index::Result<Vec<scry_index::ChunkContent>> {
        self.store_for_workspace(workspace_id, false)?
            .list_chunks_for_node_after(workspace_id, node_id, after_id, limit)
    }

    #[allow(dead_code)] // retained for tests / callers that replace the full embedding set at once.
    pub(crate) fn upsert_embeddings_for_node(
        &self,
        workspace_id: &str,
        node_id: &str,
        embeddings: &[Vec<f32>],
    ) -> scry_index::Result<u32> {
        self.store_for_workspace(workspace_id, false)?
            .upsert_embeddings_for_node(workspace_id, node_id, embeddings)
    }

    pub(crate) fn upsert_embeddings_for_chunk_ids(
        &self,
        workspace_id: &str,
        chunk_ids: &[i64],
        embeddings: &[Vec<f32>],
    ) -> scry_index::Result<u32> {
        self.store_for_workspace(workspace_id, false)?
            .upsert_embeddings_for_chunk_ids(workspace_id, chunk_ids, embeddings)
    }

    pub(crate) fn emit_index_updated_event(
        &self,
        workspace_id: &str,
        node_id: &str,
        index_version: u64,
        chunks_indexed: u32,
    ) -> scry_index::Result<i64> {
        self.store_for_workspace(workspace_id, false)?
            .emit_index_updated_event(workspace_id, node_id, index_version, chunks_indexed)
    }

    pub(crate) fn indexing_metrics_snapshot(&self) -> scry_index::Result<IndexingMetricsSnapshot> {
        let snapshots = self.aggregate_over_stores(|store| store.indexing_metrics_snapshot())?;
        let mut merged = IndexingMetricsSnapshot {
            queued_jobs: 0,
            running_jobs: 0,
            completed_jobs: 0,
            failed_jobs: 0,
            avg_running_latency_ms: None,
            avg_completion_latency_ms: None,
        };
        let mut running_latencies = Vec::<u64>::new();
        let mut completion_latencies = Vec::<u64>::new();
        for snapshot in snapshots {
            merged.queued_jobs = merged.queued_jobs.saturating_add(snapshot.queued_jobs);
            merged.running_jobs = merged.running_jobs.saturating_add(snapshot.running_jobs);
            merged.completed_jobs = merged
                .completed_jobs
                .saturating_add(snapshot.completed_jobs);
            merged.failed_jobs = merged.failed_jobs.saturating_add(snapshot.failed_jobs);
            if let Some(v) = snapshot.avg_running_latency_ms {
                running_latencies.push(v);
            }
            if let Some(v) = snapshot.avg_completion_latency_ms {
                completion_latencies.push(v);
            }
        }
        merged.avg_running_latency_ms = average_ms(&running_latencies);
        merged.avg_completion_latency_ms = average_ms(&completion_latencies);
        Ok(merged)
    }

    pub(crate) fn cleanup_events_retention(
        &self,
        max_age: std::time::Duration,
        min_events_per_workspace: u64,
    ) -> scry_index::Result<u64> {
        let mut deleted = 0u64;
        for count in self.aggregate_over_stores(|store| {
            store.cleanup_events_retention(max_age, min_events_per_workspace)
        })? {
            deleted = deleted.saturating_add(count);
        }
        Ok(deleted)
    }
}

fn average_ms(values: &[u64]) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    let sum = values
        .iter()
        .fold(0u128, |acc, value| acc.saturating_add(u128::from(*value)));
    let avg = sum / values.len() as u128;
    Some(u64::try_from(avg).unwrap_or(u64::MAX))
}
