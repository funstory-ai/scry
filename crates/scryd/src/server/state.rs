use crate::server::prelude::*;

use crate::credits;
use crate::server::auth::*;
use crate::server::handle_storage::{HandleBody, HandleBodySnapshot};
use crate::server::rpc::*;
use anyhow::Context;
use bytes::BytesMut;
use scry_index::{
    normalized_index_path, prepare_file_indexing_from_utf8,
    prepare_file_indexing_headline_only_from_head_tail, prepare_non_utf8_file_indexing,
    PreparedFileIndexing, DEFAULT_HEADLINE_EACH_BYTES, DEFAULT_SKIP_CHUNKING_OVER_BYTES,
};
use std::io::{Read, Seek, SeekFrom};
#[allow(clippy::result_large_err)]
pub(crate) struct HandleState {
    pub(crate) workspace_id: String,
    pub(crate) node_id: String,
    pub(crate) mode: IndexHandleMode,
    pub(crate) snapshot_version: u64,
    pub(crate) body: HandleBody,
    pub(crate) dirty: bool,
    pub(crate) last_touched: Instant,
    pub(crate) closing: bool,
}

#[derive(Clone)]
pub(crate) struct ExpiredDirtyHandle {
    pub(crate) handle_id: String,
    pub(crate) workspace_id: String,
    pub(crate) node_id: String,
    pub(crate) mode: IndexHandleMode,
    pub(crate) snapshot_version: u64,
    pub(crate) body: HandleBodySnapshot,
}

pub(crate) struct AppState {
    pub(crate) content_store: LocalFsContentStore,
    pub(crate) index_backend: IndexBackend,
    pub(crate) catalog: Arc<dyn catalog::WorkspaceCatalog>,
    pub(crate) embedding_provider_kind: String,
    pub(crate) embedding_model_id: String,
    pub(crate) embedding_provider: Arc<dyn embedding::EmbeddingProvider>,
    pub(crate) runtime_metrics: Arc<RuntimeMetrics>,
    pub(crate) handles: Mutex<HashMap<String, HandleState>>,
    pub(crate) io_handle_memory_limit: usize,
    pub(crate) io_flush_copy_chunk_bytes: usize,
    pub(crate) handle_idle_timeout: Duration,
    pub(crate) indexing_worker_running: Mutex<bool>,
    pub(crate) event_notifiers: Arc<RwLock<HashMap<String, broadcast::Sender<EventRecord>>>>,
    pub(crate) events_bus_capacity: usize,
}

#[derive(Debug)]
pub(crate) struct RuntimeMetrics {
    pub(crate) started_at: Instant,
    pub(crate) started_unix_seconds: u64,
    pub(crate) auth_success_total: AtomicU64,
    pub(crate) auth_unauthenticated_total: AtomicU64,
    pub(crate) auth_denied_total: AtomicU64,
    pub(crate) retention_runs_total: AtomicU64,
    pub(crate) retention_deleted_total: AtomicU64,
    pub(crate) retention_last_deleted: AtomicU64,
    pub(crate) metrics_snapshot_runs_total: AtomicU64,
    /// Credits charged for embedding (see `SCRYD_EMBED_CREDITS_PER_MILLION_TOKENS`).
    pub(crate) embedding_billed_credits_total: AtomicU64,
    /// Token count used when computing credits (upstream `usage` when present, else estimate).
    pub(crate) embedding_billed_tokens_total: AtomicU64,
    /// Embedding RPCs where billing used provider `usage` fields.
    pub(crate) embedding_settlement_upstream_usage_total: AtomicU64,
    /// Embedding RPCs where billing fell back to local token estimation.
    pub(crate) embedding_settlement_estimated_total: AtomicU64,
    /// Per-(service, method, grpc-status-code) response counts.
    pub(crate) rpc_request_counts: Mutex<HashMap<(String, String, String), u64>>,
    /// Per-(service, method) latency histogram (bucket counts align with `constants::RPC_DURATION_BUCKETS_SECS`).
    pub(crate) rpc_duration_hist: Mutex<HashMap<(String, String), RpcDurationHist>>,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct RpcDurationHist {
    pub(crate) buckets: [u64; 12],
    pub(crate) sum_ns: u64,
    pub(crate) count: u64,
}

impl RuntimeMetrics {
    pub(crate) fn new() -> Self {
        Self {
            started_at: Instant::now(),
            started_unix_seconds: unix_now_secs(),
            auth_success_total: AtomicU64::new(0),
            auth_unauthenticated_total: AtomicU64::new(0),
            auth_denied_total: AtomicU64::new(0),
            retention_runs_total: AtomicU64::new(0),
            retention_deleted_total: AtomicU64::new(0),
            retention_last_deleted: AtomicU64::new(0),
            metrics_snapshot_runs_total: AtomicU64::new(0),
            embedding_billed_credits_total: AtomicU64::new(0),
            embedding_billed_tokens_total: AtomicU64::new(0),
            embedding_settlement_upstream_usage_total: AtomicU64::new(0),
            embedding_settlement_estimated_total: AtomicU64::new(0),
            rpc_request_counts: Mutex::new(HashMap::new()),
            rpc_duration_hist: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn record_rpc(
        &self,
        service: &str,
        method: &str,
        code: &str,
        elapsed: std::time::Duration,
    ) {
        let key_triple = (service.to_string(), method.to_string(), code.to_string());
        let key_pair = (service.to_string(), method.to_string());
        let secs = elapsed.as_secs_f64();
        let ns = elapsed.as_nanos().min(u128::from(u64::MAX)) as u64;

        if let Ok(mut guard) = self.rpc_request_counts.lock() {
            *guard.entry(key_triple).or_insert(0) += 1;
        }
        if let Ok(mut guard) = self.rpc_duration_hist.lock() {
            let h = guard.entry(key_pair).or_default();
            h.sum_ns = h.sum_ns.saturating_add(ns);
            h.count = h.count.saturating_add(1);
            for (i, &le) in crate::server::constants::RPC_DURATION_BUCKETS_SECS
                .iter()
                .enumerate()
            {
                if secs <= le {
                    h.buckets[i] = h.buckets[i].saturating_add(1);
                }
            }
        }
    }
}

pub(crate) static GLOBAL_RUNTIME_METRICS: OnceLock<Arc<RuntimeMetrics>> = OnceLock::new();

pub(crate) fn global_runtime_metrics() -> Option<&'static RuntimeMetrics> {
    GLOBAL_RUNTIME_METRICS.get().map(Arc::as_ref)
}

/// Incremental UTF-8 validity check (full file) without holding the file in memory.
struct Utf8ValidityReceiver {
    valid: bool,
}

impl utf8parse::Receiver for Utf8ValidityReceiver {
    fn codepoint(&mut self, _: char) {}

    fn invalid_sequence(&mut self) {
        self.valid = false;
    }
}

fn stream_chunk_bytes_for_indexing() -> usize {
    const MIN: usize = 64 * 1024;
    const DEFAULT: usize = 1024 * 1024;
    std::env::var("SCRYD_INDEX_STREAM_CHUNK_BYTES")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .map(|n| n.max(MIN))
        .unwrap_or(DEFAULT)
}

/// Max chunks per embedding batch (P2 closure remaining-risk). Caps upstream request rows
/// even when a node produces many small chunks (e.g. minified JS).
fn embed_batch_size_rows() -> usize {
    const MIN: usize = 1;
    const DEFAULT: usize = 64;
    std::env::var("SCRYD_EMBED_BATCH_SIZE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .map(|n| n.max(MIN))
        .unwrap_or(DEFAULT)
}

/// Max combined `chunk.content` bytes per embedding batch. Bounds peak RAM & upstream body
/// size regardless of how the Markdown/section splitter happens to slice a file.
fn embed_batch_size_bytes() -> usize {
    const MIN: usize = 8 * 1024;
    const DEFAULT: usize = 256 * 1024;
    std::env::var("SCRYD_EMBED_BATCH_MAX_BYTES")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .map(|n| n.max(MIN))
        .unwrap_or(DEFAULT)
}

fn utf8_prefix_from_file(file: &mut std::fs::File, max_bytes: usize) -> Result<String, String> {
    let mut take = max_bytes;
    let mut buf = Vec::new();
    let mut scratch = [0u8; 4096];
    let scratch_len = scratch.len();
    while take > 0 {
        let n = file
            .read(&mut scratch[..take.min(scratch_len)])
            .map_err(|e| format!("read utf8 head: {e}"))?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&scratch[..n]);
        take -= n;
    }
    while !buf.is_empty() && std::str::from_utf8(&buf).is_err() {
        buf.pop();
    }
    String::from_utf8(buf).map_err(|e| format!("utf8 head: {e}"))
}

fn utf8_suffix_from_file(
    file: &mut std::fs::File,
    file_len: u64,
    max_bytes: usize,
) -> Result<String, String> {
    let tail_off = file_len.saturating_sub(max_bytes as u64);
    file.seek(SeekFrom::Start(tail_off))
        .map_err(|e| format!("seek utf8 tail: {e}"))?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)
        .map_err(|e| format!("read utf8 tail: {e}"))?;
    // Tail read may start mid-codepoint; drop 0–3 leading bytes to align to a boundary.
    for k in 0..=buf.len().min(3) {
        if std::str::from_utf8(&buf[k..]).is_ok() {
            if k > 0 {
                buf.drain(..k);
            }
            break;
        }
    }
    String::from_utf8(buf).map_err(|e| format!("utf8 tail: {e}"))
}

fn indexing_prep_from_committed_file(
    path: &std::path::Path,
    normalized_path: &str,
) -> Result<(u64, PreparedFileIndexing), String> {
    let meta = std::fs::metadata(path)
        .map_err(|err| format!("failed stat committed content {}: {err}", path.display()))?;
    let len = meta.len();
    let mut file = std::fs::File::open(path).map_err(|err| {
        format!(
            "failed opening committed content {} for indexing: {err}",
            path.display()
        )
    })?;

    let skip_chunk = std::env::var("SCRYD_INDEX_SKIP_CHUNKING_OVER_BYTES")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_SKIP_CHUNKING_OVER_BYTES);
    let headline_each = std::env::var("SCRYD_INDEX_HEADLINE_EACH_BYTES")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(DEFAULT_HEADLINE_EACH_BYTES);

    let chunk_sz = stream_chunk_bytes_for_indexing();
    let mut buf = vec![0u8; chunk_sz];
    let mut hasher = blake3::Hasher::new();
    let mut parser = utf8parse::Parser::new();
    let mut recv = Utf8ValidityReceiver { valid: true };

    loop {
        let n = file
            .read(&mut buf)
            .map_err(|e| format!("read committed content for indexing: {e}"))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        for &b in &buf[..n] {
            parser.advance(&mut recv, b);
        }
    }

    let digest_hex = hasher.finalize().to_hex().to_string();

    if !recv.valid {
        return Ok((len, prepare_non_utf8_file_indexing(digest_hex)));
    }

    if len > skip_chunk {
        file.seek(SeekFrom::Start(0))
            .map_err(|e| format!("seek for headline indexing: {e}"))?;
        let each = headline_each.max(DEFAULT_HEADLINE_EACH_BYTES);
        let head = utf8_prefix_from_file(&mut file, each)?;
        let tail = utf8_suffix_from_file(&mut file, len, each)?;
        return Ok((
            len,
            prepare_file_indexing_headline_only_from_head_tail(
                normalized_path,
                len,
                digest_hex,
                headline_each,
                &head,
                &tail,
            ),
        ));
    }

    file.seek(SeekFrom::Start(0))
        .map_err(|e| format!("seek for full utf8 indexing: {e}"))?;
    let mut content = Vec::new();
    file.read_to_end(&mut content)
        .map_err(|e| format!("read full utf8 for chunking: {e}"))?;
    let text = String::from_utf8(content).map_err(|e| format!("utf8 after validation: {e}"))?;
    Ok((
        len,
        prepare_file_indexing_from_utf8(
            normalized_path,
            &text,
            len,
            digest_hex,
            skip_chunk,
            headline_each,
        ),
    ))
}

impl AppState {
    pub(crate) const DEFAULT_HANDLE_IDLE_TIMEOUT_MS: u64 = 5 * 60 * 1000;

    #[allow(dead_code)] // convenience for embedders; production uses `new_with_io_limits`.
    pub(crate) fn new(
        content_store: LocalFsContentStore,
        index_backend: IndexBackend,
        catalog: Arc<dyn catalog::WorkspaceCatalog>,
        embedding_runtime: embedding::EmbeddingRuntime,
        events_bus_capacity: usize,
    ) -> Self {
        Self::new_with_io_limits(
            content_store,
            index_backend,
            catalog,
            embedding_runtime,
            crate::server::constants::DEFAULT_IO_HANDLE_MEMORY_LIMIT,
            crate::server::constants::DEFAULT_IO_STREAM_CHUNK_BYTES,
            Duration::from_millis(Self::DEFAULT_HANDLE_IDLE_TIMEOUT_MS),
            events_bus_capacity,
        )
    }

    #[allow(dead_code)] // tests use this; server entry uses `new_with_io_limits`.
    pub(crate) fn new_with_handle_idle_timeout(
        content_store: LocalFsContentStore,
        index_backend: IndexBackend,
        catalog: Arc<dyn catalog::WorkspaceCatalog>,
        embedding_runtime: embedding::EmbeddingRuntime,
        handle_idle_timeout: Duration,
        events_bus_capacity: usize,
    ) -> Self {
        Self::new_with_io_limits(
            content_store,
            index_backend,
            catalog,
            embedding_runtime,
            crate::server::constants::DEFAULT_IO_HANDLE_MEMORY_LIMIT,
            crate::server::constants::DEFAULT_IO_STREAM_CHUNK_BYTES,
            handle_idle_timeout,
            events_bus_capacity,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_io_limits(
        content_store: LocalFsContentStore,
        index_backend: IndexBackend,
        catalog: Arc<dyn catalog::WorkspaceCatalog>,
        embedding_runtime: embedding::EmbeddingRuntime,
        io_handle_memory_limit: usize,
        io_flush_copy_chunk_bytes: usize,
        handle_idle_timeout: Duration,
        events_bus_capacity: usize,
    ) -> Self {
        Self {
            content_store,
            index_backend,
            catalog,
            embedding_provider_kind: embedding_runtime.provider_kind,
            embedding_model_id: embedding_runtime.model_id,
            embedding_provider: embedding_runtime.provider,
            runtime_metrics: Arc::new(RuntimeMetrics::new()),
            handles: Mutex::new(HashMap::new()),
            io_handle_memory_limit: io_handle_memory_limit.max(4096),
            io_flush_copy_chunk_bytes: io_flush_copy_chunk_bytes.max(8192),
            handle_idle_timeout,
            indexing_worker_running: Mutex::new(false),
            event_notifiers: Arc::new(RwLock::new(HashMap::new())),
            events_bus_capacity: events_bus_capacity.max(32),
        }
    }

    pub(crate) fn record_embedding_charge(&self, charge: &credits::EmbeddingCharge) {
        if charge.total_tokens == 0 && charge.credits == 0 {
            return;
        }
        self.runtime_metrics
            .embedding_billed_credits_total
            .fetch_add(charge.credits, Ordering::Relaxed);
        self.runtime_metrics
            .embedding_billed_tokens_total
            .fetch_add(charge.total_tokens, Ordering::Relaxed);
        if charge.from_upstream_usage {
            self.runtime_metrics
                .embedding_settlement_upstream_usage_total
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.runtime_metrics
                .embedding_settlement_estimated_total
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn publish_event_for_workspace_by_id(&self, workspace_id: &str, event_id: i64) {
        if workspace_id.trim().is_empty() {
            return;
        }
        let event = match self.index_backend.get_event_by_id(workspace_id, event_id) {
            Ok(e) => e,
            Err(err) => {
                warn!(
                    workspace_id = %workspace_id,
                    event_id,
                    error = %err,
                    "failed to load event for broadcast"
                );
                return;
            }
        };
        self.publish_event_record(workspace_id, event);
    }

    pub(crate) fn publish_event_record(&self, workspace_id: &str, event: EventRecord) {
        if workspace_id.trim().is_empty() {
            return;
        }
        if let Ok(notifiers) = self.event_notifiers.read() {
            if let Some(sender) = notifiers.get(workspace_id) {
                let _ = sender.send(event);
                return;
            }
        }
        let mut notifiers = match self.event_notifiers.write() {
            Ok(g) => g,
            Err(_) => return,
        };
        let cap = self.events_bus_capacity;
        let sender = notifiers
            .entry(workspace_id.to_string())
            .or_insert_with(|| {
                let (s, _) = broadcast::channel(cap);
                s
            });
        let _ = sender.send(event);
    }

    pub(crate) fn subscribe_workspace_events(
        &self,
        workspace_id: &str,
    ) -> Option<broadcast::Receiver<EventRecord>> {
        if workspace_id.trim().is_empty() {
            return None;
        }
        if let Ok(notifiers) = self.event_notifiers.read() {
            if let Some(sender) = notifiers.get(workspace_id) {
                return Some(sender.subscribe());
            }
        }
        let mut notifiers = self.event_notifiers.write().ok()?;
        let cap = self.events_bus_capacity;
        let sender = notifiers
            .entry(workspace_id.to_string())
            .or_insert_with(|| {
                let (s, _) = broadcast::channel(cap);
                s
            })
            .clone();
        Some(sender.subscribe())
    }

    pub(crate) async fn write_content_file_atomic(
        &self,
        workspace_id: &str,
        relative_path: &str,
        bytes: &[u8],
    ) -> Result<(), Status> {
        let file_path = self
            .content_store
            .workspace_root(workspace_id)
            .map_err(internal_status)?
            .join(relative_path.trim_start_matches('/'));
        if let Some(parent) = file_path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|err| {
                Status::internal(format!(
                    "failed creating parent directory {}: {err}",
                    parent.display()
                ))
            })?;
        }
        let mut tmp = file_path.clone();
        let tmp_name = format!(
            ".{}.tmp.{}",
            file_path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("content"),
            Uuid::now_v7()
        );
        tmp.set_file_name(tmp_name);
        tokio::fs::write(&tmp, bytes).await.map_err(|err| {
            Status::internal(format!(
                "failed writing temp content file {}: {err}",
                tmp.display()
            ))
        })?;
        tokio::fs::rename(&tmp, &file_path).await.map_err(|err| {
            let _ = std::fs::remove_file(&tmp);
            Status::internal(format!(
                "failed committing content file {}: {err}",
                file_path.display()
            ))
        })?;
        let f = tokio::fs::File::open(&file_path).await.map_err(|err| {
            Status::internal(format!(
                "failed opening content file for fsync {}: {err}",
                file_path.display()
            ))
        })?;
        f.sync_all().await.map_err(|err| {
            Status::internal(format!(
                "failed fsync content file {}: {err}",
                file_path.display()
            ))
        })?;
        Ok(())
    }

    pub(crate) fn ensure_indexing_worker(self: &Arc<Self>) {
        let mut worker_guard = match self.indexing_worker_running.lock() {
            Ok(guard) => guard,
            Err(_) => {
                warn!("failed to lock indexing worker state");
                return;
            }
        };
        if *worker_guard {
            return;
        }
        *worker_guard = true;
        drop(worker_guard);

        let state = Arc::clone(self);
        tokio::spawn(async move {
            state.run_indexing_worker().await;
        });
    }

    pub(crate) async fn run_indexing_worker(self: Arc<Self>) {
        loop {
            let jobs = match self.index_backend.take_indexing_jobs(16) {
                Ok(jobs) => jobs,
                Err(err) => {
                    warn!(error = %err, "failed to take indexing jobs");
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    continue;
                }
            };
            if jobs.is_empty() {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            for job in jobs {
                if let Err(err) = self
                    .index_node_embeddings(&job.workspace_id, &job.node_id, job.node_version)
                    .await
                {
                    warn!(
                        job_id = job.job_id,
                        workspace_id = %job.workspace_id,
                        node_id = %job.node_id,
                        error = %err,
                        "indexing job failed"
                    );
                    let delay_ms = match job.attempts {
                        0 => 250,
                        1 => 1_000,
                        2 => 5_000,
                        _ => 30_000,
                    };
                    let _ = self.index_backend.fail_indexing_job(
                        &job.workspace_id,
                        job.job_id,
                        &err.to_string(),
                        delay_ms,
                    );
                    continue;
                }
                if let Err(err) = self
                    .index_backend
                    .complete_indexing_job(&job.workspace_id, job.job_id)
                {
                    warn!(job_id = job.job_id, error = %err, "failed to mark indexing job complete");
                }
            }
        }
    }

    pub(crate) async fn index_node_embeddings(
        &self,
        workspace_id: &str,
        node_id: &str,
        index_version: u64,
    ) -> anyhow::Result<u32> {
        let batch_rows = embed_batch_size_rows();
        let batch_bytes = embed_batch_size_bytes();
        let mut after_id: i64 = 0;
        let mut total_indexed: u32 = 0;
        let mut total_tokens: u64 = 0;
        let mut total_credits: u64 = 0;
        let mut any_upstream_usage = false;

        loop {
            let page = self
                .index_backend
                .list_chunks_for_node_after(workspace_id, node_id, after_id, batch_rows)
                .context("list index chunks page")?;
            if page.is_empty() {
                break;
            }

            let mut batch_ids: Vec<i64> = Vec::with_capacity(page.len());
            let mut batch_inputs: Vec<String> = Vec::with_capacity(page.len());
            let mut batch_running_bytes: usize = 0;
            let mut next_cursor = after_id;

            for chunk in page.into_iter() {
                let chunk_bytes = chunk.content.len();
                let would_exceed_bytes = !batch_inputs.is_empty()
                    && batch_running_bytes.saturating_add(chunk_bytes) > batch_bytes;
                if would_exceed_bytes {
                    // Defer this chunk to the next batch.
                    self.flush_embed_batch(
                        workspace_id,
                        &batch_ids,
                        &batch_inputs,
                        &mut total_indexed,
                        &mut total_tokens,
                        &mut total_credits,
                        &mut any_upstream_usage,
                    )
                    .await?;
                    batch_ids.clear();
                    batch_inputs.clear();
                    batch_running_bytes = 0;
                }
                next_cursor = chunk.chunk_id;
                batch_running_bytes = batch_running_bytes.saturating_add(chunk_bytes);
                batch_ids.push(chunk.chunk_id);
                batch_inputs.push(chunk.content);
            }

            if !batch_inputs.is_empty() {
                self.flush_embed_batch(
                    workspace_id,
                    &batch_ids,
                    &batch_inputs,
                    &mut total_indexed,
                    &mut total_tokens,
                    &mut total_credits,
                    &mut any_upstream_usage,
                )
                .await?;
            }

            after_id = next_cursor;
        }

        let event_id = self
            .index_backend
            .emit_index_updated_event(workspace_id, node_id, index_version, total_indexed)
            .context("emit IndexUpdated event")?;
        self.publish_event_for_workspace_by_id(workspace_id, event_id);
        // Bill once per node after durable success so retries do not double-count credits.
        if total_tokens > 0 || total_credits > 0 {
            self.record_embedding_charge(&credits::EmbeddingCharge {
                total_tokens,
                credits: total_credits,
                from_upstream_usage: any_upstream_usage,
            });
        }
        Ok(total_indexed)
    }

    #[allow(clippy::too_many_arguments)]
    async fn flush_embed_batch(
        &self,
        workspace_id: &str,
        batch_ids: &[i64],
        batch_inputs: &[String],
        total_indexed: &mut u32,
        total_tokens: &mut u64,
        total_credits: &mut u64,
        any_upstream_usage: &mut bool,
    ) -> anyhow::Result<()> {
        let outcome = self
            .embedding_provider
            .embed_documents(batch_inputs)
            .await
            .context("embed chunks batch via provider")?;
        let embeddings = outcome.value;
        if embeddings.len() != batch_ids.len() {
            anyhow::bail!(
                "embedding batch size mismatch: {} inputs vs {} outputs",
                batch_ids.len(),
                embeddings.len()
            );
        }
        let persisted = self
            .index_backend
            .upsert_embeddings_for_chunk_ids(workspace_id, batch_ids, &embeddings)
            .context("persist chunk embeddings batch")?;
        *total_indexed = total_indexed.saturating_add(persisted);
        *total_tokens = total_tokens.saturating_add(outcome.charge.total_tokens);
        *total_credits = total_credits.saturating_add(outcome.charge.credits);
        *any_upstream_usage = *any_upstream_usage || outcome.charge.from_upstream_usage;
        Ok(())
    }

    pub(crate) fn enqueue_index_job(&self, workspace_id: &str, node_id: &str, node_version: u64) {
        if let Err(err) =
            self.index_backend
                .enqueue_indexing_job(workspace_id, node_id, node_version, 0)
        {
            warn!(
                workspace_id = %workspace_id,
                node_id = %node_id,
                version = node_version,
                error = %err,
                "failed to enqueue indexing job"
            );
            return;
        }
        // Spawn worker loop if not running.
        let mut worker_guard = match self.indexing_worker_running.lock() {
            Ok(guard) => guard,
            Err(_) => {
                warn!("failed to lock indexing worker state");
                return;
            }
        };
        if *worker_guard {
            return;
        }
        *worker_guard = true;
        drop(worker_guard);

        let state = Arc::new(AppState {
            content_store: self.content_store.clone(),
            index_backend: self.index_backend.clone(),
            catalog: Arc::clone(&self.catalog),
            embedding_provider_kind: self.embedding_provider_kind.clone(),
            embedding_model_id: self.embedding_model_id.clone(),
            embedding_provider: Arc::clone(&self.embedding_provider),
            runtime_metrics: Arc::clone(&self.runtime_metrics),
            handles: Mutex::new(HashMap::new()),
            io_handle_memory_limit: self.io_handle_memory_limit,
            io_flush_copy_chunk_bytes: self.io_flush_copy_chunk_bytes,
            handle_idle_timeout: self.handle_idle_timeout,
            indexing_worker_running: Mutex::new(true),
            event_notifiers: Arc::clone(&self.event_notifiers),
            events_bus_capacity: self.events_bus_capacity,
        });
        tokio::spawn(async move {
            state.run_indexing_worker().await;
        });
    }

    async fn write_content_streaming_atomic(
        &self,
        workspace_id: &str,
        relative_path: &str,
        body: &mut HandleBody,
    ) -> Result<(), Status> {
        let file_path = self
            .content_store
            .workspace_root(workspace_id)
            .map_err(internal_status)?
            .join(relative_path.trim_start_matches('/'));
        if let Some(parent) = file_path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|err| {
                Status::internal(format!(
                    "failed creating parent directory {}: {err}",
                    parent.display()
                ))
            })?;
        }
        let mut tmp = file_path.clone();
        let tmp_name = format!(
            ".{}.tmp.{}",
            file_path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("content"),
            Uuid::now_v7()
        );
        tmp.set_file_name(tmp_name);
        let mut out = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)
            .await
            .map_err(|err| {
                Status::internal(format!(
                    "failed opening temp content file {}: {err}",
                    tmp.display()
                ))
            })?;
        body.stream_to_writer(&mut out, self.io_flush_copy_chunk_bytes)
            .await
            .map_err(|err| {
                Status::internal(format!(
                    "failed streaming handle content to {}: {err}",
                    tmp.display()
                ))
            })?;
        tokio::fs::rename(&tmp, &file_path).await.map_err(|err| {
            let _ = std::fs::remove_file(&tmp);
            Status::internal(format!(
                "failed committing content file {}: {err}",
                file_path.display()
            ))
        })?;
        let f = tokio::fs::File::open(&file_path).await.map_err(|err| {
            Status::internal(format!(
                "failed opening content file for fsync {}: {err}",
                file_path.display()
            ))
        })?;
        f.sync_all().await.map_err(|err| {
            Status::internal(format!(
                "failed fsync content file {}: {err}",
                file_path.display()
            ))
        })?;
        Ok(())
    }

    /// On error, returns the original `body` so callers can restore handle state.
    pub(crate) async fn flush_handle_from_body(
        &self,
        workspace_id: &str,
        node_id: &str,
        mode: IndexHandleMode,
        mut body: HandleBody,
        if_version: Option<u64>,
    ) -> std::result::Result<(FlushResponse, HandleBody), (Status, HandleBody)> {
        let node = match self.index_backend.node_record_by_id(workspace_id, node_id) {
            Ok(n) => n,
            Err(e) => return Err((index_status(e), body)),
        };
        if let Err(e) = self
            .write_content_streaming_atomic(workspace_id, &node.path, &mut body)
            .await
        {
            return Err((e, body));
        }
        let path = match self.content_store.workspace_root(workspace_id) {
            Ok(root) => root.join(node.path.trim_start_matches('/')),
            Err(e) => return Err((internal_status(e), body)),
        };
        let norm_key = match normalized_index_path(&node.path) {
            Ok(p) => p,
            Err(e) => return Err((index_status(e), body)),
        };
        let path_blk = path.clone();
        let prep = tokio::task::spawn_blocking(move || {
            indexing_prep_from_committed_file(&path_blk, norm_key.as_str())
        })
        .await;
        let inner = match prep {
            Ok(r) => r,
            Err(err) => {
                return Err((
                    Status::internal(format!("flush indexing join failed: {err}")),
                    body,
                ));
            }
        };
        let (committed_len, prepared) = match inner {
            Ok(v) => v,
            Err(e) => {
                return Err((
                    Status::internal(format!("index prep from committed file: {e}")),
                    body,
                ));
            }
        };
        let flush = match self.index_backend.flush_handle_with_prepared(
            workspace_id,
            node_id,
            mode,
            committed_len,
            prepared,
            if_version,
        ) {
            Ok(f) => f,
            Err(e) => return Err((index_status(e), body)),
        };
        self.publish_event_for_workspace_by_id(workspace_id, flush.event_id);

        self.enqueue_index_job(workspace_id, node_id, flush.version);

        let new_body =
            match HandleBody::from_committed_file(&path, self.io_handle_memory_limit).await {
                Ok(b) => b,
                Err(err) => {
                    return Err((
                        Status::internal(format!(
                            "failed reloading handle body from {}: {err}",
                            path.display()
                        )),
                        body,
                    ));
                }
            };

        Ok((
            FlushResponse {
                version: flush.version,
                attrs: Some(to_proto_attrs(&flush.attrs)),
            },
            new_body,
        ))
    }

    #[allow(dead_code)] // callers that already hold a contiguous `&[u8]` buffer.
    #[allow(dead_code)] // callers that already hold a contiguous `&[u8]` buffer.
    pub(crate) async fn flush_handle_content(
        &self,
        workspace_id: &str,
        node_id: &str,
        mode: IndexHandleMode,
        content: &[u8],
        if_version: Option<u64>,
    ) -> Result<FlushResponse, Status> {
        let mut buf = BytesMut::with_capacity(content.len());
        buf.extend_from_slice(content);
        let body = HandleBody::Memory(buf);
        match self
            .flush_handle_from_body(workspace_id, node_id, mode, body, if_version)
            .await
        {
            Ok((resp, _)) => Ok(resp),
            Err((st, _)) => Err(st),
        }
    }

    async fn commit_body_snapshot_to_content_store(
        &self,
        workspace_id: &str,
        node_path: &str,
        snapshot: &HandleBodySnapshot,
    ) -> Result<(), Status> {
        let file_path = self
            .content_store
            .workspace_root(workspace_id)
            .map_err(internal_status)?
            .join(node_path.trim_start_matches('/'));
        if let Some(parent) = file_path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|err| {
                Status::internal(format!(
                    "failed creating parent directory {}: {err}",
                    parent.display()
                ))
            })?;
        }
        let mut tmp = file_path.clone();
        let tmp_name = format!(
            ".{}.tmp.{}",
            file_path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("content"),
            Uuid::now_v7()
        );
        tmp.set_file_name(tmp_name);
        let mut out = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)
            .await
            .map_err(|err| {
                Status::internal(format!(
                    "failed opening temp content file {}: {err}",
                    tmp.display()
                ))
            })?;

        let chunk = self.io_flush_copy_chunk_bytes;
        if let Some(bytes) = &snapshot.memory {
            out.write_all(bytes).await.map_err(|err| {
                Status::internal(format!(
                    "failed writing temp content file {}: {err}",
                    tmp.display()
                ))
            })?;
        } else if let Some(path) = &snapshot.spill_path {
            let mut src = File::open(path).await.map_err(|err| {
                Status::internal(format!(
                    "failed opening spilled handle file {}: {err}",
                    path.display()
                ))
            })?;
            let mut buf = vec![0u8; chunk.max(8192)];
            let mut remaining = snapshot.spill_len;
            while remaining > 0 {
                let n = std::cmp::min(buf.len() as u64, remaining) as usize;
                src.read_exact(&mut buf[..n]).await.map_err(|err| {
                    Status::internal(format!(
                        "failed reading spilled handle file {}: {err}",
                        path.display()
                    ))
                })?;
                out.write_all(&buf[..n]).await.map_err(|err| {
                    Status::internal(format!(
                        "failed writing temp content file {}: {err}",
                        tmp.display()
                    ))
                })?;
                remaining -= n as u64;
            }
        } else {
            return Err(Status::internal("empty handle snapshot for flush"));
        }

        out.sync_all().await.map_err(|err| {
            Status::internal(format!(
                "failed fsync temp content file {}: {err}",
                tmp.display()
            ))
        })?;
        drop(out);

        tokio::fs::rename(&tmp, &file_path).await.map_err(|err| {
            let _ = std::fs::remove_file(&tmp);
            Status::internal(format!(
                "failed committing content file {}: {err}",
                file_path.display()
            ))
        })?;

        let f = tokio::fs::File::open(&file_path).await.map_err(|err| {
            Status::internal(format!(
                "failed opening content file for fsync {}: {err}",
                file_path.display()
            ))
        })?;
        f.sync_all().await.map_err(|err| {
            Status::internal(format!(
                "failed fsync content file {}: {err}",
                file_path.display()
            ))
        })?;
        Ok(())
    }

    pub(crate) async fn flush_handle_from_snapshot(
        &self,
        snapshot: &ExpiredDirtyHandle,
    ) -> Result<FlushResponse, Status> {
        let node = self
            .index_backend
            .node_record_by_id(&snapshot.workspace_id, &snapshot.node_id)
            .map_err(index_status)?;
        self.commit_body_snapshot_to_content_store(
            &snapshot.workspace_id,
            &node.path,
            &snapshot.body,
        )
        .await?;
        let path = self
            .content_store
            .workspace_root(&snapshot.workspace_id)
            .map_err(internal_status)?
            .join(node.path.trim_start_matches('/'));
        let norm_key = normalized_index_path(&node.path).map_err(index_status)?;
        let prep = tokio::task::spawn_blocking({
            let path = path.clone();
            let norm_key = norm_key.clone();
            move || indexing_prep_from_committed_file(&path, norm_key.as_str())
        })
        .await
        .map_err(|err| Status::internal(format!("flush indexing join failed: {err}")))?;
        let (committed_len, prepared) =
            prep.map_err(|e| Status::internal(format!("index prep from committed file: {e}")))?;
        let flush = self
            .index_backend
            .flush_handle_with_prepared(
                &snapshot.workspace_id,
                &snapshot.node_id,
                snapshot.mode,
                committed_len,
                prepared,
                Some(snapshot.snapshot_version),
            )
            .map_err(index_status)?;
        self.publish_event_for_workspace_by_id(&snapshot.workspace_id, flush.event_id);
        self.enqueue_index_job(&snapshot.workspace_id, &snapshot.node_id, flush.version);
        Ok(FlushResponse {
            version: flush.version,
            attrs: Some(to_proto_attrs(&flush.attrs)),
        })
    }

    pub(crate) fn cleanup_idle_handles(self: &Arc<Self>) {
        let timeout = self.handle_idle_timeout;
        let now = Instant::now();
        let mut expired_dirty = Vec::<ExpiredDirtyHandle>::new();
        let mut removed_clean = 0usize;

        let mut guard = match self.handles.lock() {
            Ok(guard) => guard,
            Err(_) => {
                warn!("failed to lock handle map during idle cleanup");
                return;
            }
        };

        let mut remove_ids = Vec::<String>::new();
        for (handle_id, state) in guard.iter_mut() {
            if state.closing {
                continue;
            }
            if now.duration_since(state.last_touched) < timeout {
                continue;
            }
            if state.dirty {
                state.closing = true;
                expired_dirty.push(ExpiredDirtyHandle {
                    handle_id: handle_id.clone(),
                    workspace_id: state.workspace_id.clone(),
                    node_id: state.node_id.clone(),
                    mode: state.mode,
                    snapshot_version: state.snapshot_version,
                    body: HandleBodySnapshot::from_body(&state.body),
                });
            } else {
                remove_ids.push(handle_id.clone());
            }
        }

        for handle_id in remove_ids {
            if guard.remove(&handle_id).is_some() {
                removed_clean += 1;
            }
        }
        drop(guard);

        if removed_clean > 0 {
            info!(removed_clean, "reclaimed idle clean handles");
        }
        for snapshot in expired_dirty {
            let state = Arc::clone(self);
            tokio::spawn(async move {
                state.flush_expired_dirty_handle(snapshot).await;
            });
        }
    }

    pub(crate) async fn flush_expired_dirty_handle(self: Arc<Self>, snapshot: ExpiredDirtyHandle) {
        match self.flush_handle_from_snapshot(&snapshot).await {
            Ok(flushed) => {
                let mut guard = match self.handles.lock() {
                    Ok(guard) => guard,
                    Err(_) => {
                        warn!(
                            handle_id = %snapshot.handle_id,
                            "failed to lock handle map after idle dirty flush"
                        );
                        return;
                    }
                };
                if guard
                    .get(&snapshot.handle_id)
                    .map(|state| state.closing)
                    .unwrap_or(false)
                {
                    guard.remove(&snapshot.handle_id);
                    info!(
                        handle_id = %snapshot.handle_id,
                        workspace_id = %snapshot.workspace_id,
                        node_id = %snapshot.node_id,
                        version = flushed.version,
                        "reclaimed idle dirty handle after successful background flush"
                    );
                }
            }
            Err(err) => {
                warn!(
                    handle_id = %snapshot.handle_id,
                    workspace_id = %snapshot.workspace_id,
                    node_id = %snapshot.node_id,
                    error = %err,
                    "background flush for idle dirty handle failed"
                );
                let mut guard = match self.handles.lock() {
                    Ok(guard) => guard,
                    Err(_) => {
                        warn!(
                            handle_id = %snapshot.handle_id,
                            "failed to lock handle map while resetting idle dirty handle state"
                        );
                        return;
                    }
                };
                if let Some(state) = guard.get_mut(&snapshot.handle_id) {
                    state.closing = false;
                    state.last_touched = Instant::now();
                }
            }
        }
    }
}
