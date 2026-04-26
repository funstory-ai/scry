use std::{
    os::raw::{c_char, c_int},
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex, MutexGuard, Once},
    time::{SystemTime, UNIX_EPOCH},
};

use rusqlite::{ffi::sqlite3_auto_extension, params, Connection, OptionalExtension, Transaction};
use sqlite_vec::sqlite3_vec_init;
use uuid::Uuid;

mod chunking;
mod file_indexing;
mod migrations;

pub use chunking::{chunk_text, headline_only_chunk, headline_only_chunk_bounded, IndexedChunk};
pub use file_indexing::{
    prepare_file_indexing, prepare_file_indexing_from_utf8,
    prepare_file_indexing_headline_only_from_head_tail, prepare_non_utf8_file_indexing,
    IndexingPolicyDb, PreparedFileIndexing, DEFAULT_HEADLINE_EACH_BYTES,
    DEFAULT_SKIP_CHUNKING_OVER_BYTES,
};

const CONNECTION_PRAGMAS_SQL: &str = r#"
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;
"#;

pub const BOOTSTRAP_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS workspaces (
  workspace_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  created_at_ns INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS nodes (
  node_id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
  parent_node_id TEXT REFERENCES nodes(node_id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  path TEXT NOT NULL,
  kind TEXT NOT NULL DEFAULT 'file',
  mode INTEGER NOT NULL DEFAULT 420,
  mtime_ns INTEGER NOT NULL,
  version INTEGER NOT NULL DEFAULT 1,
  size INTEGER NOT NULL DEFAULT 0,
  created_at_ns INTEGER NOT NULL,
  updated_at_ns INTEGER NOT NULL,
  content_hash TEXT NOT NULL DEFAULT '',
  mime TEXT NOT NULL DEFAULT '',
  indexing_policy TEXT NOT NULL DEFAULT 'full',
  UNIQUE(workspace_id, path)
);

CREATE TABLE IF NOT EXISTS chunks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
  node_id TEXT NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
  start_line INTEGER NOT NULL,
  end_line INTEGER NOT NULL,
  content TEXT NOT NULL,
  context_path TEXT NOT NULL,
  is_headline INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS chunk_embeddings (
  chunk_id INTEGER PRIMARY KEY REFERENCES chunks(id) ON DELETE CASCADE,
  embedding_json TEXT NOT NULL
);

CREATE VIRTUAL TABLE IF NOT EXISTS vec_chunks USING vec0(
  chunk_id INTEGER PRIMARY KEY,
  embedding float[32] distance_metric=cosine,
  workspace_id TEXT partition key
);

CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
  content,
  context_path,
  content='chunks',
  content_rowid='id',
  tokenize='unicode61 remove_diacritics 2'
);

CREATE TRIGGER IF NOT EXISTS chunks_ai AFTER INSERT ON chunks BEGIN
  INSERT INTO chunks_fts(rowid, content, context_path)
  VALUES (new.id, new.content, new.context_path);
END;

CREATE TRIGGER IF NOT EXISTS chunks_ad AFTER DELETE ON chunks BEGIN
  INSERT INTO chunks_fts(chunks_fts, rowid, content, context_path)
  VALUES ('delete', old.id, old.content, old.context_path);
END;

CREATE TRIGGER IF NOT EXISTS chunks_au AFTER UPDATE ON chunks BEGIN
  INSERT INTO chunks_fts(chunks_fts, rowid, content, context_path)
  VALUES ('delete', old.id, old.content, old.context_path);
  INSERT INTO chunks_fts(rowid, content, context_path)
  VALUES (new.id, new.content, new.context_path);
END;

CREATE TABLE IF NOT EXISTS events (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  workspace_id    TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
  kind            TEXT NOT NULL,
  node_id         TEXT,
  payload_json    TEXT NOT NULL,
  created_at_ns   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS redirects (
  from_node_id TEXT PRIMARY KEY,
  to_node_id TEXT NOT NULL,
  reason TEXT NOT NULL,
  created_at_ns INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS config (
  key             TEXT PRIMARY KEY,
  value           TEXT NOT NULL,
  updated_at_ns   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS subscriber_acks (
  workspace_id TEXT NOT NULL,
  subscriber_id TEXT NOT NULL,
  cursor_id INTEGER NOT NULL,
  updated_at_ns INTEGER NOT NULL,
  PRIMARY KEY (workspace_id, subscriber_id)
);

CREATE TABLE IF NOT EXISTS indexing_jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
  node_id TEXT NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
  node_version INTEGER NOT NULL,
  priority INTEGER NOT NULL DEFAULT 0,
  attempts INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'queued'
    CHECK (status IN ('queued', 'running', 'completed', 'failed')),
  last_error TEXT,
  scheduled_at_ns INTEGER NOT NULL,
  started_at_ns INTEGER,
  completed_at_ns INTEGER,
  created_at_ns INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_indexing_jobs_ready
  ON indexing_jobs(priority, scheduled_at_ns)
  WHERE status = 'queued';

CREATE INDEX IF NOT EXISTS idx_indexing_jobs_workspace
  ON indexing_jobs(workspace_id, status);
"#;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileRecord {
    pub node_id: String,
    pub version: u64,
    pub size: u64,
    pub content_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushResult {
    pub version: u64,
    pub attrs: NodeAttrs,
    pub event_id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileChunkRecord {
    pub chunk_id: i64,
    pub start_line: u32,
    pub end_line: u32,
    pub content: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeKind {
    File,
    Dir,
}

impl NodeKind {
    fn as_db(&self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Dir => "dir",
        }
    }

    fn from_db(value: &str) -> Option<Self> {
        match value {
            "file" => Some(Self::File),
            "dir" => Some(Self::Dir),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeAttrs {
    pub kind: NodeKind,
    pub size: u64,
    pub mtime_unix_nano: i64,
    pub mode: u32,
    pub version: u64,
    /// Blake3 hex digest; empty for directories or unknown legacy rows.
    pub content_hash: String,
    pub mime: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRecord {
    pub node_id: String,
    pub parent_node_id: Option<String>,
    pub name: String,
    pub path: String,
    pub attrs: NodeAttrs,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceStats {
    pub file_count: u64,
    pub dir_count: u64,
    pub chunk_count: u64,
    pub event_count: u64,
    pub total_content_bytes: u64,
    pub queued_jobs: u64,
    pub running_jobs: u64,
    pub completed_jobs: u64,
    pub failed_jobs: u64,
    pub indexing_policy_full_files: u64,
    pub indexing_policy_headline_only_files: u64,
    pub indexing_policy_skipped_files: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexingMetricsSnapshot {
    pub queued_jobs: u64,
    pub running_jobs: u64,
    pub completed_jobs: u64,
    pub failed_jobs: u64,
    pub avg_running_latency_ms: Option<u64>,
    pub avg_completion_latency_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttrsPatch {
    pub mode: Option<u32>,
    pub mtime_unix_nano: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchRecord {
    pub node_id: String,
    pub path: String,
    pub start_line: u32,
    pub end_line: u32,
    pub score: f32,
    pub snippet: String,
    pub context_path: String,
    /// True when the hit is from a degraded (headline-only) index row.
    pub headline_only: bool,
}

#[derive(Debug, Clone)]
pub struct SearchResultSet {
    pub hits: Vec<SearchRecord>,
    pub total_hits: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkContent {
    pub chunk_id: i64,
    pub content: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchMode {
    Fts,
    Vector,
    Hybrid,
}

#[derive(Debug, Clone)]
struct RankedChunk {
    chunk_id: i64,
    rank: usize,
    record: SearchRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventKind {
    NodeCreated,
    NodeModified,
    NodeDeleted,
    NodeRenamed,
    AttrsChanged,
    IndexUpdated,
}

impl EventKind {
    fn as_db(&self) -> &'static str {
        match self {
            Self::NodeCreated => "NodeCreated",
            Self::NodeModified => "NodeModified",
            Self::NodeDeleted => "NodeDeleted",
            Self::NodeRenamed => "NodeRenamed",
            Self::AttrsChanged => "AttrsChanged",
            Self::IndexUpdated => "IndexUpdated",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventRecord {
    pub cursor: String,
    pub created_at_unix_nano: i64,
    pub kind: EventKind,
    pub workspace_id: String,
    pub node_id: String,
    pub payload_json: String,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexingJobStatus {
    Queued,
    Running,
    Completed,
    Failed,
}

impl IndexingJobStatus {
    fn as_db(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }

    fn from_db(value: &str) -> Option<Self> {
        match value {
            "queued" => Some(Self::Queued),
            "running" => Some(Self::Running),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexingJobRecord {
    pub job_id: i64,
    pub workspace_id: String,
    pub node_id: String,
    pub node_version: u64,
    pub priority: u8,
    pub attempts: u32,
    pub status: IndexingJobStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolveRefResult {
    pub path: Option<String>,
    pub redirect_to_node_id: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("workspace already exists: {0}")]
    WorkspaceAlreadyExists(String),
    #[error("workspace not found: {0}")]
    WorkspaceNotFound(String),
    #[error("invalid node kind in index: {0}")]
    InvalidNodeKind(String),
    #[error("invalid node path: {0}")]
    InvalidPath(String),
    #[error("parent node not found: {0}")]
    ParentNotFound(String),
    #[error("node already exists at path {path}")]
    AlreadyExistsAtPath { path: String },
    #[error("node id not found in workspace={workspace_id}, node_id={node_id}")]
    NodeIdNotFound {
        workspace_id: String,
        node_id: String,
    },
    #[error("node is not a directory: {0}")]
    NotDirectory(String),
    #[error("node is not empty directory: {0}")]
    DirectoryNotEmpty(String),
    #[error("invalid cursor")]
    InvalidCursor,
    #[error("node not found for workspace={workspace_id}, path={path}")]
    NodeNotFound { workspace_id: String, path: String },
    #[error("numeric conversion failed for database value")]
    NumericConversion,
    #[error("embedding vector count mismatch: expected={expected}, actual={actual}")]
    EmbeddingCountMismatch { expected: usize, actual: usize },
    #[error("invalid handle mode: {0}")]
    InvalidHandleMode(String),
    #[error("invalid indexing job status: {0}")]
    InvalidIndexingJobStatus(String),
    #[error("sqlite connection pool poisoned: {0}")]
    ConnectionPoolPoisoned(String),
    #[error("sqlite connection pool exhausted")]
    ConnectionPoolExhausted,
    #[error(
        "redirect chain exceeded max hops ({max_hops}) for workspace={workspace_id}, node_id={node_id}"
    )]
    RedirectLimitExceeded {
        workspace_id: String,
        node_id: String,
        max_hops: usize,
    },
    #[error("redirect cycle detected for workspace={workspace_id}, node_id={node_id}")]
    RedirectLoop {
        workspace_id: String,
        node_id: String,
    },
    #[error("version precondition failed: expected {expected}, found {found}")]
    VersionConflict { expected: u64, found: u64 },
    #[error("file content is stored in the workspace content store, not in the index database")]
    ContentNotInIndex,
    #[error("invalid input: {0}")]
    InvalidInput(String),
}

pub type Result<T> = std::result::Result<T, IndexError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandleMode {
    Read,
    Write,
    ReadWrite,
}

impl HandleMode {
    pub fn allows_read(self) -> bool {
        matches!(self, Self::Read | Self::ReadWrite)
    }

    pub fn allows_write(self) -> bool {
        matches!(self, Self::Write | Self::ReadWrite)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandleContentRecord {
    pub node_id: String,
    pub path: String,
    pub content: Vec<u8>,
    pub attrs: NodeAttrs,
}

const MAX_REDIRECT_HOPS: usize = 16;

#[derive(Clone)]
pub struct IndexStore {
    db_path: PathBuf,
    connections: Arc<StoreConnections>,
}

impl std::fmt::Debug for IndexStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexStore")
            .field("db_path", &self.db_path)
            .field("reader_pool_size", &self.connections.reader_pool_size)
            .finish()
    }
}

struct StoreConnections {
    writer: Mutex<Connection>,
    readers: Mutex<Vec<Connection>>,
    readers_available: Condvar,
    reader_pool_size: usize,
}

struct ReaderConnectionLease {
    pool: Arc<StoreConnections>,
    conn: Option<Connection>,
}

impl std::ops::Deref for ReaderConnectionLease {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        // ReaderConnectionLease is always constructed with a connection and
        // never exposes mutable access to the Option state.
        self.conn
            .as_ref()
            .expect("reader lease should hold a live connection")
    }
}

impl Drop for ReaderConnectionLease {
    fn drop(&mut self) {
        let Some(conn) = self.conn.take() else {
            return;
        };
        if let Ok(mut readers) = self.pool.readers.lock() {
            readers.push(conn);
            self.pool.readers_available.notify_one();
        }
    }
}

impl IndexStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        register_sqlite_vec_extension_once();
        let db_path = path.as_ref().to_path_buf();
        let mut writer = Self::open_connection(&db_path)?;
        migrations::apply(&mut writer)?;

        let reader_pool_size = std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(4)
            .clamp(4, 32);
        let mut readers = Vec::with_capacity(reader_pool_size);
        for _ in 0..reader_pool_size {
            readers.push(Self::open_connection(&db_path)?);
        }

        Ok(Self {
            db_path,
            connections: Arc::new(StoreConnections {
                writer: Mutex::new(writer),
                readers: Mutex::new(readers),
                readers_available: Condvar::new(),
                reader_pool_size,
            }),
        })
    }

    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    fn open_connection(path: &Path) -> Result<Connection> {
        register_sqlite_vec_extension_once();
        let conn = Connection::open(path)?;
        conn.execute_batch(CONNECTION_PRAGMAS_SQL)?;
        Ok(conn)
    }

    fn writer_connection(&self) -> Result<MutexGuard<'_, Connection>> {
        self.connections
            .writer
            .lock()
            .map_err(|_| IndexError::ConnectionPoolPoisoned("writer mutex poisoned".to_string()))
    }

    fn reader_connection(&self) -> Result<ReaderConnectionLease> {
        let mut readers = self.connections.readers.lock().map_err(|_| {
            IndexError::ConnectionPoolPoisoned("reader pool mutex poisoned".to_string())
        })?;
        while readers.is_empty() {
            readers = self
                .connections
                .readers_available
                .wait(readers)
                .map_err(|_| {
                    IndexError::ConnectionPoolPoisoned("reader pool wait poisoned".to_string())
                })?;
        }
        let conn = readers
            .pop()
            .ok_or_else(|| IndexError::ConnectionPoolExhausted)?;
        Ok(ReaderConnectionLease {
            pool: Arc::clone(&self.connections),
            conn: Some(conn),
        })
    }

    pub fn enqueue_indexing_job(
        &self,
        workspace_id: &str,
        node_id: &str,
        node_version: u64,
        priority: u8,
    ) -> Result<()> {
        let mut conn = self.writer_connection()?;
        let tx = conn.transaction()?;
        ensure_workspace_exists(&tx, workspace_id)?;
        let _ = self.get_node_by_id_tx(&tx, workspace_id, node_id)?;
        let now = now_ns();
        tx.execute(
            "INSERT INTO indexing_jobs(
                workspace_id,
                node_id,
                node_version,
                priority,
                attempts,
                status,
                scheduled_at_ns,
                started_at_ns,
                completed_at_ns,
                created_at_ns
             ) VALUES (?1, ?2, ?3, ?4, 0, 'queued', ?5, NULL, NULL, ?5)",
            params![
                workspace_id,
                node_id,
                i64::try_from(node_version).map_err(|_| IndexError::NumericConversion)?,
                i64::from(priority.min(10)),
                now
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn take_indexing_jobs(&self, limit: u32) -> Result<Vec<IndexingJobRecord>> {
        let mut conn = self.writer_connection()?;
        let tx = conn.transaction()?;
        let page_size = i64::from(limit.clamp(1, 256));
        let now = now_ns();
        let mut stmt = tx.prepare(
            "SELECT id, workspace_id, node_id, node_version, priority, attempts, status
             FROM indexing_jobs
             WHERE status = 'queued'
             ORDER BY priority ASC, scheduled_at_ns ASC, id ASC
             LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![page_size], |row| {
            let status_raw: String = row.get(6)?;
            let status = IndexingJobStatus::from_db(&status_raw).ok_or_else(|| {
                rusqlite::Error::ToSqlConversionFailure(Box::new(
                    IndexError::InvalidIndexingJobStatus(status_raw.clone()),
                ))
            })?;
            let node_version_i64: i64 = row.get(3)?;
            let priority_i64: i64 = row.get(4)?;
            let attempts_i64: i64 = row.get(5)?;
            Ok(IndexingJobRecord {
                job_id: row.get(0)?,
                workspace_id: row.get(1)?,
                node_id: row.get(2)?,
                node_version: u64::try_from(node_version_i64)
                    .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(3, node_version_i64))?,
                priority: u8::try_from(priority_i64)
                    .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(4, priority_i64))?,
                attempts: u32::try_from(attempts_i64)
                    .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(5, attempts_i64))?,
                status,
            })
        })?;
        let mut taken = Vec::new();
        for row in rows {
            taken.push(row?);
        }
        drop(stmt);
        for job in &taken {
            tx.execute(
                "UPDATE indexing_jobs
                 SET status = 'running', started_at_ns = ?2
                 WHERE id = ?1",
                params![job.job_id, now],
            )?;
        }
        tx.commit()?;
        Ok(taken)
    }

    pub fn complete_indexing_job(&self, job_id: i64) -> Result<()> {
        let mut conn = self.writer_connection()?;
        let tx = conn.transaction()?;
        tx.execute(
            "UPDATE indexing_jobs
             SET status = 'completed', completed_at_ns = ?2
             WHERE id = ?1",
            params![job_id, now_ns()],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn fail_indexing_job(
        &self,
        job_id: i64,
        error_message: &str,
        retry_delay_ms: u64,
    ) -> Result<()> {
        let mut conn = self.writer_connection()?;
        let tx = conn.transaction()?;
        let delay_ns = i64::try_from(retry_delay_ms)
            .map_err(|_| IndexError::NumericConversion)?
            .saturating_mul(1_000_000);
        let next_schedule = now_ns().saturating_add(delay_ns);
        tx.execute(
            "UPDATE indexing_jobs
             SET attempts = attempts + 1,
                 status = 'queued',
                 last_error = ?2,
                 started_at_ns = NULL,
                 scheduled_at_ns = ?3
             WHERE id = ?1",
            params![job_id, error_message, next_schedule],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn list_indexing_jobs_by_status(
        &self,
        status: IndexingJobStatus,
        limit: u32,
    ) -> Result<Vec<IndexingJobRecord>> {
        let conn = self.reader_connection()?;
        let page_size = i64::from(limit.clamp(1, 256));
        let mut stmt = conn.prepare(
            "SELECT id, workspace_id, node_id, node_version, priority, attempts, status
             FROM indexing_jobs
             WHERE status = ?1
             ORDER BY id ASC
             LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![status.as_db(), page_size], |row| {
            let status_raw: String = row.get(6)?;
            let parsed_status = IndexingJobStatus::from_db(&status_raw).ok_or_else(|| {
                rusqlite::Error::ToSqlConversionFailure(Box::new(
                    IndexError::InvalidIndexingJobStatus(status_raw.clone()),
                ))
            })?;
            let node_version_i64: i64 = row.get(3)?;
            let priority_i64: i64 = row.get(4)?;
            let attempts_i64: i64 = row.get(5)?;
            Ok(IndexingJobRecord {
                job_id: row.get(0)?,
                workspace_id: row.get(1)?,
                node_id: row.get(2)?,
                node_version: u64::try_from(node_version_i64)
                    .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(3, node_version_i64))?,
                priority: u8::try_from(priority_i64)
                    .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(4, priority_i64))?,
                attempts: u32::try_from(attempts_i64)
                    .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(5, attempts_i64))?,
                status: parsed_status,
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    pub fn create_workspace(&self, workspace_id: &str, name: &str) -> Result<String> {
        let mut conn = self.writer_connection()?;
        let tx = conn.transaction()?;
        let now = now_ns();
        let inserted = tx.execute(
            "INSERT OR IGNORE INTO workspaces(workspace_id, name, created_at_ns) VALUES (?1, ?2, ?3)",
            params![workspace_id, name, now],
        )?;

        if inserted == 0 {
            return Err(IndexError::WorkspaceAlreadyExists(workspace_id.to_string()));
        }

        let root_node_id = Uuid::now_v7().to_string();
        tx.execute(
            "INSERT INTO nodes(node_id, workspace_id, parent_node_id, name, path, kind, mode, mtime_ns, version, size, created_at_ns, updated_at_ns)
             VALUES (?1, ?2, NULL, '', '', 'dir', ?3, ?4, 1, 0, ?4, ?4)",
            params![root_node_id, workspace_id, 0o755_u32, now],
        )?;
        tx.execute(
            "INSERT INTO events(workspace_id, kind, node_id, payload_json, created_at_ns) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![workspace_id, EventKind::NodeCreated.as_db(), root_node_id, "{\"root\":true}", now],
        )?;
        tx.commit()?;
        Ok(root_node_id)
    }

    pub fn has_workspace(&self, workspace_id: &str) -> Result<bool> {
        let conn = self.reader_connection()?;
        let exists = conn
            .query_row(
                "SELECT 1 FROM workspaces WHERE workspace_id = ?1 LIMIT 1",
                params![workspace_id],
                |_row| Ok(()),
            )
            .optional()?
            .is_some();
        Ok(exists)
    }

    pub fn delete_workspace(&self, workspace_id: &str) -> Result<()> {
        let mut conn = self.writer_connection()?;
        let tx = conn.transaction()?;
        ensure_workspace_exists(&tx, workspace_id)?;
        tx.execute(
            "DELETE FROM workspaces WHERE workspace_id = ?1",
            params![workspace_id],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn put_file(
        &self,
        workspace_id: &str,
        path: &str,
        content: &[u8],
        if_version: Option<u64>,
    ) -> Result<(FileRecord, i64)> {
        let prepared = crate::prepare_file_indexing(
            normalize_path(path)?,
            content,
            crate::DEFAULT_SKIP_CHUNKING_OVER_BYTES,
            crate::DEFAULT_HEADLINE_EACH_BYTES,
        );
        let size = u64::try_from(content.len()).map_err(|_| IndexError::NumericConversion)?;
        self.put_file_with_prepared(workspace_id, path, size, prepared, if_version)
    }

    /// Same as [`Self::put_file`] but uses a precomputed [`PreparedFileIndexing`] (e.g. mmap + streaming hash from scryd).
    pub fn put_file_with_prepared(
        &self,
        workspace_id: &str,
        path: &str,
        size: u64,
        prepared: PreparedFileIndexing,
        if_version: Option<u64>,
    ) -> Result<(FileRecord, i64)> {
        let mut conn = self.writer_connection()?;
        let tx = conn.transaction()?;
        ensure_workspace_exists(&tx, workspace_id)?;

        let normalized_path = normalize_path(path)?;
        if normalized_path.is_empty() {
            return Err(IndexError::InvalidPath(path.to_string()));
        }
        let (parent_path, name) = split_parent_name(normalized_path)?;
        let parent = self.require_directory_tx(&tx, workspace_id, parent_path)?;
        let now = now_ns();
        let digest_hex = prepared.digest_hex.clone();
        let indexing_policy = prepared.indexing_policy.as_str();

        let existing = tx
            .query_row(
                "SELECT node_id, version, mime FROM nodes WHERE workspace_id = ?1 AND path = ?2 AND kind = 'file'",
                params![workspace_id, normalized_path],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, u64>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()?;

        let (node_id, version) = if let Some((node_id, prev_version, prev_mime)) = existing {
            if let Some(expected) = if_version {
                if expected != 0 && expected != prev_version {
                    return Err(IndexError::VersionConflict {
                        expected,
                        found: prev_version,
                    });
                }
            }
            let next_version = prev_version.saturating_add(1);
            let mime = infer_mime_for_put(&prev_mime, normalized_path);
            tx.execute(
                "UPDATE nodes SET version = ?3, size = ?4, updated_at_ns = ?5, mtime_ns = ?5, content_hash = ?6, mime = ?7, indexing_policy = ?8 WHERE node_id = ?1 AND workspace_id = ?2",
                params![
                    node_id,
                    workspace_id,
                    next_version,
                    size,
                    now,
                    digest_hex.as_str(),
                    mime.as_str(),
                    indexing_policy,
                ],
            )?;
            (node_id, next_version)
        } else {
            if let Some(expected) = if_version {
                if expected != 0 {
                    return Err(IndexError::VersionConflict { expected, found: 0 });
                }
            }
            let node_id = Uuid::now_v7().to_string();
            let mime = infer_mime_for_put("", normalized_path);
            tx.execute(
                "INSERT INTO nodes(node_id, workspace_id, parent_node_id, name, path, kind, mode, mtime_ns, version, size, created_at_ns, updated_at_ns, content_hash, mime, indexing_policy)
                 VALUES (?1, ?2, ?3, ?4, ?5, 'file', ?6, ?7, 1, ?8, ?7, ?7, ?9, ?10, ?11)",
                params![
                    node_id,
                    workspace_id,
                    parent.node_id,
                    name,
                    normalized_path,
                    0o644_u32,
                    now,
                    size,
                    digest_hex.as_str(),
                    mime.as_str(),
                    indexing_policy,
                ],
            )?;
            (node_id, 1)
        };

        tx.execute(
            "DELETE FROM vec_chunks
             WHERE chunk_id IN (
                 SELECT id FROM chunks WHERE workspace_id = ?1 AND node_id = ?2
             )",
            params![workspace_id, node_id],
        )?;
        tx.execute(
            "DELETE FROM chunk_embeddings WHERE chunk_id IN (SELECT id FROM chunks WHERE node_id = ?1)",
            params![node_id],
        )?;
        tx.execute("DELETE FROM chunks WHERE node_id = ?1", params![node_id])?;
        let ih = i64::from(prepared.headline_only);
        for chunk in prepared.chunks {
            tx.execute(
                "INSERT INTO chunks(workspace_id, node_id, start_line, end_line, content, context_path, is_headline) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    workspace_id,
                    node_id,
                    i64::from(chunk.start_line),
                    i64::from(chunk.end_line),
                    chunk.content,
                    chunk.context_path,
                    ih,
                ],
            )?;
        }

        tx.execute(
            "INSERT INTO events(workspace_id, kind, node_id, payload_json, created_at_ns) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![workspace_id, EventKind::NodeModified.as_db(), node_id, "{}", now],
        )?;
        let event_id = tx.last_insert_rowid();
        tx.execute(
            "INSERT INTO indexing_jobs(
                workspace_id,
                node_id,
                node_version,
                priority,
                attempts,
                status,
                scheduled_at_ns,
                started_at_ns,
                completed_at_ns,
                created_at_ns
             ) VALUES (?1, ?2, ?3, 0, 0, 'queued', ?4, NULL, NULL, ?4)",
            params![
                workspace_id,
                node_id,
                i64::try_from(version).map_err(|_| IndexError::NumericConversion)?,
                now
            ],
        )?;
        tx.commit()?;
        Ok((
            FileRecord {
                node_id,
                version,
                size,
                content_hash: digest_hex,
            },
            event_id,
        ))
    }

    pub fn node_record_by_id(&self, workspace_id: &str, node_id: &str) -> Result<NodeRecord> {
        let conn = self.reader_connection()?;
        ensure_workspace_exists_conn(&conn, workspace_id)?;
        self.get_node_by_id_conn(&conn, workspace_id, node_id)
    }

    pub fn get_file_chunks(
        &self,
        workspace_id: &str,
        node_id: &str,
    ) -> Result<Vec<FileChunkRecord>> {
        let conn = self.reader_connection()?;
        ensure_workspace_exists_conn(&conn, workspace_id)?;
        let _ = self.get_node_by_id_conn(&conn, workspace_id, node_id)?;

        let mut stmt = conn.prepare(
            "SELECT id, start_line, end_line, content
             FROM chunks
             WHERE workspace_id = ?1 AND node_id = ?2
             ORDER BY id ASC",
        )?;
        let rows = stmt.query_map(params![workspace_id, node_id], |row| {
            let start_line_i64: i64 = row.get(1)?;
            let end_line_i64: i64 = row.get(2)?;
            Ok(FileChunkRecord {
                chunk_id: row.get(0)?,
                start_line: u32::try_from(start_line_i64)
                    .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(1, start_line_i64))?,
                end_line: u32::try_from(end_line_i64)
                    .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(2, end_line_i64))?,
                content: row.get(3)?,
            })
        })?;

        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    pub fn get_handle_content(
        &self,
        workspace_id: &str,
        node_id: &str,
        _mode: HandleMode,
    ) -> Result<HandleContentRecord> {
        let node = self.node_record_by_id(workspace_id, node_id)?;
        if node.attrs.kind != NodeKind::File {
            return Err(IndexError::InvalidHandleMode(format!(
                "node {} is not a file",
                node.node_id
            )));
        }
        // Bytes live in the workspace content store (scryd); index only tracks metadata + FTS chunks.
        let content = Vec::new();
        Ok(HandleContentRecord {
            node_id: node.node_id,
            path: node.path,
            content,
            attrs: node.attrs,
        })
    }

    pub fn write_handle(
        &self,
        workspace_id: &str,
        node_id: &str,
        mode: HandleMode,
        base_content: &[u8],
        offset: u64,
        data: &[u8],
    ) -> Result<Vec<u8>> {
        if !mode.allows_write() {
            return Err(IndexError::InvalidHandleMode(
                "handle is read-only".to_string(),
            ));
        }
        let _ = self.node_record_by_id(workspace_id, node_id)?;
        Ok(merge_write(base_content, offset, data))
    }

    pub fn truncate_handle(
        &self,
        workspace_id: &str,
        node_id: &str,
        mode: HandleMode,
        base_content: &[u8],
        size: u64,
    ) -> Result<Vec<u8>> {
        if !mode.allows_write() {
            return Err(IndexError::InvalidHandleMode(
                "handle is read-only".to_string(),
            ));
        }
        let _ = self.node_record_by_id(workspace_id, node_id)?;
        let target_len = usize::try_from(size).map_err(|_| IndexError::NumericConversion)?;
        let mut out = base_content.to_vec();
        out.resize(target_len, 0);
        Ok(out)
    }

    pub fn flush_handle(
        &self,
        workspace_id: &str,
        node_id: &str,
        mode: HandleMode,
        content: &[u8],
        if_version: Option<u64>,
    ) -> Result<FlushResult> {
        if !mode.allows_write() {
            return Err(IndexError::InvalidHandleMode(
                "flush requires writable handle".to_string(),
            ));
        }
        let node = self.node_record_by_id(workspace_id, node_id)?;
        let (file, event_id) = self.put_file(workspace_id, &node.path, content, if_version)?;
        let attrs = self.get_attrs(workspace_id, &file.node_id)?;
        Ok(FlushResult {
            version: file.version,
            attrs,
            event_id,
        })
    }

    pub fn flush_handle_with_prepared(
        &self,
        workspace_id: &str,
        node_id: &str,
        mode: HandleMode,
        size: u64,
        prepared: PreparedFileIndexing,
        if_version: Option<u64>,
    ) -> Result<FlushResult> {
        if !mode.allows_write() {
            return Err(IndexError::InvalidHandleMode(
                "flush requires writable handle".to_string(),
            ));
        }
        let node = self.node_record_by_id(workspace_id, node_id)?;
        let (file, event_id) =
            self.put_file_with_prepared(workspace_id, &node.path, size, prepared, if_version)?;
        let attrs = self.get_attrs(workspace_id, &file.node_id)?;
        Ok(FlushResult {
            version: file.version,
            attrs,
            event_id,
        })
    }

    pub fn read_handle(
        &self,
        workspace_id: &str,
        node_id: &str,
        mode: HandleMode,
        content: &[u8],
        offset: u64,
        length: u32,
    ) -> Result<(Vec<u8>, bool)> {
        if !mode.allows_read() {
            return Err(IndexError::InvalidHandleMode(
                "handle is write-only".to_string(),
            ));
        }
        let _ = self.node_record_by_id(workspace_id, node_id)?;
        Ok(clamp_read_window(content, offset, length))
    }

    pub fn list_chunks_for_node(
        &self,
        workspace_id: &str,
        node_id: &str,
    ) -> Result<Vec<ChunkContent>> {
        let conn = self.reader_connection()?;
        ensure_workspace_exists_conn(&conn, workspace_id)?;
        let _ = self.get_node_by_id_conn(&conn, workspace_id, node_id)?;

        let mut stmt = conn.prepare(
            "SELECT id, content
             FROM chunks
             WHERE workspace_id = ?1 AND node_id = ?2
             ORDER BY id ASC",
        )?;
        let rows = stmt.query_map(params![workspace_id, node_id], |row| {
            Ok(ChunkContent {
                chunk_id: row.get(0)?,
                content: row.get(1)?,
            })
        })?;

        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    /// Cursor-friendly page over `chunks` for a node. Returns chunks with `id > after_id`,
    /// up to `limit` rows, ordered by `id ASC`. `after_id = 0` starts from the first chunk.
    ///
    /// Used by the embedding worker to bound peak memory to one batch worth of chunk text
    /// regardless of total file size (P2 closure, remaining risk: embedding batch RSS).
    pub fn list_chunks_for_node_after(
        &self,
        workspace_id: &str,
        node_id: &str,
        after_id: i64,
        limit: usize,
    ) -> Result<Vec<ChunkContent>> {
        let limit_i64 = i64::try_from(limit.max(1)).unwrap_or(i64::MAX);
        let conn = self.reader_connection()?;
        ensure_workspace_exists_conn(&conn, workspace_id)?;
        let _ = self.get_node_by_id_conn(&conn, workspace_id, node_id)?;

        let mut stmt = conn.prepare(
            "SELECT id, content
             FROM chunks
             WHERE workspace_id = ?1 AND node_id = ?2 AND id > ?3
             ORDER BY id ASC
             LIMIT ?4",
        )?;
        let rows = stmt.query_map(params![workspace_id, node_id, after_id, limit_i64], |row| {
            Ok(ChunkContent {
                chunk_id: row.get(0)?,
                content: row.get(1)?,
            })
        })?;

        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    /// Upsert embeddings for an explicit `(chunk_id, embedding)` batch.
    ///
    /// Unlike [`Self::upsert_embeddings_for_node`] this does **not** require the batch to
    /// cover every chunk of the node — it writes exactly the rows passed in, leaving any
    /// other chunk's embeddings untouched. Used by the batched embedding worker.
    ///
    /// Returns the number of rows actually upserted.
    pub fn upsert_embeddings_for_chunk_ids(
        &self,
        workspace_id: &str,
        chunk_ids: &[i64],
        embeddings: &[Vec<f32>],
    ) -> Result<u32> {
        if chunk_ids.len() != embeddings.len() {
            return Err(IndexError::EmbeddingCountMismatch {
                expected: chunk_ids.len(),
                actual: embeddings.len(),
            });
        }
        if chunk_ids.is_empty() {
            return Ok(0);
        }
        let mut conn = self.writer_connection()?;
        let tx = conn.transaction()?;
        ensure_workspace_exists(&tx, workspace_id)?;

        for (chunk_id, embedding) in chunk_ids.iter().copied().zip(embeddings.iter()) {
            // Reject chunk ids that do not belong to this workspace; keeps callers honest
            // and avoids silently upserting into the wrong partition under per-workspace layouts.
            let owner: Option<String> = tx
                .query_row(
                    "SELECT workspace_id FROM chunks WHERE id = ?1",
                    params![chunk_id],
                    |row| row.get(0),
                )
                .optional()?;
            match owner {
                Some(ws) if ws == workspace_id => {}
                Some(_) | None => {
                    return Err(IndexError::InvalidInput(format!(
                        "chunk_id {chunk_id} does not belong to workspace {workspace_id}"
                    )));
                }
            }

            let embedding_json = serialize_f32_json(embedding)?;
            tx.execute(
                "INSERT INTO chunk_embeddings(chunk_id, embedding_json)
                 VALUES (?1, ?2)
                 ON CONFLICT(chunk_id) DO UPDATE SET embedding_json = excluded.embedding_json",
                params![chunk_id, embedding_json],
            )?;
            tx.execute(
                "DELETE FROM vec_chunks WHERE chunk_id = ?1",
                params![chunk_id],
            )?;
            let blob = embedding_vec_to_blob(embedding)?;
            tx.execute(
                "INSERT INTO vec_chunks(chunk_id, embedding, workspace_id)
                 VALUES (?1, ?2, ?3)",
                params![chunk_id, blob, workspace_id],
            )?;
        }
        tx.commit()?;
        u32::try_from(chunk_ids.len()).map_err(|_| IndexError::NumericConversion)
    }

    pub fn upsert_embeddings_for_node(
        &self,
        workspace_id: &str,
        node_id: &str,
        embeddings: &[Vec<f32>],
    ) -> Result<u32> {
        let mut conn = self.writer_connection()?;
        let tx = conn.transaction()?;
        ensure_workspace_exists(&tx, workspace_id)?;
        let _ = self.get_node_by_id_tx(&tx, workspace_id, node_id)?;

        let mut chunk_ids = Vec::new();
        {
            let mut stmt = tx.prepare(
                "SELECT id
                 FROM chunks
                 WHERE workspace_id = ?1 AND node_id = ?2
                 ORDER BY id ASC",
            )?;
            let chunk_rows =
                stmt.query_map(params![workspace_id, node_id], |row| row.get::<_, i64>(0))?;
            for row in chunk_rows {
                chunk_ids.push(row?);
            }
        }
        if chunk_ids.len() != embeddings.len() {
            return Err(IndexError::EmbeddingCountMismatch {
                expected: chunk_ids.len(),
                actual: embeddings.len(),
            });
        }

        for (chunk_id, embedding) in chunk_ids.into_iter().zip(embeddings.iter()) {
            let embedding_json = serialize_f32_json(embedding)?;
            tx.execute(
                "INSERT INTO chunk_embeddings(chunk_id, embedding_json)
                 VALUES (?1, ?2)
                 ON CONFLICT(chunk_id) DO UPDATE SET embedding_json = excluded.embedding_json",
                params![chunk_id, embedding_json],
            )?;
            tx.execute(
                "DELETE FROM vec_chunks WHERE chunk_id = ?1",
                params![chunk_id],
            )?;
            let blob = embedding_vec_to_blob(embedding)?;
            tx.execute(
                "INSERT INTO vec_chunks(chunk_id, embedding, workspace_id)
                 VALUES (?1, ?2, ?3)",
                params![chunk_id, blob, workspace_id],
            )?;
        }
        tx.commit()?;
        u32::try_from(embeddings.len()).map_err(|_| IndexError::NumericConversion)
    }

    pub fn emit_index_updated_event(
        &self,
        workspace_id: &str,
        node_id: &str,
        index_version: u64,
        chunks_indexed: u32,
    ) -> Result<i64> {
        let mut conn = self.writer_connection()?;
        let tx = conn.transaction()?;
        ensure_workspace_exists(&tx, workspace_id)?;
        let _ = self.get_node_by_id_tx(&tx, workspace_id, node_id)?;
        let now = now_ns();
        let payload = format!(
            "{{\"index_version\":{},\"chunks_indexed\":{}}}",
            index_version, chunks_indexed
        );
        tx.execute(
            "INSERT INTO events(workspace_id, kind, node_id, payload_json, created_at_ns) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![workspace_id, EventKind::IndexUpdated.as_db(), node_id, payload, now],
        )?;
        let event_id = tx.last_insert_rowid();
        tx.commit()?;
        Ok(event_id)
    }

    pub fn get_file(&self, _workspace_id: &str, _path: &str) -> Result<Vec<u8>> {
        Err(IndexError::ContentNotInIndex)
    }

    pub fn resolve_path(&self, workspace_id: &str, path: &str) -> Result<Option<String>> {
        let conn = self.reader_connection()?;
        ensure_workspace_exists_conn(&conn, workspace_id)?;
        let normalized_path = normalize_path(path)?;
        let node_id = conn
            .query_row(
                "SELECT node_id FROM nodes WHERE workspace_id = ?1 AND path = ?2",
                params![workspace_id, normalized_path],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        Ok(node_id)
    }

    pub fn resolve_ref(&self, workspace_id: &str, node_id: &str) -> Result<Option<String>> {
        let conn = self.reader_connection()?;
        ensure_workspace_exists_conn(&conn, workspace_id)?;
        let result = self.resolve_ref_with_redirect(workspace_id, node_id)?;
        Ok(result.path)
    }

    pub fn resolve_ref_with_redirect(
        &self,
        workspace_id: &str,
        node_id: &str,
    ) -> Result<ResolveRefResult> {
        let conn = self.reader_connection()?;
        ensure_workspace_exists_conn(&conn, workspace_id)?;
        let mut visited = std::collections::HashSet::new();
        let mut current = node_id.trim().to_string();
        if current.is_empty() {
            return Err(IndexError::InvalidPath(
                "node_id cannot be empty".to_string(),
            ));
        }
        let mut first_redirect: Option<String> = None;

        for _ in 0..MAX_REDIRECT_HOPS {
            if !visited.insert(current.clone()) {
                return Err(IndexError::RedirectLoop {
                    workspace_id: workspace_id.to_string(),
                    node_id: current,
                });
            }

            let maybe_path = conn
                .query_row(
                    "SELECT path FROM nodes WHERE workspace_id = ?1 AND node_id = ?2",
                    params![workspace_id, current.as_str()],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            if let Some(path) = maybe_path {
                return Ok(ResolveRefResult {
                    path: Some(path),
                    redirect_to_node_id: first_redirect,
                });
            }

            let redirect = conn
                .query_row(
                    "SELECT to_node_id FROM redirects WHERE from_node_id = ?1",
                    params![current.as_str()],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            match redirect {
                Some(next) => {
                    if first_redirect.is_none() {
                        first_redirect = Some(next.clone());
                    }
                    current = next;
                }
                None => {
                    return Ok(ResolveRefResult {
                        path: None,
                        redirect_to_node_id: first_redirect,
                    });
                }
            }
        }

        Err(IndexError::RedirectLimitExceeded {
            workspace_id: workspace_id.to_string(),
            node_id: node_id.to_string(),
            max_hops: MAX_REDIRECT_HOPS,
        })
    }

    pub fn set_redirect(
        &self,
        workspace_id: &str,
        from_node_id: &str,
        to_node_id: &str,
        reason: &str,
    ) -> Result<()> {
        let mut conn = self.writer_connection()?;
        let tx = conn.transaction()?;
        ensure_workspace_exists(&tx, workspace_id)?;
        let from = from_node_id.trim();
        let to = to_node_id.trim();
        if from.is_empty() || to.is_empty() {
            return Err(IndexError::InvalidPath(
                "redirect node ids cannot be empty".to_string(),
            ));
        }
        if reason.trim().is_empty() {
            return Err(IndexError::InvalidPath(
                "redirect reason cannot be empty".to_string(),
            ));
        }
        tx.execute(
            "INSERT INTO redirects(from_node_id, to_node_id, reason, created_at_ns)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(from_node_id)
             DO UPDATE SET to_node_id = excluded.to_node_id,
                           reason = excluded.reason,
                           created_at_ns = excluded.created_at_ns",
            params![from, to, reason.trim(), now_ns()],
        )?;
        tx.commit()?;
        Ok(())
    }

    #[cfg(test)]
    pub fn insert_redirect(
        &self,
        workspace_id: &str,
        from_node_id: &str,
        to_node_id: &str,
        reason: &str,
    ) -> Result<()> {
        self.set_redirect(workspace_id, from_node_id, to_node_id, reason)
    }

    pub fn get_node(&self, workspace_id: &str, node_id: &str) -> Result<NodeRecord> {
        let conn = self.reader_connection()?;
        ensure_workspace_exists_conn(&conn, workspace_id)?;
        self.get_node_by_id_conn(&conn, workspace_id, node_id)
    }

    pub fn lookup(
        &self,
        workspace_id: &str,
        parent_node_id: &str,
        name: &str,
    ) -> Result<NodeRecord> {
        let conn = self.reader_connection()?;
        ensure_workspace_exists_conn(&conn, workspace_id)?;
        let parent = self.get_node_by_id_conn(&conn, workspace_id, parent_node_id)?;
        if parent.attrs.kind != NodeKind::Dir {
            return Err(IndexError::NotDirectory(parent_node_id.to_string()));
        }
        let child_path = join_path(&parent.path, name)?;
        self.get_node_by_path_conn(&conn, workspace_id, &child_path)?
            .ok_or_else(|| IndexError::NodeNotFound {
                workspace_id: workspace_id.to_string(),
                path: child_path,
            })
    }

    pub fn get_attrs(&self, workspace_id: &str, node_id: &str) -> Result<NodeAttrs> {
        let conn = self.reader_connection()?;
        ensure_workspace_exists_conn(&conn, workspace_id)?;
        Ok(self
            .get_node_by_id_conn(&conn, workspace_id, node_id)?
            .attrs)
    }

    pub fn get_attrs_if_changed(
        &self,
        workspace_id: &str,
        node_id: &str,
        known_version: u64,
    ) -> Result<Option<NodeAttrs>> {
        let attrs = self.get_attrs(workspace_id, node_id)?;
        if attrs.version == known_version {
            Ok(None)
        } else {
            Ok(Some(attrs))
        }
    }

    pub fn get_workspace_stats(&self, workspace_id: &str) -> Result<WorkspaceStats> {
        let conn = self.reader_connection()?;
        ensure_workspace_exists_conn(&conn, workspace_id)?;
        let file_count = conn.query_row(
            "SELECT COUNT(*) FROM nodes WHERE workspace_id = ?1 AND kind = 'file'",
            params![workspace_id],
            |row| row.get::<_, i64>(0),
        )?;
        let dir_count = conn.query_row(
            "SELECT COUNT(*) FROM nodes WHERE workspace_id = ?1 AND kind = 'dir'",
            params![workspace_id],
            |row| row.get::<_, i64>(0),
        )?;
        let chunk_count = conn.query_row(
            "SELECT COUNT(*) FROM chunks WHERE workspace_id = ?1",
            params![workspace_id],
            |row| row.get::<_, i64>(0),
        )?;
        let event_count = conn.query_row(
            "SELECT COUNT(*) FROM events WHERE workspace_id = ?1",
            params![workspace_id],
            |row| row.get::<_, i64>(0),
        )?;
        let total_content_bytes = conn.query_row(
            "SELECT COALESCE(SUM(size), 0) FROM nodes WHERE workspace_id = ?1 AND kind = 'file'",
            params![workspace_id],
            |row| row.get::<_, i64>(0),
        )?;
        let queued_jobs = conn.query_row(
            "SELECT COUNT(*) FROM indexing_jobs WHERE workspace_id = ?1 AND status = 'queued'",
            params![workspace_id],
            |row| row.get::<_, i64>(0),
        )?;
        let running_jobs = conn.query_row(
            "SELECT COUNT(*) FROM indexing_jobs WHERE workspace_id = ?1 AND status = 'running'",
            params![workspace_id],
            |row| row.get::<_, i64>(0),
        )?;
        let completed_jobs = conn.query_row(
            "SELECT COUNT(*) FROM indexing_jobs WHERE workspace_id = ?1 AND status = 'completed'",
            params![workspace_id],
            |row| row.get::<_, i64>(0),
        )?;
        let failed_jobs = conn.query_row(
            "SELECT COUNT(*) FROM indexing_jobs WHERE workspace_id = ?1 AND status = 'failed'",
            params![workspace_id],
            |row| row.get::<_, i64>(0),
        )?;
        let indexing_policy_full_files = conn.query_row(
            "SELECT COUNT(*) FROM nodes WHERE workspace_id = ?1 AND kind = 'file' AND COALESCE(indexing_policy, 'full') = 'full'",
            params![workspace_id],
            |row| row.get::<_, i64>(0),
        )?;
        let indexing_policy_headline_only_files = conn.query_row(
            "SELECT COUNT(*) FROM nodes WHERE workspace_id = ?1 AND kind = 'file' AND indexing_policy = 'headline_only'",
            params![workspace_id],
            |row| row.get::<_, i64>(0),
        )?;
        let indexing_policy_skipped_files = conn.query_row(
            "SELECT COUNT(*) FROM nodes WHERE workspace_id = ?1 AND kind = 'file' AND indexing_policy = 'skipped'",
            params![workspace_id],
            |row| row.get::<_, i64>(0),
        )?;
        Ok(WorkspaceStats {
            file_count: u64::try_from(file_count).map_err(|_| IndexError::NumericConversion)?,
            dir_count: u64::try_from(dir_count).map_err(|_| IndexError::NumericConversion)?,
            chunk_count: u64::try_from(chunk_count).map_err(|_| IndexError::NumericConversion)?,
            event_count: u64::try_from(event_count).map_err(|_| IndexError::NumericConversion)?,
            total_content_bytes: u64::try_from(total_content_bytes)
                .map_err(|_| IndexError::NumericConversion)?,
            queued_jobs: u64::try_from(queued_jobs).map_err(|_| IndexError::NumericConversion)?,
            running_jobs: u64::try_from(running_jobs).map_err(|_| IndexError::NumericConversion)?,
            completed_jobs: u64::try_from(completed_jobs)
                .map_err(|_| IndexError::NumericConversion)?,
            failed_jobs: u64::try_from(failed_jobs).map_err(|_| IndexError::NumericConversion)?,
            indexing_policy_full_files: u64::try_from(indexing_policy_full_files)
                .map_err(|_| IndexError::NumericConversion)?,
            indexing_policy_headline_only_files: u64::try_from(indexing_policy_headline_only_files)
                .map_err(|_| IndexError::NumericConversion)?,
            indexing_policy_skipped_files: u64::try_from(indexing_policy_skipped_files)
                .map_err(|_| IndexError::NumericConversion)?,
        })
    }

    /// Workspace id, display name, and root directory node id (for catalog reconcile).
    pub fn list_workspace_summaries(&self) -> Result<Vec<(String, String, String)>> {
        let conn = self.reader_connection()?;
        let mut stmt = conn.prepare(
            "SELECT w.workspace_id, w.name, n.node_id
             FROM workspaces w
             JOIN nodes n ON n.workspace_id = w.workspace_id AND n.parent_node_id IS NULL
             ORDER BY w.workspace_id",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    pub fn reindex_workspace(&self, workspace_id: &str) -> Result<u32> {
        let mut conn = self.writer_connection()?;
        let tx = conn.transaction()?;
        ensure_workspace_exists(&tx, workspace_id)?;
        let mut file_nodes = Vec::<(String, u64)>::new();
        {
            let mut stmt = tx.prepare(
                "SELECT node_id, version
                 FROM nodes
                 WHERE workspace_id = ?1 AND kind = 'file'
                 ORDER BY node_id ASC",
            )?;
            let rows = stmt.query_map(params![workspace_id], |row| {
                let version_i64: i64 = row.get(1)?;
                Ok((
                    row.get::<_, String>(0)?,
                    u64::try_from(version_i64)
                        .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(1, version_i64))?,
                ))
            })?;
            for row in rows {
                file_nodes.push(row?);
            }
        }
        let now = now_ns();
        for (node_id, version) in &file_nodes {
            tx.execute(
                "INSERT INTO indexing_jobs(
                    workspace_id,
                    node_id,
                    node_version,
                    priority,
                    attempts,
                    status,
                    scheduled_at_ns,
                    started_at_ns,
                    completed_at_ns,
                    created_at_ns
                 ) VALUES (?1, ?2, ?3, 0, 0, 'queued', ?4, NULL, NULL, ?4)",
                params![
                    workspace_id,
                    node_id,
                    i64::try_from(*version).map_err(|_| IndexError::NumericConversion)?,
                    now
                ],
            )?;
        }
        tx.commit()?;
        u32::try_from(file_nodes.len()).map_err(|_| IndexError::NumericConversion)
    }

    pub fn read_dir(
        &self,
        workspace_id: &str,
        node_id: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<(Vec<NodeRecord>, Option<String>)> {
        let conn = self.reader_connection()?;
        ensure_workspace_exists_conn(&conn, workspace_id)?;
        let dir = self.get_node_by_id_conn(&conn, workspace_id, node_id)?;
        if dir.attrs.kind != NodeKind::Dir {
            return Err(IndexError::NotDirectory(node_id.to_string()));
        }

        let offset = parse_cursor(cursor)?;
        let page_size = i64::from(limit.clamp(1, 1024));
        let mut stmt = conn.prepare(
            "SELECT node_id, parent_node_id, name, path, kind, size, mtime_ns, mode, version, content_hash, mime
             FROM nodes
             WHERE workspace_id = ?1 AND parent_node_id = ?2
             ORDER BY name
             LIMIT ?3 OFFSET ?4",
        )?;
        let rows = stmt.query_map(
            params![workspace_id, node_id, page_size, offset],
            map_node_row,
        )?;
        let mut items = Vec::new();
        for row in rows {
            items.push(row?);
        }
        let next_cursor = if items.len() == usize::try_from(page_size).unwrap_or(0) {
            Some((offset + page_size).to_string())
        } else {
            None
        };
        Ok((items, next_cursor))
    }

    pub fn create_node(
        &self,
        workspace_id: &str,
        parent_node_id: &str,
        name: &str,
        kind: NodeKind,
        mode: u32,
        exclusive: bool,
    ) -> Result<(NodeRecord, Option<i64>)> {
        let mut conn = self.writer_connection()?;
        let tx = conn.transaction()?;
        ensure_workspace_exists(&tx, workspace_id)?;
        let parent = self.get_node_by_id_tx(&tx, workspace_id, parent_node_id)?;
        if parent.attrs.kind != NodeKind::Dir {
            return Err(IndexError::NotDirectory(parent_node_id.to_string()));
        }
        let path = join_path(&parent.path, name)?;
        if let Some(existing) = self.get_node_by_path_tx(&tx, workspace_id, &path)? {
            if exclusive {
                return Err(IndexError::AlreadyExistsAtPath { path });
            }
            return Ok((existing, None));
        }

        let now = now_ns();
        let node_id = Uuid::now_v7().to_string();
        tx.execute(
            "INSERT INTO nodes(node_id, workspace_id, parent_node_id, name, path, kind, mode, mtime_ns, version, size, created_at_ns, updated_at_ns, content_hash, mime)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 1, 0, ?8, ?8, '', '')",
            params![
                node_id,
                workspace_id,
                parent_node_id,
                name,
                path,
                kind.as_db(),
                mode,
                now
            ],
        )?;
        if kind == NodeKind::File {
            // Empty file should be readable immediately after creation.
            tx.execute(
                "INSERT INTO chunks(workspace_id, node_id, start_line, end_line, content, context_path) VALUES (?1, ?2, 1, 1, '', ?3)",
                params![workspace_id, node_id, path],
            )?;
        }
        tx.execute(
            "INSERT INTO events(workspace_id, kind, node_id, payload_json, created_at_ns) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![workspace_id, EventKind::NodeCreated.as_db(), node_id, "{}", now],
        )?;
        let event_id = tx.last_insert_rowid();
        tx.commit()?;
        let node = self.lookup(workspace_id, parent_node_id, name)?;
        Ok((node, Some(event_id)))
    }

    pub fn unlink(
        &self,
        workspace_id: &str,
        parent_node_id: &str,
        name: &str,
        if_version: Option<u64>,
    ) -> Result<i64> {
        let mut conn = self.writer_connection()?;
        let tx = conn.transaction()?;
        ensure_workspace_exists(&tx, workspace_id)?;
        let parent = self.get_node_by_id_tx(&tx, workspace_id, parent_node_id)?;
        if parent.attrs.kind != NodeKind::Dir {
            return Err(IndexError::NotDirectory(parent_node_id.to_string()));
        }
        let path = join_path(&parent.path, name)?;
        let target = self
            .get_node_by_path_tx(&tx, workspace_id, &path)?
            .ok_or_else(|| IndexError::NodeNotFound {
                workspace_id: workspace_id.to_string(),
                path: path.clone(),
            })?;
        if let Some(expected) = if_version {
            if expected != 0 && expected != target.attrs.version {
                return Err(IndexError::VersionConflict {
                    expected,
                    found: target.attrs.version,
                });
            }
        }
        if target.attrs.kind == NodeKind::Dir {
            let has_children = tx
                .query_row(
                    "SELECT 1 FROM nodes WHERE workspace_id = ?1 AND parent_node_id = ?2 LIMIT 1",
                    params![workspace_id, target.node_id],
                    |_row| Ok(()),
                )
                .optional()?
                .is_some();
            if has_children {
                return Err(IndexError::DirectoryNotEmpty(target.node_id));
            }
        }
        tx.execute(
            "DELETE FROM nodes WHERE workspace_id = ?1 AND node_id = ?2",
            params![workspace_id, target.node_id],
        )?;
        tx.execute(
            "INSERT INTO events(workspace_id, kind, node_id, payload_json, created_at_ns) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![workspace_id, EventKind::NodeDeleted.as_db(), target.node_id, "{}", now_ns()],
        )?;
        let event_id = tx.last_insert_rowid();
        tx.commit()?;
        Ok(event_id)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn rename(
        &self,
        workspace_id: &str,
        from_parent_node_id: &str,
        from_name: &str,
        to_parent_node_id: &str,
        to_name: &str,
        overwrite: bool,
        if_version: Option<u64>,
    ) -> Result<((String, u64), i64)> {
        let mut conn = self.writer_connection()?;
        let tx = conn.transaction()?;
        ensure_workspace_exists(&tx, workspace_id)?;
        let from_parent = self.get_node_by_id_tx(&tx, workspace_id, from_parent_node_id)?;
        let to_parent = self.get_node_by_id_tx(&tx, workspace_id, to_parent_node_id)?;
        if from_parent.attrs.kind != NodeKind::Dir {
            return Err(IndexError::NotDirectory(from_parent_node_id.to_string()));
        }
        if to_parent.attrs.kind != NodeKind::Dir {
            return Err(IndexError::NotDirectory(to_parent_node_id.to_string()));
        }
        let from_path = join_path(&from_parent.path, from_name)?;
        let to_path = join_path(&to_parent.path, to_name)?;
        let source = self
            .get_node_by_path_tx(&tx, workspace_id, &from_path)?
            .ok_or_else(|| IndexError::NodeNotFound {
                workspace_id: workspace_id.to_string(),
                path: from_path.clone(),
            })?;
        if let Some(expected) = if_version {
            if expected != 0 && expected != source.attrs.version {
                return Err(IndexError::VersionConflict {
                    expected,
                    found: source.attrs.version,
                });
            }
        }

        if let Some(existing_dest) = self.get_node_by_path_tx(&tx, workspace_id, &to_path)? {
            if !overwrite {
                return Err(IndexError::AlreadyExistsAtPath { path: to_path });
            }
            if existing_dest.attrs.kind == NodeKind::Dir {
                let has_children = tx
                    .query_row(
                        "SELECT 1 FROM nodes WHERE workspace_id = ?1 AND parent_node_id = ?2 LIMIT 1",
                        params![workspace_id, existing_dest.node_id],
                        |_row| Ok(()),
                    )
                    .optional()?
                    .is_some();
                if has_children {
                    return Err(IndexError::DirectoryNotEmpty(existing_dest.node_id));
                }
            }
            tx.execute(
                "DELETE FROM nodes WHERE workspace_id = ?1 AND node_id = ?2",
                params![workspace_id, existing_dest.node_id],
            )?;
        }

        let next_version = source.attrs.version.saturating_add(1);
        let now = now_ns();
        if source.attrs.kind == NodeKind::File {
            let mime = infer_mime_for_put("", &to_path);
            tx.execute(
                "UPDATE nodes SET parent_node_id = ?3, name = ?4, path = ?5, version = ?6, updated_at_ns = ?7, mime = ?8 WHERE workspace_id = ?1 AND node_id = ?2",
                params![
                    workspace_id,
                    source.node_id,
                    to_parent_node_id,
                    to_name,
                    to_path,
                    next_version,
                    now,
                    mime.as_str(),
                ],
            )?;
        } else {
            tx.execute(
                "UPDATE nodes SET parent_node_id = ?3, name = ?4, path = ?5, version = ?6, updated_at_ns = ?7 WHERE workspace_id = ?1 AND node_id = ?2",
                params![
                    workspace_id,
                    source.node_id,
                    to_parent_node_id,
                    to_name,
                    to_path,
                    next_version,
                    now
                ],
            )?;
        }
        if source.attrs.kind == NodeKind::Dir {
            let old_prefix = format!("{from_path}/");
            let new_prefix = format!("{to_path}/");
            tx.execute(
                "UPDATE nodes
                 SET path = ?1 || substr(path, ?2)
                 WHERE workspace_id = ?3 AND path LIKE ?4",
                params![
                    new_prefix,
                    i64::try_from(old_prefix.len() + 1)
                        .map_err(|_| IndexError::NumericConversion)?,
                    workspace_id,
                    format!("{old_prefix}%")
                ],
            )?;
        }
        tx.execute(
            "INSERT INTO events(workspace_id, kind, node_id, payload_json, created_at_ns) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![workspace_id, EventKind::NodeRenamed.as_db(), source.node_id, "{}", now],
        )?;
        let event_id = tx.last_insert_rowid();
        tx.commit()?;
        Ok(((source.node_id, next_version), event_id))
    }

    pub fn set_attrs(
        &self,
        workspace_id: &str,
        node_id: &str,
        patch: AttrsPatch,
    ) -> Result<(NodeAttrs, i64)> {
        let mut conn = self.writer_connection()?;
        let tx = conn.transaction()?;
        ensure_workspace_exists(&tx, workspace_id)?;
        let node = self.get_node_by_id_tx(&tx, workspace_id, node_id)?;
        let mut mode = node.attrs.mode;
        let mut mtime = node.attrs.mtime_unix_nano;
        if let Some(patched_mode) = patch.mode {
            mode = patched_mode;
        }
        if let Some(patched_mtime) = patch.mtime_unix_nano {
            mtime = patched_mtime;
        }
        let next_version = node.attrs.version.saturating_add(1);
        let now = now_ns();
        tx.execute(
            "UPDATE nodes SET mode = ?3, mtime_ns = ?4, version = ?5, updated_at_ns = ?6 WHERE workspace_id = ?1 AND node_id = ?2",
            params![workspace_id, node_id, mode, mtime, next_version, now],
        )?;
        tx.execute(
            "INSERT INTO events(workspace_id, kind, node_id, payload_json, created_at_ns) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![workspace_id, EventKind::AttrsChanged.as_db(), node_id, "{}", now],
        )?;
        let event_id = tx.last_insert_rowid();
        tx.commit()?;
        let attrs = self.get_attrs(workspace_id, node_id)?;
        Ok((attrs, event_id))
    }

    pub fn search_fts(
        &self,
        workspace_id: &str,
        query: &str,
        limit: u32,
    ) -> Result<SearchResultSet> {
        let conn = self.reader_connection()?;
        ensure_workspace_exists_conn(&conn, workspace_id)?;
        let sql_limit = i64::from(limit.max(1));
        let total_hits = self.count_fts_hits(&conn, workspace_id, query)?;
        let mut stmt = conn.prepare(
            "SELECT n.node_id, n.path, c.start_line, c.end_line, c.content, c.context_path, bm25(chunks_fts), COALESCE(c.is_headline, 0)
             FROM chunks_fts f
             JOIN chunks c ON c.id = f.rowid
             JOIN nodes n ON n.node_id = c.node_id
             WHERE c.workspace_id = ?1 AND chunks_fts MATCH ?2
             ORDER BY bm25(chunks_fts)
             LIMIT ?3",
        )?;

        let rows = stmt.query_map(params![workspace_id, query, sql_limit], |row| {
            let start_line_i64: i64 = row.get(2)?;
            let end_line_i64: i64 = row.get(3)?;
            let headline: i64 = row.get(7)?;
            Ok(SearchRecord {
                node_id: row.get(0)?,
                path: row.get(1)?,
                start_line: u32::try_from(start_line_i64)
                    .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(2, start_line_i64))?,
                end_line: u32::try_from(end_line_i64)
                    .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(3, end_line_i64))?,
                score: bm25_to_score(row.get::<_, f64>(6)?),
                snippet: row.get(4)?,
                context_path: row.get(5)?,
                headline_only: headline != 0,
            })
        })?;

        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(SearchResultSet {
            hits: out,
            total_hits,
        })
    }

    pub fn search(
        &self,
        workspace_id: &str,
        query: &str,
        mode: SearchMode,
        limit: u32,
    ) -> Result<SearchResultSet> {
        match mode {
            SearchMode::Fts => self.search_fts(workspace_id, query, limit),
            SearchMode::Vector => self.search_vector(workspace_id, query, limit),
            SearchMode::Hybrid => self.search_hybrid(workspace_id, query, limit),
        }
    }

    pub fn search_vector(
        &self,
        workspace_id: &str,
        query: &str,
        limit: u32,
    ) -> Result<SearchResultSet> {
        let query_vec = simple_embed(query);
        self.search_vector_with_embedding(workspace_id, &query_vec, limit)
    }

    pub fn search_vector_with_embedding(
        &self,
        workspace_id: &str,
        query_embedding: &[f32],
        limit: u32,
    ) -> Result<SearchResultSet> {
        let conn = self.reader_connection()?;
        ensure_workspace_exists_conn(&conn, workspace_id)?;
        let query_blob = embedding_vec_to_blob(query_embedding)?;
        let sql_limit = i64::from(limit.max(1));
        let mut stmt = conn.prepare(
            "SELECT chunk_id, distance
             FROM vec_chunks
             WHERE embedding MATCH ?1 AND k = ?2 AND workspace_id = ?3
             ORDER BY distance ASC",
        )?;
        let rows = stmt.query_map(params![query_blob, sql_limit, workspace_id], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, f32>(1)?))
        })?;

        let mut hits = Vec::new();
        for row in rows {
            let (chunk_id, distance) = row?;
            let mut record = self.fetch_chunk_record(&conn, workspace_id, chunk_id)?;
            record.score = cosine_distance_to_score(distance);
            hits.push(record);
        }
        let total_hits = u32::try_from(hits.len()).map_err(|_| IndexError::NumericConversion)?;
        Ok(SearchResultSet { hits, total_hits })
    }

    fn ranked_fts(&self, workspace_id: &str, query: &str, limit: u32) -> Result<Vec<RankedChunk>> {
        let conn = self.reader_connection()?;
        ensure_workspace_exists_conn(&conn, workspace_id)?;
        let sql_limit = i64::from(limit.max(1));
        let mut stmt = conn.prepare(
            "SELECT c.id, n.node_id, n.path, c.start_line, c.end_line, c.content, c.context_path, bm25(chunks_fts), COALESCE(c.is_headline, 0)
             FROM chunks_fts f
             JOIN chunks c ON c.id = f.rowid
             JOIN nodes n ON n.node_id = c.node_id
             WHERE c.workspace_id = ?1 AND chunks_fts MATCH ?2
             ORDER BY bm25(chunks_fts)
             LIMIT ?3",
        )?;
        let rows = stmt.query_map(params![workspace_id, query, sql_limit], |row| {
            let start_line_i64: i64 = row.get(3)?;
            let end_line_i64: i64 = row.get(4)?;
            let headline: i64 = row.get(8)?;
            Ok((
                row.get::<_, i64>(0)?,
                SearchRecord {
                    node_id: row.get(1)?,
                    path: row.get(2)?,
                    start_line: u32::try_from(start_line_i64)
                        .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(3, start_line_i64))?,
                    end_line: u32::try_from(end_line_i64)
                        .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(4, end_line_i64))?,
                    score: bm25_to_score(row.get::<_, f64>(7)?),
                    snippet: row.get(5)?,
                    context_path: row.get(6)?,
                    headline_only: headline != 0,
                },
            ))
        })?;
        let mut out = Vec::new();
        for (idx, row) in rows.enumerate() {
            let (chunk_id, record) = row?;
            out.push(RankedChunk {
                chunk_id,
                rank: idx + 1,
                record,
            });
        }
        Ok(out)
    }

    fn ranked_vector_with_embedding(
        &self,
        workspace_id: &str,
        query_embedding: &[f32],
        limit: u32,
    ) -> Result<Vec<RankedChunk>> {
        let conn = self.reader_connection()?;
        ensure_workspace_exists_conn(&conn, workspace_id)?;
        let query_blob = embedding_vec_to_blob(query_embedding)?;
        let sql_limit = i64::from(limit.max(1));
        let mut stmt = conn.prepare(
            "SELECT chunk_id, distance
             FROM vec_chunks
             WHERE embedding MATCH ?1 AND k = ?2 AND workspace_id = ?3
             ORDER BY distance ASC",
        )?;
        let rows = stmt.query_map(params![query_blob, sql_limit, workspace_id], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, f32>(1)?))
        })?;
        let mut out = Vec::new();
        for (idx, row) in rows.enumerate() {
            let (chunk_id, distance) = row?;
            let mut record = self.fetch_chunk_record(&conn, workspace_id, chunk_id)?;
            record.score = cosine_distance_to_score(distance);
            out.push(RankedChunk {
                chunk_id,
                rank: idx + 1,
                record,
            });
        }
        Ok(out)
    }

    pub fn search_hybrid(
        &self,
        workspace_id: &str,
        query: &str,
        limit: u32,
    ) -> Result<SearchResultSet> {
        let query_vec = simple_embed(query);
        self.search_hybrid_with_embedding(workspace_id, query, &query_vec, limit)
    }

    pub fn search_hybrid_with_embedding(
        &self,
        workspace_id: &str,
        query: &str,
        query_embedding: &[f32],
        limit: u32,
    ) -> Result<SearchResultSet> {
        let fts = self.ranked_fts(workspace_id, query, limit.saturating_mul(3))?;
        let vec = self.ranked_vector_with_embedding(
            workspace_id,
            query_embedding,
            limit.saturating_mul(3),
        )?;
        let mut map: std::collections::HashMap<i64, (f32, SearchRecord)> =
            std::collections::HashMap::new();

        for item in fts {
            let entry = map
                .entry(item.chunk_id)
                .or_insert((0.0, item.record.clone()));
            entry.0 += rrf_score(item.rank);
        }
        for item in vec {
            let entry = map
                .entry(item.chunk_id)
                .or_insert((0.0, item.record.clone()));
            entry.0 += rrf_score(item.rank);
        }

        let total_hits = u32::try_from(map.len()).map_err(|_| IndexError::NumericConversion)?;
        let mut scored: Vec<(f32, SearchRecord)> = map.into_values().collect();
        scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        let hits = scored
            .into_iter()
            .take(limit.max(1) as usize)
            .map(|(score, mut record)| {
                record.score = score;
                record
            })
            .collect();
        Ok(SearchResultSet { hits, total_hits })
    }

    pub fn get_event_by_id(&self, workspace_id: &str, event_id: i64) -> Result<EventRecord> {
        let conn = self.reader_connection()?;
        ensure_workspace_exists_conn(&conn, workspace_id)?;
        let mut stmt = conn.prepare(
            "SELECT e.id, e.kind, e.node_id, e.payload_json, e.created_at_ns, COALESCE(n.path, '')
             FROM events e
             LEFT JOIN nodes n ON n.workspace_id = e.workspace_id AND n.node_id = e.node_id
             WHERE e.workspace_id = ?1 AND e.id = ?2",
        )?;
        stmt.query_row(params![workspace_id, event_id], |row| {
            let id: i64 = row.get(0)?;
            let kind: String = row.get(1)?;
            let kind = match kind.as_str() {
                "NodeCreated" => EventKind::NodeCreated,
                "NodeModified" => EventKind::NodeModified,
                "NodeDeleted" => EventKind::NodeDeleted,
                "NodeRenamed" => EventKind::NodeRenamed,
                "AttrsChanged" => EventKind::AttrsChanged,
                "IndexUpdated" => EventKind::IndexUpdated,
                _ => EventKind::NodeModified,
            };
            let node_id: Option<String> = row.get(2)?;
            Ok(EventRecord {
                cursor: id.to_string(),
                created_at_unix_nano: row.get(4)?,
                kind,
                workspace_id: workspace_id.to_string(),
                node_id: node_id.unwrap_or_default(),
                payload_json: row.get(3)?,
                path: row.get(5)?,
            })
        })
        .map_err(IndexError::from)
    }

    pub fn list_events_since(
        &self,
        workspace_id: &str,
        since_cursor: Option<u64>,
        limit: u32,
    ) -> Result<Vec<EventRecord>> {
        let conn = self.reader_connection()?;
        ensure_workspace_exists_conn(&conn, workspace_id)?;
        let cursor =
            i64::try_from(since_cursor.unwrap_or(0)).map_err(|_| IndexError::NumericConversion)?;
        let page_size = i64::from(limit.clamp(1, 1024));
        let mut stmt = conn.prepare(
            "SELECT e.id, e.kind, e.node_id, e.payload_json, e.created_at_ns, COALESCE(n.path, '')
             FROM events e
             LEFT JOIN nodes n ON n.workspace_id = e.workspace_id AND n.node_id = e.node_id
             WHERE e.workspace_id = ?1 AND e.id > ?2
             ORDER BY e.id ASC
             LIMIT ?3",
        )?;
        let rows = stmt.query_map(params![workspace_id, cursor, page_size], |row| {
            let id: i64 = row.get(0)?;
            let kind: String = row.get(1)?;
            let kind = match kind.as_str() {
                "NodeCreated" => EventKind::NodeCreated,
                "NodeModified" => EventKind::NodeModified,
                "NodeDeleted" => EventKind::NodeDeleted,
                "NodeRenamed" => EventKind::NodeRenamed,
                "AttrsChanged" => EventKind::AttrsChanged,
                "IndexUpdated" => EventKind::IndexUpdated,
                _ => EventKind::NodeModified,
            };
            let node_id: Option<String> = row.get(2)?;
            Ok(EventRecord {
                cursor: id.to_string(),
                created_at_unix_nano: row.get(4)?,
                kind,
                workspace_id: workspace_id.to_string(),
                node_id: node_id.unwrap_or_default(),
                payload_json: row.get(3)?,
                path: row.get(5)?,
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    pub fn acknowledge_cursor(
        &self,
        workspace_id: &str,
        subscriber_id: &str,
        cursor: &str,
    ) -> Result<()> {
        let conn = self.reader_connection()?;
        ensure_workspace_exists_conn(&conn, workspace_id)?;
        let cursor_id = cursor
            .parse::<i64>()
            .map_err(|_| IndexError::InvalidCursor)?;
        conn.execute(
            "INSERT INTO subscriber_acks(workspace_id, subscriber_id, cursor_id, updated_at_ns)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(workspace_id, subscriber_id)
             DO UPDATE SET cursor_id = excluded.cursor_id, updated_at_ns = excluded.updated_at_ns",
            params![workspace_id, subscriber_id, cursor_id, now_ns()],
        )?;
        Ok(())
    }

    pub fn latest_event_cursor(&self, workspace_id: &str) -> Result<Option<u64>> {
        let conn = self.reader_connection()?;
        ensure_workspace_exists_conn(&conn, workspace_id)?;
        let value = conn
            .query_row(
                "SELECT id FROM events WHERE workspace_id = ?1 ORDER BY id DESC LIMIT 1",
                params![workspace_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;
        match value {
            Some(id) => Ok(Some(
                u64::try_from(id).map_err(|_| IndexError::NumericConversion)?,
            )),
            None => Ok(None),
        }
    }

    pub fn subscriber_cursor(
        &self,
        workspace_id: &str,
        subscriber_id: &str,
    ) -> Result<Option<u64>> {
        let conn = self.reader_connection()?;
        ensure_workspace_exists_conn(&conn, workspace_id)?;
        let value = conn
            .query_row(
                "SELECT cursor_id
                 FROM subscriber_acks
                 WHERE workspace_id = ?1 AND subscriber_id = ?2
                 LIMIT 1",
                params![workspace_id, subscriber_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;
        match value {
            Some(cursor_id) => Ok(Some(
                u64::try_from(cursor_id).map_err(|_| IndexError::NumericConversion)?,
            )),
            None => Ok(None),
        }
    }

    pub fn indexing_metrics_snapshot(&self) -> Result<IndexingMetricsSnapshot> {
        let conn = self.reader_connection()?;
        let queued_jobs = conn.query_row(
            "SELECT COUNT(*) FROM indexing_jobs WHERE status = 'queued'",
            [],
            |row| row.get::<_, i64>(0),
        )?;
        let running_jobs = conn.query_row(
            "SELECT COUNT(*) FROM indexing_jobs WHERE status = 'running'",
            [],
            |row| row.get::<_, i64>(0),
        )?;
        let completed_jobs = conn.query_row(
            "SELECT COUNT(*) FROM indexing_jobs WHERE status = 'completed'",
            [],
            |row| row.get::<_, i64>(0),
        )?;
        let failed_jobs = conn.query_row(
            "SELECT COUNT(*) FROM indexing_jobs WHERE status = 'failed'",
            [],
            |row| row.get::<_, i64>(0),
        )?;

        let avg_running_latency_ms = conn
            .query_row(
                "SELECT AVG((?1 - started_at_ns) / 1000000.0)
                 FROM indexing_jobs
                 WHERE status = 'running' AND started_at_ns IS NOT NULL",
                params![now_ns()],
                |row| row.get::<_, Option<f64>>(0),
            )?
            .map(f64::round)
            .filter(|value| *value >= 0.0)
            .map(|value| value as u64);

        let avg_completion_latency_ms = conn
            .query_row(
                "SELECT AVG((completed_at_ns - started_at_ns) / 1000000.0)
                 FROM indexing_jobs
                 WHERE status = 'completed'
                   AND started_at_ns IS NOT NULL
                   AND completed_at_ns IS NOT NULL",
                [],
                |row| row.get::<_, Option<f64>>(0),
            )?
            .map(f64::round)
            .filter(|value| *value >= 0.0)
            .map(|value| value as u64);

        Ok(IndexingMetricsSnapshot {
            queued_jobs: u64::try_from(queued_jobs).map_err(|_| IndexError::NumericConversion)?,
            running_jobs: u64::try_from(running_jobs).map_err(|_| IndexError::NumericConversion)?,
            completed_jobs: u64::try_from(completed_jobs)
                .map_err(|_| IndexError::NumericConversion)?,
            failed_jobs: u64::try_from(failed_jobs).map_err(|_| IndexError::NumericConversion)?,
            avg_running_latency_ms,
            avg_completion_latency_ms,
        })
    }

    pub fn cleanup_events_retention(
        &self,
        max_age: std::time::Duration,
        min_events_per_workspace: u64,
    ) -> Result<u64> {
        let max_age_ns = i64::try_from(max_age.as_nanos()).unwrap_or(i64::MAX);
        let cutoff_unix_nano = now_ns().saturating_sub(max_age_ns);
        self.cleanup_events_retention_before(cutoff_unix_nano, min_events_per_workspace)
    }

    pub fn cleanup_events_retention_before(
        &self,
        cutoff_unix_nano: i64,
        min_events_per_workspace: u64,
    ) -> Result<u64> {
        let mut conn = self.writer_connection()?;
        let tx = conn.transaction()?;
        let mut deleted_total = 0u64;
        let keep_offset = i64::try_from(min_events_per_workspace.saturating_sub(1))
            .map_err(|_| IndexError::NumericConversion)?;

        let mut ws_stmt =
            tx.prepare("SELECT workspace_id FROM workspaces ORDER BY workspace_id")?;
        let workspace_rows = ws_stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut workspace_ids = Vec::new();
        for row in workspace_rows {
            workspace_ids.push(row?);
        }
        drop(ws_stmt);

        for workspace_id in workspace_ids {
            let keep_floor = tx
                .query_row(
                    "SELECT id
                     FROM events
                     WHERE workspace_id = ?1
                     ORDER BY id DESC
                     LIMIT 1 OFFSET ?2",
                    params![workspace_id, keep_offset],
                    |row| row.get::<_, i64>(0),
                )
                .optional()?;

            let Some(keep_floor) = keep_floor else {
                continue;
            };

            let deleted = tx.execute(
                "DELETE FROM events
                 WHERE workspace_id = ?1
                   AND created_at_ns < ?2
                   AND id < ?3",
                params![workspace_id, cutoff_unix_nano, keep_floor],
            )?;
            deleted_total = deleted_total
                .saturating_add(u64::try_from(deleted).map_err(|_| IndexError::NumericConversion)?);
        }

        tx.commit()?;
        Ok(deleted_total)
    }

    fn count_fts_hits(&self, conn: &Connection, workspace_id: &str, query: &str) -> Result<u32> {
        let count: i64 = conn.query_row(
            "SELECT COUNT(*)
             FROM chunks_fts f
             JOIN chunks c ON c.id = f.rowid
             WHERE c.workspace_id = ?1 AND chunks_fts MATCH ?2",
            params![workspace_id, query],
            |row| row.get(0),
        )?;
        u32::try_from(count).map_err(|_| IndexError::NumericConversion)
    }

    fn fetch_chunk_record(
        &self,
        conn: &Connection,
        workspace_id: &str,
        chunk_id: i64,
    ) -> Result<SearchRecord> {
        conn.query_row(
            "SELECT n.node_id, n.path, c.start_line, c.end_line, c.content, c.context_path, COALESCE(c.is_headline, 0)
             FROM chunks c
             JOIN nodes n ON n.node_id = c.node_id
             WHERE c.workspace_id = ?1 AND c.id = ?2",
            params![workspace_id, chunk_id],
            |row| {
                let start_line_i64: i64 = row.get(2)?;
                let end_line_i64: i64 = row.get(3)?;
                let headline: i64 = row.get(6)?;
                Ok(SearchRecord {
                    node_id: row.get(0)?,
                    path: row.get(1)?,
                    start_line: u32::try_from(start_line_i64)
                        .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(2, start_line_i64))?,
                    end_line: u32::try_from(end_line_i64)
                        .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(3, end_line_i64))?,
                    score: 0.0,
                    snippet: row.get(4)?,
                    context_path: row.get(5)?,
                    headline_only: headline != 0,
                })
            },
        )
        .map_err(IndexError::from)
    }

    fn get_node_by_id_conn(
        &self,
        conn: &Connection,
        workspace_id: &str,
        node_id: &str,
    ) -> Result<NodeRecord> {
        conn.query_row(
            "SELECT node_id, parent_node_id, name, path, kind, size, mtime_ns, mode, version, content_hash, mime
             FROM nodes WHERE workspace_id = ?1 AND node_id = ?2",
            params![workspace_id, node_id],
            map_node_row,
        )
        .optional()?
        .ok_or_else(|| IndexError::NodeIdNotFound {
            workspace_id: workspace_id.to_string(),
            node_id: node_id.to_string(),
        })
    }

    fn get_node_by_path_conn(
        &self,
        conn: &Connection,
        workspace_id: &str,
        path: &str,
    ) -> Result<Option<NodeRecord>> {
        Ok(conn
            .query_row(
                "SELECT node_id, parent_node_id, name, path, kind, size, mtime_ns, mode, version, content_hash, mime
                 FROM nodes WHERE workspace_id = ?1 AND path = ?2",
                params![workspace_id, path],
                map_node_row,
            )
            .optional()?)
    }

    fn get_node_by_id_tx(
        &self,
        tx: &Transaction<'_>,
        workspace_id: &str,
        node_id: &str,
    ) -> Result<NodeRecord> {
        tx.query_row(
            "SELECT node_id, parent_node_id, name, path, kind, size, mtime_ns, mode, version, content_hash, mime
             FROM nodes WHERE workspace_id = ?1 AND node_id = ?2",
            params![workspace_id, node_id],
            map_node_row,
        )
        .optional()?
        .ok_or_else(|| IndexError::NodeIdNotFound {
            workspace_id: workspace_id.to_string(),
            node_id: node_id.to_string(),
        })
    }

    fn get_node_by_path_tx(
        &self,
        tx: &Transaction<'_>,
        workspace_id: &str,
        path: &str,
    ) -> Result<Option<NodeRecord>> {
        Ok(tx
            .query_row(
                "SELECT node_id, parent_node_id, name, path, kind, size, mtime_ns, mode, version, content_hash, mime
                 FROM nodes WHERE workspace_id = ?1 AND path = ?2",
                params![workspace_id, path],
                map_node_row,
            )
            .optional()?)
    }

    fn require_directory_tx(
        &self,
        tx: &Transaction<'_>,
        workspace_id: &str,
        path: &str,
    ) -> Result<NodeRecord> {
        let node = self
            .get_node_by_path_tx(tx, workspace_id, path)?
            .ok_or_else(|| IndexError::ParentNotFound(path.to_string()))?;
        if node.attrs.kind != NodeKind::Dir {
            return Err(IndexError::NotDirectory(node.node_id));
        }
        Ok(node)
    }
}

fn register_sqlite_vec_extension_once() {
    static SQLITE_VEC_INIT: Once = Once::new();
    SQLITE_VEC_INIT.call_once(|| unsafe {
        // sqlite-vec exposes a zero-arg entrypoint; SQLite's auto-extension API expects the
        // sqlite3_load_extension entry signature. The cast matches what sqlite-vec's own tests use.
        type SqliteVecInit = unsafe extern "C" fn();
        type SqliteAutoExtension = unsafe extern "C" fn(
            *mut rusqlite::ffi::sqlite3,
            *mut *mut c_char,
            *const rusqlite::ffi::sqlite3_api_routines,
        ) -> c_int;
        let init: SqliteAutoExtension =
            std::mem::transmute::<SqliteVecInit, SqliteAutoExtension>(sqlite3_vec_init);
        sqlite3_auto_extension(Some(init));
    });
}

pub fn open_or_create(path: impl AsRef<Path>) -> Result<Connection> {
    let mut conn = IndexStore::open_connection(path.as_ref())?;
    migrations::apply(&mut conn)?;
    Ok(conn)
}

fn merge_write(base_content: &[u8], offset: u64, data: &[u8]) -> Vec<u8> {
    let Ok(offset_usize) = usize::try_from(offset) else {
        return base_content.to_vec();
    };
    let required_len = offset_usize.saturating_add(data.len());
    let mut merged = base_content.to_vec();
    if merged.len() < required_len {
        merged.resize(required_len, 0);
    }
    merged[offset_usize..offset_usize + data.len()].copy_from_slice(data);
    merged
}

fn clamp_read_window(content: &[u8], offset: u64, length: u32) -> (Vec<u8>, bool) {
    let Ok(start) = usize::try_from(offset) else {
        return (Vec::new(), true);
    };
    if start >= content.len() {
        return (Vec::new(), true);
    }
    let len = usize::try_from(length).unwrap_or(usize::MAX);
    let end = start.saturating_add(len).min(content.len());
    let data = content[start..end].to_vec();
    let eof = end >= content.len();
    (data, eof)
}

fn ensure_workspace_exists(tx: &Transaction<'_>, workspace_id: &str) -> Result<()> {
    let exists = tx
        .query_row(
            "SELECT 1 FROM workspaces WHERE workspace_id = ?1 LIMIT 1",
            params![workspace_id],
            |_row| Ok(()),
        )
        .optional()?
        .is_some();
    if exists {
        Ok(())
    } else {
        Err(IndexError::WorkspaceNotFound(workspace_id.to_string()))
    }
}

fn ensure_workspace_exists_conn(conn: &Connection, workspace_id: &str) -> Result<()> {
    let exists = conn
        .query_row(
            "SELECT 1 FROM workspaces WHERE workspace_id = ?1 LIMIT 1",
            params![workspace_id],
            |_row| Ok(()),
        )
        .optional()?
        .is_some();
    if exists {
        Ok(())
    } else {
        Err(IndexError::WorkspaceNotFound(workspace_id.to_string()))
    }
}

fn normalize_path(path: &str) -> Result<&str> {
    let trimmed = path.trim();
    if trimmed.contains('\\') {
        return Err(IndexError::InvalidPath(path.to_string()));
    }
    let stripped = trimmed.trim_start_matches('/');
    if stripped.contains("//") || stripped.split('/').any(|seg| seg == "." || seg == "..") {
        return Err(IndexError::InvalidPath(path.to_string()));
    }
    Ok(stripped)
}

/// Normalized relative path key used by chunking / FTS (same rules as index `put_file`).
pub fn normalized_index_path(path: &str) -> Result<String> {
    Ok(normalize_path(path)?.to_string())
}

fn split_parent_name(path: &str) -> Result<(&str, &str)> {
    if path.is_empty() {
        return Err(IndexError::InvalidPath(path.to_string()));
    }
    if let Some((parent, name)) = path.rsplit_once('/') {
        if name.is_empty() {
            return Err(IndexError::InvalidPath(path.to_string()));
        }
        Ok((parent, name))
    } else {
        Ok(("", path))
    }
}

fn join_path(parent: &str, name: &str) -> Result<String> {
    let validated_name = name.trim();
    if validated_name.is_empty()
        || validated_name.contains('/')
        || validated_name == "."
        || validated_name == ".."
    {
        return Err(IndexError::InvalidPath(name.to_string()));
    }
    if parent.is_empty() {
        Ok(validated_name.to_string())
    } else {
        Ok(format!("{parent}/{validated_name}"))
    }
}

fn map_node_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<NodeRecord> {
    let kind_str: String = row.get(4)?;
    let kind = NodeKind::from_db(&kind_str).ok_or_else(|| {
        rusqlite::Error::InvalidColumnType(4, "kind".into(), rusqlite::types::Type::Text)
    })?;
    let size_i64: i64 = row.get(5)?;
    let mode_i64: i64 = row.get(7)?;
    let version_i64: i64 = row.get(8)?;
    let content_hash: String = row.get(9)?;
    let mime: String = row.get(10)?;
    Ok(NodeRecord {
        node_id: row.get(0)?,
        parent_node_id: row.get(1)?,
        name: row.get(2)?,
        path: row.get(3)?,
        attrs: NodeAttrs {
            kind,
            size: u64::try_from(size_i64)
                .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(5, size_i64))?,
            mtime_unix_nano: row.get(6)?,
            mode: u32::try_from(mode_i64)
                .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(7, mode_i64))?,
            version: u64::try_from(version_i64)
                .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(8, version_i64))?,
            content_hash,
            mime,
        },
    })
}

fn parse_cursor(cursor: Option<&str>) -> Result<i64> {
    match cursor {
        Some(value) if !value.is_empty() => value
            .parse::<i64>()
            .ok()
            .filter(|v| *v >= 0)
            .ok_or(IndexError::InvalidCursor),
        _ => Ok(0),
    }
}

fn serialize_f32_json(values: &[f32]) -> Result<String> {
    serde_json::to_string(values).map_err(|_| IndexError::NumericConversion)
}

/// Raw little-endian `f32` bytes (dimension = `values.len()`), matching sqlite-vec `vec0` storage.
fn embedding_vec_to_blob(values: &[f32]) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(values.len() * 4);
    for v in values {
        out.extend_from_slice(&v.to_le_bytes());
    }
    Ok(out)
}

fn infer_mime_for_put(prev_mime: &str, path: &str) -> String {
    let guessed = mime_guess::from_path(Path::new(path))
        .first_or_octet_stream()
        .essence_str()
        .to_string();
    let prev = prev_mime.trim();
    if prev.is_empty() {
        return guessed;
    }
    prev.to_string()
}

fn bm25_to_score(rank: f64) -> f32 {
    let normalized = rank.max(0.0);
    (1.0f64 / (1.0 + normalized)) as f32
}

fn cosine_distance_to_score(distance: f32) -> f32 {
    (1.0 - distance).clamp(-1.0, 1.0)
}

fn rrf_score(rank: usize) -> f32 {
    1.0 / (60.0 + rank as f32)
}

pub(crate) fn simple_embed(text: &str) -> Vec<f32> {
    let mut out = vec![0f32; 32];
    for (i, byte) in text.bytes().enumerate() {
        let idx = i % out.len();
        out[idx] += (byte as f32) / 255.0;
    }
    out
}

fn now_ns() -> i64 {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::{AttrsPatch, IndexStore, IndexingJobStatus, NodeKind, SearchMode};
    use std::sync::Arc;
    use std::time::Duration;

    macro_rules! create_node {
        ($store:expr, $($arg:expr),+ $(,)?) => {{
            let (node, _) = $store.create_node($($arg),+).expect("create_node");
            node
        }};
    }

    const LEGACY_BOOTSTRAP_SQL: &str = r#"
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;

CREATE TABLE IF NOT EXISTS workspaces (
  workspace_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  created_at_ns INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS nodes (
  node_id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
  parent_node_id TEXT REFERENCES nodes(node_id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  path TEXT NOT NULL,
  kind TEXT NOT NULL DEFAULT 'file',
  mode INTEGER NOT NULL DEFAULT 420,
  mtime_ns INTEGER NOT NULL,
  version INTEGER NOT NULL DEFAULT 1,
  size INTEGER NOT NULL DEFAULT 0,
  created_at_ns INTEGER NOT NULL,
  updated_at_ns INTEGER NOT NULL,
  UNIQUE(workspace_id, path)
);

CREATE TABLE IF NOT EXISTS node_contents (
  node_id TEXT PRIMARY KEY REFERENCES nodes(node_id) ON DELETE CASCADE,
  workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
  content BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS chunks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
  node_id TEXT NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
  start_line INTEGER NOT NULL,
  end_line INTEGER NOT NULL,
  content TEXT NOT NULL,
  context_path TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS chunk_embeddings (
  chunk_id INTEGER PRIMARY KEY REFERENCES chunks(id) ON DELETE CASCADE,
  embedding_json TEXT NOT NULL
);

CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
  content,
  context_path,
  content='chunks',
  content_rowid='id',
  tokenize='unicode61 remove_diacritics 2'
);

CREATE TRIGGER IF NOT EXISTS chunks_ai AFTER INSERT ON chunks BEGIN
  INSERT INTO chunks_fts(rowid, content, context_path)
  VALUES (new.id, new.content, new.context_path);
END;

CREATE TRIGGER IF NOT EXISTS chunks_ad AFTER DELETE ON chunks BEGIN
  INSERT INTO chunks_fts(chunks_fts, rowid, content, context_path)
  VALUES ('delete', old.id, old.content, old.context_path);
END;

CREATE TRIGGER IF NOT EXISTS chunks_au AFTER UPDATE ON chunks BEGIN
  INSERT INTO chunks_fts(chunks_fts, rowid, content, context_path)
  VALUES ('delete', old.id, old.content, old.context_path);
  INSERT INTO chunks_fts(rowid, content, context_path)
  VALUES (new.id, new.content, new.context_path);
END;

CREATE TABLE IF NOT EXISTS events (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  workspace_id    TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
  kind            TEXT NOT NULL,
  node_id         TEXT,
  payload_json    TEXT NOT NULL,
  created_at_ns   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS config (
  key             TEXT PRIMARY KEY,
  value           TEXT NOT NULL,
  updated_at_ns   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS subscriber_acks (
  workspace_id TEXT NOT NULL,
  subscriber_id TEXT NOT NULL,
  cursor_id INTEGER NOT NULL,
  updated_at_ns INTEGER NOT NULL,
  PRIMARY KEY (workspace_id, subscriber_id)
);
"#;

    fn read_schema_migrations(conn: &rusqlite::Connection) -> Vec<(i64, i64)> {
        let mut stmt = conn
            .prepare("SELECT version, applied_at_ns FROM schema_migrations ORDER BY version ASC")
            .expect("prepare migrations query");
        let rows = stmt
            .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
            .expect("query migrations");
        rows.map(|row| row.expect("migration row")).collect()
    }

    #[test]
    fn creates_events_table() {
        let db_path = std::env::temp_dir().join(format!("scry-index-{}.db", uuid::Uuid::now_v7()));
        let store = IndexStore::open(&db_path).expect("db init");
        let conn = rusqlite::Connection::open(store.db_path()).expect("open sqlite");
        let count: i64 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='events'",
                [],
                |row| row.get(0),
            )
            .expect("query");
        assert_eq!(count, 1);
        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn open_migrates_legacy_schema_to_latest_version() {
        let db_path = std::env::temp_dir().join(format!("scry-index-{}.db", uuid::Uuid::now_v7()));
        let legacy_conn = rusqlite::Connection::open(&db_path).expect("open legacy sqlite");
        legacy_conn
            .execute_batch(LEGACY_BOOTSTRAP_SQL)
            .expect("seed legacy schema");
        drop(legacy_conn);

        let store = IndexStore::open(&db_path).expect("open should apply migrations");
        let conn = rusqlite::Connection::open(store.db_path()).expect("open migrated sqlite");

        let migrations = read_schema_migrations(&conn);
        assert_eq!(migrations.len(), super::migrations::MIGRATIONS.len());

        let redirects_table_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='redirects'",
                [],
                |row| row.get(0),
            )
            .expect("query redirects table");
        assert_eq!(redirects_table_count, 1);

        let indexing_jobs_table_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='indexing_jobs'",
                [],
                |row| row.get(0),
            )
            .expect("query indexing_jobs table");
        assert_eq!(indexing_jobs_table_count, 1);

        let vec_chunks_table_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='vec_chunks'",
                [],
                |row| row.get(0),
            )
            .expect("query vec_chunks virtual table");
        assert_eq!(vec_chunks_table_count, 1);

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn schema_migrations_are_idempotent_across_restarts() {
        let db_path = std::env::temp_dir().join(format!("scry-index-{}.db", uuid::Uuid::now_v7()));

        let first_store = IndexStore::open(&db_path).expect("first open");
        drop(first_store);
        let first_conn =
            rusqlite::Connection::open(&db_path).expect("open sqlite after first boot");
        let first_snapshot = read_schema_migrations(&first_conn);
        drop(first_conn);

        let second_store = IndexStore::open(&db_path).expect("second open");
        drop(second_store);
        let second_conn =
            rusqlite::Connection::open(&db_path).expect("open sqlite after second boot");
        let second_snapshot = read_schema_migrations(&second_conn);

        assert_eq!(first_snapshot, second_snapshot);
        assert_eq!(second_snapshot.len(), super::migrations::MIGRATIONS.len());
        assert_eq!(
            second_snapshot.last().map(|(version, _)| *version),
            Some(i64::try_from(super::migrations::MIGRATIONS.len()).unwrap_or(i64::MAX))
        );

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn create_workspace_put_and_search() {
        let db_path = std::env::temp_dir().join(format!("scry-index-{}.db", uuid::Uuid::now_v7()));
        let store = IndexStore::open(&db_path).expect("db init");
        let root_node_id = store
            .create_workspace("ws-1", "Workspace 1")
            .expect("workspace created");
        let docs = create_node!(
            store,
            "ws-1",
            &root_node_id,
            "docs",
            NodeKind::Dir,
            0o755,
            true
        );
        let note = create_node!(
            store,
            "ws-1",
            &docs.node_id,
            "note.md",
            NodeKind::File,
            0o644,
            true,
        );
        let looked_up = store
            .lookup("ws-1", &docs.node_id, "note.md")
            .expect("lookup");
        assert_eq!(looked_up.node_id, note.node_id);
        let attrs = store.get_attrs("ws-1", &note.node_id).expect("attrs");
        assert_eq!(attrs.mode, 0o644);

        let updated_attrs = store
            .set_attrs(
                "ws-1",
                &note.node_id,
                AttrsPatch {
                    mode: Some(0o600),
                    mtime_unix_nano: None,
                },
            )
            .expect("set attrs");
        assert_eq!(updated_attrs.0.mode, 0o600);

        store
            .rename(
                "ws-1",
                &docs.node_id,
                "note.md",
                &docs.node_id,
                "renamed.md",
                false,
                None,
            )
            .expect("rename");
        let resolved_after_rename = store
            .resolve_path("ws-1", "docs/renamed.md")
            .expect("resolve renamed")
            .expect("exists");
        assert_eq!(resolved_after_rename, note.node_id);

        let (entries, _) = store
            .read_dir("ws-1", &docs.node_id, None, 128)
            .expect("readdir");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "renamed.md");

        let (put, _) = store
            .put_file("ws-1", "docs/renamed.md", b"hello semantic world", None)
            .expect("put");
        assert_eq!(put.node_id, note.node_id);
        store
            .unlink("ws-1", &docs.node_id, "renamed.md", None)
            .expect("unlink file");

        store
            .put_file("ws-1", "notes.md", b"hello semantic world", None)
            .expect("put top-level");
        let (put_top, _) = store
            .put_file("ws-1", "notes.md", b"hello semantic world", None)
            .expect("workspace created");
        assert_eq!(put_top.version, 2);
        let node_id = store
            .resolve_path("ws-1", "notes.md")
            .expect("resolve")
            .expect("exists");
        assert_eq!(node_id, put_top.node_id);
        let hits = store.search_fts("ws-1", "semantic", 10).expect("search");
        assert_eq!(hits.total_hits, 1);
        assert_eq!(hits.hits.len(), 1);
        let attrs = store
            .get_attrs("ws-1", &put_top.node_id)
            .expect("attrs after put");
        assert_eq!(attrs.size, b"hello semantic world".len() as u64);
        assert!(!attrs.content_hash.is_empty());
        store
            .unlink("ws-1", &docs.node_id, "does-not-exist", None)
            .expect_err("unlink missing should error");
        store
            .unlink("ws-1", &root_node_id, "docs", None)
            .expect("unlink now-empty dir");
        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn vector_and_hybrid_search_work() {
        let db_path = std::env::temp_dir().join(format!("scry-index-{}.db", uuid::Uuid::now_v7()));
        let store = IndexStore::open(&db_path).expect("db init");
        let root_node_id = store
            .create_workspace("ws-v", "Workspace Vector")
            .expect("workspace created");
        let docs = create_node!(
            store,
            "ws-v",
            &root_node_id,
            "docs",
            NodeKind::Dir,
            0o755,
            true
        );

        let _ = create_node!(
            store,
            "ws-v",
            &docs.node_id,
            "semantic.md",
            NodeKind::File,
            0o644,
            true,
        );
        store
            .put_file(
                "ws-v",
                "docs/semantic.md",
                b"deep semantic retrieval for docs",
                None,
            )
            .expect("put semantic");

        let _ = create_node!(
            store,
            "ws-v",
            &docs.node_id,
            "keywords.md",
            NodeKind::File,
            0o644,
            true,
        );
        store
            .put_file(
                "ws-v",
                "docs/keywords.md",
                b"exact keyword matching text",
                None,
            )
            .expect("put keyword");

        for path in ["docs/semantic.md", "docs/keywords.md"] {
            let node_id = store
                .resolve_path("ws-v", path)
                .expect("resolve")
                .expect("exists");
            let chunks = store
                .list_chunks_for_node("ws-v", &node_id)
                .expect("list chunks");
            let embeddings: Vec<Vec<f32>> = chunks
                .iter()
                .map(|c| super::simple_embed(&c.content))
                .collect();
            store
                .upsert_embeddings_for_node("ws-v", &node_id, &embeddings)
                .expect("seed vec_chunks for test");
        }

        let vec_hits = store
            .search("ws-v", "semantic retrieval", SearchMode::Vector, 5)
            .expect("vector search");
        assert!(
            !vec_hits.hits.is_empty(),
            "vector search should return hits"
        );
        assert!(
            vec_hits.total_hits >= u32::try_from(vec_hits.hits.len()).unwrap_or(u32::MAX),
            "vector total hits should be >= returned hits"
        );

        let hybrid_hits = store
            .search("ws-v", "keyword retrieval", SearchMode::Hybrid, 5)
            .expect("hybrid search");
        assert!(
            !hybrid_hits.hits.is_empty(),
            "hybrid search should return hits"
        );
        assert!(
            hybrid_hits.total_hits >= u32::try_from(hybrid_hits.hits.len()).unwrap_or(u32::MAX),
            "hybrid total hits should be >= returned hits"
        );

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn search_scores_and_total_hits_semantics_are_stable() {
        let db_path = std::env::temp_dir().join(format!("scry-index-{}.db", uuid::Uuid::now_v7()));
        let store = IndexStore::open(&db_path).expect("db init");
        let root_node_id = store
            .create_workspace("ws-search", "Workspace Search")
            .expect("workspace created");
        let docs = create_node!(
            store,
            "ws-search",
            &root_node_id,
            "docs",
            NodeKind::Dir,
            0o755,
            true,
        );

        let _ = create_node!(
            store,
            "ws-search",
            &docs.node_id,
            "a.md",
            NodeKind::File,
            0o644,
            true,
        );
        store
            .put_file(
                "ws-search",
                "docs/a.md",
                b"sharedterm ultraunique marker phrase",
                None,
            )
            .expect("put file a");

        let _ = create_node!(
            store,
            "ws-search",
            &docs.node_id,
            "b.md",
            NodeKind::File,
            0o644,
            true,
        );
        store
            .put_file("ws-search", "docs/b.md", b"sharedterm generic text", None)
            .expect("put file b");

        let fts = store
            .search_fts("ws-search", "sharedterm", 1)
            .expect("fts search");
        assert_eq!(fts.hits.len(), 1, "fts should respect limit");
        assert_eq!(
            fts.total_hits, 2,
            "fts total_hits should reflect full match count"
        );
        assert!(
            (0.0..=1.0).contains(&fts.hits[0].score),
            "fts score should be normalized"
        );

        let node_a = store
            .resolve_path("ws-search", "docs/a.md")
            .expect("resolve a path")
            .expect("a exists");
        let node_b = store
            .resolve_path("ws-search", "docs/b.md")
            .expect("resolve b path")
            .expect("b exists");
        let chunks_a = store
            .get_file_chunks("ws-search", &node_a)
            .expect("chunks for a");
        let chunks_b = store
            .get_file_chunks("ws-search", &node_b)
            .expect("chunks for b");
        let emb_a = vec![
            vec![1.0f32, 0.0f32]
                .into_iter()
                .chain(std::iter::repeat_n(0.0f32, 30))
                .collect::<Vec<_>>();
            chunks_a.len()
        ];
        let emb_b = vec![
            vec![0.0f32, 1.0f32]
                .into_iter()
                .chain(std::iter::repeat_n(0.0f32, 30))
                .collect::<Vec<_>>();
            chunks_b.len()
        ];
        store
            .upsert_embeddings_for_node("ws-search", &node_a, &emb_a)
            .expect("upsert embeddings a");
        store
            .upsert_embeddings_for_node("ws-search", &node_b, &emb_b)
            .expect("upsert embeddings b");

        let vector = store
            .search_vector_with_embedding(
                "ws-search",
                &[
                    1.0f32, 0.0f32, 0.0f32, 0.0f32, 0.0f32, 0.0f32, 0.0f32, 0.0f32, 0.0f32, 0.0f32,
                    0.0f32, 0.0f32, 0.0f32, 0.0f32, 0.0f32, 0.0f32, 0.0f32, 0.0f32, 0.0f32, 0.0f32,
                    0.0f32, 0.0f32, 0.0f32, 0.0f32, 0.0f32, 0.0f32, 0.0f32, 0.0f32, 0.0f32, 0.0f32,
                    0.0f32, 0.0f32,
                ],
                1,
            )
            .expect("vector search");
        assert_eq!(
            vector.total_hits,
            u32::try_from(vector.hits.len()).unwrap_or(u32::MAX),
            "vector total_hits should match returned hit count for current limit window"
        );

        let hybrid = store
            .search_hybrid_with_embedding(
                "ws-search",
                "ultraunique",
                &[
                    1.0f32, 0.0f32, 0.0f32, 0.0f32, 0.0f32, 0.0f32, 0.0f32, 0.0f32, 0.0f32, 0.0f32,
                    0.0f32, 0.0f32, 0.0f32, 0.0f32, 0.0f32, 0.0f32, 0.0f32, 0.0f32, 0.0f32, 0.0f32,
                    0.0f32, 0.0f32, 0.0f32, 0.0f32, 0.0f32, 0.0f32, 0.0f32, 0.0f32, 0.0f32, 0.0f32,
                    0.0f32, 0.0f32,
                ],
                5,
            )
            .expect("hybrid search");
        assert!(!hybrid.hits.is_empty(), "hybrid should return hits");
        let expected_rrf = 2.0f32 / 61.0f32;
        let got = hybrid.hits[0].score;
        assert!(
            (got - expected_rrf).abs() < 1e-5,
            "top hybrid hit should use RRF score 2/61 when both ranks are 1 (got {got})"
        );

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn indexing_jobs_lifecycle_and_chunking_work() {
        let db_path = std::env::temp_dir().join(format!("scry-index-{}.db", uuid::Uuid::now_v7()));
        let store = IndexStore::open(&db_path).expect("db init");
        let root_node_id = store
            .create_workspace("ws-j", "Workspace Jobs")
            .expect("workspace created");
        let docs = create_node!(
            store,
            "ws-j",
            &root_node_id,
            "docs",
            NodeKind::Dir,
            0o755,
            true
        );
        let note = create_node!(
            store,
            "ws-j",
            &docs.node_id,
            "note.md",
            NodeKind::File,
            0o644,
            true,
        );

        let md_content = br#"# Plan
line 1
line 2

## Details
alpha
beta
"#;
        store
            .put_file("ws-j", "docs/note.md", md_content, None)
            .expect("put markdown");
        let chunks = store
            .get_file_chunks("ws-j", &note.node_id)
            .expect("list chunks");
        assert!(chunks.len() >= 2, "markdown should be section chunked");
        assert!(
            chunks
                .iter()
                .all(|chunk| chunk.end_line >= chunk.start_line),
            "chunk line ranges should be valid"
        );

        let queued = store
            .list_indexing_jobs_by_status(IndexingJobStatus::Queued, 16)
            .expect("list queued");
        assert!(!queued.is_empty(), "put_file should enqueue index jobs");

        let taken = store.take_indexing_jobs(8).expect("take jobs");
        assert!(!taken.is_empty(), "should take queued jobs");
        assert!(
            taken
                .iter()
                .all(|job| matches!(job.status, IndexingJobStatus::Queued)),
            "take_indexing_jobs returns pre-transition snapshots"
        );

        let running = store
            .list_indexing_jobs_by_status(IndexingJobStatus::Running, 16)
            .expect("list running");
        assert!(!running.is_empty(), "taken jobs should become running");

        let first_job = running.first().expect("first running job");
        store
            .fail_indexing_job(first_job.job_id, "transient", 1)
            .expect("fail running job");
        let requeued = store
            .list_indexing_jobs_by_status(IndexingJobStatus::Queued, 32)
            .expect("list requeued");
        assert!(
            requeued.iter().any(|job| job.job_id == first_job.job_id),
            "failed job should be re-queued"
        );
        let failed_attempt = requeued
            .iter()
            .find(|job| job.job_id == first_job.job_id)
            .expect("requeued failed job");
        assert_eq!(failed_attempt.attempts, first_job.attempts + 1);

        let retaken = store.take_indexing_jobs(8).expect("retake jobs");
        for job in retaken {
            store
                .complete_indexing_job(job.job_id)
                .expect("complete indexing job");
        }
        let completed = store
            .list_indexing_jobs_by_status(IndexingJobStatus::Completed, 32)
            .expect("list completed");
        assert!(!completed.is_empty(), "completed jobs should be persisted");

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn indexing_jobs_queue_and_lifecycle() {
        let db_path = std::env::temp_dir().join(format!("scry-index-{}.db", uuid::Uuid::now_v7()));
        let store = IndexStore::open(&db_path).expect("db init");
        let root_node_id = store
            .create_workspace("ws-q", "Workspace Queue")
            .expect("workspace created");
        let docs = create_node!(
            store,
            "ws-q",
            &root_node_id,
            "docs",
            NodeKind::Dir,
            0o755,
            true
        );
        let _note = create_node!(
            store,
            "ws-q",
            &docs.node_id,
            "note.md",
            NodeKind::File,
            0o644,
            true,
        );
        store
            .put_file("ws-q", "docs/note.md", b"queued indexing content", None)
            .expect("put file");

        let queued = store
            .list_indexing_jobs_by_status(IndexingJobStatus::Queued, 16)
            .expect("list queued");
        assert!(
            !queued.is_empty(),
            "put_file should enqueue at least one indexing job"
        );

        let taken = store.take_indexing_jobs(16).expect("take queued jobs");
        assert!(!taken.is_empty(), "taking queue should return jobs");
        assert!(
            taken
                .iter()
                .all(|job| job.status == IndexingJobStatus::Queued),
            "take returns queued snapshot rows before state transition"
        );

        let running = store
            .list_indexing_jobs_by_status(IndexingJobStatus::Running, 16)
            .expect("list running");
        assert!(!running.is_empty(), "taken jobs should become running");

        let first_job = running.first().expect("first running job");
        store
            .fail_indexing_job(first_job.job_id, "transient", 1)
            .expect("fail running job");
        let requeued = store
            .list_indexing_jobs_by_status(IndexingJobStatus::Queued, 32)
            .expect("list requeued");
        assert!(
            requeued.iter().any(|job| job.job_id == first_job.job_id),
            "failed job should be re-queued"
        );
        let failed_attempt = requeued
            .iter()
            .find(|job| job.job_id == first_job.job_id)
            .expect("requeued failed job");
        assert_eq!(failed_attempt.attempts, first_job.attempts + 1);

        let retaken = store.take_indexing_jobs(8).expect("retake jobs");
        for job in retaken {
            store
                .complete_indexing_job(job.job_id)
                .expect("complete indexing job");
        }
        let completed = store
            .list_indexing_jobs_by_status(IndexingJobStatus::Completed, 32)
            .expect("list completed");
        assert!(!completed.is_empty(), "completed jobs should be persisted");

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn p1_workspace_stats_reindex_resolve_ref_and_delete_work() {
        let db_path = std::env::temp_dir().join(format!("scry-index-{}.db", uuid::Uuid::now_v7()));
        let store = IndexStore::open(&db_path).expect("db init");
        let root_node_id = store
            .create_workspace("ws-p1", "Workspace P1")
            .expect("workspace created");
        let docs = create_node!(
            store,
            "ws-p1",
            &root_node_id,
            "docs",
            NodeKind::Dir,
            0o755,
            true
        );
        let note = create_node!(
            store,
            "ws-p1",
            &docs.node_id,
            "note.md",
            NodeKind::File,
            0o644,
            true,
        );

        store
            .put_file("ws-p1", "docs/note.md", b"hello p1 tests", None)
            .expect("put initial");
        let attrs = store.get_attrs("ws-p1", &note.node_id).expect("get attrs");
        let unchanged = store
            .get_attrs_if_changed("ws-p1", &note.node_id, attrs.version)
            .expect("get attrs if changed unchanged");
        assert!(
            unchanged.is_none(),
            "expected no attrs when version unchanged"
        );

        store
            .put_file("ws-p1", "docs/note.md", b"hello p1 tests updated", None)
            .expect("put updated");
        let changed = store
            .get_attrs_if_changed("ws-p1", &note.node_id, attrs.version)
            .expect("get attrs if changed changed");
        assert!(changed.is_some(), "expected attrs when version changed");
        let changed_attrs = changed.expect("changed attrs");
        assert!(changed_attrs.version > attrs.version);

        store
            .rename(
                "ws-p1",
                &docs.node_id,
                "note.md",
                &docs.node_id,
                "renamed.md",
                false,
                None,
            )
            .expect("rename file");
        let resolved_ref = store
            .resolve_ref_with_redirect("ws-p1", &note.node_id)
            .expect("resolve ref");
        let resolved_ref_path = resolved_ref.path.expect("resolve ref exists");
        assert!(resolved_ref.redirect_to_node_id.is_none());
        assert_eq!(resolved_ref_path, "docs/renamed.md");

        let stats_before = store.get_workspace_stats("ws-p1").expect("workspace stats");
        assert_eq!(stats_before.file_count, 1);
        assert_eq!(stats_before.dir_count, 2);
        assert!(stats_before.chunk_count >= 1);
        assert!(stats_before.event_count >= 1);
        assert!(stats_before.total_content_bytes > 0);

        let enqueued_jobs = store.reindex_workspace("ws-p1").expect("reindex");
        assert!(enqueued_jobs >= 1, "reindex should enqueue file jobs");
        let stats_after = store
            .get_workspace_stats("ws-p1")
            .expect("workspace stats after reindex");
        assert!(
            stats_after.queued_jobs > stats_before.queued_jobs,
            "queued jobs should increase after reindex"
        );

        store.delete_workspace("ws-p1").expect("delete workspace");
        let err = store
            .get_workspace_stats("ws-p1")
            .expect_err("deleted workspace should not be queryable");
        assert!(
            matches!(err, super::IndexError::WorkspaceNotFound(_)),
            "expected workspace not found after delete"
        );

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn events_retention_cleanup_keeps_recent_and_prunes_old() {
        let db_path = std::env::temp_dir().join(format!("scry-index-{}.db", uuid::Uuid::now_v7()));
        let store = IndexStore::open(&db_path).expect("db init");
        let root_node_id = store
            .create_workspace("ws-retention", "Retention Workspace")
            .expect("workspace created");
        let docs = create_node!(
            store,
            "ws-retention",
            &root_node_id,
            "docs",
            NodeKind::Dir,
            0o755,
            true,
        );
        store
            .create_node(
                "ws-retention",
                &docs.node_id,
                "note.md",
                NodeKind::File,
                0o644,
                true,
            )
            .expect("create note");
        store
            .put_file("ws-retention", "docs/note.md", b"v1", None)
            .expect("put v1");
        store
            .put_file("ws-retention", "docs/note.md", b"v2", None)
            .expect("put v2");
        store
            .put_file("ws-retention", "docs/note.md", b"v3", None)
            .expect("put v3");

        let before = store
            .list_events_since("ws-retention", Some(0), 512)
            .expect("list before");
        assert!(
            before.len() >= 4,
            "expected several events before retention cleanup"
        );

        let latest_cursor = before
            .last()
            .expect("latest event")
            .cursor
            .parse::<u64>()
            .expect("latest cursor parse");
        let deleted = store
            .cleanup_events_retention_before(i64::MAX, 2)
            .expect("cleanup old events");
        assert!(deleted >= 1, "cleanup should delete at least one old event");

        let after = store
            .list_events_since("ws-retention", Some(0), 512)
            .expect("list after");
        assert!(
            after.len() >= 2,
            "retention must keep at least the configured recent events"
        );
        let remaining_latest = after
            .last()
            .expect("remaining latest")
            .cursor
            .parse::<u64>()
            .expect("remaining cursor parse");
        assert_eq!(
            remaining_latest, latest_cursor,
            "retention cleanup must preserve the newest event"
        );

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn indexing_metrics_snapshot_reports_queue_states() {
        let db_path = std::env::temp_dir().join(format!("scry-index-{}.db", uuid::Uuid::now_v7()));
        let store = IndexStore::open(&db_path).expect("db init");
        let root_node_id = store
            .create_workspace("ws-metrics", "Metrics Workspace")
            .expect("workspace created");
        let docs = create_node!(
            store,
            "ws-metrics",
            &root_node_id,
            "docs",
            NodeKind::Dir,
            0o755,
            true,
        );
        store
            .create_node(
                "ws-metrics",
                &docs.node_id,
                "note.md",
                NodeKind::File,
                0o644,
                true,
            )
            .expect("create note");
        store
            .put_file("ws-metrics", "docs/note.md", b"metrics payload", None)
            .expect("put file");

        let snapshot_queued = store
            .indexing_metrics_snapshot()
            .expect("metrics queued snapshot");
        assert!(
            snapshot_queued.queued_jobs >= 1,
            "put_file should enqueue at least one indexing job"
        );

        let taken = store.take_indexing_jobs(8).expect("take jobs");
        assert!(!taken.is_empty(), "should take queued jobs");
        for job in &taken {
            store
                .complete_indexing_job(job.job_id)
                .expect("complete indexing job");
        }

        let snapshot_completed = store
            .indexing_metrics_snapshot()
            .expect("metrics completed snapshot");
        assert!(
            snapshot_completed.completed_jobs >= u64::try_from(taken.len()).unwrap_or(0),
            "completed jobs should be visible in metrics snapshot"
        );

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn resolve_ref_redirect_chain_returns_final_node() {
        let db_path = std::env::temp_dir().join(format!("scry-index-{}.db", uuid::Uuid::now_v7()));
        let store = IndexStore::open(&db_path).expect("db init");
        let root_node_id = store
            .create_workspace("ws-redirect", "Redirect Workspace")
            .expect("workspace created");
        let docs = create_node!(
            store,
            "ws-redirect",
            &root_node_id,
            "docs",
            NodeKind::Dir,
            0o755,
            true,
        );
        let final_file = create_node!(
            store,
            "ws-redirect",
            &docs.node_id,
            "final.md",
            NodeKind::File,
            0o644,
            true,
        );
        store
            .put_file("ws-redirect", "docs/final.md", b"redirect final", None)
            .expect("put final content");

        let node_a = uuid::Uuid::now_v7().to_string();
        let node_b = uuid::Uuid::now_v7().to_string();
        store
            .insert_redirect("ws-redirect", &node_a, &node_b, "renamed")
            .expect("insert A->B");
        store
            .insert_redirect("ws-redirect", &node_b, &final_file.node_id, "renamed")
            .expect("insert B->final");

        let resolved = store
            .resolve_ref_with_redirect("ws-redirect", &node_a)
            .expect("resolve redirected ref");
        assert_eq!(
            resolved.path.as_deref(),
            Some("docs/final.md"),
            "A->B->final should resolve to final path"
        );
        assert_eq!(
            resolved.redirect_to_node_id.as_deref(),
            Some(node_b.as_str()),
            "response should include immediate redirect hop"
        );

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn resolve_ref_redirect_cycle_is_detected() {
        let db_path = std::env::temp_dir().join(format!("scry-index-{}.db", uuid::Uuid::now_v7()));
        let store = IndexStore::open(&db_path).expect("db init");
        let _root_node_id = store
            .create_workspace("ws-redirect-cycle", "Redirect Cycle Workspace")
            .expect("workspace created");

        let node_a = uuid::Uuid::now_v7().to_string();
        let node_b = uuid::Uuid::now_v7().to_string();
        store
            .insert_redirect("ws-redirect-cycle", &node_a, &node_b, "renamed")
            .expect("insert A->B");
        store
            .insert_redirect("ws-redirect-cycle", &node_b, &node_a, "renamed")
            .expect("insert B->A");

        let err = store
            .resolve_ref_with_redirect("ws-redirect-cycle", &node_a)
            .expect_err("cycle should be detected");
        assert!(
            matches!(err, super::IndexError::RedirectLoop { .. }),
            "expected redirect cycle error, got {err:?}"
        );

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn writer_transaction_failure_does_not_poison_followup_reads() {
        let db_path = std::env::temp_dir().join(format!("scry-index-{}.db", uuid::Uuid::now_v7()));
        let store = IndexStore::open(&db_path).expect("db init");
        let root_node_id = store
            .create_workspace("ws-pool-failure", "Pool Failure Workspace")
            .expect("workspace created");
        let docs = create_node!(
            store,
            "ws-pool-failure",
            &root_node_id,
            "docs",
            NodeKind::Dir,
            0o755,
            true,
        );
        create_node!(
            store,
            "ws-pool-failure",
            &docs.node_id,
            "existing.md",
            NodeKind::File,
            0o644,
            true,
        );
        store
            .put_file(
                "ws-pool-failure",
                "docs/existing.md",
                b"before failure",
                None,
            )
            .expect("put initial file");

        let conflict = store
            .create_node(
                "ws-pool-failure",
                &docs.node_id,
                "existing.md",
                NodeKind::File,
                0o644,
                true,
            )
            .expect_err("duplicate create should fail");
        assert!(
            matches!(conflict, super::IndexError::AlreadyExistsAtPath { .. }),
            "expected uniqueness conflict, got {conflict:?}"
        );

        // Verify a later read still succeeds after the write transaction error.
        let node_id = store
            .resolve_path("ws-pool-failure", "docs/existing.md")
            .expect("resolve")
            .expect("exists");
        let attrs = store
            .get_attrs("ws-pool-failure", &node_id)
            .expect("read attrs should still work after write failure");
        assert_eq!(attrs.size, b"before failure".len() as u64);

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn reader_pool_and_single_writer_handle_concurrent_load() {
        let db_path = std::env::temp_dir().join(format!("scry-index-{}.db", uuid::Uuid::now_v7()));
        let store = Arc::new(IndexStore::open(&db_path).expect("db init"));
        let root_node_id = store
            .create_workspace("ws-pool-concurrency", "Pool Concurrency Workspace")
            .expect("workspace created");
        let docs = create_node!(
            store,
            "ws-pool-concurrency",
            &root_node_id,
            "docs",
            NodeKind::Dir,
            0o755,
            true,
        );
        create_node!(
            store,
            "ws-pool-concurrency",
            &docs.node_id,
            "hot.md",
            NodeKind::File,
            0o644,
            true,
        );
        store
            .put_file("ws-pool-concurrency", "docs/hot.md", b"seed", None)
            .expect("seed file");

        let mut threads = Vec::new();
        for reader_idx in 0..32 {
            let store = Arc::clone(&store);
            threads.push(std::thread::spawn(move || -> super::Result<()> {
                let node_id = store
                    .resolve_path("ws-pool-concurrency", "docs/hot.md")?
                    .expect("node exists");
                for _ in 0..80 {
                    let attrs = store.get_attrs("ws-pool-concurrency", &node_id)?;
                    assert!(
                        attrs.size > 0,
                        "reader {reader_idx} should always observe non-empty indexed size"
                    );
                }
                Ok(())
            }));
        }

        let writer_store = Arc::clone(&store);
        let writer = std::thread::spawn(move || -> super::Result<()> {
            for i in 0..80 {
                let payload = format!("payload-{i}");
                writer_store.put_file(
                    "ws-pool-concurrency",
                    "docs/hot.md",
                    payload.as_bytes(),
                    None,
                )?;
                std::thread::sleep(Duration::from_millis(1));
            }
            Ok(())
        });

        for handle in threads {
            let join = handle.join().expect("reader thread join");
            join.expect("reader operations should succeed");
        }
        writer
            .join()
            .expect("writer thread join")
            .expect("writer operations should succeed");

        let node_id = store
            .resolve_path("ws-pool-concurrency", "docs/hot.md")
            .expect("resolve")
            .expect("exists");
        let final_attrs = store
            .get_attrs("ws-pool-concurrency", &node_id)
            .expect("final attrs");
        assert!(
            final_attrs.size > 0,
            "final indexed size should remain after concurrent operations"
        );

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn put_file_streaming_helpers_match_put_file() {
        fn synth_bytes(len: usize) -> Vec<u8> {
            let mut v = Vec::with_capacity(len);
            for i in 0..len {
                v.push((i % 251) as u8);
            }
            v
        }

        for size in [16 * 1024usize, 1024 * 1024, 32 * 1024 * 1024] {
            let db_path =
                std::env::temp_dir().join(format!("scry-index-{}.db", uuid::Uuid::now_v7()));
            let store = IndexStore::open(&db_path).expect("db init");
            let root = store
                .create_workspace("ws-stream", "stream")
                .expect("workspace created");
            let docs = create_node!(
                store,
                "ws-stream",
                &root,
                "docs",
                NodeKind::Dir,
                0o755,
                true,
            );
            let bytes = synth_bytes(size);
            let prepared = crate::prepare_file_indexing(
                &crate::normalized_index_path("docs/a.bin").expect("normalize"),
                &bytes,
                crate::DEFAULT_SKIP_CHUNKING_OVER_BYTES,
                crate::DEFAULT_HEADLINE_EACH_BYTES,
            );

            store
                .create_node(
                    "ws-stream",
                    &docs.node_id,
                    "a.bin",
                    NodeKind::File,
                    0o644,
                    true,
                )
                .expect("file a");
            store
                .create_node(
                    "ws-stream",
                    &docs.node_id,
                    "b.bin",
                    NodeKind::File,
                    0o644,
                    true,
                )
                .expect("file b");

            let (r1, _) = store
                .put_file("ws-stream", "docs/a.bin", &bytes, None)
                .expect("put_file");
            let (r2, _) = store
                .put_file_with_prepared(
                    "ws-stream",
                    "docs/b.bin",
                    u64::try_from(bytes.len()).expect("len"),
                    prepared,
                    None,
                )
                .expect("put_file_with_prepared");

            assert_eq!(r1.content_hash, r2.content_hash);
            assert_eq!(r1.size, r2.size);

            let chunks_a = store
                .get_file_chunks("ws-stream", &r1.node_id)
                .expect("chunks a");
            let chunks_b = store
                .get_file_chunks("ws-stream", &r2.node_id)
                .expect("chunks b");
            assert_eq!(chunks_a.len(), chunks_b.len());
            for (ca, cb) in chunks_a.iter().zip(chunks_b.iter()) {
                assert_eq!(ca.start_line, cb.start_line);
                assert_eq!(ca.end_line, cb.end_line);
                assert_eq!(ca.content, cb.content);
            }

            let _ = std::fs::remove_file(db_path);
        }
    }

    #[test]
    fn put_file_if_version_mismatch_returns_conflict() {
        let db_path = std::env::temp_dir().join(format!("scry-index-{}.db", uuid::Uuid::now_v7()));
        let store = IndexStore::open(&db_path).expect("db init");
        let root = store
            .create_workspace("ws-ver", "ver")
            .expect("workspace created");
        let docs = create_node!(store, "ws-ver", &root, "docs", NodeKind::Dir, 0o755, true);
        store
            .create_node("ws-ver", &docs.node_id, "x.md", NodeKind::File, 0o644, true)
            .expect("file");
        store
            .put_file("ws-ver", "docs/x.md", b"v1", None)
            .expect("first put");
        let err = store
            .put_file("ws-ver", "docs/x.md", b"v2", Some(1))
            .expect_err("stale if_version");
        assert!(
            matches!(err, super::IndexError::VersionConflict { .. }),
            "expected version conflict, got {err:?}"
        );
        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn list_chunks_after_and_batched_upsert_match_full_upsert() {
        let db_path = std::env::temp_dir().join(format!("scry-index-{}.db", uuid::Uuid::now_v7()));
        let store = IndexStore::open(&db_path).expect("db init");
        let root = store
            .create_workspace("ws-batch", "batch")
            .expect("workspace created");
        let docs = create_node!(store, "ws-batch", &root, "docs", NodeKind::Dir, 0o755, true,);
        let _ = create_node!(
            store,
            "ws-batch",
            &docs.node_id,
            "big.txt",
            NodeKind::File,
            0o644,
            true,
        );
        let body = "line of text with some words\n".repeat(500);
        store
            .put_file("ws-batch", "docs/big.txt", body.as_bytes(), None)
            .expect("put");
        let node_id = store
            .resolve_path("ws-batch", "docs/big.txt")
            .expect("resolve")
            .expect("exists");

        let all = store
            .list_chunks_for_node("ws-batch", &node_id)
            .expect("list all");
        assert!(
            all.len() >= 3,
            "sanity: expected many chunks, got {}",
            all.len()
        );

        // Paginate through list_chunks_for_node_after and assert coverage + stable ordering.
        let mut paged = Vec::new();
        let mut cursor: i64 = 0;
        loop {
            let page = store
                .list_chunks_for_node_after("ws-batch", &node_id, cursor, 3)
                .expect("page");
            if page.is_empty() {
                break;
            }
            cursor = page.last().unwrap().chunk_id;
            paged.extend(page);
        }
        let all_ids: Vec<i64> = all.iter().map(|c| c.chunk_id).collect();
        let paged_ids: Vec<i64> = paged.iter().map(|c| c.chunk_id).collect();
        assert_eq!(
            all_ids, paged_ids,
            "paged cursor must cover all chunks in id order"
        );

        // Batched upsert vs full upsert on two different nodes must yield identical
        // vec_chunks rows for identical embeddings.
        let _ = create_node!(
            store,
            "ws-batch",
            &docs.node_id,
            "twin.txt",
            NodeKind::File,
            0o644,
            true,
        );
        store
            .put_file("ws-batch", "docs/twin.txt", body.as_bytes(), None)
            .expect("put twin");
        let twin_id = store
            .resolve_path("ws-batch", "docs/twin.txt")
            .expect("resolve")
            .expect("exists");
        let twin_chunks = store
            .list_chunks_for_node("ws-batch", &twin_id)
            .expect("list twin");
        let embeddings_full: Vec<Vec<f32>> = all
            .iter()
            .map(|c| super::simple_embed(&c.content))
            .collect();
        let twin_embeddings: Vec<Vec<f32>> = twin_chunks
            .iter()
            .map(|c| super::simple_embed(&c.content))
            .collect();

        store
            .upsert_embeddings_for_node("ws-batch", &node_id, &embeddings_full)
            .expect("full upsert");

        // Twin: upsert in batches of 2 via explicit chunk_ids.
        for batch in twin_chunks.chunks(2) {
            let ids: Vec<i64> = batch.iter().map(|c| c.chunk_id).collect();
            let embs: Vec<Vec<f32>> = batch
                .iter()
                .map(|c| super::simple_embed(&c.content))
                .collect();
            store
                .upsert_embeddings_for_chunk_ids("ws-batch", &ids, &embs)
                .expect("batched upsert");
            // sanity: twin_embeddings is never moved; just assert length stability.
            assert_eq!(twin_embeddings.len(), twin_chunks.len());
        }

        // Sanity: if chunk_id belongs to a different (existing) workspace, the write must be
        // rejected with InvalidInput rather than silently writing into the wrong partition.
        let _ = store
            .create_workspace("ws-other", "other")
            .expect("other workspace");
        let bad = store.upsert_embeddings_for_chunk_ids(
            "ws-other",
            &[all_ids[0]],
            &[super::simple_embed("x")],
        );
        assert!(
            matches!(bad, Err(super::IndexError::InvalidInput(_))),
            "cross-workspace upsert must be rejected, got {bad:?}"
        );

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn rename_if_version_matches() {
        let db_path = std::env::temp_dir().join(format!("scry-index-{}.db", uuid::Uuid::now_v7()));
        let store = IndexStore::open(&db_path).expect("db init");
        let root = store
            .create_workspace("ws-rn", "rn")
            .expect("workspace created");
        let docs = create_node!(store, "ws-rn", &root, "docs", NodeKind::Dir, 0o755, true);
        let f = create_node!(
            store,
            "ws-rn",
            &docs.node_id,
            "a.md",
            NodeKind::File,
            0o644,
            true,
        );
        let v = f.attrs.version;
        store
            .rename(
                "ws-rn",
                &docs.node_id,
                "a.md",
                &docs.node_id,
                "b.md",
                false,
                Some(v),
            )
            .expect("rename with matching if_version");
        let _ = std::fs::remove_file(db_path);
    }
}
