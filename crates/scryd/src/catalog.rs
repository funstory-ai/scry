use std::{path::Path, sync::Arc, time::SystemTime};

use anyhow::Context;
use async_trait::async_trait;
use tokio::task::JoinHandle;
use tracing::warn;

use crate::server::index_backend::IndexBackend;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct WorkspaceCatalogRecord {
    pub workspace_id: String,
    pub name: String,
    pub root_node_id: String,
    pub created_at_unix_nano: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogBackendKind {
    /// Standalone SQLite catalog file (default). Holds only the `workspace_catalog`
    /// table and is independent of any per-workspace index database.
    Sqlite,
    Postgres,
}

#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("workspace already exists: {workspace_id}")]
    WorkspaceAlreadyExists { workspace_id: String },
    #[error("workspace not found: {workspace_id}")]
    WorkspaceNotFound { workspace_id: String },
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[async_trait]
pub trait WorkspaceCatalog: Send + Sync {
    #[allow(dead_code)]
    fn backend_kind(&self) -> CatalogBackendKind;

    async fn register_workspace(
        &self,
        workspace_id: &str,
        name: &str,
        root_node_id: &str,
    ) -> Result<(), CatalogError>;

    async fn delete_workspace(&self, workspace_id: &str) -> Result<(), CatalogError>;

    #[allow(dead_code)]
    async fn get_workspace(
        &self,
        workspace_id: &str,
    ) -> Result<Option<WorkspaceCatalogRecord>, CatalogError>;

    async fn list_workspaces(
        &self,
        cursor_workspace_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<WorkspaceCatalogRecord>, CatalogError>;
}

#[derive(Debug, Clone)]
pub struct CatalogConfig {
    pub backend: CatalogBackendKind,
    /// Standalone SQLite catalog file path (required for `Sqlite` backend).
    pub sqlite_db_path: Option<std::path::PathBuf>,
    pub postgres_url: Option<String>,
}

pub type CatalogBuildResult = (Arc<dyn WorkspaceCatalog>, Option<JoinHandle<()>>);

impl CatalogConfig {
    /// Resolve catalog config from env vars. `index_root` provides a default location
    /// for the standalone catalog DB if `SCRYD_CATALOG_DB_PATH` is unset.
    pub fn from_env(index_root: &Path) -> anyhow::Result<Self> {
        let backend = match std::env::var("SCRYD_CATALOG_BACKEND") {
            Ok(raw) => match raw.trim().to_ascii_lowercase().as_str() {
                "sqlite" => CatalogBackendKind::Sqlite,
                "postgres" | "postgresql" => CatalogBackendKind::Postgres,
                _ => anyhow::bail!("invalid SCRYD_CATALOG_BACKEND: {raw}"),
            },
            Err(std::env::VarError::NotPresent) => CatalogBackendKind::Sqlite,
            Err(std::env::VarError::NotUnicode(_)) => {
                anyhow::bail!("SCRYD_CATALOG_BACKEND must be valid unicode");
            }
        };
        let sqlite_db_path = match std::env::var("SCRYD_CATALOG_DB_PATH") {
            Ok(raw) => {
                let trimmed = raw.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(std::path::PathBuf::from(trimmed))
                }
            }
            Err(std::env::VarError::NotPresent) => None,
            Err(std::env::VarError::NotUnicode(_)) => {
                anyhow::bail!("SCRYD_CATALOG_DB_PATH must be valid unicode");
            }
        };
        let postgres_url = match std::env::var("SCRYD_CATALOG_POSTGRES_URL") {
            Ok(raw) => {
                let trimmed = raw.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_string())
                }
            }
            Err(std::env::VarError::NotPresent) => None,
            Err(std::env::VarError::NotUnicode(_)) => {
                anyhow::bail!("SCRYD_CATALOG_POSTGRES_URL must be valid unicode");
            }
        };
        if backend == CatalogBackendKind::Postgres && postgres_url.is_none() {
            anyhow::bail!(
                "SCRYD_CATALOG_POSTGRES_URL is required when SCRYD_CATALOG_BACKEND=postgres"
            );
        }
        let sqlite_db_path = if backend == CatalogBackendKind::Sqlite {
            Some(sqlite_db_path.unwrap_or_else(|| index_root.join("_catalog.db")))
        } else {
            sqlite_db_path
        };
        Ok(Self {
            backend,
            sqlite_db_path,
            postgres_url,
        })
    }
}

pub fn build_catalog(config: &CatalogConfig) -> anyhow::Result<CatalogBuildResult> {
    match config.backend {
        CatalogBackendKind::Sqlite => {
            let path = config
                .sqlite_db_path
                .clone()
                .context("missing catalog sqlite db path")?;
            Ok((Arc::new(SqliteCatalog::new(&path)?), None))
        }
        CatalogBackendKind::Postgres => {
            let url = config
                .postgres_url
                .clone()
                .context("missing postgres url for catalog")?;
            let (catalog, bg_task) = PostgresCatalog::new(url)?;
            Ok((Arc::new(catalog), Some(bg_task)))
        }
    }
}

struct SqliteCatalog {
    db_path: std::path::PathBuf,
}

impl SqliteCatalog {
    fn new(db_path: &Path) -> anyhow::Result<Self> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create catalog parent directory {}", parent.display()))?;
        }
        let this = Self {
            db_path: db_path.to_path_buf(),
        };
        this.ensure_schema()?;
        Ok(this)
    }

    fn connect(&self) -> Result<rusqlite::Connection, CatalogError> {
        rusqlite::Connection::open(&self.db_path)
            .with_context(|| format!("open catalog sqlite {}", self.db_path.display()))
            .map_err(CatalogError::Other)
    }

    fn ensure_schema(&self) -> Result<(), CatalogError> {
        let conn = self.connect()?;
        conn.execute_batch(
            r#"
CREATE TABLE IF NOT EXISTS workspace_catalog (
  workspace_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  root_node_id TEXT NOT NULL,
  created_at_ns INTEGER NOT NULL
);
"#,
        )
        .map_err(|e| CatalogError::Other(e.into()))?;
        Ok(())
    }
}

#[async_trait]
impl WorkspaceCatalog for SqliteCatalog {
    fn backend_kind(&self) -> CatalogBackendKind {
        CatalogBackendKind::Sqlite
    }

    async fn register_workspace(
        &self,
        workspace_id: &str,
        name: &str,
        root_node_id: &str,
    ) -> Result<(), CatalogError> {
        let conn = self.connect()?;
        let inserted = conn
            .execute(
                "INSERT OR IGNORE INTO workspace_catalog(workspace_id, name, root_node_id, created_at_ns)
                 VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![workspace_id, name, root_node_id, now_unix_nano()],
            )
            .map_err(|e| CatalogError::Other(e.into()))?;
        if inserted == 0 {
            return Err(CatalogError::WorkspaceAlreadyExists {
                workspace_id: workspace_id.to_string(),
            });
        }
        Ok(())
    }

    async fn delete_workspace(&self, workspace_id: &str) -> Result<(), CatalogError> {
        let conn = self.connect()?;
        let deleted = conn
            .execute(
                "DELETE FROM workspace_catalog WHERE workspace_id = ?1",
                rusqlite::params![workspace_id],
            )
            .map_err(|e| CatalogError::Other(e.into()))?;
        if deleted == 0 {
            return Err(CatalogError::WorkspaceNotFound {
                workspace_id: workspace_id.to_string(),
            });
        }
        Ok(())
    }

    async fn get_workspace(
        &self,
        workspace_id: &str,
    ) -> Result<Option<WorkspaceCatalogRecord>, CatalogError> {
        use rusqlite::OptionalExtension;

        let conn = self.connect()?;
        let mut stmt = conn
            .prepare(
                "SELECT workspace_id, name, root_node_id, created_at_ns
                 FROM workspace_catalog
                 WHERE workspace_id = ?1
                 LIMIT 1",
            )
            .map_err(|e| CatalogError::Other(e.into()))?;
        let row = stmt
            .query_row(rusqlite::params![workspace_id], |row| {
                Ok(WorkspaceCatalogRecord {
                    workspace_id: row.get(0)?,
                    name: row.get(1)?,
                    root_node_id: row.get(2)?,
                    created_at_unix_nano: row.get(3)?,
                })
            })
            .optional()
            .map_err(|e| CatalogError::Other(e.into()))?;
        Ok(row)
    }

    async fn list_workspaces(
        &self,
        cursor_workspace_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<WorkspaceCatalogRecord>, CatalogError> {
        let conn = self.connect()?;
        let sql = if cursor_workspace_id.is_some() {
            "SELECT workspace_id, name, root_node_id, created_at_ns
             FROM workspace_catalog
             WHERE workspace_id > ?1
             ORDER BY workspace_id
             LIMIT ?2"
        } else {
            "SELECT workspace_id, name, root_node_id, created_at_ns
             FROM workspace_catalog
             ORDER BY workspace_id
             LIMIT ?1"
        };
        let mut stmt = conn
            .prepare(sql)
            .map_err(|e| CatalogError::Other(e.into()))?;
        let mut out = Vec::new();
        if let Some(cursor) = cursor_workspace_id {
            let rows = stmt
                .query_map(
                    rusqlite::params![cursor, i64::from(limit)],
                    map_workspace_catalog_row,
                )
                .map_err(|e| CatalogError::Other(e.into()))?;
            for row in rows {
                out.push(row.map_err(|e| CatalogError::Other(e.into()))?);
            }
        } else {
            let rows = stmt
                .query_map(
                    rusqlite::params![i64::from(limit)],
                    map_workspace_catalog_row,
                )
                .map_err(|e| CatalogError::Other(e.into()))?;
            for row in rows {
                out.push(row.map_err(|e| CatalogError::Other(e.into()))?);
            }
        }
        Ok(out)
    }
}

struct PostgresCatalog {
    client: tokio_postgres::Client,
}

impl PostgresCatalog {
    fn new(url: String) -> anyhow::Result<(Self, JoinHandle<()>)> {
        let rt = tokio::runtime::Handle::try_current()
            .context("postgres catalog requires active tokio runtime")?;
        let (client, connection) = rt
            .block_on(tokio_postgres::connect(&url, tokio_postgres::NoTls))
            .with_context(|| "connect postgres catalog")?;
        let bg = tokio::spawn(async move {
            if let Err(err) = connection.await {
                warn!(error = %err, "postgres catalog background connection terminated");
            }
        });
        let catalog = Self { client };
        rt.block_on(catalog.ensure_schema())?;
        Ok((catalog, bg))
    }

    async fn ensure_schema(&self) -> anyhow::Result<()> {
        self.client
            .batch_execute(
                r#"
CREATE TABLE IF NOT EXISTS workspace_catalog (
  workspace_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  root_node_id TEXT NOT NULL,
  created_at_ns BIGINT NOT NULL
);
"#,
            )
            .await
            .context("ensure postgres workspace_catalog schema")?;
        Ok(())
    }
}

#[async_trait]
impl WorkspaceCatalog for PostgresCatalog {
    fn backend_kind(&self) -> CatalogBackendKind {
        CatalogBackendKind::Postgres
    }

    async fn register_workspace(
        &self,
        workspace_id: &str,
        name: &str,
        root_node_id: &str,
    ) -> Result<(), CatalogError> {
        let inserted = self
            .client
            .execute(
                "INSERT INTO workspace_catalog(workspace_id, name, root_node_id, created_at_ns)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (workspace_id) DO NOTHING",
                &[&workspace_id, &name, &root_node_id, &now_unix_nano()],
            )
            .await
            .map_err(|e| CatalogError::Other(e.into()))?;
        if inserted == 0 {
            return Err(CatalogError::WorkspaceAlreadyExists {
                workspace_id: workspace_id.to_string(),
            });
        }
        Ok(())
    }

    async fn delete_workspace(&self, workspace_id: &str) -> Result<(), CatalogError> {
        let deleted = self
            .client
            .execute(
                "DELETE FROM workspace_catalog WHERE workspace_id = $1",
                &[&workspace_id],
            )
            .await
            .map_err(|e| CatalogError::Other(e.into()))?;
        if deleted == 0 {
            return Err(CatalogError::WorkspaceNotFound {
                workspace_id: workspace_id.to_string(),
            });
        }
        Ok(())
    }

    async fn get_workspace(
        &self,
        workspace_id: &str,
    ) -> Result<Option<WorkspaceCatalogRecord>, CatalogError> {
        let row = self
            .client
            .query_opt(
                "SELECT workspace_id, name, root_node_id, created_at_ns
                 FROM workspace_catalog
                 WHERE workspace_id = $1",
                &[&workspace_id],
            )
            .await
            .map_err(|e| CatalogError::Other(e.into()))?;
        Ok(row.map(|row| WorkspaceCatalogRecord {
            workspace_id: row.get(0),
            name: row.get(1),
            root_node_id: row.get(2),
            created_at_unix_nano: row.get(3),
        }))
    }

    async fn list_workspaces(
        &self,
        cursor_workspace_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<WorkspaceCatalogRecord>, CatalogError> {
        let rows = if let Some(cursor) = cursor_workspace_id {
            self.client
                .query(
                    "SELECT workspace_id, name, root_node_id, created_at_ns
                     FROM workspace_catalog
                     WHERE workspace_id > $1
                     ORDER BY workspace_id
                     LIMIT $2",
                    &[&cursor, &i64::from(limit)],
                )
                .await
        } else {
            self.client
                .query(
                    "SELECT workspace_id, name, root_node_id, created_at_ns
                     FROM workspace_catalog
                     ORDER BY workspace_id
                     LIMIT $1",
                    &[&i64::from(limit)],
                )
                .await
        }
        .map_err(|e| CatalogError::Other(e.into()))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(WorkspaceCatalogRecord {
                workspace_id: row.get(0),
                name: row.get(1),
                root_node_id: row.get(2),
                created_at_unix_nano: row.get(3),
            });
        }
        Ok(out)
    }
}

fn now_unix_nano() -> i64 {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    i64::try_from(nanos).unwrap_or(i64::MAX)
}

fn map_workspace_catalog_row(
    row: &rusqlite::Row<'_>,
) -> std::result::Result<WorkspaceCatalogRecord, rusqlite::Error> {
    Ok(WorkspaceCatalogRecord {
        workspace_id: row.get(0)?,
        name: row.get(1)?,
        root_node_id: row.get(2)?,
        created_at_unix_nano: row.get(3)?,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogReconcileReport {
    pub checked_index_workspaces: u64,
    pub missing_in_catalog: u64,
    pub checked_catalog_workspaces: u64,
    pub stale_in_catalog: u64,
    pub repaired_added: u64,
    pub repaired_deleted: u64,
}

pub(crate) async fn reconcile_catalog_against_index(
    index_backend: &IndexBackend,
    catalog: &dyn WorkspaceCatalog,
    apply: bool,
) -> anyhow::Result<CatalogReconcileReport> {
    let index_rows = index_backend
        .list_all_workspace_summaries()
        .map_err(|e| anyhow::anyhow!(e))?;
    let mut index_by_id = std::collections::BTreeMap::<String, (String, String)>::new();
    for (workspace_id, name, root_node_id) in index_rows {
        index_by_id.insert(workspace_id, (name, root_node_id));
    }
    let catalog_rows = list_all_catalog_workspaces(catalog).await?;
    let mut catalog_by_id = std::collections::BTreeMap::<String, WorkspaceCatalogRecord>::new();
    for row in catalog_rows {
        catalog_by_id.insert(row.workspace_id.clone(), row);
    }

    let mut missing_in_catalog = 0u64;
    let mut stale_in_catalog = 0u64;
    let mut repaired_added = 0u64;
    let mut repaired_deleted = 0u64;

    for (workspace_id, (name, root_node_id)) in &index_by_id {
        if !catalog_by_id.contains_key(workspace_id) {
            missing_in_catalog = missing_in_catalog.saturating_add(1);
            if apply {
                catalog
                    .register_workspace(workspace_id, name, root_node_id)
                    .await
                    .with_context(|| {
                        format!("register missing catalog workspace {workspace_id}")
                    })?;
                repaired_added = repaired_added.saturating_add(1);
            }
        }
    }

    for workspace_id in catalog_by_id.keys() {
        if !index_by_id.contains_key(workspace_id) {
            stale_in_catalog = stale_in_catalog.saturating_add(1);
            if apply {
                catalog
                    .delete_workspace(workspace_id)
                    .await
                    .with_context(|| format!("delete stale catalog workspace {workspace_id}"))?;
                repaired_deleted = repaired_deleted.saturating_add(1);
            }
        }
    }

    Ok(CatalogReconcileReport {
        checked_index_workspaces: u64::try_from(index_by_id.len()).unwrap_or(u64::MAX),
        missing_in_catalog,
        checked_catalog_workspaces: u64::try_from(catalog_by_id.len()).unwrap_or(u64::MAX),
        stale_in_catalog,
        repaired_added,
        repaired_deleted,
    })
}

async fn list_all_catalog_workspaces(
    catalog: &dyn WorkspaceCatalog,
) -> Result<Vec<WorkspaceCatalogRecord>, CatalogError> {
    let mut cursor: Option<String> = None;
    let mut out = Vec::new();
    loop {
        let page = catalog.list_workspaces(cursor.as_deref(), 256).await?;
        if page.is_empty() {
            break;
        }
        cursor = page.last().map(|row| row.workspace_id.clone());
        out.extend(page);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::{
        build_catalog, reconcile_catalog_against_index, CatalogBackendKind, CatalogConfig,
        CatalogError,
    };
    use crate::server::index_backend::IndexBackend;

    fn sqlite_catalog_config(catalog_db: std::path::PathBuf) -> CatalogConfig {
        CatalogConfig {
            backend: CatalogBackendKind::Sqlite,
            sqlite_db_path: Some(catalog_db),
            postgres_url: None,
        }
    }

    #[tokio::test]
    async fn sqlite_catalog_register_get_delete_roundtrip() {
        let tmp = TempDir::new().expect("temp dir");
        let catalog_db = tmp.path().join("catalog.db");
        let (catalog, _task) =
            build_catalog(&sqlite_catalog_config(catalog_db)).expect("build sqlite catalog");

        catalog
            .register_workspace("ws-1", "Workspace 1", "root-node-1")
            .await
            .expect("register workspace");

        let row = catalog
            .get_workspace("ws-1")
            .await
            .expect("query workspace")
            .expect("workspace exists");
        assert_eq!(row.workspace_id, "ws-1");
        assert_eq!(row.name, "Workspace 1");
        assert_eq!(row.root_node_id, "root-node-1");

        catalog
            .delete_workspace("ws-1")
            .await
            .expect("delete workspace");
        let after = catalog
            .get_workspace("ws-1")
            .await
            .expect("query workspace after delete");
        assert!(after.is_none());
    }

    #[tokio::test]
    async fn sqlite_catalog_rejects_duplicate_workspace() {
        let tmp = TempDir::new().expect("temp dir");
        let catalog_db = tmp.path().join("catalog.db");
        let (catalog, _task) =
            build_catalog(&sqlite_catalog_config(catalog_db)).expect("build sqlite catalog");

        catalog
            .register_workspace("ws-dup", "Workspace Dup", "root-node-dup")
            .await
            .expect("first register succeeds");

        let err = catalog
            .register_workspace("ws-dup", "Workspace Dup 2", "root-node-dup-2")
            .await
            .expect_err("duplicate register should fail");
        assert!(matches!(
            err,
            CatalogError::WorkspaceAlreadyExists { workspace_id } if workspace_id == "ws-dup"
        ));
    }

    #[tokio::test]
    async fn sqlite_catalog_list_paginates_in_workspace_id_order() {
        let tmp = TempDir::new().expect("temp dir");
        let catalog_db = tmp.path().join("catalog.db");
        let (catalog, _task) =
            build_catalog(&sqlite_catalog_config(catalog_db)).expect("build sqlite catalog");

        catalog
            .register_workspace("ws-1", "Workspace 1", "root-1")
            .await
            .expect("register ws-1");
        catalog
            .register_workspace("ws-3", "Workspace 3", "root-3")
            .await
            .expect("register ws-3");
        catalog
            .register_workspace("ws-2", "Workspace 2", "root-2")
            .await
            .expect("register ws-2");

        let first = catalog
            .list_workspaces(None, 2)
            .await
            .expect("list first page");
        assert_eq!(first.len(), 2);
        assert_eq!(first[0].workspace_id, "ws-1");
        assert_eq!(first[1].workspace_id, "ws-2");

        let second = catalog
            .list_workspaces(Some(&first[1].workspace_id), 2)
            .await
            .expect("list second page");
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].workspace_id, "ws-3");
    }

    #[tokio::test]
    async fn reconcile_detects_and_repairs_catalog_drift() {
        let tmp = TempDir::new().expect("temp dir");
        let index_root = tmp.path().join("index");
        let backend = IndexBackend::open(index_root).expect("open per-workspace backend");
        let ws1_root = backend
            .create_workspace("ws-r1", "Workspace R1")
            .expect("create ws-r1");
        let _ws2_root = backend
            .create_workspace("ws-r2", "Workspace R2")
            .expect("create ws-r2");

        let catalog_db = tmp.path().join("catalog.db");
        let (catalog, _task) =
            build_catalog(&sqlite_catalog_config(catalog_db)).expect("build sqlite catalog");
        let catalog = Arc::clone(&catalog);
        catalog
            .register_workspace("ws-r1", "Workspace R1", &ws1_root)
            .await
            .expect("seed matching ws-r1");
        catalog
            .register_workspace("ws-stale", "Workspace Stale", "root-stale")
            .await
            .expect("seed stale workspace");

        let dry_run = reconcile_catalog_against_index(&backend, catalog.as_ref(), false)
            .await
            .expect("dry run reconcile");
        assert_eq!(dry_run.missing_in_catalog, 1);
        assert_eq!(dry_run.stale_in_catalog, 1);
        assert_eq!(dry_run.repaired_added, 0);
        assert_eq!(dry_run.repaired_deleted, 0);

        let applied = reconcile_catalog_against_index(&backend, catalog.as_ref(), true)
            .await
            .expect("apply reconcile");
        assert_eq!(applied.missing_in_catalog, 1);
        assert_eq!(applied.stale_in_catalog, 1);
        assert_eq!(applied.repaired_added, 1);
        assert_eq!(applied.repaired_deleted, 1);

        let after = reconcile_catalog_against_index(&backend, catalog.as_ref(), false)
            .await
            .expect("dry run after reconcile");
        assert_eq!(after.missing_in_catalog, 0);
        assert_eq!(after.stale_in_catalog, 0);
    }
}
