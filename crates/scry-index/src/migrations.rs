use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{params, Connection};

const CREATE_SCHEMA_MIGRATIONS_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER PRIMARY KEY,
  applied_at_ns INTEGER NOT NULL
);
"#;

pub(super) const MIGRATIONS: &[&str] = &[super::BOOTSTRAP_SQL];

pub(super) fn apply(conn: &mut Connection) -> rusqlite::Result<()> {
    conn.execute_batch(CREATE_SCHEMA_MIGRATIONS_SQL)?;

    let mut applied_versions = std::collections::BTreeSet::new();
    let mut stmt = conn.prepare("SELECT version FROM schema_migrations ORDER BY version ASC")?;
    let rows = stmt.query_map([], |row| row.get::<_, i64>(0))?;
    for row in rows {
        applied_versions.insert(row?);
    }
    drop(stmt);

    for (idx, migration_sql) in MIGRATIONS.iter().enumerate() {
        let version = i64::try_from(idx + 1).unwrap_or(i64::MAX);
        if applied_versions.contains(&version) {
            continue;
        }

        let tx = conn.transaction()?;
        tx.execute_batch(migration_sql)?;
        tx.execute(
            "INSERT INTO schema_migrations(version, applied_at_ns) VALUES (?1, ?2)",
            params![version, now_ns()],
        )?;
        tx.commit()?;
    }

    // Idempotent upgrade for older DB files: drop legacy blob table, add hash/mime columns.
    ensure_legacy_node_columns(conn)?;
    ensure_chunks_headline_column(conn)?;
    ensure_nodes_indexing_policy_column(conn)?;

    Ok(())
}

/// Drop `node_contents` and ensure `nodes.content_hash` / `nodes.mime` exist (no-op if already applied).
fn ensure_legacy_node_columns(conn: &mut Connection) -> rusqlite::Result<()> {
    conn.execute_batch("DROP TABLE IF EXISTS node_contents;")?;

    let mut stmt = conn.prepare("PRAGMA table_info(nodes)")?;
    let mut has_hash = false;
    let mut has_mime = false;
    let rows = stmt.query_map([], |row| {
        let name: String = row.get(1)?;
        Ok(name)
    })?;
    for row in rows {
        let name = row?;
        if name == "content_hash" {
            has_hash = true;
        }
        if name == "mime" {
            has_mime = true;
        }
    }
    drop(stmt);

    if !has_hash {
        conn.execute(
            "ALTER TABLE nodes ADD COLUMN content_hash TEXT NOT NULL DEFAULT ''",
            [],
        )?;
    }
    if !has_mime {
        conn.execute(
            "ALTER TABLE nodes ADD COLUMN mime TEXT NOT NULL DEFAULT ''",
            [],
        )?;
    }
    Ok(())
}

fn ensure_chunks_headline_column(conn: &mut Connection) -> rusqlite::Result<()> {
    let mut stmt = conn.prepare("PRAGMA table_info(chunks)")?;
    let mut has = false;
    let rows = stmt.query_map([], |row| {
        let name: String = row.get(1)?;
        Ok(name)
    })?;
    for row in rows {
        if row? == "is_headline" {
            has = true;
            break;
        }
    }
    drop(stmt);
    if !has {
        conn.execute(
            "ALTER TABLE chunks ADD COLUMN is_headline INTEGER NOT NULL DEFAULT 0",
            [],
        )?;
    }
    Ok(())
}

fn ensure_nodes_indexing_policy_column(conn: &mut Connection) -> rusqlite::Result<()> {
    let mut stmt = conn.prepare("PRAGMA table_info(nodes)")?;
    let mut has = false;
    let rows = stmt.query_map([], |row| {
        let name: String = row.get(1)?;
        Ok(name)
    })?;
    for row in rows {
        if row? == "indexing_policy" {
            has = true;
            break;
        }
    }
    drop(stmt);
    if !has {
        conn.execute(
            "ALTER TABLE nodes ADD COLUMN indexing_policy TEXT NOT NULL DEFAULT 'full'",
            [],
        )?;
    }
    Ok(())
}

fn now_ns() -> i64 {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
}
