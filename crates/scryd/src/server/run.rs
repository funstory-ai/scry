use crate::server::prelude::*;

use crate::catalog::{CatalogBackendKind, CatalogConfig};
use crate::server::auth::auth_from_env;
use crate::server::config::{
    env_usize_with_default, events_retention_from_env, metrics_from_env, transport_from_env,
};
use crate::server::constants::*;
use crate::server::metrics::{monitor_background_tasks, run_metrics_http_server};
use crate::server::state::{AppState, GLOBAL_RUNTIME_METRICS};
use crate::server::transport::{run_server_tcp, run_server_unix};

/// Start scryd after configuration has been loaded into the environment (YAML and/or env vars).
pub async fn run_server_after_config() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .compact()
        .init();

    let content_root =
        env::var("SCRYD_CONTENT_ROOT").unwrap_or_else(|_| "/tmp/scry/content".to_string());

    // Run safety guards (auth / transport / metrics) before the index-root requirement so
    // misconfigurations surface in a stable order regardless of which knob is set.
    let default_addr = "127.0.0.1:50051"
        .parse()
        .expect("static default socket addr must parse");
    let transport = transport_from_env(default_addr)?;
    let auth = Arc::new(auth_from_env()?);
    let retention_config = events_retention_from_env()?;
    let metrics_config = metrics_from_env()?;

    let index_root = env::var("SCRYD_INDEX_ROOT").context(
        "SCRYD_INDEX_ROOT is required (per-workspace layout is the only supported layout)",
    )?;
    let index_root = std::path::PathBuf::from(index_root);
    let catalog_config = CatalogConfig::from_env(&index_root)?;

    std::fs::create_dir_all(&index_root).with_context(|| {
        format!(
            "failed to create index root directory {}",
            index_root.display()
        )
    })?;
    std::fs::create_dir_all(&content_root)
        .with_context(|| format!("failed to create content root directory {content_root}"))?;

    let embedding_runtime = embedding::provider_from_env()?;
    info!(
        provider = %embedding_runtime.provider_kind,
        model = %embedding_runtime.model_id,
        "embedding provider configured"
    );
    let events_bus_capacity =
        env_usize_with_default("SCRYD_EVENTS_BUS_CAPACITY", DEFAULT_EVENTS_BUS_CAPACITY)?;
    if events_bus_capacity == 0 {
        anyhow::bail!("SCRYD_EVENTS_BUS_CAPACITY must be > 0");
    }
    let handle_idle_timeout_ms = match env::var("SCRYD_HANDLE_IDLE_TIMEOUT_MS") {
        Ok(raw) => raw
            .parse::<u64>()
            .with_context(|| format!("invalid SCRYD_HANDLE_IDLE_TIMEOUT_MS: {raw}"))?,
        Err(env::VarError::NotPresent) => AppState::DEFAULT_HANDLE_IDLE_TIMEOUT_MS,
        Err(env::VarError::NotUnicode(_)) => {
            anyhow::bail!("SCRYD_HANDLE_IDLE_TIMEOUT_MS must be valid unicode");
        }
    };
    let io_handle_memory_limit = env_usize_with_default(
        "SCRYD_IO_HANDLE_MEMORY_LIMIT",
        DEFAULT_IO_HANDLE_MEMORY_LIMIT,
    )?;
    if io_handle_memory_limit < 4096 {
        anyhow::bail!("SCRYD_IO_HANDLE_MEMORY_LIMIT must be >= 4096");
    }
    let io_flush_copy_chunk_bytes = env_usize_with_default(
        "SCRYD_IO_FLUSH_COPY_CHUNK_BYTES",
        transport.io_stream_chunk_bytes,
    )?;
    if io_flush_copy_chunk_bytes < 8192 {
        anyhow::bail!("SCRYD_IO_FLUSH_COPY_CHUNK_BYTES must be >= 8192");
    }
    let content_store = LocalFsContentStore::new(&content_root);
    let index_backend = IndexBackend::open(index_root.clone())?;
    let (workspace_catalog, catalog_task) = catalog::build_catalog(&catalog_config)?;
    info!(
        backend = match catalog_config.backend {
            CatalogBackendKind::Sqlite => "sqlite",
            CatalogBackendKind::Postgres => "postgres",
        },
        index_layout = index_backend.layout_label(),
        index_root = %index_root.display(),
        "workspace catalog backend configured"
    );
    let handle_idle = Duration::from_millis(handle_idle_timeout_ms);
    let state = Arc::new(AppState::new_with_io_limits(
        content_store,
        index_backend,
        workspace_catalog,
        embedding_runtime,
        io_handle_memory_limit,
        io_flush_copy_chunk_bytes,
        handle_idle,
        events_bus_capacity,
    ));
    let _ = GLOBAL_RUNTIME_METRICS.set(Arc::clone(&state.runtime_metrics));
    state.ensure_indexing_worker();
    let (shutdown_tx, _) = broadcast::channel::<()>(8);
    let monitor_state = Arc::clone(&state);
    let monitor_shutdown = shutdown_tx.subscribe();
    let monitor_metrics_config = metrics_config.clone();
    let monitor_task = tokio::spawn(async move {
        monitor_background_tasks(
            monitor_state,
            retention_config,
            monitor_metrics_config,
            monitor_shutdown,
        )
        .await;
    });
    let metrics_task = if let Some(metrics_addr) = metrics_config.endpoint_addr {
        let metrics_state = Arc::clone(&state);
        let metrics_shutdown = shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            run_metrics_http_server(metrics_addr, metrics_state, metrics_shutdown).await
        }))
    } else {
        None
    };
    let ctrl_shutdown = shutdown_tx.clone();
    let shutdown_timeout = transport.graceful_shutdown_timeout;
    let signal_task = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            info!("received ctrl-c, initiating graceful shutdown");
            let _ = ctrl_shutdown.send(());
            tokio::time::sleep(shutdown_timeout).await;
        }
    });

    let result = if let Some(unix_socket) = transport.unix_socket.clone() {
        info!(
            tcp_addr = %transport.tcp_addr,
            unix_socket = %unix_socket.display(),
            "scryd listening on tcp and unix socket"
        );
        let (tcp_result, unix_result) = tokio::join!(
            run_server_tcp(
                &transport,
                Arc::clone(&state),
                Arc::clone(&auth),
                shutdown_tx.subscribe()
            ),
            run_server_unix(
                &transport,
                &unix_socket,
                Arc::clone(&state),
                Arc::clone(&auth),
                shutdown_tx.subscribe()
            ),
        );
        tcp_result.and(unix_result)
    } else {
        info!(tcp_addr = %transport.tcp_addr, "scryd listening on tcp only");
        run_server_tcp(
            &transport,
            Arc::clone(&state),
            Arc::clone(&auth),
            shutdown_tx.subscribe(),
        )
        .await
    };
    let _ = shutdown_tx.send(());
    monitor_task.abort();
    if let Some(task) = metrics_task {
        task.abort();
    }
    if let Some(task) = catalog_task {
        task.abort();
    }
    signal_task.abort();
    result
}
