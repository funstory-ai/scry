use crate::server::prelude::*;

use crate::server::auth::{is_loopback_ip, is_loopback_socket_addr};
use crate::server::constants::*;
#[derive(Debug, Clone)]
pub(crate) struct EventsRetentionConfig {
    pub(crate) max_age: Duration,
    pub(crate) min_events_per_workspace: u64,
    pub(crate) sweep_interval: Duration,
}

#[derive(Debug, Clone)]
pub(crate) struct MetricsConfig {
    pub(crate) log_interval: Duration,
    pub(crate) endpoint_addr: Option<std::net::SocketAddr>,
}

#[derive(Debug, Clone)]
pub(crate) struct AuthConfig {
    pub(crate) active_secret: Vec<u8>,
    pub(crate) previous_secrets: Vec<Vec<u8>>,
    pub(crate) allow_local_without_token: bool,
    pub(crate) enforce_scopes: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct TransportConfig {
    pub(crate) tcp_addr: std::net::SocketAddr,
    pub(crate) unix_socket: Option<PathBuf>,
    pub(crate) tls_cert_path: Option<PathBuf>,
    pub(crate) tls_key_path: Option<PathBuf>,
    pub(crate) tls_client_ca_path: Option<PathBuf>,
    pub(crate) max_decoding_message_bytes: usize,
    pub(crate) max_encoding_message_bytes: usize,
    pub(crate) max_frame_bytes: usize,
    pub(crate) graceful_shutdown_timeout: Duration,
    pub(crate) io_stream_chunk_bytes: usize,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct YamlConfig {
    #[serde(default)]
    server: YamlServerConfig,
    #[serde(default)]
    auth: YamlAuthConfig,
    #[serde(default)]
    transport: YamlTransportConfig,
    #[serde(default)]
    events_retention: YamlEventsRetentionConfig,
    #[serde(default)]
    metrics: YamlMetricsConfig,
    #[serde(default)]
    catalog: YamlCatalogConfig,
    #[serde(default)]
    embedding: YamlEmbeddingConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct YamlServerConfig {
    content_root: Option<String>,
    index_root: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct YamlAuthConfig {
    secret: Option<String>,
    previous_secrets: Option<Vec<String>>,
    allow_local_no_auth: Option<bool>,
    enforce_scopes: Option<bool>,
    token_ttl_secs: Option<u64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct YamlTransportConfig {
    grpc_addr: Option<String>,
    grpc_unix_socket: Option<String>,
    tls_cert_path: Option<String>,
    tls_key_path: Option<String>,
    tls_client_ca_path: Option<String>,
    graceful_shutdown_timeout_secs: Option<u64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct YamlEventsRetentionConfig {
    max_age_secs: Option<u64>,
    min_events_per_workspace: Option<u64>,
    sweep_interval_secs: Option<u64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct YamlMetricsConfig {
    log_interval_secs: Option<u64>,
    addr: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct YamlCatalogConfig {
    backend: Option<String>,
    db_path: Option<String>,
    postgres_url: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct YamlEmbeddingConfig {
    provider: Option<String>,
    base_url: Option<String>,
    api_key: Option<String>,
    model: Option<String>,
    timeout_ms: Option<u64>,
    document_task: Option<String>,
    query_task: Option<String>,
    normalized: Option<bool>,
    /// Credits per 1_000_000 tokens for settlement (default 1.0).
    credits_per_million_tokens: Option<f64>,
}

#[derive(Debug, Clone)]
pub(crate) struct GlobalArgs {
    pub(crate) config_path: Option<PathBuf>,
    #[allow(dead_code)] // remainder consumed by clap after YAML is applied
    pub(crate) rest: Vec<String>,
}

pub(crate) fn parse_global_args(args: Vec<String>) -> anyhow::Result<GlobalArgs> {
    let mut config_path: Option<PathBuf> = None;
    let mut rest = Vec::<String>::new();
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--config" => {
                let raw = iter.next().context("missing value for --config")?;
                config_path = Some(PathBuf::from(raw));
            }
            "--help" | "-h" if rest.is_empty() => {
                println!(
                    "Usage: scryd [--config <path>] [mint-token|reconcile-catalog|ping] [subcommand args]"
                );
                std::process::exit(0);
            }
            other => {
                rest.push(other.to_string());
                rest.extend(iter);
                break;
            }
        }
    }
    Ok(GlobalArgs { config_path, rest })
}

pub(crate) fn resolve_config_path(cli_path: Option<PathBuf>) -> Option<PathBuf> {
    if cli_path.is_some() {
        return cli_path;
    }
    env::var("SCRYD_CONFIG_PATH")
        .ok()
        .map(|raw| raw.trim().to_string())
        .filter(|raw| !raw.is_empty())
        .map(PathBuf::from)
}

pub(crate) fn load_yaml_config(path: &Path) -> anyhow::Result<YamlConfig> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read config file {}", path.display()))?;
    serde_yaml::from_str::<YamlConfig>(&raw)
        .with_context(|| format!("failed to parse YAML config {}", path.display()))
}

fn set_env_if_absent(key: &str, value: Option<String>) {
    if env::var(key).is_ok() {
        return;
    }
    if let Some(v) = value {
        if !v.trim().is_empty() {
            // SAFETY: setting env vars during process startup before background threads.
            unsafe {
                env::set_var(key, v);
            }
        }
    }
}

fn set_env_bool_if_absent(key: &str, value: Option<bool>) {
    set_env_if_absent(
        key,
        value.map(|v| if v { "true" } else { "false" }.to_string()),
    );
}

fn set_env_u64_if_absent(key: &str, value: Option<u64>) {
    set_env_if_absent(key, value.map(|v| v.to_string()));
}

pub(crate) fn apply_yaml_to_env(cfg: &YamlConfig) -> anyhow::Result<()> {
    set_env_if_absent("SCRYD_CONTENT_ROOT", cfg.server.content_root.clone());
    set_env_if_absent("SCRYD_INDEX_ROOT", cfg.server.index_root.clone());

    set_env_if_absent("SCRYD_AUTH_SECRET", cfg.auth.secret.clone());
    if let Some(prev) = &cfg.auth.previous_secrets {
        let joined = prev
            .iter()
            .map(String::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join(",");
        set_env_if_absent("SCRYD_AUTH_PREVIOUS_SECRETS", Some(joined));
    }
    set_env_bool_if_absent("SCRYD_ALLOW_LOCAL_NO_AUTH", cfg.auth.allow_local_no_auth);
    set_env_bool_if_absent("SCRYD_AUTH_ENFORCE_SCOPES", cfg.auth.enforce_scopes);
    set_env_u64_if_absent("SCRYD_AUTH_TOKEN_TTL_SECS", cfg.auth.token_ttl_secs);

    set_env_if_absent("SCRYD_GRPC_ADDR", cfg.transport.grpc_addr.clone());
    set_env_if_absent(
        "SCRYD_GRPC_UNIX_SOCKET",
        cfg.transport.grpc_unix_socket.clone(),
    );
    set_env_if_absent("SCRYD_TLS_CERT_PATH", cfg.transport.tls_cert_path.clone());
    set_env_if_absent("SCRYD_TLS_KEY_PATH", cfg.transport.tls_key_path.clone());
    set_env_if_absent(
        "SCRYD_TLS_CLIENT_CA_PATH",
        cfg.transport.tls_client_ca_path.clone(),
    );
    set_env_u64_if_absent(
        "SCRYD_GRACEFUL_SHUTDOWN_TIMEOUT_SECS",
        cfg.transport.graceful_shutdown_timeout_secs,
    );

    set_env_u64_if_absent(
        "SCRYD_EVENTS_RETENTION_MAX_AGE_SECS",
        cfg.events_retention.max_age_secs,
    );
    set_env_u64_if_absent(
        "SCRYD_EVENTS_RETENTION_MIN_EVENTS_PER_WORKSPACE",
        cfg.events_retention.min_events_per_workspace,
    );
    set_env_u64_if_absent(
        "SCRYD_EVENTS_RETENTION_SWEEP_INTERVAL_SECS",
        cfg.events_retention.sweep_interval_secs,
    );

    set_env_u64_if_absent(
        "SCRYD_METRICS_LOG_INTERVAL_SECS",
        cfg.metrics.log_interval_secs,
    );
    set_env_if_absent("SCRYD_METRICS_ADDR", cfg.metrics.addr.clone());

    set_env_if_absent("SCRYD_CATALOG_BACKEND", cfg.catalog.backend.clone());
    set_env_if_absent("SCRYD_CATALOG_DB_PATH", cfg.catalog.db_path.clone());
    set_env_if_absent(
        "SCRYD_CATALOG_POSTGRES_URL",
        cfg.catalog.postgres_url.clone(),
    );

    set_env_if_absent("SCRYD_EMBED_PROVIDER", cfg.embedding.provider.clone());
    set_env_if_absent("SCRYD_EMBED_BASE_URL", cfg.embedding.base_url.clone());
    set_env_if_absent("SCRYD_EMBED_API_KEY", cfg.embedding.api_key.clone());
    set_env_if_absent("SCRYD_EMBED_MODEL", cfg.embedding.model.clone());
    set_env_u64_if_absent("SCRYD_EMBED_TIMEOUT_MS", cfg.embedding.timeout_ms);
    set_env_if_absent(
        "SCRYD_EMBED_DOCUMENT_TASK",
        cfg.embedding.document_task.clone(),
    );
    set_env_if_absent("SCRYD_EMBED_QUERY_TASK", cfg.embedding.query_task.clone());
    set_env_bool_if_absent("SCRYD_EMBED_NORMALIZED", cfg.embedding.normalized);
    if let Some(rate) = cfg.embedding.credits_per_million_tokens {
        set_env_if_absent(
            "SCRYD_EMBED_CREDITS_PER_MILLION_TOKENS",
            Some(rate.to_string()),
        );
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub(crate) struct ReconcileCatalogArgs {
    apply: bool,
    limit: u32,
}

fn parse_reconcile_catalog_args(
    args: impl IntoIterator<Item = String>,
) -> anyhow::Result<ReconcileCatalogArgs> {
    let mut apply = false;
    let mut limit = env_u64_with_default("SCRYD_RECONCILE_LIMIT", 256)? as u32;
    let mut iter = args.into_iter();
    while let Some(flag) = iter.next() {
        match flag.as_str() {
            "--apply" => apply = true,
            "--dry-run" => apply = false,
            "--limit" => {
                let raw = iter.next().context("missing value for --limit")?;
                limit = raw
                    .parse::<u32>()
                    .with_context(|| format!("invalid --limit value: {raw}"))?;
            }
            "--help" | "-h" => {
                println!("Usage: scryd reconcile-catalog [--dry-run|--apply] [--limit <n>]");
                std::process::exit(0);
            }
            unknown => {
                anyhow::bail!("unknown reconcile-catalog argument: {unknown}");
            }
        }
    }
    if limit == 0 {
        anyhow::bail!("--limit must be > 0");
    }
    Ok(ReconcileCatalogArgs { apply, limit })
}

pub(crate) async fn catalog_reconcile_cli(
    args: impl IntoIterator<Item = String>,
) -> anyhow::Result<()> {
    let parsed = parse_reconcile_catalog_args(args)?;
    info!(
        apply = parsed.apply,
        limit = parsed.limit,
        "running catalog reconcile"
    );
    let index_root = env::var("SCRYD_INDEX_ROOT").context(
        "SCRYD_INDEX_ROOT is required for catalog reconcile (per-workspace layout is the only \
         supported layout)",
    )?;
    let index_root = PathBuf::from(index_root);
    let index_backend = crate::server::index_backend::IndexBackend::open(index_root.clone())?;
    let catalog_config = CatalogConfig::from_env(&index_root)?;
    let (catalog, catalog_task) = catalog::build_catalog(&catalog_config)?;

    let report =
        catalog::reconcile_catalog_against_index(&index_backend, catalog.as_ref(), parsed.apply)
            .await?;
    println!("{}", format_catalog_reconcile_report(&report));
    if let Some(task) = catalog_task {
        task.abort();
    }
    Ok(())
}

fn format_catalog_reconcile_report(report: &catalog::CatalogReconcileReport) -> String {
    format!(
        "checked_index_workspaces={} missing_in_catalog={} checked_catalog_workspaces={} stale_in_catalog={} repaired_added={} repaired_deleted={}",
        report.checked_index_workspaces,
        report.missing_in_catalog,
        report.checked_catalog_workspaces,
        report.stale_in_catalog,
        report.repaired_added,
        report.repaired_deleted
    )
}
pub(crate) fn transport_from_env(
    default_addr: std::net::SocketAddr,
) -> anyhow::Result<TransportConfig> {
    let tcp_addr = match env::var("SCRYD_GRPC_ADDR") {
        Ok(raw) => raw
            .parse()
            .with_context(|| format!("invalid SCRYD_GRPC_ADDR: {raw}"))?,
        Err(env::VarError::NotPresent) => default_addr,
        Err(env::VarError::NotUnicode(_)) => anyhow::bail!("SCRYD_GRPC_ADDR must be unicode"),
    };
    let unix_socket = env::var("SCRYD_GRPC_UNIX_SOCKET")
        .ok()
        .map(|raw| raw.trim().to_string())
        .filter(|raw| !raw.is_empty())
        .map(PathBuf::from);
    let tls_cert_path = env::var("SCRYD_TLS_CERT_PATH")
        .ok()
        .map(|raw| raw.trim().to_string())
        .filter(|raw| !raw.is_empty())
        .map(PathBuf::from);
    let tls_key_path = env::var("SCRYD_TLS_KEY_PATH")
        .ok()
        .map(|raw| raw.trim().to_string())
        .filter(|raw| !raw.is_empty())
        .map(PathBuf::from);
    let tls_client_ca_path = env::var("SCRYD_TLS_CLIENT_CA_PATH")
        .ok()
        .map(|raw| raw.trim().to_string())
        .filter(|raw| !raw.is_empty())
        .map(PathBuf::from);
    if tls_cert_path.is_some() != tls_key_path.is_some() {
        anyhow::bail!("SCRYD_TLS_CERT_PATH and SCRYD_TLS_KEY_PATH must be set together");
    }
    if tls_client_ca_path.is_some() && tls_cert_path.is_none() {
        anyhow::bail!("SCRYD_TLS_CLIENT_CA_PATH requires SCRYD_TLS_CERT_PATH/SCRYD_TLS_KEY_PATH");
    }
    if !is_loopback_socket_addr(&tcp_addr) && tls_cert_path.is_none() {
        anyhow::bail!(
            "SCRYD_GRPC_ADDR={} is not loopback and TLS is not configured. Set SCRYD_TLS_CERT_PATH/SCRYD_TLS_KEY_PATH or bind loopback only; see {}",
            tcp_addr,
            HARDENING_DOC_PATH
        );
    }
    let max_decoding_message_bytes = env_usize_with_default(
        "SCRYD_MAX_DECODING_MESSAGE_BYTES",
        DEFAULT_MAX_MESSAGE_BYTES,
    )?;
    let max_encoding_message_bytes = env_usize_with_default(
        "SCRYD_MAX_ENCODING_MESSAGE_BYTES",
        DEFAULT_MAX_MESSAGE_BYTES,
    )?;
    if max_decoding_message_bytes == 0 {
        anyhow::bail!("SCRYD_MAX_DECODING_MESSAGE_BYTES must be > 0");
    }
    if max_encoding_message_bytes == 0 {
        anyhow::bail!("SCRYD_MAX_ENCODING_MESSAGE_BYTES must be > 0");
    }
    let graceful_shutdown_timeout_secs =
        env_u64_with_default("SCRYD_GRACEFUL_SHUTDOWN_TIMEOUT_SECS", 10)?;
    let io_stream_chunk_bytes =
        env_usize_with_default("SCRYD_IO_STREAM_CHUNK_BYTES", DEFAULT_IO_STREAM_CHUNK_BYTES)?;
    if io_stream_chunk_bytes == 0 {
        anyhow::bail!("SCRYD_IO_STREAM_CHUNK_BYTES must be > 0");
    }
    let max_frame_bytes =
        env_usize_with_default("SCRYD_GRPC_MAX_FRAME_BYTES", DEFAULT_GRPC_MAX_FRAME_BYTES)?;
    if max_frame_bytes == 0 {
        anyhow::bail!("SCRYD_GRPC_MAX_FRAME_BYTES must be > 0");
    }
    const H2_MAX_FRAME: usize = 16 * 1024 * 1024 - 1;
    if max_frame_bytes > H2_MAX_FRAME {
        anyhow::bail!(
            "SCRYD_GRPC_MAX_FRAME_BYTES must be <= {} (HTTP/2 SETTINGS_MAX_FRAME_SIZE limit)",
            H2_MAX_FRAME
        );
    }
    Ok(TransportConfig {
        tcp_addr,
        unix_socket,
        tls_cert_path,
        tls_key_path,
        tls_client_ca_path,
        max_decoding_message_bytes,
        max_encoding_message_bytes,
        max_frame_bytes,
        graceful_shutdown_timeout: Duration::from_secs(graceful_shutdown_timeout_secs.max(1)),
        io_stream_chunk_bytes,
    })
}

pub(crate) fn events_retention_from_env() -> anyhow::Result<EventsRetentionConfig> {
    let max_age_secs =
        env_u64_with_default("SCRYD_EVENTS_RETENTION_MAX_AGE_SECS", 7 * 24 * 60 * 60)?;
    let min_events_per_workspace =
        env_u64_with_default("SCRYD_EVENTS_RETENTION_MIN_EVENTS_PER_WORKSPACE", 100_000)?;
    let sweep_interval_secs =
        env_u64_with_default("SCRYD_EVENTS_RETENTION_SWEEP_INTERVAL_SECS", 60)?;
    Ok(EventsRetentionConfig {
        max_age: Duration::from_secs(max_age_secs.max(1)),
        min_events_per_workspace,
        sweep_interval: Duration::from_secs(sweep_interval_secs.max(1)),
    })
}

pub(crate) fn metrics_from_env() -> anyhow::Result<MetricsConfig> {
    let log_interval_secs = env_u64_with_default("SCRYD_METRICS_LOG_INTERVAL_SECS", 15)?;
    let metrics_allow_public = env_bool_with_default("SCRYD_METRICS_ALLOW_PUBLIC", false)?;
    let endpoint_addr = match env::var("SCRYD_METRICS_ADDR") {
        Ok(raw) => Some(
            raw.parse::<std::net::SocketAddr>()
                .with_context(|| format!("invalid SCRYD_METRICS_ADDR: {raw}"))?,
        ),
        Err(env::VarError::NotPresent) => None,
        Err(env::VarError::NotUnicode(_)) => {
            anyhow::bail!("SCRYD_METRICS_ADDR must be valid unicode");
        }
    };
    if let Some(addr) = endpoint_addr {
        if !is_loopback_ip(&addr.ip()) && !metrics_allow_public {
            anyhow::bail!(
                "SCRYD_METRICS_ADDR={} is not loopback. Refusing to expose /metrics publicly by default; set SCRYD_METRICS_ALLOW_PUBLIC=1 if this is intentional.",
                addr
            );
        }
    }
    Ok(MetricsConfig {
        log_interval: Duration::from_secs(log_interval_secs.max(1)),
        endpoint_addr,
    })
}
pub(crate) fn env_u64_with_default(key: &str, default: u64) -> anyhow::Result<u64> {
    match env::var(key) {
        Ok(raw) => raw
            .parse::<u64>()
            .with_context(|| format!("invalid {key}: {raw}")),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(env::VarError::NotUnicode(_)) => anyhow::bail!("{key} must be valid unicode"),
    }
}

pub(crate) fn env_usize_with_default(key: &str, default: usize) -> anyhow::Result<usize> {
    match env::var(key) {
        Ok(raw) => raw
            .parse::<usize>()
            .with_context(|| format!("invalid {key}: {raw}")),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(env::VarError::NotUnicode(_)) => anyhow::bail!("{key} must be valid unicode"),
    }
}

pub(crate) fn env_bool_with_default(key: &str, default: bool) -> anyhow::Result<bool> {
    match env::var(key) {
        Ok(raw) => {
            let normalized = raw.trim().to_ascii_lowercase();
            match normalized.as_str() {
                "1" | "true" | "yes" | "on" => Ok(true),
                "0" | "false" | "no" | "off" => Ok(false),
                _ => anyhow::bail!("invalid {key}: {raw}"),
            }
        }
        Err(env::VarError::NotPresent) => Ok(default),
        Err(env::VarError::NotUnicode(_)) => anyhow::bail!("{key} must be valid unicode"),
    }
}
