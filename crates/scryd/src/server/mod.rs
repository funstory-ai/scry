//! `scryd` gRPC server implementation split by concern (plan P2-1).

mod auth;
mod config;
pub(crate) mod constants;
mod grpc_metrics;
mod handle_storage;
mod health;
pub(crate) mod index_backend;
mod metrics;
mod prelude;
pub(crate) mod rpc;
mod run;
pub(crate) mod services;
mod state;
#[cfg(test)]
mod tests;
mod transport;

pub use run::run_server_after_config;

/// Apply `--config` / `SCRYD_CONFIG_PATH` YAML to the process environment before clap parses subcommands.
pub(crate) fn apply_startup_yaml_from_env() -> anyhow::Result<()> {
    let global = config::parse_global_args(std::env::args().skip(1).collect::<Vec<_>>())?;
    if let Some(path) = config::resolve_config_path(global.config_path.clone()) {
        let yaml = config::load_yaml_config(&path)?;
        config::apply_yaml_to_env(&yaml)?;
    }
    Ok(())
}

pub(crate) use auth::mint_token_cli;
pub(crate) use config::{catalog_reconcile_cli, transport_from_env};

// Re-exports for `tests.rs` (`#[cfg(test)]`); the compiler marks them unused in non-test lib builds.
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use auth::{auth_from_env, parse_mint_token_args, verify_auth, AuthRequirement};
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use config::{metrics_from_env, parse_global_args, AuthConfig, TransportConfig};
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use constants::{
    DEFAULT_AUTH_SECRET, DEFAULT_EVENTS_BUS_CAPACITY, DEFAULT_GRPC_MAX_FRAME_BYTES,
    DEFAULT_IO_HANDLE_MEMORY_LIMIT, DEFAULT_IO_STREAM_CHUNK_BYTES, DEFAULT_MAX_MESSAGE_BYTES,
};
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use index_backend::IndexBackend;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use state::{AppState, RuntimeMetrics};
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use transport::build_server;
