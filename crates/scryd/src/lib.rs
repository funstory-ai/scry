//! Server implementation for the `scryd` binary (plan P2-1: move bulk out of `main.rs`).

pub mod catalog;
mod cli;
mod credits;
pub mod embedding;

mod server;

pub async fn run() -> anyhow::Result<()> {
    cli::run_from_env().await
}
