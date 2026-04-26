//! CLI entrypoints (plan P2-4).

use std::path::PathBuf;

use anyhow::Context;
use clap::{Parser, Subcommand};
use scry_proto::scry::v1::admin_client::AdminClient;
use scry_proto::scry::v1::{
    CreateWorkspaceRequest, DeleteWorkspaceRequest, GetWorkspaceStatsRequest,
    ListWorkspacesRequest, ReindexWorkspaceRequest, WorkspaceConfig,
};
use tonic::metadata::MetadataValue;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Uri};
use tonic::Request;

use crate::server::{
    apply_startup_yaml_from_env, catalog_reconcile_cli, mint_token_cli, transport_from_env,
};

#[derive(Parser, Debug)]
#[command(name = "scryd", version, about = "Scry workspace daemon and admin CLI")]
pub(crate) struct ScrydCli {
    #[arg(long, global = true, value_name = "PATH")]
    pub(crate) config: Option<PathBuf>,
    #[command(subcommand)]
    pub(crate) command: Option<TopCommand>,
}

#[derive(Subcommand, Debug)]
pub(crate) enum TopCommand {
    /// Start the gRPC server (this is also the default when no subcommand is given)
    Serve,
    Ping,
    Token {
        #[command(subcommand)]
        action: TokenCmd,
    },
    Admin {
        #[command(flatten)]
        conn: AdminConnArgs,
        #[command(subcommand)]
        action: AdminCmd,
    },
    Catalog {
        #[command(subcommand)]
        action: CatalogCmd,
    },
    /// Legacy alias for `scryd token mint` (hidden; prefer `token mint`)
    #[command(hide = true)]
    MintToken {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        rest: Vec<String>,
    },
    /// Legacy alias for `scryd catalog reconcile` (hidden)
    #[command(hide = true)]
    ReconcileCatalog {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        rest: Vec<String>,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum TokenCmd {
    Mint(MintTokenCli),
}

#[derive(Parser, Debug)]
pub(crate) struct MintTokenCli {
    #[arg(long, value_name = "workspace|admin|local")]
    kind: Option<String>,
    #[arg(long)]
    workspace_id: Option<String>,
    #[arg(long)]
    scope: Option<String>,
    #[arg(long, default_value_t = 3600_u64)]
    ttl_secs: u64,
    #[arg(long)]
    secret: Option<String>,
}

#[derive(Parser, Debug, Clone)]
pub(crate) struct AdminConnArgs {
    /// gRPC endpoint, e.g. `http://127.0.0.1:50051` (overrides `SCRYD_GRPC_ADDR` when set)
    #[arg(long)]
    endpoint: Option<String>,
    /// Bearer token for admin RPCs (defaults to `SCRYD_ADMIN_TOKEN`)
    #[arg(long)]
    token: Option<String>,
    /// Trust system TLS roots and use HTTPS (for servers with TLS enabled)
    #[arg(long, default_value_t = false)]
    tls: bool,
    /// PEM path for custom CA (optional)
    #[arg(long)]
    ca_cert: Option<PathBuf>,
}

#[derive(Subcommand, Debug)]
pub(crate) enum AdminCmd {
    CreateWorkspace {
        #[arg(long)]
        name: String,
        #[arg(long, default_value_t = 32_u32)]
        embedding_dim: u32,
    },
    DeleteWorkspace {
        #[arg(long)]
        id: String,
    },
    ListWorkspaces {
        #[arg(long, default_value_t = 100_u32)]
        limit: u32,
        #[arg(long, default_value = "")]
        cursor: String,
    },
    Reindex {
        #[arg(long)]
        id: String,
    },
    Stats {
        #[arg(long)]
        id: String,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum CatalogCmd {
    Reconcile {
        #[arg(long, default_value_t = false)]
        apply: bool,
        #[arg(long, default_value_t = 256_u32)]
        limit: u32,
    },
}

fn mint_token_from_clap(args: MintTokenCli) -> anyhow::Result<()> {
    let mut out = Vec::<String>::new();
    if let Some(k) = args.kind {
        out.push("--kind".to_string());
        out.push(k);
    }
    if let Some(ws) = args.workspace_id {
        out.push("--workspace-id".to_string());
        out.push(ws);
    }
    if let Some(s) = args.scope {
        out.push("--scope".to_string());
        out.push(s);
    }
    out.push("--ttl-secs".to_string());
    out.push(args.ttl_secs.to_string());
    if let Some(sec) = args.secret {
        out.push("--secret".to_string());
        out.push(sec);
    }
    mint_token_cli(out)
}

async fn admin_channel(args: &AdminConnArgs) -> anyhow::Result<Channel> {
    let (uri, want_tls): (Uri, bool) = if let Some(ep) = &args.endpoint {
        let u: Uri = ep.parse().context("invalid --endpoint URI")?;
        let tls = args.tls || u.scheme_str() == Some("https");
        (u, tls)
    } else {
        let default_addr: std::net::SocketAddr =
            "127.0.0.1:50051".parse().expect("default socket addr");
        let t = transport_from_env(default_addr)?;
        let host = t.tcp_addr.to_string();
        let tls = args.tls || t.tls_cert_path.is_some();
        let s = if tls {
            format!("https://{host}")
        } else {
            format!("http://{host}")
        };
        (s.parse().context("derived admin URI")?, tls)
    };

    let mut endpoint = Endpoint::from_shared(uri.to_string()).context("endpoint")?;
    if want_tls {
        let mut tls = ClientTlsConfig::new();
        if let Some(ca) = &args.ca_cert {
            let pem = tokio::fs::read(ca)
                .await
                .with_context(|| format!("read CA cert {}", ca.display()))?;
            tls = tls.ca_certificate(Certificate::from_pem(pem));
        }
        endpoint = endpoint.tls_config(tls).context("tls_config")?;
    }

    endpoint.connect().await.context("connect admin gRPC")
}

fn admin_bearer(args: &AdminConnArgs) -> anyhow::Result<MetadataValue<tonic::metadata::Ascii>> {
    let raw = args
        .token
        .clone()
        .or_else(|| std::env::var("SCRYD_ADMIN_TOKEN").ok())
        .context("admin RPC requires --token or SCRYD_ADMIN_TOKEN")?;
    MetadataValue::try_from(format!("Bearer {raw}"))
        .map_err(|_| anyhow::anyhow!("invalid token for metadata"))
}

#[allow(clippy::result_large_err)]
pub(crate) async fn run_admin(args: AdminConnArgs, cmd: AdminCmd) -> anyhow::Result<()> {
    let channel = admin_channel(&args).await?;
    let bearer = admin_bearer(&args)?;
    let mut client = AdminClient::with_interceptor(channel, move |mut req: Request<()>| {
        req.metadata_mut().insert("authorization", bearer.clone());
        Ok(req)
    });
    match cmd {
        AdminCmd::CreateWorkspace {
            name,
            embedding_dim,
        } => {
            let resp = client
                .create_workspace(CreateWorkspaceRequest {
                    name,
                    config: Some(WorkspaceConfig {
                        embedding_model: String::new(),
                        embedding_dim,
                        max_file_size: 0,
                    }),
                })
                .await
                .context("CreateWorkspace")?;
            let body = resp.into_inner();
            println!("workspace_id={}", body.workspace_id);
            println!("root_node_id={}", body.root_node_id);
        }
        AdminCmd::DeleteWorkspace { id } => {
            client
                .delete_workspace(DeleteWorkspaceRequest { workspace_id: id })
                .await
                .context("DeleteWorkspace")?;
            println!("ok");
        }
        AdminCmd::ListWorkspaces { limit, cursor } => {
            let resp = client
                .list_workspaces(ListWorkspacesRequest { limit, cursor })
                .await
                .context("ListWorkspaces")?;
            for ws in resp.into_inner().workspaces {
                println!("{}\t{}\t{}", ws.workspace_id, ws.name, ws.root_node_id);
            }
        }
        AdminCmd::Reindex { id } => {
            let r = client
                .reindex_workspace(ReindexWorkspaceRequest { workspace_id: id })
                .await
                .context("ReindexWorkspace")?;
            println!("enqueued_jobs={}", r.into_inner().enqueued_jobs);
        }
        AdminCmd::Stats { id } => {
            let r = client
                .get_workspace_stats(GetWorkspaceStatsRequest { workspace_id: id })
                .await
                .context("GetWorkspaceStats")?
                .into_inner();
            println!(
                "files={} dirs={} chunks={} events={} content_bytes={}",
                r.file_count, r.dir_count, r.chunk_count, r.event_count, r.total_content_bytes
            );
            println!(
                "jobs queued={} running={} completed={} failed={}",
                r.queued_jobs, r.running_jobs, r.completed_jobs, r.failed_jobs
            );
            println!(
                "indexing_policy files full={} headline_only={} skipped={}",
                r.indexing_policy_full_files,
                r.indexing_policy_headline_only_files,
                r.indexing_policy_skipped_files
            );
        }
    }
    Ok(())
}

pub(crate) async fn run_catalog_reconcile(apply: bool, limit: u32) -> anyhow::Result<()> {
    catalog_reconcile_cli([
        if apply {
            "--apply".to_string()
        } else {
            "--dry-run".to_string()
        },
        "--limit".to_string(),
        limit.to_string(),
    ])
    .await
}

/// Run CLI after optional YAML has been applied to the environment.
pub(crate) async fn run_cli(cli: ScrydCli) -> anyhow::Result<()> {
    match cli.command {
        None | Some(TopCommand::Serve) => crate::server::run_server_after_config().await,
        Some(TopCommand::Ping) => {
            println!("pong");
            Ok(())
        }
        Some(TopCommand::Token { action }) => match action {
            TokenCmd::Mint(m) => mint_token_from_clap(m),
        },
        Some(TopCommand::Admin { conn, action }) => run_admin(conn, action).await,
        Some(TopCommand::Catalog { action }) => match action {
            CatalogCmd::Reconcile { apply, limit } => run_catalog_reconcile(apply, limit).await,
        },
        Some(TopCommand::MintToken { rest }) => mint_token_cli(rest),
        Some(TopCommand::ReconcileCatalog { rest }) => catalog_reconcile_cli(rest).await,
    }
}

/// Load `--config` / `SCRYD_CONFIG_PATH`, apply YAML to env, then run clap + dispatch.
pub(crate) async fn run_from_env() -> anyhow::Result<()> {
    apply_startup_yaml_from_env()?;
    let cli = ScrydCli::parse();
    run_cli(cli).await
}
