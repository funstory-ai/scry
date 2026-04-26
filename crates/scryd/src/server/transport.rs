use crate::server::prelude::*;

use tonic::transport::Identity;

use crate::server::config::{AuthConfig, TransportConfig};
use crate::server::grpc_metrics::GrpcRpcMetricsLayer;
use crate::server::health::HealthSvc;
use crate::server::services::*;
use crate::server::state::AppState;
pub(crate) fn load_file_bytes(path: &Path) -> anyhow::Result<Vec<u8>> {
    std::fs::read(path).with_context(|| format!("failed to read file {}", path.display()))
}

pub(crate) fn build_tls_config(
    transport: &TransportConfig,
) -> anyhow::Result<Option<ServerTlsConfig>> {
    let (Some(cert_path), Some(key_path)) = (&transport.tls_cert_path, &transport.tls_key_path)
    else {
        return Ok(None);
    };
    let cert = load_file_bytes(cert_path)?;
    let key = load_file_bytes(key_path)?;
    let mut tls = ServerTlsConfig::new().identity(Identity::from_pem(cert, key));
    if let Some(client_ca_path) = &transport.tls_client_ca_path {
        let client_ca = load_file_bytes(client_ca_path)?;
        tls = tls.client_ca_root(Certificate::from_pem(client_ca));
    }
    Ok(Some(tls))
}
pub(crate) fn build_server_with_builder<L>(
    mut builder: Server<L>,
    transport: &TransportConfig,
    state: Arc<AppState>,
    auth: Arc<AuthConfig>,
) -> tonic::transport::server::Router<L>
where
    L: tower::Layer<tonic::service::Routes> + Clone,
{
    let max_decoding = transport.max_decoding_message_bytes;
    let max_encoding = transport.max_encoding_message_bytes;
    let max_frame = u32::try_from(transport.max_frame_bytes).unwrap_or(u32::MAX);
    builder = builder.max_frame_size(Some(max_frame));
    builder
        .add_service(
            HealthServer::new(HealthSvc)
                .max_decoding_message_size(max_decoding)
                .max_encoding_message_size(max_encoding),
        )
        .add_service(
            AdminServer::new(AdminSvc {
                state: Arc::clone(&state),
                auth: Arc::clone(&auth),
            })
            .max_decoding_message_size(max_decoding)
            .max_encoding_message_size(max_encoding),
        )
        .add_service(
            NamespaceServer::new(NamespaceSvc {
                state: Arc::clone(&state),
                auth: Arc::clone(&auth),
            })
            .max_decoding_message_size(max_decoding)
            .max_encoding_message_size(max_encoding),
        )
        .add_service(
            MutationServer::new(MutationSvc {
                state: Arc::clone(&state),
                auth: Arc::clone(&auth),
            })
            .max_decoding_message_size(max_decoding)
            .max_encoding_message_size(max_encoding),
        )
        .add_service(
            FilesServer::new(FilesSvc {
                state: Arc::clone(&state),
                auth: Arc::clone(&auth),
                io_stream_chunk_bytes: transport.io_stream_chunk_bytes,
            })
            .max_decoding_message_size(max_decoding)
            .max_encoding_message_size(max_encoding),
        )
        .add_service(
            IoServer::new(IoSvc {
                state: Arc::clone(&state),
                auth: Arc::clone(&auth),
            })
            .max_decoding_message_size(max_decoding)
            .max_encoding_message_size(max_encoding),
        )
        .add_service(
            EventsServer::new(EventsSvc {
                state: Arc::clone(&state),
                auth: Arc::clone(&auth),
            })
            .max_decoding_message_size(max_decoding)
            .max_encoding_message_size(max_encoding),
        )
        .add_service(
            SearchServer::new(SearchSvc { state, auth })
                .max_decoding_message_size(max_decoding)
                .max_encoding_message_size(max_encoding),
        )
}

fn server_builder_with_rpc_observability(
    state: &Arc<AppState>,
) -> Server<tower::layer::util::Stack<GrpcRpcMetricsLayer, tower::layer::util::Identity>> {
    Server::builder()
        .trace_fn(crate::server::grpc_metrics::trace_span_for_grpc_request)
        .layer(GrpcRpcMetricsLayer {
            metrics: Arc::clone(&state.runtime_metrics),
        })
}

pub(crate) fn build_server(
    transport: &TransportConfig,
    state: Arc<AppState>,
    auth: Arc<AuthConfig>,
) -> tonic::transport::server::Router<
    tower::layer::util::Stack<GrpcRpcMetricsLayer, tower::layer::util::Identity>,
> {
    let builder = server_builder_with_rpc_observability(&state);
    build_server_with_builder(builder, transport, state, auth)
}

pub(crate) async fn run_server_tcp(
    transport: &TransportConfig,
    state: Arc<AppState>,
    auth: Arc<AuthConfig>,
    shutdown_rx: broadcast::Receiver<()>,
) -> anyhow::Result<()> {
    let mut builder = server_builder_with_rpc_observability(&state);
    if let Some(tls) = build_tls_config(transport)? {
        info!("scryd tcp grpc TLS enabled");
        builder = builder
            .tls_config(tls)
            .context("failed to configure TLS for gRPC server")?;
    }
    let addr = transport.tcp_addr;
    build_server_with_builder(builder, transport, state, auth)
        .serve_with_shutdown(addr, async move {
            let mut shutdown_rx = shutdown_rx;
            let _ = shutdown_rx.recv().await;
        })
        .await
        .context("tcp grpc server failed")
}

pub(crate) async fn run_server_unix(
    transport: &TransportConfig,
    unix_socket: &Path,
    state: Arc<AppState>,
    auth: Arc<AuthConfig>,
    shutdown_rx: broadcast::Receiver<()>,
) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    if let Some(parent) = unix_socket.parent() {
        std::fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create unix socket parent directory {}",
                parent.display()
            )
        })?;
    }
    if unix_socket.exists() {
        std::fs::remove_file(unix_socket).with_context(|| {
            format!(
                "failed to remove stale unix socket {}",
                unix_socket.display()
            )
        })?;
    }
    let listener = tokio::net::UnixListener::bind(unix_socket)
        .with_context(|| format!("failed to bind unix socket {}", unix_socket.display()))?;
    std::fs::set_permissions(unix_socket, std::fs::Permissions::from_mode(0o600)).with_context(
        || {
            format!(
                "failed to set unix socket permissions {}",
                unix_socket.display()
            )
        },
    )?;

    let incoming = UnixListenerStream::new(listener);
    build_server(transport, state, auth)
        .serve_with_incoming_shutdown(incoming, async move {
            let mut shutdown_rx = shutdown_rx;
            let _ = shutdown_rx.recv().await;
        })
        .await
        .context("unix grpc server failed")
}
