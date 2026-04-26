use std::{sync::Arc, time::Duration};

use crate::server::constants::RPC_DURATION_BUCKETS_SECS;
use crate::server::metrics::build_metrics_text;
use crate::server::{
    auth_from_env, build_server, metrics_from_env, parse_mint_token_args, transport_from_env,
    verify_auth, AppState, AuthConfig, AuthRequirement, IndexBackend, RuntimeMetrics,
    TransportConfig, DEFAULT_AUTH_SECRET, DEFAULT_EVENTS_BUS_CAPACITY,
    DEFAULT_GRPC_MAX_FRAME_BYTES, DEFAULT_IO_HANDLE_MEMORY_LIMIT, DEFAULT_IO_STREAM_CHUNK_BYTES,
    DEFAULT_MAX_MESSAGE_BYTES,
};

use scry_index::IndexStore;
use scry_proto::scry::v1::{
    admin_client::AdminClient, files_client::FilesClient, health_client::HealthClient,
    mutation_client::MutationClient, namespace_client::NamespaceClient, put_file_request,
    search_client::SearchClient, CreateRequest, CreateWorkspaceRequest, DeleteWorkspaceRequest,
    GetAttrsIfChangedRequest, GetAttrsRequest, GetFileRequest, GetWorkspaceStatsRequest,
    ListWorkspacesRequest, LookupRequest, NodeKind, PingRequest, PutFileRequest, ReadDirRequest,
    ReindexWorkspaceRequest, RenameRequest, ResolvePathRequest, ResolveRefRequest, SearchMode,
    SearchRequest, UnlinkRequest,
};
use scry_proto::scry::v1::{
    events_client::EventsClient, AcknowledgeRequest, SubscribeFilter, SubscribeRequest,
};
use scry_proto::scry::v1::{
    io_client::IoClient, FlushRequest, HandleMode, OpenRequest, ReadRequest, ReleaseRequest,
    TruncateRequest, WriteRequest,
};
use scry_storage::LocalFsContentStore;
use tempfile::TempDir;
use tokio::task::JoinHandle;
use tonic::transport::{Channel, Endpoint};
use tonic::Code;

use crate::catalog;
use crate::embedding::MockEmbeddingProvider;

struct EnvGuard {
    saved: Vec<(String, Option<String>)>,
}

impl EnvGuard {
    fn set_many(vars: &[(&str, Option<&str>)]) -> Self {
        let mut saved = Vec::with_capacity(vars.len());
        for (key, value) in vars {
            let key = (*key).to_string();
            let previous = std::env::var(&key).ok();
            match value {
                Some(v) => {
                    // SAFETY: tests mutate process env in a controlled scope and restore it in Drop.
                    unsafe { std::env::set_var(&key, v) };
                }
                None => {
                    // SAFETY: tests mutate process env in a controlled scope and restore it in Drop.
                    unsafe { std::env::remove_var(&key) };
                }
            }
            saved.push((key, previous));
        }
        Self { saved }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (key, previous) in self.saved.iter().rev() {
            match previous {
                Some(value) => {
                    // SAFETY: restoring environment values captured by this test-scoped guard.
                    unsafe { std::env::set_var(key, value) };
                }
                None => {
                    // SAFETY: restoring environment values captured by this test-scoped guard.
                    unsafe { std::env::remove_var(key) };
                }
            }
        }
    }
}

async fn connect_with_retry(addr: std::net::SocketAddr) -> Channel {
    let endpoint = Endpoint::from_shared(format!("http://{addr}")).expect("endpoint");
    for _ in 0..50 {
        if let Ok(channel) = endpoint.clone().connect().await {
            return channel;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("failed to connect to test server at {addr}");
}

async fn spawn_test_server_with_transport(
    handle_idle_timeout: Duration,
    max_decoding_message_bytes: usize,
    max_encoding_message_bytes: usize,
    io_stream_chunk_bytes: usize,
    io_handle_memory_limit: usize,
    io_flush_copy_chunk_bytes: usize,
) -> (
    TempDir,
    tokio::sync::oneshot::Sender<()>,
    JoinHandle<()>,
    Channel,
    Arc<AppState>,
) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let content_root = tmp.path().join("content");
    std::fs::create_dir_all(&content_root).expect("create content root");
    let index_root = tmp.path().join("index");
    let catalog_db = tmp.path().join("catalog.db");
    let embedding_runtime = crate::embedding::EmbeddingRuntime {
        provider_kind: "mock".to_string(),
        model_id: "mock-embedding-32d".to_string(),
        provider: Arc::new(MockEmbeddingProvider::default()),
    };
    let (catalog, _catalog_task) = catalog::build_catalog(&catalog::CatalogConfig {
        backend: catalog::CatalogBackendKind::Sqlite,
        sqlite_db_path: Some(catalog_db),
        postgres_url: None,
    })
    .expect("build sqlite catalog");
    let state = Arc::new(AppState::new_with_io_limits(
        LocalFsContentStore::new(&content_root),
        IndexBackend::open(index_root).expect("open index backend"),
        catalog,
        embedding_runtime,
        io_handle_memory_limit,
        io_flush_copy_chunk_bytes,
        handle_idle_timeout,
        DEFAULT_EVENTS_BUS_CAPACITY,
    ));

    let socket = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral socket");
    let addr = socket.local_addr().expect("socket addr");
    drop(socket);

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server_state = Arc::clone(&state);
    let auth = Arc::new(AuthConfig {
        active_secret: b"test-secret".to_vec(),
        previous_secrets: vec![],
        allow_local_without_token: true,
        enforce_scopes: true,
    });
    let transport = TransportConfig {
        tcp_addr: addr,
        unix_socket: None,
        tls_cert_path: None,
        tls_key_path: None,
        tls_client_ca_path: None,
        max_decoding_message_bytes,
        max_encoding_message_bytes,
        max_frame_bytes: DEFAULT_GRPC_MAX_FRAME_BYTES,
        graceful_shutdown_timeout: Duration::from_secs(10),
        io_stream_chunk_bytes,
    };
    let server_task = tokio::spawn(async move {
        build_server(&transport, server_state, auth)
            .serve_with_shutdown(addr, async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("serve");
    });

    let channel = connect_with_retry(addr).await;
    (tmp, shutdown_tx, server_task, channel, state)
}

async fn spawn_test_server_with_idle_timeout(
    handle_idle_timeout: Duration,
) -> (
    TempDir,
    tokio::sync::oneshot::Sender<()>,
    JoinHandle<()>,
    Channel,
    Arc<AppState>,
) {
    spawn_test_server_with_transport(
        handle_idle_timeout,
        DEFAULT_MAX_MESSAGE_BYTES,
        DEFAULT_MAX_MESSAGE_BYTES,
        DEFAULT_IO_STREAM_CHUNK_BYTES,
        DEFAULT_IO_HANDLE_MEMORY_LIMIT,
        DEFAULT_IO_STREAM_CHUNK_BYTES,
    )
    .await
}

async fn spawn_test_server() -> (
    TempDir,
    tokio::sync::oneshot::Sender<()>,
    JoinHandle<()>,
    Channel,
    Arc<AppState>,
) {
    spawn_test_server_with_idle_timeout(Duration::from_millis(
        AppState::DEFAULT_HANDLE_IDLE_TIMEOUT_MS,
    ))
    .await
}

async fn spawn_test_server_per_workspace_layout() -> (
    TempDir,
    tokio::sync::oneshot::Sender<()>,
    JoinHandle<()>,
    Channel,
    Arc<AppState>,
) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let content_root = tmp.path().join("content");
    std::fs::create_dir_all(&content_root).expect("create content root");
    let index_root = tmp.path().join("per-workspace-index");
    std::fs::create_dir_all(&index_root).expect("create per-workspace index root");
    let catalog_db = tmp.path().join("catalog.db");
    let embedding_runtime = crate::embedding::EmbeddingRuntime {
        provider_kind: "mock".to_string(),
        model_id: "mock-embedding-32d".to_string(),
        provider: Arc::new(MockEmbeddingProvider::default()),
    };
    let (catalog, _catalog_task) = catalog::build_catalog(&catalog::CatalogConfig {
        backend: catalog::CatalogBackendKind::Sqlite,
        sqlite_db_path: Some(catalog_db),
        postgres_url: None,
    })
    .expect("build sqlite catalog");
    let state = Arc::new(AppState::new_with_io_limits(
        LocalFsContentStore::new(&content_root),
        IndexBackend::open(index_root.clone()).expect("open per-workspace index backend"),
        catalog,
        embedding_runtime,
        DEFAULT_IO_HANDLE_MEMORY_LIMIT,
        DEFAULT_IO_STREAM_CHUNK_BYTES,
        Duration::from_millis(AppState::DEFAULT_HANDLE_IDLE_TIMEOUT_MS),
        DEFAULT_EVENTS_BUS_CAPACITY,
    ));

    let socket = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral socket");
    let addr = socket.local_addr().expect("socket addr");
    drop(socket);

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server_state = Arc::clone(&state);
    let auth = Arc::new(AuthConfig {
        active_secret: b"test-secret".to_vec(),
        previous_secrets: vec![],
        allow_local_without_token: true,
        enforce_scopes: true,
    });
    let transport = TransportConfig {
        tcp_addr: addr,
        unix_socket: None,
        tls_cert_path: None,
        tls_key_path: None,
        tls_client_ca_path: None,
        max_decoding_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
        max_encoding_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
        max_frame_bytes: DEFAULT_GRPC_MAX_FRAME_BYTES,
        graceful_shutdown_timeout: Duration::from_secs(10),
        io_stream_chunk_bytes: DEFAULT_IO_STREAM_CHUNK_BYTES,
    };
    let server_task = tokio::spawn(async move {
        build_server(&transport, server_state, auth)
            .serve_with_shutdown(addr, async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("serve");
    });

    let channel = connect_with_retry(addr).await;
    (tmp, shutdown_tx, server_task, channel, state)
}

#[tokio::test]
async fn grpc_per_workspace_layout_keeps_workspace_indexes_separate() {
    let (_tmp, shutdown_tx, server_task, channel, state) =
        spawn_test_server_per_workspace_layout().await;
    let mut admin = AdminClient::new(channel.clone());
    let mut mutation = MutationClient::new(channel.clone());
    let mut files = FilesClient::new(channel.clone());
    let mut namespace = NamespaceClient::new(channel.clone());
    let mut search = SearchClient::new(channel);

    let ws_a = admin
        .create_workspace(CreateWorkspaceRequest {
            name: "layout-a".to_string(),
            config: None,
        })
        .await
        .expect("create workspace A")
        .into_inner();
    let ws_b = admin
        .create_workspace(CreateWorkspaceRequest {
            name: "layout-b".to_string(),
            config: None,
        })
        .await
        .expect("create workspace B")
        .into_inner();

    let db_a = state
        .index_backend
        .db_path_for_workspace(&ws_a.workspace_id)
        .expect("workspace A db path");
    let db_b = state
        .index_backend
        .db_path_for_workspace(&ws_b.workspace_id)
        .expect("workspace B db path");
    assert_ne!(
        db_a, db_b,
        "per-workspace layout should isolate sqlite files"
    );
    assert!(db_a.exists(), "workspace A index db should exist");
    assert!(db_b.exists(), "workspace B index db should exist");

    let docs_a = mutation
        .create(CreateRequest {
            workspace_id: ws_a.workspace_id.clone(),
            parent_node_id: ws_a.root_node_id.clone(),
            name: "docs".to_string(),
            kind: NodeKind::Dir as i32,
            mode: 0,
            exclusive: true,
        })
        .await
        .expect("create docs in workspace A")
        .into_inner();
    let docs_b = mutation
        .create(CreateRequest {
            workspace_id: ws_b.workspace_id.clone(),
            parent_node_id: ws_b.root_node_id.clone(),
            name: "docs".to_string(),
            kind: NodeKind::Dir as i32,
            mode: 0,
            exclusive: true,
        })
        .await
        .expect("create docs in workspace B")
        .into_inner();

    files
        .put_file(PutFileRequest {
            workspace_id: ws_a.workspace_id.clone(),
            target: Some(put_file_request::Target::PutFilePath(
                scry_proto::scry::v1::PutFilePath {
                    parent_node_id: docs_a.node_id.clone(),
                    name: "a.md".to_string(),
                },
            )),
            content: b"alpha-index-token".to_vec(),
            if_version: 0,
            mode: 0,
            ..Default::default()
        })
        .await
        .expect("put file in workspace A");
    files
        .put_file(PutFileRequest {
            workspace_id: ws_b.workspace_id.clone(),
            target: Some(put_file_request::Target::PutFilePath(
                scry_proto::scry::v1::PutFilePath {
                    parent_node_id: docs_b.node_id.clone(),
                    name: "b.md".to_string(),
                },
            )),
            content: b"beta-index-token".to_vec(),
            if_version: 0,
            mode: 0,
            ..Default::default()
        })
        .await
        .expect("put file in workspace B");

    let hits_a = search
        .search(SearchRequest {
            workspace_id: ws_a.workspace_id.clone(),
            query: "alpha-index-token".to_string(),
            mode: SearchMode::Fts as i32,
            limit: 10,
        })
        .await
        .expect("search workspace A")
        .into_inner();
    assert_eq!(hits_a.total_hits, 1);
    assert_eq!(hits_a.hits[0].path, "docs/a.md");

    let hits_b = search
        .search(SearchRequest {
            workspace_id: ws_b.workspace_id.clone(),
            query: "alpha-index-token".to_string(),
            mode: SearchMode::Fts as i32,
            limit: 10,
        })
        .await
        .expect("search workspace B")
        .into_inner();
    assert_eq!(
        hits_b.total_hits, 0,
        "workspace B should not read workspace A index rows"
    );

    let resolved = namespace
        .resolve_path(ResolvePathRequest {
            workspace_id: ws_b.workspace_id.clone(),
            path: "docs/b.md".to_string(),
        })
        .await
        .expect("resolve path in workspace B")
        .into_inner();
    assert!(resolved.exists);

    assert!(
        state.index_backend.open_store_count() >= 2,
        "expected one opened index store per workspace"
    );

    let _ = shutdown_tx.send(());
    server_task.await.expect("server task");
}

#[tokio::test]
async fn rpc_metrics_recorded_after_health_ping() {
    let (_tmp, shutdown_tx, server_task, channel, state) = spawn_test_server().await;
    let mut health = HealthClient::new(channel.clone());

    health.ping(PingRequest {}).await.expect("ping");
    drop(health);
    // Close the gRPC channel so the server can finish graceful shutdown.
    drop(channel);

    {
        let counts = state
            .runtime_metrics
            .rpc_request_counts
            .lock()
            .expect("rpc_request_counts lock");
        let key = (
            "scry.v1.Health".to_string(),
            "Ping".to_string(),
            "0".to_string(),
        );
        assert_eq!(counts.get(&key).copied().unwrap_or(0), 1);
    }

    {
        let hist = state
            .runtime_metrics
            .rpc_duration_hist
            .lock()
            .expect("rpc_duration_hist lock");
        let h = hist
            .get(&("scry.v1.Health".to_string(), "Ping".to_string()))
            .expect("ping histogram");
        assert_eq!(h.count, 1);
        assert!(h.sum_ns > 0);
        assert_eq!(h.buckets.len(), RPC_DURATION_BUCKETS_SECS.len());
        assert_eq!(h.buckets[0], 1, "sub-ms ping should land in first bucket");
    }

    let text = build_metrics_text(state.as_ref());
    assert!(
        text.contains(
            "scryd_rpc_requests_total{service=\"scry.v1.Health\",method=\"Ping\",code=\"0\"} 1"
        ),
        "metrics text should list ping counter: {text}"
    );
    assert!(
        text.contains("scryd_rpc_duration_seconds_bucket{service=\"scry.v1.Health\",method=\"Ping\",le=\"0.001\"} 1"),
        "metrics text should list first histogram bucket: {text}"
    );

    let _ = shutdown_tx.send(());
    server_task.await.expect("server task");
}

#[tokio::test]
async fn grpc_roundtrip_workspace_namespace_mutation_search() {
    let (_tmp, shutdown_tx, server_task, channel, _state) = spawn_test_server().await;
    let mut admin = AdminClient::new(channel.clone());
    let mut namespace = NamespaceClient::new(channel.clone());
    let mut mutation = MutationClient::new(channel.clone());
    let mut files = FilesClient::new(channel.clone());
    let mut search = SearchClient::new(channel);

    let create_ws = admin
        .create_workspace(CreateWorkspaceRequest {
            name: "integration".to_string(),
            config: None,
        })
        .await
        .expect("create workspace")
        .into_inner();
    let workspace_id = create_ws.workspace_id;
    let root_node_id = create_ws.root_node_id;
    assert!(!workspace_id.is_empty());
    assert!(!root_node_id.is_empty());

    let docs = mutation
        .create(CreateRequest {
            workspace_id: workspace_id.clone(),
            parent_node_id: root_node_id.clone(),
            name: "docs".to_string(),
            kind: NodeKind::Dir as i32,
            mode: 0,
            exclusive: true,
        })
        .await
        .expect("create docs")
        .into_inner();
    let docs_node_id = docs.node_id;

    let plan = mutation
        .create(CreateRequest {
            workspace_id: workspace_id.clone(),
            parent_node_id: docs_node_id.clone(),
            name: "plan.md".to_string(),
            kind: NodeKind::File as i32,
            mode: 0,
            exclusive: true,
        })
        .await
        .expect("create plan")
        .into_inner();
    let plan_node_id = plan.node_id;

    files
        .put_file(PutFileRequest {
            workspace_id: workspace_id.clone(),
            target: Some(put_file_request::Target::NodeId(plan_node_id.clone())),
            content: b"milestone alpha\nmilestone beta".to_vec(),
            if_version: 0,
            mode: 0,
            ..Default::default()
        })
        .await
        .expect("put file");

    let looked_up = namespace
        .lookup(LookupRequest {
            workspace_id: workspace_id.clone(),
            parent_node_id: docs_node_id.clone(),
            name: "plan.md".to_string(),
        })
        .await
        .expect("lookup")
        .into_inner();
    assert_eq!(looked_up.node_id, plan_node_id);

    let attrs = namespace
        .get_attrs(GetAttrsRequest {
            workspace_id: workspace_id.clone(),
            node_id: plan_node_id.clone(),
        })
        .await
        .expect("get attrs")
        .into_inner();
    let attrs_inner = attrs.attrs.expect("attrs");
    assert_eq!(attrs_inner.kind, NodeKind::File as i32);
    assert!(attrs_inner.size > 0);

    let dir_entries = namespace
        .read_dir(ReadDirRequest {
            workspace_id: workspace_id.clone(),
            node_id: docs_node_id.clone(),
            cursor: String::new(),
            limit: 128,
        })
        .await
        .expect("read dir")
        .into_inner();
    assert_eq!(dir_entries.entries.len(), 1);
    assert_eq!(dir_entries.entries[0].name, "plan.md");

    mutation
        .rename(RenameRequest {
            workspace_id: workspace_id.clone(),
            from_parent_node_id: docs_node_id.clone(),
            from_name: "plan.md".to_string(),
            to_parent_node_id: docs_node_id.clone(),
            to_name: "roadmap.md".to_string(),
            overwrite: false,
            if_version: 0,
        })
        .await
        .expect("rename");

    let resolved = namespace
        .resolve_path(ResolvePathRequest {
            workspace_id: workspace_id.clone(),
            path: "docs/roadmap.md".to_string(),
        })
        .await
        .expect("resolve renamed")
        .into_inner();
    assert!(resolved.exists);
    assert_eq!(resolved.node_id, plan_node_id);

    let mut file_stream = files
        .get_file(GetFileRequest {
            workspace_id: workspace_id.clone(),
            path: "docs/roadmap.md".to_string(),
        })
        .await
        .expect("get file")
        .into_inner();
    let mut bytes = Vec::new();
    while let Some(chunk) = file_stream.message().await.expect("stream chunk") {
        bytes.extend(chunk.data);
        if chunk.eof {
            break;
        }
    }
    assert_eq!(bytes, b"milestone alpha\nmilestone beta");

    let hits = search
        .search(SearchRequest {
            workspace_id: workspace_id.clone(),
            query: "milestone".to_string(),
            mode: SearchMode::Fts as i32,
            limit: 10,
        })
        .await
        .expect("search")
        .into_inner();
    assert_eq!(hits.total_hits, 1);
    assert_eq!(hits.hits[0].path, "docs/roadmap.md");

    let hyphen_literal_hits = search
        .search(SearchRequest {
            workspace_id: workspace_id.clone(),
            query: "milestone-beta".to_string(),
            mode: SearchMode::Fts as i32,
            limit: 10,
        })
        .await
        .expect("fts literal with hyphen")
        .into_inner();
    assert!(
        hyphen_literal_hits.total_hits >= 1,
        "literal fts should handle hyphen without parser errors"
    );

    let vector_hits = search
        .search(SearchRequest {
            workspace_id: workspace_id.clone(),
            query: "milestone beta planning".to_string(),
            mode: SearchMode::Vector as i32,
            limit: 10,
        })
        .await
        .expect("vector search")
        .into_inner();
    assert!(vector_hits.total_hits >= 1);

    let hybrid_hits = search
        .search(SearchRequest {
            workspace_id: workspace_id.clone(),
            query: "milestone beta".to_string(),
            mode: SearchMode::Hybrid as i32,
            limit: 10,
        })
        .await
        .expect("hybrid search")
        .into_inner();
    assert!(hybrid_hits.total_hits >= 1);

    mutation
        .unlink(UnlinkRequest {
            workspace_id: workspace_id.clone(),
            parent_node_id: docs_node_id.clone(),
            name: "roadmap.md".to_string(),
            if_version: 0,
        })
        .await
        .expect("unlink file");
    mutation
        .unlink(UnlinkRequest {
            workspace_id,
            parent_node_id: root_node_id,
            name: "docs".to_string(),
            if_version: 0,
        })
        .await
        .expect("unlink dir");

    let _ = shutdown_tx.send(());
    server_task.await.expect("server task");
}

#[tokio::test]
async fn put_file_target_oneof_and_compat_path_work() {
    let (_tmp, shutdown_tx, server_task, channel, _state) = spawn_test_server().await;
    let mut admin = AdminClient::new(channel.clone());
    let mut mutation = MutationClient::new(channel.clone());
    let mut namespace = NamespaceClient::new(channel.clone());
    let mut files = FilesClient::new(channel);

    let create_ws = admin
        .create_workspace(CreateWorkspaceRequest {
            name: "put-target".to_string(),
            config: None,
        })
        .await
        .expect("create workspace")
        .into_inner();
    let workspace_id = create_ws.workspace_id;

    let docs = mutation
        .create(CreateRequest {
            workspace_id: workspace_id.clone(),
            parent_node_id: create_ws.root_node_id,
            name: "docs".to_string(),
            kind: NodeKind::Dir as i32,
            mode: 0,
            exclusive: true,
        })
        .await
        .expect("create docs")
        .into_inner();

    files
        .put_file(PutFileRequest {
            workspace_id: workspace_id.clone(),
            target: Some(put_file_request::Target::PutFilePath(
                scry_proto::scry::v1::PutFilePath {
                    parent_node_id: docs.node_id.clone(),
                    name: "target.md".to_string(),
                },
            )),
            content: b"target-v1".to_vec(),
            if_version: 0,
            mode: 0,
            ..Default::default()
        })
        .await
        .expect("put via put_file_path target");

    let target_ref = namespace
        .resolve_path(ResolvePathRequest {
            workspace_id: workspace_id.clone(),
            path: "docs/target.md".to_string(),
        })
        .await
        .expect("resolve target path")
        .into_inner();
    assert!(target_ref.exists);

    files
        .put_file(PutFileRequest {
            workspace_id: workspace_id.clone(),
            target: Some(put_file_request::Target::NodeId(target_ref.node_id.clone())),
            content: b"target-v2".to_vec(),
            if_version: 0,
            mode: 0,
            ..Default::default()
        })
        .await
        .expect("put via node_id target");

    let mut stream = files
        .get_file(GetFileRequest {
            workspace_id: workspace_id.clone(),
            path: "docs/target.md".to_string(),
        })
        .await
        .expect("get updated file")
        .into_inner();
    let mut updated = Vec::new();
    while let Some(chunk) = stream.message().await.expect("stream chunk") {
        updated.extend(chunk.data);
        if chunk.eof {
            break;
        }
    }
    assert_eq!(updated, b"target-v2");

    #[allow(deprecated)]
    {
        files
            .put_file(PutFileRequest {
                workspace_id: workspace_id.clone(),
                path: "docs/compat.md".to_string(),
                target: None,
                content: b"compat-path".to_vec(),
                if_version: 0,
                mode: 0,
            })
            .await
            .expect("put via deprecated path");
    }

    let compat_ref = namespace
        .resolve_path(ResolvePathRequest {
            workspace_id: workspace_id.clone(),
            path: "docs/compat.md".to_string(),
        })
        .await
        .expect("resolve compat path")
        .into_inner();
    assert!(compat_ref.exists);

    let err = files
        .put_file(PutFileRequest {
            workspace_id,
            target: None,
            content: b"invalid".to_vec(),
            if_version: 0,
            mode: 0,
            ..Default::default()
        })
        .await
        .expect_err("missing target should fail");
    assert_eq!(err.code(), Code::InvalidArgument);

    let _ = shutdown_tx.send(());
    server_task.await.expect("server task");
}

#[tokio::test]
async fn get_file_streams_with_configured_chunk_size() {
    let (_tmp, shutdown_tx, server_task, channel, _state) = spawn_test_server_with_transport(
        Duration::from_millis(AppState::DEFAULT_HANDLE_IDLE_TIMEOUT_MS),
        DEFAULT_MAX_MESSAGE_BYTES,
        DEFAULT_MAX_MESSAGE_BYTES,
        5,
        DEFAULT_IO_HANDLE_MEMORY_LIMIT,
        8192,
    )
    .await;
    let mut admin = AdminClient::new(channel.clone());
    let mut files = FilesClient::new(channel);

    let create_ws = admin
        .create_workspace(CreateWorkspaceRequest {
            name: "stream-chunks".to_string(),
            config: None,
        })
        .await
        .expect("create workspace")
        .into_inner();
    let workspace_id = create_ws.workspace_id;
    let payload = b"abcdefghijklm".to_vec();

    files
        .put_file(PutFileRequest {
            workspace_id: workspace_id.clone(),
            target: Some(put_file_request::Target::PutFilePath(
                scry_proto::scry::v1::PutFilePath {
                    parent_node_id: create_ws.root_node_id.clone(),
                    name: "chunked.txt".to_string(),
                },
            )),
            content: payload.clone(),
            if_version: 0,
            mode: 0,
            ..Default::default()
        })
        .await
        .expect("put chunked file");

    let mut stream = files
        .get_file(GetFileRequest {
            workspace_id,
            path: "chunked.txt".to_string(),
        })
        .await
        .expect("get chunked file")
        .into_inner();

    let mut chunks = Vec::new();
    while let Some(chunk) = stream.message().await.expect("stream chunk") {
        chunks.push(chunk);
        if chunks.last().expect("chunk").eof {
            break;
        }
    }

    assert_eq!(chunks.len(), 3);
    assert_eq!(chunks[0].data.len(), 5);
    assert_eq!(chunks[1].data.len(), 5);
    assert_eq!(chunks[2].data.len(), 3);
    assert!(!chunks[0].eof);
    assert!(!chunks[1].eof);
    assert!(chunks[2].eof);

    let combined = chunks
        .into_iter()
        .flat_map(|chunk| chunk.data.into_iter())
        .collect::<Vec<_>>();
    assert_eq!(combined, payload);

    let _ = shutdown_tx.send(());
    server_task.await.expect("server task");
}

#[tokio::test]
async fn resolve_ref_includes_redirect_to_node_id() {
    let (tmp, shutdown_tx, server_task, channel, _state) = spawn_test_server().await;
    let mut admin = AdminClient::new(channel.clone());
    let mut mutation = MutationClient::new(channel.clone());
    let mut namespace = NamespaceClient::new(channel);

    let create_ws = admin
        .create_workspace(CreateWorkspaceRequest {
            name: "resolve-ref-redirect".to_string(),
            config: None,
        })
        .await
        .expect("create workspace")
        .into_inner();
    let workspace_id = create_ws.workspace_id;

    let docs = mutation
        .create(CreateRequest {
            workspace_id: workspace_id.clone(),
            parent_node_id: create_ws.root_node_id.clone(),
            name: "docs".to_string(),
            kind: NodeKind::Dir as i32,
            mode: 0,
            exclusive: true,
        })
        .await
        .expect("create docs")
        .into_inner();
    let final_file = mutation
        .create(CreateRequest {
            workspace_id: workspace_id.clone(),
            parent_node_id: docs.node_id.clone(),
            name: "final.md".to_string(),
            kind: NodeKind::File as i32,
            mode: 0,
            exclusive: true,
        })
        .await
        .expect("create final file")
        .into_inner();

    let node_a = uuid::Uuid::now_v7().to_string();
    let node_b = uuid::Uuid::now_v7().to_string();
    let workspace_db = tmp
        .path()
        .join("index")
        .join(&workspace_id)
        .join("index.db");
    let index_store = IndexStore::open(&workspace_db).expect("open per-workspace index store");
    index_store
        .set_redirect(&workspace_id, &node_a, &node_b, "renamed")
        .expect("insert A->B redirect");
    index_store
        .set_redirect(&workspace_id, &node_b, &final_file.node_id, "renamed")
        .expect("insert B->final redirect");

    let resolved = namespace
        .resolve_ref(ResolveRefRequest {
            workspace_id,
            node_id: node_a,
        })
        .await
        .expect("resolve ref")
        .into_inner();
    assert!(resolved.exists);
    assert_eq!(resolved.path, "docs/final.md");
    assert_eq!(resolved.redirect_to_node_id, node_b);

    let _ = shutdown_tx.send(());
    server_task.await.expect("server task");
}

#[tokio::test]
async fn grpc_admin_list_workspaces_uses_catalog_pagination() {
    let (_tmp, shutdown_tx, server_task, channel, _state) = spawn_test_server().await;
    let mut admin = AdminClient::new(channel);

    for idx in 0..3_u32 {
        admin
            .create_workspace(CreateWorkspaceRequest {
                name: format!("catalog-list-{idx}"),
                config: None,
            })
            .await
            .expect("create workspace for list");
    }

    let first = admin
        .list_workspaces(ListWorkspacesRequest {
            limit: 2,
            cursor: String::new(),
        })
        .await
        .expect("list first page")
        .into_inner();
    assert_eq!(first.workspaces.len(), 2);
    assert!(!first.next_cursor.is_empty());

    let second = admin
        .list_workspaces(ListWorkspacesRequest {
            limit: 2,
            cursor: first.next_cursor.clone(),
        })
        .await
        .expect("list second page")
        .into_inner();
    assert!(!second.workspaces.is_empty());

    let mut ids = first
        .workspaces
        .iter()
        .map(|row| row.workspace_id.clone())
        .collect::<Vec<_>>();
    ids.extend(second.workspaces.iter().map(|row| row.workspace_id.clone()));
    ids.sort();
    ids.dedup();
    assert!(
        ids.len() >= 3,
        "expected all created workspaces to be listed"
    );

    let _ = shutdown_tx.send(());
    server_task.await.expect("server task");
}

#[tokio::test]
async fn grpc_failure_paths_return_expected_status_codes() {
    let (_tmp, shutdown_tx, server_task, channel, _state) = spawn_test_server().await;
    let mut admin = AdminClient::new(channel.clone());
    let mut namespace = NamespaceClient::new(channel.clone());
    let mut mutation = MutationClient::new(channel.clone());

    let create_ws = admin
        .create_workspace(CreateWorkspaceRequest {
            name: "failure-paths".to_string(),
            config: None,
        })
        .await
        .expect("create workspace")
        .into_inner();
    let workspace_id = create_ws.workspace_id;
    let root_node_id = create_ws.root_node_id;

    let docs = mutation
        .create(CreateRequest {
            workspace_id: workspace_id.clone(),
            parent_node_id: root_node_id.clone(),
            name: "docs".to_string(),
            kind: NodeKind::Dir as i32,
            mode: 0,
            exclusive: true,
        })
        .await
        .expect("create docs")
        .into_inner();
    let docs_node_id = docs.node_id;

    mutation
        .create(CreateRequest {
            workspace_id: workspace_id.clone(),
            parent_node_id: docs_node_id.clone(),
            name: "note.md".to_string(),
            kind: NodeKind::File as i32,
            mode: 0,
            exclusive: true,
        })
        .await
        .expect("create note file");

    // already exists (exclusive create)
    let err = mutation
        .create(CreateRequest {
            workspace_id: workspace_id.clone(),
            parent_node_id: docs_node_id.clone(),
            name: "note.md".to_string(),
            kind: NodeKind::File as i32,
            mode: 0,
            exclusive: true,
        })
        .await
        .expect_err("duplicate create must fail");
    assert_eq!(err.code(), Code::AlreadyExists);

    // rename destination exists when overwrite=false
    mutation
        .create(CreateRequest {
            workspace_id: workspace_id.clone(),
            parent_node_id: docs_node_id.clone(),
            name: "other.md".to_string(),
            kind: NodeKind::File as i32,
            mode: 0,
            exclusive: true,
        })
        .await
        .expect("create other file");
    let err = mutation
        .rename(RenameRequest {
            workspace_id: workspace_id.clone(),
            from_parent_node_id: docs_node_id.clone(),
            from_name: "note.md".to_string(),
            to_parent_node_id: docs_node_id.clone(),
            to_name: "other.md".to_string(),
            overwrite: false,
            if_version: 0,
        })
        .await
        .expect_err("rename conflict must fail");
    assert_eq!(err.code(), Code::AlreadyExists);

    // non-empty directory cannot unlink
    let err = mutation
        .unlink(UnlinkRequest {
            workspace_id: workspace_id.clone(),
            parent_node_id: root_node_id.clone(),
            name: "docs".to_string(),
            if_version: 0,
        })
        .await
        .expect_err("unlink non-empty dir must fail");
    assert_eq!(err.code(), Code::FailedPrecondition);

    // invalid readdir cursor must be invalid argument
    let err = namespace
        .read_dir(ReadDirRequest {
            workspace_id: workspace_id.clone(),
            node_id: docs_node_id.clone(),
            cursor: "bad-cursor".to_string(),
            limit: 10,
        })
        .await
        .expect_err("invalid cursor must fail");
    assert_eq!(err.code(), Code::InvalidArgument);

    // missing lookup -> not found
    let err = namespace
        .lookup(LookupRequest {
            workspace_id: workspace_id.clone(),
            parent_node_id: docs_node_id,
            name: "missing.md".to_string(),
        })
        .await
        .expect_err("lookup missing node must fail");
    assert_eq!(err.code(), Code::NotFound);

    let _ = shutdown_tx.send(());
    server_task.await.expect("server task");
}

#[tokio::test]
async fn grpc_namespace_and_admin_p1_rpcs_work() {
    let (_tmp, shutdown_tx, server_task, channel, _state) = spawn_test_server().await;
    let mut admin = AdminClient::new(channel.clone());
    let mut namespace = NamespaceClient::new(channel.clone());
    let mut mutation = MutationClient::new(channel.clone());
    let mut files = FilesClient::new(channel);

    let create_ws = admin
        .create_workspace(CreateWorkspaceRequest {
            name: "p1-rpcs".to_string(),
            config: None,
        })
        .await
        .expect("create workspace")
        .into_inner();
    let workspace_id = create_ws.workspace_id;
    let root_node_id = create_ws.root_node_id;

    let docs = mutation
        .create(CreateRequest {
            workspace_id: workspace_id.clone(),
            parent_node_id: root_node_id.clone(),
            name: "docs".to_string(),
            kind: NodeKind::Dir as i32,
            mode: 0,
            exclusive: true,
        })
        .await
        .expect("create docs")
        .into_inner();

    let note = mutation
        .create(CreateRequest {
            workspace_id: workspace_id.clone(),
            parent_node_id: docs.node_id.clone(),
            name: "note.md".to_string(),
            kind: NodeKind::File as i32,
            mode: 0,
            exclusive: true,
        })
        .await
        .expect("create note")
        .into_inner();
    let note_node_id = note.node_id;

    files
        .put_file(PutFileRequest {
            workspace_id: workspace_id.clone(),
            target: Some(put_file_request::Target::NodeId(note_node_id.clone())),
            content: b"p1 namespace admin test".to_vec(),
            if_version: 0,
            mode: 0,
            ..Default::default()
        })
        .await
        .expect("put file");

    let attrs = namespace
        .get_attrs(GetAttrsRequest {
            workspace_id: workspace_id.clone(),
            node_id: note_node_id.clone(),
        })
        .await
        .expect("get attrs")
        .into_inner()
        .attrs
        .expect("attrs");

    let unchanged = namespace
        .get_attrs_if_changed(GetAttrsIfChangedRequest {
            workspace_id: workspace_id.clone(),
            node_id: note_node_id.clone(),
            known_version: attrs.version,
        })
        .await
        .expect("get attrs unchanged")
        .into_inner();
    assert!(!unchanged.changed);
    assert!(unchanged.attrs.is_none());

    files
        .put_file(PutFileRequest {
            workspace_id: workspace_id.clone(),
            target: Some(put_file_request::Target::NodeId(note_node_id.clone())),
            content: b"p1 namespace admin changed".to_vec(),
            if_version: 0,
            mode: 0,
            ..Default::default()
        })
        .await
        .expect("put updated file");

    let changed = namespace
        .get_attrs_if_changed(GetAttrsIfChangedRequest {
            workspace_id: workspace_id.clone(),
            node_id: note_node_id.clone(),
            known_version: attrs.version,
        })
        .await
        .expect("get attrs changed")
        .into_inner();
    assert!(changed.changed);
    let changed_attrs = changed.attrs.expect("changed attrs");
    assert!(changed_attrs.version > attrs.version);

    mutation
        .rename(RenameRequest {
            workspace_id: workspace_id.clone(),
            from_parent_node_id: docs.node_id.clone(),
            from_name: "note.md".to_string(),
            to_parent_node_id: docs.node_id.clone(),
            to_name: "renamed.md".to_string(),
            overwrite: false,
            if_version: 0,
        })
        .await
        .expect("rename file");

    let resolved_ref = namespace
        .resolve_ref(ResolveRefRequest {
            workspace_id: workspace_id.clone(),
            node_id: note_node_id.clone(),
        })
        .await
        .expect("resolve ref")
        .into_inner();
    assert!(resolved_ref.exists);
    assert_eq!(resolved_ref.path, "docs/renamed.md");

    let stats = admin
        .get_workspace_stats(GetWorkspaceStatsRequest {
            workspace_id: workspace_id.clone(),
        })
        .await
        .expect("workspace stats")
        .into_inner();
    assert!(stats.file_count >= 1);
    assert!(stats.dir_count >= 2);
    assert!(stats.chunk_count >= 1);
    assert!(stats.total_content_bytes > 0);

    let list = admin
        .list_workspaces(ListWorkspacesRequest {
            limit: 10,
            cursor: String::new(),
        })
        .await
        .expect("list workspaces")
        .into_inner();
    assert!(
        list.workspaces
            .iter()
            .any(|workspace| workspace.workspace_id == workspace_id),
        "created workspace should appear in catalog-backed list"
    );

    let reindex = admin
        .reindex_workspace(ReindexWorkspaceRequest {
            workspace_id: workspace_id.clone(),
        })
        .await
        .expect("reindex workspace")
        .into_inner();
    assert!(reindex.enqueued_jobs >= 1);

    admin
        .delete_workspace(DeleteWorkspaceRequest {
            workspace_id: workspace_id.clone(),
        })
        .await
        .expect("delete workspace");

    let missing_ref = namespace
        .resolve_ref(ResolveRefRequest {
            workspace_id: workspace_id.clone(),
            node_id: note_node_id.clone(),
        })
        .await
        .expect_err("resolve ref on deleted workspace should fail");
    assert_eq!(missing_ref.code(), Code::NotFound);

    let _ = shutdown_tx.send(());
    server_task.await.expect("server task");
}

#[tokio::test]
async fn grpc_admin_reindex_and_delete_workspace_e2e() {
    let (tmp, shutdown_tx, server_task, channel, _state) = spawn_test_server().await;
    let mut admin = AdminClient::new(channel.clone());
    let mut mutation = MutationClient::new(channel.clone());
    let mut files = FilesClient::new(channel);

    let create_ws = admin
        .create_workspace(CreateWorkspaceRequest {
            name: "admin-e2e".to_string(),
            config: None,
        })
        .await
        .expect("create workspace")
        .into_inner();
    let workspace_id = create_ws.workspace_id;
    let root_node_id = create_ws.root_node_id;
    let workspace_content_dir = tmp.path().join("content").join(&workspace_id);
    assert!(
        workspace_content_dir.exists(),
        "workspace content directory should exist after create"
    );

    let docs = mutation
        .create(CreateRequest {
            workspace_id: workspace_id.clone(),
            parent_node_id: root_node_id,
            name: "docs".to_string(),
            kind: NodeKind::Dir as i32,
            mode: 0,
            exclusive: true,
        })
        .await
        .expect("create docs")
        .into_inner();

    let note = mutation
        .create(CreateRequest {
            workspace_id: workspace_id.clone(),
            parent_node_id: docs.node_id.clone(),
            name: "note.md".to_string(),
            kind: NodeKind::File as i32,
            mode: 0,
            exclusive: true,
        })
        .await
        .expect("create note file")
        .into_inner();
    files
        .put_file(PutFileRequest {
            workspace_id: workspace_id.clone(),
            target: Some(put_file_request::Target::NodeId(note.node_id)),
            content: b"e2e reindex payload".to_vec(),
            if_version: 0,
            mode: 0,
            ..Default::default()
        })
        .await
        .expect("put note");

    let stats_before = admin
        .get_workspace_stats(GetWorkspaceStatsRequest {
            workspace_id: workspace_id.clone(),
        })
        .await
        .expect("stats before reindex")
        .into_inner();
    let total_jobs_before = stats_before.queued_jobs
        + stats_before.running_jobs
        + stats_before.completed_jobs
        + stats_before.failed_jobs;
    assert!(stats_before.file_count >= 1);

    let reindex = admin
        .reindex_workspace(ReindexWorkspaceRequest {
            workspace_id: workspace_id.clone(),
        })
        .await
        .expect("reindex workspace")
        .into_inner();
    assert!(
        reindex.enqueued_jobs >= 1,
        "reindex should enqueue file jobs"
    );

    let stats_after = admin
        .get_workspace_stats(GetWorkspaceStatsRequest {
            workspace_id: workspace_id.clone(),
        })
        .await
        .expect("stats after reindex")
        .into_inner();
    let total_jobs_after = stats_after.queued_jobs
        + stats_after.running_jobs
        + stats_after.completed_jobs
        + stats_after.failed_jobs;
    assert!(
        total_jobs_after >= total_jobs_before + u64::from(reindex.enqueued_jobs),
        "reindex should increase total persisted job count"
    );

    admin
        .delete_workspace(DeleteWorkspaceRequest {
            workspace_id: workspace_id.clone(),
        })
        .await
        .expect("delete workspace");
    assert!(
        !workspace_content_dir.exists(),
        "workspace content directory should be removed after delete"
    );

    let missing_stats = admin
        .get_workspace_stats(GetWorkspaceStatsRequest { workspace_id })
        .await
        .expect_err("deleted workspace stats should fail");
    assert_eq!(missing_stats.code(), Code::NotFound);

    let _ = shutdown_tx.send(());
    server_task.await.expect("server task");
}

#[tokio::test]
async fn grpc_events_subscribe_replay_and_acknowledge() {
    let (_tmp, shutdown_tx, server_task, channel, _state) = spawn_test_server().await;
    let mut admin = AdminClient::new(channel.clone());
    let mut mutation = MutationClient::new(channel.clone());
    let mut files = FilesClient::new(channel.clone());
    let mut events = EventsClient::new(channel);

    let create_ws = admin
        .create_workspace(CreateWorkspaceRequest {
            name: "events".to_string(),
            config: None,
        })
        .await
        .expect("create workspace")
        .into_inner();
    let workspace_id = create_ws.workspace_id;
    let root_node_id = create_ws.root_node_id;

    let docs = mutation
        .create(CreateRequest {
            workspace_id: workspace_id.clone(),
            parent_node_id: root_node_id.clone(),
            name: "docs".to_string(),
            kind: NodeKind::Dir as i32,
            mode: 0,
            exclusive: true,
        })
        .await
        .expect("create docs")
        .into_inner();

    let note = mutation
        .create(CreateRequest {
            workspace_id: workspace_id.clone(),
            parent_node_id: docs.node_id.clone(),
            name: "note.md".to_string(),
            kind: NodeKind::File as i32,
            mode: 0,
            exclusive: true,
        })
        .await
        .expect("create note file")
        .into_inner();

    files
        .put_file(PutFileRequest {
            workspace_id: workspace_id.clone(),
            target: Some(put_file_request::Target::NodeId(note.node_id.clone())),
            content: b"event payload".to_vec(),
            if_version: 0,
            mode: 0,
            ..Default::default()
        })
        .await
        .expect("put file");

    let mut stream = events
        .subscribe(SubscribeRequest {
            workspace_id: workspace_id.clone(),
            since_cursor: "0".to_string(),
            subscriber_id: "sub-a".to_string(),
            filter: Some(SubscribeFilter {
                path_prefix: vec!["docs".to_string()],
                node_ids: vec![],
                kinds: vec![],
                include_index_events: false,
            }),
        })
        .await
        .expect("subscribe")
        .into_inner();

    let mut saw_created = false;
    let mut saw_modified = false;
    let mut last_cursor = String::new();

    for _ in 0..10 {
        let maybe_event =
            tokio::time::timeout(std::time::Duration::from_millis(800), stream.message())
                .await
                .expect("event read timeout")
                .expect("stream event");
        if let Some(event) = maybe_event {
            last_cursor = event.cursor.clone();
            if event.kind == scry_proto::scry::v1::EventKind::NodeCreated as i32 {
                saw_created = true;
            }
            if event.kind == scry_proto::scry::v1::EventKind::NodeModified as i32
                && event.node_id == docs.node_id
            {
                // ignore dir-created events for modified check
            } else if event.kind == scry_proto::scry::v1::EventKind::NodeModified as i32 {
                saw_modified = true;
            }
            if saw_created && saw_modified {
                break;
            }
        }
    }

    assert!(saw_created, "expected at least one NodeCreated event");
    assert!(saw_modified, "expected at least one NodeModified event");
    assert!(!last_cursor.is_empty(), "expected cursor from stream");

    let mut index_event_stream = events
        .subscribe(SubscribeRequest {
            workspace_id: workspace_id.clone(),
            since_cursor: "0".to_string(),
            subscriber_id: "index-sub".to_string(),
            filter: Some(SubscribeFilter {
                path_prefix: vec!["docs".to_string()],
                node_ids: vec![],
                kinds: vec![scry_proto::scry::v1::EventKind::IndexUpdated as i32],
                include_index_events: true,
            }),
        })
        .await
        .expect("subscribe index events")
        .into_inner();

    let mut saw_index_updated = false;
    for _ in 0..10 {
        let maybe_event = tokio::time::timeout(
            std::time::Duration::from_millis(800),
            index_event_stream.message(),
        )
        .await
        .expect("index event timeout")
        .expect("index stream call");
        if let Some(event) = maybe_event {
            if event.kind == scry_proto::scry::v1::EventKind::IndexUpdated as i32 {
                saw_index_updated = true;
                break;
            }
        }
    }
    assert!(
        saw_index_updated,
        "expected at least one IndexUpdated event with include_index_events=true"
    );
    events
        .acknowledge(AcknowledgeRequest {
            workspace_id: workspace_id.clone(),
            cursor: last_cursor,
            subscriber_id: "sub-a".to_string(),
        })
        .await
        .expect("acknowledge");

    files
        .put_file(PutFileRequest {
            workspace_id: workspace_id.clone(),
            target: Some(put_file_request::Target::NodeId(note.node_id.clone())),
            content: b"event payload changed".to_vec(),
            if_version: 0,
            mode: 0,
            ..Default::default()
        })
        .await
        .expect("put second file update");
    let mut resumed = events
        .subscribe(SubscribeRequest {
            workspace_id,
            since_cursor: String::new(),
            subscriber_id: "sub-a".to_string(),
            filter: Some(SubscribeFilter {
                path_prefix: vec!["docs".to_string()],
                node_ids: vec![],
                kinds: vec![],
                include_index_events: false,
            }),
        })
        .await
        .expect("resubscribe from ack")
        .into_inner();
    let resumed_event =
        tokio::time::timeout(std::time::Duration::from_millis(1000), resumed.message())
            .await
            .expect("resumed timeout")
            .expect("resumed stream call")
            .expect("resumed event");
    assert_eq!(
        resumed_event.kind,
        scry_proto::scry::v1::EventKind::NodeModified as i32
    );
    assert_eq!(resumed_event.path, "docs/note.md");

    drop(index_event_stream);
    drop(resumed);
    drop(stream);
    drop(events);
    drop(files);
    drop(mutation);
    drop(admin);
    let _ = shutdown_tx.send(());
    server_task.await.expect("server task");
}

#[tokio::test]
async fn grpc_events_filtering_and_subscriber_resume() {
    let (_tmp, shutdown_tx, server_task, channel, _state) = spawn_test_server().await;
    let mut admin = AdminClient::new(channel.clone());
    let mut mutation = MutationClient::new(channel.clone());
    let mut files = FilesClient::new(channel.clone());
    let mut events = EventsClient::new(channel);

    let create_ws = admin
        .create_workspace(CreateWorkspaceRequest {
            name: "events-filter".to_string(),
            config: None,
        })
        .await
        .expect("create workspace")
        .into_inner();
    let workspace_id = create_ws.workspace_id;
    let root_node_id = create_ws.root_node_id;

    let docs = mutation
        .create(CreateRequest {
            workspace_id: workspace_id.clone(),
            parent_node_id: root_node_id.clone(),
            name: "docs".to_string(),
            kind: NodeKind::Dir as i32,
            mode: 0,
            exclusive: true,
        })
        .await
        .expect("create docs")
        .into_inner();
    let logs = mutation
        .create(CreateRequest {
            workspace_id: workspace_id.clone(),
            parent_node_id: root_node_id,
            name: "logs".to_string(),
            kind: NodeKind::Dir as i32,
            mode: 0,
            exclusive: true,
        })
        .await
        .expect("create logs")
        .into_inner();

    files
        .put_file(PutFileRequest {
            workspace_id: workspace_id.clone(),
            target: Some(put_file_request::Target::PutFilePath(
                scry_proto::scry::v1::PutFilePath {
                    parent_node_id: docs.node_id.clone(),
                    name: "note.md".to_string(),
                },
            )),
            content: b"docs data".to_vec(),
            if_version: 0,
            mode: 0,
            ..Default::default()
        })
        .await
        .expect("put docs file");
    files
        .put_file(PutFileRequest {
            workspace_id: workspace_id.clone(),
            target: Some(put_file_request::Target::PutFilePath(
                scry_proto::scry::v1::PutFilePath {
                    parent_node_id: logs.node_id.clone(),
                    name: "log.txt".to_string(),
                },
            )),
            content: b"log data".to_vec(),
            if_version: 0,
            mode: 0,
            ..Default::default()
        })
        .await
        .expect("put logs file");

    let mut docs_stream = events
        .subscribe(SubscribeRequest {
            workspace_id: workspace_id.clone(),
            since_cursor: "0".to_string(),
            subscriber_id: "docs-sub".to_string(),
            filter: Some(SubscribeFilter {
                path_prefix: vec!["docs".to_string()],
                node_ids: vec![],
                kinds: vec![],
                include_index_events: false,
            }),
        })
        .await
        .expect("subscribe docs")
        .into_inner();

    let mut docs_cursor = String::new();
    let mut saw_docs_event = false;
    for _ in 0..10 {
        let maybe_event =
            tokio::time::timeout(std::time::Duration::from_millis(800), docs_stream.message())
                .await
                .expect("docs timeout")
                .expect("docs stream call");
        if let Some(event) = maybe_event {
            docs_cursor = event.cursor.clone();
            if event.path.starts_with("docs/") {
                saw_docs_event = true;
                break;
            }
        }
    }
    assert!(saw_docs_event, "expected docs-scoped event");
    assert!(!docs_cursor.is_empty(), "expected docs cursor");

    events
        .acknowledge(AcknowledgeRequest {
            workspace_id: workspace_id.clone(),
            cursor: docs_cursor.clone(),
            subscriber_id: "docs-sub".to_string(),
        })
        .await
        .expect("ack docs cursor");

    files
        .put_file(PutFileRequest {
            workspace_id: workspace_id.clone(),
            target: Some(put_file_request::Target::PutFilePath(
                scry_proto::scry::v1::PutFilePath {
                    parent_node_id: docs.node_id.clone(),
                    name: "after-ack.md".to_string(),
                },
            )),
            content: b"after ack".to_vec(),
            if_version: 0,
            mode: 0,
            ..Default::default()
        })
        .await
        .expect("put docs after ack");

    let mut resumed_stream = events
        .subscribe(SubscribeRequest {
            workspace_id: workspace_id.clone(),
            since_cursor: String::new(),
            subscriber_id: "docs-sub".to_string(),
            filter: Some(SubscribeFilter {
                path_prefix: vec!["docs".to_string()],
                node_ids: vec![],
                kinds: vec![scry_proto::scry::v1::EventKind::NodeModified as i32],
                include_index_events: false,
            }),
        })
        .await
        .expect("resubscribe from ack")
        .into_inner();

    let resumed = tokio::time::timeout(
        std::time::Duration::from_millis(1200),
        resumed_stream.message(),
    )
    .await
    .expect("resume timeout")
    .expect("resume stream call")
    .expect("resume event");
    assert!(
        resumed.path.starts_with("docs/"),
        "resumed event must respect docs path filter"
    );
    assert!(
        resumed.cursor.parse::<u64>().expect("cursor int")
            > docs_cursor.parse::<u64>().expect("ack cursor int"),
        "resumed stream should start after acknowledged cursor"
    );
    assert_eq!(
        resumed.kind,
        scry_proto::scry::v1::EventKind::NodeModified as i32,
        "kinds filter should retain only NodeModified"
    );

    let mut node_filtered_stream = events
        .subscribe(SubscribeRequest {
            workspace_id: workspace_id.clone(),
            since_cursor: "0".to_string(),
            subscriber_id: "node-filter".to_string(),
            filter: Some(SubscribeFilter {
                path_prefix: vec![],
                node_ids: vec![logs.node_id.clone()],
                kinds: vec![],
                include_index_events: false,
            }),
        })
        .await
        .expect("subscribe node filter")
        .into_inner();

    let mut node_filter_ok = false;
    for _ in 0..10 {
        let maybe_event = tokio::time::timeout(
            std::time::Duration::from_millis(800),
            node_filtered_stream.message(),
        )
        .await
        .expect("node filter timeout")
        .expect("node filter stream call");
        if let Some(event) = maybe_event {
            if event.node_id == logs.node_id {
                node_filter_ok = true;
                break;
            }
        }
    }
    assert!(
        node_filter_ok,
        "expected at least one event constrained to requested node id"
    );

    let mut index_stream = events
        .subscribe(SubscribeRequest {
            workspace_id: workspace_id.clone(),
            since_cursor: "0".to_string(),
            subscriber_id: "index-sub".to_string(),
            filter: Some(SubscribeFilter {
                path_prefix: vec!["docs".to_string()],
                node_ids: vec![],
                kinds: vec![scry_proto::scry::v1::EventKind::IndexUpdated as i32],
                include_index_events: true,
            }),
        })
        .await
        .expect("subscribe index filter")
        .into_inner();
    let mut saw_index_event = false;
    for _ in 0..10 {
        let maybe_event = tokio::time::timeout(
            std::time::Duration::from_millis(800),
            index_stream.message(),
        )
        .await
        .expect("index timeout")
        .expect("index stream call");
        if let Some(event) = maybe_event {
            if event.kind == scry_proto::scry::v1::EventKind::IndexUpdated as i32
                && event.path.starts_with("docs/")
            {
                saw_index_event = true;
                break;
            }
        }
    }
    assert!(saw_index_event, "expected at least one index updated event");

    let mut index_stream_excluded = events
        .subscribe(SubscribeRequest {
            workspace_id: workspace_id.clone(),
            since_cursor: "0".to_string(),
            subscriber_id: "index-sub-excluded".to_string(),
            filter: Some(SubscribeFilter {
                path_prefix: vec!["docs".to_string()],
                node_ids: vec![],
                kinds: vec![],
                include_index_events: false,
            }),
        })
        .await
        .expect("subscribe excluded index filter")
        .into_inner();
    files
        .put_file(PutFileRequest {
            workspace_id: workspace_id.clone(),
            target: Some(put_file_request::Target::PutFilePath(
                scry_proto::scry::v1::PutFilePath {
                    parent_node_id: docs.node_id.clone(),
                    name: "index-trigger.md".to_string(),
                },
            )),
            content: b"trigger index events".to_vec(),
            if_version: 0,
            mode: 0,
            ..Default::default()
        })
        .await
        .expect("put file to trigger excluded stream");
    let mut saw_non_index = false;
    for _ in 0..10 {
        let maybe_event = tokio::time::timeout(
            std::time::Duration::from_millis(800),
            index_stream_excluded.message(),
        )
        .await
        .expect("excluded timeout")
        .expect("excluded stream call");
        if let Some(event) = maybe_event {
            assert_ne!(
                event.kind,
                scry_proto::scry::v1::EventKind::IndexUpdated as i32,
                "index events should be excluded when include_index_events=false"
            );
            saw_non_index = true;
            break;
        }
    }
    assert!(
        saw_non_index,
        "expected non-index event while index events remain filtered out"
    );

    drop(index_stream_excluded);
    drop(index_stream);
    drop(node_filtered_stream);
    drop(resumed_stream);
    drop(docs_stream);
    drop(events);
    drop(files);
    drop(mutation);
    drop(admin);
    let _ = shutdown_tx.send(());
    server_task.await.expect("server task");
}

#[tokio::test]
async fn grpc_io_handle_lifecycle_read_write_flush_release() {
    let (_tmp, shutdown_tx, server_task, channel, _state) = spawn_test_server().await;
    let mut admin = AdminClient::new(channel.clone());
    let mut mutation = MutationClient::new(channel.clone());
    let mut namespace = NamespaceClient::new(channel.clone());
    let mut io = IoClient::new(channel);

    let create_ws = admin
        .create_workspace(CreateWorkspaceRequest {
            name: "io-handle".to_string(),
            config: None,
        })
        .await
        .expect("create workspace")
        .into_inner();
    let workspace_id = create_ws.workspace_id;
    let root_node_id = create_ws.root_node_id;

    let docs = mutation
        .create(CreateRequest {
            workspace_id: workspace_id.clone(),
            parent_node_id: root_node_id.clone(),
            name: "docs".to_string(),
            kind: NodeKind::Dir as i32,
            mode: 0,
            exclusive: true,
        })
        .await
        .expect("create docs dir")
        .into_inner();
    let note = mutation
        .create(CreateRequest {
            workspace_id: workspace_id.clone(),
            parent_node_id: docs.node_id.clone(),
            name: "note.md".to_string(),
            kind: NodeKind::File as i32,
            mode: 0,
            exclusive: true,
        })
        .await
        .expect("create note file")
        .into_inner();

    let opened = io
        .open(OpenRequest {
            workspace_id: workspace_id.clone(),
            node_id: note.node_id.clone(),
            mode: HandleMode::ReadWrite as i32,
        })
        .await
        .expect("open read-write handle")
        .into_inner();
    assert!(!opened.handle_id.is_empty(), "handle id should exist");
    assert_eq!(opened.snapshot_version, 1);

    let first_write = io
        .write(WriteRequest {
            workspace_id: workspace_id.clone(),
            handle_id: opened.handle_id.clone(),
            offset: 0,
            data: b"hello world".to_vec(),
        })
        .await
        .expect("write initial content")
        .into_inner();
    assert_eq!(first_write.bytes_written, 11);

    let patch_write = io
        .write(WriteRequest {
            workspace_id: workspace_id.clone(),
            handle_id: opened.handle_id.clone(),
            offset: 6,
            data: b"scry".to_vec(),
        })
        .await
        .expect("write patch content")
        .into_inner();
    assert_eq!(patch_write.bytes_written, 4);

    let read_back = io
        .read(ReadRequest {
            workspace_id: workspace_id.clone(),
            handle_id: opened.handle_id.clone(),
            offset: 0,
            length: 64,
        })
        .await
        .expect("read back before flush")
        .into_inner();
    assert_eq!(read_back.data, b"hello scryd");
    assert!(read_back.eof);

    let truncated = io
        .truncate(TruncateRequest {
            workspace_id: workspace_id.clone(),
            handle_id: opened.handle_id.clone(),
            size: 5,
        })
        .await
        .expect("truncate content")
        .into_inner();
    let _ = truncated;

    let read_after_truncate = io
        .read(ReadRequest {
            workspace_id: workspace_id.clone(),
            handle_id: opened.handle_id.clone(),
            offset: 0,
            length: 64,
        })
        .await
        .expect("read after truncate")
        .into_inner();
    assert_eq!(read_after_truncate.data, b"hello");
    assert!(read_after_truncate.eof);

    let flushed = io
        .flush(FlushRequest {
            workspace_id: workspace_id.clone(),
            handle_id: opened.handle_id.clone(),
        })
        .await
        .expect("flush handle")
        .into_inner();
    assert_eq!(flushed.version, 2);
    assert_eq!(flushed.attrs.expect("flush attrs").size, 5);

    let attrs = namespace
        .get_attrs(GetAttrsRequest {
            workspace_id: workspace_id.clone(),
            node_id: note.node_id.clone(),
        })
        .await
        .expect("get attrs after flush")
        .into_inner()
        .attrs
        .expect("attrs body");
    assert_eq!(attrs.version, 2);
    assert_eq!(attrs.size, 5);

    let release = io
        .release(ReleaseRequest {
            workspace_id: workspace_id.clone(),
            handle_id: opened.handle_id.clone(),
        })
        .await
        .expect("release handle")
        .into_inner();
    assert_eq!(release.version, 2);

    let reopen_read = io
        .open(OpenRequest {
            workspace_id: workspace_id.clone(),
            node_id: note.node_id.clone(),
            mode: HandleMode::Read as i32,
        })
        .await
        .expect("open read handle")
        .into_inner();
    let persisted = io
        .read(ReadRequest {
            workspace_id: workspace_id.clone(),
            handle_id: reopen_read.handle_id.clone(),
            offset: 0,
            length: 64,
        })
        .await
        .expect("read persisted content")
        .into_inner();
    assert_eq!(persisted.data, b"hello");
    assert!(persisted.eof);
    io.release(ReleaseRequest {
        workspace_id: workspace_id.clone(),
        handle_id: reopen_read.handle_id,
    })
    .await
    .expect("release read handle");

    let _ = shutdown_tx.send(());
    server_task.await.expect("server task");
}

/// Large handle bodies spill to a tempfile when they exceed `SCRYD_IO_HANDLE_MEMORY_LIMIT`;
/// flush streams into the content store without materializing the full file in RAM.
#[tokio::test]
async fn io_large_file_spills_to_disk() {
    const HANDLE_MEM: usize = 64 * 1024;
    const CHUNK: usize = 64 * 1024;
    const TOTAL: usize = 4 * CHUNK;

    let (_tmp, shutdown_tx, server_task, channel, _state) = spawn_test_server_with_transport(
        Duration::from_millis(AppState::DEFAULT_HANDLE_IDLE_TIMEOUT_MS),
        DEFAULT_MAX_MESSAGE_BYTES,
        DEFAULT_MAX_MESSAGE_BYTES,
        DEFAULT_IO_STREAM_CHUNK_BYTES,
        HANDLE_MEM,
        8192,
    )
    .await;
    let mut admin = AdminClient::new(channel.clone());
    let mut mutation = MutationClient::new(channel.clone());
    let mut io = IoClient::new(channel);

    let create_ws = admin
        .create_workspace(CreateWorkspaceRequest {
            name: "io-large-spill".to_string(),
            config: None,
        })
        .await
        .expect("create workspace")
        .into_inner();
    let workspace_id = create_ws.workspace_id;
    let root_node_id = create_ws.root_node_id;

    let docs = mutation
        .create(CreateRequest {
            workspace_id: workspace_id.clone(),
            parent_node_id: root_node_id,
            name: "docs".to_string(),
            kind: NodeKind::Dir as i32,
            mode: 0,
            exclusive: true,
        })
        .await
        .expect("create docs dir")
        .into_inner();
    let big = mutation
        .create(CreateRequest {
            workspace_id: workspace_id.clone(),
            parent_node_id: docs.node_id,
            name: "big.bin".to_string(),
            kind: NodeKind::File as i32,
            mode: 0,
            exclusive: true,
        })
        .await
        .expect("create big file node")
        .into_inner();

    let opened = io
        .open(OpenRequest {
            workspace_id: workspace_id.clone(),
            node_id: big.node_id.clone(),
            mode: HandleMode::ReadWrite as i32,
        })
        .await
        .expect("open read-write handle")
        .into_inner();

    for i in 0..4u64 {
        let fill = 0x41u8.wrapping_add(i as u8);
        io.write(WriteRequest {
            workspace_id: workspace_id.clone(),
            handle_id: opened.handle_id.clone(),
            offset: i * CHUNK as u64,
            data: vec![fill; CHUNK],
        })
        .await
        .expect("write chunk")
        .into_inner();
    }

    io.flush(FlushRequest {
        workspace_id: workspace_id.clone(),
        handle_id: opened.handle_id.clone(),
    })
    .await
    .expect("flush large handle")
    .into_inner();

    io.release(ReleaseRequest {
        workspace_id: workspace_id.clone(),
        handle_id: opened.handle_id,
    })
    .await
    .expect("release handle")
    .into_inner();

    let verify = io
        .open(OpenRequest {
            workspace_id: workspace_id.clone(),
            node_id: big.node_id.clone(),
            mode: HandleMode::Read as i32,
        })
        .await
        .expect("reopen for verify")
        .into_inner();

    let mut got = Vec::with_capacity(TOTAL);
    let mut off = 0u64;
    while got.len() < TOTAL {
        let r = io
            .read(ReadRequest {
                workspace_id: workspace_id.clone(),
                handle_id: verify.handle_id.clone(),
                offset: off,
                length: CHUNK as u32,
            })
            .await
            .expect("read chunk")
            .into_inner();
        got.extend_from_slice(&r.data);
        off += r.data.len() as u64;
        if r.eof {
            break;
        }
    }
    assert_eq!(got.len(), TOTAL);
    for i in 0..4 {
        let fill = (0x41 + i) as u8;
        let slice = &got[i * CHUNK..(i + 1) * CHUNK];
        assert!(slice.iter().all(|&b| b == fill), "chunk {i} mismatch");
    }

    io.release(ReleaseRequest {
        workspace_id: workspace_id.clone(),
        handle_id: verify.handle_id,
    })
    .await
    .expect("release verify handle");

    let _ = shutdown_tx.send(());
    server_task.await.expect("server task");
}

/// After `IO.Flush`, headline-only indexing must not grow RSS linearly with file size (Linux: VmHWM).
/// Default file size 256 MiB; set `SCRY_LARGE_IO_RSS_TEST=1` for 1 GiB (slow; optional).
#[tokio::test]
async fn index_flush_large_file_rss() {
    const HANDLE_MEM: usize = 64 * 1024;
    const CHUNK: usize = 1024 * 1024;
    let total: u64 = if std::env::var("SCRY_LARGE_IO_RSS_TEST").ok().as_deref() == Some("1") {
        1024 * 1024 * 1024
    } else {
        256 * 1024 * 1024
    };
    let total_usize = usize::try_from(total).expect("file size fits usize");

    let _env = EnvGuard::set_many(&[
        ("SCRYD_INDEX_STREAM_CHUNK_BYTES", Some("1048576")),
        ("SCRYD_INDEX_SKIP_CHUNKING_OVER_BYTES", Some("67108864")),
    ]);

    let (_tmp, shutdown_tx, server_task, channel, _state) = spawn_test_server_with_transport(
        Duration::from_millis(AppState::DEFAULT_HANDLE_IDLE_TIMEOUT_MS),
        DEFAULT_MAX_MESSAGE_BYTES,
        DEFAULT_MAX_MESSAGE_BYTES,
        DEFAULT_IO_STREAM_CHUNK_BYTES,
        HANDLE_MEM,
        8192,
    )
    .await;
    let mut admin = AdminClient::new(channel.clone());
    let mut mutation = MutationClient::new(channel.clone());
    let mut io = IoClient::new(channel);

    let create_ws = admin
        .create_workspace(CreateWorkspaceRequest {
            name: "index-flush-rss".to_string(),
            config: None,
        })
        .await
        .expect("create workspace")
        .into_inner();
    let workspace_id = create_ws.workspace_id;
    let root_node_id = create_ws.root_node_id;

    let docs = mutation
        .create(CreateRequest {
            workspace_id: workspace_id.clone(),
            parent_node_id: root_node_id,
            name: "docs".to_string(),
            kind: NodeKind::Dir as i32,
            mode: 0,
            exclusive: true,
        })
        .await
        .expect("create docs dir")
        .into_inner();
    let big = mutation
        .create(CreateRequest {
            workspace_id: workspace_id.clone(),
            parent_node_id: docs.node_id,
            name: "big.txt".to_string(),
            kind: NodeKind::File as i32,
            mode: 0,
            exclusive: true,
        })
        .await
        .expect("create large file node")
        .into_inner();

    let opened = io
        .open(OpenRequest {
            workspace_id: workspace_id.clone(),
            node_id: big.node_id.clone(),
            mode: HandleMode::ReadWrite as i32,
        })
        .await
        .expect("open read-write handle")
        .into_inner();

    let mut written = 0u64;
    while written < total {
        let take = ((total - written) as usize).min(CHUNK);
        let fill = b'a' + (written % 23) as u8;
        io.write(WriteRequest {
            workspace_id: workspace_id.clone(),
            handle_id: opened.handle_id.clone(),
            offset: written,
            data: vec![fill; take],
        })
        .await
        .expect("write chunk")
        .into_inner();
        written += take as u64;
    }

    #[cfg(target_os = "linux")]
    let rss_before_kb = read_linux_vmhwm_kb().expect("read VmHWM before flush");

    io.flush(FlushRequest {
        workspace_id: workspace_id.clone(),
        handle_id: opened.handle_id.clone(),
    })
    .await
    .expect("flush large handle for index rss test")
    .into_inner();

    #[cfg(target_os = "linux")]
    {
        let rss_after_kb = read_linux_vmhwm_kb().expect("read VmHWM after flush");
        let delta_bytes = rss_after_kb
            .saturating_sub(rss_before_kb)
            .saturating_mul(1024);
        let index_stream_buf: u64 = 1024 * 1024;
        let slack: u64 = 160 * 1024 * 1024;
        let limit = (HANDLE_MEM as u64) + index_stream_buf + slack;
        assert!(
            delta_bytes <= limit,
            "VmHWM grew by {delta_bytes} bytes (limit {limit} = handle_cap + stream_chunk + slack); \
             before={rss_before_kb}kB after={rss_after_kb}kB — index flush should not allocate O(file_size)"
        );
    }

    io.release(ReleaseRequest {
        workspace_id: workspace_id.clone(),
        handle_id: opened.handle_id,
    })
    .await
    .expect("release handle")
    .into_inner();

    let verify = io
        .open(OpenRequest {
            workspace_id: workspace_id.clone(),
            node_id: big.node_id.clone(),
            mode: HandleMode::Read as i32,
        })
        .await
        .expect("reopen for verify")
        .into_inner();

    let mut got = 0usize;
    let mut off = 0u64;
    while got < total_usize {
        let r = io
            .read(ReadRequest {
                workspace_id: workspace_id.clone(),
                handle_id: verify.handle_id.clone(),
                offset: off,
                length: CHUNK as u32,
            })
            .await
            .expect("read chunk")
            .into_inner();
        got += r.data.len();
        off += r.data.len() as u64;
        if r.eof {
            break;
        }
    }
    assert_eq!(got, total_usize);

    io.release(ReleaseRequest {
        workspace_id: workspace_id.clone(),
        handle_id: verify.handle_id,
    })
    .await
    .expect("release verify handle");

    let _ = shutdown_tx.send(());
    server_task.await.expect("server task");
}

#[cfg(target_os = "linux")]
fn read_linux_vmhwm_kb() -> std::io::Result<u64> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    for line in status.lines() {
        let line = line.trim_start();
        if let Some(rest) = line.strip_prefix("VmHWM:") {
            let first = rest.split_whitespace().next().ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "VmHWM line")
            })?;
            return first
                .parse()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e));
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "VmHWM not in /proc/self/status",
    ))
}

#[tokio::test]
async fn grpc_io_release_keeps_dirty_handle_when_flush_fails() {
    let (tmp, shutdown_tx, server_task, channel, _state) = spawn_test_server().await;
    let mut admin = AdminClient::new(channel.clone());
    let mut mutation = MutationClient::new(channel.clone());
    let mut io = IoClient::new(channel);

    let create_ws = admin
        .create_workspace(CreateWorkspaceRequest {
            name: "io-release-flush-failure".to_string(),
            config: None,
        })
        .await
        .expect("create workspace")
        .into_inner();
    let workspace_id = create_ws.workspace_id;
    let root_node_id = create_ws.root_node_id;

    let docs = mutation
        .create(CreateRequest {
            workspace_id: workspace_id.clone(),
            parent_node_id: root_node_id,
            name: "docs".to_string(),
            kind: NodeKind::Dir as i32,
            mode: 0,
            exclusive: true,
        })
        .await
        .expect("create docs dir")
        .into_inner();
    let note = mutation
        .create(CreateRequest {
            workspace_id: workspace_id.clone(),
            parent_node_id: docs.node_id,
            name: "note.md".to_string(),
            kind: NodeKind::File as i32,
            mode: 0,
            exclusive: true,
        })
        .await
        .expect("create note file")
        .into_inner();

    let opened = io
        .open(OpenRequest {
            workspace_id: workspace_id.clone(),
            node_id: note.node_id.clone(),
            mode: HandleMode::ReadWrite as i32,
        })
        .await
        .expect("open read-write handle")
        .into_inner();
    io.write(WriteRequest {
        workspace_id: workspace_id.clone(),
        handle_id: opened.handle_id.clone(),
        offset: 0,
        data: b"unsynced".to_vec(),
    })
    .await
    .expect("write unsynced content");

    let file_path = tmp
        .path()
        .join("content")
        .join(&workspace_id)
        .join("docs")
        .join("note.md");
    std::fs::remove_file(&file_path).expect("remove file before forcing write failure");
    std::fs::create_dir_all(&file_path).expect("create conflicting directory at file path");

    let err = io
        .release(ReleaseRequest {
            workspace_id: workspace_id.clone(),
            handle_id: opened.handle_id.clone(),
        })
        .await
        .expect_err("release should fail when content path is invalid");
    assert_eq!(err.code(), Code::Internal);

    // A failed release must keep the handle alive so the caller can retry.
    let retained = io
        .read(ReadRequest {
            workspace_id: workspace_id.clone(),
            handle_id: opened.handle_id.clone(),
            offset: 0,
            length: 64,
        })
        .await
        .expect("failed release should not drop handle")
        .into_inner();
    assert_eq!(retained.data, b"unsynced");
    assert!(retained.eof);

    std::fs::remove_dir_all(&file_path).expect("remove conflicting directory");
    let released = io
        .release(ReleaseRequest {
            workspace_id: workspace_id.clone(),
            handle_id: opened.handle_id.clone(),
        })
        .await
        .expect("release retry should succeed")
        .into_inner();
    assert!(
        released.version >= 2,
        "release retry should advance version after successful flush"
    );

    let not_found = io
        .read(ReadRequest {
            workspace_id: workspace_id.clone(),
            handle_id: opened.handle_id,
            offset: 0,
            length: 16,
        })
        .await
        .expect_err("released handle should be removed");
    assert_eq!(not_found.code(), Code::NotFound);

    let _ = shutdown_tx.send(());
    server_task.await.expect("server task");
}

#[tokio::test]
async fn grpc_io_idle_timeout_reclaims_clean_and_dirty_handles() {
    let (_tmp, shutdown_tx, server_task, channel, _state) =
        spawn_test_server_with_idle_timeout(Duration::from_millis(30)).await;
    let mut admin = AdminClient::new(channel.clone());
    let mut mutation = MutationClient::new(channel.clone());
    let mut namespace = NamespaceClient::new(channel.clone());
    let mut io = IoClient::new(channel);

    let create_ws = admin
        .create_workspace(CreateWorkspaceRequest {
            name: "io-idle-timeout".to_string(),
            config: None,
        })
        .await
        .expect("create workspace")
        .into_inner();
    let workspace_id = create_ws.workspace_id;
    let root_node_id = create_ws.root_node_id;

    let docs = mutation
        .create(CreateRequest {
            workspace_id: workspace_id.clone(),
            parent_node_id: root_node_id,
            name: "docs".to_string(),
            kind: NodeKind::Dir as i32,
            mode: 0,
            exclusive: true,
        })
        .await
        .expect("create docs dir")
        .into_inner();
    let note = mutation
        .create(CreateRequest {
            workspace_id: workspace_id.clone(),
            parent_node_id: docs.node_id,
            name: "note.md".to_string(),
            kind: NodeKind::File as i32,
            mode: 0,
            exclusive: true,
        })
        .await
        .expect("create note file")
        .into_inner();

    let clean = io
        .open(OpenRequest {
            workspace_id: workspace_id.clone(),
            node_id: note.node_id.clone(),
            mode: HandleMode::Read as i32,
        })
        .await
        .expect("open clean read handle")
        .into_inner();
    let dirty = io
        .open(OpenRequest {
            workspace_id: workspace_id.clone(),
            node_id: note.node_id.clone(),
            mode: HandleMode::ReadWrite as i32,
        })
        .await
        .expect("open dirty handle")
        .into_inner();
    io.write(WriteRequest {
        workspace_id: workspace_id.clone(),
        handle_id: dirty.handle_id.clone(),
        offset: 0,
        data: b"idle content".to_vec(),
    })
    .await
    .expect("write dirty content");

    tokio::time::sleep(Duration::from_millis(90)).await;

    let cleanup_trigger = io
        .open(OpenRequest {
            workspace_id: workspace_id.clone(),
            node_id: note.node_id.clone(),
            mode: HandleMode::Read as i32,
        })
        .await
        .expect("open to trigger idle cleanup")
        .into_inner();
    match io
        .release(ReleaseRequest {
            workspace_id: workspace_id.clone(),
            handle_id: cleanup_trigger.handle_id,
        })
        .await
    {
        Ok(_) => {}
        Err(status) if status.code() == Code::NotFound => {}
        Err(status) => panic!("unexpected cleanup-trigger release status: {status}"),
    }

    let clean_read_err = io
        .read(ReadRequest {
            workspace_id: workspace_id.clone(),
            handle_id: clean.handle_id,
            offset: 0,
            length: 16,
        })
        .await
        .expect_err("idle clean handle should be reclaimed");
    assert_eq!(clean_read_err.code(), Code::NotFound);

    let mut dirty_reclaimed = false;
    for _ in 0..20 {
        match io
            .read(ReadRequest {
                workspace_id: workspace_id.clone(),
                handle_id: dirty.handle_id.clone(),
                offset: 0,
                length: 32,
            })
            .await
        {
            Ok(_) => {}
            Err(status) if status.code() == Code::FailedPrecondition => {}
            Err(status) if status.code() == Code::NotFound => {
                dirty_reclaimed = true;
                break;
            }
            Err(status) => panic!("unexpected dirty-handle state: {status}"),
        }

        let pulse = io
            .open(OpenRequest {
                workspace_id: workspace_id.clone(),
                node_id: note.node_id.clone(),
                mode: HandleMode::Read as i32,
            })
            .await
            .expect("open pulse handle")
            .into_inner();
        io.release(ReleaseRequest {
            workspace_id: workspace_id.clone(),
            handle_id: pulse.handle_id,
        })
        .await
        .expect("release pulse handle");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        dirty_reclaimed,
        "idle dirty handle should eventually flush and be reclaimed"
    );

    let attrs = namespace
        .get_attrs(GetAttrsRequest {
            workspace_id: workspace_id.clone(),
            node_id: note.node_id.clone(),
        })
        .await
        .expect("get attrs after idle dirty flush")
        .into_inner()
        .attrs
        .expect("attrs body");
    assert_eq!(attrs.size, 12);
    assert_eq!(attrs.version, 2);

    let persisted = io
        .open(OpenRequest {
            workspace_id: workspace_id.clone(),
            node_id: note.node_id.clone(),
            mode: HandleMode::Read as i32,
        })
        .await
        .expect("open persisted read handle")
        .into_inner();
    let persisted_data = io
        .read(ReadRequest {
            workspace_id: workspace_id.clone(),
            handle_id: persisted.handle_id.clone(),
            offset: 0,
            length: 64,
        })
        .await
        .expect("read persisted data after idle flush")
        .into_inner();
    assert_eq!(persisted_data.data, b"idle content");
    io.release(ReleaseRequest {
        workspace_id: workspace_id.clone(),
        handle_id: persisted.handle_id,
    })
    .await
    .expect("release persisted read handle");

    let _ = shutdown_tx.send(());
    server_task.await.expect("server task");
}

#[tokio::test]
async fn grpc_admin_reindex_stats_and_delete_workspace_content_dir() {
    let (tmp, shutdown_tx, server_task, channel, _state) = spawn_test_server().await;
    let mut admin = AdminClient::new(channel.clone());
    let mut mutation = MutationClient::new(channel.clone());
    let mut files = FilesClient::new(channel.clone());
    let mut namespace = NamespaceClient::new(channel);

    let create_ws = admin
        .create_workspace(CreateWorkspaceRequest {
            name: "admin-e2e".to_string(),
            config: None,
        })
        .await
        .expect("create workspace")
        .into_inner();
    let workspace_id = create_ws.workspace_id;
    let root_node_id = create_ws.root_node_id;

    let content_root = tmp.path().join("content").join(&workspace_id);
    assert!(
        content_root.exists(),
        "content workspace directory should exist immediately after workspace creation"
    );

    let docs = mutation
        .create(CreateRequest {
            workspace_id: workspace_id.clone(),
            parent_node_id: root_node_id,
            name: "docs".to_string(),
            kind: NodeKind::Dir as i32,
            mode: 0,
            exclusive: true,
        })
        .await
        .expect("create docs dir")
        .into_inner();

    let file = mutation
        .create(CreateRequest {
            workspace_id: workspace_id.clone(),
            parent_node_id: docs.node_id.clone(),
            name: "note.md".to_string(),
            kind: NodeKind::File as i32,
            mode: 0,
            exclusive: true,
        })
        .await
        .expect("create note file")
        .into_inner();

    files
        .put_file(PutFileRequest {
            workspace_id: workspace_id.clone(),
            target: Some(put_file_request::Target::NodeId(file.node_id.clone())),
            content: b"admin e2e payload".to_vec(),
            if_version: 0,
            mode: 0,
            ..Default::default()
        })
        .await
        .expect("put file");

    let before = admin
        .get_workspace_stats(GetWorkspaceStatsRequest {
            workspace_id: workspace_id.clone(),
        })
        .await
        .expect("get stats before reindex")
        .into_inner();
    assert_eq!(before.file_count, 1);
    assert!(before.dir_count >= 2);
    assert!(before.chunk_count >= 1);
    assert!(before.total_content_bytes > 0);

    let reindex = admin
        .reindex_workspace(ReindexWorkspaceRequest {
            workspace_id: workspace_id.clone(),
        })
        .await
        .expect("reindex workspace")
        .into_inner();
    assert!(reindex.enqueued_jobs >= 1);

    let after = admin
        .get_workspace_stats(GetWorkspaceStatsRequest {
            workspace_id: workspace_id.clone(),
        })
        .await
        .expect("get stats after reindex")
        .into_inner();
    assert!(
        after.queued_jobs > before.queued_jobs,
        "reindex should increase queued job count"
    );

    let resolved = namespace
        .resolve_ref(ResolveRefRequest {
            workspace_id: workspace_id.clone(),
            node_id: file.node_id,
        })
        .await
        .expect("resolve file before delete")
        .into_inner();
    assert!(resolved.exists);
    assert_eq!(resolved.path, "docs/note.md");

    admin
        .delete_workspace(DeleteWorkspaceRequest {
            workspace_id: workspace_id.clone(),
        })
        .await
        .expect("delete workspace");

    assert!(
        !content_root.exists(),
        "delete workspace should remove content directory"
    );

    let err = admin
        .get_workspace_stats(GetWorkspaceStatsRequest {
            workspace_id: workspace_id.clone(),
        })
        .await
        .expect_err("stats should fail for deleted workspace");
    assert_eq!(err.code(), Code::NotFound);

    let err = namespace
        .resolve_ref(ResolveRefRequest {
            workspace_id,
            node_id: docs.node_id,
        })
        .await
        .expect_err("resolve_ref should fail for deleted workspace");
    assert_eq!(err.code(), Code::NotFound);

    let _ = shutdown_tx.send(());
    server_task.await.expect("server task");
}

#[test]
fn auth_rejects_missing_token_when_local_bypass_disabled() {
    let auth = AuthConfig {
        active_secret: b"test-secret".to_vec(),
        previous_secrets: vec![],
        allow_local_without_token: false,
        enforce_scopes: true,
    };
    let metadata = tonic::metadata::MetadataMap::new();
    let runtime_metrics = RuntimeMetrics::new();
    let err = verify_auth(
        &metadata,
        &auth,
        Some(&runtime_metrics),
        AuthRequirement::WorkspaceScoped {
            workspace_id: "ws-1",
            required_scope: "workspace.read",
        },
    )
    .expect_err("missing token should be rejected");
    assert_eq!(err.code(), Code::Unauthenticated);
}

#[test]
fn auth_allows_missing_token_when_local_bypass_enabled() {
    let auth = AuthConfig {
        active_secret: b"test-secret".to_vec(),
        previous_secrets: vec![],
        allow_local_without_token: true,
        enforce_scopes: true,
    };
    let metadata = tonic::metadata::MetadataMap::new();
    let runtime_metrics = RuntimeMetrics::new();
    verify_auth(
        &metadata,
        &auth,
        Some(&runtime_metrics),
        AuthRequirement::WorkspaceScoped {
            workspace_id: "ws-1",
            required_scope: "workspace.read",
        },
    )
    .expect("local bypass should allow missing token");
}

#[test]
fn auth_from_env_rejects_default_secret_without_dev_mode() {
    let guard = EnvGuard::set_many(&[
        ("SCRYD_AUTH_SECRET", Some(DEFAULT_AUTH_SECRET)),
        ("SCRYD_DEV_MODE", Some("0")),
        ("SCRYD_ALLOW_LOCAL_NO_AUTH", Some("false")),
    ]);
    let err = auth_from_env().expect_err("default secret should be rejected");
    let msg = format!("{err:#}");
    assert!(msg.contains("default insecure value"));
    drop(guard);
}

#[test]
fn transport_from_env_rejects_non_loopback_without_tls() {
    let guard = EnvGuard::set_many(&[
        ("SCRYD_GRPC_ADDR", Some("0.0.0.0:50051")),
        ("SCRYD_TLS_CERT_PATH", None),
        ("SCRYD_TLS_KEY_PATH", None),
    ]);
    let default_addr = "127.0.0.1:50051".parse().expect("default addr");
    let err = transport_from_env(default_addr).expect_err("transport guard should reject");
    let msg = format!("{err:#}");
    assert!(msg.contains("is not loopback and TLS is not configured"));
    drop(guard);
}

#[test]
fn metrics_from_env_rejects_public_addr_without_opt_in() {
    let guard = EnvGuard::set_many(&[
        ("SCRYD_METRICS_ADDR", Some("0.0.0.0:9090")),
        ("SCRYD_METRICS_ALLOW_PUBLIC", Some("0")),
    ]);
    let err = metrics_from_env().expect_err("metrics guard should reject");
    let msg = format!("{err:#}");
    assert!(msg.contains("Refusing to expose /metrics publicly"));
    drop(guard);
}

#[test]
fn parse_mint_token_args_rejects_default_secret_without_flag() {
    let guard = EnvGuard::set_many(&[
        ("SCRYD_AUTH_SECRET", Some(DEFAULT_AUTH_SECRET)),
        ("SCRYD_AUTH_TOKEN_TTL_SECS", Some("3600")),
    ]);
    let err = parse_mint_token_args(["--kind".to_string(), "admin".to_string()])
        .expect_err("mint-token should reject default secret");
    let msg = format!("{err:#}");
    assert!(msg.contains("requires --secret or SCRYD_AUTH_SECRET"));
    drop(guard);
}

#[test]
fn transport_from_env_parses_message_size_limits() {
    let guard = EnvGuard::set_many(&[
        ("SCRYD_GRPC_ADDR", Some("127.0.0.1:50051")),
        ("SCRYD_MAX_DECODING_MESSAGE_BYTES", Some("1234")),
        ("SCRYD_MAX_ENCODING_MESSAGE_BYTES", Some("5678")),
    ]);
    let default_addr = "127.0.0.1:50051".parse().expect("default addr");
    let transport = transport_from_env(default_addr).expect("transport should parse");
    assert_eq!(transport.max_decoding_message_bytes, 1234);
    assert_eq!(transport.max_encoding_message_bytes, 5678);
    drop(guard);
}

#[tokio::test]
async fn per_workspace_layout_creates_separate_index_dbs_and_isolates_queries() {
    let (_tmp, shutdown_tx, server_task, channel, state) =
        spawn_test_server_per_workspace_layout().await;
    let mut admin = AdminClient::new(channel.clone());
    let mut mutation = MutationClient::new(channel.clone());
    let mut files = FilesClient::new(channel.clone());
    let mut search = SearchClient::new(channel);

    let first = admin
        .create_workspace(CreateWorkspaceRequest {
            name: "per-layout-a".to_string(),
            config: None,
        })
        .await
        .expect("create workspace a")
        .into_inner();
    let second = admin
        .create_workspace(CreateWorkspaceRequest {
            name: "per-layout-b".to_string(),
            config: None,
        })
        .await
        .expect("create workspace b")
        .into_inner();

    let file_a = mutation
        .create(CreateRequest {
            workspace_id: first.workspace_id.clone(),
            parent_node_id: first.root_node_id.clone(),
            name: "a.md".to_string(),
            kind: NodeKind::File as i32,
            mode: 0,
            exclusive: true,
        })
        .await
        .expect("create file in workspace a")
        .into_inner();
    let file_b = mutation
        .create(CreateRequest {
            workspace_id: second.workspace_id.clone(),
            parent_node_id: second.root_node_id.clone(),
            name: "b.md".to_string(),
            kind: NodeKind::File as i32,
            mode: 0,
            exclusive: true,
        })
        .await
        .expect("create file in workspace b")
        .into_inner();

    files
        .put_file(PutFileRequest {
            workspace_id: first.workspace_id.clone(),
            target: Some(put_file_request::Target::NodeId(file_a.node_id)),
            content: b"workspace alpha marker".to_vec(),
            if_version: 0,
            mode: 0,
            ..Default::default()
        })
        .await
        .expect("put workspace a file");
    files
        .put_file(PutFileRequest {
            workspace_id: second.workspace_id.clone(),
            target: Some(put_file_request::Target::NodeId(file_b.node_id)),
            content: b"workspace beta marker".to_vec(),
            if_version: 0,
            mode: 0,
            ..Default::default()
        })
        .await
        .expect("put workspace b file");

    let hits_a = search
        .search(SearchRequest {
            workspace_id: first.workspace_id.clone(),
            query: "alpha marker".to_string(),
            mode: SearchMode::Fts as i32,
            limit: 10,
        })
        .await
        .expect("search workspace a")
        .into_inner();
    assert_eq!(hits_a.total_hits, 1);
    assert_eq!(hits_a.hits[0].path, "a.md");

    let hits_b = search
        .search(SearchRequest {
            workspace_id: second.workspace_id.clone(),
            query: "alpha marker".to_string(),
            mode: SearchMode::Fts as i32,
            limit: 10,
        })
        .await
        .expect("search workspace b")
        .into_inner();
    assert_eq!(hits_b.total_hits, 0);

    let ws1_db = state
        .index_backend
        .db_path_for_workspace(&first.workspace_id)
        .expect("workspace a db path");
    let ws2_db = state
        .index_backend
        .db_path_for_workspace(&second.workspace_id)
        .expect("workspace b db path");
    assert!(ws1_db.exists(), "workspace a index db should exist");
    assert!(ws2_db.exists(), "workspace b index db should exist");
    assert_ne!(ws1_db, ws2_db);

    let _ = shutdown_tx.send(());
    server_task.await.expect("server task");
}

#[test]
fn index_backend_from_env_uses_index_root() {
    let tmp = TempDir::new().expect("temp dir");
    let root = tmp.path().join("per-workspace-index");
    let root_str = root.to_string_lossy().to_string();
    let _guard = EnvGuard::set_many(&[("SCRYD_INDEX_ROOT", Some(root_str.as_str()))]);
    let backend = IndexBackend::from_env().expect("build per-workspace backend");
    assert_eq!(backend.layout_label(), "per-workspace");
    assert_eq!(backend.open_store_count(), 0);
    assert_eq!(backend.root_path(), root.as_path());
    let ws_db = backend
        .db_path_for_workspace("ws-env")
        .expect("workspace db path");
    assert!(ws_db.ends_with("ws-env/index.db"));
}

#[test]
fn index_backend_from_env_requires_index_root() {
    let _guard = EnvGuard::set_many(&[("SCRYD_INDEX_ROOT", None)]);
    match IndexBackend::from_env() {
        Ok(_) => panic!("expected error when SCRYD_INDEX_ROOT is unset"),
        Err(err) => {
            let msg = format!("{err:?}");
            assert!(
                msg.contains("SCRYD_INDEX_ROOT is required"),
                "unexpected error: {msg}"
            );
        }
    }
}

/// `SCRYD_EMBED_BATCH_SIZE` / `SCRYD_EMBED_BATCH_MAX_BYTES` actually slice embedding calls so
/// a single node never ships all chunks to the embedding provider in one request.
#[tokio::test]
async fn embed_worker_batches_chunks_by_size_and_bytes() {
    use crate::credits::EmbeddingCharge;
    use crate::embedding::{EmbedOutcome, EmbeddingError, EmbeddingProvider};
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc as StdArc;

    #[derive(Clone, Default)]
    struct CountingEmbedder {
        calls: StdArc<AtomicUsize>,
        max_batch: StdArc<AtomicUsize>,
        max_bytes: StdArc<AtomicUsize>,
    }

    #[async_trait]
    impl EmbeddingProvider for CountingEmbedder {
        fn provider_kind(&self) -> &'static str {
            "counting-mock"
        }
        fn model_id(&self) -> &str {
            "counting-mock-32d"
        }
        async fn embed_documents(
            &self,
            inputs: &[String],
        ) -> Result<EmbedOutcome<Vec<Vec<f32>>>, EmbeddingError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let this_bytes: usize = inputs.iter().map(|s| s.len()).sum();
            loop {
                let cur = self.max_batch.load(Ordering::Relaxed);
                if inputs.len() <= cur {
                    break;
                }
                if self
                    .max_batch
                    .compare_exchange(cur, inputs.len(), Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
                {
                    break;
                }
            }
            loop {
                let cur = self.max_bytes.load(Ordering::Relaxed);
                if this_bytes <= cur {
                    break;
                }
                if self
                    .max_bytes
                    .compare_exchange(cur, this_bytes, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
                {
                    break;
                }
            }
            // Deterministic 32-d float vector derived from a tiny FNV-1a hash of the input.
            let value: Vec<Vec<f32>> = inputs
                .iter()
                .map(|s| {
                    let mut h: u64 = 0xcbf29ce484222325;
                    for b in s.as_bytes() {
                        h ^= *b as u64;
                        h = h.wrapping_mul(0x100000001b3);
                    }
                    (0..32)
                        .map(|i| {
                            let r = h.rotate_left((i * 7) as u32);
                            (r as u32) as f32 / u32::MAX as f32
                        })
                        .collect()
                })
                .collect();
            Ok(EmbedOutcome {
                value,
                charge: EmbeddingCharge {
                    total_tokens: crate::credits::estimate_tokens_batch(inputs),
                    credits: 0,
                    from_upstream_usage: false,
                },
            })
        }
        async fn embed_query(&self, input: &str) -> Result<EmbedOutcome<Vec<f32>>, EmbeddingError> {
            let mut h: u64 = 0xcbf29ce484222325;
            for b in input.as_bytes() {
                h ^= *b as u64;
                h = h.wrapping_mul(0x100000001b3);
            }
            let value: Vec<f32> = (0..32)
                .map(|i| {
                    let r = h.rotate_left((i * 7) as u32);
                    (r as u32) as f32 / u32::MAX as f32
                })
                .collect();
            Ok(EmbedOutcome {
                value,
                charge: EmbeddingCharge {
                    total_tokens: crate::credits::estimate_tokens_utf8(input),
                    credits: 0,
                    from_upstream_usage: false,
                },
            })
        }
    }

    let _env = EnvGuard::set_many(&[
        ("SCRYD_EMBED_BATCH_SIZE", Some("4")),
        ("SCRYD_EMBED_BATCH_MAX_BYTES", Some("16384")),
    ]);

    let tmp = tempfile::tempdir().expect("tempdir");
    let content_root = tmp.path().join("content");
    std::fs::create_dir_all(&content_root).expect("content root");
    let index_root = tmp.path().join("index");
    let catalog_db = tmp.path().join("catalog.db");
    let counter = CountingEmbedder::default();
    let embedding_runtime = crate::embedding::EmbeddingRuntime {
        provider_kind: counter.provider_kind().to_string(),
        model_id: counter.model_id().to_string(),
        provider: Arc::new(counter.clone()),
    };
    let (catalog, _catalog_task) = catalog::build_catalog(&catalog::CatalogConfig {
        backend: catalog::CatalogBackendKind::Sqlite,
        sqlite_db_path: Some(catalog_db),
        postgres_url: None,
    })
    .expect("build sqlite catalog");
    let backend = IndexBackend::open(index_root.clone()).expect("open index backend");
    let root_node = backend
        .create_workspace("ws-embed", "embed workspace")
        .expect("create workspace");
    let state = Arc::new(AppState::new_with_io_limits(
        LocalFsContentStore::new(&content_root),
        backend.clone(),
        catalog,
        embedding_runtime,
        DEFAULT_IO_HANDLE_MEMORY_LIMIT,
        DEFAULT_IO_STREAM_CHUNK_BYTES,
        Duration::from_millis(AppState::DEFAULT_HANDLE_IDLE_TIMEOUT_MS),
        DEFAULT_EVENTS_BUS_CAPACITY,
    ));

    let ws_db = backend
        .db_path_for_workspace("ws-embed")
        .expect("workspace db path");
    let store = IndexStore::open(&ws_db).expect("open store for seeding");

    // Produce many chunks: plain text splitter caps at ~2 KiB each → 50 lines per line-group
    // over 50 line-groups yields ~40+ chunks.
    let line = "word ".repeat(200); // 1000 bytes
    let mut body = String::new();
    for _ in 0..64 {
        body.push_str(&line);
        body.push('\n');
    }
    let (fnode, _) = store
        .create_node(
            "ws-embed",
            &root_node,
            "huge.txt",
            scry_index::NodeKind::File,
            0o644,
            true,
        )
        .expect("create file node");
    let (record, _) = store
        .put_file("ws-embed", "huge.txt", body.as_bytes(), None)
        .expect("put file");

    // Drive the embedding worker directly (no gRPC).
    let chunks_indexed = state
        .index_node_embeddings("ws-embed", &fnode.node_id, record.version)
        .await
        .expect("index_node_embeddings");

    let calls = counter.calls.load(Ordering::Relaxed);
    let max_batch = counter.max_batch.load(Ordering::Relaxed);
    let max_bytes = counter.max_bytes.load(Ordering::Relaxed);

    assert!(
        chunks_indexed >= 10,
        "expected many chunks, got {chunks_indexed}"
    );
    assert!(
        calls > 1,
        "expected more than one embedding batch call, got {calls}"
    );
    assert!(
        max_batch <= 4,
        "embed batch exceeded SCRYD_EMBED_BATCH_SIZE=4: max_batch={max_batch}"
    );
    assert!(
        max_bytes <= 16384,
        "embed batch exceeded SCRYD_EMBED_BATCH_MAX_BYTES=16384: max_bytes={max_bytes}"
    );

    // Coverage sanity: FTS index still finds the chunks after batched embedding.
    let hits = state
        .index_backend
        .search_fts("ws-embed", "word", 50)
        .expect("fts search");
    assert!(!hits.hits.is_empty(), "FTS should find our chunks");
}
