//! Integration: Events.Subscribe receives NodeModified from broadcast within a bounded latency
//! after PutFile (no 100ms polling path).
use std::process::Stdio;
use std::time::Duration;

use scry_proto::scry::v1::{
    admin_client::AdminClient, events_client::EventsClient, files_client::FilesClient,
    mutation_client::MutationClient, put_file_request, CreateRequest, CreateWorkspaceRequest,
    EventKind, PutFileRequest, SubscribeFilter, SubscribeRequest, WorkspaceConfig,
};
use tokio::net::TcpListener;
use tokio::process::Command;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;

fn scryd_bin() -> String {
    std::env::var("CARGO_BIN_EXE_scryd").expect("CARGO_BIN_EXE_scryd must be set by cargo test")
}

async fn connect(addr: std::net::SocketAddr) -> Channel {
    let uri = format!("http://{addr}");
    let endpoint = Endpoint::from_shared(uri).expect("endpoint");
    for _ in 0..80 {
        if let Ok(ch) = endpoint.connect().await {
            return ch;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("failed to connect to scryd at {addr}");
}

#[allow(clippy::result_large_err)]
fn bearer_interceptor(
    token: String,
) -> impl FnMut(Request<()>) -> Result<Request<()>, tonic::Status> + Clone {
    move |mut req: Request<()>| {
        req.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from(format!("Bearer {token}"))
                .map_err(|_| tonic::Status::internal("invalid authorization metadata"))?,
        );
        Ok(req)
    }
}

struct ScrydChild {
    child: tokio::process::Child,
}

impl Drop for ScrydChild {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

/// CI runners can be noisy; local dev keeps the plan's 50ms target.
fn max_push_latency() -> Duration {
    if std::env::var("CI").is_ok() {
        Duration::from_millis(500)
    } else {
        Duration::from_millis(50)
    }
}

#[tokio::test]
async fn events_subscribe_push_latency_after_put_file() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let content_root = tmp.path().join("content");
    let index_root = tmp.path().join("index");
    std::fs::create_dir_all(&content_root).expect("content root");
    std::fs::create_dir_all(&index_root).expect("index root");

    let socket = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = socket.local_addr().expect("addr");
    drop(socket);

    const SECRET: &str = "test-events-latency-secret-not-default";

    let mut cmd = Command::new(scryd_bin());
    cmd.env("SCRYD_DEV_MODE", "1")
        .env("SCRYD_AUTH_SECRET", SECRET)
        .env("SCRYD_ALLOW_LOCAL_NO_AUTH", "1")
        .env("SCRYD_CONTENT_ROOT", &content_root)
        .env("SCRYD_INDEX_ROOT", index_root.to_str().unwrap())
        .env("SCRYD_GRPC_ADDR", addr.to_string())
        .env("SCRYD_CATALOG_BACKEND", "sqlite")
        .env("SCRYD_EMBED_PROVIDER", "mock")
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let child = ScrydChild {
        child: cmd.spawn().expect("spawn scryd"),
    };

    let channel = connect(addr).await;
    let mut admin = AdminClient::new(channel.clone());
    let ws = admin
        .create_workspace(CreateWorkspaceRequest {
            name: "evlat".to_string(),
            config: Some(WorkspaceConfig {
                embedding_model: String::new(),
                embedding_dim: 32,
                max_file_size: 0,
            }),
        })
        .await
        .expect("create_workspace")
        .into_inner();
    let workspace_id = ws.workspace_id;
    let root = ws.root_node_id;

    let token_out = Command::new(scryd_bin())
        .args([
            "mint-token",
            "--kind",
            "workspace",
            "--workspace-id",
            &workspace_id,
            "--secret",
            SECRET,
        ])
        .output()
        .await
        .expect("mint-token");
    assert!(
        token_out.status.success(),
        "mint-token failed: {}",
        String::from_utf8_lossy(&token_out.stderr)
    );
    let token = String::from_utf8(token_out.stdout)
        .expect("utf8")
        .trim()
        .to_string();

    let interceptor = bearer_interceptor(token);
    let mut files = FilesClient::with_interceptor(channel.clone(), interceptor.clone());
    let mut mutation = MutationClient::with_interceptor(channel.clone(), interceptor.clone());
    let mut events = EventsClient::with_interceptor(channel, interceptor);

    let docs = mutation
        .create(CreateRequest {
            workspace_id: workspace_id.clone(),
            parent_node_id: root.clone(),
            name: "docs".to_string(),
            kind: scry_proto::scry::v1::NodeKind::Dir as i32,
            mode: 0o755,
            exclusive: true,
        })
        .await
        .expect("create docs")
        .into_inner();

    let f = mutation
        .create(CreateRequest {
            workspace_id: workspace_id.clone(),
            parent_node_id: docs.node_id.clone(),
            name: "note.bin".to_string(),
            kind: scry_proto::scry::v1::NodeKind::File as i32,
            mode: 0o644,
            exclusive: true,
        })
        .await
        .expect("create file")
        .into_inner();

    // Empty since_cursor + empty subscriber_id => start at latest cursor (no historical replay).
    let mut stream = events
        .subscribe(SubscribeRequest {
            workspace_id: workspace_id.clone(),
            since_cursor: String::new(),
            subscriber_id: String::new(),
            filter: Some(SubscribeFilter {
                path_prefix: vec![],
                node_ids: vec![],
                kinds: vec![EventKind::NodeModified as i32],
                include_index_events: false,
            }),
        })
        .await
        .expect("subscribe")
        .into_inner();

    let t0 = std::time::Instant::now();
    let (put_res, first_msg) = tokio::join!(
        files.put_file(PutFileRequest {
            workspace_id: workspace_id.clone(),
            target: Some(put_file_request::Target::NodeId(f.node_id.clone())),
            content: b"latency probe".to_vec(),
            if_version: 0,
            mode: 0,
            ..Default::default()
        }),
        stream.message()
    );
    put_res.expect("put_file");
    let maybe = first_msg.expect("stream error");
    let ev = maybe.expect("expected first event");
    assert_eq!(
        ev.kind,
        EventKind::NodeModified as i32,
        "expected NodeModified, got kind={}",
        ev.kind
    );
    assert_eq!(ev.node_id, f.node_id);

    let elapsed = t0.elapsed();
    assert!(
        elapsed <= max_push_latency(),
        "push latency too high: {:?} (max {:?})",
        elapsed,
        max_push_latency()
    );

    drop(child);
}
