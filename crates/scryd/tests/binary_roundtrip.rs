//! Integration: binary-safe content via PutFile/GetFile and IO Open/Write/Flush/Open/Read.
use std::process::Stdio;
use std::time::Duration;

use scry_proto::scry::v1::{
    admin_client::AdminClient, files_client::FilesClient, io_client::IoClient,
    mutation_client::MutationClient, put_file_request, CreateRequest, CreateWorkspaceRequest,
    FlushRequest, GetFileRequest, HandleMode, NodeKind, OpenRequest, PutFileRequest, ReadRequest,
    WorkspaceConfig, WriteRequest,
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

#[tokio::test]
async fn binary_roundtrip_files_and_io_paths() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let content_root = tmp.path().join("content");
    let index_root = tmp.path().join("index");
    std::fs::create_dir_all(&content_root).expect("content root");
    std::fs::create_dir_all(&index_root).expect("index root");

    let socket = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = socket.local_addr().expect("addr");
    drop(socket);

    const SECRET: &str = "test-binary-roundtrip-secret-not-default";

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
            name: "binrt".to_string(),
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
    assert!(!token.is_empty(), "empty token");

    let interceptor = bearer_interceptor(token.clone());
    let mut files = FilesClient::with_interceptor(channel.clone(), interceptor.clone());
    let mut mutation = MutationClient::with_interceptor(channel.clone(), interceptor.clone());
    let mut io = IoClient::with_interceptor(channel, interceptor);

    let docs = mutation
        .create(CreateRequest {
            workspace_id: workspace_id.clone(),
            parent_node_id: root.clone(),
            name: "docs".to_string(),
            kind: NodeKind::Dir as i32,
            mode: 0o755,
            exclusive: true,
        })
        .await
        .expect("create docs")
        .into_inner();

    // 16 KiB random
    let mut rnd16k = vec![0u8; 16 * 1024];
    for (i, b) in rnd16k.iter_mut().enumerate() {
        *b = (i as u8).wrapping_mul(17).wrapping_add(3);
    }
    // ZIP magic
    let zip_magic: Vec<u8> = vec![0x50, 0x4b, 0x03, 0x04, 0, 1, 2, 3];
    // Text with NUL
    let nul_text = b"hello\0world".to_vec();

    for (name, bytes) in [
        ("rand16k.bin", rnd16k.as_slice()),
        ("archive.zip", zip_magic.as_slice()),
        ("nul.txt", nul_text.as_slice()),
    ] {
        let f = mutation
            .create(CreateRequest {
                workspace_id: workspace_id.clone(),
                parent_node_id: docs.node_id.clone(),
                name: name.to_string(),
                kind: NodeKind::File as i32,
                mode: 0o644,
                exclusive: true,
            })
            .await
            .expect("create file")
            .into_inner();

        files
            .put_file(PutFileRequest {
                workspace_id: workspace_id.clone(),
                target: Some(put_file_request::Target::NodeId(f.node_id.clone())),
                content: bytes.to_vec(),
                if_version: 0,
                mode: 0,
                ..Default::default()
            })
            .await
            .expect("put_file");

        let mut stream = files
            .get_file(GetFileRequest {
                workspace_id: workspace_id.clone(),
                path: format!("docs/{name}"),
            })
            .await
            .expect("get_file")
            .into_inner();
        let mut got = Vec::new();
        while let Some(chunk) = stream.message().await.expect("chunk") {
            got.extend(chunk.data);
            if chunk.eof {
                break;
            }
        }
        assert_eq!(got, bytes, "GetFile roundtrip for {name}");

        let opened = io
            .open(OpenRequest {
                workspace_id: workspace_id.clone(),
                node_id: f.node_id.clone(),
                mode: HandleMode::Read as i32,
            })
            .await
            .expect("open read")
            .into_inner();
        let read_back = io
            .read(ReadRequest {
                workspace_id: workspace_id.clone(),
                handle_id: opened.handle_id,
                offset: 0,
                length: bytes.len() as u32 + 1024,
            })
            .await
            .expect("read")
            .into_inner();
        assert_eq!(read_back.data, bytes, "IO read for {name}");

        let w = mutation
            .create(CreateRequest {
                workspace_id: workspace_id.clone(),
                parent_node_id: docs.node_id.clone(),
                name: format!("io-{name}"),
                kind: NodeKind::File as i32,
                mode: 0o644,
                exclusive: true,
            })
            .await
            .expect("create io file")
            .into_inner();

        let h = io
            .open(OpenRequest {
                workspace_id: workspace_id.clone(),
                node_id: w.node_id.clone(),
                mode: HandleMode::Write as i32,
            })
            .await
            .expect("open write")
            .into_inner();

        io.write(WriteRequest {
            workspace_id: workspace_id.clone(),
            handle_id: h.handle_id.clone(),
            offset: 0,
            data: bytes.to_vec(),
        })
        .await
        .expect("write")
        .into_inner();

        io.flush(FlushRequest {
            workspace_id: workspace_id.clone(),
            handle_id: h.handle_id,
        })
        .await
        .expect("flush")
        .into_inner();

        let opened2 = io
            .open(OpenRequest {
                workspace_id: workspace_id.clone(),
                node_id: w.node_id,
                mode: HandleMode::Read as i32,
            })
            .await
            .expect("open read after flush")
            .into_inner();
        let read_io = io
            .read(ReadRequest {
                workspace_id: workspace_id.clone(),
                handle_id: opened2.handle_id,
                offset: 0,
                length: bytes.len() as u32 + 1024,
            })
            .await
            .expect("read after io write")
            .into_inner();
        assert_eq!(read_io.data, bytes, "IO write+flush+read for {name}");
    }

    drop(child);
}
