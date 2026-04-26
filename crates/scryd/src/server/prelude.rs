//! Shared imports for `server::*` modules (legacy monolith split).

pub(crate) use std::{
    collections::HashMap,
    env,
    fmt::Write,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, OnceLock, RwLock,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

pub(crate) use anyhow::Context;
pub(crate) use async_stream::try_stream;
pub(crate) use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
pub(crate) use hmac::{Hmac, Mac};
pub(crate) use scry_index::{
    AttrsPatch, EventRecord, HandleMode as IndexHandleMode, IndexError, NodeKind, NodeRecord,
};
pub(crate) use scry_proto::scry::v1::{
    admin_server::{Admin, AdminServer},
    events_server::{Events, EventsServer},
    files_server::{Files, FilesServer},
    health_server::{Health, HealthServer},
    io_server::{Io, IoServer},
    mutation_server::{Mutation, MutationServer},
    namespace_server::{Namespace, NamespaceServer},
    put_file_request,
    search_server::{Search, SearchServer},
    AcknowledgeRequest, AcknowledgeResponse, Attrs, CreateRequest, CreateResponse,
    CreateWorkspaceRequest, CreateWorkspaceResponse, DeleteWorkspaceRequest,
    DeleteWorkspaceResponse, DirEntry, Event as ProtoEvent, EventKind as ProtoEventKind,
    FlushRequest, FlushResponse, GetAttrsIfChangedRequest, GetAttrsIfChangedResponse,
    GetAttrsRequest, GetAttrsResponse, GetFileChunk, GetFileRequest, GetWorkspaceStatsRequest,
    GetWorkspaceStatsResponse, HandleMode as ProtoHandleMode, ListWorkspacesRequest,
    ListWorkspacesResponse, LookupRequest, LookupResponse, NodeKind as ProtoNodeKind, OpenRequest,
    OpenResponse, PingRequest, PingResponse, PutFileRequest, PutFileResponse, ReadDirRequest,
    ReadDirResponse, ReadRequest, ReadResponse, ReindexWorkspaceRequest, ReindexWorkspaceResponse,
    ReleaseRequest, ReleaseResponse, RenameRequest, RenameResponse, ResolvePathRequest,
    ResolvePathResponse, ResolveRefRequest, ResolveRefResponse, SearchMode, SearchRequest,
    SearchResponse, SetAttrsRequest, SetAttrsResponse, SubscribeRequest, TruncateRequest,
    TruncateResponse, UnlinkRequest, UnlinkResponse, WorkspaceSummary, WriteRequest, WriteResponse,
};
pub(crate) use scry_storage::{ContentStore, LocalFsContentStore};
pub(crate) use serde::{Deserialize, Serialize};
pub(crate) use sha2::Sha256;
pub(crate) use tokio::fs::File;
pub(crate) use tokio::io::{AsyncReadExt, AsyncWriteExt};
pub(crate) use tokio::sync::broadcast;
pub(crate) use tokio_stream::wrappers::UnixListenerStream;
pub(crate) use tonic::{
    metadata::MetadataMap,
    transport::{Certificate, Server, ServerTlsConfig},
    Request, Response, Status,
};
pub(crate) use tracing::{info, warn};
pub(crate) use uuid::Uuid;

pub(crate) use crate::catalog::{self, CatalogConfig};
pub(crate) use crate::embedding;
pub(crate) use crate::server::index_backend::IndexBackend;

pub(crate) type HmacSha256 = Hmac<Sha256>;
