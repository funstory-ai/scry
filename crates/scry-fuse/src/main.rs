#[cfg(not(feature = "fuse"))]
fn main() {
    eprintln!("scry-fuse built without `fuse` feature; enable with `--features fuse`.");
}

#[cfg(feature = "fuse")]
mod fuse_app {
    use std::{
        collections::HashMap,
        ffi::OsStr,
        path::PathBuf,
        sync::Arc,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use anyhow::{Context, Result};
    use clap::Parser;
    use fuser::{
        FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyCreate, ReplyData,
        ReplyDirectoryPlus, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, Request as FuseRequest,
        TimeOrNow,
    };
    use libc::{
        EACCES, EAGAIN, EBUSY, EEXIST, EINTR, EINVAL, EIO, ENOENT, ENOSPC, ENOSYS, EOVERFLOW,
        ETIMEDOUT,
    };
    use scry_proto::scry::v1::{
        io_client::IoClient, mutation_client::MutationClient, namespace_client::NamespaceClient,
        AttrsPatch, CreateRequest, FlushRequest, GetAttrsRequest, HandleMode, LookupRequest,
        NodeKind, OpenRequest, ReadDirRequest, ReadRequest, ReleaseRequest, RenameRequest,
        SetAttrsRequest, TruncateRequest, UnlinkRequest, WriteRequest,
    };
    use tokio::runtime::Runtime;
    use tonic::{
        metadata::{Ascii, MetadataValue},
        service::Interceptor,
        transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity},
        Code, Request, Status,
    };
    use tracing::info;

    const TTL: Duration = Duration::from_secs(1);
    const READDIR_PAGE_LIMIT: u32 = 256;

    #[derive(Clone, Default)]
    struct AuthInterceptor {
        authorization: Option<MetadataValue<Ascii>>,
    }

    impl AuthInterceptor {
        fn from_bearer_token(token: Option<&str>) -> Result<Self> {
            let authorization = token
                .map(|token| MetadataValue::try_from(format!("Bearer {}", token.trim())))
                .transpose()
                .context("invalid bearer token metadata")?;
            Ok(Self { authorization })
        }
    }

    impl Interceptor for AuthInterceptor {
        fn call(&mut self, mut request: Request<()>) -> std::result::Result<Request<()>, Status> {
            if let Some(value) = self.authorization.clone() {
                request.metadata_mut().insert("authorization", value);
            }
            Ok(request)
        }
    }

    #[derive(Clone)]
    struct InodeEntry {
        workspace_id: String,
        node_id: String,
        parent_ino: u64,
        attr: FileAttr,
    }

    #[derive(Clone)]
    struct HandleEntry {
        workspace_id: String,
        handle_id: String,
    }

    #[derive(Clone)]
    struct DirRow {
        ino: u64,
        name: String,
        attr: FileAttr,
    }

    struct DirCache {
        rows: Vec<DirRow>,
        next_cursor: String,
        complete: bool,
    }

    struct ScryFuseFs {
        rt: Arc<Runtime>,
        channel: Channel,
        interceptor: AuthInterceptor,
        workspace_id: String,
        inodes: HashMap<u64, InodeEntry>,
        node_to_ino: HashMap<String, u64>,
        dir_cache: HashMap<u64, DirCache>,
        handles: HashMap<u64, HandleEntry>,
        next_ino: u64,
        next_fh: u64,
    }

    impl ScryFuseFs {
        #[allow(dead_code)]
        fn new(rt: Arc<Runtime>, channel: Channel, workspace_id: String) -> Result<Self> {
            Self::new_with_interceptor(rt, channel, workspace_id, AuthInterceptor::default())
        }

        fn new_with_interceptor(
            rt: Arc<Runtime>,
            channel: Channel,
            workspace_id: String,
            interceptor: AuthInterceptor,
        ) -> Result<Self> {
            let mut fs = Self {
                rt,
                channel,
                interceptor,
                workspace_id,
                inodes: HashMap::new(),
                node_to_ino: HashMap::new(),
                dir_cache: HashMap::new(),
                handles: HashMap::new(),
                next_ino: 2,
                next_fh: 1,
            };
            fs.bootstrap_root()?;
            Ok(fs)
        }

        fn bootstrap_root(&mut self) -> Result<()> {
            let workspace_id = self.workspace_id.clone();
            let mut ns =
                NamespaceClient::with_interceptor(self.channel.clone(), self.interceptor.clone());
            let root = self
                .rt
                .block_on(async {
                    ns.resolve_path(scry_proto::scry::v1::ResolvePathRequest {
                        workspace_id: workspace_id.clone(),
                        path: String::new(),
                    })
                    .await
                })
                .map_err(|status| {
                    if status.code() == Code::Unauthenticated {
                        anyhow::anyhow!(
                            "resolve root path failed: unauthenticated (hint: pass --auth-token or --auth-token-file)"
                        )
                    } else {
                        anyhow::Error::new(status)
                    }
                })
                .context("resolve root path")?
                .into_inner();
            if !root.exists || root.node_id.is_empty() {
                anyhow::bail!("workspace root does not exist");
            }

            let mut ns =
                NamespaceClient::with_interceptor(self.channel.clone(), self.interceptor.clone());
            let attrs = self
                .rt
                .block_on(async {
                    ns.get_attrs(GetAttrsRequest {
                        workspace_id: workspace_id.clone(),
                        node_id: root.node_id.clone(),
                    })
                    .await
                })
                .context("fetch root attrs")?
                .into_inner()
                .attrs
                .context("missing root attrs in response")?;

            let attr = proto_attrs_to_fuse(1, &attrs);
            self.inodes.insert(
                1,
                InodeEntry {
                    workspace_id,
                    node_id: root.node_id.clone(),
                    parent_ino: 1,
                    attr,
                },
            );
            self.node_to_ino.insert(root.node_id, 1);
            Ok(())
        }

        fn entry(&self, ino: u64) -> Option<&InodeEntry> {
            self.inodes.get(&ino)
        }

        fn upsert_inode(
            &mut self,
            workspace_id: &str,
            node_id: &str,
            parent_ino: u64,
            mut attr: FileAttr,
        ) -> u64 {
            if let Some(ino) = self.node_to_ino.get(node_id).copied() {
                attr.ino = ino;
                if let Some(existing) = self.inodes.get_mut(&ino) {
                    existing.parent_ino = parent_ino;
                    existing.attr = attr;
                }
                return ino;
            }
            let ino = self.next_ino;
            self.next_ino = self.next_ino.saturating_add(1);
            attr.ino = ino;
            self.node_to_ino.insert(node_id.to_string(), ino);
            self.inodes.insert(
                ino,
                InodeEntry {
                    workspace_id: workspace_id.to_string(),
                    node_id: node_id.to_string(),
                    parent_ino,
                    attr,
                },
            );
            ino
        }

        fn alloc_fh(&mut self, handle: HandleEntry) -> u64 {
            let fh = self.next_fh;
            self.next_fh = self.next_fh.saturating_add(1);
            self.handles.insert(fh, handle);
            fh
        }

        fn invalidate_dir_cache(&mut self, ino: u64) {
            self.dir_cache.remove(&ino);
        }

        fn initial_dir_cache(&self, ino: u64, entry: &InodeEntry) -> DirCache {
            let mut rows = Vec::<DirRow>::with_capacity(2);
            rows.push(DirRow {
                ino,
                name: ".".to_string(),
                attr: entry.attr,
            });
            let parent_attr = self
                .entry(entry.parent_ino)
                .map(|parent| parent.attr)
                .unwrap_or(entry.attr);
            rows.push(DirRow {
                ino: entry.parent_ino,
                name: "..".to_string(),
                attr: parent_attr,
            });
            DirCache {
                rows,
                next_cursor: String::new(),
                complete: false,
            }
        }

        fn fetch_dir_page(
            &mut self,
            entry: &InodeEntry,
            dir_ino: u64,
            cache: &mut DirCache,
        ) -> std::result::Result<(), i32> {
            if cache.complete {
                return Ok(());
            }
            let current_cursor = cache.next_cursor.clone();
            let mut ns =
                NamespaceClient::with_interceptor(self.channel.clone(), self.interceptor.clone());
            let body = self
                .rt
                .block_on(async {
                    ns.read_dir(ReadDirRequest {
                        workspace_id: entry.workspace_id.clone(),
                        node_id: entry.node_id.clone(),
                        cursor: current_cursor.clone(),
                        limit: READDIR_PAGE_LIMIT,
                    })
                    .await
                })
                .map_err(|status| status_to_errno(&status))?
                .into_inner();
            for child in body.entries {
                let Some(attrs) = child.attrs else {
                    continue;
                };
                let child_ino = self.upsert_inode(
                    &entry.workspace_id,
                    &child.node_id,
                    dir_ino,
                    proto_attrs_to_fuse(0, &attrs),
                );
                if let Some(child_entry) = self.entry(child_ino) {
                    cache.rows.push(DirRow {
                        ino: child_ino,
                        name: child.name,
                        attr: child_entry.attr,
                    });
                }
            }
            if body.next_cursor.is_empty() || body.next_cursor == current_cursor {
                cache.complete = true;
            } else {
                cache.next_cursor = body.next_cursor;
            }
            Ok(())
        }

        fn handle_mode_from_flags(flags: i32) -> i32 {
            let access_mode = flags & libc::O_ACCMODE;
            if access_mode == libc::O_WRONLY {
                HandleMode::Write as i32
            } else if access_mode == libc::O_RDWR {
                HandleMode::ReadWrite as i32
            } else {
                HandleMode::Read as i32
            }
        }
    }

    impl Filesystem for ScryFuseFs {
        fn lookup(&mut self, _req: &FuseRequest<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
            let Some(parent_entry) = self.entry(parent).cloned() else {
                reply.error(ENOENT);
                return;
            };
            let mut ns =
                NamespaceClient::with_interceptor(self.channel.clone(), self.interceptor.clone());
            match self.rt.block_on(async {
                ns.lookup(LookupRequest {
                    workspace_id: parent_entry.workspace_id.clone(),
                    parent_node_id: parent_entry.node_id.clone(),
                    name: name.to_string_lossy().to_string(),
                })
                .await
            }) {
                Ok(resp) => {
                    let body = resp.into_inner();
                    let Some(attrs) = body.attrs else {
                        reply.error(EIO);
                        return;
                    };
                    let attr = proto_attrs_to_fuse(0, &attrs);
                    let ino =
                        self.upsert_inode(&parent_entry.workspace_id, &body.node_id, parent, attr);
                    if let Some(entry) = self.inodes.get(&ino) {
                        reply.entry(&TTL, &entry.attr, 0);
                    } else {
                        reply.error(EIO);
                    }
                }
                Err(status) => reply.error(status_to_errno(&status)),
            }
        }

        fn getattr(
            &mut self,
            _req: &FuseRequest<'_>,
            ino: u64,
            _fh: Option<u64>,
            reply: ReplyAttr,
        ) {
            let Some(entry) = self.entry(ino).cloned() else {
                reply.error(ENOENT);
                return;
            };
            let mut ns =
                NamespaceClient::with_interceptor(self.channel.clone(), self.interceptor.clone());
            match self.rt.block_on(async {
                ns.get_attrs(GetAttrsRequest {
                    workspace_id: entry.workspace_id.clone(),
                    node_id: entry.node_id.clone(),
                })
                .await
            }) {
                Ok(resp) => {
                    let Some(attrs) = resp.into_inner().attrs else {
                        reply.error(EIO);
                        return;
                    };
                    let attr = proto_attrs_to_fuse(ino, &attrs);
                    if let Some(existing) = self.inodes.get_mut(&ino) {
                        existing.attr = attr;
                    }
                    reply.attr(&TTL, &attr);
                }
                Err(status) => reply.error(status_to_errno(&status)),
            }
        }

        fn readdirplus(
            &mut self,
            _req: &FuseRequest<'_>,
            ino: u64,
            _fh: u64,
            offset: i64,
            mut reply: ReplyDirectoryPlus,
        ) {
            let Some(entry) = self.entry(ino).cloned() else {
                reply.error(ENOENT);
                return;
            };
            let mut cache = self
                .dir_cache
                .remove(&ino)
                .unwrap_or_else(|| self.initial_dir_cache(ino, &entry));
            let mut index = offset.max(0) as usize;
            loop {
                while index >= cache.rows.len() && !cache.complete {
                    if let Err(errno) = self.fetch_dir_page(&entry, ino, &mut cache) {
                        self.dir_cache.insert(ino, cache);
                        reply.error(errno);
                        return;
                    }
                }
                if index >= cache.rows.len() {
                    break;
                }
                let row = cache.rows[index].clone();
                if reply.add(
                    row.ino,
                    i64::try_from(index + 1).unwrap_or(i64::MAX),
                    row.name,
                    &TTL,
                    &row.attr,
                    0,
                ) {
                    break;
                }
                index = index.saturating_add(1);
            }
            self.dir_cache.insert(ino, cache);
            reply.ok();
        }

        fn open(&mut self, _req: &FuseRequest<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
            let Some(entry) = self.entry(ino).cloned() else {
                reply.error(ENOENT);
                return;
            };
            let mut io = IoClient::with_interceptor(self.channel.clone(), self.interceptor.clone());
            match self.rt.block_on(async {
                io.open(OpenRequest {
                    workspace_id: entry.workspace_id.clone(),
                    node_id: entry.node_id.clone(),
                    mode: Self::handle_mode_from_flags(flags),
                })
                .await
            }) {
                Ok(resp) => {
                    let opened = resp.into_inner();
                    let fh = self.alloc_fh(HandleEntry {
                        workspace_id: entry.workspace_id,
                        handle_id: opened.handle_id,
                    });
                    reply.opened(fh, 0);
                }
                Err(status) => reply.error(status_to_errno(&status)),
            }
        }

        fn read(
            &mut self,
            _req: &FuseRequest<'_>,
            _ino: u64,
            fh: u64,
            offset: i64,
            size: u32,
            _flags: i32,
            _lock_owner: Option<u64>,
            reply: ReplyData,
        ) {
            let Some(handle) = self.handles.get(&fh).cloned() else {
                reply.error(ENOENT);
                return;
            };
            let mut io = IoClient::with_interceptor(self.channel.clone(), self.interceptor.clone());
            match self.rt.block_on(async {
                io.read(ReadRequest {
                    workspace_id: handle.workspace_id,
                    handle_id: handle.handle_id,
                    offset: u64::try_from(offset.max(0)).unwrap_or(0),
                    length: size,
                })
                .await
            }) {
                Ok(resp) => reply.data(&resp.into_inner().data),
                Err(status) => reply.error(status_to_errno(&status)),
            }
        }

        fn write(
            &mut self,
            _req: &FuseRequest<'_>,
            _ino: u64,
            fh: u64,
            offset: i64,
            data: &[u8],
            _write_flags: u32,
            _flags: i32,
            _lock_owner: Option<u64>,
            reply: ReplyWrite,
        ) {
            let Some(handle) = self.handles.get(&fh).cloned() else {
                reply.error(ENOENT);
                return;
            };
            let mut io = IoClient::with_interceptor(self.channel.clone(), self.interceptor.clone());
            match self.rt.block_on(async {
                io.write(WriteRequest {
                    workspace_id: handle.workspace_id,
                    handle_id: handle.handle_id,
                    offset: u64::try_from(offset.max(0)).unwrap_or(0),
                    data: data.to_vec(),
                })
                .await
            }) {
                Ok(resp) => {
                    let written =
                        u32::try_from(resp.into_inner().bytes_written).unwrap_or(u32::MAX);
                    reply.written(written);
                }
                Err(status) => reply.error(status_to_errno(&status)),
            }
        }

        fn flush(
            &mut self,
            _req: &FuseRequest<'_>,
            _ino: u64,
            fh: u64,
            _lock_owner: u64,
            reply: ReplyEmpty,
        ) {
            let Some(handle) = self.handles.get(&fh).cloned() else {
                reply.error(ENOENT);
                return;
            };
            let mut io = IoClient::with_interceptor(self.channel.clone(), self.interceptor.clone());
            match self.rt.block_on(async {
                io.flush(FlushRequest {
                    workspace_id: handle.workspace_id,
                    handle_id: handle.handle_id,
                })
                .await
            }) {
                Ok(_) => reply.ok(),
                Err(status) => reply.error(status_to_errno(&status)),
            }
        }

        fn release(
            &mut self,
            _req: &FuseRequest<'_>,
            _ino: u64,
            fh: u64,
            _flags: i32,
            _lock_owner: Option<u64>,
            _flush: bool,
            reply: ReplyEmpty,
        ) {
            let Some(handle) = self.handles.get(&fh).cloned() else {
                reply.error(ENOENT);
                return;
            };
            let mut io = IoClient::with_interceptor(self.channel.clone(), self.interceptor.clone());
            match self.rt.block_on(async {
                io.release(ReleaseRequest {
                    workspace_id: handle.workspace_id,
                    handle_id: handle.handle_id,
                })
                .await
            }) {
                Ok(_) => {
                    self.handles.remove(&fh);
                    reply.ok();
                }
                Err(status) => reply.error(status_to_errno(&status)),
            }
        }

        fn create(
            &mut self,
            _req: &FuseRequest<'_>,
            parent: u64,
            name: &OsStr,
            mode: u32,
            _umask: u32,
            flags: i32,
            reply: ReplyCreate,
        ) {
            let Some(parent_entry) = self.entry(parent).cloned() else {
                reply.error(ENOENT);
                return;
            };
            let mut mutation =
                MutationClient::with_interceptor(self.channel.clone(), self.interceptor.clone());
            let create_result = self.rt.block_on(async {
                mutation
                    .create(CreateRequest {
                        workspace_id: parent_entry.workspace_id.clone(),
                        parent_node_id: parent_entry.node_id.clone(),
                        name: name.to_string_lossy().to_string(),
                        kind: NodeKind::File as i32,
                        mode,
                        exclusive: true,
                    })
                    .await
            });
            let created = match create_result {
                Ok(resp) => resp.into_inner(),
                Err(status) => {
                    reply.error(status_to_errno(&status));
                    return;
                }
            };
            let Some(attrs) = created.attrs else {
                reply.error(EIO);
                return;
            };
            let ino = self.upsert_inode(
                &parent_entry.workspace_id,
                &created.node_id,
                parent,
                proto_attrs_to_fuse(0, &attrs),
            );
            self.invalidate_dir_cache(parent);

            let mut io = IoClient::with_interceptor(self.channel.clone(), self.interceptor.clone());
            match self.rt.block_on(async {
                io.open(OpenRequest {
                    workspace_id: parent_entry.workspace_id.clone(),
                    node_id: created.node_id,
                    mode: Self::handle_mode_from_flags(flags),
                })
                .await
            }) {
                Ok(resp) => {
                    let opened = resp.into_inner();
                    let fh = self.alloc_fh(HandleEntry {
                        workspace_id: parent_entry.workspace_id,
                        handle_id: opened.handle_id,
                    });
                    if let Some(entry) = self.inodes.get(&ino) {
                        reply.created(&TTL, &entry.attr, 0, fh, 0);
                    } else {
                        reply.error(EIO);
                    }
                }
                Err(status) => reply.error(status_to_errno(&status)),
            }
        }

        fn mkdir(
            &mut self,
            _req: &FuseRequest<'_>,
            parent: u64,
            name: &OsStr,
            mode: u32,
            _umask: u32,
            reply: ReplyEntry,
        ) {
            let Some(parent_entry) = self.entry(parent).cloned() else {
                reply.error(ENOENT);
                return;
            };
            let mut mutation =
                MutationClient::with_interceptor(self.channel.clone(), self.interceptor.clone());
            match self.rt.block_on(async {
                mutation
                    .create(CreateRequest {
                        workspace_id: parent_entry.workspace_id.clone(),
                        parent_node_id: parent_entry.node_id,
                        name: name.to_string_lossy().to_string(),
                        kind: NodeKind::Dir as i32,
                        mode,
                        exclusive: true,
                    })
                    .await
            }) {
                Ok(resp) => {
                    let body = resp.into_inner();
                    let Some(attrs) = body.attrs else {
                        reply.error(EIO);
                        return;
                    };
                    let ino = self.upsert_inode(
                        &parent_entry.workspace_id,
                        &body.node_id,
                        parent,
                        proto_attrs_to_fuse(0, &attrs),
                    );
                    self.invalidate_dir_cache(parent);
                    if let Some(entry) = self.inodes.get(&ino) {
                        reply.entry(&TTL, &entry.attr, 0);
                    } else {
                        reply.error(EIO);
                    }
                }
                Err(status) => reply.error(status_to_errno(&status)),
            }
        }

        fn unlink(&mut self, _req: &FuseRequest<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
            let Some(parent_entry) = self.entry(parent).cloned() else {
                reply.error(ENOENT);
                return;
            };
            let mut mutation =
                MutationClient::with_interceptor(self.channel.clone(), self.interceptor.clone());
            match self.rt.block_on(async {
                mutation
                    .unlink(UnlinkRequest {
                        workspace_id: parent_entry.workspace_id,
                        parent_node_id: parent_entry.node_id,
                        name: name.to_string_lossy().to_string(),
                        if_version: 0,
                    })
                    .await
            }) {
                Ok(_) => {
                    self.invalidate_dir_cache(parent);
                    reply.ok();
                }
                Err(status) => reply.error(status_to_errno(&status)),
            }
        }

        fn rmdir(&mut self, _req: &FuseRequest<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
            self.unlink(_req, parent, name, reply);
        }

        fn rename(
            &mut self,
            _req: &FuseRequest<'_>,
            parent: u64,
            name: &OsStr,
            newparent: u64,
            newname: &OsStr,
            _flags: u32,
            reply: ReplyEmpty,
        ) {
            let Some(from_parent) = self.entry(parent).cloned() else {
                reply.error(ENOENT);
                return;
            };
            let Some(to_parent) = self.entry(newparent).cloned() else {
                reply.error(ENOENT);
                return;
            };
            let mut mutation =
                MutationClient::with_interceptor(self.channel.clone(), self.interceptor.clone());
            match self.rt.block_on(async {
                mutation
                    .rename(RenameRequest {
                        workspace_id: from_parent.workspace_id.clone(),
                        from_parent_node_id: from_parent.node_id,
                        from_name: name.to_string_lossy().to_string(),
                        to_parent_node_id: to_parent.node_id,
                        to_name: newname.to_string_lossy().to_string(),
                        overwrite: true,
                        if_version: 0,
                    })
                    .await
            }) {
                Ok(resp) => {
                    let body = resp.into_inner();
                    if let Some(ino) = self.node_to_ino.get(&body.node_id).copied() {
                        if let Some(entry) = self.inodes.get_mut(&ino) {
                            entry.parent_ino = newparent;
                        }
                        self.invalidate_dir_cache(ino);
                    }
                    self.invalidate_dir_cache(parent);
                    self.invalidate_dir_cache(newparent);
                    reply.ok();
                }
                Err(status) => reply.error(status_to_errno(&status)),
            }
        }

        fn setattr(
            &mut self,
            _req: &FuseRequest<'_>,
            ino: u64,
            mode: Option<u32>,
            _uid: Option<u32>,
            _gid: Option<u32>,
            size: Option<u64>,
            _atime: Option<TimeOrNow>,
            mtime: Option<TimeOrNow>,
            _ctime: Option<SystemTime>,
            _fh: Option<u64>,
            _crtime: Option<SystemTime>,
            _chgtime: Option<SystemTime>,
            _bkuptime: Option<SystemTime>,
            _flags: Option<u32>,
            reply: ReplyAttr,
        ) {
            let Some(entry) = self.entry(ino).cloned() else {
                reply.error(ENOENT);
                return;
            };

            if let Some(target_size) = size {
                let mut io =
                    IoClient::with_interceptor(self.channel.clone(), self.interceptor.clone());
                let opened = self.rt.block_on(async {
                    io.open(OpenRequest {
                        workspace_id: entry.workspace_id.clone(),
                        node_id: entry.node_id.clone(),
                        mode: HandleMode::Write as i32,
                    })
                    .await
                });
                let opened = match opened {
                    Ok(resp) => resp.into_inner(),
                    Err(status) => {
                        reply.error(status_to_errno(&status));
                        return;
                    }
                };
                let handle_id = opened.handle_id;
                let truncate = self.rt.block_on(async {
                    io.truncate(TruncateRequest {
                        workspace_id: entry.workspace_id.clone(),
                        handle_id: handle_id.clone(),
                        size: target_size,
                    })
                    .await
                });
                if let Err(status) = truncate {
                    reply.error(status_to_errno(&status));
                    return;
                }
                let flush = self.rt.block_on(async {
                    io.flush(FlushRequest {
                        workspace_id: entry.workspace_id.clone(),
                        handle_id: handle_id.clone(),
                    })
                    .await
                });
                if let Err(status) = flush {
                    reply.error(status_to_errno(&status));
                    return;
                }
                let _ = self.rt.block_on(async {
                    io.release(ReleaseRequest {
                        workspace_id: entry.workspace_id.clone(),
                        handle_id,
                    })
                    .await
                });
            }

            if mode.is_some() || mtime.is_some() {
                let mut mutation = MutationClient::with_interceptor(
                    self.channel.clone(),
                    self.interceptor.clone(),
                );
                let request = SetAttrsRequest {
                    workspace_id: entry.workspace_id.clone(),
                    node_id: entry.node_id.clone(),
                    patch: Some(AttrsPatch {
                        has_mode: mode.is_some(),
                        mode: mode.unwrap_or_default(),
                        has_mtime_unix_nano: mtime.is_some(),
                        mtime_unix_nano: mtime.and_then(time_or_now_to_nanos).unwrap_or_default(),
                    }),
                };
                if let Err(status) = self
                    .rt
                    .block_on(async { mutation.set_attrs(request).await })
                {
                    reply.error(status_to_errno(&status));
                    return;
                }
            }

            let mut ns =
                NamespaceClient::with_interceptor(self.channel.clone(), self.interceptor.clone());
            match self.rt.block_on(async {
                ns.get_attrs(GetAttrsRequest {
                    workspace_id: entry.workspace_id,
                    node_id: entry.node_id,
                })
                .await
            }) {
                Ok(resp) => {
                    let Some(attrs) = resp.into_inner().attrs else {
                        reply.error(EIO);
                        return;
                    };
                    let attr = proto_attrs_to_fuse(ino, &attrs);
                    if let Some(existing) = self.inodes.get_mut(&ino) {
                        existing.attr = attr;
                    }
                    reply.attr(&TTL, &attr);
                }
                Err(status) => reply.error(status_to_errno(&status)),
            }
        }
    }

    fn status_to_errno(status: &tonic::Status) -> i32 {
        match status.code() {
            Code::Cancelled => EINTR,
            Code::NotFound => ENOENT,
            Code::AlreadyExists => EEXIST,
            Code::PermissionDenied | Code::Unauthenticated => EACCES,
            Code::InvalidArgument => EINVAL,
            Code::DeadlineExceeded => ETIMEDOUT,
            Code::ResourceExhausted => ENOSPC,
            Code::FailedPrecondition => EBUSY,
            Code::Aborted => EBUSY,
            Code::OutOfRange => EOVERFLOW,
            Code::Unimplemented => ENOSYS,
            Code::Unavailable => EAGAIN,
            _ => EIO,
        }
    }

    fn time_or_now_to_nanos(value: TimeOrNow) -> Option<i64> {
        let ts = match value {
            TimeOrNow::SpecificTime(ts) => ts,
            TimeOrNow::Now => SystemTime::now(),
        };
        let duration = ts.duration_since(UNIX_EPOCH).ok()?;
        i64::try_from(duration.as_nanos()).ok()
    }

    fn proto_attrs_to_fuse(ino: u64, attrs: &scry_proto::scry::v1::Attrs) -> FileAttr {
        let kind = match scry_proto::scry::v1::NodeKind::try_from(attrs.kind)
            .unwrap_or(scry_proto::scry::v1::NodeKind::Unspecified)
        {
            scry_proto::scry::v1::NodeKind::Dir => FileType::Directory,
            _ => FileType::RegularFile,
        };
        let now = SystemTime::now();
        let mtime = UNIX_EPOCH
            .checked_add(Duration::from_nanos(
                u64::try_from(attrs.mtime_unix_nano.max(0)).unwrap_or(0),
            ))
            .unwrap_or(now);
        FileAttr {
            ino,
            size: attrs.size,
            blocks: (attrs.size.saturating_add(511)) / 512,
            atime: mtime,
            mtime,
            ctime: mtime,
            crtime: mtime,
            kind,
            perm: (attrs.mode & 0o7777) as u16,
            nlink: 1,
            uid: 0,
            gid: 0,
            rdev: 0,
            flags: 0,
            blksize: 4096,
        }
    }

    #[derive(Parser, Debug)]
    #[command(name = "scry-fuse", about = "Mount a Scry workspace through FUSE")]
    struct Cli {
        #[arg(long, default_value = "127.0.0.1:50051")]
        server_addr: String,
        #[arg(long, default_value_t = false)]
        tls: bool,
        #[arg(long)]
        ca_cert: Option<PathBuf>,
        #[arg(long)]
        client_cert: Option<PathBuf>,
        #[arg(long)]
        client_key: Option<PathBuf>,
        #[arg(long)]
        auth_token: Option<String>,
        #[arg(long)]
        auth_token_file: Option<PathBuf>,
        #[arg(long)]
        workspace_id: String,
        #[arg(long)]
        mountpoint: PathBuf,
        #[arg(long, default_value_t = false)]
        read_only: bool,
        #[arg(long, default_value_t = false)]
        allow_other: bool,
        /// Skip `DefaultPermissions` so the mounting uid can create files (for unprivileged CI).
        /// Production mounts should omit this flag so normal permission checks apply.
        #[arg(long, default_value_t = false, hide = true)]
        user_mount: bool,
    }

    impl Cli {
        fn validate(&self) -> Result<()> {
            if self.auth_token.is_some() && self.auth_token_file.is_some() {
                anyhow::bail!("use either --auth-token or --auth-token-file, not both");
            }
            if !self.tls
                && (self.ca_cert.is_some()
                    || self.client_cert.is_some()
                    || self.client_key.is_some())
            {
                anyhow::bail!("--ca-cert/--client-cert/--client-key require --tls");
            }
            if self.client_cert.is_some() != self.client_key.is_some() {
                anyhow::bail!("--client-cert and --client-key must be set together");
            }
            Ok(())
        }
    }

    fn mount_options(read_only: bool, allow_other: bool, user_mount: bool) -> Vec<MountOption> {
        // Do not use `MountOption::AutoUnmount`: fuser enables `allow_other` for auto-unmount,
        // which requires `user_allow_other` in /etc/fuse.conf (often unset on CI / GitHub Actions).
        let mut opts = vec![MountOption::FSName("scry".to_string())];
        if !user_mount {
            opts.push(MountOption::DefaultPermissions);
        }
        if read_only {
            opts.push(MountOption::RO);
        }
        if allow_other {
            opts.push(MountOption::AllowOther);
        }
        opts
    }

    fn load_auth_token(cli: &Cli) -> Result<Option<String>> {
        if cli.auth_token.is_some() && cli.auth_token_file.is_some() {
            anyhow::bail!("use either --auth-token or --auth-token-file, not both");
        }
        if let Some(token) = &cli.auth_token {
            let trimmed = token.trim();
            if trimmed.is_empty() {
                anyhow::bail!("--auth-token cannot be empty");
            }
            return Ok(Some(trimmed.to_string()));
        }
        if let Some(path) = &cli.auth_token_file {
            let raw = std::fs::read_to_string(path)
                .with_context(|| format!("failed reading auth token file {}", path.display()))?;
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                anyhow::bail!("auth token file {} is empty", path.display());
            }
            return Ok(Some(trimmed.to_string()));
        }
        Ok(None)
    }

    #[cfg(test)]
    mod cli_tests {
        use super::{load_auth_token, Cli};
        use std::path::PathBuf;

        #[test]
        fn cli_validate_rejects_token_conflict() {
            let cli = Cli {
                server_addr: "127.0.0.1:50051".to_string(),
                tls: false,
                ca_cert: None,
                client_cert: None,
                client_key: None,
                auth_token: Some("token-a".to_string()),
                auth_token_file: Some(PathBuf::from("/tmp/token.txt")),
                workspace_id: "ws".to_string(),
                mountpoint: PathBuf::from("/tmp/mnt"),
                read_only: false,
                allow_other: false,
                user_mount: false,
            };
            let err = cli
                .validate()
                .expect_err("validate should reject dual token inputs");
            assert!(err
                .to_string()
                .contains("either --auth-token or --auth-token-file"));
        }

        #[test]
        fn cli_validate_rejects_tls_flags_without_tls() {
            let cli = Cli {
                server_addr: "127.0.0.1:50051".to_string(),
                tls: false,
                ca_cert: Some(PathBuf::from("/tmp/ca.pem")),
                client_cert: None,
                client_key: None,
                auth_token: None,
                auth_token_file: None,
                workspace_id: "ws".to_string(),
                mountpoint: PathBuf::from("/tmp/mnt"),
                read_only: false,
                allow_other: false,
                user_mount: false,
            };
            let err = cli
                .validate()
                .expect_err("validate should reject TLS options without --tls");
            assert!(err.to_string().contains("require --tls"));
        }

        #[test]
        fn load_auth_token_prefers_inline_token() {
            let cli = Cli {
                server_addr: "127.0.0.1:50051".to_string(),
                tls: false,
                ca_cert: None,
                client_cert: None,
                client_key: None,
                auth_token: Some("  abc-token  ".to_string()),
                auth_token_file: None,
                workspace_id: "ws".to_string(),
                mountpoint: PathBuf::from("/tmp/mnt"),
                read_only: false,
                allow_other: false,
                user_mount: false,
            };
            let token = load_auth_token(&cli).expect("token should load");
            assert_eq!(token.as_deref(), Some("abc-token"));
        }
    }

    #[cfg(test)]
    #[allow(clippy::items_after_test_module)]
    mod tests {
        use super::ScryFuseFs;
        use std::{
            collections::HashMap,
            path::{Path, PathBuf},
            process::{Child, Command, Stdio},
            sync::{Arc, Mutex},
            time::{Duration, SystemTime},
        };

        use fuser::{spawn_mount2, MountOption};
        use scry_proto::scry::v1::{
            admin_client::AdminClient, events_client::EventsClient, io_server::Io,
            io_server::IoServer, namespace_server::Namespace, namespace_server::NamespaceServer,
            search_client::SearchClient, Attrs, CreateWorkspaceRequest, DirEntry, EventKind,
            FlushRequest, FlushResponse, GetAttrsIfChangedRequest, GetAttrsIfChangedResponse,
            GetAttrsRequest, GetAttrsResponse, LookupRequest, LookupResponse, NodeKind,
            OpenRequest, OpenResponse, ReadDirRequest, ReadDirResponse, ReadRequest, ReadResponse,
            ReleaseRequest, ReleaseResponse, ResolvePathRequest, ResolvePathResponse,
            ResolveRefRequest, ResolveRefResponse, SearchMode, SearchRequest, SubscribeFilter,
            SubscribeRequest, TruncateRequest, TruncateResponse, WriteRequest, WriteResponse,
        };
        use tokio::runtime::{Builder, Runtime};
        use tonic::{
            transport::{Channel, Endpoint, Server},
            Request, Response, Status,
        };

        struct Fixture {
            workspace_id: String,
            root_node_id: String,
            file_node_id: String,
            file_name: String,
            file_content: Mutex<Vec<u8>>,
            handles: Mutex<HashMap<String, String>>,
            next_handle: Mutex<u64>,
        }

        impl Fixture {
            fn new() -> Self {
                Self {
                    workspace_id: "ws-fuse-e2e".to_string(),
                    root_node_id: "root-node".to_string(),
                    file_node_id: "file-node".to_string(),
                    file_name: "hello.txt".to_string(),
                    file_content: Mutex::new(b"hello fuse e2e\n".to_vec()),
                    handles: Mutex::new(HashMap::new()),
                    next_handle: Mutex::new(1),
                }
            }

            fn file_content_bytes(&self) -> Vec<u8> {
                self.file_content
                    .lock()
                    .expect("file content poisoned")
                    .clone()
            }

            fn root_attrs(&self) -> Attrs {
                Attrs {
                    kind: NodeKind::Dir as i32,
                    size: 0,
                    mtime_unix_nano: 1,
                    mode: 0o755,
                    version: 1,
                    content_hash: String::new(),
                    mime: String::new(),
                }
            }

            fn file_attrs(&self) -> Attrs {
                let file_len = self
                    .file_content
                    .lock()
                    .map(|content| content.len())
                    .unwrap_or_default();
                Attrs {
                    kind: NodeKind::File as i32,
                    size: u64::try_from(file_len).unwrap_or(0),
                    mtime_unix_nano: 1,
                    mode: 0o666,
                    version: 1,
                    content_hash: String::new(),
                    mime: String::new(),
                }
            }
        }

        #[derive(Clone)]
        struct FakeNamespace {
            fixture: Arc<Fixture>,
        }

        #[tonic::async_trait]
        impl Namespace for FakeNamespace {
            async fn lookup(
                &self,
                request: Request<LookupRequest>,
            ) -> Result<Response<LookupResponse>, Status> {
                let req = request.into_inner();
                if req.workspace_id != self.fixture.workspace_id {
                    return Err(Status::not_found("workspace not found"));
                }
                if req.parent_node_id != self.fixture.root_node_id
                    || req.name != self.fixture.file_name
                {
                    return Err(Status::not_found("node not found"));
                }
                Ok(Response::new(LookupResponse {
                    node_id: self.fixture.file_node_id.clone(),
                    attrs: Some(self.fixture.file_attrs()),
                }))
            }

            async fn get_attrs(
                &self,
                request: Request<GetAttrsRequest>,
            ) -> Result<Response<GetAttrsResponse>, Status> {
                let req = request.into_inner();
                if req.workspace_id != self.fixture.workspace_id {
                    return Err(Status::not_found("workspace not found"));
                }
                let attrs = if req.node_id == self.fixture.root_node_id {
                    self.fixture.root_attrs()
                } else if req.node_id == self.fixture.file_node_id {
                    self.fixture.file_attrs()
                } else {
                    return Err(Status::not_found("node not found"));
                };
                Ok(Response::new(GetAttrsResponse { attrs: Some(attrs) }))
            }

            async fn get_attrs_if_changed(
                &self,
                request: Request<GetAttrsIfChangedRequest>,
            ) -> Result<Response<GetAttrsIfChangedResponse>, Status> {
                let req = request.into_inner();
                if req.workspace_id != self.fixture.workspace_id {
                    return Err(Status::not_found("workspace not found"));
                }
                if req.node_id != self.fixture.file_node_id {
                    return Err(Status::not_found("node not found"));
                }
                if req.known_version == 1 {
                    Ok(Response::new(GetAttrsIfChangedResponse {
                        changed: false,
                        attrs: None,
                    }))
                } else {
                    Ok(Response::new(GetAttrsIfChangedResponse {
                        changed: true,
                        attrs: Some(self.fixture.file_attrs()),
                    }))
                }
            }

            async fn read_dir(
                &self,
                request: Request<ReadDirRequest>,
            ) -> Result<Response<ReadDirResponse>, Status> {
                let req = request.into_inner();
                if req.workspace_id != self.fixture.workspace_id {
                    return Err(Status::not_found("workspace not found"));
                }
                if req.node_id != self.fixture.root_node_id {
                    return Err(Status::not_found("directory not found"));
                }
                if !req.cursor.is_empty() {
                    return Ok(Response::new(ReadDirResponse {
                        entries: Vec::new(),
                        next_cursor: String::new(),
                    }));
                }
                Ok(Response::new(ReadDirResponse {
                    entries: vec![DirEntry {
                        name: self.fixture.file_name.clone(),
                        node_id: self.fixture.file_node_id.clone(),
                        attrs: Some(self.fixture.file_attrs()),
                    }],
                    next_cursor: String::new(),
                }))
            }

            async fn resolve_ref(
                &self,
                request: Request<ResolveRefRequest>,
            ) -> Result<Response<ResolveRefResponse>, Status> {
                let req = request.into_inner();
                if req.workspace_id != self.fixture.workspace_id {
                    return Err(Status::not_found("workspace not found"));
                }
                let path = if req.node_id == self.fixture.file_node_id {
                    self.fixture.file_name.clone()
                } else {
                    String::new()
                };
                Ok(Response::new(ResolveRefResponse {
                    path: path.clone(),
                    exists: !path.is_empty(),
                    redirect_to_node_id: String::new(),
                }))
            }

            async fn resolve_path(
                &self,
                request: Request<ResolvePathRequest>,
            ) -> Result<Response<ResolvePathResponse>, Status> {
                let req = request.into_inner();
                if req.workspace_id != self.fixture.workspace_id {
                    return Err(Status::not_found("workspace not found"));
                }
                if req.path.is_empty() {
                    return Ok(Response::new(ResolvePathResponse {
                        node_id: self.fixture.root_node_id.clone(),
                        exists: true,
                    }));
                }
                if req.path == self.fixture.file_name {
                    return Ok(Response::new(ResolvePathResponse {
                        node_id: self.fixture.file_node_id.clone(),
                        exists: true,
                    }));
                }
                Ok(Response::new(ResolvePathResponse {
                    node_id: String::new(),
                    exists: false,
                }))
            }
        }

        #[derive(Clone)]
        struct FakeIo {
            fixture: Arc<Fixture>,
        }

        #[tonic::async_trait]
        impl Io for FakeIo {
            async fn open(
                &self,
                request: Request<OpenRequest>,
            ) -> Result<Response<OpenResponse>, Status> {
                let req = request.into_inner();
                if req.workspace_id != self.fixture.workspace_id
                    || req.node_id != self.fixture.file_node_id
                {
                    return Err(Status::not_found("node not found"));
                }
                let mut handle_seq = self
                    .fixture
                    .next_handle
                    .lock()
                    .map_err(|_| Status::internal("handle sequence poisoned"))?;
                let handle_id = format!("h-{}", *handle_seq);
                *handle_seq = handle_seq.saturating_add(1);
                drop(handle_seq);

                let mut handles = self
                    .fixture
                    .handles
                    .lock()
                    .map_err(|_| Status::internal("handle map poisoned"))?;
                handles.insert(handle_id.clone(), req.node_id);
                drop(handles);

                Ok(Response::new(OpenResponse {
                    handle_id,
                    snapshot_version: 1,
                    attrs: Some(self.fixture.file_attrs()),
                }))
            }

            async fn read(
                &self,
                request: Request<ReadRequest>,
            ) -> Result<Response<ReadResponse>, Status> {
                let req = request.into_inner();
                if req.workspace_id != self.fixture.workspace_id {
                    return Err(Status::not_found("workspace not found"));
                }
                let handles = self
                    .fixture
                    .handles
                    .lock()
                    .map_err(|_| Status::internal("handle map poisoned"))?;
                if !handles.contains_key(&req.handle_id) {
                    return Err(Status::not_found("handle not found"));
                }
                drop(handles);

                let content = self.fixture.file_content_bytes();
                let start = usize::try_from(req.offset).unwrap_or(usize::MAX);
                if start >= content.len() {
                    return Ok(Response::new(ReadResponse {
                        data: Vec::new(),
                        eof: true,
                    }));
                }
                let len = usize::try_from(req.length).unwrap_or(usize::MAX);
                let end = start.saturating_add(len).min(content.len());
                Ok(Response::new(ReadResponse {
                    data: content[start..end].to_vec(),
                    eof: end >= content.len(),
                }))
            }

            async fn write(
                &self,
                request: Request<WriteRequest>,
            ) -> Result<Response<WriteResponse>, Status> {
                let req = request.into_inner();
                if req.workspace_id != self.fixture.workspace_id {
                    return Err(Status::not_found("workspace not found"));
                }
                let handles = self
                    .fixture
                    .handles
                    .lock()
                    .map_err(|_| Status::internal("handle map poisoned"))?;
                if !handles.contains_key(&req.handle_id) {
                    return Err(Status::not_found("handle not found"));
                }
                drop(handles);

                let offset = usize::try_from(req.offset).unwrap_or(usize::MAX);
                let mut content = self
                    .fixture
                    .file_content
                    .lock()
                    .map_err(|_| Status::internal("file content poisoned"))?;
                if offset == usize::MAX {
                    return Err(Status::invalid_argument("offset too large"));
                }
                let required = offset.saturating_add(req.data.len());
                if content.len() < required {
                    content.resize(required, 0);
                }
                content[offset..offset + req.data.len()].copy_from_slice(&req.data);
                Ok(Response::new(WriteResponse {
                    bytes_written: u64::try_from(req.data.len()).unwrap_or(u64::MAX),
                }))
            }

            async fn truncate(
                &self,
                request: Request<TruncateRequest>,
            ) -> Result<Response<TruncateResponse>, Status> {
                let req = request.into_inner();
                if req.workspace_id != self.fixture.workspace_id {
                    return Err(Status::not_found("workspace not found"));
                }
                let handles = self
                    .fixture
                    .handles
                    .lock()
                    .map_err(|_| Status::internal("handle map poisoned"))?;
                if !handles.contains_key(&req.handle_id) {
                    return Err(Status::not_found("handle not found"));
                }
                drop(handles);

                let target_len = usize::try_from(req.size)
                    .map_err(|_| Status::invalid_argument("truncate size too large"))?;
                let mut content = self
                    .fixture
                    .file_content
                    .lock()
                    .map_err(|_| Status::internal("file content poisoned"))?;
                content.resize(target_len, 0);
                Ok(Response::new(TruncateResponse {}))
            }

            async fn flush(
                &self,
                _request: Request<FlushRequest>,
            ) -> Result<Response<FlushResponse>, Status> {
                Ok(Response::new(FlushResponse {
                    version: 1,
                    attrs: Some(self.fixture.file_attrs()),
                }))
            }

            async fn release(
                &self,
                request: Request<ReleaseRequest>,
            ) -> Result<Response<ReleaseResponse>, Status> {
                let req = request.into_inner();
                if req.workspace_id != self.fixture.workspace_id {
                    return Err(Status::not_found("workspace not found"));
                }
                let mut handles = self
                    .fixture
                    .handles
                    .lock()
                    .map_err(|_| Status::internal("handle map poisoned"))?;
                if handles.remove(&req.handle_id).is_none() {
                    return Err(Status::not_found("handle not found"));
                }
                Ok(Response::new(ReleaseResponse { version: 1 }))
            }
        }

        fn should_run_fuse_e2e() -> bool {
            matches!(std::env::var("SCRY_FUSE_E2E").as_deref(), Ok("1"))
                && Path::new("/dev/fuse").exists()
                && has_fusermount_binary()
        }

        fn should_run_real_scryd_fuse_e2e() -> bool {
            matches!(
                std::env::var("SCRY_FUSE_REAL_SCRYD_E2E").as_deref(),
                Ok("1")
            ) && Path::new("/dev/fuse").exists()
                && has_fusermount_binary()
        }

        fn has_fusermount_binary() -> bool {
            [
                "/bin/fusermount3",
                "/usr/bin/fusermount3",
                "/bin/fusermount",
                "/usr/bin/fusermount",
            ]
            .iter()
            .any(|path| Path::new(path).exists())
        }

        fn unique_mountpoint() -> PathBuf {
            let nanos = SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            std::env::temp_dir().join(format!("scry-fuse-e2e-{}-{nanos}", std::process::id()))
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

        fn run_async_test<F>(future: F)
        where
            F: std::future::Future<Output = ()>,
        {
            Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("build test runtime")
                .block_on(future);
        }

        #[test]
        fn fuse_mount_e2e_reads_real_file() {
            run_async_test(async {
                if !should_run_fuse_e2e() {
                    eprintln!("skipping fuse e2e: set SCRY_FUSE_E2E=1 and ensure /dev/fuse exists");
                    return;
                }

                let fixture = Arc::new(Fixture::new());
                let namespace = FakeNamespace {
                    fixture: Arc::clone(&fixture),
                };
                let io = FakeIo {
                    fixture: Arc::clone(&fixture),
                };

                let socket = std::net::TcpListener::bind("127.0.0.1:0").expect("bind socket");
                let addr = socket.local_addr().expect("socket addr");
                drop(socket);

                let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
                let server_task = tokio::spawn(async move {
                    Server::builder()
                        .add_service(NamespaceServer::new(namespace))
                        .add_service(IoServer::new(io))
                        .serve_with_shutdown(addr, async {
                            let _ = shutdown_rx.await;
                        })
                        .await
                        .expect("serve fake grpc");
                });

                let channel = connect_with_retry(addr).await;
                let workspace_id = fixture.workspace_id.clone();
                let fs = tokio::task::spawn_blocking(move || {
                    let rt = Arc::new(Runtime::new().expect("create runtime"));
                    // Keep one Arc alive for the whole test process so runtime teardown
                    // never happens from async context during FUSE session cleanup.
                    let leaked = Arc::clone(&rt);
                    std::mem::forget(leaked);
                    ScryFuseFs::new(rt, channel, workspace_id)
                })
                .await
                .expect("build fs task join")
                .expect("create fuse fs");

                let mountpoint = unique_mountpoint();
                std::fs::create_dir_all(&mountpoint).expect("create mountpoint");
                let options: Vec<MountOption> = vec![MountOption::FSName("scry-e2e".to_string())];
                let session = spawn_mount2(fs, &mountpoint, &options).expect("mount fuse fs");

                let file_path = mountpoint.join(&fixture.file_name);
                let mut exists = false;
                for _ in 0..100 {
                    if file_path.exists() {
                        exists = true;
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                assert!(exists, "mounted file should appear in mountpoint");

                let bytes = std::fs::read(&file_path).expect("read mounted file");
                assert_eq!(
                    bytes,
                    fixture.file_content_bytes(),
                    "mounted file content mismatch"
                );

                let new_content = b"fuse e2e write path\n";
                std::fs::write(&file_path, new_content).expect("write mounted file");
                let updated = std::fs::read(&file_path).expect("read updated mounted file");
                assert_eq!(updated, new_content, "mounted file should reflect write");

                drop(session);
                tokio::time::sleep(Duration::from_millis(100)).await;
                let _ = std::fs::remove_dir_all(&mountpoint);
                let _ = shutdown_tx.send(());
                server_task.await.expect("server task");
            });
        }

        fn resolve_scryd_binary_path() -> PathBuf {
            if let Ok(bin) = std::env::var("CARGO_BIN_EXE_scryd") {
                return PathBuf::from(bin);
            }
            let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            let workspace_root = manifest_dir
                .parent()
                .and_then(|p| p.parent())
                .map(Path::to_path_buf)
                .expect("failed to resolve workspace root");
            workspace_root.join("target").join("debug").join("scryd")
        }

        fn start_real_scryd(grpc_addr: &str, content_root: &Path, index_root: &Path) -> Child {
            let scryd_bin = resolve_scryd_binary_path();
            assert!(
                scryd_bin.exists(),
                "scryd binary not found at {} (build with `cargo build -p scryd`)",
                scryd_bin.display()
            );
            Command::new(scryd_bin)
                .env("SCRYD_DEV_MODE", "1")
                .env(
                    "SCRYD_AUTH_SECRET",
                    "fuse-real-scryd-e2e-secret-not-default",
                )
                .env("SCRYD_ALLOW_LOCAL_NO_AUTH", "1")
                .env("SCRYD_GRPC_ADDR", grpc_addr)
                .env("SCRYD_CONTENT_ROOT", content_root)
                .env("SCRYD_INDEX_ROOT", index_root)
                .env("SCRYD_EMBED_PROVIDER", "mock")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("failed to start scryd")
        }

        #[test]
        fn fuse_mount_with_real_scryd_updates_index() {
            run_async_test(async {
                if !should_run_real_scryd_fuse_e2e() {
                    eprintln!(
                        "skipping real scryd fuse e2e: set SCRY_FUSE_REAL_SCRYD_E2E=1 and ensure /dev/fuse exists"
                    );
                    return;
                }

                let tmp = tempfile::tempdir().expect("tempdir");
                let content_root = tmp.path().join("content");
                std::fs::create_dir_all(&content_root).expect("create content root");
                let index_root = tmp.path().join("index");
                std::fs::create_dir_all(&index_root).expect("create index root");

                let socket = std::net::TcpListener::bind("127.0.0.1:0").expect("bind socket");
                let addr = socket.local_addr().expect("socket addr");
                drop(socket);
                let grpc_addr = format!("{addr}");

                let mut scryd = start_real_scryd(&grpc_addr, &content_root, &index_root);

                let channel = connect_with_retry(addr).await;
                let mut admin = AdminClient::new(channel.clone());
                let mut search = SearchClient::new(channel.clone());
                let mut events = EventsClient::new(channel.clone());

                let created = admin
                    .create_workspace(CreateWorkspaceRequest {
                        name: "fuse-real-scryd-e2e".to_string(),
                        config: None,
                    })
                    .await
                    .expect("create workspace")
                    .into_inner();
                let workspace_id = created.workspace_id;

                let fs_workspace_id = workspace_id.clone();
                let fs = tokio::task::spawn_blocking(move || {
                    let rt = Arc::new(Runtime::new().expect("create runtime"));
                    // Keep one Arc alive for the whole test process so runtime teardown
                    // never happens from async context during FUSE session cleanup.
                    let leaked = Arc::clone(&rt);
                    std::mem::forget(leaked);
                    ScryFuseFs::new(rt, channel, fs_workspace_id)
                })
                .await
                .expect("build fs task join")
                .expect("create fuse fs");
                let mountpoint = unique_mountpoint();
                std::fs::create_dir_all(&mountpoint).expect("create mountpoint");
                let options: Vec<MountOption> =
                    vec![MountOption::FSName("scry-real-scryd-e2e".to_string())];
                let session = spawn_mount2(fs, &mountpoint, &options).expect("mount fuse fs");

                let file_path = mountpoint.join("search.md");
                std::fs::write(&file_path, b"fuse real scryd indexing phrase\n")
                    .expect("write mounted file");
                let mounted_read = std::fs::read(&file_path).expect("read mounted file");
                assert_eq!(mounted_read, b"fuse real scryd indexing phrase\n");

                let mut stream = events
                    .subscribe(SubscribeRequest {
                        workspace_id: workspace_id.clone(),
                        since_cursor: "0".to_string(),
                        subscriber_id: "fuse-real-scryd-e2e".to_string(),
                        filter: Some(SubscribeFilter {
                            path_prefix: vec!["search.md".to_string()],
                            node_ids: vec![],
                            kinds: vec![EventKind::IndexUpdated as i32],
                            include_index_events: true,
                        }),
                    })
                    .await
                    .expect("subscribe index events")
                    .into_inner();

                let mut saw_index_updated = false;
                for _ in 0..80 {
                    let maybe = tokio::time::timeout(Duration::from_millis(200), stream.message())
                        .await
                        .expect("index event timeout")
                        .expect("index stream call");
                    if let Some(event) = maybe {
                        if event.kind == EventKind::IndexUpdated as i32
                            && event.path.ends_with("search.md")
                        {
                            saw_index_updated = true;
                            break;
                        }
                    }
                }
                assert!(
                    saw_index_updated,
                    "expected IndexUpdated event after fuse write"
                );

                let mut hit_found = false;
                for _ in 0..40 {
                    let resp = search
                        .search(SearchRequest {
                            workspace_id: workspace_id.clone(),
                            query: "indexing phrase".to_string(),
                            mode: SearchMode::Hybrid as i32,
                            limit: 10,
                        })
                        .await
                        .expect("search call")
                        .into_inner();
                    if resp.hits.iter().any(|hit| hit.path.ends_with("search.md")) {
                        hit_found = true;
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                assert!(
                    hit_found,
                    "search should hit file written through fuse mount"
                );

                drop(session);
                tokio::time::sleep(Duration::from_millis(100)).await;
                let _ = std::fs::remove_dir_all(&mountpoint);
                let _ = scryd.kill();
                let _ = scryd.wait();
            });
        }
    }

    pub fn run() -> Result<()> {
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| "info".into()),
            )
            .compact()
            .init();

        let cli = Cli::parse();
        cli.validate()?;
        let token = load_auth_token(&cli)?;
        let interceptor = AuthInterceptor::from_bearer_token(token.as_deref())?;
        let scheme = if cli.tls { "https" } else { "http" };
        let mut endpoint = Endpoint::from_shared(format!("{scheme}://{}", cli.server_addr))
            .context("invalid server address")?;
        if cli.tls {
            let mut tls = ClientTlsConfig::new();
            if let Some(path) = &cli.ca_cert {
                let ca_pem = std::fs::read(path)
                    .with_context(|| format!("failed reading CA cert {}", path.display()))?;
                tls = tls.ca_certificate(Certificate::from_pem(ca_pem));
            }
            if let (Some(cert_path), Some(key_path)) = (&cli.client_cert, &cli.client_key) {
                let cert_pem = std::fs::read(cert_path).with_context(|| {
                    format!("failed reading client cert {}", cert_path.display())
                })?;
                let key_pem = std::fs::read(key_path)
                    .with_context(|| format!("failed reading client key {}", key_path.display()))?;
                tls = tls.identity(Identity::from_pem(cert_pem, key_pem));
            }
            endpoint = endpoint
                .tls_config(tls)
                .context("invalid TLS client configuration")?;
        }

        let rt = Arc::new(Runtime::new().context("failed creating tokio runtime")?);
        let channel = rt
            .block_on(endpoint.connect())
            .context("failed connecting gRPC endpoint")?;
        let fs = ScryFuseFs::new_with_interceptor(
            Arc::clone(&rt),
            channel,
            cli.workspace_id.clone(),
            interceptor,
        )
        .context("failed bootstrapping scry fuse filesystem")?;
        info!(
            workspace_id = %cli.workspace_id,
            mountpoint = %cli.mountpoint.display(),
            read_only = cli.read_only,
            allow_other = cli.allow_other,
            "mounting scry fuse filesystem"
        );
        fuser::mount2(
            fs,
            &cli.mountpoint,
            &mount_options(cli.read_only, cli.allow_other, cli.user_mount),
        )
        .context("failed to mount fuse filesystem")
    }
}

#[cfg(feature = "fuse")]
fn main() -> anyhow::Result<()> {
    fuse_app::run()
}
