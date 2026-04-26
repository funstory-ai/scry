//! In-memory vs tempfile-backed buffer for large `IO` handles (plan P2-3).

use crate::server::prelude::*;

use bytes::{Bytes, BytesMut};
use tempfile::NamedTempFile;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

/// Writable handle body: small edits stay in `BytesMut`; larger bodies spill to a tempfile.
#[derive(Debug)]
pub(crate) enum HandleBody {
    Memory(BytesMut),
    Spilled { file: File, len: u64, path: PathBuf },
}

impl Default for HandleBody {
    fn default() -> Self {
        Self::empty()
    }
}

impl HandleBody {
    pub(crate) fn empty() -> Self {
        HandleBody::Memory(BytesMut::new())
    }

    pub(crate) fn from_bytes(data: Vec<u8>, memory_limit: usize) -> std::io::Result<Self> {
        if data.len() <= memory_limit {
            let mut m = BytesMut::with_capacity(data.len());
            m.extend_from_slice(&data);
            return Ok(HandleBody::Memory(m));
        }
        let mut tmp = NamedTempFile::new()?;
        std::io::Write::write_all(&mut tmp, &data)?;
        let path = tmp.path().to_path_buf();
        let file = tmp.into_file();
        let len = u64::try_from(data.len()).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "content length overflow")
        })?;
        file.set_len(len)?;
        Ok(HandleBody::Spilled {
            file: File::from_std(file),
            len,
            path,
        })
    }

    pub(crate) fn len(&self) -> u64 {
        match self {
            HandleBody::Memory(b) => b.len() as u64,
            HandleBody::Spilled { len, .. } => *len,
        }
    }

    pub(crate) async fn read_window(
        &mut self,
        offset: u64,
        length: u32,
    ) -> std::io::Result<(Bytes, bool)> {
        let len_u64 = self.len();
        let start = usize::try_from(offset).unwrap_or(usize::MAX);
        if offset >= len_u64 {
            return Ok((Bytes::new(), true));
        }
        let take = usize::try_from(length).unwrap_or(usize::MAX);
        let end = offset.saturating_add(take as u64).min(len_u64) as usize;
        let start = start.min(end);

        match self {
            HandleBody::Memory(b) => {
                let data = Bytes::copy_from_slice(&b[start..end]);
                let eof = end >= b.len();
                Ok((data, eof))
            }
            HandleBody::Spilled { file, len, .. } => {
                let total = *len as usize;
                file.seek(std::io::SeekFrom::Start(offset)).await?;
                let to_read = end - start;
                let mut buf = vec![0u8; to_read];
                file.read_exact(&mut buf).await?;
                let eof = end >= total;
                Ok((Bytes::from(buf), eof))
            }
        }
    }

    pub(crate) async fn write_at(
        &mut self,
        offset: u64,
        data: &[u8],
        memory_limit: usize,
    ) -> std::io::Result<()> {
        let required_end = offset.saturating_add(data.len() as u64);
        match self {
            HandleBody::Memory(b) => {
                let required_len = usize::try_from(required_end).unwrap_or(usize::MAX);
                if required_len > memory_limit {
                    let mem = std::mem::take(b);
                    *self = Self::spill_memory_to_disk(mem).await?;
                    return Box::pin(self.write_at(offset, data, memory_limit)).await;
                }
                let offset_usize = usize::try_from(offset).unwrap_or(usize::MAX);
                if b.len() < required_len {
                    b.resize(required_len, 0);
                }
                b[offset_usize..offset_usize + data.len()].copy_from_slice(data);
                Ok(())
            }
            HandleBody::Spilled { file, len, .. } => {
                if required_end > *len {
                    *len = required_end;
                    file.set_len(*len).await?;
                }
                file.seek(std::io::SeekFrom::Start(offset)).await?;
                file.write_all(data).await?;
                Ok(())
            }
        }
    }

    async fn spill_memory_to_disk(mem: BytesMut) -> std::io::Result<HandleBody> {
        let mut tmp = NamedTempFile::new()?;
        std::io::Write::write_all(&mut tmp, mem.as_ref())?;
        let path = tmp.path().to_path_buf();
        let std_file = tmp.into_file();
        let len = u64::try_from(mem.len()).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "content length overflow")
        })?;
        std_file.set_len(len)?;
        let mut tokio_file = File::from_std(std_file);
        tokio_file.seek(std::io::SeekFrom::Start(0)).await?;
        Ok(HandleBody::Spilled {
            file: tokio_file,
            len,
            path,
        })
    }

    pub(crate) async fn truncate(&mut self, size: u64, memory_limit: usize) -> std::io::Result<()> {
        let size_usize = usize::try_from(size).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "truncate size overflow")
        })?;
        match self {
            HandleBody::Memory(b) => {
                if size_usize > memory_limit {
                    let mem = std::mem::take(b);
                    *self = Self::spill_memory_to_disk(mem).await?;
                    return Box::pin(self.truncate(size, memory_limit)).await;
                }
                b.resize(size_usize, 0);
                Ok(())
            }
            HandleBody::Spilled { file, len, .. } => {
                *len = size;
                file.set_len(size).await?;
                file.seek(std::io::SeekFrom::Start(0)).await?;
                Ok(())
            }
        }
    }

    /// Stream full content to `dest` (used for atomic content commit). Does not consume the handle.
    pub(crate) async fn stream_to_writer(
        &mut self,
        dest: &mut File,
        chunk: usize,
    ) -> std::io::Result<u64> {
        let chunk = chunk.max(8192);
        match self {
            HandleBody::Memory(b) => {
                dest.write_all(b).await?;
                dest.sync_all().await?;
                Ok(b.len() as u64)
            }
            HandleBody::Spilled { file, len, .. } => {
                file.seek(std::io::SeekFrom::Start(0)).await?;
                let mut remaining = *len;
                let mut buf = vec![0u8; chunk.min(usize::try_from(remaining).unwrap_or(chunk))];
                while remaining > 0 {
                    let n = std::cmp::min(buf.len() as u64, remaining) as usize;
                    file.read_exact(&mut buf[..n]).await?;
                    dest.write_all(&buf[..n]).await?;
                    remaining -= n as u64;
                }
                file.seek(std::io::SeekFrom::Start(0)).await?;
                dest.sync_all().await?;
                Ok(*len)
            }
        }
    }

    /// Reload handle bytes from a committed on-disk file (after flush), spilling when needed.
    pub(crate) async fn from_committed_file(
        path: &Path,
        memory_limit: usize,
    ) -> std::io::Result<Self> {
        let meta = tokio::fs::metadata(path).await?;
        let len_u64 = meta.len();
        let len = usize::try_from(len_u64).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "file size overflow")
        })?;
        if len <= memory_limit {
            let data = tokio::fs::read(path).await?;
            let mut m = BytesMut::with_capacity(data.len());
            m.extend_from_slice(&data);
            return Ok(HandleBody::Memory(m));
        }
        let tmp = NamedTempFile::new()?;
        tokio::fs::copy(path, tmp.path()).await?;
        let spill_path = tmp.path().to_path_buf();
        let std_file = tmp.into_file();
        std_file.set_len(len_u64)?;
        Ok(HandleBody::Spilled {
            file: File::from_std(std_file),
            len: len_u64,
            path: spill_path,
        })
    }
}

/// Snapshot for idle-timeout flush: spilled handles re-open the tempfile path.
#[derive(Debug, Clone)]
pub(crate) struct HandleBodySnapshot {
    pub(crate) memory: Option<Vec<u8>>,
    pub(crate) spill_path: Option<PathBuf>,
    pub(crate) spill_len: u64,
}

impl HandleBodySnapshot {
    pub(crate) fn from_body(body: &HandleBody) -> Self {
        match body {
            HandleBody::Memory(b) => Self {
                memory: Some(b.to_vec()),
                spill_path: None,
                spill_len: 0,
            },
            HandleBody::Spilled { path, len, .. } => Self {
                memory: None,
                spill_path: Some(path.clone()),
                spill_len: *len,
            },
        }
    }
}
