use std::path::{Path, PathBuf};

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("workspace id cannot be empty")]
    EmptyWorkspace,
}

pub type Result<T> = std::result::Result<T, StorageError>;

pub trait ContentStore: Send + Sync {
    fn workspace_root(&self, workspace_id: &str) -> Result<PathBuf>;
    fn supports_fanotify(&self) -> bool;
}

#[derive(Debug, Clone)]
pub struct LocalFsContentStore {
    root: PathBuf,
}

impl LocalFsContentStore {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }
}

impl ContentStore for LocalFsContentStore {
    fn workspace_root(&self, workspace_id: &str) -> Result<PathBuf> {
        if workspace_id.trim().is_empty() {
            return Err(StorageError::EmptyWorkspace);
        }

        Ok(self.root.join(workspace_id))
    }

    fn supports_fanotify(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone)]
pub struct JuiceFsContentStore {
    mount_root: PathBuf,
}

impl JuiceFsContentStore {
    pub fn new(mount_root: impl AsRef<Path>) -> Self {
        Self {
            mount_root: mount_root.as_ref().to_path_buf(),
        }
    }
}

impl ContentStore for JuiceFsContentStore {
    fn workspace_root(&self, workspace_id: &str) -> Result<PathBuf> {
        if workspace_id.trim().is_empty() {
            return Err(StorageError::EmptyWorkspace);
        }

        Ok(self.mount_root.join(workspace_id))
    }

    fn supports_fanotify(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::{ContentStore, LocalFsContentStore};

    #[test]
    fn local_store_builds_workspace_path() {
        let store = LocalFsContentStore::new("/tmp/content");
        let path = store.workspace_root("ws-1").expect("path");
        assert_eq!(path.to_string_lossy(), "/tmp/content/ws-1");
    }
}
