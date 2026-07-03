//! Keyhive filesystem storage for BigRepo.
//!
//! Adapted from `subduction_cli/src/keyhive.rs`.
//! Original license: Apache-2.0/MIT.

use std::convert::Infallible;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::{future::BoxFuture, FutureExt};
use subduction_keyhive::storage::{KeyhiveStorage, MemoryKeyhiveStorage, StorageHash};

/// Subdirectory of the repo data dir holding keyhive state.
pub(crate) const KEYHIVE_SUBDIR: &str = "keyhive";

const ARCHIVES_SUBDIR: &str = "archives";
const OPS_SUBDIR: &str = "ops";
const PREKEY_SECRETS_FILE: &str = "prekey-secrets.bin";
const TMP_SUBDIR: &str = "tmp";

/// Monotonic per-process counter for temp filenames.
static NEXT_TMP_ID: AtomicU64 = AtomicU64::new(0);

/// Filesystem-backed [`KeyhiveStorage`] for BigRepo.
#[derive(Debug, Clone)]
pub(crate) struct FsKeyhiveStorage {
    root: PathBuf,
}

/// Error type returned by [`FsKeyhiveStorage`] operations.
#[derive(Debug, thiserror::Error)]
pub(crate) enum FsKeyhiveStorageError {
    /// Underlying filesystem I/O failed.
    #[error("keyhive fs storage io error: {0}")]
    Io(#[from] io::Error),
}

impl FsKeyhiveStorage {
    /// Create the storage root, its `archives/` and `ops/` subdirs.
    pub(crate) fn new(root: PathBuf) -> io::Result<Self> {
        std::fs::create_dir_all(root.join(ARCHIVES_SUBDIR))?;
        std::fs::create_dir_all(root.join(OPS_SUBDIR))?;
        std::fs::create_dir_all(root.join(TMP_SUBDIR))?;
        Ok(Self { root })
    }

    fn archive_dir(&self) -> PathBuf {
        self.root.join(ARCHIVES_SUBDIR)
    }

    fn event_dir(&self) -> PathBuf {
        self.root.join(OPS_SUBDIR)
    }

    fn tmp_dir(&self) -> PathBuf {
        self.root.join(TMP_SUBDIR)
    }

    async fn save_prekey_secrets(&self, bytes: Vec<u8>) -> io::Result<()> {
        let tmp_id = NEXT_TMP_ID.fetch_add(1, Ordering::Relaxed);
        let tmp = self.tmp_dir().join(format!(
            "{PREKEY_SECRETS_FILE}.{}.{tmp_id}.tmp",
            std::process::id()
        ));
        let dest = self.root.join(PREKEY_SECRETS_FILE);
        tokio::fs::write(&tmp, bytes).await?;
        match tokio::fs::rename(&tmp, &dest).await {
            Ok(()) => Ok(()),
            Err(err) => {
                drop(tokio::fs::remove_file(&tmp).await);
                Err(err)
            }
        }
    }

    async fn load_prekey_secrets(&self) -> io::Result<Option<Vec<u8>>> {
        let path = self.root.join(PREKEY_SECRETS_FILE);
        match tokio::fs::read(path).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err),
        }
    }

    async fn save_file(
        &self,
        parent_dir: PathBuf,
        hash: StorageHash,
        data: Vec<u8>,
    ) -> io::Result<()> {
        let filename = format!("{}.bin", hash.to_hex());
        let dest = parent_dir.join(&filename);

        let tmp_id = NEXT_TMP_ID.fetch_add(1, Ordering::Relaxed);
        let tmp = self.tmp_dir().join(format!(
            "{}.{}.{tmp_id}.tmp",
            hash.to_hex(),
            std::process::id()
        ));

        tokio::fs::write(&tmp, data).await?;
        match tokio::fs::rename(&tmp, &dest).await {
            Ok(()) => Ok(()),
            Err(e) => {
                drop(tokio::fs::remove_file(&tmp).await);
                if tokio::fs::try_exists(&dest).await.unwrap_or(false) {
                    Ok(())
                } else {
                    Err(e)
                }
            }
        }
    }

    async fn load_dir(dir: PathBuf) -> io::Result<Vec<(StorageHash, Vec<u8>)>> {
        use tokio::fs;
        let mut out = Vec::new();
        let mut rd = fs::read_dir(&dir).await?;
        while let Some(entry) = rd.next_entry().await? {
            let path = entry.path();
            let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
                continue;
            };
            let Some(hash) = StorageHash::from_hex(stem) else {
                continue;
            };
            let bytes = fs::read(&path).await?;
            out.push((hash, bytes));
        }
        Ok(out)
    }

    async fn delete_file(parent_dir: PathBuf, hash: StorageHash) -> io::Result<()> {
        let path = parent_dir.join(format!("{}.bin", hash.to_hex()));
        match tokio::fs::remove_file(path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }
}

impl KeyhiveStorage<future_form::Sendable> for FsKeyhiveStorage {
    type Error = FsKeyhiveStorageError;

    fn save_archive(
        &self,
        hash: StorageHash,
        data: Vec<u8>,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        let parent_dir = self.archive_dir();
        async move {
            self.save_file(parent_dir, hash, data)
                .await
                .map_err(Into::into)
        }
        .boxed()
    }

    fn load_archives(&self) -> BoxFuture<'_, Result<Vec<(StorageHash, Vec<u8>)>, Self::Error>> {
        let dir = self.archive_dir();
        async move { Self::load_dir(dir).await.map_err(Into::into) }.boxed()
    }

    fn delete_archive(&self, hash: StorageHash) -> BoxFuture<'_, Result<(), Self::Error>> {
        let dir = self.archive_dir();
        async move { Self::delete_file(dir, hash).await.map_err(Into::into) }.boxed()
    }

    fn save_event(
        &self,
        hash: StorageHash,
        data: Vec<u8>,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        let parent_dir = self.event_dir();
        async move {
            self.save_file(parent_dir, hash, data)
                .await
                .map_err(Into::into)
        }
        .boxed()
    }

    fn load_events(&self) -> BoxFuture<'_, Result<Vec<(StorageHash, Vec<u8>)>, Self::Error>> {
        let dir = self.event_dir();
        async move { Self::load_dir(dir).await.map_err(Into::into) }.boxed()
    }

    fn delete_event(&self, hash: StorageHash) -> BoxFuture<'_, Result<(), Self::Error>> {
        let dir = self.event_dir();
        async move { Self::delete_file(dir, hash).await.map_err(Into::into) }.boxed()
    }
}

/// Keyhive storage backend selected by the BigRepo storage mode.
#[derive(Debug, Clone)]
pub(crate) enum BigRepoKeyhiveStorage {
    Memory(MemoryKeyhiveStorage),
    Fs(FsKeyhiveStorage),
}

/// Error type returned by [`BigRepoKeyhiveStorage`] operations.
#[derive(Debug, thiserror::Error)]
pub(crate) enum BigRepoKeyhiveStorageError {
    #[error("memory keyhive storage failed: {0}")]
    Memory(#[from] Infallible),
    #[error(transparent)]
    Fs(#[from] FsKeyhiveStorageError),
}

impl BigRepoKeyhiveStorage {
    pub(crate) fn memory() -> Self {
        Self::Memory(MemoryKeyhiveStorage::new())
    }

    pub(crate) fn fs(root: PathBuf) -> io::Result<Self> {
        FsKeyhiveStorage::new(root).map(Self::Fs)
    }

    pub(crate) async fn save_prekey_secrets(&self, bytes: Vec<u8>) -> io::Result<()> {
        match self {
            Self::Memory(_) => Ok(()),
            Self::Fs(storage) => storage.save_prekey_secrets(bytes).await,
        }
    }

    pub(crate) async fn load_prekey_secrets(&self) -> io::Result<Option<Vec<u8>>> {
        match self {
            Self::Memory(_) => Ok(None),
            Self::Fs(storage) => storage.load_prekey_secrets().await,
        }
    }
}

impl KeyhiveStorage<future_form::Sendable> for BigRepoKeyhiveStorage {
    type Error = BigRepoKeyhiveStorageError;

    fn save_archive(
        &self,
        hash: StorageHash,
        data: Vec<u8>,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            match self {
                Self::Memory(storage) => <MemoryKeyhiveStorage as KeyhiveStorage<
                    future_form::Sendable,
                >>::save_archive(storage, hash, data)
                .await
                .map_err(Into::into),
                Self::Fs(storage) => storage.save_archive(hash, data).await.map_err(Into::into),
            }
        }
        .boxed()
    }

    fn load_archives(&self) -> BoxFuture<'_, Result<Vec<(StorageHash, Vec<u8>)>, Self::Error>> {
        async move {
            match self {
                Self::Memory(storage) => <MemoryKeyhiveStorage as KeyhiveStorage<
                    future_form::Sendable,
                >>::load_archives(storage)
                .await
                .map_err(Into::into),
                Self::Fs(storage) => storage.load_archives().await.map_err(Into::into),
            }
        }
        .boxed()
    }

    fn delete_archive(&self, hash: StorageHash) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            match self {
                Self::Memory(storage) => <MemoryKeyhiveStorage as KeyhiveStorage<
                    future_form::Sendable,
                >>::delete_archive(storage, hash)
                .await
                .map_err(Into::into),
                Self::Fs(storage) => storage.delete_archive(hash).await.map_err(Into::into),
            }
        }
        .boxed()
    }

    fn save_event(
        &self,
        hash: StorageHash,
        data: Vec<u8>,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            match self {
                Self::Memory(storage) => <MemoryKeyhiveStorage as KeyhiveStorage<
                    future_form::Sendable,
                >>::save_event(storage, hash, data)
                .await
                .map_err(Into::into),
                Self::Fs(storage) => storage.save_event(hash, data).await.map_err(Into::into),
            }
        }
        .boxed()
    }

    fn load_events(&self) -> BoxFuture<'_, Result<Vec<(StorageHash, Vec<u8>)>, Self::Error>> {
        async move {
            match self {
                Self::Memory(storage) => <MemoryKeyhiveStorage as KeyhiveStorage<
                    future_form::Sendable,
                >>::load_events(storage)
                .await
                .map_err(Into::into),
                Self::Fs(storage) => storage.load_events().await.map_err(Into::into),
            }
        }
        .boxed()
    }

    fn delete_event(&self, hash: StorageHash) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            match self {
                Self::Memory(storage) => <MemoryKeyhiveStorage as KeyhiveStorage<
                    future_form::Sendable,
                >>::delete_event(storage, hash)
                .await
                .map_err(Into::into),
                Self::Fs(storage) => storage.delete_event(hash).await.map_err(Into::into),
            }
        }
        .boxed()
    }
}
