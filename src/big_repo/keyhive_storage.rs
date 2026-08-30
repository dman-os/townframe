//! Keyhive filesystem storage for BigRepo.
//!
//! Adapted from `subduction_cli/src/keyhive.rs`.
//! Original license: Apache-2.0/MIT. (c) 2024 Ink & Switch

// FIXME: KeyhiveStorage requires loading all archives at once instead of
// by id which is wasteful

use crate::interlude::*;

use crate::store::sqlite::SqliteBigRepoStore;
use std::collections::HashMap;
use std::convert::Infallible;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::lock::Mutex;

use futures::{FutureExt, future::BoxFuture};
use subduction_keyhive::storage::{KeyhiveStorage, MemoryKeyhiveStorage, StorageHash};

/// Subdirectory of the repo data dir holding keyhive state.
pub(crate) const KEYHIVE_SUBDIR: &str = "keyhive";

const ARCHIVES_SUBDIR: &str = "archives";
const OPS_SUBDIR: &str = "ops";
const LOCAL_SECRETS_SUBDIR: &str = "local-secrets";
const PREKEY_SECRETS_FILE: &str = "prekey-secrets.bin";
const RESERVATIONS_SUBDIR: &str = "reservations";
const TMP_SUBDIR: &str = "tmp";

/// Magic bytes prefixing a [`DocReservation`] blob in local-secret storage.
pub(crate) const DOC_RESERVATION_MAGIC: [u8; 4] = *b"DRSV";

/// A durably reserved document identity: the ephemeral signing key whose
/// verifying key is the eventual document ID, plus the parent authorities the
/// document will be created under at finalization.
///
/// Stored in Keyhive's local-secret storage (never synchronized); it is the
/// crash-recovery record between ID allocation and Keyhive document creation.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct DocReservation {
    pub magic: [u8; 4],
    pub doc_id: [u8; 32],
    pub signing_key: [u8; 32],
    pub parents: Vec<[u8; 32]>,
    /// Keys for causal parents inherited from another document, if any.
    pub initial_keys: Vec<(Vec<u8>, [u8; 32])>,
    /// Serialized initial Automerge content, staged before Keyhive creation.
    /// None means an identity has been reserved but content creation has not
    /// started yet.
    pub initial_content: Option<Vec<u8>>,
}

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
        let secrets_dir = root.join(LOCAL_SECRETS_SUBDIR);
        std::fs::create_dir_all(&secrets_dir)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&secrets_dir, std::fs::Permissions::from_mode(0o700))?;
        }
        let reservations_dir = root.join(RESERVATIONS_SUBDIR);
        std::fs::create_dir_all(&reservations_dir)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&reservations_dir, std::fs::Permissions::from_mode(0o700))?;
        }
        std::fs::create_dir_all(root.join(TMP_SUBDIR))?;
        Ok(Self { root })
    }

    fn archive_dir(&self) -> PathBuf {
        self.root.join(ARCHIVES_SUBDIR)
    }

    fn event_dir(&self) -> PathBuf {
        self.root.join(OPS_SUBDIR)
    }

    fn local_secret_dir(&self) -> PathBuf {
        self.root.join(LOCAL_SECRETS_SUBDIR)
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
        use tokio::io::AsyncWriteExt;
        let mut file = tokio::fs::File::create(&tmp).await?;
        file.write_all(&bytes).await?;
        file.sync_all().await?;
        drop(file);
        match tokio::fs::rename(&tmp, &dest).await {
            Ok(()) => {
                if let Err(err) = Self::sync_dir(&self.root) {
                    tracing::warn!(error = %err, "failed to fsync keyhive storage root after prekey secrets rename");
                }
                Ok(())
            }
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

    fn reservation_path(&self, doc_id: [u8; 32]) -> PathBuf {
        let mut hex = String::with_capacity(64);
        for byte in doc_id {
            use std::fmt::Write;
            write!(hex, "{byte:02x}").expect("writing hex to String cannot fail");
        }
        self.root
            .join(RESERVATIONS_SUBDIR)
            .join(format!("{hex}.bin"))
    }

    async fn save_doc_reservation(&self, doc_id: [u8; 32], bytes: Vec<u8>) -> io::Result<()> {
        let tmp_id = NEXT_TMP_ID.fetch_add(1, Ordering::Relaxed);
        let tmp = self
            .tmp_dir()
            .join(format!("reservation.{}.{tmp_id}.tmp", std::process::id()));
        let dest = self.reservation_path(doc_id);
        use tokio::io::AsyncWriteExt;
        let mut file = tokio::fs::File::create(&tmp).await?;
        file.write_all(&bytes).await?;
        file.sync_all().await?;
        drop(file);
        match tokio::fs::rename(&tmp, &dest).await {
            Ok(()) => {
                let parent = dest.parent().expect("reservation parent").to_owned();
                tokio::task::spawn_blocking(move || std::fs::File::open(parent)?.sync_all())
                    .await
                    .map_err(io::Error::other)??;
                Ok(())
            }
            Err(err) => {
                drop(tokio::fs::remove_file(&tmp).await);
                Err(err)
            }
        }
    }

    async fn load_doc_reservation(&self, doc_id: [u8; 32]) -> io::Result<Option<Vec<u8>>> {
        match tokio::fs::read(self.reservation_path(doc_id)).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err),
        }
    }

    async fn list_doc_reservations(&self) -> io::Result<Vec<Vec<u8>>> {
        let dir = self.root.join(RESERVATIONS_SUBDIR);
        let mut out = Vec::new();
        let mut rd = tokio::fs::read_dir(&dir).await?;
        while let Some(entry) = rd.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("bin") {
                continue;
            }
            out.push(tokio::fs::read(&path).await?);
        }
        Ok(out)
    }

    async fn delete_doc_reservation(&self, doc_id: [u8; 32]) -> io::Result<()> {
        match tokio::fs::remove_file(self.reservation_path(doc_id)).await {
            Ok(()) => {
                let parent = self.root.join(RESERVATIONS_SUBDIR);
                tokio::task::spawn_blocking(move || std::fs::File::open(parent)?.sync_all())
                    .await
                    .map_err(io::Error::other)??;
                Ok(())
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
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

        use tokio::io::AsyncWriteExt;
        let mut file = tokio::fs::File::create(&tmp).await?;
        file.write_all(&data).await?;
        file.sync_all().await?;
        drop(file);
        match tokio::fs::rename(&tmp, &dest).await {
            Ok(()) => {
                if let Err(err) = Self::sync_dir(&parent_dir) {
                    tracing::warn!(?parent_dir, error = %err, "failed to fsync keyhive storage dir after rename");
                }
                Ok(())
            }
            Err(err) => {
                drop(tokio::fs::remove_file(&tmp).await);
                if tokio::fs::try_exists(&dest).await.unwrap_or(false) {
                    Ok(())
                } else {
                    Err(err)
                }
            }
        }
    }

    async fn save_file_if_absent(
        &self,
        parent_dir: PathBuf,
        hash: StorageHash,
        data: Vec<u8>,
    ) -> io::Result<bool> {
        let dest = parent_dir.join(format!("{}.bin", hash.to_hex()));
        let tmp_id = NEXT_TMP_ID.fetch_add(1, Ordering::Relaxed);
        let tmp = self.tmp_dir().join(format!(
            "{}.{}.{tmp_id}.tmp",
            hash.to_hex(),
            std::process::id()
        ));
        use tokio::io::AsyncWriteExt;
        let mut file = tokio::fs::File::create(&tmp).await?;
        file.write_all(&data).await?;
        file.sync_all().await?;
        drop(file);
        #[cfg(unix)]
        {
            if parent_dir.ends_with(LOCAL_SECRETS_SUBDIR) || parent_dir == self.local_secret_dir() {
                use std::os::unix::fs::PermissionsExt;
                drop(std::fs::set_permissions(
                    &tmp,
                    std::fs::Permissions::from_mode(0o600),
                ));
            }
        }
        let result = match tokio::fs::hard_link(&tmp, &dest).await {
            Ok(()) => {
                if let Err(err) = Self::sync_dir(&parent_dir) {
                    tracing::warn!(?parent_dir, error = %err, "failed to fsync keyhive storage dir after hard link");
                }
                Ok(true)
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(false),
            Err(error) => Err(error),
        };
        drop(tokio::fs::remove_file(&tmp).await);
        result
    }

    /// Flush a directory's entries to disk. On platforms where directory
    /// fsync is unsupported this is a no-op; failures are reported to the
    /// caller, which decides whether they are fatal for the write.
    fn sync_dir(path: &Path) -> io::Result<()> {
        let file = std::fs::File::open(path)?;
        match file.sync_all() {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == io::ErrorKind::Unsupported => Ok(()),
            Err(err) => Err(err),
        }
    }

    async fn load_dir(dir: PathBuf) -> io::Result<Vec<(StorageHash, Vec<u8>)>> {
        use tokio::fs;
        let mut out = Vec::new();
        let mut rd = fs::read_dir(&dir).await?;
        while let Some(entry) = rd.next_entry().await? {
            let path = entry.path();
            let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
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
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err),
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
    ) -> BoxFuture<'_, Result<bool, Self::Error>> {
        let parent_dir = self.event_dir();
        async move {
            self.save_file_if_absent(parent_dir, hash, data)
                .await
                .map_err(Into::into)
        }
        .boxed()
    }

    fn load_events(&self) -> BoxFuture<'_, Result<Vec<(StorageHash, Vec<u8>)>, Self::Error>> {
        let dir = self.event_dir();
        async move { Self::load_dir(dir).await.map_err(Into::into) }.boxed()
    }

    fn load_events_with_source(
        &self,
    ) -> BoxFuture<
        '_,
        Result<
            Vec<(
                StorageHash,
                Vec<u8>,
                Option<subduction_keyhive::KeyhivePeerId>,
            )>,
            Self::Error,
        >,
    > {
        let dir = self.event_dir();
        async move {
            Ok(Self::load_dir(dir)
                .await?
                .into_iter()
                .map(|(hash, bytes)| (hash, bytes, None))
                .collect())
        }
        .boxed()
    }

    fn delete_event(&self, hash: StorageHash) -> BoxFuture<'_, Result<(), Self::Error>> {
        let dir = self.event_dir();
        async move { Self::delete_file(dir, hash).await.map_err(Into::into) }.boxed()
    }

    fn save_local_secret(
        &self,
        hash: StorageHash,
        data: Vec<u8>,
    ) -> BoxFuture<'_, Result<bool, Self::Error>> {
        let parent_dir = self.local_secret_dir();
        async move {
            self.save_file_if_absent(parent_dir, hash, data)
                .await
                .map_err(Into::into)
        }
        .boxed()
    }

    fn load_local_secrets(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(StorageHash, Vec<u8>)>, Self::Error>> {
        let dir = self.local_secret_dir();
        async move { Self::load_dir(dir).await.map_err(Into::into) }.boxed()
    }

    fn delete_local_secret(&self, hash: StorageHash) -> BoxFuture<'_, Result<(), Self::Error>> {
        let dir = self.local_secret_dir();
        async move { Self::delete_file(dir, hash).await.map_err(Into::into) }.boxed()
    }
}

/// Keyhive storage backend selected by the BigRepo storage mode.
#[derive(Debug, Clone)]
enum BigRepoKeyhiveStorageInner {
    Memory(MemoryKeyhiveStorage),
    Sqlite {
        events: SqliteBigRepoStore,
        archives: MemoryKeyhiveStorage,
    },
    Fs {
        events: SqliteBigRepoStore,
        archives: FsKeyhiveStorage,
    },
}

/// Keyhive storage backed by the raw event log and archive storage.
#[derive(Clone)]
pub(crate) struct BigRepoKeyhiveStorage {
    inner: BigRepoKeyhiveStorageInner,
    /// In-memory document-id reservations for non-filesystem backends. The
    /// filesystem backend keeps reservations in its own `reservations/` dir.
    reservations: Arc<Mutex<HashMap<[u8; 32], Vec<u8>>>>,
}

impl std::fmt::Debug for BigRepoKeyhiveStorage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(formatter)
    }
}

/// Error type returned by [`BigRepoKeyhiveStorage`] operations.
#[derive(Debug, thiserror::Error)]
pub(crate) enum BigRepoKeyhiveStorageError {
    #[error("memory keyhive storage failed: {0}")]
    Memory(#[from] Infallible),
    #[error(transparent)]
    Fs(#[from] FsKeyhiveStorageError),
    #[error(transparent)]
    Sqlite(#[from] crate::store::sqlite::SqliteBigRepoStoreError),
}

impl BigRepoKeyhiveStorage {
    fn new(inner: BigRepoKeyhiveStorageInner) -> Self {
        Self {
            inner,
            reservations: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    #[cfg_attr(not(test), expect(dead_code))]
    pub(crate) fn memory() -> Self {
        Self::new(BigRepoKeyhiveStorageInner::Memory(
            MemoryKeyhiveStorage::new(),
        ))
    }

    pub(crate) fn memory_sqlite(events: SqliteBigRepoStore) -> Self {
        Self::new(BigRepoKeyhiveStorageInner::Sqlite {
            events,
            archives: MemoryKeyhiveStorage::new(),
        })
    }

    pub(crate) fn fs(events: SqliteBigRepoStore, root: PathBuf) -> io::Result<Self> {
        FsKeyhiveStorage::new(root)
            .map(|archives| Self::new(BigRepoKeyhiveStorageInner::Fs { events, archives }))
    }

    pub(crate) async fn save_prekey_secrets(&self, bytes: Vec<u8>) -> io::Result<()> {
        match &self.inner {
            BigRepoKeyhiveStorageInner::Memory(_) | BigRepoKeyhiveStorageInner::Sqlite { .. } => {
                Ok(())
            }
            BigRepoKeyhiveStorageInner::Fs { archives, .. } => {
                archives.save_prekey_secrets(bytes).await
            }
        }
    }

    pub(crate) async fn load_prekey_secrets(&self) -> io::Result<Option<Vec<u8>>> {
        match &self.inner {
            BigRepoKeyhiveStorageInner::Memory(_) | BigRepoKeyhiveStorageInner::Sqlite { .. } => {
                Ok(None)
            }
            BigRepoKeyhiveStorageInner::Fs { archives, .. } => archives.load_prekey_secrets().await,
        }
    }

    pub(crate) async fn save_doc_reservation(
        &self,
        reservation: &DocReservation,
    ) -> io::Result<()> {
        let bytes = bincode::serialize(reservation).map_err(io::Error::other)?;
        match &self.inner {
            BigRepoKeyhiveStorageInner::Fs { archives, .. } => {
                archives
                    .save_doc_reservation(reservation.doc_id, bytes)
                    .await
            }
            _ => {
                self.reservations
                    .lock()
                    .await
                    .insert(reservation.doc_id, bytes);
                Ok(())
            }
        }
    }

    pub(crate) async fn load_doc_reservation(
        &self,
        doc_id: [u8; 32],
    ) -> io::Result<Option<DocReservation>> {
        let bytes = match &self.inner {
            BigRepoKeyhiveStorageInner::Fs { archives, .. } => {
                archives.load_doc_reservation(doc_id).await?
            }
            _ => self.reservations.lock().await.get(&doc_id).cloned(),
        };
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        let reservation = bincode::deserialize(&bytes).map_err(io::Error::other)?;
        Ok(Some(reservation))
    }

    pub(crate) async fn list_doc_reservations(&self) -> io::Result<Vec<DocReservation>> {
        let entries = match &self.inner {
            BigRepoKeyhiveStorageInner::Fs { archives, .. } => {
                archives.list_doc_reservations().await?
            }
            _ => self
                .reservations
                .lock()
                .await
                .values()
                .cloned()
                .collect::<Vec<_>>(),
        };
        entries
            .into_iter()
            .map(|bytes| bincode::deserialize(&bytes).map_err(io::Error::other))
            .collect()
    }

    pub(crate) async fn stage_doc_reservation(
        &self,
        doc_id: [u8; 32],
        initial_content: Vec<u8>,
        initial_keys: Vec<(Vec<u8>, [u8; 32])>,
    ) -> io::Result<()> {
        let mut reservation = self
            .load_doc_reservation(doc_id)
            .await?
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "document reservation"))?;
        if let Some(existing) = &reservation.initial_content {
            if existing != &initial_content || reservation.initial_keys != initial_keys {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "document reservation has different initial content or keys",
                ));
            }
            return Ok(());
        }
        reservation.initial_content = Some(initial_content);
        reservation.initial_keys = initial_keys;
        self.save_doc_reservation(&reservation).await
    }

    pub(crate) async fn staged_doc_content(
        &self,
        doc_id: [u8; 32],
    ) -> io::Result<Option<(Vec<u8>, Vec<(Vec<u8>, [u8; 32])>)>> {
        Ok(self
            .load_doc_reservation(doc_id)
            .await?
            .and_then(|reservation| {
                reservation
                    .initial_content
                    .map(|content| (content, reservation.initial_keys))
            }))
    }

    pub(crate) async fn delete_doc_reservation(&self, doc_id: [u8; 32]) -> io::Result<()> {
        match &self.inner {
            BigRepoKeyhiveStorageInner::Fs { archives, .. } => {
                archives.delete_doc_reservation(doc_id).await
            }
            _ => {
                self.reservations.lock().await.remove(&doc_id);
                Ok(())
            }
        }
    }
}

impl KeyhiveStorage<future_form::Sendable> for BigRepoKeyhiveStorageInner {
    type Error = BigRepoKeyhiveStorageError;

    fn save_archive(
        &self,
        hash: StorageHash,
        data: Vec<u8>,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            match self {
                Self::Sqlite { archives, .. } => {
                    <MemoryKeyhiveStorage as KeyhiveStorage<future_form::Sendable>>::save_archive(
                        archives, hash, data,
                    )
                    .await
                    .map_err(Into::into)
                }
                Self::Memory(storage) => <MemoryKeyhiveStorage as KeyhiveStorage<
                    future_form::Sendable,
                >>::save_archive(storage, hash, data)
                .await
                .map_err(Into::into),
                Self::Fs { archives, .. } => {
                    archives.save_archive(hash, data).await.map_err(Into::into)
                }
            }
        }
        .boxed()
    }

    fn load_archives(&self) -> BoxFuture<'_, Result<Vec<(StorageHash, Vec<u8>)>, Self::Error>> {
        async move {
            match self {
                Self::Sqlite { archives, .. } => <MemoryKeyhiveStorage as KeyhiveStorage<
                    future_form::Sendable,
                >>::load_archives(archives)
                .await
                .map_err(Into::into),
                Self::Memory(storage) => <MemoryKeyhiveStorage as KeyhiveStorage<
                    future_form::Sendable,
                >>::load_archives(storage)
                .await
                .map_err(Into::into),
                Self::Fs { archives, .. } => archives.load_archives().await.map_err(Into::into),
            }
        }
        .boxed()
    }

    fn delete_archive(&self, hash: StorageHash) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            match self {
                Self::Sqlite { archives, .. } => <MemoryKeyhiveStorage as KeyhiveStorage<
                    future_form::Sendable,
                >>::delete_archive(archives, hash)
                .await
                .map_err(Into::into),
                Self::Memory(storage) => <MemoryKeyhiveStorage as KeyhiveStorage<
                    future_form::Sendable,
                >>::delete_archive(storage, hash)
                .await
                .map_err(Into::into),
                Self::Fs { archives, .. } => {
                    archives.delete_archive(hash).await.map_err(Into::into)
                }
            }
        }
        .boxed()
    }

    fn save_event(
        &self,
        hash: StorageHash,
        data: Vec<u8>,
    ) -> BoxFuture<'_, Result<bool, Self::Error>> {
        self.save_event_with_source(hash, data, None)
    }

    fn save_event_with_source(
        &self,
        hash: StorageHash,
        data: Vec<u8>,
        source: Option<subduction_keyhive::KeyhivePeerId>,
    ) -> BoxFuture<'_, Result<bool, Self::Error>> {
        async move {
            match self {
                Self::Sqlite { events, .. } | Self::Fs { events, .. } => events
                    .save_keyhive_event(hash, data, source)
                    .await
                    .inspect_err(|error| {
                        // TEMP-DIAGNOSTIC: the keyhive protocol wraps this as
                        // `keyhive protocol error: storage error`, discarding the
                        // sqlite cause. Log the full source chain here.
                        warn_loc!(
                            "KEYHIVE_STORAGE_DIAG save_event failed hash={hash:?}: {error:?}"
                        );
                    })
                    .map_err(Into::into),
                Self::Memory(storage) => <MemoryKeyhiveStorage as KeyhiveStorage<
                    future_form::Sendable,
                >>::save_event(storage, hash, data)
                .await
                .map_err(Into::into),
            }
        }
        .boxed()
    }

    fn load_events(&self) -> BoxFuture<'_, Result<Vec<(StorageHash, Vec<u8>)>, Self::Error>> {
        async move {
            match self {
                Self::Sqlite { events, .. } | Self::Fs { events, .. } => {
                    events.load_keyhive_events().await.map_err(Into::into)
                }
                Self::Memory(storage) => <MemoryKeyhiveStorage as KeyhiveStorage<
                    future_form::Sendable,
                >>::load_events(storage)
                .await
                .map_err(Into::into),
            }
        }
        .boxed()
    }

    fn load_events_with_source(
        &self,
    ) -> BoxFuture<
        '_,
        Result<
            Vec<(
                StorageHash,
                Vec<u8>,
                Option<subduction_keyhive::KeyhivePeerId>,
            )>,
            Self::Error,
        >,
    > {
        async move {
            match self {
                Self::Sqlite { events, .. } | Self::Fs { events, .. } => events
                    .load_keyhive_events_with_source()
                    .await
                    .map_err(Into::into),
                Self::Memory(storage) => <MemoryKeyhiveStorage as KeyhiveStorage<
                    future_form::Sendable,
                >>::load_events_with_source(storage)
                .await
                .map_err(Into::into),
            }
        }
        .boxed()
    }

    fn delete_event(&self, hash: StorageHash) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            match self {
                Self::Sqlite { events, .. } | Self::Fs { events, .. } => {
                    events.delete_keyhive_event(hash).await.map_err(Into::into)
                }
                Self::Memory(storage) => <MemoryKeyhiveStorage as KeyhiveStorage<
                    future_form::Sendable,
                >>::delete_event(storage, hash)
                .await
                .map_err(Into::into),
            }
        }
        .boxed()
    }

    fn save_local_secret(
        &self,
        hash: StorageHash,
        data: Vec<u8>,
    ) -> BoxFuture<'_, Result<bool, Self::Error>> {
        async move {
            match self {
                Self::Sqlite { archives, .. } => <MemoryKeyhiveStorage as KeyhiveStorage<
                    future_form::Sendable,
                >>::save_local_secret(
                    archives, hash, data
                )
                .await
                .map_err(Into::into),
                Self::Memory(storage) => <MemoryKeyhiveStorage as KeyhiveStorage<
                    future_form::Sendable,
                >>::save_local_secret(storage, hash, data)
                .await
                .map_err(Into::into),
                Self::Fs { archives, .. } => archives
                    .save_local_secret(hash, data)
                    .await
                    .map_err(Into::into),
            }
        }
        .boxed()
    }

    fn load_local_secrets(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(StorageHash, Vec<u8>)>, Self::Error>> {
        async move {
            match self {
                Self::Sqlite { archives, .. } => <MemoryKeyhiveStorage as KeyhiveStorage<
                    future_form::Sendable,
                >>::load_local_secrets(archives)
                .await
                .map_err(Into::into),
                Self::Memory(storage) => <MemoryKeyhiveStorage as KeyhiveStorage<
                    future_form::Sendable,
                >>::load_local_secrets(storage)
                .await
                .map_err(Into::into),
                Self::Fs { archives, .. } => {
                    archives.load_local_secrets().await.map_err(Into::into)
                }
            }
        }
        .boxed()
    }

    fn delete_local_secret(&self, hash: StorageHash) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            match self {
                Self::Sqlite { archives, .. } => <MemoryKeyhiveStorage as KeyhiveStorage<
                    future_form::Sendable,
                >>::delete_local_secret(
                    archives, hash
                )
                .await
                .map_err(Into::into),
                Self::Memory(storage) => <MemoryKeyhiveStorage as KeyhiveStorage<
                    future_form::Sendable,
                >>::delete_local_secret(storage, hash)
                .await
                .map_err(Into::into),
                Self::Fs { archives, .. } => {
                    archives.delete_local_secret(hash).await.map_err(Into::into)
                }
            }
        }
        .boxed()
    }
}

impl KeyhiveStorage<future_form::Sendable> for BigRepoKeyhiveStorage {
    type Error = BigRepoKeyhiveStorageError;

    fn save_archive(
        &self,
        hash: StorageHash,
        data: Vec<u8>,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        self.inner.save_archive(hash, data)
    }

    fn load_archives(&self) -> BoxFuture<'_, Result<Vec<(StorageHash, Vec<u8>)>, Self::Error>> {
        self.inner.load_archives()
    }

    fn delete_archive(&self, hash: StorageHash) -> BoxFuture<'_, Result<(), Self::Error>> {
        self.inner.delete_archive(hash)
    }

    fn save_event(
        &self,
        hash: StorageHash,
        data: Vec<u8>,
    ) -> BoxFuture<'_, Result<bool, Self::Error>> {
        self.save_event_with_source(hash, data, None)
    }

    fn save_event_with_source(
        &self,
        hash: StorageHash,
        data: Vec<u8>,
        source: Option<subduction_keyhive::KeyhivePeerId>,
    ) -> BoxFuture<'_, Result<bool, Self::Error>> {
        self.inner.save_event_with_source(hash, data, source)
    }

    fn load_events(&self) -> BoxFuture<'_, Result<Vec<(StorageHash, Vec<u8>)>, Self::Error>> {
        self.inner.load_events()
    }

    fn load_events_with_source(
        &self,
    ) -> BoxFuture<
        '_,
        Result<
            Vec<(
                StorageHash,
                Vec<u8>,
                Option<subduction_keyhive::KeyhivePeerId>,
            )>,
            Self::Error,
        >,
    > {
        self.inner.load_events_with_source()
    }

    fn delete_event(&self, hash: StorageHash) -> BoxFuture<'_, Result<(), Self::Error>> {
        self.inner.delete_event(hash)
    }

    fn save_local_secret(
        &self,
        hash: StorageHash,
        data: Vec<u8>,
    ) -> BoxFuture<'_, Result<bool, Self::Error>> {
        self.inner.save_local_secret(hash, data)
    }

    fn load_local_secrets(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(StorageHash, Vec<u8>)>, Self::Error>> {
        self.inner.load_local_secrets()
    }

    fn delete_local_secret(&self, hash: StorageHash) -> BoxFuture<'_, Result<(), Self::Error>> {
        self.inner.delete_local_secret(hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn save_file_if_absent_writes_exact_bytes_and_is_idempotent() -> io::Result<()> {
        let root = std::env::temp_dir().join(format!(
            "bigrepo-keyhive-storage-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or_default()
        ));
        let storage = FsKeyhiveStorage::new(root.clone())?;
        let parent_dir = root.join(LOCAL_SECRETS_SUBDIR);
        let hash = StorageHash::new([42u8; 32]);
        let data = b"secret material".to_vec();

        assert!(storage
            .save_file_if_absent(parent_dir.clone(), hash, data.clone())
            .await?);

        let written = tokio::fs::read(root.join(LOCAL_SECRETS_SUBDIR).join(format!("{}.bin", hash.to_hex()))).await?;
        assert_eq!(written, data);

        // Same hash with different data must be refused and must not clobber.
        assert!(!storage
            .save_file_if_absent(parent_dir.clone(), hash, b"clobber".to_vec())
            .await?);
        let unchanged =
            tokio::fs::read(root.join(LOCAL_SECRETS_SUBDIR).join(format!("{}.bin", hash.to_hex())))
                .await?;
        assert_eq!(unchanged, data);

        // A different hash still writes.
        let other = StorageHash::new([43u8; 32]);
        assert!(storage
            .save_file_if_absent(parent_dir.clone(), other, b"other".to_vec())
            .await?);

        tokio::fs::remove_dir_all(&root).await?;
        Ok(())
    }
}
