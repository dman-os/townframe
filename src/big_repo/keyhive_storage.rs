//! Keyhive filesystem storage for BigRepo.
//!
//! Archives live on disk under the storage root; keyhive events live in
//! sqlite; secret material (CGKA secrets, prekey sidecar, doc reservations)
//! lives in sqlite as well, AEAD-encrypted under per-kind DEKs sourced from
//! the OS keyring (or a file fallback).
//!
//! Adapted from `subduction_cli/src/keyhive.rs`.
//! Original license: Apache-2.0/MIT. (c) 2024 Ink & Switch

// FIXME: KeyhiveStorage requires loading all archives at once instead of
// by id which is wasteful

use crate::interlude::*;

use crate::store::sqlite::{SecretBlobKind, SqliteBigRepoStore, SqliteBigRepoStoreError};
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::lock::Mutex;

use futures::{FutureExt, future::BoxFuture};
use rand::RngCore;
use secrets_rs::{SecretRepo, decrypt_blob, encrypt_blob};
use subduction_keyhive::storage::{KeyhiveStorage, MemoryKeyhiveStorage, StorageHash};
use utils_rs::prelude::eyre;

/// Subdirectory of the repo data dir holding keyhive state.
pub(crate) const KEYHIVE_SUBDIR: &str = "keyhive";

const ARCHIVES_SUBDIR: &str = "archives";
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
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
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

/// Reserved blob id under which the prekey-secrets sidecar is stored.
const PREKEY_SECRETS_BLOB_ID: &str = "prekey-secrets.v1";
/// Subdirectory holding file-fallback DEKs (`<dek_id>.v<version>.bin`, 0o600).
const DEKS_SUBDIR: &str = "deks";
/// DEK family ids: one per material kind, so materials can be re-encrypted
/// (rotated) independently. Recorded per blob and in `big_repo_deks`.
const DEK_ID_LOCAL_SECRET: &str = "local-secret";
const DEK_ID_PREKEY_SIDECAR: &str = "prekey-sidecar";
const DEK_ID_RESERVATION: &str = "reservation";
/// `big_repo_deks.algorithm` labels for the desktop DEK sources.
const DEK_ALGORITHM_KEYRING: &str = "keyring";
const DEK_ALGORITHM_FILE: &str = "file";
/// Monotonic per-process counter for temp filenames.
static NEXT_TMP_ID: AtomicU64 = AtomicU64::new(0);

/// Where the DEK lives for a filesystem-backed storage. Secret material
/// itself always persists to sqlite (encrypted under the DEK); this only
/// sources the key bytes.
#[derive(Clone)]
enum SecretMaterial {
    /// File-backed DEK fallback (`deks/<dek_id>.v<version>.bin`, 0o600) for
    /// headless systems without an OS keyring.
    FileDek,
    /// OS keyring DEK via the shared `secrets_rs` repo.
    Keyring { repo: Arc<SecretRepo> },
}

impl std::fmt::Debug for SecretMaterial {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FileDek => write!(formatter, "FileDek"),
            Self::Keyring { .. } => write!(formatter, "Keyring"),
        }
    }
}

/// Error type returned by [`FsKeyhiveStorage`] operations.
#[derive(Debug, thiserror::Error)]
pub(crate) enum FsKeyhiveStorageError {
    /// Underlying filesystem I/O failed.
    #[error("keyhive fs storage io error: {0}")]
    Io(#[from] io::Error),
    #[error("keyhive secret store error: {0}")]
    Secrets(#[from] secrets_rs::SecretsError),
    #[error("keyhive sqlite secret storage error: {0}")]
    Sqlite(#[from] crate::store::sqlite::SqliteBigRepoStoreError),
    #[error("corrupt keyhive secret record: {0}")]
    Corrupt(String),
}

/// Filesystem-backed [`KeyhiveStorage`] for BigRepo.
///
/// Archives live on disk; secret material (CGKA secrets, prekey sidecar,
/// reservations) lives in sqlite, encrypted under per-kind DEKs sourced from
/// [`SecretMaterial`].
#[derive(Clone)]
pub(crate) struct FsKeyhiveStorage {
    root: PathBuf,
    store: SqliteBigRepoStore,
    secret_material: SecretMaterial,
}

impl std::fmt::Debug for FsKeyhiveStorage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FsKeyhiveStorage")
            .field("root", &self.root)
            .field("secret_material", &self.secret_material)
            .finish()
    }
}

impl FsKeyhiveStorage {
    /// Create the storage root and its subdirs. Secret material persists to
    /// sqlite encrypted under a file-fallback DEK (no OS keyring required).
    pub(crate) fn new(root: PathBuf, store: SqliteBigRepoStore) -> io::Result<Self> {
        Self::new_with_secret_material(root, store, SecretMaterial::FileDek)
    }

    /// Same layout as [`FsKeyhiveStorage::new`], but the DEK is sourced from
    /// the OS keyring via the shared `secrets_rs` repo. Secret blobs still
    /// persist to sqlite (encrypted); the keyring never holds blobs.
    pub(crate) fn with_secret_repo(
        root: PathBuf,
        store: SqliteBigRepoStore,
        repo: Arc<SecretRepo>,
    ) -> io::Result<Self> {
        Self::new_with_secret_material(root, store, SecretMaterial::Keyring { repo })
    }

    /// Whether the DEK is sourced from the OS keyring.
    fn uses_keyring_secrets(&self) -> bool {
        matches!(self.secret_material, SecretMaterial::Keyring { .. })
    }

    fn new_with_secret_material(
        root: PathBuf,
        store: SqliteBigRepoStore,
        secret_material: SecretMaterial,
    ) -> io::Result<Self> {
        std::fs::create_dir_all(root.join(ARCHIVES_SUBDIR))?;
        // The legacy filesystem layout is still created so the one-time
        // import (import_legacy_secrets_if_empty) can read it; new writes
        // never touch these dirs.
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
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(
                root.join(TMP_SUBDIR),
                std::fs::Permissions::from_mode(0o700),
            )?;
        }
        Ok(Self {
            root,
            store,
            secret_material,
        })
    }

    fn archive_dir(&self) -> PathBuf {
        self.root.join(ARCHIVES_SUBDIR)
    }

    /// Legacy `ops/` dir: the keyhive event log lives in sqlite today, so
    /// this directory is never created or written. Kept only because the
    /// [`KeyhiveStorage`] trait still requires the event methods.
    fn event_dir(&self) -> PathBuf {
        self.root.join("ops")
    }

    fn local_secret_dir(&self) -> PathBuf {
        self.root.join(LOCAL_SECRETS_SUBDIR)
    }

    fn tmp_dir(&self) -> PathBuf {
        self.root.join(TMP_SUBDIR)
    }

    async fn save_prekey_secrets(&self, bytes: Vec<u8>) -> io::Result<()> {
        self.write_secret_blob(
            SecretBlobKind::PrekeySidecar,
            PREKEY_SECRETS_BLOB_ID.as_bytes(),
            &bytes,
        )
        .await
        .map_err(|err| io::Error::other(format!("prekey secret persist failed: {err}")))?;
        Ok(())
    }

    async fn load_prekey_secrets(&self) -> io::Result<Option<Vec<u8>>> {
        self.read_secret_blob(
            SecretBlobKind::PrekeySidecar,
            PREKEY_SECRETS_BLOB_ID.as_bytes(),
        )
        .await
        .map_err(|err| io::Error::other(format!("prekey secret load failed: {err}")))
    }

    async fn save_doc_reservation(&self, doc_id: [u8; 32], bytes: Vec<u8>) -> io::Result<()> {
        self.write_secret_blob(SecretBlobKind::Reservation, &doc_id, &bytes)
            .await
            .map_err(|err| io::Error::other(format!("doc reservation persist failed: {err}")))?;
        Ok(())
    }

    async fn load_doc_reservation(&self, doc_id: [u8; 32]) -> io::Result<Option<Vec<u8>>> {
        self.read_secret_blob(SecretBlobKind::Reservation, &doc_id)
            .await
            .map_err(|err| io::Error::other(format!("doc reservation load failed: {err}")))
    }

    async fn list_doc_reservations(&self) -> io::Result<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        for blob_id in self
            .store
            .list_secret_blob_ids(SecretBlobKind::Reservation)
            .await
            .map_err(io::Error::other)?
        {
            if let Some(bytes) = self
                .read_secret_blob(SecretBlobKind::Reservation, &blob_id)
                .await
                .map_err(|err| io::Error::other(format!("doc reservation load failed: {err}")))?
            {
                out.push(bytes);
            }
        }
        Ok(out)
    }

    async fn delete_doc_reservation(&self, doc_id: [u8; 32]) -> io::Result<()> {
        self.store
            .delete_secret_blob(SecretBlobKind::Reservation, &doc_id)
            .await
            .map_err(io::Error::other)
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
    #[cfg(not(windows))]
    fn sync_dir(path: &Path) -> io::Result<()> {
        let file = std::fs::File::open(path)?;
        match file.sync_all() {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == io::ErrorKind::Unsupported => Ok(()),
            Err(err) => Err(err),
        }
    }

    #[cfg(windows)]
    fn sync_dir(_path: &Path) -> io::Result<()> {
        Ok(())
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

    /// DEK family id for a material kind: one DEK family per kind, so each
    /// kind can be re-encrypted (rotated) independently.
    fn dek_id_for(kind: SecretBlobKind) -> &'static str {
        match kind {
            SecretBlobKind::LocalSecret => DEK_ID_LOCAL_SECRET,
            SecretBlobKind::PrekeySidecar => DEK_ID_PREKEY_SIDECAR,
            SecretBlobKind::Reservation => DEK_ID_RESERVATION,
        }
    }

    /// Map a sqlite store error (eyre) into the fs storage error, preserving
    /// the chain via `SqliteBigRepoStoreError::Other`.
    fn sqlite_err(error: eyre::Report) -> FsKeyhiveStorageError {
        FsKeyhiveStorageError::Sqlite(SqliteBigRepoStoreError::Other(error))
    }

    /// DEK for `(dek_id, version)` from the configured source. Read paths
    /// never create keyring entries or file DEKs.
    async fn dek(&self, dek_id: &str, version: u64) -> Result<[u8; 32], FsKeyhiveStorageError> {
        let dek = match &self.secret_material {
            SecretMaterial::Keyring { repo } => repo
                .get_dek(dek_id, version)
                .await
                .map_err(FsKeyhiveStorageError::Secrets)?,
            SecretMaterial::FileDek => self
                .read_file_dek(dek_id, version)
                .await
                .map_err(FsKeyhiveStorageError::Io)?,
        };
        dek.ok_or_else(|| {
            FsKeyhiveStorageError::Secrets(secrets_rs::SecretsError::Missing(format!(
                "DEK {dek_id} version {version}"
            )))
        })
    }

    async fn dek_for_write(
        &self,
        dek_id: &str,
        version: u64,
    ) -> Result<[u8; 32], FsKeyhiveStorageError> {
        match &self.secret_material {
            SecretMaterial::Keyring { repo } => repo
                .get_or_create_dek(dek_id, version)
                .await
                .map_err(FsKeyhiveStorageError::Secrets),
            SecretMaterial::FileDek => self
                .create_file_dek(dek_id, version)
                .await
                .map_err(FsKeyhiveStorageError::Io),
        }
    }

    /// Algorithm label recorded in the `big_repo_deks` envelope row.
    fn dek_algorithm(&self) -> &'static str {
        match self.secret_material {
            SecretMaterial::Keyring { .. } => DEK_ALGORITHM_KEYRING,
            SecretMaterial::FileDek => DEK_ALGORITHM_FILE,
        }
    }

    async fn read_file_dek(&self, dek_id: &str, version: u64) -> io::Result<Option<[u8; 32]>> {
        let path = self
            .root
            .join(DEKS_SUBDIR)
            .join(format!("{dek_id}.v{version}.bin"));
        match tokio::fs::read(path).await {
            Ok(bytes) => bytes
                .try_into()
                .map(Some)
                .map_err(|_| io::Error::other("corrupt file DEK: bad length")),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// File-fallback DEK source (`deks/<dek_id>.v<version>.bin`, 0o600): one
    /// file per DEK entry so versioned rotation stays intact without a
    /// keyring. The DEK lives outside sqlite; the envelope row records it.
    async fn create_file_dek(&self, dek_id: &str, version: u64) -> io::Result<[u8; 32]> {
        let dir = self.root.join(DEKS_SUBDIR);
        let path = dir.join(format!("{dek_id}.v{version}.bin"));
        match tokio::fs::read(&path).await {
            Ok(bytes) => bytes
                .try_into()
                .map_err(|_| io::Error::other("corrupt file DEK: bad length")),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                tokio::fs::create_dir_all(&dir).await?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))?;
                }
                let mut dek = [0u8; 32];
                rand::rng().fill_bytes(&mut dek);
                let tmp_id = NEXT_TMP_ID.fetch_add(1, Ordering::Relaxed);
                let tmp = self
                    .tmp_dir()
                    .join(format!("dek.{}.{tmp_id}.tmp", std::process::id()));
                use tokio::io::AsyncWriteExt;
                let mut file = {
                    #[cfg(unix)]
                    {
                        tokio::fs::OpenOptions::new()
                            .write(true)
                            .create_new(true)
                            .mode(0o600)
                            .open(&tmp)
                            .await?
                    }
                    #[cfg(not(unix))]
                    {
                        tokio::fs::OpenOptions::new()
                            .write(true)
                            .create_new(true)
                            .open(&tmp)
                            .await?
                    }
                };
                file.write_all(&dek).await?;
                file.sync_all().await?;
                drop(file);
                let result = match tokio::fs::hard_link(&tmp, &path).await {
                    Ok(()) => {
                        if let Err(err) = Self::sync_dir(&dir) {
                            tracing::warn!(?dir, error = %err, "failed to fsync DEK directory after hard link");
                        }
                        Ok(dek)
                    }
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                        let bytes = tokio::fs::read(&path).await?;
                        bytes
                            .try_into()
                            .map_err(|_| io::Error::other("corrupt file DEK: bad length"))
                    }
                    Err(error) => Err(error),
                };
                drop(tokio::fs::remove_file(&tmp).await);
                result
            }
            Err(err) => Err(err),
        }
    }

    /// Encrypt `plaintext` under the newest DEK of `kind`'s family and
    /// persist the blob plus its DEK envelope row. New families start at
    /// version 0; the envelope table drives version selection so a rotation
    /// (inserting version v+1) is picked up by later writes automatically.
    ///
    /// `wrapped_dek` in the envelope row is left empty: on desktop the DEK
    /// source itself (OS keyring entry or the `deks/` file) is the protection,
    /// and the raw key must never be written into sqlite (a backup would then
    /// hold ciphertext and key together). The column is reserved for the
    /// KMS-wrapped DEK on cloud, where the read path will unwrap from it.
    async fn write_secret_blob(
        &self,
        kind: SecretBlobKind,
        blob_id: &[u8],
        plaintext: &[u8],
    ) -> Result<bool, FsKeyhiveStorageError> {
        let dek_id = Self::dek_id_for(kind);
        let versions = self
            .store
            .list_dek_versions(dek_id)
            .await
            .map_err(Self::sqlite_err)?;
        let version = versions.iter().max().copied().unwrap_or(0);
        let dek = self.dek_for_write(dek_id, version).await?;
        self.store
            .save_dek(dek_id, version, Vec::new(), 0, self.dek_algorithm())
            .await
            .map_err(Self::sqlite_err)?;
        let (ciphertext, nonce) = encrypt_blob(&dek, plaintext)?;
        let inserted = self
            .store
            .save_secret_blob(kind, blob_id, dek_id, version, ciphertext, nonce.to_vec())
            .await
            .map_err(Self::sqlite_err)?;
        Ok(inserted)
    }

    /// Start a rotation of `kind`'s DEK family: materialize the next version
    /// from the configured source (keyring entry or `deks/` file) and record
    /// its envelope row.
    ///
    /// The envelope table is the single version ledger: [`write_secret_blob`]
    /// selects the write version from it, so once this returns, subsequent
    /// writes encrypt under the new version while blobs written under older
    /// versions stay readable (their rows record the version that encrypted
    /// them) until a re-encryption pass migrates them forward and the old
    /// version row is GC'd.
    #[cfg_attr(not(test), expect(dead_code))]
    pub(crate) async fn rotate_dek(
        &self,
        kind: SecretBlobKind,
    ) -> Result<u64, FsKeyhiveStorageError> {
        let dek_id = Self::dek_id_for(kind);
        let versions = self
            .store
            .list_dek_versions(dek_id)
            .await
            .map_err(Self::sqlite_err)?;
        let version = match versions.iter().max() {
            Some(max) => max.saturating_add(1),
            None => 0,
        };
        // Materialize the DEK at the new version from the configured source
        // (creates the keyring entry or the deks/ file on first use).
        self.dek_for_write(dek_id, version).await?;
        self.store
            .save_dek(dek_id, version, Vec::new(), 0, self.dek_algorithm())
            .await
            .map_err(Self::sqlite_err)?;
        Ok(version)
    }

    /// Load and decrypt one secret blob, if present. The blob records which
    /// `(dek_id, version)` encrypted it, so older blobs stay readable after
    /// a rotation.
    async fn read_secret_blob(
        &self,
        kind: SecretBlobKind,
        blob_id: &[u8],
    ) -> Result<Option<Vec<u8>>, FsKeyhiveStorageError> {
        let Some(row) = self
            .store
            .load_secret_blob(kind, blob_id)
            .await
            .map_err(Self::sqlite_err)?
        else {
            return Ok(None);
        };
        let nonce: [u8; 12] = row
            .nonce
            .as_slice()
            .try_into()
            .map_err(|_| FsKeyhiveStorageError::Corrupt("bad nonce length".into()))?;
        let dek = self.dek(&row.dek_id, row.dek_version).await?;
        let plaintext = decrypt_blob(&dek, &row.ciphertext, &nonce)?;
        Ok(Some(plaintext))
    }

    /// One-time migration from the pre-sqlite filesystem layout. Each legacy
    /// blob is imported only when its own id is absent from sqlite, so partial
    /// imports are resumed on the next boot; legacy files remain in place.
    pub(crate) async fn import_legacy_secrets_if_empty(&self) -> Res<()> {
        use tokio::fs;

        let existing_local = self
            .store
            .list_secret_blob_ids(SecretBlobKind::LocalSecret)
            .await?
            .into_iter()
            .filter_map(|blob_id| <[u8; 32]>::try_from(blob_id).ok())
            .collect::<HashSet<_>>();
        let dir = self.local_secret_dir();
        if fs::try_exists(&dir).await? {
            let mut rd = fs::read_dir(&dir).await?;
            while let Some(entry) = rd.next_entry().await? {
                let path = entry.path();
                let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
                    continue;
                };
                let Some(hash) = StorageHash::from_hex(stem) else {
                    continue;
                };
                if existing_local.contains(hash.as_bytes()) {
                    continue;
                }
                let bytes = fs::read(&path).await?;
                self.write_secret_blob(SecretBlobKind::LocalSecret, hash.as_bytes(), &bytes)
                    .await?;
            }
        }

        if self
            .store
            .load_secret_blob(
                SecretBlobKind::PrekeySidecar,
                PREKEY_SECRETS_BLOB_ID.as_bytes(),
            )
            .await?
            .is_none()
        {
            let path = self.root.join(PREKEY_SECRETS_FILE);
            if fs::try_exists(&path).await? {
                let bytes = fs::read(&path).await?;
                self.write_secret_blob(
                    SecretBlobKind::PrekeySidecar,
                    PREKEY_SECRETS_BLOB_ID.as_bytes(),
                    &bytes,
                )
                .await?;
            }
        }

        let existing_reservations = self
            .store
            .list_secret_blob_ids(SecretBlobKind::Reservation)
            .await?
            .into_iter()
            .filter_map(|blob_id| <[u8; 32]>::try_from(blob_id).ok())
            .collect::<HashSet<_>>();
        let dir = self.root.join(RESERVATIONS_SUBDIR);
        if fs::try_exists(&dir).await? {
            let mut rd = fs::read_dir(&dir).await?;
            while let Some(entry) = rd.next_entry().await? {
                let path = entry.path();
                if path.extension().and_then(|ext| ext.to_str()) != Some("bin") {
                    continue;
                }
                // Legacy reservation files are named by doc_id hex.
                let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
                    continue;
                };
                let Some(hash) = StorageHash::from_hex(stem) else {
                    continue;
                };
                if existing_reservations.contains(hash.as_bytes()) {
                    continue;
                }
                let bytes = fs::read(&path).await?;
                self.write_secret_blob(SecretBlobKind::Reservation, hash.as_bytes(), &bytes)
                    .await?;
            }
        }
        Ok(())
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
        async move {
            let inserted = self
                .write_secret_blob(SecretBlobKind::LocalSecret, hash.as_bytes(), &data)
                .await?;
            Ok(inserted)
        }
        .boxed()
    }

    fn load_local_secrets(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(StorageHash, Vec<u8>)>, Self::Error>> {
        async move {
            let mut out = Vec::new();
            for blob_id in self
                .store
                .list_secret_blob_ids(SecretBlobKind::LocalSecret)
                .await
                .map_err(Self::sqlite_err)?
            {
                let hash_bytes: [u8; 32] = blob_id.as_slice().try_into().map_err(|_| {
                    FsKeyhiveStorageError::Corrupt("local secret blob id length".into())
                })?;
                let hash = StorageHash::new(hash_bytes);
                if let Some(bytes) = self
                    .read_secret_blob(SecretBlobKind::LocalSecret, &blob_id)
                    .await?
                {
                    out.push((hash, bytes));
                }
            }
            Ok(out)
        }
        .boxed()
    }

    fn delete_local_secret(&self, hash: StorageHash) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            self.store
                .delete_secret_blob(SecretBlobKind::LocalSecret, hash.as_bytes())
                .await
                .map_err(Self::sqlite_err)?;
            Ok(())
        }
        .boxed()
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

    pub(crate) async fn fs(events: SqliteBigRepoStore, root: PathBuf) -> eyre::Result<Self> {
        let archives = FsKeyhiveStorage::new(root, events.clone())?;
        archives.import_legacy_secrets_if_empty().await?;
        Ok(Self::new(BigRepoKeyhiveStorageInner::Fs {
            events,
            archives,
        }))
    }

    /// [`fs`] variant sourcing the DEK from the OS keyring; same on-disk
    /// layout for archives. Falls back to `fs` if the keyring cannot be
    /// initialised (headless systems).
    pub(crate) async fn fs_with_secret_repo(
        events: SqliteBigRepoStore,
        root: PathBuf,
    ) -> eyre::Result<Self> {
        match SecretRepo::boot().await {
            Ok(repo) => {
                let archives =
                    FsKeyhiveStorage::with_secret_repo(root, events.clone(), Arc::new(repo))?;
                tracing::debug!(
                    flavor = "keyring",
                    uses_keyring_secrets = archives.uses_keyring_secrets(),
                    "keyhive secret material stored in OS keyring"
                );
                archives.import_legacy_secrets_if_empty().await?;
                Ok(Self::new(BigRepoKeyhiveStorageInner::Fs {
                    events,
                    archives,
                }))
            }
            Err(err) => {
                tracing::warn!(
                    error = ?err,
                    "keyring-backed secret storage unavailable; \
                     falling back to file-based keyhive secret persistence"
                );
                Self::fs(events, root).await.map_err(|err| {
                    eyre::eyre!("file-based keyhive storage fallback failed: {err:#}")
                })
            }
        }
    }

    pub(crate) async fn save_prekey_secrets(&self, bytes: Vec<u8>) -> io::Result<()> {
        match &self.inner {
            BigRepoKeyhiveStorageInner::Memory(_) | BigRepoKeyhiveStorageInner::Sqlite { .. } => {
                // StorageConfig::Memory is ephemeral by design — all keyhive
                // state including key material is process-local; sidecar
                // persistence is intentionally skipped.
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
                // See save_prekey_secrets: Memory mode is ephemeral by design.
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

    use big_sync_core::BuckId;
    use sqlx_utils_rs::SqlCtx;

    #[tokio::test]
    async fn save_file_if_absent_writes_exact_bytes_and_is_idempotent() -> Res<()> {
        let root = unique_test_root("file-if-absent");
        let store =
            SqliteBigRepoStore::new(SqlCtx::memory().await?, "file-if-absent", BuckId::MAX_LEVEL)
                .await?;
        let storage = FsKeyhiveStorage::new(root.clone(), store)?;
        let parent_dir = root.join(LOCAL_SECRETS_SUBDIR);
        let hash = StorageHash::new([42u8; 32]);
        let data = b"secret material".to_vec();

        assert!(
            storage
                .save_file_if_absent(parent_dir.clone(), hash, data.clone())
                .await?
        );

        let written = tokio::fs::read(
            root.join(LOCAL_SECRETS_SUBDIR)
                .join(format!("{}.bin", hash.to_hex())),
        )
        .await?;
        assert_eq!(written, data);

        // Same hash with different data must be refused and must not clobber.
        assert!(
            !storage
                .save_file_if_absent(parent_dir.clone(), hash, b"clobber".to_vec())
                .await?
        );
        let unchanged = tokio::fs::read(
            root.join(LOCAL_SECRETS_SUBDIR)
                .join(format!("{}.bin", hash.to_hex())),
        )
        .await?;
        assert_eq!(unchanged, data);

        // A different hash still writes.
        let other = StorageHash::new([43u8; 32]);
        assert!(
            storage
                .save_file_if_absent(parent_dir.clone(), other, b"other".to_vec())
                .await?
        );

        tokio::fs::remove_dir_all(&root).await?;
        Ok(())
    }

    fn unique_test_root(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "bigrepo-keyhive-storage-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or_default()
        ))
    }

    async fn keyring_test_storage(label: &str) -> Res<(FsKeyhiveStorage, std::path::PathBuf)> {
        let root = unique_test_root(label);
        let store = SqliteBigRepoStore::new(
            SqlCtx::memory().await?,
            format!("keyhive-storage-test-{label}"),
            BuckId::MAX_LEVEL,
        )
        .await?;
        let repo = SecretRepo::boot().await.expect("mock keyring boot");
        let storage = FsKeyhiveStorage::with_secret_repo(root.clone(), store, Arc::new(repo))?;
        Ok((storage, root))
    }

    #[tokio::test]
    async fn keyring_mode_round_trips_local_secrets() -> Res<()> {
        let (storage, root) = keyring_test_storage("roundtrip").await?;
        assert!(storage.uses_keyring_secrets());

        let hash = StorageHash::new([7u8; 32]);
        let data = b"keyring secret".to_vec();

        assert!(storage.save_local_secret(hash, data.clone()).await?);
        let loaded = storage.load_local_secrets().await?;
        assert_eq!(loaded, vec![(hash, data.clone())]);

        // A second secret survives alongside the first.
        let other = StorageHash::new([8u8; 32]);
        storage.save_local_secret(other, b"more".to_vec()).await?;
        let loaded = storage.load_local_secrets().await?;
        assert_eq!(loaded.len(), 2);

        // Deleting removes the blob and the envelope stays valid.
        storage.delete_local_secret(hash).await?;
        let loaded = storage.load_local_secrets().await?;
        assert_eq!(loaded, vec![(other, b"more".to_vec())]);

        tokio::fs::remove_dir_all(&root).await?;
        Ok(())
    }

    #[tokio::test]
    async fn keyring_mode_round_trips_prekey_secrets() -> Res<()> {
        let (storage, root) = keyring_test_storage("prekey").await?;

        assert!(storage.load_prekey_secrets().await?.is_none());
        storage.save_prekey_secrets(b"prekey blob".to_vec()).await?;
        assert_eq!(
            storage.load_prekey_secrets().await?,
            Some(b"prekey blob".to_vec())
        );
        // Second write overwrites; last write wins.
        storage.save_prekey_secrets(b"updated".to_vec()).await?;
        assert_eq!(
            storage.load_prekey_secrets().await?,
            Some(b"updated".to_vec())
        );

        tokio::fs::remove_dir_all(&root).await?;
        Ok(())
    }

    #[tokio::test]
    async fn file_mode_round_trips_prekey_secrets_via_sqlite() -> Res<()> {
        let root = unique_test_root("prekey-file");
        let store =
            SqliteBigRepoStore::new(SqlCtx::memory().await?, "prekey-file", BuckId::MAX_LEVEL)
                .await?;
        let storage = FsKeyhiveStorage::new(root.clone(), store)?;
        assert!(!storage.uses_keyring_secrets());

        // File mode now persists to sqlite encrypted under a file-fallback
        // DEK; nothing is written to the legacy prekey file on disk.
        storage.save_prekey_secrets(b"file blob".to_vec()).await?;
        assert_eq!(
            storage.load_prekey_secrets().await?,
            Some(b"file blob".to_vec())
        );
        let dek_path = root
            .join(DEKS_SUBDIR)
            .join(format!("{DEK_ID_PREKEY_SIDECAR}.v0.bin"));
        assert!(tokio::fs::try_exists(&dek_path).await?);

        tokio::fs::remove_dir_all(&root).await?;
        Ok(())
    }

    #[tokio::test]
    async fn legacy_files_are_imported_into_sqlite_on_first_boot() -> Res<()> {
        let root = unique_test_root("legacy-import");
        // Simulate the pre-sqlite layout.
        let secrets_dir = root.join(LOCAL_SECRETS_SUBDIR);
        tokio::fs::create_dir_all(&secrets_dir).await?;
        let hash = StorageHash::new([9u8; 32]);
        tokio::fs::write(
            secrets_dir.join(format!("{}.bin", hash.to_hex())),
            b"legacy secret",
        )
        .await?;
        tokio::fs::write(root.join(PREKEY_SECRETS_FILE), b"legacy prekey").await?;
        let reservations_dir = root.join(RESERVATIONS_SUBDIR);
        tokio::fs::create_dir_all(&reservations_dir).await?;
        let doc_id = [11u8; 32];
        let expected_reservation = DocReservation {
            magic: DOC_RESERVATION_MAGIC,
            doc_id,
            signing_key: [12u8; 32],
            parents: Vec::new(),
            initial_keys: Vec::new(),
            initial_content: None,
        };
        tokio::fs::write(
            reservations_dir.join(format!("{}.bin", StorageHash::new(doc_id).to_hex())),
            bincode::serialize(&expected_reservation)?,
        )
        .await?;

        let store =
            SqliteBigRepoStore::new(SqlCtx::memory().await?, "legacy-import", BuckId::MAX_LEVEL)
                .await?;
        let storage = BigRepoKeyhiveStorage::fs(store.clone(), root.clone()).await?;

        // Local secrets round-trip through the sqlite blob store.
        let loaded = storage.load_local_secrets().await?;
        assert_eq!(loaded, vec![(hash, b"legacy secret".to_vec())]);
        assert_eq!(
            storage.load_prekey_secrets().await?,
            Some(b"legacy prekey".to_vec())
        );
        let reservations = storage.list_doc_reservations().await?;
        assert_eq!(reservations, vec![expected_reservation.clone()]);
        assert_eq!(
            storage.load_doc_reservation(doc_id).await?,
            Some(expected_reservation)
        );

        // A partial first import must resume for an id not yet in sqlite.
        let second_hash = StorageHash::new([10u8; 32]);
        tokio::fs::write(
            secrets_dir.join(format!("{}.bin", second_hash.to_hex())),
            b"second legacy secret",
        )
        .await?;
        // Idempotent: a second boot does not duplicate blobs.
        let storage2 = BigRepoKeyhiveStorage::fs(store, root).await?;
        assert_eq!(
            storage2.load_prekey_secrets().await?,
            Some(b"legacy prekey".to_vec())
        );
        let loaded2 = storage2.load_local_secrets().await?;
        assert_eq!(loaded2.len(), 2);
        assert!(loaded2.contains(&(second_hash, b"second legacy secret".to_vec())));
        assert_eq!(storage2.list_doc_reservations().await?.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn rotation_keeps_old_blobs_readable_and_new_writes_use_new_version() -> Res<()> {
        let root = unique_test_root("rotation");
        let store =
            SqliteBigRepoStore::new(SqlCtx::memory().await?, "rotation", BuckId::MAX_LEVEL).await?;
        let repo = SecretRepo::boot().await.expect("mock keyring boot");
        let storage =
            FsKeyhiveStorage::with_secret_repo(root.clone(), store.clone(), Arc::new(repo))?;

        let hash_a = StorageHash::new([21u8; 32]);
        let hash_b = StorageHash::new([22u8; 32]);
        storage
            .save_local_secret(hash_a, b"blob a".to_vec())
            .await?;

        // Rotate: the next envelope version becomes the write target.
        let version = storage.rotate_dek(SecretBlobKind::LocalSecret).await?;
        assert_eq!(version, 1);

        storage
            .save_local_secret(hash_b, b"blob b".to_vec())
            .await?;

        // The new blob is written under v1...
        let row_b = store
            .load_secret_blob(SecretBlobKind::LocalSecret, hash_b.as_bytes())
            .await?
            .expect("blob b row");
        assert_eq!(row_b.dek_id, DEK_ID_LOCAL_SECRET);
        assert_eq!(row_b.dek_version, 1);

        // ...while the old blob stays readable: the read path honors the
        // version recorded on each blob, so v0 blobs survive a rotation.
        let loaded = storage.load_local_secrets().await?;
        assert_eq!(loaded.len(), 2);
        assert!(loaded.contains(&(hash_a, b"blob a".to_vec())));
        assert!(loaded.contains(&(hash_b, b"blob b".to_vec())));

        // The envelope holds both versions, so a re-encryption pass can later
        // migrate v0 blobs forward and GC the old row once unreferenced.
        assert_eq!(
            store.list_dek_versions(DEK_ID_LOCAL_SECRET).await?,
            vec![0, 1]
        );

        tokio::fs::remove_dir_all(&root).await?;
        Ok(())
    }

    #[tokio::test]
    async fn restart_reopens_store_and_decrypts_written_blobs() -> Res<()> {
        let root = unique_test_root("restart");
        tokio::fs::create_dir_all(&root).await?;
        let db_path = root.join("sqlite.db");

        // First "boot": file-backed sqlite, file-fallback DEKs.
        let sql1 = SqlCtx::url(&format!("sqlite://{}", db_path.display())).await?;
        let store1 = SqliteBigRepoStore::new(sql1, "restart", BuckId::MAX_LEVEL).await?;
        let storage1 = FsKeyhiveStorage::new(root.clone(), store1)?;
        let hash = StorageHash::new([31u8; 32]);
        storage1
            .save_local_secret(hash, b"persisted secret".to_vec())
            .await?;
        storage1
            .save_prekey_secrets(b"persisted prekey".to_vec())
            .await?;
        drop(storage1);

        // Second "boot": reopen the same sqlite file with a fresh store; the
        // blobs must decrypt under the DEKs read back from the deks/ files.
        let sql2 = SqlCtx::url(&format!("sqlite://{}", db_path.display())).await?;
        let store2 = SqliteBigRepoStore::new(sql2, "restart", BuckId::MAX_LEVEL).await?;
        let storage2 = FsKeyhiveStorage::new(root.clone(), store2)?;
        assert_eq!(
            storage2.load_local_secrets().await?,
            vec![(hash, b"persisted secret".to_vec())]
        );
        assert_eq!(
            storage2.load_prekey_secrets().await?,
            Some(b"persisted prekey".to_vec())
        );

        // The DEK files are reused across boots (deterministic per version),
        // not rotated: no higher-version DEK appears out of nowhere.
        let dek_dir = root.join(DEKS_SUBDIR);
        assert!(
            tokio::fs::try_exists(dek_dir.join(format!("{DEK_ID_PREKEY_SIDECAR}.v0.bin"))).await?
        );
        let mut entries = tokio::fs::read_dir(&dek_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name().to_string_lossy().into_owned();
            assert!(
                !name.contains(".v1."),
                "unexpected rotated DEK file: {name}"
            );
        }

        tokio::fs::remove_dir_all(&root).await?;
        Ok(())
    }

    #[tokio::test]
    async fn envelope_row_records_metadata_not_the_dek() -> Res<()> {
        let root = unique_test_root("envelope");
        let store =
            SqliteBigRepoStore::new(SqlCtx::memory().await?, "envelope", BuckId::MAX_LEVEL).await?;
        let repo = SecretRepo::boot().await.expect("mock keyring boot");
        let storage =
            FsKeyhiveStorage::with_secret_repo(root.clone(), store.clone(), Arc::new(repo))?;

        storage.save_prekey_secrets(b"sidecar".to_vec()).await?;

        // The envelope row records metadata only. The raw DEK must never land
        // in sqlite: a backup would otherwise hold ciphertext and key
        // together. The DEK source (keyring entry) is the protection on
        // desktop; wrapped_dek is reserved for the future KMS-wrapped form.
        let dek = store
            .load_dek(DEK_ID_PREKEY_SIDECAR, 0)
            .await?
            .expect("envelope row");
        assert!(dek.wrapped_dek.is_empty());
        assert_eq!(dek.kek_version, 0);
        assert_eq!(dek.algorithm, DEK_ALGORITHM_KEYRING);

        tokio::fs::remove_dir_all(&root).await?;
        Ok(())
    }

    #[tokio::test]
    async fn reservation_round_trips_through_live_path() -> Res<()> {
        let root = unique_test_root("reservation-live");
        let store = SqliteBigRepoStore::new(
            SqlCtx::memory().await?,
            "reservation-live",
            BuckId::MAX_LEVEL,
        )
        .await?;
        let storage = BigRepoKeyhiveStorage::fs_with_secret_repo(store, root.clone()).await?;

        let doc_id = [51u8; 32];
        let reservation = DocReservation {
            magic: DOC_RESERVATION_MAGIC,
            doc_id,
            signing_key: [52u8; 32],
            parents: vec![[53u8; 32]],
            initial_keys: vec![(vec![1, 2], [54u8; 32])],
            initial_content: Some(vec![1, 2, 3]),
        };

        storage.save_doc_reservation(&reservation).await?;
        assert_eq!(
            storage.load_doc_reservation(doc_id).await?,
            Some(reservation.clone())
        );
        assert_eq!(
            storage.list_doc_reservations().await?,
            vec![reservation.clone()]
        );
        storage.delete_doc_reservation(doc_id).await?;
        assert_eq!(storage.load_doc_reservation(doc_id).await?, None);

        tokio::fs::remove_dir_all(&root).await?;
        Ok(())
    }

    #[tokio::test]
    async fn corrupt_records_fail_loudly() -> Res<()> {
        let root = unique_test_root("corrupt");
        tokio::fs::create_dir_all(&root).await?;
        let db_path = root.join("sqlite.db");
        let sql = SqlCtx::url(&format!("sqlite://{}", db_path.display())).await?;
        let store = SqliteBigRepoStore::new(sql, "corrupt", BuckId::MAX_LEVEL).await?;
        let storage = FsKeyhiveStorage::new(root.clone(), store.clone())?;

        // (a) A blob row with a non-12-byte nonce must fail with Corrupt.
        let bad_nonce_hash = StorageHash::new([41u8; 32]);
        store
            .save_dek(DEK_ID_LOCAL_SECRET, 0, Vec::new(), 0, "chacha20poly1305")
            .await?;
        store
            .save_secret_blob(
                SecretBlobKind::LocalSecret,
                bad_nonce_hash.as_bytes(),
                DEK_ID_LOCAL_SECRET,
                0,
                b"ct".to_vec(),
                vec![1, 2, 3, 4, 5],
            )
            .await?;
        let err = storage.load_local_secrets().await.unwrap_err();
        assert!(matches!(err, FsKeyhiveStorageError::Corrupt(_)));
        store
            .delete_secret_blob(SecretBlobKind::LocalSecret, bad_nonce_hash.as_bytes())
            .await?;

        // (b) Tampered ciphertext (authenticated encryption) must fail.
        let hash = StorageHash::new([42u8; 32]);
        storage
            .save_local_secret(hash, b"original".to_vec())
            .await?;
        let row = store
            .load_secret_blob(SecretBlobKind::LocalSecret, hash.as_bytes())
            .await?
            .expect("tampered row");
        let mut ciphertext = row.ciphertext.clone();
        let last = ciphertext.len() - 1;
        ciphertext[last] ^= 0xFF;
        store
            .save_secret_blob(
                SecretBlobKind::LocalSecret,
                hash.as_bytes(),
                &row.dek_id,
                row.dek_version,
                ciphertext,
                row.nonce.clone(),
            )
            .await?;
        let err = storage.load_local_secrets().await.unwrap_err();
        assert!(matches!(err, FsKeyhiveStorageError::Secrets(_)));

        // (c) A corrupt DEK file (wrong length) must fail, not silently
        // regenerate a key that would brick every existing blob.
        let dek_path = root
            .join(DEKS_SUBDIR)
            .join(format!("{DEK_ID_PREKEY_SIDECAR}.v0.bin"));
        tokio::fs::create_dir_all(root.join(DEKS_SUBDIR)).await?;
        tokio::fs::write(&dek_path, b"1234567").await?;
        let err = storage
            .save_prekey_secrets(b"x".to_vec())
            .await
            .unwrap_err();
        assert!(matches!(err.kind(), io::ErrorKind::Other));

        tokio::fs::remove_dir_all(&root).await?;
        Ok(())
    }
}
