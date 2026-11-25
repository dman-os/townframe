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

use crate::keyhive_files::KeyhiveFiles;
use crate::store::sqlite::SecretBlobKind;
use crate::store::sqlite::SqliteBigRepoStore;
use crate::store::sqlite::SqliteBigRepoStoreError;
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::Infallible;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use futures::lock::Mutex;

use futures::FutureExt;
use futures::future::BoxFuture;
use rand::RngCore;
use secrets_rs::SecretStore;
use secrets_rs::decrypt_blob;
use secrets_rs::encrypt_blob;
use subduction_keyhive::storage::KeyhiveStorage;
use subduction_keyhive::storage::MemoryKeyhiveStorage;
use subduction_keyhive::storage::StorageHash;
use utils_rs::prelude::eyre;

const ARCHIVES_SUBDIR: &str = "archives";
const LOCAL_SECRETS_SUBDIR: &str = "local-secrets";
const PREKEY_SECRETS_FILE: &str = "prekey-secrets.bin";
const RESERVATIONS_SUBDIR: &str = "reservations";
const TMP_SUBDIR: &str = "tmp";

/// Magic bytes prefixing a [`DocReservation`] blob in local-secret storage.
pub(crate) const DOC_RESERVATION_MAGIC: [u8; 4] = *b"DRSV";

/// A durably reserved document identity: the ephemeral signing key whose
/// verifying key is the eventual document ID.
///
/// Stored in Keyhive's local-secret storage (never synchronized); it is the
/// crash-recovery record between ID allocation and the document commit.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct DocReservation {
    pub magic: [u8; 4],
    pub doc_id: [u8; 32],
    pub signing_key: [u8; 32],
    /// Keys for causal parents inherited from another document, if any.
    pub initial_keys: Vec<(Vec<u8>, [u8; 32])>,
    /// Serialized initial Automerge content, staged before Keyhive creation.
    /// None means an identity has been reserved but content creation has not
    /// started yet.
    pub initial_content: Option<Vec<u8>>,
}

/// Reserved blob id under which the prekey-secrets sidecar is stored.
const PREKEY_SECRETS_BLOB_ID: &str = "prekey-secrets.v1";
/// Subdirectory holding file-fallback key material (`<dek_id>.v<version>.bin`, 0o600).
const DEKS_SUBDIR: &str = "deks";
/// Stable DEK identifiers for the kinds of secret material stored in sqlite.
/// Each identifier is recorded per blob and in `big_repo_deks`.
const DEK_ID_LOCAL_SECRET: &str = "local-secret";
const DEK_ID_PREKEY_SIDECAR: &str = "prekey-sidecar";
const DEK_ID_RESERVATION: &str = "reservation";
const DEK_SERVICE: &str = "daybook.material.v1";
/// `big_repo_deks.algorithm` labels for the desktop DEK sources.
const DEK_ALGORITHM_KEYRING: &str = "keyring";
const DEK_ALGORITHM_FILE: &str = "file";
/// Monotonic per-process counter for temp filenames.
static NEXT_TMP_ID: AtomicU64 = AtomicU64::new(0);

/// Creates the directories a keyhive storage root holds.
///
/// The legacy filesystem layout is still created so the one-time import
/// (`import_legacy_secrets_if_empty`) can read it; new writes never touch the
/// legacy secret dirs.
pub(crate) fn create_layout(root: &Path) -> io::Result<()> {
    std::fs::create_dir_all(root.join(ARCHIVES_SUBDIR))?;
    for subdir in [LOCAL_SECRETS_SUBDIR, RESERVATIONS_SUBDIR, TMP_SUBDIR] {
        let dir = root.join(subdir);
        std::fs::create_dir_all(&dir)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))?;
        }
    }
    Ok(())
}

/// Where the DEK lives for a filesystem-backed storage. Secret material
/// itself always persists to sqlite (encrypted under the DEK); this only
/// sources the key bytes.
#[derive(Clone)]
enum SecretKeySource {
    /// File-backed key fallback (`deks/<dek_id>.v<version>.bin`, 0o600) for
    /// headless systems without an OS keyring.
    FileDek,
    /// OS key material via the shared `secrets_rs` store.
    Keyring { store: Arc<SecretStore> },
}

impl std::fmt::Debug for SecretKeySource {
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
/// [`SecretKeySource`].
#[derive(Clone)]
pub(crate) struct FsKeyhiveStorage {
    root: PathBuf,
    store: SqliteBigRepoStore,
    secret_key_source: SecretKeySource,
}

impl std::fmt::Debug for FsKeyhiveStorage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FsKeyhiveStorage")
            .field("root", &self.root)
            .field("secret_key_source", &self.secret_key_source)
            .finish()
    }
}

impl FsKeyhiveStorage {
    /// Create the storage root and its subdirs. Secret material persists to
    /// sqlite encrypted under a file-fallback DEK (no OS keyring required).
    #[cfg(test)]
    pub(crate) fn new(root: PathBuf, store: SqliteBigRepoStore) -> io::Result<Self> {
        Self::new_with_secret_key_source(root, store, SecretKeySource::FileDek)
    }

    /// Same layout as [`FsKeyhiveStorage::new`], but key material is sourced
    /// from the OS keyring via the shared `SecretStore`. Secret blobs still
    /// persist to sqlite (encrypted); the keyring never holds blobs.
    #[cfg(test)]
    pub(crate) fn with_secret_store(
        root: PathBuf,
        store: SqliteBigRepoStore,
        secret_store: Arc<SecretStore>,
    ) -> io::Result<Self> {
        Self::new_with_secret_key_source(
            root,
            store,
            SecretKeySource::Keyring {
                store: secret_store,
            },
        )
    }

    /// Whether key material is sourced from the OS keyring.
    fn uses_keyring_secrets(&self) -> bool {
        matches!(self.secret_key_source, SecretKeySource::Keyring { .. })
    }

    #[cfg(test)]
    fn new_with_secret_key_source(
        root: PathBuf,
        store: SqliteBigRepoStore,
        secret_key_source: SecretKeySource,
    ) -> io::Result<Self> {
        create_layout(&root)?;
        Ok(Self {
            root,
            store,
            secret_key_source,
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
                match tokio::fs::remove_file(&tmp).await {
                    Ok(()) => {}
                    Err(cleanup) if cleanup.kind() == io::ErrorKind::NotFound => {}
                    Err(cleanup) => {
                        return Err(io::Error::other(format!(
                            "rename failed: {err}; temporary-file cleanup failed: {cleanup}"
                        )));
                    }
                }
                Err(err)
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
        let result = match publish_no_replace(&tmp, &dest).await {
            Ok(()) => {
                if let Err(err) = Self::sync_dir(&parent_dir) {
                    tracing::warn!(?parent_dir, error = %err, "failed to fsync keyhive storage dir after publishing");
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

    /// Stable DEK identifier for a material kind.
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

    /// Read the key for a blob's recorded version. Read paths never create
    /// keyring entries or file keys.
    async fn dek(&self, dek_id: &str, version: u64) -> Result<[u8; 32], FsKeyhiveStorageError> {
        let key = match &self.secret_key_source {
            SecretKeySource::Keyring { store } => store
                .get_secret(DEK_SERVICE, &Self::dek_name(dek_id, version))
                .await
                .map_err(FsKeyhiveStorageError::Secrets)?,
            SecretKeySource::FileDek => self
                .read_file_dek(dek_id, version)
                .await
                .map_err(FsKeyhiveStorageError::Io)?
                .map(Vec::from),
        };
        let key = key.ok_or_else(|| {
            FsKeyhiveStorageError::Secrets(secrets_rs::SecretsError::Missing(format!(
                "key {dek_id} version {version}"
            )))
        })?;
        key.try_into().map_err(|_| {
            FsKeyhiveStorageError::Corrupt(format!("key {dek_id} version {version} has bad length"))
        })
    }

    async fn dek_for_write(
        &self,
        dek_id: &str,
        version: u64,
    ) -> Result<[u8; 32], FsKeyhiveStorageError> {
        match &self.secret_key_source {
            SecretKeySource::Keyring { store } => {
                let key = store
                    .get_or_create_secret(DEK_SERVICE, &Self::dek_name(dek_id, version), 32)
                    .await
                    .map_err(FsKeyhiveStorageError::Secrets)?;
                key.try_into().map_err(|_| {
                    FsKeyhiveStorageError::Corrupt(format!(
                        "key {dek_id} version {version} has bad length"
                    ))
                })
            }
            SecretKeySource::FileDek => self
                .create_file_dek(dek_id, version)
                .await
                .map_err(FsKeyhiveStorageError::Io),
        }
    }

    fn dek_name(dek_id: &str, version: u64) -> String {
        format!("{dek_id}.v{version}")
    }

    /// Authenticate the blob identity and key metadata alongside its payload.
    /// This prevents moving ciphertext between rows or changing the recorded
    /// key version without detection.
    fn blob_aad(kind: SecretBlobKind, blob_id: &[u8], dek_id: &str, version: u64) -> Vec<u8> {
        let mut aad = b"townframe/secret-blob/v1".to_vec();
        aad.extend_from_slice(&kind.as_i64().to_le_bytes());
        aad.extend_from_slice(&(blob_id.len() as u64).to_le_bytes());
        aad.extend_from_slice(blob_id);
        aad.extend_from_slice(&(dek_id.len() as u64).to_le_bytes());
        aad.extend_from_slice(dek_id.as_bytes());
        aad.extend_from_slice(&version.to_le_bytes());
        aad
    }

    /// Algorithm label recorded in the `big_repo_deks` envelope row.
    fn dek_algorithm(&self) -> &'static str {
        match self.secret_key_source {
            SecretKeySource::Keyring { .. } => DEK_ALGORITHM_KEYRING,
            SecretKeySource::FileDek => DEK_ALGORITHM_FILE,
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

    /// File-fallback key source (`deks/<dek_id>.v<version>.bin`, 0o600): one
    /// file per key entry. The key lives outside sqlite; the envelope row
    /// records its identifier and version.
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
                let result = match publish_no_replace(&tmp, &path).await {
                    Ok(()) => {
                        if let Err(err) = Self::sync_dir(&dir) {
                            tracing::warn!(?dir, error = %err, "failed to fsync DEK directory after publishing");
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

    /// Encrypt `plaintext` under the fixed initial key version for `kind` and
    /// persist the blob plus its DEK envelope row. Key rotation is deferred,
    /// so new writes currently always use version 0.
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
        // Payload rotation is intentionally deferred; all writes currently use
        // the initial key version.
        let version = 0;
        let dek = self.dek_for_write(dek_id, version).await?;
        self.store
            .save_dek(dek_id, version, Vec::new(), 0, self.dek_algorithm())
            .await
            .map_err(Self::sqlite_err)?;
        let aad = Self::blob_aad(kind, blob_id, dek_id, version);
        let (ciphertext, nonce) = encrypt_blob(&dek, &aad, plaintext)?;
        let inserted = self
            .store
            .save_secret_blob(kind, blob_id, dek_id, version, ciphertext, nonce.to_vec())
            .await
            .map_err(Self::sqlite_err)?;
        Ok(inserted)
    }

    /// Load and decrypt one secret blob, if present. The blob records which
    /// `(dek_id, version)` encrypted it, so persisted records remain readable
    /// when their key version differs from the current write version.
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
        let aad = Self::blob_aad(kind, blob_id, &row.dek_id, row.dek_version);
        let plaintext = decrypt_blob(&dek, &aad, &row.ciphertext, &nonce)?;
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

    /// Archives in the opened `files`, with the secret blobs in `events`' database.
    pub(crate) async fn files(
        events: SqliteBigRepoStore,
        files: KeyhiveFiles,
    ) -> eyre::Result<Self> {
        let KeyhiveFiles { root, secrets } = files;
        let secret_key_source = match secrets {
            Some(store) => SecretKeySource::Keyring { store },
            None => SecretKeySource::FileDek,
        };
        let archives = FsKeyhiveStorage {
            root,
            store: events.clone(),
            secret_key_source,
        };
        tracing::debug!(
            uses_keyring_secrets = archives.uses_keyring_secrets(),
            "keyhive secret material key source"
        );
        archives.import_legacy_secrets_if_empty().await?;
        Ok(Self::new(BigRepoKeyhiveStorageInner::Fs {
            events,
            archives,
        }))
    }

    #[cfg(test)]
    pub(crate) async fn fs(events: SqliteBigRepoStore, root: PathBuf) -> eyre::Result<Self> {
        Self::files(events, KeyhiveFiles::open(root, None)?).await
    }

    /// [`Self::files`] with the data-encryption keys in the (mock) OS keyring.
    #[cfg(test)]
    pub(crate) async fn fs_with_secret_store(
        events: SqliteBigRepoStore,
        root: PathBuf,
    ) -> eyre::Result<Self> {
        let secrets = Arc::new(SecretStore::boot().await?);
        Self::files(events, KeyhiveFiles::open(root, Some(secrets))?).await
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

/// Publishes the fully written `tmp` at `dest` only if nothing is there yet: atomic, the first
/// writer wins, and a later writer gets `AlreadyExists` and keeps what is there (content-addressed
/// blobs and the DEK rely on this).
///
/// Android's SELinux policy refuses apps the `link` syscall on their own data files (`EACCES`), so
/// the portable no-clobber publish, a hard link, cannot be used there. On Linux and Android this is
/// `renameat2(RENAME_NOREPLACE)`. Where that call is unsupported or refused (`EINVAL`: the
/// filesystem rejects the flag; `ENOSYS`: no such syscall; `EPERM`/`EACCES`: a policy refuses it),
/// it is a check for `dest` and a plain `rename`. Any other error is the publish's own failure and
/// is returned. Other platforms publish with a hard link.
async fn publish_no_replace(tmp: &Path, dest: &Path) -> io::Result<()> {
    let (tmp, dest) = (tmp.to_owned(), dest.to_owned());
    tokio::task::spawn_blocking(move || publish_no_replace_blocking(&tmp, &dest, true))
        .await
        .expect("the publish task does not panic")
}

/// [`publish_no_replace`] on a blocking thread. Every publish in the process holds one lock for
/// its whole duration, whichever path it takes: the fallback's check-then-rename is only
/// first-writer-wins if no other publish (fast path included) can create `dest` between the check
/// and the rename. Only one process holds a node's directory (the holder lock), so a process-wide
/// lock covers every writer. `try_renameat2: false` takes the fallback directly (tests).
fn publish_no_replace_blocking(tmp: &Path, dest: &Path, try_renameat2: bool) -> io::Result<()> {
    static PUBLISHING: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _publishing = PUBLISHING
        .lock()
        .expect("no publish panics while holding the lock");
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        use rustix::io::Errno;
        static FALLBACK_REPORTED: std::sync::Once = std::sync::Once::new();
        if try_renameat2 {
            match rustix::fs::renameat_with(
                rustix::fs::CWD,
                tmp,
                rustix::fs::CWD,
                dest,
                rustix::fs::RenameFlags::NOREPLACE,
            ) {
                Ok(()) => return Ok(()),
                Err(Errno::EXIST) => return Err(io::ErrorKind::AlreadyExists.into()),
                Err(errno @ (Errno::INVAL | Errno::NOSYS | Errno::PERM | Errno::ACCESS)) => {
                    FALLBACK_REPORTED.call_once(|| {
                        tracing::warn!(%errno, "renameat2(RENAME_NOREPLACE) unsupported or refused; keyhive storage publishes with a locked check and rename");
                    });
                }
                Err(errno) => return Err(errno.into()),
            }
        }
        if dest.try_exists()? {
            return Err(io::ErrorKind::AlreadyExists.into());
        }
        std::fs::rename(tmp, dest)
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        // No `renameat2` to skip: `try_renameat2` only selects a Linux path.
        let _ = try_renameat2;
        std::fs::hard_link(tmp, dest)
    }
}

#[cfg(test)]
mod tests;
