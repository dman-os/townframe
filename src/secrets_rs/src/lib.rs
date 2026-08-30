//! Secret storage backed by OS keyrings.
//!
//! Owns the [`SecretRepo`] abstraction previously inlined in
//! `daybook_core::secrets`: repo identities for checkout provisioning plus an
//! opaque, namespaced opaque-blob API for small key-material payloads
//! (keyhive prekey/CGKA secrets).
//!
//! Blob payloads are stored base58-multibase encoded so every backend sees
//! printable text, mirroring how identities are persisted. Keyring backends
//! bound credential payloads (e.g. kernel keyutils ≈ 32 KiB), so [`SecretRepo`]
//! refuses oversized blobs up front with [`SecretsError::BlobTooLarge`] instead
//! of letting a backend fail cryptically or truncate.
//!
//! Keyring calls are synchronous/blocking; async surfaces delegate to
//! `tokio::task::spawn_blocking`, and store teardown happens off any Tokio
//! runtime thread (the zbus backend must not drop on a runtime thread).

use std::sync::Arc;

use utils_rs::expect_tags::{ERROR_IMPOSSIBLE, ERROR_TOKIO};
use utils_rs::hash::{decode_base58_multibase, encode_base58_multibase};
use utils_rs::prelude::eyre;
use utils_rs::prelude::WrapErr;

pub type Res<T> = eyre::Result<T>;

/// Service under which all material blobs are stored (distinct from the
/// `"daybook"` identity service so the two namespaces never collide).
const MATERIAL_SERVICE: &str = "daybook.material.v1";

/// Reserved blob id holding the per-namespace material index. Index entries
/// are the ids of blobs registered through [`SecretRepo::add_to_index`]; a
/// blob that is never indexed stays invisible to
/// [`SecretRepo::list_blob_ids`].
pub const MATERIAL_INDEX_ID: &str = "__index__";

/// Hard cap on a single blob's raw byte length. Base58 encoding inflates
/// payloads ~1.37x, and keyutils caps credential payloads around 32 KiB; 16
/// KiB of raw bytes stays comfortably under that bound for every backend.
pub const MAX_BLOB_BYTES: usize = 16 * 1024;

/// Errors surfaced by the material blob API.
#[derive(Debug, thiserror::Error)]
pub enum SecretsError {
    /// The blob exceeds [`MAX_BLOB_BYTES`] and cannot round-trip through
    /// keyring backends; callers must split or persist it elsewhere.
    #[error("material blob too large for keyring storage: {len} bytes exceeds {max} (id: {id})")]
    BlobTooLarge { id: String, len: usize, max: usize },
    /// Namespace or id contained characters keyring usernames must not.
    #[error("invalid secret storage name: {0}")]
    InvalidName(String),
    /// The backing keyring store rejected the operation.
    #[error("keyring error: {0}")]
    Keyring(#[from] keyring_core::Error),
    /// A stored material blob could not be decoded.
    #[error("stored material blob is corrupt: {0}")]
    Corrupt(String),
    /// Index serialization failed.
    #[error("material index encoding failed: {0}")]
    Encoding(#[from] bincode::Error),
}

/// A single material index entry.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct MaterialIndex(Vec<String>);

impl MaterialIndex {
    fn load(bytes: &[u8]) -> Result<Self, SecretsError> {
        let text = std::str::from_utf8(bytes)
            .map_err(|err| SecretsError::Corrupt(format!("material index is not utf-8: {err}")))?;
        let decoded = decode_base58_multibase(text)
            .map_err(|err| SecretsError::Corrupt(format!("material index multibase: {err}")))?;
        Ok(Self(bincode::deserialize(&decoded)?))
    }

    fn store(&self) -> Result<Vec<u8>, SecretsError> {
        let encoded = bincode::serialize(&self.0)?;
        Ok(encode_base58_multibase(&encoded).into_bytes())
    }

    fn add(&mut self, id: &str) {
        if !self.0.iter().any(|existing| existing == id) {
            self.0.push(id.to_string());
        }
    }

    fn remove(&mut self, id: &str) {
        self.0.retain(|existing| existing != id);
    }
}

/// Identity for a provisioned repository checkout.
#[derive(Debug, Clone)]
pub struct RepoIdentity {
    pub iroh_secret_key: iroh::SecretKey,
    pub iroh_public_key: iroh::PublicKey,
}

pub struct SecretRepo {
    store: Option<Arc<keyring_core::CredentialStore>>,
}

impl SecretRepo {
    const KEYRING_USERNAME: &'static str = "iroh_secret_key_v1";

    fn spawn_drop_thread<T: Send + 'static>(value: T) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || drop(value))
    }

    fn drop_off_runtime<T: Send + 'static>(value: T) {
        // The Linux keyring backend can tear down zbus state in `Drop`, and that
        // must happen off any Tokio runtime thread.
        Self::spawn_drop_thread(value)
            .join()
            .expect(ERROR_IMPOSSIBLE);
    }

    pub async fn boot() -> Res<Self> {
        Ok(Self {
            store: Some(Self::resolve_store().await?),
        })
    }

    /// Test/bootstrap constructor that bypasses the platform selection while
    /// reusing the exact store-resolution logic of [`Self::boot`].
    pub async fn boot_with_store(store: Arc<keyring_core::CredentialStore>) -> Res<Self> {
        Ok(Self { store: Some(store) })
    }

    async fn resolve_store() -> Res<Arc<keyring_core::CredentialStore>> {
        // `cfg(test)` is only set for this crate's own unit tests. Integration/e2e
        // tests build `secrets_rs` as a normal dependency, so we also honor CI
        // and the `test-support` feature here.
        let store: Arc<keyring_core::CredentialStore> =
            if cfg!(test) || cfg!(feature = "test-support") {
                static TEST_STORE: tokio::sync::OnceCell<Arc<keyring_core::mock::Store>> =
                    tokio::sync::OnceCell::const_new();
                Arc::clone(
                    TEST_STORE
                        .get_or_try_init(|| async { keyring_core::mock::Store::new() })
                        .await?,
                ) as _
            } else {
                tokio::task::spawn_blocking(Self::platform_store)
                    .await
                    .expect(ERROR_TOKIO)?
            };

        Ok(store)
    }

    #[cfg(target_os = "linux")]
    fn platform_store() -> Res<Arc<keyring_core::CredentialStore>> {
        match zbus_secret_service_keyring_store::Store::new() {
            Ok(sec) => Ok(sec as Arc<keyring_core::CredentialStore>),
            Err(_) => {
                tracing::warn!(
                    "secret-service keyring unavailable, \
                        falling back to kernel keyring"
                );
                linux_keyutils_keyring_store::Store::new()
                    .map(|sec| sec as Arc<keyring_core::CredentialStore>)
                    .map_err(|err| eyre::eyre!(err).wrap_err("kernel keyring unavailable"))
            }
        }
    }

    #[cfg(target_os = "android")]
    fn platform_store() -> Res<Arc<keyring_core::CredentialStore>> {
        android_native_keyring_store::Store::new()
            .map(|sec| sec as Arc<keyring_core::CredentialStore>)
            .map_err(|err| eyre::eyre!(err).wrap_err("android keyring unavailable"))
    }

    #[cfg(target_os = "windows")]
    fn platform_store() -> Res<Arc<keyring_core::CredentialStore>> {
        windows_native_keyring_store::Store::new()
            .map(|sec| sec as Arc<keyring_core::CredentialStore>)
            .map_err(|err| eyre::eyre!(err).wrap_err("windows keyring unavailable"))
    }

    #[cfg(any(target_os = "macos", target_os = "ios"))]
    fn platform_store() -> Res<Arc<keyring_core::CredentialStore>> {
        apple_native_keyring_store::keychain::Store::new()
            .map(|sec| sec as Arc<keyring_core::CredentialStore>)
            .map_err(|err| eyre::eyre!(err).wrap_err("apple keychain unavailable"))
    }

    #[cfg(not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "windows",
        target_os = "macos",
        target_os = "ios"
    )))]
    fn platform_store() -> Res<Arc<keyring_core::CredentialStore>> {
        eyre::bail!("no keyring backend for this platform")
    }

    fn store(&self) -> Arc<keyring_core::CredentialStore> {
        Arc::clone(self.store.as_ref().expect(ERROR_IMPOSSIBLE))
    }

    pub async fn load_identity(&self, checkout_id: &str) -> Res<Option<RepoIdentity>> {
        let store = self.store();
        let user = format!("daybook.checkout.{checkout_id}.{}", Self::KEYRING_USERNAME);
        tokio::task::spawn_blocking(move || {
            let entry = store
                .build("daybook", &user, None)
                .wrap_err("failed to create keyring entry")?;
            let secret = match entry.get_password() {
                Err(keyring_core::Error::NoEntry) => return Ok(None),
                Err(err) => {
                    return Err(eyre::eyre!(err))
                        .wrap_err("failed reading iroh secret key from keyring");
                }
                Ok(secret) => {
                    let secret = decode_base58_multibase(&secret)
                        .wrap_err("error decode bs58 secret")?;
                    if secret.len() != 32 {
                        eyre::bail!("secret corruption, bad length");
                    }
                    let mut bytes = [0_u8; 32];
                    bytes.copy_from_slice(&secret);
                    iroh::SecretKey::from_bytes(&bytes)
                }
            };
            let public = secret.public();
            Ok(Some(RepoIdentity {
                iroh_secret_key: secret,
                iroh_public_key: public,
            }))
        })
        .await
        .expect(ERROR_TOKIO)
    }

    pub async fn set_identity(
        &self,
        checkout_id: &str,
        secret: iroh::SecretKey,
    ) -> Res<RepoIdentity> {
        let store = self.store();
        let user = format!("daybook.checkout.{checkout_id}.{}", Self::KEYRING_USERNAME);
        tokio::task::spawn_blocking(move || {
            let entry = store
                .build("daybook", &user, None)
                .wrap_err("failed to create keyring entry")?;
            entry
                .set_password(&encode_base58_multibase(secret.to_bytes()))
                .wrap_err("failed setting keyring secret from provisioned clone identity")?;
            let public = secret.public();
            Ok(RepoIdentity {
                iroh_secret_key: secret,
                iroh_public_key: public,
            })
        })
        .await
        .expect(ERROR_TOKIO)
    }

    pub async fn stop(mut self) -> Res<()> {
        let store = self.store.take().expect(ERROR_IMPOSSIBLE);
        tokio::task::spawn_blocking(move || Self::drop_off_runtime(store))
            .await
            .expect(ERROR_TOKIO);
        Ok(())
    }

    async fn material_entry(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<keyring_core::Entry, SecretsError> {
        validate_name(namespace)?;
        validate_name(id)?;
        let store = self.store();
        Ok(store.build(MATERIAL_SERVICE, &format!("{namespace}.{id}"), None)?)
    }

    /// Store an opaque material blob under `{namespace}.{id}`.
    ///
    /// Overwrites any existing payload for the same namespace/id pair
    /// (writes are idempotent for content-addressed callers).
    /// Store an opaque material blob under `{namespace}.{id}`.
    ///
    /// Overwrites any existing payload for the same namespace/id pair
    /// (writes are idempotent for content-addressed callers).
    pub async fn put_blob(
        &self,
        namespace: &str,
        id: &str,
        bytes: &[u8],
    ) -> Result<(), SecretsError> {
        if bytes.len() > MAX_BLOB_BYTES {
            return Err(SecretsError::BlobTooLarge {
                id: format!("{namespace}.{id}"),
                len: bytes.len(),
                max: MAX_BLOB_BYTES,
            });
        }
        let entry = self.material_entry(namespace, id).await?;
        let encoded = encode_base58_multibase(bytes);
        tokio::task::spawn_blocking(move || {
            entry
                .set_password(&encoded)
                .map_err(SecretsError::from)
        })
        .await
        .expect(ERROR_TOKIO)
    }

    /// Load an opaque material blob previously stored with
    /// [`Self::put_blob`]. Returns `Ok(None)` when absent.
    pub async fn get_blob(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<Option<Vec<u8>>, SecretsError> {
        let entry = self.material_entry(namespace, id).await?;
        let secret = tokio::task::spawn_blocking(move || entry.get_password())
            .await
            .expect(ERROR_TOKIO);
        let decoded = match secret {
            Err(keyring_core::Error::NoEntry) => return Ok(None),
            Err(err) => return Err(SecretsError::from(err)),
            Ok(secret) => decode_base58_multibase(&secret).map_err(|err| {
                SecretsError::Corrupt(format!("material blob {namespace}.{id}: {err}"))
            })?,
        };
        Ok(Some(decoded))
    }

    /// Delete an opaque material blob. Absent blobs delete to `Ok(())`,
    /// mirroring the filesystem storage's tolerant `remove_file`.
    pub async fn delete_blob(&self, namespace: &str, id: &str) -> Result<(), SecretsError> {
        let entry = self.material_entry(namespace, id).await?;
        tokio::task::spawn_blocking(move || {
            match entry.delete_credential() {
                // A NoEntry deletion is a no-op, same as a missing file.
                Err(keyring_core::Error::NoEntry) | Ok(()) => Ok(()),
                Err(err) => Err(SecretsError::from(err)),
            }
        })
        .await
        .expect(ERROR_TOKIO)
    }

    async fn update_index<F>(&self, namespace: &str, update: F) -> Result<(), SecretsError>
    where
        F: FnOnce(&mut MaterialIndex) + Send + 'static,
    {
        let current = match self.get_blob(namespace, MATERIAL_INDEX_ID).await? {
            Some(bytes) => MaterialIndex::load(&bytes)?,
            None => MaterialIndex(Vec::new()),
        };
        let mut index = current;
        update(&mut index);
        let bytes = index.store()?;
        self.put_blob(namespace, MATERIAL_INDEX_ID, &bytes).await
    }

    /// Register `id` in the namespace's material index so
    /// [`Self::list_blob_ids`] surfaces it. Idempotent.
    pub async fn add_to_index(&self, namespace: &str, id: &str) -> Result<(), SecretsError> {
        // Capture owned strings so the index update closure is 'static.
        let namespace = namespace.to_string();
        let id = id.to_string();
        self.update_index(&namespace, move |index| index.add(id.as_str())).await
    }

    /// Unregister `id` from the namespace's material index. Idempotent; does
    /// not touch the blob itself (see [`Self::delete_blob`]).
    pub async fn remove_from_index(&self, namespace: &str, id: &str) -> Result<(), SecretsError> {
        // Capture owned strings so the index update closure is 'static.
        let namespace = namespace.to_string();
        let id = id.to_string();
        self.update_index(&namespace, move |index| index.remove(id.as_str())).await
    }

    /// All blob ids registered under `namespace`, in registration order.
    pub async fn list_blob_ids(&self, namespace: &str) -> Result<Vec<String>, SecretsError> {
        match self.get_blob(namespace, MATERIAL_INDEX_ID).await? {
            Some(bytes) => Ok(MaterialIndex::load(&bytes)?.0),
            None => Ok(Vec::new()),
        }
    }
}

fn validate_name(part: &str) -> Result<(), SecretsError> {
    let valid = !part.is_empty()
        && part.len() <= 120
        && part
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'));
    if valid {
        Ok(())
    } else {
        Err(SecretsError::InvalidName(part.to_string()))
    }
}

impl Drop for SecretRepo {
    fn drop(&mut self) {
        if let Some(store) = self.store.take() {
            Self::spawn_drop_thread(store);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct AssertDroppedOffRuntime;

    impl Drop for AssertDroppedOffRuntime {
        fn drop(&mut self) {
            assert!(
                tokio::runtime::Handle::try_current().is_err(),
                "keyring store was dropped on a tokio runtime thread"
            );
        }
    }

    #[test]
    fn drop_helper_runs_off_runtime() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        runtime.block_on(async {
            SecretRepo::drop_off_runtime(AssertDroppedOffRuntime);
        });
    }

    async fn test_repo() -> SecretRepo {
        SecretRepo::boot().await.expect("mock boot")
    }

    #[tokio::test]
    async fn blob_round_trips_and_deletes() -> Result<(), SecretsError> {
        let repo = test_repo().await;
        let namespace = "testns";
        let bytes = b"secret material".to_vec();

        assert!(repo.get_blob(namespace, "b1").await?.is_none());
        repo.put_blob(namespace, "b1", &bytes).await?;
        assert_eq!(repo.get_blob(namespace, "b1").await?, Some(bytes));
        repo.delete_blob(namespace, "b1").await?;
        // Deleting an absent blob stays idempotent.
        repo.delete_blob(namespace, "b1").await?;
        assert!(repo.get_blob(namespace, "b1").await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn blob_overwrite_is_last_write_wins() -> Result<(), SecretsError> {
        let repo = test_repo().await;
        repo.put_blob("testns", "b2", b"first").await?;
        repo.put_blob("testns", "b2", b"second").await?;
        assert_eq!(repo.get_blob("testns", "b2").await?, Some(b"second".to_vec()));
        Ok(())
    }

    #[tokio::test]
    async fn oversized_blob_is_rejected() {
        let repo = test_repo().await;
        let oversized = vec![0u8; MAX_BLOB_BYTES + 1];
        let error = repo.put_blob("testns", "big", &oversized).await.unwrap_err();
        assert!(matches!(error, SecretsError::BlobTooLarge { .. }));
    }

    #[tokio::test]
    async fn invalid_names_are_rejected() {
        let repo = test_repo().await;
        let error = repo.put_blob("bad/namespace", "id", b"x").await.unwrap_err();
        assert!(matches!(error, SecretsError::InvalidName(_)));

        let error = repo.put_blob("ns", "bad id with spaces", b"x").await.unwrap_err();
        assert!(matches!(error, SecretsError::InvalidName(_)));
    }

    #[tokio::test]
    async fn index_round_trips_and_updates() -> Result<(), SecretsError> {
        let repo = test_repo().await;
        let namespace = "indextest";
        repo.add_to_index(namespace, "a").await?;
        repo.add_to_index(namespace, "b").await?;
        // Re-adding is idempotent.
        repo.add_to_index(namespace, "a").await?;
        let listed = repo.list_blob_ids(namespace).await?;
        assert_eq!(listed, vec!["a".to_string(), "b".to_string()]);

        repo.remove_from_index(namespace, "a").await?;
        let listed = repo.list_blob_ids(namespace).await?;
        assert_eq!(listed, vec!["b".to_string()]);

        // Empty namespace lists cleanly.
        assert!(repo.list_blob_ids("freshns").await?.is_empty());
        Ok(())
    }
}