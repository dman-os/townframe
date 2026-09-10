//! DEK (data-encryption-key) storage backed by OS keyrings.
//!
//! Owns the [`SecretRepo`] abstraction: repo identities for checkout
//! provisioning, plus per-material-type data encryption keys (DEKs) used to
//! encrypt secret blobs at rest in sqlite.
//!
//! DEKs are 32-byte random keys addressable by a string `dek_id` and an
//! integer `version`. Versions make rotation incremental: a new version of a
//! DEK id can be created without re-encrypting every blob at once — blobs
//! record the version that encrypted them and old versions stay readable
//! until the last referencing blob is migrated.
//!
//! The keyring is a *key source*, not a blob store: it holds raw DEKs (and
//! identities). Secret material itself lives encrypted in the sqlite layer
//! via [`encrypt_blob`]/[`decrypt_blob`] (ChaCha20-Poly1305, random 12-byte
//! nonce per blob, no AAD).
//!
//! Keyring entries are base58-multibase encoded so every backend sees
//! printable text, mirroring how identities are persisted. Keyring calls are
//! synchronous/blocking; async surfaces delegate to
//! `tokio::task::spawn_blocking`, and store teardown happens off any Tokio
//! runtime thread (the zbus backend must not drop on a runtime thread).

use std::sync::Arc;

use chacha20poly1305::aead::Aead;
use chacha20poly1305::{ChaCha20Poly1305, Key, KeyInit, Nonce};
use rand::RngCore;
use utils_rs::expect_tags::{ERROR_IMPOSSIBLE, ERROR_TOKIO};
use utils_rs::hash::{decode_base58_multibase, encode_base58_multibase};
use utils_rs::prelude::WrapErr;
use utils_rs::prelude::eyre;

pub type Res<T> = eyre::Result<T>;

/// Service under which all DEK entries are stored (distinct from the
/// `"daybook"` identity service so the two namespaces never collide).
const MATERIAL_SERVICE: &str = "daybook.material.v1";

/// Errors surfaced by the DEK / AEAD API.
#[derive(Debug, thiserror::Error)]
pub enum SecretsError {
    /// `dek_id` contained characters keyring usernames must not.
    #[error("invalid secret storage name: {0}")]
    InvalidName(String),
    /// The backing keyring store rejected the operation.
    #[error("keyring error: {0}")]
    Keyring(#[from] keyring_core::Error),
    /// A stored DEK or version index could not be decoded.
    #[error("stored material is corrupt: {0}")]
    Corrupt(String),
    /// The requested DEK is not present in the configured secret store.
    #[error("stored material is missing: {0}")]
    Missing(String),
    /// DEK version index serialization failed.
    #[error("material index encoding failed: {0}")]
    Encoding(#[from] bincode::Error),
    /// AEAD encrypt/decrypt failed.
    #[error("crypto error: {0}")]
    Crypto(String),
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
                tracing::warn!("using in-memory keyring store");
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
                    let secret =
                        decode_base58_multibase(&secret).wrap_err("error decode bs58 secret")?;
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

    // ---- DEK API ----

    /// Read a DEK without creating it. Missing entries are returned as `None`.
    pub async fn get_dek(
        &self,
        dek_id: &str,
        version: u64,
    ) -> Result<Option<[u8; 32]>, SecretsError> {
        validate_name(dek_id)?;
        let dek_id = dek_id.to_string();
        let store = self.store();
        let username = dek_username(&dek_id, version);
        tokio::task::spawn_blocking(move || {
            let entry = store
                .build(MATERIAL_SERVICE, &username, None)
                .map_err(SecretsError::from)?;
            match entry.get_password() {
                Ok(secret) => decode_dek(&secret, &dek_id, version).map(Some),
                Err(keyring_core::Error::NoEntry) => Ok(None),
                Err(err) => Err(SecretsError::from(err)),
            }
        })
        .await
        .expect(ERROR_TOKIO)
    }

    /// The DEK for `(dek_id, version)`, generating and persisting it in the
    /// keyring on first use.
    ///
    /// Deterministic: the same `(dek_id, version)` always yields the same 32
    /// bytes. A fresh version starts a rotation — callers write new blobs
    /// with it while old blobs stay readable via their recorded version.
    pub async fn get_or_create_dek(
        &self,
        dek_id: &str,
        version: u64,
    ) -> Result<[u8; 32], SecretsError> {
        validate_name(dek_id)?;
        let dek_id = dek_id.to_string();
        let store = self.store();
        let username = dek_username(&dek_id, version);
        let dek = tokio::task::spawn_blocking(move || {
            let entry = store
                .build(MATERIAL_SERVICE, &username, None)
                .map_err(SecretsError::from)?;
            match entry.get_password() {
                Ok(secret) => decode_dek(&secret, &dek_id, version),
                Err(keyring_core::Error::NoEntry) => {
                    let mut dek = [0u8; 32];
                    rand::rng().fill_bytes(&mut dek);
                    entry
                        .set_password(&encode_base58_multibase(dek))
                        .map_err(SecretsError::from)?;
                    let mut versions = load_dek_version_list(&*store, &dek_id)?;
                    if !versions.contains(&version) {
                        versions.push(version);
                        versions.sort_unstable();
                        save_dek_version_list(&*store, &dek_id, &versions)?;
                    }
                    Ok(dek)
                }
                Err(err) => Err(SecretsError::from(err)),
            }
        })
        .await
        .expect(ERROR_TOKIO)?;
        Ok(dek)
    }

    /// Start a rotation of `dek_id`: create the next version (max existing
    /// version + 1, or 0 for a fresh id) and return `(version, dek)`.
    pub async fn create_next_dek_version(
        &self,
        dek_id: &str,
    ) -> Result<(u64, [u8; 32]), SecretsError> {
        validate_name(dek_id)?;
        let dek_id = dek_id.to_string();
        let store = self.store();
        let dek_id_next = dek_id.clone();
        let next = tokio::task::spawn_blocking(move || {
            let versions = load_dek_version_list(&*store, &dek_id_next)?;
            Ok::<u64, SecretsError>(versions.iter().max().map_or(0, |version| version + 1))
        })
        .await
        .expect(ERROR_TOKIO)?;
        let dek = self.get_or_create_dek(&dek_id, next).await?;
        Ok((next, dek))
    }

    /// All versions of `dek_id` that exist in the keyring, ascending.
    pub async fn list_dek_versions(&self, dek_id: &str) -> Result<Vec<u64>, SecretsError> {
        validate_name(dek_id)?;
        let dek_id = dek_id.to_string();
        let store = self.store();
        tokio::task::spawn_blocking(move || load_dek_version_list(&*store, &dek_id))
            .await
            .expect(ERROR_TOKIO)
    }
}

/// Keyring username for a DEK version entry: `{dek_id}.v{version}`.
fn dek_username(dek_id: &str, version: u64) -> String {
    format!("{dek_id}.v{version}")
}

/// Keyring username holding the version list for `dek_id`.
fn version_index_username(dek_id: &str) -> String {
    format!("{dek_id}.__versions__")
}

fn decode_dek(secret: &str, dek_id: &str, version: u64) -> Result<[u8; 32], SecretsError> {
    let decoded = decode_base58_multibase(secret)
        .map_err(|err| SecretsError::Corrupt(format!("dek {dek_id} v{version}: {err}")))?;
    if decoded.len() != 32 {
        return Err(SecretsError::Corrupt(format!(
            "dek {dek_id} v{version}: bad length {}",
            decoded.len()
        )));
    }
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&decoded);
    Ok(bytes)
}

fn load_dek_version_list(
    store: &keyring_core::CredentialStore,
    dek_id: &str,
) -> Result<Vec<u64>, SecretsError> {
    let entry = store.build(MATERIAL_SERVICE, &version_index_username(dek_id), None)?;
    match entry.get_password() {
        Err(keyring_core::Error::NoEntry) => Ok(Vec::new()),
        Err(err) => Err(SecretsError::from(err)),
        Ok(secret) => {
            let decoded = decode_base58_multibase(&secret).map_err(|err| {
                SecretsError::Corrupt(format!("dek version index {dek_id}: {err}"))
            })?;
            bincode::deserialize(&decoded).map_err(SecretsError::from)
        }
    }
}

fn save_dek_version_list(
    store: &keyring_core::CredentialStore,
    dek_id: &str,
    versions: &[u64],
) -> Result<(), SecretsError> {
    let encoded = bincode::serialize(versions)?;
    let entry = store.build(MATERIAL_SERVICE, &version_index_username(dek_id), None)?;
    entry
        .set_password(&encode_base58_multibase(&encoded))
        .map_err(SecretsError::from)
}

/// Encrypt `plaintext` under `dek` (ChaCha20-Poly1305, random 12-byte nonce,
/// no AAD). Returns `(ciphertext, nonce)` for the caller to persist alongside
/// the DEK id/version.
pub fn encrypt_blob(dek: &[u8; 32], plaintext: &[u8]) -> Result<(Vec<u8>, [u8; 12]), SecretsError> {
    let cipher = ChaCha20Poly1305::new(Key::from_slice(dek));
    let mut nonce = [0u8; 12];
    rand::rng().fill_bytes(&mut nonce);
    let ciphertext = cipher
        .encrypt(Nonce::from_slice(&nonce), plaintext)
        .map_err(|err| SecretsError::Crypto(err.to_string()))?;
    Ok((ciphertext, nonce))
}

/// Decrypt `ciphertext` produced by [`encrypt_blob`] under the same `dek` and
/// `nonce`.
pub fn decrypt_blob(
    dek: &[u8; 32],
    ciphertext: &[u8],
    nonce: &[u8; 12],
) -> Result<Vec<u8>, SecretsError> {
    let cipher = ChaCha20Poly1305::new(Key::from_slice(dek));
    cipher
        .decrypt(Nonce::from_slice(nonce), ciphertext)
        .map_err(|err| SecretsError::Crypto(err.to_string()))
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
    async fn dek_is_deterministic_per_id_and_version() -> Res<()> {
        let repo = test_repo().await;
        let v0_first = repo.get_or_create_dek("local-secret", 0).await?;
        let v0_second = repo.get_or_create_dek("local-secret", 0).await?;
        assert_eq!(v0_first, v0_second);
        let v1 = repo.get_or_create_dek("local-secret", 1).await?;
        assert_ne!(v0_first, v1);
        Ok(())
    }

    #[tokio::test]
    async fn reading_missing_dek_does_not_create_one() -> Res<()> {
        let repo = test_repo().await;
        assert!(repo.get_dek("read-only", 0).await?.is_none());
        assert!(repo.list_dek_versions("read-only").await?.is_empty());

        let expected = repo.get_or_create_dek("read-only", 0).await?;
        assert_eq!(repo.get_dek("read-only", 0).await?, Some(expected));
        Ok(())
    }

    #[tokio::test]
    async fn next_version_starts_at_zero_and_increments() -> Res<()> {
        let repo = test_repo().await;
        let (v0, dek0) = repo.create_next_dek_version("reservation").await?;
        assert_eq!(v0, 0);
        let (v1, dek1) = repo.create_next_dek_version("reservation").await?;
        assert_eq!(v1, 1);
        assert_ne!(dek0, dek1);
        assert_eq!(repo.list_dek_versions("reservation").await?, vec![0, 1]);
        Ok(())
    }

    #[tokio::test]
    async fn version_list_tracks_explicit_versions() -> Res<()> {
        let repo = test_repo().await;
        assert!(repo.list_dek_versions("prekey-sidecar").await?.is_empty());
        repo.get_or_create_dek("prekey-sidecar", 2).await?;
        repo.get_or_create_dek("prekey-sidecar", 5).await?;
        assert_eq!(repo.list_dek_versions("prekey-sidecar").await?, vec![2, 5]);
        Ok(())
    }

    #[tokio::test]
    async fn encrypt_decrypt_round_trips() -> Res<()> {
        let repo = test_repo().await;
        let dek = repo.get_or_create_dek("local-secret", 0).await?;
        let plaintext = b"secret material".to_vec();
        let (ciphertext, nonce) = encrypt_blob(&dek, &plaintext)?;
        assert_ne!(ciphertext, plaintext);
        assert_eq!(decrypt_blob(&dek, &ciphertext, &nonce)?, plaintext);
        Ok(())
    }

    #[tokio::test]
    async fn decrypt_with_wrong_key_or_tampering_fails() -> Res<()> {
        let repo = test_repo().await;
        let dek = repo.get_or_create_dek("local-secret", 0).await?;
        let other = repo.get_or_create_dek("local-secret", 1).await?;
        let (ciphertext, nonce) = encrypt_blob(&dek, b"secret material")?;
        assert!(decrypt_blob(&other, &ciphertext, &nonce).is_err());

        let mut tampered = ciphertext.clone();
        tampered[0] ^= 0xff;
        assert!(decrypt_blob(&dek, &tampered, &nonce).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn invalid_names_are_rejected() {
        let repo = test_repo().await;
        let error = repo.get_or_create_dek("bad/name", 0).await.unwrap_err();
        assert!(matches!(error, SecretsError::InvalidName(_)));
    }
}
