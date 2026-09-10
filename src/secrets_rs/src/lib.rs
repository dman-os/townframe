//! Generic secret storage backed by operating-system keyrings.
//!
//! [`SecretStore`] provides a small standalone API for opaque byte secrets.
//! Consumers choose their own service and entry names; this crate does not
//! attach meaning to either value. Keyring calls are synchronous/blocking, so
//! async operations delegate to `spawn_blocking`. Store teardown happens off
//! any Tokio runtime thread because some keyring backends own runtime-bound
//! resources.

use std::sync::Arc;

use chacha20poly1305::aead::Aead;
use chacha20poly1305::{ChaCha20Poly1305, Key, KeyInit, Nonce};
use rand::RngCore;
use utils_rs::expect_tags::{ERROR_IMPOSSIBLE, ERROR_TOKIO};
use utils_rs::hash::{decode_base58_multibase, encode_base58_multibase};
use utils_rs::prelude::eyre;

pub type Res<T> = eyre::Result<T>;

/// Errors surfaced by the keyring and optional local encryption helpers.
#[derive(Debug, thiserror::Error)]
pub enum SecretsError {
    /// A service or entry name contains characters unsupported by the keyring.
    #[error("invalid secret storage name: {0}")]
    InvalidName(String),
    /// The backing keyring store rejected the operation.
    #[error("keyring error: {0}")]
    Keyring(#[from] keyring_core::Error),
    /// A stored secret could not be decoded.
    #[error("stored secret is corrupt: {0}")]
    Corrupt(String),
    /// A requested secret is not present in the configured store.
    #[error("stored secret is missing: {0}")]
    Missing(String),
    /// AEAD encrypt/decrypt failed.
    #[error("crypto error: {0}")]
    Crypto(String),
}

/// Standalone storage for opaque byte secrets in an operating-system keyring.
pub struct SecretStore {
    store: Option<Arc<keyring_core::CredentialStore>>,
}

impl SecretStore {
    fn spawn_drop_thread<T: Send + 'static>(value: T) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || drop(value))
    }

    fn drop_off_runtime<T: Send + 'static>(value: T) {
        Self::spawn_drop_thread(value)
            .join()
            .expect(ERROR_IMPOSSIBLE);
    }

    /// Open the platform keyring backend.
    pub async fn boot() -> Res<Self> {
        Ok(Self {
            store: Some(Self::resolve_store().await?),
        })
    }

    /// Test/bootstrap constructor using an explicitly selected keyring store.
    pub async fn boot_with_store(store: Arc<keyring_core::CredentialStore>) -> Res<Self> {
        Ok(Self { store: Some(store) })
    }

    async fn resolve_store() -> Res<Arc<keyring_core::CredentialStore>> {
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
                    "secret-service keyring unavailable, falling back to kernel keyring"
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

    /// Read an opaque secret. Missing entries are returned as `None`.
    pub async fn get_secret(
        &self,
        service: &str,
        name: &str,
    ) -> Result<Option<Vec<u8>>, SecretsError> {
        validate_name(service)?;
        validate_name(name)?;
        let service = service.to_string();
        let name = name.to_string();
        let store = self.store();
        tokio::task::spawn_blocking(move || {
            let entry = store.build(&service, &name, None)?;
            match entry.get_password() {
                Ok(secret) => decode_secret(&secret, &service, &name).map(Some),
                Err(keyring_core::Error::NoEntry) => Ok(None),
                Err(err) => Err(SecretsError::from(err)),
            }
        })
        .await
        .expect(ERROR_TOKIO)
    }

    /// Persist an opaque secret, replacing an existing value for the entry.
    pub async fn set_secret(
        &self,
        service: &str,
        name: &str,
        secret: &[u8],
    ) -> Result<(), SecretsError> {
        validate_name(service)?;
        validate_name(name)?;
        let service = service.to_string();
        let name = name.to_string();
        let encoded = encode_base58_multibase(secret);
        let store = self.store();
        tokio::task::spawn_blocking(move || {
            let entry = store.build(&service, &name, None)?;
            entry.set_password(&encoded).map_err(SecretsError::from)
        })
        .await
        .expect(ERROR_TOKIO)
    }

    /// Return an existing opaque secret or create a random secret of `len`
    /// bytes when the entry is absent.
    pub async fn get_or_create_secret(
        &self,
        service: &str,
        name: &str,
        len: usize,
    ) -> Result<Vec<u8>, SecretsError> {
        if len == 0 {
            return Err(SecretsError::Corrupt(
                "secret length must not be zero".into(),
            ));
        }
        if let Some(secret) = self.get_secret(service, name).await? {
            return Ok(secret);
        }
        let mut secret = vec![0; len];
        rand::rng().fill_bytes(&mut secret);
        self.set_secret(service, name, &secret).await?;
        Ok(secret)
    }

    /// Stop the keyring store away from the Tokio runtime.
    pub async fn stop(mut self) -> Res<()> {
        let store = self.store.take().expect(ERROR_IMPOSSIBLE);
        tokio::task::spawn_blocking(move || Self::drop_off_runtime(store))
            .await
            .expect(ERROR_TOKIO);
        Ok(())
    }
}

fn decode_secret(secret: &str, service: &str, name: &str) -> Result<Vec<u8>, SecretsError> {
    decode_base58_multibase(secret)
        .map_err(|err| SecretsError::Corrupt(format!("{service}/{name}: {err}")))
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

/// Encrypt `plaintext` under a 32-byte key using ChaCha20-Poly1305 and a
/// random 12-byte nonce. `aad` authenticates the caller's metadata without
/// including it in the returned ciphertext. Returns `(ciphertext, nonce)` for
/// the caller to persist alongside its own key metadata.
pub fn encrypt_blob(
    key: &[u8; 32],
    aad: &[u8],
    plaintext: &[u8],
) -> Result<(Vec<u8>, [u8; 12]), SecretsError> {
    let cipher = ChaCha20Poly1305::new(Key::from_slice(key));
    let mut nonce = [0u8; 12];
    rand::rng().fill_bytes(&mut nonce);
    let ciphertext = cipher
        .encrypt(
            Nonce::from_slice(&nonce),
            chacha20poly1305::aead::Payload {
                msg: plaintext,
                aad,
            },
        )
        .map_err(|err| SecretsError::Crypto(err.to_string()))?;
    Ok((ciphertext, nonce))
}

/// Decrypt `ciphertext` produced by [`encrypt_blob`] with the same key, AAD,
/// and nonce.
pub fn decrypt_blob(
    key: &[u8; 32],
    aad: &[u8],
    ciphertext: &[u8],
    nonce: &[u8; 12],
) -> Result<Vec<u8>, SecretsError> {
    let cipher = ChaCha20Poly1305::new(Key::from_slice(key));
    cipher
        .decrypt(
            Nonce::from_slice(nonce),
            chacha20poly1305::aead::Payload {
                msg: ciphertext,
                aad,
            },
        )
        .map_err(|err| SecretsError::Crypto(err.to_string()))
}

impl Drop for SecretStore {
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
            SecretStore::drop_off_runtime(AssertDroppedOffRuntime);
        });
    }

    async fn test_store() -> SecretStore {
        SecretStore::boot().await.expect("mock boot")
    }

    #[tokio::test]
    async fn secret_is_deterministic_per_entry() -> Res<()> {
        let store = test_store().await;
        let first = store
            .get_or_create_secret("tests", "deterministic", 32)
            .await?;
        let second = store
            .get_or_create_secret("tests", "deterministic", 32)
            .await?;
        assert_eq!(first, second);
        Ok(())
    }

    #[tokio::test]
    async fn reading_missing_secret_does_not_create_one() -> Res<()> {
        let store = test_store().await;
        assert!(store.get_secret("tests", "read-only").await?.is_none());
        let expected = store.get_or_create_secret("tests", "read-only", 32).await?;
        assert_eq!(
            store.get_secret("tests", "read-only").await?,
            Some(expected)
        );
        Ok(())
    }

    #[tokio::test]
    async fn encrypt_decrypt_round_trips() -> Res<()> {
        let store = test_store().await;
        let key: [u8; 32] = store
            .get_or_create_secret("tests", "encryption", 32)
            .await?
            .try_into()
            .expect("requested key length");
        let plaintext = b"secret material".to_vec();
        let aad = b"tests/encryption";
        let (ciphertext, nonce) = encrypt_blob(&key, aad, &plaintext)?;
        assert_ne!(ciphertext, plaintext);
        assert_eq!(decrypt_blob(&key, aad, &ciphertext, &nonce)?, plaintext);
        Ok(())
    }

    #[tokio::test]
    async fn decrypt_with_wrong_key_or_tampering_fails() -> Res<()> {
        let store = test_store().await;
        let key: [u8; 32] = store
            .get_or_create_secret("tests", "key-a", 32)
            .await?
            .try_into()
            .expect("requested key length");
        let other: [u8; 32] = store
            .get_or_create_secret("tests", "key-b", 32)
            .await?
            .try_into()
            .expect("requested key length");
        let aad = b"tests/tamper";
        let (ciphertext, nonce) = encrypt_blob(&key, aad, b"secret material")?;
        assert!(decrypt_blob(&other, aad, &ciphertext, &nonce).is_err());

        let mut tampered = ciphertext.clone();
        tampered[0] ^= 0xff;
        assert!(decrypt_blob(&key, aad, &tampered, &nonce).is_err());
        assert!(decrypt_blob(&key, b"wrong-aad", &ciphertext, &nonce).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn invalid_names_are_rejected() {
        let store = test_store().await;
        let error = store
            .get_or_create_secret("bad/name", "key", 32)
            .await
            .unwrap_err();
        assert!(matches!(error, SecretsError::InvalidName(_)));
    }
}
