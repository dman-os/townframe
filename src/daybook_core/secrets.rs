use crate::interlude::*;

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
        // `cfg(test)` is only set for this crate's own unit tests. Integration/e2e
        // tests build `daybook_core` as a normal dependency, so we also honor CI
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
                tokio::task::spawn_blocking(move || {
                    #[cfg(target_os = "linux")]
                    {
                        match zbus_secret_service_keyring_store::Store::new() {
                            Ok(sec) => Ok(sec as Arc<keyring_core::CredentialStore>),
                            Err(_) => {
                                tracing::warn!(
                                    "secret-service keyring unavailable, \
                                     falling back to kernel keyring"
                                );
                                linux_keyutils_keyring_store::Store::new()
                                    .map(|sec| sec as Arc<keyring_core::CredentialStore>)
                                    .map_err(|err| {
                                        eyre::eyre!(err).wrap_err("kernel keyring unavailable")
                                    })
                            }
                        }
                    }
                    #[cfg(target_os = "android")]
                    {
                        android_native_keyring_store::Store::new()
                            .map(|sec| sec as Arc<keyring_core::CredentialStore>)
                            .map_err(|err| eyre::eyre!(err).wrap_err("android keyring unavailable"))
                    }
                    #[cfg(target_os = "windows")]
                    {
                        windows_native_keyring_store::Store::new()
                            .map(|sec| sec as Arc<keyring_core::CredentialStore>)
                            .map_err(|err| eyre::eyre!(err).wrap_err("windows keyring unavailable"))
                    }
                    #[cfg(any(target_os = "macos", target_os = "ios"))]
                    {
                        apple_native_keyring_store::keychain::Store::new()
                            .map(|sec| sec as Arc<keyring_core::CredentialStore>)
                            .map_err(|err| eyre::eyre!(err).wrap_err("apple keychain unavailable"))
                    }
                })
                .await
                .expect(ERROR_TOKIO)?
            };

        Ok(Self { store: Some(store) })
    }

    pub async fn load_identity(&self, checkout_id: &str) -> Res<Option<RepoIdentity>> {
        let store = Arc::clone(self.store.as_ref().expect(ERROR_IMPOSSIBLE));
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
                    let secret = utils_rs::hash::decode_base58_multibase(&secret)
                        .wrap_err("error decode bs58 secret")?;
                    if secret.len() != 32 {
                        eyre::bail!("secret corruption, bad length")
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
        let store = Arc::clone(self.store.as_ref().expect(ERROR_IMPOSSIBLE));
        let user = format!("daybook.checkout.{checkout_id}.{}", Self::KEYRING_USERNAME);
        tokio::task::spawn_blocking(move || {
            let entry = store
                .build("daybook", &user, None)
                .wrap_err("failed to create keyring entry")?;
            entry
                .set_password(&utils_rs::hash::encode_base58_multibase(secret.to_bytes()))
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
}

impl Drop for SecretRepo {
    fn drop(&mut self) {
        if let Some(store) = self.store.take() {
            let _ = Self::spawn_drop_thread(store);
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
}
