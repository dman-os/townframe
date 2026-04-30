use crate::interlude::*;

#[derive(Debug, Clone)]
pub struct RepoIdentity {
    pub iroh_secret_key: iroh::SecretKey,
    pub iroh_public_key: iroh::PublicKey,
}

pub struct SecretRepo {
    store: Arc<keyring_core::CredentialStore>,
}

impl SecretRepo {
    const KEYRING_USERNAME: &'static str = "iroh_secret_key_v1";

    pub async fn boot() -> Res<Self> {
        let store: Arc<keyring_core::CredentialStore> = if std::cfg!(test) {
            static TEST_STORE: tokio::sync::OnceCell<Arc<keyring_core::mock::Store>> =
                tokio::sync::OnceCell::const_new();
            TEST_STORE
                .get_or_try_init(|| async { keyring_core::mock::Store::new() })
                .await?
                .clone()
        } else {
            tokio::task::spawn_blocking(move || {
                let store;
                #[cfg(target_os = "linux")]
                {
                    store = zbus_secret_service_keyring_store::Store::new()?;
                }
                #[cfg(target_os = "android")]
                {
                    store = android_native_keyring_store::Store::new()?;
                }
                #[cfg(target_os = "windows")]
                {
                    store = windows_native_keyring_store::Store::new()?;
                }
                #[cfg(any(target_os = "macos", target_os = "ios"))]
                {
                    store = apple_native_keyring_store::keychain::Store::new()?;
                }
                eyre::Ok(store)
            })
            .await
            .expect(ERROR_TOKIO)?
        };
        Ok(Self { store })
    }
    pub async fn load_identity(&self, checkout_id: &str) -> Res<Option<RepoIdentity>> {
        let store = self.store.clone();
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
        let store = self.store.clone();
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
}
