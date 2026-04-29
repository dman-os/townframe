use crate::interlude::*;

#[derive(Debug, Clone)]
pub struct RepoIdentity {
    pub iroh_secret_key: iroh::SecretKey,
    pub iroh_public_key: iroh::PublicKey,
}

pub struct SecretRepo;

impl SecretRepo {
    const KEYRING_USERNAME: &'static str = "iroh_secret_key_v1";

    pub async fn load_identity(checkout_id: &str) -> Res<Option<RepoIdentity>> {
        let service_name = format!("daybook.checkout.{checkout_id}");
        tokio::task::spawn_blocking(move || {
            let entry =
                keyring::Entry::new(&service_name, Self::KEYRING_USERNAME).wrap_err_with(|| {
                    format!("failed creating keyring entry for iroh secret key ({service_name})")
                })?;
            let secret = match entry.get_secret() {
                Err(keyring::Error::NoEntry) => return Ok(None),
                Err(err) => {
                    return Err(eyre::eyre!(err))
                        .wrap_err("failed reading iroh secret key from keyring");
                }
                Ok(secret) => {
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

    pub async fn set_identity(checkout_id: &str, secret: iroh::SecretKey) -> Res<RepoIdentity> {
        let service_name = format!("daybook.checkout.{checkout_id}");
        tokio::task::spawn_blocking(move || {
            let entry =
                keyring::Entry::new(&service_name, Self::KEYRING_USERNAME).wrap_err_with(|| {
                    format!(
                    "failed creating keyring entry for provisioned clone identity ({service_name})"
                )
                })?;
            entry
                .set_secret(&secret.to_bytes())
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

fn is_keyring_disabled() -> bool {
    std::env::var("DAYB_DISABLE_KEYRING")
        .map(|value| value == "1")
        .unwrap_or(false)
}

fn has_persistent_keyring_backend() -> bool {
    match keyring::default::default_credential_builder().persistence() {
        keyring::credential::CredentialPersistence::UntilDelete => true,
        _ => false,
    }
}
