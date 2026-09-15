//! Daybook-specific identity storage built on the standalone [`secrets_rs`]
//! keyring abstraction.

use crate::interlude::*;

use secrets_rs::SecretStore;
const IDENTITY_SERVICE: &str = "daybook";
const IDENTITY_USERNAME_PREFIX: &str = "daybook.checkout.";
const IDENTITY_USERNAME_SUFFIX: &str = ".iroh_secret_key_v1";

#[derive(Debug, Clone)]
pub struct RepoIdentity {
    pub iroh_secret_key: iroh::SecretKey,
    pub iroh_public_key: iroh::PublicKey,
}

pub async fn load_identity(store: &SecretStore, checkout_id: &str) -> Res<Option<RepoIdentity>> {
    let username = identity_username(checkout_id);
    let Some(secret) = store.get_secret(IDENTITY_SERVICE, &username).await? else {
        return Ok(None);
    };
    if secret.len() != 32 {
        eyre::bail!("secret corruption, bad length");
    }
    let mut bytes = [0_u8; 32];
    bytes.copy_from_slice(&secret);
    let secret = iroh::SecretKey::from_bytes(&bytes);
    let public = secret.public();
    Ok(Some(RepoIdentity {
        iroh_secret_key: secret,
        iroh_public_key: public,
    }))
}

pub async fn set_identity(
    store: &SecretStore,
    checkout_id: &str,
    secret: iroh::SecretKey,
) -> Res<RepoIdentity> {
    let username = identity_username(checkout_id);
    store
        .set_secret(IDENTITY_SERVICE, &username, &secret.to_bytes())
        .await?;
    let public = secret.public();
    Ok(RepoIdentity {
        iroh_secret_key: secret,
        iroh_public_key: public,
    })
}

fn identity_username(checkout_id: &str) -> String {
    format!("{IDENTITY_USERNAME_PREFIX}{checkout_id}{IDENTITY_USERNAME_SUFFIX}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn identity_round_trips() -> Res<()> {
        let store = SecretStore::boot().await?;
        let secret = iroh::SecretKey::generate();
        let expected = secret.public();
        let identity = set_identity(&store, "test-checkout", secret).await?;
        assert_eq!(identity.iroh_public_key, expected);
        let loaded = load_identity(&store, "test-checkout")
            .await?
            .expect("identity");
        assert_eq!(loaded.iroh_public_key, expected);
        Ok(())
    }

    #[tokio::test]
    async fn missing_identity_is_not_created() -> Res<()> {
        let store = SecretStore::boot().await?;
        assert!(load_identity(&store, "missing-checkout").await?.is_none());
        Ok(())
    }
}
