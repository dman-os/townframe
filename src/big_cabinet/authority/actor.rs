use super::{ActorPublicKey, AuthorityError};
use big_cabinet_core::dmeta::{Attribution, Bytes, MultibaseKey, SignatureBytes, Uuid7};
use secrets_rs::SigningKeyHandle;
use std::sync::Arc;

#[allow(dead_code)]
pub(super) struct ActorRecord {
    pub(super) public_key: ActorPublicKey,
    pub(super) key_name: String,
    pub(super) uuid: Option<String>,
    pub(super) url: Option<String>,
    pub(super) parent_uuid: Option<String>,
    pub(super) certificate: Option<Vec<u8>>,
}

pub(super) fn actor_record(
    row: Option<sqlx::sqlite::SqliteRow>,
) -> Result<Option<ActorRecord>, AuthorityError> {
    use sqlx::Row;
    row.map(|row| {
        Ok(ActorRecord {
            public_key: super::fixed_bytes(row.try_get("public_key")?)?,
            key_name: row.try_get("key_name")?,
            uuid: row.try_get("uuid")?,
            url: row.try_get("url")?,
            parent_uuid: row.try_get("parent_uuid")?,
            certificate: row.try_get("certificate")?,
        })
    })
    .transpose()
}

pub(super) fn node_segment(node_public_key: &ActorPublicKey) -> String {
    MultibaseKey::try_from(node_public_key.as_slice())
        .expect("node key is 32 bytes")
        .to_string()
}

pub(super) fn actor_url(node_public_key: &ActorPublicKey, path: &str) -> String {
    format!(
        "db+actor://{}/{}",
        node_segment(node_public_key),
        path.trim_matches('/')
    )
}

pub(super) async fn issue_certificate(
    node_signer: &SigningKeyHandle,
    node_public_key: &ActorPublicKey,
    actor_uuid: Uuid7,
    actor_key: &ActorPublicKey,
    url: &str,
) -> Result<Bytes, AuthorityError> {
    let attribution = Attribution {
        issuer: MultibaseKey::try_from(node_public_key.as_slice())?,
        actor_uuid,
        pubkey: MultibaseKey::try_from(actor_key.as_slice())?,
        actor_url: url.to_owned(),
    };
    let signature = node_signer.sign(&attribution.signing_bytes()?).await?;
    let signature = SignatureBytes::try_from(signature.as_slice())?;
    Ok(attribution.certificate(signature)?)
}

pub(super) fn check_child_path(parent_path: &str, child_path: &str) -> Result<(), AuthorityError> {
    let child = child_path.trim_matches('/');
    if child.is_empty() || child.contains("//") {
        return Err(AuthorityError::InvalidActorPath);
    }
    if parent_path.is_empty() {
        return Ok(());
    }
    let parent = parent_path.trim_matches('/');
    if child == parent || !child.starts_with(&format!("{parent}/")) {
        return Err(AuthorityError::InvalidActorPath);
    }
    Ok(())
}

pub(super) async fn open_actor_signer(
    store: &Arc<secrets_rs::SecretStore>,
    key_name: &str,
    expected: &ActorPublicKey,
) -> Result<SigningKeyHandle, AuthorityError> {
    Ok(
        SigningKeyHandle::open(Arc::clone(store), super::NODE_SERVICE, key_name)
            .await?
            .bind_public_key(*expected),
    )
}
