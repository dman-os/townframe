//! The actor registry's own rules: path hierarchy, URL shape, key custody, and
//! the node-signed attribution certificate that binds an actor's UUID, key, and
//! URL.
//!
//! Actor and node signing seeds live in the authority's own tables, sealed with
//! ChaCha20-Poly1305 under one wrapping key held in the OS keyring. Creating an
//! actor is therefore one SQLite transaction: there is no window in which a key
//! exists without its registry row, so there is nothing to reconcile after a
//! crash.

use super::cap::Chain;
use super::store::{self, ActorRow, SealedKey};
use super::{Actor, CapId, Rights};
use super::{ActorPublicKey, AuthorityError, NODE_SERVICE};
use big_cabinet_core::dmeta::{Attribution, Bytes, MultibaseKey, SignatureBytes, Uuid7};
use ed25519_dalek::{Signer, SigningKey};
use rand::RngCore;
use sqlx::SqliteConnection;
use zeroize::Zeroizing;

const ACTOR_URL_SCHEME: &str = "db+actor://";

pub(super) async fn recognize(
    conn: &mut SqliteConnection,
    presented: &Chain,
    right: Rights,
) -> Result<Uuid7, AuthorityError> {
    let actor = store::associated_actor(conn, presented.cap.id)
        .await?
        .ok_or(AuthorityError::UnrecognizedActor)?;
    presented.require(right)?;
    Ok(actor)
}

pub(super) async fn issue(
    conn: &mut SqliteConnection,
    parent: &Chain,
    issuer: Uuid7,
    row: &ActorRow,
    rights: Rights,
) -> Result<CapId, AuthorityError> {
    if let Some(id) = store::actor_cap(conn, row.uuid, &parent.cap.id, rights).await? {
        return Ok(id);
    }
    let issued = parent.child(format!("is actor {}", row.path), rights, issuer)?;
    store::insert_cap(conn, &issued).await?;
    store::associate_actor(conn, issued.id, row.uuid).await?;
    Ok(issued.id)
}

pub(super) fn key_text(public_key: &ActorPublicKey) -> Result<MultibaseKey, AuthorityError> {
    MultibaseKey::try_from(public_key.as_slice()).map_err(Into::into)
}

/// `db+actor://<node key>/<path>`. The operator's path is empty, so its URL is
/// the node root, and every other actor's path names its whole ancestry.
pub(super) fn actor_url(node: &ActorPublicKey, path: &str) -> Result<String, AuthorityError> {
    Ok(format!("{ACTOR_URL_SCHEME}{}/{path}", key_text(node)?))
}

/// A child's path is its parent's path plus exactly one segment, so a path
/// names its parent and no two parents can claim the same child. A segment is
/// a non-empty run of ASCII letters, digits, `-`, `_`, and `.`, other than `.`
/// and `..`, so it reads the same in a URL as in the registry.
pub(super) fn check_child_path(parent: &str, child: &str) -> Result<(), AuthorityError> {
    let segment = if parent.is_empty() {
        Some(child)
    } else {
        child
            .strip_prefix(parent)
            .and_then(|rest| rest.strip_prefix('/'))
    };
    let valid = segment.is_some_and(|segment| {
        !segment.is_empty()
            && segment != "."
            && segment != ".."
            && segment
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
    });
    if valid {
        Ok(())
    } else {
        Err(AuthorityError::InvalidActorPath)
    }
}

/// The associated data a sealed seed is bound to: the row that owns it.
fn seal_label(owner: &str) -> Vec<u8> {
    format!("{NODE_SERVICE}/key/{owner}").into_bytes()
}

pub(super) fn generate_key() -> SigningKey {
    let mut seed = Zeroizing::new([0_u8; 32]);
    rand::rng().fill_bytes(seed.as_mut());
    SigningKey::from_bytes(&seed)
}

pub(super) fn seal(
    wrapping: &[u8; 32],
    owner: &str,
    key: &SigningKey,
) -> Result<SealedKey, AuthorityError> {
    let (ciphertext, nonce) =
        secrets_rs::encrypt_blob(wrapping, &seal_label(owner), key.as_bytes())?;
    Ok(SealedKey { ciphertext, nonce })
}

/// Open a sealed seed and check it is the key the row says it is, so a row
/// whose public key and sealed seed disagree cannot sign under either.
pub(super) fn unseal(
    wrapping: &[u8; 32],
    owner: &str,
    sealed: &SealedKey,
    public_key: &ActorPublicKey,
) -> Result<SigningKey, AuthorityError> {
    let seed = Zeroizing::new(secrets_rs::decrypt_blob(
        wrapping,
        &seal_label(owner),
        &sealed.ciphertext,
        &sealed.nonce,
    )?);
    let seed: &[u8; 32] = seed.as_slice().try_into().map_err(|_| {
        AuthorityError::StateCorrupt(format!("sealed key of {owner} is not 32 bytes"))
    })?;
    let key = SigningKey::from_bytes(seed);
    if key.verifying_key().as_bytes() != public_key {
        return Err(AuthorityError::StateCorrupt(format!(
            "sealed key of {owner} does not match its public key"
        )));
    }
    Ok(key)
}

pub(super) fn signature(key: &SigningKey, message: &[u8]) -> SignatureBytes {
    SignatureBytes::try_from(key.sign(message).to_bytes().as_slice())
        .expect("an Ed25519 signature is 64 bytes")
}

/// Issue the node-signed attribution certificate for one actor binding. These
/// are the exact bytes dmeta stores as `ActorMetadata.delegation`; the
/// certificate describes the binding and grants nothing (ADR 015 §5).
pub(super) fn issue_certificate(
    node: &SigningKey,
    actor_uuid: Uuid7,
    actor_key: &ActorPublicKey,
    url: &str,
) -> Result<Bytes, AuthorityError> {
    let attribution = Attribution {
        issuer: key_text(node.verifying_key().as_bytes())?,
        actor_uuid,
        pubkey: key_text(actor_key)?,
        actor_url: url.to_owned(),
    };
    let signature = signature(node, &attribution.signing_bytes()?);
    Ok(attribution.certificate(signature)?)
}

pub(super) fn public_actor(
    node: &ActorPublicKey,
    row: &ActorRow,
    capability: CapId,
) -> Result<Actor, AuthorityError> {
    Ok(Actor {
        uuid: row.uuid,
        url: actor_url(node, &row.path)?,
        public_key: key_text(&row.public_key)?,
        certificate: Bytes::from(row.certificate.clone()),
        parent_actor: row.parent,
        capability,
    })
}
