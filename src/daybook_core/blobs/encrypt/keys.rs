//! Key material for the cipherblob codec, and how a caller reaches it.
//!
//! [`MasterKey`] is the 32-octet input-keying material for one cipherblob
//! family. It is deliberately inert: this module neither persists nor
//! transmits it. Persistence lives in the document layer — [`JwkOct`] is the
//! JWK shape the `org.example.daybook.jwk` facet stores it as — and
//! Resolution belongs to the caller; the codec never reads documents or
//! decides which key reference is authorized.
//!
//! The RFC 8188 salt is *not* part of the key binding. It is derived per
//! representation as a pure function of (master key, plaintext digest) — see
//! [`MasterKey::salt_for`] — which makes the salt unpersisted state:
//! recovering it means re-deriving it when encrypting, or reading it from the
//! ciphertext's authenticated header when decrypting. The per-representation
//! derivation allows independent encryptors to share a master key without
//! coordinating random nonces. Distinct content/framing derives distinct
//! salts except for a cryptographic collision in the 128-bit salt; repeated
//! identical content/framing intentionally reproduces the same ciphertext.

use crate::interlude::*;

use rand::RngCore;

use iroh_blobs::Hash;

use super::codec::{MASTER_KEY_LEN, SALT_LEN};
use super::params::EncodingParams;
use super::Res;

/// Master key for one cipherblob family: the 32-octet input-keying material
/// the record cipher is derived from. The caller owns persistence (the facet
/// layer stores it as a [`JwkOct`]); nothing here reads or writes keys.
///
/// The salt is deliberately not part of this binding — see the module docs.
#[derive(Clone, PartialEq, Eq)]
pub struct MasterKey(pub(crate) [u8; MASTER_KEY_LEN]);

impl MasterKey {
    pub fn random() -> Self {
        let mut key = [0u8; MASTER_KEY_LEN];
        rand::rng().fill_bytes(&mut key);
        Self(key)
    }

    /// The salt for a plaintext of this representation: a pure function of
    /// (master key, plaintext digest, framing). The framing participates in
    /// the derivation because it shapes every record's plaintext: without it,
    /// the same JWK applied to the same plaintext under two different
    /// record-size or padding choices would reuse each record's (CEK, nonce)
    /// pair across *different* record contents — a GCM confidentiality and
    /// authentication break. Including content and framing in the derivation
    /// prevents that deterministic reuse; residual collisions have the
    /// cryptographic bound of the 128-bit derived salt.
    pub(crate) fn salt_for(&self, p_hash: &Hash, framing: &EncodingParams) -> [u8; SALT_LEN] {
        let mut hasher = blake3::Hasher::new_derive_key("daybook.cipherblob.salt.v2");
        hasher.update(&self.0);
        hasher.update(p_hash.as_bytes());
        hasher.update(&framing.record_size.to_be_bytes());
        hasher.update([framing.padding.domain_byte()].as_slice());
        let mut salt = [0u8; SALT_LEN];
        salt.copy_from_slice(&hasher.finalize().as_bytes()[..SALT_LEN]);
        salt
    }
}

/// JWK base64url, no padding (RFC 7515 §2); 32-octet keys encode to exactly
/// 43 characters. Decoding uses the strict canonical form: non-canonical
/// trailing bits are rejected by `data_encoding`.
fn b64url_encode_32(key: &[u8; MASTER_KEY_LEN]) -> String {
    data_encoding::BASE64URL_NOPAD.encode(key)
}

fn b64url_decode_32(encoded: &str) -> Res<[u8; MASTER_KEY_LEN]> {
    eyre::ensure!(
        encoded.len() == 43,
        "expected 43 unpadded base64url chars for a 32-octet key, got {}",
        encoded.len()
    );
    let bytes = data_encoding::BASE64URL_NOPAD
        .decode(encoded.as_bytes())
        .map_err(|err| eyre::eyre!("invalid unpadded base64url: {err}"))?;
    let out: [u8; MASTER_KEY_LEN] = bytes
        .try_into()
        .expect("43 unpadded base64url chars decode to 32 octets");
    Ok(out)
}

/// The wire form of a stored cipherblob master key: a plain RFC 7517
/// oct-sequence JWK whose `k` carries the key, base64url-encoded without
/// padding.
///
/// This is the serde shape of the `org.example.daybook.jwk` facet, so a facet
/// and this codec cannot disagree about how a key is spelled.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct JwkOct {
    pub kty: String,
    pub k: String,
}

impl JwkOct {
    pub fn from_master_key(key: &MasterKey) -> Self {
        Self {
            kty: "oct".to_owned(),
            k: b64url_encode_32(&key.0),
        }
    }

    pub fn to_master_key(&self) -> Res<MasterKey> {
        eyre::ensure!(self.kty == "oct", "unsupported JWK key type {:?}", self.kty);
        let oct = b64url_decode_32(&self.k)?;
        Ok(MasterKey(oct))
    }
}
