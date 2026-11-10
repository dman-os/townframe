//! Publisher signatures on plug envelopes (ADR 028 §2–§3).
//!
//! A version is signed by the DID that owns its plug id's namespace, and every node checks that
//! signature itself before it accepts the version, so provenance never depends on how the plug
//! was delivered or on who relayed it.
//!
//! **What is signed.** The signature is a member of the envelope ([`Plug::signature`]) but is not
//! part of what [`Plug::revision`] hashes: the revision is the canonical JSON of the envelope
//! *without* its signature. The signed message is [`SIGNING_DOMAIN`] followed by those 32 revision
//! bytes. Two consequences follow. The revision stays the immutability key, so attaching or
//! replacing a signature never makes a version "different content". And the signature covers
//! every member of the envelope, unknown ones included, because the revision does. The domain
//! prefix keeps a signature made for a plug from being valid as any other signed structure under
//! the same key, which matters because the node signs with its own long-lived key.
//!
//! **Which keys verify.** Only `did:key` carries its public key in the identifier, so it is the
//! only method that can be checked without network resolution. It is restricted to Ed25519
//! (multicodec `0xed`, varint `[0xed, 0x01]`, then the 32 key bytes, as multibase base58btc),
//! the one curve the rest of this system uses. A namespace of any other method is refused
//! ([`SignatureError::UnsupportedMethod`]) until resolvers exist: verification fails closed
//! rather than trusting a signature nobody could check.
//!
//! **Wire shape.** `signature: {alg: "ed25519", value: <multibase base58btc of the 64 signature
//! bytes>}`. `alg` is spelled out rather than implied by the DID so a later algorithm is an
//! additive value, not a reinterpretation of existing bytes, and a verifier that does not know it
//! refuses instead of guessing.

use crate::Plug;
use big_cabinet_core::datum::Extra;
use ed25519_dalek::Signature;
use ed25519_dalek::VerifyingKey;
use serde::Deserialize;
use serde::Serialize;

/// The algorithm name [`PlugSignature::alg`] carries for Ed25519.
pub const ED25519: &str = "ed25519";

/// Prepended to the revision bytes before signing; see the module documentation.
pub const SIGNING_DOMAIN: &[u8] = b"org.example.daybook.plug.signature.v1\0";

/// The multicodec varint of an Ed25519 public key, as `did:key` prefixes it.
const ED25519_PUB_MULTICODEC: [u8; 2] = [0xed, 0x01];

/// A publisher's signature over an envelope's revision.
#[macros::datum]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlugSignature {
    pub alg: String,
    /// The signature bytes as multibase base58btc.
    pub value: String,
    #[serde(flatten)]
    pub extra: Extra,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum SignatureError {
    #[error("the envelope carries no signature")]
    Missing,
    #[error("unsupported DID method for verification: `{0}`")]
    UnsupportedMethod(String),
    #[error("unsupported did:key key type (only Ed25519 is verifiable): {0}")]
    UnsupportedKey(String),
    #[error("unsupported signature algorithm `{0}`")]
    UnsupportedAlgorithm(String),
    #[error("malformed signature or key: {0}")]
    Malformed(String),
    #[error("the signature does not verify against `{0}`")]
    Invalid(String),
}

/// The `did:key` of an Ed25519 public key.
pub fn did_key(public_key: &[u8; 32]) -> String {
    let mut bytes = Vec::with_capacity(34);
    bytes.extend_from_slice(&ED25519_PUB_MULTICODEC);
    bytes.extend_from_slice(public_key);
    format!("did:key:{}", utils_rs::hash::encode_base58_multibase(bytes))
}

/// The Ed25519 key a `did:key` names. The method check comes first so the caller's error says
/// "unsupported method" for a `did:web` rather than a parse failure.
fn did_key_public_key(namespace: &str) -> Result<VerifyingKey, SignatureError> {
    let method = namespace
        .strip_prefix("did:")
        .and_then(|rest| rest.split_once(':'))
        .map(|(method, _)| method)
        .expect("a plug namespace is a syntactically valid DID");
    if method != "key" {
        return Err(SignatureError::UnsupportedMethod(method.to_owned()));
    }
    let encoded = namespace
        .strip_prefix("did:key:")
        .expect("the method is `key`");
    let bytes = utils_rs::hash::decode_base58_multibase(encoded)
        .map_err(|error| SignatureError::Malformed(format!("did:key identifier: {error}")))?;
    let key = bytes
        .strip_prefix(&ED25519_PUB_MULTICODEC)
        .ok_or_else(|| SignatureError::UnsupportedKey("multicodec is not ed25519-pub".into()))?;
    let key: &[u8; 32] = key
        .try_into()
        .map_err(|_| SignatureError::Malformed("an Ed25519 key is 32 bytes".into()))?;
    VerifyingKey::from_bytes(key)
        .map_err(|error| SignatureError::Malformed(format!("not a curve point: {error}")))
}

impl Plug {
    /// The bytes a publisher signs: [`SIGNING_DOMAIN`] and this envelope's revision.
    pub fn signing_message(&self) -> Vec<u8> {
        let mut message = SIGNING_DOMAIN.to_vec();
        message.extend_from_slice(&self.revision().0);
        message
    }

    /// Signs the envelope, replacing any signature it carried. `sign` receives
    /// [`Plug::signing_message`] and returns the 64 Ed25519 signature bytes; taking a function
    /// lets the node sign without handing its key out. Nothing here checks that the signer is the
    /// namespace's key: [`Plug::verify_signature`] does, wherever the envelope is accepted.
    pub fn sign_with(&mut self, sign: impl FnOnce(&[u8]) -> [u8; 64]) {
        let signature = sign(&self.signing_message());
        self.signature = Some(PlugSignature {
            alg: ED25519.to_owned(),
            value: utils_rs::hash::encode_base58_multibase(signature),
            extra: Default::default(),
        });
    }

    /// Checks the signature against the key the namespace DID names.
    pub fn verify_signature(&self) -> Result<(), SignatureError> {
        let signature = self.signature.as_ref().ok_or(SignatureError::Missing)?;
        if signature.alg != ED25519 {
            return Err(SignatureError::UnsupportedAlgorithm(signature.alg.clone()));
        }
        let key = did_key_public_key(&self.id.namespace)?;
        let bytes = utils_rs::hash::decode_base58_multibase(&signature.value)
            .map_err(|error| SignatureError::Malformed(format!("signature value: {error}")))?;
        let bytes: &[u8; 64] = bytes
            .as_slice()
            .try_into()
            .map_err(|_| SignatureError::Malformed("an Ed25519 signature is 64 bytes".into()))?;
        key.verify_strict(&self.signing_message(), &Signature::from_bytes(bytes))
            .map_err(|_| SignatureError::Invalid(self.id.namespace.clone()))
    }
}
