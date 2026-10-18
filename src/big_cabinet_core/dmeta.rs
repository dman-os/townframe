//! Document metadata and signed facet-claim primitives.
//!
//! This module deliberately does not know about Automerge or authority storage. A repository
//! reconstructs an ordinary claim from the state at the change that introduced its signature; this
//! module owns the stable bytes that are signed, the compact metadata carried by dmeta, and the
//! distinction between missing evidence and proven forgery.
//!
//! Dmeta is an ordinary facet — `org.example.daybook.dmeta/main` — and the document repository is
//! the only writer of it. Its shape is the one in `docs/wiki/docs.md`: aliases live in one forward
//! index, identities are multibase text, and branch relationships are not duplicated here. A branch
//! is a weak link recorded by the branch facets (ADR 007); dmeta signs those facets, it does not
//! restate them.
//!
//! A claim authenticates the value in its **native storage form**, not its canonical JSON. The
//! JSON spelling of a stored value is a function of the installed declaration — a float leaves as a
//! decimal string, a counter as a whole number, bytes as `{ "$bytes": "…" }` — so a signature over
//! JSON could not be checked on a node that has not installed the facet's schema. The native value
//! is self-describing, so verification needs no schema at all: an absent schema leaves the facet
//! uninterpreted, never unverifiable. [`encode_native`] is that encoding. The claim type only ever
//! holds the resulting CID; the repository is what reads the value back out of the transaction and
//! passes those bytes here.

mod native;
mod verification;

use jiff::Timestamp;
pub use native::{FiniteF64, NativeValue, encode_native};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::str::FromStr;
use uuid::Uuid;
pub use verification::{
    Evidence, SignatureChecks, SignedFacetClaim, VerificationState, document_verification,
    fold_checks,
};

use crate::reference::FacetKey;

pub const DMETA_FACET_TAG: &str = "org.example.daybook.dmeta";
const CLAIM_DOMAIN: &[u8] = b"daybook.facet-claim.v1\0";
const CLAIM_ID_DOMAIN: &[u8] = b"daybook.facet-claim-id.v1\0";
const CID_VERSION_RAW_BLAKE3: [u8; 4] = [0x01, 0x55, 0x1e, 0x20];
const CID_LENGTH: usize = 36;

/// The document identity shared with BigRepo: arbitrary bytes with canonical multibase text.
pub use big_sync_core::ObjKey as DocumentId;

/// Identity allocated before initial content is persisted and publication begins.
///
/// The document's own id is not a field of [`Dmeta`]: the enclosing document already knows it, and
/// a field inside the facet would be copied verbatim onto a branch, where it would name the origin
/// rather than the branch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentIdentity {
    pub id: DocumentId,
    pub created_at: Timestamp,
}

/// An actor as dmeta records it: a public key, the node-signed attribution that binds the node to
/// that key, and when the binding was made.
///
/// The delegation is opaque here. Its shape is the capability token, which belongs to the authority
/// repository, and dmeta records authorship rather than the actor's current grants — so this module
/// carries the bytes and never interprets them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ActorMetadata {
    pub actor_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegation: Option<Bytes>,
    pub created_at: Timestamp,
}

/// One alias assigned to a facet slot, and when it was assigned.
///
/// `key` is the whole facet key, tag and instance together. Concurrent creators of the same slot
/// each record an alias, and the record is the durable evidence of that concurrency after Automerge
/// no longer exposes the original conflict. The alias selects nothing: a facet's content is the
/// merged value at its key, not a value chosen by UUID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct FacetAlias {
    pub key: FacetKey,
    pub created_at: Timestamp,
}

/// The compact provenance facet. Maps are ordered so snapshots and derived evidence are stable.
///
/// There is one index of aliases, `facet_uuids`, keyed by the alias. A per-facet reverse list would
/// be a second copy of the same facts, and the two copies are exactly what a merge can pull apart.
/// The chosen alias for a signature — the lexicographically smallest one visible in the writing
/// transaction — is derived from this index by the repository, which holds the reverse lookup.
#[cfg_attr(feature = "schemars", macros::facet(id = "org.example.daybook.dmeta"))]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Dmeta {
    pub created_at: Timestamp,
    #[serde(default)]
    pub updated_at: Vec<Timestamp>,
    #[serde(default)]
    pub actors: BTreeMap<MultibaseKey, ActorMetadata>,
    #[serde(default)]
    #[cfg_attr(feature = "schemars", schemars(with = "BTreeMap<String, FacetAlias>"))]
    pub facet_uuids: BTreeMap<Uuid, FacetAlias>,
    #[serde(default)]
    pub facets: BTreeMap<FacetKey, FacetMetadata>,
    /// Members a newer dmeta wrote that this declaration does not name. An open facet keeps them,
    /// so a reader that has not been taught a new field does not delete it on the way back.
    #[serde(flatten)]
    pub extra: crate::facet::Extra,
}

impl Dmeta {
    pub fn new(created_at: Timestamp) -> Self {
        Self {
            created_at,
            updated_at: Vec::new(),
            actors: BTreeMap::new(),
            facet_uuids: BTreeMap::new(),
            facets: BTreeMap::new(),
            extra: crate::facet::Extra::new(),
        }
    }

    /// Record an alias without selecting content. Multiple aliases for one key are durable evidence
    /// of concurrent creation even after the conflict that produced them is no longer visible.
    pub fn record_facet_alias(&mut self, uuid: Uuid, alias: FacetAlias) {
        self.facet_uuids.insert(uuid, alias);
    }

    /// Whether more than one creator independently opened this slot.
    pub fn has_concurrent_creation(&self, key: &FacetKey) -> bool {
        self.facet_uuids
            .values()
            .filter(|alias| &alias.key == key)
            .count()
            > 1
    }
}

/// What dmeta records about one facet slot.
///
/// `signatures` is a **current competing set**, not an append-only audit index: a write replaces
/// the entries it observed at its base heads with its own, and concurrent writers can leave several.
/// The list is the shape of that set because a signature has no identity independent of its own
/// bytes — keying a map on a hash of the entry would store the derivation beside its inputs. Which
/// entries a write replaces is decided by the repository against the heads it wrote from; this type
/// only holds the result. Timestamps help display history. They are not signed, and they establish
/// neither identity nor authorization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct FacetMetadata {
    #[serde(default)]
    pub updated_at: Vec<Timestamp>,
    #[serde(default)]
    pub deleted_at: Vec<Timestamp>,
    #[serde(default)]
    pub signatures: Vec<FacetSignature>,
}

/// A signature a branch retained from its origin, where the origin's history is not available to
/// reconstruct the signed bytes from.
///
/// An ordinary signature stores nothing but the actor and the signature bytes: the message is
/// rebuilt from the state at the change that introduced it. A branch is a different document and
/// carries none of that history, so the bytes have to travel with it. `message` is the exact
/// domain-prefixed message the actor signed, and it is authenticated by the signature — a fork
/// that substitutes other bytes fails verification rather than being trusted. `value` is present
/// only when the value the branch copied is not the value that was signed, which happens when a
/// concurrent write landed on the origin before the branch was taken; it is the canonical native
/// bytes of that signed value, so its CID can be checked without presenting the signature as
/// covering the merged result.
///
/// What the witness cannot prove on its own is that the actor was authorized on the origin at the
/// frontier where the signature was made. That frontier lives on the origin document, and an origin
/// that is unavailable leaves the witness pending rather than forged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct InheritedWitness {
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub origin_doc: DocumentId,
    pub message: Bytes,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Bytes>,
}

/// One entry in a facet's competing set of signatures.
///
/// `inherited` is set only for a signature copied onto a branch at its creation. A signature
/// authored on the branch itself has none: its message is reconstructed from the branch's own
/// history, and its origin is the branch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct FacetSignature {
    pub actor_public_key: MultibaseKey,
    pub signature: MultibaseSignature,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inherited: Option<InheritedWitness>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClaimOperation {
    Set,
    Remove,
}

/// The logical fields authenticated by a facet signature.
///
/// `value_cid` is a CIDv1 (`raw` codec, BLAKE3-256) over the canonical encoding of the value in its
/// **native storage form**, read back from the transaction after the content mutation. It is absent
/// only for a removal. The encoding itself belongs to the repository; this type only ever holds the
/// resulting CID, so it cannot drift from the bytes that were stored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FacetClaim {
    pub origin_document: DocumentId,
    pub facet_key: FacetKey,
    pub chosen_uuid: Uuid,
    pub operation: ClaimOperation,
    pub value_cid: Option<Cid>,
    #[serde(default)]
    pub predecessors: Vec<Cid>,
}

/// A CIDv1 with the `raw` codec and a BLAKE3-256 multihash: four header bytes and the 32-byte digest.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Cid([u8; CID_LENGTH]);

impl Cid {
    pub fn as_bytes(&self) -> &[u8; CID_LENGTH] {
        &self.0
    }

    fn from_digest(digest: [u8; 32]) -> Self {
        let mut bytes = [0; CID_LENGTH];
        bytes[..4].copy_from_slice(&CID_VERSION_RAW_BLAKE3);
        bytes[4..].copy_from_slice(&digest);
        Self(bytes)
    }
}

impl Serialize for Cid {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for Cid {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = Cid;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a CIDv1 raw/BLAKE3-256 byte string")
            }

            fn visit_bytes<E: serde::de::Error>(self, bytes: &[u8]) -> Result<Cid, E> {
                Cid::try_from(bytes).map_err(E::custom)
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<Cid, A::Error> {
                let mut bytes = [0; CID_LENGTH];
                for (index, slot) in bytes.iter_mut().enumerate() {
                    *slot = seq
                        .next_element()?
                        .ok_or_else(|| serde::de::Error::invalid_length(index, &"a 36 byte CID"))?;
                }
                if seq.next_element::<u8>()?.is_some() {
                    return Err(serde::de::Error::invalid_length(
                        CID_LENGTH + 1,
                        &"a 36 byte CID",
                    ));
                }
                Cid::try_from(bytes.as_slice()).map_err(serde::de::Error::custom)
            }
        }
        deserializer.deserialize_bytes(Visitor)
    }
}

impl TryFrom<&[u8]> for Cid {
    type Error = DmetaError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        if bytes.len() != CID_LENGTH || bytes[..4] != CID_VERSION_RAW_BLAKE3 {
            return Err(DmetaError::MalformedClaim(
                "a CID is a CIDv1 raw/BLAKE3-256",
            ));
        }
        let mut cid = [0; CID_LENGTH];
        cid.copy_from_slice(bytes);
        Ok(Self(cid))
    }
}

/// An Ed25519 public key as dmeta stores it: base58btc under the multibase `z` prefix, the same text
/// form a document id has. A key is an identity, and an identity in this system is text rather than
/// an array of numbers.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MultibaseKey([u8; 32]);

impl MultibaseKey {
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl TryFrom<&[u8]> for MultibaseKey {
    type Error = DmetaError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let bytes: [u8; 32] = bytes.try_into().map_err(|_| DmetaError::InvalidPublicKey)?;
        Ok(Self(bytes))
    }
}

impl std::fmt::Display for MultibaseKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&utils_rs::hash::encode_base58_multibase(self.0))
    }
}

impl FromStr for MultibaseKey {
    type Err = DmetaError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let bytes = utils_rs::hash::decode_base58_multibase(value)
            .map_err(|_| DmetaError::InvalidPublicKey)?;
        Self::try_from(bytes.as_slice())
    }
}

impl Serialize for MultibaseKey {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for MultibaseKey {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for MultibaseKey {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "MultibaseKey".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::MultibaseKey").into()
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({ "type": "string" })
    }
}

/// An Ed25519 signature, in the same multibase text form as the key that produced it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MultibaseSignature([u8; 64]);

impl MultibaseSignature {
    pub fn as_bytes(&self) -> &[u8; 64] {
        &self.0
    }
}

impl TryFrom<&[u8]> for MultibaseSignature {
    type Error = DmetaError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let bytes: [u8; 64] = bytes.try_into().map_err(|_| DmetaError::InvalidSignature)?;
        Ok(Self(bytes))
    }
}

impl std::fmt::Display for MultibaseSignature {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&utils_rs::hash::encode_base58_multibase(self.0))
    }
}

impl FromStr for MultibaseSignature {
    type Err = DmetaError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let bytes = utils_rs::hash::decode_base58_multibase(value)
            .map_err(|_| DmetaError::InvalidSignature)?;
        Self::try_from(bytes.as_slice())
    }
}

impl Serialize for MultibaseSignature {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for MultibaseSignature {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for MultibaseSignature {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "MultibaseSignature".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::MultibaseSignature").into()
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({ "type": "string" })
    }
}

/// Opaque bytes carried inside dmeta: a delegation, or the message and value a branch inherited.
///
/// These are not identities, so they are not multibase text. They are the protocol bytes object,
/// which is what makes the facet publishable: an array of numbers is not a Lexicon type.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Bytes(Vec<u8>);

impl Bytes {
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl From<Vec<u8>> for Bytes {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}

impl Serialize for Bytes {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut object = serializer.serialize_map(Some(1))?;
        object.serialize_entry("$bytes", &data_encoding::BASE64.encode(&self.0))?;
        object.end()
    }
}

impl<'de> Deserialize<'de> for Bytes {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        let Some(stated) = value.get("$bytes").and_then(serde_json::Value::as_str) else {
            return Err(serde::de::Error::custom(
                "bytes need a $bytes base64 string",
            ));
        };
        if value.as_object().is_none_or(|object| object.len() != 1) {
            return Err(serde::de::Error::custom(
                "bytes only contain the $bytes member",
            ));
        }
        Ok(Self(decode_base64(stated).ok_or_else(|| {
            serde::de::Error::custom(format!("`{stated}` is not base64"))
        })?))
    }
}

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for Bytes {
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> std::borrow::Cow<'static, str> {
        "DmetaBytes".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::Bytes").into()
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        crate::facet::codec_schema(
            crate::facet::Codec::Bytes,
            schemars::json_schema!({
                "type": "object",
                "properties": {
                    "$bytes": {"type": "string", "contentEncoding": "base64"}
                },
                "required": ["$bytes"],
                "additionalProperties": false
            }),
        )
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DmetaError {
    #[error("claim predecessor IDs must be CID bytes")]
    InvalidPredecessor,
    #[error("claim operation and value CID disagree")]
    InvalidClaim,
    #[error("malformed facet claim: {0}")]
    MalformedClaim(&'static str),
    #[error("invalid Ed25519 public key")]
    InvalidPublicKey,
    #[error("invalid Ed25519 signature")]
    InvalidSignature,
}

impl FacetClaim {
    /// Deterministic RFC 8949 array encoding from ADR 015 §4.
    pub fn cbor_fields(&self) -> Result<Vec<u8>, DmetaError> {
        if matches!(self.operation, ClaimOperation::Set) != self.value_cid.is_some() {
            return Err(DmetaError::InvalidClaim);
        }
        let mut out = Vec::new();
        cbor_array(&mut out, 6);
        cbor_text(&mut out, &self.origin_document.to_string());
        cbor_text(&mut out, &self.facet_key.to_string());
        cbor_bytes(&mut out, self.chosen_uuid.as_bytes());
        cbor_uint(
            &mut out,
            u8::from(matches!(self.operation, ClaimOperation::Remove)) as u64,
        );
        match &self.value_cid {
            Some(cid) => cbor_bytes(&mut out, cid.as_bytes()),
            None => out.push(0xf6),
        }
        let mut predecessors = self.predecessors.clone();
        predecessors.sort();
        predecessors.dedup();
        cbor_array(&mut out, predecessors.len() as u64);
        for predecessor in &predecessors {
            cbor_bytes(&mut out, predecessor.as_bytes());
        }
        Ok(out)
    }

    pub fn signing_bytes(&self) -> Result<Vec<u8>, DmetaError> {
        let mut bytes = CLAIM_DOMAIN.to_vec();
        bytes.extend(self.cbor_fields()?);
        Ok(bytes)
    }

    /// Decode the exact deterministic-CBOR field array used by the v1 claim wire.
    ///
    /// This is deliberately stricter than a general CBOR decoder: all six fields have fixed types,
    /// arrays are definite, integers use their shortest representation, predecessor CIDs are
    /// strictly sorted and unique, and no trailing bytes survive.
    pub fn from_cbor_fields(bytes: &[u8]) -> Result<Self, DmetaError> {
        let mut cursor = 0;
        let (major, length) = read_cbor_head_strict(bytes, &mut cursor)?;
        if major != 4 || length != 6 {
            return Err(DmetaError::MalformedClaim(
                "claim must be a six-element array",
            ));
        }

        let origin_text = read_cbor_text_strict(bytes, &mut cursor)?;
        let origin_document = DocumentId::from_str(&origin_text)
            .map_err(|_| DmetaError::MalformedClaim("invalid origin document ID"))?;
        let facet_key_text = read_cbor_text_strict(bytes, &mut cursor)?;
        let facet_key = FacetKey::from_str(&facet_key_text)
            .map_err(|_| DmetaError::MalformedClaim("invalid facet key"))?;
        let uuid_bytes = read_cbor_bytes_strict(bytes, &mut cursor)?;
        let chosen_uuid = Uuid::from_slice(&uuid_bytes)
            .map_err(|_| DmetaError::MalformedClaim("chosen UUID must be 16 bytes"))?;
        let (operation_major, operation_value) = read_cbor_head_strict(bytes, &mut cursor)?;
        if operation_major != 0 || operation_value > 1 {
            return Err(DmetaError::MalformedClaim("operation must be 0 or 1"));
        }
        let operation = if operation_value == 0 {
            ClaimOperation::Set
        } else {
            ClaimOperation::Remove
        };
        let value_cid = if bytes.get(cursor) == Some(&0xf6) {
            cursor += 1;
            None
        } else {
            Some(Cid::try_from(
                read_cbor_bytes_strict(bytes, &mut cursor)?.as_slice(),
            )?)
        };
        if matches!(operation, ClaimOperation::Set) != value_cid.is_some() {
            return Err(DmetaError::InvalidClaim);
        }

        let (predecessor_major, predecessor_count) = read_cbor_head_strict(bytes, &mut cursor)?;
        if predecessor_major != 4 {
            return Err(DmetaError::MalformedClaim("predecessors must be an array"));
        }
        // Every predecessor needs at least a one-byte CBOR bytes head and one complete 36-byte CID.
        // Bound the count by the actual input before converting to usize or reserving, so a hostile
        // count cannot request an unbounded allocation.
        let remaining = bytes.len().saturating_sub(cursor);
        let minimum_predecessor_encoding = 1 + CID_LENGTH as u64;
        if predecessor_count > remaining as u64 / minimum_predecessor_encoding {
            return Err(DmetaError::MalformedClaim(
                "predecessor count exceeds input budget",
            ));
        }
        let mut predecessors = Vec::with_capacity(predecessor_count as usize);
        for _ in 0..predecessor_count {
            let predecessor =
                Cid::try_from(read_cbor_bytes_strict(bytes, &mut cursor)?.as_slice())?;
            if predecessors.last().is_some_and(|last| last >= &predecessor) {
                return Err(DmetaError::MalformedClaim(
                    "predecessors must be sorted and unique",
                ));
            }
            predecessors.push(predecessor);
        }
        if cursor != bytes.len() {
            return Err(DmetaError::MalformedClaim("trailing claim bytes"));
        }
        let claim = Self {
            origin_document,
            facet_key,
            chosen_uuid,
            operation,
            value_cid,
            predecessors,
        };
        if claim.cbor_fields()? != bytes {
            return Err(DmetaError::MalformedClaim("noncanonical claim encoding"));
        }
        Ok(claim)
    }

    /// Decode the complete domain-prefixed message that Ed25519 authenticates.
    pub fn from_signing_bytes(bytes: &[u8]) -> Result<Self, DmetaError> {
        if !bytes.starts_with(CLAIM_DOMAIN) {
            return Err(DmetaError::MalformedClaim("invalid claim domain"));
        }
        Self::from_cbor_fields(&bytes[CLAIM_DOMAIN.len()..])
    }
}

/// The CIDv1 (`raw` codec, BLAKE3-256) of an arbitrary byte string.
///
/// This is the one hashing primitive the claim wire needs. The bytes it is given for a value CID are
/// the canonical native encoding of the facet value, which the repository produces; the bytes for a
/// signature id are the domain-separated claim material below.
pub fn cid_of(bytes: &[u8]) -> Cid {
    Cid::from_digest(*blake3::hash(bytes).as_bytes())
}

fn read_cbor_head_strict(bytes: &[u8], cursor: &mut usize) -> Result<(u8, u64), DmetaError> {
    let byte = *bytes
        .get(*cursor)
        .ok_or(DmetaError::MalformedClaim("truncated CBOR"))?;
    *cursor += 1;
    let major = byte >> 5;
    let additional = byte & 0x1f;
    let (value, width_marker) = match additional {
        0..=23 => (u64::from(additional), 0),
        24 => (
            u64::from(
                *bytes
                    .get(*cursor)
                    .ok_or(DmetaError::MalformedClaim("truncated CBOR"))?,
            ),
            24,
        ),
        25 => {
            let value = u64::from(u16::from_be_bytes(
                bytes
                    .get(*cursor..*cursor + 2)
                    .ok_or(DmetaError::MalformedClaim("truncated CBOR"))?
                    .try_into()
                    .unwrap(),
            ));
            (value, 25)
        }
        26 => {
            let value = u64::from(u32::from_be_bytes(
                bytes
                    .get(*cursor..*cursor + 4)
                    .ok_or(DmetaError::MalformedClaim("truncated CBOR"))?
                    .try_into()
                    .unwrap(),
            ));
            (value, 26)
        }
        _ => return Err(DmetaError::MalformedClaim("unsupported or indefinite CBOR")),
    };
    let width = match additional {
        24 => 1,
        25 => 2,
        26 => 4,
        _ => 0,
    };
    *cursor = cursor
        .checked_add(width)
        .ok_or(DmetaError::MalformedClaim("CBOR length overflow"))?;
    let canonical = match width_marker {
        0 => true,
        24 => value >= 24,
        25 => value > u64::from(u8::MAX),
        26 => value > u64::from(u16::MAX),
        _ => false,
    };
    if !canonical {
        return Err(DmetaError::MalformedClaim("noncanonical CBOR integer"));
    }
    Ok((major, value))
}

fn read_cbor_text_strict(bytes: &[u8], cursor: &mut usize) -> Result<String, DmetaError> {
    let (major, length) = read_cbor_head_strict(bytes, cursor)?;
    if major != 3 {
        return Err(DmetaError::MalformedClaim("expected CBOR text"));
    }
    let end = cursor
        .checked_add(length as usize)
        .ok_or(DmetaError::MalformedClaim("CBOR length overflow"))?;
    let value = String::from_utf8(
        bytes
            .get(*cursor..end)
            .ok_or(DmetaError::MalformedClaim("truncated CBOR"))?
            .to_vec(),
    )
    .map_err(|_| DmetaError::MalformedClaim("invalid UTF-8 text"))?;
    *cursor = end;
    Ok(value)
}

fn read_cbor_bytes_strict(bytes: &[u8], cursor: &mut usize) -> Result<Vec<u8>, DmetaError> {
    let (major, length) = read_cbor_head_strict(bytes, cursor)?;
    if major != 2 {
        return Err(DmetaError::MalformedClaim("expected CBOR bytes"));
    }
    let end = cursor
        .checked_add(length as usize)
        .ok_or(DmetaError::MalformedClaim("CBOR length overflow"))?;
    let value = bytes
        .get(*cursor..end)
        .ok_or(DmetaError::MalformedClaim("truncated CBOR"))?
        .to_vec();
    *cursor = end;
    Ok(value)
}

fn cbor_head(out: &mut Vec<u8>, major: u8, value: u64) {
    if value < 24 {
        out.push((major << 5) | value as u8);
    } else if value <= u8::MAX as u64 {
        out.extend([(major << 5) | 24, value as u8]);
    } else if value <= u16::MAX as u64 {
        out.push((major << 5) | 25);
        out.extend((value as u16).to_be_bytes());
    } else {
        out.push((major << 5) | 26);
        out.extend((value as u32).to_be_bytes());
    }
}
fn cbor_array(out: &mut Vec<u8>, length: u64) {
    cbor_head(out, 4, length);
}
fn cbor_text(out: &mut Vec<u8>, value: &str) {
    cbor_head(out, 3, value.len() as u64);
    out.extend(value.as_bytes());
}
fn cbor_bytes(out: &mut Vec<u8>, value: &[u8]) {
    cbor_head(out, 2, value.len() as u64);
    out.extend(value);
}
fn cbor_uint(out: &mut Vec<u8>, value: u64) {
    cbor_head(out, 0, value);
}

/// Padded or unpadded base64, since the protocol bytes object accepts both.
fn decode_base64(stated: &str) -> Option<Vec<u8>> {
    data_encoding::BASE64
        .decode(stated.as_bytes())
        .or_else(|_| data_encoding::BASE64_NOPAD.decode(stated.as_bytes()))
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "schemars")]
    use crate::facet::FacetShape;
    use ed25519_dalek::{Signer, SigningKey};

    use crate::{FacetReference, ReferenceDocumentId, ReferenceError};

    fn facet_key(value: &str) -> FacetKey {
        FacetKey::from_str(value).unwrap()
    }

    fn cid_with(byte: u8) -> Cid {
        cid_of(&[byte; 32])
    }

    #[test]
    fn aliases_retain_concurrency_without_a_reverse_index() {
        let timestamp = Timestamp::from_second(0).unwrap();
        let mut dmeta = Dmeta::new(timestamp);
        let key = facet_key("org.example.note/main");
        dmeta.record_facet_alias(
            Uuid::from_u128(2),
            FacetAlias {
                key: key.clone(),
                created_at: timestamp,
            },
        );
        dmeta.record_facet_alias(
            Uuid::from_u128(1),
            FacetAlias {
                key: key.clone(),
                created_at: Timestamp::from_second(1).unwrap(),
            },
        );
        assert!(dmeta.has_concurrent_creation(&key));
        assert!(!dmeta.has_concurrent_creation(&facet_key("org.example.note/other")));
        // The chosen alias is the smallest one visible, and it falls out of the index order.
        let chosen = dmeta
            .facet_uuids
            .iter()
            .filter(|(_, alias)| alias.key == key)
            .map(|(uuid, _)| *uuid)
            .min()
            .unwrap();
        assert_eq!(chosen, Uuid::from_u128(1));
    }

    #[cfg(feature = "schemars")]
    #[test]
    fn dmeta_publishes_as_an_ordinary_facet() {
        let declaration =
            crate::facet::Declaration::of::<Dmeta>().expect("dmeta is lexicon-aligned");
        assert_eq!(declaration.facet.id, DMETA_FACET_TAG);
        assert_eq!(
            Dmeta::SHAPE,
            crate::facet::Shape::Object {
                openness: crate::facet::Openness::Open
            }
        );
        let universe = crate::facet::Universe::collect(vec![declaration]);
        let published = crate::facet::Bundle::new(&universe, Vec::new())
            .lexicons()
            .expect("an aligned declaration publishes");
        assert_eq!(published.len(), 1);
        assert_eq!(published[0].file["id"], DMETA_FACET_TAG);
        assert_eq!(published[0].file["defs"]["main"]["type"], "object");
    }

    #[test]
    fn an_actor_key_and_a_signature_round_trip_as_multibase_text() {
        let key = MultibaseKey::try_from([7; 32].as_slice()).unwrap();
        let text = key.to_string();
        assert!(text.starts_with('z'));
        assert_eq!(text.parse::<MultibaseKey>().unwrap(), key);
        assert_eq!(
            serde_json::from_str::<MultibaseKey>(&serde_json::to_string(&key).unwrap()).unwrap(),
            key
        );
        // A truncated or over-long spelling is refused rather than decoded into a different key.
        assert!(text[..text.len() - 1].parse::<MultibaseKey>().is_err());

        let signature = MultibaseSignature::try_from([9; 64].as_slice()).unwrap();
        let signature_text = signature.to_string();
        assert_eq!(
            signature_text.parse::<MultibaseSignature>().unwrap(),
            signature
        );
    }

    #[test]
    fn carried_bytes_are_the_protocol_object_not_an_array_of_numbers() {
        let bytes = Bytes::from(vec![1, 2, 3, 255]);
        let json = serde_json::to_value(&bytes).unwrap();
        assert_eq!(json, serde_json::json!({"$bytes": "AQID/w=="}));
        assert_eq!(serde_json::from_value::<Bytes>(json).unwrap(), bytes);
        // An array of numbers is what `Vec<u8>` would have produced, and it is not bytes.
        assert!(serde_json::from_value::<Bytes>(serde_json::json!([1, 2, 3, 255])).is_err());
    }

    #[test]
    fn claim_cbor_and_ed25519_vectors_are_stable() {
        let claim = FacetClaim {
            origin_document: DocumentId::new(b"doc"),
            facet_key: facet_key("org.example.note/main"),
            chosen_uuid: Uuid::from_u128(1),
            operation: ClaimOperation::Set,
            value_cid: Some(cid_of(br#"{"a":1,"b":2}"#)),
            predecessors: vec![],
        };
        let expected_cbor = [
            0x86, 0x65, b'z', b'a', b'j', b'd', b'k', 0x75, b'o', b'r', b'g', b'.', b'e', b'x',
            b'a', b'm', b'p', b'l', b'e', b'.', b'n', b'o', b't', b'e', b'/', b'm', b'a', b'i',
            b'n', 0x50, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0x58, 0x24, 0x01, 0x55,
            0x1e, 0x20, 0x8e, 0x80, 0x43, 0x9b, 0x77, 0xac, 0x62, 0xd4, 0x19, 0x44, 0x99, 0xed,
            0xd4, 0x66, 0x84, 0xc4, 0x79, 0xda, 0x3a, 0xa1, 0xac, 0x80, 0xdd, 0x55, 0x11, 0x46,
            0x8e, 0xfa, 0xe0, 0x49, 0x16, 0x6e, 0x80,
        ];
        assert_eq!(claim.cbor_fields().unwrap(), expected_cbor);
        let independently_decoded: ciborium::Value =
            ciborium::from_reader(&expected_cbor[..]).unwrap();
        let ciborium::Value::Array(fields) = independently_decoded else {
            panic!("normative claim fixture is not an array");
        };
        assert_eq!(fields.len(), 6);
        assert_eq!(fields[0], ciborium::Value::Text("zajdk".into()));
        assert_eq!(
            fields[1],
            ciborium::Value::Text("org.example.note/main".into())
        );
        assert_eq!(
            fields[2],
            ciborium::Value::Bytes(vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
        );
        assert_eq!(fields[3], ciborium::Value::Integer(0.into()));
        assert_eq!(
            fields[4],
            ciborium::Value::Bytes(claim.value_cid.as_ref().unwrap().as_bytes().to_vec())
        );
        assert_eq!(fields[5], ciborium::Value::Array(Vec::new()));
        let key = SigningKey::from_bytes(&[7; 32]);
        let signature = key.sign(&claim.signing_bytes().unwrap()).to_bytes();
        assert_eq!(
            signature,
            [
                0x48, 0xb7, 0xb4, 0xdd, 0x7b, 0x4a, 0xea, 0x80, 0xa0, 0x3f, 0x13, 0xeb, 0x7f, 0x5c,
                0xe8, 0x5b, 0x8f, 0x85, 0x74, 0x45, 0xfc, 0xa8, 0x70, 0x8a, 0xd0, 0x5a, 0x81, 0xd0,
                0x17, 0x7c, 0x18, 0x41, 0xb9, 0x58, 0x3d, 0x6d, 0xa5, 0x10, 0x78, 0x49, 0x9c, 0xad,
                0xc6, 0x30, 0xfc, 0xd8, 0x2c, 0x3c, 0x89, 0x86, 0x8f, 0x7b, 0x73, 0x8f, 0x94, 0xd4,
                0xd9, 0xb8, 0x1b, 0x4e, 0x5c, 0x5b, 0xfe, 0x0b,
            ]
        );
    }

    #[test]
    fn real_ed25519_vector_rejects_tampering() {
        let key = SigningKey::from_bytes(&[7; 32]);
        let claim = FacetClaim {
            origin_document: DocumentId::new(b"doc"),
            facet_key: facet_key("org.example.note/main"),
            chosen_uuid: Uuid::from_u128(1),
            operation: ClaimOperation::Set,
            value_cid: Some(cid_of(br#"{"a":1,"b":2}"#)),
            predecessors: vec![],
        };
        let message = claim.signing_bytes().unwrap();
        let signed = SignedFacetClaim::from_signature(
            claim.clone(),
            MultibaseKey::try_from(key.verifying_key().to_bytes().as_slice()).unwrap(),
            MultibaseSignature::try_from(key.sign(&message).to_bytes().as_slice()).unwrap(),
        )
        .unwrap();
        signed.verify().unwrap();
        assert_eq!(signed.signature_id().unwrap().as_bytes().len(), CID_LENGTH);
        let mut forged = signed.clone();
        forged.claim.facet_key = facet_key("org.example.note/mainx");
        assert!(matches!(forged.verify(), Err(DmetaError::InvalidSignature)));
    }

    #[test]
    fn an_inherited_witness_verifies_against_its_own_bytes() {
        let key = SigningKey::from_bytes(&[9; 32]);
        let signed_value = Bytes::from(b"native-value".to_vec());
        let claim = FacetClaim {
            origin_document: DocumentId::new(b"origin"),
            facet_key: facet_key("org.example.note/main"),
            chosen_uuid: Uuid::from_u128(4),
            operation: ClaimOperation::Set,
            value_cid: Some(cid_of(signed_value.as_slice())),
            predecessors: Vec::new(),
        };
        let message = claim.signing_bytes().unwrap();
        let entry = FacetSignature {
            actor_public_key: MultibaseKey::try_from(key.verifying_key().to_bytes().as_slice())
                .unwrap(),
            signature: MultibaseSignature::try_from(key.sign(&message).to_bytes().as_slice())
                .unwrap(),
            inherited: Some(InheritedWitness {
                origin_doc: DocumentId::new(b"origin"),
                message: Bytes::from(message),
                value: Some(signed_value.clone()),
            }),
        };
        // The branch copied exactly the signed value, so no witness value is required.
        assert_eq!(
            entry.verify_inherited(&claim, Some(signed_value.as_slice())),
            VerificationState::Accepted
        );
        // The branch copied a different value, and the witness carries the one that was signed.
        assert_eq!(
            entry.verify_inherited(&claim, Some(b"merged-value")),
            VerificationState::Accepted
        );
        // A witness value that is not the signed one is a forgery, not a disagreement.
        let mut forged = entry.clone();
        forged.inherited.as_mut().unwrap().value = Some(Bytes::from(b"other-value".to_vec()));
        assert_eq!(
            forged.verify_inherited(&claim, None),
            VerificationState::ProvenForgery
        );
        // A removal signs no value, so a witness that carries one is a forgery.
        let remove = FacetClaim {
            operation: ClaimOperation::Remove,
            value_cid: None,
            predecessors: Vec::new(),
            ..claim.clone()
        };
        let remove_message = remove.signing_bytes().unwrap();
        let remove_witness = FacetSignature {
            actor_public_key: entry.actor_public_key.clone(),
            signature: MultibaseSignature::try_from(
                key.sign(&remove_message).to_bytes().as_slice(),
            )
            .unwrap(),
            inherited: Some(InheritedWitness {
                origin_doc: remove.origin_document.clone(),
                message: Bytes::from(remove_message),
                value: Some(signed_value),
            }),
        };
        assert_eq!(
            remove_witness.verify_inherited(&remove, None),
            VerificationState::ProvenForgery
        );
        // No carried message means the signature cannot be reconstructed here: pending, not forged.
        let mut pending = entry;
        pending.inherited = None;
        assert_eq!(
            pending.verify_inherited(&claim, None),
            VerificationState::Pending
        );
    }

    #[test]
    fn missing_evidence_is_not_forgery_and_a_rejected_predecessor_cannot_be_laundered() {
        let present = SignatureChecks {
            message: Evidence::Present,
            delegation: Evidence::Present,
            value: Evidence::Present,
            authority: Evidence::Present,
        };
        assert_eq!(
            fold_checks(
                SignatureChecks {
                    delegation: Evidence::Missing,
                    ..present
                },
                []
            ),
            VerificationState::Pending
        );
        assert_eq!(
            fold_checks(
                SignatureChecks {
                    authority: Evidence::Invalid,
                    ..present
                },
                []
            ),
            VerificationState::ProvenForgery
        );
        assert_eq!(fold_checks(present, []), VerificationState::Accepted);
        // A successor whose own checks pass still cannot accept a forged or pending predecessor.
        assert_eq!(
            fold_checks(present, [VerificationState::ProvenForgery]),
            VerificationState::ProvenForgery
        );
        assert_eq!(
            fold_checks(present, [VerificationState::Pending]),
            VerificationState::Pending
        );
        assert_eq!(
            document_verification([VerificationState::Accepted, VerificationState::Pending]),
            VerificationState::Pending
        );
        assert_eq!(
            document_verification([VerificationState::Pending, VerificationState::ProvenForgery]),
            VerificationState::ProvenForgery
        );
    }

    #[test]
    fn an_independently_produced_ed25519_signature_is_accepted() {
        // The signature bytes below were produced by OpenSSL over this claim's signing bytes with
        // the seed [7; 32], so the check does not depend on ed25519-dalek agreeing with itself.
        let claim = FacetClaim {
            origin_document: DocumentId::new(b"doc"),
            facet_key: facet_key("org.example.note/main"),
            chosen_uuid: Uuid::from_u128(1),
            operation: ClaimOperation::Set,
            value_cid: Some(cid_of(br#"{"a":1,"b":2}"#)),
            predecessors: vec![],
        };
        let actor_public_key = MultibaseKey::try_from(
            [
                0xea, 0x4a, 0x6c, 0x63, 0xe2, 0x9c, 0x52, 0x0a, 0xbe, 0xf5, 0x50, 0x7b, 0x13, 0x2e,
                0xc5, 0xf9, 0x95, 0x47, 0x76, 0xae, 0xbe, 0xbe, 0x7b, 0x92, 0x42, 0x1e, 0xea, 0x69,
                0x14, 0x46, 0xd2, 0x2c,
            ]
            .as_slice(),
        )
        .unwrap();
        let signature = MultibaseSignature::try_from(
            [
                0x48, 0xb7, 0xb4, 0xdd, 0x7b, 0x4a, 0xea, 0x80, 0xa0, 0x3f, 0x13, 0xeb, 0x7f, 0x5c,
                0xe8, 0x5b, 0x8f, 0x85, 0x74, 0x45, 0xfc, 0xa8, 0x70, 0x8a, 0xd0, 0x5a, 0x81, 0xd0,
                0x17, 0x7c, 0x18, 0x41, 0xb9, 0x58, 0x3d, 0x6d, 0xa5, 0x10, 0x78, 0x49, 0x9c, 0xad,
                0xc6, 0x30, 0xfc, 0xd8, 0x2c, 0x3c, 0x89, 0x86, 0x8f, 0x7b, 0x73, 0x8f, 0x94, 0xd4,
                0xd9, 0xb8, 0x1b, 0x4e, 0x5c, 0x5b, 0xfe, 0x0b,
            ]
            .as_slice(),
        )
        .unwrap();
        let signed = SignedFacetClaim::from_signature(claim, actor_public_key, signature).unwrap();
        signed.verify().unwrap();
        assert_eq!(
            signed.signature_id().unwrap().as_bytes(),
            &[
                0x01, 0x55, 0x1e, 0x20, 0xec, 0x57, 0x2f, 0x95, 0x11, 0x9a, 0xa6, 0x1b, 0xbe, 0xca,
                0x0d, 0xcd, 0x46, 0xd9, 0x66, 0xe5, 0x46, 0x66, 0xf6, 0x84, 0xdd, 0xf4, 0xe1, 0x54,
                0x8a, 0xe6, 0xbc, 0x48, 0x4b, 0x4f, 0x33, 0x86,
            ]
        );
    }

    #[test]
    fn a_hostile_predecessor_count_is_refused_before_allocation() {
        let claim = FacetClaim {
            origin_document: DocumentId::new(b"doc"),
            facet_key: facet_key("org.example.note/main"),
            chosen_uuid: Uuid::from_u128(1),
            operation: ClaimOperation::Remove,
            value_cid: None,
            predecessors: vec![],
        };
        let mut fields = claim.cbor_fields().unwrap();
        assert_eq!(fields.pop(), Some(0x80));
        fields.extend([0x9a, 0xff, 0xff, 0xff, 0xff]);
        assert!(matches!(
            FacetClaim::from_cbor_fields(&fields),
            Err(DmetaError::MalformedClaim(
                "predecessor count exceeds input budget"
            ))
        ));
        let mut u64_count = claim.cbor_fields().unwrap();
        assert_eq!(u64_count.pop(), Some(0x80));
        u64_count.extend([0x9b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);
        assert!(FacetClaim::from_cbor_fields(&u64_count).is_err());
    }

    #[test]
    fn the_decoder_rejects_malformed_noncanonical_and_trailing_bytes() {
        let claim = FacetClaim {
            origin_document: DocumentId::new(b"origin"),
            facet_key: facet_key("org.example.note/main"),
            chosen_uuid: Uuid::from_u128(7),
            operation: ClaimOperation::Set,
            value_cid: Some(cid_of(b"signed-native-bytes")),
            predecessors: vec![],
        };
        let fields = claim.cbor_fields().unwrap();
        assert_eq!(FacetClaim::from_cbor_fields(&fields).unwrap(), claim);
        let signing = claim.signing_bytes().unwrap();
        assert_eq!(FacetClaim::from_signing_bytes(&signing).unwrap(), claim);
        assert!(matches!(
            FacetKey::from_str("org.example.note/"),
            Err(ReferenceError::MissingFacetKey)
        ));
        let mut trailing = fields.clone();
        trailing.push(0);
        assert!(FacetClaim::from_cbor_fields(&trailing).is_err());
        let mut noncanonical = fields.clone();
        noncanonical.splice(0..1, [0x98, 6]);
        assert!(FacetClaim::from_cbor_fields(&noncanonical).is_err());
        let mut wrong_uuid = fields.clone();
        wrong_uuid[33] = 0x4f;
        assert!(FacetClaim::from_cbor_fields(&wrong_uuid).is_err());
        let mut wrong_cid = fields;
        let cid_offset = wrong_cid.len() - 37;
        wrong_cid[cid_offset + 1] = 0x01;
        assert!(FacetClaim::from_cbor_fields(&wrong_cid).is_err());
    }

    #[test]
    fn an_inherited_claim_binds_the_source_context_not_the_branch() {
        let key = SigningKey::from_bytes(&[11; 32]);
        let predecessor_a = cid_with(1);
        let predecessor_b = cid_with(2);
        let source_claim = FacetClaim {
            origin_document: DocumentId::new(b"origin-a"),
            facet_key: facet_key("org.example.note/main"),
            chosen_uuid: Uuid::from_u128(9),
            operation: ClaimOperation::Set,
            value_cid: Some(cid_of(b"the signed value")),
            predecessors: vec![
                predecessor_b.clone(),
                predecessor_a.clone(),
                predecessor_b.clone(),
            ],
        };
        let message = source_claim.signing_bytes().unwrap();
        let witness = FacetSignature {
            actor_public_key: MultibaseKey::try_from(key.verifying_key().to_bytes().as_slice())
                .unwrap(),
            signature: MultibaseSignature::try_from(key.sign(&message).to_bytes().as_slice())
                .unwrap(),
            inherited: Some(InheritedWitness {
                origin_doc: source_claim.origin_document.clone(),
                message: Bytes::from(message),
                value: Some(Bytes::from(b"the signed value".to_vec())),
            }),
        };
        assert_eq!(
            witness.verify_inherited(&source_claim, None),
            VerificationState::Accepted
        );
        // The witness's predecessors were sorted and deduplicated before signing, so the normalized
        // form is the same claim.
        let mut normalized = source_claim.clone();
        normalized.predecessors = vec![predecessor_a.clone(), predecessor_b.clone()];
        assert_eq!(
            witness.verify_inherited(&normalized, None),
            VerificationState::Accepted
        );
        let mut wrong_frontier = normalized.clone();
        wrong_frontier.predecessors = vec![predecessor_a.clone()];
        assert_eq!(
            witness.verify_inherited(&wrong_frontier, None),
            VerificationState::ProvenForgery
        );
        let mut wrong_facet = source_claim.clone();
        wrong_facet.facet_key = facet_key("org.example.note/wrong");
        assert_eq!(
            witness.verify_inherited(&wrong_facet, None),
            VerificationState::ProvenForgery
        );
        let mut wrong_uuid = source_claim.clone();
        wrong_uuid.chosen_uuid = Uuid::from_u128(10);
        assert_eq!(
            witness.verify_inherited(&wrong_uuid, None),
            VerificationState::ProvenForgery
        );
        let mut wrong_origin = source_claim;
        wrong_origin.origin_document = DocumentId::new(b"branch-b");
        assert_eq!(
            witness.verify_inherited(&wrong_origin, None),
            VerificationState::ProvenForgery
        );
    }

    #[test]
    fn a_facet_key_with_slashes_and_non_utf8_survives_the_claim_and_the_uri() {
        let key = FacetKey::new("org.example.note", b"main/bytes/\x80".to_vec()).unwrap();
        let claim = FacetClaim {
            origin_document: DocumentId::new(b"doc"),
            facet_key: key.clone(),
            chosen_uuid: Uuid::from_u128(12),
            operation: ClaimOperation::Remove,
            value_cid: None,
            predecessors: vec![],
        };
        let encoded = claim.cbor_fields().unwrap();
        assert_eq!(FacetClaim::from_cbor_fields(&encoded).unwrap(), claim);
        assert_eq!(FacetKey::from_str(&key.to_string()).unwrap(), key);
        assert!(key.to_string().contains("\\x80"));

        let reference =
            FacetReference::new(ReferenceDocumentId::SelfDocument, key.clone()).unwrap();
        let uri = reference.to_uri_string().unwrap();
        assert!(uri.contains("%5Cx80"));
        assert_eq!(FacetReference::parse(&uri).unwrap().key(), &key);
        assert_eq!(reference.key(), &key);
    }

    #[test]
    fn predecessors_are_sorted_and_deduplicated_before_signing() {
        let first = cid_with(1);
        let second = cid_with(2);
        let claim = FacetClaim {
            origin_document: DocumentId::new(b"d"),
            facet_key: facet_key("org.example.test/main"),
            chosen_uuid: Uuid::nil(),
            operation: ClaimOperation::Remove,
            value_cid: None,
            predecessors: vec![second.clone(), first.clone(), second.clone()],
        };
        let bytes = claim.cbor_fields().unwrap();
        let decoded = FacetClaim::from_cbor_fields(&bytes).unwrap();
        assert_eq!(decoded.predecessors, vec![first, second]);
    }
}
