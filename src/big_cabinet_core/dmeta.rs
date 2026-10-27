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

mod attribution;
mod native;
#[cfg(feature = "schemars")]
mod storage;
mod verification;

#[cfg(test)]
mod e2e;

pub use attribution::{Attribution, AttributionError};
#[cfg(feature = "schemars")]
pub use storage::StorageError;

use jiff::Timestamp;
pub use native::{CanonicalF64, NativeReadBudget, NativeReadError, NativeValue, encode_native};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::str::FromStr;
use uuid::Uuid;
pub use verification::{
    Evidence, FacetValue, SignatureChecks, SignedFacetClaim, VerificationState, facet_verification,
    fold_checks, signature_id,
};

use crate::reference::FacetKey;

pub const DMETA_FACET_TAG: &str = "org.example.daybook.dmeta";
const CLAIM_DOMAIN: &[u8] = b"daybook.facet-claim.v2\0";
const CLAIM_ID_DOMAIN: &[u8] = b"daybook.facet-claim-id.v2\0";
const CID_VERSION_RAW_BLAKE3: [u8; 4] = [0x01, 0x55, 0x1e, 0x20];
const CID_LENGTH: usize = 36;

/// The document identity shared with BigRepo: arbitrary bytes with canonical multibase text.
pub use big_sync_core::ObjKey as DocId;

/// Identity allocated before initial content is persisted and publication begins.
///
/// The document's own id is not a field of [`Dmeta`]: the enclosing document already knows it, and
/// a field inside the facet would be copied verbatim onto a branch, where it would name the origin
/// rather than the branch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocIdentity {
    pub id: DocId,
    pub created_at: Timestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct Uuid7(Uuid);

impl Uuid7 {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }

    pub fn unix_millis(&self) -> u64 {
        let (seconds, nanos) = self
            .0
            .get_timestamp()
            .expect("a UUIDv7 carries a timestamp")
            .to_unix();
        seconds * 1000 + u64::from(nanos) / 1_000_000
    }
}

impl Default for Uuid7 {
    fn default() -> Self {
        Self::new()
    }
}

impl TryFrom<Uuid> for Uuid7 {
    type Error = DmetaError;

    fn try_from(value: Uuid) -> Result<Self, Self::Error> {
        if value.get_version_num() != 7 || value.get_variant() != uuid::Variant::RFC4122 {
            return Err(DmetaError::InvalidUuidVersion);
        }
        Ok(Self(value))
    }
}

impl std::fmt::Display for Uuid7 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl<'de> Deserialize<'de> for Uuid7 {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Uuid::deserialize(deserializer)?
            .try_into()
            .map_err(serde::de::Error::custom)
    }
}

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for Uuid7 {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Uuid7".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "format": "uuid",
            "pattern": "^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ActorMetadata {
    pub pubkey: MultibaseKey,
    pub actor_url: String,
    // FIXME: I don't like that this is an opaque bytes. can't we have some kind of standard
    // json object thing around delegations that ucans or whatever use?
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegation: Option<Bytes>,
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
    pub actors: BTreeMap<Uuid7, ActorMetadata>,
    /// Which document each `docUuid` names. A normal branch carries its origin's changes, and with
    /// them the origin's signature entries and this index's bindings for the origin; merging back
    /// brings the branch's bindings along. A signature's claim names the document it was made on,
    /// and this index is how the entry that carries no document id is tied to that claim.
    #[serde(default)]
    #[cfg_attr(feature = "schemars", schemars(with = "BTreeMap<Uuid7, String>"))]
    pub branch_uuids: BTreeMap<Uuid7, DocId>,
    #[serde(default)]
    pub facet_uuids: BTreeMap<Uuid7, FacetKey>,
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
            branch_uuids: BTreeMap::new(),
            facet_uuids: BTreeMap::new(),
            facets: BTreeMap::new(),
            extra: crate::facet::Extra::new(),
        }
    }

    /// The UUID a signature introduced on `document` carries as its `docUuid`: the smallest one
    /// already bound to `document`, or `minted` when none is.
    ///
    /// A normal branch *is* its origin's changes under a new [`DocId`], so it arrives with the
    /// origin's bindings and none of its own. Its first write — the branch's own
    /// `org.example.daybook.branch/<docid>` facet — binds a fresh UUID to it, so every later
    /// write on the branch finds one. Choosing the smallest visible binding keeps two stagings of
    /// the same write, and two concurrent writers that both see it, on the same UUID.
    pub fn doc_uuid(&mut self, document: &DocId, minted: Uuid7) -> Result<Uuid7, DmetaError> {
        if let Some(uuid) = self
            .branch_uuids
            .iter()
            .find_map(|(uuid, bound)| (bound == document).then_some(*uuid))
        {
            return Ok(uuid);
        }
        match self.branch_uuids.entry(minted) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(document.clone());
                Ok(minted)
            }
            std::collections::btree_map::Entry::Occupied(_) => Err(DmetaError::ReboundAlias),
        }
    }

    /// Record an alias without selecting content. Multiple aliases for one key are durable evidence
    /// of concurrent creation even after the conflict that produced them is no longer visible.
    pub fn record_facet_alias(&mut self, uuid: Uuid7, key: FacetKey) -> Result<(), DmetaError> {
        match self.facet_uuids.entry(uuid) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(key);
                Ok(())
            }
            std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &key => Ok(()),
            std::collections::btree_map::Entry::Occupied(_) => Err(DmetaError::ReboundAlias),
        }
    }

    /// Whether more than one creator independently opened this slot.
    pub fn has_concurrent_creation(&self, key: &FacetKey) -> bool {
        self.facet_uuids
            .values()
            .filter(|alias| *alias == key)
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
    pub deleted_at: Vec<Timestamp>,
    #[serde(default)]
    pub signatures: Vec<FacetSignature>,
}

/// A signature a compaction retained from history it dropped, where that history is no longer
/// available to reconstruct the signed bytes from.
///
/// An ordinary signature stores nothing but the actor and the signature bytes: the message is
/// rebuilt from the state at the change that introduced it. That includes a signature that reached
/// this document through a branch or a merge, because those carry the changes themselves. A
/// compaction does not, so the bytes have to travel with it. `message` is the exact
/// domain-prefixed message the actor signed, and it is authenticated by the signature — a copy
/// that substitutes other bytes fails verification rather than being trusted. `value` is present
/// only when the value the compaction kept is not the value that was signed, which happens when
/// concurrent writes had merged; it is the canonical native bytes of that signed value, so its CID
/// can be checked without presenting the signature as covering the merged result.
///
/// What the witness cannot prove on its own is that the actor was authorized on the origin at the
/// frontier where the signature was made. That frontier lives on the origin document, and an origin
/// that is unavailable leaves the witness pending rather than forged.
/// FIXME: making "origin must be avail" as pending is 
/// a bad ddesign because that prevents us from delting
/// the origin document for the compaction usecase.
/// well, techicnally, even if we do'nt hve the original
/// document, we'd have the keyhive entry for it but still
/// i think this might be a bit much
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct InheritedWitness {
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub origin_doc: DocId,
    pub message: Bytes,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Bytes>,
}

pub const BRANCH_FACET_TAG: &str = "org.example.daybook.branch";

/// The branch side of a branch: `org.example.daybook.branch/<docid>` on the branch document,
/// naming that document's branch path.
///
/// The facet id carries the document id, not the value, because a branch merged back brings this
/// facet along: the origin then holds the branch's entry under the branch's id, and still finds
/// its own path (if it is itself a branch) under its own id. Only Branch writes it.
#[cfg_attr(feature = "schemars", macros::facet(id = "org.example.daybook.branch"))]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BranchPath {
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<BranchOrigin>,
    #[serde(flatten)]
    pub extra: crate::facet::Extra,
}

// FIXME: this isn't an open struct and yet i'm seeing
// no requirements by the facet macro to declare it closed
// this is an issue. first of all, this should be open
// and second of all, for it to be closed, there should have been
// an explicit closed marker on here
// for third party types, the embedding site can declare it
// as "i know this is closed"
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BranchOrigin {
    pub facet_ref: crate::FacetReference,
    #[cfg_attr(feature = "schemars", schemars(with = "Vec<String>"))]
    pub ref_heads: Vec<automerge::ChangeHash>,
}

/// One entry in a facet's competing set of signatures.
///
/// `inherited` is set only for a signature carried over a compaction, where the history its
/// message would be reconstructed from is gone. Every other signature, including one that arrived
/// with a branch's or a merge's changes, has none.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct FacetSignature {
    pub actor_uuid: Uuid7,
    pub signature: SignatureBytes,
    // The document this entry was introduced on, through the `branchUuids` binding visible at its
    // introduction. A branch or a merge moves the entry to another document unchanged, so the
    // entry cannot assume the document holding it is the one its claim names. (A plain comment: a
    // doc comment would become a description beside the UUID's `$ref`, which Lexicon cannot state.)
    // FIXME: why is this present on every signature?
    // this is not needed since either it's the currrent
    // document or the inherited witness would indicate the
    // origin doc. though I think I remember authorizing
    // this chagne for some reason, do the ADRs say why?
    pub doc_uuid: Uuid7,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<Bytes>,
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
    /// The document the claim was made on. A branch or a merge later moves the signature to other
    /// documents unchanged, so this is not necessarily the document that now holds it; the entry
    /// names this document through its `docUuid` and the `branchUuids` binding.
    pub origin_document: DocId,
    pub facet_key: FacetKey,
    pub chosen_uuid: Uuid7,
    pub actor_uuid: Uuid7,
    pub operation: ClaimOperation,
    pub value_cid: Option<Cid>,
    // FIXME: are predecessor CIDs or signatures??
    #[serde(default)]
    pub predecessors: Vec<Cid>,
}

// FIXME: calling them CID might be misleading since I don't think the official
// CID spec supports blake3
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SignatureBytes([u8; 64]);

impl SignatureBytes {
    pub fn as_bytes(&self) -> &[u8; 64] {
        &self.0
    }
}

impl TryFrom<&[u8]> for SignatureBytes {
    type Error = DmetaError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let bytes: [u8; 64] = bytes.try_into().map_err(|_| DmetaError::InvalidSignature)?;
        Ok(Self(bytes))
    }
}

impl Serialize for SignatureBytes {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut object = serializer.serialize_map(Some(1))?;
        object.serialize_entry("$bytes", &data_encoding::BASE64.encode(&self.0))?;
        object.end()
    }
}

impl<'de> Deserialize<'de> for SignatureBytes {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes = Bytes::deserialize(deserializer)?;
        Self::try_from(bytes.as_slice()).map_err(serde::de::Error::custom)
    }
}

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for SignatureBytes {
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> std::borrow::Cow<'static, str> {
        "SignatureBytes".into()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        Bytes::json_schema(generator)
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
    #[error("an actor or facet alias must be an RFC 9562 UUIDv7")]
    InvalidUuidVersion,
    #[error("a facet UUID alias cannot be rebound to another key")]
    ReboundAlias,
    #[error("signed message evidence is unavailable")]
    MissingMessage,
    #[error("signature and claim contexts disagree")]
    WrongContext,
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
        cbor_array(&mut out, 7);
        cbor_text(&mut out, &self.origin_document.to_string());
        cbor_text(&mut out, &self.facet_key.to_string());
        cbor_bytes(&mut out, self.chosen_uuid.as_uuid().as_bytes());
        cbor_bytes(&mut out, self.actor_uuid.as_uuid().as_bytes());
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
        if major != 4 || length != 7 {
            return Err(DmetaError::MalformedClaim(
                "claim must be a seven-element array",
            ));
        }

        let origin_text = read_cbor_text_strict(bytes, &mut cursor)?;
        let origin_document = DocId::from_str(&origin_text)
            .map_err(|_| DmetaError::MalformedClaim("invalid origin document ID"))?;
        let facet_key_text = read_cbor_text_strict(bytes, &mut cursor)?;
        let facet_key = FacetKey::from_str(&facet_key_text)
            .map_err(|_| DmetaError::MalformedClaim("invalid facet key"))?;
        let uuid_bytes = read_cbor_bytes_strict(bytes, &mut cursor)?;
        let chosen_uuid = Uuid7::try_from(
            Uuid::from_slice(&uuid_bytes)
                .map_err(|_| DmetaError::MalformedClaim("chosen UUID must be 16 bytes"))?,
        )?;
        let actor_bytes = read_cbor_bytes_strict(bytes, &mut cursor)?;
        let actor_uuid = Uuid7::try_from(
            Uuid::from_slice(&actor_bytes)
                .map_err(|_| DmetaError::MalformedClaim("actor UUID must be 16 bytes"))?,
        )?;
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
            actor_uuid,
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

    use crate::{FacetReference, ReferenceDocId, ReferenceError};

    fn facet_key(value: &str) -> FacetKey {
        FacetKey::from_str(value).unwrap()
    }

    const PORTABLE_PROOF: &str = r#"{
        "createdAt": "1970-01-01T00:00:00Z",
        "updatedAt": [],
        "actors": {
            "01890f3e-1234-7000-8000-000000000002": {
                "pubkey": "zGmaDrppBC7P5ARKV8g3djiwP89vz1jLK23V2GBjuAEGB",
                "actorUrl": "did:key:test",
                "delegation": {"$bytes": "AQID/w=="}
            }
        },
        "branchUuids": {
            "01890f3e-1234-7000-8000-000000000009": "zajdk"
        },
        "facetUuids": {
            "01890f3e-1234-7000-8000-000000000001": "org.example.note/main"
        },
        "facets": {
            "org.example.note/main": {
                "updatedAt": [],
                "deletedAt": [],
                "signatures": [{
                    "actorUuid": "01890f3e-1234-7000-8000-000000000002",
                    "docUuid": "01890f3e-1234-7000-8000-000000000009",
                    "signature": {"$bytes": "7OJ5Fpuon7QCm9iAUz+gJ7ePC9pXGDQYBu8GxSthtkEzcmogHSGTzh8Pr+G1AvIdOK2QqQ+qr2+58orrETzcBQ=="},
                    "message": {"$bytes": "ZGF5Ym9vay5mYWNldC1jbGFpbS52MgCHZXphamRrdW9yZy5leGFtcGxlLm5vdGUvbWFpblABiQ8+EjRwAIAAAAAAAAABUAGJDz4SNHAAgAAAAAAAAAIAWCQBVR4gLWvLu8YEORo43f1WfOLngEcapPP3vFmdHGSY17U+tsaA"}
                }]
            }
        }
    }"#;

    fn uuid7(index: u128) -> Uuid7 {
        Uuid7::try_from(Uuid::from_u128(
            0x0189_0f3e_1234_7000_8000_0000_0000_0000 | index,
        ))
        .unwrap()
    }

    fn native_value() -> NativeValue {
        NativeValue::Map(BTreeMap::from([
            ("a".into(), NativeValue::Int(1)),
            ("b".into(), NativeValue::Bytes(vec![0, 255])),
        ]))
    }

    fn claim_for(value: &NativeValue) -> FacetClaim {
        FacetClaim {
            origin_document: DocId::new(b"doc"),
            facet_key: facet_key("org.example.note/main"),
            chosen_uuid: uuid7(1),
            actor_uuid: uuid7(2),
            operation: ClaimOperation::Set,
            value_cid: Some(cid_of(&encode_native(value))),
            predecessors: Vec::new(),
        }
    }

    fn signed_entry(claim: &FacetClaim) -> (ActorMetadata, FacetSignature) {
        let key = SigningKey::from_bytes(&[7; 32]);
        (
            ActorMetadata {
                pubkey: MultibaseKey::try_from(key.verifying_key().to_bytes().as_slice()).unwrap(),
                actor_url: "did:key:test".into(),
                delegation: None,
            },
            FacetSignature {
                actor_uuid: claim.actor_uuid,
                doc_uuid: uuid7(9),
                signature: SignatureBytes::try_from(
                    key.sign(&claim.signing_bytes().unwrap())
                        .to_bytes()
                        .as_slice(),
                )
                .unwrap(),
                message: None,
                inherited: None,
            },
        )
    }

    fn cid_with(byte: u8) -> Cid {
        cid_of(&[byte; 32])
    }

    #[test]
    fn aliases_retain_concurrency_without_a_reverse_index() {
        let mut dmeta = Dmeta::new(Timestamp::from_second(0).unwrap());
        let key = facet_key("org.example.note/main");
        dmeta.record_facet_alias(uuid7(2), key.clone()).unwrap();
        assert!(!dmeta.has_concurrent_creation(&key));
        dmeta.record_facet_alias(uuid7(1), key.clone()).unwrap();
        assert!(dmeta.has_concurrent_creation(&key));
        assert!(!dmeta.has_concurrent_creation(&facet_key("org.example.note/other")));
        // The chosen alias is the smallest one visible, and it falls out of the index order.
        let chosen = dmeta
            .facet_uuids
            .iter()
            .filter(|(_, alias)| **alias == key)
            .map(|(uuid, _)| *uuid)
            .min()
            .unwrap();
        assert_eq!(chosen, uuid7(1));
        assert_eq!(
            serde_json::from_value::<Dmeta>(serde_json::to_value(&dmeta).unwrap()).unwrap(),
            dmeta
        );
    }

    #[test]
    fn a_facet_alias_can_be_repeated_but_never_rebound() {
        let mut dmeta = Dmeta::new(Timestamp::from_second(0).unwrap());
        let key = facet_key("org.example.note/main");
        dmeta.record_facet_alias(uuid7(1), key.clone()).unwrap();
        let original = dmeta.clone();
        dmeta.record_facet_alias(uuid7(1), key).unwrap();
        assert_eq!(dmeta, original);
        assert!(matches!(
            dmeta.record_facet_alias(uuid7(1), facet_key("org.example.note/other")),
            Err(DmetaError::ReboundAlias)
        ));
        assert_eq!(dmeta, original);
    }

    #[test]
    fn uuid7_is_validated_in_values_and_metadata_map_keys() {
        let uuid = uuid7(1);
        assert_eq!(uuid.unix_millis(), 1_688_177_873_460);
        assert_eq!(uuid.as_uuid().get_version_num(), 7);
        let fresh = Uuid7::new();
        assert_eq!(Uuid7::try_from(*fresh.as_uuid()).unwrap(), fresh);
        assert_eq!(
            serde_json::to_value(uuid).unwrap(),
            serde_json::json!("01890f3e-1234-7000-8000-000000000001")
        );
        assert_eq!(
            serde_json::from_value::<Uuid7>(serde_json::to_value(uuid).unwrap()).unwrap(),
            uuid
        );
        let fixture: serde_json::Value = serde_json::from_str(PORTABLE_PROOF).unwrap();
        for invalid in [
            "00000000-0000-0000-0000-000000000000",
            "01890f3e-1234-4000-8000-000000000001",
            "01890f3e-1234-7000-0000-000000000001",
        ] {
            assert!(matches!(
                Uuid7::try_from(Uuid::parse_str(invalid).unwrap()),
                Err(DmetaError::InvalidUuidVersion)
            ));
            assert!(serde_json::from_value::<Uuid7>(serde_json::json!(invalid)).is_err());
            for member in ["actors", "facetUuids"] {
                let mut malformed = fixture.clone();
                let map = malformed[member].as_object_mut().unwrap();
                let valid_key = map.keys().next().unwrap().clone();
                let value = map.remove(&valid_key).unwrap();
                assert!(map.insert(invalid.into(), value).is_none());
                assert!(serde_json::from_value::<Dmeta>(malformed).is_err());
            }
            let mut malformed = fixture["facets"]["org.example.note/main"]["signatures"][0].clone();
            malformed["actorUuid"] = serde_json::json!(invalid);
            assert!(serde_json::from_value::<FacetSignature>(malformed).is_err());
        }
    }

    #[test]
    fn deleted_at_is_a_required_list_even_when_empty() {
        let metadata = FacetMetadata {
            updated_at: Vec::new(),
            deleted_at: Vec::new(),
            signatures: Vec::new(),
        };
        let json = serde_json::json!({"updatedAt": [], "deletedAt": [], "signatures": []});
        assert_eq!(serde_json::to_value(&metadata).unwrap(), json);
        assert_eq!(
            serde_json::from_value::<FacetMetadata>(json).unwrap(),
            metadata
        );
        assert_eq!(
            serde_json::from_value::<FacetMetadata>(serde_json::json!({"deletedAt": []})).unwrap(),
            metadata
        );
        for malformed in [
            serde_json::json!({}),
            serde_json::json!({"updatedAt": [], "signatures": []}),
            serde_json::json!({"deletedAt": null}),
            serde_json::json!({"deletedAt": "1970-01-01T00:00:00Z"}),
        ] {
            assert!(serde_json::from_value::<FacetMetadata>(malformed).is_err());
        }
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
    fn actor_keys_are_multibase_but_signatures_are_fixed_length_bytes() {
        let key = MultibaseKey::try_from([7; 32].as_slice()).unwrap();
        let text = key.to_string();
        assert!(text.starts_with('z'));
        assert_eq!(text.parse::<MultibaseKey>().unwrap(), key);
        assert_eq!(serde_json::to_value(&key).unwrap(), serde_json::json!(text));
        assert_eq!(
            serde_json::from_str::<MultibaseKey>(&serde_json::to_string(&key).unwrap()).unwrap(),
            key
        );
        // A truncated or over-long spelling is refused rather than decoded into a different key.
        assert!(text[..text.len() - 1].parse::<MultibaseKey>().is_err());
        assert!(format!("{text}1").parse::<MultibaseKey>().is_err());

        let fixture: serde_json::Value = serde_json::from_str(PORTABLE_PROOF).unwrap();
        let json = fixture["facets"]["org.example.note/main"]["signatures"][0]["signature"].clone();
        let signature: SignatureBytes = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(signature.as_bytes().len(), 64);
        assert_eq!(serde_json::to_value(&signature).unwrap(), json);
        let mut unpadded = json.clone();
        unpadded["$bytes"] =
            serde_json::json!(json["$bytes"].as_str().unwrap().trim_end_matches('='));
        assert_eq!(
            serde_json::from_value::<SignatureBytes>(unpadded).unwrap(),
            signature
        );
        for length in [0, 32, 63, 65] {
            let bytes = vec![9; length];
            assert!(SignatureBytes::try_from(bytes.as_slice()).is_err());
            assert!(
                serde_json::from_value::<SignatureBytes>(
                    serde_json::json!({"$bytes": data_encoding::BASE64.encode(&bytes)})
                )
                .is_err()
            );
        }
        for malformed in [
            serde_json::json!(utils_rs::hash::encode_base58_multibase(
                signature.as_bytes()
            )),
            serde_json::json!(signature.as_bytes().as_slice()),
            serde_json::json!({"$bytes": "not base64"}),
            serde_json::json!({"$bytes": json["$bytes"], "extra": true}),
        ] {
            assert!(serde_json::from_value::<SignatureBytes>(malformed).is_err());
        }
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
    fn claim_cbor_and_openssl_ed25519_vectors_are_stable() {
        use ciborium::Value as Cbor;

        let value = native_value();
        let native_bytes = data_encoding::HEXLOWER
            .decode(b"da64626b0ba26161da64626b02016162da64626b064200ff")
            .unwrap();
        assert_eq!(encode_native(&value), native_bytes);
        let expected_cid = data_encoding::HEXLOWER
            .decode(b"01551e202d6bcbbbc604391a38ddfd567ce2e780471aa4f3f7bc599d1c6498d7b53eb6c6")
            .unwrap();
        assert_eq!(cid_of(&native_bytes).as_bytes().as_slice(), expected_cid);
        let claim = claim_for(&value);
        let expected_cbor = data_encoding::HEXLOWER
            .decode(
                concat!(
                    "87657a616a646b756f72672e6578616d706c652e6e6f74652f6d61696e",
                    "5001890f3e1234700080000000000000015001890f3e12347000800000000000000200",
                    "582401551e202d6bcbbbc604391a38ddfd567ce2e780471aa4f3f7bc599d1c6498d7b53eb6c680"
                )
                .as_bytes(),
            )
            .unwrap();
        assert_eq!(claim.cbor_fields().unwrap(), expected_cbor);
        assert_eq!(FacetClaim::from_cbor_fields(&expected_cbor).unwrap(), claim);
        let independently_decoded: Cbor = ciborium::from_reader(&expected_cbor[..]).unwrap();
        assert_eq!(
            independently_decoded,
            Cbor::Array(vec![
                Cbor::Text("zajdk".into()),
                Cbor::Text("org.example.note/main".into()),
                Cbor::Bytes(uuid7(1).as_uuid().as_bytes().to_vec()),
                Cbor::Bytes(uuid7(2).as_uuid().as_bytes().to_vec()),
                Cbor::Integer(0.into()),
                Cbor::Bytes(expected_cid),
                Cbor::Array(Vec::new()),
            ])
        );
        let fixture: Dmeta = serde_json::from_str(PORTABLE_PROOF).unwrap();
        let entry = &fixture.facets[&claim.facet_key].signatures[0];
        assert_eq!(
            claim.signing_bytes().unwrap(),
            entry.message.as_ref().unwrap().as_slice()
        );
        assert_eq!(signed_entry(&claim).1.signature, entry.signature);
    }

    #[test]
    fn real_ed25519_vector_rejects_tampering_with_every_signed_field() {
        let claim = claim_for(&native_value());
        let (actor, entry) = signed_entry(&claim);
        let signed = entry.authenticate(&actor, Some(&claim)).unwrap();
        signed.verify().unwrap();
        assert_eq!(signed.signature_id().unwrap().as_bytes().len(), CID_LENGTH);
        let mut altered = Vec::new();
        let mut wrong = claim.clone();
        wrong.origin_document = DocId::new(b"another-document");
        altered.push(wrong);
        let mut wrong = claim.clone();
        wrong.facet_key = facet_key("org.example.note/mainx");
        altered.push(wrong);
        let mut wrong = claim.clone();
        wrong.chosen_uuid = uuid7(3);
        altered.push(wrong);
        let mut wrong = claim.clone();
        wrong.actor_uuid = uuid7(3);
        altered.push(wrong);
        let mut wrong = claim.clone();
        wrong.operation = ClaimOperation::Remove;
        wrong.value_cid = None;
        altered.push(wrong);
        let mut wrong = claim.clone();
        wrong.value_cid = Some(cid_of(&encode_native(&NativeValue::Null)));
        altered.push(wrong);
        let mut wrong = claim;
        wrong.predecessors.push(cid_with(1));
        altered.push(wrong);
        for wrong in altered {
            let forged = SignedFacetClaim {
                claim: wrong.clone(),
                ..signed.clone()
            };
            assert!(matches!(forged.verify(), Err(DmetaError::InvalidSignature)));
            assert!(matches!(
                SignedFacetClaim::from_signature(
                    wrong,
                    actor.pubkey.clone(),
                    entry.signature.clone()
                ),
                Err(DmetaError::InvalidSignature)
            ));
        }
        let mut forged = signed.clone();
        let mut signature = *forged.signature.as_bytes();
        signature[0] ^= 1;
        forged.signature = SignatureBytes::try_from(signature.as_slice()).unwrap();
        assert!(matches!(forged.verify(), Err(DmetaError::InvalidSignature)));
        assert_ne!(
            forged.signature_id().unwrap(),
            signed.signature_id().unwrap()
        );
        let mut forged = signed.clone();
        let other_key = SigningKey::from_bytes(&[8; 32]);
        forged.actor_public_key =
            MultibaseKey::try_from(other_key.verifying_key().as_bytes().as_slice()).unwrap();
        assert!(matches!(forged.verify(), Err(DmetaError::InvalidSignature)));
        assert_ne!(
            forged.signature_id().unwrap(),
            signed.signature_id().unwrap()
        );
    }

    #[test]
    fn an_inherited_historical_witness_does_not_cover_changed_current_content() {
        let value = native_value();
        let changed = NativeValue::Map(BTreeMap::from([
            ("a".into(), NativeValue::Int(2)),
            ("b".into(), NativeValue::Bytes(vec![0, 255])),
        ]));
        let claim = claim_for(&value);
        let (actor, mut entry) = signed_entry(&claim);
        let witness = InheritedWitness {
            origin_doc: claim.origin_document.clone(),
            message: Bytes::from(claim.signing_bytes().unwrap()),
            value: None,
        };
        entry.inherited = Some(witness);
        let signed = entry.authenticate(&actor, None).unwrap();
        assert_eq!(
            signed.value_evidence(FacetValue::Present(&value)),
            Evidence::Present
        );
        entry.inherited.as_mut().unwrap().value = Some(Bytes::from(encode_native(&value)));
        assert_eq!(entry.authenticate(&actor, Some(&claim)).unwrap(), signed);
        assert_eq!(
            signed.value_evidence(FacetValue::Present(&changed)),
            Evidence::Invalid
        );
        assert_eq!(
            fold_checks(SignatureChecks {
                message: Evidence::Present,
                attribution: Evidence::Present,
                value: signed.value_evidence(FacetValue::Present(&changed)),
            }),
            VerificationState::Uncovered
        );
        assert_eq!(
            fold_checks(SignatureChecks {
                message: Evidence::Present,
                attribution: Evidence::Missing,
                value: signed.value_evidence(FacetValue::Present(&value)),
            }),
            VerificationState::Pending
        );
        assert_eq!(
            signed.value_evidence(FacetValue::Missing),
            Evidence::Missing
        );
        let mut forged = entry.clone();
        forged.inherited.as_mut().unwrap().value = Some(Bytes::from(encode_native(&changed)));
        assert!(matches!(
            forged.authenticate(&actor, None),
            Err(DmetaError::WrongContext)
        ));
        let remove = FacetClaim {
            operation: ClaimOperation::Remove,
            value_cid: None,
            ..claim.clone()
        };
        let (_, mut remove_entry) = signed_entry(&remove);
        remove_entry.inherited = Some(InheritedWitness {
            origin_doc: remove.origin_document.clone(),
            message: Bytes::from(remove.signing_bytes().unwrap()),
            value: Some(Bytes::from(encode_native(&value))),
        });
        assert!(matches!(
            remove_entry.authenticate(&actor, None),
            Err(DmetaError::WrongContext)
        ));
        remove_entry.inherited.as_mut().unwrap().value = None;
        let signed_remove = remove_entry.authenticate(&actor, None).unwrap();
        assert_eq!(
            signed_remove.value_evidence(FacetValue::Removed),
            Evidence::Present
        );
        assert_eq!(
            signed_remove.value_evidence(FacetValue::Present(&value)),
            Evidence::Invalid
        );
        assert_eq!(
            signed_remove.value_evidence(FacetValue::Missing),
            Evidence::Missing
        );
        entry.inherited = None;
        assert!(matches!(
            entry.authenticate(&actor, None),
            Err(DmetaError::MissingMessage)
        ));
        assert_eq!(entry.authenticate(&actor, Some(&claim)).unwrap(), signed);
    }

    #[test]
    fn missing_invalid_and_uncovered_evidence_remain_distinct() {
        use Evidence::{Invalid, Missing, Present};
        use VerificationState::{Accepted, Pending, Rejected, Uncovered};

        for (checks, expected) in [
            ([Present, Present, Present], Accepted),
            ([Missing, Present, Present], Pending),
            ([Present, Missing, Present], Pending),
            ([Present, Present, Missing], Pending),
            ([Present, Present, Invalid], Uncovered),
            ([Present, Missing, Invalid], Pending),
            ([Invalid, Present, Present], Rejected),
            ([Present, Invalid, Present], Rejected),
            ([Invalid, Missing, Missing], Rejected),
            ([Missing, Invalid, Missing], Rejected),
        ] {
            let [message, attribution, value] = checks;
            assert_eq!(
                fold_checks(SignatureChecks {
                    message,
                    attribution,
                    value
                }),
                expected
            );
        }
    }

    #[test]
    fn a_valid_replacement_can_name_rejected_ancestry_without_accepting_it() {
        let old_value = native_value();
        let old_claim = claim_for(&old_value);
        let (actor, old_entry) = signed_entry(&old_claim);
        let mut forged = old_entry.authenticate(&actor, Some(&old_claim)).unwrap();
        forged.claim.value_cid = Some(cid_of(&encode_native(&NativeValue::Bool(false))));
        assert!(matches!(forged.verify(), Err(DmetaError::InvalidSignature)));
        let rejected = fold_checks(SignatureChecks {
            message: Evidence::Invalid,
            attribution: Evidence::Present,
            value: forged.value_evidence(FacetValue::Present(&old_value)),
        });
        assert_eq!(rejected, VerificationState::Rejected);
        let forged_id = forged.signature_id().unwrap();
        let new_value = NativeValue::Bytes(vec![0, 255, 42]);
        let replacement = FacetClaim {
            predecessors: vec![forged_id.clone()],
            ..claim_for(&new_value)
        };
        let (_, mut entry) = signed_entry(&replacement);
        entry.carry_message(&replacement).unwrap();
        let signed = entry.authenticate(&actor, None).unwrap();
        assert_eq!(signed.claim.predecessors, vec![forged_id]);
        let accepted = fold_checks(SignatureChecks {
            message: Evidence::Present,
            attribution: Evidence::Present,
            value: signed.value_evidence(FacetValue::Present(&new_value)),
        });
        assert_eq!(accepted, VerificationState::Accepted);
        assert_eq!(
            facet_verification([rejected, accepted]),
            VerificationState::Accepted
        );
        assert_eq!(
            facet_verification([accepted, rejected]),
            VerificationState::Accepted
        );
        assert!(matches!(forged.verify(), Err(DmetaError::InvalidSignature)));
    }

    #[test]
    fn a_static_openssl_proof_round_trips_and_verifies_without_document_history() {
        let fixture: Dmeta = serde_json::from_str(PORTABLE_PROOF).unwrap();
        let json = serde_json::to_value(&fixture).unwrap();
        assert_eq!(
            json,
            serde_json::from_str::<serde_json::Value>(PORTABLE_PROOF).unwrap()
        );
        let restored: Dmeta = serde_json::from_value(json).unwrap();
        assert_eq!(restored, fixture);
        let value = native_value();
        let claim = claim_for(&value);
        assert_eq!(restored.facet_uuids[&claim.chosen_uuid], claim.facet_key);
        let entry = &restored.facets[&claim.facet_key].signatures[0];
        let actor = &restored.actors[&entry.actor_uuid];
        assert_eq!(actor.delegation, Some(Bytes::from(vec![1, 2, 3, 255])));
        let signed = entry.authenticate(actor, None).unwrap();
        assert_eq!(signed.claim, claim);
        signed.verify().unwrap();
        let expected_id = data_encoding::HEXLOWER
            .decode(b"01551e20776f47997358fee3b289570cf1a2ef3104a2bbc4c623875c1324bf1fad6b828c")
            .unwrap();
        assert_eq!(
            signed.signature_id().unwrap().as_bytes().as_slice(),
            expected_id
        );
        assert_eq!(
            fold_checks(SignatureChecks {
                message: Evidence::Present,
                attribution: Evidence::Present,
                value: signed.value_evidence(FacetValue::Present(&value)),
            }),
            VerificationState::Accepted
        );
    }

    #[test]
    fn a_stale_signature_authenticates_but_does_not_cover_the_current_native_value() {
        let value = native_value();
        let claim = claim_for(&value);
        let (actor, mut entry) = signed_entry(&claim);
        entry.carry_message(&claim).unwrap();
        let signed = entry.authenticate(&actor, None).unwrap();
        let changed = NativeValue::Map(BTreeMap::from([
            ("a".into(), NativeValue::Uint(1)),
            ("b".into(), NativeValue::Bytes(vec![0, 255])),
        ]));
        for (current, evidence, state) in [
            (
                FacetValue::Present(&value),
                Evidence::Present,
                VerificationState::Accepted,
            ),
            (
                FacetValue::Present(&changed),
                Evidence::Invalid,
                VerificationState::Uncovered,
            ),
            (
                FacetValue::Removed,
                Evidence::Invalid,
                VerificationState::Uncovered,
            ),
            (
                FacetValue::Missing,
                Evidence::Missing,
                VerificationState::Pending,
            ),
        ] {
            signed.verify().unwrap();
            assert_eq!(signed.value_evidence(current), evidence);
            assert_eq!(
                fold_checks(SignatureChecks {
                    message: Evidence::Present,
                    attribution: Evidence::Present,
                    value: signed.value_evidence(current),
                }),
                state
            );
        }
    }

    utils_rs::table_tests! {
        verification_evidence,
        (missing, carried_message, expected),
        {
            let mut dmeta: Dmeta = serde_json::from_str(PORTABLE_PROOF).unwrap();
            let value = native_value();
            let claim = claim_for(&value);
            let node = SigningKey::from_bytes(&[42; 32]);
            let issuer = MultibaseKey::try_from(node.verifying_key().as_bytes().as_slice()).unwrap();
            let actor = dmeta.actors.get_mut(&claim.actor_uuid).unwrap();
            let attribution = Attribution {
                issuer: issuer.clone(), actor_uuid: claim.actor_uuid,
                pubkey: actor.pubkey.clone(), actor_url: actor.actor_url.clone(),
            };
            let signature = SignatureBytes::try_from(
                node.sign(&attribution.signing_bytes().unwrap()).to_bytes().as_slice(),
            ).unwrap();
            actor.delegation = Some(attribution.certificate(signature).unwrap());
            let mut issuers = BTreeMap::from([(claim.actor_uuid, issuer)]);
            if !carried_message {
                dmeta.facets.get_mut(&claim.facet_key).unwrap().signatures[0].message = None;
            }
            match missing {
                "none" => {}
                "actor" => { dmeta.actors.remove(&claim.actor_uuid).unwrap(); }
                "alias" => { dmeta.facet_uuids.remove(&claim.chosen_uuid).unwrap(); }
                "document" => { dmeta.branch_uuids.remove(&uuid7(9)).unwrap(); }
                "rebound document" => { dmeta.branch_uuids.insert(uuid7(9), DocId::new(b"other")).unwrap(); }
                "certificate" => dmeta.actors.get_mut(&claim.actor_uuid).unwrap().delegation = None,
                "issuer" => issuers.clear(),
                other => panic!("unknown case: {other}"),
            }
            assert_eq!(
                dmeta.verify_facet(
                    &claim.facet_key, FacetValue::Present(&value),
                    &BTreeMap::new(), &issuers,
                ),
                expected,
            );
        }
    }

    verification_evidence! {
        intact: ("none", true, VerificationState::Accepted),
        missing_actor_is_invalid: ("actor", true, VerificationState::Rejected),
        missing_alias_is_invalid: ("alias", true, VerificationState::Rejected),
        unbound_doc_uuid_is_invalid: ("document", true, VerificationState::Rejected),
        doc_uuid_bound_elsewhere_is_invalid: ("rebound document", true, VerificationState::Rejected),
        missing_certificate_is_invalid: ("certificate", true, VerificationState::Rejected),
        unknown_external_issuer_is_pending: ("issuer", true, VerificationState::Pending),
        unsupplied_message_is_pending: ("none", false, VerificationState::Pending),
        missing_message_does_not_hide_missing_certificate: ("certificate", false, VerificationState::Rejected),
    }

    #[test]
    fn empty_signature_sets_are_uncovered_and_competitors_do_not_poison_acceptance() {
        use VerificationState::{Accepted, Pending, Rejected, Uncovered};

        assert_eq!(facet_verification([]), Uncovered);
        assert_eq!(facet_verification([Uncovered]), Uncovered);
        assert_eq!(facet_verification([Rejected, Uncovered]), Rejected);
        assert_eq!(facet_verification([Pending, Rejected]), Pending);
        assert_eq!(facet_verification([Rejected, Pending]), Pending);
        assert_eq!(facet_verification([Uncovered, Pending]), Pending);
        assert_eq!(
            facet_verification([Accepted, Pending, Rejected, Uncovered]),
            Accepted
        );
        assert_eq!(
            facet_verification([Uncovered, Rejected, Pending, Accepted]),
            Accepted
        );
    }

    #[test]
    fn a_corrupted_other_facet_does_not_poison_a_valid_facet() {
        let value = native_value();
        let claim = claim_for(&value);
        let (actor, mut valid) = signed_entry(&claim);
        valid.carry_message(&claim).unwrap();
        let other_claim = FacetClaim {
            facet_key: facet_key("org.example.note/other"),
            chosen_uuid: uuid7(3),
            ..claim.clone()
        };
        let (_, mut corrupt) = signed_entry(&other_claim);
        corrupt.carry_message(&other_claim).unwrap();
        let mut bytes = *corrupt.signature.as_bytes();
        bytes[0] ^= 1;
        corrupt.signature = SignatureBytes::try_from(bytes.as_slice()).unwrap();
        let mut dmeta = Dmeta::new(Timestamp::from_second(0).unwrap());
        assert!(dmeta.actors.insert(claim.actor_uuid, actor).is_none());
        for (facet_claim, entry) in [(&claim, valid), (&other_claim, corrupt)] {
            dmeta
                .record_facet_alias(facet_claim.chosen_uuid, facet_claim.facet_key.clone())
                .unwrap();
            assert!(
                dmeta
                    .facets
                    .insert(
                        facet_claim.facet_key.clone(),
                        FacetMetadata {
                            updated_at: Vec::new(),
                            deleted_at: Vec::new(),
                            signatures: vec![entry],
                        }
                    )
                    .is_none()
            );
        }
        let states: BTreeMap<_, _> = dmeta
            .facets
            .iter()
            .map(|(key, metadata)| {
                let state = facet_verification(metadata.signatures.iter().map(|entry| {
                    let actor = &dmeta.actors[&entry.actor_uuid];
                    match entry.authenticate(actor, None) {
                        Ok(signed) => fold_checks(SignatureChecks {
                            message: Evidence::Present,
                            attribution: Evidence::Present,
                            value: signed.value_evidence(FacetValue::Present(&value)),
                        }),
                        Err(DmetaError::InvalidSignature) => VerificationState::Rejected,
                        Err(error) => panic!("unexpected authentication failure: {error}"),
                    }
                }));
                (key.clone(), state)
            })
            .collect();
        assert_eq!(
            states,
            BTreeMap::from([
                (claim.facet_key, VerificationState::Accepted),
                (other_claim.facet_key, VerificationState::Rejected),
            ])
        );
    }

    #[test]
    fn a_hostile_predecessor_count_is_refused_before_allocation() {
        let claim = FacetClaim {
            operation: ClaimOperation::Remove,
            value_cid: None,
            ..claim_for(&NativeValue::Null)
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
        use ciborium::Value as Cbor;

        let claim = claim_for(&native_value());
        let fields = claim.cbor_fields().unwrap();
        assert_eq!(FacetClaim::from_cbor_fields(&fields).unwrap(), claim);
        let signing = claim.signing_bytes().unwrap();
        assert_eq!(FacetClaim::from_signing_bytes(&signing).unwrap(), claim);
        assert!(matches!(
            FacetKey::from_str("org.example.note/"),
            Err(ReferenceError::MissingFacetKey)
        ));
        for length in 0..fields.len() {
            assert!(FacetClaim::from_cbor_fields(&fields[..length]).is_err());
        }
        let mut trailing = fields.clone();
        trailing.push(0);
        assert!(FacetClaim::from_cbor_fields(&trailing).is_err());
        let mut noncanonical = fields.clone();
        noncanonical.splice(0..1, [0x98, 7]);
        assert!(FacetClaim::from_cbor_fields(&noncanonical).is_err());
        let mut wrong_arity = fields.clone();
        wrong_arity[0] = 0x86;
        assert!(FacetClaim::from_cbor_fields(&wrong_arity).is_err());
        let mut invalid_utf8 = fields.clone();
        invalid_utf8[2] = 0xff;
        assert!(FacetClaim::from_cbor_fields(&invalid_utf8).is_err());
        let mut old_domain = b"daybook.facet-claim.v1\0".to_vec();
        old_domain.extend(&fields);
        assert!(FacetClaim::from_signing_bytes(&old_domain).is_err());
        assert!(FacetClaim::from_signing_bytes(&fields).is_err());
        let Cbor::Array(decoded) = ciborium::from_reader(&fields[..]).unwrap() else {
            panic!("claim fixture is not an array");
        };
        let reject_field = |index: usize, value: Cbor| {
            let mut malformed = decoded.clone();
            malformed[index] = value;
            let mut bytes = Vec::new();
            ciborium::into_writer(&Cbor::Array(malformed), &mut bytes).unwrap();
            assert!(FacetClaim::from_cbor_fields(&bytes).is_err());
        };
        for index in [2, 3] {
            for invalid in [
                Vec::new(),
                vec![0; 15],
                vec![0; 16],
                vec![0; 17],
                Uuid::parse_str("01890f3e-1234-4000-8000-000000000001")
                    .unwrap()
                    .as_bytes()
                    .to_vec(),
                Uuid::parse_str("01890f3e-1234-7000-0000-000000000001")
                    .unwrap()
                    .as_bytes()
                    .to_vec(),
            ] {
                reject_field(index, Cbor::Bytes(invalid));
            }
            reject_field(index, Cbor::Text(uuid7(1).to_string()));
        }
        reject_field(0, Cbor::Text("not a document ID".into()));
        reject_field(1, Cbor::Text("org.example.note/".into()));
        reject_field(4, Cbor::Integer(2.into()));
        reject_field(4, Cbor::Integer(1.into()));
        reject_field(5, Cbor::Null);
        reject_field(5, Cbor::Bytes(vec![0; 36]));
        reject_field(6, Cbor::Null);
        let mut invalid = claim.clone();
        invalid.value_cid = None;
        assert!(matches!(
            invalid.signing_bytes(),
            Err(DmetaError::InvalidClaim)
        ));
        invalid = claim;
        invalid.operation = ClaimOperation::Remove;
        assert!(matches!(
            invalid.signing_bytes(),
            Err(DmetaError::InvalidClaim)
        ));
    }

    #[test]
    fn an_inherited_claim_binds_the_source_context_not_the_branch() {
        let predecessor_a = cid_with(1);
        let predecessor_b = cid_with(2);
        let value = native_value();
        let source_claim = FacetClaim {
            origin_document: DocId::new(b"origin-a"),
            predecessors: vec![
                predecessor_b.clone(),
                predecessor_a.clone(),
                predecessor_b.clone(),
            ],
            ..claim_for(&value)
        };
        let (actor, mut witness) = signed_entry(&source_claim);
        witness.inherited = Some(InheritedWitness {
            origin_doc: source_claim.origin_document.clone(),
            message: Bytes::from(source_claim.signing_bytes().unwrap()),
            value: Some(Bytes::from(encode_native(&value))),
        });
        let signed = witness.authenticate(&actor, Some(&source_claim)).unwrap();
        // The witness's predecessors were sorted and deduplicated before signing, so the normalized
        // form is the same claim.
        let mut normalized = source_claim.clone();
        normalized.predecessors = vec![predecessor_a.clone(), predecessor_b];
        normalized.predecessors.sort();
        assert_eq!(signed.claim, normalized);
        assert_eq!(
            witness.authenticate(&actor, Some(&normalized)).unwrap(),
            signed
        );
        let mut wrong_frontier = normalized.clone();
        wrong_frontier.predecessors = vec![predecessor_a];
        let mut wrong_facet = source_claim.clone();
        wrong_facet.facet_key = facet_key("org.example.note/wrong");
        let mut wrong_uuid = source_claim.clone();
        wrong_uuid.chosen_uuid = uuid7(10);
        let mut wrong_actor = source_claim.clone();
        wrong_actor.actor_uuid = uuid7(10);
        let mut wrong_origin = source_claim;
        wrong_origin.origin_document = DocId::new(b"branch-b");
        for wrong in [
            wrong_frontier,
            wrong_facet,
            wrong_uuid,
            wrong_actor,
            wrong_origin,
        ] {
            assert!(matches!(
                witness.authenticate(&actor, Some(&wrong)),
                Err(DmetaError::WrongContext)
            ));
        }
        let mut wrong_origin = witness.clone();
        wrong_origin.inherited.as_mut().unwrap().origin_doc = DocId::new(b"branch-b");
        assert!(matches!(
            wrong_origin.authenticate(&actor, None),
            Err(DmetaError::WrongContext)
        ));
        let mut wrong_actor = witness.clone();
        wrong_actor.actor_uuid = uuid7(10);
        assert!(matches!(
            wrong_actor.authenticate(&actor, None),
            Err(DmetaError::WrongContext)
        ));
        let mut double_carrier = witness.clone();
        double_carrier.message = Some(Bytes::from(normalized.signing_bytes().unwrap()));
        assert!(matches!(
            double_carrier.authenticate(&actor, None),
            Err(DmetaError::WrongContext)
        ));
        let before = witness.clone();
        assert!(matches!(
            witness.carry_message(&normalized),
            Err(DmetaError::WrongContext)
        ));
        assert_eq!(witness, before);
    }

    #[test]
    fn portable_messages_bind_actor_and_expected_context() {
        let claim = claim_for(&native_value());
        let (actor, mut entry) = signed_entry(&claim);
        let reconstructed = entry.authenticate(&actor, Some(&claim)).unwrap();
        let mut wrong_actor = claim.clone();
        wrong_actor.actor_uuid = uuid7(3);
        assert!(matches!(
            entry.carry_message(&wrong_actor),
            Err(DmetaError::WrongContext)
        ));
        assert!(entry.message.is_none());
        entry.carry_message(&claim).unwrap();
        assert_eq!(entry.authenticate(&actor, None).unwrap(), reconstructed);
        assert_eq!(
            entry.authenticate(&actor, Some(&claim)).unwrap(),
            reconstructed
        );
        let mut wrong_context = claim.clone();
        wrong_context.facet_key = facet_key("org.example.note/other");
        assert!(matches!(
            entry.authenticate(&actor, Some(&wrong_context)),
            Err(DmetaError::WrongContext)
        ));
        let mut wrong_entry = entry.clone();
        wrong_entry.actor_uuid = uuid7(3);
        assert!(matches!(
            wrong_entry.authenticate(&actor, None),
            Err(DmetaError::WrongContext)
        ));
        let other_key = SigningKey::from_bytes(&[8; 32]);
        let other_actor = ActorMetadata {
            pubkey: MultibaseKey::try_from(other_key.verifying_key().as_bytes().as_slice())
                .unwrap(),
            ..actor.clone()
        };
        assert!(matches!(
            entry.authenticate(&other_actor, None),
            Err(DmetaError::InvalidSignature)
        ));
        entry.message = Some(Bytes::from(wrong_context.signing_bytes().unwrap()));
        assert!(matches!(
            entry.authenticate(&actor, None),
            Err(DmetaError::InvalidSignature)
        ));
    }

    #[test]
    fn a_facet_key_with_slashes_and_non_utf8_survives_the_claim_and_the_uri() {
        let key = FacetKey::new("org.example.note", b"main/bytes/\x80".to_vec()).unwrap();
        let claim = FacetClaim {
            origin_document: DocId::new(b"doc"),
            facet_key: key.clone(),
            chosen_uuid: uuid7(12),
            actor_uuid: uuid7(2),
            operation: ClaimOperation::Remove,
            value_cid: None,
            predecessors: vec![],
        };
        let encoded = claim.cbor_fields().unwrap();
        assert_eq!(FacetClaim::from_cbor_fields(&encoded).unwrap(), claim);
        assert_eq!(FacetKey::from_str(&key.to_string()).unwrap(), key);
        assert!(key.to_string().contains("\\x80"));

        let reference = FacetReference::new(ReferenceDocId::SelfDoc, key.clone()).unwrap();
        let uri = reference.to_uri_string().unwrap();
        assert!(uri.contains("%5Cx80"));
        assert_eq!(FacetReference::parse(&uri).unwrap().key(), &key);
        assert_eq!(reference.key(), &key);
    }

    #[test]
    fn predecessors_are_sorted_and_deduplicated_before_signing() {
        use ciborium::Value as Cbor;

        let mut predecessors = [cid_with(1), cid_with(2)];
        predecessors.sort();
        let [first, second] = predecessors;
        let claim = FacetClaim {
            operation: ClaimOperation::Remove,
            value_cid: None,
            predecessors: vec![second.clone(), first.clone(), second.clone()],
            ..claim_for(&NativeValue::Null)
        };
        let bytes = claim.cbor_fields().unwrap();
        let normalized = FacetClaim {
            predecessors: vec![first.clone(), second.clone()],
            ..claim.clone()
        };
        assert_eq!(FacetClaim::from_cbor_fields(&bytes).unwrap(), normalized);
        assert_eq!(
            claim.signing_bytes().unwrap(),
            normalized.signing_bytes().unwrap()
        );
        let (actor, entry) = signed_entry(&claim);
        let signed = entry.authenticate(&actor, Some(&claim)).unwrap();
        let normalized_signed = entry.authenticate(&actor, Some(&normalized)).unwrap();
        assert_eq!(
            signed.signature_id().unwrap(),
            normalized_signed.signature_id().unwrap()
        );
        let Cbor::Array(fields) = ciborium::from_reader(&bytes[..]).unwrap() else {
            panic!("claim fixture is not an array");
        };
        for malformed_predecessors in [vec![second, first.clone()], vec![first.clone(), first]] {
            let mut malformed = fields.clone();
            malformed[6] = Cbor::Array(
                malformed_predecessors
                    .into_iter()
                    .map(|cid| Cbor::Bytes(cid.as_bytes().to_vec()))
                    .collect(),
            );
            let mut encoded = Vec::new();
            ciborium::into_writer(&Cbor::Array(malformed), &mut encoded).unwrap();
            assert!(matches!(
                FacetClaim::from_cbor_fields(&encoded),
                Err(DmetaError::MalformedClaim(
                    "predecessors must be sorted and unique"
                ))
            ));
        }
    }
}
