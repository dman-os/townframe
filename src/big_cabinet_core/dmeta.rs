//! Document metadata and signed datum-claim primitives.
//!
//! This module deliberately does not know about Automerge or authority storage. A repository
//! reconstructs an ordinary claim from the state at the change that introduced its signature; this
//! module owns the stable bytes that are signed, the compact metadata carried by dmeta, and the
//! distinction between missing evidence and proven forgery.
//!
//! Dmeta is an ordinary datum — `org.example.daybook.dmeta/main` — and the document repository is
//! the only writer of it. Its shape is the one in `docs/wiki/docs.md`: aliases live in one forward
//! index, identities are multibase text, and branch relationships are not duplicated here. A branch
//! is a weak link recorded by the branch datums (ADR 007); dmeta signs those datums, it does not
//! restate them.
//!
//! A claim authenticates the value in its **native storage form**, not its canonical JSON. The
//! JSON spelling of a stored value is a function of the installed declaration — a float leaves as a
//! decimal string, a counter as a whole number, bytes as `{ "$bytes": "…" }` — so a signature over
//! JSON could not be checked on a node that has not installed the datum's schema. The native value
//! is self-describing, so verification needs no schema at all: an absent schema leaves the datum
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

pub use attribution::Attribution;
pub use attribution::AttributionError;
#[cfg(feature = "schemars")]
pub use storage::StorageError;

use jiff::Timestamp;
pub use native::CanonicalF64;
pub use native::NativeReadBudget;
pub use native::NativeReadError;
pub use native::NativeValue;
pub use native::encode_native;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::str::FromStr;
use uuid::Uuid;
pub use verification::DatumValue;
pub use verification::Evidence;
pub use verification::SignatureChecks;
pub use verification::SignedDatumClaim;
pub use verification::VerificationState;
pub use verification::datum_verification;
pub use verification::fold_checks;
pub use verification::signature_id;

use crate::reference::DatumKey;

pub const DMETA_DATUM_TAG: &str = "org.example.daybook.dmeta";
const CLAIM_DOMAIN: &[u8] = b"daybook.datum-claim.v2\0";
const CLAIM_ID_DOMAIN: &[u8] = b"daybook.datum-claim-id.v2\0";
const CID_VERSION_RAW_BLAKE3: [u8; 4] = [0x01, 0x55, 0x1e, 0x20];
const CID_LENGTH: usize = 36;

/// The document identity shared with BigRepo: arbitrary bytes with canonical multibase text.
pub use big_sync_core::ObjKey as DocId;

/// Identity allocated before initial content is persisted and publication begins.
///
/// The document's own id is not a field of [`Dmeta`]: the enclosing document already knows it, and
/// a field inside the datum would be copied verbatim onto a branch, where it would name the origin
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

/// The compact provenance datum. Maps are ordered so snapshots and derived evidence are stable.
///
/// There is one index of aliases, `datum_uuids`, keyed by the alias. A per-datum reverse list would
/// be a second copy of the same facts, and the two copies are exactly what a merge can pull apart.
/// The chosen alias for a signature — the lexicographically smallest one visible in the writing
/// transaction — is derived from this index by the repository, which holds the reverse lookup.
#[cfg_attr(feature = "schemars", macros::datum(id = "org.example.daybook.dmeta"))]
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
    pub datum_uuids: BTreeMap<Uuid7, DatumKey>,
    #[serde(default)]
    pub datums: BTreeMap<DatumKey, DatumMetadata>,
    /// Members a newer dmeta wrote that this declaration does not name. An open datum keeps them,
    /// so a reader that has not been taught a new field does not delete it on the way back.
    #[serde(flatten)]
    pub extra: crate::datum::Extra,
}

impl Dmeta {
    pub fn new(created_at: Timestamp) -> Self {
        Self {
            created_at,
            updated_at: Vec::new(),
            actors: BTreeMap::new(),
            branch_uuids: BTreeMap::new(),
            datum_uuids: BTreeMap::new(),
            datums: BTreeMap::new(),
            extra: crate::datum::Extra::new(),
        }
    }

    /// The UUID a signature introduced on `document` carries as its `docUuid`: the smallest one
    /// already bound to `document`, or `minted` when none is.
    ///
    /// A normal branch *is* its origin's changes under a new [`DocId`], so it arrives with the
    /// origin's bindings and none of its own. Its first write — the branch's own
    /// `org.example.daybook.branch/<docid>` datum — binds a fresh UUID to it, so every later
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
    pub fn record_datum_alias(&mut self, uuid: Uuid7, key: DatumKey) -> Result<(), DmetaError> {
        match self.datum_uuids.entry(uuid) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(key);
                Ok(())
            }
            std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &key => Ok(()),
            std::collections::btree_map::Entry::Occupied(_) => Err(DmetaError::ReboundAlias),
        }
    }

    /// Whether more than one creator independently opened this slot.
    pub fn has_concurrent_creation(&self, key: &DatumKey) -> bool {
        self.datum_uuids
            .values()
            .filter(|alias| *alias == key)
            .count()
            > 1
    }
}

/// What dmeta records about one datum slot.
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
pub struct DatumMetadata {
    #[serde(default)]
    pub updated_at: Vec<Timestamp>,
    pub deleted_at: Vec<Timestamp>,
    #[serde(default)]
    pub signatures: Vec<DatumSignature>,
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

pub const BRANCH_DATUM_TAG: &str = "org.example.daybook.branch";

/// The branch side of a branch: `org.example.daybook.branch/<docid>` on the branch document,
/// naming that document's branch path.
///
/// The datum id carries the document id, not the value, because a branch merged back brings this
/// datum along: the origin then holds the branch's entry under the branch's id, and still finds
/// its own path (if it is itself a branch) under its own id. Only Branch writes it.
#[cfg_attr(feature = "schemars", macros::datum(id = "org.example.daybook.branch"))]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BranchPath {
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<BranchOrigin>,
    #[serde(flatten)]
    pub extra: crate::datum::Extra,
}

// FIXME: this isn't an open struct and yet i'm seeing
// no requirements by the datum macro to declare it closed
// this is an issue. first of all, this should be open
// and second of all, for it to be closed, there should have been
// an explicit closed marker on here
// for third party types, the embedding site can declare it
// as "i know this is closed"
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BranchOrigin {
    pub datum_ref: crate::DatumReference,
    #[cfg_attr(feature = "schemars", schemars(with = "Vec<String>"))]
    pub ref_heads: Vec<automerge::ChangeHash>,
}

/// One entry in a datum's competing set of signatures.
///
/// `inherited` is set only for a signature carried over a compaction, where the history its
/// message would be reconstructed from is gone. Every other signature, including one that arrived
/// with a branch's or a merge's changes, has none.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct DatumSignature {
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

/// The logical fields authenticated by a datum signature.
///
/// `value_cid` is a CIDv1 (`raw` codec, BLAKE3-256) over the canonical encoding of the value in its
/// **native storage form**, read back from the transaction after the content mutation. It is absent
/// only for a removal. The encoding itself belongs to the repository; this type only ever holds the
/// resulting CID, so it cannot drift from the bytes that were stored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatumClaim {
    /// The document the claim was made on. A branch or a merge later moves the signature to other
    /// documents unchanged, so this is not necessarily the document that now holds it; the entry
    /// names this document through its `docUuid` and the `branchUuids` binding.
    pub origin_document: DocId,
    pub datum_key: DatumKey,
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
/// which is what makes the datum publishable: an array of numbers is not a Lexicon type.
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
        crate::datum::codec_schema(
            crate::datum::Codec::Bytes,
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
    #[error("an actor or datum alias must be an RFC 9562 UUIDv7")]
    InvalidUuidVersion,
    #[error("a datum UUID alias cannot be rebound to another key")]
    ReboundAlias,
    #[error("signed message evidence is unavailable")]
    MissingMessage,
    #[error("signature and claim contexts disagree")]
    WrongContext,
    #[error("claim operation and value CID disagree")]
    InvalidClaim,
    #[error("malformed datum claim: {0}")]
    MalformedClaim(&'static str),
    #[error("invalid Ed25519 public key")]
    InvalidPublicKey,
    #[error("invalid Ed25519 signature")]
    InvalidSignature,
}

impl DatumClaim {
    /// Deterministic RFC 8949 array encoding from ADR 020 §4.
    pub fn cbor_fields(&self) -> Result<Vec<u8>, DmetaError> {
        if matches!(self.operation, ClaimOperation::Set) != self.value_cid.is_some() {
            return Err(DmetaError::InvalidClaim);
        }
        let mut out = Vec::new();
        cbor_array(&mut out, 7);
        cbor_text(&mut out, &self.origin_document.to_string());
        cbor_text(&mut out, &self.datum_key.to_string());
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
        let datum_key_text = read_cbor_text_strict(bytes, &mut cursor)?;
        let datum_key = DatumKey::from_str(&datum_key_text)
            .map_err(|_| DmetaError::MalformedClaim("invalid datum key"))?;
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
            datum_key,
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
/// the canonical native encoding of the datum value, which the repository produces; the bytes for a
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
mod tests;
