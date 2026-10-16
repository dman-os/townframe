//! Document metadata and signed facet-claim primitives.
//!
//! This module deliberately does not know about Automerge or authority storage.  A repository
//! reconstructs an ordinary claim from its introducing change; this module owns the stable bytes
//! that are signed, the compact metadata carried by dmeta, and the distinction between missing
//! evidence and proven forgery.

use blake3::Hasher;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::str::FromStr;
use uuid::Uuid;

use crate::reference::FacetKey;

pub const DMETA_FACET_TAG: &str = "org.example.daybook.dmeta";
const CLAIM_DOMAIN: &[u8] = b"daybook.facet-claim.v1\0";
const CLAIM_ID_DOMAIN: &[u8] = b"daybook.facet-claim-id.v1\0";
const CID_VERSION_RAW_BLAKE3: [u8; 4] = [0x01, 0x55, 0x1e, 0x20];
const CID_LENGTH: usize = 36;

/// The document identity shared with BigRepo: arbitrary bytes with canonical multibase text.
pub use big_sync_core::ObjKey as DocumentId;

/// Identity allocated before initial content is persisted and publication begins.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentIdentity {
    pub id: DocumentId,
    pub created_at: Timestamp,
}

/// A weak branch relationship.  Neither endpoint has to be available to interpret this fact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BranchOrigin {
    pub document_id: DocumentId,
    #[serde(default)]
    pub heads: Vec<Vec<u8>>,
}

/// A branch relationship recorded on the source document.  Branches remain ordinary documents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreatedBranch {
    pub document_id: DocumentId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorMetadata {
    pub actor_url: String,
    #[serde(default)]
    pub delegation: Option<Vec<u8>>,
    pub created_at: Timestamp,
}

/// The compact provenance facet.  Maps are ordered so snapshots and derived evidence are stable.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Dmeta {
    pub id: DocumentId,
    pub created_at: Timestamp,
    #[serde(default)]
    pub updated_at: Vec<Timestamp>,
    #[serde(default)]
    pub actors: BTreeMap<String, ActorMetadata>,
    #[serde(default)]
    pub facet_uuids: BTreeMap<Uuid, String>,
    #[serde(default)]
    pub facets: BTreeMap<String, FacetMetadata>,
    /// Present only for a branch document; this is a weak link, not an authority parent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_from: Option<BranchOrigin>,
    /// Facts on the source document.  Merging a branch does not remove them.
    #[serde(default)]
    pub created_branches: Vec<CreatedBranch>,
}

impl Dmeta {
    pub fn new(identity: DocumentIdentity) -> Self {
        Self {
            id: identity.id,
            created_at: identity.created_at,
            updated_at: Vec::new(),
            actors: BTreeMap::new(),
            facet_uuids: BTreeMap::new(),
            facets: BTreeMap::new(),
            created_from: None,
            created_branches: Vec::new(),
        }
    }

    /// Record an alias without selecting content.  Multiple aliases are durable evidence of
    /// concurrent creation even after Automerge no longer exposes the original conflict.
    pub fn record_facet_uuid(&mut self, uuid: Uuid, key: impl Into<String>) {
        self.facet_uuids.insert(uuid, key.into());
    }

    /// The lexicographically smallest alias visible in this writing transaction.
    pub fn selected_facet_uuid(&self, key: &str) -> Option<Uuid> {
        self.facet_uuids
            .iter()
            .filter(|(_, value)| value.as_str() == key)
            .map(|(uuid, _)| *uuid)
            .min_by(|left, right| left.as_bytes().cmp(right.as_bytes()))
    }

    pub fn has_concurrent_creation(&self, key: &str) -> bool {
        self.facet_uuids
            .values()
            .filter(|value| value.as_str() == key)
            .count()
            > 1
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FacetMetadata {
    pub created_at: Timestamp,
    #[serde(default)]
    pub uuids: Vec<Uuid>,
    #[serde(default)]
    pub updated_at: Vec<Timestamp>,
    #[serde(default)]
    pub deleted_at: Vec<Timestamp>,
    #[serde(default)]
    pub signatures: Vec<FacetSignature>,
}

impl FacetMetadata {
    /// Select the smallest alias visible at the transaction frontier.
    pub fn selected_uuid(&self) -> Option<Uuid> {
        self.uuids
            .iter()
            .copied()
            .min_by(|left, right| left.as_bytes().cmp(right.as_bytes()))
    }

    pub fn has_concurrent_creation(&self) -> bool {
        self.uuids.len() > 1
    }
}

/// Evidence copied when a branch retains a source signature but not its Automerge history.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InheritedWitness {
    pub origin_document: DocumentId,
    pub message: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FacetSignature {
    pub actor_public_key: Vec<u8>,
    pub signature: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "originDoc")]
    pub origin_doc: Option<DocumentId>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "inheritedMessage"
    )]
    pub inherited_message: Option<Vec<u8>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "inheritedValue"
    )]
    pub inherited_value: Option<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClaimOperation {
    Set,
    Remove,
}

/// The logical fields authenticated by a facet signature.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FacetClaim {
    pub origin_document: DocumentId,
    pub facet_key: FacetKey,
    pub chosen_uuid: Uuid,
    pub operation: ClaimOperation,
    /// A CIDv1 raw/BLAKE3-256 byte string; absent only for Remove.
    pub value_cid: Option<Vec<u8>>,
    #[serde(default)]
    pub predecessors: Vec<Vec<u8>>,
}

#[derive(Debug, thiserror::Error)]
pub enum DmetaError {
    #[error("JCS cannot encode the facet value: {0}")]
    CanonicalJson(#[from] serde_json::Error),
    #[error("facet value contains a non-finite JSON number")]
    NonFiniteNumber,
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

/// Canonical RFC 8785 bytes of a JSON value.
pub fn canonical_json(value: &Value) -> Result<Vec<u8>, DmetaError> {
    reject_non_finite(value)?;
    Ok(json_canon::to_vec(value)?)
}

/// CIDv1 (`raw` codec, BLAKE3-256 multihash) of canonical JSON bytes.
pub fn value_cid(value: &Value) -> Result<Vec<u8>, DmetaError> {
    Ok(cid_bytes(&canonical_json(value)?))
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
            Some(cid) if is_cid(cid) => cbor_bytes(&mut out, cid),
            Some(_) => return Err(DmetaError::InvalidClaim),
            None => out.push(0xf6),
        }
        let mut predecessors = self.predecessors.clone();
        predecessors.sort();
        predecessors.dedup();
        if predecessors.iter().any(|id| !is_cid(id)) {
            return Err(DmetaError::InvalidPredecessor);
        }
        cbor_array(&mut out, predecessors.len() as u64);
        for predecessor in predecessors {
            cbor_bytes(&mut out, &predecessor);
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
    /// This is deliberately stricter than a general CBOR decoder: all six fields have
    /// fixed types, arrays are definite, integers use their shortest representation,
    /// predecessor CIDs are strictly sorted and unique, and no trailing bytes survive.
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
            let value = read_cbor_bytes_strict(bytes, &mut cursor)?;
            if !is_cid(&value) {
                return Err(DmetaError::MalformedClaim(
                    "value must be a CIDv1 raw/BLAKE3-256",
                ));
            }
            Some(value)
        };
        if matches!(operation, ClaimOperation::Set) != value_cid.is_some() {
            return Err(DmetaError::InvalidClaim);
        }

        let (predecessor_major, predecessor_count) = read_cbor_head_strict(bytes, &mut cursor)?;
        if predecessor_major != 4 {
            return Err(DmetaError::MalformedClaim("predecessors must be an array"));
        }
        // Every predecessor needs at least a one-byte CBOR bytes head and one complete
        // 36-byte CID. Bound the count by the actual input before converting to usize or
        // reserving, so hostile counts cannot request an unbounded allocation.
        let remaining = bytes.len().saturating_sub(cursor);
        let minimum_predecessor_encoding = 1_u64 + CID_LENGTH as u64;
        if predecessor_count > remaining as u64 / minimum_predecessor_encoding {
            return Err(DmetaError::MalformedClaim(
                "predecessor count exceeds input budget",
            ));
        }
        let mut predecessors = Vec::with_capacity(predecessor_count as usize);
        for _ in 0..predecessor_count {
            let predecessor = read_cbor_bytes_strict(bytes, &mut cursor)?;
            if !is_cid(&predecessor) {
                return Err(DmetaError::MalformedClaim(
                    "predecessor must be a CIDv1 raw/BLAKE3-256",
                ));
            }
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignedFacetClaim {
    pub actor_public_key: Vec<u8>,
    pub signature: Vec<u8>,
    pub claim: FacetClaim,
}

impl SignedFacetClaim {
    /// Assemble a claim from the actor public identity and signature returned by the
    /// secrets-only authority boundary. The core never receives or exports a private key.
    pub fn from_signature(
        claim: FacetClaim,
        actor_public_key: Vec<u8>,
        signature: Vec<u8>,
    ) -> Result<Self, DmetaError> {
        let signed = Self {
            actor_public_key,
            signature,
            claim,
        };
        signed.verify()?;
        Ok(signed)
    }

    pub fn verify(&self) -> Result<(), DmetaError> {
        let key: [u8; 32] = self
            .actor_public_key
            .as_slice()
            .try_into()
            .map_err(|_| DmetaError::InvalidPublicKey)?;
        let signature: [u8; 64] = self
            .signature
            .as_slice()
            .try_into()
            .map_err(|_| DmetaError::InvalidSignature)?;
        let key = VerifyingKey::from_bytes(&key).map_err(|_| DmetaError::InvalidPublicKey)?;
        key.verify(
            &self.claim.signing_bytes()?,
            &Signature::from_bytes(&signature),
        )
        .map_err(|_| DmetaError::InvalidSignature)
    }

    /// Stable claim identity, independent of the containing Automerge change.
    pub fn signature_id(&self) -> Result<Vec<u8>, DmetaError> {
        let mut bytes = CLAIM_ID_DOMAIN.to_vec();
        bytes.extend(&self.actor_public_key);
        bytes.extend(self.claim.signing_bytes()?);
        bytes.extend(&self.signature);
        Ok(cid_bytes(&bytes))
    }
}

/// Verification is intentionally three-valued: absent evidence is not a forgery.
impl FacetSignature {
    /// Verify a carried branch witness without pretending that a copied merged value was signed.
    /// Missing witness material is retryable (`Pending`); malformed or cryptographically invalid
    /// material is `ProvenForgery`.
    /// Verify an inherited witness against the source context reconstructed by the
    /// repository.  The destination branch is intentionally absent from this context:
    /// a valid inherited claim names its origin document, not the branch carrying it.
    pub fn verify_inherited_for(
        &self,
        expected_claim: &FacetClaim,
        copied_value: Option<&Value>,
    ) -> VerificationState {
        let Some(message) = &self.inherited_message else {
            return VerificationState::Pending;
        };
        let Ok(claim) = FacetClaim::from_signing_bytes(message) else {
            return VerificationState::ProvenForgery;
        };
        let Ok(expected_message) = expected_claim.signing_bytes() else {
            return VerificationState::ProvenForgery;
        };
        if self.origin_doc.as_ref() != Some(&expected_claim.origin_document)
            || claim.signing_bytes().ok().as_deref() != Some(expected_message.as_slice())
        {
            return VerificationState::ProvenForgery;
        }
        self.verify_inherited_claim(&claim, copied_value)
    }

    fn verify_inherited_claim(
        &self,
        claim: &FacetClaim,
        copied_value: Option<&Value>,
    ) -> VerificationState {
        let Some(message) = &self.inherited_message else {
            return VerificationState::Pending;
        };
        let expected_cid = match (
            &claim.operation,
            &claim.value_cid,
            &self.inherited_value,
            copied_value,
        ) {
            (ClaimOperation::Remove, None, Some(_), _) => return VerificationState::ProvenForgery,
            (ClaimOperation::Remove, None, None, _) => None,
            (ClaimOperation::Set, Some(cid), Some(value), _) => match value_cid(value) {
                Ok(actual) if actual == *cid => Some(cid.clone()),
                Ok(_) => return VerificationState::ProvenForgery,
                Err(_) => return VerificationState::ProvenForgery,
            },
            (ClaimOperation::Set, Some(cid), None, Some(value)) => match value_cid(value) {
                Ok(actual) if actual == *cid => Some(cid.clone()),
                Ok(_) => return VerificationState::ProvenForgery,
                Err(_) => return VerificationState::ProvenForgery,
            },
            (ClaimOperation::Set, Some(_), None, None) => return VerificationState::Pending,
            _ => return VerificationState::ProvenForgery,
        };
        self.verify_inherited_bytes(message, expected_cid)
    }

    fn verify_inherited_bytes(
        &self,
        message: &[u8],
        expected_cid: Option<Vec<u8>>,
    ) -> VerificationState {
        let Ok(key_bytes) = <[u8; 32]>::try_from(self.actor_public_key.as_slice()) else {
            return VerificationState::ProvenForgery;
        };
        let Ok(signature_bytes) = <[u8; 64]>::try_from(self.signature.as_slice()) else {
            return VerificationState::ProvenForgery;
        };
        let Ok(key) = VerifyingKey::from_bytes(&key_bytes) else {
            return VerificationState::ProvenForgery;
        };
        if !message.starts_with(CLAIM_DOMAIN)
            || key
                .verify(message, &Signature::from_bytes(&signature_bytes))
                .is_err()
        {
            return VerificationState::ProvenForgery;
        }
        match cbor_value_cid(message) {
            Some(signed_cid) if signed_cid == expected_cid => VerificationState::Accepted,
            Some(_) => VerificationState::ProvenForgery,
            None => VerificationState::ProvenForgery,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerificationState {
    Accepted,
    Pending,
    ProvenForgery,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Evidence {
    Present,
    Missing,
    Invalid,
}

pub fn verification_state(evidence: Evidence) -> VerificationState {
    match evidence {
        Evidence::Present => VerificationState::Accepted,
        Evidence::Missing => VerificationState::Pending,
        Evidence::Invalid => VerificationState::ProvenForgery,
    }
}

/// Whole-document closure for v1: any proven bad evidence closes ordinary reads and writes, while
/// pending evidence remains retryable.
pub fn document_verification(
    states: impl IntoIterator<Item = VerificationState>,
) -> VerificationState {
    let mut pending = false;
    for state in states {
        match state {
            VerificationState::ProvenForgery => return VerificationState::ProvenForgery,
            VerificationState::Pending => pending = true,
            VerificationState::Accepted => {}
        }
    }
    if pending {
        VerificationState::Pending
    } else {
        VerificationState::Accepted
    }
}

fn reject_non_finite(value: &Value) -> Result<(), DmetaError> {
    match value {
        Value::Array(values) => values.iter().try_for_each(reject_non_finite),
        Value::Object(values) => values.values().try_for_each(reject_non_finite),
        Value::Number(number)
            if !number.is_i64() && !number.is_u64() && number.as_f64().is_none() =>
        {
            Err(DmetaError::NonFiniteNumber)
        }
        _ => Ok(()),
    }
}

fn cid_bytes(data: &[u8]) -> Vec<u8> {
    let mut cid = CID_VERSION_RAW_BLAKE3.to_vec();
    let mut hasher = Hasher::new();
    hasher.update(data);
    cid.extend(hasher.finalize().as_bytes());
    cid
}

fn cbor_value_cid(message: &[u8]) -> Option<Option<Vec<u8>>> {
    FacetClaim::from_signing_bytes(message)
        .ok()
        .map(|claim| claim.value_cid)
}

fn is_cid(value: &[u8]) -> bool {
    value.len() == CID_LENGTH && value[..4] == CID_VERSION_RAW_BLAKE3
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

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};
    use serde_json::json;

    use crate::{FacetReference, ReferenceDocumentId, ReferenceError};

    fn facet_key(value: &str) -> FacetKey {
        FacetKey::from_str(value).unwrap()
    }

    #[test]
    fn aliases_select_smallest_and_retain_concurrency() {
        let timestamp = Timestamp::from_second(0).unwrap();
        let mut dmeta = Dmeta::new(DocumentIdentity {
            id: DocumentId::new(b"doc"),
            created_at: timestamp,
        });
        let key = "org.example.note/main";
        let high = Uuid::from_u128(2);
        let low = Uuid::from_u128(1);
        dmeta.record_facet_uuid(high, key);
        dmeta.record_facet_uuid(low, key);
        assert_eq!(dmeta.selected_facet_uuid(key), Some(low));
        assert!(dmeta.has_concurrent_creation(key));
    }

    #[test]
    fn canonical_json_and_value_cid_vector_is_stable() {
        let value = json!({"b": 2, "a": 1});
        assert_eq!(canonical_json(&value).unwrap(), br#"{"a":1,"b":2}"#);
        let numeric = json!({"exp": 1e-7, "safe": 9007199254740991_i64});
        assert_eq!(
            canonical_json(&numeric).unwrap(),
            br#"{"exp":1e-7,"safe":9007199254740991}"#
        );
        let safe_edges = json!({"low": -9007199254740991_i64, "high": 9007199254740991_i64});
        assert_eq!(
            canonical_json(&safe_edges).unwrap(),
            br#"{"high":9007199254740991,"low":-9007199254740991}"#
        );
        let outside_edges = json!({"low": -9007199254740992_i64, "high": 9007199254740992_i64});
        assert!(canonical_json(&outside_edges).is_err());
        let finite_spellings = json!({"decimal": 1.25, "lower": 1e-6, "upper": 1e21, "zero": -0.0});
        assert_eq!(
            canonical_json(&finite_spellings).unwrap(),
            br#"{"decimal":1.25,"lower":0.000001,"upper":1e+21,"zero":0}"#
        );
        // serde_json rejects non-finite JSON tokens before a Value can be constructed;
        // do not fabricate an impossible NaN/Infinity Value just to exercise this boundary.
        assert!(serde_json::from_str::<Value>("NaN").is_err());
        assert!(serde_json::from_str::<Value>("Infinity").is_err());
        assert_eq!(
            value_cid(&value).unwrap(),
            vec![
                0x01, 0x55, 0x1e, 0x20, 0x8e, 0x80, 0x43, 0x9b, 0x77, 0xac, 0x62, 0xd4, 0x19, 0x44,
                0x99, 0xed, 0xd4, 0x66, 0x84, 0xc4, 0x79, 0xda, 0x3a, 0xa1, 0xac, 0x80, 0xdd, 0x55,
                0x11, 0x46, 0x8e, 0xfa, 0xe0, 0x49, 0x16, 0x6e,
            ]
        );
    }

    #[test]
    fn claim_cbor_and_ed25519_vectors_are_stable() {
        let claim = FacetClaim {
            origin_document: DocumentId::new(b"doc"),
            facet_key: facet_key("org.example.note/main"),
            chosen_uuid: Uuid::from_u128(1),
            operation: ClaimOperation::Set,
            value_cid: Some(value_cid(&json!({"b": 2, "a": 1})).unwrap()),
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
            ciborium::Value::Bytes(vec![
                0x01, 0x55, 0x1e, 0x20, 0x8e, 0x80, 0x43, 0x9b, 0x77, 0xac, 0x62, 0xd4, 0x19, 0x44,
                0x99, 0xed, 0xd4, 0x66, 0x84, 0xc4, 0x79, 0xda, 0x3a, 0xa1, 0xac, 0x80, 0xdd, 0x55,
                0x11, 0x46, 0x8e, 0xfa, 0xe0, 0x49, 0x16, 0x6e,
            ])
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
            value_cid: Some(value_cid(&json!({"b": 2, "a": 1})).unwrap()),
            predecessors: vec![],
        };
        let message = claim.signing_bytes().unwrap();
        let signed = SignedFacetClaim::from_signature(
            claim.clone(),
            key.verifying_key().to_bytes().to_vec(),
            key.sign(&message).to_bytes().to_vec(),
        )
        .unwrap();
        signed.verify().unwrap();
        assert_eq!(signed.signature_id().unwrap().len(), 36);
        let mut forged = signed.clone();
        forged.claim.facet_key = facet_key("org.example.note/mainx");
        assert!(matches!(forged.verify(), Err(DmetaError::InvalidSignature)));
    }

    #[test]
    fn inherited_witness_verification_is_pending_or_accepted_not_optimistic() {
        let key = SigningKey::from_bytes(&[9; 32]);
        let value = json!({"answer": 42});
        let claim = FacetClaim {
            origin_document: DocumentId::new(b"origin"),
            facet_key: facet_key("org.example.note/main"),
            chosen_uuid: Uuid::from_u128(4),
            operation: ClaimOperation::Set,
            value_cid: Some(value_cid(&value).unwrap()),
            predecessors: Vec::new(),
        };
        let message = claim.signing_bytes().unwrap();
        let signature = key.sign(&message).to_bytes().to_vec();
        let signed = SignedFacetClaim::from_signature(
            claim.clone(),
            key.verifying_key().to_bytes().to_vec(),
            signature.clone(),
        )
        .unwrap();
        let entry = FacetSignature {
            actor_public_key: signed.actor_public_key.clone(),
            signature: signed.signature.clone(),
            origin_doc: Some(DocumentId::new(b"origin")),
            inherited_message: Some(claim.signing_bytes().unwrap()),
            inherited_value: Some(value.clone()),
        };
        assert_eq!(
            entry.verify_inherited_for(&claim, None),
            VerificationState::Accepted
        );
        assert_eq!(
            entry.verify_inherited_for(&claim, Some(&json!({"answer": 43}))),
            VerificationState::Accepted
        );
        let mut forged = entry.clone();
        forged.inherited_value = Some(json!({"answer": 43}));
        assert_eq!(
            forged.verify_inherited_for(&claim, None),
            VerificationState::ProvenForgery
        );
        let remove_claim = FacetClaim {
            origin_document: claim.origin_document.clone(),
            facet_key: claim.facet_key.clone(),
            chosen_uuid: claim.chosen_uuid,
            operation: ClaimOperation::Remove,
            value_cid: None,
            predecessors: Vec::new(),
        };
        let remove_message = remove_claim.signing_bytes().unwrap();
        let remove_witness = FacetSignature {
            actor_public_key: key.verifying_key().to_bytes().to_vec(),
            signature: key.sign(&remove_message).to_bytes().to_vec(),
            origin_doc: Some(remove_claim.origin_document.clone()),
            inherited_message: Some(remove_message),
            inherited_value: Some(value.clone()),
        };
        assert_eq!(
            remove_witness.verify_inherited_for(&remove_claim, None),
            VerificationState::ProvenForgery
        );
        let mut pending = entry;
        pending.inherited_message = None;
        assert_eq!(
            pending.verify_inherited_for(&claim, None),
            VerificationState::Pending
        );
    }

    #[test]
    fn missing_evidence_is_not_forgery_and_closure_is_conservative() {
        assert_eq!(
            verification_state(Evidence::Missing),
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
    fn independent_openssl_ed25519_claim_fixture_is_accepted() {
        let claim = FacetClaim {
            origin_document: DocumentId::new(b"doc"),
            facet_key: facet_key("org.example.note/main"),
            chosen_uuid: Uuid::from_u128(1),
            operation: ClaimOperation::Set,
            value_cid: Some(value_cid(&json!({"b": 2, "a": 1})).unwrap()),
            predecessors: vec![],
        };
        let actor_public_key = vec![
            0xea, 0x4a, 0x6c, 0x63, 0xe2, 0x9c, 0x52, 0x0a, 0xbe, 0xf5, 0x50, 0x7b, 0x13, 0x2e,
            0xc5, 0xf9, 0x95, 0x47, 0x76, 0xae, 0xbe, 0xbe, 0x7b, 0x92, 0x42, 0x1e, 0xea, 0x69,
            0x14, 0x46, 0xd2, 0x2c,
        ];
        let signature = vec![
            0x48, 0xb7, 0xb4, 0xdd, 0x7b, 0x4a, 0xea, 0x80, 0xa0, 0x3f, 0x13, 0xeb, 0x7f, 0x5c,
            0xe8, 0x5b, 0x8f, 0x85, 0x74, 0x45, 0xfc, 0xa8, 0x70, 0x8a, 0xd0, 0x5a, 0x81, 0xd0,
            0x17, 0x7c, 0x18, 0x41, 0xb9, 0x58, 0x3d, 0x6d, 0xa5, 0x10, 0x78, 0x49, 0x9c, 0xad,
            0xc6, 0x30, 0xfc, 0xd8, 0x2c, 0x3c, 0x89, 0x86, 0x8f, 0x7b, 0x73, 0x8f, 0x94, 0xd4,
            0xd9, 0xb8, 0x1b, 0x4e, 0x5c, 0x5b, 0xfe, 0x0b,
        ];
        let signed = SignedFacetClaim::from_signature(claim, actor_public_key, signature).unwrap();
        signed.verify().unwrap();
        assert_eq!(
            signed.signature_id().unwrap(),
            vec![
                0x01, 0x55, 0x1e, 0x20, 0xec, 0x57, 0x2f, 0x95, 0x11, 0x9a, 0xa6, 0x1b, 0xbe, 0xca,
                0x0d, 0xcd, 0x46, 0xd9, 0x66, 0xe5, 0x46, 0x66, 0xf6, 0x84, 0xdd, 0xf4, 0xe1, 0x54,
                0x8a, 0xe6, 0xbc, 0x48, 0x4b, 0x4f, 0x33, 0x86,
            ]
        );
    }

    #[test]
    fn hostile_predecessor_count_is_refused_before_allocation() {
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
    fn strict_claim_decoder_rejects_malformed_noncanonical_and_trailing_bytes() {
        let claim = FacetClaim {
            origin_document: DocumentId::new(b"origin"),
            facet_key: facet_key("org.example.note/main"),
            chosen_uuid: Uuid::from_u128(7),
            operation: ClaimOperation::Set,
            value_cid: Some(value_cid(&json!({"score": 1.5})).unwrap()),
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
    fn inherited_claim_binds_source_context_not_destination_branch() {
        let key = SigningKey::from_bytes(&[11; 32]);
        let mut predecessor_a = CID_VERSION_RAW_BLAKE3.to_vec();
        predecessor_a.extend([1; 32]);
        let mut predecessor_b = CID_VERSION_RAW_BLAKE3.to_vec();
        predecessor_b.extend([2; 32]);
        let source_claim = FacetClaim {
            origin_document: DocumentId::new(b"origin-a"),
            facet_key: facet_key("org.example.note/main"),
            chosen_uuid: Uuid::from_u128(9),
            operation: ClaimOperation::Set,
            value_cid: Some(value_cid(&json!({"answer": 42})).unwrap()),
            predecessors: vec![
                predecessor_b.clone(),
                predecessor_a.clone(),
                predecessor_b.clone(),
            ],
        };
        let message = source_claim.signing_bytes().unwrap();
        let witness = FacetSignature {
            actor_public_key: key.verifying_key().to_bytes().to_vec(),
            signature: key.sign(&message).to_bytes().to_vec(),
            origin_doc: Some(source_claim.origin_document.clone()),
            inherited_message: Some(message),
            inherited_value: Some(json!({"answer": 42})),
        };
        assert_eq!(
            witness.verify_inherited_for(&source_claim, None),
            VerificationState::Accepted
        );
        let mut normalized_expected = source_claim.clone();
        normalized_expected.predecessors = vec![predecessor_a.clone(), predecessor_b.clone()];
        assert_eq!(
            witness.verify_inherited_for(&normalized_expected, None),
            VerificationState::Accepted
        );
        let mut wrong_frontier = normalized_expected.clone();
        wrong_frontier.predecessors = vec![predecessor_a.clone()];
        assert_eq!(
            witness.verify_inherited_for(&wrong_frontier, None),
            VerificationState::ProvenForgery
        );
        let mut wrong_facet = source_claim.clone();
        wrong_facet.facet_key = facet_key("org.example.note/wrong");
        assert_eq!(
            witness.verify_inherited_for(&wrong_facet, None),
            VerificationState::ProvenForgery
        );
        let mut wrong_uuid = source_claim.clone();
        wrong_uuid.chosen_uuid = Uuid::from_u128(10);
        assert_eq!(
            witness.verify_inherited_for(&wrong_uuid, None),
            VerificationState::ProvenForgery
        );
        let mut wrong_origin = source_claim;
        wrong_origin.origin_document = DocumentId::new(b"branch-b");
        assert_eq!(
            witness.verify_inherited_for(&wrong_origin, None),
            VerificationState::ProvenForgery
        );
    }

    #[test]
    fn facet_key_bytes_slashes_and_uri_transport_round_trip_without_second_identity() {
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
    fn cbor_predecessors_are_sorted_and_deduplicated() {
        let mut first = CID_VERSION_RAW_BLAKE3.to_vec();
        first.extend([1; 32]);
        let mut second = CID_VERSION_RAW_BLAKE3.to_vec();
        second.extend([2; 32]);
        let claim = FacetClaim {
            origin_document: DocumentId::new(b"d"),
            facet_key: facet_key("org.example.test/main"),
            chosen_uuid: Uuid::nil(),
            operation: ClaimOperation::Remove,
            value_cid: None,
            predecessors: vec![second.clone(), first.clone(), second.clone()],
        };
        let bytes = claim.cbor_fields().unwrap();
        assert!(
            bytes
                .windows(CID_LENGTH)
                .any(|window| window == first.as_slice())
        );
        assert!(
            bytes
                .windows(CID_LENGTH)
                .any(|window| window == second.as_slice())
        );
    }
}
