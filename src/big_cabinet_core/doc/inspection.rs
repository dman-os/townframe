use crate::DatumKey;
use crate::authority::CapId;
use crate::dmeta::Bytes;
use crate::dmeta::DatumClaim;
use crate::dmeta::DatumSignature;
use crate::dmeta::DocId;
use crate::dmeta::MultibaseKey;
use automerge::ChangeHash;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum EvidenceScope {
    UnsafeUnattributedSignatureEvidence,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VerifiedSignatureEvidence {
    pub signature: DatumSignature,
    pub actor_public_key: MultibaseKey,
    pub claim: DatumClaim,
    pub introduction_marker: String,
    pub introduction_hash: ChangeHash,
    pub parents: Vec<ChangeHash>,
    pub resulting_heads: Vec<ChangeHash>,
    pub automerge_actor: Bytes,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum SignatureEvidence {
    VerifiedSignature {
        evidence: Box<VerifiedSignatureEvidence>,
    },
    Pending {
        reason: String,
    },
    ResourceRefused {
        reason: String,
    },
    Unsupported {
        reason: String,
    },
    ProvenForgery {
        reason: String,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatumClaimEvidence {
    pub marker: String,
    pub result: SignatureEvidence,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatumClaimsInspection {
    pub doc_id: DocId,
    pub datum_key: DatumKey,
    pub heads: Vec<ChangeHash>,
    pub scope: EvidenceScope,
    pub evidence: Vec<DatumClaimEvidence>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnsafeDatumInspection {
    pub native_value: Option<Bytes>,
    pub schema_known: bool,
    pub claims: DatumClaimsInspection,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnsafeInspectDatum {
    pub capability: CapId,
    pub doc_id: DocId,
    pub datum_key: DatumKey,
    pub heads: Vec<ChangeHash>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InspectDatumClaims {
    pub capability: CapId,
    pub doc_id: DocId,
    pub datum_key: DatumKey,
    pub heads: Vec<ChangeHash>,
}
