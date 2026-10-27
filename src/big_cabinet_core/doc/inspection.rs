use crate::FacetKey;
use crate::authority::CapId;
use crate::dmeta::Bytes;
use crate::dmeta::DocId;
use crate::dmeta::FacetClaim;
use crate::dmeta::FacetSignature;
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
    pub signature: FacetSignature,
    pub actor_public_key: MultibaseKey,
    pub claim: FacetClaim,
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
pub struct FacetClaimEvidence {
    pub marker: String,
    pub result: SignatureEvidence,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FacetClaimsInspection {
    pub doc_id: DocId,
    pub facet_key: FacetKey,
    pub heads: Vec<ChangeHash>,
    pub scope: EvidenceScope,
    pub evidence: Vec<FacetClaimEvidence>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnsafeFacetInspection {
    pub native_value: Option<Bytes>,
    pub schema_known: bool,
    pub claims: FacetClaimsInspection,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnsafeInspectFacet {
    pub capability: CapId,
    pub doc_id: DocId,
    pub facet_key: FacetKey,
    pub heads: Vec<ChangeHash>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InspectFacetClaims {
    pub capability: CapId,
    pub doc_id: DocId,
    pub facet_key: FacetKey,
    pub heads: Vec<ChangeHash>,
}
