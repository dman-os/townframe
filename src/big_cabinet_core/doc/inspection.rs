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

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum EvidenceScope {
    UnsafeUnattributedSignatureEvidence,
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VerifiedSignatureEvidence {
    pub signature: DatumSignature,
    pub actor_public_key: MultibaseKey,
    pub claim: DatumClaim,
    pub introduction_marker: String,
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub introduction_hash: ChangeHash,
    #[cfg_attr(feature = "schemars", schemars(with = "Vec<String>"))]
    pub parents: Vec<ChangeHash>,
    #[cfg_attr(feature = "schemars", schemars(with = "Vec<String>"))]
    pub resulting_heads: Vec<ChangeHash>,
    pub automerge_actor: Bytes,
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatumClaimEvidence {
    pub marker: String,
    pub result: SignatureEvidence,
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatumClaimsInspection {
    pub doc_id: DocId,
    pub datum_key: DatumKey,
    #[cfg_attr(feature = "schemars", schemars(with = "Vec<String>"))]
    pub heads: Vec<ChangeHash>,
    pub scope: EvidenceScope,
    pub evidence: Vec<DatumClaimEvidence>,
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnsafeDatumInspection {
    pub native_value: Option<Bytes>,
    pub schema_known: bool,
    pub claims: DatumClaimsInspection,
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnsafeInspectDatum {
    pub capability: CapId,
    pub doc_id: DocId,
    pub datum_key: DatumKey,
    #[cfg_attr(feature = "schemars", schemars(with = "Vec<String>"))]
    pub heads: Vec<ChangeHash>,
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InspectDatumClaims {
    pub capability: CapId,
    pub doc_id: DocId,
    pub datum_key: DatumKey,
    #[cfg_attr(feature = "schemars", schemars(with = "Vec<String>"))]
    pub heads: Vec<ChangeHash>,
}
