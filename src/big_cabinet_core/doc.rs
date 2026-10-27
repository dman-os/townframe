use crate::FacetKey;
use crate::FacetTag;
use crate::authority::AuthorityServiceError;
use crate::authority::Authorization;
use crate::authority::CapId;
use crate::authority::Capability;
use crate::authority::Rights;
use crate::dmeta::Bytes;
use crate::dmeta::DocId;
use crate::dmeta::FacetClaim;
use crate::dmeta::FacetSignature;
use crate::dmeta::MultibaseKey;
use crate::drawer::DrawerAccess;
use automerge::ChangeHash;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value as Json;
use service_core_rs::ContextError;
use service_core_rs::Operation;
use service_core_rs::XrpcOperation;
use service_core_rs::declare_service_protocol;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct CommandId(String);

impl CommandId {
    pub const MAX_BYTES: usize = 256;
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for CommandId {
    type Error = DocsError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.is_empty() || value.len() > Self::MAX_BYTES {
            return Err(DocsError::InvalidCommand);
        }
        Ok(Self(value))
    }
}
impl<'de> Deserialize<'de> for CommandId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .try_into()
            .map_err(serde::de::Error::custom)
    }
}
impl std::fmt::Display for CommandId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "at", content = "heads", rename_all = "camelCase")]
pub enum At {
    Current,
    Heads(Vec<ChangeHash>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "camelCase")]
pub enum WriteMode {
    IfDocUnchanged {
        heads: Vec<ChangeHash>,
    },
    IfFacetsUnchanged {
        facets: BTreeMap<FacetKey, FacetVersion>,
    },
    AtHeads {
        heads: Vec<ChangeHash>,
    },
    AtCurrent,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FacetVersion {
    pub heads: Vec<ChangeHash>,
    pub clock: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum FacetRead {
    Accepted {
        value: Json,
        version: FacetVersion,
    },
    Absent {
        version: FacetVersion,
    },
    Pending {
        reason: String,
        version: FacetVersion,
        value: Option<Json>,
    },
    Rejected {
        reason: String,
    },
    Undecodable {
        reason: String,
        version: FacetVersion,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FacetsRead {
    pub heads: Vec<ChangeHash>,
    pub facets: BTreeMap<FacetKey, FacetRead>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FacetListing {
    pub heads: Vec<ChangeHash>,
    pub facets: BTreeMap<FacetKey, FacetVersion>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WriteReceipt {
    pub doc_id: DocId,
    pub heads: Vec<ChangeHash>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum Unaccepted {
    Pending { reason: String },
    Rejected { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum DocsError {
    #[error("request context failed: {0}")]
    Context(#[from] ContextError),
    #[error("the presented capability does not permit this operation")]
    Denied,
    #[error("authority failed: {0}")]
    Authority(AuthorityServiceError),
    #[error("authority transport failed: {0}")]
    AuthorityUnavailable(String),
    #[error("document access policy failed: {0}")]
    Access(String),
    #[error("document repository failed: {0}")]
    Repository(String),
    #[error("document is not locally available")]
    DocNotFound,
    #[error("requested heads are unknown: {heads:?}")]
    UnknownHeads { heads: Vec<ChangeHash> },
    #[error("a command id is 1..=256 bytes")]
    InvalidCommand,
    #[error("command {command} is still in flight; retry once it resolves")]
    CommandInFlight { command: CommandId },
    #[error("command {command} was already used for a different request")]
    CommandReused { command: CommandId },
    #[error("no schema is registered for facet tag {facet_tag}")]
    UnknownSchema { facet_tag: String },
    #[error("facet {facet_key} is managed by the repository and cannot be written directly")]
    SystemFacet { facet_key: FacetKey },
    #[error("facet {facet_key} is both set and removed")]
    SetAndRemoved { facet_key: FacetKey },
    #[error("facet {facet_key} value is invalid: {reason}")]
    InvalidValue { facet_key: FacetKey, reason: String },
    #[error("the write condition no longer holds at {heads:?}")]
    Conflict {
        heads: Vec<ChangeHash>,
        facets: BTreeMap<FacetKey, FacetVersion>,
    },
    #[error("facet {facet_key} at the write's base is not accepted: {verdict:?}")]
    BaseNotAccepted {
        facet_key: FacetKey,
        verdict: Unaccepted,
    },
    #[error("inspection evidence is pending: {reason}")]
    Pending { reason: String },
    #[error("inspection resource limit: {reason}")]
    ResourceRefused { reason: String },
    #[error("unsupported inspection evidence: {reason}")]
    Unsupported { reason: String },
    #[error("malformed inspection evidence: {reason}")]
    MalformedEvidence { reason: String },
    #[error("the documents share no history, so one cannot be merged into the other")]
    UnrelatedHistory,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DocScope {
    pub read: BTreeSet<DocId>,
    pub write: BTreeSet<DocId>,
    pub create: BTreeSet<String>,
    pub drawers: BTreeMap<DocId, DrawerAccess>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MintDocCapability {
    pub authorization: Authorization,
    pub scope: DocScope,
    pub rights: Rights,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateDoc {
    pub capability: CapId,
    pub command: CommandId,
    pub placement: String,
    pub facets: BTreeMap<FacetKey, Json>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WriteFacets {
    pub capability: CapId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<CommandId>,
    pub doc_id: DocId,
    pub mode: WriteMode,
    #[serde(default)]
    pub set: BTreeMap<FacetKey, Json>,
    #[serde(default)]
    pub remove: BTreeSet<FacetKey>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BranchDoc {
    pub capability: CapId,
    pub command: CommandId,
    pub doc_id: DocId,
    pub at: At,
    pub path: String,
    #[serde(default)]
    pub stealth: bool,
    pub placement: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MergeDoc {
    pub capability: CapId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<CommandId>,
    pub source_id: DocId,
    pub at: At,
    pub target_id: DocId,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadFacets {
    pub capability: CapId,
    pub doc_id: DocId,
    pub at: At,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub facets: Option<BTreeSet<FacetKey>>,
    #[serde(default)]
    pub optimistic: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListFacets {
    pub capability: CapId,
    pub doc_id: DocId,
    pub at: At,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LocalDocUsability {
    NotObserved,
    NoLocalContent,
    PendingMaterialization,
    Usable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DocAvailability {
    pub doc_id: DocId,
    pub local_usability: LocalDocUsability,
    pub sync_demand: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetDocAvailability {
    pub capability: CapId,
    pub doc_id: DocId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObserveDocAvailability {
    pub capability: CapId,
    pub doc_id: DocId,
}
service_core_rs::service_operation!(ObserveDocAvailability => stream DocAvailability, DocsError, "org.example.daybook.docs.observeDocAvailability");

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "by", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum FacetSelector {
    All,
    Doc { doc_id: DocId },
    Tag { tag: FacetTag },
    Route { doc_id: DocId, facet_key: FacetKey },
    Key { facet_key: FacetKey },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryFacets {
    pub capability: CapId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drawer_id: Option<DocId>,
    pub selector: FacetSelector,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueriedFacet {
    pub doc_id: DocId,
    pub facet_key: FacetKey,
    pub doc_heads: Vec<ChangeHash>,
    pub version: FacetVersion,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "by", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum FacetRefSelector {
    Origin {
        doc_id: DocId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        facet_key: Option<FacetKey>,
    },
    Target {
        doc_id: DocId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        facet_key: Option<FacetKey>,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryFacetRefs {
    pub capability: CapId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drawer_id: Option<DocId>,
    pub selector: FacetRefSelector,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueriedFacetRef {
    pub origin_doc_id: DocId,
    pub origin_facet_key: FacetKey,
    pub path: String,
    pub target_doc_id: DocId,
    pub target_facet_key: FacetKey,
    pub ref_heads: Vec<ChangeHash>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "scope",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ObserveScope {
    Doc {
        doc_id: DocId,
    },
    Tag {
        tag: FacetTag,
    },
    Drawer {
        drawer_id: DocId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tag: Option<FacetTag>,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "state",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ObservedFacet {
    Present {
        doc_heads: Vec<ChangeHash>,
        version: FacetVersion,
    },
    Removed {
        doc_heads: Option<Vec<ChangeHash>>,
        version: FacetVersion,
    },
    AccessLost,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum FacetObserved {
    Entry {
        revision: u64,
        doc_id: DocId,
        facet_key: FacetKey,
        facet: ObservedFacet,
    },
    ReplayComplete {
        through: u64,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObserveFacets {
    pub capability: CapId,
    pub scope: ObserveScope,
    #[serde(default)]
    pub after: u64,
}
service_core_rs::service_operation!(ObserveFacets => stream FacetObserved, DocsError, "org.example.daybook.docs.observeFacets");

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

macro_rules! operation {
    ($request:ident => $output:ty, $nsid:literal) => {
        impl Operation for $request {
            type Output = $output;
            type Error = DocsError;
            const XRPC: XrpcOperation =
                XrpcOperation::procedure(concat!("org.example.daybook.docs.", $nsid))
                    .with_schemas(stringify!($output), stringify!(DocsError));
        }
    };
}
operation!(MintDocCapability => Capability, "mintDocCapability");
operation!(CreateDoc => WriteReceipt, "createDoc");
operation!(WriteFacets => WriteReceipt, "writeFacets");
operation!(BranchDoc => WriteReceipt, "branchDoc");
operation!(MergeDoc => WriteReceipt, "mergeDoc");
operation!(ReadFacets => FacetsRead, "readFacets");
operation!(ListFacets => FacetListing, "listFacets");
operation!(QueryFacets => Vec<QueriedFacet>, "queryFacets");
operation!(QueryFacetRefs => Vec<QueriedFacetRef>, "queryFacetRefs");
operation!(UnsafeInspectFacet => UnsafeFacetInspection, "unsafeInspectFacet");
operation!(InspectFacetClaims => FacetClaimsInspection, "inspectFacetClaims");

impl Operation for GetDocAvailability {
    type Output = DocAvailability;
    type Error = DocsError;
    const XRPC: XrpcOperation = XrpcOperation::query("org.example.daybook.docs.getDocAvailability")
        .with_schemas("DocAvailability", "DocsError");
}

declare_service_protocol! {
    pub enum DocsProtocol => DocsMessage {
        unary {
            MintDocCapability(MintDocCapability),
            CreateDoc(CreateDoc),
            WriteFacets(WriteFacets),
            BranchDoc(BranchDoc),
            MergeDoc(MergeDoc),
            ReadFacets(ReadFacets),
            ListFacets(ListFacets),
            QueryFacets(QueryFacets),
            QueryFacetRefs(QueryFacetRefs),
            UnsafeInspectFacet(UnsafeInspectFacet),
            InspectFacetClaims(InspectFacetClaims),
            GetDocAvailability(GetDocAvailability)
        }
        stream {
            ObserveFacets(ObserveFacets),
            ObserveDocAvailability(ObserveDocAvailability)
        }
    }
}
