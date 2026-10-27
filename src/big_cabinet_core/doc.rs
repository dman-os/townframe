use crate::DatumKey;
use crate::DatumTag;
use crate::authority::AuthorityServiceError;
use crate::authority::Authorization;
use crate::authority::CapId;
use crate::authority::Capability;
use crate::authority::Rights;
use crate::dmeta::DocId;
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

mod availability;
mod inspection;

pub use availability::DocAvailability;
pub use availability::GetDocAvailability;
pub use availability::LocalDocUsability;
pub use availability::ObserveDocAvailability;
pub use inspection::DatumClaimEvidence;
pub use inspection::DatumClaimsInspection;
pub use inspection::EvidenceScope;
pub use inspection::InspectDatumClaims;
pub use inspection::SignatureEvidence;
pub use inspection::UnsafeDatumInspection;
pub use inspection::UnsafeInspectDatum;
pub use inspection::VerifiedSignatureEvidence;

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
    IfDatumsUnchanged {
        datums: BTreeMap<DatumKey, DatumVersion>,
    },
    AtHeads {
        heads: Vec<ChangeHash>,
    },
    AtCurrent,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatumVersion {
    pub heads: Vec<ChangeHash>,
    pub clock: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum DatumRead {
    Accepted {
        value: Json,
        version: DatumVersion,
    },
    Absent {
        version: DatumVersion,
    },
    Pending {
        reason: String,
        version: DatumVersion,
        value: Option<Json>,
    },
    Rejected {
        reason: String,
    },
    Undecodable {
        reason: String,
        version: DatumVersion,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatumsRead {
    pub heads: Vec<ChangeHash>,
    pub datums: BTreeMap<DatumKey, DatumRead>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatumListing {
    pub heads: Vec<ChangeHash>,
    pub datums: BTreeMap<DatumKey, DatumVersion>,
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
    #[error("no schema is registered for datum tag {datum_tag}")]
    UnknownSchema { datum_tag: String },
    #[error("datum {datum_key} is managed by the repository and cannot be written directly")]
    SystemDatum { datum_key: DatumKey },
    #[error("datum {datum_key} is both set and removed")]
    SetAndRemoved { datum_key: DatumKey },
    #[error("datum {datum_key} value is invalid: {reason}")]
    InvalidValue { datum_key: DatumKey, reason: String },
    #[error("the write condition no longer holds at {heads:?}")]
    Conflict {
        heads: Vec<ChangeHash>,
        datums: BTreeMap<DatumKey, DatumVersion>,
    },
    #[error("datum {datum_key} at the write's base is not accepted: {verdict:?}")]
    BaseNotAccepted {
        datum_key: DatumKey,
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
    pub datums: BTreeMap<DatumKey, Json>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WriteDatums {
    pub capability: CapId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<CommandId>,
    pub doc_id: DocId,
    pub mode: WriteMode,
    #[serde(default)]
    pub set: BTreeMap<DatumKey, Json>,
    #[serde(default)]
    pub remove: BTreeSet<DatumKey>,
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
pub struct ReadDatums {
    pub capability: CapId,
    pub doc_id: DocId,
    pub at: At,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub datums: Option<BTreeSet<DatumKey>>,
    #[serde(default)]
    pub optimistic: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListDatums {
    pub capability: CapId,
    pub doc_id: DocId,
    pub at: At,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "by", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum DatumSelector {
    All,
    Doc { doc_id: DocId },
    Tag { tag: DatumTag },
    Route { doc_id: DocId, datum_key: DatumKey },
    Key { datum_key: DatumKey },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryDatums {
    pub capability: CapId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drawer_id: Option<DocId>,
    pub selector: DatumSelector,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueriedDatum {
    pub doc_id: DocId,
    pub datum_key: DatumKey,
    pub doc_heads: Vec<ChangeHash>,
    pub version: DatumVersion,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "by", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum DatumRefSelector {
    Origin {
        doc_id: DocId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        datum_key: Option<DatumKey>,
    },
    Target {
        doc_id: DocId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        datum_key: Option<DatumKey>,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryDatumRefs {
    pub capability: CapId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drawer_id: Option<DocId>,
    pub selector: DatumRefSelector,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueriedDatumRef {
    pub origin_doc_id: DocId,
    pub origin_datum_key: DatumKey,
    pub path: String,
    pub target_doc_id: DocId,
    pub target_datum_key: DatumKey,
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
        tag: DatumTag,
    },
    Drawer {
        drawer_id: DocId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tag: Option<DatumTag>,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "state",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ObservedDatum {
    Present {
        doc_heads: Vec<ChangeHash>,
        version: DatumVersion,
    },
    Removed {
        doc_heads: Option<Vec<ChangeHash>>,
        version: DatumVersion,
    },
    AccessLost,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum DatumObserved {
    Entry {
        revision: u64,
        doc_id: DocId,
        datum_key: DatumKey,
        datum: ObservedDatum,
    },
    ReplayComplete {
        through: u64,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObserveDatums {
    pub capability: CapId,
    pub scope: ObserveScope,
    #[serde(default)]
    pub after: u64,
}
service_core_rs::service_operation!(ObserveDatums => stream DatumObserved, DocsError, "org.example.daybook.docs.observeDatums");

macro_rules! operations {
    ($($request:ident => $output:ty, $kind:ident, $nsid:literal;)*) => {$(
        impl Operation for $request {
            type Output = $output;
            type Error = DocsError;
            const XRPC: XrpcOperation =
                XrpcOperation::$kind(concat!("org.example.daybook.docs.", $nsid))
                    .with_schemas(stringify!($output), stringify!(DocsError));
        }
    )*};
}
operations! {
    MintDocCapability => Capability, procedure, "mintDocCapability";
    CreateDoc => WriteReceipt, procedure, "createDoc";
    WriteDatums => WriteReceipt, procedure, "writeDatums";
    BranchDoc => WriteReceipt, procedure, "branchDoc";
    MergeDoc => WriteReceipt, procedure, "mergeDoc";
    ReadDatums => DatumsRead, procedure, "readDatums";
    ListDatums => DatumListing, procedure, "listDatums";
    QueryDatums => Vec<QueriedDatum>, procedure, "queryDatums";
    QueryDatumRefs => Vec<QueriedDatumRef>, procedure, "queryDatumRefs";
    UnsafeInspectDatum => UnsafeDatumInspection, procedure, "unsafeInspectDatum";
    InspectDatumClaims => DatumClaimsInspection, procedure, "inspectDatumClaims";
    GetDocAvailability => DocAvailability, query, "getDocAvailability";
}

declare_service_protocol! {
    pub enum DocsProtocol => DocsMessage {
        unary {
            MintDocCapability(MintDocCapability),
            CreateDoc(CreateDoc),
            WriteDatums(WriteDatums),
            BranchDoc(BranchDoc),
            MergeDoc(MergeDoc),
            ReadDatums(ReadDatums),
            ListDatums(ListDatums),
            QueryDatums(QueryDatums),
            QueryDatumRefs(QueryDatumRefs),
            UnsafeInspectDatum(UnsafeInspectDatum),
            InspectDatumClaims(InspectDatumClaims),
            GetDocAvailability(GetDocAvailability)
        }
        stream {
            ObserveDatums(ObserveDatums),
            ObserveDocAvailability(ObserveDocAvailability)
        }
    }
}
