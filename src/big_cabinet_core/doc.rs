use crate::DatumKey;
use crate::DatumTag;
use crate::authority::AuthorityServiceError;
use crate::authority::CapId;
use crate::authority::Capability;
use crate::authority::Rights;
use crate::dmeta::DocId;
use crate::dmeta::ResourceLimit;
use crate::dmeta::Uuid7;
use crate::drawer::DrawerAccess;
use crate::query::DatumSelection;
use crate::query::Predicate;
pub use automerge::ChangeHash;
use method_core::ContextError;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value as Json;
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

/// A caller-chosen retry key. It deduplicates retries of one request **on the node that received
/// it, and only there**: the command ledger is per node, never replicated. A command id is not a
/// global name. Two nodes that run the same id each execute the command, so a create under a
/// fixed id on two nodes makes two documents; to converge on one document across nodes, use a
/// name that is resolved in replicated state (for example a metadata document's `kind`/`name`
/// slot), not a command id.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "at", content = "heads", rename_all = "camelCase")]
pub enum At {
    Current,
    Heads(#[cfg_attr(feature = "schemars", schemars(with = "Vec<String>"))] Vec<ChangeHash>),
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "camelCase")]
pub enum WriteMode {
    IfDocUnchanged {
        #[cfg_attr(feature = "schemars", schemars(with = "Vec<String>"))]
        heads: Vec<ChangeHash>,
    },
    IfDatumsUnchanged {
        datums: BTreeMap<DatumKey, DatumVersion>,
    },
    AtHeads {
        #[cfg_attr(feature = "schemars", schemars(with = "Vec<String>"))]
        heads: Vec<ChangeHash>,
    },
    AtCurrent,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatumVersion {
    #[cfg_attr(feature = "schemars", schemars(with = "Vec<String>"))]
    pub heads: Vec<ChangeHash>,
    pub clock: u64,
}

/// An actor that wrote the version of a datum a read or listing reports (attribution).
///
/// These are the actors of the evidence the gate verified as currently covering the version, one
/// per distinct actor and sorted by actor id; a slot with concurrent current writes lists every
/// author. `url` is the actor URL (`dtxt:actor:<node key>[/<path>]`) taken from that verified
/// evidence rather than from the local actor registry, so authors on other nodes resolve too.
/// What the evidence is (today a per-datum signature) may change; the meaning of the field does not.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatumAuthor {
    pub actor: Uuid7,
    pub url: String,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum DatumRead {
    Accepted {
        value: Json,
        version: DatumVersion,
        /// Who wrote the accepted version, as the gate verified it for this read.
        authors: Vec<DatumAuthor>,
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

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatumsRead {
    #[cfg_attr(feature = "schemars", schemars(with = "Vec<String>"))]
    pub heads: Vec<ChangeHash>,
    pub datums: BTreeMap<DatumKey, DatumRead>,
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatumListing {
    #[cfg_attr(feature = "schemars", schemars(with = "Vec<String>"))]
    pub heads: Vec<ChangeHash>,
    pub datums: BTreeMap<DatumKey, DatumVersion>,
    /// The authors of each row's version, present only for rows of a listing that asked
    /// (`ListDatums::with_authors`) and whose version the gate accepted.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub authors: BTreeMap<DatumKey, Vec<DatumAuthor>>,
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WriteReceipt {
    pub doc_id: DocId,
    #[cfg_attr(feature = "schemars", schemars(with = "Vec<String>"))]
    pub heads: Vec<ChangeHash>,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatumPatch {
    #[serde(default)]
    pub set: BTreeMap<DatumKey, Json>,
    #[serde(default)]
    pub remove: BTreeSet<DatumKey>,
}

impl DatumPatch {
    pub fn is_empty(&self) -> bool {
        self.set.is_empty() && self.remove.is_empty()
    }
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StoreSnapshot<S> {
    #[cfg_attr(feature = "schemars", schemars(with = "Vec<String>"))]
    pub heads: Vec<ChangeHash>,
    pub value: S,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum StoreError {
    #[error("document operation failed: {0}")]
    Docs(#[from] DocsError),
    #[error("document service transport failed: {0}")]
    DocsUnavailable(String),
    #[error("access to the store document was lost")]
    AccessLost,
    #[error("invalid store edit: {0}")]
    InvalidEdit(String),
}

impl From<ContextError> for StoreError {
    fn from(error: ContextError) -> Self {
        Self::Docs(DocsError::Context(error))
    }
}

pub trait DatumStore: Clone + Send + 'static {
    fn owns(key: &DatumKey) -> bool;
    fn hydrate(read: &DatumsRead) -> Self;
    fn diff(before: &Self, after: &Self) -> Result<DatumPatch, StoreError>;
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum Unaccepted {
    Pending { reason: String },
    Rejected { reason: String },
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
    #[error("document {doc_id} is not locally available")]
    DocNotFound { doc_id: DocId },
    #[error("requested heads are unknown in document {doc_id}: {heads:?}")]
    UnknownHeads {
        doc_id: DocId,
        #[cfg_attr(feature = "schemars", schemars(with = "Vec<String>"))]
        heads: Vec<ChangeHash>,
    },
    #[error("a command id is 1..=256 bytes")]
    InvalidCommand,
    /// A document id arrived in a request that is not the 32 bytes every document id is. Ids are
    /// variable-width keys on the wire (ADR 012), but a document's id is its ed25519 key, so
    /// only a 32-byte one can name a document; refusing here keeps the width from ever reaching
    /// the repository, where it is an invariant.
    #[error("{doc_id} is not a document id: a document id is 32 bytes wide")]
    InvalidDocId { doc_id: DocId },
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
    /// A datum whose slot carries meaning (a dpath claim's slot is its dpath) was written under a
    /// slot that does not spell one.
    #[error("datum {datum_key} slot is invalid: {reason}")]
    InvalidSlot { datum_key: DatumKey, reason: String },
    #[error(
        "datum {datum_key} value is too large: it needs {observed} {limit} and the limit is {maximum}"
    )]
    ValueTooLarge {
        datum_key: DatumKey,
        limit: ResourceLimit,
        /// What the read of the value had reached when it was refused. The read stops at the
        /// limit, so this is the amount the value needed up to that point, not necessarily its
        /// whole size.
        observed: u64,
        maximum: u64,
    },
    #[error("the write condition no longer holds at {heads:?}")]
    Conflict {
        #[cfg_attr(feature = "schemars", schemars(with = "Vec<String>"))]
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

impl DocsError {
    /// Refuse `doc_id` unless it is the 32 bytes a document id is. Call this where an id enters
    /// from a request, before it reaches anything that derives the document's key from it.
    pub fn ensure_doc_id(doc_id: &DocId) -> Result<(), Self> {
        doc_id
            .to_bytes32()
            .map(drop)
            .map_err(|_| Self::InvalidDocId {
                doc_id: doc_id.clone(),
            })
    }
}

/// The datum kinds writable through a document capability.
/// `All` covers every datum; `Only` covers datum keys whose tags are in its set.
/// A requested `Only` grant is covered only when its tags are a subset of the held grant's tags.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DatumKinds {
    All,
    Only(BTreeSet<DatumTag>),
}

impl DatumKinds {
    /// Returns whether this grant permits writing the datum key.
    pub fn allows(&self, key: &DatumKey) -> bool {
        match self {
            Self::All => true,
            Self::Only(tags) => tags.contains(key.tag()),
        }
    }

    /// Returns whether the requested grant is covered by this grant.
    pub fn covers(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::All, _) => true,
            (Self::Only(_), Self::All) => false,
            (Self::Only(held), Self::Only(requested)) => requested.is_subset(held),
        }
    }

    /// Returns whether no datum kind is permitted.
    pub fn admits_nothing(&self) -> bool {
        matches!(self, Self::Only(tags) if tags.is_empty())
    }

    /// The kinds permitted by either grant. A document reached through two grants (two drawers
    /// that both hold it) is held at the union of what each lets through.
    pub fn union(self, other: Self) -> Self {
        match (self, other) {
            (Self::Only(mut held), Self::Only(more)) => {
                held.extend(more);
                Self::Only(held)
            }
            _ => Self::All,
        }
    }
}

/// What a document scope reaches through one granted drawer: the access it holds on the drawer
/// and, mirroring `DocScope.read`/`write`, the datum kinds it may read and the kinds it may write
/// in every document the drawer contains. `access` is kept beside the kinds because it carries
/// more than they do: it is the ceiling the drawer policy proves and queries documents at
/// (`Relay`, `Admin`), and creation in the drawer needs `Write` on it. A reach whose `write` kinds
/// are empty never writes, whatever its `access`.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DrawerReach {
    pub access: DrawerAccess,
    pub read: DatumKinds,
    pub write: DatumKinds,
}

impl DrawerReach {
    /// Builds a reach that admits every datum kind for reading and writing: broad access, an
    /// explicit choice.
    pub fn all_kinds(access: DrawerAccess) -> Self {
        Self {
            access,
            read: DatumKinds::All,
            write: DatumKinds::All,
        }
    }

    /// The kinds this reach reads. As for a document named in `DocScope.read`/`write`, an
    /// entry with `read` kinds reads those, and one with only `write` kinds reads its write
    /// kinds.
    pub fn readable(&self) -> &DatumKinds {
        if self.read.admits_nothing() {
            &self.write
        } else {
            &self.read
        }
    }

    /// Returns whether the requested reach is covered by this one: no stronger drawer access,
    /// and no read or write kind this reach does not hold.
    pub fn covers(&self, other: &Self) -> bool {
        self.access >= other.access
            && self.read.covers(&other.read)
            && self.write.covers(&other.write)
    }
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DocScope {
    pub read: BTreeMap<DocId, DatumKinds>,
    pub write: BTreeMap<DocId, DatumKinds>,
    pub create: BTreeSet<String>,
    pub drawers: BTreeMap<DocId, DrawerReach>,
}

impl DocScope {
    /// Builds a document-wide read grant, retaining the explicit datum-kind shape.
    pub fn read_all<I>(documents: I) -> BTreeMap<DocId, DatumKinds>
    where
        I: IntoIterator<Item = DocId>,
    {
        documents
            .into_iter()
            .map(|document| (document, DatumKinds::All))
            .collect()
    }

    /// Refuse a scope that names a document or drawer whose id is not a document id.
    pub fn ensure_doc_ids(&self) -> Result<(), DocsError> {
        self.read
            .keys()
            .chain(self.write.keys())
            .chain(self.drawers.keys())
            .try_for_each(DocsError::ensure_doc_id)
    }
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MintDocCapability {
    pub scope: DocScope,
    pub rights: Rights,
    /// The actor the new handle is bound to; `None` binds it to the presented handle's own
    /// subject. Naming another actor is an introduction: the presented handle needs
    /// `Rights::DELEGATE`, and the actor must be live and a strict descendant of the presented
    /// handle's subject in the actor tree (anyone else is `Escalation`).
    pub subject: Option<Uuid7>,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateDoc {
    pub capability: CapId,
    /// Deduplicates retries of this create on this node only. Another node given the same id
    /// creates its own, different document.
    pub command: CommandId,
    pub placement: String,
    pub datums: BTreeMap<DatumKey, Json>,
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListDatums {
    pub capability: CapId,
    pub doc_id: DocId,
    pub at: At,
    /// Also report each row's authors. Off by default: it runs the gate per row, which a plain
    /// listing never does.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub with_authors: bool,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryDatumRefs {
    pub capability: CapId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drawer_id: Option<DocId>,
    pub selector: DatumRefSelector,
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueriedDatumRef {
    pub origin_doc_id: DocId,
    pub origin_datum_key: DatumKey,
    pub path: String,
    pub target_doc_id: DocId,
    pub target_datum_key: DatumKey,
    #[serde(with = "crate::reference::head_list")]
    #[cfg_attr(feature = "schemars", schemars(with = "Vec<String>"))]
    pub ref_heads: Vec<ChangeHash>,
}

/// The result of [`QueryDatumRefs`].
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueriedDatumRefs {
    pub refs: Vec<QueriedDatumRef>,
}

/// One long poll over the datum-set frontier (ADR 023, ADR 025).
///
/// The predicate selects documents and the selection names which of their datums are delivered.
/// A current-state read is a poll from `after = 0` with `hold_ms = 0`, paged until `caught_up`.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PollDatums {
    pub capability: CapId,
    pub predicate: Predicate,
    pub datums: DatumSelection,
    /// The cursor the caller last consumed. `0` replays the frontier's current state.
    #[serde(default)]
    pub after: u64,
    /// The most entries one page carries. `0` asks for the default page size.
    #[serde(default)]
    pub limit: u32,
    /// How long the server may hold an otherwise empty page. `0` answers at once.
    #[serde(default)]
    pub hold_ms: u32,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatumPage {
    pub events: Vec<DatumEvent>,
    /// The cursor to send next. Every event here is at or below it.
    pub through: u64,
    /// The replay boundary was reached, so the events cover every fact at or below `through`.
    pub caught_up: bool,
}

/// One revision-stamped fact a poll delivers.
///
/// `datum` is one selected row of a document that matches the predicate: sent when the row
/// changed, and for every selected row of a document that moved into the result (a move-in,
/// ADR 025 §7). `leave` is a move-out and is idempotent for a consumer that never held the
/// document. The consumer tracks which documents it holds; the server keeps no state.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum DatumEvent {
    Datum {
        revision: u64,
        doc_id: DocId,
        datum_key: DatumKey,
        datum: ObservedDatum,
    },
    Leave {
        revision: u64,
        doc_id: DocId,
    },
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "state",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ObservedDatum {
    Present {
        #[cfg_attr(feature = "schemars", schemars(with = "Vec<String>"))]
        doc_heads: Vec<ChangeHash>,
        version: DatumVersion,
    },
    Removed {
        #[cfg_attr(feature = "schemars", schemars(with = "Option<Vec<String>>"))]
        doc_heads: Option<Vec<ChangeHash>>,
        version: DatumVersion,
    },
}
method_core::methods! {
    error = DocsError, nsid_prefix = "org.example.daybook.docs.";
    MintDocCapability => Capability, procedure, "mintDocCapability";
    CreateDoc => WriteReceipt, procedure, "createDoc";
    WriteDatums => WriteReceipt, procedure, "writeDatums";
    BranchDoc => WriteReceipt, procedure, "branchDoc";
    MergeDoc => WriteReceipt, procedure, "mergeDoc";
    ReadDatums => DatumsRead, procedure, "readDatums";
    ListDatums => DatumListing, procedure, "listDatums";
    PollDatums => DatumPage, procedure, "pollDatums";
    QueryDatumRefs => QueriedDatumRefs, procedure, "queryDatumRefs";
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
            PollDatums(PollDatums),
            QueryDatumRefs(QueryDatumRefs),
            UnsafeInspectDatum(UnsafeInspectDatum),
            InspectDatumClaims(InspectDatumClaims),
            GetDocAvailability(GetDocAvailability)
        }
        stream {
            ObserveDocAvailability(ObserveDocAvailability)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn document_errors_name_their_document() {
        let doc_id = DocId::new([7; 32]);
        for error in [
            DocsError::DocNotFound {
                doc_id: doc_id.clone(),
            },
            DocsError::UnknownHeads {
                doc_id: doc_id.clone(),
                heads: vec![],
            },
        ] {
            assert!(
                error.to_string().contains(&doc_id.to_string()),
                "{error} does not name {doc_id}"
            );
        }
    }

    fn only(tags: &[&str]) -> DatumKinds {
        DatumKinds::Only(
            tags.iter()
                .map(|tag| DatumTag::new(*tag).unwrap())
                .collect(),
        )
    }

    fn reach(access: DrawerAccess, read: &[&str], write: &[&str]) -> DrawerReach {
        DrawerReach {
            access,
            read: only(read),
            write: only(write),
        }
    }

    #[test]
    fn a_drawer_reach_covers_only_no_stronger_access_and_no_wider_kinds() {
        let held = reach(
            DrawerAccess::Write,
            &["org.example.a", "org.example.b"],
            &["org.example.a"],
        );
        for (requested, covered) in [
            (DrawerReach::all_kinds(DrawerAccess::Read), false),
            (reach(DrawerAccess::Read, &["org.example.a"], &[]), true),
            (
                reach(
                    DrawerAccess::Write,
                    &["org.example.a", "org.example.b"],
                    &["org.example.a"],
                ),
                true,
            ),
            // A kind held only for reading is not held for writing.
            (reach(DrawerAccess::Write, &[], &["org.example.b"]), false),
            (reach(DrawerAccess::Write, &["org.example.c"], &[]), false),
            (reach(DrawerAccess::Admin, &["org.example.a"], &[]), false),
        ] {
            assert_eq!(held.covers(&requested), covered, "{requested:?}");
        }
        assert!(DrawerReach::all_kinds(DrawerAccess::Write).covers(&held));
    }

    #[test]
    fn a_reach_with_only_write_kinds_reads_them_and_one_with_read_kinds_reads_those() {
        assert_eq!(
            reach(DrawerAccess::Write, &[], &["org.example.y"]).readable(),
            &only(&["org.example.y"])
        );
        assert_eq!(
            reach(DrawerAccess::Write, &["org.example.x"], &["org.example.y"]).readable(),
            &only(&["org.example.x"])
        );
    }

    #[test]
    fn kinds_union_is_the_widest_of_both() {
        assert_eq!(
            only(&["org.example.a"]).union(only(&["org.example.b"])),
            only(&["org.example.a", "org.example.b"])
        );
        assert_eq!(
            only(&["org.example.a"]).union(DatumKinds::All),
            DatumKinds::All
        );
        assert_eq!(
            DatumKinds::All.union(only(&["org.example.a"])),
            DatumKinds::All
        );
    }
}
