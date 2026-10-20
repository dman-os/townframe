//! The DocumentsRepo wire surface.
//!
//! A document is addressed by its `DocumentId`; its content is a map of facets, each read and
//! written as one complete canonical JSON value. Every read names the document heads it is taken
//! at and every write names the heads it is based on — current heads are an explicit choice
//! ([`At::Current`], [`WriteMode::AtCurrent`]), never a silent default.
//!
//! Provenance stays inside the repository. What leaves it is **facet heads**: the change hashes
//! that introduced each signature currently covering the facet. There is one per signature, so a
//! concurrently written facet has several. They are ordinary change hashes, so a caller can seek a
//! facet's history with them exactly the way it seeks a document's history with document heads,
//! and can hand them back to [`WriteMode::IfFacetsUnchanged`].
//!
//! The `Unsafe*`/`Inspect*` operations expose raw evidence for diagnostics and tests and are never
//! a fallback of the ordinary path.

use automerge::ChangeHash;
use big_cabinet_core::FacetKey;
use big_cabinet_core::dmeta::{Bytes, DocumentId, FacetClaim, FacetSignature, MultibaseKey};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use service_utils_rs::{ContextError, Operation, XrpcOperation, service_protocol};
use std::collections::{BTreeMap, BTreeSet};

use crate::authority::{AuthorityServiceError, CapId};

/// A caller-chosen idempotency key. A command that completed returns its original receipt when
/// replayed with the same request; one still in flight is refused rather than run twice.
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
    type Error = DocumentsError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.is_empty() || value.len() > Self::MAX_BYTES {
            return Err(DocumentsError::InvalidCommand);
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

/// Which document heads a read is taken at.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "at", content = "heads", rename_all = "camelCase")]
pub enum At {
    Current,
    Heads(Vec<ChangeHash>),
}

/// The commit condition of a write. See ADR 015 §2.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "camelCase")]
pub enum WriteMode {
    /// Commit only if the document heads still equal `heads`; the write is based on them.
    IfDocumentUnchanged { heads: Vec<ChangeHash> },
    /// Commit only if each named facet still has exactly these facet heads (empty: never written
    /// or written by nobody verifiable). The write is based on current heads at commit time.
    IfFacetsUnchanged {
        facets: BTreeMap<FacetKey, Vec<ChangeHash>>,
    },
    /// Write from these historical heads, producing a concurrent change if newer heads exist.
    AtHeads { heads: Vec<ChangeHash> },
    /// Write from the heads current when the write is prepared. Authenticity is still checked.
    AtCurrent,
}

/// One facet as read at some heads, after the repository verified it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum FacetRead {
    /// Every signature covering the facet verifies and the stored value is exactly what they
    /// signed (merged by Automerge's rules where they are concurrent).
    Accepted {
        value: Value,
        facet_heads: Vec<ChangeHash>,
    },
    /// Removed by a verified removal claim, or never written. `facet_heads` is empty when the slot
    /// was never written.
    Absent { facet_heads: Vec<ChangeHash> },
    /// Verification could not complete, such as an unsupported encoding or a resource limit.
    /// Missing metadata in an applied change is malformed, not pending. `value` is present only
    /// when the read asked to be optimistic; it can never be written on top of.
    Pending {
        reason: String,
        facet_heads: Vec<ChangeHash>,
        value: Option<Value>,
    },
    /// Proven invalid: a bad signature, or a change to the facet that no valid signature covers.
    /// The value is never returned.
    Rejected { reason: String },
    /// Authentic, but the stored value cannot be read as its declaration states (or, with no
    /// declaration, has no raw JSON spelling). It can still be overwritten.
    Undecodable {
        reason: String,
        facet_heads: Vec<ChangeHash>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FacetsRead {
    pub heads: Vec<ChangeHash>,
    pub facets: BTreeMap<FacetKey, FacetRead>,
}

/// The slots present at some heads and their facet heads. This is a listing, not a read: it
/// returns no values and verifies nothing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FacetListing {
    pub heads: Vec<ChangeHash>,
    pub facets: BTreeMap<FacetKey, Vec<ChangeHash>>,
}

/// A successful local commit. It asserts local persistence only, not delivery to any peer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WriteReceipt {
    pub document_id: DocumentId,
    pub heads: Vec<ChangeHash>,
}

/// Why a facet base or read was not accepted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum Unaccepted {
    Pending { reason: String },
    Rejected { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum DocumentsError {
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
    DocumentNotFound,
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
        facets: BTreeMap<FacetKey, Vec<ChangeHash>>,
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

/// Create a document holding `facets`, then place it through the access policy. `placement` is the
/// policy's own spelling of where the document goes (for DrawerRepo, a drawer); this repository
/// does not interpret it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateDocument {
    pub capability: CapId,
    pub command: CommandId,
    pub placement: String,
    pub facets: BTreeMap<FacetKey, Value>,
}

impl Operation for CreateDocument {
    type Output = WriteReceipt;
    type Error = DocumentsError;
    const XRPC: XrpcOperation =
        XrpcOperation::procedure("org.example.daybook.documents.createDocument")
            .with_schemas("WriteReceipt", "DocumentsError");
}

/// Set complete facet values and remove facets. Facets not named are unchanged; inside a set
/// facet, a property the submitted value omits is removed at the base heads.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WriteFacets {
    pub capability: CapId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<CommandId>,
    pub document_id: DocumentId,
    pub mode: WriteMode,
    #[serde(default)]
    pub set: BTreeMap<FacetKey, Value>,
    #[serde(default)]
    pub remove: BTreeSet<FacetKey>,
}

impl Operation for WriteFacets {
    type Output = WriteReceipt;
    type Error = DocumentsError;
    const XRPC: XrpcOperation =
        XrpcOperation::procedure("org.example.daybook.documents.writeFacets")
            .with_schemas("WriteReceipt", "DocumentsError");
}

/// Branch `document_id` at `at` into a new document, then place the branch through the access
/// policy like a created document.
///
/// A branch *is* the origin's changes up to `at` under a new document id: no value is copied, so
/// every signature on the origin reconstructs on the branch exactly as it did there, and the branch
/// can later be merged back with [`MergeDocument`]. The branch's first change is its
/// `org.example.daybook.branch/<branch id>` facet naming `path`.
///
/// A branch is announced on the origin by `org.example.daybook.branches/main`, which needs write
/// access to the origin. A `stealth` branch skips that and needs only read access.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BranchDocument {
    pub capability: CapId,
    pub command: CommandId,
    pub document_id: DocumentId,
    pub at: At,
    pub path: String,
    #[serde(default)]
    pub stealth: bool,
    pub placement: String,
}

impl Operation for BranchDocument {
    type Output = WriteReceipt;
    type Error = DocumentsError;
    const XRPC: XrpcOperation =
        XrpcOperation::procedure("org.example.daybook.documents.branchDocument")
            .with_schemas("WriteReceipt", "DocumentsError");
}

/// Merge `source` as of `at` into `target`: Automerge's merge of the source's changes, which adds
/// no change of its own. Both must share history (a branch and its origin, or two branches of one
/// origin), and every facet of the source at `at` must be accepted. The receipt names the target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MergeDocument {
    pub capability: CapId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<CommandId>,
    pub source: DocumentId,
    pub at: At,
    pub target: DocumentId,
}

impl Operation for MergeDocument {
    type Output = WriteReceipt;
    type Error = DocumentsError;
    const XRPC: XrpcOperation =
        XrpcOperation::procedure("org.example.daybook.documents.mergeDocument")
            .with_schemas("WriteReceipt", "DocumentsError");
}

/// Read facets at `at`. `facets: None` reads every slot present. An `optimistic` read also returns
/// the value of a pending facet; a rejected facet's value is never returned.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadFacets {
    pub capability: CapId,
    pub document_id: DocumentId,
    pub at: At,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub facets: Option<BTreeSet<FacetKey>>,
    #[serde(default)]
    pub optimistic: bool,
}

impl Operation for ReadFacets {
    type Output = FacetsRead;
    type Error = DocumentsError;
    const XRPC: XrpcOperation =
        XrpcOperation::procedure("org.example.daybook.documents.readFacets")
            .with_schemas("FacetsRead", "DocumentsError");
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListFacets {
    pub capability: CapId,
    pub document_id: DocumentId,
    pub at: At,
}

impl Operation for ListFacets {
    type Output = FacetListing;
    type Error = DocumentsError;
    const XRPC: XrpcOperation =
        XrpcOperation::procedure("org.example.daybook.documents.listFacets")
            .with_schemas("FacetListing", "DocumentsError");
}

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
    pub document_id: DocumentId,
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
    pub document_id: DocumentId,
    pub facet_key: FacetKey,
    pub heads: Vec<ChangeHash>,
}

impl Operation for UnsafeInspectFacet {
    type Output = UnsafeFacetInspection;
    type Error = DocumentsError;
    const XRPC: XrpcOperation =
        XrpcOperation::procedure("org.example.daybook.documents.unsafeInspectFacet")
            .with_schemas("UnsafeFacetInspection", "DocumentsError");
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InspectFacetClaims {
    pub capability: CapId,
    pub document_id: DocumentId,
    pub facet_key: FacetKey,
    pub heads: Vec<ChangeHash>,
}

impl Operation for InspectFacetClaims {
    type Output = FacetClaimsInspection;
    type Error = DocumentsError;
    const XRPC: XrpcOperation =
        XrpcOperation::procedure("org.example.daybook.documents.inspectFacetClaims")
            .with_schemas("FacetClaimsInspection", "DocumentsError");
}

service_protocol! {
    pub enum DocumentsProtocol => DocumentsMessage {
        unary {
            CreateDocument(CreateDocument),
            WriteFacets(WriteFacets),
            BranchDocument(BranchDocument),
            MergeDocument(MergeDocument),
            ReadFacets(ReadFacets),
            ListFacets(ListFacets),
            UnsafeInspectFacet(UnsafeInspectFacet),
            InspectFacetClaims(InspectFacetClaims)
        }
        stream {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    utils_rs::table_tests! {
        command_id_bounds,
        (input, valid),
        {
            let expected = if valid { Ok(CommandId(input.clone())) } else { Err(DocumentsError::InvalidCommand) };
            assert_eq!(CommandId::try_from(input.clone()), expected);
            let json = serde_json::to_string(&input).expect("serialize input");
            assert_eq!(
                serde_json::from_str::<CommandId>(&json).map_err(|error| error.classify()),
                expected.clone().map_err(|_| serde_json::error::Category::Data),
            );
            if let Ok(id) = expected {
                assert_eq!(id.as_str(), input);
                assert_eq!(id.to_string(), input);
                assert_eq!(serde_json::to_string(&id).expect("serialize command"), json);
            }
        }
    }

    command_id_bounds! {
        empty: (String::new(), false),
        single_byte: ("a".to_string(), true),
        below_max_bytes: ("x".repeat(CommandId::MAX_BYTES - 1), true),
        max_bytes: ("x".repeat(CommandId::MAX_BYTES), true),
        one_over_max: ("x".repeat(CommandId::MAX_BYTES + 1), false),
        multibyte_below_max: (format!("{}x", "é".repeat(CommandId::MAX_BYTES / 2 - 1)), true),
        multibyte_at_max: ("é".repeat(CommandId::MAX_BYTES / 2), true),
        multibyte_one_over_max: (format!("{}x", "é".repeat(CommandId::MAX_BYTES / 2)), false),
        whitespace_only: (" ".to_string(), true),
    }

    utils_rs::table_tests! {
        command_id_serde,
        (json, expected),
        {
            let expected: Option<String> = expected;
            let expected = expected.map(CommandId).ok_or(serde_json::error::Category::Data);
            let actual = serde_json::from_str::<CommandId>(&json).map_err(|error| error.classify());
            assert_eq!(actual, expected);
            if let Ok(id) = actual {
                assert_eq!(serde_json::to_value(&id).expect("serialize command"), Value::String(id.to_string()));
            }
        }
    }

    command_id_serde! {
        escaped_multibyte_at_max: (format!("\"{}\"", "\\u00e9".repeat(CommandId::MAX_BYTES / 2)), Some("é".repeat(CommandId::MAX_BYTES / 2))),
        escaped_multibyte_one_over_max: (format!("\"{}x\"", "\\u00e9".repeat(CommandId::MAX_BYTES / 2)), None),
        escaped_surrogate_pairs_at_max: (format!("\"{}\"", "\\ud800\\udc00".repeat(CommandId::MAX_BYTES / 4)), Some("\u{10000}".repeat(CommandId::MAX_BYTES / 4))),
        escaped_surrogate_pairs_one_over_max: (format!("\"{}x\"", "\\ud800\\udc00".repeat(CommandId::MAX_BYTES / 4)), None),
        null_is_not_a_command: ("null".to_string(), None),
        number_is_not_a_command: ("42".to_string(), None),
        boolean_is_not_a_command: ("true".to_string(), None),
        array_is_not_a_command: ("[]".to_string(), None),
        object_is_not_a_command: ("{}".to_string(), None),
    }

    utils_rs::table_tests! {
        at_wire_tags,
        (at, expected),
        {
            let value = serde_json::to_value(&at).expect("serialize");
            assert_eq!(value, expected);
            let back: At = serde_json::from_value(value).expect("deserialize");
            assert_eq!(back, at);
        }
    }

    at_wire_tags! {
        current: (At::Current, serde_json::json!({"at": "current"})),
        heads: (At::Heads(vec![ChangeHash([1; 32])]), serde_json::json!({"at": "heads", "heads": [ChangeHash([1; 32])]})),
    }

    utils_rs::table_tests! {
        write_mode_wire_tags,
        (mode, expected),
        {
            let value = serde_json::to_value(&mode).expect("serialize");
            assert_eq!(value, expected);
            let back: WriteMode = serde_json::from_value(value).expect("deserialize");
            assert_eq!(back, mode);
        }
    }

    write_mode_wire_tags! {
        at_current: (WriteMode::AtCurrent, serde_json::json!({"mode": "atCurrent"})),
        if_document_unchanged: (
            WriteMode::IfDocumentUnchanged { heads: vec![ChangeHash([2; 32])] },
            serde_json::json!({"mode": "ifDocumentUnchanged", "heads": [ChangeHash([2; 32])]})
        ),
        if_facets_unchanged: (
            WriteMode::IfFacetsUnchanged { facets: BTreeMap::from([(FacetKey::from_text("note", "a").expect("facet key"), vec![ChangeHash([3; 32])])]) },
            serde_json::json!({"mode": "ifFacetsUnchanged", "facets": {"note/a": [ChangeHash([3; 32])]}})
        ),
        at_heads: (
            WriteMode::AtHeads { heads: vec![ChangeHash([4; 32])] },
            serde_json::json!({"mode": "atHeads", "heads": [ChangeHash([4; 32])]})
        ),
    }
}
