//! DocsRepo: documents as maps of signed facets, read and written as whole JSON values at
//! explicit heads.
//!
//! - `<op>.rs`: one file per operation — its wire types, its implementation, and its tests.
//! - [`access`]: the policy seam deciding who may read, write, and create.
//! - `gate`: the authenticity check every read and every write base passes through.
//! - `commands`: the idempotency and crash-recovery ledger.
//! - `claim_history`: reconstruction of a signed claim from the change that introduced it.
//! - `pipeline`: the two-phase write machinery and boot recovery shared by create, write, and branch.
//!
//! A document is addressed by its `DocId`; its content is a map of facets, each read and
//! written as one complete canonical JSON value. Every read names the document heads it is taken
//! at and every write names the heads it is based on — current heads are an explicit choice
//! ([`At::Current`], [`WriteMode::AtCurrent`]), never a silent default.
//!
//! Provenance stays inside the repository. What leaves it is a [`FacetVersion`]: the change hashes
//! that introduced each signature currently covering the facet, plus a Lamport clock. There is one
//! per signature, so a concurrently written facet has several. They are ordinary change hashes, so
//! a caller can seek a facet's history with them exactly the way it seeks a document's history
//! with document heads, and can hand them back to [`WriteMode::IfFacetsUnchanged`].
//!
//! TODO(big-repo-delete): DocsRepo exposes no Delete operation, and a partially created
//! branch cannot be rolled back, because BigRepo has no physical document-deletion primitive —
//! `abandon_allocation` deletes only the reservation row, nothing from the document's events,
//! sedimentree, or bytes. Branch documents are therefore committed with no coparents/groups, so
//! an unregistered branch never leaves this node, and branch recovery rolls forward instead of
//! back. Add document deletion to BigRepo before exposing Delete or branch rollback.

pub mod access;
mod branch;
mod claim_history;
mod commands;
mod create;
pub(crate) mod gate;
mod inspect;
mod list;
mod merge;
mod observe;
mod pipeline;
mod read;
mod write;

#[cfg(test)]
mod e2e;

pub use self::access::{AccessError, AccessFuture, AccessRequest, DocAccess, LedgerAccess};
pub use self::branch::BranchDoc;
pub use self::claim_history::{
    ClaimHistoryEntry, ClaimHistoryError, ClaimHistoryResult, HistoryBudget, ReconstructedClaim,
    SignatureIntroduction, predecessor_ids, reconstruct_claim, reconstruct_claim_for_marker,
    reconstruct_current_claims,
};
pub use self::create::CreateDoc;
pub use self::inspect::{
    EvidenceScope, FacetClaimEvidence, FacetClaimsInspection, InspectFacetClaims,
    SignatureEvidence, UnsafeFacetInspection, UnsafeInspectFacet, VerifiedSignatureEvidence,
};
pub use self::list::ListFacets;
pub use self::merge::MergeDoc;
pub use self::observe::{FacetObserved, ObserveFacets, ObserveScope, ObservedFacet};
pub use self::read::ReadFacets;
pub use self::write::WriteFacets;

use self::commands::Commands;
use self::gate::{DMETA_KEY, Gate, Verdict, doc_actor, facet_version, signature_heads};
use crate::authority::{
    Actor, AuthorityProtocol, AuthorityServiceError, CapId, Capability, InspectCapability,
    MintCapability, ResolveActor, Rights, SignFacetClaim,
};
use crate::index::facet_set::FacetSetStore;
use crate::schema::{SchemaHandle, SchemaRepo};
use automerge::transaction::{CommitOptions, Transactable, Transaction};
use automerge::{Automerge, ChangeHash, ObjId, ObjType, PatchLog, ReadDoc, Value};
use big_cabinet_core::dmeta::{
    ActorMetadata, BRANCH_FACET_TAG, Bytes, Cid, ClaimOperation, DMETA_FACET_TAG, Dmeta, DocId,
    FacetClaim, FacetMetadata, FacetSignature, MultibaseKey, NativeReadBudget, NativeValue,
    SignatureBytes, Uuid7, cid_of, encode_native,
};
use big_cabinet_core::facet::{CompiledSchema, hydrate_value, reconcile_json_atomic};
use big_cabinet_core::{FacetKey, FacetTag};
use big_repo::{BigDocHandle, DocLookup, SharedBigRepo};
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;
use service_utils_rs::{
    Client, ClientError, ContextError, Envelope, EventStream, Handler, Host, Operation,
    RequestContext, ResponseEnvelope, Spawner, StopToken, Subscription, XrpcOperation,
    service_protocol,
};
use sqlx_utils_rs::SqlCtx;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

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
    IfDocUnchanged { heads: Vec<ChangeHash> },
    /// Commit only if each named facet still has exactly these facet heads (empty: never written
    /// or written by nobody verifiable). The write is based on current heads at commit time.
    IfFacetsUnchanged {
        facets: BTreeMap<FacetKey, FacetVersion>,
    },
    /// Write from these historical heads, producing a concurrent change if newer heads exist.
    AtHeads { heads: Vec<ChangeHash> },
    /// Write from the heads current when the write is prepared. Authenticity is still checked.
    AtCurrent,
}

/// The version of one facet slot: the change hashes that introduced each
/// signature currently covering it, plus a Lamport clock for ordering.
///
/// `clock` is the greatest Automerge `max_op` among `heads`. Automerge assigns
/// every new change a `start_op` above the document's current maximum, so a
/// version produced by writing on top of another always has a strictly greater
/// clock. Concurrent versions are incomparable: equal clock with different
/// heads means the versions merged or diverged, and the consumer must re-read
/// rather than pick one. The clock is an ordering aid only; write conditions
/// compare `heads` exactly.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FacetVersion {
    pub heads: Vec<ChangeHash>,
    pub clock: u64,
}

/// One facet as read at some heads, after the repository verified it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum FacetRead {
    /// Every signature covering the facet verifies and the stored value is exactly what they
    /// signed (merged by Automerge's rules where they are concurrent).
    Accepted { value: Json, version: FacetVersion },
    /// Removed by a verified removal claim, or never written. `version.heads` is empty when the
    /// slot was never written.
    Absent { version: FacetVersion },
    /// Verification could not complete, such as an unsupported encoding or a resource limit.
    /// Missing metadata in an applied change is malformed, not pending. `value` is present only
    /// when the read asked to be optimistic; it can never be written on top of.
    Pending {
        reason: String,
        version: FacetVersion,
        value: Option<Json>,
    },
    /// Proven invalid: a bad signature, or a change to the facet that no valid signature covers.
    /// The value is never returned.
    Rejected { reason: String },
    /// Authentic, but the stored value cannot be read as its declaration states (or, with no
    /// declaration, has no raw JSON spelling). It can still be overwritten.
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

/// The slots present at some heads and their facet heads. This is a listing, not a read: it
/// returns no values and verifies nothing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FacetListing {
    pub heads: Vec<ChangeHash>,
    pub facets: BTreeMap<FacetKey, FacetVersion>,
}

/// A successful local commit. It asserts local persistence only, not delivery to any peer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WriteReceipt {
    pub doc_id: DocId,
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

/// Everything DocsRepo is booted with.
pub struct DocsConfig {
    pub big_repo: SharedBigRepo,
    pub schema_repo: Arc<SchemaRepo>,
    pub authority: Client<AuthorityProtocol>,
    /// DocsRepo's own actor capability: it names the objects it mints and checks.
    pub issuer: CapId,
    /// This node's attribution issuer. Every change DocsRepo makes is authored under this key
    /// as its Automerge actor, which is what lets a verifier find the expected issuer of the
    /// attribution certificates the change carries.
    pub node_issuer: MultibaseKey,
    pub access: Arc<dyn DocAccess>,
    pub sql: SqlCtx,
    /// The facet-set projection backing `ObserveFacets`, if the embedder runs
    /// one. The embedder owns `FacetSetStore::run`; DocsRepo only reads the
    /// committed projection. Without it, `ObserveFacets` is refused.
    pub facet_set: Option<Arc<FacetSetStore>>,
}

pub struct DocsService {
    big_repo: SharedBigRepo,
    schema_repo: Arc<SchemaRepo>,
    authority: Client<AuthorityProtocol>,
    access: Arc<dyn DocAccess>,
    issuer: CapId,
    /// DocsRepo's own actor, which signs `org.example.daybook.branch/<id>` on the branch and,
    /// unless stealth, the same facet on the origin. Access is checked before either is written;
    /// signing them as the
    /// repository is what lets crash recovery finish a branch without the caller's capability.
    system_actor: Actor,
    node_issuer: MultibaseKey,
    raw: CompiledSchema,
    dmeta: SchemaHandle,
    branch: SchemaHandle,
    commands: Commands,
    facet_set: Option<Arc<FacetSetStore>>,
    /// Spawns the service's long-lived per-subscription tasks (the
    /// `ObserveFacets` pumps). Stopped after the host and the mint spawner,
    /// in reverse construction order.
    tasks: Spawner,
}

fn branch_key(branch: &DocId) -> FacetKey {
    format!("{BRANCH_FACET_TAG}/{branch}")
        .parse()
        .expect("a branch facet key is well formed")
}

/// The change on `doc` whose commit message is `command`: how a write is found after a crash.
fn committed_change(doc: &Automerge, command: &CommandId) -> Option<ChangeHash> {
    doc.get_changes(&[])
        .into_iter()
        .rev()
        .find(|change| change.message() == Some(command.as_str()))
        .map(|change| change.hash())
}

fn inspection_object(doc_id: &DocId) -> String {
    format!("DocsRepo/UNSAFE/inspection/{doc_id}")
}

fn authority_error(error: ClientError<AuthorityServiceError>) -> DocsError {
    match error {
        ClientError::Operation(AuthorityServiceError::Context(error)) => error.into(),
        ClientError::Operation(
            AuthorityServiceError::InvalidCapability
            | AuthorityServiceError::Unauthorized
            | AuthorityServiceError::UnrecognizedActor,
        ) => DocsError::Denied,
        ClientError::Operation(error) => DocsError::Authority(error),
        ClientError::Transport { message, .. } => DocsError::AuthorityUnavailable(message),
    }
}

fn access_error(error: AccessError) -> DocsError {
    DocsError::Access(error.to_string())
}

fn repository(error: impl std::fmt::Display) -> DocsError {
    DocsError::Repository(error.to_string())
}

fn sorted(mut heads: Vec<ChangeHash>) -> Vec<ChangeHash> {
    heads.sort();
    heads.dedup();
    heads
}

fn check_heads(doc: &Automerge, heads: &[ChangeHash]) -> Result<(), DocsError> {
    let unknown: Vec<_> = heads
        .iter()
        .copied()
        .filter(|head| doc.get_change_by_hash(head).is_none())
        .collect();
    if !unknown.is_empty() {
        return Err(DocsError::UnknownHeads { heads: unknown });
    }
    Ok(())
}

fn resolve(doc: &Automerge, at: &At) -> Result<Vec<ChangeHash>, DocsError> {
    match at {
        At::Current => Ok(sorted(doc.get_heads())),
        At::Heads(heads) => {
            check_heads(doc, heads)?;
            Ok(sorted(heads.clone()))
        }
    }
}

/// Facets only the repository writes. The branch facets are readable like any other facet; dmeta
/// is not a signed facet and is never read through the gate.
fn is_system(key: &FacetKey) -> bool {
    [DMETA_FACET_TAG, BRANCH_FACET_TAG].contains(&key.tag().as_str())
}

fn unaccepted(verdict: Verdict) -> Result<Vec<ChangeHash>, Unaccepted> {
    match verdict {
        Verdict::Accepted { facet_heads, .. } => Ok(facet_heads),
        Verdict::Pending { reason, .. } => Err(Unaccepted::Pending { reason }),
        Verdict::Rejected { reason } => Err(Unaccepted::Rejected { reason }),
    }
}

impl DocsService {
    async fn actor(&self, context: &RequestContext, capability: CapId) -> Result<Actor, DocsError> {
        self.authority
            .call(context.child(), ResolveActor { capability })
            .await
            .map_err(authority_error)
    }

    async fn authorize(
        &self,
        context: &RequestContext,
        actor: &Actor,
        request: AccessRequest<'_>,
    ) -> Result<(), DocsError> {
        if self
            .access
            .check(context, actor.uuid, request)
            .await
            .map_err(access_error)?
        {
            Ok(())
        } else {
            Err(DocsError::Denied)
        }
    }

    async fn handle(&self, doc_id: &DocId) -> Result<BigDocHandle, DocsError> {
        match self.big_repo.get_doc(doc_id).await.map_err(repository)? {
            DocLookup::Missing => Err(DocsError::DocNotFound),
            DocLookup::PendingMaterialization => Err(DocsError::Pending {
                reason: "document materialization".into(),
            }),
            DocLookup::Ready(document) => Ok(document),
        }
    }
}

fn present_keys(doc: &Automerge, heads: &[ChangeHash]) -> Result<BTreeSet<FacetKey>, DocsError> {
    let Some((Value::Object(ObjType::Map), facets)) = doc
        .get_at(automerge::ROOT, "facets", heads)
        .map_err(repository)?
    else {
        return Ok(BTreeSet::new());
    };
    Ok(doc
        .map_range_at(&facets, .., heads)
        .filter(|item| item.key != DMETA_KEY)
        .filter_map(|item| item.key.parse().ok())
        .collect())
}

service_protocol! {
    pub enum DocsProtocol => DocsMessage {
        unary {
            CreateDoc(CreateDoc),
            WriteFacets(WriteFacets),
            BranchDoc(BranchDoc),
            MergeDoc(MergeDoc),
            ReadFacets(ReadFacets),
            ListFacets(ListFacets),
            UnsafeInspectFacet(UnsafeInspectFacet),
            InspectFacetClaims(InspectFacetClaims)
        }
        stream { ObserveFacets(ObserveFacets) }
    }
}

pub type DocsHandler = Arc<DocsService>;

pub struct DocsRepo {
    service: Arc<DocsService>,
    mint_spawner: Spawner,
    pub host: Arc<Host<DocsHandler>>,
    pub client: Client<DocsProtocol>,
}

impl DocsRepo {
    pub async fn mint_inspection_handle(&self, doc_id: DocId) -> Result<Capability, DocsError> {
        let service = Arc::clone(&self.service);
        let owner = self.mint_spawner.clone();
        let task = self
            .mint_spawner
            .spawn(async move {
                tokio::select! {
                    biased;
                    _ = owner.cancelled() => Err(ContextError::Cancelled.into()),
                    result = service.authority.call(RequestContext::root(), MintCapability {
                        issuer: service.issuer,
                        object: inspection_object(&doc_id),
                        rights: Rights::USE,
                    }) => result.map_err(authority_error),
                }
            })
            .ok_or(ContextError::Cancelled)?;
        task.await
            .expect("inspection capability mint task panicked")
    }
}

/// Boot DocsRepo: settle the command ledger, then host the service.
pub async fn boot(config: DocsConfig) -> Result<(DocsRepo, StopToken), DocsError> {
    let actor = config
        .authority
        .call(
            RequestContext::root(),
            ResolveActor {
                capability: config.issuer,
            },
        )
        .await
        .map_err(authority_error)?;
    let core = |tag: &str| {
        config
            .schema_repo
            .resolve_for_read(tag)
            .expect("SchemaRepo always registers the core facets")
    };
    let (dmeta, branch) = (core(DMETA_FACET_TAG), core(BRANCH_FACET_TAG));
    let (service_stop, service_spawner) = StopToken::new();
    let service = Arc::new(DocsService {
        big_repo: config.big_repo,
        schema_repo: config.schema_repo,
        authority: config.authority,
        access: config.access,
        issuer: config.issuer,
        system_actor: actor,
        node_issuer: config.node_issuer,
        raw: CompiledSchema::compile(serde_json::json!({})).expect("the empty schema compiles"),
        dmeta,
        branch,
        commands: Commands::open(config.sql).await?,
        facet_set: config.facet_set,
        tasks: service_spawner,
    });
    service.recover().await?;
    let (mint_stop, mint_spawner) = StopToken::new();
    let (host, host_stop) = Host::new(Arc::clone(&service));
    let client = Client::direct(Arc::clone(&host));
    Ok((
        DocsRepo {
            service,
            mint_spawner,
            host,
            client,
        },
        host_stop.then(mint_stop).then(service_stop),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    utils_rs::table_tests! {
        command_id_bounds,
        (input, valid),
        {
            let expected = if valid { Ok(CommandId(input.clone())) } else { Err(DocsError::InvalidCommand) };
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
                assert_eq!(serde_json::to_value(&id).expect("serialize command"), Json::String(id.to_string()));
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
            WriteMode::IfDocUnchanged { heads: vec![ChangeHash([2; 32])] },
            serde_json::json!({"mode": "ifDocUnchanged", "heads": [ChangeHash([2; 32])]})
        ),
        if_facets_unchanged: (
            WriteMode::IfFacetsUnchanged { facets: BTreeMap::from([(FacetKey::from_text("note", "a").expect("facet key"), FacetVersion { heads: vec![ChangeHash([3; 32])], clock: 7 })]) },
            serde_json::json!({"mode": "ifFacetsUnchanged", "facets": {"note/a": {"heads": [ChangeHash([3; 32])], "clock": 7}}})
        ),
        at_heads: (
            WriteMode::AtHeads { heads: vec![ChangeHash([4; 32])] },
            serde_json::json!({"mode": "atHeads", "heads": [ChangeHash([4; 32])]})
        ),
    }
}
