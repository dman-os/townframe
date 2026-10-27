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
mod availability;
mod branch;
mod claim_history;
mod commands;
mod create;
pub(crate) mod gate;
mod inspect;
mod list;
mod merge;
mod observe;
pub(crate) mod pipeline;
mod query;
mod read;
mod write;

#[cfg(test)]
mod e2e;

pub use self::access::{AccessError, AccessFuture, AccessRequest, DocAccess, LedgerAccess};
pub use self::claim_history::{
    ClaimHistoryEntry, ClaimHistoryError, ClaimHistoryResult, HistoryBudget, ReconstructedClaim,
    SignatureIntroduction, predecessor_ids, reconstruct_claim, reconstruct_claim_for_marker,
    reconstruct_current_claims,
};

use self::commands::Commands;
use self::gate::{DMETA_KEY, Gate, Verdict, doc_actor, facet_version, signature_heads};
use crate::authority::{
    Actor, AuthorityProtocol, AuthorityServiceError, Authorization, CapId, Capability,
    DomainAuthority, Namespace, ResolveActor, Rights, SignFacetClaim,
};
use crate::capability::{DomainCapabilities, Invocation};
use crate::index::facet_ref::FacetRefStore;
use crate::index::facet_set::FacetSetStore;
use crate::schema::{SchemaHandle, SchemaRepo};
use automerge::transaction::{CommitOptions, Transactable, Transaction};
use automerge::{Automerge, ChangeHash, ObjId, ObjType, PatchLog, ReadDoc, Value};
use big_cabinet_core::FacetKey;
use big_cabinet_core::dmeta::{
    ActorMetadata, BRANCH_FACET_TAG, Cid, ClaimOperation, DMETA_FACET_TAG, Dmeta, DocId,
    FacetClaim, FacetMetadata, FacetSignature, MultibaseKey, NativeReadBudget, NativeValue,
    SignatureBytes, Uuid7, cid_of, encode_native,
};
use big_cabinet_core::facet::{CompiledSchema, hydrate_value, reconcile_json_atomic};
use big_repo::{BigDocHandle, DocLookup, SharedBigRepo};
use serde_json::Value as Json;
use service_utils_rs::{
    Client, ClientError, ContextError, Envelope, EventStream, Handler, Host, RequestContext,
    ResponseEnvelope, Spawner, StopToken,
};
use sqlx_utils_rs::SqlCtx;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

pub use big_cabinet_core::doc::*;

/// Everything DocsRepo is booted with.
pub struct DocsConfig {
    pub big_repo: SharedBigRepo,
    pub schema_repo: Arc<SchemaRepo>,
    pub authority: Client<AuthorityProtocol>,
    pub issuer: CapId,
    pub namespace: Namespace,
    /// This node's attribution issuer. Every change DocsRepo makes is authored under this key
    /// as its Automerge actor, which is what lets a verifier find the expected issuer of the
    /// attribution certificates the change carries.
    pub node_issuer: MultibaseKey,
    pub access: Arc<dyn DocAccess>,
    pub sql: SqlCtx,
    /// The facet-set projection backing `ObserveFacets` and `QueryFacets`, if
    /// the embedder runs one. The embedder owns `FacetSetStore::run`; DocsRepo
    /// only reads the committed projection. Without it, `ObserveFacets` and
    /// `QueryFacets` are refused.
    pub facet_set: Option<Arc<FacetSetStore>>,
    /// The declared-reference index backing `QueryFacetRefs`, if the embedder
    /// runs one.
    pub facet_ref: Option<Arc<FacetRefStore>>,
}

pub struct DocsService {
    big_repo: SharedBigRepo,
    schema_repo: Arc<SchemaRepo>,
    authority: Client<AuthorityProtocol>,
    access: Arc<dyn DocAccess>,
    issuer: CapId,
    capabilities: DomainCapabilities,
    inspection_capabilities: DomainCapabilities,
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
    facet_ref: Option<Arc<FacetRefStore>>,
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
    match error {
        AccessError::Refused(_) => DocsError::Denied,
        AccessError::Unavailable(_) => DocsError::Access(error.to_string()),
    }
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
    async fn invocation(
        &self,
        context: &RequestContext,
        capability: CapId,
    ) -> Result<Invocation<DocScope>, DocsError> {
        self.capabilities
            .resolve(context, capability)
            .await
            .map_err(authority_error)
    }

    async fn allowed(
        access_policy: &dyn DocAccess,
        context: &RequestContext,
        authority: &DomainAuthority<DocScope>,
        request: AccessRequest<'_>,
    ) -> Result<bool, DocsError> {
        match authority {
            DomainAuthority::Identity(actor) => access_policy
                .check(context, *actor, request)
                .await
                .map_err(access_error),
            DomainAuthority::Scoped(scope) => {
                let (document, needed) = match request {
                    AccessRequest::Read(doc)
                        if scope.read.contains(doc) || scope.write.contains(doc) =>
                    {
                        return Ok(true);
                    }
                    AccessRequest::Write(doc) if scope.write.contains(doc) => return Ok(true),
                    AccessRequest::Create { placement } => {
                        return Ok(scope.create.contains(placement)
                            || crate::drawer::parse_placement(placement).is_some_and(|drawer| {
                                scope.drawers.get(&drawer).is_some_and(|access| {
                                    *access >= big_cabinet_core::drawer::DrawerAccess::Write
                                })
                            }));
                    }
                    AccessRequest::Read(doc) => (doc, big_cabinet_core::drawer::DrawerAccess::Read),
                    AccessRequest::Write(doc) => {
                        (doc, big_cabinet_core::drawer::DrawerAccess::Write)
                    }
                };
                let documents = BTreeSet::from([document.clone()]);
                Ok(
                    Self::allowed_docs(access_policy, context, authority, &documents, needed)
                        .await?
                        .contains(document),
                )
            }
        }
    }

    async fn allowed_docs(
        access_policy: &dyn DocAccess,
        context: &RequestContext,
        authority: &DomainAuthority<DocScope>,
        documents: &BTreeSet<DocId>,
        needed: big_cabinet_core::drawer::DrawerAccess,
    ) -> Result<BTreeSet<DocId>, DocsError> {
        use big_cabinet_core::drawer::DrawerAccess;
        let DomainAuthority::Scoped(scope) = authority else {
            return Err(DocsError::Denied);
        };
        let mut allowed: BTreeSet<_> = documents
            .iter()
            .filter(|doc| {
                scope.write.contains(*doc)
                    || (needed == DrawerAccess::Read && scope.read.contains(*doc))
            })
            .cloned()
            .collect();
        let candidates: BTreeSet<_> = documents.difference(&allowed).cloned().collect();
        if candidates.is_empty() {
            return Ok(allowed);
        }
        let drawers: BTreeMap<_, _> = scope
            .drawers
            .iter()
            .filter(|(_, held)| **held >= needed)
            .map(|(drawer, held)| (drawer.clone(), *held))
            .collect();
        if !drawers.is_empty() {
            allowed.extend(
                access_policy
                    .scoped_docs(context, &drawers, Some(&candidates))
                    .await
                    .map_err(access_error)?
                    .into_iter()
                    .filter_map(|(doc, held)| (held >= needed).then_some(doc)),
            );
        }
        Ok(allowed)
    }

    async fn authorize(
        &self,
        context: &RequestContext,
        authority: &DomainAuthority<DocScope>,
        request: AccessRequest<'_>,
    ) -> Result<(), DocsError> {
        if Self::allowed(self.access.as_ref(), context, authority, request).await? {
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

service_utils_rs::bind_service_protocol! {
    pub struct DocsBinding for DocsProtocol => DocsMessage {
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

pub type DocsHandler = Arc<DocsService>;

pub struct DocsRepo {
    service: Arc<DocsService>,
    mint_spawner: Spawner,
    pub host: Arc<Host<DocsHandler>>,
    pub client: Client<DocsProtocol>,
}

impl DocsRepo {
    pub async fn mint_inspection_handle(
        &self,
        doc_id: DocId,
        signer: CapId,
    ) -> Result<Capability, DocsError> {
        let service = Arc::clone(&self.service);
        let owner = self.mint_spawner.clone();
        let task = self
            .mint_spawner
            .spawn(async move {
                let mint = async {
                    let context = RequestContext::root();
                    let caller = service
                        .inspection_capabilities
                        .source::<DocId>(
                            &context,
                            Authorization {
                                authority: service.issuer,
                                signer,
                            },
                        )
                        .await
                        .map_err(authority_error)?;
                    service
                        .inspection_capabilities
                        .mint(
                            &context,
                            service.issuer,
                            &caller,
                            doc_id,
                            Rights::USE,
                            Vec::new(),
                        )
                        .await
                        .map_err(authority_error)
                };
                tokio::select! {
                    biased;
                    _ = owner.cancelled() => Err(ContextError::Cancelled.into()),
                    result = mint => result,
                }
            })
            .ok_or(ContextError::Cancelled)?;
        task.await
            .expect("inspection capability mint task panicked")
    }
}

pub async fn boot(config: DocsConfig) -> Result<(DocsRepo, StopToken), DocsError> {
    let (docs, stop) = prepare(config).await?;
    if let Err(error) = docs.recover().await {
        stop.stop().await;
        return Err(error);
    }
    Ok((docs, stop))
}

pub async fn prepare(config: DocsConfig) -> Result<(DocsRepo, StopToken), DocsError> {
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
        capabilities: DomainCapabilities::new(
            config.authority.clone(),
            config.namespace,
            "DocsRepo/scope/v2/",
        ),
        inspection_capabilities: DomainCapabilities::new(
            config.authority.clone(),
            config.namespace,
            "DocsRepo/UNSAFE/inspection/v2/",
        ),
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
        facet_ref: config.facet_ref,
        tasks: service_spawner,
    });
    let (mint_stop, mint_spawner) = StopToken::new();
    let (host, host_stop) = Host::new(Arc::clone(&service));
    let stop = host_stop
        .then(mint_stop)
        .then(service_stop)
        .then(service.capabilities.stop_token());
    let client = Client::direct(DocsBinding::new(Arc::clone(&host)));
    Ok((
        DocsRepo {
            service,
            mint_spawner,
            host,
            client,
        },
        stop,
    ))
}

impl DocsRepo {
    pub async fn recover(&self) -> Result<(), DocsError> {
        self.service.recover().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    utils_rs::table_tests! {
        command_id_bounds,
        (input, valid),
        {
            let expected = if valid { Ok(CommandId::try_from(input.clone()).expect("valid command")) } else { Err(DocsError::InvalidCommand) };
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
            let expected = expected.map(|input| CommandId::try_from(input).expect("valid command")).ok_or(serde_json::error::Category::Data);
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
