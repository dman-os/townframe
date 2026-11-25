//! DocsRepo: documents as maps of signed datums, read and written as whole JSON values at
//! explicit heads.
//!
//! - `<op>.rs`: one file per operation — its wire types, its implementation, and its tests.
//! - [`access`]: the policy seam deciding who may read, write, and create.
//! - `gate`: the authenticity check every read and every write base passes through.
//! - `commands`: the idempotency and crash-recovery ledger.
//! - `claim_history`: reconstruction of a signed claim from the change that introduced it.
//! - `pipeline`: the two-phase write machinery and boot recovery shared by create, write, and branch.
//!
//! A document is addressed by its `DocId`; its content is a map of datums, each read and
//! written as one complete canonical JSON value. Every read names the document heads it is taken
//! at and every write names the heads it is based on — current heads are an explicit choice
//! ([`At::Current`], [`WriteMode::AtCurrent`]), never a silent default.
//!
//! Provenance stays inside the repository. What leaves it is a [`DatumVersion`]: the change hashes
//! that introduced each signature currently covering the datum, plus a Lamport clock. There is one
//! per signature, so a concurrently written datum has several. They are ordinary change hashes, so
//! a caller can seek a datum's history with them exactly the way it seeks a document's history
//! with document heads, and can hand them back to [`WriteMode::IfDatumsUnchanged`].
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
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "generated SQL includes test-only command fixtures"
    )
)]
mod sqlite {
    include!("doc/sqlite/gen.rs");
}
mod write;

#[cfg(test)]
mod e2e;

pub use self::access::AccessError;
pub use self::access::AccessFuture;
pub use self::access::AccessRequest;
pub use self::access::DocAccess;
pub use self::access::LedgerAccess;
pub use self::claim_history::ClaimHistoryEntry;
pub use self::claim_history::ClaimHistoryError;
pub use self::claim_history::ClaimHistoryResult;
pub use self::claim_history::HistoryBudget;
pub use self::claim_history::ReconstructedClaim;
pub use self::claim_history::SignatureIntroduction;
pub use self::claim_history::predecessor_ids;
pub use self::claim_history::reconstruct_claim;
pub use self::claim_history::reconstruct_claim_for_marker;
pub use self::claim_history::reconstruct_current_claims;

use self::commands::Commands;
use self::gate::DMETA_KEY;
use self::gate::Gate;
use self::gate::Verdict;
use self::gate::datum_version;
use self::gate::doc_actor;
use self::gate::signature_heads;
use crate::authority::DomainAuthority;
use crate::capability::DomainCapabilities;
use crate::capability::Invocation;
use crate::index::datum_ref::DatumRefStore;
use crate::index::datum_set::DatumSetStore;
use crate::schema::SchemaHandle;
use crate::schema::SchemaRepo;
use automerge::Automerge;
use automerge::ChangeHash;
use automerge::ObjId;
use automerge::ObjType;
use automerge::PatchLog;
use automerge::ReadDoc;
use automerge::Value;
use automerge::transaction::CommitOptions;
use automerge::transaction::Transactable;
use automerge::transaction::Transaction;
use big_cabinet_core::DatumKey;
use big_cabinet_core::authority::Actor;
use big_cabinet_core::authority::AuthorityProtocol;
use big_cabinet_core::authority::AuthorityServiceError;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::Capability;
use big_cabinet_core::authority::Namespace;
use big_cabinet_core::authority::ResolveAuthority;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::authority::SignDatumClaim;
use big_cabinet_core::datum::CompiledSchema;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::datum::hydrate_value;
use big_cabinet_core::datum::reconcile_json_atomic;
use big_cabinet_core::dmeta::ActorMetadata;
use big_cabinet_core::dmeta::BranchPath;
use big_cabinet_core::dmeta::ClaimOperation;
use big_cabinet_core::dmeta::DatumClaim;
use big_cabinet_core::dmeta::DatumMetadata;
use big_cabinet_core::dmeta::DatumSignature;
use big_cabinet_core::dmeta::Dmeta;
use big_cabinet_core::dmeta::DmetaRead;
use big_cabinet_core::dmeta::DmetaSchema;
use big_cabinet_core::dmeta::DmetaWrite;
use big_cabinet_core::dmeta::DocId;
use big_cabinet_core::dmeta::MultibaseKey;
use big_cabinet_core::dmeta::NativeReadBudget;
use big_cabinet_core::dmeta::NativeReadError;
use big_cabinet_core::dmeta::NativeValue;
use big_cabinet_core::dmeta::SignatureBytes;
use big_cabinet_core::dmeta::SignatureId;
use big_cabinet_core::dmeta::Uuid7;
use big_cabinet_core::dmeta::cid_of;
use big_cabinet_core::dmeta::encode_native;
use big_cabinet_core::doc::At;
use big_cabinet_core::doc::BranchDoc;
use big_cabinet_core::doc::CommandId;
use big_cabinet_core::doc::CreateDoc;
use big_cabinet_core::doc::DatumClaimEvidence;
use big_cabinet_core::doc::DatumClaimsInspection;
use big_cabinet_core::doc::DatumKinds;
use big_cabinet_core::doc::DatumListing;
use big_cabinet_core::doc::DatumRead;
use big_cabinet_core::doc::DatumRefSelector;
use big_cabinet_core::doc::DatumVersion;
use big_cabinet_core::doc::DatumsRead;
use big_cabinet_core::doc::DocAvailability;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::doc::DocsError;
use big_cabinet_core::doc::DocsMessage;
use big_cabinet_core::doc::DocsProtocol;
use big_cabinet_core::doc::EvidenceScope;
use big_cabinet_core::doc::GetDocAvailability;
use big_cabinet_core::doc::InspectDatumClaims;
use big_cabinet_core::doc::ListDatums;
use big_cabinet_core::doc::LocalDocUsability;
use big_cabinet_core::doc::MergeDoc;
use big_cabinet_core::doc::MintDocCapability;
use big_cabinet_core::doc::ObserveDocAvailability;
use big_cabinet_core::doc::PollDatums;
use big_cabinet_core::doc::QueriedDatumRef;
use big_cabinet_core::doc::QueriedDatumRefs;
use big_cabinet_core::doc::QueryDatumRefs;
use big_cabinet_core::doc::ReadDatums;
use big_cabinet_core::doc::SignatureEvidence;
use big_cabinet_core::doc::Unaccepted;
use big_cabinet_core::doc::UnsafeDatumInspection;
use big_cabinet_core::doc::UnsafeInspectDatum;
use big_cabinet_core::doc::VerifiedSignatureEvidence;
use big_cabinet_core::doc::WriteDatums;
use big_cabinet_core::doc::WriteMode;
use big_cabinet_core::doc::WriteReceipt;
use big_repo::BigDocHandle;
use big_repo::DocLookup;
use big_repo::SharedBigRepo;
use method_core::ContextError;
use method_core::EventStream;
use serde_json::Value as Json;
use service_client_native_rs::Client;
use service_client_native_rs::Transport;
use service_utils_rs::ClientError;
use service_utils_rs::Envelope;
use service_utils_rs::Handler;
use service_utils_rs::Host;
use service_utils_rs::HttpMetadata;
use service_utils_rs::RequestContext;
use service_utils_rs::ResponseEnvelope;
use service_utils_rs::Spawner;
use service_utils_rs::StopToken;
use sqlite_rs::Database;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

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
    pub sql: Database,
    /// The datum-set projection backing `PollDatums`, if the embedder runs one.
    /// The embedder owns `DatumSetStore::run`; DocsRepo only reads the committed
    /// projection. Without it, `PollDatums` is refused.
    pub datum_set: Option<Arc<DatumSetStore>>,
    /// The declared-reference index backing `QueryDatumRefs`, if the embedder
    /// runs one.
    pub datum_ref: Option<Arc<DatumRefStore>>,
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
    /// unless stealth, the same datum on the origin. Access is checked before either is written;
    /// signing them as the
    /// repository is what lets crash recovery finish a branch without the caller's capability.
    system_actor: Actor,
    node_issuer: MultibaseKey,
    raw: CompiledSchema,
    dmeta: DmetaSchema,
    branch: SchemaHandle,
    commands: Commands,
    datum_set: Option<Arc<DatumSetStore>>,
    datum_ref: Option<Arc<DatumRefStore>>,
    /// Spawns the service's long-lived per-subscription tasks (the
    /// `ObserveDocAvailability` pumps). Stopped after the host and the mint
    /// spawner, in reverse construction order.
    tasks: Spawner,
}

fn branch_key(branch: &DocId) -> DatumKey {
    format!("{}/{branch}", BranchPath::ID)
        .parse()
        .expect("a branch datum key is well formed")
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
        ClientError::Method(AuthorityServiceError::Context(error)) => error.into(),
        ClientError::Method(
            AuthorityServiceError::InvalidCapability
            | AuthorityServiceError::Unauthorized
            | AuthorityServiceError::UnrecognizedActor,
        ) => DocsError::Denied,
        ClientError::Method(error) => DocsError::Authority(error),
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

/// The refusal for a document commit big_repo rejected. A capability can authorize a write
/// this node cannot make: the operator is a cabinet superuser on every drawer it knows, but
/// keyhive enforces the node's own standing at commit time, and a drawer synced in from a peer
/// may hold only Read. That rejection ([`big_repo::NoWriteAccess`]) is the caller's write being
/// denied, and it never persisted, so it is `Some(Denied)`, a definite refusal. `None` for any
/// other failure: a repository fault whose outcome the caller cannot know.
fn commit_refusal(error: &utils_rs::prelude::eyre::Report) -> Option<DocsError> {
    error
        .downcast_ref::<big_repo::NoWriteAccess>()
        .map(|_| DocsError::Denied)
}

fn sorted(mut heads: Vec<ChangeHash>) -> Vec<ChangeHash> {
    heads.sort();
    heads.dedup();
    heads
}

/// Existence is read from change metadata: `get_change_by_hash` re-encodes the change by scanning
/// every operation in the document.
fn check_heads(doc: &Automerge, heads: &[ChangeHash]) -> Result<(), DocsError> {
    let unknown: Vec<_> = heads
        .iter()
        .copied()
        .filter(|head| doc.get_change_meta_by_hash(head).is_none())
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

/// Datums only the repository writes. The branch datums are readable like any other datum; dmeta
/// is not a signed datum and is never read through the gate.
fn is_system(key: &DatumKey) -> bool {
    [Dmeta::ID, BranchPath::ID].contains(&key.tag().as_str())
}

fn unaccepted(verdict: Verdict) -> Result<Vec<ChangeHash>, Unaccepted> {
    match verdict {
        Verdict::Accepted { datum_heads, .. } => Ok(datum_heads),
        Verdict::Pending { reason, .. } => Err(Unaccepted::Pending { reason }),
        Verdict::Rejected { reason } => Err(Unaccepted::Rejected { reason }),
    }
}

impl DocsService {
    /// The invocation of a request that signs: a create, write, branch or merge. The handle signs
    /// as its own subject unless the caller attached an `Authorization` whose `authority` is this
    /// request's handle, whose `signer` is then the handle the call signs through. That is how a
    /// service signs on behalf of its authenticated caller without being introduced to the
    /// caller's actor: the signature is exactly as wide as the handle the caller presented. Any
    /// other credential in the metadata (dispatch forwards its caller's) is the transport's, not a
    /// signer for this handle, and is not consulted.
    async fn signing_invocation(
        &self,
        context: &RequestContext,
        metadata: &HttpMetadata,
        capability: CapId,
    ) -> Result<Invocation<DocScope>, DocsError> {
        let signer = match Authorization::from_metadata(metadata) {
            Ok(authorization) if authorization.authority == capability => authorization.signer,
            _ => capability,
        };
        self.capabilities
            .resolve_signed(context, capability, signer)
            .await
            .map_err(authority_error)
    }

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

    /// The datum kinds `authority` reads of `document`, which it must already reach: a document
    /// named in `read`/`write` is held at those kinds and any other is reached through a
    /// drawer, at that drawer's kinds (see [`Self::reach`]).
    async fn authorize_read(
        &self,
        context: &RequestContext,
        authority: &DomainAuthority<DocScope>,
        document: &DocId,
    ) -> Result<DatumKinds, DocsError> {
        match authority {
            DomainAuthority::Identity(_) => {
                self.authorize(context, authority, AccessRequest::Read(document))
                    .await?;
                Ok(DatumKinds::All)
            }
            DomainAuthority::Scoped(_) => Self::reach(
                self.access.as_ref(),
                context,
                authority,
                &BTreeSet::from([document.clone()]),
                big_cabinet_core::drawer::DrawerAccess::Read,
            )
            .await?
            .remove(document)
            .ok_or(DocsError::Denied),
        }
    }

    async fn allowed(
        access_policy: &dyn DocAccess,
        context: &RequestContext,
        authority: &DomainAuthority<DocScope>,
        request: AccessRequest<'_>,
    ) -> Result<bool, DocsError> {
        // Every read, write and create names its document or drawer here, before the policy
        // derives a key from it: a request id of any other width is refused, not looked up.
        match &request {
            AccessRequest::Read(doc) | AccessRequest::Write(doc) => {
                DocsError::ensure_doc_id(doc)?;
            }
            AccessRequest::Create { placement } => {
                if let Some(drawer) = crate::drawer::parse_placement(placement) {
                    DocsError::ensure_doc_id(&drawer)?;
                }
            }
        }
        match authority {
            DomainAuthority::Identity(actor) => access_policy
                .check(context, *actor, request)
                .await
                .map_err(access_error),
            DomainAuthority::Scoped(scope) => match request {
                AccessRequest::Write(_) => unreachable!(
                    "writes go through authorize_write so datum kinds are enforced there"
                ),
                AccessRequest::Create { placement } => Ok(scope.create.contains(placement)
                    || crate::drawer::parse_placement(placement).is_some_and(|drawer| {
                        scope.drawers.get(&drawer).is_some_and(|reach| {
                            reach.access >= big_cabinet_core::drawer::DrawerAccess::Write
                                && !reach.write.admits_nothing()
                        })
                    })),
                AccessRequest::Read(doc) => Ok(Self::reach(
                    access_policy,
                    context,
                    authority,
                    &BTreeSet::from([doc.clone()]),
                    big_cabinet_core::drawer::DrawerAccess::Read,
                )
                .await?
                .contains_key(doc)),
            },
        }
    }

    /// The datum kinds `authority` may write into a document it creates at `placement`, which
    /// it must already be allowed to create at: a bare `create` placement is unnarrowed and a
    /// drawer placement carries that drawer's kinds.
    fn create_kinds(authority: &DomainAuthority<DocScope>, placement: &str) -> DatumKinds {
        let DomainAuthority::Scoped(scope) = authority else {
            return DatumKinds::All;
        };
        if scope.create.contains(placement) {
            return DatumKinds::All;
        }
        crate::drawer::parse_placement(placement)
            .and_then(|drawer| scope.drawers.get(&drawer))
            .map_or(DatumKinds::All, |reach| reach.write.clone())
    }

    /// The documents of `documents` that `authority` reaches for `needed`, each with the datum
    /// kinds it reaches it at. A document named in `write` (or, for a read, `read`) is held at
    /// that entry's kinds and is not looked up in drawers; any other is reached through the
    /// granted drawers that hold it, at the union of those drawers' kinds. This is the one place
    /// datum kinds are resolved for every read and write.
    async fn reach(
        access_policy: &dyn DocAccess,
        context: &RequestContext,
        authority: &DomainAuthority<DocScope>,
        documents: &BTreeSet<DocId>,
        needed: big_cabinet_core::drawer::DrawerAccess,
    ) -> Result<BTreeMap<DocId, DatumKinds>, DocsError> {
        use big_cabinet_core::drawer::DrawerAccess;
        documents.iter().try_for_each(DocsError::ensure_doc_id)?;
        let DomainAuthority::Scoped(scope) = authority else {
            return Err(DocsError::Denied);
        };
        let mut reached = BTreeMap::new();
        let mut candidates = BTreeSet::new();
        for doc in documents {
            let direct = scope.write.get(doc).or_else(|| {
                (needed == DrawerAccess::Read)
                    .then(|| scope.read.get(doc))
                    .flatten()
            });
            match direct {
                Some(kinds) => {
                    reached.insert(doc.clone(), kinds.clone());
                }
                None => {
                    candidates.insert(doc.clone());
                }
            }
        }
        if !candidates.is_empty() {
            reached.extend(
                Self::reach_drawers(access_policy, context, &scope.drawers, &candidates, needed)
                    .await?,
            );
        }
        Ok(reached)
    }

    /// [`Self::reach`] through `drawers` alone. Drawers granting equal kinds share one query.
    async fn reach_drawers(
        access_policy: &dyn DocAccess,
        context: &RequestContext,
        drawers: &BTreeMap<DocId, big_cabinet_core::doc::DrawerReach>,
        candidates: &BTreeSet<DocId>,
        needed: big_cabinet_core::drawer::DrawerAccess,
    ) -> Result<BTreeMap<DocId, DatumKinds>, DocsError> {
        let mut groups: Vec<(
            &DatumKinds,
            BTreeMap<DocId, big_cabinet_core::drawer::DrawerAccess>,
        )> = Vec::new();
        // The kinds a drawer reaches documents at depend on the need: a write reaches only its
        // write kinds, a read the kinds it reads. A drawer that admits no kind for the need
        // reaches nothing.
        let is_read = needed == big_cabinet_core::drawer::DrawerAccess::Read;
        for (drawer, reach) in drawers {
            let kinds = if is_read {
                reach.readable()
            } else {
                &reach.write
            };
            if reach.access < needed || kinds.admits_nothing() {
                continue;
            }
            match groups.iter_mut().find(|(group, _)| **group == *kinds) {
                Some((_, group)) => {
                    group.insert(drawer.clone(), reach.access);
                }
                None => groups.push((kinds, BTreeMap::from([(drawer.clone(), reach.access)]))),
            }
        }
        let mut reached: BTreeMap<DocId, DatumKinds> = BTreeMap::new();
        for (kinds, group) in groups {
            for (doc, held) in access_policy
                .scoped_docs(context, &group, Some(candidates))
                .await
                .map_err(access_error)?
            {
                if held < needed {
                    continue;
                }
                match reached.entry(doc) {
                    std::collections::btree_map::Entry::Vacant(slot) => {
                        slot.insert(kinds.clone());
                    }
                    std::collections::btree_map::Entry::Occupied(mut slot) => {
                        let merged = slot.get().clone().union(kinds.clone());
                        *slot.get_mut() = merged;
                    }
                }
            }
        }
        Ok(reached)
    }

    async fn authorize_write(
        &self,
        context: &RequestContext,
        authority: &DomainAuthority<DocScope>,
        document: &DocId,
    ) -> Result<DatumKinds, DocsError> {
        DocsError::ensure_doc_id(document)?;
        match authority {
            DomainAuthority::Identity(actor) => {
                if self
                    .access
                    .check(context, *actor, AccessRequest::Write(document))
                    .await
                    .map_err(access_error)?
                {
                    Ok(DatumKinds::All)
                } else {
                    Err(DocsError::Denied)
                }
            }
            DomainAuthority::Scoped(_) => Self::reach(
                self.access.as_ref(),
                context,
                authority,
                &BTreeSet::from([document.clone()]),
                big_cabinet_core::drawer::DrawerAccess::Write,
            )
            .await?
            .remove(document)
            .ok_or(DocsError::Denied),
        }
    }

    fn require_all(kinds: DatumKinds) -> Result<(), DocsError> {
        if matches!(kinds, DatumKinds::All) {
            Ok(())
        } else {
            Err(DocsError::Denied)
        }
    }

    async fn authorize(
        &self,
        context: &RequestContext,
        authority: &DomainAuthority<DocScope>,
        request: AccessRequest<'_>,
    ) -> Result<(), DocsError> {
        if let AccessRequest::Write(document) = request {
            self.authorize_write(context, authority, document)
                .await
                .and_then(Self::require_all)
        } else if Self::allowed(self.access.as_ref(), context, authority, request).await? {
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

fn present_keys(doc: &Automerge, heads: &[ChangeHash]) -> Result<BTreeSet<DatumKey>, DocsError> {
    let Some((Value::Object(ObjType::Map), datums)) = doc
        .get_at(automerge::ROOT, "datums", heads)
        .map_err(repository)?
    else {
        return Ok(BTreeSet::new());
    };
    Ok(doc
        .map_range_at(&datums, .., heads)
        .filter(|item| item.key != DMETA_KEY)
        .filter_map(|item| item.key.parse().ok())
        .collect())
}

service_utils_rs::bind_service_protocol! {
    pub struct DocsBinding for DocsProtocol => DocsMessage {
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
        subject: Uuid7,
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
                        .source::<DocId>(&context, Authorization::identity(service.issuer))
                        .await
                        .map_err(authority_error)?;
                    service
                        .inspection_capabilities
                        .mint(
                            &context,
                            &caller,
                            Some(subject),
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
            ResolveAuthority {
                capability: config.issuer,
            },
        )
        .await
        .map_err(authority_error)?
        .actor
        .ok_or_else(|| {
            authority_error(ClientError::Method(
                AuthorityServiceError::UnrecognizedActor,
            ))
        })?;
    let core = |tag: &str| {
        config
            .schema_repo
            .resolve_for_read(tag)
            .expect("SchemaRepo always registers the core datums")
    };
    let (dmeta, branch) = (core(Dmeta::ID), core(BranchPath::ID));
    let (service_stop, service_spawner) = StopToken::new();
    let service = Arc::new(DocsService {
        big_repo: config.big_repo,
        schema_repo: config.schema_repo,
        capabilities: DomainCapabilities::new(
            config.authority.clone(),
            config.namespace,
            "DocsRepo/scope/v3/",
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
        dmeta: DmetaSchema::new(dmeta.compiled().clone())
            .expect("dmeta's member declarations compile on their own"),
        branch,
        commands: Commands::open(config.sql).await?,
        datum_set: config.datum_set,
        datum_ref: config.datum_ref,
        tasks: service_spawner,
    });
    let (mint_stop, mint_spawner) = StopToken::new();
    let (host, host_stop) = Host::new(Arc::clone(&service));
    let stop = host_stop
        .then(mint_stop)
        .then(service_stop)
        .then(service.capabilities.stop_token());
    let client = Client::new(Transport::direct(DocsBinding::new(Arc::clone(&host))));
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
        if_datums_unchanged: (
            WriteMode::IfDatumsUnchanged { datums: BTreeMap::from([(DatumKey::from_text("note", "a").expect("datum key"), DatumVersion { heads: vec![ChangeHash([3; 32])], clock: 7 })]) },
            serde_json::json!({"mode": "ifDatumsUnchanged", "datums": {"note/a": {"heads": [ChangeHash([3; 32])], "clock": 7}}})
        ),
        at_heads: (
            WriteMode::AtHeads { heads: vec![ChangeHash([4; 32])] },
            serde_json::json!({"mode": "atHeads", "heads": [ChangeHash([4; 32])]})
        ),
    }
}
