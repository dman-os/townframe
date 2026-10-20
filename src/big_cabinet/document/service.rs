use super::access::{AccessError, AccessRequest, DocumentAccess};
use super::claim_history::predecessor_ids;
use super::commands::{Commands, Detail, Kind, Row, State};
use super::gate::{DMETA_KEY, Gate, Verdict, document_actor, signature_heads};
use super::protocol::{
    At, BranchDocument, CommandId, CreateDocument, DocumentsError, DocumentsProtocol,
    EvidenceScope, FacetClaimEvidence, FacetClaimsInspection, FacetListing, FacetRead, FacetsRead,
    InspectFacetClaims, ListFacets, MergeDocument, ReadFacets, SignatureEvidence, Unaccepted,
    UnsafeFacetInspection, UnsafeInspectFacet, VerifiedSignatureEvidence, WriteFacets, WriteMode,
    WriteReceipt,
};
use super::{
    ClaimHistoryEntry, ClaimHistoryError, ClaimHistoryResult, HistoryBudget,
    reconstruct_current_claims,
};
use crate::authority::{
    Actor, AuthorityProtocol, AuthorityServiceError, CapId, Capability, InspectCapability,
    MintCapability, ResolveActor, Rights, SignFacetClaim,
};
use crate::schema::{SchemaHandle, SchemaRepo};
use automerge::transaction::{CommitOptions, Transactable, Transaction};
use automerge::{Automerge, ChangeHash, ObjId, ObjType, PatchLog, ReadDoc, Value};
use big_cabinet_core::FacetKey;
use big_cabinet_core::dmeta::{
    ActorMetadata, BRANCH_FACET_TAG, BRANCHES_FACET_TAG, BranchesPath, Cid, ClaimOperation,
    DMETA_FACET_TAG, Dmeta, DocumentId, FacetClaim, FacetMetadata, FacetSignature, MultibaseKey,
    NativeReadBudget, NativeValue, SignatureBytes, Uuid7, cid_of, encode_native,
};
use big_cabinet_core::facet::{CompiledSchema, hydrate_value, reconcile_json_atomic};
use big_repo::{BigDocHandle, DocLookup, SharedBigRepo};
use serde_json::Value as Json;
use service_utils_rs::{
    Client, ClientError, ContextError, Envelope, Handler, Host, RequestContext, ResponseEnvelope,
    Spawner, StopToken,
};
use sqlx_utils_rs::SqlCtx;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// Everything DocumentsRepo is booted with.
pub struct DocumentsConfig {
    pub big_repo: SharedBigRepo,
    pub schema_repo: Arc<SchemaRepo>,
    pub authority: Client<AuthorityProtocol>,
    /// DocumentsRepo's own actor capability: it names the objects it mints and checks.
    pub issuer: CapId,
    /// This node's attribution issuer. Every change DocumentsRepo makes is authored under this key
    /// as its Automerge actor, which is what lets a verifier find the expected issuer of the
    /// attribution certificates the change carries.
    pub node_issuer: MultibaseKey,
    pub access: Arc<dyn DocumentAccess>,
    pub sql: SqlCtx,
}

pub struct DocumentsService {
    big_repo: SharedBigRepo,
    schema_repo: Arc<SchemaRepo>,
    authority: Client<AuthorityProtocol>,
    access: Arc<dyn DocumentAccess>,
    issuer: CapId,
    /// DocumentsRepo's own actor, which signs the facets only it writes: a branch's
    /// `org.example.daybook.branch/<id>` facet and an origin's `org.example.daybook.branches/main`
    /// backlink. The caller's access is checked before either is written; signing them as the
    /// repository is what lets crash recovery finish a branch without the caller's capability.
    system_actor: Actor,
    node_issuer: MultibaseKey,
    raw: CompiledSchema,
    dmeta: SchemaHandle,
    branch: SchemaHandle,
    branches: SchemaHandle,
    commands: Commands,
}

const BRANCHES_KEY: &str = "org.example.daybook.branches/main";

fn branch_key(branch: &DocumentId) -> FacetKey {
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

fn inspection_object(document_id: &DocumentId) -> String {
    format!("DocumentsRepo/UNSAFE/inspection/{document_id}")
}

fn authority_error(error: ClientError<AuthorityServiceError>) -> DocumentsError {
    match error {
        ClientError::Operation(AuthorityServiceError::Context(error)) => error.into(),
        ClientError::Operation(
            AuthorityServiceError::InvalidCapability
            | AuthorityServiceError::Unauthorized
            | AuthorityServiceError::UnrecognizedActor,
        ) => DocumentsError::Denied,
        ClientError::Operation(error) => DocumentsError::Authority(error),
        ClientError::Transport { message, .. } => DocumentsError::AuthorityUnavailable(message),
    }
}

fn access_error(error: AccessError) -> DocumentsError {
    DocumentsError::Access(error.to_string())
}

fn repository(error: impl std::fmt::Display) -> DocumentsError {
    DocumentsError::Repository(error.to_string())
}

fn sorted(mut heads: Vec<ChangeHash>) -> Vec<ChangeHash> {
    heads.sort();
    heads.dedup();
    heads
}

fn check_heads(doc: &Automerge, heads: &[ChangeHash]) -> Result<(), DocumentsError> {
    let unknown: Vec<_> = heads
        .iter()
        .copied()
        .filter(|head| doc.get_change_by_hash(head).is_none())
        .collect();
    if !unknown.is_empty() {
        return Err(DocumentsError::UnknownHeads { heads: unknown });
    }
    Ok(())
}

fn resolve(doc: &Automerge, at: &At) -> Result<Vec<ChangeHash>, DocumentsError> {
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
    [DMETA_FACET_TAG, BRANCH_FACET_TAG, BRANCHES_FACET_TAG].contains(&key.tag().as_str())
}

fn unaccepted(verdict: Verdict) -> Result<Vec<ChangeHash>, Unaccepted> {
    match verdict {
        Verdict::Accepted { facet_heads, .. } => Ok(facet_heads),
        Verdict::Pending { reason, .. } => Err(Unaccepted::Pending { reason }),
        Verdict::Rejected { reason } => Err(Unaccepted::Rejected { reason }),
    }
}

/// What a write intends, fixed before its first transaction so that the transaction it signs and
/// the transaction it commits are built from identical inputs.
struct Plan<'a> {
    document: &'a DocumentId,
    /// The `docUuid` bound to the document if it has none yet; see [`Dmeta::document_uuid`].
    document_uuid: Uuid7,
    actor: &'a Actor,
    set: BTreeMap<FacetKey, (SchemaHandle, Json)>,
    remove: BTreeSet<FacetKey>,
    now: jiff::Timestamp,
    predecessors: BTreeMap<FacetKey, Vec<Cid>>,
    /// Aliases minted for slots that had none when the plan was first staged.
    aliases: BTreeMap<FacetKey, Uuid7>,
    /// A branch to record in this document's `org.example.daybook.branches/main`. Its value is
    /// the one at the write's base with this branch added, so it is read inside [`stage`]: both
    /// stagings of the plan see the same base and so produce the same value.
    ///
    /// [`stage`]: DocumentsService::stage
    backlink: Option<Backlink>,
}

struct Backlink {
    key: FacetKey,
    branch: DocumentId,
    path: String,
}

impl Plan<'_> {
    fn keys(&self) -> impl Iterator<Item = &FacetKey> {
        self.set
            .keys()
            .chain(self.remove.iter())
            .chain(self.backlink.iter().map(|backlink| &backlink.key))
    }
}

/// The result of a write that failed. `Ambiguous` means the change may already be committed in
/// memory while its persistence failed: its command row must survive for the boot pass to settle.
enum Failure {
    Refused(DocumentsError),
    Ambiguous(DocumentsError),
}

impl From<DocumentsError> for Failure {
    fn from(error: DocumentsError) -> Self {
        Self::Refused(error)
    }
}

impl Failure {
    fn into_error(self) -> DocumentsError {
        match self {
            Self::Refused(error) | Self::Ambiguous(error) => error,
        }
    }
}

impl DocumentsService {
    async fn actor(
        &self,
        context: &RequestContext,
        capability: CapId,
    ) -> Result<Actor, DocumentsError> {
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
    ) -> Result<(), DocumentsError> {
        if self
            .access
            .check(context, actor.uuid, request)
            .await
            .map_err(access_error)?
        {
            Ok(())
        } else {
            Err(DocumentsError::Denied)
        }
    }

    async fn handle(&self, document_id: &DocumentId) -> Result<BigDocHandle, DocumentsError> {
        match self
            .big_repo
            .get_doc(document_id)
            .await
            .map_err(repository)?
        {
            DocLookup::Missing => Err(DocumentsError::DocumentNotFound),
            DocLookup::PendingMaterialization => Err(DocumentsError::Pending {
                reason: "document materialization".into(),
            }),
            DocLookup::Ready(document) => Ok(document),
        }
    }

    /// The receipt of a completed command, `None` for an unknown one, and a refusal for a command
    /// still in flight or reused for a different request.
    async fn replay(
        &self,
        command: &CommandId,
        digest: [u8; 32],
    ) -> Result<Option<WriteReceipt>, DocumentsError> {
        let Some(row) = self.commands.get(command).await? else {
            return Ok(None);
        };
        if row.digest != digest {
            return Err(DocumentsError::CommandReused {
                command: command.clone(),
            });
        }
        match row.state {
            State::Done => Ok(Some(WriteReceipt {
                document_id: row.document.expect("a done command names its document"),
                heads: row.heads,
            })),
            State::Pending | State::Committed => Err(DocumentsError::CommandInFlight {
                command: command.clone(),
            }),
        }
    }

    fn schemas(
        &self,
        set: BTreeMap<FacetKey, Json>,
        remove: &BTreeSet<FacetKey>,
    ) -> Result<BTreeMap<FacetKey, (SchemaHandle, Json)>, DocumentsError> {
        for key in set.keys().chain(remove.iter()) {
            if is_system(key) {
                return Err(DocumentsError::SystemFacet {
                    facet_key: key.clone(),
                });
            }
            if set.contains_key(key) && remove.contains(key) {
                return Err(DocumentsError::SetAndRemoved {
                    facet_key: key.clone(),
                });
            }
        }
        set.into_iter()
            .map(|(key, value)| {
                let schema = self
                    .schema_repo
                    .require_for_write(key.tag().as_str())
                    .map_err(|_| DocumentsError::UnknownSchema {
                        facet_tag: key.tag().to_string(),
                    })?;
                Ok((key, (schema, value)))
            })
            .collect()
    }

    /// Apply a plan to a transaction and derive the claim each touched facet's signature covers.
    ///
    /// The value a claim covers is read back from the transaction after the content mutation, not
    /// taken from the caller's JSON, and the alias it names is the smallest one visible in the
    /// transaction. `signatures: None` stages with placeholder signatures so that every refusal —
    /// including dmeta's — surfaces before anything is signed; the caller then rolls back.
    fn stage<'t>(
        &self,
        mut tx: Transaction<'t>,
        plan: &mut Plan<'_>,
        signatures: Option<&BTreeMap<FacetKey, SignatureBytes>>,
    ) -> Result<(Transaction<'t>, BTreeMap<FacetKey, FacetClaim>), DocumentsError> {
        let facets = match tx.get(automerge::ROOT, "facets").map_err(repository)? {
            Some((Value::Object(ObjType::Map), facets)) => facets,
            Some(_) => {
                return Err(DocumentsError::Repository("facets is not a map".into()));
            }
            None => tx
                .put_object(automerge::ROOT, "facets", ObjType::Map)
                .map_err(repository)?,
        };
        let mut values: Vec<(&FacetKey, &CompiledSchema, Json)> = plan
            .set
            .iter()
            .map(|(key, (schema, value))| (key, schema.compiled(), value.clone()))
            .collect();
        if let Some(backlink) = &plan.backlink {
            let mut branches = match tx.get(&facets, BRANCHES_KEY).map_err(repository)? {
                None => BranchesPath::default(),
                Some(_) => serde_json::from_value(
                    hydrate_value(&tx, &facets, BRANCHES_KEY.into(), self.branches.compiled())
                        .map_err(repository)?,
                )
                .map_err(repository)?,
            };
            branches.record(backlink.branch.clone(), backlink.path.clone());
            values.push((
                &backlink.key,
                self.branches.compiled(),
                serde_json::to_value(branches).expect("a branch list serializes"),
            ));
        }
        for (key, schema, value) in values {
            tx = reconcile_json_atomic(tx, schema, &facets, key.to_string().as_str().into(), value)
                .map_err(|error| DocumentsError::InvalidValue {
                    facet_key: key.clone(),
                    reason: error.to_string(),
                })?;
        }
        for key in &plan.remove {
            if tx
                .get(&facets, key.to_string())
                .map_err(repository)?
                .is_some()
            {
                tx.delete(&facets, key.to_string()).map_err(repository)?;
            }
        }
        let mut dmeta = match tx.get(&facets, DMETA_KEY).map_err(repository)? {
            None => Dmeta::new(plan.now),
            Some(_) => {
                let json = hydrate_value(&tx, &facets, DMETA_KEY.into(), self.dmeta.compiled())
                    .map_err(repository)?;
                serde_json::from_value::<Dmeta>(json).map_err(repository)?
            }
        };
        let document_uuid = dmeta
            .document_uuid(plan.document, plan.document_uuid)
            .expect("a freshly minted document UUID is unbound");
        let mut claims = BTreeMap::new();
        let keys: Vec<FacetKey> = plan.keys().cloned().collect();
        for key in keys {
            let visible = dmeta
                .facet_uuids
                .iter()
                .filter(|(_, alias)| **alias == key)
                .map(|(uuid, _)| *uuid)
                .min();
            let chosen = match visible {
                Some(chosen) => chosen,
                None => {
                    let alias = *plan.aliases.entry(key.clone()).or_default();
                    dmeta
                        .record_facet_alias(alias, key.clone())
                        .expect("a freshly minted alias is unbound");
                    alias
                }
            };
            let native = NativeValue::read_at(&tx, &facets, key.to_string()).map_err(|error| {
                DocumentsError::InvalidValue {
                    facet_key: key.clone(),
                    reason: format!("{error:?}"),
                }
            })?;
            let removed = native.is_none();
            claims.insert(
                key.clone(),
                FacetClaim {
                    origin_document: plan.document.clone(),
                    facet_key: key.clone(),
                    chosen_uuid: chosen,
                    actor_uuid: plan.actor.uuid,
                    operation: if removed {
                        ClaimOperation::Remove
                    } else {
                        ClaimOperation::Set
                    },
                    value_cid: native.map(|native| cid_of(&encode_native(&native))),
                    predecessors: plan.predecessors.get(&key).cloned().unwrap_or_default(),
                },
            );
            let signature = match signatures {
                Some(signatures) => signatures[&key].clone(),
                None => SignatureBytes::try_from([0_u8; 64].as_slice())
                    .expect("64 bytes are a signature"),
            };
            dmeta.facets.insert(
                key,
                FacetMetadata {
                    updated_at: vec![plan.now],
                    deleted_at: if removed { vec![plan.now] } else { Vec::new() },
                    signatures: vec![FacetSignature {
                        actor_uuid: plan.actor.uuid,
                        signature,
                        doc_uuid: document_uuid,
                        message: None,
                        inherited: None,
                    }],
                },
            );
        }
        dmeta
            .actors
            .entry(plan.actor.uuid)
            .or_insert_with(|| ActorMetadata {
                pubkey: plan.actor.public_key.clone(),
                actor_url: plan.actor.url.clone(),
                delegation: Some(plan.actor.certificate.clone()),
            });
        dmeta.updated_at = vec![plan.now];
        let tx = dmeta
            .reconcile_atomic(
                tx,
                self.dmeta.compiled(),
                &facets,
                DMETA_KEY.into(),
                &plan.keys().cloned().collect(),
            )
            .map_err(repository)?;
        Ok((tx, claims))
    }

    async fn sign(
        &self,
        context: &RequestContext,
        capability: CapId,
        claims: &BTreeMap<FacetKey, FacetClaim>,
    ) -> Result<BTreeMap<FacetKey, SignatureBytes>, DocumentsError> {
        let mut signatures = BTreeMap::new();
        for (key, claim) in claims {
            let signed = self
                .authority
                .call(
                    context.child(),
                    SignFacetClaim {
                        actor: capability,
                        claim: claim.clone(),
                    },
                )
                .await
                .map_err(authority_error)?;
            assert_eq!(
                &signed.claim, claim,
                "the signer signs the claim it was given"
            );
            signatures.insert(key.clone(), signed.signature);
        }
        Ok(signatures)
    }

    /// The facet heads of every named facet at `heads`, verified: a facet that does not verify
    /// cannot match a condition taken from an earlier read.
    fn facet_heads(
        doc: &Automerge,
        heads: &[ChangeHash],
        keys: impl Iterator<Item = FacetKey>,
    ) -> BTreeMap<FacetKey, Vec<ChangeHash>> {
        let gate = Gate::new(doc, heads);
        keys.map(|key| {
            let heads = match gate.verify(&key) {
                Verdict::Accepted { facet_heads, .. } | Verdict::Pending { facet_heads, .. } => {
                    facet_heads
                }
                Verdict::Rejected { .. } => Vec::new(),
            };
            (key, heads)
        })
        .collect()
    }

    /// Check a write's condition against the document as it is now and return the base to write
    /// from. Run once when the write is staged and again, under the same lock as the commit, when
    /// it is committed.
    fn condition(doc: &Automerge, mode: &WriteMode) -> Result<Vec<ChangeHash>, DocumentsError> {
        let current = sorted(doc.get_heads());
        match mode {
            WriteMode::IfDocumentUnchanged { heads } => {
                check_heads(doc, heads)?;
                if sorted(heads.clone()) != current {
                    return Err(DocumentsError::Conflict {
                        heads: current,
                        facets: BTreeMap::new(),
                    });
                }
                Ok(current)
            }
            WriteMode::IfFacetsUnchanged { facets } => {
                let actual = Self::facet_heads(doc, &current, facets.keys().cloned());
                let expected: BTreeMap<_, _> = facets
                    .iter()
                    .map(|(key, heads)| (key.clone(), sorted(heads.clone())))
                    .collect();
                if actual != expected {
                    return Err(DocumentsError::Conflict {
                        heads: current,
                        facets: actual,
                    });
                }
                Ok(current)
            }
            WriteMode::AtHeads { heads } => {
                check_heads(doc, heads)?;
                Ok(sorted(heads.clone()))
            }
            WriteMode::AtCurrent => Ok(current),
        }
    }
}

fn digest(value: &impl serde::Serialize) -> [u8; 32] {
    *blake3::hash(&serde_json::to_vec(value).expect("a request serializes")).as_bytes()
}

impl DocumentsService {
    async fn write(
        &self,
        context: &RequestContext,
        params: WriteFacets,
    ) -> Result<WriteReceipt, DocumentsError> {
        let actor = self.actor(context, params.capability).await?;
        self.authorize(context, &actor, AccessRequest::Write(&params.document_id))
            .await?;
        let request_digest = digest(&(
            "write",
            actor.uuid,
            &params.document_id,
            &params.mode,
            &params.set,
            &params.remove,
        ));
        if let Some(command) = &params.command
            && let Some(receipt) = self.replay(command, request_digest).await?
        {
            return Ok(receipt);
        }
        let set = self.schemas(params.set, &params.remove)?;
        if let Some(command) = &params.command {
            self.commands
                .begin(
                    command,
                    Kind::Write,
                    request_digest,
                    actor.uuid,
                    Some(&params.document_id),
                    None,
                    None,
                )
                .await?;
        }
        let mut plan = Plan {
            document: &params.document_id,
            document_uuid: Uuid7::new(),
            actor: &actor,
            set,
            remove: params.remove,
            now: jiff::Timestamp::now(),
            predecessors: BTreeMap::new(),
            aliases: BTreeMap::new(),
            backlink: None,
        };
        let result = self
            .commit_write(
                context,
                params.capability,
                &params.mode,
                &mut plan,
                params.command.as_ref(),
            )
            .await;
        match (&params.command, result) {
            (Some(command), Ok(heads)) => {
                self.commands.advance(command, State::Done, &heads).await?;
                Ok(WriteReceipt {
                    document_id: params.document_id,
                    heads,
                })
            }
            (None, Ok(heads)) => Ok(WriteReceipt {
                document_id: params.document_id,
                heads,
            }),
            (Some(command), Err(Failure::Refused(error))) => {
                self.commands.discard(command).await?;
                Err(error)
            }
            (_, Err(failure)) => Err(failure.into_error()),
        }
    }

    async fn commit_write(
        &self,
        context: &RequestContext,
        capability: CapId,
        mode: &WriteMode,
        plan: &mut Plan<'_>,
        command: Option<&CommandId>,
    ) -> Result<Vec<ChangeHash>, Failure> {
        let document = self.handle(plan.document).await?;
        let staged = document
            .with_document(|doc| -> Result<_, DocumentsError> {
                let base = Self::condition(doc, mode)?;
                let gate = Gate::new(doc, &base);
                for key in plan.keys() {
                    unaccepted(gate.verify(key)).map_err(|verdict| {
                        DocumentsError::BaseNotAccepted {
                            facet_key: key.clone(),
                            verdict,
                        }
                    })?;
                }
                let mut predecessors = BTreeMap::new();
                for key in plan.keys() {
                    let ids = predecessor_ids(doc, key, &base, HistoryBudget::default()).map_err(
                        |error| DocumentsError::BaseNotAccepted {
                            facet_key: key.clone(),
                            verdict: Unaccepted::Pending {
                                reason: format!("predecessors: {error:?}"),
                            },
                        },
                    )?;
                    predecessors.insert(key.clone(), ids);
                }
                plan.predecessors = predecessors;
                let tx = doc
                    .transaction_at(PatchLog::null(), &base)
                    .expect("a null patch log matches any document");
                let (tx, claims) = self.stage(tx, plan, None)?;
                tx.rollback();
                Ok((base, claims))
            })
            .await
            .map_err(repository)??;
        let (base, claims) = staged;
        let signatures = self.sign(context, capability, &claims).await?;
        let committed = document
            .with_document(|doc| -> Result<_, DocumentsError> {
                Self::condition(doc, mode)?;
                doc.set_actor(document_actor(&self.node_issuer, plan.document));
                let tx = doc
                    .transaction_at(PatchLog::null(), &base)
                    .expect("a null patch log matches any document");
                let (tx, signed) = self.stage(tx, plan, Some(&signatures))?;
                assert_eq!(signed, claims, "a plan stages the same claims twice");
                let options = match command {
                    Some(command) => CommitOptions::default().with_message(command.as_str()),
                    None => CommitOptions::default(),
                };
                tx.commit_with(options);
                Ok(sorted(doc.get_heads()))
            })
            .await
            .map_err(|error| Failure::Ambiguous(repository(error)))?;
        Ok(committed?)
    }

    async fn create(
        &self,
        context: &RequestContext,
        params: CreateDocument,
    ) -> Result<WriteReceipt, DocumentsError> {
        let actor = self.actor(context, params.capability).await?;
        self.authorize(
            context,
            &actor,
            AccessRequest::Create {
                placement: &params.placement,
            },
        )
        .await?;
        let request_digest = digest(&("create", actor.uuid, &params.placement, &params.facets));
        if let Some(receipt) = self.replay(&params.command, request_digest).await? {
            return Ok(receipt);
        }
        let set = self.schemas(params.facets, &BTreeSet::new())?;
        self.commands
            .begin(
                &params.command,
                Kind::Create,
                request_digest,
                actor.uuid,
                None,
                Some(&params.placement),
                None,
            )
            .await?;
        let document_id = self.big_repo.allocate_id().await.map_err(repository)?;
        self.commands
            .set_document(&params.command, &document_id)
            .await?;
        let mut plan = Plan {
            document: &document_id,
            document_uuid: Uuid7::new(),
            actor: &actor,
            set,
            remove: BTreeSet::new(),
            now: jiff::Timestamp::now(),
            predecessors: BTreeMap::new(),
            aliases: BTreeMap::new(),
            backlink: None,
        };
        let initial = match self
            .initial_content(context, params.capability, &mut plan, Automerge::new())
            .await
        {
            Ok(initial) => initial,
            Err(error) => {
                self.big_repo
                    .abandon_allocation(document_id.clone())
                    .await
                    .map_err(repository)?;
                self.commands.discard(&params.command).await?;
                return Err(error);
            }
        };
        let document = self
            .big_repo
            .commit_id(document_id.clone(), initial, Vec::new(), &[])
            .await
            .map_err(repository)?;
        let heads = sorted(document.with_document_read(|doc| doc.get_heads()).await);
        self.settle_create(
            context,
            &params.command,
            actor.uuid,
            &document_id,
            &params.placement,
            &heads,
        )
        .await?;
        Ok(WriteReceipt { document_id, heads })
    }

    /// Persisted identity and content are acknowledged only once the policy has placed the
    /// document; a crash in between replays the placement from the committed row.
    async fn settle_create(
        &self,
        context: &RequestContext,
        command: &CommandId,
        actor: Uuid7,
        document: &DocumentId,
        placement: &str,
        heads: &[ChangeHash],
    ) -> Result<(), DocumentsError> {
        self.commands
            .advance(command, State::Committed, heads)
            .await?;
        self.access
            .place(context, actor, document, placement)
            .await
            .map_err(access_error)?;
        self.commands.advance(command, State::Done, heads).await
    }

    /// Stage, sign and commit a plan's change on top of `doc`, a document that is not yet in BigRepo:
    /// empty for a create, the origin's forked changes for a branch.
    async fn initial_content(
        &self,
        context: &RequestContext,
        capability: CapId,
        plan: &mut Plan<'_>,
        mut doc: Automerge,
    ) -> Result<Automerge, DocumentsError> {
        doc.set_actor(document_actor(&self.node_issuer, plan.document));
        let tx = doc.transaction();
        let (tx, claims) = self.stage(tx, plan, None)?;
        tx.rollback();
        let signatures = self.sign(context, capability, &claims).await?;
        let tx = doc.transaction();
        let (tx, signed) = self.stage(tx, plan, Some(&signatures))?;
        assert_eq!(signed, claims, "a plan stages the same claims twice");
        tx.commit();
        Ok(doc)
    }
}

impl DocumentsService {
    async fn branch(
        &self,
        context: &RequestContext,
        params: BranchDocument,
    ) -> Result<WriteReceipt, DocumentsError> {
        let actor = self.actor(context, params.capability).await?;
        self.authorize(context, &actor, AccessRequest::Read(&params.document_id))
            .await?;
        if !params.stealth {
            self.authorize(context, &actor, AccessRequest::Write(&params.document_id))
                .await?;
        }
        self.authorize(
            context,
            &actor,
            AccessRequest::Create {
                placement: &params.placement,
            },
        )
        .await?;
        let request_digest = digest(&(
            "branch",
            actor.uuid,
            &params.document_id,
            &params.at,
            &params.path,
            params.stealth,
            &params.placement,
        ));
        if let Some(receipt) = self.replay(&params.command, request_digest).await? {
            return Ok(receipt);
        }
        let origin = self.handle(&params.document_id).await?;
        let fork = origin
            .with_document_read(|doc| -> Result<Automerge, DocumentsError> {
                context.validate()?;
                let heads = resolve(doc, &params.at)?;
                if !params.stealth {
                    // The backlink is written on the origin after the branch exists, and a branch
                    // cannot be rolled back; refuse up front what would refuse it then.
                    let current = sorted(doc.get_heads());
                    unaccepted(
                        Gate::new(doc, &current).verify(
                            &BRANCHES_KEY
                                .parse()
                                .expect("the branches key is well formed"),
                        ),
                    )
                    .map_err(|verdict| DocumentsError::BaseNotAccepted {
                        facet_key: BRANCHES_KEY
                            .parse()
                            .expect("the branches key is well formed"),
                        verdict,
                    })?;
                }
                doc.fork_at(&heads).map_err(repository)
            })
            .await?;
        let detail = Detail::Branch {
            origin: params.document_id.clone(),
            path: params.path.clone(),
            stealth: params.stealth,
        };
        self.commands
            .begin(
                &params.command,
                Kind::Branch,
                request_digest,
                actor.uuid,
                None,
                Some(&params.placement),
                Some(&detail),
            )
            .await?;
        let branch = self.big_repo.allocate_id().await.map_err(repository)?;
        self.commands.set_document(&params.command, &branch).await?;
        let mut plan = Plan {
            document: &branch,
            document_uuid: Uuid7::new(),
            actor: &self.system_actor,
            set: BTreeMap::from([(
                branch_key(&branch),
                (
                    self.branch.clone(),
                    serde_json::json!({ "path": params.path }),
                ),
            )]),
            remove: BTreeSet::new(),
            now: jiff::Timestamp::now(),
            predecessors: BTreeMap::new(),
            aliases: BTreeMap::new(),
            backlink: None,
        };
        let initial = match self
            .initial_content(context, self.issuer, &mut plan, fork)
            .await
        {
            Ok(initial) => initial,
            Err(error) => {
                self.big_repo
                    .abandon_allocation(branch.clone())
                    .await
                    .map_err(repository)?;
                self.commands.discard(&params.command).await?;
                return Err(error);
            }
        };
        let document = self
            .big_repo
            .commit_id(branch.clone(), initial, Vec::new(), &[])
            .await
            .map_err(repository)?;
        let heads = sorted(document.with_document_read(|doc| doc.get_heads()).await);
        self.commands
            .advance(&params.command, State::Committed, &heads)
            .await?;
        self.settle_branch(
            context,
            &Row {
                command: params.command,
                kind: Kind::Branch,
                digest: request_digest,
                actor: actor.uuid,
                document: Some(branch.clone()),
                placement: Some(params.placement),
                detail: Some(detail),
                state: State::Committed,
                heads: heads.clone(),
            },
        )
        .await?;
        Ok(WriteReceipt {
            document_id: branch,
            heads,
        })
    }

    /// Finish a committed branch from its ledger row: announce it on its origin unless it is
    /// stealth, place it, and only then acknowledge it. Replayed from the row after a crash, so
    /// each step is idempotent: the backlink is recognized by its commit message.
    async fn settle_branch(
        &self,
        context: &RequestContext,
        row: &Row,
    ) -> Result<(), DocumentsError> {
        let (
            Some(branch),
            Some(placement),
            Some(Detail::Branch {
                origin,
                path,
                stealth,
            }),
        ) = (&row.document, &row.placement, &row.detail)
        else {
            unreachable!("a committed branch row names its document, placement and origin")
        };
        let command = &row.command;
        if !stealth {
            let handle = self.handle(origin).await?;
            if handle
                .with_document_read(|doc| committed_change(doc, command))
                .await
                .is_none()
            {
                let mut plan = Plan {
                    document: origin,
                    document_uuid: Uuid7::new(),
                    actor: &self.system_actor,
                    set: BTreeMap::new(),
                    remove: BTreeSet::new(),
                    now: jiff::Timestamp::now(),
                    predecessors: BTreeMap::new(),
                    aliases: BTreeMap::new(),
                    backlink: Some(Backlink {
                        key: BRANCHES_KEY
                            .parse()
                            .expect("the branches key is well formed"),
                        branch: branch.clone(),
                        path: path.clone(),
                    }),
                };
                self.commit_write(
                    context,
                    self.issuer,
                    &WriteMode::AtCurrent,
                    &mut plan,
                    Some(command),
                )
                .await
                .map_err(Failure::into_error)?;
            }
        }
        self.access
            .place(context, row.actor, branch, placement)
            .await
            .map_err(access_error)?;
        self.commands
            .advance(command, State::Done, &row.heads)
            .await
    }

    async fn merge(
        &self,
        context: &RequestContext,
        params: MergeDocument,
    ) -> Result<WriteReceipt, DocumentsError> {
        let actor = self.actor(context, params.capability).await?;
        self.authorize(context, &actor, AccessRequest::Read(&params.source))
            .await?;
        self.authorize(context, &actor, AccessRequest::Write(&params.target))
            .await?;
        let request_digest = digest(&(
            "merge",
            actor.uuid,
            &params.source,
            &params.at,
            &params.target,
        ));
        if let Some(command) = &params.command
            && let Some(receipt) = self.replay(command, request_digest).await?
        {
            return Ok(receipt);
        }
        let source = self.handle(&params.source).await?;
        let (heads, mut fork) = source
            .with_document_read(|doc| -> Result<_, DocumentsError> {
                context.validate()?;
                let heads = resolve(doc, &params.at)?;
                // A merge brings the source's history into the target unexamined, so it is held to
                // what a write's base is held to: every facet it brings — including one whose
                // content was deleted — must be accepted.
                let gate = Gate::new(doc, &heads);
                let keys: BTreeSet<_> = present_keys(doc, &heads)?
                    .into_iter()
                    .chain(gate.keys())
                    .collect();
                for key in keys {
                    unaccepted(gate.verify(&key)).map_err(|verdict| {
                        DocumentsError::BaseNotAccepted {
                            facet_key: key,
                            verdict,
                        }
                    })?;
                }
                Ok((heads.clone(), doc.fork_at(&heads).map_err(repository)?))
            })
            .await?;
        let target = self.handle(&params.target).await?;
        if let Some(command) = &params.command {
            self.commands
                .begin(
                    command,
                    Kind::Merge,
                    request_digest,
                    actor.uuid,
                    Some(&params.target),
                    None,
                    Some(&Detail::Merge {
                        source: params.source.clone(),
                        heads: heads.clone(),
                    }),
                )
                .await?;
        }
        let merged = target
            .with_document(|doc| -> Result<_, DocumentsError> {
                // Every document starts from its creating change, which depends on nothing. Two
                // documents share history exactly when the target holds the source's.
                let related = fork
                    .get_changes(&[])
                    .iter()
                    .filter(|change| change.deps().is_empty())
                    .all(|root| doc.get_change_by_hash(&root.hash()).is_some());
                if !related {
                    return Err(DocumentsError::UnrelatedHistory);
                }
                doc.merge(&mut fork).map_err(repository)?;
                Ok(sorted(doc.get_heads()))
            })
            .await;
        match (&params.command, merged) {
            (_, Ok(Ok(heads))) => {
                if let Some(command) = &params.command {
                    self.commands.advance(command, State::Done, &heads).await?;
                }
                Ok(WriteReceipt {
                    document_id: params.target,
                    heads,
                })
            }
            (Some(command), Ok(Err(error))) => {
                self.commands.discard(command).await?;
                Err(error)
            }
            (None, Ok(Err(error))) => Err(error),
            // Persistence failed after the merge may have been applied in memory: the row stays
            // for the boot pass, which reads the outcome off the target.
            (_, Err(error)) => Err(repository(error)),
        }
    }
}

fn present_keys(
    doc: &Automerge,
    heads: &[ChangeHash],
) -> Result<BTreeSet<FacetKey>, DocumentsError> {
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

impl DocumentsService {
    fn json<D: ReadDoc>(&self, doc: &D, facets: &ObjId, key: &FacetKey) -> Result<Json, String> {
        let handle = self.schema_repo.resolve_for_read(key.tag().as_str());
        let schema = handle.as_ref().map_or(&self.raw, SchemaHandle::compiled);
        hydrate_value(doc, facets, key.to_string().as_str().into(), schema)
            .map_err(|error| error.to_string())
    }

    async fn read(
        &self,
        context: &RequestContext,
        params: ReadFacets,
    ) -> Result<FacetsRead, DocumentsError> {
        let actor = self.actor(context, params.capability).await?;
        self.authorize(context, &actor, AccessRequest::Read(&params.document_id))
            .await?;
        if let Some(key) = params
            .facets
            .iter()
            .flatten()
            .find(|key| key.tag().as_str() == DMETA_FACET_TAG)
        {
            return Err(DocumentsError::SystemFacet {
                facet_key: key.clone(),
            });
        }
        let document = self.handle(&params.document_id).await?;
        document
            .with_document(|doc| -> Result<FacetsRead, DocumentsError> {
                context.validate()?;
                let heads = resolve(doc, &params.at)?;
                let keys = match &params.facets {
                    Some(keys) => keys.clone(),
                    None => present_keys(doc, &heads)?,
                };
                let verdicts: Vec<_> = {
                    let gate = Gate::new(doc, &heads);
                    keys.into_iter()
                        .map(|key| {
                            let verdict = gate.verify(&key);
                            (key, verdict)
                        })
                        .collect()
                };
                let tx = doc
                    .transaction_at(PatchLog::null(), &heads)
                    .expect("a null patch log matches any document");
                let facets_obj = match tx.get(automerge::ROOT, "facets").map_err(repository)? {
                    Some((Value::Object(ObjType::Map), facets)) => Some(facets),
                    _ => None,
                };
                let present = |key: &FacetKey| {
                    facets_obj.as_ref().filter(|facets| {
                        tx.get(*facets, key.to_string())
                            .is_ok_and(|value| value.is_some())
                    })
                };
                let mut facets = BTreeMap::new();
                for (key, verdict) in verdicts {
                    let read = match verdict {
                        Verdict::Accepted {
                            facet_heads,
                            native: None,
                        } => FacetRead::Absent { facet_heads },
                        Verdict::Accepted { facet_heads, .. } => {
                            let facets = present(&key).expect("an accepted value is present");
                            match self.json(&tx, facets, &key) {
                                Ok(value) => FacetRead::Accepted { value, facet_heads },
                                Err(reason) => FacetRead::Undecodable {
                                    reason,
                                    facet_heads,
                                },
                            }
                        }
                        Verdict::Pending {
                            reason,
                            facet_heads,
                        } => FacetRead::Pending {
                            value: present(&key)
                                .filter(|_| params.optimistic)
                                .and_then(|facets| self.json(&tx, facets, &key).ok()),
                            reason,
                            facet_heads,
                        },
                        Verdict::Rejected { reason } => FacetRead::Rejected { reason },
                    };
                    facets.insert(key, read);
                }
                tx.rollback();
                Ok(FacetsRead { heads, facets })
            })
            .await
            .map_err(repository)?
    }

    async fn list(
        &self,
        context: &RequestContext,
        params: ListFacets,
    ) -> Result<FacetListing, DocumentsError> {
        let actor = self.actor(context, params.capability).await?;
        self.authorize(context, &actor, AccessRequest::Read(&params.document_id))
            .await?;
        let document = self.handle(&params.document_id).await?;
        document
            .with_document_read(|doc| {
                context.validate()?;
                let heads = resolve(doc, &params.at)?;
                let facets = present_keys(doc, &heads)?
                    .into_iter()
                    .map(|key| {
                        let facet_heads = signature_heads(doc, &key, &heads);
                        (key, facet_heads)
                    })
                    .collect();
                Ok(FacetListing { heads, facets })
            })
            .await
    }

    /// Settle what a crash left in the command ledger. See [`super::commands`].
    async fn recover(&self) -> Result<(), DocumentsError> {
        let context = RequestContext::root();
        let reserved: BTreeSet<DocumentId> = self
            .big_repo
            .reserved_doc_ids()
            .await
            .map_err(repository)?
            .into_iter()
            .collect();
        for row in self.commands.unresolved().await? {
            match (row.kind, row.state, row.document.clone()) {
                // The id was never allocated, or its reservation holds no content: nothing exists
                // to finish. An allocated-but-unrecorded reservation is left alone, since BigRepo
                // reservations are shared with other allocators.
                (Kind::Create, State::Pending, None) => {
                    self.commands.discard(&row.command).await?;
                }
                (Kind::Create, State::Pending, Some(document)) => {
                    let exists = self
                        .big_repo
                        .has_keyhive_document(&document)
                        .await
                        .map_err(repository)?;
                    if !exists {
                        if reserved.contains(&document) {
                            self.big_repo
                                .abandon_allocation(document)
                                .await
                                .map_err(repository)?;
                        }
                        self.commands.discard(&row.command).await?;
                        continue;
                    }
                    let handle = if reserved.contains(&document) {
                        self.big_repo
                            .commit_reserved(document.clone(), &[])
                            .await
                            .map_err(repository)?
                    } else {
                        self.handle(&document).await?
                    };
                    let heads = sorted(handle.with_document_read(|doc| doc.get_heads()).await);
                    let placement = row.placement.expect("a create row names its placement");
                    self.settle_create(
                        &context,
                        &row.command,
                        row.actor,
                        &document,
                        &placement,
                        &heads,
                    )
                    .await?;
                }
                (Kind::Create, State::Committed, Some(document)) => {
                    let placement = row.placement.expect("a create row names its placement");
                    self.access
                        .place(&context, row.actor, &document, &placement)
                        .await
                        .map_err(access_error)?;
                    self.commands
                        .advance(&row.command, State::Done, &row.heads)
                        .await?;
                }
                (Kind::Write, State::Pending, Some(document)) => {
                    let handle = match self.handle(&document).await {
                        Ok(handle) => handle,
                        Err(DocumentsError::DocumentNotFound) => {
                            self.commands.discard(&row.command).await?;
                            continue;
                        }
                        Err(_) => continue,
                    };
                    let committed = handle
                        .with_document_read(|doc| committed_change(doc, &row.command))
                        .await;
                    match committed {
                        Some(hash) => {
                            self.commands
                                .advance(&row.command, State::Done, &[hash])
                                .await?;
                        }
                        None => self.commands.discard(&row.command).await?,
                    }
                }
                (Kind::Branch, State::Pending, None) => {
                    self.commands.discard(&row.command).await?;
                }
                (Kind::Branch, State::Pending, Some(branch)) => {
                    let exists = self
                        .big_repo
                        .has_keyhive_document(&branch)
                        .await
                        .map_err(repository)?;
                    if !exists {
                        if reserved.contains(&branch) {
                            self.big_repo
                                .abandon_allocation(branch)
                                .await
                                .map_err(repository)?;
                        }
                        self.commands.discard(&row.command).await?;
                        continue;
                    }
                    let handle = if reserved.contains(&branch) {
                        self.big_repo
                            .commit_reserved(branch.clone(), &[])
                            .await
                            .map_err(repository)?
                    } else {
                        self.handle(&branch).await?
                    };
                    let heads = sorted(handle.with_document_read(|doc| doc.get_heads()).await);
                    self.commands
                        .advance(&row.command, State::Committed, &heads)
                        .await?;
                    self.recover_branch(
                        &context,
                        &Row {
                            state: State::Committed,
                            heads,
                            ..row
                        },
                    )
                    .await?;
                }
                (Kind::Branch, State::Committed, Some(_)) => {
                    self.recover_branch(&context, &row).await?;
                }
                (Kind::Merge, State::Pending, Some(target)) => {
                    let Some(Detail::Merge { heads, .. }) = &row.detail else {
                        unreachable!("a merge row records its source heads")
                    };
                    let handle = match self.handle(&target).await {
                        Ok(handle) => handle,
                        Err(DocumentsError::DocumentNotFound) => {
                            self.commands.discard(&row.command).await?;
                            continue;
                        }
                        Err(_) => continue,
                    };
                    let merged = handle
                        .with_document_read(|doc| {
                            heads
                                .iter()
                                .all(|head| doc.get_change_by_hash(head).is_some())
                                .then(|| sorted(doc.get_heads()))
                        })
                        .await;
                    match merged {
                        Some(heads) => {
                            self.commands
                                .advance(&row.command, State::Done, &heads)
                                .await?;
                        }
                        None => self.commands.discard(&row.command).await?,
                    }
                }
                (kind, state, document) => {
                    unreachable!("no command reaches {kind:?}/{state:?} with {document:?}")
                }
            }
        }
        self.commands.purge_done().await
    }

    /// Settle a committed branch from its row. A backlink the origin now refuses (its branch list
    /// stopped verifying after the branch was taken) leaves the row in flight rather than failing
    /// boot: the branch exists and cannot be rolled back, and the refusal is the origin's to repair.
    async fn recover_branch(
        &self,
        context: &RequestContext,
        row: &Row,
    ) -> Result<(), DocumentsError> {
        match self.settle_branch(context, row).await {
            Err(
                DocumentsError::BaseNotAccepted { .. }
                | DocumentsError::DocumentNotFound
                | DocumentsError::Pending { .. },
            ) => Ok(()),
            result => result,
        }
    }
}

impl Handler<CreateDocument> for DocumentsService {
    async fn call(
        &self,
        request: Envelope<CreateDocument>,
    ) -> Result<ResponseEnvelope<WriteReceipt>, DocumentsError> {
        Ok(self.create(&request.context, request.params).await?.into())
    }
}

impl Handler<WriteFacets> for DocumentsService {
    async fn call(
        &self,
        request: Envelope<WriteFacets>,
    ) -> Result<ResponseEnvelope<WriteReceipt>, DocumentsError> {
        Ok(self.write(&request.context, request.params).await?.into())
    }
}

impl Handler<BranchDocument> for DocumentsService {
    async fn call(
        &self,
        request: Envelope<BranchDocument>,
    ) -> Result<ResponseEnvelope<WriteReceipt>, DocumentsError> {
        Ok(self.branch(&request.context, request.params).await?.into())
    }
}

impl Handler<MergeDocument> for DocumentsService {
    async fn call(
        &self,
        request: Envelope<MergeDocument>,
    ) -> Result<ResponseEnvelope<WriteReceipt>, DocumentsError> {
        Ok(self.merge(&request.context, request.params).await?.into())
    }
}

impl Handler<ReadFacets> for DocumentsService {
    async fn call(
        &self,
        request: Envelope<ReadFacets>,
    ) -> Result<ResponseEnvelope<FacetsRead>, DocumentsError> {
        Ok(self.read(&request.context, request.params).await?.into())
    }
}

impl Handler<ListFacets> for DocumentsService {
    async fn call(
        &self,
        request: Envelope<ListFacets>,
    ) -> Result<ResponseEnvelope<FacetListing>, DocumentsError> {
        Ok(self.list(&request.context, request.params).await?.into())
    }
}

fn history_reason(error: ClaimHistoryError) -> String {
    match error {
        ClaimHistoryError::ResourceRefused(reason) => format!("resource limit: {reason}"),
        ClaimHistoryError::Unsupported(reason) => format!("unsupported: {reason}"),
        ClaimHistoryError::MalformedWitness(reason) => format!("malformed witness: {reason}"),
        ClaimHistoryError::InvalidSignature => "invalid signature".into(),
        ClaimHistoryError::Automerge(reason) => format!("Automerge: {reason}"),
    }
}

impl From<ClaimHistoryError> for DocumentsError {
    fn from(error: ClaimHistoryError) -> Self {
        match error {
            ClaimHistoryError::ResourceRefused(reason) => Self::ResourceRefused {
                reason: reason.into(),
            },
            ClaimHistoryError::Unsupported(reason) => Self::Unsupported {
                reason: reason.into(),
            },
            ClaimHistoryError::Automerge(reason) => Self::Repository(reason),
            error @ (ClaimHistoryError::MalformedWitness(_)
            | ClaimHistoryError::InvalidSignature) => Self::MalformedEvidence {
                reason: history_reason(error),
            },
        }
    }
}

impl From<ClaimHistoryEntry> for FacetClaimEvidence {
    fn from(entry: ClaimHistoryEntry) -> Self {
        let result = match entry.result {
            ClaimHistoryResult::Accepted(claim) => SignatureEvidence::VerifiedSignature {
                evidence: Box::new(VerifiedSignatureEvidence {
                    signature: claim.signature,
                    actor_public_key: claim.actor_public_key,
                    claim: claim.claim,
                    introduction_marker: claim.introduction.marker.to_string(),
                    introduction_hash: claim.introduction.introduction_hash,
                    parents: claim.introduction.parents,
                    resulting_heads: claim.introduction.resulting_heads,
                    automerge_actor: claim
                        .introduction
                        .automerge_actor
                        .to_bytes()
                        .to_vec()
                        .into(),
                }),
            },
            ClaimHistoryResult::Pending(ClaimHistoryError::ResourceRefused(reason)) => {
                SignatureEvidence::ResourceRefused {
                    reason: reason.into(),
                }
            }
            ClaimHistoryResult::Pending(ClaimHistoryError::Unsupported(reason)) => {
                SignatureEvidence::Unsupported {
                    reason: reason.into(),
                }
            }
            ClaimHistoryResult::Pending(error) => SignatureEvidence::Pending {
                reason: history_reason(error),
            },
            ClaimHistoryResult::ProvenForgery(error) => SignatureEvidence::ProvenForgery {
                reason: history_reason(error),
            },
        };
        Self {
            marker: entry.marker.to_string(),
            result,
        }
    }
}

impl DocumentsService {
    async fn inspected(
        &self,
        context: &RequestContext,
        capability: CapId,
        document_id: &DocumentId,
        heads: &[ChangeHash],
    ) -> Result<BigDocHandle, DocumentsError> {
        let held = self
            .authority
            .call(context.child(), InspectCapability { capability })
            .await
            .map_err(authority_error)?;
        if held.issuer != self.system_actor.uuid
            || held.object != inspection_object(document_id)
            || !held.rights.contains(Rights::USE)
        {
            return Err(DocumentsError::Denied);
        }
        if heads.len() > HistoryBudget::default().max_entries_per_frontier {
            return Err(DocumentsError::ResourceRefused {
                reason: "heads frontier".into(),
            });
        }
        context.validate()?;
        self.handle(document_id).await
    }

    fn claims(
        doc: &Automerge,
        document_id: DocumentId,
        facet_key: FacetKey,
        heads: Vec<ChangeHash>,
    ) -> Result<FacetClaimsInspection, DocumentsError> {
        let evidence =
            reconstruct_current_claims(doc, &facet_key, &heads, HistoryBudget::default())?
                .into_iter()
                .map(FacetClaimEvidence::from)
                .collect();
        Ok(FacetClaimsInspection {
            document_id,
            facet_key,
            heads,
            scope: EvidenceScope::UnsafeUnattributedSignatureEvidence,
            evidence,
        })
    }
}

impl Handler<UnsafeInspectFacet> for DocumentsService {
    async fn call(
        &self,
        request: Envelope<UnsafeInspectFacet>,
    ) -> Result<ResponseEnvelope<UnsafeFacetInspection>, DocumentsError> {
        let params = request.params;
        let document = self
            .inspected(
                &request.context,
                params.capability,
                &params.document_id,
                &params.heads,
            )
            .await?;
        let schema_known = self
            .schema_repo
            .resolve_for_read(params.facet_key.tag().as_str())
            .is_some();
        let output = document
            .with_document_read(|doc| {
                request.context.validate()?;
                check_heads(doc, &params.heads)?;
                let native_value = match doc
                    .get_at(automerge::ROOT, "facets", &params.heads)
                    .map_err(ClaimHistoryError::from)?
                {
                    None => None,
                    Some((Value::Object(ObjType::Map), facets)) => NativeValue::read_at_heads(
                        doc,
                        &facets,
                        params.facet_key.to_string(),
                        &params.heads,
                        &mut NativeReadBudget::default(),
                    )
                    .map_err(ClaimHistoryError::from)?
                    .map(|value| encode_native(&value).into()),
                    Some(_) => {
                        return Err(DocumentsError::MalformedEvidence {
                            reason: "facets is not a map".into(),
                        });
                    }
                };
                let claims = Self::claims(doc, params.document_id, params.facet_key, params.heads)?;
                Ok::<_, DocumentsError>(UnsafeFacetInspection {
                    native_value,
                    schema_known,
                    claims,
                })
            })
            .await?;
        Ok(output.into())
    }
}

impl Handler<InspectFacetClaims> for DocumentsService {
    async fn call(
        &self,
        request: Envelope<InspectFacetClaims>,
    ) -> Result<ResponseEnvelope<FacetClaimsInspection>, DocumentsError> {
        let params = request.params;
        let document = self
            .inspected(
                &request.context,
                params.capability,
                &params.document_id,
                &params.heads,
            )
            .await?;
        let output = document
            .with_document_read(|doc| {
                request.context.validate()?;
                check_heads(doc, &params.heads)?;
                Self::claims(doc, params.document_id, params.facet_key, params.heads)
            })
            .await?;
        Ok(output.into())
    }
}

pub type DocumentsHandler = Arc<DocumentsService>;

pub struct DocumentsRepo {
    service: Arc<DocumentsService>,
    mint_spawner: Spawner,
    pub host: Arc<Host<DocumentsHandler>>,
    pub client: Client<DocumentsProtocol>,
}

impl DocumentsRepo {
    pub async fn mint_inspection_handle(
        &self,
        document_id: DocumentId,
    ) -> Result<Capability, DocumentsError> {
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
                        object: inspection_object(&document_id),
                        rights: Rights::USE,
                    }) => result.map_err(authority_error),
                }
            })
            .ok_or(ContextError::Cancelled)?;
        task.await
            .expect("inspection capability mint task panicked")
    }
}

/// Boot DocumentsRepo: settle the command ledger, then host the service.
pub async fn boot(config: DocumentsConfig) -> Result<(DocumentsRepo, StopToken), DocumentsError> {
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
    let (dmeta, branch, branches) = (
        core(DMETA_FACET_TAG),
        core(BRANCH_FACET_TAG),
        core(BRANCHES_FACET_TAG),
    );
    let service = Arc::new(DocumentsService {
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
        branches,
        commands: Commands::open(config.sql).await?,
    });
    service.recover().await?;
    let (mint_stop, mint_spawner) = StopToken::new();
    let (host, host_stop) = Host::new(Arc::clone(&service));
    let client = Client::direct(Arc::clone(&host));
    Ok((
        DocumentsRepo {
            service,
            mint_spawner,
            host,
            client,
        },
        host_stop.then(mint_stop),
    ))
}
