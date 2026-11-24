//! The single owner of the reconciled nodule inventory, durable host actors, and applied drawer
//! grants.
//!
//! Desired state lives in the node's config drawer document and is projected through
//! [`DatumStoreHandle<NoduleConfig>`]. This task serializes management mutations and config
//! snapshot reconciliations, provisions/reopens each admitted nodule's durable actor under
//! `parent_path`, and applies or revokes service-owned local drawer grants within the nodules
//! actor's delegated authority.

use crate::cartridge_docs::CartridgeDocs;
use crate::cartridge_docs::PinError;
use crate::executor::HandlerSpec;
use crate::executor::Hosted;
use crate::executor::NoduleExecutor;
use crate::executor::ReensureQueue;
use crate::executor::RoutineCapabilities;
use crate::executor::RoutineSpec;
use crate::requests::GrantRequests;
use crate::requests::GrantsFeed;
use crate::requests::MissingAuthorityLookup;
use crate::requests::NetUse;
use crate::requests::Requests;
use crate::requests::RoutineUses;
use crate::store;
use crate::store::DrawerOrigin;
use big_cabinet::DatumStore;
use big_cabinet::DatumStoreHandle;
use big_cabinet::SchemaRegistration;
use big_cabinet::SchemaRegistrationGroup;
use big_cabinet::SchemaRepo;
use big_cabinet_core::DatumPin;
use big_cabinet_core::authority::AuthorityProtocol;
use big_cabinet_core::authority::AuthorityServiceError;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::InspectCapability;
use big_cabinet_core::authority::OpenActor;
use big_cabinet_core::authority::OpenedActor;
use big_cabinet_core::authority::RetractCapability;
use big_cabinet_core::authority::Rights;

use big_cabinet_core::datum::operation::OperationKind;
use big_cabinet_core::dmeta::DocId;
use big_cabinet_core::dmeta::Uuid7;
use big_cabinet_core::doc::ChangeHash;
use big_cabinet_core::doc::DatumKinds;
use big_cabinet_core::doc::DocsProtocol;
use big_cabinet_core::doc::MintDocCapability;
use big_cabinet_core::doc::StoreError;
use big_cabinet_core::doc::StoreSnapshot;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::drawer::protocol::DrawerScope;
use big_cabinet_core::drawer::protocol::DrawersError;
use big_cabinet_core::drawer::protocol::DrawersProtocol;
use big_cabinet_core::drawer::protocol::GrantDrawer;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use big_cabinet_core::drawer::protocol::RevokeDrawer;
use big_cartridges_core::CartridgeId;
use big_cartridges_core::Permbind;
use big_cartridges_core::Revision;
use big_cartridges_core::RunMode;
use big_cartridges_core::config::Component;
use big_cartridges_core::config::GrantBody;
use big_cartridges_core::config::GrantKey;
use big_cartridges_core::config::NetTarget;
use big_cartridges_core::protocol::CartridgeGrantRecord;
use big_cartridges_core::protocol::CartridgeInstallRecord;
use big_cartridges_core::protocol::CartridgesProtocol;
use big_cartridges_core::protocol::FiledGrantRequest;
use big_cartridges_core::protocol::GrantAnswer;
use big_cartridges_core::protocol::GrantRequestState;
use big_dispatch::Caller;
use big_dispatch::LexiconSource;
use big_dispatch::Operation;
use big_dispatch::Provider;
use big_dispatch::ProviderError;
use big_dispatch::ProviderKind;
use big_dispatch::ProviderState;
use big_dispatch::Router;
use big_dispatch_core::DispatchError;
use big_dispatch_core::DispatchProtocol;
use big_dispatch_core::DispatchScope;
use big_dispatch_core::InvocationContext;
use big_dispatch_core::MintDispatchCapability;
use big_dispatch_core::Readiness;
use big_nodule_core::NoduleAdmission;
use big_nodule_core::config::AdmissionCache;
use big_nodule_core::config::CartridgeConfig;
use big_nodule_core::config::DatumSlot;
use big_nodule_core::config::NoduleConfig;
use big_nodule_core::config::NoduleResolution;
use big_nodule_core::inventory::ArtifactState;
use big_nodule_core::inventory::InstalledNodule;
use big_nodule_core::inventory::InventoryError;
use big_nodule_core::inventory::RoutineScopes;
use big_nodule_core::protocol::NoduleActor;
use big_nodule_core::protocol::NoduleGrantStatus;
use big_nodule_core::protocol::NoduleInitStatus;
use big_nodule_core::protocol::NoduleMethodGrantStatus;
use big_nodule_core::protocol::NoduleRecord;
use big_nodule_core::protocol::NoduleScope;
use big_nodule_core::protocol::NodulesError;
use big_nodule_core::providers::LockedProvider;
use big_nodule_core::providers::ProviderIndex;
use big_nodule_core::providers::RequestStatus;
use big_nodule_core::providers::Unresolved;
use indexmap::IndexMap;
use serde_json::Value;
use service_client_native_rs::Client;
use service_utils_rs::ClientError;
use service_utils_rs::RequestContext;
use service_utils_rs::Spawner;
use service_utils_rs::StopToken;
use sqlite_rs::Database;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use utils_rs::expect_tags::ERROR_ACTOR;
use utils_rs::expect_tags::ERROR_CHANNEL;

type Res<T> = Result<T, NodulesError>;
type Reply<T> = oneshot::Sender<Res<T>>;

pub(crate) struct Stores {
    pub(crate) sql: Database,
    pub(crate) authority: Client<AuthorityProtocol>,
    pub(crate) drawers: Client<DrawersProtocol>,
    pub(crate) datum_store: DatumStoreHandle<NoduleConfig>,
    /// The docs service and the handle the service reads the registry with and reads, writes and
    /// creates cartridge config documents in the shared config drawer with.
    pub(crate) docs: Client<DocsProtocol>,
    pub(crate) docs_capability: CapId,
    /// The cartridges service a pinned version is resolved through, and the read handle the
    /// service presents to it.
    pub(crate) cartridges: Client<CartridgesProtocol>,
    pub(crate) cartridges_authorization: Authorization,
    /// The blobs service and the delegator handle routine blobs handles are minted from.
    pub(crate) blobs: Client<big_blobs_core::BlobsProtocol>,
    pub(crate) blobs_capability: CapId,
    pub(crate) shared_config_drawer: DrawerId,
    pub(crate) parent: OpenedActor,
    pub(crate) parent_path: String,
    pub(crate) dispatch: Arc<Router>,
    pub(crate) dispatch_client: Client<DispatchProtocol>,
    /// The operator-granted dispatch handle the service mints every nodule's own handle from.
    pub(crate) dispatch_authority: CapId,
    pub(crate) executor: Option<Arc<dyn NoduleExecutor>>,
    /// The node's schema repo: an enabled nodule's held datums are registered here and retired
    /// when it is disabled, refused, taken over or uninstalled.
    pub(crate) schemas: Arc<SchemaRepo>,
    /// `NodulesConfig::allow_slot_takeover`.
    pub(crate) allow_slot_takeover: bool,
    /// `NodulesConfig::core_cartridge`.
    pub(crate) core_cartridge: crate::CoreCartridge,
}

impl Stores {
    /// The providers the node itself serves `request`'s contract under: dispatch's routes for an
    /// operation, the boot schemas for a datum. Cartridges' own offers come from the snapshot.
    pub(crate) fn native_offers(&self, request_contract: &str, is_operation: bool) -> Vec<String> {
        if is_operation {
            self.dispatch.claimants(request_contract)
        } else if self.schemas.is_boot_schema(request_contract) {
            vec![NATIVE_SCHEMAS.to_owned()]
        } else {
            Vec::new()
        }
    }
}

/// The provider name a boot schema is offered under in the lockfile.
const NATIVE_SCHEMAS: &str = "native";

pub(crate) enum Command {
    Install {
        context: RequestContext,
        /// What the caller may install: `None` for the operator.
        scope: Option<NoduleScope>,
        pin: DatumPin,
        reply: Reply<CartridgeInstallRecord>,
    },
    SetEnabled {
        context: RequestContext,
        cartridge_id: CartridgeId,
        component: Component,
        enabled: bool,
        reply: Reply<CartridgeInstallRecord>,
    },
    SetGrant {
        context: RequestContext,
        /// The caller's authority, which must reach the drawer a drawer grant names: it is what
        /// grants the drawer to the service's own actor (see `State::hold_drawer`).
        authorization: Authorization,
        cartridge_id: CartridgeId,
        grant: GrantKey,
        value: Option<GrantBody>,
        reply: Reply<()>,
    },
    ClearConfig {
        context: RequestContext,
        cartridge_id: CartridgeId,
        reply: Reply<()>,
    },
    ConfirmResolution {
        context: RequestContext,
        cartridge_id: CartridgeId,
        binding: String,
        reply: Reply<NoduleRecord>,
    },
    SetMethodRoute {
        context: RequestContext,
        pattern: String,
        provider: Option<String>,
        reply: Reply<()>,
    },
    SetDevTakeover {
        context: RequestContext,
        cartridge_id: CartridgeId,
        by: Option<CartridgeId>,
        reply: Reply<NoduleRecord>,
    },
    Uninstall {
        context: RequestContext,
        cartridge_id: CartridgeId,
        reply: Reply<()>,
    },
    Get {
        context: RequestContext,
        cartridge_id: CartridgeId,
        reply: Reply<NoduleRecord>,
    },
    List {
        context: RequestContext,
        scope: Option<NoduleScope>,
        reply: Reply<Vec<NoduleRecord>>,
    },
    ListInstalls {
        context: RequestContext,
        reply: Reply<Vec<CartridgeInstallRecord>>,
    },
    /// The cartridge whose nodule runs as `actor_id`, which is how a request is attributed to a
    /// cartridge: from the caller's actor, never from a body.
    CartridgeOfActor {
        actor_id: Uuid7,
        reply: Reply<CartridgeId>,
    },
    FileGrantRequest {
        cartridge_id: CartridgeId,
        permbind: String,
        drawer: Option<DrawerId>,
        reply: Reply<FiledGrantRequest>,
    },
    /// A routine of the cartridge created `drawer` through its own call (see
    /// [`MissingAuthorityLookup::drawer_created`]); `access` is what its `#drawers` permbind
    /// declares for what it creates.
    DrawerCreated {
        context: RequestContext,
        cartridge_id: CartridgeId,
        drawer: DrawerId,
        access: Option<DrawerAccess>,
        reply: Reply<()>,
    },
    AnswerGrantRequest {
        context: RequestContext,
        /// The operator's authority, as for [`Command::SetGrant`].
        authorization: Authorization,
        request: Uuid7,
        answer: GrantAnswer,
        drawer: Option<DrawerId>,
        reply: Reply<GrantRequestState>,
    },
    ListGrants {
        cartridge_id: Option<CartridgeId>,
        reply: Reply<Vec<CartridgeGrantRecord>>,
    },
}

/// What caused a reconcile, which decides what it re-runs: a failed initializer is retried by
/// every trigger but its own result, or a failing one would be retried in a tight loop.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Trigger {
    /// The service starting: applied grants are re-granted, every nodule's start begins.
    Boot,
    /// A config change, an executor signal, or a dead capability.
    Change,
    /// An initializer finished.
    InitResult,
}

/// One nodule's initializers in its current start: one per (cartridge, revision, start), replaced
/// whole when the nodule is disabled, retired, refused, uninstalled or moves to another revision.
struct InitRun {
    /// Names this run. An invocation's result carries the epoch of the run that spawned it and is
    /// dropped when the cartridge's run has since been replaced, so a superseded start or revision
    /// never marks anything done.
    epoch: u64,
    revision: Revision,
    /// Initializers satisfied for this run: succeeded in it, or `Once` ones that succeeded at
    /// this revision on an earlier start (read from SQLite when the run begins).
    done: BTreeSet<String>,
    /// The key of the invocation in flight. Initializers run one at a time, in declaration order.
    running: Option<String>,
    /// Invocations of the first unsatisfied initializer (see [`NoduleInitStatus::attempts`]).
    attempts: u32,
    /// The last failure of that initializer.
    error: Option<String>,
    /// The last invocation failed and nothing has retried it.
    failed: bool,
}

/// An initializer invocation's outcome on its way back to the state actor.
struct InitFinished {
    cartridge_id: CartridgeId,
    epoch: u64,
    key: String,
    /// Whether the initializer is `Once`, whose success is persisted.
    once: bool,
    outcome: Result<(), String>,
}

#[derive(Clone)]
pub(crate) struct Handle {
    commands: mpsc::Sender<Command>,
    /// The install rows as the last reconcile left them, for [`crate::NoduleService`]'s
    /// subscription to follow.
    pub(crate) installs: watch::Receiver<Vec<CartridgeInstallRecord>>,
    /// The grant requests of this boot, for the service's list and its long polls to follow.
    pub(crate) requests: watch::Receiver<Requests>,
    /// Each cartridge's grants as the last reconcile left them, for the service's
    /// `awaitCartridgeGrants` to follow.
    pub(crate) grants: watch::Receiver<GrantsFeed>,
}

impl Handle {
    pub(crate) async fn ask<T>(&self, command: impl FnOnce(Reply<T>) -> Command) -> Res<T> {
        let (reply, answer) = oneshot::channel();
        self.commands.send(command(reply)).await.expect(ERROR_ACTOR);
        answer.await.expect(ERROR_CHANNEL)
    }
}

pub(crate) struct State {
    stores: Stores,
    persisted_actors: BTreeMap<CartridgeId, Uuid7>,
    actors: BTreeMap<CartridgeId, OpenedActor>,
    applied_grants: BTreeMap<CartridgeId, BTreeMap<DrawerId, (DrawerAccess, CapId)>>,
    /// The drawers the service's own actor holds durably, each for the cartridge whose grant it
    /// serves, mirrored from SQLite: a cartridge's grant is handed down from the service's hold
    /// (`apply_grant`), so a drawer a cartridge is granted is held here for it until its config
    /// document no longer names the drawer (`release_idle_drawers`). A drawer a routine had the
    /// service create is `Created` and never given back.
    service_drawers: BTreeMap<(DrawerId, CartridgeId), DrawerOrigin>,
    /// The dispatch handle minted for each nodule from its method grants, with the scope it
    /// was minted for.
    applied_operations: BTreeMap<CartridgeId, (DispatchScope, CapId)>,
    /// What each routine of a cartridge holds, by routine key. In memory: a start mints afresh.
    routine_capabilities: BTreeMap<CartridgeId, BTreeMap<String, AppliedRoutine>>,
    /// The per-nodule config capabilities, scoped to declared settings datums.
    applied_settings: BTreeMap<CartridgeId, (DocId, DatumKinds, CapId)>,
    /// The resolution lockfile: the provider each enabled nodule's operation request is pinned
    /// to, by binding (see [`LockedProvider`]). Mirrors SQLite.
    locks: BTreeMap<CartridgeId, BTreeMap<String, LockedProvider>>,
    applied_heads: BTreeMap<CartridgeId, Vec<ChangeHash>>,
    records: BTreeMap<CartridgeId, NoduleRecord>,
    observed_heads: Vec<ChangeHash>,
    /// Memoized per-envelope admission, shared with the config-mutation closures.
    admission_cache: Arc<AdmissionCache>,
    /// The revision the executor was last asked to host for each cartridge. A cartridge is here exactly
    /// while the executor may hold a workload for it, which is what `retire` must undo.
    hosted: BTreeMap<CartridgeId, Revision>,
    /// The revision whose datum Lexicons are registered in the node's schema repo for each cartridge,
    /// under the cartridge id as owner. A cartridge is here exactly while it is enabled and admitted.
    registered_schemas: BTreeMap<CartridgeId, Revision>,
    /// The unusable route datums already reported (see `NoduleConfig::routes`).
    reported_unreadable_routes: BTreeSet<String>,
    /// Cartridges whose executor asked to be ensured again. The state actor constructs it and
    /// outlives nothing that holds a handle to it in a way that matters (see [`ReensureQueue`]).
    reensure: Arc<ReensureQueue>,
    /// This actor's own inbox, which each cartridge's [`MissingAuthorityLookup`] reports a created
    /// drawer through. Weak, so the inbox still closes when the service drops its handle.
    commands: mpsc::WeakSender<Command>,
    /// Initializer runs by cartridge.
    inits: BTreeMap<CartridgeId, InitRun>,
    /// The revision each `Once` initializer last succeeded at on this node.
    init_done: BTreeMap<CartridgeId, BTreeMap<String, Revision>>,
    next_epoch: u64,
    /// Where initializer invocations run, and where their results come back to.
    tasks: Spawner,
    init_results: mpsc::Sender<InitFinished>,
    /// The registry documents installs pin and the cartridge config documents they name.
    docs: CartridgeDocs,
    /// Rung when a loaded cartridge config document changes.
    docs_changed: Arc<Notify>,
    /// The install rows of the last reconcile.
    installs: watch::Sender<Vec<CartridgeInstallRecord>>,
    /// The runtime grant requests of this boot and the grants that last as long (ADR 049 §4.2).
    grant_requests: GrantRequests,
    /// Each cartridge's refusal attribution, shared with its executor (see
    /// [`MissingAuthorityLookup`]).
    missing_authority: BTreeMap<CartridgeId, MissingAuthorityLookup>,
}

mod holds;

fn nodule_actor(actor: &OpenedActor) -> NoduleActor {
    NoduleActor {
        actor_id: actor.actor.actor_id,
        url: actor.actor.url.clone(),
        public_key: actor.actor.public_key.clone(),
    }
}

/// Edits `handle`'s document with `edit`, whose domain refusal comes back as itself and not as the
/// store's invalid-edit error it travels through.
async fn mutate_domain<S: DatumStore>(
    handle: &DatumStoreHandle<S>,
    context: &RequestContext,
    edit: impl FnOnce(&mut S) -> Result<(), InventoryError> + Send + 'static,
) -> Res<()> {
    let domain_error: Arc<Mutex<Option<NodulesError>>> = Arc::new(Mutex::new(None));
    let captured = Arc::clone(&domain_error);
    let result = handle
        .mutate(context, None, move |config| {
            edit(config).map_err(|err| {
                let message = err.to_string();
                *captured.lock().expect("domain error lock") = Some(err.into());
                StoreError::InvalidEdit(message)
            })
        })
        .await;
    if let Err(err) = result {
        if let Some(domain) = domain_error.lock().expect("domain error lock").take() {
            return Err(domain);
        }
        return Err(crate::store_error(err));
    }
    Ok(())
}

async fn answer<T>(mut reply: oneshot::Sender<T>, operation: impl Future<Output = T>) {
    let result = tokio::select! {
        biased;
        () = reply.closed() => return,
        result = operation => result,
    };
    drop(reply.send(result));
}

async fn collect_apply_errors(
    state: &mut State,
    context: &RequestContext,
    cartridge_id: &CartridgeId,
    desired_grants: &BTreeMap<DrawerId, DrawerAccess>,
    operations_error: &Option<String>,
    settings_error: &Option<String>,
) -> Res<Vec<String>> {
    let mut errors = operations_error
        .iter()
        .map(|error| format!("operations: {error}"))
        .chain(
            settings_error
                .iter()
                .map(|error| format!("settings: {error}")),
        )
        .collect::<Vec<_>>();
    if let Err(error) = state
        .revoke_grants_for_cartridge(context, cartridge_id, desired_grants)
        .await
    {
        errors.push(error.to_string());
    }
    Ok(errors)
}

async fn serve_nodule(
    state: &mut State,
    installed: &mut InstalledNodule,
    actor: &OpenedActor,
    routine_capabilities: BTreeMap<String, RoutineCapabilities>,
    granted: &DispatchScope,
    requests: &BTreeMap<String, RequestStatus>,
    trigger: Trigger,
) -> Readiness {
    state
        .serve(
            installed,
            actor,
            routine_capabilities,
            granted,
            requests,
            trigger,
        )
        .await
}

async fn load_applied_grants(
    sql: &Database,
) -> Res<BTreeMap<CartridgeId, BTreeMap<DrawerId, (DrawerAccess, CapId)>>> {
    let mut applied_grants: BTreeMap<CartridgeId, BTreeMap<DrawerId, (DrawerAccess, CapId)>> =
        BTreeMap::new();
    for row in store::applied_grants(sql).await? {
        applied_grants
            .entry(row.cartridge_id)
            .or_default()
            .insert(row.drawer_id, (row.access, row.capability_id));
    }
    Ok(applied_grants)
}

async fn load_applied_settings(
    sql: &Database,
) -> Res<BTreeMap<CartridgeId, (DocId, DatumKinds, CapId)>> {
    Ok(store::applied_settings(sql)
        .await?
        .into_iter()
        .map(|row| (row.cartridge_id, (row.doc_id, row.kinds, row.capability_id)))
        .collect())
}

async fn load_applied_operations(
    sql: &Database,
) -> Res<BTreeMap<CartridgeId, (DispatchScope, CapId)>> {
    Ok(store::applied_operations(sql)
        .await?
        .into_iter()
        .map(|row| (row.cartridge_id, (row.scope, row.capability_id)))
        .collect())
}

/// What a routine holds: the handles, and the scopes they were minted for, which is what a
/// later reconcile compares against to keep a handle whose scope did not change.
#[derive(Clone)]
pub(crate) struct AppliedRoutine {
    scopes: RoutineScopes,
    handles: RoutineCapabilities,
}

impl AppliedRoutine {
    fn handles(&self) -> impl Iterator<Item = CapId> {
        [
            self.handles.dispatch,
            self.handles.docs,
            self.handles.drawers,
            self.handles.blobs,
        ]
        .into_iter()
        .flatten()
    }
}

/// Per routine, what its `uses` leave refused for want of a grant someone may still give (ADR 049
/// §4.3): the methods its method permbinds declare that `effective_operations` (the
/// cartridge's grants) do not cover, with the permbind of each, and every `#net` permbind it
/// takes with whether `desired_nets` covers it, and the access a drawer the routine creates is
/// granted it at (`RoutineUses::create_access`). Drawer and docs calls are not here: they pass the
/// handle and are refused by their own service, which cannot be traced to one permbind from the
/// call alone.
fn routine_uses(
    installed: &InstalledNodule,
    effective_operations: &DispatchScope,
    desired_nets: &BTreeSet<NetTarget>,
) -> BTreeMap<String, RoutineUses> {
    installed
        .nodule
        .routines
        .iter()
        .map(|(routine_key, routine)| {
            let mut uses = RoutineUses::default();
            for (binding, usage) in &routine.uses {
                match installed
                    .resolved
                    .cartridge
                    .permbinds
                    .get(binding)
                    .expect("admission refuses a routine use without its permbind")
                {
                    Permbind::Method(method)
                        if !effective_operations
                            .operations
                            .contains(method.nsid.as_str()) =>
                    {
                        uses.ungranted
                            .insert(method.nsid.to_string(), binding.to_string());
                    }
                    Permbind::Net(net) => {
                        let target = net
                            .target()
                            .expect("admission refuses a net permbind that names no host");
                        uses.nets.push(NetUse {
                            binding: binding.to_string(),
                            permbind: net.clone(),
                            granted: desired_nets.iter().any(|grant| grant.covers(&target)),
                            target,
                        });
                    }
                    Permbind::Drawers(drawer) if usage.create && drawer.create.is_some() => {
                        // The access `InstalledNodule::routine_scopes` reaches existing drawers at.
                        let access = [
                            (usage.read && drawer.read.is_some(), DrawerAccess::Read),
                            (usage.write && drawer.write.is_some(), DrawerAccess::Write),
                            (usage.delete && drawer.delete.is_some(), DrawerAccess::Write),
                        ]
                        .into_iter()
                        .filter_map(|(used, access)| used.then_some(access))
                        .max();
                        uses.create_access = uses.create_access.max(access);
                    }
                    _ => {}
                }
            }
            (routine_key.clone(), uses)
        })
        .collect()
}

/// Mints the handles each routine wants and does not hold, recording the scope each was minted
/// for. Returns the last mint error: a failed mint leaves its handle unset.
async fn mint_missing_handles(
    state: &State,
    context: &RequestContext,
    actor: &OpenedActor,
    desired: &BTreeMap<String, RoutineScopes>,
    routines: &mut BTreeMap<String, AppliedRoutine>,
) -> Option<String> {
    let mut error = None;
    for (routine_key, scopes) in desired {
        let applied = routines
            .entry(routine_key.clone())
            .or_insert_with(|| AppliedRoutine {
                scopes: RoutineScopes::default(),
                handles: RoutineCapabilities::default(),
            });
        if applied.handles.dispatch.is_none()
            && let Some(scope) = &scopes.dispatch
        {
            applied.handles.dispatch =
                match state.stores.mint_operations(context, actor, scope).await {
                    Ok(capability) => Some(capability),
                    Err(mint_error) => {
                        error = Some(mint_error.to_string());
                        None
                    }
                };
            applied.scopes.dispatch = scopes.dispatch.clone();
        }
        if applied.handles.docs.is_none()
            && let Some(scope) = &scopes.docs
        {
            // The service's own docs handle reaches only the config documents, so a scope that
            // reaches granted drawers is proved by the drawers' policy as the service actor,
            // which holds each drawer it granted (`apply_grant`), as a drawers handle is.
            let authorization = if scope.drawers.is_empty() {
                Authorization {
                    authority: state.stores.docs_capability,
                    signer: state.stores.parent.capability,
                }
            } else {
                Authorization::identity(state.stores.parent.capability)
            };
            applied.handles.docs = match state
                .stores
                .docs
                .call_with(
                    context.child(),
                    authorization.metadata(),
                    MintDocCapability {
                        scope: scope.clone(),
                        rights: Rights::USE,
                        subject: Some(actor.actor.actor_id),
                    },
                )
                .await
            {
                Ok(capability) => Some(capability.capability_id),
                Err(mint_error) => {
                    error = Some(crate::docs_error(mint_error).to_string());
                    None
                }
            };
            applied.scopes.docs = scopes.docs.clone();
        }
        if applied.handles.drawers.is_none()
            && let Some(scope) = &scopes.drawers
        {
            applied.handles.drawers = match state
                .stores
                .drawers
                .call_with(
                    context.child(),
                    Authorization::identity(state.stores.parent.capability).metadata(),
                    MintDrawerCapability {
                        scope: scope.clone(),
                        rights: Rights::USE,
                        subject: Some(actor.actor.actor_id),
                    },
                )
                .await
            {
                Ok(capability) => Some(capability.capability_id),
                Err(mint_error) => {
                    error = Some(drawers_error(mint_error).to_string());
                    None
                }
            };
            applied.scopes.drawers = scopes.drawers.clone();
        }
        if applied.handles.blobs.is_none()
            && let Some(scope) = &scopes.blobs
        {
            // Minted from the service's delegator handle, narrowed to this routine's flags and
            // bound to its nodule actor (ADR 049 §6.1).
            applied.handles.blobs = match state
                .stores
                .blobs
                .call_with(
                    context.child(),
                    Authorization {
                        authority: state.stores.blobs_capability,
                        signer: state.stores.parent.capability,
                    }
                    .metadata(),
                    big_blobs_core::MintBlobsCapability {
                        scope: scope.clone(),
                        rights: Rights::USE,
                        subject: Some(actor.actor.actor_id),
                    },
                )
                .await
            {
                Ok(capability) => Some(capability.capability_id),
                Err(mint_error) => {
                    error = Some(mint_error.to_string());
                    None
                }
            };
            applied.scopes.blobs = scopes.blobs.clone();
        }
    }
    error
}

/// What a cartridge's grants give, as one reconcile resolved them (ADR 049 §4.1).
struct Granted<'a> {
    drawers: &'a BTreeMap<DrawerId, DrawerAccess>,
    /// The operations that cover a request of the manifest.
    operations: &'a DispatchScope,
    nets: &'a BTreeSet<NetTarget>,
}

impl<'a> Granted<'a> {
    fn new(
        drawers: &'a BTreeMap<DrawerId, DrawerAccess>,
        operations: &'a DispatchScope,
        nets: &'a BTreeSet<NetTarget>,
    ) -> Self {
        Self {
            drawers,
            operations,
            nets,
        }
    }
}

/// Brings each routine's handles to its desired scopes (ADR 047 §6): a handle whose scope did
/// not change is kept, one that changed is retracted and then minted again, and one whose
/// routine no longer wants it is retracted. Before anything is retracted dispatch is
/// registered not ready, so a call that arrives between the retraction and the new handle is
/// answered retryably, never with the old handle or a wider one.
async fn apply_nodule_capabilities(
    state: &mut State,
    context: &RequestContext,
    cartridge_id: &CartridgeId,
    actor: &OpenedActor,
    installed: &InstalledNodule,
    Granted {
        drawers: desired_grants,
        operations: effective_operations,
        nets: desired_nets,
    }: Granted<'_>,
) -> Res<(
    BTreeMap<String, RoutineCapabilities>,
    Option<String>,
    Option<String>,
    DispatchScope,
)> {
    // The cartridge-wide handles of before routines held their own.
    state
        .revoke_operations_for_cartridge(context, cartridge_id, &DispatchScope::default())
        .await?;
    revoke_settings_for_cartridge(state, context, cartridge_id, None).await?;

    let desired = if installed.enabled {
        installed.routine_scopes(desired_grants, effective_operations, |nsid| {
            method_lexicon::builtin_method(nsid).and_then(|built_in| built_in.handle)
        })
    } else {
        BTreeMap::new()
    };
    let previous = state
        .routine_capabilities
        .get(cartridge_id)
        .cloned()
        .unwrap_or_default();
    // Per service handle: one whose scope is as desired and which is still live stays, any
    // other is retracted, and what is desired but not held is minted below.
    let mut routines: BTreeMap<String, AppliedRoutine> = BTreeMap::new();
    let mut stale = Vec::new();
    for (routine_key, applied) in previous {
        let wanted = desired.get(&routine_key);
        let mut kept = AppliedRoutine {
            scopes: RoutineScopes::default(),
            handles: RoutineCapabilities::default(),
        };
        let held = [
            (
                applied.handles.dispatch,
                wanted.is_some_and(|wanted| wanted.dispatch == applied.scopes.dispatch),
            ),
            (
                applied.handles.docs,
                wanted.is_some_and(|wanted| wanted.docs == applied.scopes.docs),
            ),
            (
                applied.handles.drawers,
                wanted.is_some_and(|wanted| wanted.drawers == applied.scopes.drawers),
            ),
            (
                applied.handles.blobs,
                wanted.is_some_and(|wanted| wanted.blobs == applied.scopes.blobs),
            ),
        ];
        let mut live = [false; 4];
        for (index, (handle, matches)) in held.into_iter().enumerate() {
            let Some(capability) = handle else { continue };
            if matches && state.stores.is_cap_live(context, capability).await? {
                live[index] = true;
            } else {
                stale.push(capability);
            }
        }
        if live[0] {
            kept.handles.dispatch = applied.handles.dispatch;
            kept.scopes.dispatch = applied.scopes.dispatch;
        }
        if live[1] {
            kept.handles.docs = applied.handles.docs;
            kept.scopes.docs = applied.scopes.docs;
        }
        if live[2] {
            kept.handles.drawers = applied.handles.drawers;
            kept.scopes.drawers = applied.scopes.drawers;
        }
        if live[3] {
            kept.handles.blobs = applied.handles.blobs;
            kept.scopes.blobs = applied.scopes.blobs;
        }
        routines.insert(routine_key, kept);
    }
    if !stale.is_empty() {
        if installed.enabled {
            state.register_not_ready(
                &cartridge_id.to_string(),
                installed,
                Readiness::Initializing,
            );
        }
        for capability in stale {
            state.stores.retract_grant_cap(context, capability).await?;
        }
    }

    let error = mint_missing_handles(state, context, actor, &desired, &mut routines).await;
    // A routine the manifest no longer has holds nothing.
    routines.retain(|routine_key, _| desired.contains_key(routine_key));
    if error.is_some() {
        // A routine that got only some of its handles would run with less than it declared:
        // none keep any, and the next reconcile mints them again.
        for applied in routines.values() {
            for capability in applied.handles() {
                state.stores.retract_grant_cap(context, capability).await?;
            }
        }
        routines.clear();
    }
    let handles = desired
        .keys()
        .map(|routine_key| {
            (
                routine_key.clone(),
                routines
                    .get(routine_key)
                    .map(|applied| RoutineCapabilities {
                        actor: Some(actor.capability),
                        ..applied.handles.clone()
                    })
                    // A failed mint leaves the routine with nothing, its actor included.
                    .unwrap_or_default(),
            )
        })
        .collect();
    // What a routine's refused call is attributed from: only while its handle is the one now
    // minted for it, and none at all when the mint failed and no routine holds a handle.
    let uses = if installed.enabled {
        routine_uses(installed, effective_operations, desired_nets)
    } else {
        BTreeMap::new()
    };
    state.missing_authority(cartridge_id).set(
        routines
            .iter()
            .filter_map(|(routine_key, applied)| {
                Some((applied.handles.dispatch?, uses.get(routine_key)?.clone()))
            })
            .collect(),
    );
    state
        .routine_capabilities
        .insert(cartridge_id.clone(), routines);
    let granted_operations = if error.is_none() {
        effective_operations.clone()
    } else {
        DispatchScope::default()
    };
    Ok((handles, error, None, granted_operations))
}

async fn revoke_settings_for_cartridge(
    state: &mut State,
    context: &RequestContext,
    cartridge_id: &CartridgeId,
    keep: Option<(&DocId, &DatumKinds)>,
) -> Res<()> {
    let Some((doc_id, kinds, capability)) = state.applied_settings.get(cartridge_id).cloned()
    else {
        return Ok(());
    };

    if keep.is_some_and(|(keep_doc, keep_kinds)| doc_id == *keep_doc && kinds == *keep_kinds) {
        return Ok(());
    }
    state.stores.retract_grant_cap(context, capability).await?;
    store::delete_applied_settings(&state.stores.sql, cartridge_id).await?;
    state.applied_settings.remove(cartridge_id);
    Ok(())
}
impl State {
    pub(crate) async fn recover(stores: Stores) -> Res<(Handle, StopToken)> {
        let mut persisted_actors = BTreeMap::new();
        for row in store::actors(&stores.sql).await? {
            persisted_actors.insert(row.cartridge_id, row.actor_id);
        }
        let applied_grants = load_applied_grants(&stores.sql).await?;
        let service_drawers = store::service_drawers(&stores.sql).await?;
        let applied_settings = load_applied_settings(&stores.sql).await?;
        let applied_operations = load_applied_operations(&stores.sql).await?;
        let init_done = store::init_done(&stores.sql).await?;
        let locks = store::resolution_locks(&stores.sql).await?;

        let mut watch = stores
            .datum_store
            .watch()
            .await
            .map_err(crate::store_error)?;

        let (commands, mut inbox) = mpsc::channel(64);
        let (init_results, mut finished_inits) = mpsc::channel(16);
        let (stop, tasks) = StopToken::new();
        let cancellation = tasks.clone();
        let docs_changed = Arc::new(Notify::new());
        let docs = CartridgeDocs::new(
            stores.docs.clone(),
            stores.docs_capability,
            stores.cartridges.clone(),
            stores.cartridges_authorization,
            stores.shared_config_drawer.clone(),
            stores.sql.clone(),
            Arc::clone(&docs_changed),
            tasks.clone(),
        )
        .await?;
        let (installs, installs_rx) = watch::channel(Vec::new());
        let (grant_requests, requests_rx, grants_rx) = GrantRequests::new(&stores.core_cartridge);

        let mut state = Self {
            stores,
            persisted_actors,
            actors: BTreeMap::new(),
            applied_grants,
            service_drawers,
            applied_operations,
            routine_capabilities: BTreeMap::new(),
            applied_settings,
            locks,
            applied_heads: BTreeMap::new(),
            registered_schemas: BTreeMap::new(),
            reported_unreadable_routes: BTreeSet::new(),
            records: BTreeMap::new(),
            observed_heads: Vec::new(),
            admission_cache: Arc::default(),
            hosted: BTreeMap::new(),
            reensure: Arc::default(),
            commands: commands.downgrade(),
            inits: BTreeMap::new(),
            init_done,
            next_epoch: 0,
            tasks: tasks.clone(),
            init_results,
            docs,
            docs_changed,
            installs,
            grant_requests,
            missing_authority: BTreeMap::new(),
        };
        let context = RequestContext::root();
        // The boot reconcile may already have started initializers, and the config documents it
        // loads run tasks; a failure here must not leave them running behind a service that
        // never came up.
        let booted = async {
            let initial = state.config().await?;
            state.actors = state
                .stores
                .reopen_actors(&context, &state.persisted_actors)
                .await?;
            state.reconcile(&context, initial, Trigger::Boot).await
        }
        .await;
        if let Err(err) = booted {
            state.docs.stop_all().await;
            stop.stop().await;
            return Err(err);
        }

        // The loop's failures are programming or lifecycle errors, not per-nodule outcomes
        // (those live in the records `reconcile` builds), so the task unwraps its result: the
        // process panic handler then brings everything down instead of leaving a dead actor
        // that panics every later `Handle::ask` on a closed channel.
        let reensure = Arc::clone(&state.reensure);
        let run = async move {
            loop {
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => break,
                    // An executor's workload changed readiness after `ensure` returned. Only
                    // cartridges it is still hosting matter: a request from a retired or upgraded-away
                    // workload names nothing to redo.
                    cartridges = reensure.requested() => {
                        if cartridges.iter().any(|cartridge_id| {
                            state.hosted.contains_key(cartridge_id) || state.inits.contains_key(cartridge_id)
                        }) {
                            let snapshot = state.config().await?;
                            drop(
                                state
                                    .reconcile(&RequestContext::root(), snapshot, Trigger::Change)
                                    .await?,
                            );
                        }
                    },
                    // The state actor holds a sender, so this channel outlives the loop.
                    finished = finished_inits.recv() => {
                        state.init_finished(finished.expect(ERROR_CHANNEL)).await?;
                    },
                    command = inbox.recv() => match command {
                        Some(command) => state.run(command).await,
                        None => break,
                    },
                    changed = watch.changed() => {
                        // The watch only says the node config moved. Read the current snapshot here: one
                        // taken when the event was produced can predate the change a command of this
                        // actor made while the actor was blocked (an enable waiting on its workload),
                        // and reconciling to it would withdraw what that command just served.
                        // Shutdown stops this task before the datum store it watches, so a closed
                        // watch while we are still running is a lifecycle break.
                        changed.expect("nodule config watch closed while the state actor was running");
                        state.reconcile_if_moved().await?;
                    },
                    // A cartridge config document moved: a grant written elsewhere, or by this
                    // actor (which already reconciled it, so the heads below say nothing moved).
                    () = state.docs_changed.notified() => {
                        state.reconcile_if_moved().await?;
                    },
                }
            }
            state.docs.stop_all().await;
            // Dispatch outlives this service and its router keeps every provider registered, but
            // an executor's provider reaches back into dispatch (a Wasm guest's outgoing calls
            // go through dispatch's client), which would make dispatch own itself through the
            // router. Withdrawing the registrations at shutdown breaks that cycle; a restart
            // derives them again from the config.
            for cartridge_id in state.records.keys() {
                state.stores.dispatch.unbind(&cartridge_id.to_string());
            }
            Res::Ok(())
        };
        tasks
            .spawn(async move { run.await.expect("nodule state actor failed") })
            .expect("nodule state task starts before shutdown");
        Ok((
            Handle {
                commands,
                installs: installs_rx,
                requests: requests_rx,
                grants: grants_rx,
            },
            stop,
        ))
    }

    /// The configuration as it is now: the node's config document, and the cartridge config
    /// documents its installs name, which are loaded here the first time an install names them.
    /// The heads are the node document's followed by each cartridge document's in cartridge
    /// order, so they move exactly when any of the documents does. Every install's pinned version
    /// is read into the admission cache, which is what a reconcile resolves against.
    async fn config(&mut self) -> Res<StoreSnapshot<NoduleConfig>> {
        let mut snapshot = self
            .stores
            .datum_store
            .snapshot()
            .await
            .map_err(crate::store_error)?;
        if !self.stores.allow_slot_takeover {
            // A takeover datum that reaches the document by any other path (a sync, a hand
            // edit) must not run another cartridge as a slot on a node that did not opt in.
            snapshot.value.refuse_dev_takeover();
        }
        self.docs.sync(&snapshot.value.config_docs()).await?;
        let (heads, mut docs) = self.docs.snapshots().await?;
        snapshot.heads.extend(heads);
        for (cartridge_id, doc) in &mut docs {
            if let Some(grants) = self.grant_requests.boot_grants(cartridge_id) {
                doc.attach_boot_grants(grants.clone());
            }
        }
        snapshot.value.attach_cartridge_docs(docs);
        self.docs
            .read_pins(&snapshot.value, &self.admission_cache)
            .await?;
        Ok(snapshot)
    }

    /// Reconciles when any config document is past the heads the last reconcile saw. The watches
    /// only say a document moved; a change this actor made itself (`mutate_config` reconciles it
    /// before the watch event arrives) is already applied at these heads, and only a change from
    /// elsewhere, such as a synced config edit, has others. Revoke failures come back as a map of
    /// per-cartridge outcomes that `reconcile` already folded into the refused records, so only an
    /// `Err` (storage, actor or capability failure) is fatal.
    async fn reconcile_if_moved(&mut self) -> Res<()> {
        let snapshot = self.config().await?;
        if snapshot.heads != self.observed_heads {
            drop(
                self.reconcile(&RequestContext::root(), snapshot, Trigger::Change)
                    .await?,
            );
        }
        Ok(())
    }

    async fn run(&mut self, command: Command) {
        match command {
            Command::Install {
                context,
                scope,
                pin,
                reply,
            } => answer(reply, self.install(&context, scope, pin)).await,
            Command::SetEnabled {
                context,
                cartridge_id,
                component,
                enabled,
                reply,
            } => {
                answer(
                    reply,
                    self.set_enabled(&context, &cartridge_id, component, enabled),
                )
                .await
            }
            Command::SetGrant {
                context,
                authorization,
                cartridge_id,
                grant,
                value,
                reply,
            } => {
                answer(
                    reply,
                    self.set_grant(&context, authorization, &cartridge_id, grant, value),
                )
                .await
            }
            Command::ClearConfig {
                context,
                cartridge_id,
                reply,
            } => answer(reply, self.clear_config(&context, &cartridge_id)).await,
            Command::ConfirmResolution {
                context,
                cartridge_id,
                binding,
                reply,
            } => {
                answer(
                    reply,
                    self.confirm_resolution(&context, &cartridge_id, &binding),
                )
                .await
            }
            Command::SetMethodRoute {
                context,
                pattern,
                provider,
                reply,
            } => answer(reply, self.set_method_route(&context, pattern, provider)).await,
            Command::SetDevTakeover {
                context,
                cartridge_id,
                by,
                reply,
            } => answer(reply, self.set_dev_takeover(&context, &cartridge_id, by)).await,
            Command::Uninstall {
                context,
                cartridge_id,
                reply,
            } => answer(reply, self.uninstall(&context, &cartridge_id)).await,
            Command::Get {
                context,
                cartridge_id,
                reply,
            } => answer(reply, self.get(&context, &cartridge_id)).await,
            Command::List {
                context,
                scope,
                reply,
            } => answer(reply, self.list(&context, scope)).await,
            Command::CartridgeOfActor { actor_id, reply } => {
                answer(reply, async {
                    self.actors
                        .iter()
                        .find(|(_, actor)| actor.actor.actor_id == actor_id)
                        .map(|(cartridge_id, _)| cartridge_id.clone())
                        .ok_or(NodulesError::Denied)
                })
                .await
            }
            Command::FileGrantRequest {
                cartridge_id,
                permbind,
                drawer,
                reply,
            } => {
                answer(reply, async {
                    let held = self.records.get(&cartridge_id);
                    self.grant_requests
                        .file(cartridge_id.clone(), permbind, drawer, held)
                })
                .await
            }
            Command::DrawerCreated {
                context,
                cartridge_id,
                drawer,
                access,
                reply,
            } => {
                answer(
                    reply,
                    self.drawer_created(&context, &cartridge_id, drawer, access),
                )
                .await
            }
            Command::AnswerGrantRequest {
                context,
                authorization,
                request,
                answer: given,
                drawer,
                reply,
            } => {
                answer(
                    reply,
                    self.answer_grant_request(&context, authorization, request, given, drawer),
                )
                .await
            }
            Command::ListGrants {
                cartridge_id,
                reply,
            } => {
                answer(reply, async {
                    let snapshot = self.config().await?;
                    Ok(self
                        .grant_requests
                        .list(snapshot.value.cartridge_docs(), cartridge_id.as_ref()))
                })
                .await
            }
            Command::ListInstalls { context, reply } => {
                answer(reply, async {
                    self.refresh_if_needed(&context).await?;
                    Ok(self.installs.borrow().clone())
                })
                .await
            }
        }
    }

    async fn mutate_config(
        &mut self,
        context: &RequestContext,
        edit: impl FnOnce(&mut NoduleConfig) -> Result<(), InventoryError> + Send + 'static,
    ) -> Res<()> {
        mutate_domain(&self.stores.datum_store, context, edit).await?;
        let snapshot = self.config().await?;
        drop(self.reconcile(context, snapshot, Trigger::Change).await?);
        Ok(())
    }

    /// The install row of `cartridge_id` as the last reconcile left it.
    fn install_record(&self, cartridge_id: &CartridgeId) -> Res<CartridgeInstallRecord> {
        self.installs
            .borrow()
            .iter()
            .find(|record| &record.cartridge_id == cartridge_id)
            .cloned()
            .ok_or_else(|| NodulesError::UnknownCartridge(cartridge_id.clone()))
    }

    /// Installs the version `pin` names: reads and verifies it, finds or creates the cartridge's
    /// config document, and records the install in the node's config document. Admission (the
    /// nodule's Lexicons, requests and artifact references), Lexicon registration, resolution,
    /// the actor and the hosted workload are the reconcile's, as for any config change.
    async fn install(
        &mut self,
        context: &RequestContext,
        scope: Option<NoduleScope>,
        pin: DatumPin,
    ) -> Res<CartridgeInstallRecord> {
        let resolved = match self.docs.read_pin(&pin).await {
            Ok(resolved) => resolved,
            Err(PinError::Unavailable(reason)) => {
                return Err(NodulesError::Rejected(format!(
                    "the pinned version is not available on this node: {reason}"
                )));
            }
            Err(PinError::Invalid(reason)) => return Err(NodulesError::Rejected(reason)),
            Err(PinError::Fatal(error)) => return Err(error),
        };
        if let Some(scope) = &scope
            && !(scope.admit && scope.cartridges.contains(&resolved.cartridge.id))
        {
            return Err(NodulesError::Denied);
        }
        // The core cartridge is installed at the version the node's build carries and no other,
        // so installing that version again is the node's own idempotent boot step and any other
        // version is refused (ADR 053 §3 item 5).
        let core = &self.stores.core_cartridge;
        let is_core = resolved.cartridge.id == core.id;
        if is_core && resolved.cartridge.version != core.version {
            return Err(NodulesError::CoreCartridge(core.id.clone()));
        }
        let cartridge_id = resolved.cartridge.id.clone();
        let doc = match self.config_doc(&cartridge_id).await? {
            Some(doc) => doc,
            None => self.docs.create(&cartridge_id).await?,
        };
        // The document is loaded before the install names it, so the reconcile that records the
        // install already sees its grants.
        self.docs
            .handle(&cartridge_id, &doc)
            .await
            .map_err(crate::store_error)?;
        let cache = Arc::clone(&self.admission_cache);
        let target = cartridge_id.clone();
        self.mutate_config(context, move |config| {
            config.install(&cache, &resolved, pin, doc)?;
            // A new install starts disabled and a person enables it. Nobody can enable the core
            // cartridge (`setCartridgeEnabled` refuses it), so its nodule is enabled by the
            // install itself, in the same write.
            if is_core {
                config.set_enabled(&cache, &target, Component::Nodule, true)?;
            }
            Ok(())
        })
        .await?;
        self.install_record(&cartridge_id)
    }

    /// The config document of `cartridge_id`: the one its install names, else the one an earlier
    /// install left in the shared config drawer.
    async fn config_doc(&mut self, cartridge_id: &CartridgeId) -> Res<Option<DocId>> {
        let installed = self
            .stores
            .datum_store
            .snapshot()
            .await
            .map_err(crate::store_error)?
            .value
            .config_docs()
            .remove(cartridge_id);
        match installed {
            Some(doc) => Ok(Some(doc)),
            None => self.docs.find(cartridge_id).await,
        }
    }

    async fn set_enabled(
        &mut self,
        context: &RequestContext,
        cartridge_id: &CartridgeId,
        component: Component,
        enabled: bool,
    ) -> Res<CartridgeInstallRecord> {
        let target = cartridge_id.clone();
        let cache = Arc::clone(&self.admission_cache);
        self.mutate_config(context, move |config| {
            config.set_enabled(&cache, &target, component, enabled)
        })
        .await?;
        self.install_record(cartridge_id)
    }

    /// Sets or revokes one grant datum in the cartridge's config document. A grant is the
    /// cartridge's, not a component's (ADR 049 §4.1), so any install may be granted.
    ///
    /// A drawer grant first grants the drawer to the service's own actor on the caller's authority
    /// (`hold_drawer`), so a caller with no authority over the drawer cannot grant it.
    async fn set_grant(
        &mut self,
        context: &RequestContext,
        authorization: Authorization,
        cartridge_id: &CartridgeId,
        grant: GrantKey,
        value: Option<GrantBody>,
    ) -> Res<()> {
        self.install_record(cartridge_id)?;
        let holds = match (&grant, &value) {
            (GrantKey::Drawer(drawer), Some(_)) => {
                self.hold_drawer(context, authorization, cartridge_id, drawer)
                    .await?;
                true
            }
            _ => false,
        };
        if value.is_none() {
            // A revoke takes back an "only this time" answer too; the reconcile the edit runs
            // below re-mints without it.
            self.grant_requests
                .revoke_boot_grants(cartridge_id, Some(&grant));
        }
        let edited = self
            .edit_cartridge_config(context, cartridge_id, move |config| {
                config.set_grant(grant, value)
            })
            .await;
        if holds && edited.is_err() {
            self.release_after_failed_grant(context).await?;
        }
        edited
    }

    /// Deletes the settings and every grant of the cartridge, installed or not: the config
    /// document outlives the install.
    async fn clear_config(
        &mut self,
        context: &RequestContext,
        cartridge_id: &CartridgeId,
    ) -> Res<()> {
        self.grant_requests.revoke_boot_grants(cartridge_id, None);
        self.edit_cartridge_config(context, cartridge_id, |config| {
            config.clear();
            Ok(())
        })
        .await?;
        // The document is empty now, which the reconcile above saw only if the cartridge is
        // installed: an uninstalled cartridge's document is not loaded there.
        let held: Vec<(DrawerId, CartridgeId)> = self
            .service_drawers
            .iter()
            .filter(|((_, holder), origin)| {
                holder == cartridge_id && **origin == DrawerOrigin::Granted
            })
            .map(|(pair, _)| pair.clone())
            .collect();
        self.end_holds(context, held).await
    }

    /// Edits `cartridge_id`'s config document, the one its install names or else the one an
    /// earlier install left, and reconciles. A write there is not the node document's, so it
    /// reconciles here the way the node's own mutations do.
    async fn edit_cartridge_config(
        &mut self,
        context: &RequestContext,
        cartridge_id: &CartridgeId,
        edit: impl FnOnce(&mut CartridgeConfig) -> Result<(), InventoryError> + Send + 'static,
    ) -> Res<()> {
        let doc = self
            .config_doc(cartridge_id)
            .await?
            .ok_or_else(|| NodulesError::UnknownCartridge(cartridge_id.clone()))?;
        let handle = self
            .docs
            .handle(cartridge_id, &doc)
            .await
            .map_err(crate::store_error)?;
        mutate_domain(&handle, context, edit).await?;
        let snapshot = self.config().await?;
        drop(self.reconcile(context, snapshot, Trigger::Change).await?);
        Ok(())
    }

    /// Answers a pending request. An allow or an only-this-boot answer re-mints the cartridge's
    /// handles (the reconcile every grant change runs) before the request reads as answered, so
    /// whoever awaits it finds the access in place.
    async fn answer_grant_request(
        &mut self,
        context: &RequestContext,
        authorization: Authorization,
        request: Uuid7,
        answer: GrantAnswer,
        drawer: Option<DrawerId>,
    ) -> Res<GrantRequestState> {
        let record = self.grant_requests.pending(request)?;
        let cartridge_id = record.cartridge_id.clone();
        let state = match answer {
            GrantAnswer::Decline => {
                self.grant_requests.decline(&record);
                return Ok(GrantRequestState::Declined);
            }
            GrantAnswer::Allow => {
                let (grant, body) = GrantRequests::grant_for(&record, drawer)?;
                let holds = match &grant {
                    GrantKey::Drawer(drawer) => {
                        self.hold_drawer(context, authorization, &cartridge_id, drawer)
                            .await?;
                        true
                    }
                    _ => false,
                };
                let edited = self
                    .edit_cartridge_config(context, &cartridge_id, move |config| {
                        // An answer never narrows: a wider grant of the same thing stays.
                        let access = |body: &GrantBody| body.access.as_ref().and_then(|a| a.known());
                        let wider = config.grants().any(|(key, slot)| {
                            *key == grant
                                && matches!(slot, DatumSlot::Accepted { value, .. } if access(value) >= access(&body))
                        });
                        if wider {
                            return Ok(());
                        }
                        config.set_grant(grant, Some(body))
                    })
                    .await;
                if holds && edited.is_err() {
                    self.release_after_failed_grant(context).await?;
                }
                edited?;
                GrantRequestState::Allowed
            }
            GrantAnswer::OnlyThisBoot => {
                let (grant, body) = GrantRequests::grant_for(&record, drawer)?;
                if let GrantKey::Drawer(drawer) = &grant {
                    self.hold_drawer(context, authorization, &cartridge_id, drawer)
                        .await?;
                }
                self.grant_requests
                    .grant_this_boot(cartridge_id, grant, body);
                let snapshot = self.config().await?;
                drop(self.reconcile(context, snapshot, Trigger::Change).await?);
                GrantRequestState::OnlyThisBoot
            }
        };
        self.grant_requests.settle(request, state);
        Ok(state)
    }

    async fn set_dev_takeover(
        &mut self,
        context: &RequestContext,
        cartridge_id: &CartridgeId,
        by: Option<CartridgeId>,
    ) -> Res<NoduleRecord> {
        // Clearing is always allowed, which is how a slot refused for a stray takeover recovers.
        if by.is_some() && !self.stores.allow_slot_takeover {
            return Err(NodulesError::Rejected(
                "dev slot takeover is off: set `dev.allow_slot_takeover` in node.json".into(),
            ));
        }
        let target = cartridge_id.clone();
        let cache = Arc::clone(&self.admission_cache);
        self.mutate_config(context, move |config| {
            config.set_dev_takeover(&cache, &target, by)
        })
        .await?;
        self.records
            .get(cartridge_id)
            .cloned()
            .ok_or_else(|| NodulesError::UnknownCartridge(cartridge_id.clone()))
    }

    async fn set_method_route(
        &mut self,
        context: &RequestContext,
        pattern: String,
        provider: Option<String>,
    ) -> Res<()> {
        self.mutate_config(context, move |config| config.set_route(&pattern, provider))
            .await
    }

    /// Re-pins `binding` of `cartridge_id` to the cartridge that provides its contract now, which the
    /// operator has seen reported as [`Unresolved::ProviderChanged`] and accepts (ADR 028 §3).
    /// Refused when the request is not held at that state, so a confirmation never takes effect
    /// on anything but what was reported.
    async fn confirm_resolution(
        &mut self,
        context: &RequestContext,
        cartridge_id: &CartridgeId,
        binding: &str,
    ) -> Res<NoduleRecord> {
        self.refresh_if_needed(context).await?;
        let record = self
            .records
            .get(cartridge_id)
            .ok_or_else(|| NodulesError::UnknownCartridge(cartridge_id.clone()))?;
        let Some(RequestStatus {
            contract,
            unresolved: Some(Unresolved::ProviderChanged { .. }),
            ..
        }) = record.requests.get(binding).cloned()
        else {
            return Err(NodulesError::Rejected(format!(
                "request `{binding}` of `{cartridge_id}` is not awaiting confirmation of a changed \
                 provider"
            )));
        };
        let snapshot = self.config().await?;
        // `refresh_if_needed` brought the records to this snapshot, so the recorded provider
        // change is still what the offers say, and the one offer is the cartridge to pin.
        let resolved = snapshot.value.resolve(&self.admission_cache);
        let index = ProviderIndex::new(&resolved, snapshot.value.routes().0);
        let Some(NoduleResolution::Admitted { installed, .. }) = resolved.get(cartridge_id) else {
            unreachable!("a request awaiting confirmation belongs to an admitted nodule")
        };

        let request = installed
            .resolved
            .provider_requests()
            .into_iter()
            .find(|request| request.binding == binding)
            .expect("the record reports a status for each request of its nodule");
        let offers = index.offers(
            &contract,
            self.stores
                .native_offers(&contract, request.is_operation_call()),
        );
        let [offer] = offers.as_slice() else {
            unreachable!("a provider change is held only while exactly one provider offers it")
        };
        let mut locks = self.locks.get(cartridge_id).cloned().unwrap_or_default();
        locks.insert(binding.to_owned(), offer.clone());
        self.set_locks(cartridge_id, locks).await?;
        drop(self.reconcile(context, snapshot, Trigger::Change).await?);
        self.records
            .get(cartridge_id)
            .cloned()
            .ok_or_else(|| NodulesError::UnknownCartridge(cartridge_id.clone()))
    }

    /// Records `locks` as the lockfile of `cartridge_id`, in SQLite first, when they differ from what
    /// is recorded.
    async fn set_locks(
        &mut self,
        cartridge_id: &CartridgeId,
        locks: BTreeMap<String, LockedProvider>,
    ) -> Res<()> {
        let held = self.locks.get(cartridge_id);
        if held.is_none_or(BTreeMap::is_empty) && locks.is_empty() || held == Some(&locks) {
            return Ok(());
        }
        store::replace_resolution_locks(&self.stores.sql, cartridge_id, &locks).await?;
        if locks.is_empty() {
            self.locks.remove(cartridge_id);
        } else {
            self.locks.insert(cartridge_id.clone(), locks);
        }
        Ok(())
    }

    async fn uninstall(&mut self, context: &RequestContext, cartridge_id: &CartridgeId) -> Res<()> {
        let target = cartridge_id.clone();
        let domain_error: Arc<Mutex<Option<NodulesError>>> = Arc::new(Mutex::new(None));
        let captured = Arc::clone(&domain_error);
        let result = self
            .stores
            .datum_store
            .mutate(context, None, move |config| {
                config.uninstall(&target).map_err(|err| {
                    let message = err.to_string();
                    *captured.lock().expect("domain error lock") = Some(err.into());
                    StoreError::InvalidEdit(message)
                })
            })
            .await;
        if let Err(err) = result {
            if let Some(domain) = domain_error.lock().expect("domain error lock").take() {
                return Err(domain);
            }
            return Err(crate::store_error(err));
        }
        self.grant_requests.forget(cartridge_id);
        let snapshot = self.config().await?;
        let revoke_errors = self.reconcile(context, snapshot, Trigger::Change).await?;
        if let Some(err) = revoke_errors.get(cartridge_id) {
            return Err(err.clone());
        }
        Ok(())
    }

    async fn refresh_if_needed(&mut self, context: &RequestContext) -> Res<()> {
        let snapshot = self.config().await?;
        let has_unapplied = self
            .records
            .values()
            .any(|record| record.applied_heads.as_ref() != Some(&record.observed_heads));
        // Config heads do not move when an applied grant's source capability is retracted
        // elsewhere (for instance when the nodules actor loses Admin on the drawer), so the
        // records would keep reporting `granted: true`. Check only those capabilities' liveness
        // and run the full reconcile, which re-applies or re-reports the grant, just when one
        // died.
        if snapshot.heads != self.observed_heads
            || has_unapplied
            || self.any_applied_grant_dead(context).await?
        {
            drop(self.reconcile(context, snapshot, Trigger::Change).await?);
        }
        Ok(())
    }

    /// The cartridge's refusal attribution, which its executor keeps and the next reconcile
    /// updates in place.
    fn missing_authority(&mut self, cartridge_id: &CartridgeId) -> MissingAuthorityLookup {
        self.missing_authority
            .entry(cartridge_id.clone())
            .or_insert_with(|| {
                self.grant_requests
                    .lookup(cartridge_id.clone(), self.commands.clone())
            })
            .clone()
    }

    async fn any_applied_grant_dead(&self, context: &RequestContext) -> Res<bool> {
        let grant_caps = self
            .applied_grants
            .values()
            .flat_map(BTreeMap::values)
            .map(|(_, cap_id)| cap_id);
        let operation_caps = self.applied_operations.values().map(|(_, cap_id)| cap_id);
        let routine_caps = self
            .routine_capabilities
            .values()
            .flat_map(BTreeMap::values)
            .flat_map(AppliedRoutine::handles)
            .collect::<Vec<_>>();
        for cap_id in grant_caps
            .copied()
            .chain(operation_caps.copied())
            .chain(routine_caps)
        {
            if !self.stores.is_cap_live(context, cap_id).await? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn get(
        &mut self,
        context: &RequestContext,
        cartridge_id: &CartridgeId,
    ) -> Res<NoduleRecord> {
        self.refresh_if_needed(context).await?;
        self.records
            .get(cartridge_id)
            .cloned()
            .ok_or_else(|| NodulesError::UnknownCartridge(cartridge_id.clone()))
    }

    async fn list(
        &mut self,
        context: &RequestContext,
        scope: Option<NoduleScope>,
    ) -> Res<Vec<NoduleRecord>> {
        self.refresh_if_needed(context).await?;
        Ok(self
            .records
            .iter()
            .filter(|(cartridge_id, _)| {
                scope
                    .as_ref()
                    .is_none_or(|scope| scope.cartridges.contains(cartridge_id))
            })
            .map(|(_, record)| record.clone())
            .collect())
    }

    async fn reconcile(
        &mut self,
        context: &RequestContext,
        snapshot: StoreSnapshot<NoduleConfig>,
        trigger: Trigger,
    ) -> Res<BTreeMap<CartridgeId, NodulesError>> {
        let mut resolved = snapshot.value.resolve(&self.admission_cache);
        self.sync_schemas(&mut resolved);
        let mut revoke_errors: BTreeMap<CartridgeId, NodulesError> = BTreeMap::new();
        let active_cartridges: BTreeSet<CartridgeId> = self
            .applied_grants
            .keys()
            .chain(self.applied_operations.keys())
            .chain(self.applied_settings.keys())
            .chain(self.routine_capabilities.keys())
            .cloned()
            .collect();
        for cartridge_id in active_cartridges {
            if !matches!(
                resolved.get(&cartridge_id),
                Some(NoduleResolution::Admitted { .. })
            ) {
                // Both revocations run whatever the first reports; the first failure is the one
                // the cartridge's record carries.
                let drawers = self
                    .revoke_grants_for_cartridge(context, &cartridge_id, &BTreeMap::new())
                    .await;
                let operations = self
                    .revoke_operations_for_cartridge(
                        context,
                        &cartridge_id,
                        &DispatchScope::default(),
                    )
                    .await;
                let settings =
                    revoke_settings_for_cartridge(self, context, &cartridge_id, None).await;
                let routines = self
                    .revoke_routines_for_cartridge(context, &cartridge_id)
                    .await;
                if let Err(err) = drawers.and(operations).and(settings).and(routines) {
                    revoke_errors.insert(cartridge_id.clone(), err);
                }
                if !resolved.contains_key(&cartridge_id) {
                    self.applied_heads.remove(&cartridge_id);
                }
            }
        }

        // A cartridge that left the config (uninstall) has no resolution to unbind it from below.
        // It goes first, so its registration cannot be offered as a provider to the others.
        let removed: Vec<CartridgeId> = self
            .records
            .keys()
            .filter(|cartridge_id| !resolved.contains_key(*cartridge_id))
            .cloned()
            .collect();
        for cartridge_id in removed {
            self.withdraw(&cartridge_id).await;
            if self.init_done.remove(&cartridge_id).is_some() {
                store::delete_init_done(&self.stores.sql, &cartridge_id).await?;
            }
            self.set_locks(&cartridge_id, BTreeMap::new()).await?;
        }

        // Providers are indexed from the snapshot, not read back from the routing table as the
        // loop registers them, so a dependent that sorts before its provider resolves in the
        // same pass.
        // Routes and claims are pushed in one pass, so dispatch never routes by a table that
        // disagrees with the one dependents were resolved against.
        let (routes, unreadable) = snapshot.value.routes();
        // Reported once per distinct problem, not on every reconcile; a fixed one is forgotten
        // so it reports again if it comes back.
        for problem in &unreadable {
            if self.reported_unreadable_routes.insert(problem.clone()) {
                tracing::error!(%problem, "a route datum in the config document is unusable and is ignored");
            }
        }
        self.reported_unreadable_routes
            .retain(|problem| unreadable.contains(problem));
        self.stores.dispatch.set_routes(routes.clone());
        let providers = ProviderIndex::new(&resolved, routes);
        self.grant_requests.declare(&resolved);
        let mut new_records = BTreeMap::new();
        for (cartridge_id, resolution) in resolved {
            match resolution {
                NoduleResolution::Refused {
                    cartridge_id,
                    version,
                    revision,
                    enabled,
                    reason,
                } => {
                    self.withdraw(&cartridge_id).await;
                    let error = match revoke_errors.get(&cartridge_id) {
                        Some(revoke_err) => format!("{reason}; {revoke_err}"),
                        None => reason,
                    };
                    new_records.insert(
                        cartridge_id.clone(),
                        NoduleRecord {
                            cartridge_id: cartridge_id.clone(),
                            version,
                            revision,
                            actor: self.actors.get(&cartridge_id).map(nodule_actor),
                            filled_by: None,
                            enabled,
                            readiness: Readiness::Refused,
                            artifacts: BTreeMap::new(),
                            drawers: BTreeMap::new(),
                            methods: None,
                            requests: BTreeMap::new(),
                            init: None,
                            observed_heads: snapshot.heads.clone(),
                            applied_heads: self.applied_heads.get(&cartridge_id).cloned(),
                            error: Some(error),
                        },
                    );
                }
                NoduleResolution::Admitted {
                    installed,
                    desired_grants,
                    desired_operations,
                    desired_nets,
                } => {
                    let record = self
                        .reconcile_admitted(
                            context,
                            &snapshot.heads,
                            trigger,
                            &providers,
                            installed,
                            desired_grants,
                            desired_operations,
                            desired_nets,
                        )
                        .await?;
                    new_records.insert(cartridge_id, record);
                }
            }
        }

        self.installs.send_if_modified(|rows| {
            let installs = snapshot.value.installs();
            (*rows != installs) && {
                *rows = installs;
                true
            }
        });
        self.grant_requests
            .publish_grants(snapshot.value.cartridge_docs());
        self.observed_heads = snapshot.heads;
        self.records = new_records;
        // A revoked grant ends the service's hold on a drawer its cartridge's document no longer
        // names (a clear does so in `clear_config`; an uninstall leaves the document and so the
        // hold). Last, so the cartridges' own grants of it are retracted first.
        self.release_idle_drawers(context, &snapshot.value).await?;
        Ok(revoke_errors)
    }

    /// Brings the schema repo to what `resolved` enables: an enabled, admitted nodule's held
    /// datum records are registered under its cartridge id (so it can write its own datums and
    /// others can read them), and everything of a cartridge that is disabled, refused or gone is
    /// retired. A registration the repo refuses (a tag the node already knows, or a Lexicon that
    /// does not compile) refuses the nodule with the repo's reason, the same way an authority
    /// conflict does: cartridges are external input and this is where their Lexicons are first
    /// compiled.
    fn sync_schemas(&mut self, resolved: &mut BTreeMap<CartridgeId, NoduleResolution>) {
        let stale: Vec<CartridgeId> = self
            .registered_schemas
            .keys()
            .filter(|cartridge_id| {
                !matches!(
                    resolved.get(*cartridge_id),
                    Some(NoduleResolution::Admitted { installed, .. }) if installed.enabled
                )
            })
            .cloned()
            .collect();
        for cartridge_id in stale {
            self.stores.schemas.retire(&cartridge_id.to_string());
            self.registered_schemas.remove(&cartridge_id);
        }
        let changed: Vec<CartridgeId> = resolved
            .iter()
            .filter(|(cartridge_id, resolution)| {
                matches!(
                    resolution,
                    NoduleResolution::Admitted { installed, .. }
                        if installed.enabled
                            && self.registered_schemas.get(*cartridge_id) != Some(&installed.revision)
                )
            })
            .map(|(cartridge_id, _)| cartridge_id.clone())
            .collect();
        for cartridge_id in changed {
            let Some(NoduleResolution::Admitted {
                installed,
                desired_grants,
                desired_operations,
                desired_nets,
            }) = resolved.remove(&cartridge_id)
            else {
                unreachable!("selected as admitted above");
            };
            let split = installed
                .resolved
                .registrable_datums(installed.admitted.authority.as_deref());
            let object = |file: &IndexMap<String, Value>| {
                Value::Object(file.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            };
            let group = SchemaRegistrationGroup::new(
                None,
                split.held.iter().map(|(nsid, file, sidecar)| {
                    let registration = SchemaRegistration::new(*nsid, object(file));
                    match sidecar {
                        Some(document) => registration.with_supplement(object(document)),
                        None => registration,
                    }
                }),
                split.carried.iter().map(|file| object(file)).collect(),
            );
            let outcome = self
                .stores
                .schemas
                .register(&cartridge_id.to_string(), group);
            let resolution = match outcome {
                Ok(()) => {
                    self.registered_schemas
                        .insert(cartridge_id.clone(), installed.revision);
                    NoduleResolution::Admitted {
                        installed,
                        desired_grants,
                        desired_operations,
                        desired_nets,
                    }
                }
                Err(error) => {
                    // `register` is all or nothing, so an earlier revision's schemas are still
                    // there, and a refused nodule must not keep them.
                    self.stores.schemas.retire(&cartridge_id.to_string());
                    self.registered_schemas.remove(&cartridge_id);
                    NoduleResolution::Refused {
                        cartridge_id: cartridge_id.clone(),
                        version: Some(installed.version),
                        revision: Some(installed.revision),
                        enabled: installed.enabled,
                        reason: format!("schema registration refused: {error}"),
                    }
                }
            };
            resolved.insert(cartridge_id, resolution);
        }
    }

    /// Ends a nodule's start: stops routing to it, retires its workload and abandons its
    /// initializer run, so a result still in flight finds no run and is dropped.
    async fn withdraw(&mut self, cartridge_id: &CartridgeId) {
        self.stores.dispatch.unbind(&cartridge_id.to_string());
        self.retire(cartridge_id).await;
        self.inits.remove(cartridge_id);
    }

    /// Registers `installed`'s operations in dispatch in `state`. Admission checks a Lexicon's
    /// frame and references, but only the validator compiler knows whether a position can be
    /// checked at all. A nodule whose contract cannot be enforced is not served: it is refused,
    /// as if admission had refused it, and the state registered is that refusal.
    /// Keeps `installed`'s operations routed to `provider_id` while it cannot serve them, so
    /// callers get the retryable not-ready answer instead of an unknown operation.
    fn register_not_ready(
        &self,
        provider_id: &str,
        installed: &InstalledNodule,
        readiness: Readiness,
    ) {
        self.stores
            .dispatch
            .register(
                provider_id,
                unchecked_claims(&installed.admitted.operations),
                ProviderState::NotReady(readiness),
            )
            .expect("claims without a Lexicon compile nothing");
    }

    fn register_claims(
        &self,
        provider_id: &str,
        installed: &InstalledNodule,
        state: ProviderState,
    ) -> ProviderState {
        match self
            .stores
            .dispatch
            .register(provider_id, claims(installed, &state), state.clone())
        {
            Ok(()) => state,
            Err(error) => {
                tracing::error!(cartridge_id = %installed.cartridge_id, %error, "a nodule's method Lexicon does not compile");
                let refused = ProviderState::NotReady(Readiness::Refused);
                self.stores
                    .dispatch
                    .register(
                        provider_id,
                        unchecked_claims(&installed.admitted.operations),
                        refused.clone(),
                    )
                    .expect("claims without a Lexicon compile nothing");
                refused
            }
        }
    }

    /// Stops the executor hosting `cartridge_id`, if it was. Dispatch must already have stopped
    /// routing to it.
    async fn retire(&mut self, cartridge_id: &CartridgeId) {
        if self.hosted.remove(cartridge_id).is_some()
            && let Some(executor) = &self.stores.executor
        {
            executor.retire(cartridge_id).await;
        }
    }

    /// Makes an admitted nodule's `implements` visible through dispatch under its cartridge id and
    /// returns the readiness both dispatch and the nodule's record report.
    ///
    /// A disabled nodule is unbound instead: two providers of one operation are ambiguous, and
    /// disabling one is how an operator resolves that, which a registered-but-disabled claim
    /// would keep blocking. An enabled one with a handler is ensured with the executor, if there
    /// is one, and registered ready or not ready as the executor says, with the handler's
    /// artifacts marked available unless the executor lacks their bytes. Otherwise it is
    /// registered not ready with the readiness the inventory alone can tell.
    ///
    /// A nodule the executor hosts is registered ready only once its initializers have all
    /// succeeded in this start and revision ([`Self::advance_inits`]); until then it is
    /// registered `Initializing` or `InitFailed`, and callers get the retryable not-ready answer.
    ///
    /// Registering is a no-op when nothing changed and `ensure` is idempotent, so every
    /// reconcile may call this.
    async fn serve(
        &mut self,
        installed: &mut InstalledNodule,
        actor: &OpenedActor,
        routine_capabilities: BTreeMap<String, RoutineCapabilities>,
        granted: &DispatchScope,
        requests: &BTreeMap<String, RequestStatus>,
        trigger: Trigger,
    ) -> Readiness {
        let cartridge_id = installed.cartridge_id.clone();
        let provider_id = cartridge_id.to_string();
        // Another revision is another run: abandon the old one now, so none of its results can
        // land even while the new revision's handler is not hosted yet.
        if self
            .inits
            .get(&cartridge_id)
            .is_some_and(|run| run.revision != installed.revision)
        {
            self.inits.remove(&cartridge_id);
        }
        // A nodule with routines is hosted by an executor; one without implements nothing.
        let hosting = (installed.enabled && !installed.nodule.routines.is_empty())
            .then(|| self.stores.executor.as_ref().map(Arc::clone))
            .flatten();
        if !installed.enabled {
            self.withdraw(&cartridge_id).await;
            return Readiness::Disabled;
        }
        // A required operation with no covering grant is configuration the nodule is waiting
        // for, not a fault (Clay's "needs configuration"): nothing is hosted or initialized
        // until it is granted, and callers get the retryable not-ready.
        if !installed.resolved.unmet_requests(granted).is_empty() {
            self.retire(&cartridge_id).await;
            self.register_not_ready(&provider_id, installed, Readiness::NeedsConfiguration);
            return Readiness::NeedsConfiguration;
        }
        // Likewise a required operation nobody (or more than one, or the wrong cartridge) provides:
        // enabling is accepted and the nodule waits, as a pod waits on a missing dependency. No
        // waiter needs waking when a provider turns up: resolution is recomputed for every
        // nodule on each reconcile, and a provider can only appear through one (a config
        // change, since every nodule's enablement lives in the config document).
        if !installed.resolved.missing_providers(requests).is_empty() {
            self.retire(&cartridge_id).await;
            self.register_not_ready(&provider_id, installed, Readiness::ProviderMissing);
            return Readiness::ProviderMissing;
        }
        let Some(executor) = hosting else {
            self.retire(&cartridge_id).await;
            let readiness = installed.readiness();
            if readiness.is_ready() {
                // No handler: it implements nothing, so there is no provider to register, and
                // no initializer either (a target is one of its own operations).
                self.stores.dispatch.unbind(&provider_id);
                return readiness;
            }
            self.register_not_ready(&provider_id, installed, readiness);
            return readiness;
        };
        // Upgrading to another revision replaces the workload: the old one stops being routed
        // to and is retired before the new one is ensured (an executor hosts one workload per
        // cartridge, so the two cannot overlap). Ensuring can take as long as compiling the
        // new handler, so the operations stay registered meanwhile, not ready, and callers get
        // the retryable answer instead of an unknown operation; the registration after
        // `ensure` replaces it in one step.
        if self
            .hosted
            .get(&cartridge_id)
            .is_some_and(|hosted| *hosted != installed.revision)
        {
            self.register_not_ready(&provider_id, installed, Readiness::Initializing);
            self.retire(&cartridge_id).await;
        }
        let components = installed
            .artifacts
            .keys()
            .map(|key| {
                let component = installed
                    .resolved
                    .components
                    .get(key)
                    .expect("admission resolved every routine component");
                (key.clone(), component.clone())
            })
            .collect();
        let mut routines = BTreeMap::new();
        for (routine_key, routine) in &installed.nodule.routines {
            let big_cartridges_core::RoutineImpl::ComponentExport(export) = &routine.r#impl else {
                unreachable!("admission accepted an unknown routine implementation")
            };
            let operation = routine.implements.to_string();
            assert!(
                routines
                    .insert(
                        operation,
                        RoutineSpec {
                            routine_key: routine_key.clone(),
                            component: export.component.clone(),
                            export: export.export.clone(),
                        },
                    )
                    .is_none(),
                "each operation must have exactly one implementing routine"
            );
        }
        let spec = HandlerSpec {
            cartridge_id: cartridge_id.clone(),
            revision: installed.revision,
            components,
            routines,
            actor: actor.clone(),
            routine_capabilities: routine_capabilities.clone(),
            config_doc: installed.config_doc.clone(),

            reensure: self.reensure.handle(cartridge_id.clone()),
            missing_authority: self.missing_authority(&cartridge_id),
        };
        self.hosted.insert(cartridge_id.clone(), installed.revision);
        let state = match executor.ensure(&spec).await {
            // The runtime calls a local initializer on the provider directly, so the provider is
            // used here even while dispatch still answers not ready.
            Hosted::Ready(provider) => match self
                .advance_inits(installed, Arc::clone(&provider), trigger)
                .await
            {
                None => ProviderState::Ready(ProviderKind::Executor {
                    kind: executor.kind(),
                    provider,
                }),
                Some(pending) => ProviderState::NotReady(pending),
            },
            Hosted::NotReady(readiness) => {
                assert!(
                    !readiness.is_ready(),
                    "executor `{}` reported `{provider_id}` ready without a provider",
                    executor.kind()
                );
                ProviderState::NotReady(readiness)
            }
        };
        if state.readiness() != Readiness::ArtifactsUnavailable {
            for (_, artifact_state) in installed.artifacts.values_mut() {
                *artifact_state = ArtifactState::Available;
            }
        }

        let state = self.register_claims(&provider_id, installed, state);
        state.readiness()
    }

    /// Moves the nodule's initializers along and returns `None` when every one has succeeded in
    /// this start and revision, otherwise the readiness it is held at.
    ///
    /// They run one at a time in declaration order. Each invocation is a task off this actor
    /// (the actor never awaits a handler) whose result returns as an [`InitFinished`] message; a
    /// failed one stays failed until a reconcile other than its own result's retries it, with no
    /// backoff yet. `provider` is the hosted handler, which the target (always an operation of
    /// the cartridge's own routines, by admission) is called on directly: the host running its
    /// own lifecycle hook, as the nodule service. The executor delivers that call with the
    /// capabilities of the routine implementing the target.
    async fn advance_inits(
        &mut self,
        installed: &InstalledNodule,
        provider: Arc<dyn Provider>,
        trigger: Trigger,
    ) -> Option<Readiness> {
        let cartridge_id = &installed.cartridge_id;
        let inits = &installed.nodule.initialization;
        if inits.is_empty() {
            return None;
        }
        if !self.inits.contains_key(cartridge_id) {
            let done = inits
                .iter()
                .filter(|init| {
                    init.mode == RunMode::Once
                        && self
                            .init_done
                            .get(cartridge_id)
                            .and_then(|keys| keys.get(&init.key))
                            == Some(&installed.revision)
                })
                .map(|init| init.key.clone())
                .collect();
            self.next_epoch += 1;
            self.inits.insert(
                cartridge_id.clone(),
                InitRun {
                    epoch: self.next_epoch,
                    revision: installed.revision,
                    done,
                    running: None,
                    attempts: 0,
                    error: None,
                    failed: false,
                },
            );
        }
        let run = self
            .inits
            .get_mut(cartridge_id)
            .expect("run inserted above");
        let init = inits.iter().find(|init| !run.done.contains(&init.key))?;
        let init_key = &init.key;
        if run.running.is_some() {
            return Some(Readiness::Initializing);
        }
        if run.failed && trigger == Trigger::InitResult {
            return Some(Readiness::InitFailed);
        }

        // The executor delivers the call with the capabilities of the routine that implements
        // the target, which is exactly one.
        assert_eq!(
            installed
                .nodule
                .routines
                .values()
                .filter(|routine| routine.implements == init.target)
                .count(),
            1,
            "an initializer target has exactly one implementing routine"
        );
        let caller = Caller {
            authorization: Authorization::identity(self.stores.parent.capability),
            actor: self.stores.parent.actor.actor_id,
            scope: None,
        };

        let run = self.inits.get_mut(cartridge_id).expect("run present");
        run.attempts += 1;
        run.failed = false;
        run.running = Some(init_key.clone());
        let context = InvocationContext {
            idempotency_key: Some(format!(
                "nodule/{cartridge_id}/{}/{}",
                installed.revision, init_key
            )),
            ..Default::default()
        };
        let finished = InitFinished {
            cartridge_id: cartridge_id.clone(),
            epoch: run.epoch,
            key: init_key.clone(),
            once: init.mode == RunMode::Once,
            outcome: Ok(()),
        };
        let target = init.target.to_string();
        let invocation = async move {
            let outcome = provider
                .call(
                    &caller,
                    &target,
                    &RequestContext::root(),
                    &context,
                    serde_json::json!({}),
                    None,
                )
                .await
                .map(drop)
                .map_err(|error| match error {
                    ProviderError::InvalidInput(reason) => {
                        format!("`{target}` refused the input: {reason}")
                    }
                    ProviderError::Failed { status, body } => {
                        format!("`{target}` failed with status {status}: {body}")
                    }
                });
            InitFinished {
                outcome,
                ..finished
            }
        };
        self.spawn_init(invocation);
        Some(Readiness::Initializing)
    }

    /// Runs an invocation off the actor and sends its result back. Shutdown cancels the
    /// invocation rather than waiting for a handler, and the actor stops reading results once it
    /// is cancelled, so a send can only fail after that.
    fn spawn_init(&self, invocation: impl Future<Output = InitFinished> + Send + 'static) {
        let results = self.init_results.clone();
        let cancel = self.tasks.clone();
        let task = async move {
            let finished = tokio::select! {
                biased;
                () = cancel.cancelled() => return,
                finished = invocation => finished,
            };
            tokio::select! {
                biased;
                () = cancel.cancelled() => {}
                sent = results.send(finished) => sent.expect(ERROR_ACTOR),
            }
        };
        // `None` means shutdown already closed the service: the run stays as it is and the
        // process is going away.
        drop(self.tasks.spawn(task));
    }

    /// What the record reports of the nodule's initializers: nothing until one was invoked, and
    /// nothing again once the one that was failing has succeeded.
    fn init_status(&self, cartridge_id: &CartridgeId) -> Option<NoduleInitStatus> {
        self.inits
            .get(cartridge_id)
            .filter(|run| run.attempts > 0 || run.error.is_some())
            .map(|run| NoduleInitStatus {
                attempts: run.attempts,
                error: run.error.clone(),
            })
    }

    /// Applies an initializer's result, then reconciles so the router and the records show it
    /// and the next initializer starts. A result whose run was replaced meanwhile is dropped.
    async fn init_finished(&mut self, finished: InitFinished) -> Res<()> {
        let Some(run) = self.inits.get_mut(&finished.cartridge_id) else {
            return Ok(());
        };
        if run.epoch != finished.epoch {
            return Ok(());
        }
        assert_eq!(
            run.running.take().as_deref(),
            Some(finished.key.as_str()),
            "a result names the invocation in flight"
        );
        match finished.outcome {
            Ok(()) => {
                run.attempts = 0;
                run.error = None;
                run.done.insert(finished.key.clone());
                if finished.once {
                    let revision = run.revision;
                    store::upsert_init_done(
                        &self.stores.sql,
                        &finished.cartridge_id,
                        &finished.key,
                        revision,
                    )
                    .await?;
                    self.init_done
                        .entry(finished.cartridge_id.clone())
                        .or_default()
                        .insert(finished.key, revision);
                }
            }
            Err(error) => {
                run.failed = true;
                run.error = Some(error);
            }
        }
        let snapshot = self.config().await?;
        drop(
            self.reconcile(&RequestContext::root(), snapshot, Trigger::InitResult)
                .await?,
        );
        Ok(())
    }

    /// Resolves the operation requests of `installed` to providers and keeps the lockfile in
    /// step with the outcome.
    ///
    /// A disabled nodule has no resolution and no lockfile: enabling is the consent to the
    /// providers found then, so it begins afresh each time. For an enabled one every operation
    /// request gets a status, and a request that resolved is (re)pinned to the provider it
    /// resolved to. A request that did not keeps its earlier pin, so the same cartridge coming back
    /// resolves without asking, and a different one is reported as a change. A pin for a binding
    /// the nodule no longer has, or for another contract than the binding now names, is dropped.
    async fn resolve_requests(
        &mut self,
        installed: &InstalledNodule,
        providers: &ProviderIndex,
    ) -> Res<BTreeMap<String, RequestStatus>> {
        let cartridge_id = &installed.cartridge_id;
        if !installed.enabled {
            self.set_locks(cartridge_id, BTreeMap::new()).await?;
            return Ok(BTreeMap::new());
        }
        let mut statuses = BTreeMap::new();
        for request in installed.resolved.provider_requests() {
            let pinned = self
                .locks
                .get(cartridge_id)
                .and_then(|locks| locks.get(&request.binding))
                .filter(|pin| pin.contract == request.contract)
                // A route is the operator's consent, so a routed request re-pins to whatever
                // the route names instead of waiting on a confirmation.
                .filter(|_| !providers.is_routed(&request.contract));
            if let Some(reason) = providers.route_problem(&request.contract) {
                statuses.insert(
                    request.binding.clone(),
                    RequestStatus {
                        contract: request.contract.clone(),
                        locked: pinned.cloned(),
                        unresolved: Some(Unresolved::RouteInvalid {
                            reason: reason.to_owned(),
                        }),
                    },
                );
                continue;
            }
            let offers = providers.offers(
                &request.contract,
                self.stores
                    .native_offers(&request.contract, request.is_operation_call()),
            );
            statuses.insert(
                request.binding.clone(),
                RequestStatus::evaluate(&request, &offers, pinned),
            );
        }
        let locks = statuses
            .iter()
            .filter_map(|(binding, status)| Some((binding.clone(), status.locked.clone()?)))
            .collect();
        self.set_locks(cartridge_id, locks).await?;
        Ok(statuses)
    }

    async fn ensure_nodule_actor(
        &mut self,
        context: &RequestContext,
        cartridge_id: &CartridgeId,
    ) -> Res<OpenedActor> {
        if let Some(actor) = self.actors.get(cartridge_id) {
            return Ok(actor.clone());
        }
        let opened = self.stores.open_actor(context, cartridge_id).await?;
        if let Some(saved_actor_id) = self.persisted_actors.get(cartridge_id) {
            if opened.actor.actor_id != *saved_actor_id {
                return Err(NodulesError::Storage(format!(
                    "installed nodule `{cartridge_id}` was given actor {saved_actor_id} but its path now opens {}",
                    opened.actor.actor_id
                )));
            }
        } else {
            store::upsert_actor(&self.stores.sql, cartridge_id, opened.actor.actor_id).await?;
            self.persisted_actors
                .insert(cartridge_id.clone(), opened.actor.actor_id);
        }
        self.actors.insert(cartridge_id.clone(), opened.clone());
        Ok(opened)
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "one nodule's slice of a reconcile: the pass's snapshot facts plus the nodule's resolved pieces"
    )]
    async fn reconcile_admitted(
        &mut self,
        context: &RequestContext,
        heads: &[ChangeHash],
        trigger: Trigger,
        providers: &ProviderIndex,
        mut installed: InstalledNodule,
        desired_grants: BTreeMap<DrawerId, DrawerAccess>,
        desired_operations: DispatchScope,
        desired_nets: BTreeSet<NetTarget>,
    ) -> Res<NoduleRecord> {
        let cartridge_id = installed.cartridge_id.clone();
        let actor = self.ensure_nodule_actor(context, &cartridge_id).await?;
        // The handles are minted (or revoked) before the handler is ensured, so the executor is
        // handed the handles that are live now. A disabled nodule has none: it is not serving,
        // and re-enabling it mints fresh ones.
        // The manifest bounds what configuration can confer: only the grants that cover a
        // requested operation are minted, as those exact operations. An entry covering no
        // request is reported on the record rather than applied, which is what keeps "can X
        // ever reach Y" answerable from the manifest alone.
        let (bound_operations, unrequested_operations) =
            installed.resolved.bind_operations(&desired_operations);
        let effective_operations = if installed.enabled {
            bound_operations.clone()
        } else {
            DispatchScope::default()
        };
        let (routine_capabilities, operations_error, settings_error, granted_operations) =
            apply_nodule_capabilities(
                self,
                context,
                &cartridge_id,
                &actor,
                &installed,
                Granted::new(&desired_grants, &effective_operations, &desired_nets),
            )
            .await?;

        let requests = self.resolve_requests(&installed, providers).await?;
        let readiness = serve_nodule(
            self,
            &mut installed,
            &actor,
            routine_capabilities,
            &granted_operations,
            &requests,
            trigger,
        )
        .await;
        let mut apply_errors = collect_apply_errors(
            self,
            context,
            &cartridge_id,
            &desired_grants,
            &operations_error,
            &settings_error,
        )
        .await?;
        let mut drawer_statuses = BTreeMap::new();
        let granted = |desired| NoduleGrantStatus {
            desired,
            granted: true,
            error: None,
        };
        for (drawer_id, desired_access) in &desired_grants {
            let existing = self
                .applied_grants
                .get(&cartridge_id)
                .and_then(|grants| grants.get(drawer_id))
                .copied();
            if let Some((applied_access, cap_id)) = existing
                && applied_access == *desired_access
            {
                let live = self.stores.is_cap_live(context, cap_id).await?;
                if live {
                    if trigger == Trigger::Boot {
                        if self
                            .stores
                            .grant_with_cap(
                                context,
                                cap_id,
                                drawer_id,
                                actor.actor.actor_id,
                                *desired_access,
                                false,
                            )
                            .await
                            .is_ok()
                        {
                            drawer_statuses.insert(drawer_id.clone(), granted(*desired_access));
                            continue;
                        }
                        drop(self.stores.retract_grant_cap(context, cap_id).await);
                    } else {
                        drawer_statuses.insert(drawer_id.clone(), granted(*desired_access));
                        continue;
                    }
                }
                store::delete_applied_grant(&self.stores.sql, &cartridge_id, drawer_id).await?;
                if let Some(grants) = self.applied_grants.get_mut(&cartridge_id) {
                    grants.remove(drawer_id);
                }
            }

            match self
                .stores
                .apply_grant(
                    &self.stores.sql,
                    context,
                    &cartridge_id,
                    drawer_id,
                    actor.actor.actor_id,
                    *desired_access,
                )
                .await
            {
                Ok(cap_id) => {
                    self.applied_grants
                        .entry(cartridge_id.clone())
                        .or_default()
                        .insert(drawer_id.clone(), (*desired_access, cap_id));
                    drawer_statuses.insert(drawer_id.clone(), granted(*desired_access));
                }
                Err(NodulesError::Storage(err)) => return Err(NodulesError::Storage(err)),
                Err(err) => {
                    let msg = err.to_string();
                    apply_errors.push(format!("drawer `{drawer_id}`: {msg}"));
                    drawer_statuses.insert(
                        drawer_id.clone(),
                        NoduleGrantStatus {
                            desired: *desired_access,
                            granted: false,
                            error: Some(msg),
                        },
                    );
                }
            }
        }

        let error = if apply_errors.is_empty() {
            self.applied_heads
                .insert(cartridge_id.clone(), heads.to_vec());
            None
        } else {
            Some(apply_errors.join("; "))
        };

        Ok(NoduleRecord {
            cartridge_id: installed.cartridge_id.clone(),
            version: Some(installed.version.clone()),
            revision: Some(installed.revision),
            actor: Some(nodule_actor(&actor)),
            filled_by: installed.filled_by.clone(),
            enabled: installed.enabled,
            readiness,
            artifacts: installed.artifact_states(),
            drawers: drawer_statuses,
            methods: (!desired_operations.is_empty()).then_some(NoduleMethodGrantStatus {
                desired: bound_operations,
                unrequested: unrequested_operations,
                granted: operations_error.is_none() && !granted_operations.operations.is_empty(),
                error: operations_error,
            }),
            requests,
            init: self.init_status(&cartridge_id),
            observed_heads: heads.to_vec(),
            applied_heads: self.applied_heads.get(&cartridge_id).cloned(),
            error,
        })
    }

    /// Retracts the nodule's dispatch handle unless it was minted for exactly `keep`, and
    /// forgets it once retracted.
    async fn revoke_operations_for_cartridge(
        &mut self,
        context: &RequestContext,
        cartridge_id: &CartridgeId,
        keep: &DispatchScope,
    ) -> Res<()> {
        let Some((applied, cap_id)) = self.applied_operations.get(cartridge_id).cloned() else {
            return Ok(());
        };
        if applied == *keep {
            return Ok(());
        }
        self.stores.retract_grant_cap(context, cap_id).await?;
        store::delete_applied_operations(&self.stores.sql, cartridge_id).await?;
        self.applied_operations.remove(cartridge_id);
        Ok(())
    }

    /// Retracts every routine handle of the cartridge and forgets them.
    async fn revoke_routines_for_cartridge(
        &mut self,
        context: &RequestContext,
        cartridge_id: &CartridgeId,
    ) -> Res<()> {
        let Some(routines) = self.routine_capabilities.get(cartridge_id) else {
            return Ok(());
        };
        for capability in routines.values().flat_map(AppliedRoutine::handles) {
            self.stores.retract_grant_cap(context, capability).await?;
        }
        self.routine_capabilities.remove(cartridge_id);
        if let Some(lookup) = self.missing_authority.remove(cartridge_id) {
            lookup.set(Default::default());
        }
        Ok(())
    }

    async fn revoke_grants_for_cartridge(
        &mut self,
        context: &RequestContext,
        cartridge_id: &CartridgeId,
        keep: &BTreeMap<DrawerId, DrawerAccess>,
    ) -> Res<()> {
        let Some(existing) = self.applied_grants.get(cartridge_id).cloned() else {
            return Ok(());
        };
        let mut first_err: Option<NodulesError> = None;
        for (drawer_id, (applied_access, cap_id)) in existing {
            if keep.get(&drawer_id) == Some(&applied_access) {
                continue;
            }
            match self.stores.retract_grant_cap(context, cap_id).await {
                Ok(()) => {
                    store::delete_applied_grant(&self.stores.sql, cartridge_id, &drawer_id).await?;
                    if let Some(grants) = self.applied_grants.get_mut(cartridge_id) {
                        grants.remove(&drawer_id);
                    }
                }
                Err(err) => {
                    if first_err.is_none() {
                        first_err = Some(err);
                    }
                }
            }
        }
        if self
            .applied_grants
            .get(cartridge_id)
            .is_some_and(BTreeMap::is_empty)
        {
            self.applied_grants.remove(cartridge_id);
        }
        match first_err {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

impl Stores {
    /// Opens the actor of each persisted nodule, which must still be the one it was given: its
    /// path derives from its cartridge id and a different actor under it means the identity
    /// tree changed beneath the install.
    async fn reopen_actors(
        &self,
        context: &RequestContext,
        persisted: &BTreeMap<CartridgeId, Uuid7>,
    ) -> Res<BTreeMap<CartridgeId, OpenedActor>> {
        let mut actors = BTreeMap::new();
        for (cartridge_id, expected_actor_id) in persisted {
            let actor = self.open_actor(context, cartridge_id).await?;
            if actor.actor.actor_id != *expected_actor_id {
                return Err(NodulesError::Storage(format!(
                    "installed nodule `{cartridge_id}` was given actor {expected_actor_id} but its path now opens {}",
                    actor.actor.actor_id
                )));
            }
            actors.insert(cartridge_id.clone(), actor);
        }
        Ok(actors)
    }

    /// The actor `cartridge_id`'s nodule acts as. The core cartridge's nodule is the micros
    /// runtime (ADR 052 §2), so it acts as the `micros` subsystem actor the node opened under
    /// `nodules`, and the micro actors it opens (`nodules/micros/<cartridge>/<micro>`) are its own
    /// subtree. Every other nodule's actor is `nodules/<cartridge segment>`, opened here (a path
    /// that exists opens to the same actor).
    async fn open_actor(
        &self,
        context: &RequestContext,
        cartridge_id: &CartridgeId,
    ) -> Res<OpenedActor> {
        if *cartridge_id == self.core_cartridge.id {
            return Ok(self.core_cartridge.actor.clone());
        }
        self.authority
            .call(
                context.child(),
                OpenActor {
                    parent: self.parent.capability,
                    path: format!(
                        "{}/{}",
                        self.parent_path,
                        store::actor_segment(cartridge_id)
                    ),
                    rights: Rights::USE,
                },
            )
            .await
            .map_err(crate::authority_error)
    }

    async fn is_cap_live(&self, context: &RequestContext, capability: CapId) -> Res<bool> {
        match self
            .authority
            .call(context.child(), InspectCapability { capability })
            .await
        {
            Ok(_) => Ok(true),
            Err(ClientError::Method(AuthorityServiceError::InvalidCapability)) => Ok(false),
            Err(error) => Err(crate::authority_error(error)),
        }
    }

    async fn retract_grant_cap(&self, context: &RequestContext, capability: CapId) -> Res<()> {
        match self
            .authority
            .call(
                context.child(),
                RetractCapability {
                    via: capability,
                    capability,
                },
            )
            .await
        {
            Ok(()) | Err(ClientError::Method(AuthorityServiceError::InvalidCapability)) => Ok(()),
            Err(error) => Err(crate::authority_error(error)),
        }
    }

    /// Mints the handle a nodule invokes dispatch with, from the service's own dispatch
    /// authority, which is bound to the nodule service's actor and carries `Rights::DELEGATE`.
    /// Naming the nodule's actor as the subject is an introduction that binds the handle to it:
    /// dispatch refuses the handle under any other actor, and retracting the actor kills it.
    async fn mint_operations(
        &self,
        context: &RequestContext,
        actor: &OpenedActor,
        scope: &DispatchScope,
    ) -> Res<CapId> {
        self.dispatch_client
            .call_with(
                context.child(),
                Authorization {
                    authority: self.dispatch_authority,
                    signer: self.parent.capability,
                }
                .metadata(),
                MintDispatchCapability {
                    scope: scope.clone(),
                    rights: Rights::USE,
                    subject: Some(actor.actor.actor_id),
                },
            )
            .await
            .map(|capability| capability.capability_id)
            .map_err(dispatch_error)
    }

    async fn grant_with_cap(
        &self,
        context: &RequestContext,
        capability: CapId,
        drawer_id: &DrawerId,
        subject_id: Uuid7,
        access: DrawerAccess,
        durable: bool,
    ) -> Res<()> {
        self.drawers
            .call(
                context.child(),
                GrantDrawer {
                    capability,
                    drawer_id: drawer_id.clone(),
                    subject_id,
                    access,
                    durable,
                },
            )
            .await
            .map_err(drawers_error)
    }

    /// Grants `drawer_id` to the service's own actor at Admin, durably, with a capability minted
    /// on `authorization`'s authority: the capability only proves the granter's right to delegate
    /// and is retracted at once, as the grant is a standing one that does not depend on it.
    async fn grant_to_service(
        &self,
        context: &RequestContext,
        authorization: Authorization,
        drawer_id: &DrawerId,
    ) -> Res<()> {
        let capability = self
            .drawers
            .call_with(
                context.child(),
                authorization.metadata(),
                MintDrawerCapability {
                    scope: DrawerScope {
                        drawers: BTreeMap::from([(drawer_id.clone(), DrawerAccess::Admin)]),
                        create: false,
                    },
                    rights: Rights::USE | Rights::DELEGATE,
                    subject: None,
                },
            )
            .await
            .map_err(drawers_error)?
            .capability_id;
        let granted = self
            .grant_with_cap(
                context,
                capability,
                drawer_id,
                self.parent.actor.actor_id,
                DrawerAccess::Admin,
                true,
            )
            .await;
        self.retract_grant_cap(context, capability).await?;
        granted
    }

    /// Withdraws the service's own actor's standing grant on `drawer_id`, which a
    /// capability minted from the actor's own authority (it holds Admin there) is enough for.
    async fn revoke_from_service(&self, context: &RequestContext, drawer_id: &DrawerId) -> Res<()> {
        let capability = self
            .drawers
            .call_with(
                context.child(),
                Authorization::identity(self.parent.capability).metadata(),
                MintDrawerCapability {
                    scope: DrawerScope {
                        drawers: BTreeMap::from([(drawer_id.clone(), DrawerAccess::Admin)]),
                        create: false,
                    },
                    rights: Rights::USE,
                    subject: None,
                },
            )
            .await
            .map_err(drawers_error)?
            .capability_id;
        let revoked = self
            .drawers
            .call(
                context.child(),
                RevokeDrawer {
                    capability,
                    drawer_id: drawer_id.clone(),
                    subject_id: self.parent.actor.actor_id,
                },
            )
            .await
            .map_err(drawers_error);
        self.retract_grant_cap(context, capability).await?;
        revoked
    }

    async fn apply_grant(
        &self,
        sql: &Database,
        context: &RequestContext,
        cartridge_id: &CartridgeId,
        drawer_id: &DrawerId,
        subject_id: Uuid7,
        access: DrawerAccess,
    ) -> Res<CapId> {
        let cap = self
            .drawers
            .call_with(
                context.child(),
                Authorization::identity(self.parent.capability).metadata(),
                MintDrawerCapability {
                    scope: DrawerScope {
                        drawers: BTreeMap::from([(drawer_id.clone(), DrawerAccess::Admin)]),
                        create: false,
                    },
                    rights: Rights::USE | Rights::DELEGATE,
                    subject: None,
                },
            )
            .await
            .map_err(drawers_error)?;
        let cap_id = cap.capability_id;
        store::upsert_applied_grant(sql, cartridge_id, drawer_id, access, cap_id).await?;
        if let Err(err) = self
            .grant_with_cap(context, cap_id, drawer_id, subject_id, access, false)
            .await
        {
            drop(self.retract_grant_cap(context, cap_id).await);
            drop(store::delete_applied_grant(sql, cartridge_id, drawer_id).await);
            return Err(err);
        }
        Ok(cap_id)
    }
}

/// The operations a provider claims, with no Lexicon: for a provider that is not called.
fn unchecked_claims(
    operations: &BTreeMap<String, Option<OperationKind>>,
) -> Vec<(String, Operation)> {
    operations
        .iter()
        .map(|(nsid, kind)| (nsid.clone(), Operation::unchecked(*kind)))
        .collect()
}

/// The operations a nodule's provider claims, each with the Lexicon its own file states, if the
/// description carries one (a foreign operation no vendored Lexicon states is unchecked). The
/// nodule's other Lexicon files travel with it: the cartridge carries copies of every Lexicon it
/// depends on (ADR 028), so datum-typed fields resolve. A non-ready provider is never called, so
/// it claims with no Lexicon.
fn claims(installed: &InstalledNodule, state: &ProviderState) -> Vec<(String, Operation)> {
    if !matches!(state, ProviderState::Ready(_)) {
        return unchecked_claims(&installed.admitted.operations);
    }
    let files: Vec<Value> = installed
        .resolved
        .lexicons
        .values()
        .map(|file| Value::Object(file.clone().into_iter().collect()))
        .collect();
    installed
        .admitted
        .operations
        .iter()
        .map(|(nsid, kind)| {
            let lexicon =
                files
                    .iter()
                    .position(|file| file["id"] == *nsid)
                    .map(|at| LexiconSource {
                        file: files[at].clone(),
                        carried: files
                            .iter()
                            .enumerate()
                            .filter(|(index, _)| *index != at)
                            .map(|(_, file)| file.clone())
                            .collect(),
                    });
            (
                nsid.clone(),
                lexicon.map_or_else(
                    || Operation::unchecked(*kind),
                    |source| Operation::declared(*kind, source),
                ),
            )
        })
        .collect()
}

fn dispatch_error(error: ClientError<DispatchError>) -> NodulesError {
    match error {
        ClientError::Method(DispatchError::Context(error)) => error.into(),
        ClientError::Method(DispatchError::Authority(error)) => NodulesError::Authority(error),
        ClientError::Method(DispatchError::AuthorityUnavailable(msg)) => {
            NodulesError::AuthorityUnavailable(msg)
        }
        // Dispatch refuses a scope wider than the authority the service was granted.
        ClientError::Method(DispatchError::Denied) => NodulesError::Rejected(
            "the method grant exceeds the dispatch authority granted to the nodule service".into(),
        ),
        ClientError::Method(other) => NodulesError::Rejected(other.to_string()),
        ClientError::Transport { message, .. } => NodulesError::Storage(message),
    }
}

fn drawers_error(error: ClientError<DrawersError>) -> NodulesError {
    match error {
        ClientError::Method(DrawersError::Context(error)) => error.into(),
        ClientError::Method(DrawersError::Authority(error)) => NodulesError::Authority(error),
        ClientError::Method(DrawersError::AuthorityUnavailable(msg)) => {
            NodulesError::AuthorityUnavailable(msg)
        }
        ClientError::Method(other) => NodulesError::Rejected(other.to_string()),
        ClientError::Transport { message, .. } => NodulesError::Storage(message),
    }
}
