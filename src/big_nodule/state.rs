//! The single owner of the reconciled nodule inventory, durable host actors, and applied drawer
//! grants.
//!
//! Desired state lives in the node's config drawer document and is projected through
//! [`DatumStoreHandle<NoduleConfig>`]. This task serializes management mutations and config
//! snapshot reconciliations, provisions/reopens each admitted nodule's durable actor under
//! `parent_path`, and applies or revokes service-owned local drawer grants within the nodules
//! actor's delegated authority.

use crate::executor::HandlerSpec;
use crate::executor::Hosted;
use crate::executor::NoduleExecutor;
use crate::executor::ReensureQueue;
use crate::store;
use big_cabinet::DatumStoreHandle;
use big_cabinet::SchemaRegistration;
use big_cabinet::SchemaRegistrationGroup;
use big_cabinet::SchemaRepo;
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
use big_cabinet_core::dmeta::Uuid7;
use big_cabinet_core::doc::ChangeHash;
use big_cabinet_core::doc::StoreError;
use big_cabinet_core::doc::StoreSnapshot;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::drawer::protocol::DrawerScope;
use big_cabinet_core::drawer::protocol::DrawersError;
use big_cabinet_core::drawer::protocol::DrawersProtocol;
use big_cabinet_core::drawer::protocol::GrantDrawer;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use big_cartridges_core::Cartridge;
use big_cartridges_core::CartridgeId;
use big_cartridges_core::Revision;
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
use big_dispatch_core::Invoke;
use big_dispatch_core::MintDispatchCapability;
use big_dispatch_core::Readiness;
use big_nodule_core::RunMode;
use big_nodule_core::config::AdmissionCache;
use big_nodule_core::config::NoduleConfig;
use big_nodule_core::config::NoduleResolution;
use big_nodule_core::inventory::ArtifactState;
use big_nodule_core::inventory::InstalledNodule;
use big_nodule_core::inventory::InventoryError;
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
use futures::StreamExt;
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
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use utils_rs::expect_tags::ERROR_ACTOR;
use utils_rs::expect_tags::ERROR_CHANNEL;

type Res<T> = Result<T, NodulesError>;
type Reply<T> = oneshot::Sender<Res<T>>;

pub(crate) struct Stores {
    pub(crate) sql: Database,
    pub(crate) authority: Client<AuthorityProtocol>,
    pub(crate) drawers: Client<DrawersProtocol>,
    pub(crate) datum_store: DatumStoreHandle<NoduleConfig>,
    pub(crate) parent: OpenedActor,
    pub(crate) parent_path: String,
    pub(crate) dispatch: Arc<Router>,
    pub(crate) dispatch_client: Client<DispatchProtocol>,
    /// The operator-granted dispatch handle the service mints every nodule's own handle from.
    pub(crate) dispatch_authority: CapId,
    pub(crate) executor: Option<Arc<dyn NoduleExecutor>>,
    /// The node's schema repo: an enabled nodule's held datums are registered here and retired
    /// when it is disabled, refused, substituted away or uninstalled.
    pub(crate) schemas: Arc<SchemaRepo>,
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
    Admit {
        context: RequestContext,
        cartridge: Box<Cartridge>,
        drawers: Option<BTreeMap<DrawerId, DrawerAccess>>,
        reply: Reply<NoduleRecord>,
    },
    Configure {
        context: RequestContext,
        cartridge_id: CartridgeId,
        settings: IndexMap<String, Value>,
        reply: Reply<NoduleRecord>,
    },
    SetEnabled {
        context: RequestContext,
        cartridge_id: CartridgeId,
        enabled: bool,
        reply: Reply<NoduleRecord>,
    },
    SetDrawerGrants {
        context: RequestContext,
        cartridge_id: CartridgeId,
        drawers: BTreeMap<DrawerId, DrawerAccess>,
        reply: Reply<NoduleRecord>,
    },
    SetMethodGrants {
        context: RequestContext,
        cartridge_id: CartridgeId,
        methods: DispatchScope,
        reply: Reply<NoduleRecord>,
    },
    ConfirmResolution {
        context: RequestContext,
        cartridge_id: CartridgeId,
        binding: String,
        reply: Reply<NoduleRecord>,
    },
    SetSubstitution {
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
    /// The last invocation failed (or a dependency cycle was found) and nothing has retried it.
    failed: bool,
    /// The provider id whose not-ready operation the next initializer is waiting on, if it is
    /// waiting on a nodule: what dependency cycles are found along.
    blocked_on: Option<String>,
    /// The foreign operation the next initializer is waiting for a ready provider of. The
    /// nodule that starts providing it wakes this run (see [`State::wake_waiters`]).
    waiting_on: Option<String>,
}

/// How one initializer invocation reaches its target.
enum InitRoute {
    /// The nodule's own hosted handler, called by the runtime as the nodule service.
    Local {
        provider: Arc<dyn Provider>,
        caller: Caller,
    },
    /// Through dispatch, as the nodule's actor presenting this authorization.
    Dispatch(Authorization),
}

impl InitRoute {
    /// Calls `target` and reduces the outcome to what a record reports.
    async fn invoke(
        self,
        dispatch: &Client<DispatchProtocol>,
        target: &str,
        context: InvocationContext,
    ) -> Result<(), String> {
        let input = serde_json::json!({});
        match self {
            Self::Local { provider, caller } => provider
                .call(&caller, target, &RequestContext::root(), &context, input)
                .await
                .map(drop)
                .map_err(|error| match error {
                    ProviderError::InvalidInput(reason) => {
                        format!("`{target}` refused the input: {reason}")
                    }
                    ProviderError::Failed { status, body } => {
                        format!("`{target}` failed with status {status}: {body}")
                    }
                }),
            Self::Dispatch(authorization) => dispatch
                .call_with(
                    RequestContext::root(),
                    authorization.metadata(),
                    Invoke {
                        operation: target.to_owned(),
                        input,
                        context,
                    },
                )
                .await
                .map(drop)
                .map_err(|error| match error {
                    ClientError::Method(error) => format!("`{target}`: {error}"),
                    // Dispatch outlives the nodule service, and shutdown cancels the task
                    // that makes this call first.
                    ClientError::Transport { message, .. } => {
                        panic!("dispatch transport failed during an initializer: {message}")
                    }
                }),
        }
    }
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
    /// The dispatch handle minted for each nodule from its method grants, with the scope it
    /// was minted for.
    applied_operations: BTreeMap<CartridgeId, (DispatchScope, CapId)>,
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
    /// Cartridges whose executor asked to be ensured again. The state actor constructs it and
    /// outlives nothing that holds a handle to it in a way that matters (see [`ReensureQueue`]).
    reensure: Arc<ReensureQueue>,
    /// Initializer runs by cartridge.
    inits: BTreeMap<CartridgeId, InitRun>,
    /// The revision each `Once` initializer last succeeded at on this node.
    init_done: BTreeMap<CartridgeId, BTreeMap<String, Revision>>,
    next_epoch: u64,
    /// Where initializer invocations run, and where their results come back to.
    tasks: Spawner,
    init_results: mpsc::Sender<InitFinished>,
}

fn nodule_actor(actor: &OpenedActor) -> NoduleActor {
    NoduleActor {
        actor_id: actor.actor.actor_id,
        url: actor.actor.url.clone(),
        public_key: actor.actor.public_key.clone(),
    }
}

async fn answer<T>(mut reply: oneshot::Sender<T>, operation: impl Future<Output = T>) {
    let result = tokio::select! {
        biased;
        () = reply.closed() => return,
        result = operation => result,
    };
    drop(reply.send(result));
}

impl State {
    pub(crate) async fn recover(stores: Stores) -> Res<(Handle, StopToken)> {
        let mut persisted_actors = BTreeMap::new();
        for row in store::actors(&stores.sql).await? {
            persisted_actors.insert(row.cartridge_id, row.actor_id);
        }
        let mut applied_grants: BTreeMap<CartridgeId, BTreeMap<DrawerId, (DrawerAccess, CapId)>> =
            BTreeMap::new();
        for row in store::applied_grants(&stores.sql).await? {
            applied_grants
                .entry(row.cartridge_id)
                .or_default()
                .insert(row.drawer_id, (row.access, row.capability_id));
        }

        let applied_operations = store::applied_operations(&stores.sql)
            .await?
            .into_iter()
            .map(|row| (row.cartridge_id, (row.scope, row.capability_id)))
            .collect();

        let init_done = store::init_done(&stores.sql).await?;

        let locks = store::resolution_locks(&stores.sql).await?;

        let mut watch = stores
            .datum_store
            .watch()
            .await
            .map_err(crate::store_error)?;
        let initial = stores
            .datum_store
            .snapshot()
            .await
            .map_err(crate::store_error)?;

        let (commands, mut inbox) = mpsc::channel(64);
        let (init_results, mut finished_inits) = mpsc::channel(16);
        let (stop, tasks) = StopToken::new();
        let cancellation = tasks.clone();

        let mut state = Self {
            stores,
            persisted_actors,
            actors: BTreeMap::new(),
            applied_grants,
            applied_operations,
            locks,
            applied_heads: BTreeMap::new(),
            registered_schemas: BTreeMap::new(),
            records: BTreeMap::new(),
            observed_heads: Vec::new(),
            admission_cache: Arc::default(),
            hosted: BTreeMap::new(),
            reensure: Arc::default(),
            inits: BTreeMap::new(),
            init_done,
            next_epoch: 0,
            tasks: tasks.clone(),
            init_results,
        };
        let context = RequestContext::root();
        for (cartridge_id, expected_actor_id) in state.persisted_actors.clone() {
            let actor = state.stores.open_actor(&context, &cartridge_id).await?;
            if actor.actor.actor_id != expected_actor_id {
                return Err(NodulesError::Storage(format!(
                    "installed nodule `{cartridge_id}` was given actor {expected_actor_id} but its path now opens {}",
                    actor.actor.actor_id
                )));
            }
            state.actors.insert(cartridge_id, actor);
        }
        // The boot reconcile may already have started initializers; a failure here must not
        // leave them running behind a service that never came up.
        if let Err(err) = state.reconcile(&context, initial, Trigger::Boot).await {
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
                            let snapshot = state
                                .stores
                                .datum_store
                                .snapshot()
                                .await
                                .map_err(crate::store_error)?;
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
                    event = watch.next() => match event {
                        // Revoke failures come back as a map of per-cartridge outcomes that
                        // `reconcile` already folded into the refused records, so only
                        // an `Err` (storage, actor or capability failure) is fatal.
                        // A change this actor made itself (`mutate_config` reconciles it before
                        // its own watch event arrives) is already applied at these heads; only
                        // a change from elsewhere, such as a synced config edit, has others.
                        Some(Ok(snapshot)) => {
                            if snapshot.heads != state.observed_heads {
                                drop(
                                    state
                                        .reconcile(&RequestContext::root(), snapshot, Trigger::Change)
                                        .await?,
                                );
                            }
                        }
                        Some(Err(err)) => return Err(crate::store_error(err)),
                        // Shutdown stops this task before the datum store it watches, so a
                        // closed watch while we are still running is a lifecycle break.
                        None => {
                            return Err(NodulesError::Storage(
                                "nodule config watch closed while the state actor was running".into(),
                            ));
                        }
                    },
                }
            }
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
        Ok((Handle { commands }, stop))
    }

    async fn run(&mut self, command: Command) {
        match command {
            Command::Admit {
                context,
                cartridge,
                drawers,
                reply,
            } => answer(reply, self.admit(&context, *cartridge, drawers)).await,
            Command::Configure {
                context,
                cartridge_id,
                settings,
                reply,
            } => answer(reply, self.configure(&context, &cartridge_id, settings)).await,
            Command::SetEnabled {
                context,
                cartridge_id,
                enabled,
                reply,
            } => answer(reply, self.set_enabled(&context, &cartridge_id, enabled)).await,
            Command::SetDrawerGrants {
                context,
                cartridge_id,
                drawers,
                reply,
            } => {
                answer(
                    reply,
                    self.set_drawer_grants(&context, &cartridge_id, drawers),
                )
                .await
            }
            Command::SetMethodGrants {
                context,
                cartridge_id,
                methods,
                reply,
            } => {
                answer(
                    reply,
                    self.set_method_grants(&context, &cartridge_id, methods),
                )
                .await
            }
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
            Command::SetSubstitution {
                context,
                cartridge_id,
                by,
                reply,
            } => answer(reply, self.set_substitution(&context, &cartridge_id, by)).await,
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
        }
    }

    async fn mutate_config(
        &mut self,
        context: &RequestContext,
        edit: impl FnOnce(&mut NoduleConfig) -> Result<(), InventoryError> + Send + 'static,
    ) -> Res<()> {
        let domain_error: Arc<Mutex<Option<NodulesError>>> = Arc::new(Mutex::new(None));
        let captured = Arc::clone(&domain_error);
        let result = self
            .stores
            .datum_store
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
        let snapshot = self
            .stores
            .datum_store
            .snapshot()
            .await
            .map_err(crate::store_error)?;
        drop(self.reconcile(context, snapshot, Trigger::Change).await?);
        Ok(())
    }

    async fn admit(
        &mut self,
        context: &RequestContext,
        cartridge: Cartridge,
        drawers: Option<BTreeMap<DrawerId, DrawerAccess>>,
    ) -> Res<NoduleRecord> {
        let cartridge_id = cartridge.id.clone();
        let cache = Arc::clone(&self.admission_cache);
        self.mutate_config(context, move |config| {
            config.admit(&cache, &cartridge, drawers).map(|_| ())
        })
        .await?;
        self.records
            .get(&cartridge_id)
            .cloned()
            .ok_or(NodulesError::UnknownCartridge(cartridge_id))
    }

    async fn configure(
        &mut self,
        context: &RequestContext,
        cartridge_id: &CartridgeId,
        settings: IndexMap<String, Value>,
    ) -> Res<NoduleRecord> {
        let target = cartridge_id.clone();
        self.mutate_config(context, move |config| {
            config.configure(&target, settings).map(|_| ())
        })
        .await?;
        self.records
            .get(cartridge_id)
            .cloned()
            .ok_or_else(|| NodulesError::UnknownCartridge(cartridge_id.clone()))
    }

    async fn set_enabled(
        &mut self,
        context: &RequestContext,
        cartridge_id: &CartridgeId,
        enabled: bool,
    ) -> Res<NoduleRecord> {
        let target = cartridge_id.clone();
        let cache = Arc::clone(&self.admission_cache);
        self.mutate_config(context, move |config| {
            config.set_enabled(&cache, &target, enabled).map(|_| ())
        })
        .await?;
        self.records
            .get(cartridge_id)
            .cloned()
            .ok_or_else(|| NodulesError::UnknownCartridge(cartridge_id.clone()))
    }

    async fn set_substitution(
        &mut self,
        context: &RequestContext,
        cartridge_id: &CartridgeId,
        by: Option<CartridgeId>,
    ) -> Res<NoduleRecord> {
        let target = cartridge_id.clone();
        let cache = Arc::clone(&self.admission_cache);
        self.mutate_config(context, move |config| {
            config.set_substitution(&cache, &target, by)
        })
        .await?;
        self.records
            .get(cartridge_id)
            .cloned()
            .ok_or_else(|| NodulesError::UnknownCartridge(cartridge_id.clone()))
    }

    async fn set_drawer_grants(
        &mut self,
        context: &RequestContext,
        cartridge_id: &CartridgeId,
        drawers: BTreeMap<DrawerId, DrawerAccess>,
    ) -> Res<NoduleRecord> {
        let target = cartridge_id.clone();
        self.mutate_config(context, move |config| {
            config.set_drawer_grants(&target, drawers).map(|_| ())
        })
        .await?;
        self.records
            .get(cartridge_id)
            .cloned()
            .ok_or_else(|| NodulesError::UnknownCartridge(cartridge_id.clone()))
    }

    async fn set_method_grants(
        &mut self,
        context: &RequestContext,
        cartridge_id: &CartridgeId,
        methods: DispatchScope,
    ) -> Res<NoduleRecord> {
        let target = cartridge_id.clone();
        self.mutate_config(context, move |config| {
            config.set_method_grants(&target, methods).map(|_| ())
        })
        .await?;
        self.records
            .get(cartridge_id)
            .cloned()
            .ok_or_else(|| NodulesError::UnknownCartridge(cartridge_id.clone()))
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
        }) = record.requests.get(binding)
        else {
            return Err(NodulesError::Rejected(format!(
                "request `{binding}` of `{cartridge_id}` is not awaiting confirmation of a changed \
                 provider"
            )));
        };
        let snapshot = self
            .stores
            .datum_store
            .snapshot()
            .await
            .map_err(crate::store_error)?;
        // `refresh_if_needed` brought the records to this snapshot, so the recorded provider
        // change is still what the offers say, and the one offer is the cartridge to pin.
        let resolved = snapshot.value.resolve(&self.admission_cache);
        let index = ProviderIndex::new(&resolved);
        let Some(NoduleResolution::Admitted { installed, .. }) = resolved.get(cartridge_id) else {
            unreachable!("a request awaiting confirmation belongs to an admitted nodule")
        };
        let request = installed
            .nodule
            .requires
            .iter()
            .find(|request| request.binding == binding)
            .expect("the record reports a status for each request of its nodule");
        let offers = index.offers(
            contract,
            self.stores
                .native_offers(contract, request.is_operation_call()),
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
        let snapshot = self
            .stores
            .datum_store
            .snapshot()
            .await
            .map_err(crate::store_error)?;
        let revoke_errors = self.reconcile(context, snapshot, Trigger::Change).await?;
        if let Some(err) = revoke_errors.get(cartridge_id) {
            return Err(err.clone());
        }
        Ok(())
    }

    async fn refresh_if_needed(&mut self, context: &RequestContext) -> Res<()> {
        let snapshot = self
            .stores
            .datum_store
            .snapshot()
            .await
            .map_err(crate::store_error)?;
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

    async fn any_applied_grant_dead(&self, context: &RequestContext) -> Res<bool> {
        let grant_caps = self
            .applied_grants
            .values()
            .flat_map(BTreeMap::values)
            .map(|(_, cap_id)| cap_id);
        let operation_caps = self.applied_operations.values().map(|(_, cap_id)| cap_id);
        for cap_id in grant_caps.chain(operation_caps) {
            if !self.stores.is_cap_live(context, *cap_id).await? {
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
                if let Err(err) = drawers.and(operations) {
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
        let providers = ProviderIndex::new(&resolved);
        let mut new_records = BTreeMap::new();
        for (cartridge_id, resolution) in resolved {
            match resolution {
                NoduleResolution::Refused {
                    cartridge_id,
                    version,
                    revision,
                    enabled,
                    config,
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
                            config,
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
                        )
                        .await?;
                    new_records.insert(cartridge_id, record);
                }
            }
        }

        self.observed_heads = snapshot.heads;
        self.records = new_records;
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
            }) = resolved.remove(&cartridge_id)
            else {
                unreachable!("selected as admitted above");
            };
            let split = installed
                .nodule
                .registrable_datums(installed.admitted.authority.as_deref());
            let object = |file: &IndexMap<String, Value>| {
                Value::Object(file.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            };
            let group = SchemaRegistrationGroup::new(
                None,
                split.held.iter().map(|(nsid, file, sidecar)| {
                    let registration = SchemaRegistration::new(*nsid, object(file));
                    match sidecar {
                        Some(document) => registration.with_supplement((*document).clone()),
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
                        config: installed.config,
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
        capability: Option<CapId>,
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
        let hosting = match (&self.stores.executor, &installed.nodule.handler) {
            (Some(executor), Some(handler)) if installed.enabled => {
                Some((Arc::clone(executor), handler.artifact.clone()))
            }
            _ => None,
        };
        if !installed.enabled {
            self.withdraw(&cartridge_id).await;
            return Readiness::Disabled;
        }
        // A required operation with no covering grant is configuration the nodule is waiting
        // for, not a fault (Clay's "needs configuration"): nothing is hosted or initialized
        // until it is granted, and callers get the retryable not-ready.
        if installed.nodule.unmet_requests(granted).next().is_some() {
            self.retire(&cartridge_id).await;
            self.stores
                .dispatch
                .register(
                    &provider_id,
                    unchecked_claims(&installed.admitted.operations),
                    ProviderState::NotReady(Readiness::NeedsConfiguration),
                )
                .expect("claims without a Lexicon compile nothing");
            return Readiness::NeedsConfiguration;
        }
        // Likewise a required operation nobody (or more than one, or the wrong cartridge) provides:
        // enabling is accepted and the nodule waits, as a pod waits on a missing dependency. No
        // waiter needs waking when a provider turns up: resolution is recomputed for every
        // nodule on each reconcile, and a provider can only appear through one (a config
        // change, since every nodule's enablement lives in the config document).
        if installed
            .nodule
            .missing_providers(requests)
            .next()
            .is_some()
        {
            self.retire(&cartridge_id).await;
            self.stores
                .dispatch
                .register(
                    &provider_id,
                    unchecked_claims(&installed.admitted.operations),
                    ProviderState::NotReady(Readiness::ProviderMissing),
                )
                .expect("claims without a Lexicon compile nothing");
            return Readiness::ProviderMissing;
        }
        let Some((executor, artifact_key)) = hosting else {
            self.retire(&cartridge_id).await;
            let readiness = installed.readiness();
            if readiness.is_ready() {
                // No handler: it implements nothing, so there is no provider to register. It
                // can still have initializers, which then target foreign operations only
                // (a local target needs a handler), so they need no provider of its own.
                self.stores.dispatch.unbind(&provider_id);
                return self
                    .advance_inits(installed, actor, capability, None, trigger)
                    .await
                    .unwrap_or(readiness);
            }
            self.stores
                .dispatch
                .register(
                    &provider_id,
                    unchecked_claims(&installed.admitted.operations),
                    ProviderState::NotReady(readiness),
                )
                .expect("claims without a Lexicon compile nothing");
            return readiness;
        };
        // Upgrading to another revision replaces the workload: the old one stops being routed
        // to (the registration below swaps it in one step) and is retired before the new one
        // is ensured.
        if self
            .hosted
            .get(&cartridge_id)
            .is_some_and(|hosted| *hosted != installed.revision)
        {
            self.stores.dispatch.unbind(&provider_id);
            self.retire(&cartridge_id).await;
        }
        let (handler, _) = installed
            .artifacts
            .get(&artifact_key)
            .expect("admission resolved the handler artifact");
        let spec = HandlerSpec {
            cartridge_id: cartridge_id.clone(),
            revision: installed.revision,
            handler: handler.clone(),
            actor: actor.clone(),
            capability,
            operations: installed.admitted.operations.clone(),
            reensure: self.reensure.handle(cartridge_id.clone()),
        };
        self.hosted.insert(cartridge_id.clone(), installed.revision);
        let state = match executor.ensure(&spec).await {
            // The runtime calls a local initializer on the provider directly, so the provider is
            // used here even while dispatch still answers not ready.
            Hosted::Ready(provider) => match self
                .advance_inits(
                    installed,
                    actor,
                    capability,
                    Some(Arc::clone(&provider)),
                    trigger,
                )
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
        if state.readiness() != Readiness::ArtifactsUnavailable
            && let Some((_, artifact_state)) = installed.artifacts.get_mut(&artifact_key)
        {
            *artifact_state = ArtifactState::Available;
        }
        let state = self.register_claims(&provider_id, installed, state);
        // The one place a hosted nodule's registration turns ready, whichever way it got
        // there (its inits finished, its bytes arrived, it was enabled or installed late).
        // Comparing with the previous reconcile's record makes it a transition, so a steady
        // ready nodule never wakes anyone and a woken waiter cannot wake itself in a loop.
        let was_ready = self
            .records
            .get(&cartridge_id)
            .is_some_and(|record| record.readiness.is_ready());
        if state.readiness().is_ready() && !was_ready {
            self.wake_waiters(&installed.admitted.operations);
        }
        state.readiness()
    }

    /// Asks for a reconcile of every run waiting for one of `operations`, which a nodule has
    /// just started providing. Systemd and Kubernetes likewise re-evaluate dependents on a
    /// readiness transition rather than wait for an unrelated event.
    fn wake_waiters(&self, operations: &BTreeMap<String, Option<OperationKind>>) {
        for (cartridge_id, run) in &self.inits {
            if run
                .waiting_on
                .as_ref()
                .is_some_and(|nsid| operations.contains_key(nsid))
            {
                self.reensure.handle(cartridge_id.clone()).request();
            }
        }
    }

    /// Moves the nodule's initializers along and returns `None` when every one has succeeded in
    /// this start and revision, otherwise the readiness it is held at.
    ///
    /// They run one at a time in declaration order. Each invocation is a task off this actor
    /// (the actor never awaits a handler) whose result returns as an [`InitFinished`] message; a
    /// failed one stays failed until a reconcile other than its own result's retries it, with no
    /// backoff yet. `provider` is the hosted handler, which a local target is called on directly
    /// (the host running its own lifecycle hook, as the nodule service); a foreign target goes
    /// through dispatch as the nodule's actor under its operations handle, so it needs a grant
    /// like any other call of the nodule's.
    async fn advance_inits(
        &mut self,
        installed: &InstalledNodule,
        actor: &OpenedActor,
        capability: Option<CapId>,
        provider: Option<Arc<dyn Provider>>,
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
                    blocked_on: None,
                    waiting_on: None,
                },
            );
        }
        let run = self
            .inits
            .get_mut(cartridge_id)
            .expect("run inserted above");
        let init = inits.iter().find(|init| !run.done.contains(&init.key))?;
        if run.running.is_some() {
            return Some(Readiness::Initializing);
        }
        if run.failed && trigger == Trigger::InitResult {
            return Some(Readiness::InitFailed);
        }

        let route = if installed.admitted.operations.contains_key(&init.target) {
            InitRoute::Local {
                provider: provider.expect(
                    "a local initializer target means the nodule has a handler, and its \
                     initializers only run once it is hosted",
                ),
                caller: Caller {
                    authorization: Authorization::identity(self.stores.parent.capability),
                    actor: self.stores.parent.actor.actor_id,
                    scope: None,
                },
            }
        } else {
            // A foreign operation. Wait for its provider rather than fail the first call: it
            // may be a nodule that is itself still initializing, or not installed yet. Only a
            // provider that is ready is invoked, so a denial or a failure is the call's own.
            let waiting_on = match self.stores.dispatch.resolve(&init.target) {
                Ok(entry) if matches!(entry.state, ProviderState::Ready(_)) => None,
                Ok(entry) => Some(Some(entry.provider_id.clone())),
                Err(_) => Some(None),
            };
            if let Some(provider_id) = waiting_on {
                let run = self.inits.get_mut(cartridge_id).expect("run present");
                run.failed = false;
                run.blocked_on = provider_id;
                run.waiting_on = Some(init.target.clone());
                if self.in_dependency_cycle(cartridge_id) {
                    let run = self.inits.get_mut(cartridge_id).expect("run present");
                    run.failed = true;
                    run.error = Some(format!(
                        "initializer `{}` waits for `{}`, whose provider waits for this nodule \
                         (dependency cycle)",
                        init.key, init.target
                    ));
                    return Some(Readiness::InitFailed);
                }
                return Some(Readiness::Initializing);
            }
            InitRoute::Dispatch(match capability {
                Some(authority) => Authorization {
                    authority,
                    signer: actor.capability,
                },
                // No operations grant: dispatch refuses an identity that is not the operator's.
                None => Authorization::identity(actor.capability),
            })
        };

        let run = self.inits.get_mut(cartridge_id).expect("run present");
        run.attempts += 1;
        run.failed = false;
        run.blocked_on = None;
        run.waiting_on = None;
        run.running = Some(init.key.clone());
        let context = InvocationContext {
            idempotency_key: Some(format!(
                "nodule/{cartridge_id}/{}/{}",
                installed.revision, init.key
            )),
            ..Default::default()
        };
        let finished = InitFinished {
            cartridge_id: cartridge_id.clone(),
            epoch: run.epoch,
            key: init.key.clone(),
            once: init.mode == RunMode::Once,
            outcome: Ok(()),
        };
        let target = init.target.clone();
        let dispatch_client = self.stores.dispatch_client.clone();
        let invocation = async move {
            InitFinished {
                outcome: route.invoke(&dispatch_client, &target, context).await,
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
            .filter(|run| run.attempts > 0 || run.error.is_some() || run.waiting_on.is_some())
            .map(|run| NoduleInitStatus {
                attempts: run.attempts,
                error: run.error.clone(),
                waiting_on: run.waiting_on.clone(),
            })
    }

    /// Whether following who each blocked nodule waits on, from `cartridge_id`'s own blocked
    /// initializer, comes back to `cartridge_id`.
    fn in_dependency_cycle(&self, cartridge_id: &CartridgeId) -> bool {
        let own = cartridge_id.to_string();
        let waits: BTreeMap<String, &str> = self
            .inits
            .iter()
            .filter_map(|(cartridge, run)| {
                Some((cartridge.to_string(), run.blocked_on.as_deref()?))
            })
            .collect();
        let mut at = waits.get(&own).copied();
        // More hops than waiting nodules means a cycle that does not include this one.
        for _ in 0..=waits.len() {
            match at {
                Some(provider_id) if provider_id == own => return true,
                Some(provider_id) => at = waits.get(provider_id).copied(),
                None => return false,
            }
        }
        false
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
        let snapshot = self
            .stores
            .datum_store
            .snapshot()
            .await
            .map_err(crate::store_error)?;
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
        for request in &installed.nodule.requires {
            let pinned = self
                .locks
                .get(cartridge_id)
                .and_then(|locks| locks.get(&request.binding))
                .filter(|pin| pin.contract == request.contract);
            let offers = providers.offers(
                &request.contract,
                self.stores
                    .native_offers(&request.contract, request.is_operation_call()),
            );
            statuses.insert(
                request.binding.clone(),
                RequestStatus::evaluate(request, &offers, pinned),
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
    ) -> Res<NoduleRecord> {
        let cartridge_id = installed.cartridge_id.clone();
        let actor = self.ensure_nodule_actor(context, &cartridge_id).await?;
        // The handle is minted (or revoked) before the handler is ensured, so the executor is
        // handed the handle that is live now. A disabled nodule has none: it is not serving, and
        // re-enabling it mints a fresh one.
        // The manifest bounds what configuration can confer: only the grants that cover a
        // requested operation are minted, as those exact operations. An entry covering no
        // request is reported on the record rather than applied, which is what keeps "can X
        // ever reach Y" answerable from the manifest alone.
        let (bound_operations, unrequested_operations) =
            installed.nodule.bind_operations(&desired_operations);
        let effective_operations = if installed.enabled {
            bound_operations.clone()
        } else {
            DispatchScope::default()
        };
        let (capability, operations_error) = self
            .apply_operations(context, &cartridge_id, &actor, &effective_operations)
            .await?;
        // A grant that failed to mint covers nothing, so it cannot satisfy a request.
        let granted_operations =
            capability.map_or_else(DispatchScope::default, |_| effective_operations);
        let requests = self.resolve_requests(&installed, providers).await?;
        let readiness = self
            .serve(
                &mut installed,
                &actor,
                capability,
                &granted_operations,
                &requests,
                trigger,
            )
            .await;

        let mut apply_errors = Vec::new();
        if let Some(error) = &operations_error {
            apply_errors.push(format!("operations: {error}"));
        }
        if let Err(err) = self
            .revoke_grants_for_cartridge(context, &cartridge_id, &desired_grants)
            .await
        {
            apply_errors.push(err.to_string());
        }

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
            config: installed.config.clone(),
            artifacts: installed.artifact_states(),
            drawers: drawer_statuses,
            methods: (!desired_operations.is_empty()).then(|| NoduleMethodGrantStatus {
                desired: bound_operations,
                unrequested: unrequested_operations,
                granted: capability.is_some(),
                error: operations_error,
            }),
            requests,
            init: self.init_status(&cartridge_id),
            observed_heads: heads.to_vec(),
            applied_heads: self.applied_heads.get(&cartridge_id).cloned(),
            error,
        })
    }

    /// Brings the nodule's dispatch handle in line with `desired`, returning the live handle (if
    /// any) and the reason the grant could not be applied, which is reported on the record
    /// instead of failing the reconcile, as a drawer grant's failure is. Only storage failures
    /// are fatal.
    ///
    /// A handle minted for exactly `desired` and still live is kept (so a restart or an
    /// unrelated config change does not change the id the handler holds). Any other handle is
    /// retracted first and a new one minted, so a changed scope is never a widening of the old
    /// handle; an empty `desired` only revokes. A revocation that fails leaves the handle
    /// recorded for the next reconcile to retry, and reports none to the handler: fail closed.
    async fn apply_operations(
        &mut self,
        context: &RequestContext,
        cartridge_id: &CartridgeId,
        actor: &OpenedActor,
        desired: &DispatchScope,
    ) -> Res<(Option<CapId>, Option<String>)> {
        if let Some((applied, cap_id)) = self.applied_operations.get(cartridge_id).cloned() {
            if applied == *desired && self.stores.is_cap_live(context, cap_id).await? {
                return Ok((Some(cap_id), None));
            }
            if let Err(error) = self
                .revoke_operations_for_cartridge(context, cartridge_id, &DispatchScope::default())
                .await
            {
                return match error {
                    NodulesError::Storage(error) => Err(NodulesError::Storage(error)),
                    error => Ok((None, Some(error.to_string()))),
                };
            }
        }
        if *desired == DispatchScope::default() {
            return Ok((None, None));
        }
        let cap_id = match self.stores.mint_operations(context, actor, desired).await {
            Ok(cap_id) => cap_id,
            Err(NodulesError::Storage(error)) => return Err(NodulesError::Storage(error)),
            Err(error) => return Ok((None, Some(error.to_string()))),
        };
        if let Err(error) =
            store::upsert_applied_operations(&self.stores.sql, cartridge_id, desired, cap_id).await
        {
            // The handle exists but is not recorded, so nothing would ever retract it.
            self.stores.retract_grant_cap(context, cap_id).await?;
            return Err(error);
        }
        self.applied_operations
            .insert(cartridge_id.clone(), (desired.clone(), cap_id));
        Ok((Some(cap_id), None))
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
    async fn open_actor(
        &self,
        context: &RequestContext,
        cartridge_id: &CartridgeId,
    ) -> Res<OpenedActor> {
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
    ) -> Res<()> {
        self.drawers
            .call(
                context.child(),
                GrantDrawer {
                    capability,
                    drawer_id: drawer_id.clone(),
                    subject_id,
                    access,
                },
            )
            .await
            .map_err(drawers_error)
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
            .grant_with_cap(context, cap_id, drawer_id, subject_id, access)
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
        .nodule
        .lexicons
        .iter()
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
                Operation {
                    kind: *kind,
                    lexicon,
                },
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
