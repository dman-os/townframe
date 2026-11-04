#![recursion_limit = "256"]

mod blob_boot;
mod listeners;
pub mod peers;

use listeners::Listener;
pub use listeners::Listeners;

use big_cabinet::Authority;
use big_cabinet::DocAccess;
use big_cabinet::DocsConfig;
use big_cabinet::DocsRepo;
use big_cabinet::DrawerAccess;
use big_cabinet::DrawerConfig;
use big_cabinet::Drawers;
use big_cabinet::SchemaRepo;
use big_cabinet::drawer::access::DrawerWorker;
use big_cabinet::index::Indexes;
use big_cabinet_core::authority::Actor;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::Namespace;
use big_cabinet_core::authority::OpenActor;
use big_cabinet_core::authority::RetractCapability;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::drawer::DrawerAccess as EntryAccess;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::drawer::protocol::DrawerScope;
use big_cabinet_core::drawer::protocol::GrantDrawer;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use big_dispatch::Dispatch;
use big_dispatch::DispatchConfig;
use big_dispatch::ServiceProvider;
use big_dispatch_core::DispatchScope;
use big_dispatch_core::MintDispatchCapability;
use big_nodule::Nodules;
use big_nodule::NodulesConfig;
use big_nodule::executor::NoduleExecutor;
use big_nodule_wasm::WasmExecutor;
use big_plugs::Plugs;
use big_plugs::PlugsConfig;
use big_plugs::RegistryDrawerConfig;
use big_repo::BigRepo;
use big_repo::BigRepoStopToken;
use big_repo::SharedBigRepo;
use futures::FutureExt;
use secrets_rs::SecretStore;
use service_utils_rs::RequestContext;
use sqlite_rs::Database;
use sqlite_rs::SqliteRuntime;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::path::Path;
use std::sync::Arc;
use utils_rs::prelude::Res;

/// What a node is configured with. Every storage resource in it is a binding the composition
/// root already opened (ADR 032 section 8): no service opens a path, so the node's place on disk,
/// in memory or in a shared pool is decided by whoever builds this value.
///
/// Two nodes may be handed the same `SqliteRuntime` (it is a pool of workers) and the same
/// `repo.storage.sql` database when their `repo.scope_key`s differ. `sql` is not scoped, so each
/// node needs a database of its own for it.
pub struct Config {
    /// The repo's tuning and its bindings, [`big_repo::Config::storage`].
    pub repo: big_repo::Config,
    /// The node's own database: authority, blobs, cabinet, indexes, plugs and nodules. One per
    /// node.
    pub sql: Database,
    pub schemas: Arc<SchemaRepo>,
    /// The node's secret store: one per node. The authority keeps its wrapping key under a
    /// fixed name in it, so nodes sharing one store would seal their actor keys under the same
    /// key, and nodes booting at once could each create a different one.
    pub secrets: Arc<SecretStore>,
    pub index_concurrency: NonZeroUsize,
    /// The blob store, opened by the composition root.
    pub blob_storage: big_blobs::Storage,
    /// The operator's grant of dispatch to the nodule service: the ceiling of what any nodule
    /// can be granted to invoke. A nodule's own operation grants are minted from it, so a grant
    /// outside it is reported on the nodule's record as not applied.
    pub nodule_operations: DispatchScope,
    /// The attachments that serve the node's router. Empty is a complete node.
    pub listeners: Listeners,
    /// The network attachment: the Iroh endpoint, the peers service and the peer policy. `None`
    /// is a complete node that talks to no other node. The endpoint is keyed by
    /// `repo.node_identity_seed`, the same key as the node's keyhive identity.
    pub peers: Option<peers::PeersConfig>,
}

/// The storage bindings of "a node in a directory", for embedders that want exactly that. It is
/// the one place `big_node` opens locations, and it does so as a composition root would, handing
/// the results to the services through [`Config`]:
///
/// ```text
/// Config {
///     repo: big_repo::Config { storage: bindings.repo, .. },
///     sql: bindings.sql,
///     blob_storage: bindings.blobs,
///     ..
/// }
/// ```
pub struct Bindings {
    pub sql: Database,
    pub repo: big_repo::Storage,
    pub blobs: big_blobs::Storage,
}

impl Bindings {
    /// Opens the node's databases, keyhive archives and blob store under `directory`:
    /// `node.sqlite`, `repo/` (`big_repo.sqlite` and `keyhive/`) and `blobs/`.
    ///
    /// `keyhive_secrets` sources the keyhive data-encryption keys from the OS keyring; `None`
    /// keeps them as files under `repo/keyhive`.
    pub async fn in_directory(
        runtime: &SqliteRuntime,
        directory: &Path,
        keyhive_secrets: Option<Arc<SecretStore>>,
    ) -> Res<Self> {
        std::fs::create_dir_all(directory)?;
        Ok(Self {
            sql: runtime.database(directory.join("node.sqlite"), &[], "_sqlite_rs_migrations"),
            repo: big_repo::Storage::in_directory(
                runtime,
                &directory.join("repo"),
                keyhive_secrets,
            )?,
            blobs: big_blobs::Storage::filesystem(directory.join("blobs")).await?,
        })
    }
}

pub struct BigNode {
    pub repo: SharedBigRepo,
    pub authority: Authority,
    pub docs_actor: Actor,
    pub docs_namespace: Namespace,
    pub drawers_actor: Actor,
    pub drawers_namespace: Namespace,
    pub blobs_actor: Actor,
    pub blobs_namespace: Namespace,
    pub blobs: big_blobs::Blobs,
    pub blob_workers: big_blobs::workers::BlobWorkers,
    pub nodules_actor: Actor,
    pub plugs_actor: Actor,
    pub dispatch_actor: Actor,
    pub indexes: Arc<Indexes>,
    pub drawer_access: Arc<DrawerAccess>,
    pub docs: DocsRepo,
    pub drawers: Drawers,
    pub dispatch: Dispatch,
    pub wasm: Arc<WasmExecutor>,
    pub nodules: Nodules,
    /// The plug registry service and the grouped drawer it keeps its plugs in.
    pub plugs: Plugs,
    pub registry_drawer: DrawerId,
    /// What the node listens on, as bound (a TCP port of `0` in the config is resolved here).
    pub listeners: Listeners,
    /// The network attachment, when the config asked for one.
    pub peers: Option<peers::Peers>,
}

/// What the config attaches to a booted node, taken before the config is consumed.
struct Attachments {
    listeners: Listeners,
    peers: Option<peers::PeersConfig>,
    /// The key the peers service's endpoint is built from: the node's keyhive identity.
    identity: [u8; 32],
}

impl Attachments {
    fn of(config: &Config) -> Self {
        Self {
            listeners: config.listeners.clone(),
            peers: config.peers.clone(),
            identity: config.repo.node_identity_seed,
        }
    }
}

impl BigNode {
    /// Registers the built-in providers and starts the attachments the config asks for. They come
    /// last in boot, so they stop first: nothing is served or dialed from a node whose services
    /// are coming down. Each is pushed as soon as it runs, so a later one failing to start still
    /// stops the earlier ones.
    async fn attach(&mut self, attachments: Attachments, stop: &mut StopToken) -> Res<()> {
        stop.stages.push(self.register_built_ins());
        self.listeners = attach_listeners(self, attachments.listeners, stop).await?;
        if let Some(config) = attachments.peers {
            self.peers = Some(attach_peers(self, config, attachments.identity, stop).await?);
        }
        Ok(())
    }

    pub async fn boot(config: Config) -> Res<(Self, StopToken)> {
        let mut stop = StopToken::default();
        let attachments = Attachments::of(&config);
        // Boxed: booting every service inline makes this future large enough (in debug builds)
        // that a caller awaiting it, such as a test that reboots a node, overflows its stack.
        let boot: Res<Self> = Box::pin(async {
            let schemas = Arc::new(config.schemas.with_groups([
                big_blobs::schema::registration(),
                big_nodule::schema_group(),
                big_plugs::schema_group(),
            ])?);
            let (authority, authority_stop) =
                big_cabinet::authority::boot(&config.secrets, config.sql.clone()).await?;
            stop.stages.push(Stage::Service(authority_stop));
            let (docs_actor, docs_namespace) = open_actor(&authority, "documents").await?;
            let (drawers_actor, drawers_namespace) = open_actor(&authority, "drawers").await?;
            let (nodules_actor, nodules_namespace) = open_actor(&authority, "nodules").await?;
            let (plugs_actor, plugs_namespace) = open_actor(&authority, "plugs").await?;
            let (dispatch_actor, dispatch_namespace) = open_actor(&authority, "dispatch").await?;
            let (blobs_actor, blobs_namespace) = open_actor(&authority, "blobs").await?;

            let (repo, repo_stop) = BigRepo::boot(config.repo).await?;
            stop.stages.push(Stage::Repo(repo_stop));
            let (blobs, blobs_stop) = big_blobs::boot(big_blobs::Config {
                storage: config.blob_storage,
                sql: config.sql.clone(),
                authority: authority.client.clone(),
                namespace: blobs_namespace,
            })
            .await?;
            stop.stages.push(Stage::Service(blobs_stop));
            let (indexes, index_stop) = Indexes::boot(
                Arc::clone(&repo),
                Arc::clone(&schemas),
                config.sql.clone(),
                config.index_concurrency,
            )
            .await?;
            stop.stages.push(Stage::Indexes(index_stop));
            let (drawer_access, provider, policy_stop) = DrawerAccess::open(
                Arc::clone(&repo),
                authority.client.clone(),
                drawers_actor.capability,
                drawers_namespace,
                config.sql.clone(),
                Arc::clone(&indexes.datum_set),
            )
            .await?;
            stop.stages.push(Stage::Service(policy_stop));
            let (docs, docs_stop) = big_cabinet::doc::prepare(DocsConfig {
                big_repo: Arc::clone(&repo),
                schema_repo: Arc::clone(&schemas),
                authority: authority.client.clone(),
                issuer: docs_actor.capability,
                namespace: docs_namespace,
                node_issuer: authority.node_issuer.clone(),
                access: Arc::clone(&drawer_access) as Arc<dyn DocAccess>,
                sql: config.sql.clone(),
                datum_set: Some(Arc::clone(&indexes.datum_set)),
                datum_ref: Some(Arc::clone(&indexes.datum_ref)),
            })
            .await?;
            stop.stages.push(Stage::Service(docs_stop));
            let drawers = start_drawers(provider, &docs, &mut stop).await?;
            // The blob workers are configured with the drawers they maintain blobs for, so the
            // registry drawer is created (or found) there, before they start.
            let (blob_workers, registry_drawer) = blob_boot::boot(
                config.sql.clone(),
                &blobs,
                &docs,
                &drawers,
                &drawers_actor,
                &plugs_actor,
                &drawer_access,
                &repo,
                Arc::clone(&indexes.datum_set),
                &mut stop,
            )
            .await?;
            // The registry reads artifact bytes from the blob service and keeps them pinned
            // through the workers, so it boots after both and stops before either. Dispatch and
            // the nodule service, its future consumers, boot after it.
            let plugs = boot_plugs(
                &authority,
                &docs,
                &drawers,
                &registry_drawer,
                &plugs_actor,
                plugs_namespace,
                &blobs,
                config.sql.clone(),
                &mut stop,
            )
            .await?;
            let (dispatch, nodule_dispatch_authority) = boot_dispatch(
                &authority,
                dispatch_namespace,
                config.nodule_operations,
                &mut stop,
            )
            .await?;
            // The Wasm executor is called by the nodule service, so it boots before it and
            // stops after it. It reads handler bytes from the blob service, which stops later
            // still, and its guests call dispatch, which stops later still.
            let (wasm, wasm_stop) =
                big_nodule_wasm::boot(Arc::clone(&blobs.service), dispatch.client.clone()).await?;
            stop.stages.push(Stage::Service(wasm_stop));
            let (nodules, nodules_stop) = boot_nodules(
                &authority,
                &docs,
                &drawers,
                &drawers_actor,
                &nodules_actor,
                nodules_namespace,
                &dispatch,
                nodule_dispatch_authority,
                Arc::clone(&wasm) as Arc<dyn NoduleExecutor>,
                schemas,
                config.sql,
            )
            .await?;
            stop.stages.push(Stage::Service(nodules_stop));
            let mut node = Self {
                repo,
                authority,
                docs_actor,
                docs_namespace,
                drawers_actor,
                drawers_namespace,
                blobs_actor,
                blobs_namespace,
                blobs,
                blob_workers,
                nodules_actor,
                plugs_actor,
                dispatch_actor,
                indexes,
                drawer_access,
                docs,
                drawers,
                dispatch,
                wasm,
                nodules,
                plugs,
                registry_drawer,
                listeners: Listeners::default(),
                peers: None,
            };
            node.attach(attachments, &mut stop).await?;
            Ok(node)
        })
        .await;
        match boot {
            Ok(node) => Ok((node, stop)),
            Err(error) => match stop.stop().await {
                Ok(()) => Err(error),
                Err(shutdown) => Err(error.wrap_err(format!("boot cleanup failed: {shutdown:#}"))),
            },
        }
    }

    /// Registers the node's own services in dispatch's router as built-in providers, under the NSIDs
    /// their operations declare. They are reached through the same clients the composition root wired
    /// them with, and keep checking the `Authorization` in each request themselves: dispatch admits
    /// the caller to the NSID, it does not vouch for the call.
    ///
    /// A provider holds its service's client and the nodule service holds the router, so the
    /// registrations are unbound first on stop (the returned stage runs before any service stops) to
    /// break that cycle and to stop routing to services about to go away.
    fn register_built_ins(&self) -> Stage {
        let built_ins = [
            (
                "builtin:authority",
                ServiceProvider::default().serve(&self.authority.client),
            ),
            (
                "builtin:docs",
                ServiceProvider::default().serve(&self.docs.client),
            ),
            (
                "builtin:drawers",
                ServiceProvider::default().serve(&self.drawers.client),
            ),
            (
                "builtin:blobs",
                ServiceProvider::default().serve(&self.blobs.client),
            ),
            (
                "builtin:plugs",
                ServiceProvider::default().serve(&self.plugs.client),
            ),
            (
                "builtin:nodules",
                ServiceProvider::default().serve(&self.nodules.client),
            ),
        ];
        let mut provider_ids = Vec::with_capacity(built_ins.len());
        for (provider_id, provider) in built_ins {
            provider.register(&self.dispatch.router, provider_id);
            provider_ids.push(provider_id);
        }
        Stage::BuiltIns {
            router: Arc::clone(&self.dispatch.router),
            provider_ids,
        }
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "boot wiring of already-built services"
)]
async fn boot_plugs(
    authority: &Authority,
    docs: &DocsRepo,
    drawers: &Drawers,
    registry_drawer: &DrawerId,
    actor: &Actor,
    namespace: Namespace,
    blobs: &big_blobs::Blobs,
    sql: Database,
    stop: &mut StopToken,
) -> Res<Plugs> {
    let (plugs, plugs_stop) = big_plugs::boot(PlugsConfig {
        authority: authority.client.clone(),
        docs: docs.client.clone(),
        drawers: drawers.client.clone(),
        registry_drawer: registry_drawer.clone(),
        actor: actor.clone(),
        namespace,
        operator: authority.service.operator_uuid(),
        blobs: Arc::clone(&blobs.service),
        node_key: big_plugs::NodeKey {
            public_key: authority.service.node_public_key(),
            sign: Arc::new({
                let service = Arc::clone(&authority.service);
                // The registry hands over its whole signing message; the authority re-binds the
                // plug domain itself, so this closure cannot sign anything outside that domain.
                move |message: &[u8]| {
                    let payload = message
                        .strip_prefix(big_plugs_core::signature::SIGNING_DOMAIN)
                        .expect("the registry only signs plug signing messages");
                    service.sign_as_node(big_plugs_core::signature::SIGNING_DOMAIN, payload)
                }
            }),
        },
        sql,
    })
    .await?;
    stop.stages.push(Stage::Service(plugs_stop));
    Ok(plugs)
}

/// Starts the drawer policy worker, recovers the documents it needs, then boots the drawers
/// service on that worker. The worker is a stage of its own (it must outlive the service), so it
/// is pushed and read back from `stop` rather than owned here.
async fn start_drawers(
    provider: big_cabinet::drawer::access::DrawerProvider,
    docs: &DocsRepo,
    stop: &mut StopToken,
) -> Res<Drawers> {
    let (worker, worker_stop) = provider.start(docs.client.clone());
    stop.stages.push(Stage::DrawerWorker {
        worker,
        stop: worker_stop,
    });
    docs.recover().await?;
    let Stage::DrawerWorker { worker, .. } = stop
        .stages
        .last()
        .expect("drawer worker was constructed last")
    else {
        unreachable!("drawer worker was constructed last");
    };
    let (drawers, drawers_stop) = big_cabinet::boot_drawers(DrawerConfig { worker }).await?;
    stop.stages.push(Stage::Service(drawers_stop));
    Ok(drawers)
}

/// Starts the listeners the config asks for and returns what they bound. They come last in boot,
/// so they stop first: nothing is served from a node that is coming down. Each is pushed as soon
/// as it runs, so a later one failing to bind still stops the earlier ones.
async fn attach_listeners(
    node: &BigNode,
    requested: Listeners,
    stop: &mut StopToken,
) -> Res<Listeners> {
    let mut bound = Listeners::default();
    if let Some(path) = requested.unix_socket {
        let listener = Listener::unix(
            &path,
            node.dispatch.client.clone(),
            Authorization::identity(node.authority.root),
        )
        .await?;
        stop.stages.push(Stage::Listener(listener));
        bound.unix_socket = Some(path);
    }
    if let Some(address) = requested.tcp {
        let (listener, address) = Listener::tcp(address, node.dispatch.client.clone()).await?;
        stop.stages.push(Stage::Listener(listener));
        bound.tcp = Some(address);
    }
    Ok(bound)
}

/// Starts the peers service. It is the last attachment, so it stops first: no connection is
/// served or opened from a node whose services are coming down.
async fn attach_peers(
    node: &BigNode,
    config: peers::PeersConfig,
    identity: [u8; 32],
    stop: &mut StopToken,
) -> Res<peers::Peers> {
    let (peers, peers_stop) = peers::boot(
        peers::Node {
            repo: &node.repo,
            drawers: &node.drawers,
            blobs: &node.blobs,
            blob_workers: &node.blob_workers,
        },
        config,
        identity,
    )
    .await?;
    stop.stages.push(Stage::Peers(peers_stop));
    Ok(peers)
}

/// Opens the subsystem actor at `path` under the node root, and its namespace.
async fn open_actor(authority: &Authority, path: &str) -> Res<(Actor, Namespace)> {
    let actor = authority
        .client
        .call(
            RequestContext::root(),
            OpenActor {
                parent: authority.root,
                path: path.into(),
                rights: Rights::all(),
            },
        )
        .await?;
    let namespace = authority.service.open_namespace(actor.actor_id).await?;
    Ok((actor, namespace))
}

/// Grants the nodules actor admin over the node's config drawer, then boots the nodule service
/// on it.
#[expect(
    clippy::too_many_arguments,
    reason = "boot wiring of already-built services"
)]
async fn boot_nodules(
    authority: &Authority,
    docs: &DocsRepo,
    drawers: &Drawers,
    drawers_actor: &Actor,
    nodules_actor: &Actor,
    namespace: Namespace,
    dispatch: &Dispatch,
    dispatch_authority: CapId,
    executor: Arc<dyn NoduleExecutor>,
    schemas: Arc<SchemaRepo>,
    sql: Database,
) -> Res<(Nodules, service_utils_rs::StopToken)> {
    let config_drawer = drawers.local().config_id.clone();
    grant_drawer_admin(drawers, drawers_actor, nodules_actor, &config_drawer).await?;
    Ok(big_nodule::boot(NodulesConfig {
        authority: authority.client.clone(),
        docs: docs.client.clone(),
        drawers: drawers.client.clone(),
        config_drawer,
        actor: nodules_actor.clone(),
        actor_path: "nodules".into(),
        namespace,
        operator: authority.service.operator_uuid(),
        dispatch: Arc::clone(&dispatch.router),
        dispatch_client: dispatch.client.clone(),
        dispatch_authority,
        executor: Some(executor),
        schemas,
        sql,
    })
    .await?)
}

/// Boots dispatch first: the nodule service registers its providers in dispatch's router, so it
/// must stop first and dispatch last (reverse of construction).
///
/// Then the operator mints the dispatch handle the nodule service mints every nodule's own
/// handle from: `scope` is the most any nodule can be granted. It is retracted once the nodule
/// service has stopped, which also kills the handles minted from it.
async fn boot_dispatch(
    authority: &Authority,
    namespace: Namespace,
    scope: DispatchScope,
    stop: &mut StopToken,
) -> Res<(Dispatch, CapId)> {
    let (dispatch, dispatch_stop) = big_dispatch::boot(DispatchConfig {
        authority: authority.client.clone(),
        namespace,
        operator: authority.service.operator_uuid(),
    });
    stop.stages.push(Stage::Service(dispatch_stop));
    let capability = dispatch
        .client
        .call_with(
            RequestContext::root(),
            Authorization::identity(authority.root).metadata(),
            MintDispatchCapability {
                scope,
                rights: Rights::USE | Rights::NARROW,
            },
        )
        .await?
        .capability_id;
    let (retract, tasks) = service_utils_rs::StopToken::new();
    let cancellation = tasks.clone();
    let client = authority.client.clone();
    tasks
        .spawn(async move {
            cancellation.cancelled().await;
            client
                .call(
                    RequestContext::root(),
                    RetractCapability {
                        via: capability,
                        capability,
                    },
                )
                .await
                .expect("retracting the nodule service's dispatch authority");
        })
        .expect("a fresh stop token accepts tasks");
    stop.stages.push(Stage::Service(retract));
    Ok((dispatch, capability))
}

/// Mints the drawers actor's admin capability over `drawer` and grants admin to `subject`, which
/// is how a service's actor becomes able to write a drawer the drawers actor owns: the nodule
/// service over the node's config drawer, the plug registry over its registry drawer.
async fn grant_drawer_admin(
    drawers: &Drawers,
    issuer: &Actor,
    subject: &Actor,
    drawer: &DrawerId,
) -> Res<()> {
    let cap = drawers
        .client
        .call_with(
            RequestContext::root(),
            Authorization::identity(issuer.capability).metadata(),
            MintDrawerCapability {
                scope: DrawerScope {
                    drawers: BTreeMap::from([(drawer.clone(), EntryAccess::Admin)]),
                    create: false,
                },
                rights: Rights::USE | Rights::DELEGATE,
            },
        )
        .await?;
    drawers
        .client
        .call(
            RequestContext::root(),
            GrantDrawer {
                capability: cap.capability_id,
                drawer_id: drawer.clone(),
                subject_id: subject.actor_id,
                access: EntryAccess::Admin,
            },
        )
        .await?;
    Ok(())
}

enum Stage {
    Service(service_utils_rs::StopToken),
    Indexes(big_cabinet::index::StopToken),
    Repo(BigRepoStopToken),
    DrawerWorker {
        worker: DrawerWorker,
        stop: service_utils_rs::StopToken,
    },
    /// A listener serving the router; stopping closes it and removes its socket file.
    Listener(Listener),
    /// The network attachment; stopping closes its connections, workers, router and endpoint.
    Peers(peers::PeersStop),
    /// The built-in providers registered in dispatch's router; stopping unbinds them.
    BuiltIns {
        router: Arc<big_dispatch::Router>,
        provider_ids: Vec<&'static str>,
    },
}

impl Stage {
    async fn stop(self) -> Res<()> {
        match self {
            Self::Service(stop) => stop.stop().await,
            Self::Indexes(stop) => stop.stop().await,
            Self::Repo(stop) => return stop.stop().await,
            Self::DrawerWorker { worker, stop } => {
                stop.stop().await;
                drop(worker);
            }
            Self::Listener(listener) => return listener.stop().await,
            Self::Peers(peers) => return peers.stop().await,
            Self::BuiltIns {
                router,
                provider_ids,
            } => {
                for provider_id in provider_ids {
                    router.unbind(provider_id);
                }
            }
        }
        Ok(())
    }
}

#[must_use]
#[derive(Default)]
pub struct StopToken {
    stages: Vec<Stage>,
}

impl StopToken {
    pub async fn stop(mut self) -> Res<()> {
        let mut failure: Option<utils_rs::prelude::eyre::Report> = None;
        let mut panic = None;
        while let Some(stage) = self.stages.pop() {
            match AssertUnwindSafe(stage.stop()).catch_unwind().await {
                Ok(Ok(())) => (),
                Ok(Err(error)) => {
                    failure = Some(match failure {
                        Some(previous) => {
                            previous.wrap_err(format!("additional shutdown failure: {error:#}"))
                        }
                        None => error,
                    })
                }
                Err(payload) => {
                    if panic.is_none() {
                        panic = Some(payload);
                    }
                }
            }
        }
        if let Some(panic) = panic {
            std::panic::resume_unwind(panic);
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod e2e;
