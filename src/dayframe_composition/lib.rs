//! Shared Dayframe composition: a catalog-driven composition root, its lifecycle controls, and
//! the `Application` shell UI. The root names no micro: launchers (desktop, web) hand it every
//! installed micro as an [`InstalledMicro`], a manifest plus its static configuration and the
//! engine-specific factory for it. Engine-specific guest factories (Wasmtime on desktop, `wcmp`
//! on the web) and native built-ins are both just catalog entries (ADR 018 §7).
//!
//! The frame is the shell micro's (ADR 032): `Application` mounts the roots the catalog
//! registered, which is the shell's frame filling the window, laid out from its panels. It
//! supplies the shell what only the host knows, as [`HostInputs`]. The micro lifecycle controls
//! are a Settings section ([`lifecycle`]).
//!
//! The catalog is explicit configuration, not a heuristic: each entry says whether the
//! supervisor starts the micro eagerly, which provider satisfies each of its requirements, and
//! which declared collections it may read or write. The root declares the collections the
//! manifests name and installs every manifest-declared row into the declaring micro's
//! manifest installation scope (ADR 031 §4), so rows are visible while their micro is stopped.
//! There is no supervisor reconfiguration yet: enablement and startup are fixed by the catalog
//! except for the enable/disable/retry controls of the lifecycle section.
mod bindings;
pub mod lifecycle;

use bindings::BindingsDatum;
use bindings::DeclaredCollections;
use bindings::Resolution;
use clay_collection::ReplicaId;
use clay_dioxus::NativeComponents;
use clay_docs::DocumentGrant;
use clay_docs::DocumentWatch;
use clay_docs::FacetRoute;
use clay_docs::FacetSelection;
use clay_docs::FacetValue;
use clay_docs::WatchSelection;
use clay_guest_session::collections::durable_bytes_registrations;
use clay_guest_session::collections::BytesRegistrations;
use clay_host::CollectionContract;
use clay_host::ConfiguredMicro;
use clay_host::Factory;
use clay_host::Host;
use clay_host::Instance;
use clay_host::LocalFuture;
use clay_host::MicroId as SupervisorId;
use clay_host::MicroState;
use clay_host::Requirement;
use clay_host::Supervisor;
use clay_mdk::hooks::use_rows;
use clay_mdk::hooks::PresentationSlot;
use clay_mdk::host::declared_rows;
use clay_mdk::host::manifest_scope;
use clay_mdk::host::CapabilityPublication;
use clay_mdk::host::CollectionBindings;
use clay_mdk::host::CollectionRegistry;
use clay_mdk::host::DocumentService;
use clay_mdk::host::Resources;
use clay_mdk::host::RootRow;
use clay_mdk::Collection;
use clay_mdk::MicroId;
use clay_mdk::Nsid;
use clay_mdk::SHELL_ROOTS;
use clay_store::BatchOp;
use clay_store::OrderedStore;
use clay_store::StoreError;
use dioxus::prelude::*;
pub use micro_shell::HostInputs;
pub use micro_shell::Insets;
use serde::Deserialize;
use serde::Serialize;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::future::Future;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::sync::Notify;

/// One installed micro in the launcher-supplied catalog: its self-describing sidecar (package
/// identity, authority and manifest), its
/// static configuration, and the factory that turns the root's resources into its supervisor
/// factory. Packaging (a component file a desktop scan read, an index entry a web shell
/// fetched, a native built-in the launcher links) stays in the launcher; this crate sees only
/// the catalog.
pub struct InstalledMicro {
    /// The id the micro is installed as (ADR 028 §2): the key of its install and binding
    /// datums, its registration scopes and its endpoints. The sidecar's `id` is only the
    /// plug *name* part of it.
    pub id: MicroId,
    pub sidecar: clay_mdk::Sidecar,
    /// Started at boot rather than on first use.
    pub eager: bool,
    /// Whether admission (the first boot that sees this micro) records it enabled. Launchers
    /// set it for the native built-ins they link; a package the launcher merely discovered
    /// stays disabled until a user enables it. Only consulted when the micro has no install
    /// datum yet, because a stored datum always wins.
    pub enabled_on_admission: bool,
    /// Offered no enable/disable controls: the shell cannot run without it.
    pub locked: bool,
    /// Local requirement binding -> the provider's publication that satisfies it.
    pub requirements: BTreeMap<String, Requirement>,
    /// Builds the supervisor factory from the micro's installed id (so a factory cannot be
    /// built under another id than the one the catalog keys the micro by), the root's resources and the micro's collection
    /// bindings. The bindings handle is the composition's: it resolves the manifest's
    /// collection requests from configuration and the factory reads it at each `initialize`.
    pub factory: FactoryBuilder,
}

/// See [`InstalledMicro::factory`].
pub type FactoryBuilder = Box<
    dyn FnOnce(MicroId, Resources, CollectionBindings) -> Rc<dyn Factory<CapabilityPublication>>,
>;

impl InstalledMicro {
    /// Satisfy every requirement of this micro from `provider`: each binds the provider's
    /// publication of the same name. A catalog convention that spares each launcher spelling
    /// the same table out; a micro whose requirements come from several providers sets
    /// `requirements` itself.
    pub fn satisfied_by(mut self, provider: &MicroId) -> Self {
        for request in &self.sidecar.manifest.requires {
            self.requirements.insert(
                request.binding.clone(),
                Requirement {
                    provider: SupervisorId::from(provider),
                    publication: request.binding.clone(),
                },
            );
        }
        self
    }

    /// An on-demand, unlocked micro with no requirements, admitted disabled.
    pub fn new(
        id: MicroId,
        sidecar: clay_mdk::Sidecar,
        factory: impl FnOnce(MicroId, Resources, CollectionBindings) -> Rc<dyn Factory<CapabilityPublication>>
            + 'static,
    ) -> Self {
        Self {
            id,
            sidecar,
            eager: false,
            enabled_on_admission: false,
            locked: false,
            requirements: BTreeMap::new(),
            factory: Box::new(factory),
        }
    }
}

/// Desired enablement lives in a config document, shaped like big_nodule's install datum: one
/// facet per micro, `{ "enabled": bool, "package_version": u32 }`, keyed by micro id.
const INSTALLS_DOCUMENT: &str = "dayframe.installs";
const INSTALLS_BRANCH: &str = "main";
const INSTALL_TAG: &str = "clay.micro.install";

fn install_route(micro: &MicroId) -> FacetRoute {
    FacetRoute {
        document: INSTALLS_DOCUMENT.into(),
        branch: INSTALLS_BRANCH.into(),
        tag: INSTALL_TAG.into(),
        key_id: micro.as_str().into(),
    }
}

/// One install facet's value. Unknown fields ride along in `extra` so a datum written by a
/// newer build survives a rewrite by this one (as big_nodule's install datum does).
#[derive(Serialize, Deserialize)]
struct InstallDatum {
    enabled: bool,
    package_version: u32,
    #[serde(flatten)]
    extra: BTreeMap<String, serde_json::Value>,
}

impl InstallDatum {
    /// Only this crate writes the document, so a malformed datum is a bug.
    fn decode(value: serde_json::Value) -> Self {
        serde_json::from_value(value).expect("the install document holds only install datums")
    }

    fn encode(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("an install datum serializes")
    }
}

/// Admission of the install datums, returning each unlocked micro's enablement at boot.
fn admit_installs(
    documents: &DocumentService,
    installed: &[InstalledMicro],
) -> BTreeMap<MicroId, bool> {
    // Admission. The composition is the authority writing the install document; while the
    // node is mocked it writes through the host-side `external_change` (as big_nodule's
    // service writes its config document with its own capability), not through a granted
    // mutation. A stored datum always wins, so only its package version is refreshed.
    let installs = documents.mock_documents();
    let mut enabled_at_boot = BTreeMap::new();
    for micro in installed.iter().filter(|micro| !micro.locked) {
        let identity = micro.id.clone();
        let route = install_route(&identity);
        let version = micro.sidecar.package_version;
        let enabled = match installs.get(&route) {
            Some(stored) => {
                let mut datum = InstallDatum::decode(stored);
                if datum.package_version != version {
                    datum.package_version = version;
                    installs.external_change(route, Some(datum.encode()));
                }
                datum.enabled
            }
            None => {
                let datum = InstallDatum {
                    enabled: micro.enabled_on_admission,
                    package_version: version,
                    extra: BTreeMap::new(),
                };
                installs.external_change(route, Some(datum.encode()));
                datum.enabled
            }
        };
        enabled_at_boot.insert(identity, enabled);
    }
    enabled_at_boot
}

/// The shell's read-only watch of the install document: the install datums and the binding
/// datums, which are both per-micro facets of it.
fn watch_installs(documents: &DocumentService, host: &Host, shell: &Instance) -> DocumentWatch {
    documents
        .grant_documents(
            host,
            shell,
            vec![DocumentGrant {
                document: INSTALLS_DOCUMENT.into(),
                branch: INSTALLS_BRANCH.into(),
                facets: [INSTALL_TAG, bindings::BINDINGS_TAG]
                    .map(|tag| daybook_types::manifest::RoutineFacetAccess {
                        tag: tag.into(),
                        read: true,
                        ..Default::default()
                    })
                    .into(),
            }],
        )
        .and_then(|access| {
            access.watch(WatchSelection {
                documents: [INSTALLS_DOCUMENT.to_owned()].into(),
                branch: INSTALLS_BRANCH.into(),
                facets: FacetSelection::Tags(
                    [INSTALL_TAG, bindings::BINDINGS_TAG]
                        .map(str::to_owned)
                        .into(),
                ),
            })
        })
        .expect("the shell instance is live")
}

/// A micro as the install-document reconciler tracks it.
struct ManagedMicro {
    id: SupervisorId,
    manifest: clay_mdk::MicroManifest,
    /// Shared with the micro's factory.
    bindings: CollectionBindings,
    /// The binding resolution the factory and the supervisor currently reflect.
    applied: Resolution,
}

/// Applies the install document to the supervisor for as long as it runs: every snapshot
/// brings each micro's state in line with its install datum, and its collection bindings in
/// line with its binding datum. A disable is awaited before the next snapshot is read, and the
/// next read returns the latest revision, so a quick disable then enable converges on the
/// final datum. A changed binding resolution replaces the factory's bindings and closes the
/// micro so its next start sees them (no hot rebinding); eager roots are then started again.
/// A close error is published for the lifecycle section.
async fn reconcile_installs(
    installs: DocumentWatch,
    supervisor: Rc<Supervisor<CapabilityPublication>>,
    micros: Vec<ManagedMicro>,
    declared: DeclaredCollections,
    failure: watch::Sender<Option<String>>,
) {
    // Micros by key id, so a facet is matched without building a route or a MicroId.
    let mut micros: BTreeMap<String, ManagedMicro> = micros
        .into_iter()
        .map(|micro| (micro.id.as_str().to_owned(), micro))
        .collect();
    let mut snapshot = installs.current().expect("the install watch is live");
    loop {
        for (route, value) in &snapshot.facets {
            // The watch selects only this crate's tags; ids of micros no longer in the catalog
            // are left alone.
            let Some(micro) = micros.get_mut(route.key_id.as_str()) else {
                continue;
            };
            match route.tag.as_str() {
                INSTALL_TAG => {
                    let FacetValue::Present(datum) = value else {
                        continue;
                    };
                    let disabled = supervisor.state(&micro.id) == MicroState::Disabled;
                    if InstallDatum::decode(datum.clone()).enabled {
                        if disabled {
                            supervisor.enable(&micro.id);
                        }
                    } else if !disabled {
                        if let Err(error) = supervisor.disable(&micro.id).await {
                            failure.send_replace(Some(error.to_string()));
                        }
                    }
                }
                bindings::BINDINGS_TAG => {
                    let datum = match value {
                        FacetValue::Present(datum) => Some(BindingsDatum::decode(datum.clone())),
                        _ => None,
                    };
                    let resolution = bindings::resolve(&micro.manifest, datum.as_ref(), &declared);
                    if resolution == micro.applied {
                        continue;
                    }
                    micro.bindings.replace(resolution.bound.clone());
                    if let Err(error) = supervisor
                        .reconfigure(&micro.id, resolution.unresolved.clone())
                        .await
                    {
                        failure.send_replace(Some(error.to_string()));
                    }
                    micro.applied = resolution;
                    // A failed eager start is recorded as that micro's `Faulted` state, which
                    // the lifecycle section shows; there is no other consumer for the report.
                    drop(supervisor.reconcile().await);
                }
                tag => unreachable!("the install watch selects no tag {tag}"),
            }
        }
        snapshot = installs
            .next(snapshot.revision)
            .await
            .expect("the install watch lives until the composition shuts down");
    }
}

/// Runtime for one composition: the host, the supervisor over the catalog, and the shell's
/// mirror of the declared `shell.roots` collection.
#[derive(Clone)]
pub struct CompositionRuntime {
    host: Host,
    supervisor: Rc<Supervisor<CapabilityPublication>>,
    components: NativeComponents,
    documents: DocumentService,
    collections: CollectionRegistry<BytesRegistrations>,
    micros: Vec<MicroId>,
    replica: ReplicaId,
    /// Each micro's collection requests, which a configuration grants (`set_binding`).
    collection_requests: Rc<BTreeMap<MicroId, Vec<clay_mdk::Request>>>,
    locked: BTreeSet<MicroId>,
    /// The shell's own instance: the recipient of the references minted from rows.
    shell: Instance,
    roots: Collection<String, RootRow>,
    /// Applies changes of the install document to the supervisor; taken by the one
    /// [`Self::serve`].
    reconcile_installs: Rc<RefCell<Option<LocalFuture<'static, ()>>>>,
    /// Close errors from the install reconciler, for the lifecycle section.
    lifecycle_failure: watch::Sender<Option<String>>,
    /// The roots mirror's feed loop, taken by the one [`Self::serve`].
    feed: Rc<RefCell<Option<LocalFuture<'static, ()>>>>,
    /// Writes the mock document source back to the store; taken by the one [`Self::serve`].
    persist_documents: Rc<RefCell<Option<LocalFuture<'static, ()>>>>,
    shutdown: Rc<Notify>,
    /// Installation defects the launcher or the root found while assembling the catalog
    /// (unreadable or manifest-less package files, a failed package-index fetch, a manifest
    /// contributing to an undeclared collection). Shown in the lifecycle section; never fatal,
    /// because one bad package must not take the shell down.
    pub installation_errors: Vec<String>,
}

/// A failed store read is a boot-time external error (disk, browser storage) the launcher
/// reports; a refused declaration is a catalog defect the caller records per micro.
async fn declare<St: OrderedStore + Send + Sync + 'static>(
    collections: &CollectionRegistry<BytesRegistrations>,
    store: &Arc<St>,
    contract: CollectionContract,
) -> Result<Result<BytesRegistrations, clay_host::DeclareError>, StoreError> {
    let registrations = durable_bytes_registrations(
        Arc::clone(store),
        contract.id.as_str().as_bytes().to_vec(),
        NonZeroUsize::new(256).unwrap(),
        [0, 0],
    )
    .await?;
    Ok(collections
        .declare(contract, registrations.clone())
        .map(|()| registrations))
}

/// The replica identity of this store: read from the `replica` subspace, or generated once and
/// stored there on the store's first boot, so it is the same on every later boot. It is the
/// replica half of every writer identity (`clay_collection::WriterId`), and so of every stamp
/// durable rows carry; changing it between boots would give unchanged rows a new writer and
/// break reinstall's keep-the-stamp rule. It lives in a subspace of its own, beside `documents`
/// and `collections`, as one 16-byte value under the key `id`.
async fn load_replica<St: OrderedStore>(store: &St) -> Result<ReplicaId, StoreError> {
    const KEY: &[u8] = b"id";
    if let Some(stored) = store.get(KEY).await? {
        let bytes = <[u8; ReplicaId::LEN]>::try_from(stored.as_slice())
            .expect("the replica id is stored as exactly ReplicaId::LEN bytes");
        return Ok(ReplicaId::from_bytes(bytes));
    }
    let mut bytes = [0; ReplicaId::LEN];
    getrandom::fill(&mut bytes).expect("the platform entropy source is available");
    store
        .apply(vec![BatchOp::Put(KEY.to_vec(), bytes.to_vec())])
        .await?;
    Ok(ReplicaId::from_bytes(bytes))
}

/// Install every manifest-declared row into the manifest installation scope of the collection
/// its contribution's binding resolved to. A binding configuration left unbound has nowhere to
/// put its rows: that is a configuration decision, not a defect, so those rows are skipped.
async fn install_declared_rows(
    collections: &CollectionRegistry<BytesRegistrations>,
    installed: &[InstalledMicro],
    resolutions: &[Resolution],
    errors: &mut Vec<String>,
) -> Result<(), StoreError> {
    for (micro, resolution) in installed.iter().zip(resolutions) {
        let id = &micro.id;
        let rows = match declared_rows(id, &micro.sidecar) {
            Ok(rows) => rows,
            Err(error) => {
                errors.push(format!("{id}: {error}"));
                continue;
            }
        };
        for (binding, rows) in rows {
            let Some(bound) = resolution.bound.get(&binding) else {
                continue;
            };
            let target = collections
                .collection(&bound.id)
                .expect("a resolved binding names a declared collection");
            target
                .install(
                    &manifest_scope(&SupervisorId::from(id)),
                    clay_collection::Retention::Installation,
                    collections.writer(&SupervisorId::from(id)),
                    rows,
                )
                .await?
                .dispatch();
        }
    }
    Ok(())
}

impl CompositionRuntime {
    /// Build the composition from the launcher's catalog: declare `shell.roots` and every
    /// collection the manifests declare, bind one mock configuration document per manifest
    /// configuration request (`<binding>-config`), mock-install each manifest's declared rows,
    /// and configure the supervisor. Nothing runs until [`Self::serve`].
    ///
    /// Collections load from, and manifest rows install into, `store`: reinstalling rows a
    /// previous boot already persisted keeps their stamps. The mock document source persists
    /// in the same store, so documents survive a restart until the node replaces it; the two
    /// live in disjoint subspaces (`collections` and `documents`), and a store written before
    /// that split (collections at the root) is orphaned, not migrated. The store is only
    /// needed while building (the collections and the document persister keep their own
    /// handles), so the runtime is not generic over it. A store failure is an external error
    /// returned to the launcher, which owns how it is shown; nothing has run by then.
    pub async fn new<St: OrderedStore + Send + Sync + 'static>(
        store: Arc<St>,
        installed: Vec<InstalledMicro>,
    ) -> Result<Self, StoreError> {
        let host = Host::new();
        let components = NativeComponents::new(&host);
        let (documents, persist_documents) =
            DocumentService::durable(Arc::new(Arc::clone(&store).subspace("documents")), 16)
                .await?;
        let replica = load_replica(&Arc::clone(&store).subspace("replica")).await?;
        let store = Arc::new(store.subspace("collections"));
        let collections = CollectionRegistry::new(replica);
        let mut errors = Vec::new();
        // Admission gate: a package whose sidecar is incoherent or declares an NSID outside its
        // authority (ADR 028 §5) is refused whole, before anything it names is declared.
        let installed: Vec<InstalledMicro> = installed
            .into_iter()
            .filter(|micro| match micro.sidecar.validate() {
                Ok(()) => true,
                Err(error) => {
                    errors.push(format!("{}: {error}", micro.id));
                    false
                }
            })
            .collect();
        // A declared collection's id is its contract's id today (see `DeclaredCollections`).
        let mut declared_collections = DeclaredCollections::default();

        let shell_roots = Nsid::literal(SHELL_ROOTS);
        let roots_collection = declare(
            &collections,
            &store,
            CollectionContract {
                id: shell_roots.clone(),
                key_schema: Nsid::literal("org.example.clay.shell.rootKey"),
                value_schema: Nsid::literal("org.example.clay.shell.root"),
            },
        )
        .await?
        .expect("the root declares shell.roots first");
        declared_collections.declare(&shell_roots, &shell_roots);
        for micro in &installed {
            for request in &micro.sidecar.manifest.configuration_requests {
                documents.mock_documents().bind_config(
                    request.binding.clone(),
                    format!("{}-config", request.binding),
                    "main".into(),
                );
            }
            for contract in &micro.sidecar.manifest.collection_contracts {
                let declared = declare(
                    &collections,
                    &store,
                    CollectionContract {
                        id: contract.id.clone(),
                        key_schema: contract.key_schema.clone(),
                        value_schema: contract.value_schema.clone(),
                    },
                )
                .await?;
                match declared {
                    Ok(_) => declared_collections.declare(&contract.id, &contract.id),
                    Err(error) => errors.push(format!("{}: {error}", micro.id)),
                }
            }
        }
        // Collection bindings resolve from the stored datum (or, with none, the manifest's
        // defaults) once every collection is declared; the factory reads the shared handle at
        // each initialize, and a contribution lands in whichever collection its binding resolved
        // to.
        let resolutions: Vec<Resolution> = installed
            .iter()
            .map(|micro| {
                let identity = &micro.id;
                bindings::resolve(
                    &micro.sidecar.manifest,
                    documents
                        .mock_documents()
                        .get(&bindings::bindings_route(identity))
                        .map(BindingsDatum::decode)
                        .as_ref(),
                    &declared_collections,
                )
            })
            .collect();
        install_declared_rows(&collections, &installed, &resolutions, &mut errors).await?;

        let enabled_at_boot = admit_installs(&documents, &installed);

        let resources = Resources::new(components.clone(), documents.clone(), collections.clone());
        let mut micros = Vec::new();
        let mut collection_requests = BTreeMap::new();
        let mut managed = Vec::new();
        let mut locked = BTreeSet::new();
        let mut configured = Vec::new();
        for (micro, resolution) in installed.into_iter().zip(resolutions) {
            let identity = micro.id.clone();
            micros.push(identity.clone());
            collection_requests.insert(
                identity.clone(),
                micro.sidecar.manifest.collection_requests.clone(),
            );
            if micro.locked {
                locked.insert(identity.clone());
            }
            let bindings = CollectionBindings::new(resolution.bound.clone());
            configured.push(ConfiguredMicro {
                // Locked micros have no datum and are always enabled.
                enabled: enabled_at_boot.get(&identity).copied().unwrap_or(true),
                identity: SupervisorId::from(&identity),
                eager: micro.eager,
                requirements: micro.requirements,
                unresolved_bindings: resolution.unresolved.clone(),
                factory: (micro.factory)(identity.clone(), resources.clone(), bindings.clone()),
            });
            managed.push(ManagedMicro {
                id: SupervisorId::from(&identity),
                manifest: micro.sidecar.manifest,
                bindings,
                applied: resolution,
            });
        }
        let supervisor = Rc::new(Supervisor::new(host.clone(), configured));
        let shell = host.start("shell");
        let (roots, feed) =
            Collection::observe(&host, &shell, supervisor.activator(), &roots_collection)
                .expect("the shell instance is live");
        let installs = watch_installs(&documents, &host, &shell);
        let (lifecycle_failure, _) = watch::channel(None);
        let reconcile_installs = reconcile_installs(
            installs,
            Rc::clone(&supervisor),
            managed,
            declared_collections,
            lifecycle_failure.clone(),
        );
        Ok(Self {
            host,
            supervisor,
            components,
            documents,
            collections,
            micros,
            replica,
            collection_requests: Rc::new(collection_requests),
            locked,
            shell,
            roots,
            feed: Rc::new(RefCell::new(Some(Box::pin(feed)))),
            persist_documents: Rc::new(RefCell::new(Some(Box::pin(persist_documents)))),
            reconcile_installs: Rc::new(RefCell::new(Some(Box::pin(reconcile_installs)))),
            lifecycle_failure,
            shutdown: Rc::new(Notify::new()),
            installation_errors: errors,
        })
    }

    /// Run the composition until [`Self::request_shutdown`]: start the eager roots, serve the
    /// supervisor's activations and idle closes, and mirror `shell.roots`. Micros and their
    /// drivers spawn local tasks, so everything runs inside one `LocalSet`, which is why the
    /// shell spawns this future itself rather than driving the supervisor from its UI tasks.
    /// On shutdown the micros close first, then the host (ADR 030 §6). Runs once.
    pub async fn serve(&self) {
        let feed = self
            .feed
            .borrow_mut()
            .take()
            .expect("the composition is served once");
        let persist_documents = self
            .persist_documents
            .borrow_mut()
            .take()
            .expect("the composition is served once");
        let reconcile_installs = self
            .reconcile_installs
            .borrow_mut()
            .take()
            .expect("the composition is served once");
        tokio::task::LocalSet::new()
            .run_until(async {
                tokio::task::spawn_local(feed);
                let persisting = tokio::task::spawn_local(persist_documents);
                let mut reconciling = tokio::task::spawn_local(reconcile_installs);
                let supervisor = Rc::clone(&self.supervisor);
                // A failed eager start is recorded as the micro's `Faulted` state, which the
                // lifecycle section shows; there is no other consumer for the report.
                tokio::task::spawn_local(async move { supervisor.reconcile().await });
                tokio::select! {
                    never = self.supervisor.drive() => never,
                    () = async {
                        self.shutdown.notified().await;
                        // The reconciler never ends on its own; stop it before the supervisor
                        // shuts down so it cannot enable or disable a micro mid-shutdown.
                        reconciling.abort();
                        match (&mut reconciling).await {
                            Err(error) if error.is_cancelled() => {}
                            Err(error) => std::panic::resume_unwind(error.into_panic()),
                            Ok(()) => unreachable!("the install reconciler never completes"),
                        }
                        self.supervisor
                            .shutdown()
                            .await
                            .expect("every micro closes in an orderly shutdown");
                        self.host.close();
                    } => {}
                }
                // Micros close first (above), so no document write can arrive once the queue
                // closes; closing then lets the persister drain what is queued and finish, and
                // awaiting it means everything written before shutdown is in the store when
                // `serve` returns. A persister panic propagates through the join.
                self.documents.close_persistence();
                persisting.await.expect("the document persister panicked");
            })
            .await;
    }

    /// Ask [`Self::serve`] to shut the composition down. Safe to call before it starts.
    pub fn request_shutdown(&self) {
        self.shutdown.notify_one();
    }

    pub fn supervisor(&self) -> &Rc<Supervisor<CapabilityPublication>> {
        &self.supervisor
    }

    pub fn host(&self) -> &Host {
        &self.host
    }

    pub fn documents(&self) -> &DocumentService {
        &self.documents
    }

    /// A declared collection, for inspection and tests.
    pub fn collection(&self, id: &Nsid) -> Option<BytesRegistrations> {
        self.collections.collection(id)
    }

    /// This store's replica identity (see [`load_replica`]).
    pub fn replica(&self) -> &ReplicaId {
        &self.replica
    }

    /// The shell's own instance: the recipient of the references minted from rows (a slot
    /// redeems them as the micro that mounts it), for a root that observes a collection.
    pub fn shell(&self) -> &Instance {
        &self.shell
    }

    /// The installed micros, in catalog order.
    pub fn micros(&self) -> &[MicroId] {
        &self.micros
    }

    /// Record the desired enablement of `micro` in its install datum, keeping everything else
    /// in it. The write happens when this is called; the returned future resolves once the
    /// datum is in the store, and may be dropped by callers that do not need that. The wait
    /// exists for the browser: a page reload does not run the orderly-shutdown drain of the
    /// document persister, so the shell must not let the user reload before the write landed.
    /// It only resolves while [`Self::serve`] runs, since the persister runs inside it. The
    /// supervisor follows through the reconciler's watch, not directly. Written host-side like
    /// admission (the node is mocked). Locked micros have no datum.
    pub fn set_enabled(&self, micro: &MicroId, enabled: bool) -> impl Future<Output = ()> + use<> {
        let installs = self.documents.mock_documents();
        let route = install_route(micro);
        let mut datum = InstallDatum::decode(
            installs
                .get(&route)
                .expect("an unlocked micro was admitted with an install datum"),
        );
        datum.enabled = enabled;
        installs.external_change(route, Some(datum.encode()));
        self.documents.persisted()
    }

    /// Configure `binding` of `micro` to the declared collection `collection`, or with `None`
    /// leave it deliberately unbound; both override the manifest's default. Binding records the
    /// grant: the contract and access the micro's request asks for right now, which a later
    /// package version must not widen without being configured again (ADR 028 §3). The write happens
    /// when this is called and the returned future resolves once it is stored (like
    /// [`Self::set_enabled`]). The supervisor follows through the reconciler: the micro is
    /// restarted with the new resolution, not rebound in place. The micro must be installed.
    pub fn set_binding(
        &self,
        micro: &MicroId,
        binding: &str,
        collection: Option<&Nsid>,
    ) -> impl Future<Output = ()> + use<> {
        let grant = collection.map(|collection| {
            let request = self.collection_requests[micro]
                .iter()
                .find(|request| request.binding == binding)
                .unwrap_or_else(|| panic!("{micro} requests no collection binding {binding:?}"));
            bindings::BindingGrant::of(request, collection.clone())
        });
        self.edit_bindings(micro, |datum| {
            datum.bindings.insert(binding.to_owned(), grant);
        });
        self.documents.persisted()
    }

    /// Remove the explicit configuration of `binding`, so the manifest's default applies again.
    pub fn clear_binding(
        &self,
        micro: &MicroId,
        binding: &str,
    ) -> impl Future<Output = ()> + use<> {
        self.edit_bindings(micro, |datum| {
            datum.bindings.remove(binding);
        });
        self.documents.persisted()
    }

    fn edit_bindings(&self, micro: &MicroId, edit: impl FnOnce(&mut BindingsDatum)) {
        assert!(self.micros.contains(micro), "{micro} is not installed");
        let installs = self.documents.mock_documents();
        let route = bindings::bindings_route(micro);
        let mut datum = installs
            .get(&route)
            .map(BindingsDatum::decode)
            .unwrap_or_default();
        edit(&mut datum);
        installs.external_change(route, Some(datum.encode()));
    }

    /// Whether the install datum asks for `micro` to run: the desired state, which the
    /// supervisor's state follows. Locked micros have no datum and are always wanted.
    pub fn is_enabled(&self, micro: &MicroId) -> bool {
        self.documents
            .mock_documents()
            .get(&install_route(micro))
            .is_none_or(|datum| InstallDatum::decode(datum).enabled)
    }

    /// The latest close error of a disable the reconciler applied; `None` until one fails.
    pub fn lifecycle_failures(&self) -> watch::Receiver<Option<String>> {
        self.lifecycle_failure.subscribe()
    }

    pub fn state(&self, micro: &MicroId) -> MicroState {
        self.supervisor.state(&SupervisorId::from(micro))
    }

    pub fn is_locked(&self, micro: &MicroId) -> bool {
        self.locked.contains(micro)
    }
}

/// The shell UI. `host` is what the launcher knows about its window: the whole viewport and the
/// system chrome overlapping it. The application is the frame the shell micro's root draws,
/// edge to edge: this pads it by the insets and hands the shell the size that remains, so the
/// shell and the kit below it never see an inset. It also provides the contexts in-process
/// presentations read: the native component host, the shell's `HostInputs` and the runtime
/// itself (the lifecycle section reads it).
#[component]
pub fn Application(runtime: Signal<CompositionRuntime>, host: HostInputs) -> Element {
    let shell = runtime.read().clone();
    use_context_provider(|| shell.components.clone());
    use_context_provider(|| shell.clone());
    let insets = host.insets;
    let (window_width, window_height) = (host.width, host.height);
    let frame_width = use_memo(move || {
        let insets = insets();
        (window_width() - insets.left - insets.right).max(1.0)
    });
    let frame_height = use_memo(move || {
        let insets = insets();
        (window_height() - insets.top - insets.bottom).max(1.0)
    });
    use_context_provider(|| HostInputs {
        width: frame_width.into(),
        height: frame_height.into(),
        ..host
    });
    // The composition outlives this scope's tasks: shutdown must keep being served after the
    // scope is gone, so it is spawned forever and stopped by `request_shutdown`.
    let serving = shell.clone();
    use_hook(move || {
        dioxus::core::spawn_forever(async move { serving.serve().await });
    });
    let stopping = shell.clone();
    use_drop(move || stopping.request_shutdown());

    let roots = use_rows(&shell.roots);
    let Insets {
        top,
        right,
        bottom,
        left,
    } = insets();
    let (width, height) = (window_width(), window_height());

    // The root's presentation is a row attachment: a lazy endpoint the shell redeems, which
    // starts the micro that publishes it. Arranging other micros' presentations is the root's
    // job (the shell's panels); the composition never pushes components.
    rsx! {
        div {
            class: "bg-background text-foreground",
            style: "box-sizing:border-box;overflow:hidden;width:{width}px;height:{height}px;padding:{top}px {right}px {bottom}px {left}px",
            for row in roots.iter() {
                div {
                    key: "{row.key}",
                    "aria-label": "{row.value.title}",
                    style: "width:100%;height:100%;min-width:0;min-height:0",
                    PresentationSlot {
                        reference: row.attachments[row.value.content.index()].clone(),
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
