//! Shared Dayframe composition: a catalog-driven composition root, its lifecycle controls, and
//! the `Application` shell UI. The root names no micro: launchers (desktop, web) hand it every
//! installed micro as an [`InstalledMicro`], a manifest plus its static configuration and the
//! engine-specific factory for it. Engine-specific guest factories (Wasmtime on desktop, `wcmp`
//! on the web) and native built-ins are both just catalog entries (ADR 018 §7).
//!
//! The catalog is explicit configuration, not a heuristic: each entry says whether the
//! supervisor starts the micro eagerly, which provider satisfies each of its requirements, and
//! which declared collections it may read or write. The root declares the collections the
//! manifests name and installs every manifest-declared row into the declaring micro's
//! manifest installation scope (ADR 025 §4), so rows are visible while their micro is stopped.
//! There is no supervisor reconfiguration yet: enablement and startup are fixed by the catalog
//! except for the enable/disable/retry controls of the lifecycle panel.
use clay_dioxus::use_component_recovery;
use clay_dioxus::NativeComponents;
use clay_guest_session::collections::durable_bytes_registrations;
use clay_guest_session::collections::BytesRegistrations;
use clay_host::CollectionContract;
use clay_host::ConfiguredMicro;
use clay_host::Factory;
use clay_host::Host;
use clay_host::Instance;
use clay_host::LocalFuture;
use clay_host::MicroId;
use clay_host::MicroState;
use clay_host::Requirement;
use clay_host::Supervisor;
use clay_mdk::native::declared_rows;
use clay_mdk::native::manifest_scope;
use clay_mdk::native::CapabilityPublication;
use clay_mdk::native::CollectionRegistry;
use clay_mdk::native::CollectionWatch;
use clay_mdk::native::DocumentService;
use clay_mdk::native::Resources;
use clay_mdk::native::RootRow;
use clay_mdk::native::SHELL_ROOTS;
use clay_mdk::view::use_rows;
use clay_mdk::view::AttachedRef;
use clay_mdk::view::PresentationSlot;
use clay_store::OrderedStore;
use clay_store::StoreError;
use dioxus::prelude::*;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::sync::Arc;
use tokio::sync::Notify;

/// One installed micro in the launcher-supplied catalog: its self-describing manifest, its
/// static configuration, and the factory that turns the root's resources into its supervisor
/// factory. Packaging (a component file a desktop scan read, an index entry a web shell
/// fetched, a native built-in the launcher links) stays in the launcher; this crate sees only
/// the catalog.
pub struct InstalledMicro {
    pub manifest: clay_mdk::MicroManifest,
    /// Started at boot rather than on first use.
    pub eager: bool,
    /// Offered no enable/disable controls: the shell cannot run without it.
    pub locked: bool,
    /// Local requirement binding -> the provider's publication that satisfies it.
    pub requirements: BTreeMap<String, Requirement>,
    pub factory:
        Box<dyn FnOnce(Resources<NativeComponents>) -> Rc<dyn Factory<CapabilityPublication>>>,
}

impl InstalledMicro {
    /// An on-demand, unlocked micro with no requirements.
    pub fn new(
        manifest: clay_mdk::MicroManifest,
        factory: impl FnOnce(Resources<NativeComponents>) -> Rc<dyn Factory<CapabilityPublication>>
            + 'static,
    ) -> Self {
        Self {
            manifest,
            eager: false,
            locked: false,
            requirements: BTreeMap::new(),
            factory: Box::new(factory),
        }
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
    locked: BTreeSet<MicroId>,
    /// The shell's own instance: it mounts roots and owns the references minted from rows.
    shell: Instance,
    roots: CollectionWatch<String, RootRow>,
    /// The roots mirror's feed loop, taken by the one [`Self::serve`].
    feed: Rc<RefCell<Option<LocalFuture<'static, ()>>>>,
    /// Writes the mock document source back to the store; taken by the one [`Self::serve`].
    persist_documents: Rc<RefCell<Option<LocalFuture<'static, ()>>>>,
    shutdown: Rc<Notify>,
    /// Installation defects the launcher or the root found while assembling the catalog
    /// (unreadable or manifest-less package files, a failed package-index fetch, a manifest
    /// contributing to an undeclared collection). Shown in the lifecycle panel; never fatal,
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
        contract.id.clone().into_bytes(),
        NonZeroUsize::new(256).unwrap(),
        [0, 0],
    )
    .await?;
    Ok(collections
        .declare(contract, registrations.clone())
        .map(|()| registrations))
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
    /// handles), so the runtime is not generic over it. A store failure is an external error returned to the launcher, which owns how it is
    /// shown; nothing has run by then.
    pub async fn new<St: OrderedStore + Send + Sync + 'static>(
        store: Arc<St>,
        installed: Vec<InstalledMicro>,
    ) -> Result<Self, StoreError> {
        let host = Host::new();
        let components = NativeComponents::new(&host);
        let (documents, persist_documents) =
            DocumentService::durable(Arc::new(Arc::clone(&store).subspace("documents")), 16)
                .await?;
        let store = Arc::new(store.subspace("collections"));
        let collections = CollectionRegistry::default();
        let mut errors = Vec::new();

        let roots_collection = declare(
            &collections,
            &store,
            CollectionContract {
                id: SHELL_ROOTS.into(),
                key_schema: "dayframe.shell.root-key".into(),
                value_schema: "dayframe.shell.root".into(),
            },
        )
        .await?
        .expect("the root declares shell.roots first");
        for micro in &installed {
            for request in &micro.manifest.configuration_requests {
                documents.mock_documents().bind_config(
                    request.binding.clone(),
                    format!("{}-config", request.binding),
                    "main".into(),
                );
            }
            for contract in &micro.manifest.collection_contracts {
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
                if let Err(error) = declared {
                    errors.push(format!("{}: {error}", micro.manifest.id.as_str()));
                }
            }
        }
        for micro in &installed {
            let id = MicroId::new(micro.manifest.id.as_str());
            let rows = match declared_rows(&micro.manifest) {
                Ok(rows) => rows,
                Err(error) => {
                    errors.push(format!("{id}: {error}"));
                    continue;
                }
            };
            for (collection, rows) in rows {
                let Some(target) = collections.collection(&collection) else {
                    errors.push(format!(
                        "{id} contributes to undeclared collection {collection}"
                    ));
                    continue;
                };
                target
                    .install(
                        &manifest_scope(&id),
                        clay_collection::Retention::Installation,
                        clay_collection::WriterId::new(id.as_str()),
                        rows,
                    )
                    .await?
                    .dispatch();
            }
        }

        let resources = Resources {
            registry: components.clone(),
            documents: documents.clone(),
            collections: collections.clone(),
        };
        let mut micros = Vec::new();
        let mut locked = BTreeSet::new();
        let mut configured = Vec::new();
        for micro in installed {
            let identity = MicroId::new(micro.manifest.id.as_str());
            micros.push(identity.clone());
            if micro.locked {
                locked.insert(identity.clone());
            }
            configured.push(ConfiguredMicro {
                identity,
                eager: micro.eager,
                requirements: micro.requirements,
                factory: (micro.factory)(resources.clone()),
            });
        }
        let supervisor = Rc::new(Supervisor::new(host.clone(), configured));
        let shell = host.start("shell");
        let (roots, feed) =
            CollectionWatch::observe(&host, &shell, supervisor.activator(), &roots_collection)
                .expect("the shell instance is live");
        Ok(Self {
            host,
            supervisor,
            components,
            documents,
            collections,
            micros,
            locked,
            shell,
            roots,
            feed: Rc::new(RefCell::new(Some(Box::pin(feed)))),
            persist_documents: Rc::new(RefCell::new(Some(Box::pin(persist_documents)))),
            shutdown: Rc::new(Notify::new()),
            installation_errors: errors,
        })
    }

    /// Run the composition until [`Self::request_shutdown`]: start the eager roots, serve the
    /// supervisor's activations and idle closes, and mirror `shell.roots`. Micros and their
    /// drivers spawn local tasks, so everything runs inside one `LocalSet`, which is why the
    /// shell spawns this future itself rather than driving the supervisor from its UI tasks.
    /// On shutdown the micros close first, then the host (ADR 024 §6). Runs once.
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
        tokio::task::LocalSet::new()
            .run_until(async {
                tokio::task::spawn_local(feed);
                tokio::task::spawn_local(persist_documents);
                let supervisor = Rc::clone(&self.supervisor);
                // A failed eager start is recorded as the micro's `Faulted` state, which the
                // lifecycle panel shows; there is no other consumer for the report.
                tokio::task::spawn_local(async move { supervisor.reconcile().await });
                tokio::select! {
                    never = self.supervisor.drive() => never,
                    () = async {
                        self.shutdown.notified().await;
                        self.supervisor
                            .shutdown()
                            .await
                            .expect("every micro closes in an orderly shutdown");
                        self.host.close();
                    } => {}
                }
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
    pub fn collection(&self, id: &str) -> Option<BytesRegistrations> {
        self.collections.collection(id)
    }

    /// The installed micros, in catalog order.
    pub fn micros(&self) -> &[MicroId] {
        &self.micros
    }

    pub fn state(&self, micro: &MicroId) -> MicroState {
        self.supervisor.state(micro)
    }

    pub fn is_locked(&self, micro: &MicroId) -> bool {
        self.locked.contains(micro)
    }
}

#[component]
pub fn Application(runtime: Signal<CompositionRuntime>) -> Element {
    let shell = runtime.read().clone();
    use_context_provider(|| shell.components.clone());
    use_component_recovery(shell.components.clone());
    // The composition outlives this scope's tasks: shutdown must keep being served after the
    // scope is gone, so it is spawned forever and stopped by `request_shutdown`.
    let serving = shell.clone();
    use_hook(move || {
        dioxus::core::spawn_forever(async move { serving.serve().await });
    });
    let stopping = shell.clone();
    use_drop(move || stopping.request_shutdown());

    let mut states = use_signal(|| 0_u64);
    let supervisor = Rc::clone(&shell.supervisor);
    use_future(move || {
        let mut changes = supervisor.changes();
        async move {
            while changes.changed().await.is_ok() {
                states += 1;
            }
        }
    });
    let _ = states();
    let mut failure = use_signal(|| None::<String>);
    let mut settings_open = use_signal(|| true);
    let roots = use_rows(&shell.roots);

    rsx! {
        section { class: "flex flex-col gap-6 rounded-lg border border-border bg-surface p-6 text-foreground",
            header { class: "flex flex-wrap items-center gap-4",
                h1 { class: "text-2xl font-semibold", "Dayframe" }
                button {
                    class: "rounded-md border border-border px-4 py-2 focus-visible:outline",
                    "aria-label": if settings_open() { "Close Settings" } else { "Open Settings" },
                    onclick: move |_| settings_open.toggle(),
                    if settings_open() { "Close Settings" } else { "Open Settings" }
                }
            }
            if settings_open() {
                for row in roots {
                    div { key: "{row.key}", "aria-label": "{row.value.title}",
                        // The root's presentation is a row attachment: a lazy endpoint the
                        // shell redeems, which starts the micro that publishes it. Guest-written
                        // mounting micros (workspace layout, canvas) place other components
                        // through the wire-level slot primitive of ADR 015 §10.
                        PresentationSlot {
                            owner: shell.shell.clone(),
                            reference: AttachedRef::new(
                                row.attachments[row.value.content.index()].share(),
                            ),
                        }
                    }
                }
            }
            details { class: "rounded-md border border-border p-4",
                summary { "Micro lifecycle controls" }
                p { "Configuration uses document facets backed by the in-memory mock node; it is not saved across restarts." }
                for error in shell.installation_errors.clone() {
                    p { key: "install-{error}", role: "status", "Package installation failed: {error}" }
                }
                if let Some(problem) = failure() {
                    p { role: "alert", "{problem}" }
                }
                for micro in shell.micros() {
                    p {
                        key: "status-{micro}",
                        role: "status",
                        {
                            let state = shell.state(micro);
                            if shell.is_locked(micro) {
                                format!("{micro} runtime (locked): {state:?}")
                            } else {
                                format!("{micro} runtime: {state:?}")
                            }
                        }
                    }
                }
                div { class: "mt-4 flex flex-wrap gap-2",
                    for micro in shell.micros().to_vec() {
                        if !shell.is_locked(&micro) {
                            {
                                let supervisor = Rc::clone(&shell.supervisor);
                                let state = shell.state(&micro);
                                let toggle = micro.clone();
                                let label = if state == MicroState::Disabled {
                                    format!("Enable {micro} micro")
                                } else {
                                    format!("Disable {micro} micro")
                                };
                                let retry = matches!(state, MicroState::Faulted(_)).then(|| {
                                    (Rc::clone(&shell.supervisor), micro.clone())
                                });
                                rsx! {
                                    button {
                                        key: "toggle-{micro}",
                                        class: "rounded-md border border-border px-4 py-2 focus-visible:outline",
                                        "aria-label": "{label}",
                                        onclick: move |_| {
                                            let supervisor = Rc::clone(&supervisor);
                                            let micro = toggle.clone();
                                            spawn(async move {
                                                if supervisor.state(&micro) == MicroState::Disabled {
                                                    supervisor.enable(&micro);
                                                } else if let Err(error) = supervisor.disable(&micro).await {
                                                    failure.set(Some(error.to_string()));
                                                }
                                            });
                                        },
                                        "{label}"
                                    }
                                    if let Some((supervisor, micro)) = retry {
                                        button {
                                            key: "retry-{micro}",
                                            class: "rounded-md border border-border px-4 py-2 focus-visible:outline",
                                            "aria-label": "Retry {micro} micro",
                                            onclick: move |_| supervisor.retry(&micro),
                                            "Retry {micro} micro"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
