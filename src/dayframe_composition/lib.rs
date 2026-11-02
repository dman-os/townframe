//! Shared Dayframe composition: a catalog-driven composition root, its configuration source,
//! lifecycle controls, and the `Application` shell UI. The root names no micro: launchers
//! (desktop, web) hand it every installed micro as an [`InstalledMicro`] — a manifest plus
//! the engine-specific provider factory for it — together with the locked boot entries.
//! Engine-specific guest providers (Wasmtime on desktop, `wcmp` on the web) and native
//! built-ins are both just catalog entries (ADR 018 §7). A fresh composition facet is seeded
//! with one enabled, on-demand instance per installed micro, named by its full provider id,
//! each requirement bound to the unique installed provider of that contract (left unbound,
//! and reported by the reconciler, when there is not exactly one).
use clay_dioxus::ComponentMount;
use clay_dioxus::NativeComponents;
use clay_dioxus::use_component_recovery;
use clay_docs::Operation;
use clay_docs::OperationReceipt;
use clay_micro_core::application::ApplicationRuntime;
use clay_micro_core::micros::MicroFactory;
use clay_micro_core::micros::MicroId;
use clay_micro_core::micros::composition::ConfigurationError;
use clay_micro_core::micros::composition::ConfigurationSnapshot;
use clay_micro_core::micros::composition::ConfigurationSource;
use clay_micro_core::micros::composition::ConfiguredMicro;
use clay_micro_core::micros::composition::Enablement;
use clay_micro_core::micros::composition::FacetConfiguration;
use clay_micro_core::micros::composition::InstanceState;
use dioxus::prelude::*;
use std::collections::BTreeMap;

pub type CompositionApplication = ApplicationRuntime<NativeComponents>;

/// One installed micro in the launcher-supplied catalog: its self-describing manifest and
/// the factory that turns the composition application into its provider. Packaging (a
/// component file a desktop scan read, an index entry a web shell fetched, a native
/// built-in the launcher links) stays in the launcher; this crate sees only the catalog.
pub struct InstalledMicro {
    pub manifest: clay_mdk::MicroManifest,
    pub provider: Box<dyn FnOnce(&CompositionApplication) -> MicroFactory>,
}

impl InstalledMicro {
    pub fn new(
        manifest: clay_mdk::MicroManifest,
        provider: impl FnOnce(&CompositionApplication) -> MicroFactory + 'static,
    ) -> Self {
        Self {
            manifest,
            provider: Box::new(provider),
        }
    }
}

/// Runtime for one composition: the application with its registered providers, the in-memory
/// configuration source, and the reconciled instance states.
#[derive(Clone)]
pub struct CompositionRuntime {
    pub application: CompositionApplication,
    configuration: FacetConfiguration,
    pub states: BTreeMap<String, InstanceState>,
    /// Installation defects the launcher found while assembling the catalog (unreadable or
    /// manifest-less package files, a failed package-index fetch). Shown in the lifecycle
    /// panel; never fatal, because one bad package must not take the shell down.
    pub installation_errors: Vec<String>,
}

impl CompositionRuntime {
    /// Build the composition from the launcher's catalog: register every installed
    /// provider, bind one mock configuration document per manifest config grant
    /// (`<name>-config`), and seed the composition facet — only when absent — with one
    /// enabled on-demand instance per installed micro that `locked` does not already pin.
    pub fn new(installed: Vec<InstalledMicro>, locked: BTreeMap<String, ConfiguredMicro>) -> Self {
        let mut application = CompositionApplication::new();
        let documents = application.mock_documents();
        let manifests: Vec<clay_mdk::MicroManifest> = installed
            .iter()
            .map(|micro| micro.manifest.clone())
            .collect();
        for manifest in &manifests {
            if let Some(config) = &manifest.config {
                documents.bind_config(
                    config.name.clone(),
                    format!("{}-config", config.name),
                    "main".into(),
                );
            }
        }
        for micro in installed {
            let provider = (micro.provider)(&application);
            application.register_provider(provider).unwrap();
        }

        let seeded = seeded_instances(&manifests, &locked);
        let route = clay_docs::FacetRoute {
            document: "composition-config".into(),
            branch: "main".into(),
            tag: "org.dayframe.composition.config".into(),
            key_id: "main".into(),
        };
        documents.seed(route.clone(), serde_json::to_value(seeded).unwrap());
        let access = application
            .documents()
            .grant_documents(
                &application.host(),
                &application.root(),
                vec![clay_docs::DocumentGrant {
                    document: route.document.clone(),
                    branch: route.branch.clone(),
                    facets: vec![daybook_types::manifest::RoutineFacetAccess {
                        tag: route.tag.clone().into(),
                        key_id: Some(route.key_id.clone()),
                        read: true,
                        write: true,
                        ..Default::default()
                    }],
                }],
            )
            .unwrap();
        let facet = access
            .document(route.document, route.branch)
            .unwrap()
            .facet(route.tag, route.key_id)
            .unwrap();
        let configuration = FacetConfiguration::new(facet, locked);
        application.install_configuration_source(configuration.clone());

        Self {
            application,
            configuration,
            states: BTreeMap::new(),
            installation_errors: Vec::new(),
        }
    }

    /// Reconcile configuration immediately on a current-thread runtime (tests and headless setup).
    pub fn apply_now(&mut self) -> Result<(), ConfigurationError> {
        let states = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(self.application.reconcile_configuration())?;
        self.apply_states(states);
        Ok(())
    }

    /// Every reconciler state is a legitimate shell state, `Failed`/`Faulted` included: the
    /// reconciler retains a failure until configuration changes, and the shell's job is to show
    /// it in the micro's status row rather than tear the whole application down (ADR 014
    /// §Composition and lifecycle). Nothing is concealed behind an empty panel because a failed
    /// micro publishes no root.
    pub fn apply_states(&mut self, states: BTreeMap<String, InstanceState>) {
        self.states = states;
    }

    pub fn desired(&self) -> BTreeMap<String, ConfiguredMicro> {
        let ConfigurationSnapshot::Ready(desired) = self.configuration.current().unwrap() else {
            unreachable!("in-memory composition configuration is always hydrated")
        };
        desired
    }

    pub fn is_enabled(&self, instance: &str) -> bool {
        self.desired()
            .get(instance)
            .is_some_and(|entry| entry.enablement == Enablement::Enabled)
    }

    pub fn is_locked(&self, instance: &str) -> bool {
        self.application.is_locked(instance)
    }

    pub fn set_enablement(
        &mut self,
        instance: &str,
        enablement: Enablement,
    ) -> Result<Operation, ConfigurationError> {
        self.configuration.set_enablement(instance, enablement)
    }

    pub fn set_eager(&self, instance: &str, eager: bool) -> Result<Operation, ConfigurationError> {
        self.configuration.set_eager(instance, eager)
    }
}

/// The fresh-composition seed: one enabled, on-demand instance per installed micro that
/// `locked` does not already pin a provider for, named by the full provider id. A
/// requirement binds to the unique configured instance (locked or seeded) whose provider
/// declares that contract; with zero or several candidates it stays unbound and the
/// reconciler reports the instance blocked rather than guessing.
fn seeded_instances(
    manifests: &[clay_mdk::MicroManifest],
    locked: &BTreeMap<String, ConfiguredMicro>,
) -> BTreeMap<String, ConfiguredMicro> {
    let by_id: BTreeMap<&str, &clay_mdk::MicroManifest> = manifests
        .iter()
        .map(|manifest| (manifest.id.as_str(), manifest))
        .collect();
    let locked_providers: std::collections::BTreeSet<&str> = locked
        .values()
        .map(|configured| configured.provider.as_str())
        .collect();
    let mut instances: Vec<(String, &clay_mdk::MicroManifest)> = locked
        .iter()
        .filter_map(|(name, configured)| {
            by_id
                .get(configured.provider.as_str())
                .map(|manifest| (name.clone(), *manifest))
        })
        .collect();
    for manifest in manifests {
        if !locked_providers.contains(manifest.id.as_str()) {
            instances.push((manifest.id.as_str().to_owned(), manifest));
        }
    }
    instances
        .iter()
        .filter(|(name, _)| !locked.contains_key(name))
        .map(|(name, manifest)| {
            let mut bindings = BTreeMap::new();
            for requirement in &manifest.requirements {
                let mut candidates = instances
                    .iter()
                    .filter(|(_, provider)| {
                        provider
                            .provisions
                            .iter()
                            .any(|provision| provision.id == requirement.id)
                    })
                    .map(|(candidate, _)| candidate);
                if let (Some(only), None) = (candidates.next(), candidates.next()) {
                    bindings.insert(requirement.id.clone(), only.clone());
                }
            }
            (
                name.clone(),
                ConfiguredMicro {
                    provider: MicroId::new(manifest.id.as_str()),
                    enablement: Enablement::Enabled,
                    eager: false,
                    bindings,
                },
            )
        })
        .collect()
}

#[component]
pub fn Application(mut runtime: Signal<CompositionRuntime>) -> Element {
    let host = runtime.read().application.host();
    use_drop(move || host.close());
    use_context_provider(|| runtime.read().application.renderer());
    use_component_recovery(runtime.read().application.renderer());
    let application = runtime.read().application.clone();
    use_future(move || {
        let application = application.clone();
        async move { application.observe().await }
    });
    let reports = runtime.read().application.reports();
    use_future(move || {
        let mut reports = reports.clone();
        async move {
            loop {
                let states = reports
                    .borrow_and_update()
                    .clone()
                    .expect("invalid micro configuration");
                runtime.write().apply_states(states);
                if reports.changed().await.is_err() {
                    break;
                }
            }
        }
    });

    let mut receipt = use_signal(|| None::<OperationReceipt>);
    let mut track = move |operation: Operation| {
        let initial = operation.current();
        receipt.set(Some(initial.clone()));
        spawn(async move {
            match operation.next(initial.revision).await.unwrap() {
                Some(completed)
                    if receipt
                        .peek()
                        .as_ref()
                        .is_some_and(|current| current.id == completed.id) =>
                {
                    receipt.set(Some(completed));
                }
                _ => {}
            }
        });
    };
    let mut settings_open = use_signal(|| true);
    let roots = clay_mdk::view::use_collection(&runtime.read().application.roots());

    let configured_names: Vec<String> = runtime.read().desired().into_keys().collect();

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
                for (id, root) in roots.query(&clay_mdk::Query::new()) {
                    div { key: "{id}", "aria-label": "{root.title}",
                        // Guest-written mounting micros (workspace layout, canvas) place other
                        // components through the wire-level slot primitive of ADR 015 §10;
                        // `ComponentMount` is its native backing.
                        ComponentMount {
                            owner: runtime.read().application.root(),
                            reference: root.content,
                        }
                    }
                }
            }
            details { class: "rounded-md border border-border p-4",
                summary { "Mock node and micro lifecycle controls" }
                p { "Configuration uses document facets backed by the in-memory mock node; it is not saved across restarts." }
                for error in runtime.read().installation_errors.clone() {
                    p { key: "install-{error}", role: "status", "Package installation failed: {error}" }
                }
                if let Some(receipt) = receipt() {
                    p { role: "status", "Composition operation {receipt.id}: {receipt.status:?}" }
                }
                for name in &configured_names {
                    p {
                        key: "status-{name}",
                        role: "status",
                        {
                            let state = runtime.read().states.get(name).cloned();
                            let locked = runtime.read().is_locked(name);
                            if locked {
                                format!("{name} runtime (locked): {state:?}")
                            } else {
                                format!("{name} runtime: {state:?}")
                            }
                        }
                    }
                }
                div { class: "mt-4 flex flex-wrap gap-2",
                    for name in configured_names {
                        if !runtime.read().is_locked(&name) {
                            {
                                let target = name.clone();
                                let startup_target = name.clone();
                                let eager = runtime.read().desired()[&name].eager;
                                let enabled = runtime.read().is_enabled(&name);
                                let label = if enabled {
                                    format!("Disable {name} micro")
                                } else {
                                    format!("Enable {name} micro")
                                };
                                rsx! {
                                    button {
                                        key: "toggle-{name}",
                                        class: "rounded-md border border-border px-4 py-2 focus-visible:outline",
                                        "aria-label": "{label}",
                                        onclick: move |_| {
                                            let next = if runtime.read().is_enabled(&target) {
                                                Enablement::Disabled
                                            } else {
                                                Enablement::Enabled
                                            };
                                            track(runtime.write().set_enablement(&target, next).unwrap());
                                        },
                                        "{label}"
                                    }
                                    button {
                                        class: "rounded-md border border-border px-4 py-2 focus-visible:outline",
                                        "aria-label": "Start {name} eagerly",
                                        "aria-pressed": eager,
                                        onclick: move |_| {
                                            let next = !runtime.read().desired()[&startup_target].eager;
                                            track(runtime.read().set_eager(&startup_target, next).unwrap());
                                        },
                                        if eager { "{name}: eager startup" } else { "{name}: start on demand" }
                                    }
                                }
                            }
                        }
                    }
                    button {
                        class: "rounded-md border border-border px-4 py-2 focus-visible:outline",
                        "aria-label": "Accept pending writes",
                        onclick: move |_| runtime.read().application.mock_documents().settle(true),
                        "Accept pending writes"
                    }
                    button {
                        class: "rounded-md border border-border px-4 py-2 focus-visible:outline",
                        "aria-label": "Reject and roll back",
                        onclick: move |_| runtime.read().application.mock_documents().settle(false),
                        "Reject and roll back"
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
