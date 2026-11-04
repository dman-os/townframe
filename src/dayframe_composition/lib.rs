//! Shared Dayframe composition: explicit provider linkage, configuration source, lifecycle
//! controls, and the `Application` shell UI. Engine-specific guest providers (Wasmtime on
//! desktop, `wcmp` on the web) are injected by the launch shells; this crate owns only the
//! engine-neutral half of the composition root (ADR 018 §7).
use clay_dioxus::use_component_recovery;
use clay_dioxus::ComponentMount;
use clay_dioxus::NativeComponents;
use clay_micro_core::application::ApplicationRuntime;
use clay_micro_core::micros::composition::ConfigurationError;
use clay_micro_core::micros::composition::ConfigurationSnapshot;
use clay_micro_core::micros::composition::ConfigurationSource;
use clay_micro_core::micros::composition::ConfiguredMicro;
use clay_micro_core::micros::composition::Enablement;
use clay_micro_core::micros::composition::InstanceState;
use clay_micro_core::micros::composition::MockConfiguration;
use clay_micro_core::micros::MicroFactory;
use clay_micro_core::micros::MicroId;
use dioxus::prelude::*;
use std::collections::BTreeMap;

pub type CompositionApplication = ApplicationRuntime<NativeComponents>;

/// Runtime for one composition: the application with its registered providers, the in-memory
/// configuration source, and the reconciled instance states. Shells construct it with their
/// engine's guest providers; the native micros (settings, sample) and the configuration maps are
/// engine-neutral and always linked here.
#[derive(Clone)]
pub struct CompositionRuntime {
    pub application: CompositionApplication,
    configuration: MockConfiguration,
    pub states: BTreeMap<String, InstanceState>,
}

impl CompositionRuntime {
    /// Build the composition with engine-neutral native providers linked and `guest_providers`
    /// registered beside them (the counter and any other wasm micro).
    pub fn new(guest_providers: impl FnOnce(&CompositionApplication) -> Vec<MicroFactory>) -> Self {
        let mut application = CompositionApplication::new();
        micro_sample::bind_mock_defaults(&application.documents());
        let documents = application.mock_documents();
        documents.bind_config(
            micro_counter_manifest::CONFIG_NAME.into(),
            "counter-config".into(),
            "main".into(),
        );
        documents.seed(
            clay_docs::FacetRoute {
                document: "counter-config".into(),
                branch: "main".into(),
                tag: micro_counter_manifest::CONFIG_TAG.into(),
                key_id: micro_counter_manifest::CONFIG_KEY.into(),
            },
            serde_json::json!(0),
        );

        let mut providers = vec![
            clay_mdk::native::provider(
                &application,
                micro_settings_manifest::manifest(),
                micro_settings::activate,
            ),
            clay_mdk::native::provider(
                &application,
                micro_sample::manifest(),
                micro_sample::activate,
            ),
        ];
        providers.extend(guest_providers(&application));
        for provider in providers {
            application.register_provider(provider).unwrap();
        }

        let locked = BTreeMap::from([(
            "settings".into(),
            ConfiguredMicro {
                provider: MicroId::new("dayframe.settings"),
                enablement: Enablement::Enabled,
                bindings: BTreeMap::new(),
            },
        )]);
        let dynamic = BTreeMap::from([
            (
                "sample".into(),
                ConfiguredMicro {
                    provider: MicroId::new("dayframe.sample"),
                    enablement: Enablement::Enabled,
                    bindings: BTreeMap::from([("settings.sections".into(), "settings".into())]),
                },
            ),
            (
                "counter".into(),
                ConfiguredMicro {
                    provider: MicroId::new(micro_counter_manifest::ID),
                    enablement: Enablement::Enabled,
                    bindings: BTreeMap::from([("settings.sections".into(), "settings".into())]),
                },
            ),
        ]);
        let configuration = MockConfiguration::with_locked(locked, dynamic);
        application.install_configuration_source(configuration.clone());

        Self {
            application,
            configuration,
            states: BTreeMap::new(),
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

    pub fn apply_states(&mut self, states: BTreeMap<String, InstanceState>) {
        // Activation failures are unexpected for these linked providers and mock grants.
        // Do not conceal one behind an empty panel. Blocked/stopped states are legitimate lifecycle states.
        assert!(
            !states
                .values()
                .any(|state| matches!(state, InstanceState::Failed(_))),
            "micro activation failed: {states:?}"
        );
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
    ) -> Result<(), ConfigurationError> {
        self.configuration.set_enablement(instance, enablement)
    }
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
                                            runtime.write().set_enablement(&target, next).unwrap();
                                        },
                                        "{label}"
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
