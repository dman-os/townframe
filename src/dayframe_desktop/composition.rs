//! Desktop composition: explicit provider linkage, configuration source, and lifecycle controls.
use clay_dioxus::ComponentMount;
use clay_dioxus::NativeComponents;
use clay_dioxus::use_component_recovery;
use clay_micro_core::application::ApplicationRuntime;
use clay_micro_core::micros::MicroFactory;
use clay_micro_core::micros::MicroId;
use clay_micro_core::micros::composition::ConfigurationError;
use clay_micro_core::micros::composition::ConfigurationSnapshot;
use clay_micro_core::micros::composition::ConfigurationSource;
use clay_micro_core::micros::composition::ConfiguredMicro;
use clay_micro_core::micros::composition::Enablement;
use clay_micro_core::micros::composition::InstanceState;
use clay_micro_core::micros::composition::MockConfiguration;
use dioxus::prelude::*;
use std::collections::BTreeMap;

type DesktopApplication = ApplicationRuntime<NativeComponents>;

/// The micros this binary links, as providers the runtime registers under their manifest ids.
/// This table is the only place the desktop names a micro crate: each entry is a manifest plus
/// its backend (an `activate` for a native micro, a prepared package for a wasm one). What a
/// micro contributes to whom is declared in its manifest and routed by the backend, so adding a
/// micro here never names another micro's contract.
fn linked_providers(application: &DesktopApplication) -> Vec<MicroFactory> {
    vec![
        clay_mdk::native::provider(
            application,
            micro_settings_manifest::manifest(),
            micro_settings::activate,
        ),
        clay_mdk::native::provider(
            application,
            micro_sample::manifest(),
            micro_sample::activate,
        ),
        clay_mdk::wasm::provider(
            application,
            micro_counter_manifest::manifest(),
            clay_mdk::wasm::component_package("counter"),
        ),
    ]
}

#[derive(Clone)]
pub(crate) struct DesktopRuntime {
    pub(crate) application: DesktopApplication,
    configuration: MockConfiguration,
    states: BTreeMap<String, InstanceState>,
}

impl DesktopRuntime {
    pub(crate) fn new() -> Self {
        let mut application = DesktopApplication::new();
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

        for provider in linked_providers(&application) {
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

    #[cfg(test)]
    fn apply_now(&mut self) -> Result<(), ConfigurationError> {
        let states = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(self.application.reconcile_configuration())?;
        self.apply_states(states);
        Ok(())
    }

    fn apply_states(&mut self, states: BTreeMap<String, InstanceState>) {
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

    fn desired(&self) -> BTreeMap<String, ConfiguredMicro> {
        let ConfigurationSnapshot::Ready(desired) = self.configuration.current().unwrap() else {
            unreachable!("in-memory desktop configuration is always hydrated")
        };
        desired
    }

    fn is_enabled(&self, instance: &str) -> bool {
        self.desired()
            .get(instance)
            .is_some_and(|entry| entry.enablement == Enablement::Enabled)
    }

    fn is_locked(&self, instance: &str) -> bool {
        self.application.is_locked(instance)
    }

    fn set_enablement(
        &mut self,
        instance: &str,
        enablement: Enablement,
    ) -> Result<(), ConfigurationError> {
        self.configuration.set_enablement(instance, enablement)
    }
}

#[component]
pub(crate) fn Application(mut runtime: Signal<DesktopRuntime>) -> Element {
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
mod counter_tests;

#[cfg(test)]
mod tests;
