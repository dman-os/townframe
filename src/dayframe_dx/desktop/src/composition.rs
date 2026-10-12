//! Desktop composition: provider registration, configured instances, and desktop-only controls.
use dayframe_app::{
    application::ApplicationRuntime,
    documents::FacetRoute,
    micros::{
        self,
        composition::{ConfiguredMicro, Enablement, InstanceState},
        MicroId,
    },
    native::{use_component_recovery, ComponentMount},
};
use dioxus::prelude::*;
use std::collections::BTreeMap;

use dayframe_app::micros::sample::{SampleConfig, CONFIG_KEY, CONFIG_TAG};

struct DesktopRuntime {
    application: ApplicationRuntime,
    // Mock projection of owner configuration, not a separate persistence or permissions store.
    configuration: BTreeMap<String, ConfiguredMicro>,

    states: BTreeMap<String, InstanceState>,
}

impl DesktopRuntime {
    fn new() -> Self {
        let mut application = ApplicationRuntime::new();
        let host = application.host();
        host.mock_documents()
            .bind_config("sample".into(), "sample-config".into(), "main".into());
        host.mock_documents().seed(
            sample_route(),
            serde_json::to_value(SampleConfig {
                enabled: true,
                greeting: "Hello from a micro".into(),
            })
            .unwrap(),
        );

        micros::register_native(&mut application).unwrap();
        let configuration = BTreeMap::from([
            (
                "settings".into(),
                ConfiguredMicro {
                    provider: MicroId::new("dayframe.settings"),
                    enablement: Enablement::Enabled,
                    bindings: BTreeMap::new(),
                },
            ),
            (
                "sample".into(),
                ConfiguredMicro {
                    provider: MicroId::new("dayframe.sample"),
                    enablement: Enablement::Enabled,
                    bindings: BTreeMap::from([("settings.sections".into(), "settings".into())]),
                },
            ),
        ]);

        let mut runtime = Self {
            application,
            configuration,

            states: BTreeMap::new(),
        };
        runtime.apply_configuration();
        runtime
    }

    fn apply_configuration(&mut self) {
        let states = self.application.configure(&self.configuration).unwrap();
        self.apply_states(states);
    }

    fn apply_states(&mut self, states: BTreeMap<String, InstanceState>) {
        // Activation failures are unexpected for these linked demo providers and mock grants.
        // Do not conceal one behind an empty panel. Blocked/stopped states are legitimate lifecycle states.
        assert!(
            !states
                .values()
                .any(|state| matches!(state, InstanceState::Failed(_))),
            "demo micro activation failed: {states:?}"
        );
        self.states = states;
    }

    fn sample_enabled(&self) -> bool {
        self.configuration["sample"].enablement == Enablement::Enabled
    }

    fn enable_sample(&mut self) {
        self.configuration.get_mut("sample").unwrap().enablement = Enablement::Enabled;
        self.apply_configuration();
    }

    fn disable_sample(&mut self) {
        self.configuration.get_mut("sample").unwrap().enablement = Enablement::Disabled;
        self.apply_configuration();
    }
}

fn sample_route() -> FacetRoute {
    FacetRoute {
        document: "sample-config".into(),
        branch: "main".into(),
        tag: CONFIG_TAG.into(),
        key_id: CONFIG_KEY.into(),
    }
}

#[component]
pub fn Application() -> Element {
    let mut runtime = use_signal(DesktopRuntime::new);
    let host = runtime.read().application.host();
    use_drop(move || host.close());
    use_component_recovery(runtime.read().application.native());
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
                    .expect("invalid demo micro configuration");
                runtime.write().apply_states(states);
                if reports.changed().await.is_err() {
                    break;
                }
            }
        }
    });

    let mut settings_open = use_signal(|| true);
    let reader = runtime.read().application.roots();
    let mut roots = use_signal(|| reader.snapshot().unwrap());
    use_future(move || {
        let mut changes = reader.subscribe().unwrap();
        async move {
            loop {
                roots.set(changes.borrow_and_update().clone());
                if changes.changed().await.is_err() {
                    break;
                }
            }
        }
    });

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
                for (id, root) in roots() {
                    div { key: "{id}", "aria-label": "{root.title}",
                        ComponentMount {
                            components: runtime.read().application.native(),
                            owner: runtime.read().application.root(),
                            reference: root.content,
                        }
                    }
                }
            }
            details { class: "rounded-md border border-border p-4",
                summary { "Mock node and micro lifecycle controls" }
                p { role: "status", {format!("Sample runtime: {:?}", runtime.read().states["sample"])} }
                div { class: "mt-4 flex flex-wrap gap-2",
                    button {
                        class: "rounded-md border border-border px-4 py-2 focus-visible:outline",
                        "aria-label": if runtime.read().sample_enabled() { "Disable sample micro" } else { "Enable sample micro" },
                        onclick: move |_| {
                            if runtime.read().sample_enabled() {
                                runtime.write().disable_sample();
                            } else {
                                runtime.write().enable_sample();
                            }
                        },
                        if runtime.read().sample_enabled() { "Disable sample micro" } else { "Enable sample micro" }
                    }
                    button {
                        class: "rounded-md border border-border px-4 py-2 focus-visible:outline",
                        "aria-label": "Accept pending writes",
                        onclick: move |_| runtime.read().application.host().mock_documents().settle(true),
                        "Accept pending writes"
                    }
                    button {
                        class: "rounded-md border border-border px-4 py-2 focus-visible:outline",
                        "aria-label": "Reject and roll back",
                        onclick: move |_| runtime.read().application.host().mock_documents().settle(false),
                        "Reject and roll back"
                    }
                    button {
                        class: "rounded-md border border-border px-4 py-2 focus-visible:outline",
                        "aria-label": "Simulate node update",
                        onclick: move |_| runtime.read().application.host().mock_documents().external_change(
                            sample_route(),
                            Some(serde_json::to_value(SampleConfig {
                                enabled: true,
                                greeting: "Changed on the node".into(),
                            }).unwrap()),
                        ),
                        "Simulate node update"
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dayframe_app::micros::sample::SampleMicro;

    use dayframe_app::host::AccessError;
    #[test]
    fn provider_registration_drives_settings_and_document_access() {
        let runtime = DesktopRuntime::new();
        assert_eq!(runtime.application.provider_descriptors().len(), 2);
        assert_eq!(runtime.application.roots().snapshot().unwrap().len(), 1);
        let sample = runtime.application.output::<SampleMicro>("sample").unwrap();
        assert_eq!(
            runtime
                .application
                .output::<micros::settings::SettingsMicro>("settings")
                .unwrap()
                .sections
                .snapshot()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            sample.decode(&sample.watch().current().unwrap()).unwrap(),
            SampleConfig {
                enabled: true,
                greeting: "Hello from a micro".into(),
            }
        );
        runtime.application.host().close();
    }

    #[test]
    fn disabling_and_reenabling_a_configured_instance_reuses_durable_config() {
        let mut runtime = DesktopRuntime::new();
        let first = runtime.application.output::<SampleMicro>("sample").unwrap();
        let changed = SampleConfig {
            enabled: false,
            greeting: "Edited".into(),
        };
        first.edit(changed.clone()).unwrap();
        runtime.application.host().mock_documents().settle(false);
        assert_eq!(
            first.decode(&first.watch().current().unwrap()),
            Some(SampleConfig {
                enabled: true,
                greeting: "Hello from a micro".into(),
            })
        );
        first.edit(changed.clone()).unwrap();
        runtime.application.host().mock_documents().settle(true);
        runtime.disable_sample();
        assert_eq!(runtime.states["sample"], InstanceState::Disabled);
        assert!(matches!(
            runtime.application.output::<SampleMicro>("sample"),
            Err(AccessError::Revoked)
        ));
        assert_eq!(
            runtime.configuration["sample"].enablement,
            Enablement::Disabled
        );
        assert!(runtime
            .application
            .output::<micros::settings::SettingsMicro>("settings")
            .unwrap()
            .sections
            .snapshot()
            .unwrap()
            .is_empty());
        assert_eq!(first.watch().current().unwrap_err(), AccessError::Revoked);
        runtime.enable_sample();
        let second = runtime.application.output::<SampleMicro>("sample").unwrap();
        assert!(first.instance != second.instance);
        assert_eq!(
            second.decode(&second.watch().current().unwrap()),
            Some(changed)
        );
        runtime.application.host().close();
    }

    #[test]
    fn provider_withdrawal_closes_dependents_before_their_collection() {
        let runtime = DesktopRuntime::new();
        let sample = runtime.application.output::<SampleMicro>("sample").unwrap();
        let mut updates = sample.watch().subscribe().unwrap();
        let mount = runtime
            .application
            .host()
            .mount(
                &runtime
                    .application
                    .output::<micros::settings::SettingsMicro>("settings")
                    .unwrap()
                    .instance,
                sample.detail(),
            )
            .unwrap();
        runtime
            .application
            .output::<micros::settings::SettingsMicro>("settings")
            .unwrap()
            .instance
            .close();
        assert_eq!(sample.instance.check(), Err(AccessError::Revoked));
        assert!(!mount.is_live());
        assert!(runtime.application.roots().snapshot().unwrap().is_empty());
        assert!(updates.borrow_and_update().revoked);
        assert!(updates.borrow().facets.is_empty());
        runtime.application.host().close();
    }
}
