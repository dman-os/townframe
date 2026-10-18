//! Desktop composition: explicit provider linkage, configuration source, and lifecycle controls.
use dayframe_app::{
    application::ApplicationRuntime,
    micros::{
        MicroId,
        composition::{
            ConfigurationError, ConfigurationSource, ConfiguredMicro, Enablement, InstanceState,
            MockConfiguration,
        },
    },
    native::{ComponentMount, use_component_recovery},
};
use dioxus::prelude::*;
use std::collections::BTreeMap;

struct DesktopRuntime {
    application: ApplicationRuntime,
    configuration: MockConfiguration,
    states: BTreeMap<String, InstanceState>,
}

impl DesktopRuntime {
    fn new() -> Self {
        let mut application = ApplicationRuntime::new();
        let host = application.host();
        dayframe_sample::bind_mock_defaults(&host);

        application
            .register_provider(dayframe_settings::provider(
                application.native(),
                application.root_mounts(),
            ))
            .unwrap();
        application
            .register_provider(dayframe_sample::provider(application.native()))
            .unwrap();
        application
            .register_provider(dayframe_counter::provider(application.native()))
            .unwrap();

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
                    provider: MicroId::new(dayframe_counter::ID),
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
        self.configuration.current().unwrap()
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
                    .expect("invalid micro configuration");
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
                        onclick: move |_| runtime.read().application.host().mock_documents().settle(true),
                        "Accept pending writes"
                    }
                    button {
                        class: "rounded-md border border-border px-4 py-2 focus-visible:outline",
                        "aria-label": "Reject and roll back",
                        onclick: move |_| runtime.read().application.host().mock_documents().settle(false),
                        "Reject and roll back"
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dayframe_app::host::AccessError;
    use dayframe_counter::CounterMicro;
    use dayframe_sample::{SampleConfig, SampleMicro};
    use dayframe_settings::SettingsMicro;

    fn started() -> DesktopRuntime {
        let mut runtime = DesktopRuntime::new();
        runtime.apply_now().unwrap();
        runtime
    }

    fn set_now(runtime: &mut DesktopRuntime, instance: &str, enablement: Enablement) {
        runtime.set_enablement(instance, enablement).unwrap();
        runtime.apply_now().unwrap();
    }

    #[test]
    fn provider_registration_drives_native_and_wasm_settings_and_locked_boot_entries() {
        let mut runtime = started();
        assert_eq!(runtime.application.provider_descriptors().len(), 3);
        assert_eq!(runtime.application.roots().snapshot().unwrap().len(), 1);
        assert_eq!(
            runtime.set_enablement("settings", Enablement::Disabled),
            Err(ConfigurationError::LockedInstance("settings".into()))
        );
        let sample = runtime.application.output::<SampleMicro>("sample").unwrap();
        let counter = runtime
            .application
            .output::<CounterMicro>("counter")
            .unwrap();
        assert!(counter.instance.check().is_ok());
        let sections = runtime
            .application
            .output::<SettingsMicro>("settings")
            .unwrap()
            .sections
            .snapshot()
            .unwrap();
        assert_eq!(sections.len(), 2);
        let titles: Vec<_> = sections.values().map(|s| s.title.as_str()).collect();
        assert!(titles.contains(&"Sample micro"));
        assert!(titles.contains(&"Counter (Wasm)"));
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
    fn disabling_and_reenabling_configured_instances_reuses_durable_config_and_restarts_wasm() {
        let mut runtime = started();
        let first_sample = runtime.application.output::<SampleMicro>("sample").unwrap();
        let first_counter = runtime
            .application
            .output::<CounterMicro>("counter")
            .unwrap();
        let changed = SampleConfig {
            enabled: false,
            greeting: "Edited".into(),
        };
        first_sample.edit(changed.clone()).unwrap();
        runtime.application.host().mock_documents().settle(false);
        assert_eq!(
            first_sample.decode(&first_sample.watch().current().unwrap()),
            Some(SampleConfig {
                enabled: true,
                greeting: "Hello from a micro".into(),
            })
        );
        first_sample.edit(changed.clone()).unwrap();
        runtime.application.host().mock_documents().settle(true);
        set_now(&mut runtime, "sample", Enablement::Disabled);
        set_now(&mut runtime, "counter", Enablement::Disabled);
        assert_eq!(runtime.states["sample"], InstanceState::Disabled);
        assert_eq!(runtime.states["counter"], InstanceState::Disabled);
        assert!(matches!(
            runtime.application.output::<SampleMicro>("sample"),
            Err(AccessError::Revoked)
        ));
        assert!(matches!(
            runtime.application.output::<CounterMicro>("counter"),
            Err(AccessError::Revoked)
        ));
        assert!(
            runtime
                .application
                .output::<SettingsMicro>("settings")
                .unwrap()
                .sections
                .snapshot()
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            first_sample.watch().current().unwrap_err(),
            AccessError::Revoked
        );
        set_now(&mut runtime, "sample", Enablement::Enabled);
        set_now(&mut runtime, "counter", Enablement::Enabled);
        let second_sample = runtime.application.output::<SampleMicro>("sample").unwrap();
        let second_counter = runtime
            .application
            .output::<CounterMicro>("counter")
            .unwrap();
        assert!(first_sample.instance != second_sample.instance);
        assert!(first_counter.instance != second_counter.instance);
        assert_eq!(
            second_sample.decode(&second_sample.watch().current().unwrap()),
            Some(changed)
        );
        assert_eq!(
            runtime
                .application
                .output::<SettingsMicro>("settings")
                .unwrap()
                .sections
                .snapshot()
                .unwrap()
                .len(),
            2
        );
        runtime.application.host().close();
    }

    #[test]
    fn provider_withdrawal_closes_dependents_before_their_collection() {
        let runtime = started();
        let sample = runtime.application.output::<SampleMicro>("sample").unwrap();
        let counter = runtime
            .application
            .output::<CounterMicro>("counter")
            .unwrap();
        let mut updates = sample.watch().subscribe().unwrap();
        let settings_instance = runtime
            .application
            .output::<SettingsMicro>("settings")
            .unwrap()
            .instance;
        let sample_mount = runtime
            .application
            .host()
            .mount(&settings_instance, sample.detail())
            .unwrap();
        let counter_mount = runtime
            .application
            .host()
            .mount(&settings_instance, counter.detail())
            .unwrap();
        settings_instance.close();
        assert_eq!(sample.instance.check(), Err(AccessError::Revoked));
        assert_eq!(counter.instance.check(), Err(AccessError::Revoked));
        assert!(!sample_mount.is_live());
        assert!(!counter_mount.is_live());
        assert!(runtime.application.roots().snapshot().unwrap().is_empty());
        assert!(updates.borrow_and_update().revoked);
        assert!(updates.borrow().facets.is_empty());
        runtime.application.host().close();
    }
}
