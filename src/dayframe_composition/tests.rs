use super::*;
use micro_settings_manifest::DeclaredSection;
use std::collections::BTreeSet;

fn settings(runtime: &CompositionRuntime) -> micro_settings::SettingsMicro {
    clay_mdk::native::output(&runtime.application, "settings").unwrap()
}

fn settings_route() -> clay_docs::FacetRoute {
    clay_docs::FacetRoute {
        document: "settings-config".into(),
        branch: "main".into(),
        tag: micro_settings_manifest::CONFIG_TAG.into(),
        key_id: micro_settings_manifest::CONFIG_KEY.into(),
    }
}

#[test]
fn document_preferences_control_demand_startup_and_rollback() {
    use clay_docs::OperationError;
    use clay_docs::OperationStatus;
    let mut runtime = stub_runtime();
    let mock = runtime.application.mock_documents();
    mock.seed(
        settings_route(),
        serde_json::json!({"hidden": ["dayframe.sample"], "order": ["dayframe.sample", "dayframe.counter"]}),
    );
    runtime.apply_now().unwrap();
    assert_eq!(runtime.states["dayframe.sample"], InstanceState::Idle);
    let controller = settings(&runtime);
    assert_eq!(controller.declarations.borrow().len(), 2);
    assert!(!runtime.desired()["dayframe.sample"].eager);
    assert!(matches!(
        runtime.set_eager("settings", false),
        Err(ConfigurationError::LockedInstance(_))
    ));

    let startup = runtime.set_eager("dayframe.sample", true).unwrap();
    assert_eq!(startup.current().status, OperationStatus::LocallyApplied);
    runtime.apply_now().unwrap();
    assert_eq!(runtime.states["dayframe.sample"], InstanceState::Active);
    mock.settle(false);
    runtime.apply_now().unwrap();
    assert_eq!(runtime.states["dayframe.sample"], InstanceState::Idle);
    assert!(matches!(
        startup.current().status,
        OperationStatus::RolledBack(OperationError::Rejected { .. })
    ));

    let show = controller
        .edit(|config| {
            config.hidden.remove("dayframe.sample");
        })
        .unwrap();
    runtime.apply_now().unwrap();
    assert_eq!(runtime.states["dayframe.sample"], InstanceState::Active);
    let sample = clay_mdk::native::output::<micro_sample::SampleMicro, _>(
        &runtime.application,
        "dayframe.sample",
    )
    .unwrap();
    mock.settle(true);
    assert_eq!(show.current().status, OperationStatus::Accepted);
    runtime.set_eager("dayframe.sample", true).unwrap();
    runtime.apply_now().unwrap();
    runtime.set_eager("dayframe.sample", false).unwrap();
    runtime.apply_now().unwrap();
    assert_eq!(
        sample.instance,
        clay_mdk::native::output::<micro_sample::SampleMicro, _>(
            &runtime.application,
            "dayframe.sample"
        )
        .unwrap()
        .instance
    );
    mock.settle(true);

    let hide = controller
        .edit(|config| {
            config.hidden.insert("dayframe.sample".into());
        })
        .unwrap();
    runtime.apply_now().unwrap();
    assert_eq!(runtime.states["dayframe.sample"], InstanceState::Idle);
    assert!(runtime.application.host().check(&sample.instance).is_err());
    mock.settle(false);
    runtime.apply_now().unwrap();
    assert_eq!(runtime.states["dayframe.sample"], InstanceState::Active);
    assert!(matches!(
        hide.current().status,
        OperationStatus::RolledBack(_)
    ));
    assert_eq!(settings(&runtime).instance, controller.instance);
    assert_ne!(
        sample.instance,
        clay_mdk::native::output::<micro_sample::SampleMicro, _>(
            &runtime.application,
            "dayframe.sample"
        )
        .unwrap()
        .instance
    );
    runtime.application.host().close();
}

#[test]
fn settings_loading_and_external_preferences_are_observed_without_restart() {
    let mut runtime = stub_runtime();
    let mock = runtime.application.mock_documents();
    mock.hold_refreshes(true);
    runtime.apply_now().unwrap();
    let controller = settings(&runtime);
    assert!(
        controller
            .preferences(&controller.config.watch.current().unwrap())
            .is_none()
    );
    assert!(matches!(
        controller.edit(|_| {}),
        Err(clay_host::AccessError::Denied)
    ));
    assert_eq!(runtime.states["dayframe.sample"], InstanceState::Idle);
    mock.complete_refreshes();
    runtime.apply_now().unwrap();
    assert_eq!(runtime.states["dayframe.sample"], InstanceState::Active);
    assert_eq!(
        controller.preferences(&controller.config.watch.current().unwrap()),
        Some(micro_settings::SettingsConfig::default())
    );
    mock.external_change(
        settings_route(),
        Some(serde_json::json!({"hidden": ["dayframe.sample"], "order": ["dayframe.sample", "dayframe.counter"]})),
    );
    runtime.apply_now().unwrap();
    assert_eq!(runtime.states["dayframe.sample"], InstanceState::Idle);
    let preferences = controller
        .preferences(&controller.config.watch.current().unwrap())
        .unwrap();
    assert!(preferences.rank("dayframe.sample") < preferences.rank("dayframe.counter"));
    assert_eq!(preferences.rank("unknown"), usize::MAX);
    assert_eq!(settings(&runtime).instance, controller.instance);
    mock.external_change(settings_route(), None);
    runtime.apply_now().unwrap();
    assert_eq!(runtime.states["dayframe.sample"], InstanceState::Active);
    runtime.application.host().close();
    assert!(matches!(
        controller.edit(|_| {}),
        Err(clay_host::AccessError::Revoked)
    ));
}

fn locked_settings() -> BTreeMap<String, ConfiguredMicro> {
    BTreeMap::from([(
        "settings".into(),
        ConfiguredMicro {
            provider: MicroId::new(micro_settings_manifest::ID),
            enablement: Enablement::Enabled,
            eager: true,
            bindings: BTreeMap::new(),
        },
    )])
}

fn runtime_with_counter(
    counter: impl Fn(&clay_mdk::native::Context<NativeComponents>) -> Result<(), clay_host::AccessError>
    + 'static,
) -> CompositionRuntime {
    let installed = vec![
        InstalledMicro::new(micro_settings_manifest::manifest(), |application| {
            clay_mdk::native::provider(
                application,
                micro_settings_manifest::manifest(),
                micro_settings::activate,
            )
        }),
        InstalledMicro::new(micro_sample::manifest(), |application| {
            clay_mdk::native::provider(
                application,
                micro_sample::manifest(),
                micro_sample::activate,
            )
        }),
        InstalledMicro::new(micro_counter_manifest::manifest(), move |application| {
            clay_mdk::native::provider(application, micro_counter_manifest::manifest(), counter)
        }),
    ];
    let runtime = CompositionRuntime::new(installed, locked_settings());
    micro_sample::seed_mock_defaults(&runtime.application.documents());
    runtime.application.mock_documents().seed(
        clay_docs::FacetRoute {
            document: "counter-config".into(),
            branch: "main".into(),
            tag: micro_counter_manifest::CONFIG_TAG.into(),
            key_id: micro_counter_manifest::CONFIG_KEY.into(),
        },
        serde_json::json!(0),
    );
    runtime
}

fn stub_runtime() -> CompositionRuntime {
    runtime_with_counter(|_| Ok(()))
}

// ── linkage coherence: the provider table, the configuration map, and the
//    manifest declarations are three string-keyed surfaces that must agree;
//    drift between them only surfaces as a reconcile error otherwise ──────

#[test]
fn linked_provider_manifests_configuration_and_contributions_are_coherent() {
    let runtime = stub_runtime();
    let manifests = [
        micro_settings_manifest::manifest(),
        micro_sample::manifest(),
        micro_counter_manifest::manifest(),
    ];
    // A swapped or dropped entry in the guest providers shows up here as an
    // id-set mismatch between what the table registered and what this composition declares.
    let descriptors = runtime.application.provider_descriptors();
    let registered: BTreeSet<&str> = descriptors
        .iter()
        .map(|descriptor| descriptor.id.as_str())
        .collect();
    let declared: BTreeSet<&str> = manifests
        .iter()
        .map(|manifest| manifest.id.as_str())
        .collect();
    assert_eq!(registered, declared);

    let desired = runtime.desired();
    // Every configured instance names a linked provider, binds exactly that provider's
    // declared requirements, and routes each binding to an instance whose provider
    // actually provides the contract.
    for (name, configured) in &desired {
        let manifest = manifests
            .iter()
            .find(|manifest| manifest.id.as_str() == configured.provider.as_str())
            .unwrap_or_else(|| {
                panic!(
                    "configured instance {name} names unlinked provider {:?}",
                    configured.provider
                )
            });
        let requirements: BTreeSet<&str> = manifest
            .requires
            .iter()
            .map(|requirement| requirement.contract.id.as_str())
            .collect();
        let bound: BTreeSet<&str> = configured.bindings.keys().map(String::as_str).collect();
        assert_eq!(
            bound, requirements,
            "instance {name} bindings must cover exactly its provider's declared requirements"
        );
        for (contract, target) in &configured.bindings {
            let target_provider = desired.get(target).unwrap_or_else(|| {
                panic!("instance {name} binds {contract} to unknown instance {target}")
            });
            let target_manifest = manifests
                .iter()
                .find(|manifest| manifest.id.as_str() == target_provider.provider.as_str())
                .expect("target provider checked linked above");
            assert!(
                target_manifest
                    .provides
                    .iter()
                    .any(|provided| provided.contract.id == *contract),
                "instance {target} does not provide the {contract} contract that {name} binds to it"
            );
        }
    }

    // A declared contribution may only target one of the contributing manifest's own
    // requirement bindings, through the contract that binding requires, and its payload must
    // satisfy the target contract's acceptor — for `settings.sections` that acceptor is
    // `DeclaredSection`. Current behavior pins a shape violation as a denied contribution at
    // activation, not a pre-activation error.
    for manifest in &manifests {
        for contribution in &manifest.contributions {
            assert!(
                manifest.requires.iter().any(|requirement| {
                    requirement.binding == contribution.target_binding
                        && requirement.contract == contribution.extension_contract
                }),
                "manifest {:?} contribution {:?} targets undeclared requirement binding {:?}",
                manifest.id,
                contribution.id,
                contribution.target_binding
            );
            if contribution.extension_contract.id == "settings.sections" {
                serde_json::from_value::<DeclaredSection>(contribution.payload.clone())
                    .unwrap_or_else(|error| {
                        panic!(
                            "manifest {:?} settings.sections payload is not a DeclaredSection: {error}",
                            manifest.id
                        )
                    });
            }
        }
    }
    runtime.application.host().close();
}

#[test]
fn locked_settings_instance_cannot_flip_enablement() {
    let mut runtime = stub_runtime();
    assert!(matches!(
        runtime.set_enablement("settings", Enablement::Disabled),
        Err(ConfigurationError::LockedInstance(name)) if name == "settings"
    ));
    runtime.application.host().close();
}

// ── failure is a state the shell shows, not a reason to abort (ADR 014; F3) ──

/// A package factory that cannot produce its guest (fetch, HTTP status, compile, missing
/// artifact) is the common web failure. The reconciler retains it as `Failed` on that one
/// instance; every other micro keeps running; a configuration revision is the retry.
#[test]
fn unfetchable_guest_fails_its_own_instance_and_leaves_the_rest_running() {
    let mut runtime = runtime_with_counter(|_| Err(clay_host::AccessError::Denied));
    runtime.apply_now().unwrap();
    assert_eq!(runtime.states["settings"], InstanceState::Active);
    assert_eq!(runtime.states["dayframe.sample"], InstanceState::Active);
    assert!(
        matches!(runtime.states["dayframe.counter"], InstanceState::Failed(_)),
        "{:?}",
        runtime.states["dayframe.counter"]
    );
    // Retained, not retried, by an unrelated reconcile.
    runtime.apply_now().unwrap();
    assert!(matches!(
        runtime.states["dayframe.counter"],
        InstanceState::Failed(_)
    ));
    // Disable then re-enable is the retry; the provider still fails, so it fails again rather
    // than silently going idle.
    runtime
        .set_enablement("dayframe.counter", Enablement::Disabled)
        .unwrap();
    runtime.apply_now().unwrap();
    assert_eq!(runtime.states["dayframe.counter"], InstanceState::Disabled);
    runtime
        .set_enablement("dayframe.counter", Enablement::Enabled)
        .unwrap();
    runtime.apply_now().unwrap();
    assert!(matches!(
        runtime.states["dayframe.counter"],
        InstanceState::Failed(_)
    ));
    runtime.application.host().close();
}
