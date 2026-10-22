use super::*;
use clay_host::AccessError;
use clay_mdk::native::output;
use clay_mdk::wasm::WasmMicro;
use micro_sample::SampleConfig;
use micro_sample::SampleMicro;
use micro_settings::SettingsMicro;
use micro_settings_manifest::DeclaredSection;
use std::collections::BTreeSet;

// ── linkage coherence: the provider table, the configuration map, and the
//    manifest declarations are three string-keyed surfaces that must agree;
//    drift between them only surfaces as a reconcile error otherwise ──────

#[test]
fn linked_provider_manifests_configuration_and_contributions_are_coherent() {
    let runtime = DesktopRuntime::new();
    let manifests = [
        micro_settings_manifest::manifest(),
        micro_sample::manifest(),
        micro_counter_manifest::manifest(),
    ];
    // A swapped or dropped entry in `linked_providers` shows up here as an
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
            .requirements
            .iter()
            .map(|requirement| requirement.id.as_str())
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
                    .provisions
                    .iter()
                    .any(|provision| provision.id == *contract),
                "instance {target} does not provide the {contract} contract that {name} binds to it"
            );
        }
    }

    // A declared contribution may only target one of the contributing manifest's own
    // requirements, and its payload must satisfy the target contract's acceptor — for
    // `settings.sections` that acceptor is `DeclaredSection`. Current behavior pins a
    // shape violation as a denied contribution at activation, not a pre-activation error.
    for manifest in &manifests {
        for contribution in &manifest.contributions {
            assert!(
                manifest
                    .requirements
                    .iter()
                    .any(|requirement| requirement.id == contribution.provision),
                "manifest {:?} contributes to undeclared requirement {:?}",
                manifest.id,
                contribution.provision
            );
            if contribution.provision == "settings.sections" {
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

fn started() -> DesktopRuntime {
    let mut runtime = DesktopRuntime::new();
    runtime.apply_now().unwrap();
    runtime
}

fn set_now(runtime: &mut DesktopRuntime, instance: &str, enablement: Enablement) {
    runtime.set_enablement(instance, enablement).unwrap();
    runtime.apply_now().unwrap();
}

fn block_on<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}

// ── native edit receipts: an edit is optimistic (LocallyApplied, visible in the facet)
//    until the mock node settles it Accepted or RolledBack ──────────────────────────────

#[test]
fn sample_edit_receipt_reports_locally_applied_then_accepted_or_rolled_back() {
    let runtime = started();
    let sample = output::<SampleMicro, _>(&runtime.application, "sample").unwrap();

    let accepted = SampleConfig {
        enabled: false,
        greeting: "Accepted edit".into(),
    };
    let operation = sample.edit(accepted.clone()).unwrap();
    let initial = operation.current();
    assert_eq!(initial.status, clay_docs::OperationStatus::LocallyApplied);
    assert_eq!(
        sample.decode(&sample.watch().current().unwrap()),
        Some(accepted.clone()),
        "optimistic edit is visible before settlement"
    );
    runtime.application.mock_documents().settle(true);
    let receipt = block_on(operation.next(initial.revision))
        .unwrap()
        .expect("settled edit completes");
    assert_eq!(receipt.status, clay_docs::OperationStatus::Accepted);
    assert_eq!(
        sample.decode(&sample.watch().current().unwrap()),
        Some(accepted.clone())
    );

    let rejected = SampleConfig {
        enabled: true,
        greeting: "Rejected edit".into(),
    };
    let operation = sample.edit(rejected).unwrap();
    let initial = operation.current();
    assert_eq!(initial.status, clay_docs::OperationStatus::LocallyApplied);
    runtime.application.mock_documents().settle(false);
    let receipt = block_on(operation.next(initial.revision))
        .unwrap()
        .expect("rejected edit completes");
    assert!(matches!(
        receipt.status,
        clay_docs::OperationStatus::RolledBack(_)
    ));
    assert_eq!(
        sample.decode(&sample.watch().current().unwrap()),
        Some(accepted),
        "rolled-back edit restores the last accepted value"
    );
    runtime.application.host().close();
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
    let sample = output::<SampleMicro, _>(&runtime.application, "sample").unwrap();
    let counter = runtime.application.output::<WasmMicro>("counter").unwrap();
    assert!(runtime.application.host().check(&counter.instance).is_ok());
    let sections = output::<SettingsMicro, _>(&runtime.application, "settings")
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
    let first_sample = output::<SampleMicro, _>(&runtime.application, "sample").unwrap();
    let first_counter = runtime.application.output::<WasmMicro>("counter").unwrap();
    let changed = SampleConfig {
        enabled: false,
        greeting: "Edited".into(),
    };
    first_sample.edit(changed.clone()).unwrap();
    runtime.application.mock_documents().settle(false);
    assert_eq!(
        first_sample.decode(&first_sample.watch().current().unwrap()),
        Some(SampleConfig {
            enabled: true,
            greeting: "Hello from a micro".into(),
        })
    );
    first_sample.edit(changed.clone()).unwrap();
    runtime.application.mock_documents().settle(true);
    set_now(&mut runtime, "sample", Enablement::Disabled);
    set_now(&mut runtime, "counter", Enablement::Disabled);
    assert_eq!(runtime.states["sample"], InstanceState::Disabled);
    assert_eq!(runtime.states["counter"], InstanceState::Disabled);
    assert!(matches!(
        output::<SampleMicro, _>(&runtime.application, "sample"),
        Err(AccessError::Revoked)
    ));
    assert!(matches!(
        runtime.application.output::<WasmMicro>("counter"),
        Err(AccessError::Revoked)
    ));
    assert!(output::<SettingsMicro, _>(&runtime.application, "settings")
        .unwrap()
        .sections
        .snapshot()
        .unwrap()
        .is_empty());
    assert_eq!(
        first_sample.watch().current().unwrap_err(),
        AccessError::Revoked
    );
    set_now(&mut runtime, "sample", Enablement::Enabled);
    set_now(&mut runtime, "counter", Enablement::Enabled);
    let second_sample = output::<SampleMicro, _>(&runtime.application, "sample").unwrap();
    let second_counter = runtime.application.output::<WasmMicro>("counter").unwrap();
    assert!(first_sample.instance != second_sample.instance);
    assert!(first_counter.instance != second_counter.instance);
    assert_eq!(
        second_sample.decode(&second_sample.watch().current().unwrap()),
        Some(changed)
    );
    assert_eq!(
        output::<SettingsMicro, _>(&runtime.application, "settings")
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
    let sample = output::<SampleMicro, _>(&runtime.application, "sample").unwrap();
    let counter = runtime.application.output::<WasmMicro>("counter").unwrap();
    let mut updates = sample.watch().subscribe().unwrap();
    let settings_instance = output::<SettingsMicro, _>(&runtime.application, "settings")
        .unwrap()
        .instance;
    let sample_mount = runtime
        .application
        .host()
        .mount(&settings_instance, sample.detail().host())
        .unwrap();
    let counter_mount = runtime
        .application
        .host()
        .mount(&settings_instance, counter.surface().host())
        .unwrap();
    runtime
        .application
        .host()
        .close_instance(&settings_instance)
        .unwrap();
    assert_eq!(
        runtime.application.host().check(&sample.instance),
        Err(AccessError::Revoked)
    );
    assert_eq!(
        runtime.application.host().check(&counter.instance),
        Err(AccessError::Revoked)
    );
    assert!(!sample_mount.is_live());
    assert!(!counter_mount.is_live());
    assert!(runtime.application.roots().snapshot().unwrap().is_empty());
    assert!(updates.borrow_and_update().revoked);
    assert!(updates.borrow().facets.is_empty());
    runtime.application.host().close();
}
