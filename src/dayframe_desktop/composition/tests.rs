use super::*;
use clay_host::AccessError;
use clay_mdk::native::output;
use clay_mdk::wasm::WasmMicro;
use clay_micro_core::micros::composition::ConfigurationError;
use clay_micro_core::micros::composition::Enablement;
use clay_micro_core::micros::composition::InstanceState;
use micro_sample::SampleConfig;
use micro_sample::SampleMicro;
use micro_settings::SettingsMicro;
use std::collections::BTreeSet;

// ── desktop guest linkage: the shell's guest_providers must register the Wasmtime
//    counter beside the shared composition's native providers, or the id sets drift ──

#[test]
fn desktop_runtime_links_wasmtime_counter_with_shared_native_providers() {
    let runtime = desktop_runtime();
    let manifests = [
        micro_settings_manifest::manifest(),
        micro_sample::manifest(),
        micro_counter_manifest::manifest(),
    ];
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
    assert!(runtime
        .application
        .provider_descriptors()
        .iter()
        .any(|descriptor| descriptor.id.as_str() == micro_counter_manifest::ID));
    runtime.application.host().close();
}

fn started() -> CompositionRuntime {
    let mut runtime = desktop_runtime();
    runtime.apply_now().unwrap();
    runtime
}

fn set_now(runtime: &mut CompositionRuntime, instance: &str, enablement: Enablement) {
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
