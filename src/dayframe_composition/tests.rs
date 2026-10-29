use super::*;
use micro_settings_manifest::DeclaredSection;
use std::collections::BTreeSet;

/// Composition with a registration-only counter stub: enough for coherence (id sets,
/// bindings, contribution shapes) without an engine. Never activated.
fn stub_runtime() -> CompositionRuntime {
    CompositionRuntime::new(|application| {
        vec![clay_mdk::native::provider(
            application,
            micro_counter_manifest::manifest(),
            |_| Ok(()),
        )]
    })
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

#[test]
fn locked_settings_instance_cannot_flip_enablement() {
    let mut runtime = stub_runtime();
    assert_eq!(
        runtime.set_enablement("settings", Enablement::Disabled),
        Err(ConfigurationError::LockedInstance("settings".into()))
    );
    runtime.application.host().close();
}
