use super::*;

fn shelf() -> ContractRef {
    ContractRef {
        id: "dayframe.shelf".into(),
        version: 1,
    }
}

fn canonical_manifest() -> MicroManifest {
    MicroManifest {
        schema: MANIFEST_SCHEMA,
        id: MicroId::new("dayframe.counter"),
        package_version: 1,
        entrypoints: Entrypoints::micro_client(),
        provides: vec![ProvidedContract {
            id: "controller".into(),
            contract: ContractRef {
                id: "wasm.controller".into(),
                version: 1,
            },
            retention: Retention::Strong,
        }],
        requires: vec![RequiredContract {
            binding: "shelf".into(),
            contract: shelf(),
            necessity: Necessity::Optional,
        }],
        configuration_requests: vec![ConfigurationRequest {
            binding: "counter".into(),
            facets: vec![FacetAccess {
                owner_plug_id: None,
                tag: "org.dayframe.counter.config".into(),
                key_id: Some("count".into()),
                read: true,
                write: false,
                create: false,
                delete: false,
            }],
        }],
        collection_contracts: vec![CollectionContract {
            id: "tallies".into(),
            selector_schema: "dayframe.counter.tally-selector".into(),
            key_schema: "dayframe.counter.tally-key".into(),
            value_schema: "dayframe.counter.tally".into(),
            write_context_schema: None,
            offered: vec![CollectionCapability::Read, CollectionCapability::Register],
        }],
        contributions: vec![nested_contribution()],
        activation_defaults: vec![
            ActivationRegistrationTemplate {
                id: "toolbar-button".into(),
                trigger: Trigger::TargetLive {
                    target_binding: "shelf".into(),
                    extension_contract: shelf(),
                },
                declaration_id: "toolbar-button".into(),
            },
            ActivationRegistrationTemplate {
                id: "always-on".into(),
                trigger: Trigger::ExplicitRoot,
                declaration_id: "toolbar-button".into(),
            },
        ],
        extra: BTreeMap::new(),
    }
}

fn nested_contribution() -> ContributionDeclaration {
    ContributionDeclaration {
        id: "toolbar-button".into(),
        target_binding: "shelf".into(),
        extension_contract: shelf(),
        payload: serde_json::json!({
            "badges": [1, null, "beta"],
            "enabled": true,
            "label": { "en": "Counter" },
            "order": 2,
            "slot": "toolbar"
        }),
        capability_slots: vec![CapabilitySlotDeclaration {
            index: 0,
            contract: ContractRef {
                id: "dayframe.surface-mount".into(),
                version: 1,
            },
        }],
    }
}

// ── Roundtrips ──

#[test]
fn manifest_roundtrips_through_json_unchanged() {
    let manifest = canonical_manifest();
    let json = serde_json::to_string(&manifest).unwrap();
    assert_eq!(
        serde_json::from_str::<MicroManifest>(&json).unwrap(),
        manifest
    );
}

/// A manifest written by newer tooling — an extra top-level member, an extra entrypoint,
/// a trigger kind and vocabulary strings this build does not know — must deserialize and
/// re-serialize to the same JSON, with the unknown parts held as data rather than dropped.
#[test]
fn unknown_members_round_trip_unchanged() {
    let mut wire = serde_json::to_value(canonical_manifest()).unwrap();
    wire["future_member"] = serde_json::json!({ "nested": [1, "two", null] });
    wire["entrypoints"]["future_entry"] =
        serde_json::json!("townframe:dayframe/future-interface@9.0.0");
    wire["activation_defaults"][0]["trigger"] = serde_json::json!({
        "kind": "command-invoked",
        "command": "counter.open",
        "arguments": { "focus": true }
    });
    wire["provides"][0]["retention"] = serde_json::json!("pinned");
    wire["requires"][0]["necessity"] = serde_json::json!("eventual");
    wire["collection_contracts"][0]["offered"][1] = serde_json::json!("stream");

    let manifest: MicroManifest = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(
        manifest.extra["future_member"],
        serde_json::json!({ "nested": [1, "two", null] })
    );
    assert_eq!(
        manifest.entrypoints.extra["future_entry"],
        serde_json::json!("townframe:dayframe/future-interface@9.0.0")
    );
    assert_eq!(
        manifest.activation_defaults[0].trigger,
        Trigger::Unknown {
            kind: "command-invoked".into(),
            fields: BTreeMap::from([
                ("arguments".into(), serde_json::json!({ "focus": true })),
                ("command".into(), serde_json::json!("counter.open")),
            ]),
        }
    );
    assert_eq!(
        manifest.provides[0].retention,
        Retention::Other("pinned".into())
    );
    assert_eq!(
        manifest.requires[0].necessity,
        Necessity::Other("eventual".into())
    );
    assert_eq!(
        manifest.collection_contracts[0].offered,
        vec![
            CollectionCapability::Read,
            CollectionCapability::Other("stream".into())
        ]
    );
    assert_eq!(serde_json::to_value(&manifest).unwrap(), wire);
}

/// A known trigger kind refined by a member this build does not know (a newer manifest's
/// `when` condition, say) must not activate as the bare kind with the refinement silently
/// dropped: it is carried as `Unknown` and round-trips unchanged. A known kind missing
/// one of its own members is malformed, not unknown.
#[test]
fn known_trigger_kind_with_unknown_members_is_carried_as_unknown() {
    let refined_target_live = serde_json::json!({
        "kind": "target-live",
        "target_binding": "shelf",
        "extension_contract": { "id": "dayframe.shelf", "version": 1 },
        "when": "focused"
    });
    let refined_explicit_root = serde_json::json!({
        "kind": "explicit-root",
        "priority": 3
    });
    for (wire, kind) in [
        (refined_target_live, "target-live"),
        (refined_explicit_root, "explicit-root"),
    ] {
        let trigger: Trigger = serde_json::from_value(wire.clone()).unwrap();
        let Trigger::Unknown {
            kind: carried_kind, ..
        } = &trigger
        else {
            panic!("{kind} with an unknown member parsed as a known trigger: {trigger:?}");
        };
        assert_eq!(carried_kind, kind);
        assert_eq!(serde_json::to_value(&trigger).unwrap(), wire);
    }

    let missing_member = serde_json::json!({
        "kind": "target-live",
        "target_binding": "shelf"
    });
    assert!(serde_json::from_value::<Trigger>(missing_member).is_err());
}

// ── Validation ──

#[test]
fn validate_accepts_coherent_manifests() {
    canonical_manifest().validate().unwrap();

    let mut with_unknown_trigger = canonical_manifest();
    with_unknown_trigger.activation_defaults[0].trigger = Trigger::Unknown {
        kind: "command-invoked".into(),
        fields: BTreeMap::from([("command".into(), serde_json::json!("counter.open"))]),
    };
    with_unknown_trigger.validate().unwrap();
}

#[test]
fn validate_rejects_incoherent_manifests() {
    type Mutation = fn(&mut MicroManifest);
    let cases: &[(Mutation, &str)] = &[
        (
            |manifest| manifest.schema = MANIFEST_SCHEMA + 1,
            "manifest declares schema 2, this build reads schema 1",
        ),
        (
            |manifest| manifest.provides.push(manifest.provides[0].clone()),
            "duplicate provides id \"controller\"",
        ),
        (
            |manifest| manifest.requires.push(manifest.requires[0].clone()),
            "duplicate requires binding \"shelf\"",
        ),
        (
            |manifest| {
                manifest
                    .configuration_requests
                    .push(manifest.configuration_requests[0].clone())
            },
            "duplicate configuration_requests binding \"counter\"",
        ),
        (
            |manifest| {
                manifest
                    .collection_contracts
                    .push(manifest.collection_contracts[0].clone())
            },
            "duplicate collection_contracts id \"tallies\"",
        ),
        (
            |manifest| manifest.contributions.push(nested_contribution()),
            "duplicate contributions id \"toolbar-button\"",
        ),
        (
            |manifest| {
                manifest
                    .activation_defaults
                    .push(manifest.activation_defaults[1].clone())
            },
            "duplicate activation_defaults id \"always-on\"",
        ),
        (
            |manifest| manifest.contributions[0].target_binding = "missing".into(),
            "contribution \"toolbar-button\" targets undeclared requirement binding \
             \"missing\"",
        ),
        (
            |manifest| manifest.activation_defaults[1].declaration_id = "missing".into(),
            "activation default \"always-on\" names undeclared contribution \"missing\"",
        ),
        (
            |manifest| {
                manifest.requires.push(RequiredContract {
                    binding: "other".into(),
                    contract: shelf(),
                    necessity: Necessity::Optional,
                });
                manifest.activation_defaults[0].trigger = Trigger::TargetLive {
                    target_binding: "other".into(),
                    extension_contract: shelf(),
                };
            },
            "activation default \"toolbar-button\" trigger does not match the target \
             binding and extension contract of contribution \"toolbar-button\"",
        ),
        (
            |manifest| {
                manifest.activation_defaults[0].trigger = Trigger::TargetLive {
                    target_binding: "shelf".into(),
                    extension_contract: ContractRef {
                        id: "dayframe.shelf".into(),
                        version: 2,
                    },
                };
            },
            "activation default \"toolbar-button\" trigger does not match the target \
             binding and extension contract of contribution \"toolbar-button\"",
        ),
    ];
    for &(mutate, expected) in cases {
        let mut manifest = canonical_manifest();
        mutate(&mut manifest);
        assert_eq!(manifest.validate().unwrap_err(), expected);
    }
}

#[test]
fn contract_ref_of_uses_the_contracts_compile_time_identity() {
    #[derive(Clone)]
    struct Contract;
    impl Provision for Contract {
        const ID: &'static str = "test.contract";
    }
    assert_eq!(
        ContractRef::of::<Contract>(),
        ContractRef {
            id: "test.contract".into(),
            version: 1,
        }
    );
}

// ── Wire-shape goldens ──
//
// Manifests cross process boundaries: these snapshots pin the canonical JSON shape so a
// serde attribute or field change cannot silently break a linked guest's manifest.

#[test]
fn micro_id_is_a_bare_json_string() {
    let id = MicroId::new("dayframe.counter");
    assert_eq!(serde_json::to_string(&id).unwrap(), "\"dayframe.counter\"");
    insta::assert_snapshot!(serde_json::to_string_pretty(&id).unwrap());
}

#[test]
fn manifest_canonical_wire_shape() {
    insta::assert_snapshot!(serde_json::to_string_pretty(&canonical_manifest()).unwrap());
}

#[test]
fn contribution_nested_payload_wire_shape() {
    insta::assert_snapshot!(serde_json::to_string_pretty(&nested_contribution()).unwrap());
}

#[test]
fn facet_access_acl_wire_shape() {
    let acl = FacetAccess {
        owner_plug_id: Some("dayframe.organizer".into()),
        tag: "org.dayframe.counter.config".into(),
        key_id: Some("count".into()),
        read: true,
        write: true,
        create: false,
        delete: false,
    };
    insta::assert_snapshot!(serde_json::to_string_pretty(&acl).unwrap());
}
