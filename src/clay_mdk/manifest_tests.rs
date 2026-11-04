use super::*;

fn shelf() -> ContractIdentity {
    ContractIdentity::new("dayframe", "shelf", (1, 0, 0))
}

fn canonical_manifest() -> MicroManifest {
    MicroManifest {
        schema: MANIFEST_SCHEMA,
        id: MicroId::new("dayframe.counter"),
        package_version: 1,
        entrypoints: Entrypoints::micro_client(),
        publications: vec![
            PublicationDeclaration {
                name: "view".into(),
                kind: PublicationKind::Presentation,
                contract: None,
            },
            PublicationDeclaration {
                name: "increment".into(),
                kind: PublicationKind::Call,
                contract: Some(ContractIdentity::new(
                    "dayframe.counter",
                    "increment",
                    (1, 0, 0),
                )),
            },
        ],
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
        collection_requests: vec![CollectionRequest {
            binding: "tally-book".into(),
            contract: "tallies".into(),
            access: CollectionAccess::Write,
            necessity: Necessity::Required,
            resolution: BindingResolution::Configured,
        }],
        contributions: vec![nested_contribution()],
        extra: BTreeMap::new(),
    }
}

fn nested_contribution() -> ContributionDeclaration {
    ContributionDeclaration {
        id: "toolbar-button".into(),
        collection: "dayframe.shelf".into(),
        key: "counter".into(),
        payload: serde_json::json!({
            "badges": [1, null, "beta"],
            "enabled": true,
            "label": { "en": "Counter" },
            "order": 2,
            "slot": "toolbar"
        }),
        attachments: vec![AttachmentDeclaration {
            index: 0,
            publication: "view".into(),
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
/// a publication kind and vocabulary strings this build does not know — must deserialize and
/// re-serialize to the same JSON, with the unknown parts held as data rather than dropped.
#[test]
fn unknown_members_round_trip_unchanged() {
    let mut wire = serde_json::to_value(canonical_manifest()).unwrap();
    wire["future_member"] = serde_json::json!({ "nested": [1, "two", null] });
    wire["entrypoints"]["future_entry"] =
        serde_json::json!("townframe:dayframe/future-interface@9.0.0");
    wire["publications"][1]["kind"] = serde_json::json!("stream");
    wire["requires"][0]["necessity"] = serde_json::json!("eventual");
    wire["collection_contracts"][0]["offered"][1] = serde_json::json!("stream");
    wire["collection_requests"][0]["access"] = serde_json::json!("append");
    wire["collection_requests"][0]["resolution"] = serde_json::json!("negotiated");

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
        manifest.publications[1].kind,
        PublicationKind::Other("stream".into())
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
    assert_eq!(
        manifest.collection_requests[0].access,
        CollectionAccess::Other("append".into())
    );
    assert_eq!(
        manifest.collection_requests[0].resolution,
        BindingResolution::Other("negotiated".into())
    );
    assert_eq!(serde_json::to_value(&manifest).unwrap(), wire);
}

// ── Validation ──

#[test]
fn validate_accepts_coherent_manifests() {
    canonical_manifest().validate().unwrap();

    let mut with_unknown_kind = canonical_manifest();
    with_unknown_kind.publications[1].kind = PublicationKind::Other("stream".into());
    with_unknown_kind.validate().unwrap();
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
            |manifest| manifest.publications.push(manifest.publications[0].clone()),
            "duplicate publications name \"view\"",
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
            |manifest| {
                manifest
                    .collection_requests
                    .push(manifest.collection_requests[0].clone())
            },
            "duplicate collection_requests binding \"tally-book\"",
        ),
        (
            |manifest| manifest.contributions.push(nested_contribution()),
            "duplicate contributions id \"toolbar-button\"",
        ),
        (
            |manifest| manifest.publications[1].contract = None,
            "call publication \"increment\" declares no contract",
        ),
        (
            |manifest| manifest.publications[0].contract = Some(shelf()),
            "presentation publication \"view\" declares a contract; presentations have the \
             one presentation-component contract",
        ),
        (
            |manifest| manifest.contributions[0].attachments[0].publication = "missing".into(),
            "contribution \"toolbar-button\" attaches undeclared publication \"missing\"",
        ),
    ];
    for &(mutate, expected) in cases {
        let mut manifest = canonical_manifest();
        mutate(&mut manifest);
        assert_eq!(manifest.validate().unwrap_err(), expected);
    }
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
