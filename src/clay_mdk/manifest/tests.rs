use super::*;

fn nsid(name: &str) -> Nsid {
    Nsid::literal(&format!("org.example.test.counter.{name}"))
}

fn shelf() -> Nsid {
    Nsid::literal("org.example.test.shelf")
}

/// The canonical package: authority `org.example.test.counter` holds everything it declares.
fn canonical_sidecar() -> Sidecar {
    Sidecar::new("dayframe.counter", 1, canonical_manifest())
        .with_authority("org.example.test.counter")
}

fn canonical_manifest() -> MicroManifest {
    MicroManifest {
        entrypoints: Entrypoints::micro_client(),
        endpoints: BTreeMap::from([
            ("view".into(), Endpoint::View { implements: None }),
            (
                "increment".into(),
                Endpoint::Callback {
                    implements: nsid("increment"),
                },
            ),
            (
                "count".into(),
                Endpoint::Method {
                    nsid: nsid("count"),
                },
            ),
        ]),
        requires: vec![Request {
            binding: "shelf".into(),
            contract: shelf(),
            access: None,
            necessity: Necessity::Optional,
            resolution: BindingResolution::Configured,
            min_revision: Some(2),
        }],
        configuration_requests: vec![ConfigurationRequest {
            binding: "counter".into(),
            facets: vec![FacetAccess {
                tag: nsid("config"),
                key_id: Some("count".into()),
                read: true,
                write: false,
                create: false,
                delete: false,
            }],
        }],
        collection_contracts: vec![CollectionContract {
            id: nsid("tallies"),
            selector_schema: nsid("tallySelector"),
            key_schema: nsid("tallyKey"),
            value_schema: nsid("tally"),
            write_context_schema: None,
            offered: vec![CollectionCapability::Read, CollectionCapability::Register],
        }],
        collection_requests: vec![Request {
            binding: "tally-book".into(),
            contract: nsid("tallies"),
            access: Some(Access::Write),
            necessity: Necessity::Required,
            resolution: BindingResolution::Configured,
            min_revision: None,
        }],
        contributions: vec![nested_contribution()],
        extra: BTreeMap::new(),
    }
}

fn nested_contribution() -> ContributionDeclaration {
    ContributionDeclaration {
        id: "toolbar-button".into(),
        binding: "tally-book".into(),
        key: "counter".into(),
        value: serde_json::json!({
            "badges": [1, null, "beta"],
            "enabled": true,
            "label": { "en": "Counter" },
            "order": 2,
            "slot": "toolbar"
        }),
        links: vec![LinkDeclaration {
            index: 0,
            endpoint: "view".into(),
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
/// an endpoint member and vocabulary strings this build does not know — must deserialize and
/// re-serialize to the same JSON, with the unknown parts held as data rather than dropped.
#[test]
fn unknown_members_round_trip_unchanged() {
    let mut wire = serde_json::to_value(canonical_manifest()).unwrap();
    wire["future_member"] = serde_json::json!({ "nested": [1, "two", null] });
    wire["entrypoints"]["future_entry"] =
        serde_json::json!("townframe:dayframe/future-interface@9.0.0");
    wire["endpoints"]["stream"] = serde_json::json!({ "$type": "org.example.clay.micro.manifest#streamEndpoint", "cursor": ["a", 1] });
    wire["requires"][0]["necessity"] = serde_json::json!("eventual");
    wire["requires"][0]["resolution"] = serde_json::json!("atRuntime");
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
        manifest.endpoints["stream"],
        Endpoint::Other(
            serde_json::json!({ "$type": "org.example.clay.micro.manifest#streamEndpoint", "cursor": ["a", 1] })
                .as_object()
                .unwrap()
                .clone()
        )
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
        Some(Access::Other("append".into()))
    );
    assert_eq!(
        manifest.collection_requests[0].resolution,
        BindingResolution::Other("negotiated".into())
    );
    assert_eq!(
        manifest.requires[0].resolution,
        BindingResolution::AtRuntime
    );
    assert_eq!(serde_json::to_value(&manifest).unwrap(), wire);
}

/// Only an unknown `$type` is carried as `Other`; a known one whose members do not fit is a
/// malformed manifest, never silently demoted to an endpoint no host serves.
#[test]
fn a_known_endpoint_type_with_malformed_members_is_an_error() {
    let endpoint = |members: serde_json::Value| serde_json::from_value::<Endpoint>(members);
    let method = "org.example.clay.micro.manifest#methodEndpoint";
    let callback = "org.example.clay.micro.manifest#callbackEndpoint";
    let view = "org.example.clay.micro.manifest#viewEndpoint";
    for malformed in [
        // A method without its `nsid`, or with one that is not an NSID.
        serde_json::json!({ "$type": method }),
        serde_json::json!({ "$type": method, "nsid": 7 }),
        serde_json::json!({ "$type": method, "nsid": "not an nsid" }),
        // An anonymous callback.
        serde_json::json!({ "$type": callback }),
        serde_json::json!({ "$type": callback, "implements": null }),
        // A view whose `implements` is not an NSID.
        serde_json::json!({ "$type": view, "implements": ["org.example.nav"] }),
        // No `$type` at all.
        serde_json::json!({ "nsid": "org.example.clay.test.count" }),
    ] {
        endpoint(malformed.clone())
            .expect_err(&format!("{malformed} is malformed, not an unknown member"));
    }
    assert_eq!(
        endpoint(serde_json::json!({ "$type": view })).unwrap(),
        Endpoint::View { implements: None }
    );
    // The short `#methodEndpoint` is not a known `$type`: ids are spelled in full.
    assert!(matches!(
        endpoint(serde_json::json!({ "$type": "#methodEndpoint", "nsid": 7 })).unwrap(),
        Endpoint::Other(_)
    ));
}

// ── Validation ──

#[test]
fn validate_accepts_coherent_manifests() {
    canonical_manifest().validate().unwrap();

    let mut with_unknown_member = canonical_manifest();
    with_unknown_member
        .endpoints
        .insert("stream".into(), Endpoint::Other(Default::default()));
    with_unknown_member.validate().unwrap();
}

#[test]
fn validate_rejects_incoherent_manifests() {
    type Mutation = fn(&mut MicroManifest);
    let cases: &[(Mutation, &str)] = &[
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
            "duplicate collection_contracts id \"org.example.test.counter.tallies\"",
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
            |manifest| manifest.contributions[0].links[0].endpoint = "missing".into(),
            "contribution \"toolbar-button\" links undeclared endpoint \"missing\"",
        ),
        (
            |manifest| manifest.requires[0].access = Some(Access::Read),
            "requirement \"shelf\" declares an access; a requirement's grant is \"may call\" \
             or \"may mount\"",
        ),
        (
            |manifest| manifest.collection_requests[0].access = None,
            "collection request \"tally-book\" declares no access",
        ),
        (
            |manifest| manifest.contributions[0].binding = "nowhere".into(),
            "contribution \"toolbar-button\" targets undeclared collection binding \"nowhere\"",
        ),
        (
            |manifest| manifest.collection_requests[0].access = Some(Access::Read),
            "contribution \"toolbar-button\" targets binding \"tally-book\", which does not \
             request write access",
        ),
        (
            |manifest| {
                manifest.collection_requests[0].access = Some(Access::Other("append".into()))
            },
            "contribution \"toolbar-button\" targets binding \"tally-book\", which does not \
             request write access",
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
    let id = MicroId::local("dayframe.counter");
    assert_eq!(
        serde_json::to_string(&id).unwrap(),
        "\"did:example:clay-local/dayframe.counter\""
    );
    insta::assert_snapshot!(serde_json::to_string_pretty(&id).unwrap());
    let back: MicroId =
        serde_json::from_str("\"did:example:clay-local/dayframe.counter\"").unwrap();
    assert_eq!(back, id);
}

/// The text form is `<did>/<name>` and is checked wherever an id is made, deserialization
/// included; the name is the plug-name grammar `big_cartridges` enforces (dots allowed, never
/// leading, trailing or doubled).
#[test]
fn micro_id_text_form_is_a_did_slash_a_plug_name() {
    let id =
        MicroId::parse("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK/dayframe.counter")
            .unwrap();
    assert_eq!(
        id.namespace(),
        "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
    );
    assert_eq!(id.name(), "dayframe.counter");
    assert_eq!(id.to_string(), id.as_str());
    assert_eq!(MicroId::installed(id.namespace(), id.name()).unwrap(), id);
    // Percent escapes are uppercase hex, `:` may appear in the method-specific id.
    MicroId::parse("did:web:example.com%3A8080:users/photos").unwrap();
    for bad in [
        "dayframe.counter",
        "/dayframe.counter",
        "did:example:x/",
        "example:x/dayframe.counter",
        "did:Example:x/dayframe.counter",
        "did:example:/dayframe.counter",
        "did:example:x:/dayframe.counter",
        "did:example:x%3a/dayframe.counter",
        "did:example:x/.dayframe",
        "did:example:x/dayframe.",
        "did:example:x/day..frame",
        "did:example:x/Dayframe",
        "did:example:x/1dayframe",
        "did:example:x/dayframe.counter/more",
    ] {
        assert!(MicroId::parse(bad).is_err(), "{bad:?} must be refused");
        assert!(
            serde_json::from_value::<MicroId>(serde_json::json!(bad)).is_err(),
            "{bad:?} must not deserialize"
        );
    }
    assert!(MicroId::installed(LOCAL_NAMESPACE, &"a".repeat(65)).is_err());
}

/// A request omits `necessity` when it is `required` and `resolution` when it is `configured`,
/// exactly as `big_cartridges_core::request::Request` does, so a canonical manifest stays minimal;
/// an omitted member reads back as its default and a non-default one is kept.
#[test]
fn request_defaults_are_off_the_wire() {
    let request = Request {
        binding: "shelf".into(),
        contract: shelf(),
        access: Some(Access::Read),
        necessity: Necessity::Required,
        resolution: BindingResolution::Configured,
        min_revision: None,
    };
    let wire = serde_json::to_value(&request).unwrap();
    assert_eq!(
        wire,
        serde_json::json!({"binding": "shelf", "contract": "org.example.test.shelf", "access": "read"})
    );
    assert_eq!(serde_json::from_value::<Request>(wire).unwrap(), request);
    let explicit = Request {
        necessity: Necessity::Optional,
        resolution: BindingResolution::AtRuntime,
        ..request
    };
    let wire = serde_json::to_value(&explicit).unwrap();
    assert_eq!(wire["necessity"], "optional");
    assert_eq!(wire["resolution"], "atRuntime");
    assert_eq!(serde_json::from_value::<Request>(wire).unwrap(), explicit);
}

#[test]
fn manifest_canonical_wire_shape() {
    insta::assert_snapshot!(serde_json::to_string_pretty(&canonical_manifest()).unwrap());
}

#[test]
fn contribution_nested_value_wire_shape() {
    insta::assert_snapshot!(serde_json::to_string_pretty(&nested_contribution()).unwrap());
}

#[test]
fn facet_access_acl_wire_shape() {
    let acl = FacetAccess {
        tag: nsid("config"),
        key_id: Some("count".into()),
        read: true,
        write: true,
        create: false,
        delete: false,
    };
    insta::assert_snapshot!(serde_json::to_string_pretty(&acl).unwrap());
}

// ── Sidecar ──

#[test]
fn sidecar_wire_shape_is_flat_beside_the_manifest() {
    insta::assert_snapshot!(serde_json::to_string_pretty(&canonical_sidecar()).unwrap());
}

#[test]
fn sidecar_without_authority_omits_it_and_roundtrips() {
    let sidecar = Sidecar::new("dayframe.counter", 1, canonical_manifest());
    let json = serde_json::to_value(&sidecar).unwrap();
    assert!(json.get("authority").is_none());
    assert_eq!(serde_json::from_value::<Sidecar>(json).unwrap(), sidecar);
}

/// A declared contract may use a schema outside the package's authority only by requesting it
/// (ADR 028 §4): the request's `contract` names the schema. An unrequested one is refused.
#[test]
fn a_contract_may_reference_a_foreign_schema_it_requests() {
    let foreign = Nsid::literal("org.other.tally.value");
    let mut sidecar = canonical_sidecar();
    sidecar.manifest.collection_contracts[0].value_schema = foreign.clone();
    assert_eq!(
        sidecar.validate().unwrap_err(),
        "value schema org.other.tally.value is outside the package's authority \
         org.example.test.counter"
    );
    sidecar.manifest.requires.push(Request {
        binding: "tally-value".into(),
        contract: foreign.clone(),
        access: None,
        necessity: Necessity::Required,
        resolution: BindingResolution::Configured,
        min_revision: None,
    });
    sidecar.validate().unwrap();
    // The write-context schema is held to the same rule, and a request for another NSID does
    // not stand in for it.
    let context = Nsid::literal("org.other.tally.context");
    sidecar.manifest.collection_contracts[0].write_context_schema = Some(context.clone());
    assert!(
        sidecar
            .validate()
            .unwrap_err()
            .starts_with("write-context schema org.other.tally.context is outside")
    );
    sidecar.manifest.collection_requests.push(Request {
        binding: "tally-context".into(),
        contract: context,
        access: Some(Access::Read),
        necessity: Necessity::Optional,
        resolution: BindingResolution::Configured,
        min_revision: None,
    });
    sidecar.validate().unwrap();
}

#[test]
fn a_package_id_is_a_plug_name() {
    let mut sidecar = canonical_sidecar();
    sidecar.id = "Dayframe.Counter".into();
    assert_eq!(
        sidecar.validate().unwrap_err(),
        "package id \"Dayframe.Counter\" is not a plug name"
    );
}

#[test]
fn a_package_declares_only_nsids_under_its_authority() {
    canonical_sidecar().validate().unwrap();
    // Requests may name anyone's NSIDs: only declarations are held to the authority.
    let mut foreign_request = canonical_sidecar();
    foreign_request.manifest.collection_requests[0].contract = Nsid::literal("org.other.tallies");
    foreign_request.validate().unwrap();
    // Implementing a contract is not owning it (ADR 028 §5): a callback may implement, and a
    // method may be registered under, any NSID, while a collection contract the package
    // declares must stay under its authority.
    let mut foreign_endpoints = canonical_sidecar();
    foreign_endpoints.manifest.endpoints.insert(
        "increment".into(),
        Endpoint::Callback {
            implements: Nsid::literal("org.example.other.toolbar.action"),
        },
    );
    foreign_endpoints.manifest.endpoints.insert(
        "count".into(),
        Endpoint::Method {
            nsid: Nsid::literal("org.example.other.counter.count"),
        },
    );
    foreign_endpoints.validate().unwrap();

    type Mutation = fn(&mut Sidecar);
    let cases: &[(Mutation, &str)] = &[
        (
            |sidecar| {
                sidecar.manifest.collection_contracts[0].id = Nsid::literal("org.other.tallies")
            },
            "collection contract org.other.tallies is outside the package's authority \
             org.example.test.counter",
        ),
        (
            |sidecar| {
                sidecar.manifest.collection_contracts[0].write_context_schema =
                    Some(Nsid::literal("org.example.test.counterfeit.context"))
            },
            "write-context schema org.example.test.counterfeit.context is outside the package's \
             authority org.example.test.counter",
        ),
        (
            |sidecar| sidecar.authority = None,
            "collection contract org.example.test.counter.tallies is declared by a package with \
             no authority",
        ),
    ];
    for &(mutate, expected) in cases {
        let mut sidecar = canonical_sidecar();
        mutate(&mut sidecar);
        assert_eq!(sidecar.validate().unwrap_err(), expected);
    }
}

#[cfg(feature = "contract")]
mod builder {
    use super::*;
    use crate::CollectionDecl;
    use crate::ComponentDecl;
    use crate::ConfigDecl;

    struct Shelf;

    impl CollectionDecl for Shelf {
        const NSID: &'static str = "org.example.test.shelf";
        type Key = String;
        type Value = Card;
    }

    #[derive(Serialize, Deserialize)]
    struct Card {
        title: String,
        front: Link,
        back: Link,
    }

    struct Front;

    impl ComponentDecl for Front {
        const NAME: &'static str = "front";
    }

    struct Back;

    impl ComponentDecl for Back {
        const NAME: &'static str = "back";
    }

    struct Tally;

    impl ConfigDecl for Tally {
        const BINDING: &'static str = "tally";
        const NSID: &'static str = "org.example.test.tally";
        const KEY: &'static str = "main";
        type Value = u64;
    }

    use crate::Link;

    #[test]
    fn a_contribution_numbers_its_links_in_the_order_the_row_takes_them() {
        let manifest = ManifestBuilder::new()
            .view::<Front>()
            .view::<Back>()
            .config::<Tally>()
            .writes::<Shelf>("cards", Necessity::Optional)
            .contributes::<Shelf>("card", "k", |at| Card {
                title: "T".into(),
                back: at.own::<Back>(),
                front: at.own::<Front>(),
            })
            .build();
        let contribution = &manifest.contributions[0];
        assert_eq!(contribution.binding, "cards");
        assert_eq!(
            contribution.value,
            serde_json::json!({
                "title": "T",
                "back": { "$link": 0 },
                "front": { "$link": 1 },
            })
        );
        let linked: Vec<_> = contribution
            .links
            .iter()
            .map(|link| (link.index, link.endpoint.as_str()))
            .collect();
        assert_eq!(linked, [(0, "back"), (1, "front")]);
        assert_eq!(manifest.configuration_requests[0].binding, "tally");
    }

    #[test]
    #[should_panic(expected = "needs a write request")]
    fn a_contribution_without_a_write_request_is_a_programming_error() {
        ManifestBuilder::new()
            .view::<Front>()
            .reads::<Shelf>("cards", Necessity::Optional)
            .contributes::<Shelf>("card", "k", |at| Card {
                title: "T".into(),
                back: at.own::<Front>(),
                front: at.own::<Front>(),
            });
    }

    #[test]
    #[should_panic(expected = "does not declare")]
    fn a_link_to_an_undeclared_view_is_a_programming_error() {
        ManifestBuilder::new()
            .writes::<Shelf>("cards", Necessity::Optional)
            .contributes::<Shelf>("card", "k", |at| Card {
                title: "T".into(),
                back: at.own::<Back>(),
                front: at.own::<Back>(),
            });
    }

    #[test]
    fn a_link_is_a_tagged_index_in_cbor_and_an_object_in_json() {
        let link = Link::at(3);
        assert_eq!(
            serde_json::to_value(link).unwrap(),
            serde_json::json!({ "$link": 3 })
        );
        let mut bytes = Vec::new();
        ciborium::into_writer(&link, &mut bytes).unwrap();
        assert_eq!(
            ciborium::from_reader::<Link, _>(bytes.as_slice()).unwrap(),
            link
        );
        assert_eq!(bytes[..3], [0xd9, 0xa7, 0xf8]);
    }
}
