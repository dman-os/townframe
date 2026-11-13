use super::*;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::dmeta::DocId;
use big_cabinet_core::doc::ChangeHash;
use big_cabinet_core::reference::DatumPin;
use big_cabinet_core::reference::Location;
use serde_json::json;

fn cartridge() -> Cartridge {
    Cartridge::new(
        example_cartridge!("photos").parse().unwrap(),
        "1.2.3".parse().unwrap(),
    )
}

fn datum_pin() -> DatumPin {
    DatumPin::new(Location::parse("dtxt:datum:self/org.example.daybook/test").unwrap())
}

fn manifest_cartridge() -> Cartridge {
    let mut cartridge = cartridge();
    cartridge.title = Some("Photos".into());
    cartridge.dependencies.insert(
        example_cartridge!("search-api").parse().unwrap(),
        "^2".parse().unwrap(),
    );
    cartridge
        .lexicons
        .insert(Nsid::literal("org.example.photos.album"), datum_pin());
    cartridge
        .sidecars
        .insert(Nsid::literal("org.example.photos.sidecar"), datum_pin());
    cartridge.components.insert("handler".into(), datum_pin());
    cartridge.micro = Some(MicroContribution {
        component: "handler".into(),
        schema: "org.example.clay.micro".into(),
        description: [("entry".to_owned(), json!("gallery.js"))]
            .into_iter()
            .collect(),
        extra: Default::default(),
    });
    cartridge
}
fn datum_permbind() -> Permbind {
    Permbind::Datum(PermbindDatum {
        nsid: Nsid::literal("org.example.photos.album"),
        description: "Albums".into(),
        read: Some(Necessity::Required),
        write: Some(Necessity::Required),
        create: Some(Necessity::Required),
        delete: None,
        extra: Default::default(),
    })
}

fn routine(uses: BTreeMap<String, RoutineUse>) -> Routine {
    Routine {
        r#impl: RoutineImpl::ComponentExport(ComponentExport {
            component: "main".into(),
            export: "create-album".into(),
            extra: Default::default(),
        }),
        implements: Nsid::literal("org.example.photos.createAlbum"),
        uses,
        extra: Default::default(),
    }
}

#[test]
fn manifest_round_trips_with_typed_nsid_and_union_members() {
    let mut cartridge = cartridge();
    cartridge
        .settings
        .insert(Nsid::literal("org.example.photos.settings"));
    cartridge
        .permbinds
        .insert("albums".into(), datum_permbind());
    cartridge.nodule = Some(Nodule {
        routines: BTreeMap::from([(
            "createAlbum".into(),
            routine(BTreeMap::from([(
                "albums".into(),
                RoutineUse {
                    read: false,
                    write: true,
                    create: true,
                    delete: false,
                    extra: Default::default(),
                },
            )])),
        )]),
        ..Default::default()
    });

    cartridge.validate().unwrap();
    let wire = serde_json::to_value(&cartridge).unwrap();
    assert_eq!(wire["settings"], json!(["org.example.photos.settings"]));
    assert_eq!(wire["permbinds"]["albums"]["$type"], PermbindDatum::ID);
    assert_eq!(
        wire["nodule"]["routines"]["createAlbum"]["impl"]["$type"],
        ComponentExport::ID
    );
    assert_eq!(
        wire["nodule"]["routines"]["createAlbum"]["uses"]["albums"]["write"],
        true
    );
    assert!(
        wire["nodule"]["routines"]["createAlbum"]["uses"]["albums"]
            .get("read")
            .is_none()
    );
    assert_eq!(
        serde_json::from_value::<Cartridge>(wire).unwrap(),
        cartridge
    );
}

#[test]
fn unknown_union_members_and_fields_are_carried() {
    let mut wire = serde_json::to_value(cartridge()).unwrap();
    wire["permbinds"] = json!({
        "future": {
            "$type": "org.example.daybook.cartridge.permbind.future",
            "futureField": true
        }
    });
    wire["futureEnvelopeField"] = json!({"kept": [1, 2]});

    let decoded: Cartridge = serde_json::from_value(wire.clone()).unwrap();
    assert!(matches!(decoded.permbinds["future"], Permbind::Unknown(_)));
    assert_eq!(serde_json::to_value(decoded).unwrap(), wire);
}

#[test]
fn malformed_nsids_are_refused_at_deserialization() {
    let mut wire = serde_json::to_value(cartridge()).unwrap();
    wire["settings"] = json!(["not an nsid"]);
    assert!(serde_json::from_value::<Cartridge>(wire).is_err());

    let mut routine_wire = serde_json::to_value(routine(BTreeMap::new())).unwrap();
    routine_wire["implements"] = json!("not an nsid");
    assert!(serde_json::from_value::<Routine>(routine_wire).is_err());
}

#[test]
fn routine_uses_must_resolve_and_cannot_widen_a_permbind() {
    let mut cartridge = cartridge();
    cartridge
        .permbinds
        .insert("albums".into(), datum_permbind());
    cartridge.nodule = Some(Nodule {
        routines: BTreeMap::from([(
            "createAlbum".into(),
            routine(BTreeMap::from([(
                "missing".into(),
                RoutineUse {
                    read: true,
                    ..Default::default()
                },
            )])),
        )]),
        ..Default::default()
    });
    let error = cartridge.validate().unwrap_err().to_string();
    assert!(error.contains("must name a permbind"), "{error}");

    let mut routine_uses = BTreeMap::new();
    routine_uses.insert(
        "albums".into(),
        RoutineUse {
            delete: true,
            ..Default::default()
        },
    );
    cartridge
        .nodule
        .as_mut()
        .unwrap()
        .routines
        .get_mut("createAlbum")
        .unwrap()
        .uses = routine_uses;
    let error = cartridge.validate().unwrap_err().to_string();
    assert!(error.contains("does not declare"), "{error}");
}

#[test]
fn colliding_manifest_names_are_refused() {
    let mut cartridge = cartridge();
    cartridge.permbinds.insert("state".into(), datum_permbind());
    cartridge.nodule = Some(Nodule {
        local_state: BTreeMap::from([(
            "state".into(),
            LocalState {
                kind: StateKind::Kv,
                extra: Default::default(),
            },
        )]),
        ..Default::default()
    });
    let error = cartridge.validate().unwrap_err().to_string();
    assert!(error.contains("must be unique"), "{error}");
}

#[test]
fn a_cartridge_id_is_a_did_and_a_lowercase_name() {
    let id: CartridgeId = "did:plc:publisher/photos".parse().unwrap();
    assert_eq!(id.to_string(), "did:plc:publisher/photos");
    assert!("did:plc:publisher/Photos".parse::<CartridgeId>().is_err());
}

#[test]
fn dids_are_checked_by_the_did_core_grammar_and_methods_are_open() {
    assert!("did:key:z6MkTest/photos".parse::<CartridgeId>().is_ok());
    assert!("not-a-did/photos".parse::<CartridgeId>().is_err());
}

#[test]
fn a_cartridge_id_round_trips_through_text_serde_and_a_datum_key_slot() {
    let id: CartridgeId = "did:plc:publisher/photos".parse().unwrap();
    let value = serde_json::to_value(&id).unwrap();
    assert_eq!(value, "did:plc:publisher/photos");
    assert_eq!(serde_json::from_value::<CartridgeId>(value).unwrap(), id);
}

#[test]
fn versions_and_requirements_reject_nonsense() {
    assert!("1.2.3".parse::<Version>().is_ok());
    assert!("not-a-version".parse::<Version>().is_err());
    assert!("^1.2".parse::<VersionReq>().is_ok());
}

#[test]
fn the_authority_is_an_nsid_prefix_and_part_of_the_revision() {
    let mut cartridge = cartridge();
    cartridge.authority = Some("org.example.photos".into());
    let first = cartridge.revision();
    cartridge.authority = Some("org.example.albums".into());
    assert_ne!(first, cartridge.revision());
    cartridge.authority = Some("photos".into());
    assert!(cartridge.validate().is_err());
}

#[test]
fn the_revision_is_the_content_hash_and_sees_unknown_members() {
    let mut cartridge = cartridge();
    let first = cartridge.revision();
    cartridge
        .extra
        .insert("futureField".into(), serde_json::json!(true));
    assert_ne!(first, cartridge.revision());
}

#[test]
fn the_envelope_spells_ids_versions_and_digests_as_text() {
    let cartridge = manifest_cartridge();
    cartridge.validate().unwrap();
    let value = serde_json::to_value(&cartridge).unwrap();
    assert_eq!(value["id"], example_cartridge!("photos"));
    assert_eq!(value["version"], "1.2.3");
    assert_eq!(
        value["dependencies"][example_cartridge!("search-api")],
        "^2"
    );
    assert!(value["lexicons"]["org.example.photos.album"]["ref"].is_string());
    assert!(value["sidecars"]["org.example.photos.sidecar"]["ref"].is_string());
    assert!(value["components"]["handler"]["ref"].is_string());

    let component = WasmComponent {
        bytes: datum_pin(),
        digest: Digest::of(b"wasm"),
        size: 4,
        media_type: "application/wasm".into(),
        extra: Default::default(),
    };
    let component_value = serde_json::to_value(&component).unwrap();
    let digest = component_value["digest"].as_str().unwrap();
    assert!(digest.starts_with('z'), "{digest}");
    assert_eq!(digest.parse::<Digest>().unwrap(), Digest::of(b"wasm"));
    assert_eq!(
        serde_json::from_value::<Cartridge>(value).unwrap(),
        cartridge
    );
}

#[test]
fn an_unknown_contribution_is_carried_not_refused() {
    let mut value = serde_json::to_value(manifest_cartridge()).unwrap();
    value["futureContribution"] = json!({
        "$type": "org.example.daybook.cartridge.theme",
        "palette": ["#fff"],
    });
    value["futureField"] = json!(true);
    let cartridge: Cartridge = serde_json::from_value(value.clone()).unwrap();
    cartridge.validate().unwrap();
    assert_eq!(
        cartridge.extra["futureContribution"]["$type"],
        "org.example.daybook.cartridge.theme"
    );
    assert_eq!(serde_json::to_value(&cartridge).unwrap(), value);
}

#[test]
fn validation_owns_distribution_not_contribution_content() {
    let mut self_dependency = manifest_cartridge();
    let id = self_dependency.id.clone();
    self_dependency
        .dependencies
        .insert(id, "^1".parse().unwrap());
    let error = self_dependency.validate().unwrap_err().to_string();
    assert!(error.contains("depend on itself"), "{error}");

    let mut bad_authority = manifest_cartridge();
    bad_authority.authority = Some("org".into());
    let error = bad_authority.validate().unwrap_err().to_string();
    assert!(error.contains("authority"), "{error}");

    let mut missing_component = manifest_cartridge();
    missing_component.micro.as_mut().unwrap().component = "absent".into();
    let error = missing_component.validate().unwrap_err().to_string();
    assert!(error.contains("micro.component"), "{error}");

    let mut open_content = manifest_cartridge();
    open_content
        .micro
        .as_mut()
        .unwrap()
        .description
        .insert("anything".into(), json!(null));
    open_content.validate().unwrap();
}

#[test]
fn a_micro_names_an_existing_component_artifact() {
    let mut missing = manifest_cartridge();
    missing.micro.as_mut().unwrap().component = "absent".into();
    let error = missing.validate().unwrap_err().to_string();
    assert!(error.contains("micro.component"), "{error}");

    let mut empty = manifest_cartridge();
    empty.micro.as_mut().unwrap().component.clear();
    let error = empty.validate().unwrap_err().to_string();
    assert!(error.contains("micro.component"), "{error}");
}

#[test]
fn a_micro_carries_unknown_members_and_its_open_description() {
    let mut value = serde_json::to_value(manifest_cartridge()).unwrap();
    value["micro"]["futureMember"] = json!({"k": [1, 2]});
    value["micro"]["description"]["entrypoints"] = json!({"x": 1});
    let decoded: Cartridge = serde_json::from_value(value.clone()).unwrap();
    decoded.validate().unwrap();
    let micro = decoded.micro.as_ref().unwrap();
    assert_eq!(micro.component, "handler");
    assert_eq!(micro.description["entrypoints"], json!({"x": 1}));
    assert_eq!(serde_json::to_value(&decoded).unwrap(), value);
}

#[cfg(feature = "schemars")]
#[test]
fn the_envelope_datums_declare_publish_and_admit() {
    let universe = crate::registry::registry_universe();
    let ids: Vec<&str> = universe
        .datums
        .iter()
        .map(|datum| datum.id.as_str())
        .collect();
    assert!(ids.contains(&Cartridge::ID));
    assert!(ids.contains(&Nodule::ID));
    assert!(ids.contains(&MicroContribution::ID));
    assert!(ids.contains(&WasmComponent::ID));
    let lexicons = big_cabinet_core::datum::Bundle::new(&universe, Vec::new())
        .lexicons()
        .unwrap();
    assert_eq!(lexicons.len(), ids.len());
}

#[test]
fn keys_name_the_version_and_its_artifacts() {
    let value = serde_json::to_value(manifest_cartridge()).unwrap();
    let lexicons = value["lexicons"].as_object().unwrap();
    let sidecars = value["sidecars"].as_object().unwrap();
    let components = value["components"].as_object().unwrap();
    assert!(lexicons.contains_key("org.example.photos.album"));
    assert!(sidecars.contains_key("org.example.photos.sidecar"));
    assert!(components.contains_key("handler"));
}

#[cfg(feature = "schemars")]
#[test]
fn the_registry_universe_publishes_every_lexicon() {
    let universe = crate::registry::registry_universe();
    let lexicons = big_cabinet_core::datum::Bundle::new(&universe, Vec::new())
        .lexicons()
        .unwrap();
    assert_eq!(lexicons.len(), universe.datums.len());
    assert!(lexicons.iter().all(|lexicon| !lexicon.file.is_null()));
}

#[test]
fn the_example_namespace_is_the_did_of_the_example_seed() {
    let key = ed25519_dalek::SigningKey::from_bytes(&test_support::EXAMPLE_SEED);
    assert_eq!(
        crate::signature::did_key(key.verifying_key().as_bytes()),
        test_support::EXAMPLE_NAMESPACE
    );
}

#[test]
fn a_signature_round_trips_and_leaves_the_revision_alone() {
    let unsigned = cartridge();
    let signed = test_support::sign_as_example(unsigned.clone());
    signed.verify_signature().unwrap();
    assert_eq!(signed.revision(), unsigned.revision());
    let value = serde_json::to_value(&signed).unwrap();
    assert_eq!(value["signature"]["alg"], "ed25519");
    assert!(
        value["signature"]["value"]
            .as_str()
            .unwrap()
            .starts_with('z')
    );
    let decoded: Cartridge = serde_json::from_value(value).unwrap();
    decoded.verify_signature().unwrap();
    assert_eq!(
        unsigned.verify_signature(),
        Err(crate::signature::SignatureError::Missing)
    );
}

#[test]
fn a_tampered_envelope_does_not_verify() {
    let mut signed = test_support::sign_as_example(cartridge());
    signed.title = Some("Not what was signed".into());
    assert!(matches!(
        signed.verify_signature(),
        Err(crate::signature::SignatureError::Invalid(_))
    ));
    let mut carried = test_support::sign_as_example(cartridge());
    carried.extra.insert("futureField".into(), json!(1));
    assert!(matches!(
        carried.verify_signature(),
        Err(crate::signature::SignatureError::Invalid(_))
    ));
}

#[test]
fn another_keys_signature_does_not_verify() {
    let forged = test_support::sign_with_seed(cartridge(), &[9; 32]);
    assert!(matches!(
        forged.verify_signature(),
        Err(crate::signature::SignatureError::Invalid(_))
    ));
}

#[test]
fn only_ed25519_did_key_is_verifiable() {
    let mut web = test_support::sign_as_example(cartridge());
    web.id = CartridgeId::new("did:web:example.com", "photos").unwrap();
    assert_eq!(
        web.verify_signature(),
        Err(crate::signature::SignatureError::UnsupportedMethod(
            "web".into()
        ))
    );

    let mut other = test_support::sign_as_example(cartridge());
    other.id = CartridgeId::new(
        "did:key:zQ3shokFTS3brHcDQrn82RUDfCZESWL1ZdCEJwekUDPQiYBme",
        "photos",
    )
    .unwrap();
    assert!(matches!(
        other.verify_signature(),
        Err(crate::signature::SignatureError::UnsupportedKey(_))
    ));

    let mut unknown = test_support::sign_as_example(cartridge());
    unknown.signature.as_mut().unwrap().alg = "rsa".into();
    assert_eq!(
        unknown.verify_signature(),
        Err(crate::signature::SignatureError::UnsupportedAlgorithm(
            "rsa".into()
        ))
    );

    let mut garbled = test_support::sign_as_example(cartridge());
    garbled.signature.as_mut().unwrap().value = "z11".into();
    assert!(matches!(
        garbled.verify_signature(),
        Err(crate::signature::SignatureError::Malformed(_))
    ));
}

#[test]
fn a_pinned_datum_pin_tampering_breaks_verification() {
    let key = "org.example.photos.album";
    let mut unsigned = cartridge();
    unsigned.lexicons.insert(Nsid::literal(key), datum_pin());
    let signed = test_support::sign_as_example(unsigned);
    signed.verify_signature().unwrap();

    let mut value = serde_json::to_value(&signed).unwrap();
    let replacement_pin =
        crate::registry::version_pin(DocId::random(), &signed.version, [ChangeHash([9; 32])]);
    value["lexicons"][key]["heads"] =
        serde_json::to_value(replacement_pin).unwrap()["heads"].clone();
    let tampered: Cartridge = serde_json::from_value(value).unwrap();
    assert!(matches!(
        tampered.verify_signature(),
        Err(crate::signature::SignatureError::Invalid(_))
    ));
}
