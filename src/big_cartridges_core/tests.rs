use super::*;
use big_cabinet_core::datum::DatumMember;
use serde_json::json;

fn cartridge() -> Cartridge {
    let mut cartridge = Cartridge::new(
        example_cartridge!("photos").parse().unwrap(),
        "1.2.3".parse().unwrap(),
    );
    cartridge.title = Some("Photos".into());
    cartridge.dependencies.push(Dependency {
        id: example_cartridge!("search-api").parse().unwrap(),
        version: "^2".parse().unwrap(),
        extra: Default::default(),
    });
    cartridge.artifacts.push(Artifact {
        key: "routines".into(),
        digest: Digest::of(b"wasm"),
        size: Some(4),
        media_type: Some("application/wasm".into()),
        extra: Default::default(),
    });
    cartridge
        .contributions
        .push(Contribution::Nodule(NoduleContribution {
            schema: "org.example.daybook.nodule".into(),
            description: [("id".to_owned(), json!("org.example.photos"))]
                .into_iter()
                .collect(),
            extra: Default::default(),
        }));
    cartridge
        .contributions
        .push(Contribution::Micro(MicroContribution {
            schema: "org.example.clay.micro".into(),
            description: [("entry".to_owned(), json!("gallery.js"))]
                .into_iter()
                .collect(),
            extra: Default::default(),
        }));
    cartridge
}

#[test]
fn the_envelope_spells_ids_versions_and_digests_as_text() {
    let cartridge = cartridge();
    cartridge.validate().unwrap();
    let value = serde_json::to_value(&cartridge).unwrap();
    assert_eq!(value["id"], example_cartridge!("photos"));
    assert_eq!(value["version"], "1.2.3");
    assert_eq!(value["dependencies"][0]["version"], "^2");
    let digest = value["artifacts"][0]["digest"].as_str().unwrap();
    assert!(digest.starts_with('z'), "{digest}");
    assert_eq!(digest.parse::<Digest>().unwrap(), Digest::of(b"wasm"));
    assert_eq!(value["contributions"][0]["$type"], NoduleContribution::ID);
    assert_eq!(value["contributions"][1]["$type"], MicroContribution::ID);
    assert_eq!(
        serde_json::from_value::<Cartridge>(value).unwrap(),
        cartridge
    );
    assert_eq!(
        cartridge.nodule().unwrap().schema,
        "org.example.daybook.nodule"
    );
    assert_eq!(cartridge.micro().unwrap().schema, "org.example.clay.micro");
    assert_eq!(
        cartridge
            .artifact("routines")
            .unwrap()
            .media_type
            .as_deref(),
        Some("application/wasm")
    );
}

#[test]
fn a_cartridge_id_is_a_did_and_a_lowercase_name() {
    let text = example_cartridge!("photos");
    let id: CartridgeId = text.parse().unwrap();
    assert_eq!(id.namespace, crate::test_support::EXAMPLE_NAMESPACE);
    assert_eq!(id.name, "photos");
    assert_eq!(id.to_string(), text);
    assert_eq!(crate::test_support::cartridge_id("photos"), id);
    for bad in [
        "photos",
        "@example/photos",
        "example/photos",
        "did:key:z6Mk/",
        "did:key:z6Mk/Photos",
        "did:key:z6Mk/a/b",
        "/photos",
        "did:key:z6Mk/.photos",
        "did:key:z6Mk/photos.",
        "did:key:z6Mk/a..b",
    ] {
        assert!(bad.parse::<CartridgeId>().is_err(), "{bad}");
    }
    // Dotted names are legal, as Clay micro ids are (`dayframe.counter`).
    let dotted: CartridgeId = example_cartridge!("dayframe.counter").parse().unwrap();
    assert_eq!(dotted.name, "dayframe.counter");
}

#[test]
fn dids_are_checked_by_the_did_core_grammar_and_methods_are_open() {
    for good in [
        "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK",
        "did:plc:ewvi7nxzyoun6zhxrhs64oiz",
        "did:web:example.com",
        "did:web:example.com:user:alice",
        "did:web:localhost%3A3000:u",
        "did:a1:x",
        "did:future:a::b",
        "did:ex:a.b-c_d",
    ] {
        let text = format!("{good}/photos");
        let id: CartridgeId = text
            .parse()
            .unwrap_or_else(|error| panic!("{good}: {error}"));
        assert_eq!(id.to_string(), text);
        assert_eq!(id.namespace, good);
    }
    let too_long = format!("did:web:{}", "a".repeat(MAX_DID_OCTETS));
    for bad in [
        "",
        "did",
        "did:",
        "did:key",
        "did:key:",
        "did::x",
        "did:Key:x",
        "did:ke-y:x",
        "DID:key:x",
        // Lowercase hex is not the canonical escape: `%3a` would alias `%3A`.
        "did:web:localhost%3a3000:u",
        "did:key:x:",
        "did:key:a b",
        "did:key:a/b",
        "did:key:a%2",
        "did:key:a%zz",
        "did:key:a%",
        "did:key:a?q",
        "did:key:a#f",
        "did:key:é",
        too_long.as_str(),
    ] {
        assert!(CartridgeId::new(bad, "photos").is_err(), "{bad:?}");
    }
    let at_limit = format!("did:web:{}", "a".repeat(MAX_DID_OCTETS - "did:web:".len()));
    assert!(CartridgeId::new(&at_limit, "photos").is_ok());
}

#[test]
fn a_cartridge_id_round_trips_through_text_serde_and_a_datum_key_slot() {
    use big_cabinet_core::reference::DatumKey;
    for namespace in ["did:web:localhost%3A3000:u", "did:key:z6Mk", "did:ex:a::b"] {
        let id = CartridgeId::new(namespace, "photos").unwrap();
        let text = id.to_string();
        assert_eq!(text, format!("{namespace}/photos"));
        assert_eq!(text.parse::<CartridgeId>().unwrap(), id);
        let json = serde_json::to_value(&id).unwrap();
        assert_eq!(json, json!(text));
        assert_eq!(serde_json::from_value::<CartridgeId>(json).unwrap(), id);
        // The key slot spells the id as it stands: `:` and `%` need no escaping there, and
        // the key parses back, text and all, to the same id.
        let key = DatumKey::from_text("org.example.daybook.blob", text.clone()).unwrap();
        assert_eq!(key.slot_bytes(), text.as_bytes());
        let key_text = key.to_string();
        assert_eq!(key_text, format!("org.example.daybook.blob/{text}"));
        let reparsed: DatumKey = key_text.parse().unwrap();
        assert_eq!(
            std::str::from_utf8(reparsed.slot_bytes())
                .unwrap()
                .parse::<CartridgeId>()
                .unwrap(),
            id
        );
    }
    assert!(serde_json::from_value::<CartridgeId>(json!("@example/photos")).is_err());
}

#[test]
fn versions_and_requirements_reject_nonsense() {
    assert!("1.2".parse::<Version>().is_err());
    assert!("nonsense".parse::<VersionReq>().is_err());
}

#[test]
fn the_authority_is_an_nsid_prefix_and_part_of_the_revision() {
    let mut held = cartridge();
    for valid in ["org.example", "org.example.photos", "com.atproto"] {
        held.authority = Some(valid.into());
        held.validate().unwrap();
    }
    for invalid in [
        "",
        "org",
        "org..example",
        "org.example.",
        "Org.-example",
        "org.example/x",
    ] {
        held.authority = Some(invalid.into());
        let error = held.validate().unwrap_err().to_string();
        assert!(error.contains("`authority`"), "{invalid}: {error}");
    }
    // It is signed with the rest of the envelope: the revision sees it.
    held.authority = Some("org.example.photos".into());
    let mut other = held.clone();
    other.authority = Some("org.example.albums".into());
    assert_ne!(held.revision(), other.revision());
    let round: Cartridge = serde_json::from_value(serde_json::to_value(&held).unwrap()).unwrap();
    assert_eq!(round, held);
}

#[test]
fn the_revision_is_the_content_hash_and_sees_unknown_members() {
    let cartridge = cartridge();
    let revision = cartridge.revision();
    assert_eq!(cartridge.clone().revision(), revision);
    let mut carried = cartridge.clone();
    carried.extra.insert("futureField".into(), json!(1));
    assert_ne!(carried.revision(), revision);
    let text = revision.to_string();
    assert_eq!(text.parse::<Revision>().unwrap(), revision);
    assert!("z".parse::<Revision>().is_err());
}

#[test]
fn an_unknown_contribution_is_carried_not_refused() {
    let value = json!({
        "id": example_cartridge!("photos"),
        "version": "0.1.0",
        "contributions": [
            { "$type": "org.example.daybook.cartridge.theme", "palette": ["#fff"] },
        ],
        "futureField": true,
    });
    let cartridge: Cartridge = serde_json::from_value(value.clone()).unwrap();
    cartridge.validate().unwrap();
    assert!(matches!(
        cartridge.contributions[0],
        Contribution::Unknown(_)
    ));
    assert_eq!(cartridge.nodule(), None);
    assert_eq!(serde_json::to_value(&cartridge).unwrap(), value);
}

#[test]
fn validation_owns_distribution_not_contribution_content() {
    let mut two_nodules = cartridge();
    two_nodules
        .contributions
        .push(two_nodules.contributions[0].clone());
    let error = two_nodules.validate().unwrap_err().to_string();
    assert!(error.contains("one node-facing nodule"), "{error}");

    let mut two_micros = cartridge();
    two_micros
        .contributions
        .push(two_micros.contributions[1].clone());
    let error = two_micros.validate().unwrap_err().to_string();
    assert!(error.contains("has one micro"), "{error}");

    let mut duplicate_artifact = cartridge();
    duplicate_artifact
        .artifacts
        .push(duplicate_artifact.artifacts[0].clone());
    let error = duplicate_artifact.validate().unwrap_err().to_string();
    assert!(error.contains("artifact key `routines`"), "{error}");

    let mut self_dependency = cartridge();
    self_dependency.dependencies[0].id = self_dependency.id.clone();
    let error = self_dependency.validate().unwrap_err().to_string();
    assert!(error.contains("depend on itself"), "{error}");

    let mut bad_schema = cartridge();
    let Contribution::Nodule(nodule) = &mut bad_schema.contributions[0] else {
        unreachable!()
    };
    nodule.schema = "not an nsid".into();
    let error = bad_schema.validate().unwrap_err().to_string();
    assert!(error.contains("contributions[0].schema"), "{error}");

    // The description is a consumer's business: nonsense passes the catalog.
    let mut nonsense_description = cartridge();
    let Contribution::Nodule(nodule) = &mut nonsense_description.contributions[0] else {
        unreachable!()
    };
    nodule.description = [("anything".to_owned(), json!(null))].into_iter().collect();
    nonsense_description.validate().unwrap();
}

#[cfg(feature = "schemars")]
mod declaration {
    use super::*;
    use big_cabinet_core::datum::Bundle;
    use big_cabinet_core::datum::Declaration;
    use big_cabinet_core::datum::Universe;

    #[test]
    fn the_envelope_datums_declare_publish_and_admit() {
        let universe = Universe::collect([
            Declaration::of::<Cartridge>().unwrap(),
            Declaration::of::<NoduleContribution>().unwrap(),
            Declaration::of::<MicroContribution>().unwrap(),
        ]);
        let ids: Vec<&str> = universe
            .datums
            .iter()
            .map(|datum| datum.id.as_str())
            .collect();
        assert_eq!(
            ids,
            [
                "org.example.daybook.cartridge",
                "org.example.daybook.cartridge.nodule",
                "org.example.daybook.cartridge.micro",
            ]
        );
        let bundle = Bundle::new(&universe, Vec::new());
        let lexicons = bundle.lexicons().unwrap();
        assert_eq!(lexicons.len(), 3);
        let cartridge = &lexicons[0].file;
        let contributions = &cartridge["defs"]["main"]["properties"]["contributions"]["items"];
        assert_eq!(contributions["type"], "union", "{cartridge}");
        assert_eq!(contributions.get("closed"), None, "{cartridge}");
        assert_eq!(
            contributions["refs"],
            json!([NoduleContribution::ID, MicroContribution::ID]),
            "{cartridge}"
        );
        // A description is `unknown` to the catalog's Lexicon: any data object.
        let nodule = &lexicons[1].file;
        assert_eq!(
            nodule["defs"]["main"]["properties"]["description"]["type"], "unknown",
            "{nodule}"
        );
        for (index, lexicon) in lexicons.iter().enumerate() {
            let carried: Vec<&serde_json::Value> = lexicons
                .iter()
                .enumerate()
                .filter(|(other, _)| *other != index)
                .map(|(_, other)| &other.file)
                .collect();
            let compiled = bundle
                .validate_lexicon(&lexicon.id, &lexicon.file, &carried)
                .unwrap_or_else(|error| panic!("{}: {error}", lexicon.id));
            if lexicon.id == Cartridge::ID {
                let mut value = serde_json::to_value(super::cartridge()).unwrap();
                value["$type"] = json!(Cartridge::ID);
                for size in [json!(null), json!(0), json!(4), json!(i64::MAX)] {
                    value["artifacts"][0]["size"] = size;
                    let stored = big_cabinet_core::datum::Stored::check(&compiled, value.clone())
                        .unwrap()
                        .into_value();
                    assert_eq!(stored, value);
                    let envelope: Cartridge = serde_json::from_value(stored).unwrap();
                    envelope.validate().unwrap();
                    assert_eq!(
                        serde_json::to_value(envelope.artifacts[0].size).unwrap(),
                        value["artifacts"][0]["size"]
                    );
                }
                for size in [json!(-1), json!(0.5), json!(u64::MAX), json!("4")] {
                    value["artifacts"][0]["size"] = size;
                    assert!(
                        big_cabinet_core::datum::Stored::check(&compiled, value.clone()).is_err()
                    );
                }
            }
        }
    }
}

mod registry_layout {
    use super::*;
    use crate::registry::*;
    use big_cabinet_core::DatumKey;

    #[test]
    fn keys_name_the_version_and_its_artifacts() {
        let version: Version = "1.2.3-beta.1".parse().unwrap();
        assert_eq!(
            identity_key().to_string(),
            "org.example.daybook.cartridge.identity/main"
        );
        assert_eq!(
            version_key(&version).to_string(),
            "org.example.daybook.cartridge/1.2.3-beta.1"
        );
        let artifact = artifact_key(&version, "wasm/handler");
        assert_eq!(
            artifact.to_string(),
            "org.example.daybook.blob/cartridge.artifact/1.2.3-beta.1/wasm/handler"
        );
        assert_eq!(
            artifact_slot_version(&artifact).as_deref(),
            Some("1.2.3-beta.1")
        );
        assert_eq!(artifact_slot_version(&version_key(&version)), None);
        let foreign = DatumKey::from_text(big_blobs_core::Blob::ID, "nodule.artifact/x/y").unwrap();
        assert_eq!(artifact_slot_version(&foreign), None);
    }

    #[cfg(feature = "schemars")]
    #[test]
    fn the_registry_universe_publishes_every_lexicon() {
        let universe = registry_universe();
        let ids: Vec<&str> = universe
            .datums
            .iter()
            .map(|datum| datum.id.as_str())
            .collect();
        assert!(
            ids.contains(&CartridgeIdentity::ID) && ids.contains(&Cartridge::ID),
            "{ids:?}"
        );
        big_cabinet_core::datum::Bundle::new(&universe, Vec::new())
            .lexicons()
            .unwrap();
    }
}

mod signatures {
    use super::*;
    use crate::signature::SignatureError;
    use crate::signature::did_key;
    use crate::test_support::EXAMPLE_SEED;
    use crate::test_support::sign_as_example;
    use crate::test_support::sign_with_seed;

    #[test]
    fn the_example_namespace_is_the_did_of_the_example_seed() {
        let key = ed25519_dalek::SigningKey::from_bytes(&EXAMPLE_SEED);
        assert_eq!(
            did_key(key.verifying_key().as_bytes()),
            test_support::EXAMPLE_NAMESPACE
        );
    }

    #[test]
    fn a_signature_round_trips_and_leaves_the_revision_alone() {
        let unsigned = cartridge();
        let signed = sign_as_example(unsigned.clone());
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
        assert_eq!(unsigned.verify_signature(), Err(SignatureError::Missing));
    }

    #[test]
    fn a_tampered_envelope_does_not_verify() {
        let mut signed = sign_as_example(cartridge());
        signed.title = Some("Not what was signed".into());
        assert!(matches!(
            signed.verify_signature(),
            Err(SignatureError::Invalid(_))
        ));
        let mut carried = sign_as_example(cartridge());
        carried.extra.insert("futureField".into(), json!(1));
        assert!(matches!(
            carried.verify_signature(),
            Err(SignatureError::Invalid(_))
        ));
    }

    #[test]
    fn another_keys_signature_does_not_verify() {
        let forged = sign_with_seed(cartridge(), &[9; 32]);
        assert!(matches!(
            forged.verify_signature(),
            Err(SignatureError::Invalid(_))
        ));
    }

    #[test]
    fn only_ed25519_did_key_is_verifiable() {
        let mut web = sign_as_example(cartridge());
        web.id = CartridgeId::new("did:web:example.com", "photos").unwrap();
        assert_eq!(
            web.verify_signature(),
            Err(SignatureError::UnsupportedMethod("web".into()))
        );
        // A did:key of another key type (a secp256k1 multicodec) is refused, not guessed at.
        let mut other = sign_as_example(cartridge());
        other.id = CartridgeId::new(
            "did:key:zQ3shokFTS3brHcDQrn82RUDfCZESWL1ZdCEJwekUDPQiYBme",
            "photos",
        )
        .unwrap();
        assert!(matches!(
            other.verify_signature(),
            Err(SignatureError::UnsupportedKey(_))
        ));
        let mut unknown = sign_as_example(cartridge());
        unknown.signature.as_mut().unwrap().alg = "rsa".into();
        assert_eq!(
            unknown.verify_signature(),
            Err(SignatureError::UnsupportedAlgorithm("rsa".into()))
        );
        let mut garbled = sign_as_example(cartridge());
        garbled.signature.as_mut().unwrap().value = "z11".into();
        assert!(matches!(
            garbled.verify_signature(),
            Err(SignatureError::Malformed(_))
        ));
    }
}
