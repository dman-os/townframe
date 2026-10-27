use super::*;
#[cfg(feature = "schemars")]
use crate::facet::FacetShape;
use ed25519_dalek::Signer;
use ed25519_dalek::SigningKey;

use crate::FacetReference;
use crate::ReferenceDocId;
use crate::ReferenceError;

fn facet_key(value: &str) -> FacetKey {
    FacetKey::from_str(value).unwrap()
}

const PORTABLE_PROOF: &str = r#"{
        "createdAt": "1970-01-01T00:00:00Z",
        "updatedAt": [],
        "actors": {
            "01890f3e-1234-7000-8000-000000000002": {
                "pubkey": "zGmaDrppBC7P5ARKV8g3djiwP89vz1jLK23V2GBjuAEGB",
                "actorUrl": "did:key:test",
                "delegation": {"$bytes": "AQID/w=="}
            }
        },
        "branchUuids": {
            "01890f3e-1234-7000-8000-000000000009": "zajdk"
        },
        "facetUuids": {
            "01890f3e-1234-7000-8000-000000000001": "org.example.note/main"
        },
        "facets": {
            "org.example.note/main": {
                "updatedAt": [],
                "deletedAt": [],
                "signatures": [{
                    "actorUuid": "01890f3e-1234-7000-8000-000000000002",
                    "docUuid": "01890f3e-1234-7000-8000-000000000009",
                    "signature": {"$bytes": "7OJ5Fpuon7QCm9iAUz+gJ7ePC9pXGDQYBu8GxSthtkEzcmogHSGTzh8Pr+G1AvIdOK2QqQ+qr2+58orrETzcBQ=="},
                    "message": {"$bytes": "ZGF5Ym9vay5mYWNldC1jbGFpbS52MgCHZXphamRrdW9yZy5leGFtcGxlLm5vdGUvbWFpblABiQ8+EjRwAIAAAAAAAAABUAGJDz4SNHAAgAAAAAAAAAIAWCQBVR4gLWvLu8YEORo43f1WfOLngEcapPP3vFmdHGSY17U+tsaA"}
                }]
            }
        }
    }"#;

fn uuid7(index: u128) -> Uuid7 {
    Uuid7::try_from(Uuid::from_u128(
        0x0189_0f3e_1234_7000_8000_0000_0000_0000 | index,
    ))
    .unwrap()
}

fn native_value() -> NativeValue {
    NativeValue::Map(BTreeMap::from([
        ("a".into(), NativeValue::Int(1)),
        ("b".into(), NativeValue::Bytes(vec![0, 255])),
    ]))
}

fn claim_for(value: &NativeValue) -> FacetClaim {
    FacetClaim {
        origin_document: DocId::new(b"doc"),
        facet_key: facet_key("org.example.note/main"),
        chosen_uuid: uuid7(1),
        actor_uuid: uuid7(2),
        operation: ClaimOperation::Set,
        value_cid: Some(cid_of(&encode_native(value))),
        predecessors: Vec::new(),
    }
}

fn signed_entry(claim: &FacetClaim) -> (ActorMetadata, FacetSignature) {
    let key = SigningKey::from_bytes(&[7; 32]);
    (
        ActorMetadata {
            pubkey: MultibaseKey::try_from(key.verifying_key().to_bytes().as_slice()).unwrap(),
            actor_url: "did:key:test".into(),
            delegation: None,
        },
        FacetSignature {
            actor_uuid: claim.actor_uuid,
            doc_uuid: uuid7(9),
            signature: SignatureBytes::try_from(
                key.sign(&claim.signing_bytes().unwrap())
                    .to_bytes()
                    .as_slice(),
            )
            .unwrap(),
            message: None,
            inherited: None,
        },
    )
}

fn cid_with(byte: u8) -> Cid {
    cid_of(&[byte; 32])
}

#[test]
fn aliases_retain_concurrency_without_a_reverse_index() {
    let mut dmeta = Dmeta::new(Timestamp::from_second(0).unwrap());
    let key = facet_key("org.example.note/main");
    dmeta.record_facet_alias(uuid7(2), key.clone()).unwrap();
    assert!(!dmeta.has_concurrent_creation(&key));
    dmeta.record_facet_alias(uuid7(1), key.clone()).unwrap();
    assert!(dmeta.has_concurrent_creation(&key));
    assert!(!dmeta.has_concurrent_creation(&facet_key("org.example.note/other")));
    // The chosen alias is the smallest one visible, and it falls out of the index order.
    let chosen = dmeta
        .facet_uuids
        .iter()
        .filter(|(_, alias)| **alias == key)
        .map(|(uuid, _)| *uuid)
        .min()
        .unwrap();
    assert_eq!(chosen, uuid7(1));
    assert_eq!(
        serde_json::from_value::<Dmeta>(serde_json::to_value(&dmeta).unwrap()).unwrap(),
        dmeta
    );
}

#[test]
fn a_facet_alias_can_be_repeated_but_never_rebound() {
    let mut dmeta = Dmeta::new(Timestamp::from_second(0).unwrap());
    let key = facet_key("org.example.note/main");
    dmeta.record_facet_alias(uuid7(1), key.clone()).unwrap();
    let original = dmeta.clone();
    dmeta.record_facet_alias(uuid7(1), key).unwrap();
    assert_eq!(dmeta, original);
    assert!(matches!(
        dmeta.record_facet_alias(uuid7(1), facet_key("org.example.note/other")),
        Err(DmetaError::ReboundAlias)
    ));
    assert_eq!(dmeta, original);
}

#[test]
fn uuid7_is_validated_in_values_and_metadata_map_keys() {
    let uuid = uuid7(1);
    assert_eq!(uuid.unix_millis(), 1_688_177_873_460);
    assert_eq!(uuid.as_uuid().get_version_num(), 7);
    let fresh = Uuid7::new();
    assert_eq!(Uuid7::try_from(*fresh.as_uuid()).unwrap(), fresh);
    assert_eq!(
        serde_json::to_value(uuid).unwrap(),
        serde_json::json!("01890f3e-1234-7000-8000-000000000001")
    );
    assert_eq!(
        serde_json::from_value::<Uuid7>(serde_json::to_value(uuid).unwrap()).unwrap(),
        uuid
    );
    let fixture: serde_json::Value = serde_json::from_str(PORTABLE_PROOF).unwrap();
    for invalid in [
        "00000000-0000-0000-0000-000000000000",
        "01890f3e-1234-4000-8000-000000000001",
        "01890f3e-1234-7000-0000-000000000001",
    ] {
        assert!(matches!(
            Uuid7::try_from(Uuid::parse_str(invalid).unwrap()),
            Err(DmetaError::InvalidUuidVersion)
        ));
        assert!(serde_json::from_value::<Uuid7>(serde_json::json!(invalid)).is_err());
        for member in ["actors", "facetUuids"] {
            let mut malformed = fixture.clone();
            let map = malformed[member].as_object_mut().unwrap();
            let valid_key = map.keys().next().unwrap().clone();
            let value = map.remove(&valid_key).unwrap();
            assert!(map.insert(invalid.into(), value).is_none());
            assert!(serde_json::from_value::<Dmeta>(malformed).is_err());
        }
        let mut malformed = fixture["facets"]["org.example.note/main"]["signatures"][0].clone();
        malformed["actorUuid"] = serde_json::json!(invalid);
        assert!(serde_json::from_value::<FacetSignature>(malformed).is_err());
    }
}

#[test]
fn deleted_at_is_a_required_list_even_when_empty() {
    let metadata = FacetMetadata {
        updated_at: Vec::new(),
        deleted_at: Vec::new(),
        signatures: Vec::new(),
    };
    let json = serde_json::json!({"updatedAt": [], "deletedAt": [], "signatures": []});
    assert_eq!(serde_json::to_value(&metadata).unwrap(), json);
    assert_eq!(
        serde_json::from_value::<FacetMetadata>(json).unwrap(),
        metadata
    );
    assert_eq!(
        serde_json::from_value::<FacetMetadata>(serde_json::json!({"deletedAt": []})).unwrap(),
        metadata
    );
    for malformed in [
        serde_json::json!({}),
        serde_json::json!({"updatedAt": [], "signatures": []}),
        serde_json::json!({"deletedAt": null}),
        serde_json::json!({"deletedAt": "1970-01-01T00:00:00Z"}),
    ] {
        assert!(serde_json::from_value::<FacetMetadata>(malformed).is_err());
    }
}

#[cfg(feature = "schemars")]
#[test]
fn dmeta_publishes_as_an_ordinary_facet() {
    let declaration = crate::facet::Declaration::of::<Dmeta>().expect("dmeta is lexicon-aligned");
    assert_eq!(declaration.facet.id, DMETA_FACET_TAG);
    assert_eq!(
        Dmeta::SHAPE,
        crate::facet::Shape::Object {
            openness: crate::facet::Openness::Open
        }
    );
    let universe = crate::facet::Universe::collect(vec![declaration]);
    let published = crate::facet::Bundle::new(&universe, Vec::new())
        .lexicons()
        .expect("an aligned declaration publishes");
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].file["id"], DMETA_FACET_TAG);
    assert_eq!(published[0].file["defs"]["main"]["type"], "object");
}

#[test]
fn actor_keys_are_multibase_but_signatures_are_fixed_length_bytes() {
    let key = MultibaseKey::try_from([7; 32].as_slice()).unwrap();
    let text = key.to_string();
    assert!(text.starts_with('z'));
    assert_eq!(text.parse::<MultibaseKey>().unwrap(), key);
    assert_eq!(serde_json::to_value(&key).unwrap(), serde_json::json!(text));
    assert_eq!(
        serde_json::from_str::<MultibaseKey>(&serde_json::to_string(&key).unwrap()).unwrap(),
        key
    );
    // A truncated or over-long spelling is refused rather than decoded into a different key.
    assert!(text[..text.len() - 1].parse::<MultibaseKey>().is_err());
    assert!(format!("{text}1").parse::<MultibaseKey>().is_err());

    let fixture: serde_json::Value = serde_json::from_str(PORTABLE_PROOF).unwrap();
    let json = fixture["facets"]["org.example.note/main"]["signatures"][0]["signature"].clone();
    let signature: SignatureBytes = serde_json::from_value(json.clone()).unwrap();
    assert_eq!(signature.as_bytes().len(), 64);
    assert_eq!(serde_json::to_value(&signature).unwrap(), json);
    let mut unpadded = json.clone();
    unpadded["$bytes"] = serde_json::json!(json["$bytes"].as_str().unwrap().trim_end_matches('='));
    assert_eq!(
        serde_json::from_value::<SignatureBytes>(unpadded).unwrap(),
        signature
    );
    for length in [0, 32, 63, 65] {
        let bytes = vec![9; length];
        assert!(SignatureBytes::try_from(bytes.as_slice()).is_err());
        assert!(
            serde_json::from_value::<SignatureBytes>(
                serde_json::json!({"$bytes": data_encoding::BASE64.encode(&bytes)})
            )
            .is_err()
        );
    }
    for malformed in [
        serde_json::json!(utils_rs::hash::encode_base58_multibase(
            signature.as_bytes()
        )),
        serde_json::json!(signature.as_bytes().as_slice()),
        serde_json::json!({"$bytes": "not base64"}),
        serde_json::json!({"$bytes": json["$bytes"], "extra": true}),
    ] {
        assert!(serde_json::from_value::<SignatureBytes>(malformed).is_err());
    }
}

#[test]
fn carried_bytes_are_the_protocol_object_not_an_array_of_numbers() {
    let bytes = Bytes::from(vec![1, 2, 3, 255]);
    let json = serde_json::to_value(&bytes).unwrap();
    assert_eq!(json, serde_json::json!({"$bytes": "AQID/w=="}));
    assert_eq!(serde_json::from_value::<Bytes>(json).unwrap(), bytes);
    // An array of numbers is what `Vec<u8>` would have produced, and it is not bytes.
    assert!(serde_json::from_value::<Bytes>(serde_json::json!([1, 2, 3, 255])).is_err());
}

#[test]
fn claim_cbor_and_openssl_ed25519_vectors_are_stable() {
    use ciborium::Value as Cbor;

    let value = native_value();
    let native_bytes = data_encoding::HEXLOWER
        .decode(b"da64626b0ba26161da64626b02016162da64626b064200ff")
        .unwrap();
    assert_eq!(encode_native(&value), native_bytes);
    let expected_cid = data_encoding::HEXLOWER
        .decode(b"01551e202d6bcbbbc604391a38ddfd567ce2e780471aa4f3f7bc599d1c6498d7b53eb6c6")
        .unwrap();
    assert_eq!(cid_of(&native_bytes).as_bytes().as_slice(), expected_cid);
    let claim = claim_for(&value);
    let expected_cbor = data_encoding::HEXLOWER
        .decode(
            concat!(
                "87657a616a646b756f72672e6578616d706c652e6e6f74652f6d61696e",
                "5001890f3e1234700080000000000000015001890f3e12347000800000000000000200",
                "582401551e202d6bcbbbc604391a38ddfd567ce2e780471aa4f3f7bc599d1c6498d7b53eb6c680"
            )
            .as_bytes(),
        )
        .unwrap();
    assert_eq!(claim.cbor_fields().unwrap(), expected_cbor);
    assert_eq!(FacetClaim::from_cbor_fields(&expected_cbor).unwrap(), claim);
    let independently_decoded: Cbor = ciborium::from_reader(&expected_cbor[..]).unwrap();
    assert_eq!(
        independently_decoded,
        Cbor::Array(vec![
            Cbor::Text("zajdk".into()),
            Cbor::Text("org.example.note/main".into()),
            Cbor::Bytes(uuid7(1).as_uuid().as_bytes().to_vec()),
            Cbor::Bytes(uuid7(2).as_uuid().as_bytes().to_vec()),
            Cbor::Integer(0.into()),
            Cbor::Bytes(expected_cid),
            Cbor::Array(Vec::new()),
        ])
    );
    let fixture: Dmeta = serde_json::from_str(PORTABLE_PROOF).unwrap();
    let entry = &fixture.facets[&claim.facet_key].signatures[0];
    assert_eq!(
        claim.signing_bytes().unwrap(),
        entry.message.as_ref().unwrap().as_slice()
    );
    assert_eq!(signed_entry(&claim).1.signature, entry.signature);
}

#[test]
fn real_ed25519_vector_rejects_tampering_with_every_signed_field() {
    let claim = claim_for(&native_value());
    let (actor, entry) = signed_entry(&claim);
    let signed = entry.authenticate(&actor, Some(&claim)).unwrap();
    signed.verify().unwrap();
    assert_eq!(signed.signature_id().unwrap().as_bytes().len(), CID_LENGTH);
    let mut altered = Vec::new();
    let mut wrong = claim.clone();
    wrong.origin_document = DocId::new(b"another-document");
    altered.push(wrong);
    let mut wrong = claim.clone();
    wrong.facet_key = facet_key("org.example.note/mainx");
    altered.push(wrong);
    let mut wrong = claim.clone();
    wrong.chosen_uuid = uuid7(3);
    altered.push(wrong);
    let mut wrong = claim.clone();
    wrong.actor_uuid = uuid7(3);
    altered.push(wrong);
    let mut wrong = claim.clone();
    wrong.operation = ClaimOperation::Remove;
    wrong.value_cid = None;
    altered.push(wrong);
    let mut wrong = claim.clone();
    wrong.value_cid = Some(cid_of(&encode_native(&NativeValue::Null)));
    altered.push(wrong);
    let mut wrong = claim;
    wrong.predecessors.push(cid_with(1));
    altered.push(wrong);
    for wrong in altered {
        let forged = SignedFacetClaim {
            claim: wrong.clone(),
            ..signed.clone()
        };
        assert!(matches!(forged.verify(), Err(DmetaError::InvalidSignature)));
        assert!(matches!(
            SignedFacetClaim::from_signature(wrong, actor.pubkey.clone(), entry.signature.clone()),
            Err(DmetaError::InvalidSignature)
        ));
    }
    let mut forged = signed.clone();
    let mut signature = *forged.signature.as_bytes();
    signature[0] ^= 1;
    forged.signature = SignatureBytes::try_from(signature.as_slice()).unwrap();
    assert!(matches!(forged.verify(), Err(DmetaError::InvalidSignature)));
    assert_ne!(
        forged.signature_id().unwrap(),
        signed.signature_id().unwrap()
    );
    let mut forged = signed.clone();
    let other_key = SigningKey::from_bytes(&[8; 32]);
    forged.actor_public_key =
        MultibaseKey::try_from(other_key.verifying_key().as_bytes().as_slice()).unwrap();
    assert!(matches!(forged.verify(), Err(DmetaError::InvalidSignature)));
    assert_ne!(
        forged.signature_id().unwrap(),
        signed.signature_id().unwrap()
    );
}

#[test]
fn an_inherited_historical_witness_does_not_cover_changed_current_content() {
    let value = native_value();
    let changed = NativeValue::Map(BTreeMap::from([
        ("a".into(), NativeValue::Int(2)),
        ("b".into(), NativeValue::Bytes(vec![0, 255])),
    ]));
    let claim = claim_for(&value);
    let (actor, mut entry) = signed_entry(&claim);
    let witness = InheritedWitness {
        origin_doc: claim.origin_document.clone(),
        message: Bytes::from(claim.signing_bytes().unwrap()),
        value: None,
    };
    entry.inherited = Some(witness);
    let signed = entry.authenticate(&actor, None).unwrap();
    assert_eq!(
        signed.value_evidence(FacetValue::Present(&value)),
        Evidence::Present
    );
    entry.inherited.as_mut().unwrap().value = Some(Bytes::from(encode_native(&value)));
    assert_eq!(entry.authenticate(&actor, Some(&claim)).unwrap(), signed);
    assert_eq!(
        signed.value_evidence(FacetValue::Present(&changed)),
        Evidence::Invalid
    );
    assert_eq!(
        fold_checks(SignatureChecks {
            message: Evidence::Present,
            attribution: Evidence::Present,
            value: signed.value_evidence(FacetValue::Present(&changed)),
        }),
        VerificationState::Uncovered
    );
    assert_eq!(
        fold_checks(SignatureChecks {
            message: Evidence::Present,
            attribution: Evidence::Missing,
            value: signed.value_evidence(FacetValue::Present(&value)),
        }),
        VerificationState::Pending
    );
    assert_eq!(
        signed.value_evidence(FacetValue::Missing),
        Evidence::Missing
    );
    let mut forged = entry.clone();
    forged.inherited.as_mut().unwrap().value = Some(Bytes::from(encode_native(&changed)));
    assert!(matches!(
        forged.authenticate(&actor, None),
        Err(DmetaError::WrongContext)
    ));
    let remove = FacetClaim {
        operation: ClaimOperation::Remove,
        value_cid: None,
        ..claim.clone()
    };
    let (_, mut remove_entry) = signed_entry(&remove);
    remove_entry.inherited = Some(InheritedWitness {
        origin_doc: remove.origin_document.clone(),
        message: Bytes::from(remove.signing_bytes().unwrap()),
        value: Some(Bytes::from(encode_native(&value))),
    });
    assert!(matches!(
        remove_entry.authenticate(&actor, None),
        Err(DmetaError::WrongContext)
    ));
    remove_entry.inherited.as_mut().unwrap().value = None;
    let signed_remove = remove_entry.authenticate(&actor, None).unwrap();
    assert_eq!(
        signed_remove.value_evidence(FacetValue::Removed),
        Evidence::Present
    );
    assert_eq!(
        signed_remove.value_evidence(FacetValue::Present(&value)),
        Evidence::Invalid
    );
    assert_eq!(
        signed_remove.value_evidence(FacetValue::Missing),
        Evidence::Missing
    );
    entry.inherited = None;
    assert!(matches!(
        entry.authenticate(&actor, None),
        Err(DmetaError::MissingMessage)
    ));
    assert_eq!(entry.authenticate(&actor, Some(&claim)).unwrap(), signed);
}

#[test]
fn missing_invalid_and_uncovered_evidence_remain_distinct() {
    use Evidence::Invalid;
    use Evidence::Missing;
    use Evidence::Present;
    use VerificationState::Accepted;
    use VerificationState::Pending;
    use VerificationState::Rejected;
    use VerificationState::Uncovered;

    for (checks, expected) in [
        ([Present, Present, Present], Accepted),
        ([Missing, Present, Present], Pending),
        ([Present, Missing, Present], Pending),
        ([Present, Present, Missing], Pending),
        ([Present, Present, Invalid], Uncovered),
        ([Present, Missing, Invalid], Pending),
        ([Invalid, Present, Present], Rejected),
        ([Present, Invalid, Present], Rejected),
        ([Invalid, Missing, Missing], Rejected),
        ([Missing, Invalid, Missing], Rejected),
    ] {
        let [message, attribution, value] = checks;
        assert_eq!(
            fold_checks(SignatureChecks {
                message,
                attribution,
                value
            }),
            expected
        );
    }
}

#[test]
fn a_valid_replacement_can_name_rejected_ancestry_without_accepting_it() {
    let old_value = native_value();
    let old_claim = claim_for(&old_value);
    let (actor, old_entry) = signed_entry(&old_claim);
    let mut forged = old_entry.authenticate(&actor, Some(&old_claim)).unwrap();
    forged.claim.value_cid = Some(cid_of(&encode_native(&NativeValue::Bool(false))));
    assert!(matches!(forged.verify(), Err(DmetaError::InvalidSignature)));
    let rejected = fold_checks(SignatureChecks {
        message: Evidence::Invalid,
        attribution: Evidence::Present,
        value: forged.value_evidence(FacetValue::Present(&old_value)),
    });
    assert_eq!(rejected, VerificationState::Rejected);
    let forged_id = forged.signature_id().unwrap();
    let new_value = NativeValue::Bytes(vec![0, 255, 42]);
    let replacement = FacetClaim {
        predecessors: vec![forged_id.clone()],
        ..claim_for(&new_value)
    };
    let (_, mut entry) = signed_entry(&replacement);
    entry.carry_message(&replacement).unwrap();
    let signed = entry.authenticate(&actor, None).unwrap();
    assert_eq!(signed.claim.predecessors, vec![forged_id]);
    let accepted = fold_checks(SignatureChecks {
        message: Evidence::Present,
        attribution: Evidence::Present,
        value: signed.value_evidence(FacetValue::Present(&new_value)),
    });
    assert_eq!(accepted, VerificationState::Accepted);
    assert_eq!(
        facet_verification([rejected, accepted]),
        VerificationState::Accepted
    );
    assert_eq!(
        facet_verification([accepted, rejected]),
        VerificationState::Accepted
    );
    assert!(matches!(forged.verify(), Err(DmetaError::InvalidSignature)));
}

#[test]
fn a_static_openssl_proof_round_trips_and_verifies_without_document_history() {
    let fixture: Dmeta = serde_json::from_str(PORTABLE_PROOF).unwrap();
    let json = serde_json::to_value(&fixture).unwrap();
    assert_eq!(
        json,
        serde_json::from_str::<serde_json::Value>(PORTABLE_PROOF).unwrap()
    );
    let restored: Dmeta = serde_json::from_value(json).unwrap();
    assert_eq!(restored, fixture);
    let value = native_value();
    let claim = claim_for(&value);
    assert_eq!(restored.facet_uuids[&claim.chosen_uuid], claim.facet_key);
    let entry = &restored.facets[&claim.facet_key].signatures[0];
    let actor = &restored.actors[&entry.actor_uuid];
    assert_eq!(actor.delegation, Some(Bytes::from(vec![1, 2, 3, 255])));
    let signed = entry.authenticate(actor, None).unwrap();
    assert_eq!(signed.claim, claim);
    signed.verify().unwrap();
    let expected_id = data_encoding::HEXLOWER
        .decode(b"01551e20776f47997358fee3b289570cf1a2ef3104a2bbc4c623875c1324bf1fad6b828c")
        .unwrap();
    assert_eq!(
        signed.signature_id().unwrap().as_bytes().as_slice(),
        expected_id
    );
    assert_eq!(
        fold_checks(SignatureChecks {
            message: Evidence::Present,
            attribution: Evidence::Present,
            value: signed.value_evidence(FacetValue::Present(&value)),
        }),
        VerificationState::Accepted
    );
}

#[test]
fn a_stale_signature_authenticates_but_does_not_cover_the_current_native_value() {
    let value = native_value();
    let claim = claim_for(&value);
    let (actor, mut entry) = signed_entry(&claim);
    entry.carry_message(&claim).unwrap();
    let signed = entry.authenticate(&actor, None).unwrap();
    let changed = NativeValue::Map(BTreeMap::from([
        ("a".into(), NativeValue::Uint(1)),
        ("b".into(), NativeValue::Bytes(vec![0, 255])),
    ]));
    for (current, evidence, state) in [
        (
            FacetValue::Present(&value),
            Evidence::Present,
            VerificationState::Accepted,
        ),
        (
            FacetValue::Present(&changed),
            Evidence::Invalid,
            VerificationState::Uncovered,
        ),
        (
            FacetValue::Removed,
            Evidence::Invalid,
            VerificationState::Uncovered,
        ),
        (
            FacetValue::Missing,
            Evidence::Missing,
            VerificationState::Pending,
        ),
    ] {
        signed.verify().unwrap();
        assert_eq!(signed.value_evidence(current), evidence);
        assert_eq!(
            fold_checks(SignatureChecks {
                message: Evidence::Present,
                attribution: Evidence::Present,
                value: signed.value_evidence(current),
            }),
            state
        );
    }
}

utils_rs::table_tests! {
    verification_evidence,
    (missing, carried_message, expected),
    {
        let mut dmeta: Dmeta = serde_json::from_str(PORTABLE_PROOF).unwrap();
        let value = native_value();
        let claim = claim_for(&value);
        let node = SigningKey::from_bytes(&[42; 32]);
        let issuer = MultibaseKey::try_from(node.verifying_key().as_bytes().as_slice()).unwrap();
        let actor = dmeta.actors.get_mut(&claim.actor_uuid).unwrap();
        let attribution = Attribution {
            issuer: issuer.clone(), actor_uuid: claim.actor_uuid,
            pubkey: actor.pubkey.clone(), actor_url: actor.actor_url.clone(),
        };
        let signature = SignatureBytes::try_from(
            node.sign(&attribution.signing_bytes().unwrap()).to_bytes().as_slice(),
        ).unwrap();
        actor.delegation = Some(attribution.certificate(signature).unwrap());
        let mut issuers = BTreeMap::from([(claim.actor_uuid, issuer)]);
        if !carried_message {
            dmeta.facets.get_mut(&claim.facet_key).unwrap().signatures[0].message = None;
        }
        match missing {
            "none" => {}
            "actor" => { dmeta.actors.remove(&claim.actor_uuid).unwrap(); }
            "alias" => { dmeta.facet_uuids.remove(&claim.chosen_uuid).unwrap(); }
            "document" => { dmeta.branch_uuids.remove(&uuid7(9)).unwrap(); }
            "rebound document" => { dmeta.branch_uuids.insert(uuid7(9), DocId::new(b"other")).unwrap(); }
            "certificate" => dmeta.actors.get_mut(&claim.actor_uuid).unwrap().delegation = None,
            "issuer" => issuers.clear(),
            other => panic!("unknown case: {other}"),
        }
        assert_eq!(
            dmeta.verify_facet(
                &claim.facet_key, FacetValue::Present(&value),
                &BTreeMap::new(), &issuers,
            ),
            expected,
        );
    }
}

verification_evidence! {
    intact: ("none", true, VerificationState::Accepted),
    missing_actor_is_invalid: ("actor", true, VerificationState::Rejected),
    missing_alias_is_invalid: ("alias", true, VerificationState::Rejected),
    unbound_doc_uuid_is_invalid: ("document", true, VerificationState::Rejected),
    doc_uuid_bound_elsewhere_is_invalid: ("rebound document", true, VerificationState::Rejected),
    missing_certificate_is_invalid: ("certificate", true, VerificationState::Rejected),
    unknown_external_issuer_is_pending: ("issuer", true, VerificationState::Pending),
    unsupplied_message_is_pending: ("none", false, VerificationState::Pending),
    missing_message_does_not_hide_missing_certificate: ("certificate", false, VerificationState::Rejected),
}

#[test]
fn empty_signature_sets_are_uncovered_and_competitors_do_not_poison_acceptance() {
    use VerificationState::Accepted;
    use VerificationState::Pending;
    use VerificationState::Rejected;
    use VerificationState::Uncovered;

    assert_eq!(facet_verification([]), Uncovered);
    assert_eq!(facet_verification([Uncovered]), Uncovered);
    assert_eq!(facet_verification([Rejected, Uncovered]), Rejected);
    assert_eq!(facet_verification([Pending, Rejected]), Pending);
    assert_eq!(facet_verification([Rejected, Pending]), Pending);
    assert_eq!(facet_verification([Uncovered, Pending]), Pending);
    assert_eq!(
        facet_verification([Accepted, Pending, Rejected, Uncovered]),
        Accepted
    );
    assert_eq!(
        facet_verification([Uncovered, Rejected, Pending, Accepted]),
        Accepted
    );
}

#[test]
fn a_corrupted_other_facet_does_not_poison_a_valid_facet() {
    let value = native_value();
    let claim = claim_for(&value);
    let (actor, mut valid) = signed_entry(&claim);
    valid.carry_message(&claim).unwrap();
    let other_claim = FacetClaim {
        facet_key: facet_key("org.example.note/other"),
        chosen_uuid: uuid7(3),
        ..claim.clone()
    };
    let (_, mut corrupt) = signed_entry(&other_claim);
    corrupt.carry_message(&other_claim).unwrap();
    let mut bytes = *corrupt.signature.as_bytes();
    bytes[0] ^= 1;
    corrupt.signature = SignatureBytes::try_from(bytes.as_slice()).unwrap();
    let mut dmeta = Dmeta::new(Timestamp::from_second(0).unwrap());
    assert!(dmeta.actors.insert(claim.actor_uuid, actor).is_none());
    for (facet_claim, entry) in [(&claim, valid), (&other_claim, corrupt)] {
        dmeta
            .record_facet_alias(facet_claim.chosen_uuid, facet_claim.facet_key.clone())
            .unwrap();
        assert!(
            dmeta
                .facets
                .insert(
                    facet_claim.facet_key.clone(),
                    FacetMetadata {
                        updated_at: Vec::new(),
                        deleted_at: Vec::new(),
                        signatures: vec![entry],
                    }
                )
                .is_none()
        );
    }
    let states: BTreeMap<_, _> = dmeta
        .facets
        .iter()
        .map(|(key, metadata)| {
            let state = facet_verification(metadata.signatures.iter().map(|entry| {
                let actor = &dmeta.actors[&entry.actor_uuid];
                match entry.authenticate(actor, None) {
                    Ok(signed) => fold_checks(SignatureChecks {
                        message: Evidence::Present,
                        attribution: Evidence::Present,
                        value: signed.value_evidence(FacetValue::Present(&value)),
                    }),
                    Err(DmetaError::InvalidSignature) => VerificationState::Rejected,
                    Err(error) => panic!("unexpected authentication failure: {error}"),
                }
            }));
            (key.clone(), state)
        })
        .collect();
    assert_eq!(
        states,
        BTreeMap::from([
            (claim.facet_key, VerificationState::Accepted),
            (other_claim.facet_key, VerificationState::Rejected),
        ])
    );
}

#[test]
fn a_hostile_predecessor_count_is_refused_before_allocation() {
    let claim = FacetClaim {
        operation: ClaimOperation::Remove,
        value_cid: None,
        ..claim_for(&NativeValue::Null)
    };
    let mut fields = claim.cbor_fields().unwrap();
    assert_eq!(fields.pop(), Some(0x80));
    fields.extend([0x9a, 0xff, 0xff, 0xff, 0xff]);
    assert!(matches!(
        FacetClaim::from_cbor_fields(&fields),
        Err(DmetaError::MalformedClaim(
            "predecessor count exceeds input budget"
        ))
    ));
    let mut u64_count = claim.cbor_fields().unwrap();
    assert_eq!(u64_count.pop(), Some(0x80));
    u64_count.extend([0x9b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);
    assert!(FacetClaim::from_cbor_fields(&u64_count).is_err());
}

#[test]
fn the_decoder_rejects_malformed_noncanonical_and_trailing_bytes() {
    use ciborium::Value as Cbor;

    let claim = claim_for(&native_value());
    let fields = claim.cbor_fields().unwrap();
    assert_eq!(FacetClaim::from_cbor_fields(&fields).unwrap(), claim);
    let signing = claim.signing_bytes().unwrap();
    assert_eq!(FacetClaim::from_signing_bytes(&signing).unwrap(), claim);
    assert!(matches!(
        FacetKey::from_str("org.example.note/"),
        Err(ReferenceError::MissingFacetKey)
    ));
    for length in 0..fields.len() {
        assert!(FacetClaim::from_cbor_fields(&fields[..length]).is_err());
    }
    let mut trailing = fields.clone();
    trailing.push(0);
    assert!(FacetClaim::from_cbor_fields(&trailing).is_err());
    let mut noncanonical = fields.clone();
    noncanonical.splice(0..1, [0x98, 7]);
    assert!(FacetClaim::from_cbor_fields(&noncanonical).is_err());
    let mut wrong_arity = fields.clone();
    wrong_arity[0] = 0x86;
    assert!(FacetClaim::from_cbor_fields(&wrong_arity).is_err());
    let mut invalid_utf8 = fields.clone();
    invalid_utf8[2] = 0xff;
    assert!(FacetClaim::from_cbor_fields(&invalid_utf8).is_err());
    let mut old_domain = b"daybook.facet-claim.v1\0".to_vec();
    old_domain.extend(&fields);
    assert!(FacetClaim::from_signing_bytes(&old_domain).is_err());
    assert!(FacetClaim::from_signing_bytes(&fields).is_err());
    let Cbor::Array(decoded) = ciborium::from_reader(&fields[..]).unwrap() else {
        panic!("claim fixture is not an array");
    };
    let reject_field = |index: usize, value: Cbor| {
        let mut malformed = decoded.clone();
        malformed[index] = value;
        let mut bytes = Vec::new();
        ciborium::into_writer(&Cbor::Array(malformed), &mut bytes).unwrap();
        assert!(FacetClaim::from_cbor_fields(&bytes).is_err());
    };
    for index in [2, 3] {
        for invalid in [
            Vec::new(),
            vec![0; 15],
            vec![0; 16],
            vec![0; 17],
            Uuid::parse_str("01890f3e-1234-4000-8000-000000000001")
                .unwrap()
                .as_bytes()
                .to_vec(),
            Uuid::parse_str("01890f3e-1234-7000-0000-000000000001")
                .unwrap()
                .as_bytes()
                .to_vec(),
        ] {
            reject_field(index, Cbor::Bytes(invalid));
        }
        reject_field(index, Cbor::Text(uuid7(1).to_string()));
    }
    reject_field(0, Cbor::Text("not a document ID".into()));
    reject_field(1, Cbor::Text("org.example.note/".into()));
    reject_field(4, Cbor::Integer(2.into()));
    reject_field(4, Cbor::Integer(1.into()));
    reject_field(5, Cbor::Null);
    reject_field(5, Cbor::Bytes(vec![0; 36]));
    reject_field(6, Cbor::Null);
    let mut invalid = claim.clone();
    invalid.value_cid = None;
    assert!(matches!(
        invalid.signing_bytes(),
        Err(DmetaError::InvalidClaim)
    ));
    invalid = claim;
    invalid.operation = ClaimOperation::Remove;
    assert!(matches!(
        invalid.signing_bytes(),
        Err(DmetaError::InvalidClaim)
    ));
}

#[test]
fn an_inherited_claim_binds_the_source_context_not_the_branch() {
    let predecessor_a = cid_with(1);
    let predecessor_b = cid_with(2);
    let value = native_value();
    let source_claim = FacetClaim {
        origin_document: DocId::new(b"origin-a"),
        predecessors: vec![
            predecessor_b.clone(),
            predecessor_a.clone(),
            predecessor_b.clone(),
        ],
        ..claim_for(&value)
    };
    let (actor, mut witness) = signed_entry(&source_claim);
    witness.inherited = Some(InheritedWitness {
        origin_doc: source_claim.origin_document.clone(),
        message: Bytes::from(source_claim.signing_bytes().unwrap()),
        value: Some(Bytes::from(encode_native(&value))),
    });
    let signed = witness.authenticate(&actor, Some(&source_claim)).unwrap();
    // The witness's predecessors were sorted and deduplicated before signing, so the normalized
    // form is the same claim.
    let mut normalized = source_claim.clone();
    normalized.predecessors = vec![predecessor_a.clone(), predecessor_b];
    normalized.predecessors.sort();
    assert_eq!(signed.claim, normalized);
    assert_eq!(
        witness.authenticate(&actor, Some(&normalized)).unwrap(),
        signed
    );
    let mut wrong_frontier = normalized.clone();
    wrong_frontier.predecessors = vec![predecessor_a];
    let mut wrong_facet = source_claim.clone();
    wrong_facet.facet_key = facet_key("org.example.note/wrong");
    let mut wrong_uuid = source_claim.clone();
    wrong_uuid.chosen_uuid = uuid7(10);
    let mut wrong_actor = source_claim.clone();
    wrong_actor.actor_uuid = uuid7(10);
    let mut wrong_origin = source_claim;
    wrong_origin.origin_document = DocId::new(b"branch-b");
    for wrong in [
        wrong_frontier,
        wrong_facet,
        wrong_uuid,
        wrong_actor,
        wrong_origin,
    ] {
        assert!(matches!(
            witness.authenticate(&actor, Some(&wrong)),
            Err(DmetaError::WrongContext)
        ));
    }
    let mut wrong_origin = witness.clone();
    wrong_origin.inherited.as_mut().unwrap().origin_doc = DocId::new(b"branch-b");
    assert!(matches!(
        wrong_origin.authenticate(&actor, None),
        Err(DmetaError::WrongContext)
    ));
    let mut wrong_actor = witness.clone();
    wrong_actor.actor_uuid = uuid7(10);
    assert!(matches!(
        wrong_actor.authenticate(&actor, None),
        Err(DmetaError::WrongContext)
    ));
    let mut double_carrier = witness.clone();
    double_carrier.message = Some(Bytes::from(normalized.signing_bytes().unwrap()));
    assert!(matches!(
        double_carrier.authenticate(&actor, None),
        Err(DmetaError::WrongContext)
    ));
    let before = witness.clone();
    assert!(matches!(
        witness.carry_message(&normalized),
        Err(DmetaError::WrongContext)
    ));
    assert_eq!(witness, before);
}

#[test]
fn portable_messages_bind_actor_and_expected_context() {
    let claim = claim_for(&native_value());
    let (actor, mut entry) = signed_entry(&claim);
    let reconstructed = entry.authenticate(&actor, Some(&claim)).unwrap();
    let mut wrong_actor = claim.clone();
    wrong_actor.actor_uuid = uuid7(3);
    assert!(matches!(
        entry.carry_message(&wrong_actor),
        Err(DmetaError::WrongContext)
    ));
    assert!(entry.message.is_none());
    entry.carry_message(&claim).unwrap();
    assert_eq!(entry.authenticate(&actor, None).unwrap(), reconstructed);
    assert_eq!(
        entry.authenticate(&actor, Some(&claim)).unwrap(),
        reconstructed
    );
    let mut wrong_context = claim.clone();
    wrong_context.facet_key = facet_key("org.example.note/other");
    assert!(matches!(
        entry.authenticate(&actor, Some(&wrong_context)),
        Err(DmetaError::WrongContext)
    ));
    let mut wrong_entry = entry.clone();
    wrong_entry.actor_uuid = uuid7(3);
    assert!(matches!(
        wrong_entry.authenticate(&actor, None),
        Err(DmetaError::WrongContext)
    ));
    let other_key = SigningKey::from_bytes(&[8; 32]);
    let other_actor = ActorMetadata {
        pubkey: MultibaseKey::try_from(other_key.verifying_key().as_bytes().as_slice()).unwrap(),
        ..actor.clone()
    };
    assert!(matches!(
        entry.authenticate(&other_actor, None),
        Err(DmetaError::InvalidSignature)
    ));
    entry.message = Some(Bytes::from(wrong_context.signing_bytes().unwrap()));
    assert!(matches!(
        entry.authenticate(&actor, None),
        Err(DmetaError::InvalidSignature)
    ));
}

#[test]
fn a_facet_key_with_slashes_and_non_utf8_survives_the_claim_and_the_uri() {
    let key = FacetKey::new("org.example.note", b"main/bytes/\x80".to_vec()).unwrap();
    let claim = FacetClaim {
        origin_document: DocId::new(b"doc"),
        facet_key: key.clone(),
        chosen_uuid: uuid7(12),
        actor_uuid: uuid7(2),
        operation: ClaimOperation::Remove,
        value_cid: None,
        predecessors: vec![],
    };
    let encoded = claim.cbor_fields().unwrap();
    assert_eq!(FacetClaim::from_cbor_fields(&encoded).unwrap(), claim);
    assert_eq!(FacetKey::from_str(&key.to_string()).unwrap(), key);
    assert!(key.to_string().contains("\\x80"));

    let reference = FacetReference::new(ReferenceDocId::SelfDoc, key.clone()).unwrap();
    let uri = reference.to_uri_string().unwrap();
    assert!(uri.contains("%5Cx80"));
    assert_eq!(FacetReference::parse(&uri).unwrap().key(), &key);
    assert_eq!(reference.key(), &key);
}

#[test]
fn predecessors_are_sorted_and_deduplicated_before_signing() {
    use ciborium::Value as Cbor;

    let mut predecessors = [cid_with(1), cid_with(2)];
    predecessors.sort();
    let [first, second] = predecessors;
    let claim = FacetClaim {
        operation: ClaimOperation::Remove,
        value_cid: None,
        predecessors: vec![second.clone(), first.clone(), second.clone()],
        ..claim_for(&NativeValue::Null)
    };
    let bytes = claim.cbor_fields().unwrap();
    let normalized = FacetClaim {
        predecessors: vec![first.clone(), second.clone()],
        ..claim.clone()
    };
    assert_eq!(FacetClaim::from_cbor_fields(&bytes).unwrap(), normalized);
    assert_eq!(
        claim.signing_bytes().unwrap(),
        normalized.signing_bytes().unwrap()
    );
    let (actor, entry) = signed_entry(&claim);
    let signed = entry.authenticate(&actor, Some(&claim)).unwrap();
    let normalized_signed = entry.authenticate(&actor, Some(&normalized)).unwrap();
    assert_eq!(
        signed.signature_id().unwrap(),
        normalized_signed.signature_id().unwrap()
    );
    let Cbor::Array(fields) = ciborium::from_reader(&bytes[..]).unwrap() else {
        panic!("claim fixture is not an array");
    };
    for malformed_predecessors in [vec![second, first.clone()], vec![first.clone(), first]] {
        let mut malformed = fields.clone();
        malformed[6] = Cbor::Array(
            malformed_predecessors
                .into_iter()
                .map(|cid| Cbor::Bytes(cid.as_bytes().to_vec()))
                .collect(),
        );
        let mut encoded = Vec::new();
        ciborium::into_writer(&Cbor::Array(malformed), &mut encoded).unwrap();
        assert!(matches!(
            FacetClaim::from_cbor_fields(&encoded),
            Err(DmetaError::MalformedClaim(
                "predecessors must be sorted and unique"
            ))
        ));
    }
}
