use super::*;
use ed25519_dalek::Signer;
use ed25519_dalek::SigningKey;

fn fixture() -> (Attribution, ActorMetadata, SigningKey) {
    let seed = data_encoding::HEXLOWER
        .decode(b"9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")
        .unwrap();
    let signer = SigningKey::from_bytes(&seed.try_into().unwrap());
    let issuer = MultibaseKey::try_from(signer.verifying_key().as_bytes().as_slice()).unwrap();
    let pubkey = MultibaseKey::try_from(
        data_encoding::HEXLOWER
            .decode(b"3d4017c3e843895a92b70aa74d1b7ebc9c982ccf2ec4968cc0cd55f12af4660c")
            .unwrap()
            .as_slice(),
    )
    .unwrap();
    let attribution = Attribution {
        issuer,
        actor_uuid: uuid::Uuid::parse_str("017f22e2-79b0-7cc3-98c4-dc0c0c07398f")
            .unwrap()
            .try_into()
            .unwrap(),
        pubkey: pubkey.clone(),
        actor_url: "db+actor://node/process".into(),
    };
    let actor = ActorMetadata {
        pubkey,
        actor_url: attribution.actor_url.clone(),
        delegation: None,
    };
    (attribution, actor, signer)
}

fn verify(
    certificate: &Bytes,
    attribution: &Attribution,
    actor: &ActorMetadata,
) -> Result<Attribution, AttributionError> {
    Attribution::verify(
        certificate,
        &attribution.issuer,
        attribution.actor_uuid,
        actor,
    )
}

fn signed(attribution: &Attribution, signer: &SigningKey) -> Bytes {
    let signature = signer.sign(&attribution.signing_bytes().unwrap());
    attribution
        .certificate(SignatureBytes::try_from(signature.to_bytes().as_slice()).unwrap())
        .unwrap()
}

fn unpack(certificate: &Bytes) -> Vec<Value> {
    let Value::Tag(61, cose) = ciborium::from_reader(certificate.as_slice()).unwrap() else {
        panic!("CWT tag");
    };
    let Value::Tag(18, envelope) = *cose else {
        panic!("COSE tag");
    };
    let Value::Array(fields) = *envelope else {
        panic!("COSE array");
    };
    fields
}

fn pack(fields: Vec<Value>) -> Bytes {
    encode(&Value::Tag(
        61,
        Box::new(Value::Tag(18, Box::new(Value::Array(fields)))),
    ))
    .unwrap()
    .into()
}

fn modify_map(fields: &mut [Value], index: usize, edit: impl FnOnce(&mut Vec<(Value, Value)>)) {
    let Value::Bytes(bytes) = &fields[index] else {
        panic!("CBOR bytes");
    };
    let Value::Map(mut map) = ciborium::from_reader(bytes.as_slice()).unwrap() else {
        panic!("CBOR map");
    };
    edit(&mut map);
    fields[index] = Value::Bytes(encode(&Value::Map(map)).unwrap());
}

fn replace(map: &mut [(Value, Value)], label: i64, value: Value) {
    map.iter_mut()
        .find(|(key, _)| key == &Value::Integer(label.into()))
        .unwrap()
        .1 = value;
}

fn resign(fields: &mut [Value], signer: &SigningKey) {
    let input = encode(&Value::Array(vec![
        Value::Text("Signature1".into()),
        fields[0].clone(),
        Value::Bytes(Vec::new()),
        fields[2].clone(),
    ]))
    .unwrap();
    fields[3] = Value::Bytes(signer.sign(&input).to_bytes().to_vec());
}

#[test]
fn rfc9052_signature1_literal_fixture() {
    let (attribution, actor, signer) = fixture();
    let expected = data_encoding::HEXLOWER
        .decode(
            concat!(
                "846a5369676e617475726531584ba4013202811003706170706c69636174696f6e2f63626f72",
                "1078306170706c69636174696f6e2f766e642e646179626f6f6b2e6163746f722d6174747269",
                "627574696f6e2e76312b6377744058b4a501782d7a4656656e3358363639784c7a7369364e32",
                "563931446f69797a487a6731754167716954386a5a396e5339365a02782d7a3538365a374832",
                "76705839714e684e3254346539557475676965336f676a62787a47614d744d3345364852353a",
                "0001000050017f22e279b07cc398c4dc0c0c07398f3a000100017764622b6163746f723a2f2f",
                "6e6f64652f70726f636573733a000100027819646179626f6f6b2e6163746f722d6174747269",
                "627574696f6e",
            )
            .as_bytes(),
        )
        .unwrap();
    assert_eq!(attribution.signing_bytes().unwrap(), expected);
    let signature = signer.sign(&expected);
    let certificate = attribution
        .certificate(SignatureBytes::try_from(signature.to_bytes().as_slice()).unwrap())
        .unwrap();
    assert_eq!(verify(&certificate, &attribution, &actor), Ok(attribution));
}

#[test]
fn roundtrip_and_independent_structure() {
    let (attribution, actor, signer) = fixture();
    let certificate = signed(&attribution, &signer);
    assert_eq!(
        verify(&certificate, &attribution, &actor),
        Ok(attribution.clone())
    );
    let fields = unpack(&certificate);
    let Value::Bytes(protected) = &fields[0] else {
        panic!("protected bytes");
    };
    assert_eq!(
        ciborium::from_reader::<Value, _>(protected.as_slice()).unwrap(),
        Value::Map(vec![
            (Value::Integer(1.into()), Value::Integer((-19).into())),
            (
                Value::Integer(2.into()),
                Value::Array(vec![Value::Integer(16.into())]),
            ),
            (
                Value::Integer(3.into()),
                Value::Text("application/cbor".into()),
            ),
            (
                Value::Integer(16.into()),
                Value::Text(PRIVATE_UNREGISTERED_MEDIA_TYPE.into()),
            ),
        ])
    );
    assert_eq!(fields[1], Value::Map(Vec::new()));
    let Value::Bytes(payload) = &fields[2] else {
        panic!("payload bytes");
    };
    assert_eq!(
        ciborium::from_reader::<Value, _>(payload.as_slice()).unwrap(),
        Value::Map(vec![
            (
                Value::Integer(1.into()),
                Value::Text(attribution.issuer.to_string()),
            ),
            (
                Value::Integer(2.into()),
                Value::Text(attribution.pubkey.to_string()),
            ),
            (
                Value::Integer(ACTOR_UUID.into()),
                Value::Bytes(attribution.actor_uuid.as_uuid().as_bytes().to_vec()),
            ),
            (
                Value::Integer(ACTOR_URL.into()),
                Value::Text(attribution.actor_url.clone()),
            ),
            (
                Value::Integer(ATTRIBUTION_PURPOSE.into()),
                Value::Text(PURPOSE.into()),
            ),
        ])
    );
    let Value::Bytes(signature) = &fields[3] else {
        panic!("signature bytes");
    };
    signer
        .verifying_key()
        .verify_strict(
            &attribution.signing_bytes().unwrap(),
            &Signature::from_slice(signature).unwrap(),
        )
        .unwrap();
}

#[test]
fn verifies_received_bytes_without_reserializing_claims_or_headers() {
    let (attribution, actor, signer) = fixture();
    let mut fields = unpack(&signed(&attribution, &signer));
    modify_map(&mut fields, 0, |map| map.reverse());
    modify_map(&mut fields, 2, |map| map.reverse());
    assert_eq!(
        verify(&pack(fields.clone()), &attribution, &actor),
        Err(AttributionError::InvalidSignature)
    );
    resign(&mut fields, &signer);
    assert_eq!(verify(&pack(fields), &attribution, &actor), Ok(attribution));
}

#[test]
fn rejects_tampering_and_invalid_output_signatures() {
    let (attribution, actor, signer) = fixture();
    let certificate = signed(&attribution, &signer);
    for index in [2, 3] {
        let mut fields = unpack(&certificate);
        if index == 2 {
            modify_map(&mut fields, index, |map| {
                replace(map, ACTOR_URL, Value::Text("db+actor://node/other".into()));
            });
        } else {
            let Value::Bytes(signature) = &mut fields[index] else {
                panic!("signature bytes");
            };
            signature[0] ^= 1;
        }
        assert_eq!(
            verify(&pack(fields), &attribution, &actor),
            Err(AttributionError::InvalidSignature)
        );
    }
    let foreign_signature =
        SigningKey::from_bytes(&[9; 32]).sign(&attribution.signing_bytes().unwrap());
    assert_eq!(
        attribution.certificate(
            SignatureBytes::try_from(foreign_signature.to_bytes().as_slice()).unwrap(),
        ),
        Err(AttributionError::InvalidSignature)
    );
}

#[test]
fn checks_issuer_and_each_binding_component_without_consulting_delegation() {
    let (attribution, mut actor, signer) = fixture();
    let certificate = signed(&attribution, &signer);
    let other_uuid = uuid::Uuid::parse_str("017f22e2-79b0-7cc3-98c4-dc0c0c073990")
        .unwrap()
        .try_into()
        .unwrap();
    assert_eq!(
        Attribution::verify(&certificate, &actor.pubkey, attribution.actor_uuid, &actor),
        Err(AttributionError::WrongIssuer)
    );
    for (uuid, pubkey, url) in [
        (other_uuid, actor.pubkey.clone(), actor.actor_url.clone()),
        (
            attribution.actor_uuid,
            attribution.issuer.clone(),
            actor.actor_url.clone(),
        ),
        (
            attribution.actor_uuid,
            actor.pubkey.clone(),
            "db+actor://node/%70rocess".into(),
        ),
    ] {
        let other = ActorMetadata {
            pubkey,
            actor_url: url,
            delegation: None,
        };
        assert_eq!(
            Attribution::verify(&certificate, &attribution.issuer, uuid, &other),
            Err(AttributionError::WrongBinding)
        );
    }
    actor.delegation = Some(vec![0xff].into());
    let copied_certificate = certificate.clone();
    assert_eq!(
        verify(&copied_certificate, &attribution, &actor),
        Ok(attribution.clone())
    );
    let another_binding = Attribution {
        actor_uuid: other_uuid,
        ..attribution
    };
    assert_eq!(
        verify(&signed(&another_binding, &signer), &another_binding, &actor),
        Ok(another_binding)
    );
}

#[test]
fn rejects_other_profiles_and_extensions_even_with_valid_signatures() {
    let (attribution, actor, signer) = fixture();
    let certificate = signed(&attribution, &signer);
    for (index, label, value) in [
        (0, 1, Value::Integer((-8).into())),
        (0, 16, Value::Text("application/other+cwt".into())),
        (0, 3, Value::Text("application/cwt".into())),
        (
            0,
            2,
            Value::Array(vec![Value::Integer(16.into()), Value::Integer(99.into())]),
        ),
        (
            2,
            ATTRIBUTION_PURPOSE,
            Value::Text("daybook.authorize".into()),
        ),
    ] {
        let mut fields = unpack(&certificate);
        modify_map(&mut fields, index, |map| replace(map, label, value));
        resign(&mut fields, &signer);
        assert!(matches!(
            verify(&pack(fields), &attribution, &actor),
            Err(AttributionError::Unsupported(_))
        ));
    }
    for (index, label) in [(0, 99), (2, 3), (2, 4), (2, 5), (2, 6), (2, 9), (2, -65540)] {
        let mut fields = unpack(&certificate);
        modify_map(&mut fields, index, |map| {
            map.push((
                Value::Integer(label.into()),
                Value::Text("extension".into()),
            ));
        });
        resign(&mut fields, &signer);
        assert_eq!(
            verify(&pack(fields), &attribution, &actor),
            Err(AttributionError::Unsupported("extension field"))
        );
    }
    let mut fields = unpack(&certificate);
    fields[1] = Value::Map(vec![(Value::Integer(4.into()), Value::Bytes(vec![1]))]);
    assert_eq!(
        verify(&pack(fields), &attribution, &actor),
        Err(AttributionError::Unsupported("unprotected headers"))
    );
}

#[test]
fn rejects_duplicates_missing_fields_and_trailing_bytes() {
    let (attribution, actor, signer) = fixture();
    let certificate = signed(&attribution, &signer);
    for index in [0, 2] {
        let mut fields = unpack(&certificate);
        modify_map(&mut fields, index, |map| map.push(map[0].clone()));
        assert_eq!(
            verify(&pack(fields), &attribution, &actor),
            Err(AttributionError::Malformed("duplicate map key"))
        );
        let mut fields = unpack(&certificate);
        modify_map(&mut fields, index, |map| {
            map.remove(0);
        });
        assert_eq!(
            verify(&pack(fields), &attribution, &actor),
            Err(AttributionError::Malformed("missing field"))
        );
        let mut fields = unpack(&certificate);
        let Value::Bytes(bytes) = &mut fields[index] else {
            panic!("bytes");
        };
        bytes.push(0);
        assert_eq!(
            verify(&pack(fields), &attribution, &actor),
            Err(AttributionError::Malformed("trailing CBOR"))
        );
    }
    let mut bytes = certificate.as_slice().to_vec();
    bytes.push(0);
    assert_eq!(
        verify(&bytes.into(), &attribution, &actor),
        Err(AttributionError::Malformed("trailing CBOR"))
    );
}

#[test]
fn rejects_malformed_bindings_and_weak_keys() {
    let (attribution, actor, signer) = fixture();
    let certificate = signed(&attribution, &signer);
    let mut wrong_version = attribution.actor_uuid.as_uuid().as_bytes().to_vec();
    wrong_version[6] = (wrong_version[6] & 15) | 0x40;
    let mut wrong_variant = attribution.actor_uuid.as_uuid().as_bytes().to_vec();
    wrong_variant[8] &= 0x3f;
    let mut identity = [0; 32];
    identity[0] = 1;
    let weak_key = MultibaseKey::try_from(identity.as_slice()).unwrap();
    for (label, value) in [
        (ACTOR_UUID, Value::Bytes(vec![0; 15])),
        (ACTOR_UUID, Value::Bytes(wrong_version)),
        (ACTOR_UUID, Value::Bytes(wrong_variant)),
        (ACTOR_URL, Value::Bytes(vec![1])),
        (2, Value::Text(weak_key.to_string())),
        (2, Value::Text(format!(" {}", attribution.pubkey))),
        (
            2,
            Value::Text(format!("z1{}", &attribution.pubkey.to_string()[1..])),
        ),
        (2, Value::Text(format!("did:key:{}", attribution.pubkey))),
    ] {
        let mut fields = unpack(&certificate);
        modify_map(&mut fields, 2, |map| replace(map, label, value));
        resign(&mut fields, &signer);
        assert!(matches!(
            verify(&pack(fields), &attribution, &actor),
            Err(AttributionError::Malformed(_))
        ));
    }
    let weak_subject = Attribution {
        pubkey: weak_key.clone(),
        ..attribution.clone()
    };
    assert!(matches!(
        weak_subject.signing_bytes(),
        Err(AttributionError::Malformed(_))
    ));
    let weak_issuer = Attribution {
        issuer: weak_key,
        ..attribution
    };
    assert!(matches!(
        weak_issuer.signing_bytes(),
        Err(AttributionError::Malformed(_))
    ));
}

#[test]
fn rejects_detached_payloads_malformed_critical_headers_and_non_sign1_tokens() {
    let (attribution, actor, signer) = fixture();
    let certificate = signed(&attribution, &signer);
    for critical in [
        Value::Null,
        Value::Array(vec![]),
        Value::Array(vec![Value::Integer(16.into()); 2]),
    ] {
        let mut fields = unpack(&certificate);
        modify_map(&mut fields, 0, |map| replace(map, 2, critical));
        assert!(matches!(
            verify(&pack(fields), &attribution, &actor),
            Err(AttributionError::Malformed(_))
        ));
    }
    for (index, value) in [
        (0, Value::Map(vec![])),
        (2, Value::Null),
        (3, Value::Bytes(vec![0; 63])),
    ] {
        let mut fields = unpack(&certificate);
        fields[index] = value;
        assert!(matches!(
            verify(&pack(fields), &attribution, &actor),
            Err(AttributionError::Malformed(_))
        ));
    }
    let fields = Value::Array(unpack(&certificate));
    for value in [
        fields.clone(),
        Value::Tag(18, Box::new(fields.clone())),
        Value::Tag(61, Box::new(Value::Tag(98, Box::new(fields)))),
    ] {
        assert!(matches!(
            verify(&encode(&value).unwrap().into(), &attribution, &actor),
            Err(AttributionError::Unsupported(_))
        ));
    }
}

#[test]
fn strict_signatures_and_independent_issuers() {
    let (attribution, actor, signer) = fixture();
    let certificate = signed(&attribution, &signer);
    for offset in [0, 32] {
        let mut fields = unpack(&certificate);
        let Value::Bytes(signature) = &mut fields[3] else {
            panic!("signature bytes");
        };
        if offset == 0 {
            signature[..32].fill(0);
            signature[0] = 1;
        } else {
            signature[32..].copy_from_slice(
                &data_encoding::HEXLOWER
                    .decode(b"edd3f55c1a631258d69cf7a2def9de1400000000000000000000000000000010")
                    .unwrap(),
            );
        }
        assert_eq!(
            verify(&pack(fields), &attribution, &actor),
            Err(AttributionError::InvalidSignature)
        );
    }
    let other_signer = SigningKey::from_bytes(&[8; 32]);
    let other_issuer =
        MultibaseKey::try_from(other_signer.verifying_key().as_bytes().as_slice()).unwrap();
    let other = Attribution {
        issuer: other_issuer.clone(),
        ..attribution.clone()
    };
    let other_certificate = signed(&other, &other_signer);
    assert_eq!(
        verify(&other_certificate, &attribution, &actor),
        Err(AttributionError::WrongIssuer)
    );
    assert_eq!(
        verify(&other_certificate, &other, &actor),
        Ok(other.clone())
    );
    assert_eq!(
        verify(&certificate, &other, &actor),
        Err(AttributionError::WrongIssuer)
    );
}

#[test]
fn attribution_cannot_redelegate_issuer_trust() {
    let (mut attribution, mut actor, signer) = fixture();
    let actor_signer = SigningKey::from_bytes(&[8; 32]);
    let actor_key =
        MultibaseKey::try_from(actor_signer.verifying_key().as_bytes().as_slice()).unwrap();
    attribution.pubkey = actor_key.clone();
    actor.pubkey = actor_key.clone();
    let certificate = signed(&attribution, &signer);
    let copied_actor = ActorMetadata {
        delegation: Some(certificate.clone()),
        ..actor
    };
    assert_eq!(
        verify(&certificate, &attribution, &copied_actor),
        Ok(attribution.clone())
    );
    let descendant = Attribution {
        issuer: actor_key,
        ..attribution.clone()
    };
    assert_eq!(
        verify(
            &signed(&descendant, &actor_signer),
            &attribution,
            &copied_actor
        ),
        Err(AttributionError::WrongIssuer)
    );
}

#[test]
fn bounds_outer_and_embedded_cbor_before_decoding() {
    let (attribution, actor, signer) = fixture();
    let certificate = signed(&attribution, &signer);
    let hostile = [
        vec![0; MAX_CERTIFICATE_BYTES + 1],
        vec![0x5b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
        vec![0x9b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
        vec![0xbb, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
        [vec![0x81; MAX_CBOR_DEPTH + 1], vec![0]].concat(),
        [vec![0xc0; MAX_CBOR_DEPTH + 1], vec![0]].concat(),
        [vec![0x90], [vec![0x90], vec![0; 16]].concat().repeat(16)].concat(),
    ];
    for bytes in hostile {
        assert_eq!(
            verify(&bytes.clone().into(), &attribution, &actor),
            Err(AttributionError::ResourceLimit)
        );
        if bytes.len() <= MAX_CERTIFICATE_BYTES {
            for index in [0, 2] {
                let mut fields = unpack(&certificate);
                fields[index] = Value::Bytes(bytes.clone());
                assert_eq!(
                    verify(&pack(fields), &attribution, &actor),
                    Err(AttributionError::ResourceLimit)
                );
            }
        }
    }
    let too_long = Attribution {
        actor_url: "x".repeat(MAX_ACTOR_URL_BYTES + 1),
        ..attribution.clone()
    };
    assert_eq!(
        too_long.signing_bytes(),
        Err(AttributionError::ResourceLimit)
    );
    let mut fields = unpack(&certificate);
    modify_map(&mut fields, 2, |map| {
        replace(map, ACTOR_URL, Value::Text(too_long.actor_url));
    });
    assert_eq!(
        verify(&pack(fields), &attribution, &actor),
        Err(AttributionError::ResourceLimit)
    );
    for length in 0..certificate.as_slice().len() {
        let truncated = certificate.as_slice()[..length].to_vec().into();
        assert!(verify(&truncated, &attribution, &actor).is_err());
    }
    let maximum_url = Attribution {
        actor_url: "x".repeat(MAX_ACTOR_URL_BYTES),
        ..attribution
    };
    let maximum_actor = ActorMetadata {
        actor_url: maximum_url.actor_url.clone(),
        ..actor
    };
    assert_eq!(
        verify(&signed(&maximum_url, &signer), &maximum_url, &maximum_actor),
        Ok(maximum_url)
    );
}
