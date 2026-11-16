use crate::*;
use big_cabinet_core::datum::Bundle;
use big_cabinet_core::datum::CompiledSchema;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::datum::Stored;
use big_cabinet_core::datum::hydrate_value;
use big_cabinet_core::datum::reconcile_json_atomic;
use big_sync_core::ObjKey;
use method_core::Method;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use serde_json::json;
use std::collections::BTreeSet;
use std::collections::HashSet;

#[test]
fn digest_spellings_name_the_same_bytes_and_object() {
    for bytes in [
        [0; 32],
        [1; 32],
        [255; 32],
        std::array::from_fn(|index| index as u8),
    ] {
        let blob_id = BlobId::new(bytes);
        let bare = format!("z{}", bs58::encode(bytes).into_string());
        let mut framed = vec![0x1e, 0x20];
        framed.extend_from_slice(&bytes);
        let digest = format!("z{}", bs58::encode(framed).into_string());
        assert_eq!(blob_id.to_bytes32(), bytes);
        assert_eq!(blob_id.to_string(), bare);
        assert_eq!(blob_id.digest_str(), digest);
        assert_eq!(bare.parse::<BlobId>().unwrap(), blob_id);
        assert_eq!(digest.parse::<BlobId>().unwrap(), blob_id);
        let key = ObjKey::from(blob_id);
        assert_eq!(key.as_bytes(), bytes);
        assert_eq!(BlobId::try_from(&key).unwrap(), blob_id);
        assert_eq!(HashSet::from([blob_id, digest.parse().unwrap()]).len(), 1);
        assert_eq!(BTreeSet::from([blob_id, bare.parse().unwrap()]).len(), 1);
    }
    assert!(BlobId::new([1; 32]) < BlobId::new([2; 32]));
}

#[test]
fn digest_parsing_rejects_external_garbage_without_panicking() {
    for input in [
        "",
        "z",
        "é",
        "中文",
        "zzé",
        "z0",
        "z ",
        "/seds",
        "../escape",
        "/var/blob",
        "o:anything",
        "not-a-digest",
        "babcdefghijklmnopqrstuvwxyz234567",
    ] {
        assert!(input.parse::<BlobId>().is_err(), "{input:?}");
    }
    for length in [0, 1, 16, 31, 33, 34, 35, 64, 1024] {
        let bytes = vec![0xff; length];
        let input = format!("z{}", bs58::encode(&bytes).into_string());
        assert!(input.parse::<BlobId>().is_err(), "width {length}");
        assert!(BlobId::try_from(&ObjKey::new(bytes)).is_err());
    }
    for header in [[0x12, 0x20], [0x1e, 0x1f], [0x1e, 0x21], [0, 0x20]] {
        let mut bytes = header.to_vec();
        bytes.extend_from_slice(&[7; 32]);
        let input = format!("z{}", bs58::encode(bytes).into_string());
        assert!(input.parse::<BlobId>().is_err(), "{header:?}");
    }
    let short_multihash = format!("z{}", bs58::encode([0x1e; 33]).into_string());
    assert!(short_multihash.parse::<BlobId>().is_err());
    assert!(
        format!("z{}", "1".repeat(100_000))
            .parse::<BlobId>()
            .is_err()
    );
}

#[test]
fn digest_json_and_postcard_have_distinct_wire_forms() {
    let blob_id = BlobId::new([7; 32]);
    assert_eq!(
        serde_json::to_value(blob_id).unwrap(),
        json!(blob_id.to_string())
    );
    for text in [blob_id.to_string(), blob_id.digest_str()] {
        assert_eq!(
            serde_json::from_value::<BlobId>(json!(text)).unwrap(),
            blob_id
        );
    }
    let encoded = postcard::to_allocvec(&blob_id).unwrap();
    let mut expected = vec![32];
    expected.extend_from_slice(&blob_id.to_bytes32());
    assert_eq!(encoded, expected);
    assert_eq!(postcard::from_bytes::<BlobId>(&encoded).unwrap(), blob_id);
    for length in [0, 1, 31, 33, 64] {
        let encoded = postcard::to_allocvec(&vec![7u8; length]).unwrap();
        assert!(postcard::from_bytes::<BlobId>(&encoded).is_err());
    }
    assert!(postcard::from_bytes::<BlobId>(&[32, 7]).is_err());
    assert!(serde_json::from_value::<BlobId>(json!("é")).is_err());
    assert!(serde_json::from_value::<BlobId>(json!(vec![7; 32])).is_err());
}

fn admitted_schema(id: &str) -> CompiledSchema {
    let bundle = Bundle::new(&UNIVERSE, Vec::new());
    bundle.validate().unwrap();
    let lexicons = bundle.lexicons().unwrap();
    let lexicon = lexicons.iter().find(|lexicon| lexicon.id == id).unwrap();
    let carried: Vec<&Value> = lexicons
        .iter()
        .filter(|other| other.id != id)
        .map(|other| &other.file)
        .collect();
    bundle
        .validate_lexicon(id, &lexicon.file, &carried)
        .unwrap_or_else(|error| panic!("{id}: {error}"))
}

#[test]
fn datums_declare_publish_and_admit_under_existing_identities() {
    let universe = declarations();
    assert_eq!(
        universe
            .datums
            .iter()
            .map(|datum| datum.id.as_str())
            .collect::<Vec<_>>(),
        [
            "org.example.daybook.blob",
            "org.example.daybook.blobPin",
            "org.example.daybook.cipherBlob",
            "org.example.daybook.jwk",
        ]
    );
    for name in ["Representation", "Rfc8188EncodingParameters"] {
        assert!(
            universe
                .types
                .iter()
                .any(|declaration| declaration.name == name),
            "{name} is declared"
        );
    }
    let bundle = Bundle::new(&universe, Vec::new());
    let lexicons = bundle.lexicons().unwrap();
    let cipher = &lexicons
        .iter()
        .find(|lexicon| lexicon.id == CipherBlob::ID)
        .unwrap()
        .file;
    assert_eq!(
        cipher["defs"]["main"]["properties"]["contentEncoding"]["type"],
        "string"
    );
    assert_eq!(
        cipher["defs"]["main"]["properties"]["keyRef"]["type"],
        "string"
    );
    assert_eq!(
        cipher["defs"]["main"]["properties"]["encodingParameters"],
        json!({"type": "ref", "ref": "#Rfc8188EncodingParameters"})
    );
    let parameters = &cipher["defs"]["Rfc8188EncodingParameters"];
    assert_eq!(parameters["type"], "object");
    assert_eq!(parameters["properties"]["recordSize"]["type"], "integer");
    assert_eq!(parameters["properties"]["padding"]["type"], "string");
    assert_eq!(parameters["required"], json!(["recordSize", "padding"]));
    assert!(parameters["properties"].get("salt").is_none());
    let jwk = &lexicons
        .iter()
        .find(|lexicon| lexicon.id == Jwk::ID)
        .unwrap()
        .file;
    assert_eq!(jwk["defs"]["main"]["required"], json!(["kty"]));
    for datum in universe.datums {
        admitted_schema(&datum.id);
    }
}

fn codec_round_trip<T>(value: Value)
where
    T: DatumMember + Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let original = serde_json::to_vec(&value).unwrap();
    let typed: T = serde_json::from_value(value.clone()).unwrap();
    let encoded = serde_json::to_value(&typed).unwrap();
    assert_eq!(serde_json::to_vec(&encoded).unwrap(), original);
    let schema = admitted_schema(T::ID);
    Stored::check(&schema, value.clone()).unwrap();
    let mut doc = automerge::Automerge::new();
    let transaction = doc.transaction();
    let transaction =
        reconcile_json_atomic(transaction, &schema, &automerge::ROOT, "blob".into(), value)
            .unwrap();
    transaction.commit();
    let saved = doc.save();
    let restored = automerge::Automerge::load(&saved).unwrap();
    let hydrated = hydrate_value(&restored, &automerge::ROOT, "blob".into(), &schema).unwrap();
    assert_eq!(serde_json::to_vec(&hydrated).unwrap(), original);
    assert_eq!(serde_json::from_value::<T>(hydrated).unwrap(), typed);
}

#[test]
fn blob_known_and_future_fields_survive_the_full_codec_chain() {
    codec_round_trip::<Blob>(json!({
        "mime": "application/octet-stream",
        "lengthOctets": 4,
        "digest": BlobId::new([3; 32]).digest_str(),
        "inline": [0, 1, 2, 255],
        "urls": ["dtxt:blob:zContent", "https://example.invalid/unrecognized"],
        "futureField": {"entries": [null, true, "untouched"], "fraction": "1.25"}
    }));
    codec_round_trip::<Blob>(json!({
        "mime": "image/png", "lengthOctets": 123456,
        "digest": "future-algorithm-digest", "inline": null, "urls": null
    }));
    let blob: Blob = serde_json::from_value(json!({
        "mime": "image/png", "lengthOctets": 0, "digest": "digest"
    }))
    .unwrap();
    assert_eq!((blob.inline, blob.urls), (None, None));
}

#[test]
fn blob_pin_known_and_future_fields_survive_the_full_codec_chain() {
    codec_round_trip::<BlobPin>(json!({"lengthOctets": 12345}));
    codec_round_trip::<BlobPin>(json!({
        "lengthOctets": 67890,
        "futureRetention": {"kind": "forever", "parameters": [1, null, 2]}
    }));
}

fn cipher_value() -> Value {
    json!({
        "representation": {"digest": BlobId::new([9; 32]).digest_str(), "lengthOctets": 125337},
        "contentEncoding": "aes128gcm",
        "keyRef": "dtxt:datum:zKeyDocument/org.example.daybook.jwk/relay",
        "keyRefHeads": ["a".repeat(64), "b".repeat(64)],
        "encodingParameters": {"recordSize": 65536, "padding": "padme"}
    })
}

#[test]
fn cipher_blob_preserves_uri_heads_and_future_codec_without_interpreting_them() {
    {
        let padding = "padme";
        let mut value = cipher_value();
        value["encodingParameters"]["padding"] = json!(padding);
        codec_round_trip::<CipherBlob>(value);
    }
    let mut future = cipher_value();
    future["representation"]["futureLocation"] = json!({"shards": ["first", "second"]});
    future["contentEncoding"] = json!("future-encoding");
    future["keyRef"] = json!("urn:example:opaque-key-reference");
    future["encodingParameters"] = json!({
        "recordSize": 32768,
        "padding": "future-padding",
        "futureShape": ["future-value", {"threshold": "1.25"}, null]
    });
    future["futureCipherField"] = json!({"mode": "unknown"});
    codec_round_trip::<CipherBlob>(future);
}

#[test]
fn encoding_parameters_are_typed_open_and_require_no_persisted_salt() {
    let value = json!({
        "recordSize": 65536,
        "padding": "padme",
        "futureField": {"entries": [1, null, true, "1.25"]}
    });
    for padding in ["padme", "future-padding"] {
        let mut value = value.clone();
        value["padding"] = json!(padding);
        let parameters: Rfc8188EncodingParameters = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(
            parameters,
            Rfc8188EncodingParameters {
                record_size: 65536,
                padding: padding.into(),
                extra: [("futureField".into(), value["futureField"].clone())].into(),
            }
        );
        assert_eq!(serde_json::to_value(&parameters).unwrap(), value);
        let mut cipher = cipher_value();
        cipher["encodingParameters"] = value;
        let typed: CipherBlob = serde_json::from_value(cipher.clone()).unwrap();
        assert_eq!(typed.encoding_parameters, parameters);
        assert_eq!(serde_json::to_value(typed).unwrap(), cipher);
    }
}

fn malformed_encoding_parameters() -> [Value; 11] {
    [
        json!(null),
        json!([]),
        json!("opaque"),
        json!({}),
        json!({"recordSize": 65536}),
        json!({"padding": "legacy"}),
        json!({"recordSize": "65536", "padding": "legacy"}),
        json!({"recordSize": -1, "padding": "legacy"}),
        json!({"recordSize": 65536.5, "padding": "legacy"}),
        json!({"recordSize": 65536, "padding": false}),
        json!({"recordSize": 65536, "padding": null}),
    ]
}

#[test]
fn encoding_parameters_reject_malformed_known_fields_in_serde() {
    for parameters in malformed_encoding_parameters() {
        assert!(
            serde_json::from_value::<Rfc8188EncodingParameters>(parameters.clone()).is_err(),
            "{parameters}"
        );
        let mut cipher = cipher_value();
        cipher["encodingParameters"] = parameters;
        assert!(serde_json::from_value::<CipherBlob>(cipher).is_err());
    }
}

#[test]
fn encoding_parameters_reject_malformed_known_fields_in_admission() {
    let schema = admitted_schema(CipherBlob::ID);
    for parameters in malformed_encoding_parameters() {
        let mut cipher = cipher_value();
        cipher["encodingParameters"] = parameters;
        assert!(Stored::check(&schema, cipher.clone()).is_err(), "{cipher}");
    }
}

#[test]
fn jwk_requires_kty_preserves_foreign_members_and_redacts_debug() {
    codec_round_trip::<Jwk>(json!({"kty": "oct", "k": "secret-base64url"}));
    codec_round_trip::<Jwk>(json!({
        "kty": "future-key-type", "k": "secret-base64url",
        "futureExtension": {"secret": [1, 2, 3], "enabled": true}
    }));
    assert!(serde_json::from_value::<Jwk>(json!({"k": "secret"})).is_err());
    let jwk: Jwk = serde_json::from_value(json!({"kty": "secret-kty", "k": "secret"})).unwrap();
    assert_eq!(format!("{jwk:?}"), "Jwk { .. }");
    assert_eq!(format!("{jwk:#?}"), "Jwk { .. }");
}

#[test]
fn admitted_schemas_reject_wrong_known_field_shapes() {
    for (tag, value) in [
        (
            Blob::ID,
            json!({"mime": "image/png", "lengthOctets": "4", "digest": "digest"}),
        ),
        (BlobPin::ID, json!({"lengthOctets": -1})),
        (
            CipherBlob::ID,
            json!({"representation": [], "contentEncoding": "future"}),
        ),
        (Jwk::ID, json!({"k": "secret"})),
    ] {
        let schema = admitted_schema(tag);
        assert!(Stored::check(&schema, value).is_err(), "{tag}");
    }
}

/// A value carrying the `$bytes` object, which only a self-describing codec reads back: its
/// deserializer reads a JSON value, so a non-self-describing codec such as postcard cannot.
fn protocol_json<T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug>(value: T) {
    assert_eq!(
        serde_json::from_value::<T>(serde_json::to_value(&value).unwrap()).unwrap(),
        value
    );
}

fn protocol_wire<T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug>(value: T) {
    assert_eq!(
        serde_json::from_value::<T>(serde_json::to_value(&value).unwrap()).unwrap(),
        value
    );
    assert_eq!(
        postcard::from_bytes::<T>(&postcard::to_allocvec(&value).unwrap()).unwrap(),
        value
    );
}

#[test]
fn protocol_operations_and_channels_match_the_bounded_unary_contract() {
    fn unary<P: method_core::Unary, Proto: irpc::Service>()
    where
        service_core_rs::Envelope<P>: irpc::Channels<Proto, Tx = service_core_rs::Reply<P>>,
    {
    }
    unary::<MintBlobsCapability, BlobsProtocol>();
    unary::<PutBlob, BlobsProtocol>();
    unary::<ReadBlob, BlobsProtocol>();
    unary::<StatBlob, BlobsProtocol>();
    let _: Option<<PutBlob as method_core::Method>::Output> = None::<BlobInfo>;
    assert_eq!(
        BlobsProtocol::METHODS.unary,
        [
            MintBlobsCapability::XRPC,
            PutBlob::XRPC,
            ReadBlob::XRPC,
            StatBlob::XRPC
        ]
    );
    assert!(BlobsProtocol::METHODS.stream.is_empty());
    for (operation, nsid, output) in [
        (
            MintBlobsCapability::XRPC,
            "org.example.daybook.blobs.mintCapability",
            "Capability",
        ),
        (PutBlob::XRPC, "org.example.daybook.blobs.put", "BlobInfo"),
        (ReadBlob::XRPC, "org.example.daybook.blobs.read", "ByteBody"),
        (StatBlob::XRPC, "org.example.daybook.blobs.stat", "BlobStat"),
    ] {
        assert!(big_cabinet_core::datum::is_nsid(operation.nsid));
        assert_eq!(operation.nsid, nsid);
        assert_eq!(operation.output_schema, output);
    }
    // ADR 042 §1: the bodies of `put` and `read` are bytes, their parameters JSON; `put` answers
    // JSON, and the other methods are unchanged.
    assert_eq!(
        PutBlob::XRPC.input_encoding,
        method_core::Encoding::Bytes { mime: None }
    );
    assert_eq!(PutBlob::XRPC.output_encoding, method_core::Encoding::Json);
    assert_eq!(PutBlob::XRPC.input, method_core::HttpInput::Query);
    assert_eq!(ReadBlob::XRPC.kind, method_core::MethodKind::Query);
    assert_eq!(
        ReadBlob::XRPC.output_encoding,
        method_core::Encoding::Bytes { mime: None }
    );
    for unchanged in [MintBlobsCapability::XRPC, StatBlob::XRPC] {
        assert_eq!(unchanged.input_encoding, method_core::Encoding::Json);
        assert_eq!(unchanged.output_encoding, method_core::Encoding::Json);
        assert!(!unchanged.local_only);
    }
    // A put streams, so no 8 MiB message cap bounds it.
    assert_eq!(MAX_PUT_OCTETS, 4 * 1024 * 1024 * 1024);
}

#[test]
fn protocol_requests_accept_capability_ids_not_resolved_records() {
    use big_cabinet_core::authority::CapId;

    fn check<T: Serialize + DeserializeOwned>(request: T) {
        let mut value = serde_json::to_value(request).unwrap();
        assert_eq!(
            value["capability"],
            json!("01010101-0101-0101-0101-010101010101")
        );
        value["capability"] = json!({
            "capabilityId": value["capability"],
            "object": "opaque-native-policy-object",
            "rights": ["use"],
            "issuerId": "018f47d2-e8e6-7c08-a14a-6f71ce53f001"
        });
        assert!(serde_json::from_value::<T>(value).is_err());
    }

    let capability = CapId::from_bytes([1; 16]);
    let blob_id = BlobId::new([4; 32]);
    check(PutBlob { capability });
    check(ReadBlob {
        capability,
        blob_id,
    });
    check(StatBlob {
        capability,
        blob_id,
    });
}

#[test]
fn protocol_requests_results_and_errors_round_trip() {
    use big_cabinet_core::authority::CapId;
    let capability = CapId::from_bytes([1; 16]);
    let blob_id = BlobId::new([4; 32]);
    // The body is not a field of the request: a put's parameters are its capability alone.
    assert_eq!(
        serde_json::to_value(PutBlob { capability }).unwrap(),
        json!({ "capability": capability })
    );
    protocol_wire(PutBlob { capability });
    let scope = BlobsScope {
        put: true,
        read: BTreeSet::from([blob_id]),
    };
    assert_eq!(
        serde_json::to_value(&scope).unwrap(),
        json!({"put": true, "read": [blob_id.to_string()]})
    );
    protocol_json(scope.clone());
    assert!(scope.covers(&BlobsScope {
        put: false,
        read: BTreeSet::from([blob_id]),
    }));
    assert!(!scope.covers(&BlobsScope {
        put: false,
        read: BTreeSet::from([BlobId::new([5; 32])]),
    }));
    assert!(!BlobsScope::default().covers(&scope));
    protocol_json(MintBlobsCapability {
        scope,
        rights: big_cabinet_core::authority::Rights::USE,
        subject: None,
    });
    let read = ReadBlob {
        capability,
        blob_id,
    };
    let value = serde_json::to_value(&read).unwrap();
    assert_eq!(value["blobId"], json!(blob_id.to_string()));
    protocol_wire(read);
    protocol_wire(StatBlob {
        capability,
        blob_id,
    });
    let info = BlobInfo {
        blob_id,
        length_octets: u64::MAX,
    };
    assert_eq!(
        serde_json::to_value(&info).unwrap(),
        json!({"blobId": blob_id.to_string(), "lengthOctets": u64::MAX})
    );
    // An absent `info` is elided from the JSON body (atproto has no null), which a
    // non-self-describing codec cannot read back, so the result is checked as JSON only.
    let stat = BlobStat { info: None };
    assert_eq!(serde_json::to_value(&stat).unwrap(), json!({}));
    assert_eq!(serde_json::from_value::<BlobStat>(json!({})).unwrap(), stat);
    protocol_wire(info.clone());
    let stat = BlobStat { info: Some(info) };
    assert_eq!(
        serde_json::from_value::<BlobStat>(serde_json::to_value(&stat).unwrap()).unwrap(),
        stat
    );
    protocol_wire(Option::<BlobInfo>::None);
    protocol_wire(Some(BlobInfo {
        blob_id,
        length_octets: 0,
    }));
    for error in [
        BlobsError::Context(method_core::ContextError::Cancelled),
        BlobsError::Denied,
        BlobsError::NotFound,
        BlobsError::InvalidInput {
            reason: "range exceeds bounds".into(),
        },
        BlobsError::Storage {
            reason: "store unavailable".into(),
        },
        BlobsError::TooLarge {
            limit_octets: u64::MAX,
        },
        BlobsError::RangeNotSatisfiable { total_octets: 18 },
        BlobsError::Unreadable {
            reason: "reset by peer".into(),
        },
    ] {
        protocol_wire(error);
    }
}
