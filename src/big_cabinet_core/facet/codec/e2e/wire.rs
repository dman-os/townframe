//! Union, unknown-data, wire spelling, and resource-bound behavior.

use super::*;

#[macros::facet(id = "org.example.bytesMember", closed)]
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BytesMember {
    data: Bytes,
}

#[macros::facet(id = "org.example.textMember", closed)]
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TextMember {
    data: Text,
}

#[macros::facet]
#[derive(Debug)]
#[serde(tag = "kind", content = "payload")]
enum StorageUnion {
    Bytes(BytesMember),
    Text(TextMember),
    #[facet(other)]
    Unknown(crate::facet::UnknownMember),
}

#[macros::facet]
#[derive(Debug)]
#[serde(tag = "kind", content = "payload")]
enum ReversedStorageUnion {
    Text(TextMember),
    Bytes(BytesMember),
    #[facet(other)]
    Unknown(crate::facet::UnknownMember),
}

#[macros::facet(closed)]
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "payload")]
enum ClosedStorageUnion {
    #[serde(rename = "org.example.bytesMember")]
    Bytes(BytesMember),
    #[serde(rename = "org.example.textMember")]
    Text(TextMember),
}

#[test]
fn tagged_union_codecs_follow_the_discriminant_not_branch_order() {
    let mut documents: Vec<Value> = [
        schemars::schema_for!(StorageUnion),
        schemars::schema_for!(ReversedStorageUnion),
        schemars::schema_for!(ClosedStorageUnion),
    ]
    .into_iter()
    .map(|schema| serde_json::to_value(schema).unwrap())
    .collect();
    let mut referenced = documents[0].clone();
    referenced["$defs"]["BytesBranch"] = referenced["oneOf"][0].clone();
    referenced["$defs"]["BytesBranchAlias"] = json!({"$ref": "#/$defs/BytesBranch"});
    referenced["oneOf"][0] = json!({"$ref": "#/$defs/BytesBranchAlias"});
    // Fallback position is not dispatch priority, and branch references cannot hide a tag.
    referenced["oneOf"].as_array_mut().unwrap().reverse();
    documents.push(referenced);
    for document in documents {
        let validator = jsonschema::validator_for(&document).unwrap();
        for (json, expected) in [
            (
                json!({"kind": "org.example.bytesMember", "payload": {"data": {"$bytes": "AQID"}}}),
                "bytes",
            ),
            (
                json!({"kind": "org.example.textMember", "payload": {"data": "hello"}}),
                "text",
            ),
        ] {
            let mut doc = AutoCommit::new();
            assert!(validator.is_valid(&json), "{json}");
            let checked = Stored::check(&compiled(&document), json.clone()).unwrap();
            reconcile_json(&mut doc, &ROOT, facet(), &document, checked.into_value()).unwrap();
            assert_eq!(
                hydrate_value(&doc, &ROOT, facet(), &compiled(&document)).unwrap(),
                json
            );
            let (_, member) = doc.get(ROOT, "facet").unwrap().unwrap();
            let (_, payload) = doc.get(member, "payload").unwrap().unwrap();
            let (native, _) = doc.get(payload, "data").unwrap().unwrap();
            assert_eq!(shape(&native), expected);
        }
    }
}

#[test]
fn tagged_union_unknown_members_and_optional_null_keep_their_json() {
    let schemas = [
        schemars::schema_for!(Option<StorageUnion>),
        schemars::schema_for!(Option<ReversedStorageUnion>),
    ];
    for schema in schemas {
        let document = serde_json::to_value(schema).unwrap();
        for json in [
            Value::Null,
            json!({"kind": "org.example.future", "payload": {"data": "not base64", "nested": [null, 1]}, "extra": true}),
            json!({"kind": "org.example.textMember", "payload": {"data": "hello"}}),
        ] {
            let mut doc = AutoCommit::new();
            let checked = Stored::check(&compiled(&document), json.clone())
                .unwrap_or_else(|err| panic!("{err}; value={json}; schema={document}"));
            reconcile_json(&mut doc, &ROOT, facet(), &document, checked.into_value()).unwrap();
            assert_eq!(
                hydrate_value(&doc, &ROOT, facet(), &compiled(&document)).unwrap(),
                json
            );
        }
    }
}

#[test]
fn malformed_tagged_union_members_do_not_use_another_members_codec() {
    for schema in [
        schemars::schema_for!(StorageUnion),
        schemars::schema_for!(ReversedStorageUnion),
    ] {
        let document = serde_json::to_value(schema).unwrap();
        let validator = jsonschema::validator_for(&document).unwrap();
        let malformed = json!({"kind": "org.example.bytesMember", "payload": {"data": {"$bytes": "not base64"}}});
        // contentEncoding is an annotation to the validator; the codec must still refuse this.
        assert!(validator.is_valid(&malformed));
        assert!(Stored::check(&compiled(&document), malformed).is_err());
        for malformed in [
            json!({"kind": "org.example.bytesMember", "payload": {}}),
            json!({"kind": "org.example.textMember", "payload": 1}),
            json!({"kind": 1, "payload": {"data": "hello"}}),
            json!({"payload": {"data": "hello"}}),
        ] {
            assert!(!validator.is_valid(&malformed), "{malformed}");
        }
        for malformed in [
            json!({"kind": 1, "payload": {"data": "hello"}}),
            json!({"payload": {"data": "hello"}}),
        ] {
            assert!(Stored::check(&compiled(&document), malformed).is_err());
        }
    }
}

#[test]
fn canonical_bytes_use_the_protocol_object_and_reject_plain_base64() {
    let bytes = Bytes(vec![1, 2, 3]);
    assert_eq!(
        serde_json::to_value(&bytes).unwrap(),
        json!({"$bytes": "AQID"})
    );
    assert_eq!(
        serde_json::from_value::<Bytes>(json!({"$bytes": "AQID"})).unwrap(),
        bytes
    );
    assert!(serde_json::from_value::<Bytes>(json!("AQID")).is_err());
    assert!(
        Stored::check(
            &compiled(&document()),
            complete_stored(json!({"bytes": "AQID"}))
        )
        .is_err()
    );
}

#[test]
fn public_check_agrees_with_stock_validator_on_closed_object_boundaries() {
    let schema = document();
    let (_, valid) = value();
    let mut missing = valid.clone();
    missing.as_object_mut().unwrap().remove("text");
    let mut unknown = valid.clone();
    unknown["unmentioned"] = json!(true);
    let mut protocol = valid.clone();
    protocol["$type"] = json!("not-a-declared-union");

    for candidate in [valid, missing, unknown, protocol] {
        let stock = jsonschema::validator_for(&schema)
            .expect("the generated schema is valid JSON Schema")
            .is_valid(&candidate);
        let ours = Stored::check(&compiled(&schema), candidate).is_ok();
        assert_eq!(ours, stock, "public check disagrees with stock validator");
    }
}

#[test]
fn sidecar_validation_rejects_supported_json_schema_constraints() {
    let reject = |schema: Value, value: Value| {
        assert!(
            Stored::check(&compiled(&schema), value).is_err(),
            "the sidecar accepted an invalid value for {schema}"
        );
    };

    reject(
        json!({
            "type": "string",
            "enum": ["ready"],
            "const": "ready",
            "pattern": "^[a-z]+$",
            "minLength": 5,
            "maxLength": 8
        }),
        json!("NO"),
    );
    reject(
        json!({
            "type": "number",
            "minimum": 1,
            "maximum": 10,
            "exclusiveMinimum": 1,
            "exclusiveMaximum": 10,
            "multipleOf": 2
        }),
        json!(11),
    );
    reject(json!({"type": "integer"}), json!(1.5));
    reject(
        json!({
            "type": "array",
            "items": {"type": "number"},
            "minItems": 2,
            "maxItems": 3
        }),
        json!([1]),
    );
    reject(
        json!({
            "$ref": "#/$defs/Map",
            "$defs": {
                "Map": {
                    "type": "object",
                    "properties": {"value": {"type": "string"}},
                    "required": ["value"],
                    "additionalProperties": false
                }
            }
        }),
        json!({"value": 1}),
    );
    reject(
        json!({
            "anyOf": [{"type": "string"}, {"type": "number"}]
        }),
        json!(null),
    );
    reject(
        json!({
            "type": "object",
            "properties": {
                "$bytes": {"type": "string", "contentEncoding": "base64"}
            },
            "required": ["$bytes"],
            "additionalProperties": false,
            "x-automerge": "bytes"
        }),
        json!({"$bytes": "not base64"}),
    );
}

#[test]
fn canonical_integers_are_javascript_safe_and_application_dollars_are_rejected() {
    assert!(
        Stored::check(
            &compiled(&document()),
            complete_stored(json!({"plain_number": 9_007_199_254_740_991i64}))
        )
        .is_ok()
    );
    assert!(
        Stored::check(
            &compiled(&document()),
            complete_stored(json!({"plain_number": 9_007_199_254_740_992u64}))
        )
        .is_err()
    );
    assert!(
        Stored::check(
            &compiled(&document()),
            complete_stored(json!({"plain_number": -9_007_199_254_740_991i64})),
        )
        .is_ok()
    );
    assert!(
        Stored::check(
            &compiled(&document()),
            complete_stored(json!({"plain_number": -9_007_199_254_740_992i64})),
        )
        .is_err()
    );
    let prefix_items_schema = json!({
        "type": "array",
        "prefixItems": [{"type": "integer"}]
    });
    assert!(CompiledSchema::compile(prefix_items_schema).is_err());
    let integer_schema = json!({
        "type": "object",
        "properties": {"value": {"type": "integer"}},
        "required": ["value"],
        "additionalProperties": false
    });
    assert!(Stored::check(&compiled(&integer_schema), json!({"value": 1.5})).is_err());
    let negative_zero = Stored::check(&compiled(&integer_schema), json!({"value": -0.0}))
        .expect_err("negative zero must be refused rather than reaching integer storage");
    assert!(
        negative_zero.to_string().contains("without losing"),
        "{negative_zero}"
    );
    assert!(serde_json::from_value::<Counter>(json!(i64::MAX)).is_err());
    assert!(std::panic::catch_unwind(|| Counter::new(i64::MAX)).is_err());
    let error = Stored::check(
        &compiled(&document()),
        complete_stored(json!({"$application": true})),
    )
    .unwrap_err();
    assert!(error.to_string().contains("$application"), "{error}");
}
