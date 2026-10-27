use super::*;
use crate::datum::DatumDecl;
use crate::datum::Stored;
use crate::datum::Universe;
use crate::datum::hydrate_value;
use crate::datum::reconcile_json_atomic;
use automerge::Automerge;
use automerge::ROOT;
use automerge::ReadDoc;

const TAG: &str = "org.example.authored";
const CID: &str = "bafyreidfayvfuwqa7qlnopdjiqrxzs6blmoeu4rujcjtnci5beludirz2a";

fn record(properties: Value, required: &[&str], nullable: &[&str]) -> Value {
    json!({"lexicon": 1, "id": TAG, "defs": {"main": {"type": "record", "key": "tid", "record": {"type": "object", "properties": properties, "required": required, "nullable": nullable}}}})
}

fn compile(primary: &Value) -> CompiledSchema {
    let universe = Universe::default();
    Bundle::new(&universe, Vec::new())
        .validate_lexicon(TAG, primary, &[])
        .unwrap()
}

fn accepts(schema: &CompiledSchema, mut value: Value) -> bool {
    value["$type"] = json!(TAG);
    Stored::check(schema, value).is_ok()
}

fn supplement(properties: Value) -> Universe {
    Universe {
        datums: vec![DatumDecl {
            id: TAG.to_owned(),
            name: "Authored".to_owned(),
            schema: schemars::Schema::try_from(json!({"type": "object", "properties": properties}))
                .unwrap(),
            flags: Vec::new(),
        }],
        types: Vec::new(),
    }
}

#[test]
fn byte_and_grapheme_limits_are_not_json_schema_character_counts() {
    let primary = record(
        json!({"text": {"type": "string", "minLength": 2, "maxLength": 4, "minGraphemes": 1, "maxGraphemes": 1, "knownValues": ["suggestion"]}}),
        &["text"],
        &[],
    );
    let compiled = compile(&primary);
    for text in ["é", "e\u{301}", "界"] {
        assert!(accepts(&compiled, json!({"text": text})), "{text}");
    }
    for text in ["a", "ab", "界界", ""] {
        assert!(!accepts(&compiled, json!({"text": text})), "{text}");
    }
    let family = "\u{1f468}\u{200d}\u{1f469}\u{200d}\u{1f467}";
    let compiled = compile(&record(
        json!({"text": {"type": "string", "maxGraphemes": 1}}),
        &[],
        &[],
    ));
    assert!(accepts(&compiled, json!({"text": family})));
}

#[test]
fn required_nullable_open_fields_and_signed_integer_bounds_are_primary() {
    let primary = record(
        json!({"maybe": {"type": "string"}, "count": {"type": "integer", "minimum": -2, "maximum": 3}, "wide": {"type": "integer"}, "opaque": {"type": "unknown"}}),
        &["maybe"],
        &["maybe"],
    );
    let compiled = compile(&primary);
    assert!(accepts(
        &compiled,
        json!({"maybe": null, "count": -2, "wide": i64::MAX, "extra": {"x": [1, true]}})
    ));
    assert!(accepts(&compiled, json!({"maybe": "", "opaque": {}})));
    for value in [
        json!({}),
        json!({"maybe": null, "count": 4}),
        json!({"maybe": null, "wide": u64::MAX}),
        json!({"maybe": null, "opaque": 1}),
        json!({"maybe": null, "opaque": {"$link": CID}}),
        json!({"maybe": null, "extra": 1.25}),
        json!({"maybe": null, "extra": {"$bytes": true}}),
    ] {
        assert!(!accepts(&compiled, value));
    }
}

#[test]
fn refs_and_open_closed_unions_do_not_admit_malformed_known_variants() {
    for closed in [false, true] {
        let mut primary = record(
            json!({"member": {"type": "union", "refs": ["#variant"], "closed": closed}, "label": {"type": "ref", "ref": "#label"}}),
            &["member"],
            &[],
        );
        primary["defs"]["variant"] = json!({"type": "object", "required": ["value"], "properties": {"value": {"type": "integer", "const": 7}}});
        primary["defs"]["label"] = json!({"type": "string", "enum": ["label"]});
        let compiled = compile(&primary);
        assert!(accepts(
            &compiled,
            json!({"member": {"$type": format!("{TAG}#variant"), "value": 7}, "label": "label"})
        ));
        for member in [
            json!({"value": 7}),
            json!({"$type": format!("{TAG}#variant"), "value": 8}),
            json!({"$type": format!("{TAG}#variant")}),
            json!({"$type": "not-a-type"}),
        ] {
            assert!(!accepts(&compiled, json!({"member": member})));
        }
        assert_eq!(
            accepts(
                &compiled,
                json!({"member": {"$type": "org.example.future", "future": [1]}})
            ),
            !closed
        );
    }
    let empty = compile(&record(
        json!({"member": {"type": "union", "refs": []}}),
        &[],
        &[],
    ));
    assert!(accepts(
        &empty,
        json!({"member": {"$type": "org.example.future"}})
    ));
}

#[test]
fn recursive_primary_refs_and_carried_storage_supplements_round_trip() {
    let mut primary = record(
        json!({"tree": {"type": "ref", "ref": "#node"}}),
        &["tree"],
        &[],
    );
    primary["defs"]["node"] = json!({"type": "object", "required": ["label"], "properties": {
        "label": {"type": "string", "maxLength": 4},
        "children": {"type": "array", "items": {"type": "ref", "ref": "#node"}}
    }});
    let mut universe = supplement(json!({"tree": {"$ref": "#/$defs/Node"}}));
    universe.types.push(crate::datum::TypeDecl {
        name: "Node".to_owned(),
        schema: schemars::Schema::try_from(json!({"type": "object", "properties": {
            "label": {"type": "string", "x-automerge": "text"},
            "children": {"type": "array", "items": {"$ref": "#/$defs/Node"}}
        }}))
        .unwrap(),
    });
    let compiled = Bundle::new(&universe, Vec::new())
        .validate_lexicon(TAG, &primary, &[])
        .unwrap();
    let value = json!({"$type": TAG, "tree": {"label": "root", "children": [{"label": "leaf", "future": true}]}});
    let mut doc = Automerge::new();
    let transaction = doc.transaction();
    reconcile_json_atomic(transaction, &compiled, &ROOT, "datum".into(), value.clone())
        .unwrap()
        .commit();
    assert_eq!(
        hydrate_value(&doc, &ROOT, "datum".into(), &compiled).unwrap(),
        value
    );
    assert!(!accepts(
        &compiled,
        json!({"tree": {"label": "root", "children": [{"label": "too long"}]}})
    ));
    let (_, datum) = doc.get(ROOT, "datum").unwrap().unwrap();
    let (_, tree) = doc.get(datum, "tree").unwrap().unwrap();
    assert!(matches!(
        doc.get(tree, "label").unwrap().unwrap().0,
        automerge::Value::Object(automerge::ObjType::Text)
    ));
}

#[test]
fn nullable_primary_unions_round_trip_known_and_future_payloads() {
    let mut primary = record(
        json!({"member": {"type": "union", "refs": ["#known"]}}),
        &["member"],
        &["member"],
    );
    primary["defs"]["known"] = json!({"type": "object", "required": ["bytes"], "properties": {"bytes": {"type": "bytes"}}});
    let compiled = compile(&primary);
    let mut doc = Automerge::new();
    for member in [
        json!({"$type": format!("{TAG}#known"), "bytes": {"$bytes": "AQ=="}}),
        json!({"$type": "org.example.future", "future": {"$bytes": "AQ=="}}),
        Value::Null,
    ] {
        let value = json!({"$type": TAG, "member": member});
        let transaction = doc.transaction();
        reconcile_json_atomic(transaction, &compiled, &ROOT, "datum".into(), value.clone())
            .unwrap()
            .commit();
        assert_eq!(
            hydrate_value(&doc, &ROOT, "datum".into(), &compiled).unwrap(),
            value
        );
    }
    assert!(!accepts(
        &compiled,
        json!({"member": {"$type": format!("{TAG}#known"), "future": true}})
    ));
}

#[test]
fn bytes_and_cid_links_validate_their_actual_payloads() {
    let compiled = compile(&record(
        json!({"bytes": {"type": "bytes", "minLength": 1, "maxLength": 2}, "link": {"type": "cid-link"}}),
        &[],
        &[],
    ));
    assert!(accepts(
        &compiled,
        json!({"bytes": {"$bytes": "AQI="}, "link": {"$link": CID}})
    ));
    assert!(accepts(&compiled, json!({"bytes": {"$bytes": "AQI"}})));
    for value in [
        json!({"bytes": {"$bytes": ""}}),
        json!({"bytes": {"$bytes": "AQID"}}),
        json!({"bytes": {"$bytes": "!"}}),
        json!({"link": {"$link": "bnotacid"}}),
        json!({"link": {"$link": CID, "extra": true}}),
    ] {
        assert!(!accepts(&compiled, value));
    }
}

#[test]
fn formats_use_identifier_syntax_without_resolution_or_supported_method_restrictions() {
    for (format, valid, invalid) in [
        ("did", "did:future:identifier", "did:METHOD:identifier"),
        ("handle", "EXAMPLE.test", "example.123"),
        ("at-identifier", "did:future:x", "x"),
        ("nsid", "org.123.exampleName", "org.example.bad-name"),
        ("tid", "2222222222222", "zzzzzzzzzzzzz"),
        ("record-key", "pre:fix", ".."),
        (
            "at-uri",
            "at://did:future:x/org.example.record/key",
            "at://example.com/bad/key",
        ),
        ("uri", "urn:example:identifier", "not a uri"),
        (
            "datetime",
            "1985-04-12T23:20:50.12345678912345Z",
            "1985-04-12t23:20:50Z",
        ),
        ("cid", CID, "bnotacid"),
    ] {
        let compiled = compile(&record(
            json!({"value": {"type": "string", "format": format}}),
            &[],
            &[],
        ));
        assert!(accepts(&compiled, json!({"value": valid})), "{format}");
        assert!(!accepts(&compiled, json!({"value": invalid})), "{format}");
    }
    for value in [
        "1985-04-12T23:20:50-00:00",
        "1985-00-12T23:20:50Z",
        "0000-01-01T00:00:00+01:00",
        "1985-04-12T23:20:50z",
    ] {
        assert!(!datetime(value), "{value}");
    }
}

#[test]
fn native_supplements_round_trip_through_transactions_without_datetime_precision_loss() {
    let primary = record(
        json!({"uint": {"type": "string"}, "float": {"type": "string"}, "counter": {"type": "integer"}, "text": {"type": "string"}, "instant": {"type": "string", "format": "datetime"}, "bytes": {"type": "bytes"}, "wide": {"type": "integer"}}),
        &[],
        &[],
    );
    let universe = supplement(
        json!({"uint": {"type": "string", "x-automerge": "uint", "format": "uint64"}, "float": {"type": "string", "x-automerge": "float", "format": "double"}, "counter": {"type": "integer", "x-automerge": "counter"}, "text": {"type": "string", "x-automerge": "text"}}),
    );
    let compiled = Bundle::new(&universe, Vec::new())
        .validate_lexicon(TAG, &primary, &[])
        .unwrap();
    let mut doc = Automerge::new();
    let mut value = json!({"$type": TAG, "uint": u64::MAX, "float": 1.25, "counter": i64::MAX, "text": "text", "instant": "1985-04-12T23:20:50.12345678912345Z", "bytes": {"$bytes": "AQ=="}, "wide": i64::MAX});
    let transaction = doc.transaction();
    reconcile_json_atomic(transaction, &compiled, &ROOT, "datum".into(), value.clone())
        .unwrap()
        .commit();
    value["uint"] = json!(u64::MAX.to_string());
    value["float"] = json!("1.25");
    assert_eq!(
        hydrate_value(&doc, &ROOT, "datum".into(), &compiled).unwrap(),
        value
    );
    let (_, datum) = doc.get(ROOT, "datum").unwrap().unwrap();
    assert!(
        matches!(doc.get(&datum, "uint").unwrap().unwrap().0, automerge::Value::Scalar(value) if matches!(value.as_ref(), automerge::ScalarValue::Uint(u64::MAX)))
    );
    let before = doc.get_heads();
    let transaction = doc.transaction();
    reconcile_json_atomic(transaction, &compiled, &ROOT, "datum".into(), value.clone())
        .unwrap()
        .commit();
    assert_eq!(doc.get_heads(), before);
    value["instant"] = json!("2024-01-02T03:04:05Z");
    let transaction = doc.transaction();
    reconcile_json_atomic(transaction, &compiled, &ROOT, "datum".into(), value.clone())
        .unwrap()
        .commit();
    assert_eq!(
        hydrate_value(&doc, &ROOT, "datum".into(), &compiled).unwrap(),
        value
    );
    assert!(
        matches!(doc.get(&datum, "instant").unwrap().unwrap().0, automerge::Value::Scalar(value) if matches!(value.as_ref(), automerge::ScalarValue::Timestamp(_)))
    );
    for invalid in [
        json!({"uint": "18446744073709551616"}),
        json!({"uint": "-1"}),
        json!({"float": "not a float"}),
    ] {
        assert!(!accepts(&compiled, invalid));
    }
}

#[test]
fn a_supplement_vocabulary_restates_the_primary_enum_or_is_refused() {
    let primary = record(
        json!({"value": {"type": "string", "enum": ["a", "b"]}}),
        &["value"],
        &[],
    );
    let compiled = Bundle::new(
        &supplement(json!({"value": {"type": "string", "enum": ["a", "b"]}})),
        Vec::new(),
    )
    .validate_lexicon(TAG, &primary, &[])
    .unwrap();
    assert!(accepts(&compiled, json!({"value": "a"})));
    assert!(!accepts(&compiled, json!({"value": "c"})));
    for (disagreeing, reason) in [
        (json!({"type": "string", "enum": ["a", "c"]}), "contradicts"),
        (json!({"type": "string", "enum": []}), "string vocabulary"),
        (json!({"type": "integer", "enum": [1]}), "string vocabulary"),
    ] {
        let error = Bundle::new(&supplement(json!({"value": disagreeing})), Vec::new())
            .validate_lexicon(TAG, &primary, &[])
            .unwrap_err()
            .to_string();
        assert!(error.contains(reason), "{error}");
    }
    let unlisted = record(json!({"value": {"type": "string"}}), &["value"], &[]);
    let error = Bundle::new(
        &supplement(json!({"value": {"type": "string", "enum": ["a"]}})),
        Vec::new(),
    )
    .validate_lexicon(TAG, &unlisted, &[])
    .unwrap_err()
    .to_string();
    assert!(error.contains("contradicts"), "{error}");
}

#[test]
fn a_supplement_member_branch_restates_its_discriminant_const() {
    let primary = record(
        json!({
            "layout": {
                "type": "union",
                "refs": ["#member"],
                "closed": true,
            }
        }),
        &["layout"],
        &[],
    );
    let mut primary = primary;
    primary["defs"]["member"] = json!({
        "type": "object",
        "properties": {"empty": {"type": "boolean"}},
    });
    // The compiled union names its branches by the member's tag, not the definition's name;
    // the member file is carried, as admission requires.
    primary["defs"]["main"]["record"]["properties"]["layout"]["refs"] =
        json!(["org.example.test.member"]);
    let member = json!({
        "lexicon": 1,
        "id": "org.example.test.member",
        "defs": {"main": {"type": "object", "properties": {"empty": {"type": "boolean"}}}},
    });
    let union = supplement(json!({
        "layout": {
            "oneOf": [
                {
                    "type": "object",
                    "properties": {
                        "$type": {"type": "string", "const": "org.example.test.member"},
                        "empty": {"type": "boolean"},
                    },
                    "required": ["$type"],
                }
            ]
        }
    }));
    Bundle::new(&union, Vec::new())
        .validate_lexicon(TAG, &primary, &[&member])
        .unwrap();
    // A const that does not match the branch's compiled discriminant is refused.
    let disagreeing = supplement(json!({
        "layout": {
            "oneOf": [
                {
                    "type": "object",
                    "properties": {
                        "$type": {"type": "string", "const": "org.example.test.other"},
                    },
                    "required": ["$type"],
                }
            ]
        }
    }));
    let error = Bundle::new(&disagreeing, Vec::new())
        .validate_lexicon(TAG, &primary, &[&member])
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("const") || error.contains("absent from the primary union"),
        "{error}"
    );
}

#[test]
fn native_authoring_is_normalized_before_primary_enum_and_length_checks() {
    for (marker, canonical, authored) in [
        ("uint", "1", vec![json!(1), json!("01"), json!("+1")]),
        (
            "float",
            "1.0",
            vec![json!(1.0), json!("01.00"), json!("1e0")],
        ),
    ] {
        for restriction in [
            json!({"enum": [canonical]}),
            json!({"minLength": canonical.len(), "maxLength": canonical.len()}),
        ] {
            let mut field = restriction;
            field["type"] = json!("string");
            let primary = record(json!({"value": field}), &["value"], &["value"]);
            let universe =
                supplement(json!({"value": {"type": ["string", "null"], "x-automerge": marker}}));
            let compiled = Bundle::new(&universe, Vec::new())
                .validate_lexicon(TAG, &primary, &[])
                .unwrap();
            for value in &authored {
                assert_eq!(
                    Stored::check(&compiled, json!({"$type": TAG, "value": value}))
                        .unwrap()
                        .into_value(),
                    json!({"$type": TAG, "value": canonical})
                );
            }
            assert!(accepts(&compiled, json!({"value": null})));
            for value in [json!(10), json!("10"), json!("10.00")] {
                assert!(!accepts(&compiled, json!({"value": value})));
            }
        }
    }
    let primary = record(json!({"value": {"type": "string"}}), &[], &[]);
    let universe =
        supplement(json!({"value": {"type": "string", "x-automerge": "float", "format": "float"}}));
    let compiled = Bundle::new(&universe, Vec::new())
        .validate_lexicon(TAG, &primary, &[])
        .unwrap();
    assert_eq!(
        Stored::check(&compiled, json!({"$type": TAG, "value": 1e100}))
            .unwrap()
            .into_value(),
        json!({"$type": TAG, "value": "Infinity"})
    );
}

#[test]
fn native_annotations_cannot_make_primary_constants_unrepresentable() {
    for (marker, field) in [
        ("uint", json!({"type": "string", "const": "01"})),
        ("float", json!({"type": "string", "enum": ["1.0", "1e0"]})),
    ] {
        let universe = supplement(json!({"value": {"type": "string", "x-automerge": marker}}));
        assert!(matches!(
            Bundle::new(&universe, Vec::new()).validate_lexicon(
                TAG,
                &record(json!({"value": field}), &[], &[]),
                &[]
            ),
            Err(LexiconAdmissionError::Invalid { .. })
        ));
    }
}

#[test]
fn structural_depth_does_not_consume_reference_hops_during_native_normalization() {
    let mut field = json!({"type": "string"});
    let mut annotation = json!({"type": "string", "x-automerge": "uint"});
    let mut authored = json!(1);
    let mut expected = json!("1");
    for _ in 0..24 {
        field = json!({"type": "array", "items": field});
        annotation = json!({"type": "array", "items": annotation});
        authored = json!([authored]);
        expected = json!([expected]);
    }
    let primary = record(json!({"value": field}), &[], &[]);
    let universe = supplement(json!({"value": annotation}));
    let compiled = Bundle::new(&universe, Vec::new())
        .validate_lexicon(TAG, &primary, &[])
        .unwrap();
    assert_eq!(
        Stored::check(&compiled, json!({"$type": TAG, "value": authored}))
            .unwrap()
            .into_value(),
        json!({"$type": TAG, "value": expected})
    );
}

#[test]
fn unused_supplement_definitions_and_bundle_identity_are_checked_at_admission() {
    let primary = record(json!({"value": {"type": "string"}}), &[], &[]);
    for schema in [
        json!({"$ref": "https://example.test/not-carried"}),
        json!({"$ref": "#/$defs/Missing"}),
        json!({"$ref": "#/$defs/Authored", "type": "object"}),
        json!({"type": "string", "x-automerge": "vectorClock"}),
        json!({"type": "string", "format": "email"}),
        json!({"type": "string", "maxLength": 2}),
    ] {
        let mut universe = supplement(json!({}));
        universe.types.push(crate::datum::TypeDecl {
            name: "Unused".to_owned(),
            schema: schemars::Schema::try_from(schema).unwrap(),
        });
        assert!(
            Bundle::new(&universe, Vec::new())
                .validate_lexicon(TAG, &primary, &[])
                .is_err()
        );
    }
    let mut universe = supplement(json!({}));
    universe.datums[0].id = "org.example.other".to_owned();
    assert!(matches!(
        Bundle::new(&universe, Vec::new()).validate_lexicon(TAG, &primary, &[]),
        Err(LexiconAdmissionError::Invalid { .. })
    ));
}

#[test]
fn typed_maps_are_the_only_admitted_dynamic_key_narrowing() {
    let primary = record(json!({"map": {"type": "unknown"}}), &[], &[]);
    let universe = supplement(
        json!({"map": {"type": "object", "patternProperties": {"^[a-z]+$": {"type": "string", "x-automerge": "uint"}}, "additionalProperties": false}}),
    );
    let compiled = Bundle::new(&universe, Vec::new())
        .validate_lexicon(TAG, &primary, &[])
        .unwrap();
    assert!(accepts(
        &compiled,
        json!({"map": {"key": "18446744073709551615"}})
    ));
    assert!(!accepts(&compiled, json!({"map": {"BAD": "1"}})));
    assert!(!accepts(&compiled, json!({"map": {"key": "-1"}})));
}

#[test]
fn nested_typed_map_array_leaves_preserve_full_width_native_counters() {
    let primary = record(json!({"map": {"type": "unknown"}}), &[], &[]);
    let mut leaf = json!({"type": "integer", "x-automerge": "counter"});
    let mut value = json!(i64::MAX);
    for _ in 0..10 {
        leaf = json!({"type": "array", "items": {"type": "object", "additionalProperties": leaf}});
        value = json!([{"entry": value}]);
    }
    let universe = supplement(json!({"map": {"type": "object", "additionalProperties": leaf}}));
    let compiled = Bundle::new(&universe, Vec::new())
        .validate_lexicon(TAG, &primary, &[])
        .unwrap();
    let value = json!({"$type": TAG, "map": {"entry": value}});
    let mut doc = Automerge::new();
    let transaction = doc.transaction();
    reconcile_json_atomic(transaction, &compiled, &ROOT, "datum".into(), value.clone())
        .unwrap()
        .commit();
    assert_eq!(
        hydrate_value(&doc, &ROOT, "datum".into(), &compiled).unwrap(),
        value
    );
    let heads = doc.get_heads();
    let transaction = doc.transaction();
    reconcile_json_atomic(transaction, &compiled, &ROOT, "datum".into(), value)
        .unwrap()
        .commit();
    assert_eq!(doc.get_heads(), heads);
}

#[test]
fn nullable_supplement_wrappers_cannot_hide_contradictory_annotations() {
    let primary = record(json!({"value": {"type": "string"}}), &[], &["value"]);
    for annotation in [
        json!({"anyOf": [{"type": "string"}, {"type": "null"}], "x-automerge": "counter"}),
        json!({"anyOf": [{"type": "string"}, {"type": "null", "x-automerge": "text"}]}),
        json!({"anyOf": [{"type": "string"}, {"type": "null"}, {"type": "null"}]}),
        json!({"type": ["string", "null", "null"]}),
    ] {
        let universe = supplement(json!({"value": annotation}));
        assert!(
            Bundle::new(&universe, Vec::new())
                .validate_lexicon(TAG, &primary, &[])
                .is_err()
        );
    }
    let universe = supplement(
        json!({"value": {"anyOf": [{"type": "string", "x-automerge": "text"}, {"type": "null"}]}}),
    );
    let compiled = Bundle::new(&universe, Vec::new())
        .validate_lexicon(TAG, &primary, &[])
        .unwrap();
    assert!(accepts(&compiled, json!({"value": "text"})));
    assert!(accepts(&compiled, json!({"value": null})));
}

#[test]
fn carried_unused_storage_declarations_are_not_ignored() {
    let primary = record(json!({}), &[], &[]);
    for annotation in [
        json!({"type": "string", "x-automerge": "counter"}),
        json!({"type": "integer", "format": "date-time"}),
        json!({"type": "integer", "x-automerge": "bytes"}),
        json!({"anyOf": [{"type": "string"}], "x-automerge": "text"}),
    ] {
        let mut universe = supplement(json!({}));
        universe.types.push(crate::datum::TypeDecl {
            name: "Unused".to_owned(),
            schema: schemars::Schema::try_from(annotation).unwrap(),
        });
        assert!(
            Bundle::new(&universe, Vec::new())
                .validate_lexicon(TAG, &primary, &[])
                .is_err()
        );
    }
}

#[test]
fn typed_maps_can_select_native_bytes_and_lossless_datetime_leaves() {
    let primary = record(
        json!({"bytes": {"type": "unknown"}, "instants": {"type": "unknown"}}),
        &[],
        &[],
    );
    let universe = supplement(json!({
        "bytes": {"type": "object", "additionalProperties": {"type": "object", "x-automerge": "bytes"}},
        "instants": {"type": "object", "additionalProperties": {"type": "string", "format": "date-time"}}
    }));
    let compiled = Bundle::new(&universe, Vec::new())
        .validate_lexicon(TAG, &primary, &[])
        .unwrap();
    let mut doc = Automerge::new();
    for instant in [
        "2024-01-02T03:04:05Z",
        "2024-01-02T03:04:05.000Z",
        "2024-01-02T04:04:05+01:00",
        "2024-01-02T03:04:05.123456789123456Z",
    ] {
        let value = json!({"$type": TAG, "bytes": {"one": {"$bytes": "AQ=="}}, "instants": {"one": instant}});
        let transaction = doc.transaction();
        reconcile_json_atomic(transaction, &compiled, &ROOT, "datum".into(), value.clone())
            .unwrap()
            .commit();
        assert_eq!(
            hydrate_value(&doc, &ROOT, "datum".into(), &compiled).unwrap(),
            value
        );
        let heads = doc.get_heads();
        let transaction = doc.transaction();
        reconcile_json_atomic(transaction, &compiled, &ROOT, "datum".into(), value)
            .unwrap()
            .commit();
        assert_eq!(doc.get_heads(), heads);
    }
    let (_, datum) = doc.get(ROOT, "datum").unwrap().unwrap();
    let (_, bytes) = doc.get(&datum, "bytes").unwrap().unwrap();
    assert!(
        matches!(doc.get(bytes, "one").unwrap().unwrap().0, automerge::Value::Scalar(value) if matches!(value.as_ref(), automerge::ScalarValue::Bytes(bytes) if bytes == &[1]))
    );
    assert!(!accepts(
        &compiled,
        json!({"instants": {"one": "not an instant"}})
    ));
    assert!(!accepts(
        &compiled,
        json!({"bytes": {"one": {"$bytes": true}}})
    ));
}

#[test]
fn malformed_lexicon_metadata_and_nested_primary_types_fail_admission() {
    let empty = Universe::default();
    let mut nested = record(json!({}), &[], &[]);
    nested["defs"]["main"]["record"]["properties"]["nested"] = nested["defs"]["main"].clone();
    let mut description = record(json!({}), &[], &[]);
    description["description"] = json!(false);
    let mut publication = record(json!({}), &[], &[]);
    publication["$type"] = json!("org.example.notLexicon");
    let mut token = record(json!({}), &[], &[]);
    token["defs"]["symbol"] = json!({"type": "token", "description": 1});
    for primary in [nested, description, publication, token] {
        assert!(
            Bundle::new(&empty, Vec::new())
                .validate_lexicon(TAG, &primary, &[])
                .is_err()
        );
    }
}

#[test]
fn unsupported_primary_forms_and_sidecar_rules_are_explicit_admission_errors() {
    let empty = Universe::default();
    for field in [
        json!({"type": "blob"}),
        json!({"type": "string", "format": "language"}),
        json!({"type": "token"}),
        json!({"type": "union", "refs": [], "closed": true}),
        json!({"type": "string", "minGraphemes": "one"}),
    ] {
        assert!(
            Bundle::new(&empty, Vec::new())
                .validate_lexicon(TAG, &record(json!({"value": field}), &[], &[]), &[])
                .is_err()
        );
    }
    for field in [
        json!({"type": "integer"}),
        json!({"type": "string", "pattern": "x"}),
        json!({"type": "string", "maxLength": 5}),
        json!({"type": "string", "x-automerge": "vectorClock"}),
        json!({"type": "string", "format": "email"}),
        json!({"type": "string", "not": {"const": "x"}}),
    ] {
        let universe = supplement(json!({"value": field}));
        assert!(
            Bundle::new(&universe, Vec::new())
                .validate_lexicon(
                    TAG,
                    &record(json!({"value": {"type": "string"}}), &[], &[]),
                    &[]
                )
                .is_err()
        );
    }
}
