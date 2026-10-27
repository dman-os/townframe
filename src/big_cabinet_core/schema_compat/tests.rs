use super::*;
use serde_json::json;

#[test]
fn optional_property_on_open_object_is_compatible() {
    let old = json!({
        "type": "object",
        "properties": {"title": {"type": "string"}},
        "additionalProperties": true
    });
    let new = json!({
        "type": "object",
        "properties": {
            "title": {"type": "string"},
            "summary": {"type": "string"}
        },
        "additionalProperties": true
    });
    let report = compare_schemas(&old, &new);
    assert_eq!(report.json.result, Compatibility::Compatible);
    // Lexicon objects are open, so an optional property is an evolution in either direction.
    assert_eq!(report.lexicon.result, Compatibility::Compatible);
}

#[test]
fn required_addition_and_closed_extension_are_incompatible() {
    let old = json!({"type":"object", "properties":{"title":{"type":"string"}}, "additionalProperties":false});
    let new = json!({"type":"object", "properties":{"title":{"type":"string"},"summary":{"type":"string"}}, "required":["title","summary"], "additionalProperties":false});
    let report = compare_schemas(&old, &new);
    assert_eq!(report.json.result, Compatibility::Incompatible);
    assert!(
        report
            .json
            .diagnostics
            .iter()
            .any(|item| item.path == "/summary")
    );
    assert_eq!(report.lexicon.result, Compatibility::Incompatible);
}

#[test]
fn lexicon_nullable_change_is_incompatible_in_either_direction() {
    let present = json!({"type":"object","properties":{"value":{"type":"string"}}});
    let nullable = json!({"type":"object","properties":{"value":{"type":["string","null"]}}});
    assert_eq!(
        compare_schemas(&present, &nullable).lexicon.result,
        Compatibility::Incompatible
    );
    assert_eq!(
        compare_schemas(&nullable, &present).lexicon.result,
        Compatibility::Incompatible
    );
}

#[test]
fn open_union_and_vocabulary_allow_additions_but_closed_union_does_not() {
    let branch = |tag: Value| {
        json!({
            "type":"object",
            "properties":{"$type":tag,"value":{"type":"object","additionalProperties":true}},
            "required":["$type","value"]
        })
    };
    let fallback = |known: &[&str]| branch(json!({"type":"string", "not":{"enum":known}}));
    let old_union = json!({"anyOf":[branch(json!({"const":"known"})), fallback(&["known"])]});
    let new_union = json!({"anyOf":[
        branch(json!({"const":"known"})),
        branch(json!({"const":"future"})),
        fallback(&["known","future"])
    ]});
    let report = compare_schemas(&old_union, &new_union);
    assert_eq!(
        report.json.result,
        Compatibility::Compatible,
        "{:?}",
        report.json.diagnostics
    );

    let old_closed = json!({"oneOf":[branch(json!({"const":"known"}))]});
    let new_closed = json!({"oneOf":[
        branch(json!({"const":"known"})),
        branch(json!({"const":"future"}))
    ]});
    assert_eq!(
        compare_schemas(&old_closed, &new_closed).json.result,
        Compatibility::Incompatible
    );
    assert_eq!(
        compare_schemas(&old_closed, &new_union).json.result,
        Compatibility::NotEstablished
    );

    let old_vocab = json!({"type":"string", "knownValues":["known"]});
    let new_vocab = json!({"type":"string", "knownValues":["known","future"]});
    assert_eq!(
        compare_schemas(&old_vocab, &new_vocab).json.result,
        Compatibility::Compatible
    );
}

#[test]
fn native_codec_change_is_separate_from_json_shape() {
    let old = json!({"type":"string", "x-automerge":"text"});
    let new = json!({
        "type":"object",
        "x-automerge":"bytes",
        "properties":{"$bytes":{"type":"string"}},
        "required":["$bytes"],
        "additionalProperties":false
    });
    let report = compare_schemas(&old, &new);
    assert_eq!(
        report.native_storage.result,
        Compatibility::Incompatible,
        "{:?}",
        report.native_storage.diagnostics
    );
    assert_eq!(report.json.result, Compatibility::Incompatible);
}

#[test]
fn unresolved_reference_is_not_established() {
    let old = json!({"$ref":"#/$defs/Item"});
    let new = json!({"$ref":"#/$defs/Item"});
    let report = compare_schemas(&old, &new);
    assert_eq!(report.json.result, Compatibility::NotEstablished);
    assert!(
        report
            .json
            .diagnostics
            .iter()
            .any(|item| item.reason.contains("undeclared"))
    );
}

#[test]
fn bundle_references_are_resolved() {
    let old = SchemaBundle::with_definitions(
        json!({"$ref":"#/$defs/Item"}),
        [(
            "Item".to_owned(),
            json!({"type":"string", "knownValues":["old"]}),
        )],
    );
    let new = SchemaBundle::with_definitions(
        json!({"$ref":"#/$defs/Item"}),
        [(
            "Item".to_owned(),
            json!({"type":"string", "knownValues":["old","new"]}),
        )],
    );
    assert_eq!(compare(&old, &new).json.result, Compatibility::Compatible);
}

#[test]
fn unconstrained_enum_and_const_changes_are_not_silently_compatible() {
    let unconstrained = json!({});
    let string = json!({"type":"string"});
    assert_eq!(
        compare_schemas(&unconstrained, &string).json.result,
        Compatibility::Incompatible
    );

    let old_enum = json!({"type":"string", "enum":["a"]});
    let open_string = json!({"type":"string"});
    assert_eq!(
        compare_schemas(&old_enum, &open_string).json.result,
        Compatibility::Incompatible
    );

    let old_const = json!({"const":"a"});
    let new_const = json!({"const":"b"});
    assert_eq!(
        compare_schemas(&old_const, &new_const).json.result,
        Compatibility::Incompatible
    );
}

#[test]
fn typed_map_codec_changes_are_native_incompatible() {
    let old = json!({
        "type":"object",
        "additionalProperties":{"type":"string", "x-automerge":"text"}
    });
    let new = json!({
        "type":"object",
        "additionalProperties":{
            "type":"object",
            "x-automerge":"bytes",
            "properties":{"$bytes":{"type":"string"}},
            "required":["$bytes"],
            "additionalProperties":false
        }
    });
    assert_eq!(
        compare_schemas(&old, &new).native_storage.result,
        Compatibility::Incompatible
    );
}

#[test]
fn generated_open_union_fallback_accepts_new_member() {
    let branch = |tag: Value| {
        json!({
            "type":"object",
            "properties":{"$type":tag,"value":true},
            "required":["$type","value"]
        })
    };
    let fallback = |known: &[&str]| branch(json!({"type":"string", "not":{"enum":known}}));
    let old = json!({"oneOf":[branch(json!({"const":"known"})), fallback(&["known"])]});
    let new = json!({"oneOf":[
        branch(json!({"const":"known"})),
        branch(json!({"const":"future"})),
        fallback(&["known","future"])
    ]});
    assert_eq!(
        compare_schemas(&old, &new).json.result,
        Compatibility::Compatible
    );
}

#[test]
fn arrays_nullable_and_sidecar_constraints_have_separate_answers() {
    let old = json!({
        "type":"object",
        "properties":{"values":{"type":"array","items":{"type":["integer","null"]},"minLength":1}},
        "additionalProperties":false
    });
    let new = json!({
        "type":"object",
        "properties":{"values":{"type":"array","items":{"type":["integer","null"]},"minLength":2}},
        "additionalProperties":false
    });
    let report = compare_schemas(&old, &new);
    assert_eq!(report.json.result, Compatibility::Incompatible);
    // `minLength` is a sidecar refinement with no Lexicon spelling, so the published files agree
    // even though the sidecars do not. The native storage is untouched either way.
    assert_eq!(report.native_storage.result, Compatibility::Compatible);
}

#[test]
fn local_reference_renames_are_structural_but_changed_targets_are_reported() {
    let old = SchemaBundle::with_definitions(
        json!({"type":"object","properties":{"a/b":{"$ref":"#/$defs/One"}},"additionalProperties":false}),
        [("One".to_owned(), json!({"type":"string"}))],
    );
    let renamed = SchemaBundle::with_definitions(
        json!({"type":"object","properties":{"a/b":{"$ref":"#/$defs/Two"}},"additionalProperties":false}),
        [("Two".to_owned(), json!({"type":"string"}))],
    );
    assert_eq!(
        compare(&old, &renamed).json.result,
        Compatibility::Compatible
    );

    let changed = SchemaBundle::with_definitions(
        json!({"type":"object","properties":{"a/b":{"$ref":"#/$defs/Two"}},"additionalProperties":false}),
        [("Two".to_owned(), json!({"type":"integer"}))],
    );
    let report = compare(&old, &changed);
    assert_eq!(report.json.result, Compatibility::Incompatible);
    assert!(
        report
            .json
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.path == "/a~1b")
    );
    assert_eq!(report, compare(&old, &changed));
}

#[test]
fn supported_shapes_are_reflexive() {
    let fixtures = [
        json!({"type":"string"}),
        json!({"type":"array","items":{"type":"integer"}}),
        json!({"type":"object","properties":{"name":{"type":"string"}},"additionalProperties":true}),
        json!({"oneOf":[
            {"type":"object","properties":{"$type":{"const":"a"},"value":{"type":"object","additionalProperties":true}},"required":["$type","value"]},
            {"type":"object","properties":{"$type":{"const":"b"},"value":{"type":"object","additionalProperties":true}},"required":["$type","value"]}
        ]}),
    ];
    for schema in fixtures {
        let report = compare_schemas(&schema, &schema);
        assert_eq!(report.json.result, Compatibility::Compatible);
        assert_eq!(report.native_storage.result, Compatibility::Compatible);
        assert_eq!(
            report.lexicon.result,
            Compatibility::Compatible,
            "{:?}",
            report.lexicon.diagnostics
        );
        assert!(report.json.diagnostics.is_empty());
    }
}

#[test]
fn actual_datum_union_schema_is_compatible_when_a_member_is_added() {
    fn member(id: &'static str) -> crate::datum::MemberSchema {
        crate::datum::MemberSchema {
            id,
            schema: schemars::Schema::try_from(
                json!({"type":"object","additionalProperties":true}),
            )
            .unwrap(),
        }
    }
    let old = serde_json::to_value(crate::datum::union_schema(
        &crate::datum::UnionLayout::Adjacent {
            tag: "$type",
            content: "value",
        },
        true,
        vec![member("known")],
        &serde_json::Map::new(),
    ))
    .unwrap();
    let new = serde_json::to_value(crate::datum::union_schema(
        &crate::datum::UnionLayout::Adjacent {
            tag: "$type",
            content: "value",
        },
        true,
        vec![member("known"), member("future")],
        &serde_json::Map::new(),
    ))
    .unwrap();
    assert_eq!(
        compare_schemas(&old, &new).json.result,
        Compatibility::Compatible
    );
}

#[test]
fn unchanged_and_changed_unsupported_applicators_are_not_established() {
    let schemas = [
        json!({"type":"object","patternProperties":{"^x":{"type":"string"}}}),
        json!({"type":"array","prefixItems":[{"type":"string"}]}),
        json!({"type":"array","contains":{"type":"string"}}),
        json!({"type":"object","unevaluatedProperties":false}),
        json!({"type":"object","propertyNames":{"pattern":"x"}}),
        json!({"type":"bogus"}),
    ];
    for schema in schemas {
        let report = compare_schemas(&schema, &schema);
        assert_eq!(report.json.result, Compatibility::NotEstablished);
        assert_eq!(report.native_storage.result, Compatibility::NotEstablished);
    }
    let old = json!({"type":"array","minContains":1,"contains":{"type":"string"}});
    let new = json!({"type":"array","minContains":2,"contains":{"type":"string"}});
    assert_eq!(
        compare_schemas(&old, &new).json.result,
        Compatibility::NotEstablished
    );
}

#[test]
fn union_operator_and_malformed_carriers_are_not_silently_matched() {
    let branch = |tag: &str| {
        json!({
            "type":"object",
            "properties":{"$type":{"const":tag},"value":{"type":"string"}},
            "required":["$type","value"]
        })
    };
    let duplicate = json!({"oneOf":[branch("same"), branch("same")]});
    assert_eq!(
        compare_schemas(&duplicate, &duplicate).json.result,
        Compatibility::NotEstablished
    );

    let fallback = json!({
        "type":"object",
        "properties":{"$type":{"type":"string","not":{"enum":["known"]}},"value":{"type":"string"}},
        "required":["$type","value"]
    });
    let multiple_fallbacks = json!({"oneOf":[branch("known"), fallback.clone(), fallback]});
    assert_eq!(
        compare_schemas(&multiple_fallbacks, &multiple_fallbacks)
            .json
            .result,
        Compatibility::NotEstablished
    );

    let one = json!({"oneOf":[branch("known")]});
    let any = json!({"anyOf":[branch("known")]});
    assert_eq!(
        compare_schemas(&one, &any).json.result,
        Compatibility::NotEstablished
    );
}

#[test]
fn fallback_exclusion_changes_are_checked() {
    let branch = |tag: Value| {
        json!({
            "type":"object",
            "properties":{"$type":tag,"value":{"type":"object","additionalProperties":true}},
            "required":["$type","value"]
        })
    };
    let old = json!({"oneOf":[branch(json!({"const":"known"})), branch(json!({"type":"string","not":{"enum":["known"]}}))]});
    let changed = json!({"oneOf":[branch(json!({"const":"known"})), branch(json!({"type":"string","not":{"enum":["other"]}}))]});
    assert_eq!(
        compare_schemas(&old, &changed).json.result,
        Compatibility::NotEstablished
    );
}

#[test]
fn nullable_storage_markers_preserve_canonical_nullability() {
    // Nullability is a property of the object holding the position, not of the position, so the
    // marker is tested on an object's property. A position that is itself nullable has no
    // Lexicon spelling.
    let property = |kind: &str, marker: &str, nullable: bool| {
        let type_value = if nullable {
            json!([kind, "null"])
        } else {
            json!(kind)
        };
        json!({
            "type":"object",
            "properties":{"value":{"type":type_value,"x-automerge":marker}},
            "additionalProperties":false
        })
    };
    for (marker, kind, lexicon) in [
        ("float", "string", Compatibility::Incompatible),
        ("text", "string", Compatibility::Incompatible),
        ("counter", "integer", Compatibility::Incompatible),
        ("timestamp", "string", Compatibility::Incompatible),
        // This list fixture is an array that states no items, so it is not a publishable Lexicon.
        ("list", "array", Compatibility::NotEstablished),
    ] {
        let nullable = property(kind, marker, true);
        let present = property(kind, marker, false);
        assert_eq!(
            compare_schemas(&nullable, &nullable).json.result,
            Compatibility::Compatible,
            "{marker}"
        );
        assert_eq!(
            compare_schemas(&nullable, &present).json.result,
            Compatibility::Incompatible,
            "{marker}"
        );
        // A storage form that projects keeps nullability as a name on the holding object. That
        // name appearing or disappearing is the same incompatible change as a plain property.
        assert_eq!(
            compare_schemas(&nullable, &present).lexicon.result,
            lexicon,
            "{marker}"
        );
    }
}

#[test]
fn nullable_storage_marker_changes_are_checked_through_references_and_unions() {
    // The nullable position is an object's property, which is the only place a Lexicon can say
    // that a value admits null.
    let holding = |nullable: bool| {
        json!({
            "type":"object",
            "properties":{"text":{"type": if nullable {json!(["string","null"])} else {json!("string")}, "x-automerge":"text"}},
            "additionalProperties":false
        })
    };
    let old_value = holding(true);
    let new_value = holding(false);
    let old = SchemaBundle::with_definitions(old_value.clone(), []);
    let new = SchemaBundle::with_definitions(new_value.clone(), []);
    let reference_report = compare(&old, &new);
    assert_eq!(reference_report.json.result, Compatibility::Incompatible);
    assert_eq!(
        reference_report.native_storage.result,
        Compatibility::Compatible
    );
    assert_eq!(reference_report.lexicon.result, Compatibility::Incompatible);

    let branch = |value: Value| {
        json!({
            "type":"object",
            "properties":{
                "$type":{"const":"value"},
                "value":value
            },
            "required":["$type", "value"],
            "additionalProperties":false
        })
    };
    let old_union = json!({"oneOf":[branch(old_value)]});
    let new_union = json!({"oneOf":[branch(new_value)]});
    let union_report = compare_schemas(&old_union, &new_union);
    assert_eq!(union_report.json.result, Compatibility::Incompatible);
    assert_eq!(
        union_report.native_storage.result,
        Compatibility::Compatible
    );
    // A union file names the member id and drops the payload, so the payload's nullability is
    // that member's own lexicon, not a difference this union file can show.
    assert_eq!(union_report.lexicon.result, Compatibility::Compatible);
}

#[test]
fn canonical_bytes_are_the_protocol_object_and_an_encoding_annotation_is_not_bytes() {
    // `contentEncoding` annotates a string. It does not select storage, so a string carrying it
    // is a string in every dimension — sniffing it as bytes would report a string field
    // storage-compatible with a bytes field.
    let annotated = json!({"type":"string","contentEncoding":"base64"});
    assert_eq!(
        compare_schemas(&annotated, &json!({"type":"string"}))
            .json
            .result,
        Compatibility::Compatible
    );
    assert_eq!(
        compare_schemas(&annotated, &annotated)
            .native_storage
            .result,
        Compatibility::Compatible
    );

    let bytes = json!({
        "type":"object",
        "x-automerge":"bytes",
        "properties":{"$bytes":{"type":"string"}},
        "required":["$bytes"],
        "additionalProperties":false
    });
    assert_eq!(
        compare_schemas(&bytes, &bytes).lexicon.result,
        Compatibility::Compatible,
        "{:?}",
        compare_schemas(&bytes, &bytes).lexicon.diagnostics
    );
    // A plain number is the float opt-out, and it is not a Lexicon type.
    let plain_number = json!({"type":"number","x-automerge":"number"});
    assert_eq!(
        compare_schemas(&plain_number, &plain_number).lexicon.result,
        Compatibility::NotEstablished
    );
}

#[test]
fn lexicon_formats_nullable_unions_and_reference_identity_are_bounded() {
    let timestamp = json!({"type":"string","format":"date-time"});
    let uri = json!({"type":"string","format":"uri"});
    let unknown = json!({"type":"string","format":"made-up"});
    assert_eq!(
        compare_schemas(&timestamp, &uri).lexicon.result,
        Compatibility::Incompatible
    );
    // A format Lexicon does not know stays in the sidecar, so the published files still agree.
    assert_eq!(
        compare_schemas(&unknown, &unknown).lexicon.result,
        Compatibility::Compatible
    );

    // A nullable position is an object's `nullable` list, not a type array, so a type array is
    // not a publishable Lexicon.
    let nullable = json!({"type":["string","null"]});
    assert_eq!(
        compare_schemas(&nullable, &nullable).lexicon.result,
        Compatibility::NotEstablished
    );

    let scalar_union = json!({"oneOf":[{"type":"string"},{"type":"integer"}]});
    assert_eq!(
        compare_schemas(&scalar_union, &scalar_union).lexicon.result,
        Compatibility::NotEstablished
    );

    let old = SchemaBundle::with_definitions(
        json!({"$ref":"#/$defs/oldName"}),
        [("oldName".to_owned(), json!({"type":"string"}))],
    );
    let renamed = SchemaBundle::with_definitions(
        json!({"$ref":"#/$defs/newName"}),
        [("newName".to_owned(), json!({"type":"string"}))],
    );
    let renamed_report = compare(&old, &renamed);
    assert_eq!(renamed_report.json.result, Compatibility::Compatible);
    assert_eq!(
        renamed_report.native_storage.result,
        Compatibility::Compatible
    );
    assert_eq!(renamed_report.lexicon.result, Compatibility::Incompatible);
}

#[test]
fn a_timestamp_is_a_timestamp_and_a_unit_annotation_is_not_a_storage_kind() {
    // Nothing the crate emits states a unit. Two timestamps are the same storage kind; the unit
    // annotation is sidecar metadata and does not manufacture a native incompatibility.
    let timestamp_ms = json!({"type":"string","format":"date-time","unit":"milliseconds"});
    let timestamp_seconds = json!({"type":"string","format":"date-time","unit":"seconds"});
    assert_eq!(
        compare_schemas(&timestamp_ms, &timestamp_seconds)
            .native_storage
            .result,
        Compatibility::Compatible
    );

    let signed = json!({"type":"integer","format":"int64","minimum":-9007199254740991i64});
    let unsigned = json!({"type":"integer","format":"uint64","minimum":0});
    assert_eq!(
        compare_schemas(&signed, &unsigned).native_storage.result,
        Compatibility::Incompatible
    );
    assert_eq!(
        compare_schemas(&signed, &unsigned).json.result,
        Compatibility::Incompatible
    );

    let old = json!({"type":"array","items":{"type":"string"}});
    let new = json!({"type":"array","items":{"type":"integer"}});
    assert_eq!(
        compare_schemas(&old, &new).json.result,
        Compatibility::Incompatible
    );
}

#[test]
fn map_openness_and_null_missing_are_reported() {
    let open = json!({"type":"object","additionalProperties":true});
    let closed = json!({"type":"object","additionalProperties":false});
    assert_eq!(
        compare_schemas(&open, &closed).json.result,
        Compatibility::Incompatible
    );

    let optional = json!({"type":"object","properties":{"value":{"type":["string","null"]}},"additionalProperties":false});
    let required = json!({"type":"object","properties":{"value":{"type":["string","null"]}},"required":["value"],"additionalProperties":false});
    assert_eq!(
        compare_schemas(&optional, &required).json.result,
        Compatibility::Incompatible
    );
}

#[test]
#[should_panic(expected = "conflicting schemas supplied")]
fn conflicting_definition_constructor_does_not_last_win() {
    let _unused = SchemaBundle::with_definitions(
        json!({"type":"string"}),
        [
            ("Thing".to_owned(), json!({"type":"string"})),
            ("Thing".to_owned(), json!({"type":"integer"})),
        ],
    );
}

#[test]
fn datum_bundle_adapter_reports_all_three_dimensions_for_a_real_change() {
    let schema = |value| schemars::Schema::try_from(value).unwrap();
    let old_universe = crate::datum::Universe {
        datums: vec![crate::datum::DatumDecl {
            id: "org.example.root".to_owned(),
            name: "Root".to_owned(),
            schema: schema(
                json!({"type":"object","properties":{"value":{"type":"string"}},"required":["value"],"additionalProperties":false}),
            ),
            flags: Vec::new(),
        }],
        types: Vec::new(),
    };
    let new_universe = crate::datum::Universe {
        datums: vec![crate::datum::DatumDecl {
            id: "org.example.root".to_owned(),
            name: "Root".to_owned(),
            schema: schema(
                json!({"type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false}),
            ),
            flags: Vec::new(),
        }],
        types: Vec::new(),
    };
    let old_bundle = crate::datum::Bundle::new(&old_universe, Vec::new());
    let new_bundle = crate::datum::Bundle::new(&new_universe, Vec::new());
    let report = compare_datum_bundles(&old_bundle, "Root", &new_bundle, "Root").unwrap();
    assert_eq!(report.json.result, Compatibility::Incompatible);
    assert_eq!(report.native_storage.result, Compatibility::Incompatible);
    assert_eq!(report.lexicon.result, Compatibility::Incompatible);
}

#[test]
fn reference_backed_union_has_independent_json_native_and_lexicon_results() {
    let branch = |tag: Value| {
        json!({
            "type":"object",
            "properties":{"$type":tag,"value":{"type":"object","additionalProperties":true}},
            "required":["$type","value"]
        })
    };
    let old = SchemaBundle::with_definitions(
        json!({"oneOf":[{"$ref":"#/$defs/Known"},{"$ref":"#/$defs/Fallback"}]}),
        [
            ("Known".to_owned(), branch(json!({"const":"known"}))),
            (
                "Fallback".to_owned(),
                branch(json!({"type":"string","not":{"enum":["known"]}})),
            ),
        ],
    );
    let new = SchemaBundle::with_definitions(
        json!({"oneOf":[{"$ref":"#/$defs/Known"},{"$ref":"#/$defs/Future"},{"$ref":"#/$defs/Fallback"}]}),
        [
            ("Known".to_owned(), branch(json!({"const":"known"}))),
            ("Future".to_owned(), branch(json!({"const":"future"}))),
            (
                "Fallback".to_owned(),
                branch(json!({"type":"string","not":{"enum":["known","future"]}})),
            ),
        ],
    );
    let report = compare(&old, &new);
    assert_eq!(report.json.result, Compatibility::Compatible);
    assert_eq!(report.native_storage.result, Compatibility::NotEstablished);
    assert_eq!(report.lexicon.result, Compatibility::Compatible);
}

#[test]
fn ordinary_enum_const_and_stable_union_tags_have_distinct_identity() {
    let old_scalar = json!({"type":"string","enum":["old","other"],"const":"old"});
    let new_scalar = json!({"type":"string","enum":["new","other"],"const":"new"});
    let report = compare_schemas(&old_scalar, &new_scalar);
    assert_eq!(report.json.result, Compatibility::Incompatible);
    assert_eq!(report.native_storage.result, Compatibility::Compatible);
    assert_eq!(report.lexicon.result, Compatibility::Incompatible);

    let branch = |tag: &str| {
        json!({
            "type":"object",
            "properties":{"$type":{"const":tag},"value":{"type":"string"}},
            "required":["$type","value"]
        })
    };
    let old_union = json!({"oneOf":[branch("org.example.old")]});
    let new_union = json!({"oneOf":[branch("org.example.new")]});
    let report = compare_schemas(&old_union, &new_union);
    assert_eq!(report.json.result, Compatibility::Incompatible);
    assert_eq!(report.native_storage.result, Compatibility::Incompatible);
    assert_eq!(report.lexicon.result, Compatibility::Incompatible);
}

#[test]
fn malformed_markers_and_union_carriers_refuse_all_dimensions() {
    let malformed_marker = json!({"type":"integer","x-automerge":"bytes"});
    let report = compare_schemas(&malformed_marker, &malformed_marker);
    assert_eq!(report.json.result, Compatibility::NotEstablished);
    assert_eq!(report.native_storage.result, Compatibility::NotEstablished);
    assert_eq!(report.lexicon.result, Compatibility::NotEstablished);

    let dollar_property = json!({
        "type":"object",
        "properties":{"$private":{"type":"string"}},
        "additionalProperties":false
    });
    let report = compare_schemas(&dollar_property, &dollar_property);
    assert_eq!(report.json.result, Compatibility::Compatible);
    assert_eq!(report.native_storage.result, Compatibility::Compatible);
    assert_eq!(report.lexicon.result, Compatibility::NotEstablished);

    let branch = json!({
        "type":"object",
        "properties":{"$type":{"const":"same"},"value":{"type":"string"}},
        "required":["$type","value"]
    });
    let malformed = json!({"oneOf":[branch.clone(), branch]});
    let report = compare_schemas(&malformed, &malformed);
    assert_eq!(report.json.result, Compatibility::NotEstablished);
    assert_eq!(report.native_storage.result, Compatibility::NotEstablished);
    assert_eq!(report.lexicon.result, Compatibility::Compatible);
}
