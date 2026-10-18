//! Schema-directed authoring, normalization, and hydration behavior.

use super::*;
use autosurgeon::Hydrate as _;

/// The whole point of the facility: the codec reads what the schema says, not how the field is
/// spelled. Two facets hold a field of the same name and the same Rust type, and the schema alone
/// decides that one is bytes and the other an array of numbers.
#[macros::facet(id = "org.example.named", closed)]
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Named {
    #[serde(rename = "vectorBase64")]
    vector: Bytes,
}

#[macros::facet(id = "org.example.unnamed", closed)]
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Unnamed {
    #[schemars(extend("x-automerge" = "list"))]
    #[serde(rename = "vectorBase64")]
    vector: Vec<u8>,
}

#[test]
fn the_schema_decides_and_the_spelling_of_a_field_does_not() {
    // Both fields carry the name the old heuristics keyed on, but each uses its declared
    // canonical JSON form. The schema, not the name, selects bytes versus a list.
    let declares = |declaration: Declaration, json: Value| -> &'static str {
        let name = declaration.facet.name.clone();
        let universe = Universe::collect(vec![declaration]);
        let bundle = Bundle::new(&universe, Vec::new());
        let document = serde_json::to_value(bundle.document(&name).unwrap()).unwrap();
        let mut doc = AutoCommit::new();
        let stored = Stored::check(&compiled(&document), json.clone()).unwrap();
        reconcile_json(&mut doc, &ROOT, facet(), &document, stored.into_value()).unwrap();
        let (_, facet) = doc
            .get(&ROOT, automerge::Prop::Map("facet".to_owned()))
            .unwrap()
            .unwrap();
        let (member, _) = doc
            .get(&facet, automerge::Prop::Map("vectorBase64".to_owned()))
            .unwrap()
            .unwrap();
        shape(&member)
    };

    assert_eq!(
        declares(
            Declaration::of::<Named>().unwrap(),
            json!({"vectorBase64": {"$bytes": "AQIDBA=="}}),
        ),
        "bytes",
        "bytes because the schema says so, whatever the field is called"
    );
    assert_eq!(
        declares(
            Declaration::of::<Unnamed>().unwrap(),
            json!({"vectorBase64": [1, 2, 3, 4]}),
        ),
        "list",
        "and a field that declares a list is the list serde authored"
    );
}

/// A declaration is an assertion, so a value that does not hold it is refused *before* anything is
/// written, and the refusal says where.
#[test]
fn a_value_the_declaration_cannot_hold_is_refused_with_its_path() {
    let cases = [
        (json!({"bytes": "not base64!"}), "$.bytes"),
        (json!({"bytes": 4}), "$.bytes"),
        (json!({"instant": "yesterday"}), "$.instant"),
        (json!({"instant": 1704164645}), "$.instant"),
        (json!({"counter": 1.5}), "$.counter"),
        (json!({"counter": "seven"}), "$.counter"),
        (json!({"text": 4}), "$.text"),
        (json!({"many": [{"a": 1}]}), "$.many[0]"),
    ];
    for (broken, expected) in cases {
        let mut json = json!({
            "bytes": {"$bytes": "AQIDBA=="},
            "instant": "2024-01-02T03:04:05Z",
            "counter": 7,
            "text": "hello",
            "plain": [],
            "many": [],
        });
        for (key, value) in broken.as_object().unwrap() {
            json[key] = value.clone();
        }
        let error = Stored::check(&compiled(&document()), complete_stored(json))
            .expect_err("a value the declaration cannot hold is refused");
        let error = error.to_string();
        let expected_pointer = expected
            .strip_prefix('$')
            .expect("test paths start at the root")
            .replace(['.', '['], "/")
            .replace(']', "");
        assert!(
            error.contains(&expected_pointer) || error.contains(expected),
            "the refusal points at the position: {error}"
        );
    }
}

/// A peer that knows a storage form this build does not is refused rather than read as a plain
/// value, because reading it as one would hand back a type the writer did not write.
#[test]
fn a_storage_form_this_build_does_not_have_is_refused() {
    let mut document = document();
    redeclare(
        &mut document,
        "bytes",
        json!({"type": "string", "x-automerge": "vectorClock"}),
    );
    let error = Stored::check(
        &compiled(&document),
        complete_stored(json!({ "bytes": {"$bytes": "AQ=="} })),
    )
    .expect_err("refused");
    assert!(error.to_string().contains("JSON Schema"), "{error}");

    assert_eq!(
        Codec::of(&json!({"x-automerge": "counter"})).unwrap(),
        Some(Codec::Counter)
    );
    assert_eq!(
        Codec::of(&json!({"contentEncoding": "base64"})).unwrap(),
        None
    );
    assert_eq!(
        Codec::of(&json!({"format": "date-time"})).unwrap(),
        Some(Codec::Timestamp)
    );
    assert_eq!(Codec::of(&json!({"format": "email"})).unwrap(), None);
}

/// The two directions of a text: an editor splices what it hydrated and reconciles the whole value,
/// and a document that holds the edits reads back as the text they produced.
#[test]
fn an_edited_text_is_the_text_the_document_holds() {
    let mut doc = AutoCommit::new();
    doc.put_object(ROOT, "facet", ObjType::Map).unwrap();
    let mut edited = Text::from("hello");
    edited.0.splice(5, 0, " there");
    autosurgeon::reconcile_prop(&mut doc, ROOT, facet(), &edited).unwrap();

    let mut document = json!({"type": "object", "properties": {"facet": {"type": "string", "x-automerge": "text"}}});
    document["$defs"] = json!({});
    assert_eq!(
        hydrate_value(&doc, &ROOT, facet(), &compiled(&document)).unwrap(),
        json!("hello there")
    );
}

/// A position whose declaration states a plain kind but which holds a specialized native value
/// cannot say what it holds as: the type would have to be read differently from what a conformant
/// reader gets, and no schemaless spelling can fix what a declaration contradicts.
#[test]
fn a_specialized_native_at_a_plain_declaration_is_refused() {
    let mut doc = AutoCommit::new();
    doc.put(ROOT, "facet", automerge::ScalarValue::Timestamp(0))
        .unwrap();
    let document = json!({"type": "object", "properties": {"facet": {"type": "string"}}});
    let error = hydrate_value(&doc, &ROOT, facet(), &compiled(&document))
        .expect_err("an instant at a position declared a string is refused");
    assert!(error.to_string().contains("an instant"), "{error}");

    // A scalar a future Automerge wrote has no reader of its own in this build: what reaches the
    // codec's edge is the payload as Automerge renders it here, bytes. Pin that rendering, because
    // the raw fallback spellings are defined on it, not on an interpretation no build has.
    let mut doc = AutoCommit::new();
    let obj = doc.put_object(ROOT, "facet", ObjType::Map).unwrap();
    doc.put(
        &obj,
        "future",
        automerge::ScalarValue::Unknown {
            type_code: 7,
            bytes: vec![1, 2],
        },
    )
    .unwrap();
    let (value, _) = doc.get(&obj, "future").unwrap().unwrap();
    assert!(
        matches!(
            value,
            automerge::Value::Scalar(ref scalar) if matches!(scalar.as_ref(), automerge::ScalarValue::Bytes(_)),
        ),
        "this build renders a future Automerge's scalar as bytes: {value:?}",
    );
}

/// An open object carries what it does not name, and the codec walks it as plain JSON: the members
/// the schema does not declare have no declaration to follow.
#[test]
fn a_member_the_schema_does_not_name_is_plain_json() {
    #[macros::facet(id = "org.example.open")]
    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    struct Open {
        named: Bytes,
        #[serde(flatten)]
        extra: Extra,
    }

    let universe = Universe::collect(vec![Declaration::of::<Open>().unwrap()]);
    let bundle = Bundle::new(&universe, Vec::new());
    let document = serde_json::to_value(bundle.document("Open").unwrap()).unwrap();
    let json = json!({
        "named": {"$bytes": "AQ=="},
        "carried": {"nested": [1, 2, {"deep": true}]},
    });
    let mut doc = AutoCommit::new();
    let stored = Stored::check(&compiled(&document), json.clone()).unwrap();
    reconcile_json(&mut doc, &ROOT, facet(), &document, stored.into_value()).unwrap();
    assert_eq!(
        hydrate_value(&doc, &ROOT, facet(), &compiled(&document)).unwrap(),
        json,
        "the member nobody declared survives the round trip whole"
    );
}

/// A reference is resolved in the document it is carried with, and one that names nothing is refused
/// rather than walked as plain JSON.
#[test]
fn a_reference_that_the_document_does_not_carry_is_refused() {
    let mut document = document();
    redeclare(&mut document, "bytes", json!({"$ref": "#/$defs/Absent"}));
    let error =
        CompiledSchema::compile(document).expect_err("unresolved carried reference refused");
    assert!(error.to_string().contains("Absent"), "{error}");
}

/// The numbers a JSON document holds, with the widths their schema declares.
#[derive(Debug, autosurgeon::Hydrate)]
struct Numbers {
    signed: i64,
    unsigned: u64,
    float: f32,
}

/// A number is stored as the schema declares it and not as the token looks. JSON has a single number
/// type, so a peer sending `1` for a float — or for an `i64` — would otherwise leave a `Uint`, which
/// no float and no signed integer can be read back out of, while the JSON read still looks perfect.
/// This is the one case where the value and the schema disagree about nothing at all and the schema
/// still has to win.
#[test]
fn a_number_is_stored_as_the_schema_declares_it() {
    let document = json!({
        "type": "object",
        "$defs": {},
        "properties": {
            "signed": {"type": "integer", "format": "int64"},
            "unsigned": {"type": "integer", "format": "uint64"},
            "float": {"type": "number", "format": "float"},
        },
    });
    let mut doc = AutoCommit::new();
    let stored = Stored::check(
        &compiled(&document),
        json!({"signed": 1, "unsigned": 1, "float": 1}),
    )
    .unwrap();
    reconcile_json(&mut doc, &ROOT, facet(), &document, stored.into_value()).unwrap();

    let (_, obj) = doc
        .get(&ROOT, automerge::Prop::Map("facet".to_owned()))
        .unwrap()
        .unwrap();
    let held = |key: &str| {
        let (held, _) = doc
            .get(&obj, automerge::Prop::Map(key.to_owned()))
            .unwrap()
            .unwrap();
        shape(&held)
    };
    assert_eq!(held("signed"), "int", "an `i64` is read from `Int` only");
    assert_eq!(held("unsigned"), "uint", "and a `u64` from `Uint`");
    assert_eq!(
        held("float"),
        "f64",
        "a float is an `F64`, whatever token arrived"
    );

    // The typed read is what proves it: this is the read that used to fail with `Unexpected(Uint)`.
    let typed = Numbers::hydrate(&doc, &ROOT, facet()).expect("a declared number reads back");
    assert_eq!((typed.signed, typed.unsigned, typed.float), (1, 1, 1.0));
}

/// The same rule where a schema writes the type differently: `Option<i64>` is a list of types, a bare
/// `type: integer` states no sign and is read as signed because that type includes the negatives, and
/// a position that declares no type at all is left to the token.
#[test]
fn a_number_type_written_as_a_list_or_without_a_width_is_still_read() {
    let document = json!({
        "type": "object",
        "$defs": {},
        "properties": {
            "maybe": {"type": ["integer", "null"], "format": "int64"},
            "bare": {"type": "integer"},
            "undeclared": {},
        },
    });
    let mut doc = AutoCommit::new();
    let stored = Stored::check(
        &compiled(&document),
        json!({"maybe": 1, "bare": 1, "undeclared": 1}),
    )
    .unwrap();
    reconcile_json(&mut doc, &ROOT, facet(), &document, stored.into_value()).unwrap();

    let (_, obj) = doc
        .get(&ROOT, automerge::Prop::Map("facet".to_owned()))
        .unwrap()
        .unwrap();
    let held = |key: &str| {
        let (held, _) = doc
            .get(&obj, automerge::Prop::Map(key.to_owned()))
            .unwrap()
            .unwrap();
        shape(&held)
    };
    assert_eq!(
        held("maybe"),
        "int",
        "a nullable integer is written as a list of types"
    );
    assert_eq!(held("bare"), "int", "and a bare integer is signed");
    assert_eq!(
        held("undeclared"),
        "uint",
        "while nothing declared leaves the token to decide"
    );
}

/// `f32` and `f64` are the same thing in a document — one `F64` — so the JSON number is the only
/// place their precision is stated, and serde writes a float at the precision of its own type. A
/// value that was an `f32` therefore survives a JSON document unchanged, and an `f64` keeps every
/// digit it had. That is the opt-out, which keeps a plain JSON number; the convention's decimal
/// string is what a declared float position reads back as, and its tests are below it.
#[test]
fn a_float_union_ref_normalizes_before_owned_write() {
    let schema = json!({
        "$ref": "#/$defs/Float",
        "$defs": {
            "Float": {"type": "string", "x-automerge": "float", "format": "double"}
        }
    });
    let compiled = CompiledSchema::compile(schema.clone()).unwrap();
    let mut doc = Automerge::new();
    let tx = doc.transaction();
    let tx = reconcile_json_atomic(tx, &compiled, &ROOT, "facet".into(), json!(1.25)).unwrap();
    tx.commit();
    assert_eq!(
        hydrate_value(&doc, &ROOT, "facet".into(), &compiled).unwrap(),
        json!("1.25")
    );
}

/// A keyword's value is either a schema or instance data. `const`, `enum`, `default` and `examples`
/// hold values, so a literal that happens to contain a key named `allOf` or `$ref` is an author's
/// data and must not be read as a composition or a reference: refusing it would reject a legitimate
/// default, and walking it as schema would validate data as a document.
#[test]
fn literal_data_keys_are_not_read_as_schema_positions() {
    // `const` holding an `allOf` object: the compile refusal below is keyed on a schema position,
    // so compiling this document proves the literal was read as data. The stock validator enforces
    // the const, so the accepted value carries it exactly.
    let document = json!({
        "type": "object",
        "properties": {
            "value": {
                "const": {"allOf": [{"type": "string"}]},
                "default": {"prefixItems": []},
                "examples": [{"properties": {"Absent": {"type": "number"}}}]
            }
        },
        "required": ["value"]
    });
    let compiled = CompiledSchema::compile(document).expect("a literal is data, not schema");
    let value = json!({"value": {"allOf": [{"type": "string"}]}});
    Stored::check(&compiled, value).expect("the value is what the literal states");

    // `enum` literals are data the same way: a member holding a `$ref` key is one of the listed
    // values, not a reference the compile would have to resolve.
    let enumerated = json!({
        "enum": [{"allOf": []}, {"$ref": "#/$defs/Absent"}]
    });
    CompiledSchema::compile(enumerated).expect("an enum literal is data, not schema");
}

#[test]
fn authoring_normalization_is_stack_safe_for_wide_values_and_bounded_for_ambiguity() {
    assert!(CompiledSchema::compile(json!({"allOf": [{"type": "string"}]})).is_err());
    let wide_schema = json!({
        "type": "object",
        "additionalProperties": {
            "type": "array",
            "items": {"type": "string", "x-automerge": "float", "format": "double"}
        }
    });
    let mut wide_members = serde_json::Map::new();
    for map_index in 0..3_000 {
        wide_members.insert(format!("map_{map_index}"), json!([1.25]));
    }
    let wide = Stored::check(&compiled(&wide_schema), Value::Object(wide_members))
        .expect("wide authored values normalize without sibling stack growth")
        .into_value();
    assert_eq!(wide["map_0"][0], json!("1.25"));
    assert_eq!(wide["map_2999"][0], json!("1.25"));

    let ambiguous_schema = json!({
        "type": "object",
        "additionalProperties": {
            "anyOf": [
                {"type": "string", "x-automerge": "float", "format": "double"},
                {"type": "string", "x-automerge": "float", "format": "float"}
            ]
        }
    });
    let first = Stored::check(&compiled(&ambiguous_schema), json!({"value": 1.25}))
        .unwrap_err()
        .to_string();
    let second = Stored::check(&compiled(&ambiguous_schema), json!({"value": 1.25}))
        .unwrap_err()
        .to_string();
    assert_eq!(first, second, "ambiguous refusal is deterministic");
    assert!(first.contains("ambiguous carried branches"), "{first}");

    let constrained_schema = json!({
        "type": "object",
        "additionalProperties": {
            "anyOf": [
                {"type": "string", "x-automerge": "float", "format": "double", "pattern": "^1\\.25$"},
                {"type": "string", "x-automerge": "float", "format": "float", "pattern": "^2$"}
            ]
        }
    });
    let constrained = Stored::check(&compiled(&constrained_schema), json!({"value": 1.25}))
        .expect("branch constraints resolve an authored value uniquely")
        .into_value();
    assert_eq!(constrained["value"], json!("1.25"));
}

#[test]
fn authored_nested_union_dispatch_validates_objects_arrays_and_carried_container_refs() {
    let object_branch_a = json!({
        "type": "object",
        "properties": {
            "kind": {"const": "a"},
            "value": {"type": "string", "x-automerge": "float", "format": "double"}
        },
        "required": ["kind", "value"]
    });
    let object_branch_b = json!({
        "type": "object",
        "properties": {
            "kind": {"const": "b"},
            "value": {"type": "string", "x-automerge": "float", "format": "float"}
        },
        "required": ["kind", "value"]
    });
    let object_schema = json!({"anyOf": [object_branch_b, object_branch_a]});
    let object = Stored::check(
        &compiled(&object_schema),
        json!({"kind": "a", "value": 1.25}),
    )
    .expect("object constraints select one branch regardless of branch order")
    .into_value();
    assert_eq!(object, json!({"kind": "a", "value": "1.25"}));

    let array_schema = json!({
        "anyOf": [
            {"type": "array", "items": {"type": "string", "x-automerge": "float", "format": "double"}, "minItems": 2},
            {"type": "array", "items": {"type": "string", "x-automerge": "float", "format": "double"}, "minItems": 1, "maxItems": 1}
        ]
    });
    let array = Stored::check(&compiled(&array_schema), json!([1.25]))
        .expect("array length constraints select one normalized branch")
        .into_value();
    assert_eq!(array, json!(["1.25"]));

    let carried_schema = json!({
        "anyOf": [
            {"$ref": "#/$defs/B"},
            {"$ref": "#/$defs/A"}
        ],
        "$defs": {
            "A": {
                "type": "object",
                "properties": {
                    "tag": {"const": "a"},
                    "value": {"type": "string", "x-automerge": "float", "format": "double"}
                },
                "required": ["tag", "value"]
            },
            "B": {
                "type": "object",
                "properties": {
                    "tag": {"const": "b"},
                    "value": {"type": "string", "x-automerge": "float", "format": "double"}
                },
                "required": ["tag", "value"]
            }
        }
    });
    let carried = Stored::check(
        &compiled(&carried_schema),
        json!({"tag": "a", "value": 1.25}),
    )
    .expect("carried container references use their nested constraints")
    .into_value();
    assert_eq!(carried, json!({"tag": "a", "value": "1.25"}));

    let canonical = Stored::check(
        &compiled(&object_schema),
        json!({"kind": "a", "value": "1.25"}),
    )
    .expect("canonical valid values remain unchanged")
    .into_value();
    assert_eq!(canonical, json!({"kind": "a", "value": "1.25"}));

    let nested_ambiguous_schema = json!({
        "anyOf": [
            {"type": "object", "properties": {"value": {"type": "string", "x-automerge": "float", "format": "double"}}, "required": ["value"]},
            {"type": "object", "properties": {"value": {"type": "string", "x-automerge": "float", "format": "double"}}, "required": ["value"]}
        ]
    });
    let error = Stored::check(&compiled(&nested_ambiguous_schema), json!({"value": 1.25}))
        .expect_err("nested branch ambiguity must not pick the first branch");
    assert!(
        error.to_string().contains("ambiguous carried branches"),
        "{error}"
    );
}

#[test]
fn normalization_depth_preflight_covers_canonical_values_and_wide_canonical_values() {
    let schema = json!({});
    let mut value = json!(1);
    for _ in 0..1_025 {
        value = Value::Array(vec![value]);
    }
    let error = Stored::check(&compiled(&schema), value)
        .expect_err("depth guard must precede validator fast path");
    assert!(
        error.to_string().contains("deterministic resource budget"),
        "{error}"
    );

    let wide_schema = json!({"type": "object", "additionalProperties": {"type": "string"}});
    let mut members = serde_json::Map::new();
    for index in 0..3_000 {
        members.insert(format!("member_{index}"), json!("canonical"));
    }
    let stored = Stored::check(&compiled(&wide_schema), Value::Object(members))
        .expect("wide canonical values remain within the preflight budget");
    assert_eq!(stored.value()["member_2999"], json!("canonical"));
}

#[test]
fn a_float_round_trips_at_the_precision_its_json_had() {
    let document = json!({
        "type": "object",
        "$defs": {},
        "properties": {
            "single": {"type": "number", "format": "float"},
            "double": {"type": "number", "format": "double"},
        },
    });
    let value = json!({"single": 0.1f32, "double": 0.1f64});
    let mut doc = AutoCommit::new();
    let stored = Stored::check(&compiled(&document), value.clone()).unwrap();
    reconcile_json(&mut doc, &ROOT, facet(), &document, stored.into_value()).unwrap();
    assert_eq!(
        hydrate_value(&doc, &ROOT, facet(), &compiled(&document)).unwrap(),
        value
    );
    assert_eq!(
        serde_json::from_value::<f32>(value["single"].clone()).unwrap(),
        0.1f32,
        "a json number round trips an f32 exactly, which is what serde's own precision buys"
    );
}

/// A document can hold a number JSON has no spelling for. That is a refusal and not a panic or a
/// rounded value: `NaN` is a value, and it is not one JSON can carry.
#[test]
fn a_number_json_cannot_state_is_refused() {
    // And it never reaches a JSON document in the first place: serde writes a non-finite float as
    // `null`, so a `Some(NaN)` in an `Option<f32>` reads back as `None` with nothing to notice. That
    // happens before any codec sees the value, which is why the codec cannot refuse it and why a
    // float that has to hold a non-finite value needs a type that spells it out.
    assert_eq!(serde_json::to_string(&f64::NAN).unwrap(), "null");

    let mut doc = AutoCommit::new();
    doc.put(ROOT, "facet", automerge::ScalarValue::F64(f64::NAN))
        .unwrap();
    let document = json!({"type": "object", "properties": {"facet": {"type": "number"}}});
    let error = hydrate_value(&doc, &ROOT, facet(), &compiled(&document))
        .expect_err("NaN has no JSON form");
    assert!(error.to_string().contains("NaN"), "{error}");
}

/// A declaration's float is carried as the string the convention spells, and the opt-out keeps the
/// plain number: the carried schema is what says so, and that is all a reader without the types has.
#[test]
fn a_float_position_is_carried_as_the_string_the_convention_spells() {
    let document = document();
    let facet = &document["$defs"]["Stored0"]["properties"];

    // The convention: the number's `type` is carried as the string a float is spelled in, under
    // the keyword, and the width's `format` stays beside it.
    assert_eq!(facet["fraction"]["type"], "string");
    assert_eq!(facet["fraction"]["format"], "double");
    assert_eq!(facet["fraction"]["x-automerge"], "float");
    // The `null` of an option is not a float and is not spelled one.
    assert_eq!(facet["maybe"]["type"], json!(["string", "null"]));
    assert_eq!(facet["maybe"]["format"], "double");
    // A bare `f64` is the opt-out, stated by the field's marker, and the statement survives into
    // the schema the type derives: a plain number, named as one.
    assert_eq!(facet["plain_number"]["type"], "number");
    assert_eq!(facet["plain_number"]["x-automerge"], "number");
    assert!(
        matches!(
            Declaration::of::<Stored0>(),
            Err(crate::facet::ShapeError::NotLexiconAligned { .. })
        ),
        "a plain number is not a lexicon type"
    );
}

/// A float position accepts the number a typed writer hands it and reads back the decimal string
/// the convention spells; a string carried by a reader of the convention sits, and a position that
/// states `f32` is spelled at the width it says.
#[test]
fn a_float_travels_as_a_decimal_string_and_a_number_arrives_too() {
    let mut document = document();
    redeclare(
        &mut document,
        "maybe",
        json!({"type": "string", "x-automerge": "float", "format": "float"}),
    );

    // What a typed writer hands over is a number, and what the convention carries is a string:
    // both are the value the document holds, and the projection spells it.
    let write = |json: Value| {
        let mut doc = AutoCommit::new();
        let stored = Stored::check(&compiled(&document), complete_stored(json)).unwrap();
        reconcile_json(&mut doc, &ROOT, facet(), &document, stored.into_value()).unwrap();
        doc
    };
    let doc = write(json!({"maybe": 0.1}));
    let projected = hydrate_value(&doc, &ROOT, facet(), &compiled(&document)).unwrap();
    assert_eq!(projected["maybe"], json!("0.1"), "spelled at the f32 width");

    let doc = write(json!({"maybe": "0.1"}));
    let projected = hydrate_value(&doc, &ROOT, facet(), &compiled(&document)).unwrap();
    assert_eq!(projected["maybe"], json!("0.1"), "kept as it was spelled");

    // A whole number keeps the point that says it is not an integer.
    let doc = write(json!({"maybe": 30.0}));
    let projected = hydrate_value(&doc, &ROOT, facet(), &compiled(&document)).unwrap();
    assert_eq!(projected["maybe"], json!("30.0"));
}

/// A float that is also a whole number keeps its point in the convention's spelling, and the
/// opt-out is the spelling a canonicalizing encoder destroys: an encoder that spells `1.0` as `1`
/// (RFC 8949's preferred serialisation) still carries the string unchanged.
#[test]
fn a_float_keeps_its_floatness_through_a_canonicalizing_encoder() {
    let canonical = |value: &Value| match value.as_f64() {
        Some(float) if float.trunc() == float => json!(float as i64),
        _ => value.clone(),
    };

    let projected = read(&stored(&json!({"fraction": 30.0})));
    assert_eq!(canonical(&projected["fraction"]), projected["fraction"]);

    let projected = read(&stored(&json!({"plain_number": 30.0})));
    assert_eq!(canonical(&projected["plain_number"]), json!(30));
}

/// The three values JSON cannot state travel by their names at a position the convention spells,
/// where a number would have been written `null` and read back as `None` with nothing to notice.
#[test]
fn the_non_finite_values_travel_by_their_names() {
    let mut document = document();
    redeclare(
        &mut document,
        "fraction",
        json!({"type": "string", "x-automerge": "float", "format": "double"}),
    );

    // The name a reader gets parses to the exact value the document held, which is what makes the
    // spelling a round trip and not a rendering.
    for (spelled, expected) in [
        ("NaN", f64::NAN),
        ("Infinity", f64::INFINITY),
        ("-Infinity", f64::NEG_INFINITY),
    ] {
        let mut doc = AutoCommit::new();
        let stored = Stored::check(
            &compiled(&document),
            complete_stored(json!({"fraction": spelled})),
        )
        .unwrap();
        reconcile_json(&mut doc, &ROOT, facet(), &document, stored.into_value()).unwrap();
        let projected = hydrate_value(&doc, &ROOT, facet(), &compiled(&document)).unwrap();
        assert_eq!(projected["fraction"], json!(spelled));
        let round: f64 = projected["fraction"]
            .as_str()
            .expect("the convention spells a float as a string")
            .parse()
            .expect("the name a reader gets parses back");
        assert_eq!(round.to_bits(), expected.to_bits(), "the name parses back");
    }
}

/// A spelling that does not name a number is a refusal, not a string that hides.
#[test]
fn a_float_position_refuses_a_value_that_does_not_spell_a_number() {
    let mut document = document();
    redeclare(
        &mut document,
        "fraction",
        json!({"type": "string", "x-automerge": "float"}),
    );

    let cases = [
        (json!({"fraction": "not a number"}), "not a number"),
        (json!({"fraction": true}), "true"),
    ];
    for (broken, stated) in cases {
        let error = Stored::check(&compiled(&document), complete_stored(broken))
            .expect_err("a value that does not spell a number is refused");
        assert!(
            error.to_string().contains(stated),
            "the refusal says what it found: {error}"
        );
    }
}
/// A bare `f64` derives a plain number, and storing a scalar the Rust type cannot read is refused at
/// the check — the edge where a peer's data arrives — rather than passed into the document.
#[test]
fn a_non_number_at_a_plain_number_position_is_refused() {
    let cases = [
        json!({"plain_number": "0.1"}),
        json!({"plain_number": "not a number"}),
        json!({"plain_number": true}),
        json!({"plain_number": [1.0]}),
    ];
    for broken in cases {
        let error = Stored::check(&compiled(&document()), complete_stored(broken))
            .expect_err("a plain number's position is a number's");
        let error = error.to_string();
        assert!(
            error.contains("plain_number"),
            "the refusal points at the position: {error}"
        );
    }
}
/// A consumer can also hold the document itself — the writer's side does — and then the typed read
/// is the type's own `Reconcile`/`Hydrate` impls: a plain `f64` hydrates the document exactly and
/// a `None` round trips. Most consumers hold only the JSON the repo emits, and their path is the
/// next test along.
#[test]
fn a_consumer_that_holds_the_document_hydrates_it_typed() {
    #[derive(
        Debug,
        PartialEq,
        serde::Serialize,
        serde::Deserialize,
        schemars::JsonSchema,
        autosurgeon::Reconcile,
        autosurgeon::Hydrate,
    )]
    struct Plain {
        fraction: f64,
        maybe: Option<f64>,
        listed: Vec<f64>,
        bytes: Bytes,
        counter: Counter,
    }

    let document = json!({
        "type": "object",
        "$defs": {},
        "properties": {
            "fraction": {"type": "string", "x-automerge": "float"},
            "maybe": {"type": "string", "x-automerge": "float"},
            "listed": {"type": "array", "items": {"type": "string", "x-automerge": "float"}},
            "bytes": {"type": "object", "x-automerge": "bytes", "properties": {"$bytes": {"type": "string", "contentEncoding": "base64"}}, "required": ["$bytes"], "additionalProperties": false},
            "counter": {"x-automerge": "counter"},
        },
    });

    let json = json!({
        "fraction": 0.1,
        "maybe": 30.0,
        "listed": [0.1, 1.5],
        "bytes": {"$bytes": "AQIDBA=="},
        "counter": 7,
    });
    let mut doc = AutoCommit::new();
    let stored = Stored::check(&compiled(&document), complete_stored(json)).unwrap();
    reconcile_json(&mut doc, &ROOT, facet(), &document, stored.into_value()).unwrap();

    // The typed read is the document's, and a typed `f64` is not a string anywhere on it.
    let plain: Plain = autosurgeon::hydrate_prop(&doc, &ROOT, facet()).unwrap();
    assert_eq!(plain.fraction, 0.1);
    assert_eq!(plain.maybe, Some(30.0));
    assert_eq!(plain.listed, vec![0.1, 1.5]);
    assert_eq!(plain.bytes, Bytes(vec![1, 2, 3, 4]));
    assert_eq!(plain.counter, Counter::new(7));

    // The same two models, side by side: the projection says the strings the convention spells,
    // the typed read says the floats.
    let projected = hydrate_value(&doc, &ROOT, facet(), &compiled(&document)).unwrap();
    assert_eq!(projected["fraction"], json!("0.1"));
    assert_eq!(plain.fraction, 0.1);
}

/// The document repo emits one JSON shape, and a consumer deserializes it with the Rust type it
/// declared — plain serde, no schema fetched, no verb. This is the whole read contract.
#[test]
fn a_rust_type_deserializes_the_json_the_document_repo_emits() {
    #[derive(Debug, PartialEq, serde::Deserialize)]
    struct Plain {
        fraction: Float64,
        maybe: Option<Float64>,
        plain_number: f64,
    }

    let json = json!({
        "fraction": 0.1,
        "maybe": null,
        "plain_number": 2.5,
    });
    let document = document();
    let mut doc = AutoCommit::new();
    let stored = Stored::check(&compiled(&document), complete_stored(json)).unwrap();
    reconcile_json(&mut doc, &ROOT, facet(), &document, stored.into_value()).unwrap();

    // What the repo emits is what the type's Deserialize reads: the convention's strings at a
    // declared float, the plain number at a bare one, and none.
    let emitted = hydrate_value(&doc, &ROOT, facet(), &compiled(&document)).unwrap();
    assert_eq!(emitted["fraction"], json!("0.1"));
    assert_eq!(emitted["maybe"], Value::Null);
    assert_eq!(emitted["plain_number"], json!(2.5));

    let value: Plain =
        serde_json::from_value(emitted).expect("the emitted JSON is what serde expects");
    assert_eq!(value.fraction, Float64(0.1));
    assert!(value.maybe.is_none());
    assert_eq!(value.plain_number, 2.5);
}

#[macros::facet(id = "org.example.nullable", closed)]
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct NullableCodecs {
    bytes: Option<Bytes>,
    counter: Option<Counter>,
    text: Option<Text>,
}

#[test]
fn optional_storage_forms_stay_nullable_and_round_trip() {
    let universe = Universe::collect(vec![Declaration::of::<NullableCodecs>().unwrap()]);
    let bundle = Bundle::new(&universe, Vec::new());
    let document = serde_json::to_value(bundle.document("NullableCodecs").unwrap()).unwrap();
    let props = &document["$defs"]["NullableCodecs"]["properties"];
    assert_eq!(props["bytes"]["type"], json!(["object", "null"]));
    assert_eq!(props["bytes"]["x-automerge"], "bytes");
    assert_eq!(props["counter"]["type"], json!(["integer", "null"]));
    assert_eq!(props["counter"]["x-automerge"], "counter");
    assert_eq!(props["text"]["type"], json!(["string", "null"]));
    assert_eq!(props["text"]["x-automerge"], "text");

    let json = json!({
        "bytes": {"$bytes": "AQID"},
        "counter": 3,
        "text": "hi",
    });
    let mut doc = AutoCommit::new();
    let stored = Stored::check(&compiled(&document), json.clone()).unwrap();
    reconcile_json(&mut doc, &ROOT, facet(), &document, stored.into_value()).unwrap();
    assert_eq!(
        hydrate_value(&doc, &ROOT, facet(), &compiled(&document)).unwrap(),
        json
    );
    let (_, map) = doc.get(ROOT, "facet").unwrap().unwrap();
    assert_eq!(shape(&doc.get(&map, "bytes").unwrap().unwrap().0), "bytes");
    assert_eq!(
        shape(&doc.get(&map, "counter").unwrap().unwrap().0),
        "counter"
    );
    assert_eq!(shape(&doc.get(&map, "text").unwrap().unwrap().0), "text");

    let absent = json!({"bytes": null, "counter": null, "text": null});
    let mut doc = AutoCommit::new();
    let stored = Stored::check(&compiled(&document), absent.clone()).unwrap();
    reconcile_json(&mut doc, &ROOT, facet(), &document, stored.into_value()).unwrap();
    assert_eq!(
        hydrate_value(&doc, &ROOT, facet(), &compiled(&document)).unwrap(),
        absent
    );
}
