//! What the storage codec has to get right: a value sits in a document the way its *schema* says,
//! whichever field holds it, and what a peer's document holds comes back as the JSON it was.

use super::{
    Bytes, Codec, CompiledSchema, Counter, Float64, Stored, Text, hydrate_value, reconcile_json,
    reconcile_json_atomic,
};

mod native;
use crate::facet::{Bundle, Declaration, Extra, Universe};
use automerge::{
    AutoCommit, Automerge, ObjType, ROOT, ReadDoc, ScalarValue, transaction::Transactable,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::borrow::Cow;

/// Every storage form in one object, reached through a reference so the walk has to resolve one.
#[macros::facet(id = "org.example.stored", closed)]
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Stored0 {
    bytes: Bytes,
    instant: jiff::Timestamp,
    counter: Counter,
    text: Text,
    #[schemars(extend("x-automerge" = "list"))]
    plain: Vec<u8>,
    many: Vec<Bytes>,
    fraction: Float64,
    maybe: Option<Float64>,
    // a bare `f64` derives a plain number; the opt-out is stated by the field, and the statement
    // is in the schema it derives
    #[schemars(extend("x-automerge" = "number"))]
    plain_number: f64,
}

/// The document a value of [`Stored0`] is written under, assembled the way a peer would take it.
fn document() -> Value {
    let universe = Universe::collect(vec![Declaration::of::<Stored0>().unwrap()]);
    let bundle = Bundle::new(&universe, Vec::new());
    serde_json::to_value(bundle.document("Stored0").unwrap()).unwrap()
}

fn compiled(document: &Value) -> CompiledSchema {
    CompiledSchema::compile(document.clone()).unwrap()
}

/// Supply the required members for focused tests that intentionally vary one member. The public
/// write boundary validates complete whole-facet values, so test fixtures must not accidentally use
/// omission as a shortcut when they are testing a nested refusal.
fn complete_stored(value: Value) -> Value {
    let mut complete = json!({
        "bytes": {"$bytes": "AQ=="},
        "instant": "2024-01-02T03:04:05Z",
        "counter": 0,
        "text": "",
        "plain": [],
        "many": [],
        "fraction": "0.0",
        "maybe": null,
        "plain_number": 0.0,
    });
    let Some(members) = value.as_object() else {
        return value;
    };
    for (key, member) in members {
        complete[key] = member.clone();
    }
    complete
}

/// A value with one of everything, and the same value as the JSON a peer exchanges.
fn value() -> (Stored0, Value) {
    let value = Stored0 {
        bytes: Bytes(vec![1, 2, 3, 4]),
        instant: "2024-01-02T03:04:05Z".parse().unwrap(),
        counter: Counter::new(7),
        text: Text::from("hello"),
        // the *same* Rust type as `bytes`, and a list because this field's marker says an array
        // of numbers is what it meant, on purpose
        plain: vec![1, 2, 3, 4],
        many: vec![Bytes(vec![9]), Bytes(vec![8, 7])],
        fraction: Float64(0.1),
        maybe: Some(Float64(30.0)),
        plain_number: 1.5,
    };
    let json = serde_json::to_value(&value).unwrap();
    (value, json)
}

/// The JSON a value writes to a document, which is the whole point of the codec: the document is not
/// How a document spells what it holds there, which is what the assertions are about: the JSON and
/// the document are two different things, and the codec is the difference.
fn shape(stored: &automerge::Value) -> &'static str {
    match stored {
        automerge::Value::Scalar(scalar) => match scalar.as_ref() {
            automerge::ScalarValue::Bytes(_) => "bytes",
            automerge::ScalarValue::Str(_) => "string",
            automerge::ScalarValue::Timestamp(_) => "timestamp",
            automerge::ScalarValue::Counter(_) => "counter",
            automerge::ScalarValue::Int(_) => "int",
            automerge::ScalarValue::Uint(_) => "uint",
            automerge::ScalarValue::F64(_) => "f64",
            other => panic!("{other:?}"),
        },
        automerge::Value::Object(ObjType::List) => "list",
        automerge::Value::Object(ObjType::Text) => "text",
        other => panic!("{other:?}"),
    }
}

/// Replace what one member of the facet is declared by. A document carries the facet's definition in
/// `$defs` and its root is a reference to it, so that is where a member is declared.
fn redeclare(document: &mut Value, member: &str, declared: Value) {
    let root = document["$ref"].as_str().expect("the root is a reference");
    let name = root
        .strip_prefix("#/$defs/")
        .expect("a reference into the definitions")
        .to_owned();
    document["$defs"][name]["properties"][member] = declared;
}

/// The key both directions address the value by: a document holds it under `facet`, and the walk
/// takes a key the way `autosurgeon` hands it one.
fn facet() -> autosurgeon::Prop<'static> {
    autosurgeon::Prop::Key(Cow::Borrowed("facet"))
}

/// The JSON a value writes to a document.
fn stored(json: &Value) -> AutoCommit {
    let mut doc = AutoCommit::new();
    let document = document();
    let stored = Stored::check(&compiled(&document), complete_stored(json.clone()))
        .expect("the value is what it declares");
    reconcile_json(&mut doc, &ROOT, facet(), &document, stored.into_value()).unwrap();
    doc
}

fn read(doc: &AutoCommit) -> Value {
    hydrate_value(doc, &ROOT, facet(), &compiled(&document()))
        .expect("what a declaration wrote, a declaration reads")
}

/// The five storage forms, each in the shape only the document has.
#[test]
fn atomic_reconcile_returns_the_only_commit_capability() {
    let schema = document();
    let compiled_schema = super::walk::CompiledSchema::compile(schema.clone()).unwrap();
    let mut doc = Automerge::new();
    let tx = doc.transaction();
    let tx = reconcile_json_atomic(
        tx,
        &compiled_schema,
        &ROOT,
        facet(),
        complete_stored(json!({
            "bytes": {"$bytes": "AQID"},
            "instant": "1970-01-01T00:00:00Z",
            "counter": 1,
            "text": "atomic",
            "plain": [1],
            "many": [],
            "fraction": "1",
            "maybe": null,
            "plain_number": 1.0
        })),
    )
    .unwrap();
    tx.commit();
    assert_eq!(
        hydrate_value(&doc, &ROOT, facet(), &compiled(&schema)).unwrap()["text"],
        "atomic"
    );
}

#[test]
fn atomic_reconcile_discards_pending_transaction_on_preflight_error() {
    let schema = document();
    let compiled_schema = CompiledSchema::compile(schema).unwrap();
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    tx.put(&ROOT, "pending", "must-disappear").unwrap();
    let error = reconcile_json_atomic(
        tx,
        &compiled_schema,
        &ROOT,
        facet(),
        complete_stored(json!({
            "bytes": "plain-base64-is-not-canonical",
            "instant": "1970-01-01T00:00:00Z",
            "counter": 1,
            "text": "atomic",
            "plain": [1],
            "many": [],
            "fraction": "1",
            "maybe": null,
            "plain_number": 1.0
        })),
    )
    .expect_err("invalid canonical input must fail before apply");
    assert!(error.to_string().contains("bytes"), "{error}");
    assert!(doc.get(&ROOT, "pending").unwrap().is_none());
}

#[test]
fn atomic_integer_write_commits_signed_and_unsigned_native_kinds() {
    for (schema, value, expected) in [
        (
            json!({"type": "integer", "format": "int64"}),
            json!(1),
            ScalarValue::Int(1),
        ),
        (
            json!({"type": "integer", "format": "uint64"}),
            json!(9_007_199_254_740_991u64),
            ScalarValue::Uint(9_007_199_254_740_991),
        ),
        (
            json!({
                "$ref": "#/$defs/NullableSigned",
                "$defs": {
                    "NullableSigned": {"type": ["integer", "null"], "format": "int64"}
                }
            }),
            Value::Null,
            ScalarValue::Null,
        ),
    ] {
        let compiled = CompiledSchema::compile(schema).unwrap();
        let mut doc = Automerge::new();
        let tx = doc.transaction();
        let tx = reconcile_json_atomic(tx, &compiled, &ROOT, "facet".into(), value).unwrap();
        tx.commit();
        let (stored, _) = doc
            .get(ROOT, "facet")
            .unwrap()
            .expect("integer was committed");
        assert!(matches!(stored, automerge::Value::Scalar(actual) if actual.as_ref() == &expected));
    }
}

#[test]
fn atomic_integer_fractional_tokens_refuse_without_panicking_or_mutating() {
    let schemas = [
        json!({
            "$ref": "#/$defs/NullableSigned",
            "$defs": {
                "NullableSigned": {"type": ["integer", "null"], "format": "int64"}
            }
        }),
        json!({
            "$ref": "#/$defs/NullableUnsigned",
            "$defs": {
                "NullableUnsigned": {"type": ["integer", "null"], "format": "uint64"}
            }
        }),
    ];
    for schema in schemas {
        let compiled = CompiledSchema::compile(schema).unwrap();
        for value in [json!(1.0), json!(-0.0)] {
            let mut doc = Automerge::new();
            let mut tx = doc.transaction();
            tx.put(&ROOT, "pending", true).unwrap();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                reconcile_json_atomic(tx, &compiled, &ROOT, "facet".into(), value)
            }));
            assert!(result.is_ok(), "integer refusal must not panic");
            let error = result
                .unwrap()
                .expect_err("non-integral token must be refused");
            assert!(error.to_string().contains("without losing"), "{error}");
            assert!(doc.get(&ROOT, "pending").unwrap().is_none());
            assert!(doc.get(&ROOT, "facet").unwrap().is_none());
        }
    }
}

#[test]
fn canonical_projection_is_idempotent_at_the_target_aware_boundary() {
    let (_, authored) = value();
    let first = stored(&authored);
    let canonical = read(&first);

    let document = document();
    let mut second = first.clone();
    second.commit();
    reconcile_json(&mut second, &ROOT, facet(), &document, canonical.clone())
        .expect("canonical projection is accepted again");
    assert_eq!(
        second.pending_ops(),
        0,
        "writing canonical projection again must be idempotent"
    );
    assert_eq!(read(&second), canonical);
}

#[test]
fn a_declared_position_is_stored_by_its_declaration() {
    let (_value, json) = value();
    let doc = stored(&json);

    let facet = doc
        .get(&ROOT, automerge::Prop::Map("facet".to_owned()))
        .unwrap()
        .unwrap()
        .1;
    let at = |key: &str| {
        let (stored, id) = doc
            .get(&facet, automerge::Prop::Map(key.to_owned()))
            .unwrap()
            .expect("the member is there");
        (stored, id)
    };

    assert_eq!(shape(&at("bytes").0), "bytes");
    assert_eq!(shape(&at("instant").0), "timestamp");
    assert_eq!(shape(&at("counter").0), "counter");
    assert_eq!(shape(&at("text").0), "text");
    assert_eq!(
        shape(&at("plain").0),
        "list",
        "the same Rust type as `bytes`, and a list because this field declares nothing"
    );
    assert_eq!(doc.text(at("text").1).unwrap(), "hello");

    // The array element's declaration is the element's own.
    let (_, many) = at("many");
    for (index, expected) in ["CQ==", "CAc="].iter().enumerate() {
        let (stored, _) = doc
            .get(&many, automerge::Prop::Seq(index))
            .unwrap()
            .unwrap();
        let automerge::Value::Scalar(scalar) = stored else {
            panic!("an element of a `Vec<Bytes>` is bytes")
        };
        let automerge::ScalarValue::Bytes(bytes) = scalar.as_ref() else {
            panic!("an element of a `Vec<Bytes>` is bytes")
        };
        assert_eq!(data_encoding::BASE64.encode(bytes), *expected);
    }
    assert_eq!(shape(&at("fraction").0), "f64");
    assert_eq!(shape(&at("maybe").0), "f64");
    assert_eq!(shape(&at("plain_number").0), "f64");
}

/// An instant is a millisecond in the document and an RFC 3339 string in JSON, and the precision the
/// document has is the precision a value keeps: `Timestamp::now()` does not round trip exactly, and
/// that is the storage's limit rather than a surprise.
#[test]
fn an_instant_keeps_the_precision_a_document_has() {
    assert_eq!(
        read(&stored(&json!({
            "bytes": {"$bytes": "AQ=="},
            "instant": "2024-01-02T03:04:05.123Z",
            "counter": 0,
            "text": "",
            "plain": [],
            "many": [],
        })))["instant"],
        json!("2024-01-02T03:04:05.123Z")
    );

    let finer = read(&stored(&json!({
        "bytes": {"$bytes": "AQ=="},
        "instant": "2024-01-02T03:04:05.123456789Z",
        "counter": 0,
        "text": "",
        "plain": [],
        "many": [],
    })));
    assert_eq!(finer["instant"], json!("2024-01-02T03:04:05.123Z"));
}

/// A document reads back as the JSON that was written to it, which is what makes the codec a codec
/// and not a translation with a lossy direction.
#[test]
fn what_a_declaration_wrote_a_declaration_reads() {
    let (_value, json) = value();
    // The read-back is the JSON the declaration spells, and a float's spelling is the
    // convention's now: the numbers a typed writer handed over read back as decimal strings.
    let spelled = {
        let mut spelled = json.clone();
        spelled["fraction"] = json!("0.1");
        spelled["maybe"] = json!("30.0");
        spelled
    };
    assert_eq!(read(&stored(&json)), spelled);
}

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
        Some(Codec::Bytes)
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

use autosurgeon::Hydrate as _;

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
