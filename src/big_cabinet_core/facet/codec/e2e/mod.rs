//! What the storage codec has to get right: a value sits in a document the way its *schema* says,
//! whichever field holds it, and what a peer's document holds comes back as the JSON it was.

use super::{
    Bytes, Codec, CompiledSchema, Counter, Float64, Stored, Text, hydrate_value, reconcile_json,
    reconcile_json_atomic,
};

mod atomic;
mod authoring;
mod native;
mod wire;
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
