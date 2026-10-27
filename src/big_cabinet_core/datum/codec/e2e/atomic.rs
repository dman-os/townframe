//! Atomic writes and native scalar/storage boundaries.

use super::*;

/// The five storage forms, each in the shape only the document has.
#[test]
fn atomic_reconcile_returns_the_only_commit_capability() {
    let schema = document();
    let compiled_schema = CompiledSchema::compile(schema.clone()).unwrap();
    let mut doc = Automerge::new();
    let tx = doc.transaction();
    let tx = reconcile_json_atomic(
        tx,
        &compiled_schema,
        &ROOT,
        datum(),
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
        hydrate_value(&doc, &ROOT, datum(), &compiled(&schema)).unwrap()["text"],
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
        datum(),
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
        let tx = reconcile_json_atomic(tx, &compiled, &ROOT, "datum".into(), value).unwrap();
        tx.commit();
        let (stored, _) = doc
            .get(ROOT, "datum")
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
                reconcile_json_atomic(tx, &compiled, &ROOT, "datum".into(), value)
            }));
            assert!(result.is_ok(), "integer refusal must not panic");
            let error = result
                .unwrap()
                .expect_err("non-integral token must be refused");
            assert!(error.to_string().contains("without losing"), "{error}");
            assert!(doc.get(&ROOT, "pending").unwrap().is_none());
            assert!(doc.get(&ROOT, "datum").unwrap().is_none());
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
    reconcile_json(&mut second, &ROOT, datum(), &document, canonical.clone())
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

    let datum = doc
        .get(&ROOT, automerge::Prop::Map("datum".to_owned()))
        .unwrap()
        .unwrap()
        .1;
    let at = |key: &str| {
        let (stored, id) = doc
            .get(&datum, automerge::Prop::Map(key.to_owned()))
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
