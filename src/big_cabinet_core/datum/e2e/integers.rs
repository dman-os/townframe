use super::*;
use automerge::Automerge;
use automerge::ROOT;
use automerge::ReadDoc;
use std::collections::BTreeMap;

#[macros::datum(authority = "org.example.daybook")]
#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
struct WholeNumbers {
    signed8: i8,
    signed16: i16,
    signed32: i32,
    signed64: i64,
    signed_size: isize,
    unsigned8: u8,
    unsigned16: u16,
    unsigned32: u32,
    unsigned64: u64,
    unsigned_size: usize,
    optional: Option<i32>,
    list: Vec<u16>,
    map: BTreeMap<String, u32>,
    #[serde(flatten)]
    extra: Extra,
}

#[test]
fn plain_integers_declare_publish_admit_and_round_trip_at_their_boundaries() {
    let universe = Universe::collect([Declaration::of::<WholeNumbers>().unwrap()]);
    let bundle = Bundle::new(&universe, Vec::new());
    let lexicons = bundle.lexicons().unwrap();
    let lexicon = &lexicons[0];
    let compiled = bundle
        .validate_lexicon(&lexicon.id, &lexicon.file, &[])
        .unwrap();
    let mut doc = Automerge::new();
    for minimum in [true, false] {
        let numbers = WholeNumbers {
            signed8: if minimum { i8::MIN } else { i8::MAX },
            signed16: if minimum { i16::MIN } else { i16::MAX },
            signed32: if minimum { i32::MIN } else { i32::MAX },
            signed64: if minimum { i64::MIN } else { i64::MAX },
            signed_size: if minimum { isize::MIN } else { isize::MAX },
            unsigned8: if minimum { 0 } else { u8::MAX },
            unsigned16: if minimum { 0 } else { u16::MAX },
            unsigned32: if minimum { 0 } else { u32::MAX },
            unsigned64: if minimum { 0 } else { i64::MAX as u64 },
            unsigned_size: if minimum { 0 } else { isize::MAX as usize },
            optional: if minimum { None } else { Some(i32::MAX) },
            list: vec![0, u16::MAX],
            map: BTreeMap::from([("count".into(), u32::MAX), ("zero".into(), 0)]),
            extra: [("future".into(), json!({"value": 42}))].into(),
        };
        let mut value = serde_json::to_value(&numbers).unwrap();
        value["$type"] = json!(WholeNumbers::ID);
        reconcile_json_atomic(
            doc.transaction(),
            &compiled,
            &ROOT,
            "datum".into(),
            value.clone(),
        )
        .unwrap()
        .commit();
        let (_, object) = doc.get(ROOT, "datum").unwrap().unwrap();
        for (field, scalar) in [
            ("signed64", automerge::ScalarValue::Int(numbers.signed64)),
            (
                "unsigned64",
                automerge::ScalarValue::Uint(numbers.unsigned64),
            ),
        ] {
            assert_eq!(
                doc.get(&object, field).unwrap().unwrap().0,
                automerge::Value::Scalar(std::borrow::Cow::Owned(scalar))
            );
        }
        let hydrated = hydrate_value(&doc, &ROOT, "datum".into(), &compiled).unwrap();
        assert_eq!(hydrated, value);
        let mut fields = hydrated.as_object().unwrap().clone();
        fields.remove("$type");
        assert_eq!(
            serde_json::from_value::<WholeNumbers>(fields.into()).unwrap(),
            numbers
        );
        let heads = doc.get_heads();
        reconcile_json_atomic(doc.transaction(), &compiled, &ROOT, "datum".into(), value)
            .unwrap()
            .commit();
        assert_eq!(doc.get_heads(), heads);
    }
}

#[macros::datum(authority = "org.example.daybook")]
#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
struct WideNumbers {
    signed128: i128,
    unsigned128: u128,
    optional: Option<i128>,
    list: Vec<u128>,
    #[serde(flatten)]
    extra: Extra,
}

/// A 128-bit field is declarable and publishable, but a Lexicon integer is signed-64-bit, so the
/// field carries exactly the signed range: the boundaries it round-trips are `i64`'s, stated here
/// through the 128-bit types a declaration actually names.
#[test]
fn wide_integers_declare_publish_admit_and_round_trip_at_the_signed_lexicon_boundaries() {
    let universe = Universe::collect([Declaration::of::<WideNumbers>().unwrap()]);
    let bundle = Bundle::new(&universe, Vec::new());
    let lexicons = bundle.lexicons().unwrap();
    let lexicon = &lexicons[0];
    let properties = &lexicon.file["defs"]["main"]["properties"];
    assert_eq!(properties["signed128"], json!({"type": "integer"}));
    assert_eq!(
        properties["unsigned128"],
        json!({"type": "integer", "minimum": 0})
    );
    let compiled = bundle
        .validate_lexicon(&lexicon.id, &lexicon.file, &[])
        .unwrap();
    let mut doc = Automerge::new();
    for minimum in [true, false] {
        let numbers = WideNumbers {
            signed128: if minimum {
                i128::from(i64::MIN)
            } else {
                i128::from(i64::MAX)
            },
            unsigned128: if minimum { 0 } else { i64::MAX as u128 },
            optional: if minimum { None } else { Some(-1) },
            list: vec![0, i64::MAX as u128],
            extra: [("future".into(), json!({"value": 42}))].into(),
        };
        let mut value = serde_json::to_value(&numbers).unwrap();
        value["$type"] = json!(WideNumbers::ID);
        reconcile_json_atomic(
            doc.transaction(),
            &compiled,
            &ROOT,
            "datum".into(),
            value.clone(),
        )
        .unwrap()
        .commit();
        let (_, object) = doc.get(ROOT, "datum").unwrap().unwrap();
        for (field, scalar) in [
            (
                "signed128",
                automerge::ScalarValue::Int(numbers.signed128 as i64),
            ),
            (
                "unsigned128",
                automerge::ScalarValue::Uint(numbers.unsigned128 as u64),
            ),
        ] {
            assert_eq!(
                doc.get(&object, field).unwrap().unwrap().0,
                automerge::Value::Scalar(std::borrow::Cow::Owned(scalar))
            );
        }
        let hydrated = hydrate_value(&doc, &ROOT, "datum".into(), &compiled).unwrap();
        assert_eq!(hydrated, value);
        let mut fields = hydrated.as_object().unwrap().clone();
        fields.remove("$type");
        assert_eq!(
            serde_json::from_value::<WideNumbers>(fields.into()).unwrap(),
            numbers
        );
        let heads = doc.get_heads();
        reconcile_json_atomic(doc.transaction(), &compiled, &ROOT, "datum".into(), value)
            .unwrap()
            .commit();
        assert_eq!(doc.get_heads(), heads);
    }
}

/// Past the signed range a 128-bit value is refused at admission, not truncated into storage: the
/// JSON spelling of `i64::MAX + 1` and of `u64::MAX` both name a 128-bit value the field's type
/// can hold and the Lexicon cannot.
#[test]
fn wide_integers_outside_the_signed_lexicon_range_are_refused_at_admission() {
    let universe = Universe::collect([Declaration::of::<WideNumbers>().unwrap()]);
    let bundle = Bundle::new(&universe, Vec::new());
    let lexicons = bundle.lexicons().unwrap();
    let lexicon = &lexicons[0];
    let compiled = bundle
        .validate_lexicon(&lexicon.id, &lexicon.file, &[])
        .unwrap();
    let within = json!({
        "$type": WideNumbers::ID,
        "signed128": i64::MAX,
        "unsigned128": i64::MAX,
        "list": [],
    });
    assert_eq!(
        Stored::check(&compiled, within.clone())
            .unwrap()
            .into_value(),
        within
    );
    for (field, spelling) in [
        ("signed128", json!(i64::MAX as u64 + 1)),
        ("signed128", json!(u64::MAX)),
        ("unsigned128", json!(i64::MAX as u64 + 1)),
        ("unsigned128", json!(u64::MAX)),
    ] {
        let mut value = within.clone();
        value[field] = spelling;
        assert!(
            Stored::check(&compiled, value).is_err(),
            "{field} past the signed range is refused"
        );
    }
    let mut nested = within.clone();
    nested["list"] = json!([u64::MAX]);
    assert!(Stored::check(&compiled, nested).is_err());
    let mut optional = within.clone();
    optional["optional"] = json!(u64::MAX);
    assert!(Stored::check(&compiled, optional).is_err());
}

#[macros::datum(authority = "org.example.daybook")]
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct BoundedIntegers {
    #[schemars(range(min = -7, max = 11))]
    signed: i32,
    #[schemars(range(min = 3, max = 17))]
    unsigned: u64,
    #[serde(flatten)]
    extra: Extra,
}

#[test]
fn integer_bounds_are_published_and_enforced_without_a_native_storage_override() {
    let universe = Universe::collect([Declaration::of::<BoundedIntegers>().unwrap()]);
    let bundle = Bundle::new(&universe, Vec::new());
    let lexicons = bundle.lexicons().unwrap();
    let lexicon = &lexicons[0];
    let properties = &lexicon.file["defs"]["main"]["properties"];
    assert_eq!(
        properties["signed"],
        json!({"type": "integer", "minimum": -7, "maximum": 11})
    );
    assert_eq!(
        properties["unsigned"],
        json!({"type": "integer", "minimum": 3, "maximum": 17})
    );
    let compiled = bundle
        .validate_lexicon(&lexicon.id, &lexicon.file, &[])
        .unwrap();
    for (signed, unsigned) in [(-7, 3), (11, 17)] {
        let value = json!({"$type": BoundedIntegers::ID, "signed": signed, "unsigned": unsigned});
        assert_eq!(
            Stored::check(&compiled, value.clone())
                .unwrap()
                .into_value(),
            value
        );
    }
    for (signed, unsigned) in [(-8, 3), (12, 3), (0, 2), (0, 18)] {
        assert!(
            Stored::check(
                &compiled,
                json!({"$type": BoundedIntegers::ID, "signed": signed, "unsigned": unsigned})
            )
            .is_err()
        );
    }
}
