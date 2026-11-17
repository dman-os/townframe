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
    unsigned8: u8,
    unsigned16: u16,
    unsigned32: u32,
    unsigned64: u64,
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
            unsigned8: if minimum { 0 } else { u8::MAX },
            unsigned16: if minimum { 0 } else { u16::MAX },
            unsigned32: if minimum { 0 } else { u32::MAX },
            unsigned64: if minimum { 0 } else { i64::MAX as u64 },
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

/// A Rust width reaches the Lexicon as bounds alone: `schemars`' `format: uint32` becomes
/// `minimum: 0, maximum: 4294967295`, a signed width its two-sided range, and the 64-bit widths
/// only what an `i64` bound can state. Nothing named `format` is left on the integer.
#[test]
fn rust_integer_widths_publish_as_bounds_and_leave_no_format() {
    let universe = Universe::collect([Declaration::of::<WholeNumbers>().unwrap()]);
    let lexicons = Bundle::new(&universe, Vec::new()).lexicons().unwrap();
    let properties = &lexicons[0].file["defs"]["main"]["properties"];
    for (field, expected) in [
        (
            "signed8",
            json!({"type": "integer", "minimum": -128, "maximum": 127}),
        ),
        (
            "signed32",
            json!({"type": "integer", "minimum": i32::MIN, "maximum": i32::MAX}),
        ),
        ("signed64", json!({"type": "integer"})),
        (
            "unsigned8",
            json!({"type": "integer", "minimum": 0, "maximum": 255}),
        ),
        (
            "unsigned32",
            json!({"type": "integer", "minimum": 0, "maximum": u32::MAX}),
        ),
        ("unsigned64", json!({"type": "integer", "minimum": 0})),
    ] {
        assert_eq!(properties[field], expected, "{field}");
    }
}

/// The values a width excludes are refused at admission, not truncated into storage.
#[test]
fn values_outside_a_rust_width_are_refused_at_admission() {
    let universe = Universe::collect([Declaration::of::<WholeNumbers>().unwrap()]);
    let bundle = Bundle::new(&universe, Vec::new());
    let lexicon = &bundle.lexicons().unwrap()[0];
    let compiled = bundle
        .validate_lexicon(&lexicon.id, &lexicon.file, &[])
        .unwrap();
    let within = json!({
        "$type": WholeNumbers::ID,
        "signed8": 0, "signed16": 0, "signed32": 0, "signed64": 0,
        "unsigned8": 0, "unsigned16": 0, "unsigned32": 0, "unsigned64": 0,
        "list": [], "map": {},
    });
    Stored::check(&compiled, within.clone()).unwrap();
    for (field, spelling) in [
        ("signed8", json!(128)),
        ("signed32", json!(i64::from(i32::MAX) + 1)),
        ("unsigned8", json!(-1)),
        ("unsigned8", json!(256)),
        ("unsigned32", json!(i64::from(u32::MAX) + 1)),
        ("unsigned64", json!(-1)),
        ("unsigned64", json!(u64::MAX)),
    ] {
        let mut value = within.clone();
        value[field] = spelling;
        assert!(
            Stored::check(&compiled, value).is_err(),
            "{field} outside its width is refused"
        );
    }
}

#[macros::datum(authority = "org.example.daybook")]
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct SignedButNonNegative {
    #[schemars(range(min = 0))]
    count: i32,
    #[serde(flatten)]
    extra: Extra,
}

/// The sign is a lower bound: `minimum >= 0` is what the codec stores as an Automerge `Uint`, which
/// an `i32` cannot hydrate from. A signed width bounded that way is refused at declaration instead
/// of storing a value its own type cannot read back.
#[test]
fn a_signed_width_bounded_to_non_negative_is_refused_at_declaration() {
    let error = Declaration::of::<SignedButNonNegative>().unwrap_err();
    assert!(
        error.to_string().contains("unsigned Automerge integer"),
        "{error}"
    );
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
