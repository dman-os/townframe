//! The two ways a string vocabulary can be declared: open, which carries what it does not know, and
//! closed, which refuses it.

use super::*;

/// A vocabulary rather than a set of named types: the values are strings a peer may extend, so the
/// spelling is stated on the variant and an unknown one is kept.
#[macros::datum(open_string)]
#[derive(Debug, Clone, PartialEq)]
enum OpenColor {
    Red,
    Blue,
    #[datum(other)]
    Unknown(String),
}

/// The closed form of the same shape: the list is the whole vocabulary, so an unrecognized string is
/// an error rather than a value to carry.
#[macros::datum(closed_string)]
#[derive(Debug, Clone, PartialEq)]
enum ClosedColor {
    Red,
    Blue,
}

#[test]
fn the_macro_reports_a_string_vocabulary_and_its_openness() {
    assert_eq!(
        OpenColor::SHAPE,
        Shape::Str {
            openness: Openness::Open,
            known: &["red", "blue"],
        }
    );
    assert_eq!(
        ClosedColor::SHAPE,
        Shape::Str {
            openness: Openness::Closed,
            known: &["red", "blue"],
        }
    );
}

#[test]
fn an_open_string_vocabulary_keeps_the_strings_it_does_not_know() {
    assert_eq!(serde_json::to_value(OpenColor::Red).unwrap(), json!("red"));
    let unknown = serde_json::from_value::<OpenColor>(json!("violet")).unwrap();
    assert_eq!(unknown, OpenColor::Unknown("violet".into()));
    assert_eq!(
        serde_json::to_value(&unknown).unwrap(),
        json!("violet"),
        "an unrecognized string is written back as it arrived"
    );
}

#[test]
fn the_schema_of_an_open_string_vocabulary_agrees_with_its_declaration() {
    let schema = schema_for!(OpenColor);
    let flags = check_shape("OpenColor", &schema, OpenColor::SHAPE).unwrap();
    assert!(flags.is_empty(), "{flags:?}");
    let document = serde_json::to_value(&schema).unwrap();
    assert_eq!(document["type"], "string");
    assert_eq!(document["knownValues"], json!(["red", "blue"]));
}

/// The check has something to catch here too: a vocabulary whose schema lists values the type does
/// not know, or a different set of them, is a declaration and a schema disagreeing.
#[test]
fn an_open_string_vocabulary_whose_values_disagree_is_refused() {
    let err = check_shape(
        "OpenColor",
        &schema_for!(OpenColor),
        Shape::Str {
            openness: Openness::Open,
            known: &["red", "green"],
        },
    )
    .unwrap_err();
    assert!(
        matches!(err, ShapeError::KnownValuesDisagree { .. }),
        "{err}"
    );
}

#[test]
fn a_closed_string_vocabulary_rejects_the_strings_it_does_not_know() {
    assert_eq!(
        serde_json::to_value(ClosedColor::Red).unwrap(),
        json!("red")
    );
    assert_eq!(
        serde_json::from_value::<ClosedColor>(json!("blue")).unwrap(),
        ClosedColor::Blue
    );
    let err = serde_json::from_value::<ClosedColor>(json!("violet")).unwrap_err();
    assert!(err.to_string().contains("unknown variant"), "{err}");
}

#[test]
fn the_schema_of_a_closed_string_vocabulary_agrees_with_its_declaration() {
    let schema = schema_for!(ClosedColor);
    let flags = check_shape("ClosedColor", &schema, ClosedColor::SHAPE).unwrap();
    assert_eq!(flags, vec![ShapeFlag::ClosedString]);
    let document = serde_json::to_value(&schema).unwrap();
    assert_eq!(document["type"], "string");
    assert_eq!(document["enum"], json!(["red", "blue"]));
    assert!(
        document.get("knownValues").is_none(),
        "a closed vocabulary constrains the whole value set"
    );
}
