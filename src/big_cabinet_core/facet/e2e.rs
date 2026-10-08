//! End to end: a declaration, the wire form it produces, and the schema it publishes.

use super::*;
use crate::facet::{Bundle, Declaration, FacetDecl, Universe, UniverseError};
use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[macros::facet(id_prefix = "org.example.daybook.")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
struct Note {
    content: String,
    #[serde(flatten)]
    extra: Extra,
}

#[macros::facet(closed, id_prefix = "org.example.daybook.")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Photo {
    url: String,
}

/// The macro writes all three impls, so the derive list holds nothing it owns.
#[macros::facet]
#[derive(Debug, Clone, PartialEq)]
#[serde(tag = "$type", content = "value")]
enum OpenAttachment {
    Note(Note),
    Photo(Photo),
    #[facet(other)]
    Other(UnknownMember),
}

#[macros::facet(closed)]
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "$type", content = "value")]
enum ClosedAttachment {
    #[serde(rename = "org.example.daybook.note")]
    Note(Note),
    #[serde(rename = "org.example.daybook.photo")]
    Photo(Photo),
}

/// A vocabulary rather than a set of named types: the values are strings a peer may extend, so the
/// spelling is stated on the variant and an unknown one is kept.
#[macros::facet(open_string)]
#[derive(Debug, Clone, PartialEq)]
enum OpenColor {
    Red,
    Blue,
    #[facet(other)]
    Unknown(String),
}

/// Never declared, never seen by the macro: the case the publish-time check exists for.
#[derive(JsonSchema)]
#[expect(dead_code, reason = "the point of this type is the schema it emits")]
struct Undeclared {
    content: String,
}

#[test]
fn the_macro_reports_what_it_saw() {
    assert_eq!(
        Note::SHAPE,
        Shape::Object {
            openness: Openness::Open
        }
    );
    assert_eq!(
        Photo::SHAPE,
        Shape::Object {
            openness: Openness::Closed
        }
    );
    assert_eq!(
        OpenAttachment::SHAPE,
        Shape::Union {
            openness: Openness::Open,
            tagging: Tagging::Adjacently {
                tag: "$type",
                content: "value"
            },
        }
    );
    assert_eq!(
        OpenColor::SHAPE,
        Shape::Str {
            known: &["red", "blue"]
        }
    );
    assert_eq!(
        ClosedAttachment::SHAPE,
        Shape::Union {
            openness: Openness::Closed,
            tagging: Tagging::Adjacently {
                tag: "$type",
                content: "value"
            },
        }
    );
}

/// `id_prefix` plus the type name, lowerCamel: the same string `define_enum_and_tag!` builds, so a
/// persisted facet key does not change when a type moves onto this declaration. That the string
/// resolves back to a `WellKnownFacetTag` is a property of `FacetTag` and is asserted where it
/// lives.
#[test]
fn an_id_prefix_derives_the_key_from_the_type_name() {
    assert_eq!(Note::ID, "org.example.daybook.note");
    assert_eq!(Photo::ID, "org.example.daybook.photo");
}

#[test]
fn a_declared_member_round_trips() {
    let member = OpenAttachment::Note(Note {
        content: "hi".into(),
        extra: Extra::new(),
    });
    let wire = json!({"$type": "org.example.daybook.note", "value": {"content": "hi"}});
    assert_eq!(serde_json::to_value(&member).unwrap(), wire);
    let decoded = serde_json::from_value::<OpenAttachment>(wire).unwrap();
    assert_eq!(decoded, member);
}

/// The reason the codec exists: a member this build has never heard of survives a round trip whole,
/// discriminant included, rather than being an error or a truncated value.
#[test]
fn an_undeclared_member_round_trips_unchanged() {
    let wire = json!({
        "$type": "org.example.future",
        "value": {"nested": [1, {"deep": null}]},
        "extra": "kept"
    });
    let decoded = serde_json::from_value::<OpenAttachment>(wire.clone()).unwrap();
    let OpenAttachment::Other(unknown) = &decoded else {
        panic!("an undeclared member is the fallback: {decoded:?}");
    };
    assert_eq!(unknown.id("$type"), Some("org.example.future"));
    assert_eq!(serde_json::to_value(&decoded).unwrap(), wire);
}

/// A recognized discriminant is never quietly turned into an unknown member.
#[test]
fn a_declared_member_with_a_bad_payload_is_an_error() {
    let err = serde_json::from_value::<OpenAttachment>(json!({
        "$type": "org.example.daybook.note",
        "value": 7
    }))
    .unwrap_err();
    assert!(err.to_string().contains("invalid type"), "{err}");
}

#[test]
fn an_open_object_keeps_the_members_it_does_not_name() {
    let wire = json!({"content": "hi", "future": 1});
    let decoded = serde_json::from_value::<Note>(wire.clone()).unwrap();
    assert_eq!(decoded.extra.get("future"), Some(&json!(1)));
    assert_eq!(serde_json::to_value(&decoded).unwrap(), wire);
}

#[test]
fn the_schema_of_an_open_union_agrees_with_its_declaration() {
    let schema = schema_for!(OpenAttachment);
    let flags = check_shape("OpenAttachment", &schema, OpenAttachment::SHAPE).unwrap();
    assert!(flags.is_empty(), "{flags:?}");
    let document = serde_json::to_value(&schema).unwrap();
    let branches = document["oneOf"].as_array().unwrap();
    assert_eq!(
        branches.len(),
        3,
        "two members and the fallback: {document}"
    );
    // A union is a position, so it is never a name a peer can point at; its members are.
    assert!(document["$defs"].get("Note").is_some(), "{document}");
    assert!(
        document["$defs"].get("OpenAttachment").is_none(),
        "{document}"
    );
}

#[test]
fn the_schema_of_a_closed_union_agrees_but_is_flagged() {
    let flags = check_shape(
        "ClosedAttachment",
        &schema_for!(ClosedAttachment),
        ClosedAttachment::SHAPE,
    )
    .unwrap();
    assert_eq!(flags, vec![ShapeFlag::ClosedUnion]);
}

/// The bug worth catching: the schema is permissive because nothing said otherwise, and the type
/// drops what it does not declare. No macro ever saw this type, so only the schema can show it.
#[test]
fn an_undeclared_object_cannot_be_published_as_open() {
    let err = check_shape(
        "Undeclared",
        &schema_for!(Undeclared),
        Shape::Object {
            openness: Openness::Open,
        },
    )
    .unwrap_err();
    assert!(
        matches!(
            err,
            ShapeError::Unstated {
                declared: Openness::Open,
                ..
            }
        ),
        "{err}"
    );
}

#[test]
fn an_object_whose_openness_disagrees_with_its_schema_is_refused() {
    let err = check_shape(
        "Photo",
        &schema_for!(Photo),
        Shape::Object {
            openness: Openness::Open,
        },
    )
    .unwrap_err();
    assert!(
        matches!(
            err,
            ShapeError::Disagrees {
                schema_says_open: false,
                ..
            }
        ),
        "{err}"
    );
}

#[test]
fn a_string_vocabulary_keeps_the_strings_it_does_not_know() {
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
fn the_schema_of_a_string_vocabulary_agrees_with_its_declaration() {
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
fn a_string_vocabulary_whose_values_disagree_is_refused() {
    let err = check_shape(
        "OpenColor",
        &schema_for!(OpenColor),
        Shape::Str {
            known: &["red", "green"],
        },
    )
    .unwrap_err();
    assert!(
        matches!(err, ShapeError::KnownValuesDisagree { .. }),
        "{err}"
    );
}

/// A facet whose schema names another type: the nested one is written at a position, so it is not a
/// facet, but a peer still has to be able to resolve it.
#[macros::facet(id_prefix = "org.example.daybook.")]
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct Nested {
    inner: Photo,
    #[serde(flatten)]
    extra: Extra,
}

#[macros::facet(id_prefix = "org.example.daybook.")]
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct AlsoNested {
    photo: Photo,
    #[serde(flatten)]
    extra: Extra,
}

/// A facet's own schema is its definition, and every definition it reached travels beside it rather
/// than inside it — which is what lets two facets share one.
#[test]
fn a_declaration_hoists_the_definitions_it_reaches() {
    let declaration = Declaration::of::<Nested>().unwrap();
    assert_eq!(declaration.facet.id, "org.example.daybook.nested");

    let document = serde_json::to_value(&declaration.facet.schema).unwrap();
    assert!(
        document.get("$defs").is_none(),
        "nothing is inlined beside the definition: {document}"
    );
    assert_eq!(document["additionalProperties"], true);
    assert_eq!(
        document["properties"]["inner"]["$ref"], "#/$defs/Photo",
        "the reference is what ties it to the context: {document}"
    );

    let names: Vec<&str> = declaration
        .types
        .iter()
        .map(|decl| decl.name.as_str())
        .collect();
    assert_eq!(names, vec!["Photo"]);
}

#[test]
fn a_definition_two_facets_both_reach_is_carried_once() {
    let universe = Universe::collect(vec![
        Declaration::of::<Nested>().unwrap(),
        Declaration::of::<AlsoNested>().unwrap(),
    ]);
    assert_eq!(universe.facets.len(), 2);
    let names: Vec<&str> = universe
        .types
        .iter()
        .map(|decl| decl.name.as_str())
        .collect();
    assert_eq!(names, vec!["Photo"], "carried once, not once per facet");
}

/// A closed object is a decision with a cost, so it comes back as a flag rather than a refusal.
#[test]
fn a_declaration_carries_the_flags_of_its_shape() {
    let declaration = Declaration::of::<Photo>().unwrap();
    assert_eq!(declaration.facet.flags, vec![ShapeFlag::ClosedObject]);
    let universe = Universe::collect(vec![declaration]);
    assert_eq!(
        Bundle::new(&universe, Vec::new()).validate().unwrap(),
        vec![ShapeFlag::ClosedObject]
    );
}

/// The bundle is the context, so what the plug carries resolves and nothing else does.
#[test]
fn the_bundle_resolves_the_definitions_the_plug_carries() {
    let universe = Universe::collect(vec![Declaration::of::<Nested>().unwrap()]);
    let bundle = Bundle::new(&universe, Vec::new());
    assert!(bundle.resolve("Nested").is_some());
    assert!(bundle.resolve("Photo").is_some());
    assert!(bundle.resolve("org.example.absent").is_none());
}

#[test]
fn two_facets_claiming_one_tag_are_refused() {
    let first = Declaration::of::<Note>().unwrap();
    let mut second = Declaration::of::<Note>().unwrap();
    second.facet.name = "ADifferentName".into();
    let universe = Universe::collect(vec![first, second]);
    let bundle = Bundle::new(&universe, Vec::new());
    assert!(
        matches!(bundle.validate(), Err(UniverseError::DuplicateId { .. })),
        "{:?}",
        bundle.validate()
    );
}

/// Two declarations of one name with different schemas: only the plug can see both, which is why
/// this is checked here and not per declaration.
#[test]
fn two_declarations_disagreeing_about_a_name_are_refused() {
    let mut universe = Universe::collect(vec![Declaration::of::<Note>().unwrap()]);
    universe.types.push(crate::facet::TypeDecl {
        name: "Note".into(),
        schema: schemars::json_schema!({"type": "string"}),
    });
    let err = Bundle::new(&universe, Vec::new()).validate().unwrap_err();
    assert!(matches!(err, UniverseError::Conflict { .. }), "{err}");
}

/// A declaration whose schema points at a name it does not carry — which a derived schema never
/// does, since `schemars` puts every definition it reaches into `$defs`, but a hand-written or
/// hoisted one does. Resolving it needs the rest of the plug, and a name that leaves the plug needs
/// a dependency: the violation that has no meaning below the plug, because one declaration cannot
/// see the others.
#[test]
fn a_reference_is_resolved_against_the_plug_and_its_dependencies() {
    let ours = Universe::collect(vec![Declaration {
        facet: FacetDecl {
            id: "org.example.daybook.referencing".into(),
            name: "Referencing".into(),
            schema: schemars::json_schema!({
                "type": "object",
                "properties": { "photo": { "$ref": "#/$defs/Photo" } },
            }),
            flags: Vec::new(),
        },
        types: Vec::new(),
    }]);
    let err = Bundle::new(&ours, Vec::new()).validate().unwrap_err();
    assert!(
        matches!(err, UniverseError::Undeclared { ref name, .. } if name == "Photo"),
        "{err}"
    );

    let theirs = Universe::collect(vec![Declaration::of::<Photo>().unwrap()]);
    assert!(Bundle::new(&ours, vec![&theirs]).validate().is_ok());
}

/// What hoisting is for: a peer takes one document and a stock validator can check a value against
/// it, with no knowledge of what the manifest flattened.
#[test]
fn the_bundle_document_validates_a_value_with_a_stock_validator() {
    let universe = Universe::collect(vec![Declaration::of::<Nested>().unwrap()]);
    let bundle = Bundle::new(&universe, Vec::new());
    let document = bundle.document("Nested").expect("the facet resolves");
    let document = serde_json::to_value(&document).unwrap();
    let validator = jsonschema::validator_for(&document).unwrap();

    assert!(validator.is_valid(&json!({"inner": {"url": "https://example.com"}})));
    assert!(
        !validator.is_valid(&json!({"inner": {"url": "https://example.com", "extra": 1}})),
        "the member is a closed object and it stays closed through hoisting"
    );
    assert!(
        validator.is_valid(&json!({"inner": {"url": "https://example.com"}, "unknown": 1})),
        "and the facet is open, so a member it does not name is still valid"
    );
}

/// A value's spelling is derived, never written — and the one override is a statement rather than a
/// rule, so `rename_all` in the case that agrees with the rule is read and then consumed.
#[macros::facet(open_string)]
#[derive(Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
enum Derived {
    #[serde(rename = "org.example.custom")]
    Explicit,
    MyValue,
    #[facet(other)]
    Unknown(String),
}

#[test]
fn a_spelling_is_derived_or_stated_and_never_rewritten() {
    assert_eq!(
        Derived::SHAPE,
        Shape::Str {
            known: &["org.example.custom", "myValue"]
        },
        "an explicit rename is honoured and a variant name is derived"
    );
    assert_eq!(
        serde_json::to_value(Derived::MyValue).unwrap(),
        json!("myValue")
    );
    assert_eq!(
        serde_json::from_value::<Derived>(json!("org.example.custom")).unwrap(),
        Derived::Explicit
    );
}
