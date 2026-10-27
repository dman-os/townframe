use super::*;

/// One position of a facet, projected: the shape every case here is about.
fn project(property: Value) -> Result<Value, String> {
    project_facet(
        "org.example.test",
        &json!({ "type": "object", "properties": { "value": property } }),
    )
}

fn projected(property: Value) -> Value {
    project(property).expect("projected")["defs"]["main"]["properties"]["value"].clone()
}

fn refused(property: Value) -> String {
    project(property).expect_err("refused")
}

/// The projection the whole arrangement rests on: Lexicon has no map type, so a dynamic map is
/// an `unknown` and the sidecar states the value type it narrows to. It is not a `map`, which
/// Lexicon does not have and a peer cannot read.
#[test]
fn a_typed_dynamic_map_is_an_unknown_the_sidecar_narrows() {
    assert_eq!(
        projected(json!({"type": "object", "additionalProperties": {"type": "string"}})),
        json!({"type": "unknown"})
    );
    assert_eq!(
        projected(json!({
            "type": "object",
            "additionalProperties": {"type": "integer", "format": "uint64"}
        })),
        json!({"type": "unknown"})
    );
}

/// The narrowing has a floor: the sidecar may say less than Lexicon, never admit a value the
/// canonical data model does not have. The value type is projected for exactly this. Two
/// deliberate non-refusals: a `true` catch-all is not a value type but the untyped open object
/// (it projects as `unknown` unchecked because it states nothing to check), and a string with
/// `contentEncoding: base64` is a plain string — the projection never sniffs encodings, so it
/// projects as `{"type": "string"}` and stays canonical.
#[test]
fn a_dynamic_map_still_may_not_admit_a_non_canonical_value() {
    for values in [
        json!({"type": "number"}),
        json!({"type": "number", "x-automerge": "number"}),
    ] {
        let reason = refused(json!({"type": "object", "additionalProperties": values}));
        assert!(reason.contains(".<values>"), "{reason}");
    }
}

/// A named type travels beside the facet that reaches it and is referenced locally, which is
/// what lets a plug be read on its own. Inlining it would lose the name a peer points at, and
/// leaving the reference alone would publish a file that does not resolve.
#[test]
fn a_named_definition_is_carried_and_referenced_locally() {
    let file = project_facet(
        "org.example.parent",
        &json!({
            "type": "object",
            "properties": { "child": {"$ref": "#/$defs/Child"} },
            "$defs": {
                "Child": {
                    "type": "object",
                    "properties": { "name": {"type": "string"} },
                    "required": ["name"],
                    "x-lexicon-id": "org.example.child"
                }
            }
        }),
    )
    .expect("projected");
    assert_eq!(
        file,
        json!({
            "lexicon": 1,
            "id": "org.example.parent",
            "defs": {
                "main": {
                    "type": "object",
                    "properties": { "child": {"type": "ref", "ref": "#Child"} }
                },
                "Child": {
                    "type": "object",
                    "properties": { "name": {"type": "string"} },
                    "required": ["name"]
                }
            }
        })
    );
}

#[test]
fn a_reference_nothing_carries_is_refused_rather_than_emitted() {
    let reason = refused(json!({"$ref": "#/$defs/Absent"}));
    assert!(reason.contains("does not carry"), "{reason}");
    let reason = refused(json!({"$ref": "https://example.com/schema"}));
    assert!(
        reason.contains("not a reference into this document"),
        "{reason}"
    );
}

/// A definition that reaches itself is a reference, not a loop: the sentinel placed before the
/// recursion is what makes a recursive type project instead of exhausting the stack.
#[test]
fn a_recursive_definition_is_a_reference_to_itself() {
    let file = project_facet(
        "org.example.tree",
        &json!({
            "type": "object",
            "properties": { "child": {"$ref": "#/$defs/Node"} },
            "$defs": {
                "Node": {
                    "type": "object",
                    "properties": { "next": {"$ref": "#/$defs/Node"} },
                    "required": []
                }
            }
        }),
    )
    .expect("projected");
    assert_eq!(
        file["defs"]["Node"]["properties"]["next"],
        json!({"type": "ref", "ref": "#Node"})
    );
}

/// What Lexicon states travels; what it counts differently does not. `maxLength` is UTF-8 bytes
/// there and code points here, so copying the number would state another constraint in each —
/// and the sidecar keeps it, which is the narrowing the arrangement allows.
#[test]
fn constraints_lexicon_states_travel_and_the_ones_it_counts_differently_do_not() {
    assert_eq!(
        projected(json!({
            "type": "string",
            "enum": ["a", "b"],
            "knownValues": ["a", "b", "c"],
            "minLength": 1,
            "maxLength": 9,
            "pattern": "^a",
            "format": "uuid"
        })),
        json!({"type": "string", "enum": ["a", "b"], "knownValues": ["a", "b", "c"]})
    );
    assert_eq!(
        projected(json!({"type": "string", "format": "date-time"})),
        json!({"type": "string", "format": "datetime"})
    );
    assert_eq!(
        projected(json!({"type": "integer", "format": "uint64", "minimum": 0, "maximum": 9})),
        json!({"type": "integer", "minimum": 0, "maximum": 9})
    );
    // Lexicon counts array elements with the length keywords.
    assert_eq!(
        projected(json!({"type": "array", "items": {"type": "string"}, "minItems": 1})),
        json!({"type": "array", "items": {"type": "string"}, "minLength": 1})
    );
}

/// Nullability is a list of names on the object holding the position, so a property has one and
/// a position inside a container does not. Projecting the latter as the non-nullable type it is
/// not would publish a Lexicon that rejects a value the facet accepts.
#[test]
fn null_is_expressible_only_as_an_object_property() {
    assert_eq!(
        projected(json!({"type": "object", "properties": {
            "value": {"type": ["string", "null"]},
            "either": {"anyOf": [{"type": "integer"}, {"type": "null"}]}
        }})),
        json!({
            "type": "object",
            "properties": {
                "value": {"type": "string"},
                "either": {"type": "integer"}
            },
            "nullable": ["either", "value"]
        })
    );
    let reason = refused(json!({
        "type": "array",
        "items": {"type": ["integer", "null"]}
    }));
    assert!(
        reason.contains("more than one other type") || reason.contains("null"),
        "{reason}"
    );
}

/// The marker says how a value is stored and the JSON shape says what it looks like; the two are
/// one declaration, so they cannot disagree.
#[test]
fn a_storage_marker_disagreeing_with_its_json_shape_is_refused() {
    let reason = refused(json!({"type": "integer", "x-automerge": "bytes"}));
    assert!(reason.contains("disagree"), "{reason}");
    let reason = refused(json!({"type": "number", "x-automerge": "float"}));
    assert!(reason.contains("carried as a string"), "{reason}");
    let reason = refused(json!({"type": "string", "x-automerge": "vectorClock"}));
    assert!(reason.contains("not a storage form"), "{reason}");
}

/// The five storage forms, each in the Lexicon type its canonical JSON has.
#[test]
fn each_storage_form_projects_to_the_type_its_json_has() {
    assert_eq!(
        projected(json!({
            "type": "object",
            "x-automerge": "bytes",
            "properties": {"$bytes": {"type": "string", "contentEncoding": "base64"}},
            "required": ["$bytes"],
            "additionalProperties": false
        })),
        json!({"type": "bytes"})
    );
    assert_eq!(
        projected(json!({"type": "string", "x-automerge": "float", "format": "double"})),
        json!({"type": "string"})
    );
    assert_eq!(
        projected(json!({"type": "string", "x-automerge": "text"})),
        json!({"type": "string"})
    );
    assert_eq!(
        projected(json!({"type": "integer", "x-automerge": "counter"})),
        json!({"type": "integer"})
    );
    // The list opt-out is an ordinary array of numbers, and the marker only says the byte
    // vector was meant as one.
    assert_eq!(
        projected(json!({
            "type": "array",
            "x-automerge": "list",
            "items": {"type": "integer", "format": "uint8"}
        })),
        json!({"type": "array", "items": {"type": "integer"}})
    );
}

/// The float opt-out is a legitimate codec position and is not canonical JSON, so the facet
/// holding one is a codec document rather than a publishable declaration.
#[test]
fn a_plain_number_is_not_a_lexicon_type() {
    for stated in [
        json!({"type": "number"}),
        json!({"type": "number", "format": "double"}),
        json!({"type": "number", "x-automerge": "number"}),
    ] {
        let reason = refused(stated);
        assert!(reason.contains("plain `number`"), "{reason}");
    }
}

#[test]
fn a_position_that_admits_any_json_value_has_no_lexicon_type() {
    for stated in [json!(true), json!({})] {
        let reason = refused(stated);
        assert!(
            reason.contains("no Lexicon type") || reason.contains("states no type"),
            "{reason}"
        );
    }
}

#[test]
fn an_application_dollar_key_is_refused() {
    let reason = project(json!({
        "type": "object",
        "properties": {"$private": {"type": "string"}}
    }))
    .expect_err("refused");
    assert!(reason.contains("cannot start with `$`"), "{reason}");
}

/// The discriminant is the reference: an internally tagged branch's `$type` const *is* the id a
/// peer reads, so the union needs no wrapper and no layout of its own.
#[test]
fn an_open_union_names_its_members_and_a_closed_one_says_so() {
    let member = |id: &str| {
        json!({
            "type": "object",
            "properties": {"$type": {"const": id}, "text": {"type": "string"}},
            "required": ["$type", "text"],
            "additionalProperties": false
        })
    };
    let fallback = |known: &[&str]| {
        json!({
            "type": "object",
            "properties": {"$type": {"type": "string", "not": {"enum": known}}},
            "required": ["$type"],
            "additionalProperties": true
        })
    };
    assert_eq!(
        projected(
            json!({"oneOf": [member("org.example.a"), member("org.example.b"), fallback(&["org.example.a", "org.example.b"])]})
        ),
        json!({"type": "union", "refs": ["org.example.a", "org.example.b"]})
    );
    assert_eq!(
        projected(json!({"oneOf": [member("org.example.a"), member("org.example.b")]})),
        json!({"type": "union", "refs": ["org.example.a", "org.example.b"], "closed": true})
    );
    // A fallback that excludes something the union does not declare, or fails to exclude
    // something it does, would leave a member matching two branches or none.
    let reason = refused(json!({"oneOf": [member("org.example.a"), fallback(&["org.example.b"])]}));
    assert!(reason.contains("fallback excludes"), "{reason}");
    let reason = refused(json!({"oneOf": []}));
    assert!(reason.contains("at least one member"), "{reason}");
}

/// The union names its members and does not carry them, but a member it cannot project is a
/// member it cannot publish: the discriminant alone says nothing about the payload beside it.
#[test]
fn a_union_member_payload_is_projected_even_though_the_union_only_names_it() {
    let reason = refused(json!({"oneOf": [{
        "type": "object",
        "properties": {"$type": {"const": "org.example.a"}, "score": {"type": "number"}},
        "required": ["$type", "score"]
    }]}));
    assert!(reason.contains("<payload>"), "{reason}");
    let reason =
        refused(json!({"oneOf": [{"type": "object", "properties": {"text": {"type": "string"}}}]}));
    assert!(reason.contains("`$type` discriminant"), "{reason}");
}

/// The grammar check is the guard on the projector itself: nothing outside this crate reads
/// these files, so a type it invented is caught here rather than published.
#[test]
fn the_projection_is_checked_against_the_lexicon_grammar() {
    for (file, reason_contains) in [
        (
            json!({"lexicon": 1, "id": "org.example.test", "defs": {"main": {"type": "map", "values": {"type": "string"}}}}),
            "not a Lexicon type",
        ),
        (
            json!({"lexicon": 1, "id": "org.example.test", "defs": {"main": {"type": "object", "properties": {"a": {"type": "object", "propertyNames": {}}}}}}),
            "not a field",
        ),
        (
            json!({"lexicon": 1, "id": "org.example.test", "defs": {"main": {"type": "unknown"}}}),
            "cannot be a named definition",
        ),
        (
            json!({"lexicon": 1, "id": "org.example.test", "defs": {"main": {"type": "object", "properties": {"a": {"type": "ref", "ref": "#Absent"}}}}}),
            "not a definition this file carries",
        ),
        (
            json!({"lexicon": 1, "id": "org.example.test", "defs": {"main": {"type": "object", "properties": {"a": {"type": "ref", "ref": "#main"}}}}}),
            "never as `#main`",
        ),
        (
            json!({"lexicon": 1, "id": "org.example.test", "defs": {"main": {"type": "object", "properties": {"a": {"type": "string", "maxLength": 1}}, "nullable": ["b"]}}}),
            "not one of its properties",
        ),
        (
            json!({"lexicon": 2, "id": "org.example.test", "defs": {"main": {"type": "object", "properties": {"a": {"type": "string"}}}}}),
            "language version",
        ),
    ] {
        let reason = validate(&file).expect_err("refused");
        assert!(reason.contains(reason_contains), "{reason}");
    }
}

/// A facet is an object a peer reads; the shapes that are not one are positions inside it.
#[test]
fn a_facet_main_is_an_object() {
    let reason =
        project_facet("org.example.test", &json!({"type": "string"})).expect_err("refused");
    assert!(reason.contains("`main` is an object"), "{reason}");
}

/// An object with nothing named is the `unknown` whose sidecar states the rest, because Lexicon's
/// `object` requires the properties it does not have.
#[test]
fn an_object_with_no_properties_is_an_unknown() {
    assert_eq!(
        projected(json!({"type": "object", "additionalProperties": true})),
        json!({"type": "unknown"})
    );
    assert_eq!(
        projected(json!({"type": "object", "properties": {}, "additionalProperties": false})),
        json!({"type": "unknown"})
    );
}

#[test]
fn a_composition_lexicon_cannot_state_is_refused() {
    for keyword in ["allOf", "not", "prefixItems", "propertyNames"] {
        let reason = refused(json!({ "type": "object", keyword: [] }));
        assert!(reason.contains(keyword), "{reason}");
    }
}

/// A map keyed by one anchored pattern is a dynamic map whose keys the sidecar narrows, so it is
/// the same `unknown` and its values are still held to the canonical data model.
#[test]
fn a_keyed_map_is_an_unknown_whose_values_must_align() {
    let keyed = |values: Value| {
        json!({
            "type": "object",
            "patternProperties": {"^[0-9a-f]{8}$": values},
            "additionalProperties": false
        })
    };
    assert_eq!(
        projected(keyed(json!({"type": "string"}))),
        json!({"type": "unknown"})
    );
    let reason = refused(keyed(json!({"type": "number"})));
    assert!(reason.contains("$.value.<values>"), "{reason}");
}

/// Every other `patternProperties` leaves a member's declaration to a pattern match, which the
/// projection does not run, so it is refused with the reason rather than read past.
#[test]
fn a_pattern_map_that_is_not_keyed_is_refused() {
    let string = json!({"type": "string"});
    for (schema, reason_contains) in [
        (
            json!({"type": "object", "patternProperties": [], "additionalProperties": false}),
            "exactly one key pattern",
        ),
        (
            json!({"type": "object", "patternProperties": {"^a$": string, "^b$": string}, "additionalProperties": false}),
            "exactly one key pattern",
        ),
        (
            json!({"type": "object", "patternProperties": {"a": string}, "additionalProperties": false}),
            "not anchored",
        ),
        (
            json!({"type": "object", "patternProperties": {"^a\\$": string}, "additionalProperties": false}),
            "not anchored",
        ),
        (
            json!({"type": "object", "patternProperties": {"^a$": string}}),
            "`additionalProperties: false` alone",
        ),
        (
            json!({"type": "object", "patternProperties": {"^a$": string}, "additionalProperties": string}),
            "`additionalProperties: false` alone",
        ),
        (
            json!({"type": "object", "patternProperties": {"^a$": string}, "additionalProperties": false, "properties": {"b": string}}),
            "beside named properties",
        ),
        (
            json!({"type": "object", "patternProperties": {"^a$": true}, "additionalProperties": false}),
            "not a schema object",
        ),
        (
            json!({"type": "string", "patternProperties": {"a": string}}),
            "not anchored",
        ),
    ] {
        let reason = refused(schema);
        assert!(reason.contains(reason_contains), "{reason}");
    }
}
