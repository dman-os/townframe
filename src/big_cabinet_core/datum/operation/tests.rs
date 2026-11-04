use super::*;
use serde_json::json;

fn defs(value: Value) -> Map<String, Value> {
    value.as_object().unwrap().clone()
}

fn refuse(kind: OperationKind, value: Value, needle: &str) {
    let error = validate(kind, &defs(value), "defs")
        .unwrap_err()
        .to_string();
    assert!(error.contains(needle), "{error}");
}

/// A query main wrapped around one schema position, so the position is admitted as an output body.
fn output_schema(schema: Value) -> Value {
    json!({
        "main": {
            "type": "query",
            "output": { "encoding": "application/json", "schema": schema },
        },
    })
}

/// One schema position carried as an object property, so a body schema reaches it: a body schema is
/// an object, a ref or a union, and the grammar check applies to every position below it.
fn output_position(schema: Value) -> Value {
    output_schema(json!({ "type": "object", "properties": { "p": schema } }))
}

/// A query main wrapped around one parameter position.
fn param(property: Value) -> Value {
    json!({
        "main": {
            "type": "query",
            "parameters": { "type": "params", "properties": { "p": property } },
        },
    })
}

#[test]
fn the_three_kinds_admit_with_their_own_frames() {
    let query = json!({
        "main": {
            "type": "query",
            "description": "Search photos.",
            "parameters": {
                "type": "params",
                "required": ["q"],
                "properties": {
                    "q": { "type": "string" },
                    "limit": { "type": "integer", "minimum": 1 },
                    "tags": { "type": "array", "items": { "type": "string" } },
                },
            },
            "output": {
                "encoding": "application/json",
                "schema": {
                    "type": "object",
                    "required": ["hits"],
                    "properties": {
                        "hits": { "type": "array", "items": { "type": "ref", "ref": "#hit" } },
                        "cursor": { "type": "string" },
                    },
                },
            },
            "errors": [{ "name": "IndexCold", "description": "Not built yet." }],
        },
        "hit": {
            "type": "object",
            "properties": {
                "photo": { "type": "ref", "ref": "org.example.photos.photo" },
                "score": { "type": "integer" },
                "kind": { "type": "union", "refs": ["org.example.photos.defs#still", "#hit"] },
            },
        },
        "pinned": { "type": "token" },
    });
    let query = defs(query);
    let refs = validate(OperationKind::Query, &query, "defs").unwrap();
    let mut targets: Vec<(&str, &str)> = refs
        .iter()
        .map(|reference| (reference.path.as_str(), reference.target))
        .collect();
    targets.sort();
    assert_eq!(
        targets,
        [
            (
                "defs.hit.properties.kind.refs[0]",
                "org.example.photos.defs#still"
            ),
            ("defs.hit.properties.kind.refs[1]", "#hit"),
            ("defs.hit.properties.photo.ref", "org.example.photos.photo"),
            ("defs.main.output.schema.properties.hits.items.ref", "#hit"),
        ]
    );

    let procedure = json!({
        "main": {
            "type": "procedure",
            "input": { "encoding": "image/*" },
            "output": {
                "encoding": "application/json",
                "schema": { "type": "ref", "ref": "org.example.photos.photo" },
            },
        },
    });
    validate(OperationKind::Procedure, &defs(procedure), "defs").unwrap();

    let subscription = json!({
        "main": {
            "type": "subscription",
            "parameters": { "type": "params", "properties": { "cursor": { "type": "integer" } } },
            "message": {
                "schema": { "type": "union", "refs": ["#added", "#removed"] },
            },
        },
        "added": { "type": "object", "properties": {} },
        "removed": { "type": "object", "properties": {} },
    });
    validate(OperationKind::Subscription, &defs(subscription), "defs").unwrap();
}

#[test]
fn a_frame_is_refused_where_it_departs_from_its_kind() {
    refuse(
        OperationKind::Query,
        json!({ "main": { "type": "query", "input": { "encoding": "application/json" } } }),
        "`input` is not a field here",
    );
    refuse(
        OperationKind::Subscription,
        json!({ "main": { "type": "subscription" } }),
        "states the message",
    );
    refuse(
        OperationKind::Procedure,
        json!({ "main": { "type": "procedure", "input": { "schema": { "type": "object", "properties": {} } } } }),
        "`encoding` is a string",
    );
    refuse(
        OperationKind::Procedure,
        json!({ "main": { "type": "procedure", "input": { "encoding": "text/plain", "schema": { "type": "object", "properties": {} } } } }),
        "not `text/plain`",
    );
    refuse(
        OperationKind::Query,
        output_schema(json!({ "type": "string" })),
        "an object, a ref or a union",
    );
    refuse(
        OperationKind::Query,
        param(json!({ "type": "object", "properties": {} })),
        "`object` is not",
    );
    refuse(
        OperationKind::Query,
        json!({ "main": { "type": "query", "parameters": { "type": "params", "required": ["missing"], "properties": {} } } }),
        "`missing` is not a declared property",
    );
    refuse(
        OperationKind::Query,
        json!({ "main": { "type": "query", "errors": [{ "name": "A" }, { "name": "A" }] } }),
        "error `A` is declared twice",
    );
    refuse(
        OperationKind::Query,
        json!({ "main": { "type": "query" }, "other": { "type": "record", "record": { "type": "object", "properties": {} } } }),
        "one main",
    );
    refuse(
        OperationKind::Query,
        output_schema(json!({ "type": "ref", "ref": "not an nsid#x" })),
        "not a Lexicon reference",
    );
    refuse(
        OperationKind::Query,
        output_schema(json!({ "type": "object", "properties": { "b": { "type": "blob" } } })),
        "blob schemas",
    );
}

/// The schema positions of an operation are checked with the same grammar the datum compiler
/// compiles against: a malformed constraint is refused at admission, not at call time.
#[test]
fn a_schema_position_is_refused_where_its_constraints_malform() {
    for (schema, needle) in [
        (
            json!({ "type": "string", "minLength": "one" }),
            "`minLength` must be a non-negative integer",
        ),
        (
            json!({ "type": "string", "minLength": 5, "maxLength": 1 }),
            "inverted length bounds",
        ),
        (
            json!({ "type": "string", "format": "email" }),
            "unsupported Lexicon string format `email`",
        ),
        (
            json!({ "type": "string", "format": 7 }),
            "format must be a string",
        ),
        (
            json!({ "type": "string", "knownValues": [1] }),
            "invalid `knownValues` list",
        ),
        (
            json!({ "type": "string", "const": "a", "default": "b" }),
            "const and default are mutually exclusive",
        ),
        (
            json!({ "type": "string", "pattern": "x" }),
            "`pattern` is not a field of `string`",
        ),
        (
            json!({ "type": "object" }),
            "object properties are required",
        ),
        (
            json!({ "type": "object", "properties": {}, "required": "x" }),
            "`required` must be a string array",
        ),
        (
            json!({ "type": "object", "properties": {}, "required": ["a"] }),
            "`a` is not a declared property",
        ),
        (
            json!({ "type": "object", "properties": { "$x": { "type": "string" } } }),
            "reserved dollar keys",
        ),
        (json!({ "type": "array" }), "array items are required"),
        (json!({ "type": "union" }), "union refs must be an array"),
        (
            json!({ "type": "union", "refs": [], "closed": true }),
            "a closed union cannot be empty",
        ),
        (
            json!({ "type": "union", "refs": [7] }),
            "union references must be strings",
        ),
        (
            json!({ "type": "union", "refs": [], "closed": "yes" }),
            "closed must be boolean",
        ),
        (
            json!({ "type": "record", "key": "tid", "record": { "type": "object", "properties": {} } }),
            "a record must be a named main definition",
        ),
        (
            json!({ "type": "graph" }),
            "unsupported Lexicon type `graph`",
        ),
    ] {
        refuse(OperationKind::Query, output_position(schema), needle);
    }
    for (property, needle) in [
        (
            json!({ "type": "integer", "minimum": "one" }),
            "invalid `minimum` value",
        ),
        (
            json!({ "type": "integer", "minimum": 5, "maximum": 1 }),
            "inverted integer bounds",
        ),
        (
            json!({ "type": "integer", "enum": ["one"] }),
            "invalid `enum` list",
        ),
        (
            json!({ "type": "integer", "default": "one" }),
            "invalid `default` value",
        ),
        (
            json!({ "type": "string", "maxGraphemes": -1 }),
            "`maxGraphemes` must be a non-negative integer",
        ),
        (
            json!({ "type": "boolean", "const": "yes" }),
            "invalid `const` value",
        ),
        (
            json!({ "type": "array", "items": { "type": "integer", "minimum": "one" } }),
            "invalid `minimum` value",
        ),
        (
            json!({ "type": "string", "minLength": 1, "maxLength": 0 }),
            "inverted length bounds",
        ),
    ] {
        refuse(OperationKind::Query, param(property), needle);
    }
}

#[test]
fn a_lexicon_reference_has_three_spellings() {
    assert_eq!(
        parse_ref("org.example.photos.photo"),
        Some((Some("org.example.photos.photo"), None))
    );
    assert_eq!(
        parse_ref("org.example.photos.defs#still"),
        Some((Some("org.example.photos.defs"), Some("still")))
    );
    assert_eq!(parse_ref("#hit"), Some((None, Some("hit"))));
    for bad in ["", "#", "#1hit", "photos#hit", "org.example.photos#a-b"] {
        assert_eq!(parse_ref(bad), None, "{bad}");
    }
}
