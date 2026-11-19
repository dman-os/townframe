//! How a Lexicon may change between two published copies.
//!
//! A change is compatible when a reader built against the old copy still reads every value the
//! new copy admits: a new optional property, a new `knownValues` entry, a new member of an open
//! union. Anything else that changes the wire shape is breaking, and a breaking change mints a new
//! NSID instead of editing this one (operator rule, 2026-11-06). The classifier fails closed: a
//! keyword it has no compatible rule for counts as breaking when it differs. Only `description`,
//! prose a reader never validates against, is ignored.

use serde_json::Map;
use serde_json::Value;
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Compatible,
    Breaking,
}

/// One difference between two copies of a Lexicon, at a JSON path inside it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Difference {
    pub path: String,
    pub severity: Severity,
    pub what: String,
}

/// Every wire-visible difference between the `old` and `new` copies of one Lexicon.
pub fn classify(old: &Value, new: &Value) -> Vec<Difference> {
    let mut found = Vec::new();
    walk("", old, new, &mut found);
    found
}

/// What stops `new` from replacing `old` under the same NSID: a breaking difference, or
/// compatible differences without a bumped `revision`. An absent `revision` counts as 0.
pub fn violations(nsid: &str, old: &Value, new: &Value) -> Vec<String> {
    let differences = classify(old, new);
    let breaking: Vec<_> = differences
        .iter()
        .filter(|difference| difference.severity == Severity::Breaking)
        .map(|difference| {
            format!(
                "{nsid}: breaking change at `{}`: {}; a breaking change mints a new NSID",
                difference.path, difference.what
            )
        })
        .collect();
    if !breaking.is_empty() {
        return breaking;
    }
    let revision = |lexicon: &Value| lexicon["revision"].as_u64().unwrap_or(0);
    if differences.is_empty() || revision(new) > revision(old) {
        return Vec::new();
    }
    let listed: Vec<_> = differences
        .iter()
        .map(|difference| format!("`{}` {}", difference.path, difference.what))
        .collect();
    vec![format!(
        "{nsid}: compatible changes ({}) need a bumped `revision`, still {}",
        listed.join("; "),
        revision(old)
    )]
}

fn push(found: &mut Vec<Difference>, path: &str, severity: Severity, what: String) {
    found.push(Difference {
        path: path.to_owned(),
        severity,
        what,
    });
}

fn child(path: &str, key: &str) -> String {
    format!("{path}/{key}")
}

fn strings(value: Option<&Value>) -> BTreeSet<&str> {
    value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect()
}

/// Reports each member of `after` not in `before` as `added` and each member of `before` not in
/// `after` as `removed`, with the severity each side is given.
fn set_diff(
    found: &mut Vec<Difference>,
    path: &str,
    sets: (BTreeSet<&str>, BTreeSet<&str>),
    what: (&str, &str),
    severity: (Severity, Severity),
) {
    let (before, after) = sets;
    for name in after.difference(&before) {
        push(found, path, severity.0, format!("{} `{name}`", what.0));
    }
    for name in before.difference(&after) {
        push(found, path, severity.1, format!("{} `{name}`", what.1));
    }
}

/// The `name` of each entry of a method's `errors`.
fn error_names(value: Option<&Value>) -> BTreeSet<&str> {
    value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|error| error["name"].as_str())
        .collect()
}

fn walk(path: &str, old: &Value, new: &Value, found: &mut Vec<Difference>) {
    if old == new {
        return;
    }
    let (Value::Object(old), Value::Object(new)) = (old, new) else {
        push(
            found,
            path,
            Severity::Breaking,
            format!("changed from {old} to {new}"),
        );
        return;
    };
    if old.get("type") != new.get("type") {
        let show = |object: &Map<String, Value>| {
            object
                .get("type")
                .map_or_else(|| "none".to_owned(), Value::to_string)
        };
        push(
            found,
            path,
            Severity::Breaking,
            format!("type changed from {} to {}", show(old), show(new)),
        );
        return;
    }
    let keys: BTreeSet<&String> = old.keys().chain(new.keys()).collect();
    for key in keys {
        let at = child(path, key);
        let (before, after) = (old.get(key), new.get(key));
        let names = || (strings(before), strings(after));
        match key.as_str() {
            "description" => {}
            // Read by `violations`, which asks that a compatible change bump it.
            "revision" if path.is_empty() => {}
            // Maps from a name to a schema: a name that appears is a new optional member (it is
            // `required` that makes it mandatory), one that disappears is removed or renamed.
            "properties" | "defs" => {
                let empty = Map::new();
                let as_map = |value: Option<&Value>| {
                    value.and_then(Value::as_object).unwrap_or(&empty).clone()
                };
                let (before, after) = (as_map(before), as_map(after));
                for (name, after) in &after {
                    match before.get(name) {
                        Some(before) => walk(&child(&at, name), before, after, found),
                        None => push(
                            found,
                            &child(&at, name),
                            Severity::Compatible,
                            "added".to_owned(),
                        ),
                    }
                }
                for name in before.keys().filter(|name| !after.contains_key(*name)) {
                    push(
                        found,
                        &child(&at, name),
                        Severity::Breaking,
                        "removed or renamed".to_owned(),
                    );
                }
            }
            "required" => set_diff(
                found,
                &at,
                names(),
                ("became required:", "is no longer required:"),
                (Severity::Breaking, Severity::Breaking),
            ),
            "knownValues" => set_diff(
                found,
                &at,
                names(),
                ("new known value", "known value removed:"),
                (Severity::Compatible, Severity::Breaking),
            ),
            "enum" => set_diff(
                found,
                &at,
                names(),
                ("closed enum gained", "closed enum lost"),
                (Severity::Breaking, Severity::Breaking),
            ),
            // An open union admits members its readers do not know, so a new member is
            // compatible there; a closed union's readers reject it. Losing a member is breaking
            // either way: values of it may be stored.
            "refs" => set_diff(
                found,
                &at,
                names(),
                ("union gained member", "union lost member"),
                (
                    if new.get("closed") == Some(&Value::Bool(true)) {
                        Severity::Breaking
                    } else {
                        Severity::Compatible
                    },
                    Severity::Breaking,
                ),
            ),
            // Closing a union makes its readers reject what they accepted; opening one only
            // permits members nobody has added yet.
            "closed" => {
                let closed = |value: Option<&Value>| value == Some(&Value::Bool(true));
                let severity = if closed(after) {
                    Severity::Breaking
                } else {
                    Severity::Compatible
                };
                if closed(before) != closed(after) {
                    push(
                        found,
                        &at,
                        severity,
                        format!("closed is now {}", closed(after)),
                    );
                }
            }
            "errors" => {
                set_diff(
                    found,
                    &at,
                    (error_names(before), error_names(after)),
                    ("error added:", "error removed:"),
                    (Severity::Compatible, Severity::Breaking),
                );
            }
            // Any other keyword (a bound, a format, a body's encoding, a nested schema).
            _ => match (before, after) {
                (Some(before), Some(after)) => walk(&at, before, after, found),
                (Some(_), None) => push(found, &at, Severity::Breaking, "removed".to_owned()),
                (None, Some(_)) => push(found, &at, Severity::Breaking, "added".to_owned()),
                (None, None) => unreachable!("the key came from one of the two"),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A record Lexicon whose one object has `object`'s keywords, `revision` and a `defs` entry.
    fn record(object: Value) -> Value {
        json!({
            "lexicon": 1, "id": "x.y.z",
            "defs": { "main": { "type": "record", "key": "any", "record": object } },
        })
    }

    /// The Lexicon with `revision` stated.
    fn at_revision(mut lexicon: Value, revision: u64) -> Value {
        lexicon["revision"] = json!(revision);
        lexicon
    }

    fn object(properties: Value, required: Value) -> Value {
        json!({ "type": "object", "required": required, "properties": properties })
    }

    fn overall(old: &Value, new: &Value) -> &'static str {
        let differences = classify(old, new);
        if differences.is_empty() {
            "same"
        } else if differences
            .iter()
            .any(|difference| difference.severity == Severity::Breaking)
        {
            "breaking"
        } else {
            "compatible"
        }
    }

    utils_rs::table_tests! {
        classifies,
        (old, new, expected),
        {
            assert_eq!(overall(&old, &new), expected, "{:?}", classify(&old, &new));
        }
    }

    classifies! {
        identical: (
            record(object(json!({"a": {"type": "string"}}), json!(["a"]))),
            record(object(json!({"a": {"type": "string"}}), json!(["a"]))),
            "same"
        ),
        description_is_ignored: (
            record(object(json!({"a": {"type": "string", "description": "one"}}), json!([]))),
            record(object(json!({"a": {"type": "string", "description": "two"}}), json!([]))),
            "same"
        ),
        new_optional_property_is_compatible: (
            record(object(json!({"a": {"type": "string"}}), json!(["a"]))),
            record(object(json!({"a": {"type": "string"}, "b": {"type": "integer"}}), json!(["a"]))),
            "compatible"
        ),
        new_required_property_is_breaking: (
            record(object(json!({"a": {"type": "string"}}), json!(["a"]))),
            record(object(json!({"a": {"type": "string"}, "b": {"type": "integer"}}), json!(["a", "b"]))),
            "breaking"
        ),
        optional_property_becoming_required_is_breaking: (
            record(object(json!({"a": {"type": "string"}}), json!([]))),
            record(object(json!({"a": {"type": "string"}}), json!(["a"]))),
            "breaking"
        ),
        removed_property_is_breaking: (
            record(object(json!({"a": {"type": "string"}, "b": {"type": "string"}}), json!([]))),
            record(object(json!({"a": {"type": "string"}}), json!([]))),
            "breaking"
        ),
        renamed_property_is_breaking: (
            record(object(json!({"a": {"type": "string"}}), json!([]))),
            record(object(json!({"b": {"type": "string"}}), json!([]))),
            "breaking"
        ),
        changed_type_is_breaking: (
            record(object(json!({"a": {"type": "string"}}), json!([]))),
            record(object(json!({"a": {"type": "integer"}}), json!([]))),
            "breaking"
        ),
        changed_bound_is_breaking: (
            record(object(json!({"a": {"type": "string", "maxLength": 8}}), json!([]))),
            record(object(json!({"a": {"type": "string", "maxLength": 4}}), json!([]))),
            "breaking"
        ),
        changed_ref_is_breaking: (
            record(object(json!({"a": {"type": "ref", "ref": "#one"}}), json!([]))),
            record(object(json!({"a": {"type": "ref", "ref": "#two"}}), json!([]))),
            "breaking"
        ),
        nested_new_optional_property_is_compatible: (
            record(object(json!({"a": object(json!({"x": {"type": "string"}}), json!([]))}), json!([]))),
            record(object(json!({"a": object(json!({"x": {"type": "string"}, "y": {"type": "string"}}), json!([]))}), json!([]))),
            "compatible"
        ),
        nested_changed_type_is_breaking: (
            record(object(json!({"a": {"type": "array", "items": {"type": "string"}}}), json!([]))),
            record(object(json!({"a": {"type": "array", "items": {"type": "integer"}}}), json!([]))),
            "breaking"
        ),
        new_known_value_is_compatible: (
            record(object(json!({"a": {"type": "string", "knownValues": ["x"]}}), json!([]))),
            record(object(json!({"a": {"type": "string", "knownValues": ["x", "y"]}}), json!([]))),
            "compatible"
        ),
        lost_known_value_is_breaking: (
            record(object(json!({"a": {"type": "string", "knownValues": ["x", "y"]}}), json!([]))),
            record(object(json!({"a": {"type": "string", "knownValues": ["x"]}}), json!([]))),
            "breaking"
        ),
        closed_enum_losing_a_value_is_breaking: (
            record(object(json!({"a": {"type": "string", "enum": ["x", "y"]}}), json!([]))),
            record(object(json!({"a": {"type": "string", "enum": ["x"]}}), json!([]))),
            "breaking"
        ),
        closed_enum_gaining_a_value_is_breaking: (
            record(object(json!({"a": {"type": "string", "enum": ["x"]}}), json!([]))),
            record(object(json!({"a": {"type": "string", "enum": ["x", "y"]}}), json!([]))),
            "breaking"
        ),
        open_union_gaining_a_member_is_compatible: (
            record(object(json!({"a": {"type": "union", "refs": ["#one"]}}), json!([]))),
            record(object(json!({"a": {"type": "union", "refs": ["#one", "#two"]}}), json!([]))),
            "compatible"
        ),
        open_union_losing_a_member_is_breaking: (
            record(object(json!({"a": {"type": "union", "refs": ["#one", "#two"]}}), json!([]))),
            record(object(json!({"a": {"type": "union", "refs": ["#one"]}}), json!([]))),
            "breaking"
        ),
        closed_union_losing_a_member_is_breaking: (
            record(object(json!({"a": {"type": "union", "closed": true, "refs": ["#one", "#two"]}}), json!([]))),
            record(object(json!({"a": {"type": "union", "closed": true, "refs": ["#one"]}}), json!([]))),
            "breaking"
        ),
        closed_union_gaining_a_member_is_breaking: (
            record(object(json!({"a": {"type": "union", "closed": true, "refs": ["#one"]}}), json!([]))),
            record(object(json!({"a": {"type": "union", "closed": true, "refs": ["#one", "#two"]}}), json!([]))),
            "breaking"
        ),
        union_becoming_closed_is_breaking: (
            record(object(json!({"a": {"type": "union", "refs": ["#one"]}}), json!([]))),
            record(object(json!({"a": {"type": "union", "closed": true, "refs": ["#one"]}}), json!([]))),
            "breaking"
        ),
        union_becoming_open_is_compatible: (
            record(object(json!({"a": {"type": "union", "closed": true, "refs": ["#one"]}}), json!([]))),
            record(object(json!({"a": {"type": "union", "closed": false, "refs": ["#one"]}}), json!([]))),
            "compatible"
        ),
        removed_def_is_breaking: (
            json!({"defs": {"main": {"type": "object", "properties": {}}, "one": {"type": "string"}}}),
            json!({"defs": {"main": {"type": "object", "properties": {}}}}),
            "breaking"
        ),
        new_def_is_compatible: (
            json!({"defs": {"main": {"type": "object", "properties": {}}}}),
            json!({"defs": {"main": {"type": "object", "properties": {}}, "one": {"type": "string"}}}),
            "compatible"
        ),
        new_method_error_is_compatible: (
            json!({"defs": {"main": {"type": "query", "errors": [{"name": "A"}]}}}),
            json!({"defs": {"main": {"type": "query", "errors": [{"name": "A"}, {"name": "B"}]}}}),
            "compatible"
        ),
        removed_method_error_is_breaking: (
            json!({"defs": {"main": {"type": "query", "errors": [{"name": "A"}, {"name": "B"}]}}}),
            json!({"defs": {"main": {"type": "query", "errors": [{"name": "A"}]}}}),
            "breaking"
        ),
        a_property_named_revision_is_not_the_revision: (
            record(object(json!({"revision": {"type": "integer"}}), json!([]))),
            record(object(json!({"revision": {"type": "string"}}), json!([]))),
            "breaking"
        ),
    }

    utils_rs::table_tests! {
        judges,
        (old, new, verdict),
        {
            let found = violations("x.y.z", &old, &new);
            match verdict {
                "ok" => assert!(found.is_empty(), "{found:?}"),
                needle => assert!(
                    found.iter().any(|line| line.contains(needle)),
                    "no `{needle}` in {found:?}",
                ),
            }
        }
    }

    judges! {
        no_change_needs_no_bump: (
            record(object(json!({}), json!([]))),
            record(object(json!({}), json!([]))),
            "ok"
        ),
        compatible_change_with_bump_passes: (
            at_revision(record(object(json!({}), json!([]))), 1),
            at_revision(record(object(json!({"a": {"type": "string"}}), json!([]))), 2),
            "ok"
        ),
        compatible_change_first_bump_from_no_revision_passes: (
            record(object(json!({}), json!([]))),
            at_revision(record(object(json!({"a": {"type": "string"}}), json!([]))), 1),
            "ok"
        ),
        compatible_change_without_bump_fails: (
            at_revision(record(object(json!({}), json!([]))), 1),
            at_revision(record(object(json!({"a": {"type": "string"}}), json!([]))), 1),
            "need a bumped `revision`"
        ),
        compatible_change_with_no_revision_at_all_fails: (
            record(object(json!({}), json!([]))),
            record(object(json!({"a": {"type": "string"}}), json!([]))),
            "need a bumped `revision`"
        ),
        breaking_change_fails_even_with_a_bump: (
            at_revision(record(object(json!({"a": {"type": "string"}}), json!([]))), 1),
            at_revision(record(object(json!({"a": {"type": "integer"}}), json!([]))), 2),
            "mints a new NSID"
        ),
        breaking_change_beside_a_compatible_one_fails: (
            record(object(json!({"a": {"type": "string"}}), json!([]))),
            at_revision(record(object(json!({"b": {"type": "string"}}), json!([]))), 3),
            "removed or renamed"
        ),
    }
}
