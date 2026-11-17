//! How a Lexicon may change between two published copies.
//!
//! A change is compatible when a reader built against the old copy still reads every value the
//! new copy admits: a new optional property, a new `knownValues` entry, a new member of an open
//! union. Anything else that changes the wire shape is breaking, and a breaking change mints a new
//! NSID instead of editing this one (operator rule, 2026-11-06). The classifier fails closed: a
//! keyword it has no compatible rule for counts as breaking when it differs. Only `description`,
//! prose a reader never validates against, is ignored.
//!
//! Stability is declared per item (ADR 021): a field, definition, method or union member is under
//! the rules only when it states `since` (or `deprecated`). A stable item is never removed or
//! changed incompatibly, and a new one carries `since` equal to the file's new `revision`. Every
//! other item is unstable, the pre-release default, and is outside the rules until it is
//! stabilized.

use crate::items;
use big_cabinet_core::datum::lexicon::stability::Declared;
use serde_json::Map;
use serde_json::Value;
use std::collections::BTreeMap;
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

/// A published Lexicon and the sidecar it ships with, if it has one.
#[derive(Debug, Clone, Copy)]
pub struct Surface<'a> {
    pub lexicon: &'a Value,
    pub sidecar: Option<&'a Value>,
}

impl<'a> Surface<'a> {
    pub fn of(lexicon: &'a Value) -> Self {
        Self {
            lexicon,
            sidecar: None,
        }
    }

    fn revision(&self) -> u64 {
        self.lexicon["revision"].as_u64().unwrap_or(0)
    }
}

/// Whether the evolution rules reach a difference at `path` of the Lexicon: the nearest item
/// around it decides, and only a stable one (in the old copy, or for a new item in the new one)
/// is under them. A `required` or `nullable` entry belongs to the property it names.
fn governed(
    path: &str,
    old: &BTreeMap<String, Declared>,
    new: &BTreeMap<String, Declared>,
) -> bool {
    let segments: Vec<&str> = path.split('/').collect();
    let owner = segments
        .iter()
        .position(|segment| matches!(*segment, "required" | "nullable"))
        .filter(|at| at + 1 < segments.len())
        .map(|at| {
            format!(
                "{}/properties/{}",
                segments[..at].join("/"),
                segments[at + 1]
            )
        });
    let path = owner.as_deref().unwrap_or(path);
    old.keys()
        .chain(new.keys())
        .map(|item| (items::lexicon_path(item), item))
        .filter(|(at, _)| {
            path.strip_prefix(at.as_str())
                .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
        })
        .max_by_key(|(at, _)| at.len())
        .is_some_and(|(_, item)| {
            old.get(item)
                .or_else(|| new.get(item))
                .is_some_and(Declared::is_stable)
        })
}

/// What stops `new` from replacing `old` under the same NSID: a breaking difference, a stable item
/// that was removed or changed, a new item that does not carry the file's new revision as its
/// `since`, or compatible differences without a bumped `revision`. An absent `revision` counts as
/// 0. Differences that sit in an unstable item are outside the rules.
pub fn violations(nsid: &str, old: Surface, new: Surface) -> Vec<String> {
    let (old_items, new_items) = (items::items(&old), items::items(&new));
    let differences: Vec<Difference> = classify(old.lexicon, new.lexicon)
        .into_iter()
        .filter(|difference| governed(&difference.path, &old_items, &new_items))
        .collect();
    let mut found: Vec<String> = differences
        .iter()
        .filter(|difference| difference.severity == Severity::Breaking)
        .map(|difference| {
            format!(
                "{nsid}: breaking change at `{}`: {}; a breaking change mints a new NSID",
                difference.path, difference.what
            )
        })
        .collect();
    found.extend(item_violations(
        nsid,
        &old_items,
        &new_items,
        (old.revision(), new.revision()),
    ));
    if !found.is_empty() {
        return found;
    }
    if differences.is_empty() || new.revision() > old.revision() {
        return Vec::new();
    }
    let listed: Vec<_> = differences
        .iter()
        .map(|difference| format!("`{}` {}", difference.path, difference.what))
        .collect();
    vec![format!(
        "{nsid}: compatible changes ({}) need a bumped `revision`, still {}",
        listed.join("; "),
        old.revision()
    )]
}

/// The rules about items themselves, which the shape of the Lexicon cannot show: nothing stable is
/// removed or made unstable, a stable item keeps its `since`, and a new stable item carries the
/// new revision.
fn item_violations(
    nsid: &str,
    old: &BTreeMap<String, Declared>,
    new: &BTreeMap<String, Declared>,
    (old_revision, revision): (u64, u64),
) -> Vec<String> {
    let mut found = Vec::new();
    let named = |path: &str| {
        if path.is_empty() {
            "the file".to_owned()
        } else {
            format!("`{path}`")
        }
    };
    // An item inside an item that is itself removed (or new) is covered by the outer one.
    let inside = |path: &str, outer: &dyn Fn(&str) -> bool| {
        (0..path.len())
            .filter(|at| path.as_bytes()[*at] == b'/')
            .any(|at| at > 0 && outer(&path[..at]))
    };
    for (path, before) in old {
        if !before.is_stable() {
            continue;
        }
        match new.get(path) {
            None if inside(path, &|outer| {
                old.contains_key(outer) && !new.contains_key(outer)
            }) => {}
            None => found.push(format!(
                "{nsid}: stable item {} was removed; deprecate it instead",
                named(path)
            )),
            Some(after) if !after.is_stable() => found.push(format!(
                "{nsid}: stable item {} became unstable",
                named(path)
            )),
            Some(after) => {
                if before.since != after.since {
                    found.push(format!(
                        "{nsid}: stable item {} changed its `since` from {:?} to {:?}",
                        named(path),
                        before.since,
                        after.since
                    ));
                }
                if let Some((since, _)) = &after.deprecated
                    && before.deprecated.is_none()
                    && *since != revision
                {
                    found.push(format!(
                        "{nsid}: {} is newly deprecated since {since}, and the file's revision \
                         is {revision}",
                        named(path)
                    ));
                }
            }
        }
    }
    let is_new = |path: &str| old.get(path).is_none_or(|before| !before.is_stable());
    for (path, after) in new {
        if path.is_empty()
            || !after.is_stable()
            || !is_new(path)
            || inside(path, &|outer| {
                new.contains_key(outer) && is_new(outer) && new[outer].is_stable()
            })
        {
            continue;
        }
        if after.since != Some(revision) {
            found.push(format!(
                "{nsid}: new stable item {} must carry `since = {revision}`, the file's new \
                 revision (it was {old_revision}), and carries {:?}",
                named(path),
                after.since
            ));
        }
    }
    found
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
        push(
            found,
            &child(path, name),
            severity.0,
            format!("{} `{name}`", what.0),
        );
    }
    for name in before.difference(&after) {
        push(
            found,
            &child(path, name),
            severity.1,
            format!("{} `{name}`", what.1),
        );
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
                &child(path, "$union"),
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
            let found = violations("x.y.z", Surface::of(&old), Surface::of(&new));
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
        a_file_with_no_revision_is_unstable_and_changes_freely: (
            record(object(json!({"a": {"type": "string"}}), json!([]))),
            record(object(json!({"a": {"type": "integer"}}), json!([]))),
            "ok"
        ),
        breaking_change_fails_even_with_a_bump: (
            at_revision(record(object(json!({"a": {"type": "string"}}), json!([]))), 1),
            at_revision(record(object(json!({"a": {"type": "integer"}}), json!([]))), 2),
            "mints a new NSID"
        ),
        breaking_change_beside_a_compatible_one_fails: (
            at_revision(record(object(json!({"a": {"type": "string"}}), json!([]))), 1),
            at_revision(record(object(json!({"b": {"type": "string"}}), json!([]))), 3),
            "removed or renamed"
        ),
    }

    /// A datum's Lexicon and sidecar from `(name, extras)` properties: the extras are the
    /// stability keywords, which only the sidecar carries, and the Lexicon's `revision` is the
    /// highest revision they name. A property named `u` is an open union of `members`, whose own
    /// `extras` are keyed by member id.
    fn datum(properties: Value) -> (Value, Value) {
        let mut lexicon_properties = Map::new();
        let mut sidecar_properties = Map::new();
        let mut revision = 0;
        let mut named = |extras: &Value| {
            let declared = Declared::read(extras.as_object().unwrap()).unwrap();
            revision = revision.max(declared.revision().unwrap_or(0));
        };
        for (name, extras) in properties.as_object().unwrap() {
            let (lexicon, sidecar) = if name == "u" {
                let members: Vec<&str> = extras["members"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|member| member.as_str().unwrap())
                    .collect();
                let mut branches: Vec<Value> = members
                    .iter()
                    .map(|id| {
                        let mut branch = json!({
                            "type": "object",
                            "properties": { "$type": { "const": id } },
                        });
                        if let Some(more) = extras["extras"].get(*id) {
                            named(more);
                            branch
                                .as_object_mut()
                                .unwrap()
                                .extend(more.as_object().unwrap().clone());
                        }
                        branch
                    })
                    .collect();
                branches.push(json!({
                    "type": "object",
                    "properties": { "$type": { "type": "string", "not": { "enum": members } } },
                }));
                (
                    json!({ "type": "union", "refs": members }),
                    json!({ "oneOf": branches }),
                )
            } else {
                named(extras);
                let mut sidecar = json!({ "type": "string" });
                sidecar
                    .as_object_mut()
                    .unwrap()
                    .extend(extras.as_object().unwrap().clone());
                (json!({ "type": "string" }), sidecar)
            };
            lexicon_properties.insert(name.clone(), lexicon);
            sidecar_properties.insert(name.clone(), sidecar);
        }
        let mut lexicon = record(object(Value::Object(lexicon_properties), json!([])));
        if revision > 0 {
            lexicon["revision"] = json!(revision);
        }
        let sidecar = json!({ "type": "object", "properties": sidecar_properties });
        (lexicon, sidecar)
    }

    utils_rs::table_tests! {
        stability,
        (old, new, verdict),
        {
            let (old, new) = (datum(old), datum(new));
            let found = violations(
                "x.y.z",
                Surface { lexicon: &old.0, sidecar: Some(&old.1) },
                Surface { lexicon: &new.0, sidecar: Some(&new.1) },
            );
            match verdict {
                "ok" => assert!(found.is_empty(), "{found:?}"),
                needle => assert!(
                    found.iter().any(|line| line.contains(needle)),
                    "no `{needle}` in {found:?}",
                ),
            }
        }
    }

    stability! {
        an_unannotated_field_is_unstable_and_changes_freely: (
            json!({"a": {}, "b": {}}),
            json!({"a": {"type": "integer"}}),
            "ok"
        ),
        a_new_field_carrying_the_new_revision_is_fine: (
            json!({"a": {"x-since": 1}}),
            json!({"a": {"x-since": 1}, "b": {"x-since": 2}}),
            "ok"
        ),
        a_new_stable_field_below_the_revision_is_refused: (
            json!({"a": {"x-since": 1}}),
            json!({"a": {"x-since": 1}, "b": {"x-since": 2}, "c": {"x-since": 3}}),
            "new stable item `/properties/b` must carry `since = 3`"
        ),
        a_stable_field_is_never_removed: (
            json!({"a": {"x-since": 1}, "b": {"x-since": 1}}),
            json!({"a": {"x-since": 1}}),
            "stable item `/properties/b` was removed; deprecate it instead"
        ),
        a_deprecated_field_may_not_be_removed_either: (
            json!({"a": {}, "b": {"x-since": 1, "x-deprecated": {"since": 3, "note": "use a"}}}),
            json!({"a": {}}),
            "stable item `/properties/b` was removed"
        ),
        a_new_unannotated_field_beside_stable_ones_is_unstable: (
            json!({"a": {"x-since": 1}}),
            json!({"a": {"x-since": 1}, "b": {}}),
            "ok"
        ),
        an_unannotated_field_may_be_dropped_beside_stable_ones: (
            json!({"a": {"x-since": 1}, "b": {}}),
            json!({"a": {"x-since": 1}}),
            "ok"
        ),
        deprecating_at_the_new_revision_is_fine: (
            json!({"a": {}, "b": {"x-since": 1}}),
            json!({"a": {}, "b": {"x-since": 1, "x-deprecated": {"since": 2, "note": "use a"}}}),
            "ok"
        ),
        deprecating_at_another_revision_is_refused: (
            json!({"a": {}, "b": {"x-since": 1}, "c": {"x-since": 2}}),
            json!({"a": {}, "b": {"x-since": 1, "x-deprecated": {"since": 2, "note": "use a"}}, "c": {"x-since": 3}}),
            "newly deprecated since 2, and the file's revision is 3"
        ),
        a_stable_field_cannot_become_unstable: (
            json!({"a": {"x-since": 1}}),
            json!({"a": {"x-unstable": "drafts"}}),
            "stable item `/properties/a` became unstable"
        ),
        a_stable_field_cannot_silently_lose_its_since: (
            json!({"a": {"x-since": 1}}),
            json!({"a": {}}),
            "stable item `/properties/a` became unstable"
        ),
        a_stable_fields_since_is_fixed: (
            json!({"a": {"x-since": 2}}),
            json!({"a": {"x-since": 3}}),
            "changed its `since`"
        ),
        stabilizing_a_field_needs_the_new_revision: (
            json!({"a": {}, "b": {"x-unstable": "drafts"}}),
            json!({"a": {}, "b": {"x-since": 2}}),
            "ok"
        ),
        stabilizing_a_field_below_the_revision_is_refused: (
            json!({"a": {"x-since": 1}, "b": {}}),
            json!({"a": {"x-since": 1}, "b": {"x-since": 2}, "c": {"x-since": 3}}),
            "new stable item `/properties/b` must carry `since = 3`"
        ),
        a_new_union_member_carries_the_new_revision: (
            json!({"u": {"members": ["a.one"], "extras": {"a.one": {"x-since": 1}}}}),
            json!({"u": {"members": ["a.one", "a.two"], "extras": {"a.one": {"x-since": 1}, "a.two": {"x-since": 2}}}}),
            "ok"
        ),
        a_new_stable_union_member_below_the_revision_is_refused: (
            json!({"u": {"members": ["a.one"], "extras": {"a.one": {"x-since": 1}}}}),
            json!({"u": {"members": ["a.one", "a.two"], "extras": {"a.one": {"x-since": 1}, "a.two": {"x-since": 2}}}, "z": {"x-since": 3}}),
            "new stable item `/properties/u/$union/a.two` must carry `since = 3`"
        ),
        a_stable_union_member_is_never_dropped: (
            json!({"u": {"members": ["a.one", "a.two"], "extras": {"a.one": {"x-since": 1}, "a.two": {"x-since": 1}}}}),
            json!({"u": {"members": ["a.one"], "extras": {"a.one": {"x-since": 1}}}}),
            "stable item `/properties/u/$union/a.two` was removed"
        ),
        an_unstable_union_member_may_be_dropped: (
            json!({"u": {"members": ["a.one", "a.two"], "extras": {"a.one": {"x-since": 1}}}}),
            json!({"u": {"members": ["a.one"], "extras": {"a.one": {"x-since": 1}}}}),
            "ok"
        ),
    }

    /// A method has no sidecar, so its stability sits beside its `revision`: a file that names one
    /// is stable, and any other is unstable.
    #[test]
    fn a_file_is_stable_when_it_names_a_revision() {
        let unstable = json!({"defs": {"main": {"type": "query", "errors": [{"name": "A"}]}}});
        let changed = json!({"defs": {"main": {"type": "procedure"}}});
        assert!(violations("x.y.z", Surface::of(&unstable), Surface::of(&changed)).is_empty());

        let stable = at_revision(unstable.clone(), 1);
        let stable_changed = at_revision(changed.clone(), 1);
        assert!(
            !violations("x.y.z", Surface::of(&stable), Surface::of(&stable_changed)).is_empty()
        );
        // Stabilizing is a change nobody is held to; destabilizing is refused.
        let stabilized = violations(
            "x.y.z",
            Surface::of(&unstable),
            Surface::of(&stable_changed),
        );
        assert!(stabilized.is_empty(), "{stabilized:?}");
        let destabilized = violations("x.y.z", Surface::of(&stable), Surface::of(&changed));
        assert!(
            destabilized
                .iter()
                .any(|line| line.contains("the file became unstable")),
            "{destabilized:?}"
        );
    }
}
