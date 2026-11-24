//! How a datum's sidecar JSON Schema may change between two published copies.
//!
//! The sidecar carries what Lexicon cannot: the `x-automerge` storage forms, and the constraints
//! that narrow a Lexicon type. A change is breaking when a document that was valid and readable
//! under the old copy may no longer be, and compatible when the new copy admits everything the old
//! one did and reads it the same way (ADR 021, "Sidecars have compatibility rules too"):
//!
//! * a storage form (`x-automerge`) changing, appearing or disappearing is breaking, since the
//!   documents already stored in the old form no longer read;
//! * narrowing a typed map (its value schema, or an untyped map becoming typed) or tightening a
//!   constraint (a bound, a `pattern`, a `format`, an `enum`, a `const`) is breaking, and loosening
//!   one is compatible;
//! * a new property is compatible (that it carries its `since` is `evolution`'s rule), a removed
//!   one is breaking, and a property becoming or ceasing to be `required` is breaking either way,
//!   as it is for Lexicon;
//! * a union's members are matched by their `$type`, never by position.
//!
//! The classifier fails closed: a keyword it has no rule for is breaking when it differs. Only
//! prose and the stability keywords themselves (judged per item by `evolution`) are ignored.

use crate::evolution::Difference;
use crate::evolution::Severity;
use crate::items;
use big_cabinet_core::datum::lexicon::stability;
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

/// Every difference between two copies of one sidecar, by path into the sidecar with the datum's
/// own definition as the root (see [`items::main_form`]).
pub fn classify(old: &Value, new: &Value) -> Vec<Difference> {
    let (old, new) = (items::main_form(old), items::main_form(new));
    let mut found = Vec::new();
    Walk {
        old_defs: old.get("$defs"),
        new_defs: new.get("$defs"),
        found: &mut found,
    }
    .schema("", &old, &new);
    found
}

struct Walk<'a> {
    old_defs: Option<&'a Value>,
    new_defs: Option<&'a Value>,
    found: &'a mut Vec<Difference>,
}

/// Which way a bound or constraint tightens.
#[derive(Clone, Copy, PartialEq)]
enum Bound {
    /// A floor: raising it tightens.
    Lower,
    /// A ceiling: lowering it tightens.
    Upper,
}

impl Walk<'_> {
    fn push(&mut self, path: &str, severity: Severity, what: String) {
        self.found.push(Difference {
            path: path.to_owned(),
            severity,
            what,
        });
    }

    fn schema(&mut self, path: &str, old: &Value, new: &Value) {
        if old == new {
            return;
        }
        let (Value::Object(old), Value::Object(new)) = (old, new) else {
            // A boolean schema: `true` admits everything and `false` nothing.
            let severity = match (old, new) {
                (Value::Bool(false), Value::Bool(true) | Value::Object(_))
                | (Value::Object(_), Value::Bool(true)) => Severity::Compatible,
                _ => Severity::Breaking,
            };
            self.push(path, severity, format!("changed from {old} to {new}"));
            return;
        };
        let keys: BTreeSet<&String> = old.keys().chain(new.keys()).collect();
        for key in keys {
            let at = format!("{path}/{key}");
            let (before, after) = (old.get(key), new.get(key));
            if before == after {
                continue;
            }
            match key.as_str() {
                "description" | "title" | "$schema" | "$comment" | "examples" => {}
                // Judged per item by `evolution`.
                stability::SINCE | stability::DEPRECATED | stability::UNSTABLE => {}
                "x-automerge" => self.push(
                    &at,
                    Severity::Breaking,
                    format!(
                        "the storage form changed from {} to {}; stored documents no longer read",
                        show(before),
                        show(after)
                    ),
                ),
                "properties" | "$defs" | "patternProperties" => {
                    self.named(&at, key == "patternProperties", before, after)
                }
                "required" => self.names(&at, before, after, "required", Severity::Breaking),
                "minimum" | "exclusiveMinimum" | "minLength" | "minItems" | "minProperties"
                | "minContains" => self.bound(&at, Bound::Lower, before, after),
                "maximum" | "exclusiveMaximum" | "maxLength" | "maxItems" | "maxProperties"
                | "maxContains" => self.bound(&at, Bound::Upper, before, after),
                // A constraint that cannot be compared (a regular expression, a divisor): removing
                // it loosens, and any other change might tighten.
                "pattern" | "format" | "multipleOf" | "const" | "uniqueItems"
                | "contentEncoding" | "contentMediaType" => {
                    let (severity, what) = match (before, after) {
                        (Some(_), None) => (Severity::Compatible, "removed".to_owned()),
                        _ => (
                            Severity::Breaking,
                            format!(
                                "constraint changed from {} to {}",
                                show(before),
                                show(after)
                            ),
                        ),
                    };
                    self.push(&at, severity, what);
                }
                "enum" => self.values(&at, before, after),
                "type" => self.types(&at, before, after),
                "additionalProperties" | "unevaluatedProperties" => {
                    // An absent catch-all admits any member.
                    let open = Value::Bool(true);
                    let (before, after) = (before.unwrap_or(&open), after.unwrap_or(&open));
                    self.schema(&at, before, after);
                }
                "oneOf" | "anyOf" => self.branches(path, key, before, after),
                _ => match (before, after) {
                    (Some(before), Some(after)) => self.schema(&at, before, after),
                    _ => self.push(
                        &at,
                        Severity::Breaking,
                        format!("keyword changed from {} to {}", show(before), show(after)),
                    ),
                },
            }
        }
    }

    /// A map from a name (a property, a definition, a key pattern) to a schema. A name that appears
    /// is a new member, which is compatible for a property or definition and breaking for a key
    /// pattern, since it narrows the keys a typed map admits; one that disappears is breaking.
    fn named(&mut self, at: &str, narrows: bool, before: Option<&Value>, after: Option<&Value>) {
        let empty = serde_json::Map::new();
        let as_map = |value: Option<&'_ Value>| match value {
            Some(Value::Object(map)) => map.clone(),
            _ => empty.clone(),
        };
        let (before, after) = (as_map(before), as_map(after));
        for (name, after) in &after {
            let child = format!("{at}/{name}");
            match before.get(name) {
                Some(before) => self.schema(&child, before, after),
                None if narrows => self.push(
                    &child,
                    Severity::Breaking,
                    "a key pattern was added, which narrows the typed map".to_owned(),
                ),
                None => self.push(&child, Severity::Compatible, "added".to_owned()),
            }
        }
        for name in before.keys().filter(|name| !after.contains_key(*name)) {
            self.push(
                &format!("{at}/{name}"),
                Severity::Breaking,
                "removed or renamed".to_owned(),
            );
        }
    }

    fn names(
        &mut self,
        at: &str,
        before: Option<&Value>,
        after: Option<&Value>,
        what: &str,
        severity: Severity,
    ) {
        let names = |value: Option<&'_ Value>| -> BTreeSet<String> {
            value
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|name| name.as_str().map(str::to_owned))
                .collect()
        };
        let (before, after) = (names(before), names(after));
        for name in after.difference(&before) {
            self.push(
                &format!("{at}/{name}"),
                severity,
                format!("`{name}` became {what}"),
            );
        }
        for name in before.difference(&after) {
            self.push(
                &format!("{at}/{name}"),
                severity,
                format!("`{name}` is no longer {what}"),
            );
        }
    }

    fn bound(&mut self, at: &str, side: Bound, before: Option<&Value>, after: Option<&Value>) {
        let number = |value: Option<&'_ Value>| value.and_then(Value::as_f64);
        let (severity, what) = match (before, after) {
            (None, Some(_)) => (Severity::Breaking, "a bound was added".to_owned()),
            (Some(_), None) => (Severity::Compatible, "the bound was removed".to_owned()),
            (Some(_), Some(_)) => match (number(before), number(after)) {
                (Some(old), Some(new)) => {
                    let raised = new.partial_cmp(&old).unwrap_or(Ordering::Equal);
                    match (side, raised) {
                        (Bound::Lower, Ordering::Greater) | (Bound::Upper, Ordering::Less) => {
                            (Severity::Breaking, format!("tightened from {old} to {new}"))
                        }
                        _ => (
                            Severity::Compatible,
                            format!("loosened from {old} to {new}"),
                        ),
                    }
                }
                _ => (
                    Severity::Breaking,
                    "a bound that is not a number".to_owned(),
                ),
            },
            (None, None) => return,
        };
        self.push(at, severity, what);
    }

    /// `enum`: a value that appears loosens, one that disappears tightens.
    fn values(&mut self, at: &str, before: Option<&Value>, after: Option<&Value>) {
        let list = |value: Option<&'_ Value>| value.and_then(Value::as_array).cloned();
        match (list(before), list(after)) {
            (None, Some(_)) => {
                self.push(at, Severity::Breaking, "a value set was added".to_owned())
            }
            (Some(_), None) => self.push(
                at,
                Severity::Compatible,
                "the value set was removed".to_owned(),
            ),
            (Some(before), Some(after)) => {
                for value in after.iter().filter(|value| !before.contains(value)) {
                    self.push(at, Severity::Compatible, format!("value {value} admitted"));
                }
                for value in before.iter().filter(|value| !after.contains(value)) {
                    self.push(
                        at,
                        Severity::Breaking,
                        format!("value {value} no longer admitted"),
                    );
                }
            }
            (None, None) => self.push(at, Severity::Breaking, "`enum` is not a list".to_owned()),
        }
    }

    /// `type`: admitting more types loosens, admitting fewer tightens.
    fn types(&mut self, at: &str, before: Option<&Value>, after: Option<&Value>) {
        let set = |value: Option<&'_ Value>| -> Option<BTreeSet<String>> {
            match value? {
                Value::String(kind) => Some(BTreeSet::from([kind.clone()])),
                Value::Array(kinds) => kinds
                    .iter()
                    .map(|kind| kind.as_str().map(str::to_owned))
                    .collect(),
                _ => None,
            }
        };
        let (severity, what) = match (set(before), set(after)) {
            (Some(old), Some(new)) if old.is_subset(&new) => {
                (Severity::Compatible, format!("now also admits {new:?}"))
            }
            (Some(old), Some(new)) => (
                Severity::Breaking,
                format!("type changed from {old:?} to {new:?}"),
            ),
            _ => (
                Severity::Breaking,
                format!("type changed from {} to {}", show(before), show(after)),
            ),
        };
        self.push(at, severity, what);
    }

    /// A union, or the `anyOf` an `Option` is. Branches are matched by what they are (a union
    /// member's `$type`, `fallback`, `null`), never by position, and a list that cannot be matched
    /// that way is compared whole.
    fn branches<'b>(
        &mut self,
        path: &str,
        key: &str,
        before: Option<&'b Value>,
        after: Option<&'b Value>,
    ) {
        let at = format!("{path}/{key}");
        let keyed = |value: Option<&'b Value>, defs: Option<&Value>| {
            let list = value?.as_array()?;
            let mut keyed = BTreeMap::new();
            let wrapper = {
                let rest = list.iter().filter(|branch| !is_null(branch)).count();
                rest == 1
                    && !list
                        .iter()
                        .any(|b| b.pointer("/properties/$type").is_some())
            };
            for branch in list {
                let name = if is_null(branch) {
                    "null".to_owned()
                } else if wrapper {
                    "value".to_owned()
                } else if let Some(id) = items::member_id(branch, defs) {
                    id.to_owned()
                } else if branch.pointer("/properties/$type/not").is_some() {
                    "fallback".to_owned()
                } else {
                    return None;
                };
                if keyed.insert(name, (branch, wrapper)).is_some() {
                    return None;
                }
            }
            Some(keyed)
        };
        let (Some(old), Some(new)) = (keyed(before, self.old_defs), keyed(after, self.new_defs))
        else {
            self.push(
                &at,
                Severity::Breaking,
                "the branches cannot be matched by their members, so any change to them is \
                 treated as breaking"
                    .to_owned(),
            );
            return;
        };
        let open = new.contains_key("fallback");
        for (name, (branch, wrapper)) in &new {
            let child = if *wrapper {
                path.to_owned()
            } else {
                format!("{path}/$union/{name}")
            };
            match old.get(name) {
                // The fallback's `$type` lists every known member, so it changes with each one.
                Some(_) if name == "fallback" => {}
                Some((before, _)) => self.schema(&child, before, branch),
                None if name == "null" || name == "fallback" || open => self.push(
                    &child,
                    Severity::Compatible,
                    format!("branch `{name}` added"),
                ),
                None => self.push(
                    &child,
                    Severity::Breaking,
                    format!("a closed union gained member `{name}`"),
                ),
            }
        }
        for name in old.keys().filter(|name| !new.contains_key(*name)) {
            let child = format!("{path}/$union/{name}");
            self.push(
                &child,
                Severity::Breaking,
                format!("branch `{name}` was removed"),
            );
        }
    }
}

fn is_null(branch: &Value) -> bool {
    branch.get("type") == Some(&Value::from("null"))
}

fn show(value: Option<&Value>) -> String {
    value.map_or_else(|| "nothing".to_owned(), Value::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A datum whose one property is `property`; plain strings stand for the property's schema.
    fn datum(property: Value) -> Value {
        json!({"type": "object", "properties": {"a": property}})
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
            let (old, new) = (datum(old), datum(new));
            assert_eq!(overall(&old, &new), expected, "{:?}", classify(&old, &new));
        }
    }

    classifies! {
        identical: (json!({"type": "string"}), json!({"type": "string"}), "same"),
        prose_is_ignored: (
            json!({"type": "string", "description": "one"}),
            json!({"type": "string", "description": "two", "title": "A"}),
            "same"
        ),
        a_storage_form_changing_is_breaking: (
            json!({"type": "string", "x-automerge": "text"}),
            json!({"type": "string", "x-automerge": "timestamp"}),
            "breaking"
        ),
        a_storage_form_appearing_is_breaking: (
            json!({"type": "string"}),
            json!({"type": "string", "x-automerge": "text"}),
            "breaking"
        ),
        a_storage_form_disappearing_is_breaking: (
            json!({"type": "string", "x-automerge": "text"}),
            json!({"type": "string"}),
            "breaking"
        ),
        a_new_property_is_compatible: (
            json!({"type": "object", "properties": {"x": {"type": "string"}}}),
            json!({"type": "object", "properties": {"x": {"type": "string"}, "y": {"type": "string", "x-since": 2}}}),
            "compatible"
        ),
        a_removed_property_is_breaking: (
            json!({"type": "object", "properties": {"x": {"type": "string"}, "y": {"type": "string"}}}),
            json!({"type": "object", "properties": {"x": {"type": "string"}}}),
            "breaking"
        ),
        a_property_becoming_required_is_breaking: (
            json!({"type": "object", "properties": {"x": {"type": "string"}}}),
            json!({"type": "object", "properties": {"x": {"type": "string"}}, "required": ["x"]}),
            "breaking"
        ),
        a_property_ceasing_to_be_required_is_breaking: (
            json!({"type": "object", "properties": {"x": {"type": "string"}}, "required": ["x"]}),
            json!({"type": "object", "properties": {"x": {"type": "string"}}}),
            "breaking"
        ),
        narrowing_a_typed_maps_values_is_breaking: (
            json!({"type": "object", "additionalProperties": {"type": ["string", "integer"]}}),
            json!({"type": "object", "additionalProperties": {"type": "string"}}),
            "breaking"
        ),
        widening_a_typed_maps_values_is_compatible: (
            json!({"type": "object", "additionalProperties": {"type": "string"}}),
            json!({"type": "object", "additionalProperties": {"type": ["string", "integer"]}}),
            "compatible"
        ),
        an_open_object_becoming_a_typed_map_is_breaking: (
            json!({"type": "object"}),
            json!({"type": "object", "additionalProperties": {"type": "string"}}),
            "breaking"
        ),
        a_typed_map_becoming_an_open_object_is_compatible: (
            json!({"type": "object", "additionalProperties": {"type": "string"}}),
            json!({"type": "object"}),
            "compatible"
        ),
        a_closed_object_opening_is_compatible: (
            json!({"type": "object", "additionalProperties": false}),
            json!({"type": "object", "additionalProperties": true}),
            "compatible"
        ),
        an_object_closing_is_breaking: (
            json!({"type": "object"}),
            json!({"type": "object", "additionalProperties": false}),
            "breaking"
        ),
        a_new_key_pattern_is_breaking: (
            json!({"type": "object", "patternProperties": {"^a": {"type": "string"}}}),
            json!({"type": "object", "patternProperties": {"^a": {"type": "string"}, "^b": {"type": "string"}}}),
            "breaking"
        ),
        a_raised_minimum_is_breaking: (
            json!({"type": "integer", "minimum": 0}),
            json!({"type": "integer", "minimum": 1}),
            "breaking"
        ),
        a_lowered_minimum_is_compatible: (
            json!({"type": "integer", "minimum": 1}),
            json!({"type": "integer", "minimum": 0}),
            "compatible"
        ),
        a_lowered_maximum_is_breaking: (
            json!({"type": "string", "maxLength": 8}),
            json!({"type": "string", "maxLength": 4}),
            "breaking"
        ),
        a_raised_maximum_is_compatible: (
            json!({"type": "string", "maxLength": 4}),
            json!({"type": "string", "maxLength": 8}),
            "compatible"
        ),
        a_new_bound_is_breaking: (
            json!({"type": "string"}),
            json!({"type": "string", "maxLength": 8}),
            "breaking"
        ),
        a_removed_bound_is_compatible: (
            json!({"type": "string", "maxLength": 8}),
            json!({"type": "string"}),
            "compatible"
        ),
        a_new_pattern_is_breaking: (
            json!({"type": "string"}),
            json!({"type": "string", "pattern": "^a"}),
            "breaking"
        ),
        a_changed_pattern_is_breaking: (
            json!({"type": "string", "pattern": "^a"}),
            json!({"type": "string", "pattern": "^b"}),
            "breaking"
        ),
        a_removed_pattern_is_compatible: (
            json!({"type": "string", "pattern": "^a"}),
            json!({"type": "string"}),
            "compatible"
        ),
        a_new_enum_value_is_compatible: (
            json!({"type": "string", "enum": ["x"]}),
            json!({"type": "string", "enum": ["x", "y"]}),
            "compatible"
        ),
        a_lost_enum_value_is_breaking: (
            json!({"type": "string", "enum": ["x", "y"]}),
            json!({"type": "string", "enum": ["x"]}),
            "breaking"
        ),
        a_type_widening_is_compatible: (
            json!({"type": "string"}),
            json!({"type": ["string", "integer"]}),
            "compatible"
        ),
        a_type_narrowing_is_breaking: (
            json!({"type": ["string", "integer"]}),
            json!({"type": "string"}),
            "breaking"
        ),
        a_different_type_is_breaking: (
            json!({"type": "string"}),
            json!({"type": "integer"}),
            "breaking"
        ),
        a_changed_reference_is_breaking: (
            json!({"$ref": "#/$defs/One"}),
            json!({"$ref": "#/$defs/Two"}),
            "breaking"
        ),
        an_unknown_keyword_is_breaking_when_it_differs: (
            json!({"type": "string", "x-future": 1}),
            json!({"type": "string", "x-future": 2}),
            "breaking"
        ),
        an_unknown_keyword_appearing_is_breaking: (
            json!({"type": "string"}),
            json!({"type": "string", "x-future": 2}),
            "breaking"
        ),
        a_nullable_type_inside_anyof_narrowing_is_breaking: (
            json!({"anyOf": [{"type": "string", "maxLength": 8}, {"type": "null"}]}),
            json!({"anyOf": [{"type": "string", "maxLength": 4}, {"type": "null"}]}),
            "breaking"
        ),
        an_optional_becoming_nullable_only_is_breaking: (
            json!({"anyOf": [{"type": "string"}, {"type": "null"}]}),
            json!({"anyOf": [{"type": "string"}]}),
            "breaking"
        ),
        an_open_union_gaining_a_member_is_compatible: (open_union(&["a.one"]), open_union(&["a.one", "a.two"]), "compatible"),
        an_open_union_losing_a_member_is_breaking: (open_union(&["a.one", "a.two"]), open_union(&["a.one"]), "breaking"),
        a_closed_union_gaining_a_member_is_breaking: (closed_union(&["a.one"]), closed_union(&["a.one", "a.two"]), "breaking"),
        a_union_closing_is_breaking: (open_union(&["a.one"]), closed_union(&["a.one"]), "breaking"),
        a_union_opening_is_compatible: (closed_union(&["a.one"]), open_union(&["a.one"]), "compatible"),
        a_union_reordering_its_members_is_the_same: (
            open_union(&["a.one", "a.two"]),
            open_union(&["a.two", "a.one"]),
            "same"
        ),
    }

    fn member(id: &str) -> Value {
        json!({"type": "object", "properties": {"$type": {"const": id}}})
    }

    fn open_union(ids: &[&str]) -> Value {
        let mut branches: Vec<Value> = ids.iter().map(|id| member(id)).collect();
        branches.push(json!({
            "type": "object",
            "properties": {"$type": {"type": "string", "not": {"enum": ids}}},
        }));
        json!({"oneOf": branches})
    }

    fn closed_union(ids: &[&str]) -> Value {
        json!({"oneOf": ids.iter().map(|id| member(id)).collect::<Vec<_>>()})
    }

    #[test]
    fn a_datum_rendered_as_a_reference_is_compared_by_its_definition() {
        let rendered = |maximum: u64| {
            json!({
                "$ref": "#/$defs/Memo",
                "$defs": {"Memo": {
                    "type": "object",
                    "properties": {"a": {"type": "string", "maxLength": maximum}},
                }},
            })
        };
        let differences = classify(&rendered(8), &rendered(4));
        assert_eq!(differences.len(), 1, "{differences:?}");
        assert_eq!(differences[0].path, "/properties/a/maxLength");
        assert_eq!(differences[0].severity, Severity::Breaking);
    }
}
