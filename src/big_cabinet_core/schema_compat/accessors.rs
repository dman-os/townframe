use super::Findings;
use super::SchemaBundle;
use serde_json::Value;
use std::collections::BTreeSet;

pub(super) fn resolve<'a>(
    bundle: &'a SchemaBundle,
    schema: &'a Value,
    path: &str,
    direction: &str,
    findings: &mut Findings,
) -> Option<&'a Value> {
    let mut current = schema;
    let mut seen = BTreeSet::new();
    loop {
        let Some(reference) = current.get("$ref").and_then(Value::as_str) else {
            return Some(current);
        };
        if !seen.insert(reference.to_owned()) {
            findings.not_established(
                path,
                format!("{direction}: cyclic schema reference `{reference}`"),
            );
            return None;
        }
        let name = reference
            .strip_prefix("#/$defs/")
            .or_else(|| reference.strip_prefix("#/definitions/"));
        let Some(name) = name.map(unescape_pointer_segment) else {
            findings.not_established(
                path,
                format!("{direction}: external schema reference `{reference}` is unavailable"),
            );
            return None;
        };
        let Some(target) = bundle.definitions.get(&name) else {
            findings.not_established(
                path,
                format!("{direction}: schema reference `{reference}` is undeclared"),
            );
            return None;
        };
        current = target;
    }
}

pub(super) fn types(schema: &Value) -> Option<Vec<String>> {
    match schema.get("type") {
        Some(Value::String(kind)) => Some(vec![kind.clone()]),
        Some(Value::Array(kinds)) => Some(
            kinds
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect(),
        ),
        _ => None,
    }
}

pub(super) fn has_type(schema: &Value, kind: &str) -> bool {
    types(schema).is_some_and(|kinds| kinds.iter().any(|item| item == kind))
}

pub(super) fn required(schema: &Value) -> BTreeSet<String> {
    schema
        .get("required")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect()
}

pub(super) enum Additional<'a> {
    Allowed,
    Closed,
    Schema(&'a Value),
    Unknown,
}

pub(super) fn additional(schema: &Value) -> Additional<'_> {
    match schema.get("additionalProperties") {
        Some(Value::Bool(true)) => Additional::Allowed,
        Some(Value::Bool(false)) => Additional::Closed,
        Some(value) => Additional::Schema(value),
        None => Additional::Unknown,
    }
}

fn unescape_pointer_segment(segment: &str) -> String {
    segment.replace("~1", "/").replace("~0", "~")
}

pub(super) fn child(path: &str, name: &str) -> String {
    let escaped = name.replace('~', "~0").replace('/', "~1");
    if path == "/" {
        format!("/{escaped}")
    } else {
        format!("{path}/{escaped}")
    }
}
