//! The Lexicon schema grammar both Lexicon consumers share: the datum compiler (`input`) and the
//! operation contract validator (`operation`) read the same field vocabulary per type, the same
//! string/integer/boolean constraint rules, the same length bounds and the same reference
//! spelling, so a schema that admits in one is never refused by the other on grammar grounds.
//!
//! A check here is one position deep — recursion, depth limits and resource limits belong to the
//! caller, which owns the file it walks.

use super::is_nsid;
use serde_json::Map;
use serde_json::Value;
use std::collections::BTreeSet;

/// A schema position that departs from the Lexicon grammar.
#[derive(Debug, thiserror::Error)]
#[error("invalid Lexicon at `{path}`: {reason}")]
pub struct GrammarError {
    pub path: String,
    pub reason: String,
}

type Result<T> = std::result::Result<T, GrammarError>;

pub(crate) fn invalid(path: impl Into<String>, reason: impl Into<String>) -> GrammarError {
    GrammarError {
        path: path.into(),
        reason: reason.into(),
    }
}

/// The string formats Lexicon names. Anything else is refused: a format no consumer knows is not
/// a narrowing anyone may rely on.
const STRING_FORMATS: [&str; 10] = [
    "at-identifier",
    "at-uri",
    "cid",
    "datetime",
    "did",
    "handle",
    "nsid",
    "record-key",
    "tid",
    "uri",
];

/// Checks one schema position against the grammar and returns its Lexicon `type`.
///
/// A record is a legal position here: `key` is a string and `record` is an object schema. Whether
/// it may stand at this place — a named main definition in a datum file, nowhere in an operation
/// file — is the caller's policy, decided after this check. The caller reads the parts it recurses
/// into from the same object; they are present because this check passed.
pub(crate) fn check_position<'a>(value: &'a Value, path: &str) -> Result<&'a str> {
    let object = value
        .as_object()
        .ok_or_else(|| invalid(path, "a schema position is an object"))?;
    let kind = string(object, "type", path)?;
    let fields: &[&str] = match kind {
        "record" => &["key", "record"],
        "object" => &["properties", "required", "nullable"],
        "array" => &["items", "minLength", "maxLength"],
        "string" => &[
            "format",
            "minLength",
            "maxLength",
            "minGraphemes",
            "maxGraphemes",
            "knownValues",
            "enum",
            "const",
            "default",
        ],
        "integer" => &["minimum", "maximum", "enum", "const", "default"],
        "boolean" => &["const", "default"],
        "bytes" => &["minLength", "maxLength"],
        "cid-link" | "unknown" => &[],
        "ref" => &["ref"],
        "union" => &["refs", "closed"],
        "blob" => return Err(invalid(path, "blob schemas are deliberately unsupported")),
        _ => return Err(invalid(path, format!("unsupported Lexicon type `{kind}`"))),
    };
    for key in object.keys() {
        if !matches!(key.as_str(), "type" | "description") && !fields.contains(&key.as_str()) {
            return Err(invalid(path, format!("`{key}` is not a field of `{kind}`")));
        }
    }
    if object
        .get("description")
        .is_some_and(|value| !value.is_string())
    {
        return Err(invalid(path, "description must be a string"));
    }
    length_bounds(object, path)?;
    match kind {
        "record" => {
            string(object, "key", path)?;
            object
                .get("record")
                .filter(|record| record["type"] == "object")
                .ok_or_else(|| invalid(path, "record requires an object schema"))?;
            Ok(kind)
        }
        "object" => {
            object_position(object, path)?;
            Ok(kind)
        }
        "array" => {
            if object.get("items").is_none() {
                return Err(invalid(path, "array items are required"));
            }
            Ok(kind)
        }
        "ref" => {
            string(object, "ref", path)?;
            Ok(kind)
        }
        "union" => {
            union_position(object, path)?;
            Ok(kind)
        }
        "bytes" | "cid-link" | "unknown" => Ok(kind),
        "string" | "integer" | "boolean" => {
            primitive_constraints(kind, object, path)?;
            Ok(kind)
        }
        _ => unreachable!("the field table admitted this kind"),
    }
}

/// The length and grapheme bounds: non-negative integers, and a minimum that does not exceed its
/// maximum.
fn length_bounds(object: &Map<String, Value>, path: &str) -> Result<()> {
    for (min, max) in [("minLength", "maxLength"), ("minGraphemes", "maxGraphemes")] {
        for key in [min, max] {
            if object
                .get(key)
                .is_some_and(|value| value.as_u64().is_none())
            {
                return Err(invalid(
                    path,
                    format!("`{key}` must be a non-negative integer"),
                ));
            }
        }
        if let (Some(minimum), Some(maximum)) = (
            object.get(min).and_then(Value::as_u64),
            object.get(max).and_then(Value::as_u64),
        ) && minimum > maximum
        {
            return Err(invalid(path, "inverted length bounds"));
        }
    }
    Ok(())
}

fn object_position(object: &Map<String, Value>, path: &str) -> Result<()> {
    let properties = object
        .get("properties")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid(path, "object properties are required"))?;
    let required = names(object, "required", path)?;
    let nullable = names(object, "nullable", path)?;
    for name in required.iter().chain(&nullable) {
        if !properties.contains_key(*name) {
            return Err(invalid(
                path,
                format!("`{name}` is not a declared property"),
            ));
        }
    }
    for name in properties.keys() {
        if name.starts_with('$') {
            return Err(invalid(
                path,
                "application properties cannot use reserved dollar keys",
            ));
        }
    }
    Ok(())
}

fn union_position(object: &Map<String, Value>, path: &str) -> Result<()> {
    let values = object
        .get("refs")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid(path, "union refs must be an array"))?;
    let closed = match object.get("closed") {
        None => false,
        Some(Value::Bool(closed)) => *closed,
        _ => return Err(invalid(path, "closed must be boolean")),
    };
    if closed && values.is_empty() {
        return Err(invalid(path, "a closed union cannot be empty"));
    }
    if values.iter().any(|value| !value.is_string()) {
        return Err(invalid(path, "union references must be strings"));
    }
    Ok(())
}

/// A string, integer or boolean position: its constraint values checked against the kind, its
/// format against the supported vocabulary.
fn primitive_constraints(kind: &str, object: &Map<String, Value>, path: &str) -> Result<()> {
    if object.contains_key("const") && object.contains_key("default") {
        return Err(invalid(path, "const and default are mutually exclusive"));
    }
    let fits = |value: &Value| match kind {
        "string" => value.is_string(),
        "integer" => value.as_i64().is_some(),
        "boolean" => value.is_boolean(),
        _ => unreachable!("only primitives carry constraints"),
    };
    for key in ["const", "default", "minimum", "maximum"] {
        if object.get(key).is_some_and(|value| !fits(value)) {
            return Err(invalid(path, format!("invalid `{key}` value")));
        }
    }
    if let (Some(min), Some(max)) = (
        object.get("minimum").and_then(Value::as_i64),
        object.get("maximum").and_then(Value::as_i64),
    ) && min > max
    {
        return Err(invalid(path, "inverted integer bounds"));
    }
    for key in ["enum", "knownValues"] {
        if let Some(values) = object.get(key)
            && !values
                .as_array()
                .is_some_and(|values| values.iter().all(fits))
        {
            return Err(invalid(path, format!("invalid `{key}` list")));
        }
    }
    if let Some(format) = object.get("format") {
        let format = format
            .as_str()
            .ok_or_else(|| invalid(path, "format must be a string"))?;
        if !STRING_FORMATS.contains(&format) {
            return Err(invalid(
                path,
                format!("unsupported Lexicon string format `{format}`"),
            ));
        }
    }
    Ok(())
}

pub(crate) fn string<'a>(object: &'a Map<String, Value>, key: &str, path: &str) -> Result<&'a str> {
    object
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| invalid(path, format!("`{key}` must be a string")))
}

/// A `required` or `nullable` name list: strings, without duplicates.
pub(crate) fn names<'a>(
    object: &'a Map<String, Value>,
    key: &str,
    path: &str,
) -> Result<BTreeSet<&'a str>> {
    let Some(values) = object.get(key) else {
        return Ok(BTreeSet::new());
    };
    let values = values
        .as_array()
        .ok_or_else(|| invalid(path, format!("`{key}` must be a string array")))?;
    let mut out = BTreeSet::new();
    for name in values {
        let name = name
            .as_str()
            .ok_or_else(|| invalid(path, format!("`{key}` names must be strings")))?;
        if !out.insert(name) {
            return Err(invalid(path, format!("duplicate `{key}` name")));
        }
    }
    Ok(out)
}

/// Splits a Lexicon reference into its file and definition: `nsid` is `(Some(nsid), None)`,
/// `nsid#def` is `(Some(nsid), Some(def))`, the file-local `#def` is `(None, Some(def))`.
pub fn parse_ref(target: &str) -> Option<(Option<&str>, Option<&str>)> {
    let (file, def) = match target.split_once('#') {
        Some((file, def)) => (file, Some(def)),
        None => (target, None),
    };
    let file = if file.is_empty() {
        None
    } else {
        if !is_nsid(file) {
            return None;
        }
        Some(file)
    };
    if let Some(def) = def {
        let mut chars = def.chars();
        let starts_with_letter = chars
            .next()
            .is_some_and(|first| first.is_ascii_alphabetic());
        if !starts_with_letter || !chars.all(|char| char.is_ascii_alphanumeric()) {
            return None;
        }
    }
    if file.is_none() && def.is_none() {
        return None;
    }
    Some((file, def))
}
