//! An integer position is a plain Lexicon integer: `minimum`, `maximum`, `enum`, `const` and
//! `default` are all it states. Rust's widths never reach this module: `Declaration` turns the
//! `format: uint32` and friends `schemars` emits into bounds before a schema is admitted, and a
//! `format` left on an integer position is refused as an unsupported supplement format.

use super::*;

pub(super) fn check_supplement(object: &Map<String, Value>, path: &str) -> Result<()> {
    let integer = object.get("type").is_some_and(|kind| {
        kind == "integer"
            || kind.as_array().is_some_and(|kinds| {
                kinds.iter().any(|kind| kind == "integer")
                    && kinds.iter().all(|kind| kind == "integer" || kind == "null")
            })
    });
    if (object.contains_key("minimum") || object.contains_key("maximum"))
        && [
            "properties",
            "required",
            "items",
            "anyOf",
            "oneOf",
            "additionalProperties",
            "patternProperties",
            "not",
        ]
        .iter()
        .any(|key| object.contains_key(*key))
    {
        return Err(invalid(
            path,
            "integer bounds cannot annotate structural positions",
        ));
    }
    for key in ["minimum", "maximum"] {
        if let Some(value) = object.get(key)
            && (!integer || value.as_i64().is_none())
        {
            return Err(invalid(
                path,
                format!("`{key}` requires a signed-64-bit integer bound"),
            ));
        }
    }
    if let (Some(minimum), Some(maximum)) = (
        object.get("minimum").and_then(Value::as_i64),
        object.get("maximum").and_then(Value::as_i64),
    ) && minimum > maximum
    {
        return Err(invalid(path, "inverted integer bounds"));
    }
    Ok(())
}

pub(super) fn check_overlay(
    primary: &Value,
    object: &Map<String, Value>,
    path: &str,
) -> Result<()> {
    for key in ["minimum", "maximum"] {
        if let Some(value) = object.get(key)
            && primary.get(key) != Some(value)
        {
            return Err(invalid(
                path,
                format!("supplement `{key}` contradicts the primary integer bounds"),
            ));
        }
    }
    Ok(())
}
