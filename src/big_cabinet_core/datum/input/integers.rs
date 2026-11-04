use super::*;

pub(super) const FORMATS: &[&str] = &[
    "int", "int8", "int16", "int32", "int64", "int128", "uint", "uint8", "uint16", "uint32",
    "uint64", "uint128",
];

pub(super) fn check_supplement(object: &Map<String, Value>, path: &str) -> Result<()> {
    let integer = object.get("type").is_some_and(|kind| {
        kind == "integer"
            || kind.as_array().is_some_and(|kinds| {
                kinds.iter().any(|kind| kind == "integer")
                    && kinds.iter().all(|kind| kind == "integer" || kind == "null")
            })
    });
    if let Some(format) = object.get("format").and_then(Value::as_str)
        && FORMATS.contains(&format)
        && !integer
        && !(format == "uint64"
            && object.get("x-automerge").and_then(Value::as_str) == Some("uint"))
    {
        return Err(invalid(path, "integer format requires an integer position"));
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
    if primary["type"] == "integer"
        && object
            .get("format")
            .and_then(Value::as_str)
            .is_some_and(|format| format.starts_with("uint"))
        && primary
            .get("minimum")
            .and_then(Value::as_i64)
            .unwrap_or(i64::MIN)
            < 0
    {
        return Err(invalid(
            path,
            "unsigned integer format contradicts the primary integer bounds",
        ));
    }
    Ok(())
}
