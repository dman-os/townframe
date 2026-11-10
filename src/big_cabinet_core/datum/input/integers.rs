use super::*;

pub(super) const FORMATS: &[&str] = &[
    "int", "int8", "int16", "int32", "int64", "int128", "uint", "uint8", "uint16", "uint32",
    "uint64", "uint128",
];

/// The inclusive range of a format narrower than a signed 64-bit integer. Wider formats are
/// enforced only by the unsigned-implies-non-negative rule, because every bound is an `i64`.
fn narrow_range(format: &str) -> Option<(i64, i64)> {
    Some(match format {
        "int8" => (i64::from(i8::MIN), i64::from(i8::MAX)),
        "int16" => (i64::from(i16::MIN), i64::from(i16::MAX)),
        "int32" => (i64::from(i32::MIN), i64::from(i32::MAX)),
        "uint8" => (0, i64::from(u8::MAX)),
        "uint16" => (0, i64::from(u16::MAX)),
        "uint32" => (0, i64::from(u32::MAX)),
        _ => return None,
    })
}

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
    if let Some((low, high)) = object
        .get("format")
        .and_then(Value::as_str)
        .and_then(narrow_range)
        && (object
            .get("minimum")
            .and_then(Value::as_i64)
            .is_some_and(|minimum| minimum < low)
            || object
                .get("maximum")
                .and_then(Value::as_i64)
                .is_some_and(|maximum| maximum > high))
    {
        return Err(invalid(
            path,
            "integer bounds exceed the range of the declared format",
        ));
    }
    Ok(())
}

pub(super) fn check_overlay(
    primary: &mut Value,
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
    // A narrow format must be enforceable at the value level. Bounds the primary declares must
    // already lie inside the format's range, or the format would promise a width the primary
    // does not keep. Bounds it leaves out are the integer's i64 defaults, so they are tightened
    // to the format's range instead; that is what makes `u8`, `i32` and the like reject
    // out-of-width values when schemars states only `format` (and `minimum: 0` for unsigned).
    if let Some((low, high)) = object
        .get("format")
        .and_then(Value::as_str)
        .and_then(narrow_range)
        && primary["type"] == "integer"
    {
        let declared = |key: &str| primary[KEYWORD].get(key).and_then(Value::as_i64);
        if declared("minimum").is_some_and(|minimum| minimum < low)
            || declared("maximum").is_some_and(|maximum| maximum > high)
        {
            return Err(invalid(
                path,
                "the primary integer bounds exceed the range of the declared format",
            ));
        }
        let tightened = |key: &str, bound: i64, keep: fn(i64, i64) -> i64| {
            keep(primary[key].as_i64().unwrap_or(bound), bound)
        };
        let (minimum, maximum) = (
            tightened("minimum", low, i64::max),
            tightened("maximum", high, i64::min),
        );
        primary["minimum"] = json!(minimum);
        primary["maximum"] = json!(maximum);
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
