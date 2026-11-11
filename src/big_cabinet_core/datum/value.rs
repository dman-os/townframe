//! The value a datum holds that its own schema does not describe, and the shared Lexicon
//! object-schema and value validators.

use super::DatumShape;
use super::Shape;
use super::grammar::GrammarError;
use super::grammar::check_position;
use super::grammar::invalid;
use super::grammar::names;
use super::grammar::parse_ref;
use super::is_nsid;
use serde_json::Map;
use serde_json::Value;
use serde_json::json;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use unicode_segmentation::UnicodeSegmentation;

type Result<T> = std::result::Result<T, GrammarError>;

/// Admits an object schema identified by `target` (`nsid` or `nsid#def`) and every schema position
/// it reaches across `lexicons`.
pub fn validate_object_schema(
    lexicons: &BTreeMap<&str, &Map<String, Value>>,
    target: &str,
    path: &str,
) -> Result<()> {
    let (file, def_name, schema) = resolve_object_target(lexicons, target, path)?;
    let mut active = BTreeSet::new();
    active.insert((file, def_name));
    walk_schema(lexicons, file, schema, path, 0, &mut active)
}

/// Validates `value` against the Lexicon object schema identified by `target` (`nsid` or
/// `nsid#def`) across `lexicons`.
pub fn validate_object_value(
    lexicons: &BTreeMap<&str, &Map<String, Value>>,
    target: &str,
    value: &Value,
    path: &str,
) -> Result<()> {
    let (file, def_name, schema) = resolve_object_target(lexicons, target, path)?;
    let mut active = BTreeSet::new();
    active.insert((file, def_name));
    walk_schema(lexicons, file, schema, path, 0, &mut active)?;
    if !validate_data_model(value) {
        return Err(invalid(path, "value violates the ATProto data model"));
    }
    check_object_instance(lexicons, file, schema, value, path, 0, false)
}

fn resolve_object_target<'a>(
    lexicons: &BTreeMap<&'a str, &'a Map<String, Value>>,
    target: &str,
    path: &str,
) -> Result<(&'a str, &'a str, &'a Value)> {
    let Some((Some(_), def)) = parse_ref(target) else {
        return Err(invalid(
            path,
            format!("`{target}` is not a qualified Lexicon reference"),
        ));
    };
    let (file, def_name, schema) = resolve_def(lexicons, "", target, path)?;
    if check_position(schema, path)? != "object" {
        return Err(invalid(
            path,
            format!(
                "`{file}#{}` must be an object schema",
                def.unwrap_or("main")
            ),
        ));
    }
    Ok((file, def_name, schema))
}

fn resolve_def<'a>(
    lexicons: &BTreeMap<&'a str, &'a Map<String, Value>>,
    current_file: &'a str,
    target: &str,
    path: &str,
) -> Result<(&'a str, &'a str, &'a Value)> {
    let Some((file, def)) = parse_ref(target) else {
        return Err(invalid(
            path,
            format!("`{target}` is not a Lexicon reference: `nsid`, `nsid#def` or `#def`"),
        ));
    };
    let wanted_file = file.unwrap_or(current_file);
    let wanted_def = def.unwrap_or("main");
    let Some((&resolved_file, &defs)) = lexicons.get_key_value(wanted_file) else {
        return Err(invalid(
            path,
            format!("reference `{target}` names unknown Lexicon `{wanted_file}`"),
        ));
    };
    let Some((resolved_def, schema)) = defs.get_key_value(wanted_def) else {
        return Err(invalid(
            path,
            format!("`{resolved_file}` declares no definition `{wanted_def}`"),
        ));
    };
    let Some(kind) = schema.get("type").and_then(Value::as_str) else {
        return Err(invalid(path, "a definition states its type"));
    };
    if matches!(
        kind,
        "unknown" | "ref" | "union" | "params" | "permission" | "token"
    ) {
        return Err(invalid(
            path,
            format!("`{kind}` cannot be a referenced data definition"),
        ));
    }
    Ok((resolved_file, resolved_def.as_str(), schema))
}

fn walk_schema<'a>(
    lexicons: &BTreeMap<&'a str, &'a Map<String, Value>>,
    file: &'a str,
    schema: &'a Value,
    path: &str,
    depth: usize,
    active: &mut BTreeSet<(&'a str, &'a str)>,
) -> Result<()> {
    if depth > 64 {
        return Err(invalid(path, "schema recursion limit exceeded"));
    }
    match check_position(schema, path)? {
        "object" => {
            let properties = schema["properties"]
                .as_object()
                .expect("checked object properties");
            for (name, property) in properties {
                walk_schema(
                    lexicons,
                    file,
                    property,
                    &format!("{path}.properties.{name}"),
                    depth + 1,
                    active,
                )?;
            }
            Ok(())
        }
        "array" => walk_schema(
            lexicons,
            file,
            &schema["items"],
            &format!("{path}.items"),
            depth + 1,
            active,
        ),
        "ref" => {
            let target = schema["ref"].as_str().expect("checked ref");
            let ref_path = format!("{path}.ref");
            let (target_file, target_def, target_schema) =
                resolve_def(lexicons, file, target, &ref_path)?;
            if !active.insert((target_file, target_def)) {
                return Err(invalid(
                    &ref_path,
                    format!("cyclic Lexicon reference `{target_file}#{target_def}`"),
                ));
            }
            if check_position(target_schema, &ref_path)? == "record" {
                return Err(invalid(
                    &ref_path,
                    "an object schema cannot reference a record",
                ));
            }
            walk_schema(
                lexicons,
                target_file,
                target_schema,
                &ref_path,
                depth + 1,
                active,
            )?;
            active.remove(&(target_file, target_def));
            Ok(())
        }
        "union" => {
            let refs = schema["refs"].as_array().expect("checked union refs");
            let mut tags = BTreeSet::new();
            for (index, entry) in refs.iter().enumerate() {
                let target = entry.as_str().expect("checked union ref");
                let ref_path = format!("{path}.refs[{index}]");
                let (target_file, target_def, target_schema) =
                    resolve_def(lexicons, file, target, &ref_path)?;
                if check_position(target_schema, &ref_path)? != "object" {
                    return Err(invalid(
                        &ref_path,
                        format!("union variant `{target}` must be an object definition"),
                    ));
                }
                let tag = if target_def == "main" {
                    target_file.to_owned()
                } else {
                    format!("{target_file}#{target_def}")
                };
                if !tags.insert(tag) {
                    return Err(invalid(&ref_path, "duplicate union variant"));
                }
                if !active.insert((target_file, target_def)) {
                    return Err(invalid(
                        &ref_path,
                        format!("cyclic Lexicon reference `{target_file}#{target_def}`"),
                    ));
                }
                walk_schema(
                    lexicons,
                    target_file,
                    target_schema,
                    &ref_path,
                    depth + 1,
                    active,
                )?;
                active.remove(&(target_file, target_def));
            }
            Ok(())
        }
        "record" => Err(invalid(
            path,
            "a record cannot appear inside an object schema",
        )),
        "string" | "integer" | "boolean" | "bytes" | "cid-link" | "unknown" => Ok(()),
        _ => unreachable!("the grammar admitted this kind"),
    }
}

fn check_value<'a>(
    lexicons: &BTreeMap<&'a str, &'a Map<String, Value>>,
    file: &'a str,
    schema: &'a Value,
    instance: &Value,
    path: &str,
    depth: usize,
) -> Result<()> {
    if depth > 64 {
        return Err(invalid(path, "value recursion limit exceeded"));
    }
    match schema["type"].as_str().expect("checked schema type") {
        "object" => check_object_instance(lexicons, file, schema, instance, path, depth, false),
        "array" => {
            let Some(items) = instance.as_array() else {
                return Err(invalid(path, "expected an array"));
            };
            if !length(schema, "minLength", "maxLength", items.len()) {
                return Err(invalid(path, "array length out of bounds"));
            }
            let item_schema = &schema["items"];
            for (index, item) in items.iter().enumerate() {
                check_value(
                    lexicons,
                    file,
                    item_schema,
                    item,
                    &format!("{path}[{index}]"),
                    depth + 1,
                )?;
            }
            Ok(())
        }
        "ref" => {
            let target = schema["ref"].as_str().expect("checked ref");
            let (target_file, _, target_schema) = resolve_def(lexicons, file, target, path)?;
            check_value(
                lexicons,
                target_file,
                target_schema,
                instance,
                path,
                depth + 1,
            )
        }
        "union" => check_union_instance(lexicons, file, schema, instance, path, depth),
        "string" => {
            let Some(text) = instance.as_str() else {
                return Err(invalid(path, "expected a string"));
            };
            if !check_string_constraints(schema, text) {
                return Err(invalid(path, "string violates Lexicon constraints"));
            }
            Ok(())
        }
        "integer" => {
            if !check_integer_constraints(schema, instance) {
                return Err(invalid(path, "integer violates Lexicon constraints"));
            }
            Ok(())
        }
        "boolean" => {
            if !check_boolean_constraints(schema, instance) {
                return Err(invalid(path, "boolean violates Lexicon constraints"));
            }
            Ok(())
        }
        "bytes" => {
            let Some(bytes) = bytes_value(instance) else {
                return Err(invalid(path, "expected a `$bytes` object"));
            };
            if !check_bytes_length(schema, &bytes) {
                return Err(invalid(path, "bytes length out of bounds"));
            }
            Ok(())
        }
        "cid-link" => {
            let valid = instance
                .as_object()
                .filter(|object| object.len() == 1)
                .and_then(|object| object.get("$link"))
                .and_then(Value::as_str)
                .is_some_and(cid);
            if !valid {
                return Err(invalid(path, "expected a `$link` CID object"));
            }
            Ok(())
        }
        "unknown" => {
            let valid = instance.as_object().is_some_and(|object| {
                !object.contains_key("$bytes")
                    && !object.contains_key("$link")
                    && object.get("$type").and_then(Value::as_str) != Some("blob")
            });
            if !valid {
                return Err(invalid(path, "expected an open data object"));
            }
            Ok(())
        }
        _ => unreachable!("walk_schema verified the schema kind"),
    }
}

fn check_object_instance<'a>(
    lexicons: &BTreeMap<&'a str, &'a Map<String, Value>>,
    file: &'a str,
    schema: &'a Value,
    instance: &Value,
    path: &str,
    depth: usize,
    allow_type_tag: bool,
) -> Result<()> {
    let Some(map) = instance.as_object() else {
        return Err(invalid(path, "expected an object"));
    };
    if map.contains_key("$bytes") || map.contains_key("$link") {
        return Err(invalid(
            path,
            "bytes and cid-link objects are not general objects",
        ));
    }
    let schema_obj = schema.as_object().expect("checked object schema");
    let properties = schema_obj["properties"]
        .as_object()
        .expect("checked object properties");
    let required = names(schema_obj, "required", path)?;
    let nullable = names(schema_obj, "nullable", path)?;
    for &req in &required {
        let prop_path = format!("{path}.{req}");
        match map.get(req) {
            None => {
                return Err(invalid(
                    prop_path,
                    format!("missing required property `{req}`"),
                ));
            }
            Some(Value::Null) if !nullable.contains(req) => {
                return Err(invalid(
                    prop_path,
                    format!("property `{req}` is not nullable"),
                ));
            }
            Some(_) => {}
        }
    }
    for (key, val) in map {
        if allow_type_tag && key == "$type" {
            continue;
        }
        let prop_path = format!("{path}.{key}");
        let Some(prop_schema) = properties.get(key) else {
            return Err(invalid(prop_path, format!("unknown property `{key}`")));
        };
        if val.is_null() {
            if nullable.contains(key.as_str()) {
                continue;
            }
            return Err(invalid(
                prop_path,
                format!("property `{key}` is not nullable"),
            ));
        }
        check_value(lexicons, file, prop_schema, val, &prop_path, depth + 1)?;
    }
    Ok(())
}

fn check_union_instance<'a>(
    lexicons: &BTreeMap<&'a str, &'a Map<String, Value>>,
    file: &'a str,
    schema: &'a Value,
    instance: &Value,
    path: &str,
    depth: usize,
) -> Result<()> {
    let Some(map) = instance.as_object() else {
        return Err(invalid(path, "expected an object for union"));
    };
    if map.contains_key("$bytes") || map.contains_key("$link") {
        return Err(invalid(
            path,
            "bytes and cid-link objects cannot satisfy a union",
        ));
    }
    let Some(tag) = map.get("$type").and_then(Value::as_str) else {
        return Err(invalid(path, "union value requires a string `$type`"));
    };
    if !valid_tag(tag) {
        return Err(invalid(path, format!("invalid union `$type` `{tag}`")));
    }
    let refs = schema["refs"].as_array().expect("checked union refs");
    for entry in refs {
        let target = entry.as_str().expect("checked union ref");
        let (target_file, target_def, target_schema) = resolve_def(lexicons, file, target, path)?;
        let matches_variant = if target_def == "main" {
            tag == target_file
        } else {
            tag.strip_prefix(target_file)
                .and_then(|rest| rest.strip_prefix('#'))
                == Some(target_def)
        };
        if matches_variant {
            return check_object_instance(
                lexicons,
                target_file,
                target_schema,
                instance,
                path,
                depth + 1,
                true,
            );
        }
    }
    if schema
        .get("closed")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Err(invalid(
            path,
            format!("`$type` `{tag}` is not a member of closed union"),
        ));
    }
    Ok(())
}

pub(crate) fn valid_tag(value: &str) -> bool {
    let (id, name) = value.split_once('#').unwrap_or((value, ""));
    is_nsid(id)
        && name != "main"
        && !name.contains('#')
        && (!value.contains('#') || !name.is_empty())
}

pub(crate) fn decode_base64(stated: &str) -> Option<Vec<u8>> {
    data_encoding::BASE64
        .decode(stated.as_bytes())
        .or_else(|_| data_encoding::BASE64_NOPAD.decode(stated.as_bytes()))
        .ok()
}

pub(crate) fn bytes_value(value: &Value) -> Option<Vec<u8>> {
    let object = value.as_object()?;
    if object.len() != 1 {
        return None;
    }
    object.get("$bytes")?.as_str().and_then(decode_base64)
}

pub(crate) fn check_bytes_length(schema: &Value, bytes: &[u8]) -> bool {
    length(schema, "minLength", "maxLength", bytes.len())
}

pub(crate) fn check_string_constraints(schema: &Value, value: &str) -> bool {
    if !length(schema, "minLength", "maxLength", value.len()) {
        return false;
    }
    if (schema.get("minGraphemes").is_some() || schema.get("maxGraphemes").is_some())
        && !length(
            schema,
            "minGraphemes",
            "maxGraphemes",
            value.graphemes(true).count(),
        )
    {
        return false;
    }
    if schema
        .get("const")
        .and_then(Value::as_str)
        .is_some_and(|expected| expected != value)
    {
        return false;
    }
    if schema
        .get("enum")
        .and_then(Value::as_array)
        .is_some_and(|allowed| !allowed.iter().any(|entry| entry.as_str() == Some(value)))
    {
        return false;
    }
    schema
        .get("format")
        .and_then(Value::as_str)
        .is_none_or(|format| valid_format(format, value))
}

pub(crate) fn check_integer_constraints(schema: &Value, instance: &Value) -> bool {
    let Some(number) = instance.as_i64() else {
        return false;
    };
    if schema
        .get("minimum")
        .and_then(Value::as_i64)
        .is_some_and(|min| number < min)
    {
        return false;
    }
    if schema
        .get("maximum")
        .and_then(Value::as_i64)
        .is_some_and(|max| number > max)
    {
        return false;
    }
    if schema
        .get("const")
        .and_then(Value::as_i64)
        .is_some_and(|expected| expected != number)
    {
        return false;
    }
    schema
        .get("enum")
        .and_then(Value::as_array)
        .is_none_or(|allowed| allowed.iter().any(|entry| entry.as_i64() == Some(number)))
}

pub(crate) fn check_boolean_constraints(schema: &Value, instance: &Value) -> bool {
    let Some(flag) = instance.as_bool() else {
        return false;
    };
    if schema
        .get("const")
        .and_then(Value::as_bool)
        .is_some_and(|expected| expected != flag)
    {
        return false;
    }
    schema
        .get("enum")
        .and_then(Value::as_array)
        .is_none_or(|allowed| allowed.iter().any(|entry| entry.as_bool() == Some(flag)))
}

pub(crate) fn validate_data_model(instance: &Value) -> bool {
    let mut pending = vec![(instance, 0)];
    let mut visits = 0;
    while let Some((value, depth)) = pending.pop() {
        visits += 1;
        if visits > 1_000_000 || depth > 1024 {
            return false;
        }
        match value {
            Value::Number(number) if number.as_i64().is_none() => return false,
            Value::Object(object) => {
                if object.contains_key("$bytes") {
                    if bytes_value(value).is_none() {
                        return false;
                    }
                } else if object.contains_key("$link") {
                    if object.len() != 1 || !object["$link"].as_str().is_some_and(cid) {
                        return false;
                    }
                } else {
                    if object.get("$type").is_some_and(|value| !value.is_string()) {
                        return false;
                    }
                    if object.get("$type").and_then(Value::as_str) == Some("blob") {
                        return false;
                    }
                    pending.extend(object.values().map(|value| (value, depth + 1)));
                }
            }
            Value::Array(array) => pending.extend(array.iter().map(|value| (value, depth + 1))),
            _ => {}
        }
    }
    true
}

pub(crate) fn length(schema: &Value, min: &str, max: &str, length: usize) -> bool {
    schema
        .get(min)
        .and_then(Value::as_u64)
        .is_none_or(|min| length as u64 >= min)
        && schema
            .get(max)
            .and_then(Value::as_u64)
            .is_none_or(|max| length as u64 <= max)
}

pub(crate) fn cid(value: &str) -> bool {
    let Some(value) = value.strip_prefix('b') else {
        return false;
    };
    if value.len() != 58
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || (b'2'..=b'7').contains(&byte))
    {
        return false;
    }
    data_encoding::BASE32_NOPAD
        .decode(value.to_ascii_uppercase().as_bytes())
        .is_ok_and(|bytes| {
            bytes.len() == 36
                && bytes[0] == 1
                && matches!(bytes[1], 0x55 | 0x71)
                && bytes[2..4] == [0x12, 0x20]
        })
}

pub(crate) fn label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && !value.starts_with('-')
        && !value.ends_with('-')
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
}

pub(crate) fn handle(value: &str) -> bool {
    value.len() <= 253
        && value.contains('.')
        && value.split('.').all(label)
        && value
            .rsplit('.')
            .next()
            .is_some_and(|last| last.starts_with(|character: char| character.is_ascii_alphabetic()))
}

pub(crate) fn did(value: &str) -> bool {
    let Some((method, identifier)) = value
        .strip_prefix("did:")
        .and_then(|value| value.split_once(':'))
    else {
        return false;
    };
    value.len() <= 2048
        && !method.is_empty()
        && method.bytes().all(|byte| byte.is_ascii_lowercase())
        && !identifier.is_empty()
        && !identifier.ends_with([':', '%'])
        && identifier
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:%-".contains(&byte))
}

pub(crate) fn record_key(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 512
        && !matches!(value, "." | "..")
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b".-_:~".contains(&byte))
}

pub(crate) fn valid_format(format: &str, value: &str) -> bool {
    match format {
        "cid" => cid(value),
        "nsid" => is_nsid(value),
        "did" => did(value),
        "handle" => handle(value),
        "at-identifier" => did(value) || handle(value),
        "record-key" => record_key(value),
        "tid" => {
            value.len() == 13
                && value
                    .bytes()
                    .next()
                    .is_some_and(|byte| b"234567abcdefghij".contains(&byte))
                && value
                    .bytes()
                    .all(|byte| b"234567abcdefghijklmnopqrstuvwxyz".contains(&byte))
        }
        "at-uri" => {
            let Some(rest) = value.strip_prefix("at://") else {
                return false;
            };
            if value.len() > 8192 {
                return false;
            }
            let mut parts = rest.split('/');
            let authority = parts.next().expect("split has first entry");
            (did(authority) || handle(authority))
                && parts.next().is_none_or(is_nsid)
                && parts.next().is_none_or(record_key)
                && parts.next().is_none()
        }
        "datetime" => datetime(value),
        "uri" => {
            static VALIDATOR: std::sync::OnceLock<jsonschema::Validator> =
                std::sync::OnceLock::new();
            value.len() <= 8192
                && VALIDATOR
                    .get_or_init(|| {
                        jsonschema::options()
                            .should_validate_formats(true)
                            .build(&json!({"type": "string", "format": "uri"}))
                            .expect("URI format schema")
                    })
                    .is_valid(&json!(value))
        }
        _ => false,
    }
}

pub(crate) fn datetime(value: &str) -> bool {
    if !value.is_ascii()
        || value.len() < 20
        || value.as_bytes()[10] != b'T'
        || value.ends_with("-00:00")
    {
        return false;
    }
    let bytes = value.as_bytes();
    for (index, byte) in bytes.iter().take(19).enumerate() {
        if !match index {
            4 | 7 => *byte == b'-',
            10 => *byte == b'T',
            13 | 16 => *byte == b':',
            _ => byte.is_ascii_digit(),
        } {
            return false;
        }
    }
    let tail = &value[19..];
    let (fraction, zone) = if let Some(fraction) = tail.strip_prefix('.') {
        let end = fraction
            .find(|character: char| !character.is_ascii_digit())
            .unwrap_or(fraction.len());
        if end == 0 {
            return false;
        }
        (&fraction[..end], &fraction[end..])
    } else {
        ("", tail)
    };
    if zone != "Z"
        && !(zone.len() == 6
            && matches!(zone.as_bytes()[0], b'+' | b'-')
            && zone.as_bytes()[3] == b':'
            && zone[1..3].bytes().all(|byte| byte.is_ascii_digit())
            && zone[4..].bytes().all(|byte| byte.is_ascii_digit()))
    {
        return false;
    }
    let normalized = if fraction.is_empty() {
        value.to_owned()
    } else {
        format!(
            "{}.{}{zone}",
            &value[..19],
            &fraction[..fraction.len().min(9)]
        )
    };
    normalized.parse::<jiff::Timestamp>().is_ok_and(|instant| {
        instant
            >= "0000-01-01T00:00:00Z"
                .parse::<jiff::Timestamp>()
                .expect("minimum Lexicon datetime")
    })
}

// The leaf types this crate owns. The rest of the vocabulary, and the containers, are `datum_core`'s.
macro_rules! values {
    ($($ty:ty),* $(,)?) => {
        $(impl DatumShape for $ty {
            const SHAPE: Shape = Shape::Value;
        })*
    };
}

values!(
    crate::dmeta::Uuid7,
    crate::dmeta::MultibaseKey,
    crate::dmeta::SignatureBytes,
    crate::dmeta::Bytes,
    crate::DatumKey,
    crate::DatumReference,
    crate::drawer::GroupKey,
);

#[cfg(feature = "schemars")]
values!(
    super::Bytes,
    super::Counter,
    super::Text,
    super::Float32,
    super::Float64
);

#[cfg(test)]
mod tests {
    use super::*;
    use indexmap::IndexMap;

    #[test]
    fn containers_require_every_nested_shape() {
        struct Undeclared;
        trait AmbiguousIfShape<A> {
            fn marker() {}
        }
        impl<T: ?Sized> AmbiguousIfShape<()> for T {}
        impl<T: DatumShape + ?Sized> AmbiguousIfShape<u8> for T {}

        let _ = <Undeclared as AmbiguousIfShape<_>>::marker;
        let _ = <Option<Vec<Undeclared>> as AmbiguousIfShape<_>>::marker;
        let _ = <Box<Undeclared> as AmbiguousIfShape<_>>::marker;
        let _ = <std::sync::Arc<Undeclared> as AmbiguousIfShape<_>>::marker;
        let _ = <std::rc::Rc<Undeclared> as AmbiguousIfShape<_>>::marker;
        let _ = <&Undeclared as AmbiguousIfShape<_>>::marker;
        let _ = <[Undeclared] as AmbiguousIfShape<_>>::marker;
        let _ = <[Undeclared; 2] as AmbiguousIfShape<_>>::marker;
        let _ = <std::collections::BTreeMap<String, Undeclared> as AmbiguousIfShape<_>>::marker;
        let _ = <std::collections::HashMap<Undeclared, String> as AmbiguousIfShape<_>>::marker;
        let _ = <IndexMap<String, Undeclared> as AmbiguousIfShape<_>>::marker;
        let _ = <(String, Undeclared) as AmbiguousIfShape<_>>::marker;

        fn declared<T: DatumShape + ?Sized>() {}
        declared::<Option<Vec<Box<str>>>>();
        declared::<std::collections::BTreeMap<String, Vec<crate::dmeta::ActorMetadata>>>();
        declared::<std::collections::HashMap<String, crate::dmeta::DatumMetadata>>();
        declared::<IndexMap<String, (String, bool, crate::dmeta::BranchOrigin)>>();
        declared::<[crate::dmeta::DatumSignature; 2]>();
        declared::<&[crate::dmeta::InheritedWitness]>();
    }

    #[test]
    fn object_schema_and_value_validation_cover_refs_unions_and_constraints() {
        let config_defs = json!({
            "main": {
                "type": "object",
                "required": ["name", "count", "enabled", "mode"],
                "nullable": ["note"],
                "properties": {
                    "name": { "type": "string", "minLength": 1, "maxGraphemes": 4 },
                    "count": { "type": "integer", "minimum": 1, "maximum": 10 },
                    "enabled": { "type": "boolean", "const": true },
                    "note": { "type": "string", "format": "nsid" },
                    "tags": {
                        "type": "array",
                        "minLength": 1,
                        "maxLength": 2,
                        "items": { "type": "ref", "ref": "#tag" }
                    },
                    "mode": {
                        "type": "union",
                        "closed": true,
                        "refs": ["#fast", "org.example.shared"]
                    },
                    "openMode": {
                        "type": "union",
                        "refs": ["#fast"]
                    },
                    "secret": { "type": "bytes", "minLength": 1, "maxLength": 4 },
                    "extra": { "type": "unknown" }
                }
            },
            "tag": {
                "type": "string",
                "enum": ["alpha", "beta"]
            },
            "fast": {
                "type": "object",
                "required": ["threads"],
                "properties": {
                    "threads": { "type": "integer", "minimum": 1 }
                }
            }
        });
        let shared_defs = json!({
            "main": {
                "type": "object",
                "required": ["uri"],
                "properties": {
                    "uri": { "type": "string", "format": "uri" }
                }
            }
        });
        let mut lexicons = BTreeMap::new();
        lexicons.insert(
            "org.example.config",
            config_defs.as_object().expect("config defs"),
        );
        lexicons.insert(
            "org.example.shared",
            shared_defs.as_object().expect("shared defs"),
        );

        validate_object_schema(&lexicons, "org.example.config", "config.schema").unwrap();
        validate_object_schema(&lexicons, "org.example.config#fast", "config.schema").unwrap();
        assert!(
            validate_object_schema(&lexicons, "org.example.config#tag", "config.schema").is_err()
        );
        assert!(validate_object_schema(&lexicons, "#fast", "config.schema").is_err());

        let valid = json!({
            "name": "ok",
            "count": 5,
            "enabled": true,
            "note": null,
            "tags": ["alpha"],
            "mode": { "$type": "org.example.config#fast", "threads": 2 },
            "openMode": { "$type": "org.example.future#mode" },
            "secret": { "$bytes": "AQID" },
            "extra": { "nested": 1 }
        });
        validate_object_value(&lexicons, "org.example.config", &valid, "config.defaults").unwrap();

        for invalid_case in [
            json!({
                "name": "ok",
                "count": 5,
                "enabled": true,
                "mode": { "$type": "org.example.config#fast", "threads": 2 },
                "undeclared": "rejected"
            }),
            json!({
                "name": "too-many-graphemes",
                "count": 5,
                "enabled": true,
                "mode": { "$type": "org.example.config#fast", "threads": 2 }
            }),
            json!({
                "name": "ok",
                "count": 99,
                "enabled": true,
                "mode": { "$type": "org.example.config#fast", "threads": 2 }
            }),
            json!({
                "name": "ok",
                "count": null,
                "enabled": true,
                "mode": { "$type": "org.example.config#fast", "threads": 2 }
            }),
            json!({
                "name": "ok",
                "count": 5,
                "enabled": true,
                "mode": { "$type": "org.example.unknown", "threads": 2 }
            }),
            json!({
                "name": "ok",
                "count": 5,
                "enabled": true,
                "mode": { "$type": "org.example.config#fast", "threads": 2 },
                "float": 1.5
            }),
        ] {
            assert!(
                validate_object_value(
                    &lexicons,
                    "org.example.config",
                    &invalid_case,
                    "config.defaults"
                )
                .is_err(),
                "{invalid_case}"
            );
        }
    }

    #[test]
    fn cyclic_object_references_are_rejected() {
        let defs = json!({
            "main": {
                "type": "object",
                "properties": {
                    "next": { "type": "ref", "ref": "#other" }
                }
            },
            "other": {
                "type": "object",
                "properties": {
                    "back": { "type": "ref", "ref": "#main" }
                }
            }
        });
        let mut lexicons = BTreeMap::new();
        lexicons.insert("org.example.cycle", defs.as_object().expect("defs"));
        let err = validate_object_schema(&lexicons, "org.example.cycle", "config.schema")
            .expect_err("cycle rejected");
        assert!(err.reason.contains("cyclic"), "{err}");
    }
}
