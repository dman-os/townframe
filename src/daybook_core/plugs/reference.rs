use crate::interlude::*;

pub fn select_json_path_values<'a>(
    value: &'a serde_json::Value,
    json_path: &str,
) -> Res<Vec<&'a serde_json::Value>> {
    if json_path == "$" {
        return Ok(vec![value]);
    }

    if json_path.starts_with('/') {
        return Ok(value.pointer(json_path).into_iter().collect());
    }

    if let Some(path_tail) = json_path.strip_prefix("$.") {
        let mut current = value;
        for segment in path_tail.split('.') {
            if segment.is_empty() {
                eyre::bail!("invalid json path '{}'", json_path);
            }
            let Some(next) = current.get(segment) else {
                return Ok(vec![]);
            };
            current = next;
        }
        return Ok(vec![current]);
    }

    eyre::bail!(
        "unsupported json path '{}'; expected JSON pointer '/a/b' or root-dot path '$.a.b'",
        json_path
    )
}

pub fn schema_node_for_json_path<'a>(
    schema_root: &'a serde_json::Value,
    json_path: &str,
) -> Res<Option<&'a serde_json::Value>> {
    let path_segments = parse_json_path_segments(json_path)?;
    let mut current = resolve_schema_node(schema_root, schema_root)?;
    if path_segments.is_empty() {
        return Ok(Some(current));
    }

    for path_segment in path_segments {
        current = match schema_child_for_segment(schema_root, current, &path_segment)? {
            Some(next) => next,
            None => return Ok(None),
        };
    }

    Ok(Some(current))
}

fn parse_json_path_segments(json_path: &str) -> Res<Vec<String>> {
    if json_path == "$" {
        return Ok(Vec::new());
    }
    if let Some(tail) = json_path.strip_prefix("$.") {
        if tail.is_empty() {
            eyre::bail!("invalid json path '{}'", json_path);
        }
        let mut segments = Vec::new();
        for segment in tail.split('.') {
            if segment.is_empty() {
                eyre::bail!("invalid json path '{}'", json_path);
            }
            segments.push(segment.to_string());
        }
        return Ok(segments);
    }
    if let Some(tail) = json_path.strip_prefix('/') {
        if tail.is_empty() {
            return Ok(Vec::new());
        }
        let mut segments = Vec::new();
        for segment in tail.split('/') {
            if segment.is_empty() {
                eyre::bail!("invalid json path '{}'", json_path);
            }
            let decoded = segment.replace("~1", "/").replace("~0", "~");
            segments.push(decoded);
        }
        return Ok(segments);
    }
    eyre::bail!(
        "unsupported json path '{}'; expected JSON pointer '/a/b' or root-dot path '$.a.b'",
        json_path
    )
}

fn schema_child_for_segment<'a>(
    schema_root: &'a serde_json::Value,
    current: &'a serde_json::Value,
    segment: &str,
) -> Res<Option<&'a serde_json::Value>> {
    let node = resolve_schema_node(schema_root, current)?;

    if let Some(child) = schema_child_from_properties(schema_root, node, segment)? {
        return Ok(Some(child));
    }

    for branch_key in ["allOf", "anyOf", "oneOf"] {
        if let Some(branches) = node.get(branch_key).and_then(|value| value.as_array()) {
            for branch in branches {
                if let Some(child) = schema_child_for_segment(schema_root, branch, segment)? {
                    return Ok(Some(child));
                }
            }
        }
    }

    Ok(None)
}

fn schema_child_from_properties<'a>(
    schema_root: &'a serde_json::Value,
    node: &'a serde_json::Value,
    segment: &str,
) -> Res<Option<&'a serde_json::Value>> {
    let Some(properties) = node.get("properties").and_then(|value| value.as_object()) else {
        return Ok(None);
    };
    let Some(child) = properties.get(segment) else {
        return Ok(None);
    };
    Ok(Some(resolve_schema_node(schema_root, child)?))
}

fn resolve_schema_node<'a>(
    schema_root: &'a serde_json::Value,
    node: &'a serde_json::Value,
) -> Res<&'a serde_json::Value> {
    let mut current = node;
    for _ in 0..32 {
        let Some(schema_ref) = current.get("$ref").and_then(|value| value.as_str()) else {
            return Ok(current);
        };
        let Some(pointer) = schema_ref.strip_prefix('#') else {
            eyre::bail!("unsupported external schema ref '{}'", schema_ref);
        };
        let Some(target) = schema_root.pointer(pointer) else {
            eyre::bail!("unresolved schema ref '{}'", schema_ref);
        };
        current = target;
    }
    eyre::bail!("schema ref resolution exceeded recursion depth")
}

pub fn schema_allows_url_reference(schema_node: &serde_json::Value) -> bool {
    schema_supports_string(schema_node) || schema_supports_array_of_strings(schema_node)
}

pub fn schema_allows_array_of_strings(schema_node: &serde_json::Value) -> bool {
    schema_supports_array_of_strings(schema_node)
}

fn schema_supports_string(schema_node: &serde_json::Value) -> bool {
    if schema_has_type(schema_node, "string") {
        return true;
    }

    for branch_key in ["anyOf", "oneOf", "allOf"] {
        if let Some(branches) = schema_node
            .get(branch_key)
            .and_then(|value| value.as_array())
        {
            if branches.iter().any(schema_supports_string) {
                return true;
            }
        }
    }

    false
}

fn schema_supports_array_of_strings(schema_node: &serde_json::Value) -> bool {
    if schema_has_type(schema_node, "array") {
        let Some(items) = schema_node.get("items") else {
            return false;
        };
        if schema_supports_string(items) {
            return true;
        }
    }

    for branch_key in ["anyOf", "oneOf", "allOf"] {
        if let Some(branches) = schema_node
            .get(branch_key)
            .and_then(|value| value.as_array())
        {
            if branches.iter().any(schema_supports_array_of_strings) {
                return true;
            }
        }
    }

    false
}

fn schema_has_type(schema_node: &serde_json::Value, expected_type: &str) -> bool {
    match schema_node.get("type") {
        Some(serde_json::Value::String(type_name)) => type_name == expected_type,
        Some(serde_json::Value::Array(type_names)) => type_names
            .iter()
            .filter_map(|item| item.as_str())
            .any(|type_name| type_name == expected_type),
        _ => false,
    }
}
