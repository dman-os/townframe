use crate::interlude::*;

pub fn select_json_path_values<'a>(
    value: &'a serde_json::Value,
    json_path: &str,
) -> Res<Vec<&'a serde_json::Value>> {
    if json_path.starts_with('/') {
        return select_json_pointer_values(value, json_path);
    }

    use jsonpath_rust::JsonPath;
    let results = value
        .query(json_path)
        .map_err(|err| eyre::eyre!("jsonpath error: {err}"))?;
    Ok(results)
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum JsonPathSegment {
    Key(String),
    Wildcard,
}

fn parse_json_path_segments(json_path: &str) -> Res<Vec<JsonPathSegment>> {
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
            if segment == "*" {
                segments.push(JsonPathSegment::Wildcard);
                continue;
            }
            if let Some(prefix) = segment.strip_suffix("[*]") {
                if prefix.is_empty() {
                    eyre::bail!("invalid json path '{}'", json_path);
                }
                segments.push(JsonPathSegment::Key(prefix.to_string()));
                segments.push(JsonPathSegment::Wildcard);
                continue;
            }
            segments.push(JsonPathSegment::Key(segment.to_string()));
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
            segments.push(JsonPathSegment::Key(decoded));
        }
        return Ok(segments);
    }
    eyre::bail!(
        "unsupported json path '{}'; expected JSON pointer '/a/b' or root-dot path '$.a.b'",
        json_path
    )
}

fn select_json_pointer_values<'a>(
    value: &'a serde_json::Value,
    json_path: &str,
) -> Res<Vec<&'a serde_json::Value>> {
    let segments = parse_json_path_segments(json_path)?;
    let mut current = vec![value];
    for segment in segments {
        let mut next = Vec::new();
        for node in current {
            match (&segment, node) {
                (JsonPathSegment::Key(key), serde_json::Value::Object(map)) => {
                    if let Some(child) = map.get(key) {
                        next.push(child);
                    }
                }
                (JsonPathSegment::Key(key), serde_json::Value::Array(items)) => {
                    let index = key
                        .parse::<usize>()
                        .map_err(|_| eyre::eyre!("invalid array index in json pointer: {key}"))?;
                    if let Some(child) = items.get(index) {
                        next.push(child);
                    }
                }
                _ => {}
            }
        }
        current = next;
        if current.is_empty() {
            break;
        }
    }
    Ok(current)
}

fn schema_child_for_segment<'a>(
    schema_root: &'a serde_json::Value,
    current: &'a serde_json::Value,
    segment: &JsonPathSegment,
) -> Res<Option<&'a serde_json::Value>> {
    let node = resolve_schema_node(schema_root, current)?;

    match segment {
        JsonPathSegment::Key(segment) => {
            if let Some(child) = schema_child_from_properties(schema_root, node, segment)? {
                return Ok(Some(child));
            }
        }
        JsonPathSegment::Wildcard => {
            if let Some(child) = schema_child_from_array_items(schema_root, node)? {
                return Ok(Some(child));
            }
        }
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

fn schema_child_from_array_items<'a>(
    schema_root: &'a serde_json::Value,
    node: &'a serde_json::Value,
) -> Res<Option<&'a serde_json::Value>> {
    let Some(items) = node.get("items") else {
        return Ok(None);
    };
    Ok(Some(resolve_schema_node(schema_root, items)?))
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

pub fn schema_allows_string(schema_node: &serde_json::Value) -> bool {
    schema_supports_string(schema_node)
}

pub fn schema_allows_array_of_strings(schema_node: &serde_json::Value) -> bool {
    schema_supports_array_of_strings(schema_node)
}

pub fn schema_allows_reference_object(schema_node: &serde_json::Value) -> bool {
    if schema_has_type(schema_node, "object") {
        let Some(properties) = schema_node
            .get("properties")
            .and_then(|value| value.as_object())
        else {
            return false;
        };
        let Some(ref_schema) = properties.get("ref") else {
            return false;
        };
        let Some(heads_schema) = properties.get("heads") else {
            return false;
        };
        return schema_supports_string(ref_schema)
            && schema_supports_array_of_strings(heads_schema);
    }

    for branch_key in ["anyOf", "oneOf", "allOf"] {
        if let Some(branches) = schema_node
            .get(branch_key)
            .and_then(|value| value.as_array())
        {
            if branches.iter().any(schema_allows_reference_object) {
                return true;
            }
        }
    }

    false
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_node_for_json_path_supports_wildcard_array_items() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": {
                "srcRefs": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "ref": { "type": "string" },
                            "heads": {
                                "type": "array",
                                "items": { "type": "string" }
                            }
                        }
                    }
                }
            }
        });

        let item_schema = schema_node_for_json_path(&schema, "$.srcRefs[*]")
            .unwrap()
            .expect("wildcard path should resolve");
        assert!(schema_allows_reference_object(item_schema));

        let ref_schema = schema_node_for_json_path(&schema, "$.srcRefs[*].ref")
            .unwrap()
            .expect("nested wildcard path should resolve");
        assert!(schema_allows_string(ref_schema));
    }
}
