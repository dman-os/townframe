use super::Findings;
use super::SchemaBundle;
use super::accepts;
use super::accessors::child;
use super::accessors::resolve;
use super::unsupported_keyword;
use serde_json::Value;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

pub(super) fn compare_unions(
    reader_bundle: &SchemaBundle,
    writer_bundle: &SchemaBundle,
    reader: Option<&Vec<Value>>,
    writer: Option<&Vec<Value>>,
    path: &str,
    direction: &str,
    findings: &mut Findings,
) {
    let Some(writer) = writer else {
        findings.not_established(
            path,
            format!("{direction}: union was replaced by a non-union"),
        );
        return;
    };
    let Some(reader) = reader else {
        findings.incompatible(
            path,
            format!("{direction}: union member cannot be read by a non-union"),
        );
        return;
    };
    if !validate_union(reader_bundle, reader, path, direction, findings)
        || !validate_union(writer_bundle, writer, path, direction, findings)
    {
        return;
    }

    let reader_fallback = reader.iter().find(|branch| {
        branch_tag_resolved(reader_bundle, branch, path, direction, findings).is_none()
    });
    let mut reader_tagged = BTreeMap::new();
    for branch in reader {
        if let Some(tag) = branch_tag_resolved(reader_bundle, branch, path, direction, findings)
            && reader_tagged.insert(tag.clone(), branch).is_some()
        {
            findings.not_established(
                path,
                format!("{direction}: union contains duplicate member tag `{tag}`"),
            );
        }
    }

    for (index, writer_branch) in writer.iter().enumerate() {
        let branch_path = format!("{path}<union:{index}>");
        if let Some(tag) = branch_tag_resolved(
            writer_bundle,
            writer_branch,
            &branch_path,
            direction,
            findings,
        ) {
            if let Some(reader_branch) = reader_tagged.get(&tag) {
                compare_union_branch_pair(
                    reader_bundle,
                    writer_bundle,
                    reader_branch,
                    writer_branch,
                    &branch_path,
                    direction,
                    findings,
                );
            } else if let Some(fallback) = reader_fallback {
                compare_union_branch_pair(
                    reader_bundle,
                    writer_bundle,
                    fallback,
                    writer_branch,
                    &branch_path,
                    direction,
                    findings,
                );
            } else {
                findings.incompatible(
                    branch_path,
                    format!("{direction}: union member `{tag}` is not known and union is closed"),
                );
            }
        } else if let Some(fallback) = reader_fallback {
            compare_union_branch_pair(
                reader_bundle,
                writer_bundle,
                fallback,
                writer_branch,
                &branch_path,
                direction,
                findings,
            );
        } else {
            findings.not_established(
                branch_path,
                format!("{direction}: untagged union member cannot be matched conservatively"),
            );
        }
    }
}

pub(super) fn validate_union(
    bundle: &SchemaBundle,
    branches: &[Value],
    path: &str,
    direction: &str,
    findings: &mut Findings,
) -> bool {
    if branches.is_empty() {
        findings.not_established(path, format!("{direction}: empty union is unsupported"));
        return false;
    }
    let mut fallback_count = 0;
    let mut tags = BTreeSet::new();
    let mut valid = true;
    for (index, branch) in branches.iter().enumerate() {
        let branch_path = format!("{path}<union:{index}>");
        let Some(branch) = resolve(bundle, branch, &branch_path, direction, findings) else {
            valid = false;
            continue;
        };
        if unsupported_keyword(branch).is_some() {
            findings.not_established(
                &branch_path,
                format!("{direction}: union branch uses an unsupported keyword"),
            );
            valid = false;
        }
        let Some(tag_schema) = branch_discriminant(branch) else {
            findings.not_established(
                &branch_path,
                format!("{direction}: union branch has no `$type` discriminant"),
            );
            valid = false;
            continue;
        };
        let tag = if tag_schema.get("const").and_then(Value::as_str).is_some() {
            branch_tag(branch)
        } else if is_open_fallback_tag(tag_schema) {
            fallback_count += 1;
            None
        } else {
            findings.not_established(
                &branch_path,
                format!("{direction}: union branch has neither a stable tag nor a valid fallback"),
            );
            valid = false;
            None
        };
        if let Some(tag) = tag
            && !tags.insert(tag.clone())
        {
            findings.not_established(
                &branch_path,
                format!("{direction}: union contains duplicate member tag `{tag}`"),
            );
            valid = false;
        }
    }
    if fallback_count > 1 {
        findings.not_established(
            path,
            format!("{direction}: union contains more than one fallback branch"),
        );
        valid = false;
    }
    valid
}

/// The `$type` schema of a union branch.
///
/// A published union is internally tagged, so the discriminant is a property of the branch rather
/// than a field of a carrier. An adjacent member is the same shape: it is a declared wrapper type
/// whose payload sits under `value`, and `value` is an ordinary property of that wrapper.
fn branch_discriminant(schema: &Value) -> Option<&Value> {
    schema
        .get("properties")
        .and_then(Value::as_object)
        .and_then(|properties| properties.get("$type"))
}

pub(super) fn branch_tag_resolved(
    bundle: &SchemaBundle,
    branch: &Value,
    path: &str,
    direction: &str,
    findings: &mut Findings,
) -> Option<String> {
    resolve(bundle, branch, path, direction, findings).and_then(branch_tag)
}

fn compare_union_branch_pair(
    reader_bundle: &SchemaBundle,
    writer_bundle: &SchemaBundle,
    reader: &Value,
    writer: &Value,
    path: &str,
    direction: &str,
    findings: &mut Findings,
) {
    let Some(reader) = resolve(reader_bundle, reader, path, direction, findings) else {
        return;
    };
    let Some(writer) = resolve(writer_bundle, writer, path, direction, findings) else {
        return;
    };
    let Some(reader_tag) = branch_discriminant(reader) else {
        findings.not_established(
            path,
            format!("{direction}: reader union branch has no `$type` discriminant"),
        );
        return;
    };
    let Some(writer_tag) = branch_discriminant(writer) else {
        findings.not_established(
            path,
            format!("{direction}: writer union branch has no `$type` discriminant"),
        );
        return;
    };

    // The discriminant is compared on its own. The payload is everything else the branch declares,
    // and it is compared property by property: feeding the branch back through `accepts` would
    // re-enter the union and demand an openness the carrier never stated.
    if compare_open_union_refinement(reader_tag, writer_tag, path, direction, findings).is_none() {
        accepts(
            reader_bundle,
            writer_bundle,
            reader_tag,
            writer_tag,
            &child(path, "$type"),
            direction,
            findings,
        );
    }
    let reader_properties = reader.get("properties").and_then(Value::as_object);
    let writer_properties = writer.get("properties").and_then(Value::as_object);
    if let Some(writer_properties) = writer_properties {
        for (name, writer_property) in writer_properties {
            if name == "$type" {
                continue;
            }
            let Some(reader_property) =
                reader_properties.and_then(|properties| properties.get(name))
            else {
                findings.incompatible(
                    child(path, name),
                    format!("{direction}: union member gained a property the reader does not have"),
                );
                continue;
            };
            accepts(
                reader_bundle,
                writer_bundle,
                reader_property,
                writer_property,
                &child(path, name),
                direction,
                findings,
            );
        }
    }
}

fn excluded_values(schema: &Value) -> Option<&Vec<Value>> {
    schema
        .get("not")
        .and_then(|not| not.get("enum"))
        .and_then(Value::as_array)
}

pub(super) fn compare_open_union_refinement(
    reader: &Value,
    writer: &Value,
    path: &str,
    direction: &str,
    findings: &mut Findings,
) -> Option<bool> {
    let reader_excluded = excluded_values(reader);
    let writer_excluded = excluded_values(writer);
    match (
        reader_excluded,
        writer_excluded,
        reader.get("const"),
        writer.get("const"),
    ) {
        (Some(reader_excluded), Some(writer_excluded), _, _) => {
            let reader_subset = reader_excluded
                .iter()
                .all(|value| writer_excluded.contains(value));
            let writer_subset = writer_excluded
                .iter()
                .all(|value| reader_excluded.contains(value));
            if !(reader_subset || writer_subset) {
                findings.not_established(
                    path,
                    format!("{direction}: open-union fallback exclusion changed incompatibly"),
                );
            }
            Some(true)
        }
        (Some(reader_excluded), None, _, Some(writer_const)) => {
            if reader_excluded.contains(writer_const) {
                findings.incompatible(
                    path,
                    format!("{direction}: union tag is excluded by the reader fallback"),
                );
            }
            Some(true)
        }
        (None, Some(writer_excluded), Some(reader_const), _) => {
            if writer_excluded.contains(reader_const) {
                findings.incompatible(
                    path,
                    format!("{direction}: reader union tag is excluded by the writer fallback"),
                );
            }
            Some(true)
        }
        _ => None,
    }
}

pub(super) fn is_open_fallback_tag(schema: &Value) -> bool {
    let Some(object) = schema.as_object() else {
        return false;
    };
    matches!(object.get("type"), Some(Value::String(kind)) if kind == "string")
        && matches!(
            object.get("not"),
            Some(Value::Object(not))
                if not.len() == 1 && not.get("enum").is_some_and(Value::is_array)
        )
        && object.keys().all(|key| key == "type" || key == "not")
}

pub(super) fn union_operator(schema: &Value) -> Option<&str> {
    match (schema.get("oneOf"), schema.get("anyOf")) {
        (Some(_), Some(_)) => Some("invalid"),
        (Some(_), None) => Some("oneOf"),
        (None, Some(_)) => Some("anyOf"),
        (None, None) => None,
    }
}

pub(super) fn union_branches(schema: &Value) -> Option<&Vec<Value>> {
    schema
        .get("oneOf")
        .or_else(|| schema.get("anyOf"))
        .and_then(Value::as_array)
}

fn branch_tag(schema: &Value) -> Option<String> {
    schema
        .get("properties")
        .and_then(Value::as_object)
        .and_then(|properties| properties.get("$type"))
        .and_then(|tag| tag.get("const"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| {
            schema
                .get("const")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
}
