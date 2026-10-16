use super::{
    Additional, CompatibilityOutcome, Findings, SchemaBundle, additional, branch_tag_resolved,
    child, contradictory_annotation, is_open_fallback_tag, resolve, scan_unsupported_bundle,
    union_branches, union_operator, unsupported_keyword, validate_union,
};
use serde_json::Value;
use std::collections::BTreeSet;

pub(super) fn compare_native(old: &SchemaBundle, new: &SchemaBundle) -> CompatibilityOutcome {
    let mut findings = Findings::default();
    scan_unsupported_bundle(old, "old", &mut findings);
    scan_unsupported_bundle(new, "new", &mut findings);
    if findings.not_established {
        return findings.outcome();
    }
    native_at(
        old,
        new,
        &old.root,
        &new.root,
        "/",
        &mut findings,
        &mut BTreeSet::new(),
    );
    findings.outcome()
}

fn native_at(
    old_bundle: &SchemaBundle,
    new_bundle: &SchemaBundle,
    old: &Value,
    new: &Value,
    path: &str,
    findings: &mut Findings,
    seen: &mut BTreeSet<(String, String)>,
) {
    let old_ref = old.get("$ref").and_then(Value::as_str).unwrap_or_default();
    let new_ref = new.get("$ref").and_then(Value::as_str).unwrap_or_default();
    if (!old_ref.is_empty() || !new_ref.is_empty())
        && !seen.insert((old_ref.to_owned(), new_ref.to_owned()))
    {
        return;
    }
    let Some(old) = resolve(old_bundle, old, path, "native", findings) else {
        return;
    };
    let Some(new) = resolve(new_bundle, new, path, "native", findings) else {
        return;
    };
    if is_open_fallback_tag(old) || is_open_fallback_tag(new) {
        return;
    }
    if let Some(keyword) = unsupported_keyword(old).or_else(|| unsupported_keyword(new)) {
        findings.not_established(
            path,
            format!("native: unsupported schema keyword or type context `{keyword}`"),
        );
        return;
    }
    if let Some(reason) = contradictory_annotation(old).or_else(|| contradictory_annotation(new)) {
        findings.not_established(path, format!("native: {reason}"));
        return;
    }
    let old_operator = union_operator(old);
    let new_operator = union_operator(new);
    if (old_operator.is_some() || new_operator.is_some())
        && (old_operator == Some("invalid")
            || new_operator == Some("invalid")
            || old_operator != new_operator)
    {
        findings.not_established(path, "native: union operator or carrier shape changed");
        return;
    }
    if let Some(old_branches) = union_branches(old)
        && !validate_union(old_bundle, old_branches, path, "native", findings)
    {
        return;
    }
    if let Some(new_branches) = union_branches(new)
        && !validate_union(new_bundle, new_branches, path, "native", findings)
    {
        return;
    }
    if let (Some(old_branches), Some(new_branches)) = (union_branches(old), union_branches(new)) {
        let old_tags: BTreeSet<_> = old_branches
            .iter()
            .filter_map(|branch| branch_tag_resolved(old_bundle, branch, path, "native", findings))
            .collect();
        let new_tags: BTreeSet<_> = new_branches
            .iter()
            .filter_map(|branch| branch_tag_resolved(new_bundle, branch, path, "native", findings))
            .collect();
        if old_tags != new_tags {
            let has_fallback = old_branches.iter().any(|branch| {
                branch_tag_resolved(old_bundle, branch, path, "native", findings).is_none()
            }) && new_branches.iter().any(|branch| {
                branch_tag_resolved(new_bundle, branch, path, "native", findings).is_none()
            });
            if has_fallback && old_tags.is_subset(&new_tags) {
                findings.not_established(
                    path,
                    "native: open union member addition cannot be proven in storage",
                );
            } else {
                findings.incompatible(path, "native: stable union member tags changed");
            }
            return;
        }
    }
    let old_kind = native_kind(old, path, findings);
    let new_kind = native_kind(new, path, findings);
    if old_kind.is_none()
        && new_kind.is_none()
        && union_branches(old).is_none()
        && union_branches(new).is_none()
    {
        findings.not_established(path, "native storage kind is not established");
        return;
    }
    if old_kind != new_kind {
        match (old_kind, new_kind) {
            (Some(old_kind), Some(new_kind)) => findings.incompatible(
                path,
                format!("native storage changed from `{old_kind}` to `{new_kind}`"),
            ),
            _ => findings
                .not_established(path, "native storage declaration is incomplete or unknown"),
        }
        return;
    }

    if let (Some(old_properties), Some(new_properties)) = (
        old.get("properties").and_then(Value::as_object),
        new.get("properties").and_then(Value::as_object),
    ) {
        for (name, old_property) in old_properties {
            if let Some(new_property) = new_properties.get(name) {
                native_at(
                    old_bundle,
                    new_bundle,
                    old_property,
                    new_property,
                    &child(path, name),
                    findings,
                    seen,
                );
            }
        }
    }
    if let (Some(old_items), Some(new_items)) = (old.get("items"), new.get("items")) {
        native_at(
            old_bundle,
            new_bundle,
            old_items,
            new_items,
            &child(path, "<items>"),
            findings,
            seen,
        );
    }
    match (additional(old), additional(new)) {
        (Additional::Schema(old_schema), Additional::Schema(new_schema)) => native_at(
            old_bundle,
            new_bundle,
            old_schema,
            new_schema,
            &child(path, "<additionalProperties>"),
            findings,
            seen,
        ),
        (Additional::Schema(_), Additional::Allowed)
        | (Additional::Allowed, Additional::Schema(_))
        | (Additional::Schema(_), Additional::Unknown)
        | (Additional::Unknown, Additional::Schema(_)) => findings.not_established(
            child(path, "<additionalProperties>"),
            "typed-map storage cannot be proven across an undeclared or unrestricted map",
        ),
        _ => {}
    }
    if let (Some(old_branches), Some(new_branches)) = (union_branches(old), union_branches(new)) {
        for old_branch in old_branches {
            let old_tag = branch_tag_resolved(old_bundle, old_branch, path, "native", findings);
            if let Some(tag) = old_tag {
                if let Some(new_branch) = new_branches.iter().find(|branch| {
                    branch_tag_resolved(new_bundle, branch, path, "native", findings)
                        == Some(tag.clone())
                }) {
                    native_at(
                        old_bundle, new_bundle, old_branch, new_branch, path, findings, seen,
                    );
                }
            } else if let Some(new_branch) = new_branches.iter().find(|branch| {
                branch_tag_resolved(new_bundle, branch, path, "native", findings).is_none()
            }) {
                native_at(
                    old_bundle, new_bundle, old_branch, new_branch, path, findings, seen,
                );
            }
        }
    }
}

fn native_kind(schema: &Value, path: &str, findings: &mut Findings) -> Option<String> {
    if let Some(marker) = schema.get("x-automerge") {
        let Some(marker) = marker.as_str() else {
            findings.not_established(path, "x-automerge is not a string declaration");
            return None;
        };
        if !matches!(
            marker,
            "bytes" | "timestamp" | "counter" | "float" | "text" | "number" | "list"
        ) {
            findings.not_established(path, format!("unknown x-automerge storage `{marker}`"));
        }
        return Some(format!("x-automerge:{marker}"));
    }
    if schema
        .get("contentEncoding")
        .and_then(Value::as_str)
        .is_some_and(|value| value.eq_ignore_ascii_case("base64"))
    {
        return Some("bytes".to_owned());
    }
    for key in ["unit", "x-unit", "x-automerge-unit"] {
        if schema.get(key).is_some_and(|unit| !unit.is_string()) {
            findings.not_established(path, format!("native unit `{key}` is not a string"));
            return None;
        }
    }
    if let Some(unit) = ["unit", "x-unit", "x-automerge-unit"]
        .into_iter()
        .find_map(|key| schema.get(key).and_then(Value::as_str))
    {
        if schema.get("format").and_then(Value::as_str) == Some("date-time") {
            return Some(format!("timestamp:{unit}"));
        }
        return Some(format!("unit:{unit}"));
    }
    if schema.get("format").and_then(Value::as_str) == Some("date-time") {
        return Some("timestamp".to_owned());
    }
    if let Some(constant) = schema.get("const") {
        let kind = match constant {
            Value::Null => "null",
            Value::Bool(_) => "boolean",
            Value::Number(_) => "number",
            Value::String(_) => "string",
            Value::Array(_) => "array",
            Value::Object(_) => "object",
        };
        return Some(format!("const:{kind}"));
    }
    let (kind, nullable) = match schema.get("type") {
        Some(Value::String(kind)) => (kind.as_str(), false),
        Some(Value::Array(kinds)) => {
            let non_null: Vec<_> = kinds
                .iter()
                .filter_map(Value::as_str)
                .filter(|kind| *kind != "null")
                .collect();
            if non_null.len() != 1 {
                return None;
            }
            (non_null[0], kinds.len() != non_null.len())
        }
        _ => return None,
    };
    let prefix = if nullable { "nullable:" } else { "" };
    if matches!(kind, "integer" | "number")
        && let Some(format) = schema.get("format").and_then(Value::as_str)
    {
        return Some(format!("{prefix}{kind}:{format}"));
    }
    Some(format!("{prefix}{kind}"))
}
