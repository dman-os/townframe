use super::CompatibilityOutcome;
use super::Findings;
use super::SchemaBundle;
use super::accessors::Additional;
use super::accessors::additional;
use super::accessors::child;
use super::accessors::resolve;
use super::contradictory_annotation;
use super::scan_unsupported_bundle;
use super::union::branch_tag_resolved;
use super::union::is_open_fallback_tag;
use super::union::union_branches;
use super::union::union_operator;
use super::union::validate_union;
use super::unsupported_keyword;
use serde_json::Value;
use std::collections::BTreeSet;

pub(super) fn compare_native(old: &SchemaBundle, new: &SchemaBundle) -> CompatibilityOutcome {
    let mut findings = Findings::default();
    scan_unsupported_bundle(old, "old", &mut findings);
    scan_unsupported_bundle(new, "new", &mut findings);
    if findings.not_established {
        return findings.outcome();
    }
    NativeWalk {
        old_bundle: old,
        new_bundle: new,
        findings: &mut findings,
        seen: BTreeSet::new(),
    }
    .at(&old.root, &new.root, "/");
    findings.outcome()
}

/// The pairwise walk of an old and a new bundle's native storage, position by position. `seen`
/// holds the `$ref` pairs already walked, so a recursive definition is compared once.
struct NativeWalk<'a> {
    old_bundle: &'a SchemaBundle,
    new_bundle: &'a SchemaBundle,
    findings: &'a mut Findings,
    seen: BTreeSet<(String, String)>,
}

impl NativeWalk<'_> {
    /// Compare the storage one position takes in each bundle, then descend into its children.
    fn at(&mut self, old: &Value, new: &Value, path: &str) {
        let old_ref = old.get("$ref").and_then(Value::as_str).unwrap_or_default();
        let new_ref = new.get("$ref").and_then(Value::as_str).unwrap_or_default();
        if (!old_ref.is_empty() || !new_ref.is_empty())
            && !self.seen.insert((old_ref.to_owned(), new_ref.to_owned()))
        {
            return;
        }
        let Some(old) = resolve(self.old_bundle, old, path, "native", self.findings) else {
            return;
        };
        let Some(new) = resolve(self.new_bundle, new, path, "native", self.findings) else {
            return;
        };
        if is_open_fallback_tag(old) || is_open_fallback_tag(new) {
            return;
        }
        if let Some(keyword) = unsupported_keyword(old).or_else(|| unsupported_keyword(new)) {
            self.findings.not_established(
                path,
                format!("native: unsupported schema keyword or type context `{keyword}`"),
            );
            return;
        }
        if let Some(reason) =
            contradictory_annotation(old).or_else(|| contradictory_annotation(new))
        {
            self.findings
                .not_established(path, format!("native: {reason}"));
            return;
        }
        if !self.unions_agree(old, new, path) {
            return;
        }
        let old_kind = native_kind(old, path, self.findings);
        let new_kind = native_kind(new, path, self.findings);
        if old_kind.is_none()
            && new_kind.is_none()
            && union_branches(old).is_none()
            && union_branches(new).is_none()
        {
            self.findings
                .not_established(path, "native storage kind is not established");
            return;
        }
        if old_kind != new_kind {
            match (old_kind, new_kind) {
                (Some(old_kind), Some(new_kind)) => self.findings.incompatible(
                    path,
                    format!("native storage changed from `{old_kind}` to `{new_kind}`"),
                ),
                _ => self
                    .findings
                    .not_established(path, "native storage declaration is incomplete or unknown"),
            }
            return;
        }
        self.descend(old, new, path);
    }

    /// Whether a union position keeps its carrier: the same operator on both sides, valid
    /// branches, and the same stable member tags. `false` means a finding was recorded and the
    /// position is not compared further.
    fn unions_agree(&mut self, old: &Value, new: &Value, path: &str) -> bool {
        let findings = &mut *self.findings;
        let (old_bundle, new_bundle) = (self.old_bundle, self.new_bundle);
        let old_operator = union_operator(old);
        let new_operator = union_operator(new);
        if (old_operator.is_some() || new_operator.is_some())
            && (old_operator == Some("invalid")
                || new_operator == Some("invalid")
                || old_operator != new_operator)
        {
            findings.not_established(path, "native: union operator or carrier shape changed");
            return false;
        }
        if let Some(old_branches) = union_branches(old)
            && !validate_union(old_bundle, old_branches, path, "native", findings)
        {
            return false;
        }
        if let Some(new_branches) = union_branches(new)
            && !validate_union(new_bundle, new_branches, path, "native", findings)
        {
            return false;
        }
        if let (Some(old_branches), Some(new_branches)) = (union_branches(old), union_branches(new))
        {
            let old_tags: BTreeSet<_> = old_branches
                .iter()
                .filter_map(|branch| {
                    branch_tag_resolved(old_bundle, branch, path, "native", findings)
                })
                .collect();
            let new_tags: BTreeSet<_> = new_branches
                .iter()
                .filter_map(|branch| {
                    branch_tag_resolved(new_bundle, branch, path, "native", findings)
                })
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
                return false;
            }
        }
        true
    }

    /// Walk the children both sides declare: shared properties, items, typed-map values, and
    /// union members paired by tag (the fallback branch with the fallback branch).
    fn descend(&mut self, old: &Value, new: &Value, path: &str) {
        if let (Some(old_properties), Some(new_properties)) = (
            old.get("properties").and_then(Value::as_object),
            new.get("properties").and_then(Value::as_object),
        ) {
            for (name, old_property) in old_properties {
                if let Some(new_property) = new_properties.get(name) {
                    self.at(old_property, new_property, &child(path, name));
                }
            }
        }
        if let (Some(old_items), Some(new_items)) = (old.get("items"), new.get("items")) {
            self.at(old_items, new_items, &child(path, "<items>"));
        }
        match (additional(old), additional(new)) {
            (Additional::Schema(old_schema), Additional::Schema(new_schema)) => self.at(
                old_schema,
                new_schema,
                &child(path, "<additionalProperties>"),
            ),
            (Additional::Schema(_), Additional::Allowed)
            | (Additional::Allowed, Additional::Schema(_))
            | (Additional::Schema(_), Additional::Unknown)
            | (Additional::Unknown, Additional::Schema(_)) => self.findings.not_established(
                child(path, "<additionalProperties>"),
                "typed-map storage cannot be proven across an undeclared or unrestricted map",
            ),
            _ => {}
        }
        if let (Some(old_branches), Some(new_branches)) = (union_branches(old), union_branches(new))
        {
            for old_branch in old_branches {
                let old_tag =
                    branch_tag_resolved(self.old_bundle, old_branch, path, "native", self.findings);
                let new_bundle = self.new_bundle;
                let new_branch = if let Some(tag) = old_tag {
                    new_branches.iter().find(|branch| {
                        branch_tag_resolved(new_bundle, branch, path, "native", self.findings)
                            == Some(tag.clone())
                    })
                } else {
                    new_branches.iter().find(|branch| {
                        branch_tag_resolved(new_bundle, branch, path, "native", self.findings)
                            .is_none()
                    })
                };
                if let Some(new_branch) = new_branch {
                    self.at(old_branch, new_branch, path);
                }
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
