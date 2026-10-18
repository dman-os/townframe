//! Bidirectional evolution of a published Lexicon.
//!
//! Same-tag evolution has to work in both directions: an older reader must accept a value the newer
//! declaration produces, and the newer reader must accept a value the older one produced. Lexicon
//! objects are open, so an optional property may appear or disappear. A required property, a type, a
//! nullable name, or a closed union's member set may not: each of those rejects a value one of the
//! two readers already accepts. An open union may gain or lose a known member, because its fallback
//! already accepts the member the other side has not named. Anything this walk cannot prove is
//! `not established`, never silently compatible.

use super::{Compatibility, CompatibilityOutcome, Diagnostic};
use serde_json::{Map, Value};
use std::collections::{BTreeSet, HashSet};

pub(super) fn evolve_projected(old: &Value, new: &Value) -> CompatibilityOutcome {
    let (old_position, old_defs) = unwrap_projected(old);
    let (new_position, new_defs) = unwrap_projected(new);
    evolve(old_position, new_position, old_defs, new_defs, "/")
}

pub(super) fn evolve_files(old: &Value, new: &Value) -> CompatibilityOutcome {
    let Some(old_file) = old.as_object() else {
        return not_established("/", "old Lexicon file is not an object");
    };
    let Some(new_file) = new.as_object() else {
        return not_established("/", "new Lexicon file is not an object");
    };
    if old_file.get("lexicon") != Some(&Value::from(1))
        || new_file.get("lexicon") != Some(&Value::from(1))
    {
        return not_established("/", "Lexicon evolution rules apply to lexicon version 1");
    }
    let (Some(old_id), Some(new_id)) = (
        old_file.get("id").and_then(Value::as_str),
        new_file.get("id").and_then(Value::as_str),
    ) else {
        return not_established("/", "a Lexicon file states its id");
    };
    if old_id != new_id {
        return incompatible("/", "lexicon id changed");
    }
    let (Some(old_defs), Some(new_defs)) = (
        old_file.get("defs").and_then(Value::as_object),
        new_file.get("defs").and_then(Value::as_object),
    ) else {
        return not_established("/", "a Lexicon file states its defs");
    };
    let (Some(old_main), Some(new_main)) = (old_defs.get("main"), new_defs.get("main")) else {
        return not_established("/", "a Lexicon file has no main");
    };
    evolve(old_main, new_main, old_defs, new_defs, "/")
}

fn unwrap_projected(value: &Value) -> (&Value, &Map<String, Value>) {
    let Some(object) = value.as_object() else {
        return (value, empty_defs());
    };
    match (
        object.get("position"),
        object.get("$defs").and_then(Value::as_object),
    ) {
        (Some(position), Some(defs)) if object.len() == 2 => (position, defs),
        _ => (value, empty_defs()),
    }
}

fn empty_defs() -> &'static Map<String, Value> {
    static EMPTY: std::sync::OnceLock<Map<String, Value>> = std::sync::OnceLock::new();
    EMPTY.get_or_init(Map::new)
}

struct Walk<'a> {
    old_defs: &'a Map<String, Value>,
    new_defs: &'a Map<String, Value>,
    seen: HashSet<(String, String)>,
    findings: Findings,
}

fn evolve(
    old: &Value,
    new: &Value,
    old_defs: &Map<String, Value>,
    new_defs: &Map<String, Value>,
    path: &str,
) -> CompatibilityOutcome {
    let mut walk = Walk {
        old_defs,
        new_defs,
        seen: HashSet::new(),
        findings: Findings::default(),
    };
    walk.position(old, new, path);
    walk.findings.outcome()
}

impl Walk<'_> {
    fn position(&mut self, old: &Value, new: &Value, path: &str) {
        let (Some(old_object), Some(new_object)) = (old.as_object(), new.as_object()) else {
            self.findings
                .not_established(path, "a Lexicon position is an object");
            return;
        };
        let (Some(old_type), Some(new_type)) = (
            old_object.get("type").and_then(Value::as_str),
            new_object.get("type").and_then(Value::as_str),
        ) else {
            self.findings
                .not_established(path, "a Lexicon position states its type");
            return;
        };
        if old_type != new_type {
            self.findings.incompatible(
                path,
                format!("type changed from `{old_type}` to `{new_type}`"),
            );
            return;
        }
        if let Some(key) =
            unknown_field(old_type, old_object).or_else(|| unknown_field(new_type, new_object))
        {
            self.findings.not_established(
                path,
                format!("`{key}` is not a field this evolution walk knows"),
            );
            return;
        }
        match old_type {
            "object" => self.object(old_object, new_object, path),
            "array" => self.array(old_object, new_object, path),
            "union" => self.union(old_object, new_object, path),
            "ref" => self.reference(old_object, new_object, path),
            "string" => self.constrained(
                old_object,
                new_object,
                path,
                &[
                    "format",
                    "enum",
                    "const",
                    "minLength",
                    "maxLength",
                    "minGraphemes",
                    "maxGraphemes",
                ],
            ),
            "integer" => self.constrained(
                old_object,
                new_object,
                path,
                &["minimum", "maximum", "enum", "const"],
            ),
            "boolean" => self.constrained(old_object, new_object, path, &["const"]),
            "bytes" => self.constrained(old_object, new_object, path, &["minLength", "maxLength"]),
            "unknown" | "cid-link" | "token" => {}
            "blob" => self.constrained(old_object, new_object, path, &["accept", "maxSize"]),
            other => self
                .findings
                .not_established(path, format!("`{other}` has no evolution rule")),
        }
    }

    fn object(&mut self, old: &Map<String, Value>, new: &Map<String, Value>, path: &str) {
        let old_required = names(old.get("required"));
        let new_required = names(new.get("required"));
        if old_required != new_required {
            self.findings
                .incompatible(path, "required properties changed");
        }
        let old_nullable = names(old.get("nullable"));
        let new_nullable = names(new.get("nullable"));
        if old_nullable != new_nullable {
            self.findings
                .incompatible(path, "nullable properties changed");
        }
        let old_properties = old.get("properties").and_then(Value::as_object);
        let new_properties = new.get("properties").and_then(Value::as_object);
        let (Some(old_properties), Some(new_properties)) = (old_properties, new_properties) else {
            self.findings
                .not_established(path, "a Lexicon object states its properties");
            return;
        };
        let old_names: BTreeSet<&str> = old_properties.keys().map(String::as_str).collect();
        let new_names: BTreeSet<&str> = new_properties.keys().map(String::as_str).collect();
        for name in old_names.intersection(&new_names) {
            self.position(
                &old_properties[*name],
                &new_properties[*name],
                &child(path, name),
            );
        }
        for name in old_names.symmetric_difference(&new_names) {
            let required = old_required.contains(*name) || new_required.contains(*name);
            if required {
                self.findings.incompatible(
                    child(path, name),
                    "a required property was added or removed",
                );
            }
        }
    }

    fn array(&mut self, old: &Map<String, Value>, new: &Map<String, Value>, path: &str) {
        self.same(old, new, path, &["minLength", "maxLength"]);
        match (old.get("items"), new.get("items")) {
            (Some(old_items), Some(new_items)) => {
                self.position(old_items, new_items, &format!("{path}[]"))
            }
            _ => self
                .findings
                .not_established(path, "a Lexicon array states its items"),
        }
    }

    fn union(&mut self, old: &Map<String, Value>, new: &Map<String, Value>, path: &str) {
        let old_closed = old.get("closed") == Some(&Value::Bool(true));
        let new_closed = new.get("closed") == Some(&Value::Bool(true));
        let old_refs = names(old.get("refs"));
        let new_refs = names(new.get("refs"));
        if (old_closed || new_closed) && (old_closed != new_closed || old_refs != new_refs) {
            self.findings
                .incompatible(path, "a closed union's members or closedness changed");
        }
    }

    fn reference(&mut self, old: &Map<String, Value>, new: &Map<String, Value>, path: &str) {
        let (Some(old_ref), Some(new_ref)) = (
            old.get("ref").and_then(Value::as_str),
            new.get("ref").and_then(Value::as_str),
        ) else {
            self.findings
                .not_established(path, "a Lexicon ref states its target");
            return;
        };
        if old_ref != new_ref {
            self.findings.incompatible(
                path,
                format!("ref target changed from `{old_ref}` to `{new_ref}`"),
            );
            return;
        }
        let Some(name) = old_ref.strip_prefix('#') else {
            return;
        };
        if !self.seen.insert((old_ref.to_owned(), new_ref.to_owned())) {
            return;
        }
        match (self.old_defs.get(name), self.new_defs.get(name)) {
            (Some(old_def), Some(new_def)) => {
                self.position(old_def, new_def, &format!("{path}/{name}"))
            }
            (None, None) => {}
            _ => self
                .findings
                .not_established(path, format!("`{old_ref}` is not carried by both Lexicons")),
        }
    }

    fn constrained(
        &mut self,
        old: &Map<String, Value>,
        new: &Map<String, Value>,
        path: &str,
        keys: &[&str],
    ) {
        if keys.contains(&"enum") && old.contains_key("enum") != new.contains_key("enum") {
            self.findings
                .incompatible(path, "a closed enum became open, or the reverse");
        }
        self.same(old, new, path, keys);
    }

    fn same(
        &mut self,
        old: &Map<String, Value>,
        new: &Map<String, Value>,
        path: &str,
        keys: &[&str],
    ) {
        for key in keys {
            if old.get(*key) != new.get(*key) {
                self.findings.incompatible(path, format!("`{key}` changed"));
            }
        }
    }
}

fn unknown_field<'a>(kind: &'a str, object: &'a Map<String, Value>) -> Option<&'a str> {
    let allowed: &[&str] = match kind {
        "object" => &["type", "description", "properties", "required", "nullable"],
        "array" => &["type", "description", "items", "minLength", "maxLength"],
        "string" => &[
            "type",
            "description",
            "format",
            "minLength",
            "maxLength",
            "minGraphemes",
            "maxGraphemes",
            "knownValues",
            "enum",
            "default",
            "const",
        ],
        "integer" => &[
            "type",
            "description",
            "minimum",
            "maximum",
            "enum",
            "default",
            "const",
        ],
        "boolean" => &["type", "description", "default", "const"],
        "bytes" => &["type", "description", "minLength", "maxLength"],
        "blob" => &["type", "description", "accept", "maxSize"],
        "ref" => &["type", "description", "ref"],
        "union" => &["type", "description", "refs", "closed"],
        "cid-link" | "unknown" | "token" => &["type", "description"],
        _ => return Some(kind),
    };
    object
        .keys()
        .find(|key| !allowed.contains(&key.as_str()))
        .map(String::as_str)
}

fn names(value: Option<&Value>) -> BTreeSet<&str> {
    value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect()
}

fn child(path: &str, name: &str) -> String {
    let escaped = name.replace('~', "~0").replace('/', "~1");
    if path == "/" {
        format!("/{escaped}")
    } else {
        format!("{path}/{escaped}")
    }
}

#[derive(Default)]
struct Findings {
    diagnostics: Vec<Diagnostic>,
    incompatible: bool,
    not_established: bool,
}

impl Findings {
    fn incompatible(&mut self, path: impl Into<String>, reason: impl Into<String>) {
        self.incompatible = true;
        self.diagnostics.push(Diagnostic {
            path: path.into(),
            reason: reason.into(),
        });
    }

    fn not_established(&mut self, path: impl Into<String>, reason: impl Into<String>) {
        self.not_established = true;
        self.diagnostics.push(Diagnostic {
            path: path.into(),
            reason: reason.into(),
        });
    }

    fn outcome(self) -> CompatibilityOutcome {
        let result = if self.incompatible {
            Compatibility::Incompatible
        } else if self.not_established {
            Compatibility::NotEstablished
        } else {
            Compatibility::Compatible
        };
        CompatibilityOutcome {
            result,
            diagnostics: self.diagnostics,
        }
    }
}

fn not_established(path: &str, reason: &str) -> CompatibilityOutcome {
    CompatibilityOutcome {
        result: Compatibility::NotEstablished,
        diagnostics: vec![Diagnostic {
            path: path.to_owned(),
            reason: reason.to_owned(),
        }],
    }
}

fn incompatible(path: &str, reason: &str) -> CompatibilityOutcome {
    CompatibilityOutcome {
        result: Compatibility::Incompatible,
        diagnostics: vec![Diagnostic {
            path: path.to_owned(),
            reason: reason.to_owned(),
        }],
    }
}
