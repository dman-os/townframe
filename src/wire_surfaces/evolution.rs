//! How a Lexicon may change between two published copies.
//!
//! A change is compatible when a reader built against the old copy still reads every value the
//! new copy admits: a new optional property, a new `knownValues` entry, a new member of an open
//! union. Anything else that changes the wire shape is breaking, and a breaking change mints a new
//! NSID instead of editing this one (operator rule, 2026-11-06). The classifier fails closed: a
//! keyword it has no compatible rule for counts as breaking when it differs. Only `description`,
//! prose a reader never validates against, is ignored.
//!
//! Stability is declared per item (ADR 021): a field, definition, method or union member is under
//! the rules only when it states `since` (or `deprecated`). A stable item is never removed or
//! changed incompatibly, and a new one carries `since` equal to the file's new `revision`. Every
//! other item is unstable, the pre-release default, and is outside the rules until it is
//! stabilized.

use crate::items;
use crate::sidecar;
use big_cabinet_core::datum::lexicon::stability::Declared;
use serde_json::Map;
use serde_json::Value;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Compatible,
    Breaking,
}

/// One difference between two copies of a Lexicon, at a JSON path inside it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Difference {
    pub path: String,
    pub severity: Severity,
    pub what: String,
}

/// Every wire-visible difference between the `old` and `new` copies of one Lexicon.
pub fn classify(old: &Value, new: &Value) -> Vec<Difference> {
    let mut found = Vec::new();
    walk("", old, new, &mut found);
    found
}

/// A published Lexicon and the sidecar it ships with, if it has one.
#[derive(Debug, Clone, Copy)]
pub struct Surface<'a> {
    pub lexicon: &'a Value,
    pub sidecar: Option<&'a Value>,
}

impl<'a> Surface<'a> {
    pub fn of(lexicon: &'a Value) -> Self {
        Self {
            lexicon,
            sidecar: None,
        }
    }

    fn revision(&self) -> u64 {
        self.lexicon["revision"].as_u64().unwrap_or(0)
    }
}

/// Whether the evolution rules reach a difference at `path` of the Lexicon: the nearest item
/// around it decides, and only a stable one (in the old copy, or for a new item in the new one)
/// is under them. A `required` or `nullable` entry belongs to the property it names.
fn governed(
    path: &str,
    to_path: fn(&str) -> String,
    old: &BTreeMap<String, Declared>,
    new: &BTreeMap<String, Declared>,
) -> bool {
    let segments: Vec<&str> = path.split('/').collect();
    let owner = segments
        .iter()
        .position(|segment| matches!(*segment, "required" | "nullable"))
        .filter(|at| at + 1 < segments.len())
        .map(|at| {
            format!(
                "{}/properties/{}",
                segments[..at].join("/"),
                segments[at + 1]
            )
        });
    let path = owner.as_deref().unwrap_or(path);
    old.keys()
        .chain(new.keys())
        .map(|item| (to_path(item), item))
        .filter(|(at, _)| {
            path.strip_prefix(at.as_str())
                .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
        })
        .max_by_key(|(at, _)| at.len())
        .is_some_and(|(_, item)| {
            old.get(item)
                .or_else(|| new.get(item))
                .is_some_and(Declared::is_stable)
        })
}

/// What stops `new` from replacing `old` under the same NSID: a breaking difference, a stable item
/// that was removed or changed, a new item that does not carry the file's new revision as its
/// `since`, or compatible differences without a bumped `revision`. An absent `revision` counts as
/// 0. Differences that sit in an unstable item are outside the rules.
pub fn violations(nsid: &str, old: Surface, new: Surface) -> Vec<String> {
    let (old_items, new_items) = (items::items(&old), items::items(&new));
    let differences: Vec<Difference> = classify(old.lexicon, new.lexicon)
        .into_iter()
        .filter(|difference| {
            governed(
                &difference.path,
                items::lexicon_path,
                &old_items,
                &new_items,
            )
        })
        .collect();
    let mut found: Vec<String> = differences
        .iter()
        .filter(|difference| difference.severity == Severity::Breaking)
        .map(|difference| {
            format!(
                "{nsid}: breaking change at `{}`: {}; a breaking change mints a new NSID",
                difference.path, difference.what
            )
        })
        .collect();
    // The sidecar's rules govern stable items only, like the Lexicon's. Its compatible changes
    // (a loosened constraint, a new field) need no bump: a revision comes from `since` alone.
    let sidecar_breaking = match (old.sidecar, new.sidecar) {
        (Some(old), Some(new)) => sidecar::classify(old, new),
        (None, None) => Vec::new(),
        (before, _) => vec![Difference {
            path: String::new(),
            severity: Severity::Breaking,
            what: format!(
                "the sidecar {}",
                if before.is_some() {
                    "disappeared"
                } else {
                    "appeared"
                }
            ),
        }],
    };
    found.extend(
        sidecar_breaking
            .into_iter()
            .filter(|difference| difference.severity == Severity::Breaking)
            .filter(|difference| {
                governed(&difference.path, str::to_owned, &old_items, &new_items)
            })
            .map(|difference| {
                format!(
                    "{nsid}: breaking sidecar change at `{}`: {}; a breaking change mints a new NSID",
                    difference.path, difference.what
                )
            }),
    );
    found.extend(item_violations(
        nsid,
        &old_items,
        &new_items,
        (old.revision(), new.revision()),
    ));
    if !found.is_empty() {
        return found;
    }
    if differences.is_empty() || new.revision() > old.revision() {
        return Vec::new();
    }
    let listed: Vec<_> = differences
        .iter()
        .map(|difference| format!("`{}` {}", difference.path, difference.what))
        .collect();
    vec![format!(
        "{nsid}: compatible changes ({}) need a bumped `revision`, still {}",
        listed.join("; "),
        old.revision()
    )]
}

/// The rules about items themselves, which the shape of the Lexicon cannot show: nothing stable is
/// removed or made unstable, a stable item keeps its `since`, and a new stable item carries the
/// new revision.
fn item_violations(
    nsid: &str,
    old: &BTreeMap<String, Declared>,
    new: &BTreeMap<String, Declared>,
    (old_revision, revision): (u64, u64),
) -> Vec<String> {
    let mut found = Vec::new();
    let named = |path: &str| {
        if path.is_empty() {
            "the file".to_owned()
        } else {
            format!("`{path}`")
        }
    };
    // An item inside an item that is itself removed (or new) is covered by the outer one.
    let inside = |path: &str, outer: &dyn Fn(&str) -> bool| {
        (0..path.len())
            .filter(|at| path.as_bytes()[*at] == b'/')
            .any(|at| at > 0 && outer(&path[..at]))
    };
    for (path, before) in old {
        if !before.is_stable() {
            continue;
        }
        match new.get(path) {
            None if inside(path, &|outer| {
                old.contains_key(outer) && !new.contains_key(outer)
            }) => {}
            None => found.push(format!(
                "{nsid}: {} was stable and was removed; deprecate it instead",
                named(path)
            )),
            Some(after) if !after.is_stable() => found.push(format!(
                "{nsid}: {} was stable and became unstable",
                named(path)
            )),
            Some(after) => {
                if before.since != after.since {
                    found.push(format!(
                        "{nsid}: {} is stable and changed its `since` from {:?} to {:?}",
                        named(path),
                        before.since,
                        after.since
                    ));
                }
                if let Some((since, _)) = &after.deprecated
                    && before.deprecated.is_none()
                    && *since != revision
                {
                    found.push(format!(
                        "{nsid}: {} is newly deprecated since {since}, and the file's revision \
                         is {revision}",
                        named(path)
                    ));
                }
            }
        }
    }
    let is_new = |path: &str| old.get(path).is_none_or(|before| !before.is_stable());
    for (path, after) in new {
        if path.is_empty()
            || !after.is_stable()
            || !is_new(path)
            || inside(path, &|outer| {
                new.contains_key(outer) && is_new(outer) && new[outer].is_stable()
            })
        {
            continue;
        }
        if after.since != Some(revision) {
            found.push(format!(
                "{nsid}: new stable item {} must carry `since = {revision}`, the file's new \
                 revision (it was {old_revision}), and carries {:?}",
                named(path),
                after.since
            ));
        }
    }
    found
}

fn push(found: &mut Vec<Difference>, path: &str, severity: Severity, what: String) {
    found.push(Difference {
        path: path.to_owned(),
        severity,
        what,
    });
}

fn child(path: &str, key: &str) -> String {
    format!("{path}/{key}")
}

fn strings(value: Option<&Value>) -> BTreeSet<&str> {
    value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect()
}

/// Reports each member of `after` not in `before` as `added` and each member of `before` not in
/// `after` as `removed`, with the severity each side is given.
fn set_diff(
    found: &mut Vec<Difference>,
    path: &str,
    sets: (BTreeSet<&str>, BTreeSet<&str>),
    what: (&str, &str),
    severity: (Severity, Severity),
) {
    let (before, after) = sets;
    for name in after.difference(&before) {
        push(
            found,
            &child(path, name),
            severity.0,
            format!("{} `{name}`", what.0),
        );
    }
    for name in before.difference(&after) {
        push(
            found,
            &child(path, name),
            severity.1,
            format!("{} `{name}`", what.1),
        );
    }
}

/// The `name` of each entry of a method's `errors`.
fn error_names(value: Option<&Value>) -> BTreeSet<&str> {
    value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|error| error["name"].as_str())
        .collect()
}

fn walk(path: &str, old: &Value, new: &Value, found: &mut Vec<Difference>) {
    if old == new {
        return;
    }
    let (Value::Object(old), Value::Object(new)) = (old, new) else {
        push(
            found,
            path,
            Severity::Breaking,
            format!("changed from {old} to {new}"),
        );
        return;
    };
    if old.get("type") != new.get("type") {
        let show = |object: &Map<String, Value>| {
            object
                .get("type")
                .map_or_else(|| "none".to_owned(), Value::to_string)
        };
        push(
            found,
            path,
            Severity::Breaking,
            format!("type changed from {} to {}", show(old), show(new)),
        );
        return;
    }
    let keys: BTreeSet<&String> = old.keys().chain(new.keys()).collect();
    for key in keys {
        let at = child(path, key);
        let (before, after) = (old.get(key), new.get(key));
        let names = || (strings(before), strings(after));
        match key.as_str() {
            "description" => {}
            // Read by `violations`, which asks that a compatible change bump it.
            "revision" if path.is_empty() => {}
            // Maps from a name to a schema: a name that appears is a new optional member (it is
            // `required` that makes it mandatory), one that disappears is removed or renamed.
            "properties" | "defs" => {
                let empty = Map::new();
                let as_map = |value: Option<&Value>| {
                    value.and_then(Value::as_object).unwrap_or(&empty).clone()
                };
                let (before, after) = (as_map(before), as_map(after));
                for (name, after) in &after {
                    match before.get(name) {
                        Some(before) => walk(&child(&at, name), before, after, found),
                        None => push(
                            found,
                            &child(&at, name),
                            Severity::Compatible,
                            "added".to_owned(),
                        ),
                    }
                }
                for name in before.keys().filter(|name| !after.contains_key(*name)) {
                    push(
                        found,
                        &child(&at, name),
                        Severity::Breaking,
                        "removed or renamed".to_owned(),
                    );
                }
            }
            "required" => set_diff(
                found,
                &at,
                names(),
                ("became required:", "is no longer required:"),
                (Severity::Breaking, Severity::Breaking),
            ),
            "knownValues" => set_diff(
                found,
                &at,
                names(),
                ("new known value", "known value removed:"),
                (Severity::Compatible, Severity::Breaking),
            ),
            "enum" => set_diff(
                found,
                &at,
                names(),
                ("closed enum gained", "closed enum lost"),
                (Severity::Breaking, Severity::Breaking),
            ),
            // An open union admits members its readers do not know, so a new member is
            // compatible there; a closed union's readers reject it. Losing a member is breaking
            // either way: values of it may be stored.
            "refs" => set_diff(
                found,
                &child(path, "$union"),
                names(),
                ("union gained member", "union lost member"),
                (
                    if new.get("closed") == Some(&Value::Bool(true)) {
                        Severity::Breaking
                    } else {
                        Severity::Compatible
                    },
                    Severity::Breaking,
                ),
            ),
            // Closing a union makes its readers reject what they accepted; opening one only
            // permits members nobody has added yet.
            "closed" => {
                let closed = |value: Option<&Value>| value == Some(&Value::Bool(true));
                let severity = if closed(after) {
                    Severity::Breaking
                } else {
                    Severity::Compatible
                };
                if closed(before) != closed(after) {
                    push(
                        found,
                        &at,
                        severity,
                        format!("closed is now {}", closed(after)),
                    );
                }
            }
            "errors" => {
                set_diff(
                    found,
                    &at,
                    (error_names(before), error_names(after)),
                    ("error added:", "error removed:"),
                    (Severity::Compatible, Severity::Breaking),
                );
            }
            // Any other keyword (a bound, a format, a body's encoding, a nested schema).
            _ => match (before, after) {
                (Some(before), Some(after)) => walk(&at, before, after, found),
                (Some(_), None) => push(found, &at, Severity::Breaking, "removed".to_owned()),
                (None, Some(_)) => push(found, &at, Severity::Breaking, "added".to_owned()),
                (None, None) => unreachable!("the key came from one of the two"),
            },
        }
    }
}

#[cfg(test)]
mod tests;
