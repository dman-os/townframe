//! The per-item stability a datum declares in its sidecar (ADR 021, "Stability is declared per
//! item"): `x-since`, `x-deprecated` and `x-unstable`. An item that declares none of them is
//! unstable: `x-unstable` only spells out the default and names the feature gate.
//!
//! An author states them where the item is declared, with the `schemars` extension that already
//! carries `x-automerge`: `#[schemars(extend("x-since" = 2))]` on a field, a variant or the type,
//! `#[schemars(extend("x-deprecated" = {"since": 3, "note": "use `b`"}))]`, or
//! `#[schemars(extend("x-unstable" = "feature"))]`. Lexicon has no extension mechanism, so none of
//! this reaches the projected `defs`; the only thing it sets in the Lexicon file is `revision`,
//! the highest revision any item in the file names.

use serde_json::Map;
use serde_json::Value;

pub const SINCE: &str = "x-since";
pub const DEPRECATED: &str = "x-deprecated";
pub const UNSTABLE: &str = "x-unstable";

/// What one schema position declares about its stability.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Declared {
    /// The revision that introduced the item.
    pub since: Option<u64>,
    /// The revision that deprecated the item, and the note saying what to use instead.
    pub deprecated: Option<(u64, String)>,
    /// The feature gate of an item that is not yet stable.
    pub unstable: Option<String>,
}

impl Declared {
    /// Reads the three keywords off one schema object; a malformed one is an error naming it,
    /// since a misspelled annotation that was ignored would leave an item unprotected.
    pub fn read(object: &Map<String, Value>) -> Result<Self, String> {
        let revision = |keyword: &str, value: &Value| {
            value
                .as_u64()
                .filter(|revision| *revision > 0)
                .ok_or_else(|| format!("`{keyword}` is a revision, a positive integer"))
        };
        let since = object
            .get(SINCE)
            .map(|value| revision(SINCE, value))
            .transpose()?;
        let deprecated = object
            .get(DEPRECATED)
            .map(|value| {
                let note = value.get("note").and_then(Value::as_str).ok_or_else(|| {
                    format!("`{DEPRECATED}` is `{{\"since\": N, \"note\": \"…\"}}`")
                })?;
                let since = value
                    .get("since")
                    .ok_or_else(|| format!("`{DEPRECATED}` states the revision it `since`"))?;
                Ok::<_, String>((revision(DEPRECATED, since)?, note.to_owned()))
            })
            .transpose()?;
        let unstable = object
            .get(UNSTABLE)
            .map(|value| {
                value
                    .as_str()
                    .filter(|feature| !feature.is_empty())
                    .map(str::to_owned)
                    .ok_or_else(|| format!("`{UNSTABLE}` names a feature"))
            })
            .transpose()?;
        if unstable.is_some() && (since.is_some() || deprecated.is_some()) {
            return Err(format!(
                "`{UNSTABLE}` excludes `{SINCE}` and `{DEPRECATED}`: stabilizing is replacing \
                 `{UNSTABLE}` with `{SINCE}`"
            ));
        }
        Ok(Self {
            since,
            deprecated,
            unstable,
        })
    }

    /// Whether the item is under the evolution rules: only an item that states a `since` (or a
    /// deprecation, which a stable item alone can have) is. Everything else is unstable, the
    /// default while the corpus is pre-release, and may change or be dropped freely.
    pub fn is_stable(&self) -> bool {
        self.since.is_some() || self.deprecated.is_some()
    }

    /// The highest revision this position names.
    pub fn revision(&self) -> Option<u64> {
        self.since
            .into_iter()
            .chain(self.deprecated.as_ref().map(|(since, _)| *since))
            .max()
    }
}

/// The highest revision any position under `value` names, validating every annotation on the way.
/// `None` when nothing is annotated, which is a file with no `revision`.
///
/// `value` is a schema subtree; a `$ref` is not followed, so the caller names the definitions the
/// file carries.
pub fn revision(value: &Value) -> Result<Option<u64>, String> {
    match value {
        Value::Object(object) => object_revision(object, &[]),
        Value::Array(items) => items
            .iter()
            .try_fold(None, |highest, item| Ok(highest.max(revision(item)?))),
        _ => Ok(None),
    }
}

/// [`revision`] of one schema object, leaving out the members named in `skip`: the datum's own
/// position is walked without the `$defs` that hold every definition in scope.
pub fn object_revision(object: &Map<String, Value>, skip: &[&str]) -> Result<Option<u64>, String> {
    let mut highest = Declared::read(object)?.revision();
    for (key, child) in object {
        // An annotation's own payload is not a schema position.
        if key == DEPRECATED || skip.contains(&key.as_str()) {
            continue;
        }
        highest = highest.max(revision(child)?);
    }
    Ok(highest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    utils_rs::table_tests! {
        reads,
        (schema, expected),
        {
            assert_eq!(revision(&schema), expected);
        }
    }

    reads! {
        nothing_declared_is_no_revision: (json!({"type": "object"}), Ok(None)),
        a_field_since_is_the_revision: (
            json!({"properties": {"a": {"type": "string", "x-since": 2}}}),
            Ok(Some(2))
        ),
        the_highest_since_wins: (
            json!({"properties": {"a": {"x-since": 2}, "b": {"x-since": 5}, "c": {"x-since": 3}}}),
            Ok(Some(5))
        ),
        a_deprecation_names_its_revision_too: (
            json!({"properties": {"a": {"x-since": 1, "x-deprecated": {"since": 4, "note": "use b"}}}}),
            Ok(Some(4))
        ),
        an_unstable_item_sets_no_revision: (
            json!({"properties": {"a": {"x-unstable": "drafts"}}}),
            Ok(None)
        ),
        since_inside_an_array_item_counts: (
            json!({"oneOf": [{"x-since": 3}, {"type": "null"}]}),
            Ok(Some(3))
        ),
        a_zero_revision_is_refused: (
            json!({"x-since": 0}),
            Err("`x-since` is a revision, a positive integer".to_owned())
        ),
        a_string_revision_is_refused: (
            json!({"x-since": "2"}),
            Err("`x-since` is a revision, a positive integer".to_owned())
        ),
        an_unstable_item_with_since_is_refused: (
            json!({"x-since": 2, "x-unstable": "drafts"}),
            Err("`x-unstable` excludes `x-since` and `x-deprecated`: stabilizing is replacing `x-unstable` with `x-since`".to_owned())
        ),
        a_deprecation_without_a_note_is_refused: (
            json!({"x-deprecated": {"since": 2}}),
            Err("`x-deprecated` is `{\"since\": N, \"note\": \"…\"}`".to_owned())
        ),
    }
}
