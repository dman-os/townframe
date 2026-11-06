//! Test helpers for the differential oracle: golden loading and structural
//! JSON comparison with a path-reporting diff printer. Test-only, original to
//! this crate.

use serde::de::DeserializeOwned;
use serde_json::Value;
use std::path::PathBuf;

/// Directory holding the checked-in goldens written by `oracle/generate.mjs`.
pub fn golden_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("oracle/golden")
}

/// Loads and deserialises one golden file. Panics on a missing or malformed
/// golden: regenerate with `node oracle/generate.mjs`.
pub fn load_golden<Golden: DeserializeOwned>(file_name: &str) -> Golden {
    let path = golden_dir().join(file_name);
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("reading golden {}: {error}", path.display()));
    serde_json::from_str(&text)
        .unwrap_or_else(|error| panic!("parsing golden {}: {error}", path.display()))
}

/// Numbers that differ only in integer-vs-float representation compare equal
/// (`serde_json` distinguishes `1` from `1.0`; JavaScript does not).
fn numbers_equal(left: &serde_json::Number, right: &serde_json::Number) -> bool {
    left == right || left.as_f64() == right.as_f64()
}

/// Returns the JSON path of the first difference between `expected` and
/// `actual`, with both values rendered, or `None` when they are equal.
pub fn first_difference(expected: &Value, actual: &Value) -> Option<String> {
    first_difference_at("$", expected, actual)
}

fn first_difference_at(path: &str, expected: &Value, actual: &Value) -> Option<String> {
    match (expected, actual) {
        (Value::Number(left), Value::Number(right)) if numbers_equal(left, right) => None,
        (Value::Array(left), Value::Array(right)) => {
            for (index, (left_item, right_item)) in left.iter().zip(right).enumerate() {
                if let Some(difference) =
                    first_difference_at(&format!("{path}[{index}]"), left_item, right_item)
                {
                    return Some(difference);
                }
            }
            (left.len() != right.len()).then(|| {
                format!(
                    "{path}: array length expected {} actual {}",
                    left.len(),
                    right.len()
                )
            })
        }
        (Value::Object(left), Value::Object(right)) => {
            for (key, left_item) in left {
                match right.get(key) {
                    Some(right_item) => {
                        if let Some(difference) =
                            first_difference_at(&format!("{path}.{key}"), left_item, right_item)
                        {
                            return Some(difference);
                        }
                    }
                    None => return Some(format!("{path}.{key}: missing in actual")),
                }
            }
            right
                .keys()
                .find(|key| !left.contains_key(*key))
                .map(|key| format!("{path}.{key}: unexpected in actual"))
        }
        (left, right) if left == right => None,
        (left, right) => Some(format!("{path}: expected {left} actual {right}")),
    }
}

/// Asserts structural equality, panicking with the first differing JSON path
/// and the case description.
#[track_caller]
pub fn assert_json_eq(context: &str, expected: &Value, actual: &Value) {
    if let Some(difference) = first_difference(expected, actual) {
        panic!("{context}\n  {difference}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn integral_floats_equal_integers() {
        assert_eq!(first_difference(&json!({"a": 1}), &json!({"a": 1.0})), None);
    }

    #[test]
    fn reports_the_first_differing_path() {
        let difference = first_difference(
            &json!({"hunks": [{"lines": ["a", "b"]}]}),
            &json!({"hunks": [{"lines": ["a", "c"]}]}),
        );
        assert_eq!(
            difference.as_deref(),
            Some("$.hunks[0].lines[1]: expected \"b\" actual \"c\"")
        );
    }

    #[test]
    fn reports_missing_and_extra_keys_and_lengths() {
        assert_eq!(
            first_difference(&json!({"a": 1}), &json!({})).as_deref(),
            Some("$.a: missing in actual")
        );
        assert_eq!(
            first_difference(&json!({}), &json!({"a": 1})).as_deref(),
            Some("$.a: unexpected in actual")
        );
        assert_eq!(
            first_difference(&json!([1]), &json!([1, 2])).as_deref(),
            Some("$: array length expected 1 actual 2")
        );
    }
}
