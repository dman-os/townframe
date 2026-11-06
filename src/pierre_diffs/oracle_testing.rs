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

/// FNV-1a 64 over the sorted-key canonical JSON of `value`, the same digest as
/// `fnv64` in `oracle/generate.mjs`. Integral floats print as integers, as
/// `JSON.stringify` writes them.
pub fn fnv64(value: &Value) -> String {
    fn write(value: &Value, out: &mut String) {
        match value {
            Value::Array(items) => {
                out.push('[');
                for (index, item) in items.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    write(item, out);
                }
                out.push(']');
            }
            Value::Object(map) => {
                let mut entries: Vec<_> = map.iter().collect();
                entries.sort_by(|left, right| left.0.cmp(right.0));
                out.push('{');
                for (index, (key, entry)) in entries.into_iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    out.push_str(&serde_json::to_string(key).expect("key"));
                    out.push(':');
                    write(entry, out);
                }
                out.push('}');
            }
            Value::Number(number) => match (number.as_i64(), number.as_f64()) {
                (Some(integer), _) => out.push_str(&integer.to_string()),
                (None, Some(float)) if float.fract() == 0.0 && float.abs() < 9e15 => {
                    out.push_str(&(float as i64).to_string());
                }
                _ => out.push_str(&number.to_string()),
            },
            other => out.push_str(&serde_json::to_string(other).expect("scalar")),
        }
    }
    let mut canonical = String::new();
    write(value, &mut canonical);
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in canonical.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    format!("{hash:016x}")
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
