// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/areObjectsEqual.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: an object with arbitrary keys is a `HashMap<String, Value>` (a key present with an `undefined` value is a key whose value is the caller's "undefined" variant); `===` on values is `PartialEq`; `Object.keys` order does not matter to the result. The early `objA === objB` identity exit is `std::ptr::eq` on the references.

use std::collections::HashMap;
use std::collections::HashSet;

pub fn are_objects_equal<Value: PartialEq>(
    obj_a: Option<&HashMap<String, Value>>,
    obj_b: Option<&HashMap<String, Value>>,
    omit_keys: &[&str],
) -> bool {
    // Lets get out of here early if they are the same or either is undefined
    let (Some(obj_a), Some(obj_b)) = (obj_a, obj_b) else {
        return obj_a.is_none() && obj_b.is_none();
    };
    if std::ptr::eq(obj_a, obj_b) {
        return true;
    }

    // Convert array to Set for O(1) lookup
    let omit_set: HashSet<&str> = omit_keys.iter().copied().collect();
    let mut keys_b_set: HashSet<&String> = obj_b.keys().collect();

    // Compare keys from objA
    for (key, value_a) in obj_a {
        keys_b_set.remove(key);
        if omit_set.contains(key.as_str()) {
            continue;
        }

        match obj_b.get(key) {
            Some(value_b) if value_a == value_b => {}
            _ => return false,
        }
    }

    // If we got any remaining keys that aren't omitted in objB,
    // then we gotta return false
    for key in keys_b_set {
        if !omit_set.contains(key.as_str()) {
            return false;
        }
    }

    true
}
