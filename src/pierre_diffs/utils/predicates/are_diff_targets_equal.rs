// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/areDiffTargetsEqual.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: JavaScript's `diffA === diffB` object identity is `std::ptr::eq`
// on the references.

use crate::types::patch::FileDiffMetadata;

// Because FileDiffMetadata is a potentially unbounded and structurally deep
// object, we only check for top level equality or a cache key, otherwise we
// just assume it's different every time
pub fn are_diff_targets_equal(
    diff_a: Option<&FileDiffMetadata>,
    diff_b: Option<&FileDiffMetadata>,
) -> bool {
    let key_a = diff_a.and_then(|diff| diff.cache_key.as_ref());
    let key_b = diff_b.and_then(|diff| diff.cache_key.as_ref());
    if key_a.is_some() || key_b.is_some() {
        key_a == key_b
    } else {
        match (diff_a, diff_b) {
            (Some(a_diff), Some(b_diff)) => std::ptr::eq(a_diff, b_diff),
            (None, None) => true,
            _ => false,
        }
    }
}
