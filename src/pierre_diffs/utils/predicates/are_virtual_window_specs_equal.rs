// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/areVirtualWindowSpecsEqual.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: `VirtualWindowSpecs | undefined` is `Option<&VirtualWindowSpecs>`.

use crate::types::layout::VirtualWindowSpecs;

pub fn are_virtual_window_specs_equal(
    window_specs_a: Option<&VirtualWindowSpecs>,
    window_specs_b: Option<&VirtualWindowSpecs>,
) -> bool {
    match (window_specs_a, window_specs_b) {
        (Some(window_specs_a), Some(window_specs_b)) => {
            window_specs_a.top == window_specs_b.top
                && window_specs_a.bottom == window_specs_b.bottom
        }
        // Upstream returns `a === b` once either is null/undefined: only two
        // absent values are equal.
        (window_specs_a, window_specs_b) => window_specs_a.is_none() && window_specs_b.is_none(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_values_only_equal_each_other() {
        let specs = VirtualWindowSpecs {
            top: 1.0,
            bottom: 2.0,
        };
        assert!(are_virtual_window_specs_equal(None, None));
        assert!(!are_virtual_window_specs_equal(Some(&specs), None));
        assert!(!are_virtual_window_specs_equal(None, Some(&specs)));
        assert!(are_virtual_window_specs_equal(Some(&specs), Some(&specs)));
        let other = VirtualWindowSpecs {
            top: 1.0,
            bottom: 3.0,
        };
        assert!(!are_virtual_window_specs_equal(Some(&specs), Some(&other)));
    }
}
