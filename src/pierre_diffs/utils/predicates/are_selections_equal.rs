// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/areSelectionsEqual.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: `SelectedLineRange | undefined` is `Option<&SelectedLineRange>`; `a?.x === b?.x` on an absent side compares `undefined`, as `Option` comparison does.

use crate::types::view::SelectedLineRange;

pub fn are_selections_equal(
    selection_a: Option<&SelectedLineRange>,
    selection_b: Option<&SelectedLineRange>,
) -> bool {
    selection_a.map(|selection| selection.start) == selection_b.map(|selection| selection.start)
        && selection_a.map(|selection| selection.end) == selection_b.map(|selection| selection.end)
        && selection_a.and_then(|selection| selection.side)
            == selection_b.and_then(|selection| selection.side)
        && selection_a.and_then(|selection| selection.end_side)
            == selection_b.and_then(|selection| selection.end_side)
}
