// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/areSelectionPointsEqual.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: none beyond the types.

use crate::types::view::SelectionPoint;

pub fn are_selection_points_equal(a: &SelectionPoint, b: &SelectionPoint) -> bool {
    a.line_number == b.line_number && a.side == b.side
}
