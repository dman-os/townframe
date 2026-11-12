// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/areHunkDataEqual.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: none beyond the types.

use crate::types::lines::HunkData;

pub fn are_hunk_data_equal(hunk_a: &HunkData, hunk_b: &HunkData) -> bool {
    hunk_a.slot_name == hunk_b.slot_name
        && hunk_a.hunk_index == hunk_b.hunk_index
        && hunk_a.lines == hunk_b.lines
        && hunk_a.line_count_known == hunk_b.line_count_known
        && hunk_a.column_type == hunk_b.column_type
        && hunk_a.expandable.map(|expandable| expandable.chunked)
            == hunk_b.expandable.map(|expandable| expandable.chunked)
        && hunk_a.expandable.map(|expandable| expandable.up)
            == hunk_b.expandable.map(|expandable| expandable.up)
        && hunk_a.expandable.map(|expandable| expandable.down)
            == hunk_b.expandable.map(|expandable| expandable.down)
}
