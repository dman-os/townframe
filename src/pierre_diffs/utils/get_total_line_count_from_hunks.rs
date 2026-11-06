// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/getTotalLineCountFromHunks.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: none beyond the language.

use crate::types::patch::Hunk;
use crate::utils::get_hunk_side_boundaries::get_hunk_side_end_boundary;

pub fn get_total_line_count_from_hunks(hunks: &[Hunk]) -> usize {
    let Some(last_hunk) = hunks.last() else {
        return 0;
    };
    get_hunk_side_end_boundary(last_hunk.addition_start, last_hunk.addition_count).max(
        get_hunk_side_end_boundary(last_hunk.deletion_start, last_hunk.deletion_count),
    )
}
