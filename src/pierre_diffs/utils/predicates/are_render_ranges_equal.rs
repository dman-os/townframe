// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/areRenderRangesEqual.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: none beyond the types.

use crate::types::layout::RenderRange;

pub fn are_render_ranges_equal(
    render_range_a: Option<&RenderRange>,
    render_range_b: Option<&RenderRange>,
) -> bool {
    let (Some(render_range_a), Some(render_range_b)) = (render_range_a, render_range_b) else {
        return render_range_a.is_none() && render_range_b.is_none();
    };
    render_range_a.starting_line == render_range_b.starting_line
        && render_range_a.total_lines == render_range_b.total_lines
        && render_range_a.buffer_before == render_range_b.buffer_before
        && render_range_a.buffer_after == render_range_b.buffer_after
}
