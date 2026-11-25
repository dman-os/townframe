// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/isDefaultRenderRange.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: none beyond the types (`Infinity` is `f64::INFINITY`).

use crate::types::layout::RenderRange;

pub fn is_default_render_range(render_range: &RenderRange) -> bool {
    render_range.starting_line == 0.0
        && render_range.total_lines == f64::INFINITY
        && render_range.buffer_before == 0.0
        && render_range.buffer_after == 0.0
}
