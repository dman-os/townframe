// Part of the S12a-2c port of FileDiff.ts (see ../file_diff.rs for the header):
// the editor-facing line queries: `isLineRenderable`, `getNearestRenderableLine`
// and `revealLine`. A view over the instance (see `file_diff_view!`).
// Changes (besides the ones in the main header):
// * Line numbers are `usize` (one-based new-file lines, as `virtual_diff_layout`
//   has them); `revealLine`'s distances are computed in `isize` so a line before
//   the gap start behaves as JavaScript's signed arithmetic does.
// * `direction: 'up' | 'down'` is `RenderableDirection`.
// * `this.hunksRenderer.getExpandedHunksMap()` is handed to the layout helpers
//   as `ExpandedHunks::borrowed(map)`, shared as upstream shares the `Map`
//   (the renderer stays borrowed while a query runs; `revealLine` releases it
//   before `expandHunk` writes).
// * `this.expandHunk(...)` is `FileDiff::expand_hunk`; the `VirtualizedFileDiff`
//   override that upstream's comment mentions (CodeView's deferred
//   `pendingExpansions`) is S12b's.
// * `revealLine` passes `expansionLineCount` through as an `f64` count, as
//   `expandHunk`'s `expansionLineCountOverride` is a `number`.

use super::*;
use crate::constants::DEFAULT_COLLAPSED_CONTEXT_THRESHOLD;
use crate::types::lines::ExpandedHunks;
use crate::utils::virtual_diff_layout::GetExpandedRegionProps;
use crate::utils::virtual_diff_layout::GetTrailingExpandedRegionProps;
use crate::utils::virtual_diff_layout::IsAdditionLineRenderableProps;
use crate::utils::virtual_diff_layout::RenderableDirection;
use crate::utils::virtual_diff_layout::get_expanded_region;
use crate::utils::virtual_diff_layout::get_hunk_addition_line_range;
use crate::utils::virtual_diff_layout::get_nearest_renderable_addition_line;
use crate::utils::virtual_diff_layout::get_trailing_expanded_region;
use crate::utils::virtual_diff_layout::is_addition_line_renderable;

file_diff_view!(pub Lines);

impl<Host, Rendered, LAnnotation> Lines<'_, Host, Rendered, LAnnotation>
where
    Host: FileDiffHost,
    Rendered: RenderedContent + 'static,
    LAnnotation: Clone + PartialEq + 'static,
{
    /// `expandedHunks: expandUnchanged ? true : this.hunksRenderer.getExpandedHunksMap()`,
    /// handed to `body` while the renderer is borrowed (the map is shared, not copied).
    fn with_expanded_hunks<Output>(
        &self,
        expand_unchanged: bool,
        body: impl FnOnce(&ExpandedHunks<'_>) -> Output,
    ) -> Output {
        if expand_unchanged {
            body(&ExpandedHunks::All)
        } else {
            let renderer = self.hunks_renderer.borrow();
            body(&ExpandedHunks::borrowed(renderer.get_expanded_hunks_map()))
        }
    }

    /// `expandUnchanged` and `collapsedContextThreshold` with upstream's defaults.
    fn expansion_options(&self) -> (bool, usize) {
        let options = self.options.borrow();
        (
            options.base.expand_unchanged.unwrap_or(false),
            options
                .base
                .collapsed_context_threshold
                .unwrap_or(DEFAULT_COLLAPSED_CONTEXT_THRESHOLD),
        )
    }

    // Editor-facing visibility oracle: whether a one-based new-file line has
    // (or will have on scroll) a rendered row under the current expansion
    // state. See isAdditionLineRenderable.
    pub fn is_line_renderable(&self, line_number: usize) -> bool {
        let Some(file_diff) = self.get_rendered_diff() else {
            return true;
        };
        let (expand_unchanged, collapsed_context_threshold) = self.expansion_options();
        self.with_expanded_hunks(expand_unchanged, |expanded_hunks| {
            is_addition_line_renderable(IsAdditionLineRenderableProps {
                file_diff: &file_diff,
                line_number,
                expanded_hunks: Some(expanded_hunks),
                collapsed_context_threshold,
            })
        })
    }

    // Fold-skip companion to isLineRenderable: the nearest renderable one-based
    // new-file line at or beyond lineNumber in the given direction.
    pub fn get_nearest_renderable_line(
        &self,
        line_number: usize,
        direction: RenderableDirection,
    ) -> Option<usize> {
        let Some(file_diff) = self.get_rendered_diff() else {
            return Some(line_number);
        };
        let (expand_unchanged, collapsed_context_threshold) = self.expansion_options();
        self.with_expanded_hunks(expand_unchanged, |expanded_hunks| {
            get_nearest_renderable_addition_line(
                IsAdditionLineRenderableProps {
                    file_diff: &file_diff,
                    line_number,
                    expanded_hunks: Some(expanded_hunks),
                    collapsed_context_threshold,
                },
                direction,
            )
        })
    }

    // Expand collapsed context so a one-based new-file line can render: one
    // deterministic expansion from the nearest gap edge, sized to reach the
    // target plus the normal expansion step (clamped to the gap at render).
    // Routed through expandHunk so subclass expansion flows (CodeView's
    // deferred pendingExpansions) apply.
    pub fn reveal_line(&self, line_number: usize) -> bool {
        let file_diff = self.get_rendered_diff();
        let (expand_unchanged, collapsed_context_threshold, expansion_line_count) = {
            let options = self.options.borrow();
            (
                options.base.expand_unchanged.unwrap_or(false),
                options
                    .base
                    .collapsed_context_threshold
                    .unwrap_or(DEFAULT_COLLAPSED_CONTEXT_THRESHOLD),
                options.base.expansion_line_count.unwrap_or(100),
            )
        };
        let Some(file_diff) = file_diff.filter(|diff| !diff.is_partial && !expand_unchanged) else {
            return false;
        };
        let renderer = self.hunks_renderer.borrow();
        let expanded_hunks = ExpandedHunks::borrowed(renderer.get_expanded_hunks_map());
        let line = line_number as isize;

        for (hunk_index, hunk) in file_diff.hunks.iter().enumerate() {
            let (hunk_start, hunk_end) = get_hunk_addition_line_range(hunk);
            if line_number < hunk_start {
                let region = get_expanded_region(GetExpandedRegionProps {
                    is_partial: file_diff.is_partial,
                    range_size: hunk.collapsed_before,
                    expanded_hunks: Some(&expanded_hunks),
                    hunk_index,
                    collapsed_context_threshold,
                });
                let gap_start = hunk_start as isize - region.range_size as isize;
                if region.render_all
                    || line < gap_start + region.from_start as isize
                    || line >= hunk_start as isize - region.from_end as isize
                {
                    return false;
                }
                let from_start_distance = line - (gap_start + region.from_start as isize) + 1;
                let from_end_distance = hunk_start as isize - region.from_end as isize - line;
                let (direction, distance) = if from_start_distance <= from_end_distance {
                    (ExpansionDirections::Up, from_start_distance)
                } else {
                    (ExpansionDirections::Down, from_end_distance)
                };
                // `expandHunk` writes the renderer's map: release the shared borrow.
                drop(expanded_hunks);
                drop(renderer);
                self.expand_hunk(
                    hunk_index as i64,
                    direction,
                    Some((distance + expansion_line_count as isize) as f64),
                );
                return true;
            }
            if line_number < hunk_end {
                return false;
            }
        }

        let trailing_region = get_trailing_expanded_region(GetTrailingExpandedRegionProps {
            file_diff: &file_diff,
            hunk_index: file_diff.hunks.len().saturating_sub(1),
            expanded_hunks: Some(&expanded_hunks),
            collapsed_context_threshold,
            error_prefix: "FileDiff.revealLine",
        });
        // `expandHunk` writes the renderer's map: release the shared borrow.
        drop(expanded_hunks);
        drop(renderer);
        let Some(trailing_region) = trailing_region.filter(|region| !region.render_all) else {
            return false;
        };
        let last_hunk = file_diff
            .hunks
            .last()
            .expect("a trailing region implies hunks");
        let (_, trailing_start) = get_hunk_addition_line_range(last_hunk);
        if line_number < trailing_start + trailing_region.from_start
            || line_number >= trailing_start + trailing_region.range_size
        {
            return false;
        }
        self.expand_hunk(
            file_diff.hunks.len() as i64,
            ExpansionDirections::Up,
            Some(
                (line - (trailing_start + trailing_region.from_start) as isize
                    + 1
                    + expansion_line_count as isize) as f64,
            ),
        );
        true
    }
}
