// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/components/VirtualizedFileDiff.ts
// (S12b-2: the window half of the layout, `getLinePosition` 652-785 and
// `computeRenderRangeFromWindow` 1965-2200).
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: as in `layout.rs`, the methods are `DiffLayout` methods over a
// `LayoutContext` (what the class reads through `this`); `hasFileAnnotations`
// reads `getLayoutAnnotations()`, which arrives as the `line_annotations`
// argument. `getLinePosition(lineNumber)` takes a `usize`: upstream's
// `lineNumber < 1` guard is `line_number == 0` (a fractional or negative line
// number is not expressible). The `throw` of `getLinePosition` (a row without
// line index data) is a panic, as in `layout.rs`. `computeRenderRangeFromWindow`
// asserts `hunkLineCount > 0` after its early returns: a zero `hunkLineCount`
// makes upstream's arithmetic `NaN`, which is a misconfiguration (the type is
// `usize`, and the layout oracle's `zeroHunk` metrics are only compared through
// the layout methods that never divide by it). The callback of `iterateOverDiff`
// is `RenderScan::visit`, a method over the scan's variables (`absoluteLineTop`,
// `currentLine`, `firstVisibleHunk`, `centerHunk`, `overflowCounter`,
// `hunkOffsets`) so that the function stays within the function budget.
// `hunkOffsets` is a JavaScript array written at increasing hunk indexes that
// start at the first scanned hunk (the scan origin is a checkpoint at a hunk
// boundary), so its holes are exactly the indexes below `first_hunk`:
// `HunkOffsets` stores the contiguous tail and answers `undefined` (`None`) for
// the holes, and `.length` is `first_hunk + offsets.len()`; reading a hole as a
// number is `NaN` as in JavaScript (`NAN` below, with `Math.max(0, NaN)` being
// `NaN`: `js_max_zero`). `Math.round` is `js_round` (round half up, unlike
// `f64::round`).

use super::layout::DiffLayout;
use super::layout::LayoutContext;
use super::layout::LayoutOptions;
use super::layout::has_file_annotations;
use crate::components::file_diff::get_line_index_for_diff;
use crate::types::layout::RenderRange;
use crate::types::layout::RenderWindow;
use crate::types::layout::VirtualFileMetrics;
use crate::types::options::DiffStyle;
use crate::types::patch::FileDiffMetadata;
use crate::types::view::DiffLineAnnotation;
use crate::types::view::SelectionSide;
use crate::utils::compute_virtual_file_metrics::get_virtual_file_header_height;
use crate::utils::compute_virtual_file_metrics::get_virtual_file_header_region;
use crate::utils::compute_virtual_file_metrics::get_virtual_file_padding_bottom;
use crate::utils::iterate_over_diff::DiffLineCallbackProps;
use crate::utils::iterate_over_diff::IterateOverDiffProps;
use crate::utils::iterate_over_diff::IterationDiffStyle;
use crate::utils::iterate_over_diff::iterate_over_diff;
use crate::utils::virtual_diff_layout::HunkSeparatorBaseProps;
use crate::utils::virtual_diff_layout::get_leading_hunk_separator_layout;
use crate::utils::virtual_diff_layout::get_trailing_hunk_separator_layout;

/// `{ top: number; height: number }`, the result of `getLinePosition`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LinePosition {
    pub top: f64,
    pub height: f64,
}

fn iteration_style(diff_style: DiffStyle) -> IterationDiffStyle {
    match diff_style {
        DiffStyle::Split => IterationDiffStyle::Split,
        DiffStyle::Unified => IterationDiffStyle::Unified,
    }
}

/// `Math.round`: halves round up (towards positive infinity).
fn js_round(value: f64) -> f64 {
    let floor = value.floor();
    if value - floor >= 0.5 {
        floor + 1.0
    } else {
        floor
    }
}

/// `Math.max(0, value)`: a `NaN` stays `NaN` (`f64::max` would return `0`).
fn js_max_zero(value: f64) -> f64 {
    if value.is_nan() {
        value
    } else {
        value.max(0.0)
    }
}

/// `hunkOffsets`: see the header.
#[derive(Default)]
struct HunkOffsets {
    first_hunk: usize,
    offsets: Vec<f64>,
}

impl HunkOffsets {
    /// `hunkOffsets[hunk] = offset`; hunks arrive in increasing order, each
    /// boundary once, and the first one fixes where the array starts.
    fn set(&mut self, hunk: usize, offset: f64) {
        if self.offsets.is_empty() {
            self.first_hunk = hunk;
        }
        let index = hunk - self.first_hunk;
        if index < self.offsets.len() {
            self.offsets[index] = offset;
        } else {
            assert_eq!(index, self.offsets.len(), "hunk offsets are contiguous");
            self.offsets.push(offset);
        }
    }

    fn get(&self, hunk: usize) -> Option<f64> {
        hunk.checked_sub(self.first_hunk)
            .and_then(|index| self.offsets.get(index))
            .copied()
    }

    /// `bufferAfter` before its clamp: `codeHeight - hunkOffsets[finalHunkIndex]`
    /// when the scan reached that hunk, `codeHeight - scanned` otherwise; a hole
    /// is `NaN` (see the header), and `Math.max(0, ...)` keeps it.
    fn buffer_after(&self, final_hunk_index: f64, code_height: f64, scanned: f64) -> f64 {
        let buffer_after = if final_hunk_index < self.length() as f64 {
            code_height - self.get(final_hunk_index as usize).unwrap_or(f64::NAN)
        } else {
            code_height - scanned
        };
        js_max_zero(buffer_after)
    }

    /// `hunkOffsets.length`
    fn length(&self) -> usize {
        if self.offsets.is_empty() {
            0
        } else {
            self.first_hunk + self.offsets.len()
        }
    }
}

/// The variables `computeRenderRangeFromWindow`'s `iterateOverDiff` callback
/// closes over, with what the callback reads.
struct RenderScan<'layout> {
    layout: &'layout DiffLayout,
    file_diff: &'layout FileDiffMetadata,
    options: LayoutOptions,
    can_hydrate_partial_diff: bool,
    hunk_line_count: usize,
    top: f64,
    bottom: f64,
    file_top: f64,
    code_region_top: f64,
    viewport_center: f64,
    overflow_hunks: f64,
    absolute_line_top: f64,
    current_line: usize,
    first_visible_hunk: Option<usize>,
    center_hunk: Option<usize>,
    overflow_counter: Option<f64>,
    hunk_offsets: HunkOffsets,
}

impl RenderScan<'_> {
    /// The `iterateOverDiff` callback; `true` stops the iteration.
    fn visit(&mut self, props: DiffLineCallbackProps<'_>) -> bool {
        let DiffLineCallbackProps {
            hunk_index,
            hunk,
            collapsed_before,
            collapsed_after,
            deletion_line,
            addition_line,
            ..
        } = props;
        let line = addition_line
            .or(deletion_line)
            .expect("a row has an addition or a deletion line");
        let split_line_index = line.split_line_index;
        let unified_line_index = line.unified_line_index;
        let has_metadata = addition_line.is_some_and(|line| line.no_eofcr)
            || deletion_line.is_some_and(|line| line.no_eofcr);
        let diff_style = self.options.diff_style;
        let is_final_hunk_row = hunk_index == self.file_diff.hunks.len() - 1
            && hunk.is_some_and(|hunk| match diff_style {
                DiffStyle::Split => {
                    split_line_index == hunk.split_line_start + hunk.split_line_count - 1
                }
                DiffStyle::Unified => {
                    unified_line_index == hunk.unified_line_start + hunk.unified_line_count - 1
                }
            });
        let separator_props = HunkSeparatorBaseProps {
            separator_type: self.options.hunk_separators,
            metrics: &self.layout.metrics,
        };
        let leading_separator = if collapsed_before > 0 {
            get_leading_hunk_separator_layout(
                separator_props,
                hunk_index,
                hunk.and_then(|hunk| hunk.hunk_specs.as_deref()),
            )
        } else {
            None
        };
        let gap_adjustment = leading_separator.map_or(0.0, |layout| layout.total_height);

        self.absolute_line_top += gap_adjustment;

        let is_at_hunk_boundary = self.current_line.is_multiple_of(self.hunk_line_count);
        let current_hunk = self.current_line / self.hunk_line_count;

        // Track the boundary positional offset at a hunk
        if is_at_hunk_boundary {
            self.hunk_offsets.set(
                current_hunk,
                self.absolute_line_top - (self.file_top + self.code_region_top + gap_adjustment),
            );

            // Check if we should bail (overflow complete)
            if let Some(overflow_counter) = self.overflow_counter {
                if overflow_counter <= 0.0 {
                    return true;
                }
                self.overflow_counter = Some(overflow_counter - 1.0);
            }
        }

        let line_height = self.layout.get_line_height(
            match diff_style {
                DiffStyle::Split => split_line_index,
                DiffStyle::Unified => unified_line_index,
            } as i64,
            has_metadata,
        );

        // Track visible region
        if self.absolute_line_top > self.top - line_height && self.absolute_line_top < self.bottom {
            self.first_visible_hunk.get_or_insert(current_hunk);
        }

        // Track which hunk contains the viewport center
        // If viewport center is above this line and we haven't set centerHunk yet,
        // this is the first line at or past the center
        if self.center_hunk.is_none() && self.absolute_line_top + line_height > self.viewport_center
        {
            self.center_hunk = Some(current_hunk);
        }

        // Start overflow when we are out of the viewport at a hunk boundary
        if self.overflow_counter.is_none()
            && self.absolute_line_top >= self.bottom
            && is_at_hunk_boundary
        {
            self.overflow_counter = Some(self.overflow_hunks);
        }

        self.current_line += 1;
        self.absolute_line_top += line_height;

        if collapsed_after > 0 || (is_final_hunk_row && self.can_hydrate_partial_diff) {
            let trailing_separator = get_trailing_hunk_separator_layout(separator_props);
            if let Some(trailing_separator) = trailing_separator {
                if self.absolute_line_top < self.bottom
                    && self.absolute_line_top + trailing_separator.total_height > self.top
                {
                    self.first_visible_hunk.get_or_insert(current_hunk);
                }
                if self.center_hunk.is_none()
                    && self.absolute_line_top + trailing_separator.total_height
                        > self.viewport_center
                {
                    self.center_hunk = Some(current_hunk);
                }
                self.absolute_line_top += trailing_separator.total_height;
            }
        }

        false
    }
}

/// The file's regions, as `computeRenderRangeFromWindow` derives them before
/// looking at the window's rows.
struct FileGeometry {
    header_region: f64,
    padding_bottom: f64,
    file_annotation_height: f64,
    code_region_top: f64,
    code_height: f64,
    /// `measuredFileAnnotationVisible`
    measured_file_annotation_visible: bool,
}

impl DiffLayout {
    fn file_geometry<Metadata>(
        &self,
        file_diff: &FileDiffMetadata,
        file_top: f64,
        RenderWindow { top, bottom }: RenderWindow,
        line_annotations: &[DiffLineAnnotation<Metadata>],
        ctx: &LayoutContext<'_>,
    ) -> FileGeometry {
        let header_region =
            get_virtual_file_header_region(&self.metrics, ctx.options.disable_file_header);
        let padding_bottom = if file_diff.hunks.is_empty() {
            0.0
        } else {
            get_virtual_file_padding_bottom(&self.metrics)
        };
        let file_annotation_height = self.cache.file_annotation_height;
        let code_region_top = header_region + file_annotation_height;
        let code_height =
            (self.height - header_region - file_annotation_height - padding_bottom).max(0.0);
        let has_file_annotations = has_file_annotations(file_diff, line_annotations);
        let file_annotation_top = file_top + header_region;
        let measured_file_annotation_visible = file_annotation_height > 0.0
            && has_file_annotations
            && file_annotation_top < bottom
            && file_annotation_top + file_annotation_height > top;
        FileGeometry {
            header_region,
            padding_bottom,
            file_annotation_height,
            code_region_top,
            code_height,
            measured_file_annotation_visible,
        }
    }
}

impl DiffLayout {
    /// `getLinePosition`
    pub fn get_line_position(
        &mut self,
        line_number: usize,
        side: SelectionSide,
        ctx: &LayoutContext<'_>,
    ) -> Option<LinePosition> {
        let file_diff = ctx.layout_diff?;
        if line_number < 1 {
            return None;
        }

        let (target_unified_index, target_split_index) =
            get_line_index_for_diff(Some(file_diff), line_number, side)?;

        let LayoutOptions {
            disable_file_header,
            collapsed,
            collapsed_context_threshold,
            diff_style,
            hunk_separators,
            ..
        } = ctx.options;
        let target_line_index = match diff_style {
            DiffStyle::Split => target_split_index,
            DiffStyle::Unified => target_unified_index,
        } as i64;
        self.approximate_layout_checkpoints(file_diff, ctx);
        let header_region = get_virtual_file_header_region(&self.metrics, disable_file_header);
        let checkpoint = self.get_layout_checkpoint_before_line_index(target_line_index);
        let mut top =
            checkpoint.map_or(header_region + self.cache.file_annotation_height, |c| c.top);

        if collapsed {
            return Some(LinePosition {
                top: get_virtual_file_header_height(&self.metrics, disable_file_header),
                height: 0.0,
            });
        }

        let separator_props = HunkSeparatorBaseProps {
            separator_type: hunk_separators,
            metrics: &self.metrics,
        };
        let mut position: Option<LinePosition> = None;
        iterate_over_diff(
            IterateOverDiffProps {
                starting_line: checkpoint.map_or(0, |checkpoint| checkpoint.rendered_line_index),
                expanded_hunks: Some(ctx.effective_expanded_hunks()),
                collapsed_context_threshold,
                ..IterateOverDiffProps::new(file_diff, iteration_style(diff_style))
            },
            |DiffLineCallbackProps {
                 hunk_index,
                 hunk,
                 collapsed_before,
                 collapsed_after,
                 deletion_line,
                 addition_line,
                 ..
             }| {
                let line_index = match diff_style {
                    DiffStyle::Split => addition_line
                        .map(|line| line.split_line_index)
                        .or(deletion_line.map(|line| line.split_line_index)),
                    DiffStyle::Unified => addition_line
                        .map(|line| line.unified_line_index)
                        .or(deletion_line.map(|line| line.unified_line_index)),
                }
                .expect("VirtualizedFileDiff.getLinePosition: missing line index data")
                    as i64;

                if collapsed_before > 0 {
                    let separator = get_leading_hunk_separator_layout(
                        separator_props,
                        hunk_index,
                        hunk.and_then(|hunk| hunk.hunk_specs.as_deref()),
                    );
                    if let Some(separator) = separator {
                        top += separator.gap_before;
                        if target_line_index >= line_index - collapsed_before as i64
                            && target_line_index < line_index
                        {
                            position = Some(LinePosition {
                                top,
                                height: separator.height,
                            });
                            return true;
                        }
                        top += separator.height + separator.gap_after;
                    }
                }

                let line_height = self.get_line_height(
                    line_index,
                    addition_line.is_some_and(|line| line.no_eofcr)
                        || deletion_line.is_some_and(|line| line.no_eofcr),
                );
                if line_index == target_line_index {
                    position = Some(LinePosition {
                        top,
                        height: line_height,
                    });
                    return true;
                }
                top += line_height;

                if collapsed_after > 0 {
                    let separator = get_trailing_hunk_separator_layout(separator_props);
                    if let Some(separator) = separator {
                        if target_line_index > line_index
                            && target_line_index <= line_index + collapsed_after as i64
                        {
                            position = Some(LinePosition {
                                top: top + separator.gap_before,
                                height: separator.height,
                            });
                            return true;
                        }
                        top += separator.total_height;
                    }
                }

                false
            },
        );

        position
    }

    /// `computeRenderRangeFromWindow`; `line_annotations` is
    /// `getLayoutAnnotations()`.
    pub fn compute_render_range_from_window<Metadata>(
        &mut self,
        file_diff: &FileDiffMetadata,
        file_top: f64,
        window: RenderWindow,
        line_annotations: &[DiffLineAnnotation<Metadata>],
        ctx: &LayoutContext<'_>,
    ) -> RenderRange {
        let RenderWindow { top, bottom } = window;
        let LayoutOptions {
            collapsed_context_threshold,
            diff_style,
            has_file_loader,
            ..
        } = ctx.options;
        let VirtualFileMetrics {
            hunk_line_count,
            line_height,
            ..
        } = self.metrics;
        let can_hydrate_partial_diff =
            super::layout::can_hydrate_collapsed_context(file_diff, has_file_loader);
        let file_height = self.height;
        let mut line_count = self.get_layout_line_count(file_diff, diff_style, ctx);

        let FileGeometry {
            header_region,
            padding_bottom,
            file_annotation_height,
            code_region_top,
            code_height,
            measured_file_annotation_visible,
        } = self.file_geometry(file_diff, file_top, window, line_annotations, ctx);

        // The range of a file with nothing to render in the window.
        let empty_range = RenderRange {
            starting_line: 0.0,
            total_lines: 0.0,
            buffer_before: 0.0,
            buffer_after: file_height - header_region - padding_bottom,
        };

        // File is outside render window
        if file_top < top - file_height || file_top > bottom {
            return empty_range;
        }

        // Whole file is under hunkLineCount, just render it all
        if line_count <= hunk_line_count || file_diff.hunks.is_empty() {
            return RenderRange {
                starting_line: 0.0,
                total_lines: hunk_line_count as f64,
                buffer_before: 0.0,
                buffer_after: 0.0,
            };
        }
        assert!(
            hunk_line_count > 0,
            "VirtualizedFileDiff.computeRenderRangeFromWindow: hunkLineCount must be positive"
        );

        self.approximate_layout_checkpoints(file_diff, ctx);
        line_count = self.get_layout_line_count(file_diff, diff_style, ctx);

        let hunk_line_count_f = hunk_line_count as f64;
        let estimated_target_lines = ((bottom - top).max(0.0) / line_height).ceil();
        let total_lines = (estimated_target_lines / hunk_line_count_f).ceil() * hunk_line_count_f
            + hunk_line_count_f;
        let total_hunks = total_lines / hunk_line_count_f;
        let overflow_hunks = total_hunks;
        // Halfway between top & bottom, represented as an absolute position
        let viewport_center = (top + bottom) / 2.0;
        // Start the scan before the viewport so we collect hunk offsets that may be
        // needed for bufferBefore. This only chooses the scan origin; the returned
        // render range is still computed from the visible window below.
        let checkpoint = self.get_layout_checkpoint_before_top(
            (top - file_top - total_lines * line_height * 2.0).max(0.0),
            Some(hunk_line_count),
        );

        let mut scan = RenderScan {
            layout: self,
            file_diff,
            options: ctx.options,
            can_hydrate_partial_diff,
            hunk_line_count,
            top,
            bottom,
            file_top,
            code_region_top,
            viewport_center,
            overflow_hunks,
            absolute_line_top: file_top + checkpoint.map_or(code_region_top, |c| c.top),
            current_line: checkpoint.map_or(0, |checkpoint| checkpoint.rendered_line_index),
            first_visible_hunk: None,
            center_hunk: None,
            overflow_counter: None,
            hunk_offsets: HunkOffsets::default(),
        };
        iterate_over_diff(
            IterateOverDiffProps {
                starting_line: checkpoint.map_or(0, |checkpoint| checkpoint.rendered_line_index),
                expanded_hunks: Some(ctx.effective_expanded_hunks()),
                collapsed_context_threshold,
                ..IterateOverDiffProps::new(file_diff, iteration_style(diff_style))
            },
            |props| scan.visit(props),
        );
        let RenderScan {
            absolute_line_top,
            first_visible_hunk,
            center_hunk,
            hunk_offsets,
            ..
        } = scan;

        // No visible lines found
        let (first_visible_hunk, center_hunk) = match first_visible_hunk {
            Some(first_visible_hunk) => (first_visible_hunk, center_hunk),
            None if measured_file_annotation_visible => (0, Some(0)),
            None => {
                return empty_range;
            }
        };

        // Calculate balanced startingLine centered around the viewport center
        // Fall back to firstVisibleHunk if center wasn't found (e.g., center in a gap)
        let center_hunk = center_hunk.unwrap_or(first_visible_hunk);
        let ideal_start_hunk = js_round(center_hunk as f64 - total_hunks / 2.0);

        // Clamp startHunk: at the beginning, reduce totalLines; at the end, shift startHunk back
        let max_start_hunk =
            ((line_count as f64 / hunk_line_count_f).ceil() - total_hunks).max(0.0);
        let start_hunk = ideal_start_hunk.min(max_start_hunk).max(0.0);
        let starting_line = start_hunk * hunk_line_count_f;

        // If we wanted to start before 0, reduce totalLines by the clamped amount
        let clamped_total_lines = if ideal_start_hunk < 0.0 {
            total_lines + ideal_start_hunk * hunk_line_count_f
        } else {
            total_lines
        };

        // Use hunkOffsets array for efficient buffer calculations
        let final_hunk_index = start_hunk + clamped_total_lines / hunk_line_count_f;
        let buffer_before = if starting_line == 0.0 {
            0.0
        } else {
            file_annotation_height + hunk_offsets.get(start_hunk as usize).unwrap_or(0.0)
        };
        // Calculate bufferAfter using hunkOffset if available, otherwise use cumulative height
        let buffer_after = hunk_offsets.buffer_after(
            final_hunk_index,
            code_height,
            // We stopped early, calculate from current position
            absolute_line_top - file_top - code_region_top,
        );

        RenderRange {
            starting_line,
            total_lines: clamped_total_lines,
            buffer_before,
            buffer_after,
        }
    }
}
