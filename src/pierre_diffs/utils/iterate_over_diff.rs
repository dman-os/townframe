// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/iterateOverDiff.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: the callback returns `bool` (`true` stops iteration; JavaScript's
// `boolean | void`). The `DiffLineCallbackProps` union is one struct with
// `Option` deletion/addition lines. `Map | true | undefined` expansion state is
// `Option<&ExpandedHunks>`; `totalLines = Infinity` is `total_lines: None`.
// The 330-line function body is split into methods of `Walker` (the closure
// state of upstream's `IterationState`) and helpers; the control flow and the
// order of count updates are the upstream's. Invalid hunk indexes upstream
// `throw`; here they are unreachable by construction (`diff.hunks[index]`).

use crate::constants::DEFAULT_COLLAPSED_CONTEXT_THRESHOLD;
use crate::types::lines::ExpandedHunks;
use crate::types::patch::ChangeContent;
use crate::types::patch::FileDiffMetadata;
use crate::types::patch::Hunk;
use crate::types::patch::HunkContent;
use crate::types::patch::line_slot;
use crate::utils::get_hunk_side_boundaries::get_hunk_side_start_boundary;
use crate::utils::virtual_diff_layout::ExpandedRegionResult;
use crate::utils::virtual_diff_layout::GetExpandedRegionProps;
use crate::utils::virtual_diff_layout::GetTrailingExpandedRegionProps;
use crate::utils::virtual_diff_layout::get_expanded_region;
use crate::utils::virtual_diff_layout::get_trailing_expanded_region;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiffLineMetadata {
    pub unified_line_index: usize,
    pub split_line_index: usize,
    pub line_index: usize,
    pub line_number: usize,
    pub no_eofcr: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiffLineType {
    Change,
    Context,
    ContextExpanded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiffLineCallbackProps<'diff> {
    pub hunk_index: usize,
    /// `None` for the trailing expansion region.
    pub hunk: Option<&'diff Hunk>,
    /// > 0 means separator before this line, value = hidden lines
    pub collapsed_before: usize,
    /// > 0 only on final line if trailing collapsed content
    pub collapsed_after: usize,
    pub line_type: DiffLineType,
    /// Present for context and deletion rows.
    pub deletion_line: Option<DiffLineMetadata>,
    /// Present for context and addition rows.
    pub addition_line: Option<DiffLineMetadata>,
}

/// The local `DiffStyle = 'unified' | 'split' | 'both'` of `iterateOverDiff.ts`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IterationDiffStyle {
    Unified,
    Split,
    Both,
}

#[derive(Debug, Clone, Copy)]
pub struct IterateOverDiffProps<'diff> {
    pub diff: &'diff FileDiffMetadata,
    pub diff_style: IterationDiffStyle,
    pub starting_line: usize,
    /// `None` is upstream's default `Infinity`.
    pub total_lines: Option<usize>,
    pub expanded_hunks: Option<&'diff ExpandedHunks>,
    pub collapsed_context_threshold: usize,
}

impl<'diff> IterateOverDiffProps<'diff> {
    /// Upstream's defaults: `startingLine = 0`, `totalLines = Infinity`, no
    /// expansion, `DEFAULT_COLLAPSED_CONTEXT_THRESHOLD`.
    pub fn new(diff: &'diff FileDiffMetadata, diff_style: IterationDiffStyle) -> Self {
        Self {
            diff,
            diff_style,
            starting_line: 0,
            total_lines: None,
            expanded_hunks: None,
            collapsed_context_threshold: DEFAULT_COLLAPSED_CONTEXT_THRESHOLD,
        }
    }
}

type LineIterationBounds = (usize, usize);

/// Upstream's `IterationState` plus the callback.
struct Walker<'diff, Callback> {
    diff_style: IterationDiffStyle,
    starting_line: usize,
    viewport_start: usize,
    /// `None` when `totalLines` is infinite.
    viewport_end: Option<usize>,
    is_windowed_highlight: bool,
    split_count: usize,
    unified_count: usize,
    callback: Callback,
    marker: std::marker::PhantomData<&'diff ()>,
}

/// Per-hunk values shared by the closures of one hunk iteration.
struct HunkContext<'diff> {
    hunk_index: usize,
    hunk: &'diff Hunk,
    leading_region: ExpandedRegionResult,
    trailing_region: Option<ExpandedRegionResult>,
    consumed_collapsed: bool,
}

/// Running line indexes and numbers inside one hunk.
#[derive(Clone, Copy)]
struct Cursor {
    unified_line_index: usize,
    split_line_index: usize,
    deletion_line_index: usize,
    addition_line_index: usize,
    deletion_line_number: usize,
    addition_line_number: usize,
}

impl HunkContext<'_> {
    fn consume_pending_collapsed(&mut self) -> usize {
        if self.consumed_collapsed {
            return 0;
        }
        self.consumed_collapsed = true;
        self.leading_region.collapsed_lines
    }

    fn trailing_collapsed_after(
        &self,
        diff_style: IterationDiffStyle,
        unified_line_index: usize,
        split_line_index: usize,
    ) -> usize {
        let Some(trailing_region) = self.trailing_region else {
            return 0;
        };
        if trailing_region.collapsed_lines == 0
            || trailing_region.from_start + trailing_region.from_end > 0
        {
            return 0;
        }
        let is_last_row = if diff_style == IterationDiffStyle::Unified {
            unified_line_index + 1 == self.hunk.unified_line_start + self.hunk.unified_line_count
        } else {
            split_line_index + 1 == self.hunk.split_line_start + self.hunk.split_line_count
        };
        if is_last_row {
            trailing_region.collapsed_lines
        } else {
            0
        }
    }
}

pub fn iterate_over_diff<'diff>(
    props: IterateOverDiffProps<'diff>,
    callback: impl FnMut(DiffLineCallbackProps<'diff>) -> bool,
) {
    let IterateOverDiffProps {
        diff,
        diff_style,
        starting_line,
        total_lines,
        expanded_hunks,
        collapsed_context_threshold,
    } = props;
    let iteration_start = get_iteration_start_state(
        diff,
        diff_style,
        starting_line,
        expanded_hunks,
        collapsed_context_threshold,
    );
    let mut walker = Walker {
        diff_style,
        starting_line,
        viewport_start: starting_line,
        viewport_end: total_lines.map(|total| starting_line + total),
        is_windowed_highlight: starting_line > 0 || total_lines.is_some(),
        split_count: iteration_start.split_count,
        unified_count: iteration_start.unified_count,
        callback,
        marker: std::marker::PhantomData,
    };
    let final_hunk_index = diff.hunks.len().wrapping_sub(1);

    for hunk_index in iteration_start.hunk_index..diff.hunks.len() {
        let hunk = &diff.hunks[hunk_index];
        if walker.should_break() {
            break;
        }
        let leading_region = get_expanded_region(GetExpandedRegionProps {
            is_partial: diff.is_partial,
            range_size: hunk.collapsed_before,
            expanded_hunks,
            hunk_index,
            collapsed_context_threshold,
        });
        let trailing_region = if hunk_index == final_hunk_index {
            get_trailing_expanded_region(GetTrailingExpandedRegionProps {
                file_diff: diff,
                hunk_index,
                expanded_hunks,
                collapsed_context_threshold,
                error_prefix: "iterateOverDiff",
            })
        } else {
            None
        };
        let mut context = HunkContext {
            hunk_index,
            hunk,
            leading_region,
            trailing_region,
            consumed_collapsed: leading_region.collapsed_lines == 0,
        };
        if walker.iterate_hunk(diff, &mut context) {
            break;
        }
    }
}

impl<'diff, Callback: FnMut(DiffLineCallbackProps<'diff>) -> bool> Walker<'diff, Callback> {
    fn should_break(&self) -> bool {
        if !self.is_windowed_highlight {
            return false;
        }
        let end = self.viewport_end;
        let break_unified = end.is_some_and(|end| self.unified_count >= end);
        let break_split = end.is_some_and(|end| self.split_count >= end);
        match self.diff_style {
            IterationDiffStyle::Unified => break_unified,
            IterationDiffStyle::Split => break_split,
            IterationDiffStyle::Both => break_unified && break_split,
        }
    }

    fn should_skip(&self, unified_count: usize, split_count: usize) -> bool {
        if !self.is_windowed_highlight {
            return false;
        }
        let skip_unified =
            unified_count > 0 && self.unified_count + unified_count <= self.starting_line;
        let skip_split = split_count > 0 && self.split_count + split_count <= self.starting_line;
        match self.diff_style {
            IterationDiffStyle::Unified => skip_unified,
            IterationDiffStyle::Split => skip_split,
            IterationDiffStyle::Both => skip_unified && skip_split,
        }
    }

    fn increment_counts(&mut self, unified_value: usize, split_value: usize) {
        if matches!(
            self.diff_style,
            IterationDiffStyle::Unified | IterationDiffStyle::Both
        ) {
            self.unified_count += unified_value;
        }
        if matches!(
            self.diff_style,
            IterationDiffStyle::Split | IterationDiffStyle::Both
        ) {
            self.split_count += split_value;
        }
    }

    fn is_in_unified_window(&self, unified_height: usize) -> bool {
        !self.is_windowed_highlight
            || (self.unified_count + unified_height >= self.starting_line
                && self.viewport_end.is_none_or(|end| self.unified_count < end))
    }

    fn is_in_split_window(&self, split_height: usize) -> bool {
        !self.is_windowed_highlight
            || (self.split_count + split_height >= self.starting_line
                && self.viewport_end.is_none_or(|end| self.split_count < end))
    }

    fn is_in_window(&self, unified_height: usize, split_height: usize) -> bool {
        if !self.is_windowed_highlight {
            return true;
        }
        let unified_in_window = self.is_in_unified_window(unified_height);
        let split_in_window = self.is_in_split_window(split_height);
        match self.diff_style {
            IterationDiffStyle::Unified => unified_in_window,
            IterationDiffStyle::Split => split_in_window,
            IterationDiffStyle::Both => unified_in_window || split_in_window,
        }
    }

    /// Counts the row (unless `silent`) and runs the callback; true stops
    /// iteration.
    fn emit(&mut self, props: DiffLineCallbackProps<'diff>, silent: bool) -> bool {
        if !silent {
            match self.diff_style {
                IterationDiffStyle::Unified => self.increment_counts(1, 0),
                IterationDiffStyle::Split => self.increment_counts(0, 1),
                IterationDiffStyle::Both => self.increment_counts(1, 1),
            }
        }
        (self.callback)(props)
    }

    // Clip a run of context rows to a single bounded hull around the active rendered
    // window. `diffStyle: both` can make split and unified rows visible in disjoint
    // ranges, so these bounds may include interior gaps that still need per-row
    // filtering before emitting.
    fn context_line_iteration_bounds(&self, count: usize) -> LineIterationBounds {
        if !self.is_windowed_highlight || count == 0 {
            return (0, count);
        }
        let range_for = |current_count: usize| {
            let start = self.viewport_start.saturating_sub(current_count);
            let end = self
                .viewport_end
                .map_or(count, |end| count.min(end.saturating_sub(current_count)));
            (end > start).then_some((start, end))
        };
        let mut ranges: Vec<LineIterationBounds> = Vec::new();
        if self.diff_style != IterationDiffStyle::Split {
            ranges.extend(range_for(self.unified_count));
        }
        if self.diff_style != IterationDiffStyle::Unified {
            ranges.extend(range_for(self.split_count));
        }
        let Some(&(first_start, first_end)) = ranges.first() else {
            return (0, 0);
        };
        ranges.iter().skip(1).fold(
            (first_start, first_end),
            |(start, end), &(range_start, range_end)| (start.min(range_start), end.max(range_end)),
        )
    }

    // Walk context rows through the active window while keeping split and
    // unified counters aligned. The callback only runs after the final per-row
    // window check, which keeps `diffStyle: both` gap rows from being emitted.
    // `consume_collapsed_on_skip` is upstream's `onSkippedStart` (always
    // `consumePendingCollapsed()` where it is given); `break_per_row` is its
    // `shouldBreak` (always `state.shouldBreak()` where given).
    fn walk_context_lines(
        &mut self,
        context: &mut HunkContext<'diff>,
        count: usize,
        consume_collapsed_on_skip: bool,
        break_per_row: bool,
        mut make_props: impl FnMut(&mut HunkContext<'diff>, usize) -> DiffLineCallbackProps<'diff>,
    ) -> bool {
        let (start_index, end_index) = self.context_line_iteration_bounds(count);
        if start_index > 0 {
            self.increment_counts(start_index, start_index);
            if consume_collapsed_on_skip {
                context.consume_pending_collapsed();
            }
        }

        let mut index = start_index;
        while index < count {
            if break_per_row && self.should_break() {
                return true;
            }
            if index >= end_index {
                self.increment_counts(count - index, count - index);
                break;
            }
            if self.is_in_window(0, 0) {
                let props = make_props(context, index);
                if self.emit(props, false) {
                    return true;
                }
            } else {
                self.increment_counts(1, 1);
            }
            index += 1;
        }
        false
    }

    /// Context row of an expanded (collapsed-gap) slice.
    fn expanded_context_props(
        context: &mut HunkContext<'diff>,
        cursor: Cursor,
        index: usize,
        with_collapsed_before: bool,
    ) -> DiffLineCallbackProps<'diff> {
        DiffLineCallbackProps {
            hunk_index: context.hunk_index,
            hunk: Some(context.hunk),
            collapsed_before: if with_collapsed_before {
                context.consume_pending_collapsed()
            } else {
                0
            },
            collapsed_after: 0,
            line_type: DiffLineType::ContextExpanded,
            deletion_line: Some(DiffLineMetadata {
                line_number: cursor.deletion_line_number + index,
                line_index: cursor.deletion_line_index + index,
                no_eofcr: false,
                unified_line_index: cursor.unified_line_index + index,
                split_line_index: cursor.split_line_index + index,
            }),
            addition_line: Some(DiffLineMetadata {
                unified_line_index: cursor.unified_line_index + index,
                split_line_index: cursor.split_line_index + index,
                line_index: cursor.addition_line_index + index,
                line_number: cursor.addition_line_number + index,
                no_eofcr: false,
            }),
        }
    }

    /// Iterates one hunk; true stops the whole iteration.
    fn iterate_hunk(
        &mut self,
        diff: &'diff FileDiffMetadata,
        context: &mut HunkContext<'diff>,
    ) -> bool {
        let hunk = context.hunk;
        let deletion_boundary =
            get_hunk_side_start_boundary(hunk.deletion_start, hunk.deletion_count);
        let addition_boundary =
            get_hunk_side_start_boundary(hunk.addition_start, hunk.addition_count);
        let deletion_start_index = if !diff.is_partial && hunk.deletion_count == 0 {
            deletion_boundary
        } else {
            line_slot(hunk.deletion_line_index)
        };
        let addition_start_index = if !diff.is_partial && hunk.addition_count == 0 {
            addition_boundary
        } else {
            line_slot(hunk.addition_line_index)
        };

        if self.walk_leading_region(
            context,
            deletion_boundary,
            addition_boundary,
            deletion_start_index,
            addition_start_index,
        ) {
            return true;
        }

        let mut cursor = Cursor {
            unified_line_index: hunk.unified_line_start,
            split_line_index: hunk.split_line_start,
            deletion_line_index: deletion_start_index,
            addition_line_index: addition_start_index,
            deletion_line_number: deletion_boundary + 1,
            addition_line_number: addition_boundary + 1,
        };
        let last_index = hunk.hunk_content.len().wrapping_sub(1);
        for (content_index, content) in hunk.hunk_content.iter().enumerate() {
            if self.should_break() {
                return true;
            }
            let is_last_content = content_index == last_index;
            let stop = match content {
                HunkContent::Context(content) => {
                    let stop =
                        self.walk_hunk_context(context, content.lines, is_last_content, cursor);
                    cursor.unified_line_index += content.lines;
                    cursor.split_line_index += content.lines;
                    cursor.deletion_line_index += content.lines;
                    cursor.addition_line_index += content.lines;
                    cursor.deletion_line_number += content.lines;
                    cursor.addition_line_number += content.lines;
                    stop
                }
                HunkContent::Change(content) => {
                    let stop = self.walk_hunk_change(context, content, is_last_content, cursor);
                    cursor.unified_line_index += content.deletions + content.additions;
                    cursor.split_line_index += content.deletions.max(content.additions);
                    cursor.deletion_line_index += content.deletions;
                    cursor.addition_line_index += content.additions;
                    cursor.deletion_line_number += content.deletions;
                    cursor.addition_line_number += content.additions;
                    stop
                }
            };
            if stop {
                return true;
            }
        }

        self.walk_trailing_region(diff, context, cursor)
    }

    /// The expanded slices before the hunk (the collapsed gap's edges).
    fn walk_leading_region(
        &mut self,
        context: &mut HunkContext<'diff>,
        deletion_boundary: usize,
        addition_boundary: usize,
        deletion_start_index: usize,
        addition_start_index: usize,
    ) -> bool {
        let hunk = context.hunk;
        let leading_region = context.leading_region;
        let expanded_line_count = leading_region.from_start + leading_region.from_end;
        // Emit for expanded lines
        if self.should_skip(expanded_line_count, expanded_line_count) {
            self.increment_counts(expanded_line_count, expanded_line_count);
            context.consume_pending_collapsed();
            return false;
        }
        let from_start = Cursor {
            unified_line_index: hunk
                .unified_line_start
                .saturating_sub(leading_region.range_size),
            split_line_index: hunk
                .split_line_start
                .saturating_sub(leading_region.range_size),
            deletion_line_index: deletion_start_index.saturating_sub(leading_region.range_size),
            addition_line_index: addition_start_index.saturating_sub(leading_region.range_size),
            deletion_line_number: (deletion_boundary + 1).saturating_sub(leading_region.range_size),
            addition_line_number: (addition_boundary + 1).saturating_sub(leading_region.range_size),
        };
        if self.walk_context_lines(
            context,
            leading_region.from_start,
            false,
            false,
            |context, index| Self::expanded_context_props(context, from_start, index, false),
        ) {
            return true;
        }

        let from_end = Cursor {
            unified_line_index: hunk
                .unified_line_start
                .saturating_sub(leading_region.from_end),
            split_line_index: hunk
                .split_line_start
                .saturating_sub(leading_region.from_end),
            deletion_line_index: deletion_start_index.saturating_sub(leading_region.from_end),
            addition_line_index: addition_start_index.saturating_sub(leading_region.from_end),
            deletion_line_number: (deletion_boundary + 1).saturating_sub(leading_region.from_end),
            addition_line_number: (addition_boundary + 1).saturating_sub(leading_region.from_end),
        };
        // The collapsed separator belongs before this fromEnd slice. If the
        // render window starts inside the slice, consume it with the skipped
        // rows so it is not attached to the first emitted row.
        self.walk_context_lines(
            context,
            leading_region.from_end,
            true,
            false,
            |context, index| Self::expanded_context_props(context, from_end, index, true),
        )
    }

    /// A `context` content block of the hunk.
    fn walk_hunk_context(
        &mut self,
        context: &mut HunkContext<'diff>,
        lines: usize,
        is_last_content: bool,
        cursor: Cursor,
    ) -> bool {
        if self.should_skip(lines, lines) {
            self.increment_counts(lines, lines);
            context.consume_pending_collapsed();
            return false;
        }
        let diff_style = self.diff_style;
        // When windowing starts inside context content, the leading
        // separator was above the visible range and should not be
        // emitted on the first rendered context line.
        self.walk_context_lines(context, lines, true, false, |context, index| {
            let hunk = context.hunk;
            let is_last_line = is_last_content && index == lines - 1;
            let unified_row_index = cursor.unified_line_index + index;
            let split_row_index = cursor.split_line_index + index;
            DiffLineCallbackProps {
                hunk_index: context.hunk_index,
                hunk: Some(hunk),
                collapsed_before: context.consume_pending_collapsed(),
                collapsed_after: context.trailing_collapsed_after(
                    diff_style,
                    unified_row_index,
                    split_row_index,
                ),
                line_type: DiffLineType::Context,
                deletion_line: Some(DiffLineMetadata {
                    line_number: cursor.deletion_line_number + index,
                    line_index: cursor.deletion_line_index + index,
                    no_eofcr: is_last_line && hunk.no_eofcr_deletions,
                    unified_line_index: unified_row_index,
                    split_line_index: split_row_index,
                }),
                addition_line: Some(DiffLineMetadata {
                    unified_line_index: unified_row_index,
                    split_line_index: split_row_index,
                    line_index: cursor.addition_line_index + index,
                    line_number: cursor.addition_line_number + index,
                    no_eofcr: is_last_line && hunk.no_eofcr_additions,
                }),
            }
        })
    }

    /// A `change` content block of the hunk.
    fn walk_hunk_change(
        &mut self,
        context: &mut HunkContext<'diff>,
        content: &ChangeContent,
        is_last_content: bool,
        cursor: Cursor,
    ) -> bool {
        let split_count = content.deletions.max(content.additions);
        let unified_count = content.deletions + content.additions;
        if !self.should_skip(unified_count, split_count) {
            let iteration_ranges = self.change_iteration_ranges(content);
            let first_range_start = iteration_ranges.first().map_or(0, |range| range.0);
            if first_range_start > 0 {
                // Change rows can be windowed from the middle of the block too. In
                // that case the leading separator belongs to skipped rows, not to
                // the first visible deletion/addition row.
                context.consume_pending_collapsed();
            }
            // Change ranges are already clipped to the active window. Counts move
            // once for the whole change block after the selected rows emit.
            for (range_start, range_end) in iteration_ranges {
                for index in range_start..range_end {
                    let unified_row_index = cursor.unified_line_index + index;
                    let split_row_index = if self.diff_style == IterationDiffStyle::Unified {
                        cursor.split_line_index
                            + if index < content.deletions {
                                index
                            } else {
                                index - content.deletions
                            }
                    } else {
                        cursor.split_line_index + index
                    };
                    let collapsed_after = context.trailing_collapsed_after(
                        self.diff_style,
                        unified_row_index,
                        split_row_index,
                    );
                    let props = change_line_data(&ChangeLineDataProps {
                        hunk_index: context.hunk_index,
                        hunk: context.hunk,
                        collapsed_before: context.consume_pending_collapsed(),
                        collapsed_after,
                        diff_style: self.diff_style,
                        index,
                        cursor,
                        content,
                        is_last_content,
                        unified_count,
                        split_count,
                    });
                    if self.emit(props, true) {
                        return true;
                    }
                }
            }
        }

        context.consume_pending_collapsed();
        self.increment_counts(unified_count, split_count);
        false
    }

    /// Expanded rows after the final hunk (the synthetic trailing region).
    fn walk_trailing_region(
        &mut self,
        diff: &'diff FileDiffMetadata,
        context: &mut HunkContext<'diff>,
        cursor: Cursor,
    ) -> bool {
        let Some(trailing_region) = context.trailing_region else {
            return false;
        };
        let ExpandedRegionResult {
            collapsed_lines,
            from_start,
            from_end,
            ..
        } = trailing_region;
        let length = from_start + from_end;
        self.walk_context_lines(context, length, false, true, |_, index| {
            let is_last_line = index == length - 1;
            DiffLineCallbackProps {
                hunk_index: diff.hunks.len(),
                hunk: None,
                collapsed_before: 0,
                collapsed_after: if is_last_line { collapsed_lines } else { 0 },
                line_type: DiffLineType::ContextExpanded,
                deletion_line: Some(DiffLineMetadata {
                    line_number: cursor.deletion_line_number + index,
                    line_index: cursor.deletion_line_index + index,
                    no_eofcr: false,
                    unified_line_index: cursor.unified_line_index + index,
                    split_line_index: cursor.split_line_index + index,
                }),
                addition_line: Some(DiffLineMetadata {
                    unified_line_index: cursor.unified_line_index + index,
                    split_line_index: cursor.split_line_index + index,
                    line_index: cursor.addition_line_index + index,
                    line_number: cursor.addition_line_number + index,
                    no_eofcr: false,
                }),
            }
        })
    }

    // Clip a change block to the rows that can be visible in the active coordinate
    // space. `diffStyle: both` iterates in split row space, but includes the unified
    // ranges too so either view can render the visible change rows it needs.
    fn change_iteration_ranges(&self, content: &ChangeContent) -> Vec<LineIterationBounds> {
        // If not a window highlight, then we should just render the entire range
        if !self.is_windowed_highlight {
            return vec![(
                0,
                if self.diff_style == IterationDiffStyle::Unified {
                    content.deletions + content.additions
                } else {
                    content.deletions.max(content.additions)
                },
            )];
        }
        let use_unified = self.diff_style != IterationDiffStyle::Split;
        let use_split = self.diff_style != IterationDiffStyle::Unified;
        let unified_space = self.diff_style == IterationDiffStyle::Unified;
        let mut iteration_ranges: Vec<LineIterationBounds> = Vec::new();
        let visible_range = |start: usize, count: usize| -> Option<LineIterationBounds> {
            let end = start + count;
            if end <= self.viewport_start
                || self
                    .viewport_end
                    .is_some_and(|window_end| start >= window_end)
            {
                return None;
            }
            let visible_start = self.viewport_start.saturating_sub(start);
            let visible_end = self
                .viewport_end
                .map_or(count, |window_end| count.min(window_end - start));
            (visible_end > visible_start).then_some((visible_start, visible_end))
        };
        let mut push_range = |range: Option<LineIterationBounds>, is_addition: bool| {
            let Some((range_start, range_end)) = range else {
                return;
            };
            // For split iteration, additions/deletions are already in split row space.
            let (start, end) = if !unified_space || !is_addition {
                (range_start, range_end)
            } else {
                (
                    range_start + content.deletions,
                    range_end + content.deletions,
                )
            };
            if end > start {
                iteration_ranges.push((start, end));
            }
        };

        if use_unified {
            push_range(visible_range(self.unified_count, content.deletions), false);
            push_range(
                visible_range(self.unified_count + content.deletions, content.additions),
                true,
            );
        }
        if use_split {
            push_range(visible_range(self.split_count, content.deletions), false);
            push_range(visible_range(self.split_count, content.additions), true);
        }

        if iteration_ranges.is_empty() {
            return iteration_ranges;
        }

        iteration_ranges.sort_by_key(|range| range.0);
        let mut merged: Vec<LineIterationBounds> = vec![iteration_ranges[0]];
        for &(start, end) in &iteration_ranges[1..] {
            let last = merged.last_mut().expect("merged starts non-empty");
            if start <= last.1 {
                last.1 = last.1.max(end);
            } else {
                merged.push((start, end));
            }
        }
        merged
    }
}

struct ChangeLineDataProps<'diff, 'content> {
    hunk_index: usize,
    hunk: &'diff Hunk,
    collapsed_before: usize,
    collapsed_after: usize,
    diff_style: IterationDiffStyle,
    index: usize,
    cursor: Cursor,
    content: &'content ChangeContent,
    is_last_content: bool,
    unified_count: usize,
    split_count: usize,
}

// Build the callback payload for one change row, mapping the selected row index
// into split/unified coordinates and addition/deletion line metadata.
fn change_line_data<'diff>(props: &ChangeLineDataProps<'diff, '_>) -> DiffLineCallbackProps<'diff> {
    let ChangeLineDataProps {
        hunk_index,
        hunk,
        collapsed_before,
        collapsed_after,
        diff_style,
        index,
        cursor,
        content,
        is_last_content,
        unified_count,
        split_count,
    } = *props;
    let is_unified = diff_style == IterationDiffStyle::Unified;
    let unified_deletion_line_index =
        (index < content.deletions).then(|| cursor.unified_line_index + index);
    let unified_addition_line_index = if is_unified {
        (index >= content.deletions).then(|| cursor.unified_line_index + index)
    } else {
        (index < content.additions).then(|| cursor.unified_line_index + content.deletions + index)
    };

    let resolved_split_line_index = if is_unified {
        cursor.split_line_index
            + if index < content.deletions {
                index
            } else {
                index - content.deletions
            }
    } else {
        cursor.split_line_index + index
    };

    let deletion_line_index_value =
        (index < content.deletions).then(|| cursor.deletion_line_index + index);
    let deletion_line_number_value =
        (index < content.deletions).then(|| cursor.deletion_line_number + index);
    let addition_offset = if is_unified {
        (index >= content.deletions).then(|| index - content.deletions)
    } else {
        (index < content.additions).then_some(index)
    };
    let addition_line_index_value =
        addition_offset.map(|offset| cursor.addition_line_index + offset);
    let addition_line_number_value =
        addition_offset.map(|offset| cursor.addition_line_number + offset);

    let no_eofcr_deletion = if is_unified {
        is_last_content && index + 1 == content.deletions && hunk.no_eofcr_deletions
    } else {
        is_last_content && index + 1 == split_count && hunk.no_eofcr_deletions
    };
    let no_eofcr_addition = if is_unified {
        is_last_content && index + 1 == unified_count && hunk.no_eofcr_additions
    } else {
        is_last_content && index + 1 == split_count && hunk.no_eofcr_additions
    };

    let deletion_line = match (
        deletion_line_index_value,
        deletion_line_number_value,
        unified_deletion_line_index,
    ) {
        (Some(line_index), Some(line_number), Some(unified_line_index)) => Some(DiffLineMetadata {
            line_number,
            line_index,
            no_eofcr: no_eofcr_deletion,
            unified_line_index,
            split_line_index: resolved_split_line_index,
        }),
        _ => None,
    };
    let addition_line = match (
        addition_line_index_value,
        addition_line_number_value,
        unified_addition_line_index,
    ) {
        (Some(line_index), Some(line_number), Some(unified_line_index)) => Some(DiffLineMetadata {
            unified_line_index,
            split_line_index: resolved_split_line_index,
            line_index,
            line_number,
            no_eofcr: no_eofcr_addition,
        }),
        _ => None,
    };
    assert!(
        deletion_line.is_some() || addition_line.is_some(),
        "iterateOverDiff: missing change line data"
    );

    DiffLineCallbackProps {
        hunk_index,
        hunk: Some(hunk),
        collapsed_before,
        collapsed_after,
        line_type: DiffLineType::Change,
        deletion_line,
        addition_line,
    }
}

struct IterationStartState {
    hunk_index: usize,
    split_count: usize,
    unified_count: usize,
}

#[derive(Clone, Copy)]
struct HunkPrefixCounts {
    split_count: usize,
    unified_count: usize,
}

// Seek the iterator to the hunk that contains `startingLine` without changing
// the public meaning of `startingLine`: it is a dense rendered-row index, not
// a raw split/unified line index. We first build prefix counts for each hunk
// under the current expansion/collapse settings, binary-search those counts to
// find the first hunk whose rendered rows cross `startingLine`, then seed the
// running split/unified counters as if every prior hunk had already been
// walked.
fn get_iteration_start_state(
    diff: &FileDiffMetadata,
    diff_style: IterationDiffStyle,
    starting_line: usize,
    expanded_hunks: Option<&ExpandedHunks>,
    collapsed_context_threshold: usize,
) -> IterationStartState {
    if starting_line == 0 || diff_style == IterationDiffStyle::Both {
        return IterationStartState {
            hunk_index: 0,
            split_count: 0,
            unified_count: 0,
        };
    }

    let prefix_counts = get_hunk_prefix_counts(diff, expanded_hunks, collapsed_context_threshold);

    // First hunk whose cumulative count (after it) exceeds `starting_line`.
    let selected_count = |counts: HunkPrefixCounts| {
        if diff_style == IterationDiffStyle::Unified {
            counts.unified_count
        } else {
            counts.split_count
        }
    };
    let result =
        prefix_counts[1..].partition_point(|&counts| selected_count(counts) <= starting_line);
    let counts = prefix_counts[result];
    IterationStartState {
        hunk_index: result,
        split_count: counts.split_count,
        unified_count: counts.unified_count,
    }
}

// Build cumulative rendered-row counts at every hunk boundary for the current
// expansion state. Entry 0 is always zero rows before the first hunk; entry N
// is the split/unified row count after hunks [0, N). These counts let
// getIterationStartState binary-search by dense rendered row without replaying
// every prior hunk.
fn get_hunk_prefix_counts(
    diff: &FileDiffMetadata,
    expanded_hunks: Option<&ExpandedHunks>,
    collapsed_context_threshold: usize,
) -> Vec<HunkPrefixCounts> {
    let mut split_count = 0;
    let mut unified_count = 0;
    let final_hunk_index = diff.hunks.len().wrapping_sub(1);
    let mut prefix_counts = vec![HunkPrefixCounts {
        split_count: 0,
        unified_count: 0,
    }];

    for (index, hunk) in diff.hunks.iter().enumerate() {
        let leading_region = get_expanded_region(GetExpandedRegionProps {
            is_partial: diff.is_partial,
            range_size: hunk.collapsed_before,
            expanded_hunks,
            hunk_index: index,
            collapsed_context_threshold,
        });
        let leading_count = leading_region.from_start + leading_region.from_end;
        split_count += leading_count + hunk.split_line_count;
        unified_count += leading_count + hunk.unified_line_count;

        let trailing_region = if index == final_hunk_index {
            get_trailing_expanded_region(GetTrailingExpandedRegionProps {
                file_diff: diff,
                hunk_index: index,
                expanded_hunks,
                collapsed_context_threshold,
                error_prefix: "iterateOverDiff",
            })
        } else {
            None
        };
        if let Some(trailing_region) = trailing_region {
            let trailing_count = trailing_region.from_start + trailing_region.from_end;
            split_count += trailing_count;
            unified_count += trailing_count;
        }

        prefix_counts.push(HunkPrefixCounts {
            split_count,
            unified_count,
        });
    }

    prefix_counts
}
