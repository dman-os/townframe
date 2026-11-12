// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/virtualDiffLayout.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: the `Map<number, HunkExpansionRegion> | true | undefined` parameter
// is `Option<&ExpandedHunks>`. `errorPrefix` + `throw` for a trailing-context
// mismatch is a panic with the same message: it means the diff's line arrays
// disagree with its hunks, an invariant of a well-formed `FileDiffMetadata`.
// Positions are `usize`; every subtraction upstream relies on being
// non-negative is guarded where it could otherwise go below zero.

use crate::types::layout::VirtualFileMetrics;
use crate::types::lines::ExpandedHunks;
use crate::types::lines::HunkExpansionRegion;
use crate::types::options::HunkSeparators;
use crate::types::patch::FileDiffMetadata;
use crate::types::patch::Hunk;
use crate::utils::compute_virtual_file_metrics::get_default_hunk_separator_height;
use crate::utils::get_hunk_side_boundaries::get_hunk_side_end_boundary;
use crate::utils::get_hunk_side_boundaries::get_hunk_side_start_boundary;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExpandedRegionResult {
    pub from_start: usize,
    pub from_end: usize,
    pub range_size: usize,
    pub collapsed_lines: usize,
    pub render_all: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct GetExpandedRegionProps<'expanded> {
    pub is_partial: bool,
    pub range_size: usize,
    pub expanded_hunks: Option<&'expanded ExpandedHunks<'expanded>>,
    pub hunk_index: usize,
    pub collapsed_context_threshold: usize,
}

/// One-based new-file line range covered by a hunk, as `[start, end)`.
pub fn get_hunk_addition_line_range(hunk: &Hunk) -> (usize, usize) {
    (
        get_hunk_side_start_boundary(hunk.addition_start, hunk.addition_count) + 1,
        get_hunk_side_end_boundary(hunk.addition_start, hunk.addition_count) + 1,
    )
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HunkSeparatorLayout {
    pub height: f64,
    pub gap_before: f64,
    pub gap_after: f64,
    pub total_height: f64,
}

fn region_of(
    expanded_hunks: Option<&ExpandedHunks>,
    hunk_index: usize,
) -> Option<HunkExpansionRegion> {
    match expanded_hunks {
        Some(ExpandedHunks::Regions(regions)) => regions.get(&hunk_index).copied(),
        Some(ExpandedHunks::All) | None => None,
    }
}

// Converts a collapsed unchanged range into the slices that should render near
// the start and end of that range for the active hunk expansion state.
pub fn get_expanded_region(props: GetExpandedRegionProps<'_>) -> ExpandedRegionResult {
    let GetExpandedRegionProps {
        is_partial,
        range_size: normalized_range_size,
        expanded_hunks,
        hunk_index,
        collapsed_context_threshold,
    } = props;
    if normalized_range_size == 0 || is_partial {
        return ExpandedRegionResult {
            from_start: 0,
            from_end: 0,
            range_size: normalized_range_size,
            collapsed_lines: normalized_range_size,
            render_all: false,
        };
    }

    if matches!(expanded_hunks, Some(ExpandedHunks::All))
        || normalized_range_size <= collapsed_context_threshold
    {
        return ExpandedRegionResult {
            from_start: normalized_range_size,
            from_end: 0,
            range_size: normalized_range_size,
            collapsed_lines: 0,
            render_all: true,
        };
    }

    let region = region_of(expanded_hunks, hunk_index);
    let from_start = region
        .map_or(0, |region| region.from_start)
        .min(normalized_range_size);
    let from_end = region
        .map_or(0, |region| region.from_end)
        .min(normalized_range_size);
    let expanded_count = from_start + from_end;
    let render_all = expanded_count >= normalized_range_size;
    ExpandedRegionResult {
        from_start: if render_all {
            normalized_range_size
        } else {
            from_start
        },
        from_end: if render_all { 0 } else { from_end },
        range_size: normalized_range_size,
        collapsed_lines: normalized_range_size.saturating_sub(expanded_count),
        render_all,
    }
}

fn trailing_remaining(file_diff: &FileDiffMetadata) -> Option<(isize, isize)> {
    let last_hunk = file_diff.hunks.last()?;
    if file_diff.is_partial
        || file_diff.addition_lines.is_empty()
        || file_diff.deletion_lines.is_empty()
    {
        return None;
    }
    let addition_remaining = file_diff.addition_lines.len() as isize
        - get_hunk_side_end_boundary(last_hunk.addition_start, last_hunk.addition_count) as isize;
    let deletion_remaining = file_diff.deletion_lines.len() as isize
        - get_hunk_side_end_boundary(last_hunk.deletion_start, last_hunk.deletion_count) as isize;
    Some((addition_remaining, deletion_remaining))
}

pub fn has_trailing_context(file_diff: &FileDiffMetadata) -> bool {
    trailing_remaining(file_diff).is_some_and(|(addition_remaining, deletion_remaining)| {
        addition_remaining > 0 || deletion_remaining > 0
    })
}

// Returns true when trailing-context line counts disagree between sides.
pub fn has_trailing_context_mismatch(file_diff: &FileDiffMetadata) -> bool {
    match trailing_remaining(file_diff) {
        None => false,
        Some((addition_remaining, deletion_remaining)) => {
            if addition_remaining <= 0 && deletion_remaining <= 0 {
                return false;
            }
            addition_remaining != deletion_remaining
        }
    }
}

// Measures the unchanged tail after the final hunk. Both sides must have the
// same remaining length because trailing context represents paired lines.
//
// Panics on a mismatch (`{error_prefix}: trailing context mismatch ...`).
pub fn get_trailing_context_range_size(file_diff: &FileDiffMetadata, error_prefix: &str) -> usize {
    let Some((addition_remaining, deletion_remaining)) = trailing_remaining(file_diff) else {
        return 0;
    };
    if addition_remaining <= 0 && deletion_remaining <= 0 {
        return 0;
    }
    assert!(
        addition_remaining == deletion_remaining,
        "{error_prefix}: trailing context mismatch (additions={addition_remaining}, deletions={deletion_remaining}) for {}",
        file_diff.name
    );
    usize::try_from(addition_remaining.min(deletion_remaining)).expect("positive here")
}

#[derive(Debug, Clone, Copy)]
pub struct GetTrailingExpandedRegionProps<'diff> {
    pub file_diff: &'diff FileDiffMetadata,
    pub hunk_index: usize,
    pub expanded_hunks: Option<&'diff ExpandedHunks<'diff>>,
    pub collapsed_context_threshold: usize,
    pub error_prefix: &'static str,
}

pub fn get_trailing_expanded_region(
    props: GetTrailingExpandedRegionProps<'_>,
) -> Option<ExpandedRegionResult> {
    let GetTrailingExpandedRegionProps {
        file_diff,
        hunk_index,
        expanded_hunks,
        collapsed_context_threshold,
        error_prefix,
    } = props;
    if hunk_index + 1 != file_diff.hunks.len() {
        return None;
    }

    let trailing_range_size = get_trailing_context_range_size(file_diff, error_prefix);
    if trailing_range_size == 0 {
        return None;
    }

    if matches!(expanded_hunks, Some(ExpandedHunks::All))
        || trailing_range_size <= collapsed_context_threshold
    {
        return Some(ExpandedRegionResult {
            from_start: trailing_range_size,
            from_end: 0,
            range_size: trailing_range_size,
            collapsed_lines: 0,
            render_all: true,
        });
    }

    // The final trailing separator only exposes upward partial expansion. Treat it
    // as a bottom-only pseudo-hunk and ignore unsupported downward expansion.
    let region = region_of(expanded_hunks, file_diff.hunks.len());
    let from_start = region
        .map_or(0, |region| region.from_start)
        .min(trailing_range_size);
    Some(ExpandedRegionResult {
        from_start,
        from_end: 0,
        range_size: trailing_range_size,
        collapsed_lines: trailing_range_size - from_start,
        render_all: from_start >= trailing_range_size,
    })
}

#[derive(Debug, Clone, Copy)]
pub struct IsAdditionLineRenderableProps<'diff> {
    pub file_diff: &'diff FileDiffMetadata,
    /// One-based line number in the new file.
    pub line_number: usize,
    pub expanded_hunks: Option<&'diff ExpandedHunks<'diff>>,
    pub collapsed_context_threshold: usize,
}

/// Whether a one-based new-file line currently has (or will have on scroll) a
/// rendered row under the given expansion state -- the editor-facing
/// visibility oracle. False only for lines hidden inside a collapsed
/// unchanged region; lines outside the diff's modeled range report true so
/// callers keep their existing missing-row handling. Computed from the same
/// inputs as `iterateOverDiff` so layout math and the oracle cannot diverge.
pub fn is_addition_line_renderable(props: IsAdditionLineRenderableProps<'_>) -> bool {
    let IsAdditionLineRenderableProps {
        file_diff,
        line_number,
        expanded_hunks,
        collapsed_context_threshold,
    } = props;
    if matches!(expanded_hunks, Some(ExpandedHunks::All)) || file_diff.is_partial {
        return true;
    }

    for (hunk_index, hunk) in file_diff.hunks.iter().enumerate() {
        let (hunk_start, hunk_end) = get_hunk_addition_line_range(hunk);
        if line_number < hunk_start {
            // Inside the collapsed gap before this hunk: renderable only within
            // the expanded slices at the gap's edges.
            let region = get_expanded_region(GetExpandedRegionProps {
                is_partial: file_diff.is_partial,
                range_size: hunk.collapsed_before,
                expanded_hunks,
                hunk_index,
                collapsed_context_threshold,
            });
            let gap_start = hunk_start.saturating_sub(region.range_size);
            return region.render_all
                || line_number < gap_start + region.from_start
                || line_number + region.from_end >= hunk_start;
        }
        if line_number < hunk_end {
            return true;
        }
    }

    let trailing_region = get_trailing_expanded_region(GetTrailingExpandedRegionProps {
        file_diff,
        hunk_index: file_diff.hunks.len().saturating_sub(1),
        expanded_hunks,
        collapsed_context_threshold,
        error_prefix: "isAdditionLineRenderable",
    });
    let Some(trailing_region) = trailing_region.filter(|region| !region.render_all) else {
        return true;
    };
    let last_hunk = file_diff
        .hunks
        .last()
        .expect("a trailing region implies hunks");
    let (_, trailing_start) = get_hunk_addition_line_range(last_hunk);
    line_number < trailing_start + trailing_region.from_start
        || line_number >= trailing_start + trailing_region.range_size
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RenderableDirection {
    Up,
    Down,
}

/// The nearest renderable new-file line at or beyond `line_number` in the
/// given direction (one-based), or `None` when every line that way is
/// hidden inside collapsed regions. Sequential caret motion uses this to skip
/// over collapsed regions like code folds; it walks the hunk metadata once
/// instead of probing line by line across a gap.
pub fn get_nearest_renderable_addition_line(
    props: IsAdditionLineRenderableProps<'_>,
    direction: RenderableDirection,
) -> Option<usize> {
    let IsAdditionLineRenderableProps {
        file_diff,
        line_number,
        expanded_hunks,
        collapsed_context_threshold,
    } = props;
    if matches!(expanded_hunks, Some(ExpandedHunks::All)) || file_diff.is_partial {
        return Some(line_number);
    }

    // Renderable [start, end) line ranges in ascending order, plus the
    // exclusive end of the modeled range -- anything past it (the editor's
    // phantom document-end line) counts as renderable.
    let mut ranges: Vec<(usize, usize)> = Vec::new();
    let mut modeled_end = 1;
    for (hunk_index, hunk) in file_diff.hunks.iter().enumerate() {
        let (hunk_start, hunk_end) = get_hunk_addition_line_range(hunk);
        let region = get_expanded_region(GetExpandedRegionProps {
            is_partial: file_diff.is_partial,
            range_size: hunk.collapsed_before,
            expanded_hunks,
            hunk_index,
            collapsed_context_threshold,
        });
        let gap_start = hunk_start.saturating_sub(region.range_size);
        if region.render_all {
            ranges.push((gap_start, hunk_start));
        } else {
            if region.from_start > 0 {
                ranges.push((gap_start, gap_start + region.from_start));
            }
            if region.from_end > 0 {
                ranges.push((hunk_start - region.from_end, hunk_start));
            }
        }
        ranges.push((hunk_start, hunk_end));
        modeled_end = hunk_end;
    }
    let trailing_region = get_trailing_expanded_region(GetTrailingExpandedRegionProps {
        file_diff,
        hunk_index: file_diff.hunks.len().saturating_sub(1),
        expanded_hunks,
        collapsed_context_threshold,
        error_prefix: "getNearestRenderableAdditionLine",
    });
    if let Some(trailing_region) = trailing_region {
        let trailing_start = modeled_end;
        modeled_end = trailing_start + trailing_region.range_size;
        if trailing_region.render_all {
            ranges.push((trailing_start, modeled_end));
        } else if trailing_region.from_start > 0 {
            ranges.push((trailing_start, trailing_start + trailing_region.from_start));
        }
    }

    if line_number >= modeled_end {
        return Some(line_number);
    }
    match direction {
        RenderableDirection::Down => ranges
            .iter()
            .find(|&&(_, end)| end > line_number)
            .map(|&(start, _)| start.max(line_number)),
        RenderableDirection::Up => ranges
            .iter()
            .rev()
            .find(|&&(start, _)| start <= line_number)
            .map(|&(_, end)| (end - 1).min(line_number)),
    }
}

#[derive(Debug, Clone, Copy)]
pub struct HunkSeparatorBaseProps<'metrics> {
    pub separator_type: HunkSeparators,
    pub metrics: &'metrics VirtualFileMetrics,
}

pub fn get_hunk_separator_height(props: HunkSeparatorBaseProps<'_>) -> f64 {
    props
        .metrics
        .hunk_separator_height
        .unwrap_or_else(|| get_default_hunk_separator_height(props.separator_type))
}

pub fn get_hunk_separator_gap(props: HunkSeparatorBaseProps<'_>) -> f64 {
    match props.separator_type {
        HunkSeparators::Simple | HunkSeparators::Metadata | HunkSeparators::LineInfoBasic => 0.0,
        HunkSeparators::LineInfo | HunkSeparators::Custom => props.metrics.spacing,
    }
}

pub fn has_leading_hunk_separator(
    separator_type: HunkSeparators,
    hunk_index: usize,
    hunk_specs: Option<&str>,
) -> bool {
    match separator_type {
        HunkSeparators::Simple => hunk_index > 0,
        HunkSeparators::Metadata => hunk_specs.is_some(),
        HunkSeparators::LineInfo | HunkSeparators::LineInfoBasic | HunkSeparators::Custom => true,
    }
}

pub fn has_trailing_hunk_separator(separator_type: HunkSeparators) -> bool {
    !matches!(
        separator_type,
        HunkSeparators::Simple | HunkSeparators::Metadata
    )
}

// Mirrors the renderer/CSS spacing rules for the separator shown before a hunk.
pub fn get_leading_hunk_separator_layout(
    props: HunkSeparatorBaseProps<'_>,
    hunk_index: usize,
    hunk_specs: Option<&str>,
) -> Option<HunkSeparatorLayout> {
    if !has_leading_hunk_separator(props.separator_type, hunk_index, hunk_specs) {
        return None;
    }

    let height = get_hunk_separator_height(props);
    let gap = get_hunk_separator_gap(props);
    let gap_before = if hunk_index > 0 { gap } else { 0.0 };
    let gap_after = gap;
    Some(HunkSeparatorLayout {
        height,
        gap_before,
        gap_after,
        total_height: gap_before + height + gap_after,
    })
}

// Mirrors the renderer/CSS spacing rules for the separator shown after the last
// hunk when trailing unchanged context is collapsed.
pub fn get_trailing_hunk_separator_layout(
    props: HunkSeparatorBaseProps<'_>,
) -> Option<HunkSeparatorLayout> {
    if !has_trailing_hunk_separator(props.separator_type) {
        return None;
    }

    let height = get_hunk_separator_height(props);
    let gap_before = get_hunk_separator_gap(props);
    Some(HunkSeparatorLayout {
        height,
        gap_before,
        gap_after: 0.0,
        total_height: gap_before + height,
    })
}
