// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/computeEstimatedDiffHeights.test.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: bun:test to nextest; test titles are snake_case; the `compute`
// helper with `Partial<...>` overrides is `ComputeOverrides`. Every fixture
// has a unique minimal diff, so `similar`'s hunks equal jsdiff's and no
// assertion changes.

use std::collections::HashMap;

use crate::constants::DEFAULT_COLLAPSED_CONTEXT_THRESHOLD;
use crate::test_utils::count_declared_rows;
use crate::types::layout::VirtualFileMetrics;
use crate::types::lines::ExpandedHunks;
use crate::types::lines::HunkExpansionRegion;
use crate::types::options::HunkSeparators;
use crate::types::patch::DiffFileInput;
use crate::types::patch::FileContents;
use crate::types::patch::FileDiffMetadata;
use crate::types::patch::ParseDiffOptions;
use crate::utils::compute_estimated_diff_heights::ComputeEstimatedDiffHeightsOptions;
use crate::utils::compute_estimated_diff_heights::EstimatedDiffHeights;
use crate::utils::compute_estimated_diff_heights::compute_estimated_diff_heights;
use crate::utils::parse_diff_from_file::parse_diff_from_file;

const METRICS: VirtualFileMetrics = VirtualFileMetrics {
    hunk_line_count: 2,
    line_height: 10.0,
    diff_header_height: 30.0,
    hunk_separator_height: None,
    spacing: 4.0,
    padding_top: None,
    padding_bottom: None,
};

// Built-in separator heights mirror getDefaultHunkSeparatorHeight: 'simple'
// renders a 4px rule while the other separator types measure 32px.
const SIMPLE_SEPARATOR_HEIGHT: f64 = 4.0;
const LINE_INFO_SEPARATOR_HEIGHT: f64 = 32.0;
const METADATA_SEPARATOR_HEIGHT: f64 = 32.0;

// 'line-info' separators add `spacing` gaps around themselves except above
// the first separator and below the trailing one, mirroring
// getLeadingHunkSeparatorLayout and getTrailingHunkSeparatorLayout.
const FIRST_LINE_INFO_SEPARATOR_HEIGHT: f64 = LINE_INFO_SEPARATOR_HEIGHT + METRICS.spacing;
const MIDDLE_LINE_INFO_SEPARATOR_HEIGHT: f64 =
    METRICS.spacing + LINE_INFO_SEPARATOR_HEIGHT + METRICS.spacing;
const TRAILING_LINE_INFO_SEPARATOR_HEIGHT: f64 = METRICS.spacing + LINE_INFO_SEPARATOR_HEIGHT;

// Diffs with at least one hunk always end with bottom padding, which falls
// back to `spacing` when `paddingBottom` is not configured.
const DEFAULT_PADDING_BOTTOM: f64 = METRICS.spacing;

// Geometry for createTwoHunkDiff: a 140-line file with single-line changes at
// lines 40 and 100, leaving collapsed unchanged context before, between, and
// after the two parsed hunks.
const TWO_HUNK_FILE_LINE_COUNT: usize = 140;
const TWO_HUNK_CHANGED_LINES: [usize; 2] = [40, 100];

fn parse(name: &str, old_contents: &str, new_contents: &str) -> FileDiffMetadata {
    let file = |contents: &str| FileContents {
        name: name.to_owned(),
        contents: contents.to_owned(),
        lang: None,
        header: None,
        cache_key: None,
    };
    parse_diff_from_file(
        &DiffFileInput::Both {
            old_file: file(old_contents),
            new_file: file(new_contents),
        },
        &ParseDiffOptions::default(),
        false,
    )
    .expect("valid")
    .value
}

fn create_two_hunk_diff() -> FileDiffMetadata {
    let old_lines: Vec<String> = (1..=TWO_HUNK_FILE_LINE_COUNT)
        .map(|index| index.to_string())
        .collect();
    let new_lines: Vec<String> = old_lines
        .iter()
        .enumerate()
        .map(|(index, line)| {
            if TWO_HUNK_CHANGED_LINES.contains(&(index + 1)) {
                format!("changed-{}", index + 1)
            } else {
                line.clone()
            }
        })
        .collect();
    let file_diff = parse(
        "two-hunks.ts",
        &format!("{}\n", old_lines.join("\n")),
        &format!("{}\n", new_lines.join("\n")),
    );
    assert!(
        file_diff.hunks.len() == 2
            && file_diff.hunks[0].collapsed_before > 0
            && file_diff.hunks[1].collapsed_before > 0,
        "Expected two hunks with collapsed leading context"
    );
    file_diff
}

// Height contributed by the rows each hunk declares (plus auto-expanded gaps
// at or under the collapsed-context threshold, none in these fixtures).
fn get_declared_row_heights(file_diff: &FileDiffMetadata) -> (f64, f64) {
    (
        count_declared_rows(file_diff, true, DEFAULT_COLLAPSED_CONTEXT_THRESHOLD) as f64
            * METRICS.line_height,
        count_declared_rows(file_diff, false, DEFAULT_COLLAPSED_CONTEXT_THRESHOLD) as f64
            * METRICS.line_height,
    )
}

#[derive(Clone, Copy)]
struct ComputeOverrides<'diff> {
    metrics: VirtualFileMetrics,
    hunk_separators: HunkSeparators,
    expand_unchanged: bool,
    expanded_hunks: Option<&'diff ExpandedHunks<'diff>>,
    can_hydrate_partial_diff: bool,
}

impl Default for ComputeOverrides<'_> {
    fn default() -> Self {
        Self {
            metrics: METRICS,
            hunk_separators: HunkSeparators::LineInfo,
            expand_unchanged: false,
            expanded_hunks: None,
            can_hydrate_partial_diff: false,
        }
    }
}

fn compute(file_diff: &FileDiffMetadata, overrides: ComputeOverrides<'_>) -> EstimatedDiffHeights {
    compute_estimated_diff_heights(ComputeEstimatedDiffHeightsOptions {
        file_diff,
        metrics: &overrides.metrics,
        disable_file_header: false,
        hunk_separators: overrides.hunk_separators,
        expand_unchanged: overrides.expand_unchanged,
        expanded_hunks: overrides.expanded_hunks,
        collapsed_context_threshold: DEFAULT_COLLAPSED_CONTEXT_THRESHOLD,
        can_hydrate_partial_diff: overrides.can_hydrate_partial_diff,
    })
}

fn heights(split_height: f64, unified_height: f64) -> EstimatedDiffHeights {
    EstimatedDiffHeights {
        split_height,
        unified_height,
    }
}

#[test]
fn returns_only_the_top_region_when_a_diff_has_no_hunks() {
    let file_diff = parse("same.ts", "one\n", "one\n");
    let padding_top = 6.0;
    let header_region = METRICS.diff_header_height + padding_top;

    // paddingBottom is skipped entirely when there are no hunks to render.
    assert_eq!(
        compute(
            &file_diff,
            ComputeOverrides {
                metrics: VirtualFileMetrics {
                    padding_top: Some(padding_top),
                    padding_bottom: Some(13.0),
                    ..METRICS
                },
                ..ComputeOverrides::default()
            }
        ),
        heights(header_region, header_region)
    );
}

#[test]
fn computes_split_and_unified_heights_with_no_newline_metadata_rows() {
    let file_diff = parse("no-newline.ts", "one\ntwo", "one\nTWO");
    let (split_rows, unified_rows) = get_declared_row_heights(&file_diff);
    // Both sides lose their trailing newline on the changed final line, so
    // split rendering shares one metadata row while unified rendering shows
    // one per side.
    let split_metadata_rows = 1.0;
    let unified_metadata_rows = 2.0;

    assert_eq!(
        compute(&file_diff, ComputeOverrides::default()),
        heights(
            METRICS.diff_header_height
                + split_rows
                + split_metadata_rows * METRICS.line_height
                + DEFAULT_PADDING_BOTTOM,
            METRICS.diff_header_height
                + unified_rows
                + unified_metadata_rows * METRICS.line_height
                + DEFAULT_PADDING_BOTTOM,
        )
    );
}

#[test]
fn computes_split_metadata_when_the_no_newline_deletion_side_is_shorter() {
    let file_diff = parse(
        "deletion-shorter.ts",
        "same\nold-final",
        "same\nnew-a\nnew-b\n",
    );
    let (split_rows, unified_rows) = get_declared_row_heights(&file_diff);
    // Only the deletion side loses its trailing newline, producing a single
    // metadata row in both layouts.
    let metadata_rows = 1.0;

    assert_eq!(
        compute(&file_diff, ComputeOverrides::default()),
        heights(
            METRICS.diff_header_height
                + split_rows
                + metadata_rows * METRICS.line_height
                + DEFAULT_PADDING_BOTTOM,
            METRICS.diff_header_height
                + unified_rows
                + metadata_rows * METRICS.line_height
                + DEFAULT_PADDING_BOTTOM,
        )
    );
}

#[test]
fn computes_split_metadata_when_the_no_newline_addition_side_is_shorter() {
    let file_diff = parse(
        "addition-shorter.ts",
        "same\nold-a\nold-b\n",
        "same\nnew-final",
    );
    let (split_rows, unified_rows) = get_declared_row_heights(&file_diff);
    // Only the addition side loses its trailing newline, producing a single
    // metadata row in both layouts.
    let metadata_rows = 1.0;

    assert_eq!(
        compute(&file_diff, ComputeOverrides::default()),
        heights(
            METRICS.diff_header_height
                + split_rows
                + metadata_rows * METRICS.line_height
                + DEFAULT_PADDING_BOTTOM,
            METRICS.diff_header_height
                + unified_rows
                + metadata_rows * METRICS.line_height
                + DEFAULT_PADDING_BOTTOM,
        )
    );
}

#[test]
fn accounts_for_collapsed_leading_and_trailing_line_info_separators() {
    let file_diff = create_two_hunk_diff();
    let (split_rows, unified_rows) = get_declared_row_heights(&file_diff);

    assert_eq!(
        compute(&file_diff, ComputeOverrides::default()),
        heights(
            METRICS.diff_header_height
                + FIRST_LINE_INFO_SEPARATOR_HEIGHT
                + split_rows
                + MIDDLE_LINE_INFO_SEPARATOR_HEIGHT
                + TRAILING_LINE_INFO_SEPARATOR_HEIGHT
                + DEFAULT_PADDING_BOTTOM,
            METRICS.diff_header_height
                + FIRST_LINE_INFO_SEPARATOR_HEIGHT
                + unified_rows
                + MIDDLE_LINE_INFO_SEPARATOR_HEIGHT
                + TRAILING_LINE_INFO_SEPARATOR_HEIGHT
                + DEFAULT_PADDING_BOTTOM,
        )
    );
}

#[test]
fn preserves_current_simple_separator_behavior() {
    let file_diff = create_two_hunk_diff();
    let (split_rows, unified_rows) = get_declared_row_heights(&file_diff);

    // 'simple' separators render no rule before the first hunk and reserve
    // nothing for trailing collapsed context, leaving only the middle rule.
    assert_eq!(
        compute(
            &file_diff,
            ComputeOverrides {
                hunk_separators: HunkSeparators::Simple,
                ..ComputeOverrides::default()
            }
        ),
        heights(
            METRICS.diff_header_height
                + split_rows
                + SIMPLE_SEPARATOR_HEIGHT
                + DEFAULT_PADDING_BOTTOM,
            METRICS.diff_header_height
                + unified_rows
                + SIMPLE_SEPARATOR_HEIGHT
                + DEFAULT_PADDING_BOTTOM,
        )
    );
}

#[test]
fn expands_unchanged_context_as_rows_without_separators() {
    let file_diff = create_two_hunk_diff();
    // Full expansion renders every file line as one split row; unified adds
    // an extra row per single-line change (deletion plus addition rows).
    let split_rows = TWO_HUNK_FILE_LINE_COUNT as f64;
    let unified_rows = (TWO_HUNK_FILE_LINE_COUNT + TWO_HUNK_CHANGED_LINES.len()) as f64;

    assert_eq!(
        compute(
            &file_diff,
            ComputeOverrides {
                expand_unchanged: true,
                ..ComputeOverrides::default()
            }
        ),
        heights(
            METRICS.diff_header_height + split_rows * METRICS.line_height + DEFAULT_PADDING_BOTTOM,
            METRICS.diff_header_height
                + unified_rows * METRICS.line_height
                + DEFAULT_PADDING_BOTTOM,
        )
    );
}

fn one_expansion(hunk_index: usize, from_start: usize, from_end: usize) -> ExpandedHunks<'static> {
    ExpandedHunks::owned(HashMap::from([(
        hunk_index,
        HunkExpansionRegion {
            from_start,
            from_end,
        },
    )]))
}

#[test]
fn accounts_for_partially_expanded_leading_context() {
    let file_diff = create_two_hunk_diff();
    let (split_rows, unified_rows) = get_declared_row_heights(&file_diff);
    let expanded_hunks = one_expansion(0, 2, 3);
    // Partial expansion renders fromStart + fromEnd context rows while the
    // remaining collapsed lines keep the first hunk's separator.
    let expanded_rows = 5.0;

    assert_eq!(
        compute(
            &file_diff,
            ComputeOverrides {
                expanded_hunks: Some(&expanded_hunks),
                ..ComputeOverrides::default()
            }
        ),
        heights(
            METRICS.diff_header_height
                + expanded_rows * METRICS.line_height
                + FIRST_LINE_INFO_SEPARATOR_HEIGHT
                + split_rows
                + MIDDLE_LINE_INFO_SEPARATOR_HEIGHT
                + TRAILING_LINE_INFO_SEPARATOR_HEIGHT
                + DEFAULT_PADDING_BOTTOM,
            METRICS.diff_header_height
                + expanded_rows * METRICS.line_height
                + FIRST_LINE_INFO_SEPARATOR_HEIGHT
                + unified_rows
                + MIDDLE_LINE_INFO_SEPARATOR_HEIGHT
                + TRAILING_LINE_INFO_SEPARATOR_HEIGHT
                + DEFAULT_PADDING_BOTTOM,
        )
    );
}

#[test]
fn accounts_for_partially_expanded_trailing_context_from_the_start_only() {
    let file_diff = create_two_hunk_diff();
    let (split_rows, unified_rows) = get_declared_row_heights(&file_diff);
    let expanded_hunks = one_expansion(file_diff.hunks.len(), 2, 3);
    // Trailing context only supports upward expansion, so fromEnd is ignored
    // and the still-collapsed remainder keeps its trailing separator.
    let expanded_rows = 2.0;

    assert_eq!(
        compute(
            &file_diff,
            ComputeOverrides {
                expanded_hunks: Some(&expanded_hunks),
                ..ComputeOverrides::default()
            }
        ),
        heights(
            METRICS.diff_header_height
                + FIRST_LINE_INFO_SEPARATOR_HEIGHT
                + split_rows
                + MIDDLE_LINE_INFO_SEPARATOR_HEIGHT
                + expanded_rows * METRICS.line_height
                + TRAILING_LINE_INFO_SEPARATOR_HEIGHT
                + DEFAULT_PADDING_BOTTOM,
            METRICS.diff_header_height
                + FIRST_LINE_INFO_SEPARATOR_HEIGHT
                + unified_rows
                + MIDDLE_LINE_INFO_SEPARATOR_HEIGHT
                + expanded_rows * METRICS.line_height
                + TRAILING_LINE_INFO_SEPARATOR_HEIGHT
                + DEFAULT_PADDING_BOTTOM,
        )
    );
}

#[test]
fn does_not_reserve_a_synthetic_trailing_separator_for_partial_diffs_without_a_file_loader() {
    let file_diff = FileDiffMetadata {
        is_partial: true,
        ..create_two_hunk_diff()
    };
    let (split_rows, unified_rows) = get_declared_row_heights(&file_diff);

    // Without a loader the unknown partial tail cannot be expanded, so no
    // synthetic trailing separator is rendered or reserved.
    assert_eq!(
        compute(&file_diff, ComputeOverrides::default()),
        heights(
            METRICS.diff_header_height
                + FIRST_LINE_INFO_SEPARATOR_HEIGHT
                + split_rows
                + MIDDLE_LINE_INFO_SEPARATOR_HEIGHT
                + DEFAULT_PADDING_BOTTOM,
            METRICS.diff_header_height
                + FIRST_LINE_INFO_SEPARATOR_HEIGHT
                + unified_rows
                + MIDDLE_LINE_INFO_SEPARATOR_HEIGHT
                + DEFAULT_PADDING_BOTTOM,
        )
    );
}

#[test]
fn reserves_a_synthetic_trailing_separator_for_partial_diffs_with_a_file_loader() {
    let file_diff = FileDiffMetadata {
        is_partial: true,
        ..create_two_hunk_diff()
    };
    let (split_rows, unified_rows) = get_declared_row_heights(&file_diff);

    assert_eq!(
        compute(
            &file_diff,
            ComputeOverrides {
                can_hydrate_partial_diff: true,
                ..ComputeOverrides::default()
            }
        ),
        heights(
            METRICS.diff_header_height
                + FIRST_LINE_INFO_SEPARATOR_HEIGHT
                + split_rows
                + MIDDLE_LINE_INFO_SEPARATOR_HEIGHT
                + TRAILING_LINE_INFO_SEPARATOR_HEIGHT
                + DEFAULT_PADDING_BOTTOM,
            METRICS.diff_header_height
                + FIRST_LINE_INFO_SEPARATOR_HEIGHT
                + unified_rows
                + MIDDLE_LINE_INFO_SEPARATOR_HEIGHT
                + TRAILING_LINE_INFO_SEPARATOR_HEIGHT
                + DEFAULT_PADDING_BOTTOM,
        )
    );
}

#[test]
fn reserves_metadata_separators_only_for_hunk_specs() {
    let file_diff = create_two_hunk_diff();
    let (split_rows, unified_rows) = get_declared_row_heights(&file_diff);
    // 'metadata' separators render gapless before each hunk that carries
    // hunk specs and reserve nothing for trailing collapsed context.
    let hunks_with_specs = file_diff
        .hunks
        .iter()
        .filter(|hunk| hunk.hunk_specs.is_some())
        .count();

    assert_eq!(hunks_with_specs, file_diff.hunks.len());
    assert_eq!(
        compute(
            &file_diff,
            ComputeOverrides {
                hunk_separators: HunkSeparators::Metadata,
                ..ComputeOverrides::default()
            }
        ),
        heights(
            METRICS.diff_header_height
                + METADATA_SEPARATOR_HEIGHT * hunks_with_specs as f64
                + split_rows
                + DEFAULT_PADDING_BOTTOM,
            METRICS.diff_header_height
                + METADATA_SEPARATOR_HEIGHT * hunks_with_specs as f64
                + unified_rows
                + DEFAULT_PADDING_BOTTOM,
        )
    );
}
