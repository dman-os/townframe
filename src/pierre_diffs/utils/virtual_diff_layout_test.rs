// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/virtualDiffLayout.test.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: bun:test to nextest; test titles are snake_case; JavaScript's
// `Number.POSITIVE_INFINITY` region size is `usize::MAX` (it is clamped to the
// range size either way).

use std::collections::HashMap;

use crate::types::layout::VirtualFileMetrics;
use crate::types::lines::ExpandedHunks;
use crate::types::lines::HunkExpansionRegion;
use crate::types::options::HunkSeparators;
use crate::types::patch::ChangeTypes;
use crate::types::patch::ContextContent;
use crate::types::patch::FileDiffMetadata;
use crate::types::patch::Hunk;
use crate::types::patch::HunkContent;
use crate::utils::virtual_diff_layout::ExpandedRegionResult;
use crate::utils::virtual_diff_layout::GetExpandedRegionProps;
use crate::utils::virtual_diff_layout::GetTrailingExpandedRegionProps;
use crate::utils::virtual_diff_layout::HunkSeparatorBaseProps;
use crate::utils::virtual_diff_layout::get_expanded_region;
use crate::utils::virtual_diff_layout::get_leading_hunk_separator_layout;
use crate::utils::virtual_diff_layout::get_trailing_context_range_size;
use crate::utils::virtual_diff_layout::get_trailing_expanded_region;
use crate::utils::virtual_diff_layout::get_trailing_hunk_separator_layout;

const METRICS: VirtualFileMetrics = VirtualFileMetrics {
    hunk_line_count: 2,
    line_height: 10.0,
    diff_header_height: 30.0,
    hunk_separator_height: None,
    spacing: 4.0,
    padding_top: None,
    padding_bottom: None,
};

fn region(
    from_start: usize,
    from_end: usize,
    range_size: usize,
    collapsed_lines: usize,
    render_all: bool,
) -> ExpandedRegionResult {
    ExpandedRegionResult {
        from_start,
        from_end,
        range_size,
        collapsed_lines,
        render_all,
    }
}

fn expanded_region(
    is_partial: bool,
    expanded_hunks: Option<&ExpandedHunks>,
    range_size: usize,
) -> ExpandedRegionResult {
    get_expanded_region(GetExpandedRegionProps {
        is_partial,
        range_size,
        expanded_hunks,
        hunk_index: 1,
        collapsed_context_threshold: 1,
    })
}

fn one_region(from_start: usize, from_end: usize) -> ExpandedHunks<'static> {
    ExpandedHunks::owned(HashMap::from([(
        1,
        HunkExpansionRegion {
            from_start,
            from_end,
        },
    )]))
}

#[test]
fn keeps_collapsed_ranges_collapsed_by_default() {
    assert_eq!(
        expanded_region(false, None, 10),
        region(0, 0, 10, 10, false)
    );
}

#[test]
fn expands_all_lines_for_expand_unchanged_or_small_ranges() {
    assert_eq!(
        expanded_region(false, Some(&ExpandedHunks::All), 10),
        region(10, 0, 10, 0, true)
    );
    assert_eq!(expanded_region(false, None, 1), region(1, 0, 1, 0, true));
}

#[test]
fn clamps_explicit_expansion_regions_to_the_collapsed_range() {
    assert_eq!(
        expanded_region(false, Some(&one_region(3, 20)), 10),
        region(10, 0, 10, 0, true)
    );
}

#[test]
fn keeps_partial_diffs_collapsed_even_with_expansion_state() {
    assert_eq!(
        expanded_region(true, Some(&ExpandedHunks::All), 10),
        region(0, 0, 10, 10, false)
    );
}

fn create_lines(count: usize) -> Vec<String> {
    (0..count).map(|index| format!("line {index}\n")).collect()
}

fn create_trailing_diff(trailing_line_count: usize) -> FileDiffMetadata {
    let hunk = Hunk {
        collapsed_before: 0,
        addition_start: 1,
        addition_count: 2,
        addition_lines: 0,
        addition_line_index: 0,
        deletion_start: 1,
        deletion_count: 2,
        deletion_lines: 0,
        deletion_line_index: 0,
        hunk_content: vec![HunkContent::Context(ContextContent {
            lines: 2,
            deletion_line_index: 0,
            addition_line_index: 0,
        })],
        hunk_context: None,
        hunk_specs: Some(String::from("@@ -1,2 +1,2 @@")),
        split_line_start: 0,
        split_line_count: 2,
        unified_line_start: 0,
        unified_line_count: 2,
        no_eofcr_deletions: false,
        no_eofcr_additions: false,
    };
    let deletion_lines = create_lines(2 + trailing_line_count);
    let addition_lines = create_lines(2 + trailing_line_count);
    FileDiffMetadata {
        name: String::from("trailing.ts"),
        prev_name: None,
        lang: None,
        new_object_id: None,
        prev_object_id: None,
        mode: None,
        prev_mode: None,
        change_type: ChangeTypes::Change,
        hunks: vec![hunk],
        split_line_count: 2 + trailing_line_count,
        unified_line_count: 2 + trailing_line_count,
        is_partial: false,
        deletion_character_count: deletion_lines.concat().len(),
        addition_character_count: addition_lines.concat().len(),
        deletion_lines,
        addition_lines,
        edit_session_dirty: None,
        cache_key: None,
    }
}

fn trailing_region(
    file_diff: &FileDiffMetadata,
    expanded_hunks: &ExpandedHunks,
) -> Option<ExpandedRegionResult> {
    get_trailing_expanded_region(GetTrailingExpandedRegionProps {
        file_diff,
        hunk_index: 0,
        expanded_hunks: Some(expanded_hunks),
        collapsed_context_threshold: 0,
        error_prefix: "virtualDiffLayout.test",
    })
}

#[test]
fn preserves_known_trailing_context_only_for_full_diffs() {
    let file_diff = create_trailing_diff(5);
    assert_eq!(
        get_trailing_context_range_size(&file_diff, "virtualDiffLayout.test"),
        5
    );
    let partial = FileDiffMetadata {
        is_partial: true,
        ..file_diff
    };
    assert_eq!(
        get_trailing_context_range_size(&partial, "virtualDiffLayout.test"),
        0
    );
}

#[test]
fn ignores_unsupported_final_trailing_from_end_expansion() {
    let file_diff = create_trailing_diff(5);
    assert_eq!(
        trailing_region(&file_diff, &one_region(2, 3)),
        Some(region(2, 0, 5, 3, false))
    );
}

#[test]
fn expands_all_final_trailing_context_from_the_start() {
    let file_diff = create_trailing_diff(5);
    assert_eq!(
        trailing_region(&file_diff, &ExpandedHunks::All),
        Some(region(5, 0, 5, 0, true))
    );
    assert_eq!(
        trailing_region(&file_diff, &one_region(usize::MAX, usize::MAX)),
        Some(region(5, 0, 5, 0, true))
    );
}

fn leading_total_height(
    separator_type: HunkSeparators,
    metrics: &VirtualFileMetrics,
    hunk_index: usize,
    hunk_specs: Option<&str>,
) -> Option<f64> {
    get_leading_hunk_separator_layout(
        HunkSeparatorBaseProps {
            separator_type,
            metrics,
        },
        hunk_index,
        hunk_specs,
    )
    .map(|layout| layout.total_height)
}

#[test]
fn preserves_current_leading_separator_rules() {
    let specs = Some("@@ -1 +1 @@");
    let cases = [
        (HunkSeparators::Simple, 0, specs, None),
        (HunkSeparators::Simple, 1, specs, Some(4.0)),
        (HunkSeparators::Metadata, 0, None, None),
        (HunkSeparators::Metadata, 0, specs, Some(32.0)),
        (HunkSeparators::LineInfo, 0, specs, Some(36.0)),
        (HunkSeparators::LineInfo, 1, specs, Some(40.0)),
        (HunkSeparators::LineInfoBasic, 0, specs, Some(32.0)),
        (HunkSeparators::Custom, 0, specs, Some(36.0)),
        (HunkSeparators::Custom, 1, specs, Some(40.0)),
    ];
    for (separator_type, hunk_index, hunk_specs, total_height) in cases {
        assert_eq!(
            leading_total_height(separator_type, &METRICS, hunk_index, hunk_specs),
            total_height,
            "{separator_type:?} {hunk_index}"
        );
    }
}

#[test]
fn preserves_current_trailing_separator_rules() {
    let cases = [
        (HunkSeparators::Simple, None),
        (HunkSeparators::Metadata, None),
        (HunkSeparators::LineInfo, Some(36.0)),
        (HunkSeparators::LineInfoBasic, Some(32.0)),
        (HunkSeparators::Custom, Some(36.0)),
    ];
    for (separator_type, total_height) in cases {
        assert_eq!(
            get_trailing_hunk_separator_layout(HunkSeparatorBaseProps {
                separator_type,
                metrics: &METRICS,
            })
            .map(|layout| layout.total_height),
            total_height,
            "{separator_type:?}"
        );
    }
}

#[test]
fn uses_custom_hunk_separator_height_metrics() {
    let metrics = VirtualFileMetrics {
        hunk_separator_height: Some(12.0),
        ..METRICS
    };
    assert_eq!(
        leading_total_height(HunkSeparators::LineInfo, &metrics, 1, Some("@@ -1 +1 @@")),
        Some(20.0)
    );
}
