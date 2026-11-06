// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/computeEstimatedDiffHeights.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: `expandUnchanged ? true : configuredExpandedHunks` is resolved by
// the caller into one `Option<&ExpandedHunks>` plus the `expand_unchanged`
// flag here, as upstream; the invalid-hunk `throw` is unreachable by
// construction.

use crate::types::layout::VirtualFileMetrics;
use crate::types::lines::ExpandedHunks;
use crate::types::options::HunkSeparators;
use crate::types::patch::ChangeContent;
use crate::types::patch::FileDiffMetadata;
use crate::types::patch::Hunk;
use crate::types::patch::HunkContent;
use crate::utils::compute_virtual_file_metrics::get_virtual_file_header_region;
use crate::utils::compute_virtual_file_metrics::get_virtual_file_padding_bottom;
use crate::utils::virtual_diff_layout::GetExpandedRegionProps;
use crate::utils::virtual_diff_layout::GetTrailingExpandedRegionProps;
use crate::utils::virtual_diff_layout::HunkSeparatorBaseProps;
use crate::utils::virtual_diff_layout::get_expanded_region;
use crate::utils::virtual_diff_layout::get_leading_hunk_separator_layout;
use crate::utils::virtual_diff_layout::get_trailing_expanded_region;
use crate::utils::virtual_diff_layout::get_trailing_hunk_separator_layout;

#[derive(Debug, Clone, Copy)]
pub struct ComputeEstimatedDiffHeightsOptions<'diff> {
    pub file_diff: &'diff FileDiffMetadata,
    pub metrics: &'diff VirtualFileMetrics,
    pub disable_file_header: bool,
    pub hunk_separators: HunkSeparators,
    pub expand_unchanged: bool,
    pub expanded_hunks: Option<&'diff ExpandedHunks>,
    pub collapsed_context_threshold: usize,
    pub can_hydrate_partial_diff: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EstimatedDiffHeights {
    pub split_height: f64,
    pub unified_height: f64,
}

// Computes both split and unified baseline heights from hunk-level metadata so
// callers can avoid replaying the detailed rendered-line iterator.
pub fn compute_estimated_diff_heights(
    options: ComputeEstimatedDiffHeightsOptions<'_>,
) -> EstimatedDiffHeights {
    let ComputeEstimatedDiffHeightsOptions {
        file_diff,
        metrics,
        disable_file_header,
        hunk_separators,
        expand_unchanged,
        expanded_hunks: configured_expanded_hunks,
        collapsed_context_threshold,
        can_hydrate_partial_diff,
    } = options;
    let mut split_height = get_virtual_file_header_region(metrics, disable_file_header);
    let mut unified_height = split_height;
    let expanded_hunks = if expand_unchanged {
        Some(&ExpandedHunks::All)
    } else {
        configured_expanded_hunks
    };
    let separator_props = HunkSeparatorBaseProps {
        separator_type: hunk_separators,
        metrics,
    };
    let final_hunk_index = file_diff.hunks.len().wrapping_sub(1);

    for (hunk_index, hunk) in file_diff.hunks.iter().enumerate() {
        let leading_region = get_expanded_region(GetExpandedRegionProps {
            is_partial: file_diff.is_partial,
            range_size: hunk.collapsed_before,
            expanded_hunks,
            hunk_index,
            collapsed_context_threshold,
        });
        let leading_expanded_height =
            (leading_region.from_start + leading_region.from_end) as f64 * metrics.line_height;
        split_height += leading_expanded_height;
        unified_height += leading_expanded_height;

        if leading_region.collapsed_lines > 0 {
            let separator_height = get_leading_hunk_separator_layout(
                separator_props,
                hunk_index,
                hunk.hunk_specs.as_deref(),
            )
            .map_or(0.0, |layout| layout.total_height);
            split_height += separator_height;
            unified_height += separator_height;
        }

        split_height += hunk.split_line_count as f64 * metrics.line_height;
        unified_height += hunk.unified_line_count as f64 * metrics.line_height;

        let (metadata_split, metadata_unified) = get_no_newline_metadata_line_counts(hunk);
        split_height += metadata_split as f64 * metrics.line_height;
        unified_height += metadata_unified as f64 * metrics.line_height;

        let trailing_region = if hunk_index == final_hunk_index {
            get_trailing_expanded_region(GetTrailingExpandedRegionProps {
                file_diff,
                hunk_index,
                expanded_hunks,
                collapsed_context_threshold,
                error_prefix: "computeEstimatedDiffHeights",
            })
        } else {
            None
        };
        if let Some(trailing_region) = trailing_region {
            let trailing_expanded_height = (trailing_region.from_start + trailing_region.from_end)
                as f64
                * metrics.line_height;
            split_height += trailing_expanded_height;
            unified_height += trailing_expanded_height;

            if trailing_region.collapsed_lines > 0 {
                let separator_height = get_trailing_hunk_separator_layout(separator_props)
                    .map_or(0.0, |layout| layout.total_height);
                split_height += separator_height;
                unified_height += separator_height;
            }
        } else if hunk_index == final_hunk_index && file_diff.is_partial && can_hydrate_partial_diff
        {
            let separator_height = get_trailing_hunk_separator_layout(separator_props)
                .map_or(0.0, |layout| layout.total_height);
            split_height += separator_height;
            unified_height += separator_height;
        }
    }

    if !file_diff.hunks.is_empty() {
        let padding_bottom = get_virtual_file_padding_bottom(metrics);
        split_height += padding_bottom;
        unified_height += padding_bottom;
    }

    EstimatedDiffHeights {
        split_height,
        unified_height,
    }
}

/// `(split, unified)` rows added by the "\ No newline at end of file" markers.
fn get_no_newline_metadata_line_counts(hunk: &Hunk) -> (usize, usize) {
    if !hunk.no_eofcr_additions && !hunk.no_eofcr_deletions {
        return (0, 0);
    }

    match hunk.hunk_content.last() {
        None => (0, 0),
        Some(HunkContent::Context(context)) => {
            let metadata_rows = usize::from(context.lines > 0);
            (metadata_rows, metadata_rows)
        }
        Some(HunkContent::Change(change)) => {
            get_change_no_newline_metadata_line_counts(hunk, change)
        }
    }
}

fn get_change_no_newline_metadata_line_counts(
    hunk: &Hunk,
    content: &ChangeContent,
) -> (usize, usize) {
    let deletion_has_metadata = content.deletions > 0 && hunk.no_eofcr_deletions;
    let addition_has_metadata = content.additions > 0 && hunk.no_eofcr_additions;
    let unified = usize::from(deletion_has_metadata) + usize::from(addition_has_metadata);
    let split = usize::from(deletion_has_metadata || addition_has_metadata);
    (split, unified)
}
