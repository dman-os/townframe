// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/testUtils.ts
// (the hunk-verification helpers; the renderer-facing hast helpers are in the
// `hast` child module).
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: test-only module; `assertDefined` is `Option::expect`; the digest
// helpers return `serde_json::Value`.

pub mod hast;

use crate::types::patch::FileDiffMetadata;
use crate::types::patch::HunkContent;
use crate::types::patch::ParsedPatch;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyResult {
    pub valid: bool,
    pub errors: Vec<String>,
}

// Checks the cross-field consistency invariants of parsed hunk metadata: the
// per-hunk counts (additionCount, splitLineCount, unifiedLineCount, ...) must
// agree with what the hunkContent blocks declare, and the cumulative
// line-start/collapsedBefore bookkeeping must chain correctly across hunks.
// This intentionally looks like a second implementation of the parser's
// arithmetic -- it validates that independently-written fields stay in sync,
// not that the parser matches itself.
pub fn verify_hunk_line_values(file: &FileDiffMetadata, prefix: &str) -> Vec<String> {
    let mut errors = Vec::new();

    let mut current_split_line_total = 0usize;
    let mut current_unified_line_total = 0usize;
    let mut last_hunk_addition_end = 0usize;

    for (hunk_index, hunk) in file.hunks.iter().enumerate() {
        let hunk_prefix = format!("{prefix}.hunks[{hunk_index}]");

        // Count lines from hunkContent
        let (mut context_lines, mut addition_lines, mut deletion_lines) = (0, 0, 0);
        let (mut expected_split_line_count, mut expected_unified_line_count) = (0, 0);
        for content in &hunk.hunk_content {
            match content {
                HunkContent::Context(context) => {
                    context_lines += context.lines;
                    expected_split_line_count += context.lines;
                    expected_unified_line_count += context.lines;
                }
                HunkContent::Change(change) => {
                    addition_lines += change.additions;
                    deletion_lines += change.deletions;
                    expected_split_line_count += change.additions.max(change.deletions);
                    expected_unified_line_count += change.additions + change.deletions;
                }
            }
        }

        let mut check = |label: &str, actual: usize, expected: usize, what: &str| {
            if actual != expected {
                errors.push(format!(
                    "{hunk_prefix}: {label} ({actual}) !== {what} ({expected})"
                ));
            }
        };
        check(
            "additionCount",
            hunk.addition_count,
            addition_lines + context_lines,
            "additionLines + context",
        );
        check(
            "deletionCount",
            hunk.deletion_count,
            deletion_lines + context_lines,
            "deletionLines + context",
        );
        check(
            "additionLines",
            hunk.addition_lines,
            addition_lines,
            "counted additions",
        );
        check(
            "deletionLines",
            hunk.deletion_lines,
            deletion_lines,
            "counted deletions",
        );
        check(
            "splitLineCount",
            hunk.split_line_count,
            expected_split_line_count,
            "calculated from hunkContent",
        );
        check(
            "unifiedLineCount",
            hunk.unified_line_count,
            expected_unified_line_count,
            "calculated from hunkContent",
        );
        // Verify splitLineStart is cumulative
        check(
            "splitLineStart",
            hunk.split_line_start,
            current_split_line_total + hunk.collapsed_before,
            "expected cumulative",
        );
        current_split_line_total += hunk.collapsed_before + hunk.split_line_count;
        // Verify unifiedLineStart is cumulative
        check(
            "unifiedLineStart",
            hunk.unified_line_start,
            current_unified_line_total + hunk.collapsed_before,
            "expected cumulative",
        );
        current_unified_line_total += hunk.collapsed_before + hunk.unified_line_count;

        // A zero-count range starts after additionStart instead of on it.
        let expected_collapsed_before = (hunk.addition_start as isize
            - isize::from(hunk.addition_count != 0)
            - last_hunk_addition_end as isize)
            .max(0) as usize;
        check(
            "collapsedBefore",
            hunk.collapsed_before,
            expected_collapsed_before,
            "expected",
        );
        last_hunk_addition_end = if hunk.addition_count == 0 {
            hunk.addition_start
        } else {
            hunk.addition_start + hunk.addition_count - 1
        };
    }

    // Verify file-level totals
    let mut expected_total_split_lines: usize = file
        .hunks
        .iter()
        .map(|hunk| hunk.collapsed_before + hunk.split_line_count)
        .sum();
    let mut expected_total_unified_lines: usize = file
        .hunks
        .iter()
        .map(|hunk| hunk.collapsed_before + hunk.unified_line_count)
        .sum();

    // Account for collapsed lines after the final hunk (only for non-partial diffs)
    if let Some(last_hunk) = file.hunks.last()
        && !file.is_partial
    {
        // Clamp to 0: a diff whose addition side is empty (file deleted to
        // nothing) has additionStart 0 / additionCount 0, and without the clamp
        // the -1 end would invent a phantom trailing context line
        let last_hunk_end = if last_hunk.addition_count == 0 {
            last_hunk.addition_start
        } else {
            last_hunk.addition_start + last_hunk.addition_count - 1
        };
        let collapsed_after = file.addition_lines.len().saturating_sub(last_hunk_end);
        expected_total_split_lines += collapsed_after;
        expected_total_unified_lines += collapsed_after;
    }

    if file.split_line_count != expected_total_split_lines {
        errors.push(format!(
            "{prefix}: splitLineCount ({}) !== sum of hunk splitLineCounts ({expected_total_split_lines})",
            file.split_line_count
        ));
    }
    if file.unified_line_count != expected_total_unified_lines {
        errors.push(format!(
            "{prefix}: unifiedLineCount ({}) !== sum of hunk unifiedLineCounts ({expected_total_unified_lines})",
            file.unified_line_count
        ));
    }
    errors
}

pub fn verify_patch_hunk_values(patches: &[ParsedPatch]) -> VerifyResult {
    let mut errors = Vec::new();
    for (patch_index, patch) in patches.iter().enumerate() {
        for (file_index, file) in patch.files.iter().enumerate() {
            let prefix = format!("patch[{patch_index}].files[{file_index}] ({})", file.name);
            errors.extend(verify_hunk_line_values(file, &prefix));
        }
    }
    VerifyResult {
        valid: errors.is_empty(),
        errors,
    }
}

pub fn verify_file_diff_hunk_values(diff: &FileDiffMetadata) -> VerifyResult {
    let errors = verify_hunk_line_values(diff, "file");
    VerifyResult {
        valid: errors.is_empty(),
        errors,
    }
}

// Number of rows a full render or iteration emits for a diff: each hunk's
// declared rows plus any collapsed gap at or under the collapsed-context
// threshold, which is rendered as auto-expanded context rows instead of being
// hidden behind a separator.
pub fn count_declared_rows(
    diff: &FileDiffMetadata,
    split: bool,
    collapsed_context_threshold: usize,
) -> usize {
    diff.hunks
        .iter()
        .map(|hunk| {
            let declared = if split {
                hunk.split_line_count
            } else {
                hunk.unified_line_count
            };
            let auto_expanded =
                hunk.collapsed_before > 0 && hunk.collapsed_before <= collapsed_context_threshold;
            declared
                + if auto_expanded {
                    hunk.collapsed_before
                } else {
                    0
                }
        })
        .sum()
}
