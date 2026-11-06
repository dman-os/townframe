// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/realignChangeContent.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: only `realignChangeContentBySimilarity` (what `parsePatchFiles`
// calls) is ported. `slideBlankBoundaryBlocksUp` is used only by the editor
// (edit mode is out of scope; see PARITY.md). Line lengths and prefix/suffix
// scans are over UTF-16 code units, as `String.length` and `a[i]` are in
// JavaScript, so similarity scores match for astral characters too.

use crate::js_text;
use crate::types::patch::ChangeContent;
use crate::types::patch::FileDiffMetadata;
use crate::types::patch::HunkContent;

// The diff library emits one change block per replaced run, ordering every
// deleted line before every added line. Renderers pair a block's lines
// positionally (deletion[i] across from addition[i] in split view), so a
// block like { deletions: 1, additions: 2 } pairs the deleted line with
// whichever addition happens to come first -- even when a later addition is
// the edited version of it (e.g. pressing Enter above a changed line pushes
// a blank line in front of it). These helpers re-split such blocks so the
// most similar lines pair up and the surplus renders as pure insert/delete
// rows at the block's edges.

// Skip realignment when a block would need more than this many line
// comparisons; pathological blocks keep the library's positional pairing.
const MAX_ALIGNMENT_COMPARISONS: usize = 4096;

// A shifted pairing must beat the positional one by this much per paired
// line before the block is re-split. Near-ties (e.g. lines that merely share
// an import-statement shape) keep the library's canonical order; the
// realignment is meant for decisive wins like a blank or unrelated inserted
// line displacing an edited one, where the gap approaches 1.
const MIN_IMPROVEMENT_PER_PAIR: f64 = 0.5;

/// Re-split count-mismatched change blocks in every hunk so paired lines are
/// chosen by content similarity instead of position. Mutates `hunks` in place
/// without moving changes across context lines; rendered row counts are
/// unchanged (a split block covers the same split/unified rows as the original).
pub fn realign_change_content_by_similarity(diff: &mut FileDiffMetadata) {
    let FileDiffMetadata {
        hunks,
        addition_lines,
        deletion_lines,
        ..
    } = diff;
    for hunk in hunks {
        let mut index = 0;
        while index < hunk.hunk_content.len() {
            if let HunkContent::Change(content) = &hunk.hunk_content[index]
                && let Some(replacement) =
                    realign_change_block(addition_lines, deletion_lines, content)
            {
                let count = replacement.len();
                hunk.hunk_content.splice(index..=index, replacement);
                index += count;
            } else {
                index += 1;
            }
        }
    }
}

// Returns the split blocks for one change block, or None when the block is
// balanced, too large to scan, or already best paired positionally.
fn realign_change_block(
    addition_lines: &[String],
    deletion_lines: &[String],
    content: &ChangeContent,
) -> Option<Vec<HunkContent>> {
    let ChangeContent {
        deletions,
        additions,
        deletion_line_index,
        addition_line_index,
    } = *content;
    let pair_count = deletions.min(additions);
    let surplus = additions.abs_diff(deletions);
    if pair_count == 0 || surplus == 0 || pair_count * (surplus + 1) > MAX_ALIGNMENT_COMPARISONS {
        return None;
    }

    // Whitespace (indentation, formatter churn, trailing spaces, the line
    // break itself) is noise for deciding which lines pair -- the rendered diff
    // still shows every whitespace change on the paired row. Strip it once per
    // line here rather than per comparison.
    let stripped = |lines: &[String], start: usize, count: usize| -> Vec<Vec<u16>> {
        (0..count)
            .map(|line| strip_whitespace(lines.get(start + line).map_or("", String::as_str)))
            .collect()
    };
    let stripped_deletions = stripped(deletion_lines, deletion_line_index, deletions);
    let stripped_additions = stripped(addition_lines, addition_line_index, additions);

    // Score every offset of the shorter side along the longer side and keep
    // the best one only when it decisively beats the positional pairing.
    let additions_are_longer = additions > deletions;
    let mut best_offset = 0;
    let mut best_score = -1.0;
    for offset in 0..=surplus {
        let mut score = 0.0;
        for pair in 0..pair_count {
            score += line_similarity(
                &stripped_deletions[pair + if additions_are_longer { 0 } else { offset }],
                &stripped_additions[pair + if additions_are_longer { offset } else { 0 }],
            );
        }
        if offset == 0 {
            best_score = score + pair_count as f64 * MIN_IMPROVEMENT_PER_PAIR;
        } else if score > best_score {
            best_score = score;
            best_offset = offset;
        }
    }
    if best_offset == 0 {
        return None;
    }

    let mut blocks: Vec<HunkContent> = Vec::new();
    let mut push_block = |block_deletions: usize,
                          block_additions: usize,
                          block_deletion_index: usize,
                          block_addition_index: usize| {
        if block_deletions > 0 || block_additions > 0 {
            blocks.push(HunkContent::Change(ChangeContent {
                deletions: block_deletions,
                additions: block_additions,
                deletion_line_index: block_deletion_index,
                addition_line_index: block_addition_index,
            }));
        }
    };
    if additions_are_longer {
        push_block(0, best_offset, deletion_line_index, addition_line_index);
        push_block(
            pair_count,
            pair_count,
            deletion_line_index,
            addition_line_index + best_offset,
        );
        push_block(
            0,
            additions - pair_count - best_offset,
            deletion_line_index + pair_count,
            addition_line_index + best_offset + pair_count,
        );
    } else {
        push_block(best_offset, 0, deletion_line_index, addition_line_index);
        push_block(
            pair_count,
            pair_count,
            deletion_line_index + best_offset,
            addition_line_index,
        );
        push_block(
            deletions - pair_count - best_offset,
            0,
            deletion_line_index + best_offset + pair_count,
            addition_line_index + pair_count,
        );
    }
    Some(blocks)
}

/// `line.replace(/\s+/g, '')`, as UTF-16 code units.
fn strip_whitespace(line: &str) -> Vec<u16> {
    line.chars()
        .filter(|&ch| !js_text::is_whitespace(ch))
        .flat_map(|ch| {
            let mut units = [0u16; 2];
            ch.encode_utf16(&mut units).to_vec()
        })
        .collect()
}

// Cheap 0..1 similarity over whitespace-stripped lines: shared prefix plus
// shared suffix over the longer length. Exact for lines that differ only in
// whitespace, 0 for a blank against content -- enough to steer pairing
// without a real edit-distance pass.
fn line_similarity(first: &[u16], second: &[u16]) -> f64 {
    if first == second {
        return 1.0;
    }
    let max_length = first.len().max(second.len());
    let min_length = first.len().min(second.len());
    if min_length == 0 {
        return 0.0;
    }
    let mut prefix = 0;
    while prefix < min_length && first[prefix] == second[prefix] {
        prefix += 1;
    }
    let mut suffix = 0;
    while suffix < min_length - prefix
        && first[first.len() - 1 - suffix] == second[second.len() - 1 - suffix]
    {
        suffix += 1;
    }
    (prefix + suffix) as f64 / max_length as f64
}
