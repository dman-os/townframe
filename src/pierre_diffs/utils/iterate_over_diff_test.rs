// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/iterateOverDiff.test.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: bun:test to nextest; test titles are snake_case. Upstream's
// `fileOld`/`fileNew` fixtures live in `apps/demo` (absent from the extracted
// tree), so the tests that used them run on `big_fixture_diff`, a generated
// 600-line pair with the geometry the upstream comments rely on (a leading
// gap above the threshold, single-line gaps at the threshold, paired and pure
// change blocks); their assertions are unchanged properties, not recorded
// numbers. The hand-built diffs (`createSingleHunkDiff`,
// `createWindowedSeparatorDiff`) are ported verbatim.

use std::collections::HashMap;

use crate::constants::DEFAULT_COLLAPSED_CONTEXT_THRESHOLD;
use crate::diff::PatchOptions;
use crate::test_utils::count_declared_rows;
use crate::types::lines::ExpandedHunks;
use crate::types::lines::HunkExpansionRegion;
use crate::types::patch::ChangeContent;
use crate::types::patch::ChangeTypes;
use crate::types::patch::ContextContent;
use crate::types::patch::DiffFileInput;
use crate::types::patch::FileContents;
use crate::types::patch::FileDiffMetadata;
use crate::types::patch::Hunk;
use crate::types::patch::HunkContent;
use crate::types::patch::ParseDiffOptions;
use crate::types::patch::signed;
use crate::utf16;
use crate::utils::iterate_over_diff::DiffLineCallbackProps;
use crate::utils::iterate_over_diff::DiffLineMetadata;
use crate::utils::iterate_over_diff::DiffLineType;
use crate::utils::iterate_over_diff::IterateOverDiffProps;
use crate::utils::iterate_over_diff::IterationDiffStyle;
use crate::utils::iterate_over_diff::iterate_over_diff;
use crate::utils::parse_diff_from_file::parse_diff_from_file;

const COLLAPSED_BEFORE: usize = 10;

#[derive(Debug, Clone, PartialEq, Eq)]
struct RowSnapshot {
    line_type: DiffLineType,
    hunk_index: usize,
    collapsed_before: usize,
    collapsed_after: usize,
    deletion_line: Option<DiffLineMetadata>,
    addition_line: Option<DiffLineMetadata>,
}

fn serialize_row(props: &DiffLineCallbackProps<'_>) -> RowSnapshot {
    RowSnapshot {
        line_type: props.line_type,
        hunk_index: props.hunk_index,
        collapsed_before: props.collapsed_before,
        collapsed_after: props.collapsed_after,
        deletion_line: props.deletion_line,
        addition_line: props.addition_line,
    }
}

#[derive(Clone, Copy)]
struct Options<'diff> {
    diff: &'diff FileDiffMetadata,
    diff_style: IterationDiffStyle,
    starting_line: usize,
    total_lines: Option<usize>,
    expanded_hunks: Option<&'diff ExpandedHunks>,
}

impl<'diff> Options<'diff> {
    fn new(diff: &'diff FileDiffMetadata, diff_style: IterationDiffStyle) -> Self {
        Self {
            diff,
            diff_style,
            starting_line: 0,
            total_lines: None,
            expanded_hunks: None,
        }
    }

    fn window(self, starting_line: usize, total_lines: usize) -> Self {
        Self {
            starting_line,
            total_lines: Some(total_lines),
            ..self
        }
    }

    fn expanded(self, expanded_hunks: &'diff ExpandedHunks) -> Self {
        Self {
            expanded_hunks: Some(expanded_hunks),
            ..self
        }
    }

    fn props(self) -> IterateOverDiffProps<'diff> {
        IterateOverDiffProps {
            starting_line: self.starting_line,
            total_lines: self.total_lines,
            expanded_hunks: self.expanded_hunks,
            ..IterateOverDiffProps::new(self.diff, self.diff_style)
        }
    }
}

fn collect_rows(options: Options<'_>) -> Vec<RowSnapshot> {
    let mut rows = Vec::new();
    iterate_over_diff(options.props(), |props| {
        rows.push(serialize_row(&props));
        false
    });
    rows
}

fn regions(entries: &[(usize, usize, usize)]) -> ExpandedHunks {
    ExpandedHunks::Regions(
        entries
            .iter()
            .map(|&(index, from_start, from_end)| {
                (
                    index,
                    HunkExpansionRegion {
                        from_start,
                        from_end,
                    },
                )
            })
            .collect::<HashMap<_, _>>(),
    )
}

fn row_identities(
    rows: &[RowSnapshot],
) -> Vec<(
    DiffLineType,
    usize,
    Option<DiffLineMetadata>,
    Option<DiffLineMetadata>,
)> {
    rows.iter()
        .map(|row| {
            (
                row.line_type,
                row.hunk_index,
                row.deletion_line,
                row.addition_line,
            )
        })
        .collect()
}

fn context(lines: usize, deletion_line_index: usize, addition_line_index: usize) -> HunkContent {
    HunkContent::Context(ContextContent {
        lines,
        addition_line_index: signed(addition_line_index),
        deletion_line_index: signed(deletion_line_index),
    })
}

fn change(
    deletions: usize,
    deletion_line_index: usize,
    additions: usize,
    addition_line_index: usize,
) -> HunkContent {
    HunkContent::Change(ChangeContent {
        deletions,
        deletion_line_index: signed(deletion_line_index),
        additions,
        addition_line_index: signed(addition_line_index),
    })
}

fn is_paired_change_block(content: &HunkContent) -> bool {
    matches!(content, HunkContent::Change(change) if change.deletions > 0 && change.additions > 0)
}

struct Counts {
    addition_count: usize,
    deletion_count: usize,
    addition_lines: usize,
    deletion_lines: usize,
    split_line_count: usize,
    unified_line_count: usize,
}

fn get_hunk_content_counts(hunk_content: &[HunkContent]) -> Counts {
    let mut counts = Counts {
        addition_count: 0,
        deletion_count: 0,
        addition_lines: 0,
        deletion_lines: 0,
        split_line_count: 0,
        unified_line_count: 0,
    };
    for content in hunk_content {
        match content {
            HunkContent::Context(context) => {
                counts.addition_count += context.lines;
                counts.deletion_count += context.lines;
                counts.split_line_count += context.lines;
                counts.unified_line_count += context.lines;
            }
            HunkContent::Change(change) => {
                counts.addition_count += change.additions;
                counts.deletion_count += change.deletions;
                counts.addition_lines += change.additions;
                counts.deletion_lines += change.deletions;
                counts.split_line_count += change.additions.max(change.deletions);
                counts.unified_line_count += change.additions + change.deletions;
            }
        }
    }
    counts
}

fn create_lines(count: usize) -> Vec<String> {
    (0..count).map(|index| format!("line {index}\n")).collect()
}

/// Shared body of `createSingleHunkDiff` and `createWindowedSeparatorDiff`.
fn build_hunk_diff(
    name: &str,
    collapsed_before: usize,
    hunk_content: Vec<HunkContent>,
    trailing_line_count: usize,
) -> FileDiffMetadata {
    let counts = get_hunk_content_counts(&hunk_content);
    let hunk = Hunk {
        collapsed_before,
        addition_start: collapsed_before + 1,
        addition_count: counts.addition_count,
        addition_lines: counts.addition_lines,
        addition_line_index: signed(collapsed_before),
        deletion_start: collapsed_before + 1,
        deletion_count: counts.deletion_count,
        deletion_lines: counts.deletion_lines,
        deletion_line_index: signed(collapsed_before),
        hunk_content,
        hunk_context: None,
        hunk_specs: Some(format!(
            "@@ -{},{} +{},{} @@",
            collapsed_before + 1,
            counts.deletion_count,
            collapsed_before + 1,
            counts.addition_count
        )),
        split_line_start: collapsed_before,
        split_line_count: counts.split_line_count,
        unified_line_start: collapsed_before,
        unified_line_count: counts.unified_line_count,
        no_eofcr_deletions: false,
        no_eofcr_additions: false,
    };
    let deletion_lines =
        create_lines(collapsed_before + counts.deletion_count + trailing_line_count);
    let addition_lines =
        create_lines(collapsed_before + counts.addition_count + trailing_line_count);
    FileDiffMetadata {
        name: name.to_owned(),
        prev_name: None,
        lang: None,
        new_object_id: None,
        prev_object_id: None,
        mode: None,
        prev_mode: None,
        change_type: ChangeTypes::Change,
        hunks: vec![hunk],
        split_line_count: collapsed_before + counts.split_line_count + trailing_line_count,
        unified_line_count: collapsed_before + counts.unified_line_count + trailing_line_count,
        is_partial: false,
        deletion_character_count: utf16::len(&deletion_lines.concat()),
        addition_character_count: utf16::len(&addition_lines.concat()),
        deletion_lines,
        addition_lines,
        edit_session_dirty: None,
        cache_key: None,
    }
}

fn create_single_hunk_diff(
    collapsed_before: usize,
    hunk_content: Vec<HunkContent>,
    trailing_line_count: usize,
) -> FileDiffMetadata {
    build_hunk_diff(
        "single-hunk.ts",
        collapsed_before,
        hunk_content,
        trailing_line_count,
    )
}

// Build a minimal full-file diff where a collapsed leading gap can be partially
// expanded, letting windowed iteration start after the separator boundary.
fn create_windowed_separator_diff(hunk_content: Vec<HunkContent>) -> FileDiffMetadata {
    build_hunk_diff("windowed-separator.ts", COLLAPSED_BEFORE, hunk_content, 0)
}

/// Stand-in for the demo fixture: a leading gap of 3 (above the threshold),
/// three single-line gaps (changes 10 lines apart), replaced, inserted and
/// deleted lines, then widely spaced edits out to line 600.
fn big_fixture_diff() -> FileDiffMetadata {
    let old: Vec<String> = (1..=600).map(|index| format!("line {index}\n")).collect();
    let mut new = old.clone();
    // Single-line gaps: edits at 8, 18, 28, 38 (10 apart, 4 lines of context each side).
    for line in [8, 18, 28, 38] {
        new[line - 1] = format!("edited {line}\n");
    }
    // Widely spaced mixed edits: a replacement, then a growing pure insertion, then a deletion.
    for line in (80..=580).step_by(25) {
        new[line - 1] = format!("replaced {line}\n");
    }
    new[200] = String::from("inserted a\ninserted b\ninserted c\n");
    new.remove(301);
    new.remove(301);
    let input = DiffFileInput::Both {
        old_file: FileContents {
            name: String::from("test.txt"),
            contents: old.concat(),
            lang: None,
            header: None,
            cache_key: None,
        },
        new_file: FileContents {
            name: String::from("test.txt"),
            contents: new.concat(),
            lang: None,
            header: None,
            cache_key: None,
        },
    };
    parse_diff_from_file(&input, &ParseDiffOptions::default(), true)
        .expect("valid input")
        .value
}

#[test]
fn unified_iteration_produces_expected_sequence() {
    let diff = big_fixture_diff();
    let rows = collect_rows(Options::new(&diff, IterationDiffStyle::Unified));

    // The iterator must emit exactly the rows the hunk metadata declares plus
    // every collapsed gap at or under DEFAULT_COLLAPSED_CONTEXT_THRESHOLD,
    // which is emitted as auto-expanded context rows.
    assert_eq!(
        rows.len(),
        count_declared_rows(&diff, false, DEFAULT_COLLAPSED_CONTEXT_THRESHOLD)
    );

    // Hunk 0's collapsedBefore exceeds the threshold, so its gap stays
    // collapsed: the first emitted row is hunk 0's first declared context row,
    // positioned at unifiedLineStart (not a sequential counter) and carrying
    // the collapsed separator size.
    let first_hunk = &diff.hunks[0];
    assert!(first_hunk.collapsed_before > DEFAULT_COLLAPSED_CONTEXT_THRESHOLD);
    let first = &rows[0];
    assert_eq!(first.line_type, DiffLineType::Context);
    assert_eq!(first.hunk_index, 0);
    assert_eq!(
        first
            .addition_line
            .or(first.deletion_line)
            .map(|line| line.unified_line_index),
        Some(first_hunk.unified_line_start)
    );
    assert_eq!(first.collapsed_before, first_hunk.collapsed_before);
    // The fixture must exercise the auto-expanded single-line gaps.
    assert!(
        diff.hunks
            .iter()
            .filter(|hunk| hunk.collapsed_before == 1)
            .count()
            >= 3
    );
}

#[test]
fn split_iteration_produces_expected_sequence() {
    let diff = big_fixture_diff();
    let rows = collect_rows(Options::new(&diff, IterationDiffStyle::Split));

    // Same row contract as unified mode, in split coordinates.
    assert_eq!(
        rows.len(),
        count_declared_rows(&diff, true, DEFAULT_COLLAPSED_CONTEXT_THRESHOLD)
    );

    // Split-specific pairing: a change block emits max(deletions, additions)
    // rows, the deletion and addition sides of one row share a splitLineIndex,
    // and the longer side then continues alone. Locate the first change block
    // that pairs both sides and derive its emitted-row position from hunk
    // metadata.
    let paired_hunk_index = diff
        .hunks
        .iter()
        .position(|hunk| hunk.hunk_content.iter().any(is_paired_change_block))
        .expect("fixture must contain a paired change block");
    let paired_hunk = &diff.hunks[paired_hunk_index];

    let mut block_row_offset = 0;
    let mut paired_block: Option<&ChangeContent> = None;
    for content in &paired_hunk.hunk_content {
        match content {
            HunkContent::Change(change) if is_paired_change_block(content) => {
                paired_block = Some(change);
                break;
            }
            HunkContent::Context(context) => block_row_offset += context.lines,
            HunkContent::Change(change) => {
                block_row_offset += change.deletions.max(change.additions);
            }
        }
    }
    let ChangeContent {
        deletions,
        additions,
        ..
    } = *paired_block.expect("paired change block must exist in this hunk");

    // Rows emitted before the block: every earlier hunk's declared split rows
    // and auto-expanded gaps, this hunk's own gap if it auto-expands, then the
    // hunk-content rows preceding the block.
    let earlier = FileDiffMetadata {
        hunks: diff.hunks[..paired_hunk_index].to_vec(),
        ..diff.clone()
    };
    let block_row_start = count_declared_rows(&earlier, true, DEFAULT_COLLAPSED_CONTEXT_THRESHOLD)
        + if paired_hunk.collapsed_before <= DEFAULT_COLLAPSED_CONTEXT_THRESHOLD {
            paired_hunk.collapsed_before
        } else {
            0
        }
        + block_row_offset;
    let block_split_start = paired_hunk.split_line_start + block_row_offset;
    let block_row_count = deletions.max(additions);

    let actual: Vec<(DiffLineType, Option<usize>, Option<usize>)> = rows
        [block_row_start..block_row_start + block_row_count]
        .iter()
        .map(|row| {
            (
                row.line_type,
                row.deletion_line.map(|line| line.split_line_index),
                row.addition_line.map(|line| line.split_line_index),
            )
        })
        .collect();
    let expected: Vec<(DiffLineType, Option<usize>, Option<usize>)> = (0..block_row_count)
        .map(|index| {
            (
                DiffLineType::Change,
                (index < deletions).then_some(block_split_start + index),
                (index < additions).then_some(block_split_start + index),
            )
        })
        .collect();
    assert_eq!(actual, expected);
}

fn unified_index(row: &RowSnapshot) -> usize {
    row.addition_line
        .or(row.deletion_line)
        .map_or(0, |line| line.unified_line_index)
}

#[test]
fn windowing_skips_lines_correctly() {
    let diff = big_fixture_diff();
    let results: Vec<usize> =
        collect_rows(Options::new(&diff, IterationDiffStyle::Unified).window(10, 5))
            .iter()
            .map(unified_index)
            .collect();

    // Should get exactly 5 consecutive lines
    assert_eq!(results.len(), 5);
    // Lines should be consecutive
    for pair in results.windows(2) {
        assert_eq!(pair[1], pair[0] + 1);
    }
}

#[test]
fn windowed_iteration_matches_full_iteration_slices_for_unified_and_split_styles() {
    let diff = big_fixture_diff();
    let cases = [
        (IterationDiffStyle::Unified, 0, 8),
        (IterationDiffStyle::Unified, 10, 5),
        (IterationDiffStyle::Unified, 150, 13),
        (IterationDiffStyle::Split, 0, 8),
        (IterationDiffStyle::Split, 10, 5),
        (IterationDiffStyle::Split, 120, 17),
    ];
    for (diff_style, starting_line, total_lines) in cases {
        let full_rows = collect_rows(Options::new(&diff, diff_style));
        let windowed_rows =
            collect_rows(Options::new(&diff, diff_style).window(starting_line, total_lines));
        assert_eq!(
            windowed_rows,
            full_rows[starting_line..starting_line + total_lines],
            "{diff_style:?} {starting_line} {total_lines}"
        );
    }
}

#[test]
fn windowed_expanded_iteration_matches_full_iteration_line_identities_for_unified_and_split_styles()
{
    let expanded_diff = create_windowed_separator_diff(vec![
        context(2, COLLAPSED_BEFORE, COLLAPSED_BEFORE),
        change(2, COLLAPSED_BEFORE + 2, 3, COLLAPSED_BEFORE + 2),
        context(4, COLLAPSED_BEFORE + 4, COLLAPSED_BEFORE + 5),
    ]);
    let cases = [
        (
            "fromStart only",
            regions(&[(0, 2, 0)]),
            vec![0, 1, 2, 3, 4, 6],
        ),
        (
            "fromEnd only",
            regions(&[(0, 0, 3)]),
            vec![0, 1, 2, 3, 4, 6],
        ),
        (
            "fromStart and fromEnd",
            regions(&[(0, 2, 3)]),
            vec![0, 1, 2, 3, 4, 5, 8],
        ),
    ];
    for diff_style in [IterationDiffStyle::Unified, IterationDiffStyle::Split] {
        for (name, expanded_hunks, starting_lines) in &cases {
            let full_rows =
                collect_rows(Options::new(&expanded_diff, diff_style).expanded(expanded_hunks));
            for &starting_line in starting_lines {
                let total_lines = 3.min(full_rows.len() - starting_line);
                let windowed_rows = collect_rows(
                    Options::new(&expanded_diff, diff_style)
                        .expanded(expanded_hunks)
                        .window(starting_line, total_lines),
                );
                assert_eq!(
                    row_identities(&windowed_rows),
                    row_identities(&full_rows[starting_line..starting_line + total_lines]),
                    "{name} {diff_style:?} {starting_line}"
                );
            }
        }
    }
}

#[test]
fn both_style_includes_rows_visible_in_either_split_or_unified_coordinates_for_uneven_changes() {
    let uneven_diff = create_single_hunk_diff(0, vec![context(2, 0, 0), change(4, 2, 1, 2)], 0);
    let rows = collect_rows(Options::new(&uneven_diff, IterationDiffStyle::Both).window(6, 1));

    assert_eq!(
        rows,
        [RowSnapshot {
            line_type: DiffLineType::Change,
            hunk_index: 0,
            collapsed_before: 0,
            collapsed_after: 0,
            deletion_line: Some(DiffLineMetadata {
                unified_line_index: 2,
                split_line_index: 2,
                line_index: 2,
                line_number: 3,
                no_eofcr: false,
            }),
            addition_line: Some(DiffLineMetadata {
                unified_line_index: 6,
                split_line_index: 2,
                line_index: 2,
                line_number: 3,
                no_eofcr: false,
            }),
        }]
    );
}

#[test]
fn both_style_skips_gap_rows_between_disjoint_split_and_unified_equal_row_windows() {
    let gap_diff = create_single_hunk_diff(0, vec![change(5, 0, 5, 0), context(10, 5, 5)], 0);
    let rows = collect_rows(Options::new(&gap_diff, IterationDiffStyle::Both).window(12, 1));

    let summary: Vec<_> = rows
        .iter()
        .map(|row| {
            let line = row.deletion_line.expect("context rows have both sides");
            (
                row.line_type,
                line.unified_line_index,
                line.split_line_index,
                line.line_index,
            )
        })
        .collect();
    assert_eq!(
        summary,
        [
            (DiffLineType::Context, 12, 7, 7),
            (DiffLineType::Context, 17, 12, 12),
        ]
    );
}

#[test]
fn expanded_leading_context_preserves_from_start_and_from_end_placement() {
    let leading_diff = create_single_hunk_diff(6, vec![change(1, 6, 1, 6)], 0);
    let expanded = regions(&[(0, 2, 2)]);
    let rows =
        collect_rows(Options::new(&leading_diff, IterationDiffStyle::Unified).expanded(&expanded));

    let summary: Vec<_> = rows[..5]
        .iter()
        .map(|row| {
            (
                row.line_type,
                row.collapsed_before,
                row.deletion_line.map(|line| line.line_index),
                row.addition_line.map(|line| line.line_index),
            )
        })
        .collect();
    assert_eq!(
        summary,
        [
            (DiffLineType::ContextExpanded, 0, Some(0), Some(0)),
            (DiffLineType::ContextExpanded, 0, Some(1), Some(1)),
            (DiffLineType::ContextExpanded, 2, Some(4), Some(4)),
            (DiffLineType::ContextExpanded, 0, Some(5), Some(5)),
            (DiffLineType::Change, 0, Some(6), None),
        ]
    );
}

#[test]
fn trailing_context_is_collapsed_after_hunk_content_or_emitted_when_expanded() {
    let trailing_diff = create_single_hunk_diff(0, vec![context(2, 0, 0)], 5);

    let collapsed_rows = collect_rows(Options::new(&trailing_diff, IterationDiffStyle::Unified));
    assert_eq!(collapsed_rows.len(), 2);
    assert_eq!(
        collapsed_rows.last().map(|row| row.collapsed_after),
        Some(5)
    );

    let expanded = regions(&[(1, 2, 1)]);
    let expanded_rows =
        collect_rows(Options::new(&trailing_diff, IterationDiffStyle::Unified).expanded(&expanded));
    let summary: Vec<_> = expanded_rows[2..]
        .iter()
        .map(|row| {
            (
                row.line_type,
                row.collapsed_after,
                row.deletion_line.map(|line| line.line_index),
                row.addition_line.map(|line| line.line_index),
            )
        })
        .collect();
    assert_eq!(
        summary,
        [
            (DiffLineType::ContextExpanded, 0, Some(2), Some(2)),
            (DiffLineType::ContextExpanded, 3, Some(3), Some(3)),
        ]
    );
}

#[test]
fn expands_every_new_file_line_around_a_context_zero_deletion() {
    let old_lines: Vec<String> = (1..=7).map(|index| format!("line {index}\n")).collect();
    let mut new_lines = old_lines.clone();
    new_lines.remove(4);
    let input = DiffFileInput::Both {
        old_file: FileContents {
            name: String::from("deletion.ts"),
            contents: old_lines.concat(),
            lang: None,
            header: None,
            cache_key: None,
        },
        new_file: FileContents {
            name: String::from("deletion.ts"),
            contents: new_lines.concat(),
            lang: None,
            header: None,
            cache_key: None,
        },
    };
    let deletion_diff = parse_diff_from_file(
        &input,
        &ParseDiffOptions {
            create_patch: PatchOptions {
                context: Some(0),
                ..PatchOptions::default()
            },
            disable_line_matching: None,
        },
        false,
    )
    .expect("valid")
    .value;

    let all = ExpandedHunks::All;
    let additions: Vec<(usize, usize, String)> =
        collect_rows(Options::new(&deletion_diff, IterationDiffStyle::Split).expanded(&all))
            .iter()
            .filter_map(|row| row.addition_line)
            .map(|line| {
                (
                    line.line_index,
                    line.line_number,
                    deletion_diff.addition_lines[line.line_index].clone(),
                )
            })
            .collect();

    let expected: Vec<(usize, usize, String)> = new_lines
        .iter()
        .enumerate()
        .map(|(line_index, text)| (line_index, line_index + 1, text.clone()))
        .collect();
    assert_eq!(additions, expected);
}

#[test]
fn windowed_iteration_preserves_collapsed_before_and_collapsed_after_separator_placement() {
    let leading_diff =
        create_windowed_separator_diff(vec![context(3, COLLAPSED_BEFORE, COLLAPSED_BEFORE)]);
    let expanded = regions(&[(0, 2, 0)]);
    let leading_rows = collect_rows(
        Options::new(&leading_diff, IterationDiffStyle::Unified)
            .expanded(&expanded)
            .window(3, 1),
    );
    assert_eq!(leading_rows.len(), 1);
    assert_eq!(leading_rows[0].line_type, DiffLineType::Context);
    assert_eq!(leading_rows[0].collapsed_before, 0);

    let trailing_diff = create_single_hunk_diff(0, vec![context(3, 0, 0)], 4);
    let trailing_rows =
        collect_rows(Options::new(&trailing_diff, IterationDiffStyle::Unified).window(2, 1));
    assert_eq!(trailing_rows.len(), 1);
    assert_eq!(trailing_rows[0].line_type, DiffLineType::Context);
    assert_eq!(trailing_rows[0].collapsed_after, 4);
}

#[test]
fn callback_can_stop_iteration_early() {
    let diff = big_fixture_diff();
    let mut rows: Vec<RowSnapshot> = Vec::new();
    iterate_over_diff(
        Options::new(&diff, IterationDiffStyle::Unified).props(),
        |props| {
            rows.push(serialize_row(&props));
            rows.len() == 3
        },
    );

    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows,
        collect_rows(Options::new(&diff, IterationDiffStyle::Unified))[..rows.len()]
    );
}

#[test]
fn windowed_expansion_does_not_attach_skipped_collapsed_separators_to_visible_rows() {
    let single_context = |lines: usize| {
        create_windowed_separator_diff(vec![context(lines, COLLAPSED_BEFORE, COLLAPSED_BEFORE)])
    };
    let cases = [
        (
            "expanded fromEnd context",
            single_context(1),
            regions(&[(0, 2, 3)]),
            DiffLineType::ContextExpanded,
        ),
        (
            "hunk context content",
            single_context(3),
            regions(&[(0, 2, 0)]),
            DiffLineType::Context,
        ),
        (
            "hunk context after expanded fromEnd boundary",
            single_context(3),
            regions(&[(0, 0, 3)]),
            DiffLineType::Context,
        ),
        (
            "hunk change content",
            create_windowed_separator_diff(vec![change(3, COLLAPSED_BEFORE, 3, COLLAPSED_BEFORE)]),
            regions(&[(0, 2, 0)]),
            DiffLineType::Change,
        ),
    ];
    for (name, diff, expanded_hunks, expected_type) in &cases {
        for diff_style in [IterationDiffStyle::Unified, IterationDiffStyle::Split] {
            let rows = collect_rows(
                Options::new(diff, diff_style)
                    .expanded(expanded_hunks)
                    .window(3, 1),
            );
            let summary: Vec<_> = rows
                .iter()
                .map(|row| (row.line_type, row.collapsed_before))
                .collect();
            assert_eq!(summary, [(*expected_type, 0)], "{name} {diff_style:?}");
        }
    }
}
