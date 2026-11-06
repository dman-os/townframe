// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/isAdditionLineRenderable.test.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: bun:test to nextest; test titles are snake_case; the
// `Map<number, HunkExpansionRegion>` arguments are `ExpandedHunks::Regions`.
// The fixture diff (a 40-line file with lines 10 and 30 changed) has one
// minimal diff, so `similar`'s hunks equal jsdiff's and no assertion changes.

use std::collections::HashMap;
use std::collections::HashSet;

use crate::types::lines::ExpandedHunks;
use crate::types::lines::HunkExpansionRegion;
use crate::types::patch::DiffFileInput;
use crate::types::patch::FileContents;
use crate::types::patch::FileDiffMetadata;
use crate::types::patch::ParseDiffOptions;
use crate::utils::iterate_over_diff::IterateOverDiffProps;
use crate::utils::iterate_over_diff::IterationDiffStyle;
use crate::utils::iterate_over_diff::iterate_over_diff;
use crate::utils::parse_diff_from_file::parse_diff_from_file;
use crate::utils::virtual_diff_layout::IsAdditionLineRenderableProps;
use crate::utils::virtual_diff_layout::RenderableDirection;
use crate::utils::virtual_diff_layout::get_nearest_renderable_addition_line;
use crate::utils::virtual_diff_layout::is_addition_line_renderable;

const COLLAPSED_CONTEXT_THRESHOLD: usize = 1;

fn make_diff() -> FileDiffMetadata {
    let old_contents: String = (1..=40).map(|index| format!("line {index}\n")).collect();
    let new_contents = old_contents
        .replace("line 10\n", "line 10 changed\n")
        .replace("line 30\n", "line 30 changed\n");
    let file = |contents: String| FileContents {
        name: String::from("a.ts"),
        contents,
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

// Collects the one-based new-file line numbers iterateOverDiff emits rows for
// under the given expansion state -- the ground truth the oracle must match.
fn collect_rendered_addition_lines(
    diff: &FileDiffMetadata,
    expanded_hunks: Option<&ExpandedHunks>,
) -> HashSet<usize> {
    let mut rendered = HashSet::new();
    iterate_over_diff(
        IterateOverDiffProps {
            expanded_hunks,
            collapsed_context_threshold: COLLAPSED_CONTEXT_THRESHOLD,
            ..IterateOverDiffProps::new(diff, IterationDiffStyle::Split)
        },
        |props| {
            if let Some(addition_line) = props.addition_line {
                rendered.insert(addition_line.line_number);
            }
            false
        },
    );
    rendered
}

fn assert_oracle_matches(diff: &FileDiffMetadata, expanded_hunks: Option<&ExpandedHunks>) {
    let rendered = collect_rendered_addition_lines(diff, expanded_hunks);
    for line_number in 1..=40 {
        assert_eq!(
            is_addition_line_renderable(IsAdditionLineRenderableProps {
                file_diff: diff,
                line_number,
                expanded_hunks,
                collapsed_context_threshold: COLLAPSED_CONTEXT_THRESHOLD,
            }),
            rendered.contains(&line_number),
            "line {line_number}"
        );
    }
}

#[test]
fn matches_iterate_over_diff_with_everything_collapsed() {
    assert_oracle_matches(&make_diff(), Some(&regions(&[])));
}

#[test]
fn matches_iterate_over_diff_with_partially_expanded_regions() {
    assert_oracle_matches(
        &make_diff(),
        Some(&regions(&[
            // Leading gap of the first hunk, expanded from its start.
            (0, 3, 0),
            // Gap between the hunks, expanded from both edges.
            (1, 2, 4),
            // Trailing pseudo-key: context after the last hunk.
            (2, 3, 0),
        ])),
    );
}

#[test]
fn matches_iterate_over_diff_with_a_fully_expanded_gap() {
    assert_oracle_matches(&make_diff(), Some(&regions(&[(1, 99, 0)])));
}

#[test]
fn reports_every_line_renderable_when_everything_is_expanded() {
    assert_oracle_matches(&make_diff(), Some(&ExpandedHunks::All));
}

#[test]
fn reports_lines_beyond_the_modeled_range_as_renderable() {
    let diff = make_diff();
    // The editor document exposes one phantom line past the file end.
    assert!(is_addition_line_renderable(IsAdditionLineRenderableProps {
        file_diff: &diff,
        line_number: 41,
        expanded_hunks: Some(&regions(&[])),
        collapsed_context_threshold: COLLAPSED_CONTEXT_THRESHOLD,
    }));
}

// The 40-line fixture's hunks (context 4) cover lines 6-14 and 26-34;
// gaps are 1-5, 15-25, and trailing 35-40.
fn nearest(
    line_number: usize,
    direction: RenderableDirection,
    expanded_hunks: &ExpandedHunks,
) -> Option<usize> {
    get_nearest_renderable_addition_line(
        IsAdditionLineRenderableProps {
            file_diff: &make_diff(),
            line_number,
            expanded_hunks: Some(expanded_hunks),
            collapsed_context_threshold: COLLAPSED_CONTEXT_THRESHOLD,
        },
        direction,
    )
}

#[test]
fn returns_renderable_lines_unchanged() {
    let collapsed = regions(&[]);
    assert_eq!(nearest(10, RenderableDirection::Down, &collapsed), Some(10));
    assert_eq!(nearest(10, RenderableDirection::Up, &collapsed), Some(10));
}

#[test]
fn skips_down_across_the_collapsed_gap_to_the_next_hunk() {
    assert_eq!(
        nearest(15, RenderableDirection::Down, &regions(&[])),
        Some(26)
    );
}

#[test]
fn skips_up_across_the_collapsed_gap_to_the_previous_hunk() {
    assert_eq!(
        nearest(25, RenderableDirection::Up, &regions(&[])),
        Some(14)
    );
}

#[test]
fn lands_on_partially_expanded_gap_edges() {
    let expanded = regions(&[(1, 2, 3)]);
    // Gap 15-25: lines 15-16 and 23-25 render.
    assert_eq!(nearest(15, RenderableDirection::Down, &expanded), Some(15));
    assert_eq!(nearest(17, RenderableDirection::Down, &expanded), Some(23));
    assert_eq!(nearest(22, RenderableDirection::Up, &expanded), Some(16));
}

#[test]
fn stays_put_inside_the_collapsed_trailing_gap_going_down() {
    let collapsed = regions(&[]);
    assert_eq!(nearest(36, RenderableDirection::Down, &collapsed), None);
    assert_eq!(nearest(36, RenderableDirection::Up, &collapsed), Some(34));
}

#[test]
fn stays_put_inside_the_collapsed_leading_gap_going_up() {
    let collapsed = regions(&[]);
    assert_eq!(nearest(3, RenderableDirection::Up, &collapsed), None);
    assert_eq!(nearest(3, RenderableDirection::Down, &collapsed), Some(6));
}

#[test]
fn treats_lines_beyond_the_modeled_range_as_renderable() {
    assert_eq!(
        nearest(41, RenderableDirection::Down, &regions(&[])),
        Some(41)
    );
}
