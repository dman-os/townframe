// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/DiffHunksRendererVirtualization.test.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: bun:test to nextest; the renderer runs over the plain-text stub
// highlighter, where a windowed plain-text diff is rendered through the window
// only (upstream highlights the whole diff and windows `processDiffResult`),
// so every windowed assertion is also a differential check of the windowed
// render against the full one. `fileOld`/`fileNew` (the demo fixtures, absent
// from the extracted tree) are `big_fixture_files`; upstream's numbers that
// are specific to the demo geometry (517 rows, indices 57-106, hunk 3, ...)
// are derived from the parsed diff instead: the same relations (rows = declared
// rows + auto-expanded gaps, an expansion adds exactly its size, a window is a
// slice of the full render) with the widest collapsed gap of the stand-in as
// "hunk 3". `expandHunk(.., Infinity)` is a large finite count. The `test.each`
// of the no-newline metadata test is a loop over its cases. Tests 4.5, 4.6 and
// 5.1-5.3 (windows starting or ending mid-hunk, skipping hunks, spanning
// hunks) are the table of windows in `windows_are_slices_of_the_full_render`.

use crate::constants::DEFAULT_COLLAPSED_CONTEXT_THRESHOLD;
use crate::hast::ElementContent;
use crate::renderers::diff_hunks_renderer::DiffHunksRenderer;
use crate::renderers::test_support::render;
use crate::renderers::test_support::render_in;
use crate::renderers::test_support::renderer_with;
use crate::renderers::test_support::split_renderer;
use crate::renderers::test_support::unified_renderer;
use crate::renderers::test_support::window;
use crate::test_utils::count_declared_rows;
use crate::test_utils::hast::ColumnName;
use crate::test_utils::hast::ProjectedRow;
use crate::test_utils::hast::RowKind;
use crate::test_utils::hast::big_fixture_diff;
use crate::test_utils::hast::big_fixture_files;
use crate::test_utils::hast::collect_all_elements;
use crate::test_utils::hast::collect_row_source_mismatches;
use crate::test_utils::hast::count_rendered_lines;
use crate::test_utils::hast::count_split_rows;
use crate::test_utils::hast::diff_of;
use crate::test_utils::hast::extract_line_numbers;
use crate::test_utils::hast::has_prop;
use crate::test_utils::hast::project_column;
use crate::test_utils::hast::prop_str;
use crate::types::lines::ExpansionDirections;
use crate::types::options::DiffStyle;
use crate::types::patch::FileDiffMetadata;

fn unified_indices(
    renderer: &DiffHunksRenderer<()>,
    diff: &FileDiffMetadata,
    start: usize,
    total: usize,
) -> Vec<usize> {
    let result = render_in(renderer, diff, &window(start, total));
    // A window entirely past the content has no rows, so no column at all.
    result
        .unified_content_ast
        .as_ref()
        .map_or_else(Vec::new, |ast| extract_line_numbers(ast).0)
}

fn full_unified_indices(renderer: &DiffHunksRenderer<()>, diff: &FileDiffMetadata) -> Vec<usize> {
    let result = render(renderer, diff);
    extract_line_numbers(result.unified_content_ast.as_ref().expect("unified")).0
}

fn line_rows(ast: &[ElementContent]) -> Vec<ProjectedRow> {
    project_column(ast)
        .into_iter()
        .filter(|row| row.kind == RowKind::Line)
        .collect()
}

/// Rows of kind `kind` among the top-level nodes of a column.
fn top_level_kinds(ast: &[ElementContent]) -> Vec<&'static str> {
    project_column(ast)
        .iter()
        .map(|row| match row.kind {
            RowKind::Line => "line",
            RowKind::Buffer => "buffer",
            RowKind::NoNewline => "no-newline",
            _ => "other",
        })
        .collect()
}

fn count_no_newline_elements(ast: &[ElementContent]) -> usize {
    collect_all_elements(ast)
        .into_iter()
        .filter(|element| has_prop(element, "data-no-newline"))
        .count()
}

/// Declared rows of one split side plus the auto-expanded single-line gaps.
fn declared_side_rows(diff: &FileDiffMetadata, additions: bool) -> usize {
    diff.hunks
        .iter()
        .map(|hunk| {
            let declared = if additions {
                hunk.addition_count
            } else {
                hunk.deletion_count
            };
            let auto_expanded = hunk.collapsed_before > 0
                && hunk.collapsed_before <= DEFAULT_COLLAPSED_CONTEXT_THRESHOLD;
            declared
                + if auto_expanded {
                    hunk.collapsed_before
                } else {
                    0
                }
        })
        .sum()
}

#[test]
fn zero_buffers_unified_mode() {
    // 1.1
    let diff = big_fixture_diff();
    let result = render(&unified_renderer::<()>(), &diff);
    // The renderer passes the requested buffer sizes through on the result;
    // buffer DOM elements are created by File/FileDiff, not the renderer.
    assert_eq!(result.buffer_before, 0.0);
    assert_eq!(result.buffer_after, 0.0);
    let line_count = count_rendered_lines(result.unified_content_ast.as_ref().expect("unified"));
    // hunk-declared rows + the auto-expanded single-line gaps
    assert_eq!(
        line_count,
        count_declared_rows(&diff, false, DEFAULT_COLLAPSED_CONTEXT_THRESHOLD)
    );
}

#[test]
fn zero_buffers_split_mode() {
    // 1.2
    let diff = big_fixture_diff();
    let result = render(&split_renderer::<()>(), &diff);
    assert_eq!(result.buffer_before, 0.0);
    assert_eq!(result.buffer_after, 0.0);
    // Each column renders only the rows its own side participates in
    // (context plus its change rows)
    assert_eq!(
        count_rendered_lines(result.deletions_content_ast.as_ref().expect("deletions")),
        declared_side_rows(&diff, false)
    );
    assert_eq!(
        count_rendered_lines(result.additions_content_ast.as_ref().expect("additions")),
        declared_side_rows(&diff, true)
    );
}

#[test]
fn unified_eof_metadata() {
    for (label, previous, contents, metadata_rows) in [
        ("unchanged final line", "old\nsame", "new\nsame", 1),
        ("changed final line", "old", "new", 2),
        ("deletion lacks newline", "old", "new\n", 1),
        ("addition lacks newline", "old\n", "new", 1),
    ] {
        let diff = diff_of("ending.txt", previous, contents);
        let renderer = renderer_with::<()>(|options| {
            options.diff_style = Some(DiffStyle::Unified);
            options.expand_unchanged = Some(true);
        });
        let result = render(&renderer, &diff);
        let content = result
            .unified_content_ast
            .as_ref()
            .expect("Missing unified content");
        let gutter = result
            .unified_gutter_ast
            .as_ref()
            .expect("Missing unified gutter");
        assert_eq!(count_no_newline_elements(content), metadata_rows, "{label}");
        assert_eq!(
            collect_all_elements(gutter)
                .into_iter()
                .filter(|node| prop_str(node, "data-gutter-buffer") == Some("metadata"))
                .count(),
            metadata_rows,
            "{label}"
        );
        assert_eq!(content.len(), result.row_count, "{label}");
        assert_eq!(gutter.len(), result.row_count, "{label}");
    }
}

#[test]
fn renders_deletion_side_metadata_when_deletions_are_shorter_in_split_mode() {
    let diff = diff_of(
        "deletion-shorter.txt",
        "same\nold-final",
        "same\nnew-a\nnew-b\n",
    );
    let result = render(&split_renderer::<()>(), &diff);
    let deletions = result.deletions_content_ast.expect("deletions");
    let additions = result.additions_content_ast.expect("additions");
    assert_eq!(count_no_newline_elements(&deletions), 1);
    assert_eq!(count_no_newline_elements(&additions), 0);
    let deletion_kinds = top_level_kinds(&deletions);
    assert_eq!(
        deletion_kinds[deletion_kinds.len() - 2..],
        ["buffer", "no-newline"]
    );
    assert_eq!(top_level_kinds(&additions).last(), Some(&"buffer"));
}

#[test]
fn renders_addition_side_metadata_when_additions_are_shorter_in_split_mode() {
    let diff = diff_of(
        "addition-shorter.txt",
        "same\nold-a\nold-b\n",
        "same\nnew-final",
    );
    let result = render(&split_renderer::<()>(), &diff);
    let deletions = result.deletions_content_ast.expect("deletions");
    let additions = result.additions_content_ast.expect("additions");
    assert_eq!(count_no_newline_elements(&deletions), 0);
    assert_eq!(count_no_newline_elements(&additions), 1);
    assert_eq!(top_level_kinds(&deletions).last(), Some(&"buffer"));
    let addition_kinds = top_level_kinds(&additions);
    assert_eq!(
        addition_kinds[addition_kinds.len() - 2..],
        ["buffer", "no-newline"]
    );
}

#[test]
fn basic_window_first_n_lines() {
    // 2.2
    let diff = big_fixture_diff();
    let indices = unified_indices(&unified_renderer(), &diff, 0, 30);
    // The first hunk's collapsedBefore is the first rendered index
    assert_eq!(indices[0], diff.hunks[0].collapsed_before);
    assert_eq!(indices.len(), 30);
}

#[test]
fn basic_window_middle_lines() {
    // 2.3
    let diff = big_fixture_diff();
    let renderer = unified_renderer();
    let indices = unified_indices(&renderer, &diff, 100, 50);
    // Line indices are not continuous across collapsed regions, but the
    // window is exactly rows 100..150 of the full render
    assert_eq!(indices.len(), 50);
    assert_eq!(indices, full_unified_indices(&renderer, &diff)[100..150]);
}

#[test]
fn split_vs_unified_line_counting() {
    // 2.4
    let diff = big_fixture_diff();
    let range = window(50, 50);
    let unified = render_in(&unified_renderer::<()>(), &diff, &range);
    let split = render_in(&split_renderer::<()>(), &diff, &range);
    assert_eq!(
        count_rendered_lines(unified.unified_content_ast.as_ref().expect("unified")),
        50
    );
    // Split rows are shared by both columns: 50 rows, each column holding
    // the rows it participates in.
    assert_eq!(count_split_rows(&split), 50);
    assert!(count_rendered_lines(split.additions_content_ast.as_ref().expect("additions")) <= 50);
    assert!(count_rendered_lines(split.deletions_content_ast.as_ref().expect("deletions")) <= 50);
}

/// `(hunk index, collapsed lines, first collapsed unified index)` of the
/// widest collapsed gap: the stand-in for the demo diff's hunk 3.
fn widest_gap(diff: &FileDiffMetadata) -> (usize, usize, usize) {
    let (index, hunk) = diff
        .hunks
        .iter()
        .enumerate()
        .max_by_key(|(_, hunk)| hunk.collapsed_before)
        .expect("hunks");
    assert!(index > 0 && hunk.collapsed_before >= 30);
    (
        index,
        hunk.collapsed_before,
        hunk.unified_line_start - hunk.collapsed_before,
    )
}

fn expanded_renderer(expansion_line_count: usize) -> DiffHunksRenderer<()> {
    renderer_with(|options| {
        options.diff_style = Some(DiffStyle::Unified);
        options.expansion_line_count = Some(expansion_line_count);
    })
}

#[test]
fn fully_expanded_expand_unchanged() {
    // 3.1
    let diff = big_fixture_diff();
    let renderer = renderer_with::<()>(|options| {
        options.diff_style = Some(DiffStyle::Unified);
        options.expand_unchanged = Some(true);
    });
    let result = render(&renderer, &diff);
    // With expandUnchanged, all collapsed lines are rendered
    assert_eq!(
        count_rendered_lines(result.unified_content_ast.as_ref().expect("unified")),
        diff.unified_line_count
    );
}

#[test]
fn partially_expanded_from_start_only() {
    // 3.2
    let diff = big_fixture_diff();
    let (hunk, _, region_start) = widest_gap(&diff);
    let mut renderer = expanded_renderer(12);
    renderer.expand_hunk(hunk, ExpansionDirections::Up, None);
    let indices = full_unified_indices(&renderer, &diff);
    let unexpanded = full_unified_indices(&unified_renderer(), &diff);
    // Should have 12 more lines than unexpanded
    assert_eq!(indices.len(), unexpanded.len() + 12);
    let expanded: Vec<usize> = indices
        .iter()
        .copied()
        .filter(|index| (region_start..region_start + 12).contains(index))
        .collect();
    assert_eq!(
        expanded,
        (region_start..region_start + 12).collect::<Vec<_>>()
    );
}

#[test]
fn partially_expanded_from_end_only() {
    // 3.3
    let diff = big_fixture_diff();
    let (hunk, collapsed, region_start) = widest_gap(&diff);
    let region_end = region_start + collapsed;
    let mut renderer = expanded_renderer(9);
    renderer.expand_hunk(hunk, ExpansionDirections::Down, None);
    let indices = full_unified_indices(&renderer, &diff);
    assert_eq!(
        indices.len(),
        full_unified_indices(&unified_renderer(), &diff).len() + 9
    );
    let expanded: Vec<usize> = indices
        .iter()
        .copied()
        .filter(|index| (region_end - 9..region_end).contains(index))
        .collect();
    assert_eq!(expanded, (region_end - 9..region_end).collect::<Vec<_>>());
    // Verify line indices are monotonically increasing
    assert!(indices.windows(2).all(|pair| pair[0] <= pair[1]));
}

#[test]
fn partially_expanded_both_from_start_and_from_end() {
    // 3.4
    let diff = big_fixture_diff();
    let (hunk, collapsed, region_start) = widest_gap(&diff);
    let region_end = region_start + collapsed;
    let mut renderer = expanded_renderer(8);
    renderer.expand_hunk(hunk, ExpansionDirections::Both, None);
    let indices = full_unified_indices(&renderer, &diff);
    // 8 from start + 8 from end
    assert_eq!(
        indices.len(),
        full_unified_indices(&unified_renderer(), &diff).len() + 16
    );
    let within = |range: std::ops::Range<usize>| -> Vec<usize> {
        indices
            .iter()
            .copied()
            .filter(|index| range.contains(index))
            .collect()
    };
    assert_eq!(
        within(region_start..region_start + 8),
        (region_start..region_start + 8).collect::<Vec<_>>()
    );
    assert_eq!(
        within(region_end - 8..region_end),
        (region_end - 8..region_end).collect::<Vec<_>>()
    );
}

#[test]
fn windowing_with_expanded_regions() {
    // 3.5: a window starting before an expanded hunk must not skip it (the
    // a9ff17b7 bug did exactly that).
    let diff = big_fixture_diff();
    let (hunk, _, region_start) = widest_gap(&diff);
    let mut renderer = expanded_renderer(12);
    renderer.expand_hunk(hunk, ExpansionDirections::Up, None);
    let full = full_unified_indices(&renderer, &diff);
    let first_expanded = full
        .iter()
        .position(|index| *index == region_start)
        .expect("expanded rows");
    let start = first_expanded - 3;
    let windowed = unified_indices(&renderer, &diff, start, 50);
    // Differential: the windowed render emits exactly rows start..start+50
    // of the full render with the same expansion state
    assert_eq!(windowed.len(), 50);
    assert_eq!(windowed, full[start..start + 50]);
    assert_eq!(
        windowed
            .iter()
            .filter(|index| (region_start..region_start + 12).contains(index))
            .count(),
        12
    );
}

#[test]
fn fully_expanded_single_hunk_range() {
    // 3.6
    let diff = big_fixture_diff();
    let (hunk, collapsed, region_start) = widest_gap(&diff);
    let mut renderer = unified_renderer::<()>();
    renderer.expand_hunk(hunk, ExpansionDirections::Both, Some(1_000_000));
    let indices = full_unified_indices(&renderer, &diff);
    let unexpanded = full_unified_indices(&unified_renderer(), &diff);
    let fully_expanded: Vec<usize> = indices
        .iter()
        .copied()
        .filter(|index| (region_start..region_start + collapsed).contains(index))
        .collect();
    assert_eq!(
        fully_expanded,
        (region_start..region_start + collapsed).collect::<Vec<_>>()
    );
    // Separator rows are not lines (no data-line), so expanding this range
    // adds exactly its lines.
    assert_eq!(indices.len(), unexpanded.len() + collapsed);
    // Only this hunk's range was expanded: the leading collapsed lines of hunk 0 stay hidden.
    for hidden in 0..diff.hunks[0].collapsed_before {
        assert!(!indices.contains(&hidden));
    }
}

#[test]
fn window_boundary_edge_cases() {
    // 4.1-4.4
    let diff = big_fixture_diff();
    let renderer = unified_renderer::<()>();
    let full = full_unified_indices(&renderer, &diff);
    let first_hunk_rows = diff.hunks[0].unified_line_count;
    // 4.1: a window of exactly the first hunk's rows renders exactly them
    assert_eq!(
        unified_indices(&renderer, &diff, 0, first_hunk_rows).len(),
        first_hunk_rows
    );
    // 4.2: a window starting at the next hunk's first row
    let from_boundary = unified_indices(&renderer, &diff, first_hunk_rows, 20);
    assert!(!from_boundary.is_empty() && from_boundary.len() <= 20);
    assert!(from_boundary[0] >= first_hunk_rows);
    // 4.3: a single line window
    let single = unified_indices(&renderer, &diff, 50, 1);
    assert_eq!(single.len(), 1);
    assert!(single[0] >= 50);
    // 4.4: a window entirely past the content renders nothing
    assert!(unified_indices(&renderer, &diff, full.len() + 483, 20).is_empty());
}

#[test]
fn windows_are_slices_of_the_full_render() {
    // 4.5, 4.6, 5.1-5.3 and 7.1: windows starting or ending mid-hunk,
    // skipping whole hunks, spanning several, and ending inside the final hunk.
    let diff = big_fixture_diff();
    let renderer = unified_renderer::<()>();
    let full = full_unified_indices(&renderer, &diff);
    let final_hunk_rows = diff.hunks.last().expect("hunks").unified_line_count;
    for (start, total) in [
        (5, 10),
        (13, 40),
        (40, 25),
        (100, 60),
        (150, 80),
        (200, 100),
        (full.len() - final_hunk_rows / 2, 20),
        (full.len() - 22, 20),
    ] {
        let expected = &full[start..(start + total).min(full.len())];
        assert_eq!(
            unified_indices(&renderer, &diff, start, total),
            expected,
            "window {start}+{total}"
        );
    }
    // 7.1: ending mid-final-hunk renders without panicking, with a full window
    assert_eq!(
        unified_indices(&renderer, &diff, full.len() - 22, 20).len(),
        20
    );
}

#[test]
fn rendered_content_matches_source_unified() {
    // 6.1
    let (old, new) = big_fixture_files();
    let (old_lines, new_lines): (Vec<&str>, Vec<&str>) =
        (old.split('\n').collect(), new.split('\n').collect());
    let diff = big_fixture_diff();
    let renderer = unified_renderer::<()>();
    let result = render_in(&renderer, &diff, &window(10, 10));
    let rows = line_rows(result.unified_content_ast.as_ref().expect("unified"));
    assert_eq!(rows.len(), 10);
    // The window must be exactly rows 10-19 of the full render
    let full = render(&renderer, &diff);
    let full_rows = line_rows(full.unified_content_ast.as_ref().expect("unified"));
    assert_eq!(rows, full_rows[10..20]);
    // Every rendered row carries the exact text of its source line
    assert_eq!(
        collect_row_source_mismatches(&rows, ColumnName::Unified, &old_lines, &new_lines),
        Vec::<String>::new()
    );
}

#[test]
fn rendered_content_matches_source_split() {
    // 6.2
    let (old, new) = big_fixture_files();
    let (old_lines, new_lines): (Vec<&str>, Vec<&str>) =
        (old.split('\n').collect(), new.split('\n').collect());
    let diff = big_fixture_diff();
    let renderer = split_renderer::<()>();
    let result = render_in(&renderer, &diff, &window(10, 10));
    let addition_rows = line_rows(result.additions_content_ast.as_ref().expect("additions"));
    let deletion_rows = line_rows(result.deletions_content_ast.as_ref().expect("deletions"));
    // The window covers split row positions 10-19 of the full render; each
    // column must contain exactly its rows at those positions
    let full = render(&renderer, &diff);
    let full_additions = line_rows(full.additions_content_ast.as_ref().expect("additions"));
    let full_deletions = line_rows(full.deletions_content_ast.as_ref().expect("deletions"));
    let mut positions: Vec<Option<usize>> = full_additions
        .iter()
        .chain(&full_deletions)
        .map(|row| row.split_index)
        .collect();
    positions.sort();
    positions.dedup();
    let window_positions = &positions[10..20];
    let in_window = |rows: &[ProjectedRow]| -> Vec<ProjectedRow> {
        rows.iter()
            .filter(|row| window_positions.contains(&row.split_index))
            .cloned()
            .collect()
    };
    assert_eq!(addition_rows, in_window(&full_additions));
    assert_eq!(deletion_rows, in_window(&full_deletions));
    // Each column carries the exact text of its own side's source lines
    assert_eq!(
        collect_row_source_mismatches(
            &addition_rows,
            ColumnName::Additions,
            &old_lines,
            &new_lines
        ),
        Vec::<String>::new()
    );
    assert_eq!(
        collect_row_source_mismatches(
            &deletion_rows,
            ColumnName::Deletions,
            &old_lines,
            &new_lines
        ),
        Vec::<String>::new()
    );
}
