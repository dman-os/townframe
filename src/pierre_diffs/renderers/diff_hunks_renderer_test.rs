// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/DiffHunksRender.test.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: bun:test to nextest; the renderer runs over the plain-text stub
// highlighter (`asyncRender` is `render`), `mockDiffs.diffRowBufferTest` is the
// corpus pair `diff_row_buffer_{old,new}.tsx` (its `options` field is not in the
// corpus excerpt, so the default options apply). The `toMatchSnapshot` row
// digests are replaced by the invariants the digests pin: every rendered line
// equals its source line, and the split indexes of a column increase. HTML
// assertions (`renderFullHTML`) are asserted on the hast instead (`hastToHtml`
// is deferred): `data-container-size` as a property of the `code` elements,
// text and `data-expand-index` by walking the tree. The partial-diff fixtures
// are generated with `create_two_files_patch` and `parse_patch_files`.

use crate::diff::PatchOptions;
use crate::diff::PatchSide;
use crate::diff::create_two_files_patch;
use crate::hast::PropValue;
use crate::renderers::test_support::render;
use crate::renderers::test_support::renderer_with;
use crate::renderers::test_support::split_renderer;
use crate::renderers::test_support::texts;
use crate::renderers::test_support::unified_renderer;
use crate::test_utils::hast::ColumnName;
use crate::test_utils::hast::ProjectedRow;
use crate::test_utils::hast::RowKind;
use crate::test_utils::hast::collect_all_elements;
use crate::test_utils::hast::collect_row_source_mismatches;
use crate::test_utils::hast::count_split_rows;
use crate::test_utils::hast::diff_of;
use crate::test_utils::hast::has_prop;
use crate::test_utils::hast::project_column;
use crate::test_utils::hast::prop_number;
use crate::test_utils::hast::row_digests;
use crate::test_utils::verify_hunk_line_values;
use crate::types::lines::CodeColumnType;
use crate::types::lines::HunkData;
use crate::types::lines::HunkDataExpandable;
use crate::types::options::DiffStyle;
use crate::types::options::HunkSeparators;
use crate::types::patch::ChangeTypes;
use crate::types::patch::FileDiffMetadata;
use crate::types::patch::HunkContent;
use crate::types::patch::ParsePatchOptions;
use crate::utils::parse_patch_files::parse_patch_files;

const FILE_OLD: &str = include_str!("../oracle/corpus/diff_row_buffer_old.tsx");
const FILE_NEW: &str = include_str!("../oracle/corpus/diff_row_buffer_new.tsx");

fn row_buffer_diff(old: &str, new: &str) -> FileDiffMetadata {
    diff_of("file.tsx", old, new)
}

/// Expected split-alignment buffer sizes, derived from the parsed change
/// blocks: a block deleting more lines than it adds leaves a gap in the
/// additions column (and vice versa), which the renderer fills with one
/// buffer row of the surplus size.
fn change_block_surpluses(diff: &FileDiffMetadata) -> (Vec<usize>, Vec<usize>) {
    let (mut additions_column, mut deletions_column) = (Vec::new(), Vec::new());
    for hunk in &diff.hunks {
        for content in &hunk.hunk_content {
            let HunkContent::Change(change) = content else {
                continue;
            };
            if change.deletions > change.additions {
                additions_column.push(change.deletions - change.additions);
            } else if change.additions > change.deletions {
                deletions_column.push(change.additions - change.deletions);
            }
        }
    }
    (additions_column, deletions_column)
}

fn buffer_sizes(rows: &[ProjectedRow]) -> Vec<usize> {
    rows.iter()
        .filter(|row| row.kind == RowKind::Buffer)
        .map(|row| usize::try_from(row.buffer_size.expect("buffer size")).expect("non-negative"))
        .collect()
}

/// What the upstream row-digest snapshots pin: every line row equals its
/// source line and the split indexes of a column never go backwards.
fn assert_rows_match_source(rows: &[ProjectedRow], column: ColumnName, old: &str, new: &str) {
    let old_lines: Vec<&str> = old.split('\n').collect();
    let new_lines: Vec<&str> = new.split('\n').collect();
    assert_eq!(
        collect_row_source_mismatches(rows, column, &old_lines, &new_lines),
        Vec::<String>::new(),
        "{:#?}",
        row_digests(rows)
    );
    // A unified column advances by unified index (its change rows share split
    // indexes); a split column advances by split index.
    let indexes: Vec<usize> = rows
        .iter()
        .filter_map(|row| {
            if column == ColumnName::Unified {
                row.unified_index
            } else {
                row.split_index
            }
        })
        .collect();
    assert!(indexes.windows(2).all(|pair| pair[0] < pair[1]));
}

fn parse_partial_diff_with_collapsed_context() -> FileDiffMetadata {
    let old_contents = "keep 1\nold value\nkeep 3\n";
    let new_contents = "keep 1\nnew value\nkeep 3\n";
    let side = |contents| PatchSide {
        file_name: "partial.txt",
        contents,
        header: None,
    };
    let patch = create_two_files_patch(
        side(old_contents),
        side(new_contents),
        PatchOptions {
            context: Some(0),
            ..PatchOptions::default()
        },
    );
    let file = parse_patch_files(&patch, &parse_options("partial"))
        .expect("valid patch")
        .value
        .into_iter()
        .next()
        .expect("one patch")
        .files
        .into_iter()
        .next()
        .expect("patch contains one file");
    assert!(file.is_partial);
    assert!(file.hunks[0].collapsed_before > 0);
    file
}

fn parse_options(prefix: &str) -> ParsePatchOptions {
    ParsePatchOptions {
        disable_line_matching: None,
        cache_key_prefix: Some(prefix.to_owned()),
        throw_on_error: Some(true),
    }
}

fn parse_single_file(patch: &[&str], prefix: &str) -> FileDiffMetadata {
    parse_patch_files(&patch.concat(), &parse_options(prefix))
        .expect("valid patch")
        .value
        .into_iter()
        .next()
        .expect("one patch")
        .files
        .into_iter()
        .next()
        .expect("patch contains one file")
}

fn parse_partial_new_file() -> FileDiffMetadata {
    let file = parse_single_file(
        &[
            "diff --git a/created.txt b/created.txt\n",
            "new file mode 100644\n",
            "index 0000000..1234567\n",
            "--- /dev/null\n",
            "+++ b/created.txt\n",
            "@@ -0,0 +1,2 @@\n",
            "+added 1\n",
            "+added 2\n",
        ],
        "partial-created",
    );
    assert!(file.is_partial);
    assert_eq!(file.change_type, ChangeTypes::New);
    file
}

fn parse_partial_deleted_file() -> FileDiffMetadata {
    let file = parse_single_file(
        &[
            "diff --git a/deleted.txt b/deleted.txt\n",
            "deleted file mode 100644\n",
            "index 1234567..0000000\n",
            "--- a/deleted.txt\n",
            "+++ /dev/null\n",
            "@@ -1,2 +0,0 @@\n",
            "-deleted 1\n",
            "-deleted 2\n",
        ],
        "partial-deleted",
    );
    assert!(file.is_partial);
    assert_eq!(file.change_type, ChangeTypes::Deleted);
    file
}

fn count_inline_diff_spans(
    result: &crate::renderers::diff_hunks_renderer::HunksRenderResult,
) -> usize {
    [&result.additions_content_ast, &result.deletions_content_ast]
        .into_iter()
        .flatten()
        .flat_map(|ast| collect_all_elements(ast))
        .filter(|element| has_prop(element, "data-diff-span"))
        .count()
}

#[test]
fn proper_buffers_should_be_prepended_to_additions_colum_in_split_style() {
    let renderer = split_renderer::<()>();
    let diff = row_buffer_diff(FILE_OLD, FILE_NEW);
    assert_eq!(verify_hunk_line_values(&diff, ""), Vec::<String>::new());
    let result = render(&renderer, &diff);
    let additions = result.additions_content_ast.as_ref().expect("additions");
    let deletions = result.deletions_content_ast.as_ref().expect("deletions");
    assert!(result.unified_content_ast.is_none());

    let addition_rows = project_column(additions);
    let deletion_rows = project_column(deletions);
    let (additions_column, deletions_column) = change_block_surpluses(&diff);
    // The fixture has at least one block deleting more than it adds, so the
    // additions column must receive buffer rows of exactly those sizes
    assert!(!additions_column.is_empty());
    assert_eq!(buffer_sizes(&addition_rows), additions_column);
    assert_eq!(buffer_sizes(&deletion_rows), deletions_column);
    assert_rows_match_source(&addition_rows, ColumnName::Additions, FILE_OLD, FILE_NEW);
    assert_rows_match_source(&deletion_rows, ColumnName::Deletions, FILE_OLD, FILE_NEW);
}

#[test]
fn proper_buffers_should_be_prepended_to_deletions_colum_in_split_style() {
    let renderer = split_renderer::<()>();
    let diff = row_buffer_diff(FILE_NEW, FILE_OLD);
    assert_eq!(verify_hunk_line_values(&diff, ""), Vec::<String>::new());
    let result = render(&renderer, &diff);
    let additions = result.additions_content_ast.as_ref().expect("additions");
    let deletions = result.deletions_content_ast.as_ref().expect("deletions");
    assert!(result.unified_content_ast.is_none());

    let addition_rows = project_column(additions);
    let deletion_rows = project_column(deletions);
    let (additions_column, deletions_column) = change_block_surpluses(&diff);
    // Reversed fixture: at least one block adds more than it deletes, so the
    // deletions column must receive buffer rows of exactly those sizes
    assert!(!deletions_column.is_empty());
    assert_eq!(buffer_sizes(&deletion_rows), deletions_column);
    assert_eq!(buffer_sizes(&addition_rows), additions_column);
    assert_rows_match_source(&addition_rows, ColumnName::Additions, FILE_NEW, FILE_OLD);
    assert_rows_match_source(&deletion_rows, ColumnName::Deletions, FILE_NEW, FILE_OLD);
}

#[test]
fn an_insert_block_above_a_paired_change_keeps_split_columns_aligned() {
    // Realignment splits "blank line inserted above an edited line" into an
    // addition-only block directly followed by a paired block. The renderer
    // must flush the pending deletion-side buffer before the paired block's
    // deletion row, so the buffer renders above it and the columns stay
    // index-parallel.
    let renderer = split_renderer::<()>();
    let context = "alpha\nbravo\ncharlie\ndelta\n";
    let diff = diff_of(
        "a.ts",
        &format!("{context}const value = compute();\n{context}"),
        &format!("{context}\nconst value = computed();\n{context}"),
    );
    assert_eq!(verify_hunk_line_values(&diff, ""), Vec::<String>::new());
    let result = render(&renderer, &diff);
    let deletion_rows = project_column(result.deletions_content_ast.as_ref().expect("deletions"));
    let addition_rows = project_column(result.additions_content_ast.as_ref().expect("additions"));
    // The deletion column renders the gap above the paired old line.
    let buffer_index = deletion_rows
        .iter()
        .position(|row| row.kind == RowKind::Buffer)
        .expect("a buffer row");
    let deletion_line_index = deletion_rows
        .iter()
        .position(|row| {
            row_digests(std::slice::from_ref(row))[0].contains("const value = compute();")
        })
        .expect("the paired deletion row");
    assert_eq!(deletion_line_index, buffer_index + 1);
    // Both columns cover the same rendered rows.
    let row_span = |rows: &[ProjectedRow]| -> i64 {
        rows.iter().map(|row| row.buffer_size.unwrap_or(1)).sum()
    };
    assert_eq!(row_span(&deletion_rows), row_span(&addition_rows));
}

#[test]
fn additions_and_deletions_should_be_empty_when_unified() {
    let renderer = unified_renderer::<()>();
    let diff = row_buffer_diff(FILE_OLD, FILE_NEW);
    assert_eq!(verify_hunk_line_values(&diff, ""), Vec::<String>::new());
    let result = render(&renderer, &diff);
    assert!(result.additions_content_ast.is_none());
    assert!(result.deletions_content_ast.is_none());
    let rows = project_column(result.unified_content_ast.as_ref().expect("unified"));
    assert_rows_match_source(&rows, ColumnName::Unified, FILE_OLD, FILE_NEW);
}

#[test]
fn a_diff_with_only_additions_should_have_an_empty_deletions_column() {
    let renderer = split_renderer::<()>();
    let diff = row_buffer_diff("", FILE_NEW);
    assert_eq!(diff.hunks[0].collapsed_before, 0);
    assert_eq!(verify_hunk_line_values(&diff, ""), Vec::<String>::new());
    let result = render(&renderer, &diff);
    assert_eq!(
        crate::test_utils::hast::prop_str(&result.pre_node, "data-diff-type"),
        Some("single")
    );
    let additions = result.additions_content_ast.as_ref().expect("additions");
    assert_eq!(count_split_rows(&result), diff.split_line_count);
    assert!(result.deletions_content_ast.is_none());
    assert!(result.unified_content_ast.is_none());
    assert_rows_match_source(
        &project_column(additions),
        ColumnName::Additions,
        "",
        FILE_NEW,
    );
}

#[test]
fn a_diff_with_only_deletions_should_have_an_empty_additions_column() {
    let renderer = split_renderer::<()>();
    let diff = row_buffer_diff(FILE_OLD, "");
    assert_eq!(diff.hunks[0].collapsed_before, 0);
    assert_eq!(verify_hunk_line_values(&diff, ""), Vec::<String>::new());
    let result = render(&renderer, &diff);
    assert_eq!(
        crate::test_utils::hast::prop_str(&result.pre_node, "data-diff-type"),
        Some("single")
    );
    let deletions = result.deletions_content_ast.as_ref().expect("deletions");
    assert_eq!(count_split_rows(&result), diff.split_line_count);
    assert!(result.additions_content_ast.is_none());
    assert!(result.unified_content_ast.is_none());
    assert_rows_match_source(
        &project_column(deletions),
        ColumnName::Deletions,
        FILE_OLD,
        "",
    );
}

/// Whether any `code` element of the full AST carries `data-container-size`.
fn has_container_size(separators: HunkSeparators) -> bool {
    let renderer = renderer_with::<()>(|options| options.hunk_separators = Some(separators));
    let diff = row_buffer_diff(FILE_OLD, FILE_NEW);
    let result = render(&renderer, &diff);
    let pre = renderer.render_full_ast(&result, Vec::new());
    let code_elements: Vec<_> = pre
        .children
        .iter()
        .filter_map(|child| child.as_element())
        .filter(|element| element.tag_name == "code")
        .collect();
    assert!(!code_elements.is_empty());
    code_elements
        .iter()
        .all(|element| has_prop(element, "data-container-size"))
}

#[test]
fn adds_data_container_size_for_line_info_separators() {
    assert!(has_container_size(HunkSeparators::LineInfo));
}

#[test]
fn does_not_add_data_container_size_for_non_line_info_separators() {
    assert!(!has_container_size(HunkSeparators::LineInfoBasic));
}

#[test]
fn keeps_partial_hunk_separators_non_expandable_without_a_file_loader() {
    let renderer =
        renderer_with::<()>(|options| options.hunk_separators = Some(HunkSeparators::LineInfo));
    let result = render(&renderer, &parse_partial_diff_with_collapsed_context());
    assert!(!result.hunk_data.is_empty());
    assert!(
        result
            .hunk_data
            .iter()
            .all(|hunk| hunk.expandable.is_none())
    );
}

fn renderer_with_loader(
    diff_style: DiffStyle,
) -> crate::renderers::diff_hunks_renderer::DiffHunksRenderer<()> {
    let mut renderer = renderer_with::<()>(|options| {
        options.diff_style = Some(diff_style);
        options.hunk_separators = Some(HunkSeparators::LineInfo);
    });
    renderer.options.has_diff_file_loader = true;
    renderer
}

#[test]
fn marks_partial_hunk_separators_expandable_with_a_file_loader() {
    let renderer = renderer_with_loader(DiffStyle::Split);
    let result = render(&renderer, &parse_partial_diff_with_collapsed_context());
    assert!(!result.hunk_data.is_empty());
    assert!(
        result
            .hunk_data
            .iter()
            .all(|hunk| hunk.expandable.is_some())
    );
}

fn unified_text(result: &crate::renderers::diff_hunks_renderer::HunksRenderResult) -> String {
    texts(result.unified_content_ast.as_ref().expect("unified"))
}

fn has_expand_index(
    result: &crate::renderers::diff_hunks_renderer::HunksRenderResult,
    index: usize,
) -> bool {
    collect_all_elements(result.unified_content_ast.as_ref().expect("unified"))
        .into_iter()
        .any(|element| prop_number(element, "data-expand-index") == Some(index as i64))
}

#[test]
fn renders_synthetic_bottom_separator_for_partial_diffs_with_a_file_loader() {
    let diff = parse_partial_diff_with_collapsed_context();
    let renderer = renderer_with_loader(DiffStyle::Unified);
    let result = render(&renderer, &diff);
    let tail_hunk_data = result
        .hunk_data
        .iter()
        .find(|hunk| hunk.hunk_index == diff.hunks.len());

    assert_eq!(
        tail_hunk_data,
        Some(&HunkData {
            slot_name: format!("hunk-separator-unified-{}", diff.hunks.len()),
            hunk_index: diff.hunks.len(),
            lines: 0,
            line_count_known: false,
            column_type: CodeColumnType::Unified,
            expandable: Some(HunkDataExpandable {
                up: true,
                down: false,
                chunked: false
            }),
        })
    );
    let text = unified_text(&result);
    assert!(text.contains("More unchanged context may be available"));
    assert!(has_expand_index(&result, diff.hunks.len()));
    assert!(!text.contains("0 unmodified lines"));
}

#[test]
fn does_not_render_synthetic_bottom_separator_for_partial_diffs_without_a_file_loader() {
    let diff = parse_partial_diff_with_collapsed_context();
    let renderer = renderer_with::<()>(|options| {
        options.diff_style = Some(DiffStyle::Unified);
        options.hunk_separators = Some(HunkSeparators::LineInfo);
    });
    let result = render(&renderer, &diff);
    assert!(
        !result
            .hunk_data
            .iter()
            .any(|hunk| hunk.hunk_index == diff.hunks.len())
    );
    assert!(!unified_text(&result).contains("More unchanged context may be available"));
}

fn assert_no_synthetic_bottom_separator(diff: &FileDiffMetadata) {
    let renderer = renderer_with_loader(DiffStyle::Unified);
    let result = render(&renderer, diff);
    assert!(
        !result
            .hunk_data
            .iter()
            .any(|hunk| hunk.hunk_index == diff.hunks.len())
    );
    assert!(
        result
            .hunk_data
            .iter()
            .all(|hunk| hunk.expandable.is_none())
    );
    assert!(!unified_text(&result).contains("More unchanged context may be available"));
    assert!(!has_expand_index(&result, diff.hunks.len()));
}

#[test]
fn does_not_render_synthetic_bottom_separator_for_partial_new_files_with_a_file_loader() {
    assert_no_synthetic_bottom_separator(&parse_partial_new_file());
}

#[test]
fn does_not_render_synthetic_bottom_separator_for_partial_deleted_files_with_a_file_loader() {
    assert_no_synthetic_bottom_separator(&parse_partial_deleted_file());
}

#[test]
fn skips_inline_diff_decorations_for_changed_lines_above_max_line_diff_length() {
    let renderer = renderer_with::<()>(|options| {
        options.diff_style = Some(DiffStyle::Split);
        options.max_line_diff_length = Some(5);
    });
    let diff = diff_of(
        "example.ts",
        "const value = \"aaaaaaaaaaaa\";\n",
        "const value = \"bbbbbbbbbbbb\";\n",
    );
    assert_eq!(count_inline_diff_spans(&render(&renderer, &diff)), 0);
}

#[test]
fn keeps_inline_diff_decorations_for_changed_lines_below_max_line_diff_length() {
    let renderer = renderer_with::<()>(|options| {
        options.diff_style = Some(DiffStyle::Split);
        options.max_line_diff_length = Some(50);
    });
    let diff = diff_of("example.ts", "const x = 1;\n", "const x = 2;\n");
    assert!(count_inline_diff_spans(&render(&renderer, &diff)) > 0);
}

// `hunk_separators` property check used by the helper above.
#[test]
fn container_size_property_is_a_bare_attribute() {
    let renderer =
        renderer_with::<()>(|options| options.hunk_separators = Some(HunkSeparators::LineInfo));
    let result = render(&renderer, &row_buffer_diff(FILE_OLD, FILE_NEW));
    let pre = renderer.render_full_ast(&result, Vec::new());
    let code = pre
        .children
        .iter()
        .filter_map(|child| child.as_element())
        .find(|element| element.tag_name == "code")
        .expect("a code element");
    assert_eq!(
        code.properties.get("data-container-size"),
        Some(&PropValue::Str(String::new()))
    );
}
