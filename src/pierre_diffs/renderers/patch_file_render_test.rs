// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/patchFileRender.test.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: bun:test to nextest; the renderer runs over the plain-text stub
// highlighter. The two `toMatchSnapshot` calls (the patch digest and the
// rendered row digests) are replaced by the invariants they pin: the patch is
// the single hunk the fixture comment describes, every rendered row of both
// columns is a line of the patch with the text the patch gives it, and the
// columns cover the same number of rows.

use crate::renderers::test_support::render;
use crate::renderers::test_support::split_renderer;
use crate::test_utils::hast::RowKind;
use crate::test_utils::hast::project_column;
use crate::test_utils::verify_hunk_line_values;
use crate::types::patch::ParsePatchOptions;
use crate::utils::parse_patch_files::parse_patch_files;

// NOTE(amadeus): This was a known tricky patch that our renderer would break
// on at one point
const PATCH_FIXTURE: &str = include_str!("../oracle/corpus/file.patch");

#[test]
fn parses_and_renders_the_patch_file() {
    let parsed = parse_patch_files(
        PATCH_FIXTURE,
        &ParsePatchOptions {
            disable_line_matching: None,
            cache_key_prefix: Some(String::from("file-patch")),
            throw_on_error: None,
        },
    )
    .expect("the fixture parses")
    .value;
    assert_eq!(parsed.len(), 1);
    let file = parsed[0].files.first().expect("file should be defined");
    // The parsed hunk metadata must be internally consistent, and the single
    // hunk (replacing a block deep in the file) pins the geometry that
    // originally broke the renderer
    assert_eq!(verify_hunk_line_values(file, ""), Vec::<String>::new());
    assert_eq!(file.hunks.len(), 1);

    let result = render(&split_renderer::<()>(), file);
    let additions = project_column(result.additions_content_ast.as_ref().expect("additions"));
    let deletions = project_column(result.deletions_content_ast.as_ref().expect("deletions"));

    // Each column renders exactly the hunk's lines for its side, and every
    // rendered line is a line of the patch with the text the patch gives it.
    let hunk = &file.hunks[0];
    for (rows, lines, count) in [
        (&additions, &file.addition_lines, hunk.addition_count),
        (&deletions, &file.deletion_lines, hunk.deletion_count),
    ] {
        let line_rows: Vec<_> = rows
            .iter()
            .filter(|row| row.kind == RowKind::Line)
            .collect();
        assert_eq!(line_rows.len(), count);
        for row in line_rows {
            let rendered = row.text.as_deref().expect("text");
            assert!(
                lines
                    .iter()
                    .any(|line| line.trim_end_matches('\n') == rendered),
                "row {rendered:?} is not a line of the patch"
            );
        }
    }
    let span = |rows: &[crate::test_utils::hast::ProjectedRow]| -> i64 {
        rows.iter().map(|row| row.buffer_size.unwrap_or(1)).sum()
    };
    assert_eq!(span(&additions), span(&deletions));
}
