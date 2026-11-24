// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/getHunkSideBoundaries.test.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: bun:test to nextest; `test.each` is a loop.

use crate::types::patch::DiffFileInput;
use crate::types::patch::FileContents;
use crate::types::patch::ParseDiffOptions;
use crate::utils::get_hunk_side_boundaries::get_hunk_side_end_boundary;
use crate::utils::get_hunk_side_boundaries::get_hunk_side_start_boundary;
use crate::utils::get_total_line_count_from_hunks::get_total_line_count_from_hunks;
use crate::utils::parse_diff_from_file::parse_diff_from_file;

#[test]
fn converts_each_range_to_consumed_file_boundaries() {
    let cases = [(10, 3, 9, 12), (10, 0, 10, 10), (0, 0, 0, 0)];
    for (start, count, expected_start, expected_end) in cases {
        let range_start = get_hunk_side_start_boundary(start, count);
        let range_end = get_hunk_side_end_boundary(start, count);

        assert_eq!(range_start, expected_start, "{start},{count}");
        assert_eq!(range_end, expected_end, "{start},{count}");
        assert_eq!(range_end - range_start, count, "{start},{count}");
    }
}

#[test]
fn derives_total_lines_from_the_final_consumed_boundaries() {
    let file = |contents: &str| FileContents {
        name: String::from("two-lines.ts"),
        contents: contents.to_owned(),
        lang: None,
        header: None,
        cache_key: None,
    };
    let diff = parse_diff_from_file(
        &DiffFileInput::Both {
            old_file: file("first\nsecond\n"),
            new_file: file("first\nchanged\n"),
        },
        &ParseDiffOptions::default(),
        false,
    )
    .expect("valid")
    .value;

    assert_eq!(get_total_line_count_from_hunks(&diff.hunks), 2);
}
