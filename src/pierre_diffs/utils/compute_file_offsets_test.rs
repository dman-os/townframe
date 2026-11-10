// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/computeLineOffsets.test.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: bun:test to nextest; offsets are byte offsets (ASCII inputs here, so
// they equal upstream's UTF-16 offsets; `utils::oracle_tests::layout_oracle_tests` compares
// non-ASCII text in UTF-16 units against the published function).

use crate::utils::compute_file_offsets::compute_line_offsets;
use crate::utils::compute_file_offsets::count_line_breaks;
use crate::utils::compute_file_offsets::lines_from_file_contents;

#[test]
fn returns_a_single_start_offset_for_empty_contents() {
    assert_eq!(compute_line_offsets(""), [0]);
}

#[test]
fn computes_offsets_for_single_line() {
    assert_eq!(compute_line_offsets("hello"), [0]);
}

#[test]
fn computes_offsets_for_lf_files() {
    assert_eq!(compute_line_offsets("a\nb\n"), [0, 2, 4]);
    assert_eq!(compute_line_offsets("a\nb"), [0, 2]);
}

#[test]
fn computes_offsets_for_crlf_and_lone_cr_line_endings() {
    assert_eq!(compute_line_offsets("a\r\nb\r\n"), [0, 3, 6]);
    assert_eq!(compute_line_offsets("a\rb\r\nc\n"), [0, 2, 5, 7]);
}

#[test]
fn treats_newline_only_contents_as_two_offset_boundaries() {
    assert_eq!(compute_line_offsets("\n"), [0, 1]);
}

#[test]
fn count_line_breaks_stays_consistent_with_compute_line_offsets_across_line_endings() {
    let cases = [
        "",
        "hello",
        "a\nb",
        "a\nb\n",
        "a\r\nb\r\n",
        "\rworld\rfoo",
        "a\rb\r\nc\n",
        "\n",
        "\r",
    ];
    for contents in cases {
        assert_eq!(
            count_line_breaks(contents),
            compute_line_offsets(contents).len() - 1,
            "{contents:?}"
        );
    }
}

#[test]
fn counts_a_lone_cr_as_one_break_and_crlf_as_one_break() {
    assert_eq!(count_line_breaks(""), 0);
    assert_eq!(count_line_breaks("no breaks"), 0);
    assert_eq!(count_line_breaks("\r"), 1);
    assert_eq!(count_line_breaks("a\rb\rc"), 2);
    assert_eq!(count_line_breaks("a\r\nb"), 1);
    assert_eq!(count_line_breaks("a\nb\nc"), 2);
}

#[test]
fn lines_from_file_contents_matches_compute_line_offsets_line_count() {
    for contents in ["", "hello", "hello\n", "a\nb\n", "a\nb", "\n"] {
        assert_eq!(
            lines_from_file_contents(contents).len(),
            compute_line_offsets(contents).len(),
            "{contents:?}"
        );
    }
}

#[test]
fn preserves_newlines_so_windowed_joins_reconstruct_the_file() {
    let contents = "hello\n";
    let lines = lines_from_file_contents(contents);

    assert_eq!(lines, ["hello\n", ""]);
    assert_eq!(lines.concat(), contents);
}
