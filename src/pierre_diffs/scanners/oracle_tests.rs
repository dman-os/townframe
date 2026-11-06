//! Differential tests: the regexes of upstream `constants.ts` were run through
//! the published `dist` over a corpus and ~800 fuzzed strings built from
//! line-terminator, whitespace and header fragments
//! (`oracle/golden/scanners.json`); each scanner must agree on splits, match
//! offsets (compared in UTF-16 units) and capture groups. Test-only, original
//! to this crate.

use serde::Deserialize;

use crate::oracle_testing::load_golden;
use crate::scanners::has_git_diff_file_break;
use crate::scanners::is_merge_conflict_base_marker;
use crate::scanners::is_merge_conflict_end_marker;
use crate::scanners::is_merge_conflict_separator_marker;
use crate::scanners::is_merge_conflict_start_marker;
use crate::scanners::match_filename_header;
use crate::scanners::match_hunk_header;
use crate::scanners::match_index_line;
use crate::scanners::split_commit_metadata;
use crate::scanners::split_file_context_blobs;
use crate::scanners::split_git_diff_files;
use crate::scanners::split_unified_diff_files;
use crate::scanners::split_with_newlines;
use crate::utf16;

#[derive(Deserialize)]
struct Golden {
    cases: Vec<Case>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Case {
    text: String,
    splits: Splits,
    split_with_newlines: Vec<String>,
    git_diff_file_break_test: bool,
    hunk_header: Option<MatchRecord>,
    filename_header: Option<MatchRecord>,
    index_line: Option<MatchRecord>,
    merge_conflict: MergeConflict,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Splits {
    commit_metadata_split: Vec<String>,
    git_diff_file_break: Vec<String>,
    unified_diff_file_break: Vec<String>,
    file_context_blob: Vec<String>,
}

#[derive(Deserialize, Debug, PartialEq, Eq)]
struct MatchRecord {
    index: usize,
    length: usize,
    groups: Vec<Option<String>>,
}

#[derive(Deserialize)]
struct MergeConflict {
    start: bool,
    base: bool,
    separator: bool,
    end: bool,
}

fn owned(pieces: Vec<&str>) -> Vec<String> {
    pieces.into_iter().map(str::to_owned).collect()
}

fn group(text: &str) -> Option<String> {
    Some(text.to_owned())
}

#[test]
fn scanners_match_the_javascript_regexes() {
    let golden: Golden = load_golden("scanners.json");
    assert!(golden.cases.len() > 500);
    for case in &golden.cases {
        let text = case.text.as_str();
        let context = format!("text={text:?}");

        assert_eq!(
            owned(split_commit_metadata(text)),
            case.splits.commit_metadata_split,
            "COMMIT_METADATA_SPLIT {context}"
        );
        assert_eq!(
            owned(split_git_diff_files(text)),
            case.splits.git_diff_file_break,
            "GIT_DIFF_FILE_BREAK_REGEX {context}"
        );
        assert_eq!(
            owned(split_unified_diff_files(text)),
            case.splits.unified_diff_file_break,
            "UNIFIED_DIFF_FILE_BREAK_REGEX {context}"
        );
        assert_eq!(
            owned(split_file_context_blobs(text)),
            case.splits.file_context_blob,
            "FILE_CONTEXT_BLOB {context}"
        );
        assert_eq!(
            owned(split_with_newlines(text)),
            case.split_with_newlines,
            "SPLIT_WITH_NEWLINES {context}"
        );
        assert_eq!(
            has_git_diff_file_break(text),
            case.git_diff_file_break_test,
            "GIT_DIFF_FILE_BREAK_REGEX.test {context}"
        );

        let hunk_header = match_hunk_header(text).map(|found| MatchRecord {
            index: utf16::utf16_offset(text, found.index),
            length: utf16::len(&text[found.index..found.index + found.length]),
            groups: vec![
                group(found.old_start),
                found.old_count.and_then(group),
                group(found.new_start),
                found.new_count.and_then(group),
                found.context.and_then(group),
            ],
        });
        assert_eq!(hunk_header, case.hunk_header, "HUNK_HEADER {context}");

        let filename_header = match_filename_header(text).map(|found| MatchRecord {
            index: 0,
            length: utf16::len(&text[..found.length]),
            groups: vec![group(found.marker), group(found.name)],
        });
        assert_eq!(
            filename_header, case.filename_header,
            "FILENAME_HEADER_REGEX {context}"
        );

        let index_line = match_index_line(text).map(|found| MatchRecord {
            index: 0,
            length: utf16::len(text),
            groups: vec![
                group(found.old_id),
                group(found.new_id),
                found.mode.and_then(group),
            ],
        });
        assert_eq!(index_line, case.index_line, "INDEX_LINE_METADATA {context}");

        assert_eq!(
            is_merge_conflict_start_marker(text),
            case.merge_conflict.start,
            "MERGE_CONFLICT_START_MARKER_REGEX {context}"
        );
        assert_eq!(
            is_merge_conflict_base_marker(text),
            case.merge_conflict.base,
            "MERGE_CONFLICT_BASE_MARKER_REGEX {context}"
        );
        assert_eq!(
            is_merge_conflict_separator_marker(text),
            case.merge_conflict.separator,
            "MERGE_CONFLICT_SEPARATOR_MARKER_REGEX {context}"
        );
        assert_eq!(
            is_merge_conflict_end_marker(text),
            case.merge_conflict.end,
            "MERGE_CONFLICT_END_MARKER_REGEX {context}"
        );
    }
}
