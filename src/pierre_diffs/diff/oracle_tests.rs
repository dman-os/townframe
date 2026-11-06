//! Invariant tests for `diff.rs` over the inputs of `oracle/golden/jsdiff.json`
//! (corpus plus seeded fuzz run through the published `diff@9.0.0`). The diff
//! algorithm is `similar`'s, so outputs may legitimately differ from jsdiff's
//! where several minimal diffs exist; what must hold is: change runs
//! reproduce both texts, line-diff totals match jsdiff's (both are minimal
//! Myers), and patches have jsdiff's exact header lines and apply to the old
//! text to produce the new one. Test-only, original to this crate.

use serde::Deserialize;
use serde_json::Value;

use crate::diff::ChangeObject;
use crate::diff::HeaderOptions;
use crate::diff::PatchOptions;
use crate::diff::PatchSide;
use crate::diff::create_two_files_patch;
use crate::diff::diff_chars;
use crate::diff::diff_lines;
use crate::diff::diff_words_with_space;
use crate::oracle_testing::load_golden;
use crate::scanners::match_hunk_header;
use crate::scanners::split_with_newlines;

#[derive(Deserialize)]
struct Golden {
    cases: Vec<DiffCase>,
    patches: Vec<PatchCase>,
}

#[derive(Deserialize)]
struct DiffCase {
    #[serde(rename = "fn")]
    function: String,
    old: String,
    new: String,
    options: Value,
    expected: Option<Vec<ChangeObject>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PatchCase {
    old_name: String,
    new_name: String,
    old: String,
    new: String,
    old_header: Option<String>,
    new_header: Option<String>,
    options: Value,
    expected: Option<String>,
}

fn is_default_options(options: &Value) -> bool {
    options.as_object().is_some_and(serde_json::Map::is_empty)
}

fn joined(changes: &[ChangeObject], skip_added: bool) -> String {
    changes
        .iter()
        .filter(|change| {
            if skip_added {
                !change.added
            } else {
                !change.removed
            }
        })
        .map(|change| change.value.as_str())
        .collect()
}

fn totals(changes: &[ChangeObject]) -> (usize, usize) {
    let sum = |select: fn(&ChangeObject) -> bool| {
        changes
            .iter()
            .filter(|change| select(change))
            .map(|change| change.count)
            .sum()
    };
    (sum(|change| change.removed), sum(|change| change.added))
}

#[test]
fn change_runs_reproduce_both_texts() {
    let golden: Golden = load_golden("jsdiff.json");
    let mut line_cases = 0;
    let mut other_cases = 0;
    for case in golden
        .cases
        .iter()
        .filter(|case| is_default_options(&case.options))
    {
        let changes = match case.function.as_str() {
            "diffLines" => diff_lines(&case.old, &case.new),
            "diffChars" => diff_chars(&case.old, &case.new),
            "diffWordsWithSpace" | "diffWords" => diff_words_with_space(&case.old, &case.new),
            _ => continue,
        };
        let context = format!("{} old={:?} new={:?}", case.function, case.old, case.new);
        assert_eq!(joined(&changes, true), case.old, "old side: {context}");
        assert_eq!(joined(&changes, false), case.new, "new side: {context}");
        assert!(
            changes.windows(2).all(|pair| {
                (pair[0].added, pair[0].removed) != (pair[1].added, pair[1].removed)
            }),
            "adjacent runs must differ in kind: {context}"
        );
        assert!(
            changes
                .iter()
                .all(|change| change.count > 0 && !change.value.is_empty())
        );
        if case.function == "diffLines" {
            line_cases += 1;
            let expected = case
                .expected
                .as_ref()
                .expect("no maxEditLength in default cases");
            assert_eq!(
                totals(&changes),
                totals(expected),
                "line edit totals: {context}"
            );
        } else {
            other_cases += 1;
        }
    }
    assert!(line_cases > 10, "{line_cases}");
    assert!(other_cases > 200, "{other_cases}");
}

/// Applies the hunks of `patch` (our output) to `old` and returns the result,
/// checking every context and deleted line against `old` and every hunk
/// header count against its body.
fn apply(patch: &str, old: &str) -> String {
    let old_lines: Vec<&str> = if old.is_empty() {
        Vec::new()
    } else {
        split_with_newlines(old)
    };
    let mut output = String::new();
    let mut cursor = 0;
    let lines: Vec<&str> = patch
        .strip_suffix('\n')
        .expect("patch ends in newline")
        .split('\n')
        .collect();
    let mut index = lines
        .iter()
        .position(|line| line.starts_with("@@ "))
        .unwrap_or(lines.len());
    while index < lines.len() {
        let header = match_hunk_header(lines[index]).expect("hunk header");
        let old_start: usize = header.old_start.parse().expect("number");
        let old_count: usize = header
            .old_count
            .expect("count always present")
            .parse()
            .expect("number");
        let new_count: usize = header
            .new_count
            .expect("count always present")
            .parse()
            .expect("number");
        // Empty ranges are written one line early.
        let first_old_index = if old_count == 0 {
            old_start
        } else {
            old_start - 1
        };
        output.extend(old_lines[cursor..first_old_index].iter().copied());
        cursor = first_old_index;
        index += 1;
        let (mut seen_old, mut seen_new) = (0, 0);
        while index < lines.len() && !lines[index].starts_with("@@ ") {
            let line = lines[index];
            let (sign, text) = line.split_at(1);
            // A marker line says the previous line has no terminator.
            let has_newline = lines.get(index + 1) != Some(&"\\ No newline at end of file");
            let full = if has_newline {
                format!("{text}\n")
            } else {
                text.to_owned()
            };
            if sign == " " || sign == "-" {
                assert_eq!(old_lines[cursor], full, "old line {cursor}");
                cursor += 1;
                seen_old += 1;
            }
            if sign == " " || sign == "+" {
                output.push_str(&full);
                seen_new += 1;
            }
            index += if has_newline { 1 } else { 2 };
        }
        assert_eq!(
            (seen_old, seen_new),
            (old_count, new_count),
            "{}",
            lines[index - 1]
        );
    }
    output.extend(old_lines[cursor..].iter().copied());
    output
}

#[test]
fn patches_have_jsdiff_headers_and_apply_cleanly() {
    let golden: Golden = load_golden("jsdiff.json");
    let mut checked = 0;
    for case in &golden.patches {
        let object = case.options.as_object().expect("options object");
        if object
            .keys()
            .any(|key| key != "context" && key != "headerOptions")
        {
            continue;
        }
        let options = PatchOptions {
            context: object
                .get("context")
                .and_then(Value::as_u64)
                .map(|context| context as usize),
            header_options: object.get("headerOptions").map(|value| {
                serde_json::from_value::<HeaderOptions>(value.clone()).expect("header options")
            }),
            ignore_whitespace: false,
        };
        let patch = create_two_files_patch(
            PatchSide {
                file_name: &case.old_name,
                contents: &case.old,
                header: case.old_header.as_deref(),
            },
            PatchSide {
                file_name: &case.new_name,
                contents: &case.new,
                header: case.new_header.as_deref(),
            },
            options,
        );
        let context = format!(
            "patch {:?}->{:?} old={:?} new={:?}",
            case.old_name, case.new_name, case.old, case.new
        );
        let expected = case
            .expected
            .as_ref()
            .expect("no maxEditLength in patch cases");
        let header_of = |text: &str| {
            text.split_once("@@ ")
                .map_or(text.to_owned(), |(head, _)| head.to_owned())
        };
        assert_eq!(header_of(&patch), header_of(expected), "headers: {context}");
        assert_eq!(
            patch.contains("@@ "),
            expected.contains("@@ "),
            "has hunks: {context}"
        );
        assert_eq!(apply(&patch, &case.old), case.new, "applying: {context}");
        checked += 1;
    }
    assert!(checked > 60, "{checked}");
}
