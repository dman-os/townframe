// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/parsePatchFiles.test.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: bun:test to nextest; console.error spies are Parsed::warnings; toThrow is Err; snapshot and renderer-dependent tests handled as listed below.

//! Port of `packages/diffs/test/parsePatchFiles.test.ts` (tag diffs-v1.6.4).
//! Test names are the upstream `test()` titles in snake_case. `console.error`
//! spies become assertions on `Parsed::warnings`; `toThrow` becomes `Err`.
//!
//! Not ported, with the reason (also in `PARITY.md`):
//! * `should parse diff.patch and match its digest snapshot`: the 400 KB
//!   `diff.patch` lives in upstream's `apps/demo`, outside the extracted
//!   `packages/diffs` tree; `patches.json` covers the published parser over
//!   the corpus instead.
//! * `preserves lone surrogate characters in parsed hunk lines`: a lone
//!   surrogate is not representable in a Rust `String`.
//! * `splitLineCount/unifiedLineCount should match rendered line count`: need
//!   `DiffHunksRenderer` (S6); the same counts are checked against the hunk
//!   content by `verify_patch_hunk_values` in every test here.
//! * `toMatchSnapshot` assertions are replaced by the facts the snapshots
//!   encode plus exact comparison against the published parser in
//!   `utils::oracle_tests`.

use crate::test_utils::verify_file_diff_hunk_values;
use crate::test_utils::verify_patch_hunk_values;
use crate::types::patch::ChangeContent;
use crate::types::patch::FileDiffMetadata;
use crate::types::patch::HunkContent;
use crate::types::patch::ParsePatchOptions;
use crate::types::patch::ParsedPatch;
use crate::types::patch::signed;
use crate::utils::compose_cache_key::compose_cache_key;
use crate::utils::get_total_line_count_from_hunks::get_total_line_count_from_hunks;
use crate::utils::parse_patch_files::Parsed;
use crate::utils::parse_patch_files::ProcessFileOptions;
use crate::utils::parse_patch_files::parse_patch_files;
use crate::utils::parse_patch_files::process_file;
use crate::utils::parse_patch_files::process_patch;

const MALFORMED_PATCH: &str = include_str!("../oracle/corpus/malformed.patch");
const FINAL_BLANK_LINE_PATCH: &str = include_str!("../oracle/corpus/final_blank_line.patch");
const FORMAT_PATCH_WITH_VERSION_TRAILER: &str =
    include_str!("../oracle/corpus/format_patch_with_version_trailer.patch");

const ISSUE_1094_PATCH: &str = "--- f\n+++ f\n@@ -1,6 +1,6 @@\n-a\n+A\n c0\n c1\n-b\n+B\n";

fn valid_issue_1094_patch() -> String {
    ISSUE_1094_PATCH.replace("@@ -1,6 +1,6 @@", "@@ -1,4 +1,4 @@")
}

fn create_file_patch(name: &str) -> String {
    format!(
        "diff --git a/{name} b/{name}\nindex 1111111..2222222 100644\n--- a/{name}\n+++ b/{name}\n@@ -1 +1 @@\n-old\n+new\n"
    )
}

fn strict() -> ParsePatchOptions {
    ParsePatchOptions {
        throw_on_error: Some(true),
        ..ParsePatchOptions::default()
    }
}

fn parse(data: &str, options: &ParsePatchOptions) -> Parsed<Vec<ParsedPatch>> {
    parse_patch_files(data, options).expect("forgiving or valid input parses")
}

pub(super) fn parse_strict(data: &str) -> Vec<ParsedPatch> {
    parse_patch_files(data, &strict())
        .expect("valid input")
        .value
}

fn parse_error(data: &str, options: &ParsePatchOptions) -> String {
    parse_patch_files(data, options)
        .expect_err("strict mode errors")
        .message
}

pub(super) fn file_of(patch: &str) -> FileDiffMetadata {
    process_file(
        patch,
        ProcessFileOptions {
            throw_on_error: Some(true),
            ..ProcessFileOptions::default()
        },
    )
    .expect("valid input")
    .value
    .expect("a file")
}

fn first_file(parsed: &Parsed<Vec<ParsedPatch>>) -> &FileDiffMetadata {
    &parsed.value[0].files[0]
}

pub(super) fn lines(strings: &[&str]) -> Vec<String> {
    strings.iter().map(|line| (*line).to_owned()).collect()
}

// ---- describe.each([parsePatchFiles, processPatch]) options ----

fn file_keys(patches: &[ParsedPatch]) -> Vec<Option<String>> {
    patches[0]
        .files
        .iter()
        .map(|file| file.cache_key.clone())
        .collect()
}

#[test]
fn parse_patch_files_options_derives_file_keys_from_cache_prefix() {
    let patch = format!(
        "{}{}",
        create_file_patch("one.txt"),
        create_file_patch("two.txt")
    );
    for cache_key_prefix in [None, Some(""), Some("patch-prefix")] {
        let options = ParsePatchOptions {
            cache_key_prefix: cache_key_prefix.map(str::to_owned),
            throw_on_error: Some(true),
            disable_line_matching: Some(false),
        };
        // parsePatchFiles: keys carry the patch index and the file index.
        let keys = file_keys(&parse(&patch, &options).value);
        // processPatch: keys carry only the file index.
        let process_keys = file_keys(&[process_patch(&patch, &options).expect("valid").value]);
        match cache_key_prefix {
            None => {
                assert_eq!(keys, [None, None]);
                assert_eq!(process_keys, [None, None]);
            }
            Some(prefix) => {
                assert_eq!(
                    keys,
                    [
                        Some(compose_cache_key("patch-file", &[prefix, "0", "0"])),
                        Some(compose_cache_key("patch-file", &[prefix, "0", "1"])),
                    ]
                );
                assert_eq!(
                    process_keys,
                    [
                        Some(compose_cache_key("patch-file", &[prefix, "0"])),
                        Some(compose_cache_key("patch-file", &[prefix, "1"])),
                    ]
                );
            }
        }
        assert_eq!(
            parse(&patch, &ParsePatchOptions::default()),
            parse(&patch, &ParsePatchOptions::default())
        );
    }
}

#[test]
fn parse_patch_files_options_honors_strict_errors_with_disable_line_matching() {
    for disable_line_matching in [false, true] {
        let options = ParsePatchOptions {
            throw_on_error: Some(true),
            disable_line_matching: Some(disable_line_matching),
            ..ParsePatchOptions::default()
        };
        assert!(parse_error(MALFORMED_PATCH, &options).contains("invalid hunk line"));
        assert!(
            process_patch(MALFORMED_PATCH, &options)
                .expect_err("strict")
                .message
                .contains("invalid hunk line")
        );
    }
}

#[test]
fn parse_patch_files_options_separates_matching_modes_for_cache_prefix() {
    let patch = format!(
        "{}{}",
        create_file_patch("one.txt"),
        create_file_patch("two.txt")
    );
    for cache_key_prefix in [
        None,
        Some(""),
        Some("prefix-0:1"),
        Some("prefix\",\"no-line-matching"),
    ] {
        let get_keys = |disable_line_matching: Option<bool>| {
            file_keys(
                &parse(
                    &patch,
                    &ParsePatchOptions {
                        cache_key_prefix: cache_key_prefix.map(str::to_owned),
                        disable_line_matching,
                        throw_on_error: Some(true),
                    },
                )
                .value,
            )
        };
        let default_keys = get_keys(None);
        let unmatched_keys = get_keys(Some(true));
        assert_eq!(get_keys(Some(false)), default_keys);
        assert_eq!(get_keys(Some(true)), unmatched_keys);
        if cache_key_prefix.is_none() {
            assert_eq!(unmatched_keys, [None, None]);
        } else {
            let distinct: std::collections::BTreeSet<_> =
                default_keys.iter().chain(&unmatched_keys).collect();
            assert_eq!(distinct.len(), 4);
        }
    }
}

#[test]
fn keeps_forgiving_recovery_as_the_options_object_default() {
    let with_options = parse(MALFORMED_PATCH, &ParsePatchOptions::default());
    assert_eq!(
        with_options,
        parse(MALFORMED_PATCH, &ParsePatchOptions::default())
    );
    assert!(!with_options.warnings.is_empty());
    let processed =
        process_patch(MALFORMED_PATCH, &ParsePatchOptions::default()).expect("forgiving");
    assert!(!processed.warnings.is_empty());
}

// ---- describe('parsePatchFiles') ----

#[test]
fn preserves_the_line_16_blank_insertion_from_issue_1139() {
    let patch = [
        "diff --git a/x.ts b/x.ts",
        "--- a/x.ts",
        "+++ b/x.ts",
        "@@ -13,6 +13,7 @@",
        " // the cheap failure is charging full price for a discount we could not see,",
        " // and a provider that renames a field should not silently start billing double.",
        " ",
        "+",
        " /** One completion's token cost, split by what it actually costs to serve. */",
        " export interface CompletionUsage {",
        "   /** Prompt tokens the upstream had to process, cache reads EXCLUDED. */",
        "",
    ]
    .join("\n");

    let files = [
        process_file(
            &patch,
            ProcessFileOptions {
                is_git_diff: Some(true),
                throw_on_error: Some(true),
                ..ProcessFileOptions::default()
            },
        )
        .expect("valid")
        .value
        .expect("a file"),
        process_patch(&patch, &strict())
            .expect("valid")
            .value
            .files
            .remove(0),
        parse_strict(&patch).remove(0).files.remove(0),
    ];
    for file in files {
        assert!(file.is_partial);
        let hunk = &file.hunks[0];
        let change = hunk
            .hunk_content
            .iter()
            .find_map(|content| match content {
                HunkContent::Change(change) => Some(change),
                HunkContent::Context(_) => None,
            })
            .expect("a blank-line insertion");
        assert_eq!(change.addition_line_index, 3);
        assert_eq!(
            signed(hunk.addition_start) + change.addition_line_index - hunk.addition_line_index,
            16
        );
        let verified = verify_file_diff_hunk_values(&file);
        assert!(verified.valid, "{:?}", verified.errors);
    }
}

#[test]
fn preserves_prefix_at_blank_run_offset_in_each_hunk() {
    for prefix in ['+', '-'] {
        for offset in 0..=2usize {
            let mut blank_lines = vec![String::from(" "), String::from(" ")];
            blank_lines.insert(offset, prefix.to_string());
            let mut body_lines = vec![String::from(" anchor")];
            body_lines.extend(blank_lines);
            body_lines.push(String::from(" tail"));
            body_lines.push(String::new());
            let body = body_lines.join("\n");
            let old_count = if prefix == '-' { 5 } else { 4 };
            let new_count = if prefix == '+' { 5 } else { 4 };
            let patch = format!(
                "diff --git a/blanks.ts b/blanks.ts\n--- a/blanks.ts\n+++ b/blanks.ts\n@@ -13,{old_count} +13,{new_count} @@\n{body}@@ -40,{old_count} +{},{new_count} @@\n{body}",
                40 + new_count - old_count
            );
            let file = file_of(&patch);
            assert_eq!(file.hunks.len(), 2);
            for hunk in &file.hunks {
                let change = hunk.hunk_content.iter().find_map(|content| match content {
                    HunkContent::Change(change) => Some(change.clone()),
                    HunkContent::Context(_) => None,
                });
                assert_eq!(
                    change,
                    Some(ChangeContent {
                        additions: usize::from(prefix == '+'),
                        deletions: usize::from(prefix == '-'),
                        addition_line_index: hunk.addition_line_index + 1 + signed(offset),
                        deletion_line_index: hunk.deletion_line_index + 1 + signed(offset),
                    })
                );
            }
            let verified = verify_file_diff_hunk_values(&file);
            assert!(verified.valid, "{:?}", verified.errors);
        }
    }
}

#[test]
fn generates_deterministic_keys_from_patch_and_file_indexes_with_disable_line_matching() {
    for disable_line_matching in [false, true] {
        let scope = if disable_line_matching {
            "patch-file-raw"
        } else {
            "patch-file"
        };
        let prefix = "prefix-0:1";
        let first_patch = format!(
            "{}{}",
            create_file_patch("one.txt"),
            create_file_patch("two.txt")
        );
        let patch_file = [
            "From aaaaa first patch\n",
            &first_patch,
            "From bbbbb second patch\n",
            &create_file_patch("three.txt"),
            &create_file_patch("four.txt"),
        ]
        .concat();
        let options = ParsePatchOptions {
            cache_key_prefix: Some(prefix.to_owned()),
            disable_line_matching: Some(disable_line_matching),
            throw_on_error: Some(true),
        };
        let get_keys = || -> Vec<Vec<Option<String>>> {
            parse(&patch_file, &options)
                .value
                .iter()
                .map(|patch| {
                    patch
                        .files
                        .iter()
                        .map(|file| file.cache_key.clone())
                        .collect()
                })
                .collect()
        };
        let key = |patch: &str, file: &str| Some(compose_cache_key(scope, &[prefix, patch, file]));
        let expected_keys = vec![
            vec![key("0", "0"), key("0", "1")],
            vec![key("1", "0"), key("1", "1")],
        ];
        let keys = get_keys();
        assert_eq!(keys, expected_keys);
        let flat: std::collections::BTreeSet<_> = keys.iter().flatten().collect();
        assert_eq!(flat.len(), 4);
        assert_eq!(get_keys(), keys);

        let direct_prefix = format!("{prefix}-0");
        let direct_keys = file_keys(&[process_patch(
            &first_patch,
            &ParsePatchOptions {
                cache_key_prefix: Some(direct_prefix.clone()),
                ..options.clone()
            },
        )
        .expect("valid")
        .value]);
        assert_eq!(
            direct_keys,
            [
                Some(compose_cache_key(scope, &[&direct_prefix, "0"])),
                Some(compose_cache_key(scope, &[&direct_prefix, "1"])),
            ]
        );
        let all: std::collections::BTreeSet<_> =
            keys.iter().flatten().chain(&direct_keys).collect();
        assert_eq!(all.len(), 6);
    }
}

#[test]
fn keeps_a_direct_process_file_cache_key_authoritative_with_disable_line_matching() {
    for disable_line_matching in [None, Some(false), Some(true)] {
        let cache_key = "caller:key-0-0:hydrated";
        let file = process_file(
            &create_file_patch("direct.txt"),
            ProcessFileOptions {
                cache_key: Some(cache_key.to_owned()),
                disable_line_matching,
                is_git_diff: Some(true),
                throw_on_error: Some(true),
                ..ProcessFileOptions::default()
            },
        )
        .expect("valid")
        .value
        .expect("a file");
        assert_eq!(file.cache_key.as_deref(), Some(cache_key));
    }
}

#[test]
fn patches_with_a_final_blank_line_should_have_a_n_added() {
    let result = parse(FINAL_BLANK_LINE_PATCH, &ParsePatchOptions::default());
    // Facts encoded by the upstream snapshot ("final blank line patch").
    let file = first_file(&result);
    assert_eq!(file.addition_character_count, 338);
    assert_eq!(file.deletion_character_count, 400);
    assert_eq!(file.addition_lines.last().map(String::as_str), Some("\n"));
    assert!(result.warnings.is_empty());
}

#[test]
fn should_have_accurate_hunk_line_values() {
    for patch in [
        FINAL_BLANK_LINE_PATCH,
        FORMAT_PATCH_WITH_VERSION_TRAILER,
        &create_file_patch("a.txt"),
    ] {
        let result = parse(patch, &ParsePatchOptions::default());
        assert_eq!(
            verify_patch_hunk_values(&result.value).errors,
            Vec::<String>::new()
        );
    }
}

#[test]
fn should_warn_on_malformed_patch_with_bare_newline_in_hunk() {
    let result = parse(MALFORMED_PATCH, &ParsePatchOptions::default());
    // Should have logged an error for the invalid line, but should still try
    // to do its best to parse things out
    assert!(result.warnings[0].contains("Invalid firstChar"));
    // The declared count should be repaired to match the usable hunk body.
    let hunk = &first_file(&result).hunks[0];
    assert_eq!(hunk.deletion_count, 86);
    assert_eq!(hunk.deletion_lines, 86);
    assert_eq!(
        verify_patch_hunk_values(&result.value).errors,
        Vec::<String>::new()
    );
}

#[test]
fn throws_in_strict_mode_for_malformed_patch_with_bare_newline_in_hunk() {
    assert!(parse_error(MALFORMED_PATCH, &strict()).contains("invalid hunk line"));
}

#[test]
fn throws_in_strict_mode_when_a_hunk_has_too_few_lines() {
    let patch = "--- incomplete.txt\n+++ incomplete.txt\n@@ -1 +1 @@\n-old line\n";
    assert!(parse_error(patch, &strict()).contains("hunk line count mismatch"));
}

#[test]
fn repairs_issue_1094_hunk_counts_in_forgiving_mode() {
    let result = parse(ISSUE_1094_PATCH, &ParsePatchOptions::default());
    let file = first_file(&result);
    let hunk = &file.hunks[0];

    assert_eq!(result.warnings.len(), 1);
    let warning = &result.warnings[0];
    assert!(warning.contains("parsePatchContent: hunk line count mismatch"));
    assert!(warning.contains("@@ -1,6 +1,6 @@"));
    assert!(warning.contains("declared old/new 6/6"));
    assert!(warning.contains("parsed old/new 4/4"));
    assert_eq!(hunk.addition_count, 4);
    assert_eq!(hunk.deletion_count, 4);
    assert_eq!(file.addition_lines, lines(&["A\n", "c0\n", "c1\n", "B\n"]));
    assert_eq!(file.deletion_lines, lines(&["a\n", "c0\n", "c1\n", "b\n"]));
    let kinds: Vec<&str> = hunk
        .hunk_content
        .iter()
        .map(|content| match content {
            HunkContent::Change(_) => "change",
            HunkContent::Context(_) => "context",
        })
        .collect();
    assert_eq!(kinds, ["change", "context", "change"]);
    assert_eq!(hunk.hunk_specs.as_deref(), Some("@@ -1,6 +1,6 @@\n"));
    assert_eq!(get_total_line_count_from_hunks(&file.hunks), 4);
    assert_eq!(
        verify_patch_hunk_values(&result.value).errors,
        Vec::<String>::new()
    );
}

#[test]
fn throws_for_the_issue_1094_patch_in_strict_mode() {
    assert!(parse_error(ISSUE_1094_PATCH, &strict()).contains("hunk line count mismatch"));
}

#[test]
fn leaves_the_valid_issue_1094_patch_unchanged() {
    let result = parse(&valid_issue_1094_patch(), &ParsePatchOptions::default());
    let file = first_file(&result);
    let hunk = &file.hunks[0];

    assert!(result.warnings.is_empty());
    assert_eq!(hunk.addition_count, 4);
    assert_eq!(hunk.deletion_count, 4);
    assert_eq!(hunk.hunk_specs.as_deref(), Some("@@ -1,4 +1,4 @@\n"));
    assert_eq!(get_total_line_count_from_hunks(&file.hunks), 4);
    assert_eq!(
        verify_patch_hunk_values(&result.value).errors,
        Vec::<String>::new()
    );
}

#[test]
fn repairs_only_the_underfilled_side_of_an_asymmetric_hunk() {
    let result = parse(
        "--- asymmetric.txt\n+++ asymmetric.txt\n@@ -1,3 +1,2 @@\n-old\n+new\n context\n",
        &ParsePatchOptions::default(),
    );
    let hunk = &first_file(&result).hunks[0];
    assert_eq!(result.warnings.len(), 1);
    assert!(result.warnings[0].contains("@@ -1,3 +1,2 @@"));
    assert!(result.warnings[0].contains("declared old/new 3/2"));
    assert!(result.warnings[0].contains("parsed old/new 2/2"));
    assert_eq!(hunk.deletion_count, 2);
    assert_eq!(hunk.addition_count, 2);
    assert_eq!(
        verify_patch_hunk_values(&result.value).errors,
        Vec::<String>::new()
    );
}

#[test]
fn throws_in_strict_mode_when_a_hunk_has_extra_content_lines() {
    let patch =
        "--- extra.txt\n+++ extra.txt\n@@ -1 +1 @@\n-old line\n+new line\n-extra old line\n";
    assert!(parse_error(patch, &strict()).contains("hunk has more lines than expected"));
}

#[test]
fn throws_in_strict_mode_when_a_fake_unified_header_creates_a_file_without_hunks() {
    let patch = "--- markers.txt\n+++ markers.txt\n@@ -1 +1 @@\n--- old marker\n+++ new marker\n--- fake-old-marker\n+++ fake-new-marker\n";
    assert!(parse_error(patch, &strict()).contains("unified file has no hunks"));
}

#[test]
fn ignores_format_patch_version_trailers_after_the_final_hunk() {
    let result = parse(
        FORMAT_PATCH_WITH_VERSION_TRAILER,
        &ParsePatchOptions::default(),
    );
    assert!(result.warnings.is_empty());
    assert_eq!(
        verify_patch_hunk_values(&result.value).errors,
        Vec::<String>::new()
    );
    assert_eq!(first_file(&result).hunks[0].addition_lines, 1);
    assert_eq!(first_file(&result).hunks[0].deletion_lines, 0);
}

#[test]
fn ignores_hunk_looking_patch_metadata_before_unified_file_headers() {
    let result = parse_strict(
        "Patch metadata mentions @@ -1 +1 @@ before the file header.\n@@ -1 +1 @@ is here.\n\n--- metadata.txt\n+++ metadata.txt\n@@ -1 +1 @@\n-old line\n+new line\n",
    );
    assert_eq!(
        result[0].patch_metadata.as_deref(),
        Some(
            "Patch metadata mentions @@ -1 +1 @@ before the file header.\n@@ -1 +1 @@ is here.\n\n"
        )
    );
    assert_eq!(result[0].files.len(), 1);
    assert_eq!(result[0].files[0].name, "metadata.txt");
}

#[test]
fn parses_deleted_sql_comment_lines_as_hunk_content_in_unified_patches() {
    let result = parse_strict(
        "--- sql/test.sql\n+++ sql/test.sql\n@@ -1,5 +1,4 @@\n -- This is a test sql file\n--- This is an sql comment\n \n CREATE TABLE users (\n id BIGSERIAL PRIMARY KEY,\n",
    );
    let file = &result[0].files[0];
    assert_eq!(result[0].files.len(), 1);
    assert_eq!(file.name, "sql/test.sql");
    assert_eq!(
        file.deletion_lines,
        lines(&[
            "-- This is a test sql file\n",
            "-- This is an sql comment\n",
            "\n",
            "CREATE TABLE users (\n",
            "id BIGSERIAL PRIMARY KEY,\n",
        ])
    );
    assert_eq!(
        file.addition_lines,
        lines(&[
            "-- This is a test sql file\n",
            "\n",
            "CREATE TABLE users (\n",
            "id BIGSERIAL PRIMARY KEY,\n",
        ])
    );
}

#[test]
fn does_not_split_hunk_content_that_resembles_unified_file_headers() {
    let result = parse_strict(
        "--- markers.txt\n+++ markers.txt\n@@ -1 +1 @@\n--- old marker\n+++ new marker\n",
    );
    let file = &result[0].files[0];
    assert_eq!(result[0].files.len(), 1);
    assert_eq!(file.name, "markers.txt");
    assert_eq!(file.deletion_lines, lines(&["-- old marker\n"]));
    assert_eq!(file.addition_lines, lines(&["++ new marker\n"]));
}

#[test]
fn preserves_leading_bom_characters_in_parsed_hunk_lines() {
    let result = parse(
        "diff --git a/bom.txt b/bom.txt\nindex 1111111..2222222 100644\n--- a/bom.txt\n+++ b/bom.txt\n@@ -1 +1 @@\n-\u{feff}old\n+\u{feff}new\n",
        &ParsePatchOptions::default(),
    );
    let file = first_file(&result);
    assert_eq!(file.deletion_lines[0], "\u{feff}old\n");
    assert_eq!(file.addition_lines[0], "\u{feff}new\n");
}

#[cfg(test)]
mod headers;

mod repairs;
