// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/parseDiffFromFile.test.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: bun:test to nextest; test titles are snake_case. The diff text now
// comes from `similar`, so the assertions that depend on jsdiff's exact hunks
// are invariants (listed per test and in PARITY.md). Tests that need
// upstream's `apps/demo` fixtures `fileOld`/`fileNew` (absent from the
// extracted tree) run on the `diffRowBufferTest` pair from `test/mocks.ts`.
// `throws when both file sides are missing` is unrepresentable: inputs are a
// `DiffFileInput`, which always has a side.

use crate::diff::PatchOptions;
use crate::test_utils::verify_hunk_line_values;
use crate::types::patch::ChangeContent;
use crate::types::patch::ChangeTypes;
use crate::types::patch::DiffFileInput;
use crate::types::patch::FileContents;
use crate::types::patch::FileDiffMetadata;
use crate::types::patch::HunkContent;
use crate::types::patch::ParseDiffOptions;
use crate::utils::are_diff_targets_equal::are_diff_targets_equal;
use crate::utils::get_filetype_from_file_name::get_filetype_from_file_name;
use crate::utils::parse_diff_from_file::parse_diff_from_file;
use crate::utils::split_file_contents::split_file_contents;

const FILE_OLD: &str = include_str!("../oracle/corpus/diff_row_buffer_old.tsx");
const FILE_NEW: &str = include_str!("../oracle/corpus/diff_row_buffer_new.tsx");

fn file(name: &str, contents: &str) -> FileContents {
    FileContents {
        name: name.to_owned(),
        contents: contents.to_owned(),
        lang: None,
        header: None,
        cache_key: None,
    }
}

fn keyed(name: &str, contents: &str, cache_key: &str) -> FileContents {
    FileContents {
        cache_key: Some(cache_key.to_owned()),
        ..file(name, contents)
    }
}

fn both(old_file: &FileContents, new_file: &FileContents) -> DiffFileInput {
    DiffFileInput::Both {
        old_file: old_file.clone(),
        new_file: new_file.clone(),
    }
}

fn parse(input: &DiffFileInput) -> FileDiffMetadata {
    parse_with(input, &ParseDiffOptions::default())
}

fn parse_with(input: &DiffFileInput, options: &ParseDiffOptions) -> FileDiffMetadata {
    parse_diff_from_file(input, options, false)
        .expect("valid input")
        .value
}

fn disable_line_matching(disable: bool) -> ParseDiffOptions {
    ParseDiffOptions {
        disable_line_matching: Some(disable),
        ..ParseDiffOptions::default()
    }
}

#[test]
fn preserves_filename_across_changes_and_missing_sides() {
    for name in [
        "docs/über.md",
        "src/日本語.ts",
        "src/😀.ts",
        "weird\"quote.ts",
        "slash\\name.ts",
        "control\tname\n.ts",
        "space name.ts",
        "src/normal.ts",
        "a/über.ts",
        "b/über.ts",
        " leading-über.ts ",
        r"literal\303\274.ts",
    ] {
        let old_file = file(name, "old\n");
        let new_file = file(name, "new\n");
        let cases = [
            (both(&old_file, &new_file), ChangeTypes::Change),
            (both(&old_file, &old_file), ChangeTypes::Change),
            (
                DiffFileInput::Added {
                    new_file: new_file.clone(),
                },
                ChangeTypes::New,
            ),
            (
                DiffFileInput::Deleted {
                    old_file: old_file.clone(),
                },
                ChangeTypes::Deleted,
            ),
        ];
        for (input, change_type) in cases {
            let diff = parse(&input);
            assert_eq!(diff.name, name);
            assert_eq!(diff.prev_name, None, "{name}");
            assert_eq!(diff.change_type, change_type, "{name}");
            assert_eq!(
                verify_hunk_line_values(&diff, "file"),
                Vec::<String>::new(),
                "{name}"
            );
        }
    }
}

#[test]
fn decodes_renamed_files_with_content_changes() {
    for changed in [false, true] {
        let diff = parse(&both(
            &file("docs/über.md", "old\n"),
            &file("src/日本語.ts", if changed { "new\n" } else { "old\n" }),
        ));
        assert_eq!(diff.prev_name.as_deref(), Some("docs/über.md"));
        assert_eq!(diff.name, "src/日本語.ts");
        assert_eq!(
            diff.change_type,
            if changed {
                ChangeTypes::RenameChanged
            } else {
                ChangeTypes::RenamePure
            }
        );
        assert_eq!(get_filetype_from_file_name("docs/über.md"), "markdown");
        assert_eq!(get_filetype_from_file_name(&diff.name), "typescript");
    }
}

fn row_buffer_diff() -> FileDiffMetadata {
    parse(&both(
        &file("fileOld.txt", FILE_OLD),
        &file("fileNew.txt", FILE_NEW),
    ))
}

#[test]
fn should_parse_diff_from_file_old_and_file_new_and_match_its_digest() {
    // Upstream locks a snapshot of per-hunk geometry for its demo fixture; the
    // geometry depends on jsdiff's hunk boundaries, so this asserts the
    // properties of the result instead (invariant).
    let result = row_buffer_diff();
    assert!(!result.hunks.is_empty());
    assert_eq!(result.cache_key, None);
    assert_eq!(result.change_type, ChangeTypes::RenameChanged);
    assert_eq!(result.prev_name.as_deref(), Some("fileOld.txt"));
    assert_eq!(result.name, "fileNew.txt");
}

#[test]
fn should_have_accurate_hunk_line_values() {
    assert_eq!(
        verify_hunk_line_values(&row_buffer_diff(), "file"),
        Vec::<String>::new()
    );
}

#[test]
fn should_correctly_set_old_lines_and_new_lines() {
    let result = row_buffer_diff();
    // oldLines should match the split of fileOld
    assert_eq!(
        result.deletion_lines.len(),
        FILE_OLD.split_inclusive('\n').count()
    );
    // newLines should match the split of fileNew
    assert_eq!(
        result.addition_lines.len(),
        FILE_NEW.split_inclusive('\n').count()
    );
    assert_eq!(result.addition_lines.concat(), FILE_NEW);
    assert_eq!(result.deletion_lines.concat(), FILE_OLD);
}

#[test]
fn ignore_whitespace_hides_leading_trailing_whitespace_changes() {
    let old_file = file("test.txt", "hello world\nfoo bar\n");
    let new_file = file("test.txt", "  hello world\nfoo bar\n");
    let input = both(&old_file, &new_file);

    assert!(!parse(&input).hunks.is_empty());

    let ignoring = PatchOptions {
        ignore_whitespace: true,
        ..PatchOptions::default()
    };
    let without_whitespace = parse_with(
        &input,
        &ParseDiffOptions {
            create_patch: ignoring,
            ..ParseDiffOptions::default()
        },
    );
    assert!(without_whitespace.hunks.is_empty());

    let without_matching = parse_with(
        &input,
        &ParseDiffOptions {
            create_patch: ignoring,
            disable_line_matching: Some(true),
        },
    );
    assert!(without_matching.hunks.is_empty());
}

#[test]
fn preserves_jsdiff_context_options_when_line_matching_is_disabled() {
    let input = both(
        &file("test.txt", "before\nold\nafter\n"),
        &file("test.txt", "before\nnew\nafter\n"),
    );
    let diff = parse_diff_from_file(
        &input,
        &ParseDiffOptions {
            create_patch: PatchOptions {
                context: Some(0),
                ..PatchOptions::default()
            },
            disable_line_matching: Some(true),
        },
        true,
    )
    .expect("valid")
    .value;

    assert_eq!(diff.hunks.len(), 1);
    assert_eq!(
        diff.hunks[0].hunk_content,
        [HunkContent::Change(ChangeContent {
            deletions: 1,
            additions: 1,
            deletion_line_index: 1,
            addition_line_index: 1,
        })]
    );
    assert_eq!(verify_hunk_line_values(&diff, "file"), Vec::<String>::new());
}

#[test]
fn should_have_type_change_default_when_files_did_not_change() {
    let result = parse(&both(&file("test.txt", "abc"), &file("test.txt", "abc")));
    assert_eq!(result.change_type, ChangeTypes::Change);
    assert_eq!(result.cache_key, None);
}

#[test]
fn should_have_type_change_default_when_empty_files_did_not_change() {
    let result = parse(&both(&file("test.txt", ""), &file("test.txt", "")));
    assert_eq!(result.change_type, ChangeTypes::Change);
    assert_eq!(result.cache_key, None);
}

#[test]
fn uses_file_cache_keys_when_both_sides_provide_them() {
    let result = parse(&both(
        &keyed("test.txt", "old\n", "old-cache"),
        &keyed("test.txt", "new\n", "new-cache"),
    ));
    assert_eq!(
        result.cache_key.as_deref(),
        Some(r#"ck1:["diff","old-cache","new-cache"]"#)
    );
}

#[test]
fn gives_different_matching_modes_distinct_cache_identities() {
    let input = both(
        &keyed("test.txt", "const value = 1;\n", "old-cache"),
        &keyed("test.txt", "\nconst value = 2;\n", "new-cache"),
    );
    let default_diff = parse(&input);
    let matched_diff = parse_with(&input, &disable_line_matching(false));
    let unmatched_diff = parse_with(&input, &disable_line_matching(true));

    assert_eq!(matched_diff.cache_key, default_diff.cache_key);
    assert!(are_diff_targets_equal(
        Some(&default_diff),
        Some(&matched_diff)
    ));
    assert_ne!(unmatched_diff.hunks, default_diff.hunks);
    assert_eq!(
        unmatched_diff.cache_key.as_deref(),
        Some(r#"ck1:["diff-raw","old-cache","new-cache"]"#)
    );
    assert!(!are_diff_targets_equal(
        Some(&default_diff),
        Some(&unmatched_diff)
    ));
    assert_eq!(
        parse_with(&input, &disable_line_matching(true)).cache_key,
        unmatched_diff.cache_key
    );
}

#[test]
fn does_not_invent_source_keys_when_line_matching_is_disabled() {
    let keyed_file = keyed("test.txt", "keyed\n", "source-key");
    let unkeyed_file = file("test.txt", "unkeyed\n");
    let inputs = [
        both(&keyed_file, &unkeyed_file),
        both(&unkeyed_file, &keyed_file),
        both(&unkeyed_file, &unkeyed_file),
        DiffFileInput::Added {
            new_file: keyed_file.clone(),
        },
        DiffFileInput::Deleted {
            old_file: keyed_file.clone(),
        },
    ];
    for input in inputs {
        assert_eq!(
            parse_with(&input, &disable_line_matching(true)).cache_key,
            None
        );
    }
}

#[test]
fn encodes_caller_cache_key_segments_without_delimiter_collisions() {
    for disable in [false, true] {
        let parse_with_keys = |old_cache_key: &str, new_cache_key: &str| {
            parse_with(
                &both(
                    &keyed("test.txt", "old\n", old_cache_key),
                    &keyed("test.txt", "new\n", new_cache_key),
                ),
                &disable_line_matching(disable),
            )
            .cache_key
        };
        let first_key = parse_with_keys("a:b", "c");
        let second_key = parse_with_keys("a", "b:c");
        assert_ne!(first_key, second_key);
        assert_eq!(parse_with_keys("a:b", "c"), first_key);
    }
}

#[test]
fn leaves_cache_key_unset_when_either_side_of_a_two_sided_diff_is_unkeyed() {
    let keyed_old = keyed("test.txt", "old\n", "old-cache");
    let keyed_new = keyed("test.txt", "new\n", "new-cache");
    for contents in ["first unkeyed version\n", "second unkeyed version\n"] {
        assert_eq!(
            parse(&both(&keyed_old, &file("test.txt", contents))).cache_key,
            None
        );
    }
    assert_eq!(
        parse(&both(
            &file("test.txt", "unkeyed old version\n"),
            &keyed_new
        ))
        .cache_key,
        None
    );
}

#[test]
fn leaves_cache_key_unset_when_file_cache_keys_are_omitted() {
    let result = parse(&both(
        &file("old-name.txt", "old\n"),
        &file("new-name.txt", "new\n"),
    ));
    assert_eq!(result.cache_key, None);
}

#[test]
fn parses_a_new_file_from_a_missing_old_side() {
    let new_file = FileContents {
        lang: Some(String::from("typescript")),
        cache_key: Some(String::from("created-cache")),
        ..file("created.ts", "const created = true;\n")
    };
    let result = parse(&DiffFileInput::Added {
        new_file: new_file.clone(),
    });

    assert_eq!(result.change_type, ChangeTypes::New);
    assert_eq!(result.name, "created.ts");
    assert_eq!(result.prev_name, None);
    assert_eq!(result.lang.as_deref(), Some("typescript"));
    assert!(!result.is_partial);
    assert!(result.deletion_lines.is_empty());
    assert_eq!(
        result.addition_lines,
        split_file_contents(&new_file.contents)
    );
    assert_eq!(result.cache_key, None);
    assert_eq!(
        verify_hunk_line_values(&result, "file"),
        Vec::<String>::new()
    );
}

#[test]
fn parses_a_deleted_file_from_a_missing_new_side() {
    let old_file = FileContents {
        lang: Some(String::from("typescript")),
        cache_key: Some(String::from("deleted-cache")),
        ..file("deleted.ts", "const deleted = true;\n")
    };
    let result = parse(&DiffFileInput::Deleted {
        old_file: old_file.clone(),
    });

    assert_eq!(result.change_type, ChangeTypes::Deleted);
    assert_eq!(result.name, "deleted.ts");
    assert_eq!(result.prev_name, None);
    assert_eq!(result.lang.as_deref(), Some("typescript"));
    assert!(!result.is_partial);
    assert_eq!(
        result.deletion_lines,
        split_file_contents(&old_file.contents)
    );
    assert!(result.addition_lines.is_empty());
    assert_eq!(result.cache_key, None);
    assert_eq!(
        verify_hunk_line_values(&result, "file"),
        Vec::<String>::new()
    );
}

#[test]
fn preserves_new_and_deleted_intent_for_empty_files() {
    let empty_file = file("empty.ts", "");
    let added = parse(&DiffFileInput::Added {
        new_file: empty_file.clone(),
    });
    let deleted = parse(&DiffFileInput::Deleted {
        old_file: empty_file,
    });

    assert_eq!(added.change_type, ChangeTypes::New);
    assert_eq!(added.cache_key, None);
    assert_eq!(deleted.change_type, ChangeTypes::Deleted);
    assert_eq!(deleted.cache_key, None);
}
