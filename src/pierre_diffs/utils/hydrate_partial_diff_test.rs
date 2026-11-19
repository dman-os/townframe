// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/hydratePartialDiff.test.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: bun:test to nextest; test titles are snake_case. jsdiff's
// `createTwoFilesPatch` is `crate::diff::create_two_files_patch` (every fixture
// has a unique minimal diff, so the patches are the same text). `toThrow`
// becomes a panic-message check (hydration contradictions are panics here, see
// the module header); `toBe(partial)` after a `merge` hydration is checked as
// the in-place state of `partial`. The upstream case that passes
// `newFile: null` to a changed diff is unrepresentable (`FileDiffLoadedFiles`
// has no such variant).

use crate::diff::PatchOptions;
use crate::diff::PatchSide;
use crate::diff::create_two_files_patch;
use crate::test_utils::verify_hunk_line_values;
use crate::types::patch::ChangeTypes;
use crate::types::patch::FileContents;
use crate::types::patch::FileDiffLoadedFiles;
use crate::types::patch::FileDiffMetadata;
use crate::types::patch::HunkContent;
use crate::types::patch::ParseDiffOptions;
use crate::types::patch::ParsePatchOptions;
use crate::types::patch::signed;
use crate::utils::compose_cache_key::compose_cache_key;
use crate::utils::hydrate_partial_diff::hydrate_partial_diff_clone;
use crate::utils::hydrate_partial_diff::hydrate_partial_diff_merge;
use crate::utils::parse_patch_files::parse_patch_files;
use crate::utils::predicates::are_diff_targets_equal::are_diff_targets_equal;
use crate::utils::split_file_contents::split_file_contents;

fn parse_single_file(patch: &str, cache_key_prefix: Option<&str>) -> FileDiffMetadata {
    let file = parse_patch_files(
        patch,
        &ParsePatchOptions {
            cache_key_prefix: cache_key_prefix.map(str::to_owned),
            throw_on_error: Some(true),
            ..ParsePatchOptions::default()
        },
    )
    .expect("valid")
    .value
    .remove(0)
    .files
    .remove(0);
    assert!(file.is_partial);
    file
}

fn file(name: &str, contents: &str, cache_key: Option<&str>) -> FileContents {
    FileContents {
        name: name.to_owned(),
        contents: contents.to_owned(),
        lang: None,
        header: None,
        cache_key: cache_key.map(str::to_owned),
    }
}

fn patch_of(old_file: &FileContents, new_file: &FileContents, context: usize) -> String {
    create_two_files_patch(
        PatchSide {
            file_name: &old_file.name,
            contents: &old_file.contents,
            header: None,
        },
        PatchSide {
            file_name: &new_file.name,
            contents: &new_file.contents,
            header: None,
        },
        PatchOptions {
            context: Some(context),
            ..PatchOptions::default()
        },
    )
}

fn partial_of(
    old_file: &FileContents,
    new_file: &FileContents,
    context: usize,
) -> FileDiffMetadata {
    parse_single_file(&patch_of(old_file, new_file, context), None)
}

fn changed(old_file: &FileContents, new_file: &FileContents) -> FileDiffLoadedFiles {
    FileDiffLoadedFiles::Changed {
        old_file: old_file.clone(),
        new_file: new_file.clone(),
    }
}

fn merge(partial: &mut FileDiffMetadata, files: &FileDiffLoadedFiles) {
    hydrate_partial_diff_merge(partial, files, &ParseDiffOptions::default());
}

fn panic_message(action: impl FnOnce()) -> String {
    let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(action))
        .expect_err("expected a panic");
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| {
            payload
                .downcast_ref::<&str>()
                .map(|text| (*text).to_owned())
        })
        .expect("panic with a message")
}

struct PartialLineStateAssertions {
    addition_line_count: usize,
    deletion_line_count: usize,
    full_addition_line_count: Option<usize>,
    full_deletion_line_count: Option<usize>,
}

fn expect_partial_line_state(file_diff: &FileDiffMetadata, expected: PartialLineStateAssertions) {
    assert!(file_diff.is_partial);
    assert_eq!(file_diff.addition_lines.len(), expected.addition_line_count);
    assert_eq!(file_diff.deletion_lines.len(), expected.deletion_line_count);
    if let Some(full) = expected.full_addition_line_count {
        assert_ne!(file_diff.addition_lines.len(), full);
    }
    if let Some(full) = expected.full_deletion_line_count {
        assert_ne!(file_diff.deletion_lines.len(), full);
    }
}

fn content_indexes(content: &HunkContent) -> (i64, i64) {
    match content {
        HunkContent::Context(context) => (context.addition_line_index, context.deletion_line_index),
        HunkContent::Change(change) => (change.addition_line_index, change.deletion_line_index),
    }
}

fn expect_first_partial_hunk_indexes(file_diff: &FileDiffMetadata) {
    let partial_first_hunk = file_diff
        .hunks
        .first()
        .expect("expected partial diff to contain a first hunk");
    let partial_first_content = partial_first_hunk
        .hunk_content
        .first()
        .expect("expected first partial hunk to contain content");
    assert_eq!(partial_first_hunk.addition_line_index, 0);
    assert_eq!(partial_first_hunk.deletion_line_index, 0);
    assert_eq!(content_indexes(partial_first_content), (0, 0));
}

fn owned(lines: Vec<&str>) -> Vec<String> {
    lines.into_iter().map(str::to_owned).collect()
}

#[test]
fn hydrates_two_sided_partial_diffs_with_full_lines_and_rewritten_line_indexes() {
    let old_file = file(
        "two-sided.txt",
        "keep 1\nkeep 2\nold first\nkeep 4\nkeep 5\nkeep 6\nkeep 7\nkeep 8\nold second\nkeep 10\nkeep 11\n",
        Some("old-full"),
    );
    let new_file = file(
        "two-sided.txt",
        "keep 1\nkeep 2\nnew first\nkeep 4\nkeep 5\nkeep 6\nkeep 7\nkeep 8\nnew second\nkeep 10\nkeep 11\n",
        Some("new-full"),
    );
    let mut partial = partial_of(&old_file, &new_file, 1);
    partial.cache_key = Some(String::from("partial-cache"));
    partial.lang = Some(String::from("typescript"));
    partial.mode = Some(String::from("100644"));
    partial.prev_object_id = Some(String::from("1111111"));
    partial.new_object_id = Some(String::from("2222222"));
    let old_file_lines = owned(split_file_contents(&old_file.contents));
    let new_file_lines = owned(split_file_contents(&new_file.contents));
    let partial_hunk_specs: Vec<_> = partial
        .hunks
        .iter()
        .map(|hunk| hunk.hunk_specs.clone())
        .collect();

    expect_partial_line_state(
        &partial,
        PartialLineStateAssertions {
            addition_line_count: 6,
            deletion_line_count: 6,
            full_addition_line_count: Some(new_file_lines.len()),
            full_deletion_line_count: Some(old_file_lines.len()),
        },
    );
    expect_first_partial_hunk_indexes(&partial);

    merge(&mut partial, &changed(&old_file, &new_file));
    let first_hunk = &partial.hunks[0];
    let first_content = &first_hunk.hunk_content[0];

    assert!(!partial.is_partial);
    assert_eq!(partial.change_type, ChangeTypes::Change);
    assert_eq!(partial.lang.as_deref(), Some("typescript"));
    assert_eq!(partial.mode.as_deref(), Some("100644"));
    assert_eq!(partial.prev_object_id.as_deref(), Some("1111111"));
    assert_eq!(partial.new_object_id.as_deref(), Some("2222222"));
    assert_eq!(partial.cache_key.as_deref(), Some("partial-cache:hydrated"));
    assert_eq!(partial.addition_lines, new_file_lines);
    assert_eq!(partial.deletion_lines, old_file_lines);
    let hydrated_specs: Vec<_> = partial
        .hunks
        .iter()
        .map(|hunk| hunk.hunk_specs.clone())
        .collect();
    assert_eq!(hydrated_specs, partial_hunk_specs);
    assert_eq!(
        first_hunk.addition_line_index,
        signed(first_hunk.addition_start) - 1
    );
    assert_eq!(
        first_hunk.deletion_line_index,
        signed(first_hunk.deletion_start) - 1
    );
    assert_eq!(
        content_indexes(first_content),
        (
            signed(first_hunk.addition_start) - 1,
            signed(first_hunk.deletion_start) - 1
        )
    );
    assert_eq!(
        verify_hunk_line_values(&partial, "file"),
        Vec::<String>::new()
    );
}

fn content_shape(content: &HunkContent) -> Vec<usize> {
    match content {
        HunkContent::Change(change) => vec![change.deletions, change.additions],
        HunkContent::Context(context) => vec![context.lines],
    }
}

#[test]
fn clone_and_merge_hydration_keep_matching_modes_distinct() {
    for kind in ["clone", "merge"] {
        for cache_key_prefix in [None, Some("partial")] {
            let patch = "--- file.ts\n+++ file.ts\n@@ -1 +1,2 @@\n-const value = 1;\n+\n+const value = 2;\n";
            let old_file = file("file.ts", "const value = 1;\n", Some("old-full"));
            let new_file = file("file.ts", "\nconst value = 2;\n", Some("new-full"));
            let hydrate = |disable_line_matching: Option<bool>| {
                let partial = parse_patch_files(
                    patch,
                    &ParsePatchOptions {
                        disable_line_matching,
                        cache_key_prefix: cache_key_prefix.map(str::to_owned),
                        throw_on_error: Some(true),
                    },
                )
                .expect("valid")
                .value
                .remove(0)
                .files
                .remove(0);
                let original_key = partial.cache_key.clone();
                let files = changed(&old_file, &new_file);
                let options = ParseDiffOptions {
                    disable_line_matching,
                    ..ParseDiffOptions::default()
                };
                let hydrated = if kind == "clone" {
                    hydrate_partial_diff_clone(&partial, &files, &options)
                } else {
                    let mut merged = partial;
                    hydrate_partial_diff_merge(&mut merged, &files, &options);
                    merged
                };
                assert_eq!(
                    hydrated.cache_key,
                    Some(match original_key {
                        None => compose_cache_key(
                            if disable_line_matching == Some(true) {
                                "hydrated-files-raw"
                            } else {
                                "hydrated-files"
                            },
                            &["old-full", "new-full"],
                        ),
                        Some(original_key) => format!("{original_key}:hydrated"),
                    })
                );
                assert_eq!(
                    verify_hunk_line_values(&hydrated, "file"),
                    Vec::<String>::new()
                );
                hydrated
            };
            let matched = hydrate(None);
            let explicit_matched = hydrate(Some(false));
            let unmatched = hydrate(Some(true));

            let shapes = |diff: &FileDiffMetadata| -> Vec<Vec<usize>> {
                diff.hunks[0]
                    .hunk_content
                    .iter()
                    .map(content_shape)
                    .collect()
            };
            assert_eq!(shapes(&matched), [vec![0, 1], vec![1, 1]]);
            assert_eq!(shapes(&unmatched), [vec![1, 2]]);
            assert!(are_diff_targets_equal(
                Some(&matched),
                Some(&explicit_matched)
            ));
            assert!(!are_diff_targets_equal(Some(&matched), Some(&unmatched)));
        }
    }
}

#[test]
fn uses_loaded_file_cache_keys_without_a_partial_cache_key() {
    let old_file = file("unkeyed.txt", "old value\n", Some("old-full"));
    let new_file = file("unkeyed.txt", "new value\n", Some("new-full"));
    let mut partial = partial_of(&old_file, &new_file, 0);

    merge(&mut partial, &changed(&old_file, &new_file));

    assert_eq!(
        partial.cache_key,
        Some(compose_cache_key(
            "hydrated-files",
            &["old-full", "new-full"]
        ))
    );
}

#[test]
fn encodes_loaded_file_keys_without_delimiter_or_hydration_suffix_collisions() {
    let hydrate_with_keys = |old_cache_key: &str, new_cache_key: &str| {
        let old_file = file("collision.txt", "old value\n", Some(old_cache_key));
        let new_file = file("collision.txt", "new value\n", Some(new_cache_key));
        let mut partial = partial_of(&old_file, &new_file, 0);
        merge(&mut partial, &changed(&old_file, &new_file));
        partial.cache_key
    };

    let first_key = hydrate_with_keys("a:b", "c");
    let second_key = hydrate_with_keys("a", "b:c");

    assert_ne!(first_key, second_key);
    assert_eq!(hydrate_with_keys("a:b", "c"), first_key);
    assert_ne!(
        hydrate_with_keys("a", "b:hydrated").as_deref(),
        Some("a:b:hydrated")
    );
}

#[test]
fn does_not_use_one_loaded_file_cache_key_for_a_two_sided_diff() {
    let old_file = file("one-keyed-side.txt", "old value\n", Some("old-full"));
    let new_file = file("one-keyed-side.txt", "new value\n", None);
    let mut partial = partial_of(&old_file, &new_file, 0);

    merge(&mut partial, &changed(&old_file, &new_file));

    assert_eq!(partial.cache_key, None);
}

#[test]
fn can_hydrate_a_cloned_metadata_object_without_mutating_the_source_diff() {
    let old_file = file(
        "cloned.txt",
        "keep 1\nold value\nkeep 3\n",
        Some("old-full"),
    );
    let new_file = file(
        "cloned.txt",
        "keep 1\nnew value\nkeep 3\n",
        Some("new-full"),
    );
    let partial = partial_of(&old_file, &new_file, 0);

    let hydrated = hydrate_partial_diff_clone(
        &partial,
        &changed(&old_file, &new_file),
        &ParseDiffOptions::default(),
    );

    assert!(!hydrated.is_partial);
    assert_eq!(
        hydrated.addition_lines,
        owned(vec!["keep 1\n", "new value\n", "keep 3\n"])
    );
    assert_eq!(
        hydrated.deletion_lines,
        owned(vec!["keep 1\n", "old value\n", "keep 3\n"])
    );
    assert!(partial.is_partial);
    assert_eq!(partial.addition_lines, owned(vec!["new value\n"]));
    assert_eq!(partial.deletion_lines, owned(vec!["old value\n"]));
}

#[test]
fn falls_back_to_a_hydrated_cache_segment_when_loaded_files_are_unkeyed() {
    let old_file = file("fallback.txt", "old value\n", None);
    let new_file = file("fallback.txt", "new value\n", None);
    let mut partial = partial_of(&old_file, &new_file, 0);
    partial.cache_key = Some(String::from("partial-cache"));

    merge(&mut partial, &changed(&old_file, &new_file));

    assert_eq!(partial.cache_key.as_deref(), Some("partial-cache:hydrated"));
}

#[test]
fn requires_both_files_for_rename_changed_diffs_and_preserves_rename_metadata() {
    let old_file = file("old-name.ts", "same 1\nsame 2\nold value\nsame 4\n", None);
    let new_file = file("new-name.ts", "same 1\nsame 2\nnew value\nsame 4\n", None);
    let mut partial = partial_of(&old_file, &new_file, 0);
    partial.change_type = ChangeTypes::RenameChanged;
    partial.prev_name = Some(old_file.name.clone());
    partial.name = new_file.name.clone();
    let old_file_lines = owned(split_file_contents(&old_file.contents));
    let new_file_lines = owned(split_file_contents(&new_file.contents));

    expect_partial_line_state(
        &partial,
        PartialLineStateAssertions {
            addition_line_count: 1,
            deletion_line_count: 1,
            full_addition_line_count: Some(new_file_lines.len()),
            full_deletion_line_count: Some(old_file_lines.len()),
        },
    );
    expect_first_partial_hunk_indexes(&partial);
    let pure_renamed = FileDiffLoadedFiles::PureRenamed {
        new_file: new_file.clone(),
    };
    let message = panic_message(|| merge(&mut partial.clone(), &pure_renamed));
    assert!(message.contains("requires oldFile"), "{message}");

    merge(&mut partial, &changed(&old_file, &new_file));

    assert_eq!(partial.change_type, ChangeTypes::RenameChanged);
    assert_eq!(partial.prev_name.as_deref(), Some(old_file.name.as_str()));
    assert_eq!(partial.name, new_file.name);
    assert!(!partial.is_partial);
    assert_eq!(partial.addition_lines, new_file_lines);
    assert_eq!(partial.deletion_lines, old_file_lines);
    assert_eq!(
        verify_hunk_line_values(&partial, "file"),
        Vec::<String>::new()
    );
}

#[test]
fn rejects_added_partial_diffs() {
    let added_partial = parse_single_file(
        "diff --git a/new-file.txt b/new-file.txt\nnew file mode 100644\nindex 0000000..1111111\n--- /dev/null\n+++ b/new-file.txt\n@@ -0,0 +1,2 @@\n+alpha\n+beta\n",
        None,
    );
    let new_file = file(
        "new-file.txt",
        "alpha\nbeta\ngamma\ndelta\n",
        Some("new-full"),
    );
    let files = FileDiffLoadedFiles::PureRenamed { new_file };
    let message = panic_message(|| merge(&mut added_partial.clone(), &files));
    assert!(
        message.contains("new diffs cannot be hydrated from loaded files"),
        "{message}"
    );
    assert!(added_partial.is_partial);
}

#[test]
fn hydrates_pure_renames_as_data_only_line_arrays() {
    let mut partial = parse_single_file(
        "diff --git a/old-name.txt b/new-name.txt\nsimilarity index 100%\nrename from old-name.txt\nrename to new-name.txt\n",
        None,
    );
    partial.cache_key = Some(String::from("partial-rename-cache"));
    let new_file = file("new-name.txt", "alpha\nbeta\n", Some("new-full"));
    let new_file_lines = owned(split_file_contents(&new_file.contents));

    expect_partial_line_state(
        &partial,
        PartialLineStateAssertions {
            addition_line_count: 0,
            deletion_line_count: 0,
            full_addition_line_count: Some(new_file_lines.len()),
            full_deletion_line_count: Some(new_file_lines.len()),
        },
    );
    assert!(partial.hunks.is_empty());
    let wrongly_two_sided = changed(&file("old-name.txt", &new_file.contents, None), &new_file);
    let message = panic_message(|| merge(&mut partial.clone(), &wrongly_two_sided));
    assert!(message.contains("requires oldFile to be null"), "{message}");

    merge(
        &mut partial,
        &FileDiffLoadedFiles::PureRenamed {
            new_file: new_file.clone(),
        },
    );

    assert_eq!(partial.change_type, ChangeTypes::RenamePure);
    assert_eq!(partial.name, "new-name.txt");
    assert_eq!(partial.prev_name.as_deref(), Some("old-name.txt"));
    assert!(!partial.is_partial);
    assert!(partial.hunks.is_empty());
    assert_eq!(partial.split_line_count, 0);
    assert_eq!(partial.unified_line_count, 0);
    assert_eq!(partial.addition_lines, new_file_lines);
    assert_eq!(partial.deletion_lines, new_file_lines);
    assert_eq!(
        partial.cache_key.as_deref(),
        Some("partial-rename-cache:hydrated")
    );
}
