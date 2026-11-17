// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/parsePatchFiles.test.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: the hunk-count repair tests of the same upstream file, split into a
// child module only to stay inside the crate's module item budget; see the parent module.

use super::first_file;
use super::parse;
use crate::test_utils::verify_patch_hunk_values;
use crate::types::patch::FileContents;
use crate::types::patch::HunkContent;
use crate::types::patch::ParsePatchOptions;
use crate::utils::parse_patch_files::ProcessFileOptions;
use crate::utils::parse_patch_files::process_file;

pub(super) struct BoundaryCase {
    name: &'static str,
    header: &'static str,
    body: &'static str,
    expected_addition: (usize, usize),
    expected_deletion: (usize, usize),
    expected_error: bool,
}

#[test]
pub(super) fn preserves_boundaries_for_each_repair_case() {
    let cases = [
        BoundaryCase {
            name: "addition count changing from positive to zero",
            header: "@@ -5,1 +5,1 @@\n",
            body: "-gone\n",
            expected_addition: (4, 0),
            expected_deletion: (5, 1),
            expected_error: true,
        },
        BoundaryCase {
            name: "deletion count changing from positive to zero",
            header: "@@ -5,1 +5,1 @@\n",
            body: "+added\n",
            expected_addition: (5, 1),
            expected_deletion: (4, 0),
            expected_error: true,
        },
        BoundaryCase {
            name: "addition count changing from zero to positive",
            header: "@@ -5,1 +4,0 @@\n",
            body: "-old\n+new\n",
            expected_addition: (5, 1),
            expected_deletion: (5, 1),
            expected_error: true,
        },
        BoundaryCase {
            name: "deletion count changing from zero to positive",
            header: "@@ -4,0 +5,1 @@\n",
            body: "-old\n+new\n",
            expected_addition: (5, 1),
            expected_deletion: (5, 1),
            expected_error: true,
        },
        BoundaryCase {
            name: "both sides crossing zero in opposite directions",
            header: "@@ -5,1 +4,0 @@\n",
            body: "+added\n",
            expected_addition: (5, 1),
            expected_deletion: (4, 0),
            expected_error: true,
        },
        BoundaryCase {
            name: "both zero counts growing from hunk content",
            header: "@@ -4,0 +4,0 @@\n",
            body: "-old\n+new\n",
            expected_addition: (5, 1),
            expected_deletion: (5, 1),
            expected_error: true,
        },
        BoundaryCase {
            name: "a positive count growing from extra hunk content",
            header: "@@ -5,1 +5,1 @@\n",
            body: "-old\n+new\n-extra old\n",
            expected_addition: (5, 1),
            expected_deletion: (5, 2),
            expected_error: true,
        },
        BoundaryCase {
            name: "a valid zero-count side remaining unchanged",
            header: "@@ -4,0 +5,1 @@\n",
            body: "+added\n",
            expected_addition: (5, 1),
            expected_deletion: (4, 0),
            expected_error: false,
        },
    ];
    for case in cases {
        let patch = format!(
            "--- boundary.txt\n+++ boundary.txt\n{}{}",
            case.header, case.body
        );
        let result = parse(&patch, &ParsePatchOptions::default());
        let hunk = &first_file(&result).hunks[0];
        assert_eq!(
            result.warnings.len(),
            usize::from(case.expected_error),
            "{}",
            case.name
        );
        assert_eq!(
            (hunk.addition_start, hunk.addition_count),
            case.expected_addition,
            "{}",
            case.name
        );
        assert_eq!(
            (hunk.deletion_start, hunk.deletion_count),
            case.expected_deletion,
            "{}",
            case.name
        );
        assert_eq!(hunk.collapsed_before, 4, "{}", case.name);
        assert_eq!(
            verify_patch_hunk_values(&result.value).errors,
            Vec::<String>::new(),
            "{}",
            case.name
        );
    }
}

pub(super) fn contents(name: &str, text: &str) -> FileContents {
    FileContents {
        name: name.to_owned(),
        contents: text.to_owned(),
        lang: None,
        header: None,
        cache_key: None,
    }
}

#[test]
pub(super) fn shifts_hydrated_hunk_and_content_indexes_with_a_repaired_start() {
    let old_file = contents("hydrated.txt", "a\nb\nc\nd\nold\nz\n");
    let new_file = contents("hydrated.txt", "a\nb\nc\nd\nnew\nz\n");
    let result = process_file(
        "--- hydrated.txt\n+++ hydrated.txt\n@@ -5,1 +4,0 @@\n+new\n-old\n",
        ProcessFileOptions {
            old_file: Some(&old_file),
            new_file: Some(&new_file),
            ..ProcessFileOptions::default()
        },
    )
    .expect("forgiving");
    let file = result.value.expect("a file");
    let hunk = &file.hunks[0];

    assert_eq!(result.warnings.len(), 1);
    assert_eq!(hunk.addition_line_index, 4);
    assert_eq!(hunk.deletion_line_index, 4);
    match &hunk.hunk_content[0] {
        HunkContent::Change(change) => {
            assert_eq!(change.addition_line_index, 4);
            assert_eq!(change.deletion_line_index, 4);
        }
        HunkContent::Context(_) => panic!("expected a change block"),
    }
}

#[test]
pub(super) fn chains_later_hunk_geometry_from_a_zero_to_positive_repair() {
    let result = parse(
        "--- multiple-zero.txt\n+++ multiple-zero.txt\n@@ -5,1 +4,0 @@\n+new\n-old\n@@ -8 +8 @@\n-later old\n+later new\n",
        &ParsePatchOptions::default(),
    );
    let file = first_file(&result);
    let [first_hunk, second_hunk] = &file.hunks[..] else {
        panic!("two hunks");
    };
    assert_eq!(result.warnings.len(), 1);
    assert_eq!(first_hunk.addition_start, 5);
    assert_eq!(first_hunk.addition_count, 1);
    assert_eq!(second_hunk.collapsed_before, 2);
    assert_eq!(
        verify_patch_hunk_values(&result.value).errors,
        Vec::<String>::new()
    );
}

#[test]
pub(super) fn uses_repaired_counts_for_geometry_after_an_underfilled_hunk() {
    let result = parse(
        "--- multiple.txt\n+++ multiple.txt\n@@ -1,4 +1,4 @@\n-old\n+new\n context\n-tail\n+TAIL\n@@ -6 +6 @@\n-later old\n+later new\n",
        &ParsePatchOptions::default(),
    );
    let file = first_file(&result);
    let [first_hunk, second_hunk] = &file.hunks[..] else {
        panic!("two hunks");
    };
    assert_eq!(result.warnings.len(), 1);
    assert!(result.warnings[0].contains("@@ -1,4 +1,4 @@"));
    assert!(result.warnings[0].contains("declared old/new 4/4"));
    assert!(result.warnings[0].contains("parsed old/new 3/3"));
    assert_eq!(first_hunk.addition_count, 3);
    assert_eq!(first_hunk.deletion_count, 3);
    assert_eq!(second_hunk.addition_count, 1);
    assert_eq!(second_hunk.deletion_count, 1);
    assert_eq!(second_hunk.collapsed_before, 2);
    assert_eq!(second_hunk.split_line_start, 5);
    assert_eq!(second_hunk.unified_line_start, 7);
    assert_eq!(file.split_line_count, 6);
    assert_eq!(file.unified_line_count, 9);
    assert_eq!(
        verify_patch_hunk_values(&result.value).errors,
        Vec::<String>::new()
    );
}
