// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/parsePatchFiles.test.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: the Git/unified header-decoding tests of the same upstream file, split into a
// child module only to stay inside the crate's module item budget; see the parent module.

use super::file_of;
use super::lines;
use super::parse_strict;
use crate::types::patch::ChangeTypes;
use crate::utils::parse_patch_files::ProcessFileOptions;
use crate::utils::parse_patch_files::process_file;

#[test]
fn reads_git_header_paths() {
    let cases = [
        ("a/sp ace.ts b/sp ace.ts", "sp ace.ts", "sp ace.ts"),
        ("a/日本語.ts b/日本語.ts", "日本語.ts", "日本語.ts"),
        (
            r"a/literal\303.ts b/literal\303.ts",
            r"literal\303.ts",
            r"literal\303.ts",
        ),
        (
            r#""a/\303\274ber.ts" "b/\303\274ber.ts""#,
            "über.ts",
            "über.ts",
        ),
        (r#""a/\303\274ber.ts" b/sp ace.ts"#, "über.ts", "sp ace.ts"),
        (r#"a/sp ace.ts "b/\303\274ber.ts""#, "sp ace.ts", "über.ts"),
        (
            r#""a/a/\303\274ber.ts" "b/b/\303\274ber.ts""#,
            "a/über.ts",
            "b/über.ts",
        ),
        (
            r#""a/old\" b/inside.ts" "b/new\"name.ts""#,
            "old\" b/inside.ts",
            "new\"name.ts",
        ),
        (
            r#""a/ leading.ts " "b/ trailing.ts ""#,
            " leading.ts ",
            " trailing.ts ",
        ),
    ];
    for (paths, prev_name, name) in cases {
        let file = file_of(&format!("diff --git {paths}\nsimilarity index 100%\n"));
        assert_eq!(file.name, name, "{paths}");
        assert_eq!(
            file.prev_name.as_deref(),
            if prev_name == name {
                None
            } else {
                Some(prev_name)
            },
            "{paths}"
        );
        assert!(file.hunks.is_empty(), "{paths}");
    }
}

#[test]
fn decodes_quoted_git_file_headers_before_removing_one_prefix() {
    let file = file_of(
        "diff --git a/fallback.ts b/fallback.ts\nsimilarity index 80%\n--- \"a/a/\\303\\274ber.ts\"\n+++ \"b/b/\\303\\274ber.ts\"\n@@ -1 +1 @@\n-old\n+new\n",
    );
    assert_eq!(file.prev_name.as_deref(), Some("a/über.ts"));
    assert_eq!(file.name, "b/über.ts");
    assert_eq!(file.deletion_lines, lines(&["old\n"]));
    assert_eq!(file.addition_lines, lines(&["new\n"]));
}

#[test]
fn decodes_multiple_unified_files_with_timestamps_and_literal_directories() {
    let patch = "--- \"a/\\303\\274ber.ts\"\told timestamp\n+++ \"b/\\303\\274ber.ts\"\tnew timestamp\n@@ -1 +1 @@\n-old\n+new\n--- sp ace.ts\told timestamp\n+++ sp ace.ts\tnew timestamp\n@@ -1 +1 @@\n-before\n+after\n";
    let result = parse_strict(patch);
    let summary: Vec<(String, Option<String>, ChangeTypes)> = result[0]
        .files
        .iter()
        .map(|file| (file.name.clone(), file.prev_name.clone(), file.change_type))
        .collect();
    assert_eq!(
        summary,
        [
            (
                String::from("b/über.ts"),
                Some(String::from("a/über.ts")),
                ChangeTypes::RenameChanged
            ),
            (String::from("sp ace.ts"), None, ChangeTypes::Change),
        ]
    );
}

#[test]
fn decodes_rename_and_copy_metadata_without_stripping_real_directories() {
    for kind in ["rename", "copy"] {
        let file = file_of(&format!(
            "diff --git a/fallback-old.ts b/fallback-new.ts\nsimilarity index 100%\n{kind} from \"a/\\303\\274ber.ts\"\n{kind} to \"b/new\\\"name.ts\"\n"
        ));
        assert_eq!(file.prev_name.as_deref(), Some("a/über.ts"));
        assert_eq!(file.name, "b/new\"name.ts");
        assert_eq!(file.change_type, ChangeTypes::RenamePure);
        assert!(file.hunks.is_empty());
    }
}

#[test]
fn keeps_dev_null_absent_for_a_quoted_new_and_deleted_file() {
    for added in [true, false] {
        let file = file_of(&format!(
            "diff --git \"a/\\303\\274ber.ts\" \"b/\\303\\274ber.ts\"\n{} file mode 100644\n{}{}{}",
            if added { "new" } else { "deleted" },
            if added {
                "--- /dev/null\n"
            } else {
                "--- \"a/\\303\\274ber.ts\"\n"
            },
            if added {
                "+++ \"b/\\303\\274ber.ts\"\n"
            } else {
                "+++ /dev/null\n"
            },
            if added {
                "@@ -0,0 +1 @@\n+new\n"
            } else {
                "@@ -1 +0,0 @@\n-old\n"
            },
        ));
        assert_eq!(file.name, "über.ts");
        assert_eq!(file.prev_name, None);
        assert_eq!(
            file.change_type,
            if added {
                ChangeTypes::New
            } else {
                ChangeTypes::Deleted
            }
        );
    }
}

#[test]
fn decodes_a_hunkless_git_file_with_metadata() {
    for (metadata, change_type) in [
        ("old mode 100644\nnew mode 100755\n", ChangeTypes::Change),
        (
            "new file mode 100644\nBinary files /dev/null and b/filename differ\n",
            ChangeTypes::New,
        ),
    ] {
        let result = parse_strict(&format!(
            "diff --git \"a/\\303\\274ber.ts\" \"b/\\303\\274ber.ts\"\n{metadata}"
        ));
        let file = &result[0].files[0];
        assert_eq!(file.name, "über.ts");
        assert_eq!(file.change_type, change_type);
        assert!(file.hunks.is_empty());
    }
}

#[test]
fn compares_decoded_unified_names_and_keeps_malformed_quoted_tokens_intact() {
    let file = file_of("--- \"\\303\\274ber.ts\"\n+++ über.ts\n@@ -1 +1 @@\n-old\n+new\n");
    assert_eq!(file.name, "über.ts");
    assert_eq!(file.prev_name, None);
    assert_eq!(file.change_type, ChangeTypes::Change);

    let malformed = file_of("--- \"bad\\q.ts\"\n+++ \"bad\\q.ts\"\n@@ -1 +1 @@\n-old\n+new\n");
    assert_eq!(malformed.name, "\"bad\\q.ts\"");
}

#[test]
fn parses_quoted_git_diff_headers_with_escaped_file_names() {
    let old_name = "test/integration/image-optimizer/app/public/\\303\\244\\303\\266\\303\\274\\305\\241\\304\\215\\305\\231\\303\\255.png";
    let new_name = "test/e2e/image-optimizer/app/public/\\303\\244\\303\\266\\303\\274\\305\\241\\304\\215\\305\\231\\303\\255.png";
    let file = process_file(
        &format!("diff --git \"a/{old_name}\" \"b/{new_name}\"\nsimilarity index 100%\n"),
        ProcessFileOptions {
            is_git_diff: Some(true),
            ..ProcessFileOptions::default()
        },
    )
    .expect("forgiving")
    .value
    .expect("a file");

    assert_eq!(file.name, "test/e2e/image-optimizer/app/public/äöüščří.png");
    assert_eq!(
        file.prev_name.as_deref(),
        Some("test/integration/image-optimizer/app/public/äöüščří.png")
    );
    assert_eq!(file.change_type, ChangeTypes::RenamePure);
}
