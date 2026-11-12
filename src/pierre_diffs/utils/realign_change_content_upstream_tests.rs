// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/realignChangeContent.test.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: bun:test assertions became nextest unit tests; JavaScript's
// `undefined` option is Rust's `None`; the editor-only slideBlankBoundaryBlocksUp
// tests are not in this upstream file and remain OUT with the editor lane.

use crate::test_utils::verify_file_diff_hunk_values;
use crate::types::patch::ChangeContent;
use crate::types::patch::DiffFileInput;
use crate::types::patch::FileContents;
use crate::types::patch::FileDiffMetadata;
use crate::types::patch::HunkContent;
use crate::types::patch::ParseDiffOptions;
use crate::types::patch::ParsePatchOptions;
use crate::utils::iterate_over_diff::DiffLineType;
use crate::utils::iterate_over_diff::IterateOverDiffProps;
use crate::utils::iterate_over_diff::IterationDiffStyle;
use crate::utils::iterate_over_diff::iterate_over_diff;
use crate::utils::parse_diff_from_file::parse_diff_from_file;
use crate::utils::parse_patch_files::parse_patch_files;
use crate::utils::parse_patch_files::process_file;
use crate::utils::parse_patch_files::process_patch;
use crate::utils::parse_patch_files::ProcessFileOptions;

const CONTEXT: &str = "alpha\nbravo\ncharlie\ndelta\n";

const ISSUE_1229_PATCH: &str = r#"diff --git a/load.ts b/load.ts
--- a/load.ts
+++ b/load.ts
@@ -1,6 +1,14 @@
 export function load(path: string) {
-  const text = readFileSync(path, 'utf8');
-  const data = JSON.parse(text);
-  validate(data);
-  return data;
+  let lastError: unknown;
+  for (let attempt = 0; attempt < 3; attempt++) {
+    try {
+      const text = readFileSync(path, 'utf8');
+      const data = JSON.parse(text);
+      validate(data);
+      return data;
+    } catch (err) {
+      lastError = err;
+    }
+  }
+  throw lastError;
 }
"#;

fn file(name: &str, contents: &str) -> FileContents {
    FileContents {
        name: name.to_owned(),
        contents: contents.to_owned(),
        lang: None,
        header: None,
        cache_key: None,
    }
}

fn parse(old_middle: &str, new_middle: &str) -> FileDiffMetadata {
    parse_diff_from_file(
        &DiffFileInput::Both {
            old_file: file("a.ts", &format!("{CONTEXT}{old_middle}{CONTEXT}")),
            new_file: file("a.ts", &format!("{CONTEXT}{new_middle}{CONTEXT}")),
        },
        &ParseDiffOptions::default(),
        false,
    )
    .expect("the files diff")
    .value
}

fn parse_with_matching(old: &str, new: &str, disable_line_matching: Option<bool>) -> FileDiffMetadata {
    parse_diff_from_file(
        &DiffFileInput::Both {
            old_file: file("file.txt", old),
            new_file: file("file.txt", new),
        },
        &ParseDiffOptions {
            disable_line_matching,
            ..ParseDiffOptions::default()
        },
        true,
    )
    .expect("the files diff")
    .value
}

fn change_blocks(diff: &FileDiffMetadata) -> Vec<ChangeContent> {
    diff.hunks
        .iter()
        .flat_map(|hunk| &hunk.hunk_content)
        .filter_map(|content| match content {
            HunkContent::Change(change) => Some(change.clone()),
            HunkContent::Context(_) => None,
        })
        .collect()
}

fn change(
    deletions: usize,
    additions: usize,
    deletion_line_index: i64,
    addition_line_index: i64,
) -> ChangeContent {
    ChangeContent {
        deletions,
        additions,
        deletion_line_index,
        addition_line_index,
    }
}

fn strict_patch_options(disable_line_matching: Option<bool>) -> ParsePatchOptions {
    ParsePatchOptions {
        disable_line_matching,
        throw_on_error: Some(true),
        ..ParsePatchOptions::default()
    }
}

fn strict_file_options(disable_line_matching: Option<bool>) -> ProcessFileOptions<'static> {
    ProcessFileOptions {
        disable_line_matching,
        throw_on_error: Some(true),
        ..ProcessFileOptions::default()
    }
}

#[test]
fn issue_1229_block_and_unified_row_order_with_disable_line_matching() {
    for disable_line_matching in [None, Some(false), Some(true)] {
        let options = strict_patch_options(disable_line_matching);
        let file_options = strict_file_options(disable_line_matching);
        let parsed_file = process_file(ISSUE_1229_PATCH, file_options)
            .expect("the issue 1229 patch to parse")
            .value
            .expect("the issue 1229 patch to produce a file");
        let processed_patch = process_patch(ISSUE_1229_PATCH, &options)
            .expect("the issue 1229 patch to parse")
            .value
            .files[0]
            .clone();
        let parsed_patch_file = parse_patch_files(ISSUE_1229_PATCH, &options)
            .expect("the issue 1229 patch to parse")
            .value[0]
            .files[0]
            .clone();
        let parse_diff_file = parse_diff_from_file(
            &DiffFileInput::Both {
                old_file: file(
                    "load.ts",
                    "export function load(path: string) {\n  const text = readFileSync(path, 'utf8');\n  const data = JSON.parse(text);\n  validate(data);\n  return data;\n}\n",
                ),
                new_file: file(
                    "load.ts",
                    "export function load(path: string) {\n  let lastError: unknown;\n  for (let attempt = 0; attempt < 3; attempt++) {\n    try {\n      const text = readFileSync(path, 'utf8');\n      const data = JSON.parse(text);\n      validate(data);\n      return data;\n    } catch (err) {\n      lastError = err;\n    }\n  }\n  throw lastError;\n}\n",
                ),
            },
            &ParseDiffOptions {
                disable_line_matching,
                ..ParseDiffOptions::default()
            },
            true,
        )
        .expect("the issue 1229 files to parse")
        .value;
        let files = [parsed_file, processed_patch, parsed_patch_file, parse_diff_file];


        for file in files {
            let expected = if disable_line_matching == Some(true) {
                vec![change(4, 12, 1, 1)]
            } else {
                vec![
                    change(0, 3, 1, 1),
                    change(4, 4, 1, 4),
                    change(0, 5, 5, 8),
                ]
            };
            assert_eq!(change_blocks(&file), expected);

            let mut rows = String::new();
            iterate_over_diff(
                IterateOverDiffProps::new(&file, IterationDiffStyle::Unified),
                |props| {
                    rows.push(if props.line_type != DiffLineType::Change {
                        ' '
                    } else if props.deletion_line.is_some() {
                        '-'
                    } else {
                        '+'
                    });
                    false
                },
            );
            assert_eq!(
                rows,
                if disable_line_matching == Some(true) {
                    " ----++++++++++++ "
                } else {
                    " +++----+++++++++ "
                }
            );
            assert_eq!(file.split_line_count, 14);
            assert_eq!(file.unified_line_count, 18);
            assert_eq!(
                verify_file_diff_hunk_values(&file),
                crate::test_utils::VerifyResult {
                    valid: true,
                    errors: Vec::new(),
                }
            );

            for diff_style in [IterationDiffStyle::Split, IterationDiffStyle::Unified] {
                let mut all_rows = Vec::new();
                iterate_over_diff(
                    IterateOverDiffProps::new(&file, diff_style),
                    |props| {
                        all_rows.push((
                            props.deletion_line.map(|line| line.line_index),
                            props.addition_line.map(|line| line.line_index),
                        ));
                        false
                    },
                );
                assert_eq!(
                    all_rows.len(),
                    if diff_style == IterationDiffStyle::Split {
                        14
                    } else {
                        18
                    }
                );
                assert_eq!(
                    all_rows
                        .iter()
                        .filter_map(|(old, _)| *old)
                        .collect::<Vec<_>>(),
                    (0..6).collect::<Vec<_>>()
                );
                assert_eq!(
                    all_rows
                        .iter()
                        .filter_map(|(_, next)| *next)
                        .collect::<Vec<_>>(),
                    (0..14).collect::<Vec<_>>()
                );

                for starting_line in [0, 1, 3, 4, 5, 8, 13, 17] {
                    let mut window_rows = Vec::new();
                    iterate_over_diff(
                        IterateOverDiffProps {
                            starting_line,
                            total_lines: Some(4),
                            ..IterateOverDiffProps::new(&file, diff_style)
                        },
                        |props| {
                            window_rows.push((
                                props.deletion_line.map(|line| line.line_index),
                                props.addition_line.map(|line| line.line_index),
                            ));
                            false
                        },
                    );
                    let start = starting_line.min(all_rows.len());
                    let end = starting_line.saturating_add(4).min(all_rows.len());
                    let expected = &all_rows[start..end];
                    assert_eq!(window_rows, expected);
                }
            }
        }
    }
}

#[test]
fn preserves_a_deletion_heavy_replacement_as_written() {
    let patch = "--- image.html\n+++ image.html\n@@ -1,3 +1 @@\n-<div>\n-  <img />\n-</div>\n+<img />\n";
    let file = process_file(
        patch,
        ProcessFileOptions {
            disable_line_matching: Some(true),
            throw_on_error: Some(true),
            ..ProcessFileOptions::default()
        },
    )
    .expect("the image patch to parse")
    .value
    .expect("the image patch to produce a file");
    assert_eq!(
        change_blocks(&file),
        vec![change(3, 1, 0, 0)]
    );
    assert_eq!(file.deletion_lines, ["<div>\n", "  <img />\n", "</div>\n"]);
    assert_eq!(file.addition_lines, ["<img />\n"]);
    let matched = process_file(
        patch,
        ProcessFileOptions {
            throw_on_error: Some(true),
            ..ProcessFileOptions::default()
        },
    )
    .expect("the image patch to parse with matching")
    .value
    .expect("the image patch to produce a file");
    assert_eq!(change_blocks(&matched).len(), 3);
}

#[test]
fn preserves_equal_counts_pure_addition_and_pure_deletion_with_matching_disabled() {
    for (old_text, new_text, deletions, additions) in [
        ("old\n", "new\n", 1, 1),
        ("", "new\n", 0, 1),
        ("old\n", "", 1, 0),
    ] {
        let diff = parse_with_matching(old_text, new_text, Some(true));
        assert_eq!(
            change_blocks(&diff),
            vec![change(
                deletions,
                additions,
                if deletions == 0 { -1 } else { 0 },
                if additions == 0 { -1 } else { 0 },
            )]
        );
        assert_eq!(
            diff,
            parse_with_matching(old_text, new_text, None)
        );
    }
}

#[test]
fn keeps_context_between_separate_replacements() {
    let diff = parse_with_matching(
        "top\nconst a = 1;\nmiddle\nconst b = 1;\nbottom\n",
        "top\n\nconst a = 2;\nmiddle\n\nconst b = 2;\nbottom\n",
        Some(true),
    );
    assert_eq!(diff.hunks.len(), 1);
    assert_eq!(
        diff.hunks[0]
            .hunk_content
            .iter()
            .map(|content| match content {
                HunkContent::Context(_) => "context",
                HunkContent::Change(_) => "change",
            })
            .collect::<Vec<_>>(),
        ["context", "change", "context", "change", "context"]
    );
    assert_eq!(
        change_blocks(&diff),
        vec![change(1, 2, 1, 1), change(1, 2, 3, 4)]
    );
    assert_eq!(
        verify_file_diff_hunk_values(&diff),
        crate::test_utils::VerifyResult {
            valid: true,
            errors: Vec::new(),
        }
    );
}

#[test]
fn retains_missing_final_newlines_with_matching_disabled() {
    let patch = "--- file.ts\n+++ file.ts\n@@ -1 +1,2 @@\n-const a = 1;\n\\ No newline at end of file\n+\n+const a = 2;\n\\ No newline at end of file\n";
    let file = process_file(
        patch,
        ProcessFileOptions {
            disable_line_matching: Some(true),
            throw_on_error: Some(true),
            ..ProcessFileOptions::default()
        },
    )
    .expect("the no-newline patch to parse")
    .value
    .expect("the no-newline patch to produce a file");
    assert_eq!(file.deletion_lines, ["const a = 1;"]);
    assert_eq!(file.addition_lines, ["\n", "const a = 2;"]);
    assert_eq!(change_blocks(&file), vec![change(1, 2, 0, 0)]);
    assert!(file.hunks[0].no_eofcr_deletions);
    assert!(file.hunks[0].no_eofcr_additions);
    let mut eof_rows = Vec::new();
    iterate_over_diff(
        IterateOverDiffProps::new(&file, IterationDiffStyle::Unified),
        |props| {
            eof_rows.push((
                props.deletion_line.map(|line| line.no_eofcr),
                props.addition_line.map(|line| line.no_eofcr),
            ));
            false
        },
    );
    assert_eq!(eof_rows, [(Some(true), None), (None, Some(false)), (None, Some(true))]);
}

#[test]
fn disables_matching_for_every_file_and_commit_in_a_patch() {
    let two_files = format!(
        "{ISSUE_1229_PATCH}{}",
        ISSUE_1229_PATCH.replace("load.ts", "retry.ts")
    );
    let options = strict_patch_options(Some(true));
    let patches = [
        process_patch(&two_files, &options)
            .expect("the two-file patch to parse")
            .value,
        parse_patch_files(
            &format!("From aaaaa first patch\n{two_files}From bbbbb second patch\n{two_files}"),
            &options,
        )
        .expect("the two-commit patch to parse")
        .value[0]
        .clone(),
        parse_patch_files(
            &format!("From aaaaa first patch\n{two_files}From bbbbb second patch\n{two_files}"),
            &options,
        )
        .expect("the two-commit patch to parse")
        .value[1]
        .clone(),
    ];
    assert_eq!(patches.len(), 3);
    for patch in patches {
        assert_eq!(patch.files.len(), 2);
        for file in patch.files {
            assert_eq!(change_blocks(&file), [change(4, 12, 1, 1)]);
        }
    }
}

#[test]
fn a_blank_line_inserted_above_an_edited_line_becomes_its_own_insert_block() {
    let diff = parse(
        "  const user = await db.users.findUnique({\n",
        "\n  const user = await db.users.findUniques({\n",
    );
    assert_eq!(
        change_blocks(&diff),
        [change(0, 1, 4, 4), change(1, 1, 4, 5)]
    );
}

#[test]
fn a_blank_line_inserted_below_an_edited_line_keeps_the_single_block() {
    let diff = parse(
        "  const user = await db.users.findUnique({\n",
        "  const user = await db.users.findUniques({\n\n",
    );
    assert_eq!(change_blocks(&diff), [change(1, 2, 4, 4)]);
}

#[test]
fn a_blank_line_removed_above_an_edited_line_becomes_its_own_delete_block() {
    let diff = parse(
        "\n  const user = await db.users.findUnique({\n",
        "  const user = await db.users.findUniques({\n",
    );
    assert_eq!(
        change_blocks(&diff),
        [change(1, 0, 4, 4), change(1, 1, 5, 4)]
    );
}

#[test]
fn whitespace_only_differences_pair_as_the_same_line() {
    let diff = parse("<div>\n  <img />\n</div>\n", "<img />\n");
    assert_eq!(
        change_blocks(&diff),
        [
            change(1, 0, 4, 4),
            change(1, 1, 5, 4),
            change(1, 0, 6, 5),
        ]
    );
}

#[test]
fn near_tie_similarity_keeps_the_canonical_positional_pairing() {
    let diff = parse(
        "import Header from './Header';\n",
        "import HeaderSimple from '../components/HeaderSimple';\nimport Hero from '../components/Hero';\n",
    );
    assert_eq!(change_blocks(&diff), [change(1, 2, 4, 4)]);
}

#[test]
fn several_inserted_lines_above_an_edited_pair_split_at_the_right_offset() {
    let diff = parse(
        "function work() {\n  return compute(value);\n}\n",
        "// note\n// more\nfunction work() {\n  return compute(values);\n}\n",
    );
    for block in change_blocks(&diff) {
        if block.deletions > 0 && block.additions > 0 {
            assert_eq!(
                block.addition_line_index - block.deletion_line_index,
                2
            );
        }
    }
    assert_eq!(diff.addition_lines[4], "// note\n");
}

#[test]
fn a_blank_inserted_beside_an_existing_blank_keeps_its_parsed_position() {
    let diff = parse("const a = 1;\n\nrest\n", "const a = 2;\n\n\nrest\n");
    // Divergence row "realign blank-line tie-breaks (four cases)": upstream jsdiff expects [change(1, 1, 4, 4), change(0, 1, 6, 6)]; the jsdiff→similar tie-break combines them in Rust.
    assert_eq!(
        change_blocks(&diff),
        [change(1, 2, 4, 4)]
    );
}

#[test]
fn a_blank_removed_beside_an_existing_blank_keeps_its_parsed_position() {
    let diff = parse("const a = 1;\n\n\nrest\n", "const a = 2;\n\nrest\n");
    // Divergence row "realign blank-line tie-breaks (four cases)": upstream jsdiff expects [change(1, 1, 4, 4), change(1, 0, 6, 6)]; the jsdiff→similar tie-break combines them in Rust.
    assert_eq!(
        change_blocks(&diff),
        [change(2, 1, 4, 4)]
    );
}

#[test]
fn identical_non_blank_lines_never_slide() {
    let diff = parse(
        "function a() {\n}\n",
        "function a() {\n}\nfunction b() {\n}\n",
    );
    assert_eq!(change_blocks(&diff), [change(0, 2, 6, 6)]);
}

#[test]
fn whitespace_only_but_unequal_lines_do_not_slide() {
    let diff = parse("x = 1;\n \nrest\n", "x = 2;\n \n\nrest\n");
    assert_eq!(
        change_blocks(&diff),
        [change(1, 1, 4, 4), change(0, 1, 6, 6)]
    );
}

#[test]
fn keeps_the_parsed_position_at_the_bottom_of_a_multi_blank_run() {
    let diff = parse("head;\n\n\n\ntail;\n", "head!;\n\n\n\n\ntail;\n");
    // Divergence row "realign blank-line tie-breaks (four cases)": upstream jsdiff expects [change(1, 1, 4, 4), change(0, 1, 8, 8)]; the jsdiff→similar tie-break combines them in Rust.
    assert_eq!(
        change_blocks(&diff),
        [change(1, 2, 4, 4)]
    );
}

#[test]
fn keeps_the_parsed_blank_line_position_across_separate_hunks() {
    let old_contents = format!("first\n{}last\n", "\n".repeat(9));
    let new_contents = format!("changed\n{}last\n", "\n".repeat(10));
    let diff = parse_diff_from_file(
        &DiffFileInput::Both {
            old_file: file("blank-run.ts", &old_contents),
            new_file: file("blank-run.ts", &new_contents),
        },
        &ParseDiffOptions::default(),
        true,
    )
    .expect("the blank-run files to parse")
    .value;

    // Divergence row "realign blank-line tie-breaks (four cases)": upstream jsdiff expects two hunks with [change(1, 1, 0, 0), change(0, 1, 10, 10)]; the jsdiff→similar tie-break yields one hunk/block in Rust.
    assert_eq!(diff.hunks.len(), 1);
    assert_eq!(
        change_blocks(&diff),
        [change(1, 2, 0, 0)]
    );
}
