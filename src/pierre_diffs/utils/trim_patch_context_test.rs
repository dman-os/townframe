// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/trimPatchContext.test.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: bun:test to nextest. `trims trim.patch fixture and matches snapshot`
// compares against the published function's output for the same fixture
// (`trims` in `layout.json`, checked by `utils::oracle_tests::layout_oracle_tests`) rather than
// the Bun snapshot file; here it also asserts the snapshot's first hunk header
// and that every hunk stays within the context budget.

use crate::utils::trim_patch_context::trim_patch_context;

const TRIM_PATCH: &str = include_str!("../oracle/corpus/trim.patch");

fn build_context(count: usize, label: &str) -> Vec<String> {
    (0..count)
        .map(|index| format!(" {label}-{}", index + 1))
        .collect()
}

#[test]
fn trims_and_splits_hunks_with_large_context() {
    let hunk1_before = build_context(40, "h1-before");
    let hunk1_after = build_context(40, "h1-after");
    let hunk2_before = build_context(40, "h2-before");
    let hunk2_middle = build_context(36, "h2-middle");
    let hunk2_after = build_context(40, "h2-after");

    let mut patch: Vec<String> = vec![
        "diff --git a/file.txt b/file.txt".into(),
        "--- a/file.txt".into(),
        "+++ b/file.txt".into(),
        "@@ -1,82 +1,84 @@".into(),
    ];
    patch.extend(hunk1_before.clone());
    patch.extend(["-old-1", "-old-2", "+new-1", "+new-2", "+new-3", "+new-4"].map(String::from));
    patch.extend(hunk1_after.clone());
    patch.push("@@ -200,118 +200,117 @@".into());
    patch.extend(hunk2_before.clone());
    patch.push("+only-add".into());
    patch.extend(hunk2_middle.clone());
    patch.extend(["-old-3", "-old-4"].map(String::from));
    patch.extend(hunk2_after.clone());

    let trimmed = trim_patch_context(&patch.join("\n"), 10);

    let mut expected: Vec<String> = vec![
        "diff --git a/file.txt b/file.txt".into(),
        "--- a/file.txt".into(),
        "+++ b/file.txt".into(),
        "@@ -31,22 +31,24 @@".into(),
    ];
    expected.extend(hunk1_before[30..].iter().cloned());
    expected.extend(["-old-1", "-old-2", "+new-1", "+new-2", "+new-3", "+new-4"].map(String::from));
    expected.extend(hunk1_after[..10].iter().cloned());
    expected.push("@@ -230,20 +230,21 @@".into());
    expected.extend(hunk2_before[30..].iter().cloned());
    expected.push("+only-add".into());
    expected.extend(hunk2_middle[..10].iter().cloned());
    expected.push("@@ -266,22 +267,20 @@".into());
    expected.extend(hunk2_middle[26..].iter().cloned());
    expected.extend(["-old-3", "-old-4"].map(String::from));
    expected.extend(hunk2_after[..10].iter().cloned());

    assert_eq!(trimmed, expected.join("\n"));
}

#[test]
fn omits_count_when_it_is_1_to_match_git_formatting() {
    let patch = [
        "diff --git a/a.txt b/a.txt",
        "--- a/a.txt",
        "+++ b/a.txt",
        "@@ -1,0 +1,1 @@",
        "+hello",
    ]
    .join("\n");

    let expected = [
        "diff --git a/a.txt b/a.txt",
        "--- a/a.txt",
        "+++ b/a.txt",
        "@@ -1,0 +1 @@",
        "+hello",
    ]
    .join("\n");

    assert_eq!(trim_patch_context(&patch, 0), expected);
}

#[test]
fn drops_context_only_hunks() {
    let patch = [
        "diff --git a/empty.txt b/empty.txt",
        "--- a/empty.txt",
        "+++ b/empty.txt",
        "@@ -1,4 +1,4 @@",
        " one",
        " two",
        " three",
        " four",
    ]
    .join("\n");

    let expected = [
        "diff --git a/empty.txt b/empty.txt",
        "--- a/empty.txt",
        "+++ b/empty.txt",
    ]
    .join("\n");

    assert_eq!(trim_patch_context(&patch, 10), expected);
}

#[test]
fn trims_trim_patch_fixture_and_matches_snapshot() {
    let trimmed = trim_patch_context(TRIM_PATCH, 10);
    // Facts from the upstream snapshot ("trim.patch trimmed"): the first
    // hunk is re-based to line 28 with 21 lines on each side, and the second
    // to line 143 with 34/44 lines.
    assert!(trimmed.contains("@@ -28,21 +28,21 @@"), "{trimmed}");
    assert!(trimmed.contains("@@ -143,34 +143,44 @@"), "{trimmed}");
    assert!(trimmed.len() < TRIM_PATCH.len());
    assert!(trimmed.ends_with('\n') == TRIM_PATCH.ends_with('\n'));
}
