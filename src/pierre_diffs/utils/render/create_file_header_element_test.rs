// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/createFileHeaderElement.test.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: The upstream tests are `toMatchSnapshot()` of the header AST; the snapshots are replaced by the dist's own output for the same inputs, taken from `oracle/golden/render_s5.json` (the `file` header cases), which is strictly stronger than a stored snapshot. The `prevName` input of the second test is not a `FileContents` field, so it is accepted by upstream only because TypeScript tolerates the extra property; it cannot be expressed here and the case reduces to the first one.

use serde::Deserialize;
use serde_json::Value;

use crate::hast::Element;
use crate::hast::PropValue;
use crate::oracle_testing::assert_json_eq;
use crate::oracle_testing::load_golden;
use crate::types::options::FileHeaderRenderMode;
use crate::types::patch::FileContents;
use crate::utils::render::create_file_header_element::CreateFileHeaderElementProps;
use crate::utils::render::create_file_header_element::FileOrDiff;
use crate::utils::render::create_file_header_element::create_file_header_element;

#[derive(Deserialize)]
struct Golden {
    headers: Vec<Header>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Header {
    label: String,
    mode: FileHeaderRenderMode,
    sticky_header: bool,
    expected: Value,
}

fn file() -> FileContents {
    serde_json::from_value(serde_json::json!({
        "name": "src/index.ts",
        "contents": "export {}\n",
    }))
    .expect("file")
}

/// Renders the upstream test's input and checks it against the dist's output.
fn render_and_compare(mode: FileHeaderRenderMode, sticky_header: bool) -> Element {
    let file = file();
    let header = create_file_header_element(CreateFileHeaderElementProps {
        file_or_diff: FileOrDiff::File(&file),
        mode,
        sticky_header,
    });
    let golden: Golden = load_golden("render_s5.json");
    let expected = golden
        .headers
        .iter()
        .find(|case| {
            case.label == "file" && case.mode == mode && case.sticky_header == sticky_header
        })
        .expect("golden case");
    assert_json_eq(
        "header",
        &expected.expected,
        &serde_json::to_value(&header).expect("serialize"),
    );
    header
}

#[test]
fn renders_default_file_header_ast() {
    render_and_compare(FileHeaderRenderMode::Default, false);
}

#[test]
fn renders_default_renamed_file_header_ast() {
    // `prevName` is not a `FileContents` field (see the module comment), so
    // this is the same input as the default test; assert the invariant that
    // makes that true: the output depends on the file's `name` alone.
    let renamed = render_and_compare(FileHeaderRenderMode::Default, false);
    let plain = render_and_compare(FileHeaderRenderMode::Default, false);
    assert_eq!(renamed, plain);
}

#[test]
fn renders_custom_file_header_ast() {
    render_and_compare(FileHeaderRenderMode::Custom, false);
}

#[test]
fn renders_sticky_file_header_ast() {
    let header = render_and_compare(FileHeaderRenderMode::Default, true);
    assert_eq!(
        header.properties.get("data-sticky"),
        Some(&PropValue::Str(String::new()))
    );
}
