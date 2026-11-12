//! Differential tests for the S11a option helpers
//! (`oracle/golden/s11a.json`, `options`). Test-only, original to this crate.

use serde::Deserialize;
use serde_json::Value;

use crate::oracle_testing::load_golden;
use crate::types::patch::DiffFileInput;
use crate::types::patch::FileContents;
use crate::utils::options::get_diff_file_input::get_diff_file_input;
use crate::utils::options::should_use_token_transformer::ShouldUseTokenTransformerProps;
use crate::utils::options::should_use_token_transformer::should_use_token_transformer;

#[derive(Deserialize)]
struct TokenCase {
    #[serde(default)]
    absent: bool,
    #[serde(rename = "useTokenTransformer", default)]
    use_token_transformer: Option<bool>,
    #[serde(default)]
    click: bool,
    #[serde(default)]
    enter: bool,
    #[serde(default)]
    leave: bool,
    expected: bool,
}

#[derive(Deserialize)]
struct InputCase {
    old: String,
    new: String,
    expected: Value,
}

#[derive(Deserialize)]
struct Options {
    #[serde(rename = "tokenTransformer")]
    token_transformer: Vec<TokenCase>,
    #[serde(rename = "diffFileInput")]
    diff_file_input: Vec<InputCase>,
}

#[derive(Deserialize)]
struct Golden {
    options: Options,
}

#[test]
fn should_use_token_transformer_matches_the_published_function() {
    let golden: Golden = load_golden("s11a.json");
    for case in &golden.options.token_transformer {
        let props = ShouldUseTokenTransformerProps {
            has_on_token_click: case.click,
            has_on_token_enter: case.enter,
            has_on_token_leave: case.leave,
            use_token_transformer: case.use_token_transformer,
        };
        let actual = should_use_token_transformer((!case.absent).then_some(&props));
        assert_eq!(actual, case.expected);
    }
}

fn side(kind: &str) -> Option<Option<FileContents>> {
    match kind {
        "undefined" => None,
        "null" => Some(None),
        _ => Some(Some(FileContents {
            name: "n.ts".into(),
            contents: "c".into(),
            lang: None,
            header: None,
            cache_key: None,
        })),
    }
}

#[test]
fn get_diff_file_input_matches_the_published_function() {
    let golden: Golden = load_golden("s11a.json");
    for case in &golden.options.diff_file_input {
        let (old, new) = (side(&case.old), side(&case.new));
        if let Some(message) = case.expected.get("throws") {
            let panic = std::panic::catch_unwind(|| get_diff_file_input(old, new, "ctx"))
                .expect_err("upstream throws");
            let text = panic
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| panic.downcast_ref::<&str>().map(|text| (*text).to_owned()))
                .expect("a string panic");
            assert_eq!(&Value::String(text), message);
            continue;
        }
        let result = get_diff_file_input(old, new, "ctx");
        let label = match &result {
            None => "none",
            Some(DiffFileInput::Both { .. }) => "both",
            Some(DiffFileInput::Added { .. }) => "added",
            Some(DiffFileInput::Deleted { .. }) => "deleted",
        };
        let expected = &case.expected["result"];
        let expected_label = match (expected["oldFile"].is_null(), expected["newFile"].is_null()) {
            _ if expected.is_null() => "none",
            (true, false) => "added",
            (false, true) => "deleted",
            _ => "both",
        };
        assert_eq!(label, expected_label, "{} {}", case.old, case.new);
    }
}
