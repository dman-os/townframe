//! Differential tests for S5: the published dist's `renderTokenLines`,
//! `createFileHeaderElement` and `renderDiffWithHighlighter` ran against a
//! deterministic plain-text highlighter that mirrors `PlainTextHighlighter`
//! (`oracle/golden/render_s5.json`). Token lines and headers are compared as
//! exact hast JSON. Rendered diffs are large, so they are compared by a
//! canonical FNV-1a 64 hash of the hast JSON; where the word-diff
//! decorations come from `similar` instead of jsdiff the exact hash may
//! differ, and the flattened hash (every line reduced to its text) must
//! still match. Test-only, original to this crate.

use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::Value;

use crate::highlighter_types::DecorationItem;
use crate::highlighter_types::DecorationPosition;
use crate::highlighter_types::PlainTextHighlighter;
use crate::highlighter_types::ThemedToken;
use crate::oracle_testing::assert_json_eq;
use crate::oracle_testing::fnv64;
use crate::oracle_testing::load_golden;
use crate::types::lines::ExpandedHunks;
use crate::types::lines::ForceDiffPlainTextOptions;
use crate::types::lines::HunkExpansionRegion;
use crate::types::lines::LineInfo;
use crate::types::options::FileHeaderRenderMode;
use crate::types::options::LineDiffTypes;
use crate::types::options::RenderDiffOptions;
use crate::types::options::ThemeOption;
use crate::types::patch::FileContents;
use crate::types::patch::FileDiffMetadata;
use crate::utils::render::create_file_header_element::CreateFileHeaderElementProps;
use crate::utils::render::create_file_header_element::FileOrDiff;
use crate::utils::render::create_file_header_element::create_file_header_element;
use crate::utils::render::render_diff_with_highlighter::render_diff_with_highlighter;
use crate::utils::render::render_token_lines::MergeWhitespaces;
use crate::utils::render::render_token_lines::RenderTokenLinesOptions;
use crate::utils::render::render_token_lines::render_token_lines;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Golden {
    diffs: Vec<FileDiffMetadata>,
    token_lines: Vec<TokenLinesCase>,
    headers: Vec<HeaderCase>,
    render_diffs: Vec<RenderDiffCase>,
}

fn golden() -> Golden {
    load_golden("render_s5.json")
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TokenLinesCase {
    lines: Vec<Vec<Value>>,
    decorations: Vec<Value>,
    state: Option<Vec<LineInfo>>,
    merge_whitespaces: String,
    expected: Value,
}

fn string_pairs(value: Option<&Value>) -> Option<Vec<(String, String)>> {
    value.map(|pairs| {
        pairs
            .as_array()
            .expect("pairs")
            .iter()
            .map(|pair| {
                (
                    pair[0].as_str().expect("name").to_owned(),
                    pair[1].as_str().expect("value").to_owned(),
                )
            })
            .collect()
    })
}

fn token_from_json(value: &Value) -> ThemedToken {
    ThemedToken {
        content: value["content"].as_str().expect("content").to_owned(),
        offset: value["offset"].as_u64().expect("offset") as usize,
        color: value["color"].as_str().map(str::to_owned),
        bg_color: value["bgColor"].as_str().map(str::to_owned),
        font_style: value["fontStyle"].as_u64().map(|style| style as u8),
        html_style: string_pairs(value.get("htmlStyle")).map(Into::into),
        html_attrs: string_pairs(value.get("htmlAttrs")),
        token_type: value["type"].as_u64().map(|token_type| token_type as u8),
    }
}

fn decoration_from_json(value: &Value) -> DecorationItem {
    let position = |position: &Value| DecorationPosition {
        line: position["line"].as_u64().expect("line") as usize,
        character: position["character"].as_u64().expect("character") as usize,
    };
    DecorationItem {
        start: position(&value["start"]),
        end: position(&value["end"]),
        properties: value["properties"]
            .as_object()
            .expect("properties")
            .iter()
            .map(|(key, value)| (key.clone(), value.as_str().expect("string").to_owned()))
            .collect::<BTreeMap<_, _>>(),
        always_wrap: false,
    }
}

#[test]
fn render_token_lines_matches() {
    let golden = golden();
    assert!(golden.token_lines.len() >= 100);
    for (index, case) in golden.token_lines.iter().enumerate() {
        let lines: Vec<Vec<ThemedToken>> = case
            .lines
            .iter()
            .map(|line| line.iter().map(token_from_json).collect())
            .collect();
        let decorations: Vec<DecorationItem> =
            case.decorations.iter().map(decoration_from_json).collect();
        let state: Option<Vec<Option<LineInfo>>> = case
            .state
            .as_ref()
            .map(|state| state.iter().copied().map(Some).collect());
        let actual = render_token_lines(
            &lines,
            RenderTokenLinesOptions {
                state: state.as_deref(),
                merge_whitespaces: match case.merge_whitespaces.as_str() {
                    "always" => MergeWhitespaces::Always,
                    "never" => MergeWhitespaces::Never,
                    other => panic!("unexpected mergeWhitespaces {other}"),
                },
                decorations: &decorations,
            },
        );
        assert_json_eq(
            &format!("renderTokenLines #{index}"),
            &case.expected,
            &serde_json::to_value(&actual).expect("serialize"),
        );
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct HeaderCase {
    label: String,
    input: Value,
    mode: FileHeaderRenderMode,
    sticky_header: bool,
    expected: Value,
}

#[test]
fn create_file_header_element_matches() {
    let golden = golden();
    assert!(golden.headers.len() >= 20);
    for case in &golden.headers {
        let diff: FileDiffMetadata;
        let file: FileContents;
        let file_or_diff = if case.input.get("hunks").is_some() {
            diff = serde_json::from_value(case.input.clone()).expect("diff");
            FileOrDiff::Diff(&diff)
        } else {
            file = serde_json::from_value(case.input.clone()).expect("file");
            FileOrDiff::File(&file)
        };
        let actual = create_file_header_element(CreateFileHeaderElementProps {
            file_or_diff,
            mode: case.mode,
            sticky_header: case.sticky_header,
        });
        assert_json_eq(
            &format!(
                "header {} mode={:?} sticky={}",
                case.label, case.mode, case.sticky_header
            ),
            &case.expected,
            &serde_json::to_value(&actual).expect("serialize"),
        );
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlainCase {
    force_plain_text: bool,
    starting_line: Option<usize>,
    total_lines: Option<usize>,
    expanded: Value,
    collapsed_context_threshold: Option<usize>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RenderDiffCase {
    diff: usize,
    theme: ThemeOption,
    line_diff_type: LineDiffTypes,
    max_line_diff_length: usize,
    plain: Option<PlainCase>,
    theme_styles: String,
    base_theme_type: Option<crate::types::options::BaseThemeType>,
    deletion_count: usize,
    addition_count: usize,
    exact: [String; 2],
    flat: [String; 2],
}

/// `null | true | [[hunkIndex, {fromStart, fromEnd}]]`
fn decode_expanded(value: &Value) -> Option<ExpandedHunks<'static>> {
    match value {
        Value::Null => None,
        Value::Bool(true) => Some(ExpandedHunks::All),
        Value::Array(entries) => Some(ExpandedHunks::owned(
            entries
                .iter()
                .map(|entry| {
                    let index = entry[0].as_u64().expect("index") as usize;
                    let region: HunkExpansionRegion =
                        serde_json::from_value(entry[1].clone()).expect("region");
                    (index, region)
                })
                .collect(),
        )),
        other => panic!("unexpected expanded encoding {other}"),
    }
}

fn flatten(line: &Option<crate::hast::ElementContent>) -> Value {
    use crate::hast::ElementContent;
    match line {
        None => Value::Null,
        Some(ElementContent::Text(text)) => serde_json::to_value(text).expect("text"),
        Some(ElementContent::Element(element)) => {
            let mut flat = element.clone();
            flat.children = vec![ElementContent::Text(crate::hast::Text::new(
                &line.as_ref().expect("some").text_content(),
            ))];
            serde_json::to_value(&flat).expect("element")
        }
    }
}

#[test]
fn render_diff_with_highlighter_matches() {
    let golden = golden();
    assert!(golden.render_diffs.len() >= 150);
    let mut exact = 0;
    let mut flat_only = Vec::new();
    for (index, case) in golden.render_diffs.iter().enumerate() {
        let diff = &golden.diffs[case.diff];
        let plain_text = match &case.plain {
            None => ForceDiffPlainTextOptions {
                force_plain_text: false,
                lazy_line_ast: None,
                starting_line: None,
                total_lines: None,
                expanded_hunks: None,
                collapsed_context_threshold: None,
            },
            Some(plain) => ForceDiffPlainTextOptions {
                force_plain_text: plain.force_plain_text,
                lazy_line_ast: None,
                starting_line: plain.starting_line,
                total_lines: plain.total_lines,
                expanded_hunks: decode_expanded(&plain.expanded),
                collapsed_context_threshold: plain.collapsed_context_threshold,
            },
        };
        let result = render_diff_with_highlighter(
            diff,
            &PlainTextHighlighter,
            &RenderDiffOptions {
                theme: case.theme.clone(),
                use_token_transformer: false,
                tokenize_max_line_length: 1000,
                line_diff_type: case.line_diff_type,
                max_line_diff_length: case.max_line_diff_length,
            },
            &plain_text,
        );
        let context = format!(
            "renderDiff #{index} diff={} type={:?} max={}",
            case.diff, case.line_diff_type, case.max_line_diff_length
        );
        assert_eq!(result.theme_styles, case.theme_styles, "{context}");
        assert_eq!(result.base_theme_type, case.base_theme_type, "{context}");
        assert_eq!(
            result.code.deletion_lines.len(),
            case.deletion_count,
            "{context}"
        );
        assert_eq!(
            result.code.addition_lines.len(),
            case.addition_count,
            "{context}"
        );
        let flat_actual = [
            fnv64(&Value::Array(
                result.code.deletion_lines.iter().map(flatten).collect(),
            )),
            fnv64(&Value::Array(
                result.code.addition_lines.iter().map(flatten).collect(),
            )),
        ];
        assert_eq!(
            flat_actual, case.flat,
            "{context}: flattened text\ndecoration-only differences so far: {flat_only:#?}"
        );
        let exact_actual = [
            fnv64(&serde_json::to_value(&result.code.deletion_lines).expect("json")),
            fnv64(&serde_json::to_value(&result.code.addition_lines).expect("json")),
        ];
        if exact_actual == case.exact {
            exact += 1;
        } else {
            // Only word-diff decorations may differ, since `similar` and jsdiff
            // tokenise differently; with no line diff there is nothing to differ.
            assert_ne!(
                case.line_diff_type,
                LineDiffTypes::None,
                "{context}\ndecoration-only differences so far: {flat_only:#?}"
            );
            flat_only.push(context);
        }
    }
    eprintln!(
        "renderDiff: {exact} exact of {}; decoration-only differences: {}",
        golden.render_diffs.len(),
        flat_only.len()
    );
}
