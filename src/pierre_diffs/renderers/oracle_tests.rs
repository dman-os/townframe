//! Differential tests for S6: the published dist's `DiffHunksRenderer`
//! (`processDiffResult`, `renderFullAST`, and the composition `renderDiff`
//! performs for a plain-text diff) ran over the stub highlighter with fuzzed
//! options, annotations, expansions, render ranges and injected-row hooks
//! (`oracle/golden/render_s6.json`; the diffs are the ones of `render_s5.json`).
//!
//! Row count, total lines and the hunk data are compared exactly. The
//! rendered columns and the full AST are compared by canonical hash; where
//! the word-diff decorations come from `similar` instead of jsdiff the exact
//! hash may differ, and the hash of the same result with every code line
//! reduced to its text must still match. Test-only, original to this crate.

use serde::Deserialize;
use serde_json::Value;

use crate::hast::ElementContent;
use crate::hast::Properties;
use crate::hast::Text;
use crate::highlighter_types::PlainTextHighlighter;
use crate::oracle_testing::assert_json_eq;
use crate::oracle_testing::fnv64;
use crate::oracle_testing::load_golden;
use crate::renderers::diff_hunks_renderer::DiffHunksRenderer;
use crate::renderers::diff_hunks_renderer::DiffHunksRendererHooks;
use crate::renderers::diff_hunks_renderer::DiffHunksRendererOptions;
use crate::renderers::diff_hunks_renderer::InjectedRow;
use crate::renderers::diff_hunks_renderer::RenderedLineContext;
use crate::renderers::diff_hunks_renderer::SplitInjectedRow;
use crate::renderers::diff_hunks_renderer::SplitInjectedRowPlacement;
use crate::renderers::diff_hunks_renderer::UnifiedInjectedRowPlacement;
use crate::types::layout::RenderRange;
use crate::types::lines::ExpandedHunks;
use crate::types::lines::ExpansionDirections;
use crate::types::lines::ForceDiffPlainTextOptions;
use crate::types::lines::HunkData;
use crate::types::options::BaseCodeOptions;
use crate::types::options::BaseDiffOptions;
use crate::types::options::DiffIndicators;
use crate::types::options::DiffStyle;
use crate::types::options::FileHeaderRenderMode;
use crate::types::options::HunkSeparators;
use crate::types::options::LineDiffTypes;
use crate::types::options::Overflow;
use crate::types::options::RenderDiffOptions;
use crate::types::options::ThemeOption;
use crate::types::patch::FileDiffMetadata;
use crate::types::view::DiffLineAnnotation;
use crate::utils::render::get_line_annotation_name::AnnotationName;
use crate::utils::render::get_line_annotation_name::get_line_annotation_name;
use crate::utils::render::hast_utils::create_hast_element;
use crate::utils::render::render_diff_with_highlighter::render_diff_with_highlighter;

#[derive(Deserialize)]
struct DiffGolden {
    diffs: Vec<FileDiffMetadata>,
}

#[derive(Deserialize)]
struct Golden {
    cases: Vec<Case>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Options {
    theme: ThemeOption,
    diff_style: DiffStyle,
    hunk_separators: HunkSeparators,
    expand_unchanged: bool,
    expansion_line_count: usize,
    collapsed_context_threshold: usize,
    disable_file_header: bool,
    header_render_mode: FileHeaderRenderMode,
    sticky_header: bool,
    diff_indicators: DiffIndicators,
    disable_background: bool,
    disable_line_numbers: bool,
    overflow: Overflow,
    line_diff_type: LineDiffTypes,
    max_line_diff_length: usize,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Range {
    starting_line: f64,
    total_lines: Option<f64>,
    buffer_before: f64,
    buffer_after: f64,
}

#[derive(Deserialize)]
struct Expansion {
    index: usize,
    direction: ExpansionDirections,
    count: Option<usize>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Expected {
    row_count: usize,
    total_lines: usize,
    hunk_data: Vec<HunkData>,
    exact: [String; 2],
    flat: [String; 2],
    full: Option<Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Case {
    diff: usize,
    lang: Option<String>,
    mode: String,
    plain: bool,
    options: Options,
    has_loader: bool,
    annotations: Vec<DiffLineAnnotation<Value>>,
    expansions: Vec<Expansion>,
    hooks: bool,
    range: Range,
    expected: Expected,
}

fn injected_row(label: &str, key: usize) -> InjectedRow {
    InjectedRow {
        content: create_hast_element(
            "div",
            vec![ElementContent::Text(Text::new(label))],
            Properties::new().with("data-injected", format!("{label}-{key}")),
        ),
        gutter: create_hast_element(
            "div",
            Vec::new(),
            Properties::new().with("data-injected-gutter", format!("{label}-{key}")),
        ),
    }
}

/// Injected rows chosen by a fixed function of (hunkIndex, lineIndex), the
/// same function as `HookedRenderer` in `oracle/generate.mjs`.
struct OracleHooks;

impl DiffHunksRendererHooks for OracleHooks {
    fn unified_injected_rows_for_line(
        &self,
        context: &RenderedLineContext,
    ) -> Option<UnifiedInjectedRowPlacement> {
        let key = context.hunk_index * 31 + context.line_index;
        match key % 4 {
            1 => Some(UnifiedInjectedRowPlacement {
                before: Some(vec![injected_row("ub", key)]),
                after: None,
            }),
            2 => Some(UnifiedInjectedRowPlacement {
                before: None,
                after: Some(vec![injected_row("ua1", key), injected_row("ua2", key)]),
            }),
            3 => Some(UnifiedInjectedRowPlacement {
                before: Some(vec![injected_row("ub", key)]),
                after: Some(vec![injected_row("ua", key)]),
            }),
            _ => None,
        }
    }

    fn split_injected_rows_for_line(
        &self,
        context: &RenderedLineContext,
    ) -> Option<SplitInjectedRowPlacement> {
        let key = context.hunk_index * 31 + context.line_index;
        match key % 5 {
            1 => Some(SplitInjectedRowPlacement {
                before: Some(vec![SplitInjectedRow {
                    deletion: Some(injected_row("sbd", key)),
                    addition: None,
                }]),
                after: None,
            }),
            2 => Some(SplitInjectedRowPlacement {
                before: None,
                after: Some(vec![SplitInjectedRow {
                    deletion: None,
                    addition: Some(injected_row("saa", key)),
                }]),
            }),
            3 => Some(SplitInjectedRowPlacement {
                before: Some(vec![SplitInjectedRow {
                    deletion: Some(injected_row("sbd", key)),
                    addition: Some(injected_row("sba", key)),
                }]),
                after: Some(vec![
                    SplitInjectedRow {
                        deletion: Some(injected_row("sad", key)),
                        addition: None,
                    },
                    SplitInjectedRow {
                        deletion: None,
                        addition: Some(injected_row("saa", key)),
                    },
                ]),
            }),
            4 => Some(SplitInjectedRowPlacement {
                before: Some(vec![SplitInjectedRow {
                    deletion: None,
                    addition: None,
                }]),
                after: None,
            }),
            _ => None,
        }
    }
}

/// Every element carrying `data-line` (a rendered code line) reduced to its
/// text, as `flattenCodeLines` in generate.mjs.
fn flatten_code_lines(value: &Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.iter().map(flatten_code_lines).collect()),
        Value::Object(map) => {
            let is_code_line = map.get("type").and_then(Value::as_str) == Some("element")
                && map
                    .get("properties")
                    .is_some_and(|properties| properties.get("data-line").is_some());
            if is_code_line {
                let mut flat = map.clone();
                flat.insert(
                    "children".to_owned(),
                    serde_json::json!([{ "type": "text", "value": text_of(value) }]),
                );
                Value::Object(flat)
            } else {
                Value::Object(
                    map.iter()
                        .map(|(key, value)| (key.clone(), flatten_code_lines(value)))
                        .collect(),
                )
            }
        }
        other => other.clone(),
    }
}

fn text_of(value: &Value) -> String {
    match value.get("type").and_then(Value::as_str) {
        Some("text") => value["value"].as_str().expect("text value").to_owned(),
        _ => value
            .get("children")
            .and_then(Value::as_array)
            .map(|children| children.iter().map(text_of).collect())
            .unwrap_or_default(),
    }
}

fn renderer_options(case: &Case) -> DiffHunksRendererOptions {
    let options = &case.options;
    DiffHunksRendererOptions {
        base: BaseDiffOptions {
            base: BaseCodeOptions {
                theme: Some(options.theme.clone()),
                disable_line_numbers: Some(options.disable_line_numbers),
                overflow: Some(options.overflow),
                disable_file_header: Some(options.disable_file_header),
                sticky_header: Some(options.sticky_header),
                ..BaseCodeOptions::default()
            },
            diff_style: Some(options.diff_style),
            diff_indicators: Some(options.diff_indicators),
            disable_background: Some(options.disable_background),
            hunk_separators: Some(options.hunk_separators),
            expand_unchanged: Some(options.expand_unchanged),
            collapsed_context_threshold: Some(options.collapsed_context_threshold),
            line_diff_type: Some(options.line_diff_type),
            max_line_diff_length: Some(options.max_line_diff_length),
            expansion_line_count: Some(options.expansion_line_count),
            parse_diff_options: None,
        },
        header_render_mode: Some(options.header_render_mode),
        has_diff_file_loader: case.has_loader,
    }
}

fn render_range(range: &Range) -> RenderRange {
    RenderRange {
        starting_line: range.starting_line,
        total_lines: range.total_lines.unwrap_or(f64::INFINITY),
        buffer_before: range.buffer_before,
        buffer_after: range.buffer_after,
    }
}

#[test]
fn diff_hunks_renderer_matches() {
    let diffs = load_golden::<DiffGolden>("render_s5.json").diffs;
    let golden: Golden = load_golden("render_s6.json");
    assert!(golden.cases.len() >= 300);
    let mut exact = 0;
    let mut word_diff_only = 0;
    for (index, case) in golden.cases.iter().enumerate() {
        let mut diff = diffs[case.diff].clone();
        if let Some(lang) = &case.lang {
            diff.lang = Some(lang.clone());
        }
        let mut renderer: DiffHunksRenderer<Value> = DiffHunksRenderer::with_hooks(
            renderer_options(case),
            Box::new(|annotation| {
                get_line_annotation_name(AnnotationName::DiffLine {
                    side: annotation.side,
                    line_number: annotation.line_number,
                })
            }),
            if case.hooks {
                Box::new(OracleHooks)
            } else {
                Box::new(crate::renderers::diff_hunks_renderer::DefaultHooks)
            },
        );
        renderer.set_line_annotations(case.annotations.clone());
        for expansion in &case.expansions {
            renderer.expand_hunk(expansion.index, expansion.direction, expansion.count);
        }
        let range = render_range(&case.range);
        let context = format!(
            "case #{index} diff={} mode={} plain={} style={:?} separators={:?} lineDiff={:?} hooks={}",
            case.diff,
            case.mode,
            case.plain,
            case.options.diff_style,
            case.options.hunk_separators,
            case.options.line_diff_type,
            case.hooks
        );
        let result = if case.mode == "render" {
            renderer.render_diff(&PlainTextHighlighter, &diff, &range)
        } else {
            let render_options = RenderDiffOptions {
                theme: case.options.theme.clone(),
                use_token_transformer: false,
                tokenize_max_line_length: 1000,
                line_diff_type: case.options.line_diff_type,
                max_line_diff_length: case.options.max_line_diff_length,
            };
            let plain_text = ForceDiffPlainTextOptions {
                force_plain_text: case.plain,
                lazy_line_ast: None,
                starting_line: None,
                total_lines: None,
                expanded_hunks: case.plain.then_some(ExpandedHunks::All),
                collapsed_context_threshold: Some(case.options.collapsed_context_threshold),
            };
            let themed = render_diff_with_highlighter(
                &diff,
                &PlainTextHighlighter,
                &render_options,
                &plain_text,
            );
            renderer.process_diff_result(&diff, &range, themed)
        };

        assert_eq!(result.row_count, case.expected.row_count, "{context}");
        assert_eq!(result.total_lines, case.expected.total_lines, "{context}");
        assert_json_eq(
            &format!("{context}: hunkData"),
            &serde_json::to_value(&case.expected.hunk_data).expect("expected"),
            &serde_json::to_value(&result.hunk_data).expect("actual"),
        );
        let result_json = serde_json::to_value(&result).expect("result");
        let full_ast =
            serde_json::to_value(renderer.render_full_ast(&result, Vec::new())).expect("full ast");
        let flat = [
            fnv64(&flatten_code_lines(&result_json)),
            fnv64(&flatten_code_lines(&full_ast)),
        ];
        if flat != case.expected.flat {
            if let Some(full) = &case.expected.full {
                assert_json_eq(&format!("{context}: result"), full, &result_json);
            }
            panic!("{context}: flattened result differs from the dist's");
        }
        let hashes = [fnv64(&result_json), fnv64(&full_ast)];
        if hashes == case.expected.exact {
            exact += 1;
        } else {
            // Only word-diff decorations may differ (`similar` tokenises
            // differently from jsdiff); without a line diff nothing can.
            if case.options.line_diff_type == LineDiffTypes::None {
                if let Some(full) = &case.expected.full {
                    assert_json_eq(&format!("{context}: result"), full, &result_json);
                }
                panic!("{context}: exact hash differs without word diff");
            }
            word_diff_only += 1;
        }
    }
    eprintln!(
        "DiffHunksRenderer: {exact} exact of {}; decoration-only differences: {word_diff_only}",
        golden.cases.len()
    );
}
