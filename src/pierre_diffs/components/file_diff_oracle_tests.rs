//! Differential tests for S12a-1, the options seam of `FileDiff`
//! (`oracle/s12a_file_diff.mjs`, golden `oracle/golden/s12a.json`): the
//! published class, constructed without a DOM, answers what it derives from
//! options (renderer option snapshot, interaction wiring, theme,
//! `canPartiallyRender`, `shouldApplyColumnVariables`), the dist's
//! `areOptionsEqual` runs over random option pairs, and `getLineIndexForDiff`
//! runs over parsed diffs. Callbacks are named in the spec; one name in one
//! field is one `Rc`, so identity comparison is exercised. Test-only, original
//! to this crate.

use std::cell::RefCell;
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::rc::Rc;

use serde::Deserialize;
use serde_json::Map;
use serde_json::Value;
use serde_json::json;

use crate::components::file_diff::FileDiffHunkSeparators;
use crate::components::file_diff::FileDiffOptions;
use crate::components::file_diff::get_line_index_for_diff;
use crate::diff::PatchOptions;
use crate::document::NodeKey;
use crate::js_collections::PtrEq;
use crate::managers::interaction_manager::GetLineIndexUtility;
use crate::managers::interaction_manager::LineCallback;
use crate::managers::interaction_manager::LineHoverHighlight;
use crate::managers::interaction_manager::SelectionCallback;
use crate::oracle_testing::assert_json_eq;
use crate::oracle_testing::load_golden;
use crate::renderers::diff_hunks_renderer::DiffHunksRendererOptions;
use crate::types::options::DiffIndicators;
use crate::types::options::DiffStyle;
use crate::types::options::HighlighterTypes;
use crate::types::options::LineDiffTypes;
use crate::types::options::Overflow;
use crate::types::options::ThemeOption;
use crate::types::options::ThemeTypes;
use crate::types::patch::FileDiffMetadata;
use crate::types::patch::ParseDiffOptions;
use crate::types::view::DiffLineAnnotation;
use crate::types::view::SelectedLineRange;
use crate::types::view::SelectionSide;
use crate::utils::options::get_diff_hunks_renderer_options::get_diff_hunks_renderer_options;
use crate::utils::predicates::are_options_equal::are_options_equal;

type Options = FileDiffOptions<NodeKey, u8, ()>;

#[derive(Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
struct Spec {
    theme: Option<ThemeOption>,
    theme_type: Option<ThemeTypes>,
    disable_line_numbers: Option<bool>,
    overflow: Option<Overflow>,
    collapsed: Option<bool>,
    disable_file_header: Option<bool>,
    disable_virtualization_buffers: Option<bool>,
    sticky_header: Option<bool>,
    preferred_highlighter: Option<HighlighterTypes>,
    #[serde(rename = "useCSSClasses")]
    use_css_classes: Option<bool>,
    use_token_transformer: Option<bool>,
    tokenize_max_line_length: Option<usize>,
    tokenize_max_length: Option<usize>,
    #[serde(rename = "unsafeCSS")]
    unsafe_css: Option<String>,
    diff_style: Option<DiffStyle>,
    diff_indicators: Option<DiffIndicators>,
    disable_background: Option<bool>,
    expand_unchanged: Option<bool>,
    collapsed_context_threshold: Option<usize>,
    line_diff_type: Option<LineDiffTypes>,
    max_line_diff_length: Option<usize>,
    expansion_line_count: Option<usize>,
    line_hover_highlight: Option<LineHoverHighlight>,
    enable_gutter_utility: Option<bool>,
    enable_line_selection: Option<bool>,
    controlled_selection: Option<bool>,
    disable_error_handling: Option<bool>,
    hunk_separators: Option<SeparatorSpec>,
    parse_diff_options: Option<ParseSpec>,
    #[serde(default)]
    load_diff_files: bool,
    on_gutter_utility_click: Option<String>,
    on_line_click: Option<String>,
    on_line_number_click: Option<String>,
    on_line_enter: Option<String>,
    on_line_leave: Option<String>,
    on_line_selected: Option<String>,
    on_line_selection_start: Option<String>,
    on_line_selection_change: Option<String>,
    on_line_selection_end: Option<String>,
    get_line_index: Option<String>,
    render_header_prefix: Option<String>,
    render_header_filename_suffix: Option<String>,
    render_header_metadata: Option<String>,
    render_custom_header: Option<String>,
    render_annotation: Option<String>,
    render_gutter_utility: Option<String>,
    on_post_render: Option<String>,
}

#[derive(Deserialize, Clone)]
#[serde(untagged)]
enum SeparatorSpec {
    Name(String),
    Function { r#fn: String },
}

#[derive(Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
struct ParseSpec {
    disable_line_matching: Option<bool>,
    context: Option<usize>,
    #[serde(default)]
    ignore_whitespace: bool,
}

/// One `Rc` per (field, name), like the JavaScript function table.
#[derive(Default)]
struct Functions {
    cache: RefCell<HashMap<String, Rc<dyn std::any::Any>>>,
}

impl Functions {
    fn get<T: ?Sized + 'static>(
        &self,
        field: &str,
        name: &str,
        make: impl FnOnce() -> Rc<T>,
    ) -> Rc<T> {
        let key = format!("{field}:{name}");
        let mut cache = self.cache.borrow_mut();
        let entry = cache
            .entry(key)
            .or_insert_with(|| Rc::new(make()) as Rc<dyn std::any::Any>);
        Rc::clone(entry)
            .downcast::<Rc<T>>()
            .map(|rc| Rc::<T>::clone(&rc))
            .unwrap_or_else(|_| panic!("callback type of {field}"))
    }
}

fn build(spec: &Spec, functions: &Functions) -> Options {
    let mut options = Options::default();
    options.base.base.theme = spec.theme.clone();
    let code = &mut options.base.base;
    code.theme_type = spec.theme_type;
    code.disable_line_numbers = spec.disable_line_numbers;
    code.overflow = spec.overflow;
    code.collapsed = spec.collapsed;
    code.disable_file_header = spec.disable_file_header;
    code.disable_virtualization_buffers = spec.disable_virtualization_buffers;
    code.sticky_header = spec.sticky_header;
    code.preferred_highlighter = spec.preferred_highlighter;
    code.use_css_classes = spec.use_css_classes;
    code.use_token_transformer = spec.use_token_transformer;
    code.tokenize_max_line_length = spec.tokenize_max_line_length;
    code.tokenize_max_length = spec.tokenize_max_length;
    code.unsafe_css.clone_from(&spec.unsafe_css);
    options.base.diff_style = spec.diff_style;
    options.base.diff_indicators = spec.diff_indicators;
    options.base.disable_background = spec.disable_background;
    options.base.expand_unchanged = spec.expand_unchanged;
    options.base.collapsed_context_threshold = spec.collapsed_context_threshold;
    options.base.line_diff_type = spec.line_diff_type;
    options.base.max_line_diff_length = spec.max_line_diff_length;
    options.base.expansion_line_count = spec.expansion_line_count;
    options.base.parse_diff_options =
        spec.parse_diff_options
            .as_ref()
            .map(|parse| ParseDiffOptions {
                create_patch: PatchOptions {
                    context: parse.context,
                    header_options: None,
                    ignore_whitespace: parse.ignore_whitespace,
                },
                disable_line_matching: parse.disable_line_matching,
            });
    options.disable_error_handling = spec.disable_error_handling;
    options.has_diff_file_loader = spec.load_diff_files;
    options.hunk_separators = spec
        .hunk_separators
        .as_ref()
        .map(|separators| match separators {
            SeparatorSpec::Name(name) => match name.as_str() {
                "simple" => FileDiffHunkSeparators::Simple,
                "metadata" => FileDiffHunkSeparators::Metadata,
                "line-info" => FileDiffHunkSeparators::LineInfo,
                "line-info-basic" => FileDiffHunkSeparators::LineInfoBasic,
                other => panic!("unknown separators {other}"),
            },
            SeparatorSpec::Function { r#fn } => {
                FileDiffHunkSeparators::Custom(PtrEq(functions.get("hunkSeparators", r#fn, || {
                    Rc::new(|_: &_| None)
                        as Rc<dyn Fn(&crate::types::lines::HunkData) -> Option<u8>>
                })))
            }
        });

    let interaction = &mut options.interaction;
    interaction.line_hover_highlight = spec.line_hover_highlight;
    interaction.enable_gutter_utility = spec.enable_gutter_utility;
    interaction.enable_line_selection = spec.enable_line_selection;
    interaction.controlled_selection = spec.controlled_selection;
    let line = |field: &str, name: &Option<String>| -> Option<LineCallback<NodeKey>> {
        name.as_ref()
            .map(|name| functions.get(field, name, || Rc::new(|_: &_| {}) as LineCallback<NodeKey>))
    };
    let selection = |field: &str, name: &Option<String>| -> Option<SelectionCallback> {
        name.as_ref().map(|name| {
            functions.get(field, name, || {
                Rc::new(|_: Option<&SelectedLineRange>| {}) as SelectionCallback
            })
        })
    };
    interaction.on_gutter_utility_click = spec.on_gutter_utility_click.as_ref().map(|name| {
        functions.get("onGutterUtilityClick", name, || {
            Rc::new(|_: SelectedLineRange| {}) as Rc<dyn Fn(SelectedLineRange)>
        })
    });
    interaction.on_line_click = line("onLineClick", &spec.on_line_click);
    interaction.on_line_number_click = line("onLineNumberClick", &spec.on_line_number_click);
    interaction.on_line_enter = line("onLineEnter", &spec.on_line_enter);
    interaction.on_line_leave = line("onLineLeave", &spec.on_line_leave);
    interaction.on_line_selected = selection("onLineSelected", &spec.on_line_selected);
    interaction.on_line_selection_start =
        selection("onLineSelectionStart", &spec.on_line_selection_start);
    interaction.on_line_selection_change =
        selection("onLineSelectionChange", &spec.on_line_selection_change);
    interaction.on_line_selection_end =
        selection("onLineSelectionEnd", &spec.on_line_selection_end);
    interaction.get_line_index = spec.get_line_index.as_ref().map(|name| {
        functions.get("getLineIndex", name, || {
            Rc::new(|_: usize, _: Option<SelectionSide>| None) as GetLineIndexUtility
        })
    });

    let header = |field: &str, name: &Option<String>| {
        name.as_ref().map(|name| {
            PtrEq(functions.get(field, name, || {
                Rc::new(|_: &FileDiffMetadata| None) as Rc<dyn Fn(&FileDiffMetadata) -> Option<u8>>
            }))
        })
    };
    options.render_header_prefix = header("renderHeaderPrefix", &spec.render_header_prefix);
    options.render_header_filename_suffix = header(
        "renderHeaderFilenameSuffix",
        &spec.render_header_filename_suffix,
    );
    options.render_header_metadata = header("renderHeaderMetadata", &spec.render_header_metadata);
    options.render_custom_header = header("renderCustomHeader", &spec.render_custom_header);
    options.render_annotation = spec.render_annotation.as_ref().map(|name| {
        PtrEq(functions.get("renderAnnotation", name, || {
            Rc::new(|_: &DiffLineAnnotation<()>, _: &FileDiffMetadata| None)
                as Rc<dyn Fn(&DiffLineAnnotation<()>, &FileDiffMetadata) -> Option<u8>>
        }))
    });
    options.render_gutter_utility = spec.render_gutter_utility.as_ref().map(|name| {
        PtrEq(functions.get("renderGutterUtility", name, || {
            Rc::new(|_: &dyn Fn() -> _| None)
                as Rc<
                    dyn Fn(
                        &dyn Fn() -> Option<
                            crate::managers::interaction_manager::GetHoveredLineResult,
                        >,
                    ) -> Option<u8>,
                >
        }))
    });
    options.on_post_render = spec.on_post_render.as_ref().map(|name| {
        PtrEq(functions.get("onPostRender", name, || {
            Rc::new(|_: &NodeKey, _: crate::types::options::PostRenderPhase| {})
                as Rc<dyn Fn(&NodeKey, crate::types::options::PostRenderPhase)>
        }))
    });
    options
}

pub(super) fn renderer_options_json(renderer: &DiffHunksRendererOptions) -> Value {
    let base = &renderer.base;
    let code = &base.base;
    let mut out = Map::new();
    let mut put = |key: &str, value: Option<Value>| {
        if let Some(value) = value {
            out.insert(key.to_owned(), value);
        }
    };
    put(
        "theme",
        code.theme.as_ref().map(|theme| match theme {
            ThemeOption::Name(name) => json!(name),
            ThemeOption::Themes(themes) => json!({ "dark": themes.dark, "light": themes.light }),
        }),
    );
    put(
        "disableLineNumbers",
        code.disable_line_numbers.map(Value::from),
    );
    put(
        "overflow",
        code.overflow.map(|value| {
            json!(match value {
                Overflow::Scroll => "scroll",
                Overflow::Wrap => "wrap",
            })
        }),
    );
    put("collapsed", code.collapsed.map(Value::from));
    put(
        "disableFileHeader",
        code.disable_file_header.map(Value::from),
    );
    put(
        "disableVirtualizationBuffers",
        code.disable_virtualization_buffers.map(Value::from),
    );
    put("stickyHeader", code.sticky_header.map(Value::from));
    put(
        "preferredHighlighter",
        code.preferred_highlighter.map(|value| {
            json!(match value {
                HighlighterTypes::ShikiJs => "shiki-js",
                HighlighterTypes::ShikiWasm => "shiki-wasm",
                HighlighterTypes::Highlights => "highlights",
            })
        }),
    );
    put("useCSSClasses", code.use_css_classes.map(Value::from));
    put(
        "useTokenTransformer",
        code.use_token_transformer.map(Value::from),
    );
    put(
        "tokenizeMaxLineLength",
        code.tokenize_max_line_length.map(Value::from),
    );
    put(
        "tokenizeMaxLength",
        code.tokenize_max_length.map(Value::from),
    );
    put(
        "diffStyle",
        base.diff_style.map(|value| {
            json!(match value {
                DiffStyle::Unified => "unified",
                DiffStyle::Split => "split",
            })
        }),
    );
    put(
        "diffIndicators",
        base.diff_indicators.map(|value| {
            json!(match value {
                DiffIndicators::Bars => "bars",
                DiffIndicators::Classic => "classic",
                DiffIndicators::None => "none",
            })
        }),
    );
    put(
        "disableBackground",
        base.disable_background.map(Value::from),
    );
    put(
        "hunkSeparators",
        base.hunk_separators.map(|value| {
            json!(match value {
                crate::types::options::HunkSeparators::Simple => "simple",
                crate::types::options::HunkSeparators::Metadata => "metadata",
                crate::types::options::HunkSeparators::LineInfo => "line-info",
                crate::types::options::HunkSeparators::LineInfoBasic => "line-info-basic",
                crate::types::options::HunkSeparators::Custom => "custom",
            })
        }),
    );
    put("expandUnchanged", base.expand_unchanged.map(Value::from));
    put(
        "loadDiffFiles",
        renderer.has_diff_file_loader.then_some(json!(true)),
    );
    put(
        "parseDiffOptions",
        base.parse_diff_options.as_ref().map(|parse| {
            let mut object = Map::new();
            if let Some(value) = parse.disable_line_matching {
                object.insert("disableLineMatching".to_owned(), json!(value));
            }
            if let Some(value) = parse.create_patch.context {
                object.insert("context".to_owned(), json!(value));
            }
            if parse.create_patch.ignore_whitespace {
                object.insert("ignoreWhitespace".to_owned(), json!(true));
            }
            Value::Object(object)
        }),
    );
    put(
        "collapsedContextThreshold",
        base.collapsed_context_threshold.map(Value::from),
    );
    put(
        "lineDiffType",
        base.line_diff_type.map(|value| {
            json!(match value {
                LineDiffTypes::WordAlt => "word-alt",
                LineDiffTypes::WordLine => "word-line",
                LineDiffTypes::Word => "word",
                LineDiffTypes::Char => "char",
                LineDiffTypes::None => "none",
            })
        }),
    );
    put(
        "maxLineDiffLength",
        base.max_line_diff_length.map(Value::from),
    );
    put(
        "expansionLineCount",
        base.expansion_line_count.map(Value::from),
    );
    put(
        "headerRenderMode",
        renderer.header_render_mode.map(|value| {
            json!(match value {
                crate::types::options::FileHeaderRenderMode::Default => "default",
                crate::types::options::FileHeaderRenderMode::Custom => "custom",
            })
        }),
    );
    Value::Object(out)
}

#[derive(Deserialize)]
struct Derived {
    spec: Spec,
    result: Value,
}

#[derive(Deserialize)]
struct EqualityCase {
    left: Option<Spec>,
    right: Option<Spec>,
    expected: bool,
}

type LineIndexRow = (String, usize, Option<(usize, usize)>);

#[derive(Deserialize)]
struct LineIndexCase {
    rows: Vec<LineIndexRow>,
}

#[derive(Deserialize)]
struct Golden {
    derived: Vec<Derived>,
    #[serde(rename = "defaultTheme")]
    default_theme: Value,
    equality: Vec<EqualityCase>,
    diffs: Vec<FileDiffMetadata>,
    #[serde(rename = "lineIndex")]
    line_index: Vec<LineIndexCase>,
    #[serde(rename = "noDiff")]
    no_diff: Option<(usize, usize)>,
}

#[test]
fn derived_option_state_matches_the_published_class() {
    let golden: Golden = load_golden("s12a.json");
    let functions = Functions::default();
    assert_eq!(
        json!(match Options::default().get_theme() {
            ThemeOption::Name(name) => json!(name),
            ThemeOption::Themes(themes) => json!({ "dark": themes.dark, "light": themes.light }),
        }),
        golden.default_theme
    );
    for (index, case) in golden.derived.iter().enumerate() {
        let options = build(&case.spec, &functions);
        let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
            options.interaction_options(
                Some(Rc::new(|_, _, _| {})),
                options.interaction.get_line_index.clone(),
            )
        }));
        if let Some(message) = case.result.get("throws") {
            let payload = outcome.err().expect("upstream throws");
            let text = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| {
                    payload
                        .downcast_ref::<&str>()
                        .map(|text| (*text).to_owned())
                })
                .expect("a string panic");
            assert_eq!(&Value::String(text), message, "case {index}");
            continue;
        }
        let interaction = outcome.unwrap_or_else(|_| panic!("case {index} must not throw"));
        let expected = &case.result;
        assert_json_eq(
            &format!("renderer options of case {index}"),
            &expected["rendererOptions"],
            &renderer_options_json(&get_diff_hunks_renderer_options(&options)),
        );
        assert_eq!(
            json!(interaction.base.enable_gutter_utility),
            expected["enableGutterUtility"],
            "case {index}"
        );
        assert_eq!(
            json!(interaction.uses_custom_gutter_utility),
            expected["usesCustomGutterUtility"],
            "case {index}"
        );
        assert_eq!(
            json!(interaction.on_hunk_expand.is_some()),
            expected["hasOnHunkExpand"],
            "case {index}"
        );
        let mut can_partially = Vec::new();
        for force in [false, true] {
            for annotations in [false, true] {
                for content in [false, true] {
                    can_partially.push(options.can_partially_render(force, annotations, content));
                }
            }
        }
        assert_eq!(
            json!(can_partially),
            expected["canPartiallyRender"],
            "case {index}"
        );
        let column_variables: Vec<bool> = [Overflow::Scroll, Overflow::Wrap]
            .into_iter()
            .map(|overflow| options.should_apply_column_variables(overflow, 0, false))
            .collect();
        assert_eq!(
            json!(column_variables),
            expected["shouldApplyColumnVariables"],
            "case {index}"
        );
    }
}

#[test]
fn are_options_equal_matches_the_published_function() {
    let golden: Golden = load_golden("s12a.json");
    let functions = Functions::default();
    let mut equal = 0;
    for (index, case) in golden.equality.iter().enumerate() {
        let left = case.left.as_ref().map(|spec| build(spec, &functions));
        let right = case.right.as_ref().map(|spec| build(spec, &functions));
        let actual = are_options_equal(left.as_ref(), right.as_ref());
        assert_eq!(actual, case.expected, "pair {index}");
        equal += usize::from(actual);
        // The derived `PartialEq` agrees wherever the theme is spelled out or
        // both themes are absent (`areOptionsEqual` equates absent and default).
        if let (Some(left), Some(right)) = (&left, &right)
            && left.base.base.theme.is_some() == right.base.base.theme.is_some()
        {
            assert_eq!(left == right, case.expected, "derived pair {index}");
        }
    }
    assert!(equal > 50, "the generator must produce equal pairs");
}

#[test]
fn line_index_for_diff_matches_the_published_method() {
    let golden: Golden = load_golden("s12a.json");
    assert_eq!(
        get_line_index_for_diff(None, 1, SelectionSide::Additions),
        golden.no_diff
    );
    for (diff, case) in golden.diffs.iter().zip(&golden.line_index) {
        for (side, line_number, expected) in &case.rows {
            let side = if side == "deletions" {
                SelectionSide::Deletions
            } else {
                SelectionSide::Additions
            };
            assert_eq!(
                get_line_index_for_diff(Some(diff), *line_number, side),
                *expected,
                "{side:?} line {line_number} of {}",
                diff.name
            );
        }
    }
}
