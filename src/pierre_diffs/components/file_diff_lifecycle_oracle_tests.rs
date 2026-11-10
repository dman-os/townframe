//! Differential tests for S12a-2a, the first lifecycle seam of `FileDiff`
//! (`oracle/s12a_lifecycle.mjs`, golden `oracle/golden/s12a_lifecycle.json`:
//! handwritten scenarios plus seeded random call sequences). The published
//! class ran over a scripted fake DOM and the port replays each scenario over
//! `FakeDom` (a `FileDiffHost`), calling the same methods with the same named
//! diffs and annotation arrays. Fake-DOM nodes map to `Document` nodes by name
//! (the scenario tree is built into the document). The traces (attribute and
//! style writes, listener, observer and timer registration, frames, the
//! owner's callbacks, query results, snapshots of the instance, thrown
//! messages) must be equal. `render` is not exercised: the fields it writes
//! are set by a `set` step. Test-only, original to this crate.
//!
//! The module also holds the tests of the node ownership rule (retire and
//! discard), which have no upstream counterpart.

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::rc::Rc;

use serde::Deserialize;
use serde_json::Map;
use serde_json::Value;
use serde_json::json;

use super::file_diff_oracle_tests::renderer_options_json;
use crate::components::file_diff::FileDiff;
use crate::components::file_diff::FileDiffHunkSeparators;
use crate::components::file_diff::FileDiffOptions;
use crate::components::file_diff::FileDiffRenderData;
use crate::components::file_diff::LineAnnotations;
use crate::components::file_diff::Theme;
use crate::document::NodeKey;
use crate::fake_interaction_dom::FakeDom;
use crate::fake_interaction_dom::TreeNode;
use crate::highlighter_types::PlainTextHighlighter;
use crate::js_collections::PtrEq;
use crate::managers::interaction_manager::LineHoverHighlight;
use crate::managers::interaction_manager::SelectionCallback;
use crate::managers::interaction_manager::SelectionWriteOptions;
use crate::managers::resize_manager::ResizeObserverEntry;
use crate::managers::resize_manager::ResizeObserverHub;
use crate::managers::universal_rendering_manager::UniversalRenderingManager;
use crate::oracle_testing::assert_json_eq;
use crate::oracle_testing::load_golden;
use crate::types::options::AppliedThemeStyleCache;
use crate::types::options::BaseThemeType;
use crate::types::options::DiffStyle;
use crate::types::options::Overflow;
use crate::types::options::ThemeOption;
use crate::types::options::ThemeTypes;
use crate::types::patch::FileDiffMetadata;
use crate::types::view::AnnotationSide;
use crate::types::view::DiffLineAnnotation;
use crate::types::view::SelectedLineRange;

type Instance = FileDiff<FakeDom, u8, i64>;
type Options = FileDiffOptions<NodeKey, u8, i64>;

#[derive(Deserialize)]
struct WorldSpec {
    tree: Vec<TreeNode>,
    #[serde(default)]
    heights: HashMap<String, f64>,
}

#[derive(Deserialize, Clone)]
#[serde(untagged)]
enum SeparatorSpec {
    Name(String),
    Function { r#fn: String },
}

#[derive(Deserialize, Default, Clone)]
#[serde(rename_all = "camelCase")]
struct OptionsSpec {
    theme: Option<ThemeOption>,
    theme_type: Option<ThemeTypes>,
    overflow: Option<Overflow>,
    diff_style: Option<DiffStyle>,
    line_hover_highlight: Option<LineHoverHighlight>,
    enable_gutter_utility: Option<bool>,
    enable_line_selection: Option<bool>,
    controlled_selection: Option<bool>,
    disable_file_header: Option<bool>,
    #[serde(default)]
    on_line_selected: bool,
    #[serde(default)]
    on_line_selection_start: bool,
    #[serde(default)]
    on_line_selection_change: bool,
    #[serde(default)]
    on_line_selection_end: bool,
    #[serde(default)]
    on_gutter_utility_click: bool,
    #[serde(default)]
    has_render_gutter_utility: bool,
    hunk_separators: Option<SeparatorSpec>,
}

#[derive(Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
struct WriteOptionsSpec {
    notify: Option<bool>,
    active_line_side: Option<AnnotationSide>,
    #[serde(default)]
    line_number_only: bool,
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "camelCase")]
enum Op {
    SetOptions {
        options: Option<OptionsSpec>,
    },
    #[serde(rename_all = "camelCase")]
    SetThemeType {
        theme_type: ThemeTypes,
    },
    Set {
        fields: Map<String, Value>,
    },
    SetLineAnnotations {
        annotations: String,
    },
    SetSelectedLines {
        range: Option<SelectedLineRange>,
        options: Option<WriteOptionsSpec>,
    },
    FlushDeferredLineState,
    FlushManagers,
    Frames {
        limit: usize,
        time: f64,
    },
    Query(Map<String, Value>),
    Attr {
        target: String,
        name: String,
        value: Option<String>,
    },
    Resize {
        entries: Vec<EntrySpec>,
    },
    Snapshot,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct EntrySpec {
    target: String,
    block_size: f64,
    inline_size: f64,
}

#[derive(Deserialize)]
struct Scenario {
    name: String,
    world: WorldSpec,
    options: Option<OptionsSpec>,
    steps: Vec<Op>,
}

#[derive(Deserialize)]
struct Case {
    scenario: Scenario,
    trace: Value,
}

#[derive(Deserialize)]
struct Golden {
    diffs: HashMap<String, FileDiffMetadata>,
    annotations: HashMap<String, Vec<DiffLineAnnotation<i64>>>,
    cases: Vec<Case>,
}

type SeparatorFunction = PtrEq<dyn Fn(&crate::types::lines::HunkData) -> Option<u8>>;

/// One `Rc` per name, like the JavaScript function table.
#[derive(Default)]
struct Functions {
    separators: std::cell::RefCell<HashMap<String, SeparatorFunction>>,
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| {
            payload
                .downcast_ref::<&str>()
                .map(|text| (*text).to_owned())
        })
        .expect("a string panic")
}

fn build_options(dom: &Rc<FakeDom>, functions: &Functions, spec: &OptionsSpec) -> Options {
    let mut options = Options::default();
    options.base.base.theme = spec.theme.clone();
    options.base.base.theme_type = spec.theme_type;
    options.base.base.overflow = spec.overflow;
    options.base.base.disable_file_header = spec.disable_file_header;
    options.base.diff_style = spec.diff_style;
    options.interaction.line_hover_highlight = spec.line_hover_highlight;
    options.interaction.enable_gutter_utility = spec.enable_gutter_utility;
    options.interaction.enable_line_selection = spec.enable_line_selection;
    options.interaction.controlled_selection = spec.controlled_selection;
    let selection = |name: &'static str, enabled: bool| -> Option<SelectionCallback> {
        enabled.then(|| {
            let dom = Rc::clone(dom);
            Rc::new(move |range: Option<&SelectedLineRange>| {
                dom.emit(json!(["cb", name, range]));
            }) as SelectionCallback
        })
    };
    options.interaction.on_line_selected = selection("onLineSelected", spec.on_line_selected);
    options.interaction.on_line_selection_start =
        selection("onLineSelectionStart", spec.on_line_selection_start);
    options.interaction.on_line_selection_change =
        selection("onLineSelectionChange", spec.on_line_selection_change);
    options.interaction.on_line_selection_end =
        selection("onLineSelectionEnd", spec.on_line_selection_end);
    options.interaction.on_gutter_utility_click = spec.on_gutter_utility_click.then(|| {
        let dom = Rc::clone(dom);
        Rc::new(move |range: SelectedLineRange| {
            dom.emit(json!(["cb", "onGutterUtilityClick", range]));
        }) as Rc<dyn Fn(SelectedLineRange)>
    });
    if spec.has_render_gutter_utility {
        options.render_gutter_utility = Some(PtrEq(Rc::new(|_: &dyn Fn() -> _| None)));
    }
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
            SeparatorSpec::Function { r#fn } => FileDiffHunkSeparators::Custom(
                functions
                    .separators
                    .borrow_mut()
                    .entry(r#fn.clone())
                    .or_insert_with(|| PtrEq(Rc::new(|_: &_| None)))
                    .clone(),
            ),
        });
    options
}

/// The values the scenario names, with their identity.
struct Named {
    diffs: HashMap<String, Rc<FileDiffMetadata>>,
    annotations: HashMap<String, LineAnnotations<i64>>,
    /// The instance's `EMPTY_LINE_ANNOTATIONS`.
    empty: LineAnnotations<i64>,
}

impl Named {
    fn diff(&self, name: &str) -> Rc<FileDiffMetadata> {
        Rc::clone(&self.diffs[name])
    }

    fn diff_name(&self, diff: Option<&Rc<FileDiffMetadata>>) -> Value {
        let Some(diff) = diff else { return Value::Null };
        self.diffs
            .iter()
            .find(|(_, candidate)| Rc::ptr_eq(candidate, diff))
            .map_or(json!("?"), |(name, _)| json!(name))
    }

    fn annotations_name(&self, list: &LineAnnotations<i64>) -> Value {
        if Rc::ptr_eq(list, &self.empty) {
            return json!("EMPTY");
        }
        self.annotations
            .iter()
            .find(|(_, candidate)| Rc::ptr_eq(candidate, list))
            .map_or(json!("?"), |(name, _)| json!(name))
    }

    fn data_json(&self, data: Option<&FileDiffRenderData<i64>>) -> Value {
        data.map_or(Value::Null, |data| {
            json!({
                "fileDiff": self.diff_name(Some(&data.file_diff)),
                "lineAnnotations": self.annotations_name(&data.line_annotations),
            })
        })
    }
}

fn snapshot(instance: &Instance, named: &Named) -> Value {
    let options = instance.options();
    let interaction = instance.interaction_manager().options();
    let working = instance.get_working_diff(None);
    let latest = instance.get_latest_annotations();
    let current = instance.line_annotations();
    // One borrow of the state, released before anything re-enters the instance.
    let (
        header_html,
        header_diff,
        managers_dirty,
        refresh_pending,
        deferred,
        pending,
        rendered,
        input,
    ) = {
        let state = instance.state();
        (
            state.header_cache.header.as_ref().map(header_text),
            state.header_cache.file_diff.clone(),
            state.managers_dirty,
            state.line_state_refresh_pending,
            state.deferred_selected_lines.clone(),
            state.pending_data.clone(),
            state.rendered_data.clone(),
            state.file_diff.clone(),
        )
    };
    let separators = match &options.hunk_separators {
        None => Value::Null,
        Some(FileDiffHunkSeparators::Custom(_)) => json!("fn"),
        Some(FileDiffHunkSeparators::Simple) => json!("simple"),
        Some(FileDiffHunkSeparators::Metadata) => json!("metadata"),
        Some(FileDiffHunkSeparators::LineInfo) => json!("line-info"),
        Some(FileDiffHunkSeparators::LineInfoBasic) => json!("line-info-basic"),
    };
    json!({
        "options": {
            "themeType": options.base.base.theme_type,
            "theme": serde_json::to_value(options.get_theme()).expect("a theme serializes"),
            "overflow": options.base.base.overflow,
            "diffStyle": options.base.diff_style,
            "hunkSeparators": separators,
        },
        "renderer": renderer_options_json(&instance.hunks_renderer().options),
        "interaction": {
            "enableGutterUtility": interaction.base.enable_gutter_utility,
            "usesCustomGutterUtility": interaction.uses_custom_gutter_utility,
            "hasOnHunkExpand": interaction.on_hunk_expand.is_some(),
            "hasGetLineIndex": interaction.base.get_line_index.is_some(),
            "enableLineSelection": interaction.base.enable_line_selection,
            "controlledSelection": interaction.base.controlled_selection,
            "lineHoverHighlight": interaction
                .base
                .line_hover_highlight
                .map(line_hover_highlight_name),
        },
        "selection": instance.interaction_manager().get_selection(),
        "headerCache": {
            "html": header_html,
            "fileDiff": named.diff_name(header_diff.as_ref()),
        },
        "managersDirty": managers_dirty,
        "lineStateRefreshPending": refresh_pending,
        "deferredSelectedLines": deferred.map(|(range, options)| json!([
            range,
            {
                "notify": options.notify,
                "activeLineSide": options.active_line_side,
                "lineNumberOnly": options.line_number_only,
            },
        ])),
        "latestAnnotations": named.annotations_name(&latest),
        "lineAnnotations": named.annotations_name(&current),
        "pendingData": named.data_json(pending.as_ref()),
        "renderedData": named.data_json(rendered.as_ref()),
        "inputDiff": named.diff_name(input.as_ref()),
        "workingDiff": named.diff_name(working.as_ref()),
        "renderedDiff": named.diff_name(rendered.as_ref().map(|data| &data.file_diff)),
    })
}

fn line_hover_highlight_name(value: LineHoverHighlight) -> &'static str {
    match value {
        LineHoverHighlight::Disabled => "disabled",
        LineHoverHighlight::Both => "both",
        LineHoverHighlight::Number => "number",
        LineHoverHighlight::Line => "line",
    }
}

fn node_field(dom: &FakeDom, value: &Value) -> Option<NodeKey> {
    value.as_str().map(|name| dom.id(name))
}

/// The `set` step: fields the class's `render` writes, by their upstream
/// names. A present key with `null` clears the field.
fn apply_set(dom: &FakeDom, instance: &Instance, named: &Named, fields: &Map<String, Value>) {
    let mut state = instance.state();
    for (key, value) in fields {
        match key.as_str() {
            "pre" => state.pre = node_field(dom, value),
            "codeDeletions" => state.code_deletions = node_field(dom, value),
            "codeAdditions" => state.code_additions = node_field(dom, value),
            "fileContainer" => state.file_container = node_field(dom, value),
            "fileDiff" => state.file_diff = value.as_str().map(|name| named.diff(name)),
            "renderedData" => {
                state.rendered_data = (!value.is_null()).then(|| FileDiffRenderData {
                    file_diff: named.diff(value["fileDiff"].as_str().expect("a diff name")),
                    line_annotations: match value["lineAnnotations"].as_str().expect("a name") {
                        "EMPTY" => Rc::clone(&named.empty),
                        name => Rc::clone(&named.annotations[name]),
                    },
                });
            }
            "appliedThemeCSS" => {
                state.applied_theme_css = (!value.is_null()).then(|| AppliedThemeStyleCache {
                    theme: serde_json::from_value(value["theme"].clone()).expect("a theme"),
                    theme_styles: value["themeStyles"].as_str().expect("styles").to_owned(),
                    theme_type: serde_json::from_value(value["themeType"].clone())
                        .expect("a theme type"),
                    base_theme_type: serde_json::from_value::<Option<BaseThemeType>>(
                        value["baseThemeType"].clone(),
                    )
                    .expect("a base theme type"),
                    scrollbar_gutter: value["scrollbarGutter"].as_f64(),
                });
            }
            "headerHtml" => state.header_cache.header = value.as_str().map(header_element),
            "headerDiff" => {
                state.header_cache.file_diff = value.as_str().map(|name| named.diff(name));
            }
            "managersDirty" => state.managers_dirty = value.as_bool().expect("a bool"),
            "lineStateRefreshPending" => {
                state.line_state_refresh_pending = value.as_bool().expect("a bool");
            }
            other => panic!("unknown field {other}"),
        }
    }
}

fn query(dom: &FakeDom, instance: &Instance, named: &Named, step: &Map<String, Value>) {
    let what = step["what"].as_str().expect("a query");
    let diff = || named.diff(step["diff"].as_str().expect("a diff name"));
    let overflow = || match step["overflow"].as_str().expect("an overflow") {
        "scroll" => Overflow::Scroll,
        _ => Overflow::Wrap,
    };
    let value = match what {
        "getLatestAnnotations" => named.annotations_name(&instance.get_latest_annotations()),
        "getAnnotationsForDiff" => {
            named.annotations_name(&instance.get_annotations_for_diff(&diff()))
        }
        "isNewAnnotations" => json!(instance.is_new_annotations(
            &named.annotations[step["annotations"].as_str().expect("annotations")]
        )),
        "getWorkingDiff" => {
            let fallback = step.get("diff").map(|_| diff());
            named.diff_name(instance.get_working_diff(fallback).as_ref())
        }
        "hasThemeChanged" => json!(Theme(instance).has_theme_changed()),
        "shouldApplyColumnVariables" => json!(instance.should_apply_column_variables(overflow())),
        "getLineIndex" => {
            let side = match step.get("side").and_then(Value::as_str) {
                Some("additions") => Some(AnnotationSide::Additions),
                Some("deletions") => Some(AnnotationSide::Deletions),
                _ => None,
            };
            let line_number = step["lineNumber"].as_u64().expect("a line number");
            json!(instance.get_line_index(usize::try_from(line_number).expect("small"), side))
        }
        "getHoveredLine" => {
            assert!(
                instance.get_hovered_line().is_none(),
                "no pointer events are scripted, so nothing is hovered"
            );
            Value::Null
        }
        other => panic!("unknown query {other}"),
    };
    dom.emit(json!(["result", what, value]));
}

/// The header element standing for the oracle's header HTML string.
fn header_element(text: &str) -> crate::hast::Element {
    crate::hast::Element::new(
        "div",
        crate::hast::Properties::new().with("data-test", text),
        Vec::new(),
    )
}

fn header_text(header: &crate::hast::Element) -> String {
    match header.properties.get("data-test") {
        Some(crate::hast::PropValue::Str(text)) => text.clone(),
        other => panic!("not a test header: {other:?}"),
    }
}

#[test]
fn file_diff_lifecycle_matches_the_published_class() {
    let golden: Golden = load_golden("s12a_lifecycle.json");
    assert!(golden.cases.len() > 200);
    let diffs: HashMap<String, Rc<FileDiffMetadata>> = golden
        .diffs
        .into_iter()
        .map(|(name, diff)| (name, Rc::new(diff)))
        .collect();
    let annotations: HashMap<String, LineAnnotations<i64>> = golden
        .annotations
        .into_iter()
        .map(|(name, list)| (name, Rc::new(list)))
        .collect();
    for case in &golden.cases {
        let scenario = &case.scenario;
        let dom = Rc::new(FakeDom::default());
        for node in &scenario.world.tree {
            dom.build(node, None);
        }
        for (name, height) in &scenario.world.heights {
            dom.heights.borrow_mut().insert(dom.id(name), *height);
        }
        let functions = Functions::default();
        let initial = scenario.options.clone().unwrap_or_default();
        let hub = ResizeObserverHub::new(Rc::clone(&dom));
        let constructed = std::panic::catch_unwind(AssertUnwindSafe(|| {
            Instance::new(
                Rc::clone(&dom),
                Rc::clone(&hub),
                UniversalRenderingManager::new(Rc::clone(&dom)),
                Rc::new(PlainTextHighlighter),
                build_options(&dom, &functions, &initial),
                None,
            )
        }));
        let instance = match constructed {
            Ok(instance) => instance,
            Err(payload) => {
                dom.recording().set(true);
                dom.emit(json!(["throws", panic_message(payload.as_ref())]));
                assert_json_eq(
                    &format!("scenario `{}`", scenario.name),
                    &case.trace,
                    &Value::Array(dom.trace()),
                );
                continue;
            }
        };
        // The oracle replaces `applyThemeState` on the published class with a
        // recorder; the port's seam does the same.
        {
            let recorded = Rc::clone(&dom);
            instance.state().theme_state_recorder = Some(Rc::new(
                move |container, styles, theme_type, base_theme_type| {
                    recorded.emit(json!([
                        "applyThemeState",
                        recorded.name_of(container),
                        styles,
                        format!("{theme_type:?}").to_lowercase(),
                        match base_theme_type {
                            None => Value::Null,
                            Some(BaseThemeType::Light) => json!("light"),
                            Some(BaseThemeType::Dark) => json!("dark"),
                        },
                    ]));
                },
            ));
        }
        let named = Named {
            diffs: diffs.clone(),
            annotations: annotations.clone(),
            empty: Rc::clone(&instance.state().empty_line_annotations),
        };
        dom.recording().set(true);

        for step in &scenario.steps {
            let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| match step {
                Op::SetOptions { options } => {
                    instance.set_options(
                        options
                            .as_ref()
                            .map(|spec| build_options(&dom, &functions, spec)),
                    );
                }
                Op::SetThemeType { theme_type } => instance.set_theme_type(*theme_type),
                Op::Set { fields } => apply_set(&dom, &instance, &named, fields),
                Op::SetLineAnnotations { annotations } => {
                    instance.set_line_annotations(Rc::clone(&named.annotations[annotations]));
                }
                Op::SetSelectedLines { range, options } => {
                    let options = options.clone().unwrap_or_default();
                    instance.set_selected_lines(
                        range.clone(),
                        SelectionWriteOptions {
                            notify: options.notify,
                            active_line_side: options.active_line_side,
                            line_number_only: options.line_number_only,
                        },
                    );
                }
                Op::FlushDeferredLineState => instance.flush_deferred_line_state(),
                Op::FlushManagers => instance.flush_managers(),
                Op::Frames { limit, time } => {
                    dom.frames.flush_frames(*limit, *time);
                }
                Op::Query(step) => query(&dom, &instance, &named, step),
                Op::Attr {
                    target,
                    name,
                    value,
                } => {
                    // A silent edit of the world (no trace entry).
                    let id = dom.id(target);
                    let recording = dom.recording().replace(false);
                    match value {
                        None => dom.doc.remove_attribute(id, name),
                        Some(value) => dom.doc.set_attribute(id, name, value),
                    }
                    dom.recording().set(recording);
                }
                Op::Resize { entries } => {
                    // Entries arrive only while the shared observer exists.
                    hub.handle_shared_resize_entries(
                        &entries
                            .iter()
                            .map(|entry| ResizeObserverEntry {
                                target: dom.id(&entry.target),
                                border_box_block_size: entry.block_size,
                                content_box_inline_size: entry.inline_size,
                            })
                            .collect::<Vec<_>>(),
                    );
                }
                Op::Snapshot => dom.emit(json!(["snapshot", snapshot(&instance, &named)])),
            }));
            if let Err(payload) = outcome {
                dom.emit(json!(["throws", panic_message(payload.as_ref())]));
            }
        }
        // Leave nothing registered, as the oracle does.
        instance.resize_manager().clean_up();
        instance.scroll_sync_manager().clean_up();
        instance.interaction_manager().clean_up();
        dom.emit(json!(["snapshot", snapshot(&instance, &named)]));
        assert_json_eq(
            &format!("scenario `{}`", scenario.name),
            &case.trace,
            &Value::Array(dom.trace()),
        );
    }
}

// ---- the node ownership rule (no upstream counterpart) ---------------------------

fn pre_tree(suffix: &str) -> TreeNode {
    let column = |kind: &str| {
        json!({
            "tag": "code",
            "name": format!("code-{kind}-{suffix}"),
            "attrs": [["data-code", ""], [format!("data-{kind}"), ""]],
            "children": [
                { "tag": "div", "name": format!("gutter-{kind}-{suffix}"), "attrs": [["data-gutter", ""]] },
                { "tag": "div", "name": format!("content-{kind}-{suffix}"), "attrs": [["data-content", ""]] },
            ],
        })
    };
    serde_json::from_value(json!({
        "tag": "pre",
        "name": format!("pre-{suffix}"),
        "attrs": [["data-diff-type", "split"]],
        "children": [column("deletions"), column("additions")],
    }))
    .expect("a tree")
}

fn instance_over(dom: &Rc<FakeDom>) -> Rc<Instance> {
    Instance::new(
        Rc::clone(dom),
        ResizeObserverHub::new(Rc::clone(dom)),
        UniversalRenderingManager::new(Rc::clone(dom)),
        Rc::new(PlainTextHighlighter),
        Options::default(),
        None,
    )
}

fn mark_dirty(instance: &Instance, pre: Option<NodeKey>) {
    let mut state = instance.state();
    state.pre = pre;
    state.managers_dirty = true;
}

// The managers hold the keys of the nodes a render replaced and use them (they
// unobserve them, remove the attributes they wrote) while they set up for the
// next render; discarding earlier would be a stale-key panic.
#[test]
fn retired_nodes_are_discarded_after_the_managers_have_set_up() {
    let dom = Rc::new(FakeDom::default());
    dom.recording().set(true);
    let instance = instance_over(&dom);
    let first = dom.build(&pre_tree("1"), None);
    mark_dirty(&instance, Some(first));
    instance.flush_managers();
    let first_children = dom.doc.children(first);

    let second = dom.build(&pre_tree("2"), None);
    instance.retire(first);
    assert!(
        dom.doc.exists(first),
        "retire detaches and keeps the node alive"
    );
    assert!(first_children.iter().all(|child| dom.doc.exists(*child)));
    mark_dirty(&instance, Some(second));
    dom.shared.trace.borrow_mut().clear();
    instance.flush_managers();

    // The setup for the new render ran against the old nodes first...
    let trace = dom.trace();
    assert!(
        trace.contains(&json!(["unobserve", "code-deletions-1"])),
        "the resize manager released the old column: {trace:?}"
    );
    assert!(
        trace.contains(&json!(["attr.remove", "pre-1", "data-interactive-lines"])),
        "the interaction manager cleaned the old pre up: {trace:?}"
    );
    // ...and only then were they discarded, with their subtree.
    assert!(!dom.doc.exists(first));
    assert!(first_children.iter().all(|child| !dom.doc.exists(*child)));
    assert!(dom.doc.exists(second));
    assert!(instance.state().retired.is_empty());
}

#[test]
fn a_flush_that_sets_nothing_up_discards_nothing() {
    let dom = Rc::new(FakeDom::default());
    let instance = instance_over(&dom);
    let first = dom.build(&pre_tree("1"), None);
    let second = dom.build(&pre_tree("2"), None);
    instance.retire(first);

    // Not dirty.
    instance.flush_managers();
    assert!(dom.doc.exists(first));

    // Dirty, but no `pre` to set up against.
    instance.state().managers_dirty = true;
    instance.flush_managers();
    assert!(dom.doc.exists(first));
    assert!(!instance.state().managers_dirty);
    assert_eq!(instance.state().retired, vec![first]);

    // The next real setup releases it.
    mark_dirty(&instance, Some(second));
    instance.flush_managers();
    assert!(!dom.doc.exists(first));
}
