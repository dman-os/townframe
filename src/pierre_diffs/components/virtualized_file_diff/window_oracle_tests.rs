//! Differential tests for S12b-2, the window and measurement half of
//! `VirtualizedFileDiff` (`oracle/s12b_window.mjs`, golden
//! `oracle/golden/s12b_window.json.gz`): the published class ran handwritten and
//! seeded random scenarios over a pool of diffs and a declared DOM (heights, the
//! container's top), recording each step's result and the whole observable state
//! afterwards. Each scenario is replayed against `VirtualizedFileDiff` over
//! `FakeDom` (its declared heights and scroll geometry), with the pieces S12b-3
//! owns composed by the replay as the published class composes them: the layout
//! half of `setOptions`, the simple-mode `expandHunk`, and `render`/`rerender`
//! as recorded stubs. The C1 scenarios (`advanced` set) run the instance over a
//! `StubCodeView` (advanced mode, or the simple mode for the ones that compare
//! the two) and replay the steps C1 added (`layout`'s `reset`, `expandHunkApi`,
//! `filesLoaded`, `isLineRenderable`, `stickySpecs`, `numericAnchor`,
//! `setThemeType`, `rerender`, `cleanUp`, `codeView`). Upstream's
//! `Object.assign(fileDiff, nextDiff)` (hydration in place) is the replay's
//! `rebind`: a name denotes the object, so when the instance replaces a diff
//! `Rc` by its hydrated one the name follows. Test-only, original to this crate.

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::rc::Rc;

use serde::Deserialize;
use serde_json::Value;
use serde_json::json;

use super::advanced::AdvancedVirtualizer;
use super::advanced::VirtualizerKind;
use super::advanced_upstream_tests::StubCodeView;
use super::instance::VirtualizedFileDiff;
use super::layout::ResetLayoutCacheOptions;
use super::layout::has_diff_estimate_option_changed;
use super::layout::has_diff_layout_option_changed;
use super::layout_oracle_tests::MetricsSpec;
use super::layout_oracle_tests::SeparatorFn;
use super::layout_oracle_tests::Spec;
use super::layout_oracle_tests::options_from_spec;
use super::layout_oracle_tests::panic_message;
use crate::components::file_diff::CodeGroups;
use crate::components::file_diff::LineAnnotations;
use crate::components::virtualizer::Virtualizer;
use crate::components::virtualizer::VirtualizerConfig;
use crate::document::NodeKey;
use crate::fake_interaction_dom::FakeDom;
use crate::highlighter_types::PlainTextHighlighter;
use crate::managers::resize_manager::ResizeObserverHub;
use crate::managers::universal_rendering_manager::UniversalRenderingManager;
use crate::oracle_testing::assert_json_eq;
use crate::oracle_testing::load_golden;
use crate::types::layout::PendingCodeViewLayoutReset;
use crate::types::layout::RenderRange;
use crate::types::layout::RenderWindow;
use crate::types::layout::VirtualWindowSpecs;
use crate::types::lines::ExpansionDirections;
use crate::types::options::ThemeTypes;
use crate::types::patch::FileContents;
use crate::types::patch::FileDiffLoadedFiles;
use crate::types::patch::FileDiffMetadata;
use crate::types::view::DiffLineAnnotation;
use crate::types::view::SelectionSide;
use crate::utils::compute_virtual_file_metrics::compute_virtual_file_metrics;
use crate::utils::predicates::are_options_equal::are_options_equal;

type Instance = VirtualizedFileDiff<FakeDom, u8, u8>;

#[derive(Deserialize)]
struct Golden {
    diffs: HashMap<String, FileDiffMetadata>,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    scenario: Scenario,
    results: Vec<Value>,
    trace: Vec<Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Scenario {
    name: String,
    metrics: Option<MetricsSpec>,
    options: Spec,
    #[serde(default)]
    resize_debugging: bool,
    /// C1 scenarios only: `Some(true)` runs over a stub CodeView.
    advanced: Option<bool>,
    #[serde(default)]
    own_diffs: HashMap<String, FileDiffMetadata>,
    #[serde(default)]
    loaded: HashMap<String, LoadedSpec>,
    lists: HashMap<String, Vec<DiffLineAnnotation<u8>>>,
    steps: Vec<Step>,
}

/// What `loadDiffFiles` resolves to, two-sided.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LoadedSpec {
    old_file: FileContents,
    new_file: FileContents,
}

/// `PendingCodeViewLayoutReset` (`resetFileLayoutCache` is VirtualizedFile's).
#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "camelCase")]
struct ResetSpec {
    metrics: Option<MetricsSpec>,
    reset_diff_layout_cache: bool,
    include_estimated_diff_heights: bool,
}

impl From<ResetSpec> for PendingCodeViewLayoutReset {
    fn from(spec: ResetSpec) -> Self {
        Self {
            metrics: spec
                .metrics
                .map(|metrics| compute_virtual_file_metrics(metrics.into())),
            reset_file_layout_cache: false,
            reset_diff_layout_cache: spec.reset_diff_layout_cache,
            include_estimated_diff_heights: spec.include_estimated_diff_heights,
        }
    }
}

#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "camelCase")]
struct WindowSpec {
    top: f64,
    bottom: f64,
}

#[derive(Deserialize, Clone, Copy, Default)]
#[serde(rename_all = "camelCase")]
struct CodeViewState {
    window_top: Option<f64>,
    window_bottom: Option<f64>,
    local_top: Option<f64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Annotation {
    height: f64,
    key: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct NoNewline {
    height: f64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RowSpec {
    line_index: Option<String>,
    height: f64,
    annotation: Option<Annotation>,
    no_newline: Option<NoNewline>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GroupSpec {
    rows: Vec<RowSpec>,
    file_annotation: Option<Annotation>,
}

#[derive(Deserialize, Default)]
struct GroupSpecs {
    deletions: Option<GroupSpec>,
    additions: Option<GroupSpec>,
    unified: Option<GroupSpec>,
}

#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum Group {
    Deletions,
    Additions,
    Unified,
}

#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "camelCase")]
enum HeightTarget {
    Line,
    Extra,
    FileAnnotation,
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "camelCase", rename_all_fields = "camelCase")]
enum Step {
    Options {
        spec: Spec,
    },
    Metrics {
        metrics: Option<MetricsSpec>,
        force: bool,
    },
    Expand {
        hunk_index: usize,
        direction: ExpansionDirections,
        count: Option<usize>,
    },
    Deltas {
        entries: Vec<(i64, f64)>,
    },
    RenderRange {
        set: bool,
        range: Option<RenderRange>,
    },
    Layout {
        diff: String,
        top: f64,
        annotations: Option<String>,
        reset: Option<ResetSpec>,
    },
    ExpandHunkApi {
        hunk_index: i64,
        direction: ExpansionDirections,
        count: Option<f64>,
    },
    FilesLoaded {
        diff: String,
        loaded: String,
    },
    IsLineRenderable {
        line: usize,
    },
    StickySpecs {
        window: Option<WindowSpec>,
    },
    NumericAnchor {
        top: f64,
    },
    SetThemeType {
        theme_type: ThemeTypes,
    },
    Rerender,
    CleanUp {
        recycle: bool,
    },
    CodeView {
        state: CodeViewState,
    },
    PendingRender {
        diff: String,
        annotations: Option<String>,
    },
    SyncLineAnnotations {
        annotations: Option<String>,
    },
    Install {
        diff: String,
        container: bool,
        container_top: f64,
        placeholder: bool,
        groups: GroupSpecs,
    },
    Height {
        group: Group,
        row: usize,
        target: HeightTarget,
        height: f64,
    },
    ContainerTop {
        top: f64,
    },
    Reconcile,
    Window {
        diff: String,
        file_top: f64,
        top: f64,
        bottom: f64,
    },
    LinePosition {
        line: usize,
        side: SelectionSide,
    },
    SyncTop,
}

/// The declared DOM of one installed group: its rows and the file annotation.
struct GroupNodes {
    rows: Vec<(NodeKey, Option<NodeKey>)>,
    file_annotation: Option<NodeKey>,
}

struct Replay {
    dom: Rc<FakeDom>,
    instance: Rc<Instance>,
    diffs: HashMap<String, Rc<FileDiffMetadata>>,
    /// The diffs a name denoted before it was rebound: upstream's object is
    /// the same one after the in-place hydration, and a pending file load still
    /// holds the old `Rc`.
    retired: Vec<(String, Rc<FileDiffMetadata>)>,
    lists: HashMap<String, LineAnnotations<u8>>,
    separator_functions: HashMap<String, SeparatorFn>,
    container: Option<NodeKey>,
    groups: HashMap<&'static str, GroupNodes>,
    /// The stub CodeView of an advanced scenario.
    code_view: Option<Rc<StubCodeView>>,
    advanced: Option<bool>,
    loaded: HashMap<String, FileDiffLoadedFiles>,
}

fn group_key(group: Group) -> &'static str {
    match group {
        Group::Deletions => "deletions",
        Group::Additions => "additions",
        Group::Unified => "unified",
    }
}

impl Replay {
    fn new(golden: &Golden, scenario: &Scenario) -> Self {
        let dom = Rc::new(FakeDom::default());
        let mut separator_functions = HashMap::new();
        let options = options_from_spec(&scenario.options, &mut separator_functions);
        let code_view = (scenario.advanced == Some(true)).then(StubCodeView::new);
        let virtualizer = match &code_view {
            Some(code_view) => {
                let weak = Rc::downgrade(code_view);
                let weak: std::rc::Weak<dyn AdvancedVirtualizer> = weak;
                VirtualizerKind::Advanced(weak)
            }
            None => VirtualizerKind::Simple(Virtualizer::new(
                Rc::clone(&dom),
                UniversalRenderingManager::new(Rc::clone(&dom)),
                VirtualizerConfig {
                    resize_debugging: scenario.resize_debugging,
                    ..VirtualizerConfig::default()
                },
            )),
        };
        let instance = VirtualizedFileDiff::new(
            Rc::clone(&dom),
            ResizeObserverHub::new(Rc::clone(&dom)),
            UniversalRenderingManager::new(Rc::clone(&dom)),
            Rc::new(PlainTextHighlighter),
            options,
            virtualizer,
            scenario.metrics.map(Into::into).unwrap_or_default(),
            None,
        );
        Self {
            dom,
            instance,
            diffs: golden
                .diffs
                .iter()
                .chain(scenario.own_diffs.iter())
                .map(|(name, diff)| (name.clone(), Rc::new(diff.clone())))
                .collect(),
            retired: Vec::new(),
            lists: scenario
                .lists
                .iter()
                .map(|(name, list)| (name.clone(), Rc::new(list.clone())))
                .collect(),
            separator_functions,
            container: None,
            groups: HashMap::new(),
            code_view,
            advanced: scenario.advanced,
            loaded: scenario
                .loaded
                .iter()
                .map(|(name, loaded)| {
                    (
                        name.clone(),
                        FileDiffLoadedFiles::Changed {
                            old_file: loaded.old_file.clone(),
                            new_file: loaded.new_file.clone(),
                        },
                    )
                })
                .collect(),
        }
    }

    /// A name denotes an object upstream, which hydrates in place; here the
    /// hydrated `Rc` takes the name of the one it replaced.
    fn rebind(&mut self, name: &str, replacement: &Rc<FileDiffMetadata>) {
        if let Some(previous) = self.diffs.insert(name.to_owned(), Rc::clone(replacement)) {
            self.retired.push((name.to_owned(), previous));
        }
    }

    fn diff_name(&self, diff: Option<&Rc<FileDiffMetadata>>) -> Value {
        match diff {
            None => Value::Null,
            Some(diff) => self
                .diffs
                .iter()
                .chain(self.retired.iter().map(|(name, retired)| (name, retired)))
                .find(|(_, candidate)| Rc::ptr_eq(candidate, diff))
                .map_or_else(|| json!("?"), |(name, _)| json!(name)),
        }
    }

    fn list_name(&self, list: &LineAnnotations<u8>) -> Value {
        self.lists
            .iter()
            .find(|(_, candidate)| Rc::ptr_eq(candidate, list))
            .map_or_else(
                || {
                    if list.is_empty() {
                        json!("empty")
                    } else {
                        json!("?")
                    }
                },
                |(name, _)| json!(name),
            )
    }

    fn list(&self, name: Option<&String>) -> Option<LineAnnotations<u8>> {
        name.map(|name| Rc::clone(&self.lists[name]))
    }

    fn set_height(&self, node: NodeKey, height: f64) {
        self.dom.heights.borrow_mut().insert(node, height);
    }

    /// `FakeEl`s as the oracle builds them: `group[gutter, content[rows...]]`.
    fn build_group(&self, spec: &GroupSpec) -> (NodeKey, GroupNodes) {
        let doc = &self.dom.doc;
        let group = doc.create_element("div");
        let gutter = doc.create_element("div");
        let content = doc.create_element("div");
        doc.append_child(group, gutter);
        doc.append_child(group, content);
        let mut file_annotation = None;
        if let Some(annotation) = &spec.file_annotation {
            let element = doc.create_element("div");
            doc.set_attribute(
                element,
                "data-line-annotation",
                annotation.key.as_deref().unwrap_or("-1,-1"),
            );
            self.set_height(element, annotation.height);
            doc.append_child(content, element);
            file_annotation = Some(element);
        }
        let mut rows = Vec::new();
        for row in &spec.rows {
            let line = doc.create_element("div");
            if let Some(line_index) = &row.line_index {
                doc.set_attribute(line, "data-line-index", line_index);
            }
            self.set_height(line, row.height);
            doc.append_child(content, line);
            let mut extra = None;
            if row.annotation.is_some() || row.no_newline.is_some() {
                let element = doc.create_element("div");
                if let Some(annotation) = &row.annotation {
                    doc.set_attribute(
                        element,
                        "data-line-annotation",
                        annotation.key.as_deref().unwrap_or("0,0"),
                    );
                    self.set_height(element, annotation.height);
                }
                if let Some(no_newline) = &row.no_newline {
                    doc.set_attribute(element, "data-no-newline", "");
                    self.set_height(element, no_newline.height);
                }
                doc.append_child(content, element);
                extra = Some(element);
            }
            rows.push((line, extra));
        }
        (
            group,
            GroupNodes {
                rows,
                file_annotation,
            },
        )
    }

    fn install(
        &mut self,
        diff: &str,
        container: bool,
        container_top: f64,
        placeholder: bool,
        specs: &GroupSpecs,
    ) {
        let container = container.then(|| {
            let node = self.dom.doc.create_element("div");
            self.dom
                .world
                .borrow_mut()
                .doc_tops
                .insert(node, container_top);
            node
        });
        self.container = container;
        let place_holder = placeholder.then(|| self.dom.doc.create_element("div"));
        self.groups.clear();
        let mut code_groups = CodeGroups::default();
        for (key, spec, slot) in [
            ("deletions", &specs.deletions, &mut code_groups.deletions),
            ("additions", &specs.additions, &mut code_groups.additions),
            ("unified", &specs.unified, &mut code_groups.unified),
        ] {
            if let Some(spec) = spec {
                let (group, nodes) = self.build_group(spec);
                *slot = Some(group);
                self.groups.insert(key, nodes);
            }
        }
        self.instance
            .file_diff()
            .virtualized()
            .install_rendered_for_test(&self.diffs[diff], container, code_groups, place_holder);
    }

    /// `VirtualizedFileDiff.setOptions` without the `instanceChanged` call.
    fn set_options(&mut self, spec: &Spec) {
        let next = options_from_spec(spec, &mut self.separator_functions);
        if self.advanced == Some(true) {
            // `VirtualizedFileDiff.setOptions`: it throws under CodeView.
            self.instance.set_options(Some(next));
            return;
        }
        let file_diff = self.instance.file_diff();
        let previous = file_diff.options().clone();
        let options_changed = !are_options_equal(Some(&previous), Some(&next));
        let layout_changed = options_changed && has_diff_layout_option_changed(&previous, &next);
        let estimate_changed = has_diff_estimate_option_changed(&previous, &next);
        file_diff.set_options(Some(next));
        if layout_changed {
            self.instance.reset_layout_cache(ResetLayoutCacheOptions {
                force_simple_recompute: true,
                include_estimated_heights: estimate_changed,
                reset_render_range: true,
            });
        }
        if options_changed {
            self.instance
                .file_diff()
                .virtualized()
                .force_render_override();
        }
    }

    fn step(&mut self, step: &Step) -> Value {
        match step {
            Step::Options { spec } => {
                self.set_options(spec);
                Value::Null
            }
            Step::Metrics { metrics, force } => {
                self.instance
                    .set_metrics(metrics.map(Into::into).unwrap_or_default(), *force);
                Value::Null
            }
            Step::Expand {
                hunk_index,
                direction,
                count,
            } => {
                self.instance
                    .file_diff()
                    .virtualized()
                    .expand_renderer_hunk(*hunk_index, *direction, *count);
                self.instance.reset_layout_cache(ResetLayoutCacheOptions {
                    include_estimated_heights: true,
                    ..ResetLayoutCacheOptions::default()
                });
                self.instance.compute_approximate_size(false, None);
                Value::Null
            }
            Step::Deltas { entries } => {
                self.instance.with_layout(None, |layout, _| {
                    for (line_index, delta) in entries {
                        let next = layout
                            .cache
                            .height_deltas
                            .get(line_index)
                            .copied()
                            .unwrap_or(0.0)
                            + delta;
                        if next == 0.0 {
                            layout.cache.height_deltas.remove(line_index);
                        } else {
                            layout.cache.height_deltas.insert(*line_index, next);
                        }
                        layout.cache.measured_height_delta_total += delta;
                    }
                });
                Value::Null
            }
            Step::RenderRange { set, range } => {
                let range = if *set { *range } else { None };
                self.instance
                    .file_diff()
                    .virtualized()
                    .set_render_range(range);
                Value::Null
            }
            Step::Layout {
                diff,
                top,
                annotations,
                reset,
            } => {
                let update = self.instance.update_code_view_layout(
                    &self.diffs[diff],
                    *top,
                    reset.map(Into::into),
                    self.list(annotations.as_ref()).as_ref(),
                );
                if let Some(hydrated) = &update.hydrated_diff {
                    self.rebind(diff, hydrated);
                }
                json!(update.height)
            }
            Step::PendingRender { diff, annotations } => {
                let update = self.instance.update_pending_render(
                    &self.diffs[diff],
                    self.list(annotations.as_ref()).as_ref(),
                );
                json!({
                    "pendingRenderDiff": self.diff_name(Some(&update.pending_render_diff)),
                    "layoutDiffChanged": update.layout_diff_changed,
                    "renderedDiffChanged": update.rendered_diff_changed,
                    "annotationsChanged": update.annotations_changed,
                })
            }
            Step::SyncLineAnnotations { annotations } => {
                json!(
                    self.instance
                        .sync_line_annotations(self.list(annotations.as_ref()).as_ref())
                )
            }
            Step::Install {
                diff,
                container,
                container_top,
                placeholder,
                groups,
            } => {
                self.install(diff, *container, *container_top, *placeholder, groups);
                Value::Null
            }
            Step::Height {
                group,
                row,
                target,
                height,
            } => {
                let nodes = &self.groups[group_key(*group)];
                let node = match target {
                    HeightTarget::Line => nodes.rows[*row].0,
                    HeightTarget::Extra => nodes.rows[*row].1.expect("the row has an extra"),
                    HeightTarget::FileAnnotation => nodes
                        .file_annotation
                        .expect("the group has a file annotation"),
                };
                self.set_height(node, *height);
                Value::Null
            }
            Step::ContainerTop { top } => {
                let container = self.container.expect("an installed container");
                self.dom.world.borrow_mut().doc_tops.insert(container, *top);
                Value::Null
            }
            other => self.advanced_step(other),
        }
    }

    /// The steps C1 added.
    fn advanced_step(&mut self, step: &Step) -> Value {
        match step {
            Step::ExpandHunkApi {
                hunk_index,
                direction,
                count,
            } => {
                self.instance.expand_hunk(*hunk_index, *direction, *count);
                Value::Null
            }
            Step::FilesLoaded { diff, loaded } => {
                // `loadFilesForDiff`: the files arrive for a diff that is still the input.
                let expected = Rc::clone(&self.diffs[diff]);
                let is_input = self
                    .instance
                    .file_diff()
                    .get_input_diff()
                    .is_some_and(|input| Rc::ptr_eq(&input, &expected));
                if is_input {
                    self.instance
                        .handle_files_loaded(&expected, &self.loaded[loaded]);
                    let input = self
                        .instance
                        .file_diff()
                        .get_input_diff()
                        .expect("an input diff");
                    if !Rc::ptr_eq(&input, &expected) {
                        self.rebind(diff, &input);
                    }
                }
                Value::Null
            }
            Step::IsLineRenderable { line } => json!(self.instance.is_line_renderable(*line)),
            Step::StickySpecs { window } => self
                .instance
                .get_advanced_sticky_specs(window.map(|window| RenderWindow {
                    top: window.top,
                    bottom: window.bottom,
                }))
                .map_or(
                    Value::Null,
                    |specs| json!({ "topOffset": specs.top_offset, "height": specs.height }),
                ),
            Step::NumericAnchor { top } => self
                .instance
                .get_numeric_scroll_anchor(*top)
                .map_or(Value::Null, |anchor| {
                    serde_json::to_value(anchor).expect("an anchor")
                }),
            Step::SetThemeType { theme_type } => {
                self.instance.set_theme_type(*theme_type);
                Value::Null
            }
            Step::Rerender => {
                self.instance.rerender();
                Value::Null
            }
            Step::CleanUp { recycle } => {
                self.instance.clean_up(*recycle);
                Value::Null
            }
            Step::CodeView { state } => {
                let code_view = self.code_view.as_ref().expect("a stub CodeView");
                let window = code_view.window.get();
                code_view.window.set(VirtualWindowSpecs {
                    top: state.window_top.unwrap_or(window.top),
                    bottom: state.window_bottom.unwrap_or(window.bottom),
                });
                if let Some(local_top) = state.local_top {
                    code_view.local_top.set(local_top);
                }
                Value::Null
            }
            other => self.query_step(other),
        }
    }

    /// The steps that read: reconcile, the window, positions,
    /// `syncVirtualizedTop`.
    fn query_step(&mut self, step: &Step) -> Value {
        match step {
            Step::Reconcile => json!(self.instance.reconcile_heights()),
            Step::Window {
                diff,
                file_top,
                top,
                bottom,
            } => {
                let range = self.instance.compute_render_range_from_window(
                    &self.diffs[diff],
                    *file_top,
                    RenderWindow {
                        top: *top,
                        bottom: *bottom,
                    },
                );
                json!({
                    "startingLine": range.starting_line,
                    "totalLines": range.total_lines,
                    "bufferBefore": range.buffer_before,
                    "bufferAfter": range.buffer_after,
                })
            }
            Step::LinePosition { line, side } => {
                self.instance.get_line_position(*line, *side).map_or(
                    Value::Null,
                    |position| json!({ "top": position.top, "height": position.height }),
                )
            }
            Step::SyncTop => {
                self.instance.sync_virtualized_top();
                Value::Null
            }
            _ => unreachable!("a mutating step reached query_step"),
        }
    }

    fn snapshot(&self) -> Value {
        let file_diff = self.instance.file_diff();
        let layout = self.instance.layout();
        let cache = &layout.cache;
        let mut height_deltas: Vec<(i64, f64)> = cache
            .height_deltas
            .iter()
            .map(|(line_index, delta)| (*line_index, *delta))
            .collect();
        height_deltas.sort_by_key(|(line_index, _)| *line_index);
        let render_range = file_diff.virtualized().render_range().map(|range| {
            json!({
                "startingLine": range.starting_line,
                "totalLines": range.total_lines,
                "bufferBefore": range.buffer_before,
                "bufferAfter": range.buffer_after,
            })
        });
        let pending_render = self.instance.pending_render_diffs().map(|(working, diff)| {
            json!({
                "workingDiff": self.diff_name(Some(&working)),
                "diff": self.diff_name(Some(&diff)),
            })
        });
        let mut snapshot = json!({
            "height": layout.height,
            "top": layout.top,
            "layoutDirty": layout.layout_dirty,
            "renderRange": render_range,
            "isVisible": self.instance.is_visible(),
            "forceRenderOverride": file_diff.virtualized().is_force_render_override(),
            "pendingRender": pending_render,
            "inputDiff": self.diff_name(file_diff.get_input_diff().as_ref()),
            "renderedDiff": self.diff_name(file_diff.get_rendered_diff().as_ref()),
            "annotations": self.list_name(&file_diff.get_latest_annotations()),
            "heightDeltas": height_deltas,
            "measuredHeightDeltaTotal": cache.measured_height_delta_total,
            "estimatedSplitHeight": cache.estimated_split_height,
            "estimatedUnifiedHeight": cache.estimated_unified_height,
            "checkpoints": cache
                .checkpoints
                .iter()
                .map(|checkpoint| json!({
                    "renderedLineIndex": checkpoint.rendered_line_index,
                    "lineIndex": checkpoint.line_index,
                    "top": checkpoint.top,
                }))
                .collect::<Vec<_>>(),
            "totalLines": cache.total_lines,
            "fileAnnotationHeight": cache.file_annotation_height,
        });
        if self.advanced.is_some() {
            let calls = self
                .code_view
                .as_ref()
                .map(|code_view| code_view.instance_changed_calls.borrow().clone())
                .unwrap_or_default();
            snapshot["advanced"] = json!({
                "instanceChanged": calls,
                "pendingExpansions": self.staged_expansions(),
                "pendingHydratedDiff": self
                    .instance
                    .staged_hydration_expected()
                    .map_or(Value::Null, |diff| self.diff_name(Some(&diff))),
                "currentCollapsed": self.instance.current_collapsed(),
                "pendingFile": file_diff
                    .loading()
                    .pending_files()
                    .map_or(Value::Null, |diff| self.diff_name(Some(&diff))),
                "isSetup": self.instance.is_setup(),
                "isVisible": self.instance.is_visible(),
            });
        }
        snapshot
    }

    /// `pendingExpansions` as the oracle records them (`null` when there are none).
    fn staged_expansions(&self) -> Value {
        let staged = self.instance.staged_expansions();
        if staged.is_empty() {
            return Value::Null;
        }
        json!(
            staged
                .iter()
                .map(|expansion| json!([
                    expansion.hunk_index,
                    expansion.direction,
                    expansion.expansion_line_count_override
                ]))
                .collect::<Vec<_>>()
        )
    }
}

#[test]
fn window_and_measurement_match_the_published_class() {
    let golden: Golden = load_golden("s12b_window.json");
    assert!(
        golden.cases.len() > 300,
        "the golden has the whole scenario set"
    );
    let mut compared_steps = 0;
    for case in &golden.cases {
        let scenario = &case.scenario;
        let mut replay = Replay::new(&golden, scenario);
        assert_json_eq(
            &format!("{}: initial state", scenario.name),
            &case.trace[0],
            &replay.snapshot(),
        );
        for (index, step) in scenario.steps.iter().enumerate() {
            let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| replay.step(step)));
            let result = match outcome {
                Ok(value) => json!({ "value": value }),
                Err(payload) => json!({ "throws": panic_message(&*payload) }),
            };
            let context = format!("{} step {index}", scenario.name);
            assert_json_eq(&format!("{context}: result"), &case.results[index], &result);
            assert_json_eq(
                &format!("{context}: state"),
                &case.trace[index + 1],
                &replay.snapshot(),
            );
            compared_steps += 1;
        }
    }
    assert!(compared_steps > 10_000, "compared {compared_steps} steps");
}
