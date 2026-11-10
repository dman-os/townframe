//! Differential tests for S12b-2, the window and measurement half of
//! `VirtualizedFileDiff` (`oracle/s12b_window.mjs`, golden
//! `oracle/golden/s12b_window.json.gz`): the published class ran handwritten and
//! seeded random scenarios over a pool of diffs and a declared DOM (heights, the
//! container's top), recording each step's result and the whole observable state
//! afterwards. Each scenario is replayed against `VirtualizedFileDiff` over
//! `FakeDom` (its declared heights and scroll geometry), with the pieces S12b-3
//! owns composed by the replay as the published class composes them: the layout
//! half of `setOptions`, the simple-mode `expandHunk`, and `render`/`rerender`
//! as recorded stubs. Test-only, original to this crate.

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::rc::Rc;

use serde::Deserialize;
use serde_json::Value;
use serde_json::json;

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
use crate::components::file_diff::FileDiff;
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
use crate::types::layout::RenderRange;
use crate::types::layout::RenderWindow;
use crate::types::lines::ExpansionDirections;
use crate::types::patch::FileDiffMetadata;
use crate::types::view::DiffLineAnnotation;
use crate::types::view::SelectionSide;
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
    lists: HashMap<String, Vec<DiffLineAnnotation<u8>>>,
    steps: Vec<Step>,
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
    OnRender {
        dirty: bool,
        render_result: bool,
    },
    Visibility {
        visible: bool,
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
    instance: Instance,
    diffs: HashMap<String, Rc<FileDiffMetadata>>,
    lists: HashMap<String, LineAnnotations<u8>>,
    separator_functions: HashMap<String, SeparatorFn>,
    container: Option<NodeKey>,
    groups: HashMap<&'static str, GroupNodes>,
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
        let file_diff = FileDiff::new(
            Rc::clone(&dom),
            ResizeObserverHub::new(Rc::clone(&dom)),
            UniversalRenderingManager::new(Rc::clone(&dom)),
            Rc::new(PlainTextHighlighter),
            options,
            None,
        );
        let virtualizer = Virtualizer::new(
            Rc::clone(&dom),
            UniversalRenderingManager::new(Rc::clone(&dom)),
            VirtualizerConfig {
                resize_debugging: scenario.resize_debugging,
                ..VirtualizerConfig::default()
            },
        );
        let instance = VirtualizedFileDiff::new(
            Rc::clone(&dom),
            file_diff,
            virtualizer,
            scenario.metrics.map(Into::into).unwrap_or_default(),
        );
        Self {
            dom,
            instance,
            diffs: golden
                .diffs
                .iter()
                .map(|(name, diff)| (name.clone(), Rc::new(diff.clone())))
                .collect(),
            lists: scenario
                .lists
                .iter()
                .map(|(name, list)| (name.clone(), Rc::new(list.clone())))
                .collect(),
            separator_functions,
            container: None,
            groups: HashMap::new(),
        }
    }

    fn diff_name(&self, diff: Option<&Rc<FileDiffMetadata>>) -> Value {
        match diff {
            None => Value::Null,
            Some(diff) => self
                .diffs
                .iter()
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
            } => json!(self.instance.update_code_view_layout(
                &self.diffs[diff],
                *top,
                self.list(annotations.as_ref()).as_ref()
            )),
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
            other => self.query_step(other),
        }
    }

    /// The steps that read: reconcile, the window, positions, `onRender`,
    /// `setVisibility`, `syncVirtualizedTop`.
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
            Step::OnRender {
                dirty,
                render_result,
            } => {
                let mut calls = 0;
                let result = self.instance.on_render(*dirty, || {
                    calls += 1;
                    *render_result
                });
                json!({ "result": result, "renderCalls": calls })
            }
            Step::Visibility { visible } => {
                let mut calls = 0;
                self.instance.set_visibility(*visible, || calls += 1);
                json!({ "rerenderCalls": calls })
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
        json!({
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
        })
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
