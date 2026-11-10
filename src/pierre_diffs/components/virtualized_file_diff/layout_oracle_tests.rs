//! Differential tests for S12b-1, the layout model of `VirtualizedFileDiff`
//! (`oracle/s12b_layout.mjs`, golden `oracle/golden/s12b_layout.json.gz`): the
//! published class ran handwritten and seeded random scenarios over a pool of
//! diffs (its layout methods called directly, options through `setOptions`),
//! recording each step's result (or the message it threw) and every derived
//! value afterwards (`height`, `layoutDirty`, whether `renderRange` is set, the
//! whole layout cache). Each scenario is replayed against `DiffLayout`, with the
//! instance's side of the contract (the options, the renderer's expanded hunks,
//! the pending render's diff, the line annotations, the render range flag) held
//! by the replay as the class holds them. Test-only, original to this crate.

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::rc::Rc;

use serde::Deserialize;
use serde_json::Value;
use serde_json::json;

use super::layout::DiffLayoutCheckpoint;
use super::layout::ResetLayoutCacheOptions;
use super::layout::can_hydrate_collapsed_context;
use super::layout::get_expanded_line_count;
use super::layout::has_file_annotations;
use super::layout_test_instance::LayoutInstance;
use super::layout_test_instance::Options;
use crate::components::file_diff::FileDiffHunkSeparators;
use crate::js_collections::PtrEq;
use crate::oracle_testing::assert_json_eq;
use crate::oracle_testing::load_golden;
use crate::types::lines::ExpansionDirections;
use crate::types::lines::HunkData;
use crate::types::options::DiffIndicators;
use crate::types::options::DiffStyle;
use crate::types::options::Overflow;
use crate::types::options::ThemeTypes;
use crate::types::patch::ChangeTypes;
use crate::types::patch::FileDiffMetadata;
use crate::types::view::DiffLineAnnotation;
use crate::utils::compute_virtual_file_metrics::PartialVirtualFileMetrics;

type SeparatorFn = Rc<dyn Fn(&HunkData) -> Option<u8>>;

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
    steps: Vec<Step>,
}

#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "camelCase")]
struct MetricsSpec {
    hunk_line_count: Option<usize>,
    line_height: Option<f64>,
    diff_header_height: Option<f64>,
    hunk_separator_height: Option<f64>,
    spacing: Option<f64>,
    padding_top: Option<f64>,
    padding_bottom: Option<f64>,
}

impl From<MetricsSpec> for PartialVirtualFileMetrics {
    fn from(spec: MetricsSpec) -> Self {
        Self {
            hunk_line_count: spec.hunk_line_count,
            line_height: spec.line_height,
            diff_header_height: spec.diff_header_height,
            hunk_separator_height: spec.hunk_separator_height,
            spacing: spec.spacing,
            padding_top: spec.padding_top,
            padding_bottom: spec.padding_bottom,
        }
    }
}

#[derive(Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
struct Spec {
    diff_style: Option<DiffStyle>,
    collapsed: Option<bool>,
    expand_unchanged: Option<bool>,
    collapsed_context_threshold: Option<usize>,
    disable_file_header: Option<bool>,
    overflow: Option<Overflow>,
    disable_line_numbers: Option<bool>,
    diff_indicators: Option<DiffIndicators>,
    #[serde(rename = "unsafeCSS")]
    unsafe_css: Option<String>,
    theme_type: Option<ThemeTypes>,
    disable_background: Option<bool>,
    hunk_separators: Option<SeparatorSpec>,
    #[serde(default)]
    load_diff_files: bool,
}

#[derive(Deserialize, Clone)]
#[serde(untagged)]
enum SeparatorSpec {
    Name(String),
    Function { r#fn: String },
}

#[derive(Deserialize, Clone, Copy, Default)]
#[serde(rename_all = "camelCase")]
struct ResetSpec {
    force_simple_recompute: Option<bool>,
    include_estimated_heights: Option<bool>,
    reset_render_range: Option<bool>,
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "camelCase", rename_all_fields = "camelCase")]
enum Step {
    LayoutDiff {
        diff: Option<String>,
    },
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
    FileAnnotationHeight {
        height: f64,
    },
    Annotations {
        list: Vec<DiffLineAnnotation<u8>>,
    },
    HasFileAnnotations {
        diff: String,
    },
    RenderRange {
        set: bool,
    },
    ResizeDebugging {
        on: bool,
    },
    SetTotalLines {
        total_lines: usize,
    },
    Compute {
        force: bool,
        diff: Option<String>,
    },
    Checkpoints {
        diff: String,
    },
    ActiveEstimatedHeight {
        diff: Option<String>,
    },
    EnsureEstimated {
        diff: Option<String>,
    },
    Reset {
        options: ResetSpec,
    },
    Invalidate {
        include_estimated: bool,
        reset_render_range: bool,
    },
    LineHeight {
        index: i64,
        meta: bool,
    },
    EstimatedLineHeight {
        meta: bool,
    },
    CheckpointBeforeLine {
        index: i64,
    },
    CheckpointBeforeTop {
        top: f64,
        hunk_line_count: Option<usize>,
    },
    ExpandedLineCount {
        diff: String,
        style: DiffStyle,
    },
    LayoutLineCount {
        diff: String,
        style: DiffStyle,
    },
}

/// The instance around the layout (`LayoutInstance`) plus what builds its options.
struct Replay {
    diffs: HashMap<String, Rc<FileDiffMetadata>>,
    separator_functions: HashMap<String, SeparatorFn>,
    instance: LayoutInstance,
}

impl Replay {
    fn new(golden: &Golden, scenario: &Scenario) -> Self {
        let mut replay = Self {
            diffs: golden
                .diffs
                .iter()
                .map(|(name, diff)| (name.clone(), Rc::new(diff.clone())))
                .collect(),
            separator_functions: HashMap::new(),
            instance: LayoutInstance::new(
                Options::default(),
                scenario.metrics.map(Into::into).unwrap_or_default(),
            ),
        };
        replay.instance.options = replay.build_options(&scenario.options);
        replay.instance.resize_debugging = scenario.resize_debugging;
        replay
    }

    fn build_options(&mut self, spec: &Spec) -> Options {
        let mut options = Options::default();
        options.base.base.theme = None;
        let code = &mut options.base.base;
        code.collapsed = spec.collapsed;
        code.disable_file_header = spec.disable_file_header;
        code.overflow = spec.overflow;
        code.disable_line_numbers = spec.disable_line_numbers;
        code.unsafe_css.clone_from(&spec.unsafe_css);
        code.theme_type = spec.theme_type;
        options.base.diff_style = spec.diff_style;
        options.base.expand_unchanged = spec.expand_unchanged;
        options.base.collapsed_context_threshold = spec.collapsed_context_threshold;
        options.base.diff_indicators = spec.diff_indicators;
        options.base.disable_background = spec.disable_background;
        options.has_diff_file_loader = spec.load_diff_files;
        options.hunk_separators =
            spec.hunk_separators
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
                        let function = self
                            .separator_functions
                            .entry(r#fn.clone())
                            .or_insert_with(|| Rc::new(|_: &HunkData| None));
                        FileDiffHunkSeparators::Custom(PtrEq(Rc::clone(function)))
                    }
                });
        options
    }

    fn step(&mut self, step: &Step) -> Value {
        let diffs = &self.diffs;
        let named = |name: &str| Rc::clone(&diffs[name]);
        let instance = &mut self.instance;
        match step {
            Step::LayoutDiff { diff } => {
                instance.layout_diff = diff.as_deref().map(named);
            }
            Step::Options { spec } => {
                let next = self.build_options(spec);
                self.instance.set_options(next);
            }
            Step::Metrics { metrics, force } => {
                instance.set_metrics(metrics.map(Into::into).unwrap_or_default(), *force);
            }
            Step::Expand {
                hunk_index,
                direction,
                count,
            } => instance.expand_hunk(*hunk_index, *direction, *count),
            Step::Deltas { entries } => {
                let cache = &mut instance.layout.cache;
                for (line_index, delta) in entries {
                    let next = cache.height_deltas.get(line_index).copied().unwrap_or(0.0) + delta;
                    if next == 0.0 {
                        cache.height_deltas.remove(line_index);
                    } else {
                        cache.height_deltas.insert(*line_index, next);
                    }
                    cache.measured_height_delta_total += delta;
                }
            }
            Step::FileAnnotationHeight { height } => {
                return json!(instance.layout.set_file_annotation_height(*height));
            }
            Step::Annotations { list } => instance.annotations.clone_from(list),
            Step::HasFileAnnotations { diff } => {
                return json!(has_file_annotations(&diffs[diff], &instance.annotations));
            }
            Step::RenderRange { set } => instance.render_range_set = *set,
            Step::ResizeDebugging { on } => instance.resize_debugging = *on,
            Step::SetTotalLines { total_lines } => instance.layout.cache.total_lines = *total_lines,
            Step::Compute { force, diff } => {
                let diff = diff.as_deref().map(named);
                instance.with_context(|layout, context| {
                    layout.compute_approximate_size(*force, diff.as_deref(), context);
                });
            }
            Step::Checkpoints { diff } => {
                let diff = named(diff);
                instance.with_context(|layout, context| {
                    layout.approximate_layout_checkpoints(&diff, context);
                });
            }
            Step::ActiveEstimatedHeight { diff } => {
                let diff = diff.as_deref().map(named);
                return json!(instance.with_context(|layout, context| {
                    layout.get_active_estimated_height(diff.as_deref(), context)
                }));
            }
            Step::EnsureEstimated { diff } => {
                let diff = diff.as_deref().map(named);
                instance.with_context(|layout, context| {
                    layout.ensure_estimated_diff_heights(diff.as_deref(), context);
                });
            }
            Step::Reset { options } => instance.reset_layout_cache(ResetLayoutCacheOptions {
                force_simple_recompute: options.force_simple_recompute.unwrap_or(false),
                include_estimated_heights: options.include_estimated_heights.unwrap_or(false),
                reset_render_range: options.reset_render_range.unwrap_or(true),
            }),
            Step::Invalidate {
                include_estimated,
                reset_render_range,
            } => {
                let clear = instance
                    .layout
                    .invalidate_derived_layout_cache(*include_estimated, *reset_render_range);
                instance.apply_render_range_reset(clear);
            }
            Step::LineHeight { index, meta } => {
                return json!(instance.layout.get_line_height(*index, *meta));
            }
            Step::EstimatedLineHeight { meta } => {
                return json!(instance.layout.get_estimated_line_height(*meta));
            }
            Step::CheckpointBeforeLine { index } => {
                return checkpoint_json(
                    instance
                        .layout
                        .get_layout_checkpoint_before_line_index(*index),
                );
            }
            Step::CheckpointBeforeTop {
                top,
                hunk_line_count,
            } => {
                return checkpoint_json(
                    instance
                        .layout
                        .get_layout_checkpoint_before_top(*top, *hunk_line_count),
                );
            }
            Step::ExpandedLineCount { diff, style } => {
                let diff = named(diff);
                return json!(instance.with_context(|_, context| {
                    get_expanded_line_count(&diff, *style, context)
                }));
            }
            Step::LayoutLineCount { diff, style } => {
                let diff = named(diff);
                return json!(instance.with_context(|layout, context| {
                    layout.get_layout_line_count(&diff, *style, context)
                }));
            }
        }
        Value::Null
    }

    fn snapshot(&self) -> Value {
        let layout = &self.instance.layout;
        let cache = &layout.cache;
        let mut height_deltas: Vec<(i64, f64)> = cache
            .height_deltas
            .iter()
            .map(|(line_index, delta)| (*line_index, *delta))
            .collect();
        height_deltas.sort_by_key(|(line_index, _)| *line_index);
        json!({
            "height": layout.height,
            "top": layout.top,
            "layoutDirty": layout.layout_dirty,
            "renderRangeSet": self.instance.render_range_set,
            "heightDeltas": height_deltas,
            "measuredHeightDeltaTotal": cache.measured_height_delta_total,
            "estimatedSplitHeight": cache.estimated_split_height,
            "estimatedUnifiedHeight": cache.estimated_unified_height,
            "checkpoints": cache
                .checkpoints
                .iter()
                .map(|checkpoint| checkpoint_json(Some(*checkpoint)))
                .collect::<Vec<_>>(),
            "totalLines": cache.total_lines,
            "fileAnnotationHeight": cache.file_annotation_height,
        })
    }
}

fn checkpoint_json(checkpoint: Option<DiffLayoutCheckpoint>) -> Value {
    checkpoint.map_or(Value::Null, |checkpoint| {
        json!({
            "renderedLineIndex": checkpoint.rendered_line_index,
            "lineIndex": checkpoint.line_index,
            "top": checkpoint.top,
        })
    })
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(ToString::to_string))
        .expect("panic payload is text")
}

#[test]
fn layout_model_matches_the_published_class() {
    let golden: Golden = load_golden("s12b_layout.json");
    assert!(
        golden.cases.len() > 300,
        "the golden has the whole scenario set"
    );
    let mut compared_steps = 0;
    let mut panics = 0;
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
                Err(payload) => {
                    panics += 1;
                    json!({ "throws": panic_message(&*payload) })
                }
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
    assert!(compared_steps > 6_000, "compared {compared_steps} steps");
    assert!(panics > 0, "the throw paths are exercised");
}

#[test]
fn hydratable_collapsed_context_needs_a_loader_and_a_partial_change() {
    let golden: Golden = load_golden("s12b_layout.json");
    let partial = &golden.diffs["partial0"];
    assert!(partial.is_partial);
    assert!(can_hydrate_collapsed_context(partial, true));
    assert!(!can_hydrate_collapsed_context(partial, false));
    let full = &golden.diffs["multi1"];
    assert!(!can_hydrate_collapsed_context(full, true));
    let renamed_partial = &golden.diffs["partialRename"];
    assert_eq!(renamed_partial.change_type, ChangeTypes::RenameChanged);
    assert!(can_hydrate_collapsed_context(renamed_partial, true));
    let new_partial = &golden.diffs["huge8New"];
    assert!(new_partial.is_partial && !can_hydrate_collapsed_context(new_partial, true));
}
