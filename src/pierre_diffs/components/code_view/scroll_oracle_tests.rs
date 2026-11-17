//! Differential test for C3, the scroll model of `CodeView`
//! (`oracle/s12d_code_view_scroll.mjs`, golden
//! `oracle/golden/s12d_code_view_scroll.json.gz`): the published class, never
//! set up, ran scenarios of paged scroll math, scroll targets, the spring and
//! scroll anchors over a root stand-in, recording each call's value and, after
//! the calls that change it, the model's state. Each scenario is replayed
//! against `CodeView` over `FakeDom`, whose `world` answers the root's
//! `scrollTop` and whose `heights` its `getBoundingClientRect().height`. The
//! spring scenarios (`approx`) compare to a tolerance: `Math.exp` and
//! `f64::exp` need not agree to the last bit. Test-only, original to this
//! crate.

use std::cell::Cell;
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::rc::Rc;

use serde::Deserialize;
use serde_json::Map;
use serde_json::Value;
use serde_json::json;

use super::CodeView;
use super::options::CodeViewOptions;
use super::scroll::AlignedScrollPosition;
use super::scroll::PagedScrollPosition;
use super::state::ItemAnchor;
use super::state::ItemRef;
use super::state::LineAnchor;
use super::state::PendingAlign;
use super::state::PendingScrollTarget;
use super::state::ScrollAnchor;
use super::state::ScrollToAnimation;
use crate::components::virtualized_file_diff::layout_oracle_tests::MetricsSpec;
use crate::components::virtualized_file_diff::layout_oracle_tests::Spec;
use crate::components::virtualized_file_diff::layout_oracle_tests::options_from_spec;
use crate::components::virtualized_file_diff::layout_oracle_tests::panic_message;
use crate::document::NodeKey;
use crate::fake_interaction_dom::FakeDom;
use crate::highlighter_types::PlainTextHighlighter;
use crate::managers::resize_manager::ResizeObserverHub;
use crate::managers::universal_rendering_manager::UniversalRenderingManager;
use crate::oracle_testing::assert_json_eq;
use crate::oracle_testing::load_golden;
use crate::types::layout::CodeViewLayout;
use crate::types::layout::SmoothScrollSettings;
use crate::types::patch::FileDiffMetadata;
use crate::types::view::CodeViewDiffItem;
use crate::types::view::CodeViewItem;
use crate::types::view::CodeViewScrollBehavior;
use crate::types::view::CodeViewScrollTarget;
use crate::types::view::DiffLineAnnotation;
use crate::types::view::SelectionSide;
use crate::utils::compute_virtual_file_metrics::PartialVirtualFileMetrics;

type View = CodeView<FakeDom, u8, u8>;

#[derive(Deserialize)]
struct Golden {
    diffs: HashMap<String, FileDiffMetadata>,
    metrics: HashMap<String, MetricsSpec>,
    layouts: HashMap<String, CodeViewLayout>,
    lists: HashMap<String, Vec<DiffLineAnnotation<u8>>>,
    smooth: HashMap<String, SmoothScrollSettings>,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    scenario: Scenario,
    config: Value,
    results: Vec<Value>,
    trace: Vec<Value>,
}

#[derive(Deserialize)]
struct Scenario {
    name: String,
    #[serde(default)]
    env: Env,
    options: OptionsSpec,
    #[serde(default)]
    approx: bool,
    steps: Vec<Step>,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct Env {
    user_agent: Option<String>,
    dpr: Option<f64>,
    reduced_motion: Option<bool>,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct OptionsSpec {
    #[serde(default)]
    diff: Spec,
    sticky_headers: Option<bool>,
    item_metrics: Option<String>,
    layout: Option<String>,
    smooth_scroll_settings: Option<String>,
}

#[derive(Deserialize)]
struct ItemSpec {
    id: String,
    diff: String,
    annotations: Option<String>,
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "camelCase")]
enum Step {
    SetItems {
        items: Vec<ItemSpec>,
    },
    Recompute,
    SetOptions {
        options: OptionsSpec,
    },
    #[serde(rename_all = "camelCase")]
    SetRoot {
        scroll_top: f64,
        height: f64,
    },
    #[serde(rename_all = "camelCase")]
    WriteRoot {
        scroll_top: Option<f64>,
        height: Option<f64>,
    },
    SetState {
        state: Map<String, Value>,
    },
    Call {
        #[serde(rename = "fn")]
        function: String,
        #[serde(default)]
        args: Vec<Value>,
        available: Option<HashMap<String, String>>,
    },
}

fn build_options(spec: &OptionsSpec, golden: &Golden) -> CodeViewOptions<u8, u8> {
    let mut options = CodeViewOptions {
        diff: options_from_spec(&spec.diff, &mut HashMap::new()),
        ..CodeViewOptions::default()
    };
    options.sticky_headers = spec.sticky_headers;
    options.item_metrics = spec
        .item_metrics
        .as_ref()
        .map(|name| PartialVirtualFileMetrics::from(golden.metrics[name]));
    options.layout = spec.layout.as_ref().map(|name| golden.layouts[name]);
    options.smooth_scroll_settings = spec
        .smooth_scroll_settings
        .as_ref()
        .and_then(|name| golden.smooth.get(name).copied());
    options
}

fn align_name(align: Option<PendingAlign>) -> Option<&'static str> {
    align.map(|align| match align {
        PendingAlign::Start => "start",
        PendingAlign::Center => "center",
        PendingAlign::End => "end",
    })
}

/// The pending target as the script records it (`{ ...target, align }`).
pub(super) fn pending_json(target: &PendingScrollTarget) -> Value {
    let (plain, align) = match target {
        PendingScrollTarget::Position(target) => {
            (CodeViewScrollTarget::Position(target.clone()), None)
        }
        PendingScrollTarget::Line(target) => (
            CodeViewScrollTarget::Line(target.target.clone()),
            target.align,
        ),
        PendingScrollTarget::Range(target) => (
            CodeViewScrollTarget::Range(target.target.clone()),
            target.align,
        ),
        PendingScrollTarget::Item(target) => (
            CodeViewScrollTarget::Item(target.target.clone()),
            target.align,
        ),
    };
    let mut value = serde_json::to_value(plain).expect("a target serializes");
    let object = value.as_object_mut().expect("a target is an object");
    match align_name(align) {
        Some(name) => object.insert("align".to_owned(), json!(name)),
        None => object.remove("align"),
    };
    value
}

fn side_name(side: Option<SelectionSide>) -> Value {
    side.map_or(Value::Null, |side| {
        serde_json::to_value(side).expect("a side serializes")
    })
}

pub(super) fn anchor_json(anchor: &ScrollAnchor) -> Value {
    match anchor {
        ScrollAnchor::Item(anchor) => json!({
            "type": "item",
            "id": anchor.id,
            "viewportOffset": anchor.viewport_offset,
        }),
        ScrollAnchor::Line(anchor) => {
            let mut value = json!({
                "type": "line",
                "id": anchor.id,
                "lineNumber": anchor.line_number,
                "viewportOffset": anchor.viewport_offset,
            });
            if !side_name(anchor.side).is_null() {
                value["side"] = side_name(anchor.side);
            }
            value
        }
    }
}

fn anchor_from_json(value: &Value) -> ScrollAnchor {
    let id = value["id"].as_str().expect("an id").to_owned();
    let viewport_offset = value["viewportOffset"].as_f64().expect("an offset");
    match value["type"].as_str().expect("a type") {
        "item" => ScrollAnchor::Item(ItemAnchor {
            id,
            viewport_offset,
        }),
        "line" => ScrollAnchor::Line(LineAnchor {
            id,
            line_number: usize::try_from(value["lineNumber"].as_u64().expect("a line"))
                .expect("a line fits"),
            side: value
                .get("side")
                .map(|side| serde_json::from_value(side.clone()).expect("a side")),
            viewport_offset,
        }),
        other => panic!("unknown anchor type {other}"),
    }
}

pub(super) fn animation_from_json(value: &Value) -> ScrollToAnimation {
    ScrollToAnimation {
        position: value["position"].as_f64().expect("a position"),
        velocity: value["velocity"].as_f64().expect("a velocity"),
        last_timestamp: value["lastTimestamp"].as_f64().expect("a timestamp"),
    }
}

fn paged_json(position: PagedScrollPosition) -> Value {
    json!({
        "pagedScrollTop": position.paged_scroll_top,
        "scrollPageOffset": position.scroll_page_offset,
    })
}

fn number(value: Option<f64>) -> Value {
    value.map_or(Value::Null, |value| json!(value))
}

struct World {
    dom: Rc<FakeDom>,
    view: Rc<View>,
    golden: Rc<Golden>,
    root: Cell<Option<NodeKey>>,
    diffs: HashMap<String, Rc<FileDiffMetadata>>,
}

impl World {
    fn items(&self, specs: &[ItemSpec]) -> Vec<CodeViewItem<u8>> {
        specs
            .iter()
            .map(|spec| {
                CodeViewItem::Diff(CodeViewDiffItem {
                    id: spec.id.clone(),
                    file_diff: Rc::clone(&self.diffs[&spec.diff]),
                    annotations: spec
                        .annotations
                        .as_ref()
                        .map(|name| Rc::new(self.golden.lists[name].clone())),
                    version: None,
                    collapsed: None,
                    edit: None,
                })
            })
            .collect()
    }

    /// The script's `availableMap`: per id, the record itself (`keep`), another
    /// record (`stale`) or nothing.
    fn available(
        &self,
        available: &Option<HashMap<String, String>>,
    ) -> Option<HashMap<String, ItemRef<FakeDom, u8, u8>>> {
        let available = available.as_ref()?;
        let mut map = HashMap::new();
        for (id, how) in available {
            let record = self.view.item_by_id(id);
            match (how.as_str(), record) {
                ("keep", Some(record)) => {
                    map.insert(id.clone(), record);
                }
                ("stale", Some(record)) => {
                    let other = self
                        .view
                        .state
                        .borrow()
                        .items
                        .iter()
                        .find(|other| !Rc::ptr_eq(other, &record))
                        .cloned();
                    if let Some(other) = other {
                        map.insert(id.clone(), other);
                    }
                }
                _ => {}
            }
        }
        Some(map)
    }

    fn target(value: &Value) -> CodeViewScrollTarget {
        serde_json::from_value(value.clone()).expect("a scroll target")
    }

    /// A target the script passes as a pending one: never `nearest`.
    fn pending(&self, value: &Value) -> PendingScrollTarget {
        self.view
            .normalize_scroll_target(&Self::target(value))
            .expect("a non-nearest target is always pending")
    }

    fn call(
        &self,
        function: &str,
        args: &[Value],
        available: &Option<HashMap<String, String>>,
    ) -> Value {
        let view = &self.view;
        let number_arg = |index: usize| args[index].as_f64().expect("a number argument");
        match function {
            "getMaxScrollTop" => json!(view.get_max_scroll_top()),
            "shouldRebaseScroll" => json!(view.should_rebase_scroll()),
            "getPagedScrollHeight" => json!(view.get_paged_scroll_height()),
            "getMaxPagedScrollTop" => json!(view.get_max_paged_scroll_top()),
            "getMaxScrollPageOffset" => json!(view.get_max_scroll_page_offset()),
            "getMaxScrollTopForHeight" => json!(view.get_max_scroll_top_for_height(number_arg(0))),
            "clampScrollTop" => json!(view.clamp_scroll_top(number_arg(0))),
            "clampPagedScrollTop" => json!(view.clamp_paged_scroll_top(number_arg(0))),
            "clampScrollPageOffset" => json!(view.clamp_scroll_page_offset(number_arg(0))),
            "resolvePagedScrollPosition" => {
                paged_json(view.resolve_paged_scroll_position(number_arg(0)))
            }
            "resolveScrollPageWindow" => {
                paged_json(view.resolve_scroll_page_window(number_arg(0), number_arg(1)))
            }
            "needsScrollPageUpdate" => json!(view.needs_scroll_page_update(number_arg(0))),
            "getPagedLayoutTop" => json!(view.get_paged_layout_top(number_arg(0))),
            "getScrollTop" => json!(view.get_scroll_top()),
            "getHeight" => json!(view.get_height()),
            "getStickyHeaderOffset" => json!(view.get_sticky_header_offset()),
            "getScrollTargetRect" => view.get_scroll_target_rect(&Self::target(&args[0])).map_or(
                Value::Null,
                |rect| json!({ "top": rect.top, "height": rect.height }),
            ),
            "normalizeScrollTarget" => view
                .normalize_scroll_target(&Self::target(&args[0]))
                .map_or(Value::Null, |target| pending_json(&target)),
            "resolveScrollTargetTop" => {
                number(view.resolve_scroll_target_top(&self.pending(&args[0])))
            }
            "resolveAlignedScrollPosition" => {
                let align = match args[2].as_str() {
                    None => None,
                    Some("start") => Some(PendingAlign::Start),
                    Some("center") => Some(PendingAlign::Center),
                    Some("end") => Some(PendingAlign::End),
                    Some(other) => panic!("unknown align {other}"),
                };
                json!(view.resolve_aligned_scroll_position(AlignedScrollPosition {
                    target_top: number_arg(0),
                    target_height: number_arg(1),
                    align,
                    offset: number_arg(3),
                    sticky_offset: number_arg(4),
                }))
            }
            "resolveEffectiveScrollBehavior" => {
                let pending = self.pending(&args[0]);
                json!(
                    match view.resolve_effective_scroll_behavior(&pending, number_arg(1)) {
                        CodeViewScrollBehavior::Instant => "instant",
                        CodeViewScrollBehavior::Smooth => "smooth",
                        CodeViewScrollBehavior::SmoothAuto => "smooth-auto",
                    }
                )
            }
            "computeSpringStep" => {
                let step = view.compute_spring_step(
                    &animation_from_json(&args[0]),
                    number_arg(1),
                    number_arg(2),
                );
                json!({ "position": step.position, "velocity": step.velocity })
            }
            "computeTargetScrollTopForFrame" => {
                json!(view.compute_target_scroll_top_for_frame(number_arg(0), number_arg(1)))
            }
            "advanceScrollAnimation" => {
                number(view.advance_scroll_animation(number_arg(0), number_arg(1)))
            }
            "getScrollAnchorViewportTop" => {
                json!(view.get_scroll_anchor_viewport_top(number_arg(0), number_arg(1)))
            }
            "getScrollAnchor" => view
                .get_scroll_anchor(number_arg(0), self.available(available).as_ref())
                .map_or(Value::Null, |anchor| anchor_json(&anchor)),
            "resolveAnchoredScrollTop" => {
                number(view.resolve_anchored_scroll_top(&anchor_from_json(&args[0])))
            }
            "isPendingTargetSettled" => {
                json!(view.is_pending_target_settled(&self.pending(&args[0])))
            }
            "capturePendingLayoutAnchor" => {
                view.capture_pending_layout_anchor(self.available(available).as_ref());
                Value::Null
            }
            other => panic!("unknown call {other}"),
        }
    }

    fn set_state(&self, state: &Map<String, Value>) {
        let mut view = self.view.state.borrow_mut();
        let float = |key: &str| {
            state
                .get(key)
                .map(|value| value.as_f64().expect("a number"))
        };
        let flag = |key: &str| state.get(key).map(|value| value.as_bool().expect("a bool"));
        if let Some(value) = float("scrollTop") {
            view.scroll_top = value;
        }
        if let Some(value) = float("scrollPageOffset") {
            view.scroll_page_offset = value;
        }
        if let Some(value) = flag("scrollDirty") {
            view.scroll_dirty = value;
        }
        if let Some(value) = float("height") {
            view.height = value;
        }
        if let Some(value) = flag("heightDirty") {
            view.height_dirty = value;
        }
        if let Some(value) = float("scrollHeight") {
            view.scroll_height = value;
        }
        if let Some(value) = float("header") {
            view.header.height = value;
        }
        if let Some(value) = float("footer") {
            view.footer.height = value;
        }
        if let Some(render_state) = state.get("renderState") {
            let float = |key: &str| {
                render_state
                    .get(key)
                    .map(|value| value.as_f64().expect("a number"))
            };
            if let Some(value) = float("scrollTop") {
                view.render_state.scroll_top = value;
            }
            if let Some(value) = float("firstIndex") {
                view.render_state.first_index = value as i64;
            }
            if let Some(value) = float("lastIndex") {
                view.render_state.last_index = value as i64;
            }
            if let Some(value) = float("stickyHeight") {
                view.render_state.sticky_height = value;
            }
            if let Some(value) = float("stickyTop") {
                view.render_state.sticky_top = value;
            }
            if let Some(value) = float("stickyBottom") {
                view.render_state.sticky_bottom = value;
            }
        }
        drop(view);
        if let Some(value) = state.get("pendingScrollTarget") {
            let pending = (!value.is_null()).then(|| self.pending(value));
            self.view.state.borrow_mut().pending_scroll_target = pending;
        }
        if let Some(value) = state.get("scrollAnimation") {
            let animation = (!value.is_null()).then(|| animation_from_json(value));
            self.view.state.borrow_mut().scroll_animation = animation;
        }
        if let Some(value) = state.get("pendingLayoutAnchor") {
            let anchor = (!value.is_null()).then(|| anchor_from_json(value));
            self.view.state.borrow_mut().pending_layout_anchor = anchor;
        }
    }

    fn apply(&self, step: &Step) -> Value {
        let view = &self.view;
        match step {
            Step::SetItems { items } => {
                view.set_items(&self.items(items));
                Value::Null
            }
            Step::Recompute => {
                let pending = {
                    let mut state = view.state.borrow_mut();
                    let reset = state.pending_layout_reset;
                    state.layout_dirty_index.take().map(|index| (index, reset))
                };
                if let Some((index, reset)) = pending {
                    view.recompute_layout(index, reset);
                    view.state.borrow_mut().pending_layout_reset = None;
                }
                Value::Null
            }
            Step::SetOptions { options } => {
                view.set_options(Some(build_options(options, &self.golden)));
                Value::Null
            }
            Step::SetRoot { scroll_top, height } => {
                let root = self.root.get().unwrap_or_else(|| {
                    let root = self.dom.doc.create_element("div");
                    self.root.set(Some(root));
                    root
                });
                self.dom.world.borrow_mut().scroll_top = *scroll_top;
                self.dom.heights.borrow_mut().insert(root, *height);
                let mut state = view.state.borrow_mut();
                state.root = Some(root);
                state.scroll_dirty = true;
                state.height_dirty = true;
                Value::Null
            }
            Step::WriteRoot { scroll_top, height } => {
                if let Some(scroll_top) = scroll_top {
                    self.dom.world.borrow_mut().scroll_top = *scroll_top;
                }
                if let Some(height) = height {
                    let root = self.root.get().expect("a root was set");
                    self.dom.heights.borrow_mut().insert(root, *height);
                }
                Value::Null
            }
            Step::SetState { state } => {
                self.set_state(state);
                Value::Null
            }
            Step::Call {
                function,
                args,
                available,
            } => self.call(function, args, available),
        }
    }

    fn snapshot(&self) -> Value {
        let state = self.view.state.borrow();
        json!({
            "scrollTop": state.scroll_top,
            "scrollPageOffset": state.scroll_page_offset,
            "scrollDirty": state.scroll_dirty,
            "height": state.height,
            "heightDirty": state.height_dirty,
            "scrollHeight": state.scroll_height,
            "pendingScrollTarget": state.pending_scroll_target.as_ref().map_or(Value::Null, pending_json),
            "scrollAnimation": state.scroll_animation.map_or(Value::Null, |animation| json!({
                "position": animation.position,
                "velocity": animation.velocity,
                "lastTimestamp": animation.last_timestamp,
            })),
            "pendingLayoutAnchor": state.pending_layout_anchor.as_ref().map_or(Value::Null, anchor_json),
            "renderState": {
                "scrollTop": state.render_state.scroll_top,
                "firstIndex": state.render_state.first_index,
                "lastIndex": state.render_state.last_index,
                "stickyHeight": state.render_state.sticky_height,
                "stickyTop": state.render_state.sticky_top,
                "stickyBottom": state.render_state.sticky_bottom,
            },
            "items": state.items.iter().map(|item| {
                let item = item.borrow();
                json!([item.id(), item.top, item.height])
            }).collect::<Vec<_>>(),
        })
    }
}

/// Numbers within a relative `1e-9` (and the exact structure) compare equal.
fn close(expected: &Value, actual: &Value) -> bool {
    match (expected, actual) {
        (Value::Number(left), Value::Number(right)) => {
            let (left, right) = (left.as_f64().expect("f64"), right.as_f64().expect("f64"));
            (left - right).abs() <= 1e-9 * left.abs().max(right.abs()).max(1.0)
        }
        (Value::Array(left), Value::Array(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| close(left, right))
        }
        (Value::Object(left), Value::Object(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .all(|(key, left)| right.get(key).is_some_and(|right| close(left, right)))
        }
        (left, right) => left == right,
    }
}

fn assert_matches(approx: bool, context: &str, expected: &Value, actual: &Value) {
    if approx {
        assert!(
            close(expected, actual),
            "{context}\n  expected {expected}\n  actual {actual}"
        );
    } else {
        assert_json_eq(context, expected, actual);
    }
}

#[test]
fn code_view_scroll_model_matches_the_published_class() {
    let golden: Rc<Golden> = Rc::new(load_golden("s12d_code_view_scroll.json"));
    assert!(golden.cases.len() > 200, "{} scenarios", golden.cases.len());
    let mut steps = 0;
    for case in &golden.cases {
        let scenario = &case.scenario;
        let dom = Rc::new(FakeDom::default());
        dom.device_pixel_ratio.set(scenario.env.dpr.unwrap_or(1.0));
        dom.prefers_reduced_motion
            .set(scenario.env.reduced_motion == Some(true));
        dom.platform_info.borrow_mut().user_agent =
            scenario.env.user_agent.clone().unwrap_or_default();
        let view = CodeView::new(
            Rc::clone(&dom),
            ResizeObserverHub::new(Rc::clone(&dom)),
            UniversalRenderingManager::new(Rc::clone(&dom)),
            Rc::new(PlainTextHighlighter),
            build_options(&scenario.options, &golden),
        );
        let diffs = golden
            .diffs
            .iter()
            .map(|(name, diff)| (name.clone(), Rc::new(diff.clone())))
            .collect();
        let world = World {
            dom,
            view,
            golden: Rc::clone(&golden),
            root: Cell::new(None),
            diffs,
        };
        let context = |step: usize| format!("{} step {step}", scenario.name);
        let config = {
            let config = world.view.state.borrow().scroll_rebase_config;
            json!({
                "containerHeight": config.container_height,
                "triggerTop": config.trigger_top,
                "targetTop": config.target_top,
                "targetBottom": config.target_bottom,
                "threshold": config.threshold,
            })
        };
        assert_json_eq(&format!("{} config", scenario.name), &case.config, &config);
        assert_json_eq(&context(0), &case.trace[0], &world.snapshot());
        for (index, step) in scenario.steps.iter().enumerate() {
            let result = std::panic::catch_unwind(AssertUnwindSafe(|| world.apply(step)));
            let result = match result {
                Ok(value) => json!({ "value": value }),
                Err(payload) => json!({ "throws": panic_message(&*payload) }),
            };
            assert_matches(
                scenario.approx,
                &format!("{} result", context(index + 1)),
                &case.results[index],
                &result,
            );
            // Pure calls record no state.
            if !case.trace[index + 1].is_null() {
                assert_matches(
                    scenario.approx,
                    &context(index + 1),
                    &case.trace[index + 1],
                    &world.snapshot(),
                );
            }
            steps += 1;
        }
    }
    assert!(steps > 10_000, "{steps} steps");
}
