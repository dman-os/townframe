//! Differential tests for S11b: the published `InteractionManager` ran
//! scripted scenarios against a fake DOM (`oracle/s11b_interaction.mjs`, golden
//! `oracle/golden/s11b.json`: handwritten scenarios plus seeded random event
//! and option sequences) and the port replays each one over `FakeDom`. The
//! traces (attribute writes, the gutter utility nodes, listener registration,
//! frame requests, every option callback with its arguments, the response to
//! each event, query results, thrown messages) must be equal. The fake answers
//! `composedPath()` with the target and its ancestors and `elementFromPoint`
//! from a table; it establishes the manager's decisions for the same path and
//! coordinates, not real hit-testing or event ordering. Test-only, original to
//! this crate.

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::rc::Rc;

use serde::Deserialize;
use serde_json::Value;
use serde_json::json;

use crate::fake_interaction_dom::FakeDom;
use crate::fake_interaction_dom::TreeNode;
use crate::managers::interaction_manager::GetLineIndexUtility;
use crate::managers::interaction_manager::InteractionManager;
use crate::managers::interaction_manager::InteractionManagerBaseOptions;
use crate::managers::interaction_manager::InteractionManagerMode;
use crate::managers::interaction_manager::InteractionManagerOptions;
use crate::managers::interaction_manager::InteractionPluckOptions;
use crate::managers::interaction_manager::LineCallback;
use crate::managers::interaction_manager::LineEventKind;
use crate::managers::interaction_manager::LineEventProps;
use crate::managers::interaction_manager::LineHoverHighlight;
use crate::managers::interaction_manager::PointerEventData;
use crate::managers::interaction_manager::PointerResponse;
use crate::managers::interaction_manager::PointerType;
use crate::managers::interaction_manager::SelectionCallback;
use crate::managers::interaction_manager::SelectionWriteOptions;
use crate::managers::interaction_manager::pluck_interaction_options;
use crate::managers::universal_rendering_manager::UniversalRenderingManager;
use crate::oracle_testing::assert_json_eq;
use crate::oracle_testing::load_golden;
use crate::types::lines::ExpansionDirections;
use crate::types::view::AnnotationSide;
use crate::types::view::SelectedLineRange;
use crate::types::view::SelectionPoint;
use crate::utils::dom::is_gutter_utility_path::is_gutter_utility_path;
use crate::utils::predicates::are_selection_points_equal::are_selection_points_equal;
use crate::utils::predicates::are_selections_equal::are_selections_equal;
use crate::utils::render::create_gutter_utility_element::create_gutter_utility_element;

#[derive(Deserialize)]
struct WorldSpec {
    tree: Vec<TreeNode>,
    #[serde(default)]
    slots: HashMap<String, String>,
    #[serde(default)]
    hits: HashMap<String, Option<String>>,
    #[serde(rename = "hitTestAvailable", default = "default_true")]
    hit_test_available: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Deserialize, Default, Clone)]
#[serde(rename_all = "camelCase")]
struct OptionsSpec {
    line_hover_highlight: Option<LineHoverHighlight>,
    enable_gutter_utility: Option<bool>,
    enable_line_selection: Option<bool>,
    controlled_selection: Option<bool>,
    #[serde(default)]
    on_line_click: bool,
    #[serde(default)]
    on_line_number_click: bool,
    #[serde(default)]
    on_line_enter: bool,
    #[serde(default)]
    on_line_leave: bool,
    #[serde(default)]
    on_gutter_utility_click: bool,
    #[serde(default)]
    on_line_selected: bool,
    #[serde(default)]
    on_line_selection_start: bool,
    #[serde(default)]
    on_line_selection_change: bool,
    #[serde(default)]
    on_line_selection_end: bool,
    #[serde(default)]
    on_hunk_expand: bool,
    #[serde(default)]
    has_render_gutter_utility: bool,
    line_index: Option<HashMap<String, (i64, i64)>>,
    // `usesCustomGutterUtility` is read by the JS pluck from `renderGutterUtility`
    // only, so the key is accepted and ignored.
    #[serde(rename = "usesCustomGutterUtility")]
    _uses_custom_gutter_utility: Option<bool>,
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
struct EventSpec {
    #[serde(rename = "type")]
    event_type: String,
    target: Option<String>,
    #[serde(rename = "pointerType", default = "default_pointer_type")]
    pointer_type: String,
    #[serde(rename = "pointerId", default = "default_pointer_id")]
    pointer_id: i32,
    #[serde(default)]
    button: i16,
    #[serde(rename = "shiftKey", default)]
    shift_key: bool,
    #[serde(rename = "clientX", default = "nan")]
    client_x: f64,
    #[serde(rename = "clientY", default = "nan")]
    client_y: f64,
}

fn default_pointer_type() -> String {
    "mouse".to_owned()
}

fn default_pointer_id() -> i32 {
    1
}

fn nan() -> f64 {
    f64::NAN
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "camelCase")]
enum Op {
    SetOptions {
        options: OptionsSpec,
    },
    Setup {
        pre: String,
    },
    CleanUp,
    SetSelection {
        range: Option<SelectedLineRange>,
        options: Option<WriteOptionsSpec>,
    },
    SetSelectionDirty,
    GetSelection,
    GetHoveredLine,
    IsSelectionDirty,
    Attr {
        target: String,
        name: String,
        value: Option<String>,
    },
    Frames {
        limit: usize,
        time: f64,
    },
    Event(EventSpec),
}

#[derive(Deserialize)]
struct Scenario {
    name: String,
    mode: String,
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
struct SelectionPair {
    left: Option<SelectedLineRange>,
    right: Option<SelectedLineRange>,
    expected: bool,
}

#[derive(Deserialize)]
struct PointPair {
    left: SelectionPoint,
    right: SelectionPoint,
    expected: bool,
}

#[derive(Deserialize)]
struct PathCase {
    path: Vec<Vec<(String, String)>>,
    expected: bool,
}

#[derive(Deserialize)]
struct Golden {
    cases: Vec<Case>,
    selections: Vec<SelectionPair>,
    points: Vec<PointPair>,
    paths: Vec<PathCase>,
    #[serde(rename = "utilityButton")]
    utility_button: Value,
}

fn side_name(side: Option<AnnotationSide>) -> &'static str {
    match side {
        Some(AnnotationSide::Additions) => "additions",
        Some(AnnotationSide::Deletions) => "deletions",
        None => "undefined",
    }
}

fn pointer_type_name(pointer_type: PointerType) -> &'static str {
    match pointer_type {
        PointerType::Mouse => "mouse",
        PointerType::Touch => "touch",
        PointerType::Pen => "pen",
    }
}

fn event_out(dom: &FakeDom, event: &PointerEventData<usize>) -> Value {
    json!({
        "pointerId": event.pointer_id,
        "pointerType": pointer_type_name(event.pointer_type),
        "shiftKey": event.shift_key,
        "target": event.target.map(|target| dom.name_of(target)),
    })
}

fn event_props(dom: &FakeDom, props: &LineEventProps<usize>) -> Value {
    let base = &props.base;
    let mut out = json!({
        "type": match base.kind {
            LineEventKind::Line => "line",
            LineEventKind::DiffLine => "diff-line",
        },
        "lineNumber": base.line_number,
        "numberColumn": base.number_column,
        "lineElement": dom.name_of(base.line_element),
        "numberElement": dom.name_of(base.number_element),
    });
    if base.kind == LineEventKind::DiffLine {
        out["annotationSide"] = json!(match base.annotation_side {
            Some(AnnotationSide::Additions) => "additions",
            _ => "deletions",
        });
        out["lineType"] = json!(base.line_type.expect("diff line type").as_str());
    }
    out
}

fn build_base(dom: &Rc<FakeDom>, spec: &OptionsSpec) -> InteractionManagerBaseOptions<usize> {
    let line = |name: &'static str, enabled: bool| -> Option<LineCallback<usize>> {
        enabled.then(|| {
            let dom = Rc::clone(dom);
            Rc::new(move |props: &LineEventProps<usize>| {
                dom.emit(json!([
                    name,
                    event_props(&dom, props),
                    event_out(&dom, &props.event)
                ]));
            }) as LineCallback<usize>
        })
    };
    let selection = |name: &'static str, enabled: bool| -> Option<SelectionCallback> {
        enabled.then(|| {
            let dom = Rc::clone(dom);
            Rc::new(move |range: Option<&SelectedLineRange>| {
                dom.emit(json!([name, range]));
            }) as SelectionCallback
        })
    };
    let get_line_index: Option<GetLineIndexUtility> = spec.line_index.clone().map(|table| {
        Rc::new(move |line_number: usize, side: Option<AnnotationSide>| {
            table
                .get(&format!("{}:{line_number}", side_name(side)))
                .copied()
        }) as GetLineIndexUtility
    });
    InteractionManagerBaseOptions {
        line_hover_highlight: spec.line_hover_highlight,
        enable_gutter_utility: spec.enable_gutter_utility,
        on_gutter_utility_click: spec.on_gutter_utility_click.then(|| {
            let dom = Rc::clone(dom);
            Rc::new(move |range: SelectedLineRange| {
                dom.emit(json!(["onGutterUtilityClick", range]));
            }) as Rc<dyn Fn(SelectedLineRange)>
        }),
        on_line_click: line("onLineClick", spec.on_line_click),
        on_line_number_click: line("onLineNumberClick", spec.on_line_number_click),
        on_line_enter: line("onLineEnter", spec.on_line_enter),
        on_line_leave: line("onLineLeave", spec.on_line_leave),
        enable_line_selection: spec.enable_line_selection,
        controlled_selection: spec.controlled_selection,
        on_line_selected: selection("onLineSelected", spec.on_line_selected),
        on_line_selection_start: selection("onLineSelectionStart", spec.on_line_selection_start),
        on_line_selection_change: selection("onLineSelectionChange", spec.on_line_selection_change),
        on_line_selection_end: selection("onLineSelectionEnd", spec.on_line_selection_end),
        get_line_index,
    }
}

fn pluck(dom: &Rc<FakeDom>, spec: &OptionsSpec) -> InteractionManagerOptions<usize> {
    let base = build_base(dom, spec);
    let get_line_index = base.get_line_index.clone();
    let on_hunk_expand = spec.on_hunk_expand.then(|| {
        let dom = Rc::clone(dom);
        Rc::new(
            move |hunk_index: i64, direction: ExpansionDirections, over: Option<f64>| {
                dom.emit(json!([
                    "onHunkExpand",
                    hunk_index,
                    direction,
                    over.map(|_| "inf")
                ]));
            },
        ) as Rc<dyn Fn(i64, ExpansionDirections, Option<f64>)>
    });
    pluck_interaction_options(
        InteractionPluckOptions {
            base,
            has_render_gutter_utility: spec.has_render_gutter_utility,
        },
        on_hunk_expand,
        get_line_index,
    )
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

type Handler = fn(&InteractionManager<FakeDom>, &PointerEventData<usize>) -> PointerResponse;

fn dispatch(dom: &FakeDom, manager: &InteractionManager<FakeDom>, spec: &EventSpec) {
    let target = spec.target.as_ref().map(|name| dom.id(name));
    let event = PointerEventData {
        pointer_id: spec.pointer_id,
        pointer_type: match spec.pointer_type.as_str() {
            "touch" => PointerType::Touch,
            "pen" => PointerType::Pen,
            _ => PointerType::Mouse,
        },
        button: spec.button,
        shift_key: spec.shift_key,
        client_x: spec.client_x,
        client_y: spec.client_y,
        target,
    };
    let (registered, handler): (bool, Handler) = match spec.event_type.as_str() {
        "click" => (
            dom.pre_listener.get(),
            InteractionManager::handle_pointer_click,
        ),
        "pointerdown" => (
            dom.pre_listener.get(),
            InteractionManager::handle_pointer_down,
        ),
        "pointermove" => (
            dom.pre_listener.get(),
            InteractionManager::handle_pointer_move,
        ),
        "pointerleave" => (
            dom.pre_listener.get(),
            InteractionManager::handle_pointer_leave,
        ),
        "document:pointermove" => (
            dom.document_listener.get(),
            InteractionManager::handle_document_pointer_move,
        ),
        "document:pointerup" => (
            dom.document_listener.get(),
            InteractionManager::handle_document_pointer_up,
        ),
        "document:pointercancel" => (
            dom.document_listener.get(),
            InteractionManager::handle_document_pointer_cancel,
        ),
        other => panic!("unknown event type {other}"),
    };
    if registered {
        let response = handler(manager, &event);
        dom.emit(json!([
            "response",
            response.prevent_default,
            response.stop_propagation
        ]));
    }
}

#[test]
fn interaction_manager_matches_the_published_class() {
    let golden: Golden = load_golden("s11b.json");
    assert!(golden.cases.len() > 150);
    for case in &golden.cases {
        let scenario = &case.scenario;
        let dom = Rc::new(FakeDom::default());
        dom.hit_test_available
            .set(scenario.world.hit_test_available);
        let mut roots = Vec::new();
        for node in &scenario.world.tree {
            roots.push(dom.build(node, None));
        }
        for (slotted, slot) in &scenario.world.slots {
            let slot_id = dom.id(slot);
            let slotted_id = dom.id(slotted);
            dom.nodes.borrow_mut()[slotted_id].assigned_slot = Some(slot_id);
        }
        for (key, name) in &scenario.world.hits {
            dom.hits
                .borrow_mut()
                .insert(key.clone(), name.as_ref().map(|name| dom.id(name)));
        }
        let mode = if scenario.mode == "file" {
            InteractionManagerMode::File
        } else {
            InteractionManagerMode::Diff
        };
        let initial = scenario.options.clone().unwrap_or_default();
        let constructed = std::panic::catch_unwind(AssertUnwindSafe(|| {
            InteractionManager::new(
                Rc::clone(&dom),
                UniversalRenderingManager::new(Rc::clone(&dom)),
                mode,
                pluck(&dom, &initial),
            )
        }));
        let manager = match constructed {
            Ok(manager) => manager,
            Err(payload) => {
                dom.recording.set(true);
                dom.emit(json!(["throws", panic_message(payload.as_ref())]));
                assert_json_eq(
                    &format!("scenario `{}`", scenario.name),
                    &case.trace,
                    &Value::Array(dom.trace.borrow().clone()),
                );
                continue;
            }
        };
        dom.recording.set(true);

        for step in &scenario.steps {
            let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| match step {
                Op::SetOptions { options } => manager.set_options(pluck(&dom, options)),
                Op::Setup { pre } => manager.setup(&dom.id(pre)),
                Op::CleanUp => manager.clean_up(),
                Op::SetSelection { range, options } => {
                    let options = options.clone().unwrap_or_default();
                    manager.set_selection(
                        range.clone(),
                        SelectionWriteOptions {
                            notify: options.notify,
                            active_line_side: options.active_line_side,
                            line_number_only: options.line_number_only,
                        },
                    );
                }
                Op::SetSelectionDirty => manager.set_selection_dirty(),
                Op::GetSelection => {
                    dom.emit(json!(["result", "getSelection", manager.get_selection()]));
                }
                Op::GetHoveredLine => {
                    let hovered = manager.get_hovered_line().map(|hovered| {
                        let mut out = json!({ "lineNumber": hovered.line_number });
                        if mode == InteractionManagerMode::Diff {
                            out["side"] = json!(hovered.side.map(|side| side_name(Some(side))));
                        }
                        out
                    });
                    dom.emit(json!(["result", "getHoveredLine", hovered]));
                }
                Op::IsSelectionDirty => {
                    dom.emit(json!([
                        "result",
                        "isSelectionDirty",
                        manager.is_selection_dirty()
                    ]));
                }
                Op::Attr {
                    target,
                    name,
                    value,
                } => {
                    let id = dom.id(target);
                    let mut nodes = dom.nodes.borrow_mut();
                    let node = &mut nodes[id];
                    match value {
                        None => node.attrs.retain(|(key, _)| key != name),
                        Some(value) => match node.attrs.iter_mut().find(|(key, _)| key == name) {
                            Some((_, slot)) => slot.clone_from(value),
                            None => node.attrs.push((name.clone(), value.clone())),
                        },
                    }
                }
                Op::Frames { limit, time } => {
                    dom.frames.flush_frames(*limit, *time);
                }
                Op::Event(spec) => dispatch(&dom, &manager, spec),
            }));
            if let Err(payload) = outcome {
                dom.emit(json!(["throws", panic_message(payload.as_ref())]));
            }
        }
        assert_json_eq(
            &format!("scenario `{}`", scenario.name),
            &case.trace,
            &Value::Array(dom.trace.borrow().clone()),
        );
    }
}

#[test]
fn selection_predicates_and_helpers_match_the_published_functions() {
    let golden: Golden = load_golden("s11b.json");
    for case in &golden.selections {
        assert_eq!(
            are_selections_equal(case.left.as_ref(), case.right.as_ref()),
            case.expected,
            "{:?} {:?}",
            case.left,
            case.right
        );
    }
    for case in &golden.points {
        assert_eq!(
            are_selection_points_equal(&case.left, &case.right),
            case.expected
        );
    }
    for case in &golden.paths {
        let path: Vec<usize> = (0..case.path.len()).collect();
        let actual = is_gutter_utility_path(&path, |element, name| {
            case.path[*element]
                .iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value.clone())
        });
        assert_eq!(actual, case.expected, "{:?}", case.path);
    }
    assert_json_eq(
        "gutter utility button",
        &golden.utility_button,
        &serde_json::to_value(create_gutter_utility_element()).expect("hast serializes"),
    );
}
