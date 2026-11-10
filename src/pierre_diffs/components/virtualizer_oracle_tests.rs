//! Differential tests for S10: the published `Virtualizer` class and
//! `areVirtualWindowSpecsEqual` ran scripted scenarios against a fake DOM
//! (`oracle/virtualizer_scenarios.mjs`, golden `oracle/golden/virtualizer.json`:
//! hand-written scenarios plus seeded random step sequences). Each scenario is
//! replayed against the port over `fake_dom::FakeHost` and the traces of
//! everything the class did (instance callbacks, host writes, observer and
//! listener registration, frame requests, query results) must be equal. The
//! fake declares geometry instead of computing it, so what this establishes is
//! the control flow and the arithmetic, not a browser's behavior. Test-only,
//! original to this crate.

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::rc::Rc;
use std::rc::Weak;

use serde::Deserialize;
use serde_json::Value;
use serde_json::json;

use crate::components::virtualizer::InstanceRef;
use crate::components::virtualizer::IntersectionEntry;
use crate::components::virtualizer::ResizeEntry;
use crate::components::virtualizer::Root;
use crate::components::virtualizer::ScrollBehavior;
use crate::components::virtualizer::SubscribedInstance;
use crate::components::virtualizer::Virtualizer;
use crate::components::virtualizer::VirtualizerConfig;
use crate::components::virtualizer::VirtualizerScrollOptions;
use crate::fake_dom::FakeHost;
use crate::fake_dom::ROOT;
use crate::fake_dom::WorldPatch;
use crate::managers::universal_rendering_manager::UniversalRenderingManager;
use crate::oracle_testing::assert_json_eq;
use crate::oracle_testing::load_golden;
use crate::types::layout::VirtualWindowSpecs;
use crate::utils::predicates::are_virtual_window_specs_equal::are_virtual_window_specs_equal;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Golden {
    cases: Vec<Case>,
    window_specs_equal: Vec<SpecsEqualCase>,
}

#[derive(Deserialize)]
struct Case {
    scenario: Scenario,
    trace: Value,
}

#[derive(Deserialize)]
struct SpecsEqualCase {
    left: Option<VirtualWindowSpecs>,
    right: Option<VirtualWindowSpecs>,
    expected: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Scenario {
    name: String,
    root: String,
    #[serde(default)]
    config: Option<ConfigPatch>,
    world: WorldPatch,
    instances: BTreeMap<String, InstanceSpec>,
    steps: Vec<Op>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ConfigPatch {
    overscroll_size: Option<f64>,
    intersection_observer_margin: Option<f64>,
    resize_debugging: Option<bool>,
}

#[derive(Deserialize, Clone)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InstanceSpec {
    container: String,
    #[serde(default)]
    render: Vec<bool>,
    #[serde(default)]
    reconcile: Vec<bool>,
    #[serde(default)]
    render_effects: Vec<Effect>,
    #[serde(default)]
    reconcile_effects: Vec<Effect>,
}

#[derive(Deserialize, Clone, Default)]
#[serde(deny_unknown_fields)]
struct Effect {
    world: Option<WorldPatch>,
    #[serde(default)]
    ops: Vec<Op>,
}

#[derive(Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct ResizeEntryJson {
    target: String,
    block_size: f64,
}

#[derive(Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct IntersectionEntryJson {
    target: String,
    is_intersecting: bool,
}

#[derive(Deserialize, Clone)]
#[serde(tag = "op", rename_all = "camelCase")]
enum Op {
    Setup {
        content: Option<String>,
    },
    Connect {
        instance: String,
    },
    Dispose {
        instance: String,
    },
    Disconnect {
        container: String,
    },
    World {
        set: WorldPatch,
    },
    ScrollEvent,
    WindowResizeEvent,
    Resize {
        entries: Vec<ResizeEntryJson>,
    },
    Intersect {
        entries: Vec<IntersectionEntryJson>,
    },
    Frame {
        time: f64,
    },
    Frames {
        limit: usize,
        time: f64,
    },
    #[serde(rename_all = "camelCase")]
    InstanceChanged {
        instance: String,
        dom_dirty: bool,
    },
    RequestHeightReconcile {
        instance: String,
    },
    GetWindowSpecs,
    GetScrollTop,
    IsInstanceVisible {
        top: f64,
        height: f64,
    },
    GetOffsetInScrollContainer {
        element: String,
    },
    ScrollTo {
        top: f64,
        behavior: Option<String>,
    },
    MarkDomDirty,
    CleanUp,
}

/// The scripted `SubscribedInstance` of the JavaScript runner: answers from
/// the scenario's lists (false once exhausted) and applies the nth call's
/// effect (a world change and re-entrant ops) before returning.
struct ScriptedInstance {
    name: String,
    spec: InstanceSpec,
    render_calls: Cell<usize>,
    reconcile_calls: Cell<usize>,
    context: Weak<Context>,
}

impl ScriptedInstance {
    fn apply(&self, effect: Option<&Effect>) {
        let (Some(effect), Some(context)) = (effect, self.context.upgrade()) else {
            return;
        };
        if let Some(world) = &effect.world {
            context.host.apply_world(world);
        }
        for op in &effect.ops {
            context.run(op);
        }
    }
}

impl SubscribedInstance for ScriptedInstance {
    fn on_render(&self, dirty: bool) -> bool {
        let context = self.context.upgrade().expect("scenario outlives its calls");
        context.host.record(json!(["onRender", self.name, dirty]));
        let call = self.render_calls.replace(self.render_calls.get() + 1);
        self.apply(self.spec.render_effects.get(call));
        self.spec.render.get(call).copied().unwrap_or(false)
    }

    fn reconcile_heights(&self) -> bool {
        let context = self.context.upgrade().expect("scenario outlives its calls");
        context.host.record(json!(["reconcileHeights", self.name]));
        let call = self.reconcile_calls.replace(self.reconcile_calls.get() + 1);
        self.apply(self.spec.reconcile_effects.get(call));
        self.spec.reconcile.get(call).copied().unwrap_or(false)
    }

    fn set_visibility(&self, visible: bool) {
        let context = self.context.upgrade().expect("scenario outlives its calls");
        context
            .host
            .record(json!(["setVisibility", self.name, visible]));
    }
}

struct Context {
    document_root: bool,
    host: Rc<FakeHost>,
    virtualizer: Rc<Virtualizer<FakeHost>>,
    instances: BTreeMap<String, Rc<ScriptedInstance>>,
    containers: HashMap<String, String>,
    disposers: RefCell<HashMap<String, Box<dyn Fn()>>>,
}

impl Context {
    fn new(scenario: &Scenario) -> Rc<Self> {
        let document_root = scenario.root == "document";
        let host = Rc::new(FakeHost::new(document_root));
        host.apply_world(&scenario.world);
        let defaults = VirtualizerConfig::default();
        let config = scenario
            .config
            .as_ref()
            .map_or(defaults, |patch| VirtualizerConfig {
                overscroll_size: patch.overscroll_size.unwrap_or(defaults.overscroll_size),
                intersection_observer_margin: patch
                    .intersection_observer_margin
                    .unwrap_or(defaults.intersection_observer_margin),
                resize_debugging: patch.resize_debugging.unwrap_or(defaults.resize_debugging),
            });
        let virtualizer = Virtualizer::new(
            Rc::clone(&host),
            UniversalRenderingManager::new(Rc::clone(&host)),
            config,
        );
        Rc::new_cyclic(|context: &Weak<Context>| Context {
            document_root,
            host,
            virtualizer,
            instances: scenario
                .instances
                .iter()
                .map(|(name, spec)| {
                    (
                        name.clone(),
                        Rc::new(ScriptedInstance {
                            name: name.clone(),
                            spec: spec.clone(),
                            render_calls: Cell::new(0),
                            reconcile_calls: Cell::new(0),
                            context: Weak::clone(context),
                        }),
                    )
                })
                .collect(),
            containers: scenario
                .instances
                .iter()
                .map(|(name, spec)| (name.clone(), spec.container.clone()))
                .collect(),
            disposers: RefCell::default(),
        })
    }

    fn instance(&self, name: &str) -> InstanceRef {
        let instance: InstanceRef = Rc::<ScriptedInstance>::clone(&self.instances[name]);
        instance
    }

    fn run(&self, op: &Op) {
        let virtualizer = &self.virtualizer;
        let host = &self.host;
        match op {
            Op::Setup { content } => {
                let root = if self.document_root {
                    Root::Document
                } else {
                    Root::Element(ROOT.to_owned())
                };
                virtualizer.setup(root, content.clone());
            }
            Op::Connect { instance } => {
                let disposer =
                    virtualizer.connect(self.containers[instance].clone(), self.instance(instance));
                self.disposers
                    .borrow_mut()
                    .insert(instance.clone(), disposer);
            }
            Op::Dispose { instance } => {
                let disposers = self.disposers.borrow();
                disposers[instance]();
            }
            Op::Disconnect { container } => virtualizer.disconnect(container),
            Op::World { set } => host.apply_world(set),
            Op::ScrollEvent => {
                if host.element_scroll_listener().is_some() {
                    virtualizer.handle_element_scroll();
                }
                if host.window_scroll_listener() {
                    virtualizer.handle_window_scroll();
                }
            }
            Op::WindowResizeEvent => {
                if host.window_resize_listener() {
                    virtualizer.handle_window_resize();
                }
            }
            Op::Resize { entries } => {
                if host.observers_active() {
                    let entries: Vec<_> = entries
                        .iter()
                        .map(|entry| ResizeEntry {
                            target: entry.target.clone(),
                            block_size: entry.block_size,
                        })
                        .collect();
                    virtualizer.handle_container_resize(&entries);
                }
            }
            Op::Intersect { entries } => {
                if host.observers_active() {
                    let entries: Vec<_> = entries
                        .iter()
                        .map(|entry| IntersectionEntry {
                            target: entry.target.clone(),
                            is_intersecting: entry.is_intersecting,
                        })
                        .collect();
                    virtualizer.handle_intersection_change(&entries);
                }
            }
            Op::Frame { time } => {
                host.frames.run_round(*time);
            }
            Op::Frames { limit, time } => {
                host.frames.flush_frames(*limit, *time);
            }
            Op::InstanceChanged {
                instance,
                dom_dirty,
            } => virtualizer.instance_changed(&self.instance(instance), *dom_dirty),
            Op::RequestHeightReconcile { instance } => {
                virtualizer.request_height_reconcile(&self.instance(instance));
            }
            Op::GetWindowSpecs => {
                let specs = virtualizer.get_window_specs();
                host.record(json!(["result", "getWindowSpecs", {
                    "top": specs.top,
                    "bottom": specs.bottom,
                }]));
            }
            Op::GetScrollTop => {
                host.record(json!([
                    "result",
                    "getScrollTop",
                    virtualizer.get_scroll_top()
                ]));
            }
            Op::IsInstanceVisible { top, height } => {
                host.record(json!([
                    "result",
                    "isInstanceVisible",
                    virtualizer.is_instance_visible(*top, *height)
                ]));
            }
            Op::GetOffsetInScrollContainer { element } => {
                host.record(json!([
                    "result",
                    "getOffsetInScrollContainer",
                    virtualizer.get_offset_in_scroll_container(element)
                ]));
            }
            Op::ScrollTo { top, behavior } => {
                let behavior = match behavior.as_deref() {
                    None | Some("auto") => ScrollBehavior::Auto,
                    Some("instant") => ScrollBehavior::Instant,
                    Some("smooth") => ScrollBehavior::Smooth,
                    Some(other) => panic!("unknown scroll behavior {other}"),
                };
                virtualizer.scroll_to(VirtualizerScrollOptions {
                    top: *top,
                    behavior,
                });
            }
            Op::MarkDomDirty => virtualizer.mark_dom_dirty(),
            Op::CleanUp => virtualizer.clean_up(),
        }
    }
}

#[test]
fn scenarios_match_the_published_virtualizer() {
    let golden: Golden = load_golden("virtualizer.json");
    assert!(golden.cases.len() > 100);
    for case in &golden.cases {
        let context = Context::new(&case.scenario);
        for op in &case.scenario.steps {
            context.run(op);
        }
        assert_json_eq(
            &format!("scenario `{}`", case.scenario.name),
            &case.trace,
            &Value::Array(context.host.take_trace()),
        );
    }
}

#[test]
fn window_specs_equality_matches_the_published_function() {
    let golden: Golden = load_golden("virtualizer.json");
    for case in &golden.window_specs_equal {
        assert_eq!(
            are_virtual_window_specs_equal(case.left.as_ref(), case.right.as_ref()),
            case.expected,
            "{:?} vs {:?}",
            case.left,
            case.right
        );
    }
}
