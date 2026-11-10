//! Differential tests for the S11a managers: the published `ResizeManager`
//! and `ScrollSyncManager` ran scripted scenarios against a fake DOM
//! (`oracle/s11a_managers.mjs`, golden `oracle/golden/s11a.json`: handwritten
//! scenarios plus seeded random step sequences) and the ports replay each
//! scenario over `FakeDom`. The traces (observer calls, style and scroll
//! writes, listeners, timers, reports, thrown messages) must be equal. The
//! fake declares heights and delivers observer entries when a step says so, so
//! this establishes control flow and arithmetic, not a browser's behavior.
//! Test-only, original to this crate.

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::rc::Rc;

use serde::Deserialize;
use serde_json::Value;
use serde_json::json;

use crate::managers::resize_manager::ResizeManager;
use crate::managers::resize_manager::ResizeManagerColumnVariableMode;
use crate::managers::resize_manager::ResizeManagerHost;
use crate::managers::resize_manager::ResizeManagerSetupOptions;
use crate::managers::resize_manager::ResizeObserverEntry;
use crate::managers::resize_manager::ResizeObserverHub;
use crate::managers::scroll_sync_manager::ScrollSide;
use crate::managers::scroll_sync_manager::ScrollSyncHost;
use crate::managers::scroll_sync_manager::ScrollSyncManager;
use crate::managers::scroll_sync_manager::TimeoutId;
use crate::oracle_testing::assert_json_eq;
use crate::oracle_testing::load_golden;

#[derive(Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ElementPatch {
    tag: Option<String>,
    children: Option<Vec<String>>,
    attrs: Option<HashMap<String, String>>,
    height: Option<f64>,
    connected: Option<bool>,
    scroll_left: Option<f64>,
    dataset: Option<HashMap<String, String>>,
}

#[derive(Deserialize)]
struct WorldPatch {
    elements: BTreeMap<String, ElementPatch>,
}

#[derive(Deserialize)]
struct EntryJson {
    target: String,
    #[serde(rename = "blockSize")]
    block_size: f64,
    #[serde(rename = "inlineSize")]
    inline_size: f64,
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "camelCase")]
enum Op {
    #[serde(rename_all = "camelCase")]
    Setup {
        manager: usize,
        pre: String,
        #[serde(default)]
        disable_annotations: bool,
        column_variables: Option<ResizeManagerColumnVariableMode>,
        deletions: Option<String>,
        additions: Option<String>,
    },
    CleanUp {
        manager: usize,
    },
    World {
        set: WorldPatch,
    },
    Resize {
        entries: Vec<EntryJson>,
    },
    ScrollEvent {
        target: String,
    },
    FireTimers,
    State,
}

#[derive(Deserialize)]
struct Scenario {
    name: String,
    kind: String,
    world: WorldPatch,
    managers: Vec<u8>,
    steps: Vec<Op>,
}

#[derive(Deserialize)]
struct Case {
    scenario: Scenario,
    trace: Value,
}

#[derive(Deserialize)]
struct Golden {
    managers: Vec<Case>,
}

#[derive(Default)]
struct DomElement {
    tag: String,
    children: Vec<String>,
    attrs: HashMap<String, String>,
    height: f64,
    connected: bool,
    scroll_left: f64,
    dataset: HashMap<String, String>,
}

type PendingTimer = (TimeoutId, Box<dyn FnOnce()>);

#[derive(Default)]
struct FakeDom {
    elements: RefCell<HashMap<String, DomElement>>,
    trace: RefCell<Vec<Value>>,
    observer_active: Cell<bool>,
    listeners: RefCell<HashMap<String, ScrollSide>>,
    timers: RefCell<Vec<PendingTimer>>,
    next_timer: Cell<TimeoutId>,
}

impl FakeDom {
    fn apply(&self, patch: &WorldPatch) {
        let mut elements = self.elements.borrow_mut();
        for (name, value) in &patch.elements {
            let slot = elements.entry(name.clone()).or_insert_with(|| DomElement {
                tag: "div".to_owned(),
                connected: true,
                ..DomElement::default()
            });
            if let Some(tag) = &value.tag {
                slot.tag.clone_from(tag);
            }
            if let Some(children) = &value.children {
                slot.children.clone_from(children);
            }
            if let Some(attrs) = &value.attrs {
                slot.attrs.clone_from(attrs);
            }
            if let Some(height) = value.height {
                slot.height = height;
            }
            if let Some(connected) = value.connected {
                slot.connected = connected;
            }
            if let Some(scroll_left) = value.scroll_left {
                slot.scroll_left = scroll_left;
            }
            if let Some(dataset) = &value.dataset {
                slot.dataset.clone_from(dataset);
            }
        }
    }

    fn rec(&self, entry: Value) {
        self.trace.borrow_mut().push(entry);
    }

    fn annotation_descendants(&self, name: &str, found: &mut Vec<String>) {
        let children = self.elements.borrow()[name].children.clone();
        for child in children {
            let has_comma = self.elements.borrow()[&child]
                .attrs
                .get("data-line-annotation")
                .is_some_and(|value| value.contains(','));
            if has_comma {
                found.push(child.clone());
            }
            self.annotation_descendants(&child, found);
        }
    }
}

impl ResizeManagerHost for FakeDom {
    type Element = String;

    fn children(&self, pre: &String) -> Vec<String> {
        self.elements.borrow()[pre].children.clone()
    }

    fn tag_name(&self, element: &String) -> String {
        self.elements.borrow()[element].tag.to_uppercase()
    }

    fn first_element_child(&self, element: &String) -> Option<String> {
        self.elements.borrow()[element].children.first().cloned()
    }

    fn query_annotation_elements(&self, pre: &String) -> Vec<String> {
        let mut found = Vec::new();
        self.annotation_descendants(pre, &mut found);
        found
    }

    fn get_attribute(&self, element: &String, name: &str) -> Option<String> {
        self.elements.borrow()[element].attrs.get(name).cloned()
    }

    fn bounding_height(&self, element: &String) -> f64 {
        self.elements.borrow()[element].height
    }

    fn set_style_property(&self, element: &String, name: &str, value: &str) {
        self.rec(json!(["style.set", element, name, value]));
    }

    fn remove_style_property(&self, element: &String, name: &str) {
        self.rec(json!(["style.remove", element, name]));
    }

    fn is_connected(&self, element: &String) -> bool {
        self.elements.borrow()[element].connected
    }

    fn create_resize_observer(&self) {
        self.observer_active.set(true);
        self.rec(json!(["observer.create"]));
    }

    fn observe(&self, element: &String) {
        self.rec(json!(["observe", element]));
    }

    fn unobserve(&self, element: &String) {
        self.rec(json!(["unobserve", element]));
    }

    fn disconnect_resize_observer(&self) {
        self.observer_active.set(false);
        self.rec(json!(["observer.disconnect"]));
    }

    fn report(&self, message: &str, detail: Option<&str>) {
        self.rec(json!(["report", message, detail]));
    }
}

impl ScrollSyncHost for FakeDom {
    type Element = String;

    fn children(&self, pre: &String) -> Vec<String> {
        self.elements.borrow()[pre].children.clone()
    }

    fn dataset_has(&self, element: &String, key: &str) -> bool {
        self.elements.borrow()[element].dataset.contains_key(key)
    }

    fn add_scroll_listener(&self, element: &String, side: ScrollSide) {
        self.rec(json!(["listener.add", element, "scroll"]));
        self.listeners.borrow_mut().insert(element.clone(), side);
    }

    fn remove_scroll_listener(&self, element: &String, _side: ScrollSide) {
        self.rec(json!(["listener.remove", element, "scroll"]));
        self.listeners.borrow_mut().remove(element);
    }

    fn scroll_left(&self, element: &String) -> f64 {
        self.elements.borrow()[element].scroll_left
    }

    fn scroll_to_left(&self, element: &String, left: Option<f64>) {
        self.rec(json!(["scrollTo", element, left]));
    }

    fn set_timeout(&self, callback: Box<dyn FnOnce()>, milliseconds: u32) -> TimeoutId {
        let id = self.next_timer.get() + 1;
        self.next_timer.set(id);
        self.rec(json!(["setTimeout", id, milliseconds]));
        self.timers.borrow_mut().push((id, callback));
        id
    }

    fn clear_timeout(&self, timeout_id: TimeoutId) {
        self.rec(json!(["clearTimeout", timeout_id]));
        self.timers.borrow_mut().retain(|(id, _)| *id != timeout_id);
    }
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

#[test]
fn managers_match_the_published_classes() {
    let golden: Golden = load_golden("s11a.json");
    assert!(golden.managers.len() > 100);
    for case in &golden.managers {
        let scenario = &case.scenario;
        let dom = Rc::new(FakeDom::default());
        dom.apply(&scenario.world);
        let hub = ResizeObserverHub::new(Rc::clone(&dom));
        let resize_managers: Vec<_> = if scenario.kind == "resize" {
            scenario
                .managers
                .iter()
                .map(|_| ResizeManager::new(Rc::clone(&dom), Rc::clone(&hub)))
                .collect()
        } else {
            Vec::new()
        };
        let sync_manager =
            (scenario.kind != "resize").then(|| ScrollSyncManager::new(Rc::clone(&dom)));

        for step in &scenario.steps {
            let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| match step {
                Op::Setup {
                    manager,
                    pre,
                    disable_annotations,
                    column_variables,
                    deletions,
                    additions,
                } => {
                    if scenario.kind == "resize" {
                        resize_managers[*manager].setup(
                            pre,
                            ResizeManagerSetupOptions {
                                disable_annotations: *disable_annotations,
                                column_variables: column_variables.unwrap_or_default(),
                            },
                        );
                    } else {
                        sync_manager.as_ref().expect("scroll sync scenario").setup(
                            pre,
                            deletions.clone(),
                            additions.clone(),
                        );
                    }
                }
                Op::CleanUp { manager } => {
                    if scenario.kind == "resize" {
                        resize_managers[*manager].clean_up();
                    } else {
                        sync_manager
                            .as_ref()
                            .expect("scroll sync scenario")
                            .clean_up();
                    }
                }
                Op::World { set } => dom.apply(set),
                Op::Resize { entries } => {
                    if dom.observer_active.get() {
                        let entries: Vec<_> = entries
                            .iter()
                            .map(|entry| ResizeObserverEntry {
                                target: entry.target.clone(),
                                border_box_block_size: entry.block_size,
                                content_box_inline_size: entry.inline_size,
                            })
                            .collect();
                        hub.handle_shared_resize_entries(&entries);
                    }
                }
                Op::ScrollEvent { target } => {
                    let side = dom.listeners.borrow().get(target).copied();
                    let manager = sync_manager.as_ref().expect("scroll sync scenario");
                    match side {
                        Some(ScrollSide::Deletions) => manager.handle_deletions_scroll(),
                        Some(ScrollSide::Additions) => manager.handle_additions_scroll(),
                        None => {}
                    }
                }
                Op::FireTimers => {
                    let pending = std::mem::take(&mut *dom.timers.borrow_mut());
                    for (_, callback) in pending {
                        callback();
                    }
                }
                Op::State => {
                    let manager = sync_manager.as_ref().expect("scroll sync scenario");
                    dom.rec(json!([
                        "state",
                        manager.is_deletions_scrolling(),
                        manager.is_additions_scrolling()
                    ]));
                }
            }));
            if let Err(payload) = outcome {
                dom.rec(json!(["throws", panic_message(payload.as_ref())]));
            }
        }
        assert_json_eq(
            &format!("scenario `{}`", scenario.name),
            &case.trace,
            &Value::Array(dom.trace.borrow().clone()),
        );
    }
}
