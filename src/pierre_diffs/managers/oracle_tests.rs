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

use crate::document::Document;
use crate::document::Mutation;
use crate::document::NodeKey;
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

type PendingTimer = (TimeoutId, Box<dyn FnOnce()>);

struct Shared {
    trace: RefCell<Vec<Value>>,
    names: RefCell<HashMap<NodeKey, String>>,
}

struct FakeDom {
    doc: Document,
    shared: Rc<Shared>,
    by_name: RefCell<HashMap<String, NodeKey>>,
    heights: RefCell<HashMap<NodeKey, f64>>,
    connected: RefCell<HashMap<NodeKey, bool>>,
    scroll_left: RefCell<HashMap<NodeKey, f64>>,
    observer_active: Cell<bool>,
    listeners: RefCell<HashMap<NodeKey, ScrollSide>>,
    timers: RefCell<Vec<PendingTimer>>,
    next_timer: Cell<TimeoutId>,
}

impl Default for FakeDom {
    fn default() -> Self {
        let doc = Document::new();
        let shared = Rc::new(Shared {
            trace: RefCell::default(),
            names: RefCell::default(),
        });
        let observed = Rc::clone(&shared);
        // Style writes are the only document edits the managers make; the
        // scenario's own edits are made silently (see `silent`).
        doc.set_observer(Some(Rc::new(move |_: &Document, mutation: &Mutation| {
            let name = |node: &NodeKey| observed.names.borrow()[node].clone();
            match mutation {
                Mutation::SetStyleProperty {
                    node,
                    name: property,
                    value,
                } => observed.trace.borrow_mut().push(json!([
                    "style.set",
                    name(node),
                    property,
                    value
                ])),
                Mutation::RemoveStyleProperty {
                    node,
                    name: property,
                } => {
                    observed
                        .trace
                        .borrow_mut()
                        .push(json!(["style.remove", name(node), property]))
                }
                _ => {}
            }
        })));
        Self {
            doc,
            shared,
            by_name: RefCell::default(),
            heights: RefCell::default(),
            connected: RefCell::default(),
            scroll_left: RefCell::default(),
            observer_active: Cell::new(false),
            listeners: RefCell::default(),
            timers: RefCell::default(),
            next_timer: Cell::new(0),
        }
    }
}

impl FakeDom {
    // The element named `name`, created (as a `div`) on first sight.
    fn element(&self, name: &str) -> NodeKey {
        if let Some(key) = self.by_name.borrow().get(name) {
            return *key;
        }
        let key = self.doc.create_element("div");
        self.by_name.borrow_mut().insert(name.to_owned(), key);
        self.shared.names.borrow_mut().insert(key, name.to_owned());
        self.connected.borrow_mut().insert(key, true);
        key
    }

    fn apply(&self, patch: &WorldPatch) {
        // First sight creates the element with its tag.
        for (name, value) in &patch.elements {
            if !self.by_name.borrow().contains_key(name) {
                let key = self
                    .doc
                    .create_element(value.tag.as_deref().unwrap_or("div"));
                self.by_name.borrow_mut().insert(name.clone(), key);
                self.shared.names.borrow_mut().insert(key, name.clone());
                self.connected.borrow_mut().insert(key, true);
            }
        }
        for (name, value) in &patch.elements {
            let key = self.element(name);
            if let Some(children) = &value.children {
                let keys: Vec<NodeKey> = children.iter().map(|child| self.element(child)).collect();
                self.doc.replace_children(key, &keys);
            }
            if let Some(attrs) = &value.attrs {
                self.doc.remove_attribute(key, "data-line-annotation");
                for (attribute, attribute_value) in attrs {
                    self.doc.set_attribute(key, attribute, attribute_value);
                }
            }
            if let Some(dataset) = &value.dataset {
                for (data_key, data_value) in dataset {
                    self.doc
                        .set_attribute(key, &format!("data-{data_key}"), data_value);
                }
            }
            if let Some(height) = value.height {
                self.heights.borrow_mut().insert(key, height);
            }
            if let Some(connected) = value.connected {
                self.connected.borrow_mut().insert(key, connected);
            }
            if let Some(scroll_left) = value.scroll_left {
                self.scroll_left.borrow_mut().insert(key, scroll_left);
            }
        }
    }

    fn rec(&self, entry: Value) {
        self.shared.trace.borrow_mut().push(entry);
    }

    fn name(&self, key: NodeKey) -> String {
        self.shared.names.borrow()[&key].clone()
    }
}

impl ResizeManagerHost for FakeDom {
    fn document(&self) -> &Document {
        &self.doc
    }

    fn bounding_height(&self, element: NodeKey) -> f64 {
        self.heights.borrow().get(&element).copied().unwrap_or(0.0)
    }

    fn is_connected(&self, element: NodeKey) -> bool {
        self.connected.borrow()[&element]
    }

    fn create_resize_observer(&self) {
        self.observer_active.set(true);
        self.rec(json!(["observer.create"]));
    }

    fn observe(&self, element: NodeKey) {
        self.rec(json!(["observe", self.name(element)]));
    }

    fn unobserve(&self, element: NodeKey) {
        self.rec(json!(["unobserve", self.name(element)]));
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
    fn document(&self) -> &Document {
        &self.doc
    }

    fn add_scroll_listener(&self, element: NodeKey, side: ScrollSide) {
        self.rec(json!(["listener.add", self.name(element), "scroll"]));
        self.listeners.borrow_mut().insert(element, side);
    }

    fn remove_scroll_listener(&self, element: NodeKey, _side: ScrollSide) {
        self.rec(json!(["listener.remove", self.name(element), "scroll"]));
        self.listeners.borrow_mut().remove(&element);
    }

    fn scroll_left(&self, element: NodeKey) -> f64 {
        self.scroll_left
            .borrow()
            .get(&element)
            .copied()
            .unwrap_or(0.0)
    }

    fn scroll_to_left(&self, element: NodeKey, left: Option<f64>) {
        self.rec(json!(["scrollTo", self.name(element), left]));
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
                            &dom.element(pre),
                            ResizeManagerSetupOptions {
                                disable_annotations: *disable_annotations,
                                column_variables: column_variables.unwrap_or_default(),
                            },
                        );
                    } else {
                        sync_manager.as_ref().expect("scroll sync scenario").setup(
                            &dom.element(pre),
                            deletions.as_ref().map(|name| dom.element(name)),
                            additions.as_ref().map(|name| dom.element(name)),
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
                                target: dom.element(&entry.target),
                                border_box_block_size: entry.block_size,
                                content_box_inline_size: entry.inline_size,
                            })
                            .collect();
                        hub.handle_shared_resize_entries(&entries);
                    }
                }
                Op::ScrollEvent { target } => {
                    let side = dom.listeners.borrow().get(&dom.element(target)).copied();
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
            &Value::Array(dom.shared.trace.borrow().clone()),
        );
    }
}
