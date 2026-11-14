//! A scripted fake of the DOM facts `Virtualizer` reads, test-only and
//! original to this crate. It stands in for jsdom (which upstream's tests use
//! and which performs no layout either): geometry is declared, not computed.
//! The oracle's JavaScript counterpart (`oracle/generate.mjs`, `fakeDom`)
//! implements the same world with the same trace vocabulary, so one scenario
//! runs against the published class and against the port.

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::HashMap;

use serde::Deserialize;
use serde_json::Value;
use serde_json::json;

use crate::components::virtualizer::IntersectionObserverInit;
use crate::components::virtualizer::Root;
use crate::components::virtualizer::ScrollBehavior;
use crate::components::virtualizer::VirtualizerHost;
use crate::document::Document;
use crate::document::NodeKey;
use crate::managers::universal_rendering_manager::AnimationFrames;
use crate::managers::universal_rendering_manager::FrameId;

/// `requestAnimationFrame` that only runs frames the test asks for (upstream's
/// `installFrameHarness`).
type FrameCallback = Box<dyn FnOnce(f64)>;

#[derive(Default)]
pub struct FakeFrames {
    next_frame_id: Cell<FrameId>,
    frames: RefCell<BTreeMap<FrameId, FrameCallback>>,
    cancelled_frame_ids: RefCell<Vec<FrameId>>,
}

impl FakeFrames {
    pub fn frame_ids(&self) -> Vec<FrameId> {
        self.frames.borrow().keys().copied().collect()
    }

    pub fn cancelled_frame_ids(&self) -> Vec<FrameId> {
        self.cancelled_frame_ids.borrow().clone()
    }

    pub fn run_frame(&self, frame_id: FrameId, time: f64) {
        let callback = self
            .frames
            .borrow_mut()
            .remove(&frame_id)
            .unwrap_or_else(|| panic!("frame {frame_id} is not pending"));
        callback(time);
    }

    /// Runs the frames pending now (not the ones they request) with `time`;
    /// false when there were none.
    pub fn run_round(&self, time: f64) -> bool {
        let pending = std::mem::take(&mut *self.frames.borrow_mut());
        let ran = !pending.is_empty();
        for (_, callback) in pending {
            callback(time);
        }
        ran
    }

    /// Runs rounds until none is pending, up to `limit`, like the
    /// `flushFrames` of upstream's offscreen-placeholder tests (16 ms apart).
    /// Returns the rounds run.
    pub fn flush_frames(&self, limit: usize, start_time: f64) -> usize {
        let mut rounds = 0;
        while rounds < limit && self.run_round(start_time + 16.0 * (rounds as f64 + 1.0)) {
            rounds += 1;
        }
        rounds
    }
}

impl AnimationFrames for FakeFrames {
    fn request_animation_frame(&self, callback: Box<dyn FnOnce(f64)>) -> FrameId {
        let frame_id = self.next_frame_id.get() + 1;
        self.next_frame_id.set(frame_id);
        self.frames.borrow_mut().insert(frame_id, callback);
        frame_id
    }

    fn cancel_animation_frame(&self, frame_id: FrameId) {
        self.cancelled_frame_ids.borrow_mut().push(frame_id);
        self.frames.borrow_mut().remove(&frame_id);
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ElementPatch {
    pub doc_top: Option<f64>,
    pub height: Option<f64>,
    /// `[lineIndex, docTop]` pairs in document order.
    pub lines: Option<Vec<(String, f64)>>,
    /// The element's `firstElementChild`.
    pub first_child: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorldPatch {
    pub scroll_top: Option<f64>,
    pub viewport_height: Option<f64>,
    pub scroll_height: Option<f64>,
    pub root_top: Option<f64>,
    #[serde(default)]
    pub elements: BTreeMap<String, ElementPatch>,
}

#[derive(Default)]
struct World {
    scroll_top: f64,
    viewport_height: f64,
    scroll_height: f64,
    root_top: f64,
    doc_tops: HashMap<NodeKey, f64>,
    heights: HashMap<NodeKey, f64>,
}

/// The name of the scroll container element in the fake world.
pub const ROOT: &str = "root";

pub struct FakeHost {
    pub frames: FakeFrames,
    doc: Document,
    by_name: RefCell<HashMap<String, NodeKey>>,
    labels: RefCell<HashMap<NodeKey, String>>,
    world: RefCell<World>,
    trace: RefCell<Vec<Value>>,
    element_scroll_listener: Cell<Option<NodeKey>>,
    window_scroll_listener: Cell<bool>,
    window_resize_listener: Cell<bool>,
    /// Whether the active root is the document (the fake window).
    document_root: Cell<bool>,
    observers_active: Cell<bool>,
}

impl Default for FakeHost {
    fn default() -> Self {
        Self {
            frames: FakeFrames::default(),
            doc: Document::new(),
            by_name: RefCell::default(),
            labels: RefCell::default(),
            world: RefCell::default(),
            trace: RefCell::default(),
            element_scroll_listener: Cell::new(None),
            window_scroll_listener: Cell::new(false),
            window_resize_listener: Cell::new(false),
            document_root: Cell::new(false),
            observers_active: Cell::new(false),
        }
    }
}

impl FakeHost {
    pub fn new(document_root: bool) -> Self {
        let host = Self::default();
        host.document_root.set(document_root);
        host
    }

    /// The element named `name` (created on first use), as a key of the document.
    pub fn element(&self, name: &str) -> NodeKey {
        if let Some(key) = self.by_name.borrow().get(name) {
            return *key;
        }
        let key = self.doc.create_element("div");
        self.by_name.borrow_mut().insert(name.to_owned(), key);
        self.labels.borrow_mut().insert(key, name.to_owned());
        key
    }

    fn name_of(&self, key: NodeKey) -> String {
        self.labels.borrow()[&key].clone()
    }

    pub fn apply_world(&self, patch: &WorldPatch) {
        {
            let mut world = self.world.borrow_mut();
            if let Some(value) = patch.scroll_top {
                world.scroll_top = value;
            }
            if let Some(value) = patch.viewport_height {
                world.viewport_height = value;
            }
            if let Some(value) = patch.scroll_height {
                world.scroll_height = value;
            }
            if let Some(value) = patch.root_top {
                world.root_top = value;
            }
        }
        for (name, element) in &patch.elements {
            let key = self.element(name);
            if let Some(value) = element.doc_top {
                self.world.borrow_mut().doc_tops.insert(key, value);
            }
            if let Some(value) = element.height {
                self.world.borrow_mut().heights.insert(key, value);
            }
            if let Some(lines) = &element.lines {
                let line_keys: Vec<NodeKey> = lines
                    .iter()
                    .map(|(line_index, doc_top)| {
                        let line = self.doc.create_element("div");
                        self.doc.set_attribute(line, "data-line", "");
                        self.doc.set_attribute(line, "data-line-index", line_index);
                        self.labels
                            .borrow_mut()
                            .insert(line, format!("{name}#{line_index}"));
                        self.world.borrow_mut().doc_tops.insert(line, *doc_top);
                        line
                    })
                    .collect();
                self.doc.replace_children(key, &line_keys);
            }
            if let Some(child) = &element.first_child {
                let child = self.element(child);
                self.doc.replace_children(key, &[child]);
            }
        }
    }

    pub fn record(&self, entry: Value) {
        self.trace.borrow_mut().push(entry);
    }

    pub fn take_trace(&self) -> Vec<Value> {
        std::mem::take(&mut *self.trace.borrow_mut())
    }

    pub fn element_scroll_listener(&self) -> Option<NodeKey> {
        self.element_scroll_listener.get()
    }

    pub fn window_scroll_listener(&self) -> bool {
        self.window_scroll_listener.get()
    }

    pub fn window_resize_listener(&self) -> bool {
        self.window_resize_listener.get()
    }

    /// Between `create_observers` and `disconnect_observers`.
    pub fn observers_active(&self) -> bool {
        self.observers_active.get()
    }

    // The browser clamps programmatic scrolls to the scrollable range.
    fn scroll_to_world(&self, top: f64) {
        let mut world = self.world.borrow_mut();
        let max = (world.scroll_height - world.viewport_height).max(0.0);
        world.scroll_top = top.clamp(0.0, max);
    }

    fn behavior_name(behavior: ScrollBehavior) -> &'static str {
        match behavior {
            ScrollBehavior::Auto => "auto",
            ScrollBehavior::Instant => "instant",
            ScrollBehavior::Smooth => "smooth",
        }
    }
}

impl AnimationFrames for FakeHost {
    fn request_animation_frame(&self, callback: Box<dyn FnOnce(f64)>) -> FrameId {
        let frame_id = self.frames.request_animation_frame(callback);
        self.record(json!(["raf.request", frame_id]));
        frame_id
    }

    fn cancel_animation_frame(&self, frame_id: FrameId) {
        self.frames.cancel_animation_frame(frame_id);
        self.record(json!(["raf.cancel", frame_id]));
    }
}

impl VirtualizerHost for FakeHost {
    fn document(&self) -> &Document {
        &self.doc
    }

    fn window_scroll_y(&self) -> f64 {
        self.world.borrow().scroll_top
    }

    fn window_inner_height(&self) -> f64 {
        self.world.borrow().viewport_height
    }

    fn document_scroll_height(&self) -> f64 {
        self.world.borrow().scroll_height
    }

    fn element_scroll_top(&self, _element: &NodeKey) -> f64 {
        self.world.borrow().scroll_top
    }

    fn element_scroll_height(&self, _element: &NodeKey) -> f64 {
        self.world.borrow().scroll_height
    }

    fn element_bounding_height(&self, element: &NodeKey) -> f64 {
        let world = self.world.borrow();
        if *element == self.element(ROOT) {
            world.viewport_height
        } else {
            world.heights.get(element).copied().unwrap_or(0.0)
        }
    }

    fn element_bounding_top(&self, element: &NodeKey) -> f64 {
        let world = self.world.borrow();
        let root_top = if self.document_root.get() {
            0.0
        } else {
            world.root_top
        };
        if *element == self.element(ROOT) {
            return root_top;
        }
        world.doc_tops.get(element).copied().unwrap_or(0.0) - world.scroll_top + root_top
    }

    fn element_offset_height(&self, element: &NodeKey) -> f64 {
        self.world
            .borrow()
            .heights
            .get(element)
            .copied()
            .unwrap_or(0.0)
    }

    fn window_scroll_to(&self, top: f64, behavior: ScrollBehavior) {
        self.record(json!([
            "window.scrollTo",
            top,
            Self::behavior_name(behavior)
        ]));
        self.scroll_to_world(top);
    }

    fn element_scroll_to(&self, element: &NodeKey, top: f64, behavior: ScrollBehavior) {
        self.record(json!([
            "element.scrollTo",
            self.name_of(*element),
            top,
            Self::behavior_name(behavior)
        ]));
        self.scroll_to_world(top);
    }

    fn create_observers(&self, intersection: IntersectionObserverInit<NodeKey>) {
        self.observers_active.set(true);
        let root = match &intersection.root {
            Root::Document => "document".to_owned(),
            Root::Element(element) => self.name_of(*element),
        };
        self.record(json!([
            "observers.create",
            {
                "root": root,
                "threshold": intersection.threshold,
                "rootMargin": format!(
                    "{margin}px 0px {margin}px 0px",
                    margin = intersection.root_margin
                ),
            }
        ]));
    }

    fn disconnect_observers(&self) {
        self.observers_active.set(false);
        self.record(json!(["resize.disconnect"]));
        self.record(json!(["intersection.disconnect"]));
    }

    fn observe_resize(&self, element: &NodeKey) {
        self.record(json!(["resize.observe", self.name_of(*element)]));
    }

    fn observe_document_resize(&self) {
        self.record(json!(["resize.observe", "documentElement"]));
    }

    fn observe_intersection(&self, element: &NodeKey) {
        self.record(json!(["intersection.observe", self.name_of(*element)]));
    }

    fn unobserve_intersection(&self, element: &NodeKey) {
        self.record(json!(["intersection.unobserve", self.name_of(*element)]));
    }

    fn add_window_listeners(&self) {
        self.window_scroll_listener.set(true);
        self.window_resize_listener.set(true);
        self.record(json!(["window.addEventListener", "scroll"]));
        self.record(json!(["window.addEventListener", "resize"]));
    }

    fn add_element_scroll_listener(&self, root: &NodeKey) {
        self.element_scroll_listener.set(Some(*root));
        self.record(json!([
            "element.addEventListener",
            self.name_of(*root),
            "scroll"
        ]));
    }

    fn remove_listeners(&self, root: Option<&Root<NodeKey>>) {
        if let Some(Root::Element(element)) = root {
            self.record(json!([
                "element.removeEventListener",
                self.name_of(*element),
                "scroll"
            ]));
        }
        self.element_scroll_listener.set(None);
        self.window_scroll_listener.set(false);
        self.window_resize_listener.set(false);
        self.record(json!(["window.removeEventListener", "scroll"]));
        self.record(json!(["window.removeEventListener", "resize"]));
    }

    fn console_log(&self, message: &str, _virtualizer_id: &str, size_change: f64, new_size: f64) {
        self.record(json!(["log", message, size_change, new_size]));
    }
}
