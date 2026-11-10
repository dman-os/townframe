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

use crate::components::virtualizer::AnchorLine;
use crate::components::virtualizer::IntersectionObserverInit;
use crate::components::virtualizer::Root;
use crate::components::virtualizer::ScrollBehavior;
use crate::components::virtualizer::VirtualizerHost;
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

#[derive(Debug, Clone, Default)]
struct FakeElement {
    doc_top: f64,
    height: f64,
    lines: Vec<(String, f64)>,
    first_child: Option<String>,
}

#[derive(Default)]
struct World {
    scroll_top: f64,
    viewport_height: f64,
    scroll_height: f64,
    root_top: f64,
    elements: HashMap<String, FakeElement>,
}

/// The name of the scroll container element in the fake world.
pub const ROOT: &str = "root";

#[derive(Default)]
pub struct FakeHost {
    pub frames: FakeFrames,
    world: RefCell<World>,
    trace: RefCell<Vec<Value>>,
    element_scroll_listener: RefCell<Option<String>>,
    window_scroll_listener: Cell<bool>,
    window_resize_listener: Cell<bool>,
    /// Whether the active root is the document (the fake window).
    document_root: Cell<bool>,
    observers_active: Cell<bool>,
}

impl FakeHost {
    pub fn new(document_root: bool) -> Self {
        let host = Self::default();
        host.document_root.set(document_root);
        host
    }

    pub fn apply_world(&self, patch: &WorldPatch) {
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
        for (name, element) in &patch.elements {
            let slot = world.elements.entry(name.clone()).or_default();
            if let Some(value) = element.doc_top {
                slot.doc_top = value;
            }
            if let Some(value) = element.height {
                slot.height = value;
            }
            if let Some(value) = &element.lines {
                slot.lines = value.clone();
            }
            if let Some(value) = &element.first_child {
                slot.first_child = Some(value.clone());
            }
        }
    }

    pub fn record(&self, entry: Value) {
        self.trace.borrow_mut().push(entry);
    }

    pub fn take_trace(&self) -> Vec<Value> {
        std::mem::take(&mut *self.trace.borrow_mut())
    }

    pub fn element_scroll_listener(&self) -> Option<String> {
        self.element_scroll_listener.borrow().clone()
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
    type Element = String;

    fn window_scroll_y(&self) -> f64 {
        self.world.borrow().scroll_top
    }

    fn window_inner_height(&self) -> f64 {
        self.world.borrow().viewport_height
    }

    fn document_scroll_height(&self) -> f64 {
        self.world.borrow().scroll_height
    }

    fn element_scroll_top(&self, _element: &String) -> f64 {
        self.world.borrow().scroll_top
    }

    fn element_scroll_height(&self, _element: &String) -> f64 {
        self.world.borrow().scroll_height
    }

    fn element_bounding_height(&self, element: &String) -> f64 {
        let world = self.world.borrow();
        if element == ROOT {
            world.viewport_height
        } else {
            world.elements[element].height
        }
    }

    fn element_bounding_top(&self, element: &String) -> f64 {
        let world = self.world.borrow();
        let root_top = if self.document_root.get() {
            0.0
        } else {
            world.root_top
        };
        if element == ROOT {
            return root_top;
        }
        // A line is named `file#lineIndex` (see `anchor_lines`).
        let doc_top = match element.split_once('#') {
            Some((file, line_index)) => {
                world.elements[file]
                    .lines
                    .iter()
                    .find(|(index, _)| index == line_index)
                    .unwrap_or_else(|| panic!("no line {element}"))
                    .1
            }
            None => world.elements[element].doc_top,
        };
        doc_top - world.scroll_top + root_top
    }

    fn element_offset_height(&self, element: &String) -> f64 {
        self.world.borrow().elements[element].height
    }

    fn first_element_child(&self, element: &String) -> Option<String> {
        self.world
            .borrow()
            .elements
            .get(element)
            .and_then(|element| element.first_child.clone())
    }

    fn window_scroll_to(&self, top: f64, behavior: ScrollBehavior) {
        self.record(json!([
            "window.scrollTo",
            top,
            Self::behavior_name(behavior)
        ]));
        self.scroll_to_world(top);
    }

    fn element_scroll_to(&self, element: &String, top: f64, behavior: ScrollBehavior) {
        self.record(json!([
            "element.scrollTo",
            element,
            top,
            Self::behavior_name(behavior)
        ]));
        self.scroll_to_world(top);
    }

    fn anchor_lines(&self, file_element: &String) -> impl Iterator<Item = AnchorLine<String>> {
        let lines = self
            .world
            .borrow()
            .elements
            .get(file_element)
            .map(|element| element.lines.clone())
            .unwrap_or_default();
        let file_element = file_element.clone();
        lines.into_iter().map(move |(line_index, _)| AnchorLine {
            element: format!("{file_element}#{line_index}"),
            line_index,
        })
    }

    fn find_anchor_line(&self, file_element: &String, line_index: &str) -> Option<String> {
        self.world
            .borrow()
            .elements
            .get(file_element)?
            .lines
            .iter()
            .any(|(index, _)| index == line_index)
            .then(|| format!("{file_element}#{line_index}"))
    }

    fn create_observers(&self, intersection: IntersectionObserverInit<String>) {
        self.observers_active.set(true);
        let root = match &intersection.root {
            Root::Document => "document".to_owned(),
            Root::Element(element) => element.clone(),
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

    fn observe_resize(&self, element: &String) {
        self.record(json!(["resize.observe", element]));
    }

    fn observe_document_resize(&self) {
        self.record(json!(["resize.observe", "documentElement"]));
    }

    fn observe_intersection(&self, element: &String) {
        self.record(json!(["intersection.observe", element]));
    }

    fn unobserve_intersection(&self, element: &String) {
        self.record(json!(["intersection.unobserve", element]));
    }

    fn add_window_listeners(&self) {
        self.window_scroll_listener.set(true);
        self.window_resize_listener.set(true);
        self.record(json!(["window.addEventListener", "scroll"]));
        self.record(json!(["window.addEventListener", "resize"]));
    }

    fn add_element_scroll_listener(&self, root: &String) {
        *self.element_scroll_listener.borrow_mut() = Some(root.clone());
        self.record(json!(["element.addEventListener", root, "scroll"]));
    }

    fn remove_listeners(&self, root: Option<&Root<String>>) {
        if let Some(Root::Element(element)) = root {
            self.record(json!(["element.removeEventListener", element, "scroll"]));
        }
        *self.element_scroll_listener.borrow_mut() = None;
        self.window_scroll_listener.set(false);
        self.window_resize_listener.set(false);
        self.record(json!(["window.removeEventListener", "scroll"]));
        self.record(json!(["window.removeEventListener", "resize"]));
    }

    fn console_log(&self, message: &str, _virtualizer_id: &str, size_change: f64, new_size: f64) {
        self.record(json!(["log", message, size_change, new_size]));
    }
}
