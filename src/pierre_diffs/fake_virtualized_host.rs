//! The `VirtualizerHost` half of `FakeDom` (S12b-2, completed in S12b-3):
//! declared scroll geometry for the queries `VirtualizedFileDiff` makes through
//! its `Virtualizer`, observers, listeners and scroll writes that record into
//! the trace in the vocabulary of `fake_dom::FakeHost` (so the S10 oracle's
//! JavaScript fake and the integration oracle's world speak the same one).
//! Test-only, original to this crate.
//!
//! Two geometries. Declared: `doc_tops` and `FakeDom::heights` per node (the
//! S12b-2 oracle, whose DOM is declared too). Rules (`VirtualWorld::rules`, the
//! integration oracle, whose DOM the port renders): the heights and tops are a
//! function of the element, so both sides of the oracle can compute them for
//! nodes neither declared. The rules are `rule_height` and `rule_top`; the
//! JavaScript side (`s12b_virtualized.mjs`) has the same functions.

use std::collections::HashMap;

use serde_json::json;

use crate::components::virtualizer::IntersectionObserverInit;
use crate::components::virtualizer::Root;
use crate::components::virtualizer::ScrollBehavior;
use crate::components::virtualizer::VirtualizerHost;
use crate::document::Document;
use crate::document::NodeKey;
use crate::fake_interaction_dom::FakeDom;

/// Declared geometry: the window's scroll position and size, and each
/// element's top in the document.
#[derive(Default)]
pub struct VirtualWorld {
    pub scroll_top: f64,
    pub viewport_height: f64,
    pub scroll_height: f64,
    pub doc_tops: HashMap<NodeKey, f64>,
    /// Compute heights and tops by the rules instead of the declared maps.
    pub rules: bool,
    /// The top and height of the `diffs-container` under the rules.
    pub container_top: f64,
    pub container_height: f64,
}

/// The leading integer of a `data-line-index` (`"3,4"` is 3): JavaScript's
/// `parseInt`.
fn leading_index(line_index: &str) -> i64 {
    let digits: String = line_index
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    digits.parse().unwrap_or(0)
}

/// `rule_height` of the integration oracle: the container is declared, a line
/// row is 20 (40 when its index is a multiple of 5: a wrapped line), an
/// annotation row 30, a no-newline metadata row 16, anything else 0.
fn rule_height(document: &Document, world: &VirtualWorld, element: NodeKey) -> f64 {
    if document
        .tag_name(element)
        .eq_ignore_ascii_case("diffs-container")
    {
        return world.container_height;
    }
    if document.has_attribute(element, "data-line-annotation") {
        return 30.0;
    }
    if document.has_attribute(element, "data-no-newline") {
        return 16.0;
    }
    match document.get_attribute(element, "data-line-index") {
        Some(line_index) if leading_index(&line_index) % 5 == 0 => 40.0,
        Some(_) => 20.0,
        None => 0.0,
    }
}

/// `rule_top`: the container's declared top; a line row sits 40 below the
/// container plus 20 per line index; anything else is at the container's top.
fn rule_top(document: &Document, world: &VirtualWorld, element: NodeKey) -> f64 {
    if document
        .tag_name(element)
        .eq_ignore_ascii_case("diffs-container")
    {
        return world.container_top;
    }
    match document.get_attribute(element, "data-line-index") {
        Some(line_index) => world.container_top + 40.0 + 20.0 * leading_index(&line_index) as f64,
        None => world.container_top,
    }
}

impl FakeDom {
    fn behavior_name(behavior: ScrollBehavior) -> &'static str {
        match behavior {
            ScrollBehavior::Auto => "auto",
            ScrollBehavior::Instant => "instant",
            ScrollBehavior::Smooth => "smooth",
        }
    }

    // The browser clamps programmatic scrolls to the scrollable range.
    fn scroll_world_to(&self, top: f64) {
        let mut world = self.world.borrow_mut();
        let max = (world.scroll_height - world.viewport_height).max(0.0);
        world.scroll_top = top.clamp(0.0, max);
    }

    fn name(&self, node: NodeKey) -> String {
        self.shared.name_of(&self.doc, node)
    }
}

impl VirtualizerHost for FakeDom {
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
        if world.rules {
            return rule_height(&self.doc, &world, *element);
        }
        self.heights.borrow().get(element).copied().unwrap_or(0.0)
    }

    fn element_bounding_top(&self, element: &NodeKey) -> f64 {
        let world = self.world.borrow();
        let doc_top = if world.rules {
            rule_top(&self.doc, &world, *element)
        } else {
            world.doc_tops.get(element).copied().unwrap_or(0.0)
        };
        doc_top - world.scroll_top
    }

    fn element_offset_height(&self, element: &NodeKey) -> f64 {
        self.element_bounding_height(element)
    }

    fn window_scroll_to(&self, top: f64, behavior: ScrollBehavior) {
        self.emit(json!([
            "window.scrollTo",
            top,
            Self::behavior_name(behavior)
        ]));
        self.scroll_world_to(top);
    }

    fn element_scroll_to(&self, element: &NodeKey, top: f64, behavior: ScrollBehavior) {
        self.emit(json!([
            "element.scrollTo",
            self.name(*element),
            top,
            Self::behavior_name(behavior)
        ]));
        self.scroll_world_to(top);
    }

    fn create_observers(&self, intersection: IntersectionObserverInit<NodeKey>) {
        let root = match &intersection.root {
            Root::Document => "document".to_owned(),
            Root::Element(element) => self.name(*element),
        };
        self.emit(json!([
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
        self.emit(json!(["resize.disconnect"]));
        self.emit(json!(["intersection.disconnect"]));
    }

    fn observe_resize(&self, element: &NodeKey) {
        self.emit(json!(["resize.observe", self.name(*element)]));
    }

    fn observe_document_resize(&self) {
        self.emit(json!(["resize.observe", "documentElement"]));
    }

    fn observe_intersection(&self, element: &NodeKey) {
        self.emit(json!(["intersection.observe", self.name(*element)]));
    }

    fn unobserve_intersection(&self, element: &NodeKey) {
        self.emit(json!(["intersection.unobserve", self.name(*element)]));
    }

    fn add_window_listeners(&self) {
        self.emit(json!(["window.addEventListener", "scroll"]));
        self.emit(json!(["window.addEventListener", "resize"]));
    }

    fn add_element_scroll_listener(&self, root: &NodeKey) {
        self.emit(json!([
            "element.addEventListener",
            self.name(*root),
            "scroll"
        ]));
    }

    fn remove_listeners(&self, root: Option<&Root<NodeKey>>) {
        if let Some(Root::Element(element)) = root {
            self.emit(json!([
                "element.removeEventListener",
                self.name(*element),
                "scroll"
            ]));
        }
        self.emit(json!(["window.removeEventListener", "scroll"]));
        self.emit(json!(["window.removeEventListener", "resize"]));
    }

    fn console_log(&self, message: &str, _virtualizer_id: &str, size_change: f64, new_size: f64) {
        self.emit(json!(["log", message, size_change, new_size]));
    }
}
