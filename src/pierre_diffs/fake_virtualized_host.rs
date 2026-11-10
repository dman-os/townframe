//! The `VirtualizerHost` half of `FakeDom` (S12b-2): declared scroll geometry
//! for the queries `VirtualizedFileDiff` makes through its `Virtualizer`
//! (`getOffsetInScrollContainer`: the window's scroll position and an element's
//! top), and the element heights `FakeDom::heights` already declares. The
//! window is the scroll root (`Virtualizer::setup` is not called: observers,
//! listeners and scroll writes are S12b-3's and panic here). Test-only,
//! original to this crate.

use std::collections::HashMap;

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
        todo!("S12b-3: an element scroll root")
    }

    fn element_scroll_height(&self, _element: &NodeKey) -> f64 {
        todo!("S12b-3: an element scroll root")
    }

    fn element_bounding_height(&self, element: &NodeKey) -> f64 {
        self.heights.borrow().get(element).copied().unwrap_or(0.0)
    }

    fn element_bounding_top(&self, element: &NodeKey) -> f64 {
        let world = self.world.borrow();
        world.doc_tops.get(element).copied().unwrap_or(0.0) - world.scroll_top
    }

    fn element_offset_height(&self, element: &NodeKey) -> f64 {
        self.element_bounding_height(element)
    }

    fn window_scroll_to(&self, _top: f64, _behavior: ScrollBehavior) {
        todo!("S12b-3: scroll writes")
    }

    fn element_scroll_to(&self, _element: &NodeKey, _top: f64, _behavior: ScrollBehavior) {
        todo!("S12b-3: scroll writes")
    }

    fn create_observers(&self, _intersection: IntersectionObserverInit<NodeKey>) {
        todo!("S12b-3: observers")
    }

    fn disconnect_observers(&self) {
        todo!("S12b-3: observers")
    }

    fn observe_resize(&self, _element: &NodeKey) {
        todo!("S12b-3: observers")
    }

    fn observe_document_resize(&self) {
        todo!("S12b-3: observers")
    }

    fn observe_intersection(&self, _element: &NodeKey) {
        todo!("S12b-3: observers")
    }

    fn unobserve_intersection(&self, _element: &NodeKey) {
        todo!("S12b-3: observers")
    }

    fn add_window_listeners(&self) {
        todo!("S12b-3: listeners")
    }

    fn add_element_scroll_listener(&self, _root: &NodeKey) {
        todo!("S12b-3: listeners")
    }

    fn remove_listeners(&self, _root: Option<&Root<NodeKey>>) {
        todo!("S12b-3: listeners")
    }

    fn console_log(
        &self,
        _message: &str,
        _virtualizer_id: &str,
        _size_change: f64,
        _new_size: f64,
    ) {
        todo!("S12b-3: resize debugging logs")
    }
}
