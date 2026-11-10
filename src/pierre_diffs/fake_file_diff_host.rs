//! The rest of what `FileDiff` asks of its platform, over `FakeDom`
//! (S12a-2a): the resize observer, scroll listeners and timers record the same
//! vocabulary as the S11a world, declared heights default to 0, every element
//! is connected. With the interaction half in
//! `fake_interaction_dom.rs`, `FakeDom` is a `FileDiffHost`. Test-only,
//! original to this crate.

use serde_json::json;

use crate::document::Document;
use crate::document::NodeKey;
use crate::fake_interaction_dom::FakeDom;
use crate::managers::resize_manager::ResizeManagerHost;
use crate::managers::scroll_sync_manager::ScrollSide;
use crate::managers::scroll_sync_manager::ScrollSyncHost;
use crate::managers::scroll_sync_manager::TimeoutId;

impl ResizeManagerHost for FakeDom {
    fn document(&self) -> &Document {
        &self.doc
    }

    fn bounding_height(&self, element: NodeKey) -> f64 {
        self.heights.borrow().get(&element).copied().unwrap_or(0.0)
    }

    fn is_connected(&self, _element: NodeKey) -> bool {
        true
    }

    fn create_resize_observer(&self) {
        self.emit(json!(["observer.create"]));
    }

    fn observe(&self, element: NodeKey) {
        self.emit(json!(["observe", self.name_of(element)]));
    }

    fn unobserve(&self, element: NodeKey) {
        self.emit(json!(["unobserve", self.name_of(element)]));
    }

    fn disconnect_resize_observer(&self) {
        self.emit(json!(["observer.disconnect"]));
    }

    fn report(&self, message: &str, detail: Option<&str>) {
        self.emit(json!(["report", message, detail]));
    }
}

impl ScrollSyncHost for FakeDom {
    fn document(&self) -> &Document {
        &self.doc
    }

    fn add_scroll_listener(&self, element: NodeKey, _side: ScrollSide) {
        self.emit(json!(["listener.add", self.name_of(element), "scroll"]));
    }

    fn remove_scroll_listener(&self, element: NodeKey, _side: ScrollSide) {
        self.emit(json!(["listener.remove", self.name_of(element), "scroll"]));
    }

    fn scroll_left(&self, _element: NodeKey) -> f64 {
        0.0
    }

    fn scroll_to_left(&self, element: NodeKey, left: Option<f64>) {
        self.emit(json!(["scrollTo", self.name_of(element), left]));
    }

    // Timers never fire here; the trace records their registration.
    fn set_timeout(&self, _callback: Box<dyn FnOnce()>, milliseconds: u32) -> TimeoutId {
        let id = self.next_timer.get() + 1;
        self.next_timer.set(id);
        self.emit(json!(["setTimeout", id, milliseconds]));
        id
    }

    fn clear_timeout(&self, timeout_id: TimeoutId) {
        self.emit(json!(["clearTimeout", timeout_id]));
    }
}

/// No container is connected, so the scrollbar gutter is never measured and the
/// theme CSS carries `--diffs-scrollbar-gutter-fallback`.
impl crate::utils::dom::scrollbar_gutter::ScrollbarGutterHost for FakeDom {
    fn is_host_connected(&self) -> bool {
        false
    }

    fn measure_probe(&self, _probe: &crate::hast::Element) -> f64 {
        unreachable!("an unconnected host is never measured")
    }
}
