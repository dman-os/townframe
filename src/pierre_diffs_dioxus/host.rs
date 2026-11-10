//! The Dioxus `FileDiffHost`: what `FileDiff` asks of its platform, answered for a static render.
//!
//! The structural half of every host trait (children, attributes, queries, style writes) is the
//! port's `Document`, which this host owns and the renderer in `render.rs` turns into Dioxus
//! elements. What is left is geometry, listeners, timers and frames, which only exist once the
//! rendered nodes are mounted. This step renders without virtualization, observers or
//! interaction, so each method below is one of:
//!
//! * answered for real (`document`, `is_connected`, `element_from_point`);
//! * a registration whose events cannot arrive yet, because the renderer attaches no handlers
//!   (`observe`, `add_scroll_listener`, `add_pre_listeners`...): accepting it and keeping nothing
//!   is correct until the interactive step renders `onresize`/`onscroll`/pointer handlers for
//!   the registered nodes, and that step stores the registrations here;
//! * a read of a measurement nothing has produced yet (`bounding_height`, the scrollbar gutter),
//!   answered with "not measured";
//! * `todo!` for what only a handler reaches (`scroll_left`, timers, frames), which a static
//!   render never calls (the host tests render both diff styles with annotations to hold that).

use pierre_diffs::document::Document;
use pierre_diffs::document::NodeKey;
use pierre_diffs::hast;
use pierre_diffs::managers::interaction_manager::HitTest;
use pierre_diffs::managers::interaction_manager::InteractionHost;
use pierre_diffs::managers::resize_manager::ResizeManagerHost;
use pierre_diffs::managers::scroll_sync_manager::ScrollSide;
use pierre_diffs::managers::scroll_sync_manager::ScrollSyncHost;
use pierre_diffs::managers::scroll_sync_manager::TimeoutId;
use pierre_diffs::managers::universal_rendering_manager::AnimationFrames;
use pierre_diffs::managers::universal_rendering_manager::FrameId;
use pierre_diffs::utils::dom::scrollbar_gutter::ScrollbarGutterHost;

pub(crate) struct DioxusHost {
    document: Document,
    /// The `diffs-container` element `FileDiff` renders into; the root of everything the
    /// managers touch, so "connected" means "reachable from it".
    container: NodeKey,
}

impl DioxusHost {
    pub(crate) fn new() -> Self {
        let document = Document::new();
        let container = document.create_element(pierre_diffs::constants::DIFFS_TAG_NAME);
        Self {
            document,
            container,
        }
    }

    pub(crate) fn container(&self) -> NodeKey {
        self.container
    }
}

impl ResizeManagerHost for DioxusHost {
    fn document(&self) -> &Document {
        &self.document
    }

    /// Nothing has measured a node yet (that is `onresize`, in the interactive step), so every
    /// node reads as 0. `ResizeManager` seeds a split annotation pair's `min-height` from the
    /// larger of the two; 0 sets no minimum, and the pair is only misaligned until the first
    /// `onresize` arrives.
    fn bounding_height(&self, _element: NodeKey) -> f64 {
        0.0
    }

    /// `isConnected`: the node is still in the container's tree (shadow roots included, as the
    /// document's parent walk crosses them). Whether Dioxus has mounted it is the interactive
    /// step's `onmounted` bookkeeping.
    fn is_connected(&self, element: NodeKey) -> bool {
        let mut current = element;
        while let Some(parent) = self.document.parent(current) {
            current = parent;
        }
        current == self.container
    }

    fn create_resize_observer(&self) {}

    fn observe(&self, _element: NodeKey) {}

    fn unobserve(&self, _element: NodeKey) {}

    fn disconnect_resize_observer(&self) {}

    /// `console.error` in the manager marks a broken internal pairing, not an input error, so it
    /// crashes (the repo's error policy).
    fn report(&self, message: &str, detail: Option<&str>) {
        panic!("{message}: {detail:?}");
    }
}

impl ScrollSyncHost for DioxusHost {
    fn document(&self) -> &Document {
        &self.document
    }

    fn add_scroll_listener(&self, _element: NodeKey, _side: ScrollSide) {}

    fn remove_scroll_listener(&self, _element: NodeKey, _side: ScrollSide) {}

    fn scroll_left(&self, _element: NodeKey) -> f64 {
        todo!("interactive step: the cached ScrollData::scroll_left of the node's onscroll")
    }

    fn scroll_to_left(&self, _element: NodeKey, _left: Option<f64>) {
        todo!("interactive step: MountedData::scroll for the node")
    }

    fn set_timeout(&self, _callback: Box<dyn FnOnce()>, _milliseconds: u32) -> TimeoutId {
        todo!("interactive step: a spawned task that sleeps on the host clock")
    }

    /// `clear_timeout(-1)` is the manager's "no timer pending" and `clean_up` calls it
    /// unconditionally; a real id can only come from `set_timeout`.
    fn clear_timeout(&self, timeout_id: TimeoutId) {
        if timeout_id != -1 {
            todo!("interactive step: cancel the spawned timer task");
        }
    }
}

impl AnimationFrames for DioxusHost {
    fn request_animation_frame(&self, _callback: Box<dyn FnOnce(f64)>) -> FrameId {
        todo!("interactive step: run the queue at the end of the current dispatch")
    }

    fn cancel_animation_frame(&self, _frame_id: FrameId) {
        todo!("interactive step: drop the queued dispatch-end run")
    }
}

impl InteractionHost for DioxusHost {
    fn document(&self) -> &Document {
        &self.document
    }

    /// Hit-testing needs layout this step does not have; `Unavailable` is the port's "callers
    /// fall back to the event path", which is what a host without `elementFromPoint` gets.
    fn element_from_point(&self, _x: f64, _y: f64) -> HitTest<NodeKey> {
        HitTest::Unavailable
    }

    fn add_pre_listeners(&self, _pre: NodeKey) {}

    fn remove_pre_listeners(&self, _pre: NodeKey) {}

    fn add_document_listeners(&self) {}

    fn remove_document_listeners(&self) {}
}

impl ScrollbarGutterHost for DioxusHost {
    /// Not measurable until the host element is mounted, so the theme CSS keeps
    /// `--diffs-scrollbar-gutter-fallback`.
    fn is_host_connected(&self) -> bool {
        false
    }

    fn measure_probe(&self, _probe: &hast::Element) -> f64 {
        unreachable!("an unconnected host is never measured")
    }
}
