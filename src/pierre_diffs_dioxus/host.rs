//! The Dioxus `FileDiffHost`: what `FileDiff` and `VirtualizedFileDiff` ask of their platform.
//!
//! The structural half of every host trait (children, attributes, queries, style writes) is the
//! port's `Document`, which this host owns and the renderer in `render.rs` turns into Dioxus
//! elements. What is left is geometry, listeners, timers, frames and scroll writes. A host is
//! either static or interactive:
//!
//! * A static host (the non-virtualized `FileDiffView`) accepts registrations and keeps nothing:
//!   the renderer attaches no handlers, so no event could arrive for them.
//! * An interactive host (the virtualized view) keeps what was registered, and the renderer
//!   attaches `onresize`, `onvisible`, `onscroll` and `onmounted` handlers to exactly those nodes:
//!   "observe" is "render this node with the listener", "unobserve" is "render it without". The
//!   handlers (`events.rs`) fill the geometry cache (`geometry.rs`) that answers the synchronous
//!   reads, hand `MountedData` to the commands that write, and flush the frame queue at the end of
//!   their dispatch.
//!
//! Frames. Dioxus has no animation-frame API and the diff view uses no JS, so
//! `request_animation_frame` queues the callback and every handler ends by running the queue
//! (`flush_frames`): the queue is "the end of the current dispatch". The browser delivers scroll
//! and resize events about once per frame, so one dispatch stands in for one frame. A callback
//! that queues again (the render queue's follow-up frame) gets a spawned executor tick even when no
//! later event arrives; this preserves the layout pass's follow-up without spinning in the flush.
//!
//! Pointers. The manager takes every pointer event as data with its target as a node key, but a
//! Dioxus event does not say which element it hit, only which element's handler is running. So
//! the renderer attaches the pointer handlers to the nodes the manager resolves targets from
//! (`pointer_listeners`: the `pre` upstream listens on, its lines, line numbers, expand buttons,
//! utility and annotation wrappers) and a handler reports the node it is attached to; the manager
//! walks up from there as it walks up `composedPath()`. The document-level listeners of a drag are
//! a flag (`add_document_listeners`) that the handlers on the nodes and on the view's top node
//! read: events bubble to the top node, so a pointer move or up anywhere in the view reaches the
//! manager while a drag is active, and the flag keeps them from reaching it otherwise.
//! `element_from_point` answers from the cached row geometry (see `geometry.rs`).
//!
//! Timers. Standard Dioxus has no timer either, so the embedder passes a `sleep` function and a
//! timeout is a spawned task that awaits it (`clear_timeout` cancels the task). A callback that
//! runs from a task flushes the frame queue itself, since no handler is around it.

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::HashSet;
use std::pin::Pin;
use std::rc::Rc;
use std::rc::Weak;
use std::time::Duration;

use dioxus::core::Task;
use dioxus::html::MountedData;
use dioxus::html::geometry::PixelsVector2D;
use dioxus::prelude::*;
use pierre_diffs::components::virtualizer::IntersectionObserverInit;
use pierre_diffs::components::virtualizer::Root;
use pierre_diffs::components::virtualizer::ScrollBehavior as PierreScrollBehavior;
use pierre_diffs::components::virtualizer::VirtualizerHost;
use pierre_diffs::document::Document;
use pierre_diffs::document::NodeKey;
use pierre_diffs::hast;
use pierre_diffs::managers::resize_manager::ResizeManagerHost;
use pierre_diffs::managers::scroll_sync_manager::ScrollSide;
use pierre_diffs::managers::scroll_sync_manager::ScrollSyncHost;
use pierre_diffs::managers::scroll_sync_manager::TimeoutId;
use pierre_diffs::managers::universal_rendering_manager::AnimationFrames;
use pierre_diffs::managers::universal_rendering_manager::FrameId;
use pierre_diffs::utils::dom::scrollbar_gutter::ScrollbarGutterHost;

mod pointer;

use crate::geometry::Geometry;
use crate::geometry::ScrollSample;
use pointer::PointerState;

/// What an embedder supplies for timers: a future that resolves after the duration.
pub type SleepFuture = Pin<Box<dyn std::future::Future<Output = ()>>>;

/// The scroller a virtualized view renders: the `Virtualizer`'s root, its content wrapper, and
/// the embedder's clock.
pub(crate) struct Scroller {
    pub(crate) root: NodeKey,
    pub(crate) content: NodeKey,
    pub(crate) sleep: Rc<dyn Fn(Duration) -> SleepFuture>,
}

/// The nodes each kind of listener is rendered on.
#[derive(Default)]
struct Registrations {
    /// The `ResizeManager`'s nodes (code columns, annotation wrappers).
    resize_manager: HashSet<NodeKey>,
    /// The `Virtualizer`'s nodes (the scroller and its content).
    resize_virtualizer: HashSet<NodeKey>,
    visible: HashSet<NodeKey>,
    /// Scroll listeners: the scroller (no side) and the split columns the scroll sync follows.
    scroll: HashMap<NodeKey, Option<ScrollSide>>,
}

type FrameCallback = Box<dyn FnOnce(f64)>;

#[derive(Default)]
struct Frames {
    next_id: FrameId,
    pending: Vec<(FrameId, FrameCallback)>,
    scheduled_flush: bool,
    #[cfg(test)]
    flush_count: usize,
}

pub(crate) struct DioxusHost {
    weak_self: Weak<Self>,
    document: Document,
    /// The `diffs-container` element `FileDiff` renders into.
    container: NodeKey,
    /// The topmost node, so "connected" means "reachable from it": the container for a static
    /// view, the scroller's root for a virtualized one.
    top: NodeKey,
    scroller: Option<Scroller>,
    registrations: RefCell<Registrations>,
    geometry: RefCell<Geometry>,
    mounted: RefCell<HashMap<NodeKey, Rc<MountedData>>>,
    frames: RefCell<Frames>,
    timers: RefCell<HashMap<TimeoutId, Task>>,
    next_timeout: Cell<TimeoutId>,
    /// What the `InteractionManager`'s listeners and hit-testing need (see `host/pointer.rs`).
    pub(crate) pointer: PointerState,
}

impl DioxusHost {
    /// A static host: the container is the whole tree.
    pub(crate) fn new_static() -> Rc<Self> {
        let document = Document::new();
        let container = document.create_element(pierre_diffs::constants::DIFFS_TAG_NAME);
        Self::build(document, container, container, None, 0.0)
    }

    /// An interactive host: `root > content > container`, with `root` a scroll container of
    /// the given CSS `height` (a scroller must have a bounded height, or nothing scrolls).
    /// `line_height` is the layout's estimate for one line.
    pub(crate) fn new_scrolling(
        height: &str,
        sleep: Rc<dyn Fn(Duration) -> SleepFuture>,
        line_height: f64,
    ) -> Rc<Self> {
        let document = Document::new();
        let container = document.create_element(pierre_diffs::constants::DIFFS_TAG_NAME);
        let content = document.create_element("div");
        let root = document.create_element("div");
        // `overflow-anchor: none`: the engine's own scroll anchoring would shift the position
        // the virtualizer's scroll fix is about to correct, applying the correction twice on the
        // engines that have it.
        document.set_attribute(
            root,
            "style",
            &format!("overflow-y:auto;overflow-anchor:none;height:{height};"),
        );
        // A scroll container the keyboard can reach: without a tab stop its arrow and page keys
        // do nothing, and a mouse is the only way to move through the diff.
        document.set_attribute(root, "tabindex", "0");
        document.append_child(content, container);
        document.append_child(root, content);
        Self::build(
            document,
            container,
            root,
            Some(Scroller {
                root,
                content,
                sleep,
            }),
            line_height,
        )
    }

    fn build(
        document: Document,
        container: NodeKey,
        top: NodeKey,
        scroller: Option<Scroller>,
        line_height: f64,
    ) -> Rc<Self> {
        Rc::new_cyclic(|weak_self| Self {
            weak_self: Weak::clone(weak_self),
            document,
            container,
            top,
            scroller,
            registrations: RefCell::default(),
            geometry: RefCell::new(Geometry::new(line_height)),
            mounted: RefCell::default(),
            frames: RefCell::default(),
            timers: RefCell::default(),
            next_timeout: Cell::new(0),
            pointer: PointerState::default(),
        })
    }

    pub(crate) fn container(&self) -> NodeKey {
        self.container
    }

    /// The node the view renders from.
    pub(crate) fn top(&self) -> NodeKey {
        self.top
    }

    pub(crate) fn scroller(&self) -> Option<&Scroller> {
        self.scroller.as_ref()
    }

    fn scroller_root(&self) -> NodeKey {
        self.scroller
            .as_ref()
            .expect("the virtualizer only runs on a scrolling host")
            .root
    }

    pub(crate) fn is_interactive(&self) -> bool {
        self.scroller.is_some()
    }

    // The renderer's policy: which listeners a node gets.

    pub(crate) fn wants_resize(&self, node: NodeKey) -> bool {
        let registrations = self.registrations.borrow();
        registrations.resize_manager.contains(&node)
            || registrations.resize_virtualizer.contains(&node)
            || self.is_measured(node)
    }

    /// Whether this host keeps row heights: a virtualized one always (the port reads them), a
    /// static one only while the pointer can drag over rows, which is the one thing that reads
    /// heights there (`element_from_point`). A static view's cost is then one `onresize` per
    /// measured row (the rows `is_measured` lists) and one `onmounted` on the `pre` and a content column: it is for short diffs, and
    /// a long one is what the virtualized view is for.
    fn measures_rows(&self) -> bool {
        self.is_interactive() || self.pointer.acts_on_rows()
    }

    pub(crate) fn wants_visible(&self, node: NodeKey) -> bool {
        self.registrations.borrow().visible.contains(&node)
    }

    pub(crate) fn wants_scroll(&self, node: NodeKey) -> bool {
        self.registrations.borrow().scroll.contains_key(&node)
    }

    /// The nodes whose heights the port reads without registering them (`reconcileHeights`
    /// reads each line row and its annotation, the virtualizer the container, the scroll anchor
    /// the header above the lines): every one needs `onresize` to be answerable.
    ///
    /// A static host reads none of them, so it measures them only for the hit-test
    /// (`measures_rows`). Hunk separators are measured on every host, and only into the geometry
    /// cache (`Dispatch::resized`): the port estimates a separator in its metrics but never reads
    /// one from layout, and no style in the document sets its height, so the rows stacked below
    /// one (hit-testing, the scroll fix's tops) are off by its height without a measurement.
    fn is_measured(&self, node: NodeKey) -> bool {
        self.measures_rows()
            && (node == self.container
                || [
                    "data-line",
                    "data-line-annotation",
                    "data-no-newline",
                    "data-diffs-header",
                ]
                .iter()
                .any(|name| self.document.has_attribute(node, name))
                || self.document.has_attribute(node, "data-separator"))
    }

    /// The scroller's position; a static view has none, and its rows sit where the page puts
    /// them, which the frame measured at drag start already includes.
    fn scroll_top(&self) -> f64 {
        self.scroller.as_ref().map_or(0.0, |scroller| {
            self.geometry.borrow().scroll(scroller.root).top
        })
    }

    /// The scroll side a node's listener was registered for (`None` for the scroller itself).
    pub(crate) fn scroll_side(&self, node: NodeKey) -> Option<Option<ScrollSide>> {
        self.registrations.borrow().scroll.get(&node).copied()
    }

    /// Drops what refers to nodes the document discarded: their sizes, and their `MountedData`
    /// (it dies with the node it was captured from).
    pub(crate) fn prune(&self) {
        self.geometry.borrow_mut().prune(&self.document);
        self.mounted
            .borrow_mut()
            .retain(|node, _| self.document.exists(*node));
        let mut registrations = self.registrations.borrow_mut();
        let document = &self.document;
        registrations
            .resize_manager
            .retain(|node| document.exists(*node));
        registrations.visible.retain(|node| document.exists(*node));
        registrations
            .scroll
            .retain(|node, _| document.exists(*node));
    }

    // What the handlers record.

    pub(crate) fn record_mounted(&self, node: NodeKey, mounted: Rc<MountedData>) {
        self.mounted.borrow_mut().insert(node, mounted);
    }

    /// The previous size of the node, `None` for its first measurement.
    pub(crate) fn record_size(&self, node: NodeKey, block_size: f64) -> Option<f64> {
        self.geometry.borrow_mut().record_size(node, block_size)
    }

    pub(crate) fn record_scroll(&self, node: NodeKey, sample: ScrollSample) {
        self.geometry.borrow_mut().record_scroll(node, sample);
    }

    // Frames.

    /// The end of a dispatch: runs every callback queued so far. A callback that queues another
    /// frame is flushed on the next executor tick.
    pub(crate) fn flush_frames(&self) {
        let pending = {
            let mut frames = self.frames.borrow_mut();
            #[cfg(test)]
            {
                frames.flush_count += 1;
            }
            std::mem::take(&mut frames.pending)
        };
        // No consumer reads the timestamp (the render queues ignore it) and standard Dioxus has
        // no clock to read one from.
        for (_, callback) in pending {
            callback(0.0);
        }

        let should_schedule = {
            let mut frames = self.frames.borrow_mut();
            if frames.pending.is_empty() || frames.scheduled_flush {
                false
            } else {
                frames.scheduled_flush = true;
                true
            }
        };
        if should_schedule {
            // A callback can queue the render queue's follow-up frame. If no browser event arrives,
            // it still needs a dispatch, so schedule one executor tick later. The flag stays set
            // until that task starts: an event may flush the queue before then, and must not create
            // a second task. This is a spawned task rather than a loop in this flush because a
            // callback could queue another frame forever.
            let host = self
                .weak_self
                .upgrade()
                .expect("the host is alive while a frame flush is scheduled");
            spawn(async move {
                host.frames.borrow_mut().scheduled_flush = false;
                host.flush_frames();
            });
        }
    }

    /// Whether a flush would run anything; tests read it to see the queue drain.
    #[cfg(test)]
    pub(crate) fn frames_pending(&self) -> usize {
        self.frames.borrow().pending.len()
    }

    #[cfg(test)]
    pub(crate) fn flush_count(&self) -> usize {
        self.frames.borrow().flush_count
    }

    fn mounted_of(&self, node: NodeKey) -> Rc<MountedData> {
        Rc::clone(
            self.mounted
                .borrow()
                .get(&node)
                .expect("a node with a scroll listener is mounted before it is scrolled"),
        )
    }

    /// Scrolls `node`. `MountedData::scroll` sets both axes while the virtualizer owns one and
    /// the scroll sync the other, so the axis a caller does not set keeps the position the
    /// engine reports at the time: read in the same task that writes, because the cached one is
    /// stale by whatever the engine scrolled since its last `onscroll`.
    fn spawn_scroll(
        &self,
        node: NodeKey,
        vertical: Vertical,
        left: Option<f64>,
        behavior: dioxus::html::ScrollBehavior,
    ) {
        let mounted = self.mounted_of(node);
        let host = self
            .weak_self
            .upgrade()
            .expect("the host is alive while it is called");
        spawn(async move {
            let offset = mounted
                .get_scroll_offset()
                .await
                .expect("the platform reports a mounted element's scroll offset");
            let top = match vertical {
                Vertical::Keep => offset.y,
                Vertical::By(delta) => (offset.y + delta).max(0.0),
                Vertical::To(top) => top,
            };
            mounted
                .scroll(PixelsVector2D::new(left.unwrap_or(offset.x), top), behavior)
                .await
                .expect("the platform scrolls a mounted element");
            host.geometry.borrow_mut().set_scroll_top(node, top);
        });
    }
}

/// How a scroll command moves the vertical axis.
enum Vertical {
    Keep,
    /// Relative to the live offset, read in the command's own task.
    By(f64),
    To(f64),
}

fn dioxus_behavior(behavior: PierreScrollBehavior) -> dioxus::html::ScrollBehavior {
    match behavior {
        // `auto` is the CSS default, which scrolls instantly.
        PierreScrollBehavior::Auto | PierreScrollBehavior::Instant => {
            dioxus::html::ScrollBehavior::Instant
        }
        PierreScrollBehavior::Smooth => dioxus::html::ScrollBehavior::Smooth,
    }
}

impl ResizeManagerHost for DioxusHost {
    fn document(&self) -> &Document {
        &self.document
    }

    /// The last `onresize` of the node. A static host measures rows only for the pointer (and
    /// observes nothing), so `ResizeManager`, which reads a split annotation pair's heights as it
    /// observes them, sees none.
    fn bounding_height(&self, element: NodeKey) -> f64 {
        self.geometry.borrow().height(&self.document, element)
    }

    /// `isConnected`: the node is still in the tree under the top node (shadow roots included,
    /// as the document's parent walk crosses them). Whether Dioxus has mounted it is the
    /// `onmounted` bookkeeping, which only the nodes that are written to need.
    fn is_connected(&self, element: NodeKey) -> bool {
        self.document.is_attached(element, self.top)
    }

    fn create_resize_observer(&self) {}

    fn observe(&self, element: NodeKey) {
        if self.is_interactive() {
            self.registrations
                .borrow_mut()
                .resize_manager
                .insert(element);
        }
    }

    fn unobserve(&self, element: NodeKey) {
        self.registrations
            .borrow_mut()
            .resize_manager
            .remove(&element);
    }

    fn disconnect_resize_observer(&self) {
        self.registrations.borrow_mut().resize_manager.clear();
    }

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

    fn add_scroll_listener(&self, element: NodeKey, side: ScrollSide) {
        if self.is_interactive() {
            self.registrations
                .borrow_mut()
                .scroll
                .insert(element, Some(side));
        }
    }

    fn remove_scroll_listener(&self, element: NodeKey, _side: ScrollSide) {
        self.registrations.borrow_mut().scroll.remove(&element);
    }

    fn scroll_left(&self, element: NodeKey) -> f64 {
        self.geometry.borrow().scroll(element).left
    }

    fn scroll_to_left(&self, element: NodeKey, left: Option<f64>) {
        // `scrollTo({ left: undefined })` leaves the position alone.
        if left.is_some() {
            self.spawn_scroll(
                element,
                Vertical::Keep,
                left,
                dioxus::html::ScrollBehavior::Instant,
            );
        }
    }

    fn set_timeout(&self, callback: Box<dyn FnOnce()>, milliseconds: u32) -> TimeoutId {
        let sleep = Rc::clone(
            &self
                .scroller
                .as_ref()
                .expect("a static view registers no scroll listener, so nothing sets a timer")
                .sleep,
        );
        let id = self.next_timeout.get();
        self.next_timeout.set(id + 1);
        let host = self
            .weak_self
            .upgrade()
            .expect("the host is alive while it is called");
        let task = spawn(async move {
            sleep(Duration::from_millis(u64::from(milliseconds))).await;
            host.timers.borrow_mut().remove(&id);
            callback();
            // No handler surrounds a task, so this is the end of its dispatch.
            host.flush_frames();
        });
        self.timers.borrow_mut().insert(id, task);
        id
    }

    /// `clear_timeout(-1)` is the manager's "no timer pending" and `clean_up` calls it
    /// unconditionally.
    fn clear_timeout(&self, timeout_id: TimeoutId) {
        if let Some(task) = self.timers.borrow_mut().remove(&timeout_id) {
            task.cancel();
        }
    }
}

impl AnimationFrames for DioxusHost {
    fn request_animation_frame(&self, callback: Box<dyn FnOnce(f64)>) -> FrameId {
        let mut frames = self.frames.borrow_mut();
        let id = frames.next_id;
        frames.next_id += 1;
        frames.pending.push((id, callback));
        id
    }

    fn cancel_animation_frame(&self, frame_id: FrameId) {
        self.frames
            .borrow_mut()
            .pending
            .retain(|(id, _)| *id != frame_id);
    }
}

impl VirtualizerHost for DioxusHost {
    fn document(&self) -> &Document {
        &self.document
    }

    // The virtualizer scrolls inside the view's own scroller; there is no window to ask.
    fn window_scroll_y(&self) -> f64 {
        unreachable!("the Dioxus view virtualizes within its scroller, never the window")
    }

    fn window_inner_height(&self) -> f64 {
        unreachable!("the Dioxus view virtualizes within its scroller, never the window")
    }

    fn document_scroll_height(&self) -> f64 {
        unreachable!("the Dioxus view virtualizes within its scroller, never the window")
    }

    fn element_scroll_top(&self, element: &NodeKey) -> f64 {
        self.geometry.borrow().scroll(*element).top
    }

    /// The scroller's content is exactly as tall as what scrolls, and its `onresize` arrives
    /// before the scroller's first `onscroll` (which is when `ScrollData::scroll_height` first
    /// exists), so the content's height is the scroll height; the sample is the fallback.
    fn element_scroll_height(&self, element: &NodeKey) -> f64 {
        let geometry = self.geometry.borrow();
        let content = self
            .scroller
            .as_ref()
            .and_then(|scroller| geometry.measured_size(scroller.content));
        content.unwrap_or_else(|| geometry.scroll(*element).scroll_height)
    }

    fn element_bounding_height(&self, element: &NodeKey) -> f64 {
        let geometry = self.geometry.borrow();
        let client_height = geometry.scroll(*element).client_height;
        match geometry.measured_size(*element) {
            Some(size) => size,
            None if client_height > 0.0 => client_height,
            None => geometry.height(&self.document, *element),
        }
    }

    /// Relative to the viewport, as `getBoundingClientRect().top`: the scroller's own top is 0
    /// (the virtualizer only ever subtracts it), everything else is its distance from the top of
    /// the content less the scroll position.
    fn element_bounding_top(&self, element: &NodeKey) -> f64 {
        let root = self.scroller_root();
        if *element == root {
            return 0.0;
        }
        let geometry = self.geometry.borrow();
        geometry.top_in_content(&self.document, self.container, *element)
            - geometry.scroll(root).top
    }

    fn element_offset_height(&self, element: &NodeKey) -> f64 {
        self.element_bounding_height(element)
    }

    fn window_scroll_to(&self, _top: f64, _behavior: PierreScrollBehavior) {
        unreachable!("the Dioxus view virtualizes within its scroller, never the window")
    }

    fn element_scroll_to(&self, element: &NodeKey, top: f64, behavior: PierreScrollBehavior) {
        let left = self.geometry.borrow().scroll(*element).left;
        self.geometry.borrow_mut().set_scroll_top(*element, top);
        self.spawn_scroll(
            *element,
            Vertical::To(top),
            Some(left),
            dioxus_behavior(behavior),
        );
    }

    /// The scroll fix: read the offset and scroll by the delta in one task (see `spawn_scroll`),
    /// and move the cached position by the delta at once so reads before the task finishes
    /// already see where the scroller is going.
    fn element_scroll_by(&self, element: &NodeKey, scroll_offset: f64) {
        let top = self.geometry.borrow().scroll(*element).top;
        self.geometry
            .borrow_mut()
            .set_scroll_top(*element, (top + scroll_offset).max(0.0));
        self.spawn_scroll(
            *element,
            Vertical::By(scroll_offset),
            None,
            dioxus::html::ScrollBehavior::Instant,
        );
    }

    /// The observers are the renderer's listeners and the intersection observer's options (root,
    /// margin, thresholds) are the engine's defaults, because `onvisible` has no options.
    fn create_observers(&self, _intersection: IntersectionObserverInit<NodeKey>) {}

    fn disconnect_observers(&self) {
        let mut registrations = self.registrations.borrow_mut();
        registrations.resize_virtualizer.clear();
        registrations.visible.clear();
    }

    fn observe_resize(&self, element: &NodeKey) {
        self.registrations
            .borrow_mut()
            .resize_virtualizer
            .insert(*element);
    }

    fn observe_document_resize(&self) {
        unreachable!("the Dioxus view virtualizes within its scroller, never the window")
    }

    fn observe_intersection(&self, element: &NodeKey) {
        self.registrations.borrow_mut().visible.insert(*element);
    }

    fn unobserve_intersection(&self, element: &NodeKey) {
        self.registrations.borrow_mut().visible.remove(element);
    }

    fn add_window_listeners(&self) {
        unreachable!("the Dioxus view virtualizes within its scroller, never the window")
    }

    fn add_element_scroll_listener(&self, root: &NodeKey) {
        self.registrations.borrow_mut().scroll.insert(*root, None);
    }

    fn remove_listeners(&self, root: Option<&Root<NodeKey>>) {
        if let Some(Root::Element(root)) = root {
            self.registrations.borrow_mut().scroll.remove(root);
        }
    }

    fn console_log(&self, message: &str, virtualizer_id: &str, size_change: f64, new_size: f64) {
        // The view never enables `resizeDebugging`; a diagnostic with nowhere to go is a
        // configuration error.
        panic!(
            "{message} ({virtualizer_id}): {size_change} -> {new_size}; the Dioxus view does not enable resize debugging"
        );
    }
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
