//! The listeners the renderer attaches to the nodes the host registered, and where their events go.
//!
//! Dioxus delivers an event as platform data converted to the typed payload (`ResizeData`,
//! `VisibleData`, `ScrollData`, `MountedData`). Every handler does the same three things in the
//! same order: record what the payload says in the host's cache, tell the port's manager that
//! asked to be told (so its synchronous reads already see the new value), and flush the frame
//! queue, which is the end of the dispatch (see `host.rs`). Pointer events (`Pointers`) differ in
//! one respect: they carry no payload the cache needs, and go straight to the
//! `InteractionManager`.

use std::cell::RefCell;
use std::rc::Rc;

use dioxus::core::AttributeValue;
use dioxus::html::FocusData;
use dioxus::html::KeyboardData;
use dioxus::html::MountedData;
use dioxus::html::MouseData;
use dioxus::html::PlatformEventData;
use dioxus::html::PointerData;
use dioxus::html::ResizeData;
use dioxus::html::ScrollData;
use dioxus::html::TouchData;
use dioxus::html::VisibleData;
use dioxus::html::WheelData;
use dioxus::html::input_data::MouseButton;
use dioxus::html::point_interaction::InteractionLocation;
use dioxus::html::point_interaction::ModifiersInteraction;
use dioxus::html::point_interaction::PointerInteraction;
use dioxus::prelude::*;
use pierre_diffs::components::code_view::CodeView;
use pierre_diffs::components::virtualizer::InstanceRef;
use pierre_diffs::components::virtualizer::IntersectionEntry;
use pierre_diffs::components::virtualizer::ResizeEntry;
use pierre_diffs::components::virtualizer::Virtualizer;
use pierre_diffs::document::NodeKey;
use pierre_diffs::managers::interaction_manager::InteractionManager;
use pierre_diffs::managers::interaction_manager::PointerEventData;
use pierre_diffs::managers::interaction_manager::PointerResponse;
use pierre_diffs::managers::interaction_manager::PointerType;
use pierre_diffs::managers::resize_manager::ResizeObserverEntry;
use pierre_diffs::managers::resize_manager::ResizeObserverHub;
use pierre_diffs::managers::scroll_sync_manager::ScrollSide;
use pierre_diffs::managers::scroll_sync_manager::ScrollSyncManager;

use crate::geometry::ScrollSample;
use crate::host::DioxusHost;
use crate::scroll_root::RootNotice;
use crate::scroll_root::ScrollRoot;
use crate::slot::SlotContent;

/// Where the events of one view go: its host cache and, on a virtualized view, the managers that
/// observe through it. A static view's dispatch records sizes and `MountedData` for the pointer's
/// hit-testing and has no manager to tell.
pub(crate) struct Dispatch {
    pub(crate) host: Rc<DioxusHost>,
    pub(crate) managers: Option<ObservingManagers>,
    /// The viewer a code view's events are delivered to; `managers` is `None` there.
    pub(crate) code_view: Option<CodeViewManagers>,
    /// The resize and visibility entries of the frame in progress, waiting for their drain.
    pub(crate) batch: RefCell<Batch>,
}

/// What `Dispatch::drain` delivers: the entries of one frame.
#[derive(Default)]
pub(crate) struct Batch {
    resizes: Vec<(NodeKey, f64, f64)>,
    visibles: Vec<IntersectionEntry<NodeKey>>,
    height_changed: bool,
    scheduled: bool,
}

/// The managers a virtualized view's resize, visibility and scroll events are delivered to.
pub(crate) struct ObservingManagers {
    pub(crate) hub: Rc<ResizeObserverHub<DioxusHost>>,
    pub(crate) virtualizer: Rc<Virtualizer<DioxusHost>>,
    pub(crate) scroll_sync: Rc<ScrollSyncManager<DioxusHost>>,
    pub(crate) instance: InstanceRef,
}

/// What a code view's events do to its viewer; implemented by `CodeView` over this host, so
/// `Dispatch` need not know the annotation type.
pub(crate) trait ShellEvents {
    /// The root's `scroll` event.
    fn handle_scroll(&self);
    /// The root's `wheel`, `touchstart`, `pointerdown` and `keydown`: a person took over.
    fn clear_pending_scroll(&self);
    /// One delivery of the viewer's resize observer.
    fn handle_resize(&self, entries: &[ResizeObserverEntry<NodeKey>]);
    /// The scroll sync manager of the item a split column's scroll event came from.
    fn scroll_sync_of(&self, node: NodeKey) -> Option<Rc<ScrollSyncManager<DioxusHost>>>;
}

impl<A: Clone + PartialEq + 'static> ShellEvents for CodeView<DioxusHost, SlotContent, A> {
    fn handle_scroll(&self) {
        CodeView::handle_scroll(self);
    }

    fn clear_pending_scroll(&self) {
        CodeView::clear_pending_scroll(self);
    }

    fn handle_resize(&self, entries: &[ResizeObserverEntry<NodeKey>]) {
        CodeView::handle_resize(self, entries);
    }

    fn scroll_sync_of(&self, node: NodeKey) -> Option<Rc<ScrollSyncManager<DioxusHost>>> {
        self.scroll_sync_manager_of(node)
    }
}

/// The managers a code view's resize and scroll events are delivered to: the hub of the rows'
/// observed sizes, and the viewer (see [`ShellEvents`]). There are no visibility events: the
/// viewer renders by scroll position.
pub(crate) struct CodeViewManagers {
    pub(crate) hub: Rc<ResizeObserverHub<DioxusHost>>,
    pub(crate) events: Rc<dyn ShellEvents>,
}

impl Dispatch {
    /// The managers of a virtualized view: only its host registers visibility and scroll
    /// listeners, so only its events reach a handler that asks for them.
    fn managers(&self) -> &ObservingManagers {
        self.managers
            .as_ref()
            .expect("only a virtualized host registers visibility and scroll listeners")
    }

    pub(crate) fn mounted(&self, node: NodeKey, mounted: MountedData) {
        self.host.record_mounted(node, Rc::new(mounted));
        self.host.flush_frames();
    }

    /// `ResizeObserver`'s callback for one element: `border_height` is
    /// `borderBoxSize.blockSize` and `content_width` is `contentBoxSize.inlineSize`.
    ///
    /// The cache takes the size at once (the synchronous reads of whoever runs before the drain
    /// already see it), but the managers hear of it in the frame's one batch (`drain`): upstream's
    /// observers deliver every entry of a frame in one callback, so a window of 200 new rows is one
    /// reconcile pass, not 200.
    pub(crate) fn resized(self: &Rc<Self>, node: NodeKey, border_height: f64, content_width: f64) {
        // Upstream's ResizeObserver stops reporting a row the moment the row is removed. A Dioxus
        // `onresize` already in flight can still arrive after the render that freed the row (a
        // fast scroll swaps rows out every frame), so an event for a node the document no longer
        // holds is dropped before anything reads it.
        if !host_document(&self.host).exists(node) {
            return;
        }
        let previous = self.host.record_size(node, border_height);
        // A static view's rows, and any view's hunk separators, are measured for geometry only:
        // the port reads no separator height (its metrics carry the estimate) and nothing is
        // reconciled against it, so telling the managers would only queue passes upstream never
        // runs. The cache still feeds the hit-test and the tops the scroll fix reads, as
        // `getBoundingClientRect` would upstream.
        let is_separator = host_document(&self.host).has_attribute(node, "data-separator");
        if (self.managers.is_none() && self.code_view.is_none()) || is_separator {
            self.host.flush_frames();
            return;
        }
        {
            let mut batch = self.batch.borrow_mut();
            batch.resizes.push((node, border_height, content_width));
            // Upstream sees a row's height change at the next pass its own render triggers. Here
            // the layout that changed it arrives after that pass, so a row that grew or first
            // appeared asks for the pass that reads it (a no-op unless it has something to
            // reconcile).
            batch.height_changed |=
                previous != Some(border_height) && !self.host.is_container(node);
        }
        self.schedule_drain();
    }

    /// `IntersectionObserver`'s callback for one element, batched like `resized`.
    pub(crate) fn visible(self: &Rc<Self>, node: NodeKey, is_intersecting: bool) {
        // As in `resized`: upstream's IntersectionObserver stops reporting a removed row, while
        // an `onvisible` in flight can outlive the render that freed it.
        if !host_document(&self.host).exists(node) {
            return;
        }
        self.batch.borrow_mut().visibles.push(IntersectionEntry {
            target: node,
            is_intersecting,
        });
        self.schedule_drain();
    }

    /// One executor tick for the events of every handler that ran before it: the task is the
    /// engine's "once per frame" callback delivery.
    fn schedule_drain(self: &Rc<Self>) {
        if std::mem::replace(&mut self.batch.borrow_mut().scheduled, true) {
            return;
        }
        let dispatch = Rc::clone(self);
        spawn(async move { dispatch.drain() });
    }

    /// One callback of each observer with every entry the frame queued, in arrival order, then
    /// one reconcile request and one flush. Resize observations are delivered before
    /// intersection observations, as the rendering update orders them.
    fn drain(&self) {
        let Batch {
            resizes,
            visibles,
            height_changed,
            scheduled: _,
        } = std::mem::take(&mut *self.batch.borrow_mut());
        let document = host_document(&self.host);
        if let Some(code_view) = &self.code_view {
            self.drain_code_view(code_view, resizes);
            return;
        }
        let managers = self.managers();
        // A row freed by a render between its event and this tick is dropped here too.
        let resizes: Vec<_> = resizes
            .into_iter()
            .filter(|(node, ..)| document.exists(*node))
            .collect();
        let visibles: Vec<_> = visibles
            .into_iter()
            .filter(|entry| document.exists(entry.target))
            .collect();
        if !resizes.is_empty() {
            managers.hub.handle_shared_resize_entries(
                &resizes
                    .iter()
                    .map(|(node, height, width)| ResizeObserverEntry {
                        target: *node,
                        border_box_block_size: *height,
                        content_box_inline_size: *width,
                    })
                    .collect::<Vec<_>>(),
            );
            managers.virtualizer.handle_container_resize(
                &resizes
                    .iter()
                    .map(|(node, height, _)| ResizeEntry {
                        target: *node,
                        block_size: *height,
                    })
                    .collect::<Vec<_>>(),
            );
            if height_changed {
                managers
                    .virtualizer
                    .request_height_reconcile(&managers.instance);
            }
        }
        if !visibles.is_empty() {
            managers.virtualizer.handle_intersection_change(&visibles);
        }
        self.host.flush_frames();
    }

    /// The code view's drain: the entries of the frame go to the hub of the rows' observed sizes,
    /// then the ones of the nodes the viewer's own observer watches go to the viewer in one
    /// delivery (two observers upstream, and the viewer's reads what the first recorded).
    fn drain_code_view(&self, code_view: &CodeViewManagers, resizes: Vec<(NodeKey, f64, f64)>) {
        let document = host_document(&self.host);
        // A row freed by a pass between its event and this tick is dropped here too.
        let entries: Vec<_> = resizes
            .into_iter()
            .filter(|(node, ..)| document.exists(*node))
            .map(|(node, height, width)| ResizeObserverEntry {
                target: node,
                border_box_block_size: height,
                content_box_inline_size: width,
            })
            .collect();
        if !entries.is_empty() {
            code_view.hub.handle_shared_resize_entries(&entries);
            let observed: Vec<_> = entries
                .into_iter()
                .filter(|entry| {
                    self.host
                        .code_view()
                        .is_some_and(|code_view| code_view.observes_shell(entry.target))
                })
                .collect();
            if !observed.is_empty() {
                code_view.events.handle_resize(&observed);
            }
        }
        self.host.flush_frames();
    }

    /// The embedder's scroller told the handle something (see `scroll_root.rs`): the viewer's
    /// root takes it as its own, translated to the viewer's place in the scroller's content.
    pub(crate) fn page_notice(self: &Rc<Self>, page: &ScrollRoot, notice: RootNotice) {
        let root = self.host.root();
        match notice {
            RootNotice::Mounted => {
                let scroller = page
                    .scroller()
                    .expect("the handle has its scroller when it reports it mounted");
                self.host.record_mounted(root, scroller);
                self.host.flush_frames();
            }
            RootNotice::Scrolled => {
                let sample = page
                    .sample()
                    .expect("the handle has its sample when it reports a scroll");
                self.scrolled(root, page.viewer_sample(sample));
            }
            RootNotice::Resized => {
                let (height, width) = page
                    .size()
                    .expect("the handle has its size when it reports a resize");
                self.resized(root, height, width);
            }
            // Where the viewer sits moved, so its own scroll position did, with no scroll event
            // behind it: as for a root that resized, the viewer reads it again.
            RootNotice::Moved => {
                if let Some(sample) = page.sample() {
                    self.host.record_scroll(root, page.viewer_sample(sample));
                }
                let (height, width) = page.size().unwrap_or_default();
                let code_view = self
                    .code_view
                    .as_ref()
                    .expect("a page root belongs to a code view");
                code_view.events.handle_resize(&[ResizeObserverEntry {
                    target: root,
                    border_box_block_size: height,
                    content_box_inline_size: width,
                }]);
                self.host.flush_frames();
            }
        }
    }

    /// Puts what the handle already knows into the caches, when a viewer mounts on a scroller
    /// that has reported before it. Nothing is notified: the viewer's first pass reads them.
    pub(crate) fn seed_page(&self, page: &ScrollRoot) {
        let root = self.host.root();
        if let Some(scroller) = page.scroller() {
            self.host.record_mounted(root, scroller);
        }
        if let Some(sample) = page.sample() {
            self.host.record_scroll(root, page.viewer_sample(sample));
        }
        if let Some((height, _)) = page.size() {
            self.host.record_size(root, height);
        }
    }

    pub(crate) fn scrolled(&self, node: NodeKey, sample: ScrollSample) {
        self.host.record_scroll(node, sample);
        match (self.host.scroll_side(node), &self.code_view) {
            (Some(None), Some(code_view)) => code_view.events.handle_scroll(),
            (Some(None), None) => self.managers().virtualizer.handle_element_scroll(),
            (Some(Some(side)), None) => {
                let scroll_sync = &self.managers().scroll_sync;
                match side {
                    ScrollSide::Deletions => scroll_sync.handle_deletions_scroll(),
                    ScrollSide::Additions => scroll_sync.handle_additions_scroll(),
                }
            }
            // Each item has its own manager, found by the column's place in the tree.
            (Some(Some(side)), Some(code_view)) => {
                let scroll_sync = code_view
                    .events
                    .scroll_sync_of(node)
                    .expect("a registered column belongs to a rendered item");
                match side {
                    ScrollSide::Deletions => scroll_sync.handle_deletions_scroll(),
                    ScrollSide::Additions => scroll_sync.handle_additions_scroll(),
                }
            }
            // The listener was removed while the event was in flight.
            (None, _) => {}
        }
        self.host.flush_frames();
    }

    /// A person's input on the root (`wheel`, `touchstart`, `pointerdown`, `keydown`) ends a
    /// programmatic scroll. It queues no frame, so it ends no dispatch.
    pub(crate) fn took_over(&self) {
        self.code_view
            .as_ref()
            .expect("only a code view's root carries these listeners")
            .events
            .clear_pending_scroll();
    }

    /// The listener attributes of `node`, for the renderer to add to the element.
    pub(crate) fn listeners(self: &Rc<Self>, node: NodeKey) -> Vec<Attribute> {
        let mut attributes = Vec::new();
        if self.host.wants_resize(node) {
            let dispatch = Rc::clone(self);
            attributes.push(listener("onresize", move |data: ResizeData| {
                // The viewer's root in the embedder's scroller is as tall as its content, which
                // is no viewport: its resize only says where it may have moved to.
                if dispatch.host.is_page_root(node) {
                    dispatch
                        .host
                        .code_view()
                        .and_then(|code_view| code_view.page())
                        .expect("a page root has its handle")
                        .measure();
                    return;
                }
                // Horizontal writing mode: the block size is the height.
                let border = data
                    .get_border_box_size()
                    .expect("the platform reports a resized element's border box");
                let content = data
                    .get_content_box_size()
                    .expect("the platform reports a resized element's content box");
                dispatch.resized(node, border.height, content.width);
            }));
        }
        if self.host.wants_visible(node) {
            let dispatch = Rc::clone(self);
            attributes.push(listener("onvisible", move |data: VisibleData| {
                let intersecting = data
                    .is_intersecting()
                    .expect("the platform reports whether a watched element intersects");
                dispatch.visible(node, intersecting);
            }));
        }
        if self.host.wants_scroll(node) {
            let dispatch = Rc::clone(self);
            attributes.push(listener("onscroll", move |data: ScrollData| {
                dispatch.scrolled(
                    node,
                    ScrollSample {
                        top: data.scroll_top(),
                        left: data.scroll_left(),
                        scroll_height: f64::from(data.scroll_height()),
                        client_height: f64::from(data.client_height()),
                    },
                );
            }));
        }
        let code_view = self.host.code_view();
        if code_view.is_some_and(|code_view| code_view.wants_shell_listeners(node)) {
            // Upstream's passive root listeners: `wheel`, `touchstart`, `pointerdown` (the
            // scrollbar belongs to the root, and some webviews do not raise it) and `keydown`.
            let dispatch = Rc::clone(self);
            attributes.push(listener("onwheel", move |_: WheelData| {
                dispatch.took_over()
            }));
            let dispatch = Rc::clone(self);
            attributes.push(listener("ontouchstart", move |_: TouchData| {
                dispatch.took_over();
            }));
            let dispatch = Rc::clone(self);
            attributes.push(listener("onpointerdown", move |_: PointerData| {
                dispatch.took_over();
            }));
            let dispatch = Rc::clone(self);
            attributes.push(listener("onkeydown", move |_: KeyboardData| {
                dispatch.took_over();
            }));
        }
        if code_view.is_some_and(|code_view| code_view.is_shell(host_document(&self.host), node)) {
            let dispatch = Rc::clone(self);
            attributes.push(listener("onfocusin", move |_: FocusData| {
                dispatch
                    .host
                    .code_view()
                    .expect("only a code view's shells carry these listeners")
                    .record_focus(node, true);
            }));
            let dispatch = Rc::clone(self);
            attributes.push(listener("onfocusout", move |_: FocusData| {
                dispatch
                    .host
                    .code_view()
                    .expect("only a code view's shells carry these listeners")
                    .record_focus(node, false);
            }));
        }
        if self.host.is_page_root(node) {
            // The viewer's root in the embedder's scroller is measured, not scrolled: its own
            // place in the scroller's content is what the handle needs it for.
            let dispatch = Rc::clone(self);
            attributes.push(listener("onmounted", move |data: MountedData| {
                dispatch
                    .host
                    .code_view()
                    .and_then(|code_view| code_view.page())
                    .expect("a page root has its handle")
                    .viewer_mounted(Rc::new(data));
            }));
        } else if self.host.wants_scroll(node)
            || self
                .host
                .pointer
                .wants_mounted(host_document(&self.host), node)
        {
            // Scroll commands and client rects go through `MountedData`, which exists only from
            // `onmounted`: a node that is scrolled or measured is a node that gets it.
            let dispatch = Rc::clone(self);
            attributes.push(listener("onmounted", move |data: MountedData| {
                dispatch.mounted(node, data);
            }));
        }
        attributes
    }
}

/// An event attribute over the platform's data; what `rsx!` builds for `on<event>: handler`.
fn listener<Data>(name: &'static str, mut handler: impl FnMut(Data) + 'static) -> Attribute
where
    Data: for<'a> From<&'a PlatformEventData> + 'static,
{
    Attribute::new(
        name,
        AttributeValue::listener(move |event: Event<PlatformEventData>| {
            handler(Data::from(&*event.data()));
        }),
        None,
        false,
    )
}

/// The pointer events the `pre`'s listeners and the document's would receive upstream, on their
/// way to the `InteractionManager`.
pub(crate) struct Pointers {
    host: Rc<DioxusHost>,
    manager: Rc<InteractionManager<DioxusHost>>,
    /// The platform data of the last event a node's handler took. Every node that handles an event
    /// sees it while it bubbles (a line, then its `pre`), and the manager must get it once,
    /// from the innermost handler: it is the one that knows the deepest target. The same
    /// `Rc` reaches all of them, so pointer equality is "the same event".
    last_target_event: RefCell<Option<Rc<PlatformEventData>>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PointerKind {
    Click,
    Down,
    Move,
    Leave,
    DocumentMove,
    DocumentUp,
    DocumentCancel,
}

impl Pointers {
    pub(crate) fn new(
        host: Rc<DioxusHost>,
        manager: Rc<InteractionManager<DioxusHost>>,
    ) -> Rc<Self> {
        Rc::new(Self {
            host,
            manager,
            last_target_event: RefCell::new(None),
        })
    }

    /// Hands one event to the manager and ends the dispatch. The edits the manager makes (hover and
    /// selection attributes) reach Dioxus as any later edit does, through the document's observer.
    fn deliver(&self, kind: PointerKind, event: &PointerEventData<NodeKey>) -> PointerResponse {
        let response = match kind {
            PointerKind::Click => self.manager.handle_pointer_click(event),
            PointerKind::Down => self.manager.handle_pointer_down(event),
            PointerKind::Move => self.manager.handle_pointer_move(event),
            PointerKind::Leave => self.manager.handle_pointer_leave(event),
            PointerKind::DocumentMove => self.manager.handle_document_pointer_move(event),
            PointerKind::DocumentUp => self.manager.handle_document_pointer_up(event),
            PointerKind::DocumentCancel => self.manager.handle_document_pointer_cancel(event),
        };
        self.host.flush_frames();
        response
    }

    /// The `pointerdown`/`pointermove`/`click` handlers of a node the manager resolves targets
    /// from, and the `pointerleave` of the `pre`; plus, on the view's top node, the document
    /// handlers a drag needs. `node` is the target the manager is told.
    pub(crate) fn listeners(self: &Rc<Self>, node: NodeKey) -> Vec<Attribute> {
        let wanted = self
            .host
            .pointer
            .listeners(host_document(&self.host), self.host.top(), node);
        let mut attributes = Vec::new();
        if wanted.click {
            attributes.push(self.node_listener("onclick", PointerKind::Click, node));
        }
        if wanted.press_and_move {
            attributes.push(self.node_listener("onpointerdown", PointerKind::Down, node));
            attributes.push(self.node_listener("onpointermove", PointerKind::Move, node));
        }
        if wanted.leave {
            attributes.push(self.node_listener("onpointerleave", PointerKind::Leave, node));
        }
        if wanted.document {
            attributes.push(self.document_listener("onpointermove", PointerKind::DocumentMove));
            attributes.push(self.document_listener("onpointerup", PointerKind::DocumentUp));
            attributes.push(self.document_listener("onpointercancel", PointerKind::DocumentCancel));
        }
        attributes
    }

    fn node_listener(
        self: &Rc<Self>,
        name: &'static str,
        kind: PointerKind,
        node: NodeKey,
    ) -> Attribute {
        let pointers = Rc::clone(self);
        Attribute::new(
            name,
            AttributeValue::listener(move |event: Event<PlatformEventData>| {
                let data = event.data();
                if !pointers.first_at_node(&data) {
                    return;
                }
                let payload = if kind == PointerKind::Click {
                    click_payload(&MouseData::from(&*data), node)
                } else {
                    pointer_payload(&PointerData::from(&*data), Some(node))
                };
                let mut response = pointers.deliver(kind, &payload);
                // A drag's moves reach the document listeners after the `pre`'s, with the same
                // event, as they do upstream.
                if kind == PointerKind::Move && pointers.host.pointer.document_listeners_active() {
                    let document =
                        pointers.deliver(drag_kind(&PointerData::from(&*data)), &payload);
                    response.prevent_default |= document.prevent_default;
                    response.stop_propagation |= document.stop_propagation;
                }
                apply(&event, response);
            }),
            None,
            false,
        )
    }

    fn document_listener(self: &Rc<Self>, name: &'static str, kind: PointerKind) -> Attribute {
        let pointers = Rc::clone(self);
        Attribute::new(
            name,
            AttributeValue::listener(move |event: Event<PlatformEventData>| {
                let data = event.data();
                if !pointers.host.pointer.document_listeners_active() {
                    return;
                }
                // A move a node's handler already delivered to the document listeners.
                if kind == PointerKind::DocumentMove && !pointers.first_at_document(&data) {
                    return;
                }
                // The event's target is not known here (see `host.rs`); a drag resolves its
                // rows from coordinates, and only the event-path fallback reads a target.
                let pointer = PointerData::from(&*data);
                let kind = if kind == PointerKind::DocumentMove {
                    drag_kind(&pointer)
                } else {
                    kind
                };
                apply(
                    &event,
                    pointers.deliver(kind, &pointer_payload(&pointer, None)),
                );
            }),
            None,
            false,
        )
    }

    /// True the first time a node's handler sees `data`.
    pub(crate) fn first_at_node(&self, data: &Rc<PlatformEventData>) -> bool {
        let mut last = self.last_target_event.borrow_mut();
        if last.as_ref().is_some_and(|last| Rc::ptr_eq(last, data)) {
            return false;
        }
        *last = Some(Rc::clone(data));
        true
    }

    /// False when a node's handler already took `data`: it delivered the move to the document
    /// listeners with the target it knows.
    pub(crate) fn first_at_document(&self, data: &Rc<PlatformEventData>) -> bool {
        !self
            .last_target_event
            .borrow()
            .as_ref()
            .is_some_and(|last| Rc::ptr_eq(last, data))
    }
}

fn apply(event: &Event<PlatformEventData>, response: PointerResponse) {
    if response.prevent_default {
        event.prevent_default();
    }
    if response.stop_propagation {
        event.stop_propagation();
    }
}

/// What a drag's `pointermove` is. A mouse move with no button held while a drag is active means
/// the button came up where no event reaches the view (outside its window): the drag is over, and
/// the manager, which waits for a `pointerup` that already happened, would otherwise ignore every
/// later `pointerdown`. Touch and pen release inside the view (the browser keeps delivering to
/// the element the touch began on), so only a mouse is read this way.
fn drag_kind(data: &PointerData) -> PointerKind {
    if data.pointer_type() == "mouse" && data.held_buttons().is_empty() {
        PointerKind::DocumentUp
    } else {
        PointerKind::DocumentMove
    }
}

fn pointer_payload(data: &PointerData, target: Option<NodeKey>) -> PointerEventData<NodeKey> {
    let client = data.client_coordinates();
    PointerEventData {
        pointer_id: data.pointer_id(),
        pointer_type: match data.pointer_type().as_str() {
            "mouse" => PointerType::Mouse,
            "touch" => PointerType::Touch,
            "pen" => PointerType::Pen,
            _ => PointerType::Other,
        },
        // `-1` is the platform's "no button changed state" (a move).
        button: data.trigger_button().map_or(-1, MouseButton::into_web_code),
        shift_key: data.modifiers().shift(),
        client_x: client.x,
        client_y: client.y,
        target,
    }
}

/// A `click` is a `MouseEvent` upstream too: the manager reads its target and shift key. It has
/// no pointer id or type (the manager reads neither for a click).
fn click_payload(data: &MouseData, target: NodeKey) -> PointerEventData<NodeKey> {
    let client = data.client_coordinates();
    PointerEventData {
        pointer_id: 0,
        pointer_type: PointerType::Mouse,
        button: data.trigger_button().map_or(-1, MouseButton::into_web_code),
        shift_key: data.modifiers().shift(),
        client_x: client.x,
        client_y: client.y,
        target: Some(target),
    }
}

fn host_document(host: &DioxusHost) -> &pierre_diffs::document::Document {
    pierre_diffs::managers::resize_manager::ResizeManagerHost::document(host)
}
