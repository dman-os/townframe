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
use dioxus::html::MountedData;
use dioxus::html::MouseData;
use dioxus::html::PlatformEventData;
use dioxus::html::PointerData;
use dioxus::html::ResizeData;
use dioxus::html::ScrollData;
use dioxus::html::VisibleData;
use dioxus::html::input_data::MouseButton;
use dioxus::html::point_interaction::InteractionLocation;
use dioxus::html::point_interaction::ModifiersInteraction;
use dioxus::html::point_interaction::PointerInteraction;
use dioxus::prelude::*;
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

/// Where the events of one view go: its host cache and, on a virtualized view, the managers that
/// observe through it. A static view's dispatch records sizes and `MountedData` for the pointer's
/// hit-testing and has no manager to tell.
pub(crate) struct Dispatch {
    pub(crate) host: Rc<DioxusHost>,
    pub(crate) managers: Option<ObservingManagers>,
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
        if self.managers.is_none() || is_separator {
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
                previous != Some(border_height) && node != self.host.container();
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

    pub(crate) fn scrolled(&self, node: NodeKey, sample: ScrollSample) {
        self.host.record_scroll(node, sample);
        match self.host.scroll_side(node) {
            Some(None) => self.managers().virtualizer.handle_element_scroll(),
            Some(Some(ScrollSide::Deletions)) => {
                self.managers().scroll_sync.handle_deletions_scroll();
            }
            Some(Some(ScrollSide::Additions)) => {
                self.managers().scroll_sync.handle_additions_scroll();
            }
            // The listener was removed while the event was in flight.
            None => {}
        }
        self.host.flush_frames();
    }

    /// The listener attributes of `node`, for the renderer to add to the element.
    pub(crate) fn listeners(self: &Rc<Self>, node: NodeKey) -> Vec<Attribute> {
        let mut attributes = Vec::new();
        if self.host.wants_resize(node) {
            let dispatch = Rc::clone(self);
            attributes.push(listener("onresize", move |data: ResizeData| {
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
        // Scroll commands and client rects go through `MountedData`, which exists only from
        // `onmounted`: a node that is scrolled or measured is a node that gets it.
        if self.host.wants_scroll(node)
            || self
                .host
                .pointer
                .wants_mounted(host_document(&self.host), node)
        {
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
