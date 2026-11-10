//! The Dioxus guest adapter: one `VirtualDom` per micro incarnation, one render target per
//! granted mount, typed `WriteMutations` batches through `surface.apply`. Feature `dioxus`.
use crate::presentation;
use crate::presentation_events;
use crate::wire;

/// The component references the slots of one [`GuestDom`] mount, by slot id. A slot element
/// names its component by id (see [`wire::SLOT_ATTRIBUTE`]) because a reference is a host
/// resource that no Dioxus attribute can carry; the flush that writes the element looks the
/// reference up here and sends it with `mount-component`.
#[derive(Default)]
pub(crate) struct Slots {
    next: std::cell::Cell<u64>,
    held: std::cell::RefCell<
        std::collections::BTreeMap<u64, std::rc::Rc<presentation::ComponentRef>>,
    >,
    /// Ids whose slot element is gone. Their reference is released after the flush rather than
    /// at once, because that flush may still carry the mutation that created the element
    /// (a slot created and dropped between two flushes).
    retired: std::cell::RefCell<Vec<u64>>,
}

impl Slots {
    /// Hold `component` for the slot element about to be created and return its id.
    pub(crate) fn hold(&self, component: std::rc::Rc<presentation::ComponentRef>) -> u64 {
        let id = self.next.get();
        self.next.set(id + 1);
        self.held.borrow_mut().insert(id, component);
        id
    }

    /// The slot element of `id` is gone; release its reference after the next flush.
    pub(crate) fn retire(&self, id: u64) {
        self.retired.borrow_mut().push(id);
    }
}

/// The guest end of the element-command channel of one [`GuestDom`]: the commands sent and
/// still waiting on their reply. The host answers every command with an `element-reply` event, which
/// [`GuestDom::dispatch`] completes here.
#[derive(Default)]
pub(crate) struct ElementReplies {
    next: std::cell::Cell<u64>,
    pending:
        std::cell::RefCell<std::collections::BTreeMap<u64, std::rc::Rc<std::cell::RefCell<Reply>>>>,
}

impl ElementReplies {
    /// Register a command: its request id and the future its `element-reply` completes.
    pub(crate) fn begin(&self) -> (u64, ReplyFuture) {
        let id = self.next.get() + 1;
        self.next.set(id);
        let slot = std::rc::Rc::new(std::cell::RefCell::new(Reply::default()));
        self.pending.borrow_mut().insert(id, slot.clone());
        (id, ReplyFuture(slot))
    }
}

#[derive(Default)]
struct Reply {
    value: Option<Result<serde_json::Value, String>>,
    waker: Option<std::task::Waker>,
}

pub(crate) struct ReplyFuture(std::rc::Rc<std::cell::RefCell<Reply>>);

impl std::future::Future for ReplyFuture {
    type Output = Result<serde_json::Value, String>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut reply = self.0.borrow_mut();
        match reply.value.take() {
            Some(value) => std::task::Poll::Ready(value),
            None => {
                reply.waker = Some(cx.waker().clone());
                std::task::Poll::Pending
            }
        }
    }
}

/// What a guest `onmounted` handler gets in its `MountedData`: the imperative half of the
/// element, as element commands over the surface it is mounted on. The surface is held weakly,
/// so a handle kept past the unmount fails instead of keeping the surface alive.
#[derive(Clone)]
struct GuestElement {
    surface: std::rc::Weak<presentation::Surface>,
    element: presentation::NodeId,
    replies: std::rc::Rc<ElementReplies>,
}

type MountedFuture<T> =
    std::pin::Pin<Box<dyn std::future::Future<Output = dioxus_html::MountedResult<T>>>>;

fn operation_failed<T: 'static>(message: String) -> MountedFuture<T> {
    Box::pin(async move {
        Err(dioxus_html::MountedError::OperationFailed(Box::new(
            std::io::Error::other(message),
        )))
    })
}

impl GuestElement {
    /// Send `action` over the surface right away, from inside whatever handler called it, and
    /// wait for its `element-reply`, decoded by `decode`. The host does not run the command
    /// there: it queues commands and runs them in the order they were sent, after the dispatch
    /// that is on the stack has returned and its mutations were applied, so a handler may scroll,
    /// read, and focus in sequence and see them applied in that order. Every command is
    /// answered, so the future resolves `Ok` or `Err` the way a plain Dioxus app's would.
    fn request<T: 'static>(
        &self,
        action: presentation::ElementAction,
        decode: fn(&serde_json::Value) -> T,
    ) -> MountedFuture<T> {
        let (request, wait) = self.replies.begin();
        let sent = match self.surface.upgrade() {
            Some(surface) => surface
                .element_command(
                    self.element,
                    presentation::ElementCommand { request, action },
                )
                .map_err(|error| format!("{error:?}")),
            None => Err("the element's surface is unmounted".to_owned()),
        };
        if let Err(message) = sent {
            self.replies.pending.borrow_mut().remove(&request);
            return operation_failed(message);
        }
        settle(wait, decode)
    }
}

/// Wait for a command's reply and decode it. An error reply (the host renderer cannot answer
/// the command, as Blitz cannot measure some geometry) is the standard Dioxus `MountedError`, which a
/// plain Dioxus app sees from the same call; it is not a panic.
pub(crate) fn settle<T: 'static>(
    wait: ReplyFuture,
    decode: fn(&serde_json::Value) -> T,
) -> MountedFuture<T> {
    Box::pin(async move {
        match wait.await {
            Ok(value) => Ok(decode(&value)),
            Err(message) => Err(dioxus_html::MountedError::OperationFailed(Box::new(
                std::io::Error::other(message),
            ))),
        }
    })
}

/// `SerializedHtmlEventConverter` for every event but `mounted`, whose serialized form is a
/// `null` payload with no element behind it: the guest builds the payload itself (a
/// [`GuestElement`]) and this converter turns it into the `MountedData` a handler receives.
struct GuestEventConverter {
    serialized: dioxus_html::SerializedHtmlEventConverter,
}

macro_rules! delegate_event_converters {
    (
        enum Event {
            $(
                #[convert = $converter:ident]
                #[events = [$($events:tt)*]]
                $(#[raw = [$($raw:ident),* $(,)?]])?
                $group:ident($data:ident),
            )*
        }
    ) => {
        impl dioxus_html::HtmlEventConverter for GuestEventConverter {
            $( delegate_event_converters!(@one $converter, $data); )*
        }
    };
    (@one convert_mounted_data, MountedData) => {
        fn convert_mounted_data(
            &self,
            event: &dioxus_html::PlatformEventData,
        ) -> dioxus_html::MountedData {
            dioxus_html::MountedData::new(
                event
                    .downcast::<GuestElement>()
                    .expect("a mounted payload is a GuestElement")
                    .clone(),
            )
        }
    };
    (@one $converter:ident, $data:ident) => {
        fn $converter(&self, event: &dioxus_html::PlatformEventData) -> dioxus_html::$data {
            self.serialized.$converter(event)
        }
    };
}

dioxus_html::with_html_event_groups!(delegate_event_converters);

fn number(value: &serde_json::Value, key: &str) -> f64 {
    value[key]
        .as_f64()
        .unwrap_or_else(|| panic!("element reply has no number `{key}`: {value}"))
}

fn scroll_behavior(behavior: dioxus_html::ScrollBehavior) -> presentation::ScrollBehavior {
    match behavior {
        dioxus_html::ScrollBehavior::Instant => presentation::ScrollBehavior::Instant,
        dioxus_html::ScrollBehavior::Smooth => presentation::ScrollBehavior::Smooth,
    }
}

fn scroll_offsets(
    coordinates: dioxus_html::geometry::PixelsVector2D,
    behavior: dioxus_html::ScrollBehavior,
) -> presentation::ScrollOffsets {
    presentation::ScrollOffsets {
        left: coordinates.x,
        top: coordinates.y,
        behavior: scroll_behavior(behavior),
    }
}

impl dioxus_html::RenderedElementBacking for GuestElement {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn get_scroll_offset(&self) -> MountedFuture<dioxus_html::geometry::PixelsVector2D> {
        self.request(
            presentation::ElementAction::Read(presentation::ElementRead::ScrollOffset),
            |value| {
                dioxus_html::geometry::PixelsVector2D::new(number(value, "x"), number(value, "y"))
            },
        )
    }

    fn get_scroll_size(&self) -> MountedFuture<dioxus_html::geometry::PixelsSize> {
        self.request(
            presentation::ElementAction::Read(presentation::ElementRead::ScrollSize),
            |value| {
                dioxus_html::geometry::PixelsSize::new(
                    number(value, "width"),
                    number(value, "height"),
                )
            },
        )
    }

    fn get_client_rect(&self) -> MountedFuture<dioxus_html::geometry::PixelsRect> {
        self.request(
            presentation::ElementAction::Read(presentation::ElementRead::ClientRect),
            |value| {
                dioxus_html::geometry::PixelsRect::new(
                    dioxus_html::geometry::euclid::Point2D::new(
                        number(value, "x"),
                        number(value, "y"),
                    ),
                    dioxus_html::geometry::euclid::Size2D::new(
                        number(value, "width"),
                        number(value, "height"),
                    ),
                )
            },
        )
    }

    fn scroll(
        &self,
        coordinates: dioxus_html::geometry::PixelsVector2D,
        behavior: dioxus_html::ScrollBehavior,
    ) -> MountedFuture<()> {
        self.request(
            presentation::ElementAction::Scroll(scroll_offsets(coordinates, behavior)),
            |_| (),
        )
    }

    fn scroll_to(&self, options: dioxus_html::ScrollToOptions) -> MountedFuture<()> {
        let align = |position: &dioxus_html::ScrollLogicalPosition| match position {
            dioxus_html::ScrollLogicalPosition::Start => presentation::ScrollAlign::Start,
            dioxus_html::ScrollLogicalPosition::Center => presentation::ScrollAlign::Center,
            dioxus_html::ScrollLogicalPosition::End => presentation::ScrollAlign::End,
            dioxus_html::ScrollLogicalPosition::Nearest => presentation::ScrollAlign::Nearest,
        };
        self.request(
            presentation::ElementAction::ScrollIntoView(presentation::ScrollIntoView {
                behavior: scroll_behavior(options.behavior),
                vertical: align(&options.vertical),
                horizontal: align(&options.horizontal),
            }),
            |_| (),
        )
    }

    fn set_focus(&self, focus: bool) -> MountedFuture<()> {
        self.request(presentation::ElementAction::SetFocus(focus), |_| ())
    }
}

/// The guest owns its `VirtualDom` and event closures in linear memory; only typed `WriteMutations`
/// batches cross `surface.apply`.
pub struct GuestDom {
    pub(crate) dom: dioxus_core::VirtualDom,
    targets: std::rc::Rc<std::cell::RefCell<Vec<(u64, dioxus_core::RenderTargetId)>>>,
    pub(crate) edits:
        std::collections::BTreeMap<dioxus_core::RenderTargetId, wire::WireMutations<'static>>,
    surfaces: std::collections::BTreeMap<
        u64,
        (
            dioxus_core::RenderTargetId,
            std::rc::Rc<presentation::Surface>,
        ),
    >,
    slots: std::rc::Rc<Slots>,
    pub(crate) replies: std::rc::Rc<ElementReplies>,
}

impl GuestDom {
    pub fn new(app: fn() -> dioxus_core::Element) -> Self {
        Self::from_app(app)
    }

    pub fn new_with_props<P: Clone + 'static>(
        app: fn(P) -> dioxus_core::Element,
        props: P,
    ) -> Self {
        Self::from_app(move || app(props.clone()))
    }

    fn from_app(app: impl Fn() -> dioxus_core::Element + Clone + 'static) -> Self {
        use dioxus::prelude::*;

        dioxus_html::set_event_converter(Box::new(GuestEventConverter {
            serialized: dioxus_html::SerializedHtmlEventConverter,
        }));
        let targets = std::rc::Rc::new(std::cell::RefCell::new(Vec::<(
            u64,
            dioxus_core::RenderTargetId,
        )>::new()));
        let root_targets = targets.clone();
        let slots = std::rc::Rc::new(Slots::default());
        let root_slots = slots.clone();
        let mut dom = dioxus_core::VirtualDom::new_with_props(
            move |()| {
                use_context_provider(|| root_slots.clone());
                let children = app();
                let targets = root_targets.borrow();
                rsx! {
                    for (identity, target) in targets.iter() {
                        Portal {
                            key: "{identity}",
                            target: *target,
                            {children.clone()}
                        }
                    }
                }
            },
            (),
        );
        let mut edits = std::collections::BTreeMap::new();
        dom.rebuild(&mut edits);
        Self {
            dom,
            targets,
            edits,
            surfaces: std::collections::BTreeMap::new(),
            slots,
            replies: std::rc::Rc::default(),
        }
    }

    pub(crate) fn mount_target(&mut self, identity: u64) -> dioxus_core::RenderTargetId {
        let target = self.dom.runtime().create_render_target();
        assert!(
            self.edits
                .insert(target, wire::WireMutations::default())
                .is_none()
        );
        let mut targets = self.targets.borrow_mut();
        assert!(targets.iter().all(|(mounted, _)| *mounted != identity));
        targets.push((identity, target));
        self.dom.mark_dirty(dioxus_core::ScopeId::APP);
        target
    }

    pub(crate) fn unmount_target(&mut self, identity: u64) {
        let target = {
            let mut targets = self.targets.borrow_mut();
            let Some(index) = targets.iter().position(|(mounted, _)| *mounted == identity) else {
                return;
            };
            targets.remove(index).1
        };
        self.dom.mark_dirty(dioxus_core::ScopeId::APP);
        self.dom.render_immediate(&mut self.edits);
        self.edits
            .remove(&target)
            .expect("mounted target must have a writer");
        assert!(self.dom.runtime().remove_render_target(target));
        self.surfaces.remove(&identity);
    }

    pub fn mount(
        &mut self,
        placement: &presentation::MountPoint,
    ) -> Result<presentation::Surface, presentation::PresentationError> {
        let surface = placement.open()?;
        let retained = surface.retain()?;
        let identity = surface.identity();
        let target = self.mount_target(identity);
        assert!(
            self.surfaces
                .insert(identity, (target, std::rc::Rc::new(retained)))
                .is_none()
        );
        if let Err(error) = self.render() {
            self.unmount_target(identity);
            return Err(error);
        }
        if !self.surfaces.contains_key(&identity) {
            return Err(presentation::PresentationError::Revoked);
        }
        Ok(surface)
    }

    pub fn unmount(&mut self, surface: &presentation::Surface) {
        self.unmount_target(surface.identity());
        self.render().expect("guest presentation commit failed");
    }

    /// Applies a change that arrived outside event dispatch (a document callback, for example)
    /// inside this guest's Dioxus runtime, so signal writes schedule their subscribers, then
    /// flushes the resulting edits to every mounted surface.
    pub fn update(&mut self, change: impl FnOnce()) -> Result<(), presentation::PresentationError> {
        self.dom.in_runtime(change);
        self.render()
    }

    pub fn render(&mut self) -> Result<(), presentation::PresentationError> {
        loop {
            self.dom.render_immediate(&mut self.edits);
            let mut revoked = Vec::new();
            let held = self.slots.held.borrow();
            for (&identity, (target, surface)) in &self.surfaces {
                let edits = self
                    .edits
                    .get_mut(target)
                    .expect("mounted target must have a writer");
                if !edits.is_empty() {
                    let batch = edits.take_resolving(|id| {
                        held.get(&id)
                            .unwrap_or_else(|| panic!("slot {id} was written without a reference"))
                    });
                    match surface.apply(&batch) {
                        Ok(_) => {}
                        Err(presentation::PresentationError::Revoked) => revoked.push(identity),
                        Err(error) => return Err(error),
                    }
                }
            }
            drop(held);
            let mut held = self.slots.held.borrow_mut();
            for id in self.slots.retired.take() {
                held.remove(&id);
            }
            drop(held);
            if revoked.is_empty() {
                return Ok(());
            }
            for identity in revoked {
                self.unmount_target(identity);
            }
        }
    }

    /// Settle the command an `element-reply` answers.
    pub(crate) fn complete_reply(&self, reply: &serde_json::Value) {
        let request = reply["request"]
            .as_u64()
            .expect("element reply carries its request id");
        let outcome = match reply.get("error") {
            Some(error) => Err(error.as_str().unwrap_or("unknown failure").to_owned()),
            None => Ok(reply["value"].clone()),
        };
        let slot = self
            .replies
            .pending
            .borrow_mut()
            .remove(&request)
            .unwrap_or_else(|| panic!("element reply {request} answers no pending command"));
        let mut slot = slot.borrow_mut();
        slot.value = Some(outcome);
        if let Some(waker) = slot.waker.take() {
            waker.wake();
        }
    }

    pub fn dispatch(
        &mut self,
        surface: &presentation::Surface,
        event: presentation_events::Event,
    ) -> presentation_events::EventResponse {
        let Some((target, held)) = self.surfaces.get(&surface.identity()) else {
            return presentation_events::EventResponse {
                prevent_default: false,
                stop_propagation: false,
            };
        };
        let data = serde_json::from_str::<serde_json::Value>(&event.data_json)
            .expect("Dioxus event payload must be valid JSON");
        if event.name == wire::ELEMENT_REPLY {
            self.complete_reply(&data);
            self.render().expect("guest presentation commit failed");
            return presentation_events::EventResponse {
                prevent_default: false,
                stop_propagation: false,
            };
        }
        let envelope = serde_json::json!({
            "element": event.element,
            "name": event.name,
            "bubbles": event.bubbles,
            "data": data,
        });
        let html_event = serde_json::from_value::<dioxus_html::HtmlEvent>(envelope)
            .expect("event envelope must match Dioxus HtmlEvent");
        // The host sends `mounted` with a `null` payload: only the guest can build the handle
        // that sends commands back over this surface.
        let payload = if event.name == "mounted" {
            std::rc::Rc::new(dioxus_html::PlatformEventData::new(Box::new(
                GuestElement {
                    surface: std::rc::Rc::downgrade(held),
                    element: event.element,
                    replies: self.replies.clone(),
                },
            ))) as std::rc::Rc<dyn std::any::Any>
        } else {
            html_event.data.into_any()
        };
        let dioxus_event = dioxus_core::Event::new(payload, html_event.bubbles);
        self.dom.runtime().handle_event_for_target(
            *target,
            &html_event.name,
            dioxus_event.clone(),
            dioxus_core::ElementId::from_raw(html_event.element.raw()),
        );
        self.render().expect("guest presentation commit failed");
        presentation_events::EventResponse {
            prevent_default: !dioxus_event.default_action_enabled(),
            stop_propagation: !dioxus_event.propagates(),
        }
    }
}
