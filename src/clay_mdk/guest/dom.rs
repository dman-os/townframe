//! The Dioxus guest adapter: one `VirtualDom` per published presentation, one render target and
//! one component instance per granted mount, typed `WriteMutations` batches through
//! `surface.apply`. Feature `dioxus`.
use crate::presentation;
use crate::presentation_events;
use crate::wire;

/// The component references the slots of one [`GuestDom`] mount, by slot id (each mount owns
/// its own table, so a mount's references are released with it). A slot element
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

/// The guest owns its `VirtualDom` and event closures in linear memory; only typed `WriteMutations`
/// batches cross `surface.apply`.
pub struct GuestDom {
    pub(crate) dom: dioxus_core::VirtualDom,
    mounts: std::rc::Rc<std::cell::RefCell<Vec<MountEntry>>>,
    pub(crate) edits:
        std::collections::BTreeMap<dioxus_core::RenderTargetId, wire::WireMutations<'static>>,
    surfaces: std::collections::BTreeMap<u64, (dioxus_core::RenderTargetId, presentation::Surface)>,
}

/// One live mount as the root renders it: where it renders and the slots its scope owns.
struct MountEntry {
    identity: u64,
    target: dioxus_core::RenderTargetId,
    slots: std::rc::Rc<Slots>,
}

type RenderFn = std::rc::Rc<dyn Fn() -> dioxus_core::Element>;

/// What one mount's component instance renders from. Equal while the render function and slot
/// table are the same allocations, so re-rendering the root (another surface mounting or
/// unmounting) skips this mount's scope instead of re-running its render.
#[derive(dioxus::prelude::Props, Clone)]
struct MountProps {
    render: RenderFn,
    slots: std::rc::Rc<Slots>,
}

impl PartialEq for MountProps {
    fn eq(&self, other: &Self) -> bool {
        std::rc::Rc::ptr_eq(&self.render, &other.render)
            && std::rc::Rc::ptr_eq(&self.slots, &other.slots)
    }
}

/// The component instance a mount renders, like one instance of a custom element: the hooks
/// `render` calls live in this scope, so every mount has its own, while whatever `render` closes
/// over (the micro's collections, its store) is shared by all of them.
#[allow(non_snake_case)]
fn MountRoot(props: MountProps) -> dioxus_core::Element {
    dioxus::prelude::use_context_provider(|| props.slots.clone());
    (props.render)()
}

impl GuestDom {
    /// A presentation whose `render` runs once per mount, in that mount's own component scope.
    pub fn new(render: fn() -> dioxus_core::Element) -> Self {
        Self::with_setup(|| {}, std::rc::Rc::new(render))
    }

    pub fn new_with_props<P: Clone + 'static>(
        render: fn(P) -> dioxus_core::Element,
        props: P,
    ) -> Self {
        Self::with_setup(|| {}, std::rc::Rc::new(move || render(props.clone())))
    }

    /// `setup` runs in the root scope, once per presentation: the hooks and context providers
    /// every mount shares. `render` runs in a scope of its own for each mount, below the root, so
    /// it reads the root's context but its hooks are the mount's.
    pub fn with_setup(setup: impl Fn() + 'static, render: RenderFn) -> Self {
        use dioxus::prelude::*;

        dioxus_html::set_event_converter(Box::new(dioxus_html::SerializedHtmlEventConverter));
        let mounts = std::rc::Rc::new(std::cell::RefCell::new(Vec::<MountEntry>::new()));
        let root_mounts = mounts.clone();
        let setup = std::rc::Rc::new(setup);
        let mut dom = dioxus_core::VirtualDom::new_with_props(
            move |()| {
                (*setup)();
                let mounts = root_mounts.borrow();
                rsx! {
                    for mount in mounts.iter() {
                        Portal {
                            key: "{mount.identity}",
                            target: mount.target,
                            MountRoot { render: render.clone(), slots: mount.slots.clone() }
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
            mounts,
            edits,
            surfaces: std::collections::BTreeMap::new(),
        }
    }

    pub(crate) fn mount_target(&mut self, identity: u64) -> dioxus_core::RenderTargetId {
        let target = self.dom.runtime().create_render_target();
        assert!(
            self.edits
                .insert(target, wire::WireMutations::default())
                .is_none()
        );
        let mut mounts = self.mounts.borrow_mut();
        assert!(mounts.iter().all(|mounted| mounted.identity != identity));
        mounts.push(MountEntry {
            identity,
            target,
            slots: std::rc::Rc::default(),
        });
        self.dom.mark_dirty(dioxus_core::ScopeId::APP);
        target
    }

    pub(crate) fn unmount_target(&mut self, identity: u64) {
        let target = {
            let mut mounts = self.mounts.borrow_mut();
            let Some(index) = mounts.iter().position(|mounted| mounted.identity == identity) else {
                return;
            };
            mounts.remove(index).target
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
        assert!(self.surfaces.insert(identity, (target, retained)).is_none());
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
            let mounts = self.mounts.borrow();
            for (&identity, (target, surface)) in &self.surfaces {
                let edits = self
                    .edits
                    .get_mut(target)
                    .expect("mounted target must have a writer");
                let slots = &mounts
                    .iter()
                    .find(|mounted| mounted.identity == identity)
                    .expect("a surface's mount is registered until it unmounts")
                    .slots;
                if !edits.is_empty() {
                    let held = slots.held.borrow();
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
                // Released after the flush: it may still have carried the mutation that created
                // an element whose slot is already retired.
                let mut held = slots.held.borrow_mut();
                for id in slots.retired.take() {
                    held.remove(&id);
                }
            }
            drop(mounts);
            if revoked.is_empty() {
                return Ok(());
            }
            for identity in revoked {
                self.unmount_target(identity);
            }
        }
    }

    pub fn dispatch(
        &mut self,
        surface: &presentation::Surface,
        event: presentation_events::Event,
    ) -> presentation_events::EventResponse {
        let Some((target, _)) = self.surfaces.get(&surface.identity()) else {
            return presentation_events::EventResponse {
                prevent_default: false,
                stop_propagation: false,
            };
        };
        let data = serde_json::from_str::<serde_json::Value>(&event.data_json)
            .expect("Dioxus event payload must be valid JSON");
        let envelope = serde_json::json!({
            "element": event.element,
            "name": event.name,
            "bubbles": event.bubbles,
            "data": data,
        });
        let html_event = serde_json::from_value::<dioxus_html::HtmlEvent>(envelope)
            .expect("event envelope must match Dioxus HtmlEvent");
        let dioxus_event = dioxus_core::Event::new(html_event.data.into_any(), html_event.bubbles);
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
