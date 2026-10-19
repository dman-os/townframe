//! Guest-side bindings to Dayframe's presentation contract.
//!
//! The framework adapter owns its renderer and callbacks in guest memory. The host receives only
//! surface-scoped tree edits and listener identities, not Rust closures or framework objects.
//! Runtime implementations generate their own host bindings from the same WIT package.

pub mod bindings {
    wit_bindgen::generate!({
        path: "../presentation/wit",
        world: "micro-client",
        pub_export_macro: true,
    });
}

pub use bindings::exports::townframe::dayframe::{
    document_events, micro_lifecycle, presentation_events,
};
pub use bindings::townframe::dayframe::{documents, presentation};

pub mod wire;

/// The guest owns its `VirtualDom` and event closures in linear memory; only typed `WriteMutations`
/// batches cross `surface.apply`.
pub struct GuestDom {
    dom: dioxus_core::VirtualDom,
    targets: std::rc::Rc<std::cell::RefCell<Vec<(u64, dioxus_core::RenderTargetId)>>>,
    edits: std::collections::BTreeMap<dioxus_core::RenderTargetId, wire::WireMutations>,
    surfaces: std::collections::BTreeMap<u64, (dioxus_core::RenderTargetId, presentation::Surface)>,
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

        dioxus_html::set_event_converter(Box::new(dioxus_html::SerializedHtmlEventConverter));
        let targets = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let root_targets = targets.clone();
        let mut dom = dioxus_core::VirtualDom::new_with_props(
            move |()| {
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
        }
    }

    fn mount_target(&mut self, identity: u64) -> dioxus_core::RenderTargetId {
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

    fn unmount_target(&mut self, identity: u64) {
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
            for (&identity, (target, surface)) in &self.surfaces {
                let edits = self
                    .edits
                    .get_mut(target)
                    .expect("mounted target must have a writer");
                if !edits.is_empty() {
                    match surface.apply(&edits.take()) {
                        Ok(_) => {}
                        Err(presentation::PresentationError::Revoked) => revoked.push(identity),
                        Err(error) => return Err(error),
                    }
                }
            }
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

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
