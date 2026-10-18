//! Guest-side bindings to Dayframe's presentation contract.
//!
//! The framework adapter owns its renderer and callbacks in guest memory. The host receives only
//! surface-scoped tree edits and listener identities, not Rust closures or framework objects.
//! Runtime implementations generate their own host bindings from the same WIT package.

pub mod bindings {
    wit_bindgen::generate!({
        path: "../app/wit",
        world: "presentation-client",
        pub_export_macro: true,
    });
}

pub use bindings::exports::townframe::dayframe::presentation_events;
pub use bindings::townframe::dayframe::presentation;

pub mod wire;

/// Guest-side Dioxus runtime bound to one opened [`presentation::Surface`].
///
/// The guest owns its `VirtualDom` and event closures in linear memory; only typed `WriteMutations`
/// batches cross `surface.apply`.
pub struct GuestDom {
    dom: dioxus_core::VirtualDom,
    edits: wire::WireMutations,
}

impl GuestDom {
    pub fn mount(
        placement: &presentation::MountPoint,
        app: fn() -> dioxus_core::Element,
    ) -> Result<(Self, presentation::Surface), presentation::PresentationError> {
        Self::mount_dom(placement, dioxus_core::VirtualDom::new(app))
    }

    pub fn mount_with_props<P: Clone + 'static>(
        placement: &presentation::MountPoint,
        app: fn(P) -> dioxus_core::Element,
        props: P,
    ) -> Result<(Self, presentation::Surface), presentation::PresentationError> {
        Self::mount_dom(
            placement,
            dioxus_core::VirtualDom::new_with_props(app, props),
        )
    }

    fn mount_dom(
        placement: &presentation::MountPoint,
        mut dom: dioxus_core::VirtualDom,
    ) -> Result<(Self, presentation::Surface), presentation::PresentationError> {
        let surface = placement.open()?;
        dioxus_html::set_event_converter(Box::new(dioxus_html::SerializedHtmlEventConverter));
        let mut edits = wire::WireMutations::default();
        dom.rebuild(&mut edits);
        let batch = edits.take();
        surface.apply(&batch)?;
        Ok((Self { dom, edits }, surface))
    }

    pub fn dispatch(
        &mut self,
        surface: &presentation::Surface,
        event: presentation_events::Event,
    ) -> presentation_events::EventResponse {
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
        let dioxus_event =
            dioxus_core::Event::new(html_event.data.into_any(), html_event.bubbles);
        self.dom.runtime().handle_event_for_target(
            dioxus_core::RenderTargetId::ROOT,
            &html_event.name,
            dioxus_event.clone(),
            dioxus_core::ElementId::from_raw(html_event.element.raw()),
        );
        self.dom.render_immediate(&mut self.edits);
        if !self.edits.is_empty() {
            surface
                .apply(&self.edits.take())
                .expect("guest presentation commit failed");
        }
        presentation_events::EventResponse {
            prevent_default: !dioxus_event.default_action_enabled(),
            stop_propagation: !dioxus_event.propagates(),
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
