#![cfg(target_arch = "wasm32")]

use clay_mdk::bindings;
use clay_mdk::document_events;
use clay_mdk::documents;
use clay_mdk::micro_lifecycle;
use clay_mdk::presentation;
use clay_mdk::presentation_events::Event;
use clay_mdk::presentation_events::EventResponse;
use clay_mdk::presentation_events::{self};
use clay_mdk::wire::WireMutations;
use dioxus_core::ElementId;
use dioxus_core::WriteMutations;

struct Slots;

impl micro_lifecycle::Guest for Slots {
    fn activate(configs: Vec<documents::DocumentBinding>) -> Result<(), String> {
        assert!(configs.is_empty());
        Ok(())
    }
}

impl presentation_events::Guest for Slots {
    fn mount(
        placement: &presentation::MountPoint,
        components: Vec<presentation::ComponentRef>,
    ) -> Result<presentation::Surface, presentation::PresentationError> {
        let surface = placement.open()?;
        let mut edits = WireMutations::default();
        edits.push_id(ElementId::ROOT);
        edits.create_element("section", None);
        edits.create_text("before");
        edits.append_children(1);
        for component in &components {
            edits.mount_component(component);
            edits.append_children(1);
        }
        edits.create_text("after");
        edits.append_children(1);
        edits.append_children(1);
        edits.pop();
        surface.apply(&edits.take())?;
        Ok(surface)
    }

    fn unmount(_surface: &presentation::Surface) {}

    fn dispatch(_surface: &presentation::Surface, _event: Event) -> EventResponse {
        EventResponse {
            prevent_default: false,
            stop_propagation: false,
        }
    }
}

impl document_events::Guest for Slots {
    fn changed(_snapshot: documents::DocumentSnapshot) {
        unreachable!("slot fixture owns no document watches");
    }

    fn lagged(_watch_id: u64) {
        unreachable!("slot fixture owns no document watches");
    }

    fn completed(_receipt: documents::OperationReceipt) {
        unreachable!("slot fixture submits no document operations");
    }
}

bindings::export!(Slots with_types_in bindings);
