#![cfg(target_arch = "wasm32")]
//! Spike fixture: a guest whose sync-typed `dispatch` blocks via
//! `std::thread::sleep`, which pulls in the async
//! `wasi:clocks/monotonic-clock.wait-until` import. Pins the host's handling of
//! a presentation call that suspends (ADR 018 §3 as amended by the W2 rewrite):
//! the first poll must surface a failure — not `Pending`, not a host panic —
//! and the host must treat it as a trap (withdraw surface, close instance).

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

struct Sleeper;

impl micro_lifecycle::Guest for Sleeper {
    fn activate(configs: Vec<documents::DocumentBinding>) -> Result<(), String> {
        assert!(configs.is_empty());
        Ok(())
    }
}

impl presentation_events::Guest for Sleeper {
    fn mount(
        placement: &presentation::MountPoint,
        _components: Vec<presentation::ComponentRef>,
    ) -> Result<presentation::Surface, presentation::PresentationError> {
        let surface = placement.open()?;
        let mut edits = WireMutations::default();
        edits.push_id(ElementId::ROOT);
        edits.create_element("section", None);
        edits.create_text("sleeper");
        edits.append_children(1);
        edits.pop();
        surface.apply(&edits.take())?;
        Ok(surface)
    }

    fn unmount(_surface: &presentation::Surface) {}

    fn dispatch(_surface: &presentation::Surface, _event: Event) -> EventResponse {
        // A nonzero duration forces the async monotonic-clock import
        // (`wait-until`, what std's sleep lifts to on wasm32-wasip3) off the
        // ready path. Under the component model's may-not-suspend rule this
        // must fail with `CannotBlock` (surfaced as `Err` on the first poll)
        // rather than suspend the export.
        std::thread::sleep(std::time::Duration::from_millis(1));
        EventResponse {
            prevent_default: false,
            stop_propagation: false,
        }
    }
}

impl document_events::Guest for Sleeper {
    fn changed(_snapshot: documents::DocumentSnapshot) {
        unreachable!("sleeper fixture owns no document watches");
    }

    fn lagged(_watch_id: u64) {
        unreachable!("sleeper fixture owns no document watches");
    }

    fn completed(_receipt: documents::OperationReceipt) {
        unreachable!("sleeper fixture submits no document operations");
    }
}

bindings::export!(Sleeper with_types_in bindings);
