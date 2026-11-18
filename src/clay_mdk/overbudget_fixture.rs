#![cfg(target_arch = "wasm32")]
//! Fixture: a guest whose `dispatch` commits a batch over the `clay_surface` mutation budget.
//! Like the SDK's renderer, it treats `Revoked` as "the surface is gone" and returns, and aborts
//! on any other refusal (the SDK's `expect("guest presentation commit failed")`). Pins that a
//! budget breach faults only that micro, with the budget named as the cause, and never reaches
//! the abort, which a browser host cannot observe on the export's first poll.

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

struct Overbudget;

impl micro_lifecycle::Guest for Overbudget {
    type InstanceRoot = clay_mdk::StatelessRoot;

    async fn initialize(
        context: micro_lifecycle::InitialContext,
    ) -> Result<micro_lifecycle::Initialized, micro_lifecycle::InitializationError> {
        assert!(context.arguments.is_empty());
        assert!(context.configs.is_empty());
        assert!(context.collections.is_empty());
        assert!(context.registrations.is_empty());
        assert!(context.requirements.is_empty());
        Ok(micro_lifecycle::Initialized {
            root: micro_lifecycle::InstanceRoot::new(clay_mdk::StatelessRoot),
            publications: Vec::new(),
        })
    }

    async fn call(publication: String, _request: Vec<u8>) -> Result<Vec<u8>, String> {
        Err(format!("overbudget fixture serves no call {publication:?}"))
    }
}

impl presentation_events::Guest for Overbudget {
    fn mount(
        _publication: String,
        placement: &presentation::MountPoint,
    ) -> Result<presentation::Surface, presentation::PresentationError> {
        let surface = placement.open()?;
        let mut edits = WireMutations::default();
        edits.push_id(ElementId::ROOT);
        edits.create_element("section", None);
        edits.create_text("overbudget");
        edits.append_children(1);
        edits.append_children(1);
        edits.pop();
        surface.apply(&edits.take())?;
        Ok(surface)
    }

    fn unmount(_surface: &presentation::Surface) {}

    fn dispatch(surface: &presentation::Surface, _event: Event) -> EventResponse {
        let mut edits = WireMutations::default();
        // One past `SurfaceTree::MAX_BATCH_MUTATIONS` (4096), which a guest cannot name.
        for _ in 0..4097 {
            edits.pop();
        }
        match surface.apply(&edits.take()) {
            Ok(_) | Err(presentation::PresentationError::Revoked) => {}
            Err(error) => panic!("guest presentation commit failed: {error:?}"),
        }
        EventResponse {
            prevent_default: false,
            stop_propagation: false,
        }
    }
}

impl document_events::Guest for Overbudget {
    fn changed(_snapshot: documents::DocumentSnapshot) {
        unreachable!("overbudget fixture owns no document watches");
    }

    fn lagged(_watch_id: u64) {
        unreachable!("overbudget fixture owns no document watches");
    }

    fn completed(_receipt: documents::OperationReceipt) {
        unreachable!("overbudget fixture submits no document operations");
    }
}

bindings::export!(Overbudget with_types_in bindings);
