//! Wire probe using Dioxus's actual mutation encoder, not a Dayframe rendering language.
#![cfg(target_arch = "wasm32")]
use dayframe_guest::{
    bindings,
    presentation::{MountPoint, PresentationError, Surface},
    presentation_events::{self, Event, EventResponse},
};
use dioxus::prelude::*;
use dioxus_core::{ElementId, RenderTargetId, VirtualDom};
use dioxus_html::HtmlEvent;
use dioxus_interpreter_js::MutationState;
use serde_json::Value;
use std::cell::RefCell;

fn counter() -> Element {
    let mut count = use_signal(|| 0_u64);
    rsx! {
        button {
            onclick: move |_| count += 1,
            "{count}"
        }
    }
}

struct Counter;
thread_local! {
    // Both the virtual DOM and its mutation encoder belong to this component incarnation.
    static DOM: RefCell<Option<VirtualDom>> = const { RefCell::new(None) };
    static EDITS: RefCell<MutationState> = RefCell::new(MutationState::default());
}

impl presentation_events::Guest for Counter {
    fn mount(placement: &MountPoint) -> Result<Surface, PresentationError> {
        let surface = placement.open()?;
        dioxus_html::set_event_converter(Box::new(dioxus_html::SerializedHtmlEventConverter));
        let mut dom = VirtualDom::new(counter);
        let bytes = EDITS.with_borrow_mut(|edits| {
            dom.rebuild(edits);
            edits.export_memory()
        });
        surface.apply(&bytes)?;
        DOM.with_borrow_mut(|slot| {
            assert!(
                slot.replace(dom).is_none(),
                "counter incarnation mounted twice"
            );
        });
        Ok(surface)
    }

    fn dispatch(surface: &Surface, event: Event) -> EventResponse {
        let data = serde_json::from_str::<Value>(&event.data_json)
            .expect("Dioxus event payload must be valid JSON");
        let envelope = serde_json::json!({
            "element": event.element,
            "name": event.name,
            "bubbles": event.bubbles,
            "data": data,
        });
        let html_event = serde_json::from_value::<HtmlEvent>(envelope)
            .expect("event envelope must match Dioxus HtmlEvent");
        let dioxus_event = dioxus_core::Event::new(html_event.data.into_any(), html_event.bubbles);
        DOM.with_borrow_mut(|slot| {
            let dom = slot.as_mut().expect("counter was dispatched before mount");
            dom.runtime().handle_event_for_target(
                RenderTargetId::ROOT,
                &html_event.name,
                dioxus_event.clone(),
                ElementId::from_raw(html_event.element.raw()),
            );
            EDITS.with_borrow_mut(|edits| {
                dom.render_immediate(edits);
                let bytes = edits.export_memory();
                if !bytes.is_empty() {
                    surface
                        .apply(&bytes)
                        .expect("counter presentation commit failed");
                }
            });
        });
        EventResponse {
            prevent_default: !dioxus_event.default_action_enabled(),
            stop_propagation: !dioxus_event.propagates(),
        }
    }
}

bindings::export!(Counter with_types_in bindings);
