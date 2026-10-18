//! Dioxus counter rendered through the typed presentation wire, not a Dayframe rendering language.
#![cfg(target_arch = "wasm32")]
use dayframe_guest::{
    GuestDom, bindings,
    presentation::{MountPoint, PresentationError, Surface},
    presentation_events::{self, Event, EventResponse},
};
use dioxus::prelude::*;
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
    static DOM: RefCell<Option<GuestDom>> = const { RefCell::new(None) };
}

impl presentation_events::Guest for Counter {
    fn mount(placement: &MountPoint) -> Result<Surface, PresentationError> {
        let (dom, surface) = GuestDom::mount(placement, counter)?;
        DOM.with_borrow_mut(|slot| {
            assert!(
                slot.replace(dom).is_none(),
                "counter incarnation mounted twice"
            );
        });
        Ok(surface)
    }

    fn dispatch(surface: &Surface, event: Event) -> EventResponse {
        DOM.with_borrow_mut(|slot| {
            slot.as_mut()
                .expect("counter was dispatched before mount")
                .dispatch(surface, event)
        })
    }
}

bindings::export!(Counter with_types_in bindings);
