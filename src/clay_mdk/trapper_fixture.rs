#![cfg(target_arch = "wasm32")]
//! Fixture: a micro whose `view` is one button, and whose click traps the guest (`unreachable`)
//! inside the synchronous `presentation.dispatch` export. Pins that a guest trap in a sync export
//! faults only that micro, with the trap as the cause, on every engine: the browser's engine
//! reports a trap through a promise, so a host that polls the export once must still see it.

mod trapper_manifest;

use clay_mdk::Context;
use clay_mdk::Error;
use dioxus::prelude::*;

/// Traps with an explicit `()` return: a closure that diverges infers `!`, which a Dioxus event
/// handler does not accept.
fn trap() {
    core::arch::wasm32::unreachable()
}

async fn activate(context: Context) -> Result<(), Error> {
    context.publish::<trapper_manifest::TrapperView>(|| {
        rsx! {
            button {
                "aria-label": "Trap the guest",
                onclick: move |_| trap(),
                "Trap"
            }
        }
    })?;
    Ok(())
}

clay_mdk::export_micro!(activate, trapper_manifest::manifest);
