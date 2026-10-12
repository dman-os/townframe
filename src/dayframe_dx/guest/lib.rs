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

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
