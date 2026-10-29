//! Desktop guest selection for the shared composition runtime: the counter on Wasmtime,
//! loaded from the `DAYFRAME_COUNTER_COMPONENT` artifact (ADR 018 §7).
pub use dayframe_composition::Application;
pub use dayframe_composition::CompositionRuntime;

use clay_dioxus::NativeComponents;
use clay_micro_core::application::ApplicationRuntime;
use clay_micro_core::micros::MicroFactory;

/// Engine providers this shell links into the shared composition.
pub fn guest_providers(application: &ApplicationRuntime<NativeComponents>) -> Vec<MicroFactory> {
    vec![clay_mdk::wasm::provider(
        application,
        micro_counter_manifest::manifest(),
        || clay_mdk::wasm::component_package("counter"),
    )]
}

pub fn desktop_runtime() -> CompositionRuntime {
    CompositionRuntime::new(guest_providers)
}

#[cfg(test)]
mod counter_tests;

#[cfg(test)]
mod tests;
