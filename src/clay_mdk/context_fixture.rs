#![cfg(target_arch = "wasm32")]
//! The `context` fixture component: the authoring-surface micro of `context_micro.rs`, exported
//! as a guest by `export_micro!`. See there for what it does and which conformance scenarios
//! drive it.
mod context_micro;

clay_mdk::export_micro!(context_micro::activate, context_micro::manifest);
