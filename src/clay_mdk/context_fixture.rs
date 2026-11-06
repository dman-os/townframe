#![cfg(target_arch = "wasm32")]
//! The `context` fixture component: the authoring-surface micro of `context_micro.rs`, exported
//! as a guest by `export_micro!`. See there for what it does and which conformance scenarios
//! drive it.
#[path = "context_micro.rs"]
mod micro;

clay_mdk::export_micro!(micro::activate, micro::manifest);
