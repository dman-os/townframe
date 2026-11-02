//! Privileged composition host. Micros receive bound handles, not this constructor surface.
//!
//! This crate is the authority kernel only: lifecycle, tokens, component references, and mount
//! records. Component (wasm) hosting lives in the engine crates and document data lives in
//! `clay_docs`; neither is this crate's concern (ADR 015 §3, §6).

mod instance;

pub use instance::AccessError;
pub use instance::CloseReason;
pub use instance::ComponentRef;
pub use instance::Host;
pub use instance::HostStatus;
pub use instance::Instance;
pub use instance::Mount;
pub use instance::RendererRegistry;
pub use instance::RootMount;

#[cfg(test)]
mod tests;
