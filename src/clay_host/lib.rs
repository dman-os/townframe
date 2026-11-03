//! Privileged composition host. Micros receive bound handles, not this constructor surface.
//!
//! This crate is the authority kernel: lifecycle, tokens, component references, mount records,
//! and the engine-neutral activation supervisor (ADR 024). Component (wasm) hosting lives in the
//! engine crates and document data lives in `clay_docs`; neither is this crate's concern
//! (ADR 015 §3, §6).

mod instance;
mod supervisor;

pub use instance::AccessError;
pub use instance::Binding;
pub use instance::CloseReason;
pub use instance::ComponentRef;
pub use instance::Host;
pub use instance::HostStatus;
pub use instance::Instance;
pub use instance::Lifecycle;
pub use instance::Mount;
pub use instance::RendererRegistry;
pub use instance::RootMount;
pub use supervisor::CloseError;
pub use supervisor::ConfiguredMicro;
pub use supervisor::Factory;
pub use supervisor::Initialized;
pub use supervisor::Lease;
pub use supervisor::LocalFuture;
pub use supervisor::MicroId;
pub use supervisor::MicroState;
pub use supervisor::Requirement;
pub use supervisor::Root;
pub use supervisor::StartError;
pub use supervisor::Supervisor;

#[cfg(test)]
mod tests;
