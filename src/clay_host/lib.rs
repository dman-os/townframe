//! Privileged composition host. Micros receive bound handles, not this constructor surface.
//!
//! This crate is the authority kernel only: lifecycle, tokens, contribution collections,
//! component references, and mount records. Component (wasm) hosting lives in the engine
//! crates and document data lives in `clay_docs`; neither is this crate's concern
//! (ADR 015 §3, §6).
//!
//! Modules: [`instance`] owns lifecycle and capability authority and [`collection`] bridges that
//! authority to the `clay_collection` substrate.

pub use clay_collection::BaseDelta;
pub use clay_collection::Batch;
pub use clay_collection::DemandId;
pub use clay_collection::DemandToken;
pub use clay_collection::OverlayPolicy;
pub use clay_collection::ReadOnly;
pub use clay_collection::Replica;
pub use clay_collection::ScopeId;
pub use clay_collection::Version;
pub use clay_collection::View;
pub use clay_collection::Writable;

mod collection;
mod instance;

pub use collection::CollectionReader;
pub use collection::CollectionSubscription;
pub use collection::CollectionView;
pub use collection::CollectionWriter;
pub use collection::Entry;
pub use collection::HostedCollection;
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
