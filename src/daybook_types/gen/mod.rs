//! @generated
//! This module contains generated type definitions.
//! Do not edit manually - changes will be overwritten.

// Root types module (generated in gen/root.rs)
// Only include when automerge is NOT enabled (to avoid uniffi symbol conflicts)
#[cfg(not(feature = "automerge"))]
pub mod root;

// Re-export all generated root types (only when automerge is not enabled)
#[cfg(not(feature = "automerge"))]
pub use root::*;

// Automerge types module (generated in gen/automerge.rs)
#[cfg(feature = "automerge")]
pub mod automerge;

#[cfg(feature = "automerge")]
pub use automerge::*;

// WIT types module (generated in gen/wit.rs)
// WIT types are in a separate namespace (wit::) so they don't conflict with root/automerge types
#[cfg(feature = "wit")]
pub mod wit;

// Don't re-export WIT types at root level - they're accessed via wit:: module
