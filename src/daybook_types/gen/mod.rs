//! @generated
//! This module contains generated type definitions.
//! Do not edit manually - changes will be overwritten.

// Root types module (generated in gen/root.rs)
// Always available - root types are the primary types with all derives
pub mod root;

// Re-export all generated root types (always available)
pub use root::*;

// Automerge types module (generated in gen/automerge.rs)
// Minimal boundary types with only Hydrate/Reconcile derives
#[cfg(feature = "automerge")]
pub mod automerge;

// Don't re-export automerge types at root level - they're accessed via gen::automerge::* or automerge::*
// This prevents conflicts and makes it clear when automerge types are being used

// WIT types module (generated in gen/wit.rs)
// WIT types are in a separate namespace (wit::) so they don't conflict with root/automerge types
#[cfg(feature = "wit")]
pub mod wit;

// Don't re-export WIT types at root level - they're accessed via wit:: module
