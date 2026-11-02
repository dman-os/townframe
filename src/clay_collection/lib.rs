//! The shared collection substrate (ADR 017).
//!
//! The waist is leased partial replication. The authority publishes every transition of
//! a collection into a [`Projection`]; readers acquire a [`ProjectionLease`] and read
//! forward: whole [`Transaction`]s inline in bounded [`Page`]s, an opaque [`Cursor`] to
//! acknowledge with, long polling (parking, never spinning) when caught up, and a
//! `bootstrap_complete` boundary instead of a separate hydration phase. A reset begins a
//! new projection generation; every stale cursor then fails as data —
//! [`ReadError::ResetRequired`] — so the reader clears its [`Mirror`] and re-bootstraps.
//!
//! Optimism is host-side and travels on the rows: a [`Change`] carries
//! [`RowEvidence::Pending`] or [`RowEvidence::Authoritative`], and an overlay in the
//! [`Ledger`] retires when the authoritative row it predicted arrives.
//!
//! [`RegistrationCollection`] is the owner-scoped contribution primitive: rows keyed by
//! writer, compare-and-set mutations, and withdrawal of a writer's rows when it closes.

mod ledger;
mod mirror;
mod projection;
mod registration;
mod replica;
mod source;
mod types;

#[cfg(test)]
mod tests;

pub use ledger::Ledger;
pub use ledger::Overlay;
pub use ledger::OverlayWait;
pub use mirror::Mirror;
pub use mirror::ReadTicket;
pub use projection::Projection;
pub use projection::ProjectionLease;
pub use projection::ProjectionRead;
pub use registration::RegistrationCollection;
pub use registration::RegistrationWriter;
pub use replica::Entry;
pub use replica::Replica;
pub use source::Source;
pub use types::AcquireError;
pub use types::Change;
pub use types::Cursor;
pub use types::MutationError;
pub use types::OperationId;
pub use types::Page;
pub use types::ReadError;
pub use types::ReadLimits;
pub use types::RowEvidence;
pub use types::ScopeId;
pub use types::Transaction;
pub use types::Version;
pub use types::WriteOutcome;
pub use types::WriterId;
