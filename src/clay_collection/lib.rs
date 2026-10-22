//! The shared collection substrate: one hop of a chain of hops.
//!
//! A hop holds a base replica (latest row per key, stamped by its authority) and a
//! segregated set of whole-row optimistic overlays. A query reads the visible set —
//! overlays layered over base — and is answered locally; demand is a separate, opaque
//! token sent upstream. The waist between hops carries [`Batch`]es and never names
//! "collection", so the far end may be another hop, a facet frontier, or a future
//! query source without the near end knowing.
//!
//! Overlays are whole-row overwrites, not derived predictions: a mutation produces the
//! row it wants visible, and the overlay is removed when the write is rejected, when
//! its writer's scope disposes, or when base reaches the version the write landed at —
//! at which point replication has already carried the value back. Contributions and
//! their overrides are ordinary rows in this model; only owner tags distinguish them,
//! and owner cleanup is a batch of removes, not an overlay.

mod ledger;
mod replica;
mod types;
mod view;

#[cfg(test)]
mod oracle;
#[cfg(test)]
mod tests;

pub use ledger::Ledger;
pub use ledger::Overlay;
pub use ledger::OverlayPolicy;
pub use ledger::OverlayWait;
pub use replica::Entry;
pub use replica::Replica;
pub use types::BaseDelta;
pub use types::Batch;
pub use types::Coverage;
pub use types::DemandId;
pub use types::DemandToken;
pub use types::OperationId;
pub use types::OverlayDelta;
pub use types::ScopeId;
pub use types::Version;
pub use types::WriteOutcome;
pub use view::Downstream;
pub use view::Event;
pub use view::Query;
pub use view::ReadOnly;
pub use view::RowChange;
pub use view::Subscription;
pub use view::Upstream;
pub use view::View;
pub use view::Writable;
