//! The Clay host's document collections over a node's cabinet (ADR 054).
//!
//! A micro never streams everything its grants reach. It mounts a query, and the host keeps
//! one feed per query and authority: one upstream `pollDatums` loop publishing into one
//! [`Projection`], which every lease on it reads from. The feed lives while a lease holds it,
//! and the last lease dropping stops the loop.
//!
//! A feed belongs to one authority: it polls with that authority's own capability, so the node
//! filters every page by the authority's reach (ADR 025 §2, §6) and the host holds no grants.
//! Today the authority is one micro, so its queries share feeds; values, shared by version
//! across authorities, come later.
//!
//! A poll reads the node's datum-set index, so a row carries a version, never a value: no feed
//! hydrates a document.

mod datums;
#[cfg(any(test, feature = "fake"))]
pub mod fake;

#[cfg(test)]
mod tests;

use big_cabinet_core::doc::DatumPage;
use big_cabinet_core::doc::DocsError;
use big_cabinet_core::doc::PollDatums;
use futures::future::BoxFuture;
use std::sync::Arc;

pub use datums::Authority;
pub use datums::DatumFeeds;
pub use datums::DatumLease;
pub use datums::DatumRow;
pub use datums::DatumSelector;
pub use datums::FeedError;
pub use datums::RowKey;

/// The node's document methods, as one authority reaches them.
///
/// The real implementation dispatches over XRPC, signed as the actor the capability in each
/// request is bound to. The fake answers from memory.
pub trait CabinetDocs: Send + Sync + 'static {
    fn poll_datums(&self, request: PollDatums) -> BoxFuture<'static, Result<DatumPage, DocsError>>;
}

/// Starts a host task. The host chooses the executor; a feed's loop is one such task.
pub type Spawn = Arc<dyn Fn(BoxFuture<'static, ()>) + Send + Sync>;
