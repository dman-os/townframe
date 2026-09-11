//! Willow-native entry storage for BigRepo collection synchronization.
//!
//! See `docs/adrs/010-willow-collection-sync.md`. The upstream `willow25` stores cannot be
//! used directly: they hold `Rc` over a single-threaded lock, and the upstream `Store`
//! trait is neither object safe nor able to bound a range read. This crate owns its store,
//! and offers an adapter to the upstream trait so the implementation remains a conforming
//! Willow store rather than only a BigRepo component.

/// Shared imports.
///
/// Every module here works with Willow's `Path` and `Timestamp`, so the crate cannot
/// glob-import `utils_rs::prelude`: it re-exports `std::path::Path` and `jiff::Timestamp`,
/// which would make both names ambiguous at every use site. The prelude is narrowed to what
/// `big_willow` actually needs from it.
///
/// `camino` is deliberately not used for Willow paths. `willow25::Path` components are
/// arbitrary byte strings, whereas `Utf8Path` requires UTF-8 and carries filesystem
/// semantics that do not apply to Willow's component-prefix model. BigRepo document
/// identifiers are binary keys, so they are not representable as UTF-8 at all.
mod interlude {
    pub use utils_rs::prelude::async_trait;
}

// The conformance checks are tests, so they need the dev-dependencies: `tokio` is a dev
// dependency, and a `#[cfg(feature = "conformance")]` module in a non-test build could not
// see it.
#[cfg(all(test, feature = "conformance"))]
mod conformance;
mod mem;
mod path_codec;
#[cfg(feature = "sqlite")]
mod sqlite;
mod store;
mod upstream;

pub use mem::MemStore;
pub use path_codec::{PathCodecError, PrefixRange, decode_path, encode_path, prefix_range};
#[cfg(feature = "sqlite")]
pub use sqlite::SqliteWillowStore;
pub use store::{AreaPage, AreaReadLimits, EntryKey, InsertOutcome, StoreError, WillowStore};
#[cfg(any(test, feature = "test-support"))]
pub use store::contract;
pub use upstream::UpstreamStore;
