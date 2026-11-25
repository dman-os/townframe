//! `big_automata`'s part store, okv tables and the driver's peer cursors over SQLite.
//!
//! [`SqlitePartStore`] implements [`PartStoreWrite`](big_automata::part_store::PartStoreWrite),
//! [`PartStoreRead`](big_automata::part_store::PartStoreRead),
//! [`PartStoreServe`](big_automata::part_store::PartStoreServe) and
//! [`RevSource`](big_automata::rev::RevSource) with the semantics of the memory store, which
//! the laws in `big_automata::part_store::laws` hold it to; [`SqlitePeerCursors`] implements the
//! driver's `PeerCursors`; [`SqliteOkv`] implements `big_automata::okv::OkvStore` over tables of
//! the same database, in the part store's own transactions. All live in one schema
//! (`sqlite/migrations`), which the embedder hands to its database as [`MIGRATIONS`] under
//! [`MIGRATION_TABLE`].
//!
//! # Shape
//!
//! - **One write transaction is one SQLite write transaction.** [`SqlitePartStore::begin`] takes
//!   the writer (`BEGIN IMMEDIATE`) and every staged batch is applied inside it at once, so the
//!   transaction reads its own writes and nothing is visible to readers before
//!   [`commit`](SqlitePartTx::commit). The rev is `meta.rev + 1`, read once at `begin`; a
//!   transaction that changed no log row writes no rev and wakes nobody.
//! - **A batch is a unit of work.** Each batch loads the rows it needs (objects, their member
//!   rows, the touched buckets) with a few `IN` queries, decides in Rust exactly as the memory
//!   store does, and flushes with multi-row upserts. A bucket row is read and written once per
//!   batch, not once per member, so the root bucket is not a hot row inside a batch.
//! - **Every serve read is one read transaction** (one snapshot): verdicts, rows and payloads
//!   are read together, so no grant or revocation commits between deciding and reading.
//! - **Log reads are k-way merges of index ranges.** Each route (a part, an object) is one
//!   range query bounded by the page's size, merged in Rust, so a page costs the page, not the
//!   backlog. See `log.rs`.
//! - **Keys are stored as order-preserving blobs** ([`keys`]): the derived `Ord` of
//!   `big_automata`'s keys is length-first for short keys, not byte order.
//! - **Platforms.** The store is generic over [`PartDb`], the three things it needs of a
//!   database handle (a write transaction, a read snapshot, finishing them). `sqlite_rs`'s
//!   native [`Database`](sqlite_rs::Database) implements it; a component guest implements it
//!   over its `townframe:sqlite` connection. There is no browser build: `sqlite_rs` has none.

mod interlude {
    pub(crate) use std::collections::BTreeMap;
    pub(crate) use std::collections::BTreeSet;
    pub(crate) use std::sync::Arc;

    pub(crate) use big_automata::BuckId;
    pub(crate) use big_automata::ObjKey;
    pub(crate) use big_automata::PartKey;
    pub(crate) use big_automata::PeerKey;
    pub(crate) use utils_rs::prelude::*;
}

use sqlite_rs as sqlite_rs_alias;

mod sqlite {
    pub mod queries {
        include!("sqlite/gen.rs");
    }
}

mod buckets;
mod cursors;
mod db;
pub mod keys;
mod log;
mod okv;
mod read;
mod serve;
mod sql;
mod store;
mod write;

#[cfg(test)]
mod suite;

pub use cursors::SqlitePeerCursors;
pub use db::PartDb;
pub use okv::SqliteOkv;
pub use sqlite::queries::MIGRATION_TABLE;
pub use sqlite::queries::MIGRATIONS;
pub use store::SqlitePartStore;
pub use store::SqlitePartTx;
