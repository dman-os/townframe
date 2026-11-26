//! The Keyline authority graph in SQLite: [`SqliteKeyline`] implements
//! [`keyline::contract::Keyline`] over the tables in `sqlite/migrations`, which the embedder
//! hands to its database as [`MIGRATIONS`] under [`MIGRATION_TABLE`].
//!
//! # Shape
//!
//! - **The tables are the set.** `keyline_certificates` holds every certificate keyed by
//!   identity (`(kind, digest)` is `CertificateId` order), with the fields the queries need
//!   copied beside the signed bytes; `keyline_meta` holds the version. The set-shaped queries
//!   (`contains`, `get`, `certificates`, `revocations_of`, `delegations_to`, `subjects`,
//!   `digest`) are SQL, each served by an index (`suite/plans.rs` checks it).
//! - **The evaluation is cached in memory, keyed by version.** The stratified evaluation
//!   (positive reach, admin reach and coverage, then the live set and edge caps, both with
//!   nested fixed points over per-revocation exclusion contexts) is `keyline_memory::State`'s,
//!   rebuilt from the rows when a query's snapshot reads a version the cache is not at, and
//!   carried forward by each commit this handle makes. It is not recursive CTEs: stratum 1's
//!   member-inheritance rule is non-linear recursion (a node's reach is composed from other
//!   nodes' reach), which SQLite's single-reference recursive CTEs cannot express, and stratum
//!   2's exclusion contexts need a search per distinct revoker set inside a fixed point. A
//!   Rust-driven fixpoint over temp tables is possible and is the next step if a relay needs
//!   evaluation without holding the set in memory; the laws and the differential here are what
//!   such a backend would be held to.
//! - **A rebuild verifies again.** `insert` takes `VerifiedCertificate`s, but a cache miss
//!   reads rows, and the rows are not a witness. Each row is decoded, its signature checked and
//!   its identity compared with its key's ([`Error::Stored`]), so a file edited outside the
//!   backend fails the rebuild that reads it instead of lending its rows authority. The cost is
//!   one signature check per certificate per rebuild; a handle that wrote every commit it later
//!   reads (the common case) never pays it.
//! - **Every query is one snapshot.** The version is read in the same read transaction as the
//!   rows, so an answer carries the version of the set it read.
//!
//! # Sharing a transaction
//!
//! A grant that must commit with other rows (a document's sedimentree rows and part store, in
//! one database) goes through [`SqliteKeyline::insert_in`], which writes through a transaction
//! the caller owns and returns a [`Staged`]; after the caller commits, [`SqliteKeyline::committed`]
//! brings the cache along. A rollback drops the transaction, which drops the rows and the
//! version bump with it, and the `Staged` is simply dropped: the cache is never told of a batch
//! that did not commit. Skipping `committed` after a commit is safe, only slower: the next query
//! sees a version the cache is not at and rebuilds.
//!
//! # Platforms
//!
//! Generic over [`sqlite_rs::SqliteDb`], the handle that opens read snapshots and the writer
//! transaction (`big_automata_sqlite` runs on the same trait). `sqlite_rs`'s native `Database`
//! implements it; a component guest implements it over its `townframe:sqlite` connection. There
//! is no browser build: `sqlite_rs` has none.

mod cache;
mod error;
mod rows;
mod store;
mod write;

mod sqlite {
    pub mod queries {
        include!("sqlite/gen.rs");
    }
}

use sqlite_rs as sqlite_rs_alias;

pub use error::Error;
pub use error::StoredFault;
pub use sqlite::queries::MIGRATION_TABLE;
pub use sqlite::queries::MIGRATIONS;
pub use store::SqliteKeyline;
pub use write::Staged;

#[cfg(test)]
mod suite;

// Not on wasm: the workloads open a native `sqlite_rs::Database`.
#[cfg(all(any(test, feature = "bench-support"), not(target_family = "wasm")))]
pub mod workloads;
