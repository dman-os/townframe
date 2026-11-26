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
//! - **Stratum 1 is a table, extended at insert.** `keyline_reach` holds the positive reach in the
//!   threshold encoding, append-only. `insert_in` extends it inside the caller's transaction by
//!   semi-naive rounds, one SQL statement each, until a round inserts nothing ([`engine`]).
//! - **Stratum 2 is computed per disputed subject.** A subject whose closure holds a delegation some
//!   revocation names has a live set that can differ from its reach; it is computed by SQL rounds
//!   over exclusion contexts keyed by the excluded set's signature, and stored with the version it
//!   was computed at ([`stratum2`]). Every other subject answers from stratum 1.
//! - **Point queries are indexed reads**, a handful of rows however large the set. `members_for_subset`
//!   is the one question defined over an arbitrary subset, and runs `keyline_memory::State` over it.
//!   See `docs/scratch/hive-store-design.md` for the design, the measured costs and where it differs
//!   from the SQL sketch upstream publishes.
//! - **Every query is one snapshot.** The version is read in the same read transaction as the
//!   rows, so an answer carries the version of the set it read.
//!
//! # Sharing a transaction
//!
//! A grant that must commit with other rows (a document's sedimentree rows and part store, in
//! one database) goes through [`SqliteKeyline::insert_in`], which writes through a transaction
//! the caller owns and returns a [`Staged`]; after the caller commits, [`SqliteKeyline::committed`]
//! tells the verification memo. A rollback drops the transaction, which drops the rows, the derived reach
//! and the version bump with it, and the `Staged` is simply dropped. Skipping `committed` after a commit
//! is safe, only slower: the first read verifies. A caller that answers several questions in a
//! transaction of its own, beside writes of other stores, uses [`SqliteKeyline::members_in`] and
//! [`SqliteKeyline::effective_power_in`].
//!
//! # Platforms
//!
//! Generic over [`sqlite_rs::SqliteDb`], the handle that opens read snapshots and the writer
//! transaction (`big_automata_sqlite` runs on the same trait). `sqlite_rs`'s native `Database`
//! implements it; a component guest implements it over its `townframe:sqlite` connection. There
//! is no browser build: `sqlite_rs` has none.

mod closure;
mod engine;
mod error;
mod memo;
mod planted;
mod read;
mod rows;
mod stratum2;
mod store;
mod write;

mod sqlite {
    pub mod queries {
        include!("sqlite/gen.rs");
    }
}

use sqlite_rs as sqlite_rs_alias;

pub use engine::Extension;
pub use error::Error;
pub use error::StoredFault;
pub use sqlite::queries::MIGRATION_TABLE;
pub use sqlite::queries::MIGRATIONS;
pub use stratum2::Refresh;
pub use store::SqliteKeyline;
pub use write::Staged;

#[cfg(test)]
mod suite;

// Not on wasm: the workloads open a native `sqlite_rs::Database`.
#[cfg(all(any(test, feature = "bench-support"), not(target_family = "wasm")))]
pub mod workloads;
