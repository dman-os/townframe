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
//! - **Evaluation runs over the subject's closure, loaded on demand.** A query about `S` reads
//!   `closure(S)` (`S`, every node with standing over it in the positive graph, every certificate
//!   about those nodes and every revocation naming them) through indexed statements, one round
//!   per level of derivation depth, and hands exactly those rows to `keyline_memory::State`, the
//!   reference evaluator. The model document proves nothing outside the closure can change the
//!   answer, so the backend agrees with the reference by construction; the differential in
//!   `suite/differential.rs` checks it on random sets. Nothing is held between queries but a
//!   bounded verification memo. See [`closure`] for the rounds and why a certificate that names a
//!   node without standing costs one row, and `docs/scratch/hive-store-design.md` for the
//!   alternatives (materializing the monotone stratum at insert is the next step if closure
//!   loading shows up in a profile).
//! - **A read verifies.** `insert` takes `VerifiedCertificate`s, but the rows are not a witness.
//!   Each row a query reads is decoded, its signature checked and its identity compared with its
//!   key's ([`Error::Stored`]), so a file edited outside the backend fails the query that reads
//!   it instead of lending its rows authority. A memo of verified rows, trusted only for the exact
//!   bytes it verified, keeps that to one check per row per handle.
//! - **Every query is one snapshot.** The version is read in the same read transaction as the
//!   rows, so an answer carries the version of the set it read.
//!
//! # Sharing a transaction
//!
//! A grant that must commit with other rows (a document's sedimentree rows and part store, in
//! one database) goes through [`SqliteKeyline::insert_in`], which writes through a transaction
//! the caller owns and returns a [`Staged`]; after the caller commits, [`SqliteKeyline::committed`]
//! tells the verification memo. A rollback drops the transaction, which drops the rows and the
//! version bump with it, and the `Staged` is simply dropped. Skipping `committed` after a commit
//! is safe, only slower: the first read verifies. A caller that answers several questions in a
//! snapshot of its own, beside reads of other stores, uses [`SqliteKeyline::evaluate_in`].
//!
//! # Platforms
//!
//! Generic over [`sqlite_rs::SqliteDb`], the handle that opens read snapshots and the writer
//! transaction (`big_automata_sqlite` runs on the same trait). `sqlite_rs`'s native `Database`
//! implements it; a component guest implements it over its `townframe:sqlite` connection. There
//! is no browser build: `sqlite_rs` has none.

mod closure;
mod error;
mod memo;
mod rows;
mod store;
mod write;

mod sqlite {
    pub mod queries {
        include!("sqlite/gen.rs");
    }
}

use sqlite_rs as sqlite_rs_alias;

pub use closure::Evaluated;
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
