//! The SQLite home of a keyhive replica's state: [`SqliteKeyhiveStore`] implements keyhive's
//! [`KeyhiveStore`](keyhive_core::store::KeyhiveStore) over the tables in `sqlite/migrations`,
//! which the embedder hands to its database as [`MIGRATIONS`] under [`MIGRATION_TABLE`].
//!
//! # What it holds
//!
//! Group membership is not here; it is the Keyline certificate set (`keyline_sqlite`). This
//! crate holds what a keyhive cannot recompute, one entity per table, all prefixed `kh_`:
//!
//! | table | key | holds |
//! |---|---|---|
//! | `kh_key_pair` | public key | the secret keys this identity holds: its prekeys and the leaf keys it sampled for CGKA groups. **Sealed.** |
//! | `kh_prekey_op` | `(individual, digest)` | each individual's signed prekey operations |
//! | `kh_cgka_op` | `(doc, digest)` | each document's signed CGKA operations, in a rowid table so the digest index carries no operation bytes |
//! | `kh_cgka_op_pred` | `(doc, digest, predecessor)` | the CGKA dependency graph without the operations, with a reverse index `(doc, predecessor, digest)` |
//! | `kh_content_heads` | doc | the content heads the application last set, replaced on each set |
//! | `kh_content_key` | `(doc, content ref)` | the key of each piece of content this replica wrote or read. **Sealed.** |
//! | `kh_pending_event` | digest | events that waited for a dependency, as received |
//! | `kh_dek` | `(kind, version)` | the wrapped data-encryption keys of the sealed columns |
//!
//! Signed values are stored as their wire frames. Reading one decodes the frame, verifies the
//! signature, and compares its identity (digest, issuer or public key) with its row's, because
//! the trait promises verified values and the file may have been edited outside this crate; a row
//! that fails is [`Error::Stored`], never a value. A document appears in `list_documents` when it
//! has CGKA operations, and an individual in `list_individuals` when it has prekey operations;
//! each listing is a loose index scan whose cost follows the number of documents or individuals,
//! not of operations.
//!
//! The content keys are a durable table, not a cache: the trait allows a store to evict them, but
//! for content from before this replica joined a document, the key that arrived inside a later
//! envelope is the only copy. Nothing here evicts them.
//!
//! # Transactions
//!
//! A caller that commits keyhive rows together with sedimentree and Keyline rows owns the
//! transaction. It opens one writer transaction on the database (`SqliteDb::begin_write`), hands
//! it to each store's entry point, and commits once: for this crate, a [`Session`] from
//! [`SqliteKeyhiveStore::session`], whose methods are the trait's without a transaction of their
//! own. Rows written are visible to reads through the same transaction (read your writes) and to
//! nobody else until it commits; a rollback, or dropping the transaction, takes every row with it,
//! including a DEK created in it. Reads through a read snapshot are one consistent view, which
//! can be shared with Keyline reads on the same snapshot.
//!
//! Used as the [`KeyhiveStore`](keyhive_core::store::KeyhiveStore) a `Keyhive` is given, each
//! trait call is its own transaction (a snapshot for a read; a writer transaction, committed when
//! the call succeeds, for a write). The trait has no way to name a caller's transaction, so a
//! caller that wants the shared commit persists what a keyhive reports through its session
//! instead of leaning on the trait's writes (the trait says a keyhive never writes to its store).
//!
//! # What is encrypted, and how
//!
//! Two columns hold plaintext secrets: `kh_key_pair` (secret keys) and `kh_content_key`
//! (content keys). Each is sealed with ChaCha20-Poly1305 under a **data-encryption key** (DEK),
//! the construction of `secrets_rs::encrypt_blob` (random 12-byte nonce, the row's key as
//! associated data), so a sealed value cannot be moved to another row and still open. There is one DEK
//! per kind of column (`key_pair`, `content_key`), so a kind can be re-keyed independently, and a
//! row records the `(kind, version)` of the DEK that sealed it.
//!
//! The DEKs are in `kh_dek`, themselves sealed under a **key-encryption key** (KEK) the embedder
//! supplies through [`KekSource`]: 32 bytes and a version, kept outside the database (an OS
//! keyring, a KMS, a file elsewhere). A bare [`Kek`] is a source of itself. Rotating the KEK is
//! done by adding the new one to the source and calling [`Session::rewrap_deks`], which re-wraps
//! each DEK under the current key without touching a secret row; afterwards the old key can be
//! retired. A DEK is made in the transaction of the first write that needs one, and the cleartext
//! DEK is held only for the length of one call, so one made in a transaction that rolls back is
//! never remembered. A wrong key, a retired version or a modified row is an [`Error::Secret`], never
//! a wrong secret.
//!
//! What is not sealed: public keys, signed operations and events, content heads and the content
//! references are plaintext. A pending event carries no secret (a CGKA operation's secrets are
//! encrypted to leaf keys inside it). A file copy therefore shows which documents and individuals
//! exist, and who holds a key, but no key. This crate does not depend on `secrets_rs`, whose
//! platform keyring and tokio dependencies do not build for the browser; it reuses the same AEAD
//! and layout directly, and an embedder with a `secrets_rs::SecretStore` can implement
//! [`KekSource`] over it.
//!
//! # Platforms
//!
//! Generic over [`sqlite_rs::SqliteDb`], the handle that opens read snapshots and the writer
//! transaction, so the crate builds for the browser without a browser backend.
//! placeholder
mod error;
mod secret;
mod session;
mod store;

mod sqlite {
    pub mod queries {
        include!("sqlite/gen.rs");
    }
}

use sqlite_rs as sqlite_rs_alias;

pub use error::Error;
pub use error::SecretFault;
pub use error::StoredFault;
pub use secret::Kek;
pub use secret::KekSource;
pub use session::Session;
pub use sqlite::queries::MIGRATION_TABLE;
pub use sqlite::queries::MIGRATIONS;
pub use store::SqliteKeyhiveStore;

#[cfg(test)]
mod suite;
