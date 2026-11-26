//! What the store can fail with.

use keyhive_codec::error::DecodeError;
use keyhive_crypto::verified::VerifyError;
use sqlite_rs::SqlError;

/// A failure of the database, or of a stored row that is not what was kept.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("sqlite: {0}")]
    Sql(#[from] SqlError),

    /// A row of `table`, under `key`, does not decode, no longer verifies, or is not what its key
    /// says it is: the file was modified outside this store.
    #[error("stored row of {table} under {key:02x?} is not what was kept: {source}")]
    Stored {
        table: &'static str,
        key: Vec<u8>,
        #[source]
        source: StoredFault,
    },

    /// A secret column cannot be opened or a DEK cannot be unwrapped, which a wrong key-encryption
    /// key, a missing DEK row or a modified row all look like.
    #[error("secret of {table}: {source}")]
    Secret {
        table: &'static str,
        #[source]
        source: SecretFault,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum StoredFault {
    #[error("not a frame: {0}")]
    Frame(#[from] DecodeError),
    #[error("{0}")]
    Verify(#[from] VerifyError),
    /// The decoded value's identity (its digest, issuer or public key) is not its key's.
    #[error("its identity is not the row's")]
    Identity,
}

#[derive(Debug, thiserror::Error)]
pub enum SecretFault {
    /// The embedder's [`KekSource`](crate::KekSource) does not hold this version.
    #[error("no key-encryption key of version {0}")]
    NoKek(u64),
    /// A row names a DEK that is not in `kh_dek`.
    #[error("no data-encryption key {id}@{version}")]
    NoDek { id: String, version: i64 },
    #[error("unknown algorithm {0:?}")]
    Algorithm(String),
    /// The AEAD tag did not verify: a wrong key, or a row (or its associated data) that changed.
    #[error("authentication failed")]
    Open,
    /// The plaintext is not the length of a key.
    #[error("not a key")]
    Shape,
}
