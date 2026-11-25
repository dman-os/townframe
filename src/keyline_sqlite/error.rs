//! What the backend can fail with.

use keyhive_crypto::verified::VerifyError;
use keyline::certificate::CertificateId;
use sqlite_rs::SqlError;

/// A failure of the database, or a stored certificate that is not what was inserted.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("sqlite: {0}")]
    Sql(#[from] SqlError),

    /// A row's `signed` bytes do not decode as a certificate, or no longer verify: the file was
    /// modified outside this backend. Only a load that rebuilds the evaluation (see the crate
    /// docs) re-verifies, so this is how such a modification surfaces.
    #[error("stored certificate {id:?} is not what was inserted: {source}")]
    Stored {
        id: CertificateId,
        #[source]
        source: StoredFault,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum StoredFault {
    #[error("not a certificate frame: {0}")]
    Frame(#[from] keyhive_codec::error::DecodeError),
    #[error("{0}")]
    Verify(#[from] VerifyError),
    #[error("its identity is {found:?}, not the row's")]
    Identity { found: CertificateId },
}
