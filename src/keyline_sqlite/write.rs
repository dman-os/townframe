//! The insert: a batch of certificates into the tables of a transaction someone else owns.

use keyhive_codec::traits::Encode;
use keyline::certificate::VerifiedCertificate;
use keyline::contract::Version;
use sqlite_rs::Executor;
use sqlite_rs::Sql;

use crate::error::Error;
use crate::rows::CertificateRow;
use crate::sqlite::queries;

/// Headroom under SQLite's default `SQLITE_MAX_VARIABLE_NUMBER` of 32766.
const MAX_PARAMS: usize = 30_000;

/// What [`SqliteKeyline::insert_in`](crate::SqliteKeyline::insert_in) did inside a transaction
/// that has not ended: the rows are in it and the version has moved, both only for whoever
/// reads through that transaction until it commits.
///
/// Hand it to [`SqliteKeyline::committed`](crate::SqliteKeyline::committed) after the
/// transaction commits, so the backend's verification memo knows the batch. Drop it after a rollback:
/// nothing outside the transaction ever saw the batch.
#[must_use = "pass it to `SqliteKeyline::committed` once the transaction commits, or drop it after a rollback"]
pub struct Staged<W> {
    /// The version the transaction read before it inserted.
    pub(crate) base: i64,
    /// The version after the insert: `base`, or one more if the batch added a certificate.
    pub(crate) version: i64,
    /// The batch with the bytes written for each, if it added anything; some of it may have been in
    /// the set already. Handed to the verification memo once the transaction commits.
    pub(crate) warm: Vec<(Vec<u8>, VerifiedCertificate<W>)>,
}

impl<W> Staged<W> {
    /// The version of the set after the transaction commits: what
    /// [`Keyline::insert`](keyline::contract::Keyline::insert) would return.
    pub fn version(&self) -> Version {
        Version::new(u64::try_from(self.version).expect("a stored version is not negative"))
    }

    /// Whether the batch added a certificate and so moved the version.
    pub fn changed(&self) -> bool {
        self.version != self.base
    }
}

/// Add `batch` through `tx`, which must hold the database's writer.
///
/// `INSERT OR IGNORE` leaves certificates already in the set alone and reports how many rows
/// it added, so the version moves exactly when the set grew (by one for the whole batch).
pub async fn insert_in<W: Encode>(
    tx: &impl Executor,
    batch: Vec<VerifiedCertificate<W>>,
) -> Result<Staged<W>, Error> {
    let base = queries::meta_version(tx).await?;
    let mut inserted = 0;
    let mut warm = Vec::with_capacity(batch.len());
    for chunk in batch.chunks(MAX_PARAMS / CertificateRow::WIDTH) {
        let mut sql = Sql::new(
            "INSERT OR IGNORE INTO keyline_certificates\n     ( kind\n     , digest\n     , issuer\n     , audience\n     , subject\n     , power\n     , citation\n     , revoke\n     , retain\n     , signed\n     )\nVALUES ",
        );
        for (index, cert) in chunk.iter().enumerate() {
            if index != 0 {
                sql.push(", ");
            }
            let row = CertificateRow::of(cert);
            warm.push((row.signed().to_vec(), cert.clone()));
            sqlite_rs::BindTuple::bind_tuple(row, &mut sql);
        }
        inserted += sql.execute(tx).await?.rows_affected;
    }
    if inserted == 0 {
        return Ok(Staged {
            base,
            version: base,
            warm: Vec::new(),
        });
    }
    queries::bump_version(tx).await?;
    Ok(Staged {
        base,
        version: base + 1,
        warm,
    })
}
