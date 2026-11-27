//! The one evaluation the tables cannot answer: a question about an arbitrary subset of the set.
//!
//! `members_for_subset` is defined over exactly the certificates it is given, so the materialized
//! stratum 1 (derived from the whole set) does not apply. The certificates are read by identity,
//! verified again (a row is not a witness), and handed to `keyline_memory::State`, the reference
//! evaluator. This is the only place the reference runs in this crate.

use keyhive_codec::traits::Decode;
use keyhive_codec::traits::Encode;
use keyline::certificate::CertificateId;
use keyline_memory::state::State;
use sqlite_rs::QueryExecutor;
use sqlite_rs::Sql;

use crate::error::Error;
use crate::memo::Memo;

/// Bound variables per statement: well under SQLite's default limit of 32766 whatever the row.
const CHUNK: usize = 500;

/// The reference evaluator over the certificates with these identities, which the set may not all
/// hold.
pub async fn load_ids<W: Encode + Decode>(
    snapshot: &impl QueryExecutor,
    ids: impl IntoIterator<Item = CertificateId>,
    memo: &Memo<W>,
) -> Result<State<W>, Error> {
    let ids: Vec<(i64, Vec<u8>)> = ids.into_iter().map(crate::rows::kind_and_digest).collect();
    let mut certificates = Vec::new();
    for chunk in ids.chunks(CHUNK / 2) {
        let mut sql = Sql::new(
            "SELECT kind, digest, signed FROM keyline_certificates WHERE (kind, digest) IN ",
        );
        sql.bind_tuples(chunk.iter().cloned());
        let rows = sql.query(snapshot).await?.all_with(|row| {
            Ok((
                row.get::<i64>("kind")?,
                row.get::<Vec<u8>>("digest")?,
                row.get::<Vec<u8>>("signed")?,
            ))
        })?;
        for (kind, digest, signed) in rows {
            certificates.push(memo.verified(kind, &digest, &signed)?);
        }
    }
    let mut state = State::new();
    state.insert_all(certificates);
    Ok(state)
}
