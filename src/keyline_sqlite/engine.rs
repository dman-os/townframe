//! Stratum 1 at insert: extending the stored positive reach by the batch, inside the caller's
//! transaction.
//!
//! The reach is the least fixed point of one rule with two recursive premises (the threshold form
//! of `reaches`, `design/keyline/evaluation-notes.md` section 5), which SQL cannot state as one
//! recursive query. So each *round* is one statement and the loop is here: the batch's
//! delegations are joined against the stored reach (round zero), then each round joins only the
//! previous round's new rows against the delegations and the stored reach, until a round inserts
//! nothing. Facts are never deleted, so the computation is incremental by construction: a batch
//! costs the facts it derives, not the set.
//!
//! The same transaction marks the subjects a revocation puts in dispute (`keyline_disputed`),
//! which is what decides later whether a query needs stratum 2.

use sqlite_rs::BindTuple;
use sqlite_rs::Executor;
use sqlite_rs::Sql;

use crate::error::Error;
use crate::rows::NewRow;
use crate::stratum2;
use crate::stratum2::Refresh;
use crate::planted::Checked;
use crate::sqlite::queries;

/// Bound variables per statement when staging the batch: well under SQLite's limit.
const MAX_PARAMS: usize = 30_000;

/// What extending the reach cost.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Extension {
    /// Rounds after round zero that inserted anything, plus the final one that did not.
    pub rounds: usize,
    /// Rows of `keyline_reach` the batch added.
    pub facts: u64,
    /// What recomputing the live sets of the disputed subjects the batch touched cost.
    pub refresh: Refresh,
}

/// Extend stratum 1 by the certificates `batch` (kind and digest of each), which are already in
/// the tables of `tx`.
pub async fn extend(tx: &impl Executor, batch: Vec<NewRow>) -> Result<Extension, Error> {
    let tx = &Checked(tx);
    for chunk in batch.chunks(MAX_PARAMS / NewRow::WIDTH) {
        let mut sql = Sql::new(
            "INSERT INTO keyline_new (kind, digest, issuer, audience, subject, power, revoke) VALUES ",
        );
        for (index, row) in chunk.iter().enumerate() {
            if index != 0 {
                sql.push(", ");
            }
            row.clone().bind_tuple(&mut sql);
        }
        sql.push(" ON CONFLICT DO NOTHING");
        sql.execute(tx).await?;
    }

    let mut epoch = queries::meta_epoch(tx).await? + 1;
    let first = epoch;
    let mut facts = queries::reach_seed(tx, epoch).await?.rows_affected;
    let mut extension = Extension::default();
    let mut added = facts;
    while added > 0 {
        let previous = epoch;
        epoch += 1;
        added = queries::reach_round(tx, epoch, previous).await?.rows_affected;
        facts += added;
        extension.rounds += 1;
    }
    extension.facts = facts;

    epoch += 1;
    queries::dsubject_of_revocations(tx, epoch).await?;
    queries::dsubject_of_delegations(tx, epoch).await?;
    queries::disputed_from_dsubjects(tx, epoch).await?;
    queries::disputed_from_reach(tx, first).await?;
    queries::set_epoch(tx, epoch).await?;
    extension.refresh = stratum2::refresh_affected(tx, first).await?;
    queries::clear_new(tx).await?;
    Ok(extension)
}
