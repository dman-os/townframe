//! Stratum 2: the live set of the disputed subjects an insert can change, by SQL rounds.
//!
//! Only a subject whose closure holds a delegation some revocation names can have a live set that
//! differs from its positive reach (`keyline_disputed`); every other subject answers from
//! stratum 1. The live set of a disputed subject is **maintained at insert**, in the inserting
//! transaction: after stratum 1 is extended, [`refresh_affected`] recomputes the live set of every
//! disputed subject the batch touched (a subject that gained a certificate or a standing node, or
//! one that stands over such a subject), so a stored live set is current at every committed
//! version and a read never computes it. A rollback takes the recomputation with it.
//!
//! What is computed, for the *roots* of the affected subjects (the nodes in their closures that are
//! the subject of some delegation; each hop is judged in its own subject's graph, the daisy-chain
//! rule, so the roots are closed under what the rounds read):
//!
//! 1. For each delegation about a root that a revocation names: whether its issuer's or
//!    audience's own revocation kills it (`total`), and the set of nodes a third party's
//!    revocation covers (the union of the revokers' admin reach, from stratum 1). Delegations
//!    with the same covered set share one exclusion context, keyed by the set's canonical
//!    signature (`keyline_ctx`): a revoker's spree of `k` revocations costs one context, which is
//!    obligation 1 of `evaluation-notes.md` section 7 (k^3 to k^2).
//! 2. The live relation `live(ctx, lvl, s, n)` in every context in use, seeded by root edges and
//!    grown by semi-naive rounds until a round inserts nothing (`live_round.sql`). A node stands
//!    over itself unless the context excludes it; a hop conducts only if it is live in its own
//!    context rooted at its own subject.
//!
//! Obligation 2 (an inert revocation costs a lookup, not a search) holds because a delegation
//! whose covered set is empty gets context 0 and no row, and a revocation that covers nothing
//! reachable adds no context.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;

use sqlite_rs::BindTuple;
use sqlite_rs::Executor;
use sqlite_rs::Sql;

use crate::error::Error;
use crate::planted::Checked;
use crate::sqlite::queries;

/// Rows per statement when writing exclusion sets and disputes.
const CHUNK: usize = 300;

/// What recomputing the live sets of the affected subjects cost.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Refresh {
    /// Disputed subjects whose live set was recomputed.
    pub subjects: usize,
    /// Rounds that inserted anything, plus the final one that did not.
    pub rounds: usize,
    /// Contexts in use: the empty one and each distinct covered set.
    pub contexts: usize,
    /// Rows of `keyline_live` the refresh wrote.
    pub facts: u64,
}

struct Disputed {
    issuer: Vec<u8>,
    audience: Vec<u8>,
    revokers: Vec<Vec<u8>>,
}

/// Recompute the live set of the disputed subjects the batch touched.
///
/// `first` is the first round of stratum 1 for this batch: the reach rows from it on are the batch's.
/// A batch that touches no disputed subject costs the statements that find that out; a database
/// with no disputed subject, one.
pub async fn refresh_affected(tx: &impl Executor, first: i64) -> Result<Refresh, Error> {
    let tx = &Checked(tx);
    if !queries::has_disputed(tx).await? || crate::planted::skips_refresh() {
        return Ok(Refresh::default());
    }
    queries::touched_delegations(tx).await?;
    queries::touched_named(tx).await?;
    queries::touched_reach(tx, first).await?;
    queries::touched_admin(tx, first).await?;
    queries::affected_disputed(tx).await?;
    queries::clear_touched(tx).await?;
    if !queries::has_affected(tx).await? {
        return Ok(Refresh::default());
    }
    let subjects = queries::count_affected(tx).await?;
    let mut refresh = compute(tx).await?;
    refresh.subjects = usize::try_from(subjects).expect("a count is not negative");
    queries::clear_affected(tx).await?;
    Ok(refresh)
}

/// The live sets of the roots of the subjects in `keyline_affected`.
async fn compute(tx: &impl Executor) -> Result<Refresh, Error> {
    queries::clear_roots(tx).await?;
    queries::clear_ctx_use(tx).await?;
    queries::claim_roots(tx).await?;
    // What revocations do to each delegation about a root.
    let mut disputed: BTreeMap<Vec<u8>, Disputed> = BTreeMap::new();
    for row in queries::disputed_delegations(tx).await? {
        disputed
            .entry(row.delegation)
            .or_insert_with(|| Disputed {
                issuer: row.issuer,
                audience: row.audience,
                revokers: Vec::new(),
            })
            .revokers
            .push(row.revoker);
    }
    queries::clear_disputes(tx).await?;
    let mut admin_reach: HashMap<Vec<u8>, Vec<Vec<u8>>> = HashMap::new();
    let mut contexts: HashMap<Vec<u8>, i64> = HashMap::new();
    let mut rows: Vec<(Vec<u8>, i64, i64)> = Vec::new();
    for (delegation, dispute) in disputed {
        let total = dispute
            .revokers
            .iter()
            .any(|revoker| *revoker == dispute.issuer || *revoker == dispute.audience);
        let mut covered: BTreeSet<Vec<u8>> = BTreeSet::new();
        for revoker in &dispute.revokers {
            if *revoker == dispute.issuer || *revoker == dispute.audience {
                continue;
            }
            if !admin_reach.contains_key(revoker) {
                let reach: Vec<Vec<u8>> = queries::admin_reach_of(tx, revoker.clone())
                    .await?
                    .into_iter()
                    .collect();
                admin_reach.insert(revoker.clone(), reach);
            }
            covered.insert(revoker.clone());
            covered.extend(admin_reach[revoker].iter().cloned());
        }
        let ctx = if covered.is_empty() {
            0
        } else {
            let signature: Vec<u8> = covered.iter().flatten().copied().collect();
            if let Some(&ctx) = contexts.get(&signature) {
                ctx
            } else {
                queries::put_ctx(tx, signature.clone()).await?;
                let ctx = queries::ctx_of(tx, signature.clone()).await?;
                for chunk in covered.iter().collect::<Vec<_>>().chunks(CHUNK) {
                    let mut sql = Sql::new("INSERT INTO keyline_excl (ctx, node) VALUES ");
                    for (index, node) in chunk.iter().enumerate() {
                        if index != 0 {
                            sql.push(", ");
                        }
                        (ctx, (*node).clone()).bind_tuple(&mut sql);
                    }
                    sql.push(" ON CONFLICT DO NOTHING");
                    sql.execute(tx).await?;
                }
                contexts.insert(signature, ctx);
                ctx
            }
        };
        if total || ctx != 0 {
            rows.push((delegation, ctx, i64::from(total)));
        }
    }
    for chunk in rows.chunks(CHUNK) {
        let mut sql = Sql::new("INSERT INTO keyline_dispute (delegation, ctx, total) VALUES ");
        for (index, row) in chunk.iter().enumerate() {
            if index != 0 {
                sql.push(", ");
            }
            row.clone().bind_tuple(&mut sql);
        }
        sql.execute(tx).await?;
    }
    queries::claim_ctx_use(tx).await?;

    // The live relation of every root, in every context in use.
    queries::clear_live(tx).await?;
    let mut epoch = queries::meta_epoch(tx).await? + 1;
    let mut facts = queries::live_seed(tx, epoch).await?.rows_affected;
    let mut refresh = Refresh {
        contexts: contexts.len() + 1,
        ..Refresh::default()
    };
    let mut added = facts;
    while added > 0 {
        let previous = epoch;
        epoch += 1;
        added = queries::live_round(tx, epoch, previous).await?.rows_affected;
        facts += added;
        refresh.rounds += 1;
    }
    refresh.facts = facts;
    queries::set_epoch(tx, epoch).await?;
    queries::clear_roots(tx).await?;
    queries::clear_ctx_use(tx).await?;
    Ok(refresh)
}
