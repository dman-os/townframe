//! Loading the closure of a subject from the tables, one indexed round at a time.
//!
//! A query about `S` is answered by `keyline_memory::State`, the reference evaluator, run over
//! `closure(S)`: `S`, every node with standing over `S` in the positive graph (revocations
//! ignored), every certificate about those nodes, and every revocation naming one of those
//! certificates. The model document proves that no certificate outside it can change any answer
//! about `S`, so the evaluator agrees with itself over the whole set by construction.
//!
//! # The rounds
//!
//! 1. `nodes = {S}`. Fetch the delegations *about* the nodes not yet fetched (the index on
//!    `subject`) and the revocations naming them (the index on `revoke`).
//! 2. Compute the positive reach `R(n)` of every node from the edges fetched so far. This is the
//!    threshold program's stratum 1 at its lowest level, where existence is all that is asked: `x`
//!    is in `R(n)` when some delegation `iss -> x` is about a `sub` in `{n} u R(n)` whose issuer is
//!    in `{sub} u R(sub)`.
//! 3. `nodes = {S} u R(S)`. Anything in it not yet fetched is the next round's frontier; with none,
//!    the closure is complete.
//!
//! # Why a gift certificate costs a row
//!
//! A node enters the frontier only through a delegation whose *issuer already stands* in the
//! positive graph of what was loaded (step 2's grounding condition). A delegation that merely
//! names a node, signed by a key without standing, is fetched as a row about a node we load, and
//! its audience is not explored: a self-grounded ladder an attacker points at the subject is never
//! walked until it has standing over the subject. That is the subject-side-feed-first obligation
//! (`evaluation-notes.md` section 7, obligation 3). The other two obligations are `State`'s.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use keyhive_codec::traits::Decode;
use keyhive_codec::traits::Encode;
use keyhive_crypto::identifier::Identifier;
use keyline::certificate::CertificateId;
use keyline::certificate::VerifiedCertificate;
use keyline_memory::state::State;
use sqlite_rs::QueryExecutor;
use sqlite_rs::Sql;

use crate::error::Error;
use crate::memo::Memo;

/// Bound variables per statement: well under SQLite's default limit of 32766 whatever the row.
const CHUNK: usize = 500;

/// A set of certificates and the evaluator over it: what a query about one subject runs on.
pub struct Evaluated<W> {
    state: State<W>,
    rows_read: usize,
}

impl<W> Evaluated<W> {
    /// The reference evaluator over exactly the certificates loaded, for a query about the subject
    /// that was loaded (or, from `load_ids`, about that set).
    pub fn state(&self) -> &State<W> {
        &self.state
    }

    /// The evaluator, owned.
    pub fn into_state(self) -> State<W> {
        self.state
    }

    /// How many certificate rows the load read: what a point query costs in rows.
    pub fn rows_read(&self) -> usize {
        self.rows_read
    }
}

/// One row of a load: `(kind, digest, signed)`.
type Row = (i64, Vec<u8>, Vec<u8>);

async fn fetch(snapshot: &impl QueryExecutor, sql: Sql) -> Result<Vec<Row>, Error> {
    Ok(sql
        .query(snapshot)
        .await?
        .all_with(|row| Ok((row.get("kind")?, row.get("digest")?, row.get("signed")?)))?)
}

// `INDEXED BY` on both: with an `IN` list the planner otherwise prefers the primary key's `kind`
// range, which reads every delegation (or revocation) in the set to keep a few. `plans.rs` fails if
// either statement stops using its index.
fn about(subjects: &[Identifier]) -> Sql {
    let mut sql =
        Sql::new("SELECT kind, digest, signed FROM keyline_certificates INDEXED BY keyline_delegations_by_subject_audience WHERE kind = 0 AND subject IN ");
    sql.bind_list(subjects.iter().map(|subject| subject.to_bytes().to_vec()));
    sql
}

fn naming(delegations: &[Vec<u8>]) -> Sql {
    let mut sql =
        Sql::new("SELECT kind, digest, signed FROM keyline_certificates INDEXED BY keyline_revocations_by_target WHERE kind = 1 AND revoke IN ");
    sql.bind_list(delegations.iter().cloned());
    sql
}

/// Load `closure(subject)` through `snapshot` and put the evaluator over it.
pub async fn load<W: Encode + Decode>(
    snapshot: &impl QueryExecutor,
    subject: Identifier,
    memo: &Memo<W>,
) -> Result<Evaluated<W>, Error> {
    let mut certificates: Vec<VerifiedCertificate<W>> = Vec::new();
    let mut fetched: BTreeSet<Identifier> = BTreeSet::new();
    // `subject -> (issuer, audience)` of every delegation about it.
    let mut edges: BTreeMap<Identifier, Vec<(Identifier, Identifier)>> = BTreeMap::new();
    // `n -> R(n)`: the nodes with positive standing over `n`, for every node that matters.
    let mut reach: BTreeMap<Identifier, BTreeSet<Identifier>> = BTreeMap::new();
    reach.insert(subject, BTreeSet::new());

    let mut frontier = vec![subject];
    let mut rows_read = 0;
    while !frontier.is_empty() {
        fetched.extend(frontier.iter().copied());
        for chunk in frontier.chunks(CHUNK) {
            let mut delegations = Vec::new();
            for (kind, digest, signed) in fetch(snapshot, about(chunk)).await? {
                rows_read += 1;
                let certificate = memo.verified(kind, &digest, &signed)?;
                if let VerifiedCertificate::Delegation(delegation) = &certificate {
                    edges
                        .entry(delegation.payload().subject)
                        .or_default()
                        .push((delegation.issuer(), delegation.payload().audience));
                    delegations.push(digest);
                }
                certificates.push(certificate);
            }
            for chunk in delegations.chunks(CHUNK) {
                for (kind, digest, signed) in fetch(snapshot, naming(chunk)).await? {
                    rows_read += 1;
                    certificates.push(memo.verified(kind, &digest, &signed)?);
                }
            }
        }
        grow(&edges, &mut reach, subject);
        frontier = reach
            .keys()
            .copied()
            .filter(|node| !fetched.contains(node))
            .collect();
    }
    Ok(evaluated(certificates, rows_read))
}

/// Bugs the suite plants to prove its checks can fail (`suite/demand.rs`): a loader that never
/// composes membership, and one that explores a node whose issuer has no standing. Thread-local,
/// so a test that sets one cannot disturb another.
#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Planted {
    None,
    NoComposition,
    Ungrounded,
}

#[cfg(test)]
thread_local! {
    pub(crate) static PLANTED: std::cell::Cell<Planted> = const { std::cell::Cell::new(Planted::None) };
}

#[cfg(test)]
fn planted() -> Planted {
    PLANTED.with(std::cell::Cell::get)
}

/// Run the positive reach to its fixed point over the edges loaded, and make every node it names a
/// key of `reach`.
fn grow(
    edges: &BTreeMap<Identifier, Vec<(Identifier, Identifier)>>,
    reach: &mut BTreeMap<Identifier, BTreeSet<Identifier>>,
    subject: Identifier,
) {
    loop {
        let mut changed = false;
        let nodes: Vec<Identifier> = reach.keys().copied().collect();
        for node in nodes {
            #[cfg(test)]
            let composed = planted() != Planted::NoComposition;
            #[cfg(not(test))]
            let composed = true;
            let standing: Vec<Identifier> = std::iter::once(node)
                .chain(reach[&node].iter().copied().filter(|_| composed))
                .collect();
            for sub in standing {
                for &(issuer, audience) in edges.get(&sub).into_iter().flatten() {
                    #[cfg(test)]
                    let ungrounded = planted() == Planted::Ungrounded;
                    #[cfg(not(test))]
                    let ungrounded = false;
                    let grounded = ungrounded
                        || issuer == sub
                        || reach
                            .get(&sub)
                            .is_some_and(|standing| standing.contains(&issuer));
                    if grounded
                        && audience != node
                        && reach
                            .get_mut(&node)
                            .expect("a node iterated is a key")
                            .insert(audience)
                    {
                        changed = true;
                    }
                }
            }
        }
        // The closure is `subject` and what stands over it; those are the nodes whose rows count.
        let standing: Vec<Identifier> = reach[&subject].iter().copied().collect();
        for node in standing {
            if let std::collections::btree_map::Entry::Vacant(slot) = reach.entry(node) {
                slot.insert(BTreeSet::new());
                changed = true;
            }
        }
        if !changed {
            return;
        }
    }
}

/// The certificates with these identities, and the evaluator over exactly them: for a query about
/// a subset (`members_for_subset`), which is the whole set as far as the answer goes.
pub async fn load_ids<W: Encode + Decode>(
    snapshot: &impl QueryExecutor,
    ids: impl IntoIterator<Item = CertificateId>,
    memo: &Memo<W>,
) -> Result<Evaluated<W>, Error> {
    let ids: Vec<(i64, Vec<u8>)> = ids.into_iter().map(crate::rows::kind_and_digest).collect();
    let mut certificates = Vec::new();
    let mut rows_read = 0;
    for chunk in ids.chunks(CHUNK / 2) {
        let mut sql = Sql::new(
            "SELECT kind, digest, signed FROM keyline_certificates WHERE (kind, digest) IN ",
        );
        sql.bind_tuples(chunk.iter().cloned());
        for (kind, digest, signed) in fetch(snapshot, sql).await? {
            rows_read += 1;
            certificates.push(memo.verified(kind, &digest, &signed)?);
        }
    }
    Ok(evaluated(certificates, rows_read))
}

fn evaluated<W: Encode + Decode>(certificates: Vec<VerifiedCertificate<W>>, rows_read: usize) -> Evaluated<W> {
    let mut state = State::new();
    state.insert_all(certificates);
    Evaluated { state, rows_read }
}
