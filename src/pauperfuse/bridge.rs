//! The bridge: making one backend's state true of another (ADR 010 §4.2–§4.4).
//!
//! A pass is deliberately one-directional. It makes `target` agree with `source`
//! for the paths they disagree about, and leaves alone whatever the source does
//! not know about. Running it in both directions is a mirror; running it once,
//! from a doc-backed source into a checkout, is a checkout update.
//!
//! The other way round — a checkout as the source — is an **ingest**, and it is
//! not symmetric. The bytes are written into the target the same way, but what
//! the target's `materialize` *means* is its own business: for a doc backend it
//! is where bytes become doc content, and an edit to a path the doc already
//! covers is the act it is for. What may be claimed by an ingest at all is a
//! deployment question, not one this module answers (ADR 011 §9).
//!
//! Nothing here decides what a *change* is: the store's two reps already say
//! that, and comparing them is a merge join. Nothing here decides what two
//! *different* identities mean either: that is the target's answer
//! ([`Backend::accept`]), because it depends on schemes the core cannot read
//! (ADR 010 §2.3). What is left for this module is ordering, bookkeeping, and
//! writing down what was written.
//!
//! Two properties are worth keeping in mind when reading [`reconcile`]:
//!
//! - **the source is never modified**, so a failed pass leaves it exactly as it
//!   was and the next one retries from the same state;
//! - **the target's rep is recorded from what was written**, not from what was
//!   planned, so a scan of the target right afterwards reports nothing.

use crate::backend::{Accepted, Backend};
use crate::delta::{Delta, DiffWalk};
use crate::entry::{Avail, Entry, Payload, Token};
use crate::interlude::*;
use crate::path::RelPath;
use crate::store::VtreeStore;

/// What one reconcile pass did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Outcome {
    /// Paths written into the target.
    pub transferred: usize,
    /// Paths removed from the target.
    pub removed: usize,
    /// Deltas that asked for nothing: equal content, a stub on one side, or a
    /// metadata-only move.
    pub unchanged: usize,
    /// Paths the target holds and the source does not know about, which this
    /// pass leaves exactly as they are.
    ///
    /// A path the source has *dropped* is different, and is counted in
    /// [`removed`](Self::removed): that is a recorded path the target holds
    /// whose entry says a doc owned it. Everything else here may be a user's
    /// own file, and a pass that deletes what it does not recognize is a pass
    /// nobody can point at a directory that matters.
    pub target_only: usize,
}

/// Make `target` agree with `source`, within what both backends know.
///
/// The store's two reps are the record: this reads the disagreement between
/// them, transfers what the target is missing, removes what the target holds and
/// the source does not, and then records the target's new state in its rep.
///
/// Both backends must already have been reported into their reps (`Report::run`
/// over each); a rep that is behind makes a pass transfer work that is already
/// done, which is wasteful but never wrong.
pub async fn reconcile(
    source: &dyn Backend,
    target: &dyn Backend,
    store: &Arc<dyn VtreeStore>,
) -> Result<Outcome> {
    // The walk is left-to-right: left is the target, right is the source, so a
    // delta describes what the *target* must do to become the source. `Added` is
    // "the source has it and the target does not", `Removed` is the opposite,
    // and `Changed` carries the target's entry as `from` — which is what makes
    // `needs_transfer`'s stub check mean "the target is a stub" (ADR 010 §2.4).
    let target_rep = target.id();
    let deltas = DiffWalk::new(Arc::clone(store), &target_rep, &source.id())
        .collect()
        .await?;

    let mut outcome = Outcome::default();
    let mut recorded = Vec::new();
    let mut removals = Vec::new();

    for delta in &deltas {
        match delta {
            Delta::Added { path, entry }
            | Delta::Changed {
                path, to: entry, ..
            } => {
                // What the target recorded for this path, when it had anything:
                // for `Changed` it is the other side of the disagreement.
                let was_recorded = match delta {
                    Delta::Changed { from, .. } => Some(from),
                    _ => None,
                };
                if entry.avail == Avail::Stub {
                    // Availability, not bytes: the target records the promise and
                    // nothing moves. The bytes travel when whoever holds them says
                    // so, which is a later pass over a record that has stopped
                    // being a stub.
                    recorded.push(Delta::Added {
                        path: path.clone(),
                        entry: entry.clone(),
                    });
                    outcome.unchanged += 1;
                    continue;
                }

                // The one question the core cannot answer, put to the side that
                // holds the path and knows both identities.
                match target.accept(path, was_recorded, entry).await? {
                    Accepted::Current => {
                        // The records disagreed and the target holds these bytes
                        // anyway — under another name, or with a record of its
                        // own. Writing the source's identity into the target's
                        // record is what settles it for good; leaving it would ask
                        // the same question every pass. The stat stays the
                        // target's, since it is the answer to "may I skip work
                        // next time".
                        let kept = was_recorded
                            .and_then(|recorded| recorded.stat)
                            .or(entry.stat);
                        recorded.push(Delta::Added {
                            path: path.clone(),
                            entry: entry.clone().with_stat(kept),
                        });
                        outcome.unchanged += 1;
                    }
                    wanted => {
                        let written = transfer(source, target, path, entry, wanted).await?;
                        recorded.push(Delta::Added {
                            path: path.clone(),
                            entry: written,
                        });
                        outcome.transferred += 1;
                    }
                }
            }
            Delta::Removed { path, entry } => {
                // The path is recorded for the target and not for the source.
                // Whether that means "let it go" is a question about the past,
                // and only the target's own record can answer it: a claim says
                // some hand put the path here and may take it away, and no claim
                // means the target's own file, which this pass has no business
                // deleting (ADR 010 §8.6, §8.7).
                if target.may_remove(path, entry).await? {
                    removals.push(path.clone());
                    recorded.push(delta.clone());
                } else {
                    outcome.target_only += 1;
                }
            }
            Delta::Touched { .. } => outcome.unchanged += 1,
        }
    }

    // Removals land deepest first. A directory always sorts before its children,
    // so reversing the order is what guarantees the directory is empty by the
    // time it is removed — which is what lets `remove` refuse to recurse, and so
    // refuse to delete anything this pass did not know was there.
    removals.sort_by(|mine, theirs| theirs.cmp(mine));
    for path in removals {
        target.remove(&path).await?;
        outcome.removed += 1;
    }

    // Recording what was written, not what was planned, is what keeps the
    // target's next scan quiet (§2.3): the recorded fingerprint is the one the
    // target's own filesystem reported after the write.
    store.apply(&target_rep, &recorded).await?;
    Ok(outcome)
}

/// Put `entry`'s content into `target`, the way `target` asked for it, and by
/// reference where both sides say that is safe.
async fn transfer(
    source: &dyn Backend,
    target: &dyn Backend,
    path: &RelPath,
    entry: &Entry,
    wanted: Accepted,
) -> Result<Entry> {
    match &entry.payload {
        // A directory and a symlink are made from the entry: there are no bytes
        // to move and nothing to look up.
        Payload::Dir | Payload::Symlink { .. } => {
            let stat = target.materialize(path, entry, &[]).await?;
            Ok(entry.clone().with_stat(stat).present())
        }
        Payload::File { .. } => {
            // The target asked for a reference. It gets one only when the source
            // also says its bytes do not change in place and can name a path for
            // them — linking aliases two paths onto one inode, so an edit through
            // either would land in both, and a checkout's files are its user's
            // (ADR 010 §2.5). Both sides have to agree, and each says so for
            // itself.
            if wanted == Accepted::ByReference
                && source.capabilities().immutable_content
                && let Some(local) = source.locate(path)
            {
                let stat = target.link_from(path, &local).await?;
                return Ok(entry.clone().with_stat(stat).present());
            }
            let bytes = source.read(path, None).await?;
            let stat = target.materialize(path, entry, &bytes).await?;
            // The digest of the bytes that just moved is evidence the target did
            // not have. Recording it is what lets a later question be settled on
            // content rather than on a name the target cannot check (ADR 010
            // §2.3, "identity continuity").
            Ok(entry
                .clone()
                .with_stat(stat)
                .present()
                .with_content_evidence(Token::blake3_of(&bytes)))
        }
    }
}
