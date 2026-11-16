//! Why the actor stopped, and how a failed durable write maps onto it.
//!
//! These are the only two outcomes a commit can have besides success: the
//! authority refused it, or the store failed. Keeping the classification apart
//! from the actor makes it obvious that every rejection is decided by the
//! authority and every `Halt` is decided by the journal.

use super::authority;
use crate::interlude::*;
use wflow_core::kvstore::atomic::CommitError;

pub(super) enum CommitFailure {
    /// The authority refused it; no storage was touched and none may be.
    Rejected(authority::Rejection),
    /// Writing it failed in a way only a restart can resolve.
    Halted(Halt),
}

pub(super) enum Halt {
    /// The writer reported the transition was definitively not committed and
    /// kept doing so until the retry budget ran out. Nothing was written, so a
    /// restart replays exactly the committed prefix.
    Unavailable(eyre::Report),
    /// The writer could not say whether the transition committed: it may be
    /// durable. The next start replays it if it is.
    Ambiguous(eyre::Report),
    /// The store's revision moved past the one the journal expected, which the
    /// journal's single-writer contract forbids, so the journal cannot advance
    /// until it is reopened.
    ForeignWriter { actual_revision: u64 },
}

impl Halt {
    /// Maps a failed durable write onto the reason the actor stops.
    pub(super) fn from_commit_error(error: CommitError) -> Self {
        match error {
            CommitError::NotCommitted(error) => Self::Unavailable(error),
            CommitError::Unknown(error) => Self::Ambiguous(error),
            CommitError::Conflict { actual_revision } => Self::ForeignWriter { actual_revision },
        }
    }
}
