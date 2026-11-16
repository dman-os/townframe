use crate::interlude::*;

use wflow_wdk::StepKind;

pub mod authority;

/// Finite bounds on the retry loop. `None` means unbounded, preserving the
/// behaviour of jobs submitted before budgets existed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct RetryBudget {
    /// Maximum transient-error retries before the job settles as
    /// [`JobFailure::RetriesExhausted`]. Retry exhaustion settles directly; it
    /// does not enter a compensation phase or claim that external effects were
    /// undone.
    pub max_retries: Option<u64>,
    /// Maximum re-runs that start without new progress (crash or retry-loop
    /// restarts) before the job faults with [`JobFailure::RestartsExhausted`].
    /// A re-grant that re-attaches to a held wait is not a re-run of a crashed
    /// activation and is not charged here.
    pub max_restarts: Option<u64>,
}

/// How much of a host-reported panic message a durable fault keeps.
///
/// A receipt is retained for the life of its key while panic text is
/// host-supplied and unbounded, so the authority stores only the head of it.
/// The cap is part of the durable record: replay rejects a message longer than
/// this rather than accepting a settlement the authority would not have
/// written.
pub const MAX_PANIC_MESSAGE_BYTES: usize = 4096;

/// The classified failure that settles a job which cannot make progress.
///
/// Every variant is durable as [`authority::Outcome::Faulted`] on the job's
/// permanent receipt, so a receipt never carries a failure double-encoded
/// inside a completed result value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobFailure {
    /// The job was cancelled. Cancellation permanently fences the job from
    /// future progress; it does not claim that in-flight external effects were
    /// undone.
    Cancelled { reason: String },
    /// A replayed step diverged from its durable record.
    ///
    /// The durable side of the divergence is the journal's step log, so the
    /// fault records only what a replay cannot recover from it: the position
    /// that diverged and the boundary kind the replay offered there.
    ReplayDiverged { step: u64, replayed: StepKind },
    /// The workflow code panicked. `message` is the host's own text for the
    /// panic, truncated to [`MAX_PANIC_MESSAGE_BYTES`].
    Panicked(String),
    /// The transient-error retry budget ran out. The job settles directly
    /// without entering compensation, and in-flight external effects may
    /// remain applied.
    RetriesExhausted {
        retries: u64,
        max_retries: u64,
        last_error: String,
    },
    /// The restart budget ran out without the job making progress.
    RestartsExhausted { restarts: u64, max_restarts: u64 },
}
