use crate::interlude::*;

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
    pub max_restarts: Option<u64>,
}

/// The classified failure that settles a job which cannot make progress.
///
/// Hosts serialise this into the existing terminal-failure slot of the job
/// result envelope, so the WIT result shape is unchanged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobFailure {
    /// The job was cancelled. Cancellation permanently fences the job from
    /// future progress; it does not claim that in-flight external effects were
    /// undone.
    Cancelled { reason: String },
    /// A replayed step diverged from its durable record.
    ReplayDiverged { step: u64, detail: String },
    /// The workflow code panicked.
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
