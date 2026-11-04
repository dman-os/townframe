use crate::interlude::*;

pub mod authority;
pub mod effects;
pub mod job_events;
pub mod log;
pub mod reduce;
pub mod state;

/// Finite bounds on the retry loop. `None` means unbounded, preserving the
/// behaviour of jobs submitted before budgets existed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct RetryBudget {
    /// Maximum transient-error retries per phase: forward work exhausting it
    /// enters cleanup ([`CleanupCause::RetriesExhausted`]) with a fresh
    /// budget, and cleanup exhausting it settles the job as
    /// [`JobFailure::RetriesExhausted`].
    pub max_retries: Option<u64>,
    /// Maximum re-runs that start without new progress (crash or retry-loop
    /// restarts) before the job faults with [`JobFailure::RestartsExhausted`].
    pub max_restarts: Option<u64>,
}

/// Why a job left forward execution for its cleanup phase.
///
/// Durable in the authority's `CleanupStarted` transition, and what the
/// workflow's next forward boundary returns as its error, so a cold replay
/// re-enters cleanup at the same point with the same cause and the registered
/// compensation is rebuilt before it runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CleanupCause {
    /// An operator cancelled the job.
    Cancelled { reason: String },
    /// The workflow hit a terminal error while compensation was registered.
    Failed { error: String },
    /// Forward work used up the transient-retry budget. The cleanup phase
    /// gets a budget of its own; exhausting that one settles the job as
    /// [`JobFailure::RetriesExhausted`] without further compensation.
    RetriesExhausted {
        retries: u64,
        max_retries: u64,
        last_error: String,
    },
}

/// The classified failure that settles a job which cannot make progress.
///
/// Hosts serialise this into the existing terminal-failure slot of the job
/// result envelope, so the WIT result shape is unchanged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobFailure {
    /// The job was cancelled and its registered cleanup ran to completion.
    Cancelled { reason: String },
    /// A cleanup step failed terminally. The cleanup that remains is
    /// outstanding and stays visible in the journal; it is never archived as
    /// completed compensation.
    CleanupFailed {
        cause: CleanupCause,
        defer: String,
        error: String,
    },
    /// A replayed step diverged from its durable record.
    ReplayDiverged { step: u64, detail: String },
    /// The workflow code panicked.
    Panicked(String),
    /// The transient-error retry budget ran out: either the forward budget
    /// did and the registered cleanup then ran to completion, or the cleanup
    /// budget did and what remained is left outstanding (see
    /// [`CleanupCause::RetriesExhausted`]).
    RetriesExhausted {
        retries: u64,
        max_retries: u64,
        last_error: String,
    },
    /// The restart budget ran out without the job making progress.
    RestartsExhausted { restarts: u64, max_restarts: u64 },
    /// The host could not run the job (trap, io, cancel during shutdown).
    Host(String),
}
