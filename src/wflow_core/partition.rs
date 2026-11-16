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
/// A receipt is retained for its configured window, and every free-text field of
/// a fault is host-supplied and unbounded, so the authority stores only the head
/// of each. The cap is part of the durable record: replay rejects a message
/// longer than this rather than accepting a settlement the authority would not
/// have written.
pub const MAX_FAULT_TEXT_BYTES: usize = 4096;

/// Truncates host-supplied fault text to [`MAX_FAULT_TEXT_BYTES`].
///
/// This is the single place the durable text limit is applied, because every
/// path that can put host text into a fault must obey it: a fault reaches the
/// receipt, the result index and every later snapshot, so an unbounded string
/// would inflate partition state for the receipt's whole retention window.
/// Truncation lands on a UTF-8 boundary so the stored string stays valid JSON.
pub fn bounded_fault_text(mut text: String) -> String {
    if text.len() <= MAX_FAULT_TEXT_BYTES {
        return text;
    }
    let mut end = MAX_FAULT_TEXT_BYTES;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    text
}

/// The classified failure that settles a job which cannot make progress.
///
/// Every variant is durable as [`authority::Outcome::Faulted`] on the job's
/// permanent receipt, so a receipt never carries a failure double-encoded
/// inside a completed result value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobFailure {
    /// The job was cancelled. `reason` is caller-supplied text, truncated to
    /// [`MAX_FAULT_TEXT_BYTES`]. Cancellation permanently fences the job from
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
    /// panic, truncated to [`MAX_FAULT_TEXT_BYTES`].
    Panicked(String),
    /// The host refused to instantiate this workflow for this activation.
    ///
    /// This is the deterministic half of a spawn failure: the workflow key is
    /// unknown to the build, or the host rejected the component, its manifest,
    /// or its configuration. It is a different class from
    /// [`JobFailure::Panicked`] because nothing crashed, and from
    /// [`JobFailure::RestartsExhausted`] because no restart could ever help: a
    /// re-grant would fail identically. `message` is the host's own diagnosis,
    /// truncated to [`MAX_FAULT_TEXT_BYTES`].
    ActivationFailed { message: String },
    /// The workflow asked for a timer deadline the durable model cannot record.
    ///
    /// A durable wait stores `fire_at_ms` as an unsigned millisecond offset from
    /// the unix epoch, and the session handed the authority an instant before
    /// that epoch. `fire_at_ms` records exactly what the session offered: the
    /// requested sleep duration is not part of the session protocol, so
    /// inventing one would be a claim the authority cannot back. It settles
    /// instead of retrying because no re-run makes the request representable: a
    /// sleep long enough to leave the representable range overflows on every
    /// replay, and an instant that is merely before the epoch is a host defect
    /// that a restart would only hide.
    TimerOutOfRange { fire_at_ms: i64 },
    /// The transient-error retry budget ran out. The job settles directly
    /// without entering compensation, and in-flight external effects may
    /// remain applied. `last_error` is the workflow's own report for the last
    /// transient failure, truncated to [`MAX_FAULT_TEXT_BYTES`].
    RetriesExhausted {
        retries: u64,
        max_retries: u64,
        last_error: String,
    },
    /// The restart budget ran out without the job making progress.
    RestartsExhausted { restarts: u64, max_restarts: u64 },
}
