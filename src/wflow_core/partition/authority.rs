use crate::interlude::*;

use super::JobFailure;
use super::MAX_FAULT_TEXT_BYTES;
use super::RetryBudget;
use super::bounded_fault_text;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;
use wflow_wdk::StepId;
use wflow_wdk::StepKind;
use wflow_wdk::types::RetryPolicy;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    pub partition_incarnation: u64,
    pub authority_epoch: u64,
    pub job_incarnation: u64,
    pub generation: u64,
    pub worker: u64,
    pub position: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Step {
    pub id: StepId,
    pub kind: StepKind,
    /// The boundary name the workflow chose for this step, when it named one.
    /// Replay must reproduce it, so a re-run that renames a step diverges.
    /// Absent in serialized forms that predate named steps (the conformance
    /// model), where it round-trips as an unnamed step.
    #[serde(default)]
    pub name: Option<String>,
    pub attempt: u64,
    pub value: String,
    pub lease: Lease,
    pub position: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Terminal {
    pub value: String,
    pub lease: Lease,
    pub position: u64,
}

/// What a durable wait is suspended on. The authority never reads a clock:
/// `fire_at_ms` is the deadline the worker computed when it began the wait,
/// and the host fires the timer once that wall-clock instant has passed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WaitKind {
    Timer { fire_at_ms: u64 },
    Message,
}

impl WaitKind {
    pub fn step_kind(self) -> StepKind {
        match self {
            Self::Timer { .. } => StepKind::Sleep,
            Self::Message => StepKind::Recv,
        }
    }
}

/// A step the job is durably suspended on. It survives revocation and
/// recovery, so a re-granted invocation that reaches the same step re-attaches
/// to it instead of starting a new wait with a new deadline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Wait {
    pub step: u64,
    pub kind: WaitKind,
    /// The boundary name the wait was begun under; the resolved step inherits
    /// it so a fire/delivery needs no name of its own.
    pub name: Option<String>,
    /// The lease that began the wait; the resolved step records it.
    pub lease: Lease,
    pub position: u64,
}

/// A durable retry the job is parked on after a transient failure. The
/// authority never reads a clock: `fire_at_ms` was computed from the submitter
/// policy when the retry was scheduled, and the host grants the job once that
/// wall-clock instant has passed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryWait {
    pub fire_at_ms: u64,
    /// The transient error that scheduled the retry.
    pub error: String,
    pub position: u64,
}

/// The classified failure that settled a job which cannot make progress.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fault {
    pub failure: JobFailure,
    pub position: u64,
}

/// The terminal settlement recorded in a [`Receipt`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Outcome {
    Completed(Terminal),
    Faulted(Fault),
}

impl Outcome {
    pub fn position(&self) -> u64 {
        match self {
            Self::Completed(terminal) => terminal.position,
            Self::Faulted(fault) => fault.position,
        }
    }
}

/// Compact terminal receipt retained until its retention deadline.
///
/// Settling drops the job's replay step log, inbox, message deduplication map,
/// waits, retry state, and submission payload from live authority state and
/// snapshots while preserving idempotent submission identity and result lookup.
/// The receipt then lives until [`Receipt::retained_until_ms`], at which point
/// [`Transition::Pruned`] erases it and frees the key for a new incarnation.
///
/// This is the only retention horizon. Settlement already drops the replay log,
/// waits, inbox and retry state, so there is nothing else to expire per job: the
/// journal entries that recorded them are reclaimed by the existing
/// snapshot-watermark paging. A separate journal deadline would therefore only
/// be able to delete records the receipt no longer refers to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Receipt {
    /// Fingerprint of the submission arguments; see [`PayloadFingerprint`].
    pub payload: PayloadFingerprint,
    pub incarnation: u64,
    pub submission_position: u64,
    pub policy: RetryPolicy,
    pub budget: RetryBudget,
    pub steps: u64,
    pub outcome: Outcome,
    /// The wall-clock instant after which the receipt is pruned. The settling
    /// command stamps it from the job's frozen retention, so it is absolute:
    /// replay re-checks the deadline but never re-resolves it against
    /// configuration.
    pub retained_until_ms: u64,
}

/// The stable content fingerprint of a submission's arguments.
///
/// A receipt answers "was this key submitted with this exact payload?" for as
/// long as the receipt lives, so it records a fingerprint instead of the payload
/// itself. `policy` and `budget` stay compared as values, so the fingerprint
/// replaces only the argument string in that comparison.
///
/// Two FNV-1a lanes with distinct offset bases, each finished by splitmix64's
/// finaliser. FNV-1a is byte-at-a-time over the stored string, so no
/// serialisation can drift underneath it, and it is stable; the finaliser
/// spreads a one-byte difference across each 64-bit half instead of leaving it
/// in the low bits. Both halves must agree for two payloads to compare equal,
/// so an accidental collision needs a 128-bit match. This is an idempotency
/// comparison between payloads the same application submits under one key, not
/// a security boundary. This crate has no cryptographic hash dependency, and
/// the construction is part of the durable receipt format: it must never
/// change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadFingerprint([u64; 2]);

impl PayloadFingerprint {
    fn of(args: &str) -> Self {
        let bytes = args.as_bytes();
        Self([
            finish(fnv1a(FNV_OFFSET_A, bytes), 0x9e37_79b9_7f4a_7c15),
            finish(fnv1a(FNV_OFFSET_B, bytes), 0xc2b2_ae3d_27d4_eb4f),
        ])
    }
}

/// FNV-1a over the payload bytes at 128-bit width. The multiplication wraps,
/// which is the algorithm's own modulus.
fn fnv1a(mut state: u128, bytes: &[u8]) -> u128 {
    for byte in bytes {
        state = (state ^ u128::from(*byte)).wrapping_mul(FNV_PRIME);
    }
    state
}

/// splitmix64's finaliser over one lane, with the lane's high half folded in so
/// both halves of the fingerprint depend on the whole payload.
fn finish(state: u128, seed: u64) -> u64 {
    let mut mixed = (state as u64) ^ ((state >> 64) as u64) ^ seed;
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    mixed ^ (mixed >> 31)
}

/// The canonical FNV-1a 128-bit offset basis, a second independent lane seed,
/// and the algorithm's prime.
const FNV_OFFSET_A: u128 = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d;
const FNV_OFFSET_B: u128 = 0xcbf2_9ce4_8422_2325_cbf2_9ce4_8422_2325;
const FNV_PRIME: u128 = 0x0000_0000_0100_0000_0000_0000_0000_013b;

/// Truncates a fault's free text to the durable maximum.
///
/// Applied to every variant that carries host or caller text, at the one place
/// a fault is proposed, so no command path can commit an unbounded string. A
/// fault's text is kept for the life of the receipt.
fn bounded_failure(failure: JobFailure) -> JobFailure {
    match failure {
        JobFailure::Panicked(message) => JobFailure::Panicked(bounded_fault_text(message)),
        JobFailure::Cancelled { reason } => JobFailure::Cancelled {
            reason: bounded_fault_text(reason),
        },
        JobFailure::RetriesExhausted {
            retries,
            max_retries,
            last_error,
        } => JobFailure::RetriesExhausted {
            retries,
            max_retries,
            last_error: bounded_fault_text(last_error),
        },
        failure @ (JobFailure::ReplayDiverged { .. } | JobFailure::RestartsExhausted { .. }) => {
            failure
        }
    }
}

/// How far a job had got when its current run was granted. A re-run that
/// starts from the same progress is a restart; any committed step or resolved
/// wait since then resets the count. A re-run that re-attaches to a held wait
/// or a due retry re-enters durable state instead of restarting a crashed
/// activation, so it is not charged as one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Progress {
    pub steps: u64,
    pub waiting: Option<u64>,
}

/// An undelivered message in a job's FIFO inbox.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    pub id: String,
    pub payload: String,
    pub position: u64,
}

/// Durable receipt of an accepted message, keyed by message `id` in
/// [`Job::messages`] so a resent `id` deduplicates in `O(log n)` even after a
/// message wait has popped the message from [`Job::inbox`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageReceipt {
    pub payload: String,
    pub position: u64,
}

/// How a `WaitResolved` transition produced its step value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Resolution {
    /// The job's timer wait fired.
    Timer,
    /// A message wait began while the inbox held an undelivered message; the
    /// oldest one is consumed without the job ever durably waiting.
    Queued,
    /// The message arrived while the job was durably waiting for one; it is
    /// accepted and consumed in the same transition.
    Delivered(Message),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Job {
    pub args: String,
    pub incarnation: u64,
    pub submission_position: u64,
    pub steps: Vec<Step>,
    pub last_generation: u64,
    pub lease: Option<Lease>,
    pub wait: Option<Wait>,
    pub inbox: VecDeque<Message>,
    pub messages: BTreeMap<String, MessageReceipt>,
    /// How transient failures of this job are rescheduled.
    pub policy: RetryPolicy,
    /// Finite bounds on the retry/restart loops; unbounded by default.
    pub budget: RetryBudget,
    /// How long the receipt this job settles into is retained, measured from
    /// settlement. Frozen at submission from the runtime's resolved value so a
    /// later configuration edit cannot move the deadline.
    pub retention_ms: u64,
    /// The retry the job is parked on, if a transient failure scheduled one.
    pub retry: Option<RetryWait>,
    /// Transient retries since the last committed step or resolved wait.
    pub retry_attempts: u64,
    /// Re-runs that started without new progress since the previous run.
    pub restarts: u64,
    /// The progress recorded when the current/last run was granted.
    pub progress: Option<Progress>,
}

impl Job {
    /// How far the job is right now.
    pub fn progress(&self) -> Progress {
        Progress {
            steps: self.steps.len() as u64,
            waiting: self.wait.as_ref().map(|wait| wait.step),
        }
    }

    pub fn disposition(&self) -> Disposition {
        if self.retry.is_none() && self.lease.is_some() && self.wait.is_none() {
            Disposition::Continue
        } else {
            Disposition::Park
        }
    }

    /// The receipt this job settles into. The authority never reads a clock, so
    /// the caller supplies `retained_until_ms`: the job's frozen retention
    /// added to the instant of settlement. The only trace of the task's
    /// arguments is the fingerprint, so settling does not retain the payload
    /// itself.
    pub fn receipt(&self, outcome: Outcome, retained_until_ms: u64) -> Receipt {
        Receipt {
            payload: PayloadFingerprint::of(&self.args),
            incarnation: self.incarnation,
            submission_position: self.submission_position,
            policy: self.policy,
            budget: self.budget,
            steps: self.steps.len() as u64,
            outcome,
            retained_until_ms,
        }
    }

    /// The restart count a grant of `self` would record given its current
    /// `progress`. A due retry re-run keeps the count, and so does a re-grant
    /// that re-attaches to a held wait: neither begins a fresh activation of a
    /// crashed run. A re-run from unchanged progress (crash or lost lease)
    /// costs one; any progress resets it. Shared by `prepare_grant` and the
    /// durable `Granted` application so both agree.
    fn restarts_after(&self, progress: Progress) -> u64 {
        if self.retry.is_some() || self.wait.is_some() {
            self.restarts
        } else if self.progress == Some(progress) {
            self.restarts.checked_add(1).unwrap()
        } else {
            0
        }
    }

    /// Validates a durable grant against the job and leases it, charging the
    /// restart budget exactly as `prepare_grant` did when it proposed the lease.
    fn apply_grant(&mut self, lease: Lease, position: u64) -> Res<()> {
        if lease.position != position {
            return Err(ferr!("invalid durable grant"));
        }
        let expected = self.last_generation.checked_add(1).unwrap();
        if lease.generation != expected {
            return Err(ferr!("non-monotonic durable grant"));
        }
        let progress = self.progress();
        self.restarts = self.restarts_after(progress);
        if let Some(max_restarts) = self.budget.max_restarts
            && self.restarts > max_restarts
        {
            return Err(ferr!("durable grant exceeds the restart budget"));
        }
        self.progress = Some(progress);
        self.retry = None;
        self.last_generation = lease.generation;
        self.lease = Some(lease);
        Ok(())
    }

    /// Validates a durable retry against the job: only the current lease holder
    /// can park an unsettled, non-waiting job, and it releases the lease doing so.
    fn apply_retry(&mut self, lease: &Lease, retry: RetryWait, position: u64) -> Res<()> {
        if self.lease.as_ref() != Some(lease) || self.wait.is_some() || retry.position != position {
            return Err(ferr!("invalid durable retry"));
        }
        self.lease = None;
        self.retry = Some(retry);
        self.retry_attempts = self.retry_attempts.checked_add(1).unwrap();
        Ok(())
    }

    /// Validates a durable fault against the job and the actor that reported
    /// it.
    ///
    /// The authority settles cancellation and restart exhaustion from durable
    /// state alone, so their counters are recomputed here and nothing else is
    /// accepted. The failures a lease holder reports are external facts the log
    /// cannot derive (a panic, a divergent replay, the error behind an
    /// exhausted retry budget), so they are accepted only from the lease the
    /// job currently holds: a superseded activation must not settle a live job.
    fn validate_fault(&self, fault: &Fault, lease: Option<&Lease>, position: u64) -> Res<()> {
        if fault.position != position {
            return Err(ferr!("invalid durable fault"));
        }
        let holds_lease = lease.is_some_and(|lease| self.lease.as_ref() == Some(lease));
        match &fault.failure {
            JobFailure::Cancelled { .. } if lease.is_none() => Ok(()),
            JobFailure::RestartsExhausted {
                restarts,
                max_restarts,
            } if lease.is_none()
                && self.budget.max_restarts == Some(*max_restarts)
                && *restarts == self.restarts_after(self.progress())
                && restarts > max_restarts =>
            {
                Ok(())
            }
            JobFailure::RetriesExhausted {
                retries,
                max_retries,
                ..
            } if holds_lease
                && self.budget.max_retries == Some(*max_retries)
                && *retries == self.retry_attempts
                && retries >= max_retries =>
            {
                Ok(())
            }
            JobFailure::ReplayDiverged { .. } if holds_lease => Ok(()),
            JobFailure::Panicked(message)
                if holds_lease && message.len() <= MAX_FAULT_TEXT_BYTES =>
            {
                Ok(())
            }
            _ => Err(ferr!("invalid durable fault")),
        }
    }

    /// Validates a durably accepted message against the job and queues it: a
    /// message that would have resolved a held wait is a resolution, not an
    /// acceptance.
    fn apply_message(&mut self, message: Message, position: u64) -> Res<()> {
        let waits_for_message = self
            .wait
            .as_ref()
            .is_some_and(|wait| wait.kind == WaitKind::Message);
        if waits_for_message
            || message.position != position
            || self.messages.contains_key(&message.id)
        {
            return Err(ferr!("invalid durable message"));
        }
        self.messages.insert(
            message.id.clone(),
            MessageReceipt {
                payload: message.payload.clone(),
                position: message.position,
            },
        );
        self.inbox.push_back(message);
        Ok(())
    }

    /// Validates a durable wait resolution against the job, then commits the
    /// resolved step, consuming the inbox message it delivers.
    fn apply_resolution(&mut self, step: Step, resolution: Resolution, position: u64) -> Res<()> {
        let held = self
            .wait
            .as_ref()
            .filter(|wait| wait.step == step.id && wait.lease == step.lease)
            .map(|wait| wait.kind);
        let valid = match &resolution {
            Resolution::Timer => {
                step.kind == StepKind::Sleep && matches!(held, Some(WaitKind::Timer { .. }))
            }
            Resolution::Queued => {
                step.kind == StepKind::Recv
                    && self.wait.is_none()
                    && self.lease.as_ref() == Some(&step.lease)
                    && self
                        .inbox
                        .front()
                        .is_some_and(|queued| queued.payload == step.value)
            }
            Resolution::Delivered(message) => {
                step.kind == StepKind::Recv
                    && held == Some(WaitKind::Message)
                    && self.inbox.is_empty()
                    && message.position == position
                    && message.payload == step.value
                    && !self.messages.contains_key(&message.id)
            }
        };
        if !valid || step.id != self.steps.len() as u64 || step.position != position {
            return Err(ferr!("invalid durable wait resolution"));
        }
        match resolution {
            Resolution::Timer => {}
            Resolution::Queued => {
                self.inbox.pop_front();
            }
            Resolution::Delivered(message) => {
                self.messages.insert(
                    message.id,
                    MessageReceipt {
                        payload: message.payload,
                        position: message.position,
                    },
                );
            }
        }
        self.wait = None;
        self.retry_attempts = 0;
        self.steps.push(step);
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    Submit {
        key: String,
        args: String,
        policy: RetryPolicy,
        budget: RetryBudget,
        /// How long the receipt this submission settles into is retained,
        /// measured from settlement. The runtime has already resolved it from
        /// its configuration (`min(override, max)`); the authority freezes it
        /// on the job so a later config edit cannot move an existing deadline.
        /// The absolute deadline is stamped by the settling command, because
        /// the window counts from settlement and the authority owns no clock.
        retention_ms: u64,
    },
    Grant {
        key: String,
        incarnation: u64,
        worker: u64,
        /// The host's wall clock; only compared against durable `fire_at_ms`
        /// deadlines, never read by the authority itself.
        now_ms: u64,
    },
    CommitStep {
        key: String,
        lease: Lease,
        step: u64,
        attempt: u64,
        value: String,
        kind: StepKind,
        name: Option<String>,
    },
    Complete {
        key: String,
        lease: Lease,
        expected_steps: u64,
        value: String,
        /// The host's wall clock at settlement, stamped into the receipt's
        /// retention deadline. The authority never reads a clock itself.
        now_ms: u64,
    },
    /// The leased invocation cannot continue. `failure` is the classified,
    /// durable reason the job settles with; the authority owns whether that
    /// reason is admissible for the job's durable state.
    Fail {
        key: String,
        lease: Lease,
        failure: JobFailure,
        /// The host's wall clock at settlement; see [`Command::Complete`].
        now_ms: u64,
    },
    Revoke {
        key: String,
        lease: Lease,
    },
    /// The leased invocation reached a sleep or receive at `step`.
    BeginWait {
        key: String,
        lease: Lease,
        step: u64,
        kind: WaitKind,
        name: Option<String>,
    },
    /// The host observed that the timer wait at `step` is due.
    FireTimer {
        key: String,
        incarnation: u64,
        step: u64,
        value: String,
    },
    SendMessage {
        key: String,
        id: String,
        payload: String,
    },
    /// The leased invocation failed transiently with `error`; park the job on a
    /// retry computed from its policy. `jitter_ms` is host-supplied spread so
    /// a fleet of failures does not re-run in lockstep. `policy` is a one-shot
    /// override for this failure only; the durable submission policy is kept.
    Retry {
        key: String,
        lease: Lease,
        error: String,
        policy: Option<RetryPolicy>,
        now_ms: u64,
        jitter_ms: u64,
    },
    /// Atomically fences active work and settles a cancellation receipt.
    Cancel {
        key: String,
        incarnation: u64,
        reason: String,
        /// The host's wall clock at settlement; see [`Command::Complete`].
        now_ms: u64,
    },
    /// Retires a settled key once its retention window has elapsed.
    ///
    /// The authority reads no clock: `at_ms` is the host's instant and the
    /// comparison against the receipt's durable deadline re-runs identically on
    /// replay.
    Prune {
        key: String,
        at_ms: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Transition {
    Submitted {
        key: String,
        args: String,
        incarnation: u64,
        policy: RetryPolicy,
        budget: RetryBudget,
        /// The duration the eventual receipt keeps; see [`Command::Submit`].
        retention_ms: u64,
    },
    Granted {
        key: String,
        lease: Lease,
    },
    StepCommitted {
        key: String,
        step: Step,
    },
    Completed {
        key: String,
        terminal: Terminal,
        /// The wall-clock instant the receipt stops being retained. Stamped by
        /// the authority when admission runs, from the settling command's clock
        /// and the job's frozen retention, and copied verbatim on replay.
        retained_until_ms: u64,
    },
    Revoked {
        key: String,
        lease: Lease,
    },
    WaitStarted {
        key: String,
        wait: Wait,
    },
    WaitResolved {
        key: String,
        step: Step,
        resolution: Resolution,
    },
    MessageAccepted {
        key: String,
        message: Message,
    },
    /// The lease was released and the job parked on a durable retry.
    RetryScheduled {
        key: String,
        lease: Lease,
        retry: RetryWait,
    },
    /// The authority settled the job with a classified failure.
    Faulted {
        key: String,
        incarnation: u64,
        /// The activation that reported a failure the authority cannot
        /// recompute. `None` for the two faults the authority settles from
        /// durable state alone: cancellation (an operator fact) and restart
        /// exhaustion (charged at grant time, when no lease is held).
        lease: Option<Lease>,
        fault: Fault,
        /// The receipt's retention deadline; see [`Transition::Completed`].
        retained_until_ms: u64,
    },
    /// The receipt's retention window elapsed at `at_ms`; the receipt is
    /// deleted and the key is free to be submitted as a new incarnation.
    ///
    /// Replay re-validates the same fact admission checked: the receipt must
    /// still exist and `at_ms` must be at or past its durable deadline.
    Pruned {
        key: String,
        incarnation: u64,
        at_ms: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Intent {
    Execute {
        key: String,
        lease: Lease,
        replay: Vec<Step>,
    },
    Continue {
        key: String,
        step: Step,
    },
    Revoke {
        key: String,
        lease: Lease,
    },
    /// The job is durably suspended: hold its invocation parked and, for a
    /// timer, arm the deadline.
    Arm {
        key: String,
        wait: Wait,
    },
    Terminal {
        key: String,
        terminal: Terminal,
    },
    /// The authority settled the job; the host must notify awaiters and drop
    /// any state it holds for it.
    Fault {
        key: String,
        fault: Fault,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Disposition {
    Admitted,
    Continue,
    Park,
    Completed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ack {
    pub position: u64,
    pub disposition: Disposition,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Rejection {
    UnknownJob,
    IncarnationConflict,
    PayloadConflict,
    AlreadyTerminal,
    LeaseActive,
    StaleLease,
    OutOfOrder,
    InvalidAttempt,
    /// The job is not suspended on the named wait.
    NotWaiting,
    /// Replay reached a committed step with a different boundary kind or name.
    Nondeterministic,
    /// The job is parked on a retry that is not due yet; the host must re-arm
    /// its deadline instead of granting.
    RetryPending {
        fire_at_ms: u64,
    },
    /// The transient-error retry budget ran out; the lease remains held so the
    /// runtime can settle the job with [`Command::Fail`].
    RetriesExhausted {
        retries: u64,
        max_retries: u64,
    },
    /// The reported failure disagrees with durable job state: its counters, its
    /// budget, or the activation that reported it are not the ones this job
    /// reached.
    InvalidFault,
    /// A prune arrived before the receipt's retention deadline. The receipt,
    /// the key and the result-index row all stay exactly as they were.
    NotExpired,
    /// A prune named a key whose job is still active, so there is no receipt to
    /// retire.
    NotSettled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    Commit(Box<Proposal>),
    Duplicate(Ack),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proposal {
    pub expected_revision: u64,
    pub transition: Transition,
    pub intents: Vec<Intent>,
    ack: Ack,
}

/// The shape version of the serialized [`Snapshot`].
///
/// The journal versions its own format, but it treats the published snapshot
/// blob as opaque JSON, so the authority owns this one: a snapshot written
/// before the field existed deserializes `format` as 0 and is rejected at
/// recovery instead of being misread field by field. Version 2 added the
/// receipt's absolute retention deadline, so a version-1 blob carrying the old
/// receipt shape is rejected loudly rather than misread.
pub const SNAPSHOT_FORMAT: u32 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    /// See [`SNAPSHOT_FORMAT`].
    #[serde(default)]
    pub format: u32,
    pub partition_incarnation: u64,
    pub cut: u64,
    pub next_job_incarnation: u64,
    pub jobs: BTreeMap<String, Job>,
    #[serde(default)]
    pub receipts: BTreeMap<String, Receipt>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Authority {
    partition_incarnation: u64,
    authority_epoch: u64,
    revision: u64,
    next_job_incarnation: u64,
    jobs: BTreeMap<String, Job>,
    receipts: BTreeMap<String, Receipt>,
    #[cfg(test)]
    suffix: Vec<Transition>,
}

impl Authority {
    pub fn new(partition_incarnation: u64, authority_epoch: u64) -> Self {
        Self {
            partition_incarnation,
            authority_epoch,
            revision: 0,
            next_job_incarnation: 0,
            jobs: BTreeMap::new(),
            receipts: BTreeMap::new(),
            #[cfg(test)]
            suffix: Vec::new(),
        }
    }

    pub fn recover(
        partition_incarnation: u64,
        authority_epoch: u64,
        snapshot: Option<Snapshot>,
        suffix: &[Transition],
    ) -> Res<Self> {
        let mut authority = if let Some(snapshot) = snapshot {
            Self::validate_snapshot(partition_incarnation, &snapshot)?;
            Self {
                partition_incarnation,
                authority_epoch,
                revision: snapshot.cut,
                next_job_incarnation: snapshot.next_job_incarnation,
                jobs: snapshot.jobs,
                receipts: snapshot.receipts,
                #[cfg(test)]
                suffix: Vec::new(),
            }
        } else {
            Self::new(partition_incarnation, authority_epoch)
        };
        for transition in suffix {
            authority.apply(transition.clone())?;
        }
        for job in authority.jobs.values_mut() {
            job.lease = None;
        }
        Ok(authority)
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            format: SNAPSHOT_FORMAT,
            partition_incarnation: self.partition_incarnation,
            cut: self.revision,
            next_job_incarnation: self.next_job_incarnation,
            jobs: self.jobs.clone(),
            receipts: self.receipts.clone(),
        }
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn partition_incarnation(&self) -> u64 {
        self.partition_incarnation
    }

    pub fn authority_epoch(&self) -> u64 {
        self.authority_epoch
    }

    pub fn next_job_incarnation(&self) -> u64 {
        self.next_job_incarnation
    }

    pub fn jobs(&self) -> &BTreeMap<String, Job> {
        &self.jobs
    }

    pub fn receipts(&self) -> &BTreeMap<String, Receipt> {
        &self.receipts
    }

    #[cfg(test)]
    pub fn suffix(&self) -> &[Transition] {
        &self.suffix
    }

    pub fn prepare(&self, command: Command) -> Result<Decision, Rejection> {
        match command {
            Command::Submit {
                key,
                args,
                policy,
                budget,
                retention_ms,
            } => self.prepare_submit(key, args, policy, budget, retention_ms),
            Command::Grant {
                key,
                incarnation,
                worker,
                now_ms,
            } => self.prepare_grant(key, incarnation, worker, now_ms),
            Command::CommitStep {
                key,
                lease,
                step,
                attempt,
                value,
                kind,
                name,
            } => self.prepare_step(key, lease, step, attempt, value, kind, name),
            Command::Complete {
                key,
                lease,
                expected_steps,
                value,
                now_ms,
            } => self.prepare_complete(key, lease, expected_steps, value, now_ms),
            Command::Fail {
                key,
                lease,
                failure,
                now_ms,
            } => self.prepare_fail(key, lease, failure, now_ms),
            Command::Revoke { key, lease } => self.prepare_revoke(key, lease),
            Command::BeginWait {
                key,
                lease,
                step,
                kind,
                name,
            } => self.prepare_wait(key, lease, step, kind, name),
            Command::FireTimer {
                key,
                incarnation,
                step,
                value,
            } => self.prepare_fire(key, incarnation, step, value),
            Command::SendMessage { key, id, payload } => self.prepare_send(key, id, payload),
            Command::Retry {
                key,
                lease,
                error,
                policy,
                now_ms,
                jitter_ms,
            } => self.prepare_retry(key, lease, error, policy, now_ms, jitter_ms),
            Command::Cancel {
                key,
                incarnation,
                reason,
                now_ms,
            } => self.prepare_cancel(key, incarnation, reason, now_ms),
            Command::Prune { key, at_ms } => self.prepare_prune(key, at_ms),
        }
    }

    pub fn publish(&mut self, proposal: Proposal) -> Ack {
        assert_eq!(proposal.expected_revision, self.revision);
        self.apply(proposal.transition)
            .expect("prepared transition must remain valid until publication");
        proposal.ack
    }

    fn prepare_submit(
        &self,
        key: String,
        args: String,
        policy: RetryPolicy,
        budget: RetryBudget,
        retention_ms: u64,
    ) -> Result<Decision, Rejection> {
        if let Some(job) = self.jobs.get(&key) {
            if job.args != args || job.policy != policy || job.budget != budget {
                return Err(Rejection::PayloadConflict);
            }
            return Ok(Decision::Duplicate(Ack {
                position: job.submission_position,
                disposition: job.disposition(),
            }));
        }
        if let Some(receipt) = self.receipts.get(&key) {
            if receipt.payload != PayloadFingerprint::of(&args)
                || receipt.policy != policy
                || receipt.budget != budget
            {
                return Err(Rejection::PayloadConflict);
            }
            return Ok(Decision::Duplicate(Ack {
                position: receipt.submission_position,
                disposition: Disposition::Completed,
            }));
        }
        let incarnation = self.next_job_incarnation.checked_add(1).unwrap();
        let position = self.next_position();
        let ack = Ack {
            position,
            disposition: Disposition::Admitted,
        };
        Ok(self.proposal(
            Transition::Submitted {
                key,
                args,
                incarnation,
                policy,
                budget,
                retention_ms,
            },
            Vec::new(),
            ack,
        ))
    }

    fn prepare_grant(
        &self,
        key: String,
        incarnation: u64,
        worker: u64,
        now_ms: u64,
    ) -> Result<Decision, Rejection> {
        let job = self.active_job(&key, incarnation)?;
        if let Some(retry) = &job.retry
            && retry.fire_at_ms > now_ms
        {
            return Err(Rejection::RetryPending {
                fire_at_ms: retry.fire_at_ms,
            });
        }
        let generation = job.last_generation;
        if let Some(current) = &job.lease {
            if current.worker == worker {
                return Ok(Decision::Duplicate(Ack {
                    position: current.position,
                    disposition: Disposition::Continue,
                }));
            }
            return Err(Rejection::LeaseActive);
        }
        // Restart budget is charged at grant time: a re-run that starts
        // without new progress since the previous run costs one restart. A
        // due retry does not, and neither does a re-grant that re-attaches to
        // a held wait: those re-runs are resumptions of durable state, not
        // restarts of a crashed activation.
        let restarts = job.restarts_after(job.progress());
        if let Some(max_restarts) = job.budget.max_restarts
            && restarts > max_restarts
        {
            let fault = Fault {
                failure: JobFailure::RestartsExhausted {
                    restarts,
                    max_restarts,
                },
                position: self.next_position(),
            };
            let intents = vec![Intent::Fault {
                key: key.clone(),
                fault: fault.clone(),
            }];
            return Ok(self.proposal(
                Transition::Faulted {
                    key,
                    incarnation,
                    lease: None,
                    fault,
                    retained_until_ms: now_ms.saturating_add(job.retention_ms),
                },
                intents,
                Ack {
                    position: self.next_position(),
                    disposition: Disposition::Completed,
                },
            ));
        }
        let lease = Lease {
            partition_incarnation: self.partition_incarnation,
            authority_epoch: self.authority_epoch,
            job_incarnation: incarnation,
            generation: generation.checked_add(1).unwrap(),
            worker,
            position: self.next_position(),
        };
        let intents = vec![Intent::Execute {
            key: key.clone(),
            lease: lease.clone(),
            replay: job.steps.clone(),
        }];
        Ok(self.proposal(
            Transition::Granted { key, lease },
            intents,
            Ack {
                position: self.next_position(),
                disposition: Disposition::Continue,
            },
        ))
    }

    // Each `prepare_*` takes exactly the fields of its `Command` variant; this
    // one carries the durable step identity (`kind` + `name`) alongside the
    // attempt coordinates, which pushes it one past clippy's limit.
    #[expect(
        clippy::too_many_arguments,
        reason = "mirrors Command::Step's field list one-for-one; bundling kind+name into a struct here would hide the identity the guard is checking"
    )]
    fn prepare_step(
        &self,
        key: String,
        lease: Lease,
        step_id: u64,
        attempt: u64,
        value: String,
        kind: StepKind,
        name: Option<String>,
    ) -> Result<Decision, Rejection> {
        let job = self.job_for_lease(&key, lease.job_incarnation)?;
        if let Some(existing) = job.steps.get(step_id as usize) {
            // Sleep and recv steps are committed by the wait protocol only, so
            // a commit naming one never parks as a duplicate: it is divergent
            // replay. The lease check still runs first, mirroring the model,
            // which classifies a non-effect duplicate by lease validity
            // before kind — a stale replayer must learn its lease is gone,
            // not that the log diverged.
            let wait_protocol = matches!(kind, StepKind::Sleep | StepKind::Recv)
                || matches!(existing.kind, StepKind::Sleep | StepKind::Recv);
            if !wait_protocol
                && existing.kind == kind
                && existing.name == name
                && existing.lease == lease
                && existing.attempt == attempt
                && existing.value == value
            {
                return Ok(Decision::Duplicate(Ack {
                    position: existing.position,
                    disposition: Disposition::Park,
                }));
            }
            self.require_current_lease(job, &lease)?;
            if wait_protocol || existing.kind != kind || existing.name != name {
                return Err(Rejection::Nondeterministic);
            }
            return Err(Rejection::PayloadConflict);
        }
        self.require_current_lease(job, &lease)?;
        if kind != StepKind::Effect {
            return Err(Rejection::Nondeterministic);
        }
        if step_id != job.steps.len() as u64 || job.wait.is_some() {
            return Err(Rejection::OutOfOrder);
        }
        if attempt != 0 {
            return Err(Rejection::InvalidAttempt);
        }
        let step = Step {
            id: step_id,
            kind,
            name,
            attempt,
            value,
            lease,
            position: self.next_position(),
        };
        let intents = vec![Intent::Continue {
            key: key.clone(),
            step: step.clone(),
        }];
        Ok(self.proposal(
            Transition::StepCommitted { key, step },
            intents,
            Ack {
                position: self.next_position(),
                disposition: Disposition::Continue,
            },
        ))
    }

    fn prepare_complete(
        &self,
        key: String,
        lease: Lease,
        expected_steps: u64,
        value: String,
        now_ms: u64,
    ) -> Result<Decision, Rejection> {
        if let Some(receipt) = self.receipts.get(&key) {
            if receipt.incarnation != lease.job_incarnation {
                return Err(Rejection::IncarnationConflict);
            }
            let Outcome::Completed(terminal) = &receipt.outcome else {
                return Err(Rejection::StaleLease);
            };
            if terminal.lease != lease {
                return Err(Rejection::StaleLease);
            }
            if terminal.value == value && expected_steps == receipt.steps {
                return Ok(Decision::Duplicate(Ack {
                    position: terminal.position,
                    disposition: Disposition::Completed,
                }));
            }
            return Err(Rejection::PayloadConflict);
        }
        let job = self.job_for_lease(&key, lease.job_incarnation)?;
        self.require_current_lease(job, &lease)?;
        if expected_steps != job.steps.len() as u64 || job.wait.is_some() {
            return Err(Rejection::OutOfOrder);
        }
        let terminal = Terminal {
            value,
            lease,
            position: self.next_position(),
        };
        let intents = vec![Intent::Terminal {
            key: key.clone(),
            terminal: terminal.clone(),
        }];
        Ok(self.proposal(
            Transition::Completed {
                key,
                terminal,
                retained_until_ms: now_ms.saturating_add(job.retention_ms),
            },
            intents,
            Ack {
                position: self.next_position(),
                disposition: Disposition::Completed,
            },
        ))
    }

    /// Settles the job with the failure its lease holder reported.
    ///
    /// A failure for a key that already settled carries no new information
    /// unless it names a different failure, so the identical one is
    /// acknowledged idempotently and a different one conflicts with the durable
    /// receipt. A panic message is truncated to the durable maximum before that
    /// comparison, so a resent panic still matches the settlement it produced.
    fn prepare_fail(
        &self,
        key: String,
        lease: Lease,
        failure: JobFailure,
        now_ms: u64,
    ) -> Result<Decision, Rejection> {
        let failure = bounded_failure(failure);
        if let Some(receipt) = self.receipts.get(&key) {
            if receipt.incarnation != lease.job_incarnation {
                return Err(Rejection::IncarnationConflict);
            }
            let Outcome::Faulted(existing) = &receipt.outcome else {
                return Err(Rejection::StaleLease);
            };
            if existing.failure == failure {
                return Ok(Decision::Duplicate(Ack {
                    position: existing.position,
                    disposition: Disposition::Completed,
                }));
            }
            return Err(Rejection::PayloadConflict);
        }
        let job = self.job_for_lease(&key, lease.job_incarnation)?;
        self.require_current_lease(job, &lease)?;
        let position = self.next_position();
        let fault = Fault { failure, position };
        // Publication re-applies the transition, so admission runs the same
        // check replay will: a settlement the log would reject never commits.
        job.validate_fault(&fault, Some(&lease), position)
            .map_err(|_| Rejection::InvalidFault)?;
        let intents = vec![Intent::Fault {
            key: key.clone(),
            fault: fault.clone(),
        }];
        Ok(self.proposal(
            Transition::Faulted {
                key,
                incarnation: lease.job_incarnation,
                lease: Some(lease),
                fault,
                retained_until_ms: now_ms.saturating_add(job.retention_ms),
            },
            intents,
            Ack {
                position,
                disposition: Disposition::Completed,
            },
        ))
    }

    fn prepare_revoke(&self, key: String, lease: Lease) -> Result<Decision, Rejection> {
        let job = self.job_for_lease(&key, lease.job_incarnation)?;
        self.require_current_lease(job, &lease)?;
        let intents = vec![Intent::Revoke {
            key: key.clone(),
            lease: lease.clone(),
        }];
        Ok(self.proposal(
            Transition::Revoked { key, lease },
            intents,
            Ack {
                position: self.next_position(),
                disposition: Disposition::Park,
            },
        ))
    }

    fn prepare_wait(
        &self,
        key: String,
        lease: Lease,
        step_id: u64,
        kind: WaitKind,
        name: Option<String>,
    ) -> Result<Decision, Rejection> {
        let job = self.job_for_lease(&key, lease.job_incarnation)?;
        self.require_current_lease(job, &lease)?;
        // A re-granted invocation replays up to a wait that the authority
        // already resolved (`Continue`: resume with the committed value) or
        // still holds (`Park`): it re-attaches instead of starting a second
        // wait with a new deadline.
        if let Some(resolved) = job.steps.get(step_id as usize) {
            if resolved.kind != kind.step_kind() || resolved.name != name {
                return Err(Rejection::Nondeterministic);
            }
            return Ok(Decision::Duplicate(Ack {
                position: resolved.position,
                disposition: Disposition::Continue,
            }));
        }
        if let Some(wait) = &job.wait {
            if wait.step == step_id
                && std::mem::discriminant(&wait.kind) == std::mem::discriminant(&kind)
                && wait.name == name
            {
                return Ok(Decision::Duplicate(Ack {
                    position: wait.position,
                    disposition: Disposition::Park,
                }));
            }
            if wait.step == step_id {
                return Err(Rejection::Nondeterministic);
            }
            return Err(Rejection::OutOfOrder);
        }
        if step_id != job.steps.len() as u64 {
            return Err(Rejection::OutOfOrder);
        }
        let position = self.next_position();
        if kind == WaitKind::Message
            && let Some(queued) = job.inbox.front()
        {
            let step = Step {
                id: step_id,
                kind: StepKind::Recv,
                name,
                attempt: 0,
                value: queued.payload.clone(),
                lease,
                position,
            };
            return Ok(self.resolution(key, job, step, Resolution::Queued, Disposition::Continue));
        }
        let wait = Wait {
            step: step_id,
            kind,
            name,
            lease,
            position,
        };
        let intents = vec![Intent::Arm {
            key: key.clone(),
            wait: wait.clone(),
        }];
        Ok(self.proposal(
            Transition::WaitStarted { key, wait },
            intents,
            Ack {
                position,
                disposition: Disposition::Park,
            },
        ))
    }

    fn prepare_fire(
        &self,
        key: String,
        incarnation: u64,
        step_id: u64,
        value: String,
    ) -> Result<Decision, Rejection> {
        let job = self.active_job(&key, incarnation)?;
        if let Some(resolved) = job
            .steps
            .get(step_id as usize)
            .filter(|step| step.kind == StepKind::Sleep)
        {
            if resolved.value != value {
                return Err(Rejection::PayloadConflict);
            }
            return Ok(Decision::Duplicate(Ack {
                position: resolved.position,
                disposition: Disposition::Continue,
            }));
        }
        let Some(wait) = job
            .wait
            .as_ref()
            .filter(|wait| wait.step == step_id && matches!(wait.kind, WaitKind::Timer { .. }))
        else {
            return Err(Rejection::NotWaiting);
        };
        let step = Step {
            id: step_id,
            kind: StepKind::Sleep,
            name: wait.name.clone(),
            attempt: 0,
            value,
            lease: wait.lease.clone(),
            position: self.next_position(),
        };
        Ok(self.resolution(key, job, step, Resolution::Timer, Disposition::Continue))
    }

    fn prepare_send(
        &self,
        key: String,
        id: String,
        payload: String,
    ) -> Result<Decision, Rejection> {
        if self.receipts.contains_key(&key) {
            return Err(Rejection::AlreadyTerminal);
        }
        let job = self.jobs.get(&key).ok_or(Rejection::UnknownJob)?;
        if let Some(accepted) = job.messages.get(&id) {
            if accepted.payload != payload {
                return Err(Rejection::PayloadConflict);
            }
            return Ok(Decision::Duplicate(Ack {
                position: accepted.position,
                disposition: Disposition::Admitted,
            }));
        }
        let position = self.next_position();
        let message = Message {
            id,
            payload,
            position,
        };
        if let Some(wait) = job
            .wait
            .as_ref()
            .filter(|wait| wait.kind == WaitKind::Message)
        {
            let step = Step {
                id: wait.step,
                kind: StepKind::Recv,
                name: wait.name.clone(),
                attempt: 0,
                value: message.payload.clone(),
                lease: wait.lease.clone(),
                position,
            };
            let resolution = Resolution::Delivered(message);
            return Ok(self.resolution(key, job, step, resolution, Disposition::Admitted));
        }
        Ok(self.proposal(
            Transition::MessageAccepted { key, message },
            Vec::new(),
            Ack {
                position,
                disposition: Disposition::Admitted,
            },
        ))
    }

    /// Releases the lease and parks the job on a durable retry computed from
    /// its policy. Exhaustion rejects without releasing the current lease so the
    /// runtime can still settle the job with [`Command::Fail`].
    fn prepare_retry(
        &self,
        key: String,
        lease: Lease,
        error: String,
        policy: Option<RetryPolicy>,
        now_ms: u64,
        jitter_ms: u64,
    ) -> Result<Decision, Rejection> {
        let job = self.job_for_lease(&key, lease.job_incarnation)?;
        self.require_current_lease(job, &lease)?;
        if job.wait.is_some() {
            return Err(Rejection::OutOfOrder);
        }
        if let Some(max_retries) = job.budget.max_retries
            && job.retry_attempts >= max_retries
        {
            return Err(Rejection::RetriesExhausted {
                retries: job.retry_attempts,
                max_retries,
            });
        }
        // A per-failure override steers this retry's delay only; the durable
        // submission policy is what every later retry falls back to.
        let effective = policy.unwrap_or(job.policy);
        let retry = RetryWait {
            fire_at_ms: now_ms
                .saturating_add(effective.delay_ms(job.retry_attempts))
                .saturating_add(jitter_ms),
            error,
            position: self.next_position(),
        };
        Ok(self.proposal(
            Transition::RetryScheduled { key, lease, retry },
            Vec::new(),
            Ack {
                position: self.next_position(),
                disposition: Disposition::Park,
            },
        ))
    }

    /// Atomically settles cancellation before any host-side session teardown.
    /// Removing the live job fences every late lease result, wait resolution,
    /// retry deadline, and message while the receipt preserves the key until
    /// its retention window elapses.
    fn prepare_cancel(
        &self,
        key: String,
        incarnation: u64,
        reason: String,
        now_ms: u64,
    ) -> Result<Decision, Rejection> {
        let job = self.active_job(&key, incarnation)?;
        let fault = Fault {
            failure: JobFailure::Cancelled {
                reason: bounded_fault_text(reason),
            },
            position: self.next_position(),
        };
        let intents = vec![Intent::Fault {
            key: key.clone(),
            fault: fault.clone(),
        }];
        Ok(self.proposal(
            Transition::Faulted {
                key,
                incarnation,
                lease: None,
                fault,
                retained_until_ms: now_ms.saturating_add(job.retention_ms),
            },
            intents,
            Ack {
                position: self.next_position(),
                disposition: Disposition::Completed,
            },
        ))
    }

    /// Retires a settled key once its retention window has elapsed.
    ///
    /// A receipt is the key's only durable record, so deleting it is what frees
    /// the key: a later submission is a new incarnation and re-executes from an
    /// empty journal. The receipt's digest and the payload-conflict check die
    /// with it. Admission and replay share the same two facts: a receipt must
    /// exist, and the host's instant must be at or past the receipt's durable
    /// deadline. Neither side reads a clock.
    fn prepare_prune(&self, key: String, at_ms: u64) -> Result<Decision, Rejection> {
        let Some(receipt) = self.receipts.get(&key) else {
            return Err(if self.jobs.contains_key(&key) {
                Rejection::NotSettled
            } else {
                Rejection::UnknownJob
            });
        };
        if at_ms < receipt.retained_until_ms {
            return Err(Rejection::NotExpired);
        }
        Ok(self.proposal(
            Transition::Pruned {
                incarnation: receipt.incarnation,
                key,
                at_ms,
            },
            Vec::new(),
            Ack {
                position: self.next_position(),
                disposition: Disposition::Completed,
            },
        ))
    }

    /// Commits the step a wait resolved to. Only a leased invocation can be
    /// parked on the wait; an unleased job picks the step up from its replay
    /// when it is next granted.
    fn resolution(
        &self,
        key: String,
        job: &Job,
        step: Step,
        resolution: Resolution,
        disposition: Disposition,
    ) -> Decision {
        let intents = if job.lease.is_some() {
            vec![Intent::Continue {
                key: key.clone(),
                step: step.clone(),
            }]
        } else {
            Vec::new()
        };
        let ack = Ack {
            position: step.position,
            disposition,
        };
        self.proposal(
            Transition::WaitResolved {
                key,
                step,
                resolution,
            },
            intents,
            ack,
        )
    }

    fn proposal(&self, transition: Transition, intents: Vec<Intent>, ack: Ack) -> Decision {
        Decision::Commit(Box::new(Proposal {
            expected_revision: self.revision,
            transition,
            intents,
            ack,
        }))
    }

    fn next_position(&self) -> u64 {
        self.revision.checked_add(1).unwrap()
    }

    fn active_job(&self, key: &str, incarnation: u64) -> Result<&Job, Rejection> {
        if let Some(receipt) = self.receipts.get(key) {
            if receipt.incarnation != incarnation {
                return Err(Rejection::IncarnationConflict);
            }
            return Err(Rejection::AlreadyTerminal);
        }
        let job = self.jobs.get(key).ok_or(Rejection::UnknownJob)?;
        if job.incarnation != incarnation {
            return Err(Rejection::IncarnationConflict);
        }
        Ok(job)
    }

    fn job_for_lease(&self, key: &str, incarnation: u64) -> Result<&Job, Rejection> {
        if let Some(receipt) = self.receipts.get(key) {
            if receipt.incarnation != incarnation {
                return Err(Rejection::IncarnationConflict);
            }
            return Err(Rejection::StaleLease);
        }
        let job = self.jobs.get(key).ok_or(Rejection::UnknownJob)?;
        if job.incarnation != incarnation {
            return Err(Rejection::IncarnationConflict);
        }
        Ok(job)
    }

    fn require_current_lease(&self, job: &Job, lease: &Lease) -> Result<(), Rejection> {
        if job.lease.as_ref() != Some(lease)
            || lease.partition_incarnation != self.partition_incarnation
        {
            return Err(Rejection::StaleLease);
        }
        Ok(())
    }

    fn validate_snapshot(partition_incarnation: u64, snapshot: &Snapshot) -> Res<()> {
        if snapshot.format != SNAPSHOT_FORMAT {
            return Err(ferr!(
                "unsupported authority snapshot format {} (this build reads {SNAPSHOT_FORMAT}); offline reset or migration required",
                snapshot.format
            ));
        }
        if snapshot.partition_incarnation != partition_incarnation {
            return Err(ferr!("snapshot belongs to another partition incarnation"));
        }
        let mut incarnations = BTreeSet::new();
        for (key, job) in &snapshot.jobs {
            if snapshot.receipts.contains_key(key)
                || job.incarnation == 0
                || job.incarnation > snapshot.next_job_incarnation
                || !incarnations.insert(job.incarnation)
                || job.submission_position == 0
                || job.submission_position > snapshot.cut
            {
                return Err(ferr!("invalid snapshot job header for {key}"));
            }
            for (idx, step) in job.steps.iter().enumerate() {
                if step.id != idx as u64
                    || step.position > snapshot.cut
                    || step.lease.job_incarnation != job.incarnation
                {
                    return Err(ferr!("invalid snapshot step for {key}"));
                }
            }
            if let Some(wait) = &job.wait
                && (wait.step != job.steps.len() as u64
                    || wait.position > snapshot.cut
                    || wait.lease.job_incarnation != job.incarnation)
            {
                return Err(ferr!("invalid snapshot wait for {key}"));
            }
            if let Some(retry) = &job.retry
                && (retry.position > snapshot.cut || job.wait.is_some())
            {
                return Err(ferr!("invalid snapshot retry for {key}"));
            }
        }
        for (key, receipt) in &snapshot.receipts {
            if receipt.incarnation == 0
                || receipt.incarnation > snapshot.next_job_incarnation
                || !incarnations.insert(receipt.incarnation)
                || receipt.submission_position == 0
                || receipt.submission_position > receipt.outcome.position()
                || receipt.outcome.position() > snapshot.cut
            {
                return Err(ferr!("invalid snapshot receipt for {key}"));
            }
            if let Outcome::Completed(terminal) = &receipt.outcome
                && terminal.lease.job_incarnation != receipt.incarnation
            {
                return Err(ferr!("invalid snapshot terminal lease for {key}"));
            }
        }
        Ok(())
    }

    fn apply(&mut self, transition: Transition) -> Res<()> {
        let position = self.next_position();
        #[cfg(test)]
        let durable = transition.clone();
        match transition {
            Transition::Submitted {
                key,
                args,
                incarnation,
                policy,
                budget,
                retention_ms,
            } => {
                if incarnation != self.next_job_incarnation + 1
                    || self.jobs.contains_key(&key)
                    || self.receipts.contains_key(&key)
                {
                    return Err(ferr!("invalid durable submission"));
                }
                self.next_job_incarnation = incarnation;
                let old = self.jobs.insert(
                    key,
                    Job {
                        args,
                        incarnation,
                        submission_position: position,
                        steps: Vec::new(),
                        last_generation: 0,
                        lease: None,
                        wait: None,
                        inbox: VecDeque::new(),
                        messages: BTreeMap::new(),
                        policy,
                        budget,
                        retention_ms,
                        retry: None,
                        retry_attempts: 0,
                        restarts: 0,
                        progress: None,
                    },
                );
                assert!(old.is_none(), "fishy");
            }
            Transition::Granted { key, lease } => {
                let job = self.durable_job_mut(&key, lease.job_incarnation)?;
                job.apply_grant(lease, position)?;
            }
            Transition::StepCommitted { key, step } => {
                let job = self.durable_job_mut(&key, step.lease.job_incarnation)?;
                if step.kind != StepKind::Effect
                    || job.lease.as_ref() != Some(&step.lease)
                    || job.wait.is_some()
                    || step.id != job.steps.len() as u64
                    || step.position != position
                {
                    return Err(ferr!("invalid durable step"));
                }
                job.retry_attempts = 0;
                job.steps.push(step);
            }
            Transition::Completed {
                key,
                terminal,
                retained_until_ms,
            } => {
                let job = self.durable_job_mut(&key, terminal.lease.job_incarnation)?;
                if job.lease.as_ref() != Some(&terminal.lease)
                    || job.wait.is_some()
                    || terminal.position != position
                {
                    return Err(ferr!("invalid durable completion"));
                }
                let receipt = job.receipt(Outcome::Completed(terminal), retained_until_ms);
                self.jobs.remove(&key).expect("validated above");
                self.receipts.insert(key, receipt);
            }
            Transition::Revoked { key, lease } => {
                let job = self.durable_job_mut(&key, lease.job_incarnation)?;
                if job.lease.as_ref() != Some(&lease) {
                    return Err(ferr!("invalid durable revocation"));
                }
                job.lease = None;
            }
            Transition::WaitStarted { key, wait } => {
                let job = self.durable_job_mut(&key, wait.lease.job_incarnation)?;
                let inbox_drained = job.inbox.is_empty();
                if job.lease.as_ref() != Some(&wait.lease)
                    || job.wait.is_some()
                    || wait.step != job.steps.len() as u64
                    || wait.position != position
                    || (wait.kind == WaitKind::Message && !inbox_drained)
                {
                    return Err(ferr!("invalid durable wait"));
                }
                job.wait = Some(wait);
            }
            Transition::WaitResolved {
                key,
                step,
                resolution,
            } => {
                let job = self.durable_job_mut(&key, step.lease.job_incarnation)?;
                job.apply_resolution(step, resolution, position)?;
            }
            Transition::MessageAccepted { key, message } => {
                let job = self
                    .jobs
                    .get_mut(&key)
                    .ok_or_else(|| ferr!("durable transition names unknown job"))?;
                job.apply_message(message, position)?;
            }
            Transition::RetryScheduled { key, lease, retry } => {
                let job = self.durable_job_mut(&key, lease.job_incarnation)?;
                job.apply_retry(&lease, retry, position)?;
            }
            Transition::Faulted {
                key,
                incarnation,
                lease,
                fault,
                retained_until_ms,
            } => {
                if lease
                    .as_ref()
                    .is_some_and(|lease| lease.job_incarnation != incarnation)
                {
                    return Err(ferr!("durable fault names another incarnation"));
                }
                let job = self.durable_job_mut(&key, incarnation)?;
                job.validate_fault(&fault, lease.as_ref(), position)?;
                let receipt = job.receipt(Outcome::Faulted(fault), retained_until_ms);
                self.jobs.remove(&key).expect("validated above");
                self.receipts.insert(key, receipt);
            }
            Transition::Pruned {
                key,
                incarnation,
                at_ms,
            } => {
                let receipt = self
                    .receipts
                    .get(&key)
                    .ok_or_else(|| ferr!("durable prune names unknown receipt"))?;
                if receipt.incarnation != incarnation {
                    return Err(ferr!("durable prune names obsolete incarnation"));
                }
                if at_ms < receipt.retained_until_ms {
                    return Err(ferr!("durable prune precedes its retention deadline"));
                }
                self.receipts.remove(&key).expect("validated above");
            }
        }
        self.revision = position;
        #[cfg(test)]
        self.suffix.push(durable);
        Ok(())
    }

    fn durable_job_mut(&mut self, key: &str, incarnation: u64) -> Res<&mut Job> {
        let job = self
            .jobs
            .get_mut(key)
            .ok_or_else(|| ferr!("durable transition names unknown job"))?;
        if job.incarnation != incarnation {
            return Err(ferr!("durable transition names obsolete incarnation"));
        }
        Ok(job)
    }
}

#[cfg(test)]
mod conformance;

#[cfg(test)]
mod tests;
