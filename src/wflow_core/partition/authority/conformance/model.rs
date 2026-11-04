//! The Quint model's view of the authority: the record shapes
//! `bounded::wflow_authority::state` deserialises into, and the total
//! projections of the Rust state onto them.

use super::*;
pub(super) fn step_name(name: &Option<String>) -> String {
    name.clone().unwrap_or_default()
}
#[derive(Debug, PartialEq, Eq, Deserialize)]
pub(super) struct ModelStep {
    pub(super) id: u64,
    pub(super) kind: StepKind,
    pub(super) name: String,
    pub(super) attempt: u64,
    pub(super) value: String,
    pub(super) lease: Lease,
    pub(super) position: u64,
}
pub(super) fn model_step(step: &Step) -> ModelStep {
    ModelStep {
        id: step.id,
        kind: step.kind,
        name: step_name(&step.name),
        attempt: step.attempt,
        value: step.value.clone(),
        lease: step.lease.clone(),
        position: step.position,
    }
}
pub(super) fn empty_model_step() -> ModelStep {
    ModelStep {
        id: 0,
        kind: StepKind::Effect,
        name: String::new(),
        attempt: 0,
        value: String::new(),
        lease: empty_lease(),
        position: 0,
    }
}
#[derive(Debug, PartialEq, Eq, Deserialize)]
pub(super) struct ModelRetry {
    pub(super) fire_at: u64,
    pub(super) error: String,
    pub(super) position: u64,
}
pub(super) fn model_retry(retry: &RetryWait) -> ModelRetry {
    ModelRetry {
        fire_at: retry.fire_at_ms,
        error: retry.error.clone(),
        position: retry.position,
    }
}
pub(super) fn empty_retry() -> ModelRetry {
    ModelRetry {
        fire_at: 0,
        error: String::new(),
        position: 0,
    }
}
#[derive(Debug, PartialEq, Eq, Deserialize)]
pub(super) struct ModelProgress {
    pub(super) steps: u64,
    /// The held wait's step number, or -1 for `None` (the model cannot store
    /// an option inside a record it round-trips through JSON).
    pub(super) waiting: i64,
    pub(super) cleanup: bool,
}
pub(super) fn model_progress(progress: &Progress) -> ModelProgress {
    ModelProgress {
        steps: progress.steps,
        waiting: progress.waiting.map_or(-1, |waiting| waiting as i64),
        cleanup: progress.cleanup,
    }
}
pub(super) fn empty_progress() -> ModelProgress {
    ModelProgress {
        steps: 0,
        waiting: -1,
        cleanup: false,
    }
}
/// `Cleanup` with its cause flattened: `cause` names the [`CleanupCause`]
/// variant and `reason` carries its payload (the cancel reason or the
/// terminal error); the interrupted lease is `has_lease`/`lease`.
#[derive(Debug, PartialEq, Eq, Deserialize)]
pub(super) struct ModelCleanup {
    pub(super) cause: String,
    pub(super) reason: String,
    pub(super) steps: u64,
    pub(super) has_lease: bool,
    pub(super) lease: Lease,
    pub(super) position: u64,
}
pub(super) fn model_cleanup(cleanup: &Cleanup) -> ModelCleanup {
    let (cause, reason) = match &cleanup.cause {
        CleanupCause::Cancelled { reason } => ("cancelled", reason),
        CleanupCause::Failed { error } => ("failed", error),
        CleanupCause::RetriesExhausted { last_error, .. } => ("retries-exhausted", last_error),
    };
    ModelCleanup {
        cause: cause.into(),
        reason: reason.clone(),
        steps: cleanup.steps,
        has_lease: cleanup.lease.is_some(),
        lease: cleanup.lease.clone().unwrap_or_else(empty_lease),
        position: cleanup.position,
    }
}
pub(super) fn empty_cleanup() -> ModelCleanup {
    ModelCleanup {
        cause: String::new(),
        reason: String::new(),
        steps: 0,
        has_lease: false,
        lease: empty_lease(),
        position: 0,
    }
}
#[derive(Debug, PartialEq, Eq, Deserialize)]
pub(super) struct ModelFault {
    pub(super) failure: String,
    pub(super) restarts: u64,
    pub(super) position: u64,
}
/// Total projection of [`JobFailure`] onto the model's failure names; only
/// `RestartsExhausted` carries a restart count (the sole authority-side fault
/// the sampled traces can produce).
pub(super) fn failure_name(failure: &JobFailure) -> (&'static str, u64) {
    match failure {
        JobFailure::Cancelled { .. } => ("cancelled", 0),
        JobFailure::CleanupFailed { .. } => ("cleanup-failed", 0),
        JobFailure::ReplayDiverged { .. } => ("replay-diverged", 0),
        JobFailure::Panicked(_) => ("panicked", 0),
        JobFailure::RetriesExhausted { .. } => ("retries-exhausted", 0),
        JobFailure::RestartsExhausted { restarts, .. } => ("restarts-exhausted", *restarts),
        JobFailure::Host(_) => ("host", 0),
    }
}
pub(super) fn model_fault(fault: &Fault) -> ModelFault {
    let (failure, restarts) = failure_name(&fault.failure);
    ModelFault {
        failure: failure.into(),
        restarts,
        position: fault.position,
    }
}
pub(super) fn empty_fault() -> ModelFault {
    ModelFault {
        failure: String::new(),
        restarts: 0,
        position: 0,
    }
}
#[derive(Debug, PartialEq, Eq, Deserialize)]
pub(super) struct ModelTerminal {
    pub(super) value: String,
    pub(super) lease: Lease,
    pub(super) position: u64,
}
pub(super) fn model_terminal(terminal: &Terminal) -> ModelTerminal {
    ModelTerminal {
        value: terminal.value.clone(),
        lease: terminal.lease.clone(),
        position: terminal.position,
    }
}
#[derive(Debug, PartialEq, Eq, Deserialize)]
pub(super) struct ModelJob {
    pub(super) key: String,
    pub(super) args: String,
    pub(super) incarnation: u64,
    pub(super) submission_position: u64,
    pub(super) steps: Vec<ModelStep>,
    pub(super) last_generation: u64,
    pub(super) leased: bool,
    pub(super) lease: Lease,
    pub(super) waiting: bool,
    pub(super) wait: ModelWait,
    pub(super) inbox: Vec<Message>,
    pub(super) delivered: u64,
    pub(super) has_terminal: bool,
    pub(super) terminal: ModelTerminal,
    pub(super) retrying: bool,
    pub(super) retry: ModelRetry,
    pub(super) retry_attempts: u64,
    pub(super) restarts: u64,
    pub(super) has_progress: bool,
    pub(super) progress: ModelProgress,
    pub(super) has_fault: bool,
    pub(super) fault: ModelFault,
    pub(super) has_cleanup: bool,
    pub(super) cleanup: ModelCleanup,
}
pub(super) fn model_job(key: &str, job: &Job) -> ModelJob {
    let mut inbox: Vec<Message> = job
        .messages
        .iter()
        .map(|(id, receipt)| Message {
            id: id.clone(),
            payload: receipt.payload.clone(),
            position: receipt.position,
        })
        .collect();
    inbox.sort_by_key(|message| message.position);
    let delivered = (inbox.len() - job.inbox.len()) as u64;
    assert!(
        inbox[delivered as usize..].iter().eq(job.inbox.iter()),
        "undelivered inbox queue must match the unconsumed suffix of accepted messages"
    );
    ModelJob {
        key: key.to_owned(),
        args: job.args.clone(),
        incarnation: job.incarnation,
        submission_position: job.submission_position,
        steps: job.steps.iter().map(model_step).collect(),
        last_generation: job.last_generation,
        leased: job.lease.is_some(),
        lease: job.lease.clone().unwrap_or_else(empty_lease),
        waiting: job.wait.is_some(),
        wait: job.wait.as_ref().map_or_else(empty_wait, model_wait),
        inbox,
        delivered,
        has_terminal: false,
        terminal: empty_terminal(),
        retrying: job.retry.is_some(),
        retry: job.retry.as_ref().map_or_else(empty_retry, model_retry),
        retry_attempts: job.retry_attempts,
        restarts: job.restarts,
        has_progress: job.progress.is_some(),
        progress: job
            .progress
            .as_ref()
            .map_or_else(empty_progress, model_progress),
        has_fault: false,
        fault: empty_fault(),
        has_cleanup: job.cleanup.is_some(),
        cleanup: job
            .cleanup
            .as_ref()
            .map_or_else(empty_cleanup, model_cleanup),
    }
}
pub(super) fn model_receipt(key: &str, receipt: &Receipt) -> ModelJob {
    let (has_terminal, terminal, has_fault, fault) = match &receipt.outcome {
        Outcome::Completed(terminal) => (true, model_terminal(terminal), false, empty_fault()),
        Outcome::Faulted(fault) => (false, empty_terminal(), true, model_fault(fault)),
    };
    ModelJob {
        key: key.to_owned(),
        args: receipt.args.clone(),
        incarnation: receipt.incarnation,
        submission_position: receipt.submission_position,
        steps: Vec::new(),
        last_generation: 0,
        leased: false,
        lease: empty_lease(),
        waiting: false,
        wait: empty_wait(),
        inbox: Vec::new(),
        delivered: 0,
        has_terminal,
        terminal,
        retrying: false,
        retry: empty_retry(),
        retry_attempts: 0,
        restarts: 0,
        has_progress: false,
        progress: empty_progress(),
        has_fault,
        fault,
        has_cleanup: false,
        cleanup: empty_cleanup(),
    }
}
/// `Wait` with its kind flattened: a timer carries its `fire_at`, a message
/// wait leaves it 0.
#[derive(Debug, PartialEq, Eq, Deserialize)]
pub(super) struct ModelWait {
    pub(super) step: u64,
    pub(super) timer: bool,
    pub(super) fire_at: u64,
    pub(super) lease: Lease,
    pub(super) position: u64,
}
#[derive(Debug, PartialEq, Eq, Deserialize)]
pub(super) struct ModelIntent {
    pub(super) kind: String,
    pub(super) key: String,
    pub(super) lease: Lease,
    pub(super) replay: Vec<ModelStep>,
    pub(super) step: ModelStep,
    pub(super) wait: ModelWait,
    pub(super) terminal: ModelTerminal,
    pub(super) fault: ModelFault,
    pub(super) cleanup: ModelCleanup,
}
#[derive(Debug, PartialEq, Eq, Deserialize)]
pub(super) struct ModelOutcome {
    pub(super) kind: String,
    pub(super) position: u64,
    pub(super) disposition: String,
    pub(super) reason: String,
}
#[derive(Debug, PartialEq, Eq, Deserialize)]
pub(super) struct ModelState {
    pub(super) partition_incarnation: u64,
    pub(super) authority_epoch: u64,
    pub(super) revision: u64,
    pub(super) durable_len: u64,
    pub(super) next_job_incarnation: u64,
    pub(super) jobs: Vec<ModelJob>,
    pub(super) intents: Vec<ModelIntent>,
    pub(super) outcome: ModelOutcome,
}
pub(super) fn model_intent(intent: &Intent) -> ModelIntent {
    let mut model = ModelIntent {
        kind: String::new(),
        key: String::new(),
        lease: empty_lease(),
        replay: Vec::new(),
        step: empty_model_step(),
        wait: empty_wait(),
        terminal: empty_terminal(),
        fault: empty_fault(),
        cleanup: empty_cleanup(),
    };
    match intent {
        Intent::Execute { key, lease, replay } => {
            model.kind = "execute".into();
            model.key = key.clone();
            model.lease = lease.clone();
            model.replay = replay.iter().map(model_step).collect();
        }
        Intent::Continue { key, step } => {
            model.kind = "continue".into();
            model.key = key.clone();
            model.step = model_step(step);
        }
        Intent::Revoke { key, lease } => {
            model.kind = "revoke".into();
            model.key = key.clone();
            model.lease = lease.clone();
        }
        Intent::Arm { key, wait } => {
            model.kind = "arm".into();
            model.key = key.clone();
            model.wait = model_wait(wait);
        }
        Intent::Terminal { key, terminal } => {
            model.kind = "terminal".into();
            model.key = key.clone();
            model.terminal = model_terminal(terminal);
        }
        Intent::Fault { key, fault } => {
            model.kind = "fault".into();
            model.key = key.clone();
            model.fault = model_fault(fault);
        }
        Intent::Cleanup { key, cleanup } => {
            model.kind = "cleanup".into();
            model.key = key.clone();
            model.cleanup = model_cleanup(cleanup);
        }
    }
    model
}
pub(super) fn empty_lease() -> Lease {
    Lease {
        partition_incarnation: 0,
        authority_epoch: 0,
        job_incarnation: 0,
        generation: 0,
        worker: 0,
        position: 0,
    }
}
pub(super) fn empty_terminal() -> ModelTerminal {
    ModelTerminal {
        value: String::new(),
        lease: empty_lease(),
        position: 0,
    }
}
pub(super) fn empty_wait() -> ModelWait {
    ModelWait {
        step: 0,
        timer: false,
        fire_at: 0,
        lease: empty_lease(),
        position: 0,
    }
}
pub(super) fn model_wait(wait: &Wait) -> ModelWait {
    let (timer, fire_at) = match wait.kind {
        WaitKind::Timer { fire_at_ms } => (true, fire_at_ms),
        WaitKind::Message => (false, 0),
    };
    ModelWait {
        step: wait.step,
        timer,
        fire_at,
        lease: wait.lease.clone(),
        position: wait.position,
    }
}
pub(super) fn disposition_name(disposition: Disposition) -> &'static str {
    match disposition {
        Disposition::Admitted => "admitted",
        Disposition::Continue => "continue",
        Disposition::Park => "park",
        Disposition::Completed => "completed",
    }
}
pub(super) fn rejection_name(rejection: &Rejection) -> &'static str {
    match rejection {
        Rejection::UnknownJob => "unknown-job",
        Rejection::IncarnationConflict => "incarnation-conflict",
        Rejection::PayloadConflict => "payload-conflict",
        Rejection::AlreadyTerminal => "already-terminal",
        Rejection::LeaseActive => "lease-active",
        Rejection::StaleLease => "stale-lease",
        Rejection::OutOfOrder => "out-of-order",
        Rejection::InvalidAttempt => "invalid-attempt",
        Rejection::NotWaiting => "not-waiting",
        Rejection::Nondeterministic => "nondeterministic",
        Rejection::RetryPending { .. } => "retry-pending",
        Rejection::RetriesExhausted { .. } => "retries-exhausted",
        Rejection::RetentionPending { .. } => "retention-pending",
        Rejection::WrongPhase => "wrong-phase",
    }
}
