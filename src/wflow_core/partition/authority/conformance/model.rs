//! The Quint model's view of the authority: the record shapes
//! `bounded::wflow_authority::state` deserialises into, and the total
//! projections of the Rust state onto them.

use super::*;
use wflow_wdk::StepKind;
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
}
pub(super) fn model_progress(progress: &Progress) -> ModelProgress {
    ModelProgress {
        steps: progress.steps,
        waiting: progress.waiting.map_or(-1, |waiting| waiting as i64),
    }
}
pub(super) fn empty_progress() -> ModelProgress {
    ModelProgress {
        steps: 0,
        waiting: -1,
    }
}
#[derive(Debug, PartialEq, Eq, Deserialize)]
pub(super) struct ModelFault {
    pub(super) failure: String,
    pub(super) restarts: u64,
    pub(super) retries: u64,
    pub(super) position: u64,
}
/// Projection of a durable fault onto the model's vocabulary.
///
/// The model's `Fault` carries the two counters the authority recomputes from
/// durable state: `restarts` for `RestartsExhausted` and `retries` for
/// `RetriesExhausted`. `Cancelled`, `Panicked` and `ReplayDiverged` carry
/// neither, so both project as 0. `ReplayDiverged` is unreachable here: the
/// model ends a divergent replay at the `nondeterministic` rejection that
/// produces it, never at the settlement that follows.
pub(super) fn failure_name(failure: &JobFailure) -> (&'static str, u64, u64) {
    match failure {
        JobFailure::Cancelled { .. } => ("cancelled", 0, 0),
        JobFailure::RestartsExhausted { restarts, .. } => ("restarts-exhausted", *restarts, 0),
        JobFailure::RetriesExhausted { retries, .. } => ("retries-exhausted", 0, *retries),
        JobFailure::ReplayDiverged { .. } => ("replay-diverged", 0, 0),
        JobFailure::Panicked(_) => ("panicked", 0, 0),
    }
}
pub(super) fn model_fault(fault: &Fault) -> ModelFault {
    let (failure, restarts, retries) = failure_name(&fault.failure);
    ModelFault {
        failure: failure.into(),
        restarts,
        retries,
        position: fault.position,
    }
}
pub(super) fn empty_fault() -> ModelFault {
    ModelFault {
        failure: String::new(),
        restarts: 0,
        retries: 0,
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
    }
}
pub(super) fn model_receipt(key: &str, receipt: &Receipt) -> ModelJob {
    let (has_terminal, terminal, has_fault, fault) = match &receipt.outcome {
        Outcome::Completed(terminal) => (true, model_terminal(terminal), false, empty_fault()),
        Outcome::Faulted(fault) => (false, empty_terminal(), true, model_fault(fault)),
    };
    ModelJob {
        key: key.to_owned(),
        // A settled receipt keeps only a fingerprint of the submission
        // arguments, while the model keeps the arguments themselves: it treats
        // them as the payload identity, so the fingerprint is a 1:1 function of
        // them in this abstraction and a receipt projects back to its
        // arguments. The driver submits `args = key` for every job (`submit`),
        // so a settled receipt's identity is its key — exactly what the model
        // recorded at submission. Blanking the field would erase the identity a
        // conflicting resubmission is compared against, which is the only
        // reason the receipt carries a payload at all.
        args: key.to_owned(),
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
        Rejection::InvalidFault => "invalid-fault",
    }
}
