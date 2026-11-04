use super::*;
use quint_connect::Config;
use quint_connect::Driver;
use quint_connect::Result;
use quint_connect::State;
use quint_connect::Step as TraceStep;
use quint_connect::runner::Config as RunnerConfig;
use quint_connect::runner::RunConfig;
use quint_connect::runner::run_test;
use quint_connect::switch;
use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;

mod model;
use model::*;

/// The model file, relative to the crate root `cargo test` runs in.
const SPEC: &str = "partition/authority.qnt";

/// The deadline the model's `beginTimerWait` starts every timer with
/// (`TIMER_FIRE_AT`).
const TIMER: WaitKind = WaitKind::Timer { fire_at_ms: 5 };

/// The retry policy every sampled submission carries; the model schedules
/// `RETRY_DELAY` as the parked retry's deadline (jitter is 0, so `fire_at_ms`
/// is exactly `now_ms + RETRY_DELAY`).
const RETRY_DELAY: u64 = 3;
/// Mirrors the model's `MAX_RETRIES`: one transient failure may park a retry
/// per phase; the next moves forward work into cleanup and is rejected
/// `RetriesExhausted` once there.
const MAX_RETRIES: u64 = 1;
/// Mirrors the model's `MAX_RESTARTS`: one progress-free re-run may restart,
/// the next grant settles the job with `RestartsExhausted`.
const MAX_RESTARTS: u64 = 1;

/// The boundary names the model's `STEP_NAMES` samples from; `""` is `None`.
fn model_step_name(name: &str) -> Option<String> {
    (!name.is_empty()).then(|| name.to_owned())
}

/// Inverts a boundary name along the model's two-element `STEP_NAMES` set.
fn inverted_name(name: &Option<String>) -> String {
    if name.is_some() {
        String::new()
    } else {
        "named".into()
    }
}

/// Sleep and recv steps are committed by the wait protocol only; a step
/// command naming one is divergent replay, never a duplicate.
fn wait_protocol(kind: StepKind) -> bool {
    matches!(kind, StepKind::Sleep | StepKind::Recv)
}

struct AuthorityDriver {
    authority: Authority,
    durable: Vec<Transition>,
    intents: Vec<Intent>,
    outcome: ModelOutcome,
    /// How often each model action was replayed, shared with the test so it
    /// can refuse a trace set that never reaches part of the model.
    coverage: Rc<RefCell<BTreeMap<String, usize>>>,
}

/// The outcome the model reports for `init` and the restarts: no command ran.
fn silent_outcome(kind: &str) -> ModelOutcome {
    ModelOutcome {
        kind: kind.into(),
        position: 0,
        disposition: String::new(),
        reason: String::new(),
    }
}

impl Default for AuthorityDriver {
    fn default() -> Self {
        Self {
            authority: Authority::new(7, 1),
            durable: Vec::new(),
            intents: Vec::new(),
            outcome: silent_outcome("init"),
            coverage: Rc::default(),
        }
    }
}

impl Driver for AuthorityDriver {
    type State = ModelState;

    fn config() -> Config {
        Config {
            state: &["bounded::wflow_authority::state"],
            nondet: &[],
        }
    }

    fn step(&mut self, step: &TraceStep) -> Result {
        *self
            .coverage
            .borrow_mut()
            .entry(step.action_taken.clone())
            .or_default() += 1;
        switch!(step {
            init => self.reset(),
            submit(key: String) => self.submit(key),
            duplicateSubmit(key: String) => self.submit(key),
            grant(key: String, worker: u64) => self.grant(key, worker),
            grantDueRetry(key: String, worker: u64) => self.grant_due_retry(key, worker),
            grantCleanupThroughRetry(key: String, worker: u64) => self.grant(key, worker),
            commitStep(key: String, name: String) => self.commit_step(key, StepKind::Effect, name),
            commitDefer(key: String, name: String) => self.commit_step(key, StepKind::Defer, name),
            deferBeforeCleanup(key: String, name: String) => self.commit_step(key, StepKind::Defer, name),
            effectAfterCleanup(key: String, name: String) => self.commit_step(key, StepKind::Effect, name),
            duplicateStep(key: String) => self.duplicate_step(key),
            staleResult(key: String) => self.stale_result(key),
            renamedStep(key: String) => self.renamed_step(key),
            renamedWait(key: String) => self.renamed_wait(key),
            retry(key: String) => self.retry(key),
            retryExhausted(key: String) => self.retry(key),
            cleanupRetryExhausted(key: String) => self.retry(key),
            retryWhileWaiting(key: String) => self.retry(key),
            cancel(key: String) => self.cancel(key),
            duplicateCancel(key: String) => self.cancel(key),
            cancelSettled(key: String) => self.cancel(key),
            failJob(key: String) => self.fail(key),
            duplicateFail(key: String) => self.fail(key),
            failWhileWaiting(key: String) => self.fail(key),
            complete(key: String) => self.complete(key),
            duplicateComplete(key: String) => self.duplicate_complete(key),
            revoke(key: String) => self.revoke(key),
            beginTimerWait(key: String) => self.begin_wait(key, TIMER),
            beginMessageWait(key: String) => self.begin_wait(key, WaitKind::Message),
            waitInCleanup(key: String, timer: bool) => self.begin_wait(key, wait_kind(timer)),
            reattachWait(key: String) => self.reattach_wait(key, false),
            reattachResolvedWait(key: String, timer: bool) => self.reattach_resolved_wait(key, timer),
            conflictingWait(key: String) => self.reattach_wait(key, true),
            stepWhileWaiting(key: String) => self.commit_step(key, StepKind::Effect, String::new()),
            completeWhileWaiting(key: String) => self.complete(key),
            fireTimer(key: String) => self.fire_timer(key),
            duplicateFire(key: String) => self.duplicate_fire(key),
            staleFire(key: String) => self.fire_timer(key),
            fireAfterCancel(key: String) => self.fire_timer(key),
            sendMessage(key: String, id: String) => self.send(key, id.clone(), id),
            sendInCleanup(key: String, id: String) => self.send(key, id.clone(), id),
            duplicateMessage(key: String, id: String) => self.send(key, id.clone(), id),
            conflictingMessage(key: String, id: String) => self.send(key, id, "conflict".into()),
            expire(key: String) => self.expire(key, DEFAULT_RETENTION_MS),
            expireBeforeDeadline(key: String) => self.expire(key, 0),
            crash => self.crash(),
            snapshotRestart => self.snapshot_restart(),
        })
    }
}

/// The wait kind a model action's `timer` pick selects.
fn wait_kind(timer: bool) -> WaitKind {
    if timer { TIMER } else { WaitKind::Message }
}

impl State<AuthorityDriver> for ModelState {
    fn from_driver(driver: &AuthorityDriver) -> Result<Self> {
        let mut jobs: Vec<ModelJob> = driver
            .authority
            .jobs()
            .iter()
            .map(|(key, job)| model_job(key, job))
            .chain(
                driver
                    .authority
                    .receipts()
                    .iter()
                    .map(|(key, receipt)| model_receipt(key, receipt)),
            )
            .collect();
        jobs.sort_by_key(|job| job.submission_position);
        Ok(Self {
            partition_incarnation: driver.authority.partition_incarnation(),
            authority_epoch: driver.authority.authority_epoch(),
            revision: driver.authority.revision(),
            durable_len: driver.durable.len() as u64,
            next_job_incarnation: driver.authority.next_job_incarnation(),
            jobs,
            intents: driver.intents.iter().map(model_intent).collect(),
            outcome: ModelOutcome {
                kind: driver.outcome.kind.clone(),
                position: driver.outcome.position,
                disposition: driver.outcome.disposition.clone(),
                reason: driver.outcome.reason.clone(),
            },
        })
    }
}

impl AuthorityDriver {
    fn reset(&mut self) {
        self.authority = Authority::new(7, 1);
        self.durable.clear();
        self.intents.clear();
        self.outcome = silent_outcome("init");
    }

    fn incarnation(&self, key: &str) -> u64 {
        self.authority
            .jobs()
            .get(key)
            .map(|job| job.incarnation)
            .unwrap_or_else(|| self.authority.receipts()[key].incarnation)
    }

    fn submit(&mut self, key: String) {
        self.execute(Command::Submit {
            args: key.clone(),
            key,
            policy: RetryPolicy::Fixed {
                delay_ms: RETRY_DELAY,
            },
            budget: RetryBudget {
                max_retries: Some(MAX_RETRIES),
                max_restarts: Some(MAX_RESTARTS),
            },
            retention_ms: DEFAULT_RETENTION_MS,
        });
    }

    /// The model's `grant` runs with the host clock at 0, so a job parked on
    /// a retry with a positive deadline is always rejected `RetryPending`.
    fn grant(&mut self, key: String, worker: u64) {
        let incarnation = self.authority.jobs()[&key].incarnation;
        self.execute(Command::Grant {
            key,
            incarnation,
            worker,
            now_ms: 0,
        });
    }

    /// The model's `grantDueRetry` runs with the host clock at the parked
    /// retry's deadline, so the retry is always due.
    fn grant_due_retry(&mut self, key: String, worker: u64) {
        let job = &self.authority.jobs()[&key];
        let now_ms = job.retry.as_ref().unwrap().fire_at_ms;
        let incarnation = job.incarnation;
        self.execute(Command::Grant {
            key,
            incarnation,
            worker,
            now_ms,
        });
    }

    /// Commits a step of `kind` at the job's next step under its current
    /// lease. The model's `commitStep`/`effectAfterCleanup` report an effect,
    /// `commitDefer`/`deferBeforeCleanup` a cleanup step; the guards differ,
    /// the command does not.
    fn commit_step(&mut self, key: String, kind: StepKind, name: String) {
        let job = &self.authority.jobs()[&key];
        let value = match kind {
            StepKind::Defer => "undo",
            _ => "value",
        };
        self.execute(Command::CommitStep {
            key,
            lease: job.lease.clone().unwrap(),
            step: job.steps.len() as u64,
            attempt: 0,
            value: value.into(),
            kind,
            name: model_step_name(&name),
        });
    }

    fn duplicate_step(&mut self, key: String) {
        let job = &self.authority.jobs()[&key];
        let step = job.steps.last().unwrap().clone();
        let lease = if wait_protocol(step.kind) {
            job.lease.clone().unwrap_or(step.lease)
        } else {
            step.lease
        };
        self.execute(Command::CommitStep {
            key,
            lease,
            step: step.id,
            attempt: step.attempt,
            value: step.value,
            kind: step.kind,
            name: step.name,
        });
    }

    fn stale_result(&mut self, key: String) {
        let job = &self.authority.jobs()[&key];
        let mut lease = job.lease.clone().unwrap();
        lease.authority_epoch = 0;
        self.execute(Command::CommitStep {
            key,
            lease,
            step: job.steps.len() as u64,
            attempt: 0,
            value: "stale".into(),
            kind: StepKind::Effect,
            name: None,
        });
    }

    /// Replays the last committed step under an inverted boundary name, from
    /// the current lease when the job holds one and from the step's recorded
    /// lease otherwise (a wait-protocol step's lease is always stale, which
    /// is what makes `duplicate_step` classify by lease first).
    /// Re-issues the last committed step under the current lease (or, for an
    /// unleased job, the lease that committed it) with an inverted name: the
    /// model classifies the outcome by whether the job is leased, so a job
    /// re-granted since the step was committed must replay under its new
    /// lease to be told its replay diverged rather than that its lease is
    /// gone.
    fn renamed_step(&mut self, key: String) {
        let job = &self.authority.jobs()[&key];
        let last = job.steps.last().unwrap();
        self.execute(Command::CommitStep {
            key,
            lease: job.lease.clone().unwrap_or_else(|| last.lease.clone()),
            step: last.id,
            attempt: last.attempt,
            value: last.value.clone(),
            kind: last.kind,
            name: model_step_name(&inverted_name(&last.name)),
        });
    }

    /// Re-issues the last committed step as a wait with an inverted name: a
    /// sleep step keeps its timer kind (and its deadline, which the model
    /// never inspects), anything else is re-issued as a timer.
    fn renamed_wait(&mut self, key: String) {
        let job = &self.authority.jobs()[&key];
        let last = job.steps.last().unwrap();
        let kind = match last.kind {
            StepKind::Sleep => TIMER,
            _ => WaitKind::Message,
        };
        self.execute(Command::BeginWait {
            key,
            lease: job.lease.clone().unwrap_or_else(|| last.lease.clone()),
            step: last.id,
            kind,
            name: model_step_name(&inverted_name(&last.name)),
        });
    }

    /// The transient failure the model's `retry`, `retryExhausted`,
    /// `cleanupRetryExhausted`, and `retryWhileWaiting` actions all report;
    /// the guards differ, the command does not (clock 0, no jitter, durable
    /// policy).
    fn retry(&mut self, key: String) {
        let job = &self.authority.jobs()[&key];
        self.execute(Command::Retry {
            key,
            lease: job.lease.clone().unwrap(),
            error: "transient".into(),
            policy: None,
            now_ms: 0,
            jitter_ms: 0,
        });
    }

    /// The operator cancel the model's `cancel`, `duplicateCancel`, and
    /// `cancelSettled` actions all issue; it needs no lease.
    fn cancel(&mut self, key: String) {
        let incarnation = self.incarnation(&key);
        self.execute(Command::Cancel {
            key,
            incarnation,
            reason: "operator".into(),
        });
    }

    /// The terminal failure the model's `failJob`, `duplicateFail`, and
    /// `failWhileWaiting` actions all report from the current lease.
    fn fail(&mut self, key: String) {
        let lease = self.authority.jobs()[&key].lease.clone().unwrap();
        self.execute(Command::Fail {
            key,
            lease,
            error: "boom".into(),
        });
    }

    fn complete(&mut self, key: String) {
        let job = &self.authority.jobs()[&key];
        self.execute(Command::Complete {
            key,
            lease: job.lease.clone().unwrap(),
            expected_steps: job.steps.len() as u64,
            value: "done".into(),
            now_ms: 0,
        });
    }

    fn duplicate_complete(&mut self, key: String) {
        let receipt = &self.authority.receipts()[&key];
        let Outcome::Completed(terminal) = &receipt.outcome else {
            unreachable!("duplicateComplete requires a completed receipt");
        };
        self.execute(Command::Complete {
            key,
            lease: terminal.lease.clone(),
            expected_steps: receipt.steps,
            value: terminal.value.clone(),
            now_ms: 0,
        });
    }

    fn revoke(&mut self, key: String) {
        let lease = self.authority.jobs()[&key].lease.clone().unwrap();
        self.execute(Command::Revoke { key, lease });
    }

    fn begin_wait(&mut self, key: String, kind: WaitKind) {
        let step = self.authority.jobs()[&key].steps.len() as u64;
        self.wait(key, step, kind);
    }

    /// Re-issues the held wait from the current lease, as a re-granted
    /// invocation does after replaying up to it: a timer with its own later
    /// deadline, or, when `conflicting`, the held step with the other kind.
    fn reattach_wait(&mut self, key: String, conflicting: bool) {
        let wait = self.authority.jobs()[&key].wait.clone().unwrap();
        let kind = match (wait.kind, conflicting) {
            (WaitKind::Timer { fire_at_ms }, false) => WaitKind::Timer {
                fire_at_ms: fire_at_ms + 4,
            },
            (WaitKind::Message, false) | (WaitKind::Timer { .. }, true) => WaitKind::Message,
            (WaitKind::Message, true) => TIMER,
        };
        self.wait(key, wait.step, kind);
    }

    fn reattach_resolved_wait(&mut self, key: String, timer: bool) {
        let step = self.authority.jobs()[&key].steps.len() as u64 - 1;
        let kind = if timer { TIMER } else { WaitKind::Message };
        self.wait(key, step, kind);
    }

    fn wait(&mut self, key: String, step: u64, kind: WaitKind) {
        let lease = self.authority.jobs()[&key].lease.clone().unwrap();
        self.execute(Command::BeginWait {
            key,
            lease,
            step,
            kind,
            name: None,
        });
    }

    /// Fires the timer at the job's next step, which is the step any held
    /// wait suspends (or, for a settled receipt, its terminal step count).
    fn fire_timer(&mut self, key: String) {
        let (incarnation, step) = if let Some(job) = self.authority.jobs().get(&key) {
            (job.incarnation, job.steps.len() as u64)
        } else {
            let receipt = &self.authority.receipts()[&key];
            (receipt.incarnation, receipt.steps)
        };
        self.execute(Command::FireTimer {
            key,
            incarnation,
            step,
            value: "null".into(),
        });
    }

    fn duplicate_fire(&mut self, key: String) {
        let job = &self.authority.jobs()[&key];
        let step = job.steps.last().unwrap().id;
        self.execute(Command::FireTimer {
            key,
            incarnation: job.incarnation,
            step,
            value: "null".into(),
        });
    }

    fn send(&mut self, key: String, id: String, payload: String) {
        self.execute(Command::SendMessage { key, id, payload });
    }

    fn expire(&mut self, key: String, now_ms: u64) {
        let incarnation = self.incarnation(&key);
        self.execute(Command::Expire {
            key,
            incarnation,
            now_ms,
        });
    }

    fn crash(&mut self) {
        let epoch = self.authority.authority_epoch() + 1;
        self.authority = Authority::recover(7, epoch, None, &self.durable).unwrap();
        self.restarted();
    }

    fn snapshot_restart(&mut self) {
        let epoch = self.authority.authority_epoch() + 1;
        let snapshot = self.authority.snapshot();
        self.authority = Authority::recover(7, epoch, Some(snapshot), &[]).unwrap();
        self.restarted();
    }

    fn restarted(&mut self) {
        self.intents.clear();
        self.outcome = silent_outcome("restart");
    }

    fn execute(&mut self, command: Command) {
        self.intents.clear();
        match self.authority.prepare(command) {
            Ok(Decision::Commit(proposal)) => {
                self.intents = proposal.intents.clone();
                self.durable.push(proposal.transition.clone());
                let ack = self.authority.publish(*proposal);
                self.outcome = ModelOutcome {
                    kind: "accepted".into(),
                    position: ack.position,
                    disposition: disposition_name(ack.disposition).into(),
                    reason: String::new(),
                };
            }
            Ok(Decision::Duplicate(ack)) => {
                self.outcome = ModelOutcome {
                    kind: "duplicate".into(),
                    position: ack.position,
                    disposition: disposition_name(ack.disposition).into(),
                    reason: String::new(),
                };
            }
            Err(rejection) => {
                self.outcome = ModelOutcome {
                    kind: "rejected".into(),
                    position: 0,
                    disposition: String::new(),
                    reason: rejection_name(&rejection).into(),
                };
            }
        }
    }
}

/// The actions `step` chooses from, read off the model's `any` block so the
/// coverage check cannot drift from it as actions are added.
fn model_actions() -> BTreeSet<String> {
    let spec = std::fs::read_to_string(SPEC).unwrap();
    let (_, block) = spec.split_once("action step = any {").unwrap();
    let (block, _) = block.split_once("\n  }").unwrap();
    block
        .lines()
        .map(|line| line.trim().trim_end_matches(','))
        .filter(|action| !action.is_empty())
        .map(str::to_owned)
        .collect()
}

/// Replays sampled model traces against the authority and then refuses a
/// sample that never took one of the model's actions: a conformance pass
/// over traces that skip part of the model proves nothing about that part.
#[test]
fn authority_conformance() {
    let coverage = Rc::new(RefCell::new(BTreeMap::new()));
    let driver = AuthorityDriver {
        coverage: Rc::clone(&coverage),
        ..Default::default()
    };
    let config = RunnerConfig {
        test_name: "authority_conformance".into(),
        gen_config: RunConfig {
            spec: SPEC.into(),
            main: Some("bounded".into()),
            init: Some("init".into()),
            step: Some("step".into()),
            // Wait resolutions and the cleanup paths sit behind submit,
            // grant, and begin-wait or cancel, and the always-enabled
            // restarts and rejections dilute the sampled actions; 200
            // traces of 32 steps is what it takes for every action in
            // `step` (down to a terminal failure reported from a held wait)
            // to appear under this seed.
            max_samples: Some(200),
            max_steps: Some(32),
            seed: "424242".into(),
        },
    };
    if let Err(err) = run_test(driver, config) {
        panic!("{err}");
    }
    let coverage = coverage.borrow();
    let missing: Vec<_> = model_actions()
        .into_iter()
        .filter(|action| !coverage.contains_key(action))
        .collect();
    assert!(
        missing.is_empty(),
        "the sampled traces never took {missing:?}; observed {coverage:#?}"
    );
}
