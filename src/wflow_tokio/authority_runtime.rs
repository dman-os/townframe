//! Local workflow sessions over the partition commit authority.
//!
//! One actor task owns the [`Journal`] writer, the [`authority::Authority`]
//! and every live [`session::Session`]. Each command goes through
//! `prepare → Journal::append → publish`: the transition is appended as one
//! journal entry (so the journal frontier always equals the authority
//! revision), and only after the append is acknowledged does the actor act on
//! the proposal's intents — spawning an invocation for `Execute`, resuming a
//! parked session for `Continue`, waking result waiters for `Terminal`. A
//! workflow therefore never observes a step that is not durable, and a warm
//! step costs exactly one commit with the session parked across it.
//!
//! Admission commits `Submitted`, then `Granted` to the local worker, whose
//! `Execute` intent starts an invocation through the native or wasm host. A transient failure revokes
//! the lease and schedules a durable retry under the submission policy and
//! budget. `Completed` and `Faulted` settle the job into a compact [`authority::Receipt`]
//! (dropping step history, inbox, and message deduplication state) and, in the
//! same atomic commit, write a receipt-index entry keyed by the job key, so
//! [`AuthorityRuntime::get_result`] and [`JobHandle::await_result`] are durable
//! lookups that need no replay. When the receipt's retention window elapses,
//! `Expired` reclaims the receipt and its index entry atomically so the key can
//! be submitted again under a new incarnation.
//!
//! Restart loads the latest published snapshot (which carries only active jobs
//! and unexpired receipts) and replays only the uncompacted suffix into the
//! authority. Recovery clears every lease, so each active job is granted again
//! under the new authority epoch and its invocation replays the committed
//! step values without running their closures.
//!
//! Payloads are opaque to the authority: the submission `args` is the JSON
//! [`Invocation`], a step `value` is the raw step JSON, and a terminal `value`
//! is the JSON [`types::JobResult`]. A commit failure fail-stops the actor.
//!
//! Sleeps and receives are durable waits. A `WaitStep` yield commits
//! `BeginWait`; the actor then holds the session parked in `waiting` until
//! the authority commits the step the wait resolved to, and delivers that
//! step's value to it. A timer's deadline is the wall-clock instant the
//! session computed when the wait began; it is recorded in the `WaitStarted`
//! transition and kept in the actor's deadline queue, whose earliest entry
//! the actor sleeps towards and fires with `FireTimer`. A message wait
//! resolves when [`AuthorityRuntime::send_message`] commits a message for the
//! job, or immediately if one is already queued in its inbox.
//!
//! Recovery re-arms every durable timer from its recorded deadline and fires
//! the ones already due before re-granting, so a restart neither restarts a
//! sleep nor loses it. The re-granted invocation replays up to a pending wait
//! and re-attaches to it: `BeginWait` for a wait the authority still holds
//! parks the session again, and for a wait resolved between the grant and the
//! re-attach (a timer coming due, a message arriving) resumes it with the
//! committed value.

use std::collections::BTreeSet;
use std::hash::DefaultHasher;
use std::hash::Hasher;
use std::num::NonZeroUsize;
use std::sync::Weak;

use futures::FutureExt;
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use wflow_core::kvstore::atomic::Mutation;
use wflow_core::kvstore::atomic::Writer;
use wflow_core::kvstore::journal::Journal;
use wflow_core::kvstore::journal::JournalReader;
use wflow_core::partition::CleanupCause;
use wflow_core::partition::JobFailure;
use wflow_core::partition::RetryBudget;
use wflow_core::partition::RetryPolicy;
use wflow_core::partition::authority;
use wflow_wdk::session::ReplayStep;
use wflow_wdk::session::SessionYield;
use wflow_wdk::session::WaitYield;
use wflow_wdk::types;

use crate::interlude::*;

pub mod session;

#[cfg(test)]
mod tests;

/// The only worker of this single-process slice; leases still carry it so the
/// authority's fencing rules are exercised unchanged.
const LOCAL_WORKER: u64 = 1;
const COMMAND_QUEUE: usize = 64;
const RECOVERY_PAGE: NonZeroUsize = NonZeroUsize::new(512).unwrap();
const RESULT_INDEX_PREFIX: &[u8] = b"result/";

/// Window the host spreads retry fire times over, handed to the authority as
/// `Command::Retry::jitter_ms` and folded into the durable `fire_at_ms`.
///
/// The authority reads neither a clock nor an RNG — it stamps `fire_at_ms` as
/// `now_ms + policy.delay_ms(attempts) + jitter_ms` — so the spread is a host
/// input. Deriving it from the job key (rather than drawing randomly) keeps the
/// runtime deterministic while still de-synchronising the distinct jobs that
/// fail together, which is the herd this exists for. Zero disables it.
const RETRY_JITTER_WINDOW_MS: u64 = 0;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Invocation {
    wflow_key: String,
    args_json: String,
}

/// A job to admit: its invocation plus the retry schedule and budgets the
/// authority will hold it to.
///
/// Both are part of the durable `Submitted` transition, so a recovery re-runs
/// the job under the schedule it was admitted with — a submitter cannot change
/// a job's retry semantics by resubmitting after a restart, and the authority
/// never has to ask the host what policy applies.
///
/// ```ignore
/// runtime
///     .submit(
///         Submission::new("job/1", "greet", args)
///             .policy(RetryPolicy::CappedExponential { base_ms: 100, cap_ms: 5_000 })
///             .budget(RetryBudget { max_retries: Some(5), max_restarts: None }),
///     )
///     .await?;
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Submission {
    key: String,
    invocation: Invocation,
    policy: RetryPolicy,
    budget: RetryBudget,
    retention_ms: u64,
}

impl Submission {
    /// A job that retries transient failures immediately, without bound.
    pub fn new(
        key: impl Into<String>,
        wflow_key: impl Into<String>,
        args_json: impl Into<String>,
    ) -> Self {
        Self {
            key: key.into(),
            invocation: Invocation {
                wflow_key: wflow_key.into(),
                args_json: args_json.into(),
            },
            policy: RetryPolicy::Immediate,
            budget: RetryBudget::default(),
            retention_ms: authority::DEFAULT_RETENTION_MS,
        }
    }

    /// How transient failures of this job are rescheduled.
    pub fn policy(mut self, policy: RetryPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Finite bounds on the retry and restart loops; unbounded by default.
    pub fn budget(mut self, budget: RetryBudget) -> Self {
        self.budget = budget;
        self
    }

    /// How long the compact terminal receipt is retained after settlement
    /// before the job key may be expired and reused.
    pub fn retention_ms(mut self, retention_ms: u64) -> Self {
        self.retention_ms = retention_ms;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitError {
    /// The host has no workflow registered under the requested key.
    UnknownWorkflow,
    /// The job key was already submitted with a different invocation.
    PayloadConflict,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendError {
    /// No job was submitted under the key.
    UnknownJob,
    /// The job already has a terminal outcome and will never receive it.
    AlreadyTerminal,
    /// The job is cleaning up and will never receive again.
    InCleanup,
    /// A message with the same id but a different payload was accepted.
    PayloadConflict,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelError {
    /// No job was submitted under the key.
    UnknownJob,
    /// The job already has a terminal outcome.
    AlreadyTerminal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RevokeError {
    /// No job was submitted under the key.
    UnknownJob,
    /// The job already has a terminal outcome and holds no lease.
    AlreadyTerminal,
    /// The job is parked on a durable retry, so it holds no lease to revoke;
    /// it is re-granted when the retry comes due.
    RetryPending { fire_at_ms: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpireError {
    /// No retained terminal receipt exists under the key.
    UnknownJob,
    /// The job is still active and has not settled into a terminal receipt.
    Active,
    /// The retained receipt's retention window has not elapsed yet.
    RetentionPending { expires_at_ms: u64 },
}

/// Handle for a job key on an [`AuthorityRuntime`], exposing race-free result
/// waiting ([`Self::await_result`]) and non-blocking result/receipt inspection
/// ([`Self::get_result`] / [`Self::receipt`]).
#[derive(Clone)]
pub struct JobHandle {
    key: String,
    commands: mpsc::WeakSender<Command>,
    reader: JournalReader,
}

impl JobHandle {
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Reads the durable terminal outcome of this job from the result index;
    /// `None` while the job is unsettled or after its receipt has expired.
    pub async fn get_result(&self) -> Res<Option<types::JobResult>> {
        Ok(self.receipt().await?.as_ref().map(decode_receipt))
    }

    /// Reads the durable compact terminal receipt of this job from the result
    /// index; `None` while the job is unsettled or after its receipt has
    /// expired.
    pub async fn receipt(&self) -> Res<Option<authority::Receipt>> {
        read_receipt(&self.reader, &self.key).await
    }

    /// Resolves once this job has a durable terminal outcome; `None` if the
    /// key is unknown.
    ///
    /// Race-free when the job settles before or during registration: subscribes
    /// to completion notifications first, checks the durable terminal receipt
    /// via [`Self::get_result`], and if still active awaits the wakeup hint
    /// before reading the authoritative durable receipt.
    pub async fn await_result(&self) -> Option<types::JobResult> {
        let (reply, response) = oneshot::channel();
        self.commands
            .upgrade()
            .expect(ERROR_ACTOR)
            .send(Command::WatchResult {
                key: self.key.clone(),
                reply,
            })
            .await
            .expect(ERROR_ACTOR);
        match response.await.expect(ERROR_ACTOR) {
            WatchState::Unknown => None,
            WatchState::Settled { _guard } => self.get_result().await.unwrap(),
            WatchState::Active { wakeup, _guard } => {
                if let Some(result) = self.get_result().await.unwrap() {
                    return Some(result);
                }
                wakeup.await.ok()?;
                self.get_result().await.unwrap()
            }
        }
    }
}

pub struct AuthorityRuntime {
    commands: mpsc::Sender<Command>,
    reader: JournalReader,
    actor: tokio::task::JoinHandle<()>,
}

enum WatchState {
    Active {
        wakeup: oneshot::Receiver<()>,
        _guard: Arc<()>,
    },
    Settled {
        _guard: Arc<()>,
    },
    Unknown,
}

enum Command {
    Submit {
        submission: Submission,
        reply: oneshot::Sender<Result<authority::Ack, SubmitError>>,
    },
    WatchResult {
        key: String,
        reply: oneshot::Sender<WatchState>,
    },
    SendMessage {
        key: String,
        id: String,
        payload: String,
        reply: oneshot::Sender<Result<authority::Ack, SendError>>,
    },
    Revoke {
        key: String,
        reply: oneshot::Sender<Result<authority::Ack, RevokeError>>,
    },
    Cancel {
        key: String,
        reason: String,
        reply: oneshot::Sender<Result<authority::Ack, CancelError>>,
    },
    Expire {
        key: String,
        reply: oneshot::Sender<Result<authority::Ack, ExpireError>>,
    },
    Compact {
        reply: oneshot::Sender<Res<u64>>,
    },
}

impl AuthorityRuntime {
    /// Opens the journal (bumping the authority epoch), loads the latest
    /// published snapshot if any, replays the uncompacted suffix into the
    /// authority, and resumes every non-terminal job. Every workflow a durable
    /// job names must already be registered on `host`.
    pub async fn start<W: Writer + 'static>(
        writer: W,
        partition_incarnation: u64,
        host: Arc<dyn session::Host>,
    ) -> Res<Self> {
        let journal = Journal::open(writer, partition_incarnation).await?;
        let reader = journal.reader();
        let snapshot = match reader.snapshot().await? {
            Some(published) => {
                let snapshot: authority::Snapshot = serde_json::from_slice(&published.value)?;
                if snapshot.cut != published.cut {
                    eyre::bail!(
                        "snapshot cut {} disagrees with journal metadata cut {}",
                        snapshot.cut,
                        published.cut
                    );
                }
                Some(snapshot)
            }
            None => None,
        };
        let from = snapshot.as_ref().map_or(1, |snap| snap.cut + 1);
        let transitions = read_transitions(&reader, from, journal.frontier()).await?;
        let authority = authority::Authority::recover(
            partition_incarnation,
            journal.authority_epoch(),
            snapshot,
            &transitions,
        )?;
        let (commands, commands_rx) = mpsc::channel(COMMAND_QUEUE);
        let actor = Actor {
            journal,
            authority,
            host,
            sessions: FuturesUnordered::new(),
            waiting: HashMap::new(),
            deadlines: BTreeSet::new(),
            waiters: HashMap::new(),
            reading: HashMap::new(),
        };
        Ok(Self {
            commands,
            reader,
            actor: tokio::spawn(actor.run(commands_rx)),
        })
    }

    /// Returns a [`JobHandle`] for `key`.
    pub fn handle(&self, key: impl Into<String>) -> JobHandle {
        JobHandle {
            key: key.into(),
            commands: self.commands.downgrade(),
            reader: self.reader.clone(),
        }
    }

    /// Admits the submission durably and returns its admission receipt;
    /// resubmitting the same invocation returns the original receipt with the
    /// job's current disposition.
    pub async fn submit(&self, submission: Submission) -> Result<authority::Ack, SubmitError> {
        let (reply, response) = oneshot::channel();
        self.send(Command::Submit { submission, reply }).await;
        response.await.expect(ERROR_ACTOR)
    }

    /// Admits the submission durably and returns both its admission receipt and
    /// a [`JobHandle`] for awaiting or querying its terminal result.
    pub async fn submit_handle(
        &self,
        submission: Submission,
    ) -> Result<(authority::Ack, JobHandle), SubmitError> {
        let key = submission.key.clone();
        let ack = self.submit(submission).await?;
        Ok((ack, self.handle(key)))
    }

    /// Resolves once `key` has a durable terminal outcome; `None` if the key
    /// was never submitted (or its receipt has already expired).
    pub async fn await_result(&self, key: impl Into<String>) -> Option<types::JobResult> {
        self.handle(key).await_result().await
    }

    /// Durably accepts the message `id` for the job `key`: it resolves the
    /// job's pending receive, or queues for its next one. Resending the same
    /// `id` and payload returns the original receipt without a second
    /// delivery.
    pub async fn send_message(
        &self,
        key: impl Into<String>,
        id: impl Into<String>,
        payload_json: impl Into<String>,
    ) -> Result<authority::Ack, SendError> {
        let (reply, response) = oneshot::channel();
        self.send(Command::SendMessage {
            key: key.into(),
            id: id.into(),
            payload: payload_json.into(),
            reply,
        })
        .await;
        response.await.expect(ERROR_ACTOR)
    }

    /// Durably revokes the current lease of `key` and grants a fresh lease
    /// that replays committed steps from the journal. Any in-flight session
    /// still running under the revoked lease is fenced at the authority when
    /// it next yields and never receives continuation permission.
    pub async fn revoke(&self, key: impl Into<String>) -> Result<authority::Ack, RevokeError> {
        let (reply, response) = oneshot::channel();
        self.send(Command::Revoke {
            key: key.into(),
            reply,
        })
        .await;
        response.await.expect(ERROR_ACTOR)
    }

    /// Durably cancels `key`: forward work stops at the invocation's next
    /// cooperative boundary and its registered cleanup runs, after which the
    /// job settles as [`JobFailure::Cancelled`]. The receipt is durable once
    /// `CleanupStarted` is; cancelling a job already in cleanup returns the
    /// original receipt. A job parked on a wait or a retry is woken for its
    /// cleanup at once.
    pub async fn cancel(
        &self,
        key: impl Into<String>,
        reason: impl Into<String>,
    ) -> Result<authority::Ack, CancelError> {
        let (reply, response) = oneshot::channel();
        self.send(Command::Cancel {
            key: key.into(),
            reason: reason.into(),
            reply,
        })
        .await;
        response.await.expect(ERROR_ACTOR)
    }

    /// Durably expires the retained terminal receipt for `key` once its
    /// retention window has elapsed, freeing `key` for reuse under a fresh
    /// incarnation.
    pub async fn expire(&self, key: impl Into<String>) -> Result<authority::Ack, ExpireError> {
        let (reply, response) = oneshot::channel();
        self.send(Command::Expire {
            key: key.into(),
            reply,
        })
        .await;
        response.await.expect(ERROR_ACTOR)
    }

    /// Publishes an authority snapshot at the current revision and compacts
    /// journal entries through that cut. Returns the compacted cut revision.
    pub async fn compact(&self) -> Res<u64> {
        let (reply, response) = oneshot::channel();
        self.send(Command::Compact { reply }).await;
        response.await.expect(ERROR_ACTOR)
    }

    /// Reads the durable compact terminal receipt of `key` from the result
    /// index; `None` while the job has no committed outcome or after expiry.
    pub async fn receipt(&self, key: &str) -> Res<Option<authority::Receipt>> {
        read_receipt(&self.reader, key).await
    }

    /// Reads the durable terminal outcome of `key` from the result index;
    /// `None` while the job has no committed outcome or after expiry.
    pub async fn get_result(&self, key: &str) -> Res<Option<types::JobResult>> {
        Ok(self.receipt(key).await?.as_ref().map(decode_receipt))
    }

    /// Alias for [`Self::get_result`].
    pub async fn result(&self, key: &str) -> Res<Option<types::JobResult>> {
        self.get_result(key).await
    }

    /// Stops the actor, cancelling live invocations, and releases the journal
    /// writer. Panics if the actor failed.
    pub async fn stop(self) {
        drop(self.commands);
        self.actor.await.unwrap();
    }

    async fn send(&self, command: Command) {
        self.commands.send(command).await.expect(ERROR_ACTOR);
    }
}

async fn read_transitions(
    reader: &JournalReader,
    from: u64,
    frontier: u64,
) -> Res<Vec<authority::Transition>> {
    let mut transitions =
        Vec::with_capacity(frontier.saturating_sub(from.saturating_sub(1)) as usize);
    let mut cursor = from;
    while cursor <= frontier {
        let page = reader.read(cursor, RECOVERY_PAGE).await?;
        assert!(
            !page.entries.is_empty(),
            "journal shorter than its frontier"
        );
        cursor += page.entries.len() as u64;
        for entry in page.entries {
            transitions.push(serde_json::from_slice(&entry.value)?);
        }
    }
    Ok(transitions)
}

async fn read_receipt(reader: &JournalReader, key: &str) -> Res<Option<authority::Receipt>> {
    let Some(value) = reader.index(&result_index_key(key)).await? else {
        return Ok(None);
    };
    Ok(Some(serde_json::from_slice(&value).expect(ERROR_JSON)))
}

fn result_index_key(key: &str) -> Arc<[u8]> {
    [RESULT_INDEX_PREFIX, key.as_bytes()].concat().into()
}

/// A session's next suspension together with the lease it runs under.
struct Yielded {
    key: String,
    lease: authority::Lease,
    session: Box<dyn session::Session>,
    yielded: Option<SessionYield>,
}

/// What an armed deadline belongs to. Every kind is durable in the authority
/// (a `Wait` timer, a `RetryWait`, or a retained `Receipt` expiry), so the
/// wheel is rebuilt from the journal/snapshot on recovery.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum DeadlineRef {
    /// A durable timer wait; fires with `FireTimer` once due.
    Timer { key: String, step: u64 },
    /// A durable retry; re-grants the job once due.
    Retry { key: String },
    /// A retained terminal receipt; expires the key once its retention window
    /// elapses.
    Expire { key: String, incarnation: u64 },
}

struct Actor<W> {
    sessions: FuturesUnordered<BoxFuture<'static, Yielded>>,
    /// Sessions parked on a durable wait, with the lease they run under.
    waiting: HashMap<String, (authority::Lease, Box<dyn session::Session>)>,
    host: Arc<dyn session::Host>,
    journal: Journal<W>,
    authority: authority::Authority,
    /// Armed deadlines as `(fire_at_ms, what)`, earliest first.
    deadlines: BTreeSet<(u64, DeadlineRef)>,
    /// Completion wakeup hints for in-flight `await_result` callers.
    waiters: HashMap<String, Vec<oneshot::Sender<()>>>,
    /// Weak guards held by `await_result` callers between wakeup and reading
    /// the durable receipt, so background retention expiry does not reclaim a
    /// zero-retention receipt mid-read.
    reading: HashMap<String, Vec<Weak<()>>>,
}

impl<W: Writer> Actor<W> {
    async fn run(mut self, mut commands: mpsc::Receiver<Command>) {
        self.arm_recovered_deadlines();
        // Deadlines that came due while the runtime was down resolve before the
        // re-grant: an invocation replays a fired timer's step instead of
        // suspending on it again, a due retry is granted by the wheel rather
        // than twice, and an expired receipt frees its key before new commands
        // arrive.
        self.fire_due().await;
        let recovered: Vec<(String, u64)> = self
            .authority
            .jobs()
            .iter()
            .filter(|(_, job)| job.lease.is_none())
            .map(|(key, job)| (key.clone(), job.incarnation))
            .collect();
        for (key, incarnation) in recovered {
            self.grant(key, incarnation).await;
        }
        loop {
            let next_deadline = self.deadlines.first().map(|(fire_at_ms, ..)| *fire_at_ms);
            tokio::select! {
                command = commands.recv() => match command {
                    Some(command) => self.handle_command(command).await,
                    // The handle stopped the runtime; dropping `self` cancels
                    // every live session.
                    None => break,
                },
                Some(yielded) = self.sessions.next(), if !self.sessions.is_empty() => {
                    self.handle_yield(yielded).await;
                }
                () = tokio::time::sleep(until(next_deadline.unwrap_or_default())),
                    if next_deadline.is_some() => self.fire_due().await,
            }
        }
    }

    async fn handle_command(&mut self, command: Command) {
        match command {
            Command::Submit { submission, reply } => self.submit(submission, reply).await,
            Command::WatchResult { key, reply } => {
                let state = if self.authority.jobs().contains_key(&key) {
                    let (tx, wakeup) = oneshot::channel();
                    let guard = Arc::new(());
                    self.waiters.entry(key.clone()).or_default().push(tx);
                    self.reading
                        .entry(key)
                        .or_default()
                        .push(Arc::downgrade(&guard));
                    WatchState::Active {
                        wakeup,
                        _guard: guard,
                    }
                } else if self.authority.receipts().contains_key(&key) {
                    let guard = Arc::new(());
                    self.reading
                        .entry(key)
                        .or_default()
                        .push(Arc::downgrade(&guard));
                    WatchState::Settled { _guard: guard }
                } else {
                    WatchState::Unknown
                };
                let _cancelled = reply.send(state);
            }
            Command::SendMessage {
                key,
                id,
                payload,
                reply,
            } => {
                let command = authority::Command::SendMessage { key, id, payload };
                let result = match self.commit(command).await {
                    Ok((ack, intents)) => {
                        self.wake(intents);
                        Ok(ack)
                    }
                    Err(authority::Rejection::UnknownJob) => Err(SendError::UnknownJob),
                    Err(authority::Rejection::AlreadyTerminal) => Err(SendError::AlreadyTerminal),
                    Err(authority::Rejection::WrongPhase) => Err(SendError::InCleanup),
                    Err(authority::Rejection::PayloadConflict) => Err(SendError::PayloadConflict),
                    Err(rejection) => panic!("message rejected: {rejection:?}"),
                };
                let _cancelled = reply.send(result);
            }
            Command::Cancel { key, reason, reply } => {
                let Some(job) = self.authority.jobs().get(&key) else {
                    let err = if self.authority.receipts().contains_key(&key) {
                        CancelError::AlreadyTerminal
                    } else {
                        CancelError::UnknownJob
                    };
                    let _cancelled = reply.send(Err(err));
                    return;
                };
                let command = authority::Command::Cancel {
                    key: key.clone(),
                    incarnation: job.incarnation,
                    reason,
                };
                let intents = match self.commit(command).await {
                    Ok((ack, intents)) => {
                        let _cancelled = reply.send(Ok(ack));
                        intents
                    }
                    Err(authority::Rejection::AlreadyTerminal) => {
                        let _cancelled = reply.send(Err(CancelError::AlreadyTerminal));
                        return;
                    }
                    Err(rejection) => panic!("cancel of {key} rejected: {rejection:?}"),
                };
                self.enter_cleanup(intents).await;
            }
            Command::Revoke { key, reply } => {
                let Some(job) = self.authority.jobs().get(&key) else {
                    let err = if self.authority.receipts().contains_key(&key) {
                        RevokeError::AlreadyTerminal
                    } else {
                        RevokeError::UnknownJob
                    };
                    let _cancelled = reply.send(Err(err));
                    return;
                };
                // A job parked on a retry holds no lease: the authority
                // released it when it scheduled the retry, so there is nothing
                // to fence and the re-grant is already armed on the wheel.
                let Some(lease) = job.lease.clone() else {
                    let retry = job.retry.as_ref().expect("live job is leased or retrying");
                    let _cancelled = reply.send(Err(RevokeError::RetryPending {
                        fire_at_ms: retry.fire_at_ms,
                    }));
                    return;
                };
                let ack = self
                    .revoke_and_regrant(key, lease)
                    .await
                    .expect("current lease revoke rejected");
                let _cancelled = reply.send(Ok(ack));
            }
            Command::Expire { key, reply } => {
                let result = self.expire(key).await;
                let _cancelled = reply.send(result);
            }
            Command::Compact { reply } => {
                let result = self.compact().await;
                let _cancelled = reply.send(result);
            }
        }
    }

    /// Rebuilds the deadline wheel from durable state: every timer wait keeps
    /// the deadline recorded when it began, every parked retry keeps the fire
    /// time the authority computed for it, and every retained terminal receipt
    /// keeps its expiry deadline.
    fn arm_recovered_deadlines(&mut self) {
        for (key, job) in self.authority.jobs() {
            if let Some(authority::Wait {
                step,
                kind: authority::WaitKind::Timer { fire_at_ms },
                ..
            }) = &job.wait
            {
                self.deadlines.insert((
                    *fire_at_ms,
                    DeadlineRef::Timer {
                        key: key.clone(),
                        step: *step,
                    },
                ));
            }
            if let Some(retry) = &job.retry {
                self.deadlines
                    .insert((retry.fire_at_ms, DeadlineRef::Retry { key: key.clone() }));
            }
        }
        for (key, receipt) in self.authority.receipts() {
            self.deadlines.insert((
                receipt.outcome.expires_at_ms(),
                DeadlineRef::Expire {
                    key: key.clone(),
                    incarnation: receipt.incarnation,
                },
            ));
        }
    }

    /// Fires every armed deadline whose instant has passed. A deadline whose
    /// durable state moved on without it — a wait revoked, a job settled, or a
    /// receipt already expired — is stale and dropped: the authority, not the
    /// wheel, decides what is due.
    async fn fire_due(&mut self) {
        let now = now_ms();
        while self
            .deadlines
            .first()
            .is_some_and(|(fire_at_ms, _)| *fire_at_ms <= now)
        {
            let (_, reference) = self.deadlines.pop_first().expect("checked non-empty");
            match reference {
                DeadlineRef::Timer { key, step } => {
                    let armed = self
                        .authority
                        .jobs()
                        .get(&key)
                        .and_then(|job| job.wait.as_ref())
                        .is_some_and(|wait| wait.step == step);
                    if !armed {
                        continue;
                    }
                    let command = authority::Command::FireTimer {
                        incarnation: self.authority.jobs()[&key].incarnation,
                        key,
                        step,
                        value: "null".into(),
                    };
                    let intents = self.commit_accepted(command).await;
                    self.wake(intents);
                }
                DeadlineRef::Retry { key } => {
                    let due = self
                        .authority
                        .jobs()
                        .get(&key)
                        .is_some_and(|job| job.retry.is_some());
                    if !due {
                        continue;
                    }
                    let incarnation = self.authority.jobs()[&key].incarnation;
                    self.grant(key, incarnation).await;
                }
                DeadlineRef::Expire { key, incarnation } => {
                    let due = self.authority.receipts().get(&key).is_some_and(|receipt| {
                        receipt.incarnation == incarnation && receipt.outcome.expires_at_ms() <= now
                    });
                    if !due {
                        continue;
                    }
                    if self.has_active_readers(&key) {
                        self.deadlines.insert((
                            now.saturating_add(1),
                            DeadlineRef::Expire { key, incarnation },
                        ));
                        continue;
                    }
                    let command = authority::Command::Expire {
                        key,
                        incarnation,
                        now_ms: now,
                    };
                    self.commit_accepted(command).await;
                }
            }
        }
    }

    fn has_active_readers(&mut self, key: &str) -> bool {
        let Some(readers) = self.reading.get_mut(key) else {
            return false;
        };
        readers.retain(|weak| weak.strong_count() > 0);
        if readers.is_empty() {
            self.reading.remove(key);
            false
        } else {
            true
        }
    }

    /// Delivers resolved wait steps to the sessions parked on them. A session
    /// still replaying towards its wait is not parked yet; its `BeginWait`
    /// finds the committed step and resumes it then.
    fn wake(&mut self, intents: Vec<authority::Intent>) {
        for intent in intents {
            let authority::Intent::Continue { key, step } = intent else {
                panic!("wait resolution produced a non-continue intent: {intent:?}");
            };
            if let Some((lease, session)) = self.waiting.remove(&key) {
                self.deliver(key, lease, session, &step.value);
            }
        }
    }

    fn deliver(
        &mut self,
        key: String,
        lease: authority::Lease,
        session: Box<dyn session::Session>,
        value: &str,
    ) {
        assert!(
            session.deliver(Arc::from(value)),
            "held session was cancelled"
        );
        self.park(key, lease, session);
    }

    /// Acts on a `CleanupStarted` commit. A session parked on a durable wait is
    /// woken with the cause so the wait it was suspended on returns
    /// interrupted; a running session is left to reach its next yield, where
    /// [`Self::handle_yield`] sees the job's cleanup and interrupts it there;
    /// a job the commit left unleased (parked on a retry when cancelled, or
    /// released by the exhausted retry that began the cleanup) is granted so
    /// its cleanup can run.
    async fn enter_cleanup(&mut self, intents: Vec<authority::Intent>) {
        for intent in intents {
            let authority::Intent::Cleanup { key, cleanup } = intent else {
                panic!("cleanup produced a non-cleanup intent: {intent:?}");
            };
            let job = &self.authority.jobs()[&key];
            let Some(lease) = job.lease.clone() else {
                let incarnation = job.incarnation;
                self.grant(key, incarnation).await;
                continue;
            };
            let parked = self
                .waiting
                .get(&key)
                .is_some_and(|(parked_lease, _)| *parked_lease == lease);
            if !parked {
                continue;
            }
            let (lease, session) = self.waiting.remove(&key).expect("checked parked");
            self.interrupt(key, lease, session, cleanup.cause);
        }
    }

    /// Wakes a parked session with the cleanup cause and parks it for its next
    /// yield: the boundary it was suspended on returns interrupted and the
    /// workflow runs its defers.
    fn interrupt(
        &mut self,
        key: String,
        lease: authority::Lease,
        session: Box<dyn session::Session>,
        cause: CleanupCause,
    ) {
        assert!(session.interrupt(cause), "held session was cancelled");
        self.park(key, lease, session);
    }

    async fn submit(
        &mut self,
        submission: Submission,
        reply: oneshot::Sender<Result<authority::Ack, SubmitError>>,
    ) {
        let Submission {
            key,
            invocation,
            policy,
            budget,
            retention_ms,
        } = submission;
        if !self.host.contains_wflow(&invocation.wflow_key) {
            let _cancelled = reply.send(Err(SubmitError::UnknownWorkflow));
            return;
        }
        let args = serde_json::to_string(&invocation).expect(ERROR_JSON);
        let command = authority::Command::Submit {
            key: key.clone(),
            args,
            policy,
            budget,
            retention_ms,
        };
        let ack = match self.commit(command).await {
            Ok((ack, _)) => ack,
            Err(authority::Rejection::PayloadConflict) => {
                let _cancelled = reply.send(Err(SubmitError::PayloadConflict));
                return;
            }
            Err(rejection) => panic!("submission of {key} rejected: {rejection:?}"),
        };
        // The receipt is durable once `Submitted` is; the caller need not wait
        // for the grant.
        let _cancelled = reply.send(Ok(ack));
        if ack.disposition == authority::Disposition::Admitted {
            let incarnation = self.authority.jobs()[&key].incarnation;
            self.grant(key, incarnation).await;
        }
    }

    /// Grants a lease and starts (or restarts) the invocation it entitles.
    /// A grant the authority refuses because the job is still parked on a
    /// retry re-arms the wheel instead, and a grant that exhausts the restart
    /// budget settles the job.
    async fn grant(&mut self, key: String, incarnation: u64) {
        let command = authority::Command::Grant {
            key: key.clone(),
            incarnation,
            worker: LOCAL_WORKER,
            now_ms: now_ms(),
        };
        let intents = match self.commit(command).await {
            Ok((_, intents)) => intents,
            Err(authority::Rejection::RetryPending { fire_at_ms }) => {
                self.deadlines
                    .insert((fire_at_ms, DeadlineRef::Retry { key }));
                return;
            }
            Err(rejection) => panic!("grant of {key} rejected: {rejection:?}"),
        };
        for intent in intents {
            match intent {
                authority::Intent::Execute { key, lease, replay } => {
                    self.execute(key, lease, replay).await;
                }
                authority::Intent::Fault { key, fault } => self.settled(key, fault.expires_at_ms),
                intent => panic!("grant produced an unexpected intent: {intent:?}"),
            }
        }
    }

    async fn execute(
        &mut self,
        key: String,
        lease: authority::Lease,
        replay: Vec<authority::Step>,
    ) {
        let job = &self.authority.jobs()[&key];
        let invocation: Invocation = serde_json::from_str(&job.args).expect(ERROR_JSON);
        // A job already in cleanup is re-granted with its cause so the
        // replayed invocation interrupts itself at its first forward boundary
        // instead of running new forward effects.
        let cleanup = job.cleanup.as_ref().map(|cleanup| cleanup.cause.clone());
        let replay = replay
            .into_iter()
            .map(|step| ReplayStep {
                kind: Some(step.kind),
                // The authority journal is the only replay source that pins
                // step identity, so a re-grant validates both the kind and the
                // boundary name the workflow chose.
                name: Some(step.name.map(Arc::from)),
                value_json: Some(step.value.into()),
                failed_attempts: 0,
            })
            .collect();
        let session = self
            .host
            .spawn(
                Arc::from(key.as_str()),
                &invocation.wflow_key,
                invocation.args_json,
                replay,
                cleanup,
            )
            .await
            .unwrap_or_else(|err| {
                panic!(
                    "job {key} cannot start ({err:?}); register every workflow before starting the runtime"
                )
            });
        self.park(key, lease, session);
    }

    /// Finalizes a job settlement after its compact receipt has been durably
    /// indexed and published: drops host state, arms the retention expiry
    /// deadline, and wakes any in-flight `await_result` callers so they read
    /// the authoritative durable receipt.
    fn settled(&mut self, key: String, expires_at_ms: u64) {
        self.host.forget_job(&key);
        let incarnation = self.authority.receipts()[&key].incarnation;
        self.deadlines.insert((
            expires_at_ms,
            DeadlineRef::Expire {
                key: key.clone(),
                incarnation,
            },
        ));
        let Some(waiters) = self.waiters.remove(&key) else {
            return;
        };
        for waiter in waiters {
            let _cancelled = waiter.send(());
        }
    }

    fn park(
        &mut self,
        key: String,
        lease: authority::Lease,
        mut session: Box<dyn session::Session>,
    ) {
        self.sessions.push(
            async move {
                let yielded = session.next_yield().await;
                Yielded {
                    key,
                    lease,
                    session,
                    yielded,
                }
            }
            .boxed(),
        );
    }

    async fn handle_yield(&mut self, yielded: Yielded) {
        let Yielded {
            key,
            lease,
            session,
            yielded,
        } = yielded;
        // Only dropping a session cancels it, and the actor holds it here.
        let yielded = yielded.expect("live session stopped without a final yield");
        match yielded {
            SessionYield::PersistStep {
                step_id,
                attempt_id,
                value_json,
                meta,
                ..
            } => {
                let kind = meta.kind;
                let command = authority::Command::CommitStep {
                    key: key.clone(),
                    lease: lease.clone(),
                    step: step_id,
                    attempt: attempt_id,
                    value: value_json.to_string(),
                    kind,
                    name: meta.name,
                };
                let intents = match self.commit(command).await {
                    Ok((_, intents)) => intents,
                    // A superseded session is fenced at the authority and
                    // dropped here without receiving continuation permission.
                    Err(authority::Rejection::StaleLease) => return,
                    Err(authority::Rejection::Nondeterministic) => {
                        // Either the durable step disagrees with the replayed
                        // identity, or the workflow tried to commit a sleep or
                        // receive outside the wait protocol that owns them.
                        let detail = match self.authority.jobs()[&key].steps.get(step_id as usize) {
                            Some(durable) => format!(
                                "durably {:?} named {:?}, replayed as {kind:?}",
                                durable.kind, durable.name
                            ),
                            None => format!("{kind:?} steps are durable waits, not commits"),
                        };
                        drop(session);
                        self.complete_failure(
                            key,
                            lease,
                            JobFailure::ReplayDiverged {
                                step: step_id,
                                detail,
                            },
                        )
                        .await;
                        return;
                    }
                    Err(rejection) => panic!("step {step_id} of {key} rejected: {rejection:?}"),
                };
                let [authority::Intent::Continue { .. }] = intents.as_slice() else {
                    panic!("step {step_id} of {key} was not granted continuation: {intents:?}");
                };
                // A forward effect that was in flight when the job entered
                // cleanup commits (it ran; the defer it registers must be
                // rebuilt on replay) but is the last one: the session learns
                // the cause here instead of plain continuation.
                let cleanup = self.authority.jobs()[&key].cleanup.clone();
                match cleanup {
                    Some(cleanup) if kind != wflow_wdk::StepKind::Defer => {
                        self.interrupt(key, lease, session, cleanup.cause);
                    }
                    _ => {
                        assert!(session.resume(), "held session was cancelled");
                        self.park(key, lease, session);
                    }
                }
            }
            SessionYield::WaitStep {
                step_id,
                attempt_id,
                deets,
                meta,
                ..
            } => {
                assert_eq!(
                    attempt_id, 0,
                    "authority replays never record failed attempts"
                );
                let kind = match deets {
                    WaitYield::Timer { fire_at, .. } => authority::WaitKind::Timer {
                        fire_at_ms: u64::try_from(fire_at.as_millisecond())
                            .expect("timer deadline before the unix epoch"),
                    },
                    WaitYield::Message { .. } => authority::WaitKind::Message,
                };
                // The wait's step kind is implied by what it waits on; a
                // workflow that names its boundary `Sleep` while receiving a
                // message is not describing the step it is taking.
                assert_eq!(
                    kind.step_kind(),
                    meta.kind,
                    "wait at step {step_id} of {key} yields {:?} but is named {:?}",
                    kind.step_kind(),
                    meta.kind
                );
                self.begin_wait(key, lease, session, step_id, kind, meta.name)
                    .await;
            }
            // A transient failure is the authority's to reschedule: it releases
            // the lease, parks the job on a durable retry computed from the
            // submission policy, and the wheel re-grants it when that is due.
            SessionYield::Complete(Err(types::JobError::Transient(err))) => {
                self.retry_transient(key, lease, err).await;
            }
            // Forward work failed terminally with defers registered: the cause
            // becomes durable before the first defer runs, so a crash
            // mid-cleanup resumes the cleanup instead of re-running forward
            // work whose failure might not repeat. A cancel that landed first
            // is the durable cause the session continues with.
            SessionYield::BeginCleanup { error_json } => {
                let command = authority::Command::Fail {
                    key: key.clone(),
                    lease: lease.clone(),
                    error: error_json.to_string(),
                };
                match self.commit(command).await {
                    Ok(_) => {}
                    Err(authority::Rejection::StaleLease) => return,
                    Err(rejection) => panic!("cleanup of {key} rejected: {rejection:?}"),
                }
                let cause = self.authority.jobs()[&key]
                    .cleanup
                    .as_ref()
                    .expect("accepted failure is durable on the job")
                    .cause
                    .clone();
                self.interrupt(key, lease, session, cause);
            }
            SessionYield::Complete(result) => self.complete(key, lease, &result).await,
            SessionYield::Panicked(msg) => {
                self.complete_failure(key, lease, JobFailure::Panicked(msg))
                    .await;
            }
        }
    }

    /// Parks the job on the durable retry the authority computes, and arms the
    /// wheel for it. Forward work running out of its retry budget enters
    /// cleanup instead: the authority releases the lease and this grants the
    /// job again so the registered compensation runs. Cleanup running out of
    /// its own budget settles the job: the lease is still held (the authority
    /// rejected the retry), so the failure is committed as an ordinary
    /// terminal envelope under it.
    async fn retry_transient(
        &mut self,
        key: String,
        lease: authority::Lease,
        err: types::TransientJobError,
    ) {
        // A rejected retry leaves the lease on the job, so the settlement that
        // follows still runs under it.
        let held = lease.clone();
        let command = authority::Command::Retry {
            key: key.clone(),
            lease,
            error: err.error_json.clone(),
            policy: err.retry_policy,
            now_ms: now_ms(),
            jitter_ms: retry_jitter_ms(&key),
        };
        match self.commit(command).await {
            Ok((_, intents)) if intents.is_empty() => {}
            Ok((_, intents)) => {
                self.enter_cleanup(intents).await;
                return;
            }
            // A superseded session's failure is not this job's to reschedule.
            Err(authority::Rejection::StaleLease) => return,
            Err(authority::Rejection::RetriesExhausted {
                retries,
                max_retries,
            }) => {
                self.complete_failure(
                    key,
                    held,
                    JobFailure::RetriesExhausted {
                        retries,
                        max_retries,
                        last_error: err.error_json,
                    },
                )
                .await;
                return;
            }
            Err(rejection) => panic!("transient retry of {key} rejected: {rejection:?}"),
        }
        let retry = self.authority.jobs()[&key]
            .retry
            .as_ref()
            .expect("accepted retry is durable on the job");
        self.deadlines
            .insert((retry.fire_at_ms, DeadlineRef::Retry { key }));
    }

    /// Settles the job under its live lease with a classified failure. The
    /// durable outcome is the ordinary terminal envelope, so awaiters and the
    /// result index see one shape whether the job succeeded, failed on its own,
    /// or ran out of a budget.
    async fn complete_failure(
        &mut self,
        key: String,
        lease: authority::Lease,
        failure: JobFailure,
    ) {
        let result = failure_envelope(&failure);
        self.complete(key, lease, &result).await;
    }

    async fn revoke_and_regrant(
        &mut self,
        key: String,
        lease: authority::Lease,
    ) -> Result<authority::Ack, authority::Rejection> {
        let incarnation = lease.job_incarnation;
        let (ack, intents) = self
            .commit(authority::Command::Revoke {
                key: key.clone(),
                lease,
            })
            .await?;
        for intent in intents {
            let authority::Intent::Revoke { key, lease } = intent else {
                panic!("revoke produced a non-revoke intent: {intent:?}");
            };
            if self
                .waiting
                .get(&key)
                .is_some_and(|(parked_lease, _)| *parked_lease == lease)
            {
                self.waiting.remove(&key);
            }
        }
        self.grant(key, incarnation).await;
        Ok(ack)
    }

    async fn begin_wait(
        &mut self,
        key: String,
        lease: authority::Lease,
        session: Box<dyn session::Session>,
        step: u64,
        kind: authority::WaitKind,
        name: Option<String>,
    ) {
        let command = authority::Command::BeginWait {
            key: key.clone(),
            lease: lease.clone(),
            step,
            kind,
            name,
        };
        let (ack, intents) = match self.commit(command).await {
            Ok(committed) => committed,
            Err(authority::Rejection::StaleLease) => return,
            // The job entered cleanup while the session was running towards
            // this wait; the wait is never begun and the session is woken with
            // the cause instead.
            Err(authority::Rejection::WrongPhase) => {
                let cause = self.authority.jobs()[&key]
                    .cleanup
                    .as_ref()
                    .expect("wrong phase for a wait means the job is in cleanup")
                    .cause
                    .clone();
                self.interrupt(key, lease, session, cause);
                return;
            }
            Err(authority::Rejection::Nondeterministic) => {
                // A replayed wait that disagrees with the durable one (kind or
                // name) is divergence; settle it like any other replay fork.
                let detail = match self.authority.jobs()[&key].wait {
                    Some(ref durable) => format!(
                        "durably {:?} named {:?}, replayed as {kind:?}",
                        durable.kind, durable.name
                    ),
                    None => format!("no durable wait at step {step}"),
                };
                drop(session);
                self.complete_failure(key, lease, JobFailure::ReplayDiverged { step, detail })
                    .await;
                return;
            }
            Err(rejection) => panic!("wait {step} of {key} rejected: {rejection:?}"),
        };
        match (ack.disposition, intents.as_slice()) {
            (authority::Disposition::Park, [authority::Intent::Arm { wait, .. }]) => {
                if let authority::WaitKind::Timer { fire_at_ms } = wait.kind {
                    self.deadlines.insert((
                        fire_at_ms,
                        DeadlineRef::Timer {
                            key: key.clone(),
                            step,
                        },
                    ));
                }
                self.waiting.insert(key, (lease, session));
            }
            // Re-attached to a wait the authority still holds; its timer, if
            // any, was re-armed from the durable deadline at recovery.
            (authority::Disposition::Park, []) => {
                self.waiting.insert(key, (lease, session));
            }
            // A queued message resolved the wait at once, or the wait was
            // resolved while this invocation was replaying towards it.
            (authority::Disposition::Continue, [] | [authority::Intent::Continue { .. }]) => {
                let value = self.authority.jobs()[&key].steps[step as usize]
                    .value
                    .clone();
                self.deliver(key, lease, session, &value);
            }
            outcome => panic!("unexpected wait outcome for step {step} of {key}: {outcome:?}"),
        }
    }

    async fn complete(&mut self, key: String, lease: authority::Lease, result: &types::JobResult) {
        let command = authority::Command::Complete {
            expected_steps: self.authority.jobs()[&key].steps.len() as u64,
            key,
            lease,
            value: serde_json::to_string(result).expect(ERROR_JSON),
            now_ms: now_ms(),
        };
        let intents = match self.commit(command).await {
            Ok((_, intents)) => intents,
            Err(authority::Rejection::StaleLease) => return,
            Err(rejection) => panic!("completion rejected: {rejection:?}"),
        };
        for intent in intents {
            let authority::Intent::Terminal { key, terminal } = intent else {
                panic!("completion produced a non-terminal intent: {intent:?}");
            };
            self.settled(key, terminal.expires_at_ms);
        }
    }

    async fn expire(&mut self, key: String) -> Result<authority::Ack, ExpireError> {
        let Some(receipt) = self.authority.receipts().get(&key) else {
            return if self.authority.jobs().contains_key(&key) {
                Err(ExpireError::Active)
            } else {
                Err(ExpireError::UnknownJob)
            };
        };
        let command = authority::Command::Expire {
            key: key.clone(),
            incarnation: receipt.incarnation,
            now_ms: now_ms(),
        };
        match self.commit(command).await {
            Ok((ack, _)) => Ok(ack),
            Err(authority::Rejection::RetentionPending { expires_at_ms }) => {
                Err(ExpireError::RetentionPending { expires_at_ms })
            }
            Err(rejection) => panic!("expiry of {key} rejected: {rejection:?}"),
        }
    }

    async fn compact(&mut self) -> Res<u64> {
        let snapshot = self.authority.snapshot();
        let cut = snapshot.cut;
        let bytes: Arc<[u8]> = serde_json::to_vec(&snapshot).expect(ERROR_JSON).into();
        self.journal.save_snapshot(cut, bytes).await?;
        self.journal.compact(cut).await?;
        Ok(cut)
    }

    /// Commits a command the actor itself derived from its own state; a
    /// rejection is an invariant break.
    async fn commit_accepted(&mut self, command: authority::Command) -> Vec<authority::Intent> {
        match self.commit(command).await {
            Ok((_, intents)) => intents,
            Err(rejection) => panic!("authority rejected an actor command: {rejection:?}"),
        }
    }

    /// `prepare → append → publish`. The intents are returned only after the
    /// transition (and, for settlements/expiries, its result-index mutation) is
    /// durable.
    async fn commit(
        &mut self,
        command: authority::Command,
    ) -> Result<(authority::Ack, Vec<authority::Intent>), authority::Rejection> {
        let mut proposal = match self.authority.prepare(command)? {
            authority::Decision::Commit(proposal) => proposal,
            authority::Decision::Duplicate(ack) => return Ok((ack, Vec::new())),
        };
        let entry: Arc<[u8]> = serde_json::to_vec(&proposal.transition)
            .expect(ERROR_JSON)
            .into();
        let indexes = match &proposal.transition {
            authority::Transition::Completed { key, terminal } => {
                let receipt = self.authority.jobs()[key]
                    .receipt(authority::Outcome::Completed(terminal.clone()));
                vec![Mutation {
                    key: result_index_key(key),
                    value: Some(serde_json::to_vec(&receipt).expect(ERROR_JSON).into()),
                }]
            }
            authority::Transition::Faulted { key, fault, .. } => {
                let receipt =
                    self.authority.jobs()[key].receipt(authority::Outcome::Faulted(fault.clone()));
                vec![Mutation {
                    key: result_index_key(key),
                    value: Some(serde_json::to_vec(&receipt).expect(ERROR_JSON).into()),
                }]
            }
            authority::Transition::Expired { key, .. } => vec![Mutation {
                key: result_index_key(key),
                value: None,
            }],
            _ => Vec::new(),
        };
        let position = self
            .journal
            .append(self.authority.revision(), entry, indexes)
            .await
            .unwrap_or_else(|err| panic!("authority journal commit failed: {err}"));
        let intents = std::mem::take(&mut proposal.intents);
        let ack = self.authority.publish(*proposal);
        assert_eq!(
            position,
            self.authority.revision(),
            "journal/authority skew"
        );
        Ok((ack, intents))
    }
}

fn now_ms() -> u64 {
    u64::try_from(Timestamp::now().as_millisecond()).expect("clock before the unix epoch")
}

/// How long until the wall-clock instant `fire_at_ms`; zero once it passed.
fn until(fire_at_ms: u64) -> Duration {
    Duration::from_millis(fire_at_ms.saturating_sub(now_ms()))
}

fn decode_receipt(receipt: &authority::Receipt) -> types::JobResult {
    match &receipt.outcome {
        authority::Outcome::Completed(terminal) => decode_terminal(&terminal.value),
        authority::Outcome::Faulted(fault) => failure_envelope(&fault.failure),
    }
}

fn decode_terminal(value: &str) -> types::JobResult {
    serde_json::from_str(value).expect(ERROR_JSON)
}

/// The terminal envelope a classified failure settles with: a `Terminal` error
/// whose JSON is the [`JobFailure`], so awaiters and the result index see one
/// shape whether the job succeeded, failed on its own, or ran out of a budget.
fn failure_envelope(failure: &JobFailure) -> types::JobResult {
    Err(types::JobError::Terminal(
        serde_json::to_string(failure).expect(ERROR_JSON),
    ))
}

/// Deterministic per-key retry spread, so a fleet of simultaneous failures does
/// not re-run in lockstep yet a given key always jitters the same amount (tests
/// stay deterministic and recovery never recomputes). Zero closes the window.
fn retry_jitter_ms(key: &str) -> u64 {
    if RETRY_JITTER_WINDOW_MS == 0 {
        return 0;
    }
    let mut hasher = DefaultHasher::new();
    hasher.write(key.as_bytes());
    hasher.finish() % RETRY_JITTER_WINDOW_MS
}
