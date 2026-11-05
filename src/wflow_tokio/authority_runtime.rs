//! Local workflow sessions over the partition commit authority.
//!
//! One actor owns the journal writer and pure authority. Commands follow
//! `prepare → append → publish`; only durable intents may affect a session.
//! Host creation runs in actor-tracked tasks and returns tagged by the full lease,
//! while each session awaiting its next yield remains abortable by key and lease.
//! Late creation/yield completion is fenced against current authority state.
//!
//! Cancellation first commits and indexes a permanent fault receipt, then aborts
//! or drops process-local activation state, clears deadlines, forgets host state,
//! and wakes result waiters. Unexpected current-lease activation loss is durably
//! revoked and re-granted within the submission's restart budget.
//!
//! Restart loads the latest published snapshot and replays the uncompacted suffix.
//! Recovery clears process-local leases, then re-grants every active job so its
//! invocation replays committed values without rerunning their closures.
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

use futures::FutureExt;
use futures::StreamExt;
use futures::future::AbortHandle;
use futures::future::Abortable;
use futures::future::BoxFuture;
use futures::future::Shared;
use futures::stream::FuturesUnordered;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use wflow_core::kvstore::atomic::Mutation;
use wflow_core::kvstore::atomic::Writer;
use wflow_core::kvstore::journal::CompactionProgress;
use wflow_core::kvstore::journal::Journal;
use wflow_core::kvstore::journal::JournalReader;
use wflow_core::partition::JobFailure;
use wflow_core::partition::RetryBudget;
use wflow_core::partition::authority;
use wflow_wdk::session::ReplayStep;
use wflow_wdk::session::SessionYield;
use wflow_wdk::session::WaitYield;
use wflow_wdk::types;
use wflow_wdk::types::RetryPolicy;

use crate::interlude::*;

pub mod session;

#[cfg(test)]
mod tests;

/// The only worker of this single-process slice; leases still carry it so the
/// authority's fencing rules are exercised unchanged.
const LOCAL_WORKER: u64 = 1;
const COMMAND_QUEUE: usize = 64;
const RECOVERY_PAGE: NonZeroUsize = NonZeroUsize::new(512).unwrap();
const DEFAULT_COMPACTION_PAGE: NonZeroUsize = NonZeroUsize::new(512).unwrap();
const SNAPSHOT_TRANSITIONS: u64 = 100;
const SNAPSHOT_INTERVAL: Duration = Duration::from_secs(60);
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
}

impl Submission {
    /// A job with the canonical finite retry and host-restart policy.
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
            policy: RetryPolicy::CappedExponential {
                base_ms: 100,
                cap_ms: 30_000,
            },
            budget: RetryBudget {
                max_retries: Some(8),
                max_restarts: Some(3),
            },
        }
    }

    /// How transient failures of this job are rescheduled.
    pub fn policy(mut self, policy: RetryPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Bounds on retry and restart loops; `None` explicitly opts into no bound.
    pub fn budget(mut self, budget: RetryBudget) -> Self {
        self.budget = budget;
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
    /// `None` while the job is unsettled.
    pub async fn get_result(&self) -> Res<Option<types::JobResult>> {
        Ok(self.receipt().await?.as_ref().map(decode_receipt))
    }

    /// Reads the durable compact terminal receipt of this job from the result
    /// index; `None` while the job is unsettled.
    pub async fn receipt(&self) -> Res<Option<authority::Receipt>> {
        read_receipt(&self.reader, &self.key).await
    }

    /// Resolves once this job has a durable terminal receipt; `None` if unknown.
    /// Registration is performed by the authority actor before the durable
    /// receipt check, so settlement cannot race this waiter.
    pub async fn await_receipt(&self) -> Option<authority::Receipt> {
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
            WatchState::Settled => self.receipt().await.unwrap(),
            WatchState::Active { wakeup } => {
                if let Some(receipt) = self.receipt().await.unwrap() {
                    return Some(receipt);
                }
                wakeup.await.ok()?;
                self.receipt().await.unwrap()
            }
        }
    }

    /// Resolves once this job has a durable terminal outcome; `None` if unknown.
    pub async fn await_result(&self) -> Option<types::JobResult> {
        self.await_receipt().await.as_ref().map(decode_receipt)
    }
}

/// Runtime persistence maintenance settings.
#[derive(Debug, Clone, Copy)]
pub struct AuthorityRuntimeConfig {
    /// Maximum number of journal entries reclaimed by one maintenance turn.
    pub compaction_page_size: NonZeroUsize,
}

impl Default for AuthorityRuntimeConfig {
    fn default() -> Self {
        Self {
            compaction_page_size: DEFAULT_COMPACTION_PAGE,
        }
    }
}

type ActorCompletion = Shared<BoxFuture<'static, Result<(), Arc<tokio::task::JoinError>>>>;

pub struct AuthorityRuntime {
    commands: mpsc::Sender<Command>,
    reader: JournalReader,
    /// Every stop caller polls the same shutdown operation, including when an
    /// earlier caller is cancelled. No caller can take and lose the actor join.
    shutdown: ActorCompletion,
    #[cfg(test)]
    actor: ActorCompletion,
}

enum WatchState {
    Active { wakeup: oneshot::Receiver<()> },
    Settled,
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
    Cancel {
        key: String,
        reason: String,
        reply: oneshot::Sender<Result<authority::Ack, CancelError>>,
    },
    Shutdown,
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
        Self::start_with_config(
            writer,
            partition_incarnation,
            host,
            AuthorityRuntimeConfig::default(),
        )
        .await
    }

    /// Opens a runtime with bounded journal reclamation configured by the caller.
    pub async fn start_with_config<W: Writer + 'static>(
        writer: W,
        partition_incarnation: u64,
        host: Arc<dyn session::Host>,
        config: AuthorityRuntimeConfig,
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
        for (key, job) in authority.jobs() {
            let invocation: Invocation = serde_json::from_str(&job.args).expect(ERROR_JSON);
            if !host.contains_wflow(&invocation.wflow_key) {
                eyre::bail!(
                    "workflow {:?} required by recovered job {key:?} is not registered",
                    invocation.wflow_key
                );
            }
        }
        let dirty_transitions = journal.frontier().saturating_sub(journal.snapshot_cut());
        let dirty_since = (dirty_transitions > 0).then(tokio::time::Instant::now);
        let reclamation_through = (journal.compacted_through() < journal.snapshot_cut())
            .then_some(journal.snapshot_cut());
        let (commands, commands_rx) = mpsc::channel(COMMAND_QUEUE);
        let actor = Actor {
            journal,
            authority,
            config,
            dirty_transitions,
            dirty_since,
            maintenance_ready: reclamation_through.is_some()
                || dirty_transitions >= SNAPSHOT_TRANSITIONS,
            reclamation_through,

            host,
            sessions: FuturesUnordered::new(),
            running: HashMap::new(),
            waiting: HashMap::new(),
            deadlines: BTreeSet::new(),
            waiters: HashMap::new(),
            pending_spawns: FuturesUnordered::new(),
        };
        let actor = tokio::spawn(actor.run(commands_rx))
            .map(|result| result.map_err(Arc::new))
            .boxed()
            .shared();
        let shutdown = {
            let commands = commands.clone();
            let actor = actor.clone();
            async move {
                // A closed mailbox means the actor already exited. Its join,
                // not the send result, is the authoritative failure report.
                let _already_exited = commands.send(Command::Shutdown).await;
                actor.await
            }
            .boxed()
            .shared()
        };
        Ok(Self {
            commands,
            reader,
            shutdown,
            #[cfg(test)]
            actor,
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
    /// was never submitted.
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

    /// Durably cancels `key`. The permanent cancellation receipt is committed
    /// before the actor aborts or drops any running/parked session, and every
    /// late completion is fenced by the removed lease.
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

    /// Reads the durable compact terminal receipt of `key` from the result
    /// index; `None` while the job has no committed outcome.
    pub async fn receipt(&self, key: &str) -> Res<Option<authority::Receipt>> {
        read_receipt(&self.reader, key).await
    }

    /// Reads the durable terminal outcome of `key` from the result index;
    /// `None` while the job has no committed outcome.
    pub async fn get_result(&self, key: &str) -> Res<Option<types::JobResult>> {
        Ok(self.receipt(key).await?.as_ref().map(decode_receipt))
    }

    /// Alias for [`Self::get_result`].
    pub async fn result(&self, key: &str) -> Res<Option<types::JobResult>> {
        self.get_result(key).await
    }

    /// Stops the actor through an explicit command, forcing its final snapshot
    /// before the shared runtime owner is released. Panics if the actor failed.
    pub async fn stop(&self) {
        self.shutdown.clone().await.expect(ERROR_ACTOR);
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

enum SpawnOutcome {
    Session(Box<dyn session::Session>),
    Failed,
}

/// Host creation completes off-actor and carries the complete lease so every
/// completion can be fenced against the durable authority state.
struct Spawned {
    key: String,
    lease: authority::Lease,
    outcome: SpawnOutcome,
}

/// What an armed deadline belongs to. Every kind is durable in the authority,
/// so the wheel is rebuilt from the journal/snapshot on recovery.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum DeadlineRef {
    Timer { key: String, step: u64 },
    Retry { key: String },
}

impl DeadlineRef {
    fn belongs_to(&self, candidate: &str) -> bool {
        match self {
            Self::Timer { key, .. } | Self::Retry { key } => key == candidate,
        }
    }
}

struct Actor<W> {
    sessions: FuturesUnordered<BoxFuture<'static, Result<Yielded, futures::future::Aborted>>>,
    /// Abort handles keep a session addressable by its full activation identity
    /// while `next_yield` is pending.
    running: HashMap<String, (authority::Lease, AbortHandle)>,
    /// Sessions parked on a durable wait, with the lease they run under.
    waiting: HashMap<String, (authority::Lease, Box<dyn session::Session>)>,
    host: Arc<dyn session::Host>,
    journal: Journal<W>,
    authority: authority::Authority,
    config: AuthorityRuntimeConfig,
    dirty_transitions: u64,
    dirty_since: Option<tokio::time::Instant>,
    maintenance_ready: bool,
    reclamation_through: Option<u64>,
    /// Armed deadlines as `(fire_at_ms, what)`, earliest first.
    deadlines: BTreeSet<(u64, DeadlineRef)>,
    /// Completion wakeup hints for in-flight `await_result` callers.
    waiters: HashMap<String, Vec<oneshot::Sender<()>>>,
    /// Join handles fence host creation to the actor lifecycle. In particular,
    /// shutdown aborts and joins these before acknowledging completion.
    pending_spawns: FuturesUnordered<tokio::task::JoinHandle<Spawned>>,
}

impl<W: Writer> Actor<W> {
    async fn run(mut self, mut commands: mpsc::Receiver<Command>) {
        self.arm_recovered_deadlines();
        // Deadlines that came due while the runtime was down resolve before the
        // re-grant, so replay observes the already committed timer/retry result.
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
            let maintenance_deadline = self.dirty_since.map(|since| since + SNAPSHOT_INTERVAL);
            tokio::select! {
                command = commands.recv() => match command {
                    Some(Command::Shutdown) => {
                        self.stop_pending_spawns().await;
                        self.final_snapshot().await;
                        break;
                    }
                    Some(command) => self.handle_command(command).await,
                    None => {
                        self.stop_pending_spawns().await;
                        self.final_snapshot().await;
                        break;
                    }
                },
                Some(spawned) = self.pending_spawns.next(), if !self.pending_spawns.is_empty() => {
                    match spawned {
                        Ok(spawned) => self.handle_spawned(spawned).await,
                        Err(error) if error.is_cancelled() => {
                            panic!("host spawn task was cancelled");
                        }
                        Err(error) => {
                            self.stop_pending_spawns().await;
                            std::panic::resume_unwind(error.into_panic());
                        }
                    }
                }
                Some(yielded) = self.sessions.next(), if !self.sessions.is_empty() => {
                    if let Ok(yielded) = yielded {
                        self.handle_yield(yielded).await;
                    }
                }
                () = tokio::time::sleep(until(next_deadline.unwrap_or_default())),
                    if next_deadline.is_some() => self.fire_due().await,
                () = tokio::task::yield_now(), if self.maintenance_ready => {
                    self.maintain().await;
                }
                () = tokio::time::sleep_until(maintenance_deadline.unwrap_or_else(tokio::time::Instant::now)),
                    if maintenance_deadline.is_some() => self.maintain().await,
            }
        }
    }

    async fn final_snapshot(&mut self) {
        if self.authority.revision() <= self.journal.snapshot_cut() {
            return;
        }
        let cut = self.authority.revision();
        let snapshot = serde_json::to_vec(&self.authority.snapshot())
            .expect(ERROR_JSON)
            .into();
        self.journal
            .save_snapshot(cut, snapshot)
            .await
            .unwrap_or_else(|error| panic!("final authority snapshot failed: {error}"));
    }

    /// Performs one snapshot or one reclamation page. Keeping this to one journal
    /// transaction lets the actor return to command handling between pages.
    async fn maintain(&mut self) {
        if self.reclamation_through.is_none() {
            let cut = self.authority.revision();
            if cut > self.journal.snapshot_cut() {
                let snapshot = serde_json::to_vec(&self.authority.snapshot())
                    .expect(ERROR_JSON)
                    .into();
                self.journal
                    .save_snapshot(cut, snapshot)
                    .await
                    .unwrap_or_else(|error| panic!("authority snapshot failed: {error}"));
                self.dirty_transitions = 0;
                self.dirty_since = None;
                self.reclamation_through = Some(cut);
            } else {
                self.dirty_transitions = 0;
                self.dirty_since = None;
            }
        }
        if let Some(through) = self.reclamation_through {
            match self
                .journal
                .compact_page(through, self.config.compaction_page_size)
                .await
                .unwrap_or_else(|error| panic!("journal reclamation failed: {error}"))
            {
                CompactionProgress::Pending { .. } => {}
                CompactionProgress::Complete { .. } => self.reclamation_through = None,
            }
        }
        self.maintenance_ready =
            self.reclamation_through.is_some() || self.dirty_transitions >= SNAPSHOT_TRANSITIONS;
    }

    async fn handle_command(&mut self, command: Command) {
        match command {
            Command::Submit { submission, reply } => self.submit(submission, reply).await,
            Command::WatchResult { key, reply } => {
                let state = if self.authority.jobs().contains_key(&key) {
                    let (tx, wakeup) = oneshot::channel();
                    self.waiters.entry(key).or_default().push(tx);
                    WatchState::Active { wakeup }
                } else if self.authority.receipts().contains_key(&key) {
                    WatchState::Settled
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
                        self.wake(intents).await;
                        Ok(ack)
                    }
                    Err(authority::Rejection::UnknownJob) => Err(SendError::UnknownJob),
                    Err(authority::Rejection::AlreadyTerminal) => Err(SendError::AlreadyTerminal),
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
                let (ack, intents) = match self.commit(command).await {
                    Ok(committed) => committed,
                    Err(authority::Rejection::AlreadyTerminal) => {
                        let _cancelled = reply.send(Err(CancelError::AlreadyTerminal));
                        return;
                    }
                    Err(rejection) => panic!("cancel of {key} rejected: {rejection:?}"),
                };
                // The permanent fault receipt and result index are committed
                // before process-local activation state is torn down.
                self.settle_intents(intents);
                let _cancelled = reply.send(Ok(ack));
            }
            Command::Shutdown => unreachable!("shutdown is handled by the actor loop"),
        }
    }

    /// Rebuilds the deadline wheel from durable timer waits and retries.
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
    }

    /// Fires every armed deadline whose instant has passed. Durable state is
    /// checked again, so a cancelled or superseded deadline is harmless.
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
                    self.wake(intents).await;
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
            }
        }
    }

    /// Delivers resolved wait steps to sessions parked on them. Failure after
    /// `WaitResolved` is activation loss: revoke and replay the durable step.
    async fn wake(&mut self, intents: Vec<authority::Intent>) {
        for intent in intents {
            let authority::Intent::Continue { key, step } = intent else {
                panic!("wait resolution produced a non-continue intent: {intent:?}");
            };
            if let Some((lease, session)) = self.waiting.remove(&key) {
                self.deliver(key, lease, session, &step.value).await;
            }
        }
    }

    async fn deliver(
        &mut self,
        key: String,
        lease: authority::Lease,
        session: Box<dyn session::Session>,
        value: &str,
    ) {
        if session.deliver(Arc::from(value)) {
            self.park(key, lease, session);
        } else {
            drop(session);
            self.recover_activation_loss(key, lease).await;
        }
    }

    fn settle_intents(&mut self, intents: Vec<authority::Intent>) {
        for intent in intents {
            match intent {
                authority::Intent::Terminal { key, .. } | authority::Intent::Fault { key, .. } => {
                    self.settled(key);
                }
                intent => panic!("settlement produced an unexpected intent: {intent:?}"),
            }
        }
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
        };
        let ack = match self.commit(command).await {
            Ok((ack, _)) => ack,
            Err(authority::Rejection::PayloadConflict) => {
                let _cancelled = reply.send(Err(SubmitError::PayloadConflict));
                return;
            }
            Err(rejection) => panic!("submission of {key} rejected: {rejection:?}"),
        };
        // Admission is durable before host creation begins, and the caller need
        // not wait for potentially slow instantiation.
        let _cancelled = reply.send(Ok(ack));
        if ack.disposition == authority::Disposition::Admitted {
            let incarnation = self.authority.jobs()[&key].incarnation;
            self.grant(key, incarnation).await;
        }
    }

    /// Grants a lease and starts (or restarts) its invocation. Host creation is
    /// launched off-actor; this method never polls the host future.
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
                    self.execute(key, lease, replay);
                }
                authority::Intent::Fault { key, .. } => self.settled(key),
                intent => panic!("grant produced an unexpected intent: {intent:?}"),
            }
        }
    }

    fn execute(&mut self, key: String, lease: authority::Lease, replay: Vec<authority::Step>) {
        let job = &self.authority.jobs()[&key];
        let invocation: Invocation = serde_json::from_str(&job.args).expect(ERROR_JSON);
        let replay = replay
            .into_iter()
            .map(|step| ReplayStep {
                kind: Some(step.kind),
                // The authority journal is the only replay source that pins both
                // the boundary kind and optional name.
                name: Some(step.name.map(Arc::from)),
                value_json: Some(step.value.into()),
            })
            .collect();
        let host = Arc::clone(&self.host);
        self.pending_spawns.push(tokio::spawn(async move {
            let outcome = match host
                .spawn(
                    Arc::from(key.as_str()),
                    &invocation.wflow_key,
                    invocation.args_json,
                    replay,
                )
                .await
            {
                Ok(session) => SpawnOutcome::Session(session),
                Err(_) => SpawnOutcome::Failed,
            };
            Spawned {
                key,
                lease,
                outcome,
            }
        }));
    }

    async fn stop_pending_spawns(&mut self) {
        for spawn in self.pending_spawns.iter_mut() {
            spawn.abort();
        }
        while let Some(spawned) = self.pending_spawns.next().await {
            if let Err(error) = spawned {
                if error.is_cancelled() {
                    continue;
                }
                std::panic::resume_unwind(error.into_panic());
            }
        }
    }

    async fn handle_spawned(&mut self, spawned: Spawned) {
        let Spawned {
            key,
            lease,
            outcome,
        } = spawned;
        if !self.is_current(&key, &lease) {
            // WFR-012: late creation is fenced and any produced session is dropped
            // immediately; spawn futures need not be cancellation-safe.
            drop(outcome);
            return;
        }
        let invocation: Invocation =
            serde_json::from_str(&self.authority.jobs()[&key].args).expect(ERROR_JSON);
        assert!(
            self.host.contains_wflow(&invocation.wflow_key),
            "workflow {:?} disappeared after admission for job {key:?}",
            invocation.wflow_key
        );
        match outcome {
            SpawnOutcome::Session(session) => self.park(key, lease, session),
            SpawnOutcome::Failed => self.recover_activation_loss(key, lease).await,
        }
    }

    fn is_current(&self, key: &str, lease: &authority::Lease) -> bool {
        self.authority
            .jobs()
            .get(key)
            .is_some_and(|job| job.lease.as_ref() == Some(lease))
    }

    /// Finalizes settlement only after the compact receipt and result index are
    /// durable: aborts running work, drops parked work, clears runtime deadlines,
    /// forgets host state, and wakes result waiters.
    fn settled(&mut self, key: String) {
        if let Some((_, abort)) = self.running.remove(&key) {
            abort.abort();
        }
        self.waiting.remove(&key);
        self.deadlines
            .retain(|(_, reference)| !reference.belongs_to(&key));
        self.host.forget_job(&key);
        if let Some(waiters) = self.waiters.remove(&key) {
            for waiter in waiters {
                let _cancelled = waiter.send(());
            }
        }
    }

    fn park(
        &mut self,
        key: String,
        lease: authority::Lease,
        mut session: Box<dyn session::Session>,
    ) {
        let (abort, registration) = AbortHandle::new_pair();
        let previous = self.running.insert(key.clone(), (lease.clone(), abort));
        assert!(previous.is_none(), "job already has a running activation");
        self.sessions.push(
            Abortable::new(
                async move {
                    let yielded = session.next_yield().await;
                    Yielded {
                        key,
                        lease,
                        session,
                        yielded,
                    }
                },
                registration,
            )
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
        if self
            .running
            .get(&key)
            .is_some_and(|(running_lease, _)| running_lease == &lease)
        {
            self.running.remove(&key);
        }
        if !self.is_current(&key, &lease) {
            return;
        }
        let Some(yielded) = yielded else {
            drop(session);
            self.recover_activation_loss(key, lease).await;
            return;
        };
        match yielded {
            SessionYield::PersistStep {
                step_id,
                value_json,
                meta,
                ..
            } => {
                let kind = meta.kind;
                let command = authority::Command::CommitStep {
                    key: key.clone(),
                    lease: lease.clone(),
                    step: step_id,
                    attempt: 0,
                    value: value_json.to_string(),
                    kind,
                    name: meta.name,
                };
                let intents = match self.commit(command).await {
                    Ok((_, intents)) => intents,
                    Err(authority::Rejection::StaleLease) => return,
                    Err(authority::Rejection::Nondeterministic) => {
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
                if session.resume() {
                    self.park(key, lease, session);
                } else {
                    drop(session);
                    self.recover_activation_loss(key, lease).await;
                }
            }
            SessionYield::WaitStep {
                step_id,
                deets,
                meta,
                ..
            } => {
                let kind = match deets {
                    WaitYield::Timer { fire_at, .. } => authority::WaitKind::Timer {
                        fire_at_ms: u64::try_from(fire_at.as_millisecond())
                            .expect("timer deadline before the unix epoch"),
                    },
                    WaitYield::Message { .. } => authority::WaitKind::Message,
                };
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
            SessionYield::Complete(Err(types::JobError::Transient(err))) => {
                self.retry_transient(key, lease, err).await;
            }
            SessionYield::Complete(result) => self.complete(key, lease, &result).await,
            SessionYield::Panicked(msg) => {
                self.complete_failure(key, lease, JobFailure::Panicked(msg))
                    .await;
            }
        }
    }

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
            Ok((_, intents)) => {
                assert!(intents.is_empty(), "retry produced unexpected intents");
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

    /// Recovers unexpected loss only when `lease` is still current. Revocation
    /// is durable before runtime state is torn down, then a fresh grant replays
    /// committed progress and charges the restart budget exactly once.
    async fn recover_activation_loss(&mut self, key: String, lease: authority::Lease) {
        if !self.is_current(&key, &lease) {
            return;
        }
        let incarnation = lease.job_incarnation;
        let intents = match self
            .commit(authority::Command::Revoke {
                key: key.clone(),
                lease: lease.clone(),
            })
            .await
        {
            Ok((_, intents)) => intents,
            Err(authority::Rejection::StaleLease | authority::Rejection::UnknownJob) => return,
            Err(rejection) => panic!("activation loss revoke rejected: {rejection:?}"),
        };
        let [
            authority::Intent::Revoke {
                key: revoked_key,
                lease: revoked_lease,
            },
        ] = intents.as_slice()
        else {
            panic!("revoke produced unexpected intents: {intents:?}");
        };
        assert_eq!(revoked_key, &key);
        assert_eq!(revoked_lease, &lease);
        if self
            .running
            .get(&key)
            .is_some_and(|(running_lease, _)| running_lease == &lease)
            && let Some((_, abort)) = self.running.remove(&key)
        {
            abort.abort();
        }
        if self
            .waiting
            .get(&key)
            .is_some_and(|(waiting_lease, _)| waiting_lease == &lease)
        {
            self.waiting.remove(&key);
        }
        self.grant(key, incarnation).await;
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
                self.deliver(key, lease, session, &value).await;
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
        };
        let intents = match self.commit(command).await {
            Ok((_, intents)) => intents,
            Err(authority::Rejection::StaleLease) => return,
            Err(rejection) => panic!("completion rejected: {rejection:?}"),
        };
        self.settle_intents(intents);
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
    /// transition (and, for settlements, its result-index mutation) is
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
            _ => Vec::new(),
        };
        let position = self
            .journal
            .append(self.authority.revision(), entry, indexes)
            .await
            .unwrap_or_else(|err| panic!("authority journal commit failed: {err}"));
        let intents = std::mem::take(&mut proposal.intents);
        let ack = self.authority.publish(*proposal);
        self.dirty_transitions = self.dirty_transitions.saturating_add(1);
        if self.dirty_since.is_none() {
            self.dirty_since = Some(tokio::time::Instant::now());
        }
        if self.dirty_transitions >= SNAPSHOT_TRANSITIONS {
            self.maintenance_ready = true;
        }
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
