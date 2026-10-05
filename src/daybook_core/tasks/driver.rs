//! Native pool scheduling and owned asynchronous domain execution.
//!
//! Domains resolve captured inputs and enforce effect/time-window policy at
//! classification and again at actual start. Scheduling never resolves mutable
//! manifests/configuration or treats local unreadiness as global settlement.

use super::*;
use crate::interlude::*;
use std::{future::Future, pin::Pin};

pub type AdapterFuture<T> = Pin<Box<dyn Future<Output = Res<T>> + Send + 'static>>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptKey {
    pub pool: TaskPoolId,
    pub task_id: PoolTaskId,
    pub attempt_id: PoolAttemptId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AttemptRequest {
    pub key: AttemptKey,
    pub allocation: Option<AllocationId>,
    pub declaration: TaskDeclaration,
    pub declaration_digest: [u8; 32],
    pub invocation: ResolvedInvocation,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PreparedAttempt {
    pub request: AttemptRequest,
    pub capture_digest: [u8; 32],
    pub dispatch_id: String,
    pub workflow_partition: String,
    pub job_id: Option<String>,
    pub staging_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AttemptOutcome {
    Finished(TerminalFact),
    Failed(OpaqueReason),
}

pub type AttemptSubscription = AdapterFuture<AttemptOutcome>;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DomainReceipt {
    pub task_id: PoolTaskId,
    pub summary: TerminalSummary,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RecoveredAttempt {
    Prepared(PreparedAttempt),
    Running(PreparedAttempt),
    Finishing {
        attempt: PreparedAttempt,
        outcome: AttemptOutcome,
    },
}

/// All returned futures own their work. Dropping a subscription cancels only
/// observation; it does not imply cancellation/retirement of durable execution.
/// `start` reconciles the exact retained job idempotently on recovery. Failed
/// execution is local attempt failure, not a fabricated global Cancelled fact.
pub trait TaskExecutionAdapter: Send + Sync + 'static {
    fn classify(&self, ticket: TaskTicket) -> AdapterFuture<TaskClassification>;
    fn persist(&self, request: AttemptRequest) -> AdapterFuture<PreparedAttempt>;
    fn start(&self, attempt: PreparedAttempt) -> AdapterFuture<AttemptSubscription>;
    fn cancel(&self, key: AttemptKey, reason: OpaqueReason) -> AdapterFuture<()>;
    fn incorporate(
        &self,
        declaration: TaskDeclaration,
        summary: TerminalSummary,
        attempt: Option<AttemptKey>,
    ) -> AdapterFuture<DomainReceipt>;
    fn watch(&self, watch: ReadinessWatch) -> AdapterFuture<AdapterFuture<()>>;
    fn recover(&self, pool: TaskPoolId) -> AdapterFuture<Vec<RecoveredAttempt>>;
    fn is_proven_obsolete(&self, declaration: TaskDeclaration) -> AdapterFuture<bool>;
}

use super::rpc::*;
use super::store::TaskStore;
use super::transport::TaskSyncBackend;
use big_repo::keyhive_core::access::Access;
use big_repo::{BigEphemeralFilter, BigRepo};
use big_sync::HostPartStore;
use big_sync_core::PeerKey;
use big_sync_core::encrypted_register::{LaneState, RegisterKey};
use big_sync_core::revisioned_store::{RevisionRead, RevisionReadLimits};
use big_sync_core::rpc::{PartEvent, SubPartsRequest, SubscriptionTarget};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

/// Explicit local tuning, never a distributed lease or exactly-once guarantee.
#[derive(Clone, Copy, Debug)]
pub struct SchedulingTiming {
    pub heartbeat_interval: std::time::Duration,
    pub router_settle: std::time::Duration,
    pub router_takeover_after: std::time::Duration,
}

struct NativeIds;
impl CoordinationIds for NativeIds {
    fn next_claim_id(&mut self) -> PoolClaimId {
        PoolClaimId::new(rand::random())
    }
    fn next_allocation_id(&mut self) -> AllocationId {
        AllocationId::new(rand::random())
    }
    fn next_attempt_id(&mut self) -> PoolAttemptId {
        PoolAttemptId::random()
    }
}

/// Sequence is deliberately absent: the authenticated original's reserved
/// sequence is the sole writer fence, including gaps after interrupted writes.
#[derive(Serialize, Deserialize)]
struct ClaimPayload {
    schema: u16,
    pool: TaskPoolId,
    candidate: NodePubkey,
    incarnation: NodeIncarnationId,
    generation: u64,
    claim_id: PoolClaimId,
    scheduling_protocol: SchedulingProtocolVersion,
    observed: BTreeMap<NodePubkey, u64>,
}

impl ClaimPayload {
    fn from_claim(claim: RouterClaim) -> Self {
        Self {
            schema: ROUTER_SLOT_PAYLOAD_SCHEMA,
            pool: claim.pool,
            candidate: claim.candidate,
            incarnation: claim.incarnation,
            generation: claim.generation,
            claim_id: claim.claim_id,
            scheduling_protocol: claim.scheduling_protocol,
            observed: claim.observed,
        }
    }
    fn into_claim(self, writer: [u8; 32], writer_seq: u64, pool: &TaskPoolId) -> Res<RouterClaim> {
        eyre::ensure!(
            self.schema == ROUTER_SLOT_PAYLOAD_SCHEMA,
            "unsupported router slot schema"
        );
        eyre::ensure!(
            self.pool == *pool && self.candidate.to_bytes32() == writer,
            "router slot original author/pool mismatch"
        );
        Ok(RouterClaim {
            pool: self.pool,
            candidate: self.candidate,
            incarnation: self.incarnation,
            writer_seq,
            generation: self.generation,
            claim_id: self.claim_id,
            scheduling_protocol: self.scheduling_protocol,
            observed: self.observed,
        })
    }
}

#[derive(Serialize, Deserialize)]
struct Heartbeat {
    schema: u16,
    claim: RouterClaim,
    endpoint: [u8; 32],
}

async fn claims(tasks: &TaskStore, snapshot: &PoolDescriptorSnapshot) -> Res<Vec<RouterClaim>> {
    let Some(state) = tasks
        .register()
        .current(snapshot.descriptor.router_slot.as_bytes())
        .await?
    else {
        return Ok(Vec::new());
    };
    let mut result = Vec::with_capacity(state.lanes.len());
    for lane in state.lanes.values() {
        let LaneState::Current { representation } = lane else {
            eyre::bail!("equivocated router slot is not schedulable");
        };
        let original = tasks.register().open_original(representation).await?;
        let payload: ClaimPayload = serde_json::from_slice(&original.body)?;
        result.push(payload.into_claim(
            representation.original.writer,
            representation.original.writer_seq,
            &snapshot.descriptor.pool_id,
        )?);
    }
    Ok(result)
}

async fn admit(
    repo: &BigRepo,
    snapshot: &PoolDescriptorSnapshot,
    peer: Option<NodePubkey>,
) -> Res<()> {
    let sampled = repo
        .coordination_authority(
            snapshot.reference.document.clone(),
            snapshot.reference.authority_group,
        )
        .await?;
    let checked = repo
        .admit_coordination_access(&sampled, Access::Edit)
        .await?;
    if let Some(peer) = peer {
        eyre::ensure!(
            checked.edit_agents().contains(&peer.to_bytes32()),
            "peer lacks current pool Edit"
        );
    }
    Ok(())
}

struct PoolInner {
    snapshot: PoolDescriptorSnapshot,
    adapter: Arc<dyn TaskExecutionAdapter>,
    capabilities: CapabilitySummary,
    capacity: Capacity,
    timing: SchedulingTiming,
    tx: mpsc::Sender<Command>,
    cancel: CancellationToken,
    join: std::sync::Mutex<Option<tokio::task::JoinHandle<Res<()>>>>,
}

#[derive(Clone)]
pub struct PoolDriverHandle(Arc<PoolInner>);
impl PoolDriverHandle {
    pub async fn submit(&self, declaration: TaskDeclaration, input_witness: Vec<u8>) -> Res<()> {
        let (reply, received) = oneshot::channel();
        self.0
            .tx
            .send(Command::Submit {
                declaration,
                input_witness,
                reply,
            })
            .await
            .map_err(|_| eyre::eyre!("pool scheduling owner stopped"))?;
        received.await?
    }
    pub async fn stop(&self) -> Res<()> {
        self.0.cancel.cancel();
        let join = self.0.join.lock().expect(ERROR_MUTEX).take();
        if let Some(join) = join {
            join.await??;
        }
        Ok(())
    }
}

enum Command {
    Submit {
        declaration: TaskDeclaration,
        input_witness: Vec<u8>,
        reply: oneshot::Sender<Res<()>>,
    },
    Rpc {
        peer: PeerKey,
        endpoint: iroh::EndpointId,
        request: TaskCoordinationRpcMessage,
    },
}

enum Completion {
    Cancelled,
    Worker(WorkerEvent),
    Router(RouterEvent),
    OfferChecked {
        session: SessionKey,
        allocation_id: AllocationId,
        task_id: PoolTaskId,
        declaration: Box<TaskDeclaration>,
        ticket_version: [u8; 32],
        result: Res<bool>,
    },
    // Preparation alone carries both the requested and durable invocation.
    // One allocation here avoids inflating every other completion/future by 1 KiB.
    Prepared(Box<(AttemptRequest, Res<PreparedAttempt>)>),
    Outcome {
        key: AttemptKey,
        result: Res<AttemptOutcome>,
    },
    Started {
        key: AttemptKey,
        token: Option<super::worker::DispatchToken>,
        result: Res<AttemptSubscription>,
    },
    StartCancelled,
    ClaimStored {
        claim: RouterClaim,
        result: Res<Vec<RouterClaim>>,
    },
    SessionLost {
        session: SessionKey,
    },
    SessionOpened {
        session: SessionKey,
        result: Res<irpc::channel::mpsc::Receiver<RouterMessage>>,
    },
    SessionMessage {
        session: SessionKey,
        result: Res<Option<RouterMessage>>,
        stream: irpc::channel::mpsc::Receiver<RouterMessage>,
    },
    Ready {
        task: PoolTaskId,
        generation: u64,
        result: Res<()>,
    },
}

/// One real request consumer exists before sync registers its protocol handler.
/// The endpoint and identity map are crate-private native transport capabilities;
/// advertised endpoint bytes never establish an application identity.
pub(crate) struct NativeTaskDriver {
    repo: Arc<BigRepo>,
    transport: Arc<TaskSyncBackend>,
    endpoint: iroh::Endpoint,
    identities: big_repo::rpc::BigRepoRpcHandle,
    pools: tokio::sync::Mutex<HashMap<TaskPoolId, PoolDriverHandle>>,
    cancel: CancellationToken,
    join: std::sync::Mutex<Option<tokio::task::JoinHandle<Res<()>>>>,
}

impl NativeTaskDriver {
    pub(crate) fn boot(
        repo: Arc<BigRepo>,
        transport: Arc<TaskSyncBackend>,
        endpoint: iroh::Endpoint,
        identities: big_repo::rpc::BigRepoRpcHandle,
    ) -> (Arc<Self>, mpsc::Sender<AuthenticatedTaskRequest>) {
        let (tx, mut rx) = mpsc::channel(128);
        let owner = Arc::new(Self {
            repo,
            transport,
            endpoint,
            identities,
            pools: Default::default(),
            cancel: CancellationToken::new(),
            join: Default::default(),
        });
        let weak = Arc::downgrade(&owner);
        let cancel = owner.cancel.clone();
        let join = tokio::spawn(async move {
            loop {
                let incoming = tokio::select! {
                    _ = cancel.cancelled() => return Ok(()),
                    incoming = rx.recv() => incoming,
                };
                let Some(AuthenticatedTaskRequest {
                    peer,
                    endpoint,
                    request,
                }) = incoming
                else {
                    return Ok(());
                };
                let Some(owner) = weak.upgrade() else {
                    return Ok(());
                };
                let pool = match &request {
                    TaskCoordinationRpcMessage::RegisterExecutor(message) => &message.inner.pool,
                    TaskCoordinationRpcMessage::Report(message) => &message.inner.pool,
                };
                let handle = owner.pools.lock().await.get(pool).cloned();
                if let Some(handle) = handle {
                    if handle
                        .0
                        .tx
                        .send(Command::Rpc {
                            peer,
                            endpoint,
                            request,
                        })
                        .await
                        .is_err()
                        && !handle.0.cancel.is_cancelled()
                    {
                        panic!("live pool RPC consumer closed");
                    }
                } else {
                    reject_request(request, "pool has no scheduling owner").await;
                }
            }
        });
        *owner.join.lock().expect(ERROR_MUTEX) = Some(join);
        (owner, tx)
    }

    pub(crate) async fn attach(
        self: &Arc<Self>,
        snapshot: PoolDescriptorSnapshot,
        adapter: Arc<dyn TaskExecutionAdapter>,
        capabilities: CapabilitySummary,
        capacity: Capacity,
        timing: SchedulingTiming,
    ) -> Res<PoolDriverHandle> {
        eyre::ensure!(!self.cancel.is_cancelled(), "scheduling owner stopped");
        eyre::ensure!(
            !timing.heartbeat_interval.is_zero()
                && !timing.router_settle.is_zero()
                && timing.router_takeover_after >= timing.heartbeat_interval,
            "invalid scheduling timing"
        );
        eyre::ensure!(
            snapshot.descriptor.router_slot.as_bytes().len() != 32,
            "router logical slot collides with the task-id slot domain"
        );
        admit(&self.repo, &snapshot, None).await?;
        let mut pools = self.pools.lock().await;
        if let Some(existing) = pools.get(&snapshot.descriptor.pool_id) {
            eyre::ensure!(
                !existing.0.cancel.is_cancelled(),
                "stopped scheduling pool cannot be reattached to a live owner"
            );
            eyre::ensure!(
                existing.0.snapshot.reference == snapshot.reference
                    && existing.0.snapshot.descriptor == snapshot.descriptor
                    && Arc::ptr_eq(&existing.0.adapter, &adapter)
                    && existing.0.capabilities == capabilities
                    && existing.0.capacity == capacity
                    && existing.0.timing.heartbeat_interval == timing.heartbeat_interval
                    && existing.0.timing.router_settle == timing.router_settle
                    && existing.0.timing.router_takeover_after == timing.router_takeover_after,
                "live scheduling pool attachment changed"
            );
            return Ok(existing.clone());
        }
        let tasks = self
            .transport
            .get(&snapshot.descriptor.pool_id)
            .ok_or_else(|| eyre::eyre!("attach retained native pool before scheduling"))?;
        let node = NodePubkey::new(tasks.register().local_writer().await?);
        let incarnation = NodeIncarnationId::random();
        let protocol = SchedulingProtocolVersion(2);
        let router = RouterMachine::new(
            RouterConfig {
                pool: snapshot.descriptor.pool_id.clone(),
                node,
                incarnation,
                supported_protocols: vec![protocol],
            },
            NativeIds,
        );
        let worker = PoolWorkerMachine::new(
            WorkerConfig {
                pool: snapshot.descriptor.pool_id.clone(),
                node,
                incarnation,
                capabilities: capabilities.clone(),
                capacity,
                supported_protocols: vec![protocol],
            },
            NativeIds,
        );
        // Replaying current part members reconstructs scheduling working state;
        // this is not a document corpus scan or a retained mutation journal.
        let reader = self
            .transport
            .shared_store()
            .open_revision_reader(SubPartsRequest {
                lower_bound: 0,
                targets: [SubscriptionTarget::Part {
                    part_id: snapshot.descriptor.active_task_part.clone(),
                    cursor: 0,
                }]
                .into(),
            })
            .await?
            .map_err(|error| eyre::eyre!("task scheduling reader: {error:?}"))?;
        let ephemeral = self
            .repo
            .ephemeral()
            .subscribe(BigEphemeralFilter::new(
                snapshot.descriptor.router_heartbeat_topic,
            ))
            .await?;
        let recovered = adapter.recover(snapshot.descriptor.pool_id.clone()).await?;
        let checkpoint = tasks
            .register()
            .consumer_checkpoint(b"native-scheduling")
            .await?;
        let (tx, rx) = mpsc::channel(128);
        let cancel = CancellationToken::new();
        let handle = PoolDriverHandle(Arc::new(PoolInner {
            snapshot: snapshot.clone(),
            adapter: Arc::clone(&adapter),
            capabilities,
            capacity,
            timing,
            tx,
            cancel: cancel.clone(),
            join: Default::default(),
        }));
        let mut actor = PoolActor {
            owner: Arc::clone(self),
            snapshot,
            tasks,
            adapter,
            node,
            incarnation,
            timing,
            router,
            worker,
            reader,
            ephemeral,
            rx,
            cancel,
            jobs: JoinSet::new(),
            prepared: BTreeMap::new(),
            finishing: BTreeMap::new(),
            start_cancels: BTreeMap::new(),
            classifications: BTreeMap::new(),
            proofs: BTreeMap::new(),
            watches: BTreeMap::new(),
            sessions: BTreeMap::new(),
            client: None,
            route: None,
            connecting: None,
            session_cancel: CancellationToken::new(),
            live: BTreeMap::new(),
            watch_generations: BTreeMap::new(),
            watch_sequence: 0,
            checkpoint,
            worker_actions: Vec::new(),
            router_actions: Vec::new(),
        };
        if let Err(error) = actor.restore(recovered).await {
            actor.abort_jobs().await;
            return Err(error);
        }
        let join = tokio::spawn(async move {
            let cancel = actor.cancel.clone();
            let result = tokio::select! {
                _ = cancel.cancelled() => Ok(()),
                result = actor.run() => result,
            };
            actor.abort_jobs().await;
            if let Err(error) = &result {
                panic!("native pool scheduling failed: {error:?}");
            }
            result
        });
        *handle.0.join.lock().expect(ERROR_MUTEX) = Some(join);
        pools.insert(handle.0.snapshot.descriptor.pool_id.clone(), handle.clone());
        Ok(handle)
    }

    pub(crate) async fn stop(&self) -> Res<()> {
        self.cancel.cancel();
        let pools = self
            .pools
            .lock()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for pool in pools {
            pool.stop().await?;
        }
        let join = self.join.lock().expect(ERROR_MUTEX).take();
        if let Some(join) = join {
            join.await??;
        }
        Ok(())
    }
}

async fn reject_request(request: TaskCoordinationRpcMessage, reason: &str) {
    match request {
        TaskCoordinationRpcMessage::RegisterExecutor(message) => {
            drop(
                message
                    .tx
                    .send(RouterMessage::Rejected {
                        reason: DeclineReason::Unavailable,
                    })
                    .await,
            );
        }
        TaskCoordinationRpcMessage::Report(message) => {
            drop(message.tx.send(Err(reason.to_owned())).await);
        }
    }
}

struct PoolActor {
    owner: Arc<NativeTaskDriver>,
    snapshot: PoolDescriptorSnapshot,
    tasks: Arc<TaskStore>,
    adapter: Arc<dyn TaskExecutionAdapter>,
    node: NodePubkey,
    incarnation: NodeIncarnationId,
    timing: SchedulingTiming,
    router: RouterMachine<NativeIds>,
    worker: PoolWorkerMachine<NativeIds>,
    reader: Box<dyn big_sync::LocalPartRevisionReader>,
    ephemeral: big_repo::BigEphemeralSubscription,
    rx: mpsc::Receiver<Command>,
    cancel: CancellationToken,
    jobs: JoinSet<Completion>,
    prepared: BTreeMap<PoolAttemptId, PreparedAttempt>,
    start_cancels: BTreeMap<PoolAttemptId, CancellationToken>,
    finishing: BTreeMap<PoolAttemptId, TerminalFact>,
    session_cancel: CancellationToken,
    sessions: BTreeMap<SessionKey, SessionStream>,
    client: Option<SessionClient>,
    route: Option<(ClaimLivenessKey, iroh::EndpointId)>,
    connecting: Option<ClaimLivenessKey>,
    live: BTreeMap<ClaimLivenessKey, tokio::time::Instant>,
    classifications: BTreeMap<PoolTaskId, (ClassificationToken, CancellationToken)>,
    proofs: BTreeMap<PoolTaskId, (ObsolescenceToken, CancellationToken)>,
    watches: BTreeMap<PoolTaskId, CancellationToken>,
    watch_generations: BTreeMap<PoolTaskId, u64>,
    watch_sequence: u64,
    checkpoint: u64,
    worker_actions: Vec<WorkerAction>,
    router_actions: Vec<RouterAction>,
}

impl PoolActor {
    // A job can own the SQLite writer while a handler awaits it. Independent
    // scheduling prevents actor self-deadlock; joining aborted jobs fences their
    // transaction release on shutdown and on failed recovery attachment.
    async fn abort_jobs(&mut self) {
        self.jobs.abort_all();
        while let Some(joined) = self.jobs.join_next().await {
            match joined {
                Ok(_) => {}
                Err(error) if error.is_cancelled() => {}
                Err(error) => std::panic::resume_unwind(error.into_panic()),
            }
        }
    }
    fn worker_event(&mut self, event: WorkerEvent) {
        let terminal = match &event {
            WorkerEvent::TicketObserved { ticket } | WorkerEvent::OriginAttemptStart { ticket }
                if ticket.terminal_summary().is_some() =>
            {
                Some(ticket.task_id)
            }
            WorkerEvent::CancelReceived {
                session, task_id, ..
            } if self.worker.current_session() == Some(*session) => Some(*task_id),
            _ => None,
        };
        self.worker.on_event(event, &mut self.worker_actions);
        self.classifications.retain(|_, (token, cancel)| {
            let retain = self.worker.classification_pending(token);
            if !retain {
                cancel.cancel();
            }
            retain
        });
        if let Some(task) = terminal {
            if let Some(cancel) = self.watches.remove(&task) {
                cancel.cancel();
            }
            self.watch_generations.remove(&task);
        }
    }
    fn router_event(&mut self, event: RouterEvent) {
        self.router.on_event(event, &mut self.router_actions);
        self.proofs.retain(|_, (token, cancel)| {
            let retain = self.router.proof_pending(token);
            if !retain {
                cancel.cancel();
            }
            retain
        });
    }
    fn key(&self, task_id: PoolTaskId, attempt_id: PoolAttemptId) -> AttemptKey {
        AttemptKey {
            pool: self.snapshot.descriptor.pool_id.clone(),
            task_id,
            attempt_id,
        }
    }
    fn start_attempt(
        &mut self,
        attempt: PreparedAttempt,
        token: Option<super::worker::DispatchToken>,
    ) {
        let adapter = Arc::clone(&self.adapter);
        let key = attempt.request.key.clone();
        let cancel = CancellationToken::new();
        assert!(
            self.start_cancels
                .insert(key.attempt_id, cancel.clone())
                .is_none(),
            "duplicate in-flight start for one durable attempt"
        );
        self.jobs.spawn(async move {
            tokio::select! {
                _ = cancel.cancelled() => Completion::StartCancelled,
                result = adapter.start(attempt) => Completion::Started { key, token, result },
            }
        });
    }
    async fn restore(&mut self, recovered: Vec<RecoveredAttempt>) -> Res<()> {
        for recovered in recovered {
            let attempt = match &recovered {
                RecoveredAttempt::Prepared(attempt)
                | RecoveredAttempt::Running(attempt)
                | RecoveredAttempt::Finishing { attempt, .. } => attempt.clone(),
            };
            eyre::ensure!(
                attempt.request.key.pool == self.snapshot.descriptor.pool_id
                    && attempt.request.key.task_id == attempt.request.declaration.task_id
                    && attempt.request.declaration_digest
                        == attempt.request.declaration.canonical_digest(),
                "recovered attempt binding mismatch"
            );
            let key = attempt.request.key.clone();
            eyre::ensure!(
                self.prepared
                    .insert(key.attempt_id, attempt.clone())
                    .is_none(),
                "duplicate recovered attempt identity"
            );
            match recovered {
                RecoveredAttempt::Prepared(_) => {
                    self.worker.restore_prepared_attempt(
                        key.attempt_id,
                        Box::new(attempt.request.declaration.clone()),
                        attempt.request.invocation.clone(),
                    );
                    self.worker_event(WorkerEvent::AttemptPersisted {
                        task_id: key.task_id,
                        attempt_id: key.attempt_id,
                    });
                }
                RecoveredAttempt::Running(_) => {
                    self.worker.restore_running_attempt(
                        key.attempt_id,
                        Box::new(attempt.request.declaration.clone()),
                    );
                    self.start_attempt(attempt, None);
                }
                RecoveredAttempt::Finishing { outcome, .. } => {
                    self.worker.restore_running_attempt(
                        key.attempt_id,
                        Box::new(attempt.request.declaration.clone()),
                    );
                    self.finish(key, Ok(outcome))?;
                }
            }
        }
        Ok(())
    }

    fn finish(&mut self, key: AttemptKey, outcome: Res<AttemptOutcome>) -> Res<()> {
        if self
            .prepared
            .get(&key.attempt_id)
            .is_none_or(|attempt| attempt.request.key != key)
        {
            return Ok(());
        }
        match outcome {
            Ok(AttemptOutcome::Finished(fact)) => {
                if let TerminalFact::Succeeded { attempt_id, .. } = &fact {
                    eyre::ensure!(
                        *attempt_id == key.attempt_id,
                        "terminal outcome names another attempt"
                    );
                }
                self.finishing.insert(key.attempt_id, fact.clone());
                self.worker_event(WorkerEvent::AttemptFinished {
                    task_id: key.task_id,
                    attempt_id: key.attempt_id,
                    fact,
                });
            }
            Ok(AttemptOutcome::Failed(_)) | Err(_) => {
                self.worker_event(WorkerEvent::AttemptFailed {
                    task_id: key.task_id,
                    attempt_id: key.attempt_id,
                });
                self.prepared.remove(&key.attempt_id);
            }
        }
        Ok(())
    }

    async fn run(&mut self) -> Res<()> {
        let mut tick = tokio::time::interval(self.snapshot_timing().heartbeat_interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let takeover = tokio::time::Instant::now() + self.snapshot_timing().router_takeover_after;
        let mut next_takeover = takeover;
        loop {
            self.actions().await?;
            tokio::select! {
                _ = self.cancel.cancelled() => return Ok(()),
                command = self.rx.recv() => {
                    let Some(command) = command else { return Ok(()); };
                    match command {
                        Command::Submit { declaration, input_witness, reply } => {
                            let result = async {
                                admit(&self.owner.repo, &self.snapshot, None).await?;
                                self.tasks.submit(declaration, input_witness).await?;
                                eyre::Ok(())
                            }.await;
                            // A cancelled submit future may drop its reply receiver.
                            drop(reply.send(result));
                        }
                        Command::Rpc { peer, endpoint, request } => self.rpc(peer, endpoint, request).await.wrap_err("handling native scheduling RPC")?,
                    }
                }
                revision = self.reader.next(RevisionReadLimits::default()) => {
                    match revision.wrap_err("reading native scheduling revision")? {
                        RevisionRead::ReplayComplete { .. } => {}
                        RevisionRead::Entries { revision, entries } => {
                            for entry in entries { self.revision(entry).await.wrap_err("incorporating native scheduling revision")?; }
                            self.actions().await?;
                            if revision > self.checkpoint {
                                self.tasks.register().begin_consumer_settlement(b"native-scheduling", revision)
                                    .await.wrap_err("beginning native scheduling checkpoint")?
                                    .commit().await.wrap_err("committing native scheduling checkpoint")?;
                                self.checkpoint = revision;
                            }
                        }
                    }
                }
                event = self.ephemeral.recv() => {
                    let Some(event) = event else { eyre::bail!("native heartbeat subscription closed"); };
                    self.heartbeat(event).await.wrap_err("incorporating native scheduling heartbeat")?;
                }
                completion = self.jobs.join_next(), if !self.jobs.is_empty() => {
                    let completion = completion.expect(ERROR_IMPOSSIBLE).expect("native pool job panicked");
                    let completion_kind = std::mem::discriminant(&completion);
                    self.complete(completion).await
                        .wrap_err_with(|| format!("completing native scheduling job {completion_kind:?}"))?;
                }
                _ = tick.tick() => {
                    self.expire().await?;
                    self.router_event(RouterEvent::HeartbeatDue);
                    self.router_event(RouterEvent::RoutingTick);
                    if tokio::time::Instant::now() >= next_takeover {
                        if admit(&self.owner.repo, &self.snapshot, None).await.is_ok() {
                            self.router_event(RouterEvent::ConsiderTakeover);
                        }
                        next_takeover = tokio::time::Instant::now() + self.snapshot_timing().router_takeover_after;
                    }
                    self.reconnect().await?;
                }
            }
        }
    }
    fn snapshot_timing(&self) -> SchedulingTiming {
        // Actor configuration is retained by its handle; no mutable descriptor or
        // wall-clock conversion participates in the liveness decision.
        self.timing
    }

    async fn revision(&mut self, entry: PartEvent) -> Res<()> {
        let (object, removed) = match entry {
            PartEvent::Changed(entry) => (entry.obj_id, false),
            PartEvent::Removed(entry) => (entry.obj_id, true),
        };
        let logical = RegisterKey::decode(object.as_bytes())?;
        eyre::ensure!(
            logical.scope == self.snapshot.descriptor.register_scope,
            "foreign scheduling key scope"
        );
        if logical.slot == self.snapshot.descriptor.router_slot.as_bytes() {
            if !removed {
                let claims = claims(&self.tasks, &self.snapshot).await?;
                self.router_event(RouterEvent::SlotMerged { claims });
            }
            return Ok(());
        }
        let digest: [u8; 32] = logical
            .slot
            .try_into()
            .map_err(|_| eyre::eyre!("unknown register logical slot in task scheduling part"))?;
        let task_id = PoolTaskId::new(digest);
        if removed {
            self.router_event(RouterEvent::TaskRemoved { task_id });
        } else if let Some(ticket) = self.tasks.ticket(task_id).await? {
            // Durable remote incorporation precedes checkpoint acknowledgement.
            // The worker also propagates monotonic summary transitions through
            // its own exact finishing acknowledgement path.
            if let Some(summary) = ticket.terminal_summary() {
                let receipt = self
                    .adapter
                    .incorporate(
                        ticket.declaration.declaration.clone(),
                        summary.clone(),
                        None,
                    )
                    .await?;
                eyre::ensure!(
                    receipt.task_id == task_id && receipt.summary == summary,
                    "domain returned another terminal receipt"
                );
            }
            if ticket.terminal_summary().is_none()
                && ticket.declaration.declaration.producer == Some(self.node)
            {
                self.worker_event(WorkerEvent::OriginAttemptStart {
                    ticket: Box::new(ticket.clone()),
                });
            } else {
                self.worker_event(WorkerEvent::TicketObserved {
                    ticket: Box::new(ticket.clone()),
                });
            }
            self.router_event(RouterEvent::TaskObserved {
                ticket: Box::new(ticket),
            });
        }
        Ok(())
    }

    async fn heartbeat(&mut self, event: big_repo::BigEphemeralEvent) -> Res<()> {
        let Ok(heartbeat) = serde_json::from_slice::<Heartbeat>(&event.payload) else {
            return Ok(());
        };
        let claim = heartbeat.claim;
        if heartbeat.schema != 1
            || claim.pool != self.snapshot.descriptor.pool_id
            || event.sender.as_bytes() != &claim.candidate.to_bytes32()
        {
            return Ok(());
        }
        let Ok(endpoint) = iroh::EndpointId::from_bytes(&heartbeat.endpoint) else {
            return Ok(());
        };
        if self.owner.identities.peer_for_endpoint(endpoint)
            != Some(PeerKey::new(claim.candidate.to_bytes32()))
            || admit(&self.owner.repo, &self.snapshot, Some(claim.candidate))
                .await
                .is_err()
        {
            return Ok(());
        }
        let persisted = claims(&self.tasks, &self.snapshot).await?;
        if !persisted.iter().any(|persisted| persisted == &claim) {
            return Ok(());
        }
        let key = claim.liveness_key();
        self.live.insert(
            key,
            tokio::time::Instant::now() + self.snapshot_timing().router_takeover_after,
        );
        self.router_event(RouterEvent::SlotMerged { claims: persisted });
        self.router_event(RouterEvent::HeartbeatObserved { claim: key });
        if self
            .router
            .projected_winner()
            .is_some_and(|winner| winner.liveness_key() == key)
        {
            if self.route.is_some_and(|(old, _)| old != key)
                || (!self.router.is_routing()
                    && self.client.is_none()
                    && self.worker.current_session().is_some())
            {
                self.lose_route();
            }
            self.route = Some((key, endpoint));
            self.reconnect().await?;
        }
        Ok(())
    }

    async fn expire(&mut self) -> Res<()> {
        let now = tokio::time::Instant::now();
        let expired = self
            .live
            .iter()
            .filter(|(_, deadline)| **deadline <= now)
            .map(|(key, _)| *key)
            .collect::<Vec<_>>();
        for claim in expired {
            self.live.remove(&claim);
            self.router_event(RouterEvent::HeartbeatExpired { claim });
            if self.route.is_some_and(|(key, _)| key == claim) {
                self.lose_route();
            }
        }
        if let Some((_, endpoint)) = self.route
            && self.owner.identities.peer_for_endpoint(endpoint).is_none()
        {
            self.lose_route();
        }
        let sessions = self.sessions.keys().copied().collect::<Vec<_>>();
        for session in sessions {
            if self.sessions.get(&session).is_some_and(|stream| {
                self.owner.identities.peer_for_endpoint(stream.endpoint)
                    != Some(PeerKey::new(session.node.to_bytes32()))
            }) || admit(&self.owner.repo, &self.snapshot, Some(session.node))
                .await
                .is_err()
            {
                self.sessions.remove(&session);
                self.router_event(RouterEvent::SessionClosed { session });
            }
        }
        if !self.router.is_routing()
            && self.client.is_none()
            && self.worker.current_session().is_some()
        {
            self.lose_route();
        }
        Ok(())
    }
    fn lose_route(&mut self) {
        self.session_cancel.cancel();
        self.session_cancel = CancellationToken::new();
        if let Some(session) = self.worker.current_session() {
            self.worker_event(WorkerEvent::RouterLost { session });
        }
        self.client = None;
        self.connecting = None;
        self.route = None;
    }

    async fn reconnect(&mut self) -> Res<()> {
        if self.router.is_routing() {
            let Some(claim) = self.router.own_claim().cloned() else {
                return Ok(());
            };
            if self.worker.current_session().is_none() || self.client.is_some() {
                self.lose_route();
                let session = SessionKey {
                    node: self.node,
                    incarnation: self.incarnation,
                    session: RouterSessionId::random(),
                };
                self.worker_event(WorkerEvent::RouteDiscovered {
                    claim: Box::new(claim),
                    session,
                });
            }
            return Ok(());
        }
        let Some((key, endpoint)) = self.route else {
            return Ok(());
        };
        if self.client.is_some() || self.connecting == Some(key) {
            return Ok(());
        }
        let Some(claim) = self.router.projected_winner().cloned() else {
            return Ok(());
        };
        if claim.liveness_key() != key {
            return Ok(());
        }
        if self.owner.identities.peer_for_endpoint(endpoint)
            != Some(PeerKey::new(claim.candidate.to_bytes32()))
            || admit(&self.owner.repo, &self.snapshot, Some(claim.candidate))
                .await
                .is_err()
        {
            return Ok(());
        }
        let session = SessionKey {
            node: self.node,
            incarnation: self.incarnation,
            session: RouterSessionId::random(),
        };
        let client = irpc_iroh::client::<TaskCoordinationRpc>(
            self.owner.endpoint.clone(),
            iroh::EndpointAddr::new(endpoint),
            TASK_COORDINATION_ALPN,
        );
        let (reports, reports_rx) = mpsc::channel(128);
        self.client = Some(SessionClient {
            session,
            client,
            reports,
            reports_rx: Some(reports_rx),
        });
        self.connecting = Some(key);
        self.worker_event(WorkerEvent::RouteDiscovered {
            claim: Box::new(claim),
            session,
        });
        Ok(())
    }

    async fn rpc(
        &mut self,
        peer: PeerKey,
        endpoint: iroh::EndpointId,
        request: TaskCoordinationRpcMessage,
    ) -> Res<()> {
        let session = match &request {
            TaskCoordinationRpcMessage::RegisterExecutor(message) => message.inner.session,
            TaskCoordinationRpcMessage::Report(message) => message.inner.session,
        };
        if !self.router.is_routing()
            || peer != PeerKey::new(session.node.to_bytes32())
            || self.owner.identities.peer_for_endpoint(endpoint) != Some(peer.clone())
            || admit(&self.owner.repo, &self.snapshot, Some(session.node))
                .await
                .is_err()
        {
            reject_request(request, "router/session authority unavailable").await;
            return Ok(());
        }
        match request {
            TaskCoordinationRpcMessage::RegisterExecutor(message) => {
                let registration = message.inner.registration;
                if registration.node != session.node
                    || registration.incarnation != session.incarnation
                    || self.sessions.contains_key(&session)
                {
                    drop(
                        message
                            .tx
                            .send(RouterMessage::Rejected {
                                reason: DeclineReason::StaleSession,
                            })
                            .await,
                    );
                    return Ok(());
                }
                let stored = claims(&self.tasks, &self.snapshot).await?;
                let own = self
                    .router
                    .own_claim()
                    .expect(ERROR_IMPOSSIBLE)
                    .liveness_key();
                let claim = stored
                    .into_iter()
                    .find(|claim| claim.liveness_key() == own)
                    .expect("routing requires its persisted signed claim");
                let (sender, mut outgoing) = mpsc::channel(128);
                sender
                    .try_send(RouterMessage::Registered { claim })
                    .expect("new session queue has room for registration");
                let replaced = self
                    .sessions
                    .keys()
                    .filter(|old| old.node == session.node)
                    .copied()
                    .collect::<Vec<_>>();
                for old in replaced {
                    self.sessions.remove(&old);
                    self.router_event(RouterEvent::SessionClosed { session: old });
                }
                let stream = message.tx;
                let cancel = self.cancel.child_token();
                let writer_cancel = cancel.clone();
                let owner = Arc::clone(&self.owner);
                let snapshot = self.snapshot.clone();
                // A single owned wire writer preserves Registered/Offer/Start
                // order and cannot block election, expiry, or cancellation.
                self.jobs.spawn(async move {
                    loop {
                        let message = tokio::select! {
                            _ = writer_cancel.cancelled() => break,
                            _ = stream.closed() => break,
                            message = outgoing.recv() => match message {
                                Some(message) => message,
                                None => break,
                            },
                        };
                        if owner.identities.peer_for_endpoint(endpoint)
                            != Some(PeerKey::new(session.node.to_bytes32()))
                            || admit(&owner.repo, &snapshot, Some(session.node))
                                .await
                                .is_err()
                        {
                            break;
                        }
                        let result = tokio::select! {
                            _ = writer_cancel.cancelled() => break,
                            result = stream.send(message) => result,
                        };
                        if result.is_err() {
                            break;
                        }
                    }
                    Completion::Router(RouterEvent::SessionClosed { session })
                });
                self.sessions.insert(
                    session,
                    SessionStream {
                        endpoint,
                        sender,
                        cancel,
                    },
                );
                self.router_event(RouterEvent::RegisterExecutor {
                    session,
                    capabilities: registration.capabilities,
                    capacity: registration.capacity,
                    protocol: registration.protocol,
                    active_attempts: registration.active_attempts,
                });
            }
            TaskCoordinationRpcMessage::Report(message) => {
                if self
                    .sessions
                    .get(&session)
                    .is_none_or(|stream| stream.endpoint != endpoint)
                {
                    drop(
                        message
                            .tx
                            .send(Err("unknown executor session".into()))
                            .await,
                    );
                    return Ok(());
                }
                self.report_event(session, message.inner.report);
                drop(message.tx.send(Ok(())).await);
            }
        }
        Ok(())
    }
    fn report_event(&mut self, session: SessionKey, report: ExecutorReport) {
        let event = match report {
            ExecutorReport::Accepted {
                allocation_id,
                attempt_id,
            } => RouterEvent::OfferAccepted {
                session,
                allocation_id,
                attempt_id,
            },
            ExecutorReport::Declined {
                allocation_id,
                reason,
            } => RouterEvent::OfferDeclined {
                session,
                allocation_id,
                reason,
            },
            ExecutorReport::AttemptChanged {
                task_id,
                attempt_id,
                state,
            } => RouterEvent::AttemptChanged {
                session,
                task_id,
                attempt_id,
                state,
            },
            ExecutorReport::ReadinessWakeup { task_id } => {
                RouterEvent::ReadinessWakeup { session, task_id }
            }
            ExecutorReport::Closed => {
                self.sessions.remove(&session);
                RouterEvent::SessionClosed { session }
            }
        };
        self.router_event(event);
    }

    async fn send_report(&mut self, session: SessionKey, report: ExecutorReport) -> Res<()> {
        if self.router.is_routing() && self.client.is_none() {
            self.report_event(session, report);
        } else if let Some(client) = &self.client {
            if client.session != session {
                return Ok(());
            }
            if client.reports.try_send(report).is_err() {
                // A lost/full live session is reconstructed from durable tickets
                // and executor registration; queued reports are not settlement.
                self.lose_route();
            }
        }
        Ok(())
    }

    async fn send_router(&mut self, session: SessionKey, message: RouterMessage) -> Res<()> {
        if session.node == self.node && self.worker.current_session() == Some(session) {
            self.router_message(session, message).await?;
        } else if let Some(stream) = self.sessions.get(&session)
            && (self.owner.identities.peer_for_endpoint(stream.endpoint)
                != Some(PeerKey::new(session.node.to_bytes32()))
                || stream.sender.try_send(message).is_err())
        {
            self.sessions.remove(&session);
            self.router_event(RouterEvent::SessionClosed { session });
        }
        Ok(())
    }
    async fn router_message(&mut self, session: SessionKey, message: RouterMessage) -> Res<()> {
        if self.worker.current_session() != Some(session) {
            return Ok(());
        }
        if let Some((key, endpoint)) = self.route
            && (self.owner.identities.peer_for_endpoint(endpoint)
                != Some(PeerKey::new(key.candidate.to_bytes32()))
                || admit(&self.owner.repo, &self.snapshot, Some(key.candidate))
                    .await
                    .is_err())
        {
            self.lose_route();
            return Ok(());
        }
        match message {
            RouterMessage::Registered { claim } => {
                let persisted = claims(&self.tasks, &self.snapshot).await?;
                if !persisted.iter().any(|stored| stored == &claim)
                    || self
                        .route
                        .is_none_or(|(key, _)| key != claim.liveness_key())
                {
                    self.lose_route();
                    return Ok(());
                }
                self.connecting = None;
                let Some(client) = self.client.as_mut() else {
                    return Ok(());
                };
                let Some(mut reports) = client.reports_rx.take() else {
                    return Ok(());
                };
                let client = client.client.clone();
                let pool = self.snapshot.descriptor.pool_id.clone();
                let cancel = self.session_cancel.clone();
                // One owned writer preserves Accepted before Running/outcome.
                // A wire error or application rejection retires this session.
                self.jobs.spawn(async move {
                    loop {
                        let report = tokio::select! {
                            _ = cancel.cancelled() => break,
                            report = reports.recv() => match report {
                                Some(report) => report,
                                None => break,
                            },
                        };
                        let result = tokio::select! {
                            _ = cancel.cancelled() => break,
                            result = client.rpc(ExecutorReportRequest {
                                pool: pool.clone(), session, report,
                            }) => result,
                        };
                        if !matches!(result, Ok(Ok(()))) {
                            break;
                        }
                    }
                    Completion::SessionLost { session }
                });
            }
            RouterMessage::Offer {
                allocation_id,
                task_id,
                declaration,
            } => {
                let ticket = self.tasks.ticket(task_id).await?.map(Box::new);
                self.worker_event(WorkerEvent::OfferReceived {
                    session,
                    allocation_id,
                    task_id,
                    declaration,
                    ticket,
                });
            }
            RouterMessage::Start {
                allocation_id,
                task_id,
            } => self.worker_event(WorkerEvent::StartReceived {
                session,
                allocation_id,
                task_id,
            }),
            RouterMessage::Cancel { task_id, reason } => {
                self.worker_event(WorkerEvent::CancelReceived {
                    session,
                    task_id,
                    reason,
                })
            }
            RouterMessage::Rejected { .. } => self.lose_route(),
        }
        Ok(())
    }

    fn read_session(
        &mut self,
        session: SessionKey,
        mut stream: irpc::channel::mpsc::Receiver<RouterMessage>,
    ) {
        let cancel = self.session_cancel.clone();
        self.jobs.spawn(async move {
            let result = tokio::select! {
                _ = cancel.cancelled() => Ok(None),
                result = stream.recv() => result.map_err(Into::into),
            };
            Completion::SessionMessage {
                session,
                result,
                stream,
            }
        });
    }
    async fn complete(&mut self, completion: Completion) -> Res<()> {
        match completion {
            Completion::Cancelled => {}
            Completion::Worker(event) => self.worker_event(event),
            Completion::OfferChecked {
                session,
                allocation_id,
                task_id,
                declaration,
                ticket_version,
                result,
            } => {
                if self.router.offer_evidence(
                    session,
                    allocation_id,
                    task_id,
                    declaration.canonical_digest(),
                ) != Some(ticket_version)
                {
                    return Ok(());
                }
                if result? {
                    self.router_event(RouterEvent::TaskRemoved { task_id });
                } else if admit(&self.owner.repo, &self.snapshot, Some(session.node))
                    .await
                    .is_err()
                {
                    self.router_event(RouterEvent::SessionClosed { session });
                } else {
                    self.send_router(
                        session,
                        RouterMessage::Offer {
                            allocation_id,
                            task_id,
                            declaration,
                        },
                    )
                    .await?;
                }
            }
            Completion::Router(event) => {
                if let RouterEvent::SessionClosed { session } = &event {
                    self.sessions.remove(session);
                }
                self.router_event(event);
            }
            Completion::Prepared(completion) => {
                let (request, result) = *completion;
                match result {
                    Ok(prepared) => {
                        eyre::ensure!(
                            prepared.request.key == request.key
                                && prepared.request.declaration_digest
                                    == request.declaration_digest
                                && prepared.request.declaration == request.declaration
                                && prepared.request.invocation == request.invocation,
                            "durable preparation returned another invocation"
                        );
                        let key = request.key;
                        self.prepared.insert(key.attempt_id, prepared);
                        self.worker_event(WorkerEvent::AttemptPersisted {
                            task_id: key.task_id,
                            attempt_id: key.attempt_id,
                        });
                    }
                    Err(error) => self.worker_event(WorkerEvent::AttemptPersistFailed {
                        task_id: request.key.task_id,
                        attempt_id: request.key.attempt_id,
                        reason: OpaqueReason::from_label(error.to_string()),
                    }),
                }
            }
            Completion::Outcome { key, result } => self.finish(key, result)?,
            Completion::StartCancelled => {}
            Completion::Started { key, token, result } => {
                // Cancellation removes this owned start obligation; a buffered
                // old receipt cannot resurrect the reservation or report Running.
                if self.start_cancels.remove(&key.attempt_id).is_none()
                    || self
                        .prepared
                        .get(&key.attempt_id)
                        .is_none_or(|attempt| attempt.request.key != key)
                {
                    return Ok(());
                }
                match result {
                    Ok(subscription) => {
                        if let Some(token) = token {
                            self.worker_event(WorkerEvent::DispatchStarted { token });
                        }
                        self.jobs.spawn(async move {
                            Completion::Outcome {
                                key,
                                result: subscription.await,
                            }
                        });
                    }
                    Err(error) => self.finish(key, Err(error))?,
                }
            }
            Completion::ClaimStored { claim, result } => match result {
                Ok(claims) => {
                    let present = claims
                        .iter()
                        .any(|stored| stored.liveness_key() == claim.liveness_key());
                    self.router_event(RouterEvent::SlotMerged { claims });
                    if present {
                        self.router_event(RouterEvent::ClaimPersisted {
                            generation: claim.generation,
                            claim_id: claim.claim_id,
                        });
                    } else {
                        self.router_event(RouterEvent::ClaimPersistenceFailed {
                            generation: claim.generation,
                            claim_id: claim.claim_id,
                        });
                    }
                }
                Err(_) => self.router_event(RouterEvent::ClaimPersistenceFailed {
                    generation: claim.generation,
                    claim_id: claim.claim_id,
                }),
            },
            Completion::SessionLost { session } => {
                if self.worker.current_session() == Some(session) {
                    self.lose_route();
                }
            }
            Completion::SessionOpened { session, result } => {
                if self.worker.current_session() == Some(session) {
                    match result {
                        Ok(stream) => self.read_session(session, stream),
                        Err(_) => self.lose_route(),
                    }
                }
            }
            Completion::SessionMessage {
                session,
                result,
                stream,
            } => {
                if self.worker.current_session() != Some(session) {
                    return Ok(());
                }
                match result {
                    Ok(Some(message)) => {
                        self.router_message(session, message).await?;
                        if self.worker.current_session() == Some(session) {
                            self.read_session(session, stream);
                        }
                    }
                    Ok(None) | Err(_) => self.lose_route(),
                }
            }
            Completion::Ready {
                task,
                generation,
                result,
            } => {
                result?;
                if self.watch_generations.get(&task) == Some(&generation) {
                    self.watch_generations.remove(&task);
                    self.watches.remove(&task);
                    self.worker_event(WorkerEvent::ReadinessWakeup { task_id: task });
                }
            }
        }
        Ok(())
    }

    async fn actions(&mut self) -> Res<()> {
        while !self.worker_actions.is_empty() || !self.router_actions.is_empty() {
            for action in std::mem::take(&mut self.worker_actions) {
                let action_kind = std::mem::discriminant(&action);
                self.worker_action(action)
                    .await
                    .wrap_err_with(|| format!("worker action {action_kind:?}"))?;
            }
            for action in std::mem::take(&mut self.router_actions) {
                let action_kind = std::mem::discriminant(&action);
                self.router_action(action)
                    .await
                    .wrap_err_with(|| format!("router action {action_kind:?}"))?;
            }
        }
        Ok(())
    }

    async fn worker_action(&mut self, action: WorkerAction) -> Res<()> {
        match action {
            WorkerAction::ClassifyTask { token, ticket } => {
                if !self.worker.classification_pending(&token) {
                    return Ok(());
                }
                if let Some((_, cancel)) = self.classifications.remove(&token.task_id) {
                    cancel.cancel();
                }
                let cancel = self.cancel.child_token();
                self.classifications
                    .insert(token.task_id, (token.clone(), cancel.clone()));
                let adapter = Arc::clone(&self.adapter);
                self.jobs.spawn(async move {
                    tokio::select! {
                        _ = cancel.cancelled() => Completion::Cancelled,
                        result = adapter.classify(*ticket) => {
                            let classification = result.expect("task domain classification failed");
                            Completion::Worker(WorkerEvent::Classified { token, classification })
                        }
                    }
                });
            }
            WorkerAction::PersistAttempt {
                task_id,
                attempt_id,
                allocation,
                declaration,
                invocation,
                declaration_digest,
            } => {
                let request = AttemptRequest {
                    key: self.key(task_id, attempt_id),
                    allocation,
                    declaration: *declaration,
                    declaration_digest,
                    invocation,
                };
                let adapter = Arc::clone(&self.adapter);
                let repo = Arc::clone(&self.owner.repo);
                let snapshot = self.snapshot.clone();
                self.jobs.spawn(async move {
                    let result = async {
                        admit(&repo, &snapshot, None).await?;
                        adapter.persist(request.clone()).await
                    }
                    .await;
                    Completion::Prepared(Box::new((request, result)))
                });
            }
            WorkerAction::StartDispatch {
                token,
                task_id,
                attempt_id,
                declaration_digest,
                invocation,
                ..
            } => {
                let attempt = self
                    .prepared
                    .get(&attempt_id)
                    .expect(ERROR_IMPOSSIBLE)
                    .clone();
                eyre::ensure!(
                    attempt.request.key.task_id == task_id
                        && attempt.request.declaration_digest == declaration_digest
                        && attempt.request.invocation == invocation,
                    "start differs from durable preparation"
                );
                self.start_attempt(attempt, Some(token));
            }
            WorkerAction::DomainTerminalFact {
                declaration,
                summary,
                attempt_id,
            } => {
                tracing::info!(target: "triage_terminal_lifetime", task = %declaration.task_id, ?attempt_id, "incorporating domain terminal");
                let key = attempt_id.map(|attempt_id| self.key(declaration.task_id, attempt_id));
                let receipt = self
                    .adapter
                    .incorporate(*declaration.clone(), summary.clone(), key)
                    .await?;
                tracing::info!(target: "triage_terminal_lifetime", task = %declaration.task_id, "domain terminal incorporated");
                eyre::ensure!(
                    receipt.task_id == declaration.task_id && receipt.summary == summary,
                    "domain returned another settlement receipt"
                );
                if let Some(attempt_id) = attempt_id {
                    let fact = self
                        .finishing
                        .get(&attempt_id)
                        .expect("local terminal acknowledgement requires actual execution outcome")
                        .clone();
                    self.tasks
                        .record_terminal(declaration.task_id, fact)
                        .await?;
                    tracing::info!(target: "triage_terminal_lifetime", task = %declaration.task_id, "task terminal published");
                    self.worker_event(WorkerEvent::TerminalFactAccepted {
                        task_id: declaration.task_id,
                        attempt_id,
                    });
                    self.prepared.remove(&attempt_id);
                    self.finishing.remove(&attempt_id);
                }
            }
            WorkerAction::CancelAttempt {
                task_id,
                attempt_id,
                reason,
            } => {
                let keys = self
                    .prepared
                    .values()
                    .filter(|attempt| {
                        attempt.request.key.task_id == task_id
                            && attempt_id.is_none_or(|id| attempt.request.key.attempt_id == id)
                    })
                    .map(|attempt| attempt.request.key.clone())
                    .collect::<Vec<_>>();
                for key in keys {
                    if let Some(cancel) = self.start_cancels.remove(&key.attempt_id) {
                        cancel.cancel();
                    }
                    self.adapter.cancel(key.clone(), reason.clone()).await?;
                    // The adapter's cancellation receipt follows actual domain
                    // settlement. It is a local release, not a global task fact.
                    self.worker_event(WorkerEvent::AttemptFailed {
                        task_id: key.task_id,
                        attempt_id: key.attempt_id,
                    });
                    self.prepared.remove(&key.attempt_id);
                    self.finishing.remove(&key.attempt_id);
                }
            }
            WorkerAction::WatchReadiness { task_id, watch } => {
                self.watch_sequence = self.watch_sequence.checked_add(1).expect(ERROR_IMPOSSIBLE);
                let generation = self.watch_sequence;
                self.watch_generations.insert(task_id, generation);
                if let Some(cancel) = self.watches.remove(&task_id) {
                    cancel.cancel();
                }
                let cancel = self.cancel.child_token();
                self.watches.insert(task_id, cancel.clone());
                let adapter = Arc::clone(&self.adapter);
                self.jobs.spawn(async move {
                    tokio::select! {
                        _ = cancel.cancelled() => Completion::Cancelled,
                        result = async { adapter.watch(watch).await?.await } => {
                            Completion::Ready { task: task_id, generation, result }
                        }
                    }
                });
            }
            WorkerAction::RegisterSession {
                session,
                registration,
            } => {
                if self.router.is_routing() && self.client.is_none() {
                    self.router_event(RouterEvent::RegisterExecutor {
                        session,
                        capabilities: registration.capabilities,
                        capacity: registration.capacity,
                        protocol: registration.protocol,
                        active_attempts: registration.active_attempts,
                    });
                } else if let Some(client) = &self.client
                    && client.session == session
                {
                    let client = client.client.clone();
                    let pool = self.snapshot.descriptor.pool_id.clone();
                    let cancel = self.session_cancel.clone();
                    // Dialing belongs to the session, not the actor's heartbeat loop.
                    self.jobs.spawn(async move {
                        let result = tokio::select! {
                            _ = cancel.cancelled() => Err(ferr!("session retired while dialing")),
                            result = client.server_streaming(
                                RegisterExecutorRequest { pool, session, registration },
                                32,
                            ) => result.map_err(Into::into),
                        };
                        Completion::SessionOpened { session, result }
                    });
                }
            }
            WorkerAction::AcceptOffer {
                session,
                allocation_id,
                attempt_id,
            } => {
                self.send_report(
                    session,
                    ExecutorReport::Accepted {
                        allocation_id,
                        attempt_id,
                    },
                )
                .await?
            }
            WorkerAction::DeclineOffer {
                session,
                allocation_id,
                reason,
            } => {
                self.send_report(
                    session,
                    ExecutorReport::Declined {
                        allocation_id,
                        reason,
                    },
                )
                .await?
            }
            WorkerAction::ReportAttemptChanged {
                session,
                task_id,
                attempt_id,
                state,
            } => {
                self.send_report(
                    session,
                    ExecutorReport::AttemptChanged {
                        task_id,
                        attempt_id,
                        state,
                    },
                )
                .await?
            }
            WorkerAction::ReportReadinessWakeup { session, task_id } => {
                self.send_report(session, ExecutorReport::ReadinessWakeup { task_id })
                    .await?
            }
            WorkerAction::UnsupportedRouterProtocol { .. } => self.lose_route(),
        }
        Ok(())
    }

    async fn router_action(&mut self, action: RouterAction) -> Res<()> {
        match action {
            RouterAction::CheckObsolescence { token, ticket } => {
                if !self.router.proof_pending(&token) {
                    return Ok(());
                }
                if let Some((_, cancel)) = self.proofs.remove(&token.task_id) {
                    cancel.cancel();
                }
                let cancel = self.cancel.child_token();
                self.proofs
                    .insert(token.task_id, (token.clone(), cancel.clone()));
                let adapter = Arc::clone(&self.adapter);
                self.jobs.spawn(async move {
                    tokio::select! {
                        _ = cancel.cancelled() => Completion::Cancelled,
                        result = adapter.is_proven_obsolete(ticket.declaration.declaration) => {
                            let obsolete = result.expect("domain obsolescence proof failed");
                            Completion::Router(RouterEvent::ObsolescenceChecked { token, obsolete })
                        }
                    }
                });
            }
            RouterAction::PersistClaim { claim } => {
                let repo = Arc::clone(&self.owner.repo);
                let tasks = Arc::clone(&self.tasks);
                let snapshot = self.snapshot.clone();
                let settle = self.snapshot_timing().router_settle;
                self.jobs.spawn(async move {
                    let result = async {
                        admit(&repo, &snapshot, None).await?;
                        tasks
                            .register()
                            .publish_local(
                                snapshot.descriptor.router_slot.as_bytes(),
                                vec![],
                                serde_json::to_vec(&ClaimPayload::from_claim(claim.clone()))?,
                            )
                            .await?;
                        tokio::time::sleep(settle).await;
                        admit(&repo, &snapshot, None).await?;
                        claims(&tasks, &snapshot).await
                    }
                    .await;
                    Completion::ClaimStored { claim, result }
                });
            }
            RouterAction::PublishHeartbeat { claim } => {
                if admit(&self.owner.repo, &self.snapshot, None).await.is_err() {
                    return Ok(());
                }
                let stored = claims(&self.tasks, &self.snapshot)
                    .await
                    .wrap_err("reading persisted claim before heartbeat publication")?;
                let Some(claim) = stored
                    .into_iter()
                    .find(|stored| stored.liveness_key() == claim.liveness_key())
                else {
                    return Ok(());
                };
                let heartbeat = Heartbeat {
                    schema: 1,
                    claim: claim.clone(),
                    endpoint: *self.owner.endpoint.id().as_bytes(),
                };
                self.owner
                    .repo
                    .ephemeral()
                    .publish(
                        self.snapshot.descriptor.router_heartbeat_topic,
                        serde_json::to_vec(&heartbeat)?,
                    )
                    .await?;
            }
            RouterAction::SendOffer {
                session,
                allocation_id,
                task_id,
                declaration,
            } => {
                let Some(ticket_version) = self.router.offer_evidence(
                    session,
                    allocation_id,
                    task_id,
                    declaration.canonical_digest(),
                ) else {
                    return Ok(());
                };
                let adapter = Arc::clone(&self.adapter);
                let cancel = self.cancel.child_token();
                self.jobs.spawn(async move {
                    tokio::select! {
                        _ = cancel.cancelled() => Completion::Cancelled,
                        result = adapter.is_proven_obsolete(*declaration.clone()) => {
                            Completion::OfferChecked {
                                session, allocation_id, task_id, declaration, ticket_version, result,
                            }
                        }
                    }
                });
            }
            RouterAction::StartAttempt {
                session,
                allocation_id,
                task_id,
            } => {
                if admit(&self.owner.repo, &self.snapshot, Some(session.node))
                    .await
                    .is_ok()
                {
                    self.send_router(
                        session,
                        RouterMessage::Start {
                            allocation_id,
                            task_id,
                        },
                    )
                    .await?;
                }
            }
            RouterAction::CancelAttempt {
                session,
                task_id,
                reason,
            } => {
                self.send_router(session, RouterMessage::Cancel { task_id, reason })
                    .await?
            }
            RouterAction::RejectSession { session, reason } => {
                self.send_router(session, RouterMessage::Rejected { reason })
                    .await?
            }
            RouterAction::RejectClaim { collision } => {
                eyre::bail!("invalid authenticated router claim: {collision}")
            }
        }
        Ok(())
    }
}

struct SessionClient {
    session: SessionKey,
    client: irpc::Client<TaskCoordinationRpc>,
    reports: mpsc::Sender<ExecutorReport>,
    reports_rx: Option<mpsc::Receiver<ExecutorReport>>,
}

struct SessionStream {
    endpoint: iroh::EndpointId,
    sender: mpsc::Sender<RouterMessage>,
    cancel: CancellationToken,
}

impl Drop for SessionStream {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
