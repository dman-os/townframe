//! Native pool scheduling and owned asynchronous domain execution.
//!
//! Domains resolve captured inputs and enforce effect/time-window policy at
//! classification and again at actual start. Scheduling never resolves mutable
//! manifests/configuration or treats local unreadiness as global settlement.

use super::*;
use crate::interlude::*;
use std::{collections::VecDeque, future::Future, pin::Pin};

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

/// Durable acknowledgement of this exact request, not canonical result selection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainIncorporationAck {
    pub task_id: PoolTaskId,
    pub declaration_digest: [u8; 32],
    pub incorporated: TerminalSummary,
}

#[derive(Clone, Debug)]
pub enum DomainIncorporationRequest {
    LocalOutcome {
        attempt: AttemptKey,
        fact: TerminalFact,
    },
    ObservedTerminal {
        summary: TerminalSummary,
        affected_attempt: Option<AttemptKey>,
    },
}

impl DomainIncorporationRequest {
    pub fn summary(&self) -> TerminalSummary {
        match self {
            Self::LocalOutcome {
                fact:
                    TerminalFact::Succeeded {
                        attempt_id,
                        result_ref,
                    },
                ..
            } => TerminalSummary::Succeeded {
                attempt_id: *attempt_id,
                result_ref: result_ref.clone(),
            },
            Self::LocalOutcome {
                fact: TerminalFact::Cancelled { reason },
                ..
            } => TerminalSummary::Cancelled {
                reason: reason.clone(),
            },
            Self::ObservedTerminal { summary, .. } => summary.clone(),
        }
    }
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
    /// Request best-effort cancellation, then observe the actual durable winner.
    /// A late cancellation cannot replace a completed success with local failure.
    fn cancel(&self, key: AttemptKey, reason: OpaqueReason) -> AdapterFuture<AttemptSubscription>;
    fn incorporate(
        &self,
        declaration: TaskDeclaration,
        request: DomainIncorporationRequest,
    ) -> AdapterFuture<DomainIncorporationAck>;
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
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
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
    #[serde(with = "super::router::observed_writers")]
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
) -> Res<big_repo::CoordinationAuthority> {
    let sampled = repo
        .coordination_authority(
            snapshot.reference.document.clone(),
            snapshot.reference.authority_group,
        )
        .await?;
    let checked = repo
        .admit_coordination_access(&sampled, Access::Edit)
        .await?;
    if let Some(peer) = peer
        && !checked.edit_agents().contains(&peer.to_bytes32())
    {
        return Err(big_repo::CoordinationError::Unauthorized.into());
    }
    Ok(checked)
}

/// Network inputs fail closed on inconclusive or denied permission, but internal
/// failures must reach the owning actor rather than masquerade as rejection.
async fn permitted(
    repo: &BigRepo,
    snapshot: &PoolDescriptorSnapshot,
    peer: Option<NodePubkey>,
) -> Res<bool> {
    match admit(repo, snapshot, peer).await {
        Ok(_) => Ok(true),
        Err(error)
            if matches!(
                error.downcast_ref::<big_repo::CoordinationError>(),
                Some(
                    big_repo::CoordinationError::Pending
                        | big_repo::CoordinationError::Unauthorized
                )
            ) =>
        {
            Ok(false)
        }
        Err(error) => Err(error),
    }
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PoolStatus {
    pub(crate) routing: bool,
    pub(crate) suspended: bool,
    pub(crate) checkpoint: u64,
    pub(crate) pending_checkpoint: bool,
    pub(crate) claim: Option<RouterClaim>,
}

#[cfg(test)]
struct CheckpointAdmissionGate {
    task: PoolTaskId,
    observed: bool,
    entered: Option<oneshot::Sender<()>>,
    release: oneshot::Receiver<()>,
}

struct PoolInner {
    owner: std::sync::Weak<NativeTaskDriver>,
    #[cfg(test)]
    status: tokio::sync::watch::Receiver<PoolStatus>,
    #[cfg(test)]
    checkpoint_gate: Arc<std::sync::Mutex<Option<CheckpointAdmissionGate>>>,
    snapshot: PoolDescriptorSnapshot,
    adapter: Arc<dyn TaskExecutionAdapter>,
    capabilities: CapabilitySummary,
    capacity: Capacity,
    timing: SchedulingTiming,
    tx: mpsc::Sender<Command>,
    cancel: CancellationToken,
    shutdown: CancellationToken,
    join: std::sync::Mutex<Option<tokio::task::JoinHandle<Res<()>>>>,
}

#[derive(Clone)]
pub struct PoolDriverHandle(Arc<PoolInner>);
impl PoolDriverHandle {
    #[cfg(test)]
    pub(crate) fn status(&self) -> tokio::sync::watch::Receiver<PoolStatus> {
        self.0.status.clone()
    }

    #[cfg(test)]
    pub(crate) fn pause_checkpoint_admission(
        &self,
        task: PoolTaskId,
    ) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (entered, observed) = oneshot::channel();
        let (release, wait) = oneshot::channel();
        assert!(
            self.0
                .checkpoint_gate
                .lock()
                .expect(ERROR_MUTEX)
                .replace(CheckpointAdmissionGate {
                    task,
                    observed: false,
                    entered: Some(entered),
                    release: wait
                })
                .is_none()
        );
        (observed, release)
    }
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
    /// Quiesce for runtime shutdown without withdrawing durable admitted work.
    pub(crate) async fn shutdown(&self) -> Res<()> {
        self.0.shutdown.cancel();
        self.stop().await
    }

    /// Explicit domain detach. Retained transport data does not restart scheduling.
    pub async fn stop(&self) -> Res<()> {
        let Some(owner) = self.0.owner.upgrade() else {
            // Transport may have joined every actor before the runtime releases
            // its retained handles. A missing live owner is still an invariant.
            assert!(self.0.cancel.is_cancelled());
            assert!(self.0.join.lock().expect(ERROR_MUTEX).is_none());
            return Ok(());
        };
        let mut pools = owner.pools.lock().await;
        if pools
            .get(&self.0.snapshot.descriptor.pool_id)
            .is_some_and(|current| Arc::ptr_eq(&current.0, &self.0))
        {
            pools.remove(&self.0.snapshot.descriptor.pool_id);
        }
        // Hold attachment ownership until old jobs and sessions have joined.
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
    Classified {
        token: ClassificationToken,
        ticket: Box<TaskTicket>,
        result: Res<TaskClassification>,
    },
    Router(RouterEvent),
    Obsolescence {
        token: super::router::ObsolescenceToken,
        result: Res<bool>,
    },
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
    #[cfg(test)]
    pub(crate) revision_read_delay: std::sync::Mutex<std::time::Duration>,
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
            #[cfg(test)]
            revision_read_delay: Default::default(),
            cancel: CancellationToken::new(),
            join: Default::default(),
        });
        let weak = Arc::downgrade(&owner);
        let cancel = owner.cancel.clone();
        let join = tokio::spawn(async move {
            let run = async {
                loop {
                    let incoming = rx.recv().await;
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
                    let request = match request {
                        TaskCoordinationRpcMessage::CurrentPoolControl(message) => {
                            use super::rpc::ControlRejection;
                            let result = if owner.identities.peer_for_endpoint(endpoint)
                                != Some(peer.clone())
                            {
                                Err(big_repo::CoordinationError::Unauthorized)
                            } else {
                                async {
                                    if let Some(control) = &message.inner.offered {
                                        owner
                                            .transport
                                            .accept_current_control(
                                                &message.inner.pool,
                                                &peer,
                                                control,
                                            )
                                            .await?;
                                    }
                                    owner
                                        .transport
                                        .current_control(&message.inner.pool, &peer)
                                        .await
                                }
                                .await
                            };
                            let reply = result.map_err(ControlRejection::from);
                            // The requester may disconnect before its response.
                            drop(message.tx.send(reply).await);
                            continue;
                        }
                        TaskCoordinationRpcMessage::CurrentProcessorControl(message) => {
                            let result = if owner.identities.peer_for_endpoint(endpoint)
                                != Some(peer.clone())
                            {
                                Err(big_repo::CoordinationError::Unauthorized)
                            } else {
                                async {
                                    if let Some(control) = &message.inner.offered {
                                        owner
                                            .transport
                                            .accept_processor_control(
                                                &message.inner.processor,
                                                &peer,
                                                control,
                                            )
                                            .await?;
                                    }
                                    owner
                                        .transport
                                        .current_processor_control(&message.inner.processor, &peer)
                                        .await
                                }
                                .await
                            };
                            drop(
                                message
                                    .tx
                                    .send(result.map_err(super::rpc::ControlRejection::from))
                                    .await,
                            );
                            continue;
                        }
                        TaskCoordinationRpcMessage::RegisterSourceControl(message) => {
                            let result = if owner.identities.peer_for_endpoint(endpoint)
                                != Some(peer.clone())
                            {
                                Err(big_repo::CoordinationError::Unauthorized)
                            } else {
                                owner.transport.source_control(&peer, &message.inner).await
                            };
                            drop(
                                message
                                    .tx
                                    .send(result.map_err(super::rpc::ControlRejection::from))
                                    .await,
                            );
                            continue;
                        }
                        request => request,
                    };
                    let pool = match &request {
                        TaskCoordinationRpcMessage::RegisterExecutor(message) => {
                            &message.inner.pool
                        }
                        TaskCoordinationRpcMessage::Report(message) => &message.inner.pool,
                        TaskCoordinationRpcMessage::CurrentPoolControl(_)
                        | TaskCoordinationRpcMessage::CurrentProcessorControl(_)
                        | TaskCoordinationRpcMessage::RegisterSourceControl(_) => {
                            unreachable!("control handled before scheduling ingress")
                        }
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
            };
            // Shutdown cancels in-flight admission as well as the next receive.
            // Selection/certificates committed before cancellation are resumable.
            tokio::select! {
                _ = cancel.cancelled() => Ok(()),
                result = run => result,
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
        let authority = admit(&self.repo, &snapshot, None).await?;
        let mut pools = self.pools.lock().await;
        if let Some(existing) = pools.get(&snapshot.descriptor.pool_id) {
            eyre::ensure!(
                !existing.0.cancel.is_cancelled(),
                "stopped scheduling pool cannot be reattached to a live owner"
            );
            eyre::ensure!(
                self.transport
                    .selected_pool(&snapshot.descriptor.pool_id)
                    .is_some_and(|(selected, _)| selected.reference == snapshot.reference
                        && selected.descriptor == snapshot.descriptor)
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
                edit_agents: authority
                    .edit_agents()
                    .iter()
                    .copied()
                    .map(NodePubkey::new)
                    .collect(),
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
        let (authority_ticket, authority_rx) = self
            .repo
            .subscribe_domain_listener(big_repo::BigRepoDomainFilter)
            .await?;
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
        #[cfg(test)]
        let (status_tx, status) = tokio::sync::watch::channel(PoolStatus {
            routing: false,
            suspended: false,
            checkpoint,
            pending_checkpoint: false,
            claim: None,
        });
        #[cfg(test)]
        let checkpoint_gate = Arc::new(std::sync::Mutex::new(None));
        let (tx, rx) = mpsc::channel(128);
        let cancel = CancellationToken::new();
        let shutdown = CancellationToken::new();
        let handle = PoolDriverHandle(Arc::new(PoolInner {
            owner: Arc::downgrade(self),
            #[cfg(test)]
            status,
            #[cfg(test)]
            checkpoint_gate: Arc::clone(&checkpoint_gate),
            snapshot: snapshot.clone(),
            adapter: Arc::clone(&adapter),
            capabilities,
            capacity,
            timing,
            tx,
            cancel: cancel.clone(),
            shutdown: shutdown.clone(),
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
            reader: Some(reader),
            reader_next: None,
            ephemeral,
            _authority_ticket: authority_ticket,
            authority_rx,
            pending_revision: None,
            suspended: false,
            #[cfg(test)]
            status: status_tx,
            #[cfg(test)]
            checkpoint_gate,
            replay_required: false,
            rx,
            selection_rx: self.transport.selection_changes(),
            selection_dirty: true,
            cancel,
            shutdown,
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
            tracing::debug!(node = %actor.node, cancelled = cancel.is_cancelled(), owner_stopped = actor.owner.cancel.is_cancelled(), runtime_stopped = actor.shutdown.is_cancelled(), "native scheduling actor finished");
            if cancel.is_cancelled() {
                // Process shutdown preserves admitted durable jobs for recovery.
                // Explicit domain detach still withdraws execution participation.
                let detached = !actor.owner.cancel.is_cancelled() && !actor.shutdown.is_cancelled();
                actor
                    .withdraw(detached)
                    .await
                    .expect("detaching pool participation failed");
            }
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
        TaskCoordinationRpcMessage::CurrentPoolControl(message) => {
            drop(
                message
                    .tx
                    .send(Err(super::rpc::ControlRejection::Invalid(reason.into())))
                    .await,
            );
        }
        TaskCoordinationRpcMessage::CurrentProcessorControl(message) => {
            drop(
                message
                    .tx
                    .send(Err(super::rpc::ControlRejection::Invalid(reason.into())))
                    .await,
            );
        }
        TaskCoordinationRpcMessage::RegisterSourceControl(message) => {
            drop(
                message
                    .tx
                    .send(Err(super::rpc::ControlRejection::Invalid(reason.into())))
                    .await,
            );
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
    reader: Option<Box<dyn big_sync::LocalPartRevisionReader>>,
    #[expect(clippy::type_complexity)]
    reader_next: Option<
        Pin<
            Box<
                dyn Future<
                        Output = (
                            Box<dyn big_sync::LocalPartRevisionReader>,
                            Res<RevisionRead<u64, PartEvent>>,
                        ),
                    > + Send,
            >,
        >,
    >,
    ephemeral: big_repo::BigEphemeralSubscription,
    _authority_ticket: big_repo::BigRepoDomainListenerRegistration,
    authority_rx: mpsc::UnboundedReceiver<Vec<big_repo::BigRepoDomainNotification>>,
    selection_rx: tokio::sync::watch::Receiver<()>,
    selection_dirty: bool,
    pending_revision: Option<(u64, VecDeque<PartEvent>)>,
    suspended: bool,
    #[cfg(test)]
    status: tokio::sync::watch::Sender<PoolStatus>,
    #[cfg(test)]
    checkpoint_gate: Arc<std::sync::Mutex<Option<CheckpointAdmissionGate>>>,
    replay_required: bool,
    rx: mpsc::Receiver<Command>,
    cancel: CancellationToken,
    shutdown: CancellationToken,
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
    #[cfg(test)]
    fn publish_status(&self) {
        let status = PoolStatus {
            routing: self.router.is_routing(),
            suspended: self.suspended,
            checkpoint: self.checkpoint,
            pending_checkpoint: self.pending_revision.is_some(),
            claim: self.router.own_claim().cloned(),
        };
        self.status.send_if_modified(|current| {
            if *current == status {
                false
            } else {
                tracing::debug!(
                    pool = %self.snapshot.descriptor.pool_id,
                    node = %self.owner.repo.local_peer_id(),
                    ?status,
                    "native scheduling owner state changed"
                );
                *current = status;
                true
            }
        });
    }
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
        mut token: Option<super::worker::DispatchToken>,
    ) {
        let adapter = Arc::clone(&self.adapter);
        let repo = Arc::clone(&self.owner.repo);
        let snapshot = self.snapshot.clone();
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
                completion = async move {
                    let result = async {
                        loop {
                            match admit(&repo, &snapshot, None).await {
                                Ok(_) => break,
                                Err(error) => match error.downcast_ref::<big_repo::CoordinationError>() {
                                    Some(big_repo::CoordinationError::Pending) => {
                                        tokio::time::sleep(big_repo::CoordinationAuthority::PENDING_RECHECK_DELAY).await;
                                    }
                                    Some(big_repo::CoordinationError::Unauthorized) => {
                                        token = None;
                                        return adapter.cancel(key.clone(), OpaqueReason::from_label("pool participation revoked before start")).await;
                                    }
                                    _ => return Err(error),
                                },
                            }
                        }
                        adapter.start(attempt).await
                    }.await;
                    Completion::Started { key, token, result }
                } => completion,
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

    async fn refresh_transport(&mut self) -> Res<()> {
        if !self.selection_dirty {
            return Ok(());
        }
        let (snapshot, tasks) = self
            .owner
            .transport
            .selected_pool(&self.snapshot.descriptor.pool_id)
            .expect("live scheduling retains its transport owner");
        if snapshot.reference == self.snapshot.reference {
            self.selection_dirty = false;
            return Ok(());
        }
        admit(&self.owner.repo, &snapshot, None).await?;
        eyre::ensure!(
            snapshot.descriptor.register_scope == self.snapshot.descriptor.register_scope
                && snapshot.descriptor.register_incarnation
                    == self.snapshot.descriptor.register_incarnation
                && snapshot.descriptor.router_slot == self.snapshot.descriptor.router_slot
                && snapshot.descriptor.router_heartbeat_topic
                    == self.snapshot.descriptor.router_heartbeat_topic,
            "transport rotation changes live scheduling identity"
        );
        let reader = self
            .owner
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
            .map_err(|error| ferr!("rotated scheduling reader: {error:?}"))?;
        let checkpoint = tasks
            .register()
            .consumer_checkpoint(b"native-scheduling")
            .await?;
        // Drop the predecessor read, but retain the admitted attempts, pending
        // effects and exact unread revision. Replay reconstructs the new part;
        // its selector checkpoint comes from the durable successor binding.
        self.reader_next = None;
        self.reader = Some(reader);
        self.snapshot = snapshot;
        self.tasks = tasks;
        self.checkpoint = checkpoint;
        self.selection_dirty = false;
        Ok(())
    }

    async fn run(&mut self) -> Res<()> {
        loop {
            match self.run_admitted().await {
                Ok(()) => return Ok(()),
                Err(error) => {
                    let revoked = match error.downcast_ref::<big_repo::CoordinationError>() {
                        Some(big_repo::CoordinationError::Pending) => false,
                        Some(big_repo::CoordinationError::Unauthorized) => {
                            // A retired SQL binding rejects publication with the same
                            // typed error as native revocation. Only current native
                            // denial may cancel already admitted durable execution.
                            let (selected, _) = self
                                .owner
                                .transport
                                .selected_pool(&self.snapshot.descriptor.pool_id)
                                .expect("live scheduling retains its transport owner");
                            match admit(&self.owner.repo, &selected, None).await {
                                Ok(_) => false,
                                Err(admission) => {
                                    match admission.downcast_ref::<big_repo::CoordinationError>() {
                                        Some(big_repo::CoordinationError::Pending) => false,
                                        Some(big_repo::CoordinationError::Unauthorized) => true,
                                        _ => return Err(admission),
                                    }
                                }
                            }
                        }
                        _ => return Err(error),
                    };
                    self.withdraw(revoked).await?;
                    #[cfg(test)]
                    self.publish_status();
                    // Retain the exact unread revision/checkpoint and pending effects.
                    // Native membership notifications are incomplete, so unresolved
                    // admission uses the same bounded recheck cadence as transport.
                    tokio::select! {
                        _ = self.cancel.cancelled() => return Ok(()),
                        changed = self.selection_rx.changed() => {
                            changed.expect("retained transport selection channel closed");
                            self.selection_dirty = true;
                        }
                        changes = self.authority_rx.recv() => {
                            changes.ok_or_else(|| ferr!("pool authority listener closed"))?;
                        }
                        _ = tokio::time::sleep(big_repo::CoordinationAuthority::PENDING_RECHECK_DELAY) => {}
                        command = self.rx.recv() => {
                            match command {
                                Some(Command::Submit { reply, .. }) => { drop(reply.send(Err(error))); }
                                Some(Command::Rpc { request, .. }) => reject_request(request, "pool participation unavailable").await,
                                None => return Ok(()),
                            }
                        }
                    }
                }
            }
        }
    }

    async fn withdraw(&mut self, revoked: bool) -> Res<()> {
        if !self.suspended {
            self.suspended = true;
            self.router_event(RouterEvent::AuthorityChanged {
                edit_agents: BTreeSet::new(),
            });
            self.sessions.clear();
            self.live.clear();
            self.lose_route();
        }
        if revoked && !self.replay_required {
            self.replay_required = true;
            self.worker_event(WorkerEvent::ParticipationChanged { allowed: false });
            let actions = std::mem::take(&mut self.worker_actions);
            for action in actions {
                if matches!(action, WorkerAction::CancelAttempt { .. }) {
                    self.worker_action(action).await?;
                } else {
                    self.worker_actions.push(action);
                }
            }
        }
        Ok(())
    }

    async fn run_admitted(&mut self) -> Res<()> {
        let mut tick = tokio::time::interval(self.snapshot_timing().heartbeat_interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let takeover = tokio::time::Instant::now() + self.snapshot_timing().router_takeover_after;
        let mut next_takeover = takeover;
        loop {
            self.refresh_transport().await?;
            tracing::debug!(node = %self.owner.repo.local_peer_id(), phase = "admission", "native scheduling await beginning");
            let authority = admit(&self.owner.repo, &self.snapshot, None).await?;
            tracing::debug!(node = %self.owner.repo.local_peer_id(), phase = "admission", "native scheduling await completed");
            if !self
                .router
                .edit_agents()
                .iter()
                .map(|node| node.to_bytes32())
                .eq(authority.edit_agents().iter().copied())
            {
                let edit_agents = authority
                    .edit_agents()
                    .iter()
                    .copied()
                    .map(NodePubkey::new)
                    .collect();
                self.router_event(RouterEvent::AuthorityChanged { edit_agents });
            }
            self.sessions
                .retain(|session, _| authority.edit_agents().contains(&session.node.to_bytes32()));
            self.live.retain(|claim, _| {
                authority
                    .edit_agents()
                    .contains(&claim.candidate.to_bytes32())
            });
            if self.route.is_some_and(|(claim, _)| {
                !authority
                    .edit_agents()
                    .contains(&claim.candidate.to_bytes32())
            }) {
                self.lose_route();
            }
            if self.replay_required {
                self.reader_next = None;
                self.reader = Some(
                    self.owner
                        .transport
                        .shared_store()
                        .open_revision_reader(SubPartsRequest {
                            lower_bound: 0,
                            targets: [SubscriptionTarget::Part {
                                part_id: self.snapshot.descriptor.active_task_part.clone(),
                                cursor: 0,
                            }]
                            .into(),
                        })
                        .await?
                        .map_err(|error| ferr!("task scheduling reader: {error:?}"))?,
                );
                self.pending_revision = None;
                self.replay_required = false;
            }
            if self.suspended {
                self.worker_event(WorkerEvent::ParticipationChanged { allowed: true });
            }
            self.suspended = false;
            tracing::debug!(node = %self.owner.repo.local_peer_id(), phase = "actions", "native scheduling await beginning");
            self.actions().await?;
            tracing::debug!(node = %self.owner.repo.local_peer_id(), phase = "actions", "native scheduling await completed");
            tracing::debug!(node = %self.owner.repo.local_peer_id(), phase = "revision", "native scheduling await beginning");
            self.incorporate_pending_revision().await?;
            // Keep the multi-query read alive when heartbeats or RPC win select.
            // Recreating it on every wake can starve delivery under disk load.
            if self.reader_next.is_none() {
                let mut reader = self.reader.take().expect("revision reader has one owner");
                #[cfg(test)]
                let delay = *self.owner.revision_read_delay.lock().expect(ERROR_MUTEX);
                self.reader_next = Some(Box::pin(async move {
                    #[cfg(test)]
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    let result = reader.next(RevisionReadLimits::default()).await;
                    (reader, result)
                }));
            }
            tracing::debug!(node = %self.owner.repo.local_peer_id(), phase = "revision", "native scheduling await completed");
            #[cfg(test)]
            self.publish_status();
            tokio::select! {
                _ = self.cancel.cancelled() => return Ok(()),
                changed = self.selection_rx.changed() => {
                    changed.expect("retained transport selection channel closed");
                    self.selection_dirty = true;
                }
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
                        Command::Rpc { peer, endpoint, request } => {
                            tracing::debug!(node = %self.node, phase = "rpc", "native scheduling selected await beginning");
                            self.rpc(peer, endpoint, request).await.wrap_err("handling native scheduling RPC")?;
                            tracing::debug!(node = %self.node, phase = "rpc", "native scheduling selected await completed");
                        },
                    }
                }
                (reader, revision) = self.reader_next.as_mut().expect("owned revision read") => {
                    self.reader_next = None;
                    self.reader = Some(reader);
                    match revision.wrap_err("reading native scheduling revision")? {
                        RevisionRead::ReplayComplete { .. } => {}
                        RevisionRead::Entries { revision, entries } => {
                            self.pending_revision = Some((revision, entries.into()));
                        }
                    }
                }
                changes = self.authority_rx.recv() => {
                    changes.ok_or_else(|| ferr!("pool authority listener closed"))?;
                }
                event = self.ephemeral.recv() => {
                    let Some(event) = event else { eyre::bail!("native heartbeat subscription closed"); };
                    tracing::debug!(node = %self.node, phase = "heartbeat", "native scheduling selected await beginning");
                    self.heartbeat(event).await.wrap_err("incorporating native scheduling heartbeat")?;
                    tracing::debug!(node = %self.node, phase = "heartbeat", "native scheduling selected await completed");
                }
                completion = self.jobs.join_next(), if !self.jobs.is_empty() => {
                    let completion = completion.expect(ERROR_IMPOSSIBLE).expect("native pool job panicked");
                    let completion_kind = std::mem::discriminant(&completion);
                    tracing::debug!(node = %self.node, ?completion_kind, phase = "completion", "native scheduling selected await beginning");
                    self.complete(completion).await
                        .wrap_err_with(|| format!("completing native scheduling job {completion_kind:?}"))?;
                    tracing::debug!(node = %self.node, ?completion_kind, phase = "completion", "native scheduling selected await completed");
                }
                _ = tick.tick() => {
                    tracing::debug!(node = %self.owner.repo.local_peer_id(), phase = "tick", "native scheduling await beginning");
                    self.expire().await?;
                    self.router_event(RouterEvent::HeartbeatDue);
                    self.router_event(RouterEvent::RoutingTick);
                    if tokio::time::Instant::now() >= next_takeover {
                        if permitted(&self.owner.repo, &self.snapshot, None).await? {
                            self.router_event(RouterEvent::ConsiderTakeover);
                        }
                        next_takeover = tokio::time::Instant::now() + self.snapshot_timing().router_takeover_after;
                    }
                    self.reconnect().await?;
                    tracing::debug!(node = %self.owner.repo.local_peer_id(), phase = "tick", "native scheduling await completed");
                }
            }
        }
    }
    async fn incorporate_pending_revision(&mut self) -> Res<()> {
        while let Some((_, entries)) = &self.pending_revision {
            let Some(entry) = entries.front().cloned() else {
                break;
            };
            self.revision(entry)
                .await
                .wrap_err("incorporating native scheduling revision")?;
            self.pending_revision
                .as_mut()
                .expect(ERROR_IMPOSSIBLE)
                .1
                .pop_front();
            self.actions().await?;
        }
        if let Some((revision, _)) = &self.pending_revision {
            let revision = *revision;
            if revision > self.checkpoint {
                #[cfg(test)]
                {
                    let mut gate = self.checkpoint_gate.lock().expect(ERROR_MUTEX);
                    if let Some(paused) = gate.as_mut().filter(|paused| paused.observed) {
                        if let Some(entered) = paused.entered.take() {
                            entered.send(()).expect(ERROR_CHANNEL);
                        }
                        match paused.release.try_recv() {
                            Ok(()) => *gate = None,
                            Err(oneshot::error::TryRecvError::Empty) => {
                                return Err(big_repo::CoordinationError::Pending.into());
                            }
                            Err(oneshot::error::TryRecvError::Closed) => {
                                panic!("checkpoint admission gate closed")
                            }
                        }
                    }
                }
                self.tasks
                    .register()
                    .begin_consumer_settlement(b"native-scheduling", revision)
                    .await
                    .wrap_err("beginning native scheduling checkpoint")?
                    .commit()
                    .await
                    .wrap_err("committing native scheduling checkpoint")?;
                self.checkpoint = revision;
            }
            self.pending_revision = None;
        }
        Ok(())
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
        #[cfg(test)]
        {
            // Pause the target task checkpoint, not prior router/terminal traffic:
            // suspending earlier would reject the very submission under observation.
            if let Some(gate) = self.checkpoint_gate.lock().expect(ERROR_MUTEX).as_mut()
                && gate.task == task_id
            {
                gate.observed = true;
            }
        }
        tracing::debug!(node = %self.node, task = %task_id, removed, session = ?self.worker.current_session(), "native scheduling task revision observed");
        if removed {
            self.router_event(RouterEvent::TaskRemoved { task_id });
        } else if let Some(ticket) = self.tasks.ticket(task_id).await? {
            tracing::debug!(node = %self.node, task = %task_id, producer = ?ticket.declaration.declaration.producer, terminal = ticket.terminal_summary().is_some(), "native scheduling task ticket decoded");
            // Durable remote incorporation precedes checkpoint acknowledgement.
            // The worker also propagates monotonic summary transitions through
            // its own exact finishing acknowledgement path.
            if let Some(summary) = ticket.terminal_summary() {
                let receipt = self
                    .adapter
                    .incorporate(
                        ticket.declaration.declaration.clone(),
                        DomainIncorporationRequest::ObservedTerminal {
                            summary: summary.clone(),
                            affected_attempt: None,
                        },
                    )
                    .await?;
                eyre::ensure!(
                    receipt.task_id == task_id
                        && receipt.declaration_digest
                            == ticket.declaration.declaration.canonical_digest()
                        && receipt.incorporated == summary,
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
        if heartbeat.schema != 2
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
            || !permitted(&self.owner.repo, &self.snapshot, Some(claim.candidate)).await?
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
            }) || !permitted(&self.owner.repo, &self.snapshot, Some(session.node)).await?
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
            || !permitted(&self.owner.repo, &self.snapshot, Some(claim.candidate)).await?
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
            TaskCoordinationRpcMessage::CurrentPoolControl(_)
            | TaskCoordinationRpcMessage::CurrentProcessorControl(_)
            | TaskCoordinationRpcMessage::RegisterSourceControl(_) => {
                unreachable!("control cannot enter a scheduling actor")
            }
        };
        if !self.router.is_routing()
            || peer != PeerKey::new(session.node.to_bytes32())
            || self.owner.identities.peer_for_endpoint(endpoint) != Some(peer.clone())
            || !permitted(&self.owner.repo, &self.snapshot, Some(session.node)).await?
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
                            || !permitted(&owner.repo, &snapshot, Some(session.node))
                                .await
                                .expect("native session admission failed")
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
            TaskCoordinationRpcMessage::CurrentPoolControl(_)
            | TaskCoordinationRpcMessage::CurrentProcessorControl(_)
            | TaskCoordinationRpcMessage::RegisterSourceControl(_) => {
                unreachable!("control cannot enter a scheduling actor")
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
                || !permitted(&self.owner.repo, &self.snapshot, Some(key.candidate)).await?)
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
            Completion::Classified {
                token,
                ticket,
                result,
            } => {
                tracing::debug!(
                    task = %ticket.declaration.declaration.task_id,
                    node = %self.owner.repo.local_peer_id(),
                    classification = match &result {
                        Ok(TaskClassification::Runnable(_)) => "runnable",
                        Ok(TaskClassification::NotReady(_)) => "not-ready",
                        Ok(TaskClassification::Obsolete) => "obsolete",
                        Ok(TaskClassification::CoordinationIncomplete) => "coordination-incomplete",
                        Ok(TaskClassification::Invalid) => "invalid",
                        Err(_) => "error",
                    },
                    pending = self.worker.classification_pending(&token),
                    "native executor classification completed"
                );
                if self.worker.classification_pending(&token) {
                    match result {
                        Ok(classification) => self.worker_event(WorkerEvent::Classified {
                            token,
                            classification,
                        }),
                        Err(error) => {
                            self.worker_actions
                                .push(WorkerAction::ClassifyTask { token, ticket });
                            return Err(error);
                        }
                    }
                }
            }
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
                } else if !permitted(&self.owner.repo, &self.snapshot, Some(session.node)).await? {
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
            Completion::Obsolescence { token, result } => {
                if self.router.proof_pending(&token) {
                    let obsolete = result?;
                    self.router_event(RouterEvent::ObsolescenceChecked { token, obsolete });
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
                Err(error) => {
                    if !matches!(
                        error.downcast_ref::<big_repo::CoordinationError>(),
                        Some(
                            big_repo::CoordinationError::Pending
                                | big_repo::CoordinationError::Unauthorized
                        )
                    ) {
                        return Err(error);
                    }
                    self.router_event(RouterEvent::ClaimPersistenceFailed {
                        generation: claim.generation,
                        claim_id: claim.claim_id,
                    });
                }
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
                tracing::debug!(
                    task = %task,
                    node = %self.owner.repo.local_peer_id(),
                    generation,
                    error = ?result.as_ref().err(),
                    "native executor readiness watch completed"
                );
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
            let mut actions = std::mem::take(&mut self.worker_actions).into_iter();
            while let Some(action) = actions.next() {
                let action_kind = std::mem::discriminant(&action);
                if let Err(error) = self.worker_action(action).await {
                    self.worker_actions.extend(actions);
                    return Err(error).wrap_err_with(|| format!("worker action {action_kind:?}"));
                }
            }
            let mut actions = std::mem::take(&mut self.router_actions).into_iter();
            while let Some(action) = actions.next() {
                let action_kind = std::mem::discriminant(&action);
                if let Err(error) = self.router_action(action).await {
                    self.router_actions.extend(actions);
                    return Err(error).wrap_err_with(|| format!("router action {action_kind:?}"));
                }
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
                        result = adapter.classify(*ticket.clone()) => {
                            Completion::Classified { token, ticket, result }
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
                if !self.worker.dispatch_pending(&token) {
                    return Ok(());
                }
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
                let result = async {
                tracing::info!(target: "triage_terminal_lifetime", task = %declaration.task_id, ?attempt_id, "incorporating domain terminal");
                let request = if let Some(attempt_id) = attempt_id {
                    DomainIncorporationRequest::LocalOutcome {
                        attempt: self.key(declaration.task_id, attempt_id),
                        fact: self.finishing.get(&attempt_id).expect("local acknowledgement requires actual outcome").clone(),
                    }
                } else {
                    DomainIncorporationRequest::ObservedTerminal { summary: summary.clone(), affected_attempt: None }
                };
                eyre::ensure!(request.summary() == summary, "incorporation changes original finishing fact");
                let receipt = self
                    .adapter
                    .incorporate(*declaration.clone(), request)
                    .await?;
                tracing::info!(target: "triage_terminal_lifetime", task = %declaration.task_id, "domain terminal incorporated");
                eyre::ensure!(
                    receipt.task_id == declaration.task_id && receipt.declaration_digest == declaration.canonical_digest() && receipt.incorporated == summary,
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
                eyre::Ok(())
                }.await;
                if let Err(error) = result {
                    // Incorporation is idempotent and its receipt is retained until
                    // signed publication succeeds; permission races cannot lose it.
                    self.worker_actions.push(WorkerAction::DomainTerminalFact {
                        declaration,
                        summary,
                        attempt_id,
                    });
                    return Err(error);
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
                    let subscription = self.adapter.cancel(key.clone(), reason.clone()).await?;
                    // Keep ownership until the actual cancellation/finalization
                    // winner is durable. Revocation is not a global Cancelled fact.
                    self.jobs.spawn(async move {
                        Completion::Outcome {
                            key,
                            result: subscription.await,
                        }
                    });
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
                            Completion::Obsolescence { token, result }
                        }
                    }
                });
            }
            RouterAction::PersistClaim { claim } => {
                if self
                    .router
                    .own_claim()
                    .is_none_or(|own| own.liveness_key() != claim.liveness_key())
                {
                    return Ok(());
                }
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
                if !self.router.is_routing()
                    || self
                        .router
                        .own_claim()
                        .is_none_or(|own| own.liveness_key() != claim.liveness_key())
                {
                    return Ok(());
                }
                if !permitted(&self.owner.repo, &self.snapshot, None).await? {
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
                    schema: 2,
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
                tracing::debug!(task = %task_id, ?session, node = %self.owner.repo.local_peer_id(), "native router preparing offer");
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
                if self.router.start_pending(session, allocation_id, task_id)
                    && permitted(&self.owner.repo, &self.snapshot, Some(session.node)).await?
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
