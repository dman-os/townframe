// FIXME: split the Worker into multiple states,
// FIXME: this doesn't model deletes, i wonder
// if partitions are a good way to model deletes
// anyways.
// FIXME: write this in a sans-io manner where there
// are pluggable PartitionKind <-> Worker impls to
// assert core full sync impl

use crate::interlude::*;

use am_utils_rs::{
    partition::PartitionStore,
    repo::PeerId,
    sync::{
        machine::{SyncCompletion, SyncMachine, SyncMachineCommand},
        peer::{
            PeerSyncProgressEvent, PeerSyncWorkerEvent, PeerSyncWorkerExit, PeerSyncWorkerMsg,
            PeerSyncWorkerStopToken, SpawnPeerSyncWorkerArgs,
        },
        protocol::{PartitionId, PartitionSyncRpc, PeerKey},
        store::SyncStoreHandle,
    },
};
use iroh_blobs::Hash;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::blobs::{BlobScope, BlobsRepo};
use crate::progress::{
    CreateProgressTaskArgs, ProgressFinalState, ProgressRepo, ProgressRetentionPolicy,
    ProgressSeverity, ProgressUnit, ProgressUpdate, ProgressUpdateDeets,
};
use crate::sync::PARTITION_SYNC_ALPN;

type BigRepoConnectionId = PeerId;

mod blob_worker;
mod doc_worker;
mod scheduler;

const MIN_DOC_WORKER_FLOOR: usize = 8;
const MIN_BLOB_WORKER_FLOOR: usize = 8;

pub struct WorkerHandle {
    msg_tx: mpsc::UnboundedSender<Msg>,
    /// Option allows you to take it out
    pub events_rx: Option<tokio::sync::broadcast::Receiver<FullSyncEvent>>,
}

#[derive(Debug, Clone)]
pub enum FullSyncEvent {
    PeerFullSynced {
        peer_key: PeerKey,
        doc_count: usize,
    },
    PartitionFullSynced {
        peer_key: PeerKey,
        partition: PartitionId,
    },
    DocSyncedWithPeer {
        peer_key: PeerKey,
        doc_id: DocumentId,
    },
    BlobSynced {
        hash: Arc<str>,
    },
    BlobDownloadStarted {
        hash: Arc<str>,
    },
    BlobDownloadFinished {
        hash: Arc<str>,
        success: bool,
    },
    BlobSyncBackoff {
        hash: Arc<str>,
        delay: Duration,
        attempt_no: usize,
    },
    StalePeer {
        peer_key: PeerKey,
    },
}

#[derive(Debug)]
enum Msg {
    SetPeer {
        endpoint_addr: iroh::EndpointAddr,
        partitions: HashSet<PartitionId>,
        peer_key: PeerKey,
        connection: am_utils_rs::repo::BigRepoConnection,
        resp: tokio::sync::oneshot::Sender<()>,
    },
    // OutgoingConn {
    //     endpoint_id: EndpointId,
    //     conn_id: samod::ConnectionId,
    //     resp: tokio::sync::oneshot::Sender<()>,
    // },
    DelPeer {
        peer_id: PeerId,
        resp: tokio::sync::oneshot::Sender<()>,
    },
    BlobSyncCompleted {
        hash: Hash,
        outcome: blob_worker::SyncBlobOutcome,
    },
    BlobSyncBackoff {
        hash: Hash,
        // peer_id: PeerId,
        delay: Duration,
        previous_retry_state: RetryState,
    },
    DocSyncCompleted {
        doc_id: DocumentId,
        peer_id: PeerId,
        outcome: doc_worker::SyncDocOutcome,
    },
    DocSyncBackoff {
        doc_id: DocumentId,
        peer_id: PeerId,
        delay: Duration,
        previous_retry_state: RetryState,
    },
    GetPeerSyncSnapshot {
        peer_ids: Vec<PeerId>,
        resp: tokio::sync::oneshot::Sender<HashMap<PeerId, PeerSyncSnapshot>>,
    },
    WaitForPeersFullySynced {
        peer_ids: Vec<PeerId>,
        required_partitions: HashSet<PartitionId>,
        resp: tokio::sync::oneshot::Sender<()>,
    },
}

#[derive(Debug, Clone)]
#[expect(dead_code)]
pub(crate) struct PeerSyncSnapshot {
    pub emitted_full_synced: bool,
    pub bootstrap_ready: bool,
    pub bootstrap_synced_docs: u64,
    pub bootstrap_remaining_docs: u64,
    pub doc_pending_docs: u64,
    pub live_ready: bool,
    pub has_peer_session: bool,
    pub fully_synced_partitions: HashSet<PartitionId>,
}

struct FullSyncWaiter {
    remaining: HashSet<PeerId>,
    required_partitions: HashSet<PartitionId>,
    resp: tokio::sync::oneshot::Sender<()>,
}

#[derive(Debug)]
#[expect(clippy::enum_variant_names)]
enum SyncProgressMsg {
    BlobWorkerStarted {
        hash: Arc<str>,
    },
    BlobDownloadStarted {
        peer_id: PeerId,
        hash: Arc<str>,
    },
    BlobDownloadProgress {
        peer_id: PeerId,
        hash: Arc<str>,
        done_counter: u64,
    },
    BlobMaterializeStarted {
        hash: Arc<str>,
    },
    BlobDownloadFinished {
        hash: Arc<str>,
        error: Option<eyre::Report>,
    },
}

impl WorkerHandle {
    pub async fn set_connection(
        &self,
        connection: am_utils_rs::repo::BigRepoConnection,
        endpoint_addr: iroh::EndpointAddr,
        peer_key: PeerKey,
        partitions: HashSet<PartitionId>,
    ) -> Res<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let msg = Msg::SetPeer {
            resp: tx,
            endpoint_addr,
            partitions,
            peer_key,
            connection,
        };
        self.msg_tx.send(msg).wrap_err(ERROR_ACTOR)?;
        rx.await.wrap_err(ERROR_CHANNEL)
    }
    pub async fn del_connection(&self, peer_id: PeerId) -> Res<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let msg = Msg::DelPeer { resp: tx, peer_id };
        self.msg_tx.send(msg).wrap_err(ERROR_ACTOR)?;
        rx.await.wrap_err(ERROR_CHANNEL)
    }

    pub async fn get_peer_sync_snapshot(
        &self,
        peer_ids: Vec<PeerId>,
    ) -> Res<HashMap<PeerId, PeerSyncSnapshot>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let msg = Msg::GetPeerSyncSnapshot { peer_ids, resp: tx };
        self.msg_tx.send(msg).wrap_err(ERROR_ACTOR)?;
        rx.await.wrap_err(ERROR_CHANNEL)
    }

    pub async fn wait_for_peers_fully_synced(
        &self,
        peer_ids: Vec<PeerId>,
        required_partitions: HashSet<PartitionId>,
    ) -> Res<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let msg = Msg::WaitForPeersFullySynced {
            peer_ids,
            required_partitions,
            resp: tx,
        };
        self.msg_tx.send(msg).wrap_err(ERROR_ACTOR)?;
        rx.await.wrap_err(ERROR_CHANNEL)
    }
}

pub struct StopToken {
    pub cancel_token: CancellationToken,
    join_handle: tokio::task::JoinHandle<()>,
}

impl StopToken {
    pub async fn stop(self) -> Result<(), utils_rs::WaitOnHandleError> {
        self.cancel_token.cancel();
        utils_rs::wait_on_handle_with_timeout(
            self.join_handle,
            utils_rs::scale_timeout(Duration::from_secs(5)),
        )
        .await
    }
}

#[tracing::instrument(skip_all, fields(%local_peer_key))]
pub async fn spawn_full_sync_worker(
    big_repo: SharedBigRepo,
    partition_store: Arc<PartitionStore>,
    local_peer_key: PeerKey,
    blobs_repo: Arc<BlobsRepo>,
    progress_repo: Option<Arc<ProgressRepo>>,
    sync_store: SyncStoreHandle,
    iroh_endpoint: iroh::Endpoint,
) -> Res<(WorkerHandle, StopToken)> {
    let (msg_tx, mut msg_rx) = mpsc::unbounded_channel();
    let (sync_progress_tx, mut sync_progress_rx) = mpsc::channel::<SyncProgressMsg>(8192);
    let (peer_worker_msg_tx, mut peer_worker_msg_rx) = mpsc::channel::<PeerSyncWorkerMsg>(8192);
    let (events_tx, events_rx) = tokio::sync::broadcast::channel(1024);

    let cancel_token = CancellationToken::new();
    let task_set = utils_rs::AbortableJoinSet::new();

    // pre seed the two blob partitions to assist
    // kind_from_part_ids
    let default_partitions = [
        (
            crate::blobs::BLOB_SCOPE_DOCS_PARTITION_ID.into(),
            Partition {
                peers: default(),
                kind: PartitionKind::Blob,
            },
        ),
        (
            crate::blobs::BLOB_SCOPE_PLUGS_PARTITION_ID.into(),
            Partition {
                peers: default(),
                kind: PartitionKind::Blob,
            },
        ),
    ]
    .into();
    let mut worker = Worker {
        big_repo,
        local_peer_key,
        cancel_token: cancel_token.clone(),
        msg_tx: msg_tx.clone(),
        sync_progress_tx: sync_progress_tx.clone(),
        peer_worker_msg_tx: peer_worker_msg_tx.clone(),
        events_tx,
        partitions: default_partitions,

        blobs_repo,
        progress_repo,
        iroh_endpoint,
        task_set,

        sync_machine: SyncMachine::new(Arc::clone(&partition_store), sync_store.clone()),
        partition_store,
        sync_store,
        scheduler: default(),

        doc_request_set: default(),
        blob_requirements: default(),
        peer_sessions_to_refresh: default(),

        known_peer_set: default(),
        expected_peer_session_closes: default(),
        peer_id_by_peer_key: default(),
        seen_peer_keys: default(),
        peer_partition_sessions: default(),
        full_sync_waiters: Vec::new(),

        max_active_sync_workers: 24,
    };

    let fut = {
        let cancel_token = cancel_token.clone();
        let mut janitor_tick = tokio::time::interval(Duration::from_millis(500));
        async move {
            let mut sync_progress_buf = Vec::with_capacity(512);
            loop {
                if !worker.peer_sessions_to_refresh.is_empty() {
                    worker.batch_refresh_peer_sessions().await?;
                }
                if !worker.scheduler.docs_to_stop().is_empty() {
                    worker.batch_stop_docs().await?;
                }
                if !worker.scheduler.blobs_to_stop().is_empty() {
                    worker.batch_stop_docs().await?;
                }
                if worker.scheduler.has_docs_to_boot() {
                    worker.batch_boot_docs().await?;
                }
                if worker.scheduler.has_blobs_to_boot() {
                    worker.batch_boot_blobs().await?;
                }
                tokio::select! {
                    biased;
                    _ = cancel_token.cancelled() => {
                        break;
                    }
                    val = msg_rx.recv() => {
                        let Some(msg) = val else {
                            warn!("FullSyncWorkerHandle dropped, closing loop");
                            break;
                        };
                        info!(?msg, "XXX msg");
                        worker.handle_msg(msg).await?;
                    }
                    count = sync_progress_rx.recv_many(&mut sync_progress_buf, 512) => {
                        if count == 0 {
                            continue;
                        }
                        info!(?count, "XXX sync progress");
                        worker.handle_sync_progress_batch(&mut sync_progress_buf).await?;
                    }
                    val = peer_worker_msg_rx.recv() => {
                        let Some(msg) = val else {
                            warn!("peer worker msg channel closed");
                            break;
                        };
                    }
                    _ = janitor_tick.tick() => {
                        worker.scheduler
                            .backoff_janitor_enqueue_due(worker.max_active_sync_workers);
                    }
                }
            }
            worker.scheduler.clear_all_tasks();
            if let Err(err) = worker.batch_stop_docs().await {
                warn!(?err, "error stopping doc workers during shutdown");
            }
            if let Err(err) = worker.batch_stop_blobs().await {
                warn!(?err, "error stopping blob workers during shutdown");
            }
            let peer_ids_to_stop = worker
                .peer_partition_sessions
                .keys()
                .copied()
                .collect::<Vec<_>>();
            for peer_id in peer_ids_to_stop {
                worker.expected_peer_session_closes.insert(peer_id);
            }
            for (_endpoint_id, session) in worker.peer_partition_sessions.drain() {
                if let Err(err) = session.stop.stop().await {
                    warn!(?err, "error stopping peer session during shutdown");
                }
            }
            if let Err(err) = worker.task_set.stop(Duration::from_secs(5)).await {
                warn!(?err, "error waiting for task set during shutdown");
            }
            eyre::Ok(())
        }
    };
    let join_handle = tokio::task::spawn(
        async {
            if let Err(err) = fut.await {
                warn!(?err, "full sync worker task exited with error");
            }
        }
        .instrument(tracing::info_span!("FullSyncWorker task")),
    );
    Ok((
        WorkerHandle {
            msg_tx,
            events_rx: Some(events_rx),
        },
        StopToken {
            cancel_token,
            join_handle,
        },
    ))
}

struct Worker {
    big_repo: SharedBigRepo,
    local_peer_key: PeerKey,
    cancel_token: CancellationToken,
    blobs_repo: Arc<BlobsRepo>,
    progress_repo: Option<Arc<ProgressRepo>>,
    iroh_endpoint: iroh::Endpoint,
    task_set: utils_rs::AbortableJoinSet,

    partitions: HashMap<PartitionId, Partition>,

    scheduler: scheduler::Scheduler,

    // doc_ref_counts: HashMap<DocumentId, usize>,
    // peer_docs: HashMap<EndpointId, HashSet<DocumentId>>,
    doc_request_set: HashMap<DocumentId, MachineCommandState>,
    blob_requirements: HashMap<Hash, MachineCommandState>,
    peer_sessions_to_refresh: HashSet<PeerId>,

    sync_machine: SyncMachine,

    msg_tx: mpsc::UnboundedSender<Msg>,
    sync_progress_tx: mpsc::Sender<SyncProgressMsg>,
    peer_worker_msg_tx: mpsc::Sender<PeerSyncWorkerMsg>,
    events_tx: tokio::sync::broadcast::Sender<FullSyncEvent>,
    max_active_sync_workers: usize,

    known_peer_set: HashMap<BigRepoConnectionId, PeerSyncState>,
    /// Used to track sessions we have explicitly requested
    /// shutdown. Allows detection of AbnormalExits
    expected_peer_session_closes: HashSet<PeerId>,
    peer_id_by_peer_key: HashMap<PeerKey, PeerId>,
    seen_peer_keys: HashSet<PeerKey>,
    peer_partition_sessions: HashMap<PeerId, PeerPartitionSession>,
    full_sync_waiters: Vec<FullSyncWaiter>,
    sync_store: SyncStoreHandle,
    partition_store: Arc<PartitionStore>,
}

struct Partition {
    peers: HashSet<PeerId>,
    kind: PartitionKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PartitionKind {
    Blob,
    Doc,
}

struct PeerSyncState {
    peer_id: PeerId,
    peer_key: PeerKey,
    endpoint_addr: iroh::EndpointAddr,
    partitions: HashSet<PartitionId>,
    connection: am_utils_rs::repo::BigRepoConnection,
    bootstrap_ready: bool,
    live_ready: bool,
    bootstrap_synced_docs: u64,
    bootstrap_remaining_docs: u64,
    doc_pending_docs: u64,
    fully_synced_partitions: HashSet<PartitionId>,
    emitted_full_synced: bool,
}

struct PeerPartitionSession {
    stop: PeerSyncWorkerStopToken,
    partitions: HashSet<PartitionId>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct RetryState {
    pub attempt_no: usize,
    pub last_backoff: Duration,
    pub last_attempt_at: std::time::Instant,
}

#[derive(Default)]
struct MachineCommandState {
    item_id: Arc<str>,
    requested_peers: HashMap<PeerId, HashSet<PartitionId>>,
    partition_ids: HashMap<PartitionId, HashSet<PeerId>>,
}

struct ActiveDocSyncState {
    stop_token: doc_worker::DocSyncWorkerStopToken,
    retry: RetryState,
}
struct ActiveBlobSyncState {
    stop_token: blob_worker::BlobSyncWorkerStopToken,
    retry: RetryState,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct DocSyncTaskKey {
    doc_id: DocumentId,
    peer_id: PeerId,
}

impl Worker {
    fn peer_key_for_id(&self, peer_id: PeerId) -> Option<PeerKey> {
        let peer_state = self.known_peer_set.get(&peer_id)?;
        Some(Arc::clone(&peer_state.peer_key))
    }
    async fn handle_msg(&mut self, msg: Msg) -> Res<()> {
        match msg {
            Msg::SetPeer {
                endpoint_addr,
                resp,
                partitions,
                peer_key,
                connection,
            } => {
                let peer_id = connection.peer_id;
                let old_peer_id = self.peer_id_by_peer_key.get(&peer_key);
                let old_state = self
                    .known_peer_set
                    .remove(&peer_id)
                    .or_else(|| old_peer_id.and_then(|id| self.known_peer_set.remove(&id)));
                if let Some(old_state) = old_state.as_ref() {
                    if old_state.peer_key != peer_key {
                        self.peer_id_by_peer_key.remove(&old_state.peer_key);
                    }
                }
                let new_parts: Vec<_> = if let Some(old) = old_state {
                    partitions.difference(&old.partitions).cloned().collect()
                } else {
                    partitions.iter().cloned().collect()
                };
                self.known_peer_set.insert(
                    peer_id,
                    PeerSyncState {
                        partitions,
                        peer_id,
                        endpoint_addr,
                        peer_key: peer_key.clone(),
                        connection,
                        bootstrap_ready: false,
                        live_ready: false,
                        bootstrap_synced_docs: 0,
                        bootstrap_remaining_docs: 0,
                        doc_pending_docs: 0,
                        fully_synced_partitions: default(),
                        emitted_full_synced: false,
                    },
                );
                self.peer_id_by_peer_key
                    .insert(Arc::clone(&peer_key), peer_id);
                self.seen_peer_keys.insert(peer_key.clone());
                self.refresh_peer_fully_synced_state(peer_id).await?;
                resp.send(()).inspect_err(|_| warn!(ERROR_CALLER)).ok();
            }
            Msg::DelPeer { resp, peer_id } => {
                self.handle_del_peer(peer_id).await?;
                resp.send(()).inspect_err(|_| warn!(ERROR_CALLER)).ok();
            }
            Msg::DocSyncCompleted {
                doc_id,
                peer_id,
                outcome,
            } => {
                self.handle_doc_sync_completed(doc_id, peer_id, outcome)
                    .await?
            }
            Msg::DocSyncBackoff {
                doc_id,
                peer_id,
                delay,
                previous_retry_state,
            } => {
                self.handle_doc_request_backoff(doc_id, peer_id, delay, previous_retry_state)
                    .await?;
            }
            Msg::DocSyncCompleted {
                doc_id,
                peer_id,
                outcome,
            } => {
                self.handle_doc_sync_completed(doc_id, peer_id, outcome)
                    .await?;
            }
            Msg::DocSyncBackoff {
                doc_id,
                peer_id,
                delay,
                previous_retry_state,
            } => {
                self.handle_doc_request_backoff(doc_id, peer_id, delay, previous_retry_state)
                    .await?;
            }
            Msg::BlobSyncCompleted { hash, outcome } => {
                self.handle_blob_marked_synced(hash).await?;
            }
            Msg::BlobSyncBackoff {
                hash,
                delay,
                previous_retry_state,
            } => {
                self.handle_blob_request_backoff(hash, delay, previous_retry_state)
                    .await?;
            }
            Msg::GetPeerSyncSnapshot { peer_ids, resp } => {
                let snapshot = peer_ids
                    .into_iter()
                    .filter_map(|peer_id| {
                        let peer_state = self.known_peer_set.get(&peer_id)?;
                        Some((
                            peer_id,
                            PeerSyncSnapshot {
                                emitted_full_synced: peer_state.emitted_full_synced,
                                bootstrap_ready: peer_state.bootstrap_ready,
                                bootstrap_synced_docs: peer_state.bootstrap_synced_docs,
                                bootstrap_remaining_docs: peer_state.bootstrap_remaining_docs,
                                doc_pending_docs: peer_state.doc_pending_docs,
                                live_ready: peer_state.live_ready,
                                has_peer_session: self
                                    .peer_partition_sessions
                                    .contains_key(&peer_id),
                                fully_synced_partitions: peer_state.fully_synced_partitions.clone(),
                            },
                        ))
                    })
                    .collect();
                resp.send(snapshot)
                    .inspect_err(|_| warn!(ERROR_CALLER))
                    .ok();
            }
            Msg::WaitForPeersFullySynced {
                peer_ids,
                required_partitions,
                resp,
            } => {
                let remaining = peer_ids
                    .into_iter()
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .filter(|peer_id| {
                        self.known_peer_set.get(peer_id).is_none_or(|peer_state| {
                            !required_partitions.iter().all(|partition| {
                                peer_state.fully_synced_partitions.contains(partition)
                            })
                        })
                    })
                    .collect::<HashSet<_>>();
                if remaining.is_empty() {
                    resp.send(()).inspect_err(|_| warn!(ERROR_CALLER)).ok();
                } else {
                    self.full_sync_waiters.push(FullSyncWaiter {
                        remaining,
                        required_partitions,
                        resp,
                    });
                }
            }
        }
        eyre::Ok(())
    }

    /// Alert any full sync waiters if all the partition-peer
    /// items they've waiting on have been synced
    fn refresh_full_sync_waiters(&mut self) {
        if self.full_sync_waiters.is_empty() {
            return;
        }
        let mut pending = std::mem::take(&mut self.full_sync_waiters);
        for mut waiter in pending.drain(..) {
            // let required_partitions = waiter.required_partitions.clone();
            waiter.remaining.retain(|peer_id| {
                self.known_peer_set.get(&peer_id).is_none_or(|peer_state| {
                    !waiter
                        .required_partitions
                        .iter()
                        .all(|partition| peer_state.fully_synced_partitions.contains(partition))
                })
            });
            if waiter.remaining.is_empty() {
                waiter
                    .resp
                    .send(())
                    .inspect_err(|_| warn!(ERROR_CALLER))
                    .ok();
            } else {
                self.full_sync_waiters.push(waiter);
            }
        }
    }
}

mod peer {
    use super::*;

    // peer session related methods
    impl Worker {
        pub async fn batch_refresh_peer_sessions(&mut self) -> Res<()> {
            let mut double = std::mem::replace(&mut self.peer_sessions_to_refresh, default());
            for peer_id in double.drain() {
                let (peer_key, endpoint_addr) =
                    if let Some(peer_state) = self.known_peer_set.get_mut(&peer_id) {
                        let session = self.peer_partition_sessions.remove(&peer_id);
                        if let Some(session) = session {
                            self.expected_peer_session_closes.insert(peer_id);
                            session.stop.stop().await?;
                        }
                        self.sync_machine.clear_peer(&peer_state.peer_key);
                        peer_state.bootstrap_ready = false;
                        peer_state.live_ready = false;
                        peer_state.bootstrap_synced_docs = 0;
                        peer_state.bootstrap_remaining_docs = 0;
                        peer_state.doc_pending_docs = 0;
                        peer_state.fully_synced_partitions.clear();
                        peer_state.emitted_full_synced = false;
                        (
                            Arc::clone(&peer_state.peer_key),
                            peer_state.endpoint_addr.clone(),
                        )
                    } else {
                        continue;
                    };
                self.refresh_full_sync_waiters();

                let partitions: HashSet<_> = {
                    self.partitions
                        .iter()
                        .filter_map(|(part_key, part)| {
                            if part.peers.contains(&peer_id) {
                                Some(Arc::clone(part_key))
                            } else {
                                None
                            }
                        })
                        .collect()
                };

                if partitions.is_empty() {
                    return Ok(());
                }

                let rpc_client = irpc_iroh::client::<PartitionSyncRpc>(
                    self.iroh_endpoint.clone(),
                    endpoint_addr,
                    PARTITION_SYNC_ALPN,
                );
                let stop =
                    am_utils_rs::sync::peer::spawn_peer_sync_worker(SpawnPeerSyncWorkerArgs {
                        local_peer: self.local_peer_key.clone(),
                        remote_peer: peer_key,
                        rpc_client,
                        sync_store: self.sync_store.clone(),
                        target_partitions: partitions.clone(),
                        msg_tx: self.peer_worker_msg_tx.clone(),
                        task_set: &self.task_set,
                    })
                    .await?;
                self.peer_partition_sessions
                    .insert(peer_id, PeerPartitionSession { stop, partitions });
            }
            self.peer_sessions_to_refresh = double;
            Ok(())
        }
        pub async fn handle_peer_sync_worker_msg(&mut self, msg: PeerSyncWorkerMsg) -> Res<()> {
            info!(?msg, "XXX peer worker msg");
            let peer_key = match &msg {
                PeerSyncWorkerMsg::Progress { peer, .. }
                | PeerSyncWorkerMsg::SubscriptionItem { peer, .. } => peer.clone(),
                PeerSyncWorkerMsg::Event(event) => match event {
                    PeerSyncWorkerEvent::Bootstrapped { peer, .. }
                    | PeerSyncWorkerEvent::LiveReady { peer }
                    | PeerSyncWorkerEvent::AbnormalExit { peer, .. }
                    | PeerSyncWorkerEvent::NaturalDeath { peer } => peer.clone(),
                },
            };
            let Some(peer_id) = self.peer_id_by_peer_key.get(&peer_key).copied() else {
                assert!(self.seen_peer_keys.contains(&peer_key), "fishy");
                warn!(%peer_key, "peer worker message for disconnected peer");
                return Ok(());
            };
            match msg {
                PeerSyncWorkerMsg::SubscriptionItem { peer, item } => {
                    let commands = self.sync_machine.on_subscription_item(peer, item).await?;
                    self.dispatch_sync_commands(commands).await?;
                }
                PeerSyncWorkerMsg::Event(event) => match event {
                    PeerSyncWorkerEvent::Bootstrapped {
                        peer,
                        partition_count,
                    } => {
                        debug!(?peer, partition_count, "peer sync worker bootstrapped");
                        self.emit_peer_progress_status(
                            peer_id,
                            ProgressUpdateDeets::Status {
                                severity: ProgressSeverity::Info,
                                message: format!("bootstrapped with {partition_count} partitions"),
                            },
                        )
                        .await?;
                        let peer_state = self
                            .known_peer_set
                            .get_mut(&peer_id)
                            .expect(ERROR_UNRECONIZED);
                        peer_state.bootstrap_ready = true;
                        peer_state.bootstrap_remaining_docs = 0;
                        self.refresh_peer_fully_synced_state(peer_id).await?;
                    }
                    PeerSyncWorkerEvent::LiveReady { peer } => {
                        debug!(?peer, "peer sync worker entered live mode");
                        self.emit_peer_progress_status(
                            peer_id,
                            ProgressUpdateDeets::Completed {
                                state: ProgressFinalState::Succeeded,
                                message: Some("peer entered live mode".to_string()),
                            },
                        )
                        .await?;
                        let peer_state = self
                            .known_peer_set
                            .get_mut(&peer_id)
                            .expect(ERROR_UNRECONIZED);
                        peer_state.live_ready = true;
                        self.refresh_peer_fully_synced_state(peer_id).await?;
                    }
                    PeerSyncWorkerEvent::AbnormalExit { peer, reason } => {
                        if self.cancel_token.is_cancelled() {
                            debug!(?peer, reason = %reason, "peer sync worker exited during shutdown");
                            return Ok(());
                        }
                        if self.expected_peer_session_closes.remove(&peer_id) {
                            debug!(?peer, reason = %reason, "peer sync worker exited after an expected stop");
                            return Ok(());
                        }
                        let peer_known = self.known_peer_set.contains_key(&peer_id);
                        if !peer_known {
                            debug!(?peer, reason = %reason, "peer sync worker exited after peer was already removed");
                            return Ok(());
                        }
                        match &reason {
                            PeerSyncWorkerExit::SubscriptionStreamClosed => {
                                warn!(?peer, reason = %reason, "peer sync worker exited while peer was still active");
                            }
                            _ => {
                                warn!(?peer, reason = %reason, "peer sync worker exited abnormally");
                            }
                        }
                        self.emit_peer_progress_status(
                            peer_id,
                            ProgressUpdateDeets::Completed {
                                state: ProgressFinalState::Failed,
                                message: Some(format!("peer worker abnormal exit: {reason}")),
                            },
                        )
                        .await?;
                        self.emit_stale_peer(peer_key).await?;
                        self.peer_sessions_to_refresh.insert(peer_id);
                    }
                    PeerSyncWorkerEvent::NaturalDeath { peer } => {
                        if self.cancel_token.is_cancelled() {
                            return Ok(());
                        }
                        if self.expected_peer_session_closes.remove(&peer_id) {
                            return Ok(());
                        }
                        let peer_known = self.known_peer_set.contains_key(&peer_id);
                        if peer_known {
                            return Ok(());
                        }

                        // FIXME: I'm unsure what to clean up at this point

                        self.emit_peer_progress_status(
                            peer_id,
                            ProgressUpdateDeets::Completed {
                                state: ProgressFinalState::Succeeded,
                                message: Some(format!("peer worker natural exit")),
                            },
                        )
                        .await?;
                    }
                },
                PeerSyncWorkerMsg::Progress { event, .. } => match event {
                    PeerSyncProgressEvent::PhaseStarted { phase } => {
                        debug!(peer_id = ?peer_id, phase, "peer sync phase started");
                        self.emit_peer_progress_status(
                            peer_id,
                            ProgressUpdateDeets::Status {
                                severity: ProgressSeverity::Info,
                                message: format!("phase started: {phase}"),
                            },
                        )
                        .await?;
                    }
                    PeerSyncProgressEvent::PhaseFinished { phase, elapsed } => {
                        debug!(peer_id = ?peer_id, phase, elapsed_ms = elapsed.as_millis(), "peer sync phase finished");
                        self.emit_peer_progress_status(
                            peer_id,
                            ProgressUpdateDeets::Status {
                                severity: ProgressSeverity::Info,
                                message: format!("phase finished: {phase} ({:?})", elapsed),
                            },
                        )
                        .await?;
                    }
                },
            }
            Ok(())
        }

        pub async fn handle_del_peer(&mut self, peer_id: PeerId) -> Res<()> {
            let Some(state) = self.known_peer_set.remove(&peer_id) else {
                return Ok(());
            };
            self.peer_id_by_peer_key.remove(&state.peer_key);
            state
                .connection
                .stop()
                .await
                .inspect_err(|err| error!("error on disconnection from peer {peer_id:?}: {err}"))
                .ok();
            // rmeove the peer from the scheduler
            {
                let mut to_remove_docs = Vec::new();
                let mut to_remove_blobs = Vec::new();
                for (doc_id, state) in &mut self.doc_request_set {
                    if let Some(parts) = state.requested_peers.remove(&peer_id) {
                        for part_id in parts {
                            let remove = {
                                let parts = state
                                    .partition_ids
                                    .get_mut(&part_id)
                                    .expect(ERROR_IMPOSSIBLE);
                                parts.remove(&peer_id);
                                parts.is_empty()
                            };
                            if remove {
                                state.partition_ids.remove(&part_id);
                            }
                        }
                    }
                    let task_key = DocSyncTaskKey {
                        doc_id: doc_id.clone(),
                        peer_id,
                    };
                    self.scheduler.enqueue_stop_doc(&task_key);
                    if state.requested_peers.is_empty() {
                        to_remove_docs.push(doc_id.clone());
                    }
                }
                for (&hash, state) in &mut self.blob_requirements {
                    if let Some(parts) = state.requested_peers.remove(&peer_id) {
                        for part_id in parts {
                            let remove = {
                                let parts = state
                                    .partition_ids
                                    .get_mut(&part_id)
                                    .expect(ERROR_IMPOSSIBLE);
                                parts.remove(&peer_id);
                                parts.is_empty()
                            };
                            if remove {
                                state.partition_ids.remove(&part_id);
                            }
                        }
                    }
                    if state.requested_peers.is_empty() {
                        // we only stop the blob worker if there are no
                        // peers since blobs workers are multi peer
                        self.scheduler.enqueue_stop_blob(hash);
                        to_remove_blobs.push(hash);
                    }
                }
                assert!(
                    self.scheduler.doc_task_keys_for_peer(peer_id).is_empty(),
                    "fishy"
                );
                for id in to_remove_docs {
                    self.doc_request_set.remove(&id);
                }
                for id in to_remove_blobs {
                    self.blob_requirements.remove(&id);
                }
            }
            // stop the peer session worker
            {
                self.expected_peer_session_closes.insert(peer_id);
                let session = self.peer_partition_sessions.get(&peer_id);
                if let Some(session) = session {
                    session.stop.cancel();
                }
                self.sync_machine.clear_peer(&state.peer_key);
            }

            self.peer_id_by_peer_key
                .retain(|_peer_key, cached_endpoint| *cached_endpoint != peer_id);
            self.refresh_full_sync_waiters();
            Ok(())
        }

        // FIXME: this is broken, the sync machine should be the prime
        // source of this information
        #[tracing::instrument(skip(self))]
        pub async fn refresh_peer_fully_synced_state(&mut self, peer_id: PeerId) -> Res<()> {
            let endpoint_has_doc_work = self.scheduler.endpoint_has_doc_work(peer_id);
            let peer_state = self
                .known_peer_set
                .get_mut(&peer_id)
                .expect(ERROR_UNRECONIZED);
            let is_fully_synced = peer_state.bootstrap_ready
                && peer_state.bootstrap_remaining_docs == 0
                && peer_state.live_ready
                && !endpoint_has_doc_work
                && peer_state.doc_pending_docs == 0
                && peer_state.fully_synced_partitions == peer_state.partitions;
            debug!(
                bootstrap_ready = peer_state.bootstrap_ready,
                bootstrap_synced_docs = peer_state.bootstrap_synced_docs,
                bootstrap_remaining_docs = peer_state.bootstrap_remaining_docs,
                doc_pending_docs = peer_state.doc_pending_docs,
                endpoint_has_doc_work,
                emitted_full_synced = peer_state.emitted_full_synced,
                live_ready = peer_state.live_ready,
                partition_sync_count = peer_state.fully_synced_partitions.len(),
                is_fully_synced,
                "refresh_peer_fully_synced_state evaluated"
            );
            let peer_key = Arc::clone(&peer_state.peer_key);
            if is_fully_synced {
                if !peer_state.emitted_full_synced {
                    peer_state.emitted_full_synced = true;
                    let doc_count = peer_state.bootstrap_synced_docs as usize;
                    self.emit_peer_progress_status(
                        peer_id,
                        ProgressUpdateDeets::Status {
                            severity: ProgressSeverity::Info,
                            message: format!("peer fully synced ({doc_count} docs converged)"),
                        },
                    )
                    .await?;
                    self.emit_full_sync_event(FullSyncEvent::PeerFullSynced {
                        peer_key,
                        doc_count,
                    })
                    .await?;
                }
            } else if peer_state.emitted_full_synced {
                peer_state.emitted_full_synced = false;
                self.emit_peer_progress_status(
                    peer_id,
                    ProgressUpdateDeets::Status {
                        severity: ProgressSeverity::Warn,
                        message: "peer no longer fully synced".to_string(),
                    },
                )
                .await?;
                self.emit_full_sync_event(FullSyncEvent::StalePeer { peer_key })
                    .await?;
            }
            self.refresh_full_sync_waiters();
            Ok(())
        }

        async fn emit_stale_peer(&mut self, peer_key: PeerKey) -> Res<()> {
            let peer_id = self
                .peer_id_by_peer_key
                .get(&peer_key)
                .expect(ERROR_UNRECONIZED);
            let peer_state = self
                .known_peer_set
                .get_mut(peer_id)
                .expect(ERROR_UNRECONIZED);
            if peer_state.emitted_full_synced {
                peer_state.emitted_full_synced = false;
                self.emit_full_sync_event(FullSyncEvent::StalePeer { peer_key })
                    .await?;
            }
            Ok(())
        }
    }
}

mod machine {
    use super::*;

    impl Worker {
        fn kind_from_part_ids(&self, part_ids: &[PartitionId]) -> Res<PartitionKind> {
            let mut kind = None;
            for id in part_ids {
                let part = self
                    .partitions
                    .get(id)
                    .expect("command sent by unknown partition");
                if kind.is_some() && Some(part.kind) != kind {
                    eyre::bail!(
                        "item requested from different kinds of partitions: {:?} != {kind:?}",
                        part.kind
                    );
                }
                kind = Some(part.kind);
            }
            Ok(kind.expect(ERROR_IMPOSSIBLE))
        }
        pub async fn dispatch_sync_commands(
            &mut self,
            commands: Vec<SyncMachineCommand>,
        ) -> Res<()> {
            info!(?commands, "XXX");
            let mut pending = commands;
            while let Some(command) = pending.pop() {
                pending.extend(self.dispatch_sync_command(command).await?);
            }
            Ok(())
        }

        #[tracing::instrument(skip(self))]
        async fn dispatch_sync_command(
            &mut self,
            command: SyncMachineCommand,
        ) -> Res<Vec<SyncMachineCommand>> {
            let command_peer = match &command {
                SyncMachineCommand::ItemNewSync { key, .. }
                | SyncMachineCommand::ItemChangeSync { key, .. }
                | SyncMachineCommand::ItemDeleteSync { key, .. } => Arc::clone(&key.peer),
            };
            let peer_id = *self.peer_id_by_peer_key.get(&command_peer).ok_or_else(|| {
                eyre::eyre!("missing peer id for sync command peer {}", command_peer)
            })?;
            match command {
                SyncMachineCommand::ItemNewSync {
                    key,
                    partition_hints,
                }
                | SyncMachineCommand::ItemChangeSync {
                    key,
                    partition_hints,
                } => match self.kind_from_part_ids(&partition_hints)? {
                    PartitionKind::Blob => {
                        let iroh_hash = crate::blobs::daybook_hash_to_iroh_hash(&key.item_id)
                            .wrap_err("unable to parse hash from remote blob partition")?;
                        self.enqueue_blob_sync(peer_id, &partition_hints, iroh_hash)
                            .await?;
                    }
                    PartitionKind::Doc => {
                        let doc_id = key
                            .item_id
                            .parse::<DocumentId>()
                            .map_err(|err| ferr!("invalid item id '{}': {err}", key.item_id))?;
                        self.enqueue_doc_sync(peer_id, &partition_hints, doc_id)
                            .await?;
                    }
                },
                // FIXME: delete is not supported so we just
                // ack deletes immediately
                SyncMachineCommand::ItemDeleteSync {
                    key,
                    partition_hints,
                } => {
                    match self.kind_from_part_ids(&partition_hints)? {
                        PartitionKind::Blob => {
                            let iroh_hash =
                                crate::blobs::daybook_hash_to_iroh_hash(&key.item_id)
                                    .wrap_err("unable to parse hash from remote blob partition")?;
                            self.enqueue_blob_delete(peer_id, iroh_hash).await?;
                        }
                        PartitionKind::Doc => {
                            let doc_id = key
                                .item_id
                                .parse::<DocumentId>()
                                .map_err(|err| ferr!("invalid item id '{}': {err}", key.item_id))?;
                            self.enqueue_doc_deleted(peer_id, doc_id).await?;
                        }
                    }
                    let commands = self
                        .sync_machine
                        .on_item_sync_completed(SyncCompletion::DeletedMember {
                            peer: key.peer.clone(),
                            item_id: key.item_id.clone(),
                        })
                        .await?;
                    return Ok(commands);
                }
            }
            Ok(Vec::new())
        }

        #[tracing::instrument(skip(self))]
        async fn enqueue_doc_sync(
            &mut self,
            peer_id: PeerId,
            partition_ids: &[PartitionId],
            doc_id: DocumentId,
        ) -> Res<()> {
            let state = self.doc_request_set.entry(doc_id.clone()).or_default();
            for part_id in partition_ids {
                state
                    .partition_ids
                    .entry(Arc::clone(&part_id))
                    .or_default()
                    .insert(peer_id);
            }
            // NOTE: we replace old req partition set since the peer machine gives us
            // the full req part set
            let old = state
                .requested_peers
                .insert(peer_id, partition_ids.iter().map(Arc::clone).collect());
            if old.is_none() {
                if let Some(peer_state) = self.known_peer_set.get_mut(&peer_id) {
                    peer_state.doc_pending_docs = peer_state.doc_pending_docs.saturating_add(1);
                    self.refresh_peer_fully_synced_state(peer_id).await?;
                }
            }
            self.scheduler.set_doc_pending_now(DocSyncTaskKey {
                doc_id: doc_id.clone(),
                peer_id,
            });
            Ok(())
        }

        #[tracing::instrument(skip(self))]
        async fn enqueue_doc_deleted(&mut self, peer_id: PeerId, doc_id: DocumentId) -> Res<()> {
            let mut had_peer = false;
            let mut clear_request = false;
            if let Some(state) = self.doc_request_set.get_mut(&doc_id) {
                if let Some(parts) = state.requested_peers.remove(&peer_id) {
                    had_peer = true;
                    for part_id in parts {
                        let remove = {
                            let parts = state
                                .partition_ids
                                .get_mut(&part_id)
                                .expect(ERROR_IMPOSSIBLE);
                            parts.remove(&peer_id);
                            parts.is_empty()
                        };
                        if remove {
                            state.partition_ids.remove(&part_id);
                        }
                    }
                }
                clear_request = state.requested_peers.is_empty();
            };
            if had_peer {
                if let Some(peer_state) = self.known_peer_set.get_mut(&peer_id) {
                    peer_state.doc_pending_docs = peer_state.doc_pending_docs.saturating_sub(1);
                    self.refresh_peer_fully_synced_state(peer_id).await?;
                }
                let task_key = DocSyncTaskKey {
                    doc_id: doc_id.clone(),
                    peer_id,
                };
                self.scheduler.enqueue_stop_doc(&task_key);
            }
            if clear_request {
                debug_assert!(
                    self.scheduler.doc_task_keys_for_doc(&doc_id).is_empty(),
                    "fishy"
                );
                self.doc_request_set.remove(&doc_id);
            }
            Ok(())
        }

        async fn enqueue_blob_sync(
            &mut self,
            peer_id: PeerId,
            partition_ids: &[PartitionId],
            hash: Hash,
        ) -> Res<()> {
            let state = self.blob_requirements.entry(hash).or_default();
            for part_id in partition_ids {
                state
                    .partition_ids
                    .entry(Arc::clone(&part_id))
                    .or_default()
                    .insert(peer_id);
            }
            // NOTE: we replace old req partition set since the peer machine gives us
            // the full req part set
            let old = state
                .requested_peers
                .insert(peer_id, partition_ids.iter().map(Arc::clone).collect());
            if old.is_none() {
                if let Some(peer_state) = self.known_peer_set.get_mut(&peer_id) {
                    peer_state.doc_pending_docs = peer_state.doc_pending_docs.saturating_add(1);
                    self.refresh_peer_fully_synced_state(peer_id).await?;
                }
            }
            self.scheduler.set_blob_pending_now(hash);
            Ok(())
        }

        async fn enqueue_blob_delete(&mut self, peer_id: PeerId, hash: Hash) -> Res<()> {
            let mut had_peer = false;
            let mut clear_request = false;
            if let Some(state) = self.blob_requirements.get_mut(&hash) {
                if let Some(parts) = state.requested_peers.remove(&peer_id) {
                    had_peer = true;
                    for part_id in parts {
                        let remove = {
                            let parts = state
                                .partition_ids
                                .get_mut(&part_id)
                                .expect(ERROR_IMPOSSIBLE);
                            parts.remove(&peer_id);
                            parts.is_empty()
                        };
                        if remove {
                            state.partition_ids.remove(&part_id);
                        }
                    }
                }
                clear_request = state.requested_peers.is_empty();
            };
            if had_peer {
                if let Some(peer_state) = self.known_peer_set.get_mut(&peer_id) {
                    self.refresh_peer_fully_synced_state(peer_id).await?;
                }
            }
            // we only stop the worker if there are no peers left on it
            // unlike the doc workers which are per peer
            if clear_request {
                self.blob_requirements.remove(&hash);
                self.scheduler.enqueue_stop_blob(hash);
            }
            Ok(())
        }
    }
}

mod docs {
    use super::*;

    // Docs related methods
    impl Worker {
        pub async fn batch_boot_docs(&mut self) -> Res<()> {
            let mut budget = self
                .scheduler
                .available_doc_boot_budget(self.max_active_sync_workers);
            if budget == 0 {
                return Ok(());
            }
            let doc_tasks = self.scheduler.drain_queued_docs(budget);
            for task_key in doc_tasks {
                if self.scheduler.is_doc_active(&task_key) {
                    continue;
                }
                let prior_pending = self.scheduler.pending_doc_state(&task_key);
                let now = std::time::Instant::now();
                let retry = RetryState {
                    attempt_no: prior_pending.as_ref().map_or(0, |prior| prior.attempt_no),
                    last_backoff: prior_pending
                        .as_ref()
                        .map_or(Duration::from_millis(0), |prior| prior.last_backoff),
                    last_attempt_at: prior_pending
                        .as_ref()
                        .map_or(now, |prior| prior.last_attempt_at),
                };
                debug!(
                    endpoint_id = ?task_key.peer_id,
                    %task_key.doc_id,
                    attempt_no = retry.attempt_no,
                    "booting doc sync worker"
                );
                match self.boot_doc_sync_worker(task_key.clone(), retry).await? {
                    Some(active) => {
                        debug!(
                            endpoint_id = ?task_key.peer_id,
                            %task_key.doc_id,
                            "doc sync worker spawned"
                        );
                        budget = budget.saturating_sub(1);
                        self.scheduler.activate_doc(task_key, active);
                    }
                    None => {
                        debug!(
                            endpoint_id = ?task_key.peer_id,
                            %task_key.doc_id,
                            "doc sync worker deferred"
                        );
                        let now = std::time::Instant::now();
                        let pending = scheduler::PendingTaskState {
                            attempt_no: retry.attempt_no,
                            last_backoff: retry.last_backoff,
                            last_attempt_at: now,
                            due_at: now + Duration::from_millis(500),
                        };
                        self.scheduler.set_doc_backoff(task_key, pending);
                    }
                }
            }
            Ok(())
        }

        pub async fn boot_doc_sync_worker(
            &self,
            task_key: DocSyncTaskKey,
            retry: RetryState,
        ) -> Res<Option<ActiveDocSyncState>> {
            let doc_id = task_key.doc_id;
            let peer_id = task_key.peer_id;
            let Some(sync_state) = self.doc_request_set.get(&doc_id) else {
                return Ok(None);
            };
            if !sync_state.requested_peers.contains_key(&peer_id) {
                // FIXME: can this legitimately happen? wouldn't
                // a removed
                return Ok(None);
            }
            let Some(peer_state) = self.known_peer_set.get(&peer_id) else {
                return Ok(None);
            };
            let connection = peer_state.connection.clone();
            let stop_token = doc_worker::spawn_doc_sync_worker(
                doc_id,
                peer_id,
                connection,
                self.iroh_endpoint.clone(),
                Arc::clone(&self.big_repo),
                self.msg_tx.clone(),
                retry,
                &self.task_set,
            )?;
            Ok(Some(ActiveDocSyncState { stop_token, retry }))
        }

        pub async fn batch_stop_docs(&mut self) -> Res<()> {
            for task_key in self.scheduler.drain_stop_doc_queue() {
                if let Some(active) = self.scheduler.clear_doc_task(&task_key) {
                    active.stop_token.stop().await;
                }
                self.scheduler.clear_doc_task(&task_key);
            }
            Ok(())
        }

        #[tracing::instrument(skip(self))]
        pub async fn handle_doc_sync_completed(
            &mut self,
            doc_id: DocumentId,
            peer_id: PeerId,
            outcome: doc_worker::SyncDocOutcome,
        ) -> Res<()> {
            let task_key = DocSyncTaskKey {
                doc_id: doc_id.clone(),
                peer_id,
            };
            let Some(active) = self.scheduler.clear_doc_task(&task_key) else {
                warn!(
                    ?doc_id,
                    ?peer_id,
                    ?outcome,
                    "doc sync completed for unrecognized task (peer may have exited abnormally)"
                );
                return Ok(());
            };

            // FIXME: introduce worker ids so that we can enqueu
            // stopping the succesful work in a barch manner without
            // accidentally stopping any new jobs on the same task_key
            active.stop_token.stop().await;

            if let Some(sync_state) = self.doc_request_set.get_mut(&doc_id) {
                if sync_state.requested_peers.remove(&peer_id).is_some() {
                    if let Some(peer_state) = self.known_peer_set.get_mut(&peer_id) {
                        peer_state.doc_pending_docs = peer_state.doc_pending_docs.saturating_sub(1);
                    }
                    drop(sync_state);
                    self.refresh_peer_fully_synced_state(peer_id).await?;
                }
            }
            let peer_key = self.peer_key_for_id(peer_id).expect(ERROR_UNRECONIZED);
            let (result, update) = match outcome {
                doc_worker::SyncDocOutcome::Synced => (
                    SyncCompletion::ChangedItem {
                        peer: Arc::clone(&peer_key),
                        item_id: doc_id.to_string().into(),
                    },
                    Some(ProgressUpdateDeets::Status {
                        severity: ProgressSeverity::Info,
                        message: format!("doc sync completed: {doc_id}"),
                    }),
                ),
                doc_worker::SyncDocOutcome::Imported { heads } => (
                    SyncCompletion::AddedMember {
                        peer: Arc::clone(&peer_key),
                        item_id: doc_id.to_string().into(),
                        item_payload: json!({
                            "heads": heads
                        }),
                    },
                    Some(ProgressUpdateDeets::Status {
                        severity: ProgressSeverity::Info,
                        message: format!("doc import completed: {doc_id}"),
                    }),
                ),
                doc_worker::SyncDocOutcome::LocalPresent => (
                    SyncCompletion::Noop {
                        peer: Arc::clone(&peer_key),
                        item_id: doc_id.to_string().into(),
                    },
                    None,
                ),
            };
            let commands = self.sync_machine.on_item_sync_completed(result).await?;
            self.dispatch_sync_commands(commands).await?;
            if let Some(update) = update {
                self.emit_full_sync_event(FullSyncEvent::DocSyncedWithPeer { peer_key, doc_id })
                    .await?;
                self.emit_peer_progress_status(peer_id, update).await?;
            }

            let requested_peers_empty = self
                .doc_request_set
                .get(&doc_id)
                .map(|state| state.requested_peers.is_empty())
                .unwrap_or(true);
            if requested_peers_empty {
                self.doc_request_set.remove(&doc_id);
                assert!(
                    self.scheduler.doc_task_keys_for_doc(&doc_id).is_empty(),
                    "fishy"
                );
            } else if let Some(sync_state) = self.doc_request_set.get(&doc_id) {
                // enqueue any pending jobs on the peer in case they'd
                // gone into backoff due to issues
                for peer_id in sync_state.requested_peers.keys().copied() {
                    self.scheduler.enqueue_start_doc(DocSyncTaskKey {
                        doc_id: doc_id.clone(),
                        peer_id,
                    });
                }
            }
            Ok(())
        }

        #[tracing::instrument(skip(self))]
        pub async fn handle_doc_request_backoff(
            &mut self,
            doc_id: DocumentId,
            peer_id: PeerId,
            delay: Duration,
            previous_retry_state: RetryState,
        ) -> Res<()> {
            let task_key = DocSyncTaskKey {
                doc_id: doc_id.clone(),
                peer_id,
            };
            let Some(active) = self.scheduler.clear_doc_task(&task_key) else {
                warn!(
                    ?doc_id,
                    ?peer_id,
                    "doc backoff for unrecognized task (peer may have exited abnormally)"
                );
                return Ok(());
            };

            // FIXME: introduce worker ids so that we can enqueu
            // stopping the succesful work in a barch manner without
            // accidentally stopping any new jobs on the same task_key
            active.stop_token.stop().await;
            if self
                .doc_request_set
                .get(&doc_id)
                .is_none_or(|state| !state.requested_peers.contains_key(&peer_id))
            {
                return Ok(());
            }
            let now = std::time::Instant::now();
            let delay = delay.min(Duration::from_secs(600));
            let backoff = next_backoff_delay(previous_retry_state.last_backoff, delay);
            let pending = scheduler::PendingTaskState {
                attempt_no: previous_retry_state.attempt_no + 1,
                last_backoff: backoff,
                last_attempt_at: now,
                due_at: now + backoff,
            };
            self.scheduler.set_doc_backoff(task_key, pending);
            Ok(())
        }
    }
}

mod blobs {
    use super::*;

    // Blobs related methods
    impl Worker {
        pub async fn batch_boot_blobs(&mut self) -> Res<()> {
            let mut budget = self
                .scheduler
                .available_blob_boot_budget(self.max_active_sync_workers);
            if budget == 0 {
                return Ok(());
            }
            for hash in self.scheduler.drain_queued_blobs(budget) {
                if self.scheduler.is_blob_active(&hash) {
                    continue;
                }
                let Some(sync_state) = self.blob_requirements.get(&hash) else {
                    // FIXME: shouldn't this task be removed instead??
                    self.scheduler.enqueue_start_blob(hash);
                    continue;
                };
                let now = std::time::Instant::now();
                let retry = {
                    let prior_pending = self.scheduler.pending_blob_state(hash);
                    RetryState {
                        attempt_no: prior_pending.as_ref().map_or(0, |prior| prior.attempt_no),
                        last_backoff: prior_pending
                            .as_ref()
                            .map_or(Duration::from_millis(0), |prior| prior.last_backoff),
                        last_attempt_at: prior_pending
                            .as_ref()
                            .map_or(now, |prior| prior.last_attempt_at),
                    }
                };
                let peers: Vec<_> = sync_state.requested_peers.keys().copied().collect();
                if peers.is_empty() {
                    // FIXME: shouldn't this task be deferred instead??
                    self.scheduler.enqueue_start_blob(hash);
                    let pending = scheduler::PendingTaskState {
                        attempt_no: retry.attempt_no,
                        last_backoff: retry.last_backoff,
                        last_attempt_at: now,
                        due_at: now + Duration::from_millis(500),
                    };
                    self.scheduler.set_blob_backoff(hash, pending);
                    continue;
                }
                let stop_token = blob_worker::spawn_blob_sync_worker(
                    hash.clone(),
                    Arc::clone(&sync_state.item_id),
                    peers,
                    self.msg_tx.clone(),
                    self.sync_progress_tx.clone(),
                    Arc::clone(&self.blobs_repo),
                    self.iroh_endpoint.clone(),
                    retry,
                    &self.task_set,
                )?;
                self.scheduler
                    .activate_blob(hash, ActiveBlobSyncState { stop_token, retry });
                budget = budget.saturating_sub(1);
            }
            Ok(())
        }

        pub async fn batch_stop_blobs(&mut self) -> Res<()> {
            for hash in self.scheduler.drain_stop_blob_queue() {
                if let Some(active) = self.scheduler.clear_blob_task(hash) {
                    active.stop_token.stop().await;
                }
            }

            Ok(())
        }

        pub async fn handle_blob_marked_synced(&mut self, hash: Hash) -> Res<()> {
            let Some(active) = self.scheduler.clear_blob_task(hash) else {
                warn!("blob sync completed for unrecognized task");
                return Ok(());
            };
            active.stop_token.stop().await;
            // Blobs are identical with across peers so we just
            // ack all peer jobs
            if let Some(sync_state) = self.blob_requirements.remove(&hash) {
                for &peer_id in sync_state.requested_peers.keys() {
                    let peer_key = self.peer_key_for_id(peer_id).expect(ERROR_UNRECONIZED);
                    let commands = self
                        .sync_machine
                        .on_item_sync_completed(SyncCompletion::AddedMember {
                            peer: peer_key,
                            item_id: Arc::clone(&sync_state.item_id),
                            item_payload: json!({}),
                        })
                        .await?;
                    self.dispatch_sync_commands(commands).await?;
                    self.refresh_peer_fully_synced_state(peer_id).await?;
                }
                self.emit_full_sync_event(FullSyncEvent::BlobSynced {
                    hash: Arc::clone(&sync_state.item_id),
                })
                .await?;
            }
            Ok(())
        }

        pub async fn handle_blob_request_backoff(
            &mut self,
            hash: Hash,
            delay: Duration,
            previous_retry_state: RetryState,
        ) -> Res<()> {
            let Some(active) = self.scheduler.clear_blob_task(hash) else {
                warn!("blob backoff for unrecognized task");
                return Ok(());
            };
            active.stop_token.stop().await;

            let Some(sync_state) = self.blob_requirements.get(&hash) else {
                return Ok(());
            };
            if sync_state.requested_peers.is_empty() {
                return Ok(());
            }
            let now = std::time::Instant::now();
            let delay = delay.min(Duration::from_secs(600));
            let backoff = next_backoff_delay(previous_retry_state.last_backoff, delay);
            let pending = scheduler::PendingTaskState {
                attempt_no: previous_retry_state.attempt_no + 1,
                last_backoff: backoff,
                last_attempt_at: now,
                due_at: now + backoff,
            };
            let attempt_no = pending.attempt_no;
            let item_id = Arc::clone(&sync_state.item_id);
            drop(sync_state);
            self.scheduler.set_blob_backoff(hash, pending);
            self.emit_full_sync_event(FullSyncEvent::BlobSyncBackoff {
                hash: item_id,
                delay: backoff,
                attempt_no,
            })
            .await?;
            Ok(())
        }
    }
}

// generic progress
impl Worker {
    async fn emit_full_sync_event(&self, event: FullSyncEvent) -> Res<()> {
        debug!(event = ?event, "emitting full sync event");
        if self.events_tx.send(event).is_err() && !self.cancel_token.is_cancelled() {
            trace!("full sync event receiver dropped");
        }
        Ok(())
    }

    fn has_progress_repo(&self) -> bool {
        self.progress_repo.is_some()
    }
}

mod progress {
    use super::*;

    // progress related methods
    impl Worker {
        pub async fn emit_peer_progress_status(
            &self,
            peer_id: PeerId,
            deets: ProgressUpdateDeets,
        ) -> Res<()> {
            if !self.has_progress_repo() {
                return Ok(());
            }
            let task_id = format!("sync/full/peer/{peer_id}");
            self.emit_progress_task(
                task_id.clone(),
                vec![
                    "/type/sync".to_string(),
                    "/sync/full".to_string(),
                    "/kind/peer_sync".to_string(),
                    format!("/peer/{peer_id}"),
                ],
            )
            .await?;
            self.emit_progress_update(&task_id, deets, Some(format!("peer sync {peer_id}")))
                .await?;
            Ok(())
        }
        pub async fn handle_sync_progress_batch(
            &mut self,
            buffer: &mut Vec<SyncProgressMsg>,
        ) -> Res<()> {
            let batch_len = buffer.len();
            if batch_len == 0 {
                return Ok(());
            }
            if !self.has_progress_repo() {
                return Ok(());
            }
            debug!(batch_len, "handling sync progress batch");
            for msg in buffer.drain(..) {
                match msg {
                    SyncProgressMsg::BlobDownloadStarted { hash, .. } => {
                        self.emit_full_sync_event(FullSyncEvent::BlobDownloadStarted { hash })
                            .await?;
                    }
                    SyncProgressMsg::BlobWorkerStarted { hash } => {
                        // FIXME:
                    }
                    SyncProgressMsg::BlobDownloadProgress {
                        hash,
                        done_counter: done,
                        ..
                    } => {
                        // FIXME: the blob progress wirings have been removed
                    }
                    SyncProgressMsg::BlobMaterializeStarted { hash } => {
                        // FIXME:
                    }
                    SyncProgressMsg::BlobDownloadFinished { hash, error } => {
                        let success = error.is_none();
                        if let Some(err) = error.as_ref() {
                            warn!(%hash, ?err, "blob download finished with error");
                        }
                        self.emit_full_sync_event(FullSyncEvent::BlobDownloadFinished {
                            hash,
                            success,
                        })
                        .await?;
                    }
                }
            }

            debug!(batch_len, "drained sync progress batch");
            Ok(())
        }

        async fn emit_progress_task(&self, id: String, tags: Vec<String>) -> Res<()> {
            let Some(progress_repo) = &self.progress_repo else {
                return Ok(());
            };
            progress_repo
                .upsert_task(CreateProgressTaskArgs {
                    id,
                    tags,
                    retention: ProgressRetentionPolicy::AutoDismissAfter { seconds: 60 },
                })
                .await
        }

        async fn emit_progress_update(
            &self,
            task_id: &str,
            deets: ProgressUpdateDeets,
            title: Option<String>,
        ) -> Res<()> {
            let Some(progress_repo) = &self.progress_repo else {
                return Ok(());
            };
            progress_repo
                .add_update(
                    task_id,
                    ProgressUpdate {
                        at: jiff::Timestamp::now(),
                        title,
                        deets,
                    },
                )
                .await
        }
    }
}
fn next_backoff_delay(previous: Duration, minimum: Duration) -> Duration {
    if previous.is_zero() {
        minimum.min(Duration::from_secs(600))
    } else {
        previous
            .saturating_mul(2)
            .max(minimum)
            .min(Duration::from_secs(600))
    }
}
