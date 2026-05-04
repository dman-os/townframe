// FIXME; split the Worker into multiple states

use crate::interlude::*;

use am_utils_rs::{
    repo::PeerId,
    sync::{
        machine::{SyncCompletion, SyncMachine, SyncMachineCommand},
        peer::{
            PeerSyncProgressEvent, PeerSyncWorkerEvent, PeerSyncWorkerExit, PeerSyncWorkerMsg,
            PeerSyncWorkerStopToken, SpawnPeerSyncWorkerArgs,
        },
        protocol::{PartitionId, PartitionSyncRpc, PeerKey, SubscriptionItem},
        store::SyncStoreHandle,
    },
};
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
        partition: PartitionKey,
    },
    DocSyncedWithPeer {
        peer_key: PeerKey,
        doc_id: DocumentId,
    },
    BlobSynced {
        hash: String,
        peer_key: Option<PeerKey>,
    },
    BlobDownloadStarted {
        peer_key: PeerKey,
        partition: PartitionKey,
        hash: String,
    },
    BlobDownloadFinished {
        peer_key: PeerKey,
        partition: PartitionKey,
        hash: String,
        success: bool,
    },
    BlobSyncBackoff {
        hash: String,
        delay: Duration,
        attempt_no: usize,
    },
    StalePeer {
        peer_key: PeerKey,
    },
}

enum Msg {
    SetPeer {
        endpoint_addr: iroh::EndpointAddr,
        partitions: HashSet<PartitionKey>,
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
    DocSyncCompleted {
        doc_id: DocumentId,
        peer_id: PeerId,
        outcome: am_utils_rs::repo::SyncDocOutcome,
    },
    DocSyncBackoff {
        doc_id: DocumentId,
        peer_id: PeerId,
        delay: Duration,
        previous_attempt_no: usize,
        previous_backoff: Duration,
        previous_attempt_at: std::time::Instant,
    },
    DocSyncMissingLocal {
        doc_id: DocumentId,
    },
    ImportDocCompleted {
        doc_id: DocumentId,
        peer_id: PeerId,
        outcome: doc_worker::ImportDocOutcome,
    },
    ImportDocBackoff {
        doc_id: DocumentId,
        peer_id: PeerId,
        delay: Duration,
        previous_attempt_no: usize,
        previous_backoff: Duration,
        previous_attempt_at: std::time::Instant,
    },
    GetPeerSyncSnapshot {
        peer_ids: Vec<PeerId>,
        resp: tokio::sync::oneshot::Sender<HashMap<PeerId, PeerSyncSnapshot>>,
    },
    WaitForPeersFullySynced {
        peer_ids: Vec<PeerId>,
        required_partitions: HashSet<PartitionKey>,
        resp: tokio::sync::oneshot::Sender<()>,
    },
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct PeerSyncSnapshot {
    pub emitted_full_synced: bool,
    pub bootstrap_ready: bool,
    pub bootstrap_synced_docs: u64,
    pub bootstrap_remaining_docs: u64,
    pub doc_pending_docs: u64,
    pub live_ready: bool,
    pub has_peer_session: bool,
    pub fully_synced_partitions: HashSet<PartitionKey>,
}

struct FullSyncWaiter {
    remaining: HashSet<PeerId>,
    required_partitions: HashSet<PartitionKey>,
    resp: tokio::sync::oneshot::Sender<()>,
}

#[derive(Debug)]
#[allow(clippy::enum_variant_names)]
enum SyncProgressMsg {
    BlobWorkerStarted {
        partition: PartitionKey,
        hash: String,
    },
    BlobDownloadStarted {
        peer_id: PeerId,
        partition: PartitionKey,
        hash: String,
    },
    BlobDownloadProgress {
        peer_id: PeerId,
        partition: PartitionKey,
        hash: String,
        done: u64,
    },
    BlobMaterializeStarted {
        partition: PartitionKey,
        hash: String,
    },
    BlobDownloadFinished {
        peer_id: PeerId,
        partition: PartitionKey,
        hash: String,
        success: bool,
    },
    BlobWorkerFinished {
        partition: PartitionKey,
        hash: String,
        success: bool,
        reason: String,
        synced_peer_id: Option<PeerId>,
        backoff: Option<(Duration, RetryState)>,
    },
}

impl WorkerHandle {
    pub async fn set_connection(
        &self,
        connection: am_utils_rs::repo::BigRepoConnection,
        endpoint_addr: iroh::EndpointAddr,
        peer_key: PeerKey,
        partition_ids: HashSet<PartitionId>,
    ) -> Res<()> {
        let partitions: HashSet<PartitionKey> = partition_ids
            .iter()
            .cloned()
            .map(|partition_id| {
                BlobScope::from_partition_id(&partition_id)
                    .map(PartitionKey::BlobScope)
                    .unwrap_or(PartitionKey::BigRepoPartition(partition_id))
            })
            .collect();
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
        required_partitions: HashSet<PartitionKey>,
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

    let partition_store = big_repo.partition_store().clone();
    let mut worker = Worker {
        big_repo,
        local_peer_key,
        cancel_token: cancel_token.clone(),
        msg_tx: msg_tx.clone(),
        sync_progress_tx: sync_progress_tx.clone(),
        peer_worker_msg_tx: peer_worker_msg_tx.clone(),
        events_tx,
        partitions: [
            (
                PartitionKey::BlobScope(BlobScope::Docs),
                Partition::default(),
            ),
            (
                PartitionKey::BlobScope(BlobScope::Plugs),
                Partition::default(),
            ),
        ]
        .into(),

        blobs_repo,
        progress_repo,
        iroh_endpoint,
        task_set,

        doc_request_set: default(),
        sync_machine: SyncMachine::new(partition_store, sync_store),
        scheduler: default(),

        known_peer_set: default(),
        expected_peer_session_closes: default(),
        peer_id_by_peer_key: default(),
        seen_peer_keys: default(),
        peer_partition_sessions: default(),
        full_sync_waiters: Vec::new(),

        max_active_sync_workers: 24,
    };

    worker.bootstrap_blob_scope_memberships().await?;

    let fut = {
        let cancel_token = cancel_token.clone();
        let mut janitor_tick = tokio::time::interval(Duration::from_millis(500));
        async move {
            let mut sync_progress_buf = Vec::with_capacity(512);
            loop {
                if !worker.scheduler.partitions_to_refresh.is_empty() {
                    worker.batch_refresh_paritions().await?;
                }
                if !worker.scheduler.peer_sessions_to_refresh.is_empty() {
                    worker.batch_refresh_peer_sessions().await?;
                }
                if !worker.scheduler.docs_to_stop.is_empty() {
                    worker.batch_stop_docs().await?;
                }
                if worker.scheduler.has_queued_docs() {
                    worker.batch_boot_docs().await?;
                }
                if worker.scheduler.has_queued_blobs() {
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
                        worker.handle_msg(msg).await?;
                    }
                    count = sync_progress_rx.recv_many(&mut sync_progress_buf, 512) => {
                        if count == 0 {
                            continue;
                        }
                        worker.handle_sync_progress_batch(&mut sync_progress_buf).await?;
                    }
                    val = peer_worker_msg_rx.recv() => {
                        let Some(msg) = val else {
                            warn!("peer worker msg channel closed");
                            break;
                        };
                        let peer_key = match &msg {
                            PeerSyncWorkerMsg::Progress { peer, .. } => peer.clone(),
                            PeerSyncWorkerMsg::SubscriptionItem { peer, .. } => peer.clone(),
                            PeerSyncWorkerMsg::Event(event) => match event {
                                PeerSyncWorkerEvent::Bootstrapped { peer, .. } => peer.clone(),
                                PeerSyncWorkerEvent::LiveReady { peer } => peer.clone(),
                                PeerSyncWorkerEvent::AbnormalExit { peer, .. } => peer.clone(),
                            },
                        };
                        let Some(peer_id) = worker.peer_id_by_peer_key.get(&peer_key).copied() else {
                            warn!(%peer_key, "peer worker message for unknown peer");
                            continue;
                        };
                        match msg {
                            PeerSyncWorkerMsg::Progress { event, .. } => {
                                worker.handle_peer_sync_worker_progress(peer_id, event).await?;
                            }
                            PeerSyncWorkerMsg::SubscriptionItem { peer, item } => {
                                worker.handle_subscription_item(peer_id, peer, item).await?;
                            }
                            PeerSyncWorkerMsg::Event(event) => {
                                worker.handle_peer_sync_worker_event(peer_id, event).await?;
                            }
                        }
                    }
                    _ = janitor_tick.tick() => {
                        worker.backoff_janitor_enqueue_due();
                    }
                }
            }
            worker.scheduler.docs_to_stop.extend(
                worker
                    .scheduler
                    .active_docs
                    .keys()
                    .map(|key| key.doc_id.clone()),
            );
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

    partitions: HashMap<PartitionKey, Partition>,

    scheduler: scheduler::Scheduler,

    // doc_ref_counts: HashMap<DocumentId, usize>,
    // peer_docs: HashMap<EndpointId, HashSet<DocumentId>>,
    doc_request_set: HashMap<DocumentId, DocRequestState>,
    sync_machine: SyncMachine,

    msg_tx: mpsc::UnboundedSender<Msg>,
    sync_progress_tx: mpsc::Sender<SyncProgressMsg>,
    peer_worker_msg_tx: mpsc::Sender<PeerSyncWorkerMsg>,
    events_tx: tokio::sync::broadcast::Sender<FullSyncEvent>,
    max_active_sync_workers: usize,

    known_peer_set: HashMap<BigRepoConnectionId, PeerSyncState>,
    expected_peer_session_closes: HashSet<PeerId>,
    peer_id_by_peer_key: HashMap<PeerKey, PeerId>,
    seen_peer_keys: HashSet<PeerKey>,
    peer_partition_sessions: HashMap<PeerId, PeerPartitionSession>,
    full_sync_waiters: Vec<FullSyncWaiter>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum PartitionKey {
    BigRepoPartition(PartitionId),
    BlobScope(BlobScope),
    // Docs(Uuid),
    // Blobs(Uuid),
}

impl PartitionKey {
    pub(crate) fn from_partition_id(partition_id: PartitionId) -> Self {
        BlobScope::from_partition_id(&partition_id)
            .map(PartitionKey::BlobScope)
            .unwrap_or(PartitionKey::BigRepoPartition(partition_id))
    }

    pub(crate) fn as_tag_value(&self) -> String {
        match self {
            Self::BigRepoPartition(partition_id) => format!("big_repo_partition/{partition_id}"),
            Self::BlobScope(scope) => scope.partition_id().to_string(),
        }
    }

    fn partition_id(&self) -> PartitionId {
        match self {
            Self::BigRepoPartition(partition_id) => partition_id.clone(),
            Self::BlobScope(scope) => scope.partition_id().to_string(),
        }
    }
}

#[derive(Default)]
struct Partition {
    is_active: bool,
    peers: HashSet<PeerId>,
}

struct PeerSyncState {
    peer_id: PeerId,
    peer_key: PeerKey,
    endpoint_addr: iroh::EndpointAddr,
    partitions: HashSet<PartitionKey>,
    connection: am_utils_rs::repo::BigRepoConnection,
    bootstrap_ready: bool,
    live_ready: bool,
    bootstrap_synced_docs: u64,
    bootstrap_remaining_docs: u64,
    doc_pending_docs: u64,
    fully_synced_partitions: HashSet<PartitionKey>,
    emitted_full_synced: bool,
}

struct PeerPartitionSession {
    stop: PeerSyncWorkerStopToken,
    partitions: HashSet<PartitionKey>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct RetryState {
    pub attempt_no: usize,
    pub last_backoff: Duration,
    pub last_attempt_at: std::time::Instant,
}

#[derive(Default)]
struct DocRequestState {
    requested_peers: HashSet<PeerId>,
    partition_ids: HashSet<PartitionId>,
}

struct ActiveDocSyncState {
    stop_token: doc_worker::DocSyncWorkerStopToken,
    retry: RetryState,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct DocSyncTaskKey {
    doc_id: DocumentId,
    peer_id: PeerId,
}

enum BootDocSyncWorkerResult {
    Spawned(ActiveDocSyncState),
    Deferred,
}

struct ActiveBlobSyncState {
    stop_token: blob_worker::BlobSyncWorkerStopToken,
}

#[derive(Hash, PartialEq, Eq)]
struct BlobProgressKey {
    peer_id: PeerId,
    partition: PartitionKey,
    hash: String,
}

impl Worker {
    fn peer_key_for_id(&self, peer_id: PeerId) -> Option<PeerKey> {
        let peer_state = self.known_peer_set.get(&peer_id)?;
        Some(Arc::clone(&peer_state.peer_key))
    }

    async fn doc_item_payload_json(&self, doc_id: &DocumentId) -> Res<String> {
        let doc = self
            .big_repo
            .get_doc(doc_id)
            .await?
            .ok_or_else(|| eyre::eyre!("missing doc for sync completion: {doc_id}"))?;
        let payload = doc
            .with_document_read(|doc| {
                let heads: Arc<[automerge::ChangeHash]> = Arc::from(doc.get_heads());
                serde_json::json!({
                    "heads": am_utils_rs::serialize_commit_heads(&heads),
                })
            })
            .await;
        Ok(serde_json::to_string(&payload)?)
    }

    async fn partition_is_fully_synced(
        &self,
        peer_id: PeerId,
        peer_key: &PeerKey,
        partition: &PartitionKey,
    ) -> Res<bool> {
        let Some(peer_state) = self.known_peer_set.get(&peer_id) else {
            return Ok(false);
        };
        if !peer_state.bootstrap_ready || !peer_state.live_ready {
            return Ok(false);
        }

        let partition_id = partition.partition_id();

        if self
            .sync_machine
            .has_active_item_jobs_for_partition(peer_key, &partition_id)
        {
            return Ok(false);
        }

        for (doc_id, doc_state) in &self.doc_request_set {
            if !doc_state.requested_peers.contains(&peer_id)
                || !doc_state.partition_ids.contains(&partition_id)
            {
                continue;
            }
            if self.sync_machine.has_active_item_job_for(
                peer_key,
                &partition_id,
                &doc_id.to_string(),
            ) {
                return Ok(false);
            }
            if self.big_repo.get_doc(doc_id).await?.is_none() {
                return Ok(false);
            }
        }

        if matches!(partition, PartitionKey::BlobScope(_)) {
            for (hash, part_set) in &self.scheduler.blob_requirements {
                if !part_set.contains(partition) {
                    continue;
                }
                if !self.blobs_repo.has_hash(hash).await? {
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }

    async fn refresh_peer_partition_sync_state(&mut self, peer_id: PeerId) -> Res<()> {
        let Some(peer_state) = self.known_peer_set.get(&peer_id) else {
            return Ok(());
        };
        let peer_key = Arc::clone(&peer_state.peer_key);
        let partitions = peer_state.partitions.clone();
        let prior = peer_state.fully_synced_partitions.clone();

        let mut next = HashSet::new();
        for partition in &partitions {
            if self
                .partition_is_fully_synced(peer_id, &peer_key, partition)
                .await?
            {
                next.insert(partition.clone());
            }
        }

        let newly_synced = next.difference(&prior).cloned().collect::<Vec<_>>();
        if let Some(peer_state) = self.known_peer_set.get_mut(&peer_id) {
            peer_state.fully_synced_partitions = next.clone();
        }
        for partition in newly_synced {
            self.emit_full_sync_event(FullSyncEvent::PartitionFullSynced {
                peer_key: Arc::clone(&peer_key),
                partition,
            })
            .await?;
        }

        Ok(())
    }

    fn refresh_full_sync_waiters(&mut self) {
        if self.full_sync_waiters.is_empty() {
            return;
        }
        let mut pending = std::mem::take(&mut self.full_sync_waiters);
        for mut waiter in pending.drain(..) {
            let required_partitions = waiter.required_partitions.clone();
            waiter.remaining.retain(|peer_id| {
                self.known_peer_set.get(&peer_id).is_none_or(|peer_state| {
                    !required_partitions
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

    fn reset_peer_sync_state(&mut self, peer_id: PeerId) {
        if let Some(peer_state) = self.known_peer_set.get_mut(&peer_id) {
            peer_state.bootstrap_ready = false;
            peer_state.live_ready = false;
            peer_state.bootstrap_synced_docs = 0;
            peer_state.bootstrap_remaining_docs = 0;
            peer_state.doc_pending_docs = 0;
            peer_state.fully_synced_partitions.clear();
            peer_state.emitted_full_synced = false;
        }
        self.refresh_full_sync_waiters();
    }

    fn available_doc_boot_budget(&self) -> usize {
        self.scheduler
            .available_doc_boot_budget(self.max_active_sync_workers)
    }

    fn available_blob_boot_budget(&self) -> usize {
        self.scheduler
            .available_blob_boot_budget(self.max_active_sync_workers)
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
                    for part_key in old.partitions.difference(&partitions) {
                        self.remove_peer_from_part(part_key.clone(), peer_id)
                            .await?;
                    }
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
                for part_key in new_parts {
                    self.add_peer_to_part(part_key, peer_id).await?;
                }
                self.refresh_peer_fully_synced_state(peer_id).await?;
                resp.send(()).inspect_err(|_| warn!(ERROR_CALLER)).ok();
            }
            Msg::DelPeer { resp, peer_id } => {
                if let Some(state) = self.known_peer_set.remove(&peer_id) {
                    self.peer_id_by_peer_key.remove(&state.peer_key);
                    state
                        .connection
                        .stop()
                        .await
                        .inspect_err(|err| {
                            error!("error on disconnection from peer {peer_id:?}: {err}")
                        })
                        .ok();
                    self.remove_peer_from_doc_request_set(peer_id).await?;
                    self.remove_peer_partition_session(peer_id).await?;
                    for part_key in state.partitions {
                        self.remove_peer_from_part(part_key, peer_id).await?;
                    }
                }
                self.peer_id_by_peer_key
                    .retain(|_peer_key, cached_endpoint| *cached_endpoint != peer_id);
                self.refresh_full_sync_waiters();
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
                previous_attempt_no,
                previous_backoff,
                previous_attempt_at,
            } => {
                self.handle_doc_request_backoff(
                    doc_id,
                    peer_id,
                    delay,
                    previous_attempt_no,
                    previous_backoff,
                    previous_attempt_at,
                )
                .await?;
            }
            Msg::DocSyncMissingLocal { doc_id } => {
                self.handle_doc_missing_local(doc_id).await?;
            }
            Msg::ImportDocCompleted {
                doc_id,
                peer_id,
                outcome,
            } => {
                self.handle_import_doc_completed(doc_id, peer_id, outcome)
                    .await?;
            }
            Msg::ImportDocBackoff {
                doc_id,
                peer_id,
                delay,
                previous_attempt_no,
                previous_backoff,
                previous_attempt_at,
            } => {
                self.handle_import_doc_backoff(
                    doc_id,
                    peer_id,
                    delay,
                    previous_attempt_no,
                    previous_backoff,
                    previous_attempt_at,
                )
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

    async fn batch_refresh_paritions(&mut self) -> Res<()> {
        let mut double = std::mem::replace(&mut self.scheduler.partitions_to_refresh, default());
        let mut affected_peers = HashSet::new();
        for part_key in double.drain() {
            for peer_state in self.known_peer_set.values() {
                if peer_state.partitions.contains(&part_key) {
                    affected_peers.insert(peer_state.peer_id);
                }
            }
            let (activate, is_active) = {
                let part = self
                    .partitions
                    .get(&part_key)
                    .ok_or_eyre("parition not found")?;
                for endpoint_id in &part.peers {
                    affected_peers.insert(*endpoint_id);
                }
                let activate = !part.peers.is_empty();
                (activate, part.is_active)
            };
            self.partitions
                .get_mut(&part_key)
                .expect("partition should exist")
                .is_active = activate;
            let has_flipped = activate != is_active;
            if has_flipped && matches!(&part_key, PartitionKey::BlobScope(_)) {
                self.refresh_blob_scope_workers().await?;
            }
        }
        self.scheduler.partitions_to_refresh = double;
        let mut sessions_to_remove = Vec::new();
        for peer_id in affected_peers {
            let desired_parts = self.desired_partitions_for_peer(peer_id);
            if desired_parts.is_empty() {
                sessions_to_remove.push(peer_id);
                continue;
            }
            let Some(_peer_state) = self.known_peer_set.get(&peer_id) else {
                continue;
            };
            let Some(session) = self.peer_partition_sessions.get(&peer_id) else {
                self.scheduler.peer_sessions_to_refresh.insert(peer_id);
                continue;
            };
            if session.partitions != desired_parts {
                self.scheduler.peer_sessions_to_refresh.insert(peer_id);
            }
        }
        for peer_id in sessions_to_remove {
            self.remove_peer_partition_session(peer_id).await?;
        }
        Ok(())
    }

    fn desired_partitions_for_peer(&self, peer_id: PeerId) -> HashSet<PartitionKey> {
        self.partitions
            .iter()
            .filter_map(|(part_key, part)| {
                if part.peers.contains(&peer_id) {
                    Some(part_key.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    async fn add_peer_to_part(&mut self, part_key: PartitionKey, peer_id: PeerId) -> Res<()> {
        let part = self.partitions.entry(part_key.clone()).or_default();
        part.peers.insert(peer_id);
        match part_key {
            PartitionKey::BigRepoPartition(_) => {
                let docs: Vec<_> = self
                    .scheduler
                    .pending_tasks
                    .keys()
                    .filter_map(|task| match task {
                        scheduler::SyncTask::Doc(task_key) if task_key.peer_id == peer_id => {
                            Some(task_key.clone())
                        }
                        scheduler::SyncTask::Doc(_) => None,
                        scheduler::SyncTask::Blob(_) => None,
                    })
                    .collect();
                for task_key in docs {
                    self.scheduler.enqueue_doc(task_key);
                }
            }
            PartitionKey::BlobScope(_) => {
                let hashes: Vec<_> = self
                    .scheduler
                    .pending_tasks
                    .keys()
                    .filter_map(|task| match task {
                        scheduler::SyncTask::Blob(hash) => Some(hash.clone()),
                        scheduler::SyncTask::Doc(_) => None,
                    })
                    .collect();
                for hash in hashes {
                    self.scheduler.enqueue_blob(hash);
                }
            }
        }
        self.scheduler.partitions_to_refresh.insert(part_key);
        Ok(())
    }

    async fn remove_peer_from_part(&mut self, part_key: PartitionKey, peer_id: PeerId) -> Res<()> {
        let part = self.partitions.entry(part_key.clone()).or_default();
        part.peers.remove(&peer_id);
        self.scheduler.partitions_to_refresh.insert(part_key);
        Ok(())
    }

    fn backoff_janitor_enqueue_due(&mut self) {
        self.scheduler
            .backoff_janitor_enqueue_due(self.max_active_sync_workers);
    }
}

// Docs related methods
impl Worker {
    async fn batch_refresh_peer_sessions(&mut self) -> Res<()> {
        let mut double = std::mem::replace(&mut self.scheduler.peer_sessions_to_refresh, default());
        for peer_id in double.drain() {
            let Some(peer_state) = self.known_peer_set.get(&peer_id) else {
                continue;
            };
            let peer_key = Arc::clone(&peer_state.peer_key);
            let endpoint_addr = peer_state.endpoint_addr.clone();
            let parts = self.desired_partitions_for_peer(peer_id);
            self.refresh_peer_partition_session(peer_id, endpoint_addr, peer_key, parts)
                .await?;
        }
        self.scheduler.peer_sessions_to_refresh = double;
        Ok(())
    }

    async fn refresh_peer_partition_session(
        &mut self,
        peer_id: PeerId,
        endpoint_addr: iroh::EndpointAddr,
        peer_key: PeerKey,
        partitions: HashSet<PartitionKey>,
    ) -> Res<()> {
        self.expected_peer_session_closes.insert(peer_id);
        let session = self.peer_partition_sessions.remove(&peer_id);
        if let Some(session) = session {
            session.stop.stop().await?;
        }
        self.sync_machine.clear_peer(&peer_key);
        self.reset_peer_sync_state(peer_id);
        if partitions.is_empty() {
            return Ok(());
        }
        let rpc_client = irpc_iroh::client::<PartitionSyncRpc>(
            self.iroh_endpoint.clone(),
            endpoint_addr,
            PARTITION_SYNC_ALPN,
        );
        let stop = am_utils_rs::sync::peer::spawn_peer_sync_worker(SpawnPeerSyncWorkerArgs {
            local_peer: self.local_peer_key.clone(),
            remote_peer: peer_key.clone(),
            rpc_client,
            sync_store: self.sync_machine.sync_store().clone(),
            target_partitions: partitions.iter().map(PartitionKey::partition_id).collect(),
            msg_tx: self.peer_worker_msg_tx.clone(),
            task_set: &self.task_set,
        })
        .await?;
        self.peer_partition_sessions
            .insert(peer_id, PeerPartitionSession { stop, partitions });
        Ok(())
    }

    async fn remove_peer_partition_session(&mut self, peer_id: PeerId) -> Res<()> {
        self.expected_peer_session_closes.insert(peer_id);
        let session = self.peer_partition_sessions.remove(&peer_id);
        if let Some(session) = session {
            session.stop.stop().await?;
        }
        if let Some(peer_state) = self.known_peer_set.get(&peer_id) {
            self.sync_machine.clear_peer(&peer_state.peer_key);
        }
        Ok(())
    }

    async fn handle_subscription_item(
        &mut self,
        _peer_id: PeerId,
        peer_key: PeerKey,
        item: SubscriptionItem,
    ) -> Res<()> {
        let commands = self
            .sync_machine
            .on_subscription_item(peer_key.clone(), item)?;
        self.dispatch_sync_commands(commands).await?;
        Ok(())
    }

    async fn dispatch_sync_commands(&mut self, commands: Vec<SyncMachineCommand>) -> Res<()> {
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
        let key = match &command {
            SyncMachineCommand::ItemNewSync { key }
            | SyncMachineCommand::ItemChangeSync { key }
            | SyncMachineCommand::ItemDeleteSync { key } => key,
        };
        let command_peer = key.peer.clone();
        let peer_id = *self
            .peer_id_by_peer_key
            .get(&command_peer)
            .ok_or_else(|| eyre::eyre!("missing peer id for sync command peer {}", command_peer))?;
        match command {
            SyncMachineCommand::ItemNewSync { key }
            | SyncMachineCommand::ItemChangeSync { key } => {
                match BlobScope::from_partition_id(&key.partition_id) {
                    Some(scope) => {
                        self.add_hash_to_blob_scope(scope, key.item_id.clone())
                            .await?;
                    }
                    None => {
                        let doc_id = key
                            .item_id
                            .parse::<DocumentId>()
                            .map_err(|err| ferr!("invalid item id '{}': {err}", key.item_id))?;
                        self.enqueue_doc_sync(
                            peer_id,
                            command_peer.clone(),
                            key.partition_id.clone(),
                            doc_id,
                        )
                        .await?;
                    }
                }
            }
            SyncMachineCommand::ItemDeleteSync { key } => {
                match BlobScope::from_partition_id(&key.partition_id) {
                    Some(scope) => {
                        let commands = self
                            .sync_machine
                            .on_item_sync_completed(SyncCompletion::DeletedMember {
                                peer: key.peer.clone(),
                                partition_id: key.partition_id.clone(),
                                item_id: key.item_id.clone(),
                            })
                            .await?;
                        self.remove_hash_from_blob_scope(scope, &key.item_id)
                            .await?;
                        return Ok(commands);
                    }
                    None => {
                        let doc_id = key
                            .item_id
                            .parse::<DocumentId>()
                            .map_err(|err| ferr!("invalid item id '{}': {err}", key.item_id))?;
                        let commands = self
                            .handle_doc_deleted(
                                peer_id,
                                command_peer.clone(),
                                key.partition_id.clone(),
                                doc_id,
                            )
                            .await?;
                        return Ok(commands);
                    }
                }
            }
        }
        Ok(Vec::new())
    }

    #[tracing::instrument(skip(self))]
    async fn enqueue_doc_sync(
        &mut self,
        peer_id: PeerId,
        peer_key: PeerKey,
        partition_id: PartitionId,
        doc_id: DocumentId,
    ) -> Res<()> {
        self.emit_stale_peer(Arc::clone(&peer_key)).await?;
        let doc_request_state = self.doc_request_set.entry(doc_id.clone()).or_default();
        let was_new = doc_request_state.requested_peers.insert(peer_id);
        doc_request_state.partition_ids.insert(partition_id);
        if was_new {
            if let Some(peer_state) = self.known_peer_set.get_mut(&peer_id) {
                peer_state.doc_pending_docs = peer_state.doc_pending_docs.saturating_add(1);
            }
            self.refresh_peer_fully_synced_state(peer_id).await?;
        }
        let task_key = DocSyncTaskKey {
            doc_id: doc_id.clone(),
            peer_id,
        };
        if !self.scheduler.active_docs.contains_key(&task_key)
            && !self.scheduler.is_doc_pending(&task_key)
        {
            self.scheduler.set_doc_pending_now(&task_key);
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn handle_doc_deleted(
        &mut self,
        peer_id: PeerId,
        peer_key: PeerKey,
        partition_id: PartitionId,
        doc_id: DocumentId,
    ) -> Res<Vec<SyncMachineCommand>> {
        self.emit_stale_peer(Arc::clone(&peer_key)).await?;
        let commands = self
            .sync_machine
            .on_item_sync_completed(SyncCompletion::DeletedMember {
                peer: peer_key.clone(),
                partition_id: partition_id.clone(),
                item_id: doc_id.to_string(),
            })
            .await?;

        let mut refresh_peer = false;
        let mut clear_doc_request = false;
        if let Some(doc_state) = self.doc_request_set.get_mut(&doc_id) {
            doc_state.requested_peers.remove(&peer_id);
            clear_doc_request = doc_state.requested_peers.is_empty();
            if !clear_doc_request {
                if let Some(peer_state) = self.known_peer_set.get_mut(&peer_id) {
                    peer_state.doc_pending_docs = peer_state.doc_pending_docs.saturating_sub(1);
                    refresh_peer = true;
                }
            }
        }
        if clear_doc_request {
            self.doc_request_set.remove(&doc_id);
            for task_key in self.scheduler.doc_task_keys_for_doc(&doc_id) {
                self.scheduler.clear_doc_task(&task_key);
                if let Some(active) = self.scheduler.active_docs.remove(&task_key) {
                    active.stop_token.stop().await?;
                }
            }
        } else {
            let task_key = DocSyncTaskKey {
                doc_id: doc_id.clone(),
                peer_id,
            };
            self.scheduler.clear_doc_task(&task_key);
            if let Some(active) = self.scheduler.active_docs.remove(&task_key) {
                active.stop_token.stop().await?;
            }
        }
        if refresh_peer {
            self.refresh_peer_fully_synced_state(peer_id).await?;
        }
        Ok(commands)
    }

    async fn batch_boot_docs(&mut self) -> Res<()> {
        let docs_part_active = self
            .partitions
            .iter()
            .any(|(key, part)| matches!(key, PartitionKey::BigRepoPartition(_)) && part.is_active);
        if !docs_part_active {
            return Ok(());
        }

        let mut budget = self.available_doc_boot_budget();
        if budget == 0 {
            return Ok(());
        }
        let doc_tasks = self.scheduler.drain_queued_docs(budget);
        for task_key in doc_tasks {
            if self.scheduler.active_docs.contains_key(&task_key) {
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
                BootDocSyncWorkerResult::Spawned(active) => {
                    debug!(
                        endpoint_id = ?task_key.peer_id,
                        %task_key.doc_id,
                        "doc sync worker spawned"
                    );
                    self.scheduler.clear_doc_pending(&task_key);
                    self.scheduler.active_docs.insert(task_key, active);
                    budget = budget.saturating_sub(1);
                }
                BootDocSyncWorkerResult::Deferred => {
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
                    self.scheduler.set_doc_backoff(&task_key, pending);
                }
            }
        }
        Ok(())
    }

    async fn boot_doc_sync_worker(
        &self,
        task_key: DocSyncTaskKey,
        retry: RetryState,
    ) -> Res<BootDocSyncWorkerResult> {
        let doc_id = task_key.doc_id;
        let peer_id = task_key.peer_id;
        let Some(sync_state) = self.doc_request_set.get(&doc_id) else {
            return Ok(BootDocSyncWorkerResult::Deferred);
        };
        if !sync_state.requested_peers.contains(&peer_id) {
            return Ok(BootDocSyncWorkerResult::Deferred);
        }
        let Some(peer_state) = self.known_peer_set.get(&peer_id) else {
            return Ok(BootDocSyncWorkerResult::Deferred);
        };
        let stop_token = if self.big_repo.get_doc(&doc_id).await?.is_some() {
            let connection = peer_state.connection.clone();
            doc_worker::spawn_doc_sync_worker(
                doc_id,
                doc_worker::DocSyncTarget::Sync {
                    peer_id,
                    connection,
                },
                Arc::clone(&self.big_repo),
                self.msg_tx.clone(),
                retry,
                &self.task_set,
            )?
        } else {
            doc_worker::spawn_doc_sync_worker(
                doc_id,
                doc_worker::DocSyncTarget::Import {
                    peer_id,
                    iroh_endpoint: self.iroh_endpoint.clone(),
                },
                Arc::clone(&self.big_repo),
                self.msg_tx.clone(),
                retry,
                &self.task_set,
            )?
        };
        Ok(BootDocSyncWorkerResult::Spawned(ActiveDocSyncState {
            stop_token,
            retry,
        }))
    }

    async fn batch_stop_docs(&mut self) -> Res<()> {
        let stopped_doc_ids = self.scheduler.docs_to_stop.drain().collect::<Vec<_>>();
        for doc_id in &stopped_doc_ids {
            for task_key in self.scheduler.doc_task_keys_for_doc(doc_id) {
                if let Some(active) = self.scheduler.active_docs.remove(&task_key) {
                    active.stop_token.stop().await?;
                }
                self.scheduler.clear_doc_task(&task_key);
            }
        }
        Ok(())
    }

    async fn batch_stop_blobs(&mut self) -> Res<()> {
        self.scheduler
            .queued_tasks
            .retain(|task| !matches!(task, scheduler::SyncTask::Blob(_)));
        self.scheduler
            .pending_tasks
            .retain(|task, _| !matches!(task, scheduler::SyncTask::Blob(_)));
        self.scheduler.blob_requirements.clear();

        for (_hash, active) in self.scheduler.active_blobs.drain() {
            active.stop_token.stop().await?;
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn handle_doc_sync_completed(
        &mut self,
        doc_id: DocumentId,
        peer_id: PeerId,
        outcome: am_utils_rs::repo::SyncDocOutcome,
    ) -> Res<()> {
        let task_key = DocSyncTaskKey {
            doc_id: doc_id.clone(),
            peer_id,
        };
        let Some(active) = self.scheduler.active_docs.remove(&task_key) else {
            warn!(
                ?doc_id,
                ?peer_id,
                ?outcome,
                "doc sync completed for unrecognized task (peer may have exited abnormally)"
            );
            return Ok(());
        };
        active.stop_token.stop().await?;
        self.scheduler.clear_doc_pending(&task_key);
        debug!("doc sync worker completed");

        match outcome {
            am_utils_rs::repo::SyncDocOutcome::Success => {
                let mut refreshed_peer = false;

                if let Some(sync_state) = self.doc_request_set.get_mut(&doc_id) {
                    if sync_state.requested_peers.remove(&peer_id) {
                        if let Some(peer_state) = self.known_peer_set.get_mut(&peer_id) {
                            peer_state.doc_pending_docs =
                                peer_state.doc_pending_docs.saturating_sub(1);
                        }
                        refreshed_peer = true;
                    }
                }

                let partition_ids = self
                    .doc_request_set
                    .get(&doc_id)
                    .map(|state| state.partition_ids.clone())
                    .unwrap_or_default();
                let completion_peer = self.peer_key_for_id(peer_id).expect(ERROR_UNRECONIZED);
                let item_payload = self.doc_item_payload_json(&doc_id).await?;
                for partition_id in &partition_ids {
                    if let Some(key) = self.sync_machine.find_active_item_key_for(
                        &completion_peer,
                        partition_id,
                        &doc_id.to_string(),
                    ) {
                        let completion = match key.kind {
                            am_utils_rs::sync::machine::ItemSyncKind::New => {
                                SyncCompletion::AddedMember {
                                    peer: key.peer,
                                    partition_id: partition_id.clone(),
                                    item_id: doc_id.to_string(),
                                    item_payload: item_payload.clone(),
                                }
                            }
                            am_utils_rs::sync::machine::ItemSyncKind::Change => {
                                SyncCompletion::ChangedItem {
                                    peer: key.peer,
                                    partition_id: partition_id.clone(),
                                    item_id: doc_id.to_string(),
                                    item_payload: item_payload.clone(),
                                }
                            }
                            am_utils_rs::sync::machine::ItemSyncKind::Delete => {
                                eyre::bail!("doc sync success cannot complete delete job")
                            }
                        };
                        let commands = self.sync_machine.on_item_sync_completed(completion).await?;
                        self.dispatch_sync_commands(commands).await?;
                    }
                }

                if refreshed_peer {
                    self.refresh_peer_fully_synced_state(peer_id).await?;
                }
                let peer_key = completion_peer;
                self.emit_full_sync_event(FullSyncEvent::DocSyncedWithPeer { peer_key, doc_id })
                    .await?;
                self.emit_peer_progress_status(
                    peer_id,
                    ProgressUpdateDeets::Status {
                        severity: ProgressSeverity::Info,
                        message: format!("doc sync completed: {doc_id}"),
                    },
                )
                .await?;
            }
            am_utils_rs::repo::SyncDocOutcome::NotFoundOrUnauthorized
            | am_utils_rs::repo::SyncDocOutcome::TransportError
            | am_utils_rs::repo::SyncDocOutcome::IoError => {
                let delay = match outcome {
                    am_utils_rs::repo::SyncDocOutcome::NotFoundOrUnauthorized => {
                        Duration::from_secs(1)
                    }
                    am_utils_rs::repo::SyncDocOutcome::TransportError => Duration::from_millis(500),
                    am_utils_rs::repo::SyncDocOutcome::IoError => Duration::from_secs(1),
                    am_utils_rs::repo::SyncDocOutcome::Success => unreachable!(),
                };
                let now = std::time::Instant::now();
                let backoff = next_backoff_delay(active.retry.last_backoff, delay);
                let pending = scheduler::PendingTaskState {
                    attempt_no: active.retry.attempt_no + 1,
                    last_backoff: backoff,
                    last_attempt_at: now,
                    due_at: now + backoff,
                };
                self.scheduler.set_doc_backoff(&task_key, pending);
            }
        }

        let requested_peers_empty = self
            .doc_request_set
            .get(&doc_id)
            .map(|state| state.requested_peers.is_empty())
            .unwrap_or(true);
        if requested_peers_empty {
            self.doc_request_set.remove(&doc_id);
            for task_key in self.scheduler.doc_task_keys_for_doc(&doc_id) {
                self.scheduler.clear_doc_task(&task_key);
                if let Some(active) = self.scheduler.active_docs.remove(&task_key) {
                    active.stop_token.stop().await?;
                }
            }
        } else if let Some(sync_state) = self.doc_request_set.get(&doc_id) {
            for endpoint_id in sync_state.requested_peers.iter().copied() {
                let pending_key = DocSyncTaskKey {
                    doc_id: doc_id.clone(),
                    peer_id: endpoint_id,
                };
                if !self.scheduler.active_docs.contains_key(&pending_key)
                    && !self.scheduler.is_doc_pending(&pending_key)
                {
                    self.scheduler.set_doc_pending_now(&pending_key);
                }
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn handle_doc_request_backoff(
        &mut self,
        doc_id: DocumentId,
        peer_id: PeerId,
        delay: Duration,
        previous_attempt_no: usize,
        previous_backoff: Duration,
        _previous_attempt_at: std::time::Instant,
    ) -> Res<()> {
        let task_key = DocSyncTaskKey {
            doc_id: doc_id.clone(),
            peer_id,
        };
        if let Some(active) = self.scheduler.active_docs.remove(&task_key) {
            active.stop_token.stop().await?;
        }
        if self
            .doc_request_set
            .get(&doc_id)
            .is_none_or(|state| !state.requested_peers.contains(&peer_id))
        {
            self.scheduler.clear_doc_task(&task_key);
            return Ok(());
        }
        let now = std::time::Instant::now();
        let delay = delay.min(Duration::from_secs(600));
        let backoff = next_backoff_delay(previous_backoff, delay);
        let pending = scheduler::PendingTaskState {
            attempt_no: previous_attempt_no + 1,
            last_backoff: backoff,
            last_attempt_at: now,
            due_at: now + backoff,
        };
        self.scheduler.set_doc_backoff(&task_key, pending);
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn handle_doc_missing_local(&mut self, doc_id: DocumentId) -> Res<()> {
        for task_key in self.scheduler.doc_task_keys_for_doc(&doc_id) {
            if let Some(active) = self.scheduler.active_docs.remove(&task_key) {
                active.stop_token.stop().await?;
            }
            self.scheduler.clear_doc_task(&task_key);
        }

        let Some(sync_state) = self.doc_request_set.remove(&doc_id) else {
            return Ok(());
        };

        let endpoint_ids = sync_state
            .requested_peers
            .iter()
            .copied()
            .collect::<Vec<_>>();
        let mut peers_to_refresh = Vec::new();
        for peer_id in sync_state.requested_peers.iter() {
            if let Some(peer_state) = self.known_peer_set.get_mut(&peer_id) {
                peer_state.doc_pending_docs = peer_state.doc_pending_docs.saturating_sub(1);
                peers_to_refresh.push(*peer_id);
            }
        }
        for peer_id in peers_to_refresh {
            self.refresh_peer_fully_synced_state(peer_id).await?;
        }
        for endpoint_id in endpoint_ids {
            self.emit_peer_progress_status(
                endpoint_id,
                ProgressUpdateDeets::Completed {
                    state: ProgressFinalState::Failed,
                    message: Some(format!(
                        "doc sync requested for missing local doc: {doc_id}"
                    )),
                },
            )
            .await?;
        }
        warn!(%doc_id, "doc sync requested for missing local doc; marking request terminal");
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn handle_import_doc_completed(
        &mut self,
        doc_id: DocumentId,
        peer_id: PeerId,
        outcome: doc_worker::ImportDocOutcome,
    ) -> Res<()> {
        let task_key = DocSyncTaskKey {
            doc_id: doc_id.clone(),
            peer_id,
        };
        let prior_pending = self.scheduler.pending_doc_state(&task_key);
        if let Some(active) = self.scheduler.active_docs.remove(&task_key) {
            active.stop_token.stop().await?;
        }
        debug!("import worker completed");

        if !self.doc_request_set.contains_key(&doc_id) {
            self.scheduler.clear_doc_task(&task_key);
            return Ok(());
        }

        match outcome {
            doc_worker::ImportDocOutcome::Imported | doc_worker::ImportDocOutcome::LocalPresent => {
                if let Some(doc_state) = self.doc_request_set.get_mut(&doc_id) {
                    doc_state.requested_peers.remove(&peer_id);
                }

                let partition_ids = self
                    .doc_request_set
                    .get(&doc_id)
                    .map(|state| state.partition_ids.clone())
                    .unwrap_or_default();
                let completion_peer = self.peer_key_for_id(peer_id).expect(ERROR_UNRECONIZED);
                let item_payload = self.doc_item_payload_json(&doc_id).await?;
                for partition_id in &partition_ids {
                    if let Some(key) = self.sync_machine.find_active_item_key_for(
                        &completion_peer,
                        partition_id,
                        &doc_id.to_string(),
                    ) {
                        let completion = match outcome {
                            doc_worker::ImportDocOutcome::Imported => SyncCompletion::AddedMember {
                                peer: key.peer,
                                partition_id: partition_id.clone(),
                                item_id: doc_id.to_string(),
                                item_payload: item_payload.clone(),
                            },
                            doc_worker::ImportDocOutcome::LocalPresent => SyncCompletion::Noop {
                                peer: key.peer,
                                partition_id: partition_id.clone(),
                                item_id: doc_id.to_string(),
                            },
                            doc_worker::ImportDocOutcome::MissingOnRemote => unreachable!(),
                        };
                        let commands = self.sync_machine.on_item_sync_completed(completion).await?;
                        self.dispatch_sync_commands(commands).await?;
                    }
                }
            }
            doc_worker::ImportDocOutcome::MissingOnRemote => {
                warn!("import worker could not fetch doc from remote; waiting for replay/retry");
                if self
                    .doc_request_set
                    .get(&doc_id)
                    .is_none_or(|state| !state.requested_peers.contains(&peer_id))
                {
                    self.scheduler.clear_doc_task(&task_key);
                    return Ok(());
                }
                let now = std::time::Instant::now();
                let previous_backoff = prior_pending
                    .as_ref()
                    .map_or(Duration::from_secs(1), |pending| pending.last_backoff);
                let backoff = next_backoff_delay(previous_backoff, Duration::from_secs(1));
                let pending = scheduler::PendingTaskState {
                    attempt_no: prior_pending
                        .as_ref()
                        .map_or(1, |pending| pending.attempt_no + 1),
                    last_backoff: backoff,
                    last_attempt_at: now,
                    due_at: now + backoff,
                };
                self.scheduler.set_doc_backoff(&task_key, pending);
            }
        }

        let clear_doc_import_state = self
            .doc_request_set
            .get(&doc_id)
            .is_none_or(|state| state.requested_peers.is_empty());
        if clear_doc_import_state {
            self.doc_request_set.remove(&doc_id);
            for import_task_key in self.scheduler.doc_task_keys_for_doc(&doc_id) {
                self.scheduler.clear_doc_task(&import_task_key);
                if let Some(active) = self.scheduler.active_docs.remove(&import_task_key) {
                    active.stop_token.stop().await?;
                }
            }
        }
        self.refresh_peer_fully_synced_state(peer_id).await?;
        Ok(())
    }

    async fn handle_import_doc_backoff(
        &mut self,
        doc_id: DocumentId,
        peer_id: PeerId,
        delay: Duration,
        previous_attempt_no: usize,
        previous_backoff: Duration,
        _previous_attempt_at: std::time::Instant,
    ) -> Res<()> {
        let task_key = DocSyncTaskKey { doc_id, peer_id };
        if let Some(active) = self.scheduler.active_docs.remove(&task_key) {
            active.stop_token.stop().await?;
        }
        if self
            .doc_request_set
            .get(&task_key.doc_id)
            .is_none_or(|state| !state.requested_peers.contains(&task_key.peer_id))
        {
            self.scheduler.clear_doc_task(&task_key);
            return Ok(());
        }
        let now = std::time::Instant::now();
        let delay = delay.min(Duration::from_secs(600));
        let backoff = next_backoff_delay(previous_backoff, delay);
        let pending = scheduler::PendingTaskState {
            attempt_no: previous_attempt_no + 1,
            last_backoff: backoff,
            last_attempt_at: now,
            due_at: now + backoff,
        };
        self.scheduler.set_doc_backoff(&task_key, pending);
        Ok(())
    }
}

// Blobs related methods
impl Worker {
    async fn bootstrap_blob_scope_memberships(&mut self) -> Res<()> {
        for scope in [BlobScope::Docs, BlobScope::Plugs] {
            let partition_id = scope.partition_id().to_string();
            let mut since = None;
            loop {
                let page = self
                    .big_repo
                    .partition_store()
                    .get_partition_member_events(
                        &am_utils_rs::sync::protocol::GetPartitionMemberEventsRequest {
                            partitions: vec![am_utils_rs::sync::protocol::PartitionCursorRequest {
                                partition_id: partition_id.clone(),
                                since,
                            }],
                            limit: am_utils_rs::sync::protocol::DEFAULT_EVENT_PAGE_LIMIT,
                        },
                    )
                    .await?;
                for event in page.events {
                    match event.deets {
                        am_utils_rs::sync::protocol::PartitionMemberEventDeets::MemberUpsert {
                            item_id,
                            ..
                        } => self.add_hash_to_blob_scope(scope, item_id).await?,
                        am_utils_rs::sync::protocol::PartitionMemberEventDeets::MemberRemoved {
                            item_id,
                            ..
                        } => self.remove_hash_from_blob_scope(scope, &item_id).await?,
                    }
                }
                let cursor = page
                    .cursors
                    .into_iter()
                    .find(|cursor| cursor.partition_id == partition_id)
                    .ok_or_eyre("partition cursor page missing for blob scope bootstrap")?;
                if !cursor.has_more {
                    break;
                }
                since = Some(
                    cursor
                        .next_cursor
                        .ok_or_eyre("blob scope bootstrap cursor missing next_cursor")?,
                );
            }
        }
        Ok(())
    }

    async fn remove_blob_tracking_if_unused(&mut self, hash: &str) -> Res<()> {
        let should_drop = self
            .scheduler
            .blob_requirements
            .get(hash)
            .is_none_or(HashSet::is_empty);
        if should_drop {
            if let Some(active) = self.scheduler.active_blobs.remove(hash) {
                active.stop_token.stop().await?;
            }
            self.scheduler.blob_requirements.remove(hash);
            self.scheduler.clear_blob_task(hash);
        }
        Ok(())
    }

    fn blob_required_by_any_active_partition(&self, hash: &str) -> bool {
        let Some(partitions) = self.scheduler.blob_requirements.get(hash) else {
            return false;
        };
        partitions.iter().any(|part_key| {
            self.partitions
                .get(part_key)
                .is_some_and(|partition| partition.is_active)
        })
    }

    async fn add_hash_to_blob_scope(&mut self, scope: BlobScope, hash: String) -> Res<()> {
        if utils_rs::hash::decode_base58_multibase(&hash).is_err() {
            warn!(scope = ?scope, hash, "dropping invalid hash in blob scope partition");
            return Ok(());
        }
        let part_key = PartitionKey::BlobScope(scope);
        let part_set = self
            .scheduler
            .blob_requirements
            .entry(hash.clone())
            .or_default();
        part_set.insert(part_key);
        if self.blob_required_by_any_active_partition(&hash)
            && !self.scheduler.active_blobs.contains_key(&hash)
        {
            self.scheduler.set_blob_pending_now(&hash);
        }
        self.refresh_peers_for_blob_hash(&hash).await?;
        Ok(())
    }

    async fn remove_hash_from_blob_scope(&mut self, scope: BlobScope, hash: &str) -> Res<()> {
        let affected_peers = self.active_peers_for_blob(hash);
        if let Some(part_set) = self.scheduler.blob_requirements.get_mut(hash) {
            part_set.remove(&PartitionKey::BlobScope(scope));
        }
        self.remove_blob_tracking_if_unused(hash).await?;
        for peer_id in affected_peers {
            self.refresh_peer_fully_synced_state(peer_id).await?;
        }
        Ok(())
    }

    async fn refresh_blob_scope_workers(&mut self) -> Res<()> {
        let hashes: Vec<String> = self.scheduler.blob_requirements.keys().cloned().collect();
        for hash in hashes {
            if self.blob_required_by_any_active_partition(&hash) {
                if !self.scheduler.active_blobs.contains_key(&hash) {
                    self.scheduler.set_blob_pending_now(&hash);
                }
                continue;
            }
            if let Some(active) = self.scheduler.active_blobs.remove(&hash) {
                active.stop_token.stop().await?;
            }
            self.scheduler.clear_blob_pending(&hash);
        }
        Ok(())
    }

    fn active_peers_for_blob(&self, hash: &str) -> Vec<PeerId> {
        let mut peers = HashSet::new();
        let Some(partitions) = self.scheduler.blob_requirements.get(hash) else {
            return Vec::new();
        };
        for part_key in partitions {
            if let Some(partition) = self.partitions.get(part_key) {
                if partition.is_active {
                    peers.extend(partition.peers.iter().copied());
                }
            }
        }
        peers.into_iter().collect()
    }

    async fn refresh_peers_for_blob_hash(&mut self, hash: &str) -> Res<()> {
        for peer_id in self.active_peers_for_blob(hash) {
            self.refresh_peer_fully_synced_state(peer_id).await?;
        }
        Ok(())
    }

    fn primary_active_partition_for_blob(&self, hash: &str) -> Option<PartitionKey> {
        let part_set = self.scheduler.blob_requirements.get(hash)?;
        part_set
            .iter()
            .find(|part_key| {
                self.partitions
                    .get(*part_key)
                    .is_some_and(|partition| partition.is_active)
            })
            .cloned()
    }

    async fn batch_boot_blobs(&mut self) -> Res<()> {
        let mut budget = self.available_blob_boot_budget();
        if budget == 0 {
            return Ok(());
        }
        let hashes = self.scheduler.drain_queued_blobs(budget);
        for hash in hashes {
            if self.scheduler.active_blobs.contains_key(&hash) {
                continue;
            }
            if !self.blob_required_by_any_active_partition(&hash) {
                self.scheduler.clear_blob_pending(&hash);
                continue;
            }
            let prior_pending = self.scheduler.pending_blob_state(&hash);
            if self.blobs_repo.has_hash(&hash).await? {
                self.scheduler.clear_blob_pending(&hash);
                if let Some(part_set) = self.scheduler.blob_requirements.get(&hash).cloned() {
                    for partition_key in &part_set {
                        let partition_id = partition_key.partition_id();
                        if matches!(partition_key, PartitionKey::BigRepoPartition(_)) {
                            continue;
                        }
                        let item_payload = "{}".to_string();
                        for peer in self
                            .sync_machine
                            .active_peers_for_item(&partition_id, &hash)
                        {
                            let commands = self
                                .sync_machine
                                .on_item_sync_completed(SyncCompletion::AddedMember {
                                    peer,
                                    partition_id: partition_id.clone(),
                                    item_id: hash.clone(),
                                    item_payload: item_payload.clone(),
                                })
                                .await?;
                            self.dispatch_sync_commands(commands).await?;
                        }
                    }
                }
                self.refresh_peers_for_blob_hash(&hash).await?;
                continue;
            }
            let peers = self.active_peers_for_blob(&hash);
            if peers.is_empty() {
                self.scheduler.enqueue_blob(hash);
            } else {
                let partition = self
                    .primary_active_partition_for_blob(&hash)
                    .ok_or_eyre("missing active partition for pending blob")?;
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
                let active = ActiveBlobSyncState {
                    stop_token: blob_worker::spawn_blob_sync_worker(
                        partition,
                        hash.clone(),
                        peers,
                        self.sync_progress_tx.clone(),
                        Arc::clone(&self.blobs_repo),
                        self.iroh_endpoint.clone(),
                        retry,
                        &self.task_set,
                    )?,
                };
                self.scheduler.clear_blob_pending(&hash);
                self.scheduler.active_blobs.insert(hash, active);
                budget = budget.saturating_sub(1);
            }
        }
        Ok(())
    }

    async fn handle_blob_marked_synced(
        &mut self,
        hash: String,
        peer_id: Option<PeerId>,
    ) -> Res<()> {
        if !self.scheduler.blob_requirements.contains_key(&hash) {
            return Ok(());
        }
        let partition_keys = self
            .scheduler
            .blob_requirements
            .get(&hash)
            .cloned()
            .unwrap_or_default();
        for partition_key in &partition_keys {
            let partition_id = partition_key.partition_id();
            if matches!(partition_key, PartitionKey::BigRepoPartition(_)) {
                continue;
            }
            let item_payload = "{}".to_string();
            for peer in self
                .sync_machine
                .active_peers_for_item(&partition_id, &hash)
            {
                let commands = self
                    .sync_machine
                    .on_item_sync_completed(SyncCompletion::AddedMember {
                        peer,
                        partition_id: partition_id.clone(),
                        item_id: hash.clone(),
                        item_payload: item_payload.clone(),
                    })
                    .await?;
                self.dispatch_sync_commands(commands).await?;
            }
        }
        let sync_hash = hash.clone();
        if let Some(active) = self.scheduler.active_blobs.remove(&hash) {
            active.stop_token.stop().await?;
        }
        self.scheduler.clear_blob_pending(&hash);
        let peer_key = peer_id.and_then(|id| self.peer_key_for_id(id));
        self.emit_full_sync_event(FullSyncEvent::BlobSynced {
            hash: sync_hash,
            peer_key,
        })
        .await?;
        self.refresh_peers_for_blob_hash(&hash).await?;
        Ok(())
    }

    async fn handle_blob_request_backoff(
        &mut self,
        hash: String,
        delay: Duration,
        previous_attempt_no: usize,
        previous_backoff: Duration,
        _previous_attempt_at: std::time::Instant,
    ) -> Res<()> {
        if !self.scheduler.blob_requirements.contains_key(&hash) {
            return Ok(());
        }
        let Some(active) = self.scheduler.active_blobs.remove(&hash) else {
            return Ok(());
        };
        active.stop_token.stop().await?;
        let now = std::time::Instant::now();
        let delay = delay.min(Duration::from_secs(600));
        let backoff = next_backoff_delay(previous_backoff, delay);
        let pending = scheduler::PendingTaskState {
            attempt_no: previous_attempt_no + 1,
            last_backoff: backoff,
            last_attempt_at: now,
            due_at: now + backoff,
        };
        let attempt_no = pending.attempt_no;
        self.scheduler.set_blob_backoff(&hash, pending);
        self.emit_full_sync_event(FullSyncEvent::BlobSyncBackoff {
            hash: hash.clone(),
            delay: backoff,
            attempt_no,
        })
        .await?;
        Ok(())
    }
}

// progress related methods
impl Worker {
    async fn handle_peer_sync_worker_progress(
        &mut self,
        peer_id: PeerId,
        event: PeerSyncProgressEvent,
    ) -> Res<()> {
        match event {
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
            PeerSyncProgressEvent::SyncStatus {
                synced_items,
                remaining_items,
            } => {
                debug!(
                    peer_id = ?peer_id,
                    synced_items,
                    remaining_items,
                    "peer sync worker status"
                );
                self.emit_peer_progress_status(
                    peer_id,
                    ProgressUpdateDeets::Amount {
                        severity: ProgressSeverity::Info,
                        done: synced_items,
                        total: Some(synced_items.saturating_add(remaining_items)),
                        unit: ProgressUnit::Generic {
                            label: "items".to_string(),
                        },
                        message: Some("sync status".to_string()),
                    },
                )
                .await?;
                if let Some(peer_state) = self.known_peer_set.get_mut(&peer_id) {
                    peer_state.bootstrap_synced_docs = synced_items;
                    peer_state.bootstrap_remaining_docs = remaining_items;
                }
                self.refresh_peer_fully_synced_state(peer_id).await?;
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn handle_peer_sync_worker_event(
        &mut self,
        peer_id: PeerId,
        event: PeerSyncWorkerEvent,
    ) -> Res<()> {
        let peer_key = self.peer_key_for_id(peer_id).expect(ERROR_UNRECONIZED);
        match event {
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
                self.scheduler.peer_sessions_to_refresh.insert(peer_id);
            }
        }
        Ok(())
    }

    fn has_progress_repo(&self) -> bool {
        self.progress_repo.is_some()
    }

    async fn emit_blob_worker_status(
        &self,
        _partition: PartitionKey,
        _hash: &str,
        _message: String,
        _final_state: Option<ProgressFinalState>,
    ) -> Res<()> {
        // FIXME: wtf
        Ok(())
    }

    async fn emit_peer_progress_status(
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

    async fn remove_peer_from_doc_request_set(&mut self, peer_id: PeerId) -> Res<()> {
        let mut to_remove = Vec::new();
        let mut to_stop = Vec::new();
        for (doc_id, sync_state) in &mut self.doc_request_set {
            sync_state.requested_peers.remove(&peer_id);
            self.scheduler.clear_doc_task(&DocSyncTaskKey {
                doc_id: doc_id.clone(),
                peer_id,
            });
            if let Some(active) = self.scheduler.active_docs.remove(&DocSyncTaskKey {
                doc_id: doc_id.clone(),
                peer_id,
            }) {
                to_stop.push(active.stop_token);
            }
            if sync_state.requested_peers.is_empty() {
                to_remove.push(doc_id.clone());
            }
        }
        for task_key in self.scheduler.doc_task_keys_for_peer(peer_id) {
            self.scheduler.clear_doc_task(&task_key);
            if let Some(active) = self.scheduler.active_docs.remove(&task_key) {
                to_stop.push(active.stop_token);
            }
        }
        for stop_token in to_stop {
            stop_token.stop().await?;
        }
        for doc_id in to_remove {
            for task_key in self.scheduler.doc_task_keys_for_doc(&doc_id) {
                self.scheduler.clear_doc_task(&task_key);
                if let Some(active) = self.scheduler.active_docs.remove(&task_key) {
                    active.stop_token.stop().await?;
                }
            }
            self.scheduler.docs_to_stop.remove(&doc_id);
            self.doc_request_set.remove(&doc_id);
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn refresh_peer_fully_synced_state(&mut self, peer_id: PeerId) -> Res<()> {
        self.refresh_peer_partition_sync_state(peer_id).await?;
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

    async fn emit_full_sync_event(&self, event: FullSyncEvent) -> Res<()> {
        debug!(event = ?event, "emitting full sync event");
        if self.events_tx.send(event).is_err() && !self.cancel_token.is_cancelled() {
            trace!("full sync event receiver dropped");
        }
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

    async fn emit_blob_progress_start(
        &self,
        peer_id: PeerId,
        partition: PartitionKey,
        hash: &str,
    ) -> Res<()> {
        self.emit_peer_progress_status(
            peer_id,
            ProgressUpdateDeets::Status {
                severity: ProgressSeverity::Info,
                message: format!("blob download started: {hash}"),
            },
        )
        .await?;
        let peer_key = self.peer_key_for_id(peer_id).expect(ERROR_UNRECONIZED);
        self.emit_full_sync_event(FullSyncEvent::BlobDownloadStarted {
            peer_key,
            partition,
            hash: hash.to_string(),
        })
        .await?;
        Ok(())
    }

    async fn emit_blob_progress_amount(
        &self,
        _peer_id: PeerId,
        _partition: PartitionKey,
        _hash: &str,
        _done: u64,
    ) -> Res<()> {
        // FIXME: wtf
        Ok(())
    }

    async fn emit_blob_progress_finished(
        &self,
        peer_id: PeerId,
        partition: PartitionKey,
        hash: &str,
        success: bool,
    ) -> Res<()> {
        self.emit_peer_progress_status(
            peer_id,
            ProgressUpdateDeets::Status {
                severity: if success {
                    ProgressSeverity::Info
                } else {
                    ProgressSeverity::Error
                },
                message: format!(
                    "blob download {}: {hash}",
                    if success { "finished" } else { "failed" }
                ),
            },
        )
        .await?;
        let peer_key = self.peer_key_for_id(peer_id).expect(ERROR_UNRECONIZED);
        self.emit_full_sync_event(FullSyncEvent::BlobDownloadFinished {
            peer_key,
            partition,
            hash: hash.to_string(),
            success,
        })
        .await?;
        Ok(())
    }

    async fn handle_sync_progress_batch(&mut self, buffer: &mut Vec<SyncProgressMsg>) -> Res<()> {
        let batch_len = buffer.len();
        if batch_len == 0 {
            return Ok(());
        }
        debug!(batch_len, "handling sync progress batch");

        let mut worker_started: HashSet<(PartitionKey, String)> = HashSet::new();
        let mut started: HashSet<BlobProgressKey> = HashSet::new();
        let mut progress_latest: HashMap<BlobProgressKey, u64> = HashMap::new();
        let mut materialize_started: HashSet<(PartitionKey, String)> = HashSet::new();
        let mut finished: HashMap<BlobProgressKey, bool> = HashMap::new();
        let mut worker_finished: HashMap<
            (PartitionKey, String),
            (bool, String, Option<PeerId>, Option<(Duration, RetryState)>),
        > = HashMap::new();

        for msg in buffer.drain(..) {
            match msg {
                SyncProgressMsg::BlobWorkerStarted { partition, hash } => {
                    worker_started.insert((partition, hash));
                }
                SyncProgressMsg::BlobDownloadStarted {
                    peer_id: endpoint_id,
                    partition,
                    hash,
                } => {
                    started.insert(BlobProgressKey {
                        peer_id: endpoint_id,
                        partition,
                        hash,
                    });
                }
                SyncProgressMsg::BlobDownloadProgress {
                    peer_id: endpoint_id,
                    partition,
                    hash,
                    done,
                } => {
                    progress_latest.insert(
                        BlobProgressKey {
                            peer_id: endpoint_id,
                            partition,
                            hash,
                        },
                        done,
                    );
                }
                SyncProgressMsg::BlobMaterializeStarted { partition, hash } => {
                    materialize_started.insert((partition, hash));
                }
                SyncProgressMsg::BlobDownloadFinished {
                    peer_id: endpoint_id,
                    partition,
                    hash,
                    success,
                } => {
                    finished.insert(
                        BlobProgressKey {
                            peer_id: endpoint_id,
                            partition,
                            hash,
                        },
                        success,
                    );
                }
                SyncProgressMsg::BlobWorkerFinished {
                    partition,
                    hash,
                    success,
                    reason,
                    synced_peer_id,
                    backoff,
                } => {
                    worker_finished.insert(
                        (partition, hash),
                        (success, reason, synced_peer_id, backoff),
                    );
                }
            }
        }

        debug!(
            batch_len,
            worker_started = worker_started.len(),
            downloads_started = started.len(),
            progress_updates = progress_latest.len(),
            materialize_started = materialize_started.len(),
            downloads_finished = finished.len(),
            worker_finished = worker_finished.len(),
            "drained sync progress batch"
        );

        for (partition, hash) in worker_started {
            self.emit_blob_worker_status(partition, &hash, "worker started".to_string(), None)
                .await?;
        }

        for key in started {
            self.emit_blob_progress_start(key.peer_id, key.partition, &key.hash)
                .await?;
        }

        for (key, done) in progress_latest {
            if finished.contains_key(&key) {
                continue;
            }
            self.emit_blob_progress_amount(key.peer_id, key.partition, &key.hash, done)
                .await?;
        }

        for (partition, hash) in materialize_started {
            self.emit_blob_worker_status(
                partition,
                &hash,
                "put_from_store started".to_string(),
                None,
            )
            .await?;
        }

        for (key, success) in finished {
            self.emit_blob_progress_finished(key.peer_id, key.partition, &key.hash, success)
                .await?;
        }

        for ((partition, hash), (success, reason, synced_peer_id, backoff)) in worker_finished {
            self.emit_blob_worker_status(
                partition,
                &hash,
                reason,
                Some(if success {
                    ProgressFinalState::Succeeded
                } else {
                    ProgressFinalState::Failed
                }),
            )
            .await?;
            if let Some(peer_id) = synced_peer_id {
                self.handle_blob_marked_synced(hash.clone(), Some(peer_id))
                    .await?;
            }
            if let Some((delay, retry)) = backoff {
                self.handle_blob_request_backoff(
                    hash,
                    delay,
                    retry.attempt_no,
                    retry.last_backoff,
                    retry.last_attempt_at,
                )
                .await?;
            }
        }

        Ok(())
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
