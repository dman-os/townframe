// FIXME; split the Worker into multiple states

use crate::interlude::*;

use am_utils_rs::sync::{
    node::SyncNodeHandle,
    peer::{
        PeerSyncProgressEvent, PeerSyncWorkerEvent, PeerSyncWorkerStopToken, SamodSyncAck,
        SamodSyncRequest, SpawnPeerSyncWorkerArgs,
    },
    protocol::{PartitionId, PartitionSyncRpc, PeerKey},
    store::SyncStoreHandle,
};
use iroh::EndpointId;
use samod::ConnectionId;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::blobs::BlobsRepo;
use crate::index::{DocBlobsIndexEvent, DocBlobsIndexRepo};
use crate::progress::{
    CreateProgressTaskArgs, ProgressFinalState, ProgressRepo, ProgressRetentionPolicy,
    ProgressSeverity, ProgressUnit, ProgressUpdate, ProgressUpdateDeets,
};
use crate::repo::RepoCtx;
use crate::sync::PARTITION_SYNC_ALPN;

mod blob_worker;
mod doc_worker;

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
        endpoint_id: EndpointId,
        doc_count: usize,
    },
    DocSyncedWithPeer {
        endpoint_id: EndpointId,
        doc_id: DocumentId,
    },
    BlobSynced {
        hash: String,
        endpoint_id: Option<EndpointId>,
    },
    BlobDownloadStarted {
        endpoint_id: EndpointId,
        partition: PartitionKey,
        hash: String,
    },
    BlobDownloadFinished {
        endpoint_id: EndpointId,
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
        endpoint_id: EndpointId,
    },
}

enum Msg {
    SetPeer {
        endpoint_id: EndpointId,
        conn_id: ConnectionId,
        partitions: HashSet<PartitionKey>,
        peer_key: PeerKey,
        resp: tokio::sync::oneshot::Sender<()>,
    },
    // OutgoingConn {
    //     endpoint_id: EndpointId,
    //     conn_id: samod::ConnectionId,
    //     resp: tokio::sync::oneshot::Sender<()>,
    // },
    DelPeer {
        endpoint_id: EndpointId,
        resp: tokio::sync::oneshot::Sender<()>,
    },
    DocPeerStateViewUpdated {
        doc_id: DocumentId,
        diff: DocPeerStateView,
    },
    DocSyncRequestBackoff {
        doc_id: DocumentId,
        delay: Duration,
        previous_attempt_no: usize,
        previous_backoff: Duration,
        previous_attempt_at: std::time::Instant,
    },
    DocSyncMissingLocal {
        doc_id: DocumentId,
    },
    BlobSyncMarkedSynced {
        hash: String,
        endpoint_id: Option<EndpointId>,
    },
    BlobSyncRequestBackoff {
        hash: String,
        delay: Duration,
        previous_attempt_no: usize,
        previous_backoff: Duration,
        previous_attempt_at: std::time::Instant,
    },
    PeerSyncWorkerProgress {
        endpoint_id: EndpointId,
        event: PeerSyncProgressEvent,
    },
    PeerSyncWorkerEvent {
        endpoint_id: EndpointId,
        event: PeerSyncWorkerEvent,
    },
    GetPeerSyncSnapshot {
        endpoint_ids: Vec<EndpointId>,
        resp: tokio::sync::oneshot::Sender<HashMap<EndpointId, PeerSyncSnapshot>>,
    },
}
type DocPeerStateView = HashMap<ConnectionId, samod::PeerDocState>;

#[derive(Debug, Clone)]
pub(crate) struct PeerSyncSnapshot {
    pub emitted_full_synced: bool,
}

#[derive(Debug)]
#[allow(clippy::enum_variant_names)]
enum SyncProgressMsg {
    BlobWorkerStarted {
        partition: PartitionKey,
        hash: String,
    },
    BlobDownloadStarted {
        endpoint_id: EndpointId,
        partition: PartitionKey,
        hash: String,
    },
    BlobDownloadProgress {
        endpoint_id: EndpointId,
        partition: PartitionKey,
        hash: String,
        done: u64,
    },
    BlobMaterializeStarted {
        partition: PartitionKey,
        hash: String,
    },
    BlobDownloadFinished {
        endpoint_id: EndpointId,
        partition: PartitionKey,
        hash: String,
        success: bool,
    },
    BlobWorkerFinished {
        partition: PartitionKey,
        hash: String,
        success: bool,
        reason: String,
    },
}

impl WorkerHandle {
    pub async fn set_connection(
        &self,
        endpoint_id: EndpointId,
        conn_id: ConnectionId,
        peer_key: PeerKey,
        partition_ids: HashSet<PartitionId>,
    ) -> Res<()> {
        let mut partitions: HashSet<PartitionKey> = partition_ids
            .iter()
            .cloned()
            .map(PartitionKey::BigRepoPartition)
            .collect();
        partitions.insert(PartitionKey::DocBlobsFullSync);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let msg = Msg::SetPeer {
            resp: tx,
            conn_id,
            endpoint_id,
            partitions,
            peer_key,
        };
        self.msg_tx.send(msg).wrap_err(ERROR_ACTOR)?;
        rx.await.wrap_err(ERROR_CHANNEL)
    }
    pub async fn del_connection(&self, endpoint_id: EndpointId) -> Res<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let msg = Msg::DelPeer {
            resp: tx,
            endpoint_id,
        };
        self.msg_tx.send(msg).wrap_err(ERROR_ACTOR)?;
        rx.await.wrap_err(ERROR_CHANNEL)
    }

    pub async fn get_peer_sync_snapshot(
        &self,
        endpoint_ids: &[EndpointId],
    ) -> Res<HashMap<EndpointId, PeerSyncSnapshot>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let msg = Msg::GetPeerSyncSnapshot {
            endpoint_ids: endpoint_ids.to_vec(),
            resp: tx,
        };
        self.msg_tx.send(msg).wrap_err(ERROR_ACTOR)?;
        rx.await.wrap_err(ERROR_CHANNEL)
    }
}

pub struct StopToken {
    cancel_token: CancellationToken,
    join_handle: tokio::task::JoinHandle<()>,
}

impl StopToken {
    pub async fn stop(self) -> Result<(), utils_rs::WaitOnHandleError> {
        self.cancel_token.cancel();
        utils_rs::wait_on_handle_with_timeout(self.join_handle, Duration::from_secs(5)).await
    }
}

pub async fn start_full_sync_worker(
    rcx: Arc<RepoCtx>,
    blobs_repo: Arc<BlobsRepo>,
    doc_blobs_index_repo: Arc<DocBlobsIndexRepo>,
    progress_repo: Option<Arc<ProgressRepo>>,
    partition_sync_node: Arc<SyncNodeHandle>,
    sync_store: SyncStoreHandle,
    iroh_endpoint: iroh::Endpoint,
) -> Res<(WorkerHandle, StopToken)> {
    use crate::repos::Repo;

    let (msg_tx, mut msg_rx) = mpsc::unbounded_channel();
    let (sync_progress_tx, mut sync_progress_rx) = mpsc::channel::<SyncProgressMsg>(8192);
    let (samod_sync_tx, mut samod_sync_rx) = mpsc::channel::<SamodSyncRequest>(8192);
    let (events_tx, events_rx) = tokio::sync::broadcast::channel(16);

    let cancel_token = CancellationToken::new();

    let mut worker = Worker {
        big_repo: Arc::clone(&rcx.big_repo),
        local_peer_key: rcx.big_repo.samod_repo().peer_id().to_string(),
        cancel_token: cancel_token.clone(),
        msg_tx: msg_tx.clone(),
        sync_progress_tx: sync_progress_tx.clone(),
        samod_sync_tx: samod_sync_tx.clone(),
        events_tx,
        partitions: [(
            PartitionKey::DocBlobsFullSync,
            Partition {
                is_active: false,
                deets: ParitionDeets::DocBlobsFullSync { peers: default() },
            },
        )]
        .into(),

        blobs_repo,
        doc_blobs_index_repo,
        progress_repo,
        partition_sync_node,
        sync_store,
        iroh_endpoint,

        samod_doc_set: default(),
        active_docs: default(),
        pending_docs: default(),

        known_blob_set: default(),
        doc_to_known_hashes: default(),
        active_blobs: default(),
        pending_blobs: default(),
        synced_blobs: default(),

        known_peer_set: default(),
        conn_by_peer: default(),
        endpoint_by_peer_key: default(),
        peer_partition_sessions: default(),

        docs_to_boot: default(),
        docs_to_stop: default(),
        blobs_to_boot: default(),

        partitions_to_refresh: default(),
        peer_sessions_to_refresh: default(),
        max_active_sync_workers: 24,
    };

    let doc_blobs_rx = worker
        .doc_blobs_index_repo
        .subscribe(crate::repos::SubscribeOpts { capacity: 1024 });
    for (doc_id, hash) in worker.doc_blobs_index_repo.list_all_memberships().await? {
        worker
            .doc_to_known_hashes
            .entry(doc_id)
            .or_default()
            .insert(hash.clone());
        worker
            .known_blob_set
            .entry(hash.clone())
            .or_default()
            .insert(PartitionKey::DocBlobsFullSync);
        worker.blobs_to_boot.insert(hash);
    }

    let fut = {
        let cancel_token = cancel_token.clone();
        let mut janitor_tick = tokio::time::interval(Duration::from_millis(500));
        async move {
            let doc_blobs_rx = doc_blobs_rx;
            let mut sync_progress_buf = Vec::with_capacity(512);
            let mut samod_sync_buf = Vec::with_capacity(512);
            loop {
                if !worker.partitions_to_refresh.is_empty() {
                    worker.batch_refresh_paritions().await?;
                }
                if !worker.peer_sessions_to_refresh.is_empty() {
                    worker.batch_refresh_peer_sessions().await?;
                }
                if !worker.docs_to_stop.is_empty() {
                    worker.batch_stop_docs().await?;
                }
                if !worker.docs_to_boot.is_empty() {
                    worker.batch_boot_docs().await?;
                }
                if !worker.blobs_to_boot.is_empty() {
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
                    count = samod_sync_rx.recv_many(&mut samod_sync_buf, 512) => {
                        if count == 0 {
                            continue;
                        }
                        worker.handle_samod_sync_batch(&mut samod_sync_buf).await?;
                    }
                    _ = janitor_tick.tick() => {
                        worker.backoff_janitor_enqueue_due();
                    }
                    val = doc_blobs_rx.recv_async() => {
                        let evt = match val {
                            Ok(val) => val,
                            Err(err) => match err {
                                crate::repos::RecvError::Closed => {
                                    warn!("DocBlobsIndexRepo shutdown, closing loop");
                                    break;
                                },
                                crate::repos::RecvError::Dropped { dropped_count } => {
                                    eyre::bail!("we're dropping doc_blobs index events: dropped_count = {dropped_count}");
                                }
                            },
                        };
                        worker.handle_doc_blobs_event(&evt).await?;
                    }
                }
            }
            worker
                .docs_to_stop
                .extend(worker.active_docs.keys().cloned());
            worker.batch_stop_docs().await?;
            worker.batch_stop_blobs().await?;
            for (_endpoint_id, mut session) in worker.peer_partition_sessions.drain() {
                session.forward_cancel_token.cancel();
                session.stop.stop().await?;
                for join_handle in session.forward_handles.drain(..) {
                    utils_rs::wait_on_handle_with_timeout(join_handle, Duration::from_secs(1))
                        .await?;
                }
            }
            let registered_peers = worker
                .known_peer_set
                .values()
                .map(|state| state.peer_key.clone())
                .collect::<HashSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            for peer_key in registered_peers {
                worker
                    .partition_sync_node
                    .unregister_local_peer(peer_key)
                    .await?;
            }
            eyre::Ok(())
        }
    };
    let join_handle = tokio::task::spawn(
        async {
            fut.await.unwrap();
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
    doc_blobs_index_repo: Arc<DocBlobsIndexRepo>,
    progress_repo: Option<Arc<ProgressRepo>>,
    partition_sync_node: Arc<SyncNodeHandle>,
    sync_store: SyncStoreHandle,
    iroh_endpoint: iroh::Endpoint,

    partitions: HashMap<PartitionKey, Partition>,

    docs_to_boot: HashSet<DocumentId>,
    docs_to_stop: HashSet<DocumentId>,
    blobs_to_boot: HashSet<String>,
    partitions_to_refresh: HashSet<PartitionKey>,
    peer_sessions_to_refresh: HashSet<EndpointId>,

    // doc_ref_counts: HashMap<DocumentId, usize>,
    known_blob_set: HashMap<String, HashSet<PartitionKey>>,
    doc_to_known_hashes: HashMap<String, HashSet<String>>,
    // peer_docs: HashMap<EndpointId, HashSet<DocumentId>>,
    samod_doc_set: HashMap<DocumentId, SamodSyncedDoc>,
    active_docs: HashMap<DocumentId, ActiveDocSyncState>,
    pending_docs: HashMap<DocumentId, PendingDocSyncState>,

    active_blobs: HashMap<String, ActiveBlobSyncState>,
    pending_blobs: HashMap<String, PendingBlobSyncState>,
    synced_blobs: HashMap<String, SyncedBlobSyncState>,

    msg_tx: mpsc::UnboundedSender<Msg>,
    sync_progress_tx: mpsc::Sender<SyncProgressMsg>,
    samod_sync_tx: mpsc::Sender<SamodSyncRequest>,
    events_tx: tokio::sync::broadcast::Sender<FullSyncEvent>,
    max_active_sync_workers: usize,

    known_peer_set: HashMap<ConnectionId, PeerSyncState>,
    conn_by_peer: HashMap<EndpointId, ConnectionId>,
    endpoint_by_peer_key: HashMap<PeerKey, EndpointId>,
    peer_partition_sessions: HashMap<EndpointId, PeerPartitionSession>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum PartitionKey {
    BigRepoPartition(PartitionId),
    DocBlobsFullSync,
    // Docs(Uuid),
    // Blobs(Uuid),
}

impl PartitionKey {
    pub(crate) fn as_tag_value(&self) -> String {
        match self {
            Self::BigRepoPartition(partition_id) => format!("big_repo_partition/{partition_id}"),
            Self::DocBlobsFullSync => "doc_blobs_full".to_string(),
        }
    }
}

struct Partition {
    is_active: bool,
    deets: ParitionDeets,
}

enum ParitionDeets {
    BigRepoPartition { peers: HashSet<EndpointId> },
    DocBlobsFullSync { peers: HashSet<EndpointId> },
    // Docs { include_set: HashSet<DocumentId> },
    // Blobs { include_set: HashMap<Uuid> },
}

struct PeerSyncState {
    endpoint_id: EndpointId,
    partitions: HashSet<PartitionKey>,
    peer_key: PeerKey,
    bootstrap_ready: bool,
    live_ready: bool,
    bootstrap_synced_docs: u64,
    bootstrap_remaining_docs: u64,
    samod_pending_docs: u64,
    emitted_full_synced: bool,
}

struct PeerPartitionSession {
    stop: PeerSyncWorkerStopToken,
    samod_ack_tx: mpsc::Sender<SamodSyncAck>,
    forward_cancel_token: CancellationToken,
    forward_handles: Vec<tokio::task::JoinHandle<()>>,
    partitions: HashSet<PartitionKey>,
}

#[derive(Clone, Copy)]
pub(super) struct RetryState {
    pub attempt_no: usize,
    pub last_backoff: Duration,
    pub last_attempt_at: std::time::Instant,
}

#[derive(Default)]
struct SamodSyncedDoc {
    requested_peers: HashMap<EndpointId, HashMap<PartitionKey, HashSet<u64>>>,
}

struct ActiveDocSyncState {
    latest_heads: ChangeHashSet,
    stop_token: doc_worker::DocSyncWorkerStopToken,
}

#[derive(Debug, Clone)]
struct PendingDocSyncState {
    attempt_no: usize,
    last_backoff: Duration,
    last_attempt_at: std::time::Instant,
    due_at: std::time::Instant,
}

struct ActiveBlobSyncState {
    stop_token: blob_worker::BlobSyncWorkerStopToken,
}

#[derive(Debug, Clone)]
struct PendingBlobSyncState {
    attempt_no: usize,
    last_backoff: Duration,
    last_attempt_at: std::time::Instant,
    due_at: std::time::Instant,
}

struct SyncedBlobSyncState {
    _synced_at: std::time::Instant,
    _last_peer: Option<EndpointId>,
}

#[derive(Hash, PartialEq, Eq)]
struct BlobProgressKey {
    endpoint_id: EndpointId,
    partition: PartitionKey,
    hash: String,
}

impl Worker {
    async fn handle_peer_sync_worker_progress(
        &mut self,
        endpoint_id: EndpointId,
        event: PeerSyncProgressEvent,
    ) -> Res<()> {
        match event {
            PeerSyncProgressEvent::PhaseStarted { phase } => {
                debug!(endpoint_id = ?endpoint_id, phase, "peer sync phase started");
            }
            PeerSyncProgressEvent::PhaseFinished { phase, elapsed } => {
                debug!(endpoint_id = ?endpoint_id, phase, elapsed_ms = elapsed.as_millis(), "peer sync phase finished");
            }
            PeerSyncProgressEvent::DocSyncStatus {
                synced_docs,
                remaining_docs,
            } => {
                debug!(
                    endpoint_id = ?endpoint_id,
                    synced_docs,
                    remaining_docs,
                    "peer sync worker doc status"
                );
                if let Some(conn_id) = self.conn_by_peer.get(&endpoint_id).copied() {
                    if let Some(peer_state) = self.known_peer_set.get_mut(&conn_id) {
                        peer_state.bootstrap_synced_docs = synced_docs;
                        peer_state.bootstrap_remaining_docs = remaining_docs;
                    }
                }
                self.refresh_peer_fully_synced_state(endpoint_id).await?;
            }
        }
        Ok(())
    }

    async fn handle_peer_sync_worker_event(
        &mut self,
        endpoint_id: EndpointId,
        event: PeerSyncWorkerEvent,
    ) -> Res<()> {
        match event {
            PeerSyncWorkerEvent::Bootstrapped {
                peer,
                partition_count,
            } => {
                debug!(endpoint_id = ?endpoint_id, peer, partition_count, "peer sync worker bootstrapped");
                if let Some(conn_id) = self.conn_by_peer.get(&endpoint_id).copied() {
                    if let Some(peer_state) = self.known_peer_set.get_mut(&conn_id) {
                        peer_state.bootstrap_ready = true;
                        peer_state.bootstrap_remaining_docs = 0;
                    }
                }
                self.refresh_peer_fully_synced_state(endpoint_id).await?;
            }
            PeerSyncWorkerEvent::LiveReady { peer } => {
                debug!(endpoint_id = ?endpoint_id, peer, "peer sync worker entered live mode");
                if let Some(conn_id) = self.conn_by_peer.get(&endpoint_id).copied() {
                    if let Some(peer_state) = self.known_peer_set.get_mut(&conn_id) {
                        peer_state.live_ready = true;
                    }
                }
                self.refresh_peer_fully_synced_state(endpoint_id).await?;
            }
            PeerSyncWorkerEvent::AbnormalExit { peer, reason } => {
                warn!(endpoint_id = ?endpoint_id, peer, reason, "peer sync worker exited abnormally");
                self.emit_stale_peer(endpoint_id).await?;
                self.peer_sessions_to_refresh.insert(endpoint_id);
            }
        }
        Ok(())
    }

    fn endpoint_for_peer_key(&self, peer_key: &str) -> Option<EndpointId> {
        self.endpoint_by_peer_key.get(peer_key).copied()
    }

    fn active_worker_count(&self) -> usize {
        self.active_docs.len() + self.active_blobs.len()
    }

    fn available_total_boot_budget(&self) -> usize {
        self.max_active_sync_workers
            .saturating_sub(self.active_worker_count())
    }

    fn available_doc_boot_budget(&self) -> usize {
        let remaining_total = self.available_total_boot_budget();
        if remaining_total == 0 {
            return 0;
        }
        let doc_cap = self
            .max_active_sync_workers
            .saturating_sub(MIN_BLOB_WORKER_FLOOR);
        let remaining_doc_cap = doc_cap.saturating_sub(self.active_docs.len());
        remaining_total.min(remaining_doc_cap)
    }

    fn available_blob_boot_budget(&self) -> usize {
        let remaining_total = self.available_total_boot_budget();
        if remaining_total == 0 {
            return 0;
        }
        let blob_cap = self
            .max_active_sync_workers
            .saturating_sub(MIN_DOC_WORKER_FLOOR);
        let remaining_blob_cap = blob_cap.saturating_sub(self.active_blobs.len());
        remaining_total.min(remaining_blob_cap)
    }

    async fn handle_msg(&mut self, msg: Msg) -> Res<()> {
        match msg {
            Msg::SetPeer {
                endpoint_id,
                resp,
                partitions,
                conn_id,
                peer_key,
            } => {
                let old_conn_id = self.conn_by_peer.get(&endpoint_id).copied();
                let old_state = self
                    .known_peer_set
                    .remove(&conn_id)
                    .or_else(|| old_conn_id.and_then(|id| self.known_peer_set.remove(&id)));
                if let Some(old_state) = old_state.as_ref() {
                    if old_state.peer_key != peer_key {
                        self.endpoint_by_peer_key.remove(&old_state.peer_key);
                        self.partition_sync_node
                            .unregister_local_peer(old_state.peer_key.clone())
                            .await?;
                        self.partition_sync_node
                            .register_local_peer(peer_key.clone())
                            .await?;
                    }
                } else {
                    self.partition_sync_node
                        .register_local_peer(peer_key.clone())
                        .await?;
                }
                let new_parts: Vec<_> = if let Some(old) = old_state {
                    for part_key in old.partitions.difference(&partitions) {
                        self.remove_peer_from_part(part_key.clone(), endpoint_id)
                            .await?;
                    }
                    partitions.difference(&old.partitions).cloned().collect()
                } else {
                    partitions.iter().cloned().collect()
                };
                self.known_peer_set.insert(
                    conn_id,
                    PeerSyncState {
                        partitions,
                        endpoint_id,
                        peer_key: peer_key.clone(),
                        bootstrap_ready: false,
                        live_ready: false,
                        bootstrap_synced_docs: 0,
                        bootstrap_remaining_docs: 0,
                        samod_pending_docs: 0,
                        emitted_full_synced: false,
                    },
                );
                self.conn_by_peer.insert(endpoint_id, conn_id);
                self.endpoint_by_peer_key
                    .insert(peer_key.clone(), endpoint_id);
                for part_key in new_parts {
                    self.add_peer_to_part(part_key, endpoint_id).await?;
                }
                resp.send(()).inspect_err(|_| warn!(ERROR_CALLER)).ok();
            }
            Msg::DelPeer { endpoint_id, resp } => {
                if let Some(conn_id) = self.conn_by_peer.remove(&endpoint_id) {
                    if let Some(state) = self.known_peer_set.remove(&conn_id) {
                        self.endpoint_by_peer_key.remove(&state.peer_key);
                        self.partition_sync_node
                            .unregister_local_peer(state.peer_key)
                            .await?;
                        self.remove_peer_from_samod_doc_set(endpoint_id).await?;
                        self.remove_peer_partition_session(endpoint_id).await?;
                        for part_key in state.partitions {
                            self.remove_peer_from_part(part_key, endpoint_id).await?;
                        }
                    }
                }
                self.endpoint_by_peer_key
                    .retain(|_peer_key, cached_endpoint| *cached_endpoint != endpoint_id);
                resp.send(()).inspect_err(|_| warn!(ERROR_CALLER)).ok();
            }
            Msg::DocPeerStateViewUpdated { doc_id, diff } => {
                self.handle_doc_peer_state_change(doc_id, diff).await?
            }
            Msg::DocSyncRequestBackoff {
                doc_id,
                delay,
                previous_attempt_no,
                previous_backoff,
                previous_attempt_at,
            } => {
                self.handle_doc_request_backoff(
                    doc_id,
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
            Msg::BlobSyncMarkedSynced { hash, endpoint_id } => {
                self.handle_blob_marked_synced(hash, endpoint_id).await?;
            }
            Msg::BlobSyncRequestBackoff {
                hash,
                delay,
                previous_attempt_no,
                previous_backoff,
                previous_attempt_at,
            } => {
                self.handle_blob_request_backoff(
                    hash,
                    delay,
                    previous_attempt_no,
                    previous_backoff,
                    previous_attempt_at,
                )
                .await?;
            }
            Msg::PeerSyncWorkerProgress { endpoint_id, event } => {
                self.handle_peer_sync_worker_progress(endpoint_id, event)
                    .await?;
            }
            Msg::PeerSyncWorkerEvent { endpoint_id, event } => {
                self.handle_peer_sync_worker_event(endpoint_id, event)
                    .await?;
            }
            Msg::GetPeerSyncSnapshot { endpoint_ids, resp } => {
                let snapshot = endpoint_ids
                    .into_iter()
                    .filter_map(|endpoint_id| {
                        let conn_id = self.conn_by_peer.get(&endpoint_id).copied()?;
                        let peer_state = self.known_peer_set.get(&conn_id)?;
                        Some((
                            endpoint_id,
                            PeerSyncSnapshot {
                                emitted_full_synced: peer_state.emitted_full_synced,
                            },
                        ))
                    })
                    .collect();
                resp.send(snapshot)
                    .inspect_err(|_| warn!(ERROR_CALLER))
                    .ok();
            }
        }
        eyre::Ok(())
    }

    async fn batch_refresh_paritions(&mut self) -> Res<()> {
        let mut double = std::mem::replace(&mut self.partitions_to_refresh, default());
        let mut affected_peers = HashSet::new();
        for part_key in double.drain() {
            for peer_state in self.known_peer_set.values() {
                if peer_state.partitions.contains(&part_key) {
                    affected_peers.insert(peer_state.endpoint_id);
                }
            }
            let (activate, is_active) = {
                let part = self
                    .partitions
                    .get(&part_key)
                    .ok_or_eyre("parition not found")?;
                let activate = match &part.deets {
                    ParitionDeets::BigRepoPartition { peers } => {
                        for endpoint_id in peers {
                            affected_peers.insert(*endpoint_id);
                        }
                        !peers.is_empty()
                    }
                    ParitionDeets::DocBlobsFullSync { peers } => !peers.is_empty(),
                };
                (activate, part.is_active)
            };
            self.partitions
                .get_mut(&part_key)
                .expect("partition should exist")
                .is_active = activate;
            let has_flipped = activate != is_active;
            match &part_key {
                PartitionKey::BigRepoPartition(_) => {
                    // FIXME: this is confusing, surely we
                    // can do docs_to_stop here by looking at state

                    // NOOP: the peer sync worker needs
                    // to request new docs to boot on demand
                    if activate {
                    } else {
                        // // all peers are gone, stop all sync stuff
                        // for doc_id in self
                        //     .active_docs
                        //     .keys()
                        //     .chain(self.pending_docs.keys())
                        //     .cloned()
                        //     .collect::<HashSet<_>>()
                        // {
                        //     self.docs_to_stop.insert(doc_id);
                        // }
                    }
                }
                PartitionKey::DocBlobsFullSync => {
                    if has_flipped {
                        self.refresh_doc_blobs_workers().await?;
                    }
                }
            }
        }
        self.partitions_to_refresh = double;
        let mut sessions_to_remove = Vec::new();
        for endpoint_id in affected_peers {
            let desired_parts = self.desired_big_repo_partitions_for_peer(endpoint_id);
            let Some(conn_id) = self.conn_by_peer.get(&endpoint_id).copied() else {
                if desired_parts.is_empty() {
                    sessions_to_remove.push(endpoint_id);
                }
                continue;
            };
            let Some(_peer_state) = self.known_peer_set.get(&conn_id) else {
                if desired_parts.is_empty() {
                    sessions_to_remove.push(endpoint_id);
                }
                continue;
            };
            if desired_parts.is_empty() {
                sessions_to_remove.push(endpoint_id);
                continue;
            }
            let Some(session) = self.peer_partition_sessions.get(&endpoint_id) else {
                self.peer_sessions_to_refresh.insert(endpoint_id);
                continue;
            };
            if session.partitions != desired_parts {
                self.peer_sessions_to_refresh.insert(endpoint_id);
            }
        }
        for endpoint_id in sessions_to_remove {
            self.remove_peer_partition_session(endpoint_id).await?;
        }
        Ok(())
    }

    fn desired_big_repo_partitions_for_peer(
        &self,
        endpoint_id: EndpointId,
    ) -> HashSet<PartitionKey> {
        self.partitions
            .iter()
            .filter_map(|(part_key, part)| match (&part_key, &part.deets) {
                (PartitionKey::BigRepoPartition(_), ParitionDeets::BigRepoPartition { peers })
                    if peers.contains(&endpoint_id) =>
                {
                    Some(part_key.clone())
                }
                _ => None,
            })
            .collect()
    }

    async fn add_peer_to_part(
        &mut self,
        part_key: PartitionKey,
        endpoint_id: EndpointId,
    ) -> Res<()> {
        let part = self
            .partitions
            .entry(part_key.clone())
            .or_insert_with(|| Partition {
                is_active: false,
                deets: match &part_key {
                    PartitionKey::BigRepoPartition(_) => {
                        ParitionDeets::BigRepoPartition { peers: default() }
                    }
                    PartitionKey::DocBlobsFullSync => {
                        ParitionDeets::DocBlobsFullSync { peers: default() }
                    }
                },
            });
        match &mut part.deets {
            ParitionDeets::BigRepoPartition { peers } => {
                peers.insert(endpoint_id);
                self.docs_to_boot.extend(self.pending_docs.keys().cloned());
            }
            ParitionDeets::DocBlobsFullSync { peers } => {
                peers.insert(endpoint_id);
                self.blobs_to_boot
                    .extend(self.pending_blobs.keys().cloned());
            }
        }
        self.partitions_to_refresh.insert(part_key);
        Ok(())
    }

    async fn remove_peer_from_part(
        &mut self,
        part_key: PartitionKey,
        endpoint_id: EndpointId,
    ) -> Res<()> {
        let part = self
            .partitions
            .entry(part_key.clone())
            .or_insert_with(|| Partition {
                is_active: false,
                deets: match &part_key {
                    PartitionKey::BigRepoPartition(_) => {
                        ParitionDeets::BigRepoPartition { peers: default() }
                    }
                    PartitionKey::DocBlobsFullSync => {
                        ParitionDeets::DocBlobsFullSync { peers: default() }
                    }
                },
            });
        match &mut part.deets {
            ParitionDeets::BigRepoPartition { peers } => {
                peers.remove(&endpoint_id);
            }
            ParitionDeets::DocBlobsFullSync { peers } => {
                peers.remove(&endpoint_id);
            }
        }
        self.partitions_to_refresh.insert(part_key);
        Ok(())
    }

    fn backoff_janitor_enqueue_due(&mut self) {
        let now = std::time::Instant::now();
        let doc_budget = self.available_doc_boot_budget();
        let blob_budget = self.available_blob_boot_budget();

        let due_docs: Vec<_> = self
            .pending_docs
            .iter()
            .filter_map(|(doc_id, pending)| {
                if pending.due_at <= now && !self.active_docs.contains_key(doc_id) {
                    Some(doc_id.clone())
                } else {
                    None
                }
            })
            .take(doc_budget)
            .collect();
        self.docs_to_boot.extend(due_docs);

        self.blobs_to_boot.extend(
            self.pending_blobs
                .iter()
                .filter_map(|(hash, pending)| {
                    if pending.due_at <= now
                        && !self.active_blobs.contains_key(hash)
                        && !self.synced_blobs.contains_key(hash)
                    {
                        Some(hash.clone())
                    } else {
                        None
                    }
                })
                .take(blob_budget),
        );
    }
}

// Docs related methods
impl Worker {
    async fn batch_refresh_peer_sessions(&mut self) -> Res<()> {
        let mut double = std::mem::replace(&mut self.peer_sessions_to_refresh, default());
        for endpoint_id in double.drain() {
            let Some(conn_id) = self.conn_by_peer.get(&endpoint_id).copied() else {
                continue;
            };
            let Some(peer_state) = self.known_peer_set.get(&conn_id) else {
                continue;
            };
            let peer_key = peer_state.peer_key.clone();
            let parts = self.desired_big_repo_partitions_for_peer(endpoint_id);
            self.refresh_peer_partition_session(endpoint_id, peer_key, parts)
                .await?;
        }
        self.peer_sessions_to_refresh = double;
        Ok(())
    }

    async fn refresh_peer_partition_session(
        &mut self,
        endpoint_id: EndpointId,
        peer_key: PeerKey,
        partitions: HashSet<PartitionKey>,
    ) -> Res<()> {
        let session = self.peer_partition_sessions.remove(&endpoint_id);
        if let Some(session) = session {
            session.stop.stop().await?;
        }
        if partitions.is_empty() {
            return Ok(());
        }
        let rpc_client = irpc_iroh::client::<PartitionSyncRpc>(
            self.iroh_endpoint.clone(),
            iroh::EndpointAddr::new(endpoint_id),
            PARTITION_SYNC_ALPN,
        );
        let (samod_ack_tx, samod_ack_rx) = mpsc::channel(8192);
        let (mut handle, stop) =
            am_utils_rs::sync::peer::spawn_peer_sync_worker(SpawnPeerSyncWorkerArgs {
                local_peer: self.local_peer_key.clone(),
                remote_peer: peer_key,
                rpc_client,
                local_repo: Arc::clone(&self.big_repo),
                sync_store: self.sync_store.clone(),
                samod_sync_tx: self.samod_sync_tx.clone(),
                samod_ack_rx,
                target_partitions: partitions
                    .iter()
                    .map(|key| match key {
                        PartitionKey::DocBlobsFullSync => unreachable!(),
                        PartitionKey::BigRepoPartition(id) => id.clone(),
                    })
                    .collect(),
            })
            .await?;
        let forward_cancel_token = self.cancel_token.child_token();
        let mut forward_handles = Vec::with_capacity(2);
        if let Some(mut progress_rx) = handle.progress_rx.take() {
            let msg_tx = self.msg_tx.clone();
            let cancel = forward_cancel_token.child_token();
            let join_handle = tokio::spawn(async move {
                loop {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => break,
                        recv = progress_rx.recv() => match recv {
                            Ok(event) => {
                                msg_tx.send(Msg::PeerSyncWorkerProgress { endpoint_id, event })
                                    .inspect_err(|_| warn!("full sync worker closed while forwarding peer progress"))
                                    .ok();
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(dropped)) => {
                                warn!(endpoint_id = ?endpoint_id, dropped, "lagged forwarding peer worker progress");
                            }
                        }
                    }
                }
            });
            forward_handles.push(join_handle);
        }
        if let Some(mut events_rx) = handle.events_rx.take() {
            let msg_tx = self.msg_tx.clone();
            let cancel = forward_cancel_token.child_token();
            let join_handle = tokio::spawn(async move {
                loop {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => break,
                        recv = events_rx.recv() => match recv {
                            Ok(event) => {
                                msg_tx.send(Msg::PeerSyncWorkerEvent { endpoint_id, event })
                                    .inspect_err(|_| warn!("full sync worker closed while forwarding peer event"))
                                    .ok();
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(dropped)) => {
                                warn!(endpoint_id = ?endpoint_id, dropped, "lagged forwarding peer worker event");
                            }
                        }
                    }
                }
            });
            forward_handles.push(join_handle);
        }
        self.peer_partition_sessions.insert(
            endpoint_id,
            PeerPartitionSession {
                stop,
                samod_ack_tx,
                forward_cancel_token,
                forward_handles,
                partitions,
            },
        );
        Ok(())
    }

    async fn remove_peer_partition_session(&mut self, endpoint_id: EndpointId) -> Res<()> {
        let session = self.peer_partition_sessions.remove(&endpoint_id);
        if let Some(mut session) = session {
            session.forward_cancel_token.cancel();
            session.stop.stop().await?;
            for join_handle in session.forward_handles.drain(..) {
                utils_rs::wait_on_handle_with_timeout(join_handle, Duration::from_secs(1)).await?;
            }
        }
        Ok(())
    }

    async fn handle_samod_sync_batch(&mut self, buffer: &mut Vec<SamodSyncRequest>) -> Res<()> {
        for req in buffer.drain(..) {
            self.handle_samod_sync_request(req).await?;
        }
        Ok(())
    }

    async fn handle_samod_sync_request(&mut self, req: SamodSyncRequest) -> Res<()> {
        match req {
            SamodSyncRequest::RequestDocSync {
                peer_key,
                partition_id,
                doc_id,
                cursor,
            } => {
                let doc_id = doc_id
                    .parse::<DocumentId>()
                    .map_err(|err| ferr!("invalid doc id '{}': {err}", doc_id))?;
                let Some(endpoint_id) = self.endpoint_for_peer_key(&peer_key) else {
                    debug!(peer_key, %doc_id, "dropping samod doc sync request for unknown peer");
                    return Ok(());
                };
                self.emit_stale_peer(endpoint_id).await?;
                let samod_doc_state = self.samod_doc_set.entry(doc_id.clone()).or_default();
                let requested_parts = samod_doc_state
                    .requested_peers
                    .entry(endpoint_id)
                    .or_default();
                let was_empty_for_peer = requested_parts.values().all(|cursors| cursors.is_empty());
                requested_parts
                    .entry(PartitionKey::BigRepoPartition(partition_id.clone()))
                    .or_default()
                    .insert(cursor);
                if was_empty_for_peer {
                    if let Some(conn_id) = self.conn_by_peer.get(&endpoint_id).copied() {
                        if let Some(peer_state) = self.known_peer_set.get_mut(&conn_id) {
                            peer_state.samod_pending_docs =
                                peer_state.samod_pending_docs.saturating_add(1);
                        }
                    }
                    self.refresh_peer_fully_synced_state(endpoint_id).await?;
                }
                debug!(peer_key, %doc_id, "received samod doc sync request");
                debug!(
                    peer_key,
                    partition_id = %partition_id,
                    %doc_id,
                    cursor,
                    "full worker queued samod doc sync request"
                );
                if !self.active_docs.contains_key(&doc_id)
                    && !self.pending_docs.contains_key(&doc_id)
                {
                    self.pending_docs.insert(
                        doc_id.clone(),
                        PendingDocSyncState {
                            attempt_no: 0,
                            last_backoff: Duration::from_millis(0),
                            last_attempt_at: std::time::Instant::now(),
                            due_at: std::time::Instant::now(),
                        },
                    );
                    self.docs_to_boot.insert(doc_id);
                }
            }
            SamodSyncRequest::DocDeleted {
                peer_key,
                partition_id,
                doc_id,
            } => {
                let doc_id = doc_id
                    .parse::<DocumentId>()
                    .map_err(|err| ferr!("invalid doc id '{}': {err}", doc_id))?;
                let Some(endpoint_id) = self.endpoint_for_peer_key(&peer_key) else {
                    debug!(peer_key, %doc_id, "dropping doc-deleted request for unknown peer");
                    return Ok(());
                };
                self.emit_stale_peer(endpoint_id).await?;
                debug!(
                    peer_key,
                    partition_id = %partition_id,
                    %doc_id,
                    "received samod doc deleted request"
                );
                self.docs_to_stop.insert(doc_id.clone());

                let mut refresh_peer = false;
                let mut clear_doc_request = false;
                if let Some(sync_state) = self.samod_doc_set.get_mut(&doc_id) {
                    let had_peer_entry = sync_state.requested_peers.remove(&endpoint_id).is_some();
                    clear_doc_request = sync_state.requested_peers.is_empty();
                    if had_peer_entry {
                        if let Some(conn_id) = self.conn_by_peer.get(&endpoint_id).copied() {
                            if let Some(peer_state) = self.known_peer_set.get_mut(&conn_id) {
                                peer_state.samod_pending_docs =
                                    peer_state.samod_pending_docs.saturating_sub(1);
                                refresh_peer = true;
                            }
                        }
                    }
                }
                if clear_doc_request {
                    self.samod_doc_set.remove(&doc_id);
                    self.pending_docs.remove(&doc_id);
                    self.docs_to_boot.remove(&doc_id);
                }
                if refresh_peer {
                    self.refresh_peer_fully_synced_state(endpoint_id).await?;
                }
            }
        }
        Ok(())
    }

    async fn batch_boot_docs(&mut self) -> Res<()> {
        let docs_part_active = self
            .partitions
            .iter()
            .any(|(key, part)| matches!(key, PartitionKey::BigRepoPartition(_)) && part.is_active);
        if !docs_part_active {
            return Ok(());
        }

        let mut double = std::mem::replace(&mut self.docs_to_boot, default());
        let mut budget = self.available_doc_boot_budget();
        if budget == 0 {
            self.docs_to_boot = double;
            return Ok(());
        }
        for doc_id in double.drain() {
            if budget == 0 {
                self.docs_to_boot.insert(doc_id);
                continue;
            }
            if self.active_docs.contains_key(&doc_id) {
                continue;
            }
            let prior_pending = self.pending_docs.get(&doc_id).cloned();
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
            match self.boot_doc_sync_worker(doc_id.clone(), retry).await? {
                Some(active) => {
                    self.pending_docs.remove(&doc_id);
                    self.active_docs.insert(doc_id, active);
                    budget = budget.saturating_sub(1);
                }
                None => {
                    self.handle_doc_missing_local(doc_id).await?;
                }
            }
        }
        double.extend(self.docs_to_boot.drain());
        self.docs_to_boot = double;
        Ok(())
    }

    async fn boot_doc_sync_worker(
        &self,
        doc_id: DocumentId,
        retry: RetryState,
    ) -> Res<Option<ActiveDocSyncState>> {
        let cancel_token = self.cancel_token.child_token();
        let Some(local_doc) = self.big_repo.find_doc(&doc_id).await? else {
            return Ok(None);
        };
        let latest_heads = local_doc
            .with_document_local(|doc| ChangeHashSet(Arc::from(doc.get_heads())))
            .await?;
        let stop_token = doc_worker::spawn_doc_sync_worker(
            doc_id.clone(),
            Arc::clone(&self.big_repo),
            cancel_token.clone(),
            self.msg_tx.clone(),
            retry,
        )
        .await?;
        Ok(Some(ActiveDocSyncState {
            latest_heads,
            stop_token,
        }))
    }

    async fn batch_stop_docs(&mut self) -> Res<()> {
        use futures::StreamExt;
        use futures_buffered::BufferedStreamExt;

        let mut stop_futs = vec![];

        let stopped_doc_ids = self.docs_to_stop.drain().collect::<Vec<_>>();
        for doc_id in &stopped_doc_ids {
            if let Some(active) = self.active_docs.remove(doc_id) {
                stop_futs.push(active.stop_token.stop());
            }
            self.pending_docs.remove(doc_id);
        }

        futures::stream::iter(stop_futs)
            .buffered_unordered(16)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Res<Vec<_>>>()?;
        Ok(())
    }

    async fn batch_stop_blobs(&mut self) -> Res<()> {
        use futures::StreamExt;
        use futures_buffered::BufferedStreamExt;

        self.blobs_to_boot.clear();
        self.pending_blobs.clear();
        self.synced_blobs.clear();

        let mut stop_futs = vec![];
        for (_hash, active) in self.active_blobs.drain() {
            stop_futs.push(active.stop_token.stop());
        }

        futures::stream::iter(stop_futs)
            .buffered_unordered(16)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Res<Vec<_>>>()?;
        Ok(())
    }

    async fn handle_doc_peer_state_change(
        &mut self,
        doc_id: DocumentId,
        diff: DocPeerStateView,
    ) -> Res<()> {
        if !self.active_docs.contains_key(&doc_id) {
            return Ok(());
        }
        let local_heads = self
            .big_repo
            .find_doc(&doc_id)
            .await?
            .ok_or_eyre("active doc sync state missing local doc handle")?
            .with_document_local(|doc| ChangeHashSet(Arc::from(doc.get_heads())))
            .await?;
        self.active_docs
            .get_mut(&doc_id)
            .expect("active doc sync state should exist")
            .latest_heads = local_heads.clone();
        let mut events_to_emit = Vec::new();
        let mut peers_to_refresh = Vec::new();
        let mut acks_to_emit = Vec::new();
        for (conn_id, diff) in diff {
            let Some(peer_state) = self.known_peer_set.get_mut(&conn_id) else {
                warn!(?conn_id, "unkown connection for FullSyncWorker");
                continue;
            };
            let they_have_our_changes = diff
                .shared_heads
                .as_ref()
                .map(|heads| heads_equal_as_set(heads, &local_heads))
                .unwrap_or_default();
            let we_have_their_changes = diff
                .their_heads
                .as_ref()
                .zip(diff.shared_heads.as_ref())
                .map(|(their, shared)| heads_equal_as_set(their, shared))
                .unwrap_or_default();

            if they_have_our_changes && we_have_their_changes {
                debug!(
                    endpoint_id = ?peer_state.endpoint_id,
                    %doc_id,
                    local_head_count = local_heads.len(),
                    "full worker observed doc synced with peer"
                );
                events_to_emit.push(FullSyncEvent::DocSyncedWithPeer {
                    endpoint_id: peer_state.endpoint_id,
                    doc_id: doc_id.clone(),
                });
                let Some(samod_doc) = self.samod_doc_set.get_mut(&doc_id) else {
                    continue;
                };
                if let Some(partitions) = samod_doc.requested_peers.remove(&peer_state.endpoint_id)
                {
                    for (partition_key, cursors) in partitions {
                        let PartitionKey::BigRepoPartition(partition_id) = partition_key else {
                            continue;
                        };
                        for cursor in cursors {
                            debug!(
                                endpoint_id = ?peer_state.endpoint_id,
                                partition_id = %partition_id,
                                %doc_id,
                                cursor,
                                "full worker emitting samod ack"
                            );
                            acks_to_emit.push((
                                peer_state.endpoint_id,
                                SamodSyncAck::DocSynced {
                                    partition_id: partition_id.clone(),
                                    doc_id: doc_id.to_string(),
                                    cursor,
                                },
                            ));
                        }
                    }
                    peer_state.samod_pending_docs = peer_state.samod_pending_docs.saturating_sub(1);
                    peers_to_refresh.push(peer_state.endpoint_id);
                }
            }
            // events_to_emit.push(FullSyncEvent::PeerFullSynced {
            //     endpoint_id: peer_state.endpoint_id,
            // });
            // peer_state.emitted_full_synced = true;
        }
        for (endpoint_id, ack) in acks_to_emit {
            let Some(session) = self.peer_partition_sessions.get(&endpoint_id) else {
                warn!(endpoint_id = ?endpoint_id, "missing peer session while emitting samod ack");
                self.peer_sessions_to_refresh.insert(endpoint_id);
                continue;
            };
            if let Err(err) = session.samod_ack_tx.try_send(ack) {
                warn!(endpoint_id = ?endpoint_id, ?err, "failed sending samod ack to peer worker");
                self.peer_sessions_to_refresh.insert(endpoint_id);
            }
        }
        for endpoint_id in peers_to_refresh {
            self.refresh_peer_fully_synced_state(endpoint_id).await?;
        }
        let requested_peers_empty = self
            .samod_doc_set
            .get(&doc_id)
            .map(|state| state.requested_peers.is_empty())
            .unwrap_or(true);
        if self.active_docs.contains_key(&doc_id) && requested_peers_empty {
            let Some(active) = self.active_docs.remove(&doc_id) else {
                return Ok(());
            };
            let _latest_heads = active.latest_heads.clone();
            active.stop_token.stop().await?;
            self.pending_docs.remove(&doc_id);
            self.samod_doc_set.remove(&doc_id);
        }
        for event in events_to_emit {
            self.emit_full_sync_event(event).await?;
        }
        Ok(())
    }

    async fn handle_doc_request_backoff(
        &mut self,
        doc_id: DocumentId,
        delay: Duration,
        previous_attempt_no: usize,
        previous_backoff: Duration,
        _previous_attempt_at: std::time::Instant,
    ) -> Res<()> {
        if let Some(active) = self.active_docs.remove(&doc_id) {
            active.stop_token.stop().await?;
        }
        let now = std::time::Instant::now();
        let delay = delay.min(Duration::from_secs(600));
        let backoff = next_backoff_delay(previous_backoff, delay);
        let pending = PendingDocSyncState {
            attempt_no: previous_attempt_no + 1,
            last_backoff: backoff,
            last_attempt_at: now,
            due_at: now + backoff,
        };
        self.pending_docs.insert(doc_id, pending);
        Ok(())
    }

    async fn handle_doc_missing_local(&mut self, doc_id: DocumentId) -> Res<()> {
        if let Some(active) = self.active_docs.remove(&doc_id) {
            active.stop_token.stop().await?;
        }
        self.pending_docs.remove(&doc_id);
        self.docs_to_boot.remove(&doc_id);

        let Some(sync_state) = self.samod_doc_set.remove(&doc_id) else {
            return Ok(());
        };

        let mut peers_to_refresh = Vec::new();
        for endpoint_id in sync_state.requested_peers.keys() {
            if let Some(conn_id) = self.conn_by_peer.get(endpoint_id).copied() {
                if let Some(peer_state) = self.known_peer_set.get_mut(&conn_id) {
                    peer_state.samod_pending_docs = peer_state.samod_pending_docs.saturating_sub(1);
                    peers_to_refresh.push(*endpoint_id);
                }
            }
        }
        for endpoint_id in peers_to_refresh {
            self.refresh_peer_fully_synced_state(endpoint_id).await?;
        }
        warn!(%doc_id, "doc sync requested for missing local doc; marking request terminal");
        Ok(())
    }
}

// Blobs related methods
impl Worker {
    fn remove_blob_tracking_if_unused(&mut self, hash: &str) {
        let should_drop = match self.known_blob_set.get(hash) {
            Some(parts) => parts.is_empty(),
            None => true,
        };
        if should_drop {
            self.known_blob_set.remove(hash);
            self.synced_blobs.remove(hash);
            self.blobs_to_boot.remove(hash);
            self.pending_blobs.remove(hash);
        }
    }

    fn blob_required_by_any_active_partition(&self, hash: &str) -> bool {
        let Some(parts) = self.known_blob_set.get(hash) else {
            return false;
        };
        parts.iter().any(|part_key| {
            self.partitions
                .get(part_key)
                .is_some_and(|partition| partition.is_active)
        })
    }

    async fn remove_doc_blobs_partition_if_hash_unreferenced(&mut self, hash: &str) -> Res<()> {
        if !self
            .doc_blobs_index_repo
            .list_docs_for_hash(hash)
            .await?
            .is_empty()
        {
            return Ok(());
        }
        if let Some(parts) = self.known_blob_set.get_mut(hash) {
            parts.remove(&PartitionKey::DocBlobsFullSync);
        }
        self.remove_blob_tracking_if_unused(hash);
        Ok(())
    }

    async fn handle_doc_blobs_event(&mut self, evt: &DocBlobsIndexEvent) -> Res<()> {
        match evt {
            DocBlobsIndexEvent::Updated { doc_id } => {
                let hashes = self
                    .doc_blobs_index_repo
                    .list_hashes_for_doc(doc_id)
                    .await?;
                let next_hashes: HashSet<String> = hashes.into_iter().collect();
                let prev_hashes = self
                    .doc_to_known_hashes
                    .insert(doc_id.clone(), next_hashes.clone())
                    .unwrap_or_default();
                for stale in prev_hashes.difference(&next_hashes) {
                    self.remove_doc_blobs_partition_if_hash_unreferenced(stale)
                        .await?;
                }
                for hash in next_hashes {
                    self.known_blob_set
                        .entry(hash.clone())
                        .or_default()
                        .insert(PartitionKey::DocBlobsFullSync);
                    if !self.synced_blobs.contains_key(&hash) {
                        self.blobs_to_boot.insert(hash);
                    }
                }
            }
            DocBlobsIndexEvent::Deleted { doc_id } => {
                if let Some(hashes) = self.doc_to_known_hashes.remove(doc_id) {
                    for hash in hashes {
                        self.remove_doc_blobs_partition_if_hash_unreferenced(&hash)
                            .await?;
                    }
                } else {
                    let known_hashes: Vec<String> = self.known_blob_set.keys().cloned().collect();
                    for hash in known_hashes {
                        self.remove_doc_blobs_partition_if_hash_unreferenced(&hash)
                            .await?;
                    }
                }
            }
        }
        Ok(())
    }
    async fn refresh_doc_blobs_workers(&mut self) -> Res<()> {
        let has_active_part = self
            .partitions
            .get(&PartitionKey::DocBlobsFullSync)
            .is_some_and(|part| part.is_active);
        if !has_active_part {
            for hash in self.known_blob_set.keys() {
                if self.blob_required_by_any_active_partition(hash) {
                    continue;
                }
                if let Some(active) = self.active_blobs.remove(hash) {
                    active.stop_token.stop().await?;
                }
                self.pending_blobs.remove(hash);
                self.synced_blobs.remove(hash);
            }
            return Ok(());
        }
        for (hash, parts) in &self.known_blob_set {
            if parts.contains(&PartitionKey::DocBlobsFullSync)
                && !self.active_blobs.contains_key(hash)
                && !self.synced_blobs.contains_key(hash)
            {
                self.blobs_to_boot.insert(hash.clone());
            }
        }
        Ok(())
    }

    async fn batch_boot_blobs(&mut self) -> Res<()> {
        let blobs_part_active = self
            .partitions
            .get(&PartitionKey::DocBlobsFullSync)
            .is_some_and(|part| part.is_active);
        if !blobs_part_active {
            return Ok(());
        }

        let mut double = std::mem::replace(&mut self.blobs_to_boot, default());
        let mut budget = self.available_blob_boot_budget();
        if budget == 0 {
            self.blobs_to_boot = double;
            return Ok(());
        }
        for hash in double.drain() {
            if budget == 0 {
                self.blobs_to_boot.insert(hash);
                continue;
            }
            if self.active_blobs.contains_key(&hash) || self.synced_blobs.contains_key(&hash) {
                continue;
            }
            let prior_pending = self.pending_blobs.get(&hash).cloned();
            if self.blobs_repo.has_hash(&hash).await? {
                self.pending_blobs.remove(&hash);
                self.synced_blobs.insert(
                    hash,
                    SyncedBlobSyncState {
                        _synced_at: std::time::Instant::now(),
                        _last_peer: None,
                    },
                );
                continue;
            }
            let peers = self
                .partitions
                .get(&PartitionKey::DocBlobsFullSync)
                .and_then(|part| match &part.deets {
                    ParitionDeets::DocBlobsFullSync { peers } => {
                        Some(peers.iter().cloned().collect::<Vec<_>>())
                    }
                    _ => None,
                })
                .unwrap_or_default();
            if peers.is_empty() {
                self.blobs_to_boot.insert(hash);
            } else {
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
                        hash.clone(),
                        peers,
                        self.cancel_token.child_token(),
                        self.msg_tx.clone(),
                        self.sync_progress_tx.clone(),
                        Arc::clone(&self.blobs_repo),
                        self.iroh_endpoint.clone(),
                        retry,
                    )?,
                };
                self.pending_blobs.remove(&hash);
                self.active_blobs.insert(hash, active);
                budget = budget.saturating_sub(1);
            }
        }
        double.extend(self.blobs_to_boot.drain());
        self.blobs_to_boot = double;
        Ok(())
    }

    async fn handle_blob_marked_synced(
        &mut self,
        hash: String,
        endpoint_id: Option<EndpointId>,
    ) -> Res<()> {
        let sync_hash = hash.clone();
        if let Some(active) = self.active_blobs.remove(&hash) {
            active.stop_token.stop().await?;
        }
        self.pending_blobs.remove(&hash);
        self.synced_blobs.insert(
            hash,
            SyncedBlobSyncState {
                _synced_at: std::time::Instant::now(),
                _last_peer: endpoint_id,
            },
        );
        self.emit_full_sync_event(FullSyncEvent::BlobSynced {
            hash: sync_hash,
            endpoint_id,
        })
        .await?;
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
        let Some(active) = self.active_blobs.remove(&hash) else {
            return Ok(());
        };
        active.stop_token.stop().await?;
        let now = std::time::Instant::now();
        let delay = delay.min(Duration::from_secs(600));
        let backoff = next_backoff_delay(previous_backoff, delay);
        let pending = PendingBlobSyncState {
            attempt_no: previous_attempt_no + 1,
            last_backoff: backoff,
            last_attempt_at: now,
            due_at: now + backoff,
        };
        let attempt_no = pending.attempt_no;
        self.synced_blobs.remove(&hash);
        self.pending_blobs.insert(hash.clone(), pending);
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
    fn has_progress_repo(&self) -> bool {
        self.progress_repo.is_some()
    }

    async fn emit_blob_worker_status(
        &self,
        partition: PartitionKey,
        hash: &str,
        message: String,
        final_state: Option<ProgressFinalState>,
    ) -> Res<()> {
        if !self.has_progress_repo() {
            return Ok(());
        }
        let task_id = format!(
            "sync/full/{}/blob/{}/worker",
            partition.as_tag_value(),
            hash
        );
        self.emit_progress_task(
            task_id.clone(),
            vec![
                "/type/sync".to_string(),
                "/sync/full".to_string(),
                format!("/partition/{}", partition.as_tag_value()),
                "/kind/blob_worker".to_string(),
                format!("/blob/{hash}"),
            ],
        )
        .await?;
        match final_state {
            Some(state) => {
                self.emit_progress_update(
                    &task_id,
                    ProgressUpdateDeets::Completed {
                        state,
                        message: Some(message),
                    },
                    None,
                )
                .await?;
            }
            None => {
                self.emit_progress_update(
                    &task_id,
                    ProgressUpdateDeets::Status {
                        severity: ProgressSeverity::Info,
                        message,
                    },
                    None,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn emit_stale_peer(&mut self, endpoint_id: EndpointId) -> Res<()> {
        let mut stale = None;
        if let Some(conn_id) = self.conn_by_peer.get(&endpoint_id).copied() {
            if let Some(peer_state) = self.known_peer_set.get_mut(&conn_id) {
                if peer_state.emitted_full_synced {
                    peer_state.emitted_full_synced = false;
                    stale = Some(peer_state.endpoint_id);
                }
            }
        }
        if let Some(endpoint_id) = stale {
            self.emit_full_sync_event(FullSyncEvent::StalePeer { endpoint_id })
                .await?;
        }
        Ok(())
    }

    async fn remove_peer_from_samod_doc_set(&mut self, endpoint_id: EndpointId) -> Res<()> {
        let mut to_remove = Vec::new();
        for (doc_id, sync_state) in &mut self.samod_doc_set {
            sync_state.requested_peers.remove(&endpoint_id);
            if sync_state.requested_peers.is_empty() {
                to_remove.push(doc_id.clone());
            }
        }
        for doc_id in to_remove {
            self.pending_docs.remove(&doc_id);
            self.docs_to_boot.remove(&doc_id);
            self.docs_to_stop.remove(&doc_id);
            if let Some(active) = self.active_docs.remove(&doc_id) {
                active.stop_token.stop().await?;
            }
            self.samod_doc_set.remove(&doc_id);
        }
        Ok(())
    }

    async fn refresh_peer_fully_synced_state(&mut self, endpoint_id: EndpointId) -> Res<()> {
        let Some(conn_id) = self.conn_by_peer.get(&endpoint_id).copied() else {
            return Ok(());
        };
        let mut emit_full = None;
        let mut emit_stale = false;
        if let Some(peer_state) = self.known_peer_set.get_mut(&conn_id) {
            // "Fully synced" is a bootstrap/data convergence signal.
            // Live subscription readiness is orthogonal and can flap during reconnects.
            let is_fully_synced = peer_state.bootstrap_ready
                && peer_state.bootstrap_remaining_docs == 0
                && peer_state.samod_pending_docs == 0;
            debug!(
                endpoint_id = ?endpoint_id,
                bootstrap_ready = peer_state.bootstrap_ready,
                bootstrap_synced_docs = peer_state.bootstrap_synced_docs,
                bootstrap_remaining_docs = peer_state.bootstrap_remaining_docs,
                samod_pending_docs = peer_state.samod_pending_docs,
                emitted_full_synced = peer_state.emitted_full_synced,
                live_ready = peer_state.live_ready,
                is_fully_synced,
                "refresh_peer_fully_synced_state evaluated"
            );
            if is_fully_synced {
                if !peer_state.emitted_full_synced {
                    peer_state.emitted_full_synced = true;
                    emit_full = Some(peer_state.bootstrap_synced_docs as usize);
                }
            } else if peer_state.emitted_full_synced {
                peer_state.emitted_full_synced = false;
                emit_stale = true;
            }
        }
        if let Some(doc_count) = emit_full {
            self.emit_full_sync_event(FullSyncEvent::PeerFullSynced {
                endpoint_id,
                doc_count,
            })
            .await?;
        } else if emit_stale {
            self.emit_full_sync_event(FullSyncEvent::StalePeer { endpoint_id })
                .await?;
        }
        Ok(())
    }

    async fn emit_full_sync_event(&self, event: FullSyncEvent) -> Res<()> {
        debug!(event = ?event, "emitting full sync event");
        self.events_tx
            .send(event)
            .inspect_err(|_| warn!("full sync event receiver dropped"))
            .ok();
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
        endpoint_id: EndpointId,
        partition: PartitionKey,
        hash: &str,
    ) -> Res<()> {
        if self.has_progress_repo() {
            let task_id = format!(
                "sync/full/{}/{}/blob/{}",
                partition.as_tag_value(),
                endpoint_id,
                hash
            );
            self.emit_progress_task(
                task_id.clone(),
                vec![
                    "/type/sync".to_string(),
                    "/sync/full".to_string(),
                    format!("/partition/{}", partition.as_tag_value()),
                    format!("/peer/{endpoint_id}"),
                    "/kind/blob".to_string(),
                    format!("/blob/{hash}"),
                ],
            )
            .await?;
            self.emit_progress_update(
                &task_id,
                ProgressUpdateDeets::Status {
                    severity: ProgressSeverity::Info,
                    message: "download started".to_string(),
                },
                Some(format!("sync blob {hash}")),
            )
            .await?;
        }
        self.emit_full_sync_event(FullSyncEvent::BlobDownloadStarted {
            endpoint_id,
            partition,
            hash: hash.to_string(),
        })
        .await?;
        Ok(())
    }

    async fn emit_blob_progress_amount(
        &self,
        endpoint_id: EndpointId,
        partition: PartitionKey,
        hash: &str,
        done: u64,
    ) -> Res<()> {
        if !self.has_progress_repo() {
            return Ok(());
        }
        let task_id = format!(
            "sync/full/{}/{}/blob/{}",
            partition.as_tag_value(),
            endpoint_id,
            hash
        );
        self.emit_progress_update(
            &task_id,
            ProgressUpdateDeets::Amount {
                severity: ProgressSeverity::Info,
                done,
                total: None,
                unit: ProgressUnit::Bytes,
                message: None,
            },
            None,
        )
        .await?;
        Ok(())
    }

    async fn emit_blob_progress_finished(
        &self,
        endpoint_id: EndpointId,
        partition: PartitionKey,
        hash: &str,
        success: bool,
    ) -> Res<()> {
        if self.has_progress_repo() {
            let task_id = format!(
                "sync/full/{}/{}/blob/{}",
                partition.as_tag_value(),
                endpoint_id,
                hash
            );
            self.emit_progress_update(
                &task_id,
                ProgressUpdateDeets::Completed {
                    state: if success {
                        ProgressFinalState::Succeeded
                    } else {
                        ProgressFinalState::Failed
                    },
                    message: None,
                },
                None,
            )
            .await?;
        }
        self.emit_full_sync_event(FullSyncEvent::BlobDownloadFinished {
            endpoint_id,
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
        let mut worker_finished: HashMap<(PartitionKey, String), (bool, String)> = HashMap::new();

        for msg in buffer.drain(..) {
            match msg {
                SyncProgressMsg::BlobWorkerStarted { partition, hash } => {
                    worker_started.insert((partition, hash));
                }
                SyncProgressMsg::BlobDownloadStarted {
                    endpoint_id,
                    partition,
                    hash,
                } => {
                    started.insert(BlobProgressKey {
                        endpoint_id,
                        partition,
                        hash,
                    });
                }
                SyncProgressMsg::BlobDownloadProgress {
                    endpoint_id,
                    partition,
                    hash,
                    done,
                } => {
                    progress_latest.insert(
                        BlobProgressKey {
                            endpoint_id,
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
                    endpoint_id,
                    partition,
                    hash,
                    success,
                } => {
                    finished.insert(
                        BlobProgressKey {
                            endpoint_id,
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
                } => {
                    worker_finished.insert((partition, hash), (success, reason));
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
            self.emit_blob_progress_start(key.endpoint_id, key.partition, &key.hash)
                .await?;
        }

        for (key, done) in progress_latest {
            if finished.contains_key(&key) {
                continue;
            }
            self.emit_blob_progress_amount(key.endpoint_id, key.partition, &key.hash, done)
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
            self.emit_blob_progress_finished(key.endpoint_id, key.partition, &key.hash, success)
                .await?;
        }

        for ((partition, hash), (success, reason)) in worker_finished {
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
        }

        Ok(())
    }
}

fn heads_equal_as_set(left: &[automerge::ChangeHash], right: &[automerge::ChangeHash]) -> bool {
    left.len() == right.len() && left.iter().all(|head| right.contains(head))
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
