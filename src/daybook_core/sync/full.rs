// FIXME; split the Worker into multiple states

use crate::interlude::*;

use am_utils_rs::sync::{
    peer::{
        DocSyncAck, DocSyncRequest, PeerSyncProgressEvent, PeerSyncWorkerEvent,
        PeerSyncWorkerStopToken, SpawnPeerSyncWorkerArgs,
    },
    protocol::{PartitionId, PartitionSyncRpc, PeerKey},
    store::SyncStoreHandle,
};
use iroh::EndpointId;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::blobs::{BlobScope, BlobsRepo};
use crate::progress::{
    CreateProgressTaskArgs, ProgressFinalState, ProgressRepo, ProgressRetentionPolicy,
    ProgressSeverity, ProgressUnit, ProgressUpdate, ProgressUpdateDeets,
};
use crate::repo::RepoCtx;
use crate::sync::{PARTITION_SYNC_ALPN, REPO_SYNC_ALPN};

type BigRepoConnectionId = EndpointId;

mod blob_worker;
mod doc_worker;
mod import_worker;
mod scheduler;

const MIN_DOC_WORKER_FLOOR: usize = 8;
const MIN_IMPORT_WORKER_FLOOR: usize = 4;
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
    PeerConnectionLost {
        endpoint_id: EndpointId,
        reason: String,
    },
}

enum Msg {
    SetPeer {
        endpoint_id: EndpointId,
        endpoint_addr: iroh::EndpointAddr,
        conn_id: BigRepoConnectionId,
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
        endpoint_id: EndpointId,
        resp: tokio::sync::oneshot::Sender<()>,
    },
    DocSyncCompleted {
        doc_id: DocumentId,
        endpoint_id: EndpointId,
        outcome: am_utils_rs::repo::SyncDocOutcome,
    },
    DocSyncRequestBackoff {
        doc_id: DocumentId,
        endpoint_id: EndpointId,
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
        endpoint_id: EndpointId,
        outcome: import_worker::ImportDocOutcome,
    },
    ImportDocBackoff {
        doc_id: DocumentId,
        endpoint_id: EndpointId,
        delay: Duration,
        previous_attempt_no: usize,
        previous_backoff: Duration,
        previous_attempt_at: std::time::Instant,
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
    WaitForPeersFullySynced {
        endpoint_ids: Vec<EndpointId>,
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
}

struct FullSyncWaiter {
    remaining: HashSet<EndpointId>,
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
        endpoint_addr: iroh::EndpointAddr,
        conn_id: BigRepoConnectionId,
        peer_key: PeerKey,
        connection: am_utils_rs::repo::BigRepoConnection,
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
            conn_id,
            endpoint_id,
            endpoint_addr,
            partitions,
            peer_key,
            connection,
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

    pub async fn wait_for_peers_fully_synced(&self, endpoint_ids: &[EndpointId]) -> Res<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let msg = Msg::WaitForPeersFullySynced {
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
        utils_rs::wait_on_handle_with_timeout(
            self.join_handle,
            utils_rs::scale_timeout(Duration::from_secs(5)),
        )
        .await
    }
}

pub async fn start_full_sync_worker(
    rcx: Arc<RepoCtx>,
    blobs_repo: Arc<BlobsRepo>,
    progress_repo: Option<Arc<ProgressRepo>>,
    sync_store: SyncStoreHandle,
    iroh_endpoint: iroh::Endpoint,
) -> Res<(WorkerHandle, StopToken)> {
    let (msg_tx, mut msg_rx) = mpsc::unbounded_channel();
    let (sync_progress_tx, mut sync_progress_rx) = mpsc::channel::<SyncProgressMsg>(8192);
    let (doc_sync_tx, mut doc_sync_rx) = mpsc::channel::<DocSyncRequest>(8192);
    let (events_tx, events_rx) = tokio::sync::broadcast::channel(1024);

    let cancel_token = CancellationToken::new();

    let mut worker = Worker {
        big_repo: Arc::clone(&rcx.big_repo),
        local_peer_key: format!("/{}/{}", rcx.repo_id, iroh_endpoint.id()),
        cancel_token: cancel_token.clone(),
        msg_tx: msg_tx.clone(),
        sync_progress_tx: sync_progress_tx.clone(),
        doc_sync_tx: doc_sync_tx.clone(),
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
        sync_store,
        iroh_endpoint,

        doc_sync_set: default(),
        import_doc_set: default(),
        scheduler: default(),

        known_peer_set: default(),
        conn_by_peer: default(),
        endpoint_by_peer_key: default(),
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
                if worker.scheduler.has_queued_imports() {
                    worker.batch_boot_imports().await?;
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
                    val = doc_sync_rx.recv() => {
                        let Some(req) = val else {
                            continue;
                        };
                        worker.handle_doc_sync_request(req).await?;
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
            worker.batch_stop_docs().await?;
            worker.batch_stop_imports().await?;
            worker.batch_stop_blobs().await?;
            for (_endpoint_id, mut session) in worker.peer_partition_sessions.drain() {
                session.forward_cancel_token.cancel();
                session.stop.stop().await?;
                for join_handle in session.forward_handles.drain(..) {
                    utils_rs::wait_on_handle_with_timeout(
                        join_handle,
                        utils_rs::scale_timeout(Duration::from_secs(1)),
                    )
                    .await?;
                }
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
    sync_store: SyncStoreHandle,
    iroh_endpoint: iroh::Endpoint,

    partitions: HashMap<PartitionKey, Partition>,

    scheduler: scheduler::Scheduler,

    // doc_ref_counts: HashMap<DocumentId, usize>,
    // peer_docs: HashMap<EndpointId, HashSet<DocumentId>>,
    doc_sync_set: HashMap<DocumentId, DocSyncState>,
    import_doc_set: HashMap<DocumentId, ImportRequestedDoc>,

    msg_tx: mpsc::UnboundedSender<Msg>,
    sync_progress_tx: mpsc::Sender<SyncProgressMsg>,
    doc_sync_tx: mpsc::Sender<DocSyncRequest>,
    events_tx: tokio::sync::broadcast::Sender<FullSyncEvent>,
    max_active_sync_workers: usize,

    known_peer_set: HashMap<BigRepoConnectionId, PeerSyncState>,
    conn_by_peer: HashMap<EndpointId, BigRepoConnectionId>,
    endpoint_by_peer_key: HashMap<PeerKey, EndpointId>,
    seen_peer_keys: HashSet<PeerKey>,
    peer_partition_sessions: HashMap<EndpointId, PeerPartitionSession>,
    full_sync_waiters: Vec<FullSyncWaiter>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum PartitionKey {
    BigRepoPartition(PartitionId),
    BlobScope(BlobScope),
    // Docs(Uuid),
    // Blobs(Uuid),
}

impl PartitionKey {
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
    peers: HashSet<EndpointId>,
}

struct PeerSyncState {
    endpoint_id: EndpointId,
    endpoint_addr: iroh::EndpointAddr,
    partitions: HashSet<PartitionKey>,
    peer_key: PeerKey,
    connection: am_utils_rs::repo::BigRepoConnection,
    bootstrap_ready: bool,
    live_ready: bool,
    bootstrap_synced_docs: u64,
    bootstrap_remaining_docs: u64,
    doc_pending_docs: u64,
    emitted_full_synced: bool,
}

struct PeerPartitionSession {
    stop: PeerSyncWorkerStopToken,
    doc_ack_tx: mpsc::Sender<DocSyncAck>,
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
struct DocSyncState {
    requested_peers: HashMap<EndpointId, HashMap<PartitionKey, HashSet<u64>>>,
}

#[derive(Default)]
struct ImportRequestedDoc {
    requested_peers: HashMap<EndpointId, HashMap<PartitionKey, HashSet<u64>>>,
}

struct ActiveDocSyncState {
    stop_token: doc_worker::DocSyncWorkerStopToken,
    retry: RetryState,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct DocSyncTaskKey {
    doc_id: DocumentId,
    endpoint_id: EndpointId,
}

enum BootDocSyncWorkerResult {
    Spawned(ActiveDocSyncState),
    MissingLocal,
    Deferred,
}

struct ActiveImportSyncState {
    stop_token: import_worker::ImportSyncWorkerStopToken,
    retry: RetryState,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct ImportSyncTaskKey {
    doc_id: DocumentId,
    endpoint_id: EndpointId,
}

struct ActiveBlobSyncState {
    stop_token: blob_worker::BlobSyncWorkerStopToken,
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
                self.emit_peer_progress_status(
                    endpoint_id,
                    ProgressUpdateDeets::Status {
                        severity: ProgressSeverity::Info,
                        message: format!("phase started: {phase}"),
                    },
                )
                .await?;
            }
            PeerSyncProgressEvent::PhaseFinished { phase, elapsed } => {
                debug!(endpoint_id = ?endpoint_id, phase, elapsed_ms = elapsed.as_millis(), "peer sync phase finished");
                self.emit_peer_progress_status(
                    endpoint_id,
                    ProgressUpdateDeets::Status {
                        severity: ProgressSeverity::Info,
                        message: format!("phase finished: {phase} ({:?})", elapsed),
                    },
                )
                .await?;
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
                self.emit_peer_progress_status(
                    endpoint_id,
                    ProgressUpdateDeets::Amount {
                        severity: ProgressSeverity::Info,
                        done: synced_docs,
                        total: Some(synced_docs.saturating_add(remaining_docs)),
                        unit: ProgressUnit::Generic {
                            label: "docs".to_string(),
                        },
                        message: Some("doc sync status".to_string()),
                    },
                )
                .await?;
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
                self.emit_peer_progress_status(
                    endpoint_id,
                    ProgressUpdateDeets::Status {
                        severity: ProgressSeverity::Info,
                        message: format!("bootstrapped with {partition_count} partitions"),
                    },
                )
                .await?;
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
                self.emit_peer_progress_status(
                    endpoint_id,
                    ProgressUpdateDeets::Completed {
                        state: ProgressFinalState::Succeeded,
                        message: Some("peer entered live mode".to_string()),
                    },
                )
                .await?;
                if let Some(conn_id) = self.conn_by_peer.get(&endpoint_id).copied() {
                    if let Some(peer_state) = self.known_peer_set.get_mut(&conn_id) {
                        peer_state.live_ready = true;
                    }
                }
                self.refresh_peer_fully_synced_state(endpoint_id).await?;
            }
            PeerSyncWorkerEvent::AbnormalExit { peer, reason } => {
                warn!(endpoint_id = ?endpoint_id, peer, reason, "peer sync worker exited abnormally");
                let lost_reason = reason.clone();
                self.emit_peer_progress_status(
                    endpoint_id,
                    ProgressUpdateDeets::Completed {
                        state: ProgressFinalState::Failed,
                        message: Some(format!("peer worker abnormal exit: {reason}")),
                    },
                )
                .await?;
                self.emit_stale_peer(endpoint_id).await?;
                self.emit_full_sync_event(FullSyncEvent::PeerConnectionLost {
                    endpoint_id,
                    reason: lost_reason,
                })
                .await?;
                self.scheduler.peer_sessions_to_refresh.insert(endpoint_id);
            }
        }
        Ok(())
    }

    fn endpoint_for_peer_key(&self, peer_key: &str) -> Option<EndpointId> {
        self.endpoint_by_peer_key.get(peer_key).copied()
    }

    fn peer_key_for_endpoint(&self, endpoint_id: EndpointId) -> Option<PeerKey> {
        let conn_id = self.conn_by_peer.get(&endpoint_id).copied()?;
        let peer_state = self.known_peer_set.get(&conn_id)?;
        Some(peer_state.peer_key.clone())
    }

    fn peer_is_fully_synced(&self, endpoint_id: EndpointId) -> bool {
        let Some(conn_id) = self.conn_by_peer.get(&endpoint_id).copied() else {
            return false;
        };
        self.known_peer_set
            .get(&conn_id)
            .is_some_and(|peer_state| peer_state.emitted_full_synced)
    }

    fn refresh_full_sync_waiters(&mut self) {
        if self.full_sync_waiters.is_empty() {
            return;
        }
        let mut pending = std::mem::take(&mut self.full_sync_waiters);
        for mut waiter in pending.drain(..) {
            waiter
                .remaining
                .retain(|endpoint_id| !self.peer_is_fully_synced(*endpoint_id));
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

    fn available_doc_boot_budget(&self) -> usize {
        self.scheduler
            .available_doc_boot_budget(self.max_active_sync_workers)
    }

    fn available_blob_boot_budget(&self) -> usize {
        self.scheduler
            .available_blob_boot_budget(self.max_active_sync_workers)
    }

    fn available_import_boot_budget(&self) -> usize {
        self.scheduler
            .available_import_boot_budget(self.max_active_sync_workers)
    }

    async fn handle_msg(&mut self, msg: Msg) -> Res<()> {
        match msg {
            Msg::SetPeer {
                endpoint_id,
                endpoint_addr,
                resp,
                partitions,
                conn_id,
                peer_key,
                connection,
            } => {
                let old_conn_id = self.conn_by_peer.get(&endpoint_id).copied();
                let old_state = self
                    .known_peer_set
                    .remove(&conn_id)
                    .or_else(|| old_conn_id.and_then(|id| self.known_peer_set.remove(&id)));
                if let Some(old_state) = old_state.as_ref() {
                    if old_state.peer_key != peer_key {
                        self.endpoint_by_peer_key.remove(&old_state.peer_key);
                    }
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
                        endpoint_addr,
                        peer_key: peer_key.clone(),
                        connection,
                        bootstrap_ready: false,
                        live_ready: false,
                        bootstrap_synced_docs: 0,
                        bootstrap_remaining_docs: 0,
                        doc_pending_docs: 0,
                        emitted_full_synced: false,
                    },
                );
                self.conn_by_peer.insert(endpoint_id, conn_id);
                self.endpoint_by_peer_key
                    .insert(peer_key.clone(), endpoint_id);
                self.seen_peer_keys.insert(peer_key.clone());
                for part_key in new_parts {
                    self.add_peer_to_part(part_key, endpoint_id).await?;
                }
                self.refresh_full_sync_waiters();
                resp.send(()).inspect_err(|_| warn!(ERROR_CALLER)).ok();
            }
            Msg::DelPeer { endpoint_id, resp } => {
                if let Some(conn_id) = self.conn_by_peer.remove(&endpoint_id) {
                    if let Some(state) = self.known_peer_set.remove(&conn_id) {
                        self.endpoint_by_peer_key.remove(&state.peer_key);
                        state.connection.close().await.ok();
                        self.remove_peer_from_doc_sync_set(endpoint_id).await?;
                        self.remove_peer_from_import_doc_set(endpoint_id).await?;
                        self.remove_peer_partition_session(endpoint_id).await?;
                        for part_key in state.partitions {
                            self.remove_peer_from_part(part_key, endpoint_id).await?;
                        }
                    }
                }
                self.endpoint_by_peer_key
                    .retain(|_peer_key, cached_endpoint| *cached_endpoint != endpoint_id);
                self.refresh_full_sync_waiters();
                resp.send(()).inspect_err(|_| warn!(ERROR_CALLER)).ok();
            }
            Msg::DocSyncCompleted {
                doc_id,
                endpoint_id,
                outcome,
            } => {
                self.handle_doc_sync_completed(doc_id, endpoint_id, outcome)
                    .await?
            }
            Msg::DocSyncRequestBackoff {
                doc_id,
                endpoint_id,
                delay,
                previous_attempt_no,
                previous_backoff,
                previous_attempt_at,
            } => {
                self.handle_doc_request_backoff(
                    doc_id,
                    endpoint_id,
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
                endpoint_id,
                outcome,
            } => {
                self.handle_import_doc_completed(doc_id, endpoint_id, outcome)
                    .await?;
            }
            Msg::ImportDocBackoff {
                doc_id,
                endpoint_id,
                delay,
                previous_attempt_no,
                previous_backoff,
                previous_attempt_at,
            } => {
                self.handle_import_doc_backoff(
                    doc_id,
                    endpoint_id,
                    delay,
                    previous_attempt_no,
                    previous_backoff,
                    previous_attempt_at,
                )
                .await?;
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
                                bootstrap_ready: peer_state.bootstrap_ready,
                                bootstrap_synced_docs: peer_state.bootstrap_synced_docs,
                                bootstrap_remaining_docs: peer_state.bootstrap_remaining_docs,
                                doc_pending_docs: peer_state.doc_pending_docs,
                                live_ready: peer_state.live_ready,
                                has_peer_session: self
                                    .peer_partition_sessions
                                    .contains_key(&endpoint_id),
                            },
                        ))
                    })
                    .collect();
                resp.send(snapshot)
                    .inspect_err(|_| warn!(ERROR_CALLER))
                    .ok();
            }
            Msg::WaitForPeersFullySynced { endpoint_ids, resp } => {
                let remaining = endpoint_ids
                    .into_iter()
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .filter(|endpoint_id| !self.peer_is_fully_synced(*endpoint_id))
                    .collect::<HashSet<_>>();
                if remaining.is_empty() {
                    resp.send(()).inspect_err(|_| warn!(ERROR_CALLER)).ok();
                } else {
                    self.full_sync_waiters
                        .push(FullSyncWaiter { remaining, resp });
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
                    affected_peers.insert(peer_state.endpoint_id);
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
        for endpoint_id in affected_peers {
            let desired_parts = self.desired_partitions_for_peer(endpoint_id);
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
                self.scheduler.peer_sessions_to_refresh.insert(endpoint_id);
                continue;
            };
            if session.partitions != desired_parts {
                self.scheduler.peer_sessions_to_refresh.insert(endpoint_id);
            }
        }
        for endpoint_id in sessions_to_remove {
            self.remove_peer_partition_session(endpoint_id).await?;
        }
        Ok(())
    }

    fn desired_partitions_for_peer(&self, endpoint_id: EndpointId) -> HashSet<PartitionKey> {
        self.partitions
            .iter()
            .filter_map(|(part_key, part)| {
                if part.peers.contains(&endpoint_id) {
                    Some(part_key.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    async fn add_peer_to_part(
        &mut self,
        part_key: PartitionKey,
        endpoint_id: EndpointId,
    ) -> Res<()> {
        let part = self.partitions.entry(part_key.clone()).or_default();
        part.peers.insert(endpoint_id);
        match part_key {
            PartitionKey::BigRepoPartition(_) => {
                let docs: Vec<_> = self
                    .scheduler
                    .pending_tasks
                    .keys()
                    .filter_map(|task| match task {
                        scheduler::SyncTask::Doc(task_key)
                            if task_key.endpoint_id == endpoint_id =>
                        {
                            Some(task_key.clone())
                        }
                        scheduler::SyncTask::Doc(_) => None,
                        scheduler::SyncTask::Import(_) | scheduler::SyncTask::Blob(_) => None,
                    })
                    .collect();
                for task_key in docs {
                    self.scheduler.enqueue_doc(task_key);
                }
                let imports: Vec<_> = self
                    .scheduler
                    .pending_tasks
                    .keys()
                    .filter_map(|task| match task {
                        scheduler::SyncTask::Import(task_key)
                            if task_key.endpoint_id == endpoint_id =>
                        {
                            Some(task_key.clone())
                        }
                        scheduler::SyncTask::Import(_) => None,
                        scheduler::SyncTask::Doc(_) | scheduler::SyncTask::Blob(_) => None,
                    })
                    .collect();
                for task_key in imports {
                    self.scheduler.enqueue_import(task_key);
                }
            }
            PartitionKey::BlobScope(_) => {
                let hashes: Vec<_> = self
                    .scheduler
                    .pending_tasks
                    .keys()
                    .filter_map(|task| match task {
                        scheduler::SyncTask::Blob(hash) => Some(hash.clone()),
                        scheduler::SyncTask::Doc(_) | scheduler::SyncTask::Import(_) => None,
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

    async fn remove_peer_from_part(
        &mut self,
        part_key: PartitionKey,
        endpoint_id: EndpointId,
    ) -> Res<()> {
        let part = self.partitions.entry(part_key.clone()).or_default();
        part.peers.remove(&endpoint_id);
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
        for endpoint_id in double.drain() {
            let Some(conn_id) = self.conn_by_peer.get(&endpoint_id).copied() else {
                continue;
            };
            let Some(peer_state) = self.known_peer_set.get(&conn_id) else {
                continue;
            };
            let peer_key = peer_state.peer_key.clone();
            let endpoint_addr = peer_state.endpoint_addr.clone();
            let parts = self.desired_partitions_for_peer(endpoint_id);
            self.refresh_peer_partition_session(endpoint_id, endpoint_addr, peer_key, parts)
                .await?;
        }
        self.scheduler.peer_sessions_to_refresh = double;
        Ok(())
    }

    async fn refresh_peer_partition_session(
        &mut self,
        endpoint_id: EndpointId,
        endpoint_addr: iroh::EndpointAddr,
        peer_key: PeerKey,
        partitions: HashSet<PartitionKey>,
    ) -> Res<()> {
        let session = self.peer_partition_sessions.remove(&endpoint_id);
        if let Some(mut session) = session {
            session.forward_cancel_token.cancel();
            for join_handle in session.forward_handles.drain(..) {
                if let Err(err) = utils_rs::wait_on_handle_with_timeout(
                    join_handle,
                    utils_rs::scale_timeout(Duration::from_secs(1)),
                )
                .await
                {
                    warn!(
                        endpoint_id = ?endpoint_id,
                        ?err,
                        "error waiting for peer session forwarder during refresh"
                    );
                }
            }
            session.stop.stop().await?;
        }
        self.scheduler.clear_peer_cursor_acks(endpoint_id);
        if partitions.is_empty() {
            return Ok(());
        }
        let rpc_client = irpc_iroh::client::<PartitionSyncRpc>(
            self.iroh_endpoint.clone(),
            endpoint_addr,
            PARTITION_SYNC_ALPN,
        );
        let (doc_ack_tx, doc_ack_rx) = mpsc::channel(8192);
        let (mut handle, stop) =
            am_utils_rs::sync::peer::spawn_peer_sync_worker(SpawnPeerSyncWorkerArgs {
                local_peer: self.local_peer_key.clone(),
                remote_peer: peer_key,
                rpc_client,
                sync_store: self.sync_store.clone(),
                doc_sync_tx: self.doc_sync_tx.clone(),
                doc_ack_rx,
                target_partitions: partitions.iter().map(PartitionKey::partition_id).collect(),
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
                doc_ack_tx,
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
            for join_handle in session.forward_handles.drain(..) {
                utils_rs::wait_on_handle_with_timeout(
                    join_handle,
                    utils_rs::scale_timeout(Duration::from_secs(1)),
                )
                .await?;
            }
            session.stop.stop().await?;
        }
        self.scheduler.clear_peer_cursor_acks(endpoint_id);
        Ok(())
    }

    async fn handle_doc_sync_request(&mut self, req: DocSyncRequest) -> Res<()> {
        match req {
            DocSyncRequest::PartitionMemberEvent { peer_key, event } => {
                self.handle_partition_member_event(peer_key, event).await?;
            }
            DocSyncRequest::PartitionDocEvent { peer_key, event } => {
                self.handle_partition_doc_event(peer_key, event).await?;
            }
            DocSyncRequest::RequestDocSync {
                peer_key,
                partition_id,
                doc_id,
                cursor,
            } => {
                self.handle_request_doc_sync(peer_key, partition_id, doc_id, cursor)
                    .await?;
            }
            DocSyncRequest::DocDeleted {
                peer_key,
                partition_id,
                doc_id,
                cursor,
            } => {
                self.handle_doc_deleted_request(peer_key, partition_id, doc_id, cursor)
                    .await?;
            }
            DocSyncRequest::ImportDoc { .. } => unreachable!(),
        }
        Ok(())
    }

    async fn handle_partition_member_event(
        &mut self,
        peer_key: PeerKey,
        event: am_utils_rs::sync::protocol::PartitionMemberEvent,
    ) -> Res<()> {
        let Some(endpoint_id) = self.endpoint_for_peer_key(&peer_key) else {
            self.handle_unknown_peer_request(
                &peer_key,
                format!("partition member event for {}", event.partition_id),
            )?;
            return Ok(());
        };
        let existing = self
            .sync_store
            .get_partition_cursor(peer_key.clone(), event.partition_id.clone())
            .await?;
        if existing
            .member_cursor
            .is_some_and(|current| event.cursor <= current)
        {
            return Ok(());
        }
        match &event.deets {
            am_utils_rs::sync::protocol::PartitionMemberEventDeets::MemberUpsert {
                item_id,
                payload,
            } => {
                let payload = serde_json::from_str::<serde_json::Value>(payload)
                    .map_err(|err| ferr!("invalid member upsert payload json: {err}"))?;
                self.big_repo
                    .partition_store()
                    .add_member(&event.partition_id, item_id, &payload)
                    .await?;
                if let Some(scope) = BlobScope::from_partition_id(&event.partition_id) {
                    self.add_hash_to_blob_scope(scope, item_id.clone()).await?;
                }
            }
            am_utils_rs::sync::protocol::PartitionMemberEventDeets::MemberRemoved {
                item_id,
                payload,
            } => {
                let payload = serde_json::from_str::<serde_json::Value>(payload)
                    .map_err(|err| ferr!("invalid member removed payload json: {err}"))?;
                self.big_repo
                    .partition_store()
                    .remove_member(&event.partition_id, item_id, &payload)
                    .await?;
                if let Some(scope) = BlobScope::from_partition_id(&event.partition_id) {
                    self.remove_hash_from_blob_scope(scope, item_id).await?;
                }
            }
        }
        self.maybe_emit_member_cursor_ack(endpoint_id, &event.partition_id, event.cursor)
            .await
    }

    async fn handle_partition_doc_event(
        &mut self,
        peer_key: PeerKey,
        event: am_utils_rs::sync::protocol::PartitionDocEvent,
    ) -> Res<()> {
        if BlobScope::from_partition_id(&event.partition_id).is_some() {
            let Some(endpoint_id) = self.endpoint_for_peer_key(&peer_key) else {
                self.handle_unknown_peer_request(
                    &peer_key,
                    format!("blob-scope doc event for {}", event.partition_id),
                )?;
                return Ok(());
            };
            self.scheduler.note_cursor_ready_immediate(
                endpoint_id,
                &event.partition_id,
                event.cursor,
            );
            self.maybe_emit_cursor_advance_acks(endpoint_id, &peer_key, &event.partition_id)
                .await?;
            return Ok(());
        }
        let existing = self
            .sync_store
            .get_partition_cursor(peer_key.clone(), event.partition_id.clone())
            .await?;
        if existing
            .doc_cursor
            .is_some_and(|current| event.cursor <= current)
        {
            return Ok(());
        }
        match &event.deets {
            am_utils_rs::sync::protocol::PartitionDocEventDeets::ItemChanged {
                item_id, ..
            } => {
                let parsed = item_id
                    .parse::<DocumentId>()
                    .map_err(|err| ferr!("invalid remote doc id '{item_id}': {err}"))?;
                if self.big_repo.get_doc(&parsed).await?.is_some() {
                    self.handle_request_doc_sync(
                        peer_key,
                        event.partition_id,
                        parsed,
                        event.cursor,
                    )
                    .await?;
                } else {
                    self.enqueue_doc_import_request(
                        peer_key,
                        event.partition_id,
                        parsed,
                        event.cursor,
                    )
                    .await?;
                }
            }
            am_utils_rs::sync::protocol::PartitionDocEventDeets::ItemDeleted {
                item_id, ..
            } => {
                let parsed = item_id
                    .parse::<DocumentId>()
                    .map_err(|err| ferr!("invalid remote doc id '{item_id}': {err}"))?;
                self.handle_doc_deleted_request(peer_key, event.partition_id, parsed, event.cursor)
                    .await?;
            }
        }
        Ok(())
    }

    async fn handle_request_doc_sync(
        &mut self,
        peer_key: PeerKey,
        partition_id: PartitionId,
        doc_id: DocumentId,
        cursor: u64,
    ) -> Res<()> {
        let Some(endpoint_id) = self.endpoint_for_peer_key(&peer_key) else {
            self.handle_unknown_peer_request(
                &peer_key,
                format!("request doc sync for {}", doc_id),
            )?;
            return Ok(());
        };
        self.emit_stale_peer(endpoint_id).await?;
        let doc_sync_state = self.doc_sync_set.entry(doc_id.clone()).or_default();
        let requested_parts = doc_sync_state
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
                    peer_state.doc_pending_docs = peer_state.doc_pending_docs.saturating_add(1);
                }
            }
            self.refresh_peer_fully_synced_state(endpoint_id).await?;
        }
        self.scheduler.note_doc_sync_requested(
            endpoint_id,
            &partition_id,
            cursor,
            &doc_id.to_string(),
        );
        let task_key = DocSyncTaskKey {
            doc_id: doc_id.clone(),
            endpoint_id,
        };
        if !self.scheduler.active_docs.contains_key(&task_key)
            && !self.scheduler.is_doc_pending(&task_key)
        {
            self.scheduler.set_doc_pending_now(&task_key);
        }
        Ok(())
    }

    async fn handle_doc_deleted_request(
        &mut self,
        peer_key: PeerKey,
        partition_id: PartitionId,
        doc_id: DocumentId,
        cursor: u64,
    ) -> Res<()> {
        let Some(endpoint_id) = self.endpoint_for_peer_key(&peer_key) else {
            self.handle_unknown_peer_request(
                &peer_key,
                format!("doc deleted request for {}", doc_id),
            )?;
            return Ok(());
        };
        self.emit_stale_peer(endpoint_id).await?;
        self.big_repo
            .partition_store()
            .remove_member(&partition_id, &doc_id.to_string(), &serde_json::json!({}))
            .await?;
        let mut clear_import_request = false;
        if let Some(import_state) = self.import_doc_set.get_mut(&doc_id) {
            if let Some(requested_parts) = import_state.requested_peers.get_mut(&endpoint_id) {
                requested_parts.remove(&PartitionKey::BigRepoPartition(partition_id.clone()));
                if requested_parts.is_empty() {
                    import_state.requested_peers.remove(&endpoint_id);
                }
            }
            clear_import_request = import_state.requested_peers.is_empty();
        }
        let import_task_key = ImportSyncTaskKey {
            doc_id: doc_id.clone(),
            endpoint_id,
        };
        self.scheduler.clear_import_task(&import_task_key);
        if let Some(active) = self.scheduler.active_imports.remove(&import_task_key) {
            active.stop_token.stop().await?;
        }
        if clear_import_request {
            self.import_doc_set.remove(&doc_id);
            for task_key in self.scheduler.import_task_keys_for_doc(&doc_id) {
                self.scheduler.clear_import_task(&task_key);
                if let Some(active) = self.scheduler.active_imports.remove(&task_key) {
                    active.stop_token.stop().await?;
                }
            }
        }

        let mut refresh_peer = false;
        let mut clear_doc_request = false;
        if let Some(sync_state) = self.doc_sync_set.get_mut(&doc_id) {
            let had_peer_entry = sync_state.requested_peers.remove(&endpoint_id).is_some();
            clear_doc_request = sync_state.requested_peers.is_empty();
            if had_peer_entry {
                if let Some(conn_id) = self.conn_by_peer.get(&endpoint_id).copied() {
                    if let Some(peer_state) = self.known_peer_set.get_mut(&conn_id) {
                        peer_state.doc_pending_docs = peer_state.doc_pending_docs.saturating_sub(1);
                        refresh_peer = true;
                    }
                }
            }
        }
        if clear_doc_request {
            self.doc_sync_set.remove(&doc_id);
            for task_key in self.scheduler.doc_task_keys_for_doc(&doc_id) {
                self.scheduler.clear_doc_task(&task_key);
                if let Some(active) = self.scheduler.active_docs.remove(&task_key) {
                    active.stop_token.stop().await?;
                }
            }
        } else {
            let task_key = DocSyncTaskKey {
                doc_id: doc_id.clone(),
                endpoint_id,
            };
            self.scheduler.clear_doc_task(&task_key);
            if let Some(active) = self.scheduler.active_docs.remove(&task_key) {
                active.stop_token.stop().await?;
            }
        }
        if refresh_peer {
            self.refresh_peer_fully_synced_state(endpoint_id).await?;
        }
        self.scheduler
            .note_cursor_ready_immediate(endpoint_id, &partition_id, cursor);
        self.maybe_emit_cursor_advance_acks(endpoint_id, &peer_key, &partition_id)
            .await
    }

    async fn enqueue_doc_import_request(
        &mut self,
        peer_key: PeerKey,
        partition_id: PartitionId,
        doc_id: DocumentId,
        cursor: u64,
    ) -> Res<()> {
        let Some(endpoint_id) = self.endpoint_for_peer_key(&peer_key) else {
            self.handle_unknown_peer_request(
                &peer_key,
                format!("import doc request for {}", doc_id),
            )?;
            return Ok(());
        };
        self.emit_stale_peer(endpoint_id).await?;

        let import_state = self.import_doc_set.entry(doc_id.clone()).or_default();
        import_state
            .requested_peers
            .entry(endpoint_id)
            .or_default()
            .entry(PartitionKey::BigRepoPartition(partition_id))
            .or_default()
            .insert(cursor);

        let task_key = ImportSyncTaskKey {
            doc_id: doc_id.clone(),
            endpoint_id,
        };
        if !self.scheduler.active_imports.contains_key(&task_key)
            && self.scheduler.pending_import_state(&task_key).is_none()
        {
            self.scheduler.set_import_pending_now(&task_key);
        }
        Ok(())
    }

    async fn maybe_emit_cursor_advance_acks(
        &mut self,
        endpoint_id: EndpointId,
        peer_key: &PeerKey,
        partition_id: &PartitionId,
    ) -> Res<()> {
        loop {
            let persisted = self
                .sync_store
                .get_partition_cursor(peer_key.clone(), partition_id.clone())
                .await?
                .doc_cursor;
            let Some(next_cursor) =
                self.scheduler
                    .next_ready_cursor_to_ack(endpoint_id, partition_id, persisted)
            else {
                break;
            };
            let Some(session) = self.peer_partition_sessions.get(&endpoint_id) else {
                warn!(
                    endpoint_id = ?endpoint_id,
                    partition_id = %partition_id,
                    cursor = next_cursor,
                    "missing peer session while emitting cursor advance ack"
                );
                self.scheduler.peer_sessions_to_refresh.insert(endpoint_id);
                break;
            };
            if let Err(err) = session.doc_ack_tx.try_send(DocSyncAck::CursorAdvanced {
                partition_id: partition_id.clone(),
                cursor: next_cursor,
            }) {
                match err {
                    tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                        warn!(
                            endpoint_id = ?endpoint_id,
                            partition_id = %partition_id,
                            cursor = next_cursor,
                            "failed sending cursor advance ack to peer worker: channel closed"
                        );
                    }
                    tokio::sync::mpsc::error::TrySendError::Full(_) => {
                        warn!(
                            endpoint_id = ?endpoint_id,
                            partition_id = %partition_id,
                            cursor = next_cursor,
                            "failed sending cursor advance ack to peer worker: channel full"
                        );
                    }
                }
                self.scheduler.peer_sessions_to_refresh.insert(endpoint_id);
                break;
            }
            self.scheduler
                .commit_ack_cursor(endpoint_id, partition_id, persisted, next_cursor)?;
        }
        Ok(())
    }

    async fn maybe_emit_member_cursor_ack(
        &mut self,
        endpoint_id: EndpointId,
        partition_id: &PartitionId,
        cursor: u64,
    ) -> Res<()> {
        let Some(session) = self.peer_partition_sessions.get(&endpoint_id) else {
            self.scheduler.peer_sessions_to_refresh.insert(endpoint_id);
            return Ok(());
        };
        if let Err(err) = session
            .doc_ack_tx
            .try_send(DocSyncAck::MemberCursorAdvanced {
                partition_id: partition_id.clone(),
                cursor,
            })
        {
            match err {
                tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                    warn!(
                        endpoint_id = ?endpoint_id,
                        partition_id = %partition_id,
                        cursor,
                        "failed sending member cursor advance ack to peer worker: channel closed"
                    );
                }
                tokio::sync::mpsc::error::TrySendError::Full(_) => {
                    warn!(
                        endpoint_id = ?endpoint_id,
                        partition_id = %partition_id,
                        cursor,
                        "failed sending member cursor advance ack to peer worker: channel full"
                    );
                }
            }
            self.scheduler.peer_sessions_to_refresh.insert(endpoint_id);
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
            match self.boot_doc_sync_worker(task_key.clone(), retry).await? {
                BootDocSyncWorkerResult::Spawned(active) => {
                    self.scheduler.clear_doc_pending(&task_key);
                    self.scheduler.active_docs.insert(task_key, active);
                    budget = budget.saturating_sub(1);
                }
                BootDocSyncWorkerResult::MissingLocal => {
                    self.handle_doc_missing_local(task_key.doc_id).await?;
                }
                BootDocSyncWorkerResult::Deferred => {
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

    async fn batch_boot_imports(&mut self) -> Res<()> {
        let docs_part_active = self
            .partitions
            .iter()
            .any(|(key, part)| matches!(key, PartitionKey::BigRepoPartition(_)) && part.is_active);
        if !docs_part_active {
            return Ok(());
        }

        let mut budget = self.available_import_boot_budget();
        if budget == 0 {
            return Ok(());
        }
        let task_keys = self.scheduler.drain_queued_imports(budget);
        for task_key in task_keys {
            if self.scheduler.active_imports.contains_key(&task_key) {
                continue;
            }
            let prior_pending = self.scheduler.pending_import_state(&task_key);
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
            let Some(import_state) = self.import_doc_set.get(&task_key.doc_id) else {
                self.scheduler.clear_import_task(&task_key);
                continue;
            };
            if !import_state
                .requested_peers
                .contains_key(&task_key.endpoint_id)
            {
                self.scheduler.clear_import_task(&task_key);
                continue;
            }
            let Some(conn_id) = self.conn_by_peer.get(&task_key.endpoint_id).copied() else {
                let now = std::time::Instant::now();
                let pending = scheduler::PendingTaskState {
                    attempt_no: retry.attempt_no,
                    last_backoff: retry.last_backoff,
                    last_attempt_at: now,
                    due_at: now + Duration::from_millis(500),
                };
                self.scheduler.set_import_backoff(&task_key, pending);
                continue;
            };
            if !self.known_peer_set.contains_key(&conn_id) {
                let now = std::time::Instant::now();
                let pending = scheduler::PendingTaskState {
                    attempt_no: retry.attempt_no,
                    last_backoff: retry.last_backoff,
                    last_attempt_at: now,
                    due_at: now + Duration::from_millis(500),
                };
                self.scheduler.set_import_backoff(&task_key, pending);
                continue;
            }
            let active = ActiveImportSyncState {
                stop_token: import_worker::spawn_import_sync_worker(
                    task_key.doc_id.clone(),
                    import_worker::ImportSyncTarget {
                        endpoint_id: task_key.endpoint_id,
                    },
                    self.local_peer_key.clone(),
                    self.cancel_token.child_token(),
                    self.msg_tx.clone(),
                    Arc::clone(&self.big_repo),
                    self.iroh_endpoint.clone(),
                    retry,
                )?,
                retry,
            };
            self.scheduler.clear_import_pending(&task_key);
            self.scheduler.active_imports.insert(task_key, active);
            budget = budget.saturating_sub(1);
            if budget == 0 {
                break;
            }
        }
        Ok(())
    }

    async fn boot_doc_sync_worker(
        &self,
        task_key: DocSyncTaskKey,
        retry: RetryState,
    ) -> Res<BootDocSyncWorkerResult> {
        let cancel_token = self.cancel_token.child_token();
        let doc_id = task_key.doc_id;
        let endpoint_id = task_key.endpoint_id;
        if self.big_repo.get_doc(&doc_id).await?.is_none() {
            return Ok(BootDocSyncWorkerResult::MissingLocal);
        }
        let Some(sync_state) = self.doc_sync_set.get(&doc_id) else {
            return Ok(BootDocSyncWorkerResult::Deferred);
        };
        if !sync_state.requested_peers.contains_key(&endpoint_id) {
            return Ok(BootDocSyncWorkerResult::Deferred);
        }
        let Some(conn_id) = self.conn_by_peer.get(&endpoint_id).copied() else {
            return Ok(BootDocSyncWorkerResult::Deferred);
        };
        let Some(peer_state) = self.known_peer_set.get(&conn_id) else {
            return Ok(BootDocSyncWorkerResult::Deferred);
        };
        let connection = peer_state.connection.clone();
        let stop_token = doc_worker::spawn_doc_sync_worker(
            doc_id,
            doc_worker::DocSyncTarget {
                endpoint_id,
                connection,
            },
            Arc::clone(&self.big_repo),
            cancel_token.clone(),
            self.msg_tx.clone(),
            retry,
        )
        .await?;
        Ok(BootDocSyncWorkerResult::Spawned(ActiveDocSyncState {
            stop_token,
            retry,
        }))
    }

    async fn batch_stop_docs(&mut self) -> Res<()> {
        use futures::StreamExt;
        use futures_buffered::BufferedStreamExt;

        let mut stop_futs = vec![];

        let stopped_doc_ids = self.scheduler.docs_to_stop.drain().collect::<Vec<_>>();
        for doc_id in &stopped_doc_ids {
            for task_key in self.scheduler.doc_task_keys_for_doc(doc_id) {
                if let Some(active) = self.scheduler.active_docs.remove(&task_key) {
                    stop_futs.push(active.stop_token.stop());
                }
                self.scheduler.clear_doc_task(&task_key);
            }
        }

        futures::stream::iter(stop_futs)
            .buffered_unordered(16)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Res<Vec<_>>>()?;
        Ok(())
    }

    async fn batch_stop_imports(&mut self) -> Res<()> {
        use futures::StreamExt;
        use futures_buffered::BufferedStreamExt;

        self.scheduler
            .queued_tasks
            .retain(|task| !matches!(task, scheduler::SyncTask::Import(_)));
        self.scheduler
            .pending_tasks
            .retain(|task, _| !matches!(task, scheduler::SyncTask::Import(_)));

        let mut stop_futs = vec![];
        for (_doc_id, active) in self.scheduler.active_imports.drain() {
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

    async fn batch_stop_blobs(&mut self) -> Res<()> {
        use futures::StreamExt;
        use futures_buffered::BufferedStreamExt;

        self.scheduler
            .queued_tasks
            .retain(|task| !matches!(task, scheduler::SyncTask::Blob(_)));
        self.scheduler
            .pending_tasks
            .retain(|task, _| !matches!(task, scheduler::SyncTask::Blob(_)));
        self.scheduler.blob_requirements.clear();

        let mut stop_futs = vec![];
        for (_hash, active) in self.scheduler.active_blobs.drain() {
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

    async fn handle_doc_sync_completed(
        &mut self,
        doc_id: DocumentId,
        endpoint_id: EndpointId,
        outcome: am_utils_rs::repo::SyncDocOutcome,
    ) -> Res<()> {
        let task_key = DocSyncTaskKey {
            doc_id: doc_id.clone(),
            endpoint_id,
        };
        let Some(active) = self.scheduler.active_docs.get(&task_key) else {
            return Ok(());
        };
        let active = self
            .scheduler
            .active_docs
            .remove(&task_key)
            .expect("active doc worker must exist");
        active.stop_token.stop().await?;
        self.scheduler.clear_doc_pending(&task_key);

        match outcome {
            am_utils_rs::repo::SyncDocOutcome::Success => {
                let mut partitions_to_advance = Vec::new();
                let mut refreshed_peer = false;

                if let Some(sync_state) = self.doc_sync_set.get_mut(&doc_id) {
                    if let Some(requested_parts) = sync_state.requested_peers.remove(&endpoint_id) {
                        if let Some(conn_id) = self.conn_by_peer.get(&endpoint_id).copied() {
                            if let Some(peer_state) = self.known_peer_set.get_mut(&conn_id) {
                                peer_state.doc_pending_docs =
                                    peer_state.doc_pending_docs.saturating_sub(1);
                                refreshed_peer = true;
                            }
                        }

                        for (partition_key, cursors) in requested_parts {
                            let PartitionKey::BigRepoPartition(partition_id) = partition_key else {
                                continue;
                            };
                            for cursor in cursors {
                                self.scheduler.note_doc_synced(
                                    endpoint_id,
                                    &partition_id,
                                    cursor,
                                    &doc_id.to_string(),
                                );
                            }
                            partitions_to_advance.push(partition_id);
                        }
                    }
                }

                for partition_id in partitions_to_advance {
                    if let Some(peer_key) = self.peer_key_for_endpoint(endpoint_id) {
                        self.maybe_emit_cursor_advance_acks(endpoint_id, &peer_key, &partition_id)
                            .await?;
                    }
                }
                if refreshed_peer {
                    self.refresh_peer_fully_synced_state(endpoint_id).await?;
                }
                self.emit_full_sync_event(FullSyncEvent::DocSyncedWithPeer {
                    endpoint_id,
                    doc_id: doc_id.clone(),
                })
                .await?;
                self.emit_peer_progress_status(
                    endpoint_id,
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
            .doc_sync_set
            .get(&doc_id)
            .map(|state| state.requested_peers.is_empty())
            .unwrap_or(true);
        if requested_peers_empty {
            self.doc_sync_set.remove(&doc_id);
            for task_key in self.scheduler.doc_task_keys_for_doc(&doc_id) {
                self.scheduler.clear_doc_task(&task_key);
                if let Some(active) = self.scheduler.active_docs.remove(&task_key) {
                    active.stop_token.stop().await?;
                }
            }
        } else if let Some(sync_state) = self.doc_sync_set.get(&doc_id) {
            for endpoint_id in sync_state.requested_peers.keys().copied() {
                let pending_key = DocSyncTaskKey {
                    doc_id: doc_id.clone(),
                    endpoint_id,
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

    async fn handle_doc_request_backoff(
        &mut self,
        doc_id: DocumentId,
        endpoint_id: EndpointId,
        delay: Duration,
        previous_attempt_no: usize,
        previous_backoff: Duration,
        _previous_attempt_at: std::time::Instant,
    ) -> Res<()> {
        let task_key = DocSyncTaskKey {
            doc_id: doc_id.clone(),
            endpoint_id,
        };
        if let Some(active) = self.scheduler.active_docs.remove(&task_key) {
            active.stop_token.stop().await?;
        }
        if self
            .doc_sync_set
            .get(&doc_id)
            .and_then(|state| state.requested_peers.get(&endpoint_id))
            .is_none()
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

    async fn handle_doc_missing_local(&mut self, doc_id: DocumentId) -> Res<()> {
        for task_key in self.scheduler.doc_task_keys_for_doc(&doc_id) {
            if let Some(active) = self.scheduler.active_docs.remove(&task_key) {
                active.stop_token.stop().await?;
            }
            self.scheduler.clear_doc_task(&task_key);
        }

        let Some(sync_state) = self.doc_sync_set.remove(&doc_id) else {
            return Ok(());
        };

        let endpoint_ids = sync_state
            .requested_peers
            .keys()
            .copied()
            .collect::<Vec<_>>();
        let mut peers_to_refresh = Vec::new();
        for endpoint_id in sync_state.requested_peers.keys() {
            if let Some(conn_id) = self.conn_by_peer.get(endpoint_id).copied() {
                if let Some(peer_state) = self.known_peer_set.get_mut(&conn_id) {
                    peer_state.doc_pending_docs = peer_state.doc_pending_docs.saturating_sub(1);
                    peers_to_refresh.push(*endpoint_id);
                }
            }
        }
        for endpoint_id in peers_to_refresh {
            self.refresh_peer_fully_synced_state(endpoint_id).await?;
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

    async fn handle_import_doc_completed(
        &mut self,
        doc_id: DocumentId,
        endpoint_id: EndpointId,
        outcome: import_worker::ImportDocOutcome,
    ) -> Res<()> {
        let task_key = ImportSyncTaskKey {
            doc_id: doc_id.clone(),
            endpoint_id,
        };
        let prior_pending = self.scheduler.pending_import_state(&task_key);
        if let Some(active) = self.scheduler.active_imports.remove(&task_key) {
            active.stop_token.stop().await?;
        }
        self.scheduler.clear_import_pending(&task_key);

        if !self.import_doc_set.contains_key(&doc_id) {
            self.scheduler.clear_import_task(&task_key);
            return Ok(());
        }
        self.scheduler.clear_import_task(&task_key);

        match outcome {
            import_worker::ImportDocOutcome::Imported => {
                let endpoint_partitions = self
                    .import_doc_set
                    .get_mut(&doc_id)
                    .and_then(|state| state.requested_peers.remove(&endpoint_id));
                if let Some(partitions) = endpoint_partitions {
                    if let Some(peer_key) = self.peer_key_for_endpoint(endpoint_id) {
                        for (partition_key, cursors) in partitions {
                            let PartitionKey::BigRepoPartition(partition_id) = partition_key else {
                                continue;
                            };
                            for cursor in cursors {
                                self.scheduler.note_cursor_ready_immediate(
                                    endpoint_id,
                                    &partition_id,
                                    cursor,
                                );
                            }
                            self.maybe_emit_cursor_advance_acks(
                                endpoint_id,
                                &peer_key,
                                &partition_id,
                            )
                            .await?;
                        }
                    }
                }

                // After one successful import, doc is now local; push remaining import requests into doc-sync path.
                let remaining = self
                    .import_doc_set
                    .get_mut(&doc_id)
                    .map(|state| state.requested_peers.drain().collect::<Vec<_>>())
                    .unwrap_or_default();
                for (other_endpoint_id, partitions) in remaining {
                    let Some(peer_key) = self.peer_key_for_endpoint(other_endpoint_id) else {
                        continue;
                    };
                    for (partition_key, cursors) in partitions {
                        let PartitionKey::BigRepoPartition(partition_id) = partition_key else {
                            continue;
                        };
                        for cursor in cursors {
                            self.handle_request_doc_sync(
                                peer_key.clone(),
                                partition_id.clone(),
                                doc_id.clone(),
                                cursor,
                            )
                            .await?;
                        }
                    }
                }
            }
            import_worker::ImportDocOutcome::LocalPresent => {
                let Some(partitions) = self
                    .import_doc_set
                    .get_mut(&doc_id)
                    .and_then(|state| state.requested_peers.remove(&endpoint_id))
                else {
                    return Ok(());
                };
                let Some(peer_key) = self.peer_key_for_endpoint(endpoint_id) else {
                    return Ok(());
                };
                for (partition_key, cursors) in partitions {
                    let PartitionKey::BigRepoPartition(partition_id) = partition_key else {
                        continue;
                    };
                    for cursor in cursors {
                        self.handle_request_doc_sync(
                            peer_key.clone(),
                            partition_id.clone(),
                            doc_id.clone(),
                            cursor,
                        )
                        .await?;
                    }
                }
            }
            import_worker::ImportDocOutcome::MissingOnRemote => {
                warn!(
                    %doc_id, endpoint_id = ?endpoint_id,
                    "import worker could not fetch doc from remote; waiting for replay/retry"
                );
                if self
                    .import_doc_set
                    .get(&doc_id)
                    .and_then(|state| state.requested_peers.get(&endpoint_id))
                    .is_none()
                {
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
                self.scheduler.set_import_backoff(&task_key, pending);
            }
        }

        let clear_doc_import_state = self
            .import_doc_set
            .get(&doc_id)
            .is_none_or(|state| state.requested_peers.is_empty());
        if clear_doc_import_state {
            self.import_doc_set.remove(&doc_id);
            for import_task_key in self.scheduler.import_task_keys_for_doc(&doc_id) {
                self.scheduler.clear_import_task(&import_task_key);
                if let Some(active) = self.scheduler.active_imports.remove(&import_task_key) {
                    active.stop_token.stop().await?;
                }
            }
        }
        self.refresh_peer_fully_synced_state(endpoint_id).await?;
        Ok(())
    }

    async fn handle_import_doc_backoff(
        &mut self,
        doc_id: DocumentId,
        endpoint_id: EndpointId,
        delay: Duration,
        previous_attempt_no: usize,
        previous_backoff: Duration,
        _previous_attempt_at: std::time::Instant,
    ) -> Res<()> {
        let task_key = ImportSyncTaskKey {
            doc_id,
            endpoint_id,
        };
        if let Some(active) = self.scheduler.active_imports.remove(&task_key) {
            active.stop_token.stop().await?;
        }
        if self
            .import_doc_set
            .get(&task_key.doc_id)
            .and_then(|state| state.requested_peers.get(&task_key.endpoint_id))
            .is_none()
        {
            self.scheduler.clear_import_task(&task_key);
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
        self.scheduler.set_import_backoff(&task_key, pending);
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
                    .get_partition_member_events_for_peer(
                        &self.local_peer_key,
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
        Ok(())
    }

    async fn remove_hash_from_blob_scope(&mut self, scope: BlobScope, hash: &str) -> Res<()> {
        if let Some(part_set) = self.scheduler.blob_requirements.get_mut(hash) {
            part_set.remove(&PartitionKey::BlobScope(scope));
        }
        self.remove_blob_tracking_if_unused(hash).await
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

    fn active_peers_for_blob(&self, hash: &str) -> Vec<EndpointId> {
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
                        self.cancel_token.child_token(),
                        self.msg_tx.clone(),
                        self.sync_progress_tx.clone(),
                        Arc::clone(&self.blobs_repo),
                        self.iroh_endpoint.clone(),
                        retry,
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
        endpoint_id: Option<EndpointId>,
    ) -> Res<()> {
        if !self.scheduler.blob_requirements.contains_key(&hash) {
            return Ok(());
        }
        let sync_hash = hash.clone();
        if let Some(active) = self.scheduler.active_blobs.remove(&hash) {
            active.stop_token.stop().await?;
        }
        self.scheduler.clear_blob_pending(&hash);
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
        Ok(())
    }

    fn peer_sync_task_id(endpoint_id: EndpointId) -> String {
        format!("sync/full/peer/{endpoint_id}")
    }

    async fn emit_peer_progress_status(
        &self,
        endpoint_id: EndpointId,
        deets: ProgressUpdateDeets,
    ) -> Res<()> {
        if !self.has_progress_repo() {
            return Ok(());
        }
        let task_id = Self::peer_sync_task_id(endpoint_id);
        self.emit_progress_task(
            task_id.clone(),
            vec![
                "/type/sync".to_string(),
                "/sync/full".to_string(),
                "/kind/peer_sync".to_string(),
                format!("/peer/{endpoint_id}"),
            ],
        )
        .await?;
        self.emit_progress_update(&task_id, deets, Some(format!("peer sync {endpoint_id}")))
            .await?;
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

    async fn remove_peer_from_doc_sync_set(&mut self, endpoint_id: EndpointId) -> Res<()> {
        let mut to_remove = Vec::new();
        let mut to_stop = Vec::new();
        for (doc_id, sync_state) in &mut self.doc_sync_set {
            sync_state.requested_peers.remove(&endpoint_id);
            self.scheduler.clear_doc_task(&DocSyncTaskKey {
                doc_id: doc_id.clone(),
                endpoint_id,
            });
            if let Some(active) = self.scheduler.active_docs.remove(&DocSyncTaskKey {
                doc_id: doc_id.clone(),
                endpoint_id,
            }) {
                to_stop.push(active.stop_token);
            }
            if sync_state.requested_peers.is_empty() {
                to_remove.push(doc_id.clone());
            }
        }
        for task_key in self.scheduler.doc_task_keys_for_peer(endpoint_id) {
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
            self.doc_sync_set.remove(&doc_id);
        }
        Ok(())
    }

    async fn remove_peer_from_import_doc_set(&mut self, endpoint_id: EndpointId) -> Res<()> {
        let mut to_remove = Vec::new();
        for (doc_id, import_state) in &mut self.import_doc_set {
            import_state.requested_peers.remove(&endpoint_id);
            if import_state.requested_peers.is_empty() {
                to_remove.push(doc_id.clone());
            }
        }
        for task_key in self.scheduler.import_task_keys_for_peer(endpoint_id) {
            if let Some(active) = self.scheduler.active_imports.remove(&task_key) {
                active.stop_token.stop().await?;
            }
            self.scheduler.clear_import_task(&task_key);
        }
        for doc_id in to_remove {
            for task_key in self.scheduler.import_task_keys_for_doc(&doc_id) {
                if let Some(active) = self.scheduler.active_imports.remove(&task_key) {
                    active.stop_token.stop().await?;
                }
                self.scheduler.clear_import_task(&task_key);
            }
            self.import_doc_set.remove(&doc_id);
        }
        Ok(())
    }

    async fn refresh_peer_fully_synced_state(&mut self, endpoint_id: EndpointId) -> Res<()> {
        let Some(conn_id) = self.conn_by_peer.get(&endpoint_id).copied() else {
            return Ok(());
        };
        let mut emit_full = None;
        let mut emit_stale = false;
        let endpoint_has_doc_work = self.scheduler.endpoint_has_doc_work(endpoint_id);
        if let Some(peer_state) = self.known_peer_set.get_mut(&conn_id) {
            let is_fully_synced = peer_state.bootstrap_ready
                && peer_state.bootstrap_remaining_docs == 0
                && peer_state.live_ready
                && !endpoint_has_doc_work
                && peer_state.doc_pending_docs == 0;
            debug!(
                endpoint_id = ?endpoint_id,
                bootstrap_ready = peer_state.bootstrap_ready,
                bootstrap_synced_docs = peer_state.bootstrap_synced_docs,
                bootstrap_remaining_docs = peer_state.bootstrap_remaining_docs,
                doc_pending_docs = peer_state.doc_pending_docs,
                endpoint_has_doc_work,
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
            self.emit_peer_progress_status(
                endpoint_id,
                ProgressUpdateDeets::Status {
                    severity: ProgressSeverity::Info,
                    message: format!("peer fully synced ({doc_count} docs converged)"),
                },
            )
            .await?;
            self.emit_full_sync_event(FullSyncEvent::PeerFullSynced {
                endpoint_id,
                doc_count,
            })
            .await?;
        } else if emit_stale {
            self.emit_peer_progress_status(
                endpoint_id,
                ProgressUpdateDeets::Status {
                    severity: ProgressSeverity::Warn,
                    message: "peer no longer fully synced".to_string(),
                },
            )
            .await?;
            self.emit_full_sync_event(FullSyncEvent::StalePeer { endpoint_id })
                .await?;
        }
        self.refresh_full_sync_waiters();
        Ok(())
    }

    fn handle_unknown_peer_request(&self, peer_key: &str, context: String) -> Res<()> {
        if self.seen_peer_keys.contains(peer_key) {
            debug!(
                peer_key,
                context, "ignoring stale request for disconnected peer"
            );
            return Ok(());
        }
        eyre::bail!(
            "received sync request for never-seen peer: peer_key={peer_key}; context={context}"
        );
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
        endpoint_id: EndpointId,
        partition: PartitionKey,
        hash: &str,
    ) -> Res<()> {
        self.emit_peer_progress_status(
            endpoint_id,
            ProgressUpdateDeets::Status {
                severity: ProgressSeverity::Info,
                message: format!("blob download started: {hash}"),
            },
        )
        .await?;
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
        _endpoint_id: EndpointId,
        _partition: PartitionKey,
        _hash: &str,
        _done: u64,
    ) -> Res<()> {
        Ok(())
    }

    async fn emit_blob_progress_finished(
        &self,
        endpoint_id: EndpointId,
        partition: PartitionKey,
        hash: &str,
        success: bool,
    ) -> Res<()> {
        self.emit_peer_progress_status(
            endpoint_id,
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
