// FIXME; split the Worker into multiple states

use crate::interlude::*;

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

pub struct WorkerHandle {
    msg_tx: mpsc::UnboundedSender<Msg>,
    /// Option allows you to take it out
    pub events_rx: Option<tokio::sync::broadcast::Receiver<FullSyncEvent>>,
}

#[derive(Clone)]
pub enum FullSyncEvent {
    PeerFullSynced {
        endpoint_id: EndpointId,
        // doc_count: usize,
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
        peer_key: am_utils_rs::sync::PeerKey,
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
        event: am_utils_rs::sync::PeerSyncProgressEvent,
    },
    PeerSyncWorkerEvent {
        endpoint_id: EndpointId,
        event: am_utils_rs::sync::PeerSyncWorkerEvent,
    },
}
type DocPeerStateView = HashMap<ConnectionId, samod::PeerDocState>;

#[derive(Debug)]
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
        peer_key: am_utils_rs::sync::PeerKey,
        partition_ids: HashSet<am_utils_rs::sync::PartitionId>,
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
    partition_sync_node: Arc<am_utils_rs::sync::SyncNodeHandle>,
    sync_store: am_utils_rs::sync::SyncStoreHandle,
    iroh_endpoint: iroh::Endpoint,
) -> Res<(WorkerHandle, StopToken)> {
    use crate::repos::Repo;

    let (msg_tx, mut msg_rx) = mpsc::unbounded_channel();
    let (sync_progress_tx, mut sync_progress_rx) = mpsc::channel::<SyncProgressMsg>(8192);
    let (samod_sync_tx, mut samod_sync_rx) =
        mpsc::channel::<am_utils_rs::sync::SamodSyncRequest>(8192);
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
        active_blobs: default(),
        pending_blobs: default(),
        synced_blobs: default(),

        known_peer_set: default(),
        conn_by_peer: default(),
        peer_partition_sessions: default(),
        peer_registrations: default(),

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
    for hash in worker.doc_blobs_index_repo.list_all_hashes().await? {
        worker
            .known_blob_set
            .entry(hash.clone())
            .or_default()
            .insert(PartitionKey::DocBlobsFullSync);
        worker.blobs_to_boot.insert(hash);
    }

    // let (peer_infos, mut peer_updates) = rcx.big_repo.repo().connected_peers();
    // let (peer_observer, observer_stop) = big_repo.spawn_peer_sync_observer();
    let fut = {
        let cancel_token = cancel_token.clone();
        let mut janitor_tick = tokio::time::interval(Duration::from_millis(500));
        // let peer_observer = observer.subscribe();
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
                    // val = peer_updates.next() => {
                    //     let Some(connections) = val else {
                    //         eyre::bail!("SharedBigRepo is closed, wrong shutdown order");
                    //     };
                    //     worker.handle_peer_conn_changes(connections).await?;
                    // },
                    // val = peer_observer.recv() => {
                    //     let evt = match val {
                    //         Ok(val) => val,
                    //         Err(tokio::sync::broadcast::error::RecvError::Closed) => todo!(),
                    //         Err(tokio::sync::broadcast::error::RecvError::Lagged(dropped_count)) => {
                    //             eyre::bail!("we're laggingn on peer events: dropped_count = {dropped_count}");
                    //         },
                    //     };
                    //     worker.handle_peer_evt(evt).await?;
                    // }
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
            for (_endpoint_id, mut session) in worker.peer_partition_sessions.drain() {
                session.forward_cancel_token.cancel();
                session.stop.stop().await?;
                for join_handle in session.forward_handles.drain(..) {
                    utils_rs::wait_on_handle_with_timeout(join_handle, Duration::from_secs(1))
                        .await?;
                }
            }
            let registered_peers = worker
                .peer_registrations
                .keys()
                .cloned()
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
    local_peer_key: am_utils_rs::sync::PeerKey,
    cancel_token: CancellationToken,
    blobs_repo: Arc<BlobsRepo>,
    doc_blobs_index_repo: Arc<DocBlobsIndexRepo>,
    progress_repo: Option<Arc<ProgressRepo>>,
    partition_sync_node: Arc<am_utils_rs::sync::SyncNodeHandle>,
    sync_store: am_utils_rs::sync::SyncStoreHandle,
    iroh_endpoint: iroh::Endpoint,

    partitions: HashMap<PartitionKey, Partition>,

    docs_to_boot: HashSet<DocumentId>,
    docs_to_stop: HashSet<DocumentId>,
    blobs_to_boot: HashSet<String>,
    partitions_to_refresh: HashSet<PartitionKey>,
    peer_sessions_to_refresh: HashSet<EndpointId>,

    // doc_ref_counts: HashMap<DocumentId, usize>,
    known_blob_set: HashMap<String, HashSet<PartitionKey>>,
    // peer_docs: HashMap<EndpointId, HashSet<DocumentId>>,
    samod_doc_set: HashMap<DocumentId, SamodSyncedDoc>,
    active_docs: HashMap<DocumentId, ActiveDocSyncState>,
    pending_docs: HashMap<DocumentId, PendingDocSyncState>,

    active_blobs: HashMap<String, ActiveBlobSyncState>,
    pending_blobs: HashMap<String, PendingBlobSyncState>,
    synced_blobs: HashMap<String, SyncedBlobSyncState>,

    msg_tx: mpsc::UnboundedSender<Msg>,
    sync_progress_tx: mpsc::Sender<SyncProgressMsg>,
    samod_sync_tx: mpsc::Sender<am_utils_rs::sync::SamodSyncRequest>,
    events_tx: tokio::sync::broadcast::Sender<FullSyncEvent>,
    max_active_sync_workers: usize,

    known_peer_set: HashMap<ConnectionId, PeerSyncState>,
    conn_by_peer: HashMap<EndpointId, ConnectionId>,
    peer_partition_sessions: HashMap<EndpointId, PeerPartitionSession>,
    peer_registrations: HashMap<am_utils_rs::sync::PeerKey, usize>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum PartitionKey {
    BigRepoPartition(am_utils_rs::sync::PartitionId),
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
    samod_syncing_docs: HashSet<DocumentId>,
    partitions: HashSet<PartitionKey>,
    peer_key: am_utils_rs::sync::PeerKey,
    emitted_full_synced: bool,
}

struct PeerPartitionSession {
    stop: am_utils_rs::sync::PeerSyncWorkerStopToken,
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
    requested_peers: HashMap<EndpointId, HashSet<PartitionKey>>,
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
    async fn inc_peer_registration(&mut self, peer_key: am_utils_rs::sync::PeerKey) -> Res<()> {
        let count = self.peer_registrations.entry(peer_key.clone()).or_insert(0);
        *count += 1;
        if *count == 1 {
            self.partition_sync_node
                .register_local_peer(peer_key)
                .await?;
        }
        Ok(())
    }

    async fn dec_peer_registration(&mut self, peer_key: am_utils_rs::sync::PeerKey) -> Res<()> {
        let Some(count) = self.peer_registrations.get_mut(&peer_key) else {
            return Ok(());
        };
        *count = count.saturating_sub(1);
        if *count == 0 {
            self.peer_registrations.remove(&peer_key);
            self.partition_sync_node
                .unregister_local_peer(peer_key)
                .await?;
        }
        Ok(())
    }

    async fn handle_peer_sync_worker_progress(
        &mut self,
        endpoint_id: EndpointId,
        event: am_utils_rs::sync::PeerSyncProgressEvent,
    ) -> Res<()> {
        match event {
            am_utils_rs::sync::PeerSyncProgressEvent::PhaseStarted { phase } => {
                debug!(endpoint_id = ?endpoint_id, phase, "peer sync phase started");
            }
            am_utils_rs::sync::PeerSyncProgressEvent::PhaseFinished { phase, elapsed } => {
                debug!(endpoint_id = ?endpoint_id, phase, elapsed_ms = elapsed.as_millis(), "peer sync phase finished");
            }
            am_utils_rs::sync::PeerSyncProgressEvent::CursorUpdated { partition_id } => {
                debug!(endpoint_id = ?endpoint_id, partition_id, "peer sync cursor updated");
            }
        }
        Ok(())
    }

    async fn handle_peer_sync_worker_event(
        &mut self,
        endpoint_id: EndpointId,
        event: am_utils_rs::sync::PeerSyncWorkerEvent,
    ) -> Res<()> {
        match event {
            am_utils_rs::sync::PeerSyncWorkerEvent::Bootstrapped {
                peer,
                partition_count,
            } => {
                debug!(endpoint_id = ?endpoint_id, peer, partition_count, "peer sync worker bootstrapped");
                if let Some(conn_id) = self.conn_by_peer.get(&endpoint_id).copied() {
                    let mut emit_peer_full_synced = None;
                    if let Some(peer_state) = self.known_peer_set.get_mut(&conn_id) {
                        peer_state.emitted_full_synced = true;
                        emit_peer_full_synced = Some(FullSyncEvent::PeerFullSynced { endpoint_id });
                    }
                    if let Some(event) = emit_peer_full_synced {
                        self.emit_full_sync_event(event).await?;
                    }
                }
            }
            am_utils_rs::sync::PeerSyncWorkerEvent::Live { peer } => {
                debug!(endpoint_id = ?endpoint_id, peer, "peer sync worker entered live mode");
            }
        }
        Ok(())
    }

    fn endpoint_for_peer_key(&self, peer_key: &str) -> Option<EndpointId> {
        self.known_peer_set
            .values()
            .find(|state| state.peer_key == peer_key)
            .map(|state| state.endpoint_id)
    }

    fn available_boot_budget(&self) -> usize {
        let active_count = self.active_docs.len() + self.active_blobs.len();
        self.max_active_sync_workers.saturating_sub(active_count)
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
                let mut should_inc_registration = true;
                if let Some(old_state) = old_state.as_ref() {
                    if old_state.peer_key == peer_key {
                        should_inc_registration = false;
                    } else {
                        self.dec_peer_registration(old_state.peer_key.clone())
                            .await?;
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
                        peer_key: peer_key.clone(),
                        emitted_full_synced: false,
                        samod_syncing_docs: default(),
                    },
                );
                self.conn_by_peer.insert(endpoint_id, conn_id);
                for part_key in new_parts {
                    self.add_peer_to_part(part_key, endpoint_id).await?;
                }
                if should_inc_registration {
                    self.inc_peer_registration(peer_key.clone()).await?;
                }
                resp.send(()).inspect_err(|_| warn!(ERROR_CALLER)).ok();
            }
            Msg::DelPeer { endpoint_id, resp } => {
                if let Some(conn_id) = self.conn_by_peer.remove(&endpoint_id) {
                    if let Some(state) = self.known_peer_set.remove(&conn_id) {
                        self.dec_peer_registration(state.peer_key).await?;
                        self.remove_peer_partition_session(endpoint_id).await?;
                        for part_key in state.partitions {
                            self.remove_peer_from_part(part_key, endpoint_id).await?;
                        }
                    }
                }
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
        }
        eyre::Ok(())
    }

    async fn batch_refresh_paritions(&mut self) -> Res<()> {
        let mut double = std::mem::replace(&mut self.partitions_to_refresh, default());
        let mut peer_sessions_to_boot = HashMap::new();
        for part_key in double.drain() {
            let (activate, is_active) = {
                let part = self
                    .partitions
                    .get(&part_key)
                    .ok_or_eyre("parition not found")?;
                let activate = match &part.deets {
                    ParitionDeets::BigRepoPartition { peers } => {
                        for endpoint_id in peers {
                            peer_sessions_to_boot
                                .entry(endpoint_id.clone())
                                .or_insert_with(|| HashSet::new())
                                .insert(part_key.clone());
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
        for (_conn_id, peer_state) in &mut self.known_peer_set {
            let Some(parts) = peer_sessions_to_boot.remove(&peer_state.endpoint_id) else {
                peer_state.partitions.clear();
                continue;
            };
            let Some(session) = self.peer_partition_sessions.get(&peer_state.endpoint_id) else {
                peer_state.partitions = parts;
                self.peer_sessions_to_refresh
                    .insert(peer_state.endpoint_id.clone());
                continue;
            };
            if session.partitions != peer_state.partitions {
                peer_state.partitions = parts;
                self.peer_sessions_to_refresh
                    .insert(peer_state.endpoint_id.clone());
            }
        }
        Ok(())
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
        let mut budget = self.available_boot_budget();
        if budget == 0 {
            return;
        }

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
            .take(budget)
            .collect();
        budget = budget.saturating_sub(due_docs.len());
        self.docs_to_boot.extend(due_docs);

        if budget == 0 {
            return;
        }

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
                .take(budget),
        );
    }
}

// Docs related methods
impl Worker {
    async fn batch_refresh_peer_sessions(&mut self) -> Res<()> {
        let mut double = std::mem::replace(&mut self.peer_sessions_to_refresh, default());
        for endpoint_id in double.drain() {
            let (peer_key, parts) = {
                let conn_id = self.conn_by_peer.get(&endpoint_id).expect(ERROR_IMPOSSIBLE);
                let peer_state = self
                    .known_peer_set
                    .get_mut(conn_id)
                    .expect(ERROR_IMPOSSIBLE);
                (peer_state.peer_key.clone(), peer_state.partitions.clone())
            };
            self.refresh_peer_partition_session(endpoint_id, peer_key, parts)
                .await?;
        }
        self.peer_sessions_to_refresh = double;
        Ok(())
    }

    async fn refresh_peer_partition_session(
        &mut self,
        endpoint_id: EndpointId,
        peer_key: am_utils_rs::sync::PeerKey,
        partitions: HashSet<PartitionKey>,
    ) -> Res<()> {
        let session = self.peer_partition_sessions.remove(&endpoint_id);
        if let Some(session) = session {
            session.stop.stop().await?;
        }
        if partitions.is_empty() {
            return Ok(());
        }
        let rpc_client = irpc_iroh::client::<am_utils_rs::sync::PartitionSyncRpc>(
            self.iroh_endpoint.clone(),
            iroh::EndpointAddr::new(endpoint_id),
            PARTITION_SYNC_ALPN,
        );
        let (mut handle, stop) = am_utils_rs::sync::spawn_peer_sync_worker(
            self.local_peer_key.clone(),
            peer_key,
            rpc_client,
            Arc::clone(&self.big_repo),
            self.sync_store.clone(),
            self.samod_sync_tx.clone(),
            partitions
                .iter()
                .map(|key| match key {
                    PartitionKey::DocBlobsFullSync => unreachable!(),
                    PartitionKey::BigRepoPartition(id) => id.clone(),
                })
                .collect(),
        )
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
        handle.start().await?;
        self.peer_partition_sessions.insert(
            endpoint_id,
            PeerPartitionSession {
                stop,
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

    async fn handle_samod_sync_batch(
        &mut self,
        buffer: &mut Vec<am_utils_rs::sync::SamodSyncRequest>,
    ) -> Res<()> {
        for req in buffer.drain(..) {
            self.handle_samod_sync_request(req).await?;
        }
        Ok(())
    }

    async fn handle_samod_sync_request(
        &mut self,
        req: am_utils_rs::sync::SamodSyncRequest,
    ) -> Res<()> {
        match req {
            am_utils_rs::sync::SamodSyncRequest::RequestDocSync {
                peer_key,
                partition_id,
                doc_id,
                reason,
            } => {
                let doc_id = doc_id
                    .parse::<DocumentId>()
                    .map_err(|err| ferr!("invalid doc id '{}': {err}", doc_id))?;
                let endpoint_id = self
                    .endpoint_for_peer_key(&peer_key)
                    .expect(ERROR_IMPOSSIBLE);
                self.emit_stale_peer(endpoint_id.clone()).await?;
                let samod_doc_state = self.samod_doc_set.entry(doc_id.clone()).or_default();
                samod_doc_state
                    .requested_peers
                    .entry(endpoint_id.clone())
                    .or_default()
                    .insert(PartitionKey::BigRepoPartition(partition_id));
                debug!(peer_key, %doc_id, reason, "received samod doc sync request");
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
            am_utils_rs::sync::SamodSyncRequest::DocMissingLocal {
                peer_key,
                partition_id: _,
                doc_id,
            } => {
                let doc_id = doc_id
                    .parse::<DocumentId>()
                    .map_err(|err| ferr!("invalid doc id '{}': {err}", doc_id))?;
                let endpoint_id = self
                    .endpoint_for_peer_key(&peer_key)
                    .expect(ERROR_IMPOSSIBLE);
                self.emit_stale_peer(endpoint_id).await?;
                debug!(peer_key, %doc_id, "received samod missing-local doc request");
            }
            am_utils_rs::sync::SamodSyncRequest::DocDeleted {
                peer_key,
                partition_id: _,
                doc_id,
            } => {
                let doc_id = doc_id
                    .parse::<DocumentId>()
                    .map_err(|err| ferr!("invalid doc id '{}': {err}", doc_id))?;
                let endpoint_id = self
                    .endpoint_for_peer_key(&peer_key)
                    .expect(ERROR_IMPOSSIBLE);
                self.emit_stale_peer(endpoint_id).await?;
                debug!(peer_key, %doc_id, "received samod doc deleted request");
                self.docs_to_stop.insert(doc_id);
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
        let mut budget = self.available_boot_budget();
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
            let active = self.boot_doc_sync_worker(doc_id.clone(), retry).await?;
            self.pending_docs.remove(&doc_id);
            self.active_docs.insert(doc_id, active);
            budget = budget.saturating_sub(1);
        }
        self.docs_to_boot = double;
        Ok(())
    }

    async fn boot_doc_sync_worker(
        &self,
        doc_id: DocumentId,
        retry: RetryState,
    ) -> Res<ActiveDocSyncState> {
        let cancel_token = self.cancel_token.child_token();
        let stop_token = doc_worker::spawn_doc_sync_worker(
            doc_id.clone(),
            Arc::clone(&self.big_repo),
            cancel_token.clone(),
            self.msg_tx.clone(),
            retry,
        )
        .await?;
        Ok(ActiveDocSyncState {
            latest_heads: ChangeHashSet(Arc::from([])),
            stop_token,
        })
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

    async fn handle_doc_peer_state_change(
        &mut self,
        doc_id: DocumentId,
        diff: DocPeerStateView,
    ) -> Res<()> {
        let Some(active_state) = self.active_docs.get(&doc_id) else {
            return Ok(());
        };
        let local_heads = &active_state.latest_heads;
        let mut events_to_emit = Vec::new();
        let samod_doc = self.samod_doc_set.get_mut(&doc_id).expect(ERROR_IMPOSSIBLE);
        for (conn_id, diff) in diff {
            let Some(peer_state) = self.known_peer_set.get_mut(&conn_id) else {
                warn!(?conn_id, "unkown connection for FullSyncWorker");
                continue;
            };
            let they_have_our_changes = diff
                .shared_heads
                .as_ref()
                .map(|heads| heads_equal_as_set(heads, local_heads))
                .unwrap_or_default();
            let we_have_their_changes = diff
                .their_heads
                .as_ref()
                .zip(diff.shared_heads.as_ref())
                .map(|(their, shared)| heads_equal_as_set(their, shared))
                .unwrap_or_default();

            if they_have_our_changes && we_have_their_changes {
                events_to_emit.push(FullSyncEvent::DocSyncedWithPeer {
                    endpoint_id: peer_state.endpoint_id,
                    doc_id: doc_id.clone(),
                });
                samod_doc.requested_peers.remove(&peer_state.endpoint_id);
            }
            // events_to_emit.push(FullSyncEvent::PeerFullSynced {
            //     endpoint_id: peer_state.endpoint_id,
            // });
            // peer_state.emitted_full_synced = true;
        }
        if self.active_docs.contains_key(&doc_id) && samod_doc.requested_peers.is_empty() {
            let Some(active) = self.active_docs.remove(&doc_id) else {
                return Ok(());
            };
            let _latest_heads = active.latest_heads.clone();
            active.stop_token.stop().await?;
            self.pending_docs.remove(&doc_id);
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
}

// Blobs related methods
impl Worker {
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

    async fn handle_doc_blobs_event(&mut self, evt: &DocBlobsIndexEvent) -> Res<()> {
        match evt {
            DocBlobsIndexEvent::Updated { doc_id } => {
                let hashes = self
                    .doc_blobs_index_repo
                    .list_hashes_for_doc(doc_id)
                    .await?;
                for hash in hashes {
                    self.known_blob_set
                        .entry(hash.clone())
                        .or_default()
                        .insert(PartitionKey::DocBlobsFullSync);
                    if !self.synced_blobs.contains_key(&hash) {
                        self.blobs_to_boot.insert(hash);
                    }
                }
            }
            DocBlobsIndexEvent::Deleted { .. } => {}
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
        let mut budget = self.available_boot_budget();
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
            // FIXME: why are we adding it to pending
            // if there are no peers?
            if peers.is_empty() {
                let prior = prior_pending.unwrap_or(PendingBlobSyncState {
                    attempt_no: 0,
                    last_backoff: Duration::from_millis(0),
                    last_attempt_at: std::time::Instant::now(),
                    due_at: std::time::Instant::now(),
                });
                let delay = next_backoff_delay(prior.last_backoff, Duration::from_millis(500));
                self.pending_blobs.insert(
                    hash.clone(),
                    PendingBlobSyncState {
                        attempt_no: prior.attempt_no + 1,
                        last_backoff: delay,
                        last_attempt_at: std::time::Instant::now(),
                        due_at: std::time::Instant::now() + delay,
                    },
                );
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
        let active = self
            .active_blobs
            .remove(&hash)
            .expect("blob backoff requested by non-active worker");
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
            delay,
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

    async fn emit_full_sync_event(&self, event: FullSyncEvent) -> Res<()> {
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

        let mut worker_started: HashSet<(PartitionKey, String)> = HashSet::new();
        let mut started: HashSet<BlobProgressKey> = HashSet::new();
        let mut progress_latest: HashMap<BlobProgressKey, u64> = HashMap::new();
        let mut materialize_started: HashSet<(PartitionKey, String)> = HashSet::new();
        let mut finished: HashMap<BlobProgressKey, bool> = HashMap::new();
        let mut worker_finished: HashMap<(PartitionKey, String), (bool, String)> = HashMap::new();

        for msg in buffer.drain(..) {
            tracing::info!(?msg, "XXX");
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

        tracing::info!(
            batch_len,
            worker_started = worker_started.len(),
            download_started = started.len(),
            progress_updates = progress_latest.len(),
            materialize_started = materialize_started.len(),
            download_finished = finished.len(),
            worker_finished = worker_finished.len(),
            "handle_sync_progress_batch"
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
