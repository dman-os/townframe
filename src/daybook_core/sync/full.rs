use crate::interlude::*;

use iroh::EndpointId;
use samod::ConnectionId;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::blobs::BlobsRepo;
use crate::drawer::{DrawerEvent, DrawerRepo};
use crate::index::{DocBlobsIndexEvent, DocBlobsIndexRepo};
use crate::repo::RepoCtx;

#[path = "full_blob_worker.rs"]
mod blob_worker_impl;
#[path = "full_doc_worker.rs"]
mod doc_worker_impl;

pub struct WorkerHandle {
    msg_tx: mpsc::UnboundedSender<Msg>,
    /// Option allows you to take it out
    pub events_rx: Option<tokio::sync::broadcast::Receiver<FullSyncEvent>>,
}

#[derive(Clone)]
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
    DocHeadsUpdated {
        doc_id: DocumentId,
        heads: ChangeHashSet,
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
}
type DocPeerStateView = HashMap<ConnectionId, samod::PeerDocState>;

impl WorkerHandle {
    pub async fn set_connection(&self, endpoint_id: EndpointId, conn_id: ConnectionId) -> Res<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let msg = Msg::SetPeer {
            resp: tx,
            conn_id,
            endpoint_id,
            partitions: [PartitionKey::FullSync, PartitionKey::DocBlobsFullSync].into(),
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
        utils_rs::wait_on_handle_with_timeout(self.join_handle, Duration::from_secs(1)).await
    }
}

pub async fn start_full_sync_worker(
    rcx: Arc<RepoCtx>,
    drawer_repo: Arc<DrawerRepo>,
    blobs_repo: Arc<BlobsRepo>,
    doc_blobs_index_repo: Arc<DocBlobsIndexRepo>,
    iroh_endpoint: iroh::Endpoint,
    cancel_token: CancellationToken,
) -> Res<(WorkerHandle, StopToken)> {
    use crate::repos::Repo;

    let (msg_tx, mut msg_rx) = mpsc::unbounded_channel();
    let (events_tx, events_rx) = tokio::sync::broadcast::channel(16);

    let mut worker = Worker {
        acx: rcx.acx.clone(),
        cancel_token: cancel_token.clone(),
        msg_tx: msg_tx.clone(),
        events_tx,
        partitions: [
            //
            (
                PartitionKey::FullSync,
                Partition {
                    is_active: false,
                    deets: ParitionDeets::FullSync { peers: default() },
                },
            ),
            (
                PartitionKey::DocBlobsFullSync,
                Partition {
                    is_active: false,
                    deets: ParitionDeets::DocBlobsFullSync { peers: default() },
                },
            ),
        ]
        .into(),

        blobs_repo,
        doc_blobs_index_repo,
        iroh_endpoint,
        active_docs: default(),
        pending_docs: default(),
        synced_docs: default(),
        active_blobs: default(),
        pending_blobs: default(),
        synced_blobs: default(),
        known_peer_set: default(),
        conn_by_peer: default(),
        known_doc_set: default(),
        known_blob_set: default(),
        docs_to_boot: default(),
        docs_to_stop: default(),
        blobs_to_boot: default(),
        partitions_to_refresh: default(),
        max_active_sync_workers: 24,
    };

    let drawer_rx = drawer_repo.subscribe(crate::repos::SubscribeOpts { capacity: 1024 });
    let doc_blobs_rx =
        worker
            .doc_blobs_index_repo
            .subscribe(crate::repos::SubscribeOpts { capacity: 1024 });
    let (_drawer_heads, known_docs) = drawer_repo.list_just_ids().await?;
    worker.add_doc(rcx.doc_app.document_id().clone()).await?;
    worker.add_doc(rcx.doc_drawer.document_id().clone()).await?;
    for doc_id in known_docs {
        let doc_id: DocumentId = doc_id
            .parse()
            .wrap_err("unable to parse doc_id from drawer")?;
        worker.add_doc(doc_id.clone()).await?;
    }
    for hash in worker.doc_blobs_index_repo.list_all_hashes().await? {
        worker
            .known_blob_set
            .entry(hash.clone())
            .or_default()
            .insert(PartitionKey::DocBlobsFullSync);
        worker.blobs_to_boot.insert(hash);
    }

    // let (peer_infos, mut peer_updates) = rcx.acx.repo().connected_peers();
    // let (peer_observer, observer_stop) = acx.spawn_peer_sync_observer();
    let fut = {
        let cancel_token = cancel_token.clone();
        let mut janitor_tick = tokio::time::interval(Duration::from_millis(500));
        // let peer_observer = observer.subscribe();
        async move {
            let doc_blobs_rx = doc_blobs_rx;
            loop {
                if !worker.partitions_to_refresh.is_empty() {
                    worker.batch_refresh_paritions().await?;
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
                    //         eyre::bail!("AmCtx is closed, wrong shutdown order");
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
                    _ = janitor_tick.tick() => {
                        worker.backoff_janitor_enqueue_due();
                    }
                    val = drawer_rx.recv_async() => {
                        let evt = match val {
                            Ok(val) => val,
                            Err(err) => match err {
                                crate::repos::RecvError::Closed => {
                                    warn!("DrawerRepo shutdown, closing loop");
                                    break;
                                },
                                crate::repos::RecvError::Dropped { dropped_count } => {
                                    eyre::bail!("we're dropping drawer events: dropped_count = {dropped_count}");
                                }
                            },
                        };
                        worker.handle_drawer_event(&evt).await?;
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
    acx: AmCtx,
    cancel_token: CancellationToken,
    blobs_repo: Arc<BlobsRepo>,
    doc_blobs_index_repo: Arc<DocBlobsIndexRepo>,
    iroh_endpoint: iroh::Endpoint,

    partitions: HashMap<PartitionKey, Partition>,

    docs_to_boot: HashSet<DocumentId>,
    docs_to_stop: HashSet<DocumentId>,
    blobs_to_boot: HashSet<String>,
    partitions_to_refresh: HashSet<PartitionKey>,

    known_doc_set: HashMap<DocumentId, HashSet<PartitionKey>>,
    known_blob_set: HashMap<String, HashSet<PartitionKey>>,

    active_docs: HashMap<DocumentId, ActiveDocSyncState>,
    pending_docs: HashMap<DocumentId, PendingDocSyncState>,
    synced_docs: HashMap<DocumentId, SyncedDocSyncState>,

    active_blobs: HashMap<String, ActiveBlobSyncState>,
    pending_blobs: HashMap<String, PendingBlobSyncState>,
    synced_blobs: HashMap<String, SyncedBlobSyncState>,

    msg_tx: mpsc::UnboundedSender<Msg>,
    events_tx: tokio::sync::broadcast::Sender<FullSyncEvent>,
    max_active_sync_workers: usize,

    known_peer_set: HashMap<ConnectionId, PeerSyncState>,
    conn_by_peer: HashMap<EndpointId, ConnectionId>,
}

#[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum PartitionKey {
    FullSync,
    DocBlobsFullSync,
    // Docs(Uuid),
    // Blobs(Uuid),
}

struct Partition {
    is_active: bool,
    deets: ParitionDeets,
}

enum ParitionDeets {
    FullSync { peers: HashSet<EndpointId> },
    DocBlobsFullSync { peers: HashSet<EndpointId> },
    // Docs { include_set: HashSet<DocumentId> },
    // Blobs { include_set: HashMap<Uuid> },
}

struct PeerSyncState {
    endpoint_id: EndpointId,
    synced_docs: HashSet<DocumentId>,
    partitions: HashSet<PartitionKey>,
}

#[derive(Clone, Copy)]
pub(super) struct RetryState {
    pub attempt_no: usize,
    pub last_backoff: Duration,
    pub last_attempt_at: std::time::Instant,
}

struct ActiveDocSyncState {
    latest_heads: ChangeHashSet,
    stop_token: doc_worker_impl::DocSyncWorkerStopToken,
}

#[derive(Debug, Clone)]
struct PendingDocSyncState {
    attempt_no: usize,
    last_backoff: Duration,
    last_attempt_at: std::time::Instant,
    due_at: std::time::Instant,
}

struct SyncedDocSyncState {
    latest_heads: ChangeHashSet,
    synced_at: std::time::Instant,
}

struct ActiveBlobSyncState {
    stop_token: blob_worker_impl::BlobSyncWorkerStopToken,
}

#[derive(Debug, Clone)]
struct PendingBlobSyncState {
    attempt_no: usize,
    last_backoff: Duration,
    last_attempt_at: std::time::Instant,
    due_at: std::time::Instant,
}

struct SyncedBlobSyncState {
    synced_at: std::time::Instant,
    last_peer: Option<EndpointId>,
}

impl Worker {
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
            } => {
                let old_conn_id = self.conn_by_peer.get(&endpoint_id).copied();
                let old_state = self
                    .known_peer_set
                    .remove(&conn_id)
                    .or_else(|| old_conn_id.and_then(|id| self.known_peer_set.remove(&id)));
                let new_parts: Vec<_> = if let Some(old) = old_state {
                    for part_key in old.partitions.difference(&partitions) {
                        self.remove_peer_from_part(*part_key, endpoint_id).await?;
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
                        synced_docs: default(),
                    },
                );
                self.conn_by_peer.insert(endpoint_id, conn_id);
                for part_key in new_parts {
                    self.add_peer_to_part(part_key, endpoint_id).await?;
                }
                resp.send(())
                    .inspect_err(|_| warn!("called dropped before finish"))
                    .ok();
            }
            Msg::DelPeer { endpoint_id, resp } => {
                if let Some(conn_id) = self.conn_by_peer.remove(&endpoint_id) {
                    if let Some(state) = self.known_peer_set.remove(&conn_id) {
                        for part_key in state.partitions {
                            self.remove_peer_from_part(part_key, endpoint_id).await?;
                        }
                    }
                }
                resp.send(())
                    .inspect_err(|_| warn!("called dropped before finish"))
                    .ok();
            }
            Msg::DocHeadsUpdated { doc_id, heads } => {
                self.handle_doc_heads_change(doc_id, heads).await?;
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
        }
        eyre::Ok(())
    }



    async fn batch_refresh_paritions(&mut self) -> Res<()> {
        let part_keys: Vec<_> = self.partitions_to_refresh.drain().collect();
        for part_key in part_keys {
            let (activate, is_active, deets_kind) = {
                let part = self
                    .partitions
                    .get(&part_key)
                    .ok_or_eyre("parition not found")?;
                let activate = match &part.deets {
                    ParitionDeets::FullSync { peers } => !peers.is_empty(),
                    ParitionDeets::DocBlobsFullSync { peers } => !peers.is_empty(),
                };
                let deets_kind = match &part.deets {
                    ParitionDeets::FullSync { .. } => PartitionKey::FullSync,
                    ParitionDeets::DocBlobsFullSync { .. } => PartitionKey::DocBlobsFullSync,
                };
                (activate, part.is_active, deets_kind)
            };
            self.partitions
                .get_mut(&part_key)
                .expect("partition should exist")
                .is_active = activate;
            if activate == is_active {
                continue;
            }
            match deets_kind {
                PartitionKey::FullSync => {
                    if activate {
                        self.invalidate_synced_docs_to_pending_for_partition(part_key);
                    }
                    for (doc_id, parts) in &self.known_doc_set {
                        if !parts.contains(&PartitionKey::FullSync) {
                            continue;
                        }
                        if self.doc_required_by_any_active_partition(doc_id) {
                            if !self.active_docs.contains_key(doc_id)
                                && !self.pending_docs.contains_key(doc_id)
                                && !self.synced_docs.contains_key(doc_id)
                            {
                                self.docs_to_boot.insert(doc_id.clone());
                            }
                        } else if self.active_docs.contains_key(doc_id)
                            || self.pending_docs.contains_key(doc_id)
                            || self.synced_docs.contains_key(doc_id)
                        {
                            self.docs_to_stop.insert(doc_id.clone());
                        }
                    }
                }
                PartitionKey::DocBlobsFullSync => {
                    self.refresh_doc_blobs_workers().await?;
                }
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
            .get_mut(&part_key)
            .ok_or_eyre("parition not found")?;
        match &mut part.deets {
            ParitionDeets::FullSync { peers } => {
                peers.insert(endpoint_id);
                self.docs_to_boot
                    .extend(self.pending_docs.keys().cloned());
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
            .get_mut(&part_key)
            .ok_or_eyre("parition not found")?;
        match &mut part.deets {
            ParitionDeets::FullSync { peers } => {
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
                if pending.due_at <= now
                    && !self.active_docs.contains_key(doc_id)
                    && !self.synced_docs.contains_key(doc_id)
                {
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

        let due_blobs: Vec<_> = self
            .pending_blobs
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
            .take(budget)
            .collect();
        self.blobs_to_boot.extend(due_blobs);
    }

}

// Docs related methods
impl Worker {
    async fn handle_drawer_event(&mut self, evt: &DrawerEvent) -> Res<()> {
        match evt {
            DrawerEvent::DocUpdated { id, .. } => {
                let parsed: DocumentId = id.parse().wrap_err("invalid document id")?;
                self.invalidate_doc_to_pending(&parsed);
            }
            DrawerEvent::ListChanged { .. } => {}
            DrawerEvent::DocAdded { id, .. } => {
                let parsed: DocumentId = id.parse().wrap_err("invalid document id")?;
                self.add_doc(parsed).await?;
            }
            DrawerEvent::DocDeleted { id, .. } => {
                let parsed: DocumentId = id.parse().wrap_err("invalid document id")?;
                self.remove_doc(parsed).await?;
            }
        }
        Ok(())
    }

    async fn batch_boot_docs(&mut self) -> Res<()> {
        let docs_part_active = self
            .partitions
            .get(&PartitionKey::FullSync)
            .is_some_and(|part| part.is_active);
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
            if self.active_docs.contains_key(&doc_id) || self.synced_docs.contains_key(&doc_id) {
                continue;
            }
            let prior_pending = self.pending_docs.get(&doc_id).cloned();
            if let Some(handle) = self.acx.repo().find(doc_id.clone()).await? {
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
                let active = self
                    .boot_doc_sync_worker(doc_id.clone(), handle, retry)
                    .await?;
                self.pending_docs.remove(&doc_id);
                self.active_docs.insert(doc_id, active);
                budget = budget.saturating_sub(1);
            } else {
                let prior = prior_pending.unwrap_or(PendingDocSyncState {
                    attempt_no: 0,
                    last_backoff: Duration::from_millis(0),
                    last_attempt_at: std::time::Instant::now(),
                    due_at: std::time::Instant::now(),
                });
                let delay = next_backoff_delay(prior.last_backoff, Duration::from_millis(500));
                let pending = PendingDocSyncState {
                    attempt_no: prior.attempt_no + 1,
                    last_backoff: delay,
                    last_attempt_at: std::time::Instant::now(),
                    due_at: std::time::Instant::now() + delay,
                };
                self.pending_docs.insert(doc_id.clone(), pending);
            }
        }
        self.docs_to_boot = double;
        Ok(())
    }

    async fn boot_doc_sync_worker(
        &self,
        doc_id: DocumentId,
        handle: samod::DocHandle,
        retry: RetryState,
    ) -> Res<ActiveDocSyncState> {
        let latest_heads = ChangeHashSet(handle.with_document(|doc| doc.get_heads()).into());
        let (broker_handle, broker_stop_token) =
            self.acx.change_manager().add_doc(handle.clone()).await?;

        let cancel_token = self.cancel_token.child_token();
        let stop_token = doc_worker_impl::spawn_doc_sync_worker(
            doc_id.clone(),
            handle,
            broker_handle,
            broker_stop_token,
            cancel_token.clone(),
            self.msg_tx.clone(),
            retry,
        )
        .await?;
        Ok(ActiveDocSyncState {
            latest_heads,
            stop_token,
        })
    }
    async fn add_doc(&mut self, doc_id: DocumentId) -> Res<()> {
        let mut parts_to_add = vec![];
        for (part_key, part) in &self.partitions {
            match &part.deets {
                ParitionDeets::FullSync { .. } => {
                    parts_to_add.push(*part_key);
                }
                ParitionDeets::DocBlobsFullSync { .. } => {}
            }
        }
        let boot_doc = !parts_to_add.is_empty();
        self.known_doc_set.insert(doc_id.clone(), default());
        self.add_doc_to_paritions(doc_id.clone(), parts_to_add.into_iter())
            .await?;
        if boot_doc {
            self.docs_to_boot.insert(doc_id.clone());
        }
        Ok(())
    }

    async fn add_doc_to_paritions(
        &mut self,
        doc_id: DocumentId,
        parts: impl Iterator<Item = PartitionKey>,
    ) -> Res<()> {
        let doc_state = self
            .known_doc_set
            .get_mut(&doc_id)
            .ok_or_eyre("doc not found")?;
        doc_state.extend(parts);
        Ok(())
    }

    async fn batch_stop_docs(&mut self) -> Res<()> {
        use futures::StreamExt;
        use futures_buffered::BufferedStreamExt;

        let stopped_doc_ids: Vec<_> = self.docs_to_stop.drain().collect();
        let mut stop_futs = vec![];

        for doc_id in &stopped_doc_ids {
            if let Some(active) = self.active_docs.remove(doc_id) {
                stop_futs.push(active.stop_token.stop());
            }
            self.pending_docs.remove(doc_id);
            self.synced_docs.remove(doc_id);
        }

        futures::stream::iter(stop_futs)
            .buffered_unordered(16)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Res<Vec<_>>>()?;

        for doc_id in stopped_doc_ids {
            for peer_state in self.known_peer_set.values_mut() {
                peer_state.synced_docs.remove(&doc_id);
            }
        }
        Ok(())
    }
    async fn handle_doc_peer_state_change(
        &mut self,
        doc_id: DocumentId,
        diff: DocPeerStateView,
    ) -> Res<()> {
        let local_heads = if let Some(active) = self.active_docs.get(&doc_id) {
            &active.latest_heads
        } else if let Some(synced) = self.synced_docs.get(&doc_id) {
            &synced.latest_heads
        } else {
            return Ok(());
        };
        let fullsync_target_count = self.known_doc_set
            .iter()
            .filter(|(_, parts)| parts.contains(&PartitionKey::FullSync))
            .count();
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

            if they_have_our_changes
                && we_have_their_changes
                && peer_state.synced_docs.insert(doc_id.clone())
            {
                self.events_tx
                    .send(FullSyncEvent::DocSyncedWithPeer {
                        endpoint_id: peer_state.endpoint_id,
                        doc_id: doc_id.clone(),
                    })
                    .inspect_err(|_| warn!("full sync event receiver dropped"))
                    .ok();
            }
            if peer_state.synced_docs.len() == fullsync_target_count {
                self.events_tx
                    .send(FullSyncEvent::PeerFullSynced {
                        endpoint_id: peer_state.endpoint_id,
                        doc_count: peer_state.synced_docs.len(),
                    })
                    .inspect_err(|_| warn!("full sync event receiver dropped"))
                    .ok();
            }
        }
        if self.active_docs.contains_key(&doc_id)
            && self.is_doc_fully_synced_for_fullsync_peers(&doc_id)
        {

            let Some(active) = self.active_docs.remove(&doc_id) else {
                return Ok(());
            };
            let latest_heads = active.latest_heads.clone();
            active.stop_token.stop().await?;
            self.pending_docs.remove(&doc_id);
            self.synced_docs.insert(
                doc_id.clone(),
                SyncedDocSyncState {
                    latest_heads,
                    synced_at: std::time::Instant::now(),
                },
            );
        }
        Ok(())
    }

    async fn remove_doc(&mut self, doc_id: DocumentId) -> Res<()> {
        self.known_doc_set.remove(&doc_id);
        self.docs_to_stop.insert(doc_id);
        Ok(())
    }

    async fn handle_doc_heads_change(
        &mut self,
        doc_id: DocumentId,
        heads: ChangeHashSet,
    ) -> Res<()> {
        if let Some(active) = self.active_docs.get_mut(&doc_id) {
            active.latest_heads = heads;
        } else {
            self.invalidate_doc_to_pending(&doc_id);
        }
        for peer_state in self.known_peer_set.values_mut() {
            if peer_state.synced_docs.remove(&doc_id) {
                self.events_tx
                    .send(FullSyncEvent::StalePeer {
                        endpoint_id: peer_state.endpoint_id,
                    })
                    .inspect_err(|_| warn!("full sync event receiver dropped"))
                    .ok();
            }
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
        let active = self
            .active_docs
            .remove(&doc_id)
            .expect("doc backoff requested by non-active worker");
        active.stop_token.stop().await?;
        let now = std::time::Instant::now();
        let delay = delay.min(Duration::from_secs(600));
        let backoff = next_backoff_delay(previous_backoff, delay);
        let pending = PendingDocSyncState {
            attempt_no: previous_attempt_no + 1,
            last_backoff: backoff,
            last_attempt_at: now,
            due_at: now + backoff,
        };
        self.synced_docs.remove(&doc_id);
        self.pending_docs.insert(doc_id, pending);
        Ok(())
    }

    fn is_doc_fully_synced_for_fullsync_peers(&self, doc_id: &DocumentId) -> bool {
        let mut has_peer = false;
        for peer in self.known_peer_set.values() {
            if !peer.partitions.contains(&PartitionKey::FullSync) {
                continue;
            }
            has_peer = true;
            if !peer.synced_docs.contains(doc_id) {
                return false;
            }
        }
        has_peer
    }

    fn doc_required_by_any_active_partition(&self, doc_id: &DocumentId) -> bool {
        let Some(parts) = self.known_doc_set.get(doc_id) else {
            return false;
        };
        parts.iter().any(|part_key| {
            self.partitions
                .get(part_key)
                .is_some_and(|partition| partition.is_active)
        })
    }

    fn invalidate_doc_to_pending(&mut self, doc_id: &DocumentId) {
        if !self.known_doc_set.contains_key(doc_id) {
            return;
        }
        if !self.doc_required_by_any_active_partition(doc_id) {
            return;
        }
        if self.pending_docs.contains_key(doc_id) {
            return;
        }
        self.synced_docs.remove(doc_id);
        for peer_state in self.known_peer_set.values_mut() {
            peer_state.synced_docs.remove(doc_id);
        }
        self.pending_docs.insert(
            doc_id.clone(),
            PendingDocSyncState {
                attempt_no: 0,
                last_backoff: Duration::from_millis(0),
                last_attempt_at: std::time::Instant::now(),
                due_at: std::time::Instant::now(),
            },
        );
    }

    fn invalidate_synced_docs_to_pending_for_partition(&mut self, part_key: PartitionKey) {
        let doc_ids: Vec<_> = self
            .known_doc_set
            .iter()
            .filter_map(|(doc_id, parts)| {
                if parts.contains(&part_key) && self.synced_docs.contains_key(doc_id) {
                    Some(doc_id.clone())
                } else {
                    None
                }
            })
            .collect();
        for doc_id in doc_ids {
            self.invalidate_doc_to_pending(&doc_id);
        }
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
                let hashes = self.doc_blobs_index_repo.list_hashes_for_doc(doc_id).await?;
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
            let hashes: Vec<_> = self
                .active_blobs
                .keys()
                .chain(self.pending_blobs.keys())
                .chain(self.synced_blobs.keys())
                .cloned()
                .collect();
            for hash in hashes {
                if self.blob_required_by_any_active_partition(&hash) {
                    continue;
                }
                if let Some(active) = self.active_blobs.remove(&hash) {
                    active.stop_token.stop().await?;
                }
                self.pending_blobs.remove(&hash);
                self.synced_blobs.remove(&hash);
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
                self.synced_blobs.insert(hash, SyncedBlobSyncState {
                    synced_at: std::time::Instant::now(),
                    last_peer: None,
                });
            } else {
                let peers = self.current_blob_partition_peers();
                if peers.is_empty() {
                    let prior = prior_pending.unwrap_or(PendingBlobSyncState {
                        attempt_no: 0,
                        last_backoff: Duration::from_millis(0),
                        last_attempt_at: std::time::Instant::now(),
                        due_at: std::time::Instant::now(),
                    });
                    let delay = next_backoff_delay(prior.last_backoff, Duration::from_millis(500));
                    self.pending_blobs.insert(hash.clone(), PendingBlobSyncState {
                        attempt_no: prior.attempt_no + 1,
                        last_backoff: delay,
                        last_attempt_at: std::time::Instant::now(),
                        due_at: std::time::Instant::now() + delay,
                    });
                } else {
                    let now = std::time::Instant::now();
                    let retry = RetryState {
                        attempt_no: prior_pending
                            .as_ref()
                            .map_or(0, |prior| prior.attempt_no),
                        last_backoff: prior_pending
                            .as_ref()
                            .map_or(Duration::from_millis(0), |prior| prior.last_backoff),
                        last_attempt_at: prior_pending
                            .as_ref()
                            .map_or(now, |prior| prior.last_attempt_at),
                    };
                    let active =
                        self.boot_blob_sync_worker(hash.clone(), peers, retry)?;
                    self.pending_blobs.remove(&hash);
                    self.active_blobs.insert(hash, active);
                    budget = budget.saturating_sub(1);
                }
            }
        }
        self.blobs_to_boot = double;
        Ok(())
    }

    fn boot_blob_sync_worker(
        &self,
        hash: String,
        peers: Vec<EndpointId>,
        retry: RetryState,
    ) -> Res<ActiveBlobSyncState> {
        let cancel_token = self.cancel_token.child_token();
        let stop_token = blob_worker_impl::spawn_blob_sync_worker(
            hash,
            peers,
            cancel_token.clone(),
            self.msg_tx.clone(),
            Arc::clone(&self.blobs_repo),
            self.iroh_endpoint.clone(),
            retry,
        )?;

        Ok(ActiveBlobSyncState {
            stop_token,
        })
    }

    fn current_blob_partition_peers(&self) -> Vec<EndpointId> {
        self.partitions
            .get(&PartitionKey::DocBlobsFullSync)
            .and_then(|part| match &part.deets {
                ParitionDeets::DocBlobsFullSync { peers } => Some(peers.iter().cloned().collect()),
                _ => None,
            })
            .unwrap_or_default()
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
                synced_at: std::time::Instant::now(),
                last_peer: endpoint_id,
            },
        );
        self.events_tx
            .send(FullSyncEvent::BlobSynced {
                hash: sync_hash,
                endpoint_id,
            })
            .inspect_err(|_| warn!("full sync event receiver dropped"))
            .ok();
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
        self.events_tx
            .send(FullSyncEvent::BlobSyncBackoff {
                hash: hash.clone(),
                delay,
                attempt_no,
            })
            .inspect_err(|_| warn!("full sync event receiver dropped"))
            .ok();
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
