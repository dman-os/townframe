use std::str::FromStr;

use crate::drawer::{DrawerEvent, DrawerRepo};
use crate::interlude::*;
use crate::repos::{RecvError, Repo, SubscribeOpts};

use tokio::sync;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub enum FullSyncEvent {
    DocSynced {
        endpoint_id: String,
        doc_id: DocumentId,
        pending_count: usize,
    },
}

pub struct FullSyncHandle {
    done_rx: sync::watch::Receiver<bool>,
    events_tx: sync::broadcast::Sender<Arc<[FullSyncEvent]>>,
}

impl FullSyncHandle {
    pub async fn wait_until_done(&mut self) -> Res<()> {
        while !*self.done_rx.borrow() {
            self.done_rx.changed().await.wrap_err(ERROR_CHANNEL)?;
        }
        Ok(())
    }

    pub fn subscribe(&self) -> sync::broadcast::Receiver<Arc<[FullSyncEvent]>> {
        self.events_tx.subscribe()
    }
}

pub struct FullSyncStopToken {
    cancel_token: CancellationToken,
    join_handle: JoinHandle<()>,
}

impl FullSyncStopToken {
    pub async fn stop(self) -> Res<()> {
        self.cancel_token.cancel();
        self.join_handle.await?;
        Ok(())
    }
}

pub fn spawn_full_sync_worker(
    drawer_repo: Arc<DrawerRepo>,
    acx: AmCtx,
    snapshot: am_utils_rs::peers::PeerSyncSnapshot,
    mut updates_rx: sync::broadcast::Receiver<Arc<[am_utils_rs::peers::DocPeerSyncUpdate]>>,
    target_endpoint_ids: HashSet<String>,
) -> (FullSyncHandle, FullSyncStopToken) {
    let cancel_token = CancellationToken::new();
    let (done_tx, done_rx) = sync::watch::channel(false);
    let (events_tx, _events_rx) = sync::broadcast::channel(64);

    let task_cancel = cancel_token.clone();
    let events_tx_worker = events_tx.clone();
    let drawer_listener = drawer_repo.subscribe(SubscribeOpts::new(256));
    let join_handle = tokio::spawn(async move {
        let mut worker = FullSyncWorker::new(target_endpoint_ids);
        worker.seed_connections(snapshot.connections);
        worker
            .refresh_drawer_docs(&drawer_repo, &acx)
            .await
            .unwrap();
        worker.seed_doc_states(snapshot.docs);

        if worker.is_done() {
            let _ = done_tx.send(true);
            return;
        }

        loop {
            tokio::select! {
                biased;
                _ = task_cancel.cancelled() => {
                    break;
                }
                recv = updates_rx.recv() => {
                    match recv {
                        Ok(batch) => {
                            let events = worker.handle_peer_updates(&batch);
                            if !events.is_empty() {
                                let _ = events_tx_worker.send(events.into());
                            }
                            if worker.is_done() {
                                let _ = done_tx.send(true);
                                break;
                            }
                        }
                        Err(sync::broadcast::error::RecvError::Lagged(skipped)) => {
                            warn!(?skipped, "full sync worker lagged");
                        }
                        Err(sync::broadcast::error::RecvError::Closed) => {
                            break;
                        }
                    }
                }
                recv = drawer_listener.recv_lossy_async() => {
                    match recv {
                        Ok(event) => {
                            worker.handle_drawer_event(&event, &acx).await.unwrap();
                            if worker.is_done() {
                                let _ = done_tx.send(true);
                                break;
                            }
                        }
                        Err(RecvError::Closed) => break,
                        Err(RecvError::Dropped { .. }) => unreachable!("recv_lossy_async should never return dropped"),
                    }
                }
            }
        }
    });

    (
        FullSyncHandle { done_rx, events_tx },
        FullSyncStopToken {
            cancel_token,
            join_handle,
        },
    )
}

struct FullSyncWorker {
    target_endpoint_ids: HashSet<String>,
    connected_endpoint_ids: HashSet<String>,
    target_peer_ids: HashSet<samod::PeerId>,
    peer_to_endpoint: HashMap<samod::PeerId, String>,
    per_connection_doc:
        HashMap<(samod::ConnectionId, DocumentId), Arc<am_utils_rs::peers::DocPeerSyncState>>,
    tracked_docs: HashSet<DocumentId>,
    pending: HashSet<(samod::PeerId, DocumentId)>,
}

impl FullSyncWorker {
    fn new(target_endpoint_ids: HashSet<String>) -> Self {
        Self {
            target_endpoint_ids,
            connected_endpoint_ids: HashSet::new(),
            target_peer_ids: HashSet::new(),
            peer_to_endpoint: HashMap::new(),
            per_connection_doc: HashMap::new(),
            tracked_docs: HashSet::new(),
            pending: HashSet::new(),
        }
    }

    fn seed_connections(&mut self, connections: Vec<samod::ConnectionInfo>) {
        for conn in connections {
            let samod::ConnectionState::Connected { their_peer_id } = conn.state else {
                continue;
            };
            self.register_peer(their_peer_id);
        }
    }

    fn seed_doc_states(&mut self, docs: Vec<Arc<am_utils_rs::peers::DocPeerSyncState>>) {
        for doc_state in docs {
            self.apply_upsert(doc_state, None);
        }
    }

    fn handle_peer_updates(
        &mut self,
        updates: &[am_utils_rs::peers::DocPeerSyncUpdate],
    ) -> Vec<FullSyncEvent> {
        let mut events = Vec::new();
        for update in updates {
            match update {
                am_utils_rs::peers::DocPeerSyncUpdate::Upsert(doc_state) => {
                    self.apply_upsert(Arc::clone(doc_state), Some(&mut events));
                }
                am_utils_rs::peers::DocPeerSyncUpdate::Removed {
                    connection_id,
                    doc_id,
                } => {
                    let key = (*connection_id, doc_id.clone());
                    if let Some(prev) = self.per_connection_doc.remove(&key) {
                        let Some(peer_id) =
                            prev.peer_id.as_ref().map(|peer_arc| (**peer_arc).clone())
                        else {
                            continue;
                        };
                        self.recompute_peer_doc(&peer_id, doc_id, Some(&mut events));
                    }
                }
            }
        }
        events
    }

    async fn handle_drawer_event(&mut self, event: &DrawerEvent, acx: &AmCtx) -> Res<()> {
        match event {
            DrawerEvent::ListChanged { .. } => {
                self.refresh_drawer_docs_from_list(acx, None).await?
            }
            DrawerEvent::DocAdded { id, .. } | DrawerEvent::DocUpdated { id, .. } => {
                self.refresh_drawer_docs_from_list(acx, Some(id)).await?
            }
            DrawerEvent::DocDeleted { id, .. } => {
                let doc_id = parse_drawer_doc_id(id)?;
                self.remove_tracked_doc(&doc_id);
            }
        }
        Ok(())
    }

    async fn refresh_drawer_docs(&mut self, drawer_repo: &DrawerRepo, acx: &AmCtx) -> Res<()> {
        let docs = drawer_repo.list().await?;
        let mut next = HashSet::new();
        for entry in docs {
            let doc_id = parse_drawer_doc_id(&entry.doc_id)?;
            ensure_doc_handle_exists(acx, &doc_id).await?;
            next.insert(doc_id);
        }

        let prev_docs = self.tracked_docs.clone();
        for doc_id in next.iter().cloned() {
            self.add_tracked_doc(doc_id);
        }
        for doc_id in prev_docs.difference(&next) {
            self.remove_tracked_doc(doc_id);
        }
        Ok(())
    }

    async fn refresh_drawer_docs_from_list(
        &mut self,
        acx: &AmCtx,
        doc_id: Option<&daybook_types::doc::DocId>,
    ) -> Res<()> {
        if let Some(doc_id) = doc_id {
            let am_id = parse_drawer_doc_id(doc_id)?;
            ensure_doc_handle_exists(acx, &am_id).await?;
            self.add_tracked_doc(am_id);
            return Ok(());
        }
        Ok(())
    }

    fn apply_upsert(
        &mut self,
        doc_state: Arc<am_utils_rs::peers::DocPeerSyncState>,
        mut events: Option<&mut Vec<FullSyncEvent>>,
    ) {
        let key = (doc_state.connection_id, doc_state.doc_id.clone());
        let prev = self.per_connection_doc.insert(key, Arc::clone(&doc_state));

        if let Some(peer_id) = doc_state
            .peer_id
            .as_ref()
            .map(|peer_arc| (**peer_arc).clone())
        {
            self.register_peer(peer_id.clone());
            if let Some(event_list) = events.as_mut() {
                self.recompute_peer_doc(&peer_id, &doc_state.doc_id, Some(event_list));
            } else {
                self.recompute_peer_doc(&peer_id, &doc_state.doc_id, None);
            }
        }

        if let Some(prev) = prev {
            if let Some(prev_peer) = prev.peer_id.as_ref().map(|peer_arc| (**peer_arc).clone()) {
                if let Some(event_list) = events {
                    self.recompute_peer_doc(&prev_peer, &prev.doc_id, Some(event_list));
                } else {
                    self.recompute_peer_doc(&prev_peer, &prev.doc_id, None);
                }
            }
        }
    }

    fn add_tracked_doc(&mut self, doc_id: DocumentId) {
        if !self.tracked_docs.insert(doc_id.clone()) {
            return;
        }
        for peer_id in self.target_peer_ids.clone() {
            self.recompute_peer_doc(&peer_id, &doc_id, None);
        }
    }

    fn remove_tracked_doc(&mut self, doc_id: &DocumentId) {
        if !self.tracked_docs.remove(doc_id) {
            return;
        }
        self.pending
            .retain(|(_, existing_doc_id)| existing_doc_id != doc_id);
    }

    fn recompute_peer_doc(
        &mut self,
        peer_id: &samod::PeerId,
        doc_id: &DocumentId,
        mut events: Option<&mut Vec<FullSyncEvent>>,
    ) {
        let key = (peer_id.clone(), doc_id.clone());

        if !self.target_peer_ids.contains(peer_id) || !self.tracked_docs.contains(doc_id) {
            self.pending.remove(&key);
            return;
        }

        let is_synced = self.per_connection_doc.values().any(|state| {
            state
                .peer_id
                .as_ref()
                .map(|peer_arc| **peer_arc == *peer_id)
                .unwrap_or(false)
                && state.doc_id == *doc_id
                && is_doc_synced(&state.state)
        });

        let was_pending = self.pending.contains(&key);
        if is_synced {
            self.pending.remove(&key);
            if was_pending {
                let endpoint_id = self
                    .peer_to_endpoint
                    .get(peer_id)
                    .expect("target peer must have endpoint mapping")
                    .clone();
                if let Some(events) = events.as_mut() {
                    events.push(FullSyncEvent::DocSynced {
                        endpoint_id,
                        doc_id: doc_id.clone(),
                        pending_count: self.pending.len(),
                    });
                }
            }
        } else {
            self.pending.insert(key);
        }
    }

    fn register_peer(&mut self, peer_id: samod::PeerId) {
        let endpoint_id = self
            .peer_to_endpoint
            .entry(peer_id.clone())
            .or_insert_with(|| {
                endpoint_id_from_peer_id(&peer_id)
                    .expect("peer id must always include endpoint id suffix")
            })
            .clone();
        if !self.target_endpoint_ids.contains(&endpoint_id) {
            return;
        }

        let is_new_target_peer = self.target_peer_ids.insert(peer_id.clone());
        self.connected_endpoint_ids.insert(endpoint_id);

        if is_new_target_peer {
            for doc_id in self.tracked_docs.clone() {
                self.recompute_peer_doc(&peer_id, &doc_id, None);
            }
        }
    }

    fn is_done(&self) -> bool {
        self.connected_endpoint_ids.len() == self.target_endpoint_ids.len()
            && self.pending.is_empty()
    }
}

fn parse_drawer_doc_id(doc_id: &daybook_types::doc::DocId) -> Res<DocumentId> {
    DocumentId::from_str(&doc_id.to_string())
        .wrap_err_with(|| format!("invalid drawer document id '{}'", doc_id))
}

async fn ensure_doc_handle_exists(acx: &AmCtx, doc_id: &DocumentId) -> Res<()> {
    let handle = acx.find_doc(doc_id).await?;
    if handle.is_none() {
        eyre::bail!("drawer references missing automerge doc {}", doc_id);
    }
    Ok(())
}

fn endpoint_id_from_peer_id(peer_id: &samod::PeerId) -> Option<String> {
    peer_id.to_string().rsplit('/').next().map(str::to_string)
}

fn is_doc_synced(state: &samod::PeerDocState) -> bool {
    let Some(shared) = &state.shared_heads else {
        return false;
    };
    let Some(their) = &state.their_heads else {
        return false;
    };
    shared == their
}
