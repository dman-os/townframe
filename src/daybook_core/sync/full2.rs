use crate::interlude::*;

use iroh::EndpointId;
use samod::ConnectionId;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::drawer::{DrawerEvent, DrawerRepo};
use crate::repo::RepoCtx;

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
    StalePeer {
        endpoint_id: EndpointId,
    },
}

pub struct FullSyncSnapshot {
    known_doc_count: usize,
    known_peers: HashMap<EndpointId, KnownPeer>,
}

pub struct KnownPeer {
    synced_docs_count: usize,
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
    AddFullSyncPeer {
        endpoint_id: EndpointId,
        resp: tokio::sync::oneshot::Sender<()>,
    },
    BootDocSyncBackoff {
        doc_id: DocumentId,
    },
    DocHeadsUpdated {
        doc_id: DocumentId,
        heads: ChangeHashSet,
    },
    DocPeerStateViewUpdated {
        doc_id: DocumentId,
        diff: DocPeerStateView,
    },
    GetSnapshot {
        resp: tokio::sync::oneshot::Sender<FullSyncSnapshot>,
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
            partitions: [PartitionKey::FullSync].into(),
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

    pub async fn add_full_sync_peer(&self, endpoint_id: EndpointId) -> Res<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.msg_tx
            .send(Msg::AddFullSyncPeer {
                resp: tx,
                endpoint_id,
            })
            .wrap_err("FullSyncWorker is dead")?;
        rx.await.wrap_err(ERROR_CHANNEL)
    }

    pub async fn query_snapshot(&self) -> Res<FullSyncSnapshot> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.msg_tx
            .send(Msg::GetSnapshot { resp: tx })
            .wrap_err("FullSyncWorker is dead")?;
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
        ]
        .into(),

        synced_docs: default(),
        full_sync_peers: default(),
        known_peer_set: default(),
        conn_by_peer: default(),
        known_doc_set: default(),
        docs_to_boot: default(),
        docs_to_stop: default(),
        partitions_to_refresh: default(),
    };

    let drawer_rx = drawer_repo.subscribe(crate::repos::SubscribeOpts { capacity: 1024 });
    let (_drawer_heads, known_docs) = drawer_repo.list_just_ids().await?;
    worker.add_doc(rcx.doc_app.document_id().clone()).await?;
    worker.add_doc(rcx.doc_drawer.document_id().clone()).await?;
    for doc_id in known_docs {
        let doc_id: DocumentId = doc_id
            .parse()
            .wrap_err("unable to parse doc_id from drawer")?;
        worker.add_doc(doc_id.clone()).await?;
    }

    // let (peer_infos, mut peer_updates) = rcx.acx.repo().connected_peers();
    // let (peer_observer, observer_stop) = acx.spawn_peer_sync_observer();
    let fut = {
        let cancel_token = cancel_token.clone();
        // let peer_observer = observer.subscribe();
        async move {
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
                }
            }
            worker
                .docs_to_stop
                .extend(worker.synced_docs.keys().cloned());
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

    partitions: HashMap<PartitionKey, Partition>,

    docs_to_boot: HashSet<DocumentId>,
    docs_to_stop: HashSet<DocumentId>,
    partitions_to_refresh: HashSet<PartitionKey>,

    known_doc_set: HashMap<DocumentId, HashSet<PartitionKey>>,
    synced_docs: HashMap<DocumentId, DocSyncState>,
    msg_tx: mpsc::UnboundedSender<Msg>,
    events_tx: tokio::sync::broadcast::Sender<FullSyncEvent>,

    full_sync_peers: HashSet<EndpointId>,
    known_peer_set: HashMap<ConnectionId, PeerSyncState>,
    conn_by_peer: HashMap<EndpointId, ConnectionId>,
}

#[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum PartitionKey {
    FullSync,
    // Docs(Uuid),
    // Blobs(Uuid),
}

struct Partition {
    is_active: bool,
    deets: ParitionDeets,
}

enum ParitionDeets {
    FullSync { peers: HashSet<EndpointId> },
    // Docs { include_set: HashSet<DocumentId> },
    // Blobs { include_set: HashMap<Uuid> },
}

struct PeerSyncState {
    endpoint_id: EndpointId,
    synced_docs: HashSet<DocumentId>,
    partitions: HashSet<PartitionKey>,
}

struct DocSyncState {
    // doc_id: Arc<str>,
    // partitions: HashSet<PartitionKey>,
    deets: DocSyncStateDeets,
}

enum DocSyncStateDeets {
    Active(ActiveDocSyncState),
    Pending(PendingDocSyncState),
}

struct ActiveDocSyncState {
    latest_heads: ChangeHashSet,
    cancel_token: CancellationToken,
    join_handle: tokio::task::JoinHandle<()>,
    broker_stop_token: Arc<am_utils_rs::changes::DocChangeBrokerStopToken>,
}

impl ActiveDocSyncState {
    async fn stop(self) -> Res<()> {
        self.cancel_token.cancel();
        utils_rs::wait_on_handle_with_timeout(self.join_handle, Duration::from_secs(2)).await?;
        if let Ok(token) = Arc::try_unwrap(self.broker_stop_token) {
            token.stop().await?;
        }
        Ok(())
    }
}

struct ConnectionDeets {
    conn_id: ConnectionId,
}

#[derive(Debug, Clone)]
struct PendingDocSyncState {
    attempt_no: usize,
    last_backoff: Duration,
}

impl Worker {
    async fn handle_drawer_event(&mut self, evt: &DrawerEvent) -> Res<()> {
        match evt {
            DrawerEvent::DocUpdated { .. } | DrawerEvent::ListChanged { .. } => {
                // no-op
            }
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

    async fn handle_msg(&mut self, msg: Msg) -> Res<()> {
        match msg {
            Msg::AddFullSyncPeer { endpoint_id, resp } => {
                self.full_sync_peers.insert(endpoint_id);
                resp.send(())
                    .inspect_err(|_| warn!("called dropped before finish"))
                    .ok();
            }
            Msg::SetPeer {
                endpoint_id,
                resp,
                partitions,
                conn_id,
            } => {
                let new_parts: Vec<_> = if let Some(old) = self.known_peer_set.remove(&conn_id) {
                    for part_key in old.partitions.difference(&partitions) {
                        self.remove_peer_from_part(part_key.clone(), endpoint_id)
                            .await?;
                    }
                    partitions.difference(&old.partitions).cloned().collect()
                } else {
                    default()
                };
                self.known_peer_set.insert(
                    conn_id,
                    PeerSyncState {
                        partitions,
                        endpoint_id: endpoint_id.clone(),
                        synced_docs: default(),
                    },
                );
                self.conn_by_peer.insert(endpoint_id.clone(), conn_id);
                for part_key in new_parts {
                    self.add_peer_to_part(part_key, endpoint_id.clone()).await?;
                }
                resp.send(())
                    .inspect_err(|_| warn!("called dropped before finish"))
                    .ok();
            }
            Msg::DelPeer { endpoint_id, resp } => {
                if let Some(conn_id) = self.conn_by_peer.remove(&endpoint_id) {
                    self.known_peer_set.remove(&conn_id);
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
            Msg::BootDocSyncBackoff { doc_id } => {
                self.handle_doc_boot_backoff(doc_id).await?;
            }
            Msg::GetSnapshot { resp } => {
                let snap = FullSyncSnapshot {
                    known_doc_count: self.synced_docs.len(),
                    known_peers: self
                        .known_peer_set
                        .iter()
                        .map(|(_conn_id, state)| {
                            (
                                state.endpoint_id.clone(),
                                KnownPeer {
                                    synced_docs_count: state.synced_docs.len(),
                                },
                            )
                        })
                        .collect(),
                };
                resp.send(snap)
                    .inspect_err(|_| warn!("called dropped before finish"))
                    .ok();
            }
        }
        eyre::Ok(())
    }

    async fn batch_boot_docs(&mut self) -> Res<()> {
        let mut double = std::mem::replace(&mut self.docs_to_boot, default());
        for doc_id in double.drain() {
            let deets = if let Some(handle) = self.acx.repo().find(doc_id.clone()).await? {
                let active = self.boot_doc_sync_worker(doc_id.clone(), handle).await?;
                DocSyncStateDeets::Active(active)
            } else {
                DocSyncStateDeets::Pending(PendingDocSyncState {
                    attempt_no: default(),
                    last_backoff: default(),
                })
            };

            let old = self.synced_docs.insert(doc_id, DocSyncState { deets });
            assert!(old.is_none(), "fishy");
        }
        std::mem::replace(&mut self.docs_to_boot, double);
        Ok(())
    }

    async fn batch_stop_docs(&mut self) -> Res<()> {
        use futures::StreamExt;
        use futures_buffered::BufferedStreamExt;

        futures::stream::iter(
            self.docs_to_stop
                .drain()
                .filter_map(|doc_id| self.synced_docs.remove(&doc_id))
                .filter_map(|doc| match doc.deets {
                    DocSyncStateDeets::Active(active) => Some(active.stop()),
                    DocSyncStateDeets::Pending(_) => None,
                }),
        )
        .buffered_unordered(16)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Res<Vec<_>>>()?;
        Ok(())
    }

    async fn batch_refresh_paritions(&mut self) -> Res<()> {
        for part_key in self.partitions_to_refresh.drain() {
            let part = self
                .partitions
                .get_mut(&part_key)
                .ok_or_eyre("parition not found")?;
            let activate = match &part.deets {
                ParitionDeets::FullSync { peers } => !peers.is_empty(),
            };
            if activate == part.is_active {
                continue;
            }
            if activate {
                match &part.deets {
                    ParitionDeets::FullSync { .. } => {
                        for (doc_id, parts) in &self.known_doc_set {
                            if !parts.contains(&part_key) {
                                continue;
                            }
                            if !self.synced_docs.contains_key(&doc_id) {
                                self.docs_to_boot.insert(doc_id.clone());
                            }
                        }
                    }
                }
            }
            if !activate {
                match &part.deets {
                    ParitionDeets::FullSync { .. } => {
                        for (doc_id, parts) in &self.known_doc_set {
                            if !parts.contains(&part_key) {
                                continue;
                            }
                            if self.synced_docs.contains_key(&doc_id) {
                                self.docs_to_stop.insert(doc_id.clone());
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn add_doc(&mut self, doc_id: DocumentId) -> Res<()> {
        let mut parts_to_add = vec![];
        for (part_key, part) in &self.partitions {
            match &part.deets {
                ParitionDeets::FullSync { .. } => {
                    parts_to_add.push(part_key.clone());
                }
            }
        }
        let boot_doc = !parts_to_add.is_empty();
        self.known_doc_set.insert(doc_id.clone(), default());
        self.add_doc_to_paritions(doc_id.clone(), parts_to_add.into_iter());
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

    async fn boot_doc_sync_worker(
        &self,
        doc_id: DocumentId,
        handle: samod::DocHandle,
    ) -> Res<ActiveDocSyncState> {
        let latest_heads = ChangeHashSet(handle.with_document(|doc| doc.get_heads()).into());
        let (broker_handle, broker_stop_token) =
            self.acx.change_manager().add_doc(handle.clone()).await?;

        let cancel_token = self.cancel_token.child_token();
        let fut = {
            let cancel_token = cancel_token.clone();
            let doc_id = doc_id.clone();
            let msg_tx = self.msg_tx.clone();

            let mut heads_listener = broker_handle.get_head_listener().await?;

            let (peer_state, mut state_stream) = handle.peers();

            msg_tx
                .send(Msg::DocPeerStateViewUpdated {
                    doc_id: doc_id.clone(),
                    diff: peer_state,
                })
                .expect("impossible");
            async move {
                loop {
                    tokio::select! {
                        biased;
                        _ = cancel_token.cancelled() => {
                            debug!("cancel token lit");
                            break;
                        }
                        val = heads_listener.change_rx().recv() => {
                            let Some(heads) = val else {
                                eyre::bail!("DocChangeBroker was removed from repo, weird!");
                            };
                            msg_tx.send(Msg::DocHeadsUpdated{
                                doc_id: doc_id.clone(),
                                heads: ChangeHashSet(heads)
                            }).expect("FullSyncWorker went down without cleaning documents");
                        }
                        val = state_stream.next() => {
                            let Some(diff) = val else {
                                eyre::bail!("DocHandle was removed from repo, weird!");
                            };
                            msg_tx.send(Msg::DocPeerStateViewUpdated{
                                doc_id: doc_id.clone(),
                                diff,
                            }).expect("FullSyncWorker went down without cleaning documents");
                        }
                    }
                }
                eyre::Ok(())
            }
        };
        let join_handle = tokio::spawn(
            async move { fut.await.unwrap() }
                .instrument(tracing::info_span!("DocHandle peer changes reduce task")),
        );
        Ok(ActiveDocSyncState {
            latest_heads,
            cancel_token,
            join_handle,
            broker_stop_token,
        })
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
        }
        self.partitions_to_refresh.insert(part_key);
        Ok(())
    }

    async fn handle_doc_peer_state_change(
        &mut self,
        doc_id: DocumentId,
        diff: DocPeerStateView,
    ) -> Res<()> {
        let doc_state = self
            .synced_docs
            .get(&doc_id)
            .expect("peer state diff for unkown doc");
        for (conn_id, diff) in diff {
            let Some(peer_state) = self.known_peer_set.get_mut(&conn_id) else {
                warn!(?conn_id, "unkown connection for FullSyncWorker");
                continue;
            };
            let they_have_our_changes = diff
                .shared_heads
                .as_ref()
                .map(|heads| &heads[..] == &doc_state.latest_heads[..])
                .unwrap_or_default();
            let we_have_their_changes =
                diff.their_heads.is_some() && diff.their_heads == diff.shared_heads;

            if they_have_our_changes
                && we_have_their_changes
                && peer_state.synced_docs.contains(&doc_id)
            {
                peer_state.synced_docs.insert(doc_id.clone());
                self.events_tx.send(FullSyncEvent::DocSyncedWithPeer {
                    endpoint_id: peer_state.endpoint_id.clone(),
                    doc_id: doc_id.clone(),
                });
            }
            if peer_state.synced_docs.len() == self.synced_docs.len() {
                self.events_tx.send(FullSyncEvent::PeerFullSynced {
                    endpoint_id: peer_state.endpoint_id.clone(),
                    doc_count: peer_state.synced_docs.len(),
                });
            }
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
        let Some(state) = self.synced_docs.get_mut(&doc_id) else {
            // FIXME: turn to error if this can't happen
            warn!(?doc_id, "doc was removed by the time of heads update");
            return Ok(());
        };
        let active = match &mut state.deets {
            DocSyncStateDeets::Pending(_) => unreachable!(),
            DocSyncStateDeets::Active(active) => active,
        };
        active.latest_heads = heads;
        for (_, peer_state) in &mut self.known_peer_set {
            peer_state.synced_docs.remove(&doc_id);
            self.events_tx.send(FullSyncEvent::StalePeer {
                endpoint_id: peer_state.endpoint_id.clone(),
            });
        }
        Ok(())
    }

    async fn handle_doc_boot_backoff(&mut self, doc_id: DocumentId) -> Res<()> {
        if !self.known_doc_set.contains_key(&doc_id) {
            return Ok(());
        };
        let backoff = match self.synced_docs.get_mut(&doc_id) {
            None => {
                // must have bee removed from sync set
                return Ok(());
            }
            Some(state) => match &state.deets {
                DocSyncStateDeets::Active(_) => {
                    return Ok(());
                }
                DocSyncStateDeets::Pending(pending) => pending.clone(),
            },
        };
        if let Some(handle) = self.acx.repo().find(doc_id.clone()).await? {
            let active = self.boot_doc_sync_worker(doc_id.clone(), handle).await?;
            let Some(state) = self.synced_docs.get_mut(&doc_id) else {
                return Ok(());
            };
            state.deets = DocSyncStateDeets::Active(active);

            return Ok(());
        };
        warn!(?backoff, "added doc not found in repo, will retry");
        let delay = (backoff.last_backoff * 2)
            .max(Duration::from_millis(500))
            .min(Duration::from_secs(10));
        {
            let Some(state) = self.synced_docs.get_mut(&doc_id) else {
                return Ok(());
            };
            state.deets = DocSyncStateDeets::Pending(PendingDocSyncState {
                attempt_no: backoff.attempt_no + 1,
                last_backoff: delay,
            });
        }
        let msg_tx = self.msg_tx.clone();
        tokio::task::spawn(async move {
            tokio::time::sleep(delay).await;
            msg_tx
                .send(Msg::BootDocSyncBackoff {
                    doc_id: doc_id.clone(),
                })
                .inspect_err(|_| {
                    error!(?doc_id, "FullSyncWorker died before doc was enqued");
                })
                .ok();
        });
        Ok(())
    }
}
