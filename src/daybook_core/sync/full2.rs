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
    AddPeer {
        endpoint_id: EndpointId,
        conn_id: ConnectionId,
        resp: tokio::sync::oneshot::Sender<()>,
    },
    // OutgoingConn {
    //     endpoint_id: EndpointId,
    //     conn_id: samod::ConnectionId,
    //     resp: tokio::sync::oneshot::Sender<()>,
    // },
    RemovePeer {
        endpoint_id: EndpointId,
        resp: tokio::sync::oneshot::Sender<()>,
    },
    AddFullSyncPeer {
        endpoint_id: EndpointId,
        resp: tokio::sync::oneshot::Sender<()>,
    },
    DelayedAddDoc {
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
    pub async fn add_connection(&self, endpoint_id: EndpointId, conn_id: ConnectionId) -> Res<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let msg = Msg::AddPeer {
            resp: tx,
            conn_id,
            endpoint_id,
        };
        self.msg_tx.send(msg).wrap_err(ERROR_ACTOR)?;
        rx.await.wrap_err(ERROR_CHANNEL)
    }
    pub async fn remove_connection(&self, endpoint_id: EndpointId) -> Res<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let msg = Msg::RemovePeer {
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

        known_doc_set: default(),
        full_sync_peers: default(),
        known_peer_set: default(),
        delayed_add_docs: default(),
        conn_by_peer: default(),
    };

    let drawer_rx = drawer_repo.subscribe(crate::repos::SubscribeOpts { capacity: 1024 });
    let (_drawer_heads, known_docs) = drawer_repo.list_just_ids().await?;
    if !worker.add_doc(rcx.doc_app.document_id().clone()).await? {
        eyre::bail!("doc_app not found in repo");
    }
    if !worker.add_doc(rcx.doc_drawer.document_id().clone()).await? {
        eyre::bail!("doc_drawer not found in repo");
    }
    for doc_id in known_docs {
        let doc_id: DocumentId = doc_id
            .parse()
            .wrap_err("unable to parse doc_id from drawer")?;
        if !worker.add_doc(doc_id.clone()).await? {
            worker.delayed_add_docs.insert(
                doc_id.clone(),
                DelayedAddDocState {
                    attempt_no: 1,
                    last_delay: default(),
                },
            );
            msg_tx
                .send(Msg::DelayedAddDoc { doc_id })
                .expect("impossible");
        }
    }

    // let (peer_infos, mut peer_updates) = rcx.acx.repo().connected_peers();
    // let (peer_observer, observer_stop) = acx.spawn_peer_sync_observer();
    let fut = {
        let cancel_token = cancel_token.clone();
        // let peer_observer = observer.subscribe();
        async move {
            loop {
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
            use futures::StreamExt;
            use futures_buffered::BufferedStreamExt;

            futures::stream::iter(
                worker
                    .known_doc_set
                    .into_iter()
                    .map(|(_id, doc)| doc.stop()),
            )
            .buffered_unordered(16)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Res<Vec<_>>>()?;
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

    known_doc_set: HashMap<DocumentId, DocSyncState>,
    msg_tx: mpsc::UnboundedSender<Msg>,
    events_tx: tokio::sync::broadcast::Sender<FullSyncEvent>,

    full_sync_peers: HashSet<EndpointId>,
    known_peer_set: HashMap<ConnectionId, PeerSyncState>,
    conn_by_peer: HashMap<EndpointId, ConnectionId>,
    delayed_add_docs: HashMap<DocumentId, DelayedAddDocState>,
}

struct PeerSyncState {
    endpoint_id: EndpointId,
    synced_docs: HashSet<DocumentId>,
}

struct DocSyncState {
    // doc_id: Arc<str>,
    latest_heads: ChangeHashSet,
    cancel_token: CancellationToken,
    join_handle: tokio::task::JoinHandle<()>,
    broker_stop_token: Arc<am_utils_rs::changes::DocChangeBrokerStopToken>,
}

impl DocSyncState {
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

#[derive(Debug)]
struct DelayedAddDocState {
    attempt_no: usize,
    last_delay: Duration,
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
            Msg::AddPeer {
                endpoint_id,
                resp,
                conn_id,
            } => {
                self.known_peer_set
                    .entry(conn_id)
                    .or_insert_with(|| PeerSyncState {
                        endpoint_id: endpoint_id.clone(),
                        synced_docs: default(),
                    });
                self.conn_by_peer
                    .entry(endpoint_id)
                    .or_insert_with(|| conn_id);
                resp.send(())
                    .inspect_err(|_| warn!("called dropped before finish"))
                    .ok();
            }
            Msg::RemovePeer { endpoint_id, resp } => {
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
            Msg::DelayedAddDoc { doc_id } => {
                let Some(state) = self.delayed_add_docs.get_mut(&doc_id) else {
                    return Ok(());
                };
                warn!(?state, "added doc not found in repo, will retry");
                let delay = (state.last_delay * 2)
                    .min(Duration::from_millis(500))
                    .max(Duration::from_secs(10));
                state.last_delay = delay;
                state.attempt_no += 1;

                let msg_tx = self.msg_tx.clone();
                tokio::task::spawn(async move {
                    tokio::time::sleep(delay).await;
                    msg_tx
                        .send(Msg::DelayedAddDoc {
                            doc_id: doc_id.clone(),
                        })
                        .inspect_err(|_| {
                            error!(?doc_id, "FullSyncWorker died before doc was enqued");
                        })
                        .ok();
                });
            }
            Msg::GetSnapshot { resp } => {
                let snap = FullSyncSnapshot {
                    known_doc_count: self.known_doc_set.len(),
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

    async fn handle_doc_peer_state_change(
        &mut self,
        doc_id: DocumentId,
        diff: DocPeerStateView,
    ) -> Res<()> {
        let doc_state = self
            .known_doc_set
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

            if they_have_our_changes && we_have_their_changes {
                peer_state.synced_docs.insert(doc_id.clone());
                self.events_tx.send(FullSyncEvent::DocSyncedWithPeer {
                    endpoint_id: peer_state.endpoint_id.clone(),
                    doc_id: doc_id.clone(),
                });
            }
            if peer_state.synced_docs.len() == self.known_doc_set.len() {
                self.events_tx.send(FullSyncEvent::PeerFullSynced {
                    endpoint_id: peer_state.endpoint_id.clone(),
                    doc_count: peer_state.synced_docs.len(),
                });
            }
        }
        Ok(())
    }

    /// Returns weather or not a doc was found and added
    async fn add_doc(&mut self, doc_id: DocumentId) -> Res<bool> {
        let msg_tx = self.msg_tx.clone();

        if self.known_doc_set.contains_key(&doc_id) {
            return Ok(true);
        }

        let Some(handle) = self.acx.repo().find(doc_id.clone()).await? else {
            return Ok(false);
        };

        let latest_heads = ChangeHashSet(handle.with_document(|doc| doc.get_heads()).into());
        let (broker_handle, stop_token) = self.acx.change_manager().add_doc(handle.clone()).await?;

        let cancel_token = self.cancel_token.child_token();
        let fut = {
            let cancel_token = cancel_token.clone();
            let doc_id = doc_id.clone();

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

        let old = self.known_doc_set.insert(
            doc_id,
            DocSyncState {
                // doc_id,
                latest_heads,
                cancel_token,
                join_handle,
                broker_stop_token: stop_token,
            },
        );
        assert!(old.is_none(), "fishy");

        Ok(true)
    }

    async fn remove_doc(&mut self, doc_id: DocumentId) -> Res<()> {
        if let Some(old) = self.known_doc_set.remove(&doc_id) {
            old.stop().await?;
        }
        self.delayed_add_docs.remove(&doc_id);
        Ok(())
    }

    async fn handle_doc_heads_change(
        &mut self,
        doc_id: DocumentId,
        heads: ChangeHashSet,
    ) -> Res<()> {
        let Some(state) = self.known_doc_set.get_mut(&doc_id) else {
            // FIXME: turn to error if this can't happen
            warn!(?doc_id, "doc was removed by the time of heads update");
            return Ok(());
        };
        state.latest_heads = heads;
        for (_, peer_state) in &mut self.known_peer_set {
            peer_state.synced_docs.remove(&doc_id);
            self.events_tx.send(FullSyncEvent::StalePeer {
                endpoint_id: peer_state.endpoint_id.clone(),
            });
        }
        Ok(())
    }
}
