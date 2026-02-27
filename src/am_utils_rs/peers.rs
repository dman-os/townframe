use crate::interlude::*;

use futures::StreamExt;
use tokio::{sync, task::JoinHandle};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocPeerSyncState {
    pub connection_id: samod::ConnectionId,
    pub connection_state: Arc<samod::ConnectionState>,
    pub peer_id: Option<Arc<samod::PeerId>>,
    pub doc_id: samod::DocumentId,
    pub state: Arc<samod::PeerDocState>,
}

#[derive(Debug, Clone)]
pub enum DocPeerSyncUpdate {
    Upsert(Arc<DocPeerSyncState>),
    Removed {
        connection_id: samod::ConnectionId,
        doc_id: samod::DocumentId,
    },
}

pub struct PeerSyncObserverStopToken {
    cancel_token: CancellationToken,
    join_handle: JoinHandle<()>,
}

impl PeerSyncObserverStopToken {
    pub async fn stop(self) -> Res<()> {
        self.cancel_token.cancel();
        self.join_handle.await?;
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct PeerSyncSnapshot {
    pub connections: Vec<samod::ConnectionInfo>,
    pub docs: Vec<Arc<DocPeerSyncState>>,
}

enum PeerSyncMsg {
    Snapshot {
        resp: sync::oneshot::Sender<PeerSyncSnapshot>,
    },
}

pub struct PeerSyncObserverHandle {
    msg_tx: sync::mpsc::Sender<PeerSyncMsg>,
    updates_tx: sync::broadcast::Sender<Arc<[DocPeerSyncUpdate]>>,
}

impl PeerSyncObserverHandle {
    pub async fn snapshot(&self) -> Res<PeerSyncSnapshot> {
        let (tx, rx) = sync::oneshot::channel();
        self.msg_tx
            .send(PeerSyncMsg::Snapshot { resp: tx })
            .await
            .wrap_err("peer sync observer is dead")?;
        rx.await.wrap_err(ERROR_CHANNEL)
    }

    pub fn subscribe(&self) -> sync::broadcast::Receiver<Arc<[DocPeerSyncUpdate]>> {
        self.updates_tx.subscribe()
    }
}

impl super::AmCtx {
    pub fn spawn_peer_sync_observer(
        &self,
    ) -> (Arc<PeerSyncObserverHandle>, PeerSyncObserverStopToken) {
        let (initial_connections, mut stream) = self.repo.connected_peers();

        let (updates_tx, _updates_rx) = sync::broadcast::channel(64);

        let (msg_tx, mut msg_rx) = sync::mpsc::channel(16);
        let handle = Arc::new(PeerSyncObserverHandle {
            updates_tx: updates_tx.clone(),
            msg_tx,
        });

        let cancel_token = CancellationToken::new();

        let fut = {
            let cancel_token = cancel_token.clone();

            let initial_docs = flatten_connections(&initial_connections);
            let mut observer = PeerSyncObserver {
                connections: initial_connections,
                docs: initial_docs,
                updates_tx,
            };

            async move {
                loop {
                    tokio::select! {
                        biased;
                        _ = cancel_token.cancelled() => {
                            break;
                        }
                        val = msg_rx.recv() => {
                            let Some(msg) = val else {
                                break;
                            };
                            observer.handle_msg(msg).await?;
                        }
                        val = stream.next() => {
                            let Some(connections) = val else {
                                break;
                            };
                            observer.handle_event(connections).await?;
                        }
                    };
                }
                eyre::Ok(())
            }
        };
        let join_handle = tokio::spawn(async { fut.await.unwrap() });

        (
            handle,
            PeerSyncObserverStopToken {
                cancel_token,
                join_handle,
            },
        )
    }
}

struct PeerSyncObserver {
    connections: Vec<samod::ConnectionInfo>,
    docs: Vec<Arc<DocPeerSyncState>>,
    updates_tx: sync::broadcast::Sender<Arc<[DocPeerSyncUpdate]>>,
}

impl PeerSyncObserver {
    async fn handle_msg(&mut self, msg: PeerSyncMsg) -> Res<()> {
        match msg {
            PeerSyncMsg::Snapshot { resp } => {
                resp.send(PeerSyncSnapshot {
                    connections: self.connections.clone(),
                    docs: self.docs.clone(),
                })
                .inspect_err(|err| {
                    error!(?err, "caller dropped before response");
                })
                .ok();
            }
        }
        Ok(())
    }
    async fn handle_event(&mut self, connections: Vec<samod::ConnectionInfo>) -> Res<()> {
        let docs = flatten_connections(&connections);
        let updates = diff_doc_state(&self.docs, &docs);
        self.connections = connections;
        self.docs = docs;
        if !updates.is_empty() {
            let _ = self.updates_tx.send(updates.into());
        }
        Ok(())
    }
}

fn flatten_connections(connections: &[samod::ConnectionInfo]) -> Vec<Arc<DocPeerSyncState>> {
    let mut out = vec![];
    for conn in connections {
        let connection_state = Arc::new(conn.state.clone());
        let peer_id = match &conn.state {
            samod::ConnectionState::Connected { their_peer_id } => {
                Some(Arc::new(their_peer_id.clone()))
            }
            samod::ConnectionState::Handshaking => None,
        };
        for (doc_id, state) in &conn.docs {
            out.push(Arc::new(DocPeerSyncState {
                connection_id: conn.id,
                connection_state: Arc::clone(&connection_state),
                peer_id: peer_id.as_ref().map(Arc::clone),
                doc_id: doc_id.clone(),
                state: Arc::new(state.clone()),
            }));
        }
    }
    out
}

fn diff_doc_state(
    prev: &[Arc<DocPeerSyncState>],
    next: &[Arc<DocPeerSyncState>],
) -> Vec<DocPeerSyncUpdate> {
    let mut prev_map = std::collections::HashMap::new();
    for item in prev {
        prev_map.insert((item.connection_id, item.doc_id.clone()), Arc::clone(item));
    }
    let mut next_map = std::collections::HashMap::new();
    for item in next {
        next_map.insert((item.connection_id, item.doc_id.clone()), Arc::clone(item));
    }

    let mut updates = vec![];
    for (key, next_item) in &next_map {
        if prev_map
            .get(key)
            .map(|old| old != next_item)
            .unwrap_or(true)
        {
            updates.push(DocPeerSyncUpdate::Upsert(Arc::clone(next_item)));
        }
    }

    for key in prev_map.keys() {
        if !next_map.contains_key(key) {
            updates.push(DocPeerSyncUpdate::Removed {
                connection_id: key.0,
                doc_id: key.1.clone(),
            });
        }
    }
    updates
}
