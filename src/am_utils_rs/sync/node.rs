use crate::interlude::*;

use crate::sync::protocol::{
    GetDocsFullRpcReq, GetPartitionEventsRpcReq, ListPartitionsRpcReq, PartitionSyncRpc,
    PartitionSyncRpcMessage, SubPartitionsRpcReq,
};
use crate::sync::{
    GetDocsFullResponse, GetPartitionEventsResponse, ListPartitionsResponse, PartitionAccessPolicy,
    PartitionCursorRequest, PartitionSubscription, PartitionSyncError, PartitionSyncProvider,
    PeerKey, DEFAULT_SUBSCRIPTION_CAPACITY,
};

use irpc::WithChannels;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

pub struct SyncNodeHandle {
    msg_tx: mpsc::UnboundedSender<SyncNodeMsg>,
    rpc_client: irpc::Client<PartitionSyncRpc>,
}

enum SyncNodeMsg {
    RegisterPeer {
        peer: PeerKey,
        resp: oneshot::Sender<Res<()>>,
    },
    UnregisterPeer {
        peer: PeerKey,
        resp: oneshot::Sender<Res<()>>,
    },
}

impl SyncNodeHandle {
    pub fn rpc_client(&self) -> irpc::Client<PartitionSyncRpc> {
        self.rpc_client.clone()
    }

    pub async fn register_local_peer(&self, peer: PeerKey) -> Res<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.msg_tx
            .send(SyncNodeMsg::RegisterPeer {
                peer,
                resp: resp_tx,
            })
            .wrap_err("sync node is closed")?;
        resp_rx.await.wrap_err(ERROR_CHANNEL)?
    }

    pub async fn unregister_local_peer(&self, peer: PeerKey) -> Res<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.msg_tx
            .send(SyncNodeMsg::UnregisterPeer {
                peer,
                resp: resp_tx,
            })
            .wrap_err("sync node is closed")?;
        resp_rx.await.wrap_err(ERROR_CHANNEL)?
    }
}

pub struct SyncNodeStopToken {
    cancel_token: CancellationToken,
    join_handle: tokio::task::JoinHandle<()>,
}

impl SyncNodeStopToken {
    pub async fn stop(self) -> Res<()> {
        self.cancel_token.cancel();
        utils_rs::wait_on_handle_with_timeout(self.join_handle, Duration::from_secs(1))
            .await
            .wrap_err("failed stopping sync node")
    }
}

pub async fn spawn_sync_node(
    provider: Arc<dyn PartitionSyncProvider>,
    access_policy: Arc<dyn PartitionAccessPolicy>,
    cancel_token: CancellationToken,
) -> Res<(SyncNodeHandle, SyncNodeStopToken)> {
    let (msg_tx, mut msg_rx) = mpsc::unbounded_channel();
    let (rpc_tx, mut rpc_rx) = tokio::sync::mpsc::channel(1024);
    let rpc_client = irpc::Client::<PartitionSyncRpc>::local(rpc_tx);

    let fut = {
        let cancel_token = cancel_token.clone();
        let mut worker = SyncNodeWorker {
            known_peers: default(),
            provider,
            access_policy,
        };
        async move {
            loop {
                tokio::select! {
                    biased;
                    _ = cancel_token.cancelled() => break,
                    msg = msg_rx.recv() => {
                        let Some(msg) = msg else {
                            break;
                        };
                        match msg {
                            SyncNodeMsg::RegisterPeer { peer, resp } => {
                                worker.known_peers.insert(peer);
                                resp.send(Ok(())).inspect_err(|_| warn!(ERROR_CALLER)).ok();
                            }
                            SyncNodeMsg::UnregisterPeer { peer, resp } => {
                                worker.known_peers.remove(&peer);
                                resp.send(Ok(())).inspect_err(|_| warn!(ERROR_CALLER)).ok();
                            }
                        }
                    }
                    msg = rpc_rx.recv() => {
                        let Some(msg) = msg else {
                            break;
                        };
                        worker.handle_rpc_message(
                            msg
                        ).await;
                    }
                }
            }
            eyre::Ok(())
        }
    };
    let join_handle = tokio::spawn(async { fut.await.unwrap() });
    Ok((
        SyncNodeHandle { msg_tx, rpc_client },
        SyncNodeStopToken {
            cancel_token,
            join_handle,
        },
    ))
}

struct SyncNodeWorker {
    provider: Arc<dyn PartitionSyncProvider>,
    access_policy: Arc<dyn PartitionAccessPolicy>,
    known_peers: HashSet<PeerKey>,
}

impl SyncNodeWorker {
    async fn handle_rpc_message(&mut self, msg: PartitionSyncRpcMessage) {
        match msg {
            PartitionSyncRpcMessage::ListPartitions(req) => {
                let WithChannels { inner, tx, .. } = req;
                let ListPartitionsRpcReq { peer } = inner;
                let out = (async {
                    self.ensure_known_peer(&peer)?;
                    let mut partitions = self
                        .provider
                        .list_partitions_for_peer(&peer)
                        .await
                        .map_err(map_provider_err)?;
                    partitions.retain(|part| {
                        self.access_policy
                            .can_access_partition(&peer, &part.partition_id)
                    });
                    Ok::<_, PartitionSyncError>(ListPartitionsResponse { partitions })
                })
                .await;
                tx.send(out).await.inspect_err(|_| warn!(ERROR_CALLER)).ok();
            }
            PartitionSyncRpcMessage::GetPartitionEvents(req) => {
                let WithChannels { inner, tx, .. } = req;
                let GetPartitionEventsRpcReq { peer, req } = inner;
                let out = (async {
                    self.ensure_known_peer(&peer)?;
                    self.ensure_partition_access(&peer, &req.partitions)?;
                    let events = self
                        .provider
                        .get_partition_events(&peer, &req.partitions)
                        .await
                        .map_err(map_provider_err)?;
                    Ok::<_, PartitionSyncError>(GetPartitionEventsResponse { events })
                })
                .await;
                tx.send(out).await.inspect_err(|_| warn!(ERROR_CALLER)).ok();
            }
            PartitionSyncRpcMessage::GetDocsFull(req) => {
                let WithChannels { inner, tx, .. } = req;
                let GetDocsFullRpcReq { peer, req } = inner;
                let out = (async {
                    self.ensure_known_peer(&peer)?;
                    for doc_id in &req.doc_ids {
                        let allowed = self
                            .provider
                            .is_doc_accessible_for_peer(&peer, doc_id)
                            .await
                            .map_err(map_provider_err)?;
                        if !allowed {
                            return Err(PartitionSyncError::DocAccessDenied {
                                doc_id: doc_id.clone(),
                            });
                        }
                    }
                    let docs = self
                        .provider
                        .get_docs_full(&peer, &req.doc_ids)
                        .await
                        .map_err(map_provider_err)?;
                    Ok::<_, PartitionSyncError>(GetDocsFullResponse { docs })
                })
                .await;
                tx.send(out).await.inspect_err(|_| warn!(ERROR_CALLER)).ok();
            }
            PartitionSyncRpcMessage::SubPartitions(req) => {
                let WithChannels { inner, tx, .. } = req;
                let SubPartitionsRpcReq { peer, req } = inner;
                let maybe_sub = (async {
                    self.ensure_known_peer(&peer)?;
                    self.ensure_partition_access(&peer, &req.partitions)?;
                    self.provider
                        .subscribe(&peer, &req.partitions, DEFAULT_SUBSCRIPTION_CAPACITY)
                        .await
                        .map_err(map_provider_err)
                })
                .await;
                match maybe_sub {
                    Ok(sub) => {
                        spawn_forward_subscription(sub, tx);
                    }
                    Err(err) => {
                        warn!(?err, "failed opening subscription");
                    }
                }
            }
        }
    }

    fn ensure_partition_access(
        &mut self,
        peer: &PeerKey,
        reqs: &[PartitionCursorRequest],
    ) -> Result<(), PartitionSyncError> {
        for req in reqs {
            if !self
                .access_policy
                .can_access_partition(peer, &req.partition_id)
            {
                return Err(PartitionSyncError::AccessDenied {
                    partition_id: req.partition_id.clone(),
                });
            }
        }
        Ok(())
    }

    fn ensure_known_peer(&mut self, peer: &PeerKey) -> Result<(), PartitionSyncError> {
        if self.known_peers.contains(peer) {
            return Ok(());
        }
        Err(PartitionSyncError::Internal {
            message: format!("peer {peer:?} is not registered in sync node"),
        })
    }
}

// FIXME: recieve a child_token for cancellation and use
// a select loop over it
// and also, have the worker keep track of all join handles
// for cleanup
fn spawn_forward_subscription(
    mut sub: PartitionSubscription,
    tx: irpc::channel::mpsc::Sender<crate::sync::SubscriptionItem>,
) {
    tokio::spawn(async move {
        while let Some(item) = sub.rx.recv().await {
            if tx.send(item).await.is_err() {
                break;
            }
        }
    });
}

fn map_provider_err(err: eyre::Report) -> PartitionSyncError {
    PartitionSyncError::Internal {
        message: err.to_string(),
    }
}
