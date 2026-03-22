use crate::interlude::*;

use crate::partition::PartitionStore;
use crate::sync::protocol::*;
use crate::sync::store::SyncStoreHandle;
use crate::sync::PartitionAccessPolicy;

use irpc::WithChannels;
use tokio_util::sync::CancellationToken;

pub struct SyncNodeHandle {
    rpc_tx: tokio::sync::mpsc::Sender<PartitionSyncRpcMessage>,
    rpc_client: irpc::Client<PartitionSyncRpc>,
}

impl SyncNodeHandle {
    pub fn rpc_client(&self) -> irpc::Client<PartitionSyncRpc> {
        self.rpc_client.clone()
    }

    pub fn local_sender(&self) -> tokio::sync::mpsc::Sender<PartitionSyncRpcMessage> {
        self.rpc_tx.clone()
    }

}

pub struct SyncNodeStopToken {
    cancel_token: CancellationToken,
    subscription_tasks: Arc<utils_rs::AbortableJoinSet>,
    join_handle: tokio::task::JoinHandle<()>,
}

impl SyncNodeStopToken {
    pub async fn stop(self) -> Res<()> {
        self.cancel_token.cancel();
        self.subscription_tasks
            .stop(Duration::from_secs(5))
            .await
            .wrap_err("failed stopping sync node subscription forwarders")?;
        utils_rs::wait_on_handle_with_timeout(self.join_handle, Duration::from_secs(5))
            .await
            .wrap_err("failed stopping sync node")
    }
}

pub async fn spawn_sync_node(
    partition_store: Arc<PartitionStore>,
    sync_store: SyncStoreHandle,
    access_policy: Arc<dyn PartitionAccessPolicy>,
) -> Res<(SyncNodeHandle, SyncNodeStopToken)> {
    let (rpc_tx, mut rpc_rx) = tokio::sync::mpsc::channel(1024);
    let rpc_client = irpc::Client::<PartitionSyncRpc>::local(rpc_tx.clone());

    let cancel_token = CancellationToken::new();
    let subscription_tasks = Arc::new(utils_rs::AbortableJoinSet::new());
    let fut = {
        let cancel_token = cancel_token.clone();
        let subscription_tasks = Arc::clone(&subscription_tasks);
        let mut worker = SyncNodeWorker {
            partition_store,
            sync_store,
            access_policy,
            cancel_token: cancel_token.clone(),
            subscription_tasks,
        };
        async move {
            loop {
                tokio::select! {
                    biased;
                    _ = cancel_token.cancelled() => break,
                    msg = rpc_rx.recv() => {
                        let Some(msg) = msg else {
                            break;
                        };
                        worker.handle_rpc_message(msg).await;
                    }
                }
            }
            eyre::Ok(())
        }
    };
    let join_handle = tokio::spawn(async { fut.await.unwrap() });
    Ok((
        SyncNodeHandle {
            rpc_tx,
            rpc_client,
        },
        SyncNodeStopToken {
            cancel_token,
            subscription_tasks,
            join_handle,
        },
    ))
}

struct SyncNodeWorker {
    partition_store: Arc<PartitionStore>,
    sync_store: SyncStoreHandle,
    access_policy: Arc<dyn PartitionAccessPolicy>,
    cancel_token: CancellationToken,
    subscription_tasks: Arc<utils_rs::AbortableJoinSet>,
}

impl SyncNodeWorker {
    async fn handle_rpc_message(&mut self, msg: PartitionSyncRpcMessage) {
        match msg {
            PartitionSyncRpcMessage::ListPartitions(req) => {
                let WithChannels { inner, tx, .. } = req;
                let ListPartitionsRpcReq { peer } = inner;
                let out = (async {
                    self.ensure_known_peer(&peer).await?;
                    let mut partitions = self
                        .partition_store
                        .list_partitions_for_peer(&peer)
                        .await
                        .map_err(map_repo_err)?;
                    partitions.partitions.retain(|part| {
                        self.access_policy
                            .can_access_partition(&peer, &part.partition_id)
                    });
                    Ok::<_, PartitionSyncError>(partitions)
                })
                .await;
                tx.send(out).await.inspect_err(|_| warn!(ERROR_CALLER)).ok();
            }
            PartitionSyncRpcMessage::GetPartitionMemberEvents(req) => {
                let WithChannels { inner, tx, .. } = req;
                let GetPartitionMemberEventsRpcReq { peer, req } = inner;
                let out = (async {
                    self.ensure_known_peer(&peer).await?;
                    self.ensure_partition_access(&peer, &req.partitions)?;
                    let out = self
                        .partition_store
                        .get_partition_member_events_for_peer(&peer, &req)
                        .await
                        .map_err(map_repo_err)?;
                    Ok::<_, PartitionSyncError>(GetPartitionMemberEventsResponse {
                        events: out.events,
                        cursors: out.cursors,
                    })
                })
                .await;
                tx.send(out).await.inspect_err(|_| warn!(ERROR_CALLER)).ok();
            }
            PartitionSyncRpcMessage::GetPartitionDocEvents(req) => {
                let WithChannels { inner, tx, .. } = req;
                let GetPartitionDocEventsRpcReq { peer, req } = inner;
                let out = (async {
                    self.ensure_known_peer(&peer).await?;
                    self.ensure_partition_access(&peer, &req.partitions)?;
                    let out = self
                        .partition_store
                        .get_partition_doc_events_for_peer(&peer, &req)
                        .await
                        .map_err(map_repo_err)?;
                    Ok::<_, PartitionSyncError>(GetPartitionDocEventsResponse {
                        events: out.events,
                        cursors: out.cursors,
                    })
                })
                .await;
                tx.send(out).await.inspect_err(|_| warn!(ERROR_CALLER)).ok();
            }
            PartitionSyncRpcMessage::SubPartitions(req) => {
                let WithChannels { inner, tx, .. } = req;
                let SubPartitionsRpcReq { peer, req } = inner;
                let maybe_sub = (async {
                    self.ensure_known_peer(&peer).await?;
                    for part in &req.partitions {
                        if !self
                            .access_policy
                            .can_access_partition(&peer, &part.partition_id)
                        {
                            return Err(PartitionSyncError::AccessDenied {
                                partition_id: part.partition_id.clone(),
                            });
                        }
                    }
                    self.partition_store
                        .subscribe_partition_events_for_peer(
                            &peer,
                            &req,
                            DEFAULT_SUBSCRIPTION_CAPACITY,
                        )
                        .await
                        .map_err(map_repo_err)
                })
                .await;
                match maybe_sub {
                    Ok(mut sub) => {
                        let child_token = self.cancel_token.child_token();
                        self.subscription_tasks
                            .spawn(async move {
                                let fut = async move {
                                    loop {
                                        tokio::select! {
                                            biased;
                                            _ = child_token.cancelled() => break,
                                            item = sub.recv() => {
                                                let Some(item) = item else {
                                                    break;
                                                };
                                                if tx.send(item).await.is_err() {
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                    eyre::Ok(())
                                };
                                fut.await.unwrap();
                            })
                            .expect("failed spawning sync node subscription forwarder");
                    }
                    Err(err) => {
                        warn!(?err, "failed opening partition subscription");
                    }
                }
            }
        }
    }

    fn ensure_partition_access(
        &self,
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

    async fn ensure_known_peer(&self, peer: &PeerKey) -> Result<(), PartitionSyncError> {
        let known = self
            .sync_store
            .is_peer_allowed(peer.clone())
            .await
            .map_err(map_store_err)?;
        if known {
            return Ok(());
        }
        Err(PartitionSyncError::Internal {
            message: format!("peer {peer:?} is not registered in sync node"),
        })
    }
}

fn map_store_err(err: eyre::Report) -> PartitionSyncError {
    map_repo_err(err)
}

fn map_repo_err(err: eyre::Report) -> PartitionSyncError {
    PartitionSyncError::Internal {
        message: err.to_string(),
    }
}
