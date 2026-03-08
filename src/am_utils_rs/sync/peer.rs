use crate::interlude::*;

use crate::sync::protocol::{
    GetDocsFullRpcReq, GetPartitionEventsRpcReq, ListPartitionsRpcReq, PartitionSyncRpc,
    SubPartitionsRpcReq,
};
use crate::sync::{
    GetDocsFullRequest, GetDocsFullResponse, GetPartitionEventsRequest, GetPartitionEventsResponse,
    ListPartitionsResponse, PartitionSubscription, PeerKey, PeerSyncProgressEvent,
    SubPartitionsRequest, DEFAULT_SUBSCRIPTION_CAPACITY,
};

use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

pub type SamodSyncRequest = ();

pub struct PeerSyncWorkerHandle {
    msg_tx: mpsc::UnboundedSender<PeerMsg>,
    pub events_rx: Option<broadcast::Receiver<PeerSyncProgressEvent>>,
}

impl PeerSyncWorkerHandle {
    pub async fn list_partitions(&self) -> Res<ListPartitionsResponse> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.msg_tx
            .send(PeerMsg::ListPartitions { resp: resp_tx })
            .wrap_err("peer worker is closed")?;
        resp_rx.await.wrap_err(ERROR_CHANNEL)?
    }

    pub async fn get_partition_events(
        &self,
        req: GetPartitionEventsRequest,
    ) -> Res<GetPartitionEventsResponse> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.msg_tx
            .send(PeerMsg::GetPartitionEvents { req, resp: resp_tx })
            .wrap_err("peer worker is closed")?;
        resp_rx.await.wrap_err(ERROR_CHANNEL)?
    }

    pub async fn get_docs_full(&self, req: GetDocsFullRequest) -> Res<GetDocsFullResponse> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.msg_tx
            .send(PeerMsg::GetDocsFull { req, resp: resp_tx })
            .wrap_err("peer worker is closed")?;
        resp_rx.await.wrap_err(ERROR_CHANNEL)?
    }

    pub async fn subscribe(&self, req: SubPartitionsRequest) -> Res<PartitionSubscription> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.msg_tx
            .send(PeerMsg::Subscribe { req, resp: resp_tx })
            .wrap_err("peer worker is closed")?;
        resp_rx.await.wrap_err(ERROR_CHANNEL)?
    }
}

pub struct PeerSyncWorkerStopToken {
    cancel_token: CancellationToken,
    join_handle: tokio::task::JoinHandle<()>,
}

impl PeerSyncWorkerStopToken {
    pub async fn stop(self) -> Res<()> {
        self.cancel_token.cancel();
        utils_rs::wait_on_handle_with_timeout(self.join_handle, Duration::from_secs(1))
            .await
            .wrap_err("failed stopping peer sync worker")
    }
}

enum PeerMsg {
    ListPartitions {
        resp: oneshot::Sender<Res<ListPartitionsResponse>>,
    },
    GetPartitionEvents {
        req: GetPartitionEventsRequest,
        resp: oneshot::Sender<Res<GetPartitionEventsResponse>>,
    },
    GetDocsFull {
        req: GetDocsFullRequest,
        resp: oneshot::Sender<Res<GetDocsFullResponse>>,
    },
    Subscribe {
        req: SubPartitionsRequest,
        resp: oneshot::Sender<Res<PartitionSubscription>>,
    },
}

pub async fn spawn_peer_sync_worker(
    peer: PeerKey,
    rpc_client: irpc::Client<PartitionSyncRpc>,
    _samod_sync_rx: mpsc::Receiver<SamodSyncRequest>,
    cancel_token: CancellationToken,
) -> Res<(PeerSyncWorkerHandle, PeerSyncWorkerStopToken)> {
    let (msg_tx, mut msg_rx) = mpsc::unbounded_channel();
    let (events_tx, events_rx) = broadcast::channel(2048);
    let fut = {
        let cancel_token = cancel_token.clone();
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
                            PeerMsg::ListPartitions { resp } => {
                                let start_at = std::time::Instant::now();
                                let _ = events_tx.send(PeerSyncProgressEvent::RequestStarted {
                                    op: "list_partitions",
                                });
                                let out = match rpc_client
                                    .rpc(ListPartitionsRpcReq { peer: peer.clone() })
                                    .await
                                {
                                    Ok(Ok(value)) => Ok(value),
                                    Ok(Err(err)) => Err(err.into_report()),
                                    Err(err) => Err(ferr!("irpc list_partitions failed: {err}")),
                                };
                                let _ = events_tx.send(PeerSyncProgressEvent::RequestFinished {
                                    op: "list_partitions",
                                    success: out.is_ok(),
                                    elapsed: start_at.elapsed(),
                                });
                                resp.send(out).inspect_err(|_| warn!(ERROR_CALLER)).ok();
                            }
                            PeerMsg::GetPartitionEvents { req, resp } => {
                                let start_at = std::time::Instant::now();
                                let _ = events_tx.send(PeerSyncProgressEvent::RequestStarted {
                                    op: "get_partition_events",
                                });
                                let out = match rpc_client
                                    .rpc(GetPartitionEventsRpcReq {
                                        peer: peer.clone(),
                                        req,
                                    })
                                    .await
                                {
                                    Ok(Ok(value)) => Ok(value),
                                    Ok(Err(err)) => Err(err.into_report()),
                                    Err(err) => Err(ferr!("irpc get_partition_events failed: {err}")),
                                };
                                let _ = events_tx.send(PeerSyncProgressEvent::RequestFinished {
                                    op: "get_partition_events",
                                    success: out.is_ok(),
                                    elapsed: start_at.elapsed(),
                                });
                                resp.send(out).inspect_err(|_| warn!(ERROR_CALLER)).ok();
                            }
                            PeerMsg::GetDocsFull { req, resp } => {
                                let start_at = std::time::Instant::now();
                                let _ = events_tx.send(PeerSyncProgressEvent::RequestStarted {
                                    op: "get_docs_full",
                                });
                                let out = match rpc_client
                                    .rpc(GetDocsFullRpcReq {
                                        peer: peer.clone(),
                                        req,
                                    })
                                    .await
                                {
                                    Ok(Ok(value)) => Ok(value),
                                    Ok(Err(err)) => Err(err.into_report()),
                                    Err(err) => Err(ferr!("irpc get_docs_full failed: {err}")),
                                };
                                let _ = events_tx.send(PeerSyncProgressEvent::RequestFinished {
                                    op: "get_docs_full",
                                    success: out.is_ok(),
                                    elapsed: start_at.elapsed(),
                                });
                                resp.send(out).inspect_err(|_| warn!(ERROR_CALLER)).ok();
                            }
                            PeerMsg::Subscribe { req, resp } => {
                                let start_at = std::time::Instant::now();
                                let _ = events_tx.send(PeerSyncProgressEvent::RequestStarted {
                                    op: "subscribe",
                                });
                                let out = match rpc_client
                                    .server_streaming(
                                        SubPartitionsRpcReq {
                                            peer: peer.clone(),
                                            req,
                                        },
                                        DEFAULT_SUBSCRIPTION_CAPACITY,
                                    )
                                    .await
                                {
                                    Ok(rpc_rx) => {
                                        let (tx, rx) = mpsc::channel(DEFAULT_SUBSCRIPTION_CAPACITY);
                                        tokio::spawn(bridge_subscription_stream(
                                            rpc_rx,
                                            tx,
                                            events_tx.clone(),
                                        ));
                                        Ok(PartitionSubscription { rx })
                                    }
                                    Err(err) => Err(ferr!("irpc sub_partitions failed: {err}")),
                                };
                                let _ = events_tx.send(PeerSyncProgressEvent::RequestFinished {
                                    op: "subscribe",
                                    success: out.is_ok(),
                                    elapsed: start_at.elapsed(),
                                });
                                resp.send(out).inspect_err(|_| warn!(ERROR_CALLER)).ok();
                            }
                        }
                    }
                }
            }
            eyre::Ok(())
        }
    };
    let join_handle = tokio::spawn(async { fut.await.unwrap() });
    Ok((
        PeerSyncWorkerHandle {
            msg_tx,
            events_rx: Some(events_rx),
        },
        PeerSyncWorkerStopToken {
            cancel_token,
            join_handle,
        },
    ))
}

async fn bridge_subscription_stream(
    mut rpc_rx: irpc::channel::mpsc::Receiver<crate::sync::SubscriptionItem>,
    tx: mpsc::Sender<crate::sync::SubscriptionItem>,
    events_tx: broadcast::Sender<PeerSyncProgressEvent>,
) {
    while let Ok(Some(item)) = rpc_rx.recv().await {
        if tx.send(item).await.is_err() {
            break;
        }
        let _ = events_tx.send(PeerSyncProgressEvent::SubscriptionForwarded);
    }
}
