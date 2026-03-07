use crate::interlude::*;

use crate::sync::{
    GetDocsFullRequest, GetDocsFullResponse, GetPartitionEventsRequest, GetPartitionEventsResponse,
    ListPartitionsResponse, PartitionAccessPolicy, PartitionCursorRequest, PartitionSubscription,
    PartitionSyncError, PartitionSyncProvider, PeerKey, SubPartitionsRequest,
    DEFAULT_SUBSCRIPTION_CAPACITY,
};

use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

pub type SamodSyncRequest = ();

pub struct PeerSyncWorkerHandle {
    msg_tx: mpsc::UnboundedSender<PeerMsg>,
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
    provider: Arc<dyn PartitionSyncProvider>,
    access_policy: Arc<dyn PartitionAccessPolicy>,
    _samod_sync_rx: mpsc::Receiver<SamodSyncRequest>,
    cancel_token: CancellationToken,
) -> Res<(PeerSyncWorkerHandle, PeerSyncWorkerStopToken)> {
    let (msg_tx, mut msg_rx) = mpsc::unbounded_channel();
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
                                let out = provider
                                    .list_partitions_for_peer(&peer)
                                    .await
                                    .map(|mut parts| {
                                        parts.retain(|part| access_policy.can_access_partition(&peer, &part.partition_id));
                                        ListPartitionsResponse { partitions: parts }
                                    });
                                resp.send(out).inspect_err(|_| warn!(ERROR_CALLER)).ok();
                            }
                            PeerMsg::GetPartitionEvents { req, resp } => {
                                let out = if let Err(err) =
                                    ensure_partition_access(&peer, &req.partitions, access_policy.as_ref())
                                {
                                    Err(err)
                                } else {
                                    provider
                                        .get_partition_events(&peer, &req.partitions)
                                        .await
                                        .map(|events| GetPartitionEventsResponse { events })
                                };
                                resp.send(out).inspect_err(|_| warn!(ERROR_CALLER)).ok();
                            }
                            PeerMsg::GetDocsFull { req, resp } => {
                                let out = provider
                                    .get_docs_full(&peer, &req.doc_ids)
                                    .await
                                    .map(|docs| GetDocsFullResponse { docs });
                                resp.send(out).inspect_err(|_| warn!(ERROR_CALLER)).ok();
                            }
                            PeerMsg::Subscribe { req, resp } => {
                                let out = if let Err(err) =
                                    ensure_partition_access(&peer, &req.partitions, access_policy.as_ref())
                                {
                                    Err(err)
                                } else {
                                    provider
                                        .subscribe(
                                            &peer,
                                            &req.partitions,
                                            DEFAULT_SUBSCRIPTION_CAPACITY,
                                        )
                                        .await
                                };
                                resp.send(out).inspect_err(|_| warn!(ERROR_CALLER)).ok();
                            }
                        }
                    }
                }
            }
            eyre::Ok(())
        }
    };
    let join_handle = tokio::spawn(async {
        fut.await.unwrap();
    });
    Ok((
        PeerSyncWorkerHandle { msg_tx },
        PeerSyncWorkerStopToken {
            cancel_token,
            join_handle,
        },
    ))
}

fn ensure_partition_access(
    peer: &PeerKey,
    reqs: &[PartitionCursorRequest],
    access_policy: &dyn PartitionAccessPolicy,
) -> Res<()> {
    for req in reqs {
        if !access_policy.can_access_partition(peer, &req.partition_id) {
            return Err(PartitionSyncError::AccessDenied {
                partition_id: req.partition_id.clone(),
            }
            .into_report());
        }
    }
    Ok(())
}
