use crate::interlude::*;

use crate::sync::{PartitionAccessPolicy, PartitionSyncProvider, PeerKey};

use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

pub struct SyncNodeHandle {
    msg_tx: mpsc::UnboundedSender<SyncNodeMsg>,
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

impl SyncNodeHandle {
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

pub async fn spawn_sync_node(
    _provider: Arc<dyn PartitionSyncProvider>,
    _access_policy: Arc<dyn PartitionAccessPolicy>,
    cancel_token: CancellationToken,
) -> Res<(SyncNodeHandle, SyncNodeStopToken)> {
    let (msg_tx, mut msg_rx) = mpsc::unbounded_channel();
    let fut = {
        let cancel_token = cancel_token.clone();
        async move {
            let mut known_peers: HashSet<PeerKey> = HashSet::new();
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
                                known_peers.insert(peer);
                                resp.send(Ok(())).inspect_err(|_| warn!(ERROR_CALLER)).ok();
                            }
                            SyncNodeMsg::UnregisterPeer { peer, resp } => {
                                known_peers.remove(&peer);
                                resp.send(Ok(())).inspect_err(|_| warn!(ERROR_CALLER)).ok();
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
        SyncNodeHandle { msg_tx },
        SyncNodeStopToken {
            cancel_token,
            join_handle,
        },
    ))
}
