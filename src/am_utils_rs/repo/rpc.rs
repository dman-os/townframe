use crate::interlude::*;

use crate::repo::SharedBigRepo;
use crate::sync::store::SyncStoreHandle;
use crate::sync::{protocol::PartitionSyncError, PartitionAccessPolicy};
use irpc::{channel, rpc_requests, WithChannels};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub const MAX_GET_DOCS_FULL_DOC_IDS: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FullDoc {
    pub doc_id: String,
    pub automerge_save: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GetDocsFullRequest {
    pub doc_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GetDocsFullResponse {
    pub docs: Vec<FullDoc>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GetDocsFullRpcReq {
    pub req: GetDocsFullRequest,
}

// NOTE: this is used over an 0rtt iroh irpc impl wihch is
// only safe when all the requests are idempotent. if this
// changes for our requests, amend the 0rtt usage
#[rpc_requests(message = RepoSyncRpcMessage)]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum RepoSyncRpc {
    #[rpc(tx = channel::oneshot::Sender<Result<GetDocsFullResponse, PartitionSyncError>>)]
    GetDocsFull(GetDocsFullRpcReq),
}

pub struct RepoRpcHandle {
    rpc_tx: mpsc::Sender<(crate::sync::protocol::PeerKey, RepoSyncRpcMessage)>,
}

impl RepoRpcHandle {
    pub fn local_sender(
        &self,
    ) -> mpsc::Sender<(crate::sync::protocol::PeerKey, RepoSyncRpcMessage)> {
        self.rpc_tx.clone()
    }
}

pub struct RepoRpcStopToken {
    cancel_token: CancellationToken,
    join_handle: tokio::task::JoinHandle<()>,
}

impl RepoRpcStopToken {
    pub async fn stop(self) -> Res<()> {
        self.cancel_token.cancel();
        utils_rs::wait_on_handle_with_timeout(self.join_handle, Duration::from_secs(5))
            .await
            .wrap_err("failed stopping repo rpc")
    }
}

pub async fn spawn_repo_rpc(
    big_repo: SharedBigRepo,
    sync_store: SyncStoreHandle,
    access_policy: Arc<dyn PartitionAccessPolicy>,
) -> Res<(RepoRpcHandle, RepoRpcStopToken)> {
    let (rpc_tx, mut rpc_rx) = mpsc::channel(1024);

    let cancel_token = CancellationToken::new();
    let fut = {
        let cancel_token = cancel_token.clone();
        async move {
            loop {
                tokio::select! {
                    biased;
                    _ = cancel_token.cancelled() => break,
                    msg = rpc_rx.recv() => {
                        let Some((peer, msg)) = msg else {
                            break;
                        };
                        handle_rpc_message(&big_repo, &sync_store, access_policy.as_ref(), peer, msg).await;
                    }
                }
            }
            eyre::Ok(())
        }
    };
    let join_handle = tokio::spawn(async { fut.await.unwrap() });
    Ok((
        RepoRpcHandle { rpc_tx },
        RepoRpcStopToken {
            cancel_token,
            join_handle,
        },
    ))
}

async fn handle_rpc_message(
    big_repo: &SharedBigRepo,
    sync_store: &SyncStoreHandle,
    access_policy: &dyn PartitionAccessPolicy,
    peer: crate::sync::protocol::PeerKey,
    msg: RepoSyncRpcMessage,
) {
    match msg {
        RepoSyncRpcMessage::GetDocsFull(req) => {
            let WithChannels { inner, tx, .. } = req;
            let out = (async {
                ensure_known_peer(sync_store, &peer).await?;
                let mut allowed_partitions = big_repo
                    .partition_store
                    .list_partitions()
                    .await
                    .map_err(map_repo_err)?
                    .partitions;
                allowed_partitions
                    .retain(|part| access_policy.can_access_partition(&peer, &part.partition_id));
                let allowed_partition_ids = allowed_partitions
                    .into_iter()
                    .map(|part| part.partition_id)
                    .collect::<Vec<_>>();
                let docs = big_repo
                    .get_docs_full_in_partitions(&inner.req.doc_ids, &allowed_partition_ids)
                    .await
                    .map_err(map_repo_err)?;
                Ok::<_, PartitionSyncError>(GetDocsFullResponse { docs })
            })
            .await;
            tx.send(out).await.inspect_err(|_| warn!(ERROR_CALLER)).ok();
        }
    }
}

async fn ensure_known_peer(
    sync_store: &SyncStoreHandle,
    peer: &crate::sync::protocol::PeerKey,
) -> Result<(), PartitionSyncError> {
    let known = sync_store
        .is_peer_allowed(Arc::clone(peer))
        .await
        .map_err(map_repo_err)?;
    if known {
        return Ok(());
    }
    Err(PartitionSyncError::Internal {
        message: format!("peer {peer:?} is not allowed in repo rpc"),
    })
}

fn map_repo_err(err: eyre::Report) -> PartitionSyncError {
    PartitionSyncError::Internal {
        message: err.to_string(),
    }
}
