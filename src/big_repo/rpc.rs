use crate::interlude::*;

use crate::SharedBigRepo;
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

#[derive(
    Debug, thiserror::Error, displaydoc::Display, Clone, serde::Serialize, serde::Deserialize,
)]
pub enum BigRepoRpcError {
    /// internal error: {message}
    Internal { message: String },
}

// NOTE: this is used over an 0rtt iroh irpc impl wihch is
// only safe when all the requests are idempotent. if this
// changes for our requests, amend the 0rtt usage
#[rpc_requests(message = RepoSyncRpcMessage)]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum RepoSyncRpc {
    #[rpc(tx = channel::oneshot::Sender<Result<GetDocsFullResponse, BigRepoRpcError>>)]
    GetDocsFull(GetDocsFullRpcReq),
}

pub struct RepoRpcHandle {
    rpc_tx: mpsc::Sender<(PeerId, RepoSyncRpcMessage)>,
}

impl RepoRpcHandle {
    pub fn local_sender(&self) -> mpsc::Sender<(PeerId, RepoSyncRpcMessage)> {
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
    big_sync_host: Arc<big_sync::Ctx>,
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
                        handle_rpc_message(&big_repo, &big_sync_host, peer, msg).await;
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
    big_sync_host: &Arc<big_sync::Ctx>,
    peer: PeerId,
    msg: RepoSyncRpcMessage,
) {
    match msg {
        RepoSyncRpcMessage::GetDocsFull(req) => {
            let WithChannels { inner, tx, .. } = req;
            let out = (async {
                ensure_known_peer(&big_sync_host, &peer).await?;
                let docs = big_repo
                    .get_docs_full(&inner.req.doc_ids)
                    .await
                    .map_err(map_repo_err)?;
                Ok::<_, BigRepoRpcError>(GetDocsFullResponse { docs })
            })
            .await;
            tx.send(out).await.inspect_err(|_| warn!(ERROR_CALLER)).ok();
        }
    }
}

async fn ensure_known_peer(
    _big_sync_host: &Arc<big_sync::Ctx>,
    _peer: &PeerId,
) -> Result<(), BigRepoRpcError> {
    // TODO: permissioning
    Ok(())
}

fn map_repo_err(err: eyre::Report) -> BigRepoRpcError {
    BigRepoRpcError::Internal {
        message: err.to_string(),
    }
}
