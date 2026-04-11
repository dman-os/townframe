use super::*;

pub(super) struct DocSyncWorkerStopToken {
    cancel_token: CancellationToken,
    join_handle: tokio::task::JoinHandle<()>,
}

impl DocSyncWorkerStopToken {
    pub async fn stop(self) -> Res<()> {
        self.cancel_token.cancel();
        utils_rs::wait_on_handle_with_timeout(self.join_handle, Duration::from_secs(2)).await?;
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn spawn_doc_sync_worker(
    doc_id: DocumentId,
    endpoint_id: EndpointId,
    endpoint_addr: iroh::EndpointAddr,
    conn_id: BigRepoConnectionId,
    peer_key: PeerKey,
    local_peer_key: PeerKey,
    big_repo: SharedBigRepo,
    iroh_endpoint: iroh::Endpoint,
    cancel_token: CancellationToken,
    msg_tx: mpsc::UnboundedSender<Msg>,
    retry: RetryState,
) -> Res<DocSyncWorkerStopToken> {
    let stop_cancel_token = cancel_token.clone();
    let worker = DocSyncWorker {
        doc_id,
        endpoint_id,
        endpoint_addr,
        conn_id,
        peer_key,
        local_peer_key,
        big_repo,
        iroh_endpoint,
        msg_tx,
        retry,
    };

    let join_handle = tokio::spawn(
        async move {
            worker.run(cancel_token).await;
        }
        .instrument(tracing::info_span!("DocSyncWorker task")),
    );

    Ok(DocSyncWorkerStopToken {
        cancel_token: stop_cancel_token,
        join_handle,
    })
}

struct DocSyncWorker {
    doc_id: DocumentId,
    endpoint_id: EndpointId,
    endpoint_addr: iroh::EndpointAddr,
    conn_id: BigRepoConnectionId,
    peer_key: PeerKey,
    local_peer_key: PeerKey,
    big_repo: SharedBigRepo,
    iroh_endpoint: iroh::Endpoint,
    msg_tx: mpsc::UnboundedSender<Msg>,
    retry: RetryState,
}

impl DocSyncWorker {
    async fn run(self, cancel_token: CancellationToken) {
        let res: Res<()> = async {
            let Some(local_handle) = self.big_repo.find_doc_handle(&self.doc_id).await? else {
                self.handle_missing_doc();
                return Ok(());
            };

            let rpc_client = irpc_iroh::client::<am_utils_rs::repo::rpc::RepoSyncRpc>(
                self.iroh_endpoint.clone(),
                self.endpoint_addr.clone(),
                REPO_SYNC_ALPN,
            );

            let req = am_utils_rs::repo::rpc::GetDocsFullRpcReq {
                peer: self.local_peer_key.clone(),
                req: am_utils_rs::repo::rpc::GetDocsFullRequest {
                    doc_ids: vec![self.doc_id.to_string()],
                },
            };
            let response = tokio::select! {
                _ = cancel_token.cancelled() => return Ok(()),
                out = rpc_client.rpc(req) => out,
            }
            .wrap_err("repo get docs full rpc failed")?
            .map_err(|err| ferr!("repo get docs full rejected: {err:?}"))?;

            let Some(full_doc) = response
                .docs
                .into_iter()
                .find(|item| item.doc_id == self.doc_id.to_string())
            else {
                self.handle_timeout();
                return Ok(());
            };

            let mut remote_doc = automerge::Automerge::load(&full_doc.automerge_save)
                .map_err(|err| ferr!("invalid remote automerge payload: {err}"))?;

            local_handle
                .with_document(|local_doc| -> Res<()> {
                    local_doc.merge(&mut remote_doc)?;
                    Ok(())
                })
                .await
                .wrap_err("failed applying remote doc merge")??;

            let heads = local_handle.with_document_read(|doc| doc.get_heads()).await;
            let mut diff = HashMap::new();
            diff.insert(
                self.conn_id,
                BigRepoPeerDocState {
                    shared_heads: Some(heads.clone()),
                    their_heads: Some(heads),
                },
            );
            self.msg_tx
                .send(Msg::DocPeerStateViewUpdated {
                    doc_id: self.doc_id.clone(),
                    diff,
                })
                .expect("FullSyncWorker went down without cleaning doc sync worker");
            Ok(())
        }
        .await;

        if let Err(err) = res {
            warn!(
                doc_id = %self.doc_id,
                endpoint_id = ?self.endpoint_id,
                peer_key = %self.peer_key,
                ?err,
                "doc sync worker failed"
            );
            self.handle_timeout();
        }
    }

    fn handle_timeout(&self) {
        self.msg_tx
            .send(Msg::DocSyncRequestBackoff {
                doc_id: self.doc_id.clone(),
                delay: Duration::from_millis(500),
                previous_attempt_no: self.retry.attempt_no,
                previous_backoff: self.retry.last_backoff,
                previous_attempt_at: self.retry.last_attempt_at,
            })
            .expect("FullSyncWorker went down without cleaning boot_doc_sync_worker");
    }

    fn handle_missing_doc(&self) {
        self.msg_tx
            .send(Msg::DocSyncMissingLocal {
                doc_id: self.doc_id.clone(),
            })
            .expect("FullSyncWorker went down without cleaning boot_doc_sync_worker");
    }
}
