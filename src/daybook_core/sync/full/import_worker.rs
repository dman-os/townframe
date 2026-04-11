use super::*;

pub(super) struct ImportSyncWorkerStopToken {
    cancel_token: CancellationToken,
    join_handle: tokio::task::JoinHandle<()>,
}

impl ImportSyncWorkerStopToken {
    pub async fn stop(self) -> Res<()> {
        self.cancel_token.cancel();
        utils_rs::wait_on_handle_with_timeout(self.join_handle, Duration::from_secs(2)).await?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ImportDocOutcome {
    Imported,
    LocalPresent,
    MissingOnRemote,
}

#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_import_sync_worker(
    doc_id: DocumentId,
    endpoint_id: EndpointId,
    endpoint_addr: iroh::EndpointAddr,
    local_peer_key: PeerKey,
    cancel_token: CancellationToken,
    msg_tx: mpsc::UnboundedSender<Msg>,
    big_repo: SharedBigRepo,
    iroh_endpoint: iroh::Endpoint,
    retry: RetryState,
) -> Res<ImportSyncWorkerStopToken> {
    let stop_cancel_token = cancel_token.clone();
    let worker = ImportSyncWorker {
        doc_id,
        endpoint_id,
        endpoint_addr,
        local_peer_key,
        cancel_token,
        msg_tx,
        big_repo,
        iroh_endpoint,
        retry,
    };
    let fut = async move {
        worker.run().await;
    };
    let join_handle = tokio::spawn(
        async move {
            fut.await;
        }
        .instrument(tracing::info_span!("ImportSyncWorker task")),
    );
    Ok(ImportSyncWorkerStopToken {
        cancel_token: stop_cancel_token,
        join_handle,
    })
}

struct ImportSyncWorker {
    doc_id: DocumentId,
    endpoint_id: EndpointId,
    endpoint_addr: iroh::EndpointAddr,
    local_peer_key: PeerKey,
    cancel_token: CancellationToken,
    msg_tx: mpsc::UnboundedSender<Msg>,
    big_repo: SharedBigRepo,
    iroh_endpoint: iroh::Endpoint,
    retry: RetryState,
}

impl ImportSyncWorker {
    async fn run(self) {
        let doc_id_string = self.doc_id.to_string();
        if self.local_contains().await {
            self.complete(ImportDocOutcome::LocalPresent);
            return;
        }
        let rpc_client = irpc_iroh::client::<am_utils_rs::repo::rpc::RepoSyncRpc>(
            self.iroh_endpoint.clone(),
            self.endpoint_addr.clone(),
            REPO_SYNC_ALPN,
        );
        let rpc_response = tokio::select! {
            _ = self.cancel_token.cancelled() => return,
            response = rpc_client.rpc(am_utils_rs::repo::rpc::GetDocsFullRpcReq {
                peer: self.local_peer_key.clone(),
                req: am_utils_rs::repo::rpc::GetDocsFullRequest {
                    doc_ids: vec![doc_id_string.clone()],
                },
            }) => response,
        };
        let response = match rpc_response {
            Ok(Ok(response)) => response,
            Ok(Err(err)) => {
                warn!(%doc_id_string, endpoint_id = ?self.endpoint_id, ?err, "repo GetDocsFull rejected in import worker");
                self.request_backoff(Duration::from_secs(2));
                return;
            }
            Err(err) => {
                warn!(%doc_id_string, endpoint_id = ?self.endpoint_id, ?err, "repo GetDocsFull rpc failed in import worker");
                self.request_backoff(Duration::from_secs(2));
                return;
            }
        };

        let Some(full_doc) = response
            .docs
            .into_iter()
            .find(|doc| doc.doc_id == doc_id_string)
        else {
            self.complete(ImportDocOutcome::MissingOnRemote);
            return;
        };
        let loaded = match automerge::Automerge::load(&full_doc.automerge_save) {
            Ok(loaded) => loaded,
            Err(err) => {
                warn!(
                    doc_id = full_doc.doc_id,
                    ?err,
                    "invalid automerge payload in import worker"
                );
                self.request_backoff(Duration::from_secs(2));
                return;
            }
        };

        if self.local_contains().await {
            self.complete(ImportDocOutcome::LocalPresent);
            return;
        }

        match self.big_repo.import_doc(self.doc_id.clone(), loaded).await {
            Ok(_) => self.complete(ImportDocOutcome::Imported),
            Err(err) => {
                if self.local_contains().await {
                    self.complete(ImportDocOutcome::LocalPresent);
                    return;
                }
                warn!(%doc_id_string, endpoint_id = ?self.endpoint_id, ?err, "local import failed in import worker");
                self.request_backoff(Duration::from_secs(2));
            }
        }
    }

    async fn local_contains(&self) -> bool {
        match self.big_repo.local_contains_document(&self.doc_id).await {
            Ok(has) => has,
            Err(err) => {
                warn!(%self.doc_id, ?err, "local_contains_document failed in import worker");
                false
            }
        }
    }

    fn complete(&self, outcome: ImportDocOutcome) {
        self.msg_tx
            .send(Msg::ImportDocCompleted {
                doc_id: self.doc_id.clone(),
                outcome,
            })
            .expect("FullSyncWorker went down without cleaning import worker");
    }

    fn request_backoff(&self, delay: Duration) {
        self.msg_tx
            .send(Msg::ImportDocBackoff {
                doc_id: self.doc_id.clone(),
                delay,
                previous_attempt_no: self.retry.attempt_no,
                previous_backoff: self.retry.last_backoff,
                previous_attempt_at: self.retry.last_attempt_at,
            })
            .expect("FullSyncWorker went down without cleaning import worker");
    }
}
