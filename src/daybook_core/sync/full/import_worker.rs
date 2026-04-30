use super::*;

pub(super) struct ImportSyncWorkerStopToken {
    task_handle: utils_rs::TaskHandle,
}

impl ImportSyncWorkerStopToken {
    pub async fn stop(self) -> Res<()> {
        self.task_handle.abort();
        tokio::time::timeout(Duration::from_secs(2), self.task_handle.join())
            .await
            .ok();
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ImportDocOutcome {
    Imported,
    LocalPresent,
    MissingOnRemote,
}

#[derive(Clone)]
pub(super) struct ImportSyncTarget {
    pub peer_id: PeerId,
}

pub(super) fn spawn_import_sync_worker(
    doc_id: DocumentId,
    target: ImportSyncTarget,
    local_peer_key: PeerKey,
    msg_tx: mpsc::UnboundedSender<Msg>,
    big_repo: SharedBigRepo,
    iroh_endpoint: iroh::Endpoint,
    retry: RetryState,
    task_set: &utils_rs::AbortableJoinSet,
) -> Res<ImportSyncWorkerStopToken> {
    let worker = ImportSyncWorker {
        doc_id,
        target,
        local_peer_key,
        msg_tx,
        big_repo,
        iroh_endpoint,
        retry,
    };
    let fut = async move {
        worker.run().await;
    }
    .instrument(tracing::info_span!("ImportSyncWorker task"));
    let task_handle = task_set.spawn(fut).map_err(|_| ferr!("task set aborted"))?;
    Ok(ImportSyncWorkerStopToken { task_handle })
}

struct ImportSyncWorker {
    doc_id: DocumentId,
    target: ImportSyncTarget,
    local_peer_key: PeerKey,
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
        let target = &self.target;
        let rpc_client = irpc_iroh::client::<am_utils_rs::repo::rpc::RepoSyncRpc>(
            self.iroh_endpoint.clone(),
            iroh::EndpointAddr::new(target.peer_id.into()),
            REPO_SYNC_ALPN,
        );
        let rpc_response = rpc_client
            .rpc(am_utils_rs::repo::rpc::GetDocsFullRpcReq {
                req: am_utils_rs::repo::rpc::GetDocsFullRequest {
                    doc_ids: vec![doc_id_string.clone()],
                },
            })
            .await;
        let response = match rpc_response {
            Ok(Ok(response)) => response,
            Ok(Err(err)) => {
                warn!(%doc_id_string, endpoint_id = ?target.peer_id, ?err, "repo GetDocsFull rejected in import worker");
                self.request_backoff(Duration::from_secs(2));
                return;
            }
            Err(err) => {
                warn!(%doc_id_string, endpoint_id = ?target.peer_id, ?err, "repo GetDocsFull rpc failed in import worker");
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
                    endpoint_id = ?target.peer_id,
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

        match self.big_repo.put_doc(self.doc_id, loaded).await {
            Ok(_) => self.complete(ImportDocOutcome::Imported),
            Err(err) => {
                if self.local_contains().await {
                    self.complete(ImportDocOutcome::LocalPresent);
                    return;
                }
                warn!(%doc_id_string, endpoint_id = ?target.peer_id, ?err, "local import failed in import worker");
                self.request_backoff(Duration::from_secs(2));
            }
        }
    }

    async fn local_contains(&self) -> bool {
        match self.big_repo.get_doc(&self.doc_id).await {
            Ok(has) => has.is_some(),
            Err(err) => {
                warn!(%self.doc_id, ?err, "get_doc failed in import worker");
                false
            }
        }
    }

    fn complete(&self, outcome: ImportDocOutcome) {
        self.msg_tx
            .send(Msg::ImportDocCompleted {
                doc_id: self.doc_id.clone(),
                peer_id: self.target.peer_id,
                outcome,
            })
            .ok();
    }

    fn request_backoff(&self, delay: Duration) {
        self.msg_tx
            .send(Msg::ImportDocBackoff {
                doc_id: self.doc_id.clone(),
                peer_id: self.target.peer_id,
                delay,
                previous_attempt_no: self.retry.attempt_no,
                previous_backoff: self.retry.last_backoff,
                previous_attempt_at: self.retry.last_attempt_at,
            })
            .ok();
    }
}
