use super::*;

pub(super) struct DocSyncWorkerStopToken {
    task_handle: utils_rs::TaskHandle,
}

impl DocSyncWorkerStopToken {
    pub async fn stop(self) -> Res<()> {
        self.task_handle.abort();
        tokio::time::timeout(Duration::from_secs(2), self.task_handle.join())
            .await
            .ok();
        Ok(())
    }
}

#[derive(Clone)]
pub(super) enum DocSyncTarget {
    Sync {
        peer_id: PeerId,
        connection: am_utils_rs::repo::BigRepoConnection,
    },
    Import {
        peer_id: PeerId,
        iroh_endpoint: iroh::Endpoint,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ImportDocOutcome {
    Imported,
    LocalPresent,
    MissingOnRemote,
}

#[tracing::instrument(skip_all, fields(%doc_id))]
pub fn spawn_doc_sync_worker(
    doc_id: DocumentId,
    target: DocSyncTarget,
    big_repo: SharedBigRepo,
    msg_tx: mpsc::UnboundedSender<Msg>,
    retry: RetryState,
    task_set: &utils_rs::AbortableJoinSet,
) -> Res<DocSyncWorkerStopToken> {
    let worker = DocSyncWorker {
        doc_id,
        big_repo,
        retry,
    };

    let fut = async move {
        let msg = match target {
            DocSyncTarget::Sync {
                peer_id,
                connection,
            } => match worker.sync_doc(peer_id, connection).await {
                Ok(msg) => msg,
                Err(err) => {
                    error!(
                        doc_id = %worker.doc_id,
                        peer_id = ?peer_id,
                        ?err,
                        "doc sync worker failed"
                    );
                    Msg::DocSyncBackoff {
                        doc_id: worker.doc_id.clone(),
                        peer_id,
                        delay: Duration::from_millis(500),
                        previous_attempt_no: worker.retry.attempt_no,
                        previous_backoff: worker.retry.last_backoff,
                        previous_attempt_at: worker.retry.last_attempt_at,
                    }
                }
            },
            DocSyncTarget::Import {
                peer_id,
                iroh_endpoint,
            } => match worker.import_doc(peer_id, iroh_endpoint).await {
                Ok(msg) => msg,
                Err(err) => {
                    error!(
                        doc_id = %worker.doc_id,
                        peer_id = ?peer_id,
                        ?err,
                        "import sync worker failed"
                    );
                    Msg::ImportDocBackoff {
                        doc_id: worker.doc_id.clone(),
                        peer_id,
                        delay: Duration::from_secs(2),
                        previous_attempt_no: worker.retry.attempt_no,
                        previous_backoff: worker.retry.last_backoff,
                        previous_attempt_at: worker.retry.last_attempt_at,
                    }
                }
            },
        };
        msg_tx.send(msg).inspect_err(|_| warn!(ERROR_CALLER)).ok();
    }
    .in_current_span();

    let task_handle = task_set.spawn(fut).map_err(|_| ferr!("task set aborted"))?;

    Ok(DocSyncWorkerStopToken { task_handle })
}

struct DocSyncWorker {
    doc_id: DocumentId,
    big_repo: SharedBigRepo,
    retry: RetryState,
}

impl DocSyncWorker {
    async fn sync_doc(
        &self,
        peer_id: PeerId,
        connection: am_utils_rs::repo::BigRepoConnection,
    ) -> Res<Msg> {
        if self.big_repo.get_doc(&self.doc_id).await?.is_none() {
            return Ok(Msg::DocSyncMissingLocal {
                doc_id: self.doc_id.clone(),
            });
        }

        let outcome = connection
            .sync_with_peer(self.doc_id.clone(), Some(Duration::from_secs(10)))
            .await?;

        Ok(Msg::DocSyncCompleted {
            doc_id: self.doc_id.clone(),
            peer_id,
            outcome,
        })
    }

    async fn import_doc(&self, peer_id: PeerId, iroh_endpoint: iroh::Endpoint) -> Res<Msg> {
        if self.big_repo.get_doc(&self.doc_id).await?.is_some() {
            return Ok(Msg::ImportDocCompleted {
                doc_id: self.doc_id.clone(),
                peer_id,
                outcome: ImportDocOutcome::LocalPresent,
            });
        }
        let rpc_client = irpc_iroh::client::<am_utils_rs::repo::rpc::RepoSyncRpc>(
            iroh_endpoint.clone(),
            iroh::EndpointAddr::new(peer_id.into()),
            crate::sync::REPO_SYNC_ALPN,
        );
        let doc_id_string = self.doc_id.to_string();
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
                warn!(%doc_id_string, endpoint_id = ?peer_id, ?err, "repo GetDocsFull rejected in import worker");
                return Ok(Msg::ImportDocBackoff {
                    doc_id: self.doc_id.clone(),
                    peer_id,
                    delay: Duration::from_secs(2),
                    previous_attempt_no: self.retry.attempt_no,
                    previous_backoff: self.retry.last_backoff,
                    previous_attempt_at: self.retry.last_attempt_at,
                });
            }
            Err(err) => {
                warn!(%doc_id_string, endpoint_id = ?peer_id, ?err, "repo GetDocsFull rpc failed in import worker");
                return Ok(Msg::ImportDocBackoff {
                    doc_id: self.doc_id.clone(),
                    peer_id,
                    delay: Duration::from_secs(2),
                    previous_attempt_no: self.retry.attempt_no,
                    previous_backoff: self.retry.last_backoff,
                    previous_attempt_at: self.retry.last_attempt_at,
                });
            }
        };

        let Some(full_doc) = response
            .docs
            .into_iter()
            .find(|doc| doc.doc_id == doc_id_string)
        else {
            return Ok(Msg::ImportDocCompleted {
                doc_id: self.doc_id.clone(),
                peer_id,
                outcome: ImportDocOutcome::MissingOnRemote,
            });
        };
        let loaded = match automerge::Automerge::load(&full_doc.automerge_save) {
            Ok(loaded) => loaded,
            Err(err) => {
                warn!(
                    doc_id = full_doc.doc_id,
                    endpoint_id = ?peer_id,
                    ?err,
                    "invalid automerge payload in import worker"
                );
                return Ok(Msg::ImportDocBackoff {
                    doc_id: self.doc_id.clone(),
                    peer_id,
                    delay: Duration::from_secs(2),
                    previous_attempt_no: self.retry.attempt_no,
                    previous_backoff: self.retry.last_backoff,
                    previous_attempt_at: self.retry.last_attempt_at,
                });
            }
        };

        if self.big_repo.get_doc(&self.doc_id).await?.is_some() {
            return Ok(Msg::ImportDocCompleted {
                doc_id: self.doc_id.clone(),
                peer_id,
                outcome: ImportDocOutcome::LocalPresent,
            });
        }

        match self.big_repo.put_doc(self.doc_id.clone(), loaded).await {
            Ok(_) => Ok(Msg::ImportDocCompleted {
                doc_id: self.doc_id.clone(),
                peer_id,
                outcome: ImportDocOutcome::Imported,
            }),
            Err(err) => {
                if self.big_repo.get_doc(&self.doc_id).await?.is_some() {
                    return Ok(Msg::ImportDocCompleted {
                        doc_id: self.doc_id.clone(),
                        peer_id,
                        outcome: ImportDocOutcome::LocalPresent,
                    });
                }
                warn!(%doc_id_string, endpoint_id = ?peer_id, ?err, "local import failed in import worker");
                Ok(Msg::ImportDocBackoff {
                    doc_id: self.doc_id.clone(),
                    peer_id,
                    delay: Duration::from_secs(2),
                    previous_attempt_no: self.retry.attempt_no,
                    previous_backoff: self.retry.last_backoff,
                    previous_attempt_at: self.retry.last_attempt_at,
                })
            }
        }
    }
}
