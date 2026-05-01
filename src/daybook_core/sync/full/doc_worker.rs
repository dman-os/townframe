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
        target,
        big_repo,
        msg_tx,
        retry,
    };

    let fut = async move {
        worker.run().await;
    }
    .instrument(tracing::info_span!("DocSyncWorker task"));

    let task_handle = task_set.spawn(fut).map_err(|_| ferr!("task set aborted"))?;

    Ok(DocSyncWorkerStopToken { task_handle })
}

struct DocSyncWorker {
    doc_id: DocumentId,
    target: DocSyncTarget,
    big_repo: SharedBigRepo,
    msg_tx: mpsc::UnboundedSender<Msg>,
    retry: RetryState,
}

impl DocSyncWorker {
    async fn run(self) {
        match self.target.clone() {
            DocSyncTarget::Sync { peer_id, connection } => {
                let res: Res<()> = async {
                    if self.big_repo.get_doc(&self.doc_id).await?.is_none() {
                        self.msg_tx
                            .send(Msg::DocSyncMissingLocal {
                                doc_id: self.doc_id.clone(),
                            })
                            .ok();
                        return Ok(());
                    }

                    let outcome = connection
                        .sync_doc_with_peer(self.doc_id.clone(), false, Some(Duration::from_secs(10)))
                        .await?;

                    self.msg_tx
                        .send(Msg::DocSyncCompleted {
                            doc_id: self.doc_id.clone(),
                            peer_id,
                            outcome,
                        })
                        .ok();
                    Ok(())
                }
                .await;

                if let Err(err) = res {
                    warn!(
                        doc_id = %self.doc_id,
                        peer_id = ?peer_id,
                        ?err,
                        "doc sync worker failed"
                    );
                    self.msg_tx
                        .send(Msg::DocSyncBackoff {
                            doc_id: self.doc_id.clone(),
                            peer_id,
                            delay: Duration::from_millis(500),
                            previous_attempt_no: self.retry.attempt_no,
                            previous_backoff: self.retry.last_backoff,
                            previous_attempt_at: self.retry.last_attempt_at,
                        })
                        .ok();
                }
            }
            DocSyncTarget::Import {
                peer_id,
                iroh_endpoint,
            } => {
                let doc_id_string = self.doc_id.to_string();
                let res: Res<()> = async {
                    if self.big_repo.get_doc(&self.doc_id).await?.is_some() {
                        self.msg_tx
                            .send(Msg::ImportDocCompleted {
                                doc_id: self.doc_id.clone(),
                                peer_id,
                                outcome: ImportDocOutcome::LocalPresent,
                            })
                            .ok();
                        return Ok(());
                    }
                    let rpc_client = irpc_iroh::client::<am_utils_rs::repo::rpc::RepoSyncRpc>(
                        iroh_endpoint.clone(),
                        iroh::EndpointAddr::new(peer_id.into()),
                        crate::sync::REPO_SYNC_ALPN,
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
                            warn!(%doc_id_string, endpoint_id = ?peer_id, ?err, "repo GetDocsFull rejected in import worker");
                            self.msg_tx
                                .send(Msg::ImportDocBackoff {
                                    doc_id: self.doc_id.clone(),
                                    peer_id,
                                    delay: Duration::from_secs(2),
                                    previous_attempt_no: self.retry.attempt_no,
                                    previous_backoff: self.retry.last_backoff,
                                    previous_attempt_at: self.retry.last_attempt_at,
                                })
                                .ok();
                            return Ok(());
                        }
                        Err(err) => {
                            warn!(%doc_id_string, endpoint_id = ?peer_id, ?err, "repo GetDocsFull rpc failed in import worker");
                            self.msg_tx
                                .send(Msg::ImportDocBackoff {
                                    doc_id: self.doc_id.clone(),
                                    peer_id,
                                    delay: Duration::from_secs(2),
                                    previous_attempt_no: self.retry.attempt_no,
                                    previous_backoff: self.retry.last_backoff,
                                    previous_attempt_at: self.retry.last_attempt_at,
                                })
                                .ok();
                            return Ok(());
                        }
                    };

                    let Some(full_doc) = response
                        .docs
                        .into_iter()
                        .find(|doc| doc.doc_id == doc_id_string)
                    else {
                        self.msg_tx
                            .send(Msg::ImportDocCompleted {
                                doc_id: self.doc_id.clone(),
                                peer_id,
                                outcome: ImportDocOutcome::MissingOnRemote,
                            })
                            .ok();
                        return Ok(());
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
                            self.msg_tx
                                .send(Msg::ImportDocBackoff {
                                    doc_id: self.doc_id.clone(),
                                    peer_id,
                                    delay: Duration::from_secs(2),
                                    previous_attempt_no: self.retry.attempt_no,
                                    previous_backoff: self.retry.last_backoff,
                                    previous_attempt_at: self.retry.last_attempt_at,
                                })
                                .ok();
                            return Ok(());
                        }
                    };

                    if self.big_repo.get_doc(&self.doc_id).await?.is_some() {
                        self.msg_tx
                            .send(Msg::ImportDocCompleted {
                                doc_id: self.doc_id.clone(),
                                peer_id,
                                outcome: ImportDocOutcome::LocalPresent,
                            })
                            .ok();
                        return Ok(());
                    }

                    match self.big_repo.put_doc(self.doc_id.clone(), loaded).await {
                        Ok(_) => {
                            self.msg_tx
                                .send(Msg::ImportDocCompleted {
                                    doc_id: self.doc_id.clone(),
                                    peer_id,
                                    outcome: ImportDocOutcome::Imported,
                                })
                                .ok();
                        }
                        Err(err) => {
                            if self.big_repo.get_doc(&self.doc_id).await?.is_some() {
                                self.msg_tx
                                    .send(Msg::ImportDocCompleted {
                                        doc_id: self.doc_id.clone(),
                                        peer_id,
                                        outcome: ImportDocOutcome::LocalPresent,
                                    })
                                    .ok();
                                return Ok(());
                            }
                            warn!(%doc_id_string, endpoint_id = ?peer_id, ?err, "local import failed in import worker");
                            self.msg_tx
                                .send(Msg::ImportDocBackoff {
                                    doc_id: self.doc_id.clone(),
                                    peer_id,
                                    delay: Duration::from_secs(2),
                                    previous_attempt_no: self.retry.attempt_no,
                                    previous_backoff: self.retry.last_backoff,
                                    previous_attempt_at: self.retry.last_attempt_at,
                                })
                                .ok();
                        }
                    }
                    Ok(())
                }
                .await;

                if let Err(err) = res {
                    warn!(
                        doc_id = %self.doc_id,
                        peer_id = ?peer_id,
                        ?err,
                        "import sync worker failed"
                    );
                    self.msg_tx
                        .send(Msg::ImportDocBackoff {
                            doc_id: self.doc_id.clone(),
                            peer_id,
                            delay: Duration::from_secs(2),
                            previous_attempt_no: self.retry.attempt_no,
                            previous_backoff: self.retry.last_backoff,
                            previous_attempt_at: self.retry.last_attempt_at,
                        })
                        .ok();
                }
            }
        }
    }
}
