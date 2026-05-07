use super::*;

pub(super) struct DocSyncWorkerStopToken {
    task_handle: utils_rs::TaskHandle,
}

impl DocSyncWorkerStopToken {
    pub async fn stop(self) {
        self.task_handle
            .join(Duration::from_secs(2))
            .await
            .inspect_err(|err| error!("error joining doc sync worker: {err:?}"))
            .ok();
    }
}

#[derive(Debug, Clone)]
pub(super) enum SyncDocOutcome {
    Synced,
    Imported { heads: ChangeHashSet },
}

#[tracing::instrument(skip_all, fields(%doc_id))]
pub fn spawn_doc_sync_worker(
    doc_id: DocumentId,
    peer_id: PeerId,
    connection: am_utils_rs::repo::BigRepoConnection,
    iroh_endpoint: iroh::Endpoint,
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
        let msg = match worker.run(peer_id, connection, iroh_endpoint).await {
            Ok(outcome) => Msg::DocSyncCompleted {
                doc_id: worker.doc_id,
                peer_id,
                outcome,
            },
            Err(err) => {
                error!(
                    doc_id = %worker.doc_id,
                    peer_id = ?peer_id,
                    ?err,
                    "doc sync worker failed"
                );
                Msg::DocSyncBackoff {
                    doc_id: worker.doc_id,
                    peer_id,
                    delay: Duration::from_millis(500),
                    previous_retry_state: worker.retry,
                }
            }
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
    async fn run(
        &self,
        peer_id: PeerId,
        connection: am_utils_rs::repo::BigRepoConnection,
        iroh_endpoint: iroh::Endpoint,
    ) -> Res<SyncDocOutcome> {
        if self.big_repo.get_doc(&self.doc_id).await?.is_some() {
            let outcome = connection
                .sync_with_peer(self.doc_id, Some(Duration::from_secs(10)))
                .await?;
            match outcome {
                am_utils_rs::repo::SyncDocOutcome::Success => {
                    return Ok(SyncDocOutcome::Synced);
                }
                am_utils_rs::repo::SyncDocOutcome::NotFoundOrUnauthorized
                | am_utils_rs::repo::SyncDocOutcome::TransportError
                | am_utils_rs::repo::SyncDocOutcome::IoError => {
                    eyre::bail!("error during big_repo sync: {outcome:?}")
                }
            }
        }

        let rpc_client = irpc_iroh::client::<am_utils_rs::repo::rpc::RepoSyncRpc>(
            iroh_endpoint.clone(),
            iroh::EndpointAddr::new(peer_id.into()),
            crate::sync::REPO_SYNC_ALPN,
        );
        let doc_id_string = self.doc_id.to_string();
        let response = rpc_client
            .rpc(am_utils_rs::repo::rpc::GetDocsFullRpcReq {
                req: am_utils_rs::repo::rpc::GetDocsFullRequest {
                    doc_ids: vec![doc_id_string.clone()],
                },
            })
            .await
            .wrap_err("GetDocsFull rpc failure")?
            .wrap_err("GetDocsFull rejected")?;

        let full_doc = response
            .docs
            .into_iter()
            .find(|doc| doc.doc_id == doc_id_string)
            .ok_or_eyre("missing on remote")?;
        let loaded = automerge::Automerge::load(&full_doc.automerge_save)
            .wrap_err("invalid automerge payload from GetDocsFull")?;

        if self.big_repo.get_doc(&self.doc_id).await?.is_some() {
            let outcome = connection
                .sync_with_peer(self.doc_id, Some(Duration::from_secs(10)))
                .await?;
            match outcome {
                am_utils_rs::repo::SyncDocOutcome::Success => {
                    return Ok(SyncDocOutcome::Synced);
                }
                am_utils_rs::repo::SyncDocOutcome::NotFoundOrUnauthorized
                | am_utils_rs::repo::SyncDocOutcome::TransportError
                | am_utils_rs::repo::SyncDocOutcome::IoError => {
                    eyre::bail!("error during big_repo sync: {outcome:?}")
                }
            }
        }

        let heads = ChangeHashSet(loaded.get_heads().into());
        match self.big_repo.put_doc(self.doc_id, loaded).await {
            Ok(_) => Ok(SyncDocOutcome::Imported { heads }),
            Err(am_utils_rs::repo::PutDocError::IdOccpuied { .. }) => {
                let outcome = connection
                    .sync_with_peer(self.doc_id, Some(Duration::from_secs(10)))
                    .await?;
                match outcome {
                    am_utils_rs::repo::SyncDocOutcome::Success => Ok(SyncDocOutcome::Synced),
                    am_utils_rs::repo::SyncDocOutcome::NotFoundOrUnauthorized
                    | am_utils_rs::repo::SyncDocOutcome::TransportError
                    | am_utils_rs::repo::SyncDocOutcome::IoError => {
                        eyre::bail!("error during big_repo sync: {outcome:?}")
                    }
                }
            }
            Err(err) => Err(err).wrap_err("put_doc failed"),
        }
    }
}
