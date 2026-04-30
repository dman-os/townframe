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

#[derive(Clone)]
pub struct DocSyncTarget {
    pub peer_id: PeerId,
    pub connection: am_utils_rs::repo::BigRepoConnection,
}

impl DocSyncWorker {
    async fn run(self) {
        let res: Res<()> = async {
            if self.big_repo.get_doc(&self.doc_id).await?.is_none() {
                self.handle_missing_doc();
                return Ok(());
            }

            let outcome = self
                .target
                .connection
                .sync_doc_with_peer(self.doc_id, false, Some(Duration::from_secs(10)))
                .await?;

            self.msg_tx
                .send(Msg::DocSyncCompleted {
                    doc_id: self.doc_id,
                    peer_id: self.target.peer_id,
                    outcome,
                })
                .ok();
            Ok(())
        }
        .await;

        if let Err(err) = res {
            warn!(
                doc_id = %self.doc_id,
                peer_id = ?self.target.peer_id,
                ?err,
                "doc sync worker failed"
            );
            self.handle_timeout();
        }
    }

    fn handle_timeout(&self) {
        self.msg_tx
            .send(Msg::DocSyncRequestBackoff {
                doc_id: self.doc_id,
                peer_id: self.target.peer_id,
                delay: Duration::from_millis(500),
                previous_attempt_no: self.retry.attempt_no,
                previous_backoff: self.retry.last_backoff,
                previous_attempt_at: self.retry.last_attempt_at,
            })
            .ok();
    }

    fn handle_missing_doc(&self) {
        self.msg_tx
            .send(Msg::DocSyncMissingLocal {
                doc_id: self.doc_id,
            })
            .ok();
    }
}
