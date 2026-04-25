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
    target: DocSyncTarget,
    big_repo: SharedBigRepo,
    cancel_token: CancellationToken,
    msg_tx: mpsc::UnboundedSender<Msg>,
    retry: RetryState,
) -> Res<DocSyncWorkerStopToken> {
    let stop_cancel_token = cancel_token.clone();
    let worker = DocSyncWorker {
        doc_id,
        target,
        big_repo,
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
    target: DocSyncTarget,
    big_repo: SharedBigRepo,
    msg_tx: mpsc::UnboundedSender<Msg>,
    retry: RetryState,
}

#[derive(Clone)]
pub struct DocSyncTarget {
    pub endpoint_id: EndpointId,
    pub connection: am_utils_rs::repo::BigRepoConnection,
}

impl DocSyncWorker {
    async fn run(self, cancel_token: CancellationToken) {
        let res: Res<()> = async {
            if self.big_repo.get_doc(&self.doc_id).await?.is_none() {
                self.handle_missing_doc();
                return Ok(());
            }

            let outcome = tokio::select! {
                _ = cancel_token.cancelled() => return Ok(()),
                out = self.target.connection.sync_doc_with_peer(
                    self.doc_id,
                    false,
                    Some(Duration::from_secs(10)),
                ) => out?,
            };

            self.msg_tx
                .send(Msg::DocSyncCompleted {
                    doc_id: self.doc_id,
                    endpoint_id: self.target.endpoint_id,
                    outcome,
                })
                .expect("FullSyncWorker went down without cleaning doc sync worker");
            Ok(())
        }
        .await;

        if let Err(err) = res {
            warn!(
                doc_id = %self.doc_id,
                endpoint_id = ?self.target.endpoint_id,
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
                endpoint_id: self.target.endpoint_id,
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
                doc_id: self.doc_id,
            })
            .expect("FullSyncWorker went down without cleaning boot_doc_sync_worker");
    }
}
