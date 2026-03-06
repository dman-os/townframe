use super::*;

pub(super) struct BlobSyncWorkerStopToken {
    cancel_token: CancellationToken,
    join_handle: tokio::task::JoinHandle<()>,
}

impl BlobSyncWorkerStopToken {
    pub async fn stop(self) -> Res<()> {
        self.cancel_token.cancel();
        utils_rs::wait_on_handle_with_timeout(self.join_handle, Duration::from_secs(2)).await?;
        Ok(())
    }
}

pub fn spawn_blob_sync_worker(
    hash: String,
    peers: Vec<EndpointId>,
    cancel_token: CancellationToken,
    msg_tx: mpsc::UnboundedSender<Msg>,
    blobs_repo: Arc<BlobsRepo>,
    endpoint: iroh::Endpoint,
    retry: RetryState,
) -> Res<BlobSyncWorkerStopToken> {
    let stop_cancel_token = cancel_token.clone();
    let worker = BlobSyncWorker {
        hash,
        peers,
        cancel_token,
        msg_tx,
        blobs_repo,
        endpoint,
        retry,
    };
    let fut = async move {
        if worker.blobs_repo.has_hash(&worker.hash).await.unwrap_or(false) {
            worker.mark_synced(None);
            return;
        }

        let iroh_hash = match crate::blobs::daybook_hash_to_iroh_hash(&worker.hash) {
            Ok(hash) => hash,
            Err(_) => {
                worker.request_backoff(Duration::from_secs(5));
                return;
            }
        };
        let downloader = worker.blobs_repo.iroh_store().downloader(&worker.endpoint);

        for endpoint_id in worker.peers.clone() {
            if worker.cancel_token.is_cancelled() {
                return;
            }
            let res = tokio::select! {
                _ = worker.cancel_token.cancelled() => return,
                res = async { downloader.download(iroh_hash, vec![endpoint_id]).await } => res,
            };
            if res.is_err() {
                continue;
            }
            if worker
                .blobs_repo
                .put_from_store(&worker.hash)
                .await
                .is_ok()
            {
                worker.mark_synced(Some(endpoint_id));
                return;
            }
        }

        worker.request_backoff(Duration::from_secs(2));
    };
    let join_handle = tokio::spawn(fut.instrument(tracing::info_span!("BlobSyncWorker task")));
    Ok(BlobSyncWorkerStopToken {
        cancel_token: stop_cancel_token,
        join_handle,
    })
}

struct BlobSyncWorker {
    hash: String,
    peers: Vec<EndpointId>,
    cancel_token: CancellationToken,
    msg_tx: mpsc::UnboundedSender<Msg>,
    blobs_repo: Arc<BlobsRepo>,
    endpoint: iroh::Endpoint,
    retry: RetryState,
}

impl BlobSyncWorker {
    fn request_backoff(&self, delay: Duration) {
        self.msg_tx
            .send(Msg::BlobSyncRequestBackoff {
                hash: self.hash.clone(),
                delay,
                previous_attempt_no: self.retry.attempt_no,
                previous_backoff: self.retry.last_backoff,
                previous_attempt_at: self.retry.last_attempt_at,
            })
            .expect("FullSyncWorker went down without cleaning boot_blob_sync_worker");
    }

    fn mark_synced(&self, endpoint_id: Option<EndpointId>) {
        self.msg_tx
            .send(Msg::BlobSyncMarkedSynced {
                hash: self.hash.clone(),
                endpoint_id,
            })
            .expect("FullSyncWorker went down without cleaning boot_blob_sync_worker");
    }
}
