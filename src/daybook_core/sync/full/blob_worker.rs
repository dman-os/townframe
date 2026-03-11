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

#[allow(clippy::too_many_arguments)]
pub fn spawn_blob_sync_worker(
    hash: String,
    peers: Vec<EndpointId>,
    cancel_token: CancellationToken,
    msg_tx: mpsc::UnboundedSender<Msg>,
    sync_progress_tx: mpsc::Sender<SyncProgressMsg>,
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
        sync_progress_tx,
        blobs_repo,
        endpoint,
        retry,
    };
    let fut = async move {
        worker
            .send_progress(SyncProgressMsg::BlobWorkerStarted {
                partition: PartitionKey::DocBlobsFullSync,
                hash: worker.hash.clone(),
            })
            .await;

        let iroh_hash = match crate::blobs::daybook_hash_to_iroh_hash(&worker.hash) {
            Ok(hash) => hash,
            Err(err) => {
                tracing::warn!(?err, hash = %worker.hash, "invalid daybook blob hash");
                worker
                    .send_progress(SyncProgressMsg::BlobWorkerFinished {
                        partition: PartitionKey::DocBlobsFullSync,
                        hash: worker.hash.clone(),
                        success: false,
                        reason: "invalid hash".to_string(),
                    })
                    .await;
                return;
            }
        };

        let has_in_store = match worker.blobs_repo.iroh_store().blobs().has(iroh_hash).await {
            Ok(has) => has,
            Err(err) => {
                tracing::warn!(?err, hash = %worker.hash, "error checking iroh blob store");
                worker
                    .send_progress(SyncProgressMsg::BlobWorkerFinished {
                        partition: PartitionKey::DocBlobsFullSync,
                        hash: worker.hash.clone(),
                        success: false,
                        reason: "iroh store lookup failed".to_string(),
                    })
                    .await;
                worker.request_backoff(Duration::from_secs(2));
                return;
            }
        };

        let has_local_hash = match worker.blobs_repo.has_hash(&worker.hash).await {
            Ok(has) => has,
            Err(err) => {
                tracing::warn!(?err, hash = %worker.hash, "error checking local blob presence");
                worker
                    .send_progress(SyncProgressMsg::BlobWorkerFinished {
                        partition: PartitionKey::DocBlobsFullSync,
                        hash: worker.hash.clone(),
                        success: false,
                        reason: "local blob lookup failed".to_string(),
                    })
                    .await;
                worker.request_backoff(Duration::from_secs(2));
                return;
            }
        };

        if has_local_hash {
            worker.mark_synced(None);
            worker
                .send_progress(SyncProgressMsg::BlobWorkerFinished {
                    partition: PartitionKey::DocBlobsFullSync,
                    hash: worker.hash.clone(),
                    success: true,
                    reason: "already present in blobs repo".to_string(),
                })
                .await;
            return;
        }

        if has_in_store {
            worker
                .send_progress(SyncProgressMsg::BlobMaterializeStarted {
                    partition: PartitionKey::DocBlobsFullSync,
                    hash: worker.hash.clone(),
                })
                .await;
            match worker.blobs_repo.put_from_store(&worker.hash).await {
                Ok(_) => {
                    worker.mark_synced(None);
                    worker
                        .send_progress(SyncProgressMsg::BlobWorkerFinished {
                            partition: PartitionKey::DocBlobsFullSync,
                            hash: worker.hash.clone(),
                            success: true,
                            reason: "materialized from local iroh store".to_string(),
                        })
                        .await;
                }
                Err(err) => {
                    tracing::warn!(?err, hash = %worker.hash, "put_from_store failed from local iroh store");
                    worker
                        .send_progress(SyncProgressMsg::BlobWorkerFinished {
                            partition: PartitionKey::DocBlobsFullSync,
                            hash: worker.hash.clone(),
                            success: false,
                            reason: "put_from_store failed".to_string(),
                        })
                        .await;
                    worker.request_backoff(Duration::from_secs(2));
                }
            }
            return;
        }

        if worker.peers.is_empty() {
            worker
                .send_progress(SyncProgressMsg::BlobWorkerFinished {
                    partition: PartitionKey::DocBlobsFullSync,
                    hash: worker.hash.clone(),
                    success: false,
                    reason: "no peers available".to_string(),
                })
                .await;
            worker.request_backoff(Duration::from_secs(2));
            return;
        }

        let downloader = worker.blobs_repo.iroh_store().downloader(&worker.endpoint);
        let progress = downloader.download(iroh_hash, worker.peers.clone());
        let stream_res = progress.stream().await;
        let Ok(mut stream) = stream_res else {
            worker
                .send_progress(SyncProgressMsg::BlobWorkerFinished {
                    partition: PartitionKey::DocBlobsFullSync,
                    hash: worker.hash.clone(),
                    success: false,
                    reason: "failed to open download stream".to_string(),
                })
                .await;
            worker.request_backoff(Duration::from_secs(2));
            return;
        };

        let mut selected_endpoint: Option<EndpointId> = None;
        let mut saw_download_signal = false;
        use futures::StreamExt;
        while let Some(item) = tokio::select! {
            _ = worker.cancel_token.cancelled() => return,
            item = stream.next() => item,
        } {
            match item {
                iroh_blobs::api::downloader::DownloadProgressItem::TryProvider { id, .. } => {
                    saw_download_signal = true;
                    selected_endpoint = Some(id);
                    worker
                        .send_progress(SyncProgressMsg::BlobDownloadStarted {
                            endpoint_id: id,
                            partition: PartitionKey::DocBlobsFullSync,
                            hash: worker.hash.clone(),
                        })
                        .await;
                }
                iroh_blobs::api::downloader::DownloadProgressItem::Progress(done) => {
                    saw_download_signal = true;
                    if let Some(endpoint_id) = selected_endpoint {
                        worker
                            .send_progress(SyncProgressMsg::BlobDownloadProgress {
                                endpoint_id,
                                partition: PartitionKey::DocBlobsFullSync,
                                hash: worker.hash.clone(),
                                done,
                            })
                            .await;
                    }
                }
                iroh_blobs::api::downloader::DownloadProgressItem::PartComplete { .. } => {
                    saw_download_signal = true;
                }
                iroh_blobs::api::downloader::DownloadProgressItem::ProviderFailed { .. } => {}
                iroh_blobs::api::downloader::DownloadProgressItem::DownloadError
                | iroh_blobs::api::downloader::DownloadProgressItem::Error(_) => {}
            }
        }

        worker
            .send_progress(SyncProgressMsg::BlobMaterializeStarted {
                partition: PartitionKey::DocBlobsFullSync,
                hash: worker.hash.clone(),
            })
            .await;
        match worker.blobs_repo.put_from_store(&worker.hash).await {
            Ok(_) => {
                if let Some(endpoint_id) =
                    selected_endpoint.or_else(|| worker.peers.first().copied())
                {
                    worker
                        .send_progress(SyncProgressMsg::BlobDownloadFinished {
                            endpoint_id,
                            partition: PartitionKey::DocBlobsFullSync,
                            hash: worker.hash.clone(),
                            success: true,
                        })
                        .await;
                }
                worker
                    .send_progress(SyncProgressMsg::BlobWorkerFinished {
                        partition: PartitionKey::DocBlobsFullSync,
                        hash: worker.hash.clone(),
                        success: true,
                        reason: "download and materialize succeeded".to_string(),
                    })
                    .await;
                worker.mark_synced(selected_endpoint);
            }
            Err(err) => {
                tracing::warn!(?err, hash = %worker.hash, "put_from_store failed after download");
                if let Some(endpoint_id) = selected_endpoint {
                    worker
                        .send_progress(SyncProgressMsg::BlobDownloadFinished {
                            endpoint_id,
                            partition: PartitionKey::DocBlobsFullSync,
                            hash: worker.hash.clone(),
                            success: false,
                        })
                        .await;
                }
                worker
                    .send_progress(SyncProgressMsg::BlobWorkerFinished {
                        partition: PartitionKey::DocBlobsFullSync,
                        hash: worker.hash.clone(),
                        success: false,
                        reason: if saw_download_signal {
                            "put_from_store failed after download".to_string()
                        } else {
                            "download produced no data".to_string()
                        },
                    })
                    .await;
                worker.request_backoff(Duration::from_secs(2));
            }
        }
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
    sync_progress_tx: mpsc::Sender<SyncProgressMsg>,
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

    async fn send_progress(&self, msg: SyncProgressMsg) {
        if let Err(err) = self.sync_progress_tx.try_send(msg) {
            tracing::debug!(?err, hash = %self.hash, "dropping blob worker progress message");
        }
    }
}
