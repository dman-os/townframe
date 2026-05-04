use super::*;

pub(super) struct BlobSyncWorkerStopToken {
    task_handle: utils_rs::TaskHandle,
}

impl BlobSyncWorkerStopToken {
    pub async fn stop(self) -> Res<()> {
        self.task_handle.abort();
        tokio::time::timeout(Duration::from_secs(2), self.task_handle.join())
            .await
            .ok();
        Ok(())
    }
}

#[expect(clippy::too_many_arguments)]
pub fn spawn_blob_sync_worker(
    partition: PartitionKey,
    hash: String,
    peers: Vec<PeerId>,
    sync_progress_tx: mpsc::Sender<SyncProgressMsg>,
    blobs_repo: Arc<BlobsRepo>,
    endpoint: iroh::Endpoint,
    retry: RetryState,
    task_set: &utils_rs::AbortableJoinSet,
) -> Res<BlobSyncWorkerStopToken> {
    let worker = BlobSyncWorker {
        partition,
        hash,
        peers,
        sync_progress_tx,
        blobs_repo,
        endpoint,
        retry,
    };
    let fut = async move {
        worker
            .send_progress(SyncProgressMsg::BlobWorkerStarted {
                partition: worker.partition.clone(),
                hash: worker.hash.clone(),
            })
            .await;

        let iroh_hash = match crate::blobs::daybook_hash_to_iroh_hash(&worker.hash) {
            Ok(hash) => hash,
            Err(err) => {
                tracing::warn!(?err, hash = %worker.hash, "invalid daybook blob hash");
                worker
                    .send_progress(SyncProgressMsg::BlobWorkerFinished {
                        partition: worker.partition.clone(),
                        hash: worker.hash.clone(),
                        success: false,
                        reason: "invalid hash".to_string(),
                        synced_peer_id: None,
                        backoff: None,
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
                        partition: worker.partition.clone(),
                        hash: worker.hash.clone(),
                        success: false,
                        reason: "iroh store lookup failed".to_string(),
                        synced_peer_id: None,
                        backoff: Some((Duration::from_secs(2), worker.retry)),
                    })
                    .await;
                return;
            }
        };

        let has_local_hash = match worker.blobs_repo.has_hash(&worker.hash).await {
            Ok(has) => has,
            Err(err) => {
                tracing::warn!(?err, hash = %worker.hash, "error checking local blob presence");
                worker
                    .send_progress(SyncProgressMsg::BlobWorkerFinished {
                        partition: worker.partition.clone(),
                        hash: worker.hash.clone(),
                        success: false,
                        reason: "local blob lookup failed".to_string(),
                        synced_peer_id: None,
                        backoff: Some((Duration::from_secs(2), worker.retry)),
                    })
                    .await;
                return;
            }
        };

        if has_local_hash {
            worker
                .send_progress(SyncProgressMsg::BlobWorkerFinished {
                    partition: worker.partition.clone(),
                    hash: worker.hash.clone(),
                    success: true,
                    reason: "already present in blobs repo".to_string(),
                    synced_peer_id: None,
                    backoff: None,
                })
                .await;
            return;
        }

        if has_in_store {
            worker
                .send_progress(SyncProgressMsg::BlobMaterializeStarted {
                    partition: worker.partition.clone(),
                    hash: worker.hash.clone(),
                })
                .await;
            match worker.blobs_repo.put_from_store(&worker.hash).await {
                Ok(_) => {
                    worker
                        .send_progress(SyncProgressMsg::BlobWorkerFinished {
                            partition: worker.partition.clone(),
                            hash: worker.hash.clone(),
                            success: true,
                            reason: "materialized from local iroh store".to_string(),
                            synced_peer_id: None,
                            backoff: None,
                        })
                        .await;
                }
                Err(err) => {
                    tracing::warn!(?err, hash = %worker.hash, "put_from_store failed from local iroh store");
                    worker
                        .send_progress(SyncProgressMsg::BlobWorkerFinished {
                            partition: worker.partition.clone(),
                            hash: worker.hash.clone(),
                            success: false,
                            reason: "put_from_store failed".to_string(),
                            synced_peer_id: None,
                            backoff: Some((Duration::from_secs(2), worker.retry)),
                        })
                        .await;
                }
            }
            return;
        }

        if worker.peers.is_empty() {
            worker
                .send_progress(SyncProgressMsg::BlobWorkerFinished {
                    partition: worker.partition.clone(),
                    hash: worker.hash.clone(),
                    success: false,
                    reason: "no peers available".to_string(),
                    synced_peer_id: None,
                    backoff: Some((Duration::from_secs(2), worker.retry)),
                })
                .await;
            return;
        }

        let downloader = worker.blobs_repo.iroh_store().downloader(&worker.endpoint);
        let progress = downloader.download(iroh_hash, worker.peers.clone());
        let stream_res = progress.stream().await;
        let Ok(mut stream) = stream_res else {
            worker
                .send_progress(SyncProgressMsg::BlobWorkerFinished {
                    partition: worker.partition.clone(),
                    hash: worker.hash.clone(),
                    success: false,
                    reason: "failed to open download stream".to_string(),
                    synced_peer_id: None,
                    backoff: Some((Duration::from_secs(2), worker.retry)),
                })
                .await;
            return;
        };

        let mut selected_endpoint: Option<PeerId> = None;
        let mut saw_download_signal = false;
        let mut saw_download_error = false;
        use futures::StreamExt;
        while let Some(item) = stream.next().await {
            match item {
                iroh_blobs::api::downloader::DownloadProgressItem::TryProvider { id, .. } => {
                    saw_download_signal = true;
                    selected_endpoint = Some(id.into());
                    worker
                        .send_progress(SyncProgressMsg::BlobDownloadStarted {
                            peer_id: id.into(),
                            partition: worker.partition.clone(),
                            hash: worker.hash.clone(),
                        })
                        .await;
                }
                iroh_blobs::api::downloader::DownloadProgressItem::Progress(done) => {
                    saw_download_signal = true;
                    if let Some(peer_id) = selected_endpoint {
                        worker
                            .send_progress(SyncProgressMsg::BlobDownloadProgress {
                                peer_id,
                                partition: worker.partition.clone(),
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
                | iroh_blobs::api::downloader::DownloadProgressItem::Error(_) => {
                    saw_download_error = true;
                }
            }
        }

        if saw_download_error {
            worker
                .send_progress(SyncProgressMsg::BlobWorkerFinished {
                    partition: worker.partition.clone(),
                    hash: worker.hash.clone(),
                    success: false,
                    reason: "download reported error".to_string(),
                    synced_peer_id: None,
                    backoff: Some((Duration::from_secs(2), worker.retry)),
                })
                .await;
            return;
        }

        let has_in_store_after_download = match worker
            .blobs_repo
            .iroh_store()
            .blobs()
            .has(iroh_hash)
            .await
        {
            Ok(has) => has,
            Err(err) => {
                tracing::warn!(?err, hash = %worker.hash, "error checking iroh blob store after download");
                worker
                    .send_progress(SyncProgressMsg::BlobWorkerFinished {
                        partition: worker.partition.clone(),
                        hash: worker.hash.clone(),
                        success: false,
                        reason: "iroh store lookup failed after download".to_string(),
                        synced_peer_id: None,
                        backoff: Some((Duration::from_secs(2), worker.retry)),
                    })
                    .await;
                return;
            }
        };

        if !has_in_store_after_download {
            worker
                .send_progress(SyncProgressMsg::BlobWorkerFinished {
                    partition: worker.partition.clone(),
                    hash: worker.hash.clone(),
                    success: false,
                    reason: "download completed but blob missing from store".to_string(),
                    synced_peer_id: None,
                    backoff: Some((Duration::from_secs(2), worker.retry)),
                })
                .await;
            return;
        }

        worker
            .send_progress(SyncProgressMsg::BlobMaterializeStarted {
                partition: worker.partition.clone(),
                hash: worker.hash.clone(),
            })
            .await;
        match worker.blobs_repo.put_from_store(&worker.hash).await {
            Ok(_) => {
                let synced_peer_id = selected_endpoint.or_else(|| worker.peers.first().copied());
                if let Some(endpoint_id) = synced_peer_id {
                    worker
                        .send_progress(SyncProgressMsg::BlobDownloadFinished {
                            peer_id: endpoint_id,
                            partition: worker.partition.clone(),
                            hash: worker.hash.clone(),
                            success: true,
                        })
                        .await;
                }
                worker
                    .send_progress(SyncProgressMsg::BlobWorkerFinished {
                        partition: worker.partition.clone(),
                        hash: worker.hash.clone(),
                        success: true,
                        reason: "download and materialize succeeded".to_string(),
                        synced_peer_id,
                        backoff: None,
                    })
                    .await;
            }
            Err(err) => {
                tracing::warn!(?err, hash = %worker.hash, "put_from_store failed after download");
                if let Some(endpoint_id) = selected_endpoint {
                    worker
                        .send_progress(SyncProgressMsg::BlobDownloadFinished {
                            peer_id: endpoint_id,
                            partition: worker.partition.clone(),
                            hash: worker.hash.clone(),
                            success: false,
                        })
                        .await;
                }
                worker
                    .send_progress(SyncProgressMsg::BlobWorkerFinished {
                        partition: worker.partition.clone(),
                        hash: worker.hash.clone(),
                        success: false,
                        reason: if saw_download_signal {
                            "put_from_store failed after download".to_string()
                        } else {
                            "download produced no data".to_string()
                        },
                        synced_peer_id: None,
                        backoff: Some((Duration::from_secs(2), worker.retry)),
                    })
                    .await;
            }
        }
    };
    let task_handle = task_set
        .spawn(fut.instrument(tracing::info_span!("BlobSyncWorker task")))
        .map_err(|_| ferr!("task set aborted"))?;
    Ok(BlobSyncWorkerStopToken { task_handle })
}

struct BlobSyncWorker {
    partition: PartitionKey,
    hash: String,
    peers: Vec<PeerId>,
    sync_progress_tx: mpsc::Sender<SyncProgressMsg>,
    blobs_repo: Arc<BlobsRepo>,
    endpoint: iroh::Endpoint,
    retry: RetryState,
}

impl BlobSyncWorker {
    async fn send_progress(&self, msg: SyncProgressMsg) {
        if let Err(err) = self.sync_progress_tx.try_send(msg) {
            tracing::debug!(?err, hash = %self.hash, "dropping blob worker progress message");
        }
    }
}
