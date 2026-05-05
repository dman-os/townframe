use super::*;

pub(super) struct BlobSyncWorkerStopToken {
    task_handle: utils_rs::TaskHandle,
}

impl BlobSyncWorkerStopToken {
    pub async fn stop(self) -> Res<()> {
        self.task_handle.join(Duration::from_secs(2)).await?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(super) enum SyncBlobOutcome {
    Downloaded,
    LocalPresent,
}

#[expect(clippy::too_many_arguments)]
pub fn spawn_blob_sync_worker(
    partition: PartitionKey,
    hash: Arc<str>,
    peers: Vec<PeerId>,
    msg_tx: mpsc::UnboundedSender<Msg>,
    sync_progress_tx: mpsc::Sender<SyncProgressMsg>,
    blobs_repo: Arc<BlobsRepo>,
    endpoint: iroh::Endpoint,
    previous_retry_state: RetryState,
    task_set: &utils_rs::AbortableJoinSet,
) -> Res<BlobSyncWorkerStopToken> {
    let mut worker = BlobSyncWorker {
        partition,
        hash,
        peers,
        sync_progress_tx,
        blobs_repo,
        endpoint,
        retry: previous_retry_state,
    };
    let fut = async move {
        let msg = match worker.run().await {
            Ok(msg) => msg,
            Err(err) => {
                worker.send_progress(SyncProgressMsg::BlobDownloadFinished {
                    hash: Arc::clone(&worker.hash),
                    partition: worker.partition.clone(),
                    error: Some(err),
                });
                Msg::BlobSyncBackoff {
                    hash: worker.hash,
                    previous_retry_state,
                    delay: Duration::from_secs(2),
                }
            }
        };
        msg_tx.send(msg).inspect_err(|_| warn!(ERROR_CALLER)).ok();
    };
    let task_handle = task_set
        .spawn(fut.instrument(tracing::info_span!("BlobSyncWorker task")))
        .map_err(|_| ferr!("task set aborted"))?;
    Ok(BlobSyncWorkerStopToken { task_handle })
}

struct BlobSyncWorker {
    partition: PartitionKey,
    hash: Arc<str>,
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
    async fn run(&mut self) -> Res<Msg> {
        self.send_progress(SyncProgressMsg::BlobWorkerStarted {
            partition: self.partition.clone(),
            hash: Arc::clone(&self.hash),
        })
        .await;

        let has_local_hash = self
            .blobs_repo
            .has_hash(&self.hash)
            .await
            .wrap_err("local blob lookup failied")?;

        if has_local_hash {
            return Ok(Msg::BlobSyncCompleted {
                hash: Arc::clone(&self.hash),
                outcome: SyncBlobOutcome::LocalPresent,
            });
        }

        let iroh_hash =
            crate::blobs::daybook_hash_to_iroh_hash(&self.hash).wrap_err("invalid hash")?;

        let has_in_store = self
            .blobs_repo
            .iroh_store()
            .blobs()
            .has(iroh_hash)
            .await
            .wrap_err("iroh store lookup failed")?;

        if has_in_store {
            self.send_progress(SyncProgressMsg::BlobMaterializeStarted {
                partition: self.partition.clone(),
                hash: Arc::clone(&self.hash),
            })
            .await;
            self.blobs_repo.put_from_store(&self.hash).await?;
            return Ok(Msg::BlobSyncCompleted {
                hash: Arc::clone(&self.hash),
                outcome: SyncBlobOutcome::LocalPresent,
            });
        }

        assert!(!self.peers.is_empty());

        let downloader = self.blobs_repo.iroh_store().downloader(&self.endpoint);
        let progress = downloader.download(iroh_hash, self.peers.clone());
        let mut stream = progress.stream().await?;

        let mut selected_endpoint: Option<PeerId> = None;
        let mut saw_download_signal = false;
        let mut saw_download_error = false;
        use futures::StreamExt;
        while let Some(item) = stream.next().await {
            if saw_download_error {
                warn!("curiousity trap: we saw stream cont after error");
            }
            match &item {
                iroh_blobs::api::downloader::DownloadProgressItem::TryProvider { id, .. } => {
                    saw_download_signal = true;
                    selected_endpoint = Some((*id).into());
                    self.send_progress(SyncProgressMsg::BlobDownloadStarted {
                        peer_id: (*id).into(),
                        partition: self.partition.clone(),
                        hash: Arc::clone(&self.hash),
                    })
                    .await;
                }
                iroh_blobs::api::downloader::DownloadProgressItem::Progress(done) => {
                    saw_download_signal = true;
                    if let Some(peer_id) = selected_endpoint {
                        self.send_progress(SyncProgressMsg::BlobDownloadProgress {
                            peer_id,
                            partition: self.partition.clone(),
                            hash: Arc::clone(&self.hash),
                            done_counter: *done,
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
                    error!("download error progress: {item:?}");
                    saw_download_error = true;
                }
            }
        }

        if saw_download_error {
            eyre::bail!("error seen during download saw_download_signal={saw_download_signal}");
        }

        if !self.blobs_repo.iroh_store().blobs().has(iroh_hash).await? {
            eyre::bail!("download completed but blob missing from store");
        }

        self.send_progress(SyncProgressMsg::BlobMaterializeStarted {
            partition: self.partition.clone(),
            hash: Arc::clone(&self.hash),
        })
        .await;
        self.blobs_repo.put_from_store(&self.hash).await?;
        Ok(Msg::BlobSyncCompleted {
            hash: Arc::clone(&self.hash),
            outcome: SyncBlobOutcome::Downloaded {},
        })
    }
}
