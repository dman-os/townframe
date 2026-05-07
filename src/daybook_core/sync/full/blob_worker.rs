use super::*;

pub(super) struct BlobSyncWorkerStopToken {
    task_handle: utils_rs::TaskHandle,
}

impl BlobSyncWorkerStopToken {
    pub async fn stop(self) {
        self.task_handle
            .join(Duration::from_secs(2))
            .await
            .inspect_err(|err| error!("error joining blob sync worker: {err:?}"))
            .ok();
    }
}

#[derive(Debug, Clone)]
pub(super) enum SyncBlobOutcome {
    Downloaded,
    LocalPresent,
}

#[expect(clippy::too_many_arguments)]
pub fn spawn_blob_sync_worker(
    hash: Hash,
    dayb_hash: Arc<str>,
    peers: Vec<PeerId>,
    msg_tx: mpsc::UnboundedSender<Msg>,
    sync_progress_tx: mpsc::Sender<SyncProgressMsg>,
    blobs_repo: Arc<BlobsRepo>,
    endpoint: iroh::Endpoint,
    previous_retry_state: RetryState,
    task_set: &utils_rs::AbortableJoinSet,
) -> Res<BlobSyncWorkerStopToken> {
    let mut worker = BlobSyncWorker {
        hash,
        dayb_hash,
        peers,
        sync_progress_tx,
        blobs_repo,
        endpoint,
        retry: previous_retry_state,
    };
    let fut = async move {
        let msg = match worker.run().await {
            Ok(outcome) => Msg::BlobSyncCompleted {
                hash: worker.hash,
                outcome,
            },
            Err(err) => {
                worker
                    .send_progress(SyncProgressMsg::BlobDownloadFinished {
                        hash: Arc::clone(&worker.dayb_hash),
                        error: Some(err),
                    })
                    .await;
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
    hash: Hash,
    dayb_hash: Arc<str>,
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
    async fn run(&mut self) -> Res<SyncBlobOutcome> {
        self.send_progress(SyncProgressMsg::BlobWorkerStarted {
            hash: Arc::clone(&self.dayb_hash),
        })
        .await;

        let has_local_hash = self
            .blobs_repo
            .has_hash(&self.dayb_hash)
            .await
            .wrap_err("local blob lookup failied")?;

        if has_local_hash {
            return Ok(SyncBlobOutcome::LocalPresent);
        }

        let has_in_store = self
            .blobs_repo
            .iroh_store()
            .blobs()
            .has(self.hash)
            .await
            .wrap_err("iroh store lookup failed")?;

        if has_in_store {
            self.send_progress(SyncProgressMsg::BlobMaterializeStarted {
                hash: Arc::clone(&self.dayb_hash),
            })
            .await;
            self.blobs_repo.put_from_store(&self.dayb_hash).await?;
            return Ok(SyncBlobOutcome::LocalPresent);
        }

        assert!(!self.peers.is_empty());

        let downloader = self.blobs_repo.iroh_store().downloader(&self.endpoint);
        let progress = downloader.download(self.hash, self.peers.clone());
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
                        hash: Arc::clone(&self.dayb_hash),
                    })
                    .await;
                }
                iroh_blobs::api::downloader::DownloadProgressItem::Progress(done) => {
                    saw_download_signal = true;
                    if let Some(peer_id) = selected_endpoint {
                        self.send_progress(SyncProgressMsg::BlobDownloadProgress {
                            peer_id,
                            hash: Arc::clone(&self.dayb_hash),
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

        if !self.blobs_repo.iroh_store().blobs().has(self.hash).await? {
            eyre::bail!("download completed but blob missing from store");
        }

        self.send_progress(SyncProgressMsg::BlobMaterializeStarted {
            hash: Arc::clone(&self.dayb_hash),
        })
        .await;
        self.blobs_repo.put_from_store(&self.dayb_hash).await?;
        Ok(SyncBlobOutcome::Downloaded {})
    }
}
