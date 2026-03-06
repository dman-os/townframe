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

pub async fn spawn_doc_sync_worker(
    doc_id: DocumentId,
    handle: samod::DocHandle,
    broker_handle: Arc<am_utils_rs::changes::DocChangeBrokerHandle>,
    broker_stop_token: Arc<am_utils_rs::changes::DocChangeBrokerStopToken>,
    cancel_token: CancellationToken,
    msg_tx: mpsc::UnboundedSender<Msg>,
    retry: RetryState,
) -> Res<DocSyncWorkerStopToken> {
    let stop_cancel_token = cancel_token.clone();
    let mut heads_listener = broker_handle.get_head_listener().await?;
    let (peer_state, state_stream) = handle.peers();

    let worker = DocSyncWorker {
        doc_id: doc_id.clone(),
        msg_tx,
        retry,
    };
    worker.handle_peer_state_update(peer_state);

    let fut = {
        async move {
            let mut idle_timeout = Box::pin(tokio::time::sleep(Duration::from_secs(120)));
            let mut state_stream = state_stream.boxed();
            let loop_res: Res<()> = loop {
                tokio::select! {
                    biased;
                    _ = cancel_token.cancelled() => {
                        debug!("cancel token lit");
                        break eyre::Ok(());
                    }
                    val = heads_listener.change_rx().recv() => {
                        let Some(heads) = val else {
                            break Err(eyre::eyre!("DocChangeBroker was removed from repo, weird!"));
                        };
                        worker.handle_heads_update(heads);
                        idle_timeout
                            .as_mut()
                            .reset(tokio::time::Instant::now() + Duration::from_secs(120));
                    }
                    val = state_stream.next() => {
                        let Some(diff) = val else {
                            break Err(eyre::eyre!("DocHandle was removed from repo, weird!"));
                        };
                        worker.handle_peer_state_update(diff);
                        idle_timeout
                            .as_mut()
                            .reset(tokio::time::Instant::now() + Duration::from_secs(120));
                    }
                    _ = &mut idle_timeout => {
                        worker.handle_timeout();
                        break eyre::Ok(());
                    }
                }
            };
            if let Ok(token) = Arc::try_unwrap(broker_stop_token) {
                token.stop().await?;
            }
            loop_res
        }
    };
    let join_handle = tokio::spawn(
        async move { fut.await.unwrap() }.instrument(tracing::info_span!("DocSyncWorker task")),
    );
    Ok(DocSyncWorkerStopToken {
        cancel_token: stop_cancel_token,
        join_handle,
    })
}

struct DocSyncWorker {
    doc_id: DocumentId,
    msg_tx: mpsc::UnboundedSender<Msg>,
    retry: RetryState,
}

impl DocSyncWorker {
    fn handle_heads_update(&self, heads: Arc<[automerge::ChangeHash]>) {
        self.msg_tx
            .send(Msg::DocHeadsUpdated {
                doc_id: self.doc_id.clone(),
                heads: ChangeHashSet(heads),
            })
            .expect("FullSyncWorker went down without cleaning boot_doc_sync_worker");
    }

    fn handle_peer_state_update(&self, diff: DocPeerStateView) {
        self.msg_tx
            .send(Msg::DocPeerStateViewUpdated {
                doc_id: self.doc_id.clone(),
                diff,
            })
            .expect("FullSyncWorker went down without cleaning boot_doc_sync_worker");
    }

    fn handle_timeout(&self) {
        self.msg_tx
            .send(Msg::DocSyncRequestBackoff {
                doc_id: self.doc_id.clone(),
                delay: Duration::from_millis(500),
                previous_attempt_no: self.retry.attempt_no,
                previous_backoff: self.retry.last_backoff,
                previous_attempt_at: self.retry.last_attempt_at,
            })
            .expect("FullSyncWorker went down without cleaning boot_doc_sync_worker");
    }
}
