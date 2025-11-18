mod ingress;
mod runtime;

pub use ingress::{PartitionLogIngress, WflowIngress};
pub use runtime::{build_runtime_host, start_partition_worker, RuntimeConfig};

use crate::drawer::{DrawerEvent, DrawerRepo};
use crate::gen::doc::DocAddedEvent;
use crate::interlude::*;
use samod::DocumentId;
use std::str::FromStr;

/// Worker that listens to drawer events and schedules workflows
pub struct DocChangesWorker {
    handle: tokio::task::JoinHandle<()>,
    _listener: crate::repos::ListenerRegistration,
    _repo: Arc<DrawerRepo>,
    cancel_token: tokio_util::sync::CancellationToken,
}

impl DocChangesWorker {
    /// Abort the worker task
    pub fn abort(&self) {
        self.cancel_token.cancel();
        self.handle.abort();
    }
}

impl Drop for DocChangesWorker {
    fn drop(&mut self) {
        self.cancel_token.cancel();
        self.handle.abort();
    }
}

struct DocChangeEvent {
    inner: Arc<DrawerEvent>,
    last_attempt_backoff_secs: u64,
}

impl From<Arc<DrawerEvent>> for DocChangeEvent {
    fn from(inner: Arc<DrawerEvent>) -> Self {
        Self {
            inner,
            last_attempt_backoff_secs: 1,
        }
    }
}

impl DocChangesWorker {
    /// Spawn a worker that listens to drawer repo events and schedules workflows
    pub async fn spawn<I: WflowIngress + 'static>(
        repo: Arc<DrawerRepo>,
        ingress: Arc<I>,
    ) -> Res<Self> {
        use crate::repos::Repo;

        let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel::<DocChangeEvent>();

        let listener = repo.register_listener({
            let event_tx = event_tx.clone();
            move |event| event_tx.send(event.into()).expect(ERROR_CHANNEL)
        });

        let cancel_token = tokio_util::sync::CancellationToken::new();
        let fut = {
            let ingress = ingress.clone();
            let cancel_token = cancel_token.clone();
            async move {
                let retry = |event: DocChangeEvent| {
                    tokio::spawn({
                        let event_tx = event_tx.clone();
                        async move {
                            let new_backoff = backoff(event.last_attempt_backoff_secs, 60).await;
                            event_tx
                                .send(DocChangeEvent {
                                    last_attempt_backoff_secs: new_backoff,
                                    ..event
                                })
                                .expect(ERROR_CHANNEL);
                        }
                    });
                };

                loop {
                    tokio::select! {
                        _ = cancel_token.cancelled() => {
                            info!("DocChangesWorker cancelled");
                            break;
                        }
                        event = event_rx.recv() => {
                            let Some(event) = event else {
                                break;
                            };
                            match &*event.inner {
                                DrawerEvent::ListChanged => {
                                    // noop
                                }
                                DrawerEvent::DocUpdated { .. } => {
                                    // TODO: handle doc updates
                                }
                                DrawerEvent::DocDeleted { .. } => {
                                    // TODO: handle doc deletions
                                }
                                DrawerEvent::DocAdded { id, heads } => {
                                    info!(?id, "XXX");
                                    let job_id: Arc<str> = format!("doc-added-{}", id).into();
                                    let args_json = serde_json::to_string(&DocAddedEvent {
                                        id: id.clone(),
                                        heads: heads.clone(),
                                    })
                                    .expect(ERROR_JSON);

                                    if let Err(err) = ingress
                                        .add_job(job_id, "doc-created".to_string(), args_json, None)
                                        .await
                                    {
                                        error!(?err, doc_id = ?id, "error scheduling doc-created workflow");
                                        retry(event);
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                }
                eyre::Ok(())
            }
        };
        let handle = tokio::spawn(async move {
            fut.await.unwrap_or_log();
        });

        Ok(Self {
            _repo: repo,
            _listener: listener,
            handle,
            cancel_token,
        })
    }
}

// Returns new backoff
async fn backoff(last_backoff: u64, max: u64) -> u64 {
    let new_backoff = last_backoff * 2;
    let new_backoff = new_backoff.min(max);
    tokio::time::sleep(std::time::Duration::from_secs(new_backoff)).await;
    new_backoff
}
