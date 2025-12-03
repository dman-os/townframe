use crate::interlude::*;

use tokio_util::sync::CancellationToken;

use utils_rs::prelude::tokio::task::JoinHandle;
use wflow_core::partition::{effects, job_events, log};

use crate::partition::{state::PartitionWorkingState, PartitionCtx};

pub struct TokioEffectWorkerHandle {
    cancel_token: CancellationToken,
    join_handle: Option<JoinHandle<Res<()>>>,
}

impl TokioEffectWorkerHandle {
    pub async fn close(mut self) -> Res<()> {
        self.cancel_token.cancel();
        let join_handle = self.join_handle.take().expect("join_handle already taken");
        utils_rs::wait_on_handle_with_timeout(join_handle, 5 * 1000).await?
    }
}

impl Drop for TokioEffectWorkerHandle {
    fn drop(&mut self) {
        self.cancel_token.cancel();
        if let Some(join_handle) = self.join_handle.take() {
            join_handle.abort()
        }
    }
}

pub fn start_tokio_effect_worker(
    worker_id: usize,
    pcx: PartitionCtx,
    state: Arc<PartitionWorkingState>,
    effect_rx: async_channel::Receiver<effects::EffectId>,
    cancel_token: CancellationToken,
) -> TokioEffectWorkerHandle {
    let span = tracing::info_span!(
        "TokioEffectWorker",
        worker_id,
        partition_id = ?pcx.id,
    );
    let fut = {
        let cancel_token = cancel_token.clone();
        async move {
            let mut worker = TokioEffectWorker {
                state,
                log: pcx.log_ref(),
                pcx,
            };
            debug!("starting");
            loop {
                tokio::select! {
                    biased;
                    _ = cancel_token.cancelled() => {
                        break;
                    }
                    effect_id = effect_rx.recv() => {
                        let Ok(effect_id) = effect_id else {
                            break;
                        };
                        worker.handle_partition_effects(effect_id).await?;
                    }
                };
            }
            debug!("shutting down");
            eyre::Ok(())
        }
    }
    .boxed()
    .instrument(span);
    let join_handle = tokio::spawn(fut);
    TokioEffectWorkerHandle {
        cancel_token,
        join_handle: Some(join_handle),
    }
}

struct TokioEffectWorker {
    pcx: PartitionCtx,
    log: crate::partition::PartitionLogRef,
    state: Arc<PartitionWorkingState>,
}

impl TokioEffectWorker {
    #[tracing::instrument(skip(self))]
    async fn handle_partition_effects(&mut self, effect_id: effects::EffectId) -> Res<()> {
        let (job_id, deets) = {
            let effects_map = self.state.read_effects().await;
            let effects::PartitionEffect { job_id, deets } = effects_map
                .get(&effect_id)
                .expect("scheduled effect not found");
            (job_id.clone(), deets.clone())
        };

        match deets {
            effects::PartitionEffectDeets::RunJob(deets) => {
                let start_at = OffsetDateTime::now_utc();
                let run_id = deets.run_id;

                let result = self.run_job_effect(job_id.clone()).await;
                let end_at = OffsetDateTime::now_utc();
                self.log
                    .append(&log::PartitionLogEntry::JobEffectResult(
                        job_events::JobRunEvent {
                            job_id,
                            effect_id,
                            timestamp: end_at.clone(),
                            run_id,
                            start_at,
                            end_at,
                            result,
                        },
                    ))
                    .await?;
            }
            effects::PartitionEffectDeets::AbortJob { .. } => todo!(),
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn run_job_effect(&mut self, job_id: Arc<str>) -> job_events::JobRunResult {
        let job_state_snapshot = {
            let jobs = self.state.read_jobs().await;
            let Some(state) = jobs.active.get(&job_id) else {
                return job_events::JobRunResult::WorkerErr(
                    job_events::JobRunWorkerError::JobNotFound,
                );
            };
            state.clone()
        };

        let res = match &job_state_snapshot.wflow.service {
            wflow_core::gen::metastore::WflowServiceMeta::Wasmcloud(meta) => {
                self.pcx
                    .local_wasmcloud_host
                    .run(job_id.clone(), job_state_snapshot.clone(), meta)
                    .await
            }
            wflow_core::metastore::WflowServiceMeta::LocalNative => {
                self.pcx
                    .local_native_host
                    .run(job_id.clone(), job_state_snapshot.clone(), &())
                    .await
            }
        };
        match res {
            Ok(val) | Err(val) => val,
        }
    }
}
