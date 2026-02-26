use crate::interlude::*;

use tokio_util::sync::CancellationToken;

use utils_rs::prelude::tokio::task::JoinHandle;
use wflow_core::partition::{effects, job_events, log};

use crate::partition::{
    state::PartitionWorkingState, DirectEffectRx, EffectCancelTokens, JobToEffectId, PartitionCtx,
    WorkerId,
};

pub struct TokioEffectWorkerHandle {
    cancel_token: CancellationToken,
    join_handle: Option<JoinHandle<()>>,
}

impl TokioEffectWorkerHandle {
    pub async fn stop(mut self) -> Res<()> {
        self.cancel_token.cancel();
        let join_handle = self.join_handle.take().expect("join_handle already taken");
        utils_rs::wait_on_handle_with_timeout(join_handle, 5 * 1000).await?;
        Ok(())
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
    worker_name: WorkerId,
    pcx: PartitionCtx,
    state: Arc<PartitionWorkingState>,
    effect_cancel_tokens: EffectCancelTokens,
    job_to_effect_id: JobToEffectId,
    direct_effect_rx: DirectEffectRx,
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
                effect_cancel_tokens,
                job_to_effect_id,
                worker_id: Arc::clone(&worker_name),
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
                    effect_id = direct_effect_rx.recv() => {
                        let Ok(effect_id) = effect_id else {
                            continue;
                        };
                        worker.handle_partition_effects(effect_id).await?;
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
    let join_handle = tokio::spawn(async { fut.await.unwrap() });
    TokioEffectWorkerHandle {
        cancel_token,
        join_handle: Some(join_handle),
    }
}

struct TokioEffectWorker {
    pcx: PartitionCtx,
    log: crate::partition::PartitionLogRef,
    state: Arc<PartitionWorkingState>,
    effect_cancel_tokens: EffectCancelTokens,
    job_to_effect_id: JobToEffectId,
    worker_id: WorkerId,
}

impl TokioEffectWorker {
    #[tracing::instrument(skip(self))]
    async fn handle_partition_effects(&mut self, effect_id: effects::EffectId) -> Res<()> {
        let (job_id, deets) = {
            let effects_map = self.state.read_effects().await;
            let effects::PartitionEffect { job_id, deets } = effects_map
                .get(&effect_id)
                .expect("scheduled effect not found");
            (Arc::clone(job_id), deets.clone())
        };

        match deets {
            effects::PartitionEffectDeets::RunJob(run_deets) => {
                let run_id = run_deets.run_id;
                let start_at = Timestamp::now();
                let run_start_instant = std::time::Instant::now();
                let run_abort_token = self
                    .effect_cancel_tokens
                    .lock()
                    .await
                    .get(&effect_id)
                    .cloned()
                    .expect("RunJob effect must have cancellation token");
                self.job_to_effect_id
                    .lock()
                    .await
                    .insert(Arc::clone(&job_id), effect_id.clone());
                let run_fut = self.run_job_effect(effect_id.clone(), run_id, Arc::clone(&job_id));
                let result = tokio::select! {
                    biased;
                    _ = run_abort_token.cancelled() => job_events::JobRunResult::Aborted,
                    res = run_fut => res,
                };
                self.job_to_effect_id.lock().await.remove(&job_id);
                self.effect_cancel_tokens.lock().await.remove(&effect_id);
                let elapsed = run_start_instant.elapsed();
                let end_at = start_at.checked_add(elapsed).expect("ts overflow");
                self.log
                    .append(&log::PartitionLogEntry::JobEffectResult(
                        job_events::JobRunEvent {
                            job_id,
                            effect_id,
                            timestamp: end_at,
                            run_id,
                            worker_id: Some(Arc::clone(&self.worker_id)),
                            start_at,
                            end_at,
                            result,
                        },
                    ))
                    .await?;
            }
            effects::PartitionEffectDeets::AbortRun { .. } => {
                let run_effect_id = self.job_to_effect_id.lock().await.get(&job_id).cloned();
                if let Some(run_effect_id) = run_effect_id {
                    if let Some(abort_token) =
                        self.effect_cancel_tokens.lock().await.get(&run_effect_id)
                    {
                        abort_token.cancel();
                    }
                }
                let mut effects_map = self.state.write_effects().await;
                effects_map.remove(&effect_id);
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn run_job_effect(
        &mut self,
        effect_id: effects::EffectId,
        run_id: u64,
        job_id: Arc<str>,
    ) -> job_events::JobRunResult {
        let job_state_snapshot = {
            let jobs = self.state.read_jobs().await;
            let Some(state) = jobs.active.get(&job_id) else {
                return job_events::JobRunResult::WorkerErr(
                    job_events::JobRunWorkerError::JobNotFound,
                );
            };
            state.clone()
        };
        let run_ctx = crate::partition::service::RunJobCtx {
            effect_id,
            run_id,
            worker_id: Arc::clone(&self.worker_id),
        };

        let res = match &job_state_snapshot.wflow.service {
            wflow_core::gen::metastore::WflowServiceMeta::Wasmcloud(meta) => {
                self.pcx
                    .local_wasmcloud_host
                    .run(
                        &run_ctx,
                        Arc::clone(&job_id),
                        job_state_snapshot.clone(),
                        meta,
                    )
                    .await
            }
            wflow_core::metastore::WflowServiceMeta::LocalNative => {
                self.pcx
                    .local_native_host
                    .run(
                        &run_ctx,
                        Arc::clone(&job_id),
                        job_state_snapshot.clone(),
                        &(),
                    )
                    .await
            }
        };
        match res {
            Ok(val) | Err(val) => val,
        }
    }
}
