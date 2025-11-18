use crate::interlude::*;

use crate::plugin::binds_metastore::townframe::wflow::metastore;

use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use utils_rs::prelude::tokio::task::JoinHandle;

use crate::partition::{effects, job_events, log, service, state, PartitionCtx};

pub struct TokioEffectWorkerHandle {
    cancel_token: CancellationToken,
    join_handle: Option<JoinHandle<Res<()>>>,
}

impl TokioEffectWorkerHandle {
    pub fn cancel(&self) {
        self.cancel_token.cancel();
    }

    pub async fn close(mut self) -> Res<()> {
        self.cancel_token.cancel();
        // Move out the join_handle to await it
        let join_handle = self.join_handle.take().expect("join_handle already taken");
        // Drop will cancel again, which is safe (idempotent)
        drop(self);
        join_handle.await.wrap_err("join error")?
    }
}

impl Drop for TokioEffectWorkerHandle {
    fn drop(&mut self) {
        self.cancel_token.cancel();
    }
}

pub fn start_tokio_effect_worker(
    pcx: PartitionCtx,
    state: Arc<state::PartitionWorkingState>,
    effect_rx: async_channel::Receiver<effects::EffectId>,
    cancel_token: CancellationToken,
) -> TokioEffectWorkerHandle {
    let fut = {
        let cancel_token = cancel_token.clone();
        async move {
            let mut worker = TokioEffectWorker {
                state,
                log: pcx.log_ref(),
                pcx,
            };
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
            eyre::Ok(())
        }
    };
    let join_handle = tokio::spawn(fut);
    TokioEffectWorkerHandle {
        cancel_token,
        join_handle: Some(join_handle),
    }
}

struct TokioEffectWorker {
    pcx: PartitionCtx,
    log: crate::partition::log::PartitionLogRef,
    state: Arc<state::PartitionWorkingState>,
}

impl TokioEffectWorker {
    async fn handle_partition_effects(&mut self, effect_id: effects::EffectId) -> Res<()> {
        let (job_id, deets) = {
            let mut effects_map = self.state.effects.lock().await;
            let effects::PartitionEffect { job_id, deets } = effects_map
                .get_mut(&effect_id)
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
                    .append(&log::PartitionLogEntry::JobEvent(job_events::JobEvent {
                        job_id,
                        timestamp: end_at.clone(),
                        deets: job_events::JobEventDeets::Run(job_events::JobRunEvent {
                            run_id,
                            start_at,
                            end_at,
                            result,
                        }),
                    }))
                    .await?;
            }
            effects::PartitionEffectDeets::AbortJob { reason: _ } => todo!(),
        }
        Ok(())
    }

    async fn run_job_effect(&mut self, job_id: Arc<str>) -> job_events::JobRunResult {
        let job_state_snapshot = {
            let jobs = self.state.jobs.lock().await;
            let Some(state) = jobs.active.get(&job_id) else {
                return job_events::JobRunResult::WorkerErr(
                    job_events::JobRunWorkerError::JobNotFound,
                );
            };
            state.clone()
        };

        let res = match &job_state_snapshot.wflow.service {
            metastore::WflowServiceMeta::Wasmcloud(meta) => {
                self.pcx
                    .local_wasmcloud_host
                    .run(job_id.clone(), job_state_snapshot.clone(), meta)
                    .await
            }
        };
        match res {
            Ok(val) | Err(val) => val,
        }
    }
}
