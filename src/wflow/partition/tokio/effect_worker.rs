use crate::interlude::*;

use crate::plugin::binds_metastore::townframe::wflow::metastore;

use tokio_util::sync::CancellationToken;
use utils_rs::prelude::tokio::task::JoinHandle;

use crate::partition::{effects, job_events, log, service, state, PartitionCtx};

pub struct TokioEffectWorkerHandle {
    cancel_token: CancellationToken,
    join_handle: JoinHandle<Res<()>>,
}

impl TokioEffectWorkerHandle {
    pub async fn close(self) -> Res<()> {
        self.cancel_token.cancel();
        self.join_handle.await.wrap_err("join error")?
    }
}

pub fn start_tokio_effect_worker(
    pcx: PartitionCtx,
    state: Arc<state::PartitionWorkingState>,
    effect_rx: async_channel::Receiver<log::PartitionEffectsLogEntry>,
) -> TokioEffectWorkerHandle {
    let cancel_token = CancellationToken::new();

    let join_handle = tokio::spawn({
        let cancel_token = cancel_token.clone();
        async move {
            let mut worker = TokioEffectWorker {
                state,
                log: pcx.log_ref(),
                pcx,
            };
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        break;
                    }
                    effects = effect_rx.recv() => {
                        let Ok(effects) = effects else {
                            break;
                        };
                        worker.handle_partition_effects(effects).await?;
                    }
                };
            }
            eyre::Ok(())
        }
    });
    TokioEffectWorkerHandle {
        cancel_token,
        join_handle,
    }
}

struct TokioEffectWorker {
    pcx: PartitionCtx,
    log: crate::partition::log::PartitionLogRef,
    state: Arc<state::PartitionWorkingState>,
}

impl TokioEffectWorker {
    async fn handle_partition_effects(
        &mut self,
        effects: log::PartitionEffectsLogEntry,
    ) -> Res<()> {
        for effect in effects.effects {
            match effect.deets {
                effects::PartitionEffectDeets::RunJob(deets) => {
                    let start_at = OffsetDateTime::now_utc();
                    let result = self.run_job_effect(effect.job_id.clone(), &deets).await;
                    let end_at = OffsetDateTime::now_utc();
                    self.log
                        .append(&log::PartitionLogEntry::JobEvent(job_events::JobEvent {
                            job_id: effect.job_id,
                            timestamp: end_at.clone(),
                            deets: job_events::JobEventDeets::Run(job_events::JobRunEvent {
                                run_id: deets.run_id,
                                start_at,
                                end_at,
                                result,
                            }),
                        }))
                        .await?;
                }
                effects::PartitionEffectDeets::AbortJob { reason } => todo!(),
            }
        }
        Ok(())
    }

    async fn run_job_effect(
        &mut self,
        job_id: Arc<str>,
        payload: &effects::RunJobAttemptDeets,
    ) -> job_events::JobRunResult {
        let Some(job_state) = self.state.jobs.get(&job_id) else {
            return job_events::JobRunResult::WorkerErr(job_events::JobRunWorkerError::JobNotFound);
        };
        tracing::debug!(%job_id, ?payload, ?job_state, "running job XXX");
        let res = match &job_state.wflow.service {
            metastore::WflowServiceMeta::Wasmcloud(meta) => {
                self.pcx
                    .local_wasmcloud_host
                    .run(job_id, job_state.clone(), meta)
                    .await
            }
        };
        match res {
            Ok(val) | Err(val) => val,
        }
    }
}
