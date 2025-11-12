use crate::interlude::*;

use crate::plugin::binds_metastore::townframe::wflow::metastore;

use tokio_util::sync::CancellationToken;
use utils_rs::prelude::tokio::{sync::mpsc::Sender, task::JoinHandle};

use crate::partition::{effects, job_events, service, state, PartitionCtx};

pub struct TokioEffectWorkerHandle {
    cancel_token: CancellationToken,
    pub inbox: Sender<Vec<effects::PartitionEffect>>,
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
) -> TokioEffectWorkerHandle {
    let cancel_token = CancellationToken::new();
    let (effect_tx, mut effect_rx) = tokio::sync::mpsc::channel(100);

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
                        let Some(effects) = effects else {
                            break;
                        };
                        worker.handle(effects).await?;
                    }
                };
            }
            eyre::Ok(())
        }
    });
    TokioEffectWorkerHandle {
        cancel_token,
        inbox: effect_tx,
        join_handle,
    }
}

struct TokioEffectWorker {
    pcx: PartitionCtx,
    log: crate::partition::log::PartitionLogRef,
    state: Arc<state::PartitionWorkingState>,
}

impl TokioEffectWorker {
    async fn handle(&mut self, effects: Vec<effects::PartitionEffect>) -> Res<()> {
        for effect in effects {
            match effect.deets {
                effects::PartitionEffectDeets::RunJob(payload) => {
                    match self.run_job(effect.job_id, payload).await {
                        Ok(_) => todo!(),
                        Err(_) => todo!(),
                    }
                }
                effects::PartitionEffectDeets::AbortJob { reason } => todo!(),
            }
        }
        Ok(())
    }

    async fn run_job(&mut self, job_id: Arc<str>, payload: effects::RunJobAttemptDeets) -> Res<()> {
        let Some(job_state) = self.state.jobs.get(&job_id) else {
            return Err(JobRunWorkerError::JobNotFound);
        };
        match &job_state.wflow.service {
            metastore::WflowServiceMeta::Wasmcloud(meta) => {
                self.pcx.local_wasmcloud_host.run(RunWflowArgs {
                    wflow_key: todo!(),
                    job_id,
                    args_json: todo!(),
                    journal: todo!(),
                })
            }
        }

        Ok(())
    }
}

struct WasmcloudServiceClient {}

impl WasmcloudServiceClient {}
