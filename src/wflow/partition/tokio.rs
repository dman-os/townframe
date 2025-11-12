use std::sync::atomic::Ordering;

use crate::interlude::*;

use super::{
    effects::PartitionEffect, job_events::JobEvent, log, state::PartitionWorkingState, PartitionCtx,
};

mod effects;

use futures::StreamExt;
use tokio_util::sync::CancellationToken;
use utils_rs::prelude::tokio::{sync::mpsc::Sender, task::JoinHandle};

struct TokioPartitionWorkerHandle {
    mux: TokioEntryMuxHandle,
    effect_worker: crate::partition::tokio::effects::TokioEffectWorkerHandle,
}

pub async fn start_tokio_worker(
    pcx: PartitionCtx,
    working_state: Arc<PartitionWorkingState>,
) -> TokioPartitionWorkerHandle {
    let effect_worker = effects::start_tokio_effect_worker(pcx.clone(), working_state.clone());
    let mux = start_tokio_entry_mux(
        pcx.clone(),
        working_state.clone(),
        effect_worker.inbox.clone(),
    );

    TokioPartitionWorkerHandle { mux, effect_worker }
}

struct TokioEntryMuxHandle {
    cancel_token: CancellationToken,
    join_handle: JoinHandle<Res<()>>,
}

impl TokioEntryMuxHandle {
    pub async fn close(self) -> Res<()> {
        self.cancel_token.cancel();
        self.join_handle.await.wrap_err("join error")?
    }
}

fn start_tokio_entry_mux(
    pcx: PartitionCtx,
    state: Arc<PartitionWorkingState>,
    effect_tx: Sender<Vec<PartitionEffect>>,
) -> TokioEntryMuxHandle {
    let cancel_token = CancellationToken::new();

    let fut = {
        let cancel_token = cancel_token.clone();
        async move {
            let mut worker = TokioPartitionWorker {
                log: pcx.log_ref(),
                pcx: pcx.clone(),
                state,
                effects: default(),
                effect_tx,
            };
            let mut stream = pcx.log.tail(0).await;
            loop {
                let entry = tokio::select! {
                    biased;
                    _ = cancel_token.cancelled() => {
                        break;
                    }
                    entry = stream.next() => {
                        entry
                    }
                };
                let Some(Ok((entry_id, entry))) = entry else {
                    break;
                };
                worker.reduce(entry_id, entry).await?;
            }
            eyre::Ok(())
        }
    };
    let join_handle = tokio::spawn(fut);

    TokioEntryMuxHandle {
        cancel_token,
        join_handle,
    }
}

struct TokioPartitionWorker {
    pcx: PartitionCtx,
    log: crate::partition::log::PartitionLogRef,
    state: Arc<PartitionWorkingState>,
    effects: Vec<PartitionEffect>,
    effect_tx: Sender<Vec<PartitionEffect>>,
}

impl TokioPartitionWorker {
    async fn reduce(&mut self, entry_id: u64, entry: Arc<[u8]>) -> Res<()> {
        {
            let old = self.state.last_applied_entry_id.load(Ordering::Relaxed);
            debug_assert!(entry_id <= old, "invariant");
        }
        let evt: log::PartitionLogEntry = serde_json::from_slice(&entry).expect(ERROR_JSON);
        match evt {
            log::PartitionLogEntry::JobEvent(job_event) => {
                self.handle_job_event(entry_id, job_event).await?;
            }
            log::PartitionLogEntry::PartitionEffects(effects) => {
                self.effect_tx.send(effects).await.wrap_err(ERROR_CHANNEL)?
            }
        };

        self.state
            .last_applied_entry_id
            .store(entry_id, Ordering::SeqCst);

        Ok(())
    }

    async fn handle_job_event(&mut self, source_entry_id: u64, evt: JobEvent) -> Res<()> {
        crate::partition::reduce::reduce_job_event(&self.state.jobs, evt, &mut self.effects);

        // NOTE: this little dance gives as arena like semantics
        // without Drop issues
        let mut entry = log::PartitionEffectsLogEntry {
            source_entry_id,
            effects: vec![],
        };
        std::mem::swap(&mut self.effects, &mut entry.effects);

        let mut entry = log::PartitionLogEntry::PartitionEffects(entry);
        self.log.append(&entry).await?;

        std::mem::swap(&mut self.effects, {
            let log::PartitionLogEntry::PartitionEffects(entry) = &mut entry else {
                unreachable!()
            };
            &mut entry.effects
        });
        self.effects.clear();
        Ok(())
    }

    async fn handle_partition_effects(
        &mut self,
        effects: log::PartitionEffectsLogEntry,
    ) -> Res<()> {
        Ok(())
    }
}
