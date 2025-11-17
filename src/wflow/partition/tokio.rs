use std::sync::atomic::Ordering;

use crate::interlude::*;

use crate::metastore;
use crate::partition::{effects, job_events, log, state, PartitionCtx};

mod effect_worker;

use futures::StreamExt;
use tokio_util::sync::CancellationToken;
use utils_rs::prelude::tokio::task::JoinHandle;

pub struct TokioPartitionWorkerHandle {
    mux: TokioEntryMuxHandle,
    workers: Vec<effect_worker::TokioEffectWorkerHandle>,
}

pub async fn start_tokio_worker(
    pcx: PartitionCtx,
    working_state: Arc<state::PartitionWorkingState>,
) -> TokioPartitionWorkerHandle {
    let mut workers = vec![];
    let (effect_tx, effect_rx) = async_channel::bounded(16);
    for _ii in 0..16 {
        workers.push(effect_worker::start_tokio_effect_worker(
            pcx.clone(),
            working_state.clone(),
            effect_rx.clone(),
        ));
    }
    let mux = start_tokio_entry_mux(pcx.clone(), working_state.clone(), effect_tx);

    TokioPartitionWorkerHandle { mux, workers }
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
    state: Arc<state::PartitionWorkingState>,
    effect_tx: async_channel::Sender<log::PartitionEffectsLogEntry>,
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
    state: Arc<state::PartitionWorkingState>,
    effects: Vec<effects::PartitionEffect>,
    effect_tx: async_channel::Sender<log::PartitionEffectsLogEntry>,
}

impl TokioPartitionWorker {
    async fn reduce(&mut self, entry_id: u64, entry: Arc<[u8]>) -> Res<()> {
        {
            let old = self.state.last_applied_entry_id.load(Ordering::Relaxed);
            if old > 0 {
                debug_assert!(entry_id > old, "invariant {entry_id} <= {old}");
            }
        }
        let evt: log::PartitionLogEntry = serde_json::from_slice(&entry).expect(ERROR_JSON);
        match evt {
            log::PartitionLogEntry::JobEvent(job_event) => {
                self.handle_job_event(entry_id, job_event).await?;
            }
            log::PartitionLogEntry::PartitionEffects(effects) => {
                self.effect_tx
                    .send(effects)
                    .await
                    .wrap_err("no effect worker active")?;
            }
        };

        self.state
            .last_applied_entry_id
            .store(entry_id, Ordering::SeqCst);

        Ok(())
    }

    async fn handle_job_event(
        &mut self,
        source_entry_id: u64,
        evt: job_events::JobEvent,
    ) -> Res<()> {
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
}
