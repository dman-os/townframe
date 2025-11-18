use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::interlude::*;

use crate::partition::{effects, job_events, log, state, PartitionCtx};

mod effect_worker;

use futures::{stream, StreamExt};
use tokio_util::sync::CancellationToken;
use utils_rs::prelude::tokio::task::JoinHandle;

pub struct TokioPartitionWorkerHandle {
    part_reducer: Option<TokioPartitionReducerHandle>,
    effect_workers: Option<Vec<effect_worker::TokioEffectWorkerHandle>>,
    cancel_token: CancellationToken,
}

impl TokioPartitionWorkerHandle {
    pub async fn close(mut self) -> Res<()> {
        self.cancel_token.cancel();
        // Close all effect workers first
        if let Some(effect_workers) = self.effect_workers.take() {
            for worker in effect_workers {
                worker.close().await?;
            }
        }
        // Then close the event worker
        if let Some(reducer) = self.part_reducer.take() {
            reducer.close().await?;
        }
        // Drop will cancel again, which is safe (idempotent)
        Ok(())
    }
}

impl Drop for TokioPartitionWorkerHandle {
    fn drop(&mut self) {
        self.cancel_token.cancel();
        // Cancel all effect workers
        if let Some(ref effect_workers) = self.effect_workers {
            for worker in effect_workers {
                worker.cancel();
            }
        }
        // Cancel the event worker
        if let Some(ref event_worker) = self.part_reducer {
            event_worker.cancel();
        }
    }
}

pub async fn start_tokio_worker(
    pcx: PartitionCtx,
    working_state: Arc<state::PartitionWorkingState>,
) -> TokioPartitionWorkerHandle {
    let cancel_token = CancellationToken::new();
    let mut effect_workers = vec![];
    // Shared channel for effect scheduling
    let (effect_tx, effect_rx) = async_channel::unbounded::<effects::EffectId>();
    for _ii in 0..8 {
        effect_workers.push(effect_worker::start_tokio_effect_worker(
            pcx.clone(),
            working_state.clone(),
            effect_rx.clone(),
            cancel_token.child_token(),
        ));
    }
    let part_reducer = start_tokio_partition_reducer(
        pcx.clone(),
        working_state.clone(),
        effect_tx,
        cancel_token.child_token(),
    );

    TokioPartitionWorkerHandle {
        part_reducer: Some(part_reducer),
        effect_workers: Some(effect_workers),
        cancel_token,
    }
}

struct TokioPartitionReducerHandle {
    cancel_token: CancellationToken,
    join_handle: Option<JoinHandle<Res<()>>>,
}

impl TokioPartitionReducerHandle {
    pub fn cancel(&self) {
        self.cancel_token.cancel();
    }

    pub async fn close(mut self) -> Res<()> {
        self.cancel_token.cancel();
        // Move out the join_handle to await it
        let join_handle = self.join_handle.take().expect("join_handle already taken");
        join_handle.await.wrap_err("join error")?
    }
}

impl Drop for TokioPartitionReducerHandle {
    fn drop(&mut self) {
        self.cancel_token.cancel();
    }
}

fn start_tokio_partition_reducer(
    pcx: PartitionCtx,
    state: Arc<state::PartitionWorkingState>,
    effect_tx: async_channel::Sender<effects::EffectId>,
    cancel_token: CancellationToken,
) -> TokioPartitionReducerHandle {
    let fut = {
        let cancel_token = cancel_token.clone();
        async move {
            let mut worker = TokioPartitionReducer {
                log: pcx.log_ref(),
                pcx: pcx.clone(),
                state,
                new_effects: default(),
                event_effects: default(),
                effect_tx,
            };
            let mut stream = pcx.log.tail(0).await;
            loop {
                // Poll the stream with cancellation check
                tokio::select! {
                    biased;
                    _ = cancel_token.cancelled() => {
                        break;
                    }
                    // we avoid processing entries if there are many effects
                    // that needs to be scheduled
                    entry = stream.next(), if worker.new_effects.len() < 128 => {
                        let Some(entry) = entry else {
                            // Stream ended
                            break;
                        };
                        let (entry_id, entry) = entry?;
                        worker.reduce(entry_id, entry).await?;
                    }
                }
            }
            eyre::Ok(())
        }
    };
    let join_handle = tokio::spawn(fut);

    TokioPartitionReducerHandle {
        cancel_token,
        join_handle: Some(join_handle),
    }
}

struct TokioPartitionReducer {
    pcx: PartitionCtx,
    log: crate::partition::log::PartitionLogRef,
    state: Arc<state::PartitionWorkingState>,
    new_effects: Vec<effects::EffectId>,
    event_effects: Vec<effects::PartitionEffect>,
    effect_tx: async_channel::Sender<effects::EffectId>,
}

impl TokioPartitionReducer {
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
            log::PartitionLogEntry::NewPartitionEffects(effects) => {
                self.handle_partition_effect(entry_id, effects).await?;
            }
        };

        self.state
            .last_applied_entry_id
            .store(entry_id, Ordering::SeqCst);

        Ok(())
    }

    async fn handle_partition_effect(
        &mut self,
        entry_id: u64,
        entry: log::NewPartitionEffectsLogEntry,
    ) -> Res<()> {
        for (ii, effect) in entry.effects.into_iter().enumerate() {
            let id = effects::EffectId {
                entry_id,
                effect_idx: ii as u64,
            };
            {
                let mut effects_map = self.state.effects.lock().await;
                effects_map.insert(id.clone(), effect);
            }
            self.new_effects.push(id);
        }
        // Send all effects to the channel
        for effect_id in self.new_effects.drain(..) {
            self.effect_tx.send(effect_id).await?;
        }
        Ok(())
    }

    async fn handle_job_event(&mut self, entry_id: u64, evt: job_events::JobEvent) -> Res<()> {
        tracing::debug!(%entry_id, ?evt, "reducing job event XXX");
        {
            let mut jobs = self.state.jobs.lock().await;
            crate::partition::reduce::reduce_job_event(&mut jobs, evt, &mut self.event_effects);
        }

        // NOTE: this little dance gives as arena like semantics
        // without Drop issues
        let mut entry = log::NewPartitionEffectsLogEntry {
            source_entry_id: entry_id,
            effects: vec![],
        };
        std::mem::swap(&mut self.event_effects, &mut entry.effects);

        let mut entry = log::PartitionLogEntry::NewPartitionEffects(entry);
        self.log.append(&entry).await?;

        std::mem::swap(&mut self.event_effects, {
            let log::PartitionLogEntry::NewPartitionEffects(entry) = &mut entry else {
                unreachable!()
            };
            &mut entry.effects
        });
        self.event_effects.clear();
        Ok(())
    }
}
