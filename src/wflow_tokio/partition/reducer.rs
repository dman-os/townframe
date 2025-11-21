use crate::interlude::*;

use std::sync::atomic::Ordering;
use std::sync::Arc;

use futures::StreamExt;
use tokio_util::sync::CancellationToken;
use utils_rs::prelude::tokio::task::JoinHandle;

use wflow_core::partition::{effects, job_events, log};

use crate::partition::{state::PartitionWorkingState, PartitionCtx, PartitionLogRef};
use wflow_core::snapstore::SnapStore;

pub struct TokioPartitionReducerHandle {
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

pub fn start_tokio_partition_reducer(
    pcx: PartitionCtx,
    state: Arc<PartitionWorkingState>,
    effect_tx: async_channel::Sender<effects::EffectId>,
    cancel_token: CancellationToken,
    snap_store: Option<Arc<dyn SnapStore>>,
) -> TokioPartitionReducerHandle {
    let start_offset = state.last_applied_entry_id.load(Ordering::Relaxed);
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
                snap_store,
                entries_since_snapshot: 0,
                last_snapshot_time: OffsetDateTime::now_utc(),
                last_snapshotted_entry_id: start_offset.saturating_sub(1),
            };

            // Schedule any active effects found in the effect state
            {
                let effects = worker.state.read_effects().await;
                for effect_id in effects.keys() {
                    worker
                        .effect_tx
                        .send(effect_id.clone())
                        .await
                        .wrap_err("failed to schedule effect at startup")?;
                }
            }

            let mut stream = pcx.log.tail(start_offset).await;
            let mut snapshot_interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
            snapshot_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                // Poll the stream with cancellation check
                tokio::select! {
                    biased;
                    _ = cancel_token.cancelled() => {
                        break;
                    }
                    _ = snapshot_interval.tick() => {
                        // Time-based snapshot check
                        worker.check_and_snapshot().await?;
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

            // Save final snapshot on shutdown
            worker
                .check_and_snapshot()
                .await
                .wrap_err("failed to save final snapshot on shutdown")?;

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
    log: PartitionLogRef,
    state: Arc<PartitionWorkingState>,
    new_effects: Vec<effects::EffectId>,
    event_effects: Vec<effects::PartitionEffect>,
    effect_tx: async_channel::Sender<effects::EffectId>,
    snap_store: Option<Arc<dyn SnapStore>>,
    entries_since_snapshot: u64,
    last_snapshot_time: OffsetDateTime,
    last_snapshotted_entry_id: u64,
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

        // Check if we should snapshot (entry-based)
        self.entries_since_snapshot += 1;
        if self.entries_since_snapshot >= 100 {
            self.check_and_snapshot().await?;
        }

        Ok(())
    }

    async fn check_and_snapshot(&mut self) -> Res<()> {
        let entry_id = self.state.last_applied_entry_id.load(Ordering::SeqCst);

        // Only snapshot if we haven't already snapshotted this entry
        if entry_id > self.last_snapshotted_entry_id {
            if let Some(ref snap_store) = self.snap_store {
                let (jobs, effects) = {
                    let jobs_guard = self.state.read_jobs().await;
                    let effects_guard = self.state.read_effects().await;
                    (jobs_guard.clone(), effects_guard.clone())
                };
                let snapshot = wflow_core::snapstore::PartitionSnapshot { jobs, effects };
                snap_store
                    .save_snapshot(self.pcx.id, entry_id, &snapshot)
                    .await
                    .wrap_err("failed to save snapshot")?;
                self.entries_since_snapshot = 0;
                self.last_snapshot_time = OffsetDateTime::now_utc();
                self.last_snapshotted_entry_id = entry_id;
            }
        }
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
                let mut effects_map = self.state.write_effects().await;
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
            let mut jobs = self.state.write_jobs().await;
            wflow_core::partition::reduce::reduce_job_event(
                &mut jobs,
                evt,
                &mut self.event_effects,
            );
        }

        if !self.event_effects.is_empty() {
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
        }

        Ok(())
    }
}
