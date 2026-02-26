//! FIXME: have the reducer explicitly know the N of effect workers
//! and assign jobs directly with acks

use crate::interlude::*;

use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use futures::StreamExt;
use tokio_util::sync::CancellationToken;
use utils_rs::prelude::tokio::task::JoinHandle;

use wflow_core::partition::{effects, log};

use crate::partition::{
    state::JobCounts, state::PartitionWorkingState, EffectCancelTokens, PartitionCtx,
    PartitionLogRef, WorkerEffectSenders,
};
use wflow_core::snapstore::SnapStore;

pub struct TokioPartitionReducerHandle {
    cancel_token: CancellationToken,
    join_handle: Option<JoinHandle<()>>,
}

impl TokioPartitionReducerHandle {
    pub async fn stop(mut self) -> Res<()> {
        self.cancel_token.cancel();
        // Move out the join_handle to await it
        let join_handle = self.join_handle.take().expect("join_handle already taken");
        utils_rs::wait_on_handle_with_timeout(join_handle, 15 * 1000).await?;
        Ok(())
    }
}

impl Drop for TokioPartitionReducerHandle {
    fn drop(&mut self) {
        self.cancel_token.cancel();
        if let Some(join_handle) = self.join_handle.take() {
            join_handle.abort()
        }
    }
}

pub fn start_tokio_partition_reducer(
    pcx: PartitionCtx,
    state: Arc<PartitionWorkingState>,
    effect_cancel_tokens: EffectCancelTokens,
    effect_tx: async_channel::Sender<effects::EffectId>,
    worker_effect_senders: WorkerEffectSenders,
    cancel_token: CancellationToken,
    snap_store: Arc<dyn SnapStore<Snapshot = Arc<[u8]>>>,
) -> TokioPartitionReducerHandle {
    let start_offset = {
        let last_applied = state.last_applied_entry_id.load(Ordering::Relaxed);
        if last_applied == 0 {
            0
        } else {
            last_applied.saturating_add(1)
        }
    };
    let span = tracing::info_span!(
        "TokioPartitionReducer",
        partition_id = ?pcx.id,
    );

    let fut = {
        let cancel_token = cancel_token.clone();
        async move {
            let latest_entry_id_at_start = pcx
                .log
                .latest_idx()
                .await
                .wrap_err("error getting latest id from log")?;
            let replay_is_empty =
                latest_entry_id_at_start == 0 || start_offset > latest_entry_id_at_start;

            let mut worker = TokioPartitionReducer {
                log: pcx.log_ref(),
                pcx: pcx.clone(),
                state,
                effect_cancel_tokens,
                new_effects: default(),
                event_effects: default(),
                effect_tx,
                worker_effect_senders,
                snapstore: snap_store,
                entries_since_snapshot: 0,
                last_snapshot_time: Timestamp::now(),
                last_snapshotted_entry_id: start_offset.saturating_sub(1),
                replay_latest_entry_id: latest_entry_id_at_start,
                replay_seen_effect_sources: default(),
                did_reschedule_after_replay: false,
            };

            worker
                .index_existing_effect_sources(start_offset, latest_entry_id_at_start)
                .await?;

            let log = pcx.log_ref();
            let mut stream = log.tail(start_offset);
            let mut snapshot_interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
            snapshot_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            debug!("starting");
            if replay_is_empty {
                worker.reschedule_effects_after_replay().await?;
            }
            loop {
                // Poll the stream with cancellation check
                tokio::select! {
                    biased;
                    _ = cancel_token.cancelled() => {
                        break;
                    }
                    _ = snapshot_interval.tick() => {
                        debug!("taking interval snapshot");
                        // Time-based snapshot check
                        worker.check_and_snapshot().await?;
                    }
                    // FIXME: we need backpresure here
                    entry = stream.next() => {
                        let Some(entry) = entry else {
                            warn!("log stream closed");
                            // Stream ended
                            break;
                        };
                        let (idx, entry) = entry?;
                        if let Some(entry) = entry {
                            worker.reduce(idx, entry).await?;
                            if !replay_is_empty && idx >= latest_entry_id_at_start && !worker.did_reschedule_after_replay {
                                worker.reschedule_effects_after_replay().await?;
                            }
                        };
                    }
                }
            }

            debug!("shutting down, taking final snapshot");
            worker
                .check_and_snapshot()
                .await
                .wrap_err("failed to save final snapshot on shutdown")?;

            eyre::Ok(())
        }
    }
    .boxed()
    .instrument(span);
    let join_handle = tokio::spawn(async { fut.await.unwrap() });

    TokioPartitionReducerHandle {
        cancel_token,
        join_handle: Some(join_handle),
    }
}

struct TokioPartitionReducer {
    pcx: PartitionCtx,
    log: PartitionLogRef,
    state: Arc<PartitionWorkingState>,
    effect_cancel_tokens: EffectCancelTokens,
    new_effects: Vec<(effects::EffectId, Option<Arc<str>>)>,
    event_effects: Vec<effects::PartitionEffect>,
    effect_tx: async_channel::Sender<effects::EffectId>,
    worker_effect_senders: WorkerEffectSenders,
    snapstore: Arc<dyn SnapStore<Snapshot = Arc<[u8]>>>,
    entries_since_snapshot: u64,
    last_snapshot_time: Timestamp,
    last_snapshotted_entry_id: u64,
    replay_latest_entry_id: u64,
    replay_seen_effect_sources: HashSet<u64>,
    did_reschedule_after_replay: bool,
}

impl TokioPartitionReducer {
    async fn schedule_effect(
        &self,
        effect_id: effects::EffectId,
        preferred_worker_id: Option<&Arc<str>>,
    ) -> Res<()> {
        if let Some(worker_id) = preferred_worker_id {
            if let Some(tx) = self.worker_effect_senders.get(worker_id) {
                if tx.send(effect_id.clone()).await.is_ok() {
                    debug!(?effect_id, %worker_id, routing = "direct", "scheduled effect");
                    return Ok(());
                }
                warn!(?effect_id, %worker_id, "direct worker queue send failed; falling back");
            }
        }
        debug!(?effect_id, routing = "shared", "scheduled effect");
        self.effect_tx.send(effect_id).await?;
        Ok(())
    }

    async fn reschedule_effects_after_replay(&mut self) -> Res<()> {
        if self.did_reschedule_after_replay {
            return Ok(());
        }
        let effects_to_reschedule = {
            let effects_map = self.state.read_effects().await;
            effects_map
                .iter()
                .map(|(effect_id, effect)| {
                    (
                        effect_id.clone(),
                        matches!(effect.deets, effects::PartitionEffectDeets::RunJob(..)),
                    )
                })
                .collect::<Vec<_>>()
        };
        for (effect_id, is_run_job) in effects_to_reschedule {
            if is_run_job {
                let cancel_token = CancellationToken::new();
                self.effect_cancel_tokens
                    .lock()
                    .await
                    .insert(effect_id.clone(), cancel_token);
            }
            info!(?effect_id, "rescheduling effect back after re-boot");
            // Do not honor sticky worker hints during replay recovery. Warm sessions are process-local.
            self.schedule_effect(effect_id.clone(), None)
                .await
                .wrap_err("failed to schedule effect at startup")?;
        }
        self.did_reschedule_after_replay = true;
        Ok(())
    }

    async fn index_existing_effect_sources(
        &mut self,
        start_offset: u64,
        latest_entry_id_at_start: u64,
    ) -> Res<()> {
        if start_offset > latest_entry_id_at_start {
            return Ok(());
        }
        let mut stream = self.log.tail(start_offset);
        while let Some(entry) = stream.next().await {
            let (entry_id, entry) = entry?;
            if let Some(log::PartitionLogEntry::JobPartitionEffects(entry)) = entry {
                self.replay_seen_effect_sources
                    .insert(entry.source_entry_id);
            }
            if entry_id >= latest_entry_id_at_start {
                break;
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip(self, entry))]
    async fn reduce(&mut self, entry_id: u64, entry: log::PartitionLogEntry) -> Res<()> {
        {
            let old = self.state.last_applied_entry_id.load(Ordering::Relaxed);
            if old > 0 {
                assert!(entry_id > old, "invariant {entry_id} <= {old}");
            }
        }
        match entry {
            log::PartitionLogEntry::JobEffectResult(..)
            | log::PartitionLogEntry::JobInit(..)
            | log::PartitionLogEntry::JobCancel(..) => {
                self.handle_job_event(entry_id, entry).await?;
            }
            log::PartitionLogEntry::JobPartitionEffects(effects) => {
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

    #[tracing::instrument(skip(self))]
    async fn check_and_snapshot(&mut self) -> Res<()> {
        let entry_id = self.state.last_applied_entry_id.load(Ordering::Relaxed);
        // Only snapshot if we haven't already snapshotted this entry
        if entry_id > self.last_snapshotted_entry_id {
            debug!(latest_entry_id = ?entry_id, "snapshotting state");
            let snap = {
                let jobs_guard = self.state.read_jobs().await;
                let effects_guard = self.state.read_effects().await;
                self.snapstore.prepare_snapshot(
                    self.pcx.id,
                    entry_id,
                    wflow_core::snapstore::PartitionSnapshotRef {
                        jobs: &jobs_guard,
                        effects: &effects_guard,
                    },
                )?
            };
            self.snapstore
                .save_snapshot(self.pcx.id, entry_id, snap)
                .await
                .wrap_err("failed to save snapshot")?;
            self.entries_since_snapshot = 0;
            self.last_snapshot_time = Timestamp::now();
            self.last_snapshotted_entry_id = entry_id;
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn handle_partition_effect(
        &mut self,
        entry_id: u64,
        entry: log::JobPartitionEffectsLogEntry,
    ) -> Res<()> {
        debug!(?entry, "reducing partition event");
        for (ii, effect) in entry.effects.into_iter().enumerate() {
            let id = effects::EffectId {
                entry_id,
                effect_idx: ii as u64,
            };
            let preferred_worker_id = match &effect.deets {
                effects::PartitionEffectDeets::RunJob(run) => run.preferred_worker_id.clone(),
                _ => None,
            };
            if let effects::PartitionEffectDeets::RunJob(..) = &effect.deets {
                let cancel_token = CancellationToken::new();
                self.effect_cancel_tokens
                    .lock()
                    .await
                    .insert(id.clone(), cancel_token);
            }
            {
                let mut effects_map = self.state.write_effects().await;
                effects_map.insert(id.clone(), effect);
            }
            if entry_id > self.replay_latest_entry_id {
                self.new_effects.push((id, preferred_worker_id));
            }
        }
        // Send all effects to the channel
        let pending_effect_ids = std::mem::take(&mut self.new_effects);
        for (effect_id, preferred_worker_id) in pending_effect_ids {
            self.schedule_effect(effect_id, preferred_worker_id.as_ref())
                .await?;
        }
        Ok(())
    }

    #[tracing::instrument(skip(self, entry_id))]
    async fn handle_job_event(&mut self, entry_id: u64, evt: log::PartitionLogEntry) -> Res<()> {
        debug!("reducing job event");

        let new_counts = {
            let mut jobs = self.state.write_jobs().await;
            match evt {
                log::PartitionLogEntry::JobInit(evt) => {
                    wflow_core::partition::reduce::reduce_job_init_event(
                        &mut jobs,
                        &mut self.event_effects,
                        evt,
                    )
                }
                log::PartitionLogEntry::JobEffectResult(evt) => {
                    {
                        let mut effects = self.state.write_effects().await;
                        effects.remove(&evt.effect_id);
                    }
                    wflow_core::partition::reduce::reduce_job_run_event(
                        &mut jobs,
                        &mut self.event_effects,
                        evt,
                    )
                }
                log::PartitionLogEntry::JobCancel(evt) => {
                    wflow_core::partition::reduce::reduce_job_cancel_event(
                        &mut jobs,
                        &mut self.event_effects,
                        evt,
                    )
                }
                log::PartitionLogEntry::JobPartitionEffects(_) => {
                    unreachable!()
                }
            };
            // Calculate new counts after state update
            JobCounts {
                active: jobs.active.len(),
                archive: jobs.archive.len(),
            }
        };
        self.state.notify_counts_changed(new_counts);

        if !self.event_effects.is_empty() {
            let replay_already_emitted = entry_id <= self.replay_latest_entry_id
                && self.replay_seen_effect_sources.contains(&entry_id);
            if replay_already_emitted {
                self.event_effects.clear();
                return Ok(());
            }
            // NOTE: this little dance gives as arena like semantics
            // without Drop issues
            let mut entry = log::JobPartitionEffectsLogEntry {
                source_entry_id: entry_id,
                effects: vec![],
            };
            std::mem::swap(&mut self.event_effects, &mut entry.effects);

            let mut entry = log::PartitionLogEntry::JobPartitionEffects(entry);
            self.log.append(&entry).await?;

            std::mem::swap(&mut self.event_effects, {
                let log::PartitionLogEntry::JobPartitionEffects(entry) = &mut entry else {
                    unreachable!()
                };
                &mut entry.effects
            });
            self.event_effects.clear();
        }

        Ok(())
    }
}
