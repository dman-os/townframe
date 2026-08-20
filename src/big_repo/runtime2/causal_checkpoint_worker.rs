use crate::interlude::*;
use crate::keyhive::BigKeyhiveHandle;
use crate::sqlite_big_repo_store::SqliteBigRepoStore;
use future_form::Sendable;
use keyhive_core::event::static_event::StaticEvent;
use std::sync::Arc;

const EVENT_BATCH_SIZE: u32 = 64;
const IDLE_POLL: std::time::Duration = std::time::Duration::from_millis(25);

#[derive(Clone)]
pub struct CausalCheckpointWorkerStopToken {
    pub(crate) abort: futures::future::AbortHandle,
}

impl CausalCheckpointWorkerStopToken {
    pub fn cancel(&self) {
        self.abort.abort();
    }
}

pub struct SpawnedCausalCheckpointWorker<F: FutureForm> {
    pub stop: CausalCheckpointWorkerStopToken,
    pub run: F::Future<'static, eyre::Result<()>>,
}

pub fn spawn_causal_checkpoint_worker(
    store: SqliteBigRepoStore,
    keyhive: BigKeyhiveHandle,
    runtime: crate::runtime2::Runtime2Handle<Sendable>,
    timer: Arc<dyn crate::runtime2::Timer<Sendable>>,
    evt_tx: async_channel::Sender<crate::runtime2::Runtime2Evt>,
    state_generation: Arc<std::sync::atomic::AtomicU64>,
) -> SpawnedCausalCheckpointWorker<Sendable> {
    let (abort_handle, abort_registration) = futures::future::AbortHandle::new_pair();
    let worker = CausalCheckpointWorker {
        store,
        _keyhive: keyhive,
        runtime,
        timer,
        evt_tx,
        state_generation,
        last_acked_generation: 0,
    };

    let run = Sendable::from_future(async move {
        match futures::future::Abortable::new(worker.run(), abort_registration).await {
            Ok(result) => result,
            Err(_) => Ok(()),
        }
    });

    SpawnedCausalCheckpointWorker {
        stop: CausalCheckpointWorkerStopToken {
            abort: abort_handle,
        },
        run,
    }
}

/// Crash-recoverable consumer of Keyhive events that closes every document
/// key transition with causal content coverage before advancing its own
/// durable cursor.
struct CausalCheckpointWorker {
    store: SqliteBigRepoStore,
    _keyhive: BigKeyhiveHandle,
    runtime: crate::runtime2::Runtime2Handle<Sendable>,
    timer: Arc<dyn crate::runtime2::Timer<Sendable>>,
    evt_tx: async_channel::Sender<crate::runtime2::Runtime2Evt>,
    state_generation: Arc<std::sync::atomic::AtomicU64>,
    last_acked_generation: u64,
}

impl CausalCheckpointWorker {
    async fn run(mut self) -> Res<()> {
        let mut announced_idle = false;
        loop {
            if self.runtime.is_stopped() {
                return Ok(());
            }
            let generation = self
                .state_generation
                .load(std::sync::atomic::Ordering::Relaxed);
            let cursor = self.store.causal_checkpoint_cursor().await?;
            let events = self
                .store
                .keyhive_events_after(cursor, EVENT_BATCH_SIZE)
                .await?;

            if events.is_empty() {
                if !announced_idle || generation > self.last_acked_generation {
                    self.last_acked_generation = generation;
                    if self
                        .evt_tx
                        .send(
                            crate::runtime2::Runtime2Evt::CausalCheckpointWorkerAdvanced {
                                generation: self.last_acked_generation,
                            },
                        )
                        .await
                        .is_err()
                    {
                        return Ok(());
                    }
                    announced_idle = true;
                }
                let notified = self.store.keyhive_event_notifier();
                tokio::select! {
                    _ = notified.notified() => {}
                    _ = self.timer.sleep(IDLE_POLL) => {}
                }
                continue;
            }

            announced_idle = false;
            // FIXME: do ensure in parallel for all evts
            // and advance to the last cursor
            for row in &events {
                if self.runtime.is_stopped() {
                    return Ok(());
                }
                let event: StaticEvent<Vec<u8>> =
                    bincode::deserialize(&row.bytes).expect("persisted Keyhive event must decode");
                if let StaticEvent::CgkaOperation(operation) = event {
                    let doc_id = crate::DocumentId::new(*operation.payload().doc_id().as_bytes());
                    if let Err(err) = self.runtime.ensure_causal_coverage(doc_id).await {
                        if self.runtime.is_stopped() {
                            return Ok(());
                        }
                        return Err(err);
                    }
                }
                self.store.advance_causal_checkpoint_cursor(row.seq).await?;
            }

            if self.last_acked_generation < generation {
                self.last_acked_generation = generation;
                if self
                    .evt_tx
                    .send(
                        crate::runtime2::Runtime2Evt::CausalCheckpointWorkerAdvanced {
                            generation: self.last_acked_generation,
                        },
                    )
                    .await
                    .is_err()
                {
                    return Ok(());
                }
                announced_idle = true;
            }
        }
    }
}
