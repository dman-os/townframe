//! Durable admission-log consumer that drives the prekey janitor.
//!
//! Modeled on [`super::causal_checkpoint_worker`], but deliberately simpler:
//! the janitor's work is cheap and strictly serial (rotate a consumed
//! prekey, refill under the floor), so there is no keyed-scheduler or
//! watermark machinery — just a durable cursor and an in-order tail.
//!
//! Guarantees:
//!
//! - **At-least-once.** Admission rows exist only after their effects are
//!   visible in the keyhive graph (`register_keyhive_admission_reader`
//!   family, [`admission_events_after`]); the durable cursor is only
//!   advanced after a row's housekeeping *completed successfully*, so a
//!   crash or runtime outage re-reads the un-advanced window on the next
//!   boot. Replay safety is structural, not tracked: a replayed
//!   `CgkaOperation::Add` naming an already-rotated prekey fails the
//!   published-set precheck (the rotate tombstoned it out of `prekeys()`),
//!   so reprocessing an old row is a no-op.
//! - **Replay from durable progress.** A new janitor starts at revision zero and
//!   replays the admission log through the same serial walker used by other
//!   durable consumers. Replaying an already-handled Add is safe because the
//!   published-set guard makes it a no-op.
use crate::interlude::*;
use crate::keyhive::BigKeyhiveHandle;
use crate::runtime2::keyhive_admission;
use crate::store::sqlite::SqliteBigRepoStore;
use big_sync::SqliteDeltaWalkerStateRepo;
use big_sync_core::delta_walker_state::DeltaWalkerStateRepo;
use big_sync_core::revisioned_store::{RevisionRead, RevisionedStore};
use big_sync_core::serial_delta_walker::SerialDeltaWalker;
use keyhive_core::event::static_event::StaticEvent;

pub struct PrekeyJanitorWorkerStopToken {
    pub(crate) abort: futures::future::AbortHandle,
}

impl PrekeyJanitorWorkerStopToken {
    pub fn cancel(&self) {
        self.abort.abort();
    }
}

pub struct SpawnedPrekeyJanitorWorker {
    pub stop: PrekeyJanitorWorkerStopToken,
    pub run: <future_form::Sendable as future_form::FutureForm>::Future<'static, eyre::Result<()>>,
}

pub fn spawn_prekey_janitor_worker(
    store: SqliteBigRepoStore,
    keyhive: BigKeyhiveHandle,
    timer: std::sync::Arc<dyn crate::runtime2::Timer<future_form::Sendable>>,
) -> SpawnedPrekeyJanitorWorker {
    let (abort_handle, abort_registration) = futures::future::AbortHandle::new_pair();

    let run = future_form::Sendable::from_future(async move {
        let fut = run_prekey_janitor_tail(store, keyhive, timer);
        match futures::future::Abortable::new(fut, abort_registration).await {
            Ok(result) => result,
            Err(_) => Ok(()),
        }
    });

    SpawnedPrekeyJanitorWorker {
        stop: PrekeyJanitorWorkerStopToken {
            abort: abort_handle,
        },
        run,
    }
}

/// Process one batch of admitted rows in causal order, running the janitor
/// policy for each `CgkaOperation::Add` naming our own published prekeys.
///
/// Returns the sequence through which the durable cursor may advance
/// (`None` when the batch is empty). Any housekeeping failure aborts the
/// batch with the un-advanced rows left for the next poll.
pub(crate) async fn process_admissions(
    keyhive: &BigKeyhiveHandle,
    rows: Vec<(u64, Vec<u8>)>,
) -> Res<Option<u64>> {
    let mut handled_through: Option<u64> = None;
    for (seq, bytes) in rows {
        let event: StaticEvent<Vec<u8>> =
            bincode::deserialize(&bytes).expect("persisted keyhive admission event must decode");
        if let StaticEvent::CgkaOperation(operation) = event
            && let beekem::operation::CgkaOperation::Add { added_id, pk, .. } = operation.payload()
        {
            super::prekey_janitor::housekeep_after_add(keyhive, added_id, pk).await?;
        }
        handled_through = Some(seq);
    }
    Ok(handled_through)
}

async fn run_prekey_janitor_tail(
    store: SqliteBigRepoStore,
    keyhive: BigKeyhiveHandle,
    timer: std::sync::Arc<dyn crate::runtime2::Timer<future_form::Sendable>>,
) -> Res<()> {
    let state = SqliteDeltaWalkerStateRepo::new(
        store.sql.read_pool.clone(),
        store.sql.write_pool.clone(),
        "big_repo.prekey_janitor",
        "admission",
    )
    .await?;
    let source = keyhive_admission::Store {
        store: store.clone(),
        timer,
    };
    let durable = state.progress().await?.upstream_revision;
    // Keep the admission-log retention floor aware of this walker. The
    // generic walker state is authoritative for replay, while this reader row
    // protects unprocessed events from maintenance pruning.
    store
        .register_keyhive_admission_reader(
            crate::store::sqlite::KEYHIVE_ADMISSION_READER_PREKEY_JANITOR,
            durable,
        )
        .await?;
    let reader = source.open((), durable).await?;
    let mut walker: SerialDeltaWalker<'_, keyhive_admission::Store, SqliteDeltaWalkerStateRepo> =
        SerialDeltaWalker::open(reader, &state).await?;
    tracing::info!(cursor = durable, "prekey janitor: tail starting");

    loop {
        match walker.next().await? {
            RevisionRead::ReplayComplete { through } => {
                tracing::debug!(through, "prekey janitor: admission replay complete");
            }
            RevisionRead::Entries { revision, entries } => {
                let rows: Vec<(u64, Vec<u8>)> = entries
                    .into_iter()
                    .map(|row: keyhive_admission::AdmittedRow| (row.seq, row.bytes.to_vec()))
                    .collect();
                process_admissions(&keyhive, rows).await?;
                walker.settle(revision).await?;
                store
                    .register_keyhive_admission_reader(
                        crate::store::sqlite::KEYHIVE_ADMISSION_READER_PREKEY_JANITOR,
                        revision,
                    )
                    .await?;
                tracing::debug!(revision, "prekey janitor: advanced admission cursor");
            }
        }
    }
}
