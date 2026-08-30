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
//! - **Baseline, not rebuild.** Unlike the causal worker (whose cursor of 0
//!   triggers a full rebuild of every document), a janitor cursor of 0
//!   baselines *at the current admission head*: the archive restore already
//!   left the prekey state consistent at boot, so historical Add ops naming
//!   our prekeys predate the janitor and must not be re-rotated.
use crate::interlude::*;
use crate::keyhive::BigKeyhiveHandle;
use crate::store::sqlite::SqliteBigRepoStore;
use keyhive_core::event::static_event::StaticEvent;
use std::time::Duration;

const EVENT_BATCH_SIZE: u32 = 64;
const IDLE_POLL: Duration = Duration::from_millis(25);

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
    pub run: <future_form::Sendable as future_form::FutureForm>::Future<
        'static,
        eyre::Result<()>,
    >,
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
        let event: StaticEvent<Vec<u8>> = bincode::deserialize(&bytes)
            .expect("persisted keyhive admission event must decode");
        if let StaticEvent::CgkaOperation(operation) = event
            && let beekem::operation::CgkaOperation::Add { added_id, pk, .. } =
                operation.payload()
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
    let durable = store.prekey_janitor_cursor().await?;
    let cursor = if durable == 0 {
        // First boot for this reader: baseline at the current admission
        // head (see module docs — the janitor must not replay history; the
        // checkpoint worker's cursor=0 full-build rule does not apply
        // here).
        let head = store.admission_head().await?;
        store.advance_prekey_janitor_cursor(head).await?;
        head
    } else {
        durable
    };
    store
        .register_keyhive_admission_reader(
            crate::store::sqlite::KEYHIVE_ADMISSION_READER_PREKEY_JANITOR,
            cursor,
        )
        .await?;

    let mut cursor = cursor;
    tracing::info!(cursor, "prekey janitor: tail starting");
    loop {
        let rows: Vec<(u64, Vec<u8>)> = store
            .admission_events_after(cursor, EVENT_BATCH_SIZE)
            .await?
            .into_iter()
            .map(|row| (row.seq, row.bytes))
            .collect();
        if rows.is_empty() {
            timer.sleep(IDLE_POLL).await;
            continue;
        }
        match process_admissions(&keyhive, rows).await? {
            Some(handled_through) => {
                cursor = cursor.max(handled_through);
                tracing::debug!(cursor, "prekey janitor: advanced admission cursor");
                store.advance_prekey_janitor_cursor(cursor).await?;
            }
            None => timer.sleep(IDLE_POLL).await,
        }
    }
}

