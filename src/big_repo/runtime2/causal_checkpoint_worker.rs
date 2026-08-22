//! Crash-recoverable consumer of the Keyhive **admission** stream that closes
//! every document key transition with causal content coverage before
//! advancing its own durable cursor.
//!
//! The worker tails [`SqliteBigRepoStore::admission_events_after`] — the
//! incorporation log, whose rows exist only after their effects are visible
//! in the keyhive graph — through the generic sans-io machines:
//!
//! - [`CausalCheckpointCore`] — a pure reducer ([`crate::runtime2::driver::StreamMachine`]).
//!   Admission/dedup of event cursors goes through
//!   [`big_sync_core::watermark::WatermarkBook`] and the durable-cursor
//!   watermark advances exactly like
//!   `CursorSyncMachine::drain_ready_cursor_advances` (contiguous ready
//!   prefix). It holds no I/O handles.
//! - [`AdmissionSource`] — an [`crate::runtime2::driver::EventSource`] that polls the
//!   admission log and decodes rows into reduced events.
//! - [`CausalExec`] — a [`crate::runtime2::driver::CmdExecutor`] owning all remaining I/O
//!   (coverage calls, cursor persistence).
//!
//! Startup rule: a durable cursor of 0 means nothing was ever processed, so
//! coverage is closed once for every document keyhive knows; otherwise the
//! stream is tailed from the cursor. There is no gap handling — the arrival
//! log is never pruned, and this worker reads only admitted rows.
use crate::interlude::*;
use crate::keyhive::BigKeyhiveHandle;
use crate::sqlite_big_repo_store::SqliteBigRepoStore;
use big_sync_core::outbox::Outbox;
use big_sync_core::scheduler::{Scheduler, SpawnedTask};
use big_sync_core::watermark::WatermarkBook;
use crate::runtime2::driver;
use future_form::Sendable;
use keyhive_core::event::static_event::StaticEvent;
use std::sync::Arc;
use std::time::{Duration, Instant};

const EVENT_BATCH_SIZE: u32 = 64;
const IDLE_POLL: Duration = Duration::from_millis(25);

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
    _evt_tx: async_channel::Sender<crate::runtime2::Runtime2Evt>,
) -> SpawnedCausalCheckpointWorker<Sendable> {
    let (abort_handle, abort_registration) = futures::future::AbortHandle::new_pair();

    let run = Sendable::from_future(async move {
        let fut = run_causal_checkpoint_tail(store, keyhive, runtime, timer);
        match futures::future::Abortable::new(fut, abort_registration).await {
            Ok(result) => result,
            Err(_) => Ok(()),
        }
    });

    SpawnedCausalCheckpointWorker {
        stop: CausalCheckpointWorkerStopToken { abort: abort_handle },
        run,
    }
}

/// Commands the core emits for its executor. Serial execution order is
/// meaningful: coverage work gates cursor advancement.
#[derive(Debug)]
enum Cmd {
    /// Close a document key transition with causal content coverage.
    EnsureCausalCoverage { doc_id: crate::DocumentId, seq: u64 },
    /// Durably persist the stream watermark.
    AdvanceCursor(u64),
}

/// Events fed back into the reducer.
#[derive(Debug)]
enum Evt {
    /// An admitted row at `seq`; `cgka_doc` set only for CgkaOperation rows
    /// (the sole variant needing coverage work).
    Row { seq: u64, cgka_doc: Option<crate::DocumentId> },
    /// `ensure_causal_coverage` succeeded for the row at `seq`.
    CoverageDone { seq: u64 },
    /// The watermark was durably persisted.
    CursorPersisted(u64),
}

/// Pure reducer: events in, commands out.
#[derive(Default)]
struct CausalCheckpointCore {
    /// Event-cursor admission + contiguous-prefix advancement for the single
    /// admission stream.
    book: WatermarkBook<u64>,
    outbox: Outbox<Cmd, ()>,
    scheduler: Scheduler<()>,
}

impl CausalCheckpointCore {
    /// Admit one row. Rows needing no coverage work finish inline; drain
    /// whatever contiguous prefix is already ready (e.g. an all-non-Cgka
    /// batch).
    fn on_row(&mut self, seq: u64, cgka_doc: Option<crate::DocumentId>) {
        if !self.book.begin(seq) {
            // At-least-once replay of an already-watermarked cursor.
            return;
        }
        match cgka_doc {
            Some(doc_id) => {
                self.outbox.push(Cmd::EnsureCausalCoverage { doc_id, seq }, ());
            }
            None => self.book.finish(seq),
        }
        if let Some(watermark) = self.book.drain() {
            self.outbox.push(Cmd::AdvanceCursor(watermark), ());
        }
    }

    /// Coverage for one row closed; the row settles and the watermark may
    /// advance over the contiguous ready prefix.
    fn on_coverage_done(&mut self, seq: u64) {
        self.book.finish(seq);
        if let Some(watermark) = self.book.drain() {
            self.outbox.push(Cmd::AdvanceCursor(watermark), ());
        }
    }

    /// True when every admitted cursor has been watermarked and the outbox
    /// is empty.
    fn is_settled(&self) -> bool {
        self.book.is_settled() && self.outbox.is_empty()
    }
}

impl crate::runtime2::driver::StreamMachine for CausalCheckpointCore {
    type Evt = Evt;
    type Cmd = Cmd;
    type Seed = ();

    fn on_evt(&mut self, evt: Evt) {
        match evt {
            Evt::Row { seq, cgka_doc } => self.on_row(seq, cgka_doc),
            Evt::CoverageDone { seq } => self.on_coverage_done(seq),
            Evt::CursorPersisted(_) => {}
        }
    }

    fn front_cmd(&mut self) -> Option<(utils_rs::prelude::Uuid, &Cmd)> {
        self.outbox.front().map(|(pending, cmd)| (pending.id(), cmd))
    }

    fn complete_cmd(&mut self, id: utils_rs::prelude::Uuid) {
        drop(self.outbox.complete(id));
    }

    fn complete_job(&mut self, id: big_sync_core::TaskId) {
        self.scheduler.stop(id);
    }

    fn job_completed_evt(&mut self, job: big_sync_core::TaskId) -> Evt {
        unreachable!("causal checkpoint core never spawns scheduled jobs: got {job}")
    }

    fn drain_spawn_queue(&mut self) -> Vec<SpawnedTask<()>> {
        self.scheduler.drain_spawn_queue().collect()
    }

    fn tick_scheduler(&mut self, now: Instant) {
        self.scheduler.tick(now);
    }

    fn is_idle(&mut self) -> bool {
        self.is_settled()
    }
}

/// Polls the admission log via the shared [`driver::AdmissionSource`] and
/// decodes rows into reduced machine events.
/// Replay/fetching stays entirely outside the reducer.
struct AdmissionSource {
    inner: driver::AdmissionSource,
}

impl crate::runtime2::driver::EventSource for AdmissionSource {
    type Evt = Evt;

    async fn next_batch(&mut self) -> Res<Vec<Evt>> {
        let batch = self.inner.next_batch().await?;
        Ok(batch
            .into_iter()
            .flatten()
            .map(|row| {
                let event: StaticEvent<Vec<u8>> = bincode::deserialize(&row.bytes)
                    .expect("persisted keyhive admission event must decode");
                let cgka_doc = match &event {
                    StaticEvent::CgkaOperation(operation) => Some(crate::DocumentId::new(
                        *operation.payload().doc_id().as_bytes(),
                    )),
                    _ => None,
                };
                Evt::Row {
                    seq: row.seq,
                    cgka_doc,
                }
            })
            .collect())
    }
}

/// Executes the reducer's serial commands. Any failure is fatal (loud crash
/// via the task-set unwrap), matching the original error handling; graceful
/// shutdown wins on `is_stopped`.
struct CausalExec {
    store: SqliteBigRepoStore,
    runtime: crate::runtime2::Runtime2Handle<Sendable>,
}

impl crate::runtime2::driver::CmdExecutor<CausalCheckpointCore> for CausalExec {
    async fn execute(
        &mut self,
        cmd: &Cmd,
    ) -> Res<crate::runtime2::driver::ExecOutcome<CausalCheckpointCore>> {
        match *cmd {
            Cmd::EnsureCausalCoverage { doc_id, seq } => {
                if let Err(err) = self.runtime.ensure_causal_coverage(doc_id).await {
                    if self.runtime.is_stopped() {
                        return Ok(crate::runtime2::driver::ExecOutcome::Shutdown);
                    }
                    return Err(err);
                }
                Ok(crate::runtime2::driver::ExecOutcome::Done(Some(Evt::CoverageDone { seq })))
            }
            Cmd::AdvanceCursor(watermark) => {
                self.store.advance_causal_checkpoint_cursor(watermark).await?;
                Ok(crate::runtime2::driver::ExecOutcome::Done(Some(Evt::CursorPersisted(
                    watermark,
                ))))
            }
        }
    }
}

impl driver::SeedRunner<CausalCheckpointCore> for CausalExec {
    fn spawn_seed(
        &mut self,
        task: SpawnedTask<()>,
        _result_tx: &tokio::sync::mpsc::UnboundedSender<
            Result<big_sync_core::TaskId, eyre::Report>,
        >,
        _live: &mut HashMap<big_sync_core::TaskId, tokio::task::JoinHandle<()>>,
    ) {
        unreachable!("causal checkpoint core never spawns scheduled jobs: got {}", task.id)
    }
}

/// No site hooks: coverage gating and cursor persistence are both serial
/// commands; idling needs no maintenance work.
impl driver::DriverHooks<CausalCheckpointCore> for CausalExec {}

async fn run_causal_checkpoint_tail(
    store: SqliteBigRepoStore,
    keyhive: BigKeyhiveHandle,
    runtime: crate::runtime2::Runtime2Handle<Sendable>,
    timer: Arc<dyn crate::runtime2::Timer<Sendable>>,
) -> Res<()> {
    let cursor = store.causal_checkpoint_cursor().await?;
    if cursor == 0 {
        // Full build: nothing was ever processed, so close coverage once for
        // every document keyhive knows. Admission-row replays afterwards are
        // idempotent.
        for doc_obj in keyhive.document_ids().await {
            if runtime.is_stopped() {
                return Ok(());
            }
            let doc_id = crate::DocumentId::new(*doc_obj.as_bytes());
            runtime.ensure_causal_coverage(doc_id).await?;
        }
    }

    let source = AdmissionSource {
        inner: driver::AdmissionSource {
            store: store.clone(),
            timer,
            read_cursor: cursor,
            batch_size: EVENT_BATCH_SIZE,
            idle_poll: IDLE_POLL,
        },
    };
    let mut exec = CausalExec { store, runtime };
    crate::runtime2::driver::run_stream_driver(
        CausalCheckpointCore::default(),
        source,
        &mut exec,
        IDLE_POLL,
        std::future::pending(),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime2::driver::StreamMachine;

    fn row(seq: u64, cgka_doc: Option<u64>) -> (u64, Option<crate::DocumentId>) {
        (
            seq,
            cgka_doc.map(|doc| {
                let mut bytes = [0u8; 32];
                bytes[..8].copy_from_slice(&doc.to_be_bytes());
                crate::DocumentId::new(bytes)
            }),
        )
    }

    #[test]
    fn batch_emits_coverage_cmds_and_advances_over_finished_prefix() {
        let mut core = CausalCheckpointCore::default();
        core.on_row(1, row(1, Some(7)).1);
        core.on_row(2, None);
        core.on_row(3, row(3, Some(9)).1);
        // Only the Cgka rows need coverage work; the plain row finished
        // inline, but nothing drains while row 1 is still pending.
        assert_eq!(core.outbox.len(), 2);
        let (id, cmd) = core.outbox.front().expect("front");
        assert!(matches!(cmd, Cmd::EnsureCausalCoverage { seq: 1, .. }));

        // Driver executes coverage for row 1, reports success, the core
        // settles rows 1..=2 and queues the watermark advance.
        let _ = core.outbox.complete(id.id());
        core.on_evt(Evt::CoverageDone { seq: 1 });
        assert_eq!(core.book.watermark(), Some(2));
        // Row 3's coverage command is still ahead of the advance.
        let (_, cmd) = core.outbox.front().expect("row 3 coverage");
        assert!(matches!(cmd, Cmd::EnsureCausalCoverage { seq: 3, .. }));

        // Row 3 closes: the contiguous prefix reaches 3. The queued
        // advances drain in order: the earlier watermark 2 first, then 3.
        let (id, _) = core.outbox.front().expect("row 3");
        let _ = core.outbox.complete(id.id());
        core.on_evt(Evt::CoverageDone { seq: 3 });
        let (_, cmd) = core.outbox.front().expect("advance 2");
        assert!(matches!(cmd, Cmd::AdvanceCursor(2)));
        let (id, _) = core.outbox.front().expect("advance 2");
        let _ = core.outbox.complete(id.id());
        core.on_evt(Evt::CursorPersisted(2));
        let (_, cmd) = core.outbox.front().expect("advance 3");
        assert!(matches!(cmd, Cmd::AdvanceCursor(3)));
        assert!(!core.is_settled()); // AdvanceCursor(3) not executed yet

        // Driver persists the final watermark, then the machine settles.
        let (id, _) = core.outbox.front().expect("advance 3");
        let _ = core.outbox.complete(id.id());
        core.on_evt(Evt::CursorPersisted(3));
        assert!(core.is_settled());
    }

    #[test]
    fn non_cgka_only_batch_persists_watermark_without_coverage() {
        let mut core = CausalCheckpointCore::default();
        core.on_evt(Evt::Row { seq: 5, cgka_doc: None });
        core.on_evt(Evt::Row { seq: 6, cgka_doc: None });
        // Per-row drain: each finished row queues its own contiguous advance.
        let (_, cmd) = core.outbox.front().expect("advance 5");
        assert!(matches!(cmd, Cmd::AdvanceCursor(5)));
        let (id, _) = core.outbox.front().expect("advance 5");
        let _ = core.outbox.complete(id.id());
        core.on_evt(Evt::CursorPersisted(5));
        let (_, cmd) = core.outbox.front().expect("advance 6");
        assert!(matches!(cmd, Cmd::AdvanceCursor(6)));
        let (id, _) = core.outbox.front().expect("advance 6");
        let _ = core.outbox.complete(id.id());
        core.on_evt(Evt::CursorPersisted(6));
        assert!(core.is_settled());
    }

    #[test]
    fn duplicate_or_stale_rows_are_dropped_by_admission_guard() {
        let mut core = CausalCheckpointCore::default();
        core.on_evt(Evt::Row { seq: 1, cgka_doc: None });
        let (id, _) = core.outbox.front().expect("advance");
        let _ = core.outbox.complete(id.id());
        core.on_evt(Evt::CursorPersisted(1));
        assert!(core.is_settled());

        // Replayed row at/below the watermark: ignored entirely.
        core.on_evt(Evt::Row {
            seq: 1,
            cgka_doc: row(1, Some(4)).1,
        });
        assert!(core.outbox.is_empty());
        assert!(core.is_settled());
    }

    #[test]
    fn idle_machine_reports_idle_repeatedly_without_side_effects() {
        let mut core = CausalCheckpointCore::default();
        assert!(core.is_idle());
        // Idling must not enqueue anything nor mutate state. Admission seqs
        // start at 1 (seq 0 is below the initial watermark guard).
        core.on_evt(Evt::Row { seq: 1, cgka_doc: None });
        assert_eq!(core.book.watermark(), Some(1));
        let (id, _) = core.outbox.front().expect("advance 1");
        let _ = core.outbox.complete(id.id());
        core.on_evt(Evt::CursorPersisted(1));
        assert!(core.is_settled());
        assert!(core.is_idle(), "still idle after settling seq 1");
    }
}
