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
//!   admission watermark lanes through [`big_sync_core::watermark::WatermarkMachine`].
//!   Decode tasks are keyed by admission sequence; coverage tasks are keyed by
//!   document, so one stalled document does not stop other documents' work.
//! - [`AdmissionSource`] — an [`crate::runtime2::driver::EventSource`] that polls the
//!   admission log and supplies raw rows to keyed decode tasks.
//! - [`CausalExec`] — the driver-side owner of coverage tasks and cursor persistence.
//!
//! Startup rule: a durable cursor of 0 means nothing was ever processed, so
//! coverage is closed once for every document keyhive knows; otherwise the
//! stream is tailed from the cursor. There is no gap handling — the arrival
//! log is never pruned, and this worker reads only admitted rows.
use crate::interlude::*;
use crate::keyhive::BigKeyhiveHandle;
use crate::runtime2::{WorkerGroupScope, driver};
use crate::store::sqlite::SqliteBigRepoStore;
use big_sync_core::outbox::Outbox;
use big_sync_core::scheduler::{KeyedScheduler, SpawnedTask, TaskId};
use big_sync_core::watermark::WatermarkMachine;
use future_form::Sendable;
use keyhive_core::event::static_event::StaticEvent;
use std::collections::{BTreeMap, BTreeSet, HashMap};
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
    scope: WorkerGroupScope,
) -> SpawnedCausalCheckpointWorker<Sendable> {
    let (abort_handle, abort_registration) = futures::future::AbortHandle::new_pair();

    let run = Sendable::from_future(async move {
        let fut = run_causal_checkpoint_tail(store, keyhive, runtime, timer, scope);
        match futures::future::Abortable::new(fut, abort_registration).await {
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

/// Commands the core emits for its executor. Serial execution order is
/// meaningful: coverage work gates cursor advancement.
/// Serial commands emitted by the reducer.
#[derive(Debug)]
enum Cmd {
    AdvanceCursor(u64),
}

#[derive(Debug)]
enum Evt {
    Rows(Vec<driver::AdmittedRow>),
    Decoded {
        seq: u64,
        doc: Option<crate::DocumentId>,
    },
    TaskSettled {
        key: CausalKey,
        covered: BTreeSet<u64>,
    },
    CursorPersisted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum CausalKey {
    Decode(u64),
    Document(crate::DocumentId),
}

#[derive(Debug, Clone)]
enum CausalSeed {
    Decode {
        seq: u64,
        bytes: Arc<[u8]>,
    },
    Document {
        doc: crate::DocumentId,
        covered: BTreeSet<u64>,
    },
}

impl CausalSeed {
    fn key(&self) -> CausalKey {
        match self {
            Self::Decode { seq, .. } => CausalKey::Decode(*seq),
            Self::Document { doc, .. } => CausalKey::Document(*doc),
        }
    }

    fn merge(old: Self, new: Self) -> Self {
        match (old, new) {
            (
                Self::Decode { seq, .. },
                Self::Decode {
                    seq: new_seq,
                    bytes,
                },
            ) => {
                assert_eq!(seq, new_seq, "decode key changed during replacement");
                Self::Decode { seq, bytes }
            }
            (
                Self::Document { doc, mut covered },
                Self::Document {
                    doc: new_doc,
                    covered: new_covered,
                },
            ) => {
                assert_eq!(doc, new_doc, "document key changed during replacement");
                covered.extend(new_covered);
                Self::Document { doc, covered }
            }
            (old, new) => panic!("keyed scheduler seed variant disagrees: {old:?} vs {new:?}"),
        }
    }
}

#[derive(Debug, Clone)]
enum CausalTaskOutput {
    Decoded {
        seq: u64,
        doc: Option<crate::DocumentId>,
    },
    Settled {
        key: CausalKey,
        covered: BTreeSet<u64>,
    },
}

#[derive(Default)]
struct CausalCheckpointCore {
    machine: WatermarkMachine<(), u64, CausalKey, (), u64>,
    outbox: Outbox<Cmd, ()>,
    scheduler: KeyedScheduler<CausalKey, CausalSeed>,
    pending_rows: BTreeMap<u64, driver::AdmittedRow>,
    ready_seeds: Vec<CausalSeed>,
}

impl CausalCheckpointCore {
    fn on_evt(&mut self, evt: Evt) {
        match evt {
            Evt::Rows(rows) => self.on_rows(rows),
            Evt::Decoded { seq, doc } => self.on_decoded(seq, doc),
            Evt::TaskSettled { key, covered } => {
                for seq in covered {
                    self.settle_lane(seq, key);
                }
            }
            Evt::CursorPersisted => {}
        }
    }

    fn on_rows(&mut self, rows: Vec<driver::AdmittedRow>) {
        for row in rows {
            if self.machine.admit((), row.seq) {
                let old = self.pending_rows.insert(row.seq, row);
                debug_assert!(old.is_none(), "admitted sequence was duplicated");
            }
        }
    }

    fn take_pending_rows(&mut self) -> Vec<driver::AdmittedRow> {
        std::mem::take(&mut self.pending_rows)
            .into_values()
            .collect()
    }

    fn schedule_decode(&mut self, now: Instant, row: driver::AdmittedRow) {
        let seq = row.seq;
        self.machine
            .track((), seq, seq, [CausalKey::Decode(seq)], ());
        self.scheduler.replace(
            now,
            CausalKey::Decode(seq),
            CausalSeed::Decode {
                seq,
                bytes: row.bytes,
            },
        );
    }

    fn schedule_seed(&mut self, now: Instant, seed: CausalSeed) {
        let key = seed.key();
        self.scheduler
            .replace_with(now, key, seed, CausalSeed::merge);
    }

    fn take_ready_seeds(&mut self) -> Vec<CausalSeed> {
        std::mem::take(&mut self.ready_seeds)
    }

    fn on_decoded(&mut self, seq: u64, doc: Option<crate::DocumentId>) {
        if let Some(doc) = doc {
            let key = CausalKey::Document(doc);
            self.machine.track((), seq, seq, [key], ());
            self.ready_seeds.push(CausalSeed::Document {
                doc,
                covered: [seq].into_iter().collect(),
            });
        }
        self.settle_lane(seq, CausalKey::Decode(seq));
    }

    fn settle_lane(&mut self, seq: u64, key: CausalKey) {
        for (_, reached) in self.machine.settle(seq, seq, key) {
            if let Some(watermark) = reached {
                self.outbox.push(Cmd::AdvanceCursor(watermark), ());
            }
        }
    }

    fn has_outstanding_work(&self) -> bool {
        !self.pending_rows.is_empty()
            || !self.ready_seeds.is_empty()
            || !self.machine.is_settled(&())
            || !self.outbox.is_empty()
    }
}

impl crate::runtime2::driver::StreamMachine for CausalCheckpointCore {
    type Evt = Evt;
    type Cmd = Cmd;
    type Seed = CausalSeed;

    fn on_evt(&mut self, evt: Evt) {
        CausalCheckpointCore::on_evt(self, evt);
    }

    fn front_cmd(&mut self) -> Option<(utils_rs::prelude::Uuid, &Cmd)> {
        self.outbox
            .front()
            .map(|(pending, cmd)| (pending.id(), cmd))
    }

    fn complete_cmd(&mut self, id: utils_rs::prelude::Uuid) {
        drop(self.outbox.complete(id));
    }

    fn complete_job(&mut self, id: TaskId) -> bool {
        self.scheduler.complete(id)
    }

    fn drain_stop_queue(&mut self) -> Vec<TaskId> {
        self.scheduler.drain_stop_queue().collect()
    }

    fn job_completed_evt(&mut self, _job: TaskId) -> Evt {
        unreachable!("causal checkpoint tasks complete through keyed task results")
    }

    fn drain_spawn_queue(&mut self) -> Vec<SpawnedTask<CausalSeed>> {
        self.scheduler.drain_spawn_queue().collect()
    }

    fn tick_scheduler(&mut self, now: Instant) {
        self.scheduler.tick(now);
    }

    fn is_idle(&mut self) -> bool {
        !self.has_outstanding_work()
    }
}

/// Polls the admission log via the shared [`driver::AdmissionSource`] and
/// decodes rows into reduced machine events.
/// Replay/fetching stays entirely outside the reducer.
struct AdmissionSource {
    inner: driver::AdmissionSource,
}

#[async_trait::async_trait]
impl crate::runtime2::driver::EventSource for AdmissionSource {
    type Evt = Evt;

    async fn next_batch(&mut self) -> Res<Vec<Evt>> {
        Ok(vec![Evt::Rows(
            self.inner
                .next_batch()
                .await?
                .into_iter()
                .flatten()
                .collect(),
        )])
    }
}

/// Executes serial cursor commands and keyed coverage/decode tasks.
struct CausalExec {
    store: SqliteBigRepoStore,
    runtime: crate::runtime2::Runtime2Handle<Sendable>,
    completed: Arc<std::sync::Mutex<HashMap<TaskId, CausalTaskOutput>>>,
    keyhive: BigKeyhiveHandle,
    scope: WorkerGroupScope,
}

#[async_trait::async_trait]
impl crate::runtime2::driver::CmdExecutor<CausalCheckpointCore> for CausalExec {
    async fn execute(
        &mut self,
        cmd: &Cmd,
    ) -> Res<crate::runtime2::driver::ExecOutcome<CausalCheckpointCore>> {
        match *cmd {
            Cmd::AdvanceCursor(watermark) => {
                self.store
                    .advance_causal_checkpoint_cursor(watermark)
                    .await?;
                Ok(crate::runtime2::driver::ExecOutcome::Done(Some(
                    Evt::CursorPersisted,
                )))
            }
        }
    }
}

impl driver::SeedRunner<CausalCheckpointCore> for CausalExec {
    fn spawn_seed(
        &mut self,
        task: SpawnedTask<CausalSeed>,
        result_tx: &tokio::sync::mpsc::UnboundedSender<Result<TaskId, eyre::Report>>,
        live: &mut HashMap<TaskId, tokio::task::JoinHandle<()>>,
    ) {
        let task_id = task.id;
        let seed = task.seed;
        let runtime = self.runtime.clone();
        let keyhive = self.keyhive.clone();
        let scope = self.scope.clone();
        let completed = self.completed.clone();
        let result_tx = result_tx.clone();
        let fut = async move {
            match seed {
                CausalSeed::Decode { seq, bytes } => {
                    let event: StaticEvent<Vec<u8>> = bincode::deserialize(&bytes)
                        .expect("persisted keyhive admission event must decode");
                    let doc = match event {
                        StaticEvent::CgkaOperation(operation) => {
                            let doc =
                                crate::DocumentId::new(*operation.payload().doc_id().as_bytes());
                            scope
                                .admits_doc_groups(
                                    &keyhive.group_ids_containing_document(doc).await,
                                )
                                .then_some(doc)
                        }
                        _ => None,
                    };
                    Ok(CausalTaskOutput::Decoded { seq, doc })
                }
                CausalSeed::Document { doc, covered } => {
                    runtime.ensure_causal_coverage(doc).await?;
                    Ok(CausalTaskOutput::Settled {
                        key: CausalKey::Document(doc),
                        covered,
                    })
                }
            }
        };
        let handle = tokio::spawn(async move {
            let result = fut.await;
            if let Ok(output) = result.as_ref() {
                completed
                    .lock()
                    .expect("causal completion map poisoned")
                    .insert(task_id, output.clone());
            }
            drop(result_tx.send(result.map(|_| task_id)));
        });
        live.insert(task_id, handle);
    }
    fn task_completed(&mut self, _task: TaskId) -> Option<TaskId> {
        None
    }
    fn task_stopped(&mut self, task: TaskId) {
        self.completed
            .lock()
            .expect("causal completion map poisoned")
            .remove(&task);
    }
}

#[async_trait::async_trait]
impl driver::DriverHooks<CausalCheckpointCore> for CausalExec {
    async fn on_task_completed(
        &mut self,
        machine: &mut CausalCheckpointCore,
        task: TaskId,
    ) -> Res<()> {
        let output = self
            .completed
            .lock()
            .expect("causal completion map poisoned")
            .remove(&task)
            .expect("successful task has no completion output");
        match output {
            CausalTaskOutput::Decoded { seq, doc } => {
                machine.on_evt(Evt::Decoded { seq, doc });
            }
            CausalTaskOutput::Settled { key, covered } => {
                machine.on_evt(Evt::TaskSettled { key, covered });
            }
        }
        Ok(())
    }

    async fn pump(&mut self, machine: &mut CausalCheckpointCore) -> Res<()> {
        for row in machine.take_pending_rows() {
            machine.schedule_decode(Instant::now(), row);
        }
        let now = Instant::now();
        for seed in machine.take_ready_seeds() {
            machine.schedule_seed(now, seed);
        }
        Ok(())
    }
}

async fn run_causal_checkpoint_tail(
    store: SqliteBigRepoStore,
    keyhive: BigKeyhiveHandle,
    runtime: crate::runtime2::Runtime2Handle<Sendable>,
    timer: Arc<dyn crate::runtime2::Timer<Sendable>>,
    scope: WorkerGroupScope,
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
            if scope.admits_doc_groups(&keyhive.group_ids_containing_document(doc_id).await) {
                runtime.ensure_causal_coverage(doc_id).await?;
            }
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
    let mut exec = CausalExec {
        store,
        runtime,
        keyhive,
        scope,
        completed: Arc::new(std::sync::Mutex::new(HashMap::new())),
    };
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

    fn doc(value: u64) -> crate::DocumentId {
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&value.to_be_bytes());
        crate::DocumentId::new(bytes)
    }

    fn admit(core: &mut CausalCheckpointCore, seq: u64) {
        assert!(core.machine.admit((), seq));
        core.machine
            .track((), seq, seq, [CausalKey::Decode(seq)], ());
    }

    #[test]
    fn document_tasks_run_independently_but_cursor_waits_for_prefix() {
        let mut core = CausalCheckpointCore::default();
        admit(&mut core, 1);
        admit(&mut core, 2);
        core.on_evt(Evt::Decoded {
            seq: 1,
            doc: Some(doc(7)),
        });
        core.on_evt(Evt::Decoded {
            seq: 2,
            doc: Some(doc(9)),
        });

        core.on_evt(Evt::TaskSettled {
            key: CausalKey::Document(doc(9)),
            covered: [2].into_iter().collect(),
        });
        assert!(core.outbox.is_empty());
        core.on_evt(Evt::TaskSettled {
            key: CausalKey::Document(doc(7)),
            covered: [1].into_iter().collect(),
        });
        assert!(matches!(
            core.outbox.front().map(|(_, cmd)| cmd),
            Some(Cmd::AdvanceCursor(2))
        ));
    }

    #[test]
    fn events_without_documents_settle_decode_lane() {
        let mut core = CausalCheckpointCore::default();
        admit(&mut core, 1);
        core.on_evt(Evt::Decoded { seq: 1, doc: None });
        assert!(matches!(
            core.outbox.front().map(|(_, cmd)| cmd),
            Some(Cmd::AdvanceCursor(1))
        ));
    }

    #[test]
    fn duplicate_admission_is_ignored() {
        let mut core = CausalCheckpointCore::default();
        admit(&mut core, 1);
        assert!(!core.machine.admit((), 1));
    }

    #[test]
    fn idle_has_no_side_effects() {
        let core = CausalCheckpointCore::default();
        assert!(!core.has_outstanding_work());
    }
}
