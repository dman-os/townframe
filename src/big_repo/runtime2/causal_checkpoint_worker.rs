//! Maintains causal coverage for documents named by Keyhive admission events.
//!
//! The worker owns one admission walker, a bounded keyed task scheduler, and
//! the serial cursor outbox. Admission decoding and document coverage are
//! physical tasks; source settlement remains with the walker.

use crate::interlude::*;
use crate::keyhive::BigKeyhiveHandle;
use crate::runtime2::{WorkerGroupScope, keyhive_admission};
use crate::store::sqlite::SqliteBigRepoStore;
use big_sync::delta_walker_state::SqliteDeltaWalkerStateRepo;
use big_sync_core::concurrent_delta_walker::{
    ConcurrentDeltaRead, ConcurrentDeltaWalker, DeltaAck,
};
use big_sync_core::delta_walker_state::{DeltaWalkerStateRepo, DeltaWalkerStateTransaction};
use big_sync_core::revisioned_store::RevisionedStore;
use big_sync_core::outbox::Outbox;
use future_form::Sendable;
use keyhive_core::event::static_event::StaticEvent;
use std::collections::HashMap;
use std::sync::Arc;

const CONCURRENT_TASK_BUDGET: usize = 64;

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

#[allow(clippy::too_many_arguments)]
pub fn spawn_causal_checkpoint_worker(
    store: SqliteBigRepoStore,
    keyhive: BigKeyhiveHandle,
    runtime: crate::runtime2::Runtime2Handle<Sendable>,
    timer: Arc<dyn crate::runtime2::Timer<Sendable>>,
    // FIXME: fuck, probably a copy paste leftover from the group part worker?
    _evt_tx: async_channel::Sender<crate::runtime2::Runtime2Evt>,
    scope: WorkerGroupScope,
) -> SpawnedCausalCheckpointWorker<Sendable> {
    let (abort_handle, abort_registration) = futures::future::AbortHandle::new_pair();
    let run = Sendable::from_future(async move {
        let fut = async move {
            let cursor = store.causal_checkpoint_cursor().await?;
            store
                .register_keyhive_admission_reader(
                    crate::store::sqlite::KEYHIVE_ADMISSION_READER_CAUSAL_CHECKPOINT,
                    cursor,
                )
                .await?;

            if cursor == 0 {
                for doc_obj in keyhive.document_ids().await {
                    if runtime.is_stopped() {
                        return Ok(());
                    }
                    let doc_id = crate::DocumentId::new(*doc_obj.as_bytes());
                    let admitted = match scope.groups() {
                        None => true,
                        Some(_) => scope.admits_doc_groups(
                            &keyhive.group_ids_containing_document(doc_id).await?,
                        ),
                    };
                    if admitted {
                        runtime.ensure_causal_coverage(doc_id).await?;
                    }
                }
            }

            let state = {
                let state = SqliteDeltaWalkerStateRepo::new(
                    store.sql.read_pool.clone(),
                    store.sql.write_pool.clone(),
                    "big_repo.causal_checkpoint",
                    "admission",
                )
                .await?;
                let progress = state.progress().await?.upstream_revision;
                if progress == 0 && cursor > 0 {
                    let mut transaction = state.begin().await?;
                    transaction.advance_from(0, cursor).await?;
                    transaction.commit().await?;
                }
                state
            };
            let source = keyhive_admission::Store {
                store: store.clone(),
                timer,
            };
            let durable = state.progress().await?.upstream_revision;
            let reader = source.open((), durable).await?;
            let admission =
                ConcurrentDeltaWalker::open(reader, state, |row: &keyhive_admission::AdmittedRow| {
                let event: StaticEvent<Vec<u8>> = bincode::deserialize(&row.bytes)
                    .expect("persisted keyhive admission event must decode");
                match event {
                    StaticEvent::CgkaOperation(operation) => FrontierKey::Document(
                        crate::DocumentId::new(*operation.payload().doc_id().as_bytes()),
                    ),
                    _ => FrontierKey::Decode(row.seq),
                }
            })
            .await?;
            let worker = Worker {
                store,
                keyhive,
                runtime,
                scope,
                admission,
                tasks: big_sync_core::tokio_keyed_scheduler::TokioKeyedScheduler::new(
                    CONCURRENT_TASK_BUDGET,
                ),
                pending_admission: HashMap::new(),
                outbox: Outbox::default(),
            };
            worker.machine_loop().await
        };
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

#[derive(Debug)]
enum Cmd {
    AdvanceCursor(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum FrontierKey {
    Decode(u64),
    Document(crate::DocumentId),
}

#[derive(Debug, Clone, Copy)]
struct SourceCursor {
    key: FrontierKey,
    cursor: u64,
}

#[derive(Debug, Clone)]
enum Task {
    EnsureCoverage {
        doc_id: crate::DocumentId,
        source: SourceCursor,
    },
}

#[derive(Debug)]
enum TaskOutput {
    Covered,
    /// The document is outside the worker's group scope: its cursor settles
    /// without causal coverage.
    OutOfScope,
}

struct Worker<'a> {
    store: SqliteBigRepoStore,
    keyhive: BigKeyhiveHandle,
    runtime: crate::runtime2::Runtime2Handle<Sendable>,
    scope: WorkerGroupScope,
    admission: ConcurrentDeltaWalker<
        'a,
        keyhive_admission::Store,
        SqliteDeltaWalkerStateRepo,
        FrontierKey,
    >,
    tasks: big_sync_core::tokio_keyed_scheduler::TokioKeyedScheduler<FrontierKey, Task, TaskOutput>,
    pending_admission: HashMap<crate::DocumentId, SourceCursor>,
    outbox: Outbox<Cmd, ()>,
}

impl<'a> Worker<'a> {
    async fn machine_loop(mut self) -> Res<()> {
        loop {
            let available = CONCURRENT_TASK_BUDGET.saturating_sub(self.tasks.active_count());
            let next_deadline = self.tasks.next_deadline();
            tokio::select! {
                biased;
                completion = self.tasks.next_completion() => {
                    self.on_task_completion(completion?).await?;
                }
                _ = async {
                    if let Some(deadline) = next_deadline {
                        tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => {
                    self.tasks.tick(std::time::Instant::now())?;
                }
                admission = async {
                    if available == 0 {
                        std::future::pending().await
                    } else {
                        self.admission.next(available).await
                    }
                } => {
                    match admission? {
                        ConcurrentDeltaRead::ReplayComplete { .. } => {}
                        ConcurrentDeltaRead::Entries { entries, .. } => {
                            for delta in entries {
                                if std::env::var_os("DAYB_REST_DIAG").is_some() {
                                    tracing::warn!(
                                        ?delta,
                                        durable = self.admission.durable_revision(),
                                        "CAUSAL delta"
                                    );
                                }
                                self.on_admission_delta(delta).await?;
                            }
                        }
                    }
                }
            }
            self.drain_outbox().await?;
            if std::env::var_os("DAYB_REST_DIAG").is_some() {
                tracing::warn!(
                    durable = self.admission.durable_revision(),
                    pending = ?self.admission.pending_jobs(),
                    active = self.tasks.active_count(),
                    "CAUSAL loop iter"
                );
            }
        }
    }

    fn start_task(&mut self, key: FrontierKey, task: Task) -> Res<()> {
        let future = run_task(
            task.clone(),
            self.runtime.clone(),
            self.keyhive.clone(),
            self.scope.clone(),
        );
        self.tasks.replace(key, task, future)?;
        Ok(())
    }

    async fn on_admission_delta(
        &mut self,
        delta: big_sync_core::concurrent_delta_walker::ConcurrentDelta<
            FrontierKey,
            keyhive_admission::AdmittedRow,
        >,
    ) -> Res<()> {
        let source = SourceCursor {
            key: delta.key,
            cursor: delta.cursor,
        };
        match delta.key {
            // The walker's `key_of` already decoded the event; Cgka
            // operations key by document, so no second bincode decode.
            FrontierKey::Document(doc_id) => {
                self.pending_admission
                    .entry(doc_id)
                    .and_modify(|old| {
                        if source.cursor > old.cursor {
                            *old = source;
                        }
                    })
                    .or_insert(source);
                self.start_task(
                    FrontierKey::Document(doc_id),
                    Task::EnsureCoverage { doc_id, source },
                )
            }
            // Admission rows with no document payload only gate the cursor.
            FrontierKey::Decode(_) => {
                if std::env::var_os("DAYB_REST_DIAG").is_some() {
                    tracing::warn!(?source, "CAUSAL decode-key ack");
                }
                self.acknowledge_source(source).await
            }
        }
    }

    async fn on_task_completion(
        &mut self,
        completion: big_sync_core::tokio_keyed_scheduler::TokioTaskCompletion<Task, TaskOutput>,
    ) -> Res<()> {
        match (completion.command, completion.result) {
            (Task::EnsureCoverage { doc_id, source }, TaskOutput::Covered)
            | (Task::EnsureCoverage { doc_id, source }, TaskOutput::OutOfScope) => {
                if std::env::var_os("DAYB_REST_DIAG").is_some() {
                    tracing::warn!(?doc_id, ?source, "CAUSAL coverage done");
                }
                self.acknowledge_source(source).await?;
                if self.pending_admission.get(&doc_id).is_some_and(|pending| {
                    pending.key == source.key && pending.cursor == source.cursor
                }) {
                    self.pending_admission.remove(&doc_id);
                }
            }
            (_, Err(error)) => panic!("causal-checkpoint task failed: {error:?}"),
        }
        Ok(())
    }

    async fn acknowledge_source(&mut self, source: SourceCursor) -> Res<()> {
        if let DeltaAck::Accepted {
            through: Some(through),
        } = self.admission.ack(source.key, source.cursor).await?
        {
            self.outbox.push(Cmd::AdvanceCursor(through), ());
        }
        Ok(())
    }

    async fn drain_outbox(&mut self) -> Res<()> {
        while let Some((pending, cmd)) = self.outbox.front() {
            match cmd {
                Cmd::AdvanceCursor(cursor) => {
                    self.store.advance_causal_checkpoint_cursor(*cursor).await?;
                }
            }
            let (_cmd, _unit) = self.outbox.complete(pending.id());
        }
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_task(
    task: Task,
    runtime: crate::runtime2::Runtime2Handle<Sendable>,
    keyhive: BigKeyhiveHandle,
    scope: WorkerGroupScope,
) -> Res<TaskOutput> {
    match task {
        Task::EnsureCoverage { doc_id, .. } => {
            if let Some(_) = scope.groups()
                && !scope.admits_doc_groups(&keyhive.group_ids_containing_document(doc_id).await?)
            {
                return Ok(TaskOutput::OutOfScope);
            }
            runtime.ensure_causal_coverage(doc_id).await?;
            Ok(TaskOutput::Covered)
        }
    }
}
