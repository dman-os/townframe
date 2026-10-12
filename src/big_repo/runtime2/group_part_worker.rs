//! Crash-recoverable maintenance for Keyhive-derived policy and partitions.
//!
//! Admission rows are decoded into affected documents and group parts. The
//! worker owns the dependency graph: every derived task retains the source
//! admission row that caused it, and the source is acknowledged only after all
//! derived work has completed successfully.

use crate::interlude::*;
use crate::keyhive::{BigKeyhiveHandle, EventSubject};
use crate::runtime2::{WorkerGroupScope, keyhive_admission};
use crate::store::sqlite::{GroupPartReconciliation, SqliteBigRepoStore};
use big_sync::delta_walker_state::SqliteDeltaWalkerStateRepo;
use big_sync_core::concurrent_delta_walker::{
    ConcurrentDeltaRead, ConcurrentDeltaWalker, DeltaAck,
};
use big_sync_core::delta_walker_state::DeltaWalkerStateRepo;
use big_sync_core::outbox::Outbox;
use big_sync_core::revisioned_store::RevisionedStore;
use future_form::Sendable;
use keyhive_core::event::static_event::StaticEvent;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

const CONCURRENT_TASK_BUDGET: usize = 64;
const INITIAL_BUILD_CONCURRENCY: usize = 16;

#[derive(Clone)]
pub struct GroupPartWorkerStopToken {
    pub(crate) abort: futures::future::AbortHandle,
}

impl GroupPartWorkerStopToken {
    pub fn cancel(&self) {
        self.abort.abort();
    }
}

pub struct SpawnedGroupPartWorker<F: FutureForm> {
    pub stop: GroupPartWorkerStopToken,
    pub run: F::Future<'static, eyre::Result<()>>,
}

pub fn spawn_group_part_worker(
    store: SqliteBigRepoStore,
    keyhive: BigKeyhiveHandle,
    local_peer_id: PeerKey,
    timer: Arc<dyn crate::runtime2::Timer<Sendable>>,
    clock: Arc<dyn crate::runtime2::Clock>,
    evt_tx: async_channel::Sender<crate::runtime2::Runtime2Evt>,
    scope: WorkerGroupScope,
) -> SpawnedGroupPartWorker<Sendable> {
    let (abort_handle, abort_registration) = futures::future::AbortHandle::new_pair();
    let run = Sendable::from_future(async move {
        let fut = async move {
            let identity = crate::store::sqlite::KEYHIVE_ADMISSION_CONSUMER_GROUP_PART;
            let state = SqliteDeltaWalkerStateRepo::new(
                store.sql.read_pool.clone(),
                store.sql.write_pool.clone(),
                identity.0,
                identity.1,
            )
            .await?;

            // The walker's durable revision is the authority on what has been
            // reconciled: it advances only after every sink task for that prefix has
            // completed, so the reconciliation it names is already durable.
            let progress = state.progress().await?.upstream_revision;
            if progress == 0 {
                let initial_group_parts: HashSet<PartKey> = match scope.groups() {
                    None => store.list_parts().await?,
                    Some(groups) => groups.iter().cloned().collect(),
                };
                for part_id in &initial_group_parts {
                    store.ensure_part(part_id.clone()).await?;
                }
                let initial_group_parts = Arc::new(initial_group_parts);
                let group_agents = Arc::new(GroupAgentsMemo::default());
                let docs = keyhive.document_ids().await;
                let futs = docs.into_iter().map(|doc| {
                    let store = store.clone();
                    let keyhive = keyhive.clone();
                    let initial_group_parts = Arc::clone(&initial_group_parts);
                    let group_agents = Arc::clone(&group_agents);
                    let scope = scope.clone();
                    async move {
                        let doc_id = crate::DocumentId::new(doc.as_bytes());
                        if let Some(_) = scope.groups()
                            && !scope.admits_doc_groups(
                                &keyhive.group_ids_containing_document(doc_id).await?,
                            )
                        {
                            return Ok(());
                        }
                        let reconciliation = reconcile_doc(
                            &keyhive,
                            doc,
                            &initial_group_parts,
                            &scope,
                            &group_agents,
                        )
                        .await?;
                        store.reconcile_group_part_batch(&[reconciliation]).await
                    }
                });
                drive_buffered(futs, INITIAL_BUILD_CONCURRENCY).await?;
            }

            let source = keyhive_admission::Store {
                store: store.clone(),
                // The loop below sleeps on the same timer the admission reader
                // polls with, so both cadences are driven by one seam.
                timer: Arc::clone(&timer),
            };
            let durable = state.progress().await?.upstream_revision;
            let reader = source.open((), durable).await?;
            let admission = ConcurrentDeltaWalker::open(
                reader,
                state,
                |row: &keyhive_admission::AdmittedRow| row.seq,
            )
            .await?;
            let worker = Worker {
                store,
                keyhive,
                local_peer_id,
                evt_tx,
                scope,
                admission,
                tasks: big_sync_core::tokio_keyed_scheduler::TokioKeyedScheduler::new(
                    CONCURRENT_TASK_BUDGET,
                ),
                timer,
                clock,
                pending_sources: HashMap::new(),
                pending_documents: HashMap::new(),
                pending_group_parts: HashMap::new(),
                parked_decodes: HashMap::new(),
                outbox: Outbox::default(),
            };
            worker.machine_loop().await
        };
        match futures::future::Abortable::new(fut, abort_registration).await {
            Ok(result) => result,
            Err(_) => Ok(()),
        }
    });
    SpawnedGroupPartWorker {
        stop: GroupPartWorkerStopToken {
            abort: abort_handle,
        },
        run,
    }
}

#[derive(Debug)]
enum Cmd {
    AnnounceSettled(u64),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum GroupPartKey {
    Decode(u64),
    Document(ObjKey),
    Group(PartKey),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SourceCursor {
    key: u64,
    cursor: u64,
}

#[derive(Debug, Clone)]
enum Task {
    Decode {
        bytes: Arc<[u8]>,
        source: SourceCursor,
    },
    ReconcileDocument {
        doc: ObjKey,
        affected_group_parts: HashSet<PartKey>,
        sources: Vec<SourceCursor>,
    },
    EnsurePart {
        part: PartKey,
        sources: Vec<SourceCursor>,
    },
}

#[derive(Debug)]
enum TaskOutput {
    Decoded(AffectedEvent),
    /// The event's proof chain is not resolvable here yet: the decode is early,
    /// not wrong, and the completion handler parks it.
    Unresolved,
    Reconciled,
    Ensured,
}

#[derive(Debug, Clone, Copy)]
struct PendingSource {
    source: SourceCursor,
    remaining: usize,
}

#[derive(Debug, Default)]
struct PendingDocument {
    sources: Vec<SourceCursor>,
    affected_group_parts: HashSet<PartKey>,
    scheduled: bool,
}

#[derive(Debug, Default)]
struct PendingGroupPart {
    sources: Vec<SourceCursor>,
    scheduled: bool,
}

struct Worker<'a> {
    store: SqliteBigRepoStore,
    keyhive: BigKeyhiveHandle,
    local_peer_id: PeerKey,
    evt_tx: async_channel::Sender<crate::runtime2::Runtime2Evt>,
    scope: WorkerGroupScope,
    admission: ConcurrentDeltaWalker<'a, keyhive_admission::Store, SqliteDeltaWalkerStateRepo, u64>,
    tasks:
        big_sync_core::tokio_keyed_scheduler::TokioKeyedScheduler<GroupPartKey, Task, TaskOutput>,
    /// The loop's wait and its notion of "now" are both injected so a test can
    /// own them and drive the timer arm without wall clock (see
    /// [`crate::runtime2::tasks::manual_time`]). They must be injected together:
    /// a loop that slept on one time source and ticked from another would
    /// compute deadlines against a `now` it never actually waited on.
    timer: Arc<dyn crate::runtime2::Timer<Sendable>>,
    clock: Arc<dyn crate::runtime2::Clock>,
    pending_sources: HashMap<u64, PendingSource>,
    pending_documents: HashMap<ObjKey, PendingDocument>,
    pending_group_parts: HashMap<PartKey, PendingGroupPart>,
    /// Decodes keyed by admission row, parked on a delegate this hive has not
    /// ingested yet. Their source rows stay unsettled while parked.
    parked_decodes: HashMap<u64, Task>,
    outbox: Outbox<Cmd, ()>,
}

impl<'a> Worker<'a> {
    async fn machine_loop(mut self) -> Res<()> {
        loop {
            let available = CONCURRENT_TASK_BUDGET.saturating_sub(self.tasks.active_count());
            let next_deadline = self.tasks.next_deadline();
            // Reads the arm's start instant and clones the timer out of `self` so
            // the select arm below does not borrow the worker while other arms
            // take it mutably.
            let now = self.clock.instant();
            let timer = Arc::clone(&self.timer);
            tokio::select! {
                biased;
                completion = self.tasks.next_completion() => {
                    self.on_task_completion(completion?).await?;
                }
                _ = async {
                    if let Some(deadline) = next_deadline {
                        // Wakes at the same instant `sleep_until(deadline)` did: the
                        // wait is `deadline - now` on the injected timer, and the
                        // subtraction saturates. A deadline the clock has already passed
                        // therefore becomes a zero-length wait — the same already-expired
                        // timer the wall-clock version armed — so both the wake ordering
                        // and the number of loop turns are unchanged.
                        timer
                            .sleep(deadline.saturating_duration_since(now))
                            .await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => {
                    // Read `now` fresh: the clock has moved during the wait, and the
                    // tick must observe the instant the loop actually woke at.
                    self.tasks.tick(self.clock.instant())?;
                }
                admission = async {
                    if available == 0 {
                        std::future::pending().await
                    } else {
                        self.admission
                            .next(std::num::NonZeroUsize::new(available).expect("available is non-zero"))
                            .await
                    }
                } => {
                    match admission? {
                        ConcurrentDeltaRead::ReplayComplete { .. } => {}
                        ConcurrentDeltaRead::Entries { entries, .. } => {
                            tracing::debug!(
                                count = entries.len(),
                                active_tasks = self.tasks.active_count(),
                                pending_sources = self.pending_sources.len(),
                                "group-part admission batch read"
                            );
                            for delta in entries {
                                self.start_task(
                                    GroupPartKey::Decode(delta.entry.seq),
                                    Task::Decode {
                                        bytes: delta.entry.bytes,
                                        source: SourceCursor {
                                            key: delta.key,
                                            cursor: delta.cursor,
                                        },
                                    },
                                )?;
                            }
                            // A batch is the only thing that can clear a parked
                            // decode, so wake them here rather than on a timer.
                            self.retry_parked_decodes()?;
                        }
                    }
                }
            }
            self.drain_outbox().await?;
        }
    }

    fn start_task(&mut self, key: GroupPartKey, task: Task) -> Res<()> {
        let future = task_future(
            self.store.clone(),
            self.keyhive.clone(),
            self.scope.clone(),
            task.clone(),
        );
        self.tasks.replace(key, task, future)?;
        Ok(())
    }

    /// Park a decode whose event named a delegate this hive has not ingested.
    ///
    /// The source stays unsettled, so the admission cursor never advances over
    /// work that could not be applied, and the key is woken by the next
    /// admission row ([`Self::retry_parked_decodes`]) rather than by a timer.
    fn park_decode(&mut self, source: SourceCursor, task: Task) {
        self.tasks
            .park(GroupPartKey::Decode(source.key), task.clone());
        let old = self.parked_decodes.insert(source.key, task);
        assert!(old.is_none(), "a decode key was parked twice");
        tracing::debug!(
            source_key = source.key,
            source_cursor = source.cursor,
            local_peer_id = %self.local_peer_id,
            parked_decodes = self.parked_decodes.len(),
            "group-part decode parked: the event's proof chain is not resolvable yet"
        );
    }

    /// Wake every decode parked on an unknown delegate.
    ///
    /// Only an event this worker has not read yet can clear one, and a batch is
    /// exactly that: the admission log is appended *after* Keyhive incorporated
    /// the event (`SqliteBigRepoStore::append`), so the delegate's own ingest
    /// always brings a later row than the row it stranded.
    fn retry_parked_decodes(&mut self) -> Res<()> {
        if self.parked_decodes.is_empty() {
            return Ok(());
        }
        let parked: Vec<u64> = self.parked_decodes.keys().copied().collect();
        for source_key in parked {
            let task = self
                .parked_decodes
                .remove(&source_key)
                .expect("parked decode disappeared while retrying");
            let future = task_future(
                self.store.clone(),
                self.keyhive.clone(),
                self.scope.clone(),
                task,
            );
            tracing::debug!(
                source_key,
                local_peer_id = %self.local_peer_id,
                parked_decodes = self.parked_decodes.len(),
                "waking a parked group-part decode"
            );
            if !self.tasks.wake(GroupPartKey::Decode(source_key), future)? {
                // A duplicate delivery of the same row replaced the parked key
                // with live work: that task's own completion owns the next
                // attempt, so dropping the local entry is enough.
                tracing::debug!(
                    source_key,
                    local_peer_id = %self.local_peer_id,
                    parked_decodes = self.parked_decodes.len(),
                    "parked group-part decode superseded by live work"
                );
            }
        }
        Ok(())
    }

    async fn on_task_completion(
        &mut self,
        completion: big_sync_core::tokio_keyed_scheduler::TokioTaskCompletion<Task, TaskOutput>,
    ) -> Res<()> {
        match (completion.command, completion.result) {
            (Task::Decode { source, .. }, Ok(TaskOutput::Decoded(affected))) => {
                self.on_decoded(source, affected).await?;
            }
            (task @ Task::Decode { source, .. }, Ok(TaskOutput::Unresolved)) => {
                self.park_decode(source, task);
            }
            (Task::ReconcileDocument { doc, sources, .. }, Ok(TaskOutput::Reconciled)) => {
                self.finish_document_task(doc, sources).await?;
            }
            (Task::EnsurePart { part, sources }, Ok(TaskOutput::Ensured)) => {
                self.finish_group_part_task(part, sources).await?;
            }
            (Task::Decode { .. }, Ok(TaskOutput::Reconciled | TaskOutput::Ensured))
            | (Task::ReconcileDocument { .. }, Ok(TaskOutput::Decoded(_)))
            | (Task::EnsurePart { .. }, Ok(TaskOutput::Decoded(_)))
            | (Task::ReconcileDocument { .. }, Ok(TaskOutput::Unresolved))
            | (Task::EnsurePart { .. }, Ok(TaskOutput::Unresolved))
            | (Task::ReconcileDocument { .. }, Ok(TaskOutput::Ensured))
            | (Task::EnsurePart { .. }, Ok(TaskOutput::Reconciled)) => {
                unreachable!("group-part task produced an incompatible output")
            }
            (_, Err(error)) => panic!("group-part task failed: {error:?}"),
        }
        self.pump_tasks()?;
        Ok(())
    }

    /// A derived task completed. Its snapshot sources settle; sources that
    /// arrived while the task ran stay pending and the entry is kept
    /// unscheduled so `pump_tasks` schedules the follow-up. Dropping the
    /// entry wholesale would strand those arrivals: their walker keys would
    /// never settle and the contiguous admission cursor would freeze.
    async fn finish_document_task(&mut self, doc: ObjKey, snapshot: Vec<SourceCursor>) -> Res<()> {
        let pending = self
            .pending_documents
            .get_mut(&doc)
            .expect("completed document task had no pending entry");
        pending.sources.retain(|source| !snapshot.contains(source));
        if pending.sources.is_empty() {
            self.pending_documents.remove(&doc);
        } else {
            pending.scheduled = false;
        }
        self.settle_sources(snapshot).await
    }

    async fn finish_group_part_task(
        &mut self,
        part: PartKey,
        snapshot: Vec<SourceCursor>,
    ) -> Res<()> {
        let pending = self
            .pending_group_parts
            .get_mut(&part)
            .expect("completed group task had no pending entry");
        pending.sources.retain(|source| !snapshot.contains(source));
        if pending.sources.is_empty() {
            self.pending_group_parts.remove(&part);
        } else {
            pending.scheduled = false;
        }
        self.settle_sources(snapshot).await
    }

    async fn on_decoded(&mut self, source: SourceCursor, affected: AffectedEvent) -> Res<()> {
        let docs: HashSet<_> = affected.docs.into_iter().collect();
        tracing::debug!(
            source_key = source.key,
            source_cursor = source.cursor,
            docs = docs.len(),
            group_parts = affected.group_parts.len(),
            pending_sources = self.pending_sources.len(),
            "group-part admission decoded"
        );
        if docs.is_empty() && affected.group_parts.is_empty() {
            return self.acknowledge_source(source).await;
        }

        for doc in docs.iter().cloned() {
            let pending = self.pending_documents.entry(doc).or_default();
            if !pending.sources.iter().any(|old| old.key == source.key) {
                // Only a genuinely new source invalidates the running task;
                // a duplicate-key decode must not cancel healthy work.
                pending.scheduled = false;
                pending.sources.push(source);
                self.pending_sources
                    .entry(source.key)
                    .and_modify(|entry| entry.remaining += 1)
                    .or_insert(PendingSource {
                        source,
                        remaining: 1,
                    });
            }
            pending
                .affected_group_parts
                .extend(affected.group_parts.iter().cloned());
        }
        if docs.is_empty() {
            for part in affected.group_parts {
                let pending = self.pending_group_parts.entry(part).or_default();
                if !pending.sources.iter().any(|old| old.key == source.key) {
                    pending.scheduled = false;
                    pending.sources.push(source);
                    self.pending_sources
                        .entry(source.key)
                        .and_modify(|entry| entry.remaining += 1)
                        .or_insert(PendingSource {
                            source,
                            remaining: 1,
                        });
                }
            }
        }
        self.pump_tasks()
    }

    fn pump_tasks(&mut self) -> Res<()> {
        let documents: Vec<_> = self
            .pending_documents
            .iter()
            .filter_map(|(doc, pending)| (!pending.scheduled).then_some(doc.clone()))
            .collect();
        for doc in documents {
            if self.tasks.active_count() == CONCURRENT_TASK_BUDGET {
                break;
            }
            self.schedule_document(doc)?;
        }
        let parts: Vec<_> = self
            .pending_group_parts
            .iter()
            .filter_map(|(part, pending)| (!pending.scheduled).then_some(part.clone()))
            .collect();
        for part in parts {
            if self.tasks.active_count() == CONCURRENT_TASK_BUDGET {
                break;
            }
            self.schedule_group_part(part)?;
        }
        Ok(())
    }

    fn schedule_document(&mut self, doc: ObjKey) -> Res<()> {
        let key = GroupPartKey::Document(doc.clone());
        let Some(pending) = self.pending_documents.get(&doc) else {
            return Ok(());
        };
        if pending.scheduled || !self.tasks.has_capacity_for(key.clone()) {
            return Ok(());
        }
        let task = Task::ReconcileDocument {
            doc: doc.clone(),
            affected_group_parts: pending.affected_group_parts.clone(),
            sources: pending.sources.clone(),
        };
        self.start_task(key, task)?;
        self.pending_documents
            .get_mut(&doc)
            .expect("document pending state disappeared while scheduling")
            .scheduled = true;
        Ok(())
    }

    fn schedule_group_part(&mut self, part: PartKey) -> Res<()> {
        let key = GroupPartKey::Group(part.clone());
        let Some(pending) = self.pending_group_parts.get(&part) else {
            return Ok(());
        };
        if pending.scheduled || !self.tasks.has_capacity_for(key.clone()) {
            return Ok(());
        }
        self.start_task(
            key,
            Task::EnsurePart {
                part: part.clone(),
                sources: pending.sources.clone(),
            },
        )?;
        self.pending_group_parts
            .get_mut(&part)
            .expect("group pending state disappeared while scheduling")
            .scheduled = true;
        Ok(())
    }

    async fn settle_sources(&mut self, sources: Vec<SourceCursor>) -> Res<()> {
        tracing::debug!(
            source_count = sources.len(),
            pending_sources = self.pending_sources.len(),
            "group-part derived task settling sources"
        );
        let mut settled = Vec::new();
        for source in sources {
            let pending = self
                .pending_sources
                .get_mut(&source.key)
                .expect("derived task must retain its admission source");
            pending.remaining -= 1;
            if pending.remaining == 0 {
                settled.push(pending.source);
                self.pending_sources.remove(&source.key);
            }
        }
        for source in settled {
            self.acknowledge_source(source).await?;
        }
        Ok(())
    }

    async fn acknowledge_source(&mut self, source: SourceCursor) -> Res<()> {
        let ack = self.admission.ack(source.key, source.cursor).await?;
        tracing::debug!(
            source_key = source.key,
            source_cursor = source.cursor,
            ?ack,
            durable_revision = self.admission.durable_revision(),
            pending_sources = self.pending_sources.len(),
            "group-part admission source acknowledged"
        );
        if let DeltaAck::Accepted {
            through: Some(through),
        } = ack
        {
            self.outbox.push(Cmd::AnnounceSettled(through), ());
        }
        Ok(())
    }

    async fn drain_outbox(&mut self) -> Res<()> {
        while let Some((pending, cmd)) = self.outbox.front() {
            match cmd {
                Cmd::AnnounceSettled(seq) => {
                    tracing::debug!(
                        seq = *seq,
                        durable_revision = self.admission.durable_revision(),
                        pending_sources = self.pending_sources.len(),
                        pending_documents = self.pending_documents.len(),
                        pending_group_parts = self.pending_group_parts.len(),
                        "group-part worker announcing settled admission"
                    );
                    self.evt_tx
                        .send(crate::runtime2::Runtime2Evt::GroupPartWorkerSettled { seq: *seq })
                        .await
                        .map_err(|_| ferr!("GroupPartWorker event channel closed"))?;
                }
            }
            let (_cmd, _unit) = self.outbox.complete(pending.id());
        }
        Ok(())
    }
}

/// Build the future for one keyed task.
///
/// The parameters are owned so the returned future borrows nothing from the
/// worker: a parked decode is woken while the worker is mutably borrowed.
fn task_future(
    store: SqliteBigRepoStore,
    keyhive: BigKeyhiveHandle,
    scope: WorkerGroupScope,
    task: Task,
) -> impl Future<Output = Res<TaskOutput>> + Send + 'static {
    run_task(task, store, keyhive, scope)
}

async fn run_task(
    task: Task,
    store: SqliteBigRepoStore,
    keyhive: BigKeyhiveHandle,
    scope: WorkerGroupScope,
) -> Res<TaskOutput> {
    match task {
        Task::Decode { bytes, .. } => match affected_event(&keyhive, &bytes, &scope).await? {
            EventDecode::Affected(affected) => Ok(TaskOutput::Decoded(affected)),
            EventDecode::Unresolved => Ok(TaskOutput::Unresolved),
        },
        Task::ReconcileDocument {
            doc,
            affected_group_parts,
            ..
        } => {
            for part in &affected_group_parts {
                store.ensure_part(part.clone()).await?;
            }
            let reconciliation = reconcile_doc(
                &keyhive,
                doc,
                &affected_group_parts,
                &scope,
                &GroupAgentsMemo::default(),
            )
            .await?;
            store.reconcile_group_part_batch(&[reconciliation]).await?;
            Ok(TaskOutput::Reconciled)
        }
        Task::EnsurePart { part, .. } => {
            store.ensure_part(part).await?;
            Ok(TaskOutput::Ensured)
        }
    }
}

async fn drive_buffered<T: Send, F: Future<Output = Res<T>> + Send>(
    futs: impl IntoIterator<Item = F>,
    limit: usize,
) -> Res<Vec<T>> {
    use futures::TryStreamExt;
    use futures_buffered::BufferedStreamExt;
    futures::stream::iter(futs)
        .buffered_unordered(limit)
        .try_collect()
        .await
}

async fn reconcile_doc(
    keyhive: &BigKeyhiveHandle,
    doc: ObjKey,
    affected_group_parts: &HashSet<PartKey>,
    scope: &WorkerGroupScope,
    group_agents: &GroupAgentsMemo,
) -> Res<GroupPartReconciliation> {
    let verifying_key = ed25519_dalek::VerifyingKey::from_bytes(&doc.to_bytes32()?)
        .map_err(|_| ferr!("document id is not a valid Ed25519 point"))?;
    let has_content = keyhive
        .keyhive_document_exists(crate::DocumentId::new(doc.as_bytes()))
        .await?;
    let (agents, part_agents, candidate_group_parts) = if has_content {
        let identifier = keyhive_core::principal::identifier::Identifier::from(verifying_key);
        let agents = keyhive
            .agents_for_membered(identifier)
            .await
            .into_iter()
            .map(|(principal, access)| (PeerKey::new(principal.to_bytes()), access))
            .collect::<HashMap<_, _>>();
        // Access is granted per part, and a part is a one-way digest of a group id, so
        // the group id has to be retained here to ask Keyhive which principals may read
        // that part. Writing this doc-level union onto every containing part instead
        // would hand a principal of one group pull access to every other group holding
        // the doc.
        let mut part_agents = HashMap::new();
        let mut candidate_group_parts = HashSet::new();
        for group_id in keyhive
            .group_ids_containing_document(crate::DocumentId::new(doc.as_bytes()))
            .await?
        {
            let part = group_part_id(group_id);
            if let Some(groups) = scope.groups()
                && !groups.contains(&part)
            {
                continue;
            }
            candidate_group_parts.insert(part.clone());
            part_agents.insert(part, group_agents.agents_for(keyhive, group_id).await?);
        }
        (agents, part_agents, candidate_group_parts)
    } else {
        (HashMap::new(), HashMap::new(), HashSet::new())
    };
    let desired_group_parts = candidate_group_parts;
    // This worker owns keyhive-derived membership only: one agent set per containing group, and
    // one part per group. `/seds` is not one of them — the store populates it when it saves a
    // sedimentree, which is the moment the tree starts existing locally.
    let mut reconciled_group_parts = affected_group_parts.clone();
    reconciled_group_parts.extend(desired_group_parts.iter().cloned());
    Ok(GroupPartReconciliation {
        doc,
        agents,
        part_agents,
        managed_group_parts: reconciled_group_parts,
        desired_group_parts,
    })
}

/// Lazily-filled, shared cache of Keyhive agents per group id.
///
/// Per-part agent sets are derived per *containing group*, and docs are reconciled
/// concurrently through `buffered_unordered`: without sharing this would cost
/// O(containing groups) Keyhive calls per doc, where sharing makes it one call per
/// distinct group for the whole batch.
/// Agents of one group, shared across every document that memo hits it.
type GroupAgents = Arc<HashMap<PeerKey, keyhive_core::access::Access>>;

#[derive(Default)]
struct GroupAgentsMemo {
    agents: tokio::sync::Mutex<HashMap<[u8; 32], GroupAgents>>,
}

impl GroupAgentsMemo {
    async fn agents_for(
        &self,
        keyhive: &BigKeyhiveHandle,
        group_id: [u8; 32],
    ) -> Res<Arc<HashMap<PeerKey, keyhive_core::access::Access>>> {
        let mut cached = self.agents.lock().await;
        if let Some(agents) = cached.get(&group_id) {
            return Ok(Arc::clone(agents));
        }
        // The lock is held across the query so each group resolves exactly once even
        // when docs race. `agents_for_membered` does not re-enter this memo.
        let agents = Arc::new(
            keyhive
                .agents_for_membered(group_identifier(group_id)?)
                .await
                .into_iter()
                .map(|(principal, access)| (PeerKey::new(principal.to_bytes()), access))
                .collect::<HashMap<_, _>>(),
        );
        cached.insert(group_id, Arc::clone(&agents));
        Ok(agents)
    }
}

fn group_identifier(group_id: [u8; 32]) -> Res<keyhive_core::principal::identifier::Identifier> {
    ed25519_dalek::VerifyingKey::from_bytes(&group_id)
        .map(keyhive_core::principal::identifier::Identifier::from)
        .map_err(|_| ferr!("group id is not a valid Ed25519 point"))
}

pub(crate) fn group_part_id(group_id: [u8; 32]) -> PartKey {
    let mut bytes = b"townframe/big-repo/group-part/sedimentree/v1".to_vec();
    bytes.extend_from_slice(&group_id);
    let raw = keyhive_crypto::digest::Digest::<Vec<u8>>::hash(&bytes).raw;
    PartKey::new(*raw.as_bytes())
}

#[derive(Debug, Clone)]
struct AffectedEvent {
    docs: Vec<ObjKey>,
    group_parts: HashSet<PartKey>,
}

/// One admitted event, decoded as far as this hive can currently take it.
#[derive(Debug, Clone)]
enum EventDecode {
    Affected(AffectedEvent),
    /// The event's proof chain is not resolvable here yet. The caller parks the
    /// decode instead of failing it: Keyhive calls this a missing dependency, and
    /// the missing link's own admission row is what clears it.
    Unresolved,
}

async fn affected_event(
    keyhive: &BigKeyhiveHandle,
    bytes: &[u8],
    scope: &WorkerGroupScope,
) -> Res<EventDecode> {
    let event: StaticEvent<Vec<u8>> = bincode::deserialize(bytes)
        .map_err(|err| ferr!("persisted Keyhive event decode failed: {err}"))?;
    let mut documents = Vec::new();
    let mut group_parts = HashSet::new();
    match event {
        StaticEvent::CgkaOperation(operation) => {
            documents.push(crate::DocumentId::new(
                operation.payload().doc_id().as_bytes(),
            ));
        }
        StaticEvent::Delegated(delegation) => {
            documents.extend(
                delegation
                    .payload()
                    .after_content
                    .keys()
                    .map(|id| ObjKey::new(id.as_bytes())),
            );
            // A part and a containing-group query both name a *group*, and the group
            // this operation was dispatched to is the proof chain's root, not the
            // immediate signer — the two differ whenever a non-root member
            // re-delegates. The wire form carries proof *digests*, so the subject is
            // resolved through this hive's graph rather than off the payload.
            let subject = match keyhive
                .event_subject_id(StaticEvent::Delegated(delegation))
                .await?
            {
                EventSubject::Named(subject) => subject,
                EventSubject::Unnamed => {
                    unreachable!("a delegated event names a membered subject")
                }
                EventSubject::Unresolved => return Ok(EventDecode::Unresolved),
            };
            group_parts.insert(group_part_id(subject.to_bytes()));
            documents.extend(
                keyhive
                    .document_ids_containing_group(subject)
                    .await
                    .into_iter()
                    .map(|id| ObjKey::new(id.as_bytes())),
            );
        }
        StaticEvent::Revoked(revocation) => {
            documents.extend(
                revocation
                    .payload()
                    .after_content
                    .keys()
                    .map(|id| ObjKey::new(id.as_bytes())),
            );
            // Same as the delegation arm: the group is the proof chain's root, not
            // the immediate signer.
            let subject = match keyhive
                .event_subject_id(StaticEvent::Revoked(revocation))
                .await?
            {
                EventSubject::Named(subject) => subject,
                EventSubject::Unnamed => unreachable!("a revoked event names a membered subject"),
                EventSubject::Unresolved => return Ok(EventDecode::Unresolved),
            };
            group_parts.insert(group_part_id(subject.to_bytes()));
            documents.extend(
                keyhive
                    .document_ids_containing_group(subject)
                    .await
                    .into_iter()
                    .map(|id| ObjKey::new(id.as_bytes())),
            );
        }
        StaticEvent::PrekeysExpanded(_) | StaticEvent::PrekeyRotated(_) => {}
    }
    let mut docs = Vec::new();
    for doc in documents {
        let admitted = match scope.groups() {
            None => true,
            Some(_) => scope.admits_doc_groups(
                &keyhive
                    .group_ids_containing_document(crate::DocumentId::new(doc.as_bytes()))
                    .await?,
            ),
        };
        if admitted {
            docs.push(doc);
        }
    }
    Ok(EventDecode::Affected(AffectedEvent {
        docs,
        group_parts: group_parts
            .into_iter()
            .filter(|part| match scope {
                WorkerGroupScope::All => true,
                WorkerGroupScope::Groups(groups) => groups.contains(part),
            })
            .collect(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keyhive::BigKeyhiveAgent;
    use crate::keyhive_listener::BigRepoKeyhiveListener;
    use crate::keyhive_storage::BigRepoKeyhiveStorage;
    use big_sync_core::BuckId;
    use big_sync_core::revisioned_store::{
        RevisionRead, RevisionReadLimits, RevisionedStoreReader,
    };
    use keyhive_core::access::Access;
    use keyhive_core::event::Event;
    use keyhive_core::principal::group::id::GroupId as KhGroupId;
    use keyhive_core::principal::identifier::Identifier;
    use keyhive_core::principal::membered::Membered;
    use sqlx_utils_rs::SqlCtx;
    use std::collections::BTreeMap;
    use subduction_keyhive::storage::StorageHash;

    #[test]
    fn group_part_id_uses_sedimentree_namespace() {
        // Multibase base58btc of the derived key's bytes: the key space is a byte prefix, not
        // text, so decision 1's readable-form display falls back to multibase here.
        assert_eq!(
            group_part_id([0; 32]).to_string(),
            "zB1TtXt35pLe8AyPkUKgPLgbpFHckKjK3CHCQEytRFaLj"
        );
    }

    /// A re-delegation whose signer is a member of the subject group rather than
    /// the subject itself: the immediate signer and the graph the operation was
    /// dispatched to are two different identifiers, so only naming the subject
    /// derives the group's part. Same fixture shape as the delta-stream reading of
    /// [`crate::keyhive::BigKeyhiveHandle::event_subject_id`].
    #[tokio::test]
    async fn delegation_group_part_names_the_proof_chain_root_not_the_signer() {
        let (evt_tx, _evt_rx) = async_channel::unbounded();
        let keyhive = BigKeyhiveHandle::new(
            [23; 32],
            BigRepoKeyhiveListener {
                evt_tx,
                storage: BigRepoKeyhiveStorage::memory(),
            },
        )
        .await
        .expect("boot keyhive handle");

        let hive = keyhive.clone_keyhive();
        let group_id = hive.generate_group(vec![]).await.expect("generate group");
        let subject: Identifier = group_id.into();
        let group = hive
            .get_group(group_id)
            .await
            .expect("generated group must be present in Keyhive");
        let member: BigKeyhiveAgent = hive
            .get_agent(subject)
            .await
            .expect("a group is its own agent");
        let update = hive
            .add_member_with_manual_content(
                member,
                &Membered::Group(KhGroupId::from(subject), group),
                Access::Read,
                BTreeMap::new(),
            )
            .await
            .expect("add member");
        let event: StaticEvent<Vec<u8>> = Event::Delegated(update.delegation).into();
        let StaticEvent::Delegated(delegation) = &event else {
            unreachable!("the fixture builds a delegation")
        };
        let signer: Identifier = delegation.issuer.into();
        assert_ne!(
            signer, subject,
            "the fixture has to re-delegate from a non-root member"
        );
        let bytes = bincode::serialize(&event).expect("serialize delegation event");

        let affected = match affected_event(&keyhive, &bytes, &WorkerGroupScope::All)
            .await
            .expect("decode the admission")
        {
            EventDecode::Affected(affected) => affected,
            EventDecode::Unresolved => {
                panic!("the fixture's delegation chain is unresolvable")
            }
        };
        assert_eq!(
            affected.group_parts,
            HashSet::from([group_part_id(subject.to_bytes())]),
            "the part belongs to the proof chain's root, not to the signer"
        );
        assert!(
            affected.docs.is_empty(),
            "the group holds no documents, so no document is affected"
        );
    }

    /// The machine loop's wait is the injected timer, and this drives the
    /// admission read that loop is built on to say so: the read parks until the
    /// timer's `IDLE_POLL` sleep completes, so a row admitted while it is parked
    /// becomes visible exactly when time moves. If the loop went back to
    /// `tokio::time::sleep`, `wait_until_armed` would never resolve and the hard
    /// test timeout would be the only signal.
    #[tokio::test]
    async fn the_admission_read_waits_on_the_injected_timer_not_wall_clock() {
        let sql = SqlCtx::memory().await.expect("create sqlite database");
        let store = SqliteBigRepoStore::new(sql, "group-part-manual-time", BuckId::MAX_LEVEL)
            .await
            .expect("create sqlite big repo store");
        let manual = crate::runtime2::tasks::manual_time::ManualTime::new();
        // Binding the concrete handle first is required: `Arc::clone(&manual)` does not
        // coerce, because the expected `Arc<dyn Timer<_>>` propagates into the call's
        // argument and demands `&Arc<dyn Timer<_>>`. An unsized coercion applies at a
        // binding, so the cast belongs on this second line.
        let manual_timer: Arc<crate::runtime2::tasks::manual_time::ManualTime> =
            Arc::clone(&manual);
        let timer: Arc<dyn crate::runtime2::Timer<Sendable>> = manual_timer;
        let source = keyhive_admission::Store {
            store: store.clone(),
            timer,
        };
        let mut reader = source.open((), 0).await.expect("open admission reader");
        let limits = RevisionReadLimits::default();
        assert!(
            matches!(
                reader.next(limits).await.expect("read replay boundary"),
                RevisionRead::ReplayComplete { .. }
            ),
            "an empty log replays nothing"
        );

        // The next read finds nothing and parks in `timer.sleep(IDLE_POLL)`.
        let parked = tokio::spawn(async move { reader.next(limits).await });
        manual.wait_until_armed(1).await;

        store
            .save_keyhive_event(StorageHash::new([41; 32]), vec![41u8], None)
            .await
            .expect("save keyhive event");
        store
            .append_admitted_events(vec![StorageHash::new([41; 32])], None)
            .await
            .expect("append admitted event");
        manual.advance(keyhive_admission::IDLE_POLL);

        let read = parked
            .await
            .expect("reader task must not panic")
            .expect("read the admitted row");
        assert!(
            matches!(read, RevisionRead::Entries { revision: 1, .. }),
            "the row admitted during the wait is delivered once the timer's sleep completes: {read:?}"
        );
    }
}
