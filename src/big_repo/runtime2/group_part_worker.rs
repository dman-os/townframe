//! Crash-recoverable maintenance for Keyhive-derived policy and partitions.
//!
//! Admission rows are decoded into affected documents and group parts. The
//! worker owns the dependency graph: every derived task retains the source
//! admission row that caused it, and the source is acknowledged only after all
//! derived work has completed successfully.

use crate::interlude::*;
use crate::keyhive::BigKeyhiveHandle;
use crate::runtime2::{WorkerGroupScope, keyhive_admission};
use crate::store::sqlite::{GroupPartReconciliation, SqliteBigRepoStore};
use big_sync::delta_walker_state::SqliteDeltaWalkerStateRepo;
use big_sync_core::concurrent_delta_walker::{
    ConcurrentDeltaRead, ConcurrentDeltaWalker, DeltaAck,
};
use big_sync_core::delta_walker_state::{DeltaWalkerStateRepo, DeltaWalkerStateTransaction};
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
    local_peer_id: PeerId,
    timer: Arc<dyn crate::runtime2::Timer<Sendable>>,
    evt_tx: async_channel::Sender<crate::runtime2::Runtime2Evt>,
    scope: WorkerGroupScope,
) -> SpawnedGroupPartWorker<Sendable> {
    let (abort_handle, abort_registration) = futures::future::AbortHandle::new_pair();
    let run = Sendable::from_future(async move {
        let fut = async move {
            let cursor = store.keyhive_group_part_cursor().await?;
            store
                .register_keyhive_admission_reader(
                    crate::store::sqlite::KEYHIVE_ADMISSION_READER_GROUP_PART,
                    cursor,
                )
                .await?;

            if cursor == 0 {
                let initial_group_parts: HashSet<PartId> = match scope.groups() {
                    None => store.list_parts().await?,
                    Some(groups) => groups.iter().copied().collect(),
                };
                for part_id in &initial_group_parts {
                    store.ensure_part(*part_id).await?;
                }
                let initial_group_parts = Arc::new(initial_group_parts);
                let docs = keyhive.document_ids().await;
                let futs = docs.into_iter().map(|doc| {
                    let store = store.clone();
                    let keyhive = keyhive.clone();
                    let initial_group_parts = Arc::clone(&initial_group_parts);
                    let scope = scope.clone();
                    async move {
                        let doc_id = crate::DocumentId::new(doc.into_bytes());
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
                            local_peer_id,
                        )
                        .await?;
                        store
                            .reconcile_group_part_batch(&[reconciliation], 0, false)
                            .await
                    }
                });
                drive_buffered(futs, INITIAL_BUILD_CONCURRENCY).await?;
                store.reconcile_group_part_batch(&[], 0, true).await?;
            }

            let state = {
                let state = SqliteDeltaWalkerStateRepo::new(
                    store.sql.read_pool.clone(),
                    store.sql.write_pool.clone(),
                    "big_repo.group_part",
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
                pending_sources: HashMap::new(),
                pending_documents: HashMap::new(),
                pending_group_parts: HashMap::new(),
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
    AdvanceCursor(u64),
    AnnounceSettled(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum GroupPartKey {
    Decode(u64),
    Document(ObjId),
    Group(PartId),
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
        doc: ObjId,
        affected_group_parts: HashSet<PartId>,
        sources: Vec<SourceCursor>,
    },
    EnsurePart {
        part: PartId,
        sources: Vec<SourceCursor>,
    },
}

#[derive(Debug)]
enum TaskOutput {
    Decoded(AffectedEvent),
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
    affected_group_parts: HashSet<PartId>,
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
    local_peer_id: PeerId,
    evt_tx: async_channel::Sender<crate::runtime2::Runtime2Evt>,
    scope: WorkerGroupScope,
    admission: ConcurrentDeltaWalker<'a, keyhive_admission::Store, SqliteDeltaWalkerStateRepo, u64>,
    tasks:
        big_sync_core::tokio_keyed_scheduler::TokioKeyedScheduler<GroupPartKey, Task, TaskOutput>,
    pending_sources: HashMap<u64, PendingSource>,
    pending_documents: HashMap<ObjId, PendingDocument>,
    pending_group_parts: HashMap<PartId, PendingGroupPart>,
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
                        self.admission
                            .next(std::num::NonZeroUsize::new(available).expect("available is non-zero"))
                            .await
                    }
                } => {
                    match admission? {
                        ConcurrentDeltaRead::ReplayComplete { .. } => {}
                        ConcurrentDeltaRead::Entries { entries, .. } => {
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
                        }
                    }
                }
            }
            self.drain_outbox().await?;
        }
    }

    fn start_task(&mut self, key: GroupPartKey, task: Task) -> Res<()> {
        let future = run_task(
            task.clone(),
            self.store.clone(),
            self.keyhive.clone(),
            self.local_peer_id,
            self.scope.clone(),
        );
        self.tasks.replace(key, task, future)?;
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
            (Task::ReconcileDocument { doc, sources, .. }, Ok(TaskOutput::Reconciled)) => {
                self.finish_document_task(doc, sources).await?;
            }
            (Task::EnsurePart { part, sources }, Ok(TaskOutput::Ensured)) => {
                self.finish_group_part_task(part, sources).await?;
            }
            (Task::Decode { .. }, Ok(TaskOutput::Reconciled | TaskOutput::Ensured))
            | (Task::ReconcileDocument { .. }, Ok(TaskOutput::Decoded(_)))
            | (Task::EnsurePart { .. }, Ok(TaskOutput::Decoded(_)))
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
    async fn finish_document_task(&mut self, doc: ObjId, snapshot: Vec<SourceCursor>) -> Res<()> {
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
        part: PartId,
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
        if docs.is_empty() && affected.group_parts.is_empty() {
            return self.acknowledge_source(source).await;
        }

        for doc in docs.iter().copied() {
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
                .extend(affected.group_parts.iter().copied());
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
            .filter_map(|(doc, pending)| (!pending.scheduled).then_some(*doc))
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
            .filter_map(|(part, pending)| (!pending.scheduled).then_some(*part))
            .collect();
        for part in parts {
            if self.tasks.active_count() == CONCURRENT_TASK_BUDGET {
                break;
            }
            self.schedule_group_part(part)?;
        }
        Ok(())
    }

    fn schedule_document(&mut self, doc: ObjId) -> Res<()> {
        let key = GroupPartKey::Document(doc);
        let Some(pending) = self.pending_documents.get(&doc) else {
            return Ok(());
        };
        if pending.scheduled || !self.tasks.has_capacity_for(key) {
            return Ok(());
        }
        let task = Task::ReconcileDocument {
            doc,
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

    fn schedule_group_part(&mut self, part: PartId) -> Res<()> {
        let key = GroupPartKey::Group(part);
        let Some(pending) = self.pending_group_parts.get(&part) else {
            return Ok(());
        };
        if pending.scheduled || !self.tasks.has_capacity_for(key) {
            return Ok(());
        }
        self.start_task(
            key,
            Task::EnsurePart {
                part,
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
        if let DeltaAck::Accepted {
            through: Some(through),
        } = self.admission.ack(source.key, source.cursor).await?
        {
            self.outbox.push(Cmd::AdvanceCursor(through), ());
            self.outbox.push(Cmd::AnnounceSettled(through), ());
        }
        Ok(())
    }

    async fn drain_outbox(&mut self) -> Res<()> {
        while let Some((pending, cmd)) = self.outbox.front() {
            match cmd {
                Cmd::AdvanceCursor(cursor) => {
                    self.store
                        .reconcile_group_part_batch(&[], *cursor, true)
                        .await?;
                }
                Cmd::AnnounceSettled(seq) => {
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

async fn run_task(
    task: Task,
    store: SqliteBigRepoStore,
    keyhive: BigKeyhiveHandle,
    local_peer_id: PeerId,
    scope: WorkerGroupScope,
) -> Res<TaskOutput> {
    match task {
        Task::Decode { bytes, .. } => Ok(TaskOutput::Decoded(
            affected_event(&keyhive, &bytes, &scope).await?,
        )),
        Task::ReconcileDocument {
            doc,
            affected_group_parts,
            ..
        } => {
            for part in &affected_group_parts {
                store.ensure_part(*part).await?;
            }
            let reconciliation =
                reconcile_doc(&keyhive, doc, &affected_group_parts, &scope, local_peer_id).await?;
            store
                .reconcile_group_part_batch(&[reconciliation], 0, false)
                .await?;
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
    doc: ObjId,
    affected_group_parts: &HashSet<PartId>,
    scope: &WorkerGroupScope,
    local_principal: PeerId,
) -> Res<GroupPartReconciliation> {
    let verifying_key = ed25519_dalek::VerifyingKey::from_bytes(&doc.into_bytes())
        .map_err(|_| ferr!("document id is not a valid Ed25519 point"))?;
    let has_content = keyhive
        .document_has_content(crate::DocumentId::new(doc.into_bytes()))
        .await?;
    let (agents, candidate_group_parts) = if has_content {
        let identifier = keyhive_core::principal::identifier::Identifier::from(verifying_key);
        let agents = keyhive
            .agents_for_membered(identifier)
            .await
            .into_iter()
            .map(|(principal, access)| (PeerId::new(principal), access))
            .collect::<HashMap<_, _>>();
        let candidate_group_parts = keyhive
            .group_ids_containing_document(crate::DocumentId::new(doc.into_bytes()))
            .await?
            .into_iter()
            .map(group_part_id)
            .filter(|part| match scope.groups() {
                None => true,
                Some(groups) => groups.contains(part),
            })
            .collect();
        (agents, candidate_group_parts)
    } else {
        (HashMap::new(), HashSet::new())
    };
    let desired_group_parts = candidate_group_parts;
    let desired_global = agents
        .get(&local_principal)
        .is_some_and(|access| access.is_reader());
    let mut reconciled_group_parts = affected_group_parts.clone();
    reconciled_group_parts.extend(desired_group_parts.iter().copied());
    Ok(GroupPartReconciliation {
        doc,
        agents,
        managed_group_parts: reconciled_group_parts,
        desired_group_parts,
        desired_global,
    })
}

pub(crate) fn group_part_id(group_id: [u8; 32]) -> PartId {
    let mut bytes = b"townframe/big-repo/group-part/sedimentree/v1".to_vec();
    bytes.extend_from_slice(&group_id);
    let raw = keyhive_crypto::digest::Digest::<Vec<u8>>::hash(&bytes).raw;
    PartId::new(raw.into())
}

#[derive(Debug, Clone)]
struct AffectedEvent {
    docs: Vec<ObjId>,
    group_parts: HashSet<PartId>,
}

async fn affected_event(
    keyhive: &BigKeyhiveHandle,
    bytes: &[u8],
    scope: &WorkerGroupScope,
) -> Res<AffectedEvent> {
    let event: StaticEvent<Vec<u8>> = bincode::deserialize(bytes)
        .map_err(|err| ferr!("persisted Keyhive event decode failed: {err}"))?;
    let mut documents = Vec::new();
    let mut group_parts = HashSet::new();
    match event {
        StaticEvent::CgkaOperation(operation) => {
            documents.push(ObjId::new(*operation.payload().doc_id().as_bytes()));
        }
        StaticEvent::Delegated(delegation) => {
            group_parts.insert(group_part_id(delegation.issuer.to_bytes()));
            documents.extend(
                delegation
                    .payload()
                    .after_content
                    .keys()
                    .map(|id| ObjId::new(id.to_bytes())),
            );
            documents.extend(
                keyhive
                    .document_ids_containing_group(
                        keyhive_core::principal::identifier::Identifier::from(delegation.issuer),
                    )
                    .await
                    .into_iter()
                    .map(|id| ObjId::new(id.into_bytes())),
            );
        }
        StaticEvent::Revoked(revocation) => {
            group_parts.insert(group_part_id(revocation.issuer.to_bytes()));
            documents.extend(
                revocation
                    .payload()
                    .after_content
                    .keys()
                    .map(|id| ObjId::new(id.to_bytes())),
            );
            documents.extend(
                keyhive
                    .document_ids_containing_group(
                        keyhive_core::principal::identifier::Identifier::from(revocation.issuer),
                    )
                    .await
                    .into_iter()
                    .map(|id| ObjId::new(id.into_bytes())),
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
                    .group_ids_containing_document(crate::DocumentId::new(doc.into_bytes()))
                    .await?,
            ),
        };
        if admitted {
            docs.push(doc);
        }
    }
    Ok(AffectedEvent {
        docs,
        group_parts: group_parts
            .into_iter()
            .filter(|part| match scope {
                WorkerGroupScope::All => true,
                WorkerGroupScope::Groups(groups) => groups.contains(part),
            })
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_part_id_uses_sedimentree_namespace() {
        assert_eq!(
            group_part_id([0; 32]).to_string(),
            "B1TtXt35pLe8AyPkUKgPLgbpFHckKjK3CHCQEytRFaLj"
        );
    }
}
