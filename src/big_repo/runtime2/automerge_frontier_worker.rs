//! Background worker that maintains the Automerge Frontier partition log.
//!
//! Two independent inputs feed one publication decision:
//!
//! - **Keyhive admission stream** ([`SqliteBigRepoStore::admission_events_after`]):
//!   rows exist only after their effects are visible in the keyhive graph;
//!   publication awaits the doc bundle's per-document CGKA operation count to
//!   catch up to Keyhive's current count before reading heads. Decode work is keyed
//!   admission sequence and publication is keyed by document, allowing
//!   independent documents to make progress while the durable cursor still
//!   waits for its contiguous prefix.
//! - **Source-part revisions** (a match-all local revision reader): every
//!   part in the scope is read live, including parts created after boot — no
//!   part enumeration is frozen into the walker. Added/Changed events carry a
//!   commit watermark; publishing waits for the doc bundle to reach that
//!   watermark before reading heads.
//!
//! The split keeps I/O and physical task execution in the private [`Worker`].
//! The worker owns one concurrent walker per revision source, the task manager,
//! pending part state, and the serial persistence outbox; spawned work remains a
//! free function so it does not participate in async coordination through `&mut self`.
//! Both streams tail live from the walker's durable revision. There is no gap
//! handling: neither log is pruned. The worker's [`WorkerGroupScope`] is read
//! through a [`GroupScopeHandle`] so the embedder can update it at runtime.
use crate::changes::{BigRepoLocalNotification, LocalFilter};
use crate::interlude::*;
use crate::runtime2::{GroupScopeHandle, WorkerGroupScope, keyhive_admission};
use crate::store::sqlite::SqliteBigRepoStore;
use big_sync::delta_walker_state::SqliteDeltaWalkerStateRepo;
use big_sync::{HostPartStore, LocalPartRevisionReader};
use big_sync_core::concurrent_delta_walker::{
    ConcurrentDeltaRead, ConcurrentDeltaWalker, DeltaAck,
};
use big_sync_core::delta_walker_state::{DeltaWalkerStateRepo, DeltaWalkerStateTransaction};
use big_sync_core::outbox::Outbox;
use big_sync_core::revisioned_store::{
    RevisionRead, RevisionReadLimits, RevisionedStore, RevisionedStoreReader,
};
use big_sync_core::rpc::SubEvent;
use future_form::Sendable;
use keyhive_core::event::static_event::StaticEvent;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublishOutcome {
    Published,
    Deferred,
}

const MATERIALIZATION_WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// The frontier payload object id for a document.
///
/// Frontier payloads live in their own storage scope (derived from the main
/// scope key), so the raw `doc_id` cannot collide with the document's own
/// sedimentree object in the main scope.
pub fn automerge_doc_obj_id(doc_id: crate::DocumentId) -> ObjId {
    ObjId(big_sync_core::Byte32Id::new(*doc_id.as_bytes()))
}

pub fn automerge_obj_to_doc_id(obj_id: ObjId) -> crate::DocumentId {
    crate::DocumentId::new(*obj_id.as_bytes())
}

#[derive(Clone)]
pub struct AutomergeFrontierWorkerStopToken {
    pub(crate) abort: futures::future::AbortHandle,
}

impl AutomergeFrontierWorkerStopToken {
    pub fn cancel(&self) {
        self.abort.abort();
    }
}

pub struct SpawnedAutomergeFrontierWorker<F: FutureForm> {
    pub stop: AutomergeFrontierWorkerStopToken,
    pub run: F::Future<'static, eyre::Result<()>>,
}

#[expect(clippy::too_many_arguments)]
pub fn spawn_automerge_frontier_worker(
    store: SqliteBigRepoStore,
    big_sync_store: Arc<dyn HostPartStore>,
    frontier_store: Arc<dyn HostPartStore>,
    runtime: crate::runtime2::Runtime2Handle<Sendable>,
    // FIXME: hmm, who added this and when and why?
    _evt_tx: async_channel::Sender<crate::runtime2::Runtime2Evt>,
    change_manager: Arc<crate::changes::ChangeListenerManager>,
    keyhive: crate::keyhive::BigKeyhiveHandle,
    scope: GroupScopeHandle,
) -> SpawnedAutomergeFrontierWorker<Sendable> {
    let (abort_handle, abort_registration) = futures::future::AbortHandle::new_pair();

    let fut = {
        async move {
            let timer: Arc<dyn crate::runtime2::Timer<Sendable>> =
                Arc::new(crate::runtime2::TokioTimer);
            // The part source reads match-all (every part in the scope,
            // including parts created after this boot), so no part
            // enumeration is frozen into the walker. The scope handle is
            // consulted live at event-processing and publish time.
            let kh_read_cursor = store.automerge_keyhive_cursor().await?;
            store
                .register_keyhive_admission_reader(
                    crate::store::sqlite::KEYHIVE_ADMISSION_READER_AUTOMERGE_FRONTIER,
                    kh_read_cursor,
                )
                .await?;

            let admission_state = {
                let state = SqliteDeltaWalkerStateRepo::new(
                    store.sql.read_pool.clone(),
                    store.sql.write_pool.clone(),
                    "big_repo.automerge_frontier",
                    "keyhive-admission",
                )
                .await?;
                let progress = state.progress().await?.upstream_revision;
                if progress == 0 && kh_read_cursor > 0 {
                    let mut transaction = state.begin().await?;
                    transaction.advance_from(0, kh_read_cursor).await?;
                    transaction.commit().await?;
                }
                state
            };
            let admission_source = keyhive_admission::Store {
                store: store.clone(),
                timer: Arc::clone(&timer),
            };
            let admission_durable = admission_state.progress().await?.upstream_revision;
            let admission_reader = admission_source.open((), admission_durable).await?;
            let admission = ConcurrentDeltaWalker::open(
                admission_reader,
                admission_state,
                |row: &keyhive_admission::AdmittedRow| {
                    let event: StaticEvent<Vec<u8>> = bincode::deserialize(&row.bytes)
                        .expect("persisted keyhive admission event must decode");
                    match event {
                        StaticEvent::CgkaOperation(operation) => FrontierKey::Document(
                            crate::DocumentId::new(*operation.payload().doc_id().as_bytes()),
                        ),
                        _ => FrontierKey::Decode(row.seq),
                    }
                },
            )
            .await?;

            let part_source = LocalPartRevisionStore {
                store: Arc::clone(&big_sync_store),
            };
            let part_state = SqliteDeltaWalkerStateRepo::new(
                store.sql.read_pool.clone(),
                store.sql.write_pool.clone(),
                "big_repo.automerge_frontier",
                "part-revisions",
            )
            .await?;
            // The walker's source-wide durable revision is the replay lower
            // bound; the reader resolves the live part set on every read.
            let part_durable = part_state.progress().await?.upstream_revision;
            let part_reader = part_source.open((), part_durable).await?;
            let parts = ConcurrentDeltaWalker::open(part_reader, part_state, |event| {
                let doc_id = match event {
                    SubEvent::Added(event) => event.obj_id,
                    SubEvent::Changed(event) => event.obj_id,
                    SubEvent::Removed(event) => event.obj_id,
                    SubEvent::ReplayComplete => {
                        unreachable!("replay completion has no part event key")
                    }
                };
                FrontierKey::Document(automerge_obj_to_doc_id(doc_id))
            })
            .await?;

            let (local_registration, local_listener) = change_manager
                .subscribe_local_listener(LocalFilter { doc_id: None })
                .await?;
            let worker = Worker {
                store,
                big_sync_store,
                frontier_store,
                runtime,
                keyhive,
                scope,
                admission,
                parts,
                _local_registration: local_registration,
                local_listener,
                tasks: big_sync_core::tokio_keyed_scheduler::TokioKeyedScheduler::new(
                    CONCURRENT_TASK_BUDGET,
                ),
                pending_parts: HashMap::new(),
                pending_admission: HashMap::new(),
                pending_part_sources: HashMap::new(),
                outbox: Outbox::default(),
                wake_docs: HashSet::new(),
            };

            match futures::future::Abortable::new(worker.machine_loop(), abort_registration).await {
                Ok(result) => result,
                Err(_) => Ok(()),
            }
        }
    };
    let run = Sendable::from_future(fut);

    SpawnedAutomergeFrontierWorker {
        stop: AutomergeFrontierWorkerStopToken {
            abort: abort_handle,
        },
        run,
    }
}

/// Serial persistence commands. Physical publication is scheduled directly as
/// a keyed task; only durable cursor/membership writes remain here.
#[derive(Debug)]
enum Cmd {
    RemoveFrontierMembership {
        doc_id: crate::DocumentId,
        part_id: PartId,
    },
    AdvanceKhCursor(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum SourceKind {
    Admission,
    Parts,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum FrontierKey {
    Decode(u64),
    Document(crate::DocumentId),
}

/// Publish heads after the live bundle reaches the supplied materialization
/// barrier, then read the heads under the keyring lock. No-op when the doc
/// isn't materialized yet (its admission/part event will re-trigger later).
///
/// Storage-ahead-of-bundle races converge without a separate commit
/// watermark: a publish awaits the bundle's per-document CGKA operation count
/// to catch up to Keyhive's current count (the hub forwards every CGKA op to
/// the doc worker, which re-materializes and updates the bundle state), and any
/// later materialization completion re-triggers a keyed replacement publish
/// that overwrites the frontier with the newer heads.
async fn publish_heads(
    doc_id: crate::DocumentId,
    runtime: &crate::runtime2::Runtime2Handle<Sendable>,
    keyhive: &crate::keyhive::BigKeyhiveHandle,
    big_sync_store: &Arc<dyn HostPartStore>,
    frontier_store: &Arc<dyn HostPartStore>,
    keyhive_watermark: Option<u64>,
    scope: &WorkerGroupScope,
) -> Res<PublishOutcome> {
    let Ok(crate::runtime2::types::DocLookup::Ready(handle)) = runtime.get_doc_handle(doc_id).await
    else {
        return Ok(PublishOutcome::Deferred);
    };
    if keyhive_watermark.is_some() {
        // The admission's CGKA op advances the per-document operation count; the
        // hub forwards it to the doc worker, which re-materializes and updates
        // the bundle. Await the bundle to catch up before advertising heads.
        let target_ops_count = keyhive.current_cgka_ops_count(doc_id).await?;
        match tokio::time::timeout(
            MATERIALIZATION_WAIT_TIMEOUT,
            handle.bundle.await_cgka_ops_count(target_ops_count),
        )
        .await
        {
            Ok(result) => result?,
            Err(_) => {
                tracing::warn!(
                    %doc_id,
                    target_ops_count,
                    "timed out waiting for document materialization before publishing frontier"
                );
                return Ok(PublishOutcome::Deferred);
            }
        }
    }
    let causal_epoch = handle.bundle.current_causal_epoch();
    let heads = surelock::key::lock_scope(|key| {
        let (doc, _key) = key.lock(&handle.bundle.doc);
        doc.get_heads()
    });
    let heads_formatted = am_utils_rs::serialize_commit_heads(&heads);
    let am_obj_id = automerge_doc_obj_id(doc_id);
    let payload = serde_json::json!({
        "heads": heads_formatted,
        "causal_epoch": causal_epoch,
    });
    frontier_store.set_obj_payload(am_obj_id, payload).await?;
    let desired_parts = big_sync_store
        .obj_parts(am_obj_id)
        .await?
        .into_iter()
        .filter(|part_id| scope_includes_part(scope, *part_id))
        .collect::<Vec<_>>();
    let current_parts = frontier_store.obj_parts(am_obj_id).await?;
    if !desired_parts.is_empty() {
        frontier_store
            .add_obj_to_parts(am_obj_id, desired_parts.clone())
            .await?;
    }
    for part_id in current_parts {
        if !desired_parts.contains(&part_id) {
            frontier_store
                .remove_obj_from_part(am_obj_id, part_id)
                .await?;
        }
    }
    Ok(PublishOutcome::Published)
}

/// A scoped worker mirrors only explicitly selected partitions.
fn scope_includes_part(scope: &WorkerGroupScope, part_id: PartId) -> bool {
    scope
        .groups()
        .is_none_or(|groups| groups.contains(&part_id))
}

#[derive(Clone)]
struct LocalPartRevisionStore {
    store: Arc<dyn HostPartStore>,
}

struct LocalPartRevisionReaderAdapter {
    inner: Box<dyn LocalPartRevisionReader>,
}

#[async_trait::async_trait]
impl RevisionedStore for LocalPartRevisionStore {
    type Revision = u64;
    type Entry = SubEvent;
    type Selector = ();
    type Error = eyre::Report;
    type Reader<'a> = LocalPartRevisionReaderAdapter;

    async fn latest_revision(&self) -> Result<u64, eyre::Report> {
        self.store.latest_revision().await
    }

    async fn open<'a>(
        &'a self,
        _selector: (),
        after: u64,
    ) -> Result<Self::Reader<'a>, eyre::Report> {
        // Match-all: the reader resolves the live part set on every read, so
        // parts created after this call are still observed. `after` is the
        // walker's source-wide durable revision.
        let inner = self
            .store
            .open_local_revision_reader_all(after)
            .await
            .wrap_err("opening local part revision reader")??;
        Ok(LocalPartRevisionReaderAdapter { inner })
    }
}

#[async_trait::async_trait]
impl RevisionedStoreReader<u64, SubEvent, eyre::Report> for LocalPartRevisionReaderAdapter {
    async fn next(
        &mut self,
        limits: RevisionReadLimits,
    ) -> Result<RevisionRead<u64, SubEvent>, eyre::Report> {
        self.inner.next(limits).await
    }
}

const CONCURRENT_TASK_BUDGET: usize = 64;
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct SourceCursor {
    source: SourceKind,
    key: FrontierKey,
    cursor: u64,
}

#[derive(Debug, Clone)]
enum FrontierTask {
    Publish {
        doc_id: crate::DocumentId,
        admission: Option<SourceCursor>,
        part_source: Option<SourceCursor>,
        part_cursor: Option<u64>,
    },
}

#[derive(Debug)]
enum ConcurrentTaskOutput {
    Published {
        through: Option<u64>,
    },
    /// The document is outside the worker's current group scope: no
    /// frontier state was written (stale mirror memberships were torn
    /// down), and the sources must settle so the walker cursor advances.
    OutOfScope,
    Deferred,
}

struct Worker<'a> {
    store: SqliteBigRepoStore,
    big_sync_store: Arc<dyn HostPartStore>,
    frontier_store: Arc<dyn HostPartStore>,
    runtime: crate::runtime2::Runtime2Handle<Sendable>,
    keyhive: crate::keyhive::BigKeyhiveHandle,
    scope: GroupScopeHandle,
    admission: ConcurrentDeltaWalker<
        'a,
        keyhive_admission::Store,
        SqliteDeltaWalkerStateRepo,
        FrontierKey,
    >,
    parts:
        ConcurrentDeltaWalker<'a, LocalPartRevisionStore, SqliteDeltaWalkerStateRepo, FrontierKey>,
    _local_registration: crate::changes::LocalListenerRegistration,
    local_listener: tokio::sync::mpsc::UnboundedReceiver<Vec<BigRepoLocalNotification>>,
    tasks: big_sync_core::tokio_keyed_scheduler::TokioKeyedScheduler<
        FrontierKey,
        FrontierTask,
        ConcurrentTaskOutput,
    >,
    pending_admission: HashMap<crate::DocumentId, SourceCursor>,
    pending_part_sources: HashMap<crate::DocumentId, SourceCursor>,
    pending_parts: HashMap<crate::DocumentId, BTreeMap<PartId, u64>>,
    /// The outbox unit carries the source to acknowledge once the command's
    /// durable effect has executed — acknowledgements must not precede the
    /// effect they cover.
    outbox: Outbox<Cmd, Option<SourceCursor>>,
    wake_docs: HashSet<crate::DocumentId>,
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
                            .next(NonZeroUsize::new(available).expect("available is non-zero"))
                            .await
                    }
                } => {
                    match admission? {
                        ConcurrentDeltaRead::ReplayComplete { .. } => {}
                        ConcurrentDeltaRead::Entries { entries, .. } => {
                            for delta in entries {
                                self.on_admission_delta(delta).await?;
                            }
                        }
                    }
                }

                parts = async {
                    if available == 0 {
                        std::future::pending().await
                    } else {
                        self.parts
                            .next(std::num::NonZeroUsize::new(available).expect("available is non-zero"))
                            .await
                    }
                } => {
                    match parts? {
                        ConcurrentDeltaRead::ReplayComplete { .. } => {}
                        ConcurrentDeltaRead::Entries { entries, .. } => {
                            for delta in entries {
                                self.on_part_delta(delta).await?;
                            }
                        }
                    }
                }
                notifications = self.local_listener.recv() => {
                    let notifications = notifications
                        .ok_or_else(|| ferr!("AutomergeFrontierWorker local listener closed"))?;
                    self.on_local_notifications(notifications);
                }

                _scope_changed = async {
                    // A frozen (controller-dropped) scope never changes
                    // again; park instead of spinning.
                    loop {
                        match self.scope.changed().await {
                            Ok(()) => break,
                            Err(_) => std::future::pending::<()>().await,
                        }
                    }
                } => {
                    self.on_scope_changed().await?;
                }
            }

            self.drain_outbox().await?;
            self.start_ready_document_work()?;
        }
    }
    async fn drain_outbox(&mut self) -> Res<()> {
        while let Some((pending, cmd)) = self.outbox.front() {
            match cmd {
                Cmd::RemoveFrontierMembership { doc_id, part_id } => {
                    self.frontier_store
                        .remove_obj_from_part(automerge_doc_obj_id(*doc_id), *part_id)
                        .await?;
                }
                Cmd::AdvanceKhCursor(cursor) => {
                    self.store.commit_automerge_keyhive_cursor(*cursor).await?;
                }
            }
            let (_cmd, unit) = self.outbox.complete(pending.id());
            if let Some(source) = unit {
                // The command's effect is durable; only now may the walker
                // cursor advance past it.
                self.acknowledge_source(source).await?;
            }
        }
        Ok(())
    }
    fn start_task(&mut self, key: FrontierKey, task: FrontierTask) -> Res<()> {
        let future = run_concurrent_frontier_task(
            task.clone(),
            self.runtime.clone(),
            Arc::clone(&self.big_sync_store),
            Arc::clone(&self.frontier_store),
            self.keyhive.clone(),
            self.scope.clone(),
        );
        self.tasks.replace(key, task, future)?;
        Ok(())
    }

    fn start_publish(&mut self, doc_id: crate::DocumentId) -> Res<()> {
        self.start_task(
            FrontierKey::Document(doc_id),
            FrontierTask::Publish {
                doc_id,
                admission: self.pending_admission.get(&doc_id).copied(),
                part_source: self.pending_part_sources.get(&doc_id).copied(),
                part_cursor: self
                    .pending_parts
                    .get(&doc_id)
                    .and_then(|parts| parts.values().copied().max()),
            },
        )
    }

    fn remember_part_source(&mut self, doc_id: crate::DocumentId, source: SourceCursor) {
        self.pending_part_sources
            .entry(doc_id)
            .and_modify(|old| {
                if source.cursor > old.cursor {
                    *old = source;
                }
            })
            .or_insert(source);
    }

    async fn acknowledge_source(&mut self, source: SourceCursor) -> Res<()> {
        let ack = match source.source {
            SourceKind::Admission => self.admission.ack(source.key, source.cursor).await?,
            SourceKind::Parts => self.parts.ack(source.key, source.cursor).await?,
        };
        if source.source == SourceKind::Admission
            && let DeltaAck::Accepted {
                through: Some(through),
            } = ack
        {
            self.outbox.push(Cmd::AdvanceKhCursor(through), None);
        }
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
            source: SourceKind::Admission,
            key: delta.key,
            cursor: delta.cursor,
        };
        match delta.key {
            // The walker's `key_of` already decoded the event: Cgka
            // operations key by document and go straight to publication.
            FrontierKey::Document(doc_id) => {
                self.pending_admission
                    .entry(doc_id)
                    .and_modify(|old| {
                        if source.cursor > old.cursor {
                            *old = source;
                        }
                    })
                    .or_insert(source);
                self.start_publish(doc_id)
            }
            // Admission rows with no document payload only gate the cursor.
            FrontierKey::Decode(_) => self.acknowledge_source(source).await,
        }
    }

    async fn on_part_delta(
        &mut self,
        delta: big_sync_core::concurrent_delta_walker::ConcurrentDelta<FrontierKey, SubEvent>,
    ) -> Res<()> {
        let source = SourceCursor {
            source: SourceKind::Parts,
            key: delta.key,
            cursor: delta.cursor,
        };
        match delta.entry {
            SubEvent::Added(event) => {
                let doc_id = automerge_obj_to_doc_id(event.obj_id);
                self.remember_part_source(doc_id, source);
                self.pending_parts
                    .entry(doc_id)
                    .or_default()
                    .insert(event.part_id, event.cursor);
                self.start_publish(doc_id)?;
            }
            SubEvent::Changed(event) => {
                let doc_id = automerge_obj_to_doc_id(event.obj_id);
                self.remember_part_source(doc_id, source);
                let parts = self.pending_parts.entry(doc_id).or_default();
                for part_id in event.part_ids {
                    parts.insert(part_id, event.cursor);
                }
                self.start_publish(doc_id)?;
            }
            SubEvent::Removed(event) => {
                let doc_id = automerge_obj_to_doc_id(event.obj_id);
                let empty = self.pending_parts.get_mut(&doc_id).is_some_and(|parts| {
                    parts.remove(&event.part_id);
                    parts.is_empty()
                });
                if empty {
                    self.pending_parts.remove(&doc_id);
                    self.pending_part_sources.remove(&doc_id);
                    if !self.pending_admission.contains_key(&doc_id) {
                        self.tasks.cancel(FrontierKey::Document(doc_id));
                    }
                }
                self.outbox.push(
                    Cmd::RemoveFrontierMembership {
                        doc_id,
                        part_id: event.part_id,
                    },
                    Some(source),
                );
            }
            SubEvent::ReplayComplete => unreachable!("part replay marker is not an entry"),
        }
        Ok(())
    }

    fn on_local_notifications(&mut self, notifications: Vec<BigRepoLocalNotification>) {
        for notification in notifications {
            match notification {
                BigRepoLocalNotification::DocMaterializationReady { doc_id, .. }
                | BigRepoLocalNotification::DocHeadsUpdated { doc_id, .. } => {
                    self.wake_docs.insert(doc_id);
                }
                BigRepoLocalNotification::DocCreated { .. }
                | BigRepoLocalNotification::DocImported { .. }
                | BigRepoLocalNotification::DocMaterializationPending { .. } => {}
            }
        }
    }

    async fn on_task_completion(
        &mut self,
        completion: big_sync_core::tokio_keyed_scheduler::TokioTaskCompletion<
            FrontierTask,
            ConcurrentTaskOutput,
        >,
    ) -> Res<()> {
        match (completion.command, completion.result) {
            (
                FrontierTask::Publish {
                    doc_id,
                    admission,
                    part_source,
                    part_cursor: _,
                },
                Ok(ConcurrentTaskOutput::Published { through }),
            ) => {
                if let Some(source) = admission {
                    self.acknowledge_source(source).await?;
                    if self.pending_admission.get(&doc_id).copied() == Some(source) {
                        self.pending_admission.remove(&doc_id);
                    }
                }
                if let Some(source) = part_source {
                    self.acknowledge_source(source).await?;
                    if self.pending_part_sources.get(&doc_id).copied() == Some(source) {
                        self.pending_part_sources.remove(&doc_id);
                    }
                }
                if !self.settle_doc_txid(doc_id, through) {
                    self.wake_docs.insert(doc_id);
                }
            }
            (
                FrontierTask::Publish {
                    doc_id,
                    admission,
                    part_source,
                    part_cursor: _,
                },
                Ok(ConcurrentTaskOutput::OutOfScope),
            ) => {
                if let Some(source) = admission {
                    self.acknowledge_source(source).await?;
                    if self.pending_admission.get(&doc_id).copied() == Some(source) {
                        self.pending_admission.remove(&doc_id);
                    }
                }
                if let Some(source) = part_source {
                    self.acknowledge_source(source).await?;
                    if self.pending_part_sources.get(&doc_id).copied() == Some(source) {
                        self.pending_part_sources.remove(&doc_id);
                    }
                }
                self.pending_parts.remove(&doc_id);
            }
            (task @ FrontierTask::Publish { doc_id, .. }, Ok(ConcurrentTaskOutput::Deferred)) => {
                self.tasks.park(FrontierKey::Document(doc_id), task);
            }
            (_, Err(error)) => panic!("automerge frontier task failed: {error:?}"),
        }
        Ok(())
    }

    /// Settle the doc's pending part events once the publish covered the
    /// newest event cursor seen for it (`through`). Returns `true` when
    /// nothing newer arrived while the publish was running; otherwise the
    /// caller re-wakes the doc so the newer events are not lost.
    fn settle_doc_txid(&mut self, doc_id: crate::DocumentId, through: Option<u64>) -> bool {
        let Some(parts) = self.pending_parts.get(&doc_id) else {
            return true;
        };
        match through {
            Some(through)
                if parts
                    .values()
                    .copied()
                    .max()
                    .is_none_or(|max| max <= through) =>
            {
                self.pending_parts.remove(&doc_id);
                true
            }
            _ => false,
        }
    }
    /// A scope change rescan: every document known to keyhive is
    /// re-evaluated against the new scope. Newly eligible docs get their
    /// frontier state written; docs that left the scope get their stale
    /// mirror memberships torn down (by the publish task's `OutOfScope`
    /// path). Scope changes are rare (relay client churn), so the full
    /// enumeration cost is acceptable.
    async fn on_scope_changed(&mut self) -> Res<()> {
        // `start_ready_document_work` re-queues docs it cannot schedule, so
        // nothing is dropped when the task budget is saturated.
        for doc in self.keyhive.document_ids().await {
            let doc_id = crate::DocumentId::new(doc.into_bytes());
            self.wake_docs.insert(doc_id);
        }
        Ok(())
    }

    fn start_ready_document_work(&mut self) -> Res<()> {
        for doc_id in std::mem::take(&mut self.wake_docs) {
            if !self.tasks.has_capacity_for(FrontierKey::Document(doc_id)) {
                self.wake_docs.insert(doc_id);
                continue;
            }
            self.start_publish(doc_id)?;
        }
        Ok(())
    }
}

async fn run_concurrent_frontier_task(
    task: FrontierTask,
    runtime: crate::runtime2::Runtime2Handle<Sendable>,
    big_sync_store: Arc<dyn HostPartStore>,
    frontier_store: Arc<dyn HostPartStore>,
    keyhive: crate::keyhive::BigKeyhiveHandle,
    scope: GroupScopeHandle,
) -> Res<ConcurrentTaskOutput> {
    // The scope is read once per task; a scope change during the task is
    // handled by the worker's rescan on `GroupScopeHandle::changed`.
    let scope = scope.get();
    match task {
        FrontierTask::Publish {
            doc_id,
            admission,
            part_cursor,
            ..
        } => {
            // A scoped worker only processes documents whose live keyhive
            // group membership intersects its scope. Eligibility is checked
            // here so part events for documents that joined/left the scope
            // are handled by the same path as admission events.
            if scope.groups().is_some() {
                // The match-all part stream also carries non-document
                // part-store objects; `group_ids_containing_document` maps
                // their invalid ids to an empty group set (never in scope),
                // while real keyhive errors propagate and crash the worker
                // per the house error policy.
                let doc_groups = keyhive.group_ids_containing_document(doc_id).await?;
                if !scope.admits_doc_groups(&doc_groups) {
                    // The document left the worker's scope: tear down its
                    // frontier mirror so nothing stale is advertised to peers.
                    let am_obj_id = automerge_doc_obj_id(doc_id);
                    for part_id in frontier_store.obj_parts(am_obj_id).await? {
                        frontier_store
                            .remove_obj_from_part(am_obj_id, part_id)
                            .await?;
                    }
                    return Ok(ConcurrentTaskOutput::OutOfScope);
                }
            }
            let keyhive_watermark = admission.map(|source| source.cursor);
            let outcome = publish_heads(
                doc_id,
                &runtime,
                &keyhive,
                &big_sync_store,
                &frontier_store,
                keyhive_watermark,
                &scope,
            )
            .await?;
            Ok(match outcome {
                PublishOutcome::Published => ConcurrentTaskOutput::Published {
                    through: part_cursor,
                },
                PublishOutcome::Deferred => ConcurrentTaskOutput::Deferred,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(value: u64) -> crate::DocumentId {
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&value.to_be_bytes());
        crate::DocumentId::new(bytes)
    }

    #[test]
    fn duplicate_part_events_keep_the_latest_cursor() {
        let document = doc(12);
        let part = PartId::new([4; 32]);
        let mut pending = HashMap::new();
        pending
            .entry(document)
            .or_insert_with(BTreeMap::new)
            .insert(part, 2);
        pending.get_mut(&document).unwrap().insert(part, 9);
        assert_eq!(pending[&document][&part], 9);
    }

    #[test]
    fn scoped_frontier_mirroring_excludes_global_membership() {
        let group = PartId::new([8; 32]);
        let scope = WorkerGroupScope::Groups([group].into_iter().collect());
        assert!(!scope_includes_part(&scope, crate::GLOBAL_PART_ID));
        assert!(scope_includes_part(&scope, group));
        assert!(!scope_includes_part(&scope, PartId::new([9; 32])));
    }
}
