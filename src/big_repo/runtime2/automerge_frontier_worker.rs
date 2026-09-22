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
//!   part enumeration is frozen into the walker. Membership touches carry a
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
use big_sync_core::rpc::PartEvent;
use big_sync_core::scheduler::Retry;
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

/// Minimum delay before a publish that came back deferred is re-armed.
///
/// The deferral itself is not work applied: the document's bundle could not
/// reach the keyhive operation count within [`MATERIALIZATION_WAIT_TIMEOUT`],
/// or it was invalidated while waiting. A re-arm therefore has to be cheap but
/// not immediate — the scheduler doubles this delay per attempt up to its own
/// ceiling, and this worker's timer arm ticks the resulting deadline.
const DEFERRED_PUBLISH_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(1);

/// The frontier payload object id for a document.
///
/// Frontier payloads live in their own storage scope (derived from the main
/// scope key), so the raw `doc_id` cannot collide with the document's own
/// sedimentree object in the main scope.
pub fn automerge_doc_obj_id(doc_id: crate::DocumentId) -> ObjKey {
    ObjKey(big_sync_core::ByteKey::new(doc_id.as_bytes()))
}

pub fn automerge_obj_to_doc_id(obj_id: ObjKey) -> crate::DocumentId {
    crate::DocumentId::new(obj_id.as_bytes())
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
                            crate::DocumentId::new(operation.payload().doc_id().as_bytes()),
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
                    PartEvent::Changed(event) => event.obj_id.clone(),
                    PartEvent::Removed(event) => event.obj_id.clone(),
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
            tracing::debug!(
                keyhive_admission_cursor = kh_read_cursor,
                admission_durable_cursor = admission_durable,
                part_durable_cursor = part_durable,
                "AFW started"
            );

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
        part_id: PartKey,
    },
    AdvanceKhCursor(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum SourceKind {
    Admission,
    Parts,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
    tracing::debug!(
        %doc_id,
        keyhive_watermark = ?keyhive_watermark,
        "AFW publish task started"
    );
    // Internal acquisition: publishing a frontier must not make a document
    // nobody holds look live (that would apply received content into the
    // materialized bundle and emit user-visible change notifications for it).
    let Ok(crate::runtime2::types::DocLookup::Ready(handle)) =
        runtime.acquire_internal_doc_handle(doc_id.clone()).await
    else {
        tracing::debug!(%doc_id, "AFW publish deferred: document bundle not ready");
        return Ok(PublishOutcome::Deferred);
    };
    tracing::debug!(
        %doc_id,
        bundle_id = handle.bundle.id(),
        bundle_ops_count = handle.bundle.materialized_cgka_ops_count(),
        partially_decrypted = handle.bundle.is_partially_decrypted(),
        "AFW acquired live document bundle"
    );
    if keyhive_watermark.is_some() {
        // The admission's CGKA op advances the per-document operation count; the
        // hub forwards it to the doc worker, which re-materializes and updates
        // the bundle. Await the bundle to catch up before advertising heads.
        let target_ops_count = keyhive.current_cgka_ops_count(doc_id.clone()).await?;
        tracing::debug!(
            %doc_id,
            target_ops_count,
            bundle_ops_count = handle.bundle.materialized_cgka_ops_count(),
            "AFW waiting for bundle CGKA materialization"
        );
        match tokio::time::timeout(
            MATERIALIZATION_WAIT_TIMEOUT,
            handle.bundle.await_cgka_ops_count(target_ops_count),
        )
        .await
        {
            // The internal handle holds no lease, so the doc worker can be evicted —
            // invalidating this bundle — while this wait is in flight. Defer, exactly as
            // for a timeout: a later event publishes a fresh bundle.
            Ok(_) if handle.bundle.is_broken() => {
                tracing::debug!(
                    %doc_id,
                    "AFW publish deferred: document bundle invalidated while awaiting materialization"
                );
                return Ok(PublishOutcome::Deferred);
            }
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
        tracing::debug!(
            %doc_id,
            target_ops_count,
            bundle_ops_count = handle.bundle.materialized_cgka_ops_count(),
            "AFW bundle CGKA materialization barrier satisfied"
        );
    }
    // Never advertise heads from an invalidated bundle: its in-memory document may hold
    // mutations that never persisted (the worker was evicted, or a commit from it was
    // rejected), so publishing would advertise state that cannot be served. Failing the
    // task instead of deferring would take the worker down over a race that eviction is
    // allowed to win, which is why this is a defer and not an error.
    if handle.bundle.is_broken() {
        tracing::debug!(%doc_id, "AFW publish deferred: document bundle invalidated");
        return Ok(PublishOutcome::Deferred);
    }
    let causal_epoch = handle.bundle.current_causal_epoch();
    let heads = surelock::key::lock_scope(|key| {
        let (doc, _key) = key.lock(&handle.bundle.doc);
        doc.get_heads()
    });
    let heads_formatted = am_utils_rs::serialize_commit_heads(&heads);
    let am_obj_id = automerge_doc_obj_id(doc_id.clone());
    let payload = serde_json::json!({
        "heads": heads_formatted,
        "causal_epoch": causal_epoch,
    });
    // The check above only covers the read: eviction is allowed to win while this task is
    // blocked on the document lock or building the payload, and `mark_broken` is an atomic
    // store that takes no lock, so it can land at any instant. Re-check with no await between
    // the check and the write, so a bundle invalidated in that window defers instead of
    // publishing heads that cannot be served. (A break during the write itself needs no
    // handling here: the write is already issued, and the materialization that follows a
    // re-acquisition re-triggers a keyed replacement publish that overwrites stale heads.)
    if handle.bundle.is_broken() {
        tracing::debug!(
            %doc_id,
            "AFW publish deferred: document bundle invalidated before writing frontier payload"
        );
        return Ok(PublishOutcome::Deferred);
    }
    frontier_store
        .set_obj_payload(am_obj_id.clone(), payload)
        .await?;
    tracing::debug!(
        %doc_id,
        head_count = heads.len(),
        "AFW frontier payload written"
    );
    let desired_parts = big_sync_store
        .obj_parts(am_obj_id.clone())
        .await?
        .into_iter()
        .filter(|part_id| scope_includes_part(scope, part_id.clone()))
        .collect::<Vec<_>>();
    let current_parts = frontier_store.obj_parts(am_obj_id.clone()).await?;
    if !desired_parts.is_empty() {
        frontier_store
            .add_obj_to_parts(am_obj_id.clone(), desired_parts.clone())
            .await?;
    }
    let current_part_count = current_parts.len();
    for part_id in current_parts {
        if !desired_parts.contains(&part_id) {
            frontier_store
                .remove_obj_from_part(am_obj_id.clone(), part_id)
                .await?;
        }
    }
    tracing::debug!(
        %doc_id,
        desired_part_count = desired_parts.len(),
        current_part_count,
        "AFW frontier publication complete"
    );
    Ok(PublishOutcome::Published)
}

/// A scoped worker mirrors only explicitly selected partitions.
fn scope_includes_part(scope: &WorkerGroupScope, part_id: PartKey) -> bool {
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
    type Entry = PartEvent;
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
            .open_revision_reader_all(after)
            .await
            .wrap_err("opening local part revision reader")??;
        Ok(LocalPartRevisionReaderAdapter { inner })
    }
}

#[async_trait::async_trait]
impl RevisionedStoreReader<u64, PartEvent, eyre::Report> for LocalPartRevisionReaderAdapter {
    async fn next(
        &mut self,
        limits: RevisionReadLimits,
    ) -> Result<RevisionRead<u64, PartEvent>, eyre::Report> {
        self.inner.next(limits).await
    }
}

const CONCURRENT_TASK_BUDGET: usize = 64;
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
    pending_parts: HashMap<crate::DocumentId, BTreeMap<PartKey, u64>>,
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
                        .remove_obj_from_part(automerge_doc_obj_id(doc_id.clone()), part_id.clone())
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
        tracing::debug!(
            key = ?key,
            task = ?task,
            active_tasks = self.tasks.active_count(),
            "AFW scheduling keyed task"
        );
        let future = self.task_future(&task);
        self.tasks.replace(key.clone(), task, future)?;
        tracing::debug!(?key, "AFW keyed task accepted by scheduler");
        Ok(())
    }

    /// The publish command built from the newest work known for `doc_id`.
    fn publish_task(&self, doc_id: crate::DocumentId) -> FrontierTask {
        FrontierTask::Publish {
            doc_id: doc_id.clone(),
            admission: self.pending_admission.get(&doc_id).cloned(),
            part_source: self.pending_part_sources.get(&doc_id).cloned(),
            part_cursor: self
                .pending_parts
                .get(&doc_id)
                .and_then(|parts| parts.values().copied().max()),
        }
    }

    fn task_future(
        &self,
        task: &FrontierTask,
    ) -> impl std::future::Future<Output = Res<ConcurrentTaskOutput>> + Send + 'static {
        run_concurrent_frontier_task(
            task.clone(),
            self.runtime.clone(),
            Arc::clone(&self.big_sync_store),
            Arc::clone(&self.frontier_store),
            self.keyhive.clone(),
            self.scope.clone(),
        )
    }

    /// Re-arm a publish that came back `Deferred`, after a backoff.
    ///
    /// A deferred publish applied nothing: the bundle did not reach the keyhive
    /// operation count in time, or it was invalidated while waiting. Its
    /// admission and part-source cursors therefore stay unacked — deliberately,
    /// because a source is acked only once the effect it covers is durable, and
    /// this task has no effect to show. What must not happen is a park with no
    /// re-drive: parking is re-armed only by an external wake (a newer event for
    /// the same document), so a document whose next event never arrives would
    /// leave those cursors unacked forever and, since the walker advances only
    /// over a contiguous settled prefix, stall every document behind it.
    ///
    /// The scheduler owns the backoff: each attempt doubles the delay up to its
    /// own ceiling, and `machine_loop`'s timer arm ticks the resulting deadline,
    /// so this cannot spin. A newer event for the document stays the fast path:
    /// the wake path (`start_ready_document_work` -> `start_publish`) replaces
    /// the pending retry and publishes immediately.
    fn retry_publish(&mut self, doc_id: crate::DocumentId, retry: Retry) -> Res<()> {
        let key = FrontierKey::Document(doc_id.clone());
        let task = self.publish_task(doc_id);
        let future = self.task_future(&task);
        rearm_deferred_publish(&mut self.tasks, key.clone(), task, retry, future)?;
        tracing::debug!(
            ?key,
            attempt_no = retry.attempt_no + 1,
            "AFW re-armed a deferred publish"
        );
        Ok(())
    }

    fn start_publish(&mut self, doc_id: crate::DocumentId) -> Res<()> {
        tracing::debug!(
            %doc_id,
            active_tasks = self.tasks.active_count(),
            pending_admission = ?self.pending_admission.get(&doc_id),
            pending_part_source = ?self.pending_part_sources.get(&doc_id),
            pending_part_count = self.pending_parts.get(&doc_id).map_or(0, |parts| parts.len()),
            "AFW preparing frontier publish"
        );
        let task = self.publish_task(doc_id.clone());
        self.start_task(FrontierKey::Document(doc_id), task)
    }

    fn remember_part_source(&mut self, doc_id: crate::DocumentId, source: SourceCursor) {
        self.pending_part_sources
            .entry(doc_id)
            .and_modify(|old| {
                if source.cursor > old.cursor {
                    *old = source.clone();
                }
            })
            .or_insert(source);
    }

    async fn acknowledge_source(&mut self, source: SourceCursor) -> Res<()> {
        let ack = match source.source {
            SourceKind::Admission => self.admission.ack(source.key, source.cursor).await?,
            SourceKind::Parts => self.parts.ack(source.key, source.cursor).await?,
        };
        tracing::debug!(
            source = ?source.source,
            source_cursor = source.cursor,
            ack = ?ack,
            "AFW acknowledged source"
        );
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
            key: delta.key.clone(),
            cursor: delta.cursor,
        };
        tracing::debug!(
            source_cursor = source.cursor,
            "AFW consumed Keyhive admission delta"
        );
        match delta.key {
            // The walker's `key_of` already decoded the event: Cgka
            // operations key by document and go straight to publication.
            FrontierKey::Document(doc_id) => {
                self.pending_admission
                    .entry(doc_id.clone())
                    .and_modify(|old| {
                        if source.cursor > old.cursor {
                            *old = source.clone();
                        }
                    })
                    .or_insert(source.clone());
                tracing::debug!(
                    %doc_id,
                    source_cursor = source.cursor,
                    pending_admission_cursor = ?self.pending_admission.get(&doc_id),
                    "AFW queued admission for document publish"
                );
                self.start_publish(doc_id)
            }
            // Admission rows with no document payload only gate the cursor.
            FrontierKey::Decode(_) => self.acknowledge_source(source).await,
        }
    }

    async fn on_part_delta(
        &mut self,
        delta: big_sync_core::concurrent_delta_walker::ConcurrentDelta<FrontierKey, PartEvent>,
    ) -> Res<()> {
        let source = SourceCursor {
            source: SourceKind::Parts,
            key: delta.key,
            cursor: delta.cursor,
        };
        tracing::debug!(
            source_cursor = source.cursor,
            "AFW consumed part revision delta"
        );
        match delta.entry {
            PartEvent::Changed(event) => {
                let doc_id = automerge_obj_to_doc_id(event.obj_id);
                self.remember_part_source(doc_id.clone(), source.clone());
                tracing::debug!(%doc_id, source_cursor = source.cursor, "AFW mapped changed part revision to document");
                let parts = self.pending_parts.entry(doc_id.clone()).or_default();
                for part_id in event.part_ids {
                    parts.insert(part_id, event.cursor);
                }
                self.start_publish(doc_id)?;
            }
            PartEvent::Removed(event) => {
                let doc_id = automerge_obj_to_doc_id(event.obj_id);
                tracing::debug!(%doc_id, source_cursor = source.cursor, "AFW mapped removed part revision to document");
                let empty = self.pending_parts.get_mut(&doc_id).is_some_and(|parts| {
                    parts.remove(&event.part_id);
                    parts.is_empty()
                });
                if empty {
                    self.pending_parts.remove(&doc_id);
                    self.pending_part_sources.remove(&doc_id);
                    if !self.pending_admission.contains_key(&doc_id) {
                        self.tasks.cancel(FrontierKey::Document(doc_id.clone()));
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
        }
        Ok(())
    }

    fn on_local_notifications(&mut self, notifications: Vec<BigRepoLocalNotification>) {
        tracing::debug!(
            notification_count = notifications.len(),
            "AFW received local notifications"
        );
        for notification in notifications {
            match notification {
                BigRepoLocalNotification::DocMaterializationReady { doc_id, .. }
                | BigRepoLocalNotification::DocHeadsUpdated { doc_id, .. } => {
                    tracing::debug!(%doc_id, "AFW waking document after local notification");
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
        // Captured before the match moves the command: the scheduler hands each
        // completion the retry bookkeeping of the attempt it just ran, which is
        // what a re-arm needs to advance its backoff.
        let retry = completion.retry;
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
                tracing::debug!(
                    %doc_id,
                    through,
                    admission = ?admission,
                    part_source = ?part_source,
                    "AFW publish task completed"
                );
                if let Some(source) = admission {
                    self.acknowledge_source(source.clone()).await?;
                    if self.pending_admission.get(&doc_id).cloned() == Some(source) {
                        self.pending_admission.remove(&doc_id);
                    }
                }
                if let Some(source) = part_source {
                    self.acknowledge_source(source.clone()).await?;
                    if self.pending_part_sources.get(&doc_id).cloned() == Some(source) {
                        self.pending_part_sources.remove(&doc_id);
                    }
                }
                if !self.settle_doc_txid(doc_id.clone(), through) {
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
                    self.acknowledge_source(source.clone()).await?;
                    if self.pending_admission.get(&doc_id).cloned() == Some(source) {
                        self.pending_admission.remove(&doc_id);
                    }
                }
                if let Some(source) = part_source {
                    self.acknowledge_source(source.clone()).await?;
                    if self.pending_part_sources.get(&doc_id).cloned() == Some(source) {
                        self.pending_part_sources.remove(&doc_id);
                    }
                }
                self.pending_parts.remove(&doc_id);
            }
            (
                ref task @ FrontierTask::Publish { ref doc_id, .. },
                Ok(ConcurrentTaskOutput::Deferred),
            ) => {
                tracing::debug!(
                    %doc_id,
                    task = ?task,
                    attempt_no = retry.attempt_no,
                    "AFW publish task deferred; re-arming after a backoff"
                );
                self.retry_publish(doc_id.clone(), retry)?;
            }
            (task, Err(error)) => {
                tracing::error!(task = ?task, error = ?error, "AFW task failed");
                panic!("automerge frontier task failed: {error:?}");
            }
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
            let doc_id = crate::DocumentId::new(doc.as_bytes());
            self.wake_docs.insert(doc_id);
        }
        Ok(())
    }

    fn start_ready_document_work(&mut self) -> Res<()> {
        for doc_id in std::mem::take(&mut self.wake_docs) {
            tracing::debug!(%doc_id, active_tasks = self.tasks.active_count(), "AFW considering woken document");
            if !self
                .tasks
                .has_capacity_for(FrontierKey::Document(doc_id.clone()))
            {
                tracing::debug!(%doc_id, "AFW retaining woken document: scheduler at capacity");
                self.wake_docs.insert(doc_id);
                continue;
            }
            self.start_publish(doc_id)?;
        }
        Ok(())
    }
}

/// Re-arm a deferred publish through the scheduler's bounded backoff.
///
/// The alternative this replaces is `park`, which keeps the retained command
/// but only ever runs it again on an external wake. A deferred publish has not
/// applied its work, so its sources stay unacked (see
/// [`Worker::retry_publish`]), and no wake may ever arrive for that document —
/// in which case the unacked sources hold the walker's contiguous prefix and
/// stall every document behind it. Deliberately not `async`: it is a scheduler
/// transition, not coordination through the worker.
fn rearm_deferred_publish(
    tasks: &mut big_sync_core::tokio_keyed_scheduler::TokioKeyedScheduler<
        FrontierKey,
        FrontierTask,
        ConcurrentTaskOutput,
    >,
    key: FrontierKey,
    task: FrontierTask,
    retry: Retry,
    future: impl std::future::Future<Output = Res<ConcurrentTaskOutput>> + Send + 'static,
) -> Res<()> {
    tasks.retry(key, task, retry, DEFERRED_PUBLISH_RETRY_DELAY, future)?;
    Ok(())
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
            tracing::debug!(
                scoped = scope.groups().is_some(),
                "AFW physical publish task entered"
            );
            // This scope carries documents only: non-document derived state
            // lives in its own scope (see `BigRepo::derived_part_store`). Tests
            // assert that boundary so a new non-document producer is surfaced
            // here instead of quietly consuming a document worker.
            #[cfg(any(test, feature = "test-support"))]
            assert!(
                crate::keyhive::BigKeyhiveHandle::is_valid_keyhive_document_id(doc_id.clone()),
                "non-document object reached the automerge frontier source: obj_id={doc_id}. \
                 State that is not a document belongs in the derived scope, not the document scope"
            );
            if scope.groups().is_some() {
                // Eligibility is checked against live Keyhive membership so part
                // events for documents that joined or left the scope are handled
                // by the same path as admission events.
                // The walk is a shared-lock traversal. It is deliberately not bounded
                // by a timeout: a timeout defers the publish, and a defer with no
                // re-drive stalls this document's cursor while looking like progress,
                // which hides a hang instead of surfacing it. A `Groups(∅)` scope
                // admits no document at all, so its answer cannot depend on the walk
                // and the walk is skipped there.
                let doc_groups = if scope.admits_nothing() {
                    Default::default()
                } else {
                    tracing::debug!(%doc_id, "AFW checking live document scope membership");
                    keyhive
                        .group_ids_containing_document(doc_id.clone())
                        .await?
                };
                tracing::debug!(%doc_id, doc_groups = ?doc_groups, "AFW resolved live document scope membership");
                if !scope.admits_doc_groups(&doc_groups) {
                    // The document left the worker's scope: tear down its
                    // frontier mirror so nothing stale is advertised to peers.
                    let am_obj_id = automerge_doc_obj_id(doc_id);
                    for part_id in frontier_store.obj_parts(am_obj_id.clone()).await? {
                        frontier_store
                            .remove_obj_from_part(am_obj_id.clone(), part_id)
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
        let part = PartKey::new([4; 32]);
        let mut pending = HashMap::new();
        pending
            .entry(document.clone())
            .or_insert_with(BTreeMap::new)
            .insert(part.clone(), 2);
        pending.get_mut(&document).unwrap().insert(part.clone(), 9);
        assert_eq!(pending[&document][&part], 9);
    }

    #[test]
    fn scoped_frontier_mirroring_excludes_parts_outside_its_groups() {
        let group = PartKey::new([8; 32]);
        let scope = WorkerGroupScope::Groups([group.clone()].into_iter().collect());
        assert!(scope_includes_part(&scope, group));
        assert!(!scope_includes_part(&scope, PartKey::new([9; 32])));
    }

    // The deferred-publish tests drive the real scheduler with the real key,
    // command and output types, following the exact sequence
    // `on_task_completion`'s deferred arm performs. What they cannot cover from
    // here is the worker's own state: that the deferred path leaves
    // `pending_admission`/`pending_part_sources` unacked, and that `machine_loop`
    // ticks the deadline this re-arm installs.
    type KeyedTasks = big_sync_core::tokio_keyed_scheduler::TokioKeyedScheduler<
        FrontierKey,
        FrontierTask,
        ConcurrentTaskOutput,
    >;

    fn publish_cmd(value: u64) -> FrontierTask {
        FrontierTask::Publish {
            doc_id: doc(value),
            admission: None,
            part_source: None,
            part_cursor: None,
        }
    }

    async fn deferred() -> Res<ConcurrentTaskOutput> {
        Ok(ConcurrentTaskOutput::Deferred)
    }

    async fn published(through: Option<u64>) -> Res<ConcurrentTaskOutput> {
        Ok(ConcurrentTaskOutput::Published { through })
    }

    /// A deferred publish is re-armed on a delay. Re-arming immediately would
    /// spend the whole task budget spinning on the document that deferred, and
    /// the delay is what makes the retry bounded work instead of a hot loop.
    #[tokio::test]
    async fn a_deferred_publish_is_rearmed_after_a_delay_not_immediately() {
        let mut tasks = KeyedTasks::new(4);
        let key = FrontierKey::Document(doc(7));
        tasks
            .replace(key.clone(), publish_cmd(7), deferred())
            .unwrap();

        let completion = tasks.next_completion().await.unwrap();
        assert!(matches!(
            completion.result,
            Ok(ConcurrentTaskOutput::Deferred)
        ));

        let before = std::time::Instant::now();
        rearm_deferred_publish(
            &mut tasks,
            key.clone(),
            completion.command.clone(),
            completion.retry,
            published(Some(1)),
        )
        .unwrap();

        let deadline = tasks
            .next_deadline()
            .expect("a re-armed publish must carry a deadline");
        assert!(
            deadline >= before + DEFERRED_PUBLISH_RETRY_DELAY,
            "a deferred publish must not be re-armed immediately"
        );
        assert!(
            deadline <= std::time::Instant::now() + DEFERRED_PUBLISH_RETRY_DELAY,
            "the first re-arm waits one delay, not longer"
        );

        // The deadline is the only thing that runs it: ticking past it must
        // execute the retry, so a deferred document makes progress with no new
        // event of its own.
        tasks
            .tick(std::time::Instant::now() + DEFERRED_PUBLISH_RETRY_DELAY)
            .unwrap();
        let retried = tasks.next_completion().await.unwrap();
        assert!(matches!(
            retried.result,
            Ok(ConcurrentTaskOutput::Published { through: Some(1) })
        ));
    }

    /// A newer event for the document stays the fast path: the wake path calls
    /// `start_publish` -> `replace` for the same key, which must cancel the
    /// pending retry instead of racing it into a second task for that key.
    #[tokio::test]
    async fn a_publish_woken_while_a_retry_is_pending_replaces_it_without_double_spawning() {
        let mut tasks = KeyedTasks::new(4);
        let key = FrontierKey::Document(doc(9));
        tasks
            .replace(key.clone(), publish_cmd(9), deferred())
            .unwrap();
        let completion = tasks.next_completion().await.unwrap();
        rearm_deferred_publish(
            &mut tasks,
            key.clone(),
            completion.command.clone(),
            completion.retry,
            deferred(),
        )
        .unwrap();

        // The newer event lands before the retry is due.
        tasks
            .replace(key.clone(), publish_cmd(9), published(Some(4)))
            .unwrap();
        let woken = tasks.next_completion().await.unwrap();
        assert!(matches!(
            woken.result,
            Ok(ConcurrentTaskOutput::Published { through: Some(4) })
        ));

        assert!(
            tasks.next_deadline().is_none(),
            "the replaced retry must not stay due"
        );
        tasks
            .tick(std::time::Instant::now() + DEFERRED_PUBLISH_RETRY_DELAY * 4)
            .unwrap();
        assert!(
            tasks.next_deadline().is_none(),
            "the replaced retry must not resurface"
        );
    }

    /// The backoff belongs to the attempt, not to the key: repeated deferrals
    /// widen the delay, and an attempt that publishes resets it so the next
    /// deferral does not inherit an exhausted backoff.
    #[tokio::test]
    async fn deferred_retries_widen_their_delay_and_a_publish_resets_it() {
        let mut tasks = KeyedTasks::new(4);
        let key = FrontierKey::Document(doc(11));

        tasks
            .replace(key.clone(), publish_cmd(11), deferred())
            .unwrap();
        let first = tasks.next_completion().await.unwrap();
        rearm_deferred_publish(
            &mut tasks,
            key.clone(),
            first.command.clone(),
            first.retry,
            deferred(),
        )
        .unwrap();
        let first_deadline = tasks.next_deadline().expect("first retry is due");

        tasks.tick(first_deadline).unwrap();
        let second = tasks.next_completion().await.unwrap();
        assert_eq!(
            second.retry.attempt_no, 2,
            "the re-arm is the second attempt"
        );
        let before = std::time::Instant::now();
        rearm_deferred_publish(
            &mut tasks,
            key.clone(),
            second.command.clone(),
            second.retry,
            deferred(),
        )
        .unwrap();
        let second_deadline = tasks.next_deadline().expect("second retry is due");
        assert!(
            second_deadline > before + DEFERRED_PUBLISH_RETRY_DELAY,
            "a repeated deferral must widen the delay"
        );

        // A publish that succeeds, then defers again, starts from the minimum
        // delay rather than the widened one.
        tasks
            .replace(key.clone(), publish_cmd(11), published(None))
            .unwrap();
        tasks.next_completion().await.unwrap();
        tasks
            .replace(key.clone(), publish_cmd(11), deferred())
            .unwrap();
        let third = tasks.next_completion().await.unwrap();
        let before = std::time::Instant::now();
        rearm_deferred_publish(
            &mut tasks,
            key.clone(),
            third.command.clone(),
            third.retry,
            deferred(),
        )
        .unwrap();
        let third_deadline = tasks.next_deadline().expect("third retry is due");
        assert!(third_deadline >= before + DEFERRED_PUBLISH_RETRY_DELAY);
        assert!(
            third_deadline <= std::time::Instant::now() + DEFERRED_PUBLISH_RETRY_DELAY,
            "a published attempt must reset the backoff"
        );
    }
}
