//! Background worker that maintains the Automerge Frontier partition log.
//!
//! Two independent inputs feed one publication decision:
//!
//! - **Keyhive admission stream** ([`SqliteBigRepoStore::admission_events_after`]):
//!   rows exist only after their effects are visible in the keyhive graph;
//!   publication reattempts the live doc's materialization and waits for that
//!   admission sequence before reading heads. Decode work is keyed by admission
//!   sequence and publication is keyed by document, allowing independent documents to make
//!   progress while the durable cursor still waits for its contiguous prefix.
//! - **Source-part subscriptions** (`subscribe_local(SubPartsRequest)`):
//!   Added/Changed events carry a commit watermark; publishing waits for the
//!   doc bundle to reach that watermark before reading heads. Per-part
//!   cursors persist after success only.
//!
//! The split follows the house sans-io pattern via [`driver`]:
//! [`AutomergeFrontierCore`] is a pure reducer;
//! [`FrontierSource`] multiplexes the admission poll with the part
//! subscription and parts-change channel (all I/O);
//! [`FrontierExec`] executes concurrent document publications and serial cursor commands.
//!
//! Startup rule: a durable keyhive cursor of 0 means nothing was ever
//! published — the fresh part subscription then replays every stored object
//! at cursor 0, which *is* the full build; otherwise both streams tail from
//! their cursors. There is no gap handling: neither log is pruned.
use crate::changes::{BigRepoLocalNotification, LocalFilter};
use crate::interlude::*;
use crate::runtime2::{WorkerGroupScope, driver};
use crate::store::sqlite::SqliteBigRepoStore;
use big_sync::HostPartStore;
use big_sync_core::outbox::Outbox;
use big_sync_core::rpc::{SubEvent, SubPartsRequest, SubscriptionTarget};
use big_sync_core::scheduler::{KeyedScheduler, SpawnedTask, TaskId};
use big_sync_core::watermark::WatermarkMachine;
use future_form::Sendable;
use keyhive_core::event::static_event::StaticEvent;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

const EVENT_BATCH_SIZE: u32 = 64;
const IDLE_POLL: Duration = Duration::from_millis(25);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublishOutcome {
    Published,
    Deferred,
}

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

#[allow(clippy::too_many_arguments)]
pub fn spawn_automerge_frontier_worker(
    store: SqliteBigRepoStore,
    big_sync_store: Arc<dyn HostPartStore>,
    frontier_store: Arc<dyn HostPartStore>,
    runtime: crate::runtime2::Runtime2Handle<Sendable>,
    _evt_tx: async_channel::Sender<crate::runtime2::Runtime2Evt>,
    change_manager: Arc<crate::changes::ChangeListenerManager>,
    keyhive: crate::keyhive::BigKeyhiveHandle,
    scope: WorkerGroupScope,
) -> SpawnedAutomergeFrontierWorker<Sendable> {
    let (abort_handle, abort_registration) = futures::future::AbortHandle::new_pair();

    let run = Sendable::from_future(async move {
        let fut = run_automerge_frontier_tail(
            store,
            big_sync_store,
            frontier_store,
            runtime,
            keyhive,
            scope,
            change_manager,
        );
        match futures::future::Abortable::new(fut, abort_registration).await {
            Ok(result) => result,
            Err(_) => Ok(()),
        }
    });

    SpawnedAutomergeFrontierWorker {
        stop: AutomergeFrontierWorkerStopToken {
            abort: abort_handle,
        },
        run,
    }
}

/// Commands whose ordering matters for cursor persistence and source parts.
#[derive(Debug)]
enum Cmd {
    PublishHeadsWatermarked {
        doc_id: crate::DocumentId,
        cursor: Option<u64>,
    },
    CommitPartCursor {
        part_id: PartId,
        cursor: u64,
    },
    RemoveFrontierMembership {
        doc_id: crate::DocumentId,
        part_id: PartId,
        cursor: u64,
    },
    AdvanceKhCursor(u64),
}

#[derive(Debug)]
enum Evt {
    DocumentReady {
        doc_id: crate::DocumentId,
    },
    Published {
        doc_id: crate::DocumentId,
        through: Option<u64>,
    },
    Deferred {
        doc_id: crate::DocumentId,
    },
    KhRows(Vec<driver::AdmittedRow>),
    Decoded {
        seq: u64,
        doc: Option<crate::DocumentId>,
    },
    TaskSettled {
        key: FrontierKey,
        covered: BTreeSet<u64>,
    },
    PartAdded {
        doc_id: crate::DocumentId,
        part_id: PartId,
        cursor: u64,
    },
    PartChanged {
        doc_id: crate::DocumentId,
        cursor: u64,
        part_ids: Vec<PartId>,
    },
    PartRemoved {
        doc_id: crate::DocumentId,
        part_id: PartId,
        cursor: u64,
    },
    CursorPersisted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum FrontierKey {
    Decode(u64),
    Document(crate::DocumentId),
}

#[derive(Debug, Clone)]
enum FrontierSeed {
    Decode {
        seq: u64,
        bytes: Arc<[u8]>,
    },
    Document {
        doc: crate::DocumentId,
        covered: BTreeSet<u64>,
    },
}

impl FrontierSeed {
    fn key(&self) -> FrontierKey {
        match self {
            Self::Decode { seq, .. } => FrontierKey::Decode(*seq),
            Self::Document { doc, .. } => FrontierKey::Document(*doc),
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
enum FrontierTaskOutput {
    Decoded {
        seq: u64,
        doc: Option<crate::DocumentId>,
    },
    Settled {
        key: FrontierKey,
        covered: BTreeSet<u64>,
    },
    Deferred {
        doc: crate::DocumentId,
    },
}

#[derive(Default)]
struct AutomergeFrontierCore {
    machine: WatermarkMachine<(), u64, FrontierKey, (), u64>,
    outbox: Outbox<Cmd, ()>,
    scheduler: KeyedScheduler<FrontierKey, FrontierSeed>,
    pending_rows: BTreeMap<u64, driver::AdmittedRow>,
    ready_seeds: Vec<FrontierSeed>,
    pending_parts: HashMap<crate::DocumentId, BTreeMap<PartId, u64>>,
    queued_docs: HashSet<crate::DocumentId>,
}

impl AutomergeFrontierCore {
    fn on_evt(&mut self, evt: Evt) {
        match evt {
            Evt::KhRows(rows) => self.on_rows(rows),
            Evt::DocumentReady { doc_id } => {
                let cursor = self
                    .pending_parts
                    .get(&doc_id)
                    .and_then(|parts| parts.values().copied().max());
                self.queue_publish(doc_id, cursor);
            }
            Evt::Published { doc_id, through } => {
                self.queued_docs.remove(&doc_id);
                if let Some(parts) = self.pending_parts.remove(&doc_id) {
                    let mut remaining = BTreeMap::new();
                    for (part_id, cursor) in parts {
                        if through.is_none_or(|limit| cursor > limit) {
                            remaining.insert(part_id, cursor);
                            continue;
                        }
                        self.outbox
                            .push(Cmd::CommitPartCursor { part_id, cursor }, ());
                    }
                    if let Some(cursor) = remaining.values().copied().max() {
                        self.pending_parts.insert(doc_id, remaining);
                        self.queue_publish(doc_id, Some(cursor));
                    }
                }
            }
            Evt::Deferred { doc_id } => {
                self.queued_docs.remove(&doc_id);
            }
            Evt::Decoded { seq, doc } => self.on_decoded(seq, doc),
            Evt::TaskSettled { key, covered } => {
                for seq in covered {
                    self.settle_lane(seq, key);
                }
            }
            Evt::PartAdded {
                doc_id,
                part_id,
                cursor,
            } => {
                self.pending_parts
                    .entry(doc_id)
                    .or_default()
                    .entry(part_id)
                    .and_modify(|old| *old = (*old).max(cursor))
                    .or_insert(cursor);
                self.queue_publish(doc_id, Some(cursor));
            }
            Evt::PartChanged {
                doc_id,
                cursor,
                part_ids,
            } => {
                for part_id in part_ids {
                    self.pending_parts
                        .entry(doc_id)
                        .or_default()
                        .entry(part_id)
                        .and_modify(|old| *old = (*old).max(cursor))
                        .or_insert(cursor);
                }
                self.queue_publish(doc_id, Some(cursor));
            }
            Evt::PartRemoved {
                doc_id,
                part_id,
                cursor,
            } => {
                let empty = self.pending_parts.get_mut(&doc_id).is_some_and(|parts| {
                    parts.remove(&part_id);
                    parts.is_empty()
                });
                if empty {
                    self.pending_parts.remove(&doc_id);
                }
                self.outbox.push(
                    Cmd::RemoveFrontierMembership {
                        doc_id,
                        part_id,
                        cursor,
                    },
                    (),
                );
            }
            Evt::CursorPersisted => {
                // The cursor commit is the side effect (executed by the
                // outbox); the machine has nothing to do with the
                // acknowledgment.
            }
        }
    }

    fn queue_publish(&mut self, doc_id: crate::DocumentId, cursor: Option<u64>) {
        if self.queued_docs.insert(doc_id) {
            let cursor = self
                .pending_parts
                .get(&doc_id)
                .and_then(|parts| parts.values().copied().max())
                .or(cursor);
            self.outbox
                .push(Cmd::PublishHeadsWatermarked { doc_id, cursor }, ());
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
        // The Vec is inherent to the take-then-schedule pattern: the caller
        // mutates the machine while iterating, so the rows must be owned.
        std::mem::take(&mut self.pending_rows)
            .into_values()
            .collect()
    }

    fn schedule_decode(&mut self, now: Instant, row: driver::AdmittedRow) {
        let seq = row.seq;
        self.machine
            .track((), seq, seq, [FrontierKey::Decode(seq)], ());
        self.scheduler.replace(
            now,
            FrontierKey::Decode(seq),
            FrontierSeed::Decode {
                seq,
                bytes: row.bytes,
            },
        );
    }

    fn schedule_seed(&mut self, now: Instant, seed: FrontierSeed) {
        let key = seed.key();
        self.scheduler
            .replace_with(now, key, seed, FrontierSeed::merge);
    }

    fn take_ready_seeds(&mut self) -> Vec<FrontierSeed> {
        std::mem::take(&mut self.ready_seeds)
    }

    fn on_decoded(&mut self, seq: u64, doc: Option<crate::DocumentId>) {
        if let Some(doc) = doc {
            let key = FrontierKey::Document(doc);
            self.machine.track((), seq, seq, [key], ());
            self.ready_seeds.push(FrontierSeed::Document {
                doc,
                covered: [seq].into_iter().collect(),
            });
        }
        self.settle_lane(seq, FrontierKey::Decode(seq));
    }

    fn settle_lane(&mut self, seq: u64, key: FrontierKey) {
        // The keyhive cursor commit is monotonic (MAX), so when several
        // streams reach watermarks in one settle only the last one needs to
        // be persisted.
        let mut watermark: Option<u64> = None;
        for (_, reached) in self.machine.settle(seq, seq, key) {
            if let Some(reached) = reached {
                watermark = Some(match watermark {
                    Some(prev) => prev.max(reached),
                    None => reached,
                });
            }
        }
        if let Some(watermark) = watermark {
            self.outbox.push(Cmd::AdvanceKhCursor(watermark), ());
        }
    }

    fn has_outstanding_work(&self) -> bool {
        !self.pending_rows.is_empty()
            || !self.ready_seeds.is_empty()
            || !self.machine.is_settled(&())
            || !self.outbox.is_empty()
    }
}

impl crate::runtime2::driver::StreamMachine for AutomergeFrontierCore {
    type Evt = Evt;
    type Cmd = Cmd;
    type Seed = FrontierSeed;
    type TaskOutput = FrontierTaskOutput;

    fn on_evt(&mut self, evt: Evt) {
        AutomergeFrontierCore::on_evt(self, evt);
    }
    fn front_cmd(&mut self) -> Option<(utils_rs::prelude::Uuid, &Cmd)> {
        self.outbox
            .front()
            .map(|(pending, cmd)| (pending.id(), cmd))
    }
    fn complete_cmd(&mut self, id: utils_rs::prelude::Uuid) {
        let (_, _) = self.outbox.complete(id);
    }
    fn complete_job(&mut self, id: TaskId) -> bool {
        self.scheduler.complete(id)
    }
    fn drain_stop_queue(&mut self) -> std::collections::hash_set::Drain<'_, TaskId> {
        self.scheduler.drain_stop_queue()
    }
    fn job_completed_evt(&mut self, _job: TaskId) -> Evt {
        unreachable!("automerge frontier tasks complete through keyed task results")
    }
    fn drain_spawn_queue(&mut self) -> std::vec::Drain<'_, SpawnedTask<FrontierSeed>> {
        self.scheduler.drain_spawn_queue()
    }
    fn tick_scheduler(&mut self, now: Instant) {
        self.scheduler.tick(now);
    }
    fn is_idle(&mut self) -> bool {
        !self.has_outstanding_work()
    }
}

/// Publish heads after the live bundle reaches the supplied materialization
/// barriers, then read them under the keyring lock. No-op when the doc isn't
/// materialized yet (its admission/part event will re-trigger later).
#[allow(clippy::too_many_arguments)]
async fn publish_heads(
    doc_id: crate::DocumentId,
    runtime: &crate::runtime2::Runtime2Handle<Sendable>,
    store: &SqliteBigRepoStore,
    big_sync_store: &Arc<dyn HostPartStore>,
    frontier_store: &Arc<dyn HostPartStore>,
    commit_watermark_cursor: Option<u64>,
    keyhive_watermark: Option<u64>,
    scope: &WorkerGroupScope,
) -> Res<PublishOutcome> {
    let watermark = if let Some(cursor) = commit_watermark_cursor {
        let Some(watermark) = store.get_sync_commit_watermark(doc_id, cursor).await? else {
            return Ok(PublishOutcome::Deferred);
        };
        Some(watermark)
    } else {
        None
    };
    let Ok(crate::runtime2::types::DocLookup::Ready(bundle)) = runtime.get_doc_handle(doc_id).await
    else {
        return Ok(PublishOutcome::Deferred);
    };
    if let Some(target_seq) = keyhive_watermark {
        bundle.await_keyhive_watermark(target_seq).await?;
    }
    if let Some(target_row_id) = watermark {
        bundle.await_commit_watermark(target_row_id).await?;
    }
    let heads = surelock::key::lock_scope(|key| {
        let (doc, _key) = key.lock(&bundle.doc);
        doc.get_heads()
    });
    let heads_formatted = am_utils_rs::serialize_commit_heads(&heads);
    let am_obj_id = automerge_doc_obj_id(doc_id);
    let payload = serde_json::json!({ "heads": heads_formatted });
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

fn scope_includes_part(scope: &WorkerGroupScope, part_id: PartId) -> bool {
    scope
        .groups()
        .is_none_or(|groups| part_id == crate::GLOBAL_PART_ID || groups.contains(&part_id))
}

async fn subscribe_to_parts(
    big_sync_store: &Arc<dyn HostPartStore>,
    store: &SqliteBigRepoStore,
    parts: &HashSet<PartId>,
) -> Res<Option<big_sync_core::mpsc::Receiver<SubEvent>>> {
    if parts.is_empty() {
        return Ok(None);
    }
    let mut targets = HashSet::new();
    for &part_id in parts {
        let cursor = store.automerge_part_cursor(part_id).await?;
        targets.insert(SubscriptionTarget::Part { part_id, cursor });
    }
    match big_sync_store
        .subscribe_local(SubPartsRequest {
            lower_bound: targets
                .iter()
                .filter_map(|target| match target {
                    SubscriptionTarget::Part { cursor, .. } => Some(*cursor),
                    SubscriptionTarget::Object { .. } => None,
                })
                .min()
                .unwrap_or_default(),
            targets,
        })
        .await?
    {
        Ok(listener) => Ok(Some(listener)),
        Err(err) => {
            eyre::bail!("failed subscribing to source partitions: {err:?}");
        }
    }
}

/// Multiplexes the three input streams (admission poll, part subscription,
/// authoritative parts changes) into machine events. All I/O stays here.
struct FrontierSource {
    store: SqliteBigRepoStore,
    kh_read_cursor: u64,
    /// Explicit mode: `All` considers every part event unfiltered; a
    /// selective scope filters by its group-derived part set.
    scope: WorkerGroupScope,
    part_listener: Option<big_sync_core::mpsc::Receiver<SubEvent>>,
    _local_registration: crate::changes::LocalListenerRegistration,
    local_listener: tokio::sync::mpsc::UnboundedReceiver<Vec<BigRepoLocalNotification>>,
    /// Round-robin flag: when true the admission poll runs before the part
    /// listener wait, otherwise the listener is polled first. Toggled every
    /// `next_batch` iteration so a continuously busy admission stream cannot
    /// starve the part listener (and vice versa).
    admission_first: bool,
}

impl FrontierSource {
    async fn next_batch(&mut self) -> Res<Vec<Evt>> {
        loop {
            // Round-robin which source is checked first so a continuously busy
            // admission stream cannot starve the part listener (and vice
            // versa). Both checks are non-blocking; when both are empty we
            // block on the part subscription for the idle window. The part
            // subscription replays from each part's durable cursor, so
            // nothing is lost while the other source is checked.
            let admission_first = self.admission_first;
            self.admission_first = !self.admission_first;

            if admission_first {
                if let Some(evt) = self.poll_local_listener_nowait()? {
                    return Ok(evt);
                }
                if let Some(rows) = self.poll_admission().await? {
                    return Ok(vec![Evt::KhRows(rows)]);
                }
                if let Some(evt) = self.poll_part_listener_nowait()? {
                    return Ok(evt);
                }
            } else {
                if let Some(evt) = self.poll_local_listener_nowait()? {
                    return Ok(evt);
                }
                if let Some(evt) = self.poll_part_listener_nowait()? {
                    return Ok(evt);
                }
                if let Some(rows) = self.poll_admission().await? {
                    return Ok(vec![Evt::KhRows(rows)]);
                }
            }

            // Both sources empty: block on the part subscription for the
            // idle window (or sleep when no parts are watched) before
            // re-checking.
            tokio::select! {
                biased;
                evt = Self::poll_part_listener_blocking(&mut self.part_listener) => {
                    if let Some(evt) = evt?
                        && let Some(evt) = self.part_event_to_evt(evt)?
                    {
                        return Ok(evt);
                    }
                }
                evt = self.local_listener.recv() => {
                    let Some(evt) = evt else { return Err(ferr!("AutomergeFrontierWorker local listener closed")); };
                    let evt = self.local_event_to_evt(evt)?;
                    if !evt.is_empty() { return Ok(evt); }
                }
            }
        }
    }

    /// Non-blocking poll of the keyhive admission log.
    async fn poll_admission(&mut self) -> Res<Option<Vec<driver::AdmittedRow>>> {
        let rows = self
            .store
            .admission_events_after(self.kh_read_cursor, EVENT_BATCH_SIZE)
            .await?;
        if rows.is_empty() {
            return Ok(None);
        }
        self.kh_read_cursor = rows
            .iter()
            .map(|row| row.seq)
            .max()
            .unwrap_or(self.kh_read_cursor);
        let rows = rows
            .into_iter()
            .map(|row| driver::AdmittedRow {
                seq: row.seq,
                bytes: row.bytes.into(),
            })
            .collect();
        Ok(Some(rows))
    }

    /// Non-blocking check of the part subscription.
    fn poll_part_listener_nowait(&mut self) -> Res<Option<Vec<Evt>>> {
        let Some(listener) = &mut self.part_listener else {
            return Ok(None);
        };
        match listener.try_recv() {
            Ok(part_event) => self.part_event_to_evt(part_event),
            Err(async_channel::TryRecvError::Closed) => {
                Err(ferr!("AutomergeFrontierWorker partition listener closed"))
            }
            Err(async_channel::TryRecvError::Empty) => Ok(None),
        }
    }

    /// Blocking wait on the part subscription (up to `IDLE_POLL`).
    async fn poll_part_listener_blocking(
        listener: &mut Option<big_sync_core::mpsc::Receiver<SubEvent>>,
    ) -> Res<Option<SubEvent>> {
        let wait = match listener {
            Some(listener) => tokio::time::timeout(IDLE_POLL, listener.recv()).await.ok(),
            None => {
                tokio::time::sleep(IDLE_POLL).await;
                None
            }
        };
        let Some(part_event) = wait else {
            // Idle window elapsed: loop re-checks parts changes and the
            // admission stream.
            return Ok(None);
        };
        let Ok(part_event) = part_event else {
            return Err(ferr!("AutomergeFrontierWorker partition listener closed"));
        };
        Ok(Some(part_event))
    }

    fn part_event_to_evt(&mut self, part_event: SubEvent) -> Res<Option<Vec<Evt>>> {
        match part_event {
            SubEvent::Added(inner) => {
                if !scope_includes_part(&self.scope, inner.part_id) {
                    return Ok(None);
                }
                Ok(Some(vec![Evt::PartAdded {
                    doc_id: crate::DocumentId::new(*inner.obj_id.as_bytes()),
                    part_id: inner.part_id,
                    cursor: inner.cursor,
                }]))
            }
            SubEvent::Changed(inner) => {
                // Explicit mode: `All` considers every part event unfiltered;
                // a selective scope filters by its group-derived part set.
                let has_part_ids = !inner.part_ids.is_empty();
                let part_ids: Vec<PartId> = match self.scope.groups() {
                    None => inner.part_ids,
                    Some(_) => inner
                        .part_ids
                        .into_iter()
                        .filter(|part_id| scope_includes_part(&self.scope, *part_id))
                        .collect(),
                };
                if has_part_ids && part_ids.is_empty() {
                    return Ok(None);
                }
                Ok(Some(vec![Evt::PartChanged {
                    doc_id: crate::DocumentId::new(*inner.obj_id.as_bytes()),
                    cursor: inner.cursor,
                    part_ids,
                }]))
            }
            SubEvent::Removed(inner) => {
                if !scope_includes_part(&self.scope, inner.part_id) {
                    return Ok(None);
                }
                Ok(Some(vec![Evt::PartRemoved {
                    doc_id: crate::DocumentId::new(*inner.obj_id.as_bytes()),
                    part_id: inner.part_id,
                    cursor: inner.cursor,
                }]))
            }
            SubEvent::ReplayComplete => Ok(None),
        }
    }

    fn local_event_to_evt(&self, notifications: Vec<BigRepoLocalNotification>) -> Res<Vec<Evt>> {
        Ok(notifications
            .into_iter()
            .filter_map(|notification| match notification {
                BigRepoLocalNotification::DocMaterializationReady { doc_id, .. }
                | BigRepoLocalNotification::DocHeadsUpdated { doc_id, .. } => {
                    Some(Evt::DocumentReady { doc_id })
                }
                BigRepoLocalNotification::DocCreated { .. }
                | BigRepoLocalNotification::DocImported { .. }
                | BigRepoLocalNotification::DocMaterializationPending { .. } => None,
            })
            .collect())
    }

    fn poll_local_listener_nowait(&mut self) -> Res<Option<Vec<Evt>>> {
        match self.local_listener.try_recv() {
            Ok(events) => {
                let events = self.local_event_to_evt(events)?;
                Ok((!events.is_empty()).then_some(events))
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                Err(ferr!("AutomergeFrontierWorker local listener closed"))
            }
        }
    }
}

#[async_trait::async_trait]
impl crate::runtime2::driver::EventSource for FrontierSource {
    type Evt = Evt;

    async fn next_batch(&mut self) -> Res<Vec<Evt>> {
        self.next_batch().await
    }
}

/// Executes serial source-part/cursor commands and owns keyed task effects.
struct FrontierExec {
    store: SqliteBigRepoStore,
    big_sync_store: Arc<dyn HostPartStore>,
    frontier_store: Arc<dyn HostPartStore>,
    runtime: crate::runtime2::Runtime2Handle<Sendable>,
    keyhive: crate::keyhive::BigKeyhiveHandle,
    scope: WorkerGroupScope,
}

#[async_trait::async_trait]
impl crate::runtime2::driver::CmdExecutor<AutomergeFrontierCore> for FrontierExec {
    async fn execute(
        &mut self,
        cmd: &Cmd,
    ) -> Res<crate::runtime2::driver::ExecOutcome<AutomergeFrontierCore>> {
        match *cmd {
            Cmd::PublishHeadsWatermarked { doc_id, cursor } => {
                let outcome = publish_heads(
                    doc_id,
                    &self.runtime,
                    &self.store,
                    &self.big_sync_store,
                    &self.frontier_store,
                    cursor,
                    None,
                    &self.scope,
                )
                .await?;
                Ok(crate::runtime2::driver::ExecOutcome::Done(Some(
                    match outcome {
                        PublishOutcome::Published => Evt::Published {
                            doc_id,
                            through: cursor,
                        },
                        PublishOutcome::Deferred => Evt::Deferred { doc_id },
                    },
                )))
            }
            Cmd::CommitPartCursor { part_id, cursor } => {
                self.store
                    .commit_automerge_part_cursor(part_id, cursor)
                    .await?;
                Ok(crate::runtime2::driver::ExecOutcome::Done(None))
            }
            Cmd::RemoveFrontierMembership {
                doc_id,
                part_id,
                cursor,
            } => {
                self.frontier_store
                    .remove_obj_from_part(automerge_doc_obj_id(doc_id), part_id)
                    .await?;
                self.store
                    .commit_automerge_part_cursor(part_id, cursor)
                    .await?;
                Ok(crate::runtime2::driver::ExecOutcome::Done(None))
            }
            Cmd::AdvanceKhCursor(watermark) => {
                self.store
                    .commit_automerge_keyhive_cursor(watermark)
                    .await?;
                Ok(crate::runtime2::driver::ExecOutcome::Done(Some(
                    Evt::CursorPersisted,
                )))
            }
        }
    }
}

impl driver::SeedRunner<AutomergeFrontierCore> for FrontierExec {
    fn spawn_seed(
        &mut self,
        task: SpawnedTask<FrontierSeed>,
        result_tx: &tokio::sync::mpsc::UnboundedSender<
            Result<(TaskId, FrontierTaskOutput), eyre::Report>,
        >,
        task_set: &utils_rs::AbortableJoinSet,
        live: &mut HashMap<TaskId, utils_rs::TaskHandle>,
    ) {
        let task_id = task.id;
        let seed = task.seed;
        let runtime = self.runtime.clone();
        let store = self.store.clone();
        let big_sync_store = Arc::clone(&self.big_sync_store);
        let frontier_store = Arc::clone(&self.frontier_store);
        let keyhive = self.keyhive.clone();
        let scope = self.scope.clone();
        let result_tx = result_tx.clone();
        let fut = async move {
            match seed {
                FrontierSeed::Decode { seq, bytes } => {
                    let event: StaticEvent<Vec<u8>> = bincode::deserialize(&bytes)
                        .expect("persisted keyhive admission event must decode");
                    let doc = match event {
                        StaticEvent::CgkaOperation(operation) => {
                            let doc =
                                crate::DocumentId::new(*operation.payload().doc_id().as_bytes());
                            // `All` considers every admission event with no group
                            // lookups; only a selective scope walks the keyhive
                            // graph to check the document's groups.
                            match scope.groups() {
                                None => Some(doc),
                                Some(_) => scope
                                    .admits_doc_groups(
                                        &keyhive.group_ids_containing_document(doc).await?,
                                    )
                                    .then_some(doc),
                            }
                        }
                        _ => None,
                    };
                    Ok(FrontierTaskOutput::Decoded { seq, doc })
                }
                FrontierSeed::Document { doc, covered } => {
                    let target_seq = covered.last().copied();
                    if let Some(target_seq) = target_seq {
                        runtime.apply_keyhive_to_doc(doc, target_seq).await?;
                    }
                    let outcome = publish_heads(
                        doc,
                        &runtime,
                        &store,
                        &big_sync_store,
                        &frontier_store,
                        None,
                        target_seq,
                        &scope,
                    )
                    .await?;
                    if matches!(outcome, PublishOutcome::Deferred) {
                        return Ok(FrontierTaskOutput::Deferred { doc });
                    }
                    Ok(FrontierTaskOutput::Settled {
                        key: FrontierKey::Document(doc),
                        covered,
                    })
                }
            }
        };
        let handle = task_set
            .spawn(async move {
                let result = fut.await;
                drop(result_tx.send(result.map(|output| (task_id, output))));
            })
            .expect("driver task set must accept work while the driver runs");
        live.insert(task_id, handle);
    }
    fn task_completed(&mut self, _task: TaskId) -> Option<TaskId> {
        None
    }
}

#[async_trait::async_trait]
impl driver::DriverHooks<AutomergeFrontierCore> for FrontierExec {
    async fn on_task_completed(
        &mut self,
        machine: &mut AutomergeFrontierCore,
        _task: TaskId,
        output: FrontierTaskOutput,
    ) -> Res<()> {
        match output {
            FrontierTaskOutput::Decoded { seq, doc } => machine.on_evt(Evt::Decoded { seq, doc }),
            FrontierTaskOutput::Settled { key, covered } => {
                machine.on_evt(Evt::TaskSettled { key, covered })
            }
            FrontierTaskOutput::Deferred { doc } => {
                machine.queued_docs.remove(&doc);
            }
        }
        Ok(())
    }

    async fn pump(&mut self, machine: &mut AutomergeFrontierCore) -> Res<()> {
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

async fn run_automerge_frontier_tail(
    store: SqliteBigRepoStore,
    big_sync_store: Arc<dyn HostPartStore>,
    frontier_store: Arc<dyn HostPartStore>,
    runtime: crate::runtime2::Runtime2Handle<Sendable>,
    keyhive: crate::keyhive::BigKeyhiveHandle,
    scope: WorkerGroupScope,
    change_manager: Arc<crate::changes::ChangeListenerManager>,
) -> Res<()> {
    let initial_source_parts: HashSet<PartId> = match scope.groups() {
        // `All` watches every part currently in the store (never a keyhive
        // enumeration, so parts for groups not yet in the hive are watched
        // too); a selective scope uses its explicit group set directly.
        None => store.list_parts().await?,
        Some(groups) if groups.is_empty() => HashSet::new(),
        Some(groups) => groups
            .iter()
            .copied()
            .chain(std::iter::once(crate::GLOBAL_PART_ID))
            .collect(),
    };
    let kh_read_cursor = store.automerge_keyhive_cursor().await?;
    store
        .register_keyhive_admission_reader(
            crate::store::sqlite::KEYHIVE_ADMISSION_READER_AUTOMERGE_FRONTIER,
            kh_read_cursor,
        )
        .await?;
    let (local_registration, local_listener) = change_manager
        .subscribe_local_listener(LocalFilter { doc_id: None })
        .await?;
    let source = FrontierSource {
        kh_read_cursor,
        scope: scope.clone(),
        part_listener: subscribe_to_parts(&big_sync_store, &store, &initial_source_parts).await?,
        store: store.clone(),
        _local_registration: local_registration,
        local_listener,
        admission_first: true,
    };
    let mut exec = FrontierExec {
        store,
        big_sync_store,
        frontier_store,
        runtime: runtime.clone(),
        keyhive,
        scope,
    };
    crate::runtime2::driver::run_stream_driver(
        AutomergeFrontierCore::default(),
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

    fn admit(core: &mut AutomergeFrontierCore, seq: u64) {
        assert!(core.machine.admit((), seq));
        core.machine
            .track((), seq, seq, [FrontierKey::Decode(seq)], ());
    }

    #[test]
    fn document_publications_run_independently_but_cursor_waits_for_prefix() {
        let mut core = AutomergeFrontierCore::default();
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
            key: FrontierKey::Document(doc(9)),
            covered: [2].into_iter().collect(),
        });
        assert!(core.outbox.is_empty());
        core.on_evt(Evt::TaskSettled {
            key: FrontierKey::Document(doc(7)),
            covered: [1].into_iter().collect(),
        });
        assert!(matches!(
            core.outbox.front().map(|(_, cmd)| cmd),
            Some(Cmd::AdvanceKhCursor(2))
        ));
    }

    #[test]
    fn admission_without_document_settles_decode_lane() {
        let mut core = AutomergeFrontierCore::default();
        admit(&mut core, 1);
        core.on_evt(Evt::Decoded { seq: 1, doc: None });
        assert!(matches!(
            core.outbox.front().map(|(_, cmd)| cmd),
            Some(Cmd::AdvanceKhCursor(1))
        ));
    }

    #[test]
    fn part_cursor_waits_for_publication_and_wakes_once() {
        let mut core = AutomergeFrontierCore::default();
        let document = doc(11);
        let part = PartId::new([3; 32]);
        let later_part = PartId::new([4; 32]);
        core.on_evt(Evt::PartAdded {
            doc_id: document,
            part_id: part,
            cursor: 4,
        });
        core.on_evt(Evt::DocumentReady { doc_id: document });
        core.on_evt(Evt::PartAdded {
            doc_id: document,
            part_id: later_part,
            cursor: 7,
        });
        assert_eq!(core.outbox.iter().count(), 1);
        assert!(matches!(
            core.outbox.front().map(|(_, cmd)| cmd),
            Some(Cmd::PublishHeadsWatermarked {
                cursor: Some(4),
                ..
            })
        ));
        assert!(core.pending_parts[&document][&part] == 4);
        assert!(core.pending_parts[&document][&later_part] == 7);
        core.on_evt(Evt::Published {
            doc_id: document,
            through: Some(4),
        });
        assert!(!core.pending_parts[&document].contains_key(&part));
        assert!(core.pending_parts[&document][&later_part] == 7);
        assert!(core.outbox.iter().any(|(_, cmd, _)| matches!(cmd,
            Cmd::CommitPartCursor { part_id, cursor: 4 } if *part_id == part)));
        core.on_evt(Evt::Published {
            doc_id: document,
            through: Some(7),
        });
        assert!(core.outbox.iter().any(|(_, cmd, _)| matches!(cmd,
            Cmd::CommitPartCursor { part_id, cursor: 7 } if *part_id == later_part)));
    }

    #[test]
    fn duplicate_part_events_merge_by_max_cursor() {
        let mut core = AutomergeFrontierCore::default();
        let document = doc(12);
        let part = PartId::new([4; 32]);
        core.on_evt(Evt::PartAdded {
            doc_id: document,
            part_id: part,
            cursor: 2,
        });
        core.on_evt(Evt::PartAdded {
            doc_id: document,
            part_id: part,
            cursor: 9,
        });
        assert_eq!(core.outbox.iter().count(), 1);
        assert_eq!(core.pending_parts[&document][&part], 9);
    }

    #[test]
    fn scoped_frontier_mirroring_always_keeps_global_membership() {
        let group = PartId::new([8; 32]);
        let scope = WorkerGroupScope::Groups([group].into_iter().collect());
        assert!(scope_includes_part(&scope, crate::GLOBAL_PART_ID));
        assert!(scope_includes_part(&scope, group));
        assert!(!scope_includes_part(&scope, PartId::new([9; 32])));
    }

    #[test]
    fn removing_one_part_does_not_discard_other_pending_memberships() {
        let mut core = AutomergeFrontierCore::default();
        let document = doc(13);
        let retained = PartId::new([10; 32]);
        let removed = PartId::new([11; 32]);
        core.on_evt(Evt::PartAdded {
            doc_id: document,
            part_id: retained,
            cursor: 3,
        });
        core.on_evt(Evt::PartAdded {
            doc_id: document,
            part_id: removed,
            cursor: 3,
        });
        core.on_evt(Evt::PartRemoved {
            doc_id: document,
            part_id: removed,
            cursor: 4,
        });
        assert_eq!(core.pending_parts[&document][&retained], 3);
        assert!(!core.pending_parts[&document].contains_key(&removed));
        assert!(core.outbox.iter().any(|(_, cmd, _)| matches!(
            cmd,
            Cmd::RemoveFrontierMembership { doc_id, part_id, cursor }
                if *doc_id == document && *part_id == removed && *cursor == 4
        )));
    }
}
