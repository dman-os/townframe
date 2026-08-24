//! Crash-recoverable maintenance for Keyhive-derived policy and partitions.
//!
//! Full-pipeline pilot for the extracted sans-io machines:
//!
//! ```text
//! admission_events_after (incorporation-gated durable log)
//!   └─> GroupPartCore (pure reducer, [`StreamMachine`])
//!         ├─ WatermarkMachine: admission guard + contiguous-prefix settlement
//!         │   of the keyhive incorporation stream
//!         ├─ KeyedScheduler<GroupPartKey, GroupPartSeed>: decode/document/group
//!         │   by the driver and executed concurrently with a spawn limit
//!         └─ Outbox: serial cmds (cursor advance tx, settled-watermark ack)
//!               └─> runtime2::driver executes front-peek/complete(id)
//! ```
//!
//! The loop itself lives in [`crate::runtime2::driver::run_stream_driver`];
//! this file supplies the reducer, the source, the executors and the hooks.
//!
//! ## Incremental reconciliation (no full sweeps)
//!
//! Keyhive event rows NAME their affected documents (a `CgkaOperation`
//! payload carries its `doc_id`; delegations/revocations carry the
//! credential subjects). Reconciliation therefore touches ONLY the docs
//! named by admitted rows — never a pass over all known documents. A full
//! projection build happens exactly twice: on a fresh store (no durable
//! cursor yet) and on explicit Keyhive state-generation bumps. Rows whose
//! event carries no resolvable document (e.g. prekey rotations) still settle
//! their watermark immediately — they are hints, and every reconciliation
//! queries CURRENT Keyhive state per affected doc anyway (idempotent,
//! replay-safe). Cursors persist only via [`Cmd::AdvanceCursor`] txns.

use crate::interlude::*;
use crate::keyhive::BigKeyhiveHandle;
use crate::runtime2::{WorkerGroupScope, driver};
use crate::store::sqlite::{GroupPartReconciliation, SqliteBigRepoStore};
use big_sync_core::outbox::Outbox;
use big_sync_core::scheduler::{KeyedScheduler, SpawnedTask, TaskId};
use big_sync_core::watermark::WatermarkMachine;
use big_sync_core::{ObjId, PartId, PeerId};
use keyhive_core::event::static_event::StaticEvent;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

const EVENT_BATCH_SIZE: u32 = 64;
/// Concurrent per-document tasks during the initial full projection build.
const INITIAL_BUILD_CONCURRENCY: usize = 16;
const IDLE_POLL: std::time::Duration = std::time::Duration::from_millis(25);

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
    timer: Arc<dyn crate::runtime2::Timer<future_form::Sendable>>,
    evt_tx: async_channel::Sender<crate::runtime2::Runtime2Evt>,
    scope: WorkerGroupScope,
) -> SpawnedGroupPartWorker<future_form::Sendable> {
    let (abort_handle, abort_registration) = futures::future::AbortHandle::new_pair();

    let mut driver = GroupPartDriver {
        store,
        keyhive,
        local_peer_id,
        timer,
        evt_tx,
        scope,
        completed: Arc::new(std::sync::Mutex::new(HashMap::new())),
    };

    let fut = async move {
        let cursor = driver.store.keyhive_group_part_cursor().await?;

        // Fresh store: no durable cursor means the projection was never
        // built. One initial full build from current Keyhive state, then
        // incremental reconciliation over the admission stream takes over.
        if cursor == 0 {
            tracing::debug!("group-part worker fresh store: building initial projection");
            driver.build_initial_projection().await?;
        }

        let core = GroupPartCore::new();
        let source = RowSource(driver::AdmissionSource {
            store: driver.store.clone(),
            timer: driver.timer.clone(),
            read_cursor: cursor,
            batch_size: EVENT_BATCH_SIZE,
            idle_poll: IDLE_POLL,
        });
        crate::runtime2::driver::run_stream_driver(
            core,
            source,
            &mut driver,
            IDLE_POLL,
            std::future::pending(),
        )
        .await
    };

    let run = future_form::Sendable::from_future(async move {
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

/// The driver-side halves of the worker: executors, runner and hooks. Holds
/// no reducer state — the core travels separately through
/// [`crate::runtime2::driver::run_stream_driver`].
struct GroupPartDriver {
    store: SqliteBigRepoStore,
    keyhive: BigKeyhiveHandle,
    local_peer_id: PeerId,
    timer: Arc<dyn crate::runtime2::Timer<future_form::Sendable>>,
    evt_tx: async_channel::Sender<crate::runtime2::Runtime2Evt>,
    scope: WorkerGroupScope,
    completed: Arc<std::sync::Mutex<HashMap<TaskId, GroupPartTaskOutput>>>,
}

impl GroupPartDriver {
    /// One-time full projection build for a fresh store (durable cursor ==
    /// 0): re-derive the projection from CURRENT Keyhive state over ALL known
    /// documents. Per-document reconciliation runs concurrently with a bounded
    /// initial-build concurrency; the final empty-batch transaction commits the
    /// durable cursor so incremental reconciliation takes over from there. There
    /// are no periodic rebuilds: every later
    async fn build_initial_projection(&mut self) -> Res<()> {
        tracing::debug!("group-part worker building initial full projection");
        // ensure all currently visible group parts exist
        // before rebuilding documents. Incremental reconciliation supplies only
        // event-affected group parts instead of repeating this scan.
        //
        // A part must exist before a pending want can be advertised:
        // `summarize_parts` needs the part row even before a document payload
        // arrives, so an empty group is still handled during the initial build.
        let group_ids = self.keyhive.visible_group_ids().await;
        let initial_group_parts: HashSet<PartId> = group_ids
            .iter()
            .map(|group_id| group_id.to_bytes())
            .filter(|id| self.scope.admits_group(id))
            .map(group_part_id)
            .collect();
        for part_id in &initial_group_parts {
            self.store.ensure_part(*part_id).await?;
        }
        let initial_group_parts = Arc::new(initial_group_parts);
        const CURSOR: u64 = 0;
        let docs = self.keyhive.document_ids().await;
        let futs = docs.into_iter().map(|doc| {
            let store = self.store.clone();
            let keyhive = self.keyhive.clone();
            let initial_group_parts = initial_group_parts.clone();
            let scope = self.scope.clone();
            let local_principal = self.local_peer_id;
            async move {
                if !scope.admits_doc_groups(
                    &keyhive
                        .group_ids_containing_document(crate::DocumentId::new(doc.into_bytes()))
                        .await,
                ) {
                    return Ok(());
                }
                let reconciliation =
                    reconcile_doc(&keyhive, doc, &initial_group_parts, &scope, local_principal)
                        .await?;
                store
                    .reconcile_group_part_batch(&[reconciliation], CURSOR, false)
                    .await
            }
        });
        drive_buffered(futs, INITIAL_BUILD_CONCURRENCY).await?;
        self.store
            .reconcile_group_part_batch(&[], CURSOR, true)
            .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl crate::runtime2::driver::CmdExecutor<GroupPartCore> for GroupPartDriver {
    async fn execute(
        &mut self,
        cmd: &Cmd,
    ) -> Res<crate::runtime2::driver::ExecOutcome<GroupPartCore>> {
        use crate::runtime2::driver::ExecOutcome;
        match *cmd {
            Cmd::AdvanceCursor(watermark) => {
                self.store
                    .reconcile_group_part_batch(&[], watermark, true)
                    .await?;
                Ok(ExecOutcome::Done(Some(Evt::CursorPersisted(watermark))))
            }
            Cmd::AnnounceSettled(seq) => {
                if self
                    .evt_tx
                    .send(crate::runtime2::Runtime2Evt::GroupPartWorkerSettled { seq })
                    .await
                    .is_err()
                {
                    return Ok(ExecOutcome::Shutdown);
                }
                Ok(ExecOutcome::Done(Some(Evt::SettledAnnounced)))
            }
        }
    }
}

impl crate::runtime2::driver::SeedRunner<GroupPartCore> for GroupPartDriver {
    fn spawn_seed(
        &mut self,
        task: SpawnedTask<GroupPartSeed>,
        result_tx: &tokio::sync::mpsc::UnboundedSender<Result<TaskId, eyre::Report>>,
        live: &mut HashMap<TaskId, tokio::task::JoinHandle<()>>,
    ) {
        let task_id = task.id;
        let seed = task.seed;
        let store = self.store.clone();
        let keyhive = self.keyhive.clone();
        let scope = self.scope.clone();
        let completed = self.completed.clone();
        let result_tx = result_tx.clone();
        let fut = async move {
            match seed {
                GroupPartSeed::Decode { seq, bytes } => {
                    let affected = affected_event(&keyhive, &bytes, &scope).await?;
                    Ok(GroupPartTaskOutput::Decoded { seq, affected })
                }
                GroupPartSeed::Document {
                    doc,
                    covered,
                    affected_group_parts,
                    local_principal,
                } => {
                    let reconciliation = reconcile_doc(
                        &keyhive,
                        doc,
                        &affected_group_parts,
                        &scope,
                        local_principal,
                    )
                    .await?;
                    store
                        .reconcile_group_part_batch(&[reconciliation], 0, false)
                        .await?;
                    Ok(GroupPartTaskOutput::Settled {
                        key: GroupPartKey::Document(doc),
                        covered,
                    })
                }
                GroupPartSeed::Group { part, covered } => {
                    store.ensure_part(part).await?;
                    Ok(GroupPartTaskOutput::Settled {
                        key: GroupPartKey::Group(part),
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
                    .expect("group-part completion map poisoned")
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
            .expect("group-part completion map poisoned")
            .remove(&task);
    }
}

#[async_trait::async_trait]
impl crate::runtime2::driver::DriverHooks<GroupPartCore> for GroupPartDriver {
    async fn on_task_completed(&mut self, machine: &mut GroupPartCore, task: TaskId) -> Res<()> {
        let output = self
            .completed
            .lock()
            .expect("group-part completion map poisoned")
            .remove(&task)
            .expect("successful task has no completion output");
        match output {
            GroupPartTaskOutput::Decoded { seq, affected } => {
                machine.on_evt(Evt::Decoded {
                    seq,
                    affected,
                    local_principal: self.local_peer_id,
                });
            }
            GroupPartTaskOutput::Settled { key, covered } => {
                machine.on_evt(Evt::TaskSettled { key, covered });
            }
        }
        Ok(())
    }
    async fn pump(&mut self, machine: &mut GroupPartCore) -> Res<()> {
        for row in machine.take_pending_rows() {
            machine.schedule_decode(Instant::now(), row);
        }
        let now = Instant::now();
        for seed in machine.take_ready_seeds() {
            machine.schedule_seed(now, seed);
        }
        Ok(())
    }
    async fn on_idle(&mut self, machine: &mut GroupPartCore) -> Res<()> {
        machine.on_evt(Evt::Idle);
        Ok(())
    }
}

/// Maps raw admitted-row batches from the shared
/// [`driver::AdmissionSource`] into the core's event vocabulary.
struct RowSource(driver::AdmissionSource);

#[async_trait::async_trait]
impl crate::runtime2::driver::EventSource for RowSource {
    type Evt = Evt;

    async fn next_batch(&mut self) -> Res<Vec<Evt>> {
        Ok(self
            .0
            .next_batch()
            .await?
            .into_iter()
            .map(Evt::Rows)
            .collect())
    }
}

/// This worker has one logical lane; the unit type is sufficient.

/// An admitted-but-unsettled keyhive log row is [`driver::AdmittedRow`]: its
/// seq plus the raw event bytes the driver resolves to affected documents.

/// Keys used by the keyed task graph. Decode work is keyed by admission
/// sequence; derived document and group work coalesces by its domain key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum GroupPartKey {
    Decode(u64),
    Document(ObjId),
    Group(PartId),
}

#[derive(Debug, Clone)]
enum GroupPartSeed {
    Decode {
        seq: u64,
        bytes: Arc<[u8]>,
    },
    Document {
        doc: ObjId,
        covered: BTreeSet<u64>,
        affected_group_parts: HashSet<PartId>,
        local_principal: PeerId,
    },
    Group {
        part: PartId,
        covered: BTreeSet<u64>,
    },
}

impl GroupPartSeed {
    fn key(&self) -> GroupPartKey {
        match self {
            Self::Decode { seq, .. } => GroupPartKey::Decode(*seq),
            Self::Document { doc, .. } => GroupPartKey::Document(*doc),
            Self::Group { part, .. } => GroupPartKey::Group(*part),
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
                Self::Document {
                    doc,
                    mut covered,
                    mut affected_group_parts,
                    local_principal: _,
                },
                Self::Document {
                    doc: new_doc,
                    covered: new_covered,
                    affected_group_parts: new_parts,
                    local_principal: new_principal,
                },
            ) => {
                assert_eq!(doc, new_doc, "document key changed during replacement");
                covered.extend(new_covered);
                affected_group_parts.extend(new_parts);
                Self::Document {
                    doc,
                    covered,
                    affected_group_parts,
                    local_principal: new_principal,
                }
            }
            (
                Self::Group { part, mut covered },
                Self::Group {
                    part: new_part,
                    covered: new_covered,
                },
            ) => {
                assert_eq!(part, new_part, "group key changed during replacement");
                covered.extend(new_covered);
                Self::Group { part, covered }
            }
            (old, new) => panic!("keyed scheduler seed variant disagrees: {old:?} vs {new:?}"),
        }
    }
}

#[derive(Debug, Clone)]
enum GroupPartTaskOutput {
    Decoded {
        seq: u64,
        affected: AffectedEvent,
    },
    Settled {
        key: GroupPartKey,
        covered: BTreeSet<u64>,
    },
}

/// Serial side-effect commands executed through the [`Outbox`].
///
/// Per-document work does NOT travel here — it flows through the scheduler
/// spawn queue so the driver can run it concurrently. The outbox is strictly
/// for effects whose order matters.
#[derive(Debug)]
enum Cmd {
    /// Commit the durable event cursor at `watermark` via an empty-mutation
    /// reconciliation transaction (the store commits the cursor inside the tx).
    AdvanceCursor(u64),
    /// Publish the settled admission watermark on the runtime evt channel.
    AnnounceSettled(u64),
}

/// Events fed back into the core by the driver.
#[derive(Debug)]
enum Evt {
    Rows(Vec<driver::AdmittedRow>),
    Decoded {
        seq: u64,
        affected: AffectedEvent,
        local_principal: PeerId,
    },
    TaskSettled {
        key: GroupPartKey,
        covered: BTreeSet<u64>,
    },
    CursorPersisted(u64),
    SettledAnnounced,
    Idle,
}

#[derive(Debug)]
struct GroupPartCore {
    machine: WatermarkMachine<(), u64, GroupPartKey, (), u64>,
    outbox: Outbox<Cmd, ()>,
    scheduler: KeyedScheduler<GroupPartKey, GroupPartSeed>,
    pending_rows: BTreeMap<u64, driver::AdmittedRow>,
    ready_seeds: Vec<GroupPartSeed>,
    last_settled: u64,
    last_announced_settled: u64,
    announced_idle: bool,
}

impl GroupPartCore {
    fn new() -> Self {
        Self {
            machine: Default::default(),
            outbox: Default::default(),
            scheduler: Default::default(),
            pending_rows: BTreeMap::new(),
            ready_seeds: Vec::new(),
            last_settled: 0,
            last_announced_settled: 0,
            announced_idle: false,
        }
    }

    fn on_evt(&mut self, evt: Evt) {
        match evt {
            Evt::Rows(rows) => self.on_rows(rows),
            Evt::Decoded {
                seq,
                affected,
                local_principal,
            } => self.on_decoded(seq, affected, local_principal),
            Evt::TaskSettled { key, covered } => self.on_task_settled(key, covered),
            Evt::CursorPersisted(watermark) => self.on_cursor_persisted(watermark),
            Evt::SettledAnnounced => {}
            Evt::Idle => self.on_idle(),
        }
    }

    fn on_rows(&mut self, rows: Vec<driver::AdmittedRow>) {
        self.announced_idle = false;
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
            .track((), seq, seq, [GroupPartKey::Decode(seq)], ());
        self.scheduler.replace(
            now,
            GroupPartKey::Decode(seq),
            GroupPartSeed::Decode {
                seq,
                bytes: row.bytes,
            },
        );
    }

    fn schedule_seed(&mut self, now: Instant, seed: GroupPartSeed) {
        let key = seed.key();
        self.scheduler
            .replace_with(now, key, seed, GroupPartSeed::merge);
    }

    fn take_ready_seeds(&mut self) -> Vec<GroupPartSeed> {
        std::mem::take(&mut self.ready_seeds)
    }

    fn on_decoded(&mut self, seq: u64, affected: AffectedEvent, local_principal: PeerId) {
        let docs: BTreeSet<_> = affected.docs.into_iter().collect();
        let groups = affected.group_parts;
        for doc in docs {
            let key = GroupPartKey::Document(doc);
            self.machine.track((), seq, seq, [key], ());
            self.ready_seeds.push(GroupPartSeed::Document {
                doc,
                covered: [seq].into_iter().collect(),
                affected_group_parts: groups.clone(),
                local_principal,
            });
        }
        for part in groups {
            let key = GroupPartKey::Group(part);
            self.machine.track((), seq, seq, [key], ());
            self.ready_seeds.push(GroupPartSeed::Group {
                part,
                covered: [seq].into_iter().collect(),
            });
        }
        self.settle_lane(seq, GroupPartKey::Decode(seq));
    }

    fn on_task_settled(&mut self, key: GroupPartKey, covered: BTreeSet<u64>) {
        for seq in covered {
            self.settle_lane(seq, key);
        }
    }

    fn settle_lane(&mut self, seq: u64, lane: GroupPartKey) {
        for (_, reached) in self.machine.settle(seq, seq, lane) {
            if let Some(watermark) = reached {
                self.outbox.push(Cmd::AdvanceCursor(watermark), ());
            }
        }
    }

    fn on_cursor_persisted(&mut self, watermark: u64) {
        self.last_settled = self.last_settled.max(watermark);
        self.announced_idle = false;
    }

    fn on_idle(&mut self) {
        if !self.announced_idle || self.last_settled > self.last_announced_settled {
            self.last_announced_settled = self.last_settled;
            self.outbox
                .push(Cmd::AnnounceSettled(self.last_announced_settled), ());
            self.announced_idle = true;
        }
    }

    fn has_outstanding_work(&self) -> bool {
        !self.pending_rows.is_empty()
            || !self.ready_seeds.is_empty()
            || !self.machine.is_settled(&())
            || !self.outbox.is_empty()
    }
}

impl crate::runtime2::driver::StreamMachine for GroupPartCore {
    type Evt = Evt;
    type Cmd = Cmd;
    type Seed = GroupPartSeed;

    fn on_evt(&mut self, evt: Evt) {
        GroupPartCore::on_evt(self, evt);
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
        unreachable!("group-part tasks complete through keyed task results")
    }

    fn drain_spawn_queue(&mut self) -> Vec<SpawnedTask<GroupPartSeed>> {
        self.scheduler.drain_spawn_queue().collect()
    }

    fn tick_scheduler(&mut self, now: Instant) {
        self.scheduler.tick(now);
    }

    fn is_idle(&mut self) -> bool {
        !self.has_outstanding_work()
    }
}

/// Run futures with bounded concurrency, collecting all results. Errors are
/// fatal (first error aborts the rest), matching the original try_join_all
/// semantics but bounded.
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
    let identifier = keyhive_core::principal::identifier::Identifier::from(verifying_key);
    let agents = keyhive
        .agents_for_membered(identifier)
        .await
        .into_iter()
        .map(|(principal, access)| (PeerId::new(principal), access))
        .collect::<HashMap<_, _>>();
    let desired_group_parts: HashSet<PartId> = keyhive
        .group_ids_containing_document(crate::DocumentId::new(doc.into_bytes()))
        .await
        .into_iter()
        .filter(|id| scope.admits_group(id))
        .map(group_part_id)
        .collect();
    let mut reconciled_group_parts = affected_group_parts.clone();
    reconciled_group_parts.extend(desired_group_parts.iter().copied());
    let desired_global = agents
        .get(&local_principal)
        .is_some_and(|access| access.is_reader());
    tracing::debug!(
        ?doc,
        agent_count = agents.len(),
        local_access = ?agents.get(&local_principal),
        desired_global,
        desired_group_part_count = desired_group_parts.len(),
        "group-part worker computed document reconciliation"
    );
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

/// Resolve the documents and group part(s) named by a persisted Keyhive event.
/// Group-only events still produce a part-creation action.
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
        if scope.admits_doc_groups(
            &keyhive
                .group_ids_containing_document(crate::DocumentId::new(doc.into_bytes()))
                .await,
        ) {
            docs.push(doc);
        }
    }
    Ok(AffectedEvent {
        docs,
        group_parts: group_parts
            .into_iter()
            .filter(|part| match scope {
                WorkerGroupScope::All => true,
                WorkerGroupScope::Groups(groups) => {
                    groups.iter().any(|id| group_part_id(*id) == *part)
                }
            })
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_part_id_uses_sedimentree_namespace() {
        let actual = group_part_id([0; 32]);
        assert_eq!(
            actual.to_string(),
            "B1TtXt35pLe8AyPkUKgPLgbpFHckKjK3CHCQEytRFaLj"
        );
    }
    fn doc(n: u64) -> ObjId {
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&n.to_be_bytes());
        ObjId::new(bytes)
    }

    fn row(seq: u64) -> driver::AdmittedRow {
        driver::AdmittedRow {
            seq,
            bytes: Arc::from(vec![0u8].into_boxed_slice()),
        }
    }

    fn advance_cmds(core: &mut GroupPartCore) -> Vec<u64> {
        let mut advances = Vec::new();
        while let Some((pending, cmd)) = core.outbox.front() {
            let id = pending.id();
            if let Cmd::AdvanceCursor(watermark) = *cmd {
                advances.push(watermark);
            }
            drop(core.outbox.complete(id));
        }
        advances
    }

    #[test]
    fn duplicate_rows_are_dropped_by_admission() {
        let mut core = GroupPartCore::new();
        core.on_rows(vec![row(5)]);
        core.on_rows(vec![row(5)]);
        assert_eq!(core.pending_rows.len(), 1);
    }

    #[test]
    fn decoded_event_creates_keyed_work_and_waits_for_success() {
        let mut core = GroupPartCore::new();
        core.on_rows(vec![row(1)]);
        let pending = core.take_pending_rows();
        core.schedule_decode(Instant::now(), pending.into_iter().next().unwrap());
        let decode = core.scheduler.drain_spawn_queue().next().unwrap().id;
        assert!(crate::runtime2::driver::StreamMachine::complete_job(
            &mut core, decode
        ));
        core.on_evt(Evt::Decoded {
            seq: 1,
            affected: AffectedEvent {
                docs: vec![doc(1)],
                group_parts: HashSet::new(),
            },
            local_principal: PeerId::new([0; 32]),
        });
        let seed = core.take_ready_seeds().pop().unwrap();
        core.schedule_seed(Instant::now(), seed);
        let task = core.scheduler.drain_spawn_queue().next().unwrap().id;
        assert!(advance_cmds(&mut core).is_empty());
        assert!(crate::runtime2::driver::StreamMachine::complete_job(
            &mut core, task
        ));
        core.on_evt(Evt::TaskSettled {
            key: GroupPartKey::Document(doc(1)),
            covered: [1].into_iter().collect(),
        });
        assert_eq!(advance_cmds(&mut core), vec![1]);
    }

    #[test]
    fn replacing_document_work_merges_coverage() {
        let mut core = GroupPartCore::new();
        core.schedule_seed(
            Instant::now(),
            GroupPartSeed::Document {
                doc: doc(1),
                covered: [1].into_iter().collect(),
                affected_group_parts: HashSet::new(),
                local_principal: PeerId::new([0; 32]),
            },
        );
        let old = core.scheduler.drain_spawn_queue().next().unwrap().id;
        core.schedule_seed(
            Instant::now(),
            GroupPartSeed::Document {
                doc: doc(1),
                covered: [2].into_iter().collect(),
                affected_group_parts: HashSet::new(),
                local_principal: PeerId::new([0; 32]),
            },
        );
        assert_eq!(
            core.scheduler.drain_stop_queue().collect::<Vec<_>>(),
            vec![old]
        );
        let replacement = core.scheduler.drain_spawn_queue().next().unwrap();
        let GroupPartSeed::Document { covered, .. } = replacement.seed else {
            panic!("expected document replacement");
        };
        assert_eq!(covered, [1, 2].into_iter().collect());
    }
    #[test]
    fn idle_announces_once_then_acks_newer_settlements_only() {
        let mut core = GroupPartCore::new();
        core.on_evt(Evt::CursorPersisted(7));
        core.on_evt(Evt::Idle);
        assert_eq!(core.last_announced_settled, 7);
        core.on_evt(Evt::Idle);
        // Same settled watermark at idle: no duplicate announcement.
        assert_eq!(advance_cmds(&mut core), Vec::<u64>::new());
        core.on_evt(Evt::CursorPersisted(9));
        core.on_evt(Evt::Idle);
        assert_eq!(core.last_announced_settled, 9);
    }

    #[tokio::test]
    async fn buffered_batches_run_bounded_and_fail_loudly() {
        static LIVE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        static MAX_LIVE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        const LIMIT: usize = 2;
        let futs = (0..6).map(|i| async move {
            let live = LIVE.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            MAX_LIVE.fetch_max(live, std::sync::atomic::Ordering::SeqCst);
            LIVE.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            if i == 4 {
                Err(ferr!("batch failed"))
            } else {
                Ok(i)
            }
        });
        let out = drive_buffered(futs, LIMIT).await;
        assert!(out.is_err(), "first error surfaces loudly");
        assert!(
            MAX_LIVE.load(std::sync::atomic::Ordering::SeqCst) <= LIMIT,
            "concurrency stayed bounded"
        );

        let ok_futs = (0..4).map(|i| async move { Ok(i) });
        assert_eq!(
            drive_buffered(ok_futs, 2).await.unwrap(),
            vec![0, 1, 2, 3],
            "all results collected when everything succeeds"
        );
    }
}
