//! Crash-recoverable maintenance for Keyhive-derived policy and partitions.
//!
//! Full-pipeline pilot for the extracted sans-io machines:
//!
//! ```text
//! admission_events_after (incorporation-gated durable log)
//!   └─> GroupPartCore (pure reducer, [`StreamMachine`])
//!         ├─ WatermarkMachine: admission guard + contiguous-prefix settlement
//!         │   of the keyhive incorporation stream
//!         ├─ Scheduler<BatchSeed>: AFFECTED-DOC reconciliation jobs, drained
//!         │   by the driver and executed CONCURRENTLY as tokio tasks
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
use crate::runtime2::driver;
use crate::sqlite_big_repo_store::{GroupPartReconciliation, SqliteBigRepoStore};
use big_sync_core::outbox::Outbox;
use big_sync_core::scheduler::{Scheduler, SpawnedTask, TaskId};
use big_sync_core::watermark::WatermarkMachine;
use big_sync_core::{ObjId, PartId, PeerId};
use keyhive_core::event::static_event::StaticEvent;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

const EVENT_BATCH_SIZE: u32 = 64;
const DOC_BATCH_SIZE: usize = 64;
/// Concurrent doc-batch tasks during the initial full projection build.
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
) -> SpawnedGroupPartWorker<future_form::Sendable> {
    let (abort_handle, abort_registration) = futures::future::AbortHandle::new_pair();

    let mut driver = GroupPartDriver {
        store,
        keyhive,
        local_peer_id,
        timer,
        evt_tx,
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

/// The keyhive event log as a watermark stream.
///
/// The log has one global seq space and one durable cursor
/// (`keyhive_group_part_cursor`), so there is exactly one stream today. True
/// multi-stream adoption (per group part) needs per-part durable cursors in
/// the store schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct EventLog;

/// Maps raw admitted-row batches from the shared
/// [`driver::AdmissionSource`] into the core's event vocabulary.
struct RowSource(driver::AdmissionSource);

impl crate::runtime2::driver::EventSource for RowSource {
    type Evt = Evt;

    async fn next_batch(&mut self) -> Res<Vec<Evt>> {
        Ok(self.0.next_batch().await?.into_iter().map(Evt::Rows).collect())
    }
}

/// The single lane a reconciliation waiter waits on. Exists so the generic
/// machines keep their lane dimension without pretending this site has
/// several.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Lane;

/// An admitted-but-unsettled keyhive log row is [`driver::AdmittedRow`]: its
/// seq plus the raw event bytes the driver resolves to affected documents.

/// Everything a doc-batch task needs to reconcile its chunk against current
/// Keyhive state. Built by the driver at reconcile start; contains ONLY the
/// documents named by the covered rows.
#[derive(Debug, Clone)]
struct BatchSeed {
    covers_through: u64,
    docs: Vec<ObjId>,
    group_documents: HashMap<[u8; 32], BTreeSet<ObjId>>,
    managed_group_parts: HashSet<PartId>,
    local_principal: PeerId,
}

/// Serial side-effect commands executed through the [`Outbox`].
///
/// Parallel work (doc-batch reconciliation) does NOT travel here — it flows
/// through the [`Scheduler`] spawn queue so the driver can run batches
/// concurrently. The outbox is strictly for effects whose order matters.
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
    /// Subscription delivered keyhive log rows (seq + raw event bytes).
    Rows(Vec<driver::AdmittedRow>),
    /// A spawned doc-batch task committed its reconciliation transaction.
    BatchCommitted(TaskId),
    /// The durable cursor now reflects everything up to `watermark`.
    CursorPersisted(u64),
    /// The announcement command was delivered.
    SettledAnnounced,
    /// The driver reported the machine idle. Drives the settled-watermark
    /// announcements the hub's quiescence probe waits on.
    Idle,
}

/// Reserved job id for rows settled without any scheduled work (events whose
/// affected-document set is empty). Never collides with scheduler ids, which
/// are allocated from zero upward.
const IMMEDIATE_JOB: TaskId = TaskId::MAX;

/// Pure reducer: events in, commands and scheduled jobs out. No I/O, no
/// clock reads, no channels — the driver owns all of that.
#[derive(Debug)]
struct GroupPartCore {
    /// Multi-stream admission/settlement machine. One stream today
    /// ([`EventLog`]); the genericity stays so per-part streams slot in when
    /// the store grows per-part cursors.
    machine: WatermarkMachine<EventLog, TaskId, Lane, (), u64>,
    outbox: Outbox<Cmd, ()>,
    scheduler: Scheduler<BatchSeed>,
    /// Admitted-but-unsettled rows, ascending by seq. The admission guard
    /// keeps it deduped; reconciles drain their covered prefix from the front.
    pending_rows: Vec<driver::AdmittedRow>,
    reconcile_active: bool,
    /// Seqs covered by the active reconcile; settled via the gate job.
    reconcile_rows: Vec<u64>,
    /// Scheduler task id the reconcile's row waiters are keyed under.
    gate_job: Option<TaskId>,
    /// Scheduler task ids of the active reconcile still awaiting commit.
    pending_batches: usize,
    /// Highest admission seq whose cursor is durably persisted.
    last_settled: u64,
    /// Highest settled seq announced to the hub.
    last_announced_settled: u64,
    announced_idle: bool,
}

impl GroupPartCore {
    fn new() -> Self {
        Self {
            machine: Default::default(),
            outbox: Default::default(),
            scheduler: Default::default(),
            pending_rows: Vec::new(),
            reconcile_active: false,
            reconcile_rows: Vec::new(),
            gate_job: None,
            pending_batches: 0,
            last_settled: 0,
            last_announced_settled: 0,
            announced_idle: false,
        }
    }

    /// Hand subscription rows to the admission guard. Returns `true` when at
    /// least one new row was admitted (i.e. work exists).
    fn on_rows(&mut self, rows: Vec<driver::AdmittedRow>) -> bool {
        self.announced_idle = false;
        let mut admitted = false;
        for row in rows {
            // At-least-once replay of an already-watermarked cursor.
            if self.machine.admit(EventLog, row.seq) {
                self.pending_rows.push(row);
                admitted = true;
            }
        }
        if admitted {
            // A late replay can deliver rows below an already-pending one;
            // reconciles consume a sorted prefix, so restore order.
            self.pending_rows.sort_unstable();
            self.pending_rows.dedup();
        }
        admitted
    }

    /// True when an incremental reconcile should start: admitted-but-
    /// unsettled rows exist and none is in flight.
    fn should_reconcile(&self) -> bool {
        !self.reconcile_active && !self.pending_rows.is_empty()
    }

    /// Highest admitted-but-unsettled seq — the coverage target.
    fn reconcile_target(&self) -> u64 {
        self.pending_rows
            .last()
            .expect("reconcile_target called with no pending rows")
            .seq
    }

    /// Drain the covered prefix of pending rows (seq ≤ target).
    fn take_covered_rows(&mut self, upto: u64) -> Vec<driver::AdmittedRow> {
        let covered = self
            .pending_rows
            .iter()
            .take_while(|row| row.seq <= upto)
            .cloned()
            .collect::<Vec<_>>();
        self.pending_rows.drain(..covered.len());
        covered
    }

    /// True while any admitted row, reconcile batch, or serial command is
    /// outstanding.
    fn has_outstanding_work(&self) -> bool {
        !self.pending_rows.is_empty() || self.reconcile_active || !self.outbox.is_empty()
    }

    /// Start an incremental reconcile covering `covered` (already drained
    /// from the pending prefix). Each seed becomes a concurrent scheduler
    /// task; every covered row is gated behind the LAST batch committing.
    ///
    /// Seeds with NO documents (rows naming no resolvable doc) still settle
    /// their watermark: they are tracked under [`IMMEDIATE_JOB`] and settled
    /// synchronously — hints, not work.
    fn begin_reconcile(
        &mut self,
        now: Instant,
        seeds: Vec<BatchSeed>,
        covered: Vec<driver::AdmittedRow>,
    ) {
        // Covered rows are drained from the pending prefix before this call,
        // so the only invariant left is "no reconcile already in flight".
        debug_assert!(!self.reconcile_active);
        let covered_seqs: Vec<u64> = covered.iter().map(|row| row.seq).collect();

        if seeds.is_empty() {
            // Nothing named any doc: settle the rows outright.
            for seq in &covered_seqs {
                self.machine
                    .track(EventLog, IMMEDIATE_JOB, *seq, [Lane], ());
            }
            let reached = self.machine.settle_job(IMMEDIATE_JOB).into_iter().fold(
                None,
                |acc: Option<u64>, (_, reached)| reached.or(acc),
            );
            if let Some(watermark) = reached {
                self.outbox.push(Cmd::AdvanceCursor(watermark), ());
            }
            return;
        }

        self.reconcile_active = true;
        self.reconcile_rows = covered_seqs;
        self.pending_batches = seeds.len();

        let mut job_ids = Vec::with_capacity(seeds.len());
        for seed in seeds {
            job_ids.push(self.scheduler.spawn(now, seed));
        }
        // Gate every covered row on the reconcile: one waiter per row, all
        // sharing the lane that settles when the last batch commits. The
        // job key is the first batch's id — JobBoard keys (job, cursor)
        // pairs, so one job may wait on many cursors.
        let gate = job_ids[0];
        for seq in &self.reconcile_rows {
            self.machine.track(EventLog, gate, *seq, [Lane], ());
        }
        self.gate_job = Some(gate);
    }

    /// A doc-batch task committed its tx. When it was the reconcile's last
    /// batch, settles every covered row via the gate job and emits the
    /// reachable watermark as an [`Cmd::AdvanceCursor`].
    fn on_batch_committed(&mut self, _job: TaskId) {
        debug_assert!(self.reconcile_active);
        self.pending_batches = self.pending_batches.saturating_sub(1);
        if self.pending_batches > 0 {
            return;
        }
        self.reconcile_active = false;
        let gate_job = self.gate_job.take().expect("active reconcile has a gate");
        let mut watermark: Option<u64> = None;
        for (_, reached) in self.machine.settle_job(gate_job) {
            if reached.is_some() {
                watermark = reached;
            }
        }
        self.reconcile_rows.clear();
        if let Some(watermark) = watermark {
            self.outbox.push(Cmd::AdvanceCursor(watermark), ());
        }
    }

    fn on_cursor_persisted(&mut self, watermark: u64) {
        self.last_settled = self.last_settled.max(watermark);
        self.announced_idle = false;
    }

    /// Announce the settled admission watermark: when going idle, or when a
    /// newer cursor persisted since the last announcement.
    fn on_idle(&mut self) {
        if !self.announced_idle || self.last_settled > self.last_announced_settled {
            self.last_announced_settled = self.last_settled;
            self.outbox
                .push(Cmd::AnnounceSettled(self.last_announced_settled), ());
            self.announced_idle = true;
        }
    }

    /// Feed an event back into the reducers.
    fn on_evt(&mut self, evt: Evt) {
        match evt {
            Evt::Rows(rows) => {
                self.on_rows(rows);
            }
            Evt::BatchCommitted(job) => self.on_batch_committed(job),
            Evt::CursorPersisted(watermark) => self.on_cursor_persisted(watermark),
            Evt::SettledAnnounced => {}
            Evt::Idle => self.on_idle(),
        }
    }
}

impl crate::runtime2::driver::StreamMachine for GroupPartCore {
    type Evt = Evt;
    type Cmd = Cmd;
    type Seed = BatchSeed;

    fn on_evt(&mut self, evt: Evt) {
        GroupPartCore::on_evt(self, evt);
    }

    fn front_cmd(&mut self) -> Option<(utils_rs::prelude::Uuid, &Cmd)> {
        self.outbox.front().map(|(pending, cmd)| (pending.id(), cmd))
    }

    fn complete_cmd(&mut self, id: utils_rs::prelude::Uuid) {
        drop(self.outbox.complete(id));
    }

    fn complete_job(&mut self, id: TaskId) {
        // Stop/completion pairing (friction (c)): retire the scheduler entry
        // before the completion event lands so live-set-derived state is
        // never stale. We never respawn batch tasks.
        let _ = self.scheduler.stop(id);
    }

    fn job_completed_evt(&mut self, job: TaskId) -> Evt {
        Evt::BatchCommitted(job)
    }

    fn drain_spawn_queue(&mut self) -> Vec<SpawnedTask<BatchSeed>> {
        self.scheduler.drain_spawn_queue().collect()
    }

    fn tick_scheduler(&mut self, now: Instant) {
        self.scheduler.tick(now);
    }

    fn is_idle(&mut self) -> bool {
        !self.has_outstanding_work()
    }
}

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
        task: SpawnedTask<BatchSeed>,
        result_tx: &tokio::sync::mpsc::UnboundedSender<
            Result<TaskId, eyre::Report>,
        >,
        live: &mut HashMap<TaskId, tokio::task::JoinHandle<()>>,
    ) {
        let job = task.id;
        let seed = task.seed;
        let store = self.store.clone();
        let keyhive = self.keyhive.clone();
        let result_tx = result_tx.clone();
        let handle = tokio::spawn(async move {
            let outcome = run_batch(&store, &keyhive, &seed).await;
            // Worker loop gone (shutdown) → nothing to report to; dropping
            // the send error is the documented exception (channel closure IS
            // the shutdown signal here).
            drop(result_tx.send(outcome.map(|()| job)));
        });
        live.insert(job, handle);
    }
}

impl crate::runtime2::driver::DriverHooks<GroupPartCore> for GroupPartDriver {
    fn pump<'a>(
        &'a mut self,
        machine: &'a mut GroupPartCore,
    ) -> futures::future::BoxFuture<'a, Res<()>> {
        Box::pin(async move {
            if !machine.should_reconcile() {
                return Ok(());
            }
            let target = machine.reconcile_target();
            let covered = machine.take_covered_rows(target);
            let (group_documents, managed_group_parts) = self.snapshot_keyhive().await?;
            let docs = select_incremental_docs(
                covered
                    .iter()
                    .map(|row| affected_documents(&row.bytes, &group_documents)),
            )?;
            let seeds = docs
                .chunks(DOC_BATCH_SIZE)
                .map(|chunk| BatchSeed {
                    covers_through: target,
                    docs: chunk.to_vec(),
                    group_documents: group_documents.clone(),
                    managed_group_parts: managed_group_parts.clone(),
                    local_principal: self.local_peer_id,
                })
                .collect();
            machine.begin_reconcile(Instant::now(), seeds, covered);
            Ok(())
        })
    }

    fn on_idle<'a>(
        &'a mut self,
        machine: &'a mut GroupPartCore,
    ) -> futures::future::BoxFuture<'a, Res<()>> {
        Box::pin(async move {
            machine.on_evt(Evt::Idle);
            Ok(())
        })
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
}

impl GroupPartDriver {
    /// One-time full projection build for a fresh store (durable cursor ==
    /// 0): re-derive the projection from CURRENT Keyhive state over ALL known
    /// documents. Batches run concurrently (bounded); the final empty-batch
    /// transaction commits the durable cursor so incremental reconciliation
    /// takes over from there. There are no periodic rebuilds: every later
    /// change is attributed through the admission stream.
    async fn build_initial_projection(&mut self) -> Res<()> {
        tracing::debug!("group-part worker building initial full projection");
        let (group_documents, managed_group_parts) = self.snapshot_keyhive().await?;
        const CURSOR: u64 = 0;
        let docs: Vec<_> = self.keyhive.document_ids().await;
        // Owned batches: borrowing chunk slices here would bake their
        // lifetime into the boxed hook future and break higher-ranked
        // inference.
        let batches: Vec<Vec<ObjId>> =
            docs.chunks(DOC_BATCH_SIZE).map(<[ObjId]>::to_vec).collect();
        let futs = batches.into_iter().map(|docs| {
            let store = self.store.clone();
            let keyhive = self.keyhive.clone();
            let group_documents = group_documents.clone();
            let managed_group_parts = managed_group_parts.clone();
            let local_principal = self.local_peer_id;
            async move {
                let mut reconciliations = Vec::with_capacity(docs.len());
                for doc in docs {
                    reconciliations.push(
                        reconcile_doc(
                            &keyhive,
                            doc,
                            &group_documents,
                            &managed_group_parts,
                            local_principal,
                        )
                        .await?,
                    );
                }
                store
                    .reconcile_group_part_batch(&reconciliations, CURSOR, false)
                    .await
            }
        });
        drive_buffered(futs, INITIAL_BUILD_CONCURRENCY).await?;
        self.store
            .reconcile_group_part_batch(&[], CURSOR, true)
            .await?;
        Ok(())
    }

    /// Fetch the Keyhive-derived snapshots shared by every batch task, and
    /// pre-create group parts.
    ///
    /// Pre-creation rationale (member-before-group ordering): a group part
    /// row must exist for the part to be advertiseable (`summarize_parts`
    /// succeeds) even before any doc payload arrives — a pending want is
    /// pull access and the route must be establishable for the first pull to
    /// promote it. Source is ALL visible groups, NOT just groups referenced
    /// by known docs: membership precedes document payloads, and an empty
    /// group must still advertise its part or peers answer the route with
    /// UnkownParts and it never establishes. Runs before doc reconciliation
    /// so member-before-group and group-before-member orderings both settle.
    async fn snapshot_keyhive(
        &self,
    ) -> Res<(
        HashMap<[u8; 32], BTreeSet<ObjId>>,
        HashSet<PartId>,
    )> {
        for group_id in self.keyhive.visible_group_ids().await {
            self.store
                .ensure_part(group_part_id(group_id.to_bytes()))
                .await?;
        }
        let group_documents = self.keyhive.group_document_ids_by_id().await;
        let managed_group_parts: HashSet<PartId> =
            group_documents.keys().copied().map(group_part_id).collect();
        Ok((group_documents, managed_group_parts))
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

/// Execute one doc-batch seed: compute each document's reconciliation
/// against current Keyhive state, then commit the batch tx (without
/// advancing the durable cursor — that is [`Cmd::AdvanceCursor`]'s job).
async fn run_batch(
    store: &SqliteBigRepoStore,
    keyhive: &BigKeyhiveHandle,
    seed: &BatchSeed,
) -> Res<()> {
    let mut reconciliations = Vec::with_capacity(seed.docs.len());
    for &doc in &seed.docs {
        reconciliations.push(
            reconcile_doc(
                keyhive,
                doc,
                &seed.group_documents,
                &seed.managed_group_parts,
                seed.local_principal,
            )
            .await?,
        );
    }
    store
        .reconcile_group_part_batch(&reconciliations, seed.covers_through, false)
        .await
}

/// Resolve the documents named by a batch of keyhive events, deduplicated
/// and ascending. This is the whole "which docs does this batch touch"
/// computation — there is deliberately no fallback to scanning all known
/// documents.
fn select_incremental_docs(
    rows: impl IntoIterator<Item = Res<Vec<ObjId>>>,
) -> Res<Vec<ObjId>> {
    let mut docs: BTreeSet<ObjId> = BTreeSet::new();
    for row in rows {
        docs.extend(row?);
    }
    Ok(docs.into_iter().collect())
}

async fn reconcile_doc(
    keyhive: &BigKeyhiveHandle,
    doc: ObjId,
    group_documents: &HashMap<[u8; 32], BTreeSet<ObjId>>,
    managed_group_parts: &HashSet<PartId>,
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
    let desired_group_parts: HashSet<PartId> = group_documents
        .iter()
        .filter(|(_, documents)| documents.contains(&doc))
        .map(|(group_id, _)| group_part_id(*group_id))
        .collect();
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
        managed_group_parts: managed_group_parts.clone(),
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

/// Resolve the documents a persisted keyhive event affects. Returns an empty
/// vec for hint-only events (prekey rotations carry no document identity);
/// their rows still settle their watermark.
fn affected_documents(
    bytes: &[u8],
    group_documents: &HashMap<[u8; 32], BTreeSet<ObjId>>,
) -> Res<Vec<ObjId>> {
    let event: StaticEvent<Vec<u8>> = bincode::deserialize(bytes)
        .map_err(|err| ferr!("persisted Keyhive event decode failed: {err}"))?;
    let mut documents = Vec::new();
    match event {
        StaticEvent::CgkaOperation(operation) => {
            documents.push(ObjId::new(*operation.payload().doc_id().as_bytes()));
        }
        StaticEvent::Delegated(delegation) => {
            documents.extend(
                delegation
                    .payload()
                    .after_content
                    .keys()
                    .map(|id| ObjId::new(id.to_bytes())),
            );
            if let Some(group_docs) = group_documents.get(delegation.issuer.as_bytes()) {
                documents.extend(group_docs.iter().copied());
            }
        }
        StaticEvent::Revoked(revocation) => {
            documents.extend(
                revocation
                    .payload()
                    .after_content
                    .keys()
                    .map(|id| ObjId::new(id.to_bytes())),
            );
            if let Some(group_docs) = group_documents.get(revocation.issuer.as_bytes()) {
                documents.extend(group_docs.iter().copied());
            }
        }
        StaticEvent::PrekeysExpanded(_) | StaticEvent::PrekeyRotated(_) => {}
    }
    Ok(documents)
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

    fn seed(covers_through: u64, docs: &[ObjId]) -> BatchSeed {
        BatchSeed {
            covers_through,
            docs: docs.to_vec(),
            group_documents: HashMap::new(),
            managed_group_parts: HashSet::new(),
            local_principal: PeerId::new([0; 32]),
        }
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

    /// Drive the core's outbox to completion like the real driver would,
    /// collecting every [`Cmd::AdvanceCursor`] watermark it emitted.
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
    fn reconcile_settles_contiguous_prefix_and_emits_advance() {
        let mut core = GroupPartCore::new();
        assert!(core.on_rows(vec![row(1), row(2), row(3)]));
        assert!(core.should_reconcile());
        let now = Instant::now();
        let covered = core.take_covered_rows(core.reconcile_target());
        core.begin_reconcile(
            now,
            vec![seed(3, &[doc(1)]), seed(3, &[doc(2)])],
            covered,
        );
        assert!(!core.should_reconcile());
        // First batch commits: watermark cannot advance yet.
        let (job_a, job_b) = {
            let mut ids = core
                .scheduler
                .drain_spawn_queue()
                .map(|task| task.id)
                .collect::<Vec<_>>();
            ids.sort();
            (ids[0], ids[1])
        };
        crate::runtime2::driver::StreamMachine::complete_job(&mut core, job_a);
        core.on_evt(Evt::BatchCommitted(job_a));
        assert!(advance_cmds(&mut core).is_empty());
        // Second batch commits: the whole covered prefix settles.
        crate::runtime2::driver::StreamMachine::complete_job(&mut core, job_b);
        core.on_evt(Evt::BatchCommitted(job_b));
        assert_eq!(advance_cmds(&mut core), vec![3]);
        assert!(!core.should_reconcile());
    }

    #[test]
    fn rows_arriving_mid_reconcile_wait_for_the_next_one() {
        let mut core = GroupPartCore::new();
        core.on_rows(vec![row(1), row(2)]);
        let now = Instant::now();
        let covered = core.take_covered_rows(core.reconcile_target());
        core.begin_reconcile(now, vec![seed(2, &[doc(1)])], covered);
        let job = core
            .scheduler
            .drain_spawn_queue()
            .next()
            .expect("one task")
            .id;
        // A late row arrives while the reconcile is in flight.
        assert!(core.on_rows(vec![row(3)]));
        crate::runtime2::driver::StreamMachine::complete_job(&mut core, job);
        core.on_evt(Evt::BatchCommitted(job));
        assert_eq!(advance_cmds(&mut core), vec![2]);
        // Still unsettled work: the next reconcile must cover seq 3.
        assert!(core.should_reconcile());
        assert_eq!(core.reconcile_target(), 3);
    }

    #[test]
    fn duplicate_rows_are_dropped_by_admission() {
        let mut core = GroupPartCore::new();
        assert!(core.on_rows(vec![row(5)]));
        assert!(!core.on_rows(vec![row(5)]));
        assert_eq!(core.reconcile_target(), 5);
    }

    #[test]
    fn docless_rows_settle_immediately_without_scheduler_work() {
        // Events carrying no resolvable document (prekey rotations, unknown
        // payloads aside) are hints: they settle their watermark outright
        // and schedule ZERO batch tasks.
        let mut core = GroupPartCore::new();
        core.on_rows(vec![row(7), row(8)]);
        let now = Instant::now();
        let covered = core.take_covered_rows(core.reconcile_target());
        core.begin_reconcile(now, vec![], covered);
        // No scheduler tasks were spawned…
        assert_eq!(core.scheduler.counts().spawn_queue, 0);
        assert_eq!(core.scheduler.counts().live, 0);
        // …but the watermark advanced past both rows.
        assert_eq!(advance_cmds(&mut core), vec![8]);
        assert!(!core.should_reconcile());
    }

    #[test]
    fn select_incremental_docs_dedups_sorts_and_propagates_errors() {
        let ok = [
            Ok(vec![doc(9), doc(1)]),
            Ok(Vec::new()),
            Ok(vec![doc(5)]),
        ];
        let selected = select_incremental_docs(ok).unwrap();
        assert_eq!(
            selected,
            vec![doc(1), doc(5), doc(9)],
            "affected docs only, deduped, ascending — never all known docs"
        );

        let failing: [Res<Vec<ObjId>>; 2] =
            [Ok(vec![doc(1)]), Err(ferr!("decode failed"))];
        assert!(select_incremental_docs(failing).is_err());
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
