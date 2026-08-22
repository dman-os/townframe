//! Background worker that maintains the Automerge Frontier partition log.
//!
//! Two independent inputs feed one publication decision:
//!
//! - **Keyhive admission stream** ([`SqliteBigRepoStore::admission_events_after`]):
//!   rows exist only after their effects are visible in the keyhive graph,
//!   so a CgkaOperation row means the doc may now be decryptable and its
//!   automerge heads can be published. Processed unconditionally — these are
//!   global crypto ops and skipping any of them would lose decryption
//!   material no later watch can recover. The durable
//!   `automerge_keyhive_cursor` advances over the contiguous admitted prefix
//!   only after every publish in it succeeded (`WatermarkBook` settlement).
//! - **Source-part subscriptions** (`subscribe_local(SubPartsRequest)`):
//!   Added/Changed events carry a commit watermark; publishing waits for the
//!   doc bundle to reach that watermark before reading heads. Per-part
//!   cursors persist after success only.
//!
//! The split follows the house sans-io pattern via [`driver`]:
//! [`AutomergeFrontierCore`] is a pure reducer;
//! [`FrontierSource`] multiplexes the admission poll with the part
//! subscription and parts-change channel (all I/O);
//! [`FrontierExec`] executes serial commands (publishes, cursor commits).
//!
//! Startup rule: a durable keyhive cursor of 0 means nothing was ever
//! published — the fresh part subscription then replays every stored object
//! at cursor 0, which *is* the full build; otherwise both streams tail from
//! their cursors. There is no gap handling: neither log is pruned.
use crate::interlude::*;
use crate::sqlite_big_repo_store::SqliteBigRepoStore;
use big_sync::HostPartStore;
use big_sync_core::outbox::Outbox;
use big_sync_core::rpc::{SubEvent, SubPartsRequest, SubscriptionTarget};
use big_sync_core::scheduler::{Scheduler, SpawnedTask};
use big_sync_core::watermark::WatermarkBook;
use big_sync_core::{ObjId, PartId};
use crate::runtime2::driver;
use future_form::Sendable;
use keyhive_core::event::static_event::StaticEvent;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

const EVENT_BATCH_SIZE: u32 = 64;
const IDLE_POLL: Duration = Duration::from_millis(25);
const AUTOMERGE_OBJ_MASK: [u8; 32] = [0x5A; 32];

pub fn automerge_docs_part_id() -> PartId {
    PartId::new(*blake3::hash(b"big_repo:automerge_docs_partition").as_bytes())
}

pub fn automerge_doc_obj_id(doc_id: crate::DocumentId) -> ObjId {
    let mut bytes = *doc_id.as_bytes();
    for (byte, mask_byte) in bytes.iter_mut().zip(AUTOMERGE_OBJ_MASK.iter()) {
        *byte ^= *mask_byte;
    }
    ObjId(big_sync_core::Byte32Id::new(bytes))
}

pub fn automerge_obj_to_doc_id(obj_id: ObjId) -> crate::DocumentId {
    let mut bytes = *obj_id.as_bytes();
    for (byte, mask_byte) in bytes.iter_mut().zip(AUTOMERGE_OBJ_MASK.iter()) {
        *byte ^= *mask_byte;
    }
    crate::DocumentId::new(bytes)
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
    pub parts_tx: tokio::sync::mpsc::UnboundedSender<HashSet<PartId>>,
    pub stop: AutomergeFrontierWorkerStopToken,
    pub run: F::Future<'static, eyre::Result<()>>,
}

pub fn spawn_automerge_frontier_worker(
    store: SqliteBigRepoStore,
    big_sync_store: Arc<dyn HostPartStore>,
    runtime: crate::runtime2::Runtime2Handle<Sendable>,
    _evt_tx: async_channel::Sender<crate::runtime2::Runtime2Evt>,
    initial_source_parts: HashSet<PartId>,
) -> SpawnedAutomergeFrontierWorker<Sendable> {
    let (parts_tx, parts_rx) = tokio::sync::mpsc::unbounded_channel();
    let (abort_handle, abort_registration) = futures::future::AbortHandle::new_pair();

    let run = Sendable::from_future(async move {
        let fut = run_automerge_frontier_tail(
            store,
            big_sync_store,
            runtime,
            parts_rx,
            initial_source_parts,
        );
        match futures::future::Abortable::new(fut, abort_registration).await {
            Ok(result) => result,
            Err(_) => Ok(()),
        }
    });

    SpawnedAutomergeFrontierWorker {
        parts_tx,
        stop: AutomergeFrontierWorkerStopToken { abort: abort_handle },
        run,
    }
}

/// Commands the core emits for its executor. Serial order is meaningful:
/// a publish always precedes the cursor commits that depend on it.
#[derive(Debug)]
enum Cmd {
    /// Publish decrypted automerge heads for `doc_id`; settle kh seq on
    /// success (admission path).
    PublishHeads { doc_id: crate::DocumentId, seq: u64 },
    /// Publish heads after the doc bundle reaches `cursor` (part-event path).
    PublishHeadsWatermarked { doc_id: crate::DocumentId, cursor: u64 },
    /// Persist one source-part cursor.
    CommitPartCursor { part_id: PartId, cursor: u64 },
    /// Persist the admission-stream watermark.
    AdvanceKhCursor(u64),
}

/// Events fed into the reducer.
#[derive(Debug)]
enum Evt {
    /// Admitted keyhive row; `cgka_doc` set only for CgkaOperation rows.
    KhRow { seq: u64, cgka_doc: Option<crate::DocumentId> },
    /// The admission-path publish for `seq` succeeded.
    PublishDone { seq: u64 },
    /// A watched source part gained an object.
    PartAdded { doc_id: crate::DocumentId, part_id: PartId, cursor: u64 },
    /// A watched object changed; commits apply to every listed part.
    PartChanged { doc_id: crate::DocumentId, cursor: u64, part_ids: Vec<PartId> },
    /// An object left a part; only the cursor moves.
    PartRemoved { part_id: PartId, cursor: u64 },
}

/// Pure reducer: events in, commands out.
#[derive(Default)]
struct AutomergeFrontierCore {
    book: WatermarkBook<u64>,
    outbox: Outbox<Cmd, ()>,
    scheduler: Scheduler<()>,
}

impl AutomergeFrontierCore {
    fn on_kh_row(&mut self, seq: u64, cgka_doc: Option<crate::DocumentId>) {
        if !self.book.begin(seq) {
            // At-least-once replay of an already-watermarked cursor.
            return;
        }
        if let Some(doc_id) = cgka_doc {
            self.outbox.push(Cmd::PublishHeads { doc_id, seq }, ());
        } else {
            self.book.finish(seq);
        }
        if let Some(watermark) = self.book.drain() {
            self.outbox.push(Cmd::AdvanceKhCursor(watermark), ());
        }
    }

    fn on_publish_done(&mut self, seq: u64) {
        self.book.finish(seq);
        if let Some(watermark) = self.book.drain() {
            self.outbox.push(Cmd::AdvanceKhCursor(watermark), ());
        }
    }

    fn is_settled(&self) -> bool {
        self.book.is_settled() && self.outbox.is_empty()
    }
}

impl crate::runtime2::driver::StreamMachine for AutomergeFrontierCore {
    type Evt = Evt;
    type Cmd = Cmd;
    type Seed = ();

    fn on_evt(&mut self, evt: Evt) {
        match evt {
            Evt::KhRow { seq, cgka_doc } => self.on_kh_row(seq, cgka_doc),
            Evt::PublishDone { seq } => self.on_publish_done(seq),
            Evt::PartAdded { doc_id, part_id, cursor } => {
                self.outbox
                    .push(Cmd::PublishHeadsWatermarked { doc_id, cursor }, ());
                self.outbox.push(Cmd::CommitPartCursor { part_id, cursor }, ());
            }
            Evt::PartChanged { doc_id, cursor, part_ids } => {
                self.outbox
                    .push(Cmd::PublishHeadsWatermarked { doc_id, cursor }, ());
                for part_id in part_ids {
                    self.outbox.push(Cmd::CommitPartCursor { part_id, cursor }, ());
                }
            }
            Evt::PartRemoved { part_id, cursor } => {
                self.outbox.push(Cmd::CommitPartCursor { part_id, cursor }, ());
            }
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
        unreachable!("automerge frontier core never spawns scheduled jobs: got {job}")
    }

    fn drain_spawn_queue(&mut self) -> Vec<SpawnedTask<()>> {
        self.scheduler.drain_spawn_queue().collect()
    }

    fn tick_scheduler(&mut self, now: std::time::Instant) {
        self.scheduler.tick(now);
    }

    fn is_idle(&mut self) -> bool {
        self.is_settled()
    }
}

/// Publish heads once the doc bundle reached the given sync watermark, then
/// read them under the keyring lock. No-op when the doc isn't materialized
/// yet (its admission/part event will re-trigger later).
async fn publish_heads(
    doc_id: crate::DocumentId,
    runtime: &crate::runtime2::Runtime2Handle<Sendable>,
    store: &SqliteBigRepoStore,
    big_sync_store: &Arc<dyn HostPartStore>,
    watermark_cursor: Option<u64>,
    automerge_part_id: PartId,
) -> Res<()> {
    let watermark = if let Some(cursor) = watermark_cursor {
        store.get_sync_commit_watermark(doc_id, cursor).await?
    } else {
        None
    };
    if let Ok(crate::runtime2::types::DocLookup::Ready(bundle)) =
        runtime.get_doc_handle(doc_id).await
    {
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
        big_sync_store.set_obj_payload(am_obj_id, payload).await?;
        big_sync_store
            .add_obj_to_parts(am_obj_id, vec![automerge_part_id])
            .await?;
    }
    Ok(())
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
        .subscribe_local(SubPartsRequest { targets })
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
    big_sync_store: Arc<dyn HostPartStore>,
    kh_read_cursor: u64,
    parts_rx: tokio::sync::mpsc::UnboundedReceiver<HashSet<PartId>>,
    watched_parts: HashSet<PartId>,
    part_listener: Option<big_sync_core::mpsc::Receiver<SubEvent>>,
}

impl FrontierSource {
    async fn resubscribe(&mut self) -> Res<()> {
        self.part_listener = subscribe_to_parts(
            &self.big_sync_store,
            &self.store,
            &self.watched_parts,
        )
        .await?;
        Ok(())
    }

    async fn next_batch(&mut self) -> Res<Vec<Evt>> {
        loop {
            // Authoritative parts changes take precedence; resubscribing
            // replays from each part's durable cursor, so nothing is lost
            // while we swap listeners.
            while let Ok(new_parts) = self.parts_rx.try_recv() {
                if new_parts != self.watched_parts {
                    self.watched_parts = new_parts;
                    self.resubscribe().await?;
                }
            }

            let rows = self
                .store
                .admission_events_after(self.kh_read_cursor, EVENT_BATCH_SIZE)
                .await?;
            if !rows.is_empty() {
                let mut out = Vec::with_capacity(rows.len());
                for row in rows {
                    self.kh_read_cursor = self.kh_read_cursor.max(row.seq);
                    let event: StaticEvent<Vec<u8>> = bincode::deserialize(&row.bytes)
                        .expect("persisted keyhive admission event must decode");
                    let cgka_doc = match &event {
                        StaticEvent::CgkaOperation(operation) => Some(crate::DocumentId::new(
                            *operation.payload().doc_id().as_bytes(),
                        )),
                        _ => None,
                    };
                    out.push(Evt::KhRow { seq: row.seq, cgka_doc });
                }
                return Ok(out);
            }

            let wait = match &mut self.part_listener {
                Some(listener) => tokio::time::timeout(IDLE_POLL, listener.recv())
                    .await
                    .ok(),
                None => {
                    tokio::time::sleep(IDLE_POLL).await;
                    None
                }
            };

            let Some(part_event) = wait else {
                // Idle window elapsed: loop re-checks parts changes and the
                // admission stream.
                continue;
            };
            let Ok(part_event) = part_event else {
                return Err(ferr!("AutomergeFrontierWorker partition listener closed"));
            };
            match part_event {
                SubEvent::Added(inner) => {
                    return Ok(vec![Evt::PartAdded {
                        doc_id: crate::DocumentId::new(*inner.obj_id.as_bytes()),
                        part_id: inner.part_id,
                        cursor: inner.cursor,
                    }]);
                }
                SubEvent::Changed(inner) => {
                    let part_ids: Vec<PartId> = inner
                        .part_ids
                        .into_iter()
                        .filter(|part_id| self.watched_parts.contains(part_id))
                        .collect();
                    if part_ids.is_empty() {
                        continue;
                    }
                    return Ok(vec![Evt::PartChanged {
                        doc_id: crate::DocumentId::new(*inner.obj_id.as_bytes()),
                        cursor: inner.cursor,
                        part_ids,
                    }]);
                }
                SubEvent::Removed(inner) => {
                    return Ok(vec![Evt::PartRemoved {
                        part_id: inner.part_id,
                        cursor: inner.cursor,
                    }]);
                }
                SubEvent::ObjectChanged(_) | SubEvent::ReplayComplete => continue,
            }
        }
    }
}

impl crate::runtime2::driver::EventSource for FrontierSource {
    type Evt = Evt;

    async fn next_batch(&mut self) -> Res<Vec<Evt>> {
        self.next_batch().await
    }
}

/// Executes the reducer's serial commands. Failures are fatal except for a
/// shutting-down runtime.
struct FrontierExec {
    store: SqliteBigRepoStore,
    big_sync_store: Arc<dyn HostPartStore>,
    runtime: crate::runtime2::Runtime2Handle<Sendable>,
    automerge_part_id: PartId,
}

impl crate::runtime2::driver::CmdExecutor<AutomergeFrontierCore> for FrontierExec {
    async fn execute(
        &mut self,
        cmd: &Cmd,
    ) -> Res<crate::runtime2::driver::ExecOutcome<AutomergeFrontierCore>> {
        let shutdown_if_stopped =
            |res: Res<()>| -> Res<crate::runtime2::driver::ExecOutcome<AutomergeFrontierCore>> {
                match res {
                    Ok(()) => Ok(crate::runtime2::driver::ExecOutcome::Done(None)),
                    Err(err) => Err(err),
                }
            };
        match *cmd {
            Cmd::PublishHeads { doc_id, seq } => {
                if let Err(err) = publish_heads(
                    doc_id,
                    &self.runtime,
                    &self.store,
                    &self.big_sync_store,
                    None,
                    self.automerge_part_id,
                )
                .await
                {
                    return shutdown_if_stopped(Err(err));
                }
                Ok(crate::runtime2::driver::ExecOutcome::Done(Some(Evt::PublishDone { seq })))
            }
            Cmd::PublishHeadsWatermarked { doc_id, cursor } => {
                if let Err(err) = publish_heads(
                    doc_id,
                    &self.runtime,
                    &self.store,
                    &self.big_sync_store,
                    Some(cursor),
                    self.automerge_part_id,
                )
                .await
                {
                    return shutdown_if_stopped(Err(err));
                }
                Ok(crate::runtime2::driver::ExecOutcome::Done(None))
            }
            Cmd::CommitPartCursor { part_id, cursor } => {
                self.store
                    .commit_automerge_part_cursor(part_id, cursor)
                    .await?;
                Ok(crate::runtime2::driver::ExecOutcome::Done(None))
            }
            Cmd::AdvanceKhCursor(watermark) => {
                self.store.commit_automerge_keyhive_cursor(watermark).await?;
                Ok(crate::runtime2::driver::ExecOutcome::Done(None))
            }
        }
    }
}

impl driver::DriverHooks<AutomergeFrontierCore> for FrontierExec {}

impl driver::SeedRunner<AutomergeFrontierCore> for FrontierExec {
    fn spawn_seed(
        &mut self,
        task: SpawnedTask<()>,
        _result_tx: &tokio::sync::mpsc::UnboundedSender<
            Result<big_sync_core::TaskId, eyre::Report>,
        >,
        _live: &mut HashMap<big_sync_core::TaskId, tokio::task::JoinHandle<()>>,
    ) {
        unreachable!("automerge frontier core never spawns scheduled jobs: got {}", task.id)
    }
}

async fn run_automerge_frontier_tail(
    store: SqliteBigRepoStore,
    big_sync_store: Arc<dyn HostPartStore>,
    runtime: crate::runtime2::Runtime2Handle<Sendable>,
    parts_rx: tokio::sync::mpsc::UnboundedReceiver<HashSet<PartId>>,
    initial_source_parts: HashSet<PartId>,
) -> Res<()> {
    let kh_read_cursor = store.automerge_keyhive_cursor().await?;
    let source = FrontierSource {
        kh_read_cursor,
        parts_rx,
        watched_parts: initial_source_parts.clone(),
        part_listener: subscribe_to_parts(
            &big_sync_store,
            &store,
            &initial_source_parts,
        )
        .await?,
        store: store.clone(),
        big_sync_store: Arc::clone(&big_sync_store),
    };
    let mut exec = FrontierExec {
        store,
        big_sync_store,
        runtime: runtime.clone(),
        automerge_part_id: automerge_docs_part_id(),
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
