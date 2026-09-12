//! FIXME: find a way to avoid blocking on BigSyncMachineCommands
//! FIXME: wire up reporting for UnkownParts errors
//! FIXME: the machine will break on UnkownParts actually

mod interlude {
    pub use utils_rs::prelude::*;

    pub use future_form::FutureForm;

    // FIXME: consider using indexed map instead
    pub use std::collections::{HashMap as Map, HashSet as Set};

    pub use crate::ids::{BuckId, ObjKey, PartKey, PeerKey};
}

/// Per-part sync mode: `CursorOnly` skips the bucket-diff path entirely;
/// `Bucket` uses the full bucket-diff strategy.
///
/// The [`Default`] is the fallback for a part with no explicit hint, so it is the single
/// switch an embedder turns to change strategy for every part it syncs. It is `Bucket`:
/// `big_sync_buckets` is materialized per part, and with authorization per part an
/// authorized peer sees a whole part's membership, so both sides describe the same set and
/// pruning is sound. An embedder with a reason to avoid the bucket path opts out
/// explicitly at its own call site, where the reason is known, rather than relying on a
/// default that would silently disable the strategy for everyone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncMode {
    CursorOnly,
    #[default]
    Bucket,
}

use std::collections::VecDeque;

use crate::interlude::*;

mod bucket;
use bucket::*;
mod cursor;
use cursor::*;
pub mod concurrent_delta_walker;
pub mod delta_walker_sparse_state;
pub mod delta_walker_state;
mod fingerprint;
mod ids;
pub mod keyed_frontier;
pub mod live_revision_watch;
/// New-generation stream abstractions (see module docs). Additive only:
/// existing machines migrate onto these in the upcoming swap, nothing is
/// rewired yet.
pub mod outbox;
pub mod revisioned_store;
pub mod serial_delta_walker;
pub mod tokio_keyed_scheduler;
use bucket::BucketMachine;
pub mod mpsc;
pub mod part_store;
use part_store::*;
pub mod rpc;
use rpc::*;
pub mod scheduler;
mod tasks;
pub mod watermark;
use crate::scheduler::{Scheduler, SpawnedTask};
use crate::tasks::leaf_buckets::*;
use crate::tasks::list_bucket::*;
use tasks::decide_peer_strat::*;
use tasks::replay_page::*;
use tasks::*;

pub use fingerprint::{Fingerprint, FingerprintSeed};
pub use ids::{BuckId, ByteKey, ObjKey, PartKey, PeerKey};
#[cfg(any(test, feature = "test-support"))]
pub use tasks::TaskCounts;
pub use tasks::{
    MachineTask, MachineTaskMsg, SyncTask, SyncTaskDeets, SyncTaskKind, TaskCtx, TaskId,
};

#[cfg(feature = "uniffi")]
uniffi::setup_scaffolding!();

#[cfg(feature = "uniffi")]
uniffi::custom_type!(ByteKey, String, {
    remote,
    lower: |id| format!("{id}"),
    try_lift: |str| {
        use std::str::FromStr;
        ByteKey::from_str(&str).map_err(|err| uniffi::deps::anyhow::anyhow!("unable to parse ByteKey from {str:?}: {err:?}"))
    }
});
#[cfg(feature = "uniffi")]
uniffi::custom_newtype!(PeerKey, ByteKey);
#[cfg(feature = "uniffi")]
uniffi::custom_newtype!(PartKey, ByteKey);
#[cfg(feature = "uniffi")]
uniffi::custom_newtype!(ObjKey, ByteKey);

structstruck::strike! {
    #[structstruck::each[derive(Debug)]]
    pub enum BigSyncEvent {
        SetPeer (
            pub struct SetPeerEvent {
                pub peer_id: PeerKey,
                /// Partitions to sync from the peer
                pub parts: Set<PartKey>,
                /// Objects to follow directly from the peer.
                pub objects: Set<ObjKey>,
            }
        ),
        RemovePeer (
            pub struct RemovePeerEvent {
                pub peer_id: PeerKey,
            }
        ),
        WaitForFullSync (
            pub struct WaitForFullSyncEvent {
                pub waiter_id: u64,
                pub peer_ids: Set<PeerKey>,
                pub part_ids: Set<PartKey>,
            }
        ),
        SyncCompleted (
            pub struct SyncCompletedEvent {
                pub task_id: TaskId,
                pub peer_id: PeerKey,
                pub completion: SyncTaskCompletion,
            }
        ),
        SyncFailed (
            pub struct SyncFailedEvent {
                pub task_id: TaskId,
                pub peer_id: PeerKey,
                pub obj_id: ObjKey,
                pub err: eyre::Report,
            }
        ),
        SyncStale (
            pub struct SyncStaleEvent {
                pub task_id: TaskId,
                pub peer_id: PeerKey,
                pub obj_id: ObjKey,
            }
        ),
        RemoveCompleted (
            pub struct RemoveCompletedEvent {
                pub task_id: TaskId,
                pub peer_id: PeerKey,
                pub obj_id: ObjKey,
            }
        ),
        RemoveFailed (
            pub struct RemoveFailedEvent {
                pub task_id: TaskId,
                pub peer_id: PeerKey,
                pub obj_id: ObjKey,
                pub err: eyre::Report,
            }
        ),
    }
}

structstruck::strike! {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum SyncCompletionDeets {
        AddedMember,
        RemovedMember,
        ChangedObject,
        Noop,
    }
}

structstruck::strike! {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct SyncTaskCompletion {
        pub obj_id: ObjKey,
        pub deets: SyncCompletionDeets,
    }
}

structstruck::strike! {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct SyncJobEvt {
        pub obj_id: ObjKey,
        pub cursors: Set<CursorIndex>,
        pub deets: SyncCompletionDeets,
    }
}

structstruck::strike! {
    /// Commands must be done immediately blocking the machine and the next response
    /// must be the command result. Failing commands are not recoverable.
    /// This is different from tasks which can be retried are scheduled concurrently to machine.
    #[derive(Clone, Debug)]
    pub enum BigSyncMachineCommand {
        SetPartCursor {
            peer_id: PeerKey,
            part_id: PartKey,
            cursor: CursorIndex
        },
    }
}
structstruck::strike! {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum SyncStatEvent {
        ObjectSynced {
            peer_id: PeerKey,
            obj_id: ObjKey,
        },
        PeerPartFullySynced {
            peer_id: PeerKey,
            part_id: PartKey,
        },
        PeerPartStale {
            peer_id: PeerKey,
            part_id: PartKey,
        },
        PartFullySynced {
            part_id: PartKey,
        },
        PartStale {
            part_id: PartKey,
        },
        PeerFullySynced {
            peer_id: PeerKey,
        },
        PeerStale {
            peer_id: PeerKey,
        },
        FullSyncWaiterSatisfied {
            waiter_id: u64,
        },
    }
}

/// How long a route is left alone after the peer denied it.
///
/// A pacing knob, not a measured threshold: denial is reversible, because a
/// grant may simply not have reached the peer's store yet.
const UNAUTHORIZED_BACKOFF: Duration = Duration::from_secs(30);

/// The pacing backoff applied to a denied replay route.
///
/// Exposed for tests, so one can advance the machine's clock past the pacing
/// instead of sleeping it out. A test whose own deadline equals this constant is
/// racing it rather than measuring it.
#[cfg(any(test, feature = "test-support"))]
pub fn unauthorized_backoff() -> Duration {
    UNAUTHORIZED_BACKOFF
}

/// A replay route's stable identity.
///
/// The cursor travels in the page request, but it moves as the machine consumes
/// events, so it must not key the in-flight state: a key that changes under the
/// machine would strand the page it belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ReplayRoute {
    Part(PartKey),
    Object(ObjKey),
}

impl ReplayRoute {
    fn of(target: &SubscriptionTarget) -> Self {
        match target {
            SubscriptionTarget::Part { part_id, .. } => Self::Part(*part_id),
            SubscriptionTarget::Object { obj_id } => Self::Object(*obj_id),
        }
    }
}

structstruck::strike! {
    struct PeerState {
        sync_workers: Map<ObjKey, struct SyncWorkerState {
            task_id: TaskId,
            cursors: Set<CursorIndex>,
            part_hints: Set<PartKey>,
            remote_payload: Option<crate::part_store::ObjPayload>,
        }>,
        /// In-flight backend removal tasks keyed by object, mirroring
        /// `sync_workers` coalescing semantics.
        remove_workers: Map<ObjKey, SyncWorkerState>,
        /// Removals whose in-flight task was cancelled by a re-add. The
        /// re-removal (remaining hints) and re-sync (re-added parts) are
        /// deferred until the cancelled task's completion event lands, so the
        /// zombie task — which may still be mid-removal after a cooperative
        /// cancel — cannot race the new work.
        pending_removals: Map<ObjKey, struct PendingRemoval {
            remaining_hints: Set<PartKey>,
            re_added_parts: Set<PartKey>,
            cursors: Set<CursorIndex>,
            remote_payload: Option<crate::part_store::ObjPayload>,
        }>,
        /// One in-flight page request per replay route. Pages are discrete, so
        /// this is tracked per route rather than as one per-connection worker,
        /// and `caught_up` is what the peer-level replay-done stat aggregates.
        replay_pages: Map<ReplayRoute, struct ReplayPageState {
            task_id: TaskId,
            caught_up: bool,
        }>,
        objects: Set<ObjKey>,

        cursor_machine: CursorSyncMachine,

        cursors_cmd_buf: Vec<CursorMachineCommand>,
        bucket_cmd_buf: Vec<BucketMachineCommand>,

        parts: Map<PartKey, struct PeerPartState {
            strat: enum PeerPartStrategy {
                Pending(TaskId),
                Bucket(struct BucketState {
                    replay_cursor: CursorIndex,
                    machine: Box<BucketMachine>,
                    active_list_tasks: Map<TaskId, ListBucketsTask>,
                    active_leaf_tasks: Map<TaskId, LeafBucketsTask>,
                }),
                Cursor(struct CursorState {
                    replay_cursor: CursorIndex,
                }),
            }
        }>
    }
}

impl PeerState {
    fn cursors_for_peer_replay_worker_parts<'a>(
        &self,
        parts: impl std::iter::Iterator<Item = &'a PartKey>,
    ) -> Map<PartKey, CursorIndex> {
        parts
            .filter_map(
                |part_id| match self.parts.get(part_id).map(|state| &state.strat) {
                    // A part whose strategy is still being negotiated (or that was
                    // removed and re-added while a replay worker still references
                    // it) has no replay cursor yet. Skip it: the worker's delayed
                    // retry resubscribes once the decision lands. Replay is
                    // at-least-once, so skipping here cannot lose data.
                    None | Some(PeerPartStrategy::Pending(_)) => None,
                    Some(PeerPartStrategy::Bucket(BucketState { replay_cursor, .. })) => {
                        Some((*part_id, *replay_cursor))
                    }
                    Some(PeerPartStrategy::Cursor(CursorState { replay_cursor, .. })) => {
                        Some((*part_id, *replay_cursor))
                    }
                },
            )
            .collect()
    }
}

structstruck::strike! {
    #[structstruck::each[derive(Default)]]
    struct SyncStatMachine {
        stat_evts: Vec<SyncStatEvent>,
        last_object_syncs: Map<(PeerKey, PartKey), (ObjKey, std::time::Instant)>,
        waiters: Map<u64, struct FullSyncWaiterState {
            done_set: Set<(PeerKey, PartKey)>,
            need_set: Set<(PeerKey, PartKey)>,
        }>,
        peers: Map<PeerKey, struct PeerSyncState {
            replay_phase_done: bool,
            emitted_full_synced: bool,
            parts: Map<PartKey, struct PeerPartStatState {
                emitted_full_synced: bool,
                cursor_active: bool,
                multi_strat: bool,
                /// True while the part's strategy is still being negotiated
                /// (Pending decision task) — an undecided part must block
                /// full sync.
                pending: bool,
            }>,
            fully_synced_parts: Set<PartKey>,
        }>,
        parts: Map<PartKey, struct PartSyncState {
            emitted_full_synced: bool,
            peers: Set<PeerKey>,
            fully_synced_peers: Set<PeerKey>,
        }>,
    }
}

impl SyncStatMachine {
    #[cfg(any(test, feature = "test-support"))]
    pub fn debug_full_sync_waiters(&self) -> Map<u64, Vec<(PeerKey, PartKey)>> {
        self.waiters
            .iter()
            .map(|(&waiter_id, waiter)| {
                (
                    waiter_id,
                    waiter
                        .need_set
                        .difference(&waiter.done_set)
                        .copied()
                        .collect(),
                )
            })
            .collect()
    }

    /// TEMP-DIAGNOSTIC: per (peer, part) full-sync blocking flags.
    #[cfg(any(test, feature = "test-support"))]
    pub fn debug_peer_part_sync_flags(&self) -> Vec<(PeerKey, PartKey, bool, bool, bool, bool)> {
        let mut out = Vec::new();
        for (peer_id, peer_state) in &self.peers {
            for part_id in peer_state.parts.keys().copied() {
                let default = Default::default();
                let part_state = peer_state.parts.get(&part_id).unwrap_or(&default);
                out.push((
                    *peer_id,
                    part_id,
                    part_state.pending,
                    part_state.multi_strat,
                    peer_state.replay_phase_done,
                    part_state.cursor_active,
                ));
            }
        }
        out
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn debug_last_object_syncs(&self) -> Vec<(PeerKey, PartKey, ObjKey, std::time::Instant)> {
        self.last_object_syncs
            .iter()
            .map(|(&(peer_id, part_id), &(obj_id, at))| (peer_id, part_id, obj_id, at))
            .collect()
    }

    fn peer_part_is_fully_synced(&self, peer_id: PeerKey, part_id: PartKey) -> bool {
        let Some(peer_state) = self.peers.get(&peer_id) else {
            return false;
        };
        let Some(peer_part_state) = peer_state.parts.get(&part_id) else {
            return false;
        };
        // An undecided (Pending) part must block full sync: its strategy is
        // still being negotiated, so we cannot claim it is fully synced.
        !peer_part_state.pending
            && !peer_part_state.multi_strat
            && peer_state.replay_phase_done
            && !peer_part_state.cursor_active
    }

    fn add_full_sync_waiter(
        &mut self,
        waiter_id: u64,
        peer_ids: Set<PeerKey>,
        part_ids: Set<PartKey>,
    ) {
        let peer_count = peer_ids.len();
        let part_count = part_ids.len();
        let mut done_set = Set::new();
        let mut need_set = Set::new();
        for &peer_id in &peer_ids {
            for &part_id in &part_ids {
                need_set.insert((peer_id, part_id));
                if self.peer_part_is_fully_synced(peer_id, part_id) {
                    done_set.insert((peer_id, part_id));
                }
            }
        }
        if done_set.len() == need_set.len() {
            tracing::debug!(
                waiter_id,
                peer_count,
                part_count,
                "full sync waiter satisfied immediately"
            );
            self.stat_evts
                .push(SyncStatEvent::FullSyncWaiterSatisfied { waiter_id });
            return;
        }
        tracing::debug!(
            waiter_id,
            peer_count = peer_ids.len(),
            part_count = part_ids.len(),
            done_count = done_set.len(),
            "register full sync waiter"
        );
        let old = self
            .waiters
            .insert(waiter_id, FullSyncWaiterState { done_set, need_set });
        assert!(old.is_none(), "fishy");
    }

    fn set_peer(&mut self, peer_id: PeerKey, parts: impl Iterator<Item = PartKey>) {
        let peer_state = self.peers.entry(peer_id).or_default();
        for part_id in parts {
            let _part_state = self.parts.entry(part_id).or_default();
            let _peer_part_state = peer_state.parts.entry(part_id).or_default();
        }
    }

    fn remove_peer(&mut self, peer_id: PeerKey) {
        let Some(peer_state) = self.peers.remove(&peer_id) else {
            return;
        };
        for (part_id, _peer_part_state) in peer_state.parts {
            self.last_object_syncs.remove(&(peer_id, part_id));
            let Some(part_state) = self.parts.get_mut(&part_id) else {
                continue;
            };
            part_state.peers.remove(&peer_id);
            part_state.fully_synced_peers.remove(&peer_id);
            // When a peer is removed, clean up all waiters that reference it
            // so they don't remain stranded.
            for waiter in self.waiters.values_mut() {
                waiter.done_set.remove(&(peer_id, part_id));
                waiter.need_set.remove(&(peer_id, part_id));
            }
            if part_state.peers.is_empty() {
                self.parts.remove(&part_id);
            } else if part_state.peers.len() == part_state.fully_synced_peers.len() {
                if !part_state.emitted_full_synced {
                    part_state.emitted_full_synced = true;
                    self.stat_evts
                        .push(SyncStatEvent::PartFullySynced { part_id });
                }
            } else if part_state.emitted_full_synced {
                part_state.emitted_full_synced = false;
                self.stat_evts.push(SyncStatEvent::PartStale { part_id });
            }
        }
        for (waiter_id, _waiter) in self
            .waiters
            .extract_if(|_id, waiter| waiter.done_set.len() == waiter.need_set.len())
        {
            self.stat_evts
                .push(SyncStatEvent::FullSyncWaiterSatisfied { waiter_id });
        }
    }

    fn mark_peer_replay_done(&mut self, peer_id: PeerKey, replay_done: bool) {
        let peer_state = self.peers.entry(peer_id).or_default();
        peer_state.replay_phase_done = replay_done;
        for part_id in peer_state.parts.keys().copied().collect::<Vec<_>>() {
            if replay_done {
                self.__check_peer_part_synced(peer_id, part_id);
            } else {
                self.__check_peer_part_stale(peer_id, part_id);
            }
        }
    }

    fn mark_peer_part_only_cursor_strat(
        &mut self,
        peer_id: PeerKey,
        part_id: PartKey,
        only_cursor_strat: bool,
    ) {
        let peer_state = self.peers.entry(peer_id).or_default();
        let peer_part_state = peer_state.parts.entry(part_id).or_default();
        peer_part_state.multi_strat = !only_cursor_strat;
        if only_cursor_strat {
            self.__check_peer_part_synced(peer_id, part_id);
        } else {
            self.__check_peer_part_stale(peer_id, part_id);
        }
    }

    fn mark_peer_part_idle(&mut self, peer_id: PeerKey, part_id: PartKey) {
        let peer_state = self.peers.entry(peer_id).or_default();
        let peer_part_state = peer_state.parts.entry(part_id).or_default();
        peer_part_state.cursor_active = false;
        self.__check_peer_part_synced(peer_id, part_id);
    }

    fn mark_peer_part_cursor_active(&mut self, peer_id: PeerKey, part_id: PartKey) {
        let peer_state = self.peers.entry(peer_id).or_default();
        let part_state = self.parts.entry(part_id).or_default();
        let peer_part_state = peer_state.parts.entry(part_id).or_default();
        part_state.peers.insert(peer_id);

        peer_part_state.cursor_active = true;
        self.__check_peer_part_stale(peer_id, part_id);
    }

    fn mark_peer_part_pending(&mut self, peer_id: PeerKey, part_id: PartKey, pending: bool) {
        let peer_state = self.peers.entry(peer_id).or_default();
        let peer_part_state = peer_state.parts.entry(part_id).or_default();
        peer_part_state.pending = pending;
        if pending {
            self.__check_peer_part_stale(peer_id, part_id);
        } else {
            self.__check_peer_part_synced(peer_id, part_id);
        }
    }

    fn __check_peer_part_synced(&mut self, peer_id: PeerKey, part_id: PartKey) {
        let peer_state = self.peers.entry(peer_id).or_default();
        let part_state = self.parts.entry(part_id).or_default();
        let peer_part_state = peer_state.parts.entry(part_id).or_default();
        part_state.peers.insert(peer_id);

        if peer_part_state.pending
            || peer_part_state.multi_strat
            || !peer_state.replay_phase_done
            || peer_part_state.cursor_active
        {
            return;
        }

        peer_state.fully_synced_parts.insert(part_id);
        part_state.fully_synced_peers.insert(peer_id);

        for waiter in self.waiters.values_mut() {
            if waiter.need_set.contains(&(peer_id, part_id)) {
                waiter.done_set.insert((peer_id, part_id));
            }
        }
        for (waiter_id, _waiter) in self
            .waiters
            .extract_if(|_id, waiter| waiter.done_set.len() == waiter.need_set.len())
        {
            self.stat_evts
                .push(SyncStatEvent::FullSyncWaiterSatisfied { waiter_id });
        }

        if !peer_part_state.emitted_full_synced {
            peer_part_state.emitted_full_synced = true;
            self.stat_evts
                .push(SyncStatEvent::PeerPartFullySynced { peer_id, part_id });
        }
        if peer_state.fully_synced_parts.len() == peer_state.parts.len()
            && !peer_state.emitted_full_synced
        {
            peer_state.emitted_full_synced = true;
            self.stat_evts
                .push(SyncStatEvent::PeerFullySynced { peer_id });
        }
        if part_state.peers.len() == part_state.fully_synced_peers.len()
            && !part_state.emitted_full_synced
        {
            part_state.emitted_full_synced = true;
            self.stat_evts
                .push(SyncStatEvent::PartFullySynced { part_id });
        }
    }

    fn __check_peer_part_stale(&mut self, peer_id: PeerKey, part_id: PartKey) {
        let peer_state = self.peers.entry(peer_id).or_default();
        let part_state = self.parts.entry(part_id).or_default();
        let peer_part_state = peer_state.parts.entry(part_id).or_default();
        part_state.peers.insert(peer_id);

        if !(peer_part_state.pending
            || peer_part_state.multi_strat
            || !peer_state.replay_phase_done
            || peer_part_state.cursor_active)
        {
            return;
        }

        peer_state.fully_synced_parts.remove(&part_id);
        part_state.fully_synced_peers.remove(&peer_id);

        for waiter in self.waiters.values_mut() {
            waiter.done_set.remove(&(peer_id, part_id));
        }

        if peer_state.emitted_full_synced {
            peer_state.emitted_full_synced = false;
            self.stat_evts.push(SyncStatEvent::PeerStale { peer_id });
        }

        if peer_part_state.emitted_full_synced {
            peer_part_state.emitted_full_synced = false;
            self.stat_evts
                .push(SyncStatEvent::PeerPartStale { peer_id, part_id });
        }
        if part_state.emitted_full_synced {
            part_state.emitted_full_synced = false;
            self.stat_evts.push(SyncStatEvent::PartStale { part_id });
        }
    }

    fn record_object_synced(&mut self, peer_id: PeerKey, part_id: PartKey, obj_id: ObjKey) {
        self.last_object_syncs
            .insert((peer_id, part_id), (obj_id, std::time::Instant::now()));
    }
}

structstruck::strike! {
    #[derive(Default)]
    pub struct BigSyncMachine {
        all_seen_peer: Set<PeerKey>,
        peers: Map<PeerKey, PeerState>,
        stat_machine: SyncStatMachine,

        /// Strategy hint for parts with no explicit per-part override. A caller that
        /// wants the bucket path for every part it syncs sets this to
        /// [`SyncMode::Bucket`].
        default_sync_mode: SyncMode,

        cmds: VecDeque<(Uuid, BigSyncMachineCommand, Option<CursorIndex>, PeerKey)>,
        tasks: Scheduler<TaskSeed>,
    }
}

// public surface
impl BigSyncMachine {
    pub fn set_max_task_backoff(&mut self, max_backoff: Duration) {
        self.tasks.set_max_backoff(max_backoff);
    }

    /// Strategy hint applied to parts with no per-part override. See [`SyncMode`]:
    /// this is the knob an embedder uses to opt into (or out of) the bucket path.
    pub fn set_default_sync_mode(&mut self, mode: SyncMode) {
        self.default_sync_mode = mode;
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn task_counts(&self) -> TaskCounts {
        let counts = self.tasks.counts();
        TaskCounts {
            live: counts.live,
            delayed: counts.delayed,
            spawn_queue: counts.spawn_queue,
            stop_queue: counts.stop_queue,
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn debug_full_sync_waiters(&self) -> Map<u64, Vec<(PeerKey, PartKey)>> {
        self.stat_machine.debug_full_sync_waiters()
    }

    /// TEMP-DIAGNOSTIC: per (peer, part) full-sync blocking flags.
    #[cfg(any(test, feature = "test-support"))]
    pub fn debug_peer_part_sync_flags(&self) -> Vec<(PeerKey, PartKey, bool, bool, bool, bool)> {
        self.stat_machine.debug_peer_part_sync_flags()
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn debug_last_object_syncs(&self) -> Vec<(PeerKey, PartKey, ObjKey, std::time::Instant)> {
        self.stat_machine.debug_last_object_syncs()
    }

    /// Hand the pending sync-pipeline spawns to the driver, leaving queued
    /// machine tasks in place and in order.
    ///
    /// `Scheduler` keeps a single spawn queue because its frame does not know
    /// the two concrete seed kinds, but the driver still admits each kind under
    /// its own policy — machine tasks uncapped, sync tasks capped. That
    /// partition is the driver's, so these two drains are how it is kept:
    /// take one kind, requeue the other untouched and in order.
    pub fn drain_sync_spawn_queue(&mut self) -> std::vec::IntoIter<SyncTask> {
        let mut taken = Vec::new();
        let mut rest = Vec::new();
        for spawned in self.tasks.drain_spawn_queue() {
            match spawned.seed {
                TaskSeed::Sync(seed) => taken.push(SyncTask {
                    id: spawned.id,
                    kind: seed.kind,
                    part_hints: seed.part_hints,
                    deets: seed.deets,
                }),
                seed => rest.push(SpawnedTask {
                    id: spawned.id,
                    seed,
                }),
            }
        }
        self.tasks.requeue_spawned(rest);
        taken.into_iter()
    }

    /// Hand the pending machine-task spawns to the driver, leaving queued sync
    /// spawns in place and in order. See [`Self::drain_sync_spawn_queue`] for
    /// why the queue is partitioned at the drain rather than stored split.
    pub fn drain_machine_spawn_queue(&mut self) -> std::vec::IntoIter<MachineTask> {
        let mut taken = Vec::new();
        let mut rest = Vec::new();
        for spawned in self.tasks.drain_spawn_queue() {
            match spawned.seed {
                TaskSeed::Machine(deets) => taken.push(MachineTask { id: spawned.id, deets }),
                seed => rest.push(SpawnedTask {
                    id: spawned.id,
                    seed,
                }),
            }
        }
        self.tasks.requeue_spawned(rest);
        taken.into_iter()
    }

    pub fn drain_stop_queue(&mut self) -> std::collections::hash_set::Drain<'_, u64> {
        self.tasks.drain_stop_queue()
    }

    pub fn drain_stat_evts(&mut self) -> std::vec::Drain<'_, SyncStatEvent> {
        self.stat_machine.stat_evts.drain(..)
    }

    pub fn get_cmd(&mut self) -> Option<(Uuid, BigSyncMachineCommand)> {
        let (id, cmd, _, _) = self.cmds.front()?;
        Some((*id, cmd.clone()))
    }

    pub fn handle_evt(&mut self, evt: BigSyncEvent) {
        match evt {
            BigSyncEvent::SetPeer(evt) => self.handle_set_peer_evt(evt),
            BigSyncEvent::RemovePeer(evt) => self.handle_remove_peer_evt(evt),
            BigSyncEvent::WaitForFullSync(evt) => {
                let WaitForFullSyncEvent {
                    waiter_id,
                    peer_ids,
                    part_ids,
                } = evt;
                for peer_id in &peer_ids {
                    self.refresh_peer_replay_worker(*peer_id, true);
                }
                self.stat_machine
                    .add_full_sync_waiter(waiter_id, peer_ids, part_ids);
            }
            BigSyncEvent::SyncCompleted(evt) => {
                if self.tasks.cancel(evt.task_id).is_some() {
                    self.handle_sync_completed(evt);
                }
            }
            BigSyncEvent::SyncFailed(evt) => {
                if let Some(retry) = self.tasks.cancel(evt.task_id) {
                    self.handle_sync_failed(evt, retry);
                }
            }
            BigSyncEvent::SyncStale(evt) => {
                if let Some(retry) = self.tasks.cancel(evt.task_id) {
                    self.handle_sync_stale(evt, retry);
                }
            }
            BigSyncEvent::RemoveCompleted(evt) => {
                // Always process: a completion from a task stopped by
                // `cancel_obj_removal_hint` is the trigger for the deferred
                // re-removal/re-sync (`resume_pending_removal`). `cancel`
                // GCs the task from `live` when it is still live; the handler
                // itself filters stale completions (replaced tasks, removed
                // peers).
                self.tasks.cancel(evt.task_id);
                self.handle_remove_completed(evt);
            }
            BigSyncEvent::RemoveFailed(evt) => {
                let retry = self
                    .tasks
                    .cancel(evt.task_id)
                    .unwrap_or_else(|| Retry::fresh(std::time::Instant::now()));
                self.handle_remove_failed(evt, retry);
            }
        }
    }

    pub fn handle_cmd_success(&mut self, id: Uuid) {
        let (found_id, _last_cmd, _cursor, _peer_id) = self
            .cmds
            .pop_front()
            .expect("success for a cmd that wasn't sent");
        if id != found_id {
            panic!("unexpected cmd success, cmds must be performed serially");
        }
        // Membership mutations no longer flow through commands: object
        // removal is a backend-executed task whose completion settles the
        // membership lane via handle_remove_completed.
    }

    pub fn handle_tick(&mut self, now: std::time::Instant) {
        self.tasks.tick(now);
    }

    /// The instant at which the driver should next call [`Self::handle_tick`].
    ///
    /// `None` means nothing is paced for later, so a driver can wait on events alone
    /// instead of waking to a timer. A deadline that survives a `handle_tick(now)` is
    /// strictly later than `now`, so ticking promptly cannot leave the driver spinning.
    pub fn next_due(&self) -> Option<std::time::Instant> {
        self.tasks.next_due()
    }

    pub fn handle_task_msg(&mut self, msg: MachineTaskMsg) {
        match msg {
            MachineTaskMsg::MachineTaskResult(MachineTaskResult { task_id, deets }) => {
                let Some(retry) = self.tasks.cancel(task_id) else {
                    return;
                };
                match deets {
                    TaskResultDeets::SetPeerStrategy(evt) => {
                        self.handle_set_peer_strat(task_id, retry, evt);
                    }
                    TaskResultDeets::ListBuckets(list_buckets_result) => {
                        self.handle_list_buckets_result(task_id, list_buckets_result);
                    }
                    TaskResultDeets::LeafBuckets(leaf_buckets_result) => {
                        self.handle_leaf_buckets_result(task_id, leaf_buckets_result);
                    }
                    TaskResultDeets::ReplayPage(replay_page_result) => {
                        self.handle_replay_page_result(task_id, retry, replay_page_result);
                    }
                }
            }
            MachineTaskMsg::MachineTaskError(MachineTaskError { task_id, deets }) => {
                let Some(retry) = self.tasks.cancel(task_id) else {
                    return;
                };
                match deets {
                    MachineTaskErrDeets::DecidePeerStrategy(err) => {
                        self.handle_decide_peer_strat_err(task_id, retry, err)
                    }
                    MachineTaskErrDeets::ReplayPage(err) => {
                        self.handle_replay_page_err(task_id, retry, err)
                    }
                    MachineTaskErrDeets::ListBuckets(err) => {
                        self.handle_list_buckets_err(task_id, retry, err)
                    }
                    MachineTaskErrDeets::LeafBuckets(err) => {
                        self.handle_leaf_buckets_err(task_id, retry, err)
                    }
                }
            }
        }
    }
}

// peer support
impl BigSyncMachine {
    fn handle_set_peer_evt(
        &mut self,
        SetPeerEvent {
            peer_id,
            parts,
            objects,
        }: SetPeerEvent,
    ) {
        tracing::debug!(
            peer_id = %peer_id,
            part_count = parts.len(),
            object_count = objects.len(),
            "set peer event"
        );
        // Incrementally update peer state: preserve sync_workers, cursor_machine,
        // and resolved parts while stopping only pending/obsolete strategy tasks.
        self.all_seen_peer.insert(peer_id);
        let mut peer_state = self.peers.remove(&peer_id).unwrap_or_else(|| PeerState {
            remove_workers: default(),
            pending_removals: default(),
            sync_workers: default(),
            replay_pages: default(),
            objects: default(),
            cursor_machine: default(),
            cursors_cmd_buf: default(),
            bucket_cmd_buf: default(),
            parts: default(),
        });
        let old_part_ids: Set<_> = peer_state.parts.keys().copied().collect();
        let removed_parts: Set<_> = old_part_ids.difference(&parts).copied().collect();
        let mut pending_parts = Set::new();
        let mut pending_tasks = Set::new();
        for &part_id in old_part_ids.intersection(&parts) {
            if matches!(
                peer_state.parts.get(&part_id),
                Some(PeerPartState {
                    strat: PeerPartStrategy::Pending(_)
                })
            ) && let Some(PeerPartState {
                strat: PeerPartStrategy::Pending(task_id),
            }) = peer_state.parts.remove(&part_id)
            {
                pending_parts.insert(part_id);
                pending_tasks.insert(task_id);
            }
        }
        for task_id in pending_tasks {
            let _state = self.tasks.cancel(task_id).expect(ERROR_UNRECONIZED);
        }
        for part_id in removed_parts.iter() {
            let Some(state) = peer_state.parts.remove(part_id) else {
                continue;
            };
            match state.strat {
                PeerPartStrategy::Pending(_) | PeerPartStrategy::Cursor(_) => {}
                PeerPartStrategy::Bucket(strat) => {
                    for task_id in strat
                        .active_leaf_tasks
                        .into_keys()
                        .chain(strat.active_list_tasks.into_keys())
                    {
                        let _state = self.tasks.cancel(task_id).expect(ERROR_UNRECONIZED);
                    }
                }
            }
        }
        for &part_id in &removed_parts {
            peer_state.cursor_machine.remove_part(part_id);
        }
        // Deferred work is allowed to outlive the event that created it, but
        // not the peer route that gives its part hints meaning. In particular,
        // a cancelled removal may leave a re-sync parked in
        // `pending_removals`; blindly resuming it after SetPeer removed the
        // part would enqueue a task the worker cannot map to a backend.
        peer_state.pending_removals.retain(|_, pending| {
            pending
                .remaining_hints
                .retain(|part_id| parts.contains(part_id));
            pending
                .re_added_parts
                .retain(|part_id| parts.contains(part_id));
            !pending.remaining_hints.is_empty() || !pending.re_added_parts.is_empty()
        });

        // A queued sync task owns a snapshot of its hints, so mutating only
        // SyncWorkerState would leave the queued task stale. Stop and replace
        // every affected task with a fresh snapshot. Object-routed tasks remain
        // valid with no part hints; part-routed tasks with no surviving hints
        // are simply obsolete.
        let affected_sync_workers: Vec<_> = peer_state
            .sync_workers
            .iter()
            .filter(|(_, worker)| !worker.part_hints.is_disjoint(&removed_parts))
            .map(|(&obj_id, _)| obj_id)
            .collect();
        for obj_id in affected_sync_workers {
            let mut worker = peer_state
                .sync_workers
                .remove(&obj_id)
                .expect(ERROR_IMPOSSIBLE);
            let _state = self
                .tasks
                .cancel(worker.task_id)
                .expect(ERROR_UNRECONIZED);
            worker.part_hints.retain(|part_id| parts.contains(part_id));
            if worker.part_hints.is_empty() && !objects.contains(&obj_id) {
                continue;
            }
            worker.task_id = self.tasks.spawn(std::time::Instant::now(), TaskSeed::Sync(SyncTaskSeed {
                kind: SyncTaskKind::Sync,
                part_hints: worker.part_hints.clone(),
                deets: SyncTaskDeets {
                    peer_id,
                    obj_id,
                    remote_payload: worker.remote_payload.clone(),
                },
            }));
            let old = peer_state.sync_workers.insert(obj_id, worker);
            assert!(old.is_none(), "fishy");
        }
        let added_parts: Set<_> = parts.difference(&old_part_ids).copied().collect();
        let decision_parts: Set<_> = pending_parts.union(&added_parts).copied().collect();
        if !decision_parts.is_empty() {
            let deets = MachineTaskDeets::DecidePeerStrategy(DecidePeerStrategyTask {
                peer_id,
                parts: decision_parts.clone(),
                sync_modes: default(),
                default_sync_mode: self.default_sync_mode,
            });
            let decide_task = self.tasks.spawn(std::time::Instant::now(), TaskSeed::Machine(deets));
            for part_id in &decision_parts {
                peer_state.parts.insert(
                    *part_id,
                    PeerPartState {
                        strat: PeerPartStrategy::Pending(decide_task),
                    },
                );
            }
        }
        let stale_workers: Vec<_> = peer_state
            .sync_workers
            .iter()
            .filter_map(|(&obj_id, worker)| {
                (worker.part_hints.is_empty() && !objects.contains(&obj_id)).then_some(obj_id)
            })
            .collect();
        for obj_id in stale_workers {
            let worker = peer_state
                .sync_workers
                .remove(&obj_id)
                .expect(ERROR_IMPOSSIBLE);
            let _state = self
                .tasks
                .cancel(worker.task_id)
                .expect(ERROR_UNRECONIZED);
        }
        peer_state.objects = objects;
        self.stat_machine.set_peer(peer_id, parts.iter().copied());
        for &part_id in &decision_parts {
            self.stat_machine
                .mark_peer_part_pending(peer_id, part_id, true);
        }
        self.peers.insert(peer_id, peer_state);
        self.refresh_peer_replay_worker(peer_id, false);
    }

    fn handle_remove_peer_evt(&mut self, RemovePeerEvent { peer_id }: RemovePeerEvent) {
        if let Some(old) = self.peers.remove(&peer_id) {
            tracing::debug!(
                peer_id = %peer_id,
                part_count = old.parts.len(),
                sync_worker_count = old.sync_workers.len(),
                "remove peer event"
            );
            for worker in old.sync_workers.into_values() {
                let _state = self
                    .tasks
                    .cancel(worker.task_id)
                    .expect(ERROR_UNRECONIZED);
            }
            for worker in old.remove_workers.into_values() {
                let _state = self
                    .tasks
                    .cancel(worker.task_id)
                    .expect(ERROR_UNRECONIZED);
            }
            for (_old_part_id, state) in old.parts {
                match state.strat {
                    PeerPartStrategy::Pending(_) => {}
                    PeerPartStrategy::Cursor(_strat) => {}
                    PeerPartStrategy::Bucket(strat) => {
                        for task_id in strat
                            .active_leaf_tasks
                            .into_keys()
                            .chain(strat.active_list_tasks.into_keys())
                        {
                            let _state = self.tasks.cancel(task_id).expect(ERROR_UNRECONIZED);
                        }
                    }
                }
            }
            for (_, state) in old.replay_pages {
                let _state = self
                    .tasks
                    .cancel(state.task_id)
                    .expect(ERROR_UNRECONIZED);
            }
            self.stat_machine.remove_peer(peer_id);
        }
    }

    #[tracing::instrument(
        skip_all,
        fields(
            peer_id = %peer_id,
            task_id = %task_id,
        )
    )]
    fn handle_set_peer_strat(
        &mut self,
        task_id: TaskId,
        retry: Retry,
        SetPeerStrategy {
            peer_id,
            part_strats,
        }: SetPeerStrategy,
    ) {
        let Some(peer_state) = self.peers.get_mut(&peer_id) else {
            assert!(self.all_seen_peer.contains(&peer_id), "fishy");
            return;
        };
        let response_len = part_strats.len();
        let bucket_count = part_strats
            .values()
            .filter(|deets| matches!(*deets, PeerPartStratDecision::Bucket(_)))
            .count();
        let cursor_count = part_strats
            .values()
            .filter(|deets| matches!(*deets, PeerPartStratDecision::Cursor(_)))
            .count();
        let unknown_count = part_strats
            .values()
            .filter(|deets| matches!(*deets, PeerPartStratDecision::Unkown))
            .count();
        tracing::debug!(
            peer_id = %peer_id,
            response_len,
            bucket_count,
            cursor_count,
            unknown_count,
            "set peer strategy result"
        );
        let stale_result = part_strats.keys().any(|part_id| {
            !matches!(
                peer_state.parts.get(part_id).map(|state| &state.strat),
                Some(PeerPartStrategy::Pending(old_task_id)) if *old_task_id == task_id
            )
        });
        if stale_result {
            tracing::debug!(
                peer_id = %peer_id,
                task_id = %task_id,
                "ignoring stale set peer strategy result"
            );
            return;
        }
        let mut parts_retry = Set::new();

        for (part_id, decision) in part_strats {
            let old = peer_state.parts.remove(&part_id);
            let strat = match decision {
                PeerPartStratDecision::Unkown => {
                    self.stat_machine
                        .mark_peer_part_only_cursor_strat(peer_id, part_id, false);
                    parts_retry.insert(part_id);
                    continue;
                }
                PeerPartStratDecision::Bucket(strat) => {
                    let mut machine = Box::new(BucketMachine::new(
                        part_id,
                        strat.remote_depth,
                        strat.remote_len,
                        strat.last_cursor,
                    ));
                    machine.on_bucket_page(
                        strat.initial_filtered_buckets,
                        &mut peer_state.bucket_cmd_buf,
                    );
                    self.stat_machine
                        .mark_peer_part_only_cursor_strat(peer_id, part_id, false);
                    self.stat_machine
                        .mark_peer_part_pending(peer_id, part_id, false);
                    PeerPartStrategy::Bucket(BucketState {
                        machine,
                        // NOTE: we replay from the latest
                        // on bucket instead from the last_cursor
                        replay_cursor: strat.latest_cursor,
                        active_list_tasks: default(),
                        active_leaf_tasks: default(),
                    })
                }
                PeerPartStratDecision::Cursor(strat) => {
                    self.stat_machine
                        .mark_peer_part_only_cursor_strat(peer_id, part_id, true);
                    self.stat_machine
                        .mark_peer_part_pending(peer_id, part_id, false);
                    PeerPartStrategy::Cursor(CursorState {
                        replay_cursor: strat.last_cursor,
                    })
                }
            };
            peer_state.parts.insert(part_id, PeerPartState { strat });
            if let Some(old) = old {
                match old.strat {
                    PeerPartStrategy::Pending(old_task_id) => {
                        assert!(task_id == old_task_id, "fishy");
                    }
                    PeerPartStrategy::Bucket(_) | PeerPartStrategy::Cursor(_) => {}
                }
            }
        }

        // retry peer summary requests for any parts
        // that were not resolved in the last request
        // FIXME: probably not a good idea since we're going
        // to abort any work queued by the machines if
        // this immediately works. i.e. evalute if this leads
        // to loops,
        if !parts_retry.is_empty() {
            if retry.attempt_no > 0 && response_len > 0 {
                warn!("retry attempt is retrying again after minimal improvement");
            }
            let deets = TaskSeed::Machine(MachineTaskDeets::DecidePeerStrategy(
                DecidePeerStrategyTask {
                    peer_id,
                    parts: parts_retry.clone(),
                    sync_modes: default(),
                    default_sync_mode: self.default_sync_mode,
                },
            ));
            let decide_task = if parts_retry.len() == response_len {
                self.tasks
                    .spawn_delayed(deets, retry, Duration::from_secs(2), std::time::Instant::now())
            } else {
                self.tasks.spawn(std::time::Instant::now(), deets)
            };
            for part_id in parts_retry {
                peer_state.parts.insert(
                    part_id,
                    PeerPartState {
                        strat: PeerPartStrategy::Pending(decide_task),
                    },
                );
                self.stat_machine
                    .mark_peer_part_pending(peer_id, part_id, true);
            }
        }

        let bucket_cmd_count = peer_state.bucket_cmd_buf.len();
        let cursor_cmd_count = peer_state.cursors_cmd_buf.len();
        // refresh the replay pages if needed
        self.refresh_peer_replay_worker(peer_id, false);
        tracing::debug!(
            peer_id = %peer_id,
            bucket_cmd_count,
            cursor_cmd_count,
            "drain bucket commands from set peer strategy"
        );
        self.drain_bucket_machine_cmds(peer_id);
    }

    fn handle_decide_peer_strat_err(
        &mut self,
        task_id: TaskId,
        retry: Retry,
        DecidePeerStrategyTaskError { peer_id, deets }: DecidePeerStrategyTaskError,
    ) {
        tracing::warn!(peer_id = %peer_id, task_id, ?deets, "decide peer strategy failed");
        let Some(peer_state) = self.peers.get_mut(&peer_id) else {
            assert!(self.all_seen_peer.contains(&peer_id), "fishy");
            return;
        };
        match deets {
            DecidePeerStrategyErrorDeets::ListError(ListPartsError::UnkownParts {
                unkown_parts,
            }) => {
                // The peer does not (yet) know these parts — e.g. its part
                // row appears after the route was set (a pending want on the
                // remote is only advertiseable once its part exists). Do NOT
                // drop the parts: keep them Pending and let the retry path
                // below re-decide them with backoff once the route resolves.
                tracing::debug!(
                    peer_id = %peer_id,
                    ?unkown_parts,
                    "peer does not know requested parts; keeping them pending and retrying",
                );
            }
            DecidePeerStrategyErrorDeets::Rpc(_) => {
                // noop, retry with backoff
            }
        }
        let mut parts_retry = Set::new();

        for (part_id, state) in &peer_state.parts {
            match &state.strat {
                PeerPartStrategy::Pending(old_task_id) => {
                    if *old_task_id != task_id {
                        // the task that errored out must have been stale
                        // FIXME: this assumes that all parts for a peer
                        // are decided in the same task (having per part task ids can be misleading)
                        return;
                    }
                    parts_retry.insert(*part_id);
                }
                PeerPartStrategy::Bucket(_) | PeerPartStrategy::Cursor(_) => {}
            };
        }
        if !parts_retry.is_empty() {
            let deets = MachineTaskDeets::DecidePeerStrategy(DecidePeerStrategyTask {
                peer_id,
                parts: parts_retry.clone(),
                sync_modes: default(),
                default_sync_mode: self.default_sync_mode,
            });
            let decide_task = self.tasks.spawn_delayed(
                TaskSeed::Machine(deets),
                retry,
                Duration::from_secs(2),
                std::time::Instant::now(),
            );
            for part_id in parts_retry {
                let old = peer_state.parts.insert(
                    part_id,
                    PeerPartState {
                        strat: PeerPartStrategy::Pending(decide_task),
                    },
                );
                assert!(matches!(
                    old,
                    Some(PeerPartState {
                        strat: PeerPartStrategy::Pending(_),
                        ..
                    })
                ));
                self.stat_machine
                    .mark_peer_part_pending(peer_id, part_id, true);
            }
        }
    }
}

// cursor support
impl BigSyncMachine {
    /// Wanted replay routes, each with the target to request next.
    ///
    /// A part whose strategy is still being negotiated has no replay cursor yet;
    /// skipping it is safe because replay is at-least-once and this set is
    /// recomputed whenever a strategy lands.
    fn replay_page_targets(&self, peer_id: PeerKey) -> Map<ReplayRoute, SubscriptionTarget> {
        let Some(peer_state) = self.peers.get(&peer_id) else {
            return default();
        };
        let replay_req_parts: Set<_> = peer_state
            .parts
            .iter()
            .filter_map(|(&part_id, state)| match state.strat {
                PeerPartStrategy::Pending(_) => None,
                PeerPartStrategy::Bucket(_) | PeerPartStrategy::Cursor(_) => Some(part_id),
            })
            .collect();
        let mut targets = peer_state
            .cursors_for_peer_replay_worker_parts(replay_req_parts.iter())
            .into_iter()
            .map(|(part_id, cursor)| {
                (
                    ReplayRoute::Part(part_id),
                    SubscriptionTarget::Part { part_id, cursor },
                )
            })
            .collect::<Map<_, _>>();
        targets.extend(peer_state.objects.iter().copied().map(|obj_id| {
            (
                ReplayRoute::Object(obj_id),
                SubscriptionTarget::Object { obj_id },
            )
        }));
        targets
    }

    /// `target` carrying the cursor the machine currently holds for it, or
    /// `None` when the route is gone or its strategy is still pending.
    fn refreshed_replay_target(
        &self,
        peer_id: PeerKey,
        target: &SubscriptionTarget,
    ) -> Option<SubscriptionTarget> {
        match target {
            SubscriptionTarget::Part { part_id, .. } => self
                .peers
                .get(&peer_id)?
                .cursors_for_peer_replay_worker_parts(std::iter::once(part_id))
                .into_iter()
                .next()
                .map(|(part_id, cursor)| SubscriptionTarget::Part { part_id, cursor }),
            SubscriptionTarget::Object { obj_id } => {
                Some(SubscriptionTarget::Object { obj_id: *obj_id })
            }
        }
    }

    /// Ask for one more page and record the request as in flight.
    ///
    /// `caught_up` is the verdict of the last answer for this route, so a
    /// re-issued request keeps the peer-level replay-done stat honest.
    fn spawn_replay_page(
        &mut self,
        peer_id: PeerKey,
        target: SubscriptionTarget,
        caught_up: bool,
    ) {
        let route = ReplayRoute::of(&target);
        let deets = TaskSeed::Machine(MachineTaskDeets::ReplayPage(ReplayPageTask {
            peer_id,
            target,
            limit: ReplayPageTask::LIMIT,
        }));
        let task_id = self.tasks.spawn(std::time::Instant::now(), deets);
        if let Some(peer_state) = self.peers.get_mut(&peer_id) {
            peer_state
                .replay_pages
                .insert(route, ReplayPageState { task_id, caught_up });
        }
    }

    /// Re-issue a page for `target` after a delay, keeping its last verdict.
    fn schedule_replay_page(
        &mut self,
        peer_id: PeerKey,
        target: SubscriptionTarget,
        caught_up: bool,
        retry: Retry,
        delay: Duration,
    ) {
        let route = ReplayRoute::of(&target);
        let deets = TaskSeed::Machine(MachineTaskDeets::ReplayPage(ReplayPageTask {
            peer_id,
            target,
            limit: ReplayPageTask::LIMIT,
        }));
        let task_id = self.tasks.spawn_delayed(deets, retry, delay, std::time::Instant::now());
        if let Some(peer_state) = self.peers.get_mut(&peer_id) {
            peer_state
                .replay_pages
                .insert(route, ReplayPageState { task_id, caught_up });
        }
    }

    /// The peer-level replay verdict: caught up when every wanted route has an
    /// answer saying so. No routes means there is nothing left to replay.
    fn update_peer_replay_done(&mut self, peer_id: PeerKey) {
        let routes = self.replay_page_targets(peer_id);
        let done = routes.keys().all(|route| {
            self.peers
                .get(&peer_id)
                .and_then(|peer_state| peer_state.replay_pages.get(route))
                .is_some_and(|state| state.caught_up)
        });
        self.stat_machine.mark_peer_replay_done(peer_id, done);
    }

    fn refresh_peer_replay_worker(&mut self, peer_id: PeerKey, force: bool) {
        let targets = self.replay_page_targets(peer_id);
        let Some(peer_state) = self.peers.get_mut(&peer_id) else {
            return;
        };
        if targets.is_empty() {
            for (_, state) in peer_state.replay_pages.drain() {
                let _state = self
                    .tasks
                    .cancel(state.task_id)
                    .expect(ERROR_UNRECONIZED);
            }
            self.update_peer_replay_done(peer_id);
            return;
        }
        // Retire pages for routes we no longer want, and every page when the
        // caller is forcing a restart.
        let stale: Vec<ReplayRoute> = peer_state
            .replay_pages
            .keys()
            .filter(|route| force || !targets.contains_key(*route))
            .copied()
            .collect();
        for route in stale {
            if let Some(state) = peer_state.replay_pages.remove(&route) {
                let _state = self
                    .tasks
                    .cancel(state.task_id)
                    .expect(ERROR_UNRECONIZED);
            }
        }
        let missing: Vec<SubscriptionTarget> = targets
            .into_iter()
            .filter(|(route, _)| !peer_state.replay_pages.contains_key(route))
            .map(|(_, target)| target)
            .collect();
        tracing::debug!(
            peer_id = %peer_id,
            target_count = missing.len(),
            "refresh peer replay pages"
        );
        for target in missing {
            self.spawn_replay_page(peer_id, target, false);
        }
        self.update_peer_replay_done(peer_id);
    }

    fn handle_replay_page_result(
        &mut self,
        task_id: TaskId,
        retry: Retry,
        result: ReplayPageResult,
    ) {
        let peer_id = result.peer_id;
        let target = result.target;
        let route = ReplayRoute::of(&target);
        let Some(peer_state) = self.peers.get_mut(&peer_id) else {
            assert!(self.all_seen_peer.contains(&peer_id), "fishy");
            return;
        };
        // A page for a route we no longer want, or one superseded by a newer
        // request, must not be applied: replay is per route now.
        match peer_state.replay_pages.get(&route) {
            Some(state) if state.task_id == task_id => {}
            stale => {
                // Upstream logged this drop when the replay worker was per-peer; with
                // per-route pages the same guard rejects a result whose route was
                // retired or superseded by a newer request.
                tracing::debug!(
                    peer_id = %peer_id,
                    ?task_id,
                    ?route,
                    active_task_id = ?stale.map(|state| state.task_id),
                    "dropped peer replay page result: stale or retired route",
                );
                return;
            }
        }
        let caught_up;
        let mut delayed_retry = None;
        match result.outcome {
            ReplayPageOutcome::Events(page) => {
                // No resume point means the peer's log is caught up as of the
                // last event: the paged form of the old replay barrier.
                caught_up = page.next_cursor.is_none();
                for evt in page.events {
                    peer_state
                        .cursor_machine
                        .on_subscription_evt(evt.into(), &mut peer_state.cursors_cmd_buf);
                }
            }
            ReplayPageOutcome::UnknownPart => {
                // The peer no longer (or does not yet) know this part. Keep the
                // route and retry slowly: a restarting peer re-creates its part
                // rows, and dropping the route here would tear it permanently.
                tracing::debug!(
                    peer_id = %peer_id,
                    ?target,
                    "replay page saw an unknown part; keeping route and retrying",
                );
                caught_up = true;
                delayed_retry = Some(Duration::from_secs(2));
            }
            ReplayPageOutcome::Unauthorized => {
                // Absent access rows cannot distinguish a revocation from a grant
                // that has not landed yet, and routes come from the embedder's
                // part set. Back off rather than tear the route down: dropping it
                // here would strand a part whose grant is still in flight.
                tracing::debug!(
                    peer_id = %peer_id,
                    ?target,
                    "peer may not read this replay route; backing off",
                );
                caught_up = true;
                delayed_retry = Some(UNAUTHORIZED_BACKOFF);
            }
        }
        self.drain_cursor_machine_cmds(peer_id);
        let next_target = self.refreshed_replay_target(peer_id, &target);
        match next_target {
            Some(next_target) => match delayed_retry {
                Some(delay) => {
                    self.schedule_replay_page(peer_id, next_target, caught_up, retry, delay);
                }
                None => {
                    self.spawn_replay_page(peer_id, next_target, caught_up);
                }
            },
            None => {
                // The route or its strategy is gone: stop asking for it.
                if let Some(peer_state) = self.peers.get_mut(&peer_id) {
                    peer_state.replay_pages.remove(&route);
                }
            }
        }
        self.update_peer_replay_done(peer_id);
    }

    fn handle_replay_page_err(
        &mut self,
        task_id: TaskId,
        retry: Retry,
        ReplayPageTaskError {
            peer_id,
            target,
            deets,
        }: ReplayPageTaskError,
    ) {
        let route = ReplayRoute::of(&target);
        let Some(peer_state) = self.peers.get_mut(&peer_id) else {
            assert!(self.all_seen_peer.contains(&peer_id), "fishy");
            return;
        };
        let Some(state) = peer_state
            .replay_pages
            .get(&route)
            .filter(|state| state.task_id == task_id)
        else {
            // The page was already superseded or the route was dropped.
            return;
        };
        let caught_up = state.caught_up;
        tracing::debug!(
            peer_id = %peer_id,
            ?target,
            retry = ?retry,
            deets = ?deets,
            "replay page failed; rescheduling",
        );
        self.schedule_replay_page(peer_id, target, caught_up, retry, Duration::from_secs(2));
        self.update_peer_replay_done(peer_id);
    }
    fn drain_cursor_machine_cmds(&mut self, peer_id: PeerKey) {
        let peer_state = self.peers.get_mut(&peer_id).expect(ERROR_UNRECONIZED);
        for cmd in peer_state.cursors_cmd_buf.drain(..) {
            trace!(peer_id = %peer_id, ?cmd,"processing cursor cmd");
            match cmd {
                CursorMachineCommand::PartIdle { part_id } => {
                    self.stat_machine.mark_peer_part_idle(peer_id, part_id);
                }
                CursorMachineCommand::SyncObj {
                    obj_id,
                    remote_payload,
                    parts,
                    cursor,
                } => {
                    tracing::trace!(
                        peer_id = %peer_id,
                        ?obj_id,
                        ?cursor,
                        part_count = parts.len(),
                        payload = !remote_payload.is_null(),
                        "machine SyncObj command",
                    );
                    // A null payload means "no advertised payload" (the store
                    // strips payloads from advance notices a peer lost access
                    // to), never an object payload: keep it absent so the sync
                    // task fetches instead of decoding it as content.
                    let remote_payload = (!remote_payload.is_null()).then_some(remote_payload);
                    let (cursors, part_hints, remote_payload) =
                        if let Some(mut worker) = peer_state.sync_workers.remove(&obj_id) {
                            let _state = self
                                .tasks
                                .cancel(worker.task_id)
                                .expect(ERROR_UNRECONIZED);

                            worker.part_hints.extend(parts.iter().copied());
                            worker.cursors.insert(cursor);
                            (
                                worker.cursors,
                                worker.part_hints,
                                remote_payload.or(worker.remote_payload),
                            )
                        } else {
                            (
                                [cursor].into(),
                                parts.iter().copied().collect(),
                                remote_payload,
                            )
                        };
                    // Cancel any in-flight removal for the hinted parts. If one
                    // was cancelled, the Sync task is deferred until the removal
                    // task's completion event lands: the zombie task may still
                    // evict the re-added parts, so the re-sync must not race it.
                    let mut removal_cancelled = false;
                    for part_id in &part_hints {
                        if Self::cancel_obj_removal_hint(
                            &mut self.tasks,
                            &mut peer_state.remove_workers,
                            &mut peer_state.pending_removals,
                            obj_id,
                            *part_id,
                        ) {
                            removal_cancelled = true;
                        }
                    }
                    if removal_cancelled {
                        let pending =
                            peer_state
                                .pending_removals
                                .entry(obj_id)
                                .or_insert_with(|| PendingRemoval {
                                    remaining_hints: default(),
                                    re_added_parts: default(),
                                    cursors: default(),
                                    remote_payload: None,
                                });
                        pending.cursors.extend(cursors.iter().copied());
                        pending.re_added_parts.extend(part_hints.iter().copied());
                        if remote_payload.is_some() {
                            pending.remote_payload = remote_payload;
                        }
                        // The Sync task is issued by `resume_pending_removal`
                        // once the cancelled removal task's completion lands.
                        // `continue` (not `return`): remaining commands in the
                        // batch (other objects) must still be processed.
                        continue;
                    }
                    let deets = SyncTaskDeets {
                        peer_id,
                        obj_id,
                        remote_payload: remote_payload.clone(),
                    };
                    let task_id = self.tasks.spawn(std::time::Instant::now(), TaskSeed::Sync(SyncTaskSeed {
                        kind: SyncTaskKind::Sync,
                        part_hints: part_hints.iter().copied().collect(),
                        deets: deets.clone(),
                    }));
                    peer_state.sync_workers.insert(
                        obj_id,
                        SyncWorkerState {
                            task_id,
                            cursors,
                            part_hints,
                            remote_payload,
                        },
                    );
                    for part_id in parts {
                        self.stat_machine
                            .mark_peer_part_cursor_active(peer_id, part_id);
                    }
                }
                CursorMachineCommand::SetPartCursor { part_id, cursor } => {
                    let part = peer_state.parts.get_mut(&part_id).expect(ERROR_UNRECONIZED);
                    match &mut part.strat {
                        PeerPartStrategy::Bucket(state) => {
                            state.replay_cursor = cursor;
                        }
                        PeerPartStrategy::Cursor(state) => {
                            // we only update the peer part cursor
                            // in the cursor phase
                            self.cmds.push_back((
                                Uuid::new_v4(),
                                BigSyncMachineCommand::SetPartCursor {
                                    peer_id,
                                    part_id,
                                    cursor,
                                },
                                None,
                                peer_id,
                            ));
                            state.replay_cursor = cursor;
                        }
                        // A removal can settle a cursor before the peer
                        // replay worker has promoted the part out of
                        // `Pending`. Replay is at-least-once, so dropping the
                        // advance here is safe: the replay picks up from its
                        // own cursor when it starts.
                        PeerPartStrategy::Pending(_) => {
                            tracing::debug!(
                                peer_id = %peer_id,
                                ?part_id,
                                cursor,
                                "cursor advanced while part still pending replay; ignoring"
                            );
                        }
                    }
                }
                CursorMachineCommand::RemoveObjFromParts {
                    obj_id,
                    part_id,
                    cursor,
                } => {
                    // Trim the hint from any in-flight sync task for this
                    // object; stop it if nothing is left to fetch.
                    let stop_task = peer_state.sync_workers.get_mut(&obj_id).and_then(|worker| {
                        worker.part_hints.remove(&part_id);
                        worker.part_hints.is_empty().then_some(worker.task_id)
                    });
                    if let Some(task_id) = stop_task {
                        let worker = peer_state
                            .sync_workers
                            .remove(&obj_id)
                            .expect(ERROR_UNRECONIZED);
                        assert_eq!(worker.task_id, task_id);
                        self.tasks.cancel(task_id).expect(ERROR_UNRECONIZED);
                    }
                    Self::schedule_obj_removal(
                        &mut self.tasks,
                        &mut peer_state.remove_workers,
                        peer_id,
                        obj_id,
                        part_id,
                        Some(cursor),
                    );
                }
            }
        }
    }

    /// Coalesce an object-removal request into a backend-executed
    /// [`SyncTaskKind::RemoveFromParts`] task. Membership mutation itself
    /// happens in the backend; the machine only replays the request.
    fn schedule_obj_removal(
        tasks: &mut Scheduler<TaskSeed>,
        remove_workers: &mut Map<ObjKey, SyncWorkerState>,
        peer_id: PeerKey,
        obj_id: ObjKey,
        part_id: PartKey,
        cursor: Option<CursorIndex>,
    ) {
        let (cursors, part_hints) = if let Some(mut worker) = remove_workers.remove(&obj_id) {
            let _state = tasks.cancel(worker.task_id).expect(ERROR_UNRECONIZED);
            if let Some(cursor) = cursor {
                worker.cursors.insert(cursor);
            }
            worker.part_hints.insert(part_id);
            (worker.cursors, worker.part_hints)
        } else {
            let mut cursors: Set<CursorIndex> = default();
            if let Some(cursor) = cursor {
                cursors.insert(cursor);
            }
            (cursors, [part_id].into())
        };
        let deets = SyncTaskDeets {
            peer_id,
            obj_id,
            remote_payload: None,
        };
        let task_id = tasks.spawn(std::time::Instant::now(), TaskSeed::Sync(SyncTaskSeed {
            kind: SyncTaskKind::RemoveFromParts,
            part_hints: part_hints.iter().copied().collect(),
            deets,
        }));
        remove_workers.insert(
            obj_id,
            SyncWorkerState {
                task_id,
                cursors,
                part_hints,
                remote_payload: None,
            },
        );
    }

    /// Drop `part_id` from any in-flight removal task for the object — the
    /// inverse of `schedule_obj_removal`, used when a later event re-adds the
    /// object to a part while its removal is still pending.
    ///
    /// The in-flight task is stopped cooperatively and the remaining hints
    /// recorded as pending; the re-removal is NOT spawned here. The zombie
    /// task may still be mid-removal (it only checks the cancel token after
    /// the backend call), so spawning new work immediately would let it evict
    /// the re-added part after the new task completes. Instead the deferred
    /// re-removal and re-sync are issued by `resume_pending_removal` when the
    /// cancelled task's completion event lands, which guarantees ordering.
    ///
    /// Returns true when an in-flight removal was cancelled; the caller must
    /// defer its own Sync task for the re-added parts until the removal
    /// completes.
    fn cancel_obj_removal_hint(
        tasks: &mut Scheduler<TaskSeed>,
        remove_workers: &mut Map<ObjKey, SyncWorkerState>,
        pending_removals: &mut Map<ObjKey, PendingRemoval>,
        obj_id: ObjKey,
        part_id: PartKey,
    ) -> bool {
        let Some(worker) = remove_workers.get_mut(&obj_id) else {
            return false;
        };
        if !worker.part_hints.remove(&part_id) {
            return false;
        }
        let pending = pending_removals
            .entry(obj_id)
            .or_insert_with(|| PendingRemoval {
                remaining_hints: default(),
                re_added_parts: default(),
                cursors: default(),
                remote_payload: None,
            });
        pending.cursors.extend(worker.cursors.iter().copied());
        if worker.part_hints.is_empty() {
            let worker = remove_workers.remove(&obj_id).expect(ERROR_UNRECONIZED);
            tasks.cancel(worker.task_id).expect(ERROR_UNRECONIZED);
        } else {
            let worker = remove_workers.remove(&obj_id).expect(ERROR_UNRECONIZED);
            let _state = tasks.cancel(worker.task_id).expect(ERROR_UNRECONIZED);
            pending
                .remaining_hints
                .extend(worker.part_hints.iter().copied());
        }
        true
    }

    /// Issue the deferred re-removal (remaining hints) and re-sync (re-added
    /// parts) for an object whose in-flight removal was cancelled by a re-add.
    /// Called when the cancelled removal task's completion event lands, so the
    /// zombie task is guaranteed done and cannot race the new work.
    fn resume_pending_removal(&mut self, peer_id: PeerKey, obj_id: ObjKey) {
        let Some(peer_state) = self.peers.get_mut(&peer_id) else {
            return;
        };
        let Some(mut pending) = peer_state.pending_removals.remove(&obj_id) else {
            return;
        };
        pending
            .remaining_hints
            .retain(|part_id| peer_state.parts.contains_key(part_id));
        pending
            .re_added_parts
            .retain(|part_id| peer_state.parts.contains_key(part_id));
        if !pending.remaining_hints.is_empty() {
            let deets = SyncTaskDeets {
                peer_id,
                obj_id,
                remote_payload: None,
            };
            let task_id = self.tasks.spawn(std::time::Instant::now(), TaskSeed::Sync(SyncTaskSeed {
                kind: SyncTaskKind::RemoveFromParts,
                part_hints: pending.remaining_hints.iter().copied().collect(),
                deets,
            }));
            peer_state.remove_workers.insert(
                obj_id,
                SyncWorkerState {
                    task_id,
                    cursors: pending.cursors.clone(),
                    part_hints: pending.remaining_hints,
                    remote_payload: None,
                },
            );
        }
        if !pending.re_added_parts.is_empty() {
            let deets = SyncTaskDeets {
                peer_id,
                obj_id,
                remote_payload: pending.remote_payload.clone(),
            };
            let task_id = self.tasks.spawn(std::time::Instant::now(), TaskSeed::Sync(SyncTaskSeed {
                kind: SyncTaskKind::Sync,
                part_hints: pending.re_added_parts.iter().copied().collect(),
                deets,
            }));
            peer_state.sync_workers.insert(
                obj_id,
                SyncWorkerState {
                    task_id,
                    cursors: pending.cursors,
                    part_hints: pending.re_added_parts,
                    remote_payload: pending.remote_payload,
                },
            );
        }
    }
}

// bucket support
impl BigSyncMachine {
    fn drain_bucket_machine_cmds(&mut self, peer_id: PeerKey) {
        let peer_state = self.peers.get_mut(&peer_id).expect(ERROR_UNRECONIZED);
        // NOTE: ordering is important here, execute commands
        // in given order
        let mut refresh_peer_replaly = false;
        for cmd in peer_state.bucket_cmd_buf.drain(..) {
            trace!(peer_id = %peer_id, ?cmd, "processing bucket cmd");
            match cmd {
                BucketMachineCommand::SyncObj {
                    obj_id,
                    part_id,
                    remote_payload,
                } => {
                    let (cursors, part_hints, remote_payload) =
                        if let Some(mut worker) = peer_state.sync_workers.remove(&obj_id) {
                            let _state = self
                                .tasks
                                .cancel(worker.task_id)
                                .expect(ERROR_UNRECONIZED);

                            worker.part_hints.insert(part_id);
                            (
                                worker.cursors,
                                worker.part_hints,
                                remote_payload.or(worker.remote_payload),
                            )
                        } else {
                            (default(), [part_id].into(), remote_payload)
                        };
                    // Cancel any in-flight removal for the hinted parts. If
                    // one was cancelled, the Sync task is deferred until the
                    // removal task's completion event lands: the zombie task
                    // may still evict the re-added parts, so the re-sync must
                    // not race it.
                    let mut removal_cancelled = false;
                    for part_id in &part_hints {
                        if Self::cancel_obj_removal_hint(
                            &mut self.tasks,
                            &mut peer_state.remove_workers,
                            &mut peer_state.pending_removals,
                            obj_id,
                            *part_id,
                        ) {
                            removal_cancelled = true;
                        }
                    }
                    if removal_cancelled {
                        let pending =
                            peer_state
                                .pending_removals
                                .entry(obj_id)
                                .or_insert_with(|| PendingRemoval {
                                    remaining_hints: default(),
                                    re_added_parts: default(),
                                    cursors: default(),
                                    remote_payload: None,
                                });
                        pending.cursors.extend(cursors.iter().copied());
                        pending.re_added_parts.extend(part_hints.iter().copied());
                        if remote_payload.is_some() {
                            pending.remote_payload = remote_payload;
                        }
                        // The Sync task is issued by `resume_pending_removal`
                        // once the cancelled removal task's completion lands.
                        continue;
                    }
                    let deets = SyncTaskDeets {
                        peer_id,
                        obj_id,
                        remote_payload: remote_payload.clone(),
                    };
                    let task_id = self.tasks.spawn(std::time::Instant::now(), TaskSeed::Sync(SyncTaskSeed {
                        kind: SyncTaskKind::Sync,
                        part_hints: part_hints.iter().copied().collect(),
                        deets: deets.clone(),
                    }));
                    peer_state.sync_workers.insert(
                        obj_id,
                        SyncWorkerState {
                            task_id,
                            cursors,
                            part_hints,
                            remote_payload,
                        },
                    );
                }
                BucketMachineCommand::RemoveObjFromParts { obj_id, part_id } => {
                    let stop_task = peer_state.sync_workers.get_mut(&obj_id).and_then(|worker| {
                        worker.part_hints.remove(&part_id);
                        worker.part_hints.is_empty().then_some(worker.task_id)
                    });
                    if let Some(task_id) = stop_task {
                        let worker = peer_state
                            .sync_workers
                            .remove(&obj_id)
                            .expect(ERROR_UNRECONIZED);
                        assert_eq!(worker.task_id, task_id);
                        self.tasks.cancel(task_id).expect(ERROR_UNRECONIZED);
                    }
                    Self::schedule_obj_removal(
                        &mut self.tasks,
                        &mut peer_state.remove_workers,
                        peer_id,
                        obj_id,
                        part_id,
                        None,
                    );
                }
                BucketMachineCommand::ListBuckets {
                    offset,
                    since,
                    part_id,
                    working_level,
                } => {
                    let task = ListBucketsTask {
                        peer_id,
                        part_id,
                        offset,
                        since,
                        working_level,
                    };
                    let deets = MachineTaskDeets::ListBuckets(task.clone());
                    let part = peer_state.parts.get_mut(&part_id).expect(ERROR_UNRECONIZED);
                    let PeerPartStrategy::Bucket(state) = &mut part.strat else {
                        unreachable!()
                    };
                    let task_id = self.tasks.spawn(std::time::Instant::now(), TaskSeed::Machine(deets));
                    let old = state.active_list_tasks.insert(task_id, task);
                    assert!(old.is_none(), "fishy");
                }
                BucketMachineCommand::LeafBuckets {
                    since,
                    buckets,
                    part_id,
                } => {
                    let task = LeafBucketsTask {
                        peer_id,
                        part_id,
                        since,
                        buckets,
                    };
                    let deets = MachineTaskDeets::LeafBuckets(task.clone());
                    let part = peer_state.parts.get_mut(&part_id).expect(ERROR_UNRECONIZED);
                    let PeerPartStrategy::Bucket(state) = &mut part.strat else {
                        unreachable!()
                    };
                    let task_id = self.tasks.spawn(std::time::Instant::now(), TaskSeed::Machine(deets));
                    let old = state.active_leaf_tasks.insert(task_id, task);
                    assert!(old.is_none(), "fishy");
                }
                BucketMachineCommand::UpgradeToCursor { part_id } => {
                    let Some(PeerPartState {
                        strat: PeerPartStrategy::Bucket(old),
                    }) = peer_state.parts.remove(&part_id)
                    else {
                        unreachable!()
                    };
                    assert!(old.active_list_tasks.is_empty());
                    assert!(old.active_leaf_tasks.is_empty());
                    self.cmds.push_back((
                        Uuid::new_v4(),
                        BigSyncMachineCommand::SetPartCursor {
                            peer_id,
                            part_id,
                            cursor: old.replay_cursor,
                        },
                        None,
                        peer_id,
                    ));
                    peer_state.parts.insert(
                        part_id,
                        PeerPartState {
                            strat: PeerPartStrategy::Cursor(CursorState {
                                replay_cursor: old.replay_cursor,
                            }),
                        },
                    );
                    self.stat_machine
                        .mark_peer_part_only_cursor_strat(peer_id, part_id, true);
                    refresh_peer_replaly = true;
                }
            }
        }
        if refresh_peer_replaly {
            self.refresh_peer_replay_worker(peer_id, false);
        }
    }

    #[tracing::instrument(
        skip_all,
        fields(
            peer_id = %peer_id,
            task_id = %task_id,
        )
    )]
    fn handle_list_buckets_result(
        &mut self,
        task_id: TaskId,
        ListBucketsResult {
            peer_id,
            part_id,
            filtered_buckets,
        }: ListBucketsResult,
    ) {
        let mut bucket_cmd_buf = Vec::new();
        {
            let Some(peer_state) = self.peers.get_mut(&peer_id) else {
                assert!(self.all_seen_peer.contains(&peer_id), "fishy");
                return;
            };
            let Some(part_state) = peer_state.parts.get_mut(&part_id) else {
                return;
            };
            let PeerPartStrategy::Bucket(strat) = &mut part_state.strat else {
                return;
            };
            let Some(task) = strat.active_list_tasks.remove(&task_id) else {
                return;
            };
            assert_eq!(task.peer_id, peer_id, "fishy");
            assert_eq!(task.part_id, part_id, "fishy");
            strat
                .machine
                .on_bucket_page(filtered_buckets, &mut bucket_cmd_buf);
        }
        let peer_state = self.peers.get_mut(&peer_id).expect(ERROR_UNRECONIZED);
        peer_state.bucket_cmd_buf.extend(bucket_cmd_buf);
        tracing::debug!(
            peer_id = %peer_id,
            part_id = %part_id,
            filtered_bucket_count = peer_state.bucket_cmd_buf.len(),
            "list buckets result"
        );
        self.drain_bucket_machine_cmds(peer_id);
    }

    #[tracing::instrument(
        skip_all,
        fields(
            peer_id = %peer_id,
            task_id = %task_id,
        )
    )]
    fn handle_leaf_buckets_result(
        &mut self,
        task_id: TaskId,
        LeafBucketsResult {
            peer_id,
            filtered_objs,
        }: LeafBucketsResult,
    ) {
        let mut bucket_cmd_buf = Vec::new();
        let mut handled = false;
        {
            let Some(peer_state) = self.peers.get_mut(&peer_id) else {
                assert!(self.all_seen_peer.contains(&peer_id), "fishy");
                return;
            };
            for part_state in peer_state.parts.values_mut() {
                let PeerPartStrategy::Bucket(strat) = &mut part_state.strat else {
                    continue;
                };
                let Some(task) = strat.active_leaf_tasks.remove(&task_id) else {
                    continue;
                };
                assert_eq!(task.peer_id, peer_id, "fishy");
                let _part_id = task.part_id;
                strat
                    .machine
                    .on_obj_page(filtered_objs, &mut bucket_cmd_buf);
                handled = true;
                break;
            }
        }
        if !handled {
            return;
        }
        let peer_state = self.peers.get_mut(&peer_id).expect(ERROR_UNRECONIZED);
        peer_state.bucket_cmd_buf.extend(bucket_cmd_buf);
        tracing::debug!(
            peer_id = %peer_id,
            filtered_cmd_count = peer_state.bucket_cmd_buf.len(),
            "leaf buckets result"
        );
        self.drain_bucket_machine_cmds(peer_id);
    }

    fn handle_list_buckets_err(
        &mut self,
        task_id: TaskId,
        retry: Retry,
        ListBucketsTaskError {
            peer_id,
            part_id,
            _deets: _,
        }: ListBucketsTaskError,
    ) {
        let Some(peer_state) = self.peers.get_mut(&peer_id) else {
            assert!(self.all_seen_peer.contains(&peer_id), "fishy");
            return;
        };
        let Some(part_state) = peer_state.parts.get_mut(&part_id) else {
            return;
        };
        let PeerPartStrategy::Bucket(strat) = &mut part_state.strat else {
            return;
        };
        let Some(task) = strat.active_list_tasks.remove(&task_id) else {
            return;
        };
        assert_eq!(task.peer_id, peer_id, "fishy");
        let task_id = self.tasks.spawn_delayed(
            TaskSeed::Machine(MachineTaskDeets::ListBuckets(task.clone())),
            retry,
            Duration::from_secs(2),
            std::time::Instant::now(),
        );
        let old = strat.active_list_tasks.insert(task_id, task);
        assert!(old.is_none(), "fishy");
    }

    fn handle_leaf_buckets_err(
        &mut self,
        task_id: TaskId,
        retry: Retry,
        LeafBucketsTaskError {
            peer_id,
            part_id,
            _deets: _,
        }: LeafBucketsTaskError,
    ) {
        let Some(peer_state) = self.peers.get_mut(&peer_id) else {
            assert!(self.all_seen_peer.contains(&peer_id), "fishy");
            return;
        };
        let Some(part_state) = peer_state.parts.get_mut(&part_id) else {
            return;
        };
        let PeerPartStrategy::Bucket(strat) = &mut part_state.strat else {
            return;
        };
        let Some(task) = strat.active_leaf_tasks.remove(&task_id) else {
            return;
        };
        assert_eq!(task.peer_id, peer_id, "fishy");
        let task_id = self.tasks.spawn_delayed(
            TaskSeed::Machine(MachineTaskDeets::LeafBuckets(task.clone())),
            retry,
            Duration::from_secs(2),
            std::time::Instant::now(),
        );
        let old = strat.active_leaf_tasks.insert(task_id, task);
        assert!(old.is_none(), "fishy");
    }
}

// sync support
impl BigSyncMachine {
    fn handle_remove_completed(&mut self, evt: RemoveCompletedEvent) {
        // A removal whose worker is gone was cancelled by a re-add; the
        // deferred re-removal and re-sync are issued now that the zombie task
        // is done (its completion event only lands after the removal
        // finished).
        let cancelled = self
            .peers
            .get(&evt.peer_id)
            .and_then(|peer_state| peer_state.remove_workers.get(&evt.obj_id))
            .is_none();
        if cancelled {
            self.resume_pending_removal(evt.peer_id, evt.obj_id);
            return;
        }
        let (cursors, part_hints) = {
            let Some(peer_state) = self.peers.get_mut(&evt.peer_id) else {
                assert!(self.all_seen_peer.contains(&evt.peer_id), "fishy");
                return;
            };
            let Some(worker) = peer_state.remove_workers.get(&evt.obj_id) else {
                return;
            };
            if worker.task_id != evt.task_id {
                return;
            }
            (worker.cursors.clone(), worker.part_hints.clone())
        };
        tracing::debug!(
            peer_id = %evt.peer_id,
            task_id = evt.task_id,
            obj_id = %evt.obj_id,
            part_hints = ?part_hints,
            "backend object-membership removal completed",
        );
        let Some(peer_state) = self.peers.get_mut(&evt.peer_id) else {
            return;
        };
        peer_state.remove_workers.remove(&evt.obj_id);
        for &cursor in &cursors {
            peer_state.cursor_machine.on_obj_sync_job_evt(
                evt.obj_id,
                cursor,
                CursorJobCompletionKind::Membership,
                &mut peer_state.cursors_cmd_buf,
            );
        }
        self.drain_cursor_machine_cmds(evt.peer_id);
        self.drain_bucket_machine_cmds(evt.peer_id);
        self.resume_pending_removal(evt.peer_id, evt.obj_id);
    }

    fn handle_remove_failed(&mut self, evt: RemoveFailedEvent, retry: Retry) {
        // A removal whose worker is gone was cancelled by a re-add; the
        // deferred re-removal and re-sync are issued now that the zombie task
        // is done (its completion event only lands after the removal
        // finished).
        let cancelled = self
            .peers
            .get(&evt.peer_id)
            .and_then(|peer_state| peer_state.remove_workers.get(&evt.obj_id))
            .is_none();
        if cancelled {
            self.resume_pending_removal(evt.peer_id, evt.obj_id);
            return;
        }
        let part_hints = {
            let Some(peer_state) = self.peers.get(&evt.peer_id) else {
                assert!(self.all_seen_peer.contains(&evt.peer_id), "fishy");
                return;
            };
            let Some(worker) = peer_state.remove_workers.get(&evt.obj_id) else {
                return;
            };
            if worker.task_id != evt.task_id {
                return;
            }
            worker.part_hints.clone()
        };
        tracing::debug!(
            peer_id = %evt.peer_id,
            task_id = evt.task_id,
            obj_id = %evt.obj_id,
            retry = ?retry,
            error = %evt.err,
            part_hints = ?part_hints,
            "big sync object-removal task failed; rescheduling",
        );
        let Some(peer_state) = self.peers.get_mut(&evt.peer_id) else {
            return;
        };
        if let Some(worker) = peer_state.remove_workers.get_mut(&evt.obj_id) {
            let task_id = self.tasks.spawn_delayed(
                TaskSeed::Sync(SyncTaskSeed {
                    kind: SyncTaskKind::RemoveFromParts,
                    part_hints: part_hints.clone(),
                    deets: SyncTaskDeets {
                        peer_id: evt.peer_id,
                        obj_id: evt.obj_id,
                        remote_payload: None,
                    },
                }),
                retry,
                Duration::from_secs(2),
                std::time::Instant::now(),
            );
            worker.task_id = task_id;
        }
    }

    fn handle_sync_completed(&mut self, evt: SyncCompletedEvent) {
        let completion = evt.completion;
        let Some(peer_state) = self.peers.get(&evt.peer_id) else {
            assert!(self.all_seen_peer.contains(&evt.peer_id), "fishy");
            return;
        };
        let Some(worker) = peer_state.sync_workers.get(&completion.obj_id) else {
            return;
        };
        // FIXME: consider progressing cursors/parts
        // still on the incoming event?
        if worker.task_id != evt.task_id {
            return;
        }
        tracing::debug!(
            peer_id = %evt.peer_id,
            task_id = evt.task_id,
            obj_id = %completion.obj_id,
            deets = ?completion.deets,
            cursors = ?worker.cursors,
            part_hints = ?worker.part_hints,
            "big sync object task completed",
        );
        let (completion, part_hints) = {
            let Some(peer_state) = self.peers.get_mut(&evt.peer_id) else {
                return;
            };
            let Some(worker) = peer_state.sync_workers.remove(&completion.obj_id) else {
                return;
            };
            let completion = SyncJobEvt {
                obj_id: completion.obj_id,
                cursors: worker.cursors,
                deets: completion.deets,
            };
            let mut part_hints = worker.part_hints.clone();
            let mut stale_part_hints = Vec::new();
            for part_id in &part_hints {
                let Some(part) = peer_state.parts.get_mut(part_id) else {
                    stale_part_hints.push(*part_id);
                    continue;
                };
                match &mut part.strat {
                    PeerPartStrategy::Pending(_) => stale_part_hints.push(*part_id),
                    PeerPartStrategy::Bucket(state) => {
                        state
                            .machine
                            .on_obj_sync_completed(&completion, &mut peer_state.bucket_cmd_buf);
                    }
                    PeerPartStrategy::Cursor(_) => {}
                }
            }
            for part_id in stale_part_hints {
                // TEMP-DIAGNOSTIC: this path drops the sync completion; if the
                // cursor job for the obj still has other pending parts, their
                // waiters may never resolve (cursor_active stuck).
                tracing::warn!(?part_id, obj_id = %completion.obj_id, "discarding sync completion for stale peer part");
                peer_state.cursor_machine.remove_part(part_id);
                part_hints.remove(&part_id);
            }
            for &cursor in &completion.cursors {
                peer_state.cursor_machine.on_obj_sync_job_evt(
                    completion.obj_id,
                    cursor,
                    CursorJobCompletionKind::Sync,
                    &mut peer_state.cursors_cmd_buf,
                );
            }
            (completion, part_hints)
        };
        for part_id in &part_hints {
            self.stat_machine
                .record_object_synced(evt.peer_id, *part_id, completion.obj_id);
        }
        self.stat_machine
            .stat_evts
            .push(SyncStatEvent::ObjectSynced {
                peer_id: evt.peer_id,
                obj_id: completion.obj_id,
            });
        self.drain_cursor_machine_cmds(evt.peer_id);
        self.drain_bucket_machine_cmds(evt.peer_id);
    }

    fn handle_sync_failed(&mut self, evt: SyncFailedEvent, retry: Retry) {
        let (deets, part_hints) = {
            let Some(peer_state) = self.peers.get(&evt.peer_id) else {
                assert!(self.all_seen_peer.contains(&evt.peer_id), "fishy");
                return;
            };
            let Some(worker) = peer_state.sync_workers.get(&evt.obj_id) else {
                return;
            };
            if worker.task_id != evt.task_id {
                return;
            }
            (
                SyncTaskDeets {
                    peer_id: evt.peer_id,
                    obj_id: evt.obj_id,
                    remote_payload: worker.remote_payload.clone(),
                },
                worker.part_hints.clone(),
            )
        };
        // TEMP-DIAGNOSTIC: promote to warn — a permanently-failing sync task
        // leaves its cursor slot pending forever (cursor_active stuck).
        tracing::warn!(
            peer_id = %evt.peer_id,
            task_id = evt.task_id,
            obj_id = %evt.obj_id,
            retry = ?retry,
            error = %evt.err,
            part_hints = ?part_hints,
            "big sync object task failed; rescheduling",
        );
        let Some(peer_state) = self.peers.get_mut(&evt.peer_id) else {
            return;
        };
        if let Some(worker) = peer_state.sync_workers.get_mut(&evt.obj_id) {
            let task_id = self.tasks.spawn_delayed(
                TaskSeed::Sync(SyncTaskSeed {
                    kind: SyncTaskKind::Sync,
                    part_hints: part_hints.clone(),
                    deets,
                }),
                retry,
                Duration::from_secs(2),
                std::time::Instant::now(),
            );
            worker.task_id = task_id;
        }
    }

    fn handle_sync_stale(&mut self, evt: SyncStaleEvent, retry: Retry) {
        let (deets, part_hints) = {
            let Some(peer_state) = self.peers.get(&evt.peer_id) else {
                assert!(self.all_seen_peer.contains(&evt.peer_id), "fishy");
                return;
            };
            let Some(worker) = peer_state.sync_workers.get(&evt.obj_id) else {
                return;
            };
            if worker.task_id != evt.task_id {
                return;
            }
            (
                SyncTaskDeets {
                    peer_id: evt.peer_id,
                    obj_id: evt.obj_id,
                    remote_payload: worker.remote_payload.clone(),
                },
                worker.part_hints.clone(),
            )
        };
        tracing::debug!(
            peer_id = %evt.peer_id,
            task_id = evt.task_id,
            obj_id = %evt.obj_id,
            retry = ?retry,
            part_hints = ?part_hints,
            "big sync object task became stale; rescheduling",
        );

        let Some(peer_state) = self.peers.get_mut(&evt.peer_id) else {
            return;
        };
        if let Some(worker) = peer_state.sync_workers.get_mut(&evt.obj_id) {
            let task_id = self.tasks.spawn_delayed(
                TaskSeed::Sync(SyncTaskSeed {
                    kind: SyncTaskKind::Sync,
                    part_hints: part_hints.clone(),
                    deets,
                }),
                retry,
                Duration::from_secs(2),
                std::time::Instant::now(),
            );
            worker.task_id = task_id;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A waiter registered for a peer+part must NOT remain stranded after that
    /// peer is removed. `SyncStatMachine::remove_peer` cleans up the peer and
    /// satisfies waiters when their last remaining peer is removed.
    #[test]
    fn full_sync_waiter_does_not_strand_on_peer_removal() {
        let mut stat = SyncStatMachine::default();

        let peer = PeerKey::random();
        let part = PartKey::random();

        // Register the peer and its part.
        stat.set_peer(peer, [part].into_iter());

        // Register a waiter needing that peer + part.
        stat.add_full_sync_waiter(1, [peer].into(), [part].into());

        // Remove the peer -- the waiter must be cleaned up.
        stat.remove_peer(peer);

        let stranded = stat.debug_full_sync_waiters();
        assert!(
            stranded.is_empty(),
            "waiter {} still stranded after peer removal: need_set={:?}",
            1,
            stranded.get(&1),
        );
    }

    #[test]
    fn removal_cancels_retrying_sync_task_when_its_last_part_is_removed() {
        let mut machine = BigSyncMachine::default();
        let peer = PeerKey::random();
        let part = PartKey::random();
        let obj = ObjKey::random();
        machine.handle_evt(BigSyncEvent::SetPeer(SetPeerEvent {
            peer_id: peer,
            parts: [part].into(),
            objects: Set::new(),
        }));

        {
            let peer_state = machine.peers.get_mut(&peer).expect(ERROR_UNRECONIZED);
            peer_state.cursor_machine.on_subscription_evt(
                crate::rpc::SubEvent::Added(crate::rpc::ObjAddedToPart {
                    cursor: 1,
                    part_id: part,
                    obj_id: obj,
                    payload: serde_json::json!({"head": 1}),
                }),
                &mut peer_state.cursors_cmd_buf,
            );
        }
        machine.drain_cursor_machine_cmds(peer);
        let task_id = machine
            .peers
            .get(&peer)
            .and_then(|peer_state| peer_state.sync_workers.get(&obj))
            .map(|worker| worker.task_id)
            .expect("addition must start an object sync task");

        {
            let peer_state = machine.peers.get_mut(&peer).expect(ERROR_UNRECONIZED);
            peer_state.cursor_machine.on_subscription_evt(
                crate::rpc::SubEvent::Removed(crate::rpc::ObjRemovedFromPart {
                    cursor: 2,
                    part_id: part,
                    obj_id: obj,
                }),
                &mut peer_state.cursors_cmd_buf,
            );
        }
        machine.drain_cursor_machine_cmds(peer);

        // The sync task is cancelled…
        assert!(
            !machine
                .peers
                .get(&peer)
                .expect(ERROR_UNRECONIZED)
                .sync_workers
                .contains_key(&obj)
        );
        assert!(machine.drain_stop_queue().any(|stopped| stopped == task_id));
        // …and a backend-executed removal task replaces it.
        let removal = machine
            .peers
            .get(&peer)
            .expect(ERROR_UNRECONIZED)
            .remove_workers
            .get(&obj)
            .expect("removal must schedule a RemoveFromParts task");
        assert_ne!(removal.task_id, task_id);
        assert!(removal.part_hints.contains(&part));
    }

    #[test]
    fn removal_keeps_sync_task_for_other_visible_parts() {
        let mut machine = BigSyncMachine::default();
        let peer = PeerKey::random();
        let removed_part = PartKey::random();
        let remaining_part = PartKey::random();
        let obj = ObjKey::random();
        machine.handle_evt(BigSyncEvent::SetPeer(SetPeerEvent {
            peer_id: peer,
            parts: [removed_part, remaining_part].into(),
            objects: Set::new(),
        }));
        {
            let peer_state = machine.peers.get_mut(&peer).expect(ERROR_UNRECONIZED);
            for part_id in [removed_part, remaining_part] {
                peer_state
                    .parts
                    .get_mut(&part_id)
                    .expect(ERROR_UNRECONIZED)
                    .strat = PeerPartStrategy::Cursor(CursorState { replay_cursor: 0 });
            }
        }

        {
            let peer_state = machine.peers.get_mut(&peer).expect(ERROR_UNRECONIZED);
            peer_state.cursor_machine.on_subscription_evt(
                crate::rpc::SubEvent::Changed(crate::rpc::ObjChanged {
                    cursor: 1,
                    part_ids: vec![removed_part, remaining_part],
                    obj_id: obj,
                    payload: serde_json::json!({"head": 1}),
                }),
                &mut peer_state.cursors_cmd_buf,
            );
        }
        machine.drain_cursor_machine_cmds(peer);
        let task_id = machine.peers[&peer].sync_workers[&obj].task_id;

        {
            let peer_state = machine.peers.get_mut(&peer).expect(ERROR_UNRECONIZED);
            peer_state.cursor_machine.on_subscription_evt(
                crate::rpc::SubEvent::Removed(crate::rpc::ObjRemovedFromPart {
                    cursor: 2,
                    part_id: removed_part,
                    obj_id: obj,
                }),
                &mut peer_state.cursors_cmd_buf,
            );
        }
        machine.drain_cursor_machine_cmds(peer);

        let worker = &machine.peers[&peer].sync_workers[&obj];
        assert_eq!(worker.task_id, task_id);
        assert_eq!(worker.part_hints, [remaining_part].into());
        assert!(!machine.drain_stop_queue().any(|stopped| stopped == task_id));
    }

    #[test]
    fn partial_removal_trim_restarts_task_with_remaining_hints() {
        let mut machine = BigSyncMachine::default();
        let peer = PeerKey::random();
        let part_a = PartKey::random();
        let part_b = PartKey::random();
        let obj = ObjKey::random();
        machine.handle_evt(BigSyncEvent::SetPeer(SetPeerEvent {
            peer_id: peer,
            parts: [part_a, part_b].into(),
            objects: Set::new(),
        }));

        {
            let peer_state = machine.peers.get_mut(&peer).expect(ERROR_UNRECONIZED);
            BigSyncMachine::schedule_obj_removal(
                &mut machine.tasks,
                &mut peer_state.remove_workers,
                peer,
                obj,
                part_a,
                Some(1),
            );
            BigSyncMachine::schedule_obj_removal(
                &mut machine.tasks,
                &mut peer_state.remove_workers,
                peer,
                obj,
                part_b,
                Some(2),
            );
        }
        let first_task = machine.peers[&peer].remove_workers[&obj].task_id;
        let spawned: Vec<_> = machine.drain_sync_spawn_queue().collect();
        assert_eq!(spawned.len(), 1);
        assert_eq!(spawned[0].part_hints, [part_a, part_b].into());

        // Trim one hint: the in-flight task is stopped and the remaining
        // hint set is recorded as pending. The re-removal is NOT spawned
        // here — the zombie task may still be mid-removal (it only checks
        // the cancel token after the backend call), so spawning new work
        // immediately would let it evict the re-added part. The deferred
        // re-removal is issued when the zombie's completion event lands.
        {
            let peer_state = machine.peers.get_mut(&peer).expect(ERROR_UNRECONIZED);
            assert!(BigSyncMachine::cancel_obj_removal_hint(
                &mut machine.tasks,
                &mut peer_state.remove_workers,
                &mut peer_state.pending_removals,
                obj,
                part_a,
            ));
        }
        assert!(
            !machine.peers[&peer].remove_workers.contains_key(&obj),
            "cancelled removal worker must be removed from remove_workers"
        );
        let pending = &machine.peers[&peer].pending_removals[&obj];
        assert_eq!(pending.remaining_hints, [part_b].into());
        assert!(
            machine
                .drain_stop_queue()
                .any(|stopped| stopped == first_task)
        );
        assert!(
            machine.drain_sync_spawn_queue().next().is_none(),
            "re-removal must be deferred until the zombie completes"
        );

        // The zombie's completion lands: the deferred re-removal is issued
        // with the remaining hint set.
        machine.handle_evt(BigSyncEvent::RemoveCompleted(RemoveCompletedEvent {
            task_id: first_task,
            peer_id: peer,
            obj_id: obj,
        }));
        let worker = &machine.peers[&peer].remove_workers[&obj];
        assert_eq!(worker.part_hints, [part_b].into());
        assert_ne!(
            worker.task_id, first_task,
            "deferred re-removal must spawn a fresh task"
        );
        let respawned: Vec<_> = machine.drain_sync_spawn_queue().collect();
        assert_eq!(respawned.len(), 1);
        assert_eq!(respawned[0].kind, SyncTaskKind::RemoveFromParts);
        assert_eq!(respawned[0].part_hints, [part_b].into());
        assert!(
            !machine.peers[&peer].pending_removals.contains_key(&obj),
            "pending removal must be consumed by the resume"
        );
    }

    #[test]
    fn readd_after_removal_triggers_resync() {
        let mut machine = BigSyncMachine::default();
        let peer = PeerKey::random();
        let part = PartKey::random();
        let obj = ObjKey::random();
        machine.handle_evt(BigSyncEvent::SetPeer(SetPeerEvent {
            peer_id: peer,
            parts: [part].into(),
            objects: Set::new(),
        }));

        // Schedule a removal for the object in the part.
        {
            let peer_state = machine.peers.get_mut(&peer).expect(ERROR_UNRECONIZED);
            BigSyncMachine::schedule_obj_removal(
                &mut machine.tasks,
                &mut peer_state.remove_workers,
                peer,
                obj,
                part,
                Some(1),
            );
        }
        let removal_task = machine.peers[&peer].remove_workers[&obj].task_id;
        machine.drain_sync_spawn_queue();

        // The object is re-added to the same part: the removal hint is
        // cancelled and the re-sync is deferred until the zombie removal
        // task's completion lands (it may still be mid-removal and could
        // evict the re-added part).
        {
            let peer_state = machine.peers.get_mut(&peer).expect(ERROR_UNRECONIZED);
            peer_state.cursor_machine.on_subscription_evt(
                crate::rpc::SubEvent::Added(crate::rpc::ObjAddedToPart {
                    cursor: 2,
                    part_id: part,
                    obj_id: obj,
                    payload: serde_json::json!({"head": 2}),
                }),
                &mut peer_state.cursors_cmd_buf,
            );
        }
        machine.drain_cursor_machine_cmds(peer);

        // The removal worker is gone (its only hint was cancelled)…
        assert!(!machine.peers[&peer].remove_workers.contains_key(&obj));
        assert!(
            machine
                .drain_stop_queue()
                .any(|stopped| stopped == removal_task)
        );
        // …and the re-sync is deferred, not spawned yet.
        assert!(
            machine.drain_sync_spawn_queue().next().is_none(),
            "re-sync must be deferred until the zombie removal completes"
        );
        let pending = &machine.peers[&peer].pending_removals[&obj];
        assert_eq!(pending.re_added_parts, [part].into());

        // The zombie's completion lands: the deferred re-sync is issued.
        machine.handle_evt(BigSyncEvent::RemoveCompleted(RemoveCompletedEvent {
            task_id: removal_task,
            peer_id: peer,
            obj_id: obj,
        }));
        let sync_task = machine.peers[&peer].sync_workers[&obj].task_id;
        let spawned: Vec<_> = machine.drain_sync_spawn_queue().collect();
        assert_eq!(spawned.len(), 1);
        assert_eq!(spawned[0].kind, SyncTaskKind::Sync);
        assert_eq!(spawned[0].part_hints, [part].into());
        assert_ne!(sync_task, removal_task);
        assert!(
            !machine.peers[&peer].pending_removals.contains_key(&obj),
            "pending removal must be consumed by the resume"
        );
    }

    #[test]
    fn removed_peer_part_cancels_deferred_readd_before_removal_completes() {
        let mut machine = BigSyncMachine::default();
        let peer = PeerKey::random();
        let part = PartKey::random();
        let obj = ObjKey::random();
        machine.handle_evt(BigSyncEvent::SetPeer(SetPeerEvent {
            peer_id: peer,
            parts: [part].into(),
            objects: Set::new(),
        }));

        {
            let peer_state = machine.peers.get_mut(&peer).expect(ERROR_UNRECONIZED);
            BigSyncMachine::schedule_obj_removal(
                &mut machine.tasks,
                &mut peer_state.remove_workers,
                peer,
                obj,
                part,
                Some(1),
            );
        }
        let removal_task = machine.peers[&peer].remove_workers[&obj].task_id;
        machine.drain_sync_spawn_queue();

        {
            let peer_state = machine.peers.get_mut(&peer).expect(ERROR_UNRECONIZED);
            peer_state.cursor_machine.on_subscription_evt(
                crate::rpc::SubEvent::Added(crate::rpc::ObjAddedToPart {
                    cursor: 2,
                    part_id: part,
                    obj_id: obj,
                    payload: serde_json::json!({"head": 2}),
                }),
                &mut peer_state.cursors_cmd_buf,
            );
        }
        machine.drain_cursor_machine_cmds(peer);
        assert_eq!(
            machine.peers[&peer].pending_removals[&obj].re_added_parts,
            [part].into()
        );

        machine.handle_evt(BigSyncEvent::SetPeer(SetPeerEvent {
            peer_id: peer,
            parts: Set::new(),
            objects: Set::new(),
        }));
        assert!(machine.peers[&peer].pending_removals.is_empty());

        machine.handle_evt(BigSyncEvent::RemoveCompleted(RemoveCompletedEvent {
            task_id: removal_task,
            peer_id: peer,
            obj_id: obj,
        }));
        assert!(machine.peers[&peer].sync_workers.is_empty());
        assert!(
            machine.drain_sync_spawn_queue().next().is_none(),
            "completion must not resurrect a sync route removed by SetPeer"
        );
    }

    #[test]
    fn set_peer_replaces_queued_sync_with_only_live_part_hints() {
        let mut machine = BigSyncMachine::default();
        let peer = PeerKey::random();
        let removed_part = PartKey::random();
        let retained_part = PartKey::random();
        let obj = ObjKey::random();
        machine.handle_evt(BigSyncEvent::SetPeer(SetPeerEvent {
            peer_id: peer,
            parts: [removed_part, retained_part].into(),
            objects: Set::new(),
        }));

        let task_id = machine.tasks.spawn(std::time::Instant::now(), TaskSeed::Sync(SyncTaskSeed {
            kind: SyncTaskKind::Sync,
            part_hints: [removed_part, retained_part].into(),
            deets: SyncTaskDeets {
                peer_id: peer,
                obj_id: obj,
                remote_payload: Some(serde_json::json!({"head": 1})),
            },
        }));
        machine
            .peers
            .get_mut(&peer)
            .expect(ERROR_UNRECONIZED)
            .sync_workers
            .insert(
                obj,
                SyncWorkerState {
                    task_id,
                    cursors: [1].into(),
                    part_hints: [removed_part, retained_part].into(),
                    remote_payload: Some(serde_json::json!({"head": 1})),
                },
            );

        machine.handle_evt(BigSyncEvent::SetPeer(SetPeerEvent {
            peer_id: peer,
            parts: [retained_part].into(),
            objects: Set::new(),
        }));

        let tasks: Vec<_> = machine.drain_sync_spawn_queue().collect();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].part_hints, [retained_part].into());
        assert_ne!(tasks[0].id, task_id);
        assert_eq!(
            machine.peers[&peer].sync_workers[&obj].part_hints,
            [retained_part].into()
        );
    }

    #[test]
    fn peer_removal_stops_in_flight_removal_tasks() {
        let mut machine = BigSyncMachine::default();
        let peer = PeerKey::random();
        let part = PartKey::random();
        let obj = ObjKey::random();
        machine.handle_evt(BigSyncEvent::SetPeer(SetPeerEvent {
            peer_id: peer,
            parts: [part].into(),
            objects: Set::new(),
        }));

        {
            let peer_state = machine.peers.get_mut(&peer).expect(ERROR_UNRECONIZED);
            BigSyncMachine::schedule_obj_removal(
                &mut machine.tasks,
                &mut peer_state.remove_workers,
                peer,
                obj,
                part,
                Some(1),
            );
        }
        let removal_task = machine.peers[&peer].remove_workers[&obj].task_id;
        machine.drain_sync_spawn_queue();

        machine.handle_evt(BigSyncEvent::RemovePeer(RemovePeerEvent { peer_id: peer }));

        assert!(!machine.peers.contains_key(&peer));
        assert!(
            machine
                .drain_stop_queue()
                .any(|stopped| stopped == removal_task)
        );
    }
}
