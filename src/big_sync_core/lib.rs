//! FIXME: find a way to avoid blocking on BigSyncMachineCommands

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
        /// The peer answered that it does not know this part, so the part has
        /// no strategy and no cursor, and it keeps full sync blocked until the
        /// embedder drops it from the subscription set — or the part reaches the
        /// peer, which is the common case since the answer is usually a race.
        ///
        /// Emitted once per transition into the unanswered state: the machine
        /// re-asks on a timer, and the fact, not the attempt, is what a
        /// subscriber acts on.
        PeerPartUnanswered {
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

/// The first retry delay for a replay round the peer answered without progress: either it
/// does not know the part yet, or access rows deny it.
///
/// Only the seed lives here. `Tasks` doubles a retry's backoff per attempt and caps it at the
/// frame's `max_backoff` (a minute by default), so the growth and the cap ride the task rather
/// than being chosen at this call site. Pacing rather than a verdict is right for both arms: a
/// denial is reversible, because a grant may simply not have reached the peer's store yet.
const REPLAY_RETRY_SEED: Duration = Duration::from_secs(2);

/// A replay route's stable identity.
///
/// The cursor travels in the page request, but it moves as the machine consumes
/// events, so it must not key the in-flight state: a key that changes under the
/// machine would strand the page it belongs to.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum ReplayRoute {
    Part(PartKey),
    Object(ObjKey),
}

impl ReplayRoute {
    fn of(target: &SubscriptionTarget) -> Self {
        match target {
            SubscriptionTarget::Part { part_id, .. } => Self::Part(part_id.clone()),
            SubscriptionTarget::Object { obj_id, .. } => Self::Object(obj_id.clone()),
        }
    }
}

/// Replay scheduling is advisory: live targets get latency priority, while a target with
/// known backlog is isolated in bulk catch-up. Objects are always live; parts change lanes only
/// after the responder's per-target drained verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ReplayLane {
    Live,
    Bulk,
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
            request_id: crate::rpc::ReplayRequestId,
            lane: ReplayLane,
            caught_up: bool,
            waiting_for_credit: bool,
        }>,
        /// The peer's one logical replay subscription: the target set the responder holds for
        /// this client session, and the changes it has not acknowledged yet. Cursors remain in
        /// the page task and the cursor machine; this state only avoids repeating stable target
        /// metadata on the wire (ADR 012 decision 9).
        replay_subscription: struct ReplaySubscriptionState {
            subscription_id: crate::rpc::ReplaySubscriptionId,
            /// The generation the next `Update` carries. Sending takes the current value and
            /// leaves the next one behind, so a session's first update carries 0 — which is
            /// what opens a subscription the responder does not know.
            generation: u64,
            next_target_id: u32,
            /// Entries the responder acknowledged, and therefore the only routes a page may
            /// name.
            targets: Map<ReplayRoute, crate::rpc::ReplayTargetId>,
            /// Entries sent, or about to be sent, that no answer has acknowledged. A route
            /// stays out of `targets`, and so out of a page, until its entry lands.
            pending_additions: Map<ReplayRoute, struct PendingReplayAddition {
                id: crate::rpc::ReplayTargetId,
                target: crate::rpc::ReplaySubscriptionTarget,
            }>,
            /// Ids the next `Update` removes. An id the responder does not hold is a no-op to
            /// remove, so a re-send or a lost entry is safe to name here.
            pending_removals: Set<crate::rpc::ReplayTargetId>,
            /// The removals the update now in flight carries. They are held apart from
            /// `pending_removals` so a route dropped while the update was in flight is not
            /// lost with them.
            in_flight_removals: Set<crate::rpc::ReplayTargetId>,
            /// Routes the responder refused. A refused route stays wanted, keeps blocking
            /// full sync, and is re-added under a fresh id until an update for it succeeds.
            blocked: Set<ReplayRoute>,
            /// The scheduled task carrying the pending batch while it waits on the retry a
            /// refusal bought it.
            update_task: Option<TaskId>,
        },
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
                /// True once the peer has answered that it does not know this
                /// part. Not a negotiation in flight: it is the answer, and the
                /// embedder owns whether the part stays subscribed.
                unanswered: bool,
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
                        .cloned()
                        .collect(),
                )
            })
            .collect()
    }

    /// TEMP-DIAGNOSTIC: per (peer, part) full-sync blocking flags.
    #[cfg(any(test, feature = "test-support"))]
    pub fn debug_peer_part_sync_flags(
        &self,
    ) -> Vec<(PeerKey, PartKey, bool, bool, bool, bool, bool)> {
        let mut out = Vec::new();
        for (peer_id, peer_state) in &self.peers {
            for part_id in peer_state.parts.keys().cloned() {
                let default = Default::default();
                let part_state = peer_state.parts.get(&part_id).unwrap_or(&default);
                out.push((
                    peer_id.clone(),
                    part_id,
                    part_state.pending,
                    part_state.multi_strat,
                    peer_state.replay_phase_done,
                    part_state.cursor_active,
                    part_state.unanswered,
                ));
            }
        }
        out
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn debug_last_object_syncs(&self) -> Vec<(PeerKey, PartKey, ObjKey, std::time::Instant)> {
        self.last_object_syncs
            .iter()
            .map(|((peer_id, part_id), (obj_id, at))| {
                (peer_id.clone(), part_id.clone(), obj_id.clone(), *at)
            })
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
        for peer_id in &peer_ids {
            for part_id in &part_ids {
                need_set.insert((peer_id.clone(), part_id.clone()));
                if self.peer_part_is_fully_synced(peer_id.clone(), part_id.clone()) {
                    done_set.insert((peer_id.clone(), part_id.clone()));
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
            let _part_state = self.parts.entry(part_id.clone()).or_default();
            let _peer_part_state = peer_state.parts.entry(part_id).or_default();
        }
    }

    fn remove_peer(&mut self, peer_id: PeerKey) {
        let Some(peer_state) = self.peers.remove(&peer_id) else {
            return;
        };
        for (part_id, _peer_part_state) in peer_state.parts {
            self.last_object_syncs
                .remove(&(peer_id.clone(), part_id.clone()));
            let Some(part_state) = self.parts.get_mut(&part_id) else {
                continue;
            };
            part_state.peers.remove(&peer_id);
            part_state.fully_synced_peers.remove(&peer_id);
            // When a peer is removed, clean up all waiters that reference it
            // so they don't remain stranded.
            for waiter in self.waiters.values_mut() {
                waiter.done_set.remove(&(peer_id.clone(), part_id.clone()));
                waiter.need_set.remove(&(peer_id.clone(), part_id.clone()));
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
        let peer_state = self.peers.entry(peer_id.clone()).or_default();
        peer_state.replay_phase_done = replay_done;
        for part_id in peer_state.parts.keys().cloned().collect::<Vec<_>>() {
            if replay_done {
                self.__check_peer_part_synced(peer_id.clone(), part_id);
            } else {
                self.__check_peer_part_stale(peer_id.clone(), part_id);
            }
        }
    }

    fn mark_peer_part_only_cursor_strat(
        &mut self,
        peer_id: PeerKey,
        part_id: PartKey,
        only_cursor_strat: bool,
    ) {
        let peer_state = self.peers.entry(peer_id.clone()).or_default();
        let peer_part_state = peer_state.parts.entry(part_id.clone()).or_default();
        peer_part_state.multi_strat = !only_cursor_strat;
        if only_cursor_strat {
            self.__check_peer_part_synced(peer_id, part_id);
        } else {
            self.__check_peer_part_stale(peer_id, part_id);
        }
    }

    fn mark_peer_part_idle(&mut self, peer_id: PeerKey, part_id: PartKey) {
        let peer_state = self.peers.entry(peer_id.clone()).or_default();
        let peer_part_state = peer_state.parts.entry(part_id.clone()).or_default();
        peer_part_state.cursor_active = false;
        self.__check_peer_part_synced(peer_id, part_id);
    }

    fn mark_peer_part_cursor_active(&mut self, peer_id: PeerKey, part_id: PartKey) {
        let peer_state = self.peers.entry(peer_id.clone()).or_default();
        let part_state = self.parts.entry(part_id.clone()).or_default();
        let peer_part_state = peer_state.parts.entry(part_id.clone()).or_default();
        part_state.peers.insert(peer_id.clone());

        peer_part_state.cursor_active = true;
        self.__check_peer_part_stale(peer_id, part_id);
    }

    fn mark_peer_part_pending(&mut self, peer_id: PeerKey, part_id: PartKey, pending: bool) {
        let peer_state = self.peers.entry(peer_id.clone()).or_default();
        let peer_part_state = peer_state.parts.entry(part_id.clone()).or_default();
        peer_part_state.pending = pending;
        if !pending {
            // A decided strategy is the answer to what the event reported.
            peer_part_state.unanswered = false;
        }
        if pending {
            self.__check_peer_part_stale(peer_id, part_id);
        } else {
            self.__check_peer_part_synced(peer_id, part_id);
        }
    }

    /// Record the peer's answer that it does not know `part_id`.
    ///
    /// Reports the fact once per transition. The part is deliberately left
    /// blocking full sync: the machine does not decide that a part the peer
    /// cannot answer for is synced, so the embedder removes it from the
    /// subscription set when that is its policy, or leaves it to resolve.
    fn mark_peer_part_unanswered(&mut self, peer_id: PeerKey, part_id: PartKey) {
        let peer_state = self.peers.entry(peer_id.clone()).or_default();
        let peer_part_state = peer_state.parts.entry(part_id.clone()).or_default();
        if peer_part_state.unanswered {
            return;
        }
        peer_part_state.unanswered = true;
        self.stat_evts
            .push(SyncStatEvent::PeerPartUnanswered { peer_id, part_id });
    }

    fn __check_peer_part_synced(&mut self, peer_id: PeerKey, part_id: PartKey) {
        let peer_state = self.peers.entry(peer_id.clone()).or_default();
        let part_state = self.parts.entry(part_id.clone()).or_default();
        let peer_part_state = peer_state.parts.entry(part_id.clone()).or_default();
        part_state.peers.insert(peer_id.clone());

        if peer_part_state.pending
            || peer_part_state.multi_strat
            || !peer_state.replay_phase_done
            || peer_part_state.cursor_active
        {
            return;
        }

        peer_state.fully_synced_parts.insert(part_id.clone());
        part_state.fully_synced_peers.insert(peer_id.clone());

        for waiter in self.waiters.values_mut() {
            if waiter
                .need_set
                .contains(&(peer_id.clone(), part_id.clone()))
            {
                waiter.done_set.insert((peer_id.clone(), part_id.clone()));
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
            self.stat_evts.push(SyncStatEvent::PeerPartFullySynced {
                peer_id: peer_id.clone(),
                part_id: part_id.clone(),
            });
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
        let peer_state = self.peers.entry(peer_id.clone()).or_default();
        let part_state = self.parts.entry(part_id.clone()).or_default();
        let peer_part_state = peer_state.parts.entry(part_id.clone()).or_default();
        part_state.peers.insert(peer_id.clone());

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
            waiter.done_set.remove(&(peer_id.clone(), part_id.clone()));
        }

        if peer_state.emitted_full_synced {
            peer_state.emitted_full_synced = false;
            self.stat_evts.push(SyncStatEvent::PeerStale {
                peer_id: peer_id.clone(),
            });
        }

        if peer_part_state.emitted_full_synced {
            peer_part_state.emitted_full_synced = false;
            self.stat_evts.push(SyncStatEvent::PeerPartStale {
                peer_id,
                part_id: part_id.clone(),
            });
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
        /// Overrides [`ReplayPageTask::HOLD_MS`] for this machine. `None` is the
        /// production pacing; a shorter value is for tests, whose settling waits cannot
        /// see a parked live lane and would otherwise assert before the wake lands.
        replay_hold_ms: Option<u32>,

        /// Strategy hint for parts with no explicit per-part override. A caller that
        /// wants the bucket path for every part it syncs sets this to
        /// [`SyncMode::Bucket`].
        default_sync_mode: SyncMode,

        /// The client-owned namespace for replay subscriptions and request identifiers.
        /// It is initialized lazily so `Default` remains useful for deterministic tests.
        replay_session_id: Option<crate::rpc::ReplaySessionId>,

        /// Every page request this machine issues carries a fresh id, so the round that
        /// replaces one still in flight can name it and the responder can drop the older one
        /// if it is only waiting. Monotone, and only ever compared for equality.
        replay_request_seq: u64,

        cmds: VecDeque<(Uuid, BigSyncMachineCommand, Option<CursorIndex>, PeerKey)>,
        tasks: Scheduler<TaskSeed>,
    }
}

// public surface
impl BigSyncMachine {
    fn replay_session_id(&mut self) -> crate::rpc::ReplaySessionId {
        *self
            .replay_session_id
            .get_or_insert_with(|| crate::rpc::ReplaySessionId(rand::random::<u64>()))
    }

    pub fn set_max_task_backoff(&mut self, max_backoff: Duration) {
        self.tasks.set_max_backoff(max_backoff);
    }

    /// Strategy hint applied to parts with no per-part override. See [`SyncMode`]:
    /// this is the knob an embedder uses to opt into (or out of) the bucket path.
    pub fn set_default_sync_mode(&mut self, mode: SyncMode) {
        self.default_sync_mode = mode;
    }

    /// Shorten the live-lane replay hold. See [`ReplayPageTask::HOLD_MS`]: a test that
    /// waits for a settled cluster has to be able to observe a lane that is parked
    /// waiting for events, so it turns the production park down instead of having the
    /// responder ignore the request.
    pub fn set_replay_hold_ms(&mut self, hold_ms: u32) {
        self.replay_hold_ms = Some(hold_ms);
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
    pub fn debug_peer_part_sync_flags(
        &self,
    ) -> Vec<(PeerKey, PartKey, bool, bool, bool, bool, bool)> {
        self.stat_machine.debug_peer_part_sync_flags()
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn debug_replay_pages(&self) -> Vec<String> {
        self.peers
            .iter()
            .flat_map(|(peer_id, peer_state)| {
                peer_state.replay_pages.iter().map(move |(route, state)| {
                    format!(
                        "peer={peer_id} route={route:?} task_id={:?} request_id={:?} lane={:?} caught_up={} waiting_for_credit={}",
                        state.task_id,
                        state.request_id,
                        state.lane,
                        state.caught_up,
                        state.waiting_for_credit,
                    )
                })
            })
            .collect()
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
                TaskSeed::Machine(deets) => taken.push(MachineTask {
                    id: spawned.id,
                    deets,
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
                    self.refresh_peer_replay_worker(peer_id.clone(), true);
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
        self.all_seen_peer.insert(peer_id.clone());
        let mut peer_state = self.peers.remove(&peer_id).unwrap_or_else(|| PeerState {
            remove_workers: default(),
            pending_removals: default(),
            sync_workers: default(),
            replay_pages: default(),
            replay_subscription: ReplaySubscriptionState {
                // One subscription per client session and storage scope, named in every
                // request; the id only has to be unique within the session.
                subscription_id: crate::rpc::ReplaySubscriptionId(1),
                generation: 0,
                // Ids keep counting up across a re-open, so a fresh entry can never collide
                // with an entry of a handle the responder has forgotten.
                next_target_id: 1,
                targets: default(),
                pending_additions: default(),
                pending_removals: default(),
                in_flight_removals: default(),
                blocked: default(),
                update_task: None,
            },
            objects: default(),
            cursor_machine: default(),
            cursors_cmd_buf: default(),
            bucket_cmd_buf: default(),
            parts: default(),
        });
        let old_part_ids: Set<_> = peer_state.parts.keys().cloned().collect();
        let removed_parts: Set<_> = old_part_ids.difference(&parts).cloned().collect();
        let mut pending_parts = Set::new();
        let mut pending_tasks = Set::new();
        for part_id in old_part_ids.intersection(&parts) {
            if matches!(
                peer_state.parts.get(part_id),
                Some(PeerPartState {
                    strat: PeerPartStrategy::Pending(_)
                })
            ) && let Some(PeerPartState {
                strat: PeerPartStrategy::Pending(task_id),
            }) = peer_state.parts.remove(part_id)
            {
                pending_parts.insert(part_id.clone());
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
        for part_id in &removed_parts {
            peer_state.cursor_machine.remove_part(part_id.clone());
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
            .map(|(obj_id, _)| obj_id.clone())
            .collect();
        for obj_id in affected_sync_workers {
            let mut worker = peer_state
                .sync_workers
                .remove(&obj_id)
                .expect(ERROR_IMPOSSIBLE);
            let _state = self.tasks.cancel(worker.task_id).expect(ERROR_UNRECONIZED);
            worker.part_hints.retain(|part_id| parts.contains(part_id));
            if worker.part_hints.is_empty() && !objects.contains(&obj_id) {
                // The task is dropped here and nothing will complete it, so the
                // replay it owed is abandoned: forget the claim instead of keeping
                // a cursor slot no completion can settle.
                peer_state.cursor_machine.abandon_obj_sync(&obj_id);
                continue;
            }
            worker.task_id = self.tasks.spawn(
                std::time::Instant::now(),
                TaskSeed::Sync(SyncTaskSeed {
                    kind: SyncTaskKind::Sync,
                    part_hints: worker.part_hints.clone(),
                    deets: SyncTaskDeets {
                        peer_id: peer_id.clone(),
                        obj_id: obj_id.clone(),
                        remote_payload: worker.remote_payload.clone(),
                    },
                }),
            );
            let old = peer_state.sync_workers.insert(obj_id, worker);
            assert!(old.is_none(), "fishy");
        }
        let added_parts: Set<_> = parts.difference(&old_part_ids).cloned().collect();
        let decision_parts: Set<_> = pending_parts.union(&added_parts).cloned().collect();
        if !decision_parts.is_empty() {
            let deets = MachineTaskDeets::DecidePeerStrategy(DecidePeerStrategyTask {
                peer_id: peer_id.clone(),
                parts: decision_parts.clone(),
                sync_modes: default(),
                default_sync_mode: self.default_sync_mode,
            });
            let decide_task = self
                .tasks
                .spawn(std::time::Instant::now(), TaskSeed::Machine(deets));
            for part_id in &decision_parts {
                peer_state.parts.insert(
                    part_id.clone(),
                    PeerPartState {
                        strat: PeerPartStrategy::Pending(decide_task),
                    },
                );
            }
        }
        let stale_workers: Vec<_> = peer_state
            .sync_workers
            .iter()
            .filter_map(|(obj_id, worker)| {
                (worker.part_hints.is_empty() && !objects.contains(obj_id))
                    .then_some(obj_id.clone())
            })
            .collect();
        for obj_id in stale_workers {
            let worker = peer_state
                .sync_workers
                .remove(&obj_id)
                .expect(ERROR_IMPOSSIBLE);
            let _state = self.tasks.cancel(worker.task_id).expect(ERROR_UNRECONIZED);
            // Nothing can complete the replay this worker owed, and the object
            // route that ordered it is gone too (that is what made the worker
            // stale), so release the claim: a re-added object must reach the
            // backend again instead of reading as already in flight.
            peer_state.cursor_machine.abandon_obj_sync(&obj_id);
        }
        peer_state.objects = objects;
        // A target the embedder has dropped stops being blocked with it: a part or object that
        // comes back is a fresh entry, and only the client ever changes the target set.
        let wanted: Set<ReplayRoute> = parts
            .iter()
            .cloned()
            .map(ReplayRoute::Part)
            .chain(peer_state.objects.iter().cloned().map(ReplayRoute::Object))
            .collect();
        peer_state
            .replay_subscription
            .blocked
            .retain(|route| wanted.contains(route));
        self.stat_machine
            .set_peer(peer_id.clone(), parts.iter().cloned());
        for part_id in &decision_parts {
            self.stat_machine
                .mark_peer_part_pending(peer_id.clone(), part_id.clone(), true);
        }
        self.peers.insert(peer_id.clone(), peer_state);
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
                let _state = self.tasks.cancel(worker.task_id).expect(ERROR_UNRECONIZED);
            }
            for worker in old.remove_workers.into_values() {
                let _state = self.tasks.cancel(worker.task_id).expect(ERROR_UNRECONIZED);
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
                if self.tasks.cancel(state.task_id).is_none() {
                    tracing::debug!(
                        task_id = state.task_id,
                        "replay task was already retired during peer cleanup",
                    );
                }
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
                    self.stat_machine.mark_peer_part_only_cursor_strat(
                        peer_id.clone(),
                        part_id.clone(),
                        false,
                    );
                    self.stat_machine
                        .mark_peer_part_unanswered(peer_id.clone(), part_id.clone());
                    parts_retry.insert(part_id);
                    continue;
                }
                PeerPartStratDecision::Bucket(strat) => {
                    let mut machine = Box::new(BucketMachine::new(
                        part_id.clone(),
                        strat.remote_depth,
                        strat.remote_len,
                        strat.last_cursor,
                    ));
                    machine.on_bucket_page(
                        strat.initial_filtered_buckets,
                        &mut peer_state.bucket_cmd_buf,
                    );
                    self.stat_machine.mark_peer_part_only_cursor_strat(
                        peer_id.clone(),
                        part_id.clone(),
                        false,
                    );
                    self.stat_machine.mark_peer_part_pending(
                        peer_id.clone(),
                        part_id.clone(),
                        false,
                    );
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
                    self.stat_machine.mark_peer_part_only_cursor_strat(
                        peer_id.clone(),
                        part_id.clone(),
                        true,
                    );
                    self.stat_machine.mark_peer_part_pending(
                        peer_id.clone(),
                        part_id.clone(),
                        false,
                    );
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
                    peer_id: peer_id.clone(),
                    parts: parts_retry.clone(),
                    sync_modes: default(),
                    default_sync_mode: self.default_sync_mode,
                },
            ));
            let decide_task = if parts_retry.len() == response_len {
                self.tasks.spawn_delayed(
                    deets,
                    retry,
                    Duration::from_secs(2),
                    std::time::Instant::now(),
                )
            } else {
                self.tasks.spawn(std::time::Instant::now(), deets)
            };
            for part_id in parts_retry {
                peer_state.parts.insert(
                    part_id.clone(),
                    PeerPartState {
                        strat: PeerPartStrategy::Pending(decide_task),
                    },
                );
                self.stat_machine
                    .mark_peer_part_pending(peer_id.clone(), part_id, true);
            }
        }

        let bucket_cmd_count = peer_state.bucket_cmd_buf.len();
        let cursor_cmd_count = peer_state.cursors_cmd_buf.len();
        // refresh the replay pages if needed
        self.refresh_peer_replay_worker(peer_id.clone(), false);
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
                for part_id in &unkown_parts {
                    self.stat_machine
                        .mark_peer_part_unanswered(peer_id.clone(), part_id.clone());
                }
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
                    parts_retry.insert(part_id.clone());
                }
                PeerPartStrategy::Bucket(_) | PeerPartStrategy::Cursor(_) => {}
            };
        }
        if !parts_retry.is_empty() {
            let deets = MachineTaskDeets::DecidePeerStrategy(DecidePeerStrategyTask {
                peer_id: peer_id.clone(),
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
                    part_id.clone(),
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
                    .mark_peer_part_pending(peer_id.clone(), part_id, true);
            }
        }
    }
}

// cursor support
impl BigSyncMachine {
    /// The replay receive window is separate from sync-task concurrency. A page
    /// is allowed to refill once the cursor machine has drained below half a
    /// page of admitted work.
    const REPLAY_WORK_LOW_WATERMARK: usize = ReplayPageTask::LIMIT as usize / 2;

    /// Wanted replay routes, each with the target to request next.
    ///
    /// A part whose strategy is still being negotiated has no replay cursor yet;
    /// skipping it is safe because replay is at-least-once and this set is
    /// recomputed whenever a strategy lands.
    fn replay_page_targets(&self, peer_id: PeerKey) -> Map<ReplayRoute, SubscriptionTarget> {
        let Some(peer_state) = self.peers.get(&peer_id) else {
            return default();
        };
        let mut targets = Map::new();
        for (part_id, state) in &peer_state.parts {
            let applied = match &state.strat {
                PeerPartStrategy::Pending(_) => continue,
                PeerPartStrategy::Bucket(state) => state.replay_cursor,
                PeerPartStrategy::Cursor(state) => state.replay_cursor,
            };
            let cursor = peer_state
                .cursor_machine
                .part_replay_cursor(part_id, applied);
            targets.insert(
                ReplayRoute::Part(part_id.clone()),
                SubscriptionTarget::Part {
                    part_id: part_id.clone(),
                    cursor,
                },
            );
        }
        targets.extend(peer_state.objects.iter().cloned().map(|obj_id| {
            let cursor = peer_state.cursor_machine.obj_resume_cursor(&obj_id);
            (
                ReplayRoute::Object(obj_id.clone()),
                SubscriptionTarget::Object { obj_id, cursor },
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
            SubscriptionTarget::Part { part_id, .. } => {
                let peer_state = self.peers.get(&peer_id)?;
                let state = peer_state.parts.get(part_id)?;
                let applied = match &state.strat {
                    PeerPartStrategy::Pending(_) => return None,
                    PeerPartStrategy::Bucket(state) => state.replay_cursor,
                    PeerPartStrategy::Cursor(state) => state.replay_cursor,
                };
                Some(SubscriptionTarget::Part {
                    part_id: part_id.clone(),
                    cursor: peer_state
                        .cursor_machine
                        .part_replay_cursor(part_id, applied),
                })
            }
            SubscriptionTarget::Object { obj_id, .. } => Some(SubscriptionTarget::Object {
                obj_id: obj_id.clone(),
                cursor: self
                    .peers
                    .get(&peer_id)?
                    .cursor_machine
                    .obj_resume_cursor(obj_id),
            }),
        }
    }

    fn replay_lane(&self, peer_id: &PeerKey, target: &SubscriptionTarget) -> ReplayLane {
        if matches!(target, SubscriptionTarget::Object { .. }) {
            return ReplayLane::Live;
        }
        let route = ReplayRoute::of(target);
        self.peers
            .get(peer_id)
            .and_then(|peer_state| peer_state.replay_pages.get(&route))
            .map(|state| state.lane)
            .unwrap_or(ReplayLane::Live)
    }

    /// Ask for another page and record each lane's round as in flight.
    ///
    /// A live round and a bulk round are independent requests: objects always stay live, while a
    /// part moves to bulk only after the responder reports backlog. The target cursor still travels
    /// with each target, so each lane remains one bounded request rather than one request per part.
    fn spawn_replay_pages(
        &mut self,
        peer_id: PeerKey,
        targets: Vec<SubscriptionTarget>,
        caught_up: bool,
    ) {
        let mut live = Vec::new();
        let mut bulk = Vec::new();
        for target in targets {
            match self.replay_lane(&peer_id, &target) {
                ReplayLane::Live => live.push(target),
                ReplayLane::Bulk => bulk.push(target),
            }
        }
        self.spawn_replay_pages_inner(peer_id.clone(), ReplayLane::Live, live, caught_up, None);
        self.spawn_replay_pages_inner(peer_id, ReplayLane::Bulk, bulk, caught_up, None);
    }

    /// Re-issue a page for `targets` after a delay, keeping each route's last verdict.
    fn schedule_replay_pages(
        &mut self,
        peer_id: PeerKey,
        targets: Vec<SubscriptionTarget>,
        caught_up: bool,
        retry: Retry,
        delay: Duration,
    ) {
        let mut live = Vec::new();
        let mut bulk = Vec::new();
        for target in targets {
            match self.replay_lane(&peer_id, &target) {
                ReplayLane::Live => live.push(target),
                ReplayLane::Bulk => bulk.push(target),
            }
        }
        self.spawn_replay_pages_inner(
            peer_id.clone(),
            ReplayLane::Live,
            live,
            caught_up,
            Some((retry, delay)),
        );
        self.spawn_replay_pages_inner(
            peer_id,
            ReplayLane::Bulk,
            bulk,
            caught_up,
            Some((retry, delay)),
        );
    }

    /// Apply the wanted-set changes to the peer's subscription and send the batch they make.
    ///
    /// A route the embedder now wants takes a fresh entry id; a registered route it no longer
    /// wants is removed from the responder's set and unblocked. A newly wanted target is what
    /// makes an update due now — a fresh entry joins the pending batch and the retry a refusal
    /// bought it waits on is reset with it (ADR 012 decision 9).
    fn sync_replay_subscription(&mut self, peer_id: &PeerKey) {
        let wanted = self.replay_page_targets(peer_id.clone());
        let mut queued = false;
        if let Some(peer_state) = self.peers.get_mut(peer_id) {
            let state = &mut peer_state.replay_subscription;
            for (route, target) in &wanted {
                if !state.targets.contains_key(route)
                    && !state.pending_additions.contains_key(route)
                {
                    let id = Self::next_replay_target_id(state);
                    state.pending_additions.insert(
                        route.clone(),
                        PendingReplayAddition {
                            id,
                            target: crate::rpc::ReplaySubscriptionTarget::from(target),
                        },
                    );
                    queued = true;
                }
            }
            let dropped: Vec<(ReplayRoute, crate::rpc::ReplayTargetId)> = state
                .targets
                .iter()
                .filter(|(route, _)| !wanted.contains_key(*route))
                .map(|(route, id)| (route.clone(), *id))
                .collect();
            for (route, id) in dropped {
                state.targets.remove(&route);
                state.pending_removals.insert(id);
                // The embedder dropping the target is one of the two ways a block clears.
                state.blocked.remove(&route);
                queued = true;
            }
            let abandoned: Vec<(ReplayRoute, crate::rpc::ReplayTargetId)> = state
                .pending_additions
                .iter()
                .filter(|(route, _)| !wanted.contains_key(*route))
                .map(|(route, pending)| (route.clone(), pending.id))
                .collect();
            for (route, id) in abandoned {
                state.pending_additions.remove(&route);
                // The entry may have landed even though its answer never came back, so the id
                // is removed responder-side rather than left to be served for a dropped route.
                state.pending_removals.insert(id);
                state.blocked.remove(&route);
                queued = true;
            }
        }
        if queued {
            self.schedule_replay_update(peer_id.clone(), None);
        }
    }

    /// Send the peer's pending target-set changes as one `Update` (ADR 012 decision 9).
    ///
    /// The batch stays pending until an answer acknowledges it, so a lost answer is retried
    /// rather than lost with it. `delayed` is the retry a refusal or a failed round bought:
    /// the caller hands this task's own backoff on, and a target queued while it waits sends
    /// the batch at once instead.
    fn schedule_replay_update(&mut self, peer_id: PeerKey, delayed: Option<(Retry, Duration)>) {
        let session_id = self.replay_session_id();
        self.replay_request_seq = self.replay_request_seq.wrapping_add(1);
        let request_id = crate::rpc::ReplayRequestId(self.replay_request_seq);
        let Some(peer_state) = self.peers.get_mut(&peer_id) else {
            return;
        };
        let state = &mut peer_state.replay_subscription;
        if state.pending_additions.is_empty() && state.pending_removals.is_empty() {
            return;
        }
        if let Some(previous) = state.update_task.take()
            && self.tasks.cancel(previous).is_none()
        {
            tracing::debug!(task_id = previous, "replay update was already retired");
        }
        // Whatever the last update was carrying goes back into the batch: nothing may be
        // dropped just because an answer never came back. A removal the responder already
        // applied is a no-op to repeat, because an id it does not hold is a no-op to remove.
        let carried: Vec<_> = state.in_flight_removals.drain().collect();
        state.pending_removals.extend(carried);
        let generation = state.generation;
        state.generation = generation.checked_add(1).expect(ERROR_IMPOSSIBLE);
        let additions: Vec<_> = state
            .pending_additions
            .values()
            .map(|pending| crate::rpc::ReplaySubscriptionTargetEntry {
                id: pending.id,
                target: pending.target.clone(),
            })
            .collect();
        let removals: Vec<_> = state.pending_removals.iter().copied().collect();
        state.in_flight_removals = state.pending_removals.clone();
        state.pending_removals.clear();
        let subscription = ReplaySubscriptionTaskState {
            subscription_id: state.subscription_id,
            generation,
            request: Some(crate::rpc::ReplaySubscriptionRequest::Update {
                session_id,
                subscription_id: state.subscription_id,
                generation,
                additions,
                removals,
            }),
        };
        let deets = TaskSeed::Machine(MachineTaskDeets::ReplayPage(ReplayPageTask {
            peer_id: peer_id.clone(),
            session_id,
            request_id,
            targets: Vec::new(),
            supersede: None,
            limit: ReplayPageTask::LIMIT,
            hold_ms: 0,
            subscription: Some(subscription),
        }));
        let task_id = match delayed {
            Some((retry, delay)) => {
                self.tasks
                    .spawn_delayed(deets, retry, delay, std::time::Instant::now())
            }
            None => self.tasks.spawn(std::time::Instant::now(), deets),
        };
        tracing::debug!(
            peer_id = %peer_id,
            ?request_id,
            generation,
            delayed = delayed.is_some(),
            "spawning replay subscription update"
        );
        state.update_task = Some(task_id);
    }

    fn next_replay_target_id(state: &mut ReplaySubscriptionState) -> crate::rpc::ReplayTargetId {
        let id = crate::rpc::ReplayTargetId(state.next_target_id);
        state.next_target_id = state.next_target_id.checked_add(1).expect(ERROR_IMPOSSIBLE);
        id
    }

    /// Block one route: the responder refused its entry, so the route keeps its place in the
    /// wanted set, keeps full sync blocked, and is re-added under a fresh id (ADR 012
    /// decision 9). Only the client ever removes it.
    fn block_replay_route(
        &mut self,
        peer_id: &PeerKey,
        route: ReplayRoute,
        target: crate::rpc::ReplaySubscriptionTarget,
    ) {
        let Some(peer_state) = self.peers.get_mut(peer_id) else {
            return;
        };
        // A blocked route has no round of its own: a page names it again only once an update
        // acknowledges the entry the block re-queues below, so its round state goes with it.
        if let Some(page) = peer_state.replay_pages.remove(&route)
            && self.tasks.cancel(page.task_id).is_none()
        {
            tracing::debug!(
                task_id = page.task_id,
                "replay round was already retired when its route was blocked"
            );
        }
        let state = &mut peer_state.replay_subscription;
        // The entry that was refused leaves the acknowledged set, and its id is removed
        // responder-side so the fresh id below cannot collide with an entry the responder
        // still holds.
        if let Some(id) = state.targets.remove(&route) {
            state.pending_removals.insert(id);
        }
        if let Some(pending) = state.pending_additions.remove(&route) {
            state.pending_removals.insert(pending.id);
        }
        let id = Self::next_replay_target_id(state);
        state
            .pending_additions
            .insert(route.clone(), PendingReplayAddition { id, target });
        state.blocked.insert(route);
    }

    /// Apply the answer to one `Update`: entries it did not refuse landed, and the refused
    /// ones are returned for the caller to block (ADR 012 decision 9).
    ///
    /// An answer to an update a newer one has since replaced is ignored: the newer update
    /// carries the same entries, and its answer is the one that counts.
    fn acknowledge_replay_update(
        &mut self,
        peer_id: &PeerKey,
        task_id: TaskId,
        generation: u64,
        rejected: &[(crate::rpc::ReplayTargetId, TargetVerdict)],
    ) -> Vec<(ReplayRoute, crate::rpc::ReplaySubscriptionTarget)> {
        let mut refused = Vec::new();
        let Some(peer_state) = self.peers.get_mut(peer_id) else {
            return refused;
        };
        let state = &mut peer_state.replay_subscription;
        if state.generation != generation.saturating_add(1) {
            return refused;
        }
        if state.update_task == Some(task_id) {
            state.update_task = None;
        }
        let refused_routes: Vec<ReplayRoute> = state
            .pending_additions
            .iter()
            .filter(|(_, pending)| rejected.iter().any(|(id, _)| *id == pending.id))
            .map(|(route, _)| route.clone())
            .collect();
        for route in refused_routes {
            if let Some(pending) = state.pending_additions.remove(&route) {
                refused.push((route, pending.target));
            }
        }
        let landed: Vec<ReplayRoute> = state.pending_additions.keys().cloned().collect();
        for route in landed {
            if let Some(pending) = state.pending_additions.remove(&route) {
                state.targets.insert(route.clone(), pending.id);
                state.blocked.remove(&route);
            }
        }
        // The update applied, so the removals it carried are the responder's business no more.
        state.in_flight_removals.clear();
        refused
    }

    /// A responder that is ahead did not apply the generation this round carried, so the
    /// round is re-sent past the generation the responder holds.
    fn retry_superseded_replay_update(
        &mut self,
        peer_id: &PeerKey,
        task_id: TaskId,
        generation: u64,
        current: u64,
    ) {
        {
            let Some(peer_state) = self.peers.get_mut(peer_id) else {
                return;
            };
            let state = &mut peer_state.replay_subscription;
            if state.generation != generation.saturating_add(1) {
                // A newer update is already in flight, and it carries these entries too.
                return;
            }
            if state.update_task == Some(task_id) {
                state.update_task = None;
            }
            // Sending past the responder's generation is what makes the re-send apply: an
            // update no newer than the one it holds is not applied at all.
            state.generation = state.generation.max(current.saturating_add(1));
        }
        self.schedule_replay_update(peer_id.clone(), None);
    }

    /// Forget everything the machine believes about the peer's subscription, then re-state it.
    ///
    /// The responder answering that it does not hold the id means its handle is gone (its TTL
    /// swept it, or it restarted), so the next update re-states the whole target set under
    /// generation 0 — which is the one generation that opens a subscription it does not know.
    /// Target ids keep counting up so a re-open cannot collide with an entry of the handle
    /// that was lost; the subscription id is reused because the responder holds nothing under
    /// it, and a fresh one would spend the peer's subscription budget on the abandoned one.
    ///
    /// Forgetting is only half of a reset. A reset that left nothing queued would leave every
    /// wanted route unpageable for good: a page may only name an entry the responder has
    /// acknowledged, and only an `Update` can acknowledge one. So the reset ends by handing the
    /// wanted set to the same reconciler every other change uses, which queues it and sends the
    /// opening update itself.
    fn reset_replay_subscription(&mut self, peer_id: &PeerKey) {
        if let Some(peer_state) = self.peers.get_mut(peer_id) {
            let state = &mut peer_state.replay_subscription;
            state.generation = 0;
            state.targets.clear();
            state.pending_additions.clear();
            state.pending_removals.clear();
            state.in_flight_removals.clear();
            state.blocked.clear();
            state.update_task = None;
        }
        // The routes of the lost subscription have no page of their own any more: dropping
        // their round state is what lets the re-open's acknowledgement page them again.
        if let Some(peer_state) = self.peers.get_mut(peer_id) {
            let stale: Vec<ReplayRoute> = peer_state.replay_pages.keys().cloned().collect();
            for route in stale {
                if let Some(state) = peer_state.replay_pages.remove(&route)
                    && self.tasks.cancel(state.task_id).is_none()
                {
                    tracing::debug!(
                        task_id = state.task_id,
                        "replay task was already retired during a subscription reset",
                    );
                }
            }
        }
        // Re-state the wanted set under generation 0 and send it: this is the update that
        // opens the responder's new subscription.
        self.sync_replay_subscription(peer_id);
    }

    fn spawn_replay_pages_inner(
        &mut self,
        peer_id: PeerKey,
        lane: ReplayLane,
        targets: Vec<SubscriptionTarget>,
        caught_up: bool,
        delayed: Option<(Retry, Duration)>,
    ) {
        if targets.is_empty() {
            return;
        }
        // A page names entries the responder acknowledged. An entry still in flight, or one
        // the responder refused, is not in its target set: naming it would fail the whole
        // page instead of answering the routes it can serve (ADR 012 decision 9).
        // A page names entries the responder acknowledged. An entry still in flight, one the
        // responder refused, or one whose peer has gone is not in its target set: naming it
        // would fail the whole page instead of answering the routes it can serve (ADR 012
        // decision 9). The entry id is what the wire names, so it is paired with the route here
        // and the round carries both.
        let targets: Vec<(crate::rpc::ReplayTargetId, SubscriptionTarget)> = targets
            .into_iter()
            .filter_map(|target| {
                let id = self
                    .peers
                    .get(&peer_id)?
                    .replay_subscription
                    .targets
                    .get(&ReplayRoute::of(&target))?;
                Some((*id, target))
            })
            .collect();
        if targets.is_empty() {
            return;
        }
        let session_id = self.replay_session_id();
        self.replay_request_seq = self.replay_request_seq.wrapping_add(1);
        let request_id = crate::rpc::ReplayRequestId(self.replay_request_seq);
        let routes: Vec<ReplayRoute> = targets
            .iter()
            .map(|(_, target)| ReplayRoute::of(target))
            .collect();
        // A lane request supersedes only the previous request carrying that lane's routes. The
        // live and bulk requests are intentionally independent so a bulk backlog cannot release
        // or delay a live long-poll.
        let supersede = self.peers.get(&peer_id).and_then(|state| {
            state
                .replay_pages
                .iter()
                .filter(|(route, _)| routes.contains(route))
                .map(|(_, page)| page.request_id)
                .max()
        });
        // A lane that has not yet established a caught-up verdict is drain-only. This
        // includes a forced refresh: it must check current backlog without making the
        // full-sync waiter pay for a live hold. Once every route is caught up, the
        // next live request may long-poll for future events.
        let all_targets_caught_up = matches!(lane, ReplayLane::Live)
            && self.peers.get(&peer_id).is_some_and(|peer_state| {
                routes.iter().all(|route| {
                    peer_state
                        .replay_pages
                        .get(route)
                        .map(|state| state.caught_up)
                        .unwrap_or(caught_up)
                })
            });
        let hold_ms = if all_targets_caught_up {
            self.replay_hold_ms.unwrap_or(ReplayPageTask::HOLD_MS)
        } else {
            0
        };
        let subscription = self.peers.get(&peer_id).map(|peer_state| {
            let state = &peer_state.replay_subscription;
            ReplaySubscriptionTaskState {
                subscription_id: state.subscription_id,
                generation: state.generation,
                request: None,
            }
        });
        tracing::debug!(
            peer_id = %peer_id,
            target_count = targets.len(),
            ?request_id,
            ?supersede,
            delayed = delayed.is_some(),
            "spawning replay page"
        );
        let deets = TaskSeed::Machine(MachineTaskDeets::ReplayPage(ReplayPageTask {
            peer_id: peer_id.clone(),
            session_id,
            request_id,
            targets,
            supersede,
            limit: ReplayPageTask::LIMIT,
            hold_ms,
            subscription,
        }));
        let task_id = match delayed {
            Some((retry, delay)) => {
                self.tasks
                    .spawn_delayed(deets, retry, delay, std::time::Instant::now())
            }
            None => self.tasks.spawn(std::time::Instant::now(), deets),
        };
        if let Some(peer_state) = self.peers.get_mut(&peer_id) {
            for route in routes {
                let caught_up = peer_state
                    .replay_pages
                    .get(&route)
                    .map(|state| state.caught_up)
                    .unwrap_or(caught_up);
                peer_state.replay_pages.insert(
                    route,
                    ReplayPageState {
                        task_id,
                        request_id,
                        lane,
                        caught_up,
                        waiting_for_credit: false,
                    },
                );
            }
        }
    }

    /// The current position of every route still wanted, dropping the ones retired while a
    /// round was in flight.
    fn refresh_replay_targets(
        &self,
        peer_id: PeerKey,
        targets: Vec<SubscriptionTarget>,
    ) -> Vec<SubscriptionTarget> {
        targets
            .into_iter()
            .filter_map(|target| self.refreshed_replay_target(peer_id.clone(), &target))
            .collect()
    }

    fn replay_work_below_watermark(&self, peer_id: &PeerKey) -> bool {
        self.peers.get(peer_id).is_none_or(|state| {
            state.cursor_machine.pending_replay_work() < Self::REPLAY_WORK_LOW_WATERMARK
        })
    }

    fn pause_replay_targets_for_credit(
        &mut self,
        peer_id: &PeerKey,
        targets: impl IntoIterator<Item = SubscriptionTarget>,
    ) {
        let Some(peer_state) = self.peers.get_mut(peer_id) else {
            return;
        };
        for target in targets {
            let route = ReplayRoute::of(&target);
            if let Some(state) = peer_state.replay_pages.get_mut(&route) {
                state.waiting_for_credit = true;
            }
        }
    }

    fn resume_replay_pages_for_credit(&mut self, peer_id: PeerKey) {
        if !self.replay_work_below_watermark(&peer_id) {
            return;
        }
        let waiting: Vec<ReplayRoute> = self
            .peers
            .get(&peer_id)
            .into_iter()
            .flat_map(|state| state.replay_pages.iter())
            .filter_map(|(route, state)| state.waiting_for_credit.then_some(route.clone()))
            .collect();
        if waiting.is_empty() {
            return;
        }
        let targets = self.replay_page_targets(peer_id.clone());
        let targets: Vec<_> = waiting
            .into_iter()
            .filter_map(|route| targets.get(&route).cloned())
            .collect();
        self.spawn_replay_pages(peer_id, targets, false);
    }

    /// The peer-level replay verdict: caught up when every wanted route has an
    /// answer saying so. No routes means there is nothing left to replay.
    ///
    /// A blocked route has no answer that could make it caught up, so it keeps full sync
    /// blocked until an update for it succeeds or the embedder drops it (ADR 012 decision 9).
    fn update_peer_replay_done(&mut self, peer_id: PeerKey) {
        let routes = self.replay_page_targets(peer_id.clone());
        let done = {
            let peer_state = self.peers.get(&peer_id);
            let blocked = peer_state.map(|state| &state.replay_subscription.blocked);
            routes.keys().all(|route| {
                !blocked.is_some_and(|blocked| blocked.contains(route))
                    && peer_state
                        .and_then(|state| state.replay_pages.get(route))
                        .is_some_and(|state| state.caught_up)
            })
        };
        self.stat_machine.mark_peer_replay_done(peer_id, done);
    }

    fn refresh_peer_replay_worker(&mut self, peer_id: PeerKey, force: bool) {
        // The wanted set is reconciled into the subscription before any page is asked for: an
        // entry the responder has not acknowledged is not pageable, and a newly wanted target
        // is what sends the batch that makes it so (ADR 012 decision 9).
        self.sync_replay_subscription(&peer_id);
        let targets = self.replay_page_targets(peer_id.clone());
        let Some(peer_state) = self.peers.get_mut(&peer_id) else {
            return;
        };
        if targets.is_empty() {
            for (_, state) in peer_state.replay_pages.drain() {
                if self.tasks.cancel(state.task_id).is_none() {
                    tracing::debug!(
                        task_id = state.task_id,
                        "replay task was already retired during peer cleanup",
                    );
                }
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
            .cloned()
            .collect();
        for route in stale {
            if let Some(mut state) = peer_state.replay_pages.remove(&route) {
                if self.tasks.cancel(state.task_id).is_none() {
                    tracing::debug!(
                        task_id = state.task_id,
                        "replay task was already retired during peer cleanup",
                    );
                }
                if force {
                    // Keep the advisory lane across a forced restart for routes that are still
                    // wanted, but invalidate the prior caught-up verdict: the new request must
                    // answer before full sync can settle.
                    if targets.contains_key(&route) {
                        state.caught_up = false;
                        state.waiting_for_credit = false;
                        peer_state.replay_pages.insert(route, state);
                    }
                }
            }
        }
        let missing: Vec<SubscriptionTarget> = targets
            .into_iter()
            .filter(|(route, _)| force || !peer_state.replay_pages.contains_key(route))
            .map(|(_, target)| target)
            .collect();
        tracing::debug!(
            peer_id = %peer_id,
            target_count = missing.len(),
            "refresh peer replay pages"
        );
        // Each lane carries its routes together, so a peer with many parts still costs at most one
        // live request and one bulk request.
        self.spawn_replay_pages(peer_id.clone(), missing, false);
        self.update_peer_replay_done(peer_id);
    }

    fn handle_replay_page_result(
        &mut self,
        task_id: TaskId,
        retry: Retry,
        result: ReplayPageResult,
    ) {
        let peer_id = result.peer_id;
        let page = result.page;
        let update = result.update;
        // The events are page-level: they are applied even when a newer round has replaced
        // this one, because re-delivering them costs a watermark comparison while dropping
        // them costs a round. The verdicts are per route, and a verdict for a route this round
        // no longer owns is dropped: the round that replaced it has already been asked, and
        // its answer is the one that counts.
        let mut reask: Vec<SubscriptionTarget> = Vec::new();
        let mut refused: Vec<(ReplayRoute, crate::rpc::ReplaySubscriptionTarget)> = Vec::new();
        let mut refused_parts: Vec<PartKey> = Vec::new();
        {
            let Some(peer_state) = self.peers.get_mut(&peer_id) else {
                assert!(self.all_seen_peer.contains(&peer_id), "fishy");
                return;
            };
            tracing::debug!(
                peer_id = %peer_id,
                events = page.events.len(),
                target_count = page.targets.len(),
                "replay page answered"
            );
            for evt in page.events {
                peer_state
                    .cursor_machine
                    .on_subscription_evt(evt, &mut peer_state.cursors_cmd_buf);
            }
            for (target, verdict) in page.targets {
                let route = ReplayRoute::of(&target);
                match peer_state.replay_pages.get(&route) {
                    Some(state) if state.task_id == task_id => {}
                    stale => {
                        tracing::debug!(
                            peer_id = %peer_id,
                            ?target,
                            active_task_id = ?stale.map(|state| state.task_id),
                            "dropped peer replay page verdict: stale or retired route",
                        );
                        continue;
                    }
                }
                match verdict {
                    TargetVerdict::Events { resume, drained } => {
                        // The page resume is a scheduling cursor. It may be ahead of the
                        // applied cursor while the emitted work is still pending.
                        match &target {
                            SubscriptionTarget::Part { part_id, .. } => peer_state
                                .cursor_machine
                                .advance_part_replay_cursor(part_id.clone(), resume),
                            SubscriptionTarget::Object { obj_id, .. } => peer_state
                                .cursor_machine
                                .advance_obj_replay_cursor(obj_id.clone(), resume),
                        }
                        // A per-page verdict is pacing, never terminal: the round is re-issued
                        // from this result either way, so "caught up" only decides that the
                        // next round waits for an event instead of asking again immediately.
                        if let Some(state) = peer_state.replay_pages.get_mut(&route) {
                            state.caught_up = drained;
                            state.lane = if matches!(&target, SubscriptionTarget::Object { .. })
                                || drained
                            {
                                ReplayLane::Live
                            } else {
                                ReplayLane::Bulk
                            };
                        }
                        reask.push(target);
                    }
                    TargetVerdict::UnknownPart | TargetVerdict::Unauthorized => {
                        // Both kinds are one machine outcome: absent access rows cannot
                        // distinguish a revocation from a grant that has not landed yet, so a
                        // route the peer cannot serve is blocked rather than torn down. It
                        // keeps its place in the wanted set and in the full-sync gate, the
                        // responder never removes it, and the update that re-adds it under a
                        // fresh id is what resolves it (ADR 012 decision 9).
                        if let SubscriptionTarget::Part { part_id, .. } = &target {
                            refused_parts.push(part_id.clone());
                        }
                        refused.push((route, crate::rpc::ReplaySubscriptionTarget::from(&target)));
                    }
                }
            }
        }
        self.drain_cursor_machine_cmds(peer_id.clone());
        // The round's own update answer is the other source of refusals, and the answer's
        // landed entries are the ones a page may name from here on.
        let mut acked = false;
        if let Some(update) = update {
            match update {
                ReplayUpdateOutcome::Applied {
                    generation,
                    rejected,
                } => {
                    refused.extend(
                        self.acknowledge_replay_update(&peer_id, task_id, generation, &rejected),
                    );
                    acked = true;
                }
                ReplayUpdateOutcome::Superseded {
                    generation,
                    current,
                } => {
                    self.retry_superseded_replay_update(&peer_id, task_id, generation, current);
                }
            }
        }
        // Every refusal of this round is one machine outcome and one retry: the update re-adds
        // the refused entries under fresh ids, on the backoff this round's task handed on.
        if !refused.is_empty() {
            for (route, target) in refused {
                if let ReplayRoute::Part(part_id) = &route {
                    refused_parts.push(part_id.clone());
                }
                self.block_replay_route(&peer_id, route, target);
            }
            self.schedule_replay_update(peer_id.clone(), Some((retry, REPLAY_RETRY_SEED)));
        }
        for part_id in refused_parts {
            self.stat_machine
                .mark_peer_part_unanswered(peer_id.clone(), part_id);
        }
        // A round whose update landed makes its entries pageable, so the fresh ones are paged
        // by the same path an event-driven refresh uses.
        if acked {
            self.refresh_peer_replay_worker(peer_id.clone(), false);
        }
        // The next round asks for the same routes at their current positions, which is the
        // cursor machine's own bookkeeping: an applied event moved a position, and a route
        // that answered with nothing has nothing to move. A route retired while this round was
        // in flight is dropped here rather than asked for again.
        let reask = self.refresh_replay_targets(peer_id.clone(), reask);
        if self.replay_work_below_watermark(&peer_id) {
            if !reask.is_empty() {
                self.spawn_replay_pages(peer_id.clone(), reask, false);
            }
        } else {
            self.pause_replay_targets_for_credit(&peer_id, reask);
        }
        self.update_peer_replay_done(peer_id);
    }

    fn handle_replay_page_err(
        &mut self,
        task_id: TaskId,
        retry: Retry,
        ReplayPageTaskError {
            peer_id,
            targets,
            updated_subscription_id,
            deets,
        }: ReplayPageTaskError,
    ) {
        // Only the routes this failed round still owns are rescheduled: a route whose round
        // was replaced by a newer request belongs to that request now.
        let mut caught_up = false;
        let mut live = Vec::new();
        {
            let Some(peer_state) = self.peers.get_mut(&peer_id) else {
                assert!(self.all_seen_peer.contains(&peer_id), "fishy");
                return;
            };
            for target in targets {
                let route = ReplayRoute::of(&target);
                let Some(state) = peer_state
                    .replay_pages
                    .get(&route)
                    .filter(|state| state.task_id == task_id)
                else {
                    // The page was already superseded or the route was dropped.
                    continue;
                };
                caught_up = state.caught_up;
                live.push(target);
            }
        }
        if matches!(
            &deets,
            ReplayPageTaskErrorDeets::Rpc(rpc::RpcError::UnknownSubscription)
        ) {
            // The responder does not hold the handle any more (its TTL swept it, or it
            // restarted). The reset forgets the lost handle *and* re-states the whole target
            // set under generation 0, which is the update that opens the new subscription — so
            // the opening update is already scheduled by the time this returns, and a second
            // send here would only cancel and repeat it.
            self.reset_replay_subscription(&peer_id);
        } else if updated_subscription_id.is_some() {
            // The round carried an update and never asked for its page, so what this backoff
            // retries is the update, and the batch it carries stays pending meanwhile.
            self.schedule_replay_update(peer_id.clone(), Some((retry, Duration::from_secs(2))));
        }
        if live.is_empty() {
            self.update_peer_replay_done(peer_id);
            return;
        }
        tracing::debug!(
            peer_id = %peer_id,
            target_count = live.len(),
            retry = ?retry,
            deets = ?deets,
            "replay page failed; rescheduling",
        );
        if self.replay_work_below_watermark(&peer_id) {
            self.schedule_replay_pages(
                peer_id.clone(),
                live,
                caught_up,
                retry,
                Duration::from_secs(2),
            );
        } else {
            self.pause_replay_targets_for_credit(&peer_id, live);
        }
        self.update_peer_replay_done(peer_id);
    }
    fn drain_cursor_machine_cmds(&mut self, peer_id: PeerKey) {
        let peer_state = self.peers.get_mut(&peer_id).expect(ERROR_UNRECONIZED);
        for cmd in peer_state.cursors_cmd_buf.drain(..) {
            trace!(peer_id = %peer_id, ?cmd,"processing cursor cmd");
            match cmd {
                CursorMachineCommand::PartIdle { part_id } => {
                    self.stat_machine
                        .mark_peer_part_idle(peer_id.clone(), part_id);
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
                    let (cursors, part_hints, remote_payload) = if let Some(mut worker) =
                        peer_state.sync_workers.remove(&obj_id)
                    {
                        let _state = self.tasks.cancel(worker.task_id).expect(ERROR_UNRECONIZED);

                        worker.part_hints.extend(parts.iter().cloned());
                        worker.cursors.insert(cursor);
                        (
                            worker.cursors,
                            worker.part_hints,
                            remote_payload.or(worker.remote_payload),
                        )
                    } else {
                        (
                            [cursor].into(),
                            parts.iter().cloned().collect(),
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
                            obj_id.clone(),
                            part_id.clone(),
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
                        pending.re_added_parts.extend(part_hints.iter().cloned());
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
                        peer_id: peer_id.clone(),
                        obj_id: obj_id.clone(),
                        remote_payload: remote_payload.clone(),
                    };
                    let task_id = self.tasks.spawn(
                        std::time::Instant::now(),
                        TaskSeed::Sync(SyncTaskSeed {
                            kind: SyncTaskKind::Sync,
                            part_hints: part_hints.iter().cloned().collect(),
                            deets: deets.clone(),
                        }),
                    );
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
                            .mark_peer_part_cursor_active(peer_id.clone(), part_id);
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
                                    peer_id: peer_id.clone(),
                                    part_id,
                                    cursor,
                                },
                                None,
                                peer_id.clone(),
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
                    // object. An object-routed task is not obsolete at zero hints —
                    // its route owes the content (see `SetPeer`) — so only a
                    // part-routed task with nothing left to fetch stops here.
                    let object_routed = peer_state.objects.contains(&obj_id);
                    let stop_task = peer_state.sync_workers.get_mut(&obj_id).and_then(|worker| {
                        worker.part_hints.remove(&part_id);
                        (worker.part_hints.is_empty() && !object_routed).then_some(worker.task_id)
                    });
                    if let Some(task_id) = stop_task {
                        let worker = peer_state
                            .sync_workers
                            .remove(&obj_id)
                            .expect(ERROR_UNRECONIZED);
                        assert_eq!(worker.task_id, task_id);
                        self.tasks.cancel(task_id).expect(ERROR_UNRECONIZED);
                        // The stopped task owed the object a replay and the
                        // removal below only settles membership, so release the
                        // claim: nothing else can settle it, and keeping it would
                        // suppress every later delivery of that cursor.
                        peer_state.cursor_machine.abandon_obj_sync(&obj_id);
                    }
                    Self::schedule_obj_removal(
                        &mut self.tasks,
                        &mut peer_state.remove_workers,
                        peer_id.clone(),
                        obj_id,
                        part_id,
                        Some(cursor),
                    );
                }
            }
        }
        self.resume_replay_pages_for_credit(peer_id);
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
            obj_id: obj_id.clone(),
            remote_payload: None,
        };
        let task_id = tasks.spawn(
            std::time::Instant::now(),
            TaskSeed::Sync(SyncTaskSeed {
                kind: SyncTaskKind::RemoveFromParts,
                part_hints: part_hints.iter().cloned().collect(),
                deets,
            }),
        );
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
            .entry(obj_id.clone())
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
                .extend(worker.part_hints.iter().cloned());
        }
        true
    }

    /// Issue the deferred re-removal (remaining hints) and re-sync (re-added
    /// parts) for an object whose in-flight removal was cancelled by a re-add.
    /// Called when the cancelled removal task's terminal event lands, so the
    /// zombie task is guaranteed done and cannot race the new work.
    ///
    /// A cancelled removal whose terminal event landed (either outcome: the
    /// backend applied it, or the task failed). Either way the re-add that
    /// cancelled it supersedes the removal's intent, which is why an
    /// unapplied zombie is not a reason to keep its membership lanes owed.
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
        // Each worker settles every cursor it holds with its own fixed lane, so
        // each one is handed only the cursors that still owe that lane. A cursor
        // owing both lanes is handed to both, and whichever task applies its half
        // settles that half. Handing the whole pool to both workers is what let a
        // membership cursor settle the sync lane it never owed (panic) and left
        // that same cursor's membership lane with no worker to settle it (part
        // watermark stuck).
        let mut membership_cursors: Set<CursorIndex> = default();
        let mut sync_cursors: Set<CursorIndex> = default();
        for &cursor in &pending.cursors {
            if peer_state.cursor_machine.owes_obj_job_lane(
                &obj_id,
                cursor,
                CursorJobCompletionKind::Membership,
            ) {
                membership_cursors.insert(cursor);
            }
            if peer_state.cursor_machine.owes_obj_job_lane(
                &obj_id,
                cursor,
                CursorJobCompletionKind::Sync,
            ) {
                sync_cursors.insert(cursor);
            }
        }
        // With no re-removal left to run, nothing will ever settle these
        // membership lanes: their task was cancelled and every hint it held was
        // cancelled by a re-add, which is the only thing that cancels a hint
        // (`cancel_obj_removal_hint` is called from the re-add path). The removal
        // is therefore moot — the object belongs in those parts again — so the
        // lanes finish here. This is not an acknowledgement of a mutation that
        // never ran: a Membership completion carries no acknowledgement, the
        // object's replay position advances only on a Sync completion.
        let settle_membership = pending.remaining_hints.is_empty();
        if settle_membership {
            for &cursor in &membership_cursors {
                peer_state.cursor_machine.on_obj_sync_job_evt(
                    obj_id.clone(),
                    cursor,
                    CursorJobCompletionKind::Membership,
                    &mut peer_state.cursors_cmd_buf,
                );
            }
        }
        let settled_membership = settle_membership && !membership_cursors.is_empty();
        if !pending.remaining_hints.is_empty() {
            let deets = SyncTaskDeets {
                peer_id: peer_id.clone(),
                obj_id: obj_id.clone(),
                remote_payload: None,
            };
            let task_id = self.tasks.spawn(
                std::time::Instant::now(),
                TaskSeed::Sync(SyncTaskSeed {
                    kind: SyncTaskKind::RemoveFromParts,
                    part_hints: pending.remaining_hints.iter().cloned().collect(),
                    deets,
                }),
            );
            peer_state.remove_workers.insert(
                obj_id.clone(),
                SyncWorkerState {
                    task_id,
                    cursors: membership_cursors,
                    part_hints: pending.remaining_hints,
                    remote_payload: None,
                },
            );
        }
        if !pending.re_added_parts.is_empty() {
            let deets = SyncTaskDeets {
                peer_id: peer_id.clone(),
                obj_id: obj_id.clone(),
                remote_payload: pending.remote_payload.clone(),
            };
            let task_id = self.tasks.spawn(
                std::time::Instant::now(),
                TaskSeed::Sync(SyncTaskSeed {
                    kind: SyncTaskKind::Sync,
                    part_hints: pending.re_added_parts.iter().cloned().collect(),
                    deets,
                }),
            );
            peer_state.sync_workers.insert(
                obj_id.clone(),
                SyncWorkerState {
                    task_id,
                    cursors: sync_cursors,
                    part_hints: pending.re_added_parts,
                    remote_payload: pending.remote_payload,
                },
            );
        }
        if settled_membership {
            self.drain_cursor_machine_cmds(peer_id);
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
                    let (cursors, part_hints, remote_payload) = if let Some(mut worker) =
                        peer_state.sync_workers.remove(&obj_id)
                    {
                        let _state = self.tasks.cancel(worker.task_id).expect(ERROR_UNRECONIZED);

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
                            obj_id.clone(),
                            part_id.clone(),
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
                        pending.re_added_parts.extend(part_hints.iter().cloned());
                        if remote_payload.is_some() {
                            pending.remote_payload = remote_payload;
                        }
                        // The Sync task is issued by `resume_pending_removal`
                        // once the cancelled removal task's completion lands.
                        continue;
                    }
                    let deets = SyncTaskDeets {
                        peer_id: peer_id.clone(),
                        obj_id: obj_id.clone(),
                        remote_payload: remote_payload.clone(),
                    };
                    let task_id = self.tasks.spawn(
                        std::time::Instant::now(),
                        TaskSeed::Sync(SyncTaskSeed {
                            kind: SyncTaskKind::Sync,
                            part_hints: part_hints.iter().cloned().collect(),
                            deets: deets.clone(),
                        }),
                    );
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
                    // As in the cursor-strategy arm: an object-routed task stays
                    // live at zero hints because its route owes the content.
                    let object_routed = peer_state.objects.contains(&obj_id);
                    let stop_task = peer_state.sync_workers.get_mut(&obj_id).and_then(|worker| {
                        worker.part_hints.remove(&part_id);
                        (worker.part_hints.is_empty() && !object_routed).then_some(worker.task_id)
                    });
                    if let Some(task_id) = stop_task {
                        let worker = peer_state
                            .sync_workers
                            .remove(&obj_id)
                            .expect(ERROR_UNRECONIZED);
                        assert_eq!(worker.task_id, task_id);
                        self.tasks.cancel(task_id).expect(ERROR_UNRECONIZED);
                        // Same as the cursor-strategy removal above: the stopped
                        // task owed the object a replay, and the removal that
                        // follows settles membership only.
                        peer_state.cursor_machine.abandon_obj_sync(&obj_id);
                    }
                    Self::schedule_obj_removal(
                        &mut self.tasks,
                        &mut peer_state.remove_workers,
                        peer_id.clone(),
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
                        peer_id: peer_id.clone(),
                        part_id: part_id.clone(),
                        offset,
                        since,
                        working_level,
                    };
                    let deets = MachineTaskDeets::ListBuckets(task.clone());
                    let part = peer_state.parts.get_mut(&part_id).expect(ERROR_UNRECONIZED);
                    let PeerPartStrategy::Bucket(state) = &mut part.strat else {
                        unreachable!()
                    };
                    let task_id = self
                        .tasks
                        .spawn(std::time::Instant::now(), TaskSeed::Machine(deets));
                    let old = state.active_list_tasks.insert(task_id, task);
                    assert!(old.is_none(), "fishy");
                }
                BucketMachineCommand::LeafBuckets {
                    since,
                    buckets,
                    part_id,
                } => {
                    let task = LeafBucketsTask {
                        peer_id: peer_id.clone(),
                        part_id: part_id.clone(),
                        since,
                        buckets,
                    };
                    let deets = MachineTaskDeets::LeafBuckets(task.clone());
                    let part = peer_state.parts.get_mut(&part_id).expect(ERROR_UNRECONIZED);
                    let PeerPartStrategy::Bucket(state) = &mut part.strat else {
                        unreachable!()
                    };
                    let task_id = self
                        .tasks
                        .spawn(std::time::Instant::now(), TaskSeed::Machine(deets));
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
                            peer_id: peer_id.clone(),
                            part_id: part_id.clone(),
                            cursor: old.replay_cursor,
                        },
                        None,
                        peer_id.clone(),
                    ));
                    peer_state.parts.insert(
                        part_id.clone(),
                        PeerPartState {
                            strat: PeerPartStrategy::Cursor(CursorState {
                                replay_cursor: old.replay_cursor,
                            }),
                        },
                    );
                    self.stat_machine.mark_peer_part_only_cursor_strat(
                        peer_id.clone(),
                        part_id,
                        true,
                    );
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
            // The lane can be gone: a re-add's supersede drops a cursor's
            // membership lane, and this task was already in flight then.
            // Settling a lane the waiter no longer owes panics the job board.
            if !peer_state.cursor_machine.owes_obj_job_lane(
                &evt.obj_id,
                cursor,
                CursorJobCompletionKind::Membership,
            ) {
                tracing::debug!(
                    peer_id = %evt.peer_id,
                    obj_id = %evt.obj_id,
                    cursor,
                    "removal completion holds a cursor that no longer owes the membership lane",
                );
                continue;
            }
            peer_state.cursor_machine.on_obj_sync_job_evt(
                evt.obj_id.clone(),
                cursor,
                CursorJobCompletionKind::Membership,
                &mut peer_state.cursors_cmd_buf,
            );
        }
        self.drain_cursor_machine_cmds(evt.peer_id.clone());
        self.drain_bucket_machine_cmds(evt.peer_id.clone());
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
                    stale_part_hints.push(part_id.clone());
                    continue;
                };
                match &mut part.strat {
                    PeerPartStrategy::Pending(_) => stale_part_hints.push(part_id.clone()),
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
                peer_state.cursor_machine.remove_part(part_id.clone());
                part_hints.remove(&part_id);
            }
            for &cursor in &completion.cursors {
                // The machine decides what this completion settles: the part board
                // owes a lane only where a part-scoped replay registered it, while
                // the sync's own acknowledgement belongs to the object. That
                // acknowledgement is applied for every cursor the backend reports,
                // so a cursor shared with a removal's membership lane cannot strand
                // the object route it acknowledges; the membership lane stays owed
                // until its own task completes.
                peer_state.cursor_machine.on_obj_sync_job_evt(
                    completion.obj_id.clone(),
                    cursor,
                    CursorJobCompletionKind::Sync,
                    &mut peer_state.cursors_cmd_buf,
                );
            }
            (completion, part_hints)
        };
        for part_id in &part_hints {
            self.stat_machine.record_object_synced(
                evt.peer_id.clone(),
                part_id.clone(),
                completion.obj_id.clone(),
            );
        }
        self.stat_machine
            .stat_evts
            .push(SyncStatEvent::ObjectSynced {
                peer_id: evt.peer_id.clone(),
                obj_id: completion.obj_id,
            });
        self.drain_cursor_machine_cmds(evt.peer_id.clone());
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
                    peer_id: evt.peer_id.clone(),
                    obj_id: evt.obj_id.clone(),
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
                    peer_id: evt.peer_id.clone(),
                    obj_id: evt.obj_id.clone(),
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

    /// A part the peer answers it does not know is reported to the embedder once
    /// per transition, and it keeps blocking full sync: the machine does not
    /// decide that a part its peer cannot answer for is synced. Dropping the part
    /// from the subscription set is the embedder's call.
    #[test]
    fn an_unanswered_part_is_reported_once_and_still_blocks_full_sync() {
        let mut machine = BigSyncMachine::default();
        let peer = PeerKey::random();
        let part = PartKey::random();
        machine.handle_evt(BigSyncEvent::SetPeer(SetPeerEvent {
            peer_id: peer.clone(),
            parts: [part.clone()].into(),
            objects: Set::new(),
        }));
        machine.drain_stat_evts().for_each(drop);

        let refusal = || DecidePeerStrategyTaskError {
            peer_id: peer.clone(),
            deets: DecidePeerStrategyErrorDeets::ListError(ListPartsError::UnkownParts {
                unkown_parts: vec![part.clone()],
            }),
        };
        let retry = || crate::scheduler::Retry {
            attempt_no: 0,
            backoff: Duration::ZERO,
            queued_at: std::time::Instant::now(),
        };

        machine.handle_decide_peer_strat_err(1, retry(), refusal());
        let reported: Vec<_> = machine.drain_stat_evts().collect();
        assert_eq!(reported.len(), 1, "the answer is a fact, reported once");
        assert!(
            matches!(reported[0], SyncStatEvent::PeerPartUnanswered { .. }),
            "the report is the unanswered part"
        );
        assert!(
            !machine
                .stat_machine
                .peer_part_is_fully_synced(peer.clone(), part.clone()),
            "an unanswered part keeps blocking full sync until the embedder drops it"
        );

        machine.handle_decide_peer_strat_err(1, retry(), refusal());
        assert!(
            machine.drain_stat_evts().next().is_none(),
            "re-asking the same question is not a new fact"
        );

        // A strategy landing answers the question, so a later refusal is new.
        machine
            .stat_machine
            .mark_peer_part_pending(peer.clone(), part.clone(), false);
        machine.drain_stat_evts().for_each(drop);
        machine.handle_decide_peer_strat_err(1, retry(), refusal());
        assert_eq!(
            machine.drain_stat_evts().count(),
            1,
            "a later refusal of the same part is reported again"
        );
    }

    #[test]
    fn replay_work_watermark_stops_page_admission_without_advancing_applied_state() {
        let mut machine = BigSyncMachine::default();
        let peer = PeerKey::random();
        machine.handle_evt(BigSyncEvent::SetPeer(SetPeerEvent {
            peer_id: peer.clone(),
            parts: Set::new(),
            objects: Set::new(),
        }));
        machine.drain_stat_evts().for_each(drop);

        {
            let state = machine.peers.get_mut(&peer).expect("peer was inserted");
            for cursor in 0..BigSyncMachine::REPLAY_WORK_LOW_WATERMARK {
                state.cursor_machine.on_subscription_evt(
                    PartEvent::Changed(ObjChanged {
                        cursor: cursor as CursorIndex + 1,
                        part_ids: Vec::new(),
                        obj_id: ObjKey::random(),
                        payload: serde_json::Value::Null,
                    }),
                    &mut state.cursors_cmd_buf,
                );
            }

            assert_eq!(
                state.cursor_machine.pending_replay_work(),
                BigSyncMachine::REPLAY_WORK_LOW_WATERMARK
            );
        }
        assert!(!machine.replay_work_below_watermark(&peer));
    }

    #[test]
    fn replay_lane_keeps_objects_live_and_parts_advisory() {
        let mut machine = BigSyncMachine::default();
        let peer = PeerKey::random();
        let part = PartKey::random();
        let object = ObjKey::random();
        machine.handle_evt(BigSyncEvent::SetPeer(SetPeerEvent {
            peer_id: peer.clone(),
            parts: [part.clone()].into(),
            objects: [object.clone()].into(),
        }));
        machine
            .peers
            .get_mut(&peer)
            .expect(ERROR_UNRECONIZED)
            .replay_pages
            .insert(
                ReplayRoute::Part(part.clone()),
                ReplayPageState {
                    task_id: 0,
                    request_id: crate::rpc::ReplayRequestId(0),
                    lane: ReplayLane::Bulk,
                    caught_up: false,
                    waiting_for_credit: false,
                },
            );

        assert_eq!(
            machine.replay_lane(
                &peer,
                &SubscriptionTarget::Part {
                    part_id: part,
                    cursor: 0,
                },
            ),
            ReplayLane::Bulk,
        );
        assert_eq!(
            machine.replay_lane(
                &peer,
                &SubscriptionTarget::Object {
                    obj_id: object,
                    cursor: 0,
                },
            ),
            ReplayLane::Live,
        );
    }

    /// A machine with one peer that wants `parts`, each with an already-decided strategy, so
    /// every part is a replay route and nothing else gates it.
    fn replay_machine_with_parts(
        parts: impl IntoIterator<Item = PartKey>,
    ) -> (BigSyncMachine, PeerKey) {
        let mut machine = BigSyncMachine::default();
        let peer = PeerKey::random();
        let parts: Vec<PartKey> = parts.into_iter().collect();
        machine.handle_evt(BigSyncEvent::SetPeer(SetPeerEvent {
            peer_id: peer.clone(),
            parts: Set::new(),
            objects: Set::new(),
        }));
        let peer_state = machine.peers.get_mut(&peer).expect("peer was inserted");
        for part in parts {
            peer_state.parts.insert(
                part,
                PeerPartState {
                    strat: PeerPartStrategy::Cursor(CursorState { replay_cursor: 0 }),
                },
            );
        }
        (machine, peer)
    }

    fn replay_machine_with_one_part() -> (BigSyncMachine, PeerKey, PartKey) {
        let part = PartKey::random();
        let (machine, peer) = replay_machine_with_parts([part.clone()]);
        (machine, peer, part)
    }

    fn test_retry() -> crate::scheduler::Retry {
        crate::scheduler::Retry {
            attempt_no: 0,
            backoff: Duration::ZERO,
            queued_at: std::time::Instant::now(),
        }
    }

    /// The route a page round carries at `index`, without its responder entry id: the tests here
    /// are about routes, and the round names each one by the id the wire uses.
    fn paged_route(page: &ReplayPageTask, index: usize) -> SubscriptionTarget {
        page.targets[index].1.clone()
    }

    fn take_replay_task(machine: &mut BigSyncMachine, what: &str) -> (TaskId, ReplayPageTask) {
        let spawned = machine
            .drain_machine_spawn_queue()
            .next()
            .unwrap_or_else(|| panic!("{what}"));
        let MachineTask {
            id,
            deets: MachineTaskDeets::ReplayPage(task),
        } = spawned
        else {
            panic!("{what}: spawned a non-replay task");
        };
        (id, task)
    }

    fn empty_page_result(
        peer_id: &PeerKey,
        update: Option<ReplayUpdateOutcome>,
    ) -> ReplayPageResult {
        ReplayPageResult {
            peer_id: peer_id.clone(),
            page: crate::rpc::ReplayPage {
                events: Vec::new(),
                targets: Vec::new(),
            },
            update,
        }
    }

    /// Open `peer`'s subscription for its wanted routes and acknowledge it, leaving the routes
    /// pageable, and hand back the page round the machine then spawns.
    fn open_and_acknowledge_replay(
        machine: &mut BigSyncMachine,
        peer: &PeerKey,
    ) -> (TaskId, ReplayPageTask) {
        machine.refresh_peer_replay_worker(peer.clone(), true);
        let (opening_id, opening) =
            take_replay_task(machine, "a refresh spawns the opening update");
        assert!(
            opening.targets.is_empty(),
            "an update round carries no page targets"
        );
        let generation = opening
            .subscription
            .as_ref()
            .expect("an update round belongs to a subscription")
            .generation;
        machine.handle_replay_page_result(
            opening_id,
            test_retry(),
            empty_page_result(
                peer,
                Some(ReplayUpdateOutcome::Applied {
                    generation,
                    rejected: Vec::new(),
                }),
            ),
        );
        take_replay_task(machine, "an acknowledged route is paged")
    }

    /// A peer that answers `UnknownSubscription` has forgotten the handle the client is naming:
    /// its TTL swept it, or it restarted. Forgetting it back is not enough — the machine must
    /// re-state the whole wanted set under generation 0, which is the update that opens the new
    /// subscription, and then resume paging from the same cursors. Objects stay live; parts
    /// return through their own lanes.
    #[test]
    fn a_forgotten_subscription_is_reopened_with_the_whole_wanted_set() {
        let part = PartKey::random();
        let object = ObjKey::random();
        let (mut machine, peer) = replay_machine_with_parts([part.clone()]);
        machine
            .stat_machine
            .set_peer(peer.clone(), [part.clone()].into_iter());
        machine
            .peers
            .get_mut(&peer)
            .expect("peer was inserted")
            .objects
            .insert(object.clone());
        let part_route = ReplayRoute::Part(part.clone());
        let object_route = ReplayRoute::Object(object.clone());
        let wanted: Set<ReplayRoute> = machine
            .replay_page_targets(peer.clone())
            .keys()
            .cloned()
            .collect();
        assert_eq!(
            wanted,
            Set::from([part_route.clone(), object_route.clone()]),
            "both routes are wanted before anything goes wrong"
        );

        // Healthy first: both routes acknowledged and drained.
        let (page_id, page) = open_and_acknowledge_replay(&mut machine, &peer);
        let verdicts: Vec<_> = page
            .targets
            .into_iter()
            .map(|(_, target)| {
                let resume = target.cursor();
                (
                    target,
                    TargetVerdict::Events {
                        resume,
                        drained: true,
                    },
                )
            })
            .collect();
        machine.handle_replay_page_result(
            page_id,
            test_retry(),
            ReplayPageResult {
                peer_id: peer.clone(),
                page: crate::rpc::ReplayPage {
                    events: Vec::new(),
                    targets: verdicts,
                },
                update: None,
            },
        );
        assert!(
            machine
                .stat_machine
                .peer_part_is_fully_synced(peer.clone(), part.clone()),
            "the routed part starts out drained and synced"
        );

        // The peer forgets the handle: the live round it answers cannot name the subscription.
        let (forgotten_id, forgotten) =
            take_replay_task(&mut machine, "a caught-up peer re-asks its live round");
        machine.handle_replay_page_err(
            forgotten_id,
            test_retry(),
            ReplayPageTaskError {
                peer_id: peer.clone(),
                targets: forgotten
                    .targets
                    .iter()
                    .map(|(_, target)| target.clone())
                    .collect(),
                updated_subscription_id: None,
                deets: ReplayPageTaskErrorDeets::Rpc(crate::rpc::RpcError::UnknownSubscription),
            },
        );

        // The reset must re-open: an update carrying every wanted route under generation 0.
        let (_reopen_id, reopen) = take_replay_task(
            &mut machine,
            "a forgotten subscription re-opens with an update",
        );
        assert!(
            reopen.targets.is_empty(),
            "the re-open is an update round, not a page"
        );
        let subscription = reopen
            .subscription
            .as_ref()
            .expect("an update round belongs to a subscription");
        assert_eq!(
            subscription.generation, 0,
            "the re-open must open a handle the responder does not have"
        );
        let Some(crate::rpc::ReplaySubscriptionRequest::Update {
            generation,
            additions,
            removals,
            ..
        }) = subscription.request.clone()
        else {
            panic!("the re-open must carry an update");
        };
        assert_eq!(generation, 0, "an opening update carries generation 0");
        assert!(removals.is_empty(), "a re-open removes nothing");
        let reopened: Set<ReplayRoute> = additions
            .iter()
            .map(|entry| match &entry.target {
                crate::rpc::ReplaySubscriptionTarget::Part { part_id } => {
                    ReplayRoute::Part(part_id.clone())
                }
                crate::rpc::ReplaySubscriptionTarget::Object { obj_id } => {
                    ReplayRoute::Object(obj_id.clone())
                }
            })
            .collect();
        assert_eq!(
            reopened, wanted,
            "the re-open restates every wanted route, not a delta"
        );
    }

    /// A target the responder refuses is blocked: it stays in the wanted set, it keeps full
    /// sync blocked, and the update that re-adds it — under a fresh id — is retried on the
    /// task's own backoff (ADR 012 decision 9).
    #[test]
    fn a_refused_target_is_blocked_and_readded_under_a_fresh_id() {
        let (mut machine, peer, part) = replay_machine_with_one_part();
        let (page_id, page) = open_and_acknowledge_replay(&mut machine, &peer);
        let route = ReplayRoute::Part(part.clone());
        let first_id = machine.peers[&peer].replay_subscription.targets[&route];
        let target = paged_route(&page, 0);
        machine.drain_stat_evts().for_each(drop);

        machine.handle_replay_page_result(
            page_id,
            test_retry(),
            ReplayPageResult {
                peer_id: peer.clone(),
                page: crate::rpc::ReplayPage {
                    events: Vec::new(),
                    targets: vec![(target, TargetVerdict::UnknownPart)],
                },
                update: None,
            },
        );

        let state = &machine.peers[&peer].replay_subscription;
        assert!(
            state.blocked.contains(&route),
            "a refused target is blocked"
        );
        let pending = state
            .pending_additions
            .get(&route)
            .expect("a refused entry is re-queued");
        assert_ne!(
            pending.id, first_id,
            "the re-add travels under a fresh id so it cannot collide with the refused entry"
        );
        assert!(
            state.in_flight_removals.contains(&first_id),
            "the refused entry's own id is what the retry's update removes"
        );
        assert!(
            !state.targets.contains_key(&route),
            "a blocked route is not pageable"
        );
        assert!(
            !machine
                .stat_machine
                .peer_part_is_fully_synced(peer.clone(), part.clone()),
            "a blocked target keeps full sync blocked"
        );
        assert!(
            machine
                .drain_stat_evts()
                .any(|evt| matches!(evt, SyncStatEvent::PeerPartUnanswered { .. })),
            "the embedder is told the part cannot be answered"
        );
        // The page is not paced per target by the refusal: it is the update that waits.
        let counts = machine.task_counts();
        assert_eq!(counts.delayed, 1, "the re-add waits on its own retry");
        assert_eq!(counts.spawn_queue, 0, "the retry is not run now");
        assert!(
            machine.drain_machine_spawn_queue().next().is_none(),
            "a refusal spawns no page"
        );
    }

    /// A part the responder reports as unauthorized is one machine outcome with one it does not
    /// know: absent access rows cannot tell a revocation from a grant that has not landed yet,
    /// so both block the target and keep full sync blocked (ADR 012 decision 9).
    #[test]
    fn an_unauthorized_verdict_blocks_its_target_like_an_unknown_part() {
        let (mut machine, peer, part) = replay_machine_with_one_part();
        let (page_id, page) = open_and_acknowledge_replay(&mut machine, &peer);
        let route = ReplayRoute::Part(part.clone());
        let target = paged_route(&page, 0);
        machine.drain_stat_evts().for_each(drop);

        machine.handle_replay_page_result(
            page_id,
            test_retry(),
            ReplayPageResult {
                peer_id: peer.clone(),
                page: crate::rpc::ReplayPage {
                    events: Vec::new(),
                    targets: vec![(target, TargetVerdict::Unauthorized)],
                },
                update: None,
            },
        );

        let state = &machine.peers[&peer].replay_subscription;
        assert!(state.blocked.contains(&route));
        assert!(state.pending_additions.contains_key(&route));
        assert!(
            !machine
                .stat_machine
                .peer_part_is_fully_synced(peer.clone(), part.clone()),
            "a denied target keeps full sync blocked too"
        );
    }

    /// The re-add lands: the block clears, the fresh entry is acknowledged, and the route is
    /// paged again — which is what lets full sync settle.
    #[test]
    fn a_refused_target_unblocks_when_its_readd_lands() {
        let (mut machine, peer, part) = replay_machine_with_one_part();
        let (page_id, page) = open_and_acknowledge_replay(&mut machine, &peer);
        let route = ReplayRoute::Part(part.clone());
        let first_id = machine.peers[&peer].replay_subscription.targets[&route];
        machine.handle_replay_page_result(
            page_id,
            test_retry(),
            ReplayPageResult {
                peer_id: peer.clone(),
                page: crate::rpc::ReplayPage {
                    events: Vec::new(),
                    targets: vec![(paged_route(&page, 0), TargetVerdict::UnknownPart)],
                },
                update: None,
            },
        );
        let fresh_id = machine.peers[&peer].replay_subscription.pending_additions[&route].id;

        // The retry runs and its update re-adds the part under the fresh id.
        machine
            .tasks
            .tick(std::time::Instant::now() + Duration::from_secs(3));
        let (retry_id, retry) = take_replay_task(&mut machine, "the retry runs its update");
        let subscription = retry
            .subscription
            .as_ref()
            .expect("the retry carries an update");
        let Some(crate::rpc::ReplaySubscriptionRequest::Update {
            additions,
            removals,
            ..
        }) = subscription.request.clone()
        else {
            panic!("the retry carries an update");
        };
        assert_eq!(
            additions
                .iter()
                .find(|entry| entry.id == fresh_id)
                .map(|entry| &entry.target),
            Some(&crate::rpc::ReplaySubscriptionTarget::Part {
                part_id: part.clone(),
            }),
            "the retry re-adds the same part under the fresh id"
        );
        assert!(
            removals.contains(&first_id),
            "the retry removes the entry the refusal came from"
        );
        machine.handle_replay_page_result(
            retry_id,
            test_retry(),
            empty_page_result(
                &peer,
                Some(ReplayUpdateOutcome::Applied {
                    generation: subscription.generation,
                    rejected: Vec::new(),
                }),
            ),
        );

        let state = &machine.peers[&peer].replay_subscription;
        assert!(
            state.blocked.is_empty(),
            "an update for the entry clears the block"
        );
        assert_eq!(state.targets[&route], fresh_id);
        let (_, page) = take_replay_task(&mut machine, "the unblocked route is paged again");
        assert_eq!(
            page.targets,
            vec![(
                fresh_id,
                SubscriptionTarget::Part {
                    part_id: part.clone(),
                    cursor: 0,
                },
            )],
            "the page names the acknowledged route under the id the update registered"
        );
    }

    /// A refusal paces the update, not the page: the routes of the same round that answered
    /// with events are asked for again immediately (ADR 012 decision 9).
    #[test]
    fn a_refused_route_does_not_pace_the_page_of_its_neighbours() {
        let refused_part = PartKey::random();
        let served_part = PartKey::random();
        let (mut machine, peer) =
            replay_machine_with_parts([refused_part.clone(), served_part.clone()]);
        let (page_id, page) = open_and_acknowledge_replay(&mut machine, &peer);
        let refused_route = ReplayRoute::Part(refused_part.clone());
        let verdicts = page
            .targets
            .into_iter()
            .map(|(_, route)| match &route {
                SubscriptionTarget::Part { part_id, .. } if *part_id == refused_part => {
                    (route, TargetVerdict::UnknownPart)
                }
                _ => (
                    route,
                    TargetVerdict::Events {
                        resume: 3,
                        drained: true,
                    },
                ),
            })
            .collect();
        machine.handle_replay_page_result(
            page_id,
            test_retry(),
            ReplayPageResult {
                peer_id: peer.clone(),
                page: crate::rpc::ReplayPage {
                    events: Vec::new(),
                    targets: verdicts,
                },
                update: None,
            },
        );

        assert!(
            machine.peers[&peer]
                .replay_subscription
                .blocked
                .contains(&refused_route),
            "the refused route is the blocked one"
        );
        let counts = machine.task_counts();
        assert_eq!(
            counts.delayed, 1,
            "only the refused route's re-add is delayed"
        );
        let spawned: Vec<_> = machine.drain_machine_spawn_queue().collect();
        assert_eq!(
            spawned.len(),
            1,
            "the route that answered with events is asked again now"
        );
    }

    /// Dropping a blocked target clears its block with it: only the client ever changes the
    /// wanted set, and the same part or object coming back is a fresh entry (ADR 012
    /// decision 9).
    #[test]
    fn dropping_a_blocked_target_clears_its_block() {
        let (mut machine, peer, part) = replay_machine_with_one_part();
        let (page_id, page) = open_and_acknowledge_replay(&mut machine, &peer);
        let route = ReplayRoute::Part(part.clone());
        machine.handle_replay_page_result(
            page_id,
            test_retry(),
            ReplayPageResult {
                peer_id: peer.clone(),
                page: crate::rpc::ReplayPage {
                    events: Vec::new(),
                    targets: vec![(paged_route(&page, 0), TargetVerdict::UnknownPart)],
                },
                update: None,
            },
        );
        assert!(
            machine.peers[&peer]
                .replay_subscription
                .blocked
                .contains(&route)
        );

        machine.handle_evt(BigSyncEvent::SetPeer(SetPeerEvent {
            peer_id: peer.clone(),
            parts: Set::new(),
            objects: Set::new(),
        }));

        let state = &machine.peers[&peer].replay_subscription;
        assert!(
            state.blocked.is_empty(),
            "the embedder dropping the target clears its block"
        );
        assert!(
            !state.pending_additions.contains_key(&route),
            "a dropped target is not re-added"
        );
        assert!(
            machine.drain_machine_spawn_queue().next().is_some(),
            "dropping the target tells the responder to remove its entry"
        );
    }

    #[test]
    fn replay_catch_up_pages_do_not_long_poll_until_caught_up() {
        let mut machine = BigSyncMachine::default();
        let peer = PeerKey::random();
        let part = PartKey::random();
        machine.handle_evt(BigSyncEvent::SetPeer(SetPeerEvent {
            peer_id: peer.clone(),
            parts: Set::new(),
            objects: Set::new(),
        }));
        machine
            .peers
            .get_mut(&peer)
            .expect("peer was inserted")
            .parts
            .insert(
                part.clone(),
                PeerPartState {
                    strat: PeerPartStrategy::Cursor(CursorState { replay_cursor: 0 }),
                },
            );
        machine.refresh_peer_replay_worker(peer.clone(), true);
        // A route the responder has not acknowledged is not pageable, so the forced refresh
        // opens the subscription first: one `Update` carrying the route's fresh entry.
        let opening = machine
            .drain_machine_spawn_queue()
            .next()
            .expect("forced refresh spawned the opening update");
        let MachineTask {
            id: opening_id,
            deets: MachineTaskDeets::ReplayPage(opening),
        } = opening
        else {
            panic!("forced refresh spawned a non-replay task");
        };
        assert!(opening.targets.is_empty(), "an update round pages nothing");
        assert_eq!(opening.hold_ms, 0, "an update round never long-polls");
        let opening_generation = opening
            .subscription
            .as_ref()
            .expect("the opening round belongs to a subscription")
            .generation;
        machine.handle_replay_page_result(
            opening_id,
            crate::scheduler::Retry {
                attempt_no: 0,
                backoff: Duration::ZERO,
                queued_at: std::time::Instant::now(),
            },
            ReplayPageResult {
                peer_id: peer.clone(),
                page: crate::rpc::ReplayPage {
                    events: Vec::new(),
                    targets: Vec::new(),
                },
                update: Some(ReplayUpdateOutcome::Applied {
                    generation: opening_generation,
                    rejected: Vec::new(),
                }),
            },
        );
        // The acknowledged route is pageable now, and a route with no caught-up verdict is
        // drain-only: the full-sync waiter must not pay for a live hold.
        let first = machine
            .drain_machine_spawn_queue()
            .next()
            .expect("an acknowledged route spawns a replay page");
        let MachineTask {
            deets: MachineTaskDeets::ReplayPage(first),
            ..
        } = first
        else {
            panic!("the acknowledged route spawned a non-replay task");
        };
        assert_eq!(first.hold_ms, 0);
        machine
            .peers
            .get_mut(&peer)
            .expect("peer was inserted")
            .replay_pages
            .values_mut()
            .next()
            .expect("replay route was inserted")
            .caught_up = true;
        machine.spawn_replay_pages(
            peer,
            vec![SubscriptionTarget::Part {
                part_id: part,
                cursor: 0,
            }],
            false,
        );
        let next = machine
            .drain_machine_spawn_queue()
            .next()
            .expect("caught-up refresh spawned a replay page");
        let MachineTask {
            deets: MachineTaskDeets::ReplayPage(next),
            ..
        } = next
        else {
            panic!("caught-up refresh spawned a non-replay task");
        };
        assert_eq!(next.hold_ms, ReplayPageTask::HOLD_MS);
    }

    /// A waiter registered for a peer+part must NOT remain stranded after that
    /// peer is removed. `SyncStatMachine::remove_peer` cleans up the peer and
    /// satisfies waiters when their last remaining peer is removed.
    #[test]
    fn full_sync_waiter_does_not_strand_on_peer_removal() {
        let mut stat = SyncStatMachine::default();

        let peer = PeerKey::random();
        let part = PartKey::random();

        // Register the peer and its part.
        stat.set_peer(peer.clone(), [part.clone()].into_iter());

        // Register a waiter needing that peer + part.
        stat.add_full_sync_waiter(1, [peer.clone()].into(), [part].into());

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

    /// An object route carries the position its replay has reached, so a store is asked
    /// for the object's next events instead of its first ones. Without it a route with
    /// more than one page re-reads its first page forever.
    #[test]
    fn an_object_route_is_refreshed_with_the_cursor_its_replay_acknowledged() {
        let mut machine = BigSyncMachine::default();
        let peer = PeerKey::random();
        let obj = ObjKey::random();
        machine.handle_evt(BigSyncEvent::SetPeer(SetPeerEvent {
            peer_id: peer.clone(),
            parts: Set::new(),
            objects: [obj.clone()].into(),
        }));

        let route = SubscriptionTarget::Object {
            obj_id: obj.clone(),
            cursor: 0,
        };
        assert_eq!(
            machine.refreshed_replay_target(peer.clone(), &route),
            Some(SubscriptionTarget::Object {
                obj_id: obj.clone(),
                cursor: 0,
            }),
            "nothing is acknowledged yet, so the route starts at the beginning"
        );

        {
            let peer_state = machine.peers.get_mut(&peer).expect(ERROR_UNRECONIZED);
            peer_state.cursor_machine.on_subscription_evt(
                crate::rpc::PartEvent::Changed(crate::rpc::ObjChanged {
                    cursor: 7,
                    part_ids: Vec::new(),
                    obj_id: obj.clone(),
                    payload: serde_json::json!({"head": 7}),
                }),
                &mut peer_state.cursors_cmd_buf,
            );
            peer_state.cursor_machine.on_obj_sync_job_evt(
                obj.clone(),
                7,
                CursorJobCompletionKind::Sync,
                &mut peer_state.cursors_cmd_buf,
            );
        }

        assert_eq!(
            machine.refreshed_replay_target(peer.clone(), &route),
            Some(SubscriptionTarget::Object {
                obj_id: obj.clone(),
                cursor: 7,
            }),
            "the re-issued page resumes from the acknowledged replay position"
        );
    }

    /// Regression guard: an object-target replay registers no part job, so the
    /// part JobBoard owes nothing for its cursors and its completion has to be
    /// settled by the claim `object_replays` holds. Dropping that completion left
    /// `acknowledged` at 0, so the route re-read the object's first page forever.
    #[test]
    fn an_object_only_replay_completion_advances_the_route_it_acknowledged() {
        let mut machine = BigSyncMachine::default();
        let peer = PeerKey::random();
        let obj = ObjKey::random();
        machine.handle_evt(BigSyncEvent::SetPeer(SetPeerEvent {
            peer_id: peer.clone(),
            parts: Set::new(),
            objects: [obj.clone()].into(),
        }));
        let route = SubscriptionTarget::Object {
            obj_id: obj.clone(),
            cursor: 0,
        };

        {
            let peer_state = machine.peers.get_mut(&peer).expect(ERROR_UNRECONIZED);
            peer_state.cursor_machine.on_subscription_evt(
                crate::rpc::PartEvent::Changed(crate::rpc::ObjChanged {
                    cursor: 7,
                    part_ids: Vec::new(),
                    obj_id: obj.clone(),
                    payload: serde_json::json!({"head": 7}),
                }),
                &mut peer_state.cursors_cmd_buf,
            );
        }
        machine.drain_cursor_machine_cmds(peer.clone());
        let task_id = machine.peers[&peer].sync_workers[&obj].task_id;

        // The object-only replay completed: the backend observed it, so the route
        // it belongs to must resume past it.
        machine.handle_evt(BigSyncEvent::SyncCompleted(SyncCompletedEvent {
            task_id,
            peer_id: peer.clone(),
            completion: SyncTaskCompletion {
                obj_id: obj.clone(),
                deets: SyncCompletionDeets::ChangedObject,
            },
        }));

        assert_eq!(
            machine.refreshed_replay_target(peer, &route),
            Some(SubscriptionTarget::Object {
                obj_id: obj,
                cursor: 7,
            }),
            "an object-target replay's completion must advance the route it acknowledged"
        );
    }

    #[test]
    fn removal_cancels_retrying_sync_task_when_its_last_part_is_removed() {
        let mut machine = BigSyncMachine::default();
        let peer = PeerKey::random();
        let part = PartKey::random();
        let obj = ObjKey::random();
        machine.handle_evt(BigSyncEvent::SetPeer(SetPeerEvent {
            peer_id: peer.clone(),
            parts: [part.clone()].into(),
            objects: Set::new(),
        }));

        {
            let peer_state = machine.peers.get_mut(&peer).expect(ERROR_UNRECONIZED);
            peer_state.cursor_machine.on_subscription_evt(
                crate::rpc::PartEvent::Changed(crate::rpc::ObjChanged {
                    cursor: 1,
                    part_ids: vec![part.clone()],
                    obj_id: obj.clone(),
                    payload: serde_json::json!({"head": 1}),
                }),
                &mut peer_state.cursors_cmd_buf,
            );
        }
        machine.drain_cursor_machine_cmds(peer.clone());
        let task_id = machine
            .peers
            .get(&peer)
            .and_then(|peer_state| peer_state.sync_workers.get(&obj))
            .map(|worker| worker.task_id)
            .expect("addition must start an object sync task");

        {
            let peer_state = machine.peers.get_mut(&peer).expect(ERROR_UNRECONIZED);
            peer_state.cursor_machine.on_subscription_evt(
                crate::rpc::PartEvent::Removed(crate::rpc::ObjRemovedFromPart {
                    cursor: 2,
                    part_id: part.clone(),
                    obj_id: obj.clone(),
                }),
                &mut peer_state.cursors_cmd_buf,
            );
        }
        machine.drain_cursor_machine_cmds(peer.clone());

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
            peer_id: peer.clone(),
            parts: [removed_part.clone(), remaining_part.clone()].into(),
            objects: Set::new(),
        }));
        {
            let peer_state = machine.peers.get_mut(&peer).expect(ERROR_UNRECONIZED);
            for part_id in [removed_part.clone(), remaining_part.clone()] {
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
                crate::rpc::PartEvent::Changed(crate::rpc::ObjChanged {
                    cursor: 1,
                    part_ids: vec![removed_part.clone(), remaining_part.clone()],
                    obj_id: obj.clone(),
                    payload: serde_json::json!({"head": 1}),
                }),
                &mut peer_state.cursors_cmd_buf,
            );
        }
        machine.drain_cursor_machine_cmds(peer.clone());
        let task_id = machine.peers[&peer].sync_workers[&obj].task_id;

        {
            let peer_state = machine.peers.get_mut(&peer).expect(ERROR_UNRECONIZED);
            peer_state.cursor_machine.on_subscription_evt(
                crate::rpc::PartEvent::Removed(crate::rpc::ObjRemovedFromPart {
                    cursor: 2,
                    part_id: removed_part,
                    obj_id: obj.clone(),
                }),
                &mut peer_state.cursors_cmd_buf,
            );
        }
        machine.drain_cursor_machine_cmds(peer.clone());

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
            peer_id: peer.clone(),
            parts: [part_a.clone(), part_b.clone()].into(),
            objects: Set::new(),
        }));

        {
            let peer_state = machine.peers.get_mut(&peer).expect(ERROR_UNRECONIZED);
            BigSyncMachine::schedule_obj_removal(
                &mut machine.tasks,
                &mut peer_state.remove_workers,
                peer.clone(),
                obj.clone(),
                part_a.clone(),
                Some(1),
            );
            BigSyncMachine::schedule_obj_removal(
                &mut machine.tasks,
                &mut peer_state.remove_workers,
                peer.clone(),
                obj.clone(),
                part_b.clone(),
                Some(2),
            );
        }
        let first_task = machine.peers[&peer].remove_workers[&obj].task_id;
        let spawned: Vec<_> = machine.drain_sync_spawn_queue().collect();
        assert_eq!(spawned.len(), 1);
        assert_eq!(
            spawned[0].part_hints,
            [part_a.clone(), part_b.clone()].into()
        );

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
                obj.clone(),
                part_a,
            ));
        }
        assert!(
            !machine.peers[&peer].remove_workers.contains_key(&obj),
            "cancelled removal worker must be removed from remove_workers"
        );
        let pending = &machine.peers[&peer].pending_removals[&obj];
        assert_eq!(pending.remaining_hints, [part_b.clone()].into());
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
            peer_id: peer.clone(),
            obj_id: obj.clone(),
        }));
        let worker = &machine.peers[&peer].remove_workers[&obj];
        assert_eq!(worker.part_hints, [part_b.clone()].into());
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
            peer_id: peer.clone(),
            parts: [part.clone()].into(),
            objects: Set::new(),
        }));

        // Schedule a removal for the object in the part.
        {
            let peer_state = machine.peers.get_mut(&peer).expect(ERROR_UNRECONIZED);
            BigSyncMachine::schedule_obj_removal(
                &mut machine.tasks,
                &mut peer_state.remove_workers,
                peer.clone(),
                obj.clone(),
                part.clone(),
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
                crate::rpc::PartEvent::Changed(crate::rpc::ObjChanged {
                    cursor: 2,
                    part_ids: vec![part.clone()],
                    obj_id: obj.clone(),
                    payload: serde_json::json!({"head": 2}),
                }),
                &mut peer_state.cursors_cmd_buf,
            );
        }
        machine.drain_cursor_machine_cmds(peer.clone());

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
        assert_eq!(pending.re_added_parts, [part.clone()].into());

        // The zombie's completion lands: the deferred re-sync is issued.
        machine.handle_evt(BigSyncEvent::RemoveCompleted(RemoveCompletedEvent {
            task_id: removal_task,
            peer_id: peer.clone(),
            obj_id: obj.clone(),
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

    /// The real event path: a `Removed` then a `Changed` for the same part and
    /// object cancels the removal task and leaves two cursors pending — one
    /// owing the membership lane, one owing the sync lane.
    ///
    /// Handing that whole pool to both workers settled the membership cursor's
    /// sync lane (which it never owed: panic) and left its membership lane with
    /// no worker at all (part watermark stuck). Each lane must be settled by the
    /// task that applies that half of the work, and by nothing else.
    #[test]
    fn readd_settles_each_lane_from_the_worker_that_owes_it() {
        let mut machine = BigSyncMachine::default();
        let peer = PeerKey::random();
        let part = PartKey::random();
        let obj = ObjKey::random();
        machine.handle_evt(BigSyncEvent::SetPeer(SetPeerEvent {
            peer_id: peer.clone(),
            parts: [part.clone()].into(),
            objects: Set::new(),
        }));
        {
            // The part is past replay, so a cursor advance shows up as its
            // replay cursor instead of being dropped as pending.
            let peer_state = machine.peers.get_mut(&peer).expect(ERROR_UNRECONIZED);
            peer_state
                .parts
                .get_mut(&part)
                .expect(ERROR_UNRECONIZED)
                .strat = PeerPartStrategy::Cursor(CursorState { replay_cursor: 0 });
        }

        // Removed from the part: cursor 1 owes the membership lane.
        {
            let peer_state = machine.peers.get_mut(&peer).expect(ERROR_UNRECONIZED);
            peer_state.cursor_machine.on_subscription_evt(
                crate::rpc::PartEvent::Removed(crate::rpc::ObjRemovedFromPart {
                    cursor: 1,
                    part_id: part.clone(),
                    obj_id: obj.clone(),
                }),
                &mut peer_state.cursors_cmd_buf,
            );
        }
        machine.drain_cursor_machine_cmds(peer.clone());
        let removal_task = machine.peers[&peer].remove_workers[&obj].task_id;
        assert_eq!(
            machine.peers[&peer].remove_workers[&obj].cursors,
            [1].into()
        );
        machine.drain_sync_spawn_queue();

        // Re-added to the same part: cursor 2 owes the sync lane, the removal
        // hint is cancelled, and the re-sync is deferred until the zombie's
        // terminal event lands.
        {
            let peer_state = machine.peers.get_mut(&peer).expect(ERROR_UNRECONIZED);
            peer_state.cursor_machine.on_subscription_evt(
                crate::rpc::PartEvent::Changed(crate::rpc::ObjChanged {
                    cursor: 2,
                    part_ids: vec![part.clone()],
                    obj_id: obj.clone(),
                    payload: serde_json::json!({"head": 2}),
                }),
                &mut peer_state.cursors_cmd_buf,
            );
        }
        machine.drain_cursor_machine_cmds(peer.clone());
        {
            let pending = &machine.peers[&peer].pending_removals[&obj];
            assert_eq!(pending.cursors, [1, 2].into());
            assert_eq!(pending.re_added_parts, [part.clone()].into());
            assert!(pending.remaining_hints.is_empty());
        }
        assert!(machine.drain_sync_spawn_queue().next().is_none());
        assert_eq!(
            part_replay_cursor(&machine, &peer, &part),
            0,
            "neither lane has settled yet, so the part must not have advanced"
        );

        // The zombie's completion: the removal it queued did run, so cursor 1's
        // membership lane settles here, and the re-sync is issued for the
        // re-added part alone.
        machine.handle_evt(BigSyncEvent::RemoveCompleted(RemoveCompletedEvent {
            task_id: removal_task,
            peer_id: peer.clone(),
            obj_id: obj.clone(),
        }));
        assert_eq!(
            part_replay_cursor(&machine, &peer, &part),
            1,
            "the membership lane settling must advance the part to its own cursor"
        );
        let spawned: Vec<_> = machine.drain_sync_spawn_queue().collect();
        assert_eq!(spawned.len(), 1);
        assert_eq!(spawned[0].kind, SyncTaskKind::Sync);
        let sync_task = machine.peers[&peer].sync_workers[&obj].task_id;
        assert_eq!(
            machine.peers[&peer].sync_workers[&obj].cursors,
            [2].into(),
            "the sync worker must not be handed the membership-only cursor"
        );

        // Completing the re-sync settles cursor 2's sync lane and advances the
        // part past it.
        machine.handle_evt(BigSyncEvent::SyncCompleted(SyncCompletedEvent {
            task_id: sync_task,
            peer_id: peer.clone(),
            completion: SyncTaskCompletion {
                obj_id: obj.clone(),
                deets: SyncCompletionDeets::ChangedObject,
            },
        }));
        assert_eq!(part_replay_cursor(&machine, &peer, &part), 2);
        let cursor_machine = &machine.peers[&peer].cursor_machine;
        assert!(
            !cursor_machine.owes_obj_job_lane(&obj, 1, CursorJobCompletionKind::Membership),
            "the membership lane must be settled once its task completed"
        );
        assert!(
            !cursor_machine.owes_obj_job_lane(&obj, 2, CursorJobCompletionKind::Sync),
            "the sync lane must be settled once its task completed"
        );
        assert!(
            !cursor_machine.owes_obj_job_lane(&obj, 1, CursorJobCompletionKind::Sync),
            "the membership cursor was never owed a sync lane"
        );
    }

    #[test]
    fn a_failed_cancelled_removal_still_finishes_its_membership_lanes() {
        let mut machine = BigSyncMachine::default();
        let peer = PeerKey::random();
        let part = PartKey::random();
        let obj = ObjKey::random();
        machine.handle_evt(BigSyncEvent::SetPeer(SetPeerEvent {
            peer_id: peer.clone(),
            parts: [part.clone()].into(),
            objects: Set::new(),
        }));
        {
            let peer_state = machine.peers.get_mut(&peer).expect(ERROR_UNRECONIZED);
            peer_state
                .parts
                .get_mut(&part)
                .expect(ERROR_UNRECONIZED)
                .strat = PeerPartStrategy::Cursor(CursorState { replay_cursor: 0 });
        }

        // Removed from the part: cursor 1 owes the membership lane.
        {
            let peer_state = machine.peers.get_mut(&peer).expect(ERROR_UNRECONIZED);
            peer_state.cursor_machine.on_subscription_evt(
                crate::rpc::PartEvent::Removed(crate::rpc::ObjRemovedFromPart {
                    cursor: 1,
                    part_id: part.clone(),
                    obj_id: obj.clone(),
                }),
                &mut peer_state.cursors_cmd_buf,
            );
        }
        machine.drain_cursor_machine_cmds(peer.clone());
        let removal_task = machine.peers[&peer].remove_workers[&obj].task_id;
        machine.drain_sync_spawn_queue();

        // Re-added before the removal landed: the removal's only hint is
        // cancelled, so no re-removal is left to run and the re-sync defers
        // until the cancelled task's terminal event lands.
        {
            let peer_state = machine.peers.get_mut(&peer).expect(ERROR_UNRECONIZED);
            peer_state.cursor_machine.on_subscription_evt(
                crate::rpc::PartEvent::Changed(crate::rpc::ObjChanged {
                    cursor: 2,
                    part_ids: vec![part.clone()],
                    obj_id: obj.clone(),
                    payload: serde_json::json!({"head": 2}),
                }),
                &mut peer_state.cursors_cmd_buf,
            );
        }
        machine.drain_cursor_machine_cmds(peer.clone());
        {
            let pending = &machine.peers[&peer].pending_removals[&obj];
            assert!(pending.remaining_hints.is_empty());
            assert_eq!(pending.re_added_parts, [part.clone()].into());
        }
        assert_eq!(part_replay_cursor(&machine, &peer, &part), 0);

        // The cancelled task never applied the removal. The re-add supersedes
        // it, and with no re-removal left nothing else could ever settle cursor
        // 1's membership lane, so it must finish here instead of freezing the
        // part's cursor for the life of the route.
        machine.handle_evt(BigSyncEvent::RemoveFailed(RemoveFailedEvent {
            task_id: removal_task,
            peer_id: peer.clone(),
            obj_id: obj.clone(),
            err: eyre::eyre!("removal task failed"),
        }));
        assert_eq!(
            part_replay_cursor(&machine, &peer, &part),
            1,
            "the mooted membership lane must settle even though the task failed"
        );
        assert!(
            !machine.peers[&peer].cursor_machine.owes_obj_job_lane(
                &obj,
                1,
                CursorJobCompletionKind::Membership
            ),
            "no lane may be left owed that no worker will settle"
        );
        let spawned: Vec<_> = machine.drain_sync_spawn_queue().collect();
        assert_eq!(spawned.len(), 1, "the re-add still needs its sync");
        assert_eq!(spawned[0].kind, SyncTaskKind::Sync);
        assert_eq!(machine.peers[&peer].sync_workers[&obj].cursors, [2].into());
    }

    fn part_replay_cursor(machine: &BigSyncMachine, peer: &PeerKey, part: &PartKey) -> CursorIndex {
        let part = machine
            .peers
            .get(peer)
            .expect(ERROR_UNRECONIZED)
            .parts
            .get(part)
            .expect(ERROR_UNRECONIZED);
        match &part.strat {
            PeerPartStrategy::Cursor(state) => state.replay_cursor,
            _ => panic!("the part must be in its cursor phase for an advance to be visible"),
        }
    }

    #[test]
    fn removed_peer_part_cancels_deferred_readd_before_removal_completes() {
        let mut machine = BigSyncMachine::default();
        let peer = PeerKey::random();
        let part = PartKey::random();
        let obj = ObjKey::random();
        machine.handle_evt(BigSyncEvent::SetPeer(SetPeerEvent {
            peer_id: peer.clone(),
            parts: [part.clone()].into(),
            objects: Set::new(),
        }));

        {
            let peer_state = machine.peers.get_mut(&peer).expect(ERROR_UNRECONIZED);
            BigSyncMachine::schedule_obj_removal(
                &mut machine.tasks,
                &mut peer_state.remove_workers,
                peer.clone(),
                obj.clone(),
                part.clone(),
                Some(1),
            );
        }
        let removal_task = machine.peers[&peer].remove_workers[&obj].task_id;
        machine.drain_sync_spawn_queue();

        {
            let peer_state = machine.peers.get_mut(&peer).expect(ERROR_UNRECONIZED);
            peer_state.cursor_machine.on_subscription_evt(
                crate::rpc::PartEvent::Changed(crate::rpc::ObjChanged {
                    cursor: 2,
                    part_ids: vec![part.clone()],
                    obj_id: obj.clone(),
                    payload: serde_json::json!({"head": 2}),
                }),
                &mut peer_state.cursors_cmd_buf,
            );
        }
        machine.drain_cursor_machine_cmds(peer.clone());
        assert_eq!(
            machine.peers[&peer].pending_removals[&obj].re_added_parts,
            [part].into()
        );

        machine.handle_evt(BigSyncEvent::SetPeer(SetPeerEvent {
            peer_id: peer.clone(),
            parts: Set::new(),
            objects: Set::new(),
        }));
        assert!(machine.peers[&peer].pending_removals.is_empty());

        machine.handle_evt(BigSyncEvent::RemoveCompleted(RemoveCompletedEvent {
            task_id: removal_task,
            peer_id: peer.clone(),
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
            peer_id: peer.clone(),
            parts: [removed_part.clone(), retained_part.clone()].into(),
            objects: Set::new(),
        }));

        let task_id = machine.tasks.spawn(
            std::time::Instant::now(),
            TaskSeed::Sync(SyncTaskSeed {
                kind: SyncTaskKind::Sync,
                part_hints: [removed_part.clone(), retained_part.clone()].into(),
                deets: SyncTaskDeets {
                    peer_id: peer.clone(),
                    obj_id: obj.clone(),
                    remote_payload: Some(serde_json::json!({"head": 1})),
                },
            }),
        );
        machine
            .peers
            .get_mut(&peer)
            .expect(ERROR_UNRECONIZED)
            .sync_workers
            .insert(
                obj.clone(),
                SyncWorkerState {
                    task_id,
                    cursors: [1].into(),
                    part_hints: [removed_part, retained_part.clone()].into(),
                    remote_payload: Some(serde_json::json!({"head": 1})),
                },
            );

        machine.handle_evt(BigSyncEvent::SetPeer(SetPeerEvent {
            peer_id: peer.clone(),
            parts: [retained_part.clone()].into(),
            objects: Set::new(),
        }));

        let tasks: Vec<_> = machine.drain_sync_spawn_queue().collect();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].part_hints, [retained_part.clone()].into());
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
            peer_id: peer.clone(),
            parts: [part.clone()].into(),
            objects: Set::new(),
        }));

        {
            let peer_state = machine.peers.get_mut(&peer).expect(ERROR_UNRECONIZED);
            BigSyncMachine::schedule_obj_removal(
                &mut machine.tasks,
                &mut peer_state.remove_workers,
                peer.clone(),
                obj.clone(),
                part,
                Some(1),
            );
        }
        let removal_task = machine.peers[&peer].remove_workers[&obj].task_id;
        machine.drain_sync_spawn_queue();

        machine.handle_evt(BigSyncEvent::RemovePeer(RemovePeerEvent {
            peer_id: peer.clone(),
        }));

        assert!(!machine.peers.contains_key(&peer));
        assert!(
            machine
                .drain_stop_queue()
                .any(|stopped| stopped == removal_task)
        );
    }

    /// An object-routed content fetch is not obsolete when a membership removal
    /// empties its part hints: the *route* owes the content. Stopping the task
    /// also abandoned the object's replay claim, and object pages are only
    /// re-issued when a route is rebuilt, so the peer never got the payload.
    #[test]
    fn a_removal_keeps_an_object_routed_content_fetch_alive() {
        let mut machine = BigSyncMachine::default();
        let peer = PeerKey::random();
        let part = PartKey::random();
        let obj = ObjKey::random();
        machine.handle_evt(BigSyncEvent::SetPeer(SetPeerEvent {
            peer_id: peer.clone(),
            parts: [part.clone()].into(),
            objects: [obj.clone()].into(),
        }));

        // Touched on the part it sits in, which leaves the worker a hint while
        // its route is the object's.
        {
            let peer_state = machine.peers.get_mut(&peer).expect(ERROR_UNRECONIZED);
            peer_state.cursor_machine.on_subscription_evt(
                crate::rpc::PartEvent::Changed(crate::rpc::ObjChanged {
                    cursor: 5,
                    part_ids: vec![part.clone()],
                    obj_id: obj.clone(),
                    payload: serde_json::json!({"head": 5}),
                }),
                &mut peer_state.cursors_cmd_buf,
            );
        }
        machine.drain_cursor_machine_cmds(peer.clone());
        assert_eq!(
            machine.peers[&peer].sync_workers[&obj].part_hints,
            [part.clone()].into()
        );

        {
            let peer_state = machine.peers.get_mut(&peer).expect(ERROR_UNRECONIZED);
            peer_state.cursor_machine.on_subscription_evt(
                crate::rpc::PartEvent::Removed(crate::rpc::ObjRemovedFromPart {
                    cursor: 6,
                    part_id: part.clone(),
                    obj_id: obj.clone(),
                }),
                &mut peer_state.cursors_cmd_buf,
            );
        }
        machine.drain_cursor_machine_cmds(peer.clone());

        assert!(
            machine.peers[&peer].sync_workers.contains_key(&obj),
            "the object route still owes the object's content"
        );
        // The claim survived as well: an abandoned claim drops this
        // acknowledgement, which is what makes the route re-read page one.
        {
            let peer_state = machine.peers.get_mut(&peer).expect(ERROR_UNRECONIZED);
            peer_state.cursor_machine.on_obj_sync_job_evt(
                obj.clone(),
                5,
                CursorJobCompletionKind::Sync,
                &mut peer_state.cursors_cmd_buf,
            );
        }
        assert_eq!(
            machine.refreshed_replay_target(
                peer.clone(),
                &SubscriptionTarget::Object {
                    obj_id: obj.clone(),
                    cursor: 0,
                },
            ),
            Some(SubscriptionTarget::Object {
                obj_id: obj.clone(),
                cursor: 5,
            }),
            "the surviving route still acknowledges the replay it was owed"
        );
    }

    /// Control for the test above: a worker with no object route is still stopped
    /// once its last part hint is removed, so this does not make removals inert.
    #[test]
    fn a_removal_still_stops_a_part_routed_fetch_with_no_hints_left() {
        let mut machine = BigSyncMachine::default();
        let peer = PeerKey::random();
        let part = PartKey::random();
        let obj = ObjKey::random();
        machine.handle_evt(BigSyncEvent::SetPeer(SetPeerEvent {
            peer_id: peer.clone(),
            parts: [part.clone()].into(),
            objects: Set::new(),
        }));

        {
            let peer_state = machine.peers.get_mut(&peer).expect(ERROR_UNRECONIZED);
            peer_state.cursor_machine.on_subscription_evt(
                crate::rpc::PartEvent::Changed(crate::rpc::ObjChanged {
                    cursor: 5,
                    part_ids: vec![part.clone()],
                    obj_id: obj.clone(),
                    payload: serde_json::json!({"head": 5}),
                }),
                &mut peer_state.cursors_cmd_buf,
            );
        }
        machine.drain_cursor_machine_cmds(peer.clone());
        assert!(machine.peers[&peer].sync_workers.contains_key(&obj));

        {
            let peer_state = machine.peers.get_mut(&peer).expect(ERROR_UNRECONIZED);
            peer_state.cursor_machine.on_subscription_evt(
                crate::rpc::PartEvent::Removed(crate::rpc::ObjRemovedFromPart {
                    cursor: 6,
                    part_id: part.clone(),
                    obj_id: obj.clone(),
                }),
                &mut peer_state.cursors_cmd_buf,
            );
        }
        machine.drain_cursor_machine_cmds(peer.clone());

        assert!(
            !machine.peers[&peer].sync_workers.contains_key(&obj),
            "a part-routed worker with no hints left is obsolete"
        );
    }
}
