//! FIXME: find a way to avoid blocking on BigSyncMachineCommands
//! A part the peer answers it does not know keeps full sync blocked until the embedder
//! drops it from the subscription set: the refusal is usually a race, and the machine
//! does not decide that a part its peer cannot answer for is synced. The answer is
//! reported once per transition as [`SyncStatEvent::PeerPartUnanswered`].

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
        /// One logical target registry per replay lane. Cursors remain in the page task and
        /// cursor machine; this state only avoids repeating stable target metadata on the wire.
        replay_subscriptions: Map<ReplayLane, struct ReplaySubscriptionState {
            subscription_id: crate::rpc::ReplaySubscriptionId,
            generation: u64,
            next_target_id: u32,
            opened: bool,
            targets: Map<ReplayRoute, crate::rpc::ReplayTargetId>,
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
    pub fn debug_peer_part_sync_flags(&self) -> Vec<(PeerKey, PartKey, bool, bool, bool, bool)> {
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

        /// Strategy hint for parts with no explicit per-part override. A caller that
        /// wants the bucket path for every part it syncs sets this to
        /// [`SyncMode::Bucket`].
        default_sync_mode: SyncMode,

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
            replay_subscriptions: default(),
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
        self.spawn_replay_pages_inner(peer_id.clone(), live, caught_up, None);
        self.spawn_replay_pages_inner(peer_id, bulk, caught_up, None);
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
        self.spawn_replay_pages_inner(peer_id.clone(), live, caught_up, Some((retry, delay)));
        self.spawn_replay_pages_inner(peer_id, bulk, caught_up, Some((retry, delay)));
    }

    fn spawn_replay_pages_inner(
        &mut self,
        peer_id: PeerKey,
        targets: Vec<SubscriptionTarget>,
        caught_up: bool,
        delayed: Option<(Retry, Duration)>,
    ) {
        if targets.is_empty() {
            return;
        }
        self.replay_request_seq = self.replay_request_seq.wrapping_add(1);
        let request_id = crate::rpc::ReplayRequestId(self.replay_request_seq);
        let routes: Vec<ReplayRoute> = targets.iter().map(ReplayRoute::of).collect();
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
        let lane = self.replay_lane(&peer_id, &targets[0]);
        let subscription = self.peers.get_mut(&peer_id).map(|peer_state| {
            let state = peer_state
                .replay_subscriptions
                .entry(lane)
                .or_insert_with(|| {
                    let subscription_id = crate::rpc::ReplaySubscriptionId(match lane {
                        ReplayLane::Live => 1,
                        ReplayLane::Bulk => 2,
                    });
                    ReplaySubscriptionState {
                        subscription_id,
                        generation: 0,
                        next_target_id: 1,
                        opened: false,
                        targets: Map::new(),
                    }
                });
            let current_routes: Set<_> = routes.iter().cloned().collect();
            let mut additions = Vec::new();
            for target in &targets {
                let route = ReplayRoute::of(target);
                if !state.targets.contains_key(&route) {
                    let target_id = crate::rpc::ReplayTargetId(state.next_target_id);
                    state.next_target_id =
                        state.next_target_id.checked_add(1).expect(ERROR_IMPOSSIBLE);
                    state.targets.insert(route, target_id);
                    additions.push(crate::rpc::ReplaySubscriptionTargetEntry {
                        id: target_id,
                        target: crate::rpc::ReplaySubscriptionTarget::from(target),
                    });
                }
            }
            let removals: Vec<_> = state
                .targets
                .iter()
                .filter(|(route, _)| !current_routes.contains(*route))
                .map(|(route, target_id)| (route.clone(), *target_id))
                .collect();
            for (route, _) in &removals {
                state.targets.remove(route);
            }
            let entries: Vec<_> = targets
                .iter()
                .map(|target| {
                    let route = ReplayRoute::of(target);
                    crate::rpc::ReplaySubscriptionTargetEntry {
                        id: state.targets[&route],
                        target: crate::rpc::ReplaySubscriptionTarget::from(target),
                    }
                })
                .collect();
            let request = if !state.opened {
                state.opened = true;
                Some(crate::rpc::ReplaySubscriptionRequest::Open {
                    subscription_id: state.subscription_id,
                    generation: state.generation,
                    targets: entries.clone(),
                })
            } else if !additions.is_empty() || !removals.is_empty() {
                state.generation = state.generation.checked_add(1).expect(ERROR_IMPOSSIBLE);
                Some(crate::rpc::ReplaySubscriptionRequest::Update {
                    subscription_id: state.subscription_id,
                    generation: state.generation,
                    additions,
                    removals: removals.iter().map(|(_, target_id)| *target_id).collect(),
                })
            } else {
                None
            };
            ReplaySubscriptionTaskState {
                subscription_id: state.subscription_id,
                generation: state.generation,
                targets: entries,
                request,
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
            request_id,
            targets,
            supersede,
            limit: ReplayPageTask::LIMIT,
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
    fn update_peer_replay_done(&mut self, peer_id: PeerKey) {
        let routes = self.replay_page_targets(peer_id.clone());
        let done = routes.keys().all(|route| {
            self.peers
                .get(&peer_id)
                .and_then(|peer_state| peer_state.replay_pages.get(route))
                .is_some_and(|state| state.caught_up)
        });
        self.stat_machine.mark_peer_replay_done(peer_id, done);
    }

    fn refresh_peer_replay_worker(&mut self, peer_id: PeerKey, force: bool) {
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
        // The events are page-level: they are applied even when a newer round has replaced
        // this one, because re-delivering them costs a watermark comparison while dropping
        // them costs a round. The verdicts are per route, and a verdict for a route this round
        // no longer owns is dropped: the round that replaced it has already been asked, and
        // its answer is the one that counts.
        let mut immediate: Vec<SubscriptionTarget> = Vec::new();
        let mut backed_off: Vec<SubscriptionTarget> = Vec::new();
        let mut backoff: Option<Duration> = None;
        let mut unanswered: Vec<PartKey> = Vec::new();
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
                        immediate.push(target);
                    }
                    TargetVerdict::UnknownPart => {
                        // The peer does not know this part (yet). That is not the part being
                        // synced: record it as unanswered so full sync stays blocked, keep the
                        // route and retry slowly — a restarting peer re-creates its part rows,
                        // and dropping the route here would tear it permanently.
                        if let SubscriptionTarget::Part { part_id, .. } = &target {
                            unanswered.push(part_id.clone());
                        }
                        if let Some(state) = peer_state.replay_pages.get_mut(&route) {
                            state.caught_up = false;
                        }
                        backed_off.push(target);
                        backoff = Some(Duration::from_secs(2));
                    }
                    TargetVerdict::Unauthorized => {
                        // Absent access rows cannot distinguish a revocation from a grant that
                        // has not landed yet, so back off rather than tear the route down:
                        // dropping it here would strand a part whose grant is still in flight.
                        // This caller is done with the route either way, and it is deliberately
                        // not recorded as unanswered — blocking full sync on a part this caller
                        // may never read hangs every topology whose access matrix leaves a part
                        // unreadable to one side.
                        if let Some(state) = peer_state.replay_pages.get_mut(&route) {
                            state.caught_up = true;
                        }
                        backed_off.push(target);
                        backoff = Some(UNAUTHORIZED_BACKOFF);
                    }
                }
            }
        }
        for part_id in unanswered {
            self.stat_machine
                .mark_peer_part_unanswered(peer_id.clone(), part_id);
        }
        self.drain_cursor_machine_cmds(peer_id.clone());
        // The next round asks for the same routes at their current positions, which is the
        // cursor machine's own bookkeeping: an applied event moved a position, and a route
        // that answered with nothing has nothing to move. A route retired while this round was
        // in flight is dropped here rather than asked for again.
        let immediate = self.refresh_replay_targets(peer_id.clone(), immediate);
        let backed_off = self.refresh_replay_targets(peer_id.clone(), backed_off);
        if self.replay_work_below_watermark(&peer_id) {
            if !immediate.is_empty() {
                self.spawn_replay_pages(peer_id.clone(), immediate, false);
            }
            if !backed_off.is_empty() {
                // A round that saw both an unknown and an unauthorized target backs off for the
                // larger of the two: a delay is pacing, and both routes are retried either way.
                let delay = backoff.unwrap_or_else(|| Duration::from_secs(2));
                self.schedule_replay_pages(peer_id.clone(), backed_off, false, retry, delay);
            }
        } else {
            self.pause_replay_targets_for_credit(&peer_id, immediate.into_iter().chain(backed_off));
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
        if live.is_empty() {
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
