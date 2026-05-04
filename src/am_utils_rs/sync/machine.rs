//! TODO: emit sync stats event for use in full.rs

use crate::interlude::*;

use crate::partition::PartitionStore;
use crate::sync::protocol::{
    CursorIndex, PartitionId, PartitionItemEventDeets, PartitionMemberEventDeets, PeerKey,
    SubscriptionEvent, SubscriptionStreamKind,
};
use crate::sync::store::SyncStoreHandle;

use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ItemSyncKind {
    New,
    Change,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ItemSyncKey {
    pub peer: PeerKey,
    pub kind: ItemSyncKind,
    pub item_id: Arc<str>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct ItemJobId {
    peer: PeerKey,
    item_id: Arc<str>,
}

#[derive(Debug, Clone)]
pub struct CursorWaiter {
    pub peer: PeerKey,
    pub partition_id: PartitionId,
    pub sync_kind: ItemSyncKind,
    pub stream_event: SubscriptionStreamKind,
}

/// Look at [`on_item_sync_completed`] impl for how this
/// actually works in more detail.
///
/// [`on_item_sync_completed`]: SyncMachine::on_item_sync_completed
#[derive(Debug, Clone)]
struct ItemJobState {
    /// Since a item can be a member of multiple partitions
    /// from a single peer and be involved in multiple
    /// events (consquetive changes) we, have these events
    /// wait on the same job to dedpe work.
    waiters: BTreeMap<CursorIndex, CursorWaiter>,
    /// Since the job represents a waiting state for multiple
    /// cursor events, we must ensure to tend to the last one.
    ///
    /// This is only ever Delete if the item is deleted from all
    /// local partitions.
    last_change_kind: ItemSyncKind,
    /// This is true if no new cursor waiters
    /// have been added since we last dispatched
    /// a command for this job.
    ///
    /// We must dispatch a command again for an item
    /// if a new change comes in for it while we're
    /// waiting for a previous sync command to complete.
    /// We only advance the waiting cursors as we detect
    /// this rest state.
    dirty: bool,
    /// Any cursors below these can be assumed
    /// to be processsed
    high_water_at_last_dispatch: CursorIndex,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncMachineCommand {
    ItemNewSync { key: ItemSyncKey },
    ItemChangeSync { key: ItemSyncKey },
    ItemDeleteSync { key: ItemSyncKey },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncCompletion {
    AddedMember {
        peer: PeerKey,
        item_id: Arc<str>,
        item_payload: serde_json::Value,
    },
    /// NOTE: changed item doesn't carry payloads
    /// modifiers are expected to update the partition
    /// store instead using [`record_item_change`].
    ///
    /// [`upsert_item`] can be used for local additions.
    /// This is to allow BigRepoRuntime and other local systems
    /// to indicate item additions/changes in a single transactional
    /// context and avoid event based data loss.
    /// Cursor based frontiers are reliable enough to ensure
    /// no work is lost when processing remote events but
    /// this is the best for now when it comes to local changes.
    ///
    /// [`record_item_change`]: crate::partition::PartitionStore::record_item_change
    /// [`upsert_item`]: crate::partition::PartitionStore::upsert_item
    ChangedItem {
        peer: PeerKey,
        item_id: Arc<str>,
    },
    DeletedMember {
        peer: PeerKey,
        item_id: Arc<str>,
    },
    Noop {
        peer: PeerKey,
        item_id: Arc<str>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CursorSlotState {
    Pending,
    Ready,
}

#[derive(Debug, Default)]
struct CursorStreamState {
    // FIXME: last_emitted_cursor and persisted_cursor are identical
    persisted_cursor: Option<CursorIndex>,
    last_emitted_cursor: Option<CursorIndex>,
    slots: BTreeMap<CursorIndex, CursorSlotState>,
}

impl CursorStreamState {
    fn floor(&self) -> CursorIndex {
        self.persisted_cursor
            .unwrap_or(0)
            .max(self.last_emitted_cursor.unwrap_or(0))
    }
}

#[derive(Debug)]
pub struct SyncMachine {
    partition_store: Arc<PartitionStore>,
    sync_store: SyncStoreHandle,
    cursor_state: HashMap<PeerKey, HashMap<PartitionId, PartitionCursorState>>,
    active_item_jobs: BTreeMap<ItemJobId, ItemJobState>,
}

#[derive(Debug, Default)]
struct PartitionCursorState {
    member: CursorStreamState,
    item: CursorStreamState,
}

impl SyncMachine {
    pub fn new(partition_store: Arc<PartitionStore>, sync_store: SyncStoreHandle) -> Self {
        Self {
            partition_store,
            sync_store,
            cursor_state: HashMap::new(),
            active_item_jobs: BTreeMap::new(),
        }
    }

    pub fn clear_peer(&mut self, peer: &PeerKey) {
        self.cursor_state.remove(peer);
        self.active_item_jobs
            .retain(|job_id, _| &job_id.peer != peer);
    }

    pub async fn on_subscription_item(
        &mut self,
        peer: PeerKey,
        remote_item: SubscriptionEvent,
    ) -> Res<Vec<SyncMachineCommand>> {
        let (sync_kind, item_id, partition_id, cursor, sub_kind) = match remote_item {
            SubscriptionEvent::ReplayComplete { .. } => return Ok(Vec::new()),
            SubscriptionEvent::Lagged { dropped } => {
                eyre::bail!("partition subscription lagged; dropped={dropped}")
            }
            SubscriptionEvent::MemberEvent(event) => {
                let (kind, item_id) = match event.deets {
                    PartitionMemberEventDeets::MemberUpsert { item_id } => {
                        (ItemSyncKind::New, item_id)
                    }
                    PartitionMemberEventDeets::MemberRemoved { item_id } => {
                        (ItemSyncKind::Delete, item_id)
                    }
                };
                (
                    kind,
                    item_id,
                    Arc::clone(&event.partition_id),
                    event.cursor,
                    SubscriptionStreamKind::Member,
                )
            }
            SubscriptionEvent::ItemEvent(event) => {
                let (kind, item_id) = match event.deets {
                    PartitionItemEventDeets::ItemChanged { item_id, .. } => {
                        (ItemSyncKind::Change, item_id)
                    }
                    PartitionItemEventDeets::ItemDeleted { item_id, .. } => {
                        (ItemSyncKind::Delete, item_id)
                    }
                };
                (
                    kind,
                    item_id,
                    Arc::clone(&event.partition_id),
                    event.cursor,
                    SubscriptionStreamKind::Item,
                )
            }
        };
        // clone self field AOT to avoid stream_state_mut mutable borrow
        // leading to borrow issues
        let part_store = self.partition_store.clone();
        let state = self.stream_state_mut(&peer, &partition_id, sub_kind);
        if cursor <= state.floor() {
            panic!(
                "cursority trap: cursor ({cursor}) seen below floor ({})",
                state.floor()
            );
        }
        if matches!(sync_kind, ItemSyncKind::Delete) {
            let item_payloads_len = part_store.item_payloads(&item_id).await?.len();
            if item_payloads_len > 1 {
                part_store
                    .remove_item(&partition_id, Arc::clone(&item_id))
                    .await?;
                let old = state.slots.insert(cursor, CursorSlotState::Ready);
                assert!(old.is_none(), "fishy");
                // if item is still in other partitons, no need for a delete command
                return Ok(default());
            }
        }
        let old = state.slots.insert(cursor, CursorSlotState::Pending);
        assert!(old.is_none(), "fishy");

        let key = ItemSyncKey {
            peer: peer.clone(),
            kind: sync_kind,
            item_id,
        };
        let job_id = ItemJobId {
            peer: Arc::clone(&key.peer),
            item_id: Arc::clone(&key.item_id),
        };
        let entry = self
            .active_item_jobs
            .entry(job_id)
            .or_insert_with(|| ItemJobState {
                dirty: true,
                high_water_at_last_dispatch: cursor,
                last_change_kind: key.kind,
                waiters: default(),
            });
        entry.dirty = true;
        assert!(entry.high_water_at_last_dispatch < cursor, "fishy");
        entry.high_water_at_last_dispatch = cursor;
        match (entry.last_change_kind, sync_kind) {
            // item added to new partition
            (ItemSyncKind::Change, ItemSyncKind::New)
            | (ItemSyncKind::New, ItemSyncKind::New)
            // item changed
            | (ItemSyncKind::New, ItemSyncKind::Change)
            | (ItemSyncKind::Change, ItemSyncKind::Change)
            // item deleted
            |(ItemSyncKind::Change, ItemSyncKind::Delete)
            | (ItemSyncKind::Delete, ItemSyncKind::Delete)
            | (ItemSyncKind::New, ItemSyncKind::Delete) => {}
            // FIXME: I'm not sure how well we've modeled deletes
            // in one partitions
            (ItemSyncKind::Delete, ItemSyncKind::New)
            | (ItemSyncKind::Delete, ItemSyncKind::Change)
            => panic!("curiosity trap: event for deleted item"),
        }
        entry.high_water_at_last_dispatch = cursor;
        entry.waiters.insert(
            cursor,
            CursorWaiter {
                peer,
                partition_id,
                sync_kind,
                stream_event: sub_kind,
            },
        );
        Ok(
            // FIXME: consider waiting until next dispatch for new commands
            vec![match key.kind {
                ItemSyncKind::New => SyncMachineCommand::ItemNewSync { key },
                ItemSyncKind::Change => SyncMachineCommand::ItemChangeSync { key },
                ItemSyncKind::Delete => SyncMachineCommand::ItemDeleteSync { key },
            }],
        )
    }

    pub async fn on_item_sync_completed(
        &mut self,
        completion: SyncCompletion,
    ) -> Res<Vec<SyncMachineCommand>> {
        let job_id = match &completion {
            SyncCompletion::AddedMember { peer, item_id, .. }
            | SyncCompletion::ChangedItem { peer, item_id }
            | SyncCompletion::DeletedMember { peer, item_id }
            | SyncCompletion::Noop { peer, item_id } => ItemJobId {
                peer: Arc::clone(&peer),
                item_id: Arc::clone(&item_id),
            },
        };
        let Some(job) = self.active_item_jobs.get_mut(&job_id) else {
            return Ok(Vec::new());
        };
        let mut commands = Vec::new();
        if job.dirty {
            job.dirty = false;

            let next_job_kind = match (&job.last_change_kind, &completion) {
                (ItemSyncKind::New, SyncCompletion::AddedMember { .. })
                | (ItemSyncKind::Change, SyncCompletion::AddedMember { .. })
                | (ItemSyncKind::New, SyncCompletion::Noop { .. })
                | (ItemSyncKind::New, SyncCompletion::ChangedItem { .. })
                | (ItemSyncKind::Change, SyncCompletion::Noop { .. })
                | (ItemSyncKind::Change, SyncCompletion::ChangedItem { .. }) => {
                    ItemSyncKind::Change
                }
                (ItemSyncKind::Change, SyncCompletion::DeletedMember { .. })
                | (ItemSyncKind::New, SyncCompletion::DeletedMember { .. }) => ItemSyncKind::New,
                (ItemSyncKind::Delete, SyncCompletion::AddedMember { .. })
                // NOTE: we still enueue a delete on previous delete in case
                // there was a transient Added flip
                | (ItemSyncKind::Delete, SyncCompletion::DeletedMember { .. })
                | (ItemSyncKind::Delete, SyncCompletion::ChangedItem { .. })
                | (ItemSyncKind::Delete, SyncCompletion::Noop { .. }) => ItemSyncKind::Delete,
            };
            // We had new waiters since we last
            // dispatched for this job, we must
            // dispatch a new command
            commands.push(match next_job_kind {
                ItemSyncKind::New => SyncMachineCommand::ItemNewSync {
                    key: ItemSyncKey {
                        peer: Arc::clone(&job_id.peer),
                        item_id: Arc::clone(&job_id.peer),
                        kind: next_job_kind,
                    },
                },
                ItemSyncKind::Change => SyncMachineCommand::ItemChangeSync {
                    key: ItemSyncKey {
                        peer: Arc::clone(&job_id.peer),
                        item_id: Arc::clone(&job_id.peer),
                        kind: next_job_kind,
                    },
                },
                ItemSyncKind::Delete => SyncMachineCommand::ItemChangeSync {
                    key: ItemSyncKey {
                        peer: Arc::clone(&job_id.peer),
                        item_id: Arc::clone(&job_id.peer),
                        kind: next_job_kind,
                    },
                },
            });
        }
        // remove it temporarily to allow mutable borrows
        // on self
        let Some(mut job) = self.active_item_jobs.remove(&job_id) else {
            return Ok(Vec::new());
        };

        let mut per_partition_cursors = HashMap::new();
        for (&ii, waiter) in job.waiters.iter() {
            if ii > job.high_water_at_last_dispatch {
                continue;
            }
            if !per_partition_cursors.contains_key(&waiter.partition_id) {
                per_partition_cursors.insert(Arc::clone(&waiter.partition_id), vec![]);
            }
            let part_cursors = per_partition_cursors
                .get_mut(&waiter.partition_id)
                .expect(ERROR_IMPOSSIBLE);
            part_cursors.push(ii);
        }

        for (part_id, cursors) in per_partition_cursors {
            debug_assert!(cursors.is_sorted());
            for &ii in &cursors {
                let waiter = job.waiters.remove(&ii).expect(ERROR_IMPOSSIBLE);
                let state = self.stream_state_mut(&waiter.peer, &part_id, waiter.stream_event);
                // mark slot ready
                state.slots.insert(ii, CursorSlotState::Ready);
            }

            match &completion {
                SyncCompletion::AddedMember { item_payload, .. } => {
                    self.partition_store
                        .upsert_item(&part_id, Arc::clone(&job_id.item_id), item_payload)
                        .await?;
                }
                SyncCompletion::DeletedMember { .. } => {
                    self.partition_store
                        .remove_item(&part_id, Arc::clone(&job_id.item_id))
                        .await?;
                }
                SyncCompletion::ChangedItem { .. } | SyncCompletion::Noop { .. } => {}
            }
            self.drain_ready_cursor_advances(&job_id.peer, &part_id)
                .await?;
        }

        // emit sync requests on any other peers that
        // have job on item
        for (ii_id, ii_job) in &mut self.active_item_jobs {
            if ii_id.item_id != job_id.item_id {
                continue;
            }
            if ii_job.last_change_kind != ItemSyncKind::New {
                continue;
            }
            if job_id.peer != ii_id.peer {
                continue;
            }
            ii_job.last_change_kind = ItemSyncKind::Change;
            // FIXME: consider not setting it dirty
            ii_job.dirty = true;
            commands.push(SyncMachineCommand::ItemChangeSync {
                key: ItemSyncKey {
                    peer: Arc::clone(&ii_id.peer),
                    item_id: Arc::clone(&ii_id.item_id),
                    kind: ItemSyncKind::Change,
                },
            });
        }

        if !job.waiters.is_empty() {
            self.active_item_jobs.insert(job_id, job);
        }

        Ok(commands)
    }

    pub fn active_peers_for_item(&self, item_id: &str) -> Vec<PeerKey> {
        self.active_item_jobs
            .keys()
            .filter(|job_id| &job_id.item_id[..] == item_id)
            .map(|job_id| job_id.peer.clone())
            .collect()
    }

    pub fn has_active_item_job_for(&self, peer: PeerKey, item_id: Arc<str>) -> bool {
        self.active_item_jobs.contains_key(&ItemJobId {
            peer: Arc::clone(&peer),
            item_id,
        })
    }

    async fn drain_ready_cursor_advances(
        &mut self,
        peer: &PeerKey,
        partition_id: &PartitionId,
    ) -> Res<()> {
        let state = {
            if !self.cursor_state.contains_key(peer) {
                self.cursor_state.insert(Arc::clone(peer), default());
            }
            let state = self.cursor_state.get_mut(peer).expect(ERROR_IMPOSSIBLE);
            if !state.contains_key(partition_id) {
                state.insert(Arc::clone(partition_id), default());
            }
            state.get_mut(partition_id).expect(ERROR_IMPOSSIBLE)
        };
        // calculate Ready highmark
        let (member_cursor, item_cursor) = {
            let member_cursor = {
                let mut latest_ready = None;
                let floor = state.member.floor();
                for (cursor, slot) in state.member.slots.range(floor.saturating_add(1)..) {
                    match slot {
                        CursorSlotState::Ready => latest_ready = Some(*cursor),
                        CursorSlotState::Pending => break,
                    }
                }
                latest_ready
            };
            let item_cursor = {
                let mut latest_ready = None;
                let floor = state.item.floor();
                for (cursor, slot) in state.item.slots.range(floor.saturating_add(1)..) {
                    match slot {
                        CursorSlotState::Ready => latest_ready = Some(*cursor),
                        CursorSlotState::Pending => break,
                    }
                }
                latest_ready
            };
            (member_cursor, item_cursor)
        };

        if let (None, None) = (member_cursor, item_cursor) {
            return Ok(());
        }

        // update sync store
        let existing = self
            .sync_store
            .get_partition_cursor(PeerKey::clone(peer), Arc::clone(&partition_id))
            .await?;
        let next_member_cursor = member_cursor.or(existing.member_cursor);
        let next_item_cursor = item_cursor.or(existing.item_cursor);
        self.sync_store
            .set_partition_cursor(
                PeerKey::clone(peer),
                partition_id.clone(),
                next_member_cursor,
                next_item_cursor,
            )
            .await?;

        // remove cursor slots and update state
        if let Some(cursor) = member_cursor {
            while state
                .member
                .slots
                .first_key_value()
                .is_some_and(|(slot_cursor, _)| *slot_cursor <= cursor)
            {
                state.member.slots.pop_first();
            }
            state.member.last_emitted_cursor = Some(cursor);
            state.member.persisted_cursor = Some(cursor);
        }
        if let Some(cursor) = item_cursor {
            while state
                .item
                .slots
                .first_key_value()
                .is_some_and(|(slot_cursor, _)| *slot_cursor <= cursor)
            {
                state.item.slots.pop_first();
            }
            state.item.last_emitted_cursor = Some(cursor);
            state.item.persisted_cursor = Some(cursor);
        }
        Ok(())
    }

    fn stream_state_mut(
        &mut self,
        peer: &PeerKey,
        partition_id: &PartitionId,
        stream: SubscriptionStreamKind,
    ) -> &mut CursorStreamState {
        if !self.cursor_state.contains_key(peer) {
            self.cursor_state.insert(Arc::clone(peer), default());
        }
        let state = self.cursor_state.get_mut(peer).expect(ERROR_IMPOSSIBLE);
        if !state.contains_key(partition_id) {
            state.insert(Arc::clone(partition_id), default());
        }
        let partition = state.get_mut(partition_id).expect(ERROR_IMPOSSIBLE);
        match stream {
            SubscriptionStreamKind::Member => &mut partition.member,
            SubscriptionStreamKind::Item => &mut partition.item,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::protocol::{PartitionItemEvent, PartitionMemberEvent};
    use crate::sync::store::{spawn_sync_store, SyncStoreStopToken};
    use sqlx::sqlite::SqlitePoolOptions;

    fn peer(raw: &str) -> PeerKey {
        raw.into()
    }

    fn changed(partition_id: &str, item_id: &str, cursor: CursorIndex) -> SubscriptionEvent {
        SubscriptionEvent::ItemEvent(PartitionItemEvent {
            cursor,
            partition_id: partition_id.into(),
            deets: PartitionItemEventDeets::ItemChanged {
                item_id: item_id.into(),
                payload: "{}".to_string(),
            },
        })
    }

    fn upsert(partition_id: &str, item_id: &str, cursor: CursorIndex) -> SubscriptionEvent {
        SubscriptionEvent::MemberEvent(PartitionMemberEvent {
            cursor,
            partition_id: partition_id.into(),
            deets: PartitionMemberEventDeets::MemberUpsert {
                item_id: item_id.into(),
            },
        })
    }

    async fn make_machine() -> Res<(SyncMachine, SyncStoreStopToken, Arc<PartitionStore>)> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await?;
        let (partition_store, _ps_stop) = PartitionStore::boot(pool.clone()).await?;
        let (sync_store, stop) = spawn_sync_store(pool).await?;
        Ok((
            SyncMachine::new(Arc::clone(&partition_store), sync_store),
            stop,
            partition_store,
        ))
    }

    async fn ensure_partition(machine: &SyncMachine, partition_id: &str) -> Res<()> {
        machine
            .partition_store
            .ensure_partition(&partition_id.into())
            .await
    }

    #[tokio::test]
    async fn sparse_ready_cursors_advance_over_observed_prefix() -> Res<()> {
        let (mut machine, stop, _ps) = make_machine().await?;
        ensure_partition(&machine, "part").await?;
        let p = peer("peer-a");
        let cmds = machine
            .on_subscription_item(PeerKey::clone(&p), changed("part", "a", 10))
            .await?;
        let [SyncMachineCommand::ItemChangeSync { key: key_a }] = cmds.as_slice() else {
            panic!("unexpected commands: {cmds:?}");
        };
        let cmds = machine
            .on_subscription_item(PeerKey::clone(&p), changed("part", "b", 15))
            .await?;
        let [SyncMachineCommand::ItemChangeSync { key: key_b }] = cmds.as_slice() else {
            panic!("unexpected commands: {cmds:?}");
        };

        assert_eq!(key_a.peer, p);
        machine
            .on_item_sync_completed(SyncCompletion::ChangedItem {
                peer: key_a.peer.clone(),
                item_id: key_a.item_id.clone(),
            })
            .await?;
        machine
            .on_item_sync_completed(SyncCompletion::ChangedItem {
                peer: key_b.peer.clone(),
                item_id: key_b.item_id.clone(),
            })
            .await?;
        let cursor = machine
            .sync_store
            .get_partition_cursor(p, "part".into())
            .await?;
        assert_eq!(cursor.item_cursor, Some(15));
        stop.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn pending_lower_cursor_blocks_later_ready_cursor() -> Res<()> {
        let (mut machine, stop, _ps) = make_machine().await?;
        ensure_partition(&machine, "part").await?;
        let p = peer("peer-a");
        let key_10 = match machine
            .on_subscription_item(PeerKey::clone(&p), changed("part", "a", 10))
            .await?
            .pop()
            .unwrap()
        {
            SyncMachineCommand::ItemChangeSync { key } => key,
            other => panic!("unexpected command: {other:?}"),
        };
        let key_15 = match machine
            .on_subscription_item(PeerKey::clone(&p), changed("part", "b", 15))
            .await?
            .pop()
            .unwrap()
        {
            SyncMachineCommand::ItemChangeSync { key } => key,
            other => panic!("unexpected command: {other:?}"),
        };

        machine
            .on_item_sync_completed(SyncCompletion::ChangedItem {
                peer: key_15.peer.clone(),
                item_id: key_15.item_id.clone(),
            })
            .await?;
        let cursor = machine
            .sync_store
            .get_partition_cursor(PeerKey::clone(&p), "part".into())
            .await?;
        assert_eq!(cursor.item_cursor, None);
        machine
            .on_item_sync_completed(SyncCompletion::ChangedItem {
                peer: key_10.peer.clone(),
                item_id: key_10.item_id.clone(),
            })
            .await?;
        let cursor = machine
            .sync_store
            .get_partition_cursor(p, "part".into())
            .await?;
        assert_eq!(cursor.item_cursor, Some(15));
        stop.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn emits_per_peer_jobs_for_same_item() -> Res<()> {
        let (mut machine, stop, _ps) = make_machine().await?;
        ensure_partition(&machine, "part").await?;
        let p1 = peer("peer-a");
        let p2 = peer("peer-b");
        let cmds_1 = machine
            .on_subscription_item(PeerKey::clone(&p1), changed("part", "a", 10))
            .await?;
        let cmds_2 = machine
            .on_subscription_item(PeerKey::clone(&p2), changed("part", "a", 20))
            .await?;
        assert_eq!(cmds_1.len(), 1);
        assert_eq!(cmds_2.len(), 1);
        stop.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn member_events_wait_for_item_materialization() -> Res<()> {
        let (mut machine, stop, _ps) = make_machine().await?;
        ensure_partition(&machine, "part").await?;
        let p = peer("peer-a");
        let key = match machine
            .on_subscription_item(PeerKey::clone(&p), upsert("part", "a", 7))
            .await?
            .pop()
            .unwrap()
        {
            SyncMachineCommand::ItemNewSync { key } => key,
            other => panic!("unexpected command: {other:?}"),
        };
        machine
            .on_item_sync_completed(SyncCompletion::AddedMember {
                peer: key.peer.clone(),
                item_id: key.item_id.clone(),
                item_payload: json!({}),
            })
            .await?;
        let cursor = machine
            .sync_store
            .get_partition_cursor(p, "part".into())
            .await?;
        assert_eq!(cursor.member_cursor, Some(7));
        stop.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn new_completion_upgrades_other_peers_to_change() -> Res<()> {
        let (mut machine, stop, _ps) = make_machine().await?;
        ensure_partition(&machine, "part").await?;
        let p1 = peer("peer-a");
        let p2 = peer("peer-b");
        let key_1 = match machine
            .on_subscription_item(PeerKey::clone(&p1), upsert("part", "a", 10))
            .await?
            .pop()
            .unwrap()
        {
            SyncMachineCommand::ItemNewSync { key } => key,
            other => panic!("unexpected command: {other:?}"),
        };
        let _ = machine
            .on_subscription_item(PeerKey::clone(&p2), upsert("part", "a", 20))
            .await?;
        let cmds = machine
            .on_item_sync_completed(SyncCompletion::AddedMember {
                peer: key_1.peer.clone(),
                item_id: key_1.item_id.clone(),
                item_payload: json!({}),
            })
            .await?;
        assert_eq!(cmds.len(), 1);
        let [SyncMachineCommand::ItemChangeSync { key }] = cmds.as_slice() else {
            panic!("unexpected commands: {cmds:?}");
        };
        assert_eq!(key.peer, p2);
        stop.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn noop_new_does_not_advance_cursor_and_waits_for_change() -> Res<()> {
        let (mut machine, stop, ps) = make_machine().await?;
        ensure_partition(&machine, "part").await?;
        let p1 = peer("peer-a");
        let p2 = peer("peer-b");
        let _ = machine
            .on_subscription_item(PeerKey::clone(&p1), upsert("part", "a", 10))
            .await?;
        let key_2 = match machine
            .on_subscription_item(PeerKey::clone(&p2), upsert("part", "a", 20))
            .await?
            .pop()
            .unwrap()
        {
            SyncMachineCommand::ItemNewSync { key } => key,
            other => panic!("unexpected command: {other:?}"),
        };

        let cmds = machine
            .on_item_sync_completed(SyncCompletion::Noop {
                peer: key_2.peer.clone(),
                item_id: key_2.item_id.clone(),
            })
            .await?;
        assert_eq!(cmds.len(), 2);
        assert!(cmds
            .iter()
            .all(|cmd| matches!(cmd, SyncMachineCommand::ItemChangeSync { .. })));
        assert!(cmds.iter().any(
            |cmd| matches!(cmd, SyncMachineCommand::ItemChangeSync { key } if key.peer == p1)
        ));
        assert!(cmds.iter().any(
            |cmd| matches!(cmd, SyncMachineCommand::ItemChangeSync { key } if key.peer == p2)
        ));

        let cursor = machine
            .sync_store
            .get_partition_cursor(p2, "part".into())
            .await?;
        assert_eq!(cursor.item_cursor, None);
        let count_after = ps.member_count(&"part".into()).await?;
        assert_eq!(count_after, 0);
        stop.stop().await?;
        Ok(())
    }
}
