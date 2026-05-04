use crate::interlude::*;

use crate::partition::PartitionStore;
use crate::sync::protocol::{
    CursorIndex, PartitionId, PartitionItemEventDeets, PartitionMemberEventDeets, PeerKey,
    SubscriptionItem, SubscriptionStreamKind,
};
use crate::sync::store::SyncStoreHandle;

use std::collections::{BTreeMap, BTreeSet, HashMap};

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
    pub partition_id: PartitionId,
    pub item_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct ItemJobId {
    peer: PeerKey,
    partition_id: PartitionId,
    item_id: String,
}

#[derive(Debug, Clone)]
struct ItemJobState {
    kind: ItemSyncKind,
    waiters: BTreeSet<CursorWaiter>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CursorWaiter {
    pub peer: PeerKey,
    pub partition_id: PartitionId,
    pub stream: SubscriptionStreamKind,
    pub cursor: CursorIndex,
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
        partition_id: PartitionId,
        item_id: String,
        item_payload: String,
    },
    ChangedItem {
        peer: PeerKey,
        partition_id: PartitionId,
        item_id: String,
        item_payload: String,
    },
    DeletedMember {
        peer: PeerKey,
        partition_id: PartitionId,
        item_id: String,
    },
    Noop {
        peer: PeerKey,
        partition_id: PartitionId,
        item_id: String,
    },
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

#[derive(Debug, Default)]
struct CursorStreamState {
    persisted_cursor: Option<CursorIndex>,
    last_emitted_cursor: Option<CursorIndex>,
    slots: BTreeMap<CursorIndex, CursorSlotState>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CursorSlotState {
    Pending,
    Ready,
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

    pub fn sync_store(&self) -> &SyncStoreHandle {
        &self.sync_store
    }

    pub fn on_subscription_item(
        &mut self,
        peer: PeerKey,
        item: SubscriptionItem,
    ) -> Res<Vec<SyncMachineCommand>> {
        match item {
            SubscriptionItem::MemberEvent(event) => {
                let partition_id = event.partition_id.clone();
                if self.note_pending(
                    peer.clone(),
                    partition_id.clone(),
                    SubscriptionStreamKind::Member,
                    event.cursor,
                ) {
                    return Ok(Vec::new());
                }
                let (kind, item_id) = match event.deets {
                    PartitionMemberEventDeets::MemberUpsert { item_id } => {
                        (ItemSyncKind::New, item_id)
                    }
                    PartitionMemberEventDeets::MemberRemoved { item_id } => {
                        (ItemSyncKind::Delete, item_id)
                    }
                };
                let key = ItemSyncKey {
                    peer: peer.clone(),
                    kind,
                    partition_id: partition_id.clone(),
                    item_id: item_id.clone(),
                };
                let is_new = self.ensure_item_job(
                    key.clone(),
                    CursorWaiter {
                        peer,
                        partition_id,
                        stream: SubscriptionStreamKind::Member,
                        cursor: event.cursor,
                    },
                );
                Ok(if is_new {
                    vec![Self::command_for(key)]
                } else {
                    Vec::new()
                })
            }
            SubscriptionItem::ItemEvent(event) => {
                let partition_id = event.partition_id.clone();
                if self.note_pending(
                    peer.clone(),
                    partition_id.clone(),
                    SubscriptionStreamKind::Item,
                    event.cursor,
                ) {
                    return Ok(Vec::new());
                }
                let (kind, item_id) = match event.deets {
                    PartitionItemEventDeets::ItemChanged { item_id, .. } => {
                        (ItemSyncKind::Change, item_id)
                    }
                    PartitionItemEventDeets::ItemDeleted { item_id, .. } => {
                        (ItemSyncKind::Delete, item_id)
                    }
                };
                let key = ItemSyncKey {
                    peer: peer.clone(),
                    kind,
                    partition_id: partition_id.clone(),
                    item_id: item_id.clone(),
                };
                let is_new = self.ensure_item_job(
                    key.clone(),
                    CursorWaiter {
                        peer,
                        partition_id,
                        stream: SubscriptionStreamKind::Item,
                        cursor: event.cursor,
                    },
                );
                Ok(if is_new {
                    vec![Self::command_for(key)]
                } else {
                    Vec::new()
                })
            }
            SubscriptionItem::ReplayComplete { .. } => Ok(Vec::new()),
            SubscriptionItem::Lagged { dropped } => {
                eyre::bail!("partition subscription lagged; dropped={dropped}")
            }
        }
    }

    pub async fn on_item_sync_completed(
        &mut self,
        completion: SyncCompletion,
    ) -> Res<Vec<SyncMachineCommand>> {
        let (peer, partition_id, item_id) = match &completion {
            SyncCompletion::AddedMember {
                peer,
                partition_id,
                item_id,
                ..
            }
            | SyncCompletion::ChangedItem {
                peer,
                partition_id,
                item_id,
                ..
            }
            | SyncCompletion::DeletedMember {
                peer,
                partition_id,
                item_id,
                ..
            }
            | SyncCompletion::Noop {
                peer,
                partition_id,
                item_id,
            } => (peer.clone(), partition_id.clone(), item_id.clone()),
        };
        let job_id = ItemJobId {
            peer: peer.clone(),
            partition_id: partition_id.clone(),
            item_id: item_id.clone(),
        };
        let Some(job) = self.active_item_jobs.get(&job_id).cloned() else {
            return Ok(Vec::new());
        };

        let mut commands = Vec::new();
        match &completion {
            SyncCompletion::AddedMember { item_payload, .. } => {
                let item_payload: serde_json::Value = serde_json::from_str(item_payload)?;
                self.partition_store
                    .upsert_item(&partition_id, &item_id, &item_payload)
                    .await?;
            }
            SyncCompletion::ChangedItem { item_payload, .. } => {
                let payload: serde_json::Value = serde_json::from_str(item_payload)?;
                self.partition_store
                    .upsert_item(&partition_id, &item_id, &payload)
                    .await?;
            }
            SyncCompletion::DeletedMember { .. } => {
                self.partition_store
                    .remove_item(&partition_id, &item_id)
                    .await?;
            }
            SyncCompletion::Noop { .. } => {}
        }

        if job.kind == ItemSyncKind::New
            && matches!(
                completion,
                SyncCompletion::AddedMember { .. } | SyncCompletion::Noop { .. }
            )
        {
            let include_completed_peer = matches!(completion, SyncCompletion::Noop { .. });
            commands.extend(self.upgrade_new_jobs_to_change(
                &partition_id,
                &item_id,
                &peer,
                include_completed_peer,
            ));
        }

        let should_complete_job = match (&job.kind, &completion) {
            (ItemSyncKind::New, SyncCompletion::AddedMember { .. }) => true,
            (ItemSyncKind::New, SyncCompletion::Noop { .. }) => false,
            (ItemSyncKind::Change, SyncCompletion::ChangedItem { .. }) => true,
            (ItemSyncKind::Delete, SyncCompletion::DeletedMember { .. }) => true,
            (_, SyncCompletion::Noop { .. }) => false,
            _ => true,
        };

        if should_complete_job {
            self.complete_job(&job_id).await?;
        }

        Ok(commands)
    }

    pub fn find_active_item_key_for(
        &self,
        peer: &PeerKey,
        partition_id: &PartitionId,
        item_id: &str,
    ) -> Option<ItemSyncKey> {
        let job_id = ItemJobId {
            peer: peer.clone(),
            partition_id: partition_id.clone(),
            item_id: item_id.to_string(),
        };
        let job = self.active_item_jobs.get(&job_id)?;
        Some(ItemSyncKey {
            peer: peer.clone(),
            kind: job.kind,
            partition_id: partition_id.clone(),
            item_id: item_id.to_string(),
        })
    }

    pub fn active_peers_for_item(&self, partition_id: &PartitionId, item_id: &str) -> Vec<PeerKey> {
        self.active_item_jobs
            .keys()
            .filter(|job_id| job_id.partition_id == *partition_id && job_id.item_id == item_id)
            .map(|job_id| job_id.peer.clone())
            .collect()
    }

    pub fn has_active_item_job_for(
        &self,
        peer: &PeerKey,
        partition_id: &PartitionId,
        item_id: &str,
    ) -> bool {
        self.active_item_jobs.contains_key(&ItemJobId {
            peer: peer.clone(),
            partition_id: partition_id.clone(),
            item_id: item_id.to_string(),
        })
    }

    pub fn has_active_item_jobs_for_partition(
        &self,
        peer: &PeerKey,
        partition_id: &PartitionId,
    ) -> bool {
        self.active_item_jobs
            .keys()
            .any(|job_id| job_id.peer == *peer && job_id.partition_id == *partition_id)
    }

    fn note_pending(
        &mut self,
        peer: PeerKey,
        partition_id: PartitionId,
        stream: SubscriptionStreamKind,
        cursor: CursorIndex,
    ) -> bool {
        let state = self.stream_state_mut(&peer, &partition_id, stream);
        if cursor <= state.floor() {
            return true;
        }
        state
            .slots
            .entry(cursor)
            .or_insert(CursorSlotState::Pending);
        false
    }

    fn mark_ready(
        &mut self,
        peer: &PeerKey,
        partition_id: &PartitionId,
        stream: SubscriptionStreamKind,
        cursor: CursorIndex,
    ) {
        let state = self.stream_state_mut(peer, partition_id, stream);
        if cursor <= state.floor() {
            return;
        }
        state.slots.insert(cursor, CursorSlotState::Ready);
    }

    fn ensure_item_job(&mut self, key: ItemSyncKey, waiter: CursorWaiter) -> bool {
        let job_id = ItemJobId {
            peer: key.peer,
            partition_id: key.partition_id,
            item_id: key.item_id,
        };
        let entry = self
            .active_item_jobs
            .entry(job_id)
            .or_insert_with(|| ItemJobState {
                kind: key.kind,
                waiters: BTreeSet::new(),
            });
        let already_active = !entry.waiters.is_empty();
        entry.waiters.insert(waiter);
        !already_active
    }

    fn upgrade_new_jobs_to_change(
        &mut self,
        partition_id: &PartitionId,
        item_id: &str,
        completed_peer: &PeerKey,
        include_completed_peer: bool,
    ) -> Vec<SyncMachineCommand> {
        let mut commands = Vec::new();
        for (job_id, job) in &mut self.active_item_jobs {
            if job_id.partition_id != *partition_id || job_id.item_id != item_id {
                continue;
            }
            if job.kind != ItemSyncKind::New {
                continue;
            }
            if !include_completed_peer && &job_id.peer == completed_peer {
                continue;
            }
            job.kind = ItemSyncKind::Change;
            commands.push(SyncMachineCommand::ItemChangeSync {
                key: ItemSyncKey {
                    peer: job_id.peer.clone(),
                    kind: ItemSyncKind::Change,
                    partition_id: partition_id.clone(),
                    item_id: item_id.to_string(),
                },
            });
        }
        commands
    }

    async fn complete_job(&mut self, job_id: &ItemJobId) -> Res<()> {
        let Some(job) = self.active_item_jobs.remove(job_id) else {
            return Ok(());
        };
        for waiter in job.waiters {
            self.mark_ready(
                &waiter.peer,
                &waiter.partition_id,
                waiter.stream,
                waiter.cursor,
            );
            self.drain_ready_cursor_advances(&waiter.peer, &waiter.partition_id, waiter.stream)
                .await?;
        }
        Ok(())
    }

    async fn drain_ready_cursor_advances(
        &mut self,
        peer: &PeerKey,
        partition_id: &PartitionId,
        stream: SubscriptionStreamKind,
    ) -> Res<()> {
        let (cursor, member_cursor, item_cursor) = {
            let state = self.stream_state_mut(peer, partition_id, stream);
            let floor = state.floor();
            let mut latest_ready = None;
            for (cursor, slot) in state.slots.range(floor.saturating_add(1)..) {
                match slot {
                    CursorSlotState::Ready => latest_ready = Some(*cursor),
                    CursorSlotState::Pending => break,
                }
            }
            let Some(cursor) = latest_ready else {
                return Ok(());
            };
            let (member_cursor, item_cursor) = match stream {
                SubscriptionStreamKind::Member => (Some(cursor), None),
                SubscriptionStreamKind::Item => (None, Some(cursor)),
            };
            (cursor, member_cursor, item_cursor)
        };

        let existing = self
            .sync_store
            .get_partition_cursor(PeerKey::clone(peer), partition_id.clone())
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

        let state = self.stream_state_mut(peer, partition_id, stream);
        while state
            .slots
            .first_key_value()
            .is_some_and(|(slot_cursor, _)| *slot_cursor <= cursor)
        {
            state.slots.pop_first();
        }
        state.last_emitted_cursor = Some(cursor);
        state.persisted_cursor = Some(cursor);
        Ok(())
    }

    fn stream_state_mut(
        &mut self,
        peer: &PeerKey,
        partition_id: &PartitionId,
        stream: SubscriptionStreamKind,
    ) -> &mut CursorStreamState {
        let partition = self
            .cursor_state
            .entry(PeerKey::clone(peer))
            .or_default()
            .entry(partition_id.clone())
            .or_default();
        match stream {
            SubscriptionStreamKind::Member => &mut partition.member,
            SubscriptionStreamKind::Item => &mut partition.item,
        }
    }

    fn command_for(key: ItemSyncKey) -> SyncMachineCommand {
        match key.kind {
            ItemSyncKind::New => SyncMachineCommand::ItemNewSync { key },
            ItemSyncKind::Change => SyncMachineCommand::ItemChangeSync { key },
            ItemSyncKind::Delete => SyncMachineCommand::ItemDeleteSync { key },
        }
    }
}

impl CursorStreamState {
    fn floor(&self) -> CursorIndex {
        self.persisted_cursor
            .unwrap_or(0)
            .max(self.last_emitted_cursor.unwrap_or(0))
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

    fn changed(partition_id: &str, item_id: &str, cursor: CursorIndex) -> SubscriptionItem {
        SubscriptionItem::ItemEvent(PartitionItemEvent {
            cursor,
            partition_id: partition_id.to_string(),
            deets: PartitionItemEventDeets::ItemChanged {
                item_id: item_id.to_string(),
                payload: "{}".to_string(),
            },
        })
    }

    fn upsert(partition_id: &str, item_id: &str, cursor: CursorIndex) -> SubscriptionItem {
        SubscriptionItem::MemberEvent(PartitionMemberEvent {
            cursor,
            partition_id: partition_id.to_string(),
            deets: PartitionMemberEventDeets::MemberUpsert {
                item_id: item_id.to_string(),
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
            .ensure_partition(&partition_id.to_string())
            .await
    }

    #[tokio::test]
    async fn sparse_ready_cursors_advance_over_observed_prefix() -> Res<()> {
        let (mut machine, stop, _ps) = make_machine().await?;
        ensure_partition(&machine, "part").await?;
        let p = peer("peer-a");
        let cmds = machine.on_subscription_item(PeerKey::clone(&p), changed("part", "a", 10))?;
        let [SyncMachineCommand::ItemChangeSync { key: key_a }] = cmds.as_slice() else {
            panic!("unexpected commands: {cmds:?}");
        };
        let cmds = machine.on_subscription_item(PeerKey::clone(&p), changed("part", "b", 15))?;
        let [SyncMachineCommand::ItemChangeSync { key: key_b }] = cmds.as_slice() else {
            panic!("unexpected commands: {cmds:?}");
        };

        assert_eq!(key_a.peer, p);
        machine
            .on_item_sync_completed(SyncCompletion::ChangedItem {
                peer: key_a.peer.clone(),
                partition_id: "part".to_string(),
                item_id: key_a.item_id.clone(),
                item_payload: "{}".to_string(),
            })
            .await?;
        machine
            .on_item_sync_completed(SyncCompletion::ChangedItem {
                peer: key_b.peer.clone(),
                partition_id: "part".to_string(),
                item_id: key_b.item_id.clone(),
                item_payload: "{}".to_string(),
            })
            .await?;
        let cursor = machine
            .sync_store
            .get_partition_cursor(p, "part".to_string())
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
            .on_subscription_item(PeerKey::clone(&p), changed("part", "a", 10))?
            .pop()
            .unwrap()
        {
            SyncMachineCommand::ItemChangeSync { key } => key,
            other => panic!("unexpected command: {other:?}"),
        };
        let key_15 = match machine
            .on_subscription_item(PeerKey::clone(&p), changed("part", "b", 15))?
            .pop()
            .unwrap()
        {
            SyncMachineCommand::ItemChangeSync { key } => key,
            other => panic!("unexpected command: {other:?}"),
        };

        machine
            .on_item_sync_completed(SyncCompletion::ChangedItem {
                peer: key_15.peer.clone(),
                partition_id: "part".to_string(),
                item_id: key_15.item_id.clone(),
                item_payload: "{}".to_string(),
            })
            .await?;
        let cursor = machine
            .sync_store
            .get_partition_cursor(PeerKey::clone(&p), "part".to_string())
            .await?;
        assert_eq!(cursor.item_cursor, None);
        machine
            .on_item_sync_completed(SyncCompletion::ChangedItem {
                peer: key_10.peer.clone(),
                partition_id: "part".to_string(),
                item_id: key_10.item_id.clone(),
                item_payload: "{}".to_string(),
            })
            .await?;
        let cursor = machine
            .sync_store
            .get_partition_cursor(p, "part".to_string())
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
        let cmds_1 = machine.on_subscription_item(PeerKey::clone(&p1), changed("part", "a", 10))?;
        let cmds_2 = machine.on_subscription_item(PeerKey::clone(&p2), changed("part", "a", 20))?;
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
            .on_subscription_item(PeerKey::clone(&p), upsert("part", "a", 7))?
            .pop()
            .unwrap()
        {
            SyncMachineCommand::ItemNewSync { key } => key,
            other => panic!("unexpected command: {other:?}"),
        };
        machine
            .on_item_sync_completed(SyncCompletion::AddedMember {
                peer: key.peer.clone(),
                partition_id: "part".to_string(),
                item_id: key.item_id.clone(),
                item_payload: "{}".to_string(),
            })
            .await?;
        let cursor = machine
            .sync_store
            .get_partition_cursor(p, "part".to_string())
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
            .on_subscription_item(PeerKey::clone(&p1), upsert("part", "a", 10))?
            .pop()
            .unwrap()
        {
            SyncMachineCommand::ItemNewSync { key } => key,
            other => panic!("unexpected command: {other:?}"),
        };
        let _ = machine.on_subscription_item(PeerKey::clone(&p2), upsert("part", "a", 20))?;
        let cmds = machine
            .on_item_sync_completed(SyncCompletion::AddedMember {
                peer: key_1.peer.clone(),
                partition_id: "part".to_string(),
                item_id: key_1.item_id.clone(),
                item_payload: "{}".to_string(),
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
        let _ = machine.on_subscription_item(PeerKey::clone(&p1), upsert("part", "a", 10))?;
        let key_2 = match machine
            .on_subscription_item(PeerKey::clone(&p2), upsert("part", "a", 20))?
            .pop()
            .unwrap()
        {
            SyncMachineCommand::ItemNewSync { key } => key,
            other => panic!("unexpected command: {other:?}"),
        };

        let cmds = machine
            .on_item_sync_completed(SyncCompletion::Noop {
                peer: key_2.peer.clone(),
                partition_id: key_2.partition_id.clone(),
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
            .get_partition_cursor(p2, "part".to_string())
            .await?;
        assert_eq!(cursor.item_cursor, None);
        let count_after = ps.member_count(&"part".to_string()).await?;
        assert_eq!(count_after, 0);
        stop.stop().await?;
        Ok(())
    }
}
