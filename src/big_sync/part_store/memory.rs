use crate::interlude::*;

use big_sync_core::part_store::{CursorIndex, ObjPayload};
use big_sync_core::rpc::{
    BucketMemberKind, BucketObjPageEntry, BucketSummary, BucketSummaryState,
    GetChangedBucketsRequest, LeafBucketPage, LeafBucketResult, LeafBucketsError,
    LeafBucketsRequest, ListPartsError, PartEvent, PartPage, PartSummary, SubEvent,
    SubPartsRequest,
};
use big_sync_core::{mpsc, BuckId, Byte32Id, Fingerprint, ObjId, PartId, PeerId};

use super::{obj_id_bounds_for_bucket, HostPartStore};
#[cfg(test)]
use crate::test_support::{
    ObservedObjSnapshot, ObservedStore, ObservedStoreSnapshot, TestStoreSetup,
};

use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::BTreeSet;

structstruck::strike! {
    pub struct MemoryPartStore {
        pub(crate) owner_peer_id: PeerId,
        inner: Arc<surelock::mutex::Mutex<
            #[derive(Default)]
            struct MemoryPartStoreScopeState {
                global_cursor:
                    #[derive(Default)]
                    struct GlobalCursor {
                        counter: std::sync::atomic::AtomicU64,
                    },
                parts: HashMap<
                    PartId,
                    struct PartState {
                        #![derive(Default)]
                        latest_cursor: CursorIndex,
                        members: BTreeMap<
                            ObjId,
                            #[derive(Clone)]
                            struct PartMemberState {
                                added_at: CursorIndex,
                                changed_at: CursorIndex,
                                removed_at: Option<CursorIndex>,
                            }
                        >,
                        bucket_stats: BTreeMap<BuckId, BucketSummaryState>,
                    }
                >,
                events: BTreeMap<CursorIndex, PartEvent>,
                bus: struct MemorySubsBus {
                    #![derive(Default)]
                    events_to_drop: Vec<CursorIndex>,
                    buf: Vec<PartEvent>,
                    subs_to_drop: Vec<Uuid>,
                    subs_by_part: HashMap<PartId, HashSet<Uuid>>,
                    part_by_sub: HashMap<Uuid, HashSet<PartId>>,
                    subs: HashMap<Uuid, big_sync_core::mpsc::Sender<SubEvent>>
                },
                objs: HashMap<
                    ObjId,
                    struct ObjDeets {
                        #![derive(Default)]
                        payload: Option<ObjPayload>,
                        parts: HashSet<PartId>,
                    }
                >,
                tombstoned_objs: HashMap<ObjId, CursorIndex>,
                peer_obj_payloads: HashMap<(PeerId, ObjId), Option<ObjPayload>>,
                peer_part_cursors: HashMap<(PeerId, PartId), CursorIndex>,
            }
        >>,
    }
}

impl MemoryPartStore {
    pub fn new(owner_peer_id: PeerId) -> Self {
        Self {
            owner_peer_id,
            inner: Arc::new(surelock::mutex::Mutex::new(default())),
        }
    }

    pub async fn ensure_part(&self, part_id: PartId) -> Res<()> {
        surelock::key::lock_scope(|key| {
            let (mut guard, _key) = key.lock(&self.inner);
            guard.parts.entry(part_id).or_default();
            Ok(())
        })
    }

    fn next_byte32_id(counter: &mut u64, domain: u8) -> Byte32Id {
        *counter = counter.checked_add(1).expect(ERROR_IMPOSSIBLE);
        let mut bytes = [0; 32];
        bytes[0] = domain;
        bytes[1..9].copy_from_slice(&counter.to_be_bytes());
        Byte32Id::new(bytes)
    }
}

impl MemoryPartStoreScopeState {
    fn bucket_items_for_path(
        &self,
        part_id: PartId,
        path: BuckId,
    ) -> Vec<(ObjId, CursorIndex, bool)> {
        let Some(part) = self.parts.get(&part_id) else {
            return Vec::new();
        };
        let (lower, upper) = obj_id_bounds_for_bucket(path);
        let mut items = Vec::new();
        match upper {
            Some(upper) => {
                for (&obj_id, member) in part.members.range(lower..upper) {
                    let cursor = member.removed_at.unwrap_or(member.changed_at);
                    items.push((obj_id, cursor, member.removed_at.is_some()));
                }
            }
            None => {
                for (&obj_id, member) in part.members.range(lower..) {
                    let cursor = member.removed_at.unwrap_or(member.changed_at);
                    items.push((obj_id, cursor, member.removed_at.is_some()));
                }
            }
        }
        items
    }

    fn bucket_summary(&self, part_id: PartId, path: BuckId) -> BucketSummary {
        self.parts
            .get(&part_id)
            .and_then(|part| part.bucket_stats.get(&path).cloned())
            .unwrap_or_default()
            .summary(path)
    }

    fn changed_bucket_summaries(
        &self,
        part_id: PartId,
        offset: BuckId,
        since: CursorIndex,
        limit_hint: u32,
    ) -> Result<Vec<BucketSummary>, ListPartsError> {
        let Some(part) = self.parts.get(&part_id) else {
            return Err(ListPartsError::UnkownParts {
                unkown_parts: vec![part_id],
            });
        };
        let mut buckets: Vec<_> = part
            .bucket_stats
            .range(offset..)
            .filter(|(buck_id, summary)| {
                buck_id.level() == offset.level() && summary.changed_at() > since
            })
            .map(|(&buck_id, summary)| summary.summary(buck_id))
            .collect();
        if buckets.is_empty() {
            return Ok(buckets);
        }
        if limit_hint == 0 {
            return Ok(Vec::new());
        }
        if buckets.len() > limit_hint as usize {
            let limit = limit_hint as usize;
            let last_parent = buckets[limit - 1].id.parent();
            let mut out = buckets.drain(..limit).collect::<Vec<_>>();
            while let Some(next) = buckets.first() {
                if next.id.parent() != last_parent {
                    break;
                }
                out.push(buckets.remove(0));
            }
            Ok(out)
        } else {
            Ok(buckets)
        }
    }
}

impl PartState {
    fn apply_bucket_transition(
        &mut self,
        obj_id: ObjId,
        cursor: CursorIndex,
        old: BucketMemberKind<'_>,
        new: BucketMemberKind<'_>,
    ) {
        for level in 0..=BuckId::MAX_LEVEL {
            let buck_id = BuckId::from_obj_id(level, &obj_id);
            let agg = self.bucket_stats.entry(buck_id).or_default();
            agg.apply_transition(buck_id, obj_id, cursor, old, new);
        }
    }
}
impl MemoryPartStoreScopeState {
    fn flush(&mut self) {
        for ii in self.bus.events_to_drop.drain(..) {
            self.events.remove(&ii);
        }
        for evt in self.bus.buf.drain(..) {
            let (parts, cursor, sub_evt) = match &evt {
                // PartEvent::Upserted(inner) => (inner.cursor, SubEvent::Upserted(inner.clone())),
                // PartEvent::Deleted(inner) => (inner.cursor, SubEvent::Deleted(inner.clone())),
                PartEvent::Changed(inner) => (
                    inner.part_ids.clone(),
                    inner.cursor,
                    SubEvent::Changed(inner.clone()),
                ),
                PartEvent::Added(inner) => (
                    vec![inner.part_id],
                    inner.cursor,
                    SubEvent::Added(inner.clone()),
                ),
                PartEvent::Removed(inner) => (
                    vec![inner.part_id],
                    inner.cursor,
                    SubEvent::Removed(inner.clone()),
                ),
            };
            for part_id in parts {
                let Some(subs) = self.bus.subs_by_part.get(&part_id) else {
                    continue;
                };
                for sub_id in subs {
                    let sub = self.bus.subs.get(sub_id).expect(ERROR_IMPOSSIBLE);
                    if sub.try_send(sub_evt.clone()).is_err() {
                        self.bus.subs_to_drop.push(*sub_id)
                    }
                }
            }
            self.events.insert(cursor, evt);
            self.bus.subs_to_drop.sort_unstable();
            self.bus.subs_to_drop.dedup();
            self.bus.subs_to_drop.reverse();
            for sub_id in self.bus.subs_to_drop.drain(..) {
                self.bus.subs.remove(&sub_id);
                let parts = self
                    .bus
                    .part_by_sub
                    .remove(&sub_id)
                    .expect(ERROR_IMPOSSIBLE);
                for part_id in parts {
                    let Some(set) = self.bus.subs_by_part.get_mut(&part_id) else {
                        continue;
                    };
                    set.remove(&sub_id);
                }
            }
        }
    }
}

impl MemorySubsBus {
    fn queue_evt(&mut self, evt: PartEvent) {
        self.buf.push(evt);
    }
    fn remove_evt(&mut self, idx: CursorIndex) {
        self.events_to_drop.push(idx);
    }
}
impl GlobalCursor {
    fn get(&mut self) -> CursorIndex {
        let ii = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        ii
    }
}

#[async_trait]
impl HostPartStore for MemoryPartStore {
    async fn summarize_parts(
        &self,
        parts: HashSet<PartId>,
    ) -> Res<Result<HashMap<PartId, PartSummary>, ListPartsError>> {
        Ok(surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            let mut out = HashMap::new();
            for part_id in parts {
                let Some(part) = guard.parts.get(&part_id) else {
                    return Err(ListPartsError::UnkownParts {
                        unkown_parts: vec![part_id],
                    });
                };
                out.insert(
                    part_id,
                    PartSummary {
                        latest_cursor: part.latest_cursor,
                        member_count: part
                            .members
                            .values()
                            .filter(|member| member.removed_at.is_none())
                            .count() as _,
                    },
                );
            }
            Ok(out)
        }))
    }
    async fn get_changed_buckets(
        &self,
        req: GetChangedBucketsRequest,
    ) -> Res<Result<Vec<BucketSummary>, ListPartsError>> {
        let result = surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            guard.changed_bucket_summaries(req.part_id, req.offset, req.since, req.limit_hint)
        });
        tracing::debug!(
            part_id = %req.part_id,
            offset = ?req.offset,
            since = req.since,
            limit_hint = req.limit_hint,
            bucket_count = result.as_ref().map(|b| b.len()).unwrap_or(0),
            "memory store get changed buckets"
        );
        Ok(result)
    }
    async fn leaf_buckets(
        &self,
        req: LeafBucketsRequest,
    ) -> Res<Result<LeafBucketResult, LeafBucketsError>> {
        let result = surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            let Some(part) = guard.parts.get(&req.part_id) else {
                return Err(LeafBucketsError::UnkownPart);
            };
            let mut bucks = HashMap::new();
            for buck_req in req.buckets {
                let buck_id = buck_req.buck_id;
                if guard.bucket_summary(req.part_id, buck_id).changed_at <= req.since {
                    bucks.insert(
                        buck_id,
                        LeafBucketPage {
                            entries: Vec::new(),
                            next_after: None,
                            done: true,
                        },
                    );
                    continue;
                }
                let items = guard.bucket_items_for_path(req.part_id, buck_id);
                let start = match buck_req.after {
                    Some(after) => items
                        .iter()
                        .position(|(obj_id, _, _)| *obj_id > after)
                        .unwrap_or(items.len()),
                    None => 0,
                };
                let take = req.limit_hint.max(1) as usize;
                let end = (start + take).min(items.len());
                let done = end == items.len();
                let next_after = if done || start >= end {
                    None
                } else {
                    Some(items[end - 1].0)
                };
                let entries = items
                    .into_iter()
                    .skip(start)
                    .take(take)
                    .map(|(obj_id, _cursor, dead)| {
                        let fp = if dead {
                            Fingerprint::new(
                                &req.seed,
                                &("big-sync-obj-fp-v1", obj_id, serde_json::Value::Null),
                            )
                        } else {
                            let payload = part
                                .members
                                .get(&obj_id)
                                .and_then(|member| member.removed_at.is_none().then_some(()))
                                .and_then(|_| guard.objs.get(&obj_id))
                                .and_then(|obj| obj.payload.clone())
                                .unwrap_or(serde_json::Value::Null);
                            Fingerprint::new(&req.seed, &("big-sync-obj-fp-v1", obj_id, payload))
                        };
                        BucketObjPageEntry { obj_id, dead, fp }
                    })
                    .collect();
                bucks.insert(
                    buck_id,
                    LeafBucketPage {
                        entries,
                        next_after,
                        done,
                    },
                );
            }
            Ok(LeafBucketResult {
                seed: req.seed,
                bucks,
            })
        });
        tracing::debug!(
            part_id = %req.part_id,
            bucket_count = result.as_ref().map(|r| r.bucks.len()).unwrap_or(0),
            "memory store leaf buckets"
        );
        Ok(result)
    }

    async fn member_count(&self, part_id: PartId) -> Res<u64> {
        surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            Ok(guard
                .parts
                .get(&part_id)
                .map(|part| {
                    part.members
                        .values()
                        .filter(|member| member.removed_at.is_none())
                        .count() as u64
                })
                .unwrap_or(0))
        })
    }
    async fn obj_payload(&self, obj_id: ObjId) -> Res<Option<ObjPayload>> {
        surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            Ok(guard.objs.get(&obj_id).and_then(|obj| obj.payload.clone()))
        })
    }

    async fn get_bucket_summary(&self, part_id: PartId, id: BuckId) -> Res<BucketSummary> {
        Ok(surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            guard.bucket_summary(part_id, id)
        }))
    }

    async fn obj_parts(&self, obj_id: ObjId) -> Res<Vec<PartId>> {
        surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            Ok(guard
                .objs
                .get(&obj_id)
                .map(|deets| deets.parts.iter().copied().collect())
                .unwrap_or_default())
        })
    }

    async fn set_obj_payload(&self, obj_id: ObjId, payload: ObjPayload) -> Res<()> {
        surelock::key::lock_scope(|key| {
            let (mut guard, _key) = key.lock(&self.inner);
            let guard = &mut *guard;
            guard.tombstoned_objs.remove(&obj_id);
            let obj_state = guard.objs.entry(obj_id).or_default();
            let old_payload = obj_state.payload.clone().unwrap_or(serde_json::Value::Null);
            let event_payload = payload.clone();
            obj_state.payload = Some(payload);
            if obj_state.parts.is_empty() {
                guard.flush();
                return Ok(());
            }
            let new_payload = obj_state.payload.as_ref().expect(ERROR_IMPOSSIBLE);

            let cursor = guard.global_cursor.get();
            for &part_id in &obj_state.parts {
                let part = guard.parts.entry(part_id).or_default();
                let (added_at, changed_at) = {
                    let part_obj_state = part.members.get(&obj_id).expect(ERROR_IMPOSSIBLE);
                    assert!(part_obj_state.removed_at.is_none());
                    (part_obj_state.added_at, part_obj_state.changed_at)
                };
                if changed_at != added_at {
                    guard.bus.remove_evt(changed_at);
                }
                part.apply_bucket_transition(
                    obj_id,
                    cursor,
                    BucketMemberKind::Live(&old_payload),
                    BucketMemberKind::Live(new_payload),
                );
                let part_obj_state = part.members.get_mut(&obj_id).expect(ERROR_IMPOSSIBLE);
                assert!(part_obj_state.removed_at.is_none());
                part_obj_state.changed_at = cursor;
                part.latest_cursor = cursor;
            }
            guard
                .bus
                .queue_evt(PartEvent::Changed(big_sync_core::rpc::ObjChanged {
                    cursor,
                    part_ids: obj_state.parts.iter().copied().collect(),
                    obj_id,
                    payload: event_payload.clone(),
                }));
            guard.flush();
            Ok(())
        })
    }
    async fn add_obj_to_parts(&self, obj_id: ObjId, parts: Vec<PartId>) -> Res<()> {
        tracing::debug!(obj_id = %obj_id, part_count = parts.len(), "memory store add obj to parts");
        surelock::key::lock_scope(|key| {
            let (mut guard, _key) = key.lock(&self.inner);
            let guard = &mut *guard;
            let obj_state = guard.objs.entry(obj_id).or_default();

            guard.tombstoned_objs.remove(&obj_id);
            let payload = obj_state.payload.clone();
            let bucket_payload = payload.clone().unwrap_or(serde_json::Value::Null);
            obj_state.parts.extend(&parts);
            let cursor = guard.global_cursor.get();
            for &part_id in &parts {
                let part = guard.parts.entry(part_id).or_default();
                let old_state = part.members.get(&obj_id).cloned();
                match old_state {
                    Some(state) if state.removed_at.is_none() => continue,
                    Some(state) => {
                        if let Some(old_removed_at) = state.removed_at {
                            guard.bus.remove_evt(old_removed_at);
                        }
                        part.apply_bucket_transition(
                            obj_id,
                            cursor,
                            BucketMemberKind::Dead,
                            BucketMemberKind::Live(&bucket_payload),
                        );
                        if let Some(old) = part.members.get_mut(&obj_id) {
                            old.changed_at = cursor;
                            old.removed_at = None;
                        }
                    }
                    None => {
                        part.apply_bucket_transition(
                            obj_id,
                            cursor,
                            BucketMemberKind::Absent,
                            BucketMemberKind::Live(&bucket_payload),
                        );
                        part.members.insert(
                            obj_id,
                            PartMemberState {
                                added_at: cursor,
                                changed_at: cursor,
                                removed_at: None,
                            },
                        );
                    }
                }
                part.latest_cursor = cursor;
                guard
                    .bus
                    .queue_evt(PartEvent::Added(big_sync_core::rpc::ObjAddedToPart {
                        cursor,
                        part_id,
                        obj_id,
                        payload: payload.clone(),
                    }));
            }
            guard.flush();
            Ok(())
        })
    }
    async fn remove_obj_from_part(&self, obj_id: ObjId, part_id: PartId) -> Res<()> {
        tracing::debug!(obj_id = %obj_id, part_id = %part_id, "memory store remove obj from part");
        surelock::key::lock_scope(|key| {
            let (mut guard, _key) = key.lock(&self.inner);
            let guard = &mut *guard;

            let Some(obj_state) = guard.objs.get_mut(&obj_id) else {
                return Ok(());
            };

            let part = guard.parts.entry(part_id).or_default();
            let Some(old_state) = part.members.get(&obj_id).cloned() else {
                return Ok(());
            };
            if old_state.removed_at.is_some() {
                return Ok(());
            }
            let cursor = guard.global_cursor.get();
            let null_payload = serde_json::Value::Null;
            let old_payload = obj_state.payload.as_ref().unwrap_or(&null_payload);
            if let Some(old) = part.members.get_mut(&obj_id) {
                guard.bus.remove_evt(old.changed_at);
                guard.bus.remove_evt(old.added_at);
                old.removed_at = Some(cursor);
                old.changed_at = cursor;
            }
            part.apply_bucket_transition(
                obj_id,
                cursor,
                BucketMemberKind::Live(&old_payload),
                BucketMemberKind::Dead,
            );
            part.latest_cursor = cursor;
            obj_state.parts.remove(&part_id);
            guard
                .bus
                .queue_evt(PartEvent::Removed(big_sync_core::rpc::ObjRemovedFromPart {
                    cursor,
                    part_id,
                    obj_id,
                }));
            if obj_state.parts.is_empty() {
                let cursor = part.latest_cursor;
                guard.tombstoned_objs.insert(obj_id, cursor);
                guard.objs.remove(&obj_id);
            }

            guard.flush();

            Ok(())
        })
    }

    async fn get_peer_part_cursor(&self, peer_id: PeerId, part_id: PartId) -> Res<CursorIndex> {
        surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);

            Ok(guard
                .peer_part_cursors
                .get(&(peer_id, part_id))
                .cloned()
                .unwrap_or_default())
        })
    }

    async fn set_peer_part_cursor(
        &self,
        peer_id: PeerId,
        part_id: PartId,
        cursor: CursorIndex,
    ) -> Res<()> {
        tracing::debug!(peer_id = %peer_id, part_id = %part_id, cursor, "memory store set peer part cursor");
        surelock::key::lock_scope(|key| {
            let (mut guard, _key) = key.lock(&self.inner);
            guard.peer_part_cursors.insert((peer_id, part_id), cursor);
            Ok(())
        })
    }

    async fn list_events(
        &self,
        parts: HashSet<PartId>,
        cursor: CursorIndex,
        limit: u32,
    ) -> Res<Result<HashMap<PartId, PartPage>, ListPartsError>> {
        let part_count = parts.len();
        let result = surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            let mut out: HashMap<PartId, PartPage> = default();
            for part_id in parts {
                let Some(_part) = guard.parts.get(&part_id) else {
                    return Err(ListPartsError::UnkownParts {
                        unkown_parts: vec![part_id],
                    });
                };
                let mut next_cursor = None;
                let mut events = vec![];
                for (&ii, evt) in guard.events.range(cursor.saturating_add(1)..) {
                    if events.len() >= limit as usize {
                        next_cursor = Some(ii);
                        break;
                    }
                    let push = match evt {
                        PartEvent::Changed(inner) => inner.part_ids.contains(&part_id),
                        PartEvent::Added(inner) => inner.part_id == part_id,
                        PartEvent::Removed(inner) => inner.part_id == part_id,
                    };
                    if push {
                        events.push(evt.clone());
                    }
                }
                out.insert(
                    part_id,
                    PartPage {
                        events,
                        next_cursor,
                    },
                );
            }
            Ok(out)
        });
        tracing::debug!(
            part_count,
            cursor,
            limit,
            page_count = result.as_ref().map(|r| r.len()).unwrap_or(0),
            "memory store list events"
        );
        Ok(result)
    }

    async fn subscribe(
        &self,
        reqs: SubPartsRequest,
    ) -> Res<Result<mpsc::Receiver<SubEvent>, ListPartsError>> {
        tracing::debug!(part_count = reqs.parts.len(), "memory store subscribe");
        // make sure the parts exist first
        if let Err(err) = surelock::key::lock_scope(|key| {
            let (mut guard, _key) = key.lock(&self.inner);
            let guard = &mut *guard;
            for req in &reqs.parts {
                let part_id = req.part_id;
                if !guard.parts.contains_key(&part_id) {
                    return Err(ListPartsError::UnkownParts {
                        unkown_parts: vec![part_id],
                    });
                };
            }
            Ok(())
        }) {
            return Ok(Err(err));
        }
        let (tx, rx) = mpsc::unbounded("MemoryPartStore".into(), "caller".into());
        let state = Arc::clone(&self.inner);
        let fut = async move {
            let mut replay_done = false;
            let limit = 50;
            let mut events = Vec::with_capacity(limit);
            let parts: HashSet<_> = reqs.parts.iter().map(|req| req.part_id).collect();
            let mut cursor = reqs
                .parts
                .iter()
                .map(|req| req.cursor)
                .min()
                .unwrap_or(0)
                .saturating_add(1);
            while !replay_done {
                let mut next_cursor = cursor;
                surelock::key::lock_scope(|key| {
                    let (mut guard, _key) = key.lock(&state);
                    let guard = &mut *guard;

                    for (&ii, evt) in guard.events.range(cursor..) {
                        if events.len() >= limit {
                            next_cursor = ii + 1;
                            break;
                        }
                        let push = match evt {
                            PartEvent::Changed(inner) => {
                                inner.part_ids.iter().any(|id| parts.contains(id))
                            }
                            PartEvent::Added(inner) => parts.contains(&inner.part_id),
                            PartEvent::Removed(inner) => parts.contains(&inner.part_id),
                        };
                        if push {
                            events.push(evt.clone())
                        }
                        next_cursor = ii + 1;
                    }
                });
                replay_done = events.is_empty();
                for evt in events.drain(..) {
                    if tx
                        .send(match evt {
                            PartEvent::Changed(inner) => SubEvent::Changed(inner),
                            PartEvent::Added(inner) => SubEvent::Added(inner),
                            PartEvent::Removed(inner) => SubEvent::Removed(inner),
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                cursor = next_cursor;
            }
            if tx.send(SubEvent::ReplayComplete).await.is_err() {
                return;
            }
            surelock::key::lock_scope(|key| {
                let (mut guard, _key) = key.lock(&state);
                let guard = &mut *guard;

                let sub_id = Uuid::new_v4();
                guard.bus.subs.insert(sub_id, tx);
                for &part_id in &parts {
                    let subs = guard.bus.subs_by_part.entry(part_id).or_default();
                    subs.insert(sub_id);
                }
                guard.bus.part_by_sub.insert(sub_id, parts);
            });
        };
        tokio::spawn(fut);
        Ok(Ok(rx))
    }
}

// impl MemoryPartStore {
//     pub async fn is_tombstoned(&self, obj_id: ObjId) -> Res<bool> {
//         surelock::key::lock_scope(|key| {
//             let (guard, _key) = key.lock(&self.inner);
//             Ok(guard.tombstoned_objs.contains_key(&obj_id))
//         })
//     }
//
//     pub async fn obj_sync_version(&self, obj_id: ObjId) -> Res<u64> {
//         surelock::key::lock_scope(|key| {
//             let (guard, _key) = key.lock(&self.inner);
//             Ok(guard.obj_sync_version_locked(obj_id))
//         })
//     }
//
//     pub(crate) async fn obj_sync_stamp(&self, obj_id: ObjId) -> Res<ObjSyncStamp> {
//         surelock::key::lock_scope(|key| {
//             let (guard, _key) = key.lock(&self.inner);
//             Ok(guard.obj_sync_stamp_locked(obj_id))
//         })
//     }
//
//     async fn get_peer_obj_payload(
//         &self,
//         peer_id: PeerId,
//         obj_id: ObjId,
//     ) -> Res<Option<Option<ObjPayload>>> {
//         surelock::key::lock_scope(|key| {
//             let (guard, _key) = key.lock(&self.inner);
//             Ok(guard.peer_obj_payloads.get(&(peer_id, obj_id)).cloned())
//         })
//     }
//
//     async fn set_peer_obj_payload(
//         &self,
//         peer_id: PeerId,
//         obj_id: ObjId,
//         payload: Option<ObjPayload>,
//     ) -> Res<()> {
//         surelock::key::lock_scope(|key| {
//             let (mut guard, _key) = key.lock(&self.inner);
//             guard.peer_obj_payloads.insert((peer_id, obj_id), payload);
//             Ok(())
//         })
//     }
//
//     #[tracing::instrument(
//         skip(self, payload, parts),
//         fields(obj_id = %obj_id, part_count = parts.len())
//     )]
//     pub(crate) async fn sync_upsert_obj(
//         &self,
//         obj_id: ObjId,
//         payload: ObjPayload,
//         parts: Vec<PartId>,
//         expected_stamp: ObjSyncStamp,
//         sync_version: u64,
//         sync_stamp: ObjSyncStamp,
//     ) -> Res<SyncMutationOutcome> {
//         surelock::key::lock_scope(|key| {
//             let (mut guard, _key) = key.lock(&self.inner);
//             if guard.obj_sync_stamp_locked(obj_id) != expected_stamp {
//                 tracing::trace!(
//                     obj_id = %obj_id,
//                     part_count = parts.len(),
//                     expected_stamp = ?expected_stamp,
//                     "sync upsert stale"
//                 );
//                 return Ok(SyncMutationOutcome::Stale);
//             }
//             guard.upsert_obj_locked(
//                 self.owner_peer_id,
//                 obj_id,
//                 payload,
//                 parts,
//                 Some(sync_version),
//                 Some(sync_stamp),
//             );
//             tracing::trace!(
//                 obj_id = %obj_id,
//                 part_count = guard
//                     .objs
//                     .get(&obj_id)
//                     .map(|obj| obj.parts.len())
//                     .unwrap_or_default(),
//                 expected_stamp = ?expected_stamp,
//                 "sync upsert applied"
//             );
//             Ok(SyncMutationOutcome::Applied)
//         })
//     }
//
//     #[tracing::instrument(
//     skip(self),
//     fields(obj_id = %obj_id, part_id = %part_id)
// )]
//     pub(crate) async fn sync_remove_obj_from_part(
//         &self,
//         obj_id: ObjId,
//         part_id: PartId,
//         expected_stamp: ObjSyncStamp,
//         sync_version: u64,
//         sync_stamp: ObjSyncStamp,
//     ) -> Res<SyncMutationOutcome> {
//         surelock::key::lock_scope(|key| {
//             let (mut guard, _key) = key.lock(&self.inner);
//             if guard.obj_sync_stamp_locked(obj_id) != expected_stamp {
//                 tracing::trace!(
//                     obj_id = %obj_id,
//                     part_id = %part_id,
//                     expected_stamp = ?expected_stamp,
//                     "sync remove stale"
//                 );
//                 return Ok(SyncMutationOutcome::Stale);
//             }
//             let Some(part) = guard.parts.get(&part_id) else {
//                 tracing::trace!(
//                     obj_id = %obj_id,
//                     part_id = %part_id,
//                     expected_stamp = ?expected_stamp,
//                     "sync remove stale missing part"
//                 );
//                 return Ok(SyncMutationOutcome::Stale);
//             };
//             let Some(state) = part.members.get(&obj_id) else {
//                 tracing::trace!(
//                     obj_id = %obj_id,
//                     part_id = %part_id,
//                     expected_stamp = ?expected_stamp,
//                     "sync remove stale missing member"
//                 );
//                 return Ok(SyncMutationOutcome::Stale);
//             };
//             if state.removed_at.is_some() {
//                 tracing::trace!(
//                     obj_id = %obj_id,
//                     part_id = %part_id,
//                     expected_stamp = ?expected_stamp,
//                     "sync remove stale already removed"
//                 );
//                 return Ok(SyncMutationOutcome::Stale);
//             }
//             guard.remove_obj_from_part_locked(
//                 self.owner_peer_id,
//                 obj_id,
//                 part_id,
//                 Some(sync_version),
//                 Some(sync_stamp),
//             );
//             tracing::trace!(
//                 obj_id = %obj_id,
//                 part_id = %part_id,
//                 expected_stamp = ?expected_stamp,
//                 "sync remove applied"
//             );
//             Ok(SyncMutationOutcome::Applied)
//         })
//     }
//
//     // #[tracing::instrument(
//     //     skip(self),
//     //     fields(obj_id = %obj_id)
//     // )]
//     // async fn sync_tombstone_obj(
//     //     &self,
//     //     obj_id: ObjId,
//     //     expected_stamp: ObjSyncStamp,
//     //     sync_version: u64,
//     //     sync_stamp: ObjSyncStamp,
//     // ) -> Res<SyncMutationOutcome> {
//     //     surelock::key::lock_scope(|key| {
//     //         let (mut guard, _key) = key.lock(&self.inner);
//     //         if guard.obj_sync_stamp_locked(obj_id) != expected_stamp {
//     //             tracing::trace!(
//     //                 obj_id = %obj_id,
//     //                 expected_stamp = ?expected_stamp,
//     //                 "sync tombstone stale"
//     //             );
//     //             return Ok(SyncMutationOutcome::Stale);
//     //         }
//     //         let Some(parts) = guard
//     //             .objs
//     //             .get(&obj_id)
//     //             .map(|obj| obj.parts.iter().copied().collect::<Vec<_>>())
//     //         else {
//     //             if guard.tombstoned_objs.contains_key(&obj_id) {
//     //                 return Ok(SyncMutationOutcome::Applied);
//     //             }
//     //             return Ok(SyncMutationOutcome::Stale);
//     //         };
//     //         for part_id in parts {
//     //             guard.remove_obj_from_part_locked(
//     //                 self.owner_peer_id,
//     //                 obj_id,
//     //                 part_id,
//     //                 Some(sync_version),
//     //                 Some(sync_stamp.clone()),
//     //             );
//     //         }
//     //         Ok(SyncMutationOutcome::Applied)
//     //     })
//     // }
// }

#[cfg(test)]
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MemoryPartStoreSnapshot {
    pub objs: BTreeMap<ObjId, MemoryObjSnapshot>,
    pub peer_part_cursors: BTreeMap<(PeerId, PartId), CursorIndex>,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MemoryObjSnapshot {
    pub payload: Option<ObjPayload>,
    pub parts: BTreeSet<PartId>,
}

#[cfg(test)]
impl MemoryPartStore {
    pub(crate) async fn snapshot(&self) -> Res<MemoryPartStoreSnapshot> {
        surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            let objs = guard
                .objs
                .iter()
                .map(|(&obj_id, obj)| {
                    (
                        obj_id,
                        MemoryObjSnapshot {
                            payload: obj.payload.clone(),
                            parts: obj.parts.iter().copied().collect(),
                        },
                    )
                })
                .collect();
            Ok(MemoryPartStoreSnapshot {
                objs,
                peer_part_cursors: guard.peer_part_cursors.clone().into_iter().collect(),
            })
        })
    }
}

#[cfg(test)]
impl From<MemoryPartStoreSnapshot> for ObservedStoreSnapshot {
    fn from(value: MemoryPartStoreSnapshot) -> Self {
        Self {
            objs: value
                .objs
                .into_iter()
                .map(|(obj_id, snapshot)| {
                    (
                        obj_id,
                        ObservedObjSnapshot {
                            payload: snapshot.payload,
                            parts: snapshot.parts,
                        },
                    )
                })
                .collect(),
            peer_part_cursors: value.peer_part_cursors,
        }
    }
}

#[cfg(test)]
#[async_trait]
impl ObservedStore for MemoryPartStore {
    async fn observed_snapshot(&self) -> Res<ObservedStoreSnapshot> {
        Ok(self.snapshot().await?.into())
    }
}

#[cfg(test)]
#[async_trait]
impl TestStoreSetup for MemoryPartStore {
    async fn ensure_test_part(&self, part_id: PartId) -> Res<()> {
        self.ensure_part(part_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::part_store::host_contract::{self, HostPartStoreContractHarness};

    struct MemoryHostHarness {
        store: MemoryPartStore,
    }

    #[async_trait]
    impl HostPartStoreContractHarness for MemoryHostHarness {
        fn store(&self) -> &dyn HostPartStore {
            &self.store
        }

        async fn ensure_part(&self, part_id: PartId) -> Res<()> {
            self.store.ensure_part(part_id).await
        }
    }

    fn test_store() -> MemoryPartStore {
        MemoryPartStore::new(PeerId(Byte32Id::new([7; 32])))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn memory_host_part_store_contract() -> Res<()> {
        let harness = MemoryHostHarness {
            store: test_store(),
        };
        host_contract::assert_host_part_store_contract(&harness).await
    }
}
