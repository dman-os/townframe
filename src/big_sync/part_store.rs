use crate::interlude::*;

use crate::{ScopeRef, ScopedIdResolver, ScopedObjRef, ScopedPartRef};
use big_sync_core::part_store::{CursorIndex, ObjPayload};
use big_sync_core::rpc::{
    BucketObjPageEntry, BucketSummary, GetChangedBucketsRequest, LeafBucketsError,
    LeafBucketsRequest, LeafBucketResult, ListPartsError, PartEvent, PartPage, PartSummary,
    SubEvent, SubPartsRequest,
};
use big_sync_core::{BuckId, Byte32Id, Fingerprint, FingerprintSeed, ObjId, PartId, PeerId};
use tokio::sync::mpsc;

use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::BTreeSet;

// pub mod sqlite;

#[async_trait]
pub trait HostPartitionStore: Send + Sync {
    async fn summarize_parts(
        &self,
        parts: HashSet<PartId>,
    ) -> Res<Result<HashMap<PartId, PartSummary>, ListPartsError>>;
    async fn get_changed_buckets(
        &self,
        req: GetChangedBucketsRequest,
    ) -> Res<Result<Vec<BucketSummary>, ListPartsError>>;
    async fn leaf_buckets(
        &self,
        req: LeafBucketsRequest,
    ) -> Res<Result<LeafBucketResult, LeafBucketsError>>;
    async fn member_count(&self, part_id: PartId) -> Res<u64>;
    async fn obj_payload(&self, obj_id: ObjId) -> Res<Option<ObjPayload>>;
    async fn get_bucket_summary(&self, part_id: PartId, id: BuckId) -> Res<BucketSummary>;

    async fn upsert_obj(&self, obj_id: ObjId, payload: ObjPayload, parts: Vec<PartId>) -> Res<()>;

    async fn obj_parts(&self, obj_id: ObjId) -> Res<Vec<PartId>>;

    async fn add_obj_to_parts(&self, obj_id: ObjId, parts: Vec<PartId>) -> Res<()>;
    async fn remove_obj_from_part(&self, obj_id: ObjId, part_id: PartId) -> Res<()>;

    async fn get_peer_part_cursor(&self, peer_id: PeerId, part_id: PartId) -> Res<CursorIndex>;

    async fn set_peer_part_cursor(
        &self,
        peer_id: PeerId,
        part_id: PartId,
        cursor: CursorIndex,
    ) -> Res<()>;

    async fn list_events(
        &self,
        parts: HashSet<PartId>,
        cursor: CursorIndex,
        limit: u32,
    ) -> Res<Result<HashMap<PartId, PartPage>, ListPartsError>>;

    async fn subscribe(
        &self,
        reqs: SubPartsRequest,
    ) -> Res<Result<mpsc::UnboundedReceiver<SubEvent>, ListPartsError>>;
}

structstruck::strike! {
    pub struct MemoryPartStore {
        inner: Arc<surelock::mutex::Mutex<
            #[derive(Default)]
            struct MemoryPartStoreState {
                next_scope_id: u64,
                next_part_id: u64,
                next_obj_id: u64,
                scopes_by_name: HashMap<ScopeRef, u64>,
                scope_names_by_id: HashMap<u64, ScopeRef>,
                parts_by_scoped_ref: HashMap<(u64, Arc<str>), PartId>,
                scoped_refs_by_part: HashMap<PartId, (u64, Arc<str>)>,
                objs_by_scoped_ref: HashMap<(u64, Arc<str>), ObjId>,
                scoped_refs_by_obj: HashMap<ObjId, (u64, Arc<str>)>,
                global_cursor:
                    #[derive(Default)]
                    struct GlobalCursor {
                        counter: std::sync::atomic::AtomicU64,
                        event_to_part: BTreeMap<CursorIndex, PartId>,
                    },
                parts: HashMap<
                    PartId,
                    struct PartState {
                        #![derive(Default)]
                        latest_cursor: CursorIndex,

                        events: BTreeMap<CursorIndex, PartEvent>,

                        members: HashMap<
                            ObjId,
                            struct PartMemberState {
                                changed_at: CursorIndex,
                                removed_at: Option<CursorIndex>,
                            }
                        >,

                        bus: struct MemorySubsBus {
                            #![derive(Default)]
                            events_to_drop: Vec<CursorIndex>,
                            buf: Vec<PartEvent>,
                            subs_to_drop: Vec<usize>,
                            subs: Vec<mpsc::UnboundedSender<SubEvent>>
                        },
                    }
                >,
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

impl Default for MemoryPartStore {
    fn default() -> Self {
        Self {
            inner: Arc::new(surelock::mutex::Mutex::new(default())),
        }
    }
}

impl MemoryPartStore {
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

impl MemoryPartStoreState {
    fn scope_id(&mut self, scope: &ScopeRef) -> u64 {
        if let Some(scope_id) = self.scopes_by_name.get(scope).copied() {
            return scope_id;
        }
        self.next_scope_id = self.next_scope_id.checked_add(1).expect(ERROR_IMPOSSIBLE);
        let scope_id = self.next_scope_id;
        let old = self.scopes_by_name.insert(scope.clone(), scope_id);
        assert!(old.is_none(), "fishy");
        let old = self.scope_names_by_id.insert(scope_id, scope.clone());
        assert!(old.is_none(), "fishy");
        scope_id
    }

    fn scope_ref(&self, scope_id: u64) -> ScopeRef {
        self.scope_names_by_id
            .get(&scope_id)
            .cloned()
            .expect(ERROR_IMPOSSIBLE)
    }

    fn bucket_items_for_path(
        &self,
        part_id: PartId,
        path: BuckId,
    ) -> Vec<(ObjId, CursorIndex, bool)> {
        let Some(part) = self.parts.get(&part_id) else {
            return Vec::new();
        };
        let level = path.level();
        let mut items = Vec::new();
        for (&obj_id, member) in &part.members {
            if BuckId::from_obj_id(level, &obj_id) != path {
                continue;
            }
            let cursor = member.removed_at.unwrap_or(member.changed_at);
            items.push((obj_id, cursor, member.removed_at.is_some()));
        }
        items.sort_by_key(|(obj_id, _, _)| *obj_id);
        items
    }

    fn bucket_summary(&self, part_id: PartId, path: BuckId) -> BucketSummary {
        let items = self.bucket_items_for_path(part_id, path);
        let live_items: Vec<_> = items
            .iter()
            .filter_map(|(obj_id, _, dead)| {
                if *dead {
                    None
                } else {
                    let payload = self
                        .objs
                        .get(obj_id)
                        .and_then(|obj| obj.payload.clone())
                        .expect(ERROR_IMPOSSIBLE);
                    Some((*obj_id, payload))
                }
            })
            .collect();
        let dead_items: Vec<_> = items
            .iter()
            .filter_map(|(obj_id, _, dead)| dead.then_some(*obj_id))
            .collect();
        let live_fp = Fingerprint::new(&FingerprintSeed::new(0x6c697665, 0x6275636b), &live_items);
        let dead_fp = Fingerprint::new(&FingerprintSeed::new(0x64656164, 0x6275636b), &dead_items);
        BucketSummary {
            id: path,
            len: items.len() as u32,
            live_count: live_items.len() as u32,
            fp: (live_fp.as_u64(), dead_fp.as_u64()),
            changed_at: items.iter().map(|(_, cursor, _)| *cursor).max().unwrap_or(0),
        }
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
        let mut buckets: BTreeMap<BuckId, BucketSummary> = BTreeMap::new();
        for &obj_id in part.members.keys() {
            let buck_id = BuckId::from_obj_id(offset.level(), &obj_id);
            buckets
                .entry(buck_id)
                .or_insert_with(|| self.bucket_summary(part_id, buck_id));
        }
        let mut buckets: Vec<_> = buckets
            .into_iter()
            .filter(|(buck_id, summary)| *buck_id >= offset && summary.changed_at > since)
            .map(|(_, summary)| summary)
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

#[async_trait]
impl ScopedIdResolver for MemoryPartStore {
    async fn resolve_part(&self, part: &ScopedPartRef) -> Res<PartId> {
        surelock::key::lock_scope(|key| {
            let (mut guard, _key) = key.lock(&self.inner);
            let scope_id = guard.scope_id(&part.scope);
            let key = (scope_id, Arc::<str>::clone(&part.part));
            if let Some(part_id) = guard.parts_by_scoped_ref.get(&key).copied() {
                guard.parts.entry(part_id).or_default();
                return Ok(part_id);
            }

            let part_id = PartId(Self::next_byte32_id(&mut guard.next_part_id, 1));
            let old = guard.parts_by_scoped_ref.insert(key.clone(), part_id);
            assert!(old.is_none(), "fishy");
            let old = guard.scoped_refs_by_part.insert(part_id, key);
            assert!(old.is_none(), "fishy");
            guard.parts.entry(part_id).or_default();
            Ok(part_id)
        })
    }

    async fn resolve_obj(&self, obj: &ScopedObjRef) -> Res<ObjId> {
        surelock::key::lock_scope(|key| {
            let (mut guard, _key) = key.lock(&self.inner);
            let scope_id = guard.scope_id(&obj.scope);
            let key = (scope_id, Arc::<str>::clone(&obj.obj));
            if let Some(obj_id) = guard.objs_by_scoped_ref.get(&key).copied() {
                return Ok(obj_id);
            }

            let obj_id = ObjId(Self::next_byte32_id(&mut guard.next_obj_id, 2));
            let old = guard.objs_by_scoped_ref.insert(key.clone(), obj_id);
            assert!(old.is_none(), "fishy");
            let old = guard.scoped_refs_by_obj.insert(obj_id, key);
            assert!(old.is_none(), "fishy");
            Ok(obj_id)
        })
    }

    async fn scoped_part(&self, part_id: PartId) -> Res<ScopedPartRef> {
        surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            let (scope_id, part) = guard
                .scoped_refs_by_part
                .get(&part_id)
                .cloned()
                .ok_or_else(|| ferr!("part id {part_id} is not scoped in memory part store"))?;
            Ok(ScopedPartRef::new(guard.scope_ref(scope_id), part))
        })
    }

    async fn scoped_obj(&self, obj_id: ObjId) -> Res<ScopedObjRef> {
        surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            let (scope_id, obj) = guard
                .scoped_refs_by_obj
                .get(&obj_id)
                .cloned()
                .ok_or_else(|| ferr!("obj id {obj_id} is not scoped in memory part store"))?;
            Ok(ScopedObjRef::new(guard.scope_ref(scope_id), obj))
        })
    }
}

impl PartState {
    fn flush(&mut self, global_cursor: &mut GlobalCursor) {
        for ii in self.bus.events_to_drop.drain(..) {
            self.events.remove(&ii);
            global_cursor.drop_cur(ii);
        }
        for evt in self.bus.buf.drain(..) {
            let (cursor, sub_evt) = match &evt {
                PartEvent::Upserted(inner) => (inner.cursor, SubEvent::Upserted(inner.clone())),
                PartEvent::Deleted(inner) => (inner.cursor, SubEvent::Deleted(inner.clone())),
            };
            self.events.insert(cursor, evt);
            for (ii, sub) in self.bus.subs.iter().enumerate() {
                if sub.send(sub_evt.clone()).is_err() {
                    self.bus.subs_to_drop.push(ii)
                }
            }
            self.bus.subs_to_drop.sort_unstable();
            self.bus.subs_to_drop.dedup();
            self.bus.subs_to_drop.reverse();
            for ii in self.bus.subs_to_drop.drain(..) {
                self.bus.subs.swap_remove(ii);
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
    fn get(&mut self, part_id: PartId) -> CursorIndex {
        let ii = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        self.event_to_part.insert(ii, part_id);
        ii
    }

    fn drop_cur(&mut self, ii: CursorIndex) {
        self.event_to_part.remove(&ii);
    }
}

#[async_trait]
impl HostPartitionStore for MemoryPartStore {
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
    async fn get_bucket_summary(&self, part_id: PartId, id: BuckId) -> Res<BucketSummary> {
        Ok(surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            guard.bucket_summary(part_id, id)
        }))
    }
    async fn get_changed_buckets(
        &self,
        req: GetChangedBucketsRequest,
    ) -> Res<Result<Vec<BucketSummary>, ListPartsError>> {
        Ok(surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            guard.changed_bucket_summaries(req.part_id, req.offset, req.since, req.limit_hint)
        }))
    }

    async fn leaf_buckets(
        &self,
        req: LeafBucketsRequest,
    ) -> Res<Result<LeafBucketResult, LeafBucketsError>> {
        Ok(surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            let Some(part) = guard.parts.get(&req.part_id) else {
                return Err(LeafBucketsError::UnkownPart);
            };
            let mut bucks = HashMap::new();
            for buck_id in req.buckets {
                let items = guard.bucket_items_for_path(req.part_id, buck_id);
                let entries = items
                    .into_iter()
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
                                .expect(ERROR_IMPOSSIBLE);
                            Fingerprint::new(&req.seed, &("big-sync-obj-fp-v1", obj_id, payload))
                        };
                        BucketObjPageEntry { obj_id, dead, fp }
                    })
                    .collect();
                bucks.insert(buck_id, entries);
            }
            Ok(LeafBucketResult {
                seed: req.seed,
                bucks,
            })
        }))
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

    async fn upsert_obj(&self, obj_id: ObjId, payload: ObjPayload, parts: Vec<PartId>) -> Res<()> {
        surelock::key::lock_scope(|key| {
            let (mut guard, _key) = key.lock(&self.inner);
            let guard = &mut *guard;
            let event_payload = payload.clone();
            guard.tombstoned_objs.remove(&obj_id);

            let obj = guard.objs.entry(obj_id).or_default();
            obj.payload = Some(payload);
            obj.parts.extend(&parts);

            for &part_id in &parts {
                let part = guard.parts.entry(part_id).or_default();
                if let Some(old) = part.members.get_mut(&obj_id) {
                    if let Some(old_removed_at) = old.removed_at.take() {
                        part.bus.remove_evt(old_removed_at);
                    } else {
                        part.bus.remove_evt(old.changed_at);
                    }
                    let cursor = guard.global_cursor.get(part_id);
                    old.changed_at = cursor;
                    part.latest_cursor = cursor;
                    part.bus.queue_evt(PartEvent::Upserted(big_sync_core::rpc::ObjUpserted {
                        cursor,
                        part_id,
                        obj_id,
                        payload: event_payload.clone(),
                    }));
                } else {
                    let state = PartMemberState {
                        changed_at: guard.global_cursor.get(part_id),
                        removed_at: None,
                    };
                    part.latest_cursor = state.changed_at;
                    part.bus.queue_evt(PartEvent::Upserted(big_sync_core::rpc::ObjUpserted {
                        cursor: state.changed_at,
                        part_id,
                        obj_id,
                        payload: event_payload.clone(),
                    }));
                    part.members.insert(obj_id, state);
                }
                part.flush(&mut guard.global_cursor);
            }
            Ok(())
        })
    }

    async fn add_obj_to_parts(&self, obj_id: ObjId, parts: Vec<PartId>) -> Res<()> {
        surelock::key::lock_scope(|key| {
            let (mut guard, _key) = key.lock(&self.inner);
            let guard = &mut *guard;
            let obj = guard.objs.entry(obj_id).or_default();
            let payload = obj.payload.clone().expect(ERROR_IMPOSSIBLE);
            guard.tombstoned_objs.remove(&obj_id);
            obj.parts.extend(&parts);
            for &part_id in &parts {
                let part = guard.parts.entry(part_id).or_default();
                if let Some(old) = part.members.get_mut(&obj_id) {
                    if old.removed_at.is_some() {
                        if let Some(old_removed_at) = old.removed_at.take() {
                            part.bus.remove_evt(old_removed_at);
                        }
                        old.changed_at = guard.global_cursor.get(part_id);
                        part.latest_cursor = old.changed_at;
                        part.bus.queue_evt(PartEvent::Upserted(big_sync_core::rpc::ObjUpserted {
                            cursor: old.changed_at,
                            part_id,
                            obj_id,
                            payload: payload.clone(),
                        }));
                    }
                } else {
                    let state = PartMemberState {
                        changed_at: guard.global_cursor.get(part_id),
                        removed_at: None,
                    };
                    part.latest_cursor = state.changed_at;
                    part.bus.queue_evt(PartEvent::Upserted(big_sync_core::rpc::ObjUpserted {
                        cursor: state.changed_at,
                        part_id,
                        obj_id,
                        payload: payload.clone(),
                    }));
                    part.members.insert(obj_id, state);
                }
                part.flush(&mut guard.global_cursor);
            }
            Ok(())
        })
    }
    async fn remove_obj_from_part(&self, obj_id: ObjId, part_id: PartId) -> Res<()> {
        surelock::key::lock_scope(|key| {
            let (mut guard, _key) = key.lock(&self.inner);
            let guard = &mut *guard;
            let part = guard.parts.entry(part_id).or_default();
            if let Some(old) = part.members.get_mut(&obj_id) {
                if old.removed_at.is_some() {
                    return Ok(());
                }
                part.bus.remove_evt(old.changed_at);
                let cursor = guard.global_cursor.get(part_id);
                part.latest_cursor = cursor;
                old.removed_at = Some(cursor);
                part.bus.queue_evt(PartEvent::Deleted(big_sync_core::rpc::ObjRemoved {
                    cursor,
                    part_id,
                    obj_id,
                }));
            }
            let remove_obj = if let Some(obj) = guard.objs.get_mut(&obj_id) {
                obj.parts.remove(&part_id);
                obj.parts.is_empty()
            } else {
                false
            };
            if remove_obj {
                let cursor = part.latest_cursor;
                guard.tombstoned_objs.insert(obj_id, cursor);
                guard.objs.remove(&obj_id);
            }

            part.flush(&mut guard.global_cursor);
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
        Ok(surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            let mut out = HashMap::new();
            for part_id in parts {
                let mut next_cursor = None;
                let mut events = vec![];
                let Some(part) = guard.parts.get(&part_id) else {
                    return Err(ListPartsError::UnkownParts {
                        unkown_parts: vec![part_id],
                    });
                };
                for (&ii, evt) in part.events.range(cursor.saturating_add(1)..) {
                    if events.len() >= limit as usize {
                        next_cursor = Some(ii);
                        break;
                    }
                    events.push(match evt {
                        PartEvent::Upserted(inner) => PartEvent::Upserted(inner.clone()),
                        PartEvent::Deleted(inner) => PartEvent::Deleted(inner.clone()),
                    })
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
        }))
    }

    async fn subscribe(
        &self,
        reqs: SubPartsRequest,
    ) -> Res<Result<mpsc::UnboundedReceiver<SubEvent>, ListPartsError>> {
        // make sure the parts exist first
        if let Err(err) = surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
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
        let (tx, rx) = mpsc::unbounded_channel();
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

                    for (&ii, part_id) in guard.global_cursor.event_to_part.range(cursor..) {
                        if events.len() >= limit {
                            next_cursor = ii + 1;
                            break;
                        }
                        let part = guard.parts.get(part_id).expect(ERROR_IMPOSSIBLE);
                        let evt = part.events.get(&ii).expect(ERROR_UNRECONIZED);
                        if parts.contains(part_id) {
                            events.push(match evt {
                                PartEvent::Upserted(inner) => PartEvent::Upserted(inner.clone()),
                                PartEvent::Deleted(inner) => PartEvent::Deleted(inner.clone()),
                            })
                        }
                        next_cursor = ii + 1;
                    }
                });
                replay_done = events.is_empty();
                for evt in events.drain(..) {
                    if tx
                        .send(match evt {
                            PartEvent::Upserted(inner) => SubEvent::Upserted(inner.clone()),
                            PartEvent::Deleted(inner) => SubEvent::Deleted(inner.clone()),
                        })
                        .is_err()
                    {
                        return;
                    }
                }
                cursor = next_cursor;
            }
            if tx.send(SubEvent::ReplayComplete).is_err() {
                return;
            }
            surelock::key::lock_scope(|key| {
                let (mut guard, _key) = key.lock(&state);
                let guard = &mut *guard;

                for part_id in parts {
                    let part = guard.parts.get_mut(&part_id).expect(ERROR_IMPOSSIBLE);
                    part.bus.subs.push(tx.clone());
                }
            });
        };
        tokio::spawn(fut);
        Ok(Ok(rx))
    }

}

impl MemoryPartStore {
    pub async fn is_tombstoned(&self, obj_id: ObjId) -> Res<bool> {
        surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            Ok(guard.tombstoned_objs.contains_key(&obj_id))
        })
    }

    pub async fn get_peer_obj_payload(
        &self,
        peer_id: PeerId,
        obj_id: ObjId,
    ) -> Res<Option<Option<ObjPayload>>> {
        surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            Ok(guard.peer_obj_payloads.get(&(peer_id, obj_id)).cloned())
        })
    }

    pub async fn set_peer_obj_payload(
        &self,
        peer_id: PeerId,
        obj_id: ObjId,
        payload: Option<ObjPayload>,
    ) -> Res<()> {
        surelock::key::lock_scope(|key| {
            let (mut guard, _key) = key.lock(&self.inner);
            guard.peer_obj_payloads.insert((peer_id, obj_id), payload);
            Ok(())
        })
    }

    pub async fn sync_upsert_obj(
        &self,
        obj_id: ObjId,
        payload: ObjPayload,
        parts: Vec<PartId>,
    ) -> Res<()> {
        if self.is_tombstoned(obj_id).await? {
            return Ok(());
        }
        self.upsert_obj(obj_id, payload, parts).await
    }
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MemoryPartStoreSnapshot {
    pub objs: BTreeMap<ObjId, MemoryObjSnapshot>,
    pub scoped_objs: BTreeMap<ScopedObjRef, MemoryScopedObjSnapshot>,
    pub peer_part_cursors: BTreeMap<(PeerId, PartId), CursorIndex>,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MemoryObjSnapshot {
    pub payload: Option<ObjPayload>,
    pub parts: BTreeSet<PartId>,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MemoryScopedObjSnapshot {
    pub payload: Option<ObjPayload>,
    pub parts: BTreeSet<ScopedPartRef>,
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
            let scoped_objs = guard
                .objs
                .iter()
                .filter_map(|(&obj_id, obj)| {
                    let (scope_id, obj_name) = guard.scoped_refs_by_obj.get(&obj_id)?.clone();
                    let scope = guard.scope_ref(scope_id);
                    let parts = obj
                        .parts
                        .iter()
                        .filter_map(|part_id| {
                            let (scope_id, part_name) =
                                guard.scoped_refs_by_part.get(part_id)?.clone();
                            Some(ScopedPartRef::new(guard.scope_ref(scope_id), part_name))
                        })
                        .collect();
                    Some((
                        ScopedObjRef::new(scope, obj_name),
                        MemoryScopedObjSnapshot {
                            payload: obj.payload.clone(),
                            parts,
                        },
                    ))
                })
                .collect();
            Ok(MemoryPartStoreSnapshot {
                objs,
                scoped_objs,
                peer_part_cursors: guard.peer_part_cursors.clone().into_iter().collect(),
            })
        })
    }
}
