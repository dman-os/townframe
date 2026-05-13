use crate::interlude::*;

use crate::{ScopeRef, ScopedIdResolver, ScopedObjRef, ScopedPartRef};
use big_sync_core::part_store::{CursorIndex, ObjPayload};
use big_sync_core::rpc::{
    ListPartsError, PartEvent, PartPage, PartSummary, PartTransition, SubEvent, SubPartsRequest,
};
use big_sync_core::{Byte32Id, ObjId, PartId, PeerId};
use tokio::sync::mpsc;

use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::BTreeSet;

pub mod sqlite;

#[async_trait]
pub trait HostPartitionStore: Send + Sync {
    async fn summarize_parts(
        &self,
        parts: HashSet<PartId>,
    ) -> Res<Result<HashMap<PartId, PartSummary>, ListPartsError>>;
    async fn member_count(&self, part_id: PartId) -> Res<u64>;
    async fn obj_payload(&self, obj_id: ObjId) -> Res<Option<ObjPayload>>;

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
                    part.bus.queue_evt(PartEvent::Upserted(PartTransition {
                        cursor,
                        part_id,
                        obj_id,
                    }));
                } else {
                    let state = PartMemberState {
                        changed_at: guard.global_cursor.get(part_id),
                        removed_at: None,
                    };
                    part.latest_cursor = state.changed_at;
                    part.bus.queue_evt(PartEvent::Upserted(PartTransition {
                        cursor: state.changed_at,
                        part_id,
                        obj_id,
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
                        part.bus.queue_evt(PartEvent::Upserted(PartTransition {
                            cursor: old.changed_at,
                            part_id,
                            obj_id,
                        }));
                    }
                } else {
                    let state = PartMemberState {
                        changed_at: guard.global_cursor.get(part_id),
                        removed_at: None,
                    };
                    part.latest_cursor = state.changed_at;
                    part.bus.queue_evt(PartEvent::Upserted(PartTransition {
                        cursor: state.changed_at,
                        part_id,
                        obj_id,
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
                if let Some(old_removed_at) = old.removed_at.take() {
                    part.bus.remove_evt(old_removed_at);
                } else {
                    part.bus.remove_evt(old.changed_at);
                }
                let cursor = guard.global_cursor.get(part_id);
                part.latest_cursor = cursor;
                old.removed_at = Some(cursor);
                part.bus.queue_evt(PartEvent::Deleted(PartTransition {
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
                    events.push(evt.clone())
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
                            events.push(evt.clone())
                        }
                        next_cursor = ii + 1;
                    }
                });
                replay_done = events.is_empty();
                for evt in events.drain(..) {
                    if tx
                        .send(match evt {
                            PartEvent::Upserted(inner) => SubEvent::Upserted(inner),
                            PartEvent::Deleted(inner) => SubEvent::Deleted(inner),
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
