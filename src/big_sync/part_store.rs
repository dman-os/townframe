use std::collections::BTreeMap;

use crate::interlude::*;

use big_sync_core::part_store::{CursorIndex, ObjPayload};
use big_sync_core::rpc::{
    ListPartsError, PartEvent, PartMemberEvent, PartMemberEventDeets, PartPage, PartSummary,
    SubEvent, SubPartsRequest,
};
use big_sync_core::{ObjId, PartId, PeerId};
use utils_rs::prelude::tokio::sync::mpsc;

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
                                added_at: CursorIndex,
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

impl PartState {
    fn flush(&mut self) {
        for ii in self.bus.events_to_drop.drain(..) {
            self.events.remove(&ii);
        }
        for evt in self.bus.buf.drain(..) {
            let (cursor, sub_evt) = match &evt {
                PartEvent::MemberEvent(inner) => {
                    (inner.cursor, SubEvent::MemberEvent(inner.clone()))
                }
                PartEvent::ObjChangeEvent(inner) => {
                    (inner.cursor, SubEvent::ObjChangeEvent(inner.clone()))
                }
            };
            self.events.insert(cursor, evt);
            for (ii, sub) in self.bus.subs.iter().enumerate() {
                if let Err(_) = sub.send(sub_evt.clone()) {
                    self.bus.subs_to_drop.push(ii)
                }
            }
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
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
                        member_count: part.members.len() as _,
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
                .map(|part| part.members.len() as u64)
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
                    part.bus.remove_evt(old.changed_at);
                    old.changed_at = guard.global_cursor.get(part_id);
                    part.bus.queue_evt(PartEvent::MemberEvent(PartMemberEvent {
                        cursor: old.changed_at,
                        part_id,
                        deets: PartMemberEventDeets::MemberUpsert(obj_id),
                    }));
                    part.latest_cursor = old.changed_at;
                } else {
                    let state = PartMemberState {
                        added_at: guard.global_cursor.get(part_id),
                        changed_at: guard.global_cursor.get(part_id),
                        removed_at: None,
                    };
                    part.bus.queue_evt(PartEvent::MemberEvent(PartMemberEvent {
                        cursor: state.added_at,
                        part_id,
                        deets: PartMemberEventDeets::MemberUpsert(obj_id),
                    }));
                    part.bus.queue_evt(PartEvent::MemberEvent(PartMemberEvent {
                        cursor: state.changed_at,
                        part_id,
                        deets: PartMemberEventDeets::MemberUpsert(obj_id),
                    }));
                    part.latest_cursor = state.changed_at;
                    part.members.insert(obj_id, state);
                }
                part.flush();
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
                if let None = part.members.get_mut(&obj_id) {
                    let state = PartMemberState {
                        added_at: guard.global_cursor.get(part_id),
                        changed_at: guard.global_cursor.get(part_id),
                        removed_at: None,
                    };
                    part.bus.queue_evt(PartEvent::MemberEvent(PartMemberEvent {
                        cursor: state.added_at,
                        part_id,
                        deets: PartMemberEventDeets::MemberUpsert(obj_id),
                    }));
                    part.bus.queue_evt(PartEvent::MemberEvent(PartMemberEvent {
                        cursor: state.changed_at,
                        part_id,
                        deets: PartMemberEventDeets::MemberUpsert(obj_id),
                    }));
                    part.latest_cursor = state.changed_at;
                    part.members.insert(obj_id, state);
                }
                part.flush();
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
                if let Some(old) = old.removed_at {
                    part.bus.remove_evt(old);
                }
                part.latest_cursor = guard.global_cursor.get(part_id);
                old.removed_at = Some(part.latest_cursor);
                part.bus.queue_evt(PartEvent::MemberEvent(PartMemberEvent {
                    cursor: old.changed_at,
                    part_id,
                    deets: PartMemberEventDeets::MemberRemove(obj_id),
                }));
            }
            let obj = guard.objs.entry(obj_id).or_default();
            obj.parts.remove(&part_id);

            part.flush();
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
                for (&ii, evt) in part.events.range(cursor..) {
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
                if let None = guard.parts.get(&part_id) {
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
        let state = self.inner.clone();
        let fut = async move {
            let mut replay_done = false;
            let limit = 50;
            let mut events = Vec::with_capacity(limit);
            let parts: HashSet<_> = reqs.parts.iter().map(|req| req.part_id).collect();
            let mut cursor = reqs.parts.iter().map(|req| req.cursor).min().unwrap_or(0);
            while !replay_done {
                surelock::key::lock_scope(|key| {
                    let (mut guard, _key) = key.lock(&state);
                    let guard = &mut *guard;

                    for (&ii, part_id) in guard.global_cursor.event_to_part.range(cursor..) {
                        if events.len() >= limit as usize {
                            cursor = ii + 1;
                            break;
                        }
                        let part = guard.parts.get(&part_id).expect(ERROR_IMPOSSIBLE);
                        let evt = part.events.get(&ii).expect(ERROR_UNRECONIZED);
                        if parts.contains(&part_id) {
                            events.push(evt.clone())
                        }
                    }
                });
                replay_done = events.is_empty();
                for evt in events.drain(..) {
                    if let Err(_) = tx.send(match evt {
                        PartEvent::MemberEvent(inner) => SubEvent::MemberEvent(inner),
                        PartEvent::ObjChangeEvent(inner) => SubEvent::ObjChangeEvent(inner),
                    }) {
                        return;
                    }
                }
            }
            if let Err(_) = tx.send(SubEvent::ReplayComplete) {
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
        tokio::spawn(async move { fut.await });
        Ok(Ok(rx))
    }
}
