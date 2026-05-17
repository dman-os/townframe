//! NOTE: storage errors are not represented here. Implementations should
//! either recover within the methods, panic or tear down the sync machine

use crate::interlude::*;

use crate::rpc::BucketSummary;

/// NOTE: cursors are cross-part and peer global.
pub type CursorIndex = u64;
pub type ObjPayload = serde_json::Value;

pub trait PartStoreReadOnly<K: FutureForm> {
    fn member_count<'a>(&'a self, part_id: PartId) -> K::Future<'a, u64>;
    fn obj_payload<'a>(&'a self, obj_id: ObjId) -> K::Future<'a, Option<ObjPayload>>;

    fn obj_parts<'a>(&'a self, obj_id: ObjId) -> K::Future<'a, Vec<PartId>>;
    fn get_peer_part_cursor<'a>(
        &'a self,
        peer_id: PeerId,
        part_id: PartId,
    ) -> K::Future<'a, CursorIndex>;

    fn get_bucket_summary<'a>(
        &'a self,
        part_id: PartId,
        id: BuckId,
    ) -> K::Future<'a, BucketSummary>;
}
pub trait PartStore<K: FutureForm>: PartStoreReadOnly<K> {
    fn upsert_obj<'a>(
        &'a self,
        obj_id: ObjId,
        payload: &ObjPayload,
        parts: &[PartId],
    ) -> K::Future<'a, ()>;

    fn add_obj_to_parts<'a>(&'a self, obj_id: ObjId, parts: &[PartId]) -> K::Future<'a, ()>;
    fn remove_obj_from_part<'a>(&'a self, obj_id: ObjId, part_id: PartId) -> K::Future<'a, ()>;

    fn set_peer_part_cursor<'a>(
        &'a self,
        peer_id: PeerId,
        part_id: PartId,
        cursor: CursorIndex,
    ) -> K::Future<'a, ()>;
}

#[cfg(any(test, feature = "test-support"))]
pub mod contract {
    use super::*;
    use future_form::Sendable;

    pub async fn assert_membership_semantics<S>(store: &S, part_id: PartId, obj_id: ObjId)
    where
        S: PartStore<Sendable> + Sync,
    {
        let payload_a = serde_json::json!({"phase": "a"});
        let payload_b = serde_json::json!({"phase": "b"});
        let payload_c = serde_json::json!({"phase": "c"});

        store.upsert_obj(obj_id, &payload_a, &[part_id]).await;
        assert_eq!(store.obj_payload(obj_id).await, Some(payload_a.clone()));
        assert_eq!(store.obj_parts(obj_id).await, vec![part_id]);
        assert_eq!(store.member_count(part_id).await, 1);

        store.upsert_obj(obj_id, &payload_b, &[part_id]).await;
        assert_eq!(store.obj_payload(obj_id).await, Some(payload_b.clone()));
        assert_eq!(store.obj_parts(obj_id).await, vec![part_id]);
        assert_eq!(store.member_count(part_id).await, 1);

        store.remove_obj_from_part(obj_id, part_id).await;
        assert_eq!(store.obj_payload(obj_id).await, None);
        assert_eq!(store.obj_parts(obj_id).await, Vec::<PartId>::new());
        assert_eq!(store.member_count(part_id).await, 0);

        store.upsert_obj(obj_id, &payload_c, &[part_id]).await;
        assert_eq!(store.obj_payload(obj_id).await, Some(payload_c.clone()));
        assert_eq!(store.obj_parts(obj_id).await, vec![part_id]);
        assert_eq!(store.member_count(part_id).await, 1);
    }

    pub async fn assert_add_obj_to_parts_is_idempotent<S>(store: &S, part_id: PartId, obj_id: ObjId)
    where
        S: PartStore<Sendable> + Sync,
    {
        let payload = serde_json::json!({"phase": "restore"});

        store.upsert_obj(obj_id, &payload, &[part_id]).await;
        store.add_obj_to_parts(obj_id, &[part_id]).await;

        assert_eq!(store.obj_payload(obj_id).await, Some(payload.clone()));
        assert_eq!(store.obj_parts(obj_id).await, vec![part_id]);
        assert_eq!(store.member_count(part_id).await, 1);
    }

    pub async fn assert_peer_cursor_roundtrip<S>(store: &S, peer_id: PeerId, part_id: PartId)
    where
        S: PartStore<Sendable> + Sync,
    {
        assert_eq!(store.get_peer_part_cursor(peer_id, part_id).await, 0);
        store.set_peer_part_cursor(peer_id, part_id, 17).await;
        assert_eq!(store.get_peer_part_cursor(peer_id, part_id).await, 17);
        store.set_peer_part_cursor(peer_id, part_id, 23).await;
        assert_eq!(store.get_peer_part_cursor(peer_id, part_id).await, 23);
    }
}

#[cfg(test)]
mod tests {
    use super::contract;
    use super::*;
    use crate::ids::Byte32Id;
    use future_form::{FutureForm, Sendable};
    use futures::executor::block_on;

    #[derive(Default)]
    struct HarnessStore {
        inner: std::sync::Mutex<HarnessState>,
    }

    #[derive(Default)]
    struct HarnessState {
        objs: Map<ObjId, HarnessObj>,
        peer_part_cursors: Map<(PeerId, PartId), CursorIndex>,
    }

    #[derive(Default)]
    struct HarnessObj {
        payload: Option<ObjPayload>,
        parts: Set<PartId>,
    }

    impl PartStoreReadOnly<Sendable> for HarnessStore {
        fn member_count<'a>(
            &'a self,
            part_id: PartId,
        ) -> <Sendable as FutureForm>::Future<'a, u64> {
            Sendable::from_future(async move {
                let state = self.inner.lock().expect(ERROR_MUTEX);
                state
                    .objs
                    .values()
                    .filter(|obj| obj.parts.contains(&part_id))
                    .count() as u64
            })
        }

        fn obj_payload<'a>(
            &'a self,
            obj_id: ObjId,
        ) -> <Sendable as FutureForm>::Future<'a, Option<ObjPayload>> {
            Sendable::from_future(async move {
                let state = self.inner.lock().expect(ERROR_MUTEX);
                state.objs.get(&obj_id).and_then(|obj| obj.payload.clone())
            })
        }

        fn obj_parts<'a>(
            &'a self,
            obj_id: ObjId,
        ) -> <Sendable as FutureForm>::Future<'a, Vec<PartId>> {
            Sendable::from_future(async move {
                let state = self.inner.lock().expect(ERROR_MUTEX);
                state
                    .objs
                    .get(&obj_id)
                    .map(|obj| obj.parts.iter().copied().collect())
                    .unwrap_or_default()
            })
        }

        fn get_peer_part_cursor<'a>(
            &'a self,
            peer_id: PeerId,
            part_id: PartId,
        ) -> <Sendable as FutureForm>::Future<'a, CursorIndex> {
            Sendable::from_future(async move {
                let state = self.inner.lock().expect(ERROR_MUTEX);
                state
                    .peer_part_cursors
                    .get(&(peer_id, part_id))
                    .copied()
                .unwrap_or_default()
            })
        }

        fn get_bucket_summary<'a>(
            &'a self,
            _part_id: PartId,
            id: BuckId,
        ) -> <Sendable as FutureForm>::Future<'a, BucketSummary> {
            Sendable::from_future(async move {
                BucketSummary {
                    id,
                    len: 0,
                    live_count: 0,
                    fp: default(),
                    changed_at: 0,
                }
            })
        }
    }
    impl PartStore<Sendable> for HarnessStore {
        fn upsert_obj<'a>(
            &'a self,
            obj_id: ObjId,
            payload: &ObjPayload,
            parts: &[PartId],
        ) -> <Sendable as FutureForm>::Future<'a, ()> {
            let payload = payload.clone();
            let parts = parts.to_vec();
            Sendable::from_future(async move {
                let mut state = self.inner.lock().expect(ERROR_MUTEX);
                let obj = state.objs.entry(obj_id).or_default();
                obj.payload = Some(payload);
                obj.parts.extend(parts);
            })
        }
        fn add_obj_to_parts<'a>(
            &'a self,
            obj_id: ObjId,
            parts: &[PartId],
        ) -> <Sendable as FutureForm>::Future<'a, ()> {
            let parts = parts.to_vec();
            Sendable::from_future(async move {
                let mut state = self.inner.lock().expect(ERROR_MUTEX);
                let obj = state.objs.entry(obj_id).or_default();
                obj.parts.extend(parts);
            })
        }

        fn remove_obj_from_part<'a>(
            &'a self,
            obj_id: ObjId,
            part_id: PartId,
        ) -> <Sendable as FutureForm>::Future<'a, ()> {
            Sendable::from_future(async move {
                let mut state = self.inner.lock().expect(ERROR_MUTEX);
                let remove_obj = if let Some(obj) = state.objs.get_mut(&obj_id) {
                    obj.parts.remove(&part_id);
                    obj.parts.is_empty()
                } else {
                    false
                };
                if remove_obj {
                    state.objs.remove(&obj_id);
                }
            })
        }

        fn set_peer_part_cursor<'a>(
            &'a self,
            peer_id: PeerId,
            part_id: PartId,
            cursor: CursorIndex,
        ) -> <Sendable as FutureForm>::Future<'a, ()> {
            Sendable::from_future(async move {
                let mut state = self.inner.lock().expect(ERROR_MUTEX);
                state.peer_part_cursors.insert((peer_id, part_id), cursor);
            })
        }
    }

    #[test]
    fn membership_semantics() {
        block_on(async {
            let store = HarnessStore::default();
            contract::assert_membership_semantics(
                &store,
                PartId(Byte32Id::new([1; 32])),
                ObjId(Byte32Id::new([2; 32])),
            )
            .await;
        });
    }

    #[test]
    fn add_obj_to_parts_is_idempotent() {
        block_on(async {
            let store = HarnessStore::default();
            contract::assert_add_obj_to_parts_is_idempotent(
                &store,
                PartId(Byte32Id::new([3; 32])),
                ObjId(Byte32Id::new([4; 32])),
            )
            .await;
        });
    }

    #[test]
    fn peer_cursor_roundtrip() {
        block_on(async {
            let store = HarnessStore::default();
            contract::assert_peer_cursor_roundtrip(
                &store,
                PeerId(Byte32Id::new([5; 32])),
                PartId(Byte32Id::new([6; 32])),
            )
            .await;
        });
    }
}
