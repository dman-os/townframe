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
    fn upsert_obj<'a>(&'a self, obj_id: ObjId, payload: &ObjPayload) -> K::Future<'a, ()>;

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

        store.upsert_obj(obj_id, &payload_a).await;
        store.add_obj_to_parts(obj_id, &[part_id]).await;
        assert_eq!(store.obj_payload(obj_id).await, Some(payload_a.clone()));
        assert_eq!(store.obj_parts(obj_id).await, vec![part_id]);
        assert_eq!(store.member_count(part_id).await, 1);

        store.upsert_obj(obj_id, &payload_b).await;
        store.add_obj_to_parts(obj_id, &[part_id]).await;
        assert_eq!(store.obj_payload(obj_id).await, Some(payload_b.clone()));
        assert_eq!(store.obj_parts(obj_id).await, vec![part_id]);
        assert_eq!(store.member_count(part_id).await, 1);

        store.remove_obj_from_part(obj_id, part_id).await;
        assert_eq!(store.obj_payload(obj_id).await, None);
        assert_eq!(store.obj_parts(obj_id).await, Vec::<PartId>::new());
        assert_eq!(store.member_count(part_id).await, 0);

        store.upsert_obj(obj_id, &payload_c).await;
        store.add_obj_to_parts(obj_id, &[part_id]).await;
        assert_eq!(store.obj_payload(obj_id).await, Some(payload_c.clone()));
        assert_eq!(store.obj_parts(obj_id).await, vec![part_id]);
        assert_eq!(store.member_count(part_id).await, 1);
    }

    pub async fn assert_add_obj_to_parts_is_idempotent<S>(store: &S, part_id: PartId, obj_id: ObjId)
    where
        S: PartStore<Sendable> + Sync,
    {
        let payload = serde_json::json!({"phase": "restore"});

        store.upsert_obj(obj_id, &payload).await;
        store.add_obj_to_parts(obj_id, &[part_id]).await;
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
