//! NOTE: storage errors are not represented here. Implementations should
//! either recover within the methods, panic or tear down the sync machine

use crate::interlude::*;

use crate::rpc::BucketSummary;

/// NOTE: cursors are cross-part and peer global.
pub type CursorIndex = u64;
pub type ObjPayload = serde_json::Value;

/// The count of *relevant* changes a principal is behind on one part, counted
/// against that principal's own cursor.
///
/// Relevance has two sources: member transitions inside the part, and access
/// transitions for this principal on this part (a grant, or a re-stamp). Both are
/// counts over indexed columns, so a principal that has caught up advances past
/// irrelevant churn elsewhere in the scope without re-reading it.
///
/// This is deliberately *not* derived from `big_sync_parts.latest_cursor`: that is a
/// watermark on the global scale, and a difference of global counter values counts
/// churn in every other part as well.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PartDirtyCount {
    /// Member transitions in the part newer than the principal's cursor.
    pub member_changes: u64,
    /// Access transitions for this principal on this part newer than its cursor.
    pub access_changes: u64,
}

impl PartDirtyCount {
    /// The total relevance the principal is behind on.
    #[must_use]
    pub const fn total(&self) -> u64 {
        self.member_changes + self.access_changes
    }
}

pub trait PartStoreReadOnly<K: FutureForm> {
    fn member_count<'a>(&'a self, part_id: PartKey) -> K::Future<'a, u64>;
    fn obj_payload<'a>(&'a self, obj_id: ObjKey) -> K::Future<'a, Option<ObjPayload>>;

    fn obj_parts<'a>(&'a self, obj_id: ObjKey) -> K::Future<'a, Vec<PartKey>>;
    fn get_peer_part_cursor<'a>(
        &'a self,
        peer_id: PeerKey,
        part_id: PartKey,
    ) -> K::Future<'a, CursorIndex>;

    fn get_bucket_summary<'a>(
        &'a self,
        part_id: PartKey,
        id: BuckId,
    ) -> K::Future<'a, BucketSummary>;

    /// The relevance `principal` is behind on `part_id` at `since`.
    ///
    /// `None` is the local principal: the caller that access rows do not gate (the
    /// same reading as `permitted_parts`), so only the member half is meaningful
    /// for it.
    fn part_dirty_count<'a>(
        &'a self,
        part_id: PartKey,
        principal: Option<PeerKey>,
        since: CursorIndex,
    ) -> K::Future<'a, PartDirtyCount>;
}
pub trait PartStore<K: FutureForm>: PartStoreReadOnly<K> {
    fn upsert_obj<'a>(&'a self, obj_id: ObjKey, payload: &ObjPayload) -> K::Future<'a, ()>;

    fn add_obj_to_parts<'a>(&'a self, obj_id: ObjKey, parts: &[PartKey]) -> K::Future<'a, ()>;
    fn remove_obj_from_part<'a>(&'a self, obj_id: ObjKey, part_id: PartKey) -> K::Future<'a, ()>;

    fn set_peer_part_cursor<'a>(
        &'a self,
        peer_id: PeerKey,
        part_id: PartKey,
        cursor: CursorIndex,
    ) -> K::Future<'a, ()>;
}

#[cfg(any(test, feature = "test-support"))]
pub mod contract {
    use super::*;
    use future_form::Sendable;

    pub async fn assert_membership_semantics<S>(store: &S, part_id: PartKey, obj_id: ObjKey)
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
        assert_eq!(store.obj_parts(obj_id).await, Vec::<PartKey>::new());
        assert_eq!(store.member_count(part_id).await, 0);

        store.upsert_obj(obj_id, &payload_c).await;
        store.add_obj_to_parts(obj_id, &[part_id]).await;
        assert_eq!(store.obj_payload(obj_id).await, Some(payload_c.clone()));
        assert_eq!(store.obj_parts(obj_id).await, vec![part_id]);
        assert_eq!(store.member_count(part_id).await, 1);
    }

    pub async fn assert_add_obj_to_parts_is_idempotent<S>(store: &S, part_id: PartKey, obj_id: ObjKey)
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

    pub async fn assert_peer_cursor_roundtrip<S>(store: &S, peer_id: PeerKey, part_id: PartKey)
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
