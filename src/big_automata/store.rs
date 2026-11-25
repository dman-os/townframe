use crate::interlude::*;

pub mod bucket;

pub type CursorIndex = u64;
pub type ObjPayload = serde_json::Value;

pub trait PartStoreReadOnly<K: FutureForm> {
    fn get_peer_part_cursor<'a>(
        &'a self,
        peer_key: PeerKey,
        part_key: PartKey,
    ) -> K::Future<'a, Option<CursorIndex>>;
    // FIXME: per-entry reads; the bucket review wants batched `bucket_summaries` and
    // `obj_states` (digest + membership) instead.
    fn get_bucket_summary<'a>(
        &'a self,
        part_key: PartKey,
        id: BuckId,
    ) -> K::Future<'a, bucket::BucketSummary>;
    fn obj_payload<'a>(&'a self, obj_key: ObjKey) -> K::Future<'a, Option<ObjPayload>>;
    fn obj_parts<'a>(&'a self, obj_key: ObjKey) -> K::Future<'a, Vec<PartKey>>;
}

pub trait PartStore<K: FutureForm>: PartStoreReadOnly<K> {
    fn set_peer_part_cursor<'a>(
        &'a self,
        peer_key: PeerKey,
        part_key: PartKey,
        cursor: CursorIndex,
    ) -> K::Future<'a, ()>;
}
