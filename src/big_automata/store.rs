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
}

pub trait PartStore<K: FutureForm>: PartStoreReadOnly<K> {
    fn set_peer_part_cursor<'a>(
        &'a self,
        peer_key: PeerKey,
        part_key: PartKey,
        cursor: CursorIndex,
    ) -> K::Future<'a, ()>;
}
