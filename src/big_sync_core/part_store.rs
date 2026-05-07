//! NOTE: storage errors are not represented here. Implementations should
//! either recover within the methods, panic or tear down the sync machine

use crate::interlude::*;

pub type CursorIndex = u64;
pub type ObjPayload = serde_json::Value;

#[derive(Debug, Serialize, Deserialize)]
pub struct PeerPartCursors {
    pub member_cursor: CursorIndex,
    pub obj_cursor: CursorIndex,
}

pub trait PartitionStore<K: FutureForm> {
    fn member_count<'a>(&'a self, part_id: PartId) -> K::Future<'a, u64>;
    fn obj_payload<'a>(&'a self, obj_id: ObjId) -> K::Future<'a, Option<ObjPayload>>;

    fn upsert_obj<'a>(
        &'a self,
        obj_id: ObjId,
        payload: &ObjPayload,
        parts: &[PartId],
    ) -> K::Future<'a, ()>;

    fn obj_parts<'a>(&'a self, obj_id: ObjId) -> K::Future<'a, Vec<PartId>>;

    fn remove_obj_from_part<'a>(&'a self, obj_id: ObjId, part_id: PartId) -> K::Future<'a, ()>;

    fn get_peer_part_cursor<'a>(
        &'a self,
        peer_id: PeerId,
        part_id: PartId,
    ) -> K::Future<'a, PeerPartCursors>;

    fn set_peer_part_cursor<'a>(
        &'a self,
        peer_id: PeerId,
        part_id: PartId,
        cursors: PeerPartCursors,
    ) -> K::Future<'a, ()>;
}
