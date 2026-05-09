use crate::interlude::*;

use big_sync_core::part_store::{CursorIndex, ObjPayload, PeerPartCursors};
use big_sync_core::{ObjId, PartId, PeerId};

#[async_trait]
pub trait HostPartitionStore: Send + Sync {
    async fn member_count(&self, part_id: PartId) -> Res<u64>;
    async fn obj_payload(&self, obj_id: ObjId) -> Res<Option<ObjPayload>>;

    async fn upsert_obj(&self, obj_id: ObjId, payload: ObjPayload, parts: Vec<PartId>) -> Res<()>;

    async fn obj_parts(&self, obj_id: ObjId) -> Res<Vec<PartId>>;

    async fn add_obj_to_parts(&self, obj_id: ObjId, parts: Vec<PartId>) -> Res<()>;
    async fn remove_obj_from_part(&self, obj_id: ObjId, part_id: PartId) -> Res<()>;

    async fn get_peer_part_cursor(&self, peer_id: PeerId, part_id: PartId) -> Res<PeerPartCursors>;

    async fn set_peer_part_cursor(
        &self,
        peer_id: PeerId,
        part_id: PartId,
        cursors: PeerPartCursors,
    ) -> Res<()>;
}

struct MemoryPartStore {}

#[async_trait]
impl HostPartitionStore for MemoryPartStore {
    async fn member_count(&self, part_id: PartId) -> Res<u64> {
        todo!()
    }
    async fn obj_payload(&self, obj_id: ObjId) -> Res<Option<ObjPayload>> {
        todo!()
    }

    async fn upsert_obj(&self, obj_id: ObjId, payload: ObjPayload, parts: Vec<PartId>) -> Res<()> {
        todo!()
    }

    async fn obj_parts(&self, obj_id: ObjId) -> Res<Vec<PartId>> {
        todo!()
    }

    async fn add_obj_to_parts(&self, obj_id: ObjId, parts: Vec<PartId>) -> Res<()> {
        todo!()
    }
    async fn remove_obj_from_part(&self, obj_id: ObjId, part_id: PartId) -> Res<()> {
        todo!()
    }

    async fn get_peer_part_cursor(&self, peer_id: PeerId, part_id: PartId) -> Res<PeerPartCursors> {
        todo!()
    }

    async fn set_peer_part_cursor(
        &self,
        peer_id: PeerId,
        part_id: PartId,
        cursors: PeerPartCursors,
    ) -> Res<()> {
        todo!()
    }
}
