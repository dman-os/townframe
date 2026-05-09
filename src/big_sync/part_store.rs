use crate::interlude::*;

use big_sync_core::part_store::{ObjPayload, PeerPartCursors};
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

structstruck::strike! {
    struct MemoryPartStore {
        parts: HashMap<
            PartId,
            struct PartDeets {
                #![derive(Default)]
                members: HashSet<ObjId>,
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
        peer_part_cursors: HashMap<(PeerId, PartId), PeerPartCursors>
    }
}

#[async_trait]
impl HostPartitionStore for Arc<std::sync::Mutex<MemoryPartStore>> {
    async fn member_count(&self, part_id: PartId) -> Res<u64> {
        let lock = self.lock().expect(ERROR_MUTEX);
        Ok(lock
            .parts
            .get(&part_id)
            .map(|part| part.members.len() as u64)
            .unwrap_or(0))
    }
    async fn obj_payload(&self, obj_id: ObjId) -> Res<Option<ObjPayload>> {
        let lock = self.lock().expect(ERROR_MUTEX);
        Ok(lock.objs.get(&obj_id).and_then(|obj| obj.payload.clone()))
    }

    async fn upsert_obj(&self, obj_id: ObjId, payload: ObjPayload, parts: Vec<PartId>) -> Res<()> {
        let mut lock = self.lock().expect(ERROR_MUTEX);
        let obj = lock.objs.entry(obj_id).or_default();
        obj.payload = Some(payload);
        obj.parts.extend(parts);
        Ok(())
    }

    async fn obj_parts(&self, obj_id: ObjId) -> Res<Vec<PartId>> {
        let lock = self.lock().expect(ERROR_MUTEX);

        Ok(lock
            .objs
            .get(&obj_id)
            .map(|deets| deets.parts.iter().copied().collect())
            .unwrap_or_default())
    }

    async fn add_obj_to_parts(&self, obj_id: ObjId, parts: Vec<PartId>) -> Res<()> {
        let mut lock = self.lock().expect(ERROR_MUTEX);
        let obj = lock.objs.entry(obj_id).or_default();
        obj.parts.extend(&parts);
        for part_id in parts {
            let part = lock.parts.entry(part_id).or_default();
            part.members.insert(obj_id);
        }
        Ok(())
    }
    async fn remove_obj_from_part(&self, obj_id: ObjId, part_id: PartId) -> Res<()> {
        let mut lock = self.lock().expect(ERROR_MUTEX);
        let part = lock.parts.entry(part_id).or_default();
        part.members.remove(&obj_id);
        let obj = lock.objs.entry(obj_id).or_default();
        obj.parts.remove(&part_id);
        Ok(())
    }

    async fn get_peer_part_cursor(&self, peer_id: PeerId, part_id: PartId) -> Res<PeerPartCursors> {
        let lock = self.lock().expect(ERROR_MUTEX);

        Ok(lock
            .peer_part_cursors
            .get(&(peer_id, part_id))
            .cloned()
            .unwrap_or_default())
    }

    async fn set_peer_part_cursor(
        &self,
        peer_id: PeerId,
        part_id: PartId,
        cursors: PeerPartCursors,
    ) -> Res<()> {
        let mut lock = self.lock().expect(ERROR_MUTEX);
        lock.peer_part_cursors.insert((peer_id, part_id), cursors);
        Ok(())
    }
}
