use std::collections::HashMap;
use std::sync::RwLock;

use big_sync::ObjAccessPolicy;
use big_sync_core::{ObjId, PartId, PeerId};
use keyhive_core::access::Access;

/// Keyhive-governed access policy for big_repo's part store.
///
/// The member map (obj → peer → access) is derived from the keyhive event
/// log by the group-part reconciliation worker and rehydrated from the
/// `big_sync_syncable` table at boot. Missing membership is fail-closed:
/// remote principals that keyhive has not granted fetch access are denied.
/// Trusted local subscribers (`principal == None`) bypass the check.
#[derive(Default)]
pub struct KeyhiveMembershipPolicy {
    members: RwLock<HashMap<ObjId, HashMap<PeerId, Access>>>,
}

impl KeyhiveMembershipPolicy {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ObjAccessPolicy for KeyhiveMembershipPolicy {
    fn is_event_permitted(
        &self,
        part_id: Option<PartId>,
        obj_id: ObjId,
        principal: Option<PeerId>,
    ) -> bool {
        match principal {
            None => true,
            Some(peer) => self
                .members
                .read()
                .expect(ERROR_POLICY_LOCK)
                .get(&obj_id)
                .and_then(|members| members.get(&peer))
                .is_some_and(|access| {
                    if part_id.is_some() {
                        access.is_reader()
                    } else {
                        access.is_fetcher()
                    }
                }),
        }
    }

    fn set_obj_members(&self, obj: ObjId, agents: HashMap<PeerId, Access>) {
        self.members
            .write()
            .expect(ERROR_POLICY_LOCK)
            .insert(obj, agents);
    }

    fn add_obj_member(&self, obj: ObjId, member: PeerId, access: Access) {
        self.members
            .write()
            .expect(ERROR_POLICY_LOCK)
            .entry(obj)
            .or_default()
            .insert(member, access);
    }

    fn remove_obj_member(&self, obj: ObjId, member: PeerId) {
        self.members
            .write()
            .expect(ERROR_POLICY_LOCK)
            .entry(obj)
            .or_default()
            .remove(&member);
    }
}

const ERROR_POLICY_LOCK: &str = "keyhive membership policy lock poisoned";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relay_access_allows_direct_fetch_without_partition_discovery() {
        let policy = KeyhiveMembershipPolicy::new();
        let object = ObjId::new([1; 32]);
        let relay = PeerId::new([2; 32]);
        policy.add_obj_member(object, relay, Access::Relay);

        assert!(policy.is_event_permitted(None, object, Some(relay)));
        assert!(!policy.is_event_permitted(
            Some(PartId::new([3; 32])),
            object,
            Some(relay),
        ));
    }

    #[test]
    fn reader_access_allows_partition_discovery() {
        let policy = KeyhiveMembershipPolicy::new();
        let object = ObjId::new([4; 32]);
        let reader = PeerId::new([5; 32]);
        policy.add_obj_member(object, reader, Access::Read);

        assert!(policy.is_event_permitted(
            Some(PartId::new([6; 32])),
            object,
            Some(reader),
        ));
    }
}
