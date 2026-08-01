use std::collections::HashMap;

use keyhive_core::access::Access;

use big_sync_core::{ObjId, PartId, PeerId};

/// Decides whether a subscriber may receive events for an object.
///
/// The store owns the subscription bus and the *persistence* of the member
/// map (the `big_sync_syncable` SQL table, used to rehydrate the policy on
/// boot). The policy owns the decision and the authoritative in-memory map;
/// stores consult it at event-forward time and forward member mutations to
/// it via the write-through `set_obj_members`/`add_obj_member`/
/// `remove_obj_member` hooks.
pub trait ObjAccessPolicy: Send + Sync {
    /// Whether `principal` may receive events for `obj_id`.
    ///
    /// - `principal == None` (trusted local subscriber): always permitted.
    /// - `part_id` is the part the event belongs to when known; obj-level
    ///   events such as `ObjectChanged` pass `None`.
    fn is_event_permitted(
        &self,
        part_id: Option<PartId>,
        obj_id: ObjId,
        principal: Option<PeerId>,
    ) -> bool;

    /// Replace the member set for `obj`.
    fn set_obj_members(&self, obj: ObjId, agents: HashMap<PeerId, Access>);

    /// Add a single member to `obj`.
    fn add_obj_member(&self, obj: ObjId, member: PeerId, access: Access);

    /// Remove a single member from `obj`.
    fn remove_obj_member(&self, obj: ObjId, member: PeerId);
}

/// Content-addressed policy: possession of the object hash is authorization.
///
/// Any principal with an active subscription may receive the object's
/// events; member mutations are no-ops. This is the policy for parts whose
/// data is self-authenticating (e.g. blob partitions), not a default.
#[derive(Debug, Default, Clone, Copy)]
pub struct AllowAllPolicy;

impl ObjAccessPolicy for AllowAllPolicy {
    fn is_event_permitted(
        &self,
        _part_id: Option<PartId>,
        _obj_id: ObjId,
        _principal: Option<PeerId>,
    ) -> bool {
        true
    }
    fn set_obj_members(&self, _obj: ObjId, _agents: HashMap<PeerId, Access>) {}
    fn add_obj_member(&self, _obj: ObjId, _member: PeerId, _access: Access) {}
    fn remove_obj_member(&self, _obj: ObjId, _member: PeerId) {}
}

/// Deterministic membership policy used by the host-store contract tests.
/// Production callers should provide their Keyhive-backed policy instead.
#[cfg(any(test, feature = "test-support"))]
#[derive(Default)]
pub struct MembershipPolicy {
    members: std::sync::RwLock<HashMap<ObjId, HashMap<PeerId, Access>>>,
}

#[cfg(any(test, feature = "test-support"))]
impl ObjAccessPolicy for MembershipPolicy {
    fn is_event_permitted(
        &self,
        _part_id: Option<PartId>,
        obj_id: ObjId,
        principal: Option<PeerId>,
    ) -> bool {
        let Some(principal) = principal else {
            return true;
        };
        self.members
            .read()
            .expect("membership policy lock poisoned")
            .get(&obj_id)
            .and_then(|members| members.get(&principal))
            .is_some_and(|access| access.is_fetcher())
    }

    fn set_obj_members(&self, obj: ObjId, agents: HashMap<PeerId, Access>) {
        self.members
            .write()
            .expect("membership policy lock poisoned")
            .insert(obj, agents);
    }

    fn add_obj_member(&self, obj: ObjId, member: PeerId, access: Access) {
        self.members
            .write()
            .expect("membership policy lock poisoned")
            .entry(obj)
            .or_default()
            .insert(member, access);
    }

    fn remove_obj_member(&self, obj: ObjId, member: PeerId) {
        if let Some(members) = self
            .members
            .write()
            .expect("membership policy lock poisoned")
            .get_mut(&obj)
        {
            members.remove(&member);
        }
    }
}
