use super::*;

#[test]
fn replay_lane_keeps_objects_live_and_parts_advisory() {
    let mut machine = BigSyncMachine::default();
    let peer = PeerKey::random();
    let part = PartKey::random();
    let object = ObjKey::random();
    machine.handle_evt(BigSyncEvent::SetPeer(SetPeerEvent {
        peer_id: peer.clone(),
        parts: [part.clone()].into(),
        objects: [object.clone()].into(),
    }));
    machine
        .peers
        .get_mut(&peer)
        .expect(ERROR_UNRECONIZED)
        .replay_pages
        .insert(
            ReplayRoute::Part(part.clone()),
            ReplayPageState {
                task_id: 0,
                request_id: crate::rpc::ReplayRequestId(0),
                lane: ReplayLane::Bulk,
                caught_up: false,
                waiting_for_credit: false,
            },
        );

    assert_eq!(
        machine.replay_lane(
            &peer,
            &SubscriptionTarget::Part {
                part_id: part,
                cursor: 0,
            },
        ),
        ReplayLane::Bulk,
    );
    assert_eq!(
        machine.replay_lane(
            &peer,
            &SubscriptionTarget::Object {
                obj_id: object,
                cursor: 0,
            },
        ),
        ReplayLane::Live,
    );
}
