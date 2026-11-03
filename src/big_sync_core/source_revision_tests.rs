use super::*;

#[test]
fn source_revisions_survive_sync_coalescing_retries_and_route_replacement() {
    let mut machine = BigSyncMachine::default();
    let peer = PeerKey::random();
    let other_peer = PeerKey::random();
    let part_a = PartKey::random();
    let part_b = PartKey::random();
    let obj = ObjKey::random();
    for peer_id in [&peer, &other_peer] {
        machine.handle_evt(BigSyncEvent::SetPeer(SetPeerEvent {
            peer_id: peer_id.clone(),
            parts: [part_a.clone(), part_b.clone()].into(),
            objects: [obj.clone()].into(),
        }));
    }
    feed_part_changed(&mut machine, &peer, 10, &part_a, &obj);
    feed_part_changed(&mut machine, &peer, 30, &part_b, &obj);
    feed_part_changed(&mut machine, &peer, 20, &part_a, &obj);
    feed_part_changed(&mut machine, &other_peer, 3, &part_a, &obj);
    let state = machine.peers.get_mut(&peer).unwrap();
    state.bucket_cmd_buf.push(BucketMachineCommand::SyncObj {
        obj_id: obj.clone(),
        part_id: part_a.clone(),
        source_revision: 5,
        remote_payload: None,
    });
    machine.drain_bucket_machine_cmds(peer.clone());
    let state = machine.peers.get_mut(&peer).unwrap();
    state.cursors_cmd_buf.push(CursorMachineCommand::SyncObj {
        obj_id: obj.clone(),
        parts: Vec::new(),
        cursor: 90,
        remote_payload: serde_json::Value::Null,
    });
    machine.drain_cursor_machine_cmds(peer.clone());
    let expected: PartObservations = [(part_a.clone(), 20), (part_b.clone(), 30)].into();
    let tasks: Vec<_> = machine.drain_sync_spawn_queue().collect();
    assert_eq!(tasks.len(), 2);
    let task = tasks
        .iter()
        .find(|task| task.deets.peer_id == peer)
        .unwrap();
    assert_eq!(task.part_observations, expected);
    assert_eq!(
        machine.peers[&peer].sync_workers[&obj].cursors,
        [10, 20, 30, 90].into()
    );
    assert_eq!(
        tasks
            .iter()
            .find(|task| task.deets.peer_id == other_peer)
            .unwrap()
            .part_observations,
        [(part_a.clone(), 3)].into()
    );
    machine.handle_evt(BigSyncEvent::SyncFailed(SyncFailedEvent {
        task_id: task.id,
        peer_id: peer.clone(),
        obj_id: obj.clone(),
        err: eyre::eyre!("retry observation"),
    }));
    machine
        .tasks
        .tick(std::time::Instant::now() + Duration::from_secs(3));
    let retry = machine.drain_sync_spawn_queue().next().unwrap();
    assert_eq!(retry.part_observations, expected);
    machine.handle_evt(BigSyncEvent::SyncStale(SyncStaleEvent {
        task_id: retry.id,
        peer_id: peer.clone(),
        obj_id: obj.clone(),
    }));
    machine
        .tasks
        .tick(std::time::Instant::now() + Duration::from_secs(10));
    let retry = machine.drain_sync_spawn_queue().next().unwrap();
    assert_eq!(retry.part_observations, expected);
    machine.handle_evt(BigSyncEvent::UpdatePeer(UpdatePeerEvent {
        peer_id: peer.clone(),
        removed_parts: [part_b].into(),
        added_parts: Set::new(),
        added_objects: Set::new(),
        removed_objects: Set::new(),
    }));
    let replaced = machine.drain_sync_spawn_queue().next().unwrap();
    assert_eq!(replaced.part_observations, [(part_a.clone(), 20)].into());
    machine.handle_evt(BigSyncEvent::UpdatePeer(UpdatePeerEvent {
        peer_id: peer,
        removed_parts: [part_a].into(),
        added_parts: Set::new(),
        added_objects: Set::new(),
        removed_objects: Set::new(),
    }));
    assert!(
        machine
            .drain_sync_spawn_queue()
            .next()
            .unwrap()
            .part_observations
            .is_empty()
    );
}

#[test]
fn source_revisions_survive_removal_coalescing_and_retry() {
    let mut machine = BigSyncMachine::default();
    let peer = PeerKey::random();
    let obj = ObjKey::random();
    let part_a = PartKey::random();
    let part_b = PartKey::random();
    machine.handle_evt(BigSyncEvent::SetPeer(SetPeerEvent {
        peer_id: peer.clone(),
        parts: [part_a.clone(), part_b.clone()].into(),
        objects: Set::new(),
    }));
    for (part, cursor) in [
        (part_a.clone(), 11),
        (part_b.clone(), 29),
        (part_a.clone(), 19),
    ] {
        let state = machine.peers.get_mut(&peer).unwrap();
        state
            .cursors_cmd_buf
            .push(CursorMachineCommand::RemoveObjFromParts {
                obj_id: obj.clone(),
                part_id: part,
                cursor,
            });
        machine.drain_cursor_machine_cmds(peer.clone());
    }
    let state = machine.peers.get_mut(&peer).unwrap();
    state
        .bucket_cmd_buf
        .push(BucketMachineCommand::RemoveObjFromParts {
            obj_id: obj.clone(),
            part_id: part_a.clone(),
            source_revision: 7,
        });
    machine.drain_bucket_machine_cmds(peer.clone());
    let task = machine.drain_sync_spawn_queue().next().unwrap();
    let expected: PartObservations = [(part_a, 19), (part_b, 29)].into();
    assert_eq!(task.part_observations, expected);
    assert_eq!(task.deets.peer_id, peer);
    assert_eq!(
        machine.peers[&peer].remove_workers[&obj].cursors,
        [11, 19, 29].into()
    );
    machine.handle_evt(BigSyncEvent::RemoveFailed(RemoveFailedEvent {
        task_id: task.id,
        peer_id: peer,
        obj_id: obj,
        err: eyre::eyre!("retry observation"),
    }));
    machine
        .tasks
        .tick(std::time::Instant::now() + Duration::from_secs(3));
    let retry = machine.drain_sync_spawn_queue().next().unwrap();
    assert_eq!(retry.kind, SyncTaskKind::RemoveFromParts);
    assert_eq!(retry.part_observations, expected);
}

/// Feed one object-changed fact into a part's cursor stream and let the machine
/// handle the commands it emits (the sync that owes the new cursor).
pub(super) fn feed_part_changed(
    machine: &mut BigSyncMachine,
    peer: &PeerKey,
    cursor: CursorIndex,
    part_id: &PartKey,
    obj_id: &ObjKey,
) {
    {
        let peer_state = machine.peers.get_mut(peer).expect(ERROR_UNRECONIZED);
        peer_state.cursor_machine.on_subscription_evt(
            crate::rpc::PartEvent::Changed(crate::rpc::ObjChanged {
                cursor,
                part_ids: vec![part_id.clone()],
                obj_id: obj_id.clone(),
                payload: serde_json::json!({"head": cursor}),
            }),
            &mut peer_state.cursors_cmd_buf,
        );
    }
    machine.drain_cursor_machine_cmds(peer.clone());
}
