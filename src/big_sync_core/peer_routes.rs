use super::*;
use crate::scheduler::Scheduler;
use crate::tasks::SyncTaskDeets;
use crate::tasks::SyncTaskKind;
use crate::tasks::SyncTaskSeed;
use crate::tasks::TaskSeed;

/// Takes the parts whose strategy decision is still pending out of `peer_state`,
/// skipping parts the route change removed, and returns them with the decision
/// tasks the caller must cancel before re-deciding them.
pub(crate) fn take_pending_decisions(
    peer_state: &mut PeerState,
    removed_parts: &Set<PartKey>,
) -> (Set<PartKey>, Set<TaskId>) {
    let pending: Vec<_> = peer_state
        .parts
        .iter()
        .filter(|(part_id, state)| {
            !removed_parts.contains(*part_id) && matches!(state.strat, PeerPartStrategy::Pending(_))
        })
        .map(|(part_id, _)| part_id.clone())
        .collect();
    let mut pending_parts = Set::new();
    let mut pending_tasks = Set::new();
    for part_id in pending {
        if let Some(PeerPartState {
            strat: PeerPartStrategy::Pending(task_id),
        }) = peer_state.parts.remove(&part_id)
        {
            pending_parts.insert(part_id);
            pending_tasks.insert(task_id);
        }
    }
    (pending_parts, pending_tasks)
}

/// Tears down every removed part: bucket strategy tasks are cancelled and the
/// cursor machine forgets the part. A pending decision task is left alone: it may
/// be shared with surviving parts, and its completion ignores parts that are gone.
pub(crate) fn teardown_removed_parts(
    peer_state: &mut PeerState,
    tasks: &mut Scheduler<TaskSeed>,
    removed_parts: &Set<PartKey>,
) {
    for part_id in removed_parts.iter() {
        let Some(state) = peer_state.parts.remove(part_id) else {
            continue;
        };
        match state.strat {
            PeerPartStrategy::Pending(_) | PeerPartStrategy::Cursor(_) => {}
            PeerPartStrategy::Bucket(strat) => {
                for task_id in strat
                    .active_leaf_tasks
                    .into_keys()
                    .chain(strat.active_list_tasks.into_keys())
                {
                    let _state = tasks.cancel(task_id).expect(ERROR_UNRECONIZED);
                }
            }
        }
    }
    for part_id in removed_parts {
        peer_state.cursor_machine.remove_part(part_id.clone());
    }
}

/// Restarts sync workers whose queued task snapshot names a removed part. A queued
/// sync task owns a snapshot of its hints, so mutating only SyncWorkerState would
/// leave the queued task stale: every affected task is stopped and replaced with a
/// fresh snapshot. Object-routed tasks remain valid with no part hints; part-routed
/// tasks with no surviving hints are simply obsolete.
pub(crate) fn restart_hinted_sync_workers(
    peer_state: &mut PeerState,
    tasks: &mut Scheduler<TaskSeed>,
    peer_id: &PeerKey,
    removed_parts: &Set<PartKey>,
) {
    let affected_sync_workers: Vec<_> = peer_state
        .sync_workers
        .iter()
        .filter(|(_, worker)| !worker.part_hints.is_disjoint(removed_parts))
        .map(|(obj_id, _)| obj_id.clone())
        .collect();
    for obj_id in affected_sync_workers {
        let mut worker = peer_state
            .sync_workers
            .remove(&obj_id)
            .expect(ERROR_IMPOSSIBLE);
        let _state = tasks.cancel(worker.task_id).expect(ERROR_UNRECONIZED);
        worker
            .part_hints
            .retain(|part_id| !removed_parts.contains(part_id));
        if worker.part_hints.is_empty() && !peer_state.objects.contains(&obj_id) {
            // The task is dropped here and nothing will complete it, so the
            // replay it owed is abandoned: forget the claim instead of keeping
            // a cursor slot no completion can settle.
            peer_state.cursor_machine.abandon_obj_sync(&obj_id);
            continue;
        }
        worker.task_id = tasks.spawn(
            std::time::Instant::now(),
            TaskSeed::Sync(SyncTaskSeed {
                kind: SyncTaskKind::Sync,
                part_hints: worker.part_hints.clone(),
                deets: SyncTaskDeets {
                    peer_id: peer_id.clone(),
                    obj_id: obj_id.clone(),
                    remote_payload: worker.remote_payload.clone(),
                },
            }),
        );
        let old = peer_state.sync_workers.insert(obj_id, worker);
        assert!(old.is_none(), "fishy");
    }
}

#[cfg(test)]
mod tests {
    use crate::*;

    /// An incremental update acts on exactly the routes it names: a removed part is
    /// torn down, an added part gets its strategy decision, and a surviving part's
    /// pending decision is not restarted by an update that never mentioned it.
    #[test]
    fn update_peer_applies_only_the_named_delta() {
        let mut machine = BigSyncMachine::default();
        let peer = PeerKey::random();
        let part_a = PartKey::random();
        let part_b = PartKey::random();
        let part_c = PartKey::random();
        let object = ObjKey::random();
        machine.handle_evt(BigSyncEvent::SetPeer(SetPeerEvent {
            peer_id: peer.clone(),
            parts: [part_a.clone(), part_b.clone()].into(),
            objects: Set::new(),
        }));
        let pending_of = |machine: &BigSyncMachine, part: &PartKey| match &machine.peers[&peer]
            .parts[part]
            .strat
        {
            PeerPartStrategy::Pending(task_id) => *task_id,
            _ => panic!("the part must be pending a decision"),
        };
        let part_b_decision = pending_of(&machine, &part_b);

        machine.handle_evt(BigSyncEvent::UpdatePeer(UpdatePeerEvent {
            peer_id: peer.clone(),
            added_parts: [part_c.clone()].into(),
            removed_parts: [part_a.clone()].into(),
            added_objects: [object.clone()].into(),
            removed_objects: Set::new(),
        }));

        let peer_state = &machine.peers[&peer];
        assert!(
            !peer_state.parts.contains_key(&part_a),
            "a removed part is torn down"
        );
        assert_eq!(
            pending_of(&machine, &part_b),
            part_b_decision,
            "a part the delta did not name keeps its pending decision"
        );
        assert!(
            peer_state.parts.contains_key(&part_c),
            "an added part is being decided"
        );
        assert!(
            peer_state.objects.contains(&object),
            "an added object is followed"
        );

        machine.handle_evt(BigSyncEvent::UpdatePeer(UpdatePeerEvent {
            peer_id: peer.clone(),
            added_parts: Set::new(),
            removed_parts: Set::new(),
            added_objects: Set::new(),
            removed_objects: [object.clone()].into(),
        }));
        assert!(
            !machine.peers[&peer].objects.contains(&object),
            "a removed object is no longer followed"
        );
    }
}
