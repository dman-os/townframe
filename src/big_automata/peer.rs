use crate::tasks::{MachineTaskDeets, decide_peer_strat::DecidePeerStrategyTask};

use super::*;

strike! {
    #[derive(Default)]
    pub struct PeerState {
        objs: Set<ObjKey>,
        parts: Map<PartKey, struct PeerPartState {
            strat: enum PeerPartStrat {
                Pending(TaskId),
                Cursor(struct Cursor {
                    replay_cursor: CursorIndex
                })
            }
        }>
    }
}

impl BigSyncMachine {
    #[tracing::instrument(skip(self))]
    pub(super) fn handle_update_peer_evt(
        &mut self,
        UpdatePeerEvent {
            peer_key,
            parts,
            objects,
            allowed_strat_override,
        }: UpdatePeerEvent,
    ) {
        self.all_seen_peer.insert(peer_key.clone());
        // mutate any old peer_state in place if found
        let mut peer_state = self.peers.remove(&peer_key).unwrap_or_default();
        let old_parts: Set<_> = peer_state.parts.keys().cloned().collect();

        let removed_parts: Set<_> = old_parts.difference(&parts).cloned().collect();
        let mut pending_parts = Set::new();

        // for all retained parts
        for part_key in old_parts.intersection(&parts) {
            // only remove if pending
            // the other parts keep working as is
            if matches!(
                peer_state.parts.get(part_key),
                Some(PeerPartState {
                    strat: PeerPartStrat::Pending(_)
                })
            ) && let Some(PeerPartState {
                strat: PeerPartStrat::Pending(task_id),
            }) = peer_state.parts.remove(part_key)
            {
                pending_parts.insert(part_key.clone());
                // remove existing decide peer strat tasks
                // FIXME: we could techincally avoid doing this if the incoming set
                // has the same decide needs as the existing one
                self.machine_tasks.stop(task_id);
            }
        }

        for part_key in &removed_parts {
            // remove from cursor machine
            let Some(state) = peer_state.parts.remove(part_key) else {
                continue;
            };
            match state.strat {
                PeerPartStrat::Pending(_) | PeerPartStrat::Cursor(_) => {}
                PeerPartStrat::Bucket(strat) => {
                    for task_id in strat
                        .active_leaf_tasks
                        .into_keys()
                        .chain(strat.active_list_tasks.into_keys())
                    {
                        let _state = self.tasks.cancel(task_id).expect(ERROR_UNRECONIZED);
                    }
                }
            }
        }

        let added_parts: Set<_> = parts.difference(&old_parts).cloned().collect();
        let decide_parts: Set<_> = pending_parts.union(&added_parts).cloned().collect();
        if !decide_parts.is_empty() {
            let deets = MachineTaskDeets::DecidePeerStrategy(DecidePeerStrategyTask {
                peer_key: peer_key.clone(),
                parts: decide_parts.clone(),
                allowed_strat_override: allowed_strat_override.clone(),
                default_allowed_strat: self.default_allowed_strat,
            });
            let decide_task = self.machine_tasks.queue((self.clock)(), deets);
            for part_key in decide_parts {
                peer_state.parts.insert(
                    part_key.clone(),
                    PeerPartState {
                        strat: PeerPartStrat::Pending(decide_task),
                    },
                );
            }
        }
    }

    #[tracing::instrument(skip(self))]
    pub(super) fn handle_remove_peer_evt(&mut self, RemovePeerEvent { peer_key }: RemovePeerEvent) {
        let Some(peer_state) = self.peers.remove(&peer_key) else {
            panic!("unknown peer removed");
        };
        for (part_key, part_state) in peer_state.parts {
            match part_state.strat {
                PeerPartStrat::Pending(task_id) => {}
                PeerPartStrat::Cursor(cursor) => todo!(),
            }
        }
    }
}

#[cfg(test)]
mod test {

    #[test]
    fn adding_and_removing_peer_smoke() {}

    #[test]
    #[should_panic]
    fn removing_peer_twice_panics() {}
}
