use super::*;

pub type PartObservations = std::collections::BTreeMap<PartKey, CursorIndex>;

pub(super) fn merge_part_observations(
    observations: &mut PartObservations,
    incoming: impl IntoIterator<Item = (PartKey, CursorIndex)>,
) {
    for (part_id, revision) in incoming {
        observations
            .entry(part_id)
            .and_modify(|current| *current = (*current).max(revision))
            .or_insert(revision);
    }
}

structstruck::strike! {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum SyncCompletionDeets {
        AddedMember,
        RemovedMember,
        ChangedObject,
        Noop,
    }
}

structstruck::strike! {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct SyncTaskCompletion {
        pub obj_id: ObjKey,
        pub deets: SyncCompletionDeets,
    }
}

structstruck::strike! {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct SyncJobEvt {
        pub obj_id: ObjKey,
        pub cursors: Set<CursorIndex>,
        pub deets: SyncCompletionDeets,
    }
}

impl PeerState {
    pub(super) fn sync_from_cursor(
        &mut self,
        tasks: &mut Scheduler<TaskSeed>,
        stat_machine: &mut SyncStatMachine,
        peer_id: PeerKey,
        changed: ObjChanged,
    ) {
        let ObjChanged {
            obj_id,
            payload: remote_payload,
            part_ids: parts,
            cursor,
        } = changed;
        tracing::trace!(
            peer_id = %peer_id,
            ?obj_id,
            ?cursor,
            part_count = parts.len(),
            payload = !remote_payload.is_null(),
            "machine SyncObj command",
        );
        // A null payload means "no advertised payload" (the store
        // strips payloads from advance notices a peer lost access
        // to), never an object payload: keep it absent so the sync
        // task fetches instead of decoding it as content.
        let remote_payload = (!remote_payload.is_null()).then_some(remote_payload);
        let (cursors, part_observations, remote_payload) =
            if let Some(mut worker) = self.sync_workers.remove(&obj_id) {
                let _state = tasks.cancel(worker.task_id).expect(ERROR_UNRECONIZED);
                merge_part_observations(
                    &mut worker.part_observations,
                    parts.iter().cloned().map(|part| (part, cursor)),
                );
                worker.cursors.insert(cursor);
                (
                    worker.cursors,
                    worker.part_observations,
                    remote_payload.or(worker.remote_payload),
                )
            } else {
                (
                    [cursor].into(),
                    parts.iter().cloned().map(|part| (part, cursor)).collect(),
                    remote_payload,
                )
            };
        // Cancel any in-flight removal for the hinted parts. If one
        // was cancelled, the Sync task is deferred until the removal
        // task's completion event lands: the zombie task may still
        // evict the re-added parts, so the re-sync must not race it.
        let mut removal_cancelled = false;
        for part_id in part_observations.keys() {
            if BigSyncMachine::cancel_obj_removal_hint(
                tasks,
                &mut self.remove_workers,
                &mut self.pending_removals,
                obj_id.clone(),
                part_id.clone(),
            ) {
                removal_cancelled = true;
            }
        }
        if removal_cancelled {
            let pending = self
                .pending_removals
                .entry(obj_id)
                .or_insert_with(|| PendingRemoval {
                    remaining_hints: default(),
                    re_added_parts: default(),
                    cursors: default(),
                    remote_payload: None,
                });
            pending.cursors.extend(cursors.iter().copied());
            merge_part_observations(&mut pending.re_added_parts, part_observations.clone());
            if remote_payload.is_some() {
                pending.remote_payload = remote_payload;
            }
            // The Sync task is issued by `resume_pending_removal`
            // once the cancelled removal task's completion lands.
            return;
        }
        let deets = SyncTaskDeets {
            peer_id: peer_id.clone(),
            obj_id: obj_id.clone(),
            remote_payload: remote_payload.clone(),
        };
        let task_id = tasks.spawn(
            std::time::Instant::now(),
            TaskSeed::Sync(SyncTaskSeed {
                kind: SyncTaskKind::Sync,
                part_observations: part_observations.clone(),
                deets: deets.clone(),
            }),
        );
        self.sync_workers.insert(
            obj_id,
            SyncWorkerState {
                task_id,
                cursors,
                part_observations,
                remote_payload,
            },
        );
        for part_id in parts {
            stat_machine.mark_peer_part_cursor_active(peer_id.clone(), part_id);
        }
    }

    pub(super) fn sync_from_bucket(
        &mut self,
        tasks: &mut Scheduler<TaskSeed>,
        peer_id: PeerKey,
        obj_id: ObjKey,
        part_id: PartKey,
        source_revision: CursorIndex,
        remote_payload: Option<ObjPayload>,
    ) {
        let (cursors, part_observations, remote_payload) = if let Some(mut worker) =
            self.sync_workers.remove(&obj_id)
        {
            let _state = tasks.cancel(worker.task_id).expect(ERROR_UNRECONIZED);
            merge_part_observations(&mut worker.part_observations, [(part_id, source_revision)]);
            (
                worker.cursors,
                worker.part_observations,
                remote_payload.or(worker.remote_payload),
            )
        } else {
            (
                default(),
                [(part_id, source_revision)].into(),
                remote_payload,
            )
        };
        // Cancel any in-flight removal for the hinted parts. If
        // one was cancelled, the Sync task is deferred until the
        // removal task's completion event lands: the zombie task
        // may still evict the re-added parts, so the re-sync must
        // not race it.
        let mut removal_cancelled = false;
        for part_id in part_observations.keys() {
            if BigSyncMachine::cancel_obj_removal_hint(
                tasks,
                &mut self.remove_workers,
                &mut self.pending_removals,
                obj_id.clone(),
                part_id.clone(),
            ) {
                removal_cancelled = true;
            }
        }
        if removal_cancelled {
            let pending = self
                .pending_removals
                .entry(obj_id)
                .or_insert_with(|| PendingRemoval {
                    remaining_hints: default(),
                    re_added_parts: default(),
                    cursors: default(),
                    remote_payload: None,
                });
            pending.cursors.extend(cursors.iter().copied());
            merge_part_observations(&mut pending.re_added_parts, part_observations.clone());
            if remote_payload.is_some() {
                pending.remote_payload = remote_payload;
            }
            // The Sync task is issued by `resume_pending_removal`
            // once the cancelled removal task's completion lands.
            return;
        }
        let deets = SyncTaskDeets {
            peer_id,
            obj_id: obj_id.clone(),
            remote_payload: remote_payload.clone(),
        };
        let task_id = tasks.spawn(
            std::time::Instant::now(),
            TaskSeed::Sync(SyncTaskSeed {
                kind: SyncTaskKind::Sync,
                part_observations: part_observations.clone(),
                deets: deets.clone(),
            }),
        );
        self.sync_workers.insert(
            obj_id,
            SyncWorkerState {
                task_id,
                cursors,
                part_observations,
                remote_payload,
            },
        );
    }
}
