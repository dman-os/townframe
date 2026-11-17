use crate::tasks::{MachineTaskDeets, decide_peer_strat::DecidePeerStrategyTask};

use super::*;

structstruck::strike! {
    struct PeerState {
        sync_workers: Map<ObjKey, struct SyncWorkerState {
            task_id: TaskId,
            cursors: Set<CursorIndex>,
            part_hints: Set<PartKey>,
            remote_payload: Option<crate::part_store::ObjPayload>,
        }>,
        /// In-flight backend removal tasks keyed by object, mirroring
        /// `sync_workers` coalescing semantics.
        remove_workers: Map<ObjKey, SyncWorkerState>,
        /// Removals whose in-flight task was cancelled by a re-add. The
        /// re-removal (remaining hints) and re-sync (re-added parts) are
        /// deferred until the cancelled task's completion event lands, so the
        /// zombie task — which may still be mid-removal after a cooperative
        /// cancel — cannot race the new work.
        pending_removals: Map<ObjKey, struct PendingRemoval {
            remaining_hints: Set<PartKey>,
            re_added_parts: Set<PartKey>,
            cursors: Set<CursorIndex>,
            remote_payload: Option<crate::part_store::ObjPayload>,
        }>,
        /// One in-flight page request per replay route. Pages are discrete, so
        /// this is tracked per route rather than as one per-connection worker,
        /// and `caught_up` is what the peer-level replay-done stat aggregates.
        replay_pages: Map<ReplayRoute, struct ReplayPageState {
            task_id: TaskId,
            request_id: crate::rpc::ReplayRequestId,
            lane: ReplayLane,
            caught_up: bool,
            waiting_for_credit: bool,
        }>,
        /// The peer's one logical replay subscription: the target set the responder holds for
        /// this client session, and the changes it has not acknowledged yet. Cursors remain in
        /// the page task and the cursor machine; this state only avoids repeating stable target
        /// metadata on the wire (ADR 012 decision 9).
        replay_subscription: struct ReplaySubscriptionState {
            subscription_id: crate::rpc::ReplaySubscriptionId,
            /// The generation the next `Update` carries. Sending takes the current value and
            /// leaves the next one behind, so a session's first update carries 0 — which is
            /// what opens a subscription the responder does not know.
            generation: u64,
            next_target_id: u32,
            /// Entries the responder acknowledged, and therefore the only routes a page may
            /// name.
            targets: Map<ReplayRoute, crate::rpc::ReplayTargetId>,
            /// Entries sent, or about to be sent, that no answer has acknowledged. A route
            /// stays out of `targets`, and so out of a page, until its entry lands.
            pending_additions: Map<ReplayRoute, struct PendingReplayAddition {
                id: crate::rpc::ReplayTargetId,
                target: crate::rpc::ReplaySubscriptionTarget,
            }>,
            /// Ids the next `Update` removes. An id the responder does not hold is a no-op to
            /// remove, so a re-send or a lost entry is safe to name here.
            pending_removals: Set<crate::rpc::ReplayTargetId>,
            /// The removals the update now in flight carries. They are held apart from
            /// `pending_removals` so a route dropped while the update was in flight is not
            /// lost with them.
            in_flight_removals: Set<crate::rpc::ReplayTargetId>,
            /// Routes the responder refused. A refused route stays wanted, keeps blocking
            /// full sync, and is re-added under a fresh id until an update for it succeeds.
            blocked: Set<ReplayRoute>,
            /// The scheduled task carrying the pending batch while it waits on the retry a
            /// refusal bought it.
            update_task: Option<TaskId>,
        },
        objects: Set<ObjKey>,

        cursor_machine: CursorSyncMachine,

        cursors_cmd_buf: Vec<CursorMachineCommand>,
        bucket_cmd_buf: Vec<BucketMachineCommand>,

        parts: Map<PartKey, struct PeerPartState {
            strat: enum PeerPartStrategy {
                Pending(TaskId),
                Bucket(struct BucketState {
                    replay_cursor: CursorIndex,
                    machine: Box<BucketMachine>,
                    active_list_tasks: Map<TaskId, ListBucketsTask>,
                    active_leaf_tasks: Map<TaskId, LeafBucketsTask>,
                }),
                Cursor(struct CursorState {
                    replay_cursor: CursorIndex,
                }),
            }
        }>
    }
}

// peer support
impl BigSyncMachine {
    /// Applies one peer route delta: added parts get a strategy decision, removed parts
    /// and objects are torn down, and everything the delta does not name is left exactly
    /// as it was. `redecide_pending` is set only by a full `SetPeer` re-assertion, which
    /// restarts strategy decisions still pending for surviving parts; an incremental
    /// update never disturbs a decision it did not name.
    fn handle_peer_route_change(
        &mut self,
        peer_id: PeerKey,
        added_parts: Set<PartKey>,
        removed_parts: Set<PartKey>,
        added_objects: Set<ObjKey>,
        removed_objects: Set<ObjKey>,
        redecide_pending: bool,
    ) {
        // Incrementally update peer state: preserve sync_workers, cursor_machine,
        // and resolved parts while stopping only pending/obsolete strategy tasks.
        self.all_seen_peer.insert(peer_id.clone());
        let mut peer_state = self.peers.remove(&peer_id).unwrap_or_else(|| PeerState {
            remove_workers: default(),
            pending_removals: default(),
            sync_workers: default(),
            replay_pages: default(),
            replay_subscription: ReplaySubscriptionState {
                // One subscription per client session and storage scope, named in every
                // request; the id only has to be unique within the session.
                subscription_id: crate::rpc::ReplaySubscriptionId(1),
                generation: 0,
                // Ids keep counting up across a re-open, so a fresh entry can never collide
                // with an entry of a handle the responder has forgotten.
                next_target_id: 1,
                targets: default(),
                pending_additions: default(),
                pending_removals: default(),
                in_flight_removals: default(),
                blocked: default(),
                update_task: None,
            },
            objects: default(),
            cursor_machine: default(),
            cursors_cmd_buf: default(),
            bucket_cmd_buf: default(),
            parts: default(),
        });
        // The object delta is applied up front so every route check below reads the
        // final object set.
        for obj_id in &removed_objects {
            peer_state.objects.remove(obj_id);
        }
        for obj_id in &added_objects {
            peer_state.objects.insert(obj_id.clone());
        }
        let pending_parts = if redecide_pending {
            let (pending_parts, pending_tasks) =
                take_pending_decisions(&mut peer_state, &removed_parts);
            for task_id in pending_tasks {
                let _state = self.tasks.cancel(task_id).expect(ERROR_UNRECONIZED);
            }
            pending_parts
        } else {
            Set::new()
        };
        teardown_removed_parts(&mut peer_state, &mut self.tasks, &removed_parts);
        // Deferred work is allowed to outlive the event that created it, but
        // not the peer route that gives its part hints meaning. In particular,
        // a cancelled removal may leave a re-sync parked in
        // `pending_removals`; blindly resuming it after SetPeer removed the
        // part would enqueue a task the worker cannot map to a backend.
        peer_state.pending_removals.retain(|_, pending| {
            pending
                .remaining_hints
                .retain(|part_id| !removed_parts.contains(part_id));
            pending
                .re_added_parts
                .retain(|part_id| !removed_parts.contains(part_id));
            !pending.remaining_hints.is_empty() || !pending.re_added_parts.is_empty()
        });

        restart_hinted_sync_workers(&mut peer_state, &mut self.tasks, &peer_id, &removed_parts);
        let decision_parts: Set<_> = pending_parts.union(&added_parts).cloned().collect();
        if !decision_parts.is_empty() {
            let deets = MachineTaskDeets::DecidePeerStrategy(DecidePeerStrategyTask {
                peer_id: peer_id.clone(),
                parts: decision_parts.clone(),
                sync_modes: default(),
                default_sync_mode: self.default_sync_mode,
            });
            let decide_task = self
                .tasks
                .spawn(std::time::Instant::now(), TaskSeed::Machine(deets));
            for part_id in &decision_parts {
                peer_state.parts.insert(
                    part_id.clone(),
                    PeerPartState {
                        strat: PeerPartStrategy::Pending(decide_task),
                    },
                );
            }
        }
        let stale_workers: Vec<_> = peer_state
            .sync_workers
            .iter()
            .filter_map(|(obj_id, worker)| {
                (worker.part_hints.is_empty() && !peer_state.objects.contains(obj_id))
                    .then_some(obj_id.clone())
            })
            .collect();
        for obj_id in stale_workers {
            let worker = peer_state
                .sync_workers
                .remove(&obj_id)
                .expect(ERROR_IMPOSSIBLE);
            let _state = self.tasks.cancel(worker.task_id).expect(ERROR_UNRECONIZED);
            // Nothing can complete the replay this worker owed, and the object
            // route that ordered it is gone too (that is what made the worker
            // stale), so release the claim: a re-added object must reach the
            // backend again instead of reading as already in flight.
            peer_state.cursor_machine.abandon_obj_sync(&obj_id);
        }
        // A target the embedder has dropped stops being blocked with it: a part or object that
        // comes back is a fresh entry, and only the client ever changes the target set.
        peer_state
            .replay_subscription
            .blocked
            .retain(|route| match route {
                ReplayRoute::Part(part_id) => !removed_parts.contains(part_id),
                ReplayRoute::Object(obj_id) => !removed_objects.contains(obj_id),
            });
        self.stat_machine
            .set_peer(peer_id.clone(), added_parts.iter().cloned());
        for part_id in &decision_parts {
            self.stat_machine
                .mark_peer_part_pending(peer_id.clone(), part_id.clone(), true);
        }
        self.peers.insert(peer_id.clone(), peer_state);
        self.refresh_peer_replay_worker(peer_id, false);
    }

    fn handle_remove_peer_evt(&mut self, RemovePeerEvent { peer_id }: RemovePeerEvent) {
        if let Some(old) = self.peers.remove(&peer_id) {
            tracing::debug!(
                peer_id = %peer_id,
                part_count = old.parts.len(),
                sync_worker_count = old.sync_workers.len(),
                "remove peer event"
            );
            for worker in old.sync_workers.into_values() {
                let _state = self.tasks.cancel(worker.task_id).expect(ERROR_UNRECONIZED);
            }
            for worker in old.remove_workers.into_values() {
                let _state = self.tasks.cancel(worker.task_id).expect(ERROR_UNRECONIZED);
            }
            for (_old_part_id, state) in old.parts {
                match state.strat {
                    PeerPartStrategy::Pending(_) => {}
                    PeerPartStrategy::Cursor(_strat) => {}
                    PeerPartStrategy::Bucket(strat) => {
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
            for (_, state) in old.replay_pages {
                if self.tasks.cancel(state.task_id).is_none() {
                    tracing::debug!(
                        task_id = state.task_id,
                        "replay task was already retired during peer cleanup",
                    );
                }
            }
            self.stat_machine.remove_peer(peer_id);
        }
    }

    #[tracing::instrument(
        skip_all,
        fields(
            peer_id = %peer_id,
            task_id = %task_id,
        )
    )]
    fn handle_set_peer_strat(
        &mut self,
        task_id: TaskId,
        retry: Retry,
        SetPeerStrategy {
            peer_id,
            part_strats,
        }: SetPeerStrategy,
    ) {
        let Some(peer_state) = self.peers.get_mut(&peer_id) else {
            assert!(self.all_seen_peer.contains(&peer_id), "fishy");
            return;
        };
        let response_len = part_strats.len();
        let bucket_count = part_strats
            .values()
            .filter(|deets| matches!(*deets, PeerPartStratDecision::Bucket(_)))
            .count();
        let cursor_count = part_strats
            .values()
            .filter(|deets| matches!(*deets, PeerPartStratDecision::Cursor(_)))
            .count();
        let unknown_count = part_strats
            .values()
            .filter(|deets| matches!(*deets, PeerPartStratDecision::Unkown))
            .count();
        tracing::debug!(
            peer_id = %peer_id,
            response_len,
            bucket_count,
            cursor_count,
            unknown_count,
            "set peer strategy result"
        );
        let stale_result = part_strats.keys().any(|part_id| {
            !matches!(
                peer_state.parts.get(part_id).map(|state| &state.strat),
                Some(PeerPartStrategy::Pending(old_task_id)) if *old_task_id == task_id
            )
        });
        if stale_result {
            tracing::debug!(
                peer_id = %peer_id,
                task_id = %task_id,
                "ignoring stale set peer strategy result"
            );
            return;
        }
        // This answer consumed the request the refusal was about, so the latch is cleared:
        // a refusal of the next request is a new fact.
        self.stat_machine.clear_peer_part_set_too_large(&peer_id);
        let mut parts_retry = Set::new();

        for (part_id, decision) in part_strats {
            let old = peer_state.parts.remove(&part_id);
            let strat = match decision {
                PeerPartStratDecision::Unkown => {
                    self.stat_machine.mark_peer_part_only_cursor_strat(
                        peer_id.clone(),
                        part_id.clone(),
                        false,
                    );
                    self.stat_machine
                        .mark_peer_part_unanswered(peer_id.clone(), part_id.clone());
                    parts_retry.insert(part_id);
                    continue;
                }
                PeerPartStratDecision::Bucket(strat) => {
                    let mut machine = Box::new(BucketMachine::new(
                        part_id.clone(),
                        strat.remote_depth,
                        strat.remote_len,
                        strat.last_cursor,
                    ));
                    machine.on_bucket_page(
                        strat.initial_filtered_buckets,
                        &mut peer_state.bucket_cmd_buf,
                    );
                    self.stat_machine.mark_peer_part_only_cursor_strat(
                        peer_id.clone(),
                        part_id.clone(),
                        false,
                    );
                    self.stat_machine.mark_peer_part_pending(
                        peer_id.clone(),
                        part_id.clone(),
                        false,
                    );
                    PeerPartStrategy::Bucket(BucketState {
                        machine,
                        // NOTE: we replay from the latest
                        // on bucket instead from the last_cursor
                        replay_cursor: strat.latest_cursor,
                        active_list_tasks: default(),
                        active_leaf_tasks: default(),
                    })
                }
                PeerPartStratDecision::Cursor(strat) => {
                    self.stat_machine.mark_peer_part_only_cursor_strat(
                        peer_id.clone(),
                        part_id.clone(),
                        true,
                    );
                    self.stat_machine.mark_peer_part_pending(
                        peer_id.clone(),
                        part_id.clone(),
                        false,
                    );
                    PeerPartStrategy::Cursor(CursorState {
                        replay_cursor: strat.last_cursor,
                    })
                }
            };
            peer_state.parts.insert(part_id, PeerPartState { strat });
            if let Some(old) = old {
                match old.strat {
                    PeerPartStrategy::Pending(old_task_id) => {
                        assert!(task_id == old_task_id, "fishy");
                    }
                    PeerPartStrategy::Bucket(_) | PeerPartStrategy::Cursor(_) => {}
                }
            }
        }

        // retry peer summary requests for any parts
        // that were not resolved in the last request
        // FIXME: probably not a good idea since we're going
        // to abort any work queued by the machines if
        // this immediately works. i.e. evalute if this leads
        // to loops,
        if !parts_retry.is_empty() {
            if retry.attempt_no > 0 && response_len > 0 {
                warn!("retry attempt is retrying again after minimal improvement");
            }
            let deets = TaskSeed::Machine(MachineTaskDeets::DecidePeerStrategy(
                DecidePeerStrategyTask {
                    peer_id: peer_id.clone(),
                    parts: parts_retry.clone(),
                    sync_modes: default(),
                    default_sync_mode: self.default_sync_mode,
                },
            ));
            let decide_task = if parts_retry.len() == response_len {
                self.tasks.spawn_delayed(
                    deets,
                    retry,
                    Duration::from_secs(2),
                    std::time::Instant::now(),
                )
            } else {
                self.tasks.spawn(std::time::Instant::now(), deets)
            };
            for part_id in parts_retry {
                peer_state.parts.insert(
                    part_id.clone(),
                    PeerPartState {
                        strat: PeerPartStrategy::Pending(decide_task),
                    },
                );
                self.stat_machine
                    .mark_peer_part_pending(peer_id.clone(), part_id, true);
            }
        }

        let bucket_cmd_count = peer_state.bucket_cmd_buf.len();
        let cursor_cmd_count = peer_state.cursors_cmd_buf.len();
        // refresh the replay pages if needed
        self.refresh_peer_replay_worker(peer_id.clone(), false);
        tracing::debug!(
            peer_id = %peer_id,
            bucket_cmd_count,
            cursor_cmd_count,
            "drain bucket commands from set peer strategy"
        );
        self.drain_bucket_machine_cmds(peer_id);
    }

    fn handle_decide_peer_strat_err(
        &mut self,
        task_id: TaskId,
        retry: Retry,
        DecidePeerStrategyTaskError { peer_id, deets }: DecidePeerStrategyTaskError,
    ) {
        match &deets {
            DecidePeerStrategyErrorDeets::PeerSummary(PeerSummaryError::TooManyParts {
                ..
            }) => {
                // A refusal that is a property of the request repeats identically on every
                // retry, so it is reported once as a fact on the stat channel instead of
                // warned once per attempt.
                tracing::debug!(
                    peer_id = %peer_id,
                    task_id,
                    ?deets,
                    "peer refused the summary request"
                );
            }
            _ => {
                tracing::warn!(peer_id = %peer_id, task_id, ?deets, "decide peer strategy failed");
            }
        }
        let Some(peer_state) = self.peers.get_mut(&peer_id) else {
            assert!(self.all_seen_peer.contains(&peer_id), "fishy");
            return;
        };
        match deets {
            DecidePeerStrategyErrorDeets::ListError(ListPartsError::UnkownParts {
                unkown_parts,
            }) => {
                for part_id in &unkown_parts {
                    self.stat_machine
                        .mark_peer_part_unanswered(peer_id.clone(), part_id.clone());
                }
                // The peer does not (yet) know these parts — e.g. its part
                // row appears after the route was set (a pending want on the
                // remote is only advertiseable once its part exists). Do NOT
                // drop the parts: keep them Pending and let the retry path
                // below re-decide them with backoff once the route resolves.
                tracing::debug!(
                    peer_id = %peer_id,
                    ?unkown_parts,
                    "peer does not know requested parts; keeping them pending and retrying",
                );
            }
            DecidePeerStrategyErrorDeets::Rpc(_) => {
                // noop, retry with backoff
            }
            DecidePeerStrategyErrorDeets::PeerSummary(PeerSummaryError::TooManyParts {
                requested,
                cap,
            }) => {
                // Reported once, as a fact on the stat channel, rather than per attempt:
                // the ceiling is a property of the part set the asker itself built, so no
                // retry clears it — only the embedder narrowing that set does — and the
                // retry path below keeps re-asking until then, so the state heals by
                // itself once the plan changes. There is deliberately no catch-all arm: a
                // new refusal shape has to be classified here rather than silently
                // inheriting this one's verdict.
                self.stat_machine
                    .mark_peer_part_set_too_large(peer_id.clone(), requested, cap);
            }
        }
        let mut parts_retry = Set::new();

        for (part_id, state) in &peer_state.parts {
            match &state.strat {
                PeerPartStrategy::Pending(old_task_id) => {
                    if *old_task_id != task_id {
                        // the task that errored out must have been stale
                        // FIXME: this assumes that all parts for a peer
                        // are decided in the same task (having per part task ids can be misleading)
                        return;
                    }
                    parts_retry.insert(part_id.clone());
                }
                PeerPartStrategy::Bucket(_) | PeerPartStrategy::Cursor(_) => {}
            };
        }
        if !parts_retry.is_empty() {
            let deets = MachineTaskDeets::DecidePeerStrategy(DecidePeerStrategyTask {
                peer_id: peer_id.clone(),
                parts: parts_retry.clone(),
                sync_modes: default(),
                default_sync_mode: self.default_sync_mode,
            });
            let decide_task = self.tasks.spawn_delayed(
                TaskSeed::Machine(deets),
                retry,
                Duration::from_secs(2),
                std::time::Instant::now(),
            );
            for part_id in parts_retry {
                let old = peer_state.parts.insert(
                    part_id.clone(),
                    PeerPartState {
                        strat: PeerPartStrategy::Pending(decide_task),
                    },
                );
                assert!(matches!(
                    old,
                    Some(PeerPartState {
                        strat: PeerPartStrategy::Pending(_),
                        ..
                    })
                ));
                self.stat_machine
                    .mark_peer_part_pending(peer_id.clone(), part_id, true);
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
