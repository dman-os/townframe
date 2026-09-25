use crate::interlude::*;

strike! {
    #[structstruck::each[derive(Default)]]
    pub struct StatMachine {
        evts: Vec<SyncStatEvent>,

        #[cfg(any(test, feature = "test-support"))]
        last_object_syncs: Map<(PeerKey, PartKey), (ObjKey, std::time::Instant)>,

        waiters: Map<u64, struct FullSyncWaiterState {
            done_set: Set<(PeerKey, PartKey)>,
            need_set: Set<(PeerKey, PartKey)>,
        }>,
        peers: Map<PeerKey, struct PeerSyncState {
            replay_phase_done: bool,
            emitted_full_synced: bool,
            parts: Map<PartKey, struct PeerPartStatState {
                emitted_full_synced: bool,
                cursor_active: bool,
                multi_strat: bool,
                /// True while the part's strategy is still being negotiated
                /// (Pending decision task) — an undecided part must block
                /// full sync.
                pending: bool,
                /// True once the peer has answered that it does not know this
                /// part.
                unknown: bool,
            }>,
            fully_synced_parts: Set<PartKey>,
        }>,
        parts: Map<PartKey, struct PartSyncState {
            emitted_full_synced: bool,
            peers: Set<PeerKey>,
            fully_synced_peers: Set<PeerKey>,
        }>,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncStatEvent {
    ObjectSynced {
        peer_key: PeerKey,
        obj_key: ObjKey,
    },
    PeerPartFullySynced {
        peer_key: PeerKey,
        part_key: PartKey,
    },
    PeerPartStale {
        peer_key: PeerKey,
        part_key: PartKey,
    },
    /// The peer answered that it does not know this part, so the part has
    /// no strategy and no cursor, and it keeps full sync blocked until the
    /// embedder drops it from the subscription set — or the part reaches the
    /// peer, which is the common case since the answer is usually a race.
    ///
    /// Emitted once per transition into the unanswered state: the machine
    /// re-asks on a backoff but only on the transition is this emitted.
    PeerPartUnknown {
        peer_key: PeerKey,
        part_key: PartKey,
    },
    PartFullySynced {
        part_key: PartKey,
    },
    PartStale {
        part_key: PartKey,
    },
    PeerFullySynced {
        peer_key: PeerKey,
    },
    PeerStale {
        peer_key: PeerKey,
    },
    FullSyncWaiterSatisfied {
        waiter_id: u64,
    },
}

impl StatMachine {
    pub fn drain_evts(&mut self) -> std::vec::Drain<'_, SyncStatEvent> {
        self.evts.drain(..)
    }

    fn peer_part_is_fully_synced(&self, peer_key: PeerKey, part_key: PartKey) -> bool {
        let Some(peer_state) = self.peers.get(&peer_key) else {
            return false;
        };
        let Some(peer_part_state) = peer_state.parts.get(&part_key) else {
            return false;
        };
        !peer_part_state.pending
            && !peer_part_state.unknown
            && !peer_part_state.multi_strat
            && peer_state.replay_phase_done
            && !peer_part_state.cursor_active
    }

    pub fn add_full_sync_waiter(
        &mut self,
        waiter_id: u64,
        peer_keys: Set<PeerKey>,
        part_keys: Set<PartKey>,
    ) {
        let peer_count = peer_keys.len();
        let part_count = part_keys.len();
        let mut done_set = Set::new();
        let mut need_set = Set::new();
        for peer_key in &peer_keys {
            for part_key in &part_keys {
                need_set.insert((peer_key.clone(), part_key.clone()));
                if self.peer_part_is_fully_synced(peer_key.clone(), part_key.clone()) {
                    done_set.insert((peer_key.clone(), part_key.clone()));
                }
            }
        }
        if done_set.len() == need_set.len() {
            tracing::debug!(
                waiter_id,
                peer_count,
                part_count,
                "full sync waiter satisfied immediately"
            );
            self.evts
                .push(SyncStatEvent::FullSyncWaiterSatisfied { waiter_id });
            return;
        }
        tracing::debug!(
            waiter_id,
            peer_count = peer_keys.len(),
            part_count = part_keys.len(),
            done_count = done_set.len(),
            "register full sync waiter"
        );
        let old = self
            .waiters
            .insert(waiter_id, FullSyncWaiterState { done_set, need_set });
        assert!(old.is_none(), "fishy");
    }

    fn set_peer(&mut self, peer_key: PeerKey, parts: impl Iterator<Item = PartKey>) {
        let peer_state = self.peers.entry(peer_key).or_default();
        for part_key in parts {
            let _part_state = self.parts.entry(part_key.clone()).or_default();
            let _peer_part_state = peer_state.parts.entry(part_key).or_default();
        }
    }

    fn remove_peer(&mut self, peer_key: PeerKey) {
        let Some(peer_state) = self.peers.remove(&peer_key) else {
            return;
        };
        for (part_key, _peer_part_state) in peer_state.parts {
            #[cfg(any(test, feature = "test-support"))]
            self.last_object_syncs
                .remove(&(peer_key.clone(), part_key.clone()));

            let Some(part_state) = self.parts.get_mut(&part_key) else {
                continue;
            };
            part_state.peers.remove(&peer_key);
            part_state.fully_synced_peers.remove(&peer_key);
            // When a peer is removed, clean up all waiters that reference it
            // so they don't remain stranded.
            for waiter in self.waiters.values_mut() {
                waiter
                    .done_set
                    .remove(&(peer_key.clone(), part_key.clone()));
                waiter
                    .need_set
                    .remove(&(peer_key.clone(), part_key.clone()));
            }
            if part_state.peers.is_empty() {
                self.parts.remove(&part_key);
            } else if part_state.peers.len() == part_state.fully_synced_peers.len() {
                if !part_state.emitted_full_synced {
                    part_state.emitted_full_synced = true;
                    self.evts.push(SyncStatEvent::PartFullySynced { part_key });
                }
            } else if part_state.emitted_full_synced {
                part_state.emitted_full_synced = false;
                self.evts.push(SyncStatEvent::PartStale { part_key });
            }
        }
        for (waiter_id, _waiter) in self
            .waiters
            .extract_if(|_id, waiter| waiter.done_set.len() == waiter.need_set.len())
        {
            self.evts
                .push(SyncStatEvent::FullSyncWaiterSatisfied { waiter_id });
        }
    }

    fn mark_peer_replay_done(&mut self, peer_key: PeerKey, replay_done: bool) {
        let peer_state = self.peers.entry(peer_key.clone()).or_default();
        peer_state.replay_phase_done = replay_done;
        for part_key in peer_state.parts.keys().cloned().collect::<Vec<_>>() {
            if replay_done {
                self.__check_peer_part_synced(peer_key.clone(), part_key);
            } else {
                self.__check_peer_part_stale(peer_key.clone(), part_key);
            }
        }
    }

    fn mark_peer_part_only_cursor_strat(
        &mut self,
        peer_key: PeerKey,
        part_key: PartKey,
        only_cursor_strat: bool,
    ) {
        let peer_state = self.peers.entry(peer_key.clone()).or_default();
        let peer_part_state = peer_state.parts.entry(part_key.clone()).or_default();
        peer_part_state.multi_strat = !only_cursor_strat;
        if only_cursor_strat {
            self.__check_peer_part_synced(peer_key, part_key);
        } else {
            self.__check_peer_part_stale(peer_key, part_key);
        }
    }

    fn mark_peer_part_keyle(&mut self, peer_key: PeerKey, part_key: PartKey) {
        let peer_state = self.peers.entry(peer_key.clone()).or_default();
        let peer_part_state = peer_state.parts.entry(part_key.clone()).or_default();
        peer_part_state.cursor_active = false;
        self.__check_peer_part_synced(peer_key, part_key);
    }

    fn mark_peer_part_cursor_active(&mut self, peer_key: PeerKey, part_key: PartKey) {
        let peer_state = self.peers.entry(peer_key.clone()).or_default();
        let part_state = self.parts.entry(part_key.clone()).or_default();
        let peer_part_state = peer_state.parts.entry(part_key.clone()).or_default();
        part_state.peers.insert(peer_key.clone());

        peer_part_state.cursor_active = true;
        self.__check_peer_part_stale(peer_key, part_key);
    }

    fn mark_peer_part_pending(&mut self, peer_key: PeerKey, part_key: PartKey, pending: bool) {
        let peer_state = self.peers.entry(peer_key.clone()).or_default();
        let peer_part_state = peer_state.parts.entry(part_key.clone()).or_default();
        peer_part_state.pending = pending;
        if !pending {
            // A decided strategy is the answer to what the event reported.
            peer_part_state.unknown = false;
        }
        if pending {
            self.__check_peer_part_stale(peer_key, part_key);
        } else {
            self.__check_peer_part_synced(peer_key, part_key);
        }
    }

    /// Record the peer's answer that it does not know `part_key`.
    ///
    /// Reports the fact once per transition. The part is deliberately left
    /// blocking full sync: the machine does not decide that a part the peer
    /// cannot answer for is synced, so the embedder removes it from the
    /// subscription set when that is its policy, or leaves it to resolve.
    fn mark_peer_part_unknown(&mut self, peer_key: PeerKey, part_key: PartKey) {
        let peer_state = self.peers.entry(peer_key.clone()).or_default();
        let peer_part_state = peer_state.parts.entry(part_key.clone()).or_default();
        if peer_part_state.unknown {
            return;
        }
        peer_part_state.unknown = true;
        self.evts
            .push(SyncStatEvent::PeerPartUnknown { peer_key, part_key });
    }

    fn __check_peer_part_synced(&mut self, peer_key: PeerKey, part_key: PartKey) {
        let peer_state = self.peers.entry(peer_key.clone()).or_default();
        let part_state = self.parts.entry(part_key.clone()).or_default();
        let peer_part_state = peer_state.parts.entry(part_key.clone()).or_default();
        part_state.peers.insert(peer_key.clone());

        if peer_part_state.pending
            || peer_part_state.multi_strat
            || !peer_state.replay_phase_done
            || peer_part_state.cursor_active
        {
            return;
        }

        peer_state.fully_synced_parts.insert(part_key.clone());
        part_state.fully_synced_peers.insert(peer_key.clone());

        for waiter in self.waiters.values_mut() {
            if waiter
                .need_set
                .contains(&(peer_key.clone(), part_key.clone()))
            {
                waiter.done_set.insert((peer_key.clone(), part_key.clone()));
            }
        }
        for (waiter_id, _waiter) in self
            .waiters
            .extract_if(|_id, waiter| waiter.done_set.len() == waiter.need_set.len())
        {
            self.evts
                .push(SyncStatEvent::FullSyncWaiterSatisfied { waiter_id });
        }

        if !peer_part_state.emitted_full_synced {
            peer_part_state.emitted_full_synced = true;
            self.evts.push(SyncStatEvent::PeerPartFullySynced {
                peer_key: peer_key.clone(),
                part_key: part_key.clone(),
            });
        }
        if peer_state.fully_synced_parts.len() == peer_state.parts.len()
            && !peer_state.emitted_full_synced
        {
            peer_state.emitted_full_synced = true;
            self.evts.push(SyncStatEvent::PeerFullySynced { peer_key });
        }
        if part_state.peers.len() == part_state.fully_synced_peers.len()
            && !part_state.emitted_full_synced
        {
            part_state.emitted_full_synced = true;
            self.evts.push(SyncStatEvent::PartFullySynced { part_key });
        }
    }

    fn __check_peer_part_stale(&mut self, peer_key: PeerKey, part_key: PartKey) {
        let peer_state = self.peers.entry(peer_key.clone()).or_default();
        let part_state = self.parts.entry(part_key.clone()).or_default();
        let peer_part_state = peer_state.parts.entry(part_key.clone()).or_default();
        part_state.peers.insert(peer_key.clone());

        if !(peer_part_state.pending
            || peer_part_state.multi_strat
            || !peer_state.replay_phase_done
            || peer_part_state.cursor_active)
        {
            return;
        }

        peer_state.fully_synced_parts.remove(&part_key);
        part_state.fully_synced_peers.remove(&peer_key);

        for waiter in self.waiters.values_mut() {
            waiter
                .done_set
                .remove(&(peer_key.clone(), part_key.clone()));
        }

        if peer_state.emitted_full_synced {
            peer_state.emitted_full_synced = false;
            self.evts.push(SyncStatEvent::PeerStale {
                peer_key: peer_key.clone(),
            });
        }

        if peer_part_state.emitted_full_synced {
            peer_part_state.emitted_full_synced = false;
            self.evts.push(SyncStatEvent::PeerPartStale {
                peer_key,
                part_key: part_key.clone(),
            });
        }
        if part_state.emitted_full_synced {
            part_state.emitted_full_synced = false;
            self.evts.push(SyncStatEvent::PartStale { part_key });
        }
    }
}

impl StatMachine {
    #[cfg(any(test, feature = "test-support"))]
    fn record_object_synced(&mut self, peer_key: PeerKey, part_key: PartKey, obj_key: ObjKey) {
        self.last_object_syncs
            .insert((peer_key, part_key), (obj_key, std::time::Instant::now()));
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn debug_full_sync_waiters(&self) -> Map<u64, Vec<(PeerKey, PartKey)>> {
        self.waiters
            .iter()
            .map(|(&waiter_id, waiter)| {
                (
                    waiter_id,
                    waiter
                        .need_set
                        .difference(&waiter.done_set)
                        .cloned()
                        .collect(),
                )
            })
            .collect()
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn debug_last_object_syncs(&self) -> Vec<(PeerKey, PartKey, ObjKey, std::time::Instant)> {
        self.last_object_syncs
            .iter()
            .map(|((peer_key, part_key), (obj_key, at))| {
                (peer_key.clone(), part_key.clone(), obj_key.clone(), *at)
            })
            .collect()
    }
}
