use crate::interlude::*;

use crate::{
    SyncMode,
    bucket::{BucketMachine, calc_working_level},
    part_store::{CursorIndex, PartStoreReadOnly},
    rpc::{
        BigSyncRpcClient, BuckLevel, BucketSummary, GetChangedBucketsRequest, ListPartsError,
        PartStratSummary, PeerSummaryRequest, RpcError,
    },
    tasks::{TaskCtx, TaskResultDeets},
};

#[derive(Debug, Clone)]
pub struct DecidePeerStrategyTask {
    pub peer_id: PeerKey,
    pub parts: Set<PartKey>,
    /// Per-part strategy override. A part with no entry here uses
    /// [`Self::default_sync_mode`].
    pub sync_modes: Map<PartKey, SyncMode>,
    /// Strategy hint for parts absent from [`Self::sync_modes`]. The bucket-diff
    /// strategy is still opt-in, so this stays `CursorOnly` unless an embedder
    /// turns it on.
    pub default_sync_mode: SyncMode,
}

structstruck::strike! {
    #[structstruck::each[derive(Debug)]]
    pub struct SetPeerStrategy {
        pub peer_id: PeerKey,
        pub part_strats: Map<PartKey, pub enum PeerPartStratDecision{
            Unkown,
            Cursor(struct CursorStrat {
                #![derive(PartialEq, Eq)]
                pub latest_cursor: CursorIndex,
                /// THe cursor that was last procssed from the
                /// peer
                pub last_cursor: CursorIndex,
            }),
            Bucket(struct BucketStrat {
                pub latest_cursor: CursorIndex,
                pub last_cursor: CursorIndex,
                pub remote_depth: BuckLevel,
                pub remote_len: u64,
                pub initial_filtered_buckets: Vec<BucketSummary>,
            }),
        }>
    }
}
structstruck::strike! {
    #[structstruck::each[derive(Debug)]]
    pub struct DecidePeerStrategyTaskError {
        pub peer_id: PeerKey,
        pub deets:
            enum DecidePeerStrategyErrorDeets {
                #![derive(thiserror::Error, displaydoc::Display)]
                /// {0}
                ListError(#[from] ListPartsError)
                /// {0}
                Rpc(#[from] RpcError),
            }

    }
}

impl DecidePeerStrategyTask {
    #[tracing::instrument(skip(self, cx), fields(peer_id = %self.peer_id, part_count = self.parts.len()))]
    pub async fn run<K, PStore, Rpc, Rng>(
        self,
        cx: &mut TaskCtx<K, PStore, Rpc, Rng>,
    ) -> Result<TaskResultDeets, DecidePeerStrategyTaskError>
    where
        K: FutureForm,
        PStore: PartStoreReadOnly<K>,
        Rpc: BigSyncRpcClient<K>,
        Rng: rand::Rng,
    {
        let peer_id = self.peer_id.clone();
        self.run_run(cx)
            .await
            .map_err(|deets| DecidePeerStrategyTaskError { peer_id, deets })
    }
    async fn run_run<K, PStore, Rpc, Rng>(
        self,
        cx: &mut TaskCtx<K, PStore, Rpc, Rng>,
    ) -> Result<TaskResultDeets, DecidePeerStrategyErrorDeets>
    where
        K: FutureForm,
        PStore: PartStoreReadOnly<K>,
        Rpc: BigSyncRpcClient<K>,
        Rng: rand::Rng,
    {
        let Some(peer_rpc) = cx.rpc_clients.get(&self.peer_id) else {
            // Peer teardown can race a queued strategy task. Treat the
            // missing client as a transport failure instead of panicking.
            return Err(DecidePeerStrategyErrorDeets::Rpc(RpcError::TransportError));
        };

        // The peer counts relevance against the cursor we advertise, so gather the
        // cursors we hold for it first and reuse them as this decision's own
        // bookkeeping: the value we advertise and the value we decide with have to be
        // the same one.
        let mut asker_part_cursors = Map::new();
        for part_id in &self.parts {
            let cursor = cx.part_store.get_peer_part_cursor(self.peer_id.clone(), part_id.clone()).await;
            asker_part_cursors.insert(part_id.clone(), cursor);
        }
        let summary = peer_rpc
            .peer_summary(PeerSummaryRequest {
                parts: self.parts.clone(),
                asker_part_cursors: asker_part_cursors.clone(),
            })
            .await??;
        tracing::debug!(
            peer_id = %self.peer_id,
            part_count = summary.parts.len(),
            "decide peer strategy summary"
        );

        let mut part_strats: Map<_, _> = default();
        for part_id in self.parts {
            let Some(strat_summaries) = summary.parts.get(&part_id) else {
                part_strats.insert(part_id, PeerPartStratDecision::Unkown);
                continue;
            };
            let cursor_summary = strat_summaries.iter().find_map(|strat| match strat {
                PartStratSummary::Cursor(cursor) => Some(cursor),
                PartStratSummary::Bucket(_) => None,
            });
            let bucket_summary = strat_summaries.iter().find_map(|strat| match strat {
                PartStratSummary::Bucket(bucket) => Some(bucket),
                PartStratSummary::Cursor(_) => None,
            });
            let last_peer_cursor = asker_part_cursors.get(&part_id).copied().unwrap_or(0);
            // FIXME: the real issue with bucket is not a deadlock,
            // it just doens't deal with filtered sets well enough
            // ─────────────────────────────────────────────────────────────
            // ⚠️ BUCKET-STRAT DISABLED FOR UNCONFIGURED EMBEDDERS ⚠️
            //
            // The bucket-diff strategy DEADLOCKS in the big_repo/daybook
            // offline-reopen scenario: with a >256-event cursor diff the
            // picker chose Bucket, the bucket machine started post-reopen and
            // never completed (multi_strat never cleared), permanently
            // blocking `wait_for_full_sync` — the four-node stress hang.
            //
            // Until the bucket machine's stall is fixed, embedders that do
            // not explicitly opt in via `sync_modes` always get CursorOnly.
            // big_sync's own suite opts in explicitly where it tests the
            // bucket path.
            // ─────────────────────────────────────────────────────────────
            let sync_mode = self
                .sync_modes
                .get(&part_id)
                .copied()
                .unwrap_or(self.default_sync_mode);
            let Some(cursor_summary) = cursor_summary else {
                // No cursor strat advertised for this part (bucket-only).
                // Drive it through the bucket path if a bucket summary exists;
                // otherwise the part is unknown to the peer.
                let Some(bucket) = bucket_summary else {
                    part_strats.insert(part_id, PeerPartStratDecision::Unkown);
                    continue;
                };
                let working_level =
                    calc_working_level(bucket.member_count, bucket.deepest_bucket_level);
                let mut offset = BuckId::ROOT;
                loop {
                    let buckets = peer_rpc
                        .get_changed_buckets(GetChangedBucketsRequest {
                            part_id: part_id.clone(),
                            offset,
                            to_level: working_level,
                            limit_hint: BucketMachine::GET_BUCKET_LIMIT_HINT,
                            since: last_peer_cursor,
                        })
                        .await??;
                    let filtered = crate::bucket::filter_buckets(
                        part_id.clone(),
                        working_level,
                        buckets,
                        &cx.part_store,
                    )
                    .await;
                    let strat = match filtered {
                        crate::bucket::FilteredBuckets::Relist(buck_id) => {
                            offset = buck_id;
                            continue;
                        }
                        crate::bucket::FilteredBuckets::Done => {
                            PeerPartStratDecision::Cursor(CursorStrat {
                                latest_cursor: last_peer_cursor,
                                last_cursor: last_peer_cursor,
                            })
                        }
                        crate::bucket::FilteredBuckets::Handoff(buckets) => {
                            PeerPartStratDecision::Bucket(BucketStrat {
                                latest_cursor: last_peer_cursor,
                                initial_filtered_buckets: buckets,
                                last_cursor: last_peer_cursor,
                                remote_depth: bucket.deepest_bucket_level,
                                remote_len: bucket.member_count,
                            })
                        }
                    };
                    part_strats.insert(part_id, strat);
                    break;
                }
                continue;
            };
            let latest_cursor = cursor_summary.latest_cursor;
            let diff = latest_cursor.abs_diff(last_peer_cursor);
            // The peer published this count, counted on its own cursor scale against the
            // cursor we advertised above. Instrumentation sits before any cutoff AND before
            // the mode check: both numbers are logged for every band, so the placeholder
            // below can be measured rather than guessed, and so the count can be compared
            // with the raw counter gap it replaces. A `CursorOnly` decision is exactly the
            // case where the count was never consulted and the measurement is still wanted.
            let dirty = cursor_summary.dirty_count;
            tracing::debug!(
                peer_id = %self.peer_id,
                ?part_id,
                remote_cursor = latest_cursor,
                stored_peer_cursor = last_peer_cursor,
                cursor_diff = diff,
                dirty_member_changes = dirty.member_changes,
                dirty_access_changes = dirty.access_changes,
                dirty_total = dirty.total(),
                ?sync_mode,
                "decide peer part strategy",
            );
            if sync_mode == SyncMode::CursorOnly {
                part_strats.insert(
                    part_id,
                    PeerPartStratDecision::Cursor(CursorStrat {
                        latest_cursor,
                        last_cursor: last_peer_cursor,
                    }),
                );
                continue;
            }
            if dirty.total() <= BucketMachine::BUCKET_DIRTY_THRESHOLD {
                part_strats.insert(
                    part_id,
                    PeerPartStratDecision::Cursor(CursorStrat {
                        latest_cursor,
                        last_cursor: last_peer_cursor,
                    }),
                );
                continue;
            }
            let Some(bucket) = bucket_summary else {
                // The peer advertises only a cursor strat for this part and
                // the diff is large; degrade to the cursor strat.
                part_strats.insert(
                    part_id,
                    PeerPartStratDecision::Cursor(CursorStrat {
                        latest_cursor,
                        last_cursor: last_peer_cursor,
                    }),
                );
                continue;
            };
            let mut offset = BuckId::ROOT;
            let working_level =
                calc_working_level(bucket.member_count, bucket.deepest_bucket_level);
            loop {
                let buckets = peer_rpc
                    .get_changed_buckets(GetChangedBucketsRequest {
                        part_id: part_id.clone(),
                        offset,
                        to_level: working_level,
                        limit_hint: BucketMachine::GET_BUCKET_LIMIT_HINT,
                        since: last_peer_cursor,
                    })
                    .await??;
                let filtered =
                    crate::bucket::filter_buckets(part_id.clone(), working_level, buckets, &cx.part_store)
                        .await;
                let strat = match filtered {
                    crate::bucket::FilteredBuckets::Relist(buck_id) => {
                        offset = buck_id;
                        continue;
                    }
                    crate::bucket::FilteredBuckets::Done => {
                        PeerPartStratDecision::Cursor(CursorStrat {
                            latest_cursor,
                            last_cursor: last_peer_cursor,
                        })
                    }
                    crate::bucket::FilteredBuckets::Handoff(buckets) => {
                        PeerPartStratDecision::Bucket(BucketStrat {
                            latest_cursor,
                            initial_filtered_buckets: buckets,
                            last_cursor: last_peer_cursor,
                            remote_depth: bucket.deepest_bucket_level,
                            remote_len: bucket.member_count,
                        })
                    }
                };
                part_strats.insert(part_id, strat);
                break;
            }
        }
        tracing::debug!(
            peer_id = %self.peer_id,
            bucket_parts = part_strats
                .values()
                .filter(|deets| matches!(*deets, PeerPartStratDecision::Bucket(_)))
                .count(),
            cursor_parts = part_strats
                .values()
                .filter(|deets| matches!(*deets, PeerPartStratDecision::Cursor(_)))
                .count(),
            unknown_parts = part_strats
                .values()
                .filter(|deets| matches!(*deets, PeerPartStratDecision::Unkown))
                .count(),
            "decide peer strategy result"
        );
        Ok(TaskResultDeets::SetPeerStrategy(SetPeerStrategy {
            peer_id: self.peer_id,
            part_strats,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        BuckId, ObjKey, PeerKey, PartKey, SyncMode,
        mpsc,
        part_store::{ObjPayload, PartDirtyCount, PartStoreReadOnly},
        rpc::{
            BigSyncRpcResult, BucketPartSummary, BucketSummary, CursorPartSummary, LeafBucketResult,
            LeafBucketsError, ListPartsError, PartStratSummary, PeerSummaryRequest,
            PeerSummaryResult,
        },
        tasks::{MachineTaskMsg, TaskCtx, TaskResultDeets},
    };
    use future_form::Local;
    use futures::future::LocalBoxFuture;
    use std::cell::Cell;
    use std::rc::Rc;

    const PART_BYTES: [u8; 32] = [7; 32];
    const PEER_BYTES: [u8; 32] = [9; 32];

    /// A peer that advertises both strats and publishes the relevance the asker is
    /// behind on, so the decision side is the only thing that picks the band. Entering
    /// the bucket walk means calling this listing.
    struct FakeRpc {
        latest_cursor: CursorIndex,
        /// What the peer publishes: its own rows counted against the cursor the request
        /// advertises.
        dirty_count: PartDirtyCount,
        /// The cursor the request has to advertise for that count to mean anything, so
        /// the fake checks it rather than trusting the call site.
        asker_cursor: CursorIndex,
        dirty_bucket: BucketSummary,
        bucket_walk_entered: Rc<Cell<bool>>,
    }

    impl BigSyncRpcClient<Local> for FakeRpc {
        fn peer_summary<'a>(
            &'a self,
            req: PeerSummaryRequest,
        ) -> LocalBoxFuture<'a, BigSyncRpcResult<Result<PeerSummaryResult, ListPartsError>>> {
            assert_eq!(
                req.asker_part_cursors
                    .get(&PartKey::new(PART_BYTES))
                    .copied(),
                Some(self.asker_cursor),
                "the asker must advertise the cursor it holds, otherwise the peer cannot \
                 count relevance on the scale that cursor belongs to"
            );
            let parts = Map::from_iter([(
                PartKey::new(PART_BYTES),
                vec![
                    PartStratSummary::Cursor(CursorPartSummary {
                        latest_cursor: self.latest_cursor,
                        dirty_count: self.dirty_count,
                    }),
                    PartStratSummary::Bucket(BucketPartSummary {
                        deepest_bucket_level: 0,
                        member_count: 1,
                    }),
                ],
            )]);
            Local::from_future(async move { Ok(Ok(PeerSummaryResult { parts })) })
        }

        fn replay_page<'a>(
            &'a self,
            _req: crate::rpc::ReplayPageRequest,
        ) -> LocalBoxFuture<'a, BigSyncRpcResult<crate::rpc::ReplayPageOutcome>> {
            unreachable!("the decision task does not page events")
        }

        fn get_changed_buckets<'a>(
            &'a self,
            _req: GetChangedBucketsRequest,
        ) -> LocalBoxFuture<'a, BigSyncRpcResult<Result<Vec<BucketSummary>, ListPartsError>>> {
            self.bucket_walk_entered.set(true);
            let bucket = self.dirty_bucket.clone();
            Local::from_future(async move { Ok(Ok(vec![bucket])) })
        }

        fn leaf_buckets<'a>(
            &'a self,
            _req: crate::rpc::LeafBucketsRequest,
        ) -> LocalBoxFuture<'a, BigSyncRpcResult<Result<LeafBucketResult, LeafBucketsError>>> {
            unreachable!("the decision task does not leaf")
        }
    }

    /// Answers only what the decision task reads: the stored peer cursor and the local
    /// bucket summary.
    ///
    /// `part_dirty_count` is unreachable on purpose. The puller must take the count
    /// from the peer's published summary: a local read would count this side's rows
    /// against a cursor from the peer's stream, which is the bug the exchanged
    /// descriptor exists to fix.
    struct FakeStore {
        peer_cursor: CursorIndex,
        local_bucket: BucketSummary,
    }

    impl PartStoreReadOnly<Local> for FakeStore {
        fn member_count<'a>(&'a self, _part_id: PartKey) -> LocalBoxFuture<'a, u64> {
            unreachable!("the decision task does not read member counts")
        }

        fn obj_payload<'a>(&'a self, _obj_id: ObjKey) -> LocalBoxFuture<'a, Option<ObjPayload>> {
            unreachable!("the decision task does not read payloads")
        }

        fn obj_parts<'a>(&'a self, _obj_id: ObjKey) -> LocalBoxFuture<'a, Vec<PartKey>> {
            unreachable!("the decision task does not read object parts")
        }

        fn get_peer_part_cursor<'a>(
            &'a self,
            _peer_id: PeerKey,
            _part_id: PartKey,
        ) -> LocalBoxFuture<'a, CursorIndex> {
            let cursor = self.peer_cursor;
            Local::from_future(async move { cursor })
        }

        fn get_bucket_summary<'a>(
            &'a self,
            _part_id: PartKey,
            _id: BuckId,
        ) -> LocalBoxFuture<'a, BucketSummary> {
            let summary = self.local_bucket.clone();
            Local::from_future(async move { summary })
        }

        fn part_dirty_count<'a>(
            &'a self,
            _part_id: PartKey,
            _principal: Option<PeerKey>,
            _since: CursorIndex,
        ) -> LocalBoxFuture<'a, PartDirtyCount> {
            unreachable!("the puller must read the count from the peer's published summary")
        }
    }

    /// Run the real decision task against the fake store, returning the band it chose
    /// and whether it entered the peer's bucket walk. `dirty` is what the peer
    /// publishes, not something this side can count locally.
    async fn decide(
        remote_latest_cursor: CursorIndex,
        peer_cursor: CursorIndex,
        dirty: PartDirtyCount,
    ) -> (PeerPartStratDecision, bool) {
        let part_id = PartKey::new(PART_BYTES);
        let peer_id = PeerKey::new(PEER_BYTES);
        let bucket_walk_entered = Rc::new(Cell::new(false));
        let rpc = FakeRpc {
            latest_cursor: remote_latest_cursor,
            dirty_count: dirty,
            asker_cursor: peer_cursor,
            dirty_bucket: BucketSummary {
                id: BuckId::ROOT,
                len: 1,
                live_count: 1,
                fp: (0xdead, 0xbeef),
                changed_at: peer_cursor,
            },
            bucket_walk_entered: Rc::clone(&bucket_walk_entered),
        };
        let store = FakeStore {
            peer_cursor,
            // Disagrees with the remote bucket, so the walk hands it off as dirty
            // instead of pruning it as clean.
            local_bucket: BucketSummary {
                id: BuckId::ROOT,
                len: 0,
                live_count: 0,
                fp: (0, 0),
                changed_at: peer_cursor,
            },
        };
        let (main_tx, _main_rx): (mpsc::Sender<MachineTaskMsg>, mpsc::Receiver<MachineTaskMsg>) =
            mpsc::unbounded("test".into(), "test".into());
        let mut cx = TaskCtx {
            task_id: 1,
            main_tx,
            rpc_clients: Map::from_iter([(peer_id.clone(), rpc)]),
            part_store: store,
            rng: rand::rng(),
            _phantom: std::marker::PhantomData,
        };
        let task = DecidePeerStrategyTask {
            peer_id,
            parts: Set::from([part_id.clone()]),
            // The bucket band is opt-in only, so a part left on `CursorOnly` would
            // short-circuit before the band choice is reached. The per-part override
            // selects it here; the machine default stays `CursorOnly`, so this also
            // covers the override layer.
            sync_modes: Map::from_iter([(part_id.clone(), SyncMode::Bucket)]),
            default_sync_mode: SyncMode::CursorOnly,
        };
        let deets = task
            .run(&mut cx)
            .await
            .expect("the decision task must succeed");
        let TaskResultDeets::SetPeerStrategy(mut strategy) = deets else {
            panic!("expected a peer strategy result, got {deets:?}");
        };
        let strat = strategy
            .part_strats
            .remove(&part_id)
            .expect("the requested part must have a strategy");
        (strat, bucket_walk_entered.get())
    }

    /// The band comes from the relevance the peer reports. This peer's stored cursor
    /// trails the part's latest by 100_000, but the peer says exactly one of those
    /// changes concerns this part, so cursor replay is the right band. Choosing on the
    /// raw cursor difference picked the bucket walk here.
    #[tokio::test(flavor = "current_thread")]
    async fn large_cursor_gap_with_small_dirty_count_stays_on_cursor() {
        let (strat, bucket_walk_entered) = decide(
            100_000,
            0,
            PartDirtyCount {
                member_changes: 1,
                access_changes: 0,
            },
        )
        .await;
        assert!(
            !bucket_walk_entered,
            "one relevant change must not enter the bucket walk"
        );
        match strat {
            PeerPartStratDecision::Cursor(cursor) => {
                assert_eq!(cursor.latest_cursor, 100_000);
                assert_eq!(cursor.last_cursor, 0);
            }
            other => panic!("expected the cursor band, got {other:?}"),
        }
    }

    /// The converse, and the reason a cursor difference cannot stand in for the
    /// published count: a tiny cursor gap with a large amount of relevant change still
    /// needs the bucket walk.
    #[tokio::test(flavor = "current_thread")]
    async fn small_cursor_gap_with_large_dirty_count_uses_bucket_walk() {
        let (strat, bucket_walk_entered) = decide(
            12,
            10,
            PartDirtyCount {
                member_changes: 200,
                access_changes: 100,
            },
        )
        .await;
        assert!(
            bucket_walk_entered,
            "300 relevant changes must enter the bucket walk"
        );
        assert!(
            matches!(strat, PeerPartStratDecision::Bucket(_)),
            "expected the bucket band, got {strat:?}"
        );
    }
}

