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

#[derive(Debug)]
pub struct DecidePeerStrategyTask {
    pub peer_id: PeerId,
    pub parts: Set<PartId>,
    /// Per-part sync mode; defaults to `Bucket` if not specified.
    pub sync_modes: Map<PartId, SyncMode>,
}

structstruck::strike! {
    #[structstruck::each[derive(Debug)]]
    pub struct SetPeerStrategy {
        pub peer_id: PeerId,
        pub part_strats: Map<PartId, pub enum PeerPartStratDecision{
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
        pub peer_id: PeerId,
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
        let peer_id = self.peer_id;
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

        let summary = peer_rpc
            .peer_summary(PeerSummaryRequest {
                parts: self.parts.clone(),
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
                PartStratSummary::Cursor(cursor) => Some(cursor.latest_cursor),
                PartStratSummary::Bucket(_) => None,
            });
            let bucket_summary = strat_summaries.iter().find_map(|strat| match strat {
                PartStratSummary::Bucket(bucket) => Some(bucket),
                PartStratSummary::Cursor(_) => None,
            });
            let last_peer_cursor = cx
                .part_store
                .get_peer_part_cursor(self.peer_id, part_id)
                .await;
            // Per-part sync mode: look up from peer_state or default to Bucket.
            // The mode is supplied by the embedder via set_peer's parts map.
            let sync_mode = self
                .sync_modes
                .get(&part_id)
                .copied()
                .unwrap_or(SyncMode::Bucket);
            let Some(latest_cursor) = cursor_summary else {
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
                            part_id,
                            offset,
                            limit_hint: BucketMachine::GET_BUCKET_LIMIT_HINT,
                            since: last_peer_cursor,
                        })
                        .await??;
                    let filtered = crate::bucket::filter_buckets(
                        part_id,
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
            let diff = latest_cursor.abs_diff(last_peer_cursor);
            tracing::debug!(
                peer_id = %self.peer_id,
                ?part_id,
                remote_cursor = latest_cursor,
                stored_peer_cursor = last_peer_cursor,
                cursor_diff = diff,
                ?sync_mode,
                "decide peer part strategy",
            );
            if diff <= BucketMachine::BUCKET_DIFF_THRESHOLD {
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
                        part_id,
                        offset,
                        limit_hint: BucketMachine::GET_BUCKET_LIMIT_HINT,
                        since: last_peer_cursor,
                    })
                    .await??;
                let filtered =
                    crate::bucket::filter_buckets(part_id, working_level, buckets, &cx.part_store)
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
