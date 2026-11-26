use crate::interlude::*;

use crate::store::bucket::BucketSummary;
use crate::{
    AllowedStrat,
    rpc::{
        BigSyncRpcClient, RpcError,
        bucket::{GetChangedBucketsError, GetChangedBucketsRequest},
        summary::{
            PeerPartStratSummary, PeerPartSummary, PeerPartsSummaryError, PeerPartsSummaryRequest,
        },
    },
    store::PartStoreReadOnly,
    tasks::{TaskCtx, TaskResultDeets},
};

#[derive(Debug, Clone)]
pub struct DecidePeerStrategyTask {
    pub peer_key: PeerKey,
    pub parts: Set<PartKey>,
    pub allowed_strat_override: Map<PartKey, AllowedStrat>,
    pub default_allowed_strat: AllowedStrat,
}

structstruck::strike! {
    #[structstruck::each[derive(Debug)]]
    pub struct SetPeerStrategy {
        pub peer_key: PeerKey,
        pub part_strats: Map<PartKey, pub enum PeerPartStratDecision{
            Unkown,
            ProtocolError,
            StratAllowedMismatch(AllowedStrat),
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
        pub peer_key: PeerKey,
        pub deets:
            enum DecidePeerStrategyErrorDeets {
                #![derive(thiserror::Error, displaydoc::Display)]
                /// {0}
                GetChangedBucketsError(#[from] GetChangedBucketsError)
                /// {0}
                PeerPartsSummaryError(#[from] PeerPartsSummaryError)
                /// {0}
                Rpc(#[from] RpcError),
            }

    }
}

impl DecidePeerStrategyTask {
    #[tracing::instrument(skip(self, cx), fields(peer_key = %self.peer_key, part_count = self.parts.len()))]
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
        let peer_key = self.peer_key.clone();
        self.run_run(cx)
            .await
            .map_err(|deets| DecidePeerStrategyTaskError { peer_key, deets })
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
        let Some(peer_rpc) = cx.rpc_clients.get(&self.peer_key) else {
            // Peer teardown can race a queued strategy task. Treat the
            // missing client as a transport failure instead of panicking.
            return Err(DecidePeerStrategyErrorDeets::Rpc(RpcError::TransportError));
        };

        // The peer counts relevance against the cursor we advertise, so gather the
        // cursors we hold for it first and reuse them as this decision's own
        // bookkeeping: the value we advertise and the value we decide with have to be
        // the same one.
        let mut part_cursors = Map::new();
        for part_key in &self.parts {
            let cursor = cx
                .part_store
                .get_peer_part_cursor(self.peer_key.clone(), part_key.clone())
                .await
                .unwrap_or(0);
            part_cursors.insert(part_key.clone(), cursor);
        }
        let summary = peer_rpc
            .get_peer_part_summaries(PeerPartsSummaryRequest {
                parts: part_cursors,
            })
            .await??;
        tracing::debug!(
            peer_key = %self.peer_key,
            part_count = summary.parts.len(),
            "decide peer strategy summary"
        );

        let mut part_strats: Map<_, _> = default();
        for part_key in self.parts {
            let Some(strat_summaries) = summary.parts.get(&part_key) else {
                error!(
                    ?part_key,
                    "peer didn't include requested part in response set"
                );
                part_strats.insert(part_key, PeerPartStratDecision::ProtocolError);
                continue;
            };
            let PeerPartSummary::ReplayAvail(strat_summaries) = strat_summaries else {
                part_strats.insert(part_key, PeerPartStratDecision::Unkown);
                continue;
            };
            if strat_summaries.is_empty() {
                error!(?part_key, "peer part has zero strats in replay avail set");
                part_strats.insert(part_key, PeerPartStratDecision::ProtocolError);
                continue;
            }

            let cursor_summary = strat_summaries.iter().find_map(|strat| match strat {
                PeerPartStratSummary::CursorPartSummary(cursor) => Some(cursor),
                _ => None,
            });
            let bucket_summary = strat_summaries.iter().find_map(|strat| match strat {
                PeerPartStratSummary::BucketPartSummary(bucket) => Some(bucket),
                _ => None,
            });

            let allowed_strat = self
                .allowed_strat_override
                .get(&part_key)
                .copied()
                .unwrap_or(self.default_allowed_strat);

            let last_peer_cursor = part_cursors.get(&part_key).copied().unwrap_or(0);
            match (cursor_summary, bucket_summary, allowed_strat) {
                (None, None, _) => {
                    error!(
                        ?part_key,
                        ?strat_summaries,
                        "peer part advertises only unkown strats"
                    );
                    part_strats.insert(part_key, PeerPartStratDecision::ProtocolError);
                    continue;
                }
                (None, Some(_), AllowedStrat::Cursor) => {
                    warn!(
                        ?part_key,
                        ?strat_summaries,
                        "peer part doesn't support crsor but only cursor allowed"
                    );
                    part_strats.insert(
                        part_key,
                        PeerPartStratDecision::StratAllowedMismatch(allowed_strat),
                    );
                    continue;
                }
                (Some(cursor), Some(_), AllowedStrat::Cursor)
                | (Some(cursor), None, AllowedStrat::CursorAndBucket)
                | (Some(cursor), None, AllowedStrat::Cursor) => {
                    part_strats.insert(
                        part_key,
                        PeerPartStratDecision::Cursor(CursorStrat {
                            latest_cursor: cursor.latest_cursor,
                            last_cursor: last_peer_cursor,
                        }),
                    );
                }
                (Some(cursor), Some(_), AllowedStrat::CursorAndBucket)
                    if cursor.dirty_count <= BucketMachine::BUCKET_DIRTY_THRESHOLD =>
                {
                    part_strats.insert(
                        part_key,
                        PeerPartStratDecision::Cursor(CursorStrat {
                            latest_cursor: cursor.latest_cursor,
                            last_cursor: last_peer_cursor,
                        }),
                    );
                }
                (None, Some(bucket), AllowedStrat::CursorAndBucket)
                | (Some(_), Some(bucket), AllowedStrat::CursorAndBucket) => {
                    let working_level =
                        calc_working_level(bucket.member_count, bucket.deepest_bucket_level);
                    let mut offset = BuckId::ROOT;
                    loop {
                        let buckets = peer_rpc
                            .get_changed_buckets(GetChangedBucketsRequest {
                                part_key: part_key.clone(),
                                offset,
                                to_level: working_level,
                                limit_hint: BucketMachine::GET_BUCKET_LIMIT_HINT,
                                since: last_peer_cursor,
                            })
                            .await??;
                        let filtered = crate::bucket::filter_buckets(
                            part_key.clone(),
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
                        part_strats.insert(part_key, strat);
                        break;
                    }
                }
            }
        }
        tracing::debug!(
            peer_key = %self.peer_key,
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
            peer_key: self.peer_key,
            part_strats,
        }))
    }
}
