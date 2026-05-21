use crate::interlude::*;

use crate::{
    bucket::{calc_working_level, BucketMachine},
    part_store::{CursorIndex, PartStoreReadOnly},
    rpc::{
        BigSyncRpcClient, BuckLevel, BucketSummary, GetChangedBucketsRequest, ListPartsError,
        PeerSummaryRequest, RpcError,
    },
    tasks::{TaskCtx, TaskResultDeets},
};

pub struct DecidePeerStrategyTask {
    pub peer_id: PeerId,
    pub parts: Set<PartId>,
}

structstruck::strike! {
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
    pub struct DecidePeerStrategyTaskError {
        pub peer_id: PeerId,
        pub deets:
            enum DecidePeerStrategyErrorDeets {
                #![derive(Debug, thiserror::Error, displaydoc::Display)]
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
        let peer_rpc = cx.rpc_clients.get(&self.peer_id).expect(ERROR_UNRECONIZED);

        let summary = peer_rpc
            .peer_summary(PeerSummaryRequest {
                parts: self.parts.clone(),
            })
            .await??;
        tracing::debug!(
            peer_id = %self.peer_id,
            part_count = summary.parts.len(),
            deepest_bucket_level = summary.deepest_bucket_level,
            "decide peer strategy summary"
        );

        let mut part_strats: Map<_, _> = default();
        for part_id in self.parts {
            let Some(part_summary) = summary.parts.get(&part_id) else {
                part_strats.insert(part_id, PeerPartStratDecision::Unkown);
                continue;
            };
            let last_peer_cursor = cx
                .part_store
                .get_peer_part_cursor(self.peer_id, part_id)
                .await;
            let diff = part_summary.latest_cursor.abs_diff(last_peer_cursor);
            if diff <= BucketMachine::BUCKET_DIFF_THRESHOLD {
                part_strats.insert(
                    part_id,
                    PeerPartStratDecision::Cursor(CursorStrat {
                        latest_cursor: part_summary.latest_cursor,
                        last_cursor: last_peer_cursor,
                    }),
                );
                continue;
            }
            let mut offset = BuckId::ROOT;
            let working_level =
                calc_working_level(part_summary.member_count, summary.deepest_bucket_level);
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
                            latest_cursor: part_summary.latest_cursor,
                            last_cursor: last_peer_cursor,
                        })
                    }
                    crate::bucket::FilteredBuckets::Handoff(buckets) => {
                        PeerPartStratDecision::Bucket(BucketStrat {
                            latest_cursor: part_summary.latest_cursor,
                            initial_filtered_buckets: buckets,
                            last_cursor: last_peer_cursor,
                            remote_depth: summary.deepest_bucket_level,
                            remote_len: part_summary.member_count,
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
