use crate::bucket::BucketObjLeafPage;
use crate::interlude::*;

use crate::{
    bucket::BucketMachine,
    fingerprint::FingerprintSeed,
    part_store::{CursorIndex, PartStoreReadOnly},
    rpc::{BigSyncRpcClient, LeafBucketRequest, LeafBucketsError, LeafBucketsRequest, RpcError},
    tasks::{TaskCtx, TaskResultDeets},
};

#[derive(Debug, Clone)]
pub struct LeafBucketsTask {
    pub peer_id: PeerId,
    pub part_id: PartId,
    pub since: CursorIndex,
    pub buckets: Vec<LeafBucketRequest>,
}

#[derive(Debug)]
pub struct LeafBucketsResult {
    pub peer_id: PeerId,
    pub filtered_objs: Map<BuckId, BucketObjLeafPage>,
}

structstruck::strike! {
    #[structstruck::each[derive(Debug)]]
    pub struct LeafBucketsTaskError {
        pub peer_id: PeerId,
        pub part_id: PartId,
        pub _deets:
            pub enum LeafBucketsErrorDeets {
                #![derive(thiserror::Error, displaydoc::Display)]
                /// {0}
                LeafErrror(#[from] LeafBucketsError)
                /// {0}
                Rpc(#[from] RpcError),
            }
    }
}

impl LeafBucketsTask {
    #[tracing::instrument(skip(self, cx), fields(peer_id = %self.peer_id, part_id = %self.part_id, since = self.since, bucket_count = self.buckets.len()))]
    pub async fn run<K, PStore, Rpc, Rng>(
        self,
        cx: &mut TaskCtx<K, PStore, Rpc, Rng>,
    ) -> Result<TaskResultDeets, LeafBucketsTaskError>
    where
        K: FutureForm,
        PStore: PartStoreReadOnly<K>,
        Rpc: BigSyncRpcClient<K>,
        Rng: rand::Rng,
    {
        let peer_id = self.peer_id;
        let part_id = self.part_id;
        self.run_run(cx)
            .await
            .map_err(|deets| LeafBucketsTaskError {
                peer_id,
                part_id,
                _deets: deets,
            })
    }

    async fn run_run<K, PStore, Rpc, Rng>(
        self,
        cx: &mut TaskCtx<K, PStore, Rpc, Rng>,
    ) -> Result<TaskResultDeets, LeafBucketsErrorDeets>
    where
        K: FutureForm,
        PStore: PartStoreReadOnly<K>,
        Rpc: BigSyncRpcClient<K>,
        Rng: rand::Rng,
    {
        let peer_rpc = cx.rpc_clients.get(&self.peer_id).expect(ERROR_UNRECONIZED);
        let seed = FingerprintSeed::new(cx.rng.next_u64(), cx.rng.next_u64());
        let response = peer_rpc
            .leaf_buckets(LeafBucketsRequest {
                part_id: self.part_id,
                since: self.since,
                buckets: self.buckets,
                seed,
                limit_hint: BucketMachine::LEAF_BUCKET_LIMIT_HINT,
            })
            .await??;
        assert_eq!(seed, response.seed);
        let filtered = crate::bucket::filter_objects(
            self.part_id,
            response.bucks,
            response.seed,
            &cx.part_store,
        )
        .await;
        tracing::debug!(
            peer_id = %self.peer_id,
            part_id = %self.part_id,
            filtered_bucket_count = filtered.len(),
            "leaf bucket batch"
        );
        Ok(TaskResultDeets::LeafBuckets(LeafBucketsResult {
            peer_id: self.peer_id,
            filtered_objs: filtered,
        }))
    }
}
