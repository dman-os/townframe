use crate::interlude::*;

use crate::{
    bucket::BucketMachine,
    part_store::{CursorIndex, PartStore},
    rpc::{
        BigSyncRpcClient, BuckLevel, BucketSummary, GetChangedBucketsRequest, ListPartsError,
        RpcError,
    },
    tasks::{TaskCtx, TaskResultDeets},
};

pub struct ListBucketsTask {
    pub peer_id: PeerId,
    pub part_id: PartId,
    pub offset: BuckId,
    pub since: CursorIndex,
    pub working_level: BuckLevel,
}

pub struct ListBucketsResult {
    pub peer_id: PeerId,
    pub part_id: PartId,
    pub filtered_buckets: Vec<BucketSummary>,
}

structstruck::strike! {
    pub struct ListBucketsTaskError {
        pub peer_id: PeerId,
        pub part_id: PartId,
        pub deets:
            pub enum ListBucketsTaskErrorDeets {
                #![derive(Debug, thiserror::Error, displaydoc::Display)]
                /// {0}
                ListError(#[from] ListPartsError)
                /// {0}
                Rpc(#[from] RpcError),
            }
    }

}

impl ListBucketsTask {
    pub async fn run<K, PStore, Rpc, Rng>(
        self,
        cx: &mut TaskCtx<K, PStore, Rpc, Rng>,
    ) -> Result<TaskResultDeets, ListBucketsTaskError>
    where
        K: FutureForm,
        PStore: PartStore<K>,
        Rpc: BigSyncRpcClient<K>,
        Rng: rand::Rng,
    {
        let peer_id = self.peer_id;
        let part_id = self.part_id;
        self.run_run(cx)
            .await
            .map_err(|deets| ListBucketsTaskError {
                peer_id,
                part_id,
                deets,
            })
    }

    async fn run_run<K, PStore, Rpc, Rng>(
        self,
        cx: &mut TaskCtx<K, PStore, Rpc, Rng>,
    ) -> Result<TaskResultDeets, ListBucketsTaskErrorDeets>
    where
        K: FutureForm,
        PStore: PartStore<K>,
        Rpc: BigSyncRpcClient<K>,
        Rng: rand::Rng,
    {
        let peer_rpc = cx.rpc_clients.get(&self.peer_id).expect(ERROR_UNRECONIZED);
        let mut offset = self.offset;
        loop {
            let buckets = peer_rpc
                .get_changed_buckets(GetChangedBucketsRequest {
                    part_id: self.part_id,
                    offset,
                    limit_hint: BucketMachine::GET_BUCKET_LIMIT_HINT,
                    since: self.since,
                })
                .await??;
            let filtered =
                crate::bucket::filter_buckets(self.working_level, buckets, &cx.part_store).await;
            let buckets = match filtered {
                crate::bucket::FilteredBuckets::Relist(buck_id) => {
                    offset = buck_id;
                    continue;
                }
                crate::bucket::FilteredBuckets::Done => default(),
                crate::bucket::FilteredBuckets::Handoff(buckets) => buckets,
            };
            return Ok(TaskResultDeets::ListBuckets(ListBucketsResult {
                peer_id: self.peer_id,
                part_id: self.part_id,
                filtered_buckets: buckets,
            }));
        }
    }
}
