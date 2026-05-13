use crate::interlude::*;

use crate::{
    merkle::{BucketId, MerkleFingerprintSeed, MerkleLeafItem},
    part_store::PartitionStore,
    rpc,
    rpc::BigSyncRpcClient,
    tasks::{TaskCtx, TaskResultDeets},
};

pub struct MerkleLeafTask {
    pub peer_id: PeerId,
    pub part_id: PartId,
    pub path: BucketId,
    pub seed: MerkleFingerprintSeed,
}
pub struct MerkleLeafResult {
    pub peer_id: PeerId,
    pub part_id: PartId,
    pub path: BucketId,
    pub seed: MerkleFingerprintSeed,
    pub remote_items: Vec<MerkleLeafItem>,
}

structstruck::strike! {
    pub struct MerkleLeafError {
        pub peer_id: PeerId,
        pub part_id: PartId,
        pub deets:
            pub enum MerkleLeafErrorDeets {
                #![derive(Debug, thiserror::Error, displaydoc::Display)]
                /// {0}
                ListError(#[from] rpc::ListPartsError)
                /// {0}
                Rpc(#[from] rpc::RpcError),
            }
    }
}

impl MerkleLeafTask {
    pub async fn run<K: FutureForm, S: PartitionStore<K>, R: BigSyncRpcClient<K>>(
        self,
        cx: &TaskCtx<K, S, R>,
    ) -> Result<TaskResultDeets, MerkleLeafError> {
        let peer_id = self.peer_id;
        let part_id = self.part_id;
        self.run_run(cx).await.map_err(|deets| MerkleLeafError {
            peer_id,
            part_id,
            deets,
        })
    }

    async fn run_run<K: FutureForm, S: PartitionStore<K>, R: BigSyncRpcClient<K>>(
        self,
        cx: &TaskCtx<K, S, R>,
    ) -> Result<TaskResultDeets, MerkleLeafErrorDeets> {
        let peer_rpc = cx.rpc_clients.get(&self.peer_id).expect(ERROR_UNRECONIZED);
        let response = peer_rpc
            .merkle_leaf_items(rpc::MerkleLeafItemsRequest {
                part_id: self.part_id,
                path: self.path.clone(),
                seed: self.seed,
            })
            .await??;
        Ok(TaskResultDeets::MerkleLeaf(MerkleLeafResult {
            peer_id: self.peer_id,
            part_id: self.part_id,
            path: self.path,
            seed: self.seed,
            remote_items: response.items,
        }))
    }
}
