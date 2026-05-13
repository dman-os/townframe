use crate::interlude::*;

use big_sync_core::{
    mpsc::Receiver,
    rpc::{
        BigSyncRpcResult, ListPartsError, PartSummary, PeerSummaryRequest, PeerSummaryResult,
        SubEvent, SubPartsRequest,
    },
    SyncCompletion, SyncTaskDeets,
};

use crate::part_store::{HostPartitionStore, MemoryPartStore};
use crate::SharedPartitionStore;

struct TestSyncBackend {}

#[async_trait]
impl crate::SyncBackend for TestSyncBackend {
    async fn run(&self, task: SyncTaskDeets) -> Res<Vec<SyncCompletion>> {
        Ok(vec![SyncCompletion::Noop {
            peer: task.peer_id,
            obj_id: task.obj_id,
        }])
    }
}

struct TestRpcClient {
    target_part_store: Arc<std::sync::Mutex<MemoryPartStore>>,
}

#[async_trait]
impl crate::rpc::HostBigRpcClient for TestRpcClient {
    async fn peer_summary(
        &self,
        req: PeerSummaryRequest,
    ) -> Res<BigSyncRpcResult<Result<PeerSummaryResult, ListPartsError>>> {
        let parts = self.target_part_store.list_partitions().await?;
        let parts = parts
            .into_iter()
            .filter(|(part_id, _)| req.parts.contains(part_id))
            .collect();
        Ok(Ok(PeerSummaryResult { parts }))
    }

    async fn sub_parts(
        &self,
        req: SubPartsRequest,
    ) -> Res<BigSyncRpcResult<Result<Receiver<SubEvent>, ListPartsError>>> {
    }
}

fn init_node() -> Res<()> {
    let part_store = MemoryPartStore::new();

    let (test_bend_id, test_bend) = (0u64, TestSyncBackend {});
    let worker = crate::spawn_big_sync_worker(
        Arc::clone(&part_store) as _,
        [(test_bend_id, Arc::new(test_bend) as _)].into(),
    )?;
}

fn test() -> Res<()> {
    Ok(())
}
