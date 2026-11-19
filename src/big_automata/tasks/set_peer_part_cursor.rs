use crate::interlude::*;

use crate::part_store::PartStoreRead;
use crate::rev::Rev;
use crate::rpc::BigSyncRpcClient;
use crate::tasks::TaskCtx;
use crate::tasks::TaskResultDeets;

#[derive(Debug, Clone)]
pub struct SetPeerPartCursorTask {
    pub peer_key: PeerKey,
    pub part_key: PartKey,
    pub cursor: Rev,
}

impl SetPeerPartCursorTask {
    #[tracing::instrument(skip(cx))]
    pub async fn run<PStore, Rpc, Rng>(self, cx: &mut TaskCtx<PStore, Rpc, Rng>) -> TaskResultDeets
    where
        PStore: PartStoreRead,
        Rpc: BigSyncRpcClient,
        Rng: rand::Rng,
    {
        self.run_run(cx).await;
        TaskResultDeets::SetPeerPartCursor(())
    }

    async fn run_run<PStore, Rpc, Rng>(self, cx: &mut TaskCtx<PStore, Rpc, Rng>)
    where
        PStore: PartStoreRead,
        Rpc: BigSyncRpcClient,
        Rng: rand::Rng,
    {
        cx.part_store
            .set_peer_part_cursor(self.peer_key.clone(), self.part_key.clone(), self.cursor)
            .await;
    }
}
