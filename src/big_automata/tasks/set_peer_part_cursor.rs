use crate::interlude::*;

use crate::{
    rpc::BigSyncRpcClient,
    store::PartStore,
    tasks::{TaskCtx, TaskResultDeets},
};

#[derive(Debug, Clone)]
pub struct SetPeerPartCursorTask {
    pub peer_key: PeerKey,
    pub part_key: PartKey,
    pub cursor: CursorIndex,
}

impl SetPeerPartCursorTask {
    #[tracing::instrument(skip(cx))]
    pub async fn run<K, PStore, Rpc, Rng>(
        self,
        cx: &mut TaskCtx<K, PStore, Rpc, Rng>,
    ) -> TaskResultDeets
    where
        K: FutureForm,
        PStore: PartStore<K>,
        Rpc: BigSyncRpcClient<K>,
        Rng: rand::Rng,
    {
        self.run_run(cx).await;
        TaskResultDeets::SetPeerPartCursor(())
    }

    async fn run_run<K, PStore, Rpc, Rng>(self, cx: &mut TaskCtx<K, PStore, Rpc, Rng>)
    where
        K: FutureForm,
        PStore: PartStore<K>,
        Rpc: BigSyncRpcClient<K>,
        Rng: rand::Rng,
    {
        cx.part_store
            .set_peer_part_cursor(self.peer_key.clone(), self.part_key.clone(), self.cursor)
            .await;
    }
}
