use crate::interlude::*;

use crate::{
    part_store::PartStoreReadOnly,
    rpc,
    rpc::BigSyncRpcClient,
    tasks::{TaskCtx, TaskResultDeets},
};

/// One bounded page of a single target's events.
///
/// The task is discrete: it asks for one page and completes. Re-issuing is the
/// machine's job, so a page cannot outlive the connection and a client's
/// processing rate is what decides how fast events arrive (ADR 012 decision 9).
#[derive(Debug, Clone)]
pub struct ReplayPageTask {
    pub peer_id: PeerKey,
    pub target: rpc::SubscriptionTarget,
    pub limit: u32,
}

#[derive(Debug)]
pub struct ReplayPageResult {
    pub peer_id: PeerKey,
    pub target: rpc::SubscriptionTarget,
    pub outcome: rpc::ReplayPageOutcome,
}

structstruck::strike! {
    #[structstruck::each[derive(Debug)]]
    pub struct ReplayPageTaskError {
        pub peer_id: PeerKey,
        pub target: rpc::SubscriptionTarget,
        pub deets:
            pub enum ReplayPageTaskErrorDeets {
                #![derive(thiserror::Error, displaydoc::Display)]
                /// {0}
                Rpc(#[from] rpc::RpcError),
            }
    }
}

impl ReplayPageTask {
    /// Events per page. A knob rather than a threshold, but it was measured: on
    /// the 100k-object catchup case, 256-event pages took 64s against 31s for
    /// 1024, because every page costs a fresh responder-side subscription. The
    /// number exists to cap a page, so it should not be small.
    pub const LIMIT: u32 = 1024;

    /// How long the client is willing to have a request held while its target is
    /// caught up. The client's processing rate is the flow control, so this is
    /// the client's own pacing choice; the responder caps it.
    pub const HOLD_MS: u32 = 15_000;

    pub async fn run<K, PStore, Rpc, Rng>(
        self,
        cx: &mut TaskCtx<K, PStore, Rpc, Rng>,
    ) -> Result<TaskResultDeets, ReplayPageTaskError>
    where
        K: FutureForm,
        PStore: PartStoreReadOnly<K>,
        Rpc: BigSyncRpcClient<K>,
        Rng: rand::Rng,
    {
        let peer_id = self.peer_id.clone();
        let target = self.target.clone();
        self.run_run(cx).await.map_err(|deets| ReplayPageTaskError {
            peer_id,
            target,
            deets,
        })
    }

    async fn run_run<K, PStore, Rpc, Rng>(
        self,
        cx: &mut TaskCtx<K, PStore, Rpc, Rng>,
    ) -> Result<TaskResultDeets, ReplayPageTaskErrorDeets>
    where
        K: FutureForm,
        PStore: PartStoreReadOnly<K>,
        Rpc: BigSyncRpcClient<K>,
        Rng: rand::Rng,
    {
        let Some(peer_rpc) = cx.rpc_clients.get(&self.peer_id) else {
            // No client for this peer means the route is gone, which is a
            // transport failure rather than an empty page.
            return Err(ReplayPageTaskErrorDeets::Rpc(rpc::RpcError::TransportError));
        };
        let outcome = peer_rpc
            .replay_page(rpc::ReplayPageRequest {
                target: self.target.clone(),
                limit: self.limit,
                hold_ms: Self::HOLD_MS,
            })
            .await?;
        Ok(TaskResultDeets::ReplayPage(ReplayPageResult {
            peer_id: self.peer_id,
            target: self.target,
            outcome,
        }))
    }
}
