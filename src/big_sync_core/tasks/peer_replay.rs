use crate::interlude::*;

use crate::{
    mpsc,
    part_store::{CursorIndex, PartStoreReadOnly},
    rpc,
    rpc::BigSyncRpcClient,
    tasks::{MachineTaskMsg, TaskCtx, TaskId, TaskResultDeets},
};

#[derive(Debug)]
pub struct PeerReplayTask {
    pub peer_id: PeerId,
    pub parts: Map<PartId, CursorIndex>,
}

#[derive(Debug)]
pub struct PeerReplayWorkerMsg {
    pub task_id: TaskId,
    pub peer_id: PeerId,
    pub evt: rpc::SubEvent,
}

structstruck::strike! {
    #[structstruck::each[derive(Debug)]]
    pub struct PeerReplayWorkerError {
        pub peer_id: PeerId,
        pub deets:
            pub enum PeerReplayWorkerErrorDeets {
                #![derive(thiserror::Error, displaydoc::Display)]
                /// StreamClosed
                StreamClosed,
                /// {0}
                SubError(#[from] rpc::ListPartsError)
                /// {0}
                Rpc(#[from] rpc::RpcError),
                /// {0}
                MpscSend(#[from] mpsc::SendError),
                /// {0}
                MpscRecv(#[from] mpsc::RecvError),
            }
    }
}

impl PeerReplayTask {
    pub async fn run<K, PStore, Rpc, Rng>(
        self,
        cx: &mut TaskCtx<K, PStore, Rpc, Rng>,
    ) -> Result<TaskResultDeets, PeerReplayWorkerError>
    where
        K: FutureForm,
        PStore: PartStoreReadOnly<K>,
        Rpc: BigSyncRpcClient<K>,
        Rng: rand::Rng,
    {
        let peer_id = self.peer_id;
        self.run_run(cx)
            .await
            .map_err(|deets| PeerReplayWorkerError { peer_id, deets })
    }

    async fn run_run<K, PStore, Rpc, Rng>(
        self,
        cx: &mut TaskCtx<K, PStore, Rpc, Rng>,
    ) -> Result<TaskResultDeets, PeerReplayWorkerErrorDeets>
    where
        K: FutureForm,
        PStore: PartStoreReadOnly<K>,
        Rpc: BigSyncRpcClient<K>,
        Rng: rand::Rng,
    {
        let peer_rpc = cx.rpc_clients.get(&self.peer_id).expect(ERROR_UNRECONIZED);
        let rx = peer_rpc
            .sub_parts(rpc::SubPartsRequest {
                parts: self
                    .parts
                    .into_iter()
                    .map(|(part_id, cursor)| rpc::PartStreamCursorRequest { part_id, cursor })
                    .collect(),
            })
            .await??;
        loop {
            let evt = rx.recv().await;
            match evt {
                Err(_) => {
                    return Err(PeerReplayWorkerErrorDeets::StreamClosed);
                }
                Ok(evt) => {
                    cx.main_tx
                        .send(MachineTaskMsg::PeerReplayWorker(PeerReplayWorkerMsg {
                            peer_id: self.peer_id,
                            task_id: cx.task_id,
                            evt,
                        }))
                        .await?;
                }
            }
        }
    }
}
