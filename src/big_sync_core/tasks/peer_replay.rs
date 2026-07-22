use crate::interlude::*;

use crate::{
    mpsc,
    part_store::PartStoreReadOnly,
    rpc,
    rpc::BigSyncRpcClient,
    tasks::{MachineTaskMsg, TaskCtx, TaskId, TaskResultDeets},
};

#[derive(Debug)]
pub struct PeerReplayTask {
    pub peer_id: PeerId,
    pub targets: Set<rpc::SubscriptionTarget>,
    pub updates: mpsc::Receiver<Set<rpc::SubscriptionTarget>>,
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
        struct ActiveSubscription {
            target: rpc::SubscriptionTarget,
            receiver: mpsc::Receiver<rpc::SubEvent>,
            replay_done: bool,
        }

        let peer_rpc = cx.rpc_clients.get(&self.peer_id).expect(ERROR_UNRECONIZED);
        let mut subscriptions = Vec::with_capacity(self.targets.len());
        for target in self.targets {
            let receiver = peer_rpc
                .sub_parts(rpc::SubPartsRequest { target })
                .await??;
            subscriptions.push(ActiveSubscription {
                target,
                receiver,
                replay_done: false,
            });
        }
        let mut replay_complete_sent = false;
        loop {
            if subscriptions.is_empty() {
                return Err(PeerReplayWorkerErrorDeets::StreamClosed);
            }
            enum Selected {
                Event(Result<rpc::SubEvent, mpsc::RecvError>, usize),
                Update(Result<Set<rpc::SubscriptionTarget>, mpsc::RecvError>),
            }
            let selected = match futures::future::select(
                futures::future::select_all(
                    subscriptions
                        .iter()
                        .map(|subscription| Box::pin(subscription.receiver.recv())),
                ),
                Box::pin(self.updates.recv()),
            )
            .await
            {
                futures::future::Either::Left((evt, _updates)) => {
                    let (evt, index, remaining_events) = evt;
                    drop(remaining_events);
                    Selected::Event(evt, index)
                }
                futures::future::Either::Right((targets, events)) => {
                    drop(events);
                    Selected::Update(targets)
                }
            };
            match selected {
                Selected::Event(evt, index) => match evt {
                    Err(_) => {
                        subscriptions.swap_remove(index);
                        if subscriptions.is_empty() {
                            return Err(PeerReplayWorkerErrorDeets::StreamClosed);
                        }
                    }
                    Ok(rpc::SubEvent::ReplayComplete) => {
                        subscriptions[index].replay_done = true;
                        if !replay_complete_sent
                            && subscriptions
                                .iter()
                                .all(|subscription| subscription.replay_done)
                        {
                            cx.main_tx
                                .send(MachineTaskMsg::PeerReplayWorker(PeerReplayWorkerMsg {
                                    peer_id: self.peer_id,
                                    task_id: cx.task_id,
                                    evt: rpc::SubEvent::ReplayComplete,
                                }))
                                .await?;
                            replay_complete_sent = true;
                        }
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
                },
                Selected::Update(targets) => {
                    let targets = targets?;
                    let old_targets: Set<_> = subscriptions
                        .iter()
                        .map(|subscription| subscription.target)
                        .collect();
                    subscriptions.retain(|subscription| targets.contains(&subscription.target));
                    for target in targets.difference(&old_targets).copied() {
                        let receiver = peer_rpc
                            .sub_parts(rpc::SubPartsRequest { target })
                            .await??;
                        subscriptions.push(ActiveSubscription {
                            target,
                            receiver,
                            replay_done: false,
                        });
                    }
                    replay_complete_sent = false;
                    if subscriptions
                        .iter()
                        .all(|subscription| subscription.replay_done)
                    {
                        cx.main_tx
                            .send(MachineTaskMsg::PeerReplayWorker(PeerReplayWorkerMsg {
                                peer_id: self.peer_id,
                                task_id: cx.task_id,
                                evt: rpc::SubEvent::ReplayComplete,
                            }))
                            .await?;
                        replay_complete_sent = true;
                    }
                }
            }
        }
    }
}
