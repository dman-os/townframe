use crate::interlude::*;

use crate::sync::protocol::*;
use crate::sync::store::SyncStoreHandle;

use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub enum PeerSyncProgressEvent {
    PhaseStarted {
        phase: &'static str,
    },
    PhaseFinished {
        phase: &'static str,
        elapsed: Duration,
    },
    SyncStatus {
        synced_items: u64,
        remaining_items: u64,
    },
}

#[derive(Debug, Clone)]
pub enum PeerSyncWorkerEvent {
    Bootstrapped {
        peer: PeerKey,
        partition_count: usize,
    },
    LiveReady {
        peer: PeerKey,
    },
    AbnormalExit {
        peer: PeerKey,
        reason: String,
    },
}

#[derive(Debug, Clone)]
pub enum PeerSyncWorkerMsg {
    Progress {
        peer: PeerKey,
        event: PeerSyncProgressEvent,
    },
    SubscriptionItem {
        peer: PeerKey,
        item: SubscriptionItem,
    },
    Event(PeerSyncWorkerEvent),
}

pub struct SpawnPeerSyncWorkerArgs<'a> {
    pub local_peer: PeerKey,
    pub remote_peer: PeerKey,
    pub rpc_client: irpc::Client<PartitionSyncRpc>,
    pub sync_store: SyncStoreHandle,
    pub target_partitions: Vec<PartitionId>,
    pub msg_tx: mpsc::Sender<PeerSyncWorkerMsg>,
    pub task_set: &'a utils_rs::AbortableJoinSet,
}

pub struct PeerSyncWorkerStopToken {
    task_handle: utils_rs::TaskHandle,
    remote_peer: PeerKey,
}

impl PeerSyncWorkerStopToken {
    pub async fn stop(self) -> Res<()> {
        debug!(remote_peer = %self.remote_peer, "stopping peer sync worker");
        self.task_handle.abort();
        let res = tokio::time::timeout(Duration::from_secs(5), self.task_handle.join()).await;
        debug!(remote_peer = %self.remote_peer, result = ?res.as_ref().map(|_| ()), "peer sync worker stop finished");
        Ok(())
    }
}

pub async fn spawn_peer_sync_worker(
    args: SpawnPeerSyncWorkerArgs<'_>,
) -> Res<PeerSyncWorkerStopToken> {
    let remote_peer_for_stop = args.remote_peer.clone();
    let remote_peer_for_task = args.remote_peer.clone();
    let msg_tx = args.msg_tx.clone();
    let mut worker = PeerSyncWorker {
        local_peer: args.local_peer,
        remote_peer: args.remote_peer.clone(),
        rpc_client: args.rpc_client,
        sync_store: args.sync_store,
        target_partitions: args.target_partitions,
        msg_tx: args.msg_tx,
    };
    let fut = async move {
        let t0 = Instant::now();
        worker.emit_phase_started("list_partitions");
        let (parts, frontiers) = worker
            .get_partition_frontiers()
            .await
            .wrap_err("error during catchup")?;
        worker.emit_phase_finished("list_partitions", t0.elapsed());

        let subscribe_started_at = Instant::now();

        let mut member_replay_complete = false;
        let mut item_replay_complete = false;
        let mut bootstrap_emitted = false;
        let mut replay_phase_finished = false;
        let mut replay_phase_transition_emitted = false;
        worker.emit_phase_started("subscribe_replay");

        let mut rpc_rx = {
            let mut reqs = Vec::with_capacity(parts.len());
            for part in &parts {
                let cursor = worker
                    .sync_store
                    .get_partition_cursor(worker.remote_peer.clone(), part.clone())
                    .await?;
                reqs.push(PartitionStreamCursorRequest {
                    partition_id: part.clone(),
                    since_member: cursor.member_cursor,
                    since_item: cursor.item_cursor,
                });
            }
            worker
                .rpc_client
                .server_streaming(
                    SubPartitionsRpcReq {
                        req: SubPartitionsRequest { partitions: reqs },
                    },
                    DEFAULT_SUBSCRIPTION_CAPACITY,
                )
                .await
                .wrap_err("subscription rpc failed")?
        };

        worker
            .msg_tx
            .try_send(PeerSyncWorkerMsg::Event(PeerSyncWorkerEvent::LiveReady {
                peer: worker.remote_peer.clone(),
            }))
            .ok();

        loop {
            if replay_phase_finished && !replay_phase_transition_emitted {
                worker.emit_phase_finished("subscribe_replay", subscribe_started_at.elapsed());
                worker.emit_phase_started("subscribe_tail");
                replay_phase_transition_emitted = true;
            }
            tokio::select! {
                biased;
                recv = rpc_rx.recv() => {
                    let item = recv
                        .wrap_err("subscription recv failed")?
                        .ok_or_else(|| ferr!("subscription stream closed"))?;
                    worker
                        .handle_subscription_item(
                            item,
                            frontiers.len(),
                            &mut member_replay_complete,
                            &mut item_replay_complete,
                            &mut bootstrap_emitted,
                            &mut replay_phase_finished,
                        )
                        .await?;
                }
            }
        }
        Ok(())
    };
    let span = tracing::info_span!("PeerSyncWorker", remote_peer = %args.remote_peer);
    let wrapped = async move {
        let run_res: Res<()> = fut.await;
        if let Err(err) = &run_res {
            msg_tx
                .try_send(PeerSyncWorkerMsg::Event(
                    PeerSyncWorkerEvent::AbnormalExit {
                        peer: remote_peer_for_task,
                        reason: err.to_string(),
                    },
                ))
                .ok();
        }
        debug!(result = ?run_res.as_ref().map(|_| ()), "peer sync worker future exiting");
        if let Err(err) = run_res {
            let is_closed_by_peer = err.chain().any(|cause| {
                let msg = cause.to_string();
                msg.contains("closed by peer: 0") || msg.contains("closed by peer")
            });
            if is_closed_by_peer {
                info!("peer sync worker exited after remote close");
            } else {
                warn!(?err, "peer sync worker exiting with abnormal error");
            }
        }
    }
    .instrument(span);

    let task_handle = args
        .task_set
        .spawn(wrapped)
        .map_err(|_| ferr!("task set aborted"))?;

    Ok(PeerSyncWorkerStopToken {
        task_handle,
        remote_peer: remote_peer_for_stop,
    })
}

struct PeerSyncWorker {
    local_peer: PeerKey,
    remote_peer: PeerKey,
    rpc_client: irpc::Client<PartitionSyncRpc>,
    sync_store: SyncStoreHandle,
    target_partitions: Vec<PartitionId>,
    msg_tx: mpsc::Sender<PeerSyncWorkerMsg>,
}

impl PeerSyncWorker {
    async fn get_partition_frontiers(
        &mut self,
    ) -> Res<(Vec<PartitionId>, HashMap<PartitionId, u64>)> {
        let partitions = self
            .rpc_client
            .rpc(ListPartitionsRpcReq)
            .await
            .wrap_err("list partitions rpc failed")??
            .partitions;

        let mut remote_latest_by_partition: HashMap<PartitionId, u64> = partitions
            .iter()
            .map(|item| (item.partition_id.clone(), item.latest_cursor))
            .collect();

        let mut frontiers: HashMap<PartitionId, u64> = default();
        for part in &self.target_partitions {
            let Some((id, remote_latest)) = remote_latest_by_partition.remove_entry(part) else {
                eyre::bail!("requested partition not found on peer {part:?}");
            };
            frontiers.insert(id, remote_latest);
        }
        let targets = self.target_partitions.clone();

        Ok((targets, frontiers))
    }

    async fn handle_subscription_item(
        &mut self,
        item: SubscriptionItem,
        partition_count: usize,
        member_replay_complete: &mut bool,
        item_replay_complete: &mut bool,
        bootstrap_emitted: &mut bool,
        replay_phase_finished: &mut bool,
    ) -> Res<()> {
        match item {
            SubscriptionItem::Lagged { dropped } => {
                eyre::bail!("partition subscription lagged; dropped={dropped}")
            }
            SubscriptionItem::ReplayComplete { stream } => {
                debug!(
                    remote_peer = %self.remote_peer,
                    stream = ?stream,
                    member_replay_complete = *member_replay_complete,
                    item_replay_complete = *item_replay_complete,
                    bootstrap_emitted = *bootstrap_emitted,
                    "subscription replay complete"
                );
                if stream == SubscriptionStreamKind::Member {
                    *member_replay_complete = true;
                } else if stream == SubscriptionStreamKind::Item {
                    *item_replay_complete = true;
                }
                if *member_replay_complete && *item_replay_complete && !*replay_phase_finished {
                    *replay_phase_finished = true;
                }
                if *member_replay_complete && *item_replay_complete && !*bootstrap_emitted {
                    self.msg_tx
                        .try_send(PeerSyncWorkerMsg::Event(
                            PeerSyncWorkerEvent::Bootstrapped {
                                peer: self.remote_peer.clone(),
                                partition_count,
                            },
                        ))
                        .ok();
                    *bootstrap_emitted = true;
                }
                Ok(())
            }
            SubscriptionItem::MemberEvent(event) => {
                debug!(
                    remote_peer = %self.remote_peer,
                    partition_id = %event.partition_id,
                    cursor = event.cursor,
                    deets = ?event.deets,
                    "received member subscription event"
                );
                self.msg_tx
                    .send(PeerSyncWorkerMsg::SubscriptionItem {
                        peer: self.remote_peer.clone(),
                        item: SubscriptionItem::MemberEvent(event),
                    })
                    .await
                    .map_err(|err| eyre::eyre!("peer worker msg channel closed: {err}"))?;
                Ok(())
            }
            SubscriptionItem::ItemEvent(event) => {
                debug!(
                    remote_peer = %self.remote_peer,
                    partition_id = %event.partition_id,
                    cursor = event.cursor,
                    deets = ?event.deets,
                    "received item subscription event"
                );
                self.msg_tx
                    .send(PeerSyncWorkerMsg::SubscriptionItem {
                        peer: self.remote_peer.clone(),
                        item: SubscriptionItem::ItemEvent(event),
                    })
                    .await
                    .map_err(|err| eyre::eyre!("peer worker msg channel closed: {err}"))?;
                Ok(())
            }
        }
    }

    fn emit_phase_started(&self, phase: &'static str) {
        self.msg_tx
            .try_send(PeerSyncWorkerMsg::Progress {
                peer: self.remote_peer.clone(),
                event: PeerSyncProgressEvent::PhaseStarted { phase },
            })
            .ok();
    }

    fn emit_phase_finished(&self, phase: &'static str, elapsed: Duration) {
        self.msg_tx
            .try_send(PeerSyncWorkerMsg::Progress {
                peer: self.remote_peer.clone(),
                event: PeerSyncProgressEvent::PhaseFinished { phase, elapsed },
            })
            .ok();
    }

}
