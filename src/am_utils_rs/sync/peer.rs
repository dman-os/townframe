use crate::interlude::*;

use crate::sync::protocol::*;
use crate::sync::store::SyncStoreHandle;

use std::collections::HashMap;
use std::time::Instant;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub enum PeerSyncProgressEvent {
    PhaseStarted {
        phase: &'static str,
    },
    PhaseFinished {
        phase: &'static str,
        elapsed: Duration,
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
        reason: PeerSyncWorkerExit,
    },
    NaturalDeath {
        peer: PeerKey,
    },
}

#[derive(Debug, Clone, Error)]
pub enum PeerSyncWorkerExit {
    #[error("subscription stream closed")]
    SubscriptionStreamClosed,
    #[error("list partitions failed: {reason}")]
    ListPartitionsFailed { reason: String },
    #[error("partition cursor lookup failed: {reason}")]
    PartitionCursorLookupFailed { reason: String },
    #[error("subscription rpc failed: {reason}")]
    SubscriptionRpcFailed { reason: String },
    #[error("subscription recv failed: {reason}")]
    SubscriptionRecvFailed { reason: String },
    #[error("subscription item handling failed: {reason}")]
    SubscriptionItemFailed { reason: String },
}

#[derive(Debug, Clone)]
pub enum PeerSyncWorkerMsg {
    Progress {
        peer: PeerKey,
        event: PeerSyncProgressEvent,
    },
    SubscriptionItem {
        peer: PeerKey,
        item: SubscriptionEvent,
    },
    Event(PeerSyncWorkerEvent),
}

pub struct SpawnPeerSyncWorkerArgs<'a> {
    pub remote_peer: PeerKey,
    pub rpc_client: irpc::Client<PartitionSyncRpc>,
    pub sync_store: SyncStoreHandle,
    pub target_partitions: HashSet<PartitionId>,
    pub msg_tx: mpsc::Sender<PeerSyncWorkerMsg>,
    pub task_set: &'a utils_rs::AbortableJoinSet,
}

pub struct PeerSyncWorkerStopToken {
    task_handle: utils_rs::TaskHandle,
    cancel_token: CancellationToken,
}

impl PeerSyncWorkerStopToken {
    pub fn cancel(&self) {
        self.cancel_token.cancel();
    }
    pub async fn stop(self) -> Res<()> {
        self.cancel_token.cancel();
        self.task_handle.join(Duration::from_secs(5)).await?;
        Ok(())
    }
}

pub async fn spawn_peer_sync_worker(
    args: SpawnPeerSyncWorkerArgs<'_>,
) -> Res<PeerSyncWorkerStopToken> {
    let remote_peer_for_task = Arc::clone(&args.remote_peer);
    let msg_tx = args.msg_tx.clone();
    let mut worker = PeerSyncWorker {
        remote_peer: Arc::clone(&args.remote_peer),
        rpc_client: args.rpc_client,
        sync_store: args.sync_store,
        target_partitions: args.target_partitions,
        msg_tx: args.msg_tx,
    };
    let cancel_token = CancellationToken::new();
    let fut = {
        let cancel_token = cancel_token.clone();
        async move {
            let t0 = Instant::now();
            worker.emit_phase_started("list_partitions");
            let (parts, frontiers) = worker.get_partition_frontiers().await.map_err(|err| {
                PeerSyncWorkerExit::ListPartitionsFailed {
                    reason: err.to_string(),
                }
            })?;
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
                        .get_partition_cursor(Arc::clone(&worker.remote_peer), Arc::clone(part))
                        .await
                        .map_err(|err| PeerSyncWorkerExit::PartitionCursorLookupFailed {
                            reason: err.to_string(),
                        })?;
                    reqs.push(PartitionStreamCursorRequest {
                        partition_id: Arc::clone(part),
                        since_member: cursor.member_cursor,
                        since_event: cursor.item_cursor,
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
                    .map_err(|err| PeerSyncWorkerExit::SubscriptionRpcFailed {
                        reason: err.to_string(),
                    })?
            };

            worker
                .msg_tx
                .try_send(PeerSyncWorkerMsg::Event(PeerSyncWorkerEvent::LiveReady {
                    peer: Arc::clone(&worker.remote_peer),
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
                    _ = cancel_token.cancelled() => break,
                    recv = rpc_rx.recv() => {
                        let item = recv.map_err(|err| PeerSyncWorkerExit::SubscriptionRecvFailed {
                            reason: err.to_string(),
                        })?;
                        let Some(item) = item else {
                            return Err(PeerSyncWorkerExit::SubscriptionStreamClosed);
                        };
                        worker
                            .handle_subscription_item(
                                item,
                                frontiers.len(),
                                &mut member_replay_complete,
                                &mut item_replay_complete,
                                &mut bootstrap_emitted,
                                &mut replay_phase_finished,
                            )
                            .await
                            .map_err(|err| PeerSyncWorkerExit::SubscriptionItemFailed {
                                reason: err.to_string(),
                            })?;
                    }
                }
            }
            Ok(())
        }
    };
    let span = tracing::info_span!("PeerSyncWorker", remote_peer = %args.remote_peer);
    let wrapped = async move {
        let run_res: Result<(), PeerSyncWorkerExit> = fut.await;
        let evt = if let Err(err) = &run_res {
            PeerSyncWorkerEvent::AbnormalExit {
                peer: remote_peer_for_task,
                reason: err.clone(),
            }
        } else {
            PeerSyncWorkerEvent::NaturalDeath {
                peer: remote_peer_for_task,
            }
        };
        msg_tx.try_send(PeerSyncWorkerMsg::Event(evt)).ok();
        debug!(result = ?run_res.as_ref().map(|_| ()), "peer sync worker future exiting");
    }
    .instrument(span);

    let task_handle = args
        .task_set
        .spawn(wrapped)
        .map_err(|_| ferr!("task set aborted"))?;

    Ok(PeerSyncWorkerStopToken {
        task_handle,
        cancel_token,
    })
}

struct PeerSyncWorker {
    remote_peer: PeerKey,
    rpc_client: irpc::Client<PartitionSyncRpc>,
    sync_store: SyncStoreHandle,
    target_partitions: HashSet<PartitionId>,
    msg_tx: mpsc::Sender<PeerSyncWorkerMsg>,
}

impl PeerSyncWorker {
    async fn get_partition_frontiers(
        &mut self,
    ) -> Res<(HashSet<PartitionId>, HashMap<PartitionId, u64>)> {
        let partitions = self
            .rpc_client
            .rpc(ListPartitionsRpcReq)
            .await
            .wrap_err("list partitions rpc failed")??
            .partitions;

        let mut remote_latest_by_partition: HashMap<PartitionId, u64> = partitions
            .iter()
            .map(|item| (Arc::clone(&item.partition_id), item.latest_cursor))
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
        item: SubscriptionEvent,
        partition_count: usize,
        member_replay_complete: &mut bool,
        item_replay_complete: &mut bool,
        bootstrap_emitted: &mut bool,
        replay_phase_finished: &mut bool,
    ) -> Res<()> {
        match item {
            SubscriptionEvent::Lagged { dropped } => {
                eyre::bail!("partition subscription lagged; dropped={dropped}")
            }
            SubscriptionEvent::ReplayComplete { stream } => {
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
                                peer: Arc::clone(&self.remote_peer),
                                partition_count,
                            },
                        ))
                        .ok();
                    *bootstrap_emitted = true;
                }
                Ok(())
            }
            SubscriptionEvent::MemberEvent(event) => {
                debug!(
                    remote_peer = %self.remote_peer,
                    partition_id = %event.partition_id,
                    cursor = event.cursor,
                    deets = ?event.deets,
                    "received member subscription event"
                );
                self.msg_tx
                    .send(PeerSyncWorkerMsg::SubscriptionItem {
                        peer: Arc::clone(&self.remote_peer),
                        item: SubscriptionEvent::MemberEvent(event),
                    })
                    .await
                    .map_err(|err| eyre::eyre!("peer worker msg channel closed: {err}"))?;
                Ok(())
            }
            SubscriptionEvent::ItemEvent(event) => {
                debug!(
                    remote_peer = %self.remote_peer,
                    partition_id = %event.partition_id,
                    cursor = event.cursor,
                    deets = ?event.deets,
                    "received item subscription event"
                );
                self.msg_tx
                    .send(PeerSyncWorkerMsg::SubscriptionItem {
                        peer: Arc::clone(&self.remote_peer),
                        item: SubscriptionEvent::ItemEvent(event),
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
                peer: Arc::clone(&self.remote_peer),
                event: PeerSyncProgressEvent::PhaseStarted { phase },
            })
            .ok();
    }

    fn emit_phase_finished(&self, phase: &'static str, elapsed: Duration) {
        self.msg_tx
            .try_send(PeerSyncWorkerMsg::Progress {
                peer: Arc::clone(&self.remote_peer),
                event: PeerSyncProgressEvent::PhaseFinished { phase, elapsed },
            })
            .ok();
    }
}
