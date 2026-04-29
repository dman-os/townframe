use crate::interlude::*;

use crate::sync::protocol::*;
use crate::sync::store::SyncStoreHandle;
use crate::DocumentId;

use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub enum DocSyncRequest {
    PartitionMemberEvent {
        peer_key: PeerKey,
        event: PartitionMemberEvent,
    },
    PartitionDocEvent {
        peer_key: PeerKey,
        event: PartitionDocEvent,
    },
    RequestDocSync {
        peer_key: PeerKey,
        partition_id: PartitionId,
        doc_id: DocumentId,
        cursor: u64,
    },
    ImportDoc {
        peer_key: PeerKey,
        partition_id: PartitionId,
        doc_id: DocumentId,
        cursor: u64,
    },
    /// NOTE: this doesn't mean a request for deletion from repo (since that's not yet
    /// an avail thing in the repo). It only means to cleanup any resources associated
    /// with a document.
    DocDeleted {
        peer_key: PeerKey,
        partition_id: PartitionId,
        doc_id: DocumentId,
        cursor: u64,
    },
}

#[derive(Debug, Clone)]
pub enum DocSyncAck {
    MemberCursorAdvanced {
        partition_id: PartitionId,
        cursor: u64,
    },
    CursorAdvanced {
        partition_id: PartitionId,
        cursor: u64,
    },
    DocSynced {
        partition_id: PartitionId,
        doc_id: DocumentId,
        cursor: u64,
    },
}

#[derive(Debug, Clone)]
pub enum PeerSyncProgressEvent {
    PhaseStarted {
        phase: &'static str,
    },
    PhaseFinished {
        phase: &'static str,
        elapsed: Duration,
    },
    DocSyncStatus {
        synced_docs: u64,
        remaining_docs: u64,
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
    Event(PeerSyncWorkerEvent),
}

pub struct SpawnPeerSyncWorkerArgs<'a> {
    pub local_peer: PeerKey,
    pub remote_peer: PeerKey,
    pub rpc_client: irpc::Client<PartitionSyncRpc>,
    pub sync_store: SyncStoreHandle,
    pub doc_sync_tx: mpsc::Sender<DocSyncRequest>,
    pub doc_ack_rx: mpsc::Receiver<DocSyncAck>,
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
        doc_sync_tx: args.doc_sync_tx,
        doc_ack_rx: args.doc_ack_rx,
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
        let mut doc_replay_complete = false;
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
                    since_doc: cursor.doc_cursor,
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
                recv = worker.doc_ack_rx.recv() => {
                    let Some(ack) = recv else {
                        debug!("doc ack channel closed; stopping peer sync worker");
                        break;
                    };
                    worker.handle_doc_ack(ack).await?;
                }
                recv = rpc_rx.recv() => {
                    let item = recv
                        .wrap_err("subscription recv failed")?
                        .ok_or_else(|| ferr!("subscription stream closed"))?;
                    worker
                        .handle_subscription_item(
                            item,
                            frontiers.len(),
                            &mut member_replay_complete,
                            &mut doc_replay_complete,
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
                .try_send(PeerSyncWorkerMsg::Event(PeerSyncWorkerEvent::AbnormalExit {
                    peer: remote_peer_for_task,
                    reason: err.to_string(),
                }))
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
    doc_sync_tx: mpsc::Sender<DocSyncRequest>,
    doc_ack_rx: mpsc::Receiver<DocSyncAck>,
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
        // persist in the store the latest frontiers
        for (id, &remote_latest) in &frontiers {
            let existing = self
                .sync_store
                .get_partition_cursor(self.remote_peer.clone(), id.clone())
                .await?;
            let next_member_cursor = existing
                .member_cursor
                .filter(|cursor| *cursor <= remote_latest);
            let next_doc_cursor = existing
                .doc_cursor
                .filter(|cursor| *cursor <= remote_latest);
            if next_member_cursor == existing.member_cursor
                && next_doc_cursor == existing.doc_cursor
            {
                continue;
            }
            self.sync_store
                .set_partition_cursor(
                    self.remote_peer.clone(),
                    id.clone(),
                    next_member_cursor,
                    next_doc_cursor,
                )
                .await?;
        }

        let targets = self.target_partitions.clone();

        Ok((targets, frontiers))
    }

    async fn handle_subscription_item(
        &mut self,
        item: SubscriptionItem,
        partition_count: usize,
        member_replay_complete: &mut bool,
        doc_replay_complete: &mut bool,
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
                    doc_replay_complete = *doc_replay_complete,
                    bootstrap_emitted = *bootstrap_emitted,
                    "subscription replay complete"
                );
                if stream == SubscriptionStreamKind::Member {
                    *member_replay_complete = true;
                } else if stream == SubscriptionStreamKind::Doc {
                    *doc_replay_complete = true;
                }
                if *member_replay_complete && *doc_replay_complete && !*replay_phase_finished {
                    *replay_phase_finished = true;
                }
                if *member_replay_complete && *doc_replay_complete && !*bootstrap_emitted {
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
                if self
                    .member_event_is_stale(&event.partition_id, event.cursor)
                    .await?
                {
                    debug!(
                        partition_id = %event.partition_id,
                        cursor = event.cursor,
                        "ignoring stale member subscription event"
                    );
                    return Ok(());
                }
                self.doc_sync_tx
                    .send(DocSyncRequest::PartitionMemberEvent {
                        peer_key: self.remote_peer.clone(),
                        event,
                    })
                    .await
                    .map_err(|err| eyre::eyre!("doc sync channel closed: {err}"))?;
                Ok(())
            }
            SubscriptionItem::DocEvent(event) => {
                debug!(
                    remote_peer = %self.remote_peer,
                    partition_id = %event.partition_id,
                    cursor = event.cursor,
                    deets = ?event.deets,
                    "received doc subscription event"
                );
                if self
                    .doc_event_is_stale(&event.partition_id, event.cursor)
                    .await?
                {
                    debug!(
                        partition_id = %event.partition_id,
                        cursor = event.cursor,
                        "ignoring stale doc subscription event"
                    );
                    return Ok(());
                }
                self.doc_sync_tx
                    .send(DocSyncRequest::PartitionDocEvent {
                        peer_key: self.remote_peer.clone(),
                        event,
                    })
                    .await
                    .map_err(|err| eyre::eyre!("doc sync channel closed: {err}"))?;
                Ok(())
            }
        }
    }

    async fn member_event_is_stale(&self, partition_id: &PartitionId, cursor: u64) -> Res<bool> {
        let existing = self
            .sync_store
            .get_partition_cursor(self.remote_peer.clone(), partition_id.clone())
            .await?;
        Ok(existing
            .member_cursor
            .is_some_and(|current| cursor <= current))
    }

    async fn doc_event_is_stale(&self, partition_id: &PartitionId, cursor: u64) -> Res<bool> {
        let existing = self
            .sync_store
            .get_partition_cursor(self.remote_peer.clone(), partition_id.clone())
            .await?;
        Ok(existing.doc_cursor.is_some_and(|current| cursor <= current))
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

    fn assert_cursor_monotonic(&self, current: Option<u64>, next: u64) -> Res<()> {
        let Some(current) = current else {
            return Ok(());
        };
        if next < current {
            eyre::bail!("cursor regression detected: current={current} next={next}");
        }
        Ok(())
    }

    async fn handle_doc_ack(&mut self, ack: DocSyncAck) -> Res<()> {
        match ack {
            DocSyncAck::MemberCursorAdvanced {
                partition_id,
                cursor,
            } => {
                self.apply_member_cursor_advance_ack(partition_id, cursor)
                    .await
            }
            DocSyncAck::CursorAdvanced {
                partition_id,
                cursor,
            }
            | DocSyncAck::DocSynced {
                partition_id,
                doc_id: _,
                cursor,
            } => self.apply_cursor_advance_ack(partition_id, cursor).await,
        }
    }

    async fn apply_member_cursor_advance_ack(
        &mut self,
        partition_id: PartitionId,
        cursor: u64,
    ) -> Res<()> {
        let existing = self
            .sync_store
            .get_partition_cursor(self.remote_peer.clone(), partition_id.clone())
            .await?;
        if existing
            .member_cursor
            .is_some_and(|current| cursor <= current)
        {
            debug!(
                partition_id,
                cursor,
                current = existing.member_cursor,
                "ignoring stale member cursor advance ack"
            );
            return Ok(());
        }
        self.assert_cursor_monotonic(existing.member_cursor, cursor)?;
        self.sync_store
            .set_partition_cursor(
                self.remote_peer.clone(),
                partition_id,
                Some(cursor),
                existing.doc_cursor,
            )
            .await?;
        Ok(())
    }

    async fn apply_cursor_advance_ack(
        &mut self,
        partition_id: PartitionId,
        cursor: u64,
    ) -> Res<()> {
        let existing = self
            .sync_store
            .get_partition_cursor(self.remote_peer.clone(), partition_id.clone())
            .await?;
        if existing.doc_cursor.is_some_and(|current| cursor <= current) {
            debug!(
                partition_id,
                cursor,
                current = existing.doc_cursor,
                "ignoring stale cursor advance ack"
            );
            return Ok(());
        }
        self.assert_cursor_monotonic(existing.doc_cursor, cursor)?;
        self.sync_store
            .set_partition_cursor(
                self.remote_peer.clone(),
                partition_id.clone(),
                existing.member_cursor,
                Some(cursor),
            )
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::store::spawn_sync_store;
    use crate::sync::store::SyncStoreStopToken;
    use sqlx::sqlite::SqlitePoolOptions;

    async fn make_worker() -> Res<(PeerSyncWorker, SyncStoreStopToken)> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await?;
        let (sync_store, stop) = spawn_sync_store(pool).await?;
        let (rpc_tx, _rpc_rx) = mpsc::channel(1);
        let (doc_sync_tx, _doc_sync_rx) = mpsc::channel(1);
        let (_doc_ack_tx, doc_ack_rx) = mpsc::channel(1);
        let (msg_tx, _msg_rx) = mpsc::channel(1);
        let worker = PeerSyncWorker {
            local_peer: "local".into(),
            remote_peer: "remote".into(),
            rpc_client: irpc::Client::<PartitionSyncRpc>::local(rpc_tx),
            sync_store,
            doc_sync_tx,
            doc_ack_rx,
            target_partitions: Vec::new(),
            msg_tx,
        };
        Ok((worker, stop))
    }

    #[tokio::test]
    async fn apply_cursor_advance_ack_updates_persisted_cursor() -> Res<()> {
        let (mut worker, stop): (PeerSyncWorker, SyncStoreStopToken) = make_worker().await?;
        let partition_id: PartitionId = "p-doc".into();

        worker
            .apply_cursor_advance_ack(partition_id.clone(), 8)
            .await?;

        let cursor = worker
            .sync_store
            .get_partition_cursor(worker.remote_peer.clone(), partition_id)
            .await?;
        assert_eq!(cursor.doc_cursor, Some(8));

        stop.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn apply_cursor_advance_ack_ignores_stale_cursor() -> Res<()> {
        let (mut worker, stop): (PeerSyncWorker, SyncStoreStopToken) = make_worker().await?;
        let partition_id: PartitionId = "p-stale".into();

        worker
            .sync_store
            .set_partition_cursor(
                worker.remote_peer.clone(),
                partition_id.clone(),
                None,
                Some(9),
            )
            .await?;
        worker
            .apply_cursor_advance_ack(partition_id.clone(), 4)
            .await?;

        let cursor = worker
            .sync_store
            .get_partition_cursor(worker.remote_peer.clone(), partition_id)
            .await?;
        assert_eq!(cursor.doc_cursor, Some(9));

        stop.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn apply_member_cursor_advance_ack_updates_member_cursor_independently() -> Res<()> {
        let (mut worker, stop): (PeerSyncWorker, SyncStoreStopToken) = make_worker().await?;
        let partition_id: PartitionId = "p-member".into();

        worker
            .sync_store
            .set_partition_cursor(
                worker.remote_peer.clone(),
                partition_id.clone(),
                None,
                Some(3),
            )
            .await?;
        worker
            .apply_member_cursor_advance_ack(partition_id.clone(), 7)
            .await?;

        let cursor = worker
            .sync_store
            .get_partition_cursor(worker.remote_peer.clone(), partition_id)
            .await?;
        assert_eq!(cursor.member_cursor, Some(7));
        assert_eq!(cursor.doc_cursor, Some(3));

        stop.stop().await?;
        Ok(())
    }
}
