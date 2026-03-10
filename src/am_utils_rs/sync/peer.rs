use crate::interlude::*;

use crate::repo::SharedBigRepo;
use crate::sync::protocol::{
    GetDocsFullRpcReq, GetPartitionDocEventsRpcReq, GetPartitionMemberEventsRpcReq,
    ListPartitionsRpcReq, PartitionSyncRpc, SubPartitionsRpcReq,
};
use crate::sync::{
    GetDocsFullRequest, GetPartitionDocEventsRequest, GetPartitionMemberEventsRequest,
    PartitionCursorRequest, PartitionDocEvent, PartitionDocEventDeets, PartitionId,
    PartitionMemberEvent, PartitionMemberEventDeets, PartitionStreamCursorRequest, PeerKey,
    SubPartitionsRequest, SubscriptionItem, SubscriptionStreamKind, SyncStoreHandle,
    DEFAULT_DOC_BATCH_LIMIT, DEFAULT_EVENT_PAGE_LIMIT, DEFAULT_SUBSCRIPTION_CAPACITY,
};
use samod::DocumentId;
use std::collections::HashSet;
use std::str::FromStr;
use std::time::Instant;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub enum SamodSyncRequest {
    RequestDocSync {
        peer_key: PeerKey,
        partition_id: PartitionId,
        doc_id: String,
        reason: &'static str,
    },
    DocMissingLocal {
        peer_key: PeerKey,
        partition_id: PartitionId,
        doc_id: String,
    },
    DocDeleted {
        peer_key: PeerKey,
        partition_id: PartitionId,
        doc_id: String,
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
    CursorUpdated {
        partition_id: PartitionId,
    },
}

#[derive(Debug, Clone)]
pub enum PeerSyncWorkerEvent {
    Bootstrapped {
        peer: PeerKey,
        partition_count: usize,
    },
    Live {
        peer: PeerKey,
    },
}

pub struct PeerSyncWorkerHandle {
    msg_tx: mpsc::UnboundedSender<PeerMsg>,
    pub progress_rx: Option<broadcast::Receiver<PeerSyncProgressEvent>>,
    pub events_rx: Option<broadcast::Receiver<PeerSyncWorkerEvent>>,
}

impl PeerSyncWorkerHandle {
    pub async fn start(&self) -> Res<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.msg_tx
            .send(PeerMsg::Start { resp: resp_tx })
            .wrap_err("peer worker closed")?;
        resp_rx.await.wrap_err(ERROR_CHANNEL)?
    }

    pub async fn stop(&self) -> Res<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.msg_tx
            .send(PeerMsg::Stop { resp: resp_tx })
            .wrap_err("peer worker closed")?;
        resp_rx.await.wrap_err(ERROR_CHANNEL)?
    }
}

pub struct PeerSyncWorkerStopToken {
    cancel_token: CancellationToken,
    join_handle: tokio::task::JoinHandle<()>,
}

impl PeerSyncWorkerStopToken {
    pub async fn stop(self) -> Res<()> {
        self.cancel_token.cancel();
        utils_rs::wait_on_handle_with_timeout(self.join_handle, Duration::from_secs(1))
            .await
            .wrap_err("failed stopping peer sync worker")
    }
}

enum PeerMsg {
    Start { resp: oneshot::Sender<Res<()>> },
    Stop { resp: oneshot::Sender<Res<()>> },
}

struct RunnerTask {
    cancel_token: CancellationToken,
    join_handle: tokio::task::JoinHandle<()>,
}

pub async fn spawn_peer_sync_worker(
    local_peer: PeerKey,
    remote_peer: PeerKey,
    rpc_client: irpc::Client<PartitionSyncRpc>,
    local_repo: SharedBigRepo,
    sync_store: SyncStoreHandle,
    samod_sync_tx: mpsc::Sender<SamodSyncRequest>,
    target_partitions: Vec<PartitionId>,
) -> Res<(PeerSyncWorkerHandle, PeerSyncWorkerStopToken)> {
    let (msg_tx, mut msg_rx) = mpsc::unbounded_channel();
    let (progress_tx, progress_rx) = broadcast::channel(2048);
    let (events_tx, events_rx) = broadcast::channel(256);
    let config = PeerSyncWorkerConfig {
        local_peer,
        remote_peer,
        rpc_client,
        local_repo,
        sync_store,
        samod_sync_tx,
        target_partitions,
        progress_tx,
        events_tx,
    };

    let cancel_token = CancellationToken::new();
    let fut = {
        let cancel_token = cancel_token.clone();
        async move {
            let mut runner_task: Option<RunnerTask> = None;
            loop {
                tokio::select! {
                    biased;
                    _ = cancel_token.cancelled() => {
                        if let Some(task) = runner_task.take() {
                            task.cancel_token.cancel();
                            if let Err(err) = utils_rs::wait_on_handle_with_timeout(
                                task.join_handle,
                                Duration::from_secs(1),
                            )
                            .await
                            {
                                warn!(?err, "peer sync runner did not stop in time on cancel");
                            }
                        }
                        break;
                    }
                    msg = msg_rx.recv() => {
                        let Some(msg) = msg else {
                            break;
                        };
                        match msg {
                            PeerMsg::Start { resp } => {
                                let out = if runner_task.is_none() {
                                    let runner_cancel = cancel_token.child_token();
                                    let mut runner = config.clone_with_cancel(runner_cancel.clone());
                                    let join_handle = tokio::spawn(async move {
                                        if let Err(err) = runner.run_loop().await {
                                            warn!(?err, "peer sync runner exited with error");
                                        }
                                    });
                                    runner_task = Some(RunnerTask {
                                        cancel_token: runner_cancel,
                                        join_handle,
                                    });
                                    Ok(())
                                } else {
                                    Ok(())
                                };
                                resp.send(out).inspect_err(|_| warn!(ERROR_CALLER)).ok();
                            }
                            PeerMsg::Stop { resp } => {
                                let out = async {
                                    if let Some(task) = runner_task.take() {
                                        task.cancel_token.cancel();
                                        if let Err(err) = utils_rs::wait_on_handle_with_timeout(
                                            task.join_handle,
                                            Duration::from_secs(1),
                                        )
                                        .await
                                        {
                                            warn!(
                                                ?err,
                                                "peer sync runner did not stop in time on explicit stop"
                                            );
                                        }
                                    }
                                    Ok(())
                                }
                                .await;
                                resp.send(out).inspect_err(|_| warn!(ERROR_CALLER)).ok();
                            }
                        }
                    }
                }
            }
            eyre::Ok(())
        }
    };
    let join_handle = tokio::spawn(async { fut.await.unwrap() });

    Ok((
        PeerSyncWorkerHandle {
            msg_tx,
            progress_rx: Some(progress_rx),
            events_rx: Some(events_rx),
        },
        PeerSyncWorkerStopToken {
            cancel_token,
            join_handle,
        },
    ))
}

struct PeerSyncWorkerConfig {
    local_peer: PeerKey,
    remote_peer: PeerKey,
    rpc_client: irpc::Client<PartitionSyncRpc>,
    local_repo: SharedBigRepo,
    sync_store: SyncStoreHandle,
    samod_sync_tx: mpsc::Sender<SamodSyncRequest>,
    target_partitions: Vec<PartitionId>,
    progress_tx: broadcast::Sender<PeerSyncProgressEvent>,
    events_tx: broadcast::Sender<PeerSyncWorkerEvent>,
}

impl PeerSyncWorkerConfig {
    fn clone_with_cancel(&self, cancel_token: CancellationToken) -> PeerSyncRunner {
        PeerSyncRunner {
            local_peer: self.local_peer.clone(),
            remote_peer: self.remote_peer.clone(),
            rpc_client: self.rpc_client.clone(),
            local_repo: Arc::clone(&self.local_repo),
            sync_store: self.sync_store.clone(),
            samod_sync_tx: self.samod_sync_tx.clone(),
            target_partitions: self.target_partitions.clone(),
            progress_tx: self.progress_tx.clone(),
            events_tx: self.events_tx.clone(),
            cancel_token,
        }
    }
}

enum RunDisposition {
    Reconnect,
    Idle,
}

struct PeerSyncRunner {
    local_peer: PeerKey,
    remote_peer: PeerKey,
    rpc_client: irpc::Client<PartitionSyncRpc>,
    local_repo: SharedBigRepo,
    sync_store: SyncStoreHandle,
    samod_sync_tx: mpsc::Sender<SamodSyncRequest>,
    target_partitions: Vec<PartitionId>,
    progress_tx: broadcast::Sender<PeerSyncProgressEvent>,
    events_tx: broadcast::Sender<PeerSyncWorkerEvent>,
    cancel_token: CancellationToken,
}

impl PeerSyncRunner {
    async fn run_loop(&mut self) -> Res<()> {
        loop {
            if self.cancel_token.is_cancelled() {
                break;
            }
            let disposition = match self.run_once().await {
                Ok(disposition) => disposition,
                Err(err) => {
                    warn!(
                        ?err,
                        local_peer = ?self.local_peer,
                        remote_peer = ?self.remote_peer,
                        "peer sync cycle failed, retrying"
                    );
                    RunDisposition::Reconnect
                }
            };
            let delay = match disposition {
                RunDisposition::Reconnect => Duration::from_millis(250),
                RunDisposition::Idle => Duration::from_secs(2),
            };
            tokio::select! {
                biased;
                _ = self.cancel_token.cancelled() => break,
                _ = tokio::time::sleep(delay) => {}
            }
        }
        Ok(())
    }

    async fn run_once(&mut self) -> Res<RunDisposition> {
        self.emit_phase_started("list_partitions");
        let t0 = Instant::now();
        let partitions = self
            .rpc_client
            .rpc(ListPartitionsRpcReq {
                peer: self.local_peer.clone(),
            })
            .await
            .map_err(|err| ferr!("list partitions rpc failed: {err}"))?
            .map_err(|err| err.into_report())?
            .partitions;
        self.emit_phase_finished("list_partitions", t0.elapsed());

        let available: HashSet<PartitionId> = partitions
            .into_iter()
            .map(|item| item.partition_id)
            .collect();
        let available_count = available.len();
        let selected = if self.target_partitions.is_empty() {
            available.into_iter().collect::<Vec<_>>()
        } else {
            self.target_partitions
                .iter()
                .filter(|part| available.contains(*part))
                .cloned()
                .collect::<Vec<_>>()
        };
        if selected.is_empty() {
            debug!(
                local_peer = ?self.local_peer,
                remote_peer = ?self.remote_peer,
                target_count = self.target_partitions.len(),
                available_count,
                "peer sync worker has no selected partitions"
            );
            return Ok(RunDisposition::Idle);
        }

        self.bootstrap_members(&selected).await?;
        self.bootstrap_docs(&selected).await?;
        self.events_tx
            .send(PeerSyncWorkerEvent::Bootstrapped {
                peer: self.remote_peer.clone(),
                partition_count: selected.len(),
            })
            .expect(ERROR_CALLER);
        self.live_subscribe(&selected).await?;
        self.events_tx
            .send(PeerSyncWorkerEvent::Live {
                peer: self.remote_peer.clone(),
            })
            .expect(ERROR_CALLER);
        Ok(RunDisposition::Reconnect)
    }

    async fn bootstrap_members(&self, selected: &[PartitionId]) -> Res<()> {
        self.emit_phase_started("bootstrap_members");
        let t0 = Instant::now();
        loop {
            let mut req_parts = Vec::with_capacity(selected.len());
            for part in selected {
                let cursor = self
                    .sync_store
                    .get_partition_cursor(self.remote_peer.clone(), part.clone())
                    .await?;
                req_parts.push(PartitionCursorRequest {
                    partition_id: part.clone(),
                    since: cursor.member_cursor,
                });
            }
            let response = self
                .rpc_client
                .rpc(GetPartitionMemberEventsRpcReq {
                    peer: self.local_peer.clone(),
                    req: GetPartitionMemberEventsRequest {
                        partitions: req_parts,
                        limit: DEFAULT_EVENT_PAGE_LIMIT,
                    },
                })
                .await
                .map_err(|err| ferr!("member events rpc failed: {err}"))?
                .map_err(|err| err.into_report())?;

            for event in &response.events {
                match &event.deets {
                    PartitionMemberEventDeets::MemberUpsert { doc_id } => {
                        self.local_repo
                            .add_doc_to_partition(&event.partition_id, doc_id)
                            .await?;
                    }
                    PartitionMemberEventDeets::MemberRemoved { doc_id } => {
                        self.local_repo
                            .remove_doc_from_partition(&event.partition_id, doc_id)
                            .await?;
                    }
                }
            }
            let mut any_more = false;
            for page in &response.cursors {
                let existing = self
                    .sync_store
                    .get_partition_cursor(self.remote_peer.clone(), page.partition_id.clone())
                    .await?;
                self.sync_store
                    .set_partition_cursor(
                        self.remote_peer.clone(),
                        page.partition_id.clone(),
                        page.next_cursor.clone().or(existing.member_cursor),
                        existing.doc_cursor,
                    )
                    .await?;
                self.emit_cursor_updated(page.partition_id.clone());
                any_more |= page.has_more;
            }
            if !any_more {
                break;
            }
        }
        self.emit_phase_finished("bootstrap_members", t0.elapsed());
        Ok(())
    }

    async fn bootstrap_docs(&self, selected: &[PartitionId]) -> Res<()> {
        self.emit_phase_started("bootstrap_docs");
        let t0 = Instant::now();
        loop {
            let mut req_parts = Vec::with_capacity(selected.len());
            for part in selected {
                let cursor = self
                    .sync_store
                    .get_partition_cursor(self.remote_peer.clone(), part.clone())
                    .await?;
                req_parts.push(PartitionCursorRequest {
                    partition_id: part.clone(),
                    since: cursor.doc_cursor,
                });
            }
            let response = self
                .rpc_client
                .rpc(GetPartitionDocEventsRpcReq {
                    peer: self.local_peer.clone(),
                    req: GetPartitionDocEventsRequest {
                        partitions: req_parts,
                        limit: DEFAULT_EVENT_PAGE_LIMIT,
                    },
                })
                .await
                .map_err(|err| ferr!("doc events rpc failed: {err}"))?
                .map_err(|err| err.into_report())?;
            self.apply_doc_events(&response.events).await?;
            let mut any_more = false;
            for page in &response.cursors {
                let existing = self
                    .sync_store
                    .get_partition_cursor(self.remote_peer.clone(), page.partition_id.clone())
                    .await?;
                self.sync_store
                    .set_partition_cursor(
                        self.remote_peer.clone(),
                        page.partition_id.clone(),
                        existing.member_cursor,
                        page.next_cursor.clone().or(existing.doc_cursor),
                    )
                    .await?;
                self.emit_cursor_updated(page.partition_id.clone());
                any_more |= page.has_more;
            }
            if !any_more {
                break;
            }
        }
        self.emit_phase_finished("bootstrap_docs", t0.elapsed());
        Ok(())
    }

    async fn live_subscribe(&self, selected: &[PartitionId]) -> Res<()> {
        self.emit_phase_started("subscribe");
        let t0 = Instant::now();
        let mut subscribe_phase_finished = false;
        let mut reqs = Vec::with_capacity(selected.len());
        for part in selected {
            let cursor = self
                .sync_store
                .get_partition_cursor(self.remote_peer.clone(), part.clone())
                .await?;
            reqs.push(PartitionStreamCursorRequest {
                partition_id: part.clone(),
                since_member: cursor.member_cursor,
                since_doc: cursor.doc_cursor,
            });
        }

        let mut rpc_rx = self
            .rpc_client
            .server_streaming(
                SubPartitionsRpcReq {
                    peer: self.local_peer.clone(),
                    req: SubPartitionsRequest { partitions: reqs },
                },
                DEFAULT_SUBSCRIPTION_CAPACITY,
            )
            .await
            .map_err(|err| ferr!("subscription rpc failed: {err}"))?;

        loop {
            let item = tokio::select! {
                biased;
                _ = self.cancel_token.cancelled() => return Ok(()),
                recv = rpc_rx.recv() => recv,
            };
            let Some(item) = item? else {
                break;
            };
            match item {
                SubscriptionItem::MemberEvent(event) => {
                    self.apply_member_event(&event).await?;
                    self.update_member_cursor(&event).await?;
                }
                SubscriptionItem::DocEvent(event) => {
                    self.apply_doc_events(std::slice::from_ref(&event)).await?;
                    self.update_doc_cursor(&event).await?;
                }
                SubscriptionItem::SnapshotComplete { stream } => {
                    if stream == SubscriptionStreamKind::Doc && !subscribe_phase_finished {
                        subscribe_phase_finished = true;
                        self.emit_phase_finished("subscribe", t0.elapsed());
                    }
                }
                SubscriptionItem::Lagged { dropped } => {
                    eyre::bail!("partition subscription lagged; dropped={dropped}");
                }
            }
        }
        if !subscribe_phase_finished {
            self.emit_phase_finished("subscribe", t0.elapsed());
        }
        Ok(())
    }

    async fn apply_member_event(&self, event: &PartitionMemberEvent) -> Res<()> {
        match &event.deets {
            PartitionMemberEventDeets::MemberUpsert { doc_id } => {
                self.local_repo
                    .add_doc_to_partition(&event.partition_id, doc_id)
                    .await?;
            }
            PartitionMemberEventDeets::MemberRemoved { doc_id } => {
                self.local_repo
                    .remove_doc_from_partition(&event.partition_id, doc_id)
                    .await?;
            }
        }
        Ok(())
    }

    async fn apply_doc_events(&self, events: &[PartitionDocEvent]) -> Res<()> {
        let mut missing = Vec::<String>::new();
        for event in events {
            match &event.deets {
                PartitionDocEventDeets::DocChanged { doc_id, .. } => {
                    let found = self
                        .local_repo
                        .is_doc_present_in_partition_state(&event.partition_id, doc_id)
                        .await?;
                    if found {
                        self.samod_sync_tx
                            .send(SamodSyncRequest::RequestDocSync {
                                peer_key: self.remote_peer.clone(),
                                partition_id: event.partition_id.clone(),
                                doc_id: doc_id.clone(),
                                reason: "remote_doc_changed",
                            })
                            .await
                            .expect(ERROR_CHANNEL);
                    } else {
                        self.samod_sync_tx
                            .send(SamodSyncRequest::DocMissingLocal {
                                peer_key: self.remote_peer.clone(),
                                partition_id: event.partition_id.clone(),
                                doc_id: doc_id.clone(),
                            })
                            .await
                            .expect(ERROR_CHANNEL);
                        missing.push(doc_id.clone());
                    }
                }
                PartitionDocEventDeets::DocDeleted { doc_id, .. } => {
                    self.samod_sync_tx
                        .send(SamodSyncRequest::DocDeleted {
                            peer_key: self.remote_peer.clone(),
                            partition_id: event.partition_id.clone(),
                            doc_id: doc_id.clone(),
                        })
                        .await
                        .expect(ERROR_CHANNEL);
                }
            }
        }
        missing.sort();
        missing.dedup();
        for chunk in missing.chunks(DEFAULT_DOC_BATCH_LIMIT) {
            let docs = self
                .rpc_client
                .rpc(GetDocsFullRpcReq {
                    peer: self.local_peer.clone(),
                    req: GetDocsFullRequest {
                        doc_ids: chunk.to_vec(),
                    },
                })
                .await
                .map_err(|err| ferr!("get_docs_full rpc failed: {err}"))?
                .map_err(|err| err.into_report())?;
            for doc in docs.docs {
                let parsed = DocumentId::from_str(&doc.doc_id)
                    .map_err(|err| ferr!("invalid remote doc id '{}': {err}", doc.doc_id))?;
                let remote_doc = automerge::Automerge::load(&doc.automerge_save)
                    .map_err(|err| ferr!("failed loading remote automerge save: {err}"))?;
                if let Err(err) = self.local_repo.import_doc(parsed, remote_doc).await {
                    warn!(?err, doc_id = %doc.doc_id, "failed importing fetched doc");
                }
            }
        }
        Ok(())
    }

    async fn update_member_cursor(&self, event: &PartitionMemberEvent) -> Res<()> {
        let existing = self
            .sync_store
            .get_partition_cursor(self.remote_peer.clone(), event.partition_id.clone())
            .await?;
        self.sync_store
            .set_partition_cursor(
                self.remote_peer.clone(),
                event.partition_id.clone(),
                Some(event.cursor.clone()),
                existing.doc_cursor,
            )
            .await?;
        self.emit_cursor_updated(event.partition_id.clone());
        Ok(())
    }

    async fn update_doc_cursor(&self, event: &PartitionDocEvent) -> Res<()> {
        let existing = self
            .sync_store
            .get_partition_cursor(self.remote_peer.clone(), event.partition_id.clone())
            .await?;
        self.sync_store
            .set_partition_cursor(
                self.remote_peer.clone(),
                event.partition_id.clone(),
                existing.member_cursor,
                Some(event.cursor.clone()),
            )
            .await?;
        self.emit_cursor_updated(event.partition_id.clone());
        Ok(())
    }

    fn emit_phase_started(&self, phase: &'static str) {
        self.progress_tx
            .send(PeerSyncProgressEvent::PhaseStarted { phase })
            .expect(ERROR_CHANNEL);
    }

    fn emit_phase_finished(&self, phase: &'static str, elapsed: Duration) {
        self.progress_tx
            .send(PeerSyncProgressEvent::PhaseFinished { phase, elapsed })
            .expect(ERROR_CHANNEL);
    }

    fn emit_cursor_updated(&self, partition_id: PartitionId) {
        self.progress_tx
            .send(PeerSyncProgressEvent::CursorUpdated { partition_id })
            .expect(ERROR_CHANNEL);
    }
}
