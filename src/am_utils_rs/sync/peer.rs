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
    PhaseStarted { phase: &'static str },
    PhaseFinished { phase: &'static str, elapsed: Duration },
    CursorUpdated { partition_id: PartitionId },
}

#[derive(Debug, Clone)]
pub enum PeerSyncWorkerEvent {
    Bootstrapped { peer: PeerKey, partition_count: usize },
    Live { peer: PeerKey },
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

pub async fn spawn_peer_sync_worker(
    peer: PeerKey,
    rpc_client: irpc::Client<PartitionSyncRpc>,
    local_repo: SharedBigRepo,
    sync_store: SyncStoreHandle,
    samod_sync_tx: mpsc::Sender<SamodSyncRequest>,
    target_partitions: Vec<PartitionId>,
    cancel_token: CancellationToken,
) -> Res<(PeerSyncWorkerHandle, PeerSyncWorkerStopToken)> {
    let (msg_tx, mut msg_rx) = mpsc::unbounded_channel();
    let (progress_tx, progress_rx) = broadcast::channel(2048);
    let (events_tx, events_rx) = broadcast::channel(256);
    let mut worker = PeerSyncWorker {
        peer,
        rpc_client,
        local_repo,
        sync_store,
        samod_sync_tx,
        target_partitions,
        progress_tx,
        events_tx,
        running: false,
    };

    let fut = {
        let cancel_token = cancel_token.clone();
        async move {
            loop {
                tokio::select! {
                    biased;
                    _ = cancel_token.cancelled() => break,
                    msg = msg_rx.recv() => {
                        let Some(msg) = msg else {
                            break;
                        };
                        match msg {
                            PeerMsg::Start { resp } => {
                                let out = worker.run_once().await;
                                if out.is_ok() {
                                    worker.running = true;
                                }
                                resp.send(out).inspect_err(|_| warn!(ERROR_CALLER)).ok();
                            }
                            PeerMsg::Stop { resp } => {
                                worker.running = false;
                                resp.send(Ok(())).inspect_err(|_| warn!(ERROR_CALLER)).ok();
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

struct PeerSyncWorker {
    peer: PeerKey,
    rpc_client: irpc::Client<PartitionSyncRpc>,
    local_repo: SharedBigRepo,
    sync_store: SyncStoreHandle,
    samod_sync_tx: mpsc::Sender<SamodSyncRequest>,
    target_partitions: Vec<PartitionId>,
    progress_tx: broadcast::Sender<PeerSyncProgressEvent>,
    events_tx: broadcast::Sender<PeerSyncWorkerEvent>,
    running: bool,
}

impl PeerSyncWorker {
    async fn run_once(&mut self) -> Res<()> {
        self.emit_phase_started("list_partitions");
        let t0 = Instant::now();
        let partitions = self
            .rpc_client
            .rpc(ListPartitionsRpcReq {
                peer: self.peer.clone(),
            })
            .await
            .map_err(|err| ferr!("list partitions rpc failed: {err}"))?
            .map_err(|err| err.into_report())?
            .partitions;
        self.emit_phase_finished("list_partitions", t0.elapsed());

        let available: HashSet<PartitionId> =
            partitions.into_iter().map(|item| item.partition_id).collect();
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
            return Ok(());
        }

        self.bootstrap_members(&selected).await?;
        self.bootstrap_docs(&selected).await?;
        let _ = self.events_tx.send(PeerSyncWorkerEvent::Bootstrapped {
            peer: self.peer.clone(),
            partition_count: selected.len(),
        });
        self.live_subscribe(&selected).await?;
        let _ = self.events_tx.send(PeerSyncWorkerEvent::Live {
            peer: self.peer.clone(),
        });
        Ok(())
    }

    async fn bootstrap_members(&self, selected: &[PartitionId]) -> Res<()> {
        self.emit_phase_started("bootstrap_members");
        let t0 = Instant::now();
        loop {
            let mut req_parts = Vec::with_capacity(selected.len());
            for part in selected {
                let cursor = self
                    .sync_store
                    .get_partition_cursor(self.peer.clone(), part.clone())
                    .await?;
                req_parts.push(PartitionCursorRequest {
                    partition_id: part.clone(),
                    since: cursor.member_cursor,
                });
            }
            let response = self
                .rpc_client
                .rpc(GetPartitionMemberEventsRpcReq {
                    peer: self.peer.clone(),
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
                    .get_partition_cursor(self.peer.clone(), page.partition_id.clone())
                    .await?;
                self.sync_store
                    .set_partition_cursor(
                        self.peer.clone(),
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
                    .get_partition_cursor(self.peer.clone(), part.clone())
                    .await?;
                req_parts.push(PartitionCursorRequest {
                    partition_id: part.clone(),
                    since: cursor.doc_cursor,
                });
            }
            let response = self
                .rpc_client
                .rpc(GetPartitionDocEventsRpcReq {
                    peer: self.peer.clone(),
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
                    .get_partition_cursor(self.peer.clone(), page.partition_id.clone())
                    .await?;
                self.sync_store
                    .set_partition_cursor(
                        self.peer.clone(),
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
        let mut reqs = Vec::with_capacity(selected.len());
        for part in selected {
            let cursor = self
                .sync_store
                .get_partition_cursor(self.peer.clone(), part.clone())
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
                    peer: self.peer.clone(),
                    req: SubPartitionsRequest { partitions: reqs },
                },
                DEFAULT_SUBSCRIPTION_CAPACITY,
            )
            .await
            .map_err(|err| ferr!("subscription rpc failed: {err}"))?;

        while let Ok(Some(item)) = rpc_rx.recv().await {
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
                    if stream == SubscriptionStreamKind::Doc {
                        self.emit_phase_finished("subscribe", t0.elapsed());
                        break;
                    }
                }
                SubscriptionItem::Lagged { dropped } => {
                    eyre::bail!("partition subscription lagged; dropped={dropped}");
                }
            }
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
                        let _ = self
                            .samod_sync_tx
                            .send(SamodSyncRequest::RequestDocSync {
                                peer_key: self.peer.clone(),
                                partition_id: event.partition_id.clone(),
                                doc_id: doc_id.clone(),
                                reason: "remote_doc_changed",
                            })
                            .await;
                    } else {
                        let _ = self
                            .samod_sync_tx
                            .send(SamodSyncRequest::DocMissingLocal {
                                peer_key: self.peer.clone(),
                                partition_id: event.partition_id.clone(),
                                doc_id: doc_id.clone(),
                            })
                            .await;
                        missing.push(doc_id.clone());
                    }
                }
                PartitionDocEventDeets::DocDeleted { doc_id, .. } => {
                    let _ = self
                        .samod_sync_tx
                        .send(SamodSyncRequest::DocDeleted {
                            peer_key: self.peer.clone(),
                            partition_id: event.partition_id.clone(),
                            doc_id: doc_id.clone(),
                        })
                        .await;
                }
            }
        }
        missing.sort();
        missing.dedup();
        for chunk in missing.chunks(DEFAULT_DOC_BATCH_LIMIT) {
            let docs = self
                .rpc_client
                .rpc(GetDocsFullRpcReq {
                    peer: self.peer.clone(),
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
            .get_partition_cursor(self.peer.clone(), event.partition_id.clone())
            .await?;
        self.sync_store
            .set_partition_cursor(
                self.peer.clone(),
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
            .get_partition_cursor(self.peer.clone(), event.partition_id.clone())
            .await?;
        self.sync_store
            .set_partition_cursor(
                self.peer.clone(),
                event.partition_id.clone(),
                existing.member_cursor,
                Some(event.cursor.clone()),
            )
            .await?;
        self.emit_cursor_updated(event.partition_id.clone());
        Ok(())
    }

    fn emit_phase_started(&self, phase: &'static str) {
        let _ = self.progress_tx.send(PeerSyncProgressEvent::PhaseStarted { phase });
    }

    fn emit_phase_finished(&self, phase: &'static str, elapsed: Duration) {
        let _ = self
            .progress_tx
            .send(PeerSyncProgressEvent::PhaseFinished { phase, elapsed });
    }

    fn emit_cursor_updated(&self, partition_id: PartitionId) {
        let _ = self
            .progress_tx
            .send(PeerSyncProgressEvent::CursorUpdated { partition_id });
    }
}
