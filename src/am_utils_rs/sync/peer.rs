use crate::interlude::*;

use crate::repo::SharedBigRepo;
use crate::sync::protocol::*;
use crate::sync::store::SyncStoreHandle;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Instant;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub enum SamodSyncRequest {
    RequestDocSync {
        peer_key: PeerKey,
        partition_id: PartitionId,
        doc_id: String,
        cursor: u64,
    },
    /// NOTE: this doesn't mean a request for deletion from repo (since that's not yet
    /// an avail thing in the repo). It only means to cleanup any resources associated
    /// with a document.
    DocDeleted {
        peer_key: PeerKey,
        partition_id: PartitionId,
        doc_id: String,
    },
}

#[derive(Debug, Clone)]
pub enum SamodSyncAck {
    DocSynced {
        partition_id: PartitionId,
        doc_id: String,
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

pub struct PeerSyncWorkerHandle {
    pub progress_rx: Option<broadcast::Receiver<PeerSyncProgressEvent>>,
    pub events_rx: Option<broadcast::Receiver<PeerSyncWorkerEvent>>,
}

pub struct SpawnPeerSyncWorkerArgs {
    pub local_peer: PeerKey,
    pub remote_peer: PeerKey,
    pub rpc_client: irpc::Client<PartitionSyncRpc>,
    pub local_repo: SharedBigRepo,
    pub sync_store: SyncStoreHandle,
    pub samod_sync_tx: mpsc::Sender<SamodSyncRequest>,
    pub samod_ack_rx: mpsc::Receiver<SamodSyncAck>,
    pub target_partitions: Vec<PartitionId>,
}

pub struct PeerSyncWorkerStopToken {
    cancel_token: CancellationToken,
    join_handle: tokio::task::JoinHandle<()>,
    remote_peer: PeerKey,
}

impl PeerSyncWorkerStopToken {
    pub async fn stop(self) -> Res<()> {
        debug!(remote_peer = %self.remote_peer, "stopping peer sync worker");
        self.cancel_token.cancel();
        let res = utils_rs::wait_on_handle_with_timeout(self.join_handle, Duration::from_secs(5))
            .await
            .wrap_err("failed stopping peer sync worker");
        debug!(remote_peer = %self.remote_peer, result = ?res.as_ref().map(|_| ()), "peer sync worker stop finished");
        res
    }
}

pub async fn spawn_peer_sync_worker(
    args: SpawnPeerSyncWorkerArgs,
) -> Res<(PeerSyncWorkerHandle, PeerSyncWorkerStopToken)> {
    let remote_peer_for_stop = args.remote_peer.clone();
    let (progress_tx, progress_rx) = broadcast::channel(2048);
    let (events_tx, events_rx) = broadcast::channel(256);

    let cancel_token = CancellationToken::new();
    let mut worker = PeerSyncWorker {
        local_peer: args.local_peer,
        remote_peer: args.remote_peer.clone(),
        rpc_client: args.rpc_client,
        local_repo: args.local_repo,
        sync_store: args.sync_store,
        samod_sync_tx: args.samod_sync_tx,
        samod_ack_rx: args.samod_ack_rx,
        target_partitions: args.target_partitions,
        progress_tx,
        events_tx: events_tx.clone(),
        doc_cursor_state: default(),
    };
    let fut = async move {
        let t0 = Instant::now();
        worker.emit_phase_started("list_partitions");
        let (parts, frontiers) = worker
            .get_partition_frontiers()
            .await
            .wrap_err("error during catchup")?;
        worker.emit_phase_finished("list_partitions", t0.elapsed());

        let t0 = Instant::now();
        worker.emit_phase_started("bootstrap_members");
        worker.bootstrap_members(&parts).await?;
        worker.emit_phase_finished("bootstrap_members", t0.elapsed());

        let t0 = Instant::now();
        worker.emit_phase_started("bootstrap_docs");
        worker.bootstrap_docs(&parts).await?;
        worker.emit_phase_finished("bootstrap_docs", t0.elapsed());

        worker
            .events_tx
            .send(PeerSyncWorkerEvent::Bootstrapped {
                peer: worker.remote_peer.clone(),
                partition_count: frontiers.len(),
            })
            .ok();

        let subscribe_started_at = Instant::now();

        let mut replay_phase_finished = false;
        let mut replay_phase_transition_emitted = false;
        worker.emit_phase_started("subscribe_replay");

        let reqs = {
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
            reqs
        };
        let mut rpc_rx = worker
            .rpc_client
            .server_streaming(
                SubPartitionsRpcReq {
                    peer: worker.local_peer.clone(),
                    req: SubPartitionsRequest { partitions: reqs },
                },
                DEFAULT_SUBSCRIPTION_CAPACITY,
            )
            .await
            .wrap_err("subscription rpc failed")?;

        worker
            .events_tx
            .send(PeerSyncWorkerEvent::LiveReady {
                peer: worker.remote_peer.clone(),
            })
            .ok();

        loop {
            if replay_phase_finished && !replay_phase_transition_emitted {
                worker.emit_phase_finished("subscribe_replay", subscribe_started_at.elapsed());
                worker.emit_phase_started("subscribe_tail");
                replay_phase_transition_emitted = true;
            }
            tokio::select! {
                biased;
                recv = worker.samod_ack_rx.recv() => {
                    let Some(ack) = recv else {
                        eyre::bail!("samod ack channel closed");
                    };
                    worker.handle_samod_ack(ack).await?;
                }
                recv = rpc_rx.recv() => {
                    let item = recv.map_err(|err| ferr!("subscription recv failed: {err}"))?;
                    let Some(item) = item else {
                        eyre::bail!("subscription ended");
                    };
                    worker
                        .handle_subscription_item(
                            item,
                            &mut replay_phase_finished,
                        )
                        .await?;
                }
            }
        }
    };
    let join_handle = tokio::spawn({
        let cancel_token = cancel_token.clone();
        // let cancel_token = cancel_token.clone();
        let span = tracing::info_span!("PeerSyncWorker", remote_peer = %args.remote_peer);
        async move {
            let run_res = match cancel_token.run_until_cancelled(fut).await {
                Some(out) => out,
                None => Ok(()),
            };
            // if cancel_token.is_cancelled() {
            //     debug!("peer sync worker exiting cleanly after cancellation");
            //     return;
            // }
            if let Err(err) = &run_res {
                events_tx
                    .send(PeerSyncWorkerEvent::AbnormalExit {
                        peer: args.remote_peer,
                        reason: err.to_string(),
                    })
                    .ok();
            }
            debug!(result = ?run_res.as_ref().map(|_| ()), "peer sync worker future exiting");
            run_res.unwrap();
        }
        .instrument(span)
    });

    Ok((
        PeerSyncWorkerHandle {
            progress_rx: Some(progress_rx),
            events_rx: Some(events_rx),
        },
        PeerSyncWorkerStopToken {
            cancel_token,
            join_handle,
            remote_peer: remote_peer_for_stop,
        },
    ))
}

struct PeerSyncWorker {
    local_peer: PeerKey,
    remote_peer: PeerKey,
    rpc_client: irpc::Client<PartitionSyncRpc>,
    local_repo: SharedBigRepo,
    sync_store: SyncStoreHandle,
    samod_sync_tx: mpsc::Sender<SamodSyncRequest>,
    samod_ack_rx: mpsc::Receiver<SamodSyncAck>,
    target_partitions: Vec<PartitionId>,
    progress_tx: broadcast::Sender<PeerSyncProgressEvent>,
    events_tx: broadcast::Sender<PeerSyncWorkerEvent>,
    // This tracks partition -> cursor -> docids.
    // Used for tracking events that are being processed
    // like samod syncs
    doc_cursor_state: HashMap<PartitionId, PartitionDocCursorState>,
}

#[derive(Default, Debug, Clone)]
struct ApplyDocEventsStats {
    synced_docs: u64,
    unresolved_by_partition: HashMap<PartitionId, u64>,
}

enum DocEventDecision {
    RequestSamodSync {
        partition_id: PartitionId,
        doc_id: String,
        cursor: u64,
    },
    FetchMissingDoc {
        partition_id: PartitionId,
        doc_id: String,
    },
    ForwardDelete {
        partition_id: PartitionId,
        doc_id: String,
    },
}

fn take_requested_doc(docs: Vec<FullDoc>, requested_doc_id: &str) -> Option<FullDoc> {
    docs.into_iter().find(|doc| doc.doc_id == requested_doc_id)
}

#[derive(Debug, Default)]
struct PartitionDocCursorState {
    slots: BTreeMap<u64, CursorSlotState>,
}

#[derive(Debug, Clone)]
enum CursorSlotState {
    Pending(HashSet<String>),
    Blocked,
}

impl PeerSyncWorker {
    async fn get_partition_frontiers(
        &mut self,
    ) -> Res<(Vec<PartitionId>, HashMap<PartitionId, u64>)> {
        let partitions = self
            .rpc_client
            .rpc(ListPartitionsRpcReq {
                peer: self.local_peer.clone(),
            })
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
            // FIXME: why are we removing here?
            self.doc_cursor_state.remove(id);
        }

        let targets = self.target_partitions.clone();

        Ok((targets, frontiers))
    }

    /// get all unseen member events from peer using cursor
    /// and apply them to the local partition
    async fn bootstrap_members(&self, selected: &[PartitionId]) -> Res<()> {
        loop {
            let mut req_parts = Vec::with_capacity(selected.len());
            for part in selected {
                // we refresh the latest cursor on each loop go round
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
                .wrap_err("members event rpc failed")??;

            for event in &response.events {
                self.apply_member_event(event).await?;
            }
            let mut any_more = false;
            // FIXME: why are we updating the same state in a loop?
            for page in &response.cursors {
                let existing = self
                    .sync_store
                    .get_partition_cursor(self.remote_peer.clone(), page.partition_id.clone())
                    .await?;
                self.sync_store
                    .set_partition_cursor(
                        self.remote_peer.clone(),
                        page.partition_id.clone(),
                        page.next_cursor.or(existing.member_cursor),
                        existing.doc_cursor,
                    )
                    .await?;
                any_more |= page.has_more;
            }
            if !any_more {
                break;
            }
        }
        Ok(())
    }

    /// this one is a bit tricky.
    async fn bootstrap_docs(&mut self, selected: &[PartitionId]) -> Res<()> {
        let mut synced_docs = 0_u64;
        let mut unresolved_total = 0_u64;
        for part in selected {
            loop {
                let cursor = self
                    .sync_store
                    .get_partition_cursor(self.remote_peer.clone(), part.clone())
                    .await?;
                let req_parts = vec![PartitionCursorRequest {
                    partition_id: part.clone(),
                    since: cursor.doc_cursor,
                }];
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
                    .wrap_err("doc events rpc failed")??;
                debug!(
                    event_count = response.events.len(),
                    cursor_count = response.cursors.len(),
                    "bootstrap_docs page received"
                );
                let stats = self.apply_doc_events(&response.events).await?;
                let next_cursor = self
                    .sync_store
                    .get_partition_cursor(self.remote_peer.clone(), part.clone())
                    .await?
                    .doc_cursor;
                let unresolved = stats
                    .unresolved_by_partition
                    .get(part)
                    .copied()
                    .unwrap_or_default();
                if unresolved > 0 {
                    unresolved_total = unresolved_total.saturating_add(unresolved);
                    debug!(
                        partition = %part,
                        unresolved,
                        "stopping bootstrap cursor advancement for partition due to unresolved docs"
                    );
                    break;
                }
                synced_docs = synced_docs.saturating_add(stats.synced_docs);
                let mut any_more = false;
                for page in &response.cursors {
                    any_more |= page.has_more;
                }
                self.emit_doc_sync_status(synced_docs, unresolved);
                if any_more && next_cursor == cursor.doc_cursor {
                    debug!(
                        partition = %part,
                        cursor = ?cursor.doc_cursor,
                        next_cursor = ?next_cursor,
                        "bootstrap doc cursor paused: has_more=true with unchanged cursor (likely waiting for samod ack)"
                    );
                    break;
                }
                if !any_more {
                    break;
                }
            }
        }
        self.emit_doc_sync_status(synced_docs, unresolved_total);
        Ok(())
    }

    async fn handle_subscription_item(
        &mut self,
        item: SubscriptionItem,
        replay_phase_finished: &mut bool,
    ) -> Res<()> {
        match item {
            SubscriptionItem::Lagged { dropped } => {
                eyre::bail!("partition subscription lagged; dropped={dropped}")
            }
            SubscriptionItem::ReplayComplete { stream } => {
                if stream == SubscriptionStreamKind::Doc && !*replay_phase_finished {
                    *replay_phase_finished = true;
                }
                Ok(())
            }
            SubscriptionItem::MemberEvent(event) => {
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
                self.apply_member_event(&event).await?;
                self.update_member_cursor(&event).await?;
                Ok(())
            }
            SubscriptionItem::DocEvent(event) => {
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
                let stats = self.apply_doc_events(std::slice::from_ref(&event)).await?;
                let unresolved = stats
                    .unresolved_by_partition
                    .get(&event.partition_id)
                    .copied()
                    .unwrap_or_default();
                if unresolved > 0 {
                    debug!(
                        partition = %event.partition_id,
                        cursor = %event.cursor,
                        unresolved,
                        "live doc event unresolved; keeping cursor at previous frontier"
                    );
                    return Ok(());
                }
                Ok(())
            }
        }
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

    async fn apply_doc_events(&mut self, events: &[PartitionDocEvent]) -> Res<ApplyDocEventsStats> {
        let mut out = ApplyDocEventsStats::default();
        for event in events {
            if self
                .doc_event_is_stale(&event.partition_id, event.cursor)
                .await?
            {
                warn!(
                    partition_id = %event.partition_id,
                    cursor = event.cursor,
                    "ignoring stale doc event. how did this even happen????"
                );
                continue;
            }
            self.note_doc_cursor_seen(&event.partition_id, event.cursor);
            let decision = match self.reduce_doc_event(event).await {
                Ok(decision) => decision,
                Err(err) => {
                    if let PartitionDocEventDeets::DocChanged { doc_id, .. } = &event.deets {
                        let entry = out
                            .unresolved_by_partition
                            .entry(event.partition_id.clone())
                            .or_insert(0);
                        *entry = entry.saturating_add(1);
                        warn!(
                            ?err,
                            partition = %event.partition_id,
                            doc_id,
                            cursor = event.cursor,
                            "failed reducing remote doc event"
                        );
                        continue;
                    }
                    return Err(err);
                }
            };
            self.apply_doc_event_decision(event, decision, &mut out)
                .await?;
        }
        Ok(out)
    }

    async fn reduce_doc_event(&self, event: &PartitionDocEvent) -> Res<DocEventDecision> {
        match &event.deets {
            PartitionDocEventDeets::DocChanged { doc_id, .. } => {
                let parsed = doc_id
                    .parse::<samod::DocumentId>()
                    .map_err(|err| ferr!("invalid remote doc id '{doc_id}': {err}"))?;
                let local_has_doc = self.local_repo.local_contains_document(&parsed).await?;
                if local_has_doc {
                    Ok(DocEventDecision::RequestSamodSync {
                        partition_id: event.partition_id.clone(),
                        doc_id: doc_id.clone(),
                        cursor: event.cursor,
                    })
                } else {
                    Ok(DocEventDecision::FetchMissingDoc {
                        partition_id: event.partition_id.clone(),
                        doc_id: doc_id.clone(),
                    })
                }
            }
            PartitionDocEventDeets::DocDeleted { doc_id, .. } => {
                Ok(DocEventDecision::ForwardDelete {
                    partition_id: event.partition_id.clone(),
                    doc_id: doc_id.clone(),
                })
            }
        }
    }

    async fn apply_doc_event_decision(
        &mut self,
        event: &PartitionDocEvent,
        decision: DocEventDecision,
        out: &mut ApplyDocEventsStats,
    ) -> Res<()> {
        match decision {
            DocEventDecision::RequestSamodSync {
                partition_id,
                doc_id,
                cursor,
            } => {
                debug!(
                    partition_id,
                    doc_id, cursor, "peer worker requesting samod doc sync"
                );
                self.note_doc_ack_required(&partition_id, cursor, &doc_id);
                self.samod_sync_tx
                    .send(SamodSyncRequest::RequestDocSync {
                        peer_key: self.remote_peer.clone(),
                        partition_id,
                        doc_id,
                        cursor,
                    })
                    .await
                    .map_err(|err| eyre::eyre!("samod sync channel closed: {err}"))?;
                out.synced_docs = out.synced_docs.saturating_add(1);
                Ok(())
            }
            DocEventDecision::FetchMissingDoc {
                partition_id,
                doc_id,
            } => match self.import_doc_from_remote(&doc_id).await {
                Ok(()) => {
                    debug!(
                        partition_id,
                        doc_id,
                        cursor = event.cursor,
                        "peer worker imported missing remote doc"
                    );
                    out.synced_docs = out.synced_docs.saturating_add(1);
                    self.mark_doc_cursor_resolved_immediate(&event.partition_id, event.cursor);
                    self.maybe_advance_doc_cursor(&event.partition_id).await?;
                    Ok(())
                }
                Err(err) => {
                    let entry = out
                        .unresolved_by_partition
                        .entry(event.partition_id.clone())
                        .or_insert(0);
                    *entry = entry.saturating_add(1);
                    warn!(
                        ?err,
                        partition = %event.partition_id,
                        doc_id,
                        cursor = %event.cursor,
                        "failed importing remote missing doc event"
                    );
                    Ok(())
                }
            },
            DocEventDecision::ForwardDelete {
                partition_id,
                doc_id,
            } => {
                debug!(
                    partition_id,
                    doc_id,
                    cursor = event.cursor,
                    "peer worker forwarding doc delete"
                );
                self.local_repo
                    .remove_doc_from_partition(&partition_id, &doc_id)
                    .await?;
                self.samod_sync_tx
                    .send(SamodSyncRequest::DocDeleted {
                        peer_key: self.remote_peer.clone(),
                        partition_id,
                        doc_id,
                    })
                    .await
                    .map_err(|err| eyre::eyre!("samod sync channel closed: {err}"))?;
                self.mark_doc_cursor_resolved_immediate(&event.partition_id, event.cursor);
                self.maybe_advance_doc_cursor(&event.partition_id).await?;
                Ok(())
            }
        }
    }

    async fn import_doc_from_remote(&self, doc_id: &str) -> Res<()> {
        let response = self
            .rpc_client
            .rpc(GetDocsFullRpcReq {
                peer: self.local_peer.clone(),
                req: GetDocsFullRequest {
                    doc_ids: vec![doc_id.to_owned()],
                },
            })
            .await
            .wrap_err("get_docs_full rpc failed")??;
        let Some(doc) = take_requested_doc(response.docs, doc_id) else {
            eyre::bail!("remote did not return doc '{doc_id}'");
        };
        let parsed = doc
            .doc_id
            .parse::<samod::DocumentId>()
            .map_err(|err| ferr!("invalid remote doc id '{}': {err}", doc.doc_id))?;
        let loaded = automerge::Automerge::load(&doc.automerge_save)
            .map_err(|err| ferr!("invalid automerge payload for '{}': {err}", doc.doc_id))?;
        self.local_repo
            .import_doc_fast(parsed.clone(), loaded)
            .await?;
        Ok(())
    }

    async fn update_member_cursor(&self, event: &PartitionMemberEvent) -> Res<()> {
        let existing = self
            .sync_store
            .get_partition_cursor(self.remote_peer.clone(), event.partition_id.clone())
            .await?;
        self.assert_cursor_monotonic(existing.member_cursor, event.cursor)?;
        self.sync_store
            .set_partition_cursor(
                self.remote_peer.clone(),
                event.partition_id.clone(),
                Some(event.cursor),
                existing.doc_cursor,
            )
            .await?;
        Ok(())
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
        self.progress_tx
            .send(PeerSyncProgressEvent::PhaseStarted { phase })
            .ok();
    }

    fn emit_phase_finished(&self, phase: &'static str, elapsed: Duration) {
        self.progress_tx
            .send(PeerSyncProgressEvent::PhaseFinished { phase, elapsed })
            .ok();
    }

    fn emit_doc_sync_status(&self, synced_docs: u64, remaining_docs: u64) {
        self.progress_tx
            .send(PeerSyncProgressEvent::DocSyncStatus {
                synced_docs,
                remaining_docs,
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

    async fn handle_samod_ack(&mut self, ack: SamodSyncAck) -> Res<()> {
        match ack {
            SamodSyncAck::DocSynced {
                partition_id,
                doc_id,
                cursor,
            } => {
                let existing = self
                    .sync_store
                    .get_partition_cursor(self.remote_peer.clone(), partition_id.clone())
                    .await?;
                if existing.doc_cursor.is_some_and(|current| cursor <= current) {
                    debug!(
                        partition_id,
                        doc_id,
                        cursor,
                        current = existing.doc_cursor,
                        "ignoring stale samod ack"
                    );
                    return Ok(());
                }
                debug!(
                    partition_id,
                    doc_id, cursor, "peer worker received samod ack"
                );
                let Some(state) = self.doc_cursor_state.get_mut(&partition_id) else {
                    debug!(
                        partition_id,
                        doc_id, cursor, "ignoring ack for unknown partition state"
                    );
                    return Ok(());
                };
                let Some(slot) = state.slots.get_mut(&cursor) else {
                    debug!(
                        partition_id,
                        doc_id, cursor, "ignoring ack for unknown cursor slot"
                    );
                    return Ok(());
                };
                match slot {
                    CursorSlotState::Pending(pending_doc_ids) => {
                        if !pending_doc_ids.remove(&doc_id) {
                            debug!(
                                partition_id,
                                doc_id, cursor, "ignoring ack for unknown pending doc"
                            );
                            return Ok(());
                        }
                    }
                    CursorSlotState::Blocked => {
                        debug!(
                            partition_id,
                            doc_id, cursor, "ignoring ack for blocked cursor slot"
                        );
                        return Ok(());
                    }
                }
                self.maybe_advance_doc_cursor(&partition_id).await?;
                Ok(())
            }
        }
    }

    fn note_doc_cursor_seen(&mut self, partition_id: &PartitionId, cursor: u64) {
        let state = self
            .doc_cursor_state
            .entry(partition_id.clone())
            .or_default();
        state
            .slots
            .entry(cursor)
            .or_insert(CursorSlotState::Blocked);
    }

    async fn discard_stale_doc_slots(&mut self, partition_id: &PartitionId) -> Res<()> {
        let existing = self
            .sync_store
            .get_partition_cursor(self.remote_peer.clone(), partition_id.clone())
            .await?;
        let Some(current) = existing.doc_cursor else {
            return Ok(());
        };
        let Some(state) = self.doc_cursor_state.get_mut(partition_id) else {
            return Ok(());
        };
        while state
            .slots
            .first_key_value()
            .is_some_and(|(cursor, _)| *cursor <= current)
        {
            let (stale_cursor, _) = state
                .slots
                .pop_first()
                .expect("first cursor slot should exist");
            debug!(
                partition_id,
                stale_cursor, current, "discarding stale in-memory doc cursor slot"
            );
        }
        Ok(())
    }

    fn note_doc_ack_required(&mut self, partition_id: &PartitionId, cursor: u64, doc_id: &str) {
        self.note_doc_cursor_seen(partition_id, cursor);
        let state = self
            .doc_cursor_state
            .get_mut(partition_id)
            .expect("partition cursor state should exist");
        state.slots.insert(
            cursor,
            CursorSlotState::Pending([doc_id.to_string()].into_iter().collect()),
        );
    }

    fn mark_doc_cursor_resolved_immediate(&mut self, partition_id: &PartitionId, cursor: u64) {
        self.note_doc_cursor_seen(partition_id, cursor);
        let state = self
            .doc_cursor_state
            .get_mut(partition_id)
            .expect("partition cursor state should exist");
        state
            .slots
            .insert(cursor, CursorSlotState::Pending(HashSet::new()));
    }

    async fn maybe_advance_doc_cursor(&mut self, partition_id: &PartitionId) -> Res<()> {
        self.discard_stale_doc_slots(partition_id).await?;
        let Some(state) = self.doc_cursor_state.get_mut(partition_id) else {
            return Ok(());
        };
        let mut latest_ready = None;
        while state
            .slots
            .first_key_value()
            .is_some_and(|(_, slot_state)| {
                matches!(
                    slot_state,
                    CursorSlotState::Pending(pending_doc_ids) if pending_doc_ids.is_empty()
                )
            })
        {
            let (cursor, _) = state
                .slots
                .pop_first()
                .expect("first cursor slot should exist");
            latest_ready = Some(cursor);
        }
        let Some(next_cursor) = latest_ready else {
            return Ok(());
        };
        let existing = self
            .sync_store
            .get_partition_cursor(self.remote_peer.clone(), partition_id.clone())
            .await?;
        self.assert_cursor_monotonic(existing.doc_cursor, next_cursor)?;
        self.sync_store
            .set_partition_cursor(
                self.remote_peer.clone(),
                partition_id.clone(),
                existing.member_cursor,
                Some(next_cursor),
            )
            .await?;
        debug!(partition_id, next_cursor, "peer worker advanced doc cursor");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn take_requested_doc_picks_exact_match_not_first_item() {
        let docs = vec![
            FullDoc {
                doc_id: "other-doc".to_string(),
                automerge_save: vec![1],
            },
            FullDoc {
                doc_id: "wanted-doc".to_string(),
                automerge_save: vec![2],
            },
        ];
        let selected = take_requested_doc(docs, "wanted-doc")
            .expect("expected exact doc match to be selected");
        assert_eq!(selected.doc_id, "wanted-doc");
        assert_eq!(selected.automerge_save, vec![2]);
    }
}
