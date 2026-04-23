//! This is just a crappy reimpl of samod to get subduction working

use crate::interlude::*;
use crate::partition::PartitionStore;
use crate::repo::{BigRepoChangeOrigin, DocumentId, PeerId};

use core::convert::Infallible;
use futures::future::BoxFuture;
use sedimentree_core::collections::Set;
use sedimentree_core::commit::{CommitStore, CountLeadingZeroBytes, FragmentState};
use sedimentree_core::crypto::digest::Digest;
use sedimentree_core::sedimentree::{Sedimentree, SedimentreeItem};
use sedimentree_core::{blob::Blob, id::SedimentreeId, loose_commit::id::CommitId};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::changes;
use subduction_core::subduction::request::FragmentRequested;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncDocOutcome {
    Success,
    NotFoundOrUnauthorized,
    TransportError,
    IoError,
}

enum RuntimeCmd {
    PutDoc {
        doc_id: DocumentId,
        initial_content: automerge::Automerge,
        done: oneshot::Sender<Res<Arc<crate::repo::LiveDocBundle>>>,
    },
    GetDocHandle {
        doc_id: DocumentId,
        done: oneshot::Sender<Res<Option<Arc<crate::repo::LiveDocBundle>>>>,
    },
    ExportDocSave {
        doc_id: DocumentId,
        done: oneshot::Sender<Res<Option<Vec<u8>>>>,
    },
    CommitDelta {
        doc_id: DocumentId,
        commits: Vec<(CommitId, BTreeSet<CommitId>, Vec<u8>)>,
        heads: Vec<automerge::ChangeHash>,
        patches: Vec<automerge::Patch>,
        origin: BigRepoChangeOrigin,
        done: oneshot::Sender<Res<()>>,
    },
    ConnectOutgoing {
        endpoint: iroh::Endpoint,
        endpoint_addr: iroh::EndpointAddr,
        peer_id: PeerId,
        done: oneshot::Sender<Res<()>>,
    },
    AcceptIncoming {
        quic_conn: iroh::endpoint::Connection,
        done: oneshot::Sender<Res<PeerId>>,
    },
    CloseConnection {
        peer_id: PeerId,
        done: Option<oneshot::Sender<Res<()>>>,
    },
    SyncDocWithPeer {
        doc_id: DocumentId,
        peer_id: PeerId,
        subscribe: bool,
        timeout: Option<Duration>,
        done: oneshot::Sender<Res<SyncDocOutcome>>,
    },
    ReleaseDocLease {
        doc_id: DocumentId,
    },
}

enum RuntimeEvt {
    RemoteHeadsObserved {
        doc_id: DocumentId,
        peer_id: PeerId,
    },
    LiveDocRefreshLoaded {
        doc_id: DocumentId,
        peer_id: PeerId,
        snapshot: Res<Option<automerge::Automerge>>,
    },
    FragmentRequestsReady {
        doc_id: DocumentId,
        requests: Vec<FragmentRequested>,
    },
    FragmentProcessingFinished {
        doc_id: DocumentId,
    },
    PutDocSucceeded {
        doc_id: DocumentId,
    },
    PutDocFailed {
        doc_id: DocumentId,
        error: String,
    },
    ConnectionEstablished {
        peer_id: PeerId,
        stop_token: RuntimePeerConnectionStopToken,
    },
    ConnectionLost {
        peer_id: PeerId,
    },
}

#[derive(Clone, Debug)]
pub(super) struct BigRepoRuntimeHandle {
    cmd_tx: mpsc::UnboundedSender<RuntimeCmd>,
}

pub(super) struct BigRepoRuntimeStopToken {
    cancel_token: CancellationToken,
    done_rx: tokio::sync::Mutex<Option<oneshot::Receiver<()>>>,
}

#[derive(Clone, Debug)]
pub(super) struct RuntimeDocLease {
    runtime: BigRepoRuntimeHandle,
    doc_id: DocumentId,
}

struct PendingPutDoc {
    initial_content: automerge::Automerge,
    done: oneshot::Sender<Res<Arc<crate::repo::LiveDocBundle>>>,
}

impl Drop for RuntimeDocLease {
    fn drop(&mut self) {
        self.runtime.release_doc_lease(self.doc_id);
    }
}

#[derive(Clone)]
pub(super) struct RuntimePeerConnectionStopToken {
    cancel_token: CancellationToken,
}

impl RuntimePeerConnectionStopToken {
    fn new() -> Self {
        Self {
            cancel_token: CancellationToken::new(),
        }
    }

    fn cancel(&self) {
        self.cancel_token.cancel();
    }

    fn child_token(&self) -> CancellationToken {
        self.cancel_token.child_token()
    }

    fn is_cancelled(&self) -> bool {
        self.cancel_token.is_cancelled()
    }
}

type SubductionSedimentrees =
    Arc<subduction_core::sharded_map::ShardedMap<SedimentreeId, Sedimentree, 256>>;
type RuntimeIrohTransport = subduction_core::transport::message::MessageTransport<
    subduction_iroh::transport::IrohTransport,
>;
type RuntimeSyncHandler<S> = subduction_core::handler::sync::SyncHandler<
    future_form::Sendable,
    S,
    RuntimeIrohTransport,
    subduction_core::policy::open::OpenPolicy,
    sedimentree_core::depth::CountLeadingZeroBytes,
    256,
    RuntimeRemoteHeadsBridge,
>;
type RuntimeSubduction<S> = subduction_core::subduction::Subduction<
    'static,
    future_form::Sendable,
    S,
    RuntimeIrohTransport,
    RuntimeSyncHandler<S>,
    subduction_core::policy::open::OpenPolicy,
    subduction_crypto::signer::memory::MemorySigner,
    subduction_websocket::tokio::TimeoutTokio,
    sedimentree_core::depth::CountLeadingZeroBytes,
    256,
>;
pub trait RuntimeSubductionStorage:
    subduction_core::storage::traits::Storage<
        future_form::Sendable,
        Error: std::fmt::Display + Send + Sync + 'static,
    > + Clone
    + Send
    + Sync
    + std::fmt::Debug
    + 'static
{
}
impl<T> RuntimeSubductionStorage for T where
    T: subduction_core::storage::traits::Storage<
            future_form::Sendable,
            Error: std::fmt::Display + Send + Sync + 'static,
        > + Clone
        + Send
        + Sync
        + std::fmt::Debug
        + 'static
{
}

#[derive(Clone)]
struct RuntimeRemoteHeadsBridge {
    evt_tx: mpsc::UnboundedSender<RuntimeEvt>,
}

impl subduction_core::remote_heads::RemoteHeadsObserver for RuntimeRemoteHeadsBridge {
    fn on_remote_heads(
        &self,
        id: SedimentreeId,
        peer: subduction_core::peer::id::PeerId,
        _heads: subduction_core::remote_heads::RemoteHeads,
    ) {
        self.evt_tx
            .send(RuntimeEvt::RemoteHeadsObserved {
                doc_id: id.into(),
                peer_id: peer.into(),
            })
            .expect(ERROR_CHANNEL);
    }
}

impl BigRepoRuntimeHandle {
    pub(super) async fn put_doc(
        &self,
        doc_id: DocumentId,
        initial_content: automerge::Automerge,
    ) -> Res<Arc<crate::repo::LiveDocBundle>> {
        let (done_tx, done_rx) = oneshot::channel();
        self.cmd_tx
            .send(RuntimeCmd::PutDoc {
                doc_id,
                initial_content,
                done: done_tx,
            })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        done_rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?
    }

    pub(super) async fn get_doc_handle(
        &self,
        doc_id: DocumentId,
    ) -> Res<Option<Arc<crate::repo::LiveDocBundle>>> {
        let (done_tx, done_rx) = oneshot::channel();
        self.cmd_tx
            .send(RuntimeCmd::GetDocHandle {
                doc_id,
                done: done_tx,
            })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        done_rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?
    }

    pub(super) async fn export_doc_save(&self, doc_id: DocumentId) -> Res<Option<Vec<u8>>> {
        let (done_tx, done_rx) = oneshot::channel();
        self.cmd_tx
            .send(RuntimeCmd::ExportDocSave {
                doc_id,
                done: done_tx,
            })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        done_rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?
    }

    pub(super) async fn commit_delta(
        &self,
        doc_id: DocumentId,
        commits: Vec<(CommitId, BTreeSet<CommitId>, Vec<u8>)>,
        heads: Vec<automerge::ChangeHash>,
        patches: Vec<automerge::Patch>,
        origin: BigRepoChangeOrigin,
    ) -> Res<()> {
        let (done_tx, done_rx) = oneshot::channel();
        self.cmd_tx
            .send(RuntimeCmd::CommitDelta {
                doc_id,
                commits,
                heads,
                patches,
                origin,
                done: done_tx,
            })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        done_rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?
    }

    pub(super) async fn ensure_peer_connection(
        &self,
        endpoint: iroh::Endpoint,
        endpoint_addr: iroh::EndpointAddr,
        peer_id: PeerId,
    ) -> Res<()> {
        let (done_tx, done_rx) = oneshot::channel();
        self.cmd_tx
            .send(RuntimeCmd::ConnectOutgoing {
                endpoint,
                endpoint_addr,
                peer_id,
                done: done_tx,
            })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        done_rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?
    }

    pub(super) async fn accept_incoming_connection(
        &self,
        quic_conn: iroh::endpoint::Connection,
    ) -> Res<PeerId> {
        let (done_tx, done_rx) = oneshot::channel();
        self.cmd_tx
            .send(RuntimeCmd::AcceptIncoming {
                quic_conn,
                done: done_tx,
            })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        done_rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?
    }

    pub(super) async fn close_peer_connection(&self, peer_id: PeerId) -> Res<()> {
        let (done_tx, done_rx) = oneshot::channel();
        self.cmd_tx
            .send(RuntimeCmd::CloseConnection {
                peer_id,
                done: Some(done_tx),
            })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        done_rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?
    }

    pub(super) async fn sync_doc_with_peer(
        &self,
        doc_id: DocumentId,
        peer_id: PeerId,
        subscribe: bool,
        timeout: Option<Duration>,
    ) -> Res<SyncDocOutcome> {
        let (done_tx, done_rx) = oneshot::channel();
        self.cmd_tx
            .send(RuntimeCmd::SyncDocWithPeer {
                doc_id,
                peer_id,
                subscribe,
                timeout,
                done: done_tx,
            })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        done_rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?
    }

    fn release_doc_lease(&self, doc_id: DocumentId) {
        if self
            .cmd_tx
            .send(RuntimeCmd::ReleaseDocLease { doc_id })
            .is_err()
        {
            debug!(%doc_id, "runtime stopped before releasing doc lease");
        }
    }

    pub(super) fn request_close_peer_connection(&self, peer_id: PeerId) {
        if self
            .cmd_tx
            .send(RuntimeCmd::CloseConnection {
                peer_id,
                done: None,
            })
            .is_err()
        {
            debug!(%peer_id, "runtime stopped before closing peer connection");
        }
    }
}

pub(super) fn spawn_big_repo_runtime<S>(
    join_set: Arc<utils_rs::AbortableJoinSet>,
    signer: subduction_crypto::signer::memory::MemorySigner,
    storage: S,
    partition_store: Arc<PartitionStore>,
    change_manager: Arc<changes::ChangeListenerManager>,
) -> Res<(BigRepoRuntimeHandle, BigRepoRuntimeStopToken)>
where
    S: RuntimeSubductionStorage,
{
    use subduction_core::{policy::open::OpenPolicy, subduction::Subduction};
    use subduction_websocket::tokio::{TimeoutTokio, TokioSpawn};

    let policy = Arc::new(OpenPolicy);
    let connect_signer = signer.clone();
    let local_peer_id =
        subduction_core::peer::id::PeerId::new(*connect_signer.verifying_key().as_bytes());
    let nonce_cache = Arc::new(subduction_core::nonce_cache::NonceCache::new(
        Duration::from_secs(60),
    ));
    let storage_for_reads = storage.clone();
    let runtime_stop = CancellationToken::new();
    let (done_tx, done_rx) = oneshot::channel();

    // FIXME: question, why not use the normal join set for this?
    // is this to allow stopping the runtime tasks from the main
    // select loop?
    let runtime_tasks = Arc::new(utils_rs::AbortableJoinSet::new());

    let sedimentrees = Arc::new(subduction_core::sharded_map::ShardedMap::new());
    let connections = Arc::new(async_lock::Mutex::new(
        sedimentree_core::collections::Map::new(),
    ));
    let subscriptions = Arc::new(async_lock::Mutex::new(
        sedimentree_core::collections::Map::new(),
    ));
    let pending_blob_requests = Arc::new(async_lock::Mutex::new(
        subduction_core::subduction::pending_blob_requests::PendingBlobRequests::new(256),
    ));

    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<RuntimeCmd>();
    let (evt_tx, mut evt_rx) = mpsc::unbounded_channel::<RuntimeEvt>();
    let observer = RuntimeRemoteHeadsBridge {
        evt_tx: evt_tx.clone(),
    };
    let storage_powerbox = subduction_core::storage::powerbox::StoragePowerbox::new(
        storage.clone(),
        Arc::clone(&policy),
    );
    let sync_handler = subduction_core::handler::sync::SyncHandler::with_remote_heads_observer(
        Arc::clone(&sedimentrees),
        Arc::clone(&connections),
        Arc::clone(&subscriptions),
        storage_powerbox.clone(),
        Arc::clone(&pending_blob_requests),
        sedimentree_core::depth::CountLeadingZeroBytes,
        observer,
    );
    let send_counter = sync_handler.send_counter().clone();
    let handler = Arc::new(sync_handler);
    let (subduction, listener, manager) = Subduction::new(
        handler,
        None,
        signer,
        Arc::clone(&sedimentrees),
        Arc::clone(&connections),
        Arc::clone(&subscriptions),
        storage_powerbox,
        Arc::clone(&pending_blob_requests),
        send_counter,
        subduction_core::nonce_cache::NonceCache::new(Duration::from_secs(60)),
        TimeoutTokio,
        Duration::from_secs(30),
        sedimentree_core::depth::CountLeadingZeroBytes,
        TokioSpawn,
    );
    let subduction_handle: Arc<RuntimeSubduction<S>> = Arc::clone(&subduction);
    let runtime_worker = BigRepoRuntimeWorker {
        subduction: subduction_handle,
        sedimentrees: Arc::clone(&sedimentrees),
        storage_for_reads,
        live_bundles: HashMap::new(),
        partition_store,
        change_manager,
        runtime_tasks: Arc::clone(&runtime_tasks),
        connect_signer,
        local_peer_id,
        nonce_cache,
        runtime_stop: runtime_stop.clone(),
        cmd_tx: cmd_tx.clone(),
        evt_tx: evt_tx.clone(),
        connected_peers: default(),
        doc_lease_counts: default(),
        refresh_in_flight: default(),
        sync_publish_in_flight: default(),
        pending_fragment_requests: default(),
        fragment_processing: default(),
        pending_put_docs: default(),
    };

    runtime_tasks
        .spawn({
            let stop = runtime_stop.child_token();
            async move {
                let _ = stop
                    .run_until_cancelled(async move {
                        listener.await.unwrap();
                    })
                    .await;
            }
        })
        .expect(ERROR_TOKIO);
    runtime_tasks
        .spawn({
            let stop = runtime_stop.child_token();
            async move {
                let _ = stop
                    .run_until_cancelled(async move {
                        manager.await.unwrap();
                    })
                    .await;
            }
        })
        .expect(ERROR_TOKIO);

    join_set
        .spawn({
            let runtime_stop = runtime_stop.clone();
            let mut runtime_worker = runtime_worker;
            async move {
                loop {
                    tokio::select! {
                        biased;
                        _ = runtime_stop.cancelled() => break,
                        cmd = cmd_rx.recv() => {
                            let Some(cmd) = cmd else { break; };
                            runtime_worker.handle_cmd(cmd).await;
                        },
                        evt = evt_rx.recv() => {
                            let Some(evt) = evt else { break; };
                            runtime_worker.handle_evt(evt).await;
                        }
                    }
                }

                let _ = runtime_worker
                    .runtime_tasks
                    .stop(Duration::from_secs(5))
                    .await;
                let _ = done_tx.send(());
            }
        })
        .expect(ERROR_TOKIO);

    Ok((
        BigRepoRuntimeHandle { cmd_tx },
        BigRepoRuntimeStopToken {
            cancel_token: runtime_stop,
            done_rx: tokio::sync::Mutex::new(Some(done_rx)),
        },
    ))
}

struct BigRepoRuntimeWorker<S>
where
    S: RuntimeSubductionStorage,
{
    subduction: Arc<RuntimeSubduction<S>>,
    sedimentrees: SubductionSedimentrees,
    storage_for_reads: S,
    live_bundles: HashMap<DocumentId, std::sync::Weak<crate::repo::LiveDocBundle>>,
    partition_store: Arc<PartitionStore>,
    change_manager: Arc<changes::ChangeListenerManager>,
    runtime_tasks: Arc<utils_rs::AbortableJoinSet>,
    connect_signer: subduction_crypto::signer::memory::MemorySigner,
    local_peer_id: subduction_core::peer::id::PeerId,
    nonce_cache: Arc<subduction_core::nonce_cache::NonceCache>,
    runtime_stop: CancellationToken,
    cmd_tx: mpsc::UnboundedSender<RuntimeCmd>,
    evt_tx: mpsc::UnboundedSender<RuntimeEvt>,
    connected_peers: Arc<tokio::sync::Mutex<HashMap<PeerId, RuntimePeerConnectionStopToken>>>,
    doc_lease_counts: Arc<tokio::sync::Mutex<HashMap<DocumentId, usize>>>,
    refresh_in_flight: Arc<tokio::sync::Mutex<HashSet<DocumentId>>>,
    sync_publish_in_flight: Arc<tokio::sync::Mutex<HashSet<DocumentId>>>,
    pending_fragment_requests: HashMap<DocumentId, BTreeSet<FragmentRequested>>,
    fragment_processing: HashSet<DocumentId>,
    pending_put_docs: HashMap<DocumentId, PendingPutDoc>,
}

impl<S> BigRepoRuntimeWorker<S>
where
    S: RuntimeSubductionStorage,
{
    async fn handle_cmd(&mut self, cmd: RuntimeCmd) {
        match cmd {
            RuntimeCmd::PutDoc {
                doc_id,
                initial_content,
                done,
            } => self.handle_put_doc(doc_id, initial_content, done).await,
            RuntimeCmd::GetDocHandle { doc_id, done } => {
                self.handle_get_doc_handle(doc_id, done).await
            }
            RuntimeCmd::ExportDocSave { doc_id, done } => {
                self.handle_export_doc_save(doc_id, done).await
            }
            RuntimeCmd::CommitDelta {
                doc_id,
                commits,
                heads,
                patches,
                origin,
                done,
            } => {
                self.handle_commit_delta(doc_id, commits, heads, patches, origin, done)
                    .await
            }
            RuntimeCmd::ConnectOutgoing {
                endpoint,
                endpoint_addr,
                peer_id,
                done,
            } => {
                self.handle_connect_outgoing(endpoint, endpoint_addr, peer_id, done)
                    .await
            }
            RuntimeCmd::AcceptIncoming { quic_conn, done } => {
                self.handle_accept_incoming_connection(quic_conn, done)
                    .await;
            }
            RuntimeCmd::CloseConnection { peer_id, done } => {
                self.handle_close_peer_connection(peer_id, done).await;
            }
            RuntimeCmd::SyncDocWithPeer {
                doc_id,
                peer_id,
                subscribe,
                timeout,
                done,
            } => {
                self.handle_sync_doc_with_peer(doc_id, peer_id, subscribe, timeout, done)
                    .await
            }
            RuntimeCmd::ReleaseDocLease { doc_id } => self.handle_release_doc_lease(doc_id).await,
        }
    }

    async fn handle_evt(&mut self, evt: RuntimeEvt) {
        match evt {
            RuntimeEvt::RemoteHeadsObserved { doc_id, peer_id } => {
                self.handle_remote_heads_observed(doc_id, peer_id).await;
            }
            RuntimeEvt::LiveDocRefreshLoaded {
                doc_id,
                peer_id,
                snapshot,
            } => {
                self.handle_live_doc_refresh_loaded(doc_id, peer_id, snapshot)
                    .await;
            }
            RuntimeEvt::FragmentRequestsReady { doc_id, requests } => {
                self.handle_fragment_requests_ready(doc_id, requests).await;
            }
            RuntimeEvt::FragmentProcessingFinished { doc_id } => {
                self.handle_fragment_processing_finished(doc_id).await;
            }
            RuntimeEvt::PutDocSucceeded { doc_id } => {
                self.handle_put_doc_succeeded(doc_id).await;
            }
            RuntimeEvt::PutDocFailed { doc_id, error } => {
                self.handle_put_doc_failed(doc_id, error).await;
            }
            RuntimeEvt::ConnectionEstablished {
                peer_id,
                stop_token,
            } => {
                self.handle_connection_established(peer_id, stop_token)
                    .await;
            }
            RuntimeEvt::ConnectionLost { peer_id } => {
                self.handle_connection_lost(peer_id).await;
            }
        }
    }

    fn spawn_background<F>(&self, fut: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let stop = self.runtime_stop.child_token();
        self.runtime_tasks
            .spawn(async move {
                let _ = stop.run_until_cancelled(fut).await;
            })
            .expect(ERROR_TOKIO);
    }

    async fn handle_put_doc(
        &mut self,
        doc_id: DocumentId,
        initial_content: automerge::Automerge,
        done: oneshot::Sender<Res<Arc<crate::repo::LiveDocBundle>>>,
    ) {
        if self
            .live_bundles
            .get(&doc_id)
            .and_then(|entry| entry.upgrade())
            .is_some()
            || self.pending_put_docs.contains_key(&doc_id)
        {
            done.send(Err(eyre::eyre!("document already exists locally")))
                .inspect_err(|_| warn!(ERROR_CALLER))
                .ok();
            return;
        }

        let count: i64 =
            match sqlx::query_scalar("SELECT COUNT(*) FROM big_repo_docs WHERE doc_id = ?")
                .bind(doc_id.to_string())
                .fetch_one(self.partition_store.state_pool())
                .await
            {
                Ok(count) => count,
                Err(err) => {
                    done.send(Err(eyre::eyre!("failed checking big_repo_docs: {err}")))
                        .inspect_err(|_| warn!(ERROR_CALLER))
                        .ok();
                    return;
                }
            };
        if count > 0 {
            done.send(Err(eyre::eyre!("document already exists locally")))
                .inspect_err(|_| warn!(ERROR_CALLER))
                .ok();
            return;
        }

        self.pending_put_docs.insert(
            doc_id,
            PendingPutDoc {
                initial_content: initial_content.clone(),
                done,
            },
        );

        let subduction: Arc<RuntimeSubduction<S>> = Arc::clone(&self.subduction);
        let evt_tx = self.evt_tx.clone();
        let fut = async move {
            let sedimentree_id: SedimentreeId = doc_id.into();
            let ingested =
                automerge_sedimentree::ingest::ingest_automerge(&initial_content, sedimentree_id)
                    .map_err(|err| ferr!("failed ingesting automerge doc: {err}"))?;
            subduction
                .add_sedimentree(sedimentree_id, ingested.sedimentree, ingested.blobs)
                .await
                .map_err(|err| ferr!("failed add_sedimentree: {err}"))?;
            eyre::Ok(())
        };
        self.spawn_background(async move {
            match fut.await {
                Ok(()) => evt_tx
                    .send(RuntimeEvt::PutDocSucceeded { doc_id })
                    .expect(ERROR_CHANNEL),
                Err(err) => evt_tx
                    .send(RuntimeEvt::PutDocFailed {
                        doc_id,
                        error: err.to_string(),
                    })
                    .expect(ERROR_CHANNEL),
            }
        });
    }

    async fn handle_put_doc_succeeded(&mut self, doc_id: DocumentId) {
        let Some(pending) = self.pending_put_docs.remove(&doc_id) else {
            return;
        };
        let lease = match self.acquire_doc_lease_now(doc_id).await {
            Ok(lease) => lease,
            Err(err) => {
                let _ = pending
                    .done
                    .send(Err(err))
                    .inspect_err(|_| warn!(ERROR_CALLER));
                return;
            }
        };
        let bundle = Arc::new(crate::repo::LiveDocBundle::new(
            doc_id,
            pending.initial_content,
            lease,
        ));
        self.register_live_bundle(Arc::clone(&bundle));
        if let Err(err) = self.upsert_known_doc_now(doc_id).await {
            self.live_bundles.remove(&doc_id);
            self.release_doc_lease_now(doc_id).await;
            let _ = pending
                .done
                .send(Err(err))
                .inspect_err(|_| warn!(ERROR_CALLER));
            return;
        }
        let item_payload = {
            let doc = bundle.doc.lock().await;
            let heads = Arc::<[automerge::ChangeHash]>::from(doc.get_heads());

            // FIXME: this is hella broken, instead of cocunting total change hashes
            // in automerge,
            // what I want the change count to be is to be a counter to how many
            // times a single partition_store payload have changed
            let change_count_hint = doc.get_changes(&[]).len().max(1) as u64;
            let item_payload = serde_json::json!({
                "heads": crate::serialize_commit_heads(&heads),
                "change_count_hint": change_count_hint.max(1),
            });
            (item_payload, heads)
        };
        let (item_payload, heads) = item_payload;
        if let Err(err) = self
            .partition_store
            .record_member_item_change(&bundle.doc_id.to_string(), &item_payload)
            .await
        {
            self.live_bundles.remove(&doc_id);
            self.release_doc_lease_now(doc_id).await;
            let _ = pending
                .done
                .send(Err(err))
                .inspect_err(|_| warn!(ERROR_CALLER));
            return;
        }
        self.change_manager
            .notify_doc_created(bundle.doc_id, Arc::clone(&heads))
            .expect("failed notifying doc created");
        self.change_manager
            .notify_local_doc_created(bundle.doc_id, heads)
            .expect("failed notifying local doc created");
        let _ = pending
            .done
            .send(Ok(bundle))
            .inspect_err(|_| warn!(ERROR_CALLER));
    }

    async fn handle_put_doc_failed(&mut self, doc_id: DocumentId, error: String) {
        if let Some(pending) = self.pending_put_docs.remove(&doc_id) {
            let _ = pending
                .done
                .send(Err(eyre::eyre!("{error}")))
                .inspect_err(|_| warn!(ERROR_CALLER));
        }
    }

    async fn handle_get_doc_handle(
        &mut self,
        doc_id: DocumentId,
        done: oneshot::Sender<Res<Option<Arc<crate::repo::LiveDocBundle>>>>,
    ) {
        // FIXME: why are we doing this on the main thread when it'd block the event loop?
        // also, inline it
        let result = async move {
            if let Some(bundle) = self
                .live_bundles
                .get(&doc_id)
                .and_then(|entry| entry.upgrade())
            {
                return Ok(Some(bundle));
            }
            let Some(doc) =
                    // FIXME: load_doc_snapshot should never be used on the main thread
                    load_doc_snapshot(&self.sedimentrees, &self.storage_for_reads, doc_id).await?
                else {
                    return Ok(None);
                };
            let lease = self.acquire_doc_lease_now(doc_id).await?;
            let bundle = Arc::new(crate::repo::LiveDocBundle::new(doc_id, doc, lease));
            self.upsert_known_doc_now(doc_id).await?;
            self.register_live_bundle(Arc::clone(&bundle));
            Ok(Some(bundle))
        }
        .await;
        done.send(result).inspect_err(|_| warn!(ERROR_CALLER)).ok();
    }

    async fn handle_export_doc_save(
        &self,
        doc_id: DocumentId,
        done: oneshot::Sender<Res<Option<Vec<u8>>>>,
    ) {
        let live_bundle = self
            .live_bundles
            .get(&doc_id)
            .and_then(|entry| entry.upgrade());
        let storage_for_reads = self.storage_for_reads.clone();
        let sedimentrees = Arc::clone(&self.sedimentrees);
        self.spawn_background(async move {
            let doc_id: DocumentId = doc_id.into();
            let res = if let Some(bundle) = live_bundle {
                let doc = bundle.doc.lock().await;
                Ok(Some(doc.save()))
            } else {
                load_doc_snapshot(&sedimentrees, &storage_for_reads, doc_id)
                    .await
                    .map(|maybe_doc| maybe_doc.map(|doc| doc.save()))
            };
            done.send(res).expect(ERROR_CALLER);
        });
    }

    fn register_live_bundle(&mut self, bundle: Arc<crate::repo::LiveDocBundle>) {
        self.live_bundles
            .insert(bundle.doc_id, Arc::downgrade(&bundle));
    }

    async fn upsert_known_doc_now(&self, doc_id: DocumentId) -> Res<()> {
        sqlx::query("INSERT INTO big_repo_docs(doc_id) VALUES(?) ON CONFLICT(doc_id) DO NOTHING")
            .bind(doc_id.to_string())
            .execute(self.partition_store.state_pool())
            .await
            .wrap_err("failed upserting big_repo_docs")?;
        Ok(())
    }

    async fn acquire_doc_lease_now(&self, doc_id: DocumentId) -> Res<RuntimeDocLease> {
        let mut counts = self.doc_lease_counts.lock().await;
        let count = counts.entry(doc_id).or_insert(0);
        *count += 1;
        drop(counts);
        Ok(RuntimeDocLease {
            runtime: BigRepoRuntimeHandle {
                cmd_tx: self.cmd_tx.clone(),
            },
            doc_id,
        })
    }

    async fn release_doc_lease_now(&self, doc_id: DocumentId) {
        let mut counts = self.doc_lease_counts.lock().await;
        if let Some(count) = counts.get_mut(&doc_id) {
            if *count > 1 {
                *count -= 1;
            } else {
                counts.remove(&doc_id);
            }
        }
    }

    async fn handle_commit_delta(
        &self,
        doc_id: DocumentId,
        commits: Vec<(CommitId, BTreeSet<CommitId>, Vec<u8>)>,
        heads: Vec<automerge::ChangeHash>,
        patches: Vec<automerge::Patch>,
        origin: BigRepoChangeOrigin,
        done: oneshot::Sender<Res<()>>,
    ) {
        let partition_store = Arc::clone(&self.partition_store);
        let change_manager = Arc::clone(&self.change_manager);
        let subduction: Arc<RuntimeSubduction<S>> = Arc::clone(&self.subduction);
        let evt_tx = self.evt_tx.clone();
        self.spawn_background(async move {
            let out = (async {
                let sedimentree_id: SedimentreeId = doc_id.into();
                let change_count = commits.len().max(1) as u64;
                let mut fragment_requests = BTreeSet::new();
                for (head, parents, blob) in commits {
                    let maybe_request = subduction
                        .add_commit(sedimentree_id, head, parents, Blob::new(blob))
                        .await
                        .map_err(|err| ferr!("failed add_commit: {err}"))?;
                    if let Some(request) = maybe_request {
                        fragment_requests.insert(request);
                    }
                }
                commit_delta_bookkeep(
                    &partition_store,
                    &change_manager,
                    doc_id,
                    change_count,
                    heads,
                    patches,
                    origin,
                )
                .await?;

                if !fragment_requests.is_empty() {
                    evt_tx
                        .send(RuntimeEvt::FragmentRequestsReady {
                            doc_id,
                            requests: fragment_requests.into_iter().collect(),
                        })
                        .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
                }

                eyre::Ok(())
            })
            .await;
            done.send(out).inspect_err(|_| warn!(ERROR_CALLER)).ok();
        });
    }
    async fn handle_fragment_requests_ready(
        &mut self,
        doc_id: DocumentId,
        requests: Vec<FragmentRequested>,
    ) {
        let pending = self.pending_fragment_requests.entry(doc_id).or_default();
        for request in requests {
            pending.insert(request);
        }
        self.maybe_start_fragment_processing(doc_id).await;
    }

    async fn handle_fragment_processing_finished(&mut self, doc_id: DocumentId) {
        self.fragment_processing.remove(&doc_id);
        self.maybe_start_fragment_processing(doc_id).await;
    }

    async fn maybe_start_fragment_processing(&mut self, doc_id: DocumentId) {
        if self.fragment_processing.contains(&doc_id) {
            return;
        }
        let Some(requests) = self.pending_fragment_requests.remove(&doc_id) else {
            return;
        };
        let Some(bundle) = self
            .live_bundles
            .get(&doc_id)
            .and_then(|entry| entry.upgrade())
        else {
            return;
        };

        self.fragment_processing.insert(doc_id);

        let subduction: Arc<RuntimeSubduction<S>> = Arc::clone(&self.subduction);
        let evt_tx = self.evt_tx.clone();
        self.spawn_background(async move {
            if let Err(err) = process_fragment_requests(bundle, requests, subduction).await {
                warn!(%doc_id, ?err, "fragment follow-up failed");
            }
            evt_tx
                .send(RuntimeEvt::FragmentProcessingFinished { doc_id })
                .expect(ERROR_CHANNEL);
        });
    }

    async fn handle_connect_outgoing(
        &self,
        endpoint: iroh::Endpoint,
        endpoint_addr: iroh::EndpointAddr,
        peer_id: PeerId,
        done: oneshot::Sender<Res<()>>,
    ) {
        let subduction: Arc<RuntimeSubduction<S>> = Arc::clone(&self.subduction);
        let connect_signer = self.connect_signer.clone();
        let evt_tx = self.evt_tx.clone();
        let runtime_tasks = Arc::clone(&self.runtime_tasks);
        let connected_peers = Arc::clone(&self.connected_peers);
        let fut = async move {
            if connected_peers.lock().await.contains_key(&peer_id) {
                return Ok(());
            }
            let connect = connect_outgoing(endpoint, endpoint_addr, &connect_signer)
                .await
                .map_err(|err| ferr!("failed subduction iroh connect: {err}"))?;
            let stop_token = RuntimePeerConnectionStopToken::new();
            let fut_listener = {
                let stop = stop_token.child_token();
                let peer_id = peer_id;
                let evt_tx = evt_tx.clone();
                let fut = connect.listener_task;
                async move {
                    tokio::select! {
                        biased;
                        _ = stop.cancelled() => {}
                        res = fut => {
                            match res {
                                Ok(()) => {
                                    evt_tx
                                        .send(RuntimeEvt::ConnectionLost {
                                            peer_id: peer_id,
                                        })
                                        .expect(ERROR_CHANNEL);
                                }
                                Err(err) => {
                                    warn!(?err, %peer_id, "connection listener task error");
                                    evt_tx
                                        .send(RuntimeEvt::ConnectionLost {
                                            peer_id: peer_id,
                                        })
                                        .expect(ERROR_CHANNEL);
                                }
                            }
                        }
                    }
                }
            };
            let fut_sender = {
                let stop = stop_token.child_token();
                let peer_id = peer_id;
                let evt_tx = evt_tx.clone();
                let fut = connect.sender_task;
                async move {
                    tokio::select! {
                        biased;
                        _ = stop.cancelled() => {}
                        res = fut => {
                            match res {
                                Ok(()) => {
                                    evt_tx
                                        .send(RuntimeEvt::ConnectionLost {
                                            peer_id: peer_id,
                                        })
                                        .expect(ERROR_CHANNEL);
                                }
                                Err(err) => {
                                    warn!(?err, %peer_id, "connection sender task error");
                                    evt_tx
                                        .send(RuntimeEvt::ConnectionLost {
                                            peer_id: peer_id,
                                        })
                                        .expect(ERROR_CHANNEL);
                                }
                            }
                        }
                    }
                }
            };
            runtime_tasks.spawn(fut_listener).expect(ERROR_TOKIO);
            runtime_tasks.spawn(fut_sender).expect(ERROR_TOKIO);
            subduction
                .add_connection(connect.authenticated)
                .await
                .map_err(|err| {
                    stop_token.cancel();
                    ferr!("failed subduction add_connection: {err}")
                })?;
            evt_tx
                .send(RuntimeEvt::ConnectionEstablished {
                    peer_id,
                    stop_token,
                })
                .expect(ERROR_CHANNEL);
            Ok(())
        };
        self.spawn_background(async move {
            done.send(fut.await).expect(ERROR_CALLER);
        });
    }

    async fn handle_accept_incoming_connection(
        &self,
        quic_conn: iroh::endpoint::Connection,
        done: oneshot::Sender<Res<PeerId>>,
    ) {
        let subduction: Arc<RuntimeSubduction<S>> = Arc::clone(&self.subduction);
        let connect_signer = self.connect_signer.clone();
        let nonce_cache = Arc::clone(&self.nonce_cache);
        let local_peer_id = self.local_peer_id;
        let evt_tx = self.evt_tx.clone();
        let runtime_tasks = Arc::clone(&self.runtime_tasks);
        let connected_peers = Arc::clone(&self.connected_peers);
        let fut = async move {
            // do handshake
            let accepted = accept_incoming(
                quic_conn,
                &connect_signer,
                nonce_cache.as_ref(),
                local_peer_id,
            )
            .await
            .map_err(|err| ferr!("failed subduction iroh accept: {err}"))?;
            let peer_id = PeerId::from(accepted.authenticated.peer_id());
            if connected_peers.lock().await.contains_key(&peer_id) {
                return Ok(peer_id);
            }
            let stop_token = RuntimePeerConnectionStopToken::new();
            let fut_listener = {
                let stop = stop_token.child_token();
                let evt_tx = evt_tx.clone();
                let peer_id = peer_id;
                let fut = accepted.listener_task;
                async move {
                    tokio::select! {
                        biased;
                        _ = stop.cancelled() => {}
                        res = fut => {
                            match res {
                                Ok(()) => {
                                    evt_tx
                                        .send(RuntimeEvt::ConnectionLost {
                                            peer_id,
                                        })
                                        .expect(ERROR_CHANNEL);
                                }
                                Err(err) => {
                                    warn!(?err, %peer_id, "incoming connection listener task exited");
                                    evt_tx
                                        .send(RuntimeEvt::ConnectionLost {
                                            peer_id: peer_id,
                                        })
                                        .expect(ERROR_CHANNEL);
                                }
                            }
                        }
                    }
                }
            };
            let fut_sender = {
                let stop = stop_token.child_token();
                let peer_id = peer_id;
                let evt_tx = evt_tx.clone();
                let fut = accepted.sender_task;
                async move {
                    tokio::select! {
                        biased;
                        _ = stop.cancelled() => {}
                        res = fut => {
                            match res {
                                Ok(()) => {
                                    evt_tx
                                        .send(RuntimeEvt::ConnectionLost {
                                            peer_id: peer_id,
                                        })
                                        .expect(ERROR_CHANNEL);
                                }
                                Err(err) => {
                                    warn!(?err, %peer_id, "incoming connection sender task exited");
                                    evt_tx
                                        .send(RuntimeEvt::ConnectionLost {
                                            peer_id: peer_id,
                                        })
                                        .expect(ERROR_CHANNEL);
                                }
                            }
                        }
                    }
                }
            };
            runtime_tasks.spawn(fut_listener).expect(ERROR_TOKIO);
            runtime_tasks.spawn(fut_sender).expect(ERROR_TOKIO);
            subduction
                .add_connection(accepted.authenticated)
                .await
                .inspect_err(|_| {
                    stop_token.cancel();
                })
                .wrap_err("failed subduction add_connection")?;
            evt_tx
                .send(RuntimeEvt::ConnectionEstablished {
                    peer_id,
                    stop_token,
                })
                .expect(ERROR_CHANNEL);
            Ok(peer_id)
        };
        self.spawn_background(async move {
            done.send(fut.await).expect(ERROR_CALLER);
        });
    }

    async fn handle_close_peer_connection(
        &self,
        peer_id: PeerId,
        done: Option<oneshot::Sender<Res<()>>>,
    ) {
        let subduction: Arc<RuntimeSubduction<S>> = Arc::clone(&self.subduction);
        let connected_peers = Arc::clone(&self.connected_peers);
        self.spawn_background(async move {
            let out = async {
                let stop_token = connected_peers.lock().await.remove(&peer_id);
                if let Some(stop_token) = stop_token {
                    stop_token.cancel();
                    let remote_peer_id: subduction_core::peer::id::PeerId = peer_id.into();
                    subduction
                        .disconnect_from_peer(&remote_peer_id)
                        .await
                        .map_err(|err| ferr!("failed subduction disconnect_from_peer: {err}"))?;
                }
                Ok(())
            }
            .await;
            if let Some(done) = done {
                done.send(out).expect(ERROR_CALLER);
            }
        });
    }

    async fn handle_sync_doc_with_peer(
        &self,
        doc_id: DocumentId,
        peer_id: PeerId,
        subscribe: bool,
        timeout: Option<Duration>,
        done: oneshot::Sender<Res<SyncDocOutcome>>,
    ) {
        let subduction: Arc<RuntimeSubduction<S>> = Arc::clone(&self.subduction);
        let storage_for_reads = self.storage_for_reads.clone();
        let sedimentrees = Arc::clone(&self.sedimentrees);
        let partition_store = Arc::clone(&self.partition_store);
        let change_manager = Arc::clone(&self.change_manager);
        let sync_publish_in_flight = Arc::clone(&self.sync_publish_in_flight);
        let fut = async move {
            let sedimentree_id: SedimentreeId = doc_id.into();
            let before_doc =
                load_doc_snapshot(&sedimentrees, &storage_for_reads, sedimentree_id).await?;
            let remote_peer_id: subduction_core::peer::id::PeerId = peer_id.into();
            sync_publish_in_flight.lock().await.insert(doc_id);
            let result = subduction
                .sync_with_peer(&remote_peer_id, sedimentree_id, subscribe, timeout)
                .await;
            let outcome = match result {
                Ok((had_success, _stats, conn_errs)) => {
                    if had_success {
                        SyncDocOutcome::Success
                    } else if conn_errs.is_empty() {
                        SyncDocOutcome::NotFoundOrUnauthorized
                    } else {
                        SyncDocOutcome::TransportError
                    }
                }
                Err(err) => {
                    warn!(?err, %doc_id, %peer_id, "subduction sync_with_peer io error");
                    SyncDocOutcome::IoError
                }
            };
            // FIXME: this is hella broken
            if matches!(outcome, SyncDocOutcome::Success) {
                let after_doc =
                    load_doc_snapshot(&sedimentrees, &storage_for_reads, sedimentree_id).await?;
                if let Some(after_doc) = after_doc {
                    let before_heads = before_doc
                        .as_ref()
                        .map_or_else(Vec::new, |doc| doc.get_heads());
                    let after_heads = after_doc.get_heads();
                    if before_heads != after_heads {
                        let patches = after_doc.diff(&before_heads, &after_heads);
                        let change_count = after_doc.get_changes(&before_heads).len().max(1) as u64;
                        commit_delta_bookkeep(
                            &partition_store,
                            &change_manager,
                            doc_id,
                            change_count,
                            after_heads,
                            patches,
                            BigRepoChangeOrigin::Remote { peer_id },
                        )
                        .await?;
                    }
                }
            }
            eyre::Ok(outcome)
        };
        let sync_publish_in_flight = Arc::clone(&self.sync_publish_in_flight);
        self.spawn_background(async move {
            let out = fut.await;
            sync_publish_in_flight.lock().await.remove(&doc_id);
            done.send(out).expect(ERROR_CALLER);
        });
    }

    async fn handle_release_doc_lease(&self, doc_id: DocumentId) {
        self.release_doc_lease_now(doc_id).await;
    }

    async fn handle_remote_heads_observed(&mut self, doc_id: DocumentId, peer_id: PeerId) {
        if self
            .live_bundles
            .get(&doc_id)
            .and_then(|entry| entry.upgrade())
            .is_none()
        {
            return;
        }
        let mut in_flight = self.refresh_in_flight.lock().await;
        if !in_flight.insert(doc_id) {
            return;
        }
        drop(in_flight);
        let storage_for_reads = self.storage_for_reads.clone();
        let evt_tx = self.evt_tx.clone();
        let sedimentrees = Arc::clone(&self.sedimentrees);
        self.spawn_background(async move {
            // FIXME:  yeah so this code is hella confused. Subduction runs
            // the sync algorithm right?  The automerge sync algorithm? What happens
            // when subduction syncs a doc between two peers. Might have
            // to look into ./symlinks code and automerge-repo integration
            // but I'm not sure if we need to do sync/merge ourselves
            // (not to mention apply the commit delta on top after? wthhh)
            let snapshot = load_doc_snapshot(&sedimentrees, &storage_for_reads, doc_id).await;
            evt_tx
                .send(RuntimeEvt::LiveDocRefreshLoaded {
                    doc_id,
                    peer_id,
                    snapshot,
                })
                .expect(ERROR_CHANNEL);
        });
    }

    async fn handle_live_doc_refresh_loaded(
        &mut self,
        doc_id: DocumentId,
        peer_id: PeerId,
        snapshot: Res<Option<automerge::Automerge>>,
    ) {
        self.refresh_in_flight.lock().await.remove(&doc_id);
        let publish = !self.sync_publish_in_flight.lock().await.contains(&doc_id);
        let Ok(Some(after_doc)) = snapshot else {
            return;
        };
        let Some(bundle) = self
            .live_bundles
            .get(&doc_id)
            .and_then(|entry| entry.upgrade())
        else {
            return;
        };
        self.apply_live_doc_refresh(bundle, after_doc, Some(peer_id), publish)
            .await;
    }

    async fn handle_connection_lost(&self, peer_id: PeerId) {
        let maybe_stop = self.connected_peers.lock().await.remove(&peer_id);
        if let Some(connection) = maybe_stop {
            connection.cancel();
            let remote_peer_id: subduction_core::peer::id::PeerId = peer_id.into();
            let _ = self.subduction.disconnect_from_peer(&remote_peer_id).await;
        }
    }

    async fn handle_connection_established(
        &self,
        peer_id: PeerId,
        stop_token: RuntimePeerConnectionStopToken,
    ) {
        if stop_token.is_cancelled() {
            return;
        }
        let mut connected_peers = self.connected_peers.lock().await;
        if let Some(previous) = connected_peers.insert(peer_id, stop_token) {
            previous.cancel();
        }
    }

    async fn apply_live_doc_refresh(
        &self,
        bundle: Arc<crate::repo::LiveDocBundle>,
        mut after_doc: automerge::Automerge,
        peer_id: Option<PeerId>,
        publish: bool,
    ) {
        let doc_id = bundle.doc_id;
        let before_heads = {
            let doc = bundle.doc.lock().await;
            doc.get_heads()
        };
        let maybe_delta = {
            let mut doc = bundle.doc.lock().await;
            if let Err(err) = doc.merge(&mut after_doc) {
                warn!(?err, %doc_id, "failed merging refreshed doc into live bundle");
                return;
            }
            let after_heads = doc.get_heads();
            if before_heads == after_heads {
                None
            } else {
                let patches = doc.diff(&before_heads, &after_heads);
                let change_count = doc.get_changes(&before_heads).len().max(1) as u64;
                Some((after_heads, patches, change_count))
            }
        };
        let Some((after_heads, patches, change_count)) = maybe_delta else {
            return;
        };
        if !publish {
            return;
        }
        let origin = peer_id
            .map(|peer_id| BigRepoChangeOrigin::Remote { peer_id })
            .unwrap_or(BigRepoChangeOrigin::Bootstrap);
        // FIXME: uhh, is this needed? doesn't subduction send sync changes
        // to the storage??
        //
        // also, you're not doing this on spawn_background unless the
        // otehr usage of the method
        if let Err(err) = commit_delta_bookkeep(
            &self.partition_store,
            &self.change_manager,
            doc_id,
            change_count,
            after_heads,
            patches,
            origin,
        )
        .await
        {
            warn!(?err, %doc_id, "failed to record refreshed live doc delta");
        }
    }
}

impl BigRepoRuntimeStopToken {
    pub async fn stop(self) -> Res<()> {
        self.cancel_token.cancel();
        let done_rx = self
            .done_rx
            .lock()
            .await
            .take()
            .ok_or_else(|| eyre::eyre!("runtime stop token already consumed"))?;
        done_rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?;
        Ok(())
    }
}

async fn commit_delta_bookkeep(
    partition_store: &Arc<PartitionStore>,
    change_manager: &Arc<changes::ChangeListenerManager>,
    doc_id: DocumentId,
    change_count_hint: u64,
    heads: Vec<automerge::ChangeHash>,
    patches: Vec<automerge::Patch>,
    origin: BigRepoChangeOrigin,
) -> Res<()> {
    let item_payload = serde_json::json!({
        "heads": crate::serialize_commit_heads(&heads),
        "change_count_hint": change_count_hint.max(1),
    });
    partition_store
        .record_member_item_change(&doc_id.to_string(), &item_payload)
        .await?;

    let heads_arc = Arc::<[automerge::ChangeHash]>::from(heads);
    change_manager.notify_doc_heads_changed(doc_id, Arc::clone(&heads_arc), origin.clone())?;
    if matches!(origin, BigRepoChangeOrigin::Local) {
        change_manager.notify_local_doc_heads_updated(doc_id, Arc::clone(&heads_arc))?;
    }
    for patch in patches {
        change_manager.notify_doc_changed(
            doc_id,
            Arc::new(patch),
            Arc::clone(&heads_arc),
            origin.clone(),
        )?;
    }

    Ok(())
}

struct FragmentWorkItem {
    head: CommitId,
    boundary: BTreeSet<CommitId>,
    checkpoints: Vec<CommitId>,
    blob: Vec<u8>,
}

async fn process_fragment_requests<S>(
    bundle: Arc<crate::repo::LiveDocBundle>,
    requests: BTreeSet<FragmentRequested>,
    subduction: Arc<RuntimeSubduction<S>>,
) -> Res<()>
where
    S: RuntimeSubductionStorage,
{
    if requests.is_empty() {
        return Ok(());
    }

    let doc_id = bundle.doc_id;
    let work = {
        let doc = bundle.doc.lock().await;
        let heads: Vec<CommitId> = requests.iter().map(|request| request.head()).collect();
        let mut known = bundle.fragment_state_store.lock().await;
        let store = OwnedSedimentreeAutomerge::from(&*doc);
        let states = store
            .build_fragment_store(&heads, &mut known, &CountLeadingZeroBytes)
            .map_err(|err| ferr!("failed building fragment store: {err}"))?
            .into_iter()
            .cloned()
            .collect::<Vec<FragmentState<Set<CommitId>>>>();
        let mut out = Vec::with_capacity(states.len());
        for state in states {
            let members: Vec<automerge::ChangeHash> = state
                .members()
                .iter()
                .map(|member| automerge::ChangeHash(*member.as_bytes()))
                .collect();
            let fragment = doc.bundle(members).map_err(|err| {
                ferr!(
                    "failed building fragment bundle for {}: {err}",
                    state.head_id()
                )
            })?;
            out.push(FragmentWorkItem {
                head: state.head_id(),
                boundary: state.boundary().keys().copied().collect(),
                checkpoints: state.checkpoints().iter().copied().collect(),
                blob: fragment.bytes().to_vec(),
            });
        }
        out
    };

    for item in work {
        subduction
            .add_fragment(
                doc_id.into(),
                item.head,
                item.boundary,
                &item.checkpoints,
                Blob::new(item.blob),
            )
            .await
            .map_err(|err| ferr!("failed add_fragment: {err}"))?;
    }

    return Ok(());

    struct OwnedSedimentreeAutomerge<'a>(&'a automerge::Automerge);

    impl<'a> From<&'a automerge::Automerge> for OwnedSedimentreeAutomerge<'a> {
        fn from(value: &'a automerge::Automerge) -> Self {
            Self(value)
        }
    }

    impl<'a> CommitStore<'a> for OwnedSedimentreeAutomerge<'a> {
        type Node = Set<CommitId>;
        type LookupError = Infallible;

        fn lookup(&self, id: CommitId) -> Result<Option<Self::Node>, Self::LookupError> {
            let change_hash = automerge::ChangeHash(*id.as_bytes());
            Ok(self
                .0
                .get_change_meta_by_hash(&change_hash)
                .map(|meta| meta.deps.iter().map(|dep| CommitId::new(dep.0)).collect()))
        }
    }
}

struct IrohConnectResult {
    authenticated: subduction_core::authenticated::Authenticated<
        subduction_core::transport::message::MessageTransport<
            subduction_iroh::transport::IrohTransport,
        >,
        future_form::Sendable,
    >,
    listener_task: BoxFuture<'static, Result<(), subduction_iroh::error::RunError>>,
    sender_task: BoxFuture<'static, Result<(), subduction_iroh::error::RunError>>,
}

async fn connect_outgoing(
    endpoint: iroh::Endpoint,
    endpoint_addr: iroh::EndpointAddr,
    signer: &subduction_crypto::signer::memory::MemorySigner,
) -> Res<IrohConnectResult> {
    let connected = subduction_iroh::client::connect(
        &endpoint,
        endpoint_addr,
        signer,
        subduction_core::handshake::audience::Audience::discover(b"townframe-subduction"),
    )
    .await
    .map_err(|err| ferr!("subduction iroh connect failed: {err}"))?;
    Ok(IrohConnectResult {
        authenticated: connected
            .authenticated
            .map(subduction_core::transport::message::MessageTransport::new),
        listener_task: connected.listener_task,
        sender_task: connected.sender_task,
    })
}

async fn accept_incoming(
    quic_conn: iroh::endpoint::Connection,
    signer: &subduction_crypto::signer::memory::MemorySigner,
    nonce_cache: &subduction_core::nonce_cache::NonceCache,
    local_peer_id: subduction_core::peer::id::PeerId,
) -> Res<IrohConnectResult> {
    let (send, recv) = quic_conn
        .accept_bi()
        .await
        .map_err(|err| ferr!("failed accepting subduction bidi stream: {err}"))?;
    let now = subduction_core::timestamp::TimestampSeconds::now();
    let handshake = subduction_iroh::handshake::IrohHandshake::new(send, recv);
    let quic_conn_clone = quic_conn.clone();
    let (authenticated, (listener_task, sender_task)) = subduction_core::handshake::respond(
        handshake,
        move |handshake, peer_id| {
            let (send, recv) = handshake.into_parts();
            let (transport, outbound_rx) =
                subduction_iroh::transport::IrohTransport::new(peer_id, quic_conn_clone);
            let listener_transport = transport.clone();
            let listener_task = Box::pin(subduction_iroh::tasks::listener_task(
                listener_transport,
                recv,
            ));
            let sender_task = Box::pin(subduction_iroh::tasks::sender_task(send, outbound_rx));
            (transport, (listener_task, sender_task))
        },
        signer,
        nonce_cache,
        local_peer_id,
        Some(subduction_core::handshake::audience::Audience::discover(
            b"townframe-subduction",
        )),
        now,
        Duration::from_secs(600),
    )
    .await
    .map_err(|err| ferr!("subduction handshake respond failed: {err}"))?;
    Ok(IrohConnectResult {
        authenticated: authenticated
            .map(subduction_core::transport::message::MessageTransport::new),
        listener_task,
        sender_task,
    })
}

async fn load_doc_snapshot<S, D>(
    sedimentrees: &SubductionSedimentrees,
    storage_for_reads: &S,
    doc_id: D,
) -> Res<Option<automerge::Automerge>>
where
    S: RuntimeSubductionStorage,
    D: Into<DocumentId>,
{
    let doc_id: DocumentId = doc_id.into();
    let sedimentree_id: SedimentreeId = doc_id.into();

    let loose_commits =
        <S as subduction_core::storage::traits::Storage<future_form::Sendable>>::load_loose_commits(
            storage_for_reads,
            sedimentree_id,
        );
    let fragments =
        <S as subduction_core::storage::traits::Storage<future_form::Sendable>>::load_fragments(
            storage_for_reads,
            sedimentree_id,
        );
    let (loose_commits, fragments) = futures::future::try_join(loose_commits, fragments)
        .await
        .wrap_err("failed reading blobs from storage")?;
    if loose_commits.is_empty() && fragments.is_empty() {
        return Ok(None);
    }
    let blobs = loose_commits
        .iter()
        .map(|verified| verified.blob().clone())
        .chain(fragments.iter().map(|verified| verified.blob().clone()))
        .collect::<Vec<_>>();

    let (tree, fresh) = match sedimentrees.get_cloned(&sedimentree_id).await {
        Some(tree) => (tree, false),
        None => {
            let tree = Sedimentree::new(
                fragments
                    .into_iter()
                    .map(|verified| verified.payload().clone())
                    .collect(),
                loose_commits
                    .into_iter()
                    .map(|verified| verified.payload().clone())
                    .collect(),
            );
            (tree, true)
        }
    };
    let blob_by_digest = blobs
        .iter()
        .map(|blob| (Digest::hash(blob), blob.as_slice()))
        .collect::<sedimentree_core::collections::Map<_, _>>();
    let order = tree
        .topsorted_blob_order()
        .map_err(|err| ferr!("failed ordering sedimentree blobs: {err}"))?;
    let fragments: Vec<_> = tree.fragments().collect();
    let loose: Vec<_> = tree.loose_commits().collect();

    let mut buf = Vec::new();
    for item in &order {
        let digest = match item {
            SedimentreeItem::Fragment(ii) => fragments[*ii].summary().blob_meta().digest(),
            SedimentreeItem::LooseCommit(ii) => loose[*ii].blob_meta().digest(),
        };
        let raw = blob_by_digest
            .get(&digest)
            .ok_or_else(|| ferr!("blob not found for ordered sedimentree item"))?;
        buf.extend_from_slice(raw);
    }

    let mut doc = automerge::Automerge::new();
    doc.load_incremental(&buf)
        .map_err(|err| ferr!("failed reconstructing automerge doc from ordered blobs: {err}"))?;

    if fresh {
        sedimentrees.insert(sedimentree_id, tree).await;
    }
    Ok(Some(doc))
}
