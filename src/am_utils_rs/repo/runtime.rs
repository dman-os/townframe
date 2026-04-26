//! This is just a crappy reimpl of samod to get subduction working

use crate::interlude::*;
use crate::partition::PartitionStore;
use crate::repo::{BigRepoChangeOrigin, DocumentId, PeerId};

use core::convert::Infallible;
use futures::future::BoxFuture;
use sedimentree_core::commit::{CommitStore, CountLeadingZeroBytes, FragmentState};
use sedimentree_core::crypto::digest::Digest;
use sedimentree_core::sedimentree::{Sedimentree, SedimentreeItem};
use sedimentree_core::{blob::Blob, id::SedimentreeId, loose_commit::id::CommitId};
use std::collections::{BTreeSet, HashMap};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::changes;
use automerge_sedimentree::indexed::OwnedParents;
use subduction_core::subduction::request::FragmentRequested;

const DOC_WORKER_IDLE_TTL: Duration = Duration::from_secs(3);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncDocOutcome {
    Success,
    NotFoundOrUnauthorized,
    TransportError,
    IoError,
}

#[derive(Debug)]
pub(super) struct LiveDocBundle {
    pub(super) doc_id: DocumentId,
    pub(super) doc: tokio::sync::Mutex<automerge::Automerge>,
    pub(super) fragment_state_store: tokio::sync::Mutex<
        sedimentree_core::collections::Map<CommitId, FragmentState<OwnedParents>>,
    >,
    pub(super) _lease: RuntimeDocLease,
}

impl LiveDocBundle {
    fn new(
        doc_id: DocumentId,
        doc: automerge::Automerge,
        fragment_state_store: sedimentree_core::collections::Map<
            CommitId,
            FragmentState<OwnedParents>,
        >,
        lease: RuntimeDocLease,
    ) -> Self {
        Self {
            doc_id,
            doc: tokio::sync::Mutex::new(doc),
            fragment_state_store: tokio::sync::Mutex::new(fragment_state_store),
            _lease: lease,
        }
    }
}

enum RuntimeCmd {
    PutDoc {
        doc_id: DocumentId,
        initial_content: automerge::Automerge,
        done: oneshot::Sender<Res<Arc<LiveDocBundle>>>,
    },
    GetDocHandle {
        doc_id: DocumentId,
        done: oneshot::Sender<Res<Option<Arc<LiveDocBundle>>>>,
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
        done: oneshot::Sender<Res<PeerId>>,
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
    SyncSessionObserved {
        session: subduction_core::sync_session::SyncSession,
    },
    ConnectionEstablished {
        peer_id: PeerId,
        stop_token: RuntimePeerConnectionStopToken,
    },
    ConnectionLost {
        peer_id: PeerId,
    },
    DocWorkerTransientFinished {
        doc_id: DocumentId,
    },
    DocWorkerHandleAcquired {
        bundle: Arc<LiveDocBundle>,
    },
    DocWorkerStopped {
        doc_id: DocumentId,
    },
    FatalWorkerError {
        doc_id: Option<DocumentId>,
        context: &'static str,
        error: String,
    },
}

#[derive(Clone, Debug)]
pub(super) struct BigRepoRuntimeHandle {
    cmd_tx: mpsc::UnboundedSender<RuntimeCmd>,
}

pub(super) struct BigRepoRuntimeStopToken {
    cancel_token: CancellationToken,
    done_rx: tokio::sync::Mutex<Option<oneshot::Receiver<()>>>,
    runtime_tasks: Arc<utils_rs::AbortableJoinSet>,
}

#[derive(Clone, Debug)]
pub(super) struct RuntimeDocLease {
    runtime: BigRepoRuntimeHandle,
    doc_id: DocumentId,
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
    shutdown: CancellationToken,
}

impl subduction_core::remote_heads::RemoteHeadsObserver for RuntimeRemoteHeadsBridge {
    fn on_remote_heads(
        &self,
        id: SedimentreeId,
        peer: subduction_core::peer::id::PeerId,
        _heads: subduction_core::remote_heads::RemoteHeads,
    ) {
        send_runtime_evt(
            &self.evt_tx,
            &self.shutdown,
            RuntimeEvt::RemoteHeadsObserved {
                doc_id: id.into(),
                peer_id: peer.into(),
            },
        );
    }
}

#[derive(Clone)]
struct RuntimeSyncSessionBridge {
    evt_tx: mpsc::UnboundedSender<RuntimeEvt>,
    shutdown: CancellationToken,
}

impl subduction_core::sync_session::SyncSessionObserver for RuntimeSyncSessionBridge {
    fn on_sync_session(&self, session: subduction_core::sync_session::SyncSession) {
        send_runtime_evt(
            &self.evt_tx,
            &self.shutdown,
            RuntimeEvt::SyncSessionObserved { session },
        );
    }
}

fn send_runtime_evt(
    evt_tx: &mpsc::UnboundedSender<RuntimeEvt>,
    shutdown: &CancellationToken,
    evt: RuntimeEvt,
) {
    if shutdown.is_cancelled() {
        let _ = evt_tx.send(evt);
        return;
    }
    evt_tx.send(evt).expect(ERROR_CHANNEL);
}

impl BigRepoRuntimeHandle {
    pub(super) async fn put_doc(
        &self,
        doc_id: DocumentId,
        initial_content: automerge::Automerge,
    ) -> Res<Arc<LiveDocBundle>> {
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
    ) -> Res<Option<Arc<LiveDocBundle>>> {
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

    #[tracing::instrument(skip_all, fields(%peer_id))]
    pub(super) async fn ensure_peer_connection(
        &self,
        endpoint: iroh::Endpoint,
        endpoint_addr: iroh::EndpointAddr,
        peer_id: PeerId,
    ) -> Res<PeerId> {
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

    #[tracing::instrument(skip_all)]
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

    #[tracing::instrument(skip_all, fields(%peer_id))]
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

    #[tracing::instrument(
        skip_all,
        fields(%doc_id, %peer_id, subscribe, timeout = ?timeout)
    )]
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
    let runtime_stop = CancellationToken::new();
    let (done_tx, done_rx) = oneshot::channel();

    let runtime_tasks = Arc::clone(&join_set);

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
    let storage_for_reads = storage.clone();
    let observer = RuntimeRemoteHeadsBridge {
        evt_tx: evt_tx.clone(),
        shutdown: runtime_stop.clone(),
    };
    let sync_session_observer: Arc<
        dyn subduction_core::sync_session::SyncSessionObserver + Send + Sync,
    > = Arc::new(RuntimeSyncSessionBridge {
        evt_tx: evt_tx.clone(),
        shutdown: runtime_stop.clone(),
    });
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
    sync_handler.set_sync_session_observer(sync_session_observer.clone());
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
    subduction.set_sync_session_observer(sync_session_observer);
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
        doc_workers: default(),
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
                let mut janitor_tick = tokio::time::interval(Duration::from_millis(500));
                loop {
                    tokio::select! {
                        biased;
                        _ = runtime_stop.cancelled() => break,
                        _ = janitor_tick.tick() => {
                            runtime_worker.handle_doc_worker_janitor_tick();
                        },
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

                let _ = done_tx.send(());
            }
        })
        .expect(ERROR_TOKIO);

    Ok((
        BigRepoRuntimeHandle { cmd_tx },
        BigRepoRuntimeStopToken {
            cancel_token: runtime_stop,
            done_rx: tokio::sync::Mutex::new(Some(done_rx)),
            runtime_tasks: Arc::clone(&runtime_tasks),
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
    live_bundles: HashMap<DocumentId, std::sync::Weak<LiveDocBundle>>,
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
    doc_workers: HashMap<DocumentId, DocWorkerEntry>,
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
            RuntimeEvt::SyncSessionObserved { session } => {
                self.handle_sync_session_observed(session).await;
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
            RuntimeEvt::DocWorkerTransientFinished { doc_id } => {
                self.handle_doc_worker_transient_finished(doc_id);
            }
            RuntimeEvt::DocWorkerHandleAcquired { bundle } => {
                self.handle_doc_worker_handle_acquired(bundle);
            }
            RuntimeEvt::DocWorkerStopped { doc_id } => {
                self.handle_doc_worker_stopped(doc_id);
            }
            RuntimeEvt::FatalWorkerError {
                doc_id,
                context,
                error,
            } => {
                panic!("fatal runtime worker error doc={doc_id:?} context={context}: {error}");
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

    fn clear_doc_worker_eviction(&mut self, doc_id: DocumentId) {
        if let Some(entry) = self.doc_workers.get_mut(&doc_id) {
            entry.eviction_deadline = None;
        }
    }

    fn schedule_doc_worker_eviction_if_idle(&mut self, doc_id: DocumentId) {
        let Some(entry) = self.doc_workers.get_mut(&doc_id) else {
            return;
        };
        if entry.local_handles > 0 || entry.transient_work > 0 {
            entry.eviction_deadline = None;
            return;
        }
        entry.eviction_deadline = Some(Instant::now() + DOC_WORKER_IDLE_TTL);
    }

    fn handle_doc_worker_janitor_tick(&mut self) {
        let now = Instant::now();
        for entry in self.doc_workers.values() {
            if entry
                .eviction_deadline
                .is_some_and(|deadline| deadline <= now)
            {
                entry.stop.cancel();
            }
        }
    }

    fn handle_doc_worker_transient_finished(&mut self, doc_id: DocumentId) {
        let Some(entry) = self.doc_workers.get_mut(&doc_id) else {
            return;
        };
        entry.transient_work = entry.transient_work.saturating_sub(1);
        self.schedule_doc_worker_eviction_if_idle(doc_id);
    }

    fn handle_doc_worker_handle_acquired(&mut self, bundle: Arc<LiveDocBundle>) {
        let doc_id = bundle.doc_id;
        self.register_live_bundle(Arc::clone(&bundle));
        if let Some(entry) = self.doc_workers.get_mut(&doc_id) {
            entry.local_handles += 1;
            entry.eviction_deadline = None;
        }
    }

    fn handle_doc_worker_stopped(&mut self, doc_id: DocumentId) {
        self.doc_workers.remove(&doc_id);
    }

    fn spawn_doc_worker(&mut self, doc_id: DocumentId) -> Res<()> {
        if self.doc_workers.contains_key(&doc_id) {
            self.clear_doc_worker_eviction(doc_id);
            return Ok(());
        }
        let (msg_tx, mut msg_rx) = mpsc::unbounded_channel();
        let cancel_token = self.runtime_stop.child_token();
        let worker_cancel = cancel_token.clone();
        let runtime_evt_tx = self.evt_tx.clone();
        let runtime_tasks = Arc::clone(&self.runtime_tasks);
        let mut worker = DocWorker {
            doc_id,
            state: DocWorkerDocState::Unloaded,
            pending_fragment_requests: BTreeSet::new(),
            subduction: Arc::clone(&self.subduction),
            sedimentrees: Arc::clone(&self.sedimentrees),
            storage_for_reads: self.storage_for_reads.clone(),
            partition_store: Arc::clone(&self.partition_store),
            change_manager: Arc::clone(&self.change_manager),
            runtime_handle: BigRepoRuntimeHandle {
                cmd_tx: self.cmd_tx.clone(),
            },
            runtime_evt_tx: runtime_evt_tx.clone(),
            shutdown: worker_cancel.clone(),
        };
        runtime_tasks
            .spawn(async move {
                let res: Res<()> = async {
                    loop {
                        tokio::select! {
                            biased;
                            _ = worker_cancel.cancelled() => break,
                            msg = msg_rx.recv() => {
                                let Some(msg) = msg else { break; };
                                worker.handle_msg(msg).await?;
                            }
                        }
                    }
                    Ok(())
                }
                .await;
                if let Err(err) = res {
                    send_runtime_evt(
                        &runtime_evt_tx,
                        &worker_cancel,
                        RuntimeEvt::FatalWorkerError {
                            doc_id: Some(doc_id),
                            context: "doc worker",
                            error: format!("{err:?}"),
                        },
                    );
                }
                send_runtime_evt(
                    &worker.runtime_evt_tx,
                    &worker_cancel,
                    RuntimeEvt::DocWorkerStopped { doc_id },
                );
            })
            .expect(ERROR_TOKIO);
        self.doc_workers.insert(
            doc_id,
            DocWorkerEntry {
                handle: DocWorkerHandle { msg_tx },
                stop: DocWorkerStopToken { cancel_token },
                local_handles: 0,
                transient_work: 0,
                eviction_deadline: Some(Instant::now() + DOC_WORKER_IDLE_TTL),
            },
        );
        Ok(())
    }

    fn doc_worker_handle(&mut self, doc_id: DocumentId) -> Res<DocWorkerHandle> {
        self.spawn_doc_worker(doc_id)?;
        let entry = self
            .doc_workers
            .get_mut(&doc_id)
            .ok_or_eyre("doc worker missing after spawn")?;
        entry.eviction_deadline = None;
        Ok(entry.handle.clone())
    }

    async fn handle_put_doc(
        &mut self,
        doc_id: DocumentId,
        initial_content: automerge::Automerge,
        done: oneshot::Sender<Res<Arc<LiveDocBundle>>>,
    ) {
        let worker = self.doc_worker_handle(doc_id).expect(ERROR_ACTOR);
        worker
            .send(DocWorkerMsg::PutDoc {
                initial_content,
                done,
            })
            .expect(ERROR_ACTOR);
    }

    async fn handle_get_doc_handle(
        &mut self,
        doc_id: DocumentId,
        done: oneshot::Sender<Res<Option<Arc<LiveDocBundle>>>>,
    ) {
        let worker = self.doc_worker_handle(doc_id).expect(ERROR_ACTOR);
        worker
            .send(DocWorkerMsg::AcquireHandle { done })
            .expect(ERROR_ACTOR);
    }

    async fn handle_export_doc_save(
        &mut self,
        doc_id: DocumentId,
        done: oneshot::Sender<Res<Option<Vec<u8>>>>,
    ) {
        let worker = self.doc_worker_handle(doc_id).expect(ERROR_ACTOR);
        if let Some(entry) = self.doc_workers.get_mut(&doc_id) {
            entry.transient_work += 1;
        }
        worker
            .send(DocWorkerMsg::ExportDocSave { done })
            .expect(ERROR_ACTOR);
    }

    fn register_live_bundle(&mut self, bundle: Arc<LiveDocBundle>) {
        self.live_bundles
            .insert(bundle.doc_id, Arc::downgrade(&bundle));
    }

    async fn handle_release_doc_lease(&mut self, doc_id: DocumentId) {
        if let Some(entry) = self.doc_workers.get_mut(&doc_id) {
            entry.local_handles = entry.local_handles.saturating_sub(1);
            entry
                .handle
                .send(DocWorkerMsg::ReleaseHandleLease)
                .expect(ERROR_ACTOR);
        }
        self.schedule_doc_worker_eviction_if_idle(doc_id);
    }

    async fn handle_commit_delta(
        &mut self,
        doc_id: DocumentId,
        commits: Vec<(CommitId, BTreeSet<CommitId>, Vec<u8>)>,
        heads: Vec<automerge::ChangeHash>,
        patches: Vec<automerge::Patch>,
        origin: BigRepoChangeOrigin,
        done: oneshot::Sender<Res<()>>,
    ) {
        let worker = self.doc_worker_handle(doc_id).expect(ERROR_ACTOR);
        if let Some(entry) = self.doc_workers.get_mut(&doc_id) {
            entry.transient_work += 1;
        }
        worker
            .send(DocWorkerMsg::CommitDelta {
                commits,
                heads,
                patches,
                origin,
                done,
            })
            .expect(ERROR_ACTOR);
    }

    #[tracing::instrument(skip_all, fields(session = ?session))]
    async fn handle_sync_session_observed(
        &mut self,
        session: subduction_core::sync_session::SyncSession,
    ) {
        let doc_id: DocumentId = session.sedimentree_id.into();
        debug!(
            peer_id = %session.peer_id,
            kind = ?session.kind,
            "observed sync session"
        );
        let worker = self.doc_worker_handle(doc_id).expect(ERROR_ACTOR);
        if let Some(entry) = self.doc_workers.get_mut(&doc_id) {
            entry.transient_work += 1;
        }
        worker
            .send(DocWorkerMsg::ApplySyncSession { session })
            .expect(ERROR_ACTOR);
    }
}

#[derive(Clone)]
struct DocWorkerHandle {
    msg_tx: mpsc::UnboundedSender<DocWorkerMsg>,
}

impl DocWorkerHandle {
    fn send(&self, msg: DocWorkerMsg) -> Res<()> {
        self.msg_tx.send(msg).map_err(|_| eyre::eyre!(ERROR_ACTOR))
    }
}

struct DocWorkerStopToken {
    cancel_token: CancellationToken,
}

impl DocWorkerStopToken {
    fn cancel(&self) {
        self.cancel_token.cancel();
    }
}

struct DocWorkerEntry {
    handle: DocWorkerHandle,
    stop: DocWorkerStopToken,
    local_handles: usize,
    transient_work: usize,
    eviction_deadline: Option<Instant>,
}

enum DocWorkerMsg {
    PutDoc {
        initial_content: automerge::Automerge,
        done: oneshot::Sender<Res<Arc<LiveDocBundle>>>,
    },
    AcquireHandle {
        done: oneshot::Sender<Res<Option<Arc<LiveDocBundle>>>>,
    },
    ExportDocSave {
        done: oneshot::Sender<Res<Option<Vec<u8>>>>,
    },
    CommitDelta {
        commits: Vec<(CommitId, BTreeSet<CommitId>, Vec<u8>)>,
        heads: Vec<automerge::ChangeHash>,
        patches: Vec<automerge::Patch>,
        origin: BigRepoChangeOrigin,
        done: oneshot::Sender<Res<()>>,
    },
    ApplySyncSession {
        session: subduction_core::sync_session::SyncSession,
    },
    SyncWithPeer {
        peer_id: PeerId,
        subscribe: bool,
        timeout: Option<Duration>,
        done: oneshot::Sender<Res<SyncDocOutcome>>,
    },
    ReleaseHandleLease,
}

struct DocWorker<S>
where
    S: RuntimeSubductionStorage,
{
    doc_id: DocumentId,
    state: DocWorkerDocState,
    pending_fragment_requests: BTreeSet<FragmentRequested>,
    subduction: Arc<RuntimeSubduction<S>>,
    sedimentrees: SubductionSedimentrees,
    storage_for_reads: S,
    partition_store: Arc<PartitionStore>,
    change_manager: Arc<changes::ChangeListenerManager>,
    runtime_handle: BigRepoRuntimeHandle,
    runtime_evt_tx: mpsc::UnboundedSender<RuntimeEvt>,
    shutdown: CancellationToken,
}

enum DocWorkerDocState {
    Unloaded,
    Transient(automerge::Automerge),
    Live(Arc<LiveDocBundle>),
}

impl<S> DocWorker<S>
where
    S: RuntimeSubductionStorage,
{
    async fn handle_msg(&mut self, msg: DocWorkerMsg) -> Res<()> {
        match msg {
            DocWorkerMsg::PutDoc {
                initial_content,
                done,
            } => {
                let res = self.handle_put_doc(initial_content).await;
                done.send(res).expect(ERROR_CALLER);
            }
            DocWorkerMsg::AcquireHandle { done } => {
                let res = self.handle_acquire_handle().await;
                done.send(res).expect(ERROR_CALLER);
            }
            DocWorkerMsg::ExportDocSave { done } => {
                let res = self.handle_export_doc_save().await;
                send_runtime_evt(
                    &self.runtime_evt_tx,
                    &self.shutdown,
                    RuntimeEvt::DocWorkerTransientFinished {
                        doc_id: self.doc_id,
                    },
                );
                done.send(res).expect(ERROR_CALLER);
            }
            DocWorkerMsg::CommitDelta {
                commits,
                heads,
                patches,
                origin,
                done,
            } => {
                let res = self
                    .handle_commit_delta(commits, heads, patches, origin)
                    .await;
                send_runtime_evt(
                    &self.runtime_evt_tx,
                    &self.shutdown,
                    RuntimeEvt::DocWorkerTransientFinished {
                        doc_id: self.doc_id,
                    },
                );
                done.send(res).expect(ERROR_CALLER);
            }
            DocWorkerMsg::ApplySyncSession { session } => {
                self.handle_apply_sync_session(session).await?;
                send_runtime_evt(
                    &self.runtime_evt_tx,
                    &self.shutdown,
                    RuntimeEvt::DocWorkerTransientFinished {
                        doc_id: self.doc_id,
                    },
                );
            }
            DocWorkerMsg::SyncWithPeer {
                peer_id,
                subscribe,
                timeout,
                done,
            } => {
                let res = self
                    .handle_sync_with_peer(peer_id, subscribe, timeout)
                    .await;
                send_runtime_evt(
                    &self.runtime_evt_tx,
                    &self.shutdown,
                    RuntimeEvt::DocWorkerTransientFinished {
                        doc_id: self.doc_id,
                    },
                );
                done.send(res).expect(ERROR_CALLER);
            }
            DocWorkerMsg::ReleaseHandleLease => {}
        }
        Ok(())
    }

    async fn handle_put_doc(
        &mut self,
        initial_content: automerge::Automerge,
    ) -> Res<Arc<LiveDocBundle>> {
        if !matches!(self.state, DocWorkerDocState::Unloaded) {
            eyre::bail!("document already exists locally");
        }
        if load_doc_snapshot(&self.sedimentrees, &self.storage_for_reads, self.doc_id)
            .await?
            .is_some()
        {
            eyre::bail!("document already exists locally");
        }
        let sedimentree_id: SedimentreeId = self.doc_id.into();
        let ingested =
            automerge_sedimentree::ingest::ingest_automerge(&initial_content, sedimentree_id)
                .map_err(|err| ferr!("failed ingesting automerge doc: {err}"))?;
        self.subduction
            .add_sedimentree(sedimentree_id, ingested.sedimentree, ingested.blobs)
            .await
            .map_err(|err| ferr!("failed add_sedimentree: {err}"))?;
        self.upsert_known_doc_now().await?;
        let bundle = Arc::new(LiveDocBundle::new(
            self.doc_id,
            initial_content,
            ingested.fragment_state_store,
            RuntimeDocLease {
                runtime: self.runtime_handle.clone(),
                doc_id: self.doc_id,
            },
        ));
        let (item_payload, heads) = {
            let doc = bundle.doc.lock().await;
            let heads = Arc::<[automerge::ChangeHash]>::from(doc.get_heads());
            let item_payload = serde_json::json!({
                "heads": crate::serialize_commit_heads(&heads),
            });
            (item_payload, heads)
        };
        self.partition_store
            .record_member_item_change(&self.doc_id.to_string(), &item_payload)
            .await?;
        self.change_manager
            .notify_doc_created(self.doc_id, Arc::clone(&heads))?;
        self.change_manager
            .notify_local_doc_created(self.doc_id, heads)?;
        self.state = DocWorkerDocState::Live(Arc::clone(&bundle));
        send_runtime_evt(
            &self.runtime_evt_tx,
            &self.shutdown,
            RuntimeEvt::DocWorkerHandleAcquired {
                bundle: Arc::clone(&bundle),
            },
        );
        Ok(bundle)
    }

    async fn handle_acquire_handle(&mut self) -> Res<Option<Arc<LiveDocBundle>>> {
        if let DocWorkerDocState::Live(bundle) = &self.state {
            send_runtime_evt(
                &self.runtime_evt_tx,
                &self.shutdown,
                RuntimeEvt::DocWorkerHandleAcquired {
                    bundle: Arc::clone(bundle),
                },
            );
            return Ok(Some(Arc::clone(bundle)));
        }
        let Some(doc) = self.take_or_load_transient_doc().await? else {
            return Ok(None);
        };
        let fragment_state_store = build_fragment_state_store(&doc).await?;
        let bundle = Arc::new(LiveDocBundle::new(
            self.doc_id,
            doc,
            fragment_state_store,
            RuntimeDocLease {
                runtime: self.runtime_handle.clone(),
                doc_id: self.doc_id,
            },
        ));
        self.state = DocWorkerDocState::Live(Arc::clone(&bundle));
        send_runtime_evt(
            &self.runtime_evt_tx,
            &self.shutdown,
            RuntimeEvt::DocWorkerHandleAcquired {
                bundle: Arc::clone(&bundle),
            },
        );
        Ok(Some(bundle))
    }

    async fn handle_export_doc_save(&mut self) -> Res<Option<Vec<u8>>> {
        match &self.state {
            DocWorkerDocState::Live(bundle) => {
                let doc = bundle.doc.lock().await;
                Ok(Some(doc.save()))
            }
            DocWorkerDocState::Transient(doc) => Ok(Some(doc.save())),
            DocWorkerDocState::Unloaded => {
                let Some(doc) =
                    load_doc_snapshot(&self.sedimentrees, &self.storage_for_reads, self.doc_id)
                        .await?
                else {
                    return Ok(None);
                };
                let save = doc.save();
                self.state = DocWorkerDocState::Transient(doc);
                Ok(Some(save))
            }
        }
    }

    async fn handle_commit_delta(
        &mut self,
        commits: Vec<(CommitId, BTreeSet<CommitId>, Vec<u8>)>,
        heads: Vec<automerge::ChangeHash>,
        patches: Vec<automerge::Patch>,
        origin: BigRepoChangeOrigin,
    ) -> Res<()> {
        let sedimentree_id: SedimentreeId = self.doc_id.into();
        for (head, parents, blob) in commits {
            let maybe_request = self
                .subduction
                .add_commit(sedimentree_id, head, parents, Blob::new(blob))
                .await
                .map_err(|err| ferr!("failed add_commit: {err}"))?;
            if let Some(request) = maybe_request {
                self.pending_fragment_requests.insert(request);
            }
        }
        commit_delta_bookkeep(
            &self.partition_store,
            &self.change_manager,
            self.doc_id,
            heads,
            patches,
            origin,
        )
        .await?;
        self.process_pending_fragment_requests().await?;
        Ok(())
    }

    async fn handle_apply_sync_session(
        &mut self,
        session: subduction_core::sync_session::SyncSession,
    ) -> Res<()> {
        if session.received_commit_ids.is_empty() && session.received_fragment_ids.is_empty() {
            return Ok(());
        }
        let mut blobs = Vec::new();
        for commit_id in &session.received_commit_ids {
            let verified = self
                .storage_for_reads
                .load_loose_commit(session.sedimentree_id, *commit_id)
                .await?
                .ok_or_eyre("synced loose commit missing")?;
            blobs.push(verified.blob().clone().into_contents());
        }
        for fragment_id in &session.received_fragment_ids {
            let verified = self
                .storage_for_reads
                .load_fragment(session.sedimentree_id, *fragment_id)
                .await?
                .ok_or_eyre("synced fragment missing")?;
            blobs.push(verified.blob().clone().into_contents());
        }
        let maybe_delta = self
            .current_doc_mut(|doc| {
                let before_heads = doc.get_heads();
                for blob in blobs {
                    doc.load_incremental(&blob)
                        .map_err(|err| eyre::eyre!("failed applying sync session blob: {err}"))?;
                }
                let after_heads = doc.get_heads();
                if before_heads == after_heads {
                    return Ok(None);
                }
                let patches = doc.diff(&before_heads, &after_heads);
                Ok(Some((after_heads, patches)))
            })
            .await?;
        let Some((after_heads, patches)) = maybe_delta else {
            return Ok(());
        };
        commit_delta_bookkeep(
            &self.partition_store,
            &self.change_manager,
            self.doc_id,
            after_heads,
            patches,
            BigRepoChangeOrigin::Remote {
                peer_id: session.peer_id.into(),
            },
        )
        .await
    }

    async fn handle_sync_with_peer(
        &self,
        peer_id: PeerId,
        subscribe: bool,
        timeout: Option<Duration>,
    ) -> Res<SyncDocOutcome> {
        let sedimentree_id: SedimentreeId = self.doc_id.into();
        let remote_peer_id: subduction_core::peer::id::PeerId = peer_id.into();
        let result = self
            .subduction
            .sync_with_peer(&remote_peer_id, sedimentree_id, subscribe, timeout)
            .await;
        Ok(match result {
            Ok((had_success, _stats, conn_errs)) => {
                if had_success {
                    SyncDocOutcome::Success
                } else if conn_errs.is_empty() {
                    SyncDocOutcome::NotFoundOrUnauthorized
                } else {
                    SyncDocOutcome::TransportError
                }
            }
            Err(_) => SyncDocOutcome::IoError,
        })
    }

    async fn process_pending_fragment_requests(&mut self) -> Res<()> {
        let DocWorkerDocState::Live(bundle) = &self.state else {
            return Ok(());
        };
        if self.pending_fragment_requests.is_empty() {
            return Ok(());
        }
        let requests = std::mem::take(&mut self.pending_fragment_requests);
        process_fragment_requests(Arc::clone(bundle), requests, Arc::clone(&self.subduction)).await
    }

    async fn current_doc_mut<R>(
        &mut self,
        f: impl FnOnce(&mut automerge::Automerge) -> Res<R>,
    ) -> Res<R> {
        match std::mem::replace(&mut self.state, DocWorkerDocState::Unloaded) {
            DocWorkerDocState::Live(bundle) => {
                let mut doc = bundle.doc.lock().await;
                let out = f(&mut doc)?;
                drop(doc);
                self.state = DocWorkerDocState::Live(bundle);
                Ok(out)
            }
            DocWorkerDocState::Transient(mut doc) => {
                let out = f(&mut doc)?;
                self.state = DocWorkerDocState::Transient(doc);
                Ok(out)
            }
            DocWorkerDocState::Unloaded => {
                let mut doc =
                    load_doc_snapshot(&self.sedimentrees, &self.storage_for_reads, self.doc_id)
                        .await?
                        .unwrap_or_else(automerge::Automerge::new);
                let out = f(&mut doc)?;
                self.state = DocWorkerDocState::Transient(doc);
                Ok(out)
            }
        }
    }

    async fn take_or_load_transient_doc(&mut self) -> Res<Option<automerge::Automerge>> {
        match std::mem::replace(&mut self.state, DocWorkerDocState::Unloaded) {
            DocWorkerDocState::Transient(doc) => Ok(Some(doc)),
            DocWorkerDocState::Unloaded => {
                load_doc_snapshot(&self.sedimentrees, &self.storage_for_reads, self.doc_id).await
            }
            DocWorkerDocState::Live(bundle) => {
                self.state = DocWorkerDocState::Live(bundle);
                eyre::bail!("document already live")
            }
        }
    }

    async fn upsert_known_doc_now(&self) -> Res<()> {
        sqlx::query("INSERT INTO big_repo_docs(doc_id) VALUES(?) ON CONFLICT(doc_id) DO NOTHING")
            .bind(self.doc_id.to_string())
            .execute(self.partition_store.state_pool())
            .await
            .wrap_err("failed upserting big_repo_docs")?;
        Ok(())
    }
}

// connections support
impl<S> BigRepoRuntimeWorker<S>
where
    S: RuntimeSubductionStorage,
{
    async fn handle_connect_outgoing(
        &self,
        endpoint: iroh::Endpoint,
        endpoint_addr: iroh::EndpointAddr,
        peer_id: PeerId,
        done: oneshot::Sender<Res<PeerId>>,
    ) {
        let subduction: Arc<RuntimeSubduction<S>> = Arc::clone(&self.subduction);
        let connect_signer = self.connect_signer.clone();
        let evt_tx = self.evt_tx.clone();
        let shutdown = self.runtime_stop.clone();
        let runtime_tasks = Arc::clone(&self.runtime_tasks);
        let connected_peers = Arc::clone(&self.connected_peers);
        let fut = async move {
            if connected_peers.lock().await.contains_key(&peer_id) {
                return Ok(peer_id);
            }
            let connect = connect_outgoing(endpoint, endpoint_addr, &connect_signer)
                .await
                .map_err(|err| ferr!("failed subduction iroh connect: {err}"))?;
            let peer_id = PeerId::from(connect.authenticated.peer_id());
            let stop_token = RuntimePeerConnectionStopToken::new();
            let fut_listener = {
                let stop = stop_token.child_token();
                let peer_id = peer_id;
                let evt_tx = evt_tx.clone();
                let shutdown = shutdown.clone();
                let fut = connect.listener_task;
                async move {
                    tokio::select! {
                        biased;
                        _ = stop.cancelled() => {}
                        res = fut => {
                            match res {
                                Ok(()) => {
                                    send_runtime_evt(
                                        &evt_tx,
                                        &shutdown,
                                        RuntimeEvt::ConnectionLost { peer_id: peer_id },
                                    );
                                }
                                Err(err) => {
                                    warn!(?err, %peer_id, "connection listener task error");
                                    send_runtime_evt(
                                        &evt_tx,
                                        &shutdown,
                                        RuntimeEvt::ConnectionLost { peer_id: peer_id },
                                    );
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
                let shutdown = shutdown.clone();
                let fut = connect.sender_task;
                async move {
                    tokio::select! {
                        biased;
                        _ = stop.cancelled() => {}
                        res = fut => {
                            match res {
                                Ok(()) => {
                                    send_runtime_evt(
                                        &evt_tx,
                                        &shutdown,
                                        RuntimeEvt::ConnectionLost { peer_id: peer_id },
                                    );
                                }
                                Err(err) => {
                                    warn!(?err, %peer_id, "connection sender task error");
                                    send_runtime_evt(
                                        &evt_tx,
                                        &shutdown,
                                        RuntimeEvt::ConnectionLost { peer_id: peer_id },
                                    );
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
            send_runtime_evt(
                &evt_tx,
                &shutdown,
                RuntimeEvt::ConnectionEstablished { peer_id, stop_token },
            );
            Ok(peer_id)
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
        let shutdown = self.runtime_stop.clone();
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
                let shutdown = shutdown.clone();
                let peer_id = peer_id;
                let fut = accepted.listener_task;
                async move {
                    tokio::select! {
                        biased;
                        _ = stop.cancelled() => {}
                        res = fut => {
                            match res {
                                Ok(()) => {
                                    send_runtime_evt(
                                        &evt_tx,
                                        &shutdown,
                                        RuntimeEvt::ConnectionLost { peer_id },
                                    );
                                }
                                Err(err) => {
                                    warn!(?err, %peer_id, "incoming connection listener task exited");
                                    send_runtime_evt(
                                        &evt_tx,
                                        &shutdown,
                                        RuntimeEvt::ConnectionLost { peer_id: peer_id },
                                    );
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
                let shutdown = shutdown.clone();
                let fut = accepted.sender_task;
                async move {
                    tokio::select! {
                        biased;
                        _ = stop.cancelled() => {}
                        res = fut => {
                            match res {
                                Ok(()) => {
                                    send_runtime_evt(
                                        &evt_tx,
                                        &shutdown,
                                        RuntimeEvt::ConnectionLost { peer_id: peer_id },
                                    );
                                }
                                Err(err) => {
                                    warn!(?err, %peer_id, "incoming connection sender task exited");
                                    send_runtime_evt(
                                        &evt_tx,
                                        &shutdown,
                                        RuntimeEvt::ConnectionLost { peer_id: peer_id },
                                    );
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
            send_runtime_evt(
                &evt_tx,
                &shutdown,
                RuntimeEvt::ConnectionEstablished { peer_id, stop_token },
            );
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

    #[tracing::instrument(skip_all, fields(%peer_id))]
    async fn handle_connection_lost(&self, peer_id: PeerId) {
        let maybe_stop = self.connected_peers.lock().await.remove(&peer_id);
        if let Some(connection) = maybe_stop {
            connection.cancel();
            let remote_peer_id: subduction_core::peer::id::PeerId = peer_id.into();
            let _ = self.subduction.disconnect_from_peer(&remote_peer_id).await;
        }
    }

    #[tracing::instrument(skip_all, fields(%peer_id))]
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
}

// sync support
impl<S> BigRepoRuntimeWorker<S>
where
    S: RuntimeSubductionStorage,
{
    #[tracing::instrument(
        skip_all,
        fields(%doc_id, %peer_id, subscribe, timeout = ?timeout)
    )]
    async fn handle_sync_doc_with_peer(
        &mut self,
        doc_id: DocumentId,
        peer_id: PeerId,
        subscribe: bool,
        timeout: Option<Duration>,
        done: oneshot::Sender<Res<SyncDocOutcome>>,
    ) {
        let worker = self.doc_worker_handle(doc_id).expect(ERROR_ACTOR);
        if let Some(entry) = self.doc_workers.get_mut(&doc_id) {
            entry.transient_work += 1;
        }
        worker
            .send(DocWorkerMsg::SyncWithPeer {
                peer_id,
                subscribe,
                timeout,
                done,
            })
            .expect(ERROR_ACTOR);
    }

    #[tracing::instrument(skip_all, fields(%doc_id, %peer_id))]
    async fn handle_remote_heads_observed(&mut self, doc_id: DocumentId, peer_id: PeerId) {
        if self
            .live_bundles
            .get(&doc_id)
            .and_then(|entry| entry.upgrade())
            .is_none()
        {
            return;
        }
        debug!(%doc_id, %peer_id, "remote heads observed for live doc");
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
        match self.runtime_tasks.stop(Duration::from_secs(5)).await {
            Ok(()) => {}
            Err(utils_rs::AbortableJoinSetStopError::Timeout(_))
            | Err(utils_rs::AbortableJoinSetStopError::Aborted) => {}
            Err(err) => return Err(err.into()),
        }
        Ok(())
    }
}

async fn commit_delta_bookkeep(
    partition_store: &Arc<PartitionStore>,
    change_manager: &Arc<changes::ChangeListenerManager>,
    doc_id: DocumentId,
    heads: Vec<automerge::ChangeHash>,
    patches: Vec<automerge::Patch>,
    origin: BigRepoChangeOrigin,
) -> Res<()> {
    let item_payload = serde_json::json!({
        "heads": crate::serialize_commit_heads(&heads),
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
    bundle: Arc<LiveDocBundle>,
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
            .collect::<Vec<FragmentState<OwnedParents>>>();
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
        type Node = OwnedParents;
        type LookupError = Infallible;

        fn lookup(&self, id: CommitId) -> Result<Option<Self::Node>, Self::LookupError> {
            let change_hash = automerge::ChangeHash(*id.as_bytes());
            Ok(self.0.get_change_meta_by_hash(&change_hash).map(|meta| {
                OwnedParents::from(
                    meta.deps
                        .iter()
                        .map(|dep| CommitId::new(dep.0))
                        .collect::<sedimentree_core::collections::Set<_>>(),
                )
            }))
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

async fn build_fragment_state_store(
    doc: &automerge::Automerge,
) -> Res<sedimentree_core::collections::Map<CommitId, FragmentState<OwnedParents>>> {
    let metadata = doc.get_changes_meta(&[]);
    let store =
        automerge_sedimentree::indexed::IndexedSedimentreeAutomerge::from_metadata(&metadata);
    let heads: Vec<CommitId> = doc.get_heads().iter().map(|h| CommitId::new(h.0)).collect();
    let mut known: sedimentree_core::collections::Map<CommitId, FragmentState<OwnedParents>> =
        sedimentree_core::collections::Map::new();
    store
        .build_fragment_store(&heads, &mut known, &CountLeadingZeroBytes)
        .map_err(|err| ferr!("failed building fragment state store: {err}"))?;
    Ok(known)
}
