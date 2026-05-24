//! This is just a crappy reimpl of samod to get subduction working

use crate::interlude::*;

use crate::{BigRepoChangeOrigin, ConnFinishSignal, DocumentId, PeerId};

use core::convert::Infallible;
use futures::future::BoxFuture;
use sedimentree_core::commit::{CommitStore, CountLeadingZeroBytes, FragmentState};
use sedimentree_core::crypto::digest::Digest;
use sedimentree_core::sedimentree::{Sedimentree, SedimentreeItem};
use sedimentree_core::{blob::Blob, id::SedimentreeId, loose_commit::id::CommitId};
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::changes;
use automerge_sedimentree::indexed::OwnedParents;
use subduction_core::subduction::request::FragmentRequested;

const DOC_WORKER_IDLE_TTL: Duration = Duration::from_secs(3);
type SharedPartitionStore = Arc<dyn big_sync::HostPartStore>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncDocOutcome {
    Success,
    NotFoundOrUnauthorized,
    TransportError,
    IoError,
}

#[derive(educe::Educe)]
#[educe(Debug)]
pub struct LiveDocBundle {
    pub doc_id: DocumentId,
    #[educe(Debug(ignore))]
    pub doc: tokio::sync::Mutex<automerge::Automerge>,
    #[educe(Debug(ignore))]
    pub fragment_state_store: tokio::sync::Mutex<
        sedimentree_core::collections::Map<CommitId, FragmentState<OwnedParents>>,
    >,
    #[educe(Debug(ignore))]
    _lease: RuntimeDocLease,
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

#[derive(Clone, Debug)]
struct RuntimeDocLease {
    runtime: BigRepoRuntimeHandle,
    doc_id: DocumentId,
}

impl Drop for RuntimeDocLease {
    fn drop(&mut self) {
        self.runtime.release_doc_lease(self.doc_id);
    }
}

#[derive(thiserror::Error, displaydoc::Display, Debug)]
pub enum PutDocError {
    /// IdOccpuied {id}
    IdOccpuied { id: DocumentId },
    /// {0:}
    Other(#[from] eyre::Report),
}

enum RuntimeCmd {
    PutDoc {
        doc_id: DocumentId,
        initial_content: Box<automerge::Automerge>,
        resp: oneshot::Sender<Result<Arc<LiveDocBundle>, PutDocError>>,
    },
    GetDocHandle {
        doc_id: DocumentId,
        resp: oneshot::Sender<Res<Option<Arc<LiveDocBundle>>>>,
    },
    ExportDocSave {
        doc_id: DocumentId,
        resp: oneshot::Sender<Res<Option<Vec<u8>>>>,
    },
    CommitDelta {
        doc_id: DocumentId,
        commits: Vec<(CommitId, BTreeSet<CommitId>, Vec<u8>)>,
        heads: Vec<automerge::ChangeHash>,
        patches: Vec<automerge::Patch>,
        origin: BigRepoChangeOrigin,
        resp: oneshot::Sender<Res<()>>,
    },
    OpenConnIroh {
        endpoint: iroh::Endpoint,
        endpoint_addr: iroh::EndpointAddr,
        peer_id: PeerId,
        end_signal_tx: Option<tokio::sync::mpsc::UnboundedSender<ConnFinishSignal>>,
        resp: oneshot::Sender<Res<(PeerId, Arc<AtomicBool>)>>,
    },
    AcceptConnIroh {
        conn: iroh::endpoint::Connection,
        end_signal_tx: Option<tokio::sync::mpsc::UnboundedSender<ConnFinishSignal>>,
        resp: oneshot::Sender<Res<(PeerId, Arc<AtomicBool>)>>,
    },
    CloseConnIroh {
        peer_id: PeerId,
        resp: Option<oneshot::Sender<Res<()>>>,
    },
    SyncDocWithPeer {
        doc_id: DocumentId,
        peer_id: PeerId,
        timeout: Option<Duration>,
        resp: oneshot::Sender<Res<SyncDocOutcome>>,
    },
    ReleaseDocLease {
        doc_id: DocumentId,
    },
}

enum ConnTask {
    Sender,
    Listener,
}

enum RuntimeEvt {
    SyncSessionObserved {
        session: subduction_core::sync_session::SyncSession,
    },
    ConnEstablishedIroh {
        peer_id: PeerId,
        deets: RuntimePeerConnDeets,
    },
    ConnLostIroh {
        peer_id: PeerId,
        src_task: ConnTask,
        error: Option<subduction_iroh::error::RunError>,
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

struct RuntimePeerConnDeets {
    cancel_token: CancellationToken,
    end_signal_tx: Option<tokio::sync::mpsc::UnboundedSender<ConnFinishSignal>>,
    closed: Arc<AtomicBool>,
}

#[derive(Clone, Debug)]
pub struct BigRepoRuntimeHandle {
    cmd_tx: mpsc::UnboundedSender<RuntimeCmd>,
}

impl BigRepoRuntimeHandle {
    pub async fn put_doc(
        &self,
        doc_id: DocumentId,
        initial_content: automerge::Automerge,
    ) -> Result<Arc<LiveDocBundle>, PutDocError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(RuntimeCmd::PutDoc {
                doc_id,
                initial_content: initial_content.into(),
                resp: tx,
            })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?
    }

    pub async fn get_doc_handle(&self, doc_id: DocumentId) -> Res<Option<Arc<LiveDocBundle>>> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(RuntimeCmd::GetDocHandle { doc_id, resp: tx })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        let out = rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?;
        out
    }

    pub async fn export_doc_save(&self, doc_id: DocumentId) -> Res<Option<Vec<u8>>> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(RuntimeCmd::ExportDocSave { doc_id, resp: tx })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?
    }

    pub async fn commit_delta(
        &self,
        doc_id: DocumentId,
        commits: Vec<(CommitId, BTreeSet<CommitId>, Vec<u8>)>,
        heads: Vec<automerge::ChangeHash>,
        patches: Vec<automerge::Patch>,
        origin: BigRepoChangeOrigin,
    ) -> Res<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(RuntimeCmd::CommitDelta {
                doc_id,
                commits,
                heads,
                patches,
                origin,
                resp: tx,
            })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?
    }

    #[tracing::instrument(skip_all, fields(%peer_id))]
    pub async fn open_connection_iroh(
        &self,
        endpoint: iroh::Endpoint,
        endpoint_addr: iroh::EndpointAddr,
        peer_id: PeerId,
        end_signal_tx: Option<tokio::sync::mpsc::UnboundedSender<ConnFinishSignal>>,
    ) -> Res<(PeerId, Arc<AtomicBool>)> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(RuntimeCmd::OpenConnIroh {
                endpoint,
                endpoint_addr,
                peer_id,
                end_signal_tx,
                resp: tx,
            })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?
    }

    #[tracing::instrument(skip_all)]
    pub async fn accept_connection_iroh(
        &self,
        conn: iroh::endpoint::Connection,
        end_signal_tx: Option<tokio::sync::mpsc::UnboundedSender<ConnFinishSignal>>,
    ) -> Res<(PeerId, Arc<AtomicBool>)> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(RuntimeCmd::AcceptConnIroh {
                conn,
                end_signal_tx,
                resp: tx,
            })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?
    }

    #[tracing::instrument(skip_all, fields(%peer_id))]
    pub async fn close_peer_connection(&self, peer_id: PeerId) -> Res<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(RuntimeCmd::CloseConnIroh {
                peer_id,
                resp: Some(tx),
            })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?
    }

    #[tracing::instrument(
        skip_all,
        fields(%doc_id, %peer_id, timeout = ?timeout)
    )]
    pub async fn sync_doc_with_peer(
        &self,
        doc_id: DocumentId,
        peer_id: PeerId,
        timeout: Option<Duration>,
    ) -> Res<SyncDocOutcome> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(RuntimeCmd::SyncDocWithPeer {
                doc_id,
                peer_id,
                timeout,
                resp: tx,
            })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?
    }

    fn release_doc_lease(&self, doc_id: DocumentId) {
        if self
            .cmd_tx
            .send(RuntimeCmd::ReleaseDocLease { doc_id })
            .is_err()
        {
            //let bt = std::backtrace::Backtrace::capture();
            warn!(%doc_id, "runtime stopped before releasing doc lease");
        }
    }
}

pub struct BigRepoRuntimeStopToken {
    cancel_token: CancellationToken,
    machine_loop_handle: tokio::task::JoinHandle<()>,
    runtime_tasks: Arc<utils_rs::AbortableJoinSet>,
}

impl BigRepoRuntimeStopToken {
    pub async fn stop(self) -> Res<()> {
        self.cancel_token.cancel();
        match self.runtime_tasks.stop(Duration::from_secs(5)).await {
            Ok(()) => {}
            Err(utils_rs::AbortableJoinSetStopError::Timeout(_))
            | Err(utils_rs::AbortableJoinSetStopError::Aborted) => {}
            Err(err) => return Err(err.into()),
        }
        utils_rs::wait_on_handle_with_timeout(self.machine_loop_handle, Duration::from_secs(5))
            .await?;
        Ok(())
    }
}

type SubductionSedimentrees =
    Arc<subduction_core::sharded_map::ShardedMap<SedimentreeId, Sedimentree, 256>>;
type BigRepoIrohTransport = subduction_core::transport::message::MessageTransport<
    subduction_iroh::transport::IrohTransport,
>;
type BigRepoSyncHandler<S> = subduction_core::handler::sync::SyncHandler<
    future_form::Sendable,
    S,
    BigRepoIrohTransport,
    subduction_core::policy::open::OpenPolicy,
    sedimentree_core::depth::CountLeadingZeroBytes,
    256,
>;
type BigRepoSubduction<S> = subduction_core::subduction::Subduction<
    'static,
    future_form::Sendable,
    S,
    BigRepoIrohTransport,
    BigRepoSyncHandler<S>,
    subduction_core::policy::open::OpenPolicy,
    subduction_crypto::signer::memory::MemorySigner,
    subduction_websocket::tokio::TimeoutTokio,
    sedimentree_core::depth::CountLeadingZeroBytes,
    256,
>;
pub trait BigRepoSubductionStorage:
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
impl<T> BigRepoSubductionStorage for T where
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
struct BigRepoSyncSessionBridge {
    evt_tx: mpsc::UnboundedSender<RuntimeEvt>,
}

impl subduction_core::sync_session::SyncSessionObserver for BigRepoSyncSessionBridge {
    fn on_sync_session(&self, session: subduction_core::sync_session::SyncSession) {
        self.evt_tx
            .send(RuntimeEvt::SyncSessionObserved { session })
            .expect(ERROR_CHANNEL);
    }
}
pub fn spawn_big_repo_runtime<S>(
    signer: subduction_crypto::signer::memory::MemorySigner,
    storage: S,
    big_sync_store: SharedPartitionStore,
    change_manager: Arc<changes::ChangeListenerManager>,
) -> Res<(BigRepoRuntimeHandle, BigRepoRuntimeStopToken)>
where
    S: BigRepoSubductionStorage,
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
    let storage_for_reads = storage.clone();
    let sync_session_observer: Arc<
        dyn subduction_core::sync_session::SyncSessionObserver + Send + Sync,
    > = Arc::new(BigRepoSyncSessionBridge {
        evt_tx: evt_tx.clone(),
    });
    let storage_powerbox = subduction_core::storage::powerbox::StoragePowerbox::new(
        storage.clone(),
        Arc::clone(&policy),
    );
    let sync_handler = subduction_core::handler::sync::SyncHandler::new(
        Arc::clone(&sedimentrees),
        Arc::clone(&connections),
        Arc::clone(&subscriptions),
        storage_powerbox.clone(),
        Arc::clone(&pending_blob_requests),
        sedimentree_core::depth::CountLeadingZeroBytes,
    );
    sync_handler.set_sync_session_observer(Arc::clone(&sync_session_observer));
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
    let subduction_handle: Arc<BigRepoSubduction<S>> = Arc::clone(&subduction);
    let runtime_worker = BigRepoRuntimeWorker {
        subduction: subduction_handle,
        sedimentrees: Arc::clone(&sedimentrees),
        storage_for_reads,
        big_sync_store,
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

    let peer_id = runtime_worker.local_peer_id;

    runtime_tasks
        .spawn({
            let stop = runtime_stop.clone();
            async move {
                let _ = stop
                    .run_until_cancelled(async move {
                        listener.await.unwrap();
                    })
                    .await;
            }
            .instrument(
                tracing::info_span!("BigRepoRuntime subduction listener", peer_id = %peer_id),
            )
        })
        .expect(ERROR_TOKIO);
    runtime_tasks
        .spawn({
            let stop = runtime_stop.clone();
            async move {
                let _ = stop
                    .run_until_cancelled(async move {
                        // NOTE: manager only returns abort signal on Subduction drop
                        manager.await.ok();
                    })
                    .await;
            }
            .instrument(
                tracing::info_span!("BigRepoRuntime subduction manager", peer_id = %peer_id),
            )
        })
        .expect(ERROR_TOKIO);

    let fut = {
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
                        runtime_worker.handle_cmd(cmd).await?;
                    },
                    evt = evt_rx.recv() => {
                        let Some(evt) = evt else { break; };
                        runtime_worker.handle_evt(evt).await?;
                    }
                }
            }
            eyre::Ok(())
        }
        .instrument(tracing::info_span!("BigRepoRuntime machine_loop", peer_id = %peer_id))
    };
    // the main loop is not on the abortable join set
    // to allow the children to die first before main does
    let machine_loop_handle = tokio::spawn(async move {
        fut.await.unwrap();
    });

    Ok((
        BigRepoRuntimeHandle { cmd_tx },
        BigRepoRuntimeStopToken {
            cancel_token: runtime_stop,
            machine_loop_handle,
            runtime_tasks,
        },
    ))
}

struct BigRepoRuntimeWorker<S>
where
    S: BigRepoSubductionStorage,
{
    subduction: Arc<BigRepoSubduction<S>>,
    sedimentrees: SubductionSedimentrees,
    storage_for_reads: S,
    big_sync_store: SharedPartitionStore,
    change_manager: Arc<changes::ChangeListenerManager>,
    runtime_tasks: Arc<utils_rs::AbortableJoinSet>,
    connect_signer: subduction_crypto::signer::memory::MemorySigner,
    local_peer_id: subduction_core::peer::id::PeerId,
    nonce_cache: Arc<subduction_core::nonce_cache::NonceCache>,
    runtime_stop: CancellationToken,
    cmd_tx: mpsc::UnboundedSender<RuntimeCmd>,
    evt_tx: mpsc::UnboundedSender<RuntimeEvt>,
    connected_peers: Arc<tokio::sync::Mutex<HashMap<PeerId, RuntimePeerConnDeets>>>,
    doc_workers: HashMap<DocumentId, DocWorkerEntry>,
}

impl<S> BigRepoRuntimeWorker<S>
where
    S: BigRepoSubductionStorage,
{
    async fn handle_cmd(&mut self, cmd: RuntimeCmd) -> Res<()> {
        match cmd {
            RuntimeCmd::PutDoc {
                doc_id,
                initial_content,
                resp,
            } => {
                let worker = self.doc_worker_handle(doc_id).expect(ERROR_ACTOR);
                worker
                    .send(DocWorkerMsg::PutDoc {
                        initial_content,
                        resp,
                    })
                    .expect(ERROR_ACTOR);
            }
            RuntimeCmd::GetDocHandle { doc_id, resp } => {
                let worker = self.doc_worker_handle(doc_id).expect(ERROR_ACTOR);
                worker
                    .send(DocWorkerMsg::AcquireHandle { resp })
                    .expect(ERROR_ACTOR);
            }
            RuntimeCmd::ExportDocSave { doc_id, resp } => {
                let worker = self.doc_worker_handle(doc_id).expect(ERROR_ACTOR);
                if let Some(entry) = self.doc_workers.get_mut(&doc_id) {
                    entry.transient_work += 1;
                }
                worker
                    .send(DocWorkerMsg::ExportDocSave { resp })
                    .expect(ERROR_ACTOR);
            }
            RuntimeCmd::CommitDelta {
                doc_id,
                commits,
                heads,
                patches,
                origin,
                resp,
            } => {
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
                        resp,
                    })
                    .expect(ERROR_ACTOR);
            }
            RuntimeCmd::OpenConnIroh {
                endpoint,
                endpoint_addr,
                peer_id,
                end_signal_tx,
                resp: done,
            } => {
                self.handle_open_conn_iroh(endpoint, endpoint_addr, peer_id, end_signal_tx, done)
                    .await
            }
            RuntimeCmd::AcceptConnIroh {
                conn,
                end_signal_tx,
                resp: done,
            } => {
                self.handle_accept_conn_iroh(conn, end_signal_tx, done)
                    .await;
            }
            RuntimeCmd::CloseConnIroh {
                peer_id,
                resp: done,
            } => {
                self.handle_close_iroh(peer_id, done).await;
            }
            RuntimeCmd::SyncDocWithPeer {
                doc_id,
                peer_id,
                timeout,
                resp: done,
            } => {
                let worker = self.doc_worker_handle(doc_id).expect(ERROR_ACTOR);
                if let Some(entry) = self.doc_workers.get_mut(&doc_id) {
                    entry.transient_work += 1;
                }
                worker
                    .send(DocWorkerMsg::SyncWithPeer {
                        peer_id,
                        timeout,
                        done,
                    })
                    .expect(ERROR_ACTOR);
            }
            RuntimeCmd::ReleaseDocLease { doc_id } => self.handle_release_doc_lease(doc_id).await,
        }

        Ok(())
    }

    async fn handle_evt(&mut self, evt: RuntimeEvt) -> Res<()> {
        match evt {
            RuntimeEvt::SyncSessionObserved { session } => {
                self.handle_sync_session_observed(session).await;
            }
            RuntimeEvt::ConnEstablishedIroh {
                peer_id,
                deets: cancel_token,
            } => {
                self.handle_connection_established(peer_id, cancel_token)
                    .await;
            }
            RuntimeEvt::ConnLostIroh {
                peer_id,
                error,
                src_task,
            } => {
                self.handle_connection_lost(peer_id, error, src_task)
                    .await?;
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
        Ok(())
    }

    fn spawn_background<F>(&self, fut: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let stop = self.runtime_stop.child_token();
        self.runtime_tasks
            .spawn(
                async move {
                    let _ = stop.run_until_cancelled(fut).await;
                }
                .instrument(tracing::info_span!("spawn_background")),
            )
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
        if let Some(entry) = self.doc_workers.get_mut(&doc_id) {
            entry.local_handles += 1;
            entry.eviction_deadline = None;
        }
    }

    fn handle_doc_worker_stopped(&mut self, doc_id: DocumentId) {
        self.doc_workers.remove(&doc_id);
    }

    #[tracing::instrument(skip_all,fields(%doc_id))]
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
        let doc_id_subduction = SedimentreeId::new(doc_id.into_bytes());
        let mut worker = DocWorker {
            doc_id_subduction,
            doc_id,
            state: DocWorkerDocState::Unloaded,
            pending_fragment_requests: BTreeSet::new(),
            subduction: Arc::clone(&self.subduction),
            sedimentrees: Arc::clone(&self.sedimentrees),
            storage_for_reads: self.storage_for_reads.clone(),
            big_sync_store: Arc::clone(&self.big_sync_store),
            change_manager: Arc::clone(&self.change_manager),
            runtime_handle: BigRepoRuntimeHandle {
                cmd_tx: self.cmd_tx.clone(),
            },
            runtime_evt_tx,
            // shutdown: worker_cancel.clone(),
        };
        runtime_tasks
            .spawn(
                async move {
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
                    // FIXME: why two errors instead of stopped with error variant
                    if let Err(err) = res {
                        worker
                            .runtime_evt_tx
                            .send(RuntimeEvt::FatalWorkerError {
                                doc_id: Some(doc_id),
                                context: "doc worker",
                                error: format!("{err:?}"),
                            })
                            .inspect_err(|_| warn!(ERROR_CHANNEL))
                            .ok();
                    }
                    worker
                        .runtime_evt_tx
                        .send(RuntimeEvt::DocWorkerStopped { doc_id })
                        .inspect_err(|_| warn!(ERROR_CHANNEL))
                        .ok();
                }
                .in_current_span(),
            )
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

    #[tracing::instrument(skip_all)]
    async fn handle_sync_session_observed(
        &mut self,
        session: subduction_core::sync_session::SyncSession,
    ) {
        let doc_id = DocumentId::new(*session.sedimentree_id.as_bytes());
        debug!(
            peer_id = %session.peer_id,
            kind = ?session.kind,
            received_commit_ids = session.received_commit_ids.len(),
            received_fragment_ids = session.received_fragment_ids.len(),
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

// connections support
impl<S> BigRepoRuntimeWorker<S>
where
    S: BigRepoSubductionStorage,
{
    #[tracing::instrument(skip_all)]
    async fn handle_open_conn_iroh(
        &self,
        endpoint: iroh::Endpoint,
        endpoint_addr: iroh::EndpointAddr,
        peer_id: PeerId,
        end_signal_tx: Option<tokio::sync::mpsc::UnboundedSender<ConnFinishSignal>>,
        done: oneshot::Sender<Res<(PeerId, Arc<AtomicBool>)>>,
    ) {
        let subduction: Arc<BigRepoSubduction<S>> = Arc::clone(&self.subduction);
        let connect_signer = self.connect_signer.clone();
        let evt_tx = self.evt_tx.clone();
        let runtime_tasks = Arc::clone(&self.runtime_tasks);
        let connected_peers = Arc::clone(&self.connected_peers);
        let cancel_token = self.runtime_stop.child_token();
        let fut = async move {
            if let Some(deets) = connected_peers.lock().await.get(&peer_id) {
                return Ok((peer_id, Arc::clone(&deets.closed)));
            }
            let connect = connect_outgoing(endpoint, endpoint_addr, &connect_signer).await?;
            let peer_id = PeerId::new(*connect.authenticated.peer_id().as_bytes());
            for (fut, src_task) in [
                (connect.listener_task, ConnTask::Listener),
                (connect.sender_task, ConnTask::Sender),
            ] {
                let fut_wrapped = {
                    let cancel_token = cancel_token.clone();
                    let evt_tx = evt_tx.clone();
                    async move {
                        let Some(res) = cancel_token.run_until_cancelled(fut).await else {
                            // cancellation token lit
                            return;
                        };
                        match res {
                            Ok(()) => {
                                evt_tx
                                    .send(RuntimeEvt::ConnLostIroh {
                                        peer_id,
                                        error: None,
                                        src_task,
                                    })
                                    .expect(ERROR_CHANNEL);
                            }
                            Err(err) => {
                                evt_tx
                                    .send(RuntimeEvt::ConnLostIroh {
                                        peer_id,
                                        error: Some(err),
                                        src_task,
                                    })
                                    .expect(ERROR_CHANNEL);
                            }
                        }
                    }
                };
                runtime_tasks.spawn(fut_wrapped).expect(ERROR_TOKIO);
            }
            subduction
                .add_connection(connect.authenticated)
                .await
                .map_err(|err| {
                    cancel_token.cancel();
                    ferr!("failed subduction add_connection: {err}")
                })?;
            let closed = Arc::new(AtomicBool::new(false));
            evt_tx
                .send(RuntimeEvt::ConnEstablishedIroh {
                    peer_id,
                    deets: RuntimePeerConnDeets {
                        cancel_token,
                        end_signal_tx,
                        closed: Arc::clone(&closed),
                    },
                })
                .expect(ERROR_CHANNEL);
            Ok((peer_id, closed))
        };
        self.spawn_background(async move {
            done.send(fut.await)
                .inspect_err(|_| warn!(ERROR_CALLER))
                .ok();
        });
    }

    #[tracing::instrument(skip_all)]
    async fn handle_accept_conn_iroh(
        &self,
        conn: iroh::endpoint::Connection,
        end_signal_tx: Option<tokio::sync::mpsc::UnboundedSender<ConnFinishSignal>>,
        done: oneshot::Sender<Res<(PeerId, Arc<AtomicBool>)>>,
    ) {
        let subduction: Arc<BigRepoSubduction<S>> = Arc::clone(&self.subduction);
        let connect_signer = self.connect_signer.clone();
        let nonce_cache = Arc::clone(&self.nonce_cache);
        let local_peer_id = self.local_peer_id;
        let evt_tx = self.evt_tx.clone();
        let cancel_token = self.runtime_stop.child_token();
        let runtime_tasks = Arc::clone(&self.runtime_tasks);
        // let connected_peers = Arc::clone(&self.connected_peers);
        let fut = async move {
            // do handshake
            let accepted =
                accept_incoming(conn, &connect_signer, nonce_cache.as_ref(), local_peer_id).await?;
            let peer_id = PeerId::new(*accepted.authenticated.peer_id().as_bytes());

            // WARN: should we block incoming when there's already a connection?
            // if let Some(deets) = connected_peers.lock().await.get(&peer_id) {
            //     return Ok((peer_id, deets.closed.clone()));
            // }

            for (fut, src_task) in [
                (accepted.listener_task, ConnTask::Listener),
                (accepted.sender_task, ConnTask::Sender),
            ] {
                let fut_wrapped = {
                    let cancel_token = cancel_token.clone();
                    let evt_tx = evt_tx.clone();
                    async move {
                        let Some(res) = cancel_token.run_until_cancelled(fut).await else {
                            // cancellation token lit
                            return;
                        };
                        match res {
                            Ok(()) => {
                                evt_tx
                                    .send(RuntimeEvt::ConnLostIroh {
                                        peer_id,
                                        error: None,
                                        src_task,
                                    })
                                    .expect(ERROR_CHANNEL);
                            }
                            Err(err) => {
                                evt_tx
                                    .send(RuntimeEvt::ConnLostIroh {
                                        peer_id,
                                        error: Some(err),
                                        src_task,
                                    })
                                    .expect(ERROR_CHANNEL);
                            }
                        }
                    }
                };
                runtime_tasks.spawn(fut_wrapped).expect(ERROR_TOKIO);
            }
            subduction
                .add_connection(accepted.authenticated)
                .await
                .inspect_err(|_| {
                    cancel_token.cancel();
                })
                .wrap_err("failed subduction add_connection")?;
            let closed = Arc::new(AtomicBool::new(false));
            evt_tx
                .send(RuntimeEvt::ConnEstablishedIroh {
                    peer_id,
                    deets: RuntimePeerConnDeets {
                        cancel_token,
                        end_signal_tx,
                        closed: Arc::clone(&closed),
                    },
                })
                .expect(ERROR_CHANNEL);
            Ok((peer_id, closed))
        };
        self.spawn_background(async move {
            done.send(fut.await)
                .inspect_err(|_| warn!(ERROR_CALLER))
                .ok();
        });
    }

    #[tracing::instrument(skip_all)]
    async fn handle_close_iroh(&self, peer_id: PeerId, done: Option<oneshot::Sender<Res<()>>>) {
        let subduction: Arc<BigRepoSubduction<S>> = Arc::clone(&self.subduction);
        let connected_peers = Arc::clone(&self.connected_peers);
        self.spawn_background(async move {
            let out = async {
                let deets = connected_peers.lock().await.remove(&peer_id);
                if let Some(deets) = deets {
                    deets.closed.store(true, Ordering::SeqCst);
                    deets.cancel_token.cancel();
                    let remote_peer_id =
                        subduction_core::peer::id::PeerId::new(peer_id.into_bytes());
                    subduction
                        .disconnect_from_peer(&remote_peer_id)
                        .await
                        .map_err(|err| ferr!("failed subduction disconnect_from_peer: {err}"))?;
                }
                Ok(())
            }
            .await;
            if let Some(done) = done {
                done.send(out).inspect_err(|_| warn!(ERROR_CALLER)).ok();
            }
        });
    }

    #[tracing::instrument(skip_all, fields(%peer_id))]
    async fn handle_connection_lost(
        &self,
        peer_id: PeerId,
        err: Option<subduction_iroh::error::RunError>,
        _task: ConnTask,
    ) -> Res<()> {
        let deets = self.connected_peers.lock().await.remove(&peer_id);
        if let Some(deets) = deets {
            deets.closed.store(true, Ordering::SeqCst);
            deets.cancel_token.cancel();
            let remote_peer_id = subduction_core::peer::id::PeerId::new(peer_id.into_bytes());
            self.subduction
                .disconnect_from_peer(&remote_peer_id)
                .await
                .wrap_err("error on disconnect")?;
            if let Some(tx) = deets.end_signal_tx {
                tx.send(ConnFinishSignal {
                    peer_id,
                    err: err.map(|err| ferr!("connection task error: {err}")),
                })
                .inspect_err(|_| warn!(ERROR_CALLER))
                .ok();
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(%peer_id))]
    async fn handle_connection_established(&self, peer_id: PeerId, deets: RuntimePeerConnDeets) {
        let mut connected_peers = self.connected_peers.lock().await;
        if let Some(previous) = connected_peers.insert(peer_id, deets) {
            previous.cancel_token.cancel();
            panic!("fish trap: how did we establish a new connection without closing the previous? is this normal?")
        }
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
        initial_content: Box<automerge::Automerge>,
        resp: oneshot::Sender<Result<Arc<LiveDocBundle>, PutDocError>>,
    },
    AcquireHandle {
        resp: oneshot::Sender<Res<Option<Arc<LiveDocBundle>>>>,
    },
    ExportDocSave {
        resp: oneshot::Sender<Res<Option<Vec<u8>>>>,
    },
    CommitDelta {
        commits: Vec<(CommitId, BTreeSet<CommitId>, Vec<u8>)>,
        heads: Vec<automerge::ChangeHash>,
        patches: Vec<automerge::Patch>,
        origin: BigRepoChangeOrigin,
        resp: oneshot::Sender<Res<()>>,
    },
    ApplySyncSession {
        session: subduction_core::sync_session::SyncSession,
    },
    SyncWithPeer {
        peer_id: PeerId,
        timeout: Option<Duration>,
        done: oneshot::Sender<Res<SyncDocOutcome>>,
    },
    ReleaseHandleLease,
}

struct DocWorker<S>
where
    S: BigRepoSubductionStorage,
{
    doc_id: DocumentId,
    doc_id_subduction: SedimentreeId,
    state: DocWorkerDocState,
    pending_fragment_requests: BTreeSet<FragmentRequested>,
    subduction: Arc<BigRepoSubduction<S>>,
    sedimentrees: SubductionSedimentrees,
    storage_for_reads: S,
    big_sync_store: SharedPartitionStore,
    change_manager: Arc<changes::ChangeListenerManager>,
    runtime_handle: BigRepoRuntimeHandle,
    runtime_evt_tx: mpsc::UnboundedSender<RuntimeEvt>,
}

enum DocWorkerDocState {
    Unloaded,
    Transient(Box<automerge::Automerge>),
    Live(std::sync::Weak<LiveDocBundle>),
}

impl<S> DocWorker<S>
where
    S: BigRepoSubductionStorage,
{
    async fn handle_msg(&mut self, msg: DocWorkerMsg) -> Res<()> {
        match msg {
            DocWorkerMsg::PutDoc {
                initial_content,
                resp: done,
            } => {
                let res = self.handle_put_doc(initial_content).await;
                done.send(res).inspect_err(|_| warn!(ERROR_CALLER)).ok();
            }
            DocWorkerMsg::AcquireHandle { resp: done } => {
                let res = self.handle_acquire_handle().await;
                done.send(res).inspect_err(|_| warn!(ERROR_CALLER)).ok();
            }
            DocWorkerMsg::ExportDocSave { resp: done } => {
                let res = self.handle_export_doc_save().await;
                self.runtime_evt_tx
                    .send(RuntimeEvt::DocWorkerTransientFinished {
                        doc_id: self.doc_id,
                    })
                    .expect(ERROR_CHANNEL);
                done.send(res).inspect_err(|_| warn!(ERROR_CALLER)).ok();
            }
            DocWorkerMsg::CommitDelta {
                commits,
                heads,
                patches,
                origin,
                resp: done,
            } => {
                let res = self
                    .handle_commit_delta(commits, heads, patches, origin)
                    .await;
                self.runtime_evt_tx
                    .send(RuntimeEvt::DocWorkerTransientFinished {
                        doc_id: self.doc_id,
                    })
                    .expect(ERROR_CHANNEL);
                done.send(res).inspect_err(|_| warn!(ERROR_CALLER)).ok();
            }
            DocWorkerMsg::ApplySyncSession { session } => {
                self.handle_apply_sync_session(session).await?;
                self.runtime_evt_tx
                    .send(RuntimeEvt::DocWorkerTransientFinished {
                        doc_id: self.doc_id,
                    })
                    .expect(ERROR_CHANNEL);
            }
            DocWorkerMsg::SyncWithPeer {
                peer_id,
                timeout,
                done,
            } => {
                let res = self.handle_sync_with_peer(peer_id, timeout).await;
                let evt_tx: &mpsc::UnboundedSender<RuntimeEvt> = &self.runtime_evt_tx;
                let evt = RuntimeEvt::DocWorkerTransientFinished {
                    doc_id: self.doc_id,
                };
                evt_tx.send(evt).expect(ERROR_CHANNEL);
                done.send(res).inspect_err(|_| warn!(ERROR_CALLER)).ok();
            }
            DocWorkerMsg::ReleaseHandleLease => {}
        }
        Ok(())
    }

    async fn handle_put_doc(
        &mut self,
        doc: Box<automerge::Automerge>,
    ) -> Result<Arc<LiveDocBundle>, PutDocError> {
        if !matches!(self.state, DocWorkerDocState::Unloaded) {
            return Err(PutDocError::IdOccpuied { id: self.doc_id });
        }
        if load_doc_snapshot(&self.sedimentrees, &self.storage_for_reads, self.doc_id)
            .await?
            .is_some()
        {
            return Err(PutDocError::IdOccpuied { id: self.doc_id });
        }
        let sedimentree_id: SedimentreeId = self.doc_id_subduction;
        let ingested = automerge_sedimentree::ingest::ingest_automerge(&doc, sedimentree_id)
            .map_err(|err| ferr!("failed ingesting automerge doc: {err}"))?;
        info!("adding sedimentree");
        self.subduction
            .add_sedimentree(sedimentree_id, ingested.sedimentree, ingested.blobs)
            .await
            .map_err(|err| ferr!("failed add_sedimentree: {err}"))?;
        let (item_payload, heads) = {
            let heads = Arc::<[automerge::ChangeHash]>::from(doc.get_heads());
            let item_payload = serde_json::json!({
                "heads": am_utils_rs::serialize_commit_heads(&heads),
            });
            (item_payload, heads)
        };
        let bundle = Arc::new(LiveDocBundle::new(
            self.doc_id,
            *doc,
            ingested.fragment_state_store,
            RuntimeDocLease {
                runtime: self.runtime_handle.clone(),
                doc_id: self.doc_id,
            },
        ));
        self.big_sync_store
            .set_obj_payload(self.doc_id, item_payload, vec![], None)
            .await?;
        self.change_manager
            .notify_doc_created(self.doc_id, Arc::clone(&heads))?;
        self.change_manager
            .notify_local_doc_created(self.doc_id, Arc::clone(&heads))?;
        self.state = DocWorkerDocState::Live(Arc::downgrade(&bundle));
        self.runtime_evt_tx
            .send(RuntimeEvt::DocWorkerHandleAcquired {
                bundle: Arc::clone(&bundle),
            })
            .expect(ERROR_CHANNEL);
        Ok(bundle)
    }

    async fn handle_acquire_handle(&mut self) -> Res<Option<Arc<LiveDocBundle>>> {
        if let DocWorkerDocState::Live(bundle) = &self.state {
            if let Some(bundle) = bundle.upgrade() {
                self.runtime_evt_tx
                    .send(RuntimeEvt::DocWorkerHandleAcquired {
                        bundle: Arc::clone(&bundle),
                    })
                    .expect(ERROR_CHANNEL);
                return Ok(Some(Arc::clone(&bundle)));
            }
            self.state = DocWorkerDocState::Unloaded;
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
        self.state = DocWorkerDocState::Live(Arc::downgrade(&bundle));
        self.runtime_evt_tx
            .send(RuntimeEvt::DocWorkerHandleAcquired {
                bundle: Arc::clone(&bundle),
            })
            .expect(ERROR_CHANNEL);
        Ok(Some(bundle))
    }

    async fn handle_export_doc_save(&mut self) -> Res<Option<Vec<u8>>> {
        match &self.state {
            DocWorkerDocState::Live(bundle) => match bundle.upgrade() {
                Some(bundle) => {
                    let doc = bundle.doc.lock().await;
                    Ok(Some(doc.save()))
                }
                None => {
                    let Some(doc) =
                        load_doc_snapshot(&self.sedimentrees, &self.storage_for_reads, self.doc_id)
                            .await?
                    else {
                        return Ok(None);
                    };
                    let save = doc.save();
                    self.state = DocWorkerDocState::Transient(doc.into());
                    Ok(Some(save))
                }
            },
            DocWorkerDocState::Transient(doc) => Ok(Some(doc.save())),
            DocWorkerDocState::Unloaded => {
                let Some(doc) =
                    load_doc_snapshot(&self.sedimentrees, &self.storage_for_reads, self.doc_id)
                        .await?
                else {
                    return Ok(None);
                };
                let save = doc.save();
                self.state = DocWorkerDocState::Transient(doc.into());
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
        let sedimentree_id: SedimentreeId = self.doc_id_subduction;
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
            &self.big_sync_store,
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

    #[tracing::instrument(
        skip_all,
        fields(peer_id = %session.peer_id,kind = ?session.kind)
    )]
    async fn handle_apply_sync_session(
        &mut self,
        session: subduction_core::sync_session::SyncSession,
    ) -> Res<()> {
        info!(
            doc_id = %self.doc_id,
            peer_id = %session.peer_id,
            kind = ?session.kind,
            received_commit_ids = session.received_commit_ids.len(),
            received_fragment_ids = session.received_fragment_ids.len(),
            "applying sync session"
        );
        if session.is_empty() {
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
        info!(
            doc_id = %self.doc_id,
            peer_id = %session.peer_id,
            kind = ?session.kind,
            blobs = blobs.len(),
            is_unloaded = matches!(self.state, DocWorkerDocState::Unloaded),
            is_transient = matches!(self.state, DocWorkerDocState::Transient(_)),
            is_live = matches!(self.state, DocWorkerDocState::Live(_)),
            "sync session applied blobs and is selecting doc state path"
        );
        let maybe_delta = match std::mem::replace(&mut self.state, DocWorkerDocState::Unloaded) {
            DocWorkerDocState::Unloaded => {
                // since the doc in storage will have the latest blobs,
                // we must load the before_heads from the partition payloads
                info!(
                    doc_id = %self.doc_id,
                    peer_id = %session.peer_id,
                    kind = ?session.kind,
                    "unloaded sync session loading doc snapshot"
                );
                let mut doc =
                    load_doc_snapshot(&self.sedimentrees, &self.storage_for_reads, self.doc_id)
                        .await?
                        .unwrap_or_else(automerge::Automerge::new);
                info!(
                    doc_id = %self.doc_id,
                    peer_id = %session.peer_id,
                    kind = ?session.kind,
                    "unloaded sync session loading partition heads"
                );
                let before_heads =
                    super::partition_doc_heads_payload(&self.big_sync_store, self.doc_id)
                        .await?
                        .unwrap_or_else(|| doc.get_heads().into());
                info!(
                    doc_id = %self.doc_id,
                    peer_id = %session.peer_id,
                    kind = ?session.kind,
                    heads = before_heads.len(),
                    loaded_heads = doc.get_heads().len(),
                    "unloaded sync session loaded doc snapshot"
                );
                let loaded_heads = doc.get_heads();
                let out = if before_heads[..] == loaded_heads[..] {
                    for blob in blobs {
                        doc.load_incremental(&blob).map_err(|err| {
                            eyre::eyre!("failed applying sync session blob: {err}")
                        })?;
                    }
                    let after_heads = doc.get_heads();
                    if before_heads[..] == after_heads[..] {
                        info!(
                            doc_id = %self.doc_id,
                            peer_id = %session.peer_id,
                            kind = ?session.kind,
                            "unloaded sync session produced no delta after applying blobs"
                        );
                        None
                    } else {
                        let patches = doc.diff(&before_heads, &after_heads);
                        info!(
                            doc_id = %self.doc_id,
                            peer_id = %session.peer_id,
                            kind = ?session.kind,
                            heads = after_heads.len(),
                            patches = patches.len(),
                            "unloaded sync session produced delta after applying blobs"
                        );
                        Some((after_heads, patches))
                    }
                } else {
                    let patches = doc.diff(&before_heads, &loaded_heads);
                    info!(
                        doc_id = %self.doc_id,
                        peer_id = %session.peer_id,
                        kind = ?session.kind,
                        heads = loaded_heads.len(),
                        patches = patches.len(),
                        "unloaded sync session used loaded snapshot delta"
                    );
                    Some((loaded_heads, patches))
                };
                self.state = DocWorkerDocState::Transient(doc.into());
                out
            }
            DocWorkerDocState::Transient(mut doc) => {
                let out = {
                    let before_heads = doc.get_heads();
                    for blob in blobs {
                        doc.load_incremental(&blob).map_err(|err| {
                            eyre::eyre!("failed applying sync session blob: {err}")
                        })?;
                    }
                    let after_heads = doc.get_heads();
                    if before_heads == after_heads {
                        None
                    } else {
                        let patches = doc.diff(&before_heads, &after_heads);
                        Some((after_heads, patches))
                    }
                };
                self.state = DocWorkerDocState::Transient(doc);
                out
            }
            DocWorkerDocState::Live(bundle) => match bundle.upgrade() {
                Some(bundle) => {
                    info!(doc_id = %self.doc_id, "current doc mut using live bundle");
                    let mut doc = bundle.doc.lock().await;
                    let before_heads = doc.get_heads();
                    for blob in blobs {
                        doc.load_incremental(&blob).map_err(|err| {
                            eyre::eyre!("failed applying sync session blob: {err}")
                        })?;
                    }
                    let after_heads = doc.get_heads();
                    let out = if before_heads == after_heads {
                        None
                    } else {
                        let patches = doc.diff(&before_heads, &after_heads);
                        Some((after_heads, patches))
                    };
                    drop(doc);
                    self.state = DocWorkerDocState::Live(Arc::downgrade(&bundle));
                    out
                }
                None => {
                    info!(
                        doc_id = %self.doc_id,
                        peer_id = %session.peer_id,
                        kind = ?session.kind,
                        "live bundle expired; recovering sync delta from partition heads"
                    );
                    let mut doc =
                        load_doc_snapshot(&self.sedimentrees, &self.storage_for_reads, self.doc_id)
                            .await?
                            .unwrap_or_else(automerge::Automerge::new);
                    let loaded_heads = doc.get_heads();
                    let before_heads =
                        super::partition_doc_heads_payload(&self.big_sync_store, self.doc_id)
                            .await?
                            .unwrap_or_else(|| loaded_heads.clone().into());
                    let out = if before_heads[..] == loaded_heads[..] {
                        for blob in blobs {
                            doc.load_incremental(&blob).map_err(|err| {
                                eyre::eyre!("failed applying sync session blob: {err}")
                            })?;
                        }
                        let after_heads = doc.get_heads();
                        if before_heads[..] == after_heads[..] {
                            None
                        } else {
                            let patches = doc.diff(&before_heads, &after_heads);
                            Some((after_heads, patches))
                        }
                    } else {
                        let patches = doc.diff(&before_heads, &loaded_heads);
                        Some((loaded_heads, patches))
                    };
                    self.state = DocWorkerDocState::Transient(doc.into());
                    out
                }
            },
        };
        let Some((after_heads, patches)) = maybe_delta else {
            return Ok(());
        };
        commit_delta_bookkeep(
            &self.big_sync_store,
            &self.change_manager,
            self.doc_id,
            after_heads,
            patches,
            BigRepoChangeOrigin::Remote {
                peer_id: PeerId::new(*session.peer_id.as_bytes()),
            },
        )
        .await
    }

    async fn handle_sync_with_peer(
        &self,
        peer_id: PeerId,
        timeout: Option<Duration>,
    ) -> Res<SyncDocOutcome> {
        let sedimentree_id = self.doc_id_subduction;
        let remote_peer_id = subduction_core::peer::id::PeerId::new(peer_id.into_bytes());
        let result = self
            .subduction
            .sync_with_peer(&remote_peer_id, sedimentree_id, false, timeout)
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

    // FIXME: this only does fragment processing if there's
    // pending requests, is this wise? seems efficient but
    // confirm that we can defer fragment processing like this
    async fn process_pending_fragment_requests(&mut self) -> Res<()> {
        let DocWorkerDocState::Live(bundle) = &self.state else {
            return Ok(());
        };
        let Some(bundle) = bundle.upgrade() else {
            return Ok(());
        };
        if self.pending_fragment_requests.is_empty() {
            return Ok(());
        }
        let requests = std::mem::take(&mut self.pending_fragment_requests);
        process_fragment_requests(Arc::clone(&bundle), requests, Arc::clone(&self.subduction)).await
    }

    async fn take_or_load_transient_doc(&mut self) -> Res<Option<automerge::Automerge>> {
        let out = match std::mem::replace(&mut self.state, DocWorkerDocState::Unloaded) {
            DocWorkerDocState::Transient(doc) => Ok(Some(*doc)),
            DocWorkerDocState::Unloaded => {
                load_doc_snapshot(&self.sedimentrees, &self.storage_for_reads, self.doc_id).await
            }
            DocWorkerDocState::Live(bundle) => {
                self.state = DocWorkerDocState::Live(bundle);
                eyre::bail!("document already live")
            }
        };
        out
    }
}

async fn commit_delta_bookkeep(
    big_sync_store: &SharedPartitionStore,
    change_manager: &Arc<changes::ChangeListenerManager>,
    doc_id: DocumentId,
    heads: Vec<automerge::ChangeHash>,
    patches: Vec<automerge::Patch>,
    origin: BigRepoChangeOrigin,
) -> Res<()> {
    let item_payload = serde_json::json!({
        "heads": am_utils_rs::serialize_commit_heads(&heads),
    });
    let has_change_listener_interest = change_manager.has_change_listener_interest(doc_id, &origin);
    info!(
        %doc_id,
        ?origin,
        heads = heads.len(),
        patches = patches.len(),
        has_change_listener_interest,
        "bookkeeping committed delta"
    );
    big_sync_store
        .set_obj_payload(doc_id, item_payload, vec![], None)
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
    subduction: Arc<BigRepoSubduction<S>>,
) -> Res<()>
where
    S: BigRepoSubductionStorage,
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
                SedimentreeId::new(doc_id.into_bytes()),
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
    conn: iroh::endpoint::Connection,
    signer: &subduction_crypto::signer::memory::MemorySigner,
    nonce_cache: &subduction_core::nonce_cache::NonceCache,
    local_peer_id: subduction_core::peer::id::PeerId,
) -> Res<IrohConnectResult> {
    let (send, recv) = conn
        .accept_bi()
        .await
        .map_err(|err| ferr!("failed accepting subduction bidi stream: {err}"))?;
    let now = subduction_core::timestamp::TimestampSeconds::now();
    let handshake = subduction_iroh::handshake::IrohHandshake::new(send, recv);
    let (authenticated, (listener_task, sender_task)) = subduction_core::handshake::respond(
        handshake,
        move |handshake, peer_id| {
            let (send, recv) = handshake.into_parts();
            let (transport, outbound_rx) =
                subduction_iroh::transport::IrohTransport::new(peer_id, conn);
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
    S: BigRepoSubductionStorage,
    D: Into<DocumentId>,
{
    let doc_id: DocumentId = doc_id.into();
    let sedimentree_id = SedimentreeId::new(doc_id.into_bytes());
    info!(%doc_id, sedimentree_id = %sedimentree_id, "loading doc snapshot");

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
    info!(
        %doc_id,
        loose_commits = loose_commits.len(),
        fragments = fragments.len(),
        "loaded doc snapshot blobs"
    );
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
    info!(%doc_id, fresh, blobs = blobs.len(), "building sedimentree order");
    let blob_by_digest = blobs
        .iter()
        .map(|blob| (Digest::hash(blob), blob.as_slice()))
        .collect::<sedimentree_core::collections::Map<_, _>>();
    let order = tree
        .topsorted_blob_order()
        .map_err(|err| ferr!("failed ordering sedimentree blobs: {err}"))?;
    info!(%doc_id, items = order.len(), "built sedimentree order");
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
            .ok_or_eyre("blob not found for ordered sedimentree item")?;
        buf.extend_from_slice(raw);
    }

    info!(%doc_id, buf_len = buf.len(), "loading doc snapshot into automerge");
    let mut doc = automerge::Automerge::new();
    doc.load_incremental(&buf)
        .map_err(|err| ferr!("failed reconstructing automerge doc from ordered blobs: {err}"))?;
    info!(%doc_id, heads = doc.get_heads().len(), "loaded doc snapshot");

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
    let heads: Vec<CommitId> = doc
        .get_heads()
        .iter()
        .map(|hh| CommitId::new(hh.0))
        .collect();
    let mut known: sedimentree_core::collections::Map<CommitId, FragmentState<OwnedParents>> =
        sedimentree_core::collections::Map::new();
    store
        .build_fragment_store(&heads, &mut known, &CountLeadingZeroBytes)
        .map_err(|err| ferr!("failed building fragment state store: {err}"))?;
    Ok(known)
}
