//! This is just a crappy reimpl of samod to get subduction working

use crate::interlude::*;

use crate::{
    handler::{boot_keyhive, BigRepoComposedHandler, BigRepoKeyhiveProtocol, BigRepoSubduction},
    keyhive_conn::BigRepoKeyhiveConnAdapter,
    keyhive_storage::BigRepoKeyhiveStorage,
    BigKeyhiveHandle, BigRepoChangeOrigin, ConnFinishSignal, DocumentId, PeerId,
};
use subduction_keyhive::{KeyhiveConnection, KeyhivePeerId};

use futures::future::BoxFuture;
use sedimentree_core::crypto::digest::Digest;
use sedimentree_core::sedimentree::{Sedimentree, SedimentreeItem};
use sedimentree_core::{blob::Blob, id::SedimentreeId, loose_commit::id::CommitId};
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::changes;
use subduction_core::subduction::request::FragmentRequested;

const DOC_WORKER_IDLE_TTL: Duration = Duration::from_secs(3);
type SharedPartitionStore = Arc<dyn big_sync::HostPartStore>;

#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum SyncDocError {
    /// Document not found
    NotFound,
    /// Document exists but peer lacks access
    Unauthorized,
    /// Document exists but decryption key not yet available (pending keyhive sync)
    PendingKeys,
    /// TransportError
    TransportError,
    /// IoError
    IoError(eyre::Report),
    /// Unexpected {0}
    Other(#[from] eyre::Report),
}

#[derive(educe::Educe)]
#[educe(Debug)]
pub struct LiveDocBundle {
    pub doc_id: DocumentId,
    #[educe(Debug(ignore))]
    pub doc: tokio::sync::Mutex<automerge::Automerge>,
    #[educe(Debug(ignore))]
    _lease: RuntimeDocLease,
}

impl LiveDocBundle {
    fn new(doc_id: DocumentId, doc: automerge::Automerge, lease: RuntimeDocLease) -> Self {
        Self {
            doc_id,
            doc: tokio::sync::Mutex::new(doc),
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

#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum CreateDocError {
    /// keyhive doc creation failed: {0}
    Keyhive(#[from] eyre::Report),
    /// storage put failed: {0}
    Put(#[from] PutDocError),
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
        resp: oneshot::Sender<Result<(), SyncDocError>>,
    },
    SyncKeyhiveWithPeer {
        peer_id: PeerId,
        timeout: Option<Duration>,
        resp: oneshot::Sender<Res<()>>,
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

#[derive(Clone, educe::Educe)]
#[educe(Debug)]
pub struct BigRepoRuntimeHandle {
    #[educe(Debug(ignore))]
    cmd_tx: mpsc::UnboundedSender<RuntimeCmd>,
    #[educe(Debug(ignore))]
    keyhive: BigKeyhiveHandle,
    #[educe(Debug(ignore))]
    keyhive_protocol: BigRepoKeyhiveProtocol,
}

impl BigRepoRuntimeHandle {
    pub async fn create_doc(
        &self,
        initial_content: automerge::Automerge,
    ) -> Result<Arc<LiveDocBundle>, CreateDocError> {
        use nonempty::NonEmpty;
        let heads = initial_content.get_heads();
        // FIXME: do we use heads from subduction or from automerge???
        let content_heads = NonEmpty::from_vec(heads.iter().map(|h| h.0).collect())
            .ok_or_else(|| eyre::eyre!("automerge doc has no heads"))?;
        let doc_id = self.keyhive.create_doc(content_heads).await?;
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(RuntimeCmd::PutDoc {
                doc_id,
                initial_content: initial_content.into(),
                resp: tx,
            })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await
            .map_err(|_| eyre::eyre!(ERROR_CHANNEL))?
            .map_err(CreateDocError::from)
    }

    pub(crate) async fn put_keyhive_doc(
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
    ) -> Result<(), SyncDocError> {
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

    /// Initiate a keyhive protocol sync with the given peer,
    /// blocking until the sync exchange completes.
    pub async fn sync_keyhive_with_peer(
        &self,
        peer_id: PeerId,
        timeout: Option<std::time::Duration>,
    ) -> Res<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(RuntimeCmd::SyncKeyhiveWithPeer {
                peer_id,
                timeout,
                resp: tx,
            })
            .map_err(|_| ferr!("runtime channel closed"))?;
        rx.await
            .map_err(|_| ferr!("sync keyhive response channel closed"))??;
        let deadline = tokio::time::Instant::now()
            + timeout.unwrap_or_else(|| utils_rs::scale_timeout(Duration::from_secs(5)));
        let keyhive_peer_id = KeyhivePeerId::from_bytes(*peer_id.as_bytes());
        loop {
            if self
                .keyhive_protocol
                .syncpoint_for_peer(&keyhive_peer_id)
                .await
                .is_some()
            {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(ferr!("keyhive sync timed out waiting for syncpoint"));
            }
            tokio::time::sleep(utils_rs::scale_timeout(Duration::from_millis(10))).await;
        }
    }
}

pub struct BigRepoRuntimeStopToken {
    cancel_token: CancellationToken,
    machine_loop_handle: tokio::task::JoinHandle<()>,
    runtime_tasks: Arc<utils_rs::AbortableJoinSet>,
    keyhive_protocol: BigRepoKeyhiveProtocol,
    keyhive_archive_id: subduction_keyhive::storage::StorageHash,
}

impl BigRepoRuntimeStopToken {
    pub async fn stop(self) -> Res<()> {
        self.keyhive_protocol
            .compact(self.keyhive_archive_id)
            .await
            .wrap_err("failed persisting keyhive state during BigRepo shutdown")?;
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

type SubductionSedimentrees = Arc<
    subduction_core::collections::bounded_sharded_map::BoundedShardedMap<
        SedimentreeId,
        sedimentree_core::sedimentree::minimized::MinimizedSedimentree,
        256,
    >,
>;
pub(crate) type BigRepoIrohTransport = subduction_core::transport::message::MessageTransport<
    subduction_iroh::transport::IrohTransport,
>;
pub(crate) type BigRepoPolicy = subduction_keyhive::policy::SubductionKeyhive<
    future_form::Sendable,
    keyhive_crypto::signer::memory::MemorySigner,
    Vec<u8>,
    Vec<u8>,
    keyhive_core::store::ciphertext::memory::MemoryCiphertextStore<Vec<u8>, Vec<u8>>,
    keyhive_core::listener::no_listener::NoListener,
    rand_08::rngs::OsRng,
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
pub async fn spawn_big_repo_runtime<S>(
    signer: subduction_crypto::signer::memory::MemorySigner,
    storage: S,
    policy: Arc<BigRepoPolicy>,
    keyhive: BigKeyhiveHandle,
    keyhive_storage: BigRepoKeyhiveStorage,
    big_sync_store: SharedPartitionStore,
    change_manager: Arc<changes::ChangeListenerManager>,
) -> Res<(BigRepoRuntimeHandle, BigRepoRuntimeStopToken)>
where
    S: BigRepoSubductionStorage,
{
    use subduction_core::subduction::Subduction;
    use subduction_websocket::tokio::{TimeoutTokio, TokioSpawn};

    let connect_signer = signer.clone();
    let local_peer_id =
        subduction_core::peer::id::PeerId::new(*connect_signer.verifying_key().as_bytes());
    let nonce_cache = Arc::new(subduction_core::nonce_cache::NonceCache::new(
        Duration::from_secs(60),
    ));
    let runtime_stop = CancellationToken::new();
    let runtime_tasks = Arc::new(utils_rs::AbortableJoinSet::new());

    let sedimentrees =
        Arc::new(subduction_core::collections::bounded_sharded_map::BoundedShardedMap::new());
    let connections = Arc::new(async_lock::Mutex::new(
        sedimentree_core::collections::Map::new(),
    ));
    let subscriptions = Arc::new(async_lock::Mutex::new(
        sedimentree_core::collections::Map::new(),
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
        sedimentree_core::depth::CountLeadingZeroBytes,
        TokioSpawn,
    );
    sync_handler.set_sync_session_observer(Arc::clone(&sync_session_observer));
    let send_counter = sync_handler.send_counter().clone();
    let sync_handler = Arc::new(sync_handler);

    // Boot keyhive protocol and handler
    let (keyhive_protocol, keyhive_handler) = boot_keyhive(&keyhive, keyhive_storage).await?;
    let composed_handler = Arc::new(BigRepoComposedHandler::new(sync_handler, keyhive_handler));

    let (subduction, listener, manager) = Subduction::new(
        composed_handler,
        None,
        signer,
        Arc::clone(&sedimentrees),
        Arc::clone(&connections),
        Arc::clone(&subscriptions),
        storage_powerbox,
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
        keyhive: keyhive.clone(),
        keyhive_protocol: Arc::clone(&keyhive_protocol),
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
        BigRepoRuntimeHandle {
            cmd_tx,
            keyhive: keyhive.clone(),
            keyhive_protocol: Arc::clone(&keyhive_protocol),
        },
        BigRepoRuntimeStopToken {
            cancel_token: runtime_stop,
            machine_loop_handle,
            runtime_tasks,
            keyhive_protocol,
            keyhive_archive_id: subduction_keyhive::storage::StorageHash::new(
                *local_peer_id.as_bytes(),
            ),
        },
    ))
}

struct BigRepoRuntimeWorker<S>
where
    S: BigRepoSubductionStorage,
{
    keyhive: BigKeyhiveHandle,
    keyhive_protocol: BigRepoKeyhiveProtocol,
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
            RuntimeCmd::SyncKeyhiveWithPeer {
                peer_id,
                timeout: _,
                resp,
            } => {
                let kh_peer_id = KeyhivePeerId::from_bytes(*peer_id.as_bytes());
                tracing::info!("SyncKeyhiveWithPeer: refreshing cache + initiating sync");
                // Refresh cache to pick up external keyhive mutations
                // (e.g. grant_doc_access). TODO: do this periodically.
                if let Err(e) = self.keyhive_protocol.refresh_cache().await {
                    tracing::warn!(error = %e, "refresh_cache failed");
                }
                self.keyhive_protocol
                    .clear_syncpoint_for_peer(&kh_peer_id)
                    .await;
                if let Err(e) = self.keyhive_protocol.sync_keyhive(Some(&kh_peer_id)).await {
                    let _ = resp.send(Err(ferr!("keyhive sync failed: {e}")));
                } else {
                    let _ = resp.send(Ok(()));
                }
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
            pending_sync_jobs: default(),
            state: DocWorkerDocState::Unloaded,
            pending_fragment_requests: BTreeSet::new(),
            subduction: Arc::clone(&self.subduction),
            sedimentrees: Arc::clone(&self.sedimentrees),
            storage_for_reads: self.storage_for_reads.clone(),
            big_sync_store: Arc::clone(&self.big_sync_store),
            change_manager: Arc::clone(&self.change_manager),
            runtime_handle: BigRepoRuntimeHandle {
                cmd_tx: self.cmd_tx.clone(),
                keyhive: self.keyhive.clone(),
                keyhive_protocol: Arc::clone(&self.keyhive_protocol),
            },
            runtime_evt_tx,
            // shutdown: worker_cancel.clone(),
        };
        runtime_tasks
            .spawn(
                async move {
                    let res = worker_cancel
                        .run_until_cancelled(async {
                            loop {
                                let Some(msg) = msg_rx.recv().await else {
                                    break;
                                };
                                worker.handle_msg(msg).await?;
                            }
                            eyre::Ok(())
                        })
                        .await;
                    // FIXME: why two errors instead of stopped with error variant
                    if let Some(Err(err)) = res {
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
            sent_commit_ids = session.sent_commit_ids.len(),
            sent_fragment_ids = session.sent_fragment_ids.len(),
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
        let kh_proto = Arc::clone(&self.keyhive_protocol);
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
                                    .inspect_err(|_| warn!(ERROR_CALLER))
                                    .ok();
                            }
                            Err(err) => {
                                evt_tx
                                    .send(RuntimeEvt::ConnLostIroh {
                                        peer_id,
                                        error: Some(err),
                                        src_task,
                                    })
                                    .inspect_err(|_| warn!(ERROR_CALLER))
                                    .ok();
                            }
                        }
                    }
                };
                runtime_tasks.spawn(fut_wrapped).expect(ERROR_TOKIO);
            }
            let auth = connect.authenticated.clone();
            subduction
                .add_connection(connect.authenticated)
                .await
                .map_err(|err| {
                    cancel_token.cancel();
                    ferr!("failed subduction add_connection: {err}")
                })?;
            let adapter = BigRepoKeyhiveConnAdapter::new(auth);
            kh_proto.add_peer(adapter.peer_id(), adapter).await;
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
                .inspect_err(|_| warn!(ERROR_CALLER))
                .ok();
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
        let kh_proto = Arc::clone(&self.keyhive_protocol);
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
                                    .inspect_err(|_| warn!(ERROR_CALLER))
                                    .ok();
                            }
                            Err(err) => {
                                evt_tx
                                    .send(RuntimeEvt::ConnLostIroh {
                                        peer_id,
                                        error: Some(err),
                                        src_task,
                                    })
                                    .inspect_err(|_| warn!(ERROR_CALLER))
                                    .ok();
                            }
                        }
                    }
                };
                runtime_tasks.spawn(fut_wrapped).expect(ERROR_TOKIO);
            }
            let auth = accepted.authenticated.clone();
            subduction
                .add_connection(accepted.authenticated)
                .await
                .inspect_err(|_| {
                    cancel_token.cancel();
                })
                .wrap_err("failed subduction add_connection")?;
            let adapter = BigRepoKeyhiveConnAdapter::new(auth);
            kh_proto.add_peer(adapter.peer_id(), adapter).await;
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
                .inspect_err(|_| warn!(ERROR_CALLER))
                .ok();
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
        done: oneshot::Sender<Result<(), SyncDocError>>,
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
    pending_sync_jobs: HashMap<PeerId, Vec<oneshot::Sender<Result<(), SyncDocError>>>>,
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
                    .wrap_err(ERROR_CHANNEL)?;
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
                    .wrap_err(ERROR_CHANNEL)?;
                done.send(res).inspect_err(|_| warn!(ERROR_CALLER)).ok();
            }
            DocWorkerMsg::ApplySyncSession { session } => {
                let peer_id = PeerId::new(*session.peer_id.as_bytes());
                self.handle_apply_sync_session(session).await?;
                self.runtime_evt_tx
                    .send(RuntimeEvt::DocWorkerTransientFinished {
                        doc_id: self.doc_id,
                    })
                    .wrap_err(ERROR_CHANNEL)?;
                if let Some(waiters) = self.pending_sync_jobs.remove(&peer_id) {
                    for sender in waiters {
                        sender
                            .send(Ok(()))
                            .inspect_err(|_| warn!(ERROR_CALLER))
                            .ok();
                        self.runtime_evt_tx
                            .send(RuntimeEvt::DocWorkerTransientFinished {
                                doc_id: self.doc_id,
                            })
                            .wrap_err(ERROR_CHANNEL)?;
                    }
                }
            }
            DocWorkerMsg::SyncWithPeer {
                peer_id,
                timeout,
                done,
            } => {
                self.handle_sync_with_peer(peer_id, timeout, done).await?;
            }
            DocWorkerMsg::ReleaseHandleLease => {}
        }
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn handle_put_doc(
        &mut self,
        doc: Box<automerge::Automerge>,
    ) -> Result<Arc<LiveDocBundle>, PutDocError> {
        if !matches!(self.state, DocWorkerDocState::Unloaded) {
            return Err(PutDocError::IdOccpuied { id: self.doc_id });
        }
        if load_doc_snapshot(
            &self.sedimentrees,
            &self.storage_for_reads,
            Some(&self.runtime_handle.keyhive),
            self.doc_id,
        )
        .await?
        .is_some()
        {
            return Err(PutDocError::IdOccpuied { id: self.doc_id });
        }
        let sedimentree_id: SedimentreeId = self.doc_id_subduction;
        let ingested = ingest_automerge(&doc, sedimentree_id);

        // Encrypt blobs via keyhive before storage
        let (sedimentree, blobs) =
            encrypt_ingested_blobs(&ingested, &self.runtime_handle.keyhive, sedimentree_id)
                .await
                .map_err(|e| PutDocError::Other(ferr!("encryption failed: {e}")))?;

        self.subduction
            .store_sedimentree(sedimentree_id, sedimentree, blobs)
            .await
            .map_err(|err| ferr!("failed store_sedimentree: {err}"))?;
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
            RuntimeDocLease {
                runtime: self.runtime_handle.clone(),
                doc_id: self.doc_id,
            },
        ));
        self.big_sync_store
            .set_obj_payload(self.doc_id.into(), item_payload)
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
        let bundle = Arc::new(LiveDocBundle::new(
            self.doc_id,
            doc,
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

    #[tracing::instrument(skip_all)]
    async fn handle_export_doc_save(&mut self) -> Res<Option<Vec<u8>>> {
        match &self.state {
            DocWorkerDocState::Live(bundle) => match bundle.upgrade() {
                Some(bundle) => {
                    let doc = bundle.doc.lock().await;
                    Ok(Some(doc.save()))
                }
                None => {
                    let Some(doc) = load_doc_snapshot(
                        &self.sedimentrees,
                        &self.storage_for_reads,
                        Some(&self.runtime_handle.keyhive),
                        self.doc_id,
                    )
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
                let Some(doc) = load_doc_snapshot(
                    &self.sedimentrees,
                    &self.storage_for_reads,
                    Some(&self.runtime_handle.keyhive),
                    self.doc_id,
                )
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
        // Track content_ref -> SymmetricKey within this batch for the
        // application-level predecessor key chain during encrypt.
        let mut batch_keys: std::collections::HashMap<
            CommitId,
            keyhive_crypto::symmetric_key::SymmetricKey,
        > = std::collections::HashMap::new();
        for (head, parents, blob) in commits {
            let (encrypted_blob, app_key) = encrypt_loose_commit(
                &self.runtime_handle.keyhive,
                sedimentree_id,
                head,
                &parents,
                &blob,
                &batch_keys,
            )
            .await
            .map_err(|e| PutDocError::Other(ferr!("encryption failed: {e}")))?;
            batch_keys.insert(head, app_key);
            let maybe_request = self
                .subduction
                .store_commit(sedimentree_id, head, parents, encrypted_blob)
                .await
                .map_err(|err| ferr!("failed store_commit: {err}"))?;
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
        fields(
            peer_id = %session.peer_id,
            kind = ?session.kind,
            received_commit_ids = session.received_commit_ids.len(),
            received_fragment_ids = session.received_fragment_ids.len(),
            is_unloaded = matches!(self.state, DocWorkerDocState::Unloaded),
            is_transient = matches!(self.state, DocWorkerDocState::Transient(_)),
            is_live = matches!(self.state, DocWorkerDocState::Live(_)),
        )
    )]
    async fn handle_apply_sync_session(
        &mut self,
        session: subduction_core::sync_session::SyncSession,
    ) -> Res<()> {
        // FIXME: enabling this quick exit breaks things
        // if session.received_commit_ids.is_empty() && session.received_fragment_ids.is_empty() {
        //     return Ok(());
        // }
        use keyhive_crypto::symmetric_key::SymmetricKey;
        // Pass 1: CGKA decrypt, collect keys
        let mut decrypted: Vec<Option<Vec<u8>>> = Vec::new();
        // Per-position keys (index → key)
        let mut pos_keys: std::collections::HashMap<usize, SymmetricKey> =
            std::collections::HashMap::new();
        // DAG ancestor pool: content_ref → key from Envelope.ancestors
        let mut ancestor_pool: std::collections::HashMap<Vec<u8>, SymmetricKey> =
            std::collections::HashMap::new();
        let mut raws: Vec<Vec<u8>> = Vec::new();

        for commit_id in &session.received_commit_ids {
            let verified = self
                .storage_for_reads
                .load_loose_commit(session.sedimentree_id, *commit_id)
                .await?
                .ok_or_eyre("synced loose commit missing")?;
            raws.push(verified.blob().clone().into_contents());
        }
        for fragment_id in &session.received_fragment_ids {
            let verified = self
                .storage_for_reads
                .load_fragment(session.sedimentree_id, *fragment_id)
                .await?
                .ok_or_eyre("synced fragment missing")?;
            raws.push(verified.blob().clone().into_contents());
        }

        // Pass 1: CGKA decrypt, collect keys and ancestors
        for raw in &raws {
            match decrypt_snapshot_blob_keyed(
                raw,
                &self.runtime_handle.keyhive,
                self.doc_id_subduction,
                None,
            )
            .await
            {
                Ok((plaintext, key, ancestors)) => {
                    let idx = decrypted.len();
                    pos_keys.insert(idx, key);
                    ancestor_pool.extend(ancestors);
                    decrypted.push(Some(plaintext));
                }
                Err(e) => {
                    tracing::debug!(?e, "sync blob CGKA decrypt failed, will retry with chain");
                    decrypted.push(None);
                }
            }
        }

        // Pass 2: retry with chain keys (position + ancestor pool)
        let mut made_progress = !pos_keys.is_empty() || !ancestor_pool.is_empty();
        while made_progress {
            made_progress = false;
            for (i, slot) in decrypted.iter_mut().enumerate() {
                if slot.is_some() {
                    continue;
                }
                for chain_key in pos_keys.values().chain(ancestor_pool.values()) {
                    match decrypt_snapshot_blob_keyed(
                        &raws[i],
                        &self.runtime_handle.keyhive,
                        self.doc_id_subduction,
                        Some(chain_key),
                    )
                    .await
                    {
                        Ok((plaintext, key, ancestors)) => {
                            pos_keys.insert(i, key);
                            ancestor_pool.extend(ancestors);
                            *slot = Some(plaintext);
                            made_progress = true;
                            break;
                        }
                        Err(_) => continue,
                    }
                }
            }
        }

        // Filter out synthetic entry blobs before loading into automerge
        let blobs: Vec<Vec<u8>> = decrypted
            .into_iter()
            .enumerate()
            .map(|(i, opt)| opt.ok_or_else(|| ferr!("decrypt sync session blob {i} failed")))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|b| !b.starts_with(SENTINEL_PLAINTEXT_MAGIC))
            .collect();
        let maybe_delta = match std::mem::replace(&mut self.state, DocWorkerDocState::Unloaded) {
            DocWorkerDocState::Unloaded => {
                // since the doc in storage will have the latest blobs,
                // we must load the before_heads from the partition payloads
                let mut doc = load_doc_snapshot(
                    &self.sedimentrees,
                    &self.storage_for_reads,
                    Some(&self.runtime_handle.keyhive),
                    self.doc_id,
                )
                .await?
                .unwrap_or_else(automerge::Automerge::new);
                let cached_before_heads =
                    super::partition_doc_heads_payload(&self.big_sync_store, self.doc_id).await?;
                let before_heads = cached_before_heads
                    .clone()
                    .unwrap_or_else(|| doc.get_heads().into());
                let had_cached_before_heads = cached_before_heads.is_some();
                let loaded_heads = doc.get_heads();
                let out = if before_heads[..] == loaded_heads[..] {
                    for blob in blobs {
                        doc.load_incremental(&blob).map_err(|err| {
                            eyre::eyre!("failed applying sync session blob: {err}")
                        })?;
                    }
                    let after_heads = doc.get_heads();
                    if before_heads[..] == after_heads[..] {
                        if had_cached_before_heads {
                            None
                        } else {
                            Some((after_heads, Vec::new()))
                        }
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
                    let mut doc = load_doc_snapshot(
                        &self.sedimentrees,
                        &self.storage_for_reads,
                        Some(&self.runtime_handle.keyhive),
                        self.doc_id,
                    )
                    .await?
                    .unwrap_or_else(automerge::Automerge::new);
                    let loaded_heads = doc.get_heads();
                    let cached_before_heads =
                        super::partition_doc_heads_payload(&self.big_sync_store, self.doc_id)
                            .await?;
                    let before_heads = cached_before_heads
                        .clone()
                        .unwrap_or_else(|| loaded_heads.clone().into());
                    let had_cached_before_heads = cached_before_heads.is_some();
                    let out = if before_heads[..] == loaded_heads[..] {
                        for blob in blobs {
                            doc.load_incremental(&blob).map_err(|err| {
                                eyre::eyre!("failed applying sync session blob: {err}")
                            })?;
                        }
                        let after_heads = doc.get_heads();
                        if before_heads[..] == after_heads[..] {
                            if had_cached_before_heads {
                                None
                            } else {
                                Some((after_heads, Vec::new()))
                            }
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
        &mut self,
        peer_id: PeerId,
        timeout: Option<Duration>,
        sender: oneshot::Sender<Result<(), SyncDocError>>,
    ) -> Res<()> {
        let sedimentree_id = self.doc_id_subduction;
        let remote_peer_id = subduction_core::peer::id::PeerId::new(peer_id.into_bytes());
        let timeout = timeout
            .map(|duration| {
                subduction_core::timeout::call::CallTimeout::TimeoutMillis(
                    duration
                        .as_millis()
                        .try_into()
                        .expect("timeout fits in u64"),
                )
            })
            .unwrap_or(subduction_core::timeout::call::CallTimeout::Default);
        let result = self
            .subduction
            .sync_with_peer(&remote_peer_id, sedimentree_id, false, timeout)
            .await;
        let res = match result {
            Ok((had_success, _stats, conn_errs)) => {
                if had_success {
                    // NOTE: we pre-optimize when stats says we recieved
                    // 0 incoming but this might be a flakeout
                    if _stats.commits_received > 0 || _stats.fragments_received > 0 {
                        self.pending_sync_jobs
                            .entry(peer_id)
                            .or_default()
                            .push(sender);
                        return Ok(());
                    }
                    Ok(())
                } else if conn_errs.is_empty() {
                    Err(SyncDocError::NotFound)
                } else {
                    Err(SyncDocError::TransportError)
                }
            }
            Err(err) => Err(SyncDocError::IoError(ferr!("{err}"))),
        };
        let evt = RuntimeEvt::DocWorkerTransientFinished {
            doc_id: self.doc_id,
        };
        self.runtime_evt_tx.send(evt).wrap_err(ERROR_CHANNEL)?;
        sender.send(res).inspect_err(|_| warn!(ERROR_CALLER)).ok();
        Ok(())
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

    #[tracing::instrument(skip_all)]
    async fn take_or_load_transient_doc(&mut self) -> Res<Option<automerge::Automerge>> {
        let out = match std::mem::replace(&mut self.state, DocWorkerDocState::Unloaded) {
            DocWorkerDocState::Transient(doc) => Ok(Some(*doc)),
            DocWorkerDocState::Unloaded => {
                load_doc_snapshot(
                    &self.sedimentrees,
                    &self.storage_for_reads,
                    Some(&self.runtime_handle.keyhive),
                    self.doc_id,
                )
                .await
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
    big_sync_store
        .set_obj_payload(doc_id.into(), item_payload)
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
    let sed_id = SedimentreeId::new(doc_id.into_bytes());
    let mut work = Vec::with_capacity(requests.len());
    {
        let doc = bundle.doc.lock().await;
        for req in requests {
            let fragment = doc
                .get_fragment(automerge::ChangeHash(*req.head().as_bytes()))
                .expect("error resolving requested fragment from automerge::Automerge");
            debug_assert_eq!(req.depth().0, fragment.level as u32);
            let raw = doc
                .bundle(fragment.members.iter().cloned())
                .wrap_err("unable to resolve bundle for fragment")?
                .bytes()
                .to_vec();
            work.push((
                req.head(),
                fragment
                    .boundary
                    .iter()
                    .map(|head| CommitId::new(head.0))
                    .collect::<BTreeSet<_>>(),
                fragment
                    .checkpoints
                    .iter()
                    .map(|head| CommitId::new(head.0))
                    .collect::<Vec<_>>(),
                Blob::new(raw),
            ));
        }
    }
    for (commit_id, boundary, checkpoints, blob) in work {
        subduction
            .add_fragment(sed_id, commit_id, boundary, &checkpoints, blob)
            .await
            .map_err(|err| ferr!("failed add_fragment: {err}"))?;
    }
    Ok(())
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
    keyhive: Option<&BigKeyhiveHandle>,
    doc_id: D,
) -> Res<Option<automerge::Automerge>>
where
    S: BigRepoSubductionStorage,
    D: Into<DocumentId>,
{
    let doc_id: DocumentId = doc_id.into();
    let sedimentree_id = SedimentreeId::new(doc_id.into_bytes());

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
            let tree = sedimentree_core::sedimentree::minimized::MinimizedSedimentree::new(
                Sedimentree::new(
                    fragments
                        .iter()
                        .map(|verified| verified.payload().clone())
                        .collect(),
                    loose_commits
                        .iter()
                        .map(|verified| verified.payload().clone())
                        .collect(),
                ),
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

    // Collect raw bytes and decrypt in reverse causal order (newest first).
    // The latest blob is most likely to have been encrypted with a PCS key
    // that includes the current members. From its key we chain backward
    // through the predecessor keys embedded in each blob.
    use keyhive_crypto::symmetric_key::SymmetricKey;
    let raws: Vec<&[u8]> = order
        .iter()
        .map(|item| {
            let wanted = match item {
                SedimentreeItem::Fragment(ii) => OrderedBlobIdentity {
                    kind: OrderedBlobKind::Fragment,
                    head: fragments[*ii].head(),
                    blob_digest: fragments[*ii].summary().blob_meta().digest(),
                },
                SedimentreeItem::LooseCommit(ii) => OrderedBlobIdentity {
                    kind: OrderedBlobKind::LooseCommit,
                    head: loose[*ii].head(),
                    blob_digest: loose[*ii].blob_meta().digest(),
                },
            };
            *blob_by_digest
                .get(&wanted.blob_digest)
                .expect("blob must exist in digest map")
        })
        .collect();

    let mut plaintexts: Vec<Vec<u8>> = vec![Vec::new(); raws.len()];
    // Per-position keys from successful decrypts (index → key)
    let mut pos_keys: HashMap<usize, SymmetricKey> = HashMap::new();
    // DAG ancestor pool: content_ref → key, populated from Envelope.ancestors
    let mut ancestor_pool: HashMap<Vec<u8>, SymmetricKey> = HashMap::new();

    if let Some(kh) = keyhive {
        // Process newest-to-oldest. The newest blob should be CGKA-decryptable
        // (its PCS key includes members added before it was encrypted).
        // Its Envelope.ancestors then provide keys for older blobs.
        for (i, raw) in raws.iter().enumerate().rev() {
            // Try CGKA first (no chain key)
            if let Ok((plaintext, key, ancestors)) =
                decrypt_snapshot_blob_keyed(raw, kh, sedimentree_id, None).await
            {
                plaintexts[i] = plaintext;
                pos_keys.insert(i, key);
                ancestor_pool.extend(ancestors);
                continue;
            }
            // Try chain: any known key (position or ancestor) might decrypt
            let mut decrypted = false;
            for chain_key in pos_keys.values().chain(ancestor_pool.values()) {
                if let Ok((plaintext, key, ancestors)) =
                    decrypt_snapshot_blob_keyed(raw, kh, sedimentree_id, Some(chain_key)).await
                {
                    plaintexts[i] = plaintext;
                    pos_keys.insert(i, key);
                    ancestor_pool.extend(ancestors);
                    decrypted = true;
                    break;
                }
            }
            if !decrypted {
                return Err(ferr!(
                    "failed to decrypt blob {i}: CGKA and chain both failed"
                ));
            }
        }
    } else {
        for (i, raw) in raws.iter().enumerate() {
            plaintexts[i] = raw.to_vec();
        }
    }

    // Build automerge doc from non-sentinel plaintexts.
    // Synthetic entry blobs contribute ancestors but not automerge content.
    let mut buf = Vec::new();
    for plaintext in &plaintexts {
        if plaintext.starts_with(SENTINEL_PLAINTEXT_MAGIC) {
            continue;
        }
        buf.extend_from_slice(plaintext);
    }

    let mut doc = automerge::Automerge::new();
    doc.load_incremental(&buf)
        .map_err(|err| ferr!("failed reconstructing automerge doc from ordered blobs: {err}"))?;

    if fresh {
        sedimentrees
            .get_or_insert_with(sedimentree_id, || tree)
            .await;
    }
    Ok(Some(doc))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OrderedBlobKind {
    Fragment,
    LooseCommit,
}

impl OrderedBlobKind {
    const fn label(self) -> &'static str {
        match self {
            Self::Fragment => "fragment",
            Self::LooseCommit => "loose_commit",
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct OrderedBlobIdentity {
    kind: OrderedBlobKind,
    head: CommitId,
    blob_digest: Digest<Blob>,
}

type LoadedOrderedBlob = OrderedBlobIdentity;

fn missing_ordered_blob_context(
    wanted: OrderedBlobIdentity,
    loaded_items: &[LoadedOrderedBlob],
) -> String {
    let same_kind_head = loaded_items
        .iter()
        .filter(|item| item.kind == wanted.kind && item.head == wanted.head)
        .map(|item| blob_digest_hex(item.blob_digest))
        .collect::<Vec<_>>();
    let loaded_kind_count = loaded_items
        .iter()
        .filter(|item| item.kind == wanted.kind)
        .count();

    if same_kind_head.is_empty() {
        format!(
            "wanted_kind={} wanted_head={:?} wanted_blob_digest={} loaded_kind_count={} same_head_loaded_blob_digests=[]",
            wanted.kind.label(),
            wanted.head,
            blob_digest_hex(wanted.blob_digest),
            loaded_kind_count,
        )
    } else {
        format!(
            "wanted_kind={} wanted_head={:?} wanted_blob_digest={} loaded_kind_count={} same_head_loaded_blob_digests={same_kind_head:?}",
            wanted.kind.label(),
            wanted.head,
            blob_digest_hex(wanted.blob_digest),
            loaded_kind_count,
        )
    }
}

fn blob_digest_hex(digest: Digest<Blob>) -> String {
    format!("{digest:?}")
}

/// Metadata for a single fragment from ingest, used to rebuild after encryption.
struct FragmentEntry {
    head: CommitId,
    boundary: BTreeSet<CommitId>,
    checkpoints: Vec<CommitId>,
}

/// Metadata for a single loose commit from ingest, used to rebuild after encryption.
struct LooseEntry {
    head: CommitId,
    parents: BTreeSet<CommitId>,
}

/// Result of ingesting an Automerge document.
struct IngestResult {
    sedimentree: Sedimentree,
    blobs: Vec<Blob>,
    fragment_entries: Vec<FragmentEntry>,
    loose_entries: Vec<LooseEntry>,
    _change_count: usize,
    _covered_count: usize,
    _loose_count: usize,
    _fragment_count: usize,
}

/// Ingest an Automerge document into a [`Sedimentree`].
///
/// Thin adapter over [`Automerge::fragments`] and
/// [`Automerge::bundle_fragments`]: maps each level-1+ `automerge::Fragment`
/// into a sedimentree [`Fragment`] and each level-0 fragment into a
/// [`LooseCommit`].
fn ingest_automerge(doc: &automerge::Automerge, sedimentree_id: SedimentreeId) -> IngestResult {
    use sedimentree_core::{blob::BlobMeta, fragment::Fragment, loose_commit::LooseCommit};

    let cached = doc.fragments(1..);
    let loose = doc.fragments(0..=0);
    let cached_bytes = doc.bundle_fragments(cached.iter().cloned());
    let loose_bytes = doc.bundle_fragments(loose.iter().cloned());

    let mut fragments = Vec::with_capacity(cached.len());
    let mut blobs = Vec::with_capacity(cached.len() + loose.len());
    let mut fragment_entries = Vec::with_capacity(cached.len());
    let mut loose_entries = Vec::with_capacity(loose.len());
    let mut covered: sedimentree_core::collections::Set<CommitId> = default();
    for (fragment, raw) in cached.iter().zip(cached_bytes) {
        for member in &fragment.members {
            covered.insert(CommitId::new(member.0));
        }
        let head = CommitId::new(fragment.head.0);
        let boundary: BTreeSet<CommitId> = fragment
            .boundary
            .iter()
            .map(|head| CommitId::new(head.0))
            .collect();
        let checkpoints: Vec<CommitId> = fragment
            .checkpoints
            .iter()
            .map(|head| CommitId::new(head.0))
            .collect();
        let blob = Blob::new(raw);
        let meta = BlobMeta::new(&blob);
        fragments.push(Fragment::new(
            sedimentree_id,
            head,
            boundary.clone(),
            &checkpoints,
            meta,
        ));
        blobs.push(blob);
        fragment_entries.push(FragmentEntry {
            head,
            boundary,
            checkpoints,
        });
    }

    let mut loose_commits = Vec::with_capacity(loose.len());
    for (fragment, raw) in loose.iter().zip(loose_bytes) {
        let head = CommitId::new(fragment.head.0);
        let parents: BTreeSet<CommitId> = fragment
            .boundary
            .iter()
            .map(|pp| CommitId::new(pp.0))
            .collect();
        let blob = Blob::new(raw);
        let meta = BlobMeta::new(&blob);
        loose_commits.push(LooseCommit::new(
            sedimentree_id,
            head,
            parents.clone(),
            meta,
        ));
        blobs.push(blob);
        loose_entries.push(LooseEntry { head, parents });
    }

    let fragment_count = fragments.len();
    let loose_count = loose_commits.len();
    let covered_count = covered.len();

    IngestResult {
        sedimentree: Sedimentree::new(fragments, loose_commits),
        blobs,
        fragment_entries,
        loose_entries,
         _change_count: doc.get_changes_meta(&[]).len(),
        _covered_count: covered_count,
        _loose_count: loose_count,
        _fragment_count: fragment_count,
    }
}

/// Sentinel plaintext prefix identifying a synthetic entry blob.
///
/// Synthetic blobs are minted at grant time to bridge CGKA epochs for new
/// members. Their plaintext is just this magic followed by nothing useful.
/// During `load_doc_snapshot` they contribute their ancestor keys to the
/// chain pool but are NOT loaded into automerge.
const SENTINEL_PLAINTEXT_MAGIC: &[u8] = b"KEYHIVE_SYNTHETIC_ENTRY";

/// DRISL envelope for an encrypted blob.
///
/// Flattens `EncryptedContent` fields inline. The predecessor key chain
/// lives inside the plaintext via keyhive_core's `Envelope<C, T>` wrapper
/// (bincode-serialized before encryption).
///
/// Format: `[0x02][DRISL(EncryptedBlobEnvelope)]`
#[derive(serde::Serialize, serde::Deserialize)]
struct EncryptedBlobEnvelope {
    #[serde(flatten)]
    encrypted: beekem::encrypted::EncryptedContent<Vec<u8>, Vec<u8>>,
}

/// Encrypt ingested blobs via the keyhive, rebuilding the sedimentree with
/// updated BlobMeta matching the encrypted bytes.
///
/// If the sedimentree ID does not correspond to a valid keyhive document
/// (e.g. random IDs used in tests), the ingested data is returned unchanged.
/// Encrypt ingested blobs via the keyhive, serializing each encrypted blob as
/// DRISL (deterministic CBOR) prefixed with a 1-byte discriminator:
/// - `0x00` = plaintext
/// - `0x02` = encrypted: [0x02][DRISL(EncryptedBlobEnvelope)]
///
/// TODO: Upstream (keyhive/subduction_keyhive) hasn't specified the canonical
/// EncryptedContent storage format yet. Revisit when upstream publishes a spec.
async fn encrypt_ingested_blobs(
    ingested: &IngestResult,
    keyhive_handle: &BigKeyhiveHandle,
    sedimentree_id: SedimentreeId,
) -> Res<(Sedimentree, Vec<Blob>)> {
    use keyhive_core::crypto::envelope::Envelope;
    use keyhive_crypto::symmetric_key::SymmetricKey;
    use sedimentree_core::{blob::BlobMeta, fragment::Fragment, loose_commit::LooseCommit};

    let keyhive = keyhive_handle.clone_keyhive().await;
    let vk = ed25519_dalek::VerifyingKey::from_bytes(sedimentree_id.as_bytes())
        .map_err(|_| ferr!("not a valid Keyhive DocumentId"))?;
    let kh_doc_id = keyhive_core::principal::document::id::DocumentId::from(
        keyhive_core::principal::identifier::Identifier::from(vk),
    );
    let kh_doc = keyhive.get_document(kh_doc_id).await.ok_or_else(|| {
        ferr!("keyhive doc not found in local keyhive; only the doc owner can call put_doc")
    })?;

    let mut encrypted_blobs: Vec<Blob> = Vec::with_capacity(ingested.blobs.len());
    let mut new_fragments: Vec<Fragment> = Vec::with_capacity(ingested.fragment_entries.len());
    let mut new_loose_commits: Vec<LooseCommit> = Vec::with_capacity(ingested.loose_entries.len());

    // Track content_ref -> SymmetricKey for building ancestor maps
    let mut key_index: std::collections::HashMap<Vec<u8>, SymmetricKey> =
        std::collections::HashMap::new();

    // Encrypt fragment blobs
    for (entry, blob) in ingested
        .fragment_entries
        .iter()
        .zip(ingested.blobs.iter().take(ingested.fragment_entries.len()))
    {
        let content_ref: Vec<u8> = entry.head.as_bytes().to_vec();
        let pred_refs: Vec<Vec<u8>> = entry
            .boundary
            .iter()
            .map(|c| c.as_bytes().to_vec())
            .collect();
        // Build ancestors map from all known predecessor keys
        let ancestors: std::collections::HashMap<Vec<u8>, SymmetricKey> = pred_refs
            .iter()
            .filter_map(|pred| key_index.get(pred).map(|k| (pred.clone(), *k)))
            .collect();
        let envelope = Envelope {
            plaintext: blob.as_slice().to_vec(),
            ancestors,
        };
        let envelope_bytes =
            bincode::serialize(&envelope).map_err(|e| ferr!("bincode encode envelope: {e}"))?;

        let (encrypted, app_key) = keyhive
            .try_encrypt_content_keyed(kh_doc.clone(), &content_ref, &pred_refs, &envelope_bytes)
            .await
            .map_err(|e| ferr!("encrypt fragment failed: {e}"))?;

        let env = EncryptedBlobEnvelope {
            encrypted: encrypted.encrypted_content().clone(),
        };
        let drisl_env = atproto_dasl::drisl::to_vec(&env)
            .map_err(|e| ferr!("DRISL encode encrypted blob envelope: {e}"))?;
        let mut encrypted_bytes = vec![0x02u8];
        encrypted_bytes.extend(drisl_env);

        key_index.insert(content_ref.clone(), app_key);

        let encrypted_blob = Blob::new(encrypted_bytes);
        let meta = BlobMeta::new(&encrypted_blob);
        new_fragments.push(Fragment::new(
            sedimentree_id,
            entry.head,
            entry.boundary.clone(),
            &entry.checkpoints,
            meta,
        ));
        encrypted_blobs.push(encrypted_blob);
    }

    // Encrypt loose commit blobs
    for (entry, blob) in ingested
        .loose_entries
        .iter()
        .zip(ingested.blobs.iter().skip(ingested.fragment_entries.len()))
    {
        let content_ref: Vec<u8> = entry.head.as_bytes().to_vec();
        let pred_refs: Vec<Vec<u8>> = entry
            .parents
            .iter()
            .map(|c| c.as_bytes().to_vec())
            .collect();

        let ancestors: std::collections::HashMap<Vec<u8>, SymmetricKey> = pred_refs
            .iter()
            .filter_map(|pred| key_index.get(pred).map(|k| (pred.clone(), *k)))
            .collect();
        let envelope = Envelope {
            plaintext: blob.as_slice().to_vec(),
            ancestors,
        };
        let envelope_bytes =
            bincode::serialize(&envelope).map_err(|e| ferr!("bincode encode envelope: {e}"))?;

        let (encrypted, app_key) = keyhive
            .try_encrypt_content_keyed(kh_doc.clone(), &content_ref, &pred_refs, &envelope_bytes)
            .await
            .map_err(|e| ferr!("encrypt loose commit failed: {e}"))?;

        let env = EncryptedBlobEnvelope {
            encrypted: encrypted.encrypted_content().clone(),
        };
        let drisl_env = atproto_dasl::drisl::to_vec(&env)
            .map_err(|e| ferr!("DRISL encode encrypted blob envelope: {e}"))?;
        let mut encrypted_bytes = vec![0x02u8];
        encrypted_bytes.extend(drisl_env);

        key_index.insert(content_ref.clone(), app_key);

        let encrypted_blob = Blob::new(encrypted_bytes);
        let meta = BlobMeta::new(&encrypted_blob);
        new_loose_commits.push(LooseCommit::new(
            sedimentree_id,
            entry.head,
            entry.parents.clone(),
            meta,
        ));
        encrypted_blobs.push(encrypted_blob);
    }

    Ok((
        Sedimentree::new(new_fragments, new_loose_commits),
        encrypted_blobs,
    ))
}

/// Encrypt a single loose commit blob via keyhive.
///
/// The caller provides `batch_keys` (CommitId → SymmetricKey) so we can build
/// the `Envelope.ancestors` map from all known parent keys.
async fn encrypt_loose_commit(
    keyhive_handle: &BigKeyhiveHandle,
    sedimentree_id: SedimentreeId,
    head: CommitId,
    parents: &BTreeSet<CommitId>,
    blob: &[u8],
    batch_keys: &std::collections::HashMap<CommitId, keyhive_crypto::symmetric_key::SymmetricKey>,
) -> Res<(Blob, keyhive_crypto::symmetric_key::SymmetricKey)> {
    use keyhive_core::crypto::envelope::Envelope;
    use keyhive_crypto::symmetric_key::SymmetricKey;
    let keyhive = keyhive_handle.clone_keyhive().await;
    let vk = ed25519_dalek::VerifyingKey::from_bytes(sedimentree_id.as_bytes())
        .map_err(|_| ferr!("not a valid Keyhive DocumentId"))?;
    let kh_doc_id = keyhive_core::principal::document::id::DocumentId::from(
        keyhive_core::principal::identifier::Identifier::from(vk),
    );
    let kh_doc = keyhive
        .get_document(kh_doc_id)
        .await
        .ok_or_else(|| ferr!("keyhive doc not found for commit encryption"))?;
    let content_ref: Vec<u8> = head.as_bytes().to_vec();
    let pred_refs: Vec<Vec<u8>> = parents.iter().map(|c| c.as_bytes().to_vec()).collect();

    let ancestors: std::collections::HashMap<Vec<u8>, SymmetricKey> = {
        // Try batch_keys first, then fall back to known_decryption_keys
        // (populated by prior encrypts on the same Document)
        let doc_keys = kh_doc.lock().await.known_decryption_keys().clone();
        parents
            .iter()
            .filter_map(|p| {
                let pref: Vec<u8> = p.as_bytes().to_vec();
                batch_keys
                    .get(p)
                    .map(|k| (pref.clone(), *k))
                    .or_else(|| doc_keys.get(&pref).map(|k| (pref, *k)))
            })
            .collect()
    };
    let envelope = Envelope {
        plaintext: blob.to_vec(),
        ancestors,
    };
    let envelope_bytes =
        bincode::serialize(&envelope).map_err(|e| ferr!("bincode encode envelope: {e}"))?;

    let (encrypted, app_key) = keyhive
        .try_encrypt_content_keyed(kh_doc.clone(), &content_ref, &pred_refs, &envelope_bytes)
        .await
        .map_err(|e| ferr!("encrypt commit failed: {e}"))?;

    let env = EncryptedBlobEnvelope {
        encrypted: encrypted.encrypted_content().clone(),
    };
    let drisl_env = atproto_dasl::drisl::to_vec(&env)
        .map_err(|e| ferr!("DRISL encode encrypted blob envelope: {e}"))?;
    let mut encrypted_bytes = vec![0x02u8];
    encrypted_bytes.extend(drisl_env);

    Ok((Blob::new(encrypted_bytes), app_key))
}

/// Decrypt a single snapshot blob stored as encrypted content.
///
/// Format: `[0x02][DRISL(EncryptedBlobEnvelope)]`
/// Plaintext is bincode-encoded keyhive `Envelope<Vec<u8>, Vec<u8>>`.
async fn decrypt_snapshot_blob(
    raw_bytes: &[u8],
    keyhive_handle: &BigKeyhiveHandle,
    sedimentree_id: SedimentreeId,
) -> Res<Vec<u8>> {
    decrypt_snapshot_blob_keyed(raw_bytes, keyhive_handle, sedimentree_id, None)
        .await
        .map(|(plaintext, _key, _ancestors)| plaintext)
}

/// Decrypt and also return the application secret key and ancestors map for
/// DAG-based causal decryption.
///
/// If `chain_key` is provided and CGKA decryption fails, `chain_key` is used
/// to directly decrypt the ciphertext (bypassing CGKA).
async fn decrypt_snapshot_blob_keyed(
    raw_bytes: &[u8],
    keyhive_handle: &BigKeyhiveHandle,
    sedimentree_id: SedimentreeId,
    chain_key: Option<&keyhive_crypto::symmetric_key::SymmetricKey>,
) -> Res<(
    Vec<u8>,
    keyhive_crypto::symmetric_key::SymmetricKey,
    std::collections::HashMap<Vec<u8>, keyhive_crypto::symmetric_key::SymmetricKey>,
)> {
    use keyhive_core::crypto::envelope::Envelope;
    use keyhive_crypto::symmetric_key::SymmetricKey;

    // Parse: [0x02][DRISL(EncryptedBlobEnvelope)]
    if raw_bytes.first() != Some(&0x02) {
        return Err(ferr!("blob is not encrypted (missing 0x02 discriminator)"));
    }
    let rest = &raw_bytes[1..];

    let envelope: EncryptedBlobEnvelope = atproto_dasl::drisl::from_slice(rest)
        .map_err(|e| ferr!("DRISL decode encrypted blob envelope: {e}"))?;

    let encrypted = envelope.encrypted;

    let keyhive = keyhive_handle.clone_keyhive().await;
    let vk = ed25519_dalek::VerifyingKey::from_bytes(sedimentree_id.as_bytes())
        .map_err(|_| ferr!("not a valid Keyhive DocumentId"))?;
    let kh_doc_id = keyhive_core::principal::document::id::DocumentId::from(
        keyhive_core::principal::identifier::Identifier::from(vk),
    );
    let kh_doc = keyhive
        .get_document(kh_doc_id)
        .await
        .ok_or_else(|| ferr!("keyhive doc not found for decryption"))?;

    // Helper: after AEAD decrypt, deserialize the Envelope and return
    // (plaintext, key_used, ancestors).
    let unwrap_envelope = |ciphertext: &[u8], key: SymmetricKey| -> Res<_> {
        let payload: Envelope<Vec<u8>, Vec<u8>> =
            bincode::deserialize(ciphertext).map_err(|e| ferr!("bincode decode envelope: {e}"))?;
        Ok((payload.plaintext, key, payload.ancestors))
    };

    // Try CGKA first
    match keyhive
        .try_decrypt_content(kh_doc.clone(), &encrypted)
        .await
    {
        Ok(plaintext) => {
            let (_pt, key) = keyhive
                .try_decrypt_content_keyed(kh_doc, &encrypted)
                .await
                .map_err(|e| ferr!("decrypt keyed failed after successful decrypt: {e}"))?;
            return unwrap_envelope(&plaintext, key);
        }
        Err(keyhive_core::principal::document::DecryptError::KeyNotFound) => {
            // CGKA forward-secrecy prevents direct derivation.
            // Try chain key: direct AEAD decrypt bypassing CGKA.
            if let Some(chain_key) = chain_key {
                let plaintext = encrypted
                    .try_decrypt(*chain_key)
                    .map_err(|e| ferr!("chain decrypt failed: {e}"))?;
                return unwrap_envelope(&plaintext, *chain_key);
            }
        }
        Err(e) => {
            return Err(ferr!("decryption failed: {e}"));
        }
    }

    Err(ferr!(
        "decryption failed: KeyNotFound (no chain key available)"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sedimentree_core::blob::Blob;

    #[test]
    fn missing_ordered_blob_context_reports_same_head_digest_mismatch() {
        let head = CommitId::new([7; 32]);
        let wanted_blob = Blob::new(b"wanted-fragment-bytes".to_vec());
        let loaded_blob = Blob::new(b"loaded-fragment-bytes".to_vec());
        let wanted = OrderedBlobIdentity {
            kind: OrderedBlobKind::Fragment,
            head,
            blob_digest: Digest::hash(&wanted_blob),
        };
        let loaded_items = [LoadedOrderedBlob {
            kind: OrderedBlobKind::Fragment,
            head,
            blob_digest: Digest::hash(&loaded_blob),
        }];

        let context = missing_ordered_blob_context(wanted, &loaded_items);

        assert!(context.contains("wanted_kind=fragment"));
        assert!(context.contains("loaded_kind_count=1"));
        assert!(context.contains("same_head_loaded_blob_digests=["));
        assert!(context.contains(&blob_digest_hex(Digest::hash(&wanted_blob))));
        assert!(context.contains(&blob_digest_hex(Digest::hash(&loaded_blob))));
    }
}
