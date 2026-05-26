//! FIXME: don't expose unregister_remote_repo_peer but handle the lifecycle
//! internally by sharing the Arc<Mutex> of the registry to the runtime

mod interlude {
    pub use big_sync_core::{ObjId, PeerId};
    pub use utils_rs::prelude::*;
}

use crate::interlude::*;
use crate::rpc::FullDoc;

use std::collections::BTreeSet;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};

use automerge::ChangeHash;
use autosurgeon::{Hydrate, Prop, Reconcile};
use sedimentree_core::loose_commit::id::CommitId;

// FIXME: properly test the changes impl and investigate
// why it no longer has users
mod backend;
#[expect(unused)]
mod changes;
pub mod rpc;
mod runtime;
pub use runtime::{PutDocError, SyncDocOutcome};

use backend::BigRepoSyncBackend;

pub use changes::{
    path_prefix_matches as big_repo_path_prefix_matches, BigRepoChangeNotification,
    BigRepoChangeOrigin, ChangeFilter as BigRepoChangeFilter,
    ChangeListenerRegistration as BigRepoChangeListenerRegistration,
    DocIdFilter as BigRepoDocIdFilter, OriginFilter as BigRepoOriginFilter,
};

pub type DocumentId = ObjId;
pub type SharedPartStore = Arc<dyn big_sync::HostPartStore>;

#[derive(Debug, Clone)]
pub struct Config {
    pub peer_id: PeerId,
    pub secret_key_bytes: [u8; 32],
    pub storage: StorageConfig,
}

#[derive(Debug, Clone)]
pub enum StorageConfig {
    Disk { path: PathBuf },
    Memory,
}

#[derive(educe::Educe)]
#[educe(Debug)]
pub struct BigRepo {
    local_peer_id: PeerId,
    #[educe(Debug(ignore))]
    big_sync_store: SharedPartStore,
    #[educe(Debug(ignore))]
    runtime: runtime::BigRepoRuntimeHandle,
    #[educe(Debug(ignore))]
    change_manager: Arc<changes::ChangeListenerManager>,
    #[educe(Debug(ignore))]
    change_manager_stop: std::sync::Mutex<Option<changes::ChangeListenerManagerStopToken>>,
    #[educe(Debug(ignore))]
    sync_backend: tokio::sync::OnceCell<Arc<BigRepoSyncBackend>>,
}

pub type SharedBigRepo = Arc<BigRepo>;

impl BigRepo {
    pub const BACKEND_ID: &'static str = "BigRepoSyncBackend";

    pub async fn boot(
        config: Config,
        big_sync_store: SharedPartStore,
    ) -> Res<(Arc<Self>, BigRepoStopToken)> {
        let Config {
            peer_id,
            secret_key_bytes,
            storage,
        } = config;

        let (change_manager, change_manager_stop) = changes::ChangeListenerManager::boot();
        let signer = subduction_crypto::signer::memory::MemorySigner::from_bytes(&secret_key_bytes);
        let (runtime, runtime_stop) = match storage {
            StorageConfig::Memory => runtime::spawn_big_repo_runtime(
                signer,
                subduction_core::storage::memory::MemoryStorage::new(),
                Arc::clone(&big_sync_store),
                Arc::clone(&change_manager),
            )?,
            StorageConfig::Disk { path } => {
                let subduction_dir = path.join("subduction");
                std::fs::create_dir_all(&subduction_dir).wrap_err_with(|| {
                    format!(
                        "Failed to create subduction directory: {}",
                        subduction_dir.display()
                    )
                })?;
                let fs_storage = sedimentree_fs_storage::FsStorage::new(subduction_dir)
                    .wrap_err("failed booting subduction fs storage")?;
                runtime::spawn_big_repo_runtime(
                    signer,
                    fs_storage,
                    Arc::clone(&big_sync_store),
                    Arc::clone(&change_manager),
                )?
            }
        };

        let out = Arc::new(Self {
            local_peer_id: peer_id,
            big_sync_store,
            runtime,
            change_manager,
            change_manager_stop: std::sync::Mutex::new(Some(change_manager_stop)),
            sync_backend: tokio::sync::OnceCell::new(),
        });

        let sync_backend = Arc::new(
            BigRepoSyncBackend::boot(Arc::downgrade(&out))
                .await
                .wrap_err("failed booting big repo sync backend")?,
        );
        out.sync_backend
            .set(Arc::clone(&sync_backend))
            .unwrap_or_else(|_| panic!("big repo sync backend already initialized"));

        let change_manager_stop = out
            .change_manager_stop
            .lock()
            .expect(ERROR_MUTEX)
            .take()
            .expect("BigRepo change manager stop token missing");

        Ok((
            Arc::clone(&out),
            BigRepoStopToken {
                runtime_stop,
                change_manager_stop: Some(change_manager_stop),
            },
        ))
    }

    pub fn local_peer_id(&self) -> PeerId {
        self.local_peer_id
    }

    pub fn sync_backend(self: &Arc<Self>) -> Arc<dyn big_sync::SyncBackend> {
        let backend = self.sync_backend.get().expect(ERROR_IMPOSSIBLE);
        let backend: Arc<dyn big_sync::SyncBackend> = Arc::clone(backend) as _;
        backend
    }

    fn register_remote_repo_peer(
        self: &Arc<Self>,
        peer_id: PeerId,
        endpoint_addr: iroh::EndpointAddr,
    ) {
        let backend = self
            .sync_backend
            .get()
            .expect("big repo sync backend not initialized");
        backend.register_remote_peer(peer_id, endpoint_addr)
    }

    pub fn unregister_remote_repo_peer(self: &Arc<Self>, peer_id: PeerId) {
        let backend = self
            .sync_backend
            .get()
            .expect("big repo sync backend not initialized");
        backend.unregister_remote_peer(peer_id)
    }
}

// main methods
impl BigRepo {
    #[tracing::instrument(
        skip_all,
        fields(%document_id, %self.local_peer_id)
    )]
    pub async fn get_doc(self: &Arc<Self>, document_id: &DocumentId) -> Res<Option<BigDocHandle>> {
        let out = self
            .runtime
            .get_doc_handle(*document_id)
            .await?
            .map(|bundle| BigDocHandle {
                repo: Arc::clone(self),
                bundle,
            });
        Ok(out)
    }

    #[tracing::instrument(skip_all, fields(%document_id, %self.local_peer_id))]
    pub async fn put_doc(
        self: &Arc<Self>,
        document_id: DocumentId,
        initial_content: automerge::Automerge,
    ) -> Result<BigDocHandle, runtime::PutDocError> {
        let bundle = self.runtime.put_doc(document_id, initial_content).await?;
        Ok(BigDocHandle {
            repo: Arc::clone(self),
            bundle,
        })
    }

    #[tracing::instrument(
        skip_all,
        fields(%doc_id, %self.local_peer_id)
    )]
    pub async fn export_doc(&self, doc_id: &DocumentId) -> Res<Option<Vec<u8>>> {
        self.runtime.export_doc_save(*doc_id).await
    }
}

// iroh support
impl BigRepo {
    #[tracing::instrument(
        skip_all,
        fields(?peer_id, ?endpoint_addr, %self.local_peer_id)
    )]
    pub async fn open_connection_iroh(
        self: &Arc<Self>,
        endpoint: iroh::Endpoint,
        endpoint_addr: iroh::EndpointAddr,
        peer_id: PeerId,
        end_signal_tx: Option<tokio::sync::mpsc::UnboundedSender<ConnFinishSignal>>,
    ) -> Res<BigRepoConnection> {
        let register_endpoint_addr = endpoint_addr.clone();
        let (peer_id, closed) = self
            .runtime
            .open_connection_iroh(endpoint, endpoint_addr, peer_id, end_signal_tx)
            .await?;
        self.register_remote_repo_peer(peer_id, register_endpoint_addr);
        Ok(BigRepoConnection {
            repo: Arc::clone(self),
            peer_id,
            closed,
        })
    }

    #[tracing::instrument(
        skip_all,
        fields(%self.local_peer_id)
    )]
    pub async fn accept_connection_iroh(
        self: &Arc<Self>,
        conn: iroh::endpoint::Connection,
        end_signal_tx: Option<tokio::sync::mpsc::UnboundedSender<ConnFinishSignal>>,
    ) -> Res<BigRepoConnection> {
        let (peer_id, closed) = self
            .runtime
            .accept_connection_iroh(conn, end_signal_tx)
            .await?;
        let endpoint_addr = iroh::EndpointAddr::new(
            iroh::PublicKey::from_bytes(peer_id.as_bytes())
                .expect("big repo peer id must be a valid iroh public key"),
        );
        self.register_remote_repo_peer(peer_id, endpoint_addr);
        Ok(BigRepoConnection {
            repo: Arc::clone(self),
            peer_id,
            closed,
        })
    }
}

#[derive(Clone, educe::Educe)]
#[educe(Debug)]
pub struct BigRepoConnection {
    #[educe(Debug(ignore))]
    repo: Arc<BigRepo>,
    pub peer_id: PeerId,
    #[educe(Debug(ignore))]
    closed: Arc<AtomicBool>,
}

pub struct ConnFinishSignal {
    pub peer_id: PeerId,
    pub err: Option<eyre::Report>,
}

impl BigRepoConnection {
    pub fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// NOTE: a succesful outcome doesn't correspond to doc
    /// handles having the latest heads
    pub async fn sync_with_peer(
        &self,
        doc_id: DocumentId,
        timeout: Option<std::time::Duration>,
    ) -> Res<SyncDocOutcome> {
        if self.is_closed() {
            eyre::bail!("connection is closed");
        }
        self.repo
            .runtime
            .sync_doc_with_peer(doc_id, self.peer_id, timeout)
            .await
    }

    pub async fn stop(self) -> Res<()> {
        self.repo.unregister_remote_repo_peer(self.peer_id);
        self.repo.runtime.close_peer_connection(self.peer_id).await
    }
}

// change listeners
impl BigRepo {
    pub async fn subscribe_change_listener(
        self: &Arc<Self>,
        filter: BigRepoChangeFilter,
    ) -> Res<(
        BigRepoChangeListenerRegistration,
        tokio::sync::mpsc::UnboundedReceiver<Vec<BigRepoChangeNotification>>,
    )> {
        let (registration, change_rx) = self.change_manager.subscribe_listener(filter).await?;
        Ok((registration, change_rx))
    }
}

// big_sync support
impl BigRepo {
    pub async fn doc_payload_heads(&self, doc_id: DocumentId) -> Res<Option<Arc<[ChangeHash]>>> {
        partition_doc_heads_payload(&self.big_sync_store, doc_id).await
    }

    pub async fn get_docs_full(&self, doc_ids: &[String]) -> Res<Vec<FullDoc>> {
        if doc_ids.len() > crate::rpc::MAX_GET_DOCS_FULL_DOC_IDS {
            return Err(crate::rpc::BigRepoRpcError::Internal {
                message: format!(
                    "requested too many docs: {} exceeds max {}",
                    doc_ids.len(),
                    crate::rpc::MAX_GET_DOCS_FULL_DOC_IDS
                ),
            }
            .into());
        }

        let mut dedup = HashSet::new();
        let requested_doc_ids: Vec<String> = doc_ids
            .iter()
            .filter(|doc_id| dedup.insert((*doc_id).clone()))
            .cloned()
            .collect();

        use futures::StreamExt;
        use futures_buffered::BufferedStreamExt;
        let rows = futures::stream::iter(requested_doc_ids.into_iter().map(|doc_id| async move {
            let parsed = match DocumentId::from_str(&doc_id) {
                Ok(val) => val,
                Err(_) => return Ok(None),
            };
            let Some(automerge_save) = self.export_doc(&parsed).await? else {
                return Ok(None);
            };
            Ok(Some(FullDoc {
                doc_id,
                automerge_save,
            }))
        }))
        .buffered_unordered(16)
        .collect::<Vec<Res<Option<FullDoc>>>>()
        .await;

        let mut out = Vec::new();
        for row in rows {
            if let Some(doc) = row? {
                out.push(doc);
            }
        }
        Ok(out)
    }
}

pub struct BigRepoStopToken {
    runtime_stop: runtime::BigRepoRuntimeStopToken,
    change_manager_stop: Option<changes::ChangeListenerManagerStopToken>,
}

impl BigRepoStopToken {
    pub async fn stop(mut self) -> Res<()> {
        self.runtime_stop.stop().await?;
        if let Some(stop_token) = self.change_manager_stop.take() {
            stop_token.stop().await?;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct BigDocHandle {
    repo: Arc<BigRepo>,
    bundle: Arc<runtime::LiveDocBundle>,
}

impl std::fmt::Debug for BigDocHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BigDocHandle")
            .field("document_id", &self.document_id())
            .finish()
    }
}

impl BigDocHandle {
    pub fn document_id(&self) -> DocumentId {
        self.bundle.doc_id
    }

    #[cfg(test)]
    async fn fragment_state_store_len(&self) -> usize {
        self.bundle.fragment_state_store.lock().await.len()
    }

    pub async fn with_document_read<F, R>(&self, operation: F) -> R
    where
        F: FnOnce(&automerge::Automerge) -> R,
    {
        let doc = self.bundle.doc.lock().await;

        operation(&doc)
    }

    pub async fn with_document<F, R>(&self, operation: F) -> Res<R>
    where
        F: FnOnce(&mut automerge::Automerge) -> R,
    {
        self.with_document_with_origin(operation, BigRepoChangeOrigin::Local)
            .await
    }

    pub async fn with_document_with_origin<F, R>(
        &self,
        operation: F,
        origin: BigRepoChangeOrigin,
    ) -> Res<R>
    where
        F: FnOnce(&mut automerge::Automerge) -> R,
    {
        let mut doc = self.bundle.doc.lock().await;

        let before_heads = doc.get_heads();
        let out = operation(&mut doc);
        let after_heads = doc.get_heads();
        if before_heads == after_heads {
            return Ok(out);
        }

        let changes = doc
            .get_changes(&before_heads)
            .into_iter()
            .map(|change| {
                let head = CommitId::new(change.hash().0);
                let parents = change
                    .deps()
                    .iter()
                    .map(|dep| CommitId::new(dep.0))
                    .collect::<BTreeSet<_>>();
                (head, parents, change.raw_bytes().to_vec())
            })
            .collect::<Vec<_>>();
        let patches = if self
            .repo
            .change_manager
            .has_change_listener_interest(self.document_id(), &origin)
        {
            doc.diff(&before_heads, &after_heads)
        } else {
            Vec::new()
        };
        drop(doc);

        self.repo
            .runtime
            .commit_delta(self.document_id(), changes, after_heads, patches, origin)
            .await?;

        Ok(out)
    }

    pub async fn reconcile_prop_with_actor<'a, T, P>(
        &self,
        obj_id: automerge::ObjId,
        prop_name: P,
        update: &T,
        actor_id: Option<automerge::ActorId>,
    ) -> Res<Option<ChangeHash>>
    where
        T: Hydrate + Reconcile + Send + Sync + 'static,
        P: Into<autosurgeon::Prop<'a>> + Send + Sync + 'static,
    {
        let res = self
            .with_document(|doc| {
                if let Some(actor) = &actor_id {
                    doc.set_actor(actor.clone());
                }
                doc.transact(|tx| {
                    autosurgeon::reconcile_prop(tx, obj_id, prop_name, update)
                        .wrap_err("error reconciling")?;
                    eyre::Ok(())
                })
            })
            .await
            .wrap_err("error on reconcile transaction")?;
        match res {
            Ok(success) => Ok(success.hash),
            Err(failure) => Err(ferr!("error on reconcile transaction: {failure:?}")),
        }
    }

    pub async fn hydrate_path<T: Hydrate + Reconcile + Send + Sync + 'static>(
        &self,
        obj_id: automerge::ObjId,
        path: Vec<Prop<'static>>,
    ) -> Res<Option<(T, Arc<[automerge::ChangeHash]>)>> {
        self.with_document_read(|doc| -> Res<Option<(T, Arc<[automerge::ChangeHash]>)>> {
            let heads: Arc<[automerge::ChangeHash]> = Arc::from(doc.get_heads());
            if path.is_empty() && obj_id == automerge::ROOT {
                let value: T = autosurgeon::hydrate(doc).wrap_err("error hydrating")?;
                Ok(Some((value, heads)))
            } else {
                match autosurgeon::hydrate_path(doc, &obj_id, path.clone()) {
                    Ok(Some(value)) => Ok(Some((value, heads))),
                    Ok(None) => Ok(None),
                    Err(err) => Err(ferr!("error hydrating: {err:?}")),
                }
            }
        })
        .await
    }

    pub async fn hydrate_path_at_heads<T: Hydrate + Reconcile + Send + Sync + 'static>(
        &self,
        heads: &[automerge::ChangeHash],
        obj_id: automerge::ObjId,
        path: Vec<Prop<'static>>,
    ) -> Res<Option<(T, Arc<[automerge::ChangeHash]>)>> {
        self.with_document_read(|doc| -> Res<Option<(T, Arc<[automerge::ChangeHash]>)>> {
            let heads: Arc<[automerge::ChangeHash]> = Arc::from(heads.to_vec());
            if path.is_empty() && obj_id == automerge::ROOT {
                let value: T = autosurgeon::hydrate_at(doc, &heads).wrap_err("error hydrating")?;
                Ok(Some((value, heads)))
            } else {
                match autosurgeon::hydrate_path_at(doc, &obj_id, path.clone(), &heads) {
                    Ok(Some(value)) => Ok(Some((value, heads))),
                    Ok(None) => Ok(None),
                    Err(err) => Err(ferr!("error hydrating: {err:?}")),
                }
            }
        })
        .await
    }
}

async fn partition_doc_heads_payload(
    big_sync_store: &SharedPartStore,
    doc_id: DocumentId,
) -> Res<Option<Arc<[ChangeHash]>>> {
    Ok(big_sync_store
        .obj_payload(doc_id)
        .await?
        .map(doc_heads_from_payload))
}

fn doc_heads_from_payload(payload: serde_json::Value) -> Arc<[ChangeHash]> {
    let heads = payload
        .as_object()
        .expect(ERROR_IMPOSSIBLE)
        .get("heads")
        .cloned()
        .expect(ERROR_IMPOSSIBLE);
    let heads: Vec<String> = serde_json::from_value(heads).expect(ERROR_IMPOSSIBLE);
    Arc::from(am_utils_rs::parse_commit_heads(&heads).expect(ERROR_IMPOSSIBLE))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    use am_utils_rs::codecs::ThroughJson;
    use automerge::{transaction::Transactable, ReadDoc, ScalarValue};
    use autosurgeon::Prop;
    use big_sync::backend::contract::{
        self, SyncBackendHarness, SyncBackendOutcome, SyncBackendScenario,
    };
    use big_sync::stress_support::{self, StressFixture};
    use big_sync::{HostPartStore, SyncBackend};
    use big_sync_core::{Byte32Id, PartId, SyncCompletionDeets};
    use sqlx::sqlite::SqliteConnectOptions;
    use std::collections::{BTreeMap, BTreeSet, HashMap};
    use std::fmt::Write as _;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::tempdir;
    use tokio::{sync::Notify, time::timeout};

    pub async fn boot_part_store(
        sqlite_url: &str,
    ) -> Res<(Arc<big_sync::Ctx>, big_sync::StopToken)> {
        let (read_pool, write_pool) = {
            let connect_options = SqliteConnectOptions::from_str(sqlite_url)
                .expect(ERROR_IMPOSSIBLE)
                .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
                .create_if_missing(true);
            let read_pool = sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(4)
                .connect_with(connect_options.clone())
                .await
                .wrap_err("failed connecting big repo sqlite read pool")?;
            let write_pool = sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(1)
                .connect_with(connect_options)
                .await
                .wrap_err("failed connecting big repo sqlite write pool")?;
            (read_pool, write_pool)
        };

        let store = Arc::new(
            big_sync::SqlitePartStore::new(
                read_pool,
                write_pool,
                sqlite_url.to_owned(),
                big_sync_core::BuckId::MAX_LEVEL,
            )
            .await?,
        );
        let store_for_worker: Arc<dyn big_sync::HostPartStore> = store.clone();
        let (worker, stop) =
            big_sync::spawn_big_sync_worker(store_for_worker.clone(), HashMap::new())?;
        Ok((
            Arc::new(big_sync::Ctx {
                store: store_for_worker,
                worker,
            }),
            stop,
        ))
    }
    pub async fn boot_repo() -> Res<(
        Arc<BigRepo>,
        Arc<big_sync::Ctx>,
        Box<dyn FnOnce() -> futures::future::BoxFuture<'static, Res<()>>>,
    )> {
        let (big_sync_host, big_sync_stop) = boot_part_store("sqlite::memory:").await?;
        let (repo, stop) = BigRepo::boot(
            Config {
                peer_id: PeerId::new([7_u8; 32]),
                secret_key_bytes: [7_u8; 32],
                storage: StorageConfig::Memory,
            },
            Arc::clone(&big_sync_host.store),
        )
        .await?;
        Ok((
            repo,
            big_sync_host,
            Box::new(move || {
                async move {
                    stop.stop().await?;
                    big_sync_stop.stop().await?;
                    eyre::Ok(())
                }
                .boxed()
            }),
        ))
    }

    pub async fn boot_disk_repo(
        path: PathBuf,
    ) -> Res<(
        Arc<BigRepo>,
        Arc<big_sync::Ctx>,
        Box<dyn FnOnce() -> futures::future::BoxFuture<'static, Res<()>>>,
    )> {
        std::fs::create_dir_all(&path)
            .wrap_err_with(|| format!("failed creating disk repo path: {}", path.display()))?;
        let (big_sync_host, big_sync_stop) = boot_part_store(&format!(
            "sqlite://{}",
            path.join("part_store.db").display()
        ))
        .await?;
        let (repo, stop) = BigRepo::boot(
            Config {
                peer_id: PeerId::new([7_u8; 32]),
                secret_key_bytes: [7_u8; 32],
                storage: StorageConfig::Disk { path },
            },
            Arc::clone(&big_sync_host.store),
        )
        .await?;
        Ok((
            repo,
            big_sync_host,
            Box::new(move || {
                async move {
                    stop.stop().await?;
                    big_sync_stop.stop().await?;
                    eyre::Ok(())
                }
                .boxed()
            }),
        ))
    }

    fn get_int_at_root(doc: &automerge::Automerge, key: &str) -> i64 {
        let value = doc
            .get(automerge::ROOT, key)
            .expect("failed reading document")
            .expect("missing key");
        let automerge::Value::Scalar(scalar) = value.0 else {
            panic!("expected scalar value at root");
        };
        match scalar.as_ref() {
            ScalarValue::Int(value) => *value,
            _ => panic!("expected int scalar"),
        }
    }

    fn get_str_at_root(doc: &automerge::Automerge, key: &str) -> String {
        let value = doc
            .get(automerge::ROOT, key)
            .expect("failed reading document")
            .expect("missing key");
        let automerge::Value::Scalar(scalar) = value.0 else {
            panic!("expected scalar value at root");
        };
        match scalar.as_ref() {
            ScalarValue::Str(value) => value.to_string(),
            _ => panic!("expected string scalar"),
        }
    }

    fn random_doc_id() -> DocumentId {
        DocumentId::random()
    }

    async fn recv_change_batch(
        rx: &mut tokio::sync::mpsc::UnboundedReceiver<Vec<BigRepoChangeNotification>>,
    ) -> Vec<BigRepoChangeNotification> {
        timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("timed out waiting for change batch")
            .expect("change listener closed unexpectedly")
    }

    async fn recv_head_batch(
        rx: &mut tokio::sync::mpsc::UnboundedReceiver<Vec<super::changes::BigRepoHeadNotification>>,
    ) -> Vec<super::changes::BigRepoHeadNotification> {
        timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("timed out waiting for head batch")
            .expect("head listener closed unexpectedly")
    }

    fn read_proc_status_value(label: &str) -> Option<u64> {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        status.lines().find_map(|line| {
            let rest = line.strip_prefix(label)?;
            rest.split_whitespace().next()?.parse().ok()
        })
    }

    fn rss_kib() -> Option<u64> {
        read_proc_status_value("VmRSS:")
    }

    fn hwm_kib() -> Option<u64> {
        read_proc_status_value("VmHWM:")
    }

    #[tokio::test]
    async fn put_doc_get_doc_and_export_roundtrip() -> Res<()> {
        let (repo, _part_store, _stop_token) = boot_repo().await?;
        let doc_id = random_doc_id();
        let mut doc = automerge::Automerge::new();
        doc.transact(|tx| tx.put(automerge::ROOT, "title", "seed"))
            .expect("failed seeding doc");

        let handle = repo.put_doc(doc_id, doc).await?;
        let fetched = repo.get_doc(&doc_id).await?.expect("doc should exist");
        assert_eq!(fetched.document_id(), doc_id);
        assert_eq!(
            fetched
                .with_document_read(|doc| get_str_at_root(doc, "title"))
                .await,
            "seed"
        );
        let exported = repo
            .export_doc(&doc_id)
            .await?
            .expect("export should exist");
        assert!(!exported.is_empty());
        drop(handle);
        Ok(())
    }

    #[tokio::test]
    async fn reopened_doc_rehydrates_fragment_state_store() -> Res<()> {
        let temp_root = tempdir()?;
        let repo_path = temp_root.path().join("repo");
        let doc_id = random_doc_id();
        let doc = {
            let mut out = None;
            for _ in 0..64 {
                let payload = uuid::Uuid::new_v4().to_string();
                let value = make_sync_doc_value_with_payload("base", 1024, &payload);
                let mut doc = automerge::Automerge::new();
                write_sync_doc_value_as_transactions(&mut doc, &value);
                if sync_doc_fragment_state_count(&doc) > 0 {
                    out = Some(doc);
                    break;
                }
            }
            out.expect("test fixture should generate a fragmented doc")
        };

        let (repo, _part_store, stop_token) = boot_disk_repo(repo_path.clone()).await?;
        let handle = repo.put_doc(doc_id, doc).await?;
        assert!(
            handle.fragment_state_store_len().await > 0,
            "test fixture should start with fragment state"
        );
        drop(handle);
        stop_token().await?;

        let (repo, _part_store, stop_token) = boot_disk_repo(repo_path).await?;
        let reopened = repo.get_doc(&doc_id).await?.expect("doc should exist");
        assert!(
            reopened.fragment_state_store_len().await > 0,
            "reopened docs should rebuild fragment state"
        );
        drop(reopened);
        stop_token().await?;
        Ok(())
    }

    fn sync_doc_fragment_state_count(doc: &automerge::Automerge) -> usize {
        use automerge_sedimentree::indexed::IndexedSedimentreeAutomerge;
        use sedimentree_core::{
            collections::Map,
            commit::{CommitStore, CountLeadingZeroBytes, FragmentState},
        };

        let metadata = doc.get_changes_meta(&[]);
        let heads: Vec<_> = doc
            .get_heads()
            .iter()
            .map(|hash| sedimentree_core::loose_commit::id::CommitId::new(hash.0))
            .collect();
        let store = IndexedSedimentreeAutomerge::from_metadata(&metadata);
        let mut known: Map<
            sedimentree_core::loose_commit::id::CommitId,
            FragmentState<automerge_sedimentree::indexed::OwnedParents>,
        > = Map::new();
        store
            .build_fragment_store(&heads, &mut known, &CountLeadingZeroBytes)
            .expect("build_fragment_store")
            .len()
    }

    #[tokio::test]
    async fn put_doc_rejects_existing_local_doc_id() -> Res<()> {
        let (repo, _part_store, _stop_token) = boot_repo().await?;
        let doc_id = random_doc_id();
        let _ = repo.put_doc(doc_id, automerge::Automerge::new()).await?;
        let err = repo
            .put_doc(doc_id, automerge::Automerge::new())
            .await
            .expect_err("expected conflict");
        assert!(matches!(
            err,
            runtime::PutDocError::IdOccpuied { id } if id == doc_id
        ));
        Ok(())
    }

    #[test]
    #[ignore]
    fn large_fragment_store_build_profile_par() -> Res<()> {
        use automerge_sedimentree::indexed::IndexedSedimentreeAutomerge;
        use sedimentree_core::{
            collections::Map,
            commit::{CommitStore, CountLeadingZeroBytes, FragmentState},
        };
        use std::time::Instant;

        const SWEEP_COUNTS: &[usize] = &[128, 256, 512, 1024, 2048, 4096];

        for &item_count in SWEEP_COUNTS {
            let payload = uuid::Uuid::new_v4().to_string();
            let value_started = Instant::now();
            let value = make_sync_doc_value_with_payload("base", item_count, &payload);
            let value_elapsed = value_started.elapsed();

            let mut doc = automerge::Automerge::new();
            let write_started = Instant::now();
            write_sync_doc_value_as_transactions(&mut doc, &value);
            let write_elapsed = write_started.elapsed();

            let meta_started = Instant::now();
            let metadata = doc.get_changes_meta(&[]);
            let meta_elapsed = meta_started.elapsed();
            let heads: Vec<_> = doc
                .get_heads()
                .iter()
                .map(|hash| sedimentree_core::loose_commit::id::CommitId::new(hash.0))
                .collect();
            let store = IndexedSedimentreeAutomerge::from_metadata(&metadata);
            let mut known: Map<
                sedimentree_core::loose_commit::id::CommitId,
                FragmentState<automerge_sedimentree::indexed::OwnedParents>,
            > = Map::new();

            let rss_before = rss_kib();
            let hwm_before = hwm_kib();
            let start = Instant::now();
            let fresh = store
                .build_fragment_store_par(&heads, &mut known, &CountLeadingZeroBytes)
                .expect("build_fragment_store");
            let elapsed = start.elapsed();
            let rss_after = rss_kib();
            let hwm_after = hwm_kib();

            eprintln!(
                "large_fragment_store_profile_par item_count={} value_ms={} write_ms={} metadata_ms={} build_ms={} changes={} heads={} fragments={} rss_kib_before={:?} rss_kib_after={:?} hwm_kib_before={:?} hwm_kib_after={:?}",
                item_count,
                value_elapsed.as_millis(),
                write_elapsed.as_millis(),
                meta_elapsed.as_millis(),
                elapsed.as_millis(),
                metadata.len(),
                heads.len(),
                fresh.len(),
                rss_before,
                rss_after,
                hwm_before,
                hwm_after,
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn with_document_roundtrip_rehydrates_from_storage() -> Res<()> {
        let (repo, _part_store, _stop_token) = boot_repo().await?;
        let mut doc = automerge::Automerge::new();
        doc.transact(|tx| tx.put(automerge::ROOT, "title", "before"))
            .expect("failed initializing title");

        let doc_id = random_doc_id();
        let handle = repo.put_doc(doc_id, doc).await?;
        handle
            .with_document(|doc| {
                doc.transact(|tx| tx.put(automerge::ROOT, "title", "after"))
                    .expect("failed mutating doc");
            })
            .await?;
        drop(handle);

        let reloaded = repo.get_doc(&doc_id).await?.expect("doc should exist");
        let title = reloaded
            .with_document_read(|doc| get_str_at_root(doc, "title"))
            .await;
        assert_eq!(title, "after");
        Ok(())
    }

    #[tokio::test]
    async fn change_listener_doc_id_filter_only_receives_target_doc() -> Res<()> {
        let (repo, _part_store, _stop_token) = boot_repo().await?;
        let first_handle = repo
            .put_doc(random_doc_id(), automerge::Automerge::new())
            .await?;
        let first_doc_id = first_handle.document_id();
        let second_handle = repo
            .put_doc(random_doc_id(), automerge::Automerge::new())
            .await?;

        let (_registration, mut rx) = repo
            .subscribe_change_listener(BigRepoChangeFilter {
                doc_id: Some(BigRepoDocIdFilter::new(first_doc_id)),
                origin: None,
                path: Vec::new(),
            })
            .await?;

        first_handle
            .with_document(|doc| {
                doc.transact(|tx| tx.put(automerge::ROOT, "title", "first"))
                    .expect("failed mutating first doc");
            })
            .await?;
        second_handle
            .with_document(|doc| {
                doc.transact(|tx| tx.put(automerge::ROOT, "title", "second"))
                    .expect("failed mutating second doc");
            })
            .await?;

        let batch = recv_change_batch(&mut rx).await;
        assert!(!batch.is_empty());
        assert!(batch.iter().all(|item| match item {
            BigRepoChangeNotification::DocCreated { doc_id, .. }
            | BigRepoChangeNotification::DocImported { doc_id, .. }
            | BigRepoChangeNotification::DocChanged { doc_id, .. } => *doc_id == first_doc_id,
        }));
        Ok(())
    }

    #[tokio::test]
    async fn change_listener_path_filter_matches_only_prefix() -> Res<()> {
        let (repo, _part_store, _stop_token) = boot_repo().await?;
        let handle = repo
            .put_doc(random_doc_id(), automerge::Automerge::new())
            .await?;
        let doc_id = handle.document_id();

        handle
            .with_document(|doc| {
                doc.transact(|tx| {
                    let profile = tx
                        .put_object(automerge::ROOT, "profile", automerge::ObjType::Map)
                        .expect("failed creating profile object");
                    tx.put(&profile, "title", "seed")
                        .expect("failed seeding profile title");
                    eyre::Ok(())
                })
                .expect("failed seeding nested profile");
            })
            .await?;

        let profile_obj = handle
            .with_document_read(|doc| {
                let Some((automerge::Value::Object(_), profile_obj)) = doc
                    .get(automerge::ROOT, "profile")
                    .expect("failed reading profile")
                else {
                    panic!("expected profile object");
                };
                profile_obj
            })
            .await;

        let (_registration, mut rx) = repo
            .subscribe_change_listener(BigRepoChangeFilter {
                doc_id: Some(BigRepoDocIdFilter::new(doc_id)),
                origin: None,
                path: vec![Prop::Key("profile".into())],
            })
            .await?;

        handle
            .with_document(|doc| {
                doc.transact(|tx| {
                    tx.put(&profile_obj, "title", "one")
                        .expect("failed mutating profile title");
                    eyre::Ok(())
                })
                .expect("failed mutating nested profile");
            })
            .await?;
        handle
            .with_document(|doc| {
                doc.transact(|tx| tx.put(automerge::ROOT, "body", "two"))
                    .expect("failed mutating body");
            })
            .await?;

        let batch = recv_change_batch(&mut rx).await;
        assert_eq!(batch.len(), 1);
        let BigRepoChangeNotification::DocChanged {
            doc_id: seen_doc_id,
            patch,
            ..
        } = &batch[0]
        else {
            panic!("expected doc changed notification");
        };
        assert_eq!(*seen_doc_id, doc_id);
        assert!(big_repo_path_prefix_matches(
            &[Prop::Key("profile".into())],
            &patch.path[..]
        ));
        Ok(())
    }

    #[tokio::test]
    async fn change_listener_origin_filter_works_for_local_events() -> Res<()> {
        let (repo, _part_store, _stop_token) = boot_repo().await?;
        let (_registration, mut rx) = repo
            .subscribe_change_listener(BigRepoChangeFilter {
                doc_id: None,
                origin: Some(BigRepoOriginFilter::Local),
                path: Vec::new(),
            })
            .await?;

        let handle = repo
            .put_doc(random_doc_id(), automerge::Automerge::new())
            .await?;
        let doc_id = handle.document_id();

        let batch = recv_change_batch(&mut rx).await;
        assert!(batch.iter().any(|item| matches!(
            item,
            BigRepoChangeNotification::DocCreated {
                doc_id: seen_doc_id,
                ..
            } | BigRepoChangeNotification::DocImported {
                doc_id: seen_doc_id,
                ..
            } if *seen_doc_id == doc_id
        )));
        Ok(())
    }

    #[tokio::test]
    async fn change_and_head_listeners_ignore_noop_mutation() -> Res<()> {
        let (repo, _part_store, _stop_token) = boot_repo().await?;
        let handle = repo
            .put_doc(random_doc_id(), automerge::Automerge::new())
            .await?;
        let doc_id = handle.document_id();

        let (_change_registration, mut change_rx) = repo
            .subscribe_change_listener(BigRepoChangeFilter {
                doc_id: Some(BigRepoDocIdFilter::new(doc_id)),
                origin: Some(BigRepoOriginFilter::Local),
                path: Vec::new(),
            })
            .await?;
        let (_head_registration, mut head_rx) = repo
            .change_manager
            .subscribe_head_listener(super::changes::HeadFilter {
                doc_id: Some(super::changes::DocIdFilter::new(doc_id)),
            })
            .await?;

        handle
            .with_document(|_| {
                // No-op on purpose.
            })
            .await?;

        assert!(timeout(Duration::from_millis(250), change_rx.recv())
            .await
            .is_err());
        assert!(timeout(Duration::from_millis(250), head_rx.recv())
            .await
            .is_err());
        Ok(())
    }

    #[tokio::test]
    async fn remote_change_and_head_notifications_survive_handle_reopen() -> Res<()> {
        let (repo, _part_store, _stop_token) = boot_repo().await?;
        let doc_id = random_doc_id();
        let mut doc = automerge::Automerge::new();
        doc.transact(|tx| tx.put(automerge::ROOT, "title", "seed"))
            .expect("failed seeding title");

        let handle = repo.put_doc(doc_id, doc).await?;
        drop(handle);
        let handle = repo.get_doc(&doc_id).await?.expect("doc should exist");

        let (_change_registration, mut change_rx) = repo
            .subscribe_change_listener(BigRepoChangeFilter {
                doc_id: Some(BigRepoDocIdFilter::new(doc_id)),
                origin: Some(BigRepoOriginFilter::Remote),
                path: Vec::new(),
            })
            .await?;
        let (_head_registration, mut head_rx) = repo
            .change_manager
            .subscribe_head_listener(super::changes::HeadFilter {
                doc_id: Some(super::changes::DocIdFilter::new(doc_id)),
            })
            .await?;

        handle
            .with_document_with_origin(
                |doc| {
                    doc.transact(|tx| tx.put(automerge::ROOT, "title", "remote-after"))
                        .expect("failed mutating remote doc");
                },
                BigRepoChangeOrigin::Remote {
                    peer_id: PeerId::new([9_u8; 32]),
                },
            )
            .await?;

        let change_batch = recv_change_batch(&mut change_rx).await;
        assert!(matches!(
            change_batch.as_slice(),
            [BigRepoChangeNotification::DocChanged {
                doc_id: seen_doc_id,
                origin: BigRepoChangeOrigin::Remote { .. },
                ..
            }] if *seen_doc_id == doc_id
        ));

        let head_batch: Vec<super::changes::BigRepoHeadNotification> =
            recv_head_batch(&mut head_rx).await;
        assert!(matches!(
            head_batch.as_slice(),
            [super::changes::BigRepoHeadNotification::DocHeadsChanged {
                doc_id: seen_doc_id,
                origin: BigRepoChangeOrigin::Remote { .. },
                ..
            }] if *seen_doc_id == doc_id
        ));

        let title = repo
            .get_doc(&doc_id)
            .await?
            .expect("doc should still exist")
            .with_document_read(|doc| get_str_at_root(doc, "title"))
            .await;
        assert_eq!(title, "remote-after");
        Ok(())
    }

    #[tokio::test]
    async fn with_document_handles_concurrent_writers() -> Res<()> {
        let (repo, _part_store, _stop_token) = boot_repo().await?;
        let handle = repo
            .put_doc(random_doc_id(), automerge::Automerge::new())
            .await?;
        let doc_id = handle.document_id();
        handle
            .with_document(|doc| {
                doc.transact(|tx| tx.put(automerge::ROOT, "count", 0))
                    .expect("failed initializing count");
            })
            .await?;

        let writer_count = 8_u64;
        let increments_per_writer = 25_u64;
        let mut joins = Vec::new();
        for _ in 0..writer_count {
            let repo = Arc::clone(&repo);
            joins.push(tokio::spawn(async move {
                let handle = repo
                    .get_doc(&doc_id)
                    .await
                    .expect("failed finding doc")
                    .expect("missing doc");
                for _ in 0..increments_per_writer {
                    handle
                        .with_document(|doc| {
                            doc.transact(|tx| {
                                let current = tx
                                    .get(automerge::ROOT, "count")
                                    .expect("failed reading count")
                                    .map(|(value, _)| match value {
                                        automerge::Value::Scalar(scalar) => match scalar.as_ref() {
                                            ScalarValue::Int(value) => *value,
                                            _ => panic!("unexpected scalar for count"),
                                        },
                                        _ => panic!("unexpected value type for count"),
                                    })
                                    .unwrap_or(0);
                                tx.put(automerge::ROOT, "count", current + 1)
                            })
                            .expect("failed incrementing count");
                        })
                        .await
                        .expect("with_document failed");
                }
            }));
        }
        for join in joins {
            join.await.expect("writer task panicked");
        }

        let final_count = repo
            .get_doc(&doc_id)
            .await?
            .expect("doc should exist")
            .with_document_read(|doc| get_int_at_root(doc, "count"))
            .await;
        assert_eq!(final_count, (writer_count * increments_per_writer) as i64);
        Ok(())
    }

    const SYNC_DOC_ITEMS: usize = 32;
    const SYNC_DOC_PAYLOAD_LEN: usize = 384;
    const SYNC_LARGE_DOC_ITEMS: usize = 1000;
    const SYNC_LARGE_DOC_PAYLOAD_LEN: usize = 1024;
    const SYNC_PROPAGATION_TIMEOUT: Duration = Duration::from_secs(10);
    const SYNC_CASE_TIMEOUT: Duration = Duration::from_secs(30);

    #[derive(Clone, Copy, Debug)]
    struct SyncMutation {
        item_idx: usize,
        note_key: &'static str,
        side_label: &'static str,
    }

    fn make_sync_doc_value(
        title: &str,
        item_count: usize,
        payload_len: usize,
    ) -> serde_json::Value {
        let payload = "v".repeat(payload_len.max(1));
        make_sync_doc_value_with_payload(title, item_count, &payload)
    }

    fn make_sync_doc_value_with_payload(
        title: &str,
        item_count: usize,
        payload: &str,
    ) -> serde_json::Value {
        serde_json::json!({
            "title": title,
            "items": (0..item_count)
                .map(|idx| serde_json::json!({
                    "value": format!("{title}-{idx}-{payload}"),
                    "note": ""
                }))
                .collect::<Vec<_>>(),
        })
    }

    fn apply_sync_mutation(
        doc: &mut serde_json::Value,
        mutation: SyncMutation,
        payload_len: usize,
    ) {
        let items = doc
            .get_mut("items")
            .and_then(serde_json::Value::as_array_mut)
            .expect("sync doc should contain an items array");
        let item = items
            .get_mut(mutation.item_idx)
            .and_then(serde_json::Value::as_object_mut)
            .expect("sync mutation item index should exist");
        item.insert(
            "note".into(),
            serde_json::Value::String(format!(
                "{}:{}:{}",
                mutation.side_label,
                mutation.note_key,
                "n".repeat(payload_len.max(1))
            )),
        );
    }

    fn sync_item_note(doc: &serde_json::Value, item_idx: usize) -> &str {
        doc.get("items")
            .and_then(serde_json::Value::as_array)
            .and_then(|items| items.get(item_idx))
            .and_then(serde_json::Value::as_object)
            .and_then(|item| item.get("note"))
            .and_then(serde_json::Value::as_str)
            .expect("sync doc item note should exist")
    }

    fn sync_note_snapshot(doc: &serde_json::Value, item_indices: &[usize]) -> Vec<(usize, String)> {
        item_indices
            .iter()
            .copied()
            .map(|item_idx| (item_idx, sync_item_note(doc, item_idx).to_string()))
            .collect()
    }

    fn apply_sync_mutation_in_place(
        doc: &mut automerge::Automerge,
        mutation: SyncMutation,
        payload_len: usize,
    ) {
        let note = format!(
            "{}:{}:{}",
            mutation.side_label,
            mutation.note_key,
            "n".repeat(payload_len.max(1))
        );
        let items_obj = doc
            .get(automerge::ROOT, "items")
            .expect("failed reading sync items list")
            .expect("sync doc should contain an items list")
            .1;
        let item_obj = doc
            .get(&items_obj, mutation.item_idx)
            .expect("failed reading sync item")
            .expect("sync mutation item index should exist")
            .1;
        doc.transact(|tx| {
            tx.put(&item_obj, "note", note.as_str())
                .expect("failed writing sync item note");
            eyre::Ok(())
        })
        .expect("failed applying sync mutation in place");
    }

    fn write_sync_doc_value(doc: &mut automerge::Automerge, value: &serde_json::Value) {
        let title = value
            .get("title")
            .and_then(serde_json::Value::as_str)
            .expect("sync doc should contain a title");
        let items = value
            .get("items")
            .and_then(serde_json::Value::as_array)
            .expect("sync doc should contain an items array");
        doc.transact(|tx| {
            tx.put(automerge::ROOT, "title", title)
                .expect("failed writing sync title");
            let items_obj = tx
                .put_object(automerge::ROOT, "items", automerge::ObjType::List)
                .expect("failed creating sync items list");
            for item in items.iter().rev() {
                let item_obj = tx
                    .insert_object(&items_obj, 0, automerge::ObjType::Map)
                    .expect("failed inserting sync item");
                let item_value = item
                    .get("value")
                    .and_then(serde_json::Value::as_str)
                    .expect("sync item should contain a string value");
                let item_note = item
                    .get("note")
                    .and_then(serde_json::Value::as_str)
                    .expect("sync item should contain a string note");
                tx.put(&item_obj, "value", item_value)
                    .expect("failed writing sync item value");
                tx.put(&item_obj, "note", item_note)
                    .expect("failed writing sync item note");
            }
            eyre::Ok(())
        })
        .expect("failed writing sync doc");
    }

    fn write_sync_doc_value_as_transactions(
        doc: &mut automerge::Automerge,
        value: &serde_json::Value,
    ) {
        let title = value
            .get("title")
            .and_then(serde_json::Value::as_str)
            .expect("sync doc should contain a title");
        let items = value
            .get("items")
            .and_then(serde_json::Value::as_array)
            .expect("sync doc should contain an items array");
        doc.transact(|tx| {
            tx.put(automerge::ROOT, "title", title)
                .expect("failed writing sync title");
            tx.put_object(automerge::ROOT, "items", automerge::ObjType::List)
                .expect("failed creating sync items list");
            eyre::Ok(())
        })
        .expect("failed creating sync doc root");

        let items_obj = doc
            .get(automerge::ROOT, "items")
            .expect("sync doc should contain an items list")
            .unwrap()
            .1;

        for (idx, item) in items.iter().enumerate() {
            let item_value = item
                .get("value")
                .and_then(serde_json::Value::as_str)
                .expect("sync item should contain a string value");
            let item_note = item
                .get("note")
                .and_then(serde_json::Value::as_str)
                .expect("sync item should contain a string note");
            doc.transact(|tx| {
                let item_obj = tx
                    .insert_object(&items_obj, idx, automerge::ObjType::Map)
                    .expect("failed inserting sync item");
                tx.put(&item_obj, "value", item_value)
                    .expect("failed writing sync item value");
                tx.put(&item_obj, "note", item_note)
                    .expect("failed writing sync item note");
                eyre::Ok(())
            })
            .expect("failed writing sync item");
        }
    }

    fn sync_test_part() -> PartId {
        PartId(Byte32Id::new([
            32, 12, 54, 54, 65, 112, 213, 43, 12, 54, 123, 123, 54, 23, 68, 12, //
            32, 12, 54, 54, 65, 112, 213, 43, 12, 54, 123, 123, 54, 23, 68, 12,
        ]))
    }

    fn sync_test_parts() -> Vec<PartId> {
        vec![sync_test_part()]
    }

    fn sync_test_parts_multi() -> Vec<PartId> {
        vec![sync_test_part(), PartId(Byte32Id::new([7; 32]))]
    }

    struct BigRepoSyncBackendContractHarness {
        backend: Arc<dyn SyncBackend>,
        store: Arc<dyn HostPartStore>,
    }

    #[async_trait::async_trait]
    impl SyncBackendHarness for BigRepoSyncBackendContractHarness {
        fn backend(&self) -> &dyn SyncBackend {
            self.backend.as_ref()
        }

        fn store(&self) -> &dyn HostPartStore {
            self.store.as_ref()
        }
    }

    #[tracing::instrument(skip_all, fields(doc_id = %handle.document_id()))]
    async fn read_json_doc(handle: &BigDocHandle) -> serde_json::Value {
        handle
            .with_document(|doc| {
                autosurgeon::hydrate::<_, ThroughJson<serde_json::Value>>(doc)
                    .expect("failed hydrating sync doc")
                    .0
            })
            .await
            .expect("sync doc should always hydrate as json")
    }

    #[tracing::instrument(skip_all, fields(doc_id = %handle.document_id(), timeout_ms = timeout_dur.as_millis() as u64))]
    async fn wait_for_json_doc(
        handle: &BigDocHandle,
        expected: &serde_json::Value,
        timeout_dur: Duration,
    ) {
        timeout(timeout_dur, async {
            loop {
                if read_json_doc(handle).await == *expected {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("timed out waiting for JSON document to converge");
    }

    #[derive(Clone, Debug)]
    struct SubductionProtocolHandler {
        repo: Arc<BigRepo>,
        track_accepts: bool,
        accept_count: Arc<AtomicUsize>,
        accept_notify: Arc<Notify>,
        accepted_connection: Arc<tokio::sync::Mutex<Option<BigRepoConnection>>>,
    }

    impl iroh::protocol::ProtocolHandler for SubductionProtocolHandler {
        async fn accept(
            &self,
            conn: iroh::endpoint::Connection,
        ) -> Result<(), iroh::protocol::AcceptError> {
            let connection = self
                .repo
                .accept_connection_iroh(conn, None)
                .await
                .map_err(|err| iroh::protocol::AcceptError::from_boxed(err.into()))?;
            if self.track_accepts {
                *self.accepted_connection.lock().await = Some(connection.clone());
                self.accept_count.fetch_add(1, Ordering::SeqCst);
                self.accept_notify.notify_waiters();
            }
            Ok(())
        }
    }

    #[derive(Clone, Debug)]
    struct RepoRpcProtocolHandler {
        tx: tokio::sync::mpsc::Sender<(PeerId, crate::rpc::RepoSyncRpcMessage)>,
    }

    impl iroh::protocol::ProtocolHandler for RepoRpcProtocolHandler {
        async fn accept(
            &self,
            conn: iroh::endpoint::Connection,
        ) -> Result<(), iroh::protocol::AcceptError> {
            let peer_id = PeerId::new(*conn.remote_id().as_bytes());
            loop {
                let msg = match irpc_iroh::read_request::<crate::rpc::RepoSyncRpc>(&conn).await {
                    Ok(Some(msg)) => msg,
                    Ok(None) => break,
                    Err(err) => {
                        tracing::warn!(?err, "error reading repo rpc request");
                        break;
                    }
                };
                if self.tx.send((peer_id, msg)).await.is_err() {
                    break;
                }
            }
            Ok(())
        }
    }

    struct StressBigSyncRpcClient {
        target_part_store: SharedPartStore,
    }

    #[async_trait::async_trait]
    impl big_sync::rpc::HostBigRpcClient for StressBigSyncRpcClient {
        async fn peer_summary(
            &self,
            req: big_sync_core::rpc::PeerSummaryRequest,
        ) -> Res<
            big_sync_core::rpc::BigSyncRpcResult<
                Result<big_sync_core::rpc::PeerSummaryResult, big_sync_core::rpc::ListPartsError>,
            >,
        > {
            let parts = self.target_part_store.summarize_parts(req.parts).await??;
            Ok(Ok(Ok(big_sync_core::rpc::PeerSummaryResult {
                parts,
                deepest_bucket_level: big_sync_core::BuckId::MAX_LEVEL,
            })))
        }

        async fn sub_parts(
            &self,
            req: big_sync_core::rpc::SubPartsRequest,
        ) -> Res<
            big_sync_core::rpc::BigSyncRpcResult<
                Result<
                    big_sync_core::mpsc::Receiver<big_sync_core::rpc::SubEvent>,
                    big_sync_core::rpc::ListPartsError,
                >,
            >,
        > {
            Ok(Ok(self.target_part_store.subscribe(req).await?))
        }

        async fn get_changed_buckets(
            &self,
            req: big_sync_core::rpc::GetChangedBucketsRequest,
        ) -> Res<
            big_sync_core::rpc::BigSyncRpcResult<
                Result<Vec<big_sync_core::rpc::BucketSummary>, big_sync_core::rpc::ListPartsError>,
            >,
        > {
            Ok(Ok(self.target_part_store.get_changed_buckets(req).await?))
        }

        async fn leaf_buckets(
            &self,
            req: big_sync_core::rpc::LeafBucketsRequest,
        ) -> Res<
            big_sync_core::rpc::BigSyncRpcResult<
                Result<big_sync_core::rpc::LeafBucketResult, big_sync_core::rpc::LeafBucketsError>,
            >,
        > {
            Ok(Ok(self.target_part_store.leaf_buckets(req).await?))
        }
    }

    async fn endpoint_addr_from_remote_info(
        endpoint: &iroh::Endpoint,
        endpoint_id: iroh::PublicKey,
    ) -> Res<iroh::EndpointAddr> {
        let remote_info = endpoint
            .remote_info(endpoint_id)
            .await
            .ok_or_eyre("unable to get remote endpoint info")?;
        Ok(iroh::EndpointAddr::from_parts(
            remote_info.id(),
            remote_info.into_addrs().map(|addr| addr.into_addr()),
        ))
    }

    struct SyncRepoNode {
        repo: Arc<BigRepo>,
        big_sync_store: SharedPartStore,
        big_sync_worker: big_sync::BigSyncWorkerHandle,
        docs: Arc<tokio::sync::Mutex<HashMap<ObjId, Arc<BigDocHandle>>>>,
        connections: Arc<tokio::sync::Mutex<HashMap<PeerId, BigRepoConnection>>>,
        stop_token: BigRepoStopToken,
        endpoint: iroh::Endpoint,
        router: iroh::protocol::Router,
        repo_rpc_stop: Option<crate::rpc::BigRepoRpcStopToken>,
        accept_count: Arc<AtomicUsize>,
        accept_notify: Arc<Notify>,
        accepted_connection: Arc<tokio::sync::Mutex<Option<BigRepoConnection>>>,
        big_sync_stop: big_sync::StopToken,
    }

    impl SyncRepoNode {
        #[tracing::instrument(skip(path), fields(seed, accept_incoming))]
        async fn boot(path: PathBuf, seed: u8, accept_incoming: bool) -> Res<Self> {
            tracing::info!(path = %path.display(), "booting sync repo node");
            std::fs::create_dir_all(&path)
                .wrap_err_with(|| format!("failed creating sync repo path: {}", path.display()))?;
            let (big_sync_host, big_sync_stop) = boot_part_store(&format!(
                "sqlite://{}",
                path.join("part_store.db").display()
            ))
            .await?;
            let part_init_obj = ObjId(big_sync_core::Byte32Id::new(
                [255_u8.wrapping_sub(seed); 32],
            ));
            big_sync_host
                .store
                .set_obj_payload(
                    part_init_obj,
                    serde_json::json!({ "heads": Vec::<String>::new() }),
                )
                .await?;
            big_sync_host
                .store
                .remove_obj_from_part(part_init_obj, stress_support::test_part())
                .await?;
            let secret_key_bytes = [seed; 32];
            let signer =
                subduction_crypto::signer::memory::MemorySigner::from_bytes(&secret_key_bytes);
            let peer_id = PeerId::new(*signer.verifying_key().as_bytes());
            let (repo, stop_token) = BigRepo::boot(
                Config {
                    peer_id,
                    secret_key_bytes,
                    storage: StorageConfig::Disk { path },
                },
                Arc::clone(&big_sync_host.store),
            )
            .await?;
            big_sync_stop.stop().await?;
            let mut sync_backends = HashMap::new();
            sync_backends.insert(BigRepo::BACKEND_ID.into(), repo.sync_backend());
            let (big_sync_worker, big_sync_stop) =
                big_sync::spawn_big_sync_worker(Arc::clone(&big_sync_host.store), sync_backends)?;
            let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
                .clear_ip_transports()
                .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))?
                .relay_mode(iroh::RelayMode::Disabled)
                .bind()
                .await
                .wrap_err("failed binding iroh endpoint")?;

            let (repo_rpc, repo_rpc_stop) = crate::rpc::spawn_repo_rpc(Arc::clone(&repo)).await?;
            let accept_count = Arc::new(AtomicUsize::new(0));
            let accept_notify = Arc::new(Notify::new());
            let accepted_connection = Arc::new(tokio::sync::Mutex::new(None));
            let docs = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
            let connections = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
            let router = iroh::protocol::Router::builder(endpoint.clone())
                .accept(
                    subduction_iroh::ALPN,
                    SubductionProtocolHandler {
                        repo: Arc::clone(&repo),
                        track_accepts: accept_incoming,
                        accept_count: Arc::clone(&accept_count),
                        accept_notify: Arc::clone(&accept_notify),
                        accepted_connection: Arc::clone(&accepted_connection),
                    },
                )
                .accept(
                    crate::rpc::REPO_SYNC_ALPN,
                    RepoRpcProtocolHandler {
                        tx: repo_rpc.local_sender(),
                    },
                )
                .spawn();

            tracing::info!(
                repo_peer_id = %repo.local_peer_id(),
                endpoint_id = %router.endpoint().addr().id,
                accept_incoming,
                "booted sync repo node"
            );

            Ok(Self {
                repo,
                big_sync_store: Arc::clone(&big_sync_host.store),
                big_sync_worker,
                docs,
                connections,
                stop_token,
                big_sync_stop,
                endpoint,
                router,
                repo_rpc_stop: Some(repo_rpc_stop),
                accept_count,
                accept_notify,
                accepted_connection,
            })
        }

        fn peer_id(&self) -> PeerId {
            self.repo.local_peer_id()
        }

        #[tracing::instrument(skip(self), fields(expected))]
        async fn wait_for_accepts(&self, expected: usize) {
            timeout(SYNC_PROPAGATION_TIMEOUT, async {
                loop {
                    if self.accept_count.load(Ordering::SeqCst) >= expected {
                        break;
                    }
                    self.accept_notify.notified().await;
                }
            })
            .await
            .expect("timed out waiting for iroh accept loop");
        }

        async fn accepted_connection(&self) -> BigRepoConnection {
            self.accepted_connection
                .lock()
                .await
                .clone()
                .expect("expected accepted connection to be available")
        }

        async fn connect_to(&self, remote: &SyncRepoNode) -> Res<()> {
            {
                let mut connections = self.connections.lock().await;
                if connections
                    .get(&remote.peer_id())
                    .is_some_and(|conn| !conn.is_closed())
                {
                    return Ok(());
                }
                connections.remove(&remote.peer_id());
            }
            let conn = self
                .repo
                .open_connection_iroh(
                    self.endpoint.clone(),
                    remote.endpoint.addr(),
                    remote.peer_id(),
                    None,
                )
                .await?;
            let remote_addr = endpoint_addr_from_remote_info(&self.endpoint, remote.endpoint.id())
                .await
                .unwrap_or_else(|_| remote.endpoint.addr());
            self.repo
                .register_remote_repo_peer(remote.peer_id(), remote_addr);
            let self_addr = endpoint_addr_from_remote_info(&remote.endpoint, self.endpoint.id())
                .await
                .unwrap_or_else(|_| self.endpoint.addr());
            remote
                .repo
                .register_remote_repo_peer(self.peer_id(), self_addr);
            let parts = stress_support::test_parts()
                .into_iter()
                .map(|part_id| (part_id, BigRepo::BACKEND_ID.into()))
                .collect();
            self.big_sync_worker
                .set_peer(
                    remote.peer_id(),
                    Arc::new(StressBigSyncRpcClient {
                        target_part_store: Arc::clone(&remote.big_sync_store),
                    }),
                    parts,
                )
                .await?;
            let parts = stress_support::test_parts()
                .into_iter()
                .map(|part_id| (part_id, BigRepo::BACKEND_ID.into()))
                .collect();
            remote
                .big_sync_worker
                .set_peer(
                    self.peer_id(),
                    Arc::new(StressBigSyncRpcClient {
                        target_part_store: Arc::clone(&self.big_sync_store),
                    }),
                    parts,
                )
                .await?;
            self.connections.lock().await.insert(remote.peer_id(), conn);
            Ok(())
        }

        async fn disconnect_from(&self, remote: &SyncRepoNode) -> Res<()> {
            if let Some(conn) = self.connections.lock().await.remove(&remote.peer_id()) {
                conn.stop().await?;
            }
            self.big_sync_worker.remove_peer(remote.peer_id()).await?;
            remote.big_sync_worker.remove_peer(self.peer_id()).await?;
            Ok(())
        }

        async fn upsert_payload(&self, obj_id: ObjId, payload: serde_json::Value) -> Res<()> {
            let handle = {
                let mut docs = self.docs.lock().await;
                if let Some(handle) = docs.get(&obj_id) {
                    Arc::clone(handle)
                } else {
                    let handle = match self.repo.get_doc(&obj_id).await? {
                        Some(handle) => Arc::new(handle),
                        None => {
                            let mut doc = automerge::Automerge::new();
                            doc.transact(|tx| {
                                autosurgeon::reconcile(tx, ThroughJson(payload.clone()))
                                    .expect("failed seeding big repo stress doc");
                                eyre::Ok(())
                            })
                            .expect("failed seeding big repo stress doc");
                            match self.repo.put_doc(obj_id, doc).await {
                                Ok(handle) => Arc::new(handle),
                                Err(runtime::PutDocError::IdOccpuied { .. }) => Arc::new(
                                    self.repo
                                        .get_doc(&obj_id)
                                        .await?
                                        .expect("doc should exist after put_doc occupied"),
                                ),
                                Err(err) => return Err(err.into()),
                            }
                        }
                    };
                    docs.insert(obj_id, Arc::clone(&handle));
                    handle
                }
            };

            handle
                .with_document(|doc| {
                    doc.transact(|tx| {
                        autosurgeon::reconcile(tx, ThroughJson(payload.clone()))
                            .expect("failed updating big repo stress doc");
                        eyre::Ok(())
                    })
                    .expect("failed updating big repo stress doc");
                })
                .await?;
            self.repo
                .big_sync_store
                .add_obj_to_parts(obj_id, stress_support::test_parts())
                .await?;
            Ok(())
        }

        async fn snapshot_docs(&self, all_docs: &[ObjId]) -> Res<BigRepoStressObservation> {
            let worker = self.big_sync_worker.snapshot().await?;
            let mut sync_store = BTreeMap::new();
            let mut memberships = BTreeMap::new();
            for obj_id in all_docs {
                let heads = self.repo.big_sync_store.obj_payload(*obj_id).await?;
                let obj_parts = self.repo.big_sync_store.obj_parts(*obj_id).await?;
                sync_store.insert(*obj_id, heads);
                memberships.insert(*obj_id, obj_parts);
            }
            let connected_peers = self.connections.lock().await.keys().copied().collect();
            Ok(BigRepoStressObservation {
                connected_peers,
                worker,
                sync_store,
                parts: memberships,
            })
        }

        #[tracing::instrument(skip(self))]
        async fn shutdown(mut self) -> Res<()> {
            tracing::info!(
                repo_peer_id = %self.repo.local_peer_id(),
                "shutting down sync repo node"
            );
            self.endpoint.close().await;
            if let Some(stop) = self.repo_rpc_stop.take() {
                stop.stop().await?;
            }
            self.stop_token.stop().await?;
            self.big_sync_stop.stop().await?;
            drop(self.router);
            Ok(())
        }
    }

    #[derive(Clone, Debug)]
    struct BigRepoStressObservation {
        connected_peers: BTreeSet<PeerId>,
        worker: big_sync::WorkerSnapshot,
        sync_store: BTreeMap<ObjId, Option<serde_json::Value>>,
        parts: BTreeMap<ObjId, Vec<PartId>>,
    }

    impl PartialEq for BigRepoStressObservation {
        fn eq(&self, other: &Self) -> bool {
            // self.connected_peers == other.connected_peers
            //     && self.worker == other.worker
            self.sync_store == other.sync_store && self.parts == other.parts
        }
    }

    #[derive(Default, Clone)]
    struct BigRepoStressFixture {
        all_docs: Arc<tokio::sync::Mutex<BTreeSet<ObjId>>>,
    }

    impl BigRepoStressFixture {
        async fn track_doc(&self, obj_id: ObjId) {
            self.all_docs.lock().await.insert(obj_id);
        }

        async fn tracked_doc_ids(&self) -> Vec<ObjId> {
            self.all_docs.lock().await.iter().copied().collect()
        }
    }

    #[async_trait::async_trait]
    impl StressFixture for BigRepoStressFixture {
        type World = ();
        type Node = SyncRepoNode;
        type Observation = BigRepoStressObservation;

        fn label(&self) -> &'static str {
            "big_repo"
        }

        fn make_doc_content(
            &self,
            phase: &str,
            step: usize,
            node_idx: usize,
            obj_id: &ObjId,
            nonce: u64,
            _written_at: u64,
            _writer_id: PeerId,
        ) -> serde_json::Value {
            serde_json::json!({
                "phase": phase,
                "step": step,
                "node": node_idx,
                "obj": format!("{obj_id:?}"),
                "nonce": nonce,
            })
        }

        async fn boot_node(&self, _world: Arc<Self::World>, peer_seed: u8) -> Res<Self::Node> {
            let path = tempfile::tempdir()?.keep();
            SyncRepoNode::boot(path, peer_seed, true).await
        }

        async fn stop_node(&self, node: Self::Node) -> Res<()> {
            node.shutdown().await
        }

        async fn connect_pair(&self, left: &Self::Node, right: &Self::Node) -> Res<()> {
            if left.peer_id() <= right.peer_id() {
                left.connect_to(right).await
            } else {
                right.connect_to(left).await
            }
        }

        async fn disconnect_pair(&self, left: &Self::Node, right: &Self::Node) -> Res<()> {
            if left.peer_id() <= right.peer_id() {
                left.disconnect_from(right).await
            } else {
                right.disconnect_from(left).await
            }
        }

        async fn seed_new_obj(
            &self,
            node: &Self::Node,
            _nodes: &[Option<Self::Node>],
            obj: ObjId,
            payload: serde_json::Value,
        ) -> Res<()> {
            self.track_doc(obj).await;
            node.upsert_payload(obj, payload).await
        }

        async fn seed_obj(
            &self,
            node: &Self::Node,
            obj: ObjId,
            payload: serde_json::Value,
        ) -> Res<()> {
            self.track_doc(obj).await;
            node.upsert_payload(obj, payload).await
        }

        async fn observed_state(&self, node: &Self::Node) -> Res<Self::Observation> {
            let all_docs = self.tracked_doc_ids().await;
            node.snapshot_docs(&all_docs).await
        }

        fn peer_id(&self, node: &Self::Node) -> PeerId {
            node.peer_id()
        }

        async fn assert_cluster_alignment(&self, nodes: &[&Self::Node]) -> Res<()> {
            let peer_ids: Vec<PeerId> = nodes.iter().map(|node| node.peer_id()).collect();
            let part_ids = stress_support::test_parts();
            let deadline = std::time::Instant::now() + Duration::from_secs(45);
            let full_sync_timeout = Duration::from_secs(20);
            let mut last_snapshots: Option<Vec<(PeerId, BigRepoStressObservation)>> = None;
            let mut stable_rounds = 0usize;

            for node in nodes {
                let node_peer_id = node.peer_id();
                let peers = peer_ids
                    .iter()
                    .copied()
                    .filter(|peer_id| *peer_id != node_peer_id)
                    .collect::<Vec<_>>();
                let parts = part_ids.clone();
                let wait = node
                    .big_sync_worker
                    .wait_for_full_sync(peers.iter().copied(), parts.iter().copied());
                if tokio::time::timeout(full_sync_timeout, wait).await.is_err() {
                    let worker = node.big_sync_worker.snapshot().await?;
                    let observed = self.observed_state(node).await?;
                    let mut out = String::new();
                    let _ = writeln!(
                        out,
                        "timed out waiting for full sync on peer {node_peer_id:?} after {full_sync_timeout:?}"
                    );
                    let _ = writeln!(out, "requested peers={peers:?} parts={parts:?}");
                    let _ = writeln!(out, "worker snapshot={worker:#?}");
                    let _ = writeln!(out, "observed state={observed:#?}");
                    eyre::bail!("{out}");
                }
            }

            loop {
                let mut snapshots = Vec::with_capacity(nodes.len());
                for node in nodes {
                    snapshots.push((node.peer_id(), self.observed_state(node).await?));
                }

                let aligned = snapshots.windows(2).all(|pair| pair[0].1 == pair[1].1);
                if aligned
                    && last_snapshots
                        .as_ref()
                        .is_some_and(|prev| prev == &snapshots)
                {
                    stable_rounds += 1;
                    if stable_rounds >= 5 {
                        return Ok(());
                    }
                } else {
                    stable_rounds = 0;
                }
                last_snapshots = Some(snapshots.clone());

                if std::time::Instant::now() >= deadline {
                    let mut out = String::new();
                    let _ = writeln!(
                        out,
                        "timed out waiting for big repo cluster alignment; last snapshots:"
                    );
                    if let Some((baseline_peer, baseline)) = snapshots.first() {
                        for (peer_id, snapshot) in snapshots.iter().skip(1) {
                            let _ =
                                writeln!(out, "peer {peer_id:?} vs baseline {baseline_peer:?}:");
                            let _ = writeln!(
                                out,
                                "  baseline vs snapshot sync_store {}",
                                pretty_assertions::Comparison::new(
                                    &baseline.sync_store,
                                    &snapshot.sync_store
                                )
                            );
                            let _ = writeln!(
                                out,
                                "  baseline vs snapshot parts {}",
                                pretty_assertions::Comparison::new(
                                    &baseline.parts,
                                    &snapshot.parts
                                )
                            );
                            let differing_sync_store = baseline
                                .sync_store
                                .iter()
                                .filter_map(|(obj_id, left_payload)| {
                                    let right_payload = snapshot.sync_store.get(obj_id)?;
                                    if left_payload == right_payload {
                                        None
                                    } else {
                                        Some((*obj_id, left_payload, right_payload))
                                    }
                                })
                                .take(12)
                                .collect::<Vec<_>>();
                            let differing_parts = baseline
                                .parts
                                .iter()
                                .filter_map(|(obj_id, left_parts)| {
                                    let right_parts = snapshot.parts.get(obj_id)?;
                                    if left_parts == right_parts {
                                        None
                                    } else {
                                        Some((*obj_id, left_parts, right_parts))
                                    }
                                })
                                .take(12)
                                .collect::<Vec<_>>();
                            let _ = writeln!(
                                out,
                                "  differing sync_store entries={differing_sync_store:?}"
                            );
                            let _ = writeln!(out, "  differing parts entries={differing_parts:?}");
                            let missing_sync_store = baseline
                                .sync_store
                                .keys()
                                .filter(|obj_id| !snapshot.sync_store.contains_key(obj_id))
                                .take(12)
                                .collect::<Vec<_>>();

                            let extra_sync_store = snapshot
                                .sync_store
                                .keys()
                                .filter(|obj_id| !baseline.sync_store.contains_key(obj_id))
                                .take(12)
                                .collect::<Vec<_>>();

                            let _ =
                                writeln!(out, "  missing sync_store keys={missing_sync_store:?}");
                            let _ = writeln!(out, "  extra sync_store keys={extra_sync_store:?}");

                            let missing_parts = baseline
                                .parts
                                .keys()
                                .filter(|obj_id| !snapshot.parts.contains_key(obj_id))
                                .take(12)
                                .collect::<Vec<_>>();

                            let extra_parts = snapshot
                                .parts
                                .keys()
                                .filter(|obj_id| !baseline.parts.contains_key(obj_id))
                                .take(12)
                                .collect::<Vec<_>>();

                            let _ = writeln!(out, "  missing parts={missing_parts:?}");
                            let _ = writeln!(out, "  extra parts={extra_parts:?}");

                            writeln!(
                                out,
                                "sync_store eq={}",
                                baseline.sync_store == snapshot.sync_store
                            )?;
                            writeln!(
                                out,
                                "sync_store eq={}",
                                baseline.sync_store == snapshot.sync_store
                            )?;
                            writeln!(out, "parts eq={}", baseline.parts == snapshot.parts)?;
                            let left = format!("{:#?}", baseline.sync_store);
                            let right = format!("{:#?}", snapshot.sync_store);
                            writeln!(out, "sync_store debug_eq={}", left == right)?;
                            writeln!(out, "snapshot eq={}", baseline == snapshot)?;
                            let _ = writeln!(
                                out,
                                "  field equality: connected_peers={} worker={} sync_store={} parts={}",
                                baseline.connected_peers == snapshot.connected_peers,
                                baseline.worker == snapshot.worker,
                                baseline.sync_store == snapshot.sync_store,
                                baseline.parts == snapshot.parts,
                            );
                        }
                    }
                    for node in nodes {
                        let worker = node.big_sync_worker.snapshot().await?;
                        let _ = writeln!(
                            out,
                            "worker peer={:?} task_counts={:?} active_machine_tasks={} active_sync_tasks={} zombie_tasks={} full_sync_waiters={:?}",
                            node.peer_id(),
                            worker.task_counts,
                            worker.active_machine_tasks,
                            worker.active_sync_tasks,
                            worker.zombie_tasks,
                            worker.full_sync_waiters,
                        );
                    }
                    eyre::bail!("{out}");
                }

                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }

    #[tracing::instrument(skip_all, fields(item_count, payload_len, ?local_mutation, ?remote_mutation))]
    async fn run_sync_case(
        item_count: usize,
        payload_len: usize,
        local_mutation: Option<SyncMutation>,
        remote_mutation: Option<SyncMutation>,
        exit_after_put: bool,
    ) -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        tracing::info!("starting sync case");
        let temp_root = tempdir()?;
        let server_path = temp_root.path().join("server");
        let client_path = temp_root.path().join("client");

        let mut expected_doc = make_sync_doc_value("base", item_count, payload_len);
        let mut client_expected_doc = expected_doc.clone();
        let mut server_expected_doc = expected_doc.clone();
        let mut base_doc = automerge::Automerge::new();
        write_sync_doc_value(&mut base_doc, &expected_doc);

        tracing::info!("booting server and client repos");
        let server = SyncRepoNode::boot(server_path, 51, true).await?;
        let client = SyncRepoNode::boot(client_path, 61, false).await?;
        let doc_id = random_doc_id();

        tracing::info!(%doc_id, "seeding initial docs");
        let server_doc = server.repo.put_doc(doc_id, base_doc.clone()).await?;
        let client_doc = client.repo.put_doc(doc_id, base_doc).await?;

        if exit_after_put {
            tracing::info!("exiting sync case immediately after put_doc seeding");
            server.shutdown().await?;
            client.shutdown().await?;
            return Ok(());
        }

        set_doc_actor(&server_doc, automerge::ActorId::from([51_u8; 16])).await?;
        set_doc_actor(&client_doc, automerge::ActorId::from([61_u8; 16])).await?;

        if let Some(mutation) = local_mutation {
            tracing::info!(?mutation, "applying local mutation");
            client_doc
                .with_document(|doc| {
                    apply_sync_mutation_in_place(doc, mutation, payload_len);
                })
                .await?;
            apply_sync_mutation(&mut expected_doc, mutation, payload_len);
            apply_sync_mutation(&mut client_expected_doc, mutation, payload_len);
        }
        if let Some(mutation) = remote_mutation {
            tracing::info!(?mutation, "applying remote mutation");
            server_doc
                .with_document(|doc| {
                    apply_sync_mutation_in_place(doc, mutation, payload_len);
                })
                .await?;
            apply_sync_mutation(&mut expected_doc, mutation, payload_len);
            apply_sync_mutation(&mut server_expected_doc, mutation, payload_len);
        }

        tracing::info!("connecting client to server");
        let client_conn = client
            .repo
            .open_connection_iroh(
                client.endpoint.clone(),
                server.endpoint.addr(),
                server.peer_id(),
                None,
            )
            .await?;
        server.wait_for_accepts(1).await;

        if local_mutation.is_some() && remote_mutation.is_some() {
            let server_conn = server.accepted_connection().await;
            tracing::info!(
                client_peer_id = %client_conn.peer_id(),
                server_peer_id = %server_conn.peer_id(),
                "running concurrent sync_doc_with_peer"
            );
            let (client_result, server_result) = tokio::join!(
                timeout(
                    SYNC_CASE_TIMEOUT,
                    client_conn.sync_with_peer(doc_id, Some(SYNC_PROPAGATION_TIMEOUT),),
                ),
                timeout(
                    SYNC_CASE_TIMEOUT,
                    server_conn.sync_with_peer(doc_id, Some(SYNC_PROPAGATION_TIMEOUT),),
                ),
            );
            let client_outcome =
                client_result.expect("timed out waiting for sync_doc_with_peer")?;
            let server_outcome =
                server_result.expect("timed out waiting for reverse sync_doc_with_peer")?;
            assert_eq!(client_outcome, SyncDocOutcome::Success);
            assert_eq!(server_outcome, SyncDocOutcome::Success);

            drop(client_doc);
            drop(server_doc);

            let client_doc = client
                .repo
                .get_doc(&doc_id)
                .await?
                .expect("client doc should exist");
            let server_doc = server
                .repo
                .get_doc(&doc_id)
                .await?
                .expect("server doc should exist");
            let client_state = read_json_doc(&client_doc).await;
            let server_state = read_json_doc(&server_doc).await;
            tracing::info!(
                client_state = ?client_state,
                server_state = ?server_state,
                "post-sync diverged-head state"
            );
            tracing::info!(
                client_expected_notes = ?sync_note_snapshot(&client_expected_doc, &[5, 17]),
                server_expected_notes = ?sync_note_snapshot(&server_expected_doc, &[5, 17]),
                expected_notes = ?sync_note_snapshot(&expected_doc, &[5, 17]),
                client_state_notes = ?sync_note_snapshot(&client_state, &[5, 17]),
                server_state_notes = ?sync_note_snapshot(&server_state, &[5, 17]),
                "post-sync diverged-head note snapshot"
            );
            wait_for_json_doc(&client_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
            wait_for_json_doc(&server_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
        } else {
            tracing::info!(
                peer_id = %client_conn.peer_id(),
                "running sync_doc_with_peer"
            );
            let outcome = timeout(
                SYNC_CASE_TIMEOUT,
                client_conn.sync_with_peer(doc_id, Some(SYNC_PROPAGATION_TIMEOUT)),
            )
            .await
            .expect("timed out waiting for sync_doc_with_peer")?;
            assert_eq!(outcome, SyncDocOutcome::Success);

            tracing::info!("verifying doc convergence");
            wait_for_json_doc(&client_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
            wait_for_json_doc(&server_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
        }

        tracing::info!("closing client connection and shutting down repos");
        client_conn.stop().await?;
        server.shutdown().await?;
        client.shutdown().await?;
        Ok(())
    }

    #[tracing::instrument(
        skip_all,
        fields(item_count, payload_len, ?first_remote_mutation, ?second_local_mutation)
    )]
    async fn run_restart_reconnect_case(
        item_count: usize,
        payload_len: usize,
        first_remote_mutation: Option<SyncMutation>,
        second_local_mutation: Option<SyncMutation>,
    ) -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        tracing::info!("starting reconnect case");
        let temp_root = tempdir()?;
        let server_path = temp_root.path().join("server");
        let client_path = temp_root.path().join("client");

        let mut expected_doc = make_sync_doc_value("base", item_count, payload_len);
        let mut base_doc = automerge::Automerge::new();
        write_sync_doc_value(&mut base_doc, &expected_doc);
        let server = SyncRepoNode::boot(server_path.clone(), 71, true).await?;
        let client = SyncRepoNode::boot(client_path, 81, false).await?;
        let doc_id = random_doc_id();
        let server_doc = server.repo.put_doc(doc_id, base_doc.clone()).await?;
        let client_doc = client.repo.put_doc(doc_id, base_doc).await?;
        set_doc_actor(&server_doc, automerge::ActorId::from([71_u8; 16])).await?;
        set_doc_actor(&client_doc, automerge::ActorId::from([81_u8; 16])).await?;

        if let Some(mutation) = first_remote_mutation {
            tracing::info!(?mutation, "applying first remote mutation");
            server_doc
                .with_document(|doc| {
                    apply_sync_mutation_in_place(doc, mutation, payload_len);
                })
                .await?;
            apply_sync_mutation(&mut expected_doc, mutation, payload_len);
        }

        tracing::info!("connecting client to server");
        let client_conn = client
            .repo
            .open_connection_iroh(
                client.endpoint.clone(),
                server.endpoint.addr(),
                server.peer_id(),
                None,
            )
            .await?;
        server.wait_for_accepts(1).await;

        tracing::info!("running initial sync before server shutdown");
        let outcome = timeout(
            SYNC_CASE_TIMEOUT,
            client_conn.sync_with_peer(doc_id, Some(SYNC_PROPAGATION_TIMEOUT)),
        )
        .await
        .expect("timed out waiting for initial sync_doc_with_peer")?;
        assert_eq!(outcome, SyncDocOutcome::Success);
        wait_for_json_doc(&client_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
        wait_for_json_doc(&server_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;

        tracing::info!("shutting down server while connection is still live");
        server.shutdown().await?;
        client_conn.stop().await?;

        tracing::info!("rebooting server from the same disk path");
        let server = SyncRepoNode::boot(server_path, 71, true).await?;
        let server_doc = server
            .repo
            .get_doc(&doc_id)
            .await?
            .expect("server doc should persist across restart");
        wait_for_json_doc(&server_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;

        if let Some(mutation) = second_local_mutation {
            tracing::info!(?mutation, "applying second local mutation after restart");
            client_doc
                .with_document(|doc| {
                    apply_sync_mutation_in_place(doc, mutation, payload_len);
                })
                .await?;
            apply_sync_mutation(&mut expected_doc, mutation, payload_len);
        }

        tracing::info!("reconnecting after server restart");
        let client_conn = client
            .repo
            .open_connection_iroh(
                client.endpoint.clone(),
                server.endpoint.addr(),
                server.peer_id(),
                None,
            )
            .await?;
        server.wait_for_accepts(1).await;

        tracing::info!("running sync after restart");
        let outcome = timeout(
            SYNC_CASE_TIMEOUT,
            client_conn.sync_with_peer(doc_id, Some(SYNC_PROPAGATION_TIMEOUT)),
        )
        .await
        .expect("timed out waiting for reconnect sync_doc_with_peer")?;
        assert_eq!(outcome, SyncDocOutcome::Success);
        wait_for_json_doc(&client_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
        wait_for_json_doc(&server_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;

        client_conn.stop().await?;
        server.shutdown().await?;
        client.shutdown().await?;
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(item_count, payload_len, ?remote_mutation))]
    async fn run_remote_change_listener_without_live_handle_case(
        item_count: usize,
        payload_len: usize,
        remote_mutation: SyncMutation,
    ) -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        tracing::info!("starting remote listener without live handle case");
        let temp_root = tempdir()?;
        let server_path = temp_root.path().join("server");
        let client_path = temp_root.path().join("client");

        let mut expected_doc = make_sync_doc_value("base", item_count, payload_len);
        let mut base_doc = automerge::Automerge::new();
        write_sync_doc_value(&mut base_doc, &expected_doc);

        let server = SyncRepoNode::boot(server_path, 91, true).await?;
        let client = SyncRepoNode::boot(client_path, 92, false).await?;
        let doc_id = random_doc_id();

        let server_doc = server.repo.put_doc(doc_id, base_doc.clone()).await?;
        let client_doc = client.repo.put_doc(doc_id, base_doc).await?;
        set_doc_actor(&server_doc, automerge::ActorId::from([91_u8; 16])).await?;
        set_doc_actor(&client_doc, automerge::ActorId::from([92_u8; 16])).await?;

        let (_change_registration, mut change_rx) = server
            .repo
            .subscribe_change_listener(BigRepoChangeFilter {
                doc_id: Some(BigRepoDocIdFilter::new(doc_id)),
                origin: Some(BigRepoOriginFilter::Remote),
                path: Vec::new(),
            })
            .await?;
        let (_head_registration, mut head_rx) = server
            .repo
            .change_manager
            .subscribe_head_listener(super::changes::HeadFilter {
                doc_id: Some(super::changes::DocIdFilter::new(doc_id)),
            })
            .await?;

        drop(server_doc);
        tracing::info!("dropped the server doc handle before remote sync");

        client_doc
            .with_document(|doc| {
                apply_sync_mutation_in_place(doc, remote_mutation, payload_len);
            })
            .await?;
        apply_sync_mutation(&mut expected_doc, remote_mutation, payload_len);

        let client_conn = connect_sync_pair(&client, &server).await?;
        server.wait_for_accepts(1).await;

        let outcome = timeout(
            SYNC_CASE_TIMEOUT,
            client_conn.sync_with_peer(doc_id, Some(SYNC_PROPAGATION_TIMEOUT)),
        )
        .await
        .expect("timed out waiting for remote sync_doc_with_peer")?;
        assert_eq!(outcome, SyncDocOutcome::Success);

        let change_batch = recv_change_batch(&mut change_rx).await;
        assert!(matches!(
            change_batch.as_slice(),
            [BigRepoChangeNotification::DocChanged {
                doc_id: seen_doc_id,
                origin: BigRepoChangeOrigin::Remote { .. },
                ..
            }] if *seen_doc_id == doc_id
        ));

        let head_batch: Vec<super::changes::BigRepoHeadNotification> =
            recv_head_batch(&mut head_rx).await;
        assert!(matches!(
            head_batch.as_slice(),
            [super::changes::BigRepoHeadNotification::DocHeadsChanged {
                doc_id: seen_doc_id,
                origin: BigRepoChangeOrigin::Remote { .. },
                ..
            }] if *seen_doc_id == doc_id
        ));

        let reopened = server
            .repo
            .get_doc(&doc_id)
            .await?
            .expect("server doc should remain persisted");
        wait_for_json_doc(&reopened, &expected_doc, SYNC_CASE_TIMEOUT).await;

        client_conn.stop().await?;
        server.shutdown().await?;
        client.shutdown().await?;
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(doc_id = %handle.document_id()))]
    async fn set_doc_actor(handle: &BigDocHandle, actor: automerge::ActorId) -> Res<()> {
        handle
            .with_document(|doc| {
                doc.set_actor(actor);
            })
            .await?;
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(doc_id = %doc_id, ?mutation))]
    async fn apply_local_sync_mutation_and_assert_notifications(
        repo: &Arc<BigRepo>,
        conn: &BigRepoConnection,
        handle: &BigDocHandle,
        doc_id: DocumentId,
        mutation: SyncMutation,
        payload_len: usize,
    ) -> Res<()> {
        let (_change_registration, mut change_rx) = repo
            .subscribe_change_listener(BigRepoChangeFilter {
                doc_id: Some(BigRepoDocIdFilter::new(doc_id)),
                origin: Some(BigRepoOriginFilter::Local),
                path: Vec::new(),
            })
            .await?;
        let (_head_registration, mut head_rx) = repo
            .change_manager
            .subscribe_head_listener(super::changes::HeadFilter {
                doc_id: Some(super::changes::DocIdFilter::new(doc_id)),
            })
            .await?;

        handle
            .with_document(|doc| {
                apply_sync_mutation_in_place(doc, mutation, payload_len);
            })
            .await?;

        let change_batch = recv_change_batch(&mut change_rx).await;
        assert!(matches!(
            change_batch.as_slice(),
            [BigRepoChangeNotification::DocChanged {
                doc_id: seen_doc_id,
                origin: BigRepoChangeOrigin::Local,
                ..
            }] if *seen_doc_id == doc_id
        ));

        let head_batch: Vec<super::changes::BigRepoHeadNotification> =
            recv_head_batch(&mut head_rx).await;
        assert!(matches!(
            head_batch.as_slice(),
            [super::changes::BigRepoHeadNotification::DocHeadsChanged {
                doc_id: seen_doc_id,
                origin: BigRepoChangeOrigin::Local,
                ..
            }] if *seen_doc_id == doc_id
        ));

        let outcome = timeout(
            SYNC_CASE_TIMEOUT,
            conn.sync_with_peer(doc_id, Some(SYNC_PROPAGATION_TIMEOUT)),
        )
        .await
        .expect("timed out waiting for local sync_doc_with_peer")?;
        assert_eq!(outcome, SyncDocOutcome::Success);
        Ok(())
    }

    async fn connect_sync_pair(
        client: &SyncRepoNode,
        server: &SyncRepoNode,
    ) -> Res<BigRepoConnection> {
        client
            .repo
            .open_connection_iroh(
                client.endpoint.clone(),
                server.endpoint.addr(),
                server.peer_id(),
                None,
            )
            .await
    }

    #[tracing::instrument(
        skip_all,
        fields(?local_mutation, ?remote_mutation, ?expected_deets, expect_client_doc)
    )]
    async fn run_sync_backend_case(
        local_mutation: Option<SyncMutation>,
        remote_mutation: Option<SyncMutation>,
        expected_deets: SyncCompletionDeets,
        expect_client_doc: bool,
        sync_part_hints: Vec<PartId>,
        remote_payload_missing: bool,
    ) -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        tracing::info!("starting sync backend case");
        let temp_root = tempdir()?;
        let server_path = temp_root.path().join("server");
        let client_path = temp_root.path().join("client");

        let mut expected_doc = make_sync_doc_value("base", SYNC_DOC_ITEMS, SYNC_DOC_PAYLOAD_LEN);
        let mut base_doc = automerge::Automerge::new();
        write_sync_doc_value(&mut base_doc, &expected_doc);

        let server = SyncRepoNode::boot(server_path, 131, true).await?;
        let client = SyncRepoNode::boot(client_path, 132, false).await?;
        let doc_id = random_doc_id();
        let server_doc = server.repo.put_doc(doc_id, base_doc.clone()).await?;
        let client_doc = if expect_client_doc {
            Some(client.repo.put_doc(doc_id, base_doc).await?)
        } else {
            None
        };
        set_doc_actor(&server_doc, automerge::ActorId::from([131_u8; 16])).await?;
        if let Some(client_doc) = &client_doc {
            set_doc_actor(client_doc, automerge::ActorId::from([132_u8; 16])).await?;
        }

        if let Some(mutation) = local_mutation {
            tracing::info!(?mutation, "applying local mutation");
            client_doc
                .as_ref()
                .expect("client doc should exist for local mutation")
                .with_document(|doc| {
                    apply_sync_mutation_in_place(doc, mutation, SYNC_DOC_PAYLOAD_LEN);
                })
                .await?;
            apply_sync_mutation(&mut expected_doc, mutation, SYNC_DOC_PAYLOAD_LEN);
        }
        if let Some(mutation) = remote_mutation {
            tracing::info!(?mutation, "applying remote mutation");
            server_doc
                .with_document(|doc| {
                    apply_sync_mutation_in_place(doc, mutation, SYNC_DOC_PAYLOAD_LEN);
                })
                .await?;
            apply_sync_mutation(&mut expected_doc, mutation, SYNC_DOC_PAYLOAD_LEN);
        }

        let client_conn = connect_sync_pair(&client, &server).await?;
        server.wait_for_accepts(1).await;

        let backend = client.repo.sync_backend();
        let local_payload = client.big_sync_store.obj_payload(doc_id).await?;
        let remote_payload = server.big_sync_store.obj_payload(doc_id).await?;
        let scenario = SyncBackendScenario {
            name: "big_repo_sync_backend_case",
            peer_id: client_conn.peer_id(),
            obj_id: doc_id,
            initial_payload: local_payload.clone(),
            initial_parts: sync_part_hints.clone(),
            remote_payload: if remote_payload_missing {
                None
            } else {
                remote_payload.clone()
            },
            expected_outcome: SyncBackendOutcome::Completion(expected_deets.clone()),
            expected_payload: match &expected_deets {
                SyncCompletionDeets::Noop => local_payload.clone(),
                SyncCompletionDeets::ChangedObject | SyncCompletionDeets::AddedMember => {
                    remote_payload.clone()
                }
                SyncCompletionDeets::RemovedMember => {
                    unreachable!("big repo sync backend should not report RemovedMember")
                }
            },
            expected_parts: sync_part_hints.clone(),
        };
        let harness = BigRepoSyncBackendContractHarness {
            backend,
            store: Arc::clone(&client.big_sync_store),
        };
        contract::assert_sync_backend_case(&harness, &scenario).await?;

        if let Some(client_doc) = &client_doc {
            wait_for_json_doc(client_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
        } else {
            let imported_client_doc = client
                .repo
                .get_doc(&doc_id)
                .await?
                .expect("client doc should be imported");
            wait_for_json_doc(&imported_client_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
        }
        wait_for_json_doc(&server_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;

        client_conn.stop().await?;
        server.shutdown().await?;
        client.shutdown().await?;
        Ok(())
    }

    async fn run_sync_backend_added_member_case(remote_mutation: Option<SyncMutation>) -> Res<()> {
        run_sync_backend_case(
            None,
            remote_mutation,
            SyncCompletionDeets::AddedMember,
            false,
            sync_test_parts(),
            false,
        )
        .await
    }

    async fn run_sync_backend_remote_payload_missing_noop_case() -> Res<()> {
        run_sync_backend_case(
            None,
            None,
            SyncCompletionDeets::Noop,
            true,
            sync_test_parts(),
            true,
        )
        .await
    }

    async fn run_sync_backend_remote_payload_missing_changed_case(
        sync_part_hints: Vec<PartId>,
    ) -> Res<()> {
        run_sync_backend_case(
            None,
            Some(SyncMutation {
                item_idx: 29,
                note_key: "remote_missing",
                side_label: "remote",
            }),
            SyncCompletionDeets::ChangedObject,
            true,
            sync_part_hints,
            true,
        )
        .await
    }

    async fn run_sync_backend_put_doc_conflict_case() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        tracing::info!("starting sync backend put-doc-conflict case");
        let temp_root = tempdir()?;
        let server_path = temp_root.path().join("server");
        let client_path = temp_root.path().join("client");

        let mut expected_doc = make_sync_doc_value("base", SYNC_DOC_ITEMS, SYNC_DOC_PAYLOAD_LEN);
        let mut base_doc = automerge::Automerge::new();
        write_sync_doc_value(&mut base_doc, &expected_doc);

        let server = SyncRepoNode::boot(server_path, 131, true).await?;
        let client = SyncRepoNode::boot(client_path, 132, false).await?;
        let doc_id = random_doc_id();
        let server_doc = server.repo.put_doc(doc_id, base_doc.clone()).await?;
        let client_doc = client.repo.put_doc(doc_id, base_doc).await?;
        set_doc_actor(&server_doc, automerge::ActorId::from([131_u8; 16])).await?;
        set_doc_actor(&client_doc, automerge::ActorId::from([132_u8; 16])).await?;

        let remote_mutation = SyncMutation {
            item_idx: 21,
            note_key: "remote_conflict",
            side_label: "remote",
        };
        server_doc
            .with_document(|doc| {
                apply_sync_mutation_in_place(doc, remote_mutation, SYNC_DOC_PAYLOAD_LEN);
            })
            .await?;
        apply_sync_mutation(&mut expected_doc, remote_mutation, SYNC_DOC_PAYLOAD_LEN);

        let client_conn = connect_sync_pair(&client, &server).await?;
        server.wait_for_accepts(1).await;

        client
            .big_sync_store
            .remove_obj_from_part(doc_id, sync_test_part())
            .await?;

        let backend = client.repo.sync_backend();
        let remote_payload = server.big_sync_store.obj_payload(doc_id).await?;
        let scenario = SyncBackendScenario {
            name: "put_doc_conflict_retries_sync_and_materializes_heads",
            peer_id: client_conn.peer_id(),
            obj_id: doc_id,
            initial_payload: None,
            initial_parts: sync_test_parts(),
            remote_payload,
            expected_outcome: SyncBackendOutcome::Completion(SyncCompletionDeets::ChangedObject),
            expected_payload: server.big_sync_store.obj_payload(doc_id).await?,
            expected_parts: sync_test_parts(),
        };
        let harness = BigRepoSyncBackendContractHarness {
            backend,
            store: Arc::clone(&client.big_sync_store),
        };
        contract::assert_sync_backend_case(&harness, &scenario).await?;

        wait_for_json_doc(&client_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
        wait_for_json_doc(&server_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;

        client_conn.stop().await?;
        server.shutdown().await?;
        client.shutdown().await?;
        Ok(())
    }

    async fn wait_for_pair_full_sync(left: &SyncRepoNode, right: &SyncRepoNode) -> Res<()> {
        let left_wait = timeout(
            SYNC_CASE_TIMEOUT,
            left.big_sync_worker
                .wait_for_full_sync([right.peer_id()], stress_support::test_parts()),
        );
        let right_wait = timeout(
            SYNC_CASE_TIMEOUT,
            right
                .big_sync_worker
                .wait_for_full_sync([left.peer_id()], stress_support::test_parts()),
        );
        left_wait
            .await
            .expect("timed out waiting for left node full sync")?;
        right_wait
            .await
            .expect("timed out waiting for right node full sync")?;
        Ok(())
    }

    async fn assert_pair_sync_alignment(
        left: &SyncRepoNode,
        right: &SyncRepoNode,
        doc_id: ObjId,
    ) -> Res<()> {
        let left_heads = left.repo.doc_payload_heads(doc_id).await?;
        let right_heads = right.repo.doc_payload_heads(doc_id).await?;
        assert_eq!(
            left_heads, right_heads,
            "payload heads diverged for doc {doc_id:?}"
        );

        let left_parts = left.big_sync_store.obj_parts(doc_id).await?;
        let right_parts = right.big_sync_store.obj_parts(doc_id).await?;
        assert_eq!(
            left_parts, right_parts,
            "part membership diverged for doc {doc_id:?}"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn big_repo_sync_backend_returns_noop_when_heads_match() -> Res<()> {
        timeout(
            SYNC_CASE_TIMEOUT,
            run_sync_backend_case(
                None,
                None,
                SyncCompletionDeets::Noop,
                true,
                sync_test_parts(),
                false,
            ),
        )
        .await
        .expect("sync backend test timed out")?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn big_repo_sync_backend_applies_remote_update() -> Res<()> {
        timeout(
            SYNC_CASE_TIMEOUT,
            run_sync_backend_case(
                None,
                Some(SyncMutation {
                    item_idx: 17,
                    note_key: "remote_backend",
                    side_label: "remote",
                }),
                SyncCompletionDeets::ChangedObject,
                true,
                sync_test_parts(),
                false,
            ),
        )
        .await
        .expect("sync backend test timed out")?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn big_repo_sync_backend_applies_remote_update_with_empty_part_hints() -> Res<()> {
        timeout(
            SYNC_CASE_TIMEOUT,
            run_sync_backend_case(
                None,
                Some(SyncMutation {
                    item_idx: 18,
                    note_key: "remote_backend_empty",
                    side_label: "remote",
                }),
                SyncCompletionDeets::ChangedObject,
                true,
                vec![],
                false,
            ),
        )
        .await
        .expect("sync backend test timed out")?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn big_repo_sync_backend_applies_remote_update_with_multiple_part_hints() -> Res<()> {
        timeout(
            SYNC_CASE_TIMEOUT,
            run_sync_backend_case(
                None,
                Some(SyncMutation {
                    item_idx: 19,
                    note_key: "remote_backend_multi",
                    side_label: "remote",
                }),
                SyncCompletionDeets::ChangedObject,
                true,
                sync_test_parts_multi(),
                false,
            ),
        )
        .await
        .expect("sync backend test timed out")?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn big_repo_sync_backend_returns_noop_when_remote_payload_is_missing() -> Res<()> {
        timeout(
            SYNC_CASE_TIMEOUT,
            run_sync_backend_remote_payload_missing_noop_case(),
        )
        .await
        .expect("sync backend test timed out")?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn big_repo_sync_backend_applies_remote_update_when_remote_payload_is_missing() -> Res<()>
    {
        timeout(
            SYNC_CASE_TIMEOUT,
            run_sync_backend_remote_payload_missing_changed_case(sync_test_parts()),
        )
        .await
        .expect("sync backend test timed out")?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn big_repo_sync_backend_adds_missing_doc() -> Res<()> {
        timeout(
            SYNC_CASE_TIMEOUT,
            run_sync_backend_added_member_case(Some(SyncMutation {
                item_idx: 23,
                note_key: "added_member",
                side_label: "remote",
            })),
        )
        .await
        .expect("sync backend test timed out")?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn big_repo_sync_backend_recovers_from_put_doc_conflict() -> Res<()> {
        timeout(SYNC_CASE_TIMEOUT, run_sync_backend_put_doc_conflict_case())
            .await
            .expect("sync backend test timed out")?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn big_repo_payload_first_membership_late_reconnects_cleanly() -> Res<()> {
        timeout(SYNC_CASE_TIMEOUT, async {
            utils_rs::testing::setup_tracing_once();
            tracing::info!("starting payload-first membership-late reconnect regression");
            let temp_root = tempdir()?;
            let left_path = temp_root.path().join("left");
            let right_path = temp_root.path().join("right");
            let left = SyncRepoNode::boot(left_path, 141, true).await?;
            let right = SyncRepoNode::boot(right_path, 142, false).await?;
            let doc_id = random_doc_id();
            let expected_doc = make_sync_doc_value("payload-first-reconnect", 8, 48);
            let mut base_doc = automerge::Automerge::new();
            write_sync_doc_value(&mut base_doc, &expected_doc);

            let left_doc = left.repo.put_doc(doc_id, base_doc).await?;
            left.big_sync_store
                .add_obj_to_parts(doc_id, stress_support::test_parts())
                .await?;

            left.connect_to(&right).await?;
            wait_for_pair_full_sync(&left, &right).await?;

            wait_for_json_doc(&left_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
            let right_doc = right
                .repo
                .get_doc(&doc_id)
                .await?
                .expect("right doc should exist after initial sync");
            wait_for_json_doc(&right_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
            assert_pair_sync_alignment(&left, &right, doc_id).await?;

            left.disconnect_from(&right).await?;
            left.connect_to(&right).await?;
            wait_for_pair_full_sync(&left, &right).await?;

            wait_for_json_doc(&left_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
            let right_doc = right
                .repo
                .get_doc(&doc_id)
                .await?
                .expect("right doc should exist after reconnect");
            wait_for_json_doc(&right_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
            assert_pair_sync_alignment(&left, &right, doc_id).await?;

            left.disconnect_from(&right).await?;
            left.shutdown().await?;
            right.shutdown().await?;
            eyre::Ok(())
        })
        .await
        .expect("payload-first reconnect regression timed out")?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn big_repo_membership_first_payload_late_reconnects_cleanly() -> Res<()> {
        timeout(SYNC_CASE_TIMEOUT, async {
            utils_rs::testing::setup_tracing_once();
            tracing::info!("starting membership-first payload-late reconnect regression");
            let temp_root = tempdir()?;
            let left_path = temp_root.path().join("left");
            let right_path = temp_root.path().join("right");
            let left = SyncRepoNode::boot(left_path, 143, true).await?;
            let right = SyncRepoNode::boot(right_path, 144, false).await?;
            let doc_id = random_doc_id();
            let expected_doc = make_sync_doc_value("membership-first-reconnect", 8, 48);
            let mut base_doc = automerge::Automerge::new();
            write_sync_doc_value(&mut base_doc, &expected_doc);

            left.big_sync_store
                .add_obj_to_parts(doc_id, stress_support::test_parts())
                .await?;

            left.connect_to(&right).await?;

            let left_doc = left.repo.put_doc(doc_id, base_doc).await?;
            wait_for_pair_full_sync(&left, &right).await?;

            wait_for_json_doc(&left_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
            let right_doc = right
                .repo
                .get_doc(&doc_id)
                .await?
                .expect("right doc should exist after initial sync");
            wait_for_json_doc(&right_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
            assert_pair_sync_alignment(&left, &right, doc_id).await?;

            left.disconnect_from(&right).await?;
            left.connect_to(&right).await?;
            wait_for_pair_full_sync(&left, &right).await?;

            wait_for_json_doc(&left_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
            let right_doc = right
                .repo
                .get_doc(&doc_id)
                .await?
                .expect("right doc should exist after reconnect");
            wait_for_json_doc(&right_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
            assert_pair_sync_alignment(&left, &right, doc_id).await?;

            left.disconnect_from(&right).await?;
            left.shutdown().await?;
            right.shutdown().await?;
            eyre::Ok(())
        })
        .await
        .expect("membership-first reconnect regression timed out")?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sync_with_peer_uses_remote_changes_when_only_remote_diverged() -> Res<()> {
        timeout(
            SYNC_CASE_TIMEOUT,
            run_sync_case(
                SYNC_DOC_ITEMS,
                SYNC_DOC_PAYLOAD_LEN,
                None,
                Some(SyncMutation {
                    item_idx: 7,
                    note_key: "remote_note",
                    side_label: "remote",
                }),
                false,
            ),
        )
        .await
        .expect("sync test timed out")?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sync_with_peer_uses_local_changes_when_only_local_diverged() -> Res<()> {
        timeout(
            SYNC_CASE_TIMEOUT,
            run_sync_case(
                SYNC_DOC_ITEMS,
                SYNC_DOC_PAYLOAD_LEN,
                Some(SyncMutation {
                    item_idx: 11,
                    note_key: "local_note",
                    side_label: "local",
                }),
                None,
                false,
            ),
        )
        .await
        .expect("sync test timed out")?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sync_with_peer_both_diverged_loses_remote_change() -> Res<()> {
        timeout(
            SYNC_CASE_TIMEOUT,
            run_sync_case(
                SYNC_DOC_ITEMS,
                SYNC_DOC_PAYLOAD_LEN,
                Some(SyncMutation {
                    item_idx: 5,
                    note_key: "local_note",
                    side_label: "local",
                }),
                Some(SyncMutation {
                    item_idx: 17,
                    note_key: "remote_note",
                    side_label: "remote",
                }),
                false,
            ),
        )
        .await
        .expect("sync test timed out")?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn sync_with_peer_handles_large_fragmented_remote_docs() -> Res<()> {
        timeout(
            SYNC_CASE_TIMEOUT,
            run_sync_case(
                SYNC_LARGE_DOC_ITEMS,
                SYNC_LARGE_DOC_PAYLOAD_LEN,
                None,
                Some(SyncMutation {
                    item_idx: 777,
                    note_key: "remote_note",
                    side_label: "remote",
                }),
                true,
            ),
        )
        .await
        .expect("sync test timed out")?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sync_with_peer_survives_repo_restart_with_live_connection() -> Res<()> {
        timeout(
            SYNC_CASE_TIMEOUT * 2,
            run_restart_reconnect_case(
                SYNC_DOC_ITEMS,
                SYNC_DOC_PAYLOAD_LEN,
                Some(SyncMutation {
                    item_idx: 7,
                    note_key: "remote_note",
                    side_label: "remote",
                }),
                Some(SyncMutation {
                    item_idx: 3,
                    note_key: "local_after_restart",
                    side_label: "local",
                }),
            ),
        )
        .await
        .expect("sync test timed out")?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sync_with_peer_local_write_emits_notifications_while_connected() -> Res<()> {
        timeout(SYNC_CASE_TIMEOUT, async {
            let temp_root = tempdir()?;
            let server_path = temp_root.path().join("server");
            let client_path = temp_root.path().join("client");

            let mut expected_doc =
                make_sync_doc_value("base", SYNC_DOC_ITEMS, SYNC_DOC_PAYLOAD_LEN);
            let mut base_doc = automerge::Automerge::new();
            write_sync_doc_value(&mut base_doc, &expected_doc);

            let server = SyncRepoNode::boot(server_path, 101, true).await?;
            let client = SyncRepoNode::boot(client_path, 102, false).await?;
            let doc_id = random_doc_id();
            let server_doc = server.repo.put_doc(doc_id, base_doc.clone()).await?;
            let client_doc = client.repo.put_doc(doc_id, base_doc).await?;
            set_doc_actor(&server_doc, automerge::ActorId::from([101_u8; 16])).await?;
            set_doc_actor(&client_doc, automerge::ActorId::from([102_u8; 16])).await?;

            let client_conn = connect_sync_pair(&client, &server).await?;
            server.wait_for_accepts(1).await;

            apply_local_sync_mutation_and_assert_notifications(
                &client.repo,
                &client_conn,
                &client_doc,
                doc_id,
                SyncMutation {
                    item_idx: 4,
                    note_key: "local_connected",
                    side_label: "local",
                },
                SYNC_DOC_PAYLOAD_LEN,
            )
            .await?;
            apply_sync_mutation(
                &mut expected_doc,
                SyncMutation {
                    item_idx: 4,
                    note_key: "local_connected",
                    side_label: "local",
                },
                SYNC_DOC_PAYLOAD_LEN,
            );

            wait_for_json_doc(&client_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
            wait_for_json_doc(&server_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;

            client_conn.stop().await?;
            server.shutdown().await?;
            client.shutdown().await?;
            eyre::Ok(())
        })
        .await
        .expect("sync test timed out")?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sync_with_peer_remote_change_notifies_without_live_handle() -> Res<()> {
        timeout(
            SYNC_CASE_TIMEOUT,
            run_remote_change_listener_without_live_handle_case(
                SYNC_DOC_ITEMS,
                SYNC_DOC_PAYLOAD_LEN,
                SyncMutation {
                    item_idx: 13,
                    note_key: "remote_no_handle",
                    side_label: "remote",
                },
            ),
        )
        .await
        .expect("sync test timed out")?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sync_with_peer_remote_change_notifies_with_live_handle_and_listeners() -> Res<()> {
        timeout(SYNC_CASE_TIMEOUT, async {
            let temp_root = tempdir()?;
            let server_path = temp_root.path().join("server");
            let client_path = temp_root.path().join("client");

            let mut expected_doc =
                make_sync_doc_value("base", SYNC_DOC_ITEMS, SYNC_DOC_PAYLOAD_LEN);
            let mut base_doc = automerge::Automerge::new();
            write_sync_doc_value(&mut base_doc, &expected_doc);

            let server = SyncRepoNode::boot(server_path, 111, true).await?;
            let client = SyncRepoNode::boot(client_path, 112, false).await?;
            let doc_id = random_doc_id();
            let server_doc = server.repo.put_doc(doc_id, base_doc.clone()).await?;
            let client_doc = client.repo.put_doc(doc_id, base_doc).await?;
            set_doc_actor(&server_doc, automerge::ActorId::from([111_u8; 16])).await?;
            set_doc_actor(&client_doc, automerge::ActorId::from([112_u8; 16])).await?;

            let (_change_registration, mut change_rx) = server
                .repo
                .subscribe_change_listener(BigRepoChangeFilter {
                    doc_id: Some(BigRepoDocIdFilter::new(doc_id)),
                    origin: Some(BigRepoOriginFilter::Remote),
                    path: Vec::new(),
                })
                .await?;
            let (_head_registration, mut head_rx) = server
                .repo
                .change_manager
                .subscribe_head_listener(super::changes::HeadFilter {
                    doc_id: Some(super::changes::DocIdFilter::new(doc_id)),
                })
                .await?;

            let client_conn = connect_sync_pair(&client, &server).await?;
            server.wait_for_accepts(1).await;

            client_doc
                .with_document(|doc| {
                    apply_sync_mutation_in_place(
                        doc,
                        SyncMutation {
                            item_idx: 7,
                            note_key: "remote_with_handle",
                            side_label: "remote",
                        },
                        SYNC_DOC_PAYLOAD_LEN,
                    );
                })
                .await?;
            apply_sync_mutation(
                &mut expected_doc,
                SyncMutation {
                    item_idx: 7,
                    note_key: "remote_with_handle",
                    side_label: "remote",
                },
                SYNC_DOC_PAYLOAD_LEN,
            );

            let outcome = timeout(
                SYNC_CASE_TIMEOUT,
                client_conn.sync_with_peer(doc_id, Some(SYNC_PROPAGATION_TIMEOUT)),
            )
            .await
            .expect("timed out waiting for remote sync_doc_with_peer")?;
            assert_eq!(outcome, SyncDocOutcome::Success);

            let change_batch = recv_change_batch(&mut change_rx).await;
            assert!(matches!(
                change_batch.as_slice(),
                [BigRepoChangeNotification::DocChanged {
                    doc_id: seen_doc_id,
                    origin: BigRepoChangeOrigin::Remote { .. },
                    ..
                }] if *seen_doc_id == doc_id
            ));

            let head_batch: Vec<super::changes::BigRepoHeadNotification> =
                recv_head_batch(&mut head_rx).await;
            assert!(matches!(
                head_batch.as_slice(),
                [super::changes::BigRepoHeadNotification::DocHeadsChanged {
                    doc_id: seen_doc_id,
                    origin: BigRepoChangeOrigin::Remote { .. },
                    ..
                }] if *seen_doc_id == doc_id
            ));

            wait_for_json_doc(&server_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
            wait_for_json_doc(&client_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;

            client_conn.stop().await?;
            server.shutdown().await?;
            client.shutdown().await?;
            eyre::Ok(())
        })
        .await
        .expect("sync test timed out")?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sync_with_peer_local_change_without_change_listener_only_emits_heads() -> Res<()> {
        timeout(SYNC_CASE_TIMEOUT, async {
            let temp_root = tempdir()?;
            let server_path = temp_root.path().join("server");
            let client_path = temp_root.path().join("client");

            let mut expected_doc =
                make_sync_doc_value("base", SYNC_DOC_ITEMS, SYNC_DOC_PAYLOAD_LEN);
            let mut base_doc = automerge::Automerge::new();
            write_sync_doc_value(&mut base_doc, &expected_doc);

            let server = SyncRepoNode::boot(server_path, 121, true).await?;
            let client = SyncRepoNode::boot(client_path, 122, false).await?;
            let doc_id = random_doc_id();
            let server_doc = server.repo.put_doc(doc_id, base_doc.clone()).await?;
            let client_doc = client.repo.put_doc(doc_id, base_doc).await?;
            set_doc_actor(&server_doc, automerge::ActorId::from([121_u8; 16])).await?;
            set_doc_actor(&client_doc, automerge::ActorId::from([122_u8; 16])).await?;

            let (_head_registration, mut head_rx) = client
                .repo
                .change_manager
                .subscribe_head_listener(super::changes::HeadFilter {
                    doc_id: Some(super::changes::DocIdFilter::new(doc_id)),
                })
                .await?;
            assert!(
                !client
                    .repo
                    .change_manager
                    .has_change_listener_interest(doc_id, &BigRepoChangeOrigin::Local),
                "no change listeners should be interested before mutation"
            );

            let client_conn = connect_sync_pair(&client, &server).await?;
            server.wait_for_accepts(1).await;

            client_doc
                .with_document(|doc| {
                    apply_sync_mutation_in_place(
                        doc,
                        SyncMutation {
                            item_idx: 2,
                            note_key: "heads_only",
                            side_label: "local",
                        },
                        SYNC_DOC_PAYLOAD_LEN,
                    );
                })
                .await?;
            apply_sync_mutation(
                &mut expected_doc,
                SyncMutation {
                    item_idx: 2,
                    note_key: "heads_only",
                    side_label: "local",
                },
                SYNC_DOC_PAYLOAD_LEN,
            );

            let head_batch: Vec<super::changes::BigRepoHeadNotification> =
                recv_head_batch(&mut head_rx).await;
            assert!(matches!(
                head_batch.as_slice(),
                [super::changes::BigRepoHeadNotification::DocHeadsChanged {
                    doc_id: seen_doc_id,
                    origin: BigRepoChangeOrigin::Local,
                    ..
                }] if *seen_doc_id == doc_id
            ));

            wait_for_json_doc(&client_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
            wait_for_json_doc(&server_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;

            client_conn.stop().await?;
            server.shutdown().await?;
            client.shutdown().await?;
            eyre::Ok(())
        })
        .await
        .expect("sync test timed out")?;
        eyre::Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn big_repo_sync_randomized_four_node_stress_converges() -> Res<()> {
        stress_support::run_randomized_four_node_stress_with_settle_timeout(
            BigRepoStressFixture::default(),
            Arc::new(()),
            stress_support::PHASE1_MUTATIONS,
            stress_support::PHASE2_MUTATIONS,
            stress_support::PHASE3_MUTATIONS,
            Duration::from_secs(20),
        )
        .await
    }
}
