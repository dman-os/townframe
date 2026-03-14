use crate::interlude::*;

use automerge::ChangeHash;
use autosurgeon::{Hydrate, Prop, Reconcile};
use samod::DocumentId;
use sqlx::sqlite::SqliteConnectOptions;
use std::str::FromStr;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

mod changes;
pub mod iroh;
mod partition;

pub use changes::{
    path_prefix_matches as big_repo_path_prefix_matches, BigRepoChangeNotification,
    BigRepoHeadNotification, BigRepoLocalNotification, ChangeFilter as BigRepoChangeFilter,
    ChangeListenerRegistration as BigRepoChangeListenerRegistration,
    DocChangeBrokerLease as BigRepoDocChangeBrokerLease, DocIdFilter as BigRepoDocIdFilter,
    HeadFilter as BigRepoHeadFilter, HeadListenerRegistration as BigRepoHeadListenerRegistration,
    LocalFilter as BigRepoLocalFilter,
    LocalListenerRegistration as BigRepoLocalListenerRegistration,
    OriginFilter as BigRepoOriginFilter,
};
pub use samod_core::ChangeOrigin as BigRepoChangeOrigin;

#[derive(Debug, Clone)]
pub struct BigRepoConfig {
    pub sqlite_url: String,
    pub subscription_capacity: usize,
}

/// Configuration for Automerge storage
#[derive(Debug, Clone)]
pub struct Config {
    /// Peer ID for this client
    pub peer_id: String,
    /// Storage directory for Automerge documents
    pub storage: StorageConfig,
}

#[derive(Debug, Clone)]
pub enum StorageConfig {
    Disk { path: PathBuf },
    Memory,
}

impl BigRepoConfig {
    pub fn new(sqlite_url: impl Into<String>) -> Self {
        Self {
            sqlite_url: sqlite_url.into(),
            subscription_capacity: crate::sync::DEFAULT_SUBSCRIPTION_CAPACITY,
        }
    }
}

#[derive(educe::Educe)]
#[educe(Debug)]
pub struct BigRepo {
    #[educe(Debug(ignore))]
    repo: samod::Repo,
    #[educe(Debug(ignore))]
    state_pool: sqlx::SqlitePool,
    #[educe(Debug(ignore))]
    partition_events_tx: broadcast::Sender<crate::sync::PartitionEvent>,
    #[educe(Debug(ignore))]
    change_manager: Arc<changes::ChangeListenerManager>,
    #[educe(Debug(ignore))]
    partition_forwarder_cancel: CancellationToken,
    #[educe(Debug(ignore))]
    partition_forwarders: Arc<utils_rs::AbortableJoinSet>,
    #[educe(Debug(ignore))]
    change_manager_stop: std::sync::Mutex<Option<changes::ChangeListenerManagerStopToken>>,
}

pub type SharedBigRepo = Arc<BigRepo>;

#[derive(Debug)]
pub enum ImportDocFastOutcome {
    Imported(samod::DocHandle),
    AlreadyExists,
}

impl BigRepo {
    pub async fn boot_with_repo(repo: samod::Repo, config: BigRepoConfig) -> Res<Arc<Self>> {
        let connect_options = SqliteConnectOptions::from_str(&config.sqlite_url)
            .wrap_err_with(|| format!("invalid sqlite url: {}", config.sqlite_url))?
            .create_if_missing(true);
        let state_pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(connect_options)
            .await
            .wrap_err("failed connecting big repo sqlite")?;
        let (partition_events_tx, _) = broadcast::channel(config.subscription_capacity.max(1));
        let (change_manager, change_manager_stop) = changes::ChangeListenerManager::boot();

        let out = Arc::new(Self {
            repo,
            state_pool,
            partition_events_tx,
            change_manager,
            partition_forwarder_cancel: CancellationToken::new(),
            partition_forwarders: Arc::new(utils_rs::AbortableJoinSet::new()),
            change_manager_stop: std::sync::Mutex::new(Some(change_manager_stop)),
        });
        let (_head_reg, mut head_rx) = out
            .subscribe_head_listener(BigRepoHeadFilter { doc_id: None })
            .await?;
        {
            let cancel_token = out.partition_forwarder_cancel.child_token();
            let repo = Arc::downgrade(&out);
            out.partition_forwarders
                .spawn(async move {
                    let _head_reg = _head_reg;
                    let fut = async {
                        loop {
                            tokio::select! {
                                biased;
                                _ = cancel_token.cancelled() => break,
                                val = head_rx.recv() => {
                                    let Some(batch) = val else {
                                        eyre::bail!("head listener channel closed")
                                    };
                                    let Some(repo) = repo.upgrade() else {
                                        break;
                                    };
                                    for msg in batch {
                                        let BigRepoHeadNotification::DocHeadsChanged { doc_id, heads, origin } = msg;
                                        if !matches!(origin, samod_core::ChangeOrigin::Remote { .. }) {
                                            continue;
                                        }
                                        repo.record_doc_heads_change(&doc_id, heads.to_vec())
                                            .await
                                            .expect("failed recording remote doc heads change");
                                    }
                                }
                            }
                        }
                        eyre::Ok(())
                    };
                    fut.await.unwrap();
                })
                .expect("failed spawning remote heads forwarding worker");
        }
        out.ensure_schema().await?;
        Ok(out)
    }

    pub async fn boot<A: samod::AnnouncePolicy>(
        config: Config,
        announce_policy: Option<A>,
    ) -> Res<(Arc<Self>, BigRepoStopToken)> {
        let peer_id = samod::PeerId::from_string(config.peer_id);
        let repo = samod::Repo::build_tokio().with_peer_id(peer_id);
        let (repo, sqlite_url) = match config.storage {
            StorageConfig::Disk { path } => {
                std::fs::create_dir_all(&path).wrap_err_with(|| {
                    format!("Failed to create storage directory: {}", path.display())
                })?;
                let repo = repo.with_storage(samod::storage::TokioFilesystemStorage::new(
                    path.to_string_lossy().as_ref(),
                ));
                let loaded = if let Some(policy) = announce_policy {
                    repo.with_announce_policy(policy).load().await
                } else {
                    repo.load().await
                };
                let sqlite_url = format!("sqlite://{}", path.join("big_repo.sqlite").display());
                (loaded, sqlite_url)
            }
            StorageConfig::Memory => {
                let repo = repo.with_storage(samod::storage::InMemoryStorage::new());
                let loaded = if let Some(policy) = announce_policy {
                    repo.with_announce_policy(policy).load().await
                } else {
                    repo.load().await
                };
                (loaded, "sqlite::memory:".to_string())
            }
        };
        let out = Self::boot_with_repo(repo.clone(), BigRepoConfig::new(sqlite_url)).await?;
        let change_manager_stop = out
            .change_manager_stop
            .lock()
            .expect(ERROR_MUTEX)
            .take()
            .expect("BigRepo change manager stop token missing");
        Ok((
            Arc::clone(&out),
            BigRepoStopToken {
                repo,
                change_manager_stop: Some(change_manager_stop),
                partition_forwarder_cancel: out.partition_forwarder_cancel.clone(),
                partition_forwarders: Arc::clone(&out.partition_forwarders),
            },
        ))
    }

    pub fn samod_repo(&self) -> &samod::Repo {
        &self.repo
    }

    pub fn state_pool(&self) -> &sqlx::SqlitePool {
        &self.state_pool
    }

    pub fn subscribe_partition_events(&self) -> broadcast::Receiver<crate::sync::PartitionEvent> {
        self.partition_events_tx.subscribe()
    }

    pub async fn ensure_change_broker(
        self: &Arc<Self>,
        handle: samod::DocHandle,
    ) -> Res<Arc<changes::DocChangeBrokerLease>> {
        self.change_manager.add_doc_listener(handle).await
    }

    pub async fn subscribe_change_listener(
        self: &Arc<Self>,
        filter: BigRepoChangeFilter,
    ) -> Res<(
        BigRepoChangeListenerRegistration,
        tokio::sync::mpsc::UnboundedReceiver<Vec<BigRepoChangeNotification>>,
    )> {
        let mut broker_leases = Vec::new();
        if let Some(target_doc) = filter.doc_id.as_ref() {
            let handle = self.find_doc_handle(&target_doc.doc_id).await?;
            if let Some(handle) = handle {
                let lease = self.ensure_change_broker(handle).await?;
                lease.wait_until_ready().await;
                broker_leases.push(lease);
            }
        }
        let (registration, change_rx) = self.change_manager.subscribe_listener(filter).await?;
        Ok((registration.with_broker_leases(broker_leases), change_rx))
    }

    pub async fn subscribe_local_listener(
        self: &Arc<Self>,
        filter: BigRepoLocalFilter,
    ) -> Res<(
        BigRepoLocalListenerRegistration,
        tokio::sync::mpsc::UnboundedReceiver<Vec<BigRepoLocalNotification>>,
    )> {
        self.change_manager.subscribe_local_listener(filter).await
    }

    pub async fn subscribe_head_listener(
        self: &Arc<Self>,
        filter: BigRepoHeadFilter,
    ) -> Res<(
        BigRepoHeadListenerRegistration,
        tokio::sync::mpsc::UnboundedReceiver<Vec<BigRepoHeadNotification>>,
    )> {
        let mut broker_leases = Vec::new();
        if let Some(target_doc) = filter.doc_id.as_ref() {
            let handle = self.find_doc_handle(&target_doc.doc_id).await?;
            if let Some(handle) = handle {
                let lease = self.ensure_change_broker(handle).await?;
                lease.wait_until_ready().await;
                broker_leases.push(lease);
            }
        }
        let (registration, rx) = self.change_manager.subscribe_head_listener(filter).await?;
        Ok((registration.with_broker_leases(broker_leases), rx))
    }

    pub async fn create_doc(
        self: &Arc<Self>,
        initial_content: automerge::Automerge,
    ) -> Res<BigDocHandle> {
        let handle = self
            .repo
            .create(initial_content)
            .await
            .map_err(|err| ferr!("failed creating doc: {err}"))?;
        let out = BigDocHandle {
            repo: Arc::clone(self),
            inner: handle,
        };
        let heads = out
            .inner
            .with_document(|doc| Arc::<[automerge::ChangeHash]>::from(doc.get_heads()));
        self.change_manager
            .notify_doc_created(out.document_id().clone(), Arc::clone(&heads))?;
        self.change_manager
            .notify_local_doc_created(out.document_id().clone(), heads)?;
        self.record_doc_heads_change(
            out.document_id(),
            out.inner.with_document(|doc| doc.get_heads()),
        )
        .await?;
        Ok(out)
    }

    pub async fn import_doc(
        self: &Arc<Self>,
        document_id: DocumentId,
        initial_content: automerge::Automerge,
    ) -> Res<BigDocHandle> {
        let handle = self
            .repo
            .import(document_id, initial_content)
            .await
            .map_err(|err| ferr!("failed importing doc: {err}"))?;
        let out = BigDocHandle {
            repo: Arc::clone(self),
            inner: handle,
        };
        let heads = out
            .inner
            .with_document(|doc| Arc::<[automerge::ChangeHash]>::from(doc.get_heads()));
        self.change_manager
            .notify_doc_imported(out.document_id().clone(), Arc::clone(&heads))?;
        self.change_manager
            .notify_local_doc_imported(out.document_id().clone(), heads)?;
        self.record_doc_heads_change(
            out.document_id(),
            out.inner.with_document(|doc| doc.get_heads()),
        )
        .await?;
        Ok(out)
    }

    pub async fn import_doc_fast(
        self: &Arc<Self>,
        document_id: DocumentId,
        initial_content: automerge::Automerge,
    ) -> Res<ImportDocFastOutcome> {
        match self.repo.import(document_id, initial_content).await {
            Ok(handle) => {
                let heads = handle
                    .with_document(|doc| Arc::<[automerge::ChangeHash]>::from(doc.get_heads()));
                self.change_manager
                    .notify_doc_imported(handle.document_id().clone(), Arc::clone(&heads))?;
                self.change_manager
                    .notify_local_doc_imported(handle.document_id().clone(), heads)?;
                self.record_doc_heads_change(
                    handle.document_id(),
                    handle.with_document(|doc| doc.get_heads()),
                )
                .await?;
                Ok(ImportDocFastOutcome::Imported(handle))
            }
            Err(samod::ImportError::AlreadyExists { .. }) => {
                Ok(ImportDocFastOutcome::AlreadyExists)
            }
            Err(err) => Err(ferr!("failed importing doc (fast path): {err}")),
        }
    }

    pub async fn find_doc(self: &Arc<Self>, document_id: &DocumentId) -> Res<Option<BigDocHandle>> {
        let handle = self
            .repo
            .find(document_id.clone())
            .await
            .map_err(|err| ferr!("failed finding doc: {err}"))?;
        let Some(inner) = handle else {
            return Ok(None);
        };
        Ok(Some(BigDocHandle {
            repo: Arc::clone(self),
            inner,
        }))
    }

    pub async fn add_doc(
        self: &Arc<Self>,
        initial_content: automerge::Automerge,
    ) -> Res<samod::DocHandle> {
        let handle = self
            .repo
            .create(initial_content)
            .await
            .map_err(|err| ferr!("failed creating doc: {err}"))?;
        let heads =
            handle.with_document(|doc| Arc::<[automerge::ChangeHash]>::from(doc.get_heads()));
        self.change_manager
            .notify_doc_created(handle.document_id().clone(), Arc::clone(&heads))?;
        self.change_manager
            .notify_local_doc_created(handle.document_id().clone(), heads)?;
        self.record_doc_heads_change(
            handle.document_id(),
            handle.with_document(|doc| doc.get_heads()),
        )
        .await?;
        Ok(handle)
    }

    pub async fn find_doc_handle(
        self: &Arc<Self>,
        document_id: &DocumentId,
    ) -> Res<Option<samod::DocHandle>> {
        let handle = self
            .repo
            .find(document_id.clone())
            .await
            .map_err(|err| ferr!("failed finding doc: {err}"))?;
        let Some(handle) = handle else {
            return Ok(None);
        };
        Ok(Some(handle))
    }

    pub async fn local_contains_document(self: &Arc<Self>, document_id: &DocumentId) -> Res<bool> {
        self.repo
            .local_contains_document(document_id.clone())
            .await
            .map_err(|err| ferr!("failed checking local doc presence: {err}"))
    }

    async fn on_doc_heads_changed(
        &self,
        doc_id: &DocumentId,
        heads: Vec<automerge::ChangeHash>,
    ) -> Res<()> {
        self.change_manager.notify_local_doc_heads_updated(
            doc_id.clone(),
            Arc::<[automerge::ChangeHash]>::from(heads.clone()),
        )?;
        self.record_doc_heads_change(doc_id, heads).await
    }

    pub async fn spawn_ws_connector(&self, addr: Url) -> Res<tokio::task::JoinHandle<()>> {
        let repo = self.repo.clone();
        let handle = repo
            .dial_websocket(addr, samod::BackoffConfig::default())
            .wrap_err("error setting up dialer")?;
        let fut = async move {
            let mut events = handle.events();
            while let Some(event) = events.next().await {
                use samod::DialerEvent;
                match event {
                    DialerEvent::Connected { peer_info } => {
                        info!(?peer_info, "connection established")
                    }
                    DialerEvent::Disconnected { reason } => {
                        warn!(?reason, "error connecting to server")
                    }
                    DialerEvent::Reconnecting { attempt } => {
                        warn!(?attempt, "retrying to conect to server")
                    }
                    DialerEvent::MaxRetriesReached => {
                        unreachable!("we don't have max retries")
                    }
                }
            }
        };
        Ok(tokio::spawn(fut.instrument(tracing::info_span!(
            "websocket sync server connector task"
        ))))
    }
}

// autosurgeon helpers
impl BigRepo {
    pub async fn reconcile_prop_with_actor<'a, T, P>(
        self: &Arc<Self>,
        doc_id: &DocumentId,
        obj_id: automerge::ObjId,
        prop_name: P,
        update: &T,
        actor_id: Option<automerge::ActorId>,
    ) -> Res<Option<ChangeHash>>
    where
        T: Hydrate + Reconcile + Send + Sync + 'static,
        P: Into<autosurgeon::Prop<'a>> + Send + Sync + 'static,
    {
        let handle = self
            .find_doc_handle(doc_id)
            .await?
            .ok_or_eyre("doc not found")?;
        let res = handle
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
            .map_err(|err| ferr!("error on samod txn: {err:?}"))?;
        Ok(res.hash)
    }

    pub async fn hydrate_path<T: Hydrate + Reconcile + Send + Sync + 'static>(
        self: &Arc<Self>,
        doc_id: &DocumentId,
        obj_id: automerge::ObjId,
        path: Vec<Prop<'static>>,
    ) -> Res<Option<(T, Arc<[automerge::ChangeHash]>)>> {
        let handle = self
            .find_doc_handle(doc_id)
            .await?
            .ok_or_eyre("doc not found")?;
        handle.with_document(|doc| -> Res<Option<(T, Arc<[automerge::ChangeHash]>)>> {
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
    }

    pub async fn hydrate_path_at_heads<T: Hydrate + Reconcile + Send + Sync + 'static>(
        self: &Arc<Self>,
        doc_id: &DocumentId,
        heads: &[automerge::ChangeHash],
        obj_id: automerge::ObjId,
        path: Vec<Prop<'static>>,
    ) -> Res<Option<(T, Arc<[automerge::ChangeHash]>)>> {
        let handle = self
            .find_doc_handle(doc_id)
            .await?
            .ok_or_eyre("doc not found")?;
        handle.with_document(|doc| -> Res<Option<(T, Arc<[automerge::ChangeHash]>)>> {
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
    }
}

pub struct BigRepoStopToken {
    pub repo: samod::Repo,
    change_manager_stop: Option<changes::ChangeListenerManagerStopToken>,
    partition_forwarder_cancel: CancellationToken,
    partition_forwarders: Arc<utils_rs::AbortableJoinSet>,
}

impl BigRepoStopToken {
    pub async fn stop(mut self) -> Res<()> {
        self.partition_forwarder_cancel.cancel();
        self.partition_forwarders
            .stop(Duration::from_secs(5))
            .await?;
        if let Some(stop_token) = self.change_manager_stop.take() {
            stop_token.stop().await?;
        }
        self.repo.stop().await;
        Ok(())
    }
}
pub struct RepoConnection {
    pub id: samod::ConnectionId,
    pub peer_id: Arc<str>,
    pub peer_info: samod::PeerInfo,
    #[cfg(feature = "iroh")]
    pub endpoint_id: Option<::iroh::EndpointId>,
    // NOTE: if optionaly, we are using a connection that
    // uses a task we don't manage
    join_handle: Option<tokio::task::JoinHandle<()>>,
    cancel_token: CancellationToken,
}

impl RepoConnection {
    pub async fn stop(self) -> Res<()> {
        self.cancel_token.cancel();
        if let Some(join_handle) = self.join_handle {
            utils_rs::wait_on_handle_with_timeout(join_handle, Duration::from_secs(5)).await?;
        }
        Ok(())
    }
}

pub struct ConnFinishSignal {
    pub conn_id: samod::ConnectionId,
    pub peer_id: Arc<str>,
    pub reason: String,
}

#[derive(Clone)]
pub struct BigDocHandle {
    repo: Arc<BigRepo>,
    inner: samod::DocHandle,
}

impl std::fmt::Debug for BigDocHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BigDocHandle")
            .field("document_id", self.document_id())
            .finish()
    }
}

impl BigDocHandle {
    pub fn document_id(&self) -> &DocumentId {
        self.inner.document_id()
    }

    // pub fn raw_handle(&self) -> &samod::DocHandle {
    //     &self.inner
    // }

    pub async fn with_document<F, R>(&self, operation: F) -> Res<R>
    where
        F: 'static + Send + Sync + FnOnce(&mut automerge::Automerge) -> R,
        R: 'static + Send + Sync,
    {
        let handle = self.inner.clone();
        let (before_heads, out, after_heads) = tokio::task::spawn_blocking(move || {
            handle.with_document(|doc| {
                let before_heads = doc.get_heads();
                let out = operation(doc);
                let after_heads = doc.get_heads();
                (before_heads, out, after_heads)
            })
        })
        .await
        .expect(ERROR_TOKIO);
        if before_heads != after_heads {
            self.repo
                .on_doc_heads_changed(self.document_id(), after_heads)
                .await?;
        }
        Ok(out)
    }

    /// WARN: do not use this over join! or select!, it blocks the
    /// current tokio task while running document access inline.
    pub async fn with_document_local<F, R>(&self, operation: F) -> Res<R>
    where
        F: FnOnce(&mut automerge::Automerge) -> R,
    {
        let (before_heads, out, after_heads) = self.inner.with_document(|doc| {
            let before_heads = doc.get_heads();
            let out = operation(doc);
            let after_heads = doc.get_heads();
            (before_heads, out, after_heads)
        });
        if before_heads != after_heads {
            self.repo
                .on_doc_heads_changed(self.document_id(), after_heads)
                .await?;
        }
        Ok(out)
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;

    use crate::repo::{BigRepo, BigRepoConfig};
    use automerge::transaction::Transactable;
    use samod::DocumentId;
    use std::str::FromStr;
    use tokio::time::timeout;

    async fn boot_big_repo(peer: &str) -> Res<Arc<BigRepo>> {
        let repo = samod::Repo::build_tokio()
            .with_peer_id(samod::PeerId::from_string(format!("bigrepo-{peer}")))
            .with_storage(samod::storage::InMemoryStorage::new())
            .load()
            .await;
        BigRepo::boot_with_repo(repo, BigRepoConfig::new("sqlite::memory:".to_string())).await
    }

    #[tokio::test]
    async fn create_doc_emits_created_notification() -> Res<()> {
        let repo = boot_big_repo("create").await?;
        let (_registration, mut rx) = repo
            .subscribe_change_listener(BigRepoChangeFilter {
                doc_id: None,
                origin: None,
                path: vec![],
            })
            .await?;

        let handle = repo.create_doc(automerge::Automerge::new()).await?;
        let expected = handle.document_id().clone();

        let events = timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for create notification")
            .expect("change listener channel closed");
        assert!(events.iter().any(|event| {
            matches!(
                event,
                BigRepoChangeNotification::DocCreated { doc_id, heads, .. } if *doc_id == expected && !heads.is_empty()
            )
        }));
        Ok(())
    }

    #[tokio::test]
    async fn import_doc_emits_imported_notification() -> Res<()> {
        let src = boot_big_repo("src").await?;
        let dst = boot_big_repo("dst").await?;
        let (_registration, mut rx) = dst
            .subscribe_change_listener(BigRepoChangeFilter {
                doc_id: None,
                origin: None,
                path: vec![],
            })
            .await?;

        let src_handle = src.create_doc(automerge::Automerge::new()).await?;
        src_handle
            .with_document(|doc| {
                let mut tx = doc.transaction();
                tx.put(automerge::ROOT, "x", 1_i64)
                    .expect("failed writing source doc");
                tx.commit();
            })
            .await?;
        let doc_id = src_handle.document_id().to_string();
        let bytes = src_handle.inner.with_document(|doc| doc.save());

        let imported_id = DocumentId::from_str(&doc_id)
            .map_err(|err| ferr!("failed parsing doc id '{doc_id}': {err}"))?;
        let imported_doc = automerge::Automerge::load(&bytes)
            .map_err(|err| ferr!("failed loading save bytes for import test: {err}"))?;
        dst.import_doc(imported_id.clone(), imported_doc).await?;

        let events = timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for import notification")
            .expect("change listener channel closed");
        assert!(events.iter().any(|event| {
            matches!(
                event,
                BigRepoChangeNotification::DocImported { doc_id, heads, .. } if *doc_id == imported_id && !heads.is_empty()
            )
        }));
        Ok(())
    }

    #[tokio::test]
    async fn import_doc_fast_updates_partition_doc_state() -> Res<()> {
        let part_id = "fast-import-part".to_string();
        let src = boot_big_repo("fast-import-src").await?;
        let dst = boot_big_repo("fast-import-dst").await?;
        let _partition_events_rx = dst.subscribe_partition_events();

        let src_handle = src.create_doc(automerge::Automerge::new()).await?;
        src_handle
            .with_document(|doc| {
                let mut tx = doc.transaction();
                tx.put(automerge::ROOT, "x", 1_i64)
                    .expect("failed writing source doc");
                tx.commit();
            })
            .await?;
        let doc_id = src_handle.document_id().clone();
        let exported = src
            .samod_repo()
            .local_export(doc_id.clone())
            .await
            .map_err(|err| ferr!("failed exporting source doc: {err}"))?;

        dst.add_doc_to_partition(&part_id, &doc_id.to_string())
            .await?;

        let outcome = dst.import_doc_fast(doc_id.clone(), exported).await?;
        assert!(
            matches!(outcome, ImportDocFastOutcome::Imported(_)),
            "expected fast import to materialize new doc"
        );

        assert!(
            dst.is_doc_present_in_partition_state(&part_id, &doc_id.to_string())
                .await?,
            "fast import must keep the doc present in partition state for existing memberships"
        );

        let events = dst
            .get_partition_doc_events_for_peer(
                &"peer-fast-import".into(),
                &crate::sync::GetPartitionDocEventsRequest {
                    partitions: vec![crate::sync::PartitionCursorRequest {
                        partition_id: part_id.clone(),
                        since: None,
                    }],
                    limit: 32,
                },
            )
            .await?;
        assert!(
            events.events.iter().any(|event| {
                event.partition_id == part_id
                    && matches!(
                        &event.deets,
                        crate::sync::PartitionDocEventDeets::DocChanged {
                            doc_id: event_doc_id,
                            heads,
                            ..
                        } if event_doc_id == &doc_id.to_string() && !heads.is_empty()
                    )
            }),
            "fast import must produce current partition doc events with imported heads"
        );
        Ok(())
    }

    #[tokio::test]
    async fn with_document_emits_changed_notification() -> Res<()> {
        let repo = boot_big_repo("changed").await?;
        let handle = repo.create_doc(automerge::Automerge::new()).await?;
        let target = handle.document_id().clone();

        let (_registration, mut rx) = repo
            .subscribe_change_listener(BigRepoChangeFilter {
                doc_id: Some(BigRepoDocIdFilter {
                    doc_id: target.clone(),
                }),
                origin: None,
                path: vec![],
            })
            .await?;

        handle
            .with_document(|doc| {
                let mut tx = doc.transaction();
                tx.put(automerge::ROOT, "name", "abc")
                    .expect("failed writing doc");
                tx.commit();
            })
            .await?;

        loop {
            let events = timeout(Duration::from_secs(2), rx.recv())
                .await
                .expect("timed out waiting for changed notification")
                .expect("change listener channel closed");
            if events.iter().any(|event| {
                matches!(
                    event,
                    BigRepoChangeNotification::DocChanged { doc_id, .. } if *doc_id == target
                )
            }) {
                break;
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn change_listener_doc_id_filter_only_receives_target_doc() -> Res<()> {
        let repo = boot_big_repo("change-doc-filter").await?;
        let doc_a = repo.create_doc(automerge::Automerge::new()).await?;
        let doc_b = repo.create_doc(automerge::Automerge::new()).await?;
        let doc_a_id = doc_a.document_id().clone();
        let doc_b_id = doc_b.document_id().clone();

        let (_registration, mut rx) = repo
            .subscribe_change_listener(BigRepoChangeFilter {
                doc_id: Some(BigRepoDocIdFilter {
                    doc_id: doc_a_id.clone(),
                }),
                origin: None,
                path: vec![],
            })
            .await?;

        doc_b
            .with_document(|doc| {
                let mut tx = doc.transaction();
                tx.put(automerge::ROOT, "b", true)
                    .expect("failed writing doc b");
                tx.commit();
            })
            .await?;
        assert!(
            timeout(Duration::from_millis(300), rx.recv())
                .await
                .is_err(),
            "doc_id filtered change listener unexpectedly received doc_b event"
        );

        doc_a
            .with_document(|doc| {
                let mut tx = doc.transaction();
                tx.put(automerge::ROOT, "a", true)
                    .expect("failed writing doc a");
                tx.commit();
            })
            .await?;

        loop {
            let events = timeout(Duration::from_secs(2), rx.recv())
                .await
                .expect("timed out waiting for doc_a change event")
                .expect("change listener channel closed");
            if events.iter().any(|event| {
                matches!(
                    event,
                    BigRepoChangeNotification::DocChanged { doc_id, .. } if *doc_id == doc_a_id
                )
            }) {
                assert!(
                    !events.iter().any(|event| {
                        matches!(
                            event,
                            BigRepoChangeNotification::DocChanged { doc_id, .. } if *doc_id == doc_b_id
                        )
                    }),
                    "filtered change listener received doc_b event"
                );
                break;
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn change_listener_path_filter_matches_only_prefix() -> Res<()> {
        let repo = boot_big_repo("change-path-filter").await?;
        let handle = repo.create_doc(automerge::Automerge::new()).await?;
        let target_id = handle.document_id().clone();

        let (_registration, mut rx) = repo
            .subscribe_change_listener(BigRepoChangeFilter {
                doc_id: Some(BigRepoDocIdFilter {
                    doc_id: target_id.clone(),
                }),
                origin: None,
                path: vec!["container".into()],
            })
            .await?;

        // Create is not a DocChanged patch and should not match non-empty path filters.
        assert!(
            timeout(Duration::from_millis(300), rx.recv())
                .await
                .is_err(),
            "path-filtered listener unexpectedly received non-patch event"
        );

        handle
            .with_document(|doc| {
                let mut tx = doc.transaction();
                tx.put(automerge::ROOT, "other_key", "ignored")
                    .expect("failed writing other_key");
                tx.commit();
            })
            .await?;
        assert!(
            timeout(Duration::from_millis(300), rx.recv())
                .await
                .is_err(),
            "path-filtered listener unexpectedly matched unrelated path"
        );

        handle
            .with_document(|doc| {
                let mut tx = doc.transaction();
                let container = tx
                    .put_object(automerge::ROOT, "container", automerge::ObjType::Map)
                    .expect("failed creating container object");
                tx.put(&container, "inner", "matched")
                    .expect("failed writing container.inner");
                tx.commit();
            })
            .await?;

        loop {
            let events = timeout(Duration::from_secs(2), rx.recv())
                .await
                .expect("timed out waiting for path-filtered change event")
                .expect("change listener channel closed");
            if events.iter().any(|event| {
                matches!(
                    event,
                    BigRepoChangeNotification::DocChanged { doc_id, .. } if *doc_id == target_id
                )
            }) {
                break;
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn change_listener_origin_filter_works_for_local_events() -> Res<()> {
        let repo = boot_big_repo("origin-filter").await?;

        let (_remote_registration, mut remote_rx) = repo
            .subscribe_change_listener(BigRepoChangeFilter {
                doc_id: None,
                origin: Some(BigRepoOriginFilter::Remote),
                path: vec![],
            })
            .await?;

        let (_local_registration, mut local_rx) = repo
            .subscribe_change_listener(BigRepoChangeFilter {
                doc_id: None,
                origin: Some(BigRepoOriginFilter::Local),
                path: vec![],
            })
            .await?;

        let handle = repo.create_doc(automerge::Automerge::new()).await?;
        let target = handle.document_id().clone();

        assert!(
            timeout(Duration::from_millis(300), remote_rx.recv())
                .await
                .is_err(),
            "remote origin filter should not receive local create events"
        );
        let local_events = timeout(Duration::from_secs(2), local_rx.recv())
            .await
            .expect("timed out waiting for local-origin event")
            .expect("local origin channel closed");
        assert!(local_events.iter().any(|event| {
            matches!(
                event,
                BigRepoChangeNotification::DocCreated { doc_id, origin, .. } if *doc_id == target && matches!(origin, samod_core::ChangeOrigin::Local)
            )
        }));

        Ok(())
    }

    #[tokio::test]
    async fn local_listener_receives_create_import_and_heads_updates() -> Res<()> {
        let src = boot_big_repo("localsrc").await?;
        let dst = boot_big_repo("localdst").await?;
        let (_registration, mut rx) = dst
            .subscribe_local_listener(BigRepoLocalFilter { doc_id: None })
            .await?;

        let created = dst.create_doc(automerge::Automerge::new()).await?;
        let created_id = created.document_id().clone();
        let create_events = timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for local create")
            .expect("local channel closed");
        assert!(create_events.iter().any(|event| {
            matches!(
                event,
                BigRepoLocalNotification::DocCreated { doc_id, heads } if *doc_id == created_id && !heads.is_empty()
            )
        }));

        created
            .with_document(|doc| {
                let mut tx = doc.transaction();
                tx.put(automerge::ROOT, "key", "value")
                    .expect("failed updating created doc");
                tx.commit();
            })
            .await?;
        loop {
            let events = timeout(Duration::from_secs(2), rx.recv())
                .await
                .expect("timed out waiting for local heads update")
                .expect("local channel closed");
            if events.iter().any(|event| {
                matches!(
                    event,
                    BigRepoLocalNotification::DocHeadsUpdated { doc_id, heads } if *doc_id == created_id && !heads.is_empty()
                )
            }) {
                break;
            }
        }

        let src_doc = src.create_doc(automerge::Automerge::new()).await?;
        src_doc
            .with_document(|doc| {
                let mut tx = doc.transaction();
                tx.put(automerge::ROOT, "n", 1_i64)
                    .expect("failed writing source");
                tx.commit();
            })
            .await?;
        let import_id = src_doc.document_id().clone();
        let imported_doc =
            automerge::Automerge::load(&src_doc.inner.with_document(|doc| doc.save()))
                .map_err(|err| ferr!("failed loading source save for import: {err}"))?;
        dst.import_doc(import_id.clone(), imported_doc).await?;
        loop {
            let events = timeout(Duration::from_secs(2), rx.recv())
                .await
                .expect("timed out waiting for local import")
                .expect("local channel closed");
            if events.iter().any(|event| {
                matches!(
                    event,
                    BigRepoLocalNotification::DocImported { doc_id, heads } if *doc_id == import_id && !heads.is_empty()
                )
            }) {
                break;
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn local_listener_doc_id_filter_only_receives_target_doc() -> Res<()> {
        let repo = boot_big_repo("localfilter").await?;
        let doc_a = repo.create_doc(automerge::Automerge::new()).await?;
        let doc_b = repo.create_doc(automerge::Automerge::new()).await?;
        let doc_a_id = doc_a.document_id().clone();
        let doc_b_id = doc_b.document_id().clone();

        let (_registration, mut rx) = repo
            .subscribe_local_listener(BigRepoLocalFilter {
                doc_id: Some(BigRepoDocIdFilter {
                    doc_id: doc_a_id.clone(),
                }),
            })
            .await?;

        doc_b
            .with_document(|doc| {
                let mut tx = doc.transaction();
                tx.put(automerge::ROOT, "b", true)
                    .expect("failed writing doc b");
                tx.commit();
            })
            .await?;
        assert!(
            timeout(Duration::from_millis(200), rx.recv())
                .await
                .is_err(),
            "doc_id filtered listener unexpectedly received doc_b event"
        );

        doc_a
            .with_document(|doc| {
                let mut tx = doc.transaction();
                tx.put(automerge::ROOT, "a", true)
                    .expect("failed writing doc a");
                tx.commit();
            })
            .await?;
        loop {
            let events = timeout(Duration::from_secs(2), rx.recv())
                .await
                .expect("timed out waiting for doc_a local event")
                .expect("local channel closed");
            if events.iter().any(|event| {
                matches!(
                    event,
                    BigRepoLocalNotification::DocHeadsUpdated { doc_id, .. } if *doc_id == doc_a_id
                )
            }) {
                break;
            }
            assert!(
                !events.iter().any(|event| {
                    matches!(
                        event,
                        BigRepoLocalNotification::DocHeadsUpdated { doc_id, .. } if *doc_id == doc_b_id
                    )
                }),
                "filtered listener received doc_b local event"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn local_listener_does_not_receive_raw_samod_changes() -> Res<()> {
        let repo = boot_big_repo("localscope").await?;
        let handle = repo.create_doc(automerge::Automerge::new()).await?;
        let target_id = handle.document_id().clone();

        let (_registration, mut rx) = repo
            .subscribe_local_listener(BigRepoLocalFilter {
                doc_id: Some(BigRepoDocIdFilter {
                    doc_id: target_id.clone(),
                }),
            })
            .await?;

        // Drain create event from listener setup lifecycle
        let _ = timeout(Duration::from_secs(2), rx.recv()).await;

        // This bypasses BigDocHandle::with_document and therefore should not hit local listener.
        handle.inner.with_document(|doc| {
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "raw", "samod")
                .expect("failed raw samod write");
            tx.commit();
        });

        assert!(
            timeout(Duration::from_millis(300), rx.recv())
                .await
                .is_err(),
            "local listener should ignore raw samod-originated changes"
        );
        Ok(())
    }
}
