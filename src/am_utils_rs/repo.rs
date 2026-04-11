use crate::interlude::*;
use crate::partition::PartitionStore;

use automerge::ChangeHash;
use autosurgeon::{Hydrate, Prop, Reconcile};
use sedimentree_core::loose_commit::id::CommitId;
use sqlx::sqlite::SqliteConnectOptions;
use std::collections::BTreeSet;
use std::str::FromStr;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

mod changes;
mod partition;
pub mod rpc;
mod runtime;

pub use changes::{
    path_prefix_matches as big_repo_path_prefix_matches, BigRepoChangeNotification,
    BigRepoChangeOrigin, BigRepoHeadNotification, BigRepoLocalNotification,
    ChangeFilter as BigRepoChangeFilter,
    ChangeListenerRegistration as BigRepoChangeListenerRegistration,
    DocChangeBrokerLease as BigRepoDocChangeBrokerLease, DocIdFilter as BigRepoDocIdFilter,
    HeadFilter as BigRepoHeadFilter, HeadListenerRegistration as BigRepoHeadListenerRegistration,
    LocalFilter as BigRepoLocalFilter,
    LocalListenerRegistration as BigRepoLocalListenerRegistration,
    OriginFilter as BigRepoOriginFilter,
};

pub type DocumentId = crate::ids::DocId32;
pub type PeerId = crate::ids::PeerId32;

#[derive(Debug, Clone)]
pub struct Config {
    pub peer_id: PeerId,
    pub storage: StorageConfig,
}

#[derive(Debug, Clone)]
pub enum StorageConfig {
    Disk { path: PathBuf },
    Memory,
}

struct LiveDocBundle {
    doc_id: DocumentId,
    doc: tokio::sync::Mutex<automerge::Automerge>,
}

impl LiveDocBundle {
    fn new(doc_id: DocumentId, doc: automerge::Automerge) -> Self {
        Self {
            doc_id,
            doc: tokio::sync::Mutex::new(doc),
        }
    }
}

#[derive(educe::Educe)]
#[educe(Debug)]
pub struct BigRepo {
    local_peer_id: PeerId,
    #[educe(Debug(ignore))]
    state_pool: sqlx::SqlitePool,
    #[educe(Debug(ignore))]
    partition_store: Arc<PartitionStore>,
    #[educe(Debug(ignore))]
    runtime: runtime::BigRepoRuntimeHandle,
    #[educe(Debug(ignore))]
    live_bundles: DHashMap<DocumentId, std::sync::Weak<LiveDocBundle>>,
    #[educe(Debug(ignore))]
    change_manager: Arc<changes::ChangeListenerManager>,
    #[educe(Debug(ignore))]
    partition_forwarder_cancel: CancellationToken,
    #[educe(Debug(ignore))]
    join_set: Arc<utils_rs::AbortableJoinSet>,
    #[educe(Debug(ignore))]
    change_manager_stop: std::sync::Mutex<Option<changes::ChangeListenerManagerStopToken>>,
}

pub type SharedBigRepo = Arc<BigRepo>;

impl BigRepo {
    pub async fn boot(config: Config) -> Res<(Arc<Self>, BigRepoStopToken)> {
        let Config { peer_id, storage } = config;
        let sqlite_url = match &storage {
            StorageConfig::Memory => "sqlite::memory:".to_string(),
            StorageConfig::Disk { path } => {
                std::fs::create_dir_all(path).wrap_err_with(|| {
                    format!("Failed to create storage directory: {}", path.display())
                })?;
                format!("sqlite://{}", path.join("big_repo.sqlite").display())
            }
        };

        let state_pool = {
            let connect_options = SqliteConnectOptions::from_str(&sqlite_url)
                .wrap_err_with(|| format!("invalid sqlite url: {sqlite_url}"))?
                .create_if_missing(true);
            sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(1)
                .connect_with(connect_options)
                .await
                .wrap_err("failed connecting big repo sqlite")?
        };
        ensure_docs_schema(&state_pool).await?;

        let join_set = Arc::new(utils_rs::AbortableJoinSet::new());
        let partition_forwarder_cancel = CancellationToken::new();
        let partition_store = {
            let (partition_events_tx, _) =
                broadcast::channel(crate::sync::protocol::DEFAULT_SUBSCRIPTION_CAPACITY);
            Arc::new(PartitionStore::new(
                state_pool.clone(),
                partition_events_tx,
                partition_forwarder_cancel.clone(),
                Arc::clone(&join_set),
            ))
        };
        partition_store.ensure_schema().await?;

        let (change_manager, change_manager_stop) = changes::ChangeListenerManager::boot();
        let signer =
            subduction_crypto::signer::memory::MemorySigner::from_bytes(peer_id.as_bytes());
        let runtime = match storage {
            StorageConfig::Memory => runtime::spawn_big_repo_runtime(
                Arc::clone(&join_set),
                signer,
                subduction_core::storage::memory::MemoryStorage::new(),
                Arc::clone(&partition_store),
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
                    Arc::clone(&join_set),
                    signer,
                    fs_storage,
                    Arc::clone(&partition_store),
                    Arc::clone(&change_manager),
                )?
            }
        };

        let out = Arc::new(Self {
            local_peer_id: peer_id,
            state_pool,
            partition_store,
            runtime,
            live_bundles: default(),
            change_manager,
            partition_forwarder_cancel,
            join_set,
            change_manager_stop: std::sync::Mutex::new(Some(change_manager_stop)),
        });

        let change_manager_stop = out
            .change_manager_stop
            .lock()
            .expect(ERROR_MUTEX)
            .take()
            .expect("BigRepo change manager stop token missing");

        Ok((
            Arc::clone(&out),
            BigRepoStopToken {
                runtime: out.runtime.clone(),
                change_manager_stop: Some(change_manager_stop),
                partition_forwarder_cancel: out.partition_forwarder_cancel.clone(),
                partition_forwarders: Arc::clone(&out.join_set),
            },
        ))
    }

    pub fn partition_store(&self) -> Arc<PartitionStore> {
        Arc::clone(&self.partition_store)
    }

    pub fn local_peer_id(&self) -> PeerId {
        self.local_peer_id
    }

    pub async fn subscribe_partition_doc_events_local(
        &self,
        partition_id: &crate::sync::protocol::PartitionId,
        since: Option<u64>,
        capacity: usize,
    ) -> Res<tokio::sync::mpsc::Receiver<crate::sync::protocol::PartitionDocEvent>> {
        self.partition_store
            .subscribe_partition_doc_events_local(partition_id, since, capacity)
            .await
    }

    pub fn subscribe_partition_events(
        &self,
    ) -> broadcast::Receiver<crate::sync::protocol::PartitionEvent> {
        self.partition_store.subscribe_partition_events()
    }

    pub async fn ensure_change_broker(
        self: &Arc<Self>,
        handle: BigDocHandle,
    ) -> Res<Arc<changes::DocChangeBrokerLease>> {
        self.change_manager
            .add_doc_listener(*handle.document_id())
            .await
    }

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
        let (registration, rx) = self.change_manager.subscribe_head_listener(filter).await?;
        Ok((registration, rx))
    }

    pub async fn create_doc(
        self: &Arc<Self>,
        initial_content: automerge::Automerge,
    ) -> Res<BigDocHandle> {
        let mut doc_id = DocumentId::random();
        while self.local_contains_document(&doc_id).await? {
            doc_id = DocumentId::random();
        }
        let bundle = Arc::new(LiveDocBundle::new(doc_id, initial_content));
        self.persist_full_bundle(&bundle).await?;
        self.upsert_known_doc(bundle.doc_id).await?;

        self.live_bundles
            .insert(bundle.doc_id, Arc::downgrade(&bundle));

        let out = BigDocHandle {
            repo: Arc::clone(self),
            bundle,
        };

        let heads = Arc::<[automerge::ChangeHash]>::from(
            out.with_document_read(|doc| doc.get_heads()).await,
        );
        self.record_doc_heads_change(out.document_id(), heads.to_vec())
            .await?;
        self.change_manager
            .notify_doc_created(*out.document_id(), Arc::clone(&heads))?;
        self.change_manager
            .notify_local_doc_created(*out.document_id(), heads)?;
        Ok(out)
    }

    pub async fn add_doc(
        self: &Arc<Self>,
        initial_content: automerge::Automerge,
    ) -> Res<BigDocHandle> {
        self.create_doc(initial_content).await
    }

    pub async fn import_doc(
        self: &Arc<Self>,
        document_id: DocumentId,
        initial_content: automerge::Automerge,
    ) -> Res<BigDocHandle> {
        let bundle = Arc::new(LiveDocBundle::new(document_id, initial_content));
        self.persist_full_bundle(&bundle).await?;
        self.upsert_known_doc(bundle.doc_id).await?;

        self.live_bundles
            .insert(bundle.doc_id, Arc::downgrade(&bundle));

        let out = BigDocHandle {
            repo: Arc::clone(self),
            bundle,
        };

        let heads = Arc::<[automerge::ChangeHash]>::from(
            out.with_document_read(|doc| doc.get_heads()).await,
        );
        self.record_doc_heads_change(out.document_id(), heads.to_vec())
            .await?;
        self.change_manager
            .notify_doc_imported(*out.document_id(), Arc::clone(&heads))?;
        self.change_manager
            .notify_local_doc_imported(*out.document_id(), heads)?;
        Ok(out)
    }

    pub async fn find_doc(self: &Arc<Self>, document_id: &DocumentId) -> Res<Option<BigDocHandle>> {
        self.find_doc_handle(document_id).await
    }

    pub async fn find_doc_handle(
        self: &Arc<Self>,
        document_id: &DocumentId,
    ) -> Res<Option<BigDocHandle>> {
        if let Some(bundle) = self.load_live_bundle(document_id).await? {
            return Ok(Some(BigDocHandle {
                repo: Arc::clone(self),
                bundle,
            }));
        }
        Ok(None)
    }

    pub async fn local_contains_document(self: &Arc<Self>, document_id: &DocumentId) -> Res<bool> {
        if self
            .live_bundles
            .get(document_id)
            .and_then(|entry| entry.value().upgrade())
            .is_some()
        {
            return Ok(true);
        }
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM big_repo_docs WHERE doc_id = ?")
            .bind(document_id.to_string())
            .fetch_one(&self.state_pool)
            .await
            .wrap_err("failed checking big_repo_docs")?;
        Ok(count > 0)
    }

    async fn load_live_bundle(
        self: &Arc<Self>,
        document_id: &DocumentId,
    ) -> Res<Option<Arc<LiveDocBundle>>> {
        if let Some(existing) = self
            .live_bundles
            .get(document_id)
            .and_then(|entry| entry.value().upgrade())
        {
            return Ok(Some(existing));
        }

        let Some(doc) = self.load_automerge(document_id).await? else {
            return Ok(None);
        };
        let bundle = Arc::new(LiveDocBundle::new(*document_id, doc));
        self.live_bundles
            .insert(*document_id, Arc::downgrade(&bundle));
        Ok(Some(bundle))
    }

    async fn upsert_known_doc(&self, doc_id: DocumentId) -> Res<()> {
        sqlx::query("INSERT INTO big_repo_docs(doc_id) VALUES(?) ON CONFLICT(doc_id) DO NOTHING")
            .bind(doc_id.to_string())
            .execute(&self.state_pool)
            .await
            .wrap_err("failed upserting big_repo_docs")?;
        Ok(())
    }

    async fn persist_full_bundle(&self, bundle: &LiveDocBundle) -> Res<()> {
        let doc = bundle.doc.lock().await;
        self.runtime.ingest_full(bundle.doc_id, doc.save()).await
    }

    pub(crate) async fn load_automerge(
        &self,
        doc_id: &DocumentId,
    ) -> Res<Option<automerge::Automerge>> {
        self.runtime.load_doc(*doc_id).await
    }

    async fn apply_commit_delta(
        &self,
        doc_id: DocumentId,
        commits: Vec<(CommitId, BTreeSet<CommitId>, Vec<u8>)>,
        heads: Vec<automerge::ChangeHash>,
        patches: Vec<automerge::Patch>,
        origin: BigRepoChangeOrigin,
    ) -> Res<()> {
        self.runtime
            .commit_delta(doc_id, commits, heads, patches, origin)
            .await
    }
}

// autosurgeon suport
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
        let handle = self.find_doc(doc_id).await?.ok_or_eyre("doc not found")?;
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
            .await
            .wrap_err("error on reconcile transaction")?;
        match res {
            Ok(success) => Ok(success.hash),
            Err(failure) => Err(ferr!("error on reconcile transaction: {failure:?}")),
        }
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
        handle
            .with_document_read(|doc| -> Res<Option<(T, Arc<[automerge::ChangeHash]>)>> {
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
        handle
            .with_document_read(|doc| -> Res<Option<(T, Arc<[automerge::ChangeHash]>)>> {
                let heads: Arc<[automerge::ChangeHash]> = Arc::from(heads.to_vec());
                if path.is_empty() && obj_id == automerge::ROOT {
                    let value: T =
                        autosurgeon::hydrate_at(doc, &heads).wrap_err("error hydrating")?;
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

pub struct BigRepoStopToken {
    runtime: runtime::BigRepoRuntimeHandle,
    change_manager_stop: Option<changes::ChangeListenerManagerStopToken>,
    partition_forwarder_cancel: CancellationToken,
    partition_forwarders: Arc<utils_rs::AbortableJoinSet>,
}

impl BigRepoStopToken {
    pub async fn stop(mut self) -> Res<()> {
        self.runtime.shutdown().await;
        self.partition_forwarder_cancel.cancel();
        match self.partition_forwarders.stop(Duration::from_secs(5)).await {
            Ok(()) => {}
            Err(utils_rs::AbortableJoinSetStopError::Timeout(_))
            | Err(utils_rs::AbortableJoinSetStopError::Aborted) => {
                // Subduction listener/manager tasks are long-lived service loops.
                // On process/repo shutdown we can continue after aborting them.
            }
            Err(err) => return Err(err.into()),
        }
        if let Some(stop_token) = self.change_manager_stop.take() {
            stop_token.stop().await?;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct BigDocHandle {
    repo: Arc<BigRepo>,
    bundle: Arc<LiveDocBundle>,
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
        &self.bundle.doc_id
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
        let mut doc = self.bundle.doc.lock().await;

        let before_heads = doc.get_heads();
        let out = operation(&mut doc);
        let after_heads = doc.get_heads();
        if before_heads == after_heads {
            return Ok(out);
        }

        let patches = doc.diff(&before_heads, &after_heads);
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

        self.repo
            .apply_commit_delta(
                *self.document_id(),
                changes,
                after_heads,
                patches,
                BigRepoChangeOrigin::Local,
            )
            .await?;

        Ok(out)
    }
}

async fn ensure_docs_schema(state_pool: &sqlx::SqlitePool) -> Res<()> {
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS big_repo_docs(
            doc_id TEXT PRIMARY KEY
        )"#,
    )
    .execute(state_pool)
    .await
    .wrap_err("failed creating big_repo_docs schema")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use automerge::{transaction::Transactable, ReadDoc, ScalarValue};
    use tokio::time::{timeout, Duration};

    async fn boot_repo() -> Res<(Arc<BigRepo>, BigRepoStopToken)> {
        BigRepo::boot(Config {
            peer_id: PeerId::new([7_u8; 32]),
            storage: StorageConfig::Memory,
        })
        .await
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

    #[tokio::test]
    async fn with_document_roundtrip_rehydrates_from_storage() -> Res<()> {
        let (repo, _stop_token) = boot_repo().await?;
        let mut doc = automerge::Automerge::new();
        doc.transact(|tx| tx.put(automerge::ROOT, "title", "before"))
            .expect("failed initializing title");

        let handle = repo.create_doc(doc).await?;
        let doc_id = *handle.document_id();
        handle
            .with_document(|doc| {
                doc.transact(|tx| tx.put(automerge::ROOT, "title", "after"))
                    .expect("failed mutating doc");
            })
            .await?;
        drop(handle);

        let reloaded = repo.find_doc(&doc_id).await?.expect("doc should exist");
        let title = reloaded
            .with_document_read(|doc| get_str_at_root(doc, "title"))
            .await;
        assert_eq!(title, "after");

        Ok(())
    }

    #[tokio::test]
    async fn with_document_emits_local_heads_without_background_gap() -> Res<()> {
        let (repo, _stop_token) = boot_repo().await?;
        let handle = repo.create_doc(automerge::Automerge::new()).await?;
        let doc_id = *handle.document_id();
        let (_registration, mut rx) = repo
            .subscribe_local_listener(BigRepoLocalFilter {
                doc_id: Some(BigRepoDocIdFilter::new(doc_id)),
            })
            .await?;

        handle
            .with_document(|doc| {
                doc.transact(|tx| tx.put(automerge::ROOT, "count", 1))
                    .expect("failed mutating doc");
            })
            .await?;

        let mut has_doc_heads_updated = false;
        for _ in 0..4 {
            let batch = timeout(Duration::from_secs(1), rx.recv())
                .await
                .expect("timed out waiting for local head update")
                .expect("local listener closed unexpectedly");
            has_doc_heads_updated = batch.into_iter().any(
                |item| matches!(item, BigRepoLocalNotification::DocHeadsUpdated { doc_id: seen_doc_id, .. } if seen_doc_id == doc_id),
            );
            if has_doc_heads_updated {
                break;
            }
        }
        assert!(has_doc_heads_updated);

        Ok(())
    }

    #[tokio::test]
    async fn with_document_handles_concurrent_writers() -> Res<()> {
        let (repo, _stop_token) = boot_repo().await?;
        let handle = repo.create_doc(automerge::Automerge::new()).await?;
        let doc_id = *handle.document_id();
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
                    .find_doc(&doc_id)
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
            .find_doc(&doc_id)
            .await?
            .expect("doc should exist")
            .with_document_read(|doc| get_int_at_root(doc, "count"))
            .await;
        assert_eq!(final_count, (writer_count * increments_per_writer) as i64);

        Ok(())
    }

    #[tokio::test]
    async fn subscribe_change_listener_does_not_hydrate_known_docs() -> Res<()> {
        let (repo, _stop_token) = boot_repo().await?;
        for idx in 0..3 {
            let mut doc = automerge::Automerge::new();
            doc.transact(|tx| tx.put(automerge::ROOT, "idx", idx))
                .expect("failed initializing doc");
            let handle = repo.create_doc(doc).await?;
            drop(handle);
        }

        repo.live_bundles.clear();
        assert_eq!(repo.live_bundles.len(), 0);

        let (_registration, _rx) = repo
            .subscribe_change_listener(BigRepoChangeFilter {
                doc_id: None,
                origin: None,
                path: Vec::new(),
            })
            .await?;

        assert_eq!(
            repo.live_bundles.len(),
            0,
            "subscription setup should not hydrate documents into live cache",
        );

        Ok(())
    }

    #[tokio::test]
    async fn subscribe_head_listener_does_not_hydrate_known_docs() -> Res<()> {
        let (repo, _stop_token) = boot_repo().await?;
        for idx in 0..3 {
            let mut doc = automerge::Automerge::new();
            doc.transact(|tx| tx.put(automerge::ROOT, "idx", idx))
                .expect("failed initializing doc");
            let handle = repo.create_doc(doc).await?;
            drop(handle);
        }

        repo.live_bundles.clear();
        assert_eq!(repo.live_bundles.len(), 0);

        let (_registration, _rx) = repo
            .subscribe_head_listener(BigRepoHeadFilter { doc_id: None })
            .await?;

        assert_eq!(
            repo.live_bundles.len(),
            0,
            "head subscription setup should not hydrate documents into live cache",
        );

        Ok(())
    }

    #[tokio::test]
    async fn create_doc_emits_change_notifications_without_manual_broker_pin() -> Res<()> {
        let (repo, _stop_token) = boot_repo().await?;
        let (_registration, mut rx) = repo
            .subscribe_change_listener(BigRepoChangeFilter {
                doc_id: None,
                origin: None,
                path: Vec::new(),
            })
            .await?;

        let handle = repo.create_doc(automerge::Automerge::new()).await?;
        let doc_id = *handle.document_id();
        let batch = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("timed out waiting for create notification")
            .expect("change listener closed unexpectedly");
        assert!(batch.into_iter().any(|n| {
            matches!(
                n,
                BigRepoChangeNotification::DocCreated {
                    doc_id: seen_doc_id,
                    ..
                } if seen_doc_id == doc_id
            )
        }));
        Ok(())
    }

    #[tokio::test]
    async fn import_doc_emits_change_notifications_without_manual_broker_pin() -> Res<()> {
        let (repo, _stop_token) = boot_repo().await?;
        let (_registration, mut rx) = repo
            .subscribe_change_listener(BigRepoChangeFilter {
                doc_id: None,
                origin: None,
                path: Vec::new(),
            })
            .await?;

        let imported_doc_id = DocumentId::random();
        let _handle = repo
            .import_doc(imported_doc_id, automerge::Automerge::new())
            .await?;

        let batch = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("timed out waiting for import notification")
            .expect("change listener closed unexpectedly");
        assert!(batch.into_iter().any(|n| {
            matches!(
                n,
                BigRepoChangeNotification::DocImported {
                    doc_id: seen_doc_id,
                    ..
                } if seen_doc_id == imported_doc_id
            )
        }));
        Ok(())
    }
}
