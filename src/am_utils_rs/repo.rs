use crate::interlude::*;

use std::collections::BTreeSet;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};

use automerge::ChangeHash;
use autosurgeon::{Hydrate, Prop, Reconcile};
use sedimentree_core::loose_commit::id::CommitId;
use sqlx::sqlite::SqliteConnectOptions;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

mod changes;
mod partition;
pub mod rpc;
mod runtime;
pub use runtime::SyncDocOutcome;

use crate::partition::PartitionStore;
use crate::sync::protocol::*;

pub use changes::{
    path_prefix_matches as big_repo_path_prefix_matches, BigRepoChangeNotification,
    BigRepoChangeOrigin, ChangeFilter as BigRepoChangeFilter,
    ChangeListenerRegistration as BigRepoChangeListenerRegistration,
    DocIdFilter as BigRepoDocIdFilter, OriginFilter as BigRepoOriginFilter,
};

pub type DocumentId = crate::ids::DocId32;
pub type PeerId = crate::ids::PeerId32;

// FIXME: this is used by SubdctionProtocolHandler in core::sync
// should that be moved here instead? Is that needeed in the scope
// of subduction_iroh. Does it not provide it's own Router protocol?
//
// hmm, we already have RuntimeIrohTransport. I suspect the one in core, it's vestigial
pub const SUBDUCTION_ALPN: &[u8] = b"subduction/0";

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

// FIXME: so essentially, we moved from a single live_buldes mutex to a per bundle mutex righ? 
// let's move this to runtime
#[derive(Debug)]
struct LiveDocBundle {
    doc_id: DocumentId,
    doc: tokio::sync::Mutex<automerge::Automerge>,
    _lease: runtime::RuntimeDocLease,
}

impl LiveDocBundle {
    fn new(doc_id: DocumentId, doc: automerge::Automerge, lease: runtime::RuntimeDocLease) -> Self {
        Self {
            doc_id,
            doc: tokio::sync::Mutex::new(doc),
            _lease: lease,
        }
    }
}

#[derive(educe::Educe)]
#[educe(Debug)]
pub struct BigRepo {
    local_peer_id: PeerId,
    #[educe(Debug(ignore))]
    partition_store: Arc<PartitionStore>,
    #[educe(Debug(ignore))]
    runtime: runtime::BigRepoRuntimeHandle,
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

#[derive(Clone)]
pub struct BigRepoConnection {
    repo: Arc<BigRepo>,
    peer_id: PeerId,
    closed: Arc<AtomicBool>,
}

impl BigRepoConnection {
    pub fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    fn mark_closed(&self) -> bool {
        !self.closed.swap(true, Ordering::SeqCst)
    }

    pub async fn close(&self) -> Res<()> {
        if !self.mark_closed() {
            return Ok(());
        }
        self.repo.runtime.close_peer_connection(self.peer_id).await
    }

    pub async fn sync_doc_with_peer(
        &self,
        doc_id: DocumentId,
        subscribe: bool,
        timeout: Option<std::time::Duration>,
    ) -> Res<SyncDocOutcome> {
        if self.is_closed() {
            eyre::bail!("connection is closed");
        }
        self.repo
            .runtime
            .sync_doc_with_peer(doc_id, self.peer_id, subscribe, timeout)
            .await
    }
}

// FIXME: let's replace this with a stop token instead
// similar to the main branch (actually check how we did it there)
impl Drop for BigRepoConnection {
    fn drop(&mut self) {
        if !self.mark_closed() {
            return;
        }
        self.repo
            .runtime
            .request_close_peer_connection(self.peer_id);
    }
}

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

        // FIXME: let's make PartitionStore::new into PartitionStore::boot
        // and have it be async and ensure it's own schema and
        // PartitionStore should return a stop token meaning
        // it should also manage it's own join_set and CancellationToken
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
        let (runtime, runtime_stop) = match storage {
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
            partition_store,
            runtime,
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
                runtime_stop,
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

}

// main methods
impl BigRepo {
    pub async fn get_doc(self: &Arc<Self>, document_id: &DocumentId) -> Res<Option<BigDocHandle>> {
        Ok(self
            .runtime
            .get_doc_handle(*document_id)
            .await?
            .map(|bundle| BigDocHandle {
                repo: Arc::clone(self),
                bundle,
            }))
    }

    pub async fn put_doc(
        self: &Arc<Self>,
        document_id: DocumentId,
        initial_content: automerge::Automerge,
    ) -> Res<BigDocHandle> {
        let bundle = self.runtime.put_doc(document_id, initial_content).await?;
        Ok(BigDocHandle {
            repo: Arc::clone(self),
            bundle,
        })
    }

    pub async fn export_doc(&self, doc_id: &DocumentId) -> Res<Option<Vec<u8>>> {
        self.runtime.export_doc_save(*doc_id).await
    }

    pub async fn connect_with_peer(
        self: &Arc<Self>,
        endpoint: iroh::Endpoint,
        endpoint_addr: iroh::EndpointAddr,
        peer_id: PeerId,
    ) -> Res<BigRepoConnection> {
        self.runtime
            .ensure_peer_connection(endpoint, endpoint_addr, peer_id)
            .await?;
        Ok(BigRepoConnection {
            repo: Arc::clone(self),
            peer_id,
            closed: Arc::new(AtomicBool::new(false)),
        })
    }

    pub async fn accept_peer_connection(
        self: &Arc<Self>,
        quic_conn: iroh::endpoint::Connection,
    ) -> Res<BigRepoConnection> {
        let peer_id = self.runtime.accept_incoming_connection(quic_conn).await?;
        Ok(BigRepoConnection {
            repo: Arc::clone(self),
            peer_id,
            closed: Arc::new(AtomicBool::new(false)),
        })
    }
}

// change listeners
impl BigRepo {
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

// partition support
impl BigRepo {
    pub async fn partition_member_count(&self, part_id: &PartitionId) -> Res<i64> {
        self.partition_store.member_count(part_id).await
    }

    pub async fn is_member_present_in_partition_item_state(
        &self,
        partition_id: &PartitionId,
        member_id: &str,
    ) -> Res<bool> {
        self.partition_store
            .is_member_present_in_item_state(partition_id, member_id)
            .await
    }

    pub async fn list_partitions_for_peer(&self, peer: &PeerKey) -> Res<Vec<PartitionSummary>> {
        Ok(self
            .partition_store
            .list_partitions_for_peer(peer)
            .await?
            .partitions)
    }

    pub async fn get_partition_member_events_for_peer(
        &self,
        peer: &PeerKey,
        req: &GetPartitionMemberEventsRequest,
    ) -> Res<GetPartitionMemberEventsResponse> {
        self.partition_store
            .get_partition_member_events_for_peer(peer, req)
            .await
    }

    pub async fn get_partition_doc_events_for_peer(
        &self,
        peer: &PeerKey,
        req: &GetPartitionDocEventsRequest,
    ) -> Res<GetPartitionDocEventsResponse> {
        self.partition_store
            .get_partition_doc_events_for_peer(peer, req)
            .await
    }

    pub async fn subscribe_partition_events_for_peer(
        &self,
        peer: &PeerKey,
        reqs: &SubPartitionsRequest,
        capacity: usize,
    ) -> Res<tokio::sync::mpsc::Receiver<SubscriptionItem>> {
        self.partition_store
            .subscribe_partition_events_for_peer(peer, reqs, capacity)
            .await
    }

    pub async fn get_docs_full_in_partitions(
        &self,
        doc_ids: &[String],
        allowed_partitions: &[PartitionId],
    ) -> Res<Vec<FullDoc>> {
        if doc_ids.len() > MAX_GET_DOCS_FULL_DOC_IDS {
            return Err(PartitionSyncError::TooManyDocIds {
                requested: doc_ids.len(),
                max: MAX_GET_DOCS_FULL_DOC_IDS,
            }
            .into());
        }

        let mut dedup = HashSet::new();
        let requested_doc_ids: Vec<String> = doc_ids
            .iter()
            .filter(|doc_id| dedup.insert((*doc_id).clone()))
            .cloned()
            .collect();
        let denied_doc_id = self
            .find_first_inaccessible_doc_in_partitions(&requested_doc_ids, allowed_partitions)
            .await?;
        if let Some(denied) = denied_doc_id {
            return Err(PartitionSyncError::DocAccessDenied { doc_id: denied }.into());
        }

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

    pub async fn is_doc_accessible_in_partitions(
        &self,
        doc_id: &str,
        allowed_partitions: &[PartitionId],
    ) -> Res<bool> {
        self.partition_store
            .is_item_present_in_membership_partitions(doc_id, allowed_partitions)
            .await
    }

    async fn find_first_inaccessible_doc_in_partitions(
        &self,
        doc_ids: &[String],
        allowed_partitions: &[PartitionId],
    ) -> Res<Option<String>> {
        self.partition_store
            .find_first_item_missing_membership_in_partitions(doc_ids, allowed_partitions)
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
        let handle = self.get_doc(doc_id).await?.ok_or_eyre("doc not found")?;
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
        let handle = self.get_doc(doc_id).await?.ok_or_eyre("doc not found")?;
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
        let handle = self.get_doc(doc_id).await?.ok_or_eyre("doc not found")?;
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
    runtime_stop: runtime::BigRepoRuntimeStopToken,
    change_manager_stop: Option<changes::ChangeListenerManagerStopToken>,
    partition_forwarder_cancel: CancellationToken,
    partition_forwarders: Arc<utils_rs::AbortableJoinSet>,
}

impl BigRepoStopToken {
    pub async fn stop(mut self) -> Res<()> {
        self.runtime_stop.stop().await?;
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

        let doc_save = doc.save();
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
            .has_change_listener_interest(*self.document_id(), &origin)
        {
            doc.diff(&before_heads, &after_heads)
        } else {
            Vec::new()
        };

        self.repo
            .runtime
            .commit_delta(*self.document_id(), changes, after_heads, patches, origin)
            .await?;

        // FIXME: this is a bug, why are we ingesting in addition
        // to sending the commit delta? why are we saving??
        //
        // THIS IS VERY BROKEN. We shouldn't need this.
        // and this is the only use place of ingest full,
        // we should remove it then
        self.repo
            .runtime
            .ingest_full(*self.document_id(), doc_save)
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
    use autosurgeon::Prop;
    use std::sync::atomic::AtomicBool;
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

    async fn recv_change_batch(
        rx: &mut tokio::sync::mpsc::UnboundedReceiver<Vec<BigRepoChangeNotification>>,
    ) -> Vec<BigRepoChangeNotification> {
        timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("timed out waiting for change batch")
            .expect("change listener closed unexpectedly")
    }

    #[tokio::test]
    async fn put_doc_get_doc_and_export_roundtrip() -> Res<()> {
        let (repo, _stop_token) = boot_repo().await?;
        let doc_id = DocumentId::random();
        let mut doc = automerge::Automerge::new();
        doc.transact(|tx| tx.put(automerge::ROOT, "title", "seed"))
            .expect("failed seeding doc");

        let handle = repo.put_doc(doc_id, doc).await?;
        let fetched = repo.get_doc(&doc_id).await?.expect("doc should exist");
        assert_eq!(fetched.document_id(), &doc_id);
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
    async fn put_doc_rejects_existing_local_doc_id() -> Res<()> {
        let (repo, _stop_token) = boot_repo().await?;
        let doc_id = DocumentId::random();
        let _ = repo.put_doc(doc_id, automerge::Automerge::new()).await?;
        let err = repo
            .put_doc(doc_id, automerge::Automerge::new())
            .await
            .expect_err("expected conflict");
        assert!(err.to_string().contains("already exists locally"));
        Ok(())
    }

    #[tokio::test]
    async fn connection_close_is_idempotent() -> Res<()> {
        let (repo, _stop_token) = boot_repo().await?;
        let connection = BigRepoConnection {
            repo,
            peer_id: PeerId::new([9_u8; 32]),
            closed: Arc::new(AtomicBool::new(false)),
        };

        connection.close().await?;
        connection.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn with_document_roundtrip_rehydrates_from_storage() -> Res<()> {
        let (repo, _stop_token) = boot_repo().await?;
        let mut doc = automerge::Automerge::new();
        doc.transact(|tx| tx.put(automerge::ROOT, "title", "before"))
            .expect("failed initializing title");

        let doc_id = DocumentId::random();
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
        let (repo, _stop_token) = boot_repo().await?;
        let first_handle = repo
            .put_doc(DocumentId::random(), automerge::Automerge::new())
            .await?;
        let first_doc_id = *first_handle.document_id();
        let second_handle = repo
            .put_doc(DocumentId::random(), automerge::Automerge::new())
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
        let (repo, _stop_token) = boot_repo().await?;
        let handle = repo
            .put_doc(DocumentId::random(), automerge::Automerge::new())
            .await?;
        let doc_id = *handle.document_id();

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
        let (repo, _stop_token) = boot_repo().await?;
        let (_registration, mut rx) = repo
            .subscribe_change_listener(BigRepoChangeFilter {
                doc_id: None,
                origin: Some(BigRepoOriginFilter::Local),
                path: Vec::new(),
            })
            .await?;

        let handle = repo
            .put_doc(DocumentId::random(), automerge::Automerge::new())
            .await?;
        let doc_id = *handle.document_id();

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
    async fn with_document_handles_concurrent_writers() -> Res<()> {
        let (repo, _stop_token) = boot_repo().await?;
        let handle = repo
            .put_doc(DocumentId::random(), automerge::Automerge::new())
            .await?;
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
}
