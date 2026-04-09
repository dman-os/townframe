use crate::interlude::*;
use crate::partition::PartitionStore;

use automerge::ChangeHash;
use autosurgeon::{Hydrate, Prop, Reconcile};
use sqlx::sqlite::SqliteConnectOptions;
use utils_rs::prelude::futures::future::BoxFuture;
use std::str::FromStr;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

mod changes;
pub mod iroh;
mod partition;
pub mod rpc;

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

/// Configuration for Automerge storage
#[derive(Debug, Clone)]
pub struct Config {
    pub peer_id: PeerId,
    /// Storage directory for Automerge documents
    pub storage: StorageConfig,
}

#[derive(Debug, Clone)]
pub enum StorageConfig {
    Disk { path: PathBuf },
    Memory,
}

#[derive(Debug, Clone)]
struct SubductionMirror {}

impl SubductionMirror {}

#[derive(educe::Educe)]
#[educe(Debug)]
pub struct BigRepo {
    #[educe(Debug(ignore))]
    state_pool: sqlx::SqlitePool,
    #[educe(Debug(ignore))]
    partition_store: Arc<PartitionStore>,
    #[educe(Debug(ignore))]
    change_manager: Arc<changes::ChangeListenerManager>,
    #[educe(Debug(ignore))]
    partition_forwarder_cancel: CancellationToken,
    #[educe(Debug(ignore))]
    join_set: Arc<utils_rs::AbortableJoinSet>,
    #[educe(Debug(ignore))]
    change_manager_stop: std::sync::Mutex<Option<changes::ChangeListenerManagerStopToken>>,
    #[educe(Debug(ignore))]
    persistent_change_brokers:
        std::sync::Mutex<std::collections::HashMap<DocumentId, Arc<changes::DocChangeBrokerLease>>>,
    // #[educe(Debug(ignore))]
    // subduction_storage: sedimentree_fs_storage::FsStorage,
    // #[educe(Debug(ignore))]
    // subduction_signer: subduction_crypto::signer::memory::MemorySigner,
}

pub type SharedBigRepo = Arc<BigRepo>;

impl BigRepo {
    pub async fn boot(config: Config) -> Res<(Arc<Self>, BigRepoStopToken)> {
        let Config { peer_id, storage } = config;
        let (subduction_storage, sqlite_url) = match storage {
            StorageConfig::Memory => (
                Box::new(
                    subduction_core::storage::memory::MemoryStorage::new()
                ) as Box<dyn subduction_core::storage::traits::Storage + Send + Sync + 'static>,
                "sqlite::memory".to_string(),
            ),
            StorageConfig::Disk { path } => {
                let subduction_dir = path.join("subduction");
                std::fs::create_dir_all(&subduction_dir).wrap_err_with(|| {
                    format!("Failed to create storage directory: {}", path.display())
                })?;
                (
                    Box::new(sedimentree_fs_storage::FsStorage::new(subduction_dir)
                        .wrap_err("failed booting subduction fs storage")?),
                    format!("sqlite://{}", path.join("big_repo.sqlite").display()),
                )
            }, 
        };
        // let keyhive = keyhive_core::
        // subduction_keyhive_policy::SubductionKeyhive::new()
        let join_set = Arc::new(utils_rs::AbortableJoinSet::new());

        // let (subduction, handler, listener, manager) = subduction_core::subduction::builder::SubductionBuilder::new()
        //         .storage(subduction_core::storage::memory::MemoryStorage::new(), Arc::new(subduction_core::policy::open::OpenPolicy))
        //         .spawner(AbortableTokioSpawn{ set: Arc::clone(&join_set) })
        //         .signer(subduction_crypto::signer::memory::MemorySigner::from_bytes(peer_id.as_bytes()))
        //         .timer(TimeoutTokio)
        //         .build::<future_form::Sendable>()
        //     ;
        // join_set.spawn(listener)?;
        // join_set.spawn(manager)?;
        let storebox = subduction_core::storage::powerbox::StoragePowerbox::new(
            subduction_core::storage::memory::MemoryStorage::new(),
            Arc::new(subduction_core::policy::open::OpenPolicy)
        );

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

        let out = Arc::new(Self {
            state_pool,
            partition_store,
            change_manager,
            partition_forwarder_cancel,
            join_set,
            change_manager_stop: std::sync::Mutex::new(Some(change_manager_stop)),
            persistent_change_brokers: std::sync::Mutex::new(default()),
        });
        let (_head_reg, mut head_rx) = out
            .subscribe_head_listener(BigRepoHeadFilter { doc_id: None })
            .await?;
        {
            let cancel_token = out.partition_forwarder_cancel.child_token();
            let repo = Arc::downgrade(&out);
            out.join_set
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
                                        if !matches!(origin, BigRepoChangeOrigin::Remote { .. }) {
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
        let change_manager_stop = out
            .change_manager_stop
            .lock()
            .expect(ERROR_MUTEX)
            .take()
            .expect("BigRepo change manager stop token missing");
        Ok((
            Arc::clone(&out),
            BigRepoStopToken {
                change_manager_stop: Some(change_manager_stop),
                partition_forwarder_cancel: out.partition_forwarder_cancel.clone(),
                partition_forwarders: Arc::clone(&out.join_set),
            },
        ))
    }

    pub fn partition_store(&self) -> Arc<PartitionStore> {
        Arc::clone(&self.partition_store)
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

    pub async fn ensure_change_broker(
        self: &Arc<Self>,
        handle: BigDocHandle,
    ) -> Res<Arc<changes::DocChangeBrokerLease>> {
        self.change_manager
            .add_doc_listener(handle.document_id().clone())
            .await
    }

    async fn ensure_persistent_change_broker(
        self: &Arc<Self>,
        handle: BigDocHandle,
    ) -> Res<Arc<changes::DocChangeBrokerLease>> {
        let doc_id = handle.document_id().clone();
        if let Some(existing) = self
            .persistent_change_brokers
            .lock()
            .expect(ERROR_MUTEX)
            .get(&doc_id)
            .cloned()
        {
            return Ok(existing);
        }
        let lease = self.ensure_change_broker(handle).await?;
        lease.wait_until_ready().await;
        let mut persistent = self.persistent_change_brokers.lock().expect(ERROR_MUTEX);
        if let Some(existing) = persistent.get(&doc_id).cloned() {
            return Ok(existing);
        }
        persistent.insert(doc_id, Arc::clone(&lease));
        Ok(lease)
    }

    async fn ensure_persistent_change_brokers_for_known_docs(
        self: &Arc<Self>,
    ) -> Res<Vec<Arc<changes::DocChangeBrokerLease>>> {
        let doc_ids = self.partition_store.list_known_item_ids().await?;
        let mut leases = Vec::new();
        for raw_doc_id in doc_ids {
            let Ok(doc_id) = DocumentId::from_str(&raw_doc_id) else {
                debug!(
                    raw_doc_id,
                    "skipping non-document partition item while bootstrapping persistent change brokers"
                );
                continue;
            };
            if let Some(handle) = self.find_doc_handle(&doc_id).await? {
                let lease = self.ensure_persistent_change_broker(handle).await?;
                leases.push(lease);
            }
        }
        Ok(leases)
    }

    pub async fn subscribe_change_listener(
        self: &Arc<Self>,
        filter: BigRepoChangeFilter,
    ) -> Res<(
        BigRepoChangeListenerRegistration,
        tokio::sync::mpsc::UnboundedReceiver<Vec<BigRepoChangeNotification>>,
    )> {
        let broker_leases = if let Some(target_doc) = filter.doc_id.as_ref() {
            let mut leases = Vec::new();
            let handle = self.find_doc_handle(&target_doc.doc_id).await?;
            if let Some(handle) = handle {
                leases.push(self.ensure_persistent_change_broker(handle).await?);
            }
            leases
        } else {
            self.ensure_persistent_change_brokers_for_known_docs()
                .await?
        };
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
        let broker_leases = if let Some(target_doc) = filter.doc_id.as_ref() {
            let mut leases = Vec::new();
            let handle = self.find_doc_handle(&target_doc.doc_id).await?;
            if let Some(handle) = handle {
                leases.push(self.ensure_persistent_change_broker(handle).await?);
            }
            leases
        } else {
            self.ensure_persistent_change_brokers_for_known_docs()
                .await?
        };
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
        };
        let _lease = self.ensure_persistent_change_broker(out.clone()).await?;
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
        };
        let _lease = self.ensure_persistent_change_broker(out.clone()).await?;
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
        }))
    }

    pub async fn add_doc(
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
        };
        let _lease = self.ensure_persistent_change_broker(out.clone()).await?;
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

    pub async fn find_doc_handle(
        self: &Arc<Self>,
        document_id: &DocumentId,
    ) -> Res<Option<BigDocHandle>> {
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
        }))
    }
    //
    // pub async fn watch_doc_peer_states(
    //     self: &Arc<Self>,
    //     document_id: &DocumentId,
    // ) -> Res<Option<(BigRepoDocPeerStateView, BigRepoDocPeerStateStream)>> {
    //     use futures::StreamExt as _;
    //
    //     let Some(handle) = self.find_doc_handle(document_id).await? else {
    //         return Ok(None);
    //     };
    //     let (peer_state, state_stream) = handle.inner.peers();
    //     Ok(Some((peer_state, state_stream.boxed())))
    // }

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
        let heads_arc = Arc::<[automerge::ChangeHash]>::from(heads.clone());
        self.change_manager.notify_doc_heads_changed(
            doc_id.clone(),
            Arc::clone(&heads_arc),
            BigRepoChangeOrigin::Local,
        )?;
        self.change_manager
            .notify_local_doc_heads_updated(doc_id.clone(), heads_arc)?;
        self.record_doc_heads_change(doc_id, heads).await
    }

    fn on_doc_patches_changed(
        &self,
        doc_id: &DocumentId,
        heads: Vec<automerge::ChangeHash>,
        patches: Vec<automerge::Patch>,
    ) -> Res<()> {
        let heads_arc = Arc::<[automerge::ChangeHash]>::from(heads);
        for patch in patches {
            self.change_manager.notify_doc_changed(
                doc_id.clone(),
                Arc::new(patch),
                Arc::clone(&heads_arc),
                BigRepoChangeOrigin::Local,
            )?;
        }
        Ok(())
    }

    async fn mirror_doc_into_subduction(&self, doc_id: &DocumentId) -> Res<()> {
        let Some(mirror) = &self.subduction_mirror else {
            return Ok(());
        };
        let exported = match self.repo.local_export(doc_id.clone()).await {
            Ok(exported) => exported,
            Err(samod::LocalExportError::NotFound { .. }) => return Ok(()),
            Err(err) => {
                return Err(eyre::Report::from(err).wrap_err("failed local-exporting doc"));
            }
        };
        mirror.persist_automerge(doc_id, &exported).await
    }
}

// subduction helpers
impl BigRepo {
    async fn persist_automerge(&self, doc_id: &DocumentId, doc: &automerge::Automerge) -> Res<()> {
        use sedimentree_core::{
            blob::{verified::VerifiedBlobMeta, Blob},
            loose_commit::id::CommitId,
        };
        use std::collections::BTreeSet;
        use subduction_core::storage::traits::Storage;
        use subduction_crypto::verified_meta::VerifiedMeta;

        let sedimentree_id = doc_id.into();
        let res = automerge_sedimentree::ingest::ingest_automerge(doc, sedimentree_id)
            .wrap_err("error ingesting automerge into sedimentree")?;

        self.subduction_storage.save_batch(sedimentree_id, res.sedimentree)
        <sedimentree_fs_storage::FsStorage as Storage<future_form::Sendable>>::save_sedimentree_id(
            &self.subduction_storage,
            sedimentree_id,
        )
        .await
        .map_err(|err| ferr!("failed persisting sedimentree id: {err}"))?;
        <sedimentree_fs_storage::FsStorage as Storage<future_form::Sendable>>::delete_loose_commits(
            &self.subduction_storage,
            sedimentree_id,
        )
        .await
            .map_err(|err| ferr!("failed clearing prior loose commits: {err}"))?;
        <sedimentree_fs_storage::FsStorage as Storage<future_form::Sendable>>::delete_fragments(
            &self.subduction_storage,
            sedimentree_id,
        )
        .await
        .map_err(|err| ferr!("failed clearing prior fragments: {err}"))?;

        for change in doc.get_changes(&[]) {
            let head = CommitId::new(change.hash().0);
            let parents = change
                .deps()
                .iter()
                .map(|dep| CommitId::new(dep.0))
                .collect::<BTreeSet<_>>();
            let blob = Blob::new(change.raw_bytes().to_vec());
            let verified_blob = VerifiedBlobMeta::new(blob);
            let verified = VerifiedMeta::seal::<future_form::Sendable, _>(
                &self.signer,
                (sedimentree_id, head, parents),
                verified_blob,
            )
            .await;
            <sedimentree_fs_storage::FsStorage as Storage<future_form::Sendable>>::save_loose_commit(
                &self.storage,
                sedimentree_id,
                verified,
            )
            .await
            .map_err(|err| ferr!("failed saving loose commit: {err}"))?;
        }
        Ok(())
    }

    async fn load_automerge(&self, doc_id: &DocumentId) -> Res<Option<automerge::Automerge>> {
        use subduction_core::storage::traits::Storage;

        let sedimentree_id = doc_id.into();
        let loose =
            <sedimentree_fs_storage::FsStorage as Storage<future_form::Sendable>>::load_loose_commits(
                &self.storage,
                sedimentree_id,
            )
            .await
            .map_err(|err| ferr!("failed loading loose commits: {err}"))?;
        let fragments =
            <sedimentree_fs_storage::FsStorage as Storage<future_form::Sendable>>::load_fragments(
                &self.storage,
                sedimentree_id,
            )
            .await
            .map_err(|err| ferr!("failed loading fragments: {err}"))?;
        if loose.is_empty() && fragments.is_empty() {
            return Ok(None);
        }

        let mut pending: Vec<Vec<u8>> = fragments
            .into_iter()
            .map(|item| item.blob().as_slice().to_vec())
            .chain(
                loose
                    .into_iter()
                    .map(|item| item.blob().as_slice().to_vec()),
            )
            .collect::<Vec<_>>();
        let mut doc = automerge::Automerge::new();
        while !pending.is_empty() {
            let mut next = Vec::new();
            let mut progressed = false;
            for blob in pending {
                match doc.load_incremental(&blob) {
                    Ok(_) => progressed = true,
                    Err(_) => next.push(blob),
                }
            }
            if !progressed {
                eyre::bail!("failed reconstructing automerge doc from subduction blobs");
            }
            pending = next;
        }
        Ok(Some(doc))
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
        let handle = self.find_doc(doc_id).await?.ok_or_eyre("doc not found")?;
        let res = handle
            .with_document_local(|doc| {
                // FIXME: consider re-setting found actor id at the end
                // or better yet, use big repo wide actor id defaults
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
        handle.with_document_sync(|doc| -> Res<Option<(T, Arc<[automerge::ChangeHash]>)>> {
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
        handle.with_document_sync(|doc| -> Res<Option<(T, Arc<[automerge::ChangeHash]>)>> {
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
        self.repo.stop().await;
        if let Some(stop_token) = self.change_manager_stop.take() {
            stop_token.stop().await?;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct BigDocHandle {
    repo: Arc<BigRepo>,
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

    pub fn with_document_sync<F, R>(&self, operation: F) -> R
    where
        F: FnOnce(&mut automerge::Automerge) -> R,
    {
        self.with_document(operation)
    }

    // pub fn raw_handle(&self) -> &samod::DocHandle {
    //     &self.inner
    // }

    pub fn with_document<F, R>(&self, operation: F) -> R
    where
        F: FnOnce(&mut automerge::Automerge) -> R,
    {
        let (before_heads, out, after_heads, patches) = self.inner.with_document(|doc| {
            let before_heads = doc.get_heads();
            let out = operation(doc);
            let after_heads = doc.get_heads();
            let patches = if before_heads != after_heads {
                doc.diff(&before_heads, &after_heads)
            } else {
                Vec::new()
            };
            (before_heads, out, after_heads, patches)
        });
        if before_heads != after_heads {
            let repo = Arc::clone(&self.repo);
            let doc_id = self.document_id().clone();
            tokio::spawn(async move {
                repo.on_doc_patches_changed(&doc_id, after_heads.clone(), patches)
                    .unwrap();
                repo.on_doc_heads_changed(&doc_id, after_heads)
                    .await
                    .unwrap();
            });
        }
        out
    }

    /// WARN: do not use this over join! or select!, it blocks the
    /// current tokio task while running document access inline.
    pub async fn with_document_local<F, R>(&self, operation: F) -> Res<R>
    where
        F: FnOnce(&mut automerge::Automerge) -> R,
    {
        let (before_heads, out, after_heads, patches) = self.inner.with_document(|doc| {
            let before_heads = doc.get_heads();
            let out = operation(doc);
            let after_heads = doc.get_heads();
            let patches = if before_heads != after_heads {
                doc.diff(&before_heads, &after_heads)
            } else {
                Vec::new()
            };
            (before_heads, out, after_heads, patches)
        });
        if before_heads != after_heads {
            self.repo
                .on_doc_patches_changed(self.document_id(), after_heads.clone(), patches)?;
            self.repo
                .on_doc_heads_changed(self.document_id(), after_heads)
                .await?;
        }
        Ok(out)
    }

    pub fn peers(
        &self,
    ) -> (
        std::collections::HashMap<samod::ConnectionId, samod::PeerDocState>,
        futures::stream::BoxStream<
            'static,
            std::collections::HashMap<samod::ConnectionId, samod::PeerDocState>,
        >,
    ) {
        use futures::StreamExt as _;

        let (peer_state, state_stream) = self.inner.peers();
        (peer_state, state_stream.boxed())
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

    #[cfg(unix)]
    #[tokio::test]
    async fn boot_rejects_non_utf8_storage_path() -> Res<()> {
        use std::os::unix::ffi::OsStringExt;

        let non_utf8_component = std::ffi::OsString::from_vec(vec![0xff, b'x']);
        let storage_path = std::env::temp_dir().join(std::path::PathBuf::from(non_utf8_component));
        let res = BigRepo::boot(Config {
            peer_id: "bigrepo-nonutf8".to_string(),
            storage: StorageConfig::Disk {
                path: storage_path,
            },
        })
        .await;
        let err = match res {
            Ok(_) => eyre::bail!("boot should reject non-UTF8 storage paths"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("storage path contains invalid UTF-8"),
            "unexpected error: {err:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn change_and_head_listener_subscriptions_bootstrap_brokers() -> Res<()> {
        let repo = boot_big_repo("broker-subscribe").await?;
        let handle = repo.create_doc(automerge::Automerge::new()).await?;
        let target_doc_id = handle.document_id().clone();

        let (_change_reg, _change_rx) = repo
            .subscribe_change_listener(BigRepoChangeFilter {
                doc_id: None,
                origin: None,
                path: vec![],
            })
            .await?;
        let (_head_reg, _head_rx) = repo
            .subscribe_head_listener(BigRepoHeadFilter { doc_id: None })
            .await?;

        assert!(
            repo.persistent_change_brokers
                .lock()
                .expect(ERROR_MUTEX)
                .contains_key(&target_doc_id),
            "global change/head listener subscriptions should keep doc brokers alive"
        );
        Ok(())
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
        src_handle.with_document(|doc| {
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "x", 1_i64)
                .expect("failed writing source doc");
            tx.commit();
        });
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
    async fn import_doc_updates_partition_doc_state() -> Res<()> {
        let part_id = "fast-import-part".to_string();
        let src = boot_big_repo("fast-import-src").await?;
        let dst = boot_big_repo("fast-import-dst").await?;
        let _partition_events_rx = dst.subscribe_partition_events();

        let src_handle = src.create_doc(automerge::Automerge::new()).await?;
        src_handle.with_document(|doc| {
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "x", 1_i64)
                .expect("failed writing source doc");
            tx.commit();
        });
        let doc_id = src_handle.document_id().clone();
        let exported = src
            .samod_repo()
            .local_export(doc_id.clone())
            .await
            .map_err(|err| ferr!("failed exporting source doc: {err}"))?;

        dst.partition_store()
            .add_member(&part_id, &doc_id.to_string(), &serde_json::json!({}))
            .await?;

        let _handle = dst.import_doc(doc_id.clone(), exported).await?;

        assert!(
            dst.is_member_present_in_partition_item_state(&part_id, &doc_id.to_string())
                .await?,
            "import must keep the doc present in partition state for existing memberships"
        );

        let events = dst
            .get_partition_doc_events_for_peer(
                &"peer-fast-import".into(),
                &crate::sync::protocol::GetPartitionDocEventsRequest {
                    partitions: vec![crate::sync::protocol::PartitionCursorRequest {
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
                        crate::sync::protocol::PartitionDocEventDeets::ItemChanged {
                            item_id: event_doc_id,
                            payload,
                        } if event_doc_id == &doc_id.to_string()
                            && serde_json::from_str::<serde_json::Value>(payload)
                                .ok()
                                .and_then(|value| {
                                    value
                                        .get("heads")
                                        .and_then(|heads| heads.as_array())
                                        .map(|heads| !heads.is_empty())
                                })
                                .unwrap_or(false)
                    )
            }),
            "import must produce current partition doc events with imported heads"
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

        handle.with_document(|doc| {
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "name", "abc")
                .expect("failed writing doc");
            tx.commit();
        });

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

        doc_b.with_document(|doc| {
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "b", true)
                .expect("failed writing doc b");
            tx.commit();
        });
        assert!(
            timeout(Duration::from_millis(300), rx.recv())
                .await
                .is_err(),
            "doc_id filtered change listener unexpectedly received doc_b event"
        );

        doc_a.with_document(|doc| {
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "a", true)
                .expect("failed writing doc a");
            tx.commit();
        });

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

        handle.with_document(|doc| {
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "other_key", "ignored")
                .expect("failed writing other_key");
            tx.commit();
        });
        assert!(
            timeout(Duration::from_millis(300), rx.recv())
                .await
                .is_err(),
            "path-filtered listener unexpectedly matched unrelated path"
        );

        handle.with_document(|doc| {
            let mut tx = doc.transaction();
            let container = tx
                .put_object(automerge::ROOT, "container", automerge::ObjType::Map)
                .expect("failed creating container object");
            tx.put(&container, "inner", "matched")
                .expect("failed writing container.inner");
            tx.commit();
        });

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
                BigRepoChangeNotification::DocCreated { doc_id, origin, .. } if *doc_id == target && matches!(origin, BigRepoChangeOrigin::Local)
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

        created.with_document(|doc| {
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "key", "value")
                .expect("failed updating created doc");
            tx.commit();
        });
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
        src_doc.with_document(|doc| {
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "n", 1_i64)
                .expect("failed writing source");
            tx.commit();
        });
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

        doc_b.with_document(|doc| {
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "b", true)
                .expect("failed writing doc b");
            tx.commit();
        });
        assert!(
            timeout(Duration::from_millis(200), rx.recv())
                .await
                .is_err(),
            "doc_id filtered listener unexpectedly received doc_b event"
        );

        doc_a.with_document(|doc| {
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "a", true)
                .expect("failed writing doc a");
            tx.commit();
        });
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

pub struct AbortableTokioSpawn {
    set: Arc<utils_rs::AbortableJoinSet>
}
impl subduction_core::connection::manager::Spawn<future_form::Sendable> for AbortableTokioSpawn{
    fn spawn(&self, fut: BoxFuture<'static, ()>) -> futures::stream::AbortHandle {
        let (handle, reg) = futures::stream::AbortHandle::new_pair();
        self.set.spawn(async move {
            let _ = futures::stream::Abortable::new(fut, reg).await;
        }).expect("error spawning task");
        handle
    }
}
#[derive(Debug, Clone, Copy, Default)]
pub struct TimeoutTokio;

impl subduction_core::timeout::Timeout<future_form::Sendable> for TimeoutTokio {
    fn timeout<'a, T: 'a>(
        &'a self,
        dur: Duration,
        fut: BoxFuture<'a, T>,
    ) -> BoxFuture<'a, Result<T, subduction_core::timeout::TimedOut>> {
        async move {
            match tokio::time::timeout(dur, fut).await {
                Ok(v) => Ok(v),
                Err(_elapsed) => Err(subduction_core::timeout::TimedOut),
            }
        }
        .boxed()
    }
}
