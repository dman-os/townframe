//! FIXME: don't expose unregister_remote_repo_peer but handle the lifecycle
//! internally by sharing the Arc<Mutex> of the registry to the runtime

mod interlude {
    #[allow(unused_imports)]
    pub use big_sync_core::{ObjId, PeerId};
    use future_form::{FutureForm, Sendable};
    pub use utils_rs::prelude::*;
}

use crate::interlude::*;
use crate::keyhive_storage::{BigRepoKeyhiveStorage, KEYHIVE_SUBDIR};
use sqlx_utils_rs::SqlCtx;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};

use automerge::ChangeHash;
use autosurgeon::{Hydrate, Prop, Reconcile};
use sedimentree_core::loose_commit::id::CommitId;

// FIXME: properly test the changes impl and investigate
// why it no longer has users
mod backend;
#[expect(unused)]
mod changes;
mod encrypted_blob;
pub mod ephemeral;
pub(crate) mod handler;
mod keyhive;
pub(crate) mod keyhive_conn;
pub(crate) mod keyhive_listener;
pub(crate) mod keyhive_storage;
pub mod rpc;
mod runtime;
/// runtime2 — the tractable, runtime-neutral rewrite.
/// See `play.big_repo.runtime2.md`.
pub(crate) mod runtime2;
mod sqlite_big_repo_store;
pub use sqlite_big_repo_store::SqliteBigRepoStore;
pub(crate) mod wire;
pub use runtime::{
    CreateDocError, DocLookup, GetDocError, PutDocError, SyncDocError, SyncDocPolicyError,
};
#[cfg(test)]
pub(crate) mod test;
#[cfg(test)]
pub(crate) mod test2;

pub use backend::BigRepoSyncBackend;
pub use ephemeral::{
    BigEphemeral, BigEphemeralEvent, BigEphemeralFilter, BigEphemeralSubscription,
    BigEphemeralTopic,
};
pub use keyhive::{BigKeyhiveAgent, BigKeyhiveAuthority, BigKeyhiveGroup, BigKeyhiveHandle};

pub use changes::{
    path_prefix_matches as big_repo_path_prefix_matches, BigRepoChangeNotification,
    BigRepoChangeOrigin, ChangeFilter as BigRepoChangeFilter,
    ChangeListenerRegistration as BigRepoChangeListenerRegistration,
    DocIdFilter as BigRepoDocIdFilter, DomainFilter as BigRepoDomainFilter,
    DomainListenerRegistration as BigRepoDomainListenerRegistration,
    OriginFilter as BigRepoOriginFilter,
};
pub use changes::{BigRepoAccess, BigRepoDomainNotification, GroupId};

pub type DocumentId = big_sync_core::ObjId;
pub type SharedPartStore = Arc<dyn big_sync::HostPartStore>;

/// The global partition: every doc we can read appears here as a marker.
/// Embedders pass this PartId to big_sync's `set_peer`.
pub const GLOBAL_PART_ID: big_sync_core::PartId = big_sync_core::PartId::new([
    0x67, 0x6c, 0x6f, 0x62, 0x61, 0x6c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
]);

#[derive(Debug, Clone)]
pub struct Config {
    /// Single identity seed used to derive both the Keyhive individual and
    /// the Subduction signer.
    pub node_identity_seed: [u8; 32],
    pub storage: StorageConfig,
    /// Scope key used to isolate this BigRepo instance's data in SQLite storage.
    pub scope_key: Arc<str>,
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
    keyhive: BigKeyhiveHandle,
    #[educe(Debug(ignore))]
    keyhive_storage: BigRepoKeyhiveStorage,
    #[educe(Debug(ignore))]
    sync_policy: runtime::BigRepoSyncPolicy,
    #[educe(Debug(ignore))]
    big_sync_store: SharedPartStore,
    #[educe(Debug(ignore))]
    sqlite_store: SqliteBigRepoStore,
    #[educe(Debug(ignore))]
    runtime: runtime2::Runtime2Handle<future_form::Sendable>,
    #[educe(Debug(ignore))]
    ephemeral: BigEphemeral,
    #[educe(Debug(ignore))]
    keyhive_change_tx: tokio::sync::broadcast::Sender<()>,
    #[educe(Debug(ignore))]
    change_manager: Arc<changes::ChangeListenerManager>,
    #[educe(Debug(ignore))]
    change_manager_stop: std::sync::Mutex<Option<changes::ChangeListenerManagerStopToken>>,
}

pub type SharedBigRepo = Arc<BigRepo>;

impl BigRepo {
    pub const BACKEND_ID: &'static str = "BigRepoSyncBackend";

    /// Boot BigRepo, constructing its own SQLite-backed store for both the
    /// big-sync partition layer and subduction/runtime storage.
    ///
    /// The [`Config::scope_key`] isolates this instance's data from other
    /// BigRepo instances sharing the same SQLite database.
    pub async fn boot(config: Config) -> Res<(Arc<Self>, BigRepoStopToken)> {
        let Config {
            node_identity_seed,
            storage,
            scope_key,
        } = config;
        let sql = match &storage {
            StorageConfig::Memory => SqlCtx::memory().await?,
            StorageConfig::Disk { path } => {
                std::fs::create_dir_all(path)
                    .wrap_err_with(|| format!("failed creating BigRepo data directory: {}", path.display()))?;
                let db_path = path.join("big_repo.sqlite");
                SqlCtx::url(&format!("sqlite://{}", db_path.display())).await?
            }
        };
        let store = SqliteBigRepoStore::new(
            sql,
            scope_key.clone(),
            big_sync_core::BuckId::MAX_LEVEL,
        )
        .await?;
        Self::boot_inner(
            Config {
                node_identity_seed,
                storage,
                scope_key,
            },
            store,
        )
        .await
    }
    #[cfg(test)]
    pub(crate) async fn boot_with_store(
        config: Config,
        store: SqliteBigRepoStore,
    ) -> Res<(Arc<Self>, BigRepoStopToken)> {
        Self::boot_inner(config, store).await
    }
    #[cfg(test)]
    pub(crate) fn shared_part_store(&self) -> SharedPartStore {
        Arc::clone(&self.big_sync_store)
    }
    #[cfg(test)]
    pub(crate) fn sqlite_store(&self) -> SqliteBigRepoStore {
        self.sqlite_store.clone()
    }

    async fn boot_inner(
        config: Config,
        store: SqliteBigRepoStore,
    ) -> Res<(Arc<Self>, BigRepoStopToken)> {
        let Config {
            node_identity_seed,
            storage,
            scope_key: _,
        } = config;
        let big_sync_store: SharedPartStore = Arc::new(store.clone());
        let keyhive_events = store.clone();
        let subduction_storage = store;
        // `SubductionKeyhive` authorizes peers by matching the peer signing
        // identity to the Keyhive individual identifier, so BigRepo derives
        // both identities from this one seed.
        let sync_policy = runtime::BigRepoSyncPolicy::default();
        let keyhive_storage = match &storage {
            StorageConfig::Memory => BigRepoKeyhiveStorage::memory_sqlite(keyhive_events.clone()),
            StorageConfig::Disk { path } => BigRepoKeyhiveStorage::fs(
                keyhive_events.clone(),
                path.join(KEYHIVE_SUBDIR),
            )
            .wrap_err("failed booting keyhive storage")?,
        };
        // Create the listener channel before constructing Keyhive so the
        // listener can be wired in (avoids the reference cycle). Only the
        // sender side is used by the listener; the receiver is forwarded into
        // the runtime's own event channel via a background task.
        let (listener_evt_tx, listener_evt_rx) = tokio::sync::mpsc::unbounded_channel();
        let listener = crate::keyhive_listener::BigRepoKeyhiveListener {
            evt_tx: listener_evt_tx.clone(),
        };
        let keyhive = if let Some(restored) = BigKeyhiveHandle::restore_from_storage_archive(
            node_identity_seed,
            &keyhive_storage,
            listener.clone(),
        )
        .await?
        {
            restored
        } else {
            BigKeyhiveHandle::new(node_identity_seed, listener).await?
        };
        keyhive.import_prekey_secrets(&keyhive_storage).await?;
        keyhive.ingest_from_storage(&keyhive_storage).await?;
        keyhive.save_prekey_secrets(&keyhive_storage).await?;
        let policy_keyhive = keyhive.clone_keyhive();
        let policy = Arc::new(subduction_keyhive::policy::SubductionKeyhive::new(
            policy_keyhive,
        ));
        let signer =
            subduction_crypto::signer::memory::MemorySigner::from_bytes(&node_identity_seed);
        let peer_id = PeerId::new(*signer.verifying_key().as_bytes());
        let (change_manager, change_manager_stop) = changes::ChangeListenerManager::boot();
        let (keyhive_change_tx, _) = tokio::sync::broadcast::channel(128);

        let (runtime, ephemeral, _events, runtime_stop) = runtime2::native::spawn_native_runtime2(
            signer,
            subduction_storage.clone(),
            big_sync_store.clone(),
            Arc::clone(&policy),
            sync_policy,
            keyhive.clone(),
            keyhive_storage.clone(),
            Arc::clone(&change_manager),
            listener_evt_tx,
            listener_evt_rx,
            keyhive_change_tx.clone(),
        )
        .await?;

        let out = Arc::new(Self {
            local_peer_id: peer_id,
            keyhive,
            keyhive_storage,
            sync_policy,
            big_sync_store,
            sqlite_store: subduction_storage.clone(),
            runtime,
            ephemeral,
            keyhive_change_tx,
            change_manager,
            change_manager_stop: std::sync::Mutex::new(Some(change_manager_stop)),
        });

        // Boot full reindex: seed the doc-members index for our own principal
        // before exposing the repo to callers. Running this detached allowed a
        // later local grant refresh to be overwritten by the boot task.
        {
            let own_id = PeerId::new(out.keyhive.clone_keyhive().id().to_bytes());
            let agent = keyhive_core::principal::identifier::Identifier::from(
                ed25519_dalek::VerifyingKey::from_bytes(own_id.0.as_bytes())
                    .expect("own id is valid"),
            );
            let docs = out.keyhive.docs_for_agent(&agent).await;
            for (doc_id, access) in docs {
                let mut agents = HashMap::new();
                agents.insert(own_id, access);
                out.big_sync_store.set_doc_members(doc_id, agents).await;
            }
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
                runtime_stop,
                change_manager_stop: Some(change_manager_stop),
            },
        ))
    }

    pub fn local_peer_id(&self) -> PeerId {
        self.local_peer_id
    }
    pub fn keyhive(&self) -> &BigKeyhiveHandle {
        &self.keyhive
    }

    pub(crate) fn sync_policy(&self) -> runtime::BigRepoSyncPolicy {
        self.sync_policy
    }

    pub(crate) fn big_sync_store(&self) -> &SharedPartStore {
        &self.big_sync_store
    }

    pub fn ephemeral(&self) -> BigEphemeral {
        self.ephemeral.clone()
    }

    pub(crate) fn subscribe_keyhive_changes(&self) -> tokio::sync::broadcast::Receiver<()> {
        self.keyhive_change_tx.subscribe()
    }

    /// Synchronize local Keyhive state with a directly connected peer.
    pub async fn sync_keyhive_with_peer(
        &self,
        peer_id: PeerId,
        timeout: Option<std::time::Duration>,
    ) -> Res<()> {
        self.runtime.sync_keyhive_with_peer(peer_id, timeout).await
    }

    #[cfg(test)]
    pub(crate) async fn inspect_stored_doc_blobs(&self, doc_id: DocumentId) -> Res<Vec<Vec<u8>>> {
        self.runtime.inspect_stored_doc_blobs(doc_id).await
    }
}

// main methods
impl BigRepo {
    #[tracing::instrument(
        skip_all,
        fields(%document_id, %self.local_peer_id)
    )]
    pub async fn get_doc(
        self: &Arc<Self>,
        document_id: &DocumentId,
    ) -> Res<DocLookup<BigDocHandle>> {
        let out = self.runtime.get_doc_handle(*document_id).await?;
        Ok(out.map_ready(|bundle| BigDocHandle {
            repo: Arc::clone(self),
            bundle,
        }))
    }

    #[tracing::instrument(skip_all, fields(%self.local_peer_id))]
    pub async fn doc_head_state(&self, document_id: DocumentId) -> Res<runtime2::DocHeadState> {
        self.runtime.doc_head_state(document_id).await
    }

    /// Wait until finite runtime work currently admitted to this repository
    /// has drained. Pending materialization due to unavailable keys is allowed.
    pub async fn wait_for_quiescence(&self, timeout: Option<std::time::Duration>) -> Res<()> {
        self.runtime.wait_for_quiescence(timeout).await
    }

    pub async fn create_doc(
        self: &Arc<Self>,
        initial_content: automerge::Automerge,
    ) -> Result<BigDocHandle, CreateDocError> {
        let bundle = self.runtime.create_doc(initial_content, Vec::new()).await?;
        Ok(BigDocHandle {
            repo: Arc::clone(self),
            bundle,
        })
    }

    pub async fn create_doc_with_parents(
        self: &Arc<Self>,
        initial_content: automerge::Automerge,
        parents: Vec<BigKeyhiveAuthority>,
    ) -> Result<BigDocHandle, CreateDocError> {
        let bundle = self.runtime.create_doc(initial_content, parents).await?;
        Ok(BigDocHandle {
            repo: Arc::clone(self),
            bundle,
        })
    }

    pub async fn create_group_with_parents(
        self: &Arc<Self>,
        parents: Vec<BigKeyhiveAuthority>,
    ) -> Res<BigKeyhiveGroup> {
        let group = self
            .keyhive
            .create_group_with_parents(parents, &self.keyhive_storage)
            .await?;
        self.runtime.note_local_keyhive_changed().await?;
        Ok(group)
    }

    /// Add a principal to a group and propagate reader membership into every
    /// document governed by that group. Reader additions also create one
    /// history checkpoint per affected document.
    pub async fn add_member_to_group(
        self: &Arc<Self>,
        member: impl Into<BigKeyhiveAuthority>,
        group: &BigKeyhiveGroup,
        access: keyhive_core::access::Access,
    ) -> Res<()> {
        let mut docs = BTreeMap::new();
        for doc_id in self.keyhive.group_document_ids(group).await {
            let doc = self.get_doc(&doc_id).await?.into_ready(doc_id)?;
            docs.insert(doc_id, doc);
        }

        let mut after_content = BTreeMap::new();
        for doc_id in docs.keys().copied() {
            let heads = self.doc_head_state(doc_id).await?.sedimentree_heads;
            after_content.insert(doc_id, heads.iter().map(|head| head.0.to_vec()).collect());
        }

        let affected_docs = self
            .keyhive
            .add_member_to_group(member, group, access, after_content, &self.keyhive_storage)
            .await?;

        // BigRepo's contract is history-inclusive for reader grants. Keyhive
        // updates each affected document's CGKA tree, while this layer creates
        // one real content checkpoint per affected document so the new member
        // receives a decryptable entry point to the existing history.
        if access.is_reader() {
            for doc_id in &affected_docs {
                let doc = docs
                    .get(doc_id)
                    .ok_or_else(|| ferr!("affected document was not preflighted: {doc_id}"))?;
                doc.with_document(|doc| {
                    let _ = doc.empty_commit(automerge::transaction::CommitOptions::default());
                })
                .await?;
            }
        }

        self.runtime.note_local_keyhive_changed().await?;
        for doc_id in affected_docs {
            self.refresh_doc_access_index(doc_id).await?;
        }
        Ok(())
    }

    /// Grant document access.
    ///
    /// Reader grants also write a real Automerge checkpoint so the readable
    /// history survives reopen and sync.
    pub async fn grant_doc_access(
        self: &Arc<Self>,
        doc_id: DocumentId,
        principal: impl Into<BigKeyhiveAuthority>,
        access: keyhive_core::access::Access,
    ) -> Res<()> {
        let doc = self.get_doc(&doc_id).await?.into_ready(doc_id)?;
        let heads = self.doc_head_state(doc_id).await?.sedimentree_heads;
        let after_content = heads.iter().map(|head| head.0.to_vec()).collect();

        self.keyhive
            .grant_doc_access(
                principal,
                doc_id,
                access,
                after_content,
                &self.keyhive_storage,
            )
            .await?;

        if access.is_reader() {
            // Create the checkpoint after the grant so the checkpoint itself is
            // written under the newly granted epoch and can carry the prior
            // content history forward.
            doc.with_document(|doc| {
                let _ = doc.empty_commit(automerge::transaction::CommitOptions::default());
            })
            .await?;
        }

        self.runtime.note_local_keyhive_changed().await?;
        self.refresh_doc_access_index(doc_id).await?;

        Ok(())
    }

    /// Revoke an authority's access using the current sedimentree frontier.
    pub async fn revoke_doc_access(
        self: &Arc<Self>,
        doc_id: DocumentId,
        principal: impl Into<BigKeyhiveAuthority>,
    ) -> Res<()> {
        let _doc = self.get_doc(&doc_id).await?.into_ready(doc_id)?;
        let heads = self.doc_head_state(doc_id).await?.sedimentree_heads;
        let after_content = heads.iter().map(|head| head.0.to_vec()).collect();
        self.keyhive
            .revoke_doc_access(
                principal,
                doc_id,
                true,
                after_content,
                &self.keyhive_storage,
            )
            .await?;
        self.runtime.note_local_keyhive_changed().await?;
        self.refresh_doc_access_index(doc_id).await?;
        Ok(())
    }

    async fn refresh_doc_access_index(&self, doc_id: DocumentId) -> Res<()> {
        let verifying_key = ed25519_dalek::VerifyingKey::from_bytes(&doc_id.into_bytes())
            .map_err(|_| ferr!("doc_id is not a valid Ed25519 point"))?;
        self.runtime
            .refresh_big_sync_doc_access(keyhive_core::principal::identifier::Identifier::from(
                verifying_key,
            ))
            .await?;
        Ok(())
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
        let _ = end_signal_tx;
        let (peer_id, closed) = self
            .runtime
            .open_connection(peer_id, Box::new((endpoint, endpoint_addr)))
            .await?;
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
        let _ = end_signal_tx;
        let (peer_id, closed) = self.runtime.accept_connection(Box::new(conn)).await?;
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

    /// Initiate a keyhive protocol sync with the connected peer.
    pub async fn sync_keyhive_with_peer(&self, timeout: Option<std::time::Duration>) -> Res<()> {
        if self.is_closed() {
            return Err(ferr!("connection is closed"));
        }
        self.repo
            .runtime
            .sync_keyhive_with_peer(self.peer_id, timeout)
            .await
    }

    /// NOTE: a succesful outcome doesn't correspond to doc
    /// handles having the latest heads
    pub async fn sync_doc_with_peer(
        &self,
        doc_id: DocumentId,
        timeout: Option<std::time::Duration>,
    ) -> Result<(), SyncDocError> {
        if self.is_closed() {
            return Err(SyncDocError::IoError(ferr!("connection is closed")));
        }
        self.repo
            .runtime
            .sync_doc_with_peer(doc_id, self.peer_id, timeout)
            .await
    }

    pub async fn stop(self) -> Res<()> {
        self.repo.runtime.close_connection(self.peer_id).await
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

    pub async fn subscribe_domain_listener(
        self: &Arc<Self>,
        filter: BigRepoDomainFilter,
    ) -> Res<(
        BigRepoDomainListenerRegistration,
        tokio::sync::mpsc::UnboundedReceiver<Vec<crate::changes::BigRepoDomainNotification>>,
    )> {
        let (registration, domain_rx) = self
            .change_manager
            .subscribe_domain_listener(filter)
            .await?;
        Ok((registration, domain_rx))
    }
}

// big_sync support
impl BigRepo {
    pub async fn doc_payload_heads(&self, doc_id: DocumentId) -> Res<Option<Arc<[ChangeHash]>>> {
        partition_doc_heads_payload(&self.big_sync_store, doc_id).await
    }
}

pub struct BigRepoStopToken {
    runtime_stop: runtime2::Runtime2StopToken<future_form::Sendable, runtime2::TokioTaskRuntime>,
    change_manager_stop: Option<changes::ChangeListenerManagerStopToken>,
}

impl BigRepoStopToken {
    pub async fn stop(mut self) -> Res<()> {
        self.runtime_stop
            .stop(std::time::Duration::from_secs(5))
            .await?;
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

    pub async fn with_document_read<F, R>(&self, operation: F) -> R
    where
        F: FnOnce(&automerge::Automerge) -> R,
    {
        let doc = self.bundle.doc.lock().await;

        operation(&doc)
    }

    pub async fn export(&self) -> Vec<u8> {
        self.with_document_read(|doc| doc.save()).await
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
    #[cfg(test)]

    pub async fn hydrate_path_at_heads<T: Hydrate + Reconcile + Send + Sync + 'static>(
        &self,
        heads: &[automerge::ChangeHash],
        obj_id: automerge::ObjId,
        path: Vec<Prop<'static>>,
    ) -> Res<Option<T>> {
        self.with_document_read(|doc| -> Res<Option<T>> {
            if path.is_empty() && obj_id == automerge::ROOT {
                let value: T = autosurgeon::hydrate_at(doc, heads).wrap_err("error hydrating")?;
                Ok(Some(value))
            } else {
                match autosurgeon::hydrate_path_at(doc, &obj_id, path, heads) {
                    Ok(Some(value)) => Ok(Some(value)),
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
        .as_ref()
        .map(doc_heads_from_payload))
}

fn doc_heads_from_payload(payload: &serde_json::Value) -> Arc<[ChangeHash]> {
    let heads = payload
        .as_object()
        .expect(ERROR_IMPOSSIBLE)
        .get("heads")
        .cloned()
        .expect(ERROR_IMPOSSIBLE);
    let heads: Vec<String> = serde_json::from_value(heads).expect(ERROR_IMPOSSIBLE);
    am_utils_rs::parse_commit_heads(&heads).expect(ERROR_IMPOSSIBLE)
}
