mod interlude {
    pub use big_sync_core::{ObjId, PartId, PeerId};

    pub use future_form::{FutureForm, Local, Sendable};
    pub use utils_rs::prelude::*;
}

use crate::interlude::*;
use crate::keyhive_storage::{BigRepoKeyhiveStorage, KEYHIVE_SUBDIR};
use sqlx_utils_rs::SqlCtx;

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};

use automerge::ChangeHash;
use autosurgeon::{Hydrate, Prop, Reconcile};
use sedimentree_core::loose_commit::id::CommitId;

pub(crate) mod access_policy;
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

mod runtime2;
pub use runtime2::doc_revision_store::{
    AutomergeFrontierEvent, AutomergeFrontierRevisionStore, AutomergeFrontierSelector,
    AutomergeFrontierTarget,
};
pub use runtime2::types::{
    CreateDocError, DocLookup, GetDocError, GroupScopeController, GroupScopeHandle,
    KeyhiveSyncCancelled, PutDocError, SyncDocError, SyncDocOutcome, SyncDocPolicyError,
    SyncDocReceipt, WorkerGroupScope,
};
pub use runtime2::{DocHeadState, MaterializationState};
mod store;
pub use runtime2::{automerge_doc_obj_id, automerge_obj_to_doc_id};
#[cfg(feature = "test-support")]
pub use store::sqlite::BigSyncStoreSnapshot;
pub use store::sqlite::SqliteBigRepoStore;
mod wire;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocumentSyncStage {
    NotPersisted,
    Persisted,
    Indexed,
    Materialized,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentSyncSnapshot {
    pub doc_id: DocumentId,
    pub stage: DocumentSyncStage,
    pub head_state: Option<DocHeadState>,
    pub indexed_parts: usize,
    pub payload_present: bool,
}
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
pub use keyhive_core;

pub use changes::{BigRepoAccess, BigRepoDomainNotification, GroupId};
pub use changes::{
    BigRepoChangeNotification, BigRepoChangeOrigin, BigRepoLocalNotification,
    ChangeFilter as BigRepoChangeFilter,
    ChangeListenerRegistration as BigRepoChangeListenerRegistration,
    DocIdFilter as BigRepoDocIdFilter, DomainFilter as BigRepoDomainFilter,
    DomainListenerRegistration as BigRepoDomainListenerRegistration,
    LocalFilter as BigRepoLocalFilter,
    LocalListenerRegistration as BigRepoLocalListenerRegistration,
    OriginFilter as BigRepoOriginFilter, path_prefix_matches as big_repo_path_prefix_matches,
};

pub type DocumentId = big_sync_core::ObjId;
pub type SharedPartStore = Arc<dyn big_sync::HostPartStore>;

/// The global partition: every doc we can read appears here as a marker.
/// Embedders pass this PartId to big_sync's `set_peer`.
pub const GLOBAL_PART_ID: big_sync_core::PartId = big_sync_core::PartId::new([
    0x67, 0x6c, 0x6f, 0x62, 0x61, 0x6c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
]);
/// Return the deterministic BigSync partition derived from a Keyhive group.
pub fn group_part_id(group_id: [u8; 32]) -> big_sync_core::PartId {
    runtime2::group_part_id(group_id)
}

#[derive(Debug, Clone)]
pub struct Config {
    /// Single identity seed used to derive both the Keyhive individual and
    /// the Subduction signer.
    pub node_identity_seed: [u8; 32],
    pub storage: StorageConfig,
    /// Scope key used to isolate this BigRepo instance's data in SQLite storage.
    pub scope_key: Arc<str>,
    pub hidden_parts: HashSet<PartId>,
    /// Keyhive groups whose documents the Automerge frontier worker processes.
    pub automerge_frontier_group_scope: WorkerGroupScope,
    /// Keyhive groups whose documents the causal checkpoint worker processes.
    pub causal_checkpoint_group_scope: WorkerGroupScope,
    /// Keyhive groups whose documents and group parts the group-part worker manages.
    pub group_part_group_scope: WorkerGroupScope,
    /// Test-only: unwire the per-connection `SubscribeKeyhiveChanges`
    /// subscription so peers only learn keyhive changes through explicit
    /// `sync_keyhive_with_peer` rounds. Lets tests pin late-keyhive-sync
    /// semantics with a membership view that is stale by construction.
    /// Production nodes always wire the subscription.
    #[cfg(test)]
    pub keyhive_change_notifs: bool,
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
    sync_policy: runtime2::types::BigRepoSyncPolicy,
    #[educe(Debug(ignore))]
    big_sync_store: SharedPartStore,
    #[educe(Debug(ignore))]
    frontier_store: SharedPartStore,
    #[educe(Debug(ignore))]
    sqlite_store: SqliteBigRepoStore,
    #[educe(Debug(ignore))]
    runtime: runtime2::Runtime2Handle<future_form::Sendable>,
    #[educe(Debug(ignore))]
    ephemeral: BigEphemeral,
    #[educe(Debug(ignore))]
    keyhive_protocol: handler::BigRepoKeyhiveProtocol,
    #[educe(Debug(ignore))]
    keyhive_dispatcher: runtime2::keyhive_dispatcher::KeyhiveChangeDispatcher,
    #[educe(Debug(ignore))]
    change_manager: Arc<changes::ChangeListenerManager>,
    #[educe(Debug(ignore))]
    change_manager_stop: std::sync::Mutex<Option<changes::ChangeListenerManagerStopToken>>,
    #[educe(Debug(ignore))]
    connection_tasks: Arc<utils_rs::AbortableJoinSet>,
    #[educe(Debug(ignore))]
    automerge_frontier_group_scope: GroupScopeController,
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
        #[cfg(test)]
        let keyhive_change_notifs = config.keyhive_change_notifs;
        let Config {
            node_identity_seed,
            storage,
            scope_key,
            hidden_parts,
            automerge_frontier_group_scope,
            causal_checkpoint_group_scope,
            group_part_group_scope,
            ..
        } = config;
        let sql = match &storage {
            StorageConfig::Memory => SqlCtx::memory().await?,
            StorageConfig::Disk { path } => {
                std::fs::create_dir_all(path).wrap_err_with(|| {
                    format!("failed creating BigRepo data directory: {}", path.display())
                })?;
                let db_path = path.join("big_repo.sqlite");
                SqlCtx::url(&format!("sqlite://{}", db_path.display())).await?
            }
        };
        let store = SqliteBigRepoStore::new_with_config(
            sql,
            Arc::clone(&scope_key),
            big_sync_core::BuckId::MAX_LEVEL,
            big_sync::HostPartStoreConfig {
                hidden_parts: hidden_parts.clone(),
                ..Default::default()
            },
        )
        .await?;
        Self::boot_inner(
            Config {
                node_identity_seed,
                storage,
                scope_key,
                hidden_parts,
                automerge_frontier_group_scope,
                causal_checkpoint_group_scope,
                group_part_group_scope,
                #[cfg(test)]
                keyhive_change_notifs,
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
    /// Return the partition store owned by this BigRepo.
    ///
    /// BigSync consumers must use this handle so they share BigRepo's
    /// authority-derived partition membership.
    pub fn shared_part_store(&self) -> SharedPartStore {
        Arc::clone(&self.big_sync_store)
    }

    /// Return the local-only frontier partition store (automerge heads
    /// payloads). Never registered with the big-sync RPC server.
    pub fn frontier_part_store(&self) -> SharedPartStore {
        Arc::clone(&self.frontier_store)
    }

    /// The SQLite context backing this BigRepo's storage. Consumers that need
    /// additional scopes in the same database (e.g. the blob partitions)
    /// construct their scoped store from this context.
    pub fn sql_ctx(&self) -> SqlCtx {
        self.sqlite_store.sql.clone()
    }
    #[cfg(feature = "test-support")]
    pub async fn big_sync_store_snapshot(&self) -> Res<BigSyncStoreSnapshot> {
        let mut snapshot = self.sqlite_store.big_sync_store_snapshot().await?;
        snapshot.local_cgka_secret_count = subduction_keyhive::load_local_cgka_secrets::<
            _,
            future_form::Sendable,
        >(&self.keyhive_storage)
        .await?
        .len();
        snapshot.local_prekey_secret_count = subduction_keyhive::load_local_prekey_secrets::<
            _,
            future_form::Sendable,
        >(&self.keyhive_storage)
        .await?
        .len();
        Ok(snapshot)
    }
    #[cfg(test)]
    pub(crate) fn sqlite_store(&self) -> SqliteBigRepoStore {
        self.sqlite_store.clone()
    }

    /// The keyhive sidecar storage (archives, WAL events, local secret
    /// material). Test-only: lets tier tests trigger compaction and inspect
    /// durable keyhive state exactly as a production shutdown would leave it.
    #[cfg(test)]
    pub(crate) fn keyhive_storage(&self) -> BigRepoKeyhiveStorage {
        self.keyhive_storage.clone()
    }

    async fn boot_inner(
        config: Config,
        store: SqliteBigRepoStore,
    ) -> Res<(Arc<Self>, BigRepoStopToken)> {
        #[cfg(test)]
        let keyhive_change_notifs = config.keyhive_change_notifs;
        #[cfg(not(test))]
        let keyhive_change_notifs = true;
        let Config {
            node_identity_seed,
            storage,
            scope_key,
            hidden_parts: _,
            automerge_frontier_group_scope,
            causal_checkpoint_group_scope,
            group_part_group_scope,
            ..
        } = config;
        let big_sync_store: SharedPartStore = Arc::new(store.clone());
        // Frontier payloads live in their own storage scope so the raw doc_id
        // object ids cannot collide with the document sedimentree objects in
        // the main scope. The scope is local-only (never registered with the
        // big-sync RPC server).
        let frontier_store: SharedPartStore = Arc::new(
            SqliteBigRepoStore::new_with_config(
                store.sql.clone(),
                format!("{scope_key}:automerge-frontier"),
                big_sync_core::BuckId::MAX_LEVEL,
                big_sync::HostPartStoreConfig::default(),
            )
            .await?,
        );
        let keyhive_events = store.clone();
        let subduction_storage = store;
        // `SubductionKeyhive` authorizes peers by matching the peer signing
        // identity to the Keyhive individual identifier, so BigRepo derives
        // both identities from this one seed.
        let sync_policy = runtime2::types::BigRepoSyncPolicy::default();
        let keyhive_storage = match &storage {
            StorageConfig::Memory => BigRepoKeyhiveStorage::memory_sqlite(keyhive_events.clone()),
            StorageConfig::Disk { path } => {
                let keyhive_root = path.join(KEYHIVE_SUBDIR);
                // Key material goes through the OS keyring where available;
                // fsync'd files remain the fallback of record.
                BigRepoKeyhiveStorage::fs_with_secret_repo(keyhive_events.clone(), keyhive_root)
                    .await
                    .wrap_err("failed booting keyhive storage")?
            }
        };
        // Create the runtime event channel before constructing Keyhive so the
        // listener can be wired in (avoids the reference cycle). The listener
        // and the keyhive sync-done observer send `Runtime2Evt` directly into
        // this channel; the runtime consumes it.
        let (evt_tx, evt_rx) = async_channel::unbounded::<crate::runtime2::Runtime2Evt>();
        let listener = crate::keyhive_listener::BigRepoKeyhiveListener {
            evt_tx: evt_tx.clone(),
            storage: keyhive_storage.clone(),
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
        keyhive.import_prekey_state(&keyhive_storage).await?;
        keyhive.save_prekey_state(&keyhive_storage).await?;
        let policy_keyhive = keyhive.clone_keyhive();
        let policy = Arc::new(subduction_keyhive::policy::SubductionKeyhive::new(
            policy_keyhive,
        ));
        let signer =
            subduction_crypto::signer::memory::MemorySigner::from_bytes(&node_identity_seed);
        let peer_id = PeerId::new(*signer.verifying_key().as_bytes());
        let (change_manager, change_manager_stop) = changes::ChangeListenerManager::boot();

        // The embedder-facing scope controller: workers read the live scope
        // through a handle, so a relay can grow/shrink its document set at
        // runtime without restarting the worker.
        let automerge_frontier_group_scope_controller =
            GroupScopeController::new(automerge_frontier_group_scope);

        let (runtime, ephemeral, keyhive_protocol, keyhive_dispatcher, runtime_stop) =
            runtime2::native::spawn_native_runtime2(
                signer,
                subduction_storage.clone(),
                Arc::clone(&frontier_store),
                subduction_storage.clone(),
                Arc::clone(&policy),
                sync_policy,
                keyhive.clone(),
                keyhive_storage.clone(),
                Arc::clone(&change_manager),
                evt_tx,
                evt_rx,
                automerge_frontier_group_scope_controller.handle(),
                causal_checkpoint_group_scope,
                group_part_group_scope,
                keyhive_change_notifs,
            )
            .await?;

        let connection_tasks = Arc::new(utils_rs::AbortableJoinSet::new());
        let out = Arc::new(Self {
            local_peer_id: peer_id,
            keyhive,
            keyhive_storage,
            sync_policy,
            big_sync_store,
            frontier_store,
            sqlite_store: subduction_storage.clone(),
            runtime,
            ephemeral,
            keyhive_protocol,
            keyhive_dispatcher,
            change_manager,
            change_manager_stop: std::sync::Mutex::new(Some(change_manager_stop)),
            connection_tasks: Arc::clone(&connection_tasks),
            automerge_frontier_group_scope: automerge_frontier_group_scope_controller,
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
                connection_tasks,
            },
        ))
    }

    pub fn local_peer_id(&self) -> PeerId {
        self.local_peer_id
    }

    /// Update the Automerge frontier worker's group scope at runtime.
    ///
    /// Daybook sync nodes keep [`WorkerGroupScope::All`]; relays constrain
    /// this to the (dynamic) set of group-part ids backing the documents they
    /// use to communicate with their clients. Workers rescan on change: newly
    /// eligible docs gain frontier state, docs that left the scope lose their
    /// frontier mirror.
    pub fn set_automerge_frontier_group_scope(&self, scope: WorkerGroupScope) {
        self.automerge_frontier_group_scope.set(scope);
    }

    /// The current live Automerge frontier worker scope.
    pub fn automerge_frontier_group_scope(&self) -> WorkerGroupScope {
        self.automerge_frontier_group_scope.get()
    }
    pub fn keyhive(&self) -> &BigKeyhiveHandle {
        &self.keyhive
    }
    /// Resolve a persisted Keyhive group without exposing the upstream id type.
    pub async fn get_group_by_id(&self, id: [u8; 32]) -> Option<BigKeyhiveGroup> {
        self.keyhive
            .get_group(keyhive_core::principal::group::id::GroupId::new(
                keyhive_core::principal::identifier::Identifier::from(
                    ed25519_dalek::VerifyingKey::from_bytes(&id)
                        .expect("group id must be a verifying key"),
                ),
            ))
            .await
    }
    /// Resolve this repository's local Keyhive agent.
    pub async fn local_keyhive_agent(&self) -> Res<BigKeyhiveAgent> {
        let peer_id = subduction_keyhive::KeyhivePeerId::from_bytes(*self.local_peer_id.as_bytes());
        self.keyhive
            .get_agent_by_peer_id(&peer_id)
            .await?
            .ok_or_eyre("local Keyhive agent is unavailable")
    }
    /// Return the contact card that identifies this repository's local
    /// Keyhive agent. Clone provisioning sends this before any repo-sync
    /// connection exists.
    pub fn local_keyhive_contact_card(&self) -> keyhive_core::contact_card::ContactCard {
        self.keyhive.contact_card().clone()
    }
    /// Learn and durably persist a peer's contact card before granting it
    /// access. The returned agent can be used directly in the grant.
    pub async fn receive_keyhive_contact_card(
        &self,
        contact_card: &keyhive_core::contact_card::ContactCard,
    ) -> Res<BigKeyhiveAgent> {
        self.keyhive.receive_contact_card(contact_card).await
    }
    /// Resolve a connected peer's Keyhive agent.
    pub async fn keyhive_agent_for_peer(&self, peer_id: PeerId) -> Res<Option<BigKeyhiveAgent>> {
        let keyhive_peer = subduction_keyhive::KeyhivePeerId::from_bytes(*peer_id.as_bytes());
        self.keyhive.get_agent_by_peer_id(&keyhive_peer).await
    }
    /// Grant administrative membership without exposing the Keyhive access type.
    pub async fn add_admin_member_to_group(
        self: &Arc<Self>,
        member: impl Into<BigKeyhiveAuthority>,
        group: &BigKeyhiveGroup,
    ) -> Res<()> {
        self.add_member_to_group(member, group, keyhive_core::access::Access::Admin)
            .await
    }
    /// Grant administrative access to a document without exposing Keyhive's access type.
    pub async fn add_admin_member_to_doc(
        self: &Arc<Self>,
        doc_id: DocumentId,
        member: impl Into<BigKeyhiveAuthority>,
    ) -> Res<()> {
        self.grant_doc_access(doc_id, member, keyhive_core::access::Access::Admin)
            .await
    }

    pub(crate) fn sync_policy(&self) -> runtime2::types::BigRepoSyncPolicy {
        self.sync_policy
    }

    pub fn ephemeral(&self) -> BigEphemeral {
        self.ephemeral.clone()
    }

    /// Register a peer's notification stream with the keyhive change
    /// dispatcher. The peer immediately receives the initial confirmation
    /// event; subsequent payload-free change hints are debounced and
    /// classified by the dispatcher.
    pub(crate) async fn subscribe_keyhive_changes(
        &self,
        peer_id: PeerId,
        tx: irpc::channel::mpsc::Sender<crate::rpc::KeyhiveChangedRpcEvent>,
    ) -> Uuid {
        self.keyhive_dispatcher.subscribe(peer_id, tx).await
    }

    /// Unregister a peer's notification stream for the given subscription ID.
    pub(crate) async fn unsubscribe_keyhive_changes(&self, peer_id: &PeerId, sub_id: Uuid) {
        self.keyhive_dispatcher.unsubscribe(peer_id, sub_id).await;
    }

    /// Synchronize local Keyhive state with a directly connected peer.
    pub async fn sync_keyhive_with_peer(&self, peer_id: PeerId) -> Res<()> {
        self.runtime.sync_keyhive_with_peer(peer_id).await
    }

    /// Synchronize a document with a directly connected peer.
    pub async fn sync_doc_with_peer(
        &self,
        doc_id: DocumentId,
        peer_id: PeerId,
    ) -> Result<SyncDocReceipt, SyncDocError> {
        self.runtime
            .sync_doc_with_peer_receipt(doc_id, peer_id)
            .await
    }

    pub async fn inspect_stored_doc_blobs(&self, doc_id: DocumentId) -> Res<Vec<Vec<u8>>> {
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

    pub async fn document_sync_snapshot(
        &self,
        document_id: DocumentId,
    ) -> Res<DocumentSyncSnapshot> {
        let head_state = self.runtime.inspect_doc_head_state(document_id).await?;
        let store = &self.big_sync_store;
        let indexed_parts = store.obj_parts(document_id).await?.len();
        let payload_present = store.obj_payload(document_id).await?.is_some();
        let stage = match head_state.as_ref().map(|state| state.state) {
            Some(
                MaterializationState::Materialized | MaterializationState::PartiallyMaterialized,
            ) => DocumentSyncStage::Materialized,
            Some(MaterializationState::Pending | MaterializationState::Missing)
                if indexed_parts > 0 =>
            {
                DocumentSyncStage::Indexed
            }
            _ if payload_present => DocumentSyncStage::Persisted,
            _ => DocumentSyncStage::NotPersisted,
        };
        Ok(DocumentSyncSnapshot {
            doc_id: document_id,
            stage,
            head_state,
            indexed_parts,
            payload_present,
        })
    }

    /// Return a compact, non-sensitive snapshot for sync timeout diagnostics.
    pub async fn document_sync_diagnostics(&self, document_id: DocumentId) -> Res<String> {
        let snapshot = self.document_sync_snapshot(document_id).await?;
        Ok(format!(
            "peer={} doc={} stage={:?} state={:?} indexed_parts={} payload_present={}",
            self.local_peer_id,
            snapshot.doc_id,
            snapshot.stage,
            snapshot.head_state.as_ref().map(|state| state.state),
            snapshot.indexed_parts,
            snapshot.payload_present
        ))
    }

    /// Wait until Keyhive event reconciliation currently queued on this repository finishes.
    ///
    /// This may block for a long time while Keyhive synchronization and durable
    /// projection settlement complete. Applications that need a boot deadline should
    /// apply their timeout at the application boundary.
    pub async fn wait_for_keyhive_reconciliation(&self) -> Res<()> {
        self.runtime.wait_for_keyhive_reconciliation().await
    }

    /// Wait until finite runtime work currently admitted to this repository
    /// has drained. Pending materialization due to unavailable keys is allowed.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn wait_for_quiescence(&self, timeout: Option<std::time::Duration>) -> Res<()> {
        self.runtime.wait_for_quiescence(timeout).await
    }

    /// Like [`BigRepo::wait_for_quiescence`], but freezes the hub once
    /// quiescence is reached: no events are processed and all non-unfreeze
    /// commands are held until [`BigRepo::unfreeze`].
    #[cfg(any(test, feature = "test-support"))]
    pub async fn wait_for_quiescence_freeze(
        &self,
        timeout: Option<std::time::Duration>,
    ) -> Res<()> {
        self.runtime.wait_for_quiescence_freeze(timeout, true).await
    }

    /// Resume event/command processing after a frozen quiescence wait.
    pub async fn unfreeze(&self) -> Res<()> {
        self.runtime.unfreeze().await
    }

    /// Whether the repository currently stores a sedimentree with `doc_id`.
    pub async fn contains_sedimentree_id(&self, doc_id: DocumentId) -> Res<bool> {
        self.runtime.contains_sedimentree_id(doc_id).await
    }

    /// Return the documents currently administered by `group`.
    pub async fn documents_in_group(&self, group: &BigKeyhiveGroup) -> BTreeSet<DocumentId> {
        self.keyhive.group_document_ids(group).await
    }

    /// List document IDs with a durable reservation but no Keyhive document
    /// yet (or whose reservation cleanup is pending). These are the crash-
    /// recovery candidates between ID allocation and finalization.
    pub async fn reserved_doc_ids(&self) -> Res<Vec<DocumentId>> {
        let reservations = self
            .keyhive_storage
            .list_doc_reservations()
            .await
            .map_err(|err| ferr!("failed listing document reservations: {err}"))?;
        Ok(reservations
            .into_iter()
            .map(|reservation| DocumentId::new(reservation.doc_id))
            .collect())
    }

    pub async fn recover_allocated_doc(
        self: &Arc<Self>,
        doc_id: DocumentId,
        pending_group: BigKeyhiveGroup,
    ) -> Result<bool, CreateDocError> {
        let Some((bytes, initial_keys)) = self
            .keyhive_storage
            .staged_doc_content(doc_id.into_bytes())
            .await
            .map_err(|err| {
                CreateDocError::from(eyre::eyre!("failed loading staged document content: {err}"))
            })?
        else {
            return Ok(false);
        };
        let content = automerge::Automerge::load(&bytes).map_err(|err| {
            CreateDocError::from(eyre::eyre!(
                "failed decoding staged document content: {err}"
            ))
        })?;
        self.finalize_allocated_doc_with_keys(doc_id, content, pending_group, initial_keys)
            .await?;
        Ok(true)
    }

    pub async fn allocate_doc(
        self: &Arc<Self>,
        parents: Vec<BigKeyhiveAuthority>,
    ) -> Result<DocumentId, CreateDocError> {
        Ok(self.runtime.allocate_doc(parents).await?)
    }

    pub async fn finalize_allocated_doc(
        self: &Arc<Self>,
        doc_id: DocumentId,
        initial_content: automerge::Automerge,
        pending_group: BigKeyhiveGroup,
    ) -> Result<BigDocHandle, CreateDocError> {
        self.finalize_allocated_doc_with_keys(doc_id, initial_content, pending_group, Vec::new())
            .await
    }

    pub async fn finalize_allocated_doc_with_keys(
        self: &Arc<Self>,
        doc_id: DocumentId,
        initial_content: automerge::Automerge,
        pending_group: BigKeyhiveGroup,
        initial_keys: Vec<(Vec<u8>, [u8; 32])>,
    ) -> Result<BigDocHandle, CreateDocError> {
        let bundle = self
            .runtime
            .finalize_allocated_doc(doc_id, initial_content, pending_group, initial_keys)
            .await?;
        Ok(BigDocHandle {
            repo: Arc::clone(self),
            bundle,
        })
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
        let (group, _hashes) = self
            .keyhive
            .create_group_with_parents(parents, &self.keyhive_protocol)
            .await?;
        self.wait_for_keyhive_reconciliation().await?;
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

        let (affected_docs, _hashes) = self
            .keyhive
            .add_member_to_group(member, group, access, after_content, &self.keyhive_protocol)
            .await?;

        // BigRepo's contract is history-inclusive for reader grants. Publish a
        // key-only post-grant entrypoint without mutating the Automerge doc.
        if access.is_reader() {
            for doc_id in &affected_docs {
                let _doc = docs
                    .get(doc_id)
                    .ok_or_else(|| ferr!("affected document was not preflighted: {doc_id}"))?;
                if !self.runtime.ensure_causal_coverage(*doc_id).await? {
                    tracing::debug!(%doc_id, "group grant causal checkpoint deferred to durable event reconciliation");
                }
            }
        }

        self.wait_for_keyhive_reconciliation().await?;
        Ok(())
    }

    /// Grant document access.
    ///
    /// Reader grants also attempt a key-only causal checkpoint so the readable
    /// history survives reopen and sync without mutating document content. A
    /// durable Keyhive-event consumer retries when causal keys are still in flight.
    pub async fn grant_doc_access(
        self: &Arc<Self>,
        doc_id: DocumentId,
        principal: impl Into<BigKeyhiveAuthority>,
        access: keyhive_core::access::Access,
    ) -> Res<()> {
        let heads = match self.doc_head_state(doc_id).await {
            Ok(state) => state.sedimentree_heads,
            Err(err) => {
                tracing::debug!(%doc_id, %err, "doc_head_state unavailable for grant_doc_access boundary; using empty heads");
                Default::default()
            }
        };
        let after_content = heads.iter().map(|head| head.0.to_vec()).collect();

        let _hashes = self
            .keyhive
            .grant_doc_access(
                principal,
                doc_id,
                access,
                after_content,
                &self.keyhive_protocol,
            )
            .await?;

        if access.is_reader() && !self.runtime.ensure_causal_coverage(doc_id).await? {
            tracing::debug!(%doc_id, "document grant causal checkpoint deferred to durable event reconciliation");
        }

        self.wait_for_keyhive_reconciliation().await?;
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
        let _hashes = self
            .keyhive
            .revoke_doc_access(
                principal,
                doc_id,
                true,
                after_content,
                &self.keyhive_protocol,
            )
            .await?;
        if !self.runtime.ensure_causal_coverage(doc_id).await? {
            tracing::debug!(%doc_id, "document revocation causal checkpoint deferred to durable event reconciliation");
        }
        self.wait_for_keyhive_reconciliation().await?;
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
        let (peer_id, closed, end_rx) = self
            .runtime
            .open_connection(peer_id, Box::new((endpoint, endpoint_addr)))
            .await?;
        watch_connection_end(
            peer_id,
            Arc::clone(&closed),
            end_rx,
            end_signal_tx,
            &self.connection_tasks,
        );
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
        endpoint: iroh::Endpoint,
        end_signal_tx: Option<tokio::sync::mpsc::UnboundedSender<ConnFinishSignal>>,
    ) -> Res<BigRepoConnection> {
        let (peer_id, closed, end_rx) = self
            .runtime
            .accept_connection(Box::new((conn, Some(endpoint))))
            .await?;
        watch_connection_end(
            peer_id,
            Arc::clone(&closed),
            end_rx,
            end_signal_tx,
            &self.connection_tasks,
        );
        Ok(BigRepoConnection {
            repo: Arc::clone(self),
            peer_id,
            closed,
        })
    }
}

/// Forward a runtime connection-end to the caller's `ConnFinishSignal`
/// channel (used by the sync layer to release per-peer state when a
/// connection drops, whether outbound or inbound).
pub(crate) fn watch_connection_end(
    peer_id: PeerId,
    closed_flag: std::sync::Arc<std::sync::atomic::AtomicBool>,
    end_rx: futures::channel::oneshot::Receiver<(
        std::sync::Arc<std::sync::atomic::AtomicBool>,
        eyre::Result<()>,
    )>,
    end_signal_tx: Option<tokio::sync::mpsc::UnboundedSender<ConnFinishSignal>>,
    tasks: &utils_rs::AbortableJoinSet,
) {
    let Some(end_signal_tx) = end_signal_tx else {
        return;
    };
    drop(tasks.spawn(async move {
        let (closed, result) = end_rx.await.unwrap_or_else(|_| {
            // The runtime stopped before its watcher fired; treat the
            // connection as ended without a transport error. Use the connection's
            // existing closed flag to preserve pointer identity.
            closed_flag.store(true, std::sync::atomic::Ordering::SeqCst);
            (closed_flag, Ok(()))
        });
        let err = result.err();
        end_signal_tx
            .send(ConnFinishSignal {
                peer_id,
                closed,
                err,
            })
            .ok();
    }));
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
    /// The ended connection's end flag (shared with the runtime's watcher).
    /// Lets consumers distinguish WHICH connection ended when a peer id is
    /// reused across connections (e.g. re-establishment after a replace).
    pub closed: std::sync::Arc<std::sync::atomic::AtomicBool>,
    pub err: Option<eyre::Report>,
}

impl BigRepoConnection {
    pub fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    /// The connection's end flag, shared with the runtime's watcher; use for
    /// identity comparisons against [`ConnFinishSignal::closed`].
    pub fn closed_flag(&self) -> std::sync::Arc<std::sync::atomic::AtomicBool> {
        std::sync::Arc::clone(&self.closed)
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Initiate a keyhive protocol sync with the connected peer.
    pub async fn sync_keyhive_with_peer(&self) -> Res<()> {
        if self.is_closed() {
            return Err(ferr!("connection is closed"));
        }
        self.repo.runtime.sync_keyhive_with_peer(self.peer_id).await
    }

    /// NOTE: a succesful outcome doesn't correspond to doc
    /// handles having the latest heads
    pub async fn sync_doc_with_peer(&self, doc_id: DocumentId) -> Result<(), SyncDocError> {
        if self.is_closed() {
            return Err(SyncDocError::IoError(ferr!("connection is closed")));
        }
        self.repo
            .runtime
            .sync_doc_with_peer(doc_id, self.peer_id)
            .await
    }

    pub async fn sync_doc_with_peer_receipt(
        &self,
        doc_id: DocumentId,
    ) -> Result<SyncDocReceipt, SyncDocError> {
        if self.is_closed() {
            return Err(SyncDocError::IoError(ferr!("connection is closed")));
        }
        self.repo
            .runtime
            .sync_doc_with_peer_receipt(doc_id, self.peer_id)
            .await
    }

    pub async fn stop(self) -> Res<()> {
        // Per-connection close: pass this connection's end flag so the
        // runtime tears down only this connection's registration (a
        // superseded connection's stop must not affect the replacement).
        self.repo
            .runtime
            .close_connection(self.peer_id, std::sync::Arc::clone(&self.closed))
            .await
    }
}

// change listeners
impl BigRepo {
    /// Subscribe to local document lifecycle and materialization notifications.
    ///
    /// The returned registration must be retained for as long as the receiver
    /// is needed; dropping it unregisters the listener. Consumers should use
    /// these notifications as wakeups and re-read durable state rather than as
    /// a source of projection data.
    pub async fn subscribe_local_listener(
        self: &Arc<Self>,
        filter: BigRepoLocalFilter,
    ) -> Res<(
        BigRepoLocalListenerRegistration,
        tokio::sync::mpsc::UnboundedReceiver<Vec<BigRepoLocalNotification>>,
    )> {
        self.change_manager.subscribe_local_listener(filter).await
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
    connection_tasks: Arc<utils_rs::AbortableJoinSet>,
}

impl BigRepoStopToken {
    pub async fn stop(mut self) -> Res<()> {
        let _res = self
            .connection_tasks
            .stop(std::time::Duration::from_secs(5))
            .await;
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
    bundle: Arc<runtime2::types::LiveDocBundle>,
}

impl std::fmt::Debug for BigDocHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BigDocHandle")
            .field("document_id", &self.document_id())
            .finish()
    }
}

impl BigRepo {
    /// Finalize a new branch while retaining the source document's encrypted
    /// causal history. The keys remain inside BigRepo.
    pub async fn finalize_allocated_doc_from_parent(
        self: &Arc<BigRepo>,
        doc_id: DocumentId,
        initial_content: automerge::Automerge,
        pending_group: BigKeyhiveGroup,
        source: &BigDocHandle,
    ) -> Result<BigDocHandle, CreateDocError> {
        let initial_keys = source.content_keys().await?;
        self.finalize_allocated_doc_with_keys(doc_id, initial_content, pending_group, initial_keys)
            .await
    }
}

impl BigDocHandle {
    pub fn document_id(&self) -> DocumentId {
        self.bundle.doc_id
    }

    pub(crate) async fn content_keys(&self) -> Res<Vec<(Vec<u8>, [u8; 32])>> {
        self.repo
            .keyhive
            .document_content_keys(self.document_id())
            .await
    }

    /// Whether this live handle is missing one or more decryption keys.
    pub fn is_partially_decrypted(&self) -> bool {
        self.bundle.is_partially_decrypted()
    }

    /// The current BeeKEM/PCS epoch observed by this document handle.
    pub fn current_causal_epoch(&self) -> Option<[u8; 32]> {
        self.bundle.current_causal_epoch()
    }

    pub async fn with_document_read<F, R>(&self, operation: F) -> R
    where
        F: FnOnce(&automerge::Automerge) -> R,
    {
        surelock::key::lock_scope(|key| {
            let (doc, _key) = key.lock(&self.bundle.doc);
            operation(&doc)
        })
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
        // Fast-fail on an invalidated handle before doing any work. The
        // authoritative rejection happens at the worker commit path; this
        // check only avoids running the mutation against a known-dead bundle.
        if self.bundle.is_broken() {
            return Err(ferr!(
                "document write rejected: handle invalidated by an earlier rejected commit; re-acquire the document"
            ));
        }

        // All automerge work happens under a short sync lock; nothing is held
        // across an await (the commit goes out only after the lock scope ends).
        let (out, commit) = surelock::key::lock_scope(|key| {
            let (mut doc, _key) = key.lock(&self.bundle.doc);
            let before_heads = doc.get_heads();
            let out = operation(&mut doc);
            let after_heads = doc.get_heads();
            if before_heads == after_heads {
                return (out, None);
            }

            // Capture the current materialization while holding the same lock
            // as the mutation. Any newly retained head must remain
            // independently loadable after sedimentree minimization discards
            // its predecessors.
            let snapshot = doc.save();
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
                    let bytes = if after_heads.contains(&change.hash()) {
                        snapshot.clone()
                    } else {
                        change.raw_bytes().to_vec()
                    };
                    (head, parents, bytes)
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
            (out, Some((after_heads, changes, patches)))
        });
        let Some((after_heads, changes, patches)) = commit else {
            return Ok(out);
        };

        self.repo
            .runtime
            .commit_delta(
                self.document_id(),
                self.bundle.id(),
                changes,
                after_heads,
                patches,
                origin,
            )
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
