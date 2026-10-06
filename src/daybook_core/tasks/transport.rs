//! Retained encrypted pool records over the dedicated native task scope.
//!
//! The owner, not a directory hint or delivering peer, selects a pool binding.
//! BigSync completion follows checked durable admission. Remote membership loss
//! is not authenticated retirement and is rejected rather than deleting evidence.

use super::{TaskPoolId, pool::PoolDescriptorSnapshot, storage::RegisterStore, store::TaskStore};
use crate::interlude::*;
use crate::rt::triage::domain::{ProcessorRotationControl, control::ProcessorTransportSnapshot};
use big_repo::keyhive_core::access::Access;
use big_repo::{BigRepo, CoordinationError};
use big_sync::{BackendId, HostPartStore, SqlitePartStore, SyncBackend, SyncTaskRunOutcome};
use big_sync_core::encrypted_register::MergeOutcome;
use big_sync_core::part_store::ObjPayload;
use big_sync_core::{ObjKey, PartKey, PeerKey, SyncCompletionDeets, SyncTaskCompletion};
use utils_rs::prelude::eyre::ensure;

pub(crate) const TASK_BACKEND_ID: &str = "daybook-task-registers";
pub(crate) const TASK_SCOPE_KEY: &str = "daybook-tasks";

struct OwnedPool {
    snapshot: PoolDescriptorSnapshot,
    tasks: Arc<TaskStore>,
    audience: HashMap<PeerKey, Access>,
    refresh_required: bool,
    permissions_dirty: bool,
}

struct OwnedProcessor {
    snapshot: Arc<ProcessorTransportSnapshot>,
    slots: Arc<crate::rt::triage::slots::ProcessorSlotStore>,
    audience: HashMap<PeerKey, Access>,
    refresh_required: bool,
    permissions_dirty: bool,
    hint_pending: bool,
}

enum BoundStore {
    Tasks(Arc<TaskStore>),
    Processor(Arc<crate::rt::triage::slots::ProcessorSlotStore>),
}

impl BoundStore {
    fn register(&self) -> &RegisterStore {
        match self {
            Self::Tasks(store) => store.register(),
            Self::Processor(store) => store.register(),
        }
    }
}

#[cfg(test)]
struct PermissionPause {
    part: Option<PartKey>,
    entered: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
}

/// Exactly one strongly retained task-store owner per explicitly attached pool.
/// Its one task-scope store and Notify are shared by registers, worker and RPC.
/// There is no scheduling consumer or scheduling ALPN in this transport owner.
pub struct TaskSyncBackend {
    rcx: std::sync::Weak<crate::repo::RepoCtx>,
    repo: Arc<BigRepo>,
    parts: Arc<SqlitePartStore>,
    pools: std::sync::RwLock<HashMap<TaskPoolId, OwnedPool>>,
    processors: std::sync::RwLock<HashMap<PartKey, OwnedProcessor>>,
    admission: tokio::sync::Mutex<()>,
    native_owner: std::sync::OnceLock<std::sync::Weak<crate::sync::IrohSyncRepo>>,
    refresh_wakeup: tokio::sync::Notify,
    selection_changed: tokio::sync::watch::Sender<()>,
    #[cfg(test)]
    admission_errors: tokio::sync::broadcast::Sender<ObjKey>,
    #[cfg(test)]
    pending_once: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    permission_pause: std::sync::Mutex<Option<PermissionPause>>,
    #[cfg(test)]
    permission_updates: tokio::sync::broadcast::Sender<bool>,
}

impl TaskSyncBackend {
    pub(crate) async fn boot(rcx: Arc<crate::repo::RepoCtx>) -> Res<Self> {
        let owner = Self {
            rcx: Arc::downgrade(&rcx),
            parts: rcx.coordination_part_store().await?,
            repo: Arc::clone(&rcx.big_repo),
            pools: Default::default(),
            processors: Default::default(),
            admission: Default::default(),
            native_owner: Default::default(),
            refresh_wakeup: Default::default(),
            selection_changed: tokio::sync::watch::channel(()).0,
            #[cfg(test)]
            admission_errors: tokio::sync::broadcast::channel(16).0,
            #[cfg(test)]
            pending_once: Default::default(),
            #[cfg(test)]
            permission_pause: Default::default(),
            #[cfg(test)]
            permission_updates: tokio::sync::broadcast::channel(16).0,
        };
        let mut write = owner
            .parts
            .begin_obj_write(ObjKey::new(b"daybook-task-transport-control"))
            .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS task_pool_transport_bindings (
              scope_id INTEGER NOT NULL
            , pool_id TEXT NOT NULL
            , register_scope BLOB NOT NULL
            , part_id BLOB NOT NULL
            , identity_json TEXT NOT NULL
            , PRIMARY KEY(scope_id, pool_id)
            , UNIQUE(scope_id, register_scope)
            , UNIQUE(scope_id, part_id)
        )",
        )
        .execute(&mut **write.context_mut())
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS processor_slot_transport_bindings (
                scope_id INTEGER NOT NULL
              , processor_id TEXT NOT NULL
              , register_scope BLOB NOT NULL
              , part_id BLOB NOT NULL
              , identity_json TEXT NOT NULL
              , PRIMARY KEY(scope_id, processor_id)
              , UNIQUE(scope_id, register_scope)
              , UNIQUE(scope_id, part_id)
            )",
        )
        .execute(&mut **write.context_mut())
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS task_pool_current_control (
                scope_id INTEGER NOT NULL
              , pool_id TEXT NOT NULL
              , certificate_json TEXT NOT NULL
              , PRIMARY KEY(scope_id, pool_id)
            )",
        )
        .execute(&mut **write.context_mut())
        .await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS encrypted_register_current_control (register_scope BLOB PRIMARY KEY, certificate_json TEXT NOT NULL)")
            .execute(&mut **write.context_mut()).await?;
        write.commit().await?;
        // The dedicated task scope also serves retained predecessors. None of
        // their persisted grants is authority at restart; clear the whole part
        // audience atomically before RPC starts, without scanning task records.
        owner.parts.clear_part_members().await?;
        Ok(owner)
    }

    pub(crate) fn shared_store(&self) -> Arc<SqlitePartStore> {
        Arc::clone(&self.parts)
    }

    pub(crate) fn selected_pool(
        &self,
        pool: &TaskPoolId,
    ) -> Option<(PoolDescriptorSnapshot, Arc<TaskStore>)> {
        self.pools
            .read()
            .expect(ERROR_MUTEX)
            .get(pool)
            .map(|owned| (owned.snapshot.clone(), Arc::clone(&owned.tasks)))
    }

    pub(crate) fn selection_changes(&self) -> tokio::sync::watch::Receiver<()> {
        self.selection_changed.subscribe()
    }

    pub(crate) fn set_native_owner(&self, owner: &Arc<crate::sync::IrohSyncRepo>) {
        assert!(
            self.native_owner.set(Arc::downgrade(owner)).is_ok(),
            "native task owner already installed"
        );
    }

    pub(crate) async fn source_control(
        &self,
        peer: &PeerKey,
        request: &super::rpc::RegisterSourceControlRequest,
    ) -> Result<super::RegisterSourceControl, CoordinationError> {
        let store = self
            .processors
            .read()
            .expect(ERROR_MUTEX)
            .values()
            .find(|owned| owned.slots.register().scope() == request.register_scope)
            .map(|owned| BoundStore::Processor(Arc::clone(&owned.slots)))
            .or_else(|| {
                self.pools
                    .read()
                    .expect(ERROR_MUTEX)
                    .values()
                    .find(|owned| owned.tasks.register().scope() == request.register_scope)
                    .map(|owned| BoundStore::Tasks(Arc::clone(&owned.tasks)))
            })
            .ok_or(CoordinationError::Invalid(
                "source register is not attached",
            ))?;
        store
            .register()
            .source_control(peer, &request.authority_document, &request.key_ref)
            .await
            .map_err(|error| match error.downcast::<CoordinationError>() {
                Ok(error) => error,
                Err(error) => CoordinationError::Other(error),
            })
    }

    #[cfg(test)]
    pub(crate) fn admission_errors(&self) -> tokio::sync::broadcast::Receiver<ObjKey> {
        self.admission_errors.subscribe()
    }

    pub(crate) fn get(&self, pool: &TaskPoolId) -> Option<Arc<TaskStore>> {
        self.pools
            .read()
            .expect(ERROR_MUTEX)
            .get(pool)
            .map(|owned| Arc::clone(&owned.tasks))
    }

    /// Current control carries no history. Reuse only this receiver's durably
    /// admitted source provenance; an RPC peer cannot supply that projection.
    pub(crate) async fn retain_control_sources(
        &self,
        snapshot: &mut PoolDescriptorSnapshot,
    ) -> Res<()> {
        let mut write = self
            .parts
            .begin_obj_write(ObjKey::new(b"daybook-task-transport-control"))
            .await?;
        let identity: Option<String> = sqlx::query_scalar(
            "SELECT identity_json
               FROM task_pool_transport_bindings
              WHERE scope_id = ? AND pool_id = ?",
        )
        .bind(write.scope_id())
        .bind(snapshot.descriptor.pool_id.as_str())
        .fetch_optional(&mut **write.context_mut())
        .await?;
        // This is an indexed read of an existing control object, not publication.
        drop(write);
        let Some(identity) = identity else {
            return Ok(());
        };
        let (document, group, descriptor): (String, [u8; 32], String) =
            serde_json::from_str(&identity)?;
        let previous = super::pool::TaskPoolDescriptor::decode(&descriptor)?;
        let previous_document: DocumentId = document.parse()?;
        ensure!(
            group == snapshot.reference.authority_group
                && previous.register_scope == snapshot.descriptor.register_scope
                && previous.register_incarnation == snapshot.descriptor.register_incarnation,
            "current control changes admitted logical or authority identity"
        );
        if snapshot.descriptor.rotation_generation < previous.rotation_generation
            || (snapshot.descriptor.rotation_generation == previous.rotation_generation
                && snapshot.reference.document > previous_document)
        {
            return Ok(());
        }
        let next = &mut snapshot.descriptor;
        if previous.active_task_part != next.active_task_part {
            next.predecessor_parts.insert(previous.active_task_part);
        }
        next.predecessor_parts.extend(previous.predecessor_parts);
        next.allowed_key_refs.extend(previous.allowed_key_refs);
        for (document, group) in previous.historical_key_authorities {
            if let Some(existing) = next.historical_key_authorities.insert(document, group) {
                ensure!(
                    existing == group,
                    "retained control provenance changes authority"
                );
            }
        }
        if previous_document != snapshot.reference.document {
            next.historical_key_authorities
                .insert(previous_document.to_string(), group);
        }
        Ok(())
    }

    pub(crate) fn sponsored_processors(&self) -> Vec<String> {
        self.processors
            .read()
            .expect(ERROR_MUTEX)
            .values()
            .map(|owned| owned.snapshot.processor_full_id.clone())
            .collect()
    }

    pub(crate) async fn cached_processor_control(
        &self,
        processor: &str,
    ) -> Res<Option<ProcessorRotationControl>> {
        let mut write = self
            .parts
            .begin_obj_write(ObjKey::new(b"daybook-task-transport-control"))
            .await?;
        let certificate: Option<String> = sqlx::query_scalar(
            "SELECT control.certificate_json
               FROM encrypted_register_current_control AS control
               JOIN processor_slot_transport_bindings AS binding
                 ON binding.register_scope = control.register_scope
              WHERE binding.scope_id = ? AND binding.processor_id = ?",
        )
        .bind(write.scope_id())
        .bind(processor)
        .fetch_optional(&mut **write.context_mut())
        .await?;
        certificate
            .map(|certificate| serde_json::from_str(&certificate).map_err(Into::into))
            .transpose()
    }

    pub(crate) async fn current_processor_control(
        &self,
        processor: &str,
        peer: &PeerKey,
    ) -> Result<ProcessorRotationControl, CoordinationError> {
        // Control must remain readable while post-commit permission installation awaits.
        // The durable-selection check below fences a sampled retained owner.
        let (reference, slots) = {
            let processors = self.processors.read().expect(ERROR_MUTEX);
            let owned = processors
                .values()
                .find(|owned| owned.snapshot.processor_full_id == processor)
                .ok_or(CoordinationError::Invalid("processor is not attached"))?;
            (owned.snapshot.reference.clone(), Arc::clone(&owned.slots))
        };
        let authority = slots
            .register()
            .control_authority()
            .await
            .map_err(|error| match error.downcast::<CoordinationError>() {
                Ok(error) => error,
                Err(error) => CoordinationError::Other(error),
            })?;
        if authority
            .local_access()
            .is_some_and(|access| access >= Access::Edit)
        {
            let statement = {
                let processors = self.processors.read().expect(ERROR_MUTEX);
                let owned = processors
                    .values()
                    .find(|owned| owned.snapshot.processor_full_id == processor)
                    .ok_or(CoordinationError::Invalid("processor is not attached"))?;
                if owned.snapshot.reference != reference {
                    return Err(CoordinationError::Pending);
                }
                ProcessorRotationControl::statement(&owned.snapshot)?
            };
            return ProcessorRotationControl::sign(&self.repo, statement, peer, &authority).await;
        }
        let control = self
            .cached_processor_control(processor)
            .await
            .map_err(CoordinationError::Other)?
            .ok_or(CoordinationError::Unauthorized)?;
        let admitted = control
            .admit(&self.repo, processor, reference.authority_group, Some(peer))
            .await
            .map_err(|error| match error.downcast::<CoordinationError>() {
                Ok(error) => error,
                Err(error) => CoordinationError::Other(error),
            })?;
        if admitted.reference != reference
            || admitted.register_binding.generation != slots.register().generation()
        {
            // Certificate/selection commit can precede transport-owner replacement.
            return Err(CoordinationError::Pending);
        }
        Ok(control)
    }

    // The authenticated locator is additive; it never enables local execution.
    // Reconstructed owners retry this after cancellation or restart.
    async fn publish_processor_hint(&self, processor: &str) -> Res<()> {
        let snapshot = {
            let processors = self.processors.read().expect(ERROR_MUTEX);
            let owned = processors
                .values()
                .find(|owned| owned.snapshot.processor_full_id == processor)
                .ok_or_eyre("processor is not attached")?;
            if !owned.hint_pending {
                return Ok(());
            }
            Arc::clone(&owned.snapshot)
        };
        let rcx = self.rcx.upgrade().ok_or_eyre("processor owner is closed")?;
        crate::rt::triage::domain::ProcessorDomainRepo::new(
            Arc::clone(&rcx.big_repo),
            rcx.local_actor_id.clone(),
        )
        .publish_candidate(
            &rcx.doc_config.document_id(),
            processor,
            crate::rt::triage::domain::ProcessorConfigCandidate {
                generation: snapshot.register_binding.generation,
                reference: snapshot.reference.clone(),
            },
        )
        .await?;
        self.processors
            .write()
            .expect(ERROR_MUTEX)
            .values_mut()
            .find(|owned| owned.snapshot.processor_full_id == processor)
            .expect(ERROR_IMPOSSIBLE)
            .hint_pending = false;
        Ok(())
    }

    pub(crate) async fn accept_processor_control(
        &self,
        processor: &str,
        peer: &PeerKey,
        control: &ProcessorRotationControl,
    ) -> Result<bool, CoordinationError> {
        let _admission = self.admission.lock().await;
        let (reference, generation) = {
            let processors = self.processors.read().expect(ERROR_MUTEX);
            let owned = processors
                .values()
                .find(|owned| owned.snapshot.processor_full_id == processor)
                .ok_or(CoordinationError::Invalid("processor is not attached"))?;
            (
                owned.snapshot.reference.clone(),
                owned.slots.register().generation(),
            )
        };
        let snapshot = control
            .admit(&self.repo, processor, reference.authority_group, Some(peer))
            .await
            .map_err(|error| match error.downcast::<CoordinationError>() {
                Ok(error) => error,
                Err(_) => CoordinationError::Invalid("invalid offered processor control"),
            })?;
        if snapshot.register_binding.generation < generation
            || (snapshot.register_binding.generation == generation
                && snapshot.reference.document > reference.document)
        {
            return Ok(false);
        }
        let advanced =
            snapshot.register_binding.generation > generation || snapshot.reference != reference;
        if !advanced
            && let Some(cached) = self
                .cached_processor_control(processor)
                .await
                .map_err(CoordinationError::Other)?
        {
            match cached
                .admit(&self.repo, processor, reference.authority_group, None)
                .await
            {
                Ok(admitted)
                    if admitted.reference == reference
                        && admitted.register_binding.generation == generation =>
                {
                    self.publish_processor_hint(processor)
                        .await
                        .map_err(|error| match error.downcast::<CoordinationError>() {
                            Ok(error) => error,
                            Err(error) => CoordinationError::Other(error),
                        })?;
                    return Ok(false);
                }
                Ok(_) => {
                    return Err(CoordinationError::Invalid(
                        "cached processor control is not selected",
                    ));
                }
                Err(error) => match error.downcast::<CoordinationError>() {
                    Ok(CoordinationError::Unauthorized) => {}
                    Ok(error) => return Err(error),
                    Err(error) => return Err(CoordinationError::Other(error)),
                },
            }
        }
        self.attach_processor_locked(snapshot, Some(control))
            .await
            .map_err(|error| match error.downcast::<CoordinationError>() {
                Ok(error) => error,
                Err(error) => CoordinationError::Other(error),
            })?;
        Ok(advanced)
    }

    pub(crate) fn sponsored_pools(&self) -> Vec<TaskPoolId> {
        self.pools
            .read()
            .expect(ERROR_MUTEX)
            .keys()
            .cloned()
            .collect()
    }

    /// An offer can advance an existing sponsorship, never create a new one.
    /// Authenticate even losing offers before comparing their generation.
    pub(crate) async fn accept_current_control(
        &self,
        pool: &TaskPoolId,
        peer: &PeerKey,
        control: &super::pool::PoolRotationControl,
    ) -> Result<bool, CoordinationError> {
        let _admission = self.admission.lock().await;
        let selected = self
            .pools
            .read()
            .expect(ERROR_MUTEX)
            .get(pool)
            .ok_or(CoordinationError::Invalid("pool is not attached"))?
            .snapshot
            .reference
            .clone();
        let mut snapshot = control
            .admit_for_peer(&self.repo, pool, selected.authority_group, Some(peer))
            .await
            .map_err(|error| match error.downcast::<CoordinationError>() {
                Ok(error) => error,
                Err(_) => CoordinationError::Invalid("invalid offered current control"),
            })?;
        if snapshot.reference.rotation_generation < selected.rotation_generation
            || (snapshot.reference.rotation_generation == selected.rotation_generation
                && snapshot.reference.document > selected.document)
        {
            return Ok(false);
        }
        let advanced = snapshot.reference != selected;
        if !advanced {
            // A Relay initially knows its provisioned binding, not a signed
            // advertisement. Retain its first certificate without repeatedly
            // reinstalling permissions on every equal-generation exchange.
            if let Some(cached) = self
                .cached_current_control(pool)
                .await
                .map_err(CoordinationError::Other)?
            {
                match cached
                    .admit(&self.repo, pool, selected.authority_group)
                    .await
                {
                    Ok(admitted) if admitted.reference == selected => return Ok(false),
                    Ok(_) => {
                        return Err(CoordinationError::Invalid("cached control is not selected"));
                    }
                    Err(error) => match error.downcast::<CoordinationError>() {
                        Ok(CoordinationError::Unauthorized) => {}
                        Ok(error) => return Err(error),
                        Err(error) => return Err(CoordinationError::Other(error)),
                    },
                }
            }
        }
        // Native admission supplied the control; carry only our own durable
        // provenance, not historical identities selected by the sending peer.
        self.retain_control_sources(&mut snapshot)
            .await
            .map_err(CoordinationError::Other)?;
        self.attach_locked(snapshot, Some(control))
            .await
            .map_err(|error| match error.downcast::<CoordinationError>() {
                Ok(error) => error,
                Err(error) => CoordinationError::Other(error),
            })?;
        Ok(advanced)
    }

    pub(crate) async fn current_control(
        &self,
        pool: &TaskPoolId,
        peer: &PeerKey,
    ) -> Result<super::pool::PoolRotationControl, CoordinationError> {
        // Admission may be suspended after commit; advertisement is a fenced read,
        // not another mutation of that admission transaction.
        let (reference, tasks) = {
            let pools = self.pools.read().expect(ERROR_MUTEX);
            let owned = pools
                .get(pool)
                .ok_or(CoordinationError::Invalid("pool is not attached"))?;
            (owned.snapshot.reference.clone(), Arc::clone(&owned.tasks))
        };
        let authority = tasks
            .register()
            .control_authority()
            .await
            .map_err(|error| match error.downcast::<CoordinationError>() {
                Ok(error) => error,
                Err(error) => CoordinationError::Other(error),
            })?;
        if authority
            .local_access()
            .is_some_and(|access| access >= Access::Edit)
        {
            let statement = {
                let pools = self.pools.read().expect(ERROR_MUTEX);
                let owned = pools
                    .get(pool)
                    .ok_or(CoordinationError::Invalid("pool is not attached"))?;
                if owned.snapshot.reference != reference {
                    return Err(CoordinationError::Pending);
                }
                super::pool::PoolRotationControl::statement(&owned.snapshot)?
            };
            return super::pool::PoolRotationControl::sign(&self.repo, statement, peer, &authority)
                .await;
        }
        // Relay forwarding preserves the publisher's signature; Relay never
        // borrows an Edit signer or substitutes its own transport declaration.
        let control = self
            .cached_current_control(pool)
            .await
            .map_err(CoordinationError::Other)?
            .ok_or(CoordinationError::Unauthorized)?;
        let admitted = control
            .admit_for_peer(&self.repo, pool, reference.authority_group, Some(peer))
            .await
            .map_err(|error| match error.downcast::<CoordinationError>() {
                Ok(error) => error,
                Err(error) => CoordinationError::Other(error),
            })?;
        if admitted.reference != reference {
            return Err(CoordinationError::Pending);
        }
        Ok(control)
    }

    pub(crate) async fn cached_current_control(
        &self,
        pool: &TaskPoolId,
    ) -> Res<Option<super::pool::PoolRotationControl>> {
        let mut write = self
            .parts
            .begin_obj_write(ObjKey::new(b"daybook-task-transport-control"))
            .await?;
        let certificate: Option<String> = sqlx::query_scalar(
            "SELECT certificate_json
               FROM task_pool_current_control
              WHERE scope_id = ? AND pool_id = ?",
        )
        .bind(write.scope_id())
        .bind(pool.as_str())
        .fetch_optional(&mut **write.context_mut())
        .await?;
        drop(write);
        certificate
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(Into::into)
    }

    async fn remember_current_control(
        write: &mut big_sync::SqliteObjWrite<'_>,
        pool: &TaskPoolId,
        control: &super::pool::PoolRotationControl,
    ) -> Res<()> {
        let certificate = serde_json::to_string(control)?;
        sqlx::query(
            "INSERT INTO task_pool_current_control (scope_id, pool_id, certificate_json)
             VALUES (?, ?, ?)
             ON CONFLICT(scope_id, pool_id) DO UPDATE
                SET certificate_json = excluded.certificate_json",
        )
        .bind(write.scope_id())
        .bind(pool.as_str())
        .bind(certificate)
        .execute(&mut **write.context_mut())
        .await?;
        Ok(())
    }

    pub(crate) fn routes(&self, peer: &PeerKey) -> HashMap<PartKey, BackendId> {
        let mut routes = HashMap::new();
        let backend: BackendId = Arc::from(TASK_BACKEND_ID);
        for owned in self.pools.read().expect(ERROR_MUTEX).values() {
            if owned
                .audience
                .get(peer)
                .is_some_and(|access| *access >= Access::Relay)
            {
                owned.tasks.register().for_each_transport_part(|part| {
                    routes.insert(part.clone(), Arc::clone(&backend));
                });
            }
        }
        for owned in self.processors.read().expect(ERROR_MUTEX).values() {
            if owned
                .audience
                .get(peer)
                .is_some_and(|access| *access >= Access::Relay)
            {
                owned.slots.register().for_each_transport_part(|part| {
                    routes.insert(part.clone(), Arc::clone(&backend));
                });
            }
        }
        routes
    }

    fn validate_pool_successor(
        previous_document: &DocumentId,
        previous: &super::pool::TaskPoolDescriptor,
        next: &PoolDescriptorSnapshot,
    ) -> Res<()> {
        let descriptor = &next.descriptor;
        ensure!(
            descriptor.pool_id == previous.pool_id
                && descriptor.authority_group == previous.authority_group
                && descriptor.register_scope == previous.register_scope
                && descriptor.register_incarnation == previous.register_incarnation,
            "pool successor changes logical or authority identity"
        );
        ensure!(
            descriptor.rotation_generation > previous.rotation_generation
                || (descriptor.rotation_generation == previous.rotation_generation
                    && next.reference.document < *previous_document),
            "pool successor is not the deterministic winner"
        );
        ensure!(
            descriptor
                .predecessor_parts
                .contains(&previous.active_task_part)
                || descriptor.active_task_part == previous.active_task_part,
            "pool successor omits predecessor transport"
        );
        // Previously admitted key provenance is retained by RegisterStore's
        // durable owner handoff, including keys from concurrent losing branches.
        Ok(())
    }

    pub(crate) async fn attach(
        &self,
        snapshot: PoolDescriptorSnapshot,
        control: Option<&super::pool::PoolRotationControl>,
    ) -> Res<Arc<TaskStore>> {
        let _admission = self.admission.lock().await;
        self.attach_locked(snapshot, control).await
    }

    // The caller holds admission through durable selection and owner replacement.
    async fn attach_locked(
        &self,
        snapshot: PoolDescriptorSnapshot,
        control: Option<&super::pool::PoolRotationControl>,
    ) -> Res<Arc<TaskStore>> {
        if let Some(control) = control {
            let admitted = control
                .admit(
                    &self.repo,
                    &snapshot.descriptor.pool_id,
                    snapshot.reference.authority_group,
                )
                .await?;
            assert_eq!(
                admitted.reference, snapshot.reference,
                "admitted control attachment identity"
            );
        }
        let binding = snapshot
            .register_binding_for_access(&self.repo, Access::Relay)
            .await?;
        let source_owner = {
            let pools = self.pools.read().expect(ERROR_MUTEX);
            pools
                .get(&snapshot.descriptor.pool_id)
                .and_then(|existing| {
                    let selected = &existing.snapshot;
                    (snapshot.reference != selected.reference
                        && (snapshot.descriptor.rotation_generation
                            < selected.descriptor.rotation_generation
                            || (snapshot.descriptor.rotation_generation
                                == selected.descriptor.rotation_generation
                                && snapshot.reference.document > selected.reference.document)))
                        .then(|| (selected.clone(), Arc::clone(&existing.tasks)))
                })
        };
        if let Some((selected, tasks)) = source_owner {
            ensure!(
                snapshot.descriptor.authority_group == selected.descriptor.authority_group
                    && snapshot.descriptor.register_scope == selected.descriptor.register_scope
                    && snapshot.descriptor.register_incarnation
                        == selected.descriptor.register_incarnation,
                "losing pool proposal changes logical or authority identity"
            );
            tasks.register().adopt_source(&binding).await?;
            self.pools
                .write()
                .expect(ERROR_MUTEX)
                .get_mut(&selected.descriptor.pool_id)
                .expect(ERROR_IMPOSSIBLE)
                .permissions_dirty = true;
            self.install_audience(&selected).await?;
            return Ok(tasks);
        }
        let existing = {
            let pools = self.pools.read().expect(ERROR_MUTEX);
            if let Some(existing) = pools.get(&snapshot.descriptor.pool_id) {
                if existing.snapshot.reference == snapshot.reference
                    && existing.snapshot.descriptor == snapshot.descriptor
                {
                    Some(Arc::clone(&existing.tasks))
                } else {
                    Self::validate_pool_successor(
                        &existing.snapshot.reference.document,
                        &existing.snapshot.descriptor,
                        &snapshot,
                    )?;
                    None
                }
            } else {
                ensure!(
                    pools.values().all(|existing| {
                        existing.snapshot.descriptor.active_task_part != binding.part
                            && existing.snapshot.descriptor.register_scope != binding.scope
                    }),
                    "pool transport part or register scope is already owned"
                );
                None
            }
        };
        if let Some(tasks) = existing {
            if let Some(control) = control {
                let mut write = self
                    .parts
                    .begin_obj_write(ObjKey::new(b"daybook-task-transport-control"))
                    .await?;
                Self::remember_current_control(&mut write, &snapshot.descriptor.pool_id, control)
                    .await?;
                write.commit().await?;
                tasks
                    .register()
                    .remember_current_control(serde_json::to_string(control)?)
                    .await?;
            }
            self.install_audience(&snapshot).await?;
            return Ok(tasks);
        }
        // Retain binding identity across reopen. A transport attachment cannot
        // reinterpret prior writer fences/checkpoints under another descriptor.
        let identity = serde_json::to_string(&(
            snapshot.reference.document.to_string(),
            snapshot.reference.authority_group,
            snapshot.descriptor.encode()?,
        ))?;
        let mut write = self
            .parts
            .begin_obj_write(ObjKey::new(binding.part.as_bytes()))
            .await?;
        let scope_id = write.scope_id();
        let processor_collision: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                SELECT 1
                  FROM processor_slot_transport_bindings
                 WHERE scope_id = ? AND (part_id = ? OR register_scope = ?)
            )",
        )
        .bind(scope_id)
        .bind(binding.part.as_bytes())
        .bind(&binding.scope)
        .fetch_one(&mut **write.context_mut())
        .await?;
        ensure!(
            !processor_collision,
            "task pool transport overlaps a processor slot binding"
        );
        let existing: Option<String> = sqlx::query_scalar(
            "SELECT identity_json FROM task_pool_transport_bindings WHERE scope_id = ? AND pool_id = ?"
        ).bind(scope_id).bind(snapshot.descriptor.pool_id.as_str())
            .fetch_optional(&mut **write.context_mut()).await?;
        if let Some(existing) = existing {
            if existing != identity {
                let (document, group, descriptor): (String, [u8; 32], String) =
                    serde_json::from_str(&existing)?;
                ensure!(
                    group == snapshot.reference.authority_group,
                    "pool successor changes authority"
                );
                Self::validate_pool_successor(
                    &document.parse()?,
                    &super::pool::TaskPoolDescriptor::decode(&descriptor)?,
                    &snapshot,
                )?;
                sqlx::query(
                    "UPDATE task_pool_transport_bindings
                        SET part_id = ?
                          , identity_json = ?
                      WHERE scope_id = ? AND pool_id = ?",
                )
                .bind(binding.part.as_bytes())
                .bind(&identity)
                .bind(scope_id)
                .bind(snapshot.descriptor.pool_id.as_str())
                .execute(&mut **write.context_mut())
                .await?;
            }
        } else {
            let occupied: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM big_sync_parts WHERE scope_id = ? AND part_id = ?)",
            )
            .bind(scope_id)
            .bind(binding.part.as_bytes())
            .fetch_one(&mut **write.context_mut())
            .await?;
            ensure!(
                !occupied,
                "task part is already owned by another native store domain"
            );
            sqlx::query(
                "INSERT INTO task_pool_transport_bindings (
                  scope_id
                , pool_id
                , register_scope
                , part_id
                , identity_json
            ) VALUES (?, ?, ?, ?, ?)",
            )
            .bind(scope_id)
            .bind(snapshot.descriptor.pool_id.as_str())
            .bind(&binding.scope)
            .bind(binding.part.as_bytes())
            .bind(identity)
            .execute(&mut **write.context_mut())
            .await?;
        }
        if let Some(control) = control {
            Self::remember_current_control(&mut write, &snapshot.descriptor.pool_id, control)
                .await?;
        } else {
            // A provisioner-selected successor has no certificate yet. Clear
            // the old selection's certificate in the same binding transaction;
            // a Relay must not advertise or resume it as the new selection.
            sqlx::query("DELETE FROM task_pool_current_control WHERE scope_id = ? AND pool_id = ?")
                .bind(scope_id)
                .bind(snapshot.descriptor.pool_id.as_str())
                .execute(&mut **write.context_mut())
                .await?;
        }
        write.commit().await?;
        let register = RegisterStore::open_current(
            Arc::clone(&self.parts),
            Arc::clone(&self.repo),
            binding,
            control.map(serde_json::to_string).transpose()?,
        )
        .await?;
        let tasks = Arc::new(TaskStore::new(
            register,
            snapshot.descriptor.pool_id.clone(),
        ));
        // Track the owner and its unresolved projection before any permission
        // commit. Caller cancellation cannot leave a served, untracked part.
        self.pools.write().expect(ERROR_MUTEX).insert(
            snapshot.descriptor.pool_id.clone(),
            OwnedPool {
                snapshot: snapshot.clone(),
                tasks: Arc::clone(&tasks),
                audience: HashMap::new(),
                refresh_required: true,
                permissions_dirty: true,
            },
        );
        // Scheduling observes the strong owner, never the earlier SQL-only cut.
        // A watch wake cannot block native ingress on an actor awaiting peer RPC.
        self.selection_changed.send_replace(());
        self.refresh_wakeup.notify_one();
        self.install_audience(&snapshot).await?;
        Ok(tasks)
    }

    async fn audience(
        &self,
        document: &DocumentId,
        group: [u8; 32],
    ) -> Res<(HashMap<PeerKey, Access>, bool)> {
        #[cfg(test)]
        if self
            .pending_once
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Ok((HashMap::new(), true));
        }
        let authority = match self
            .repo
            .coordination_authority(document.clone(), group)
            .await
        {
            Ok(authority) => authority,
            Err(CoordinationError::Unauthorized) => return Ok((HashMap::new(), false)),
            Err(CoordinationError::Pending) => return Ok((HashMap::new(), true)),
            Err(error) => return Err(error.into()),
        };
        let authority = match self
            .repo
            .admit_coordination_access(&authority, Access::Relay)
            .await
        {
            Ok(authority) => authority,
            Err(CoordinationError::Unauthorized) => return Ok((HashMap::new(), false)),
            Err(CoordinationError::Pending) => return Ok((HashMap::new(), true)),
            Err(error) => return Err(error.into()),
        };
        Ok((
            authority
                .effective_agents()
                .iter()
                .filter(|(_, access)| **access >= Access::Relay)
                .map(|(agent, access)| (PeerKey::new(agent), *access))
                .collect(),
            false,
        ))
    }

    async fn install_audience(&self, snapshot: &PoolDescriptorSnapshot) -> Res<bool> {
        let newly_dirty = {
            let mut pools = self.pools.write().expect(ERROR_MUTEX);
            let owned = pools
                .get_mut(&snapshot.descriptor.pool_id)
                .expect(ERROR_IMPOSSIBLE);
            let newly_dirty = !owned.refresh_required;
            owned.refresh_required = true;
            newly_dirty
        };
        if newly_dirty {
            self.refresh_wakeup.notify_one();
        }
        let (audience, pending) = self
            .audience(
                &snapshot.reference.document,
                snapshot.reference.authority_group,
            )
            .await?;
        let replace_rows = {
            let mut pools = self.pools.write().expect(ERROR_MUTEX);
            let owned = pools
                .get_mut(&snapshot.descriptor.pool_id)
                .expect(ERROR_IMPOSSIBLE);
            let replace = owned.permissions_dirty || owned.audience != audience;
            // Cancellation after commit but before cache installation must force
            // another replacement, even if the next sample equals the old cache.
            owned.permissions_dirty |= replace;
            replace
        };
        if replace_rows {
            let parts = self
                .pools
                .read()
                .expect(ERROR_MUTEX)
                .get(&snapshot.descriptor.pool_id)
                .expect(ERROR_IMPOSSIBLE)
                .tasks
                .register()
                .transport_parts();
            self.parts.set_parts_members(&parts, &audience).await?;
        }
        #[cfg(test)]
        self.wait_permission_commit(&snapshot.descriptor.active_task_part)
            .await;
        let mut pools = self.pools.write().expect(ERROR_MUTEX);
        let owned = pools
            .get_mut(&snapshot.descriptor.pool_id)
            .expect(ERROR_IMPOSSIBLE);
        let changed = owned.audience != audience;
        owned.audience = audience;
        owned.refresh_required = pending;
        owned.permissions_dirty = false;
        #[cfg(test)]
        drop(self.permission_updates.send(pending));
        Ok(changed)
    }

    #[cfg(test)]
    fn force_pending_once(&self) {
        self.pending_once
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    fn pause_permission_commit(
        &self,
        part: Option<PartKey>,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (entered, observed) = tokio::sync::oneshot::channel();
        let (release, wait) = tokio::sync::oneshot::channel();
        *self.permission_pause.lock().expect(ERROR_MUTEX) = Some(PermissionPause {
            part,
            entered,
            release: wait,
        });
        (observed, release)
    }

    #[cfg(test)]
    async fn wait_permission_commit(&self, part: &PartKey) {
        let pause = {
            let mut pause = self.permission_pause.lock().expect(ERROR_MUTEX);
            if pause
                .as_ref()
                .is_some_and(|pause| pause.part.as_ref().is_none_or(|target| target == part))
            {
                pause.take()
            } else {
                None
            }
        };
        if let Some(pause) = pause {
            pause.entered.send(()).expect(ERROR_CHANNEL);
            pause.release.await.expect(ERROR_CHANNEL);
        }
    }

    pub(crate) fn refresh_pending(&self) -> bool {
        self.pools
            .read()
            .expect(ERROR_MUTEX)
            .values()
            .any(|pool| pool.refresh_required)
            || self
                .processors
                .read()
                .expect(ERROR_MUTEX)
                .values()
                .any(|owned| owned.refresh_required)
    }

    pub(crate) async fn refresh_notified(&self) {
        self.refresh_wakeup.notified().await;
    }

    /// Called from the durable native access walker, before acknowledging its
    /// delta. Named group intersections can change through nested groups, so any
    /// group delta recomputes the explicitly attached (bounded) pool set.
    pub(crate) async fn refresh_authority(&self, pending_only: bool) -> Res<bool> {
        let _admission = self.admission.lock().await;
        let snapshots = self
            .pools
            .read()
            .expect(ERROR_MUTEX)
            .values()
            .filter(|owned| !pending_only || owned.refresh_required)
            .map(|owned| owned.snapshot.clone())
            .collect::<Vec<_>>();
        let mut changed = false;
        for snapshot in snapshots {
            changed |= self.install_audience(&snapshot).await?;
        }
        let processors: Vec<_> = self
            .processors
            .read()
            .expect(ERROR_MUTEX)
            .iter()
            .filter(|(_, owned)| !pending_only || owned.refresh_required)
            .map(|(part, owned)| (part.clone(), owned.snapshot.reference.clone()))
            .collect();
        for (part, reference) in processors {
            changed |= self.install_processor_audience(&part, &reference).await?;
        }
        Ok(changed)
    }

    pub(crate) async fn attach_processor(
        &self,
        snapshot: Arc<ProcessorTransportSnapshot>,
        control: Option<&ProcessorRotationControl>,
    ) -> Res<()> {
        let _admission = self.admission.lock().await;
        self.attach_processor_locked(snapshot, control).await
    }

    // No remote await occurs while holding admission. Permission maintenance and
    // advertisements cannot observe a new certificate with an old owner.
    async fn attach_processor_locked(
        &self,
        snapshot: Arc<ProcessorTransportSnapshot>,
        control: Option<&ProcessorRotationControl>,
    ) -> Res<()> {
        if let Some(control) = control {
            let admitted = control
                .admit(
                    &self.repo,
                    &snapshot.processor_full_id,
                    snapshot.reference.authority_group,
                    None,
                )
                .await?;
            ensure!(
                admitted.reference == snapshot.reference
                    && admitted.register_binding.generation == snapshot.register_binding.generation
                    && admitted.register_binding.publication_key
                        == snapshot.register_binding.publication_key,
                "processor control attachment mismatch"
            );
        }
        let rcx = self.rcx.upgrade().ok_or(CoordinationError::Unauthorized)?;
        rcx.attach_processor_slot_store(&snapshot, control.map(serde_json::to_string).transpose()?)
            .await?;
        let (snapshot, slots) = {
            let stores = rcx.processor_slot_stores.lock().await;
            let selected = stores
                .get(&snapshot.processor_full_id)
                .expect(ERROR_IMPOSSIBLE);
            (Arc::clone(&selected.snapshot), Arc::clone(&selected.store))
        };
        #[cfg(test)]
        self.wait_permission_commit(&snapshot.register_binding.part)
            .await;
        let part = snapshot.register_binding.part.clone();
        ensure!(
            slots.register().part() == &part
                && slots.register().scope() == snapshot.register_binding.scope.as_slice(),
            "processor slot store is detached from its native binding"
        );
        let existing = {
            let processors = self.processors.read().expect(ERROR_MUTEX);
            processors
                .get(&part)
                .map(|owned| {
                    if owned.snapshot.reference == snapshot.reference
                        && Arc::ptr_eq(&owned.slots, &slots)
                    {
                        return Ok::<_, eyre::Report>(true);
                    }
                    ensure!(
                        snapshot.register_binding.scope.as_slice()
                            == owned.slots.register().scope()
                            && slots.register().recognizes_authority(
                                &owned.snapshot.reference.document,
                                &owned.snapshot.reference.authority_group,
                            ),
                        "processor successor changes retained native identity"
                    );
                    Ok(false)
                })
                .transpose()?
        };
        if existing == Some(true) {
            let reference = snapshot.reference.clone();
            let processor = snapshot.processor_full_id.clone();
            {
                let mut processors = self.processors.write().expect(ERROR_MUTEX);
                let owned = processors.get_mut(&part).expect(ERROR_IMPOSSIBLE);
                owned.snapshot = snapshot;
                // A late retained source must receive native permissions too.
                owned.permissions_dirty = true;
            }
            self.install_processor_audience(&part, &reference).await?;
            self.publish_processor_hint(&processor).await?;
            return Ok(());
        }
        let identity = serde_json::to_string(&(
            snapshot.reference.document.to_string(),
            snapshot.reference.authority_group,
        ))?;
        let mut write = self
            .parts
            .begin_obj_write(ObjKey::new(part.as_bytes()))
            .await?;
        let scope = write.scope_id();
        let collision: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM task_pool_transport_bindings WHERE scope_id = ? AND (part_id = ? OR register_scope = ?))",
        ).bind(scope).bind(part.as_bytes()).bind(&snapshot.register_binding.scope)
            .fetch_one(&mut **write.context_mut()).await?;
        ensure!(
            !collision,
            "processor slot part is already owned by a task pool"
        );
        let previous: Option<String> = sqlx::query_scalar(
            "SELECT identity_json FROM processor_slot_transport_bindings WHERE scope_id = ? AND processor_id = ?",
        ).bind(scope).bind(&snapshot.processor_full_id).fetch_optional(&mut **write.context_mut()).await?;
        if let Some(previous) = previous {
            if previous != identity {
                let (document, group): (String, [u8; 32]) = serde_json::from_str(&previous)?;
                ensure!(
                    slots
                        .register()
                        .recognizes_authority(&document.parse()?, &group),
                    "processor successor omits historical authority"
                );
                let selected: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM encrypted_register_rotation
                       WHERE register_scope = ? AND part_id = ? AND authority_document = ?)",
                )
                .bind(&snapshot.register_binding.scope)
                .bind(part.as_bytes())
                .bind(snapshot.reference.document.to_string())
                .fetch_one(&mut **write.context_mut())
                .await?;
                ensure!(
                    selected,
                    "processor transport is not the selected durable binding"
                );
                sqlx::query(
                    "UPDATE processor_slot_transport_bindings
                        SET part_id = ?
                          , identity_json = ?
                      WHERE scope_id = ? AND processor_id = ?",
                )
                .bind(part.as_bytes())
                .bind(&identity)
                .bind(scope)
                .bind(&snapshot.processor_full_id)
                .execute(&mut **write.context_mut())
                .await?;
            }
        } else {
            sqlx::query(
                "INSERT INTO processor_slot_transport_bindings (
                    scope_id
                  , processor_id
                  , register_scope
                  , part_id
                  , identity_json
                ) VALUES (?, ?, ?, ?, ?)",
            )
            .bind(scope)
            .bind(&snapshot.processor_full_id)
            .bind(&snapshot.register_binding.scope)
            .bind(part.as_bytes())
            .bind(identity)
            .execute(&mut **write.context_mut())
            .await?;
        }
        write.commit().await?;
        let reference = snapshot.reference.clone();
        let scope = &snapshot.register_binding.scope;
        self.processors
            .write()
            .expect(ERROR_MUTEX)
            .retain(|_, owned| owned.slots.register().scope() != scope.as_slice());
        self.processors.write().expect(ERROR_MUTEX).insert(
            part.clone(),
            OwnedProcessor {
                snapshot,
                slots,
                audience: HashMap::new(),
                refresh_required: true,
                permissions_dirty: true,
                hint_pending: true,
            },
        );
        self.refresh_wakeup.notify_one();
        self.install_processor_audience(&part, &reference).await?;
        let processor = {
            let processors = self.processors.read().expect(ERROR_MUTEX);
            processors
                .get(&part)
                .expect(ERROR_IMPOSSIBLE)
                .snapshot
                .processor_full_id
                .clone()
        };
        self.publish_processor_hint(&processor).await?;
        Ok(())
    }

    async fn install_processor_audience(
        &self,
        part: &PartKey,
        reference: &crate::rt::triage::domain::ProcessorDomainReference,
    ) -> Res<bool> {
        let newly_dirty = {
            let mut processors = self.processors.write().expect(ERROR_MUTEX);
            let owned = processors.get_mut(part).expect(ERROR_IMPOSSIBLE);
            let newly_dirty = !owned.refresh_required;
            owned.refresh_required = true;
            newly_dirty
        };
        if newly_dirty {
            self.refresh_wakeup.notify_one();
        }
        let (audience, pending) = self
            .audience(&reference.document, reference.authority_group)
            .await?;
        let replace = {
            let mut processors = self.processors.write().expect(ERROR_MUTEX);
            let owned = processors.get_mut(part).expect(ERROR_IMPOSSIBLE);
            let replace = owned.permissions_dirty || owned.audience != audience;
            owned.permissions_dirty |= replace;
            replace
        };
        if replace {
            let parts = self
                .processors
                .read()
                .expect(ERROR_MUTEX)
                .get(part)
                .expect(ERROR_IMPOSSIBLE)
                .slots
                .register()
                .transport_parts();
            self.parts.set_parts_members(&parts, &audience).await?;
        }
        let mut processors = self.processors.write().expect(ERROR_MUTEX);
        let owned = processors.get_mut(part).expect(ERROR_IMPOSSIBLE);
        let changed = owned.audience != audience;
        owned.audience = audience;
        owned.refresh_required = pending;
        owned.permissions_dirty = false;
        Ok(changed)
    }

    fn bound_store(&self, parts: &[PartKey]) -> Res<BoundStore> {
        ensure!(
            !parts.is_empty(),
            "task object has no configured part binding"
        );
        let mut selected = None;
        for owned in self.processors.read().expect(ERROR_MUTEX).values() {
            if parts
                .iter()
                .all(|part| owned.slots.register().accepts_part(part))
            {
                ensure!(
                    selected.is_none(),
                    "ambiguous coordination register binding"
                );
                selected = Some(BoundStore::Processor(Arc::clone(&owned.slots)));
            }
        }
        for owned in self.pools.read().expect(ERROR_MUTEX).values() {
            if parts
                .iter()
                .all(|part| owned.tasks.register().accepts_part(part))
            {
                ensure!(
                    selected.is_none(),
                    "ambiguous coordination register binding"
                );
                selected = Some(BoundStore::Tasks(Arc::clone(&owned.tasks)));
            }
        }
        // A carried object can name both current and predecessor parts. The
        // binding must be one logical register, not one identical physical hint.
        selected.ok_or_else(|| ferr!("unknown coordination part binding"))
    }
}

#[async_trait]
impl SyncBackend for TaskSyncBackend {
    async fn sync_obj(
        &self,
        peer_id: PeerKey,
        obj_id: ObjKey,
        parts: Vec<PartKey>,
        remote_payload: Option<ObjPayload>,
    ) -> Res<SyncTaskRunOutcome> {
        let tasks = self.bound_store(&parts)?;
        let Some(payload) = remote_payload else {
            return Ok(SyncTaskRunOutcome::Stale);
        };
        let was_member = self
            .parts
            .obj_parts(obj_id.clone())
            .await?
            .contains(tasks.register().part());
        let admission = async {
            let register = tasks.register();
            let incoming = register.prepare_receive(&obj_id, &parts, payload)?;
            for key_ref in register.missing_sources(&incoming)? {
                let native = self
                    .native_owner
                    .get()
                    .and_then(std::sync::Weak::upgrade)
                    .ok_or_eyre("native source control owner stopped")?;
                let control = native
                    .fetch_register_source_control(
                        &peer_id,
                        register.scope(),
                        register.authority_document(),
                        &key_ref,
                    )
                    .await?;
                register
                    .import_source_control(&control, &key_ref, &peer_id)
                    .await?;
            }
            register.receive_prepared(incoming).await
        }
        .await;
        let outcome = match admission {
            Ok(outcome) => outcome,
            Err(error) => {
                #[cfg(test)]
                // Broadcast permits zero listeners. This test observation is
                // never a domain wakeup or a substitute for durable revisions.
                drop(self.admission_errors.send(obj_id.clone()));
                return Err(error);
            }
        };
        tracing::debug!(node = %self.repo.local_peer_id(), object = %obj_id, ?outcome, was_member, "native register transport admission committed");
        // receive returns only after its authenticated current-state + publication
        // transaction commits; readers wake through the shared durable frontier.
        let deets = if !was_member {
            SyncCompletionDeets::AddedMember
        } else if outcome == MergeOutcome::Unchanged {
            SyncCompletionDeets::Noop
        } else {
            SyncCompletionDeets::ChangedObject
        };
        Ok(SyncTaskRunOutcome::Completion(SyncTaskCompletion {
            obj_id,
            deets,
        }))
    }

    async fn remove_obj_from_parts(&self, _obj_id: ObjKey, parts: Vec<PartKey>) -> Res<()> {
        self.bound_store(&parts)?;
        eyre::bail!("remote task membership removal is not authenticated retirement; unsupported")
    }
}

#[cfg(test)]
mod tests;
