//! Retained encrypted pool records over the dedicated native task scope.
//!
//! The owner, not a directory hint or delivering peer, selects a pool binding.
//! BigSync completion follows checked durable admission. Remote membership loss
//! is not authenticated retirement and is rejected rather than deleting evidence.

use super::{TaskPoolId, pool::PoolDescriptorSnapshot, storage::RegisterStore, store::TaskStore};
use crate::interlude::*;
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
    reference: crate::rt::triage::domain::ProcessorDomainReference,
    slots: Arc<crate::rt::triage::slots::ProcessorSlotStore>,
    audience: HashMap<PeerKey, Access>,
    refresh_required: bool,
    permissions_dirty: bool,
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
    entered: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
}

/// Exactly one strongly retained task-store owner per explicitly attached pool.
/// Its one task-scope store and Notify are shared by registers, worker and RPC.
/// There is no scheduling consumer or scheduling ALPN in this transport owner.
pub struct TaskSyncBackend {
    repo: Arc<BigRepo>,
    parts: Arc<SqlitePartStore>,
    pools: std::sync::RwLock<HashMap<TaskPoolId, OwnedPool>>,
    processors: std::sync::RwLock<HashMap<PartKey, OwnedProcessor>>,
    admission: tokio::sync::Mutex<()>,
    refresh_wakeup: tokio::sync::Notify,
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
            parts: rcx.coordination_part_store().await?,
            repo: Arc::clone(&rcx.big_repo),
            pools: Default::default(),
            processors: Default::default(),
            admission: Default::default(),
            refresh_wakeup: Default::default(),
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
        let old_parts: Vec<Vec<u8>> = sqlx::query_scalar(
            "SELECT part_id FROM task_pool_transport_bindings WHERE scope_id = ?",
        )
        .bind(write.scope_id())
        .fetch_all(&mut **write.context_mut())
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
        let processor_parts: Vec<Vec<u8>> = sqlx::query_scalar(
            "SELECT part_id FROM processor_slot_transport_bindings WHERE scope_id = ?",
        )
        .bind(write.scope_id())
        .fetch_all(&mut **write.context_mut())
        .await?;
        write.commit().await?;
        // Persisted permission rows are not authority at restart. Clear only
        // previously bound task parts before RPC starts serving; explicit pool
        // attachment seeds fresh native permissions. No task-record scan occurs.
        for part in old_parts.into_iter().chain(processor_parts) {
            owner
                .parts
                .set_part_members(PartKey::new(part), HashMap::new())
                .await?;
        }
        Ok(owner)
    }

    pub(crate) fn shared_store(&self) -> Arc<SqlitePartStore> {
        Arc::clone(&self.parts)
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

    pub(crate) fn routes(&self, peer: &PeerKey) -> HashMap<PartKey, BackendId> {
        let mut routes: HashMap<PartKey, BackendId> = self
            .pools
            .read()
            .expect(ERROR_MUTEX)
            .values()
            .filter(|owned| {
                owned
                    .audience
                    .get(peer)
                    .is_some_and(|access| *access >= Access::Relay)
            })
            .map(|owned| {
                (
                    owned.snapshot.descriptor.active_task_part.clone(),
                    Arc::from(TASK_BACKEND_ID),
                )
            })
            .collect();
        for (part, owned) in self.processors.read().expect(ERROR_MUTEX).iter() {
            if owned
                .audience
                .get(peer)
                .is_some_and(|access| *access >= Access::Relay)
            {
                routes.insert(part.clone(), Arc::from(TASK_BACKEND_ID));
            }
        }
        routes
    }

    pub(crate) async fn attach(&self, snapshot: PoolDescriptorSnapshot) -> Res<Arc<TaskStore>> {
        let _admission = self.admission.lock().await;
        let binding = snapshot
            .register_binding_for_access(&self.repo, Access::Relay)
            .await?;
        let existing = {
            let pools = self.pools.read().expect(ERROR_MUTEX);
            if let Some(existing) = pools.get(&snapshot.descriptor.pool_id) {
                ensure!(
                    existing.snapshot.reference == snapshot.reference
                        && existing.snapshot.descriptor == snapshot.descriptor,
                    "attached pool binding changed; explicit successor binding is required"
                );
                Some(Arc::clone(&existing.tasks))
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
            ensure!(
                existing == identity,
                "persisted pool binding changed; explicit successor binding is required"
            );
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
        write.commit().await?;
        let register =
            RegisterStore::open(Arc::clone(&self.parts), Arc::clone(&self.repo), binding).await?;
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
            self.parts
                .set_part_members(
                    snapshot.descriptor.active_task_part.clone(),
                    audience.clone(),
                )
                .await?;
        }
        #[cfg(test)]
        {
            let pause = self.permission_pause.lock().expect(ERROR_MUTEX).take();
            if let Some(pause) = pause {
                pause.entered.send(()).expect(ERROR_CHANNEL);
                pause.release.await.expect(ERROR_CHANNEL);
            }
        }
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
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (entered, observed) = tokio::sync::oneshot::channel();
        let (release, wait) = tokio::sync::oneshot::channel();
        *self.permission_pause.lock().expect(ERROR_MUTEX) = Some(PermissionPause {
            entered,
            release: wait,
        });
        (observed, release)
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
            .map(|(part, owned)| (part.clone(), owned.reference.clone()))
            .collect();
        for (part, reference) in processors {
            changed |= self.install_processor_audience(&part, &reference).await?;
        }
        Ok(changed)
    }

    pub(crate) async fn attach_processor(
        &self,
        snapshot: &crate::rt::triage::domain::ProcessorDomainSnapshot,
        slots: Arc<crate::rt::triage::slots::ProcessorSlotStore>,
    ) -> Res<()> {
        let _admission = self.admission.lock().await;
        let part = &snapshot.register_binding.part;
        ensure!(
            slots.register().part() == part
                && slots.register().key(b"")?.scope == snapshot.register_binding.scope,
            "processor slot store is detached from its native binding"
        );
        let existing = {
            let processors = self.processors.read().expect(ERROR_MUTEX);
            processors
                .get(part)
                .map(|owned| {
                    ensure!(
                        owned.reference == snapshot.reference && Arc::ptr_eq(&owned.slots, &slots),
                        "processor part has another publication owner or authority binding"
                    );
                    Ok::<_, eyre::Report>(())
                })
                .transpose()?
        };
        if existing.is_some() {
            self.install_processor_audience(part, &snapshot.reference)
                .await?;
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
            ensure!(
                previous == identity,
                "processor authority binding changed; explicit migration is required"
            );
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
        self.processors.write().expect(ERROR_MUTEX).insert(
            part.clone(),
            OwnedProcessor {
                reference: snapshot.reference.clone(),
                slots,
                audience: HashMap::new(),
                refresh_required: true,
                permissions_dirty: true,
            },
        );
        self.refresh_wakeup.notify_one();
        self.install_processor_audience(part, &snapshot.reference)
            .await?;
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
            self.parts
                .set_part_members(part.clone(), audience.clone())
                .await?;
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
        let part = parts
            .first()
            .ok_or_else(|| ferr!("task object has no configured part binding"))?;
        ensure!(
            parts.iter().all(|hint| hint == part),
            "ambiguous task part binding"
        );
        if let Some(store) = self.processors.read().expect(ERROR_MUTEX).get(part) {
            return Ok(BoundStore::Processor(Arc::clone(&store.slots)));
        }
        self.pools
            .read()
            .expect(ERROR_MUTEX)
            .values()
            .find(|owned| owned.snapshot.descriptor.active_task_part == *part)
            .map(|owned| BoundStore::Tasks(Arc::clone(&owned.tasks)))
            .ok_or_else(|| ferr!("unknown coordination part binding"))
    }
}

#[async_trait]
impl SyncBackend for TaskSyncBackend {
    async fn sync_obj(
        &self,
        _peer_id: PeerKey,
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
        let outcome = match tasks.register().receive(&obj_id, &parts, payload).await {
            Ok(outcome) => outcome,
            Err(error) => {
                #[cfg(test)]
                // Broadcast permits zero listeners. This test observation is
                // never a domain wakeup or a substitute for durable revisions.
                drop(self.admission_errors.send(obj_id.clone()));
                return Err(error);
            }
        };
        // receive returns only after its authenticated current-state + publication
        // transaction commits; readers wake through the shared durable frontier.
        let deets = if outcome == MergeOutcome::Unchanged {
            SyncCompletionDeets::Noop
        } else if was_member {
            SyncCompletionDeets::ChangedObject
        } else {
            SyncCompletionDeets::AddedMember
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
