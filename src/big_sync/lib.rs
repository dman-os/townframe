mod interlude {
    pub use utils_rs::prelude::*;

    pub use tokio_util::sync::CancellationToken;
}

use big_sync_core::{ObjId, PartId, PeerId};

use big_sync_core::part_store::ObjPayload;

use crate::interlude::*;

mod part_store;
mod rpc;
#[cfg(test)]
mod test;
mod trap;
mod worker;

// pub use part_store::sqlite::SqlitePartStore;
pub use part_store::{HostPartitionStore, MemoryPartStore};
pub use rpc::HostBigRpcClient;
pub use worker::{
    spawn_big_sync_worker, BackendId, BigSyncWorkerError, BigSyncWorkerHandle, StopToken,
    SyncBackend,
};

pub type SharedPartitionStore = Arc<dyn HostPartitionStore>;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ScopeRef(pub Url);

impl ScopeRef {
    pub fn new(value: Url) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ScopedPartRef {
    pub scope: ScopeRef,
    pub part: Arc<str>,
}

impl ScopedPartRef {
    pub fn new(scope: ScopeRef, part: impl Into<Arc<str>>) -> Self {
        Self {
            scope,
            part: part.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ScopedObjRef {
    pub scope: ScopeRef,
    pub obj: Arc<str>,
}

impl ScopedObjRef {
    pub fn new(scope: ScopeRef, obj: impl Into<Arc<str>>) -> Self {
        Self {
            scope,
            obj: obj.into(),
        }
    }
}

#[async_trait]
pub trait ScopedIdResolver: Send + Sync {
    async fn resolve_part(&self, part: &ScopedPartRef) -> Res<PartId>;
    async fn resolve_obj(&self, obj: &ScopedObjRef) -> Res<ObjId>;
    async fn scoped_part(&self, part_id: PartId) -> Res<ScopedPartRef>;
    async fn scoped_obj(&self, obj_id: ObjId) -> Res<ScopedObjRef>;
}

#[derive(Clone)]
pub struct BigSyncHost<R> {
    store: Arc<dyn HostPartitionStore>,
    resolver: Arc<R>,
    worker: BigSyncWorkerHandle,
    default_backend_id: BackendId,
}

impl<R> BigSyncHost<R>
where
    R: ScopedIdResolver,
{
    pub fn new(
        store: Arc<dyn HostPartitionStore>,
        resolver: Arc<R>,
        worker: BigSyncWorkerHandle,
        default_backend_id: BackendId,
    ) -> Self {
        Self {
            store,
            resolver,
            worker,
            default_backend_id,
        }
    }

    pub fn resolver(&self) -> &Arc<R> {
        &self.resolver
    }

    pub async fn resolve_part(&self, part: &ScopedPartRef) -> Res<PartId> {
        self.resolver.resolve_part(part).await
    }

    pub async fn resolve_obj(&self, obj: &ScopedObjRef) -> Res<ObjId> {
        self.resolver.resolve_obj(obj).await
    }

    pub async fn upsert_obj(
        &self,
        obj: &ScopedObjRef,
        payload: ObjPayload,
        parts: impl IntoIterator<Item = ScopedPartRef>,
    ) -> Res<()> {
        let obj_id = self.resolver.resolve_obj(obj).await?;
        let mut part_ids = Vec::new();
        for part in parts {
            part_ids.push(self.resolver.resolve_part(&part).await?);
        }
        let changed_parts = part_ids.iter().copied().collect();
        self.store.upsert_obj(obj_id, payload, part_ids).await?;
        self.worker.notify_local_parts_changed(changed_parts).await
    }

    pub async fn remove_obj_from_part(&self, obj: &ScopedObjRef, part: &ScopedPartRef) -> Res<()> {
        let obj_id = self.resolver.resolve_obj(obj).await?;
        let part_id = self.resolver.resolve_part(part).await?;
        self.store.remove_obj_from_part(obj_id, part_id).await?;
        self.worker
            .notify_local_parts_changed([part_id].into_iter().collect())
            .await
    }

    pub async fn set_peer(
        &self,
        peer_id: PeerId,
        client: Arc<dyn HostBigRpcClient>,
        parts: impl IntoIterator<Item = ScopedPartRef>,
    ) -> Res<()> {
        let mut resolved_parts = HashMap::new();
        for part in parts {
            let old = resolved_parts.insert(
                self.resolver.resolve_part(&part).await?,
                self.default_backend_id,
            );
            assert!(old.is_none(), "fishy");
        }
        self.worker.set_peer(peer_id, client, resolved_parts).await
    }

    pub async fn remove_peer(&self, peer_id: PeerId) -> Res<()> {
        self.worker.remove_peer(peer_id).await
    }
}
