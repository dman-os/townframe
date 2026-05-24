mod interlude {
    pub use utils_rs::prelude::*;

    pub use tokio_util::sync::CancellationToken;
}

use crate::interlude::*;

pub mod backend;
mod part_store;
pub mod rpc;
#[cfg(any(test, feature = "test-support"))]
pub mod stress_support;
#[cfg(test)]
mod test;
#[cfg(test)]
mod test_support;
mod trap;
mod worker;

pub use backend::SyncBackend;
pub use big_sync_core::part_store::ObjPayload;
pub use part_store::memory::MemoryPartStore;
pub use part_store::sqlite::SqlitePartStore;
pub use part_store::HostPartStore;
#[cfg(any(test, feature = "test-support"))]
pub use worker::WorkerSnapshot;
pub use worker::{
    spawn_big_sync_worker, BackendId, BigSyncWorkerError, BigSyncWorkerHandle, StopToken,
    SyncTaskRunOutcome,
};

#[derive(Clone)]
pub struct Ctx {
    pub store: Arc<dyn HostPartStore>,
    pub worker: BigSyncWorkerHandle,
}
