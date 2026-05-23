mod interlude {
    pub use utils_rs::prelude::*;

    pub use tokio_util::sync::CancellationToken;
}

use crate::interlude::*;

mod part_store;
mod rpc;
#[cfg(any(test, feature = "test-support"))]
pub mod stress_support;
#[cfg(test)]
mod test;
#[cfg(test)]
mod test_support;
mod trap;
mod worker;

pub use big_sync_core::part_store::ObjPayload;
pub use part_store::sqlite::SqlitePartStore;
pub use part_store::{HostPartitionStore, ObjStoreLease};
pub use rpc::HostBigRpcClient;
pub use worker::{
    spawn_big_sync_worker, BackendId, BigSyncWorkerError, BigSyncWorkerHandle, StopToken,
    SyncBackend, SyncTaskRunOutcome,
};

#[derive(Clone)]
pub struct Ctx {
    pub store: Arc<dyn HostPartitionStore>,
    pub worker: BigSyncWorkerHandle,
}
