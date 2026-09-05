mod interlude {
    pub use utils_rs::prelude::*;

    pub use tokio_util::sync::CancellationToken;
}

use crate::interlude::*;

pub mod backend;
pub mod delta_walker_state;
pub mod keyed_frontier;
mod part_store;
pub mod rpc;
#[cfg(any(test, feature = "test-support"))]
pub mod stress_support;
#[cfg(test)]
mod test;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
mod trap;
mod worker;

pub use backend::SyncBackend;
pub use big_sync_core::delta_walker_sparse_state::{
    DeltaWalkerSparseStateRepo, DeltaWalkerSparseStateTransaction,
};
pub use big_sync_core::delta_walker_state::{
    DeltaWalkerProgress, DeltaWalkerStateError, DeltaWalkerStateRepo, DeltaWalkerStateResult,
    DeltaWalkerStateTransaction,
};
pub use big_sync_core::part_store::ObjPayload;
pub use delta_walker_state::{SqliteDeltaWalkerStateRepo, SqliteDeltaWalkerStateTransaction};
#[cfg(feature = "test-support")]
pub use part_store::host_contract as host_part_store_contract;
#[cfg(feature = "test-support")]
pub use part_store::host_contract::HostPartStoreContractHarness;
pub use part_store::memory::MemoryPartStore;
pub use part_store::sqlite::{
    SqlitePartStore, open_sqlite_local_revision_reader, open_sqlite_local_revision_reader_all,
};
pub use part_store::sqlite_core;
pub use part_store::{HostPartStore, HostPartStoreConfig, LocalPartRevisionReader};
#[cfg(any(test, feature = "test-support"))]
pub use worker::WorkerSnapshot;
pub use worker::{
    BackendId, BigSyncWorkerError, BigSyncWorkerHandle, StopToken, SyncTaskRunOutcome,
    spawn_big_sync_worker, spawn_big_sync_worker_with_options,
};

#[derive(Clone)]
pub struct Ctx {
    pub store: Arc<dyn HostPartStore>,
    pub worker: BigSyncWorkerHandle,
}
