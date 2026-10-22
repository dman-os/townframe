mod interlude {
    pub use utils_rs::prelude::*;

    pub use tokio_util::sync::CancellationToken;
}

use crate::interlude::*;
use sqlite_rs as sqlite_rs_alias;

mod sqlite {
    #[cfg_attr(not(test), expect(dead_code))]
    pub mod queries {
        include!("sqlite/gen.rs");
    }
}

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
pub use big_sync_core::SyncMode;
pub use big_sync_core::delta_walker_sparse_state::DeltaWalkerSparseStateRepo;
pub use big_sync_core::delta_walker_sparse_state::DeltaWalkerSparseStateTransaction;
pub use big_sync_core::delta_walker_state::DeltaWalkerProgress;
pub use big_sync_core::delta_walker_state::DeltaWalkerStateError;
pub use big_sync_core::delta_walker_state::DeltaWalkerStateRepo;
pub use big_sync_core::delta_walker_state::DeltaWalkerStateResult;
pub use big_sync_core::delta_walker_state::DeltaWalkerStateTransaction;
pub use big_sync_core::part_store::ObjPayload;
pub use delta_walker_state::SqliteDeltaWalkerStateRepo;
pub use delta_walker_state::SqliteDeltaWalkerStateTransaction;
pub use part_store::HostPartStore;
pub use part_store::HostPartStoreConfig;
pub use part_store::LocalPartRevisionReader;
pub use part_store::PartScope;
pub use part_store::PartStoreStats;
pub use part_store::ReadTarget;
pub use part_store::bucket_index_bounds;
#[cfg(feature = "test-support")]
pub use part_store::host_contract as host_part_store_contract;
#[cfg(feature = "test-support")]
pub use part_store::host_contract::HostPartStoreContractHarness;
#[cfg(feature = "test-support")]
pub use part_store::host_contract::SingleTargetPageStore;
pub use part_store::memory::MemoryPartStore;
pub use part_store::sqlite::SqlitePartStore;
pub use part_store::sqlite::open_sqlite_page_reader;
pub use part_store::sqlite::open_sqlite_revision_reader;
pub use part_store::sqlite::open_sqlite_revision_reader_all;
pub use part_store::sqlite_core;
pub use worker::BackendId;
pub use worker::BigSyncWorkerError;
pub use worker::BigSyncWorkerHandle;
pub use worker::StopToken;
pub use worker::SyncTaskRunOutcome;
#[cfg(any(test, feature = "test-support"))]
pub use worker::WorkerSnapshot;
pub use worker::spawn_big_sync_worker;
pub use worker::spawn_big_sync_worker_with_options;

#[derive(Clone)]
pub struct Ctx {
    pub store: Arc<dyn HostPartStore>,
    pub worker: BigSyncWorkerHandle,
}
