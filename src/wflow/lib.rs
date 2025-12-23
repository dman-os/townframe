mod interlude {
    pub use utils_rs::prelude::*;
}

#[cfg(any(test, feature = "test-harness"))]
pub mod test;

#[cfg(any(test, feature = "test-harness"))]
pub use test::{InitialWorkload, WflowTestContext, WflowTestContextBuilder};

pub mod ingress;
pub mod kvstore;

pub use ingress::{PartitionLogIngress, WflowIngress};
pub use kvstore::SqliteKvStore;
pub use wash_plugin_wflow::WflowPlugin;
pub use wflow_core::kvstore::log::KvStoreLog;
pub use wflow_core::kvstore::metastore::KvStoreMetadtaStore;
pub use wflow_core::kvstore::snapstore::AtomicKvSnapStore;
pub use wflow_core::kvstore::{CasError, CasGuard, KvStore};
pub use wflow_core::snapstore::PartitionSnapshot;

use crate::interlude::*;

use wash_runtime::*;
use wflow_core::gen::types::PartitionId;

// pub struct Config {}

#[derive(Clone)]
pub struct Ctx {
    pub metastore: Arc<dyn wflow_core::metastore::MetdataStore>,
    pub log_store: Arc<dyn wflow_core::log::LogStore>,
    pub snapstore: Arc<dyn wflow_core::snapstore::SnapStore>,
}

pub async fn build_wash_host(
    plugins: Vec<Arc<dyn plugin::HostPlugin>>,
) -> Res<wash_runtime::host::Host> {
    let engine = engine::Engine::builder().build().to_eyre()?;

    let mut host = host::HostBuilder::new().with_engine(engine);
    for plugin in plugins {
        host = host.with_plugin(plugin).to_eyre()?
    }
    let host = host.build().to_eyre()?;

    Ok(host)
}

/// Start the partition worker for processing workflow jobs
/// Returns both the worker handle and the working state for observation
pub async fn start_partition_worker(
    wcx: &Ctx,
    wflow_plugin: Arc<wash_plugin_wflow::WflowPlugin>,
    partition_id: PartitionId,
) -> Res<(
    wflow_tokio::partition::TokioPartitionWorkerHandle,
    Arc<wflow_tokio::partition::state::PartitionWorkingState>,
)> {
    // Load state from snapshot if available
    let (next_entry_id, initial_jobs_state, initial_effects) =
        match wcx.snapstore.load_latest_snapshot(partition_id).await? {
            Some((entry_id, snapshot)) => {
                tracing::info!(partition_id, entry_id, "loaded state from snapshot");
                (entry_id + 1, snapshot.jobs, snapshot.effects) // Resume from next entry after snapshot
            }
            None => {
                tracing::info!(partition_id, "no snapshot found, starting from beginning");
                (
                    0,
                    wflow_core::partition::state::PartitionJobsState::default(),
                    default(),
                )
            }
        };

    let pcx = wflow_tokio::partition::PartitionCtx::new(
        partition_id,
        wcx.metastore.clone(),
        wcx.log_store.clone(),
        next_entry_id,
        wflow_plugin,
        Arc::new(wflow_tokio::local_native_host::LocalNativeHost {}),
    );

    let last_applied_entry_id = next_entry_id.saturating_sub(1);
    let active_state = wflow_tokio::partition::state::PartitionWorkingState::new(
        last_applied_entry_id,
        initial_jobs_state,
        initial_effects,
    );
    let active_state = Arc::new(active_state);

    let worker = wflow_tokio::partition::start_tokio_worker(
        pcx,
        active_state.clone(),
        wcx.snapstore.clone(),
    )
    .await;

    Ok((worker, active_state))
}
