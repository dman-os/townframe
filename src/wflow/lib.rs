mod interlude {
    pub use utils_rs::prelude::*;
}

#[cfg(test)]
mod test;

pub mod ingress;
pub mod kvstore;
pub mod log;
pub mod metastore;
pub mod snapstore;

pub use ingress::{PartitionLogIngress, WflowIngress};
pub use kvstore::{CasError, CasGuard, KvStore};
pub use log::KvStoreLog;
pub use metastore::KvStoreMetadtaStore;
pub use snapstore::AtomicKvSnapStore;
pub use wflow_core::snapstore::PartitionSnapshot;

use crate::interlude::*;

use utils_rs::am::AmCtx;
use wash_runtime::*;
use wflow_core::gen::types::PartitionId;

// pub struct Config {}

#[derive(Clone)]
pub struct Ctx {
    pub acx: Arc<AmCtx>,
    pub metastore: Arc<dyn wflow_core::metastore::MetdataStore>,
    pub log_store: Arc<dyn wflow_core::log::LogStore>,
    pub partition_id: PartitionId,
    pub snap_store: Option<Arc<dyn wflow_core::snapstore::SnapStore>>,
}

/// Build and start a wash runtime host with wflow and am-repo plugins
pub async fn build_wash_host(
    plugins: Vec<Arc<dyn plugin::HostPlugin>>,
) -> Res<Arc<wash_runtime::host::Host>> {
    let engine = engine::Engine::builder().build().to_eyre()?;

    let mut host = host::HostBuilder::new().with_engine(engine);
    for plugin in plugins {
        host = host.with_plugin(plugin).to_eyre()?
    }
    let host = host.build().to_eyre()?;

    let host = host.start().await.to_eyre()?;

    Ok(host)
}

/// Start the partition worker for processing workflow jobs
/// Returns both the worker handle and the working state for observation
pub async fn start_partition_worker(
    wcx: &Ctx,
    wflow_plugin: Arc<wash_plugin_wflow::TownframewflowPlugin>,
) -> Res<(
    wflow_tokio::partition::TokioPartitionWorkerHandle,
    Arc<wflow_tokio::partition::state::PartitionWorkingState>,
)> {
    // Load state from snapshot if available
    let (initial_entry_id, initial_jobs_state, initial_effects) =
        if let Some(ref snap_store) = wcx.snap_store {
            match snap_store.load_latest_snapshot(wcx.partition_id).await? {
                Some((entry_id, snapshot)) => {
                    tracing::info!(
                        partition_id = wcx.partition_id,
                        entry_id,
                        "loaded state from snapshot"
                    );
                    (entry_id + 1, snapshot.jobs, snapshot.effects) // Resume from next entry after snapshot
                }
                None => {
                    tracing::info!(
                        partition_id = wcx.partition_id,
                        "no snapshot found, starting from beginning"
                    );
                    (
                        0,
                        wflow_core::partition::state::PartitionJobsState::default(),
                        default(),
                    )
                }
            }
        } else {
            (
                0,
                wflow_core::partition::state::PartitionJobsState::default(),
                default(),
            )
        };

    let pcx = wflow_tokio::partition::PartitionCtx::new(
        wcx.partition_id,
        wcx.metastore.clone(),
        wcx.log_store.clone(),
        initial_entry_id,
        wflow_plugin,
    );

    let active_state = wflow_tokio::partition::state::PartitionWorkingState::new(
        initial_entry_id,
        initial_jobs_state,
        initial_effects,
    );
    let active_state = Arc::new(active_state);

    let worker = wflow_tokio::partition::start_tokio_worker(
        pcx,
        active_state.clone(),
        wcx.snap_store.clone(),
    )
    .await;

    Ok((worker, active_state))
}
