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
pub use kvstore::{SqliteKvFactory, SqliteKvStore};
pub use wash_plugin_wflow;
pub use wflow_core;
pub use wflow_tokio;

use crate::interlude::*;

use wash_runtime::*;
use wflow_core::{gen::types::PartitionId, kvstore::KvStore};

// pub struct Config {}

#[derive(Clone)]
pub struct Ctx {
    pub metastore: Arc<dyn wflow_core::metastore::MetdataStore>,
    pub logstore: Arc<dyn wflow_core::log::LogStore>,
    pub snapstore: Arc<dyn wflow_core::snapstore::SnapStore<Snapshot = Arc<[u8]>>>,
    pub factory: Option<SqliteKvFactory>,
}

impl Ctx {
    pub async fn init(db_url: &str) -> Res<Self> {
        let factory = SqliteKvFactory::boot(db_url).await?;
        let metastore_kv = Arc::new(factory.open_store("wflow_metastore").await?);
        let logstore_kv = Arc::new(factory.open_store("wflow_logstore").await?);
        let snapstore_kv = Arc::new(factory.open_store("wflow_snapstore").await?);

        // Create the stores
        let metastore = Arc::new(
            wflow_core::kvstore::metastore::KvStoreMetadtaStore::new(
                metastore_kv as Arc<dyn KvStore + Send + Sync>,
                wflow_core::gen::metastore::PartitionsMeta {
                    version: "0".into(),
                    partition_count: 1,
                },
            )
            .await?,
        );

        let logstore = wflow_core::kvstore::log::KvStoreLog::new(
            logstore_kv.clone() as Arc<dyn KvStore + Send + Sync>
        )
        .await?;
        let logstore = Arc::new(logstore);
        let snapstore = Arc::new(wflow_core::kvstore::snapstore::KvSnapStore::new(
            snapstore_kv,
        ));
        Ok(Self {
            metastore,
            logstore,
            snapstore,
            factory: Some(factory),
        })
    }
}

pub async fn build_wash_host(
    plugins: Vec<Arc<dyn plugin::HostPlugin>>,
) -> Res<wash_runtime::host::Host> {
    // Create a unique engine instance for each wash host to ensure complete isolation
    // This prevents conflicts when tests run in parallel
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
        wcx.logstore.clone(),
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
