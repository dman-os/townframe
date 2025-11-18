use crate::interlude::*;

use utils_rs::am::AmCtx;
use wash_runtime::*;
use wflow::log;
use wflow::metastore;
use wflow::partition;
use wflow::plugin::binds_partition_host::townframe::wflow::partition_host;

/// Configuration for building the wflow runtime
#[derive(Clone)]
pub struct RuntimeConfig {
    pub am_ctx: Arc<AmCtx>,
    pub metastore: Arc<dyn metastore::MetdataStore>,
    pub log_store: Arc<dyn log::LogStore>,
    pub partition_id: partition_host::PartitionId,
}

/// Build and start a wash runtime host with wflow and am-repo plugins
pub async fn build_runtime_host(
    config: RuntimeConfig,
) -> Res<(
    Arc<wash_runtime::host::Host>,
    Arc<wflow::plugin::TownframewflowPlugin>,
)> {
    let wflow_plugin = wflow::plugin::TownframewflowPlugin::new(config.metastore.clone());
    let wflow_plugin = Arc::new(wflow_plugin);

    let am_repo_plugin = wash_plugin_am_repo::AmRepoPlugin::new(config.am_ctx.clone());
    let am_repo_plugin = Arc::new(am_repo_plugin);

    let engine = engine::Engine::builder().build().to_eyre()?;

    let runtime_config_plugin = plugin::wasi_config::RuntimeConfig::default();

    let host = host::HostBuilder::new()
        .with_engine(engine)
        .with_plugin(Arc::new(runtime_config_plugin))
        .to_eyre()?
        .with_plugin(wflow_plugin.clone())
        .to_eyre()?
        .with_plugin(am_repo_plugin)
        .to_eyre()?
        .build()
        .to_eyre()?;

    let host = host.start().await.to_eyre()?;

    Ok((host, wflow_plugin))
}

/// Start the partition worker for processing workflow jobs
pub async fn start_partition_worker(
    config: &RuntimeConfig,
    wflow_plugin: Arc<wflow::plugin::TownframewflowPlugin>,
) -> Res<partition::tokio::TokioPartitionWorkerHandle> {
    let cx = wflow::Ctx::new(config.metastore.clone());

    let pcx = partition::PartitionCtx::new(
        cx,
        config.partition_id,
        config.log_store.clone(),
        0,
        wflow_plugin,
    );

    let active_state = partition::state::PartitionWorkingState::default();
    let active_state = Arc::new(active_state);

    let worker = partition::tokio::start_tokio_worker(pcx, active_state).await;

    Ok(worker)
}
