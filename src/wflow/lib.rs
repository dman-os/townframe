mod interlude {
    pub use api_utils_rs::{api, prelude::*};
}

use crate::interlude::*;

mod log;
mod metastore;
mod partition;
mod plugin;

struct Ctx {
    metadata: Arc<dyn metastore::MetdataStore>,
}
type SharedCtx = Arc<Ctx>;

impl Ctx {
    fn new(metadata: Arc<dyn metastore::MetdataStore>) -> Arc<Self> {
        Arc::new(Self { metadata })
    }
}

pub async fn start_host() -> Res<Arc<wash_runtime::host::Host>> {
    use wash_runtime::*;

    let metastore = {
        let kv = DHashMap::default();
        let kv = Arc::new(kv);
        let metastore = crate::metastore::KvStoreMetadtaStore::new(
            kv,
            metastore::PartitionsMeta {
                version: "0".into(),
                partition_count: 1,
            },
        )
        .await?;
        Arc::new(metastore)
    };

    let cx = crate::Ctx::new(metastore.clone());

    let wflow_plugin = crate::plugin::TownframewflowPlugin::new(metastore);
    let wflow_plugin = Arc::new(wflow_plugin);

    let pcx = partition::PartitionCtx::new(
        cx.clone(),
        0,
        {
            let kv = DHashMap::default();
            let kv = Arc::new(kv);
            let log = crate::log::KvStoreLog::new(kv, 0);
            Arc::new(log)
        },
        0,
        wflow_plugin.clone(),
    );
    let mut log_ref = pcx.log_ref();

    // TODO: recover from snapshhot
    let active_state = partition::state::PartitionWorkingState::default();
    let active_state = Arc::new(active_state);

    let worker = partition::tokio::start_tokio_worker(pcx, active_state.clone()).await;

    let host = {
        // Create a Wasmtime engine
        let engine = engine::Engine::builder().build().to_eyre()?;

        // Configure plugins
        let http_plugin = plugin::wasi_http::HttpServer::new("127.0.0.1:8080".parse()?);
        let runtime_config_plugin = plugin::wasi_config::RuntimeConfig::default();
        // Build and start the host
        host::HostBuilder::new()
            .with_engine(engine)
            .with_plugin(Arc::new(http_plugin))
            .to_eyre()?
            .with_plugin(Arc::new(runtime_config_plugin))
            .to_eyre()?
            .with_plugin(wflow_plugin.clone())
            .to_eyre()?
            .build()
            .to_eyre()?
    };

    let host = host.start().await.to_eyre()?;

    Ok(host)
}

#[async_trait]
trait KvStore {
    async fn count(&self) -> Res<u64>;
    async fn get(&self, key: &[u8]) -> Res<Option<Arc<[u8]>>>;
    async fn set(&self, key: Arc<[u8]>, value: Arc<[u8]>) -> Res<Option<Arc<[u8]>>>;
    async fn del(&self, key: &[u8]) -> Res<Option<Arc<[u8]>>>;
}

#[async_trait]
impl KvStore for DHashMap<Arc<[u8]>, Arc<[u8]>> {
    async fn count(&self) -> Res<u64> {
        Ok(self.len() as u64)
    }
    async fn get(&self, key: &[u8]) -> Res<Option<Arc<[u8]>>> {
        Ok(self.get(key).map(|v| v.value().clone()))
    }
    async fn set(&self, key: Arc<[u8]>, value: Arc<[u8]>) -> Res<Option<Arc<[u8]>>> {
        Ok(self.insert(key, value))
    }
    async fn del(&self, key: &[u8]) -> Res<Option<Arc<[u8]>>> {
        Ok(self.remove(key).map(|(_, val)| val))
    }
}
