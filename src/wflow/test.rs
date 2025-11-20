use crate::interlude::*;

mod fails_once;
mod keyvalue_plugin;

use utils_rs::am::AmCtx;

use wash_runtime::{host::HostApi, plugin, types, wit::WitInterface};
use wflow_core::metastore;
use wflow_tokio::{metastore::KvStoreMetadtaStore, AtomicKvSnapStore, SnapStore};

/// Test context for wflow tests
#[allow(unused)]
pub struct WflowTestContext {
    pub am_ctx: Arc<AmCtx>,
    pub metastore: Arc<dyn metastore::MetdataStore>,
    pub log_store: Arc<dyn wflow_core::log::LogStore>,
    pub snap_store: Option<Arc<dyn SnapStore>>,
    pub partition_log: wflow_tokio::partition::PartitionLogRef,
    pub ingress: Arc<crate::ingress::PartitionLogIngress>,
    pub host: Arc<wash_runtime::host::Host>,
    pub wflow_plugin: Arc<wash_plugin_wflow::TownframewflowPlugin>,
    pub worker_handle: wflow_tokio::partition::TokioPartitionWorkerHandle,
}

impl WflowTestContext {
    /// Create a new test context with in-memory stores
    pub async fn new() -> Res<Self> {
        // Initialize AmCtx with memory storage
        let acx = AmCtx::boot(
            utils_rs::am::Config {
                peer_id: "test".to_string(),
                storage: utils_rs::am::StorageConfig::Memory,
            },
            Option::<samod::AlwaysAnnounce>::None,
        )
        .await?;
        let acx = Arc::new(acx);

        // Create metastore
        let metastore = {
            KvStoreMetadtaStore::new(
                {
                    let kv: DHashMap<Arc<[u8]>, Arc<[u8]>> = default();
                    let kv = Arc::new(kv);
                    Arc::new(kv)
                },
                wflow_core::gen::metastore::PartitionsMeta {
                    version: "0".into(),
                    partition_count: 1,
                },
            )
            .await?
        };
        let metastore = Arc::new(metastore);

        // Create log store
        let log_store = wflow_tokio::KvStoreLog::new(
            {
                let kv: DHashMap<Arc<[u8]>, Arc<[u8]>> = default();
                let kv = Arc::new(kv);
                Arc::new(kv)
            },
            0,
        );
        let log_store = Arc::new(log_store);

        // Create partition log reference
        let partition_log = wflow_tokio::partition::PartitionLogRef::new(log_store.clone());

        // Create ingress
        let ingress =
            crate::ingress::PartitionLogIngress::new(partition_log.clone(), metastore.clone());
        let ingress = Arc::new(ingress);

        // Create snap store
        let snap_store = Arc::new(AtomicKvSnapStore::new({
            let kv: DHashMap<Arc<[u8]>, Arc<[u8]>> = default();
            let kv = Arc::new(kv);
            Arc::new(kv)
        })) as Arc<dyn SnapStore>;

        // Build runtime host
        let wcx = crate::Ctx {
            acx: acx.clone(),
            metastore: metastore.clone(),
            log_store: log_store.clone(),
            partition_id: 0,
            snap_store: Some(snap_store.clone()),
        };

        let wflow_plugin = wash_plugin_wflow::TownframewflowPlugin::new(wcx.metastore.clone());
        let wflow_plugin = Arc::new(wflow_plugin);

        let am_repo_plugin = wash_plugin_am_repo::AmRepoPlugin::new(wcx.acx.clone());
        let am_repo_plugin = Arc::new(am_repo_plugin);

        let runtime_config_plugin = plugin::wasi_config::WasiConfig::default();
        let keyvalue_plugin = keyvalue_plugin::WasiKeyvalue::new();

        let host = crate::build_wash_host(vec![
            wflow_plugin.clone(),
            am_repo_plugin,
            Arc::new(runtime_config_plugin),
            Arc::new(keyvalue_plugin),
        ])
        .await?;

        // Start partition worker
        let worker_handle = crate::start_partition_worker(&wcx, wflow_plugin.clone()).await?;

        Ok(Self {
            am_ctx: acx,
            metastore,
            log_store,
            snap_store: Some(snap_store),
            partition_log,
            ingress,
            host,
            wflow_plugin,
            worker_handle,
        })
    }

    /// Register a workload from a WASM file
    pub async fn register_workload(&self, wasm_path: &str, wflow_keys: Vec<String>) -> Res<()> {
        let wasm_bytes = tokio::fs::read(wasm_path).await?;

        let req = types::WorkloadStartRequest {
            workload_id: "workload_123".into(),
            workload: types::Workload {
                namespace: "test".to_string(),
                name: format!("test-wflows-{}", wflow_keys.join("-")),
                annotations: std::collections::HashMap::new(),
                service: None,
                components: vec![types::Component {
                    bytes: wasm_bytes.into(),
                    ..default()
                }],
                host_interfaces: vec![
                    WitInterface {
                        config: [("wflow_keys".to_owned(), wflow_keys.join(","))].into(),
                        ..WitInterface::from("townframe:wflow/bundle")
                    },
                    WitInterface {
                        ..WitInterface::from("townframe:am-repo/repo")
                    },
                    WitInterface {
                        ..WitInterface::from("wasi:keyvalue/store")
                    },
                ],
                volumes: vec![],
            },
        };

        self.host.workload_start(req).await.to_eyre()?;
        Ok(())
    }

    /// Schedule a workflow job
    pub async fn schedule_job(
        &self,
        job_id: Arc<str>,
        wflow_key: String,
        args_json: String,
    ) -> Res<()> {
        use crate::WflowIngress;
        self.ingress
            .add_job(job_id, wflow_key, args_json, None)
            .await
    }

    /// Wait for a job to complete successfully
    pub async fn wait_for_job_success(&self, timeout_secs: u64) -> Res<()> {
        use futures::StreamExt;
        use tokio::time::{sleep, Duration};
        use wflow_core::partition::job_events::{JobEventDeets, JobRunResult};
        use wflow_core::partition::log::PartitionLogEntry;

        let mut stream = self.log_store.tail(0).await;
        let timeout = sleep(Duration::from_secs(timeout_secs));
        tokio::pin!(timeout);

        loop {
            let entry = tokio::select! {
                _ = &mut timeout => {
                    return Err(eyre::eyre!("timeout waiting for workflow to complete"));
                }
                entry = stream.next() => {
                    entry
                }
            };
            let Some(Ok((_, entry_bytes))) = entry else {
                continue;
            };

            let log_entry: PartitionLogEntry =
                serde_json::from_slice(&entry_bytes[..]).wrap_err("failed to parse log entry")?;

            match log_entry {
                PartitionLogEntry::JobEvent(job_event) => {
                    match job_event.deets {
                        JobEventDeets::Run(run_event) => {
                            match run_event.result {
                                JobRunResult::Success { .. } => {
                                    tracing::info!("Workflow completed successfully!");
                                    return Ok(());
                                }
                                JobRunResult::WflowErr(err) => {
                                    return Err(eyre::eyre!("workflow error: {:?}", err));
                                }
                                JobRunResult::WorkerErr(err) => {
                                    return Err(eyre::eyre!("worker error: {:?}", err));
                                }
                                JobRunResult::StepEffect(_) => {
                                    // Still processing, continue waiting
                                }
                            }
                        }
                        JobEventDeets::Init(_) => {
                            // Job initialized, continue waiting
                        }
                    }
                }
                PartitionLogEntry::NewPartitionEffects(_) => {
                    // Effects entry, continue waiting
                }
            }
        }
    }

    /// Cleanup: shutdown all workers
    pub async fn close(self) -> Res<()> {
        self.worker_handle.close().await?;
        Ok(())
    }
}
