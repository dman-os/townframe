use crate::interlude::*;

mod fails_once;
#[cfg(any(test, feature = "test-harness"))]
#[allow(unused)]
mod keyvalue_plugin;

use utils_rs::am::AmCtx;

use crate::{AtomicKvSnapStore, KvStoreLog, KvStoreMetadtaStore};
use wash_runtime::{host::HostApi, plugin, types, wit::WitInterface};
use wflow_core::metastore;
use wflow_core::snapstore::SnapStore;

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
    pub working_state: Arc<wflow_tokio::partition::state::PartitionWorkingState>,
}

impl WflowTestContext {
    /// Create a new test context with in-memory stores
    pub async fn new() -> Res<Self> {
        Self::with_am_ctx(None).await
    }

    /// Create a new test context with an existing AmCtx (or create a new one if None)
    pub async fn with_am_ctx(acx: Option<Arc<AmCtx>>) -> Res<Self> {
        // Use provided AmCtx or create a new one
        let acx = match acx {
            Some(acx) => acx,
            None => Arc::new(
                AmCtx::boot(
                    utils_rs::am::Config {
                        peer_id: "test".to_string(),
                        storage: utils_rs::am::StorageConfig::Memory,
                    },
                    Option::<samod::AlwaysAnnounce>::None,
                )
                .await?,
            ),
        };

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
        let log_store = KvStoreLog::new(
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
        let utils_plugin =
            wash_plugin_utils::UtilsPlugin::new().wrap_err("error creating utils plugin")?;

        let host = crate::build_wash_host(vec![
            wflow_plugin.clone(),
            am_repo_plugin,
            utils_plugin,
            Arc::new(runtime_config_plugin),
            Arc::new(keyvalue_plugin),
        ])
        .await?;

        // Start partition worker
        let (worker_handle, working_state) =
            crate::start_partition_worker(&wcx, wflow_plugin.clone()).await?;

        {
            let log_store = log_store.clone();
            tokio::spawn(async move {
                use futures::StreamExt;
                use wflow_core::log::LogStore;
                let mut stream = log_store.tail(0).await;
                while let Some(entry) = stream.next().await {
                    match entry {
                        Ok((entry_id, entry)) => {
                            let entry: serde_json::Value =
                                serde_json::from_slice(&entry[..]).expect(ERROR_JSON);
                            info!(?entry, entry_id, "log entry");
                        }
                        Err(err) => {
                            error!(?err, "log tail error");
                        }
                    }
                }
            });
        }

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
            working_state,
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
                        ..WitInterface::from("townframe:utils/llm-chat")
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

    /// Wait until there are no active jobs, with a timeout
    pub async fn wait_until_no_active_jobs(&self, timeout_secs: u64) -> Res<()> {
        use tokio::time::{Duration, Instant};

        let start = Instant::now();
        let timeout_duration = Duration::from_secs(timeout_secs);
        let mut change_rx = self.working_state.change_receiver();

        // Get initial counts without holding a lock
        let mut counts = *change_rx.borrow();
        if counts.active == 0 && counts.archive > 0 {
            // No active jobs, we're done
            tracing::info!("done, {} active jobs, {} archived jobs", counts.active, counts.archive);
            return Ok(());
        }

        loop {
            // Calculate remaining time
            let elapsed = start.elapsed();
            let remaining = timeout_duration.saturating_sub(elapsed);
            if remaining.is_zero() {
                return Err(eyre::eyre!(
                    "timeout waiting for no active jobs after {} seconds (elapsed: {:?}, active jobs: {})",
                    timeout_secs,
                    elapsed,
                    counts.active
                ));
            }

            tracing::debug!(
                "Waiting for count change or timeout (active jobs: {}, remaining: {:?})",
                counts.active,
                remaining
            );

            // Wait for the next count change or timeout
            match tokio::time::timeout(remaining, change_rx.changed()).await {
                Ok(Ok(())) => {
                    // Counts changed, update our local copy
                    counts = *change_rx.borrow();
                    if counts.active == 0 && counts.archive > 0 {
                        // No active jobs, we're done
                        tracing::info!("done, {} active jobs, {} archived jobs", counts.active, counts.archive);
                        return Ok(());
                    }
                    // Continue waiting
                    tracing::debug!("Counts changed, rechecking active jobs");
                    continue;
                }
                Ok(Err(_)) => {
                    // Channel closed, worker might be shutting down
                    return Err(eyre::eyre!("worker state channel closed"));
                }
                Err(_) => {
                    // Timeout reached
                    let final_elapsed = start.elapsed();
                    let final_counts = self.working_state.get_job_counts().await;
                    return Err(eyre::eyre!(
                        "timeout waiting for no active jobs after {} seconds (elapsed: {:?}, active jobs: {})",
                        timeout_secs,
                        final_elapsed,
                        final_counts.active
                    ));
                }
            }
        }
    }

    /// Get the full partition log for snapshot testing
    pub async fn get_partition_log_snapshot(
        &self,
    ) -> Res<Vec<(u64, wflow_core::partition::log::PartitionLogEntry)>> {
        use futures::StreamExt;
        use tokio::time::Duration;

        let mut entries = Vec::new();
        let mut stream = self.log_store.tail(0).await;

        // Read entries with a timeout to avoid waiting forever
        // If no entry comes for 100ms, we've read all available entries
        loop {
            match tokio::time::timeout(Duration::from_millis(100), stream.next()).await {
                Ok(Some(Ok((entry_id, entry_bytes)))) => {
                    let log_entry: wflow_core::partition::log::PartitionLogEntry =
                        serde_json::from_slice(&entry_bytes[..])
                            .wrap_err("failed to parse log entry")?;
                    entries.push((entry_id, log_entry));
                }
                Ok(Some(Err(err))) => {
                    return Err(err);
                }
                Ok(None) => {
                    // Stream ended
                    break;
                }
                Err(_) => {
                    // Timeout reached, we've read all available entries
                    break;
                }
            }
        }

        Ok(entries)
    }

    /// Assert a snapshot of the partition log with standard filters
    pub async fn assert_partition_log_snapshot(&self, snapshot_name: &str) -> Res<()> {
        let log_snapshot = self.get_partition_log_snapshot().await?;

        insta::with_settings!({
            filters => vec![
                (r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z", "[timestamp]"),
                (r"\w*Location.*:\d+:\d+", "[location]"),
            ]
        }, {
            insta::assert_yaml_snapshot!(snapshot_name, log_snapshot);
        });

        Ok(())
    }

    /// Cleanup: shutdown all workers
    pub async fn close(self) -> Res<()> {
        self.worker_handle.close().await?;
        Ok(())
    }
}
