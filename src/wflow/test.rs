use crate::interlude::*;

#[cfg(test)]
mod fails_once;
#[cfg(test)]
mod fails_until_told;
#[cfg(any(test, feature = "test-harness"))]
#[allow(unused)]
mod keyvalue_plugin;

use crate::{AtomicKvSnapStore, KvStoreLog, KvStoreMetadtaStore};
use wash_runtime::{host::HostApi, plugin, types, wit::WitInterface};
use wflow_core::metastore;
use wflow_core::snapstore::SnapStore;

/// Builder used to configure [`WflowTestContext`]
pub struct WflowTestContextBuilder {
    temp_dir: tempfile::TempDir,
    metastore: Option<Arc<dyn metastore::MetdataStore>>,
    log_store: Option<Arc<dyn wflow_core::log::LogStore>>,
    snap_store: Option<Arc<dyn SnapStore>>,
    keyvalue_plugin: Option<Arc<keyvalue_plugin::WasiKeyvalue>>,
    initial_workloads: Vec<InitialWorkload>,
    plugins: Vec<Arc<dyn plugin::HostPlugin>>,
}

impl WflowTestContextBuilder {
    pub fn new() -> Self {
        Self {
            temp_dir: tokio::task::block_in_place(|| tempfile::tempdir())
                .expect("failed to create temp dir"),
            metastore: None,
            log_store: None,
            snap_store: None,
            keyvalue_plugin: None,
            initial_workloads: Vec::new(),
            plugins: Vec::new(),
        }
    }

    pub fn with_metastore(mut self, metastore: Arc<dyn metastore::MetdataStore>) -> Self {
        self.metastore = Some(metastore);
        self
    }

    pub fn with_log_store(mut self, log_store: Arc<dyn wflow_core::log::LogStore>) -> Self {
        self.log_store = Some(log_store);
        self
    }

    pub fn with_snapstore(mut self, snap_store: Arc<dyn SnapStore>) -> Self {
        self.snap_store = Some(snap_store);
        self
    }

    pub fn with_keyvalue_plugin(
        mut self,
        keyvalue_plugin: Arc<keyvalue_plugin::WasiKeyvalue>,
    ) -> Self {
        self.keyvalue_plugin = Some(keyvalue_plugin);
        self
    }

    pub fn add_initial_workload(mut self, workload: InitialWorkload) -> Self {
        self.initial_workloads.push(workload);
        self
    }

    pub fn initial_workloads<I>(mut self, workloads: I) -> Self
    where
        I: IntoIterator<Item = InitialWorkload>,
    {
        self.initial_workloads.extend(workloads);
        self
    }

    pub fn with_plugin(mut self, plugin: Arc<dyn plugin::HostPlugin>) -> Self {
        self.plugins.push(plugin);
        self
    }

    pub async fn build(mut self) -> Res<WflowTestContext> {
        let temp_dir = self.temp_dir;

        let metastore = match self.metastore {
            Some(store) => store,
            None => {
                let meta = KvStoreMetadtaStore::new(
                    new_in_memory_kv_store(),
                    wflow_core::gen::metastore::PartitionsMeta {
                        version: "0".into(),
                        partition_count: 1,
                    },
                )
                .await?;
                Arc::new(meta)
            }
        };

        let log_store = match self.log_store {
            Some(store) => store,
            None => Arc::new(KvStoreLog::new(new_in_memory_kv_store(), 0)),
        };

        let partition_log = wflow_tokio::partition::PartitionLogRef::new(log_store.clone());
        let ingress = Arc::new(crate::ingress::PartitionLogIngress::new(
            partition_log.clone(),
            metastore.clone(),
        ));

        let snapstore = match self.snap_store {
            Some(store) => store,
            None => Arc::new(AtomicKvSnapStore::new(new_in_memory_kv_store())),
        };

        let keyvalue_plugin = self
            .keyvalue_plugin
            .unwrap_or_else(|| Arc::new(keyvalue_plugin::WasiKeyvalue::new()));

        let wflow_plugin = Arc::new(wash_plugin_wflow::WflowPlugin::new(metastore.clone()));
        let runtime_config_plugin = plugin::wasi_config::WasiConfig::default();

        self.plugins.extend_from_slice(&[
            wflow_plugin.clone(),
            Arc::new(runtime_config_plugin),
            keyvalue_plugin.clone(),
        ]);

        let host = crate::build_wash_host(self.plugins).await?;
        Ok(WflowTestContext {
            temp_dir,
            metastore,
            log_store,
            snapstore,
            partition_log,
            ingress,
            keyvalue_plugin,
            initial_workloads: self.initial_workloads,
            pending_host: Some(host),
            host: None,
            wflow_plugin,
            worker_handle: None,
            working_state: None,
        })
    }
}
/// Test context for wflow tests
#[allow(unused)]
pub struct WflowTestContext {
    pub temp_dir: tempfile::TempDir,
    pub metastore: Arc<dyn metastore::MetdataStore>,
    pub log_store: Arc<dyn wflow_core::log::LogStore>,
    pub snapstore: Arc<dyn SnapStore>,
    pub partition_log: wflow_tokio::partition::PartitionLogRef,
    pub ingress: Arc<crate::ingress::PartitionLogIngress>,
    pub keyvalue_plugin: Arc<keyvalue_plugin::WasiKeyvalue>,
    initial_workloads: Vec<InitialWorkload>,
    pending_host: Option<wash_runtime::host::Host>,
    host: Option<Arc<wash_runtime::host::Host>>,
    wflow_plugin: Arc<wash_plugin_wflow::WflowPlugin>,
    worker_handle: Option<wflow_tokio::partition::TokioPartitionWorkerHandle>,
    working_state: Option<Arc<wflow_tokio::partition::state::PartitionWorkingState>>,
}

/// Workload to register before starting the worker
pub struct InitialWorkload {
    pub wasm_path: String,
    pub wflow_keys: Vec<String>,
}

impl WflowTestContext {
    /// Returns true if the wash host and partition worker have been started.
    pub fn is_started(&self) -> bool {
        self.host.is_some()
    }

    /// Start the wash host and partition worker using the configured stores.
    /// Safe to call multiple times; subsequent calls are no-ops.
    pub async fn start(mut self) -> Res<Self> {
        if self.is_started() {
            return Ok(self);
        }

        let wcx = crate::Ctx {
            metastore: self.metastore.clone(),
            log_store: self.log_store.clone(),
            snapstore: self.snapstore.clone(),
        };

        let host = self.pending_host.take().expect("bad builder");
        let host = host.start().await.to_eyre()?;

        // Register any initial workloads before starting the worker
        for workload in &self.initial_workloads {
            register_workload_on_host(
                &host,
                workload.wasm_path.as_str(),
                workload.wflow_keys.clone(),
            )
            .await?;
        }

        self.host = Some(host);

        let (worker_handle, working_state) =
            crate::start_partition_worker(&wcx, self.wflow_plugin.clone(), 0).await?;

        self.worker_handle = Some(worker_handle);
        self.working_state = Some(working_state);

        Ok(self)
    }

    fn host(&self) -> Res<&Arc<wash_runtime::host::Host>> {
        self.host
            .as_ref()
            .ok_or_else(|| ferr!("wflow test context not started. call start().await?"))
    }

    fn working_state(&self) -> Res<&Arc<wflow_tokio::partition::state::PartitionWorkingState>> {
        self.working_state
            .as_ref()
            .ok_or_else(|| ferr!("wflow test context not started. call start().await?"))
    }

    /// Create a new builder for [`WflowTestContext`]
    pub fn builder() -> WflowTestContextBuilder {
        WflowTestContextBuilder::new()
    }

    /// Register a workload from a WASM file
    pub async fn register_workload(&self, wasm_path: &str, wflow_keys: Vec<String>) -> Res<()> {
        let host = self.host()?;
        register_workload_on_host(host.as_ref(), wasm_path, wflow_keys).await
    }

    /// Schedule a workflow job
    pub async fn schedule_job(
        &self,
        job_id: Arc<str>,
        wflow_key: &str,
        args_json: String,
    ) -> Res<()> {
        use crate::WflowIngress;
        self.ingress
            .add_job(job_id, wflow_key.into(), args_json, None)
            .await
    }

    /// Wait until there are no active jobs, with a timeout
    pub async fn wait_until_no_active_jobs(&self, timeout_secs: u64) -> Res<()> {
        use tokio::time::{Duration, Instant};

        let start = Instant::now();
        let timeout_duration = Duration::from_secs(timeout_secs);
        let working_state = self.working_state()?;
        let mut change_rx = working_state.change_receiver();

        // Get initial counts without holding a lock
        let mut counts = *change_rx.borrow();
        if counts.active == 0 && counts.archive > 0 {
            // No active jobs, we're done
            tracing::info!(
                "done, {} active jobs, {} archived jobs",
                counts.active,
                counts.archive
            );
            return Ok(());
        }

        loop {
            // Calculate remaining time
            let elapsed = start.elapsed();
            let remaining = timeout_duration.saturating_sub(elapsed);
            if remaining.is_zero() {
                return Err(ferr!(
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
                        tracing::info!(
                            "done, {} active jobs, {} archived jobs",
                            counts.active,
                            counts.archive
                        );
                        return Ok(());
                    }
                    // Continue waiting
                    tracing::debug!("Counts changed, rechecking active jobs");
                    continue;
                }
                Ok(Err(_)) => {
                    // Channel closed, worker might be shutting down
                    return Err(ferr!("worker state channel closed"));
                }
                Err(_) => {
                    // Timeout reached
                    let final_elapsed = start.elapsed();
                    let final_counts = working_state.get_job_counts().await;
                    return Err(ferr!(
                        "timeout waiting for no active jobs after {} seconds (elapsed: {:?}, active jobs: {})",
                        timeout_secs,
                        final_elapsed,
                        final_counts.active
                    ));
                }
            }
        }
    }

    /// Wait until a log entry matches the provided condition
    /// The callback receives (entry_id, log_entry) and should return true when the condition is met
    pub async fn wait_until_entry<F>(
        &self,
        start_entry_id: u64,
        timeout_secs: u64,
        mut condition: F,
    ) -> Res<(u64, wflow_core::partition::log::PartitionLogEntry)>
    where
        F: FnMut(u64, &wflow_core::partition::log::PartitionLogEntry) -> bool,
    {
        use futures::StreamExt;
        use tokio::time::{Duration, Instant};

        let start = Instant::now();
        let timeout_duration = Duration::from_secs(timeout_secs);
        let mut stream = self.log_store.tail(start_entry_id).await;

        loop {
            // Calculate remaining time
            let elapsed = start.elapsed();
            let remaining = timeout_duration.saturating_sub(elapsed);
            if remaining.is_zero() {
                return Err(ferr!(
                    "timeout waiting for log entry condition after {} seconds",
                    timeout_secs
                ));
            }

            // Wait for next entry with timeout
            match tokio::time::timeout(remaining, stream.next()).await {
                Ok(Some(Ok((entry_id, entry_bytes)))) => {
                    let log_entry: wflow_core::partition::log::PartitionLogEntry =
                        serde_json::from_slice(&entry_bytes[..])
                            .wrap_err("failed to parse log entry")?;

                    if condition(entry_id, &log_entry) {
                        return Ok((entry_id, log_entry));
                    }
                    // Continue waiting
                }
                Ok(Some(Err(err))) => {
                    return Err(err);
                }
                Ok(None) => {
                    // Stream ended, wait a bit and retry
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                Err(_) => {
                    // Timeout reached
                    return Err(ferr!(
                        "timeout waiting for log entry condition after {} seconds",
                        timeout_secs
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

    /// Set a value in the keyvalue store (for testing)
    pub async fn set_keyvalue(&self, bucket: &str, key: &str, value: Vec<u8>) -> Res<()> {
        // Use the hardcoded workload_id from register_workload
        self.keyvalue_plugin
            .set_value("workload_123", bucket, key, value)
            .await
            .to_eyre()?;
        Ok(())
    }

    /// Cleanup: shutdown all workers
    pub async fn close(self) -> Res<()> {
        if let Some(worker_handle) = self.worker_handle {
            worker_handle.close().await?;
        }
        Ok(())
    }
}

fn new_in_memory_kv_store() -> Arc<Arc<DHashMap<Arc<[u8]>, Arc<[u8]>>>> {
    let kv: DHashMap<Arc<[u8]>, Arc<[u8]>> = default();
    let kv = Arc::new(kv);
    Arc::new(kv)
}

async fn register_workload_on_host(
    host: &wash_runtime::host::Host,
    wasm_path: &str,
    wflow_keys: Vec<String>,
) -> Res<()> {
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
                    ..WitInterface::from("townframe:daybook/drawer")
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

    host.workload_start(req).await.to_eyre()?;
    Ok(())
}
