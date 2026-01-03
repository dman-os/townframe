use crate::interlude::*;

use wflow_core::metastore;
use wflow_core::partition::job_events::JobInitEvent;
use wflow_core::partition::log::PartitionLogEntry;
use wflow_tokio::partition::PartitionLogRef;

/// Trait for scheduling workflow jobs
///
/// Implementations can schedule workflows through different mechanisms:
/// - Direct partition log appends (for local execution)
/// - HTTP API (for remote execution via wflow_ingress_http)
#[async_trait]
pub trait WflowIngress: Send + Sync {
    /// Add a workflow job to the queue
    ///
    /// # Arguments
    /// * `job_id` - Unique identifier for the job
    /// * `wflow_key` - The workflow key to execute
    /// * `args_json` - JSON arguments for the workflow
    /// * `retry_policy` - Optional retry policy override
    async fn add_job(
        &self,
        job_id: Arc<str>,
        wflow_key: &str,
        args_json: String,
        retry_policy: Option<wflow_core::partition::RetryPolicy>,
    ) -> Res<u64>;
}

/// Implementation that appends directly to partition log
pub struct PartitionLogIngress {
    log: PartitionLogRef,
    metastore: Arc<dyn metastore::MetdataStore>,
}

impl PartitionLogIngress {
    pub fn new(log: PartitionLogRef, metastore: Arc<dyn metastore::MetdataStore>) -> Self {
        Self { log, metastore }
    }
}

#[async_trait]
impl WflowIngress for PartitionLogIngress {
    async fn add_job(
        &self,
        job_id: Arc<str>,
        wflow_key: &str,
        args_json: String,
        retry_policy: Option<wflow_core::partition::RetryPolicy>,
    ) -> Res<u64> {
        // Get workflow metadata
        let wflow_meta = self
            .metastore
            .get_wflow(&wflow_key)
            .await
            .wrap_err("error getting workflow metadata")?
            .ok_or_eyre(format!("workflow not found: {wflow_key}"))?;

        // Append to partition log
        let mut log = self.log.clone();
        let entry_id = log
            .append(&PartitionLogEntry::JobInit(JobInitEvent {
                args_json: args_json.into(),
                override_wflow_retry_policy: retry_policy,
                wflow: wflow_meta,
                timestamp: Timestamp::now(),
                job_id,
            }))
            .await?;

        Ok(entry_id)
    }
}
