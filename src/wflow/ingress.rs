use crate::interlude::*;

use wflow_core::metastore;
use wflow_core::partition::job_events::{JobCancelEvent, JobInitEvent, JobMessageEvent};
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
    /// * `wflow` - Complete captured metadata, including the original handler key
    /// * `args_json` - JSON arguments for the workflow
    /// * `retry_policy` - Optional retry policy override
    async fn add_job(
        &self,
        job_id: Arc<str>,
        wflow: metastore::WflowMeta,
        args_json: String,
        retry_policy: Option<wflow_core::partition::RetryPolicy>,
    ) -> Res<u64>;

    /// Request cancellation of a job. Appends JobCancel to partition log.
    async fn cancel_job(&self, job_id: Arc<str>, reason: String) -> Res<u64>;

    /// Send an external message to a running job.
    async fn send_message(
        &self,
        job_id: Arc<str>,
        message_id: Arc<str>,
        payload_json: String,
    ) -> Res<u64>;
}

/// Implementation that appends directly to partition log
pub struct PartitionLogIngress {
    log: PartitionLogRef,
}

impl PartitionLogIngress {
    pub fn new(log: PartitionLogRef) -> Self {
        Self { log }
    }
}

#[async_trait]
impl WflowIngress for PartitionLogIngress {
    async fn add_job(
        &self,
        job_id: Arc<str>,
        wflow: metastore::WflowMeta,
        args_json: String,
        retry_policy: Option<wflow_core::partition::RetryPolicy>,
    ) -> Res<u64> {
        // Append to partition log
        let mut log = self.log.clone();
        let entry_id = log
            .append(&PartitionLogEntry::JobInit(JobInitEvent {
                args_json: args_json.into(),
                override_wflow_retry_policy: retry_policy,
                wflow,
                timestamp: Timestamp::now(),
                job_id,
            }))
            .await?;

        Ok(entry_id)
    }

    async fn cancel_job(&self, job_id: Arc<str>, reason: String) -> Res<u64> {
        let mut log = self.log.clone();
        let entry_id = log
            .append(&PartitionLogEntry::JobCancel(JobCancelEvent {
                job_id,
                timestamp: Timestamp::now(),
                reason: reason.into(),
            }))
            .await?;
        Ok(entry_id)
    }

    async fn send_message(
        &self,
        job_id: Arc<str>,
        message_id: Arc<str>,
        payload_json: String,
    ) -> Res<u64> {
        let mut log = self.log.clone();
        let entry_id = log
            .append(&PartitionLogEntry::JobMessage(JobMessageEvent {
                job_id,
                message_id,
                timestamp: Timestamp::now(),
                payload_json: payload_json.into(),
            }))
            .await?;
        Ok(entry_id)
    }
}
