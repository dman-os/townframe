use crate::interlude::*;

use crate::metastore;
use crate::plugin::binds_partition_host::townframe::wflow::partition_host;

pub mod effects;
pub mod job_events;
pub mod log;
mod reduce;
pub mod service;
pub mod state;
pub mod tokio;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RetryPolicy {
    Immediate,
}

#[derive(Clone)]
pub struct PartitionCtx {
    id: partition_host::PartitionId,
    cx: crate::SharedCtx,
    processed_entries_offset: u64,
    log: Arc<dyn crate::log::LogStore>,
    local_wasmcloud_host: Arc<
        dyn service::WflowServiceHost<ExtraArgs = metastore::WasmcloudWflowServiceMeta>
            + Sync
            + Send,
    >,
}

impl PartitionCtx {
    pub fn new(
        cx: crate::SharedCtx,
        id: partition_host::PartitionId,
        log: Arc<dyn crate::log::LogStore>,
        processed_entries_offset: u64,
        local_wasmcloud_host: Arc<
            dyn service::WflowServiceHost<ExtraArgs = metastore::WasmcloudWflowServiceMeta>
                + Sync
                + Send,
        >,
    ) -> Self {
        Self {
            id,
            cx,
            processed_entries_offset,
            log,
            local_wasmcloud_host,
        }
    }

    pub fn log_ref(&self) -> log::PartitionLogRef {
        log::PartitionLogRef::new(self.log.clone())
    }
}
