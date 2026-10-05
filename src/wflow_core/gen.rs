//! @generated
use super::*;

pub mod metastore {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub struct WasmcloudWflowServiceMeta {
        pub workload_id: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    #[serde(rename_all = "camelCase")]
    pub enum WflowServiceMeta {
        Wasmcloud(WasmcloudWflowServiceMeta),
        LocalNative,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub struct WflowMeta {
        pub key: String,
        pub service: WflowServiceMeta,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub struct PartitionsMeta {
        pub version: String,
        pub partition_count: u64,
    }
}
pub mod types {
    use super::*;

    pub type JobId = String;

    pub type PartitionId = u64;
}
