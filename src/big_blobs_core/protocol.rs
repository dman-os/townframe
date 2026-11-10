use crate::BlobId;
use big_cabinet_core::authority::CapId;
use method_core::ContextError;
use serde::Deserialize;
use serde::Serialize;

pub const MAX_READ_BYTES: u32 = 1024 * 1024;
pub const MAX_PUT_BYTES: usize = 8 * 1024 * 1024;

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlobInfo {
    pub blob_id: BlobId,
    pub length_octets: u64,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlobRange {
    pub bytes: Vec<u8>,
    pub length_octets: u64,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum BlobsError {
    #[error("request context failed: {0}")]
    Context(#[from] ContextError),
    #[error("the presented capability does not permit this operation")]
    Denied,
    #[error("blob is not locally available")]
    NotFound,
    #[error("invalid blob request: {reason}")]
    InvalidInput { reason: String },
    #[error("blob storage failed: {reason}")]
    Storage { reason: String },
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PutBlob {
    pub capability: CapId,
    pub bytes: Vec<u8>,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadBlob {
    pub capability: CapId,
    pub blob_id: BlobId,
    pub offset: u64,
    pub length: u32,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatBlob {
    pub capability: CapId,
    pub blob_id: BlobId,
}

service_core_rs::service_method!(PutBlob => BlobInfo, BlobsError, procedure("org.example.daybook.blobs.put"));
service_core_rs::service_method!(ReadBlob => BlobRange, BlobsError, procedure("org.example.daybook.blobs.read"));
service_core_rs::service_method!(StatBlob => Option<BlobInfo>, BlobsError, procedure("org.example.daybook.blobs.stat"));

service_core_rs::declare_service_protocol! {
    pub enum BlobsProtocol => BlobsMessage {
        unary {
            PutBlob(PutBlob),
            ReadBlob(ReadBlob),
            StatBlob(StatBlob)
        }
        stream {}
    }
}
