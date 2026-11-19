use crate::BlobId;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::Capability;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::dmeta::Bytes;
use big_cabinet_core::dmeta::Uuid7;
use method_core::ContextError;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeSet;

pub const MAX_READ_BYTES: u32 = 1024 * 1024;
/// The most one [`PutBlob`] carries, counted in decoded bytes. On the wire the payload is base64
/// inside the `$bytes` object, so it is about a third larger than this and still fits the 16 MiB
/// body a node listener accepts.
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
    /// The datum model's bytes object, `{"$bytes": "<base64>"}`: a JSON array of numbers is
    /// neither compact nor a Lexicon type.
    pub bytes: Bytes,
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
    /// The datum model's bytes object, `{"$bytes": "<base64>"}`.
    pub bytes: Bytes,
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

/// What a minted blobs handle may do: put new bytes, and read (range or stat) each blob in
/// `read`. There is no read-everything scope, so a read handle names the blobs it was minted for.
/// A handle can only be minted with a scope its minter covers, so a scope only ever narrows; the
/// node operator's own identity covers every scope.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BlobsScope {
    #[serde(default)]
    pub put: bool,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub read: BTreeSet<BlobId>,
}

impl BlobsScope {
    pub fn covers(&self, other: &Self) -> bool {
        (self.put || !other.put) && other.read.is_subset(&self.read)
    }
}

/// Mints a blobs handle for `scope` and `rights`, authorized by the request's `Authorization`
/// credential rather than a body field: the node operator's identity may mint any scope, and a
/// blobs handle may mint only a subset of its own scope and rights.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MintBlobsCapability {
    pub scope: BlobsScope,
    pub rights: Rights,
    /// The actor the new handle is bound to; `None` binds it to the presented handle's own
    /// subject. Naming another actor is an introduction: the presented handle needs
    /// `Rights::DELEGATE`, and the actor must be live and a strict descendant of the presented
    /// handle's subject in the actor tree (anyone else is `Escalation`).
    pub subject: Option<Uuid7>,
}

service_core_rs::service_method!(MintBlobsCapability => Capability, BlobsError, procedure("org.example.daybook.blobs.mintCapability"));
service_core_rs::service_method!(PutBlob => BlobInfo, BlobsError, procedure("org.example.daybook.blobs.put"));
service_core_rs::service_method!(ReadBlob => BlobRange, BlobsError, procedure("org.example.daybook.blobs.read"));
/// The result of [`StatBlob`]: `info` is absent when the blob is not held locally.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlobStat {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub info: Option<BlobInfo>,
}

service_core_rs::service_method!(StatBlob => BlobStat, BlobsError, procedure("org.example.daybook.blobs.stat"));

service_core_rs::declare_service_protocol! {
    pub enum BlobsProtocol => BlobsMessage {
        unary {
            MintBlobsCapability(MintBlobsCapability),
            PutBlob(PutBlob),
            ReadBlob(ReadBlob),
            StatBlob(StatBlob)
        }
        stream {}
    }
}
