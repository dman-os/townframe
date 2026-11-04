use big_cabinet_core::datum::Declaration;
use big_cabinet_core::datum::Extra;
use big_cabinet_core::datum::Universe;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::sync::LazyLock;

#[macros::datum(authority = "org.example.daybook")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Blob {
    pub mime: String,
    pub length_octets: u64,
    pub digest: String,
    #[schemars(extend("x-automerge" = "list"))]
    pub inline: Option<Vec<u8>>,
    pub urls: Option<Vec<String>>,
    #[serde(flatten)]
    pub extra: Extra,
}

#[macros::datum(authority = "org.example.daybook")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BlobPin {
    pub length_octets: u64,
    #[serde(flatten)]
    pub extra: Extra,
}

#[macros::datum]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Representation {
    pub digest: String,
    pub length_octets: u64,
    #[serde(flatten)]
    pub extra: Extra,
}

#[macros::datum]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Rfc8188EncodingParameters {
    pub record_size: u64,
    pub padding: String,
    #[serde(flatten)]
    pub extra: Extra,
}

#[macros::datum(authority = "org.example.daybook")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CipherBlob {
    pub representation: Representation,
    pub content_encoding: String,
    pub key_ref: String,
    pub key_ref_heads: Vec<String>,
    pub encoding_parameters: Rfc8188EncodingParameters,
    #[serde(flatten)]
    pub extra: Extra,
}

#[macros::datum(authority = "org.example.daybook")]
#[derive(Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Jwk {
    pub kty: String,
    #[serde(flatten)]
    pub members: Extra,
}

impl std::fmt::Debug for Jwk {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Jwk").finish_non_exhaustive()
    }
}

pub fn declarations() -> Universe {
    Universe::collect([
        Declaration::of::<Blob>().expect("Blob declares a valid datum"),
        Declaration::of::<BlobPin>().expect("BlobPin declares a valid datum"),
        Declaration::of::<CipherBlob>().expect("CipherBlob declares a valid datum"),
        Declaration::of::<Jwk>().expect("Jwk declares a valid datum"),
    ])
}

pub static UNIVERSE: LazyLock<Universe> = LazyLock::new(declarations);
