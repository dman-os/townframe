pub mod doc {
    use crate::interlude::*;

    use crate::doc as root_doc;
    use api_utils_rs::wit::townframe::api_utils::utils::Datetime;
    pub use root_doc::{Blob, BlobPin, DocId, FacetKey, MimeType, Multihash, Note, UserPathBuf};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct UserMeta {
        pub user_path: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Pending {
        pub key: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    #[serde(rename_all = "camelCase")]
    pub struct Body {
        pub order: Vec<String>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    #[serde(rename_all = "camelCase")]
    pub struct NoteMimeOption {
        pub mime: MimeType,
        pub label: String,
        pub description: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    #[serde(rename_all = "camelCase")]
    pub struct NoteEditorConfig {
        pub mime_options: Vec<NoteMimeOption>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde_with::serde_as]
    pub struct FacetMeta {
        #[serde(with = "api_utils_rs::codecs::datetime")]
        pub created_at: Datetime,
        #[serde_as(as = "Vec<Datetime>")]
        pub updated_at: Vec<Datetime>,
        #[serde(default)]
        #[serde_as(as = "Vec<Datetime>")]
        pub deleted_at: Vec<Datetime>,
        pub uuid: Vec<String>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde_with::serde_as]
    pub struct Dmeta {
        pub id: String,
        #[serde(with = "api_utils_rs::codecs::datetime")]
        pub created_at: Datetime,
        #[serde_as(as = "Vec<Datetime>")]
        pub updated_at: Vec<Datetime>,
        pub actors: String,
        pub facet_uuids: Vec<(String, String)>,
        pub facets: Vec<(String, FacetMeta)>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    #[serde(rename_all = "camelCase")]
    pub struct Representation {
        pub digest: Multihash,
        pub length_octets: u64,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    #[serde(rename_all = "camelCase")]
    pub struct CipherBlob {
        pub representation: Representation,
        pub content_encoding: String,
        /// Facet reference URL: `db+facet:///<doc-id|self>/<tag>/<key-id>`.
        pub key_ref: String,
        pub key_ref_heads: Vec<String>,
        /// Scheme-selected inputs, kept as JSON because `contentEncoding` is
        /// what defines their schema.
        pub encoding_parameters: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    #[serde(rename_all = "camelCase")]
    pub struct KnownPlug {
        pub latest: String,
        pub latest_version: String,
        pub latest_rejection: Option<String>,
        pub last_valid: String,
        pub last_valid_version: String,
        pub last_enabled_version: Option<String>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct PlugsConfig {
        pub enabled: Vec<(String, String)>,
        pub known_plugs: Vec<(String, KnownPlug)>,
        pub plug_config_doc_ids: Vec<(String, String)>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct Point {
        pub x: f32,
        pub y: f32,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    #[serde(rename_all = "camelCase")]
    pub struct OcrTextRegion {
        pub bounding_box: Vec<Point>,
        pub text: Option<String>,
        pub confidence_score: Option<f32>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    #[serde(rename_all = "camelCase")]
    pub struct ImageMetadata {
        pub facet_ref: String,
        pub ref_heads: Vec<String>,
        pub mime: MimeType,
        pub width_px: u64,
        pub height_px: u64,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    #[serde(rename_all = "camelCase")]
    pub struct OcrResult {
        pub facet_ref: String,
        pub ref_heads: Vec<String>,
        pub model_tag: String,
        pub text: String,
        pub text_regions: Option<Vec<OcrTextRegion>>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    #[serde(rename_all = "camelCase")]
    pub enum EmbeddingCompression {
        Zstd,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    #[serde(rename_all = "camelCase")]
    pub enum EmbeddingDtype {
        F32,
        F16,
        I8,
        Binary,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    #[serde(rename_all = "camelCase")]
    pub struct Embedding {
        pub facet_ref: String,
        pub ref_heads: Vec<String>,
        pub model_tag: String,
        pub vector: Vec<u8>,
        pub dim: u32,
        pub dtype: EmbeddingDtype,
        pub compression: Option<EmbeddingCompression>,
    }

    pub type DocFacet = String;

    pub fn facet_from(value: &root_doc::FacetRaw) -> DocFacet {
        serde_json::to_string(&value).expect(ERROR_JSON)
    }
    pub fn facet_into(value: &str) -> serde_json::Result<root_doc::FacetRaw> {
        serde_json::from_str(value)
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct DocPatch {
        pub id: DocId,
        pub facets_set: Vec<(String, DocFacet)>,
        pub facets_remove: Vec<String>,
        pub user_path: Option<String>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct DocAddedEvent {
        pub id: DocId,
        pub heads: Vec<String>,
    }

    // --- Conversions Main <-> WIT ---

    impl TryFrom<DocPatch> for root_doc::DocPatch {
        type Error = serde_json::Error;

        fn try_from(val: DocPatch) -> Result<Self, Self::Error> {
            Ok(Self {
                id: val.id,
                facets_set: val
                    .facets_set
                    .into_iter()
                    .map(|(key, val)| Ok((FacetKey::from(&key), facet_into(&val)?)))
                    .collect::<Result<_, _>>()?,
                facets_remove: val
                    .facets_remove
                    .into_iter()
                    .map(|key| FacetKey::from(&key))
                    .collect(),
                user_path: val.user_path.map(root_doc::UserPathBuf::from),
            })
        }
    }
}
