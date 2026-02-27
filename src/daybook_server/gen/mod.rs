//! @generated
use super::*;

pub mod doc {
    use super::*;

    pub type MimeType = String;

    pub type Multihash = String;

    #[derive(Debug, Clone, Hydrate, Reconcile, PartialEq, Serialize, Deserialize)]
    pub struct DocBlob {
        pub length_octets: u64,
        pub hash: Multihash,
    }

    pub type DocId = String;

    #[derive(Debug, Clone, Hydrate, Reconcile, Serialize, Deserialize, PartialEq)]
    #[serde(rename_all = "camelCase")]
    pub enum DocContentKind {
        Text,
        Blob,
    }

    #[derive(Debug, Clone, Hydrate, Reconcile, Serialize, Deserialize, PartialEq)]
    #[serde(rename_all = "camelCase")]
    pub enum DocContent {
        Text(String),
        Blob(DocBlob),
    }

    pub type DocRef = DocId;

    #[derive(Debug, Clone, Hydrate, Reconcile, PartialEq, Serialize, Deserialize)]
    pub struct ImageMeta {
        pub mime: MimeType,
        pub width_px: u64,
        pub height_px: u64,
    }

    #[derive(Debug, Clone, Hydrate, Reconcile, Serialize, Deserialize, PartialEq)]
    #[serde(rename_all = "camelCase")]
    pub enum DocProp {
        /// A link to another document.
        RefGeneric(DocRef),
        LabelGeneric(String),
        ImageMetadata(ImageMeta),
        PseudoLabel(Vec<String>),
        PathGeneric(String),
        TitleGeneric(String),
    }

    #[derive(Debug, Clone, Hydrate, Reconcile, PartialEq, Serialize, Deserialize)]
    pub struct Doc {
        pub id: DocId,
        #[autosurgeon(with = "am_utils_rs::codecs::date")]
        pub created_at: Timestamp,
        #[autosurgeon(with = "am_utils_rs::codecs::date")]
        pub updated_at: Timestamp,
        pub content: DocContent,
        pub props: Vec<DocProp>,
    }

    #[derive(Debug, Clone, Hydrate, Reconcile, PartialEq, Serialize, Deserialize)]
    pub struct DocAddedEvent {
        pub id: DocId,
        pub heads: Vec<String>,
    }

    pub mod doc_create {
        use super::*;

        #[derive(Debug, Clone)]
        pub struct DocCreate;

        pub type Output = Doc;

        #[derive(Debug, Clone, Hydrate, Reconcile, Serialize, Deserialize)]
        pub struct Input {
            pub id: Uuid,
        }

        #[derive(
            Debug,
            Clone,
            thiserror::Error,
            displaydoc::Display,
            Serialize,
            Deserialize,
            Hydrate,
            Reconcile,
        )]
        /// Id occupied: {id}
        pub struct ErrorIdOccupied {
            pub id: String,
        }

        #[derive(
            Debug, thiserror::Error, displaydoc::Display, Serialize, Deserialize, Hydrate, Reconcile,
        )]
        pub enum Error {
            /// Id occupied {0}
            IdOccupied(#[from] ErrorIdOccupied),
            /// Invalid input {0}
            InvalidInput(#[from] ErrorsValidation),
            /// Internal server error {0}
            Internal(#[from] ErrorInternal),
        }
    }
}
