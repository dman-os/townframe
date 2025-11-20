//! @generated
use super::*;

pub mod doc {
    use super::*;

    pub type MimeType = String;

    pub type Multihash = String;

    #[derive(Debug, Clone, Hydrate, Reconcile, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct DocImage {
        pub mime: MimeType,
        pub width_px: u64,
        pub height_px: u64,
        pub blurhash: Option<DocId>,
        pub blob: DocId,
    }

    #[derive(Debug, Clone, Hydrate, Reconcile, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct DocBlob {
        pub length_octets: u64,
        pub hash: Multihash,
    }

    pub type DocId = String;

    #[derive(Debug, Clone, Hydrate, Reconcile, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", untagged)]
    pub enum DocKind {
        Text,
        Blob,
        Image,
    }
    impl DocKind {
        pub unsafe fn _lift(val:u8) -> DocKind {
            if !cfg!(debug_assertions){
                return unsafe {
                    ::core::mem::transmute::<u8, DocKind>(val)
                };
            }
            match val {

                0 => DocKind::Text,
                1 => DocKind::Blob,
                2 => DocKind::Image,

                _ => panic!("invalid enum discriminant"),
            }
        }
    }


    #[derive(Debug, Clone, Hydrate, Reconcile, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", untagged)]
    pub enum DocContent {
        Text(String),
        Blob(DocBlob),
        Image(DocImage),
    }

    pub type DocRef = DocId;

    #[derive(Debug, Clone, Hydrate, Reconcile, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", untagged)]
    pub enum DocTagKind {
        RefGeneric,
        LabelGeneric,
    }
    impl DocTagKind {
        pub unsafe fn _lift(val:u8) -> DocTagKind {
            if !cfg!(debug_assertions){
                return unsafe {
                    ::core::mem::transmute::<u8, DocTagKind>(val)
                };
            }
            match val {

                0 => DocTagKind::RefGeneric,
                1 => DocTagKind::LabelGeneric,

                _ => panic!("invalid enum discriminant"),
            }
        }
    }


    #[derive(Debug, Clone, Hydrate, Reconcile, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", untagged)]
    pub enum DocTag {
        /// A link to another document.
        RefGeneric(DocRef),
        LabelGeneric(String),
    }

    #[derive(Debug, Clone, Hydrate, Reconcile, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Doc {
        pub id: DocId,
        #[serde(with = "api_utils_rs::codecs::sane_iso8601")]
        #[autosurgeon(with = "utils_rs::am::codecs::date")]
        pub created_at: OffsetDateTime,
        #[serde(with = "api_utils_rs::codecs::sane_iso8601")]
        #[autosurgeon(with = "utils_rs::am::codecs::date")]
        pub updated_at: OffsetDateTime,
        pub content: DocContent,
        pub tags: Vec<DocTag>,
    }

    #[derive(Debug, Clone, Hydrate, Reconcile, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
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
        #[serde(rename_all = "camelCase")]
        pub struct Input {
            pub id: Uuid,
        }

        #[derive(Debug, Clone, thiserror::Error, displaydoc::Display, Serialize, Deserialize, Hydrate, Reconcile)]
        #[serde(rename_all = "camelCase", tag = "error")]
        /// Id occupied: {id}
        pub struct ErrorIdOccupied {
            pub id: String,
        }

        #[derive(Debug, thiserror::Error, displaydoc::Display, Serialize, Deserialize, Hydrate, Reconcile)]
        #[serde(rename_all = "camelCase", tag = "error")]
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
