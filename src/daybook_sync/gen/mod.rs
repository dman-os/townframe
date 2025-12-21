//! @generated
use super::*;

pub mod doc {
    use super::*;

    pub type MimeType = String;

    pub type Multihash = String;

    #[derive(Debug, Clone, Hydrate, Reconcile, Serialize, Deserialize)]
    pub struct DocBlob {
        pub length_octets: u64,
        pub hash: Multihash,
    }

    pub type DocId = String;

    #[derive(Debug, Clone, Hydrate, Reconcile, Serialize, Deserialize)]
    pub enum DocContentKind {
        Text,
        Blob,
    }
    impl DocContentKind {
        pub fn _lift(val:u8) -> DocContentKind {
            match val {

                0 => DocContentKind::Text,
                1 => DocContentKind::Blob,

                _ => panic!("invalid enum discriminant"),
            }
        }
    }


    #[derive(Debug, Clone, Hydrate, Reconcile, Serialize, Deserialize)]
    pub enum DocContent {
        Text(String),
        Blob(DocBlob),
    }

    pub type DocRef = DocId;

    #[derive(Debug, Clone, Hydrate, Reconcile, Serialize, Deserialize)]
    pub struct ImageMeta {
        pub mime: MimeType,
        pub width_px: u64,
        pub height_px: u64,
    }

    #[derive(Debug, Clone, Hydrate, Reconcile, Serialize, Deserialize)]
    pub enum DocTagKind {
        RefGeneric,
        LabelGeneric,
        ImageMetadata,
        PseudoLabel,
        PathGeneric,
        TitleGeneric,
    }
    impl DocTagKind {
        pub fn _lift(val:u8) -> DocTagKind {
            match val {

                0 => DocTagKind::RefGeneric,
                1 => DocTagKind::LabelGeneric,
                2 => DocTagKind::ImageMetadata,
                3 => DocTagKind::PseudoLabel,
                4 => DocTagKind::PathGeneric,
                5 => DocTagKind::TitleGeneric,

                _ => panic!("invalid enum discriminant"),
            }
        }
    }


    #[derive(Debug, Clone, Hydrate, Reconcile, Serialize, Deserialize)]
    pub enum DocTag {
        /// A link to another document.
        RefGeneric(DocRef),
        LabelGeneric(String),
        ImageMetadata(ImageMeta),
        PseudoLabel(Vec<String>),
        PathGeneric(String),
        TitleGeneric(String),
    }

    #[derive(Debug, Clone, Hydrate, Reconcile, Serialize, Deserialize)]
    pub struct Doc {
        pub id: DocId,
        #[serde(with = "utils_rs::codecs::sane_iso8601")]
        #[autosurgeon(with = "utils_rs::am::codecs::date")]
        pub created_at: OffsetDateTime,
        #[serde(with = "utils_rs::codecs::sane_iso8601")]
        #[autosurgeon(with = "utils_rs::am::codecs::date")]
        pub updated_at: OffsetDateTime,
        pub content: DocContent,
        pub tags: Vec<DocTag>,
    }

    #[derive(Debug, Clone, Hydrate, Reconcile, Serialize, Deserialize)]
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

        #[derive(Debug, Clone, thiserror::Error, displaydoc::Display, Serialize, Deserialize, Hydrate, Reconcile)]
        /// Id occupied: {id}
        pub struct ErrorIdOccupied {
            pub id: String,
        }

        #[derive(Debug, thiserror::Error, displaydoc::Display, Serialize, Deserialize, Hydrate, Reconcile)]
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
