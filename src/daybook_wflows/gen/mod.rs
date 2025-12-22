//! @generated
use super::*;

pub mod doc {
    use super::*;

    pub type MimeType = String;

    pub type Multihash = String;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub struct DocBlob {
        pub length_octets: u64,
        pub hash: Multihash,
    }

    pub type DocId = String;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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


    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub enum DocContent {
        Text(String),
        Blob(DocBlob),
    }

    pub type DocRef = DocId;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub struct ImageMeta {
        pub mime: MimeType,
        pub width_px: u64,
        pub height_px: u64,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub enum DocPropKind {
        RefGeneric,
        LabelGeneric,
        ImageMetadata,
        PseudoLabel,
        PathGeneric,
        TitleGeneric,
    }
    impl DocPropKind {
        pub fn _lift(val:u8) -> DocPropKind {
            match val {

                0 => DocPropKind::RefGeneric,
                1 => DocPropKind::LabelGeneric,
                2 => DocPropKind::ImageMetadata,
                3 => DocPropKind::PseudoLabel,
                4 => DocPropKind::PathGeneric,
                5 => DocPropKind::TitleGeneric,

                _ => panic!("invalid enum discriminant"),
            }
        }
    }


    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub enum DocProp {
        /// A link to another document.
        RefGeneric(DocRef),
        LabelGeneric(String),
        ImageMetadata(ImageMeta),
        PseudoLabel(Vec<String>),
        PathGeneric(String),
        TitleGeneric(String),
    }


    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub struct DocAddedEvent {
        pub id: DocId,
        pub heads: Vec<String>,
    }

    pub mod doc_create {
        use super::*;

        #[derive(Debug, Clone)]
        pub struct DocCreate;

        pub type Output = daybook_types::Doc;

        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub struct Input {
            pub id: String,
        }

        #[derive(Debug, Clone, thiserror::Error, displaydoc::Display, Serialize, Deserialize)]
        /// Id occupied: {id}
        pub struct ErrorIdOccupied {
            pub id: String,
        }

        #[derive(Debug, thiserror::Error, displaydoc::Display, Serialize, Deserialize)]
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
