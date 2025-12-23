//! @generated
use super::*;

pub mod doc {
    use super::*;

    pub const TAG: api::Tag = api::Tag {
        name: "doc",
        desc: "Doc mgmt.",
    };

    pub type MimeType = String;

    pub type Multihash = String;

    #[derive(Debug, Clone, PartialEq, utoipa::ToSchema, Serialize, Deserialize)]
    pub struct DocBlob {
        pub length_octets: u64,
        pub hash: Multihash,
    }

    pub type DocId = String;

    #[derive(Debug, Clone, utoipa::ToSchema, Serialize, Deserialize, PartialEq)]
    #[serde(rename_all = "camelCase")]
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


    #[derive(Debug, Clone, utoipa::ToSchema, Serialize, Deserialize, PartialEq)]
    #[serde(rename_all = "camelCase")]
    pub enum DocContent {
        Text(String),
        Blob(DocBlob),
    }

    pub type DocRef = DocId;

    #[derive(Debug, Clone, PartialEq, utoipa::ToSchema, Serialize, Deserialize)]
    pub struct ImageMeta {
        pub mime: MimeType,
        pub width_px: u64,
        pub height_px: u64,
    }

    #[derive(Debug, Clone, utoipa::ToSchema, Serialize, Deserialize, PartialEq)]
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


    #[derive(Debug, Clone, PartialEq, utoipa::ToSchema, Serialize, Deserialize)]
    pub struct DocAddedEvent {
        pub id: DocId,
        pub heads: Vec<String>,
    }

    pub mod doc_create {
        use super::*;

        #[derive(Debug, Clone)]
        pub struct DocCreate;

        pub type Output = SchemaRef<daybook_types::Doc>;

        #[derive(Debug, Clone, utoipa::ToSchema, Serialize, Deserialize)]
        pub struct Input {
            #[schema(min_length = 1, max_length = 1024)]
            pub id: String,
        }

        #[derive(Debug, Clone, thiserror::Error, displaydoc::Display, utoipa::ToSchema, Serialize, Deserialize)]
        /// Id occupied: {id}
        pub struct ErrorIdOccupied {
            pub id: String,
        }

        #[derive(Debug, thiserror::Error, displaydoc::Display, utoipa::ToSchema, macros::HttpError, Serialize, Deserialize)]
        pub enum Error {
            /// Id occupied {0}
            #[http(code(StatusCode::BAD_REQUEST), desc("Id occupied"))]
            IdOccupied(#[from] ErrorIdOccupied),
            /// Invalid input {0}
            #[http(code(StatusCode::BAD_REQUEST), desc("Invalid input"))]
            InvalidInput(#[from] ErrorsValidation),
            /// Internal server error {0}
            #[http(code(StatusCode::INTERNAL_SERVER_ERROR), desc("Internal server error"))]
            Internal(#[from] ErrorInternal),
        }
    }

}
