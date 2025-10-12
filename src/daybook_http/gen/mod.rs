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

    #[derive(Debug, Clone, utoipa::ToSchema, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct DocImage {
        pub mime: MimeType,
        pub width_px: u64,
        pub height_px: u64,
        pub blurhash: Option<DocId>,
        pub blob: DocId,
    }

    #[derive(Debug, Clone, utoipa::ToSchema, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct DocBlob {
        pub length_octets: u64,
        pub hash: Multihash,
    }

    pub type DocId = String;

    #[derive(Debug, Clone, utoipa::ToSchema, Serialize, Deserialize)]
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
                    ::core::mem::transmute(val)
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


    #[derive(Debug, Clone, utoipa::ToSchema, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", tag = "ty")]
    pub enum DocContent {
        Text(String), 
        Blob(DocBlob), 
        Image(DocImage), 
    }

    pub type DocRef = DocId;

    #[derive(Debug, Clone, utoipa::ToSchema, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", untagged)]
    pub enum DocTagKind {
        RefGeneric,
        LabelGeneric,
    }
    impl DocTagKind {
        pub unsafe fn _lift(val:u8) -> DocTagKind {
            if !cfg!(debug_assertions){
                return unsafe {
                    ::core::mem::transmute(val)
                };
            }
            match val {

                0 => DocTagKind::RefGeneric,
                1 => DocTagKind::LabelGeneric,

                _ => panic!("invalid enum discriminant"),
            }
        }
    }


    #[derive(Debug, Clone, utoipa::ToSchema, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", tag = "ty")]
    pub enum DocTag {
        /// A link to another document.
        RefGeneric(DocRef), 
        LabelGeneric(String), 
    }

    #[derive(Debug, Clone, utoipa::ToSchema, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Doc {
        pub id: DocId,
        pub created_at: Datetime,
        pub updated_at: Datetime,
        pub content: DocContent,
        pub tags: Vec<DocTag>,
    }

    pub mod doc_create {
        use super::*;

        #[derive(Debug, Clone)]
        pub struct DocCreate;

        pub type Output = SchemaRef<Doc>;

        #[derive(Debug, Clone, utoipa::ToSchema, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase")]
        pub struct Input {
            #[schema(min_length = 1, max_length = 1024)]
            pub id: String,
        }

        #[derive(Debug, Clone, thiserror::Error, displaydoc::Display, utoipa::ToSchema, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase", tag = "error")]
        /// Id occupied: {id}
        pub struct ErrorIdOccupied {
            pub id: String,
        }

        #[derive(Debug, thiserror::Error, displaydoc::Display, utoipa::ToSchema, macros::HttpError, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase", tag = "error")]
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
