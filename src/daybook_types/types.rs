//! @generated
use super::*;   

pub mod doc {
    use super::*;

    #[cfg(feature = "automerge")]
    pub type OffsetDateTime = time::OffsetDateTime;
    #[cfg(not(feature = "automerge"))]
    pub type OffsetDateTime = Datetime;

    #[cfg(feature = "utoipa")]
    pub const TAG: api::Tag = api::Tag {
        name: "doc",
        desc: "Doc mgmt.",
    };

    pub type MimeType = String;

    pub type DocId = String;

    #[derive(Debug, Clone)]
    #[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    #[cfg_attr(feature = "serde", serde(rename_all = "camelCase"))]
    #[cfg_attr(feature = "automerge", derive(Reconcile, Hydrate))]
    #[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
    pub struct DocImage {
        pub mime: MimeType,
        pub width_px: u64,
        pub height_px: u64,
        pub blurhash: Option<Mutlihash>,
        pub blob: Mutlihash,
    }

    #[derive(Debug, Clone)]
    #[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    #[cfg_attr(feature = "serde", serde(rename_all = "camelCase"))]
    #[cfg_attr(feature = "automerge", derive(Reconcile, Hydrate))]
    #[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
    pub struct DocBlob {
        pub length_octets: u64,
        pub hash: DocId,
    }

    pub type Mutlihash = String;

    #[derive(Debug, Clone)]
    #[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    #[cfg_attr(feature = "serde", serde(rename_all = "camelCase", untagged))]
    #[cfg_attr(feature = "automerge", derive(Reconcile, Hydrate))]
    #[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
    pub enum DocKind {
        Text,
        Blob,
        Image,
    }

    #[derive(Debug, Clone)]
    #[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    #[cfg_attr(feature = "serde", serde(rename_all = "camelCase", tag = "ty"))]
    #[cfg_attr(feature = "automerge", derive(Reconcile, Hydrate))]
    #[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
    pub enum DocContent {
        Text(String), 
        Blob(DocBlob), 
        Image(DocImage), 
    }

    #[derive(Debug, Clone)]
    #[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    #[cfg_attr(feature = "serde", serde(rename_all = "camelCase", untagged))]
    #[cfg_attr(feature = "automerge", derive(Reconcile, Hydrate))]
    #[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
    pub enum DocTagKind {
        RefGeneric,
        LabelGeneric,
    }

    #[derive(Debug, Clone)]
    #[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    #[cfg_attr(feature = "serde", serde(rename_all = "camelCase", tag = "ty"))]
    #[cfg_attr(feature = "automerge", derive(Reconcile, Hydrate))]
    #[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
    pub enum DocTag {
        /// A link to another document.
        RefGeneric(Mutlihash), 
        LabelGeneric(String), 
    }

    #[derive(Debug, Clone)]
    #[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    #[cfg_attr(feature = "serde", serde(rename_all = "camelCase"))]
    #[cfg_attr(feature = "automerge", derive(Reconcile, Hydrate))]
    #[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
    pub struct Doc {
        pub id: Mutlihash,
        #[cfg_attr(all(feature = "serde", feature = "automerge"), serde(with = "api_utils_rs::codecs::sane_iso8601"))]
        #[cfg_attr(feature = "automerge", autosurgeon(with = "utils_rs::am::autosurgeon_date"))]
        pub created_at: OffsetDateTime,
        #[cfg_attr(all(feature = "serde", feature = "automerge"), serde(with = "api_utils_rs::codecs::sane_iso8601"))]
        #[cfg_attr(feature = "automerge", autosurgeon(with = "utils_rs::am::autosurgeon_date"))]
        pub updated_at: OffsetDateTime,
        pub content: DocKind,
        pub tags: Vec<DocTag>,
    }

    pub mod doc_create {
        use super::*;

        #[derive(Debug, Clone)]
        pub struct DocCreate;


        #[cfg(feature = "utoipa")]
        pub type Output = SchemaRef<Doc>;
        #[cfg(not(feature = "utoipa"))]
        pub type Output = Doc;


        #[derive(Debug, Clone, garde::Validate)]
        #[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
        #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
        #[cfg_attr(feature = "serde", serde(rename_all = "camelCase"))]
        pub struct Input {
            #[cfg_attr(feature = "utoipa", schema(min_length = 1, max_length = 1024))]
            #[garde(length(min = 1, max = 1024))]
            pub id: String,
        }

        #[derive(Debug, Clone, thiserror::Error, displaydoc::Display, utoipa::ToSchema)]
        #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
        #[cfg_attr(feature = "serde", serde(rename_all = "camelCase", tag = "error"))]
        /// Id occupied: {id}
        pub struct ErrorIdOccupied {
            pub id: String,
        }
        #[derive(
            Debug,
            thiserror::Error,
            displaydoc::Display,
        )]
        #[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema, macros::HttpError))]
        #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
        #[cfg_attr(feature = "serde", serde(rename_all = "camelCase", tag = "error"))]
        pub enum Error {
            /// Id occupied {0}
            #[cfg_attr(feature = "utoipa", http(code(StatusCode::BAD_REQUEST), desc("Id occupied")))]
            IdOccupied(#[from] ErrorIdOccupied),
            /// Invalid input {0}
            #[cfg_attr(feature = "utoipa", http(code(StatusCode::BAD_REQUEST), desc("Invalid input")))]
            InvalidInput(#[from] ErrorsValidation),
            /// Internal server error {0}
            #[cfg_attr(feature = "utoipa", http(code(StatusCode::INTERNAL_SERVER_ERROR), desc("Internal server error")))]
            Internal(#[from] ErrorInternal),
        }
    }

}
