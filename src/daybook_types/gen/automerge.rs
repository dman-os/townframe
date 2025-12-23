//! @generated
//! Do not edit manually - changes will be overwritten.

use crate::interlude::*;

pub mod doc {
    use super::*;

    pub type MimeType = String;

    pub type Multihash = String;

    #[derive(Debug, Clone, Hydrate, Reconcile, PartialEq)]
    pub struct DocBlob {
        pub length_octets: u64,
        pub hash: Multihash,
    }

    pub type DocId = String;

    #[derive(Debug, Clone, Hydrate, Reconcile, PartialEq)]
    pub enum DocContentKind {
        Text,
        Blob,
    }

    #[derive(Debug, Clone, Hydrate, Reconcile, PartialEq)]
    pub enum DocContent {
        Text(String),
        Blob(DocBlob),
    }

    pub type DocRef = DocId;

    #[derive(Debug, Clone, Hydrate, Reconcile, PartialEq)]
    pub struct ImageMeta {
        pub mime: MimeType,
        pub width_px: u64,
        pub height_px: u64,
    }

    #[derive(Debug, Clone, Hydrate, Reconcile, PartialEq)]
    pub enum DocProp {
        /// A link to another document.
        RefGeneric(DocRef),
        LabelGeneric(String),
        ImageMetadata(ImageMeta),
        PseudoLabel(Vec<String>),
        PathGeneric(String),
        TitleGeneric(String),
    }


    #[derive(Debug, Clone, Hydrate, Reconcile, PartialEq)]
    pub struct DocAddedEvent {
        pub id: DocId,
        pub heads: Vec<String>,
    }
}
