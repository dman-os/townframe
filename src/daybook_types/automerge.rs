//! Automerge module
//! 
//! This module provides automerge-compatible types with Hydrate/Reconcile derives
//! and From/Into implementations for converting between root and automerge types.

pub use crate::gen::automerge::*;

use autosurgeon::{Hydrate, Reconcile};
use time::OffsetDateTime;

use crate::gen::automerge::doc::*;

/// Document type for automerge - manually written (excluded from generation)
/// This is a minimal boundary type with only Hydrate/Reconcile derives.
/// Use root types (crate::doc::Doc) for most operations.
#[derive(Debug, Clone, Hydrate, Reconcile, PartialEq)]
pub struct Doc {
    pub id: DocId,
    #[autosurgeon(with = "utils_rs::am::codecs::date")]
    pub created_at: OffsetDateTime,
    #[autosurgeon(with = "utils_rs::am::codecs::date")]
    pub updated_at: OffsetDateTime,
    pub content: DocContent,
    pub props: Vec<DocProp>,
}

// From/Into impls between root and automerge types
impl From<crate::doc::Doc> for Doc {
    fn from(root: crate::doc::Doc) -> Self {
        Self {
            id: root.id,
            created_at: root.created_at,
            updated_at: root.updated_at,
            content: root.content.into(),
            props: root.props.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<Doc> for crate::doc::Doc {
    fn from(am: Doc) -> Self {
        Self {
            id: am.id,
            created_at: am.created_at,
            updated_at: am.updated_at,
            content: am.content.into(),
            props: am.props.into_iter().map(Into::into).collect(),
        }
    }
}

// Helper functions for conversions that can be used without importing the types
impl Doc {
    /// Convert from root Doc to automerge Doc
    pub fn to_automerge(root: crate::doc::Doc) -> Self {
        root.into()
    }
    
    /// Convert from automerge Doc to root Doc
    pub fn to_root(self) -> crate::doc::Doc {
        self.into()
    }
}

// Conversions for nested types
use crate::gen::root::doc as root_doc;
use crate::gen::automerge::doc as am_doc;

impl From<root_doc::DocContent> for am_doc::DocContent {
    fn from(root: root_doc::DocContent) -> Self {
        match root {
            root_doc::DocContent::Text(text) => am_doc::DocContent::Text(text),
            root_doc::DocContent::Blob(blob) => am_doc::DocContent::Blob(blob.into()),
        }
    }
}

impl From<am_doc::DocContent> for root_doc::DocContent {
    fn from(am: am_doc::DocContent) -> Self {
        match am {
            am_doc::DocContent::Text(text) => root_doc::DocContent::Text(text),
            am_doc::DocContent::Blob(blob) => root_doc::DocContent::Blob(blob.into()),
        }
    }
}

impl From<root_doc::DocBlob> for am_doc::DocBlob {
    fn from(root: root_doc::DocBlob) -> Self {
        Self {
            length_octets: root.length_octets,
            hash: root.hash,
        }
    }
}

impl From<am_doc::DocBlob> for root_doc::DocBlob {
    fn from(am: am_doc::DocBlob) -> Self {
        Self {
            length_octets: am.length_octets,
            hash: am.hash,
        }
    }
}

impl From<root_doc::DocProp> for am_doc::DocProp {
    fn from(root: root_doc::DocProp) -> Self {
        match root {
            root_doc::DocProp::RefGeneric(ref_id) => am_doc::DocProp::RefGeneric(ref_id),
            root_doc::DocProp::LabelGeneric(label) => am_doc::DocProp::LabelGeneric(label),
            root_doc::DocProp::ImageMetadata(meta) => am_doc::DocProp::ImageMetadata(meta.into()),
            root_doc::DocProp::PseudoLabel(labels) => am_doc::DocProp::PseudoLabel(labels),
            root_doc::DocProp::PathGeneric(path) => am_doc::DocProp::PathGeneric(path),
            root_doc::DocProp::TitleGeneric(title) => am_doc::DocProp::TitleGeneric(title),
        }
    }
}

impl From<am_doc::DocProp> for root_doc::DocProp {
    fn from(am: am_doc::DocProp) -> Self {
        match am {
            am_doc::DocProp::RefGeneric(ref_id) => root_doc::DocProp::RefGeneric(ref_id),
            am_doc::DocProp::LabelGeneric(label) => root_doc::DocProp::LabelGeneric(label),
            am_doc::DocProp::ImageMetadata(meta) => root_doc::DocProp::ImageMetadata(meta.into()),
            am_doc::DocProp::PseudoLabel(labels) => root_doc::DocProp::PseudoLabel(labels),
            am_doc::DocProp::PathGeneric(path) => root_doc::DocProp::PathGeneric(path),
            am_doc::DocProp::TitleGeneric(title) => root_doc::DocProp::TitleGeneric(title),
        }
    }
}

impl From<root_doc::ImageMeta> for am_doc::ImageMeta {
    fn from(root: root_doc::ImageMeta) -> Self {
        Self {
            mime: root.mime,
            width_px: root.width_px,
            height_px: root.height_px,
        }
    }
}

impl From<am_doc::ImageMeta> for root_doc::ImageMeta {
    fn from(am: am_doc::ImageMeta) -> Self {
        Self {
            mime: am.mime,
            width_px: am.width_px,
            height_px: am.height_px,
        }
    }
}
