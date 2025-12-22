//! WIT module
//! 
//! This module provides WIT-compatible types with wit_bindgen support
//! and From/Into implementations for converting between root and WIT types.

pub use crate::gen::wit::*;

use serde::{Deserialize, Serialize};
use api_utils_rs::wit::townframe::api_utils::utils::Datetime;

use crate::gen::wit::doc::*;

/// Document type for WIT - manually written (excluded from generation)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Doc {
    pub id: DocId,
    #[serde(with = "api_utils_rs::codecs::datetime")]
    pub created_at: Datetime,
    #[serde(with = "api_utils_rs::codecs::datetime")]
    pub updated_at: Datetime,
    pub content: DocContent,
    pub props: Vec<DocProp>,
}

// From/Into impls between root and WIT types
// Note: WIT types use Datetime instead of OffsetDateTime, so we need to convert

pub mod conversions {
    use super::*;
    use crate::gen::wit::doc as wit_doc;
    use api_utils_rs::wit::townframe::api_utils::utils::Datetime;
    use time::OffsetDateTime;
    
    impl From<crate::doc::Doc> for super::Doc {
        fn from(root: crate::doc::Doc) -> Self {
            // Convert from root Doc (always available) to WIT Doc
            use crate::gen::root::doc as root_doc;
            Self {
                id: root.id,
                created_at: Datetime::from(root.created_at),
                updated_at: Datetime::from(root.updated_at),
                content: match root.content {
                    root_doc::DocContent::Text(text) => wit_doc::DocContent::Text(text),
                    root_doc::DocContent::Blob(blob) => wit_doc::DocContent::Blob(wit_doc::DocBlob {
                        length_octets: blob.length_octets,
                        hash: blob.hash,
                    }),
                },
                props: root.props.into_iter().map(|prop| {
                    match prop {
                        root_doc::DocProp::RefGeneric(ref_id) => wit_doc::DocProp::RefGeneric(ref_id),
                        root_doc::DocProp::LabelGeneric(label) => wit_doc::DocProp::LabelGeneric(label),
                        root_doc::DocProp::ImageMetadata(meta) => wit_doc::DocProp::ImageMetadata(wit_doc::ImageMeta {
                            mime: meta.mime,
                            width_px: meta.width_px,
                            height_px: meta.height_px,
                        }),
                        root_doc::DocProp::PseudoLabel(labels) => wit_doc::DocProp::PseudoLabel(labels),
                        root_doc::DocProp::PathGeneric(path) => wit_doc::DocProp::PathGeneric(path),
                        root_doc::DocProp::TitleGeneric(title) => wit_doc::DocProp::TitleGeneric(title),
                    }
                }).collect(),
            }
        }
    }
    
    impl From<super::Doc> for crate::doc::Doc {
        fn from(wit: super::Doc) -> Self {
            // Convert from WIT Doc to root Doc (always available)
            use crate::gen::root::doc as root_doc;
            Self {
                id: wit.id,
                created_at: OffsetDateTime::from_unix_timestamp(wit.created_at.seconds as i64)
                    .unwrap_or_else(|_| OffsetDateTime::UNIX_EPOCH)
                    .replace_nanosecond(wit.created_at.nanoseconds)
                    .unwrap_or_else(|_| OffsetDateTime::UNIX_EPOCH),
                updated_at: OffsetDateTime::from_unix_timestamp(wit.updated_at.seconds as i64)
                    .unwrap_or_else(|_| OffsetDateTime::UNIX_EPOCH)
                    .replace_nanosecond(wit.updated_at.nanoseconds)
                    .unwrap_or_else(|_| OffsetDateTime::UNIX_EPOCH),
                content: match wit.content {
                    wit_doc::DocContent::Text(text) => root_doc::DocContent::Text(text),
                    wit_doc::DocContent::Blob(blob) => root_doc::DocContent::Blob(root_doc::DocBlob {
                        length_octets: blob.length_octets,
                        hash: blob.hash,
                    }),
                },
                props: wit.props.into_iter().map(|prop| {
                    match prop {
                        wit_doc::DocProp::RefGeneric(ref_id) => root_doc::DocProp::RefGeneric(ref_id),
                        wit_doc::DocProp::LabelGeneric(label) => root_doc::DocProp::LabelGeneric(label),
                        wit_doc::DocProp::ImageMetadata(meta) => root_doc::DocProp::ImageMetadata(root_doc::ImageMeta {
                            mime: meta.mime,
                            width_px: meta.width_px,
                            height_px: meta.height_px,
                        }),
                        wit_doc::DocProp::PseudoLabel(labels) => root_doc::DocProp::PseudoLabel(labels),
                        wit_doc::DocProp::PathGeneric(path) => root_doc::DocProp::PathGeneric(path),
                        wit_doc::DocProp::TitleGeneric(title) => root_doc::DocProp::TitleGeneric(title),
                    }
                }).collect(),
            }
        }
    }
}
