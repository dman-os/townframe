pub mod doc {
    use std::collections::HashMap;

    use crate::interlude::*;

    pub use crate::doc::{Blob, DocContent, DocContentKind, DocId, DocPropKey, MimeType, Text};

    crate::define_enum_and_tag!(
        "wk.db",
        // we use the standard keys
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        __WellKnownPropTagAm,
        #[derive(Debug, Clone, Reconcile, Hydrate, PartialEq)]
        WellKnownPropAm {
            // type CreatedAt OffsetDateTime,
            // type UpdatedAt OffsetDateTime,
            RefGeneric type (DocId),
            LabelGeneric type (String),
            PseudoLabel type (String),
            TitleGeneric type (String),
            PathGeneric type (String),
            ImageMetadata type (crate::doc::ImageMetadata),
            Content type (DocContent),
            Pending type (crate::doc::Pending),
        }
    );

    #[derive(Debug, Clone, Reconcile, Hydrate, PartialEq)]
    pub enum DocPropAm {
        WellKnown(WellKnownPropAm),
        Any(utils_rs::am::AutosurgeonJson),
    }

    #[derive(Debug, Clone, Reconcile, Hydrate, PartialEq)]
    pub struct Doc {
        pub id: DocId,
        #[autosurgeon(with = "utils_rs::am::codecs::date")]
        pub created_at: time::OffsetDateTime,
        #[autosurgeon(with = "utils_rs::am::codecs::date")]
        pub updated_at: time::OffsetDateTime,
        #[autosurgeon(with = "autosurgeon::map_with_parseable_keys")]
        pub props: HashMap<DocPropKey, DocPropAm>,
    }

    impl From<crate::doc::WellKnownProp> for WellKnownPropAm {
        fn from(val: crate::doc::WellKnownProp) -> Self {
            match val {
                crate::doc::WellKnownProp::RefGeneric(val) => Self::RefGeneric(val),
                crate::doc::WellKnownProp::LabelGeneric(val) => Self::LabelGeneric(val),
                crate::doc::WellKnownProp::PseudoLabel(val) => Self::PseudoLabel(val),
                crate::doc::WellKnownProp::TitleGeneric(val) => Self::TitleGeneric(val),
                crate::doc::WellKnownProp::PathGeneric(val) => {
                    Self::PathGeneric(val.to_string_lossy().into_owned())
                }
                crate::doc::WellKnownProp::ImageMetadata(val) => Self::ImageMetadata(val),
                crate::doc::WellKnownProp::Content(val) => Self::Content(val),
                crate::doc::WellKnownProp::Pending(val) => Self::Pending(val),
            }
        }
    }

    impl From<WellKnownPropAm> for crate::doc::WellKnownProp {
        fn from(val: WellKnownPropAm) -> Self {
            match val {
                WellKnownPropAm::RefGeneric(val) => Self::RefGeneric(val),
                WellKnownPropAm::LabelGeneric(val) => Self::LabelGeneric(val),
                WellKnownPropAm::PseudoLabel(val) => Self::PseudoLabel(val),
                WellKnownPropAm::TitleGeneric(val) => Self::TitleGeneric(val),
                WellKnownPropAm::PathGeneric(val) => Self::PathGeneric(val.into()),
                WellKnownPropAm::ImageMetadata(val) => Self::ImageMetadata(val),
                WellKnownPropAm::Content(val) => Self::Content(val),
                WellKnownPropAm::Pending(val) => Self::Pending(val),
            }
        }
    }

    impl From<crate::doc::DocProp> for DocPropAm {
        fn from(val: crate::doc::DocProp) -> Self {
            match val {
                crate::doc::DocProp::WellKnown(v) => Self::WellKnown(v.into()),
                crate::doc::DocProp::Any(v) => Self::Any(utils_rs::am::AutosurgeonJson(v)),
            }
        }
    }

    impl From<DocPropAm> for crate::doc::DocProp {
        fn from(val: DocPropAm) -> Self {
            match val {
                DocPropAm::WellKnown(v) => Self::WellKnown(v.into()),
                DocPropAm::Any(v) => Self::Any(v.0),
            }
        }
    }

    impl From<crate::doc::Doc> for Doc {
        fn from(val: crate::doc::Doc) -> Self {
            Self {
                id: val.id,
                created_at: val.created_at,
                updated_at: val.updated_at,
                props: val.props.into_iter().map(|(k, v)| (k, v.into())).collect(),
            }
        }
    }

    impl From<Doc> for crate::doc::Doc {
        fn from(val: Doc) -> Self {
            Self {
                id: val.id,
                created_at: val.created_at,
                updated_at: val.updated_at,
                props: val.props.into_iter().map(|(k, v)| (k, v.into())).collect(),
            }
        }
    }
}
