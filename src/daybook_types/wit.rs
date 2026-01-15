pub mod doc {
    use crate::interlude::*;

    use crate::doc as root_doc;
    use api_utils_rs::wit::townframe::api_utils::utils::Datetime;
    pub use root_doc::{
        Blob, DocContent, DocContentKind, DocId, DocPropKey, ImageMetadata, MimeType, Multihash,
        UserPath,
    };

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Pending {
        pub key: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum WellKnownProp {
        RefGeneric(DocId),
        LabelGeneric(String),
        PseudoLabel(String),
        TitleGeneric(String),
        PathGeneric(String),
        ImageMetadata(ImageMetadata),
        Content(DocContent),
        Pending(Pending),
        // UpdatedAt(Datetime),
        // CreatedAt(Datetime),
    }

    pub type DocProp = String;

    pub fn doc_prop_from(value: &root_doc::DocProp) -> DocProp {
        serde_json::to_string(&value).expect(ERROR_JSON)
    }
    pub fn doc_prop_into(value: &str) -> serde_json::Result<root_doc::DocProp> {
        serde_json::from_str(value)
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Doc {
        pub id: DocId,
        #[serde(with = "api_utils_rs::codecs::datetime")]
        pub created_at: Datetime,
        #[serde(with = "api_utils_rs::codecs::datetime")]
        pub updated_at: Datetime,
        pub props: Vec<(String, DocProp)>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct DocPatch {
        pub id: DocId,
        pub props_set: Vec<(String, DocProp)>,
        pub props_remove: Vec<String>,
        pub user_path: Option<String>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct DocAddedEvent {
        pub id: DocId,
        pub heads: Vec<String>,
    }

    // --- Conversions Main <-> WIT ---

    impl From<root_doc::DocPatch> for DocPatch {
        fn from(val: root_doc::DocPatch) -> Self {
            Self {
                id: val.id,
                props_set: val
                    .props_set
                    .into_iter()
                    .map(|(key, val)| (key.to_string(), doc_prop_from(&val)))
                    .collect(),
                props_remove: val
                    .props_remove
                    .into_iter()
                    .map(|key| key.to_string())
                    .collect(),
                user_path: val.user_path.map(|path| path.to_string_lossy().to_string()),
            }
        }
    }

    impl TryFrom<DocPatch> for root_doc::DocPatch {
        type Error = serde_json::Error;

        fn try_from(val: DocPatch) -> Result<Self, Self::Error> {
            Ok(Self {
                id: val.id,
                props_set: val
                    .props_set
                    .into_iter()
                    .map(|(key, val)| Ok((DocPropKey::from(&key), doc_prop_into(&val)?)))
                    .collect::<Result<_, _>>()?,
                props_remove: val
                    .props_remove
                    .into_iter()
                    .map(|key| DocPropKey::from(&key))
                    .collect(),
                user_path: val.user_path.map(root_doc::UserPath::from),
            })
        }
    }

    impl From<root_doc::WellKnownProp> for WellKnownProp {
        fn from(val: root_doc::WellKnownProp) -> Self {
            match val {
                root_doc::WellKnownProp::RefGeneric(val) => Self::RefGeneric(val),
                root_doc::WellKnownProp::LabelGeneric(val) => Self::LabelGeneric(val),
                root_doc::WellKnownProp::PseudoLabel(val) => Self::PseudoLabel(val),
                root_doc::WellKnownProp::TitleGeneric(val) => Self::TitleGeneric(val),
                root_doc::WellKnownProp::PathGeneric(val) => {
                    Self::PathGeneric(val.to_string_lossy().into_owned())
                }
                root_doc::WellKnownProp::ImageMetadata(val) => Self::ImageMetadata(val),
                root_doc::WellKnownProp::Content(val) => Self::Content(val),
                root_doc::WellKnownProp::Pending(pending) => Self::Pending(Pending {
                    key: pending.key.to_string(),
                }),
                // root_doc::WellKnownProp::CreatedAt(timestamp) => Self::CreatedAt(timestamp.into()),
                // root_doc::WellKnownProp::UpdatedAt(timestamp) => Self::UpdatedAt(timestamp.into()),
            }
        }
    }

    impl TryFrom<WellKnownProp> for root_doc::WellKnownProp {
        type Error = root_doc::DocPropTagParseError;

        fn try_from(val: WellKnownProp) -> Result<Self, Self::Error> {
            Ok(match val {
                WellKnownProp::RefGeneric(val) => Self::RefGeneric(val),
                WellKnownProp::LabelGeneric(val) => Self::LabelGeneric(val),
                WellKnownProp::PseudoLabel(val) => Self::PseudoLabel(val),
                WellKnownProp::TitleGeneric(val) => Self::TitleGeneric(val),
                WellKnownProp::PathGeneric(val) => Self::PathGeneric(val.into()),
                WellKnownProp::ImageMetadata(val) => Self::ImageMetadata(val),
                WellKnownProp::Content(val) => Self::Content(val),
                WellKnownProp::Pending(val) => Self::Pending(crate::doc::Pending {
                    key: val.key.into(),
                }),
                // WellKnownProp::CreatedAt(datetime) => Self::CreatedAt(datetime.into()),
                // WellKnownProp::UpdatedAt(datetime) => Self::CreatedAt(datetime.into()),
            })
        }
    }

    impl From<root_doc::Doc> for Doc {
        fn from(
            root_doc::Doc {
                id,
                created_at,
                updated_at,
                props,
            }: root_doc::Doc,
        ) -> Self {
            Self {
                id,
                created_at: Datetime::from(created_at),
                updated_at: Datetime::from(updated_at),
                props: props
                    .into_iter()
                    .map(|(key, val)| (key.to_string(), doc_prop_from(&val)))
                    .collect(),
            }
        }
    }

    impl TryFrom<Doc> for root_doc::Doc {
        type Error = serde_json::Error;

        fn try_from(val: Doc) -> Result<Self, Self::Error> {
            Ok(Self {
                id: val.id,
                created_at: val.created_at.into(),
                updated_at: val.updated_at.into(),
                props: val
                    .props
                    .into_iter()
                    .map(|(key, val)| Ok((DocPropKey::from(&key), doc_prop_into(&val)?)))
                    .collect::<Result<_, _>>()?,
            })
        }
    }
}
