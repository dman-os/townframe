pub mod doc {
    use crate::interlude::*;

    use crate::doc as root_doc;
    use api_utils_rs::wit::townframe::api_utils::utils::Datetime;
    pub use root_doc::{
        Blob, DocContent, DocContentKind, DocId, DocPropKey, ImageMetadata, MimeType, Multihash,
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
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub enum DocProp {
        WellKnown(WellKnownProp),
        Any(String), // JSON string
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
                    .map(|(k, v)| (k.to_string(), v.into()))
                    .collect(),
                props_remove: val
                    .props_remove
                    .into_iter()
                    .map(|k| k.to_string())
                    .collect(),
            }
        }
    }

    impl TryFrom<DocPatch> for root_doc::DocPatch {
        type Error = root_doc::DocPropTagParseError;

        fn try_from(val: DocPatch) -> Result<Self, Self::Error> {
            Ok(Self {
                id: val.id,
                props_set: val
                    .props_set
                    .into_iter()
                    .map(|(key, val)| Ok((DocPropKey::from(&key), val.try_into()?)))
                    .collect::<Result<_, _>>()?,
                props_remove: val
                    .props_remove
                    .into_iter()
                    .map(|key| DocPropKey::from(&key))
                    .collect(),
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
            })
        }
    }

    impl From<root_doc::DocProp> for DocProp {
        fn from(val: root_doc::DocProp) -> Self {
            match val {
                root_doc::DocProp::WellKnown(val) => Self::WellKnown(val.into()),
                root_doc::DocProp::Any(val) => Self::Any(val.to_string()),
            }
        }
    }

    impl TryFrom<DocProp> for root_doc::DocProp {
        type Error = root_doc::DocPropTagParseError;

        fn try_from(val: DocProp) -> Result<Self, Self::Error> {
            Ok(match val {
                DocProp::WellKnown(val) => Self::WellKnown(val.try_into()?),
                DocProp::Any(val) => {
                    Self::Any(serde_json::from_str(&val).unwrap_or(serde_json::Value::String(val)))
                }
            })
        }
    }

    impl From<root_doc::Doc> for Doc {
        fn from(val: root_doc::Doc) -> Self {
            Self {
                id: val.id,
                created_at: Datetime::from(val.created_at),
                updated_at: Datetime::from(val.updated_at),
                props: val
                    .props
                    .into_iter()
                    .map(|(key, val)| (key.to_string(), val.into()))
                    .collect(),
            }
        }
    }

    impl TryFrom<Doc> for root_doc::Doc {
        type Error = root_doc::DocPropTagParseError;

        fn try_from(val: Doc) -> Result<Self, Self::Error> {
            Ok(Self {
                id: val.id,
                created_at: time::OffsetDateTime::from_unix_timestamp(
                    val.created_at.seconds as i64,
                )
                .unwrap_or(time::OffsetDateTime::UNIX_EPOCH)
                .replace_nanosecond(val.created_at.nanoseconds)
                .unwrap_or(time::OffsetDateTime::UNIX_EPOCH),
                updated_at: time::OffsetDateTime::from_unix_timestamp(
                    val.updated_at.seconds as i64,
                )
                .unwrap_or(time::OffsetDateTime::UNIX_EPOCH)
                .replace_nanosecond(val.updated_at.nanoseconds)
                .unwrap_or(time::OffsetDateTime::UNIX_EPOCH),
                props: val
                    .props
                    .into_iter()
                    .map(|(key, val)| {
                        Ok((
                            DocPropKey::from(&key),
                            //.unwrap_or(DocPropKey::Tag(root_doc::DocPropTag::Any(key))),
                            val.try_into()?,
                        ))
                    })
                    .collect::<Result<_, _>>()?,
            })
        }
    }
}
