pub mod doc {
    use crate::interlude::*;

    use crate::doc as root_doc;
    use api_utils_rs::wit::townframe::api_utils::utils::Datetime;
    pub use root_doc::{Blob, DocId, FacetKey, ImageMetadata, MimeType, Multihash, Note, UserPath};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Pending {
        pub key: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde_with::serde_as]
    pub struct FacetMeta {
        #[serde(with = "api_utils_rs::codecs::datetime")]
        pub created_at: Datetime,
        #[serde_as(as = "Vec<Datetime>")]
        pub updated_at: Vec<Datetime>,
        pub uuid: Vec<String>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde_with::serde_as]
    pub struct Dmeta {
        pub id: String,
        #[serde(with = "api_utils_rs::codecs::datetime")]
        pub created_at: Datetime,
        #[serde_as(as = "Vec<Datetime>")]
        pub updated_at: Vec<Datetime>,
        pub facet_uuids: Vec<(String, String)>,
        pub facets: Vec<(String, FacetMeta)>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum WellKnownFacet {
        RefGeneric(DocId),
        LabelGeneric(String),
        PseudoLabel(Vec<String>),
        TitleGeneric(String),
        PathGeneric(String),
        ImageMetadata(ImageMetadata),
        Pending(Pending),
        Dmeta(Dmeta),
        Note(Note),
        Blob(Blob),
    }

    pub type DocFacet = String;

    pub fn facet_from(value: &root_doc::FacetRaw) -> DocFacet {
        serde_json::to_string(&value).expect(ERROR_JSON)
    }
    pub fn facet_into(value: &str) -> serde_json::Result<root_doc::FacetRaw> {
        serde_json::from_str(value)
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Doc {
        pub id: DocId,
        pub facets: Vec<(String, DocFacet)>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct DocPatch {
        pub id: DocId,
        pub facets_set: Vec<(String, DocFacet)>,
        pub facets_remove: Vec<String>,
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
                facets_set: val
                    .facets_set
                    .into_iter()
                    .map(|(key, val)| (key.to_string(), facet_from(&val)))
                    .collect(),
                facets_remove: val
                    .facets_remove
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
                facets_set: val
                    .facets_set
                    .into_iter()
                    .map(|(key, val)| Ok((FacetKey::from(&key), facet_into(&val)?)))
                    .collect::<Result<_, _>>()?,
                facets_remove: val
                    .facets_remove
                    .into_iter()
                    .map(|key| FacetKey::from(&key))
                    .collect(),
                user_path: val.user_path.map(root_doc::UserPath::from),
            })
        }
    }

    impl From<root_doc::WellKnownFacet> for WellKnownFacet {
        fn from(val: root_doc::WellKnownFacet) -> Self {
            match val {
                root_doc::WellKnownFacet::RefGeneric(val) => Self::RefGeneric(val),
                root_doc::WellKnownFacet::LabelGeneric(val) => Self::LabelGeneric(val),
                root_doc::WellKnownFacet::PseudoLabel(val) => Self::PseudoLabel(val),
                root_doc::WellKnownFacet::TitleGeneric(val) => Self::TitleGeneric(val),
                root_doc::WellKnownFacet::PathGeneric(val) => {
                    Self::PathGeneric(val.to_string_lossy().into_owned())
                }
                root_doc::WellKnownFacet::ImageMetadata(val) => Self::ImageMetadata(val),
                root_doc::WellKnownFacet::Pending(pending) => Self::Pending(Pending {
                    key: pending.key.to_string(),
                }),
                root_doc::WellKnownFacet::Dmeta(dmeta) => Self::Dmeta(Dmeta {
                    id: dmeta.id,
                    created_at: dmeta.created_at.into(),
                    updated_at: dmeta.updated_at.into_iter().map(Into::into).collect(),
                    facet_uuids: dmeta
                        .facet_uuids
                        .into_iter()
                        .map(|(uuid, key)| (uuid.to_string(), key.to_string()))
                        .collect(),
                    facets: dmeta
                        .facets
                        .into_iter()
                        .map(|(key, meta)| {
                            (
                                key.to_string(),
                                FacetMeta {
                                    created_at: meta.created_at.into(),
                                    updated_at: meta
                                        .updated_at
                                        .into_iter()
                                        .map(Into::into)
                                        .collect(),
                                    uuid: meta.uuid.into_iter().map(|id| id.to_string()).collect(),
                                },
                            )
                        })
                        .collect(),
                }),
                root_doc::WellKnownFacet::Note(note) => Self::Note(Note {
                    mime: note.mime,
                    content: note.content,
                }),
                root_doc::WellKnownFacet::Blob(blob) => Self::Blob(blob),
            }
        }
    }

    impl TryFrom<WellKnownFacet> for root_doc::WellKnownFacet {
        type Error = uuid::Error;

        fn try_from(val: WellKnownFacet) -> Result<Self, Self::Error> {
            Ok(match val {
                WellKnownFacet::RefGeneric(val) => Self::RefGeneric(val),
                WellKnownFacet::LabelGeneric(val) => Self::LabelGeneric(val),
                WellKnownFacet::PseudoLabel(val) => Self::PseudoLabel(val),
                WellKnownFacet::TitleGeneric(val) => Self::TitleGeneric(val),
                WellKnownFacet::PathGeneric(val) => Self::PathGeneric(val.into()),
                WellKnownFacet::ImageMetadata(val) => Self::ImageMetadata(val),
                WellKnownFacet::Pending(val) => Self::Pending(crate::doc::Pending {
                    key: val.key.into(),
                }),
                WellKnownFacet::Dmeta(dmeta) => Self::Dmeta(root_doc::Dmeta {
                    id: dmeta.id,
                    created_at: dmeta.created_at.into(),
                    updated_at: dmeta.updated_at.into_iter().map(Into::into).collect(),
                    facet_uuids: dmeta
                        .facet_uuids
                        .into_iter()
                        .map(|(key, uuid)| Ok((uuid.parse()?, FacetKey::from(&key))))
                        .collect::<Result<_, uuid::Error>>()?,
                    facets: dmeta
                        .facets
                        .into_iter()
                        .map(|(key, meta)| {
                            Ok((
                                FacetKey::from(&key),
                                root_doc::FacetMeta {
                                    uuid: meta
                                        .uuid
                                        .into_iter()
                                        .map(|uuid| uuid.parse())
                                        .collect::<Result<_, _>>()?,
                                    created_at: meta.created_at.into(),
                                    updated_at: meta
                                        .updated_at
                                        .into_iter()
                                        .map(Into::into)
                                        .collect(),
                                },
                            ))
                        })
                        .collect::<Result<_, uuid::Error>>()?,
                }),
                WellKnownFacet::Note(note) => Self::Note(root_doc::Note {
                    mime: note.mime,
                    content: note.content,
                }),
                WellKnownFacet::Blob(blob) => Self::Blob(blob),
            })
        }
    }

    impl From<root_doc::Doc> for Doc {
        fn from(root_doc::Doc { id, facets }: root_doc::Doc) -> Self {
            Self {
                id,
                facets: facets
                    .into_iter()
                    .map(|(key, val)| (key.to_string(), facet_from(&val)))
                    .collect(),
            }
        }
    }

    impl TryFrom<Doc> for root_doc::Doc {
        type Error = serde_json::Error;

        fn try_from(val: Doc) -> Result<Self, Self::Error> {
            Ok(Self {
                id: val.id,
                facets: val
                    .facets
                    .into_iter()
                    .map(|(key, val)| Ok((FacetKey::from(&key), facet_into(&val)?)))
                    .collect::<Result<_, _>>()?,
            })
        }
    }
}
