pub mod doc {
    use crate::interlude::*;

    // Re-export all generated WIT types from gen::wit::doc
    pub use crate::gen::wit::doc::*;

    use api_utils_rs::wit::townframe::api_utils::utils::Datetime;

    /// Document type for WIT - manually written (excluded from generation)
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Doc {
        pub id: DocId,
        #[serde(with = "api_utils_rs::codecs::datetime")]
        pub created_at: Datetime,
        #[serde(with = "api_utils_rs::codecs::datetime")]
        pub updated_at: Datetime,
        pub content: DocContent,
        pub props: Vec<(String, DocProp)>,
    }

    /// Document patch type for WIT - manually written (excluded from generation)
    /// This patch type is used to update documents. It does not include id, created_at, or updated_at
    /// as those are maintained by the drawer. Props are updated using set/remove operations.
    #[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
    pub struct DocPatch {
        pub id: DocId,
        /// Optional content update
        pub content: Option<DocContent>,
        /// Props to set (insert or update) - list of (key string, doc-prop) tuples
        #[serde(default)]
        #[serde(rename = "propsSet")]
        pub props_set: Vec<(String, DocProp)>,
        /// Props to remove (by key string)
        #[serde(default)]
        #[serde(rename = "propsRemove")]
        pub props_remove: Vec<String>,
    }

    // For conversions, use the feature modules
    use crate::doc as root_doc;
    use crate::wit::doc as wit_doc;

    impl From<crate::doc::DocPatch> for wit_doc::DocPatch {
        fn from(root: crate::doc::DocPatch) -> Self {
            // Convert from root DocPatch to WIT DocPatch
            Self {
                id: root.id,
                content: root.content.map(|c| {
                    // Convert root::DocContent to wit::DocContent using the same pattern as Doc conversion
                    match c {
                        root_doc::DocContent::Text(text) => wit_doc::DocContent::Text(text),
                        root_doc::DocContent::Blob(blob) => {
                            wit_doc::DocContent::Blob(wit_doc::DocBlob {
                                length_octets: blob.length_octets,
                                hash: blob.hash,
                            })
                        }
                    }
                }),
                props_set: root
                    .props_set
                    .into_iter()
                    .map(|kv| {
                        // Serialize DocPropKeys to String for WIT
                        let key_str = serde_json::to_string(&kv.key)
                            .unwrap_or_else(|_| format!("{:?}", kv.key));
                        let wit_prop = match kv.value {
                            root_doc::DocProp::RefGeneric(ref_id) => {
                                wit_doc::DocProp::RefGeneric(ref_id)
                            }
                            root_doc::DocProp::LabelGeneric(label) => {
                                wit_doc::DocProp::LabelGeneric(label)
                            }
                            root_doc::DocProp::ImageMetadata(meta) => {
                                wit_doc::DocProp::ImageMetadata(wit_doc::ImageMeta {
                                    mime: meta.mime,
                                    width_px: meta.width_px,
                                    height_px: meta.height_px,
                                })
                            }
                            root_doc::DocProp::PseudoLabel(labels) => {
                                wit_doc::DocProp::PseudoLabel(labels)
                            }
                            root_doc::DocProp::PathGeneric(path) => {
                                wit_doc::DocProp::PathGeneric(path)
                            }
                            root_doc::DocProp::TitleGeneric(title) => {
                                wit_doc::DocProp::TitleGeneric(title)
                            }
                        };
                        (key_str, wit_prop)
                    })
                    .collect(),
                props_remove: root
                    .props_remove
                    .into_iter()
                    .map(|key| serde_json::to_string(&key).unwrap_or_else(|_| format!("{:?}", key)))
                    .collect(),
            }
        }
    }

    impl From<wit_doc::DocPatch> for crate::doc::DocPatch {
        fn from(wit: wit_doc::DocPatch) -> Self {
            // Convert from WIT DocPatch to root DocPatch
            use crate::doc::DocPropKey;
            Self {
                id: wit.id,
                content: wit.content.map(|c| {
                    // Convert wit::DocContent to root::DocContent using the same pattern as Doc conversion
                    match c {
                        wit_doc::DocContent::Text(text) => root_doc::DocContent::Text(text),
                        wit_doc::DocContent::Blob(blob) => {
                            root_doc::DocContent::Blob(root_doc::DocBlob {
                                length_octets: blob.length_octets,
                                hash: blob.hash,
                            })
                        }
                    }
                }),
                props_set: wit
                    .props_set
                    .into_iter()
                    .map(|(key_str, prop)| {
                        // Deserialize String back to DocPropKeys
                        let key: DocPropKey = serde_json::from_str(&key_str)
                            .unwrap_or_else(|_| DocPropKey::Str(key_str.clone()));
                        let root_prop = match prop {
                            wit_doc::DocProp::RefGeneric(ref_id) => {
                                root_doc::DocProp::RefGeneric(ref_id)
                            }
                            wit_doc::DocProp::LabelGeneric(label) => {
                                root_doc::DocProp::LabelGeneric(label)
                            }
                            wit_doc::DocProp::ImageMetadata(meta) => {
                                root_doc::DocProp::ImageMetadata(root_doc::ImageMeta {
                                    mime: meta.mime,
                                    width_px: meta.width_px,
                                    height_px: meta.height_px,
                                })
                            }
                            wit_doc::DocProp::PseudoLabel(labels) => {
                                root_doc::DocProp::PseudoLabel(labels)
                            }
                            wit_doc::DocProp::PathGeneric(path) => {
                                root_doc::DocProp::PathGeneric(path)
                            }
                            wit_doc::DocProp::TitleGeneric(title) => {
                                root_doc::DocProp::TitleGeneric(title)
                            }
                        };
                        crate::doc::DocPropKeyValue {
                            key,
                            value: root_prop,
                        }
                    })
                    .collect(),
                props_remove: wit
                    .props_remove
                    .into_iter()
                    .map(|key_str| {
                        serde_json::from_str(&key_str).unwrap_or_else(|_| DocPropKey::Str(key_str))
                    })
                    .collect(),
            }
        }
    }

    impl From<crate::doc::Doc> for wit_doc::Doc {
        fn from(root: crate::doc::Doc) -> Self {
            // Convert from root Doc (always available) to WIT Doc
            Self {
                id: root.id,
                created_at: Datetime::from(root.created_at),
                updated_at: Datetime::from(root.updated_at),
                content: match root.content {
                    root_doc::DocContent::Text(text) => wit_doc::DocContent::Text(text),
                    root_doc::DocContent::Blob(blob) => {
                        wit_doc::DocContent::Blob(wit_doc::DocBlob {
                            length_octets: blob.length_octets,
                            hash: blob.hash,
                        })
                    }
                },
                props: root
                    .props
                    .into_iter()
                    .map(|(key, prop)| {
                        // Serialize DocPropKeys to String for WIT
                        let key_str =
                            serde_json::to_string(&key).unwrap_or_else(|_| format!("{:?}", key));
                        let wit_prop = match prop {
                            root_doc::DocProp::RefGeneric(ref_id) => {
                                wit_doc::DocProp::RefGeneric(ref_id)
                            }
                            root_doc::DocProp::LabelGeneric(label) => {
                                wit_doc::DocProp::LabelGeneric(label)
                            }
                            root_doc::DocProp::ImageMetadata(meta) => {
                                wit_doc::DocProp::ImageMetadata(wit_doc::ImageMeta {
                                    mime: meta.mime,
                                    width_px: meta.width_px,
                                    height_px: meta.height_px,
                                })
                            }
                            root_doc::DocProp::PseudoLabel(labels) => {
                                wit_doc::DocProp::PseudoLabel(labels)
                            }
                            root_doc::DocProp::PathGeneric(path) => {
                                wit_doc::DocProp::PathGeneric(path)
                            }
                            root_doc::DocProp::TitleGeneric(title) => {
                                wit_doc::DocProp::TitleGeneric(title)
                            }
                        };
                        (key_str, wit_prop)
                    })
                    .collect(),
            }
        }
    }

    impl From<wit_doc::Doc> for crate::doc::Doc {
        fn from(wit: wit_doc::Doc) -> Self {
            // Convert from WIT Doc to root Doc (always available)
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
                    wit_doc::DocContent::Blob(blob) => {
                        root_doc::DocContent::Blob(root_doc::DocBlob {
                            length_octets: blob.length_octets,
                            hash: blob.hash,
                        })
                    }
                },
                props: {
                    use crate::doc::DocPropKey;
                    wit.props
                        .into_iter()
                        .map(|(key_str, prop)| {
                            // Deserialize String back to DocPropKeys
                            let key: DocPropKey = serde_json::from_str(&key_str)
                                .unwrap_or_else(|_| DocPropKey::Str(key_str.clone()));
                            let root_prop = match prop {
                                wit_doc::DocProp::RefGeneric(ref_id) => {
                                    root_doc::DocProp::RefGeneric(ref_id)
                                }
                                wit_doc::DocProp::LabelGeneric(label) => {
                                    root_doc::DocProp::LabelGeneric(label)
                                }
                                wit_doc::DocProp::ImageMetadata(meta) => {
                                    root_doc::DocProp::ImageMetadata(root_doc::ImageMeta {
                                        mime: meta.mime,
                                        width_px: meta.width_px,
                                        height_px: meta.height_px,
                                    })
                                }
                                wit_doc::DocProp::PseudoLabel(labels) => {
                                    root_doc::DocProp::PseudoLabel(labels)
                                }
                                wit_doc::DocProp::PathGeneric(path) => {
                                    root_doc::DocProp::PathGeneric(path)
                                }
                                wit_doc::DocProp::TitleGeneric(title) => {
                                    root_doc::DocProp::TitleGeneric(title)
                                }
                            };
                            (key, root_prop)
                        })
                        .collect()
                },
            }
        }
    }

    impl From<wit_doc::DocProp> for crate::doc::DocProp {
        fn from(value: wit_doc::DocProp) -> Self {
            match value {
                DocProp::RefGeneric(val) => Self::RefGeneric(val),
                DocProp::LabelGeneric(val) => Self::LabelGeneric(val),
                DocProp::ImageMetadata(val) => Self::ImageMetadata(root_doc::ImageMeta {
                    mime: val.mime,
                    width_px: val.width_px,
                    height_px: val.height_px,
                }),
                DocProp::PseudoLabel(val) => Self::PseudoLabel(val),
                DocProp::PathGeneric(val) => Self::PathGeneric(val),
                DocProp::TitleGeneric(val) => Self::TitleGeneric(val),
            }
        }
    }

    impl From<crate::doc::DocProp> for wit_doc::DocProp {
        fn from(value: crate::doc::DocProp) -> Self {
            use crate::doc::DocProp;
            match value {
                DocProp::RefGeneric(val) => Self::RefGeneric(val),
                DocProp::LabelGeneric(val) => Self::LabelGeneric(val),
                DocProp::ImageMetadata(val) => Self::ImageMetadata(wit_doc::ImageMeta {
                    mime: val.mime,
                    width_px: val.width_px,
                    height_px: val.height_px,
                }),
                DocProp::PseudoLabel(val) => Self::PseudoLabel(val),
                DocProp::PathGeneric(val) => Self::PathGeneric(val),
                DocProp::TitleGeneric(val) => Self::TitleGeneric(val),
            }
        }
    }
}
