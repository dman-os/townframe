use crate::interlude::*;

use std::collections::HashMap;

pub type Multihash = String;

crate::define_enum_and_tag!(
    "",
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    #[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
    #[cfg_attr(feature = "automerge", derive(Reconcile, Hydrate))]
    DocContentKind,
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    #[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
    #[cfg_attr(feature = "automerge", derive(Reconcile, Hydrate))]
    #[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
    DocContent {
        // type CreatedAt OffsetDateTime,
        // type UpdatedAt OffsetDateTime,
        Text type (String),
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
        #[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
        #[cfg_attr(feature = "automerge", derive(Reconcile, Hydrate))]
        #[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
        Blob struct {
            pub length_octets: u64,
            pub hash: Multihash,
        },
    }
);

pub type MimeType = String;

crate::define_enum_and_tag!(
    "org.example.daybook",
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    #[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
    #[cfg_attr(feature = "automerge", derive(Reconcile, Hydrate))]
    #[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
    WellKnownPropTag,
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    #[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
    #[serde(rename_all = "camelCase", untagged)]
    #[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
    WellKnownProp {
        // type CreatedAt OffsetDateTime,
        // type UpdatedAt OffsetDateTime,
        RefGeneric type (DocId),
        LabelGeneric type (String),
        PseudoLabel type (String),
        TitleGeneric type (String),
        PathGeneric type (PathBuf),
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
        #[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
        #[cfg_attr(feature = "automerge", derive(Reconcile, Hydrate))]
        #[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
        ImageMetadata struct {
            pub mime: MimeType,
            pub width_px: u64,
            pub height_px: u64,
        },
        Content type (DocContent),
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
        #[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
        #[cfg_attr(feature = "automerge", derive(Reconcile, Hydrate))]
        #[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
        Pending struct {
            pub key: DocPropKey
        },
    }
);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
#[cfg_attr(feature = "automerge", derive(Reconcile, Hydrate))]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum DocPropTag {
    WellKnown(WellKnownPropTag),
    Any(String),
}

impl From<WellKnownPropTag> for DocPropTag {
    fn from(value: WellKnownPropTag) -> Self {
        Self::WellKnown(value)
    }
}

#[derive(Debug, thiserror::Error, displaydoc::Display, PartialEq)]
#[cfg_attr(feature = "automerge", derive(Reconcile, Hydrate))]
pub enum DocPropTagParseError {
    /// A valid key must consist of a reverse domain name notation
    NotDomainName { tag: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
#[cfg_attr(feature = "automerge", derive(Reconcile, Hydrate))]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum DocPropKey {
    Tag(DocPropTag),
    TagAndId { tag: DocPropTag, id: String },
}

impl<T> From<T> for DocPropKey
where
    T: Into<DocPropTag>,
{
    fn from(value: T) -> Self {
        Self::Tag(value.into())
    }
}

impl DocPropKey {
    pub const TAG_ID_SEPARATOR: char = '/';
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", untagged)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum DocProp {
    WellKnown(WellKnownProp),
    Any(serde_json::Value),
}

impl<T> From<T> for DocProp
where
    T: Into<WellKnownProp>,
{
    fn from(value: T) -> Self {
        Self::WellKnown(value.into())
    }
}

pub type DocId = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct Doc {
    pub id: DocId,
    #[serde(with = "utils_rs::codecs::sane_iso8601")]
    pub created_at: time::OffsetDateTime,
    #[serde(with = "utils_rs::codecs::sane_iso8601")]
    pub updated_at: time::OffsetDateTime,
    // FIXME: I'm not sure I like this
    #[serde(with = "ser_de::props_serde")]
    pub props: HashMap<DocPropKey, DocProp>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct DocPatch {
    pub id: DocId,
    /// Props to set (insert or update)
    pub props_set: HashMap<DocPropKey, DocProp>,
    /// Props to remove (by key)
    pub props_remove: Vec<DocPropKey>,
}

impl DocPatch {
    pub fn is_empty(&self) -> bool {
        self.props_set.is_empty() && self.props_remove.is_empty()
    }

    pub fn apply(&mut self, doc: &mut Doc) {
        for (key, value) in self.props_set.drain() {
            doc.props.insert(key, value);
        }
        for key in self.props_remove.drain(..) {
            doc.props.remove(&key);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct DocAddedEvent {
    pub id: DocId,
    pub heads: Vec<String>,
}

#[cfg(feature = "automerge")]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChangeHashSet(pub Arc<[automerge::ChangeHash]>);

#[cfg(feature = "automerge")]
impl autosurgeon::Hydrate for ChangeHashSet {
    fn hydrate_seq<D: autosurgeon::ReadDoc>(
        doc: &D,
        obj: &automerge::ObjId,
    ) -> Result<Self, autosurgeon::HydrateError> {
        let inner: Arc<[automerge::ChangeHash]> = autosurgeon::Hydrate::hydrate_seq(doc, obj)?;
        Ok(ChangeHashSet(inner))
    }
}

#[cfg(feature = "automerge")]
impl autosurgeon::Reconcile for ChangeHashSet {
    type Key<'a> = ();

    fn reconcile<R: autosurgeon::Reconciler>(&self, reconciler: R) -> Result<(), R::Error> {
        autosurgeon::Reconcile::reconcile(&self.0, reconciler)
    }
}

#[cfg(feature = "automerge")]
impl std::ops::Deref for ChangeHashSet {
    type Target = [automerge::ChangeHash];

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

mod ser_de {
    use super::*;

    use std::fmt;
    use std::str::FromStr;

    use serde::{de::Visitor, Deserializer, Serializer};

    impl FromStr for DocPropTag {
        type Err = DocPropTagParseError;

        fn from_str(str: &str) -> Result<Self, Self::Err> {
            if let Some(val) = WellKnownPropTag::from_str(str) {
                return Ok(Self::WellKnown(val));
            }
            let _parsed = addr::parse_domain_name(str)
                .map_err(|_err| DocPropTagParseError::NotDomainName { tag: str.into() })?;
            Ok(Self::Any(_parsed.as_str().into()))
        }
    }

    impl std::fmt::Display for DocPropTag {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                DocPropTag::WellKnown(val) => write!(f, "{val}"),
                DocPropTag::Any(val) => write!(f, "{val}"),
            }
        }
    }

    impl serde::Serialize for DocPropTag {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            serializer.serialize_str(&self.to_string())
        }
    }

    impl<'de> serde::Deserialize<'de> for DocPropTag {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct TagVisitor;

            impl<'de> Visitor<'de> for TagVisitor {
                type Value = DocPropTag;

                fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                    f.write_str("a valid DocPropTag")
                }

                fn visit_str<E>(self, v: &str) -> Result<DocPropTag, E>
                where
                    E: serde::de::Error,
                {
                    v.parse().map_err(E::custom)
                }

                fn visit_borrowed_str<E>(self, v: &'de str) -> Result<DocPropTag, E>
                where
                    E: serde::de::Error,
                {
                    v.parse().map_err(E::custom)
                }
            }

            deserializer.deserialize_str(TagVisitor)
        }
    }

    impl FromStr for DocPropKey {
        type Err = DocPropTagParseError;

        fn from_str(str: &str) -> Result<Self, Self::Err> {
            if let Some((tag, id)) = str.split_once(Self::TAG_ID_SEPARATOR) {
                return Ok(Self::TagAndId {
                    tag: tag.parse()?,
                    id: id.into(),
                });
            }
            str.parse::<DocPropTag>().map(Self::Tag)
        }
    }

    impl std::fmt::Display for DocPropKey {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                DocPropKey::Tag(val) => write!(f, "{val}"),
                DocPropKey::TagAndId { tag, id } => {
                    write!(f, "{tag}{sep}{id}", sep = Self::TAG_ID_SEPARATOR)
                }
            }
        }
    }
    impl serde::Serialize for DocPropKey {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            serializer.serialize_str(&self.to_string())
        }
    }

    impl<'de> serde::Deserialize<'de> for DocPropKey {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct KeyVisitor;

            impl<'de> Visitor<'de> for KeyVisitor {
                type Value = DocPropKey;

                fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                    f.write_str("a valid DocPropKey")
                }

                fn visit_str<E>(self, v: &str) -> Result<DocPropKey, E>
                where
                    E: serde::de::Error,
                {
                    v.parse().map_err(E::custom)
                }

                fn visit_borrowed_str<E>(self, v: &'de str) -> Result<DocPropKey, E>
                where
                    E: serde::de::Error,
                {
                    v.parse().map_err(E::custom)
                }
            }

            deserializer.deserialize_str(KeyVisitor)
        }
    }

    /// Custom serializer/deserializer for HashMap<DocPropKeys, DocProp>
    /// Since JSON object keys must be strings, we serialize as a list of tuples
    pub(super) mod props_serde {
        use super::*;
        use serde::de::{SeqAccess, Visitor};
        use serde::ser::SerializeSeq;
        use serde::{Deserializer, Serializer};

        pub fn serialize<S>(
            props: &HashMap<DocPropKey, DocProp>,
            serializer: S,
        ) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let mut seq = serializer.serialize_seq(Some(props.len()))?;
            for (key, value) in props {
                seq.serialize_element(&(key, value))?;
            }
            seq.end()
        }

        pub fn deserialize<'de, D>(
            deserializer: D,
        ) -> Result<HashMap<DocPropKey, DocProp>, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct PropsVisitor;

            impl<'de> Visitor<'de> for PropsVisitor {
                type Value = HashMap<DocPropKey, DocProp>;

                fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                    formatter.write_str("a sequence of (DocPropKeys, DocProp) tuples")
                }

                fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
                where
                    A: SeqAccess<'de>,
                {
                    let mut map = HashMap::new();
                    while let Some((key, value)) = seq.next_element::<(DocPropKey, DocProp)>()? {
                        map.insert(key, value);
                    }
                    Ok(map)
                }
            }

            deserializer.deserialize_seq(PropsVisitor)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use automerge::transaction::Transactable;
    use autosurgeon::{hydrate_prop, reconcile_prop};

    #[test]
    fn test_change_hash_set_hydrate_seq() {
        let mut doc = automerge::AutoCommit::new();
        let list_id = doc
            .put_object(automerge::ROOT, "heads", automerge::ObjType::List)
            .unwrap();

        // Create some change hashes
        let hash1 = automerge::ChangeHash([1u8; 32]);
        let hash2 = automerge::ChangeHash([2u8; 32]);
        let hash3 = automerge::ChangeHash([3u8; 32]);

        // Insert hashes as bytes (convert [u8; 32] to Vec<u8>)
        doc.insert(&list_id, 0, hash1.0.to_vec()).unwrap();
        doc.insert(&list_id, 1, hash2.0.to_vec()).unwrap();
        doc.insert(&list_id, 2, hash3.0.to_vec()).unwrap();

        let hydrated: ChangeHashSet = hydrate_prop(&doc, automerge::ROOT, "heads").unwrap();
        let heads = hydrated.0.as_ref();

        assert_eq!(heads.len(), 3);
        assert_eq!(heads[0], hash1);
        assert_eq!(heads[1], hash2);
        assert_eq!(heads[2], hash3);
    }

    #[test]
    fn test_change_hash_set_reconcile() {
        let mut doc = automerge::AutoCommit::new();
        let hash1 = automerge::ChangeHash([1u8; 32]);
        let hash2 = automerge::ChangeHash([2u8; 32]);
        let original = ChangeHashSet(Arc::from([hash1, hash2]));

        reconcile_prop(&mut doc, automerge::ROOT, "heads", &original).unwrap();

        let hydrated: ChangeHashSet = hydrate_prop(&doc, automerge::ROOT, "heads").unwrap();
        assert_eq!(hydrated, original);
    }

    #[test]
    fn test_change_hash_set_round_trip() {
        let mut doc = automerge::AutoCommit::new();
        let hashes = vec![
            automerge::ChangeHash([1u8; 32]),
            automerge::ChangeHash([2u8; 32]),
            automerge::ChangeHash([3u8; 32]),
        ];
        let original = ChangeHashSet(Arc::from(hashes.clone()));

        // Reconcile into document
        reconcile_prop(&mut doc, automerge::ROOT, "heads", &original).unwrap();

        // Hydrate back
        let hydrated: ChangeHashSet = hydrate_prop(&doc, automerge::ROOT, "heads").unwrap();
        assert_eq!(hydrated, original);
    }

    #[test]
    fn test_change_hash_set_empty() {
        let mut doc = automerge::AutoCommit::new();
        let original = ChangeHashSet(Arc::from([]));

        reconcile_prop(&mut doc, automerge::ROOT, "heads", &original).unwrap();

        let hydrated: ChangeHashSet = hydrate_prop(&doc, automerge::ROOT, "heads").unwrap();
        assert_eq!(hydrated, original);
    }
}
