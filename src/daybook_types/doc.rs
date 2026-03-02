use crate::interlude::*;

pub type Multihash = String;

pub type MimeType = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct FacetMeta {
    pub created_at: Timestamp,
    pub uuid: Vec<Uuid>,
    // NOTE: field oredring is important for reconcilation order
    pub updated_at: Vec<Timestamp>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, PartialOrd)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Point {
    pub x: f32,
    pub y: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct OcrTextRegion {
    /// Clockwise
    pub bounding_box: Vec<Point>,
    pub text: Option<String>,
    /// Normalized to 0-1
    pub confidence_score: Option<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct PseudoLabelCandidate {
    pub label: String,
    pub prompts: Vec<String>,
    pub negative_prompts: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct PseudoLabelCandidatesFacet {
    pub labels: Vec<PseudoLabelCandidate>,
}

crate::define_enum_and_tag!(
    "org.example.daybook.",
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    #[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
    #[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
    WellKnownFacetTag,
    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
    #[serde(rename_all = "camelCase", untagged)]
    #[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
    WellKnownFacet {
        #[derive(Debug, Clone, Serialize, Deserialize)]
        #[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
        #[serde(rename_all = "camelCase")]
        #[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
        Dmeta struct {
            pub id: DocId,
            // FIXME: unix timestamp codec
            pub created_at: Timestamp,
            pub updated_at: Vec<Timestamp>,
            pub facet_uuids: HashMap<Uuid,FacetKey>,
            pub facets: HashMap<FacetKey, FacetMeta>
        },
        RefGeneric type (DocId),
        LabelGeneric type (String),
        PseudoLabel type (Vec<String>),
        PseudoLabelCandidates type (PseudoLabelCandidatesFacet),
        TitleGeneric type (String),
        PathGeneric type (PathBuf),
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
        #[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
        #[serde(rename_all = "camelCase")]
        #[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
        Pending struct {
            pub key: FacetKey
        },
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
        #[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
        #[serde(rename_all = "camelCase")]
        #[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
        Body struct {
            pub order: Vec<Url>,
        },
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
        #[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
        #[serde(rename_all = "camelCase")]
        #[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
        Note struct {
            pub mime: MimeType,
            pub content: String,
        },
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
        #[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
        #[serde(rename_all = "camelCase")]
        #[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
        Blob struct {
            pub mime: MimeType,
            pub length_octets: u64,
            pub digest: Multihash,
            /// Only to be used for small blobs
            pub inline: Option<Vec<u8>>,
            pub urls: Option<Vec<String>>
        },
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
        #[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
        #[serde(rename_all = "camelCase")]
        #[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
        ImageMetadata struct {
            // URL to src Blob facet
            pub facet_ref: Url,
            pub ref_heads: ChangeHashSet,
            pub mime: MimeType,
            pub width_px: u64,
            pub height_px: u64,
        },
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
        #[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
        #[serde(rename_all = "camelCase")]
        #[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
        OcrResult struct {
            // URL to src ImageMetadata facet
            pub facet_ref: Url,
            pub ref_heads: ChangeHashSet,
            pub model_tag: String,
            pub text: String,
            pub text_regions: Option<Vec<OcrTextRegion>>
        },
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
        #[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
        #[serde(rename_all = "camelCase")]
        #[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
        Embedding struct {
            // URL to src facet like Note or ImageMetadata
            pub facet_ref: Url,
            pub ref_heads: ChangeHashSet,
            pub model_tag: String,
            // FIXME: double check these types
            /// little-endian
            /// check that this does translate to bytes
            #[serde(rename = "vectorBase64", alias = "vector")]
            #[serde(
                serialize_with = "serialize_bytes_as_base64",
                deserialize_with = "deserialize_bytes_from_base64"
            )]
            #[cfg_attr(
                feature = "schemars",
                schemars(schema_with = "base64_string_json_schema")
            )]
            pub vector: Vec<u8>,
            pub dim: u32,
            pub dtype: EmbeddingDtype,
            /// method tag
            pub compression: Option<EmbeddingCompression>,
        }
    }
);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, PartialOrd)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum EmbeddingCompression {
    Zstd,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, PartialOrd)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum EmbeddingDtype {
    F32,
    F16,
    I8,
    Binary,
}

pub fn embedding_f32_bytes_to_json(vector: &[u8], dim: u32) -> Res<String> {
    let expected_len = dim as usize * std::mem::size_of::<f32>();
    if vector.len() != expected_len {
        eyre::bail!(
            "embedding bytes length mismatch: got {}, expected {}",
            vector.len(),
            expected_len
        );
    }
    let values = vector
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .map(|value| value.to_string())
        .collect::<Vec<_>>();
    Ok(format!("[{}]", values.join(",")))
}

pub fn embedding_f32_slice_to_le_bytes(values: &[f32]) -> Vec<u8> {
    values
        .iter()
        .flat_map(|value| value.to_le_bytes())
        .collect::<Vec<u8>>()
}

fn serialize_bytes_as_base64<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&data_encoding::BASE64.encode(bytes))
}

fn deserialize_bytes_from_base64<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum BytesRepr {
        Base64(String),
        Raw(Vec<u8>),
    }

    match BytesRepr::deserialize(deserializer)? {
        BytesRepr::Base64(encoded) => data_encoding::BASE64
            .decode(encoded.as_bytes())
            .or_else(|_| data_encoding::BASE64_NOPAD.decode(encoded.as_bytes()))
            .map_err(serde::de::Error::custom),
        BytesRepr::Raw(bytes) => Ok(bytes),
    }
}

#[cfg(feature = "schemars")]
fn base64_string_json_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
    let mut map = serde_json::Map::new();
    map.insert(
        "type".to_string(),
        serde_json::Value::String("string".into()),
    );
    map.insert(
        "contentEncoding".to_string(),
        serde_json::Value::String("base64".into()),
    );
    map.into()
}

impl<T> From<T> for Note
where
    T: Into<String>,
{
    fn from(value: T) -> Self {
        Note {
            mime: "text/plain".into(),
            content: value.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum FacetTag {
    WellKnown(WellKnownFacetTag),
    Any(String),
}

impl From<WellKnownFacetTag> for FacetTag {
    fn from(value: WellKnownFacetTag) -> Self {
        Self::WellKnown(value)
    }
}

#[derive(Debug, thiserror::Error, displaydoc::Display, PartialEq)]
pub enum FacetTagParseError {
    /// A valid key must consist of a reverse domain name notation
    NotDomainName { _tag: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct FacetKey {
    pub tag: FacetTag,
    pub id: String,
}

impl FacetKey {
    pub const TAG_ID_SEPARATOR: char = '/';
}

pub const DEFAULT_FACET_ID: &str = "main";
// Convention: custom facet key IDs use snake_case (underscores), e.g.
// "daybook_wip_learned_image_label_proposals". Keep this stable for consistency.

impl From<WellKnownFacetTag> for FacetKey {
    fn from(tag: WellKnownFacetTag) -> Self {
        Self {
            tag: tag.into(),
            id: DEFAULT_FACET_ID.into(),
        }
    }
}

impl From<FacetTag> for FacetKey {
    fn from(tag: FacetTag) -> Self {
        Self {
            tag,
            id: DEFAULT_FACET_ID.into(),
        }
    }
}

pub type FacetRaw = serde_json::Value;
pub type ArcFacetRaw = Arc<serde_json::Value>;

// #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
// #[serde(rename_all = "camelCase", untagged)]
// #[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
// #[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
// pub enum Facet {
//     WellKnown(WellKnownFacet),
//     Any(serde_json::Value),
// }
//
// impl<T> From<T> for Facet
// where
//     T: Into<WellKnownFacet>,
// {
//     fn from(value: T) -> Self {
//         Self::WellKnown(value.into())
//     }
// }

pub type DocId = String;

pub type DocUserId = automerge::ActorId;
pub type FacetBlame = HashMap<FacetKey, DocUserId>;

pub type UserPath = std::path::PathBuf;
pub type BranchPath = std::path::PathBuf;

pub mod user_path {
    use super::*;

    pub fn new(device: &str, plug: Option<&str>, routine: Option<&str>) -> UserPath {
        let mut path = PathBuf::from("/");
        path.push(device);
        if let Some(plug) = plug {
            path.push(plug);
            if let Some(routine) = routine {
                path.push(routine);
            }
        }
        path
    }

    pub fn to_actor_id(path: &UserPath) -> automerge::ActorId {
        let path_str = path.to_string_lossy();
        let hash = blake3::hash(path_str.as_bytes());
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&hash.as_bytes()[..16]);
        automerge::ActorId::from(bytes)
    }

    pub fn device(path: &UserPath) -> &str {
        path.components()
            .nth(1)
            .and_then(|component| component.as_os_str().to_str())
            .unwrap_or("")
    }

    pub fn plug(path: &UserPath) -> Option<&str> {
        path.components()
            .nth(2)
            .and_then(|component| component.as_os_str().to_str())
    }

    pub fn routine(path: &UserPath) -> Option<&str> {
        path.components()
            .nth(3)
            .and_then(|component| component.as_os_str().to_str())
    }

    pub fn parse(input: &str) -> Res<UserPath> {
        let path = PathBuf::from(input);
        if !path.as_path().has_root() {
            eyre::bail!("UserPath must start with /");
        }
        Ok(path)
    }
}

pub struct Users {
    pub users: HashMap<String, DocUserId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct Doc {
    pub id: DocId,
    pub facets: HashMap<FacetKey, FacetRaw>,
}

impl Doc {
    /// Calculate the difference between two documents and return a patch.
    /// The patch can be applied to `old` to get `new`.
    pub fn diff(old: &Doc, new: &Doc) -> DocPatch {
        let mut facets_set = HashMap::new();
        let mut facets_remove = Vec::new();

        // Find added or changed properties
        for (key, val) in &new.facets {
            if old.facets.get(key) != Some(val) {
                facets_set.insert(key.clone(), val.clone());
            }
        }

        // Find removed properties
        for key in old.facets.keys() {
            if !new.facets.contains_key(key) {
                facets_remove.push(key.clone());
            }
        }

        DocPatch {
            id: new.id.clone(),
            facets_set,
            facets_remove,
            user_path: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct AddDocArgs {
    pub branch_path: BranchPath,
    pub facets: HashMap<FacetKey, FacetRaw>,
    pub user_path: Option<UserPath>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct DocPatch {
    pub id: DocId,
    /// facets to set (insert or update)
    pub facets_set: HashMap<FacetKey, FacetRaw>,
    /// facets to remove (by key)
    pub facets_remove: Vec<FacetKey>,
    /// Optional user path for recording in drawer
    pub user_path: Option<UserPath>,
}

impl DocPatch {
    pub fn is_empty(&self) -> bool {
        self.facets_set.is_empty() && self.facets_remove.is_empty()
    }

    pub fn apply(&mut self, doc: &mut Doc) {
        for (key, value) in self.facets_set.drain() {
            doc.facets.insert(key, value);
        }
        for key in self.facets_remove.drain(..) {
            doc.facets.remove(&key);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct DocAddedEvent {
    pub id: DocId,
    pub heads: Vec<String>,
}

#[cfg(feature = "automerge")]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct ChangeHashSet(pub Arc<[automerge::ChangeHash]>);

#[cfg(feature = "automerge")]
#[cfg(feature = "schemars")]
impl schemars::JsonSchema for ChangeHashSet {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ChangeHashSet".into()
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::schema_for!(Vec<String>)
    }
}

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

mod ord {
    use super::*;

    impl PartialOrd for FacetTag {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Ord for FacetTag {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            match (self, other) {
                (FacetTag::WellKnown(one), FacetTag::WellKnown(two)) => {
                    one.as_str().cmp(two.as_str())
                }
                (FacetTag::WellKnown(one), FacetTag::Any(two)) => one.as_str().cmp(two.as_str()),
                (FacetTag::Any(one), FacetTag::WellKnown(two)) => one.as_str().cmp(two.as_str()),
                (FacetTag::Any(two), FacetTag::Any(one)) => one.as_str().cmp(two.as_str()),
            }
        }
    }

    impl PartialOrd for FacetKey {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Ord for FacetKey {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            match self.tag.cmp(&other.tag) {
                // id always comes afer none ided keys
                std::cmp::Ordering::Equal => self.id.cmp(&other.id),
                ord => ord,
            }
        }
    }
}

mod ser_de {
    use super::*;

    use std::{borrow::Cow, fmt};

    use serde::{de::Visitor, Deserializer, Serializer};

    // impl FromStr for FacetTag {
    //     type Err = FacetTagParseError;
    //
    //     fn from_str(str: &str) -> Result<Self, Self::Err> {
    //         if let Some(val) = WellKnownFacetTag::from_str(str) {
    //             return Ok(Self::WellKnown(val));
    //         }
    //         let _parsed = addr::parse_domain_name(str)
    //             .map_err(|_err| FacetTagParseError::NotDomainName { tag: str.into() })?;
    //         Ok(Self::Any(_parsed.as_str().into()))
    //     }
    // }
    impl<'a, T> From<T> for FacetTag
    where
        T: Into<Cow<'a, str>>,
    {
        fn from(value: T) -> Self {
            let value = value.into();
            if let Some(val) = WellKnownFacetTag::from_str(&value[..]) {
                return Self::WellKnown(val);
            }
            // let _parsed = addr::parse_domain_name(str)
            //     .map_err(|_err| FacetTagParseError::NotDomainName { tag: str.into() })?;
            // Ok(Self::Any(_parsed.as_str().into()))
            Self::Any(value.into())
        }
    }
    // impl From<&str> for FacetTag {
    //     fn from(value: &str) -> Self {
    //         if let Some(val) = WellKnownFacetTag::from_str(&value[..]) {
    //             return Self::WellKnown(val);
    //         }
    //         // let _parsed = addr::parse_domain_name(str)
    //         //     .map_err(|_err| FacetTagParseError::NotDomainName { tag: str.into() })?;
    //         // Ok(Self::Any(_parsed.as_str().into()))
    //         Self::Any(value.into())
    //     }
    // }
    // impl From<String> for FacetTag {
    //     fn from(value: String) -> Self {
    //         if let Some(val) = WellKnownFacetTag::from_str(&value[..]) {
    //             return Self::WellKnown(val);
    //         }
    //         Self::Any(value.into())
    //     }
    // }
    // impl From<Box<str>> for FacetTag {
    //     fn from(value: Box<str>) -> Self {
    //         if let Some(val) = WellKnownFacetTag::from_str(&value[..]) {
    //             return Self::WellKnown(val);
    //         }
    //         Self::Any(value.into())
    //     }
    // }

    impl std::fmt::Display for FacetTag {
        fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                FacetTag::WellKnown(val) => write!(fmt, "{val}"),
                FacetTag::Any(val) => write!(fmt, "{val}"),
            }
        }
    }

    impl serde::Serialize for FacetTag {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            serializer.serialize_str(&self.to_string())
        }
    }

    impl<'de> serde::Deserialize<'de> for FacetTag {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct TagVisitor;

            impl<'de> Visitor<'de> for TagVisitor {
                type Value = FacetTag;

                fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
                    fmt.write_str("a valid FacetTag")
                }

                fn visit_str<E>(self, val: &str) -> Result<FacetTag, E>
                where
                    E: serde::de::Error,
                {
                    Ok(val.into())
                    // val.parse().map_err(E::custom)
                }

                fn visit_borrowed_str<E>(self, val: &'de str) -> Result<FacetTag, E>
                where
                    E: serde::de::Error,
                {
                    Ok(val.into())
                    // val.parse().map_err(E::custom)
                }
            }

            deserializer.deserialize_str(TagVisitor)
        }
    }

    // impl FromStr for FacetKey {
    //     type Err = FacetTagParseError;
    //
    //     fn from_str(str: &str) -> Result<Self, Self::Err> {
    //         if let Some((tag, id)) = str.split_once(Self::TAG_ID_SEPARATOR) {
    //             return Ok(Self::TagAndId {
    //                 tag: tag.parse()?,
    //                 id: id.into(),
    //             });
    //         }
    //         str.parse::<FacetTag>().map(Self::Tag)
    //     }
    // }
    impl<'a, T> From<T> for FacetKey
    where
        T: Into<Cow<'a, str>>,
    {
        fn from(value: T) -> Self {
            let value = value.into();
            if let Some((tag, id)) = value.split_once(Self::TAG_ID_SEPARATOR) {
                return Self {
                    tag: tag.into(),
                    id: id.into(),
                };
            }
            let tag: FacetTag = value.into();
            tag.into()
        }
    }
    // impl From<&str> for FacetKey {
    //     fn from(value: &str) -> Self {
    //         if let Some((tag, id)) = value.split_once(Self::TAG_ID_SEPARATOR) {
    //             return Self::TagAndId {
    //                 tag: tag.into(),
    //                 id: id.into(),
    //             };
    //         }
    //         let tag: FacetTag = value.into();
    //         Self::Tag(tag)
    //     }
    // }
    //
    // impl From<String> for FacetKey {
    //     fn from(value: String) -> Self {
    //         if let Some((tag, id)) = value.split_once(Self::TAG_ID_SEPARATOR) {
    //             return Self::TagAndId {
    //                 tag: tag.into(),
    //                 id: id.into(),
    //             };
    //         }
    //         let tag: FacetTag = value.into();
    //         Self::Tag(tag)
    //     }
    // }
    // impl From<&Arc<str>> for FacetKey {
    //     fn from(value: &Arc<str>) -> Self {
    //         if let Some((tag, id)) = value.split_once(Self::TAG_ID_SEPARATOR) {
    //             return Self::TagAndId {
    //                 tag: tag.into(),
    //                 id: id.into(),
    //             };
    //         }
    //         let tag: FacetTag = (&value[..]).into();
    //         Self::Tag(tag)
    //     }
    // }
    //
    // impl From<Box<str>> for FacetKey {
    //     fn from(value: Box<str>) -> Self {
    //         if let Some((tag, id)) = value.split_once(Self::TAG_ID_SEPARATOR) {
    //             return Self::TagAndId {
    //                 tag: tag.into(),
    //                 id: id.into(),
    //             };
    //         }
    //         let tag: FacetTag = value.into();
    //         Self::Tag(tag)
    //     }
    // }

    impl std::fmt::Display for FacetKey {
        fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let FacetKey { tag, id } = self;
            write!(fmt, "{tag}{sep}{id}", sep = Self::TAG_ID_SEPARATOR)
        }
    }
    impl serde::Serialize for FacetKey {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            serializer.serialize_str(&self.to_string())
        }
    }

    impl<'de> serde::Deserialize<'de> for FacetKey {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct KeyVisitor;

            impl<'de> Visitor<'de> for KeyVisitor {
                type Value = FacetKey;

                fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
                    fmt.write_str("a valid FacetKey")
                }

                fn visit_str<E>(self, val: &str) -> Result<FacetKey, E>
                where
                    E: serde::de::Error,
                {
                    Ok(val.into())
                    // val.parse().map_err(E::custom)
                }

                fn visit_borrowed_str<E>(self, val: &'de str) -> Result<FacetKey, E>
                where
                    E: serde::de::Error,
                {
                    Ok(val.into())
                    // val.parse().map_err(E::custom)
                }
            }

            deserializer.deserialize_str(KeyVisitor)
        }
    }

    // Custom serializer/deserializer for HashMap<FacetKeys, Facet>
    // pub(super) mod facets_serde {
    //     use super::*;
    //     use serde::de::{SeqAccess, Visitor};
    //     use serde::ser::SerializeSeq;
    //     use serde::{Deserializer, Serializer};
    //
    //     pub fn serialize<S>(
    //         facets: &HashMap<FacetKey, Facet>,
    //         serializer: S,
    //     ) -> Result<S::Ok, S::Error>
    //     where
    //         S: Serializer,
    //     {
    //         let mut seq = serializer.serialize_seq(Some(facets.len()))?;
    //         for (key, value) in facets {
    //             seq.serialize_element(&(key, value))?;
    //         }
    //         seq.end()
    //     }
    //
    //     pub fn deserialize<'de, D>(
    //         deserializer: D,
    //     ) -> Result<HashMap<FacetKey, Facet>, D::Error>
    //     where
    //         D: Deserializer<'de>,
    //     {
    //         struct facetsVisitor;
    //
    //         impl<'de> Visitor<'de> for facetsVisitor {
    //             type Value = HashMap<FacetKey, Facet>;
    //
    //             fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
    //                 formatter.write_str("a sequence of (FacetKeys, Facet) tuples")
    //             }
    //
    //             fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    //             where
    //                 A: SeqAccess<'de>,
    //             {
    //                 let mut map = HashMap::new();
    //                 while let Some((key, value)) = seq.next_element::<(FacetKey, Facet)>()? {
    //                     map.insert(key, value);
    //                 }
    //                 Ok(map)
    //             }
    //         }
    //
    //         deserializer.deserialize_seq(facetsVisitor)
    //     }
    // }
    impl From<WellKnownFacet> for FacetRaw {
        fn from(value: WellKnownFacet) -> Self {
            serde_json::to_value(value).expect(ERROR_JSON)
        }
    }

    impl WellKnownFacet {
        pub fn from_json(value: serde_json::Value, tag: WellKnownFacetTag) -> Res<Self> {
            Ok(match tag {
                WellKnownFacetTag::RefGeneric => Self::RefGeneric(
                    serde_json::from_value(value)
                        .wrap_err_with(|| format!("error parsing json as {tag} value"))?,
                ),
                WellKnownFacetTag::LabelGeneric => Self::LabelGeneric(
                    serde_json::from_value(value)
                        .wrap_err_with(|| format!("error parsing json as {tag} value"))?,
                ),
                WellKnownFacetTag::PseudoLabel => Self::PseudoLabel(
                    serde_json::from_value(value)
                        .wrap_err_with(|| format!("error parsing json as {tag} value"))?,
                ),
                WellKnownFacetTag::PseudoLabelCandidates => Self::PseudoLabelCandidates(
                    serde_json::from_value(value)
                        .wrap_err_with(|| format!("error parsing json as {tag} value"))?,
                ),
                WellKnownFacetTag::TitleGeneric => Self::TitleGeneric(
                    serde_json::from_value(value)
                        .wrap_err_with(|| format!("error parsing json as {tag} value"))?,
                ),
                WellKnownFacetTag::PathGeneric => Self::PathGeneric(
                    serde_json::from_value(value)
                        .wrap_err_with(|| format!("error parsing json as {tag} value"))?,
                ),
                WellKnownFacetTag::ImageMetadata => Self::ImageMetadata(
                    serde_json::from_value(value)
                        .wrap_err_with(|| format!("error parsing json as {tag} value"))?,
                ),
                WellKnownFacetTag::OcrResult => Self::OcrResult(
                    serde_json::from_value(value)
                        .wrap_err_with(|| format!("error parsing json as {tag} value"))?,
                ),
                WellKnownFacetTag::Embedding => Self::Embedding(
                    serde_json::from_value(value)
                        .wrap_err_with(|| format!("error parsing json as {tag} value"))?,
                ),
                WellKnownFacetTag::Pending => Self::Pending(
                    serde_json::from_value(value)
                        .wrap_err_with(|| format!("error parsing json as {tag} value"))?,
                ),
                WellKnownFacetTag::Body => Self::Body(
                    serde_json::from_value(value)
                        .wrap_err_with(|| format!("error parsing json as {tag} value"))?,
                ),
                WellKnownFacetTag::Dmeta => Self::Dmeta(
                    serde_json::from_value(value)
                        .wrap_err_with(|| format!("error parsing json as {tag} value"))?,
                ),
                WellKnownFacetTag::Note => Self::Note(
                    serde_json::from_value(value)
                        .wrap_err_with(|| format!("error parsing json as {tag} value"))?,
                ),
                WellKnownFacetTag::Blob => Self::Blob(
                    serde_json::from_value(value)
                        .wrap_err_with(|| format!("error parsing json as {tag} value"))?,
                ),
            })
        }
    }

    struct EfficientChangeHash(automerge::ChangeHash);

    impl Serialize for EfficientChangeHash {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            if serializer.is_human_readable() {
                utils_rs::hash::encode_base58_multibase(self.0 .0).serialize(serializer)
            } else {
                serializer.serialize_bytes(&self.0 .0)
            }
        }
    }

    impl<'de> serde::Deserialize<'de> for EfficientChangeHash {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            if deserializer.is_human_readable() {
                let str = String::deserialize(deserializer)?;
                let mut buf = [0u8; 32];
                utils_rs::hash::decode_base58_multibase_onto(&str, &mut buf)
                    .map_err(serde::de::Error::custom)?;
                Ok(Self(automerge::ChangeHash(buf)))
            } else {
                struct MyVisitor;
                impl<'de> serde::de::Visitor<'de> for MyVisitor {
                    type Value = [u8; 32];

                    fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
                        fmt.write_str("a 32 length byte string")
                    }

                    fn visit_bytes<E>(self, val: &[u8]) -> Result<Self::Value, E>
                    where
                        E: serde::de::Error,
                    {
                        if val.len() != 32 {
                            return Err(serde::de::Error::invalid_length(
                                val.len(),
                                &"32 length byte array",
                            ));
                        }
                        let mut buf = [0u8; 32];
                        buf.copy_from_slice(val);
                        Ok(buf)
                    }
                }
                deserializer
                    .deserialize_str(MyVisitor)
                    .map(|buf| Self(automerge::ChangeHash(buf)))
                //deserializer.deserialize_bytes(&self.0 .0)
            }
        }
    }

    impl Serialize for ChangeHashSet {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            use serde::ser::SerializeSeq;
            let mut seq = serializer.serialize_seq(Some(self.len()))?;
            for hash in &self.0[..] {
                seq.serialize_element(&EfficientChangeHash(*hash))?;
            }
            seq.end()
        }
    }

    impl<'de> serde::Deserialize<'de> for ChangeHashSet {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            // FIXME: optimze for non-human readable formats
            let set = <Vec<String>>::deserialize(deserializer)?;
            Ok(Self(
                am_utils_rs::parse_commit_heads(&set).map_err(serde::de::Error::custom)?,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use automerge::transaction::Transactable;
    use autosurgeon::{hydrate_prop, reconcile_prop};

    #[test]
    fn test_doc_diff() {
        let mut old = Doc {
            id: "doc1".into(),
            facets: HashMap::new(),
        };
        let tag_title = FacetTag::WellKnown(WellKnownFacetTag::TitleGeneric);
        let tag_label = FacetTag::WellKnown(WellKnownFacetTag::LabelGeneric);

        old.facets.insert(
            tag_title.clone().into(),
            WellKnownFacet::TitleGeneric("Old Title".into()).into(),
        );
        old.facets.insert(
            tag_label.clone().into(),
            WellKnownFacet::LabelGeneric("Label".into()).into(),
        );

        let mut new = old.clone();
        // Update Title
        new.facets.insert(
            tag_title.clone().into(),
            WellKnownFacet::TitleGeneric("New Title".into()).into(),
        );
        // Remove Label
        new.facets.remove(&tag_label.clone().into());
        // Add Path
        let tag_path = FacetTag::WellKnown(WellKnownFacetTag::PathGeneric);
        new.facets.insert(
            tag_path.clone().into(),
            WellKnownFacet::PathGeneric(std::path::PathBuf::from("/tmp")).into(),
        );

        let patch = Doc::diff(&old, &new);

        assert_eq!(patch.id, "doc1");
        // Check facets_set
        assert_eq!(patch.facets_set.len(), 2);
        assert!(patch.facets_set.contains_key(&tag_title.into()));
        assert!(patch.facets_set.contains_key(&tag_path.into()));

        // Check facets_remove
        assert_eq!(patch.facets_remove.len(), 1);
        assert_eq!(patch.facets_remove[0], tag_label.into());

        // Test no changes
        let patch_none = Doc::diff(&new, &new);
        assert!(patch_none.is_empty());
    }

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
