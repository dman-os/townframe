//! Document types module
//!
//! This module contains the root Doc type and related types.
//! All generated root types are re-exported here to hide the generation details.

use crate::interlude::*;
use std::collections::HashMap;
use uuid::Uuid;

// Re-export all generated root types from gen::root::doc
// These are the public API - generation details are hidden
pub use crate::gen::root::doc::{
    DocAddedEvent, DocBlob, DocContent, DocContentKind, DocId, DocProp, DocRef, ImageMeta,
    MimeType, Multihash,
};

/// Well-known document property keys using reverse domain name scheme
/// This enum is manually written (excluded from generation)
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum WellKnownDocPropKeys {
    #[serde(rename = "org.example.daybook.ref_generic")]
    RefGeneric,
    #[serde(rename = "org.example.daybook.label_generic")]
    LabelGeneric,
    #[serde(rename = "org.example.daybook.image_metadata")]
    ImageMetadata,
    #[serde(rename = "org.example.daybook.pseudo_label")]
    PseudoLabel,
    #[serde(rename = "org.example.daybook.path_generic")]
    PathGeneric,
    #[serde(rename = "org.example.daybook.title_generic")]
    TitleGeneric,
}

/// Document property key - can be a well-known key, a string, or a well-known key with UUID
/// This enum is manually written (excluded from generation)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase", untagged)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum DocPropKey {
    WellKnown(WellKnownDocPropKeys),
    Str(String),
    WellKnownId {
        well_known: WellKnownDocPropKeys,
        id: Uuid,
    },
}

// FIXME: tests for these
impl From<String> for DocPropKey {
    fn from(value: String) -> Self {
        value.as_str().into()
    }
}
impl From<&str> for DocPropKey {
    fn from(key: &str) -> Self {
        let key: DocPropKey = match serde_json::from_str(key) {
            Ok(val) => val,
            Err(err) => {
                panic!("unable to parse string to DocPropKey: {err:?}")
            }
        };
        key
    }
}

impl ToString for DocPropKey {
    fn to_string(&self) -> String {
        let key = serde_json::to_value(self).expect(ERROR_JSON);
        let serde_json::Value::String(key) = key else {
            panic!("DocPropKey doesn't serialize into a string: {key}")
        };
        key
    }
}

/// Key-value pair for props operations (needed because UniFFI doesn't support tuples)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct DocPropKeyValue {
    pub key: DocPropKey,
    pub value: DocProp,
}
impl From<(DocPropKey, DocProp)> for DocPropKeyValue {
    fn from((key, value): (DocPropKey, DocProp)) -> Self {
        Self { key, value }
    }
}

/// Custom serializer/deserializer for HashMap<DocPropKeys, DocProp>
/// Since JSON object keys must be strings, we serialize as a list of tuples
mod props_serde {
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

    pub fn deserialize<'de, D>(deserializer: D) -> Result<HashMap<DocPropKey, DocProp>, D::Error>
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

/// Document type - manually written (excluded from generation)
/// This is the primary type with all derives (serde, uniffi).
/// Use automerge types only at hydrate/reconcile boundaries.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct Doc {
    pub id: DocId,
    #[serde(with = "utils_rs::codecs::sane_iso8601")]
    pub created_at: time::OffsetDateTime,
    #[serde(with = "utils_rs::codecs::sane_iso8601")]
    pub updated_at: time::OffsetDateTime,
    // FIXME: content could just be a well known prop
    pub content: DocContent,
    // FIXME: I'm not sure I like this
    #[serde(with = "props_serde")]
    pub props: HashMap<DocPropKey, DocProp>,
}

/// Document patch - manually written
/// This patch type is used to update documents. It does not include id, created_at, or updated_at
/// as those are maintained by the drawer. Props are updated using set/remove operations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct DocPatch {
    pub id: DocId,
    /// Optional content update
    pub content: Option<DocContent>,
    /// Props to set (insert or update)
    pub props_set: Vec<DocPropKeyValue>,
    /// Props to remove (by key)
    pub props_remove: Vec<DocPropKey>,
}

impl DocPatch {
    /// Check if the patch is empty (no changes)
    pub fn is_empty(&self) -> bool {
        self.content.is_none() && self.props_set.is_empty() && self.props_remove.is_empty()
    }

    /// Apply this patch to a document
    pub fn apply(&self, doc: &mut Doc) {
        if let Some(content) = &self.content {
            doc.content = content.clone();
        }

        // Apply props_set operations
        for kv in &self.props_set {
            doc.props.insert(kv.key.clone(), kv.value.clone());
        }

        // Apply props_remove operations
        for key in &self.props_remove {
            doc.props.remove(key);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChangeHashSet(pub Arc<[automerge::ChangeHash]>);

impl autosurgeon::Hydrate for ChangeHashSet {
    fn hydrate_seq<D: autosurgeon::ReadDoc>(
        doc: &D,
        obj: &automerge::ObjId,
    ) -> Result<Self, autosurgeon::HydrateError> {
        let inner: Arc<[automerge::ChangeHash]> = autosurgeon::Hydrate::hydrate_seq(doc, obj)?;
        Ok(ChangeHashSet(inner))
    }
}

impl autosurgeon::Reconcile for ChangeHashSet {
    type Key<'a> = ();

    fn reconcile<R: autosurgeon::Reconciler>(&self, reconciler: R) -> Result<(), R::Error> {
        autosurgeon::Reconcile::reconcile(&self.0, reconciler)
    }
}

impl std::ops::Deref for ChangeHashSet {
    type Target = [automerge::ChangeHash];

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
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
