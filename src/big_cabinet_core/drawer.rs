//! The drawer datums: a drawer is a doc carrying `org.example.daybook.drawer/main`, and a
//! single-group drawer lists what it holds as roster entries on that same doc (ADR 019).
//!
//! These are core declarations like dmeta — Cabinet writes them, so their sidecar is the codec's
//! authority by construction and an embedder cannot shadow them. The entry datums are keyed by the
//! id of the thing they list (`docEntry/<docId>`, `childEntry/<drawerId>`), the same pattern the
//! branch datum uses: the key slot is the target, so an entry needs no reference field.

pub mod protocol;

use serde::Deserialize;
use serde::Serialize;
use std::fmt;
use std::str::FromStr;

use crate::datum::DatumMember;
use crate::dmeta::DocId;

/// The descriptor: `org.example.daybook.drawer/main` on the doc whose id is the `DrawerId`.
#[macros::datum(authority = "org.example.daybook")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Drawer {
    pub layout: DrawerLayout,
    #[serde(flatten)]
    pub extra: crate::datum::Extra,
}

impl Drawer {
    /// The groups this descriptor states, for the layouts that have them.
    pub fn groups(&self) -> Option<DrawerGroups> {
        match &self.layout {
            DrawerLayout::SelfDoc(_) | DrawerLayout::Unknown(_) => None,
            DrawerLayout::SingleGroup(groups) => Some(DrawerGroups {
                meta_group: groups.meta_group,
                docs_group: groups.docs_group,
                sensitive_group: groups.sensitive_group,
            }),
        }
    }

    /// The sensitive metadata root a grouped layout names, `None` for the layouts without one.
    /// The descriptor and this doc are both public discovery facts; only the root's contents
    /// are withheld from a Relay principal.
    pub fn sensitive_root(&self) -> Option<DocId> {
        match &self.layout {
            DrawerLayout::SingleGroup(groups) => Some(groups.sensitive_root.clone()),
            DrawerLayout::SelfDoc(_) | DrawerLayout::Unknown(_) => None,
        }
    }

    pub fn self_doc() -> Self {
        Self {
            layout: DrawerLayout::SelfDoc(SelfDocLayout {
                empty: Some(true),
                extra: Default::default(),
            }),
            extra: Default::default(),
        }
    }

    pub fn single_group(
        meta_group: GroupKey,
        docs_group: GroupKey,
        sensitive_group: GroupKey,
        sensitive_root: DocId,
    ) -> Self {
        Self {
            layout: DrawerLayout::SingleGroup(SingleGroupLayout {
                meta_group,
                docs_group,
                sensitive_group,
                sensitive_root,
                extra: Default::default(),
            }),
            extra: Default::default(),
        }
    }
}

/// How the drawer is laid out. Internally tagged like every published union (ADR 021 §5): the
/// member object carries `$type` beside its own fields. Open (docs/wiki/docs.md §Evolvability):
/// a layout this build does not know is carried untouched — reading, storing and listing a
/// drawer never requires operating it. Only an operation that must interpret the layout
/// refuses, via [`DrawerLayout::known`] returning `None`. Adding a member is a new payload
/// type under the same datum id, not a new datum tag.
#[macros::datum]
#[derive(Debug, Clone, PartialEq)]
pub enum DrawerLayout {
    SelfDoc(SelfDocLayout),
    SingleGroup(SingleGroupLayout),
    #[datum(other)]
    Unknown(crate::datum::UnknownMember),
}

impl DrawerLayout {
    /// The layout if it is one this build operates.
    pub fn known(&self) -> Option<LayoutKind> {
        match self {
            Self::SelfDoc(_) => Some(LayoutKind::SelfDoc),
            Self::SingleGroup(_) => Some(LayoutKind::SingleGroup),
            Self::Unknown(_) => None,
        }
    }

    /// The member's `$type` tag, known or carried.
    pub fn tag(&self) -> Option<&str> {
        match self {
            Self::SelfDoc(_) => Some(SelfDocLayout::ID),
            Self::SingleGroup(_) => Some(SingleGroupLayout::ID),
            Self::Unknown(member) => member.id("$type"),
        }
    }
}

/// The known layouts as a closed service vocabulary: the union is open so unknown members
/// survive a round-trip, but the service refuses what it cannot operate. Serde spells it by the
/// member's own `$type` tag, so the wire form is the union's spelling whether the value was
/// read from a descriptor or answered by the service.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LayoutKind {
    SelfDoc,
    SingleGroup,
}

impl LayoutKind {
    pub const ALL: [LayoutKind; 2] = [Self::SelfDoc, Self::SingleGroup];

    /// The `$type` tag the member layout declares.
    pub fn tag(self) -> &'static str {
        match self {
            Self::SelfDoc => SelfDocLayout::ID,
            Self::SingleGroup => SingleGroupLayout::ID,
        }
    }

    pub fn has_groups(self) -> bool {
        match self {
            Self::SelfDoc => false,
            Self::SingleGroup => true,
        }
    }
}

impl fmt::Display for LayoutKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.tag())
    }
}

#[derive(Debug, thiserror::Error, displaydoc::Display)]
/// `{0}` is not a drawer layout tag this build operates
pub struct UnknownLayoutTag(pub String);

impl FromStr for LayoutKind {
    type Err = UnknownLayoutTag;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|layout| layout.tag() == value)
            .ok_or_else(|| UnknownLayoutTag(value.to_owned()))
    }
}

impl Serialize for LayoutKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.tag())
    }
}

impl<'de> Deserialize<'de> for LayoutKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

/// `org.example.daybook.drawer.selfDoc/main` (as a union member, the id is the `$type`):
/// `empty: true` is the convention for a member with nothing to say — an object is required, the
/// property keeps it honest, and later fields land beside it without a new tag. The value is
/// const: a member with `empty: false` is no longer empty, so it is not this member.
#[macros::datum(authority = Drawer::ID, name = "selfDoc")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SelfDocLayout {
    #[cfg_attr(feature = "schemars", schemars(with = "bool"))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub empty: Option<bool>,
    #[serde(flatten)]
    pub extra: crate::datum::Extra,
}

#[macros::datum(authority = Drawer::ID, name = "singleGroup")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SingleGroupLayout {
    /// Authority over the descriptor, which is the public metadata root, and its roster.
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub meta_group: GroupKey,
    /// The collection group included docs delegate access to.
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub docs_group: GroupKey,
    /// Authority over the sensitive metadata root: its membership is withheld from a Relay
    /// principal, so plaintext inventories named there reach only Read+.
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub sensitive_group: GroupKey,
    /// The sensitive metadata root doc. Its existence and id are public discovery facts; its
    /// contents are not. It carries no descriptor, roster, doc entries or child entries.
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub sensitive_root: DocId,
    #[serde(flatten)]
    pub extra: crate::datum::Extra,
}

/// The three Keyhive groups a grouped layout names, as service surfaces carry them.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DrawerGroups {
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub meta_group: GroupKey,
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub docs_group: GroupKey,
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub sensitive_group: GroupKey,
}

/// Which metadata root a `metaDoc` reference is published on, which decides the group whose
/// delegation admits it: the descriptor-bearing doc (the public metadata root) or the separate
/// sensitive metadata root the descriptor names.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MetaRoot {
    Public,
    Sensitive,
}

impl MetaRoot {
    pub const ALL: [MetaRoot; 2] = [Self::Public, Self::Sensitive];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Sensitive => "sensitive",
        }
    }
}

impl fmt::Display for MetaRoot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, thiserror::Error, displaydoc::Display)]
/// `{0}` is not a metadata root (public or sensitive)
pub struct UnknownMetaRoot(pub String);

impl FromStr for MetaRoot {
    type Err = UnknownMetaRoot;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|root| root.as_str() == value)
            .ok_or_else(|| UnknownMetaRoot(value.to_owned()))
    }
}

/// `org.example.daybook.drawer.roster/main`: marks the canonical roster on the drawer doc.
/// Today every entry is a datum on the drawer doc itself, so the marker has nothing to say:
/// it carries the `empty: true` convention, and the fields a future roster layout needs
/// (external docs, ranges, shard routing — ADR 019 sharding design) land beside it as optional
/// fields without a new datum tag. A separate one-member layout vocabulary would state no fact.
#[macros::datum(authority = Drawer::ID, name = "roster")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DrawerRoster {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub empty: Option<bool>,
    #[serde(flatten)]
    pub extra: crate::datum::Extra,
}

impl DrawerRoster {
    pub fn inline() -> Self {
        Self {
            empty: Some(true),
            extra: Default::default(),
        }
    }
}

/// `org.example.daybook.drawer.docEntry/<docId>`: the doc is officially in the drawer, and the
/// Keyhive grant the drawer's docs group was asked for on it. The roster is what makes inclusion
/// official; the grant is what makes it available, and the two can disagree while an operation
/// is in flight.
#[macros::datum(authority = Drawer::ID, name = "docEntry")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DrawerDocEntry {
    pub access: EntryAccess,
    #[serde(flatten)]
    pub extra: crate::datum::Extra,
}

impl DrawerDocEntry {
    pub fn new(access: DrawerAccess) -> Self {
        Self {
            access: access.into(),
            extra: Default::default(),
        }
    }
}

/// `org.example.daybook.drawer.childEntry/<drawerId>`: the child drawer is officially part of
/// this one, and the access the parent's groups were delegated on the child's groups. Recorded
/// in the parent only; a child does not list its parents.
#[macros::datum(authority = Drawer::ID, name = "childEntry")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DrawerChildEntry {
    pub access: EntryAccess,
    #[serde(flatten)]
    pub extra: crate::datum::Extra,
}

impl DrawerChildEntry {
    pub fn new(access: DrawerAccess) -> Self {
        Self {
            access: access.into(),
            extra: Default::default(),
        }
    }
}

/// `org.example.daybook.drawer.metaDoc/<kind>/<name>`: a named slot on one metadata root
/// naming a metadata-class doc that belongs to the drawer — an inventory, a keys document, a
/// future shard. `kind` is an open string vocabulary and `name` picks one slot inside it, both
/// carried in the key slot; concurrent creators of one `(kind, name)` write the same key, so
/// Automerge's last-writer-wins leaves exactly one doc named
/// ([ADR 019](../../../docs/adrs/019-cabinet-drawers-and-selective-mirroring.md),
/// [ADR 026](../../../docs/adrs/026-drawer-native-blobs.md)).
#[macros::datum(authority = Drawer::ID, name = "metaDoc")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DrawerMetaDoc {
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub doc_id: DocId,
    #[serde(flatten)]
    pub extra: crate::datum::Extra,
}

impl DrawerMetaDoc {
    pub fn new(doc_id: DocId) -> Self {
        Self {
            doc_id,
            extra: Default::default(),
        }
    }
}

/// `org.example.daybook.drawer.knownEntry/<drawerId>`: this node explicitly remembers the
/// drawer, and what it intends to do about it — not a roster entry. Remembering grants no
/// access, mirroring is intent rather than fetched bytes, and observing a tag never writes
/// one of these; only the drawer service's remember operation does.
#[macros::datum(authority = Drawer::ID, name = "knownEntry")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KnownDrawerEntry {
    /// The node wants this drawer's content fetched and retained. Absent means it does not;
    /// explicit `false` means it did and stopped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mirror: Option<bool>,
    #[serde(flatten)]
    pub extra: crate::datum::Extra,
}

/// `org.example.daybook.drawer.successor/main`: this drawer has been replaced by another.
/// Fixed singleton on the descriptor-bearing doc — the public metadata root of a grouped
/// drawer, the content doc of a self-doc drawer. Resolution returns only the new id: no
/// grant, no roster or child copy, no blob move, no silent mirror-policy retarget.
#[macros::datum(authority = Drawer::ID, name = "successor")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DrawerSuccessor {
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub drawer_id: DocId,
    #[serde(flatten)]
    pub extra: crate::datum::Extra,
}

/// An access level as a roster entry datum spells it: open (`knownValues`), so an entry written by
/// a peer that knows a future access level is carried untouched on the document rather than
/// rejected by the schema or making the roster unreadable. Only [`Self::known`] turns it into a
/// [`DrawerAccess`] that this build orders and grants; an unknown value never grants or orders.
#[macros::datum(open_string)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EntryAccess {
    #[serde(rename = "Relay")]
    Relay,
    #[serde(rename = "Read")]
    Read,
    #[serde(rename = "Write")]
    Write,
    #[serde(rename = "Admin")]
    Admin,
    #[datum(other)]
    Unknown(String),
}

impl EntryAccess {
    pub fn known(&self) -> Option<DrawerAccess> {
        match self {
            Self::Relay => Some(DrawerAccess::Relay),
            Self::Read => Some(DrawerAccess::Read),
            Self::Write => Some(DrawerAccess::Write),
            Self::Admin => Some(DrawerAccess::Admin),
            Self::Unknown(_) => None,
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Relay => "Relay",
            Self::Read => "Read",
            Self::Write => "Write",
            Self::Admin => "Admin",
            Self::Unknown(value) => value.as_str(),
        }
    }
}

impl From<DrawerAccess> for EntryAccess {
    fn from(access: DrawerAccess) -> Self {
        match access {
            DrawerAccess::Relay => Self::Relay,
            DrawerAccess::Read => Self::Read,
            DrawerAccess::Write => Self::Write,
            DrawerAccess::Admin => Self::Admin,
        }
    }
}

/// Keyhive's access vocabulary as service operations grant and order it. A closed vocabulary of
/// strings: a value outside it is not an access level this node can grant or order, so service
/// requests refuse it while [`EntryAccess`] carries it on roster datums. Ordered weakest to
/// strongest, which is the order Keyhive's `Access` has too.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum DrawerAccess {
    Relay,
    Read,
    Write,
    Admin,
}

impl DrawerAccess {
    pub const ALL: [DrawerAccess; 4] = [Self::Relay, Self::Read, Self::Write, Self::Admin];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Relay => "Relay",
            Self::Read => "Read",
            Self::Write => "Write",
            Self::Admin => "Admin",
        }
    }
}

impl fmt::Display for DrawerAccess {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, thiserror::Error, displaydoc::Display)]
/// `{0}` is not a drawer access level (Relay, Read, Write or Admin)
pub struct UnknownDrawerAccess(pub String);

impl FromStr for DrawerAccess {
    type Err = UnknownDrawerAccess;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|access| access.as_str() == value)
            .ok_or_else(|| UnknownDrawerAccess(value.to_owned()))
    }
}

/// A Keyhive group id as a drawer datum spells it: the 32 identifier bytes as multibase base58btc,
/// the same text form every other 32-byte key in this tree has. Core does not depend on
/// `keyhive_core`, so the conversion to a `GroupId` lives with the service that holds a Keyhive.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupKey(pub [u8; 32]);

/// The text form `Display` writes.
#[cfg(feature = "schemars")]
impl schemars::JsonSchema for GroupKey {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "GroupKey".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::GroupKey").into()
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({ "type": "string" })
    }
}

impl fmt::Display for GroupKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&utils_rs::hash::encode_base58_multibase(self.0))
    }
}

impl fmt::Debug for GroupKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "GroupKey({self})")
    }
}

#[derive(Debug, thiserror::Error, displaydoc::Display)]
/// `{0}` is not a group key: expected 32 bytes as multibase base58btc
pub struct InvalidGroupKey(pub String);

impl FromStr for GroupKey {
    type Err = InvalidGroupKey;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty() {
            return Err(InvalidGroupKey(value.to_owned()));
        }
        let bytes = utils_rs::hash::decode_base58_multibase(value)
            .map_err(|_| InvalidGroupKey(value.to_owned()))?;
        let bytes: [u8; 32] = bytes
            .try_into()
            .map_err(|_| InvalidGroupKey(value.to_owned()))?;
        Ok(Self(bytes))
    }
}

impl Serialize for GroupKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for GroupKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn group(byte: u8) -> GroupKey {
        GroupKey([byte; 32])
    }

    #[test]
    fn a_single_group_descriptor_is_an_internally_tagged_member() {
        let root = DocId::new([9; 32]);
        let drawer = Drawer::single_group(group(1), group(2), group(3), root.clone());
        let value = serde_json::to_value(&drawer).unwrap();
        assert_eq!(
            value,
            json!({
                "layout": {
                    "$type": SingleGroupLayout::ID,
                    "metaGroup": group(1).to_string(),
                    "docsGroup": group(2).to_string(),
                    "sensitiveGroup": group(3).to_string(),
                    "sensitiveRoot": root.to_string(),
                }
            }),
        );
        assert_eq!(serde_json::from_value::<Drawer>(value).unwrap(), drawer);
        let groups = drawer.groups().expect("a grouped drawer states its groups");
        assert_eq!(groups.sensitive_group, group(3));
        assert_eq!(drawer.sensitive_root(), Some(root));
        assert_eq!(Drawer::self_doc().sensitive_root(), None);
        assert_eq!(Drawer::self_doc().groups(), None);
    }

    #[test]
    fn metadata_roots_and_references_carry_no_domain_classification() {
        for root in MetaRoot::ALL {
            assert_eq!(root.to_string().parse::<MetaRoot>().unwrap(), root);
            assert_eq!(serde_json::to_value(root).unwrap(), json!(root.as_str()));
        }
        assert!("both".parse::<MetaRoot>().is_err());
        let entry = DrawerMetaDoc::new(DocId::new([7; 32]));
        let value = serde_json::to_value(&entry).unwrap();
        assert_eq!(value, json!({"docId": entry.doc_id.to_string()}));
        assert_eq!(
            serde_json::from_value::<DrawerMetaDoc>(value).unwrap(),
            entry
        );
        let value = json!({"docId": entry.doc_id.to_string(), "future": {"policy": 3}});
        let carried: DrawerMetaDoc = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(carried).unwrap(), value);
    }

    #[test]
    fn a_successor_pointer_preserves_unknown_fields() {
        let drawer_id = DocId::new([8; 32]);
        let value = json!({
            "drawerId": drawer_id.to_string(),
            "future": {"reason": "replacement", "revision": 2},
        });
        let pointer: DrawerSuccessor = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(pointer.drawer_id, drawer_id);
        assert_eq!(serde_json::to_value(pointer).unwrap(), value);
        assert!(serde_json::from_value::<DrawerSuccessor>(json!({"future": 1})).is_err());
    }

    #[test]
    fn a_self_doc_descriptor_carries_only_its_tag() {
        let drawer = Drawer::self_doc();
        let value = serde_json::to_value(&drawer).unwrap();
        assert_eq!(
            value,
            json!({ "layout": { "$type": SelfDocLayout::ID, "empty": true } })
        );
        assert_eq!(serde_json::from_value::<Drawer>(value).unwrap(), drawer);
        assert_eq!(drawer.layout.known(), Some(LayoutKind::SelfDoc));
        assert!(!LayoutKind::SelfDoc.has_groups());
        assert!(LayoutKind::SingleGroup.has_groups());
    }

    #[test]
    fn the_roster_marker_is_an_empty_object() {
        let value = serde_json::to_value(DrawerRoster::inline()).unwrap();
        assert_eq!(value, json!({ "empty": true }));
        assert_eq!(
            serde_json::from_value::<DrawerRoster>(value).unwrap(),
            DrawerRoster::inline()
        );
    }

    #[test]
    fn an_unknown_layout_is_carried_not_refused() {
        // Carrying is not operating: a layout this build does not know round-trips untouched,
        // and `known()` is where operating code learns it cannot act on it.
        let value = json!({
            "layout": { "$type": "org.example.daybook.drawer.sharded", "shards": ["zShard"] },
            "futureField": 1,
        });
        let drawer: Drawer = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(drawer.layout.known(), None);
        assert_eq!(
            drawer.layout.tag(),
            Some("org.example.daybook.drawer.sharded")
        );
        assert_eq!(serde_json::to_value(&drawer).unwrap(), value);
    }

    #[test]
    fn entries_spell_access_as_keyhive_does_and_carry_unknown_values() {
        for access in DrawerAccess::ALL {
            let entry = DrawerDocEntry::new(access);
            let value = serde_json::to_value(&entry).unwrap();
            assert_eq!(value, json!({ "access": access.as_str() }));
            let decoded: DrawerDocEntry = serde_json::from_value(value).unwrap();
            assert_eq!(decoded, entry);
            assert_eq!(decoded.access.known(), Some(access));
            assert_eq!(access.as_str().parse::<DrawerAccess>().unwrap(), access);
        }
        assert!(DrawerAccess::Relay < DrawerAccess::Read);
        assert!(DrawerAccess::Write < DrawerAccess::Admin);
        assert!("read".parse::<DrawerAccess>().is_err());
        let foreign = json!({ "access": "Pull", "future": 1 });
        let entry: DrawerChildEntry = serde_json::from_value(foreign.clone()).unwrap();
        assert_eq!(entry.access, EntryAccess::Unknown("Pull".into()));
        assert_eq!(entry.access.known(), None);
        assert_eq!(entry.access.as_str(), "Pull");
        assert_eq!(serde_json::to_value(&entry).unwrap(), foreign);
    }

    #[test]
    fn a_group_key_round_trips_through_its_text_form() {
        let key = group(7);
        let text = key.to_string();
        assert!(text.starts_with('z'), "{text}");
        assert_eq!(text.parse::<GroupKey>().unwrap(), key);
        assert!("".parse::<GroupKey>().is_err());
        assert!("z".parse::<GroupKey>().is_err());
        assert!(
            utils_rs::hash::encode_base58_multibase([1u8; 16])
                .parse::<GroupKey>()
                .is_err(),
            "a shorter key is not a group id"
        );
    }

    #[cfg(feature = "schemars")]
    mod declaration {
        use super::*;
        use crate::datum::Bundle;
        use crate::datum::Declaration;
        use crate::datum::Universe;

        fn universe() -> Universe {
            Universe::collect([
                Declaration::of::<Drawer>().unwrap(),
                Declaration::of::<SelfDocLayout>().unwrap(),
                Declaration::of::<SingleGroupLayout>().unwrap(),
                Declaration::of::<DrawerRoster>().unwrap(),
                Declaration::of::<DrawerDocEntry>().unwrap(),
                Declaration::of::<DrawerChildEntry>().unwrap(),
                Declaration::of::<DrawerMetaDoc>().unwrap(),
                Declaration::of::<KnownDrawerEntry>().unwrap(),
                Declaration::of::<DrawerSuccessor>().unwrap(),
            ])
        }

        #[test]
        fn the_drawer_datums_declare_and_publish() {
            let universe = universe();
            let ids: Vec<&str> = universe
                .datums
                .iter()
                .map(|datum| datum.id.as_str())
                .collect();
            assert_eq!(
                ids,
                [
                    Drawer::ID,
                    SelfDocLayout::ID,
                    SingleGroupLayout::ID,
                    DrawerRoster::ID,
                    DrawerDocEntry::ID,
                    DrawerChildEntry::ID,
                    DrawerMetaDoc::ID,
                    KnownDrawerEntry::ID,
                    DrawerSuccessor::ID,
                ]
            );
            let bundle = Bundle::new(&universe, Vec::new());
            let lexicons = bundle.lexicons().unwrap();
            assert_eq!(lexicons.len(), 9);
            let drawer = &lexicons[0].file;
            assert_eq!(drawer["id"], Drawer::ID);
            let layout = &drawer["defs"]["main"]["properties"]["layout"];
            assert_eq!(layout["type"], "union", "{drawer}");
            assert_eq!(layout.get("closed"), None, "{drawer}");
            assert_eq!(
                layout["refs"],
                json!([SelfDocLayout::ID, SingleGroupLayout::ID]),
                "{drawer}"
            );
            for entry_id in [DrawerDocEntry::ID, DrawerChildEntry::ID] {
                let entry_lex = lexicons.iter().find(|lex| lex.id == entry_id).unwrap();
                let access = &entry_lex.file["defs"]["main"]["properties"]["access"];
                assert_eq!(access["type"], "string", "{}", entry_lex.file);
                assert_eq!(
                    access["knownValues"],
                    json!(["Relay", "Read", "Write", "Admin"]),
                    "{}",
                    entry_lex.file
                );
                assert_eq!(access.get("enum"), None, "{}", entry_lex.file);
            }
            let meta = lexicons
                .iter()
                .find(|lex| lex.id == DrawerMetaDoc::ID)
                .unwrap();
            assert_eq!(
                meta.file["defs"]["main"]["properties"]
                    .as_object()
                    .unwrap()
                    .keys()
                    .collect::<Vec<_>>(),
                vec!["docId"]
            );
            // Admission is what the service does with these at boot: each datum's lexicon is
            // checked against its declaration with the others carried, members included.
            for (index, lexicon) in lexicons.iter().enumerate() {
                let carried: Vec<&serde_json::Value> = lexicons
                    .iter()
                    .enumerate()
                    .filter(|(other, _)| *other != index)
                    .map(|(_, other)| &other.file)
                    .collect();
                bundle
                    .validate_lexicon(&lexicon.id, &lexicon.file, &carried)
                    .unwrap_or_else(|error| panic!("{}: {error}", lexicon.id));
            }
        }
    }
}
