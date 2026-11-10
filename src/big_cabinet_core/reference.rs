//! `dtxt:` URLs: `dtxt:<kind>:<opaque path>[?heads=…][#fragment]` (ADR 035).
//!
//! A [`DatumReference`] is only an address. Parsing it does not check whether a
//! document or datum exists, is retained, is readable, or is reachable. The path after
//! the kind is opaque (nothing follows the scheme's colon with `/`), so no URL parser
//! normalizes it and this module parses it itself.

use std::fmt;
use std::str::FromStr;

use crate::dmeta::MultibaseKey;
use automerge::ChangeHash;
use big_sync_core::ObjKey;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use unicode_properties::GeneralCategory;
use unicode_properties::UnicodeGeneralCategory;

pub const REFERENCE_SCHEME: &str = "dtxt";
const DATUM_KIND: &str = "datum";
const ACTOR_KIND: &str = "actor";
/// The only query key defined (ADR 035): the document heads a reference was made at.
const HEADS_KEY: &str = "heads";
pub const SELF_DOC: &str = "self";

/// Unicode data used when serializing canonical Go-style datum references.
///
/// This is deliberately fixed to Unicode 15.0.0, the data version used by the
/// published Go 1.23 reference vectors. Changing it changes canonical spelling
/// for code points whose Unicode assignments change, so it is a serialization
/// change rather than an incidental dependency update. Parsing remains liberal:
/// incoming UTF-8 and escaped bytes are still accepted independently of this
/// outgoing classification table.
pub const CANONICAL_REFERENCE_UNICODE_VERSION: (u64, u64, u64) = (15, 0, 0);

const _: () = match unicode_properties::UNICODE_VERSION {
    (15, 0, 0) => (),
    _ => panic!("canonical reference Unicode data must be 15.0.0"),
};

/// The concrete document identity used by Cabinet and BigRepo.
pub type DocId = ObjKey;

/// The stable, schema-independent identity of a datum vocabulary.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DatumTag(String);

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for DatumTag {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "DatumTag".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::DatumTag").into()
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({ "type": "string" })
    }
}

impl DatumTag {
    /// Construct a tag that can occupy one datum-reference path component.
    ///
    /// Tags are intentionally not restricted to an NSID or to ASCII. The only
    /// structural restrictions here are the ones required by the reference
    /// grammar: the component is non-empty and contains no slash.
    pub fn new(value: impl Into<String>) -> Result<Self, ReferenceError> {
        let value = value.into();
        if value.is_empty() {
            return Err(ReferenceError::EmptyDatumTag);
        }
        if value.contains('/') {
            return Err(ReferenceError::InvalidDatumTag(value));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DatumTag {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Serialize for DatumTag {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for DatumTag {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// A datum tag plus its key slot.
///
/// The slot is bytes rather than a `String`: dpath-like names may contain
/// `/`, and arbitrary non-UTF-8 bytes are represented by reversible Go
/// string-literal escapes in the reference grammar.
/// `Ord` is by the canonical spelling, the same order the claim wire sorts by, so a `BTreeMap`
/// keyed on a datum key agrees with the bytes a signature covers.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DatumKey {
    tag: DatumTag,
    slot: Vec<u8>,
}

impl DatumKey {
    /// Construct a key with a non-empty, slash-preserving slot.
    pub fn new(tag: impl Into<String>, slot: impl Into<Vec<u8>>) -> Result<Self, ReferenceError> {
        let tag = DatumTag::new(tag)?;
        let slot = slot.into();
        validate_key_slot(&slot)?;
        Ok(Self { tag, slot })
    }

    pub fn from_text(
        tag: impl Into<String>,
        slot: impl Into<String>,
    ) -> Result<Self, ReferenceError> {
        Self::new(tag, slot.into().into_bytes())
    }

    pub fn tag(&self) -> &DatumTag {
        &self.tag
    }

    pub fn slot_bytes(&self) -> &[u8] {
        &self.slot
    }
}

impl fmt::Display for DatumKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}/{}", self.tag, go_string_escape(&self.slot))
    }
}

impl FromStr for DatumKey {
    type Err = ReferenceError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (tag, slot) = value
            .split_once('/')
            .ok_or(ReferenceError::MissingDatumKey)?;
        if slot.is_empty() {
            return Err(ReferenceError::MissingDatumKey);
        }
        Self::new(tag, go_string_unescape(slot.as_bytes())?)
    }
}

impl Serialize for DatumKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for DatumKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for DatumReference {
    /// A reference is a position, never a name a peer points at, so it is inlined.
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> std::borrow::Cow<'static, str> {
        "DatumReference".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::DatumReference").into()
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        crate::datum::codec_schema(
            crate::datum::Codec::Reference,
            schemars::json_schema!({ "type": "string" }),
        )
    }
}

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for DatumKey {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "DatumKey".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::DatumKey").into()
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({ "type": "string" })
    }
}

/// Whether a datum reference names a concrete document or its containing one.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ReferenceDocId {
    Concrete(DocId),
    SelfDoc,
}

impl Serialize for ReferenceDocId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Concrete(document) => serializer.serialize_str(&document.to_string()),
            Self::SelfDoc => serializer.serialize_str(SELF_DOC),
        }
    }
}

impl<'de> Deserialize<'de> for ReferenceDocId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        if value == SELF_DOC {
            return Ok(Self::SelfDoc);
        }
        DocId::from_str(&value)
            .map(Self::Concrete)
            .map_err(serde::de::Error::custom)
    }
}

impl ReferenceDocId {
    /// Resolve `self` against the document containing the referencing datum.
    pub fn resolve(&self, containing_document: &DocId) -> DocId {
        match self {
            Self::Concrete(doc_id) => doc_id.clone(),
            Self::SelfDoc => containing_document.clone(),
        }
    }
}

/// A schema-independent address for one datum, optionally pinned to a version of its document.
///
/// Text form: `dtxt:datum:<doc>/<tag>/<slot>[?heads=<head>|<head>…]` (ADR 035). A reference
/// inside a datum value holds the text without its query and keeps the pin in the sibling
/// `refHeads` member; [`Self::parse_stored`] and the serde impls are that stored form, and
/// [`fmt::Display`] and [`Self::parse`] are the text form.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DatumReference {
    document: ReferenceDocId,
    key: DatumKey,
    /// Sorted and deduplicated; empty follows the document's current frontier.
    heads: Vec<ChangeHash>,
}

/// A position inside a datum: a [`DatumReference`] plus an optional fragment. Annotation targets
/// and other selectors take this; APIs that take a whole datum take a [`DatumReference`] and so
/// refuse a fragment. A location without a fragment is the whole datum.
///
/// The fragment is kept as the text after `#`. Its grammar (automerge-repo's path grammar, ADR
/// 035) is interpreted by the target's type, so it is checked to be well-formed URL text but not
/// resolved here.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Location {
    reference: DatumReference,
    fragment: Option<String>,
}

/// `dtxt:actor:<node key>[/<path>]`: an actor's identity inside signed attribution
/// certificates (ADR 035 §3, ADR 022). The operator's path is empty, so its URL is the bare node
/// key; every other actor's path names its whole ancestry. It has no query or fragment.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ActorUrl {
    node_key: MultibaseKey,
    path: String,
}

impl DatumReference {
    pub fn new(document: ReferenceDocId, key: DatumKey) -> Self {
        Self {
            document,
            key,
            heads: Vec::new(),
        }
    }

    /// The same address pinned to `heads`.
    #[must_use]
    pub fn pinned(mut self, heads: impl IntoIterator<Item = ChangeHash>) -> Self {
        self.heads = heads.into_iter().collect();
        self.heads.sort();
        self.heads.dedup();
        self
    }

    pub fn document(&self) -> &ReferenceDocId {
        &self.document
    }

    pub fn key(&self) -> &DatumKey {
        &self.key
    }

    pub fn heads(&self) -> &[ChangeHash] {
        &self.heads
    }

    /// Parse the text form. Non-canonical input (unsorted heads, lowercase percent escapes, a
    /// scheme in capitals) is accepted and canonicalized; a fragment is refused, see [`Location`].
    pub fn parse(value: &str) -> Result<Self, ReferenceError> {
        let location = Location::parse(value)?;
        if location.fragment.is_some() {
            return Err(ReferenceError::FragmentNotAllowed);
        }
        Ok(location.reference)
    }

    /// Parse the stored form: the text without its query, because the pin lives in the sibling
    /// `refHeads` member.
    pub fn parse_stored(value: &str) -> Result<Self, ReferenceError> {
        let reference = Self::parse(value)?;
        if !reference.heads.is_empty() {
            return Err(ReferenceError::PinInStoredReference);
        }
        Ok(reference)
    }

    fn parse_path(path: &str) -> Result<(ReferenceDocId, DatumKey), ReferenceError> {
        let (document, remainder) = path.split_once('/').ok_or(ReferenceError::MissingDoc)?;
        let (tag, key_tail) = remainder
            .split_once('/')
            .ok_or(ReferenceError::MissingDatumKey)?;
        if document.is_empty() || tag.is_empty() || key_tail.is_empty() {
            return Err(ReferenceError::MissingDatumKey);
        }

        let document = decode_uri_component(document)?;
        let tag = decode_uri_component(tag)?;
        let key_tail = decode_uri_component(key_tail)?;
        let key = DatumKey::new(tag, go_string_unescape(key_tail.as_bytes())?)?;

        let document = if document == SELF_DOC {
            ReferenceDocId::SelfDoc
        } else {
            ReferenceDocId::Concrete(
                DocId::from_str(&document).map_err(|_| ReferenceError::InvalidDocId(document))?,
            )
        };
        Ok((document, key))
    }

    fn write_path(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let document = match &self.document {
            ReferenceDocId::Concrete(document) => document.to_string(),
            ReferenceDocId::SelfDoc => SELF_DOC.to_owned(),
        };
        write!(
            formatter,
            "{REFERENCE_SCHEME}:{DATUM_KIND}:{}/{}/{}",
            encode_uri_component(document.as_bytes(), false),
            encode_uri_component(self.key.tag.as_str().as_bytes(), false),
            encode_uri_component(go_string_escape(self.key.slot_bytes()).as_bytes(), true),
        )?;
        if !self.heads.is_empty() {
            // `self.heads` is kept in hash-byte order, the canonical order (ADR 035): base58
            // strings of equal-length values differ in length when they lead with zero bytes, so
            // sorting the spelled heads would disagree with the bytes and make equal pins spell
            // differently from their stored `refHeads`.
            let heads = self.heads.iter().map(head_text).collect::<Vec<_>>();
            write!(formatter, "?{HEADS_KEY}={}", heads.join("|"))?;
        }
        Ok(())
    }
}

impl fmt::Display for DatumReference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.write_path(formatter)
    }
}

impl FromStr for DatumReference {
    type Err = ReferenceError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

/// The stored form. A pinned reference cannot be serialized as one string: its pin belongs in
/// the sibling `refHeads` member, and dropping it silently would change what the reference means.
impl Serialize for DatumReference {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if !self.heads.is_empty() {
            return Err(serde::ser::Error::custom(
                ReferenceError::PinInStoredReference,
            ));
        }
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for DatumReference {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse_stored(&value).map_err(serde::de::Error::custom)
    }
}

impl Location {
    pub fn new(
        reference: DatumReference,
        fragment: Option<String>,
    ) -> Result<Self, ReferenceError> {
        if let Some(fragment) = &fragment {
            validate_fragment(fragment)?;
        }
        Ok(Self {
            reference,
            fragment,
        })
    }

    pub fn reference(&self) -> &DatumReference {
        &self.reference
    }

    pub fn fragment(&self) -> Option<&str> {
        self.fragment.as_deref()
    }

    /// The datum this location is inside, without its fragment.
    pub fn into_reference(self) -> DatumReference {
        self.reference
    }

    /// Parse the stored form: the text without its query, because the pin lives in the sibling
    /// `refHeads` member, as for [`DatumReference::parse_stored`]. The fragment stays: it moves
    /// when the position moves, not when the pin does (ADR 035 §2.8).
    pub fn parse_stored(value: &str) -> Result<Self, ReferenceError> {
        let location = Self::parse(value)?;
        if !location.reference.heads.is_empty() {
            return Err(ReferenceError::PinInStoredReference);
        }
        Ok(location)
    }

    pub fn parse(value: &str) -> Result<Self, ReferenceError> {
        let rest = strip_scheme_and_kind(value, DATUM_KIND)?;
        let (before_fragment, fragment) = match rest.split_once('#') {
            Some((before, fragment)) => (before, Some(fragment)),
            None => (rest, None),
        };
        let (path, query) = match before_fragment.split_once('?') {
            Some((path, query)) => (path, Some(query)),
            None => (before_fragment, None),
        };
        let (document, key) = DatumReference::parse_path(path)?;
        let heads = query
            .map(parse_heads_query)
            .transpose()?
            .unwrap_or_default();
        let reference = DatumReference::new(document, key).pinned(heads);
        Self::new(reference, fragment.map(str::to_owned))
    }
}

impl fmt::Display for Location {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.reference.write_path(formatter)?;
        if let Some(fragment) = &self.fragment {
            write!(formatter, "#{fragment}")?;
        }
        Ok(())
    }
}

impl FromStr for Location {
    type Err = ReferenceError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

/// The stored form, as for [`DatumReference`]: a pinned location is refused rather than written
/// without its pin.
impl Serialize for Location {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if !self.reference.heads.is_empty() {
            return Err(serde::ser::Error::custom(
                ReferenceError::PinInStoredReference,
            ));
        }
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Location {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse_stored(&value).map_err(serde::de::Error::custom)
    }
}

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for Location {
    /// A position, like a reference, so it is inlined.
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Location".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::Location").into()
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        crate::datum::codec_schema(
            crate::datum::Codec::Location,
            schemars::json_schema!({ "type": "string" }),
        )
    }
}

impl ActorUrl {
    /// An actor's URL from its node's key and its path (empty for the operator). Each path
    /// segment is a non-empty run of ASCII letters, digits, `-`, `_` and `.`, other than `.` and
    /// `..`, so it reads the same in a URL as in the actor registry.
    pub fn new(node_key: MultibaseKey, path: impl Into<String>) -> Result<Self, ReferenceError> {
        let path = path.into();
        if !path.is_empty()
            && !path.split('/').all(|segment| {
                !segment.is_empty()
                    && segment != "."
                    && segment != ".."
                    && segment
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
            })
        {
            return Err(ReferenceError::InvalidActorPath(path));
        }
        Ok(Self { node_key, path })
    }

    pub fn node_key(&self) -> &MultibaseKey {
        &self.node_key
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn parse(value: &str) -> Result<Self, ReferenceError> {
        let rest = strip_scheme_and_kind(value, ACTOR_KIND)?;
        if rest.contains(['?', '#']) {
            return Err(ReferenceError::QueryOrFragmentNotAllowed);
        }
        let (node_key, path) = match rest.split_once('/') {
            // A slash introduces a path, so a trailing one is an empty segment.
            Some((_, "")) => return Err(ReferenceError::InvalidActorPath(String::new())),
            Some(split) => split,
            None => (rest, ""),
        };
        // The multibase decoder indexes its prefix, so it is given only non-empty ASCII.
        let node_key = Some(node_key)
            .filter(|key| !key.is_empty() && key.is_ascii())
            .ok_or_else(|| ReferenceError::InvalidActorKey(node_key.to_owned()))?
            .parse()
            .map_err(|_| ReferenceError::InvalidActorKey(node_key.to_owned()))?;
        Self::new(node_key, path)
    }
}

impl fmt::Display for ActorUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{REFERENCE_SCHEME}:{ACTOR_KIND}:{}",
            self.node_key
        )?;
        if !self.path.is_empty() {
            write!(formatter, "/{}", self.path)?;
        }
        Ok(())
    }
}

impl FromStr for ActorUrl {
    type Err = ReferenceError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

/// The text form. An actor URL names an actor, not a datum, so the reference index never reads
/// it and its schema is a plain string.
impl Serialize for ActorUrl {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for ActorUrl {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).map_err(serde::de::Error::custom)
    }
}

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for ActorUrl {
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ActorUrl".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::ActorUrl").into()
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({ "type": "string" })
    }
}

/// A document head as it is written in `?heads=` and in the sibling `refHeads` member: the 32
/// hash bytes as multibase base58btc, like our other ids.
pub fn head_text(head: &ChangeHash) -> String {
    utils_rs::hash::encode_base58_multibase(head.0)
}

pub fn parse_head(value: &str) -> Result<ChangeHash, ReferenceError> {
    if !value.is_ascii() || value.is_empty() {
        return Err(ReferenceError::InvalidHead(value.to_owned()));
    }
    let bytes = utils_rs::hash::decode_base58_multibase(value)
        .map_err(|_| ReferenceError::InvalidHead(value.to_owned()))?;
    <[u8; 32]>::try_from(bytes)
        .map(ChangeHash)
        .map_err(|_| ReferenceError::InvalidHead(value.to_owned()))
}

/// Serde for a sibling `refHeads` member: an array of [`head_text`] strings.
pub mod head_list {
    use super::ChangeHash;
    use super::head_text;
    use super::parse_head;
    use serde::Deserialize;
    use serde::Deserializer;
    use serde::Serializer;

    pub fn serialize<S: Serializer>(
        heads: &[ChangeHash],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(heads.iter().map(head_text))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<ChangeHash>, D::Error> {
        Vec::<String>::deserialize(deserializer)?
            .iter()
            .map(|head| parse_head(head).map_err(serde::de::Error::custom))
            .collect()
    }
}

fn parse_heads_query(query: &str) -> Result<Vec<ChangeHash>, ReferenceError> {
    let heads = query
        .strip_prefix(HEADS_KEY)
        .and_then(|rest| rest.strip_prefix('='))
        .ok_or_else(|| ReferenceError::UnsupportedQuery(query.to_owned()))?;
    if heads.is_empty() {
        return Err(ReferenceError::EmptyHeads);
    }
    heads.split('|').map(parse_head).collect()
}

/// `value` without its `dtxt:<kind>:` prefix. The scheme is matched case-insensitively and
/// canonicalized; kinds are lowercase words.
fn strip_scheme_and_kind<'a>(value: &'a str, kind: &str) -> Result<&'a str, ReferenceError> {
    let (scheme, remainder) = value
        .split_once(':')
        .ok_or_else(|| ReferenceError::InvalidReference(value.to_owned()))?;
    if !scheme.eq_ignore_ascii_case(REFERENCE_SCHEME) {
        return Err(ReferenceError::WrongScheme(scheme.to_owned()));
    }
    let (found, rest) = remainder
        .split_once(':')
        .ok_or_else(|| ReferenceError::InvalidReference(value.to_owned()))?;
    if found != kind {
        return Err(ReferenceError::UnexpectedKind {
            expected: kind.to_owned(),
            found: found.to_owned(),
        });
    }
    Ok(rest)
}

/// A fragment is URL text: non-empty, no space, control or non-ASCII byte, no second `#`, and
/// well-formed percent escapes.
fn validate_fragment(fragment: &str) -> Result<(), ReferenceError> {
    let bytes = fragment.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' => {
                if !bytes
                    .get(index + 1..index + 3)
                    .is_some_and(|escape| escape.iter().all(|byte| hex(*byte).is_some()))
                {
                    return Err(ReferenceError::InvalidFragment(fragment.to_owned()));
                }
                index += 3;
            }
            b'#' | b' ' | 0..=0x1f | 0x7f..=0xff => {
                return Err(ReferenceError::InvalidFragment(fragment.to_owned()));
            }
            _ => index += 1,
        }
    }
    if fragment.is_empty() {
        return Err(ReferenceError::InvalidFragment(String::new()));
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum ReferenceError {
    #[error("invalid reference {0:?}")]
    InvalidReference(String),
    #[error("wrong URI scheme {0:?}")]
    WrongScheme(String),
    #[error("expected a `dtxt:{expected}:` reference, found kind {found:?}")]
    UnexpectedKind { expected: String, found: String },
    #[error("a datum reference cannot have a fragment; a location can")]
    FragmentNotAllowed,
    #[error("this reference kind cannot have a query or fragment")]
    QueryOrFragmentNotAllowed,
    #[error("unsupported query {0:?}: only `heads=` is defined")]
    UnsupportedQuery(String),
    #[error("`heads=` is empty; an unpinned reference has no query")]
    EmptyHeads,
    #[error("{0:?} is not a document head: expected 32 bytes as multibase base58btc")]
    InvalidHead(String),
    #[error("a stored reference holds no query; its pin is the sibling `refHeads` member")]
    PinInStoredReference,
    #[error("invalid fragment {0:?}")]
    InvalidFragment(String),
    #[error("datum reference is missing a document id")]
    MissingDoc,
    #[error("datum reference is missing a datum tag or key slot")]
    MissingDatumKey,
    #[error("datum reference has an empty datum tag")]
    EmptyDatumTag,
    #[error("invalid datum tag {0:?}")]
    InvalidDatumTag(String),
    #[error("invalid percent escape in reference component")]
    InvalidPercentEscape,
    #[error("invalid Go string escape in datum key slot")]
    InvalidGoEscape,
    #[error("invalid UTF-8 in reference component")]
    InvalidUtf8,
    #[error("invalid document id {0:?}")]
    InvalidDocId(String),
    #[error("{0:?} is not an actor node key")]
    InvalidActorKey(String),
    #[error("invalid actor path {0:?}")]
    InvalidActorPath(String),
}

fn validate_key_slot(slot: &[u8]) -> Result<(), ReferenceError> {
    if slot.is_empty() {
        return Err(ReferenceError::MissingDatumKey);
    }
    Ok(())
}

/// The body of a Go double-quoted string literal, without its delimiters.
///
/// This follows the Go `strconv.Quote` source contract: printable Unicode is retained,
/// while non-printable Unicode uses `\\u`/`\\U` and invalid input bytes use `\\x`.
fn go_string_escape(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match std::str::from_utf8(&bytes[index..]) {
            Ok(text) => {
                for character in text.chars() {
                    append_go_escaped_rune(&mut output, character);
                }
                break;
            }
            Err(error) if error.valid_up_to() != 0 => {
                let valid = error.valid_up_to();
                let text = std::str::from_utf8(&bytes[index..index + valid]).unwrap();
                for character in text.chars() {
                    append_go_escaped_rune(&mut output, character);
                }
                index += valid;
            }
            Err(_) => {
                push_hex_escape(&mut output, bytes[index]);
                index += 1;
            }
        }
    }
    output
}

fn append_go_escaped_rune(output: &mut String, character: char) {
    match character {
        '\\' => output.push_str("\\\\"),
        '"' => output.push_str("\\\""),
        '\x07' => output.push_str("\\a"),
        '\x08' => output.push_str("\\b"),
        '\x0c' => output.push_str("\\f"),
        '\n' => output.push_str("\\n"),
        '\r' => output.push_str("\\r"),
        '\t' => output.push_str("\\t"),
        '\x0b' => output.push_str("\\v"),
        character if character.is_ascii() && character.is_ascii_control() => {
            push_hex_escape(output, character as u8)
        }
        character if character.is_ascii() => output.push(character),
        character if go_is_print(character) => output.push(character),
        character if (character as u32) <= 0xffff => {
            output.push_str("\\u");
            push_fixed_hex(output, character as u32, 4);
        }
        character => {
            output.push_str("\\U");
            push_fixed_hex(output, character as u32, 8);
        }
    }
}

/// Go's `strconv.IsPrint`: Unicode letters, marks, numbers, punctuation, and symbols,
/// plus ASCII space through `~`; Unicode space separators are intentionally excluded.
///
/// The exact `unicode-properties = 0.1.1` dependency supplies Unicode 15.0.0
/// general-category data. Keep that dependency exact: changing its data can change
/// canonical output for code points assigned after the frozen Go 1.23 data set.
fn go_is_print(character: char) -> bool {
    if character <= '\u{7f}' {
        return character >= ' ';
    }
    if character <= '\u{ff}' {
        return character >= '\u{a1}' && character != '\u{ad}';
    }
    matches!(
        character.general_category(),
        GeneralCategory::UppercaseLetter
            | GeneralCategory::LowercaseLetter
            | GeneralCategory::TitlecaseLetter
            | GeneralCategory::ModifierLetter
            | GeneralCategory::OtherLetter
            | GeneralCategory::NonspacingMark
            | GeneralCategory::SpacingMark
            | GeneralCategory::EnclosingMark
            | GeneralCategory::DecimalNumber
            | GeneralCategory::LetterNumber
            | GeneralCategory::OtherNumber
            | GeneralCategory::ConnectorPunctuation
            | GeneralCategory::DashPunctuation
            | GeneralCategory::OpenPunctuation
            | GeneralCategory::ClosePunctuation
            | GeneralCategory::InitialPunctuation
            | GeneralCategory::FinalPunctuation
            | GeneralCategory::OtherPunctuation
            | GeneralCategory::MathSymbol
            | GeneralCategory::CurrencySymbol
            | GeneralCategory::ModifierSymbol
            | GeneralCategory::OtherSymbol
    )
}

fn push_fixed_hex(output: &mut String, value: u32, width: usize) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for shift in (0..width).rev() {
        output.push(HEX[((value >> (shift * 4)) & 0xf) as usize] as char);
    }
}

fn push_hex_escape(output: &mut String, byte: u8) {
    output.push_str("\\x");
    push_fixed_hex(output, u32::from(byte), 2);
}

fn go_string_unescape(bytes: &[u8]) -> Result<Vec<u8>, ReferenceError> {
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'\\' {
            if bytes[index] == b'"' || bytes[index] == b'\n' {
                return Err(ReferenceError::InvalidGoEscape);
            }
            let character = std::str::from_utf8(&bytes[index..])
                .map_err(|_| ReferenceError::InvalidUtf8)?
                .chars()
                .next()
                .ok_or(ReferenceError::InvalidUtf8)?;
            let mut encoded = [0; 4];
            output.extend_from_slice(character.encode_utf8(&mut encoded).as_bytes());
            index += character.len_utf8();
            continue;
        }
        index += 1;
        let escape = *bytes.get(index).ok_or(ReferenceError::InvalidGoEscape)?;
        index += 1;
        match escape {
            b'a' => output.push(0x07),
            b'b' => output.push(0x08),
            b'f' => output.push(0x0c),
            b'n' => output.push(b'\n'),
            b'r' => output.push(b'\r'),
            b't' => output.push(b'\t'),
            b'v' => output.push(0x0b),
            b'\\' => output.push(b'\\'),
            b'"' => output.push(b'"'),
            b'x' => {
                let high = hex(*bytes.get(index).ok_or(ReferenceError::InvalidGoEscape)?)
                    .ok_or(ReferenceError::InvalidGoEscape)?;
                let low = hex(*bytes
                    .get(index + 1)
                    .ok_or(ReferenceError::InvalidGoEscape)?)
                .ok_or(ReferenceError::InvalidGoEscape)?;
                output.push(high << 4 | low);
                index += 2;
            }
            b'u' => {
                let (value, consumed) = parse_unicode_escape(bytes, index, 4)?;
                append_decoded_unicode(&mut output, value)?;
                index += consumed;
            }
            b'U' => {
                let (value, consumed) = parse_unicode_escape(bytes, index, 8)?;
                append_decoded_unicode(&mut output, value)?;
                index += consumed;
            }
            b'0'..=b'7' => {
                // Go string literals require exactly three octal digits. Accumulate in
                // a wider integer so \400 and \777 return an error instead of wrapping.
                let mut value = u32::from(escape - b'0');
                for _ in 0..2 {
                    let next = *bytes.get(index).ok_or(ReferenceError::InvalidGoEscape)?;
                    if !(b'0'..=b'7').contains(&next) {
                        return Err(ReferenceError::InvalidGoEscape);
                    }
                    value = value * 8 + u32::from(next - b'0');
                    index += 1;
                }
                if value > u32::from(u8::MAX) {
                    return Err(ReferenceError::InvalidGoEscape);
                }
                output.push(value as u8);
            }
            _ => return Err(ReferenceError::InvalidGoEscape),
        }
    }
    Ok(output)
}

fn append_decoded_unicode(output: &mut Vec<u8>, value: u32) -> Result<(), ReferenceError> {
    let character = char::from_u32(value).ok_or(ReferenceError::InvalidGoEscape)?;
    let mut encoded = [0; 4];
    output.extend_from_slice(character.encode_utf8(&mut encoded).as_bytes());
    Ok(())
}

fn parse_unicode_escape(
    bytes: &[u8],
    index: usize,
    width: usize,
) -> Result<(u32, usize), ReferenceError> {
    let mut value = 0u32;
    for offset in 0..width {
        value = value
            .checked_mul(16)
            .and_then(|value| {
                hex(*bytes.get(index + offset)?).map(|digit| value + u32::from(digit))
            })
            .ok_or(ReferenceError::InvalidGoEscape)?;
    }
    Ok((value, width))
}

/// Percent-encode one path component. Only what the URL grammar needs is escaped (`%`, `?`,
/// `#`, controls, space, non-ASCII and the other bytes RFC 3986 does not allow in a path), with
/// uppercase hex digits, so each component has one canonical spelling (ADR 035 §2.7). `/` is
/// kept raw when `allow_slash`.
fn encode_uri_component(bytes: &[u8], allow_slash: bool) -> String {
    let mut output = String::with_capacity(bytes.len());
    for &byte in bytes {
        if (allow_slash && byte == b'/')
            || byte.is_ascii_alphanumeric()
            || b"-._~!$&'()*+,;=:@".contains(&byte)
        {
            output.push(byte as char);
        } else {
            output.push('%');
            output.push(hex_char(byte >> 4));
            output.push(hex_char(byte & 0x0f));
        }
    }
    output
}

fn decode_uri_component(value: &str) -> Result<String, ReferenceError> {
    let mut bytes = Vec::with_capacity(value.len());
    let raw = value.as_bytes();
    let mut index = 0;
    while index < raw.len() {
        if raw[index] == b'%' {
            if index + 2 >= raw.len() {
                return Err(ReferenceError::InvalidPercentEscape);
            }
            let high = hex(raw[index + 1]).ok_or(ReferenceError::InvalidPercentEscape)?;
            let low = hex(raw[index + 2]).ok_or(ReferenceError::InvalidPercentEscape)?;
            bytes.push(high << 4 | low);
            index += 3;
        } else {
            let character = value[index..]
                .chars()
                .next()
                .ok_or(ReferenceError::InvalidUtf8)?;
            let mut encoded = [0; 4];
            bytes.extend_from_slice(character.encode_utf8(&mut encoded).as_bytes());
            index += character.len_utf8();
        }
    }
    String::from_utf8(bytes).map_err(|_| ReferenceError::InvalidUtf8)
}

fn hex(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn hex_char(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        10..=15 => (b'A' + value - 10) as char,
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests;
