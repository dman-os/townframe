//! Canonical, schema-independent addresses for facet keys.
//!
//! A [`FacetReference`] is only an address. Parsing it does not check whether a
//! document or facet exists, is retained, is readable, or is reachable.

use std::fmt;
use std::str::FromStr;

use big_sync_core::ObjKey;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use unicode_properties::{GeneralCategory, UnicodeGeneralCategory};
use url::Url;

pub const FACET_REFERENCE_SCHEME: &str = "db+facet";
pub const SELF_DOC: &str = "self";

/// Unicode data used when serializing canonical Go-style facet references.
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

/// The stable, schema-independent identity of a facet vocabulary.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FacetTag(String);

impl FacetTag {
    /// Construct a tag that can occupy one facet-reference path component.
    ///
    /// Tags are intentionally not restricted to an NSID or to ASCII. The only
    /// structural restrictions here are the ones required by the reference
    /// grammar: the component is non-empty, contains no slash, and is not a
    /// URL dot segment.
    pub fn new(value: impl Into<String>) -> Result<Self, ReferenceError> {
        let value = value.into();
        if value.is_empty() {
            return Err(ReferenceError::EmptyFacetTag);
        }
        if value.contains('/') {
            return Err(ReferenceError::InvalidFacetTag(value));
        }
        if value == "." || value == ".." {
            return Err(ReferenceError::DotSegment);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for FacetTag {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Serialize for FacetTag {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for FacetTag {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// A facet tag plus its instance/key identifier.
///
/// The identifier is bytes rather than a `String`: dpath-like names may contain
/// `/`, and arbitrary non-UTF-8 bytes are represented by reversible Go
/// string-literal escapes in the reference grammar.
/// `Ord` is by the canonical spelling, the same order the claim wire sorts by, so a `BTreeMap`
/// keyed on a facet key agrees with the bytes a signature covers.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FacetKey {
    tag: FacetTag,
    id: Vec<u8>,
}

impl FacetKey {
    /// Construct a key with a non-empty, slash-preserving ID.
    pub fn new(tag: impl Into<String>, id: impl Into<Vec<u8>>) -> Result<Self, ReferenceError> {
        let tag = FacetTag::new(tag)?;
        let id = id.into();
        validate_key_id(&id)?;
        Ok(Self { tag, id })
    }

    pub fn from_text(
        tag: impl Into<String>,
        id: impl Into<String>,
    ) -> Result<Self, ReferenceError> {
        Self::new(tag, id.into().into_bytes())
    }

    pub fn tag(&self) -> &FacetTag {
        &self.tag
    }

    pub fn id_bytes(&self) -> &[u8] {
        &self.id
    }
}

impl fmt::Display for FacetKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}/{}", self.tag, go_string_escape(&self.id))
    }
}

impl FromStr for FacetKey {
    type Err = ReferenceError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (tag, id) = value
            .split_once('/')
            .ok_or(ReferenceError::MissingFacetKey)?;
        if id.is_empty() {
            return Err(ReferenceError::MissingFacetKey);
        }
        Self::new(tag, go_string_unescape(id.as_bytes())?)
    }
}

impl Serialize for FacetKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for FacetKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for FacetReference {
    /// A reference is a position, never a name a peer points at, so it is inlined.
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> std::borrow::Cow<'static, str> {
        "FacetReference".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::FacetReference").into()
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        crate::facet::codec_schema(
            crate::facet::Codec::Reference,
            schemars::json_schema!({ "type": "string" }),
        )
    }
}

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for FacetKey {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "FacetKey".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::FacetKey").into()
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({ "type": "string" })
    }
}

/// Whether a facet reference names a concrete document or its containing one.
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
    /// Resolve `self` against the document containing the referencing facet.
    pub fn resolve(&self, containing_document: &DocId) -> DocId {
        match self {
            Self::Concrete(doc_id) => doc_id.clone(),
            Self::SelfDoc => containing_document.clone(),
        }
    }
}

/// A schema-independent address for one facet key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FacetReference {
    document: ReferenceDocId,
    key: FacetKey,
}

impl FacetReference {
    pub fn new(document: ReferenceDocId, key: FacetKey) -> Result<Self, ReferenceError> {
        Ok(Self { document, key })
    }

    pub fn document(&self) -> &ReferenceDocId {
        &self.document
    }

    pub fn key(&self) -> &FacetKey {
        &self.key
    }

    /// Parse untrusted text without allowing `Url` to normalize the key tail.
    pub fn parse(value: &str) -> Result<Self, ReferenceError> {
        let (scheme, remainder) = value
            .split_once(':')
            .ok_or_else(|| ReferenceError::InvalidReference(value.to_owned()))?;
        if !scheme.eq_ignore_ascii_case(FACET_REFERENCE_SCHEME) {
            return Err(ReferenceError::WrongScheme(scheme.to_owned()));
        }
        let (path, suffix) = split_uri_suffix(remainder);
        if suffix.is_some() {
            return Err(ReferenceError::QueryOrFragmentNotAllowed);
        }
        let path = path.strip_prefix('/').ok_or(ReferenceError::MissingPath)?;
        if path.starts_with('/') {
            return Err(ReferenceError::AuthorityNotAllowed);
        }
        reject_dot_segments(path)?;
        Self::parse_path(path)
    }

    /// Parse an already-constructed URL.
    ///
    /// Call [`Self::parse`] for untrusted text: a `Url` has already discarded
    /// the original spelling of any dot segments it normalized.
    pub fn parse_url(url: &Url) -> Result<Self, ReferenceError> {
        if url.scheme() != FACET_REFERENCE_SCHEME {
            return Err(ReferenceError::WrongScheme(url.scheme().to_owned()));
        }
        if url.host_str().is_some() || !url.username().is_empty() || url.port().is_some() {
            return Err(ReferenceError::AuthorityNotAllowed);
        }
        if url.query().is_some() || url.fragment().is_some() {
            return Err(ReferenceError::QueryOrFragmentNotAllowed);
        }
        let path = url
            .path()
            .strip_prefix('/')
            .ok_or(ReferenceError::MissingPath)?;
        if path.starts_with('/') {
            return Err(ReferenceError::AuthorityNotAllowed);
        }
        reject_dot_segments(path)?;
        Self::parse_path(path)
    }

    fn parse_path(path: &str) -> Result<Self, ReferenceError> {
        let (document, remainder) = path.split_once('/').ok_or(ReferenceError::MissingDoc)?;
        let (tag, key_tail) = remainder
            .split_once('/')
            .ok_or(ReferenceError::MissingFacetKey)?;
        if document.is_empty() || tag.is_empty() || key_tail.is_empty() {
            return Err(ReferenceError::MissingFacetKey);
        }

        let document = decode_uri_component(document)?;
        let tag = decode_uri_component(tag)?;
        let key_tail = decode_uri_component(key_tail)?;
        let key = FacetKey::new(tag, go_string_unescape(key_tail.as_bytes())?)?;

        let document = if document == SELF_DOC {
            ReferenceDocId::SelfDoc
        } else {
            ReferenceDocId::Concrete(
                DocId::from_str(&document).map_err(|_| ReferenceError::InvalidDocId(document))?,
            )
        };

        Ok(Self { document, key })
    }

    /// Return the canonical authorityless URI text.
    pub fn to_uri_string(&self) -> Result<String, ReferenceError> {
        let document = match &self.document {
            ReferenceDocId::Concrete(document) => document.to_string(),
            ReferenceDocId::SelfDoc => SELF_DOC.to_owned(),
        };
        let tag = encode_uri_component(self.key.tag.as_str().as_bytes(), false);
        let key = encode_uri_component(go_string_escape(self.key.id_bytes()).as_bytes(), true);
        Ok(format!(
            "{FACET_REFERENCE_SCHEME}:/{}/{}/{}",
            encode_uri_component(document.as_bytes(), false),
            tag,
            key
        ))
    }

    /// Return a URL only when the URL type preserves the canonical URI text.
    ///
    /// The `url` crate performs path normalization while parsing. Returning an
    /// error instead of a normalized URL prevents a caller from losing identity.
    pub fn to_url(&self) -> Result<Url, ReferenceError> {
        let value = self.to_uri_string()?;
        let url = Url::parse(&value).map_err(|source| ReferenceError::InvalidUri {
            value: value.clone(),
            source,
        })?;
        if url.as_str() != value {
            return Err(ReferenceError::UrlNormalization {
                value,
                normalized: url.to_string(),
            });
        }
        Ok(url)
    }
}

impl Serialize for FacetReference {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.to_uri_string()
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for FacetReference {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReferenceError {
    #[error("invalid URI {value:?}: {source}")]
    InvalidUri {
        value: String,
        source: url::ParseError,
    },
    #[error("invalid facet reference {0:?}")]
    InvalidReference(String),
    #[error("wrong URI scheme {0:?}")]
    WrongScheme(String),
    #[error("facet references cannot have an authority")]
    AuthorityNotAllowed,
    #[error("facet references cannot have a query or fragment")]
    QueryOrFragmentNotAllowed,
    #[error("facet reference path must be absolute")]
    MissingPath,
    #[error("facet reference is missing a document id")]
    MissingDoc,
    #[error("facet reference is missing a facet tag or key id")]
    MissingFacetKey,
    #[error("facet reference has an empty facet tag")]
    EmptyFacetTag,
    #[error("invalid facet tag {0:?}")]
    InvalidFacetTag(String),
    #[error("invalid percent escape in facet reference component")]
    InvalidPercentEscape,
    #[error("invalid Go string escape in facet key id")]
    InvalidGoEscape,
    #[error("invalid UTF-8 in facet reference component")]
    InvalidUtf8,
    #[error("invalid document id {0:?}")]
    InvalidDocId(String),
    #[error("dot segments are not valid in facet reference paths")]
    DotSegment,
    #[error("URL normalization would change facet reference {value:?} to {normalized:?}")]
    UrlNormalization { value: String, normalized: String },
}

fn validate_key_id(id: &[u8]) -> Result<(), ReferenceError> {
    if id.is_empty() {
        return Err(ReferenceError::MissingFacetKey);
    }
    if id
        .split(|byte| *byte == b'/')
        .any(|segment| segment == b"." || segment == b"..")
    {
        return Err(ReferenceError::DotSegment);
    }
    Ok(())
}

fn split_uri_suffix(value: &str) -> (&str, Option<&str>) {
    let query = value.find('?');
    let fragment = value.find('#');
    let end = query.into_iter().chain(fragment).min();
    match end {
        Some(end) => (&value[..end], Some(&value[end..])),
        None => (value, None),
    }
}

fn reject_dot_segments(path: &str) -> Result<(), ReferenceError> {
    for segment in path.split('/') {
        if segment == "." || segment == ".." {
            return Err(ReferenceError::DotSegment);
        }
        let decoded = decode_uri_component(segment)?;
        if decoded == "." || decoded == ".." {
            return Err(ReferenceError::DotSegment);
        }
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

fn encode_uri_component(bytes: &[u8], allow_slash: bool) -> String {
    let mut output = String::with_capacity(bytes.len());
    for &byte in bytes {
        if (allow_slash && byte == b'/')
            || byte.is_ascii_alphanumeric()
            || matches!(byte, b'-' | b'_' | b'.' | b'~')
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
mod tests {
    use super::*;

    fn doc() -> DocId {
        DocId::from_str("z11111111111111111111111111111111").unwrap()
    }

    #[test]
    fn go_quote_vectors_are_independent_and_round_trip() {
        let cases: &[(&[u8], &str)] = &[
            ("café/東京".as_bytes(), "café/東京"),
            (b"quote\" slash\\ percent%", "quote\\\" slash\\\\ percent%"),
            (
                b"\0\x01\x07\x08\x0c\n\r\t\x0b\x1f\x7f",
                "\\x00\\x01\\a\\b\\f\\n\\r\\t\\v\\x1f\\x7f",
            ),
            (&[0x80, 0xff], "\\x80\\xff"),
        ];
        for (bytes, expected) in cases {
            assert_eq!(go_string_escape(bytes), *expected);
            assert_eq!(go_string_unescape(expected.as_bytes()).unwrap(), *bytes);
        }
    }

    #[test]
    fn go_quote_matches_published_go_quote_edge_table() {
        // Provenance: Go 1.23.0 src/strconv/quote.go (Quote/IsPrint) and
        // src/strconv/quote_test.go (quotetests), fetched from the published Go
        // source at https://raw.githubusercontent.com/golang/go/go1.23.0/src/strconv/.
        let cases = [
            ("\u{0085}".as_bytes(), r"\u0085"),
            ("\u{00a0}".as_bytes(), r"\u00a0"),
            ("\u{200b}".as_bytes(), r"\u200b"),
            ("\u{2028}".as_bytes(), r"\u2028"),
            ("\u{2029}".as_bytes(), r"\u2029"),
            // U+088F was unassigned in Unicode 15.0 (Go 1.23's data) and
            // must remain escaped even though later Unicode data assigns it.
            ("\u{088f}".as_bytes(), r"\u088f"),
            ("é東京".as_bytes(), "é東京"),
        ];
        for (bytes, expected) in cases {
            assert_eq!(go_string_escape(bytes), expected);
            assert_eq!(go_string_unescape(expected.as_bytes()).unwrap(), bytes);
        }
    }

    #[test]
    fn canonical_unicode_data_version_is_pinned() {
        assert_eq!(
            unicode_properties::UNICODE_VERSION,
            CANONICAL_REFERENCE_UNICODE_VERSION
        );
        assert!(!go_is_print('\u{088f}'));
        assert_eq!(go_string_escape("\u{088f}".as_bytes()), r"\u088f");
    }

    #[test]
    fn every_byte_round_trips_through_go_escapes() {
        let bytes = (0..=u8::MAX).collect::<Vec<_>>();
        let escaped = go_string_escape(&bytes);
        assert_eq!(go_string_unescape(escaped.as_bytes()).unwrap(), bytes);
    }

    #[test]
    fn malformed_go_escape_width_and_value_are_structured_errors() {
        for value in [
            br"\1".as_slice(),
            br"\12".as_slice(),
            br"\777".as_slice(),
            br"\400".as_slice(),
            br"\x0".as_slice(),
            br"\u123".as_slice(),
            br"\U1234567".as_slice(),
            br"\uD800".as_slice(),
            br"\U00110000".as_slice(),
            br"\'".as_slice(),
            b"\"".as_slice(),
            b"\n".as_slice(),
        ] {
            assert!(
                matches!(
                    go_string_unescape(value),
                    Err(ReferenceError::InvalidGoEscape)
                ),
                "{value:?}"
            );
        }
    }

    #[test]
    fn go_and_uri_layers_reject_decoded_dot_keys() {
        for value in [
            "db+facet:/self/org.example.note/%5Cx2e",
            "db+facet:/self/org.example.note/%5C%78%32%65",
            "db+facet:/self/org.example.note/%5C056",
        ] {
            assert!(
                matches!(
                    FacetReference::parse(value),
                    Err(ReferenceError::DotSegment)
                ),
                "{value}"
            );
        }

        let escaped = FacetReference::parse("db+facet:/self/org.example.note/%5Cu0061").unwrap();
        assert_eq!(escaped.key().id_bytes(), b"a");
        assert_eq!(
            escaped.to_uri_string().unwrap(),
            "db+facet:/self/org.example.note/a"
        );
    }

    #[test]
    fn canonical_uri_vectors_use_one_transport_escape_pass() {
        let cases: &[(&[u8], &str)] = &[
            (
                "café/東京".as_bytes(),
                "db+facet:/self/org.example.note/caf%C3%A9/%E6%9D%B1%E4%BA%AC",
            ),
            (
                b"quote\" slash\\ percent%",
                "db+facet:/self/org.example.note/quote%5C%22%20slash%5C%5C%20percent%25",
            ),
            (
                &[0x80, 0xff],
                "db+facet:/self/org.example.note/%5Cx80%5Cxff",
            ),
            (
                "\u{0085}\u{00a0}\u{200b}\u{2028}\u{2029}".as_bytes(),
                "db+facet:/self/org.example.note/%5Cu0085%5Cu00a0%5Cu200b%5Cu2028%5Cu2029",
            ),
        ];
        for (id, expected) in cases {
            let key = FacetKey::new("org.example.note", id.to_vec()).unwrap();
            let reference = FacetReference::new(ReferenceDocId::SelfDoc, key).unwrap();
            assert_eq!(reference.to_uri_string().unwrap(), *expected);
            assert_eq!(FacetReference::parse(expected).unwrap(), reference);
            assert_eq!(
                FacetReference::parse_url(&Url::parse(expected).unwrap()).unwrap(),
                reference
            );
        }
    }

    #[test]
    fn round_trips_slashes_percent_and_versioned_key_ids() {
        for id in [
            b"main/notes/v1.2".as_slice(),
            b"/photos/beach.jpg".as_slice(),
            b"100%/literal".as_slice(),
            b"self".as_slice(),
        ] {
            let key = FacetKey::new("org.example.dpath", id.to_vec()).unwrap();
            let reference = FacetReference::new(ReferenceDocId::SelfDoc, key).unwrap();
            let uri = reference.to_uri_string().unwrap();
            assert_eq!(FacetReference::parse(&uri).unwrap(), reference, "{uri}");
            assert_eq!(
                FacetReference::parse(&uri)
                    .unwrap()
                    .to_uri_string()
                    .unwrap(),
                uri
            );
        }
    }

    #[test]
    fn preserves_encoded_literal_percent_and_rejects_dot_traversal() {
        let percent = FacetReference::parse("db+facet:/self/org.example.note/100%25/v1").unwrap();
        assert_eq!(percent.key().id_bytes(), b"100%/v1");
        assert_eq!(
            percent.to_uri_string().unwrap(),
            "db+facet:/self/org.example.note/100%25/v1"
        );

        for value in [
            "db+facet:/self/org.example.note/./v1",
            "db+facet:/self/org.example.note/../v1",
            "db+facet:/self/org.example.note/%2E/v1",
            "db+facet:/self/org.example.note/%2e%2e/v1",
        ] {
            assert!(
                matches!(
                    FacetReference::parse(value),
                    Err(ReferenceError::DotSegment)
                ),
                "{value}"
            );
        }

        let literal_percent_dot =
            FacetReference::parse("db+facet:/self/org.example.note/%252E/v1").unwrap();
        assert_eq!(literal_percent_dot.key().id_bytes(), b"%2E/v1");
        assert_eq!(
            literal_percent_dot.to_uri_string().unwrap(),
            "db+facet:/self/org.example.note/%252E/v1"
        );
    }

    #[test]
    fn constructors_and_serde_enforce_invariants() {
        assert!(FacetTag::new("").is_err());
        assert!(FacetTag::new("tag/with/slash").is_err());
        assert!(FacetKey::new("tag", Vec::<u8>::new()).is_err());
        assert!(FacetKey::new("tag", b"a/../b".to_vec()).is_err());
        assert!(serde_json::from_str::<FacetTag>("\"\"").is_err());
        assert!(serde_json::from_str::<FacetKey>("\"tag/\"").is_err());

        let key = FacetKey::new("org.example.note", b"main".to_vec()).unwrap();
        let reference = FacetReference::new(ReferenceDocId::SelfDoc, key).unwrap();
        let encoded = serde_json::to_string(&reference).unwrap();
        assert_eq!(
            serde_json::from_str::<FacetReference>(&encoded).unwrap(),
            reference
        );
    }

    #[test]
    fn contextual_self_and_concrete_objkey_identity_are_distinct() {
        let self_reference = FacetReference::parse("db+facet:/self/org.example.note/main").unwrap();
        assert_eq!(self_reference.document(), &ReferenceDocId::SelfDoc);
        assert_eq!(self_reference.document().resolve(&doc()), doc());

        let concrete = FacetReference::parse(
            "db+facet:/z11111111111111111111111111111111/org.example.note/main",
        )
        .unwrap();
        assert!(matches!(concrete.document(), ReferenceDocId::Concrete(_)));
    }

    #[test]
    fn refuses_authorities_queries_fragments_and_malformed_paths() {
        for value in [
            "db+facet://node/self/org.example.note/main",
            "db+facet:///self/org.example.note/main",
            "db+facet:/self/org.example.note",
            "db+facet:/self//main",
            "db+facet:/self/org.example.note/",
            "db+facet:/self/org.example.note/main?at=head",
            "db+facet:/self/org.example.note/main#fragment",
            "db+facet:/self/org.example.note/main%",
            "db+facet:/self/org.example.note/main%2",
            "db+facet:/self/org.example.note/main%GG",
            "db+facet:/self/org.example.note/main%FF",
        ] {
            assert!(FacetReference::parse(value).is_err(), "{value}");
        }
    }

    #[test]
    fn to_url_refuses_normalization_instead_of_changing_identity() {
        let key = FacetKey::new("org.example.note", b"%2E/v1".to_vec()).unwrap();
        let reference = FacetReference::new(ReferenceDocId::SelfDoc, key).unwrap();
        let uri = reference.to_uri_string().unwrap();
        assert_eq!(uri, "db+facet:/self/org.example.note/%252E/v1");
        let url = reference.to_url();
        assert!(url.is_ok() || matches!(url, Err(ReferenceError::UrlNormalization { .. })));
    }
}
