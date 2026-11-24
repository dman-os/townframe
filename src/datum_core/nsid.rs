//! Namespaced identifiers (NSIDs), the one name format for contracts and schemas (ADR 028 §4).
//!
//! An NSID is AT Protocol syntax: a reverse-DNS authority followed by one name segment, like
//! `org.example.clay.shell`. Compatibility lives in the name: a breaking change is a new
//! NSID, so no version is part of the identifier and two equal strings are the same contract.
//! The type validates at construction, so every held [`Nsid`] is well formed and consumers
//! never re-check it; wire data that is not an NSID fails to deserialize.
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;

use crate::is_nsid;

/// A validated NSID. Cloning shares the text.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Nsid(Arc<str>);

/// Why a string is not an NSID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvalidNsid {
    text: String,
}

impl fmt::Display for InvalidNsid {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?} is not a valid NSID", self.text)
    }
}

impl std::error::Error for InvalidNsid {}

impl Nsid {
    /// Validate `text` with [`is_nsid`] (the AT Protocol NSID grammar) and keep the text.
    pub fn parse(text: &str) -> Result<Self, InvalidNsid> {
        if !is_nsid(text) {
            return Err(InvalidNsid {
                text: text.to_owned(),
            });
        }
        Ok(Self(text.into()))
    }

    /// An NSID written as a literal in source. A malformed literal is a programming error.
    pub fn literal(text: &str) -> Self {
        Self::parse(text).unwrap_or_else(|error| panic!("{error}"))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Whether this NSID lies under `authority`, a reverse-DNS prefix of whole segments: the
    /// prefix followed by a dot (ADR 028 §5). `org.example.counter` holds
    /// `org.example.counter.count` but not `org.example.counterfeit.count`.
    pub fn is_under(&self, authority: &str) -> bool {
        self.0
            .strip_prefix(authority)
            .is_some_and(|rest| rest.starts_with('.'))
    }
}

impl FromStr for Nsid {
    type Err = InvalidNsid;

    fn from_str(text: &str) -> Result<Self, InvalidNsid> {
        Self::parse(text)
    }
}

impl TryFrom<String> for Nsid {
    type Error = InvalidNsid;

    fn try_from(text: String) -> Result<Self, InvalidNsid> {
        Self::parse(&text)
    }
}

impl From<Nsid> for String {
    fn from(nsid: Nsid) -> Self {
        nsid.0.as_ref().to_owned()
    }
}

impl crate::DatumShape for Nsid {
    const SHAPE: crate::Shape = crate::Shape::Value;
}

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for Nsid {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Nsid".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        "datum_core::Nsid".into()
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({"type": "string"})
    }
}
impl fmt::Display for Nsid {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl fmt::Debug for Nsid {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Nsid({:?})", &*self.0)
    }
}

/// A reference to one definition of a Lexicon, `nsid#def` (ADR 050 §2). A bare NSID is the file's
/// `main`, and `nsid#main` is the same reference, so the two spellings are one value and the
/// canonical text omits `#main`. A collection definition is such a reference to a `token` def,
/// with its row key and value as sibling defs of the same file.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct LexRef {
    /// The canonical text: the NSID, then `#def` unless the def is `main`.
    text: Arc<str>,
    nsid: Nsid,
}

impl LexRef {
    /// Parse `nsid` or `nsid#def`, where `def` is an AT Protocol definition name: ASCII letters
    /// and digits, starting with a letter.
    pub fn parse(text: &str) -> Result<Self, InvalidNsid> {
        let invalid = || InvalidNsid {
            text: text.to_owned(),
        };
        let Some((nsid, def)) = text.split_once('#') else {
            return Nsid::parse(text).map(Self::from);
        };
        let named = def.starts_with(|first: char| first.is_ascii_alphabetic())
            && def.chars().all(|c| c.is_ascii_alphanumeric());
        if !named {
            return Err(invalid());
        }
        let nsid = Nsid::parse(nsid).map_err(|_| invalid())?;
        Ok(if def == "main" {
            Self::from(nsid)
        } else {
            Self {
                text: text.into(),
                nsid,
            }
        })
    }

    /// The canonical text, `nsid` or `nsid#def`.
    pub fn as_str(&self) -> &str {
        &self.text
    }

    /// A reference written as a literal in source. A malformed literal is a programming error.
    pub fn literal(text: &str) -> Self {
        Self::parse(text).unwrap_or_else(|error| panic!("{error}"))
    }

    /// The Lexicon file the reference points into.
    pub fn nsid(&self) -> &Nsid {
        &self.nsid
    }

    /// The definition name, `main` when the reference is a bare NSID.
    pub fn def(&self) -> &str {
        self.text.split_once('#').map_or("main", |(_, def)| def)
    }

    /// Whether this is the file's `main`: the only definition an operation or a datum names.
    pub fn is_main(&self) -> bool {
        self.text.len() == self.nsid.0.len()
    }
}

impl From<Nsid> for LexRef {
    fn from(nsid: Nsid) -> Self {
        Self {
            text: Arc::clone(&nsid.0),
            nsid,
        }
    }
}

impl FromStr for LexRef {
    type Err = InvalidNsid;

    fn from_str(text: &str) -> Result<Self, InvalidNsid> {
        Self::parse(text)
    }
}

impl TryFrom<String> for LexRef {
    type Error = InvalidNsid;

    fn try_from(text: String) -> Result<Self, InvalidNsid> {
        Self::parse(&text)
    }
}

impl From<LexRef> for String {
    fn from(reference: LexRef) -> Self {
        reference.text.as_ref().to_owned()
    }
}

impl fmt::Display for LexRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.text)
    }
}

impl fmt::Debug for LexRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "LexRef({self})")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_atproto_nsids() {
        for text in [
            "org.example.clay.shell.roots",
            "com.example.fooBar",
            "net.users.bob.ping",
            "a-0.b-1.c",
            "a.b.c",
        ] {
            assert_eq!(Nsid::parse(text).unwrap().as_str(), text);
        }
    }

    #[test]
    fn rejects_malformed_nsids() {
        for text in [
            "",
            "shell.roots",
            "settings",
            "org..roots",
            "org.example.",
            ".org.example.roots",
            "org.-example.roots",
            "org.example-.roots",
            "0rg.example.roots",
            "org.example.0roots",
            "org.example.roots-v2",
            "org.example.root_s",
            "org.exämple.roots",
            "org.example.shell.root-key",
        ] {
            assert!(Nsid::parse(text).is_err(), "{text:?} must be rejected");
        }
        assert!(Nsid::parse(&format!("org.example.{}", "a".repeat(64))).is_err());
        assert!(Nsid::parse(&format!("org.{}.name", "a".repeat(64))).is_err());
        assert!(Nsid::parse(&format!("org.example.{}", "a".repeat(63))).is_ok());
        let long_authority = vec!["a".repeat(63); 5].join(".");
        assert!(Nsid::parse(&format!("{long_authority}.name")).is_err());
    }

    #[test]
    fn authority_is_a_prefix_of_whole_segments() {
        let nsid = Nsid::literal("org.example.counter.count");
        assert!(nsid.is_under("org.example.counter"));
        assert!(nsid.is_under("org.example"));
        assert!(!nsid.is_under("org.example.count"));
        assert!(!nsid.is_under("org.example.counter.count"));
        assert!(!nsid.is_under(""));
    }

    #[test]
    fn a_reference_names_a_def_and_main_is_the_bare_nsid() {
        let token = LexRef::parse("org.example.clay.shell#roots").unwrap();
        assert_eq!(token.nsid().as_str(), "org.example.clay.shell");
        assert_eq!(token.def(), "roots");
        assert!(!token.is_main());
        assert_eq!(token.to_string(), "org.example.clay.shell#roots");

        let main = LexRef::parse("org.example.clay.shell#main").unwrap();
        assert_eq!(main, LexRef::parse("org.example.clay.shell").unwrap());
        assert_eq!(main, LexRef::from(Nsid::literal("org.example.clay.shell")));
        assert_eq!(main.to_string(), "org.example.clay.shell");
        assert_eq!(main.def(), "main");
    }

    #[test]
    fn rejects_malformed_references() {
        for text in [
            "",
            "shell#roots",
            "org.example.clay.shell#",
            "org.example.clay.shell#0roots",
            "org.example.clay.shell#ro-ots",
            "org.example.clay.shell#roots#again",
            "#roots",
        ] {
            assert!(LexRef::parse(text).is_err(), "{text:?} must be rejected");
        }
        let token: LexRef = serde_json::from_str("\"org.example.clay.shell#roots\"").unwrap();
        assert_eq!(
            serde_json::to_string(&token).unwrap(),
            "\"org.example.clay.shell#roots\""
        );
    }

    #[test]
    fn serde_validates_on_the_wire() {
        let nsid: Nsid = serde_json::from_str("\"org.example.clay.shell.roots\"").unwrap();
        assert_eq!(
            serde_json::to_string(&nsid).unwrap(),
            "\"org.example.clay.shell.roots\""
        );
        assert!(serde_json::from_str::<Nsid>("\"shell.roots\"").is_err());
    }
}
