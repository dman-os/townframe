//! Namespaced identifiers (NSIDs), the one name format for contracts and schemas (ADR 028 §4).
//!
//! An NSID is AT Protocol syntax: a reverse-DNS authority followed by one name segment, like
//! `org.example.clay.shell.roots`. Compatibility lives in the name: a breaking change is a new
//! NSID, so no version is part of the identifier and two equal strings are the same contract.
//! The type validates at construction, so every held [`Nsid`] is well formed and consumers
//! never re-check it; wire data that is not an NSID fails to deserialize.
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;

/// A validated NSID. Cloning shares the text.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Nsid(Arc<str>);

/// Why a string is not an NSID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvalidNsid {
    text: String,
    reason: &'static str,
}

impl fmt::Display for InvalidNsid {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{:?} is not a valid NSID: {}",
            self.text, self.reason
        )
    }
}

impl std::error::Error for InvalidNsid {}

impl Nsid {
    const MAX_LENGTH: usize = 317;
    const MAX_AUTHORITY_LENGTH: usize = 253;
    const MAX_SEGMENT_LENGTH: usize = 63;

    /// Validate `text` per the AT Protocol NSID grammar: at most 317 characters in at least
    /// three dot-separated segments. The authority segments (all but the last) are 1 to 63
    /// ASCII letters, digits or hyphens that neither start nor end with a hyphen, the first
    /// starting with a letter, and together at most 253 characters. The name segment (the last)
    /// is 1 to 63 ASCII letters and digits starting with a letter.
    pub fn parse(text: &str) -> Result<Self, InvalidNsid> {
        let invalid = |reason| InvalidNsid {
            text: text.to_owned(),
            reason,
        };
        if text.len() > Self::MAX_LENGTH {
            return Err(invalid("longer than 317 characters"));
        }
        let segments: Vec<&str> = text.split('.').collect();
        let [authority @ .., name] = segments.as_slice() else {
            unreachable!("splitting always yields a segment");
        };
        if authority.len() < 2 {
            return Err(invalid(
                "needs an authority of two or more segments and a name",
            ));
        }
        if authority
            .iter()
            .map(|segment| segment.len() + 1)
            .sum::<usize>()
            - 1
            > Self::MAX_AUTHORITY_LENGTH
        {
            return Err(invalid("authority longer than 253 characters"));
        }
        for (position, segment) in authority.iter().enumerate() {
            let bytes = segment.as_bytes();
            if bytes.is_empty() || bytes.len() > Self::MAX_SEGMENT_LENGTH {
                return Err(invalid("authority segments are 1 to 63 characters"));
            }
            if !bytes
                .iter()
                .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'-')
            {
                return Err(invalid(
                    "authority segments are letters, digits and hyphens",
                ));
            }
            if bytes[0] == b'-' || bytes[bytes.len() - 1] == b'-' {
                return Err(invalid(
                    "authority segments do not start or end with a hyphen",
                ));
            }
            if position == 0 && !bytes[0].is_ascii_alphabetic() {
                return Err(invalid("the first segment starts with a letter"));
            }
        }
        let bytes = name.as_bytes();
        if bytes.is_empty() || bytes.len() > Self::MAX_SEGMENT_LENGTH {
            return Err(invalid("the name segment is 1 to 63 characters"));
        }
        if !bytes.iter().all(u8::is_ascii_alphanumeric) || !bytes[0].is_ascii_alphabetic() {
            return Err(invalid(
                "the name segment is letters and digits, starting with a letter",
            ));
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
    fn serde_validates_on_the_wire() {
        let nsid: Nsid = serde_json::from_str("\"org.example.clay.shell.roots\"").unwrap();
        assert_eq!(
            serde_json::to_string(&nsid).unwrap(),
            "\"org.example.clay.shell.roots\""
        );
        assert!(serde_json::from_str::<Nsid>("\"shell.roots\"").is_err());
    }
}
