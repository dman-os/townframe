//! Dpaths: path-shaped labels a document claims, so files imported from disk keep their paths
//! and a grant can name "documents under /projects" (FDR 001 §1).
//!
//! A dpath is UTF-8, begins with `/`, and has non-empty segments separated by single `/`. A
//! segment is never `.` or `..`, and there is no trailing `/`. The root `/` alone has no
//! segment to claim and is rejected. Comparison is byte-exact: no case folding, no Unicode
//! normalization, no reserved subtrees, no uniqueness across documents. The type validates at
//! construction, so every held [`Dpath`] is well formed and consumers never re-check it; wire
//! data that is not a dpath fails to deserialize.
//!
//! A [`Dpath`] is the validated UTF-8 label. A filename that is not UTF-8 has no `Dpath`; its
//! reversible import belongs to the datum slot layer, whose Go string-literal escaping already
//! carries arbitrary bytes (ADR 035).
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;

/// A validated dpath. Cloning shares the text.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Dpath(Arc<str>);

/// Why a string is not a dpath.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InvalidDpath {
    /// The text does not begin with `/`.
    MissingLeadingSlash,
    /// The text is the root `/` alone: no segment to claim.
    Root,
    /// The text ends with `/`.
    TrailingSlash,
    /// Segment `index` is empty (`//`).
    EmptySegment { index: usize },
    /// Segment `index` is `.` or `..`.
    DotSegment { index: usize },
}

impl fmt::Display for InvalidDpath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingLeadingSlash => formatter.write_str("a dpath must begin with '/'"),
            Self::Root => formatter.write_str("the root '/' alone is not a claimable dpath"),
            Self::TrailingSlash => formatter.write_str("a dpath must not end with '/'"),
            Self::EmptySegment { index } => {
                write!(formatter, "dpath segment {index} is empty")
            }
            Self::DotSegment { index } => {
                write!(formatter, "dpath segment {index} is '.' or '..'")
            }
        }
    }
}

impl std::error::Error for InvalidDpath {}

/// Whether `segment` may be one dpath segment: non-empty, not `.`/`..`, no `/`.
fn is_segment(segment: &str) -> bool {
    !segment.is_empty() && segment != "." && segment != ".." && !segment.contains('/')
}

impl Dpath {
    /// Validate `text` against the dpath grammar and keep the text.
    pub fn parse(text: &str) -> Result<Self, InvalidDpath> {
        let rest = text
            .strip_prefix('/')
            .ok_or(InvalidDpath::MissingLeadingSlash)?;
        if rest.is_empty() {
            return Err(InvalidDpath::Root);
        }
        if rest.ends_with('/') {
            return Err(InvalidDpath::TrailingSlash);
        }
        for (index, segment) in rest.split('/').enumerate() {
            if segment.is_empty() {
                return Err(InvalidDpath::EmptySegment { index });
            }
            if segment == "." || segment == ".." {
                return Err(InvalidDpath::DotSegment { index });
            }
        }
        Ok(Self(text.into()))
    }

    /// A dpath written as a literal in source. A malformed literal is a programming error.
    pub fn literal(text: &str) -> Self {
        Self::parse(text).unwrap_or_else(|error| panic!("{text:?}: {error}"))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The segments, outermost first. Never empty: a dpath has at least one.
    pub fn segments(&self) -> impl DoubleEndedIterator<Item = &str> {
        self.0[1..].split('/')
    }

    /// The dpath one segment up, or `None` for a top-level dpath (the root is not a dpath).
    pub fn parent(&self) -> Option<Self> {
        let cut = self.0.rfind('/').filter(|cut| *cut > 0)?;
        Some(Self(self.0[..cut].into()))
    }

    /// This dpath extended by one segment, or `None` if `segment` is empty, `.`, `..` or
    /// contains `/`.
    pub fn join(&self, segment: &str) -> Option<Self> {
        is_segment(segment).then(|| Self(format!("{}/{segment}", self.0).into()))
    }

    /// Whether `self` is `ancestor` or lies beneath it, on segment boundaries: `/a/b` is under
    /// `/a` but `/ab` is not.
    pub fn starts_with(&self, ancestor: &Dpath) -> bool {
        self.0
            .strip_prefix(&*ancestor.0)
            .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
    }
}

impl FromStr for Dpath {
    type Err = InvalidDpath;

    fn from_str(text: &str) -> Result<Self, InvalidDpath> {
        Self::parse(text)
    }
}

impl TryFrom<String> for Dpath {
    type Error = InvalidDpath;

    fn try_from(text: String) -> Result<Self, InvalidDpath> {
        Self::parse(&text)
    }
}

impl From<Dpath> for String {
    fn from(dpath: Dpath) -> Self {
        dpath.0.as_ref().to_owned()
    }
}

impl crate::DatumShape for Dpath {
    const SHAPE: crate::Shape = crate::Shape::Value;
}

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for Dpath {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Dpath".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        "datum_core::Dpath".into()
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({"type": "string"})
    }
}

impl fmt::Display for Dpath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl fmt::Debug for Dpath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Dpath({:?})", &*self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grammar_table() {
        for input in [
            "/inbox/hello.md",
            "/a",
            "/a/b/c",
            "/by-id/x",
            "/.dtree/x",
            "/tagged/urgent/2026",
            "/hello world/with spaces.md",
            "/..hidden/note",
            "/café/naïve.md",
        ] {
            assert_eq!(Dpath::parse(input).unwrap().as_str(), input);
        }

        for (input, expected) in [
            ("", InvalidDpath::MissingLeadingSlash),
            ("inbox/hello.md", InvalidDpath::MissingLeadingSlash),
            ("/", InvalidDpath::Root),
            ("/a/", InvalidDpath::TrailingSlash),
            ("/a//b", InvalidDpath::EmptySegment { index: 1 }),
            ("//a", InvalidDpath::EmptySegment { index: 0 }),
            ("/a/./b", InvalidDpath::DotSegment { index: 1 }),
            ("/a/../b", InvalidDpath::DotSegment { index: 1 }),
            ("/..", InvalidDpath::DotSegment { index: 0 }),
        ] {
            assert_eq!(Dpath::parse(input).unwrap_err(), expected, "on {input:?}");
        }
    }

    #[test]
    fn segments_parent_and_join() {
        let dpath = Dpath::literal("/DCIM/2026/06/IMG_1234.jpg");
        assert_eq!(
            dpath.segments().collect::<Vec<_>>(),
            ["DCIM", "2026", "06", "IMG_1234.jpg"]
        );
        assert_eq!(dpath.segments().next_back(), Some("IMG_1234.jpg"));
        assert_eq!(dpath.parent().unwrap().as_str(), "/DCIM/2026/06");
        assert_eq!(Dpath::literal("/a").parent(), None);
        assert_eq!(dpath.to_string(), dpath.as_str());
        assert_eq!("/a/b".parse::<Dpath>().unwrap().as_str(), "/a/b");

        let a = Dpath::literal("/a");
        assert_eq!(a.join("b").unwrap(), Dpath::literal("/a/b"));
        assert_eq!(a.join("b").unwrap().parent().unwrap(), a);
        for bad in ["", ".", "..", "b/c", "/b"] {
            assert_eq!(a.join(bad), None, "{bad:?} is not a segment");
        }
    }

    #[test]
    fn starts_with_is_on_segment_boundaries() {
        let ab = Dpath::literal("/a/b");
        assert!(ab.starts_with(&Dpath::literal("/a")));
        assert!(ab.starts_with(&ab));
        assert!(!Dpath::literal("/ab").starts_with(&Dpath::literal("/a")));
        assert!(!Dpath::literal("/a").starts_with(&ab));
        assert!(!ab.starts_with(&Dpath::literal("/b")));
        assert!(Dpath::literal("/a/b/c").starts_with(&ab));
    }

    #[test]
    fn comparison_is_byte_exact() {
        assert_ne!(
            Dpath::literal("/Inbox/Hello.md"),
            Dpath::literal("/inbox/hello.md")
        );
        assert_ne!(
            Dpath::literal("/café.md"),
            Dpath::literal("/cafe\u{301}.md")
        );
        assert!(!Dpath::literal("/Inbox/x").starts_with(&Dpath::literal("/inbox")));
    }

    #[test]
    fn surface_names_are_legal_labels() {
        for input in ["/by-id/x", "/by-id", "/.dtree", "/.dnode/state"] {
            assert!(Dpath::parse(input).is_ok(), "should accept {input}");
        }
    }

    #[test]
    fn serde_validates_on_the_wire() {
        let good = Dpath::literal("/a/b.md");
        assert_eq!(serde_json::from_str::<Dpath>("\"/a/b.md\"").unwrap(), good);
        assert_eq!(serde_json::to_string(&good).unwrap(), "\"/a/b.md\"");
        assert!(serde_json::from_str::<Dpath>("\"/a//b\"").is_err());
        assert!(serde_json::from_str::<Dpath>("\"/\"").is_err());
    }
}
