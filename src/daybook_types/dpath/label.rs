//! The dpath label itself: parse/validate per FDR 001 §1.

use crate::interlude::*;

use serde::de::Error as DeError;

use crate::doc::{FacetKey, FacetTag};

/// The facet tag under which dpath claims live (FDR 001 §2).
///
/// The facet **key-id is the dpath string itself**, leading `/` included, so a
/// claim on `/inbox/hello.md` is the facet key
/// `org.example.daybook.dpath//inbox/hello.md` — see [`Dpath::facet_key`].
pub const DPATH_FACET_TAG: &str = "org.example.daybook.dpath";

/// A dpath: a label with path syntax (FDR 001 §1).
///
/// Canonical form: exactly one leading `/`, no trailing `/`, no empty
/// segments, no `.`/`..` segments. Comparison is byte-exact (`Ord` is on the
/// raw bytes); labels are otherwise *unrestricted* — there are no reserved
/// names in daybook, and `/by-id/…` is a perfectly legal dpath. Checkout-owned
/// surfaces are a separate materialization concern, not label rewriting here.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
/// camino-backed validated label (`Utf8PathBuf`): interoperates with `UserPathBuf`
/// and `derive_tree`'s real paths, while equality/hash/order stay byte-exact
/// (camino compares `as_str`). The stored string is exactly the validated label —
/// camino's rewriting path ops are deliberately not exposed here.
pub struct Dpath(camino::Utf8PathBuf);

/// The typed read validates: a `Dpath` value is always a well-formed label.
/// Unvalidated input arrives as a raw facet key-id instead — see
/// [`Dpath::parse_facet_key`] and [`crate::dpath::RawClaim`], which report
/// malformed labels rather than failing (FDR 001 §4).
impl<'de> Deserialize<'de> for Dpath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        Self::parse(&raw).map_err(DeError::custom)
    }
}

impl Dpath {
    /// Parse and validate a dpath label.
    pub fn parse(input: &str) -> Result<Self, DpathError> {
        let Some(rest) = input.strip_prefix('/') else {
            return Err(DpathError::MissingLeadingSlash {
                raw: input.to_string(),
            });
        };
        if rest.is_empty() {
            return Err(DpathError::Empty {
                raw: input.to_string(),
            });
        }
        if rest.ends_with('/') {
            return Err(DpathError::TrailingSlash {
                raw: input.to_string(),
            });
        }
        for (index, segment) in rest.split('/').enumerate() {
            if segment.is_empty() {
                return Err(DpathError::EmptySegment {
                    raw: input.to_string(),
                    index,
                });
            }
            if segment == "." || segment == ".." {
                return Err(DpathError::DotSegment {
                    raw: input.to_string(),
                    segment: segment.to_string(),
                    index,
                });
            }
        }
        Ok(Self(camino::Utf8PathBuf::from(input)))
    }

    /// The canonical dpath string, leading `/` included.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// The path segments, outermost first.
    pub fn segments(&self) -> impl Iterator<Item = &str> {
        self.0.as_str()[1..].split('/')
    }

    /// The number of segments.
    pub fn segment_count(&self) -> usize {
        self.segments().count()
    }

    /// The last segment (the claimed name at its parent).
    pub fn file_name(&self) -> &str {
        self.0.as_str()[self.0.as_str().rfind('/').unwrap_or(0) + 1..].trim_start_matches('/')
    }

    /// The extension of the last segment, if any (FDR 001 §9: a lens hint).
    pub fn extension(&self) -> Option<&str> {
        extension_of(self.file_name())
    }

    /// The parent dpath, or `None` at the top level.
    pub fn parent(&self) -> Option<Self> {
        let parent = &self.0.as_str()[..self.0.as_str().rfind('/')?];
        if parent.is_empty() {
            return None;
        }
        Self::parse(parent).ok()
    }

    /// The facet key of a claim at this dpath (FDR 001 §2).
    pub fn facet_key(&self) -> FacetKey {
        FacetKey {
            tag: FacetTag::Any(DPATH_FACET_TAG.to_string()),
            id: self.0.as_str().to_string(),
        }
    }

    /// Recover a dpath from a facet key, if the key is a dpath claim.
    ///
    /// `None` means the key is not a dpath facet; `Some(Err(..))` means it is a
    /// dpath facet whose key-id is malformed (FDR 001 §4: opinions are kept, the
    /// malformed ones are reported, never silently dropped).
    pub fn parse_facet_key(key: &FacetKey) -> Option<Result<Self, DpathError>> {
        if key.tag.to_string() != DPATH_FACET_TAG {
            return None;
        }
        Some(Self::parse(&key.id))
    }
}

impl std::str::FromStr for Dpath {
    type Err = DpathError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Self::parse(input)
    }
}

impl std::fmt::Display for Dpath {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0.as_str())
    }
}

/// Why a dpath label is not a valid dpath (FDR 001 §1).
#[derive(Debug, Clone, thiserror::Error, displaydoc::Display, PartialEq, Eq)]
pub enum DpathError {
    /// a dpath must start with '/', got {raw}
    MissingLeadingSlash { raw: String },
    /// a dpath must have at least one segment, got {raw}
    Empty { raw: String },
    /// a dpath must not end with '/', got {raw}
    TrailingSlash { raw: String },
    /// a dpath must not contain an empty segment (segment {index}), got {raw}
    EmptySegment { raw: String, index: usize },
    /// a dpath segment must not be '.' or '..' (segment {index} is {segment}), got {raw}
    DotSegment {
        raw: String,
        segment: String,
        index: usize,
    },
}

/// The extension of a name, if it has one (a leading dot is not an extension).
pub(crate) fn extension_of(name: &str) -> Option<&str> {
    let (stem, extension) = name.rsplit_once('.')?;
    if stem.is_empty() || extension.is_empty() {
        return None;
    }
    Some(extension)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dpath_grammar_table() {
        let accepted = [
            "/inbox/hello.md",
            "/a",
            "/a/b/c",
            "/by-id/x",
            "/.dtree/x",
            "/tagged/urgent/2026",
            "/hello world/with spaces.md",
            "/..hidden/note",
            "/café/naïve.md",
        ];
        for input in accepted {
            assert!(Dpath::parse(input).is_ok(), "should accept {input}");
        }

        let rejected: [(&str, DpathError); 7] = [
            ("", DpathError::MissingLeadingSlash { raw: String::new() }),
            (
                "inbox/hello.md",
                DpathError::MissingLeadingSlash {
                    raw: "inbox/hello.md".to_string(),
                },
            ),
            (
                "/",
                DpathError::Empty {
                    raw: "/".to_string(),
                },
            ),
            (
                "/a/",
                DpathError::TrailingSlash {
                    raw: "/a/".to_string(),
                },
            ),
            (
                "/a//b",
                DpathError::EmptySegment {
                    raw: "/a//b".to_string(),
                    index: 1,
                },
            ),
            (
                "/a/./b",
                DpathError::DotSegment {
                    raw: "/a/./b".to_string(),
                    segment: ".".to_string(),
                    index: 1,
                },
            ),
            (
                "/a/../b",
                DpathError::DotSegment {
                    raw: "/a/../b".to_string(),
                    segment: "..".to_string(),
                    index: 1,
                },
            ),
        ];
        for (input, expected) in rejected {
            assert_eq!(Dpath::parse(input).unwrap_err(), expected, "on {input}");
        }
    }

    #[test]
    fn dpath_accessors() {
        let dpath = Dpath::parse("/DCIM/2026/06/IMG_1234.jpg").unwrap();
        assert_eq!(dpath.as_str(), "/DCIM/2026/06/IMG_1234.jpg");
        assert_eq!(dpath.segment_count(), 4);
        assert_eq!(
            dpath.segments().collect::<Vec<_>>(),
            ["DCIM", "2026", "06", "IMG_1234.jpg"]
        );
        assert_eq!(dpath.file_name(), "IMG_1234.jpg");
        assert_eq!(dpath.extension(), Some("jpg"));
        assert_eq!(dpath.parent().unwrap().as_str(), "/DCIM/2026/06");
        assert_eq!(Dpath::parse("/a").unwrap().parent(), None);
        assert_eq!(Dpath::parse("/a/.hidden").unwrap().extension(), None);
        assert_eq!(
            Dpath::parse("/a/archive.tar.gz").unwrap().extension(),
            Some("gz")
        );
        assert_eq!(dpath.to_string(), dpath.as_str());
        assert_eq!("/a/b".parse::<Dpath>().unwrap().as_str(), "/a/b");
    }

    #[test]
    fn dpath_comparison_is_byte_exact() {
        // FDR 001 §1: no case folding, no Unicode normalization.
        let upper = Dpath::parse("/Inbox/Hello.md").unwrap();
        let lower = Dpath::parse("/inbox/hello.md").unwrap();
        assert_ne!(upper, lower);

        let composed = Dpath::parse("/café.md").unwrap();
        let decomposed = Dpath::parse("/cafe\u{301}.md").unwrap();
        assert_ne!(composed, decomposed);
    }

    #[test]
    fn checkout_surface_names_remain_legal_dpaths() {
        // Checkout-owned surface names do not reserve dpath labels.
        for input in ["/by-id/x", "/by-id", "/.dtree", "/.dnode/state"] {
            assert!(Dpath::parse(input).is_ok(), "should accept {input}");
        }
    }

    #[test]
    fn dpath_facet_key_round_trip() {
        let dpath = Dpath::parse("/inbox/hello.md").unwrap();
        let key = dpath.facet_key();
        assert_eq!(key.tag.to_string(), DPATH_FACET_TAG);
        assert_eq!(key.id, "/inbox/hello.md");
        // FDR 001 §2: one dpath facet per exact dpath string per doc — the key
        // renders as `<tag>//<dpath>` and splits back on the first '/'.
        assert_eq!(key.to_string(), "org.example.daybook.dpath//inbox/hello.md");

        let reparsed = FacetKey::from(key.to_string().as_str());
        assert_eq!(reparsed, key);
        assert_eq!(Dpath::parse_facet_key(&reparsed), Some(Ok(dpath.clone())));
    }

    #[test]
    fn foreign_and_malformed_facet_keys() {
        let other = FacetKey::from("org.example.daybook.blob/main");
        assert_eq!(Dpath::parse_facet_key(&other), None);

        let malformed = FacetKey::from("org.example.daybook.dpath//a//b");
        assert!(matches!(
            Dpath::parse_facet_key(&malformed),
            Some(Err(DpathError::EmptySegment { .. }))
        ));
    }

    #[test]
    fn typed_dpath_reads_validate_while_raw_keys_report() {
        let good = Dpath::parse("/a/b.md").unwrap();
        assert_eq!(serde_json::from_str::<Dpath>("\"/a/b.md\"").unwrap(), good);
        assert_eq!(serde_json::to_string(&good).unwrap(), "\"/a/b.md\"");

        // The typed read is validated: a malformed label cannot be a `Dpath`.
        assert!(serde_json::from_str::<Dpath>("\"/a//b\"").is_err());

        // Raw facet key-ids stay tolerated and reported (FDR 001 §4).
        let key = FacetKey::from("org.example.daybook.dpath//a//b");
        assert!(matches!(
            Dpath::parse_facet_key(&key),
            Some(Err(DpathError::EmptySegment { .. }))
        ));
    }
}
