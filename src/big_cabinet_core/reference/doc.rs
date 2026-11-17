//! [`DocReference`]: `dtxt:doc:<doc id>`, a whole document with an optional pin and locator hint.

use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;
use std::str::FromStr;

use super::DocId;
use super::HEADS_KEY;
use super::REFERENCE_SCHEME;
use super::ReferenceError;
use super::head_text;
use super::parse_heads_query;
use super::strip_scheme_and_kind;
use crate::dmeta::MultibaseKey;
use automerge::ChangeHash;

const DOC_KIND: &str = "doc";
const VIA_KEY: &str = "via";

/// `dtxt:doc:<doc id>[?heads=<head>|<head>…]`: a whole document, optionally pinned (ADR 035 §3).
/// The document is always a concrete id (`self` names a datum's container, and a reference to a
/// whole document that is already the container has no use); there is no fragment. It is a text
/// form only: positions that declare datum references refuse it, and a stored pin would be a
/// sibling `heads` field (§2.8), which no position needs yet.
///
/// `?via=<node key>|<node key>` is a locator hint (ADR 035 §2.2, `doc` kind): the nodes that are
/// known to hold the document. It is never part of the reference's identity, so [`PartialEq`] and
/// [`Hash`] compare the document and heads only: two references that differ in `via` are equal,
/// and a reader that keys a map by reference finds the same entry whichever hint arrived. A caller
/// that needs the hint reads [`DocReference::via`].
#[derive(Debug, Clone)]
pub struct DocReference {
    document: DocId,
    /// Sorted and deduplicated; empty follows the document's current frontier.
    heads: Vec<ChangeHash>,
    /// Sorted and deduplicated; a hint only, ignored by equality and hashing.
    via: Vec<MultibaseKey>,
}

impl PartialEq for DocReference {
    fn eq(&self, other: &Self) -> bool {
        self.document == other.document && self.heads == other.heads
    }
}

impl Eq for DocReference {}

impl Hash for DocReference {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.document.hash(state);
        self.heads.hash(state);
    }
}

impl DocReference {
    pub fn new(document: DocId) -> Self {
        Self {
            document,
            heads: Vec::new(),
            via: Vec::new(),
        }
    }

    /// The same reference naming the nodes that hold the document, a hint that does not change
    /// its identity.
    #[must_use]
    pub fn with_via(mut self, via: impl IntoIterator<Item = MultibaseKey>) -> Self {
        self.via = via.into_iter().collect();
        self.via.sort();
        self.via.dedup();
        self
    }

    /// The nodes the reference hints at, sorted; empty when it names none.
    pub fn via(&self) -> &[MultibaseKey] {
        &self.via
    }

    /// The same document pinned to `heads`.
    #[must_use]
    pub fn pinned(mut self, heads: impl IntoIterator<Item = ChangeHash>) -> Self {
        self.heads = heads.into_iter().collect();
        self.heads.sort();
        self.heads.dedup();
        self
    }

    pub fn document(&self) -> &DocId {
        &self.document
    }

    pub fn heads(&self) -> &[ChangeHash] {
        &self.heads
    }

    /// Parse the text form. Non-canonical input (unsorted heads, a scheme in capitals) is
    /// accepted and canonicalized; a fragment is refused.
    pub fn parse(value: &str) -> Result<Self, ReferenceError> {
        let rest = strip_scheme_and_kind(value, DOC_KIND)?;
        if rest.contains('#') {
            return Err(ReferenceError::QueryOrFragmentNotAllowed);
        }
        let (document, query) = match rest.split_once('?') {
            Some((document, query)) => (document, Some(query)),
            None => (rest, None),
        };
        // The id parser is given only non-empty ASCII, as for an actor's node key.
        let document = Some(document)
            .filter(|document| !document.is_empty() && document.is_ascii())
            .ok_or(ReferenceError::MissingDoc)?;
        let document = DocId::from_str(document)
            .map_err(|_| ReferenceError::InvalidDocId(document.to_owned()))?;
        let mut heads = None;
        let mut via = None;
        for pair in query.into_iter().flat_map(|query| query.split('&')) {
            let (key, value) = pair
                .split_once('=')
                .ok_or_else(|| ReferenceError::UnsupportedQuery(pair.to_owned()))?;
            let seen = match key {
                HEADS_KEY => heads.is_some(),
                VIA_KEY => via.is_some(),
                _ => return Err(ReferenceError::UnsupportedQuery(pair.to_owned())),
            };
            if seen {
                return Err(ReferenceError::DuplicateQueryKey(key.to_owned()));
            }
            if key == HEADS_KEY {
                heads = Some(parse_heads_query(pair)?);
            } else {
                via = Some(parse_via(value)?);
            }
        }
        Ok(Self::new(document)
            .pinned(heads.unwrap_or_default())
            .with_via(via.unwrap_or_default()))
    }
}

/// The `|`-joined node keys of a `via=` value; none may be empty or malformed.
fn parse_via(value: &str) -> Result<Vec<MultibaseKey>, ReferenceError> {
    if value.is_empty() {
        return Err(ReferenceError::EmptyVia);
    }
    value
        .split('|')
        .map(|key| {
            // The multibase decoder indexes its prefix, so it is given only non-empty ASCII.
            Some(key)
                .filter(|key| !key.is_empty() && key.is_ascii())
                .and_then(|key| key.parse().ok())
                .ok_or_else(|| ReferenceError::InvalidViaKey(key.to_owned()))
        })
        .collect()
}

impl fmt::Display for DocReference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{REFERENCE_SCHEME}:{DOC_KIND}:{}", self.document)?;
        if !self.heads.is_empty() {
            // Hash-byte order, the canonical order, as for `DatumReference`.
            let heads = self.heads.iter().map(head_text).collect::<Vec<_>>();
            write!(formatter, "?{HEADS_KEY}={}", heads.join("|"))?;
        }
        if !self.via.is_empty() {
            let separator = if self.heads.is_empty() { '?' } else { '&' };
            let via = self.via.iter().map(ToString::to_string).collect::<Vec<_>>();
            write!(formatter, "{separator}{VIA_KEY}={}", via.join("|"))?;
        }
        Ok(())
    }
}

impl FromStr for DocReference {
    type Err = ReferenceError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}
