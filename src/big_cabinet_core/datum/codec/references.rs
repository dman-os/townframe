//! Extracting the references a schema declares out of a datum value.
//!
//! A position holds a reference when its Lexicon type refers to one of the reference defs
//! (`org.example.daybook.defs#datumRef`, `#datumLocation`, `#datumPin`, `#keyRef`; ADR 035 §2.6, §2.9), the way an AT
//! Protocol record refers to `com.atproto.repo.strongRef`. The compiler stamps every compiled
//! definition with the Lexicon identity it came from, so extraction is a co-walk of value and schema
//! like the codec's own that reads that stamp: only a declared position produces an edge, and a
//! string that happens to spell a `dtxt:datum:` address where nothing declares a reference is data.
//! The value was checked on the way in; what a peer wrote may not have been, so a position whose
//! schema does not resolve for the value, and a value that does not parse, are skipped rather than
//! refusing the rest of the walk.

use super::Codec;
use super::CodecError;
use super::validation::CompiledSchema;
use super::walk::Kind;
use super::walk::Position;
use super::walk::discriminant_matches;
use crate::DatumKey;
use crate::DatumPin;
use crate::DatumReference;
use crate::KeyReference;
use crate::Location;
use crate::datum::input::DEF;
use crate::datum::value::valid_format;
use crate::reference::DATUM_LOCATION;
use crate::reference::DATUM_PIN;
use crate::reference::DATUM_REF;
use crate::reference::KEY_REF;
use automerge::ChangeHash;
use serde_json::Value;

/// How deep a value may nest before extraction stops looking. A value checked by [`Stored`](super::Stored)
/// is within the same bound; a deeper one a peer wrote is not walked into.
const DEPTH: usize = 1024;

/// Which reference def a compiled position resolves to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReferenceForm {
    /// `#datumRef`: a whole datum, a string with no query and no fragment.
    Datum,
    /// `#datumLocation`: a position inside a datum, a string that may carry a fragment.
    Location,
    /// `#datumPin`: `{ "ref": <location>, "heads"?: [<head>…] }`.
    Pin,
    /// `#keyRef`: `{ "key": "<tag>/<slot>" }`, a content-addressed datum named without a document.
    Key,
}

impl ReferenceForm {
    /// The reference def `schema` was compiled from, if it is one.
    pub(crate) fn of(schema: &Value) -> Option<Self> {
        match schema.get(DEF)?.as_str()? {
            DATUM_REF => Some(Self::Datum),
            DATUM_LOCATION => Some(Self::Location),
            DATUM_PIN => Some(Self::Pin),
            KEY_REF => Some(Self::Key),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Datum => "datumRef",
            Self::Location => "datumLocation",
            Self::Pin => "datumPin",
            Self::Key => "keyRef",
        }
    }

    /// What `value` addresses: for a datum reference, the datum without its pin and the heads it
    /// is pinned at (empty: the current frontier); for a key reference, the key. A location's
    /// fragment is a position inside that datum, not part of its identity, so it stays in the value
    /// rather than in what is returned.
    pub(crate) fn read(self, value: &Value) -> Result<ReferenceTarget, CodecError> {
        let refused = |reason: String| CodecError::NotADatumReference {
            form: self.name(),
            stated: value.to_string(),
            reason,
        };
        // The reference defs state `format: "uri"`, which the compiler leaves out of the carried
        // constraint (see its handling of `DEF`) so that it is checked here: the parsers below
        // accept text a URI may not hold (an unencoded `{` or `"` in a fragment), and checking
        // the format in the carried constraint would refuse such a value as a bare format
        // violation, or as an ambiguity under a nullable union, instead of naming the form.
        let uri = |text: &str| {
            if valid_format("uri", text) {
                Ok(())
            } else {
                Err(refused(
                    "it is not a valid URI: characters a URI cannot hold (a space, `{`, `\"`, \
                     `<`, ...) must be percent-encoded"
                        .to_owned(),
                ))
            }
        };
        let stated = || {
            let text = value
                .as_str()
                .ok_or_else(|| refused("it is a `dtxt:datum:` address string".to_owned()))?;
            uri(text)?;
            Ok::<_, CodecError>(text)
        };
        let (reference, heads) = match self {
            Self::Datum => (
                DatumReference::parse_stored(stated()?)
                    .map_err(|error| refused(error.to_string()))?,
                Vec::new(),
            ),
            Self::Location => (
                Location::parse_stored(stated()?)
                    .map_err(|error| refused(error.to_string()))?
                    .into_reference(),
                Vec::new(),
            ),
            Self::Pin => {
                // The pin's `ref` position is not walked into, so its format is checked here.
                if let Some(location) = value.get("ref").and_then(Value::as_str) {
                    uri(location)?;
                }
                let pinned = serde_json::from_value::<DatumPin>(value.clone())
                    .map_err(|error| refused(error.to_string()))?
                    .into_location()
                    .into_reference();
                let heads = pinned.heads().to_vec();
                (pinned, heads)
            }
            Self::Key => {
                return serde_json::from_value::<KeyReference>(value.clone())
                    .map(|reference| ReferenceTarget::Key(reference.key))
                    .map_err(|error| refused(error.to_string()));
            }
        };
        Ok(ReferenceTarget::Datum {
            reference: DatumReference::new(reference.document().clone(), reference.key().clone()),
            heads,
        })
    }
}

/// What a declared reference addresses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReferenceTarget {
    /// A datum in a document, unpinned. A location's fragment is a position inside that datum and
    /// not part of its identity, so it is left at the reference's path for the reader rather than
    /// indexed.
    Datum {
        reference: DatumReference,
        /// The frontier a `#datumPin` pins. Empty means unpinned: the reference follows the
        /// target's current frontier.
        heads: Vec<ChangeHash>,
    },
    /// A content-addressed datum named by its key alone (`#keyRef`, ADR 035 §2.9): no document, no
    /// heads, because the content cannot change under its key.
    Key(DatumKey),
}

/// One reference found at the position its schema declares it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeclaredReference {
    /// Where the reference sits in the datum value, as a JSON pointer (`""` is the root).
    pub path: String,
    pub target: ReferenceTarget,
}

/// Every reference a value states at a position its schema declares as one.
pub fn declared_references(schema: &CompiledSchema, value: &Value) -> Vec<DeclaredReference> {
    let mut found = Vec::new();
    let mut path = Vec::new();
    references(
        &Position::new(schema.document(), Some(schema.document())),
        value,
        &mut path,
        &mut found,
        0,
    );
    found
}

fn references<'a>(
    position: &Position<'a>,
    value: &'a Value,
    path: &mut Vec<Segment<'a>>,
    found: &mut Vec<DeclaredReference>,
    depth: usize,
) {
    if depth > DEPTH {
        return;
    }
    let resolved = position.resolved(
        Kind::of_json(value),
        |key, declaration| {
            Ok::<bool, CodecError>(discriminant_matches(
                value.get(key).and_then(Value::as_str),
                declaration,
            ))
        },
        |error| error,
    );
    let Ok(Some(schema)) = resolved else {
        // Undeclared positions declare nothing beneath them either, and a position whose schema
        // this value cannot narrow is not one extraction can say anything about.
        return;
    };
    if let Some(form) = ReferenceForm::of(schema) {
        if let Ok(target) = form.read(value) {
            found.push(DeclaredReference {
                path: pointer(path),
                target,
            });
        }
        return;
    }
    if let Ok(Some(_)) = Codec::of(schema) {
        // Every storage form is a scalar at this position; nothing declared sits beneath it.
        return;
    }
    match value {
        Value::Object(members) => {
            for (key, member) in members {
                path.push(Segment::Key(key));
                references(
                    &position.member(Some(schema), key),
                    member,
                    path,
                    found,
                    depth + 1,
                );
                path.pop();
            }
        }
        Value::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                path.push(Segment::Index(index));
                references(
                    &position.element(Some(schema), index),
                    item,
                    path,
                    found,
                    depth + 1,
                );
                path.pop();
            }
        }
        _ => {}
    }
}

enum Segment<'a> {
    Key(&'a str),
    Index(usize),
}

/// The JSON pointer of a position: a member's `~` and `/` are escaped, an element is its index.
fn pointer(path: &[Segment<'_>]) -> String {
    let mut rendered = String::new();
    for segment in path {
        rendered.push('/');
        match segment {
            Segment::Key(key) => {
                for character in key.chars() {
                    match character {
                        '~' => rendered.push_str("~0"),
                        '/' => rendered.push_str("~1"),
                        character => rendered.push(character),
                    }
                }
            }
            Segment::Index(index) => rendered.push_str(&index.to_string()),
        }
    }
    rendered
}

#[cfg(test)]
mod tests;
