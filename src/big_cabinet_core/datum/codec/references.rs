//! Extracting the references a schema declares out of a datum value.
//!
//! A position holds a reference when its Lexicon type refers to one of the reference defs
//! (`org.example.daybook.defs#datumRef`, `#datumLocation`, `#datumPin`; ADR 035 §2.6), the way an AT
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
use crate::DatumPin;
use crate::DatumReference;
use crate::Location;
use crate::datum::input::DEF;
use crate::datum::value::valid_format;
use crate::reference::DATUM_LOCATION;
use crate::reference::DATUM_PIN;
use crate::reference::DATUM_REF;
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
}

impl ReferenceForm {
    /// The reference def `schema` was compiled from, if it is one.
    pub(crate) fn of(schema: &Value) -> Option<Self> {
        match schema.get(DEF)?.as_str()? {
            DATUM_REF => Some(Self::Datum),
            DATUM_LOCATION => Some(Self::Location),
            DATUM_PIN => Some(Self::Pin),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Datum => "datumRef",
            Self::Location => "datumLocation",
            Self::Pin => "datumPin",
        }
    }

    /// The datum `value` addresses, without its pin, and the heads it is pinned at (empty: the
    /// current frontier). A location's fragment is a position inside that datum, not part of its
    /// identity, so it stays in the value rather than in what is returned.
    pub(crate) fn read(
        self,
        value: &Value,
    ) -> Result<(DatumReference, Vec<ChangeHash>), CodecError> {
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
        };
        Ok((
            DatumReference::new(reference.document().clone(), reference.key().clone()),
            heads,
        ))
    }
}

/// One reference found at the position its schema declares it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeclaredReference {
    /// Where the reference sits in the datum value, as a JSON pointer (`""` is the root).
    pub path: String,
    /// The datum the value addresses, unpinned. A location's fragment is a position inside that
    /// datum and not part of its identity, so it is left at `path` for the reader rather than
    /// indexed.
    pub reference: DatumReference,
    /// The frontier a `#datumPin` pins. Empty means unpinned: the reference follows the target's
    /// current frontier.
    pub heads: Vec<ChangeHash>,
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
        if let Ok((reference, heads)) = form.read(value) {
            found.push(DeclaredReference {
                path: pointer(path),
                reference,
                heads,
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
mod tests {
    use super::*;
    use crate::reference::head_text;
    use schemars::JsonSchema;
    use serde::Deserialize;
    use serde::Serialize;
    use serde_json::json;
    use std::collections::BTreeMap;

    #[macros::datum(authority = "org.example")]
    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    #[serde(rename_all = "camelCase")]
    struct Reffed {
        link: DatumReference,
        pinned: DatumPin,
        nested: Nested,
        #[serde(default)]
        listed: Vec<DatumReference>,
        // A name a JSON pointer has to escape.
        #[serde(rename = "odd/key~")]
        odd: DatumReference,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        maybe: Option<DatumReference>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        at: Option<Location>,
        #[serde(flatten)]
        extra: crate::datum::Extra,
    }

    #[macros::datum]
    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    struct Nested {
        inner: DatumReference,
        #[serde(flatten)]
        extra: crate::datum::Extra,
    }

    /// The schema a node compiles for `Reffed`: its projected Lexicon, admitted with the reference
    /// defs it refers to, which is what stamps the reference positions.
    fn compiled() -> (CompiledSchema, String) {
        let universe = crate::datum::Universe::collect(vec![
            crate::datum::Declaration::of::<Reffed>().unwrap(),
        ]);
        let bundle = crate::datum::Bundle::new(&universe, Vec::new());
        let lexicons = bundle.lexicons().unwrap();
        let defs = crate::reference::defs_lexicon();
        let id = universe.datums[0].id.clone();
        let compiled = bundle
            .validate_lexicon(&id, &lexicons[0].file, &[&defs])
            .unwrap();
        (compiled, id)
    }

    fn reference(suffix: &str) -> String {
        format!("dtxt:datum:self/org.example.note/{suffix}")
    }

    fn pin(suffix: &str, heads: &[u8]) -> Value {
        json!({
            "ref": reference(suffix),
            "heads": heads.iter().map(|byte| head_text(&ChangeHash([*byte; 32]))).collect::<Vec<_>>(),
        })
    }

    #[test]
    fn schema_declares_references_only_by_referring_to_the_defs() {
        assert!(compiled().0.declares_references());
        let plain = CompiledSchema::compile(json!({"type": "string"})).unwrap();
        assert!(!plain.declares_references());
    }

    #[test]
    fn extraction_finds_declared_positions_with_paths_and_pins() {
        let (schema, id) = compiled();
        let value = json!({
            "$type": id,
            "link": reference("main"),
            "pinned": pin("pinned", &[2, 1]),
            "nested": {"inner": reference("inner")},
            "listed": [reference("first"), reference("second")],
            "odd/key~": reference("odd"),
            "maybe": null,
            "at": format!("{}#text/@3", reference("located")),
        });
        let found = declared_references(&schema, &value);
        let by_path: BTreeMap<&str, &DeclaredReference> = found
            .iter()
            .map(|edge| (edge.path.as_str(), edge))
            .collect();
        assert_eq!(found.len(), 7, "{by_path:?}");
        // A location is indexed by the datum it is inside; its fragment stays in the value.
        assert_eq!(
            by_path["/at"].reference,
            DatumReference::parse(&reference("located")).unwrap()
        );
        assert_eq!(
            by_path["/link"].reference,
            DatumReference::parse(&reference("main")).unwrap()
        );
        // A pin is read from its own object, in hash-byte order whatever order it was written in.
        assert_eq!(
            by_path["/pinned"].reference,
            DatumReference::parse(&reference("pinned")).unwrap()
        );
        assert_eq!(
            by_path["/pinned"].heads,
            vec![ChangeHash([1; 32]), ChangeHash([2; 32])]
        );
        assert_eq!(by_path["/nested/inner"].path, "/nested/inner");
        assert_eq!(by_path["/listed/0"].path, "/listed/0");
        assert_eq!(by_path["/listed/1"].path, "/listed/1");
        // A member's name is pointer-escaped.
        assert_eq!(by_path["/odd~1key~0"].path, "/odd~1key~0");
        // Only a `datumPin` pins: every other position follows the target's current frontier.
        for (path, edge) in by_path {
            if path != "/pinned" {
                assert!(edge.heads.is_empty(), "{path}");
            }
        }
    }

    #[test]
    fn a_reference_round_trips_through_document_storage() {
        let (schema, id) = compiled();
        let mut doc = automerge::Automerge::new();
        let tx = doc.transaction();
        let tx = crate::datum::reconcile_json_atomic(
            tx,
            &schema,
            &automerge::ROOT,
            "datum".into(),
            json!({
                "$type": id,
                "link": reference("main"),
                "pinned": pin("pinned", &[1]),
                "nested": {"inner": reference("inner")},
                "listed": [],
                "odd/key~": reference("odd"),
                "maybe": null,
            }),
        )
        .unwrap();
        tx.commit();
        let hydrated =
            crate::datum::hydrate_value(&doc, &automerge::ROOT, "datum".into(), &schema).unwrap();
        let found = declared_references(&schema, &hydrated);
        assert_eq!(found.len(), 4, "{found:?}");
        let mut paths: Vec<_> = found.iter().map(|edge| edge.path.as_str()).collect();
        paths.sort_unstable();
        assert_eq!(paths, ["/link", "/nested/inner", "/odd~1key~0", "/pinned"]);
        let pinned = found.iter().find(|edge| edge.path == "/pinned").unwrap();
        assert_eq!(pinned.heads, vec![ChangeHash([1; 32])]);
    }

    #[test]
    fn extraction_skips_undeclared_and_unparseable_positions() {
        let (schema, id) = compiled();
        // A string that spells an address where nothing declares a reference is data.
        let plain = json!({"type": "object", "properties": {"plain": {"type": "string"}}});
        let value =
            json!({"plain": reference("sniffed"), "unknown": {"nested": reference("hidden")}});
        assert!(declared_references(&CompiledSchema::compile(plain).unwrap(), &value).is_empty());

        // What a peer wrote may not have been checked: a value that does not parse as the def its
        // position refers to is skipped, and the rest of the walk goes on.
        let value = json!({
            "$type": id,
            // A whole-datum position does not take a location's fragment.
            "link": format!("{}#text/@3", reference("main")),
            "pinned": {"ref": reference("pinned"), "heads": ["not a multibase head"]},
            "nested": {"inner": reference("inner")},
            "odd/key~": reference("odd"),
            "listed": ["not a reference"],
            "maybe": null,
        });
        let found = declared_references(&schema, &value);
        let mut paths: Vec<_> = found.iter().map(|edge| edge.path.as_str()).collect();
        paths.sort_unstable();
        assert_eq!(paths, ["/nested/inner", "/odd~1key~0"]);
    }

    /// A reference position whose value does not parse is refused as a reference, naming its path
    /// and the parse failure, required or optional: the optional field is a nullable union, whose
    /// branch resolution must not answer first with an ambiguity or a bare format violation.
    #[test]
    fn an_unparseable_reference_is_refused_as_one_at_its_path() {
        #[macros::datum(authority = "org.example", name = "refused")]
        #[derive(Debug, Serialize, Deserialize, JsonSchema)]
        struct Refused {
            at: Location,
            link: DatumReference,
            pinned: DatumPin,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            maybe_at: Option<Location>,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            maybe_link: Option<DatumReference>,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            maybe_pinned: Option<DatumPin>,
            #[serde(flatten)]
            extra: crate::datum::Extra,
        }
        let universe = crate::datum::Universe::collect(vec![
            crate::datum::Declaration::of::<Refused>().unwrap(),
        ]);
        let bundle = crate::datum::Bundle::new(&universe, Vec::new());
        let lexicons = bundle.lexicons().unwrap();
        let defs = crate::reference::defs_lexicon();
        let id = universe.datums[0].id.clone();
        let schema = bundle
            .validate_lexicon(&id, &lexicons[0].file, &[&defs])
            .unwrap();

        let good = reference("main");
        let raw_fragment = format!(r#"{good}#toolCalls/{{"id":"toolu_01"}}"#);
        let fragment = format!("{good}#text/@3");
        let complete = |member: &str, value: Value| {
            let mut whole = json!({
                "$type": id,
                "at": good,
                "link": good,
                "pinned": {"ref": good},
            });
            whole[member] = value;
            whole
        };
        const URI: &str = "not a valid URI";
        const NO_FRAGMENT: &str = "cannot have a fragment";
        let cases = [
            ("at", json!(raw_fragment), "$.at", URI),
            ("maybe_at", json!(raw_fragment), "$.maybe_at", URI),
            // a whole-datum position takes no fragment at all, however well formed
            ("link", json!(fragment), "$.link", NO_FRAGMENT),
            ("maybe_link", json!(fragment), "$.maybe_link", NO_FRAGMENT),
            ("link", json!(raw_fragment), "$.link", URI),
            ("pinned", json!({"ref": raw_fragment}), "$.pinned", URI),
            (
                "maybe_pinned",
                json!({"ref": raw_fragment}),
                "$.maybe_pinned",
                URI,
            ),
        ];
        for (member, value, path, reason) in cases {
            let error = crate::datum::Stored::check(&schema, complete(member, value.clone()))
                .expect_err("an unparseable reference is refused");
            let message = error.to_string();
            let crate::datum::CodecError::At { path: at, source } = &error else {
                panic!("{member} {value}: not refused at a path: {message}");
            };
            assert_eq!(at, path, "{message}");
            assert!(
                matches!(
                    **source,
                    crate::datum::CodecError::NotADatumReference { .. }
                ),
                "{member} {value}: {message}"
            );
            assert!(message.contains(path), "{message}");
            assert!(message.contains(reason), "{message}");
        }
    }
}
