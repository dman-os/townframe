//! Extracting the references a schema declares out of a facet value.
//!
//! A reference is a string the schema marks with the `reference` storage form, so extraction is a
//! co-walk of value and schema like the codec's own: only a declared position produces an edge,
//! and a string that happens to spell a `db+facet:` address where nothing declares a reference is
//! data, not a reference. The value was checked on the way in; what a peer wrote may not have
//! been, so a position whose schema does not resolve for the value, and a string that does not
//! parse, are skipped rather than refusing the rest of the walk.

use super::validation::CompiledSchema;
use super::walk::{Kind, Position, discriminant_matches};
use super::{Codec, CodecError};
use crate::FacetReference;
use automerge::ChangeHash;
use serde_json::Value;

/// The sibling member that pins the frontier a reference is read at (ADR 016).
const REF_HEADS: &str = "refHeads";

/// How deep a value may nest before extraction stops looking. A value checked by [`Stored`](super::Stored)
/// is within the same bound; a deeper one a peer wrote is not walked into.
const DEPTH: usize = 1024;

/// One reference found at the position its schema declares it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeclaredReference {
    /// Where the reference sits in the facet value, as a JSON pointer (`""` is the root).
    pub path: String,
    /// The address the string holds.
    pub reference: FacetReference,
    /// The frontier the sibling `refHeads` pins. Empty means unpinned: the reference follows the
    /// target's current frontier. An entry that does not parse is dropped, so a malformed sibling
    /// degrades the pin rather than the reference.
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
        None,
        &mut found,
        0,
    );
    found
}

fn references<'a>(
    position: &Position<'a>,
    value: &'a Value,
    path: &mut Vec<Segment<'a>>,
    heads: Option<&'a Value>,
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
    if let Ok(Some(codec)) = Codec::of(schema) {
        if codec == Codec::Reference
            && let Some(stated) = value.as_str()
            && let Ok(reference) = FacetReference::parse(stated)
        {
            found.push(DeclaredReference {
                path: pointer(path),
                reference,
                heads: ref_heads(heads),
            });
        }
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
                    members.get(REF_HEADS),
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
                    None,
                    found,
                    depth + 1,
                );
                path.pop();
            }
        }
        _ => {}
    }
}

/// The pin a sibling `refHeads` states: an array of change-hash hex strings. Anything else, and
/// any entry that does not parse, is not a pin.
fn ref_heads(stated: Option<&Value>) -> Vec<ChangeHash> {
    stated
        .and_then(Value::as_array)
        .map(|heads| {
            heads
                .iter()
                .filter_map(Value::as_str)
                .filter_map(|head| head.parse().ok())
                .collect()
        })
        .unwrap_or_default()
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
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::str::FromStr;

    #[macros::facet(id = "org.example.reffed", closed)]
    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct Reffed {
        link: FacetReference,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ref_heads: Option<Vec<String>>,
        nested: Nested,
        #[serde(default)]
        listed: Vec<FacetReference>,
        #[serde(default)]
        mapped: BTreeMap<String, FacetReference>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        maybe: Option<FacetReference>,
    }

    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    #[serde(deny_unknown_fields)]
    struct Nested {
        inner: FacetReference,
    }

    fn compiled() -> CompiledSchema {
        let universe = crate::facet::Universe::collect(vec![
            crate::facet::Declaration::assemble::<Reffed>().unwrap(),
        ]);
        let bundle = crate::facet::Bundle::new(&universe, Vec::new());
        let document = serde_json::to_value(bundle.document("Reffed").unwrap()).unwrap();
        CompiledSchema::compile(document).unwrap()
    }

    fn reference(suffix: &str) -> String {
        format!("db+facet:/self/org.example.note/{suffix}")
    }

    #[test]
    fn schema_declares_references_only_when_marked() {
        assert!(compiled().declares_references());
        let plain = CompiledSchema::compile(json!({"type": "string"})).unwrap();
        assert!(!plain.declares_references());
    }

    #[test]
    fn extraction_finds_declared_positions_with_paths_and_pins() {
        let heads = vec!["a".repeat(64), "b".repeat(64)];
        let value = json!({
            "link": reference("main"),
            "refHeads": heads,
            "nested": {"inner": reference("inner")},
            "listed": [reference("first"), reference("second")],
            "mapped": {"key/one": reference("mapped")},
            "maybe": null,
        });
        let found = declared_references(&compiled(), &value);
        let by_path: BTreeMap<&str, &DeclaredReference> = found
            .iter()
            .map(|edge| (edge.path.as_str(), edge))
            .collect();
        assert_eq!(found.len(), 5, "{by_path:?}");
        assert_eq!(
            by_path["/link"].reference,
            FacetReference::parse(&reference("main")).unwrap()
        );
        assert_eq!(
            by_path["/link"].heads,
            vec![
                ChangeHash::from_str(&"a".repeat(64)).unwrap(),
                ChangeHash::from_str(&"b".repeat(64)).unwrap()
            ]
        );
        assert_eq!(by_path["/nested/inner"].path, "/nested/inner");
        assert_eq!(by_path["/listed/0"].path, "/listed/0");
        assert_eq!(by_path["/listed/1"].path, "/listed/1");
        // A keyed-map member's key is pointer-escaped, and an element has no sibling to pin by.
        assert_eq!(by_path["/mapped/key~1one"].path, "/mapped/key~1one");
        for (path, edge) in by_path {
            if path != "/link" {
                assert!(edge.heads.is_empty(), "{path}");
            }
        }
    }

    #[test]
    fn a_reference_round_trips_through_document_storage() {
        let schema = compiled();
        let mut doc = automerge::Automerge::new();
        let tx = doc.transaction();
        let tx = crate::facet::reconcile_json_atomic(
            tx,
            &schema,
            &automerge::ROOT,
            "facet".into(),
            json!({
                "link": reference("main"),
                "refHeads": ["a".repeat(64)],
                "nested": {"inner": reference("inner")},
                "listed": [],
                "mapped": {},
                "maybe": null,
            }),
        )
        .unwrap();
        tx.commit();
        let hydrated =
            crate::facet::hydrate_value(&doc, &automerge::ROOT, "facet".into(), &schema).unwrap();
        let found = declared_references(&schema, &hydrated);
        assert_eq!(found.len(), 2, "{found:?}");
        assert_eq!(found[0].path, "/link");
        assert_eq!(found[0].heads.len(), 1);
        assert_eq!(found[1].path, "/nested/inner");
    }

    #[test]
    fn extraction_skips_undeclared_unparseable_and_null_positions() {
        let document = json!({
            "type": "object",
            "properties": {
                "declared": {"type": "string", "x-automerge": "reference"},
                "plain": {"type": "string"},
            },
        });
        let value = json!({
            "declared": "not a reference",
            "plain": reference("sniffed"),
            "unknown": {"nested": reference("hidden")},
        });
        assert!(
            declared_references(&CompiledSchema::compile(document).unwrap(), &value).is_empty()
        );

        let value = json!({
            "link": reference("main"),
            "refHeads": ["not hex at all"],
            "nested": {"inner": reference("inner")},
            "maybe": null,
        });
        let found = declared_references(&compiled(), &value);
        assert_eq!(found.len(), 2);
        assert!(found[0].heads.is_empty());
    }
}
