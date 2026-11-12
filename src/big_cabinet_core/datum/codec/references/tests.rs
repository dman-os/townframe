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
    let universe =
        crate::datum::Universe::collect(vec![crate::datum::Declaration::of::<Reffed>().unwrap()]);
    let bundle = crate::datum::Bundle::new(&universe, Vec::new());
    let lexicons = bundle.lexicons().unwrap();
    let defs = crate::reference::defs_lexicon();
    let id = universe.datums[0].id.clone();
    let compiled = bundle
        .validate_lexicon(&id, &lexicons[0].file, &[&defs])
        .unwrap();
    (compiled, id)
}

/// The datum and pin heads a declared reference addresses.
fn datum(edge: &DeclaredReference) -> (&DatumReference, &[ChangeHash]) {
    match &edge.target {
        ReferenceTarget::Datum { reference, heads } => (reference, heads),
        other => panic!("{} is not a datum reference: {other:?}", edge.path),
    }
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
        datum(by_path["/at"]).0,
        &DatumReference::parse(&reference("located")).unwrap()
    );
    assert_eq!(
        datum(by_path["/link"]).0,
        &DatumReference::parse(&reference("main")).unwrap()
    );
    // A pin is read from its own object, in hash-byte order whatever order it was written in.
    assert_eq!(
        datum(by_path["/pinned"]).0,
        &DatumReference::parse(&reference("pinned")).unwrap()
    );
    assert_eq!(
        datum(by_path["/pinned"]).1,
        [ChangeHash([1; 32]), ChangeHash([2; 32])]
    );
    assert_eq!(by_path["/nested/inner"].path, "/nested/inner");
    assert_eq!(by_path["/listed/0"].path, "/listed/0");
    assert_eq!(by_path["/listed/1"].path, "/listed/1");
    // A member's name is pointer-escaped.
    assert_eq!(by_path["/odd~1key~0"].path, "/odd~1key~0");
    // Only a `datumPin` pins: every other position follows the target's current frontier.
    for (path, edge) in by_path {
        if path != "/pinned" {
            assert!(datum(edge).1.is_empty(), "{path}");
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
    assert_eq!(datum(pinned).1, [ChangeHash([1; 32])]);
}

#[test]
fn extraction_skips_undeclared_and_unparseable_positions() {
    let (schema, id) = compiled();
    // A string that spells an address where nothing declares a reference is data.
    let plain = json!({"type": "object", "properties": {"plain": {"type": "string"}}});
    let value = json!({"plain": reference("sniffed"), "unknown": {"nested": reference("hidden")}});
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
    let universe =
        crate::datum::Universe::collect(vec![crate::datum::Declaration::of::<Refused>().unwrap()]);
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

/// A kind whose slot is git's object id of its `raw` bytes.
#[macros::datum(authority = "org.example")]
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(extend("x-content-hash" = {"algorithm": "gitObject", "type": "blob", "content": "raw"}))]
struct Blob {
    raw: crate::datum::Bytes,
    #[serde(flatten)]
    extra: crate::datum::Extra,
}

/// A kind that declares its slot hashes a member it does not have.
#[macros::datum(authority = "org.example")]
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(extend("x-content-hash" = {"algorithm": "gitObject", "type": "blob", "content": "missing"}))]
struct Unhashable {
    raw: crate::datum::Bytes,
    #[serde(flatten)]
    extra: crate::datum::Extra,
}

#[macros::datum(authority = "org.example")]
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct Pointer {
    target: crate::KeyReference,
    #[serde(default)]
    more: Vec<crate::KeyReference>,
    #[serde(flatten)]
    extra: crate::datum::Extra,
}

/// The schema a node compiles for `T`, admitted with the reference defs and `T`'s own sidecar.
fn admitted<T>() -> (
    Result<CompiledSchema, crate::datum::LexiconAdmissionError>,
    String,
)
where
    T: crate::datum::DatumMember + crate::datum::DatumShape + JsonSchema,
{
    let universe =
        crate::datum::Universe::collect(vec![crate::datum::Declaration::of::<T>().unwrap()]);
    let bundle = crate::datum::Bundle::new(&universe, Vec::new());
    let lexicons = bundle.lexicons().unwrap();
    let defs = crate::reference::defs_lexicon();
    let id = universe.datums[0].id.clone();
    (
        bundle.validate_lexicon(&id, &lexicons[0].file, &[&defs]),
        id,
    )
}

#[test]
fn a_key_reference_is_extracted_by_its_key_and_names_no_document() {
    let (schema, id) = admitted::<Pointer>();
    let schema = schema.unwrap();
    assert!(schema.declares_references());
    let key = |slot: &str| crate::DatumKey::from_text("org.example.blob", slot).unwrap();
    let value = json!({
        "$type": id,
        "target": {"key": key("aa").to_string()},
        "more": [{"key": key("bb").to_string()}, {"key": "no-slot"}],
    });
    let found = declared_references(&schema, &value);
    // A key with no slot is not a key, so that position is skipped and the walk goes on.
    let expected = [("/more/0", "bb"), ("/target", "aa")];
    let mut found: Vec<_> = found
        .iter()
        .map(|edge| (edge.path.as_str(), &edge.target))
        .collect();
    found.sort_by_key(|(path, _)| *path);
    assert_eq!(found.len(), expected.len(), "{found:?}");
    for ((path, target), (want_path, slot)) in found.iter().zip(expected) {
        assert_eq!(*path, want_path);
        assert_eq!(**target, ReferenceTarget::Key(key(slot)));
    }
}

#[test]
fn a_key_reference_that_is_not_a_key_is_refused_at_write() {
    let (schema, id) = admitted::<Pointer>();
    let schema = schema.unwrap();
    for target in [
        json!({"key": "no-slot"}),
        json!({}),
        json!("org.example.blob/aa"),
    ] {
        let mut doc = automerge::Automerge::new();
        let refused = crate::datum::reconcile_json_atomic(
            doc.transaction(),
            &schema,
            &automerge::ROOT,
            "datum".into(),
            json!({"$type": id, "target": target}),
        );
        assert!(refused.is_err(), "{target}");
    }
}

#[test]
fn a_sidecar_declares_which_kinds_have_a_content_hash_slot() {
    let blob = admitted::<Blob>().0.unwrap();
    assert_eq!(
        blob.content_hash(),
        Some(&crate::datum::ContentHash::GitObject {
            object_type: "blob",
            content: "raw".to_owned(),
        })
    );
    assert_eq!(admitted::<Pointer>().0.unwrap().content_hash(), None);
    // The declaration names the member the value is hashed from, so one that is not a member
    // could never verify and is refused when the kind is admitted.
    let refused = admitted::<Unhashable>().0.unwrap_err();
    assert!(refused.to_string().contains("missing"), "{refused}");
}
