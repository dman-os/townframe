//! What a write inside a base-selected transaction keeps, converges and takes: the evidence for
//! native preservation, skipped unchanged values, checked counter deltas and omission at the base.

use super::{facet, shape};
use crate::facet::codec::{hydrate_value, reconcile_json, reconcile_json_atomic};
use crate::facet::{Bundle, Declaration, Extra, Universe};
use automerge::transaction::Transactable;
use automerge::{ActorId, Automerge, ObjType, ROOT, ReadDoc, ScalarValue};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, json};

/// An open facet: its title is what it declares, and everything else is what a peer's version
/// carried that this schema does not name.
#[macros::facet(id = "org.example.nativeOpen")]
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct OpenFacet {
    title: String,
    #[serde(flatten)]
    extra: Extra,
}

fn open_document() -> Json {
    let universe = Universe::collect(vec![Declaration::of::<OpenFacet>().unwrap()]);
    let bundle = Bundle::new(&universe, Vec::new());
    serde_json::to_value(bundle.document("OpenFacet").unwrap()).unwrap()
}

#[macros::facet(id = "org.example.listFacet", closed)]
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ListFacet {
    values: Vec<String>,
}

fn list_document() -> Json {
    let universe = Universe::collect(vec![Declaration::of::<ListFacet>().unwrap()]);
    let bundle = Bundle::new(&universe, Vec::new());
    serde_json::to_value(bundle.document("ListFacet").unwrap()).unwrap()
}

/// A target-aware list write uses the same positional/no-key Autosurgeon baseline as a direct
/// reconciler: concurrent insertion survives, an edit at the supplied head targets that baseline
/// position, and omission deletes only the supplied base tail.
#[test]
fn list_writes_follow_autosurgeon_positional_heads() {
    let document = list_document();
    let mut doc = Automerge::new();
    let heads0 = {
        let mut tx = doc.transaction();
        reconcile_json(
            &mut tx,
            &ROOT,
            facet(),
            &document,
            json!({"values": ["a", "b", "c"]}),
        )
        .unwrap();
        let (heads, _) = tx.commit();
        heads.expect("the initial list commits")
    };

    let mut peer = doc.fork();
    peer.set_actor(ActorId::from([9u8; 32]));
    {
        let mut tx = peer.transaction();
        let (_, map) = tx.get(ROOT, "facet").unwrap().unwrap();
        let (_, list) = tx.get(&map, "values").unwrap().unwrap();
        tx.insert(&list, 1, "peer").unwrap();
        tx.commit();
    }
    let peer_changes = peer.get_changes(&[heads0]);

    let mut tx = doc
        .transaction_at(automerge::PatchLog::null(), &[heads0])
        .unwrap();
    reconcile_json(
        &mut tx,
        &ROOT,
        facet(),
        &document,
        json!({"values": ["a", "B"]}),
    )
    .unwrap();
    tx.commit();
    doc.apply_changes(peer_changes).unwrap();

    assert_eq!(
        hydrate_value(&doc, &ROOT, facet(), &super::compiled(&document)).unwrap(),
        json!({"values": ["a", "peer", "B"]}),
        "the remote insertion survives while the supplied-head edit and omission apply"
    );
}

/// An old-schema document knows only the member its declaration names, so its writer's write
/// carries the neighbor and leaves what the schema does not name native: a future collaborative
/// text is not turned into a Str, and a splice a concurrent writer makes after the base survives,
/// because nothing was written over the text at all.
#[test]
fn list_delete_and_move_conflicts_follow_the_same_baseline() {
    let document = list_document();
    let mut doc = Automerge::new();
    let heads0 = {
        let mut tx = doc.transaction();
        reconcile_json(
            &mut tx,
            &ROOT,
            facet(),
            &document,
            json!({"values": ["a", "b", "c"]}),
        )
        .unwrap();
        let (heads, _) = tx.commit();
        heads.expect("the initial list commits")
    };

    let mut peer = doc.fork();
    peer.set_actor(ActorId::from([10u8; 32]));
    {
        let mut tx = peer.transaction();
        let (_, map) = tx.get(ROOT, "facet").unwrap().unwrap();
        let (_, list) = tx.get(&map, "values").unwrap().unwrap();
        tx.delete(&list, 1).unwrap();
        tx.insert(&list, 1, "b").unwrap();
        tx.commit();
    }
    let peer_changes = peer.get_changes(&[heads0]);

    let mut tx = doc
        .transaction_at(automerge::PatchLog::null(), &[heads0])
        .unwrap();
    reconcile_json(
        &mut tx,
        &ROOT,
        facet(),
        &document,
        json!({"values": ["a", "b", "B", "c"]}),
    )
    .unwrap();
    tx.commit();
    doc.apply_changes(peer_changes).unwrap();

    assert_eq!(
        hydrate_value(&doc, &ROOT, facet(), &super::compiled(&document)).unwrap(),
        json!({"values": ["a", "b", "B", "c"]}),
        "a delete-plus-insert move and edit preserve both no-key list insertions"
    );
}

#[test]
fn an_undeclared_native_text_keeps_its_object_while_a_neighbor_changes() {
    let document = open_document();
    let mut doc = Automerge::new();
    let heads0 = {
        let mut tx = doc.transaction();
        let map = tx.put_object(ROOT, "facet", ObjType::Map).unwrap();
        tx.put(&map, "title", "base").unwrap();
        // and the future writer's content: a collaborative text at a member the schema does not
        // name
        let note = tx.put_object(&map, "note", ObjType::Text).unwrap();
        tx.splice_text(&note, 0, 0, "hello").unwrap();
        let (heads, _) = tx.commit();
        heads.expect("a creator change commits")
    };

    let mut peer = doc.fork();
    peer.set_actor(ActorId::from([2u8; 32]));
    {
        let mut tx = peer.transaction();
        let (_, map) = tx.get(ROOT, "facet").unwrap().unwrap();
        let (_, note) = tx.get(&map, "note").unwrap().unwrap();
        tx.splice_text(&note, 5, 0, " world").unwrap();
        tx.commit();
    }
    let changes = peer.get_changes(&[heads0]);

    // the caller's write, based at the heads before the splice, changes the neighbor and carries
    // the text it read unchanged
    let mut tx = doc
        .transaction_at(automerge::PatchLog::null(), &[heads0])
        .unwrap();
    reconcile_json(
        &mut tx,
        &ROOT,
        facet(),
        &document,
        json!({"title": "changed", "note": "hello"}),
    )
    .unwrap();
    assert_eq!(tx.pending_ops(), 1, "the unchanged text wrote nothing");
    tx.commit();

    doc.apply_changes(changes).unwrap();

    let (_, map) = doc.get(ROOT, "facet").unwrap().unwrap();
    let (note, note_id) = doc.get(&map, "note").unwrap().unwrap();
    assert_eq!(
        shape(&note),
        "text",
        "the member the schema does not name keeps its native storage"
    );
    assert_eq!(
        doc.text(&note_id).unwrap(),
        "hello world",
        "a splice no writer rewrote merges with the change"
    );
    assert_eq!(
        hydrate_value(&doc, &ROOT, facet(), &super::compiled(&document)).unwrap(),
        json!({"title": "changed", "note": "hello world"}),
        "the read is the carry the contract spells"
    );
}

/// The declaration a position makes is what the position keeps on write, and not a suggestion the
/// native storage may outvote: a plain Str wins over the text a peer's unsanctioned write left,
/// equal or changed, before anything reads a drifted position as its declaration.
#[test]
fn a_declared_plain_string_wins_over_a_native_text() {
    #[macros::facet(id = "org.example.plainNote", closed)]
    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    #[serde(deny_unknown_fields)]
    struct PlainNote {
        note: String,
    }

    let universe = Universe::collect(vec![Declaration::of::<PlainNote>().unwrap()]);
    let bundle = Bundle::new(&universe, Vec::new());
    let document = serde_json::to_value(bundle.document("PlainNote").unwrap()).unwrap();

    // the first submission changes the text, the second submits what it already reads: either
    // way the declared Str is what lands
    for (stated, submitted) in [
        ("bonjour", json!({"note": "bonjour"})),
        ("hello", json!({"note": "hello"})),
    ] {
        let mut doc = Automerge::new();
        let heads = {
            let mut tx = doc.transaction();
            let map = tx.put_object(ROOT, "facet", ObjType::Map).unwrap();
            let note_obj = tx.put_object(&map, "note", ObjType::Text).unwrap();
            tx.splice_text(&note_obj, 0, 0, "hello").unwrap();
            let (heads, _) = tx.commit();
            heads.expect("a creator change commits")
        };
        let mut tx = doc
            .transaction_at(automerge::PatchLog::null(), &[heads])
            .unwrap();
        reconcile_json(&mut tx, &ROOT, facet(), &document, submitted.clone()).unwrap();
        assert_eq!(
            tx.pending_ops(),
            1,
            "a drifted position converges even when the values are equal"
        );
        tx.commit();

        let (_, map) = doc.get(ROOT, "facet").unwrap().unwrap();
        let (note, _) = doc.get(&map, "note").unwrap().unwrap();
        assert_eq!(
            shape(&note),
            "string",
            "{stated} is a Str, spelled by the declaration"
        );
        assert_eq!(
            hydrate_value(&doc, &ROOT, facet(), &super::compiled(&document)).unwrap(),
            submitted,
        );
    }
}

/// A read-modify-write that changes nothing writes nothing, wherever the content is native.
#[test]
fn an_unchanged_write_emits_nothing() {
    let document = super::document();
    let mut doc = Automerge::new();
    let json = super::value().1;
    let heads = {
        let mut tx = doc.transaction();
        reconcile_json(&mut tx, &ROOT, facet(), &document, json.clone()).unwrap();
        let (heads, _) = tx.commit();
        heads.expect("a creator change commits")
    };
    let mut tx = doc
        .transaction_at(automerge::PatchLog::null(), &[heads])
        .unwrap();
    reconcile_json(&mut tx, &ROOT, facet(), &document, json).unwrap();
    assert_eq!(
        tx.pending_ops(),
        0,
        "nothing that did not change was written"
    );
    tx.commit();

    assert_eq!(
        doc.get_heads(),
        vec![heads],
        "an unchanged write is no change"
    );
}

/// A counter the position declares is changed by the checked delta it needs, which is additive:
/// what a concurrent writer incremented after the base accumulates with it, and an unchanged one
/// is no write at all.
#[test]
fn a_declared_counter_takes_the_checked_delta_of_a_concurrent_change() {
    let document = super::document();
    let mut doc = Automerge::new();
    let heads0 = {
        let mut json = super::value().1;
        json["counter"] = json!(5);
        let mut tx = doc.transaction();
        reconcile_json(&mut tx, &ROOT, facet(), &document, json).unwrap();
        let (heads, _) = tx.commit();
        heads.expect("a creator change commits")
    };
    let mut peer = doc.fork();
    peer.set_actor(ActorId::from([3u8; 32]));
    {
        let mut tx = peer.transaction();
        let (_, map) = tx.get(ROOT, "facet").unwrap().unwrap();
        tx.increment(&map, "counter", 3).unwrap();
        tx.commit();
    }
    let changes = peer.get_changes(&[heads0]);

    // the caller's write counts from 5 to 7: the delta is two, not the fresh counter a put would be
    let mut tx = doc
        .transaction_at(automerge::PatchLog::null(), &[heads0])
        .unwrap();
    let mut json = super::value().1;
    json["counter"] = json!(7);
    reconcile_json(&mut tx, &ROOT, facet(), &document, json).unwrap();
    assert_eq!(tx.pending_ops(), 1, "an increment, not a counter put");
    tx.commit();

    doc.apply_changes(changes).unwrap();

    let projected = hydrate_value(&doc, &ROOT, facet(), &super::compiled(&document)).unwrap();
    assert_eq!(
        projected["counter"],
        json!(10),
        "5 + 2 + 3: the concurrent increments accumulate"
    );

    // and a counter that already holds what is submitted is not written again
    let mut tx = doc
        .transaction_at(automerge::PatchLog::null(), &doc.get_heads())
        .unwrap();
    let mut json = super::value().1;
    json["counter"] = json!(10);
    reconcile_json(&mut tx, &ROOT, facet(), &document, json).unwrap();
    assert_eq!(tx.pending_ops(), 0, "no delta, no write");
    tx.commit();
}

/// A delta no counter can arrive at is refused, and a refusal writes nothing.
#[test]
fn a_counter_delta_outside_the_counter_is_refused_before_anything_is_written() {
    let mut doc = Automerge::new();
    let heads = {
        let mut tx = doc.transaction();
        let map = tx.put_object(ROOT, "facet", ObjType::Map).unwrap();
        // The canonical maximum is safe JSON, but it still cannot be reached from i64::MIN.
        tx.put(&map, "counter", ScalarValue::Counter(i64::MIN.into()))
            .unwrap();
        let (heads, _) = tx.commit();
        heads.expect("a creator change commits")
    };
    let document = super::document();
    let mut tx = doc
        .transaction_at(automerge::PatchLog::null(), &[heads])
        .unwrap();
    let mut json = super::value().1;
    json["counter"] = json!(9_007_199_254_740_991i64);
    let error = reconcile_json(&mut tx, &ROOT, facet(), &document, json)
        .expect_err("the delta is outside the counter's range");
    assert!(error.to_string().contains("cannot arrive at"), "{error}");
    assert_eq!(tx.pending_ops(), 0, "the refusal happened before any write");
}

/// Deletion is what omitting a member means, at the heads the write is based on: what the base
/// holds is removed, and an addition a concurrent writer made that the caller could not have seen
/// survives the merge, because the base view is what the write diffed against.
#[test]
fn omission_removes_at_the_supplied_base_and_an_unseen_addition_survives() {
    let document = open_document();
    let mut doc = Automerge::new();
    let heads0 = {
        let mut tx = doc.transaction();
        let map = tx.put_object(ROOT, "facet", ObjType::Map).unwrap();
        tx.put(&map, "title", "base").unwrap();
        // an enclosing object whose contents go wherever the object does
        let enclosed = tx.put_object(&map, "enclosed", ObjType::Map).unwrap();
        tx.put(&enclosed, "deep", "x").unwrap();
        let (heads, _) = tx.commit();
        heads.expect("a creator change commits")
    };
    let mut peer = doc.fork();
    peer.set_actor(ActorId::from([4u8; 32]));
    {
        let mut tx = peer.transaction();
        let (_, map) = tx.get(ROOT, "facet").unwrap().unwrap();
        tx.put(&map, "pasted", 1).unwrap();
        tx.commit();
    }
    let changes = peer.get_changes(&[heads0]);

    let mut tx = doc
        .transaction_at(automerge::PatchLog::null(), &[heads0])
        .unwrap();
    reconcile_json(
        &mut tx,
        &ROOT,
        facet(),
        &document,
        json!({"title": "changed"}),
    )
    .unwrap();
    tx.commit();

    doc.apply_changes(changes).unwrap();

    let projected = hydrate_value(&doc, &ROOT, facet(), &super::compiled(&document)).unwrap();
    assert_eq!(
        projected,
        json!({"title": "changed", "pasted": 1}),
        "what the base held is removed with what the value omitted, and what nothing here \
         declared is not a deletion anyone saw"
    );
}

/// Bytes and an instant at a position the schema does not name ride the raw spellings, and a
/// read-modify-write that carries them unchanged does not rewrite the storage they hold.
///
/// There is no schema for these: what comes back is the best-effort projection of what the
/// storage is, and not the fidelity a declaration gives. A float's width and a scalar's kind
/// against the schema's are questions of their declaration, which none of these has traveled
/// with, so nothing is inferred here.
#[test]
fn undeclared_bytes_and_an_instant_round_trip_in_their_raw_spellings() {
    let document = open_document();
    let mut doc = Automerge::new();
    let heads = {
        let mut tx = doc.transaction();
        let map = tx.put_object(ROOT, "facet", ObjType::Map).unwrap();
        tx.put(&map, "title", "base").unwrap();
        // a future writer's specialized members, in the storage only it knows
        tx.put(&map, "blob", ScalarValue::Bytes(vec![1, 2]))
            .unwrap();
        tx.put(&map, "when", ScalarValue::Timestamp(0)).unwrap();
        let (heads, _) = tx.commit();
        heads.expect("a creator change commits")
    };
    let projected = hydrate_value(&doc, &ROOT, facet(), &super::compiled(&document)).unwrap();
    assert_eq!(
        projected["blob"],
        json!({"$bytes": "AQI="}),
        "the canonical bytes object"
    );
    assert_eq!(
        projected["when"],
        json!("1970-01-01T00:00:00Z"),
        "an RFC 3339 instant"
    );

    // unchanged: nothing to write
    let mut tx = doc
        .transaction_at(automerge::PatchLog::null(), &[heads])
        .unwrap();
    reconcile_json(&mut tx, &ROOT, facet(), &document, projected).unwrap();
    assert_eq!(
        tx.pending_ops(),
        0,
        "unchanged native values are not rewritten"
    );
    tx.commit();

    // changed: the bytes change and the instant does not
    let mut tx = doc
        .transaction_at(automerge::PatchLog::null(), &[heads])
        .unwrap();
    reconcile_json(
        &mut tx,
        &ROOT,
        facet(),
        &document,
        json!({"title": "base", "blob": {"$bytes": "AQID"}, "when": "1970-01-01T00:00:00Z"}),
    )
    .unwrap();
    assert_eq!(
        tx.pending_ops(),
        1,
        "the change is the bytes' put, and nothing else"
    );
    tx.commit();

    let projected = hydrate_value(&doc, &ROOT, facet(), &super::compiled(&document)).unwrap();
    assert_eq!(projected["blob"], json!({"$bytes": "AQID"}));
    assert_eq!(projected["when"], json!("1970-01-01T00:00:00Z"));
}

/// The declaration a position makes is one spelling for one concept — a storage form, an ordinary
/// kind, or genuinely nothing — and `scalar_checked` must classify by THAT, not by "no number kind
/// was declared". A kind the schema states wins over the specialized native value underneath,
/// equal or changed, and none of the specialized spellings is applied to it.
///
/// The three specialized kinds the fallback spells are each planted at a position the schema
/// declares an ordinary string: the projection they would ride shows up as the wrong native type
/// if a drifted position keeps or re-spells it, so both the equal and the changed submission have
/// to land the declared Str.
#[test]
fn a_declared_kind_wins_over_each_specialized_native() {
    let cases = [
        (ScalarValue::Bytes(vec![1, 2]), json!("AQI="), json!("AQID")),
        (ScalarValue::Counter(5i64.into()), json!("5"), json!("6")),
        (
            ScalarValue::Timestamp(0),
            json!("1970-01-01T00:00:00Z"),
            json!("1971-01-01T00:00:00Z"),
        ),
    ];
    let document = json!({
        "type": "object",
        "$defs": {},
        "properties": {"field": {"type": "string"}},
    });
    for (planted, equal, changed) in cases {
        for (stated, how) in [(equal, "equal"), (changed, "changed")] {
            let mut doc = Automerge::new();
            let heads = {
                let mut tx = doc.transaction();
                let map = tx.put_object(ROOT, "facet", ObjType::Map).unwrap();
                tx.put(&map, "field", planted.clone()).unwrap();
                let (heads, _) = tx.commit();
                heads.expect("a creator change commits")
            };
            let mut tx = doc
                .transaction_at(automerge::PatchLog::null(), &[heads])
                .unwrap();
            reconcile_json(&mut tx, &ROOT, facet(), &document, json!({"field": stated}))
                .unwrap_or_else(|err| panic!("{how} submission refused: {err}"));
            assert_eq!(tx.pending_ops(), 1, "the declared Str lands, {how} or not");
            tx.commit();

            let (_, facet_map) = doc.get(ROOT, "facet").unwrap().unwrap();
            let (field, _) = doc.get(&facet_map, "field").unwrap().unwrap();
            assert_eq!(
                shape(&field),
                "string",
                "the declared kind is what landed ({how}): {field:?}"
            );
        }
    }
}

/// And where the schema declares it signed, the declaration takes the whole value over the
/// counter in the same way.
#[test]
fn a_declared_integer_takes_whole_values_over_a_counter() {
    let document = json!({
        "type": "object",
        "$defs": {},
        "properties": {"field": {"type": "integer"}},
    });
    for (stated, how) in [(json!(5), "equal"), (json!(6), "changed")] {
        let mut doc = Automerge::new();
        let heads = {
            let mut tx = doc.transaction();
            let map = tx.put_object(ROOT, "facet", ObjType::Map).unwrap();
            tx.put(&map, "field", ScalarValue::Counter(5i64.into()))
                .unwrap();
            let (heads, _) = tx.commit();
            heads.expect("a creator change commits")
        };
        let mut tx = doc
            .transaction_at(automerge::PatchLog::null(), &[heads])
            .unwrap();
        reconcile_json(&mut tx, &ROOT, facet(), &document, json!({"field": stated}))
            .unwrap_or_else(|err| panic!("{how} submission refused: {err}"));
        assert_eq!(tx.pending_ops(), 1, "the declared Int lands, {how} or not");
        tx.commit();

        let (_, facet_map) = doc.get(ROOT, "facet").unwrap().unwrap();
        let (field, _) = doc.get(&facet_map, "field").unwrap().unwrap();
        assert_eq!(
            shape(&field),
            "int",
            "the declared sign is what landed ({how}): {field:?}"
        );
        assert_eq!(
            hydrate_value(&doc, &ROOT, facet(), &super::compiled(&document)).unwrap()["field"],
            stated,
        );
    }
}

/// The keyword's plain markers name a plain position on purpose, so a marker standing alone is
/// still a declaration and not an absence: a specialized native value there is a drift to
/// converge, and the raw spellings are not projected for it.
#[test]
fn a_plain_marker_without_a_type_is_still_a_declaration() {
    // a number marker alone, with the token's kind of integer planted under it
    let number_document = json!({
        "type": "object",
        "$defs": {},
        "properties": {"facet": {"type": "object"}},
    });
    let mut number_document = number_document;
    number_document["properties"]["field"] = json!({"x-automerge": "number"});

    let mut doc = Automerge::new();
    let heads = {
        let mut tx = doc.transaction();
        let map = tx.put_object(ROOT, "facet", ObjType::Map).unwrap();
        tx.put(&map, "field", ScalarValue::Int(5)).unwrap();
        let (heads, _) = tx.commit();
        heads.expect("a creator change commits")
    };
    let mut tx = doc
        .transaction_at(automerge::PatchLog::null(), &[heads])
        .unwrap();
    reconcile_json(
        &mut tx,
        &ROOT,
        facet(),
        &number_document,
        json!({"field": 5}),
    )
    .unwrap();
    assert_eq!(
        tx.pending_ops(),
        1,
        "a plain number's producible kind is the f64 its write puts, equal or not"
    );
    tx.commit();
    let (_, facet_map) = doc.get(ROOT, "facet").unwrap().unwrap();
    let (field, _) = doc.get(&facet_map, "field").unwrap().unwrap();
    assert_eq!(
        shape(&field),
        "f64",
        "the marker's kind is what landed: {field:?}"
    );

    // a list marker alone: the value is a CRDT list, so a list nothing declared about is what
    // rides it, and a specialized scalar planted there is a refusal rather than a projection
    let list_document = json!({});
    let mut list_document = list_document;
    list_document["type"] = json!("object");
    list_document["$defs"] = json!({});
    list_document["properties"]["field"] = json!({"x-automerge": "list"});

    let mut doc = Automerge::new();
    {
        let mut tx = doc.transaction();
        let _obj = tx.put_object(ROOT, "facet", ObjType::Map).unwrap();
        tx.put(&_obj, "field", ScalarValue::Timestamp(0)).unwrap();
        tx.commit();
    };
    let error = hydrate_value(&doc, &ROOT, facet(), &super::compiled(&list_document))
        .expect_err("a marker states a plain position, and nothing projects a projection there");
    assert!(error.to_string().contains("an instant"), "{error}");
}

/// A plan's entries must be found where they were checked and nowhere else: a rendered chain is
/// not injective, since a member may be named `a.b` or `a[0]`, so a structural key is what keys
/// the plan. A collision would let one position's decision overwrite another's and then apply to
/// both.
#[test]
fn a_member_named_like_a_chain_position_is_not_another_position() {
    let cases = [
        // (the base, the change, the name-of-the-collision)
        (
            json!({"a.b": "old", "a": {"b": "old"}}),
            json!({"a.b": "new", "a": {"b": "old"}}),
            json!({"a.b": "new", "a": {"b": "old"}}),
            "dotted",
        ),
        (
            json!({"a[0]": "old", "a": ["old"]}),
            json!({"a[0]": "new", "a": ["old"]}),
            json!({"a[0]": "new", "a": ["old"]}),
            "bracket",
        ),
        (
            json!({"a/b": "old", "a": {"b": "old"}}),
            json!({"a/b": "new", "a": {"b": "old"}}),
            json!({"a/b": "new", "a": {"b": "old"}}),
            "slash",
        ),
        (
            json!({"a~1": "old", "a": {"1": "old"}}),
            json!({"a~1": "new", "a": {"1": "old"}}),
            json!({"a~1": "new", "a": {"1": "old"}}),
            "tilde",
        ),
    ];
    let document = json!({
        "type": "object",
        "$defs": {},
        "properties": {"facet": {"type": "object"}},
    });
    for (_base, changed, expected, name) in cases {
        let mut doc = Automerge::new();
        let heads = {
            let mut tx = doc.transaction();
            let map = tx.put_object(ROOT, "facet", ObjType::Map).unwrap();
            // each base holds both spellings the collision would conflate
            match name {
                "dotted" | "slash" | "tilde" => {
                    let first = match name {
                        "dotted" => "a.b",
                        "slash" => "a/b",
                        _ => "a~1",
                    };
                    tx.put(&map, first, "old").unwrap();
                    let nested = tx.put_object(&map, "a", ObjType::Map).unwrap();
                    // the nested member's key is the one the collision test conflate; its value
                    // is what the submission carries unchanged
                    tx.put(
                        &nested,
                        match name {
                            "tilde" => "1",
                            _ => "b",
                        },
                        "old",
                    )
                    .unwrap();
                }
                _ => {
                    tx.put(&map, "a[0]", "old").unwrap();
                    let list = tx.put_object(&map, "a", ObjType::List).unwrap();
                    tx.insert(&list, 0, "old").unwrap();
                }
            }
            let (heads, _) = tx.commit();
            heads.expect("a creator change commits")
        };
        let mut tx = doc
            .transaction_at(automerge::PatchLog::null(), &[heads])
            .unwrap();
        reconcile_json(&mut tx, &ROOT, facet(), &document, changed.clone())
            .unwrap_or_else(|err| panic!("{name} key submission refused: {err}"));
        assert_eq!(
            tx.pending_ops(),
            1,
            "only the changed member was written ({name})"
        );
        tx.commit();

        let projected = hydrate_value(&doc, &ROOT, facet(), &super::compiled(&document)).unwrap();
        assert_eq!(
            projected, expected,
            "the decisions landed where they were checked ({name})"
        );
    }
}

#[test]
fn nested_changes_preserve_native_identity_metadata_and_conflicts() {
    let document = json!({
        "type": "object",
        "properties": {
            "title": {"type": "string"},
            "nested": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "changed": {"type": "string"},
                        "stable": {
                            "type": "object",
                            "properties": {
                                "blob": {"x-automerge": "bytes"},
                                "text": {"type": "string", "x-automerge": "text"},
                                "counter": {"type": "integer", "x-automerge": "counter"},
                                "record": {"type": "object"}
                            }
                        }
                    }
                }
            },
            "metadata": {
                "type": "object",
                "properties": {
                    "createdAt": {"type": "string", "format": "date-time"},
                    "updatedAt": {
                        "type": "array", "items": {"type": "string", "format": "date-time"}
                    },
                    "deletedAt": {
                        "type": "array", "items": {"type": "string", "format": "date-time"}
                    },
                    "signatures": {"type": "array", "items": {"type": "object"}}
                }
            }
        }
    });
    let declared = super::compiled(&document);
    for schema in [declared.clone(), super::compiled(&open_document())] {
        let mut doc = Automerge::new();
        doc.set_actor(ActorId::from([1u8; 32]));
        let tx = reconcile_json_atomic(
            doc.transaction(),
            &declared,
            &ROOT,
            facet(),
            json!({
                "title": "base",
                "nested": [{
                    "changed": "before",
                    "stable": {
                        "blob": {"$bytes": "AA=="},
                        "text": "hello",
                        "counter": 5,
                        "record": {"label": "base"}
                    }
                }],
                "metadata": {
                    "createdAt": "2024-01-01T00:00:00Z",
                    "updatedAt": ["2024-01-02T00:00:00Z"],
                    "deletedAt": [],
                    "signatures": [{"signer": "base", "signature": {"$bytes": "AA=="}}]
                }
            }),
        )
        .unwrap();
        tx.commit();
        let map = doc.get(ROOT, "facet").unwrap().unwrap().1;
        let nested = doc.get(&map, "nested").unwrap().unwrap().1;
        let item = doc.get(&nested, 0).unwrap().unwrap().1;
        let stable = doc.get(&item, "stable").unwrap().unwrap().1;
        let mut peer = doc.fork();
        peer.set_actor(ActorId::from([2u8; 32]));
        for (branch, byte, label) in [(&mut doc, 1, "local"), (&mut peer, 2, "peer")] {
            let mut tx = branch.transaction();
            tx.put(&stable, "blob", ScalarValue::Bytes(vec![byte]))
                .unwrap();
            let record = tx.put_object(&stable, "record", ObjType::Map).unwrap();
            tx.put(&record, "label", label).unwrap();
            tx.commit();
        }
        doc.merge(&mut peer).unwrap();
        assert_eq!(doc.get_all(&stable, "blob").unwrap().len(), 2);
        assert_eq!(doc.get_all(&stable, "record").unwrap().len(), 2);

        let metadata = doc.get(&map, "metadata").unwrap().unwrap().1;
        let mut positions = vec![(ROOT, automerge::Prop::from("facet"))];
        for (obj, keys) in [
            (&map, vec!["title", "nested", "metadata"]),
            (&item, vec!["stable"]),
            (&stable, vec!["blob", "text", "counter", "record"]),
            (
                &metadata,
                vec!["createdAt", "updatedAt", "deletedAt", "signatures"],
            ),
        ] {
            positions.extend(keys.into_iter().map(|key| (obj.clone(), key.into())));
        }
        positions.push((nested, 0.into()));
        for (_, record) in doc.get_all(&stable, "record").unwrap() {
            positions.push((record, "label".into()));
        }
        let updated = doc.get(&metadata, "updatedAt").unwrap().unwrap().1;
        positions.push((updated, 0.into()));
        let signatures = doc.get(&metadata, "signatures").unwrap().unwrap().1;
        let signature = doc.get(&signatures, 0).unwrap().unwrap().1;
        positions.extend([
            (signatures, 0.into()),
            (signature.clone(), "signer".into()),
            (signature, "signature".into()),
        ]);
        let snapshot = |doc: &Automerge| {
            positions
                .iter()
                .map(|(obj, prop)| {
                    doc.get_all(obj, prop.clone())
                        .unwrap()
                        .into_iter()
                        .map(|(value, id)| (value.into_owned(), id))
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        };
        let before = snapshot(&doc);
        let mut submitted = hydrate_value(&doc, &ROOT, facet(), &schema).unwrap();
        submitted["nested"][0]["changed"] = json!("after");
        let tx = reconcile_json_atomic(
            doc.transaction(),
            &schema,
            &ROOT,
            facet(),
            submitted.clone(),
        )
        .unwrap();
        assert_eq!(tx.pending_ops(), 1);
        tx.commit();
        assert_eq!(snapshot(&doc), before);
        assert_eq!(
            hydrate_value(&doc, &ROOT, facet(), &schema).unwrap(),
            submitted
        );

        let heads = doc.get_heads();
        let tx =
            reconcile_json_atomic(doc.transaction(), &schema, &ROOT, facet(), submitted).unwrap();
        assert_eq!(tx.pending_ops(), 0);
        assert!(tx.commit().0.is_none());
        assert_eq!(doc.get_heads(), heads);
        assert_eq!(snapshot(&doc), before);
    }
}

#[test]
fn float_noop_preserves_bits_and_conflicts() {
    let float = json!({"type": "string", "x-automerge": "float", "format": "double"});
    let cases = [
        (float.clone(), 1.25, json!("1.25")),
        (float.clone(), 1.25, json!(1.25)),
        (float.clone(), -0.0, json!("-0.0")),
        (float.clone(), 0.0, json!("0.0")),
        (float.clone(), f64::INFINITY, json!("Infinity")),
        (float.clone(), f64::NEG_INFINITY, json!("-Infinity")),
        (float.clone(), f64::NAN, json!("NaN")),
        (
            float.clone(),
            f64::from_bits(0x7ff8_0000_0000_0001),
            json!("NaN"),
        ),
        (float, f64::from_bits(0xfff8_0000_0000_0002), json!("NaN")),
        (json!({"type": "number"}), -0.0, json!(-0.0)),
        (json!({}), 0.0, json!(0.0)),
    ];
    for (document, held, submitted) in cases {
        let schema = super::compiled(&document);
        let mut doc = Automerge::new();
        doc.set_actor(ActorId::from([1u8; 32]));
        let mut tx = doc.transaction();
        tx.put(ROOT, "facet", ScalarValue::Null).unwrap();
        tx.commit();
        let mut peer = doc.fork();
        peer.set_actor(ActorId::from([2u8; 32]));
        for (branch, value) in [(&mut doc, -123.0), (&mut peer, held)] {
            let mut tx = branch.transaction();
            tx.put(ROOT, "facet", ScalarValue::F64(value)).unwrap();
            tx.commit();
        }
        doc.merge(&mut peer).unwrap();
        let snapshot = |doc: &Automerge| {
            doc.get_all(ROOT, "facet")
                .unwrap()
                .into_iter()
                .map(|(value, id)| (value.to_f64().unwrap().to_bits(), id))
                .collect::<Vec<_>>()
        };
        let before = snapshot(&doc);
        assert_eq!(before.len(), 2);
        assert_eq!(
            doc.get(ROOT, "facet")
                .unwrap()
                .unwrap()
                .0
                .to_f64()
                .unwrap()
                .to_bits(),
            held.to_bits()
        );
        let heads = doc.get_heads();
        let tx = reconcile_json_atomic(
            doc.transaction(),
            &schema,
            &ROOT,
            facet(),
            submitted.clone(),
        )
        .unwrap();
        assert_eq!(tx.pending_ops(), 0, "{held:?} resubmitted as {submitted}");
        assert!(tx.commit().0.is_none());
        assert_eq!(doc.get_heads(), heads);
        assert_eq!(snapshot(&doc), before);
    }
}

#[test]
fn positional_signature_replacements_merge_fields_not_entries() {
    let schema = super::compiled(&json!({
        "type": "array", "items": {"type": "object"}
    }));
    for (left, right, expected, conflicts) in [
        (
            json!({"signer": "alice", "signature": {"$bytes": "AQ=="}}),
            json!({"signer": "bob", "signature": {"$bytes": "Ag=="}}),
            json!({"signer": "bob", "signature": {"$bytes": "Ag=="}}),
            2,
        ),
        (
            json!({"signer": "alice", "signature": {"$bytes": "AA=="}}),
            json!({"signer": "base", "signature": {"$bytes": "Ag=="}}),
            json!({"signer": "alice", "signature": {"$bytes": "Ag=="}}),
            1,
        ),
    ] {
        let mut doc = Automerge::new();
        doc.set_actor(ActorId::from([1u8; 32]));
        let tx = reconcile_json_atomic(
            doc.transaction(),
            &schema,
            &ROOT,
            facet(),
            json!([{"signer": "base", "signature": {"$bytes": "AA=="}}]),
        )
        .unwrap();
        tx.commit();
        let list = doc.get(ROOT, "facet").unwrap().unwrap().1;
        let entry = doc.get(&list, 0).unwrap().unwrap().1;
        let mut peer = doc.fork();
        peer.set_actor(ActorId::from([2u8; 32]));
        for (branch, replacement) in [(&mut doc, left), (&mut peer, right)] {
            let tx = reconcile_json_atomic(
                branch.transaction(),
                &schema,
                &ROOT,
                facet(),
                json!([replacement]),
            )
            .unwrap();
            assert_eq!(tx.pending_ops(), conflicts);
            tx.commit();
        }
        let mut reverse = peer.clone();
        reverse.merge(&mut doc.clone()).unwrap();
        doc.merge(&mut peer).unwrap();
        for merged in [&doc, &reverse] {
            assert_eq!(merged.get(ROOT, "facet").unwrap().unwrap().1, list);
            assert_eq!(merged.length(&list), 1);
            assert_eq!(merged.get(&list, 0).unwrap().unwrap().1, entry);
            assert_eq!(merged.get_all(&list, 0).unwrap().len(), 1);
            assert_eq!(merged.get_all(&entry, "signer").unwrap().len(), conflicts);
            assert_eq!(
                merged.get_all(&entry, "signature").unwrap().len(),
                conflicts
            );
            assert_eq!(
                hydrate_value(merged, &ROOT, facet(), &schema).unwrap(),
                json!([expected])
            );
        }
    }
}

#[test]
fn invalid_late_content_is_preflighted_before_any_write() {
    let document = open_document();
    let mut doc = Automerge::new();
    let heads = {
        let mut tx = doc.transaction();
        let map = tx.put_object(ROOT, "facet", ObjType::Map).unwrap();
        tx.put(&map, "title", "base").unwrap();
        let (heads, _) = tx.commit();
        heads.expect("a creator change commits")
    };

    let mut tx = doc
        .transaction_at(automerge::PatchLog::null(), &[heads])
        .unwrap();
    let error = reconcile_json(
        &mut tx,
        &ROOT,
        facet(),
        &document,
        json!({"title": "changed", "note": {"$application": true}}),
    )
    .expect_err("application dollar keys are refused");
    assert!(error.to_string().contains("$application"), "{error}");
    assert_eq!(
        tx.pending_ops(),
        0,
        "preflight refused before writing title"
    );
}
