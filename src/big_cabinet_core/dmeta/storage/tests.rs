use super::*;
use crate::datum::Bundle;
use crate::datum::Declaration;
use crate::datum::Universe;
use crate::dmeta::ActorMetadata;
use crate::dmeta::Bytes;
use crate::dmeta::DatumMetadata;
use crate::dmeta::DatumSignature;
use crate::dmeta::InheritedWitness;
use crate::dmeta::MultibaseKey;
use crate::dmeta::SignatureBytes;
use crate::dmeta::Uuid7;
use automerge::ActorId;
use automerge::Automerge;
use automerge::ROOT;
use serde_json::json;

fn schema() -> DmetaSchema {
    let universe = Universe::collect(vec![Declaration::of::<Dmeta>().unwrap()]);
    let bundle = Bundle::new(&universe, Vec::new());
    DmetaSchema::new(
        CompiledSchema::compile(serde_json::to_value(bundle.document("Dmeta").unwrap()).unwrap())
            .unwrap(),
    )
    .unwrap()
}

fn fixture() -> Dmeta {
    let mut dmeta = Dmeta::new(jiff::Timestamp::from_millisecond(0).unwrap());
    let actor = Uuid7::new();
    dmeta.actors.insert(
        actor,
        ActorMetadata {
            extra: Default::default(),
            created_at: Vec::new(),
            pubkey: MultibaseKey::try_from([1; 32].as_slice()).unwrap(),
            actor_url: "actor:alice".to_owned(),
            delegation: Some(Bytes::from(vec![2, 3])),
        },
    );
    dmeta
        .doc_uuids
        .insert(Uuid7::new(), crate::dmeta::DocId::new(b"doc"));
    let key = "org.example.note/main".parse().unwrap();
    dmeta.datums.insert(
        key,
        DatumMetadata {
            uuids: BTreeMap::from([(Uuid7::new(), true)]),
            extra: Default::default(),
            updated_at: Vec::new(),
            deleted_at: Vec::new(),
            signatures: Vec::new(),
        },
    );
    dmeta.extra.insert(
        "nested".to_owned(),
        json!({
            "stable": [{"blob": {"$bytes": "AQI="}, "label": "unchanged"}],
            "changed": "before"
        }),
    );
    dmeta
}

fn signature(dmeta: &Dmeta, byte: u8) -> DatumSignature {
    DatumSignature {
        extra: Default::default(),
        actor_uuid: *dmeta.actors.keys().next().unwrap(),
        doc_uuid: *dmeta.doc_uuids.keys().next().unwrap(),
        signature: SignatureBytes::try_from([byte; 64].as_slice()).unwrap(),
        message: Some(Bytes::from(vec![byte, 0])),
        inherited: Some(InheritedWitness {
            extra: Default::default(),
            origin_doc: crate::dmeta::DocId::new(vec![byte]),
            message: Bytes::from(vec![byte, 1]),
            value: Some(Bytes::from(vec![byte, 2])),
        }),
    }
}

/// The write a repository write makes: every datum the value holds metadata for, as the
/// fixture's actor on the fixture's document.
fn named(dmeta: &Dmeta) -> (BTreeSet<crate::DatumKey>, Uuid7, Uuid7) {
    (
        dmeta.datums.keys().cloned().collect(),
        *dmeta.actors.keys().next().unwrap(),
        *dmeta.doc_uuids.keys().next().unwrap(),
    )
}

fn write_atomic<'a>(
    tx: automerge::transaction::Transaction<'a>,
    schema: &DmetaSchema,
    dmeta: &Dmeta,
    (datums, actor, doc_uuid): &(BTreeSet<crate::DatumKey>, Uuid7, Uuid7),
) -> Result<automerge::transaction::Transaction<'a>, StorageError> {
    dmeta.write_atomic(
        tx,
        schema,
        &ROOT,
        "dmeta".into(),
        &DmetaWrite {
            datums,
            actor: *actor,
            doc_uuid: *doc_uuid,
        },
    )
}

fn write(doc: &mut Automerge, schema: &DmetaSchema, dmeta: &Dmeta) -> usize {
    let tx = write_atomic(doc.transaction(), schema, dmeta, &named(dmeta)).unwrap();
    let count = tx.pending_ops();
    tx.commit();
    count
}

fn read(doc: &Automerge, schema: &DmetaSchema) -> Dmeta {
    Dmeta::hydrate(doc, &ROOT, "dmeta".into(), schema.root()).unwrap()
}

fn objects(doc: &Automerge) -> (ObjId, ObjId, [ObjId; 4]) {
    let root = doc.get(ROOT, "dmeta").unwrap().unwrap().1;
    let datums = doc.get(&root, "datums").unwrap().unwrap().1;
    let datum = doc
        .get(&datums, "org.example.note/main")
        .unwrap()
        .unwrap()
        .1;
    let lists = [
        doc.get(&root, "updatedAt").unwrap().unwrap().1,
        doc.get(&datum, "updatedAt").unwrap().unwrap().1,
        doc.get(&datum, "deletedAt").unwrap().unwrap().1,
        doc.get(&datum, "signatures").unwrap().unwrap().1,
    ];
    (root, datum, lists)
}

#[test]
fn a_first_write_lays_down_dmeta_and_later_writes_only_what_they_name() {
    let schema = schema();
    let mut value = fixture();
    let mut doc = Automerge::new();
    assert!(write(&mut doc, &schema, &value) > 0);
    let before = objects(&doc);
    for list in &before.2 {
        assert_eq!(doc.object_type(list).unwrap(), ObjType::List);
        assert_eq!(doc.length(list), 0);
    }
    assert_eq!(read(&doc, &schema), value);
    let nested = doc.get(&before.0, "nested").unwrap().unwrap().1;
    let heads = doc.get_heads();
    // Unknown root members are not part of any write after the first: a change to them in the
    // typed value is not written, and neither is anything else the write does not name.
    value.extra.get_mut("nested").unwrap()["changed"] = json!("after");
    value.created_at = jiff::Timestamp::from_millisecond(5).unwrap();
    assert_eq!(write(&mut doc, &schema, &value), 0);
    assert_eq!(doc.get_heads(), heads);
    assert_eq!(objects(&doc), before);
    assert_eq!(doc.get(&before.0, "nested").unwrap().unwrap().1, nested);
    let stored = read(&doc, &schema);
    assert_eq!(stored.extra["nested"]["changed"], json!("before"));
    assert_eq!(stored.created_at, fixture().created_at);
}

#[test]
fn nested_unknown_metadata_survives_native_round_trips_and_known_updates() {
    let schema = schema();
    let mut value = fixture();
    value
        .actors
        .values_mut()
        .next()
        .unwrap()
        .extra
        .insert("future".to_owned(), json!({"actor": [1, true]}));
    let mut entry = signature(&value, 1);
    entry
        .extra
        .insert("future".to_owned(), json!({"signature": "kept"}));
    entry.inherited.as_mut().unwrap().extra.insert(
        "future".to_owned(),
        json!({"witness": [null, {"$bytes": "AQI="}]}),
    );
    let metadata = value.datums.values_mut().next().unwrap();
    metadata
        .extra
        .insert("future".to_owned(), json!({"datum": true}));
    metadata.signatures.push(entry);
    let mut doc = Automerge::new();
    write(&mut doc, &schema, &value);
    let mut restored = read(&doc, &schema);
    assert_eq!(restored, value);
    restored.updated_at = vec![jiff::Timestamp::from_second(1).unwrap()];
    restored.datums.values_mut().next().unwrap().updated_at = restored.updated_at.clone();
    assert!(write(&mut doc, &schema, &restored) > 0);
    assert_eq!(read(&doc, &schema), restored);
    assert_eq!(write(&mut doc, &schema, &restored), 0);
}

#[test]
fn timestamps_replace_instead_of_growing_and_resurrection_clears_observed_deletions() {
    let schema = schema();
    let mut value = fixture();
    let mut doc = Automerge::new();
    write(&mut doc, &schema, &value);
    let before = objects(&doc);
    let first = jiff::Timestamp::from_millisecond(1000).unwrap();
    let second = jiff::Timestamp::from_millisecond(2000).unwrap();
    value.updated_at = vec![first];
    value.datums.values_mut().next().unwrap().updated_at = vec![first];
    assert_eq!(write(&mut doc, &schema, &value), 2);
    value.updated_at = vec![second];
    value.datums.values_mut().next().unwrap().updated_at = vec![second];
    assert_eq!(write(&mut doc, &schema, &value), 4);
    assert_eq!(read(&doc, &schema), value);
    value.datums.values_mut().next().unwrap().deleted_at = vec![second];
    assert_eq!(write(&mut doc, &schema, &value), 1);
    value.datums.values_mut().next().unwrap().deleted_at.clear();
    assert_eq!(write(&mut doc, &schema, &value), 1);
    assert_eq!(read(&doc, &schema), value);
    assert_eq!(objects(&doc), before);
    assert_eq!(doc.length(&before.2[2]), 0);
    assert_eq!(write(&mut doc, &schema, &value), 0);
}

utils_rs::table_tests! {
    concurrently_created_metadata,
    replaces,
    {
        let schema = schema();
        let key: crate::DatumKey = "org.example.note/main".parse().unwrap();
        let mut initial = fixture();
        initial.datums.clear();
        let mut doc = Automerge::new();
        doc.set_actor(ActorId::from([1u8; 32]));
        write(&mut doc, &schema, &initial);
        let mut peer = doc.fork();
        peer.set_actor(ActorId::from([2u8; 32]));
        let aliases = [Uuid7::new(), Uuid7::new()];
        for (branch, byte) in [(&mut doc, 1), (&mut peer, 2)] {
            let mut created = initial.clone();
            created.datums.insert(key.clone(), DatumMetadata {
                uuids: BTreeMap::from([(aliases[byte as usize - 1], true)]),
                extra: Default::default(),
                updated_at: Vec::new(),
                deleted_at: Vec::new(),
                signatures: vec![signature(&initial, byte)],
            });
            write(branch, &schema, &created);
        }
        doc.merge(&mut peer).unwrap();
        let lists = |doc: &Automerge| -> Vec<usize> {
            let root = doc.get(ROOT, "dmeta").unwrap().unwrap().1;
            let datums = doc.get(&root, "datums").unwrap().unwrap().1;
            let mut lengths: Vec<_> = doc
                .get_all(&datums, key.to_string())
                .unwrap()
                .into_iter()
                .map(|(_, metadata)| {
                    let list = doc.get(&metadata, "signatures").unwrap().unwrap().1;
                    doc.length(&list)
                })
                .collect();
            lengths.sort();
            lengths
        };
        assert_eq!(lists(&doc), [1, 1]);
        // Each creator's alias lives in its own copy of the entry; the visible copy shows one, and
        // the read unions both, so the chosen alias is the smaller of the two whichever copy wins.
        let mut observed = read(&doc, &schema);
        assert_eq!(
            observed.datums[&key].uuids.keys().collect::<Vec<_>>(),
            aliases.iter().collect::<BTreeSet<_>>().into_iter().collect::<Vec<_>>()
        );
        assert!(observed.has_concurrent_creation(&key));
        if replaces {
            observed.datums.get_mut(&key).unwrap().signatures = vec![signature(&initial, 3)];
        }
        let (_, actor, doc_uuid) = named(&observed);
        let datums = if replaces { BTreeSet::from([key.clone()]) } else { BTreeSet::new() };
        write_atomic(doc.transaction(), &schema, &observed, &(datums, actor, doc_uuid))
            .unwrap()
            .commit();
        assert_eq!(lists(&doc), if replaces { [0, 1] } else { [1, 1] });
        assert_eq!(read(&doc, &schema), observed);
    }
}

concurrently_created_metadata! {
    a_write_naming_the_datum_retires_the_losing_copy: true,
    a_write_not_naming_it_leaves_the_losing_copy_alone: false,
}

#[test]
fn concurrent_actor_introductions_keep_every_created_at() {
    let schema = schema();
    let initial = fixture();
    let (uuid, binding) = initial.actors.iter().next().unwrap();
    let (uuid, binding) = (*uuid, binding.clone());
    let first = jiff::Timestamp::from_millisecond(1000).unwrap();
    let second = jiff::Timestamp::from_millisecond(2000).unwrap();
    // The base binds only carol, so alice's binding is introduced concurrently on both sides.
    let mut base = initial.clone();
    base.actors.clear();
    let carol = Uuid7::new();
    base.actors.insert(
        carol,
        ActorMetadata {
            pubkey: MultibaseKey::try_from([7; 32].as_slice()).unwrap(),
            actor_url: "actor:carol".to_owned(),
            ..binding.clone()
        },
    );
    let mut left = base.clone();
    left.actors.insert(
        uuid,
        ActorMetadata {
            created_at: vec![first],
            ..binding.clone()
        },
    );
    let mut right = base.clone();
    right.actors.insert(
        uuid,
        ActorMetadata {
            created_at: vec![second],
            ..binding
        },
    );

    let mut doc = Automerge::new();
    doc.set_actor(ActorId::from([1u8; 32]));
    write(&mut doc, &schema, &base);
    let mut peer = doc.fork();
    peer.set_actor(ActorId::from([2u8; 32]));
    let (datums, _, doc_uuid) = named(&base);
    let as_alice = (datums, uuid, doc_uuid);
    write_atomic(doc.transaction(), &schema, &left, &as_alice)
        .unwrap()
        .commit();
    write_atomic(peer.transaction(), &schema, &right, &as_alice)
        .unwrap()
        .commit();
    doc.merge(&mut peer).unwrap();

    let observed = read(&doc, &schema);
    let actor = &observed.actors[&uuid];
    assert_eq!(actor.created_at.len(), 2);
    assert!(actor.created_at.contains(&first));
    assert!(actor.created_at.contains(&second));
    assert_eq!(actor.pubkey, left.actors[&uuid].pubkey);
    assert_eq!(actor.actor_url, left.actors[&uuid].actor_url);
    assert_eq!(actor.delegation, left.actors[&uuid].delegation);
    write_atomic(doc.transaction(), &schema, &observed, &as_alice)
        .unwrap()
        .commit();
    let created_at = &read(&doc, &schema).actors[&uuid].created_at;
    assert_eq!(created_at.len(), 2);
    assert!(created_at.contains(&first));
    assert!(created_at.contains(&second));
}

#[test]
fn concurrent_replacements_keep_both_signatures_intact_at_supplied_heads() {
    let schema = schema();
    let mut initial = fixture();
    let first = signature(&initial, 1);
    initial.updated_at = vec![initial.created_at];
    let metadata = initial.datums.values_mut().next().unwrap();
    metadata.signatures = vec![first];
    metadata.updated_at = vec![initial.created_at];
    metadata.deleted_at = vec![initial.created_at];
    let mut doc = Automerge::new();
    doc.set_actor(ActorId::from([1u8; 32]));
    write(&mut doc, &schema, &initial);
    let base = doc.get_heads();
    let before = objects(&doc);
    let old_entry = doc.get(&before.2[3], 0).unwrap().unwrap().1;
    let mut peer = doc.fork();
    peer.set_actor(ActorId::from([2u8; 32]));
    let mut left = initial.clone();
    let mut right = initial.clone();
    left.updated_at = vec![jiff::Timestamp::from_millisecond(1000).unwrap()];
    right.updated_at = vec![jiff::Timestamp::from_millisecond(2000).unwrap()];
    left.datums.values_mut().next().unwrap().signatures = vec![signature(&initial, 2)];
    right.datums.values_mut().next().unwrap().signatures = vec![signature(&initial, 3)];
    left.datums.values_mut().next().unwrap().updated_at = left.updated_at.clone();
    right.datums.values_mut().next().unwrap().updated_at = right.updated_at.clone();
    left.datums.values_mut().next().unwrap().deleted_at.clear();
    right.datums.values_mut().next().unwrap().deleted_at = right.updated_at.clone();
    let bob = Uuid7::new();
    right.actors.insert(
        bob,
        ActorMetadata {
            extra: Default::default(),
            created_at: Vec::new(),
            pubkey: MultibaseKey::try_from([4; 32].as_slice()).unwrap(),
            actor_url: "actor:bob".to_owned(),
            delegation: None,
        },
    );
    right
        .datums
        .values_mut()
        .next()
        .unwrap()
        .uuids
        .insert(Uuid7::new(), true);
    let (datums, _, doc_uuid) = named(&right);
    write_atomic(
        peer.transaction(),
        &schema,
        &right,
        &(datums, bob, doc_uuid),
    )
    .unwrap()
    .commit();
    doc.merge(&mut peer).unwrap();
    let tx = doc
        .transaction_at(automerge::PatchLog::null(), &base)
        .unwrap();
    write_atomic(tx, &schema, &left, &named(&left))
        .unwrap()
        .commit();
    peer.merge(&mut doc).unwrap();
    for merged in [&doc, &peer] {
        assert_eq!(objects(merged), before);
        let observed = read(merged, &schema);
        let signatures = &observed.datums.values().next().unwrap().signatures;
        assert_eq!(signatures.len(), 2);
        assert!(signatures.contains(&signature(&initial, 2)));
        assert!(signatures.contains(&signature(&initial, 3)));
        assert_eq!(observed.updated_at.len(), 2);
        assert!(observed.updated_at.contains(&left.updated_at[0]));
        assert!(observed.updated_at.contains(&right.updated_at[0]));
        let metadata = observed.datums.values().next().unwrap();
        assert_eq!(metadata.updated_at.len(), 2);
        assert!(metadata.updated_at.contains(&left.updated_at[0]));
        assert!(metadata.updated_at.contains(&right.updated_at[0]));
        assert_eq!(metadata.deleted_at, right.updated_at);
        assert_eq!(observed.actors, right.actors);
        assert_eq!(
            observed.datums.values().next().unwrap().uuids,
            right.datums.values().next().unwrap().uuids
        );
        for index in 0..2 {
            let entry = merged.get(&before.2[3], index).unwrap().unwrap().1;
            assert_ne!(entry, old_entry);
            assert_eq!(merged.get_all(&before.2[3], index).unwrap().len(), 1);
            assert_eq!(merged.get_all(&entry, "signature").unwrap().len(), 1);
            assert!(
                matches!(merged.get(&entry, "signature").unwrap().unwrap().0.to_scalar(),
                Some(ScalarValue::Bytes(bytes)) if bytes.len() == 64)
            );
            assert!(matches!(
                merged
                    .get(&entry, "message")
                    .unwrap()
                    .unwrap()
                    .0
                    .to_scalar(),
                Some(ScalarValue::Bytes(_))
            ));
        }
    }
    let merged = read(&doc, &schema);
    assert_eq!(write(&mut doc, &schema, &merged), 0);
}

#[test]
fn set_order_duplicates_and_timestamp_precision_do_not_rewrite_equal_entries() {
    let schema = schema();
    let mut value = fixture();
    let first = signature(&value, 1);
    let second = signature(&value, 2);
    value.datums.values_mut().next().unwrap().signatures =
        vec![first.clone(), second.clone(), first.clone()];
    let fractional: jiff::Timestamp = "2024-01-01T00:00:00.123456Z".parse().unwrap();
    value.updated_at = vec![fractional, fractional];
    let mut doc = Automerge::new();
    write(&mut doc, &schema, &value);
    let lists = objects(&doc).2;
    assert_eq!(doc.length(&lists[0]), 1);
    assert_eq!(doc.length(&lists[3]), 2);
    let entries = (0..2)
        .map(|index| doc.get(&lists[3], index).unwrap().unwrap().1)
        .collect::<Vec<_>>();
    value.datums.values_mut().next().unwrap().signatures = vec![second, first];
    assert_eq!(write(&mut doc, &schema, &value), 0);
    assert_eq!(
        (0..2)
            .map(|index| doc.get(&lists[3], index).unwrap().unwrap().1)
            .collect::<Vec<_>>(),
        entries
    );
    let mut tx = doc.transaction();
    tx.insert(
        &lists[0],
        1,
        ScalarValue::Timestamp(fractional.as_millisecond()),
    )
    .unwrap();
    tx.commit();
    assert_eq!(write(&mut doc, &schema, &value), 1);
    assert_eq!(doc.length(&lists[0]), 1);
    assert_eq!(write(&mut doc, &schema, &value), 0);
}

#[test]
fn a_hidden_conflicting_binding_is_not_silently_rebound_to_the_winner() {
    let schema = schema();
    for index in ["actors", "docUuids"] {
        let initial = fixture();
        let mut doc = Automerge::new();
        doc.set_actor(ActorId::from([1u8; 32]));
        write(&mut doc, &schema, &initial);
        let root = objects(&doc).0;
        let bindings = doc.get(&root, index).unwrap().unwrap().1;
        let uuid = match index {
            "actors" => initial.actors.keys().next().unwrap().to_string(),
            _ => initial.doc_uuids.keys().next().unwrap().to_string(),
        };
        let (object, prop) = if index == "actors" {
            (
                doc.get(&bindings, uuid.as_str()).unwrap().unwrap().1,
                "pubkey".to_owned(),
            )
        } else {
            (bindings, uuid)
        };
        let mut peer = doc.fork();
        peer.set_actor(ActorId::from([2u8; 32]));
        for (branch, byte) in [(&mut doc, 2), (&mut peer, 3)] {
            let replacement = match index {
                "actors" => MultibaseKey::try_from([byte; 32].as_slice())
                    .unwrap()
                    .to_string(),
                _ => crate::dmeta::DocId::new([byte]).to_string(),
            };
            let mut tx = branch.transaction();
            tx.put(&object, prop.as_str(), replacement).unwrap();
            tx.commit();
        }
        doc.merge(&mut peer).unwrap();
        let observed = read(&doc, &schema);
        let heads = doc.get_heads();
        assert_eq!(doc.get_all(&object, prop.as_str()).unwrap().len(), 2);
        // The write's own actor and `docUuid` are the conflicted bindings.
        let error =
            write_atomic(doc.transaction(), &schema, &observed, &named(&observed)).unwrap_err();
        assert!(
            matches!(error, StorageError::ReboundBinding { index: actual, .. } if actual == index)
        );
        assert_eq!(doc.get_heads(), heads);
        assert_eq!(doc.get_all(&object, prop.as_str()).unwrap().len(), 2);
    }
}

#[test]
fn rebound_bindings_and_late_codec_errors_roll_back_the_owned_transaction() {
    let schema = schema();
    let initial = fixture();
    let main: crate::DatumKey = "org.example.note/main".parse().unwrap();
    let mut doc = Automerge::new();
    write(&mut doc, &schema, &initial);
    let heads = doc.get_heads();
    let refused = |doc: &mut Automerge, submitted: &Dmeta, named: (BTreeSet<_>, Uuid7, Uuid7)| {
        let mut tx = doc.transaction();
        tx.put(ROOT, "pending", "must roll back").unwrap();
        let error = write_atomic(tx, &schema, submitted, &named).unwrap_err();
        assert!(doc.get(ROOT, "pending").unwrap().is_none());
        error
    };
    for index in ["actors", "docUuids"] {
        let mut submitted = initial.clone();
        let named = named(&initial);
        match index {
            "actors" => {
                submitted.actors.values_mut().next().unwrap().pubkey =
                    MultibaseKey::try_from([9; 32].as_slice()).unwrap();
            }
            "docUuids" => {
                *submitted.doc_uuids.values_mut().next().unwrap() =
                    crate::dmeta::DocId::new(b"other");
            }
            _ => unreachable!(),
        }
        let error = refused(&mut doc, &submitted, named);
        assert!(
            matches!(error, StorageError::ReboundBinding { index: actual, .. } if actual == index),
            "{index}: {error:?}"
        );
        assert_eq!(doc.get_heads(), heads);
        assert_eq!(read(&doc, &schema), initial);
    }
    // A named datum's metadata is checked against its member schema.
    let mut invalid = initial.clone();
    invalid
        .datums
        .get_mut(&main)
        .unwrap()
        .extra
        .insert("$invalid".to_owned(), json!(true));
    assert!(matches!(
        refused(&mut doc, &invalid, named(&invalid)),
        StorageError::Codec(_)
    ));
    assert_eq!(doc.get_heads(), heads);
    // A late, target-aware codec error rolls back the list reconciliation that preceded it.
    let datum = objects(&doc).1;
    let mut tx = doc.transaction();
    tx.put(&datum, "opaque", ScalarValue::Bytes(vec![1]))
        .unwrap();
    tx.commit();
    let heads = doc.get_heads();
    let mut submitted = initial.clone();
    submitted.updated_at = vec![jiff::Timestamp::from_millisecond(1000).unwrap()];
    submitted
        .datums
        .get_mut(&main)
        .unwrap()
        .extra
        .insert("opaque".to_owned(), json!("not bytes"));
    assert!(matches!(
        refused(&mut doc, &submitted, named(&submitted)),
        StorageError::Codec(_)
    ));
    assert_eq!(doc.get_heads(), heads);
    assert_eq!(doc.length(&objects(&doc).2[0]), 0);
}

#[test]
fn a_partial_read_hydrates_only_what_the_write_names() {
    let schema = schema();
    let mut dmeta = fixture();
    let other: crate::DatumKey = "org.example.other/main".parse().unwrap();
    dmeta
        .doc_uuids
        .insert(Uuid7::new(), crate::dmeta::DocId::new(b"elsewhere"));
    let mut other_metadata = dmeta.datums.values().next().unwrap().clone();
    other_metadata.uuids = BTreeMap::from([(Uuid7::new(), true)]);
    dmeta.datums.insert(other, other_metadata);
    let mut doc = Automerge::new();
    // Bind everything the fixture holds, the unrelated datum and document included.
    let all: BTreeSet<_> = dmeta.datums.keys().cloned().collect();
    let (_, actor, doc_uuid) = named(&dmeta);
    write_atomic(doc.transaction(), &schema, &dmeta, &(all, actor, doc_uuid))
        .unwrap()
        .commit();
    let full = read(&doc, &schema);
    let key: crate::DatumKey = "org.example.note/main".parse().unwrap();
    let document = crate::dmeta::DocId::new(b"doc");

    let touched = BTreeSet::from([key.clone()]);
    let partial = Dmeta::hydrate_for(
        &doc,
        &ROOT,
        "dmeta".into(),
        &schema,
        &DmetaRead {
            datums: &touched,
            actor,
            document: &document,
        },
    )
    .unwrap();

    assert_eq!(partial.created_at, full.created_at);
    assert_eq!(
        partial.datums,
        BTreeMap::from([(key.clone(), full.datums[&key].clone())])
    );
    assert_eq!(
        partial.doc_uuids,
        full.doc_uuids
            .iter()
            .filter(|(_, bound)| **bound == document)
            .map(|(uuid, bound)| (*uuid, bound.clone()))
            .collect::<BTreeMap<_, _>>()
    );
    assert_eq!(partial.actors.keys().collect::<Vec<_>>(), vec![&actor]);
    assert!(partial.updated_at.is_empty());
}

/// The writer's alias choice reads the same union the verifier reconstructs: every copy of
/// `datums`, not just the visible one, each with its entry's aliases.
#[test]
fn hydrate_for_unions_aliases_across_concurrent_datums_maps() {
    let schema = schema();
    let key: crate::DatumKey = "org.example.note/main".parse().unwrap();
    let mut initial = fixture();
    initial.datums.clear();
    let mut doc = Automerge::new();
    doc.set_actor(ActorId::from([1u8; 32]));
    write(&mut doc, &schema, &initial);
    let mut peer = doc.fork();
    peer.set_actor(ActorId::from([2u8; 32]));
    let aliases = [Uuid7::new(), Uuid7::new()];
    for (branch, alias) in [(&mut doc, aliases[0]), (&mut peer, aliases[1])] {
        let root = branch.get(ROOT, "dmeta").unwrap().unwrap().1;
        let mut tx = branch.transaction();
        let datums = tx.put_object(&root, "datums", ObjType::Map).unwrap();
        let entry = tx
            .put_object(&datums, key.to_string(), ObjType::Map)
            .unwrap();
        for list in ["updatedAt", "deletedAt", "signatures"] {
            tx.put_object(&entry, list, ObjType::List).unwrap();
        }
        let uuids = tx.put_object(&entry, "uuids", ObjType::Map).unwrap();
        tx.put(&uuids, alias.to_string(), true).unwrap();
        tx.commit();
    }
    doc.merge(&mut peer).unwrap();
    let root = doc.get(ROOT, "dmeta").unwrap().unwrap().1;
    assert_eq!(doc.get_all(&root, "datums").unwrap().len(), 2);
    let (_, actor, _) = named(&initial);
    let document = crate::dmeta::DocId::new(b"doc");
    let touched = BTreeSet::from([key.clone()]);
    let partial = Dmeta::hydrate_for(
        &doc,
        &ROOT,
        "dmeta".into(),
        &schema,
        &DmetaRead {
            datums: &touched,
            actor,
            document: &document,
        },
    )
    .unwrap();
    assert_eq!(
        partial.datums[&key].uuids.keys().collect::<BTreeSet<_>>(),
        aliases.iter().collect::<BTreeSet<_>>()
    );
}
