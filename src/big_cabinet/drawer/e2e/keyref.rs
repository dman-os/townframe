//! Key references (ADR 035 §2.9): a `#keyRef` names a content-addressed datum by its key alone, and
//! resolves to whichever readable document holds a copy that hashes to the key.

use super::*;
use crate::schema::SchemaRegistration;
use crate::schema::SchemaRegistrationGroup;
use big_cabinet_core::datum::Bundle;
use big_cabinet_core::datum::Bytes;
use big_cabinet_core::datum::DatumDecl;
use big_cabinet_core::datum::Universe;
use big_cabinet_core::doc::ResolveKeyRef;
use big_cabinet_core::doc::ResolvedKeyRef;

const BLOB: &str = "org.example.blob";
const POINTER: &str = "org.example.pointer";
/// `printf 'hi\n' | git hash-object --stdin`: the git object id of the three bytes `hi\n`.
const HI: &str = "45b983be36b73c0788dc9cbcb76cbb80fc7bb057";

fn blob_key(slot: &str) -> DatumKey {
    DatumKey::from_text(BLOB, slot).unwrap()
}

/// A blob whose `raw` bytes are `bytes`.
fn blob(bytes: &[u8]) -> Value {
    json!({"$type": BLOB, "raw": serde_json::to_value(Bytes(bytes.to_vec())).unwrap()})
}

fn pointer(key: &DatumKey) -> (DatumKey, Value) {
    (
        DatumKey::from_text(POINTER, "main").unwrap(),
        json!({"$type": POINTER, "target": {"key": key.to_string()}}),
    )
}

/// Register the blob kind as `owner`'s, replacing its earlier registration. A node that has not
/// seen the sidecar's `x-content-hash` stores whatever is written under any key, which is how a
/// copy that does not hash to its key reaches a node that later learns the declaration.
fn register_blob(node: &Node, declared: bool) {
    let mut schema = json!({
        "type": "object",
        "properties": {"raw": {
            "type": "object",
            "properties": {"$bytes": {"type": "string", "contentEncoding": "base64"}},
            "required": ["$bytes"],
            "additionalProperties": false,
            "x-automerge": "bytes",
        }},
        "required": ["raw"],
    });
    if declared {
        schema["x-content-hash"] =
            json!({"algorithm": "gitObject", "type": "blob", "content": "raw"});
    }
    let universe = Universe {
        datums: vec![DatumDecl {
            id: BLOB.to_owned(),
            name: "Blob".to_owned(),
            schema: schemars::Schema::try_from(schema).unwrap(),
            flags: Vec::new(),
        }],
        types: Vec::new(),
    };
    let lexicon = json!({
        "lexicon": 1,
        "id": BLOB,
        "defs": {"main": {"type": "record", "key": "any", "record": {
            "type": "object",
            "required": ["raw"],
            "properties": {"raw": {"type": "bytes"}}
        }}}
    });
    node.schemas
        .register(
            "blob",
            SchemaRegistrationGroup::new(
                Some(Bundle::new(&universe, Vec::new())),
                [SchemaRegistration::new(BLOB, lexicon)],
                Vec::new(),
            ),
        )
        .unwrap();
}

fn register_pointer(node: &Node) {
    let lexicon = json!({
        "lexicon": 1,
        "id": POINTER,
        "defs": {"main": {"type": "record", "key": "any", "record": {
            "type": "object",
            "required": ["target"],
            "properties": {"target": {"type": "ref", "ref": "org.example.daybook.defs#keyRef"}}
        }}}
    });
    node.schemas
        .register(
            "pointer",
            SchemaRegistrationGroup::new(
                None,
                [SchemaRegistration::new(POINTER, lexicon)],
                Vec::new(),
            ),
        )
        .unwrap();
}

impl Node {
    async fn resolve(
        &self,
        reader: &OpenedActor,
        readable: &[&DocId],
        key: &DatumKey,
    ) -> Result<ResolvedKeyRef, DocsError> {
        let capability = self
            .mint_doc(
                reader,
                DocScope {
                    read: readable.iter().map(|doc| (*doc).clone()).collect(),
                    ..Default::default()
                },
            )
            .await?;
        self.docs()
            .call(
                RequestContext::root(),
                ResolveKeyRef {
                    capability,
                    key: key.clone(),
                },
            )
            .await
            .map_err(docs_op)
    }

    /// The `#keyRef` referrers of `key` that `reader` may see, with `readable` as its read scope.
    async fn referrers_of(
        &self,
        reader: &OpenedActor,
        readable: &[&DocId],
        key: &DatumKey,
    ) -> Vec<QueriedDatumRef> {
        let capability = self
            .mint_doc(
                reader,
                DocScope {
                    read: readable.iter().map(|doc| (*doc).clone()).collect(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        self.docs()
            .call(
                RequestContext::root(),
                QueryDatumRefs {
                    capability,
                    drawer_id: None,
                    selector: DatumRefSelector::Key { key: key.clone() },
                },
            )
            .await
            .unwrap()
            .refs
    }

    /// Wait until the datum-set index carries `key` in each of `docs`.
    async fn wait_holders(&self, key: &DatumKey, docs: &[&DocId]) {
        for doc in docs {
            self.wait_indexed(key.tag().as_str(), |route, membership| {
                route.doc_id == **doc
                    && route.datum_key == *key
                    && matches!(membership, DatumMembership::Present { .. })
            })
            .await;
        }
    }

    async fn remove_datum(&self, actor: &OpenedActor, doc: &DocId, key: &DatumKey) {
        self.docs()
            .call(
                RequestContext::root(),
                WriteDatums {
                    capability: self
                        .mint_doc(
                            actor,
                            DocScope {
                                write: std::collections::BTreeMap::from([(
                                    doc.clone(),
                                    big_cabinet_core::doc::DatumKinds::All,
                                )]),
                                ..Default::default()
                            },
                        )
                        .await
                        .unwrap(),
                    command: None,
                    doc_id: doc.clone(),
                    mode: WriteMode::AtCurrent,
                    set: BTreeMap::new(),
                    remove: BTreeSet::from([key.clone()]),
                },
            )
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn a_key_resolves_through_any_holder_and_survives_deleting_one_copy() {
    let node = Node::boot().await;
    register_blob(&node, true);
    let alice = node.creator("alice").await;
    let drawer = node.collection(&alice, "blobs").await;
    let key = blob_key(HI);
    let mut holders = Vec::new();
    for name in ["first", "second"] {
        holders.push(
            node.create_doc_with(&alice, &drawer, name, [(key.clone(), blob(b"hi\n"))])
                .await
                .unwrap(),
        );
    }
    let [first, second] = <[DocId; 2]>::try_from(holders).unwrap();
    node.wait_holders(&key, &[&first, &second]).await;

    // Either holder is the datum: the key is all the reference states.
    let resolved = node
        .resolve(&alice, &[&first, &second], &key)
        .await
        .unwrap();
    assert!([&first, &second].contains(&&resolved.doc_id));
    assert_eq!(resolved.value, blob(b"hi\n"));

    // Deleting the copy it resolved through leaves the other.
    let survivor = if resolved.doc_id == first {
        &second
    } else {
        &first
    };
    node.remove_datum(&alice, &resolved.doc_id, &key).await;
    let again = node
        .resolve(&alice, &[&first, &second], &key)
        .await
        .unwrap();
    assert_eq!(&again.doc_id, survivor);

    // With the last copy gone nothing holds it.
    node.remove_datum(&alice, survivor, &key).await;
    assert_eq!(
        node.resolve(&alice, &[&first, &second], &key).await,
        Err(DocsError::KeyRefUnresolved { datum_key: key })
    );
    node.stop().await;
}

#[tokio::test]
async fn a_copy_that_does_not_hash_to_its_key_is_skipped_as_absent() {
    let node = Node::boot().await;
    // A node that does not know the kind declares its slot a hash stores a copy whose content is
    // not what its key says, signed and accepted like any other write.
    register_blob(&node, false);
    let alice = node.creator("alice").await;
    let drawer = node.collection(&alice, "blobs").await;
    let key = blob_key(HI);
    let tampered = node
        .create_doc_with(&alice, &drawer, "tampered", [(key.clone(), blob(b"bye\n"))])
        .await
        .unwrap();
    register_blob(&node, true);
    let genuine = node
        .create_doc_with(&alice, &drawer, "genuine", [(key.clone(), blob(b"hi\n"))])
        .await
        .unwrap();
    node.wait_holders(&key, &[&tampered, &genuine]).await;

    let resolved = node
        .resolve(&alice, &[&tampered, &genuine], &key)
        .await
        .unwrap();
    assert_eq!(resolved.doc_id, genuine);
    assert_eq!(resolved.value, blob(b"hi\n"));
    // The tampered copy is not another version of the datum: alone, it resolves to nothing.
    assert_eq!(
        node.resolve(&alice, &[&tampered], &key).await,
        Err(DocsError::KeyRefUnresolved { datum_key: key })
    );
    node.stop().await;
}

#[tokio::test]
async fn a_reader_who_can_read_no_holder_gets_the_same_answer_as_when_there_is_none() {
    let node = Node::boot().await;
    register_blob(&node, true);
    let alice = node.creator("alice").await;
    let bob = node.creator("bob").await;
    let key = blob_key(HI);
    let held = node
        .create_doc_with(
            &alice,
            &node.collection(&alice, "alice").await,
            "held",
            [(key.clone(), blob(b"hi\n"))],
        )
        .await
        .unwrap();
    let theirs = node
        .create_doc_with(
            &bob,
            &node.collection(&bob, "bob").await,
            "theirs",
            [(
                DatumKey::from_text(NOTE, "main").unwrap(),
                json!({"$type": NOTE, "value": "bob's"}),
            )],
        )
        .await
        .unwrap();
    node.wait_holders(&key, &[&held]).await;

    node.resolve(&alice, &[&held], &key).await.unwrap();
    // Bob names a document he can read and a key he cannot reach through it: answering
    // differently from "no such key" would tell him which documents hold it.
    let unreadable = Err(DocsError::KeyRefUnresolved {
        datum_key: key.clone(),
    });
    assert_eq!(node.resolve(&bob, &[&theirs], &key).await, unreadable);
    assert_eq!(
        node.resolve(&bob, &[&theirs], &blob_key(&"00".repeat(20)))
            .await,
        Err(DocsError::KeyRefUnresolved {
            datum_key: blob_key(&"00".repeat(20))
        })
    );
    node.stop().await;
}

#[tokio::test]
async fn only_a_kind_that_declares_a_content_hash_slot_can_be_key_referenced_or_written_by_hash() {
    let node = Node::boot().await;
    register_blob(&node, true);
    register_pointer(&node);
    let alice = node.creator("alice").await;
    let drawer = node.collection(&alice, "blobs").await;
    let key = blob_key(HI);
    let held = node
        .create_doc_with(&alice, &drawer, "held", [(key.clone(), blob(b"hi\n"))])
        .await
        .unwrap();

    // A reference to a kind whose key says nothing about its value is refused at write, as is
    // one to a kind this node does not know.
    for target in [
        DatumKey::from_text(NOTE, "main").unwrap(),
        DatumKey::from_text("org.example.unknown", "x").unwrap(),
    ] {
        assert_eq!(
            node.create_doc_with(&alice, &drawer, "bad-pointer", [pointer(&target)])
                .await,
            Err(DocsError::NotContentAddressed {
                datum_key: target.clone()
            })
        );
        // And resolving one has no content to hash a copy against.
        assert_eq!(
            node.resolve(&alice, &[&held], &target).await,
            Err(DocsError::NotContentAddressed { datum_key: target })
        );
    }
    node.create_doc_with(&alice, &drawer, "pointer", [pointer(&key)])
        .await
        .unwrap();

    // A write whose value does not hash to its declared key is refused; the matching one is not.
    let refused = node
        .create_doc_with(&alice, &drawer, "wrong", [(key.clone(), blob(b"bye\n"))])
        .await;
    assert!(
        matches!(&refused, Err(DocsError::ContentHashMismatch { datum_key, .. }) if *datum_key == key),
        "{refused:?}"
    );
    node.create_doc_with(&alice, &drawer, "right", [(key, blob(b"hi\n"))])
        .await
        .unwrap();
    node.stop().await;
}

#[tokio::test]
async fn the_reference_index_finds_referrers_by_key_and_only_among_readable_documents() {
    let node = Node::boot().await;
    register_blob(&node, true);
    register_pointer(&node);
    let alice = node.creator("alice").await;
    let bob = node.creator("bob").await;
    let drawer = node.collection(&alice, "pointers").await;
    let key = blob_key(HI);
    let other = blob_key(&"00".repeat(20));
    let mut pointers = Vec::new();
    for (name, target) in [("first", &key), ("second", &key), ("unrelated", &other)] {
        pointers.push(
            node.create_doc_with(&alice, &drawer, name, [pointer(target)])
                .await
                .unwrap(),
        );
    }
    let [first, second, unrelated] = <[DocId; 3]>::try_from(pointers).unwrap();
    let mut settled = node.datum_set.subscribe_settled();
    loop {
        let referrers = node
            .datum_ref
            .query(DatumRefQuery::Key(key.clone()))
            .await
            .unwrap();
        if referrers.len() == 2 {
            break;
        }
        settled.changed().await.unwrap();
    }

    let pointer_key = DatumKey::from_text(POINTER, "main").unwrap();
    let found = node
        .referrers_of(&alice, &[&first, &second, &unrelated], &key)
        .await;
    assert_eq!(
        found
            .iter()
            .map(|referrer| &referrer.origin_doc_id)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([&first, &second])
    );
    for referrer in &found {
        assert_eq!(referrer.origin_datum_key, pointer_key);
        assert_eq!(referrer.path, "/target");
        // A key reference names no document.
        assert_eq!(referrer.target_doc_id, None);
        assert_eq!(referrer.target_datum_key, key);
    }
    // A referrer the caller cannot read is not reported, whichever key it asks about.
    let mine = node
        .create_doc_with(
            &bob,
            &node.collection(&bob, "bob").await,
            "mine",
            [(
                DatumKey::from_text(NOTE, "main").unwrap(),
                json!({"$type": NOTE, "value": "x"}),
            )],
        )
        .await
        .unwrap();
    assert!(node.referrers_of(&bob, &[&mine], &key).await.is_empty());
    node.stop().await;
}
