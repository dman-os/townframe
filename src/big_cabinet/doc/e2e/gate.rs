//! ADR 020 §6: the default read and the write base reject forged datum state.
//!
//! The gate compares the datum at the requested heads with the datum at its signature heads; it
//! does not search history for unsigned operations that are invisible at the requested heads
//! (ADR 020 §6, ledger D27). These tests pin both sides of that line.

use super::*;
use automerge::ActorId;
use automerge::ObjId;
use automerge::ObjType;
use automerge::PatchLog;
use automerge::ROOT;
use automerge::ReadDoc;
use automerge::transaction::Transactable;
use big_cabinet_core::authority::SignDatumClaim;
use big_cabinet_core::dmeta::ClaimOperation;
use big_cabinet_core::dmeta::DatumClaim;
use big_cabinet_core::dmeta::NativeValue;
use big_cabinet_core::dmeta::cid_of;
use big_cabinet_core::dmeta::encode_native;

const DMETA: &str = "org.example.daybook.dmeta/main";

fn object(doc: &impl ReadDoc, parent: &ObjId, prop: &str) -> ObjId {
    doc.get(parent, prop).unwrap().unwrap().1
}

fn record(doc: &impl ReadDoc, datum: &DatumKey) -> ObjId {
    let datums = object(doc, &ROOT, "datums");
    object(doc, &datums, &datum.to_string())
}

fn signatures(doc: &impl ReadDoc, datum: &DatumKey) -> ObjId {
    let datums = object(doc, &ROOT, "datums");
    let dmeta = object(doc, &datums, DMETA);
    let metadata = object(doc, &dmeta, "datums");
    let metadata = object(doc, &metadata, &datum.to_string());
    object(doc, &metadata, "signatures")
}

fn rejected(read: &DatumRead) -> &str {
    match read {
        DatumRead::Rejected { reason } => reason,
        other => panic!("expected a rejected datum, got {other:?}"),
    }
}

fn base_refused(error: DocsError, datum: &DatumKey) -> Unaccepted {
    match error {
        DocsError::BaseNotAccepted { datum_key, verdict } if &datum_key == datum => verdict,
        other => panic!("expected the base of {datum} to be refused, got {other:?}"),
    }
}

/// Plant an unsigned assignment to the note's text at `heads` under a foreign Automerge actor.
fn unsigned_text(doc: &mut Automerge, datum: &DatumKey, heads: &[ChangeHash], text: &str) {
    doc.set_actor(ActorId::from([1_u8; 32]));
    let mut tx = doc.transaction_at(PatchLog::null(), heads).unwrap();
    let record = record(&tx, datum);
    tx.put(&record, "value", text).unwrap();
    tx.commit();
}

const NOT_SIGNED: &str = "not the one its signatures cover";

async fn seeded(node: &Node) -> (Actor, WriteReceipt) {
    let alice = node.writer("alice").await;
    let created = node
        .create(
            &alice,
            "create",
            [(key("main"), note("safe")), (key("other"), note("other"))],
        )
        .await
        .unwrap();
    (alice, created)
}

#[tokio::test]
async fn an_unsigned_edit_rejects_the_datum_for_reads_and_for_write_bases() {
    let node = Node::boot().await;
    let (alice, created) = seeded(&node).await;
    let document = &created.doc_id;
    let forged = node
        .sabotage(document, |doc| {
            unsigned_text(doc, &key("main"), &created.heads, "evil")
        })
        .await;
    let read = node
        .read_one(&alice, document, At::Current, &key("main"))
        .await;
    assert!(rejected(&read).contains(NOT_SIGNED), "{read:?}");
    let refused = node
        .write(
            &alice,
            document,
            WriteMode::AtCurrent,
            [(key("main"), note("mine"))],
            [],
        )
        .await
        .unwrap_err();
    assert!(matches!(
        base_refused(refused, &key("main")),
        Unaccepted::Rejected { .. }
    ));
    // Removal builds on the forged base just as much as a set does.
    let refused = node
        .write(&alice, document, WriteMode::AtCurrent, [], [key("main")])
        .await
        .unwrap_err();
    assert!(matches!(
        base_refused(refused, &key("main")),
        Unaccepted::Rejected { .. }
    ));
    let read = node
        .read(&alice, document, At::Current, false)
        .await
        .unwrap();
    assert_eq!(read.heads, forged, "a refused write commits nothing");
    node.stop().await;
}

/// An unsigned assignment that loses the visible conflict to a concurrent signed write is not
/// checked: it is invisible at the merged heads, and no ordinary read can reach a conflict loser,
/// so the merged datum is exactly what the winning signature covers.
#[tokio::test]
async fn a_verified_base_stays_usable_beside_a_concurrent_forgery_that_loses() {
    let node = Node::boot().await;
    let (alice, created) = seeded(&node).await;
    let document = &created.doc_id;
    node.sabotage(document, |doc| {
        unsigned_text(doc, &key("main"), &created.heads, "evil")
    })
    .await;
    let old = node
        .read_one(
            &alice,
            document,
            At::Heads(created.heads.clone()),
            &key("main"),
        )
        .await;
    assert_eq!(accepted(&old).0, &note("safe"));
    // Automerge resolves a conflict by the greater operation counter first, and a new change
    // starts above every operation the document already holds, even when it is made at
    // historical heads. The signed write comes after the forgery, so its assignment wins.
    let written = node
        .write(
            &alice,
            document,
            WriteMode::AtHeads {
                heads: created.heads.clone(),
            },
            [(key("main"), note("from the old base"))],
            [],
        )
        .await
        .unwrap();
    node.sabotage(document, |doc| {
        let record = record(doc, &key("main"));
        assert_eq!(
            doc.get(&record, "value").unwrap().unwrap().0.to_string(),
            "\"from the old base\"",
            "the forgery lost the visible conflict"
        );
        assert_eq!(doc.get_all(&record, "value").unwrap().len(), 2);
    })
    .await;
    let read = node
        .read_one(&alice, document, At::Current, &key("main"))
        .await;
    let (value, datum_heads) = accepted(&read);
    assert_eq!(value, &note("from the old base"));
    // The document heads are the signed write and the forgery; only the former signs the datum.
    assert_eq!(written.heads.len(), 2);
    assert!(
        matches!(datum_heads, [head] if written.heads.contains(head)),
        "{datum_heads:?}"
    );
    node.write(
        &alice,
        document,
        WriteMode::AtCurrent,
        [(key("main"), note("next"))],
        [],
    )
    .await
    .unwrap();
    node.stop().await;
}

/// The same race with the forgery winning: it is made after the signed write, so its assignment
/// carries the greater operation counter. The merged value at the requested heads then differs
/// from the value at the signature heads, which hold only the signed side.
#[tokio::test]
async fn a_concurrent_forgery_that_wins_the_conflict_rejects_the_datum() {
    let node = Node::boot().await;
    let (alice, created) = seeded(&node).await;
    let document = &created.doc_id;
    node.write(
        &alice,
        document,
        WriteMode::AtCurrent,
        [(key("main"), note("signed"))],
        [],
    )
    .await
    .unwrap();
    node.sabotage(document, |doc| {
        unsigned_text(doc, &key("main"), &created.heads, "evil")
    })
    .await;
    node.sabotage(document, |doc| {
        let record = record(doc, &key("main"));
        assert_eq!(
            doc.get(&record, "value").unwrap().unwrap().0.to_string(),
            "\"evil\"",
            "the forgery won the visible conflict"
        );
    })
    .await;
    let read = node
        .read_one(&alice, document, At::Current, &key("main"))
        .await;
    assert!(rejected(&read).contains(NOT_SIGNED), "{read:?}");
    let refused = node
        .write(
            &alice,
            document,
            WriteMode::AtCurrent,
            [(key("main"), note("laundered"))],
            [],
        )
        .await
        .unwrap_err();
    assert!(matches!(
        base_refused(refused, &key("main")),
        Unaccepted::Rejected { .. }
    ));
    node.stop().await;
}

/// A slot with no signatures has no signed value, so it is accepted only while it is absent.
#[tokio::test]
async fn a_new_key_present_without_a_signature_is_rejected() {
    let node = Node::boot().await;
    let (alice, created) = seeded(&node).await;
    let document = &created.doc_id;
    node.sabotage(document, |doc| {
        doc.set_actor(ActorId::from([1_u8; 32]));
        let mut tx = doc.transaction();
        let datums = object(&tx, &ROOT, "datums");
        let planted = tx
            .put_object(&datums, key("planted").to_string(), ObjType::Map)
            .unwrap();
        tx.put(&planted, "value", "planted").unwrap();
        tx.commit();
    })
    .await;
    let read = node
        .read(&alice, document, At::Current, false)
        .await
        .unwrap();
    assert!(
        rejected(&read.datums[&key("planted")]).contains(NOT_SIGNED),
        "{read:?}"
    );
    assert_eq!(accepted(&read.datums[&key("main")]).0, &note("safe"));
    let refused = node
        .write(
            &alice,
            document,
            WriteMode::AtCurrent,
            [(key("planted"), note("over"))],
            [],
        )
        .await
        .unwrap_err();
    assert!(matches!(
        base_refused(refused, &key("planted")),
        Unaccepted::Rejected { .. }
    ));
    node.stop().await;
}

#[tokio::test]
async fn a_forgery_in_another_datum_leaves_this_one_readable_and_writable() {
    let node = Node::boot().await;
    let (alice, created) = seeded(&node).await;
    let document = &created.doc_id;
    node.sabotage(document, |doc| {
        unsigned_text(doc, &key("other"), &created.heads, "evil")
    })
    .await;
    let read = node
        .read(&alice, document, At::Current, false)
        .await
        .unwrap();
    rejected(&read.datums[&key("other")]);
    assert_eq!(accepted(&read.datums[&key("main")]).0, &note("safe"));
    node.write(
        &alice,
        document,
        WriteMode::AtCurrent,
        [(key("main"), note("next"))],
        [],
    )
    .await
    .unwrap();
    node.stop().await;
}

#[tokio::test]
async fn a_bogus_signature_entry_rejects_only_its_own_datum() {
    let node = Node::boot().await;
    let (alice, created) = seeded(&node).await;
    let document = &created.doc_id;
    node.sabotage(document, |doc| {
        doc.set_actor(ActorId::from([1_u8; 32]));
        let mut tx = doc.transaction();
        let list = signatures(&tx, &key("main"));
        let honest = tx.get(&list, 0).unwrap().unwrap().1;
        let doc_uuid = tx
            .get(&honest, "docUuid")
            .unwrap()
            .unwrap()
            .0
            .to_str()
            .unwrap()
            .to_owned();
        let entry = tx.insert_object(&list, 0, ObjType::Map).unwrap();
        tx.put(&entry, "actorUuid", alice.actor_id.to_string())
            .unwrap();
        tx.put(&entry, "docUuid", doc_uuid).unwrap();
        tx.put(&entry, "signature", vec![9_u8; 64]).unwrap();
        tx.commit();
    })
    .await;
    let read = node
        .read_one(&alice, document, At::Current, &key("main"))
        .await;
    assert!(rejected(&read).contains("invalid signature"), "{read:?}");
    assert_eq!(
        accepted(
            &node
                .read_one(&alice, document, At::Current, &key("other"))
                .await
        )
        .0,
        &note("other")
    );
    node.stop().await;
}

#[tokio::test]
async fn deleting_the_signatures_under_a_value_rejects_it() {
    let node = Node::boot().await;
    let (alice, created) = seeded(&node).await;
    let document = &created.doc_id;
    node.sabotage(document, |doc| {
        doc.set_actor(ActorId::from([1_u8; 32]));
        let mut tx = doc.transaction();
        let list = signatures(&tx, &key("main"));
        tx.delete(&list, 0).unwrap();
        tx.commit();
    })
    .await;
    let read = node
        .read_one(&alice, document, At::Current, &key("main"))
        .await;
    assert!(rejected(&read).contains(NOT_SIGNED), "{read:?}");
    node.stop().await;
}

/// What erasing a container leaves the main datum as. `Rejected`: the content outlives its
/// signatures (or a replaced container leaves content its tombstone does not cover). `Absent`: the
/// content went with the signatures, so the slot reads as never written. Erasing a signed value
/// together with every signature on it is the forgery the gate deliberately does not see (D27): it
/// leaves nothing at the requested heads to compare.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Erased {
    Rejected,
    Absent,
}

utils_rs::table_tests! {
    erased_containers tokio,
    (target, replace, tombstone, expect),
    {
        let node = Node::boot().await;
        let (alice, created) = seeded(&node).await;
        let document = &created.doc_id;
        if tombstone {
            node.write(&alice, document, WriteMode::AtCurrent, [], [key("main")]).await.unwrap();
        }
        node.sabotage(document, |doc| {
            let mut tx = doc.transaction();
            let datums = object(&tx, &ROOT, "datums");
            let dmeta = object(&tx, &datums, DMETA);
            let metadata = object(&tx, &dmeta, "datums");
            let slot = object(&tx, &metadata, &key("main").to_string());
            let (parent, property) = match target {
                "envelope" => (ROOT, "datums".to_string()),
                "dmeta" => (datums.clone(), DMETA.to_string()),
                "metadata_envelope" => (dmeta, "datums".to_string()),
                "metadata" => (metadata, key("main").to_string()),
                "signatures" => (slot, "signatures".to_string()),
                "content" => (datums, key("main").to_string()),
                other => panic!("unknown target {other}"),
            };
            if replace {
                let kind = if target == "signatures" { ObjType::List } else { ObjType::Map };
                tx.put_object(&parent, &property, kind).unwrap();
            } else {
                tx.delete(&parent, &property).unwrap();
            }
            tx.commit();
        }).await;
        let old = node.read_one(&alice, document, At::Heads(created.heads.clone()), &key("main")).await;
        assert_eq!(accepted(&old).0, &note("safe"));
        let read = node.read_one(&alice, document, At::Current, &key("main")).await;
        if expect == Erased::Absent {
            assert!(matches!(&read, DatumRead::Absent { version } if version.heads.is_empty()), "{read:?}");
            let written = node.write(&alice, document, WriteMode::AtCurrent, [(key("main"), note("again"))], []).await;
            if replace && matches!(target, "dmeta" | "metadata") {
                // The gate passes the absent base, but staging hydrates the whole dmeta, and an
                // empty map in place of the dmeta or of the slot's metadata is not one.
                assert!(matches!(written, Err(DocsError::Repository(_))), "{written:?}");
            } else {
                written.unwrap();
                assert_eq!(
                    accepted(&node.read_one(&alice, document, At::Current, &key("main")).await).0,
                    &note("again")
                );
            }
            node.stop().await;
            return Ok(());
        }
        let before = node.read(&alice, document, At::Current, false).await.unwrap();
        rejected(&read);
        let explicit = op(node.client().call(
            RequestContext::root(),
            ReadDatums {
                capability: node.capability(&alice, DocScope {
                    read: BTreeSet::from([document.clone()]),
                    ..DocScope::default()
                }).await.unwrap(),
                doc_id: document.clone(),
                at: At::Current,
                datums: Some(BTreeSet::from([key("main")])),
                optimistic: true,
            },
        ).await).unwrap();
        assert_eq!(explicit.datums[&key("main")], read);
        for remove in [false, true] {
            let (set, removed) = if remove {
                (Vec::new(), vec![key("main")])
            } else {
                (vec![(key("main"), note("over"))], Vec::new())
            };
            let refused = node.write(&alice, document, WriteMode::AtCurrent, set, removed).await.unwrap_err();
            assert!(matches!(base_refused(refused, &key("main")), Unaccepted::Rejected { .. }));
        }
        assert_eq!(node.read(&alice, document, At::Current, false).await.unwrap(), before);
        assert_eq!(node.read_one(&alice, document, At::Current, &key("main")).await, read);
        assert_eq!(
            node.read_one(&alice, document, At::Current, &key("never-written")).await,
            DatumRead::Absent {
                version: DatumVersion {
                    heads: Vec::new(),
                    clock: 0
                }
            }
        );
        node.stop().await;
    },
}

erased_containers! {
    deleted_envelope: ("envelope", false, false, Erased::Absent),
    replaced_envelope: ("envelope", true, false, Erased::Absent),
    deleted_dmeta: ("dmeta", false, false, Erased::Rejected),
    replaced_dmeta: ("dmeta", true, false, Erased::Rejected),
    deleted_metadata_envelope: ("metadata_envelope", false, false, Erased::Rejected),
    replaced_metadata_envelope: ("metadata_envelope", true, false, Erased::Rejected),
    deleted_metadata: ("metadata", false, false, Erased::Rejected),
    replaced_metadata: ("metadata", true, false, Erased::Rejected),
    deleted_signatures: ("signatures", false, false, Erased::Rejected),
    replaced_signatures: ("signatures", true, false, Erased::Rejected),
    deleted_content: ("content", false, false, Erased::Rejected),
    replaced_content: ("content", true, false, Erased::Rejected),
    deleted_tombstone_envelope: ("envelope", false, true, Erased::Absent),
    replaced_tombstone_envelope: ("envelope", true, true, Erased::Absent),
    deleted_tombstone_dmeta: ("dmeta", false, true, Erased::Absent),
    replaced_tombstone_dmeta: ("dmeta", true, true, Erased::Absent),
    deleted_tombstone_metadata_envelope: ("metadata_envelope", false, true, Erased::Absent),
    replaced_tombstone_metadata_envelope: ("metadata_envelope", true, true, Erased::Absent),
    deleted_tombstone_metadata: ("metadata", false, true, Erased::Absent),
    replaced_tombstone_metadata: ("metadata", true, true, Erased::Absent),
    deleted_tombstone_signatures: ("signatures", false, true, Erased::Absent),
    replaced_tombstone_signatures: ("signatures", true, true, Erased::Absent),
    replaced_tombstone_content: ("content", true, true, Erased::Rejected),
}

#[derive(Debug, Clone, Copy)]
enum Damage {
    None,
    MissingCertificate,
    MissingUrl,
    MissingActor,
    MissingAlias,
    MissingActorUuid,
    UnboundDocUuid,
    MissingSignature,
    ShortSignature,
    BadMessage,
    BadCertificate,
    UnsupportedCertificate,
    WrongUrl,
    InheritedValue,
}

async fn signed_outside(
    node: &Node,
    document: &DocId,
    signer: &Actor,
    damage: Damage,
    node_actor: ActorId,
    text: &str,
) {
    let datum = key("main");
    let stage = |doc: &mut Automerge, signature: Option<Vec<u8>>| -> DatumClaim {
        let base = doc.get_heads();
        let predecessors =
            crate::doc::predecessor_ids(doc, &datum, &base, crate::doc::HistoryBudget::default())
                .unwrap();
        doc.set_actor(node_actor.clone());
        let mut tx = doc.transaction();
        let record = record(&tx, &datum);
        tx.put(&record, "value", text).unwrap();
        let datums = object(&tx, &ROOT, "datums");
        let dmeta = object(&tx, &datums, DMETA);
        let actors = object(&tx, &dmeta, "actors");
        let binding = tx
            .put_object(&actors, signer.actor_id.to_string(), ObjType::Map)
            .unwrap();
        tx.put(&binding, "pubkey", signer.public_key.to_string())
            .unwrap();
        tx.put(&binding, "actorUrl", signer.url.as_str()).unwrap();
        tx.put(
            &binding,
            "delegation",
            signer.certificate.as_slice().to_vec(),
        )
        .unwrap();
        let aliases = object(&tx, &dmeta, "datumUuids");
        let target = datum.to_string();
        let chosen = tx
            .keys(&aliases)
            .filter(|alias| {
                tx.get(&aliases, alias.as_str())
                    .unwrap()
                    .unwrap()
                    .0
                    .to_str()
                    == Some(target.as_str())
            })
            .map(|alias| alias.parse::<uuid::Uuid>().unwrap())
            .min()
            .unwrap();
        let native = NativeValue::read_at(&tx, &datums, datum.to_string())
            .unwrap()
            .unwrap();
        let list = signatures(&tx, &datum);
        tx.delete(&list, 0).unwrap();
        let entry = tx.insert_object(&list, 0, ObjType::Map).unwrap();
        tx.put(&entry, "actorUuid", signer.actor_id.to_string())
            .unwrap();
        let documents = object(&tx, &dmeta, "docUuids");
        let doc_uuid = tx
            .keys(&documents)
            .find(|uuid| {
                tx.get(&documents, uuid.as_str())
                    .unwrap()
                    .unwrap()
                    .0
                    .to_str()
                    == Some(document.to_string().as_str())
            })
            .unwrap();
        tx.put(&entry, "docUuid", doc_uuid).unwrap();
        tx.put(
            &entry,
            "signature",
            signature.clone().unwrap_or(vec![0; 64]),
        )
        .unwrap();
        let claim = DatumClaim {
            origin_document: document.clone(),
            datum_key: datum.clone(),
            chosen_uuid: chosen.try_into().unwrap(),
            actor_uuid: signer.actor_id,
            operation: ClaimOperation::Set,
            value_cid: Some(cid_of(&encode_native(&native))),
            predecessors,
        };
        match damage {
            Damage::None => {}
            Damage::MissingCertificate => tx.delete(&binding, "delegation").unwrap(),
            Damage::MissingUrl => tx.delete(&binding, "actorUrl").unwrap(),
            Damage::MissingActor => tx.delete(&actors, signer.actor_id.to_string()).unwrap(),
            Damage::MissingAlias => tx.delete(&aliases, chosen.to_string()).unwrap(),
            Damage::UnboundDocUuid => tx
                .put(
                    &entry,
                    "docUuid",
                    big_cabinet_core::dmeta::Uuid7::new().to_string(),
                )
                .unwrap(),
            Damage::MissingActorUuid => tx.delete(&entry, "actorUuid").unwrap(),
            Damage::MissingSignature => tx.delete(&entry, "signature").unwrap(),
            Damage::ShortSignature => tx.put(&entry, "signature", vec![0_u8; 63]).unwrap(),
            Damage::BadMessage => tx.put(&entry, "message", vec![0_u8]).unwrap(),
            Damage::BadCertificate => tx.put(&binding, "delegation", vec![0xff_u8]).unwrap(),
            Damage::UnsupportedCertificate => {
                tx.put(&binding, "delegation", vec![0xf6_u8]).unwrap()
            }
            Damage::WrongUrl => tx
                .put(&binding, "actorUrl", "db+actor://wrong/bob")
                .unwrap(),
            Damage::InheritedValue => {
                let inherited = tx.put_object(&entry, "inherited", ObjType::Map).unwrap();
                tx.put(&inherited, "originDoc", document.to_string())
                    .unwrap();
                tx.put(&inherited, "message", claim.signing_bytes().unwrap())
                    .unwrap();
                tx.put(&record, "value", "never signed").unwrap();
            }
        }
        if signature.is_some() {
            tx.commit();
        } else {
            tx.rollback();
        }
        claim
    };
    let mut claim = None;
    node.sabotage(document, |doc| claim = Some(stage(doc, None)))
        .await;
    let signed = node
        .authority
        .client
        .call(
            RequestContext::root(),
            SignDatumClaim {
                actor: signer.capability,
                claim: claim.unwrap(),
            },
        )
        .await
        .unwrap();
    let signature = signed.signature.as_bytes().to_vec();
    node.sabotage(document, |doc| {
        assert_eq!(stage(doc, Some(signature)), signed.claim);
    })
    .await;
}

fn node_actor(node: &Node, document: &DocId) -> ActorId {
    crate::doc::gate::doc_actor(&node.authority.node_issuer, document)
}

#[tokio::test]
async fn a_correctly_signed_write_made_outside_the_repository_is_accepted() {
    let node = Node::boot().await;
    let (alice, created) = seeded(&node).await;
    let bob = node.writer("bob").await;
    signed_outside(
        &node,
        &created.doc_id,
        &bob,
        Damage::None,
        node_actor(&node, &created.doc_id),
        "bob",
    )
    .await;
    let read = node
        .read_one(&alice, &created.doc_id, At::Current, &key("main"))
        .await;
    assert_eq!(accepted(&read).0, &note("bob"));
    node.stop().await;
}

/// An unsigned edit a signer built on is beneath that signature: the signer's change descends from
/// it, so the value at the signature heads already contains whatever the edit left visible.
#[tokio::test]
async fn an_unsigned_edit_a_signer_saw_is_accepted() {
    let node = Node::boot().await;
    let (alice, created) = seeded(&node).await;
    let document = &created.doc_id;
    node.sabotage(document, |doc| {
        unsigned_text(doc, &key("main"), &created.heads, "unsigned")
    })
    .await;
    rejected(
        &node
            .read_one(&alice, document, At::Current, &key("main"))
            .await,
    );
    let bob = node.writer("bob").await;
    signed_outside(
        &node,
        document,
        &bob,
        Damage::None,
        node_actor(&node, document),
        "unsigned",
    )
    .await;
    let read = node
        .read_one(&alice, document, At::Current, &key("main"))
        .await;
    assert_eq!(accepted(&read).0, &note("unsigned"));
    node.stop().await;
}

utils_rs::table_tests! {
    malformed_evidence tokio,
    (damage, reason),
    {
        let node = Node::boot().await;
        let (alice, created) = seeded(&node).await;
        let document = &created.doc_id;
        let bob = node.writer("bob").await;
        signed_outside(&node, document, &bob, damage, node_actor(&node, document), "unattributed").await;
        let before = node.read(&alice, document, At::Current, false).await.unwrap();
        assert!(rejected(&before.datums[&key("main")]).contains(reason), "{damage:?}: {before:?}");
        let optimistic = node.read(&alice, document, At::Current, true).await.unwrap();
        assert_eq!(optimistic, before, "optimistic reads never expose rejected values");
        assert_eq!(accepted(&before.datums[&key("other")]).0, &note("other"));
        for remove in [false, true] {
            let (set, removed) = if remove {
                (Vec::new(), vec![key("main")])
            } else {
                (vec![(key("main"), note("over"))], Vec::new())
            };
            let refused = node.write(&alice, document, WriteMode::AtCurrent, set, removed).await.unwrap_err();
            assert!(matches!(base_refused(refused, &key("main")), Unaccepted::Rejected { .. }));
        }
        assert_eq!(node.read(&alice, document, At::Current, false).await.unwrap(), before);
        assert_eq!(
            accepted(&node.read_one(&alice, document, At::Heads(created.heads.clone()), &key("main")).await).0,
            &note("safe")
        );
        node.stop().await;
    },
}

malformed_evidence! {
    absent_certificate: (Damage::MissingCertificate, "attribution certificate"),
    absent_actor_url: (Damage::MissingUrl, "attribution certificate or URL"),
    absent_actor_binding: (Damage::MissingActor, "unbound actor"),
    absent_datum_alias: (Damage::MissingAlias, "datum UUID alias"),
    absent_actor_uuid: (Damage::MissingActorUuid, "metadata text"),
    unbound_doc_uuid: (Damage::UnboundDocUuid, "unbound document UUID"),
    absent_signature: (Damage::MissingSignature, "signature bytes"),
    short_signature: (Damage::ShortSignature, "signature length"),
    disagreeing_message: (Damage::BadMessage, "messages disagree"),
    malformed_certificate: (Damage::BadCertificate, "attribution"),
    rebound_actor_url: (Damage::WrongUrl, "attribution"),
    inherited_claim_with_unsigned_value: (Damage::InheritedValue, "inherited claim does not cover"),
}

#[tokio::test]
async fn unverifiable_encoding_has_no_value_unless_optimistic_and_cannot_be_written_over() {
    let node = Node::boot().await;
    let (alice, created) = seeded(&node).await;
    let document = &created.doc_id;
    let bob = node.writer("bob").await;
    signed_outside(
        &node,
        document,
        &bob,
        Damage::UnsupportedCertificate,
        node_actor(&node, document),
        "unverified",
    )
    .await;
    let read = node
        .read(&alice, document, At::Current, false)
        .await
        .unwrap();
    let datum_heads = match &read.datums[&key("main")] {
        DatumRead::Pending {
            value: None,
            version,
            reason,
        } if reason.contains("unsupported") => version.heads.clone(),
        other => panic!("expected incomplete verification, got {other:?}"),
    };
    assert_eq!(datum_heads.len(), 1);
    let optimistic = node
        .read(&alice, document, At::Current, true)
        .await
        .unwrap();
    assert!(
        matches!(&optimistic.datums[&key("main")], DatumRead::Pending { value: Some(value), version: actual, .. } if value == &note("unverified") && actual.heads == datum_heads)
    );
    let refused = node
        .write(
            &alice,
            document,
            WriteMode::AtCurrent,
            [(key("main"), note("over"))],
            [],
        )
        .await
        .unwrap_err();
    assert!(matches!(
        base_refused(refused, &key("main")),
        Unaccepted::Pending { .. }
    ));
    assert_eq!(
        node.read(&alice, document, At::Current, false)
            .await
            .unwrap(),
        read
    );
    node.stop().await;
}

#[tokio::test]
async fn an_attribution_from_a_node_other_than_the_author_is_rejected() {
    let node = Node::boot().await;
    let (alice, created) = seeded(&node).await;
    let bob = node.writer("bob").await;
    let impostor = crate::doc::gate::doc_actor(
        &big_cabinet_core::dmeta::MultibaseKey::try_from(
            ed25519_dalek::SigningKey::from_bytes(&[5; 32])
                .verifying_key()
                .as_bytes()
                .as_slice(),
        )
        .unwrap(),
        &created.doc_id,
    );
    signed_outside(
        &node,
        &created.doc_id,
        &bob,
        Damage::None,
        impostor,
        "impostor",
    )
    .await;
    let read = node
        .read_one(&alice, &created.doc_id, At::Current, &key("main"))
        .await;
    assert!(rejected(&read).contains("attribution"), "{read:?}");
    node.stop().await;
}
