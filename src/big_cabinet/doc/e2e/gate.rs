//! ADR 020 §7: the default read and the write base reject forged datum state.

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
    assert!(rejected(&read).contains("without signing"), "{read:?}");
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

/// The §6 scenario: an unsigned assignment that loses the visible conflict still stays in the
/// datum's state. A writer building on that state is stopped at its base check, before signing.
#[tokio::test]
async fn a_hidden_unsigned_contribution_rejects_the_datum_even_when_it_loses() {
    let node = Node::boot().await;
    let (alice, created) = seeded(&node).await;
    let document = &created.doc_id;
    node.sabotage(document, |doc| {
        unsigned_text(doc, &key("main"), &created.heads, "evil")
    })
    .await;
    // The signed write is made later, so its operation outranks the forgery's in the conflict.
    node.write(
        &alice,
        document,
        WriteMode::AtHeads {
            heads: created.heads.clone(),
        },
        [(key("main"), note("still safe"))],
        [],
    )
    .await
    .unwrap();
    node.sabotage(document, |doc| {
        let record = record(doc, &key("main"));
        let visible = doc.get(&record, "value").unwrap().unwrap().0.to_string();
        assert_eq!(
            visible, "\"still safe\"",
            "the forgery lost the visible conflict"
        );
        assert_eq!(doc.get_all(&record, "value").unwrap().len(), 2);
    })
    .await;
    let read = node
        .read_one(&alice, document, At::Current, &key("main"))
        .await;
    rejected(&read);
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

#[tokio::test]
async fn an_older_verified_base_stays_usable_beside_a_newer_forgery() {
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
    node.write(
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
    // The merged frontier still contains the forgery and is checked on its own.
    rejected(
        &node
            .read_one(&alice, document, At::Current, &key("main"))
            .await,
    );
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
    assert!(rejected(&read).contains("without signing"), "{read:?}");
    node.stop().await;
}

utils_rs::table_tests! {
    erased_containers tokio,
    (target, replace, tombstone),
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
        let before = node.read(&alice, document, At::Current, false).await.unwrap();
        let read = node.read_one(&alice, document, At::Current, &key("main")).await;
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
        let old = node.read_one(&alice, document, At::Heads(created.heads.clone()), &key("main")).await;
        assert_eq!(accepted(&old).0, &note("safe"));
        node.stop().await;
    },
}

erased_containers! {
    deleted_envelope: ("envelope", false, false),
    replaced_envelope: ("envelope", true, false),
    deleted_dmeta: ("dmeta", false, false),
    replaced_dmeta: ("dmeta", true, false),
    deleted_metadata_envelope: ("metadata_envelope", false, false),
    replaced_metadata_envelope: ("metadata_envelope", true, false),
    deleted_metadata: ("metadata", false, false),
    replaced_metadata: ("metadata", true, false),
    deleted_signatures: ("signatures", false, false),
    replaced_signatures: ("signatures", true, false),
    deleted_content: ("content", false, false),
    replaced_content: ("content", true, false),
    deleted_tombstone_envelope: ("envelope", false, true),
    replaced_tombstone_envelope: ("envelope", true, true),
    deleted_tombstone_dmeta: ("dmeta", false, true),
    replaced_tombstone_dmeta: ("dmeta", true, true),
    deleted_tombstone_metadata_envelope: ("metadata_envelope", false, true),
    replaced_tombstone_metadata_envelope: ("metadata_envelope", true, true),
    deleted_tombstone_metadata: ("metadata", false, true),
    replaced_tombstone_metadata: ("metadata", true, true),
    deleted_tombstone_signatures: ("signatures", false, true),
    replaced_tombstone_signatures: ("signatures", true, true),
    replaced_tombstone_content: ("content", true, true),
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
