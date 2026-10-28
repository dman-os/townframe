use super::*;
use automerge::Prop;
use automerge::transaction::Transactable;
use automerge::transaction::Transaction;
use big_cabinet_core::dmeta::CanonicalF64;
use ed25519_dalek::Signer;
use ed25519_dalek::SigningKey;
use std::collections::BTreeMap;

const DATUM: &str = "org.example.note/main";
const DMETA: &str = "org.example.daybook.dmeta/main";

struct Fixture {
    doc: Automerge,
    document: DocId,
    doc_uuid: Uuid7,
    datum: DatumKey,
    alias: Uuid7,
    actor: Uuid7,
    key: SigningKey,
}

struct Paths {
    datums: ObjId,
    dmeta: ObjId,
    metadata: ObjId,
    signatures: ObjId,
    aliases: ObjId,
}

impl Fixture {
    fn new() -> Self {
        let mut fixture = Self {
            doc: Automerge::new(),
            document: DocId::new([7; 32]),
            doc_uuid: Reconstruction::uuid("01940000-0000-7000-8000-000000000003").unwrap(),
            datum: DatumKey::from_text("org.example.note", "main").unwrap(),
            alias: Reconstruction::uuid("01940000-0000-7000-8000-000000000001").unwrap(),
            actor: Reconstruction::uuid("01940000-0000-7000-8000-000000000002").unwrap(),
            key: SigningKey::from_bytes(&[9; 32]),
        };
        let mut tx = fixture.doc.transaction();
        let datums = tx
            .put_object(automerge::ROOT, "datums", ObjType::Map)
            .unwrap();
        let dmeta = tx.put_object(&datums, DMETA, ObjType::Map).unwrap();
        tx.put(&dmeta, "createdAt", ScalarValue::Timestamp(0))
            .unwrap();
        tx.put_object(&dmeta, "updatedAt", ObjType::List).unwrap();
        let aliases = tx.put_object(&dmeta, "datumUuids", ObjType::Map).unwrap();
        tx.put(&aliases, fixture.alias.to_string(), DATUM).unwrap();
        let documents = tx.put_object(&dmeta, "docUuids", ObjType::Map).unwrap();
        tx.put(
            &documents,
            fixture.doc_uuid.to_string(),
            fixture.document.to_string(),
        )
        .unwrap();
        let actors = tx.put_object(&dmeta, "actors", ObjType::Map).unwrap();
        let actor = tx
            .put_object(&actors, fixture.actor.to_string(), ObjType::Map)
            .unwrap();
        let public_key =
            MultibaseKey::try_from(fixture.key.verifying_key().as_bytes().as_slice()).unwrap();
        tx.put(&actor, "pubkey", public_key.to_string()).unwrap();
        tx.put(&actor, "actorUrl", "db+actor://node/test").unwrap();
        let metadata = tx.put_object(&dmeta, "datums", ObjType::Map).unwrap();
        let metadata = tx.put_object(&metadata, DATUM, ObjType::Map).unwrap();
        tx.put_object(&metadata, "updatedAt", ObjType::List)
            .unwrap();
        tx.put_object(&metadata, "deletedAt", ObjType::List)
            .unwrap();
        tx.put_object(&metadata, "signatures", ObjType::List)
            .unwrap();
        tx.put(&datums, DATUM, "seed").unwrap();
        tx.commit();
        fixture
    }

    fn paths(&self) -> Paths {
        let (_, datums) = self.doc.get(automerge::ROOT, "datums").unwrap().unwrap();
        let (_, dmeta) = self.doc.get(&datums, DMETA).unwrap().unwrap();
        let (_, aliases) = self.doc.get(&dmeta, "datumUuids").unwrap().unwrap();
        let (_, metadata) = self.doc.get(&dmeta, "datums").unwrap().unwrap();
        let (_, metadata) = self.doc.get(&metadata, DATUM).unwrap().unwrap();
        let (_, signatures) = self.doc.get(&metadata, "signatures").unwrap().unwrap();
        Paths {
            datums,
            dmeta,
            metadata,
            signatures,
            aliases,
        }
    }

    fn signed(&self, value: Option<&NativeValue>) -> SignedDatumClaim {
        let mut state = Reconstruction::new(&self.doc, &self.datum, HistoryBudget::default());
        let claim = DatumClaim {
            origin_document: self.document.clone(),
            datum_key: self.datum.clone(),
            chosen_uuid: self.alias,
            actor_uuid: self.actor,
            operation: if value.is_some() {
                ClaimOperation::Set
            } else {
                ClaimOperation::Remove
            },
            value_cid: value.map(|value| cid_of(&encode_native(value))),
            predecessors: state.predecessor_ids(&self.doc.get_heads()).unwrap(),
        };
        SignedDatumClaim {
            signature: SignatureBytes::try_from(
                self.key
                    .sign(&claim.signing_bytes().unwrap())
                    .to_bytes()
                    .as_slice(),
            )
            .unwrap(),
            actor_public_key: MultibaseKey::try_from(
                self.key.verifying_key().as_bytes().as_slice(),
            )
            .unwrap(),
            claim,
        }
    }

    fn install(
        &mut self,
        value: Option<&NativeValue>,
        signed: &SignedDatumClaim,
    ) -> (ObjId, ChangeHash) {
        self.install_entry(
            value,
            &DatumSignature {
                extra: Default::default(),
                actor_uuid: signed.claim.actor_uuid,
                doc_uuid: self.doc_uuid,
                signature: signed.signature.clone(),
                message: None,
                inherited: None,
            },
        )
    }

    fn install_entry(
        &mut self,
        value: Option<&NativeValue>,
        entry: &DatumSignature,
    ) -> (ObjId, ChangeHash) {
        let paths = self.paths();
        let mut tx = self.doc.transaction();
        match value {
            Some(value) => put_native(&mut tx, &paths.datums, DATUM.into(), value),
            None => tx.delete(&paths.datums, DATUM).unwrap(),
        }
        for index in (0..tx.length(&paths.signatures)).rev() {
            tx.delete(&paths.signatures, index).unwrap();
        }
        let marker = tx
            .insert_object(&paths.signatures, 0, ObjType::Map)
            .unwrap();
        tx.put(&marker, "actorUuid", entry.actor_uuid.to_string())
            .unwrap();
        tx.put(&marker, "docUuid", entry.doc_uuid.to_string())
            .unwrap();
        tx.put(&marker, "signature", entry.signature.as_bytes().to_vec())
            .unwrap();
        if let Some(message) = &entry.message {
            tx.put(&marker, "message", message.as_slice().to_vec())
                .unwrap();
        }
        if let Some(witness) = &entry.inherited {
            let inherited = tx.put_object(&marker, "inherited", ObjType::Map).unwrap();
            tx.put(&inherited, "originDoc", witness.origin_doc.to_string())
                .unwrap();
            tx.put(&inherited, "message", witness.message.as_slice().to_vec())
                .unwrap();
            if let Some(value) = &witness.value {
                tx.put(&inherited, "value", value.as_slice().to_vec())
                    .unwrap();
            }
        }
        let hash = tx.commit().0.unwrap();
        (marker, hash)
    }

    fn write(&mut self, value: Option<&NativeValue>) -> (ObjId, ChangeHash, SignedDatumClaim) {
        let signed = self.signed(value);
        let (marker, hash) = self.install(value, &signed);
        (marker, hash, signed)
    }

    fn reconstruct(&self, marker: &ObjId) -> ClaimHistoryResult {
        reconstruct_claim_for_marker(&self.doc, &self.datum, marker, HistoryBudget::default())
    }
}

fn put_native(tx: &mut Transaction<'_>, object: &ObjId, prop: Prop, value: &NativeValue) {
    let scalar = match value {
        NativeValue::Null => ScalarValue::Null,
        NativeValue::Bool(value) => ScalarValue::Boolean(*value),
        NativeValue::Int(value) => ScalarValue::Int(*value),
        NativeValue::Uint(value) => ScalarValue::Uint(*value),
        NativeValue::Float(value) => ScalarValue::F64(value.get()),
        NativeValue::Str(value) => ScalarValue::Str(value.as_str().into()),
        NativeValue::Bytes(value) => ScalarValue::Bytes(value.clone()),
        NativeValue::Timestamp(value) => ScalarValue::Timestamp(*value),
        NativeValue::Counter(value) => ScalarValue::Counter((*value).into()),
        NativeValue::Text(value) => {
            let text = tx.put_object(object, prop, ObjType::Text).unwrap();
            tx.splice_text(&text, 0, 0, value).unwrap();
            return;
        }
        NativeValue::Map(values) => {
            let map = tx.put_object(object, prop, ObjType::Map).unwrap();
            for (key, value) in values {
                put_native(tx, &map, key.as_str().into(), value);
            }
            return;
        }
        NativeValue::List(values) => {
            let list = tx.put_object(object, prop, ObjType::List).unwrap();
            for (index, value) in values.iter().enumerate() {
                tx.insert(&list, index, ScalarValue::Null).unwrap();
                put_native(tx, &list, index.into(), value);
            }
            return;
        }
    };
    tx.put(object, prop, scalar).unwrap();
}

fn accepted(result: ClaimHistoryResult) -> ReconstructedClaim {
    match result {
        ClaimHistoryResult::Accepted(claim) => *claim,
        other => panic!("{other:?}"),
    }
}

#[test]
fn introduction_uses_historical_value_alias_and_actor_binding() {
    let mut fixture = Fixture::new();
    let parents = fixture.doc.get_heads();
    let (marker, hash, signed) = fixture.write(Some(&NativeValue::Str("first".into())));
    let paths = fixture.paths();
    let mut tx = fixture.doc.transaction();
    tx.put(&paths.datums, DATUM, "later").unwrap();
    tx.put(
        &paths.aliases,
        "01930000-0000-7000-8000-000000000001",
        DATUM,
    )
    .unwrap();
    let (_, actors) = tx.get(&paths.dmeta, "actors").unwrap().unwrap();
    let (_, actor) = tx.get(&actors, fixture.actor.to_string()).unwrap().unwrap();
    tx.put(&actor, "pubkey", "invalid-later-binding").unwrap();
    tx.commit();
    let reconstructed = accepted(fixture.reconstruct(&marker));
    assert_eq!(reconstructed.claim, signed.claim);
    assert_eq!(reconstructed.actor_public_key, signed.actor_public_key);
    assert_eq!(reconstructed.introduction.parents, parents);
    assert_eq!(reconstructed.introduction.resulting_heads, vec![hash]);
    assert_eq!(
        accepted(reconstruct_claim(
            &fixture.doc,
            &fixture.datum,
            hash,
            HistoryBudget::default()
        )),
        reconstructed
    );
}

#[test]
fn native_kinds_are_signed_without_schema_and_survive_save_load() {
    let mut fixture = Fixture::new();
    let value = NativeValue::Map(BTreeMap::from([
        ("bytes".into(), NativeValue::Bytes(vec![0, 255])),
        ("text".into(), NativeValue::Text("é𐐀".into())),
        ("counter".into(), NativeValue::Counter(3)),
        ("timestamp".into(), NativeValue::Timestamp(-7)),
        ("uint".into(), NativeValue::Uint(u64::MAX)),
        (
            "float".into(),
            NativeValue::List(vec![
                NativeValue::Float(CanonicalF64::new(f64::NAN)),
                NativeValue::Float(CanonicalF64::new(-0.0)),
            ]),
        ),
    ]));
    let (marker, _, signed) = fixture.write(Some(&value));
    fixture.doc = Automerge::load(&fixture.doc.save()).unwrap();
    assert_eq!(accepted(fixture.reconstruct(&marker)).claim, signed.claim);
}

#[test]
fn slot_presence_not_unsigned_deleted_at_decides_operation() {
    let mut fixture = Fixture::new();
    let metadata = fixture.paths().metadata;
    let mut tx = fixture.doc.transaction();
    let (_, deleted) = tx.get(&metadata, "deletedAt").unwrap().unwrap();
    tx.insert(&deleted, 0, ScalarValue::Timestamp(5)).unwrap();
    tx.commit();
    let (null_marker, _, null_signed) = fixture.write(Some(&NativeValue::Null));
    assert_eq!(
        accepted(fixture.reconstruct(&null_marker)).claim,
        null_signed.claim
    );
    let (remove_marker, _, removed) = fixture.write(None);
    assert_eq!(
        accepted(fixture.reconstruct(&remove_marker)).claim,
        removed.claim
    );
    assert_eq!(removed.claim.operation, ClaimOperation::Remove);
    assert_eq!(removed.claim.value_cid, None);
}

#[test]
fn invalid_predecessor_is_signed_context_not_a_recursive_trust_gate() {
    let mut fixture = Fixture::new();
    fixture.write(Some(&NativeValue::Str("first".into())));
    let value = NativeValue::Str("invalid".into());
    let mut invalid = fixture.signed(Some(&value));
    invalid.signature = SignatureBytes::try_from([0_u8; 64].as_slice()).unwrap();
    let (invalid_marker, _) = fixture.install(Some(&value), &invalid);
    assert_eq!(
        fixture.reconstruct(&invalid_marker),
        ClaimHistoryResult::ProvenForgery(ClaimHistoryError::InvalidSignature)
    );
    let invalid_id = invalid.signature_id().unwrap();
    let (marker, _, signed) = fixture.write(Some(&NativeValue::Str("replacement".into())));
    assert_eq!(signed.claim.predecessors, vec![invalid_id]);
    assert_eq!(accepted(fixture.reconstruct(&marker)).claim, signed.claim);
}

#[test]
fn competing_bad_and_incomplete_entries_do_not_erase_accepted_evidence() {
    let mut fixture = Fixture::new();
    let (good, _, signed) = fixture.write(Some(&NativeValue::Str("good".into())));
    let paths = fixture.paths();
    let mut tx = fixture.doc.transaction();
    let bad = tx
        .insert_object(&paths.signatures, 1, ObjType::Map)
        .unwrap();
    tx.put(&bad, "actorUuid", fixture.actor.to_string())
        .unwrap();
    tx.put(&bad, "docUuid", fixture.doc_uuid.to_string())
        .unwrap();
    tx.put(&bad, "signature", vec![0_u8; 64]).unwrap();
    let incomplete = tx
        .insert_object(&paths.signatures, 2, ObjType::Map)
        .unwrap();
    tx.put(&incomplete, "signature", vec![0_u8; 64]).unwrap();
    tx.commit();
    let claims = reconstruct_current_claims(
        &fixture.doc,
        &fixture.datum,
        &fixture.doc.get_heads(),
        HistoryBudget::default(),
    )
    .unwrap();
    assert_eq!(claims.len(), 3);
    assert_eq!(
        accepted(
            claims
                .iter()
                .find(|entry| entry.marker == good)
                .unwrap()
                .result
                .clone()
        )
        .claim,
        signed.claim
    );
    assert_eq!(
        claims
            .iter()
            .find(|entry| entry.marker == bad)
            .unwrap()
            .result,
        ClaimHistoryResult::ProvenForgery(ClaimHistoryError::InvalidSignature)
    );
    assert_eq!(
        claims
            .iter()
            .find(|entry| entry.marker == incomplete)
            .unwrap()
            .result,
        ClaimHistoryResult::ProvenForgery(ClaimHistoryError::MalformedWitness("metadata text"))
    );
}

#[test]
fn mutated_signature_is_not_backdated_by_frontier_cache() {
    let mut fixture = Fixture::new();
    let (marker, _, signed) = fixture.write(Some(&NativeValue::Str("first".into())));
    let paths = fixture.paths();
    let mut tx = fixture.doc.transaction();
    tx.put(&paths.datums, DATUM, "later").unwrap();
    tx.put(&marker, "signature", vec![0_u8; 64]).unwrap();
    tx.commit();
    assert_eq!(accepted(fixture.reconstruct(&marker)).claim, signed.claim);
    let entries = reconstruct_current_claims(
        &fixture.doc,
        &fixture.datum,
        &fixture.doc.get_heads(),
        HistoryBudget::default(),
    )
    .unwrap();
    assert_eq!(
        entries,
        vec![ClaimHistoryEntry {
            marker,
            result: ClaimHistoryResult::ProvenForgery(ClaimHistoryError::MalformedWitness(
                "signature entry changed after introduction"
            ))
        }]
    );
}

#[test]
fn unrelated_marker_is_not_a_signature_for_this_datum() {
    let mut fixture = Fixture::new();
    let mut tx = fixture.doc.transaction();
    let list = tx
        .put_object(automerge::ROOT, "unrelated", ObjType::List)
        .unwrap();
    let marker = tx.insert_object(&list, 0, ObjType::Map).unwrap();
    tx.commit();
    assert_eq!(
        fixture.reconstruct(&marker),
        ClaimHistoryResult::ProvenForgery(ClaimHistoryError::MalformedWitness(
            "marker outside target signature set"
        ))
    );
}

#[test]
fn distinct_markers_in_one_change_have_independent_verdicts() {
    let mut fixture = Fixture::new();
    let value = NativeValue::Str("same".into());
    let signed = fixture.signed(Some(&value));
    let paths = fixture.paths();
    let mut tx = fixture.doc.transaction();
    tx.put(&paths.datums, DATUM, "same").unwrap();
    for index in 0..2 {
        let marker = tx
            .insert_object(&paths.signatures, index, ObjType::Map)
            .unwrap();
        tx.put(&marker, "actorUuid", fixture.actor.to_string())
            .unwrap();
        tx.put(&marker, "docUuid", fixture.doc_uuid.to_string())
            .unwrap();
        tx.put(&marker, "signature", signed.signature.as_bytes().to_vec())
            .unwrap();
    }
    let hash = tx.commit().0.unwrap();
    let claims = reconstruct_current_claims(
        &fixture.doc,
        &fixture.datum,
        &[hash],
        HistoryBudget::default(),
    )
    .unwrap();
    assert_eq!(claims.len(), 2);
    assert_ne!(claims[0].marker, claims[1].marker);
    for entry in claims {
        assert_eq!(accepted(entry.result).claim, signed.claim);
    }
    assert_eq!(
        reconstruct_claim(&fixture.doc, &fixture.datum, hash, HistoryBudget::default()),
        ClaimHistoryResult::ProvenForgery(ClaimHistoryError::MalformedWitness(
            "one signature marker at introduction"
        ))
    );
}

#[test]
fn concurrent_writers_reconstruct_their_own_transaction_views() {
    let mut left = Fixture::new();
    let mut right = Fixture {
        doc: left.doc.fork(),
        document: left.document.clone(),
        doc_uuid: left.doc_uuid,
        datum: left.datum.clone(),
        alias: left.alias,
        actor: left.actor,
        key: left.key.clone(),
    };
    let (_, _, left_signed) = left.write(Some(&NativeValue::Str("left".into())));
    let (_, _, right_signed) = right.write(Some(&NativeValue::Str("right".into())));
    left.doc.merge(&mut right.doc).unwrap();
    let claims = reconstruct_current_claims(
        &left.doc,
        &left.datum,
        &left.doc.get_heads(),
        HistoryBudget::default(),
    )
    .unwrap();
    let mut actual: Vec<_> = claims
        .into_iter()
        .map(|entry| accepted(entry.result).claim)
        .collect();
    let mut expected = vec![left_signed.claim, right_signed.claim];
    actual.sort_by(|left, right| left.value_cid.cmp(&right.value_cid));
    expected.sort_by(|left, right| left.value_cid.cmp(&right.value_cid));
    assert_eq!(actual, expected);
}

#[test]
fn carried_message_must_agree_with_reconstructible_context() {
    let mut fixture = Fixture::new();
    let value = NativeValue::Str("value".into());
    let signed = fixture.signed(Some(&value));
    let mut entry = DatumSignature {
        extra: Default::default(),
        actor_uuid: fixture.actor,
        doc_uuid: fixture.doc_uuid,
        signature: signed.signature.clone(),
        message: Some(Bytes::from(signed.claim.signing_bytes().unwrap())),
        inherited: None,
    };
    let (marker, _) = fixture.install_entry(Some(&value), &entry);
    assert_eq!(accepted(fixture.reconstruct(&marker)).claim, signed.claim);
    let signed = fixture.signed(Some(&value));
    entry.signature = signed.signature;
    entry.message = Some(Bytes::from(vec![1, 2, 3]));
    let (marker, _) = fixture.install_entry(Some(&value), &entry);
    assert_eq!(
        fixture.reconstruct(&marker),
        ClaimHistoryResult::ProvenForgery(ClaimHistoryError::MalformedWitness(
            "carried and reconstructed messages disagree"
        ))
    );
}

utils_rs::table_tests! {
    doc_uuid_binding_is_read_at_the_introduction,
    (binding, expected),
    {
        let expected: Option<&str> = expected;
        let mut fixture = Fixture::new();
        let value = NativeValue::Str("value".into());
        let unbound = Reconstruction::uuid("01940000-0000-7000-8000-000000000004").unwrap();
        let other = DocId::new([8; 32]);
        let (_, documents) = fixture
            .doc
            .get(&fixture.paths().dmeta, "docUuids")
            .unwrap()
            .unwrap();
        let doc_uuid = match binding {
            "bound" | "rebound later" => fixture.doc_uuid,
            "unbound" | "bound later" => unbound,
            "conflicting" => {
                let mut peer = fixture.doc.fork();
                let third = DocId::new([9; 32]);
                for (doc, document) in [(&mut peer, &other), (&mut fixture.doc, &third)] {
                    let mut tx = doc.transaction();
                    tx.put(&documents, fixture.doc_uuid.to_string(), document.to_string()).unwrap();
                    tx.commit();
                }
                fixture.doc.merge(&mut peer).unwrap();
                fixture.doc_uuid
            }
            other => panic!("unknown case: {other}"),
        };
        let signed = fixture.signed(Some(&value));
        let entry = DatumSignature {
            extra: Default::default(),
            actor_uuid: fixture.actor,
            doc_uuid,
            signature: signed.signature.clone(),
            message: None,
            inherited: None,
        };
        let (marker, _) = fixture.install_entry(Some(&value), &entry);
        let later = match binding {
            "bound later" => Some((unbound, &fixture.document)),
            "rebound later" => Some((fixture.doc_uuid, &other)),
            _ => None,
        };
        if let Some((uuid, document)) = later {
            let mut tx = fixture.doc.transaction();
            tx.put(&documents, uuid.to_string(), document.to_string()).unwrap();
            tx.commit();
        }
        match expected {
            None => assert_eq!(accepted(fixture.reconstruct(&marker)).claim, signed.claim),
            Some(reason) => assert_eq!(
                fixture.reconstruct(&marker),
                ClaimHistoryResult::ProvenForgery(ClaimHistoryError::MalformedWitness(reason)),
            ),
        }
    }
}

doc_uuid_binding_is_read_at_the_introduction! {
    bound: ("bound", None),
    rebinding_after_the_introduction_changes_nothing: ("rebound later", None),
    unbound: ("unbound", Some("unbound document UUID")),
    binding_after_the_introduction_is_too_late: ("bound later", Some("unbound document UUID")),
    conflicting_bindings: ("conflicting", Some("conflicting metadata scalar")),
}

#[test]
fn inherited_message_keeps_origin_authorship_distinct_from_the_compacted_value() {
    let origin = Fixture::new();
    let value = NativeValue::Text("historical".into());
    let signed = origin.signed(Some(&value));
    let mut branch = Fixture::new();
    branch.document = DocId::new([8; 32]);
    let entry = DatumSignature {
        extra: Default::default(),
        actor_uuid: origin.actor,
        doc_uuid: branch.doc_uuid,
        signature: signed.signature.clone(),
        message: None,
        inherited: Some(InheritedWitness {
            extra: Default::default(),
            origin_doc: origin.document,
            message: Bytes::from(signed.claim.signing_bytes().unwrap()),
            value: Some(Bytes::from(encode_native(&value))),
        }),
    };
    let (marker, _) = branch.install_entry(
        Some(&NativeValue::Str("different compacted value".into())),
        &entry,
    );
    assert_eq!(accepted(branch.reconstruct(&marker)).claim, signed.claim);
    let mut wrong = entry;
    wrong.inherited.as_mut().unwrap().value = Some(Bytes::from(encode_native(&NativeValue::Str(
        "not signed".into(),
    ))));
    let (marker, _) = branch.install_entry(Some(&NativeValue::Str("different".into())), &wrong);
    assert_eq!(
        branch.reconstruct(&marker),
        ClaimHistoryResult::ProvenForgery(ClaimHistoryError::MalformedWitness("inherited context"))
    );
}

#[test]
fn reconstruction_does_not_recurse_into_ancestor_claims() {
    let mut fixture = Fixture::new();
    let mut last_marker = None;
    for step in 0..100 {
        let (marker, _, _) = fixture.write(Some(&NativeValue::Str(format!("step-{step}"))));
        last_marker = Some(marker);
    }
    let marker = last_marker.unwrap();
    accepted(reconstruct_claim_for_marker(
        &fixture.doc,
        &fixture.datum,
        &marker,
        HistoryBudget {
            max_claims: 1,
            max_depth: 1,
            ..HistoryBudget::default()
        },
    ));
}

utils_rs::table_tests! {
    atomic_change_delivery,
    order,
    {
        let mut fixture = Fixture::new();
        fixture.write(Some(&NativeValue::Str("first".into())));
        fixture.write(Some(&NativeValue::Str("second".into())));
        let changes = fixture.doc.get_changes(&[]);
        assert_eq!(changes.len(), 3);
        let mut receiver = Automerge::new();
        let mut delivered = [false; 3];
        for index in order {
            receiver.apply_changes([changes[index].clone()]).unwrap();
            delivered[index] = true;
            let applied = delivered.iter().take_while(|seen| **seen).count();
            assert_eq!(receiver.get_changes(&[]).len(), applied);
            receiver = Automerge::load(&receiver.save()).unwrap();
            let entries = reconstruct_current_claims(
                &receiver,
                &fixture.datum,
                &receiver.get_heads(),
                HistoryBudget::default(),
            ).unwrap();
            assert_eq!(entries.len(), usize::from(applied >= 2));
            for entry in entries {
                let claim = accepted(entry.result);
                assert_eq!(
                    receiver.hash_for_opid(&entry.marker),
                    Some(claim.introduction.introduction_hash),
                );
                assert!(receiver.get_change_by_hash(&claim.introduction.introduction_hash).is_some());
                assert_eq!(claim.claim.chosen_uuid, fixture.alias);
                assert_eq!(claim.claim.actor_uuid, fixture.actor);
            }
        }
        assert_eq!(receiver.get_heads(), fixture.doc.get_heads());
    }
}

atomic_change_delivery! {
    causal_order: [0, 1, 2],
    reversed: [2, 1, 0],
    last_before_first: [2, 0, 1],
    middle_before_first: [1, 0, 2],
    first_then_last: [0, 2, 1],
    first_arrives_last: [1, 2, 0],
}

#[test]
fn resource_refusals_remain_pending_and_unknown_heads_are_malformed() {
    let mut fixture = Fixture::new();
    let (marker, _, _) = fixture.write(Some(&NativeValue::Str("bounded".into())));
    for budget in [
        HistoryBudget {
            max_claims: 0,
            ..HistoryBudget::default()
        },
        HistoryBudget {
            max_entries_per_frontier: 0,
            ..HistoryBudget::default()
        },
        HistoryBudget {
            max_value_nodes: 0,
            ..HistoryBudget::default()
        },
        HistoryBudget {
            max_bytes: 0,
            ..HistoryBudget::default()
        },
        HistoryBudget {
            max_depth: 0,
            ..HistoryBudget::default()
        },
    ] {
        assert!(matches!(
            reconstruct_claim_for_marker(&fixture.doc, &fixture.datum, &marker, budget),
            ClaimHistoryResult::Pending(ClaimHistoryError::ResourceRefused(_))
        ));
    }
    assert_eq!(
        reconstruct_current_claims(
            &fixture.doc,
            &fixture.datum,
            &[ChangeHash([255; 32])],
            HistoryBudget::default()
        ),
        Err(ClaimHistoryError::MalformedWitness("unknown heads"))
    );
}
