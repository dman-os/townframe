use super::{
    ActorMetadata, Attribution, Cid, ClaimOperation, Dmeta, DmetaError, DocumentId, Evidence,
    FacetClaim, FacetMetadata, FacetSignature, FacetValue, MultibaseKey, NativeValue,
    SignatureBytes, SignedFacetClaim, Uuid7, VerificationState, cid_of, encode_native,
};
use automerge::{ActorId, Automerge, ObjId, ObjType, ROOT, ReadDoc, transaction::Transactable};
use ed25519_dalek::{Signer, SigningKey};
use std::collections::BTreeMap;
use uuid::Uuid;

const FACET: &str = "org.example.note/main";

fn uuid7(index: u128) -> Uuid7 {
    Uuid7::try_from(Uuid::from_u128(
        0x0189_0f3e_1234_7000_8000_0000_0000_0000 | index,
    ))
    .unwrap()
}

fn current_value(doc: &impl ReadDoc) -> NativeValue {
    NativeValue::read_at(doc, &ROOT, FACET).unwrap().unwrap()
}

fn signed_claim(doc: &impl ReadDoc, actor: u8, predecessors: Vec<Cid>) -> SignedFacetClaim {
    let value = current_value(doc);
    let claim = FacetClaim {
        origin_document: DocumentId::new(b"facet-verification-e2e"),
        facet_key: FACET.parse().unwrap(),
        chosen_uuid: uuid7(1),
        actor_uuid: uuid7(100 + u128::from(actor)),
        operation: ClaimOperation::Set,
        value_cid: Some(cid_of(&encode_native(&value))),
        predecessors,
    };
    let key = SigningKey::from_bytes(&[actor; 32]);
    let signature = SignatureBytes::try_from(
        key.sign(&claim.signing_bytes().unwrap())
            .to_bytes()
            .as_slice(),
    )
    .unwrap();
    let signed = SignedFacetClaim::from_signature(
        claim,
        MultibaseKey::try_from(key.verifying_key().as_bytes().as_slice()).unwrap(),
        signature,
    )
    .unwrap();
    signed.verify().unwrap();
    assert_eq!(
        signed.value_evidence(FacetValue::Present(&value)),
        Evidence::Present
    );
    signed
}

fn fixture() -> (Automerge, ObjId, SignedFacetClaim) {
    let mut doc = Automerge::new().with_actor(ActorId::from([0]));
    let mut transaction = doc.transaction();
    let facet = transaction.put_object(ROOT, FACET, ObjType::Map).unwrap();
    transaction.put(&facet, "n", 0_i64).unwrap();
    let signed = signed_claim(&transaction, 0, Vec::new());
    transaction.commit();
    (doc, facet, signed)
}

fn verify_current(doc: &Automerge, claims: &[&SignedFacetClaim]) -> VerificationState {
    let mut dmeta = Dmeta::new(jiff::Timestamp::from_second(0).unwrap());
    let doc_uuid = dmeta
        .document_uuid(&DocumentId::new(b"facet-verification-e2e"), uuid7(9))
        .unwrap();
    let mut issuers = BTreeMap::new();
    let mut signatures = Vec::new();
    let node = SigningKey::from_bytes(&[250; 32]);
    let issuer = MultibaseKey::try_from(node.verifying_key().as_bytes().as_slice()).unwrap();
    for signed in claims {
        assert_eq!(
            signed.claim.origin_document,
            DocumentId::new(b"facet-verification-e2e")
        );
        assert_eq!(signed.claim.facet_key, FACET.parse().unwrap());
        let attribution = Attribution {
            issuer: issuer.clone(),
            actor_uuid: signed.claim.actor_uuid,
            pubkey: signed.actor_public_key.clone(),
            actor_url: format!("test:actor:{}", signed.claim.actor_uuid),
        };
        let certificate = attribution
            .certificate(
                SignatureBytes::try_from(
                    node.sign(&attribution.signing_bytes().unwrap())
                        .to_bytes()
                        .as_slice(),
                )
                .unwrap(),
            )
            .unwrap();
        let actor = ActorMetadata {
            pubkey: attribution.pubkey.clone(),
            actor_url: attribution.actor_url.clone(),
            delegation: Some(certificate),
        };
        assert_eq!(
            Attribution::verify(
                actor.delegation.as_ref().unwrap(),
                &issuer,
                signed.claim.actor_uuid,
                &actor,
            )
            .unwrap(),
            attribution
        );
        assert!(
            dmeta
                .actors
                .insert(signed.claim.actor_uuid, actor)
                .is_none()
        );
        assert!(
            issuers
                .insert(signed.claim.actor_uuid, issuer.clone())
                .is_none()
        );
        dmeta
            .record_facet_alias(signed.claim.chosen_uuid, signed.claim.facet_key.clone())
            .unwrap();
        let mut entry = FacetSignature {
            actor_uuid: signed.claim.actor_uuid,
            doc_uuid,
            signature: signed.signature.clone(),
            message: None,
            inherited: None,
        };
        entry.carry_message(&signed.claim).unwrap();
        signatures.push(entry);
    }
    assert!(
        dmeta
            .facets
            .insert(
                FACET.parse().unwrap(),
                FacetMetadata {
                    updated_at: Vec::new(),
                    deleted_at: Vec::new(),
                    signatures,
                }
            )
            .is_none()
    );
    let value = current_value(doc);
    dmeta.verify_facet(
        &FACET.parse().unwrap(),
        FacetValue::Present(&value),
        &BTreeMap::new(),
        &issuers,
    )
}

#[test]
fn historical_value_only_merge_can_resurrect_a_losing_unsigned_assignment() {
    let (base, facet, genesis) = fixture();
    let mut safe = base.fork().with_actor(ActorId::from([2]));
    let mut transaction = safe.transaction();
    transaction.put(&facet, "x", "safe").unwrap();
    let common = signed_claim(&transaction, 2, vec![genesis.signature_id().unwrap()]);
    transaction.commit();
    let common_id = common.signature_id().unwrap();

    let mut unsigned = base.fork().with_actor(ActorId::from([1]));
    let mut transaction = unsigned.transaction();
    transaction.put(&facet, "x", "evil").unwrap();
    transaction.commit();
    let unsigned_assignment = unsigned.get(&facet, "x").unwrap().unwrap().1;
    let unsigned_change = unsigned.get_heads();

    let mut left = safe.fork().with_actor(ActorId::from([3]));
    left.merge(&mut unsigned).unwrap();
    assert_eq!(left.get_all(&facet, "x").unwrap().len(), 2);
    assert_eq!(
        NativeValue::read_at(&left, &facet, "x").unwrap(),
        Some(NativeValue::Str("safe".into()))
    );
    let mut transaction = left.transaction();
    transaction.put(&facet, "n", 1_i64).unwrap();
    let left_claim = signed_claim(&transaction, 3, vec![common_id.clone()]);
    transaction.commit();
    let left_heads = left.get_heads();

    let mut right = safe.fork().with_actor(ActorId::from([4]));
    let mut transaction = right.transaction();
    transaction.delete(&facet, "x").unwrap();
    let right_claim = signed_claim(&transaction, 4, vec![common_id.clone()]);
    transaction.commit();
    let right_heads = right.get_heads();
    assert_eq!(right.get(&facet, "x").unwrap(), None);
    assert_eq!(left_claim.claim.predecessors, vec![common_id.clone()]);
    assert_eq!(right_claim.claim.predecessors, vec![common_id]);
    assert_eq!(
        verify_current(&left, &[&left_claim]),
        VerificationState::Accepted
    );
    assert_eq!(
        verify_current(&right, &[&right_claim]),
        VerificationState::Accepted
    );

    let mut current = left.clone();
    current.merge(&mut right).unwrap();
    let merged_value = current_value(&current);
    assert_eq!(
        merged_value,
        NativeValue::Map(BTreeMap::from([
            ("n".into(), NativeValue::Int(1)),
            ("x".into(), NativeValue::Str("evil".into())),
        ]))
    );
    assert_eq!(
        current.get(&facet, "x").unwrap().unwrap().1,
        unsigned_assignment
    );
    assert_eq!(
        current.hash_for_opid(&unsigned_assignment),
        Some(unsigned_change[0])
    );

    let mut reconstructed_left = current.fork_at(&left_heads).unwrap();
    let mut reconstructed_right = current.fork_at(&right_heads).unwrap();
    assert_eq!(reconstructed_left.get_all(&facet, "x").unwrap().len(), 2);
    for (claim, historical) in [
        (&left_claim, &reconstructed_left),
        (&right_claim, &reconstructed_right),
    ] {
        claim.verify().unwrap();
        assert_eq!(
            claim.value_evidence(FacetValue::Present(&current_value(historical))),
            Evidence::Present
        );
    }
    reconstructed_left.merge(&mut reconstructed_right).unwrap();
    assert_eq!(current_value(&reconstructed_left), merged_value);
    for claim in [&genesis, &common, &left_claim, &right_claim] {
        claim.verify().unwrap();
        assert_eq!(
            claim.value_evidence(FacetValue::Present(&merged_value)),
            Evidence::Invalid
        );
    }
    assert_eq!(
        verify_current(&current, &[&left_claim, &right_claim]),
        VerificationState::Uncovered
    );
}

#[test]
fn valid_concurrent_disjoint_writes_merge_to_a_value_neither_actor_signed() {
    let (base, facet, common) = fixture();
    let common_id = common.signature_id().unwrap();
    let mut left = base.fork().with_actor(ActorId::from([1]));
    let mut transaction = left.transaction();
    transaction.put(&facet, "x", "left").unwrap();
    let left_claim = signed_claim(&transaction, 1, vec![common_id.clone()]);
    transaction.commit();
    let left_heads = left.get_heads();
    let mut right = base.fork().with_actor(ActorId::from([2]));
    let mut transaction = right.transaction();
    transaction.put(&facet, "y", "right").unwrap();
    let right_claim = signed_claim(&transaction, 2, vec![common_id.clone()]);
    transaction.commit();
    let right_heads = right.get_heads();
    assert_eq!(left_claim.claim.predecessors, vec![common_id.clone()]);
    assert_eq!(right_claim.claim.predecessors, vec![common_id]);
    assert_eq!(
        verify_current(&left, &[&left_claim]),
        VerificationState::Accepted
    );
    assert_eq!(
        verify_current(&right, &[&right_claim]),
        VerificationState::Accepted
    );

    left.merge(&mut right).unwrap();
    let merged_value = current_value(&left);
    assert_eq!(
        merged_value,
        NativeValue::Map(BTreeMap::from([
            ("n".into(), NativeValue::Int(0)),
            ("x".into(), NativeValue::Str("left".into())),
            ("y".into(), NativeValue::Str("right".into())),
        ]))
    );
    for (claim, heads) in [(&left_claim, left_heads), (&right_claim, right_heads)] {
        claim.verify().unwrap();
        let historical = left.fork_at(&heads).unwrap();
        assert_eq!(
            claim.value_evidence(FacetValue::Present(&current_value(&historical))),
            Evidence::Present
        );
        assert_eq!(
            claim.value_evidence(FacetValue::Present(&merged_value)),
            Evidence::Invalid
        );
    }
    assert_eq!(
        verify_current(&left, &[&left_claim, &right_claim]),
        VerificationState::Uncovered
    );
}

#[test]
fn exact_replacement_of_forged_content_recovers_without_trusting_its_parent() {
    let (mut doc, old_facet, common) = fixture();
    let mut transaction = doc.transaction();
    transaction.put(&old_facet, "n", 9_i64).unwrap();
    transaction.put(&old_facet, "x", "forged").unwrap();
    let mut forged = signed_claim(&transaction, 1, vec![common.signature_id().unwrap()]);
    transaction.commit();
    let forged_heads = doc.get_heads();
    let forged_value = current_value(&doc);
    let mut signature = *forged.signature.as_bytes();
    signature[0] ^= 1;
    forged.signature = SignatureBytes::try_from(signature.as_slice()).unwrap();
    assert!(matches!(forged.verify(), Err(DmetaError::InvalidSignature)));
    assert_eq!(
        forged.value_evidence(FacetValue::Present(&forged_value)),
        Evidence::Present
    );
    assert_eq!(
        verify_current(&doc, &[&forged]),
        VerificationState::Rejected
    );
    let forged_id = forged.signature_id().unwrap();

    let mut transaction = doc.transaction();
    let replacement = transaction.put_object(ROOT, FACET, ObjType::Map).unwrap();
    transaction.put(&replacement, "n", 2_i64).unwrap();
    let repaired = signed_claim(&transaction, 2, vec![forged_id.clone()]);
    transaction.commit();
    assert_ne!(replacement, old_facet);
    assert_eq!(doc.get_all(ROOT, FACET).unwrap().len(), 1);
    let repaired_value = current_value(&doc);
    assert_eq!(
        repaired_value,
        NativeValue::Map(BTreeMap::from([("n".into(), NativeValue::Int(2)),]))
    );
    repaired.verify().unwrap();
    assert_eq!(repaired.claim.predecessors, vec![forged_id]);
    assert_eq!(
        repaired.value_evidence(FacetValue::Present(&repaired_value)),
        Evidence::Present
    );
    assert_eq!(
        verify_current(&doc, &[&repaired]),
        VerificationState::Accepted
    );
    assert_eq!(
        verify_current(&doc, &[&forged, &repaired]),
        VerificationState::Accepted
    );
    assert_eq!(
        verify_current(&doc, &[&repaired, &forged]),
        VerificationState::Accepted
    );
    let history = doc.fork_at(&forged_heads).unwrap();
    assert_eq!(current_value(&history), forged_value);
    assert!(matches!(forged.verify(), Err(DmetaError::InvalidSignature)));
    assert_eq!(
        verify_current(&history, &[&forged]),
        VerificationState::Rejected
    );
}

#[test]
fn unsigned_post_signature_mutation_is_uncovered_not_a_forged_signature() {
    let (mut doc, facet, signed) = fixture();
    let signed_heads = doc.get_heads();
    let signed_value = current_value(&doc);
    assert_eq!(
        verify_current(&doc, &[&signed]),
        VerificationState::Accepted
    );
    let mut transaction = doc.transaction();
    transaction.put(&facet, "n", 1_i64).unwrap();
    transaction.commit();
    let mutated_value = current_value(&doc);
    assert_ne!(mutated_value, signed_value);
    signed.verify().unwrap();
    assert_eq!(
        signed.value_evidence(FacetValue::Present(&mutated_value)),
        Evidence::Invalid
    );
    assert_eq!(
        verify_current(&doc, &[&signed]),
        VerificationState::Uncovered
    );
    let historical = doc.fork_at(&signed_heads).unwrap();
    assert_eq!(current_value(&historical), signed_value);
    assert_eq!(
        verify_current(&historical, &[&signed]),
        VerificationState::Accepted
    );
}
