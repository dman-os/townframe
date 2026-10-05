use super::*;
use ed25519_dalek::SigningKey;

fn key() -> RegisterKey {
    RegisterKey {
        scope: vec![0, 255, b'/'],
        slot: vec![1, 0],
    }
}
fn limits() -> Limits {
    Limits {
        key: key(),
        max_key_bytes: 256,
        max_ciphertext_bytes: 4096,
        max_frontier_entries: 8,
        max_metadata_bytes: 512,
    }
}
fn binding(version: u8) -> RepresentationBinding {
    RepresentationBinding {
        incarnation: [3; 32],
        key_ref: b"doc/key#jwk".to_vec(),
        key_heads: vec![[version; 32]],
        encoding: b"aes128gcm".to_vec(),
        parameters: vec![0, 1],
    }
}
fn original(writer: u8, sequence: u64, body: &[u8], frontier: Vec<CausalVersion>) -> LaneHeader {
    let signer = SigningKey::from_bytes(&[writer; 32]);
    OriginalStatement::sign(
        key(),
        sequence,
        frontier,
        body.to_vec(),
        signer.verifying_key(),
        &signer,
    )
    .unwrap()
    .1
}
fn representation(header: LaneHeader, version: u8) -> Representation {
    let signer = SigningKey::from_bytes(&[99; 32]);
    Representation::sign(
        header,
        binding(version),
        vec![version; 32],
        signer.verifying_key(),
        &signer,
    )
    .unwrap()
}
fn snapshot(register: &EncryptedRegister) -> RegisterSnapshot {
    serde_json::from_slice(&serde_json::to_vec(&register.snapshot()).unwrap()).unwrap()
}
fn bytes(register: &EncryptedRegister) -> Vec<u8> {
    serde_json::to_vec(&register.snapshot()).unwrap()
}

#[test]
fn opaque_component_boundaries_and_malformed_frames() {
    for scope in [vec![], vec![0], vec![255, 0, b'/']] {
        for slot in [vec![], vec![0], vec![0, 255]] {
            let key = RegisterKey {
                scope: scope.clone(),
                slot,
            };
            let encoded = key.encode();
            assert_eq!(RegisterKey::decode(&encoded).unwrap(), key);
            for end in 0..encoded.len() {
                assert_eq!(
                    RegisterKey::decode(&encoded[..end]),
                    Err(RegisterError::InvalidKey)
                );
            }
            let mut extra = encoded;
            extra.push(0);
            assert_eq!(RegisterKey::decode(&extra), Err(RegisterError::InvalidKey));
        }
    }
    assert_ne!(
        RegisterKey {
            scope: vec![1],
            slot: vec![2, 3]
        }
        .encode(),
        RegisterKey {
            scope: vec![1, 2],
            slot: vec![3]
        }
        .encode()
    );
}

#[test]
fn concurrent_lanes_and_causal_projection_preserve_replay_fences() {
    let first = original(1, 1, b"first", vec![]);
    let second = original(2, 1, b"second", vec![]);
    let mut register = EncryptedRegister::new(limits());
    register.merge(representation(first.clone(), 1)).unwrap();
    register.merge(representation(second, 1)).unwrap();
    let mut heads: Vec<_> = register.heads().map(|head| head.writer).collect();
    heads.sort();
    let mut expected = vec![
        SigningKey::from_bytes(&[1; 32]).verifying_key().to_bytes(),
        SigningKey::from_bytes(&[2; 32]).verifying_key().to_bytes(),
    ];
    expected.sort();
    assert_eq!(heads, expected);
    let observed = original(
        2,
        2,
        b"observed",
        vec![CausalVersion {
            record: key(),
            writer: first.writer,
            writer_seq: 1,
        }],
    );
    register.merge(representation(observed.clone(), 1)).unwrap();
    assert_eq!(
        register
            .heads()
            .map(|head| head.semantic_identity())
            .collect::<Vec<_>>(),
        vec![observed.semantic_identity()]
    );
    assert_eq!(register.lanes()[&first.writer].writer_seq(), 1);
    let before = bytes(&register);
    assert_eq!(
        register.merge(representation(first, 1)).unwrap(),
        MergeOutcome::Unchanged
    );
    assert_eq!(bytes(&register), before);
}

#[test]
fn historical_wrappers_join_independently_of_arrival_and_restore() {
    let header = original(1, 7, b"stable", vec![]);
    let old = representation(header.clone(), 1);
    let new = representation(header, 2);
    let mut forward = EncryptedRegister::new(limits());
    forward.merge(old.clone()).unwrap();
    forward.merge(new.clone()).unwrap();
    let mut reverse = EncryptedRegister::new(limits());
    reverse.merge(new).unwrap();
    reverse.merge(old).unwrap();
    assert_eq!(bytes(&forward), bytes(&reverse));
    let restored = EncryptedRegister::restore(limits(), snapshot(&forward)).unwrap();
    assert_eq!(bytes(&restored), bytes(&forward));
    assert_eq!(
        restored
            .representation(&SigningKey::from_bytes(&[1; 32]).verifying_key().to_bytes())
            .unwrap()
            .binding
            .key_heads,
        vec![[1; 32]]
    );
}

#[test]
fn equivocation_is_transferable_bounded_and_order_independent() {
    let evidence = [
        representation(original(1, 4, b"a", vec![]), 1),
        representation(original(1, 4, b"b", vec![]), 1),
        representation(original(1, 4, b"c", vec![]), 1),
    ];
    let mut expected = None;
    for order in [
        [0, 1, 2],
        [2, 1, 0],
        [1, 0, 2],
        [2, 0, 1],
        [0, 2, 1],
        [1, 2, 0],
    ] {
        let mut register = EncryptedRegister::new(limits());
        for index in order {
            register.merge(evidence[index].clone()).unwrap();
        }
        assert_eq!(register.heads().next(), None);
        if let Some(expected) = &expected {
            assert_eq!(&bytes(&register), expected);
        } else {
            expected = Some(bytes(&register));
        }
        let mut recipient = EncryptedRegister::new(limits());
        recipient.merge_snapshot(snapshot(&register)).unwrap();
        assert_eq!(bytes(&recipient), bytes(&register));
        let next = original(1, 5, b"next", vec![]);
        recipient.merge(representation(next.clone(), 1)).unwrap();
        assert_eq!(recipient.heads().next(), Some(&next));
    }
}

#[test]
fn invalid_multi_lane_snapshot_cannot_partially_mutate() {
    let mut local = EncryptedRegister::new(limits());
    local
        .merge(representation(original(1, 1, b"local", vec![]), 1))
        .unwrap();
    let before = bytes(&local);
    let mut remote = EncryptedRegister::new(limits());
    remote
        .merge(representation(original(2, 2, b"new", vec![]), 1))
        .unwrap();
    remote
        .merge(representation(original(3, 2, b"bad", vec![]), 1))
        .unwrap();
    let mut invalid = snapshot(&remote);
    let writer = SigningKey::from_bytes(&[3; 32]).verifying_key().to_bytes();
    let LaneState::Current { representation } = invalid.lanes.get_mut(&writer).unwrap() else {
        unreachable!()
    };
    representation.ciphertext[0] ^= 1;
    assert_eq!(local.merge_snapshot(invalid), Err(RegisterError::Signature));
    assert_eq!(bytes(&local), before);
    local.merge_snapshot(snapshot(&remote)).unwrap();
    assert_eq!(
        local.lanes()[&SigningKey::from_bytes(&[1; 32]).verifying_key().to_bytes()].writer_seq(),
        1
    );
}

#[test]
fn all_outer_bindings_are_authenticated_and_original_is_bound() {
    let signer = SigningKey::from_bytes(&[1; 32]);
    let (statement, header) = OriginalStatement::sign(
        key(),
        1,
        vec![],
        b"private".to_vec(),
        signer.verifying_key(),
        &signer,
    )
    .unwrap();
    statement.verify_header(&header).unwrap();
    let envelope = representation(header.clone(), 1);
    for field in 0..7 {
        let mut altered = envelope.clone();
        match field {
            0 => altered.binding.incarnation[0] ^= 1,
            1 => altered.binding.key_ref.push(1),
            2 => altered.binding.key_heads[0][0] ^= 1,
            3 => altered.binding.encoding.push(1),
            4 => altered.binding.parameters.push(1),
            5 => altered.ciphertext.push(1),
            6 => altered.publisher[0] ^= 1,
            _ => unreachable!(),
        };
        assert!(altered.verify().is_err());
    }
    let (other, _) = OriginalStatement::sign(
        key(),
        1,
        vec![],
        b"different".to_vec(),
        signer.verifying_key(),
        &signer,
    )
    .unwrap();
    assert_eq!(other.verify_header(&header), Err(RegisterError::Binding));
    let wrong = SigningKey::from_bytes(&[2; 32]);
    assert_eq!(
        OriginalStatement::sign(key(), 1, vec![], vec![], wrong.verifying_key(), &signer),
        Err(RegisterError::Signature)
    );
}

#[test]
fn structural_limits_and_noncanonical_metadata_fail_without_mutation() {
    let mut register = EncryptedRegister::new(limits());
    let before = bytes(&register);
    let header = original(1, 1, b"valid", vec![]);
    let mut envelope = representation(header.clone(), 1);
    envelope.ciphertext = vec![0; 4097];
    assert_eq!(register.merge(envelope), Err(RegisterError::BodyTooLarge));
    assert_eq!(bytes(&register), before);
    let signer = SigningKey::from_bytes(&[99; 32]);
    let mut duplicate = binding(1);
    duplicate.key_heads.push([1; 32]);
    assert_eq!(
        Representation::sign(
            header.clone(),
            duplicate,
            vec![1],
            signer.verifying_key(),
            &signer
        ),
        Err(RegisterError::Binding)
    );
    let mut oversized = binding(1);
    oversized.key_ref = vec![0; 513];
    let envelope =
        Representation::sign(header, oversized, vec![1], signer.verifying_key(), &signer).unwrap();
    assert_eq!(
        register.merge(envelope),
        Err(RegisterError::MetadataTooLarge)
    );
    assert_eq!(bytes(&register), before);
}

#[test]
fn frontier_changes_are_equivocation_and_other_records_do_not_dominate() {
    let writer = SigningKey::from_bytes(&[2; 32]).verifying_key().to_bytes();
    let mut other_key = key();
    other_key.slot.push(9);
    let frontier = vec![CausalVersion {
        record: other_key,
        writer,
        writer_seq: 99,
    }];
    let plain = original(1, 1, b"same body", vec![]);
    let contextual = original(1, 1, b"same body", frontier);
    let unrelated = original(2, 1, b"other writer", vec![]);
    assert!(!contextual.observes(&unrelated));
    let mut register = EncryptedRegister::new(limits());
    register.merge(representation(plain.clone(), 1)).unwrap();
    assert_eq!(
        register.merge(representation(contextual, 1)).unwrap(),
        MergeOutcome::EquivocationRetained
    );
    assert!(matches!(
        register.lanes()[&plain.writer],
        LaneState::Equivocated { .. }
    ));
    register
        .merge(representation(unrelated.clone(), 1))
        .unwrap();
    assert_eq!(register.heads().next(), Some(&unrelated));
}

#[test]
fn cross_scope_snapshot_and_duplicate_writer_decode_are_rejected() {
    let mut register = EncryptedRegister::new(limits());
    register
        .merge(representation(original(1, 1, b"local", vec![]), 1))
        .unwrap();
    let before = bytes(&register);
    let mut wrong = snapshot(&register);
    wrong.key.scope.push(1);
    assert_eq!(register.merge_snapshot(wrong), Err(RegisterError::Binding));
    assert_eq!(bytes(&register), before);
    let mut wrong_writer = snapshot(&register);
    let lane = wrong_writer.lanes.pop_first().unwrap().1;
    wrong_writer.lanes.insert([0; 32], lane);
    assert_eq!(
        register.merge_snapshot(wrong_writer),
        Err(RegisterError::Binding)
    );
    assert_eq!(bytes(&register), before);
    let mut json = serde_json::to_value(register.snapshot()).unwrap();
    let lanes = json["lanes"].as_array_mut().unwrap();
    lanes.push(lanes[0].clone());
    assert!(serde_json::from_value::<RegisterSnapshot>(json).is_err());
}
