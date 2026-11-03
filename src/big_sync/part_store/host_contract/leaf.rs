use super::*;

pub(super) fn live_entry(
    seed: FingerprintSeed,
    obj_id: ObjKey,
    source_revision: CursorIndex,
    payload: ObjPayload,
) -> BucketObjPageEntry {
    BucketObjPageEntry {
        fp: Fingerprint::new(&seed, &("big-sync-obj-fp-v1", obj_id.clone(), payload)),
        obj_id,
        source_revision,
        dead: false,
    }
}

pub(super) fn dead_entry(
    seed: FingerprintSeed,
    obj_id: ObjKey,
    source_revision: CursorIndex,
) -> BucketObjPageEntry {
    BucketObjPageEntry {
        fp: Fingerprint::new(
            &seed,
            &(
                "big-sync-obj-fp-v1",
                obj_id.clone(),
                serde_json::Value::Null,
            ),
        ),
        obj_id,
        source_revision,
        dead: true,
    }
}

pub(super) fn encoded_bytes(page: &LeafBucketPage) -> usize {
    page.entries
        .iter()
        .map(|entry| leaf_entry_wire_bytes(entry.obj_id.as_bytes().len(), entry.source_revision))
        .sum()
}

pub(super) async fn assert_unknown_part(
    store: &dyn HostPartStore,
    part_id: PartKey,
    buck_id: BuckId,
    seed: FingerprintSeed,
    subscriber: PeerKey,
) -> Res<()> {
    match store
        .leaf_buckets(
            LeafBucketsRequest {
                part_id,
                since: 0,
                buckets: vec![LeafBucketRequest {
                    buck_id,
                    after: None,
                }],
                seed,
                limit_hint: 2,
            },
            subscriber,
        )
        .await?
    {
        Err(LeafBucketsError::UnkownPart) => {}
        other => panic!("unexpected leaf_buckets result: {other:?}"),
    }
    Ok(())
}

/// An object key of `key_len` bytes that lands in `bucket_id` at its own level.
///
/// [`obj_in_bucket`]'s wide twin: only the leading counter bytes are searched and the tail
/// is filler, so a key's *width* is a parameter while the bucket it lands in is still found
/// rather than assumed.
pub(super) fn wide_obj_in_bucket(bucket_id: BuckId, key_len: usize, salt: u8) -> ObjKey {
    assert!(key_len > 4, "the counter needs room in front of the filler");
    let mut matches: u8 = 0;
    let mut counter: u32 = 0;
    loop {
        let mut bytes = vec![0u8; key_len];
        bytes[..4].copy_from_slice(&counter.to_be_bytes());
        let obj_id = ObjKey::new(bytes);
        if BuckId::from_obj_key(bucket_id.level(), &obj_id) == bucket_id {
            matches += 1;
            if matches == salt {
                return obj_id;
            }
        }
        counter = counter
            .checked_add(1)
            .expect("some key must hash into the requested bucket");
    }
}

/// A 32-byte key that lands in `bucket_id` and sorts strictly after `after`.
///
/// The keyset page resumes at `obj_id > after_id`, so pinning "the byte budget
/// stops the page" needs a row the budget rejected to be followed by a narrower
/// row that would fit; only a key ordered after the wide one makes skipping it
/// observable. The search walks counters upward, so the first match is the next
/// key in `obj_id` order.
pub(super) fn narrow_obj_after(bucket_id: BuckId, after: &ObjKey) -> ObjKey {
    let mut counter: u32 = 0;
    loop {
        let mut bytes = [0u8; 32];
        bytes[..4].copy_from_slice(&counter.to_be_bytes());
        let obj_id = ObjKey::new(bytes);
        if BuckId::from_obj_key(bucket_id.level(), &obj_id) == bucket_id && obj_id > *after {
            return obj_id;
        }
        counter = counter.checked_add(1).expect("some key must sort after");
    }
}

pub async fn assert_leaf_source_revision_contract<S: HostPartStore + Sync + ?Sized>(
    store: &S,
) -> Res<()> {
    let part_a = test_part(61);
    let part_b = test_part(62);
    let obj = ObjKey::new(b"source-revision-member".as_slice());
    let other = ObjKey::new(b"source-revision-other".as_slice());
    let subscriber = grant_bucket_read(store, [part_a.clone(), part_b.clone()]).await?;
    store
        .set_obj_payload(obj.clone(), payload("revision", 1))
        .await?;
    store
        .add_obj_to_parts(obj.clone(), vec![part_a.clone()])
        .await?;
    let revision_a = store.latest_revision().await?;
    store
        .add_obj_to_parts(obj.clone(), vec![part_b.clone()])
        .await?;
    let revision_b = store.latest_revision().await?;
    assert!(revision_a < revision_b);
    store
        .set_obj_payload(other.clone(), payload("other", 1))
        .await?;
    store
        .add_obj_to_parts(other, vec![part_a.clone(), part_b.clone()])
        .await?;
    assert!(store.latest_revision().await? > revision_b);
    for (part, revision) in [(part_a.clone(), revision_a), (part_b.clone(), revision_b)] {
        let leaf = store
            .leaf_buckets(
                LeafBucketsRequest {
                    part_id: part.clone(),
                    since: 0,
                    buckets: vec![LeafBucketRequest {
                        buck_id: BuckId::ROOT,
                        after: None,
                    }],
                    seed: FingerprintSeed::new(5, 6),
                    limit_hint: 16,
                },
                subscriber.clone(),
            )
            .await??;
        let entry = leaf.bucks[&BuckId::ROOT]
            .entries
            .iter()
            .find(|entry| entry.obj_id == obj)
            .unwrap();
        assert!(!entry.dead);
        assert_eq!(entry.source_revision, revision);
        let events = store.list_events([part.clone()].into(), 0, 16).await??;
        assert!(events[&part].events.iter().any(|event| matches!(event, PartEvent::Changed(event) if event.obj_id == obj && event.cursor == revision)));
    }
    store
        .remove_obj_from_part(obj.clone(), part_a.clone())
        .await?;
    let removed_revision = store.latest_revision().await?;
    for (part, revision, dead) in [
        (part_a, removed_revision, true),
        (part_b, revision_b, false),
    ] {
        let leaf = store
            .leaf_buckets(
                LeafBucketsRequest {
                    part_id: part,
                    since: 0,
                    buckets: vec![LeafBucketRequest {
                        buck_id: BuckId::ROOT,
                        after: None,
                    }],
                    seed: FingerprintSeed::new(5, 6),
                    limit_hint: 16,
                },
                subscriber.clone(),
            )
            .await??;
        let entry = leaf.bucks[&BuckId::ROOT]
            .entries
            .iter()
            .find(|entry| entry.obj_id == obj)
            .unwrap();
        assert_eq!(entry.dead, dead);
        assert_eq!(entry.source_revision, revision);
    }
    Ok(())
}
