//! The payload laws: dropping an object's payload and listing the objects ready for it (laws
//! 1 to 4 in the [part store's docs](crate::part_store)).

use super::*;

/// Law 1: a dropped payload is no longer served or observable, tombstones stay, and dropping
/// what is not there is a no-op that stamps nothing.
pub async fn a_dropped_payload_is_gone<B: Backend>(backend: &B) -> Res<()> {
    let mut harness = Harness::open(backend).await?;
    let principal = principal(0);
    harness
        .commit(vec![
            set(&[(0, 1), (1, 2)]),
            add(&[(0, 0), (1, 0)]),
            grant(&[(0, 0)]),
        ])
        .await?;
    harness.commit(vec![remove(&[(0, 0)])]).await?;
    let before = RevSource::latest_rev(&harness.store).await?;
    assert!(harness.store.obj_states(&[obj(0)]).await?[0].is_some());

    assert_eq!(
        harness.commit(vec![drop_payloads(&[0])]).await?,
        None,
        "a part-less object has no live row: dropping stamps nothing"
    );
    assert_eq!(RevSource::latest_rev(&harness.store).await?, before);
    assert_eq!(harness.store.obj_states(&[obj(0)]).await?, vec![None]);
    assert_eq!(
        harness
            .store
            .readable_obj_states(&principal, &[obj(0), obj(1)])
            .await?[0],
        None
    );
    // the tombstone stays, and no read carries a payload for it
    let routes = LogRoutes {
        parts: [(part(0), 0)].into(),
        objects: [(obj(0), 0)].into(),
    };
    let page = harness
        .read_authorized(&principal, &routes, 0, before, limits(usize::MAX, None))
        .await?;
    assert!(page.payloads.iter().all(|(key, _)| *key != obj(0)));
    assert!(page.rows.entries.iter().any(|entry| {
        entry.key
            == LogKey::Member {
                obj: obj(0),
                part: part(0),
            }
            && entry.value.is_none()
    }));
    // the member still holds its payload
    assert!(harness.store.obj_states(&[obj(1)]).await?[0].is_some());

    // idempotent, and an object never set is a no-op
    assert_eq!(harness.commit(vec![drop_payloads(&[0, 0, 3])]).await?, None);

    // an object with only pending memberships has no payload to drop; they stay pending
    harness.commit(vec![add(&[(2, 1)])]).await?;
    harness.commit(vec![drop_payloads(&[2])]).await?;
    assert_eq!(
        harness.store.obj_states(&[obj(2)]).await?[0]
            .as_ref()
            .map(|state| state.parts.clone()),
        Some([part(1)].into())
    );

    // a dropped object can come back: added, it is pending; its payload brings it live
    harness.commit(vec![add(&[(0, 0)])]).await?;
    assert_eq!(harness.store.member_count(&part(0)).await?, 1);
    assert_eq!(
        harness.commit(vec![set(&[(0, 1)])]).await?,
        Some(before + 1)
    );
    assert_eq!(harness.store.member_count(&part(0)).await?, 2);
    Ok(())
}

/// Law 2: dropping the payload of a live member is a caller bug and panics. A test of a backend
/// expects the panic (`#[should_panic(expected = "live member")]`).
pub async fn dropping_the_payload_of_a_member_is_a_caller_bug<B: Backend>(backend: &B) -> Res<()> {
    let mut harness = Harness::open(backend).await?;
    harness.commit(vec![set(&[(0, 1)]), add(&[(0, 0)])]).await?;
    // straight to the store, not through the oracle (which has the same check): the store's
    // own panic is what a backend's test expects
    let mut tx = harness.backend.begin(&harness.store).await?;
    stage(&harness.store, &mut tx, drop_payloads(&[0])).await?;
    harness.backend.commit(tx).await?;
    unreachable!("the drop of a member's payload went through without a panic");
}

/// Law 2, the valid order: remove from every part, then drop, in one transaction. The check
/// sees the transaction's earlier batches.
pub async fn removing_then_dropping_in_one_transaction_is_valid<B: Backend>(
    backend: &B,
) -> Res<()> {
    let mut harness = Harness::open(backend).await?;
    harness
        .commit(vec![set(&[(0, 1)]), add(&[(0, 0), (0, 1)])])
        .await?;
    harness
        .commit(vec![remove(&[(0, 0), (0, 1)]), drop_payloads(&[0])])
        .await?;
    assert_eq!(harness.store.obj_states(&[obj(0)]).await?, vec![None]);
    assert!(
        harness
            .store
            .partless_objects(usize::MAX, None)
            .await?
            .is_empty()
    );
    Ok(())
}

async fn listed<B: Backend>(harness: &Harness<B>) -> Res<Vec<ObjKey>> {
    harness.store.partless_objects(usize::MAX, None).await
}

/// Law 3: a part-less object is listed, in key order and keyset-paged, from the moment its last
/// membership goes until its payload is dropped or it joins a part again; nothing else is.
pub async fn a_partless_object_is_listed_until_its_payload_is_dropped<B: Backend>(
    backend: &B,
) -> Res<()> {
    let mut harness = Harness::open(backend).await?;
    harness
        .commit(vec![
            set(&[(0, 1), (1, 1), (2, 1)]),
            add(&[(0, 0), (1, 0), (1, 1), (2, 2), (3, 0)]),
        ])
        .await?;
    assert_eq!(listed(&harness).await?, vec![], "members, and a pending 3");

    harness.commit(vec![remove(&[(1, 0)])]).await?;
    assert_eq!(listed(&harness).await?, vec![], "1 is still in part 1");
    harness
        .commit(vec![remove(&[(1, 1), (0, 0), (2, 2)])])
        .await?;
    assert_eq!(listed(&harness).await?, vec![obj(0), obj(1), obj(2)]);

    // keyset paging
    let store = &harness.store;
    assert_eq!(store.partless_objects(2, None).await?, vec![obj(0), obj(1)]);
    assert_eq!(
        store.partless_objects(2, Some(&obj(1))).await?,
        vec![obj(2)]
    );
    assert_eq!(store.partless_objects(0, None).await?, vec![]);

    harness.commit(vec![add(&[(1, 2)])]).await?;
    assert_eq!(listed(&harness).await?, vec![obj(0), obj(2)], "1 joined");
    harness.commit(vec![drop_payloads(&[0])]).await?;
    assert_eq!(listed(&harness).await?, vec![obj(2)], "0 was dropped");
    harness.commit(vec![drop_payloads(&[2])]).await?;
    assert_eq!(listed(&harness).await?, vec![]);
    // 3 gets its payload only now: a member, not part-less
    harness.commit(vec![set(&[(3, 1)])]).await?;
    assert_eq!(listed(&harness).await?, vec![]);
    Ok(())
}

/// Law 4: the drop is a batch of its transaction: invisible before commit, gone with a
/// rollback, in order with the other batches after commit.
pub async fn dropping_is_atomic_with_its_transaction<B: Backend>(backend: &B) -> Res<()> {
    let mut harness = Harness::open(backend).await?;
    harness
        .commit(vec![set(&[(0, 1), (1, 1)]), add(&[(0, 0), (1, 0)])])
        .await?;
    harness.commit(vec![remove(&[(0, 0), (1, 0)])]).await?;
    let writes = vec![drop_payloads(&[0, 1]), add(&[(1, 1)])];

    harness.rollback(writes.clone()).await?;
    assert_eq!(
        harness.store.partless_objects(usize::MAX, None).await?,
        vec![obj(0), obj(1)],
        "a rollback keeps the payloads"
    );

    let mut tx = harness.backend.begin(&harness.store).await?;
    for write in writes.clone() {
        stage(&harness.store, &mut tx, write).await?;
    }
    harness.check().await?;
    assert_eq!(
        harness.store.partless_objects(usize::MAX, None).await?,
        vec![obj(0), obj(1)],
        "staged, not committed: the payloads are still there"
    );
    harness.backend.commit(tx).await?;
    harness.oracle.commit(writes);
    harness.check().await?;
    assert_eq!(harness.store.obj_states(&[obj(0)]).await?, vec![None]);
    assert_eq!(
        harness.store.obj_states(&[obj(1)]).await?[0]
            .as_ref()
            .map(|state| (state.content.clone(), state.parts.clone())),
        Some((None, [part(1)].into())),
        "the add after the drop is pending"
    );
    // a payload set after the drop, in the same transaction, survives
    harness.commit(vec![set(&[(0, 1)])]).await?;
    harness
        .commit(vec![drop_payloads(&[0]), set(&[(0, 2)])])
        .await?;
    assert_eq!(
        harness.store.obj_states(&[obj(0)]).await?[0]
            .as_ref()
            .map(|state| state.content.clone().map(|(payload, _)| payload)),
        Some(Some(payload(2)))
    );
    Ok(())
}
