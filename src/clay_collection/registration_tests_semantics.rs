use std::future::Future;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use super::*;
use crate::types::ReadLimits;

fn ready<T>(future: impl Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    let Poll::Ready(value) = future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("expected a ready read");
    };
    value
}

fn rows(
    collection: &RegistrationCollection<u32, &'static str>,
) -> Vec<((RegistrationScopeId, u32), &'static str)> {
    collection
        .snapshot()
        .into_iter()
        .map(|(key, entry)| (key, entry.value))
        .collect()
}

#[test]
fn scopes_sharing_a_local_key_hold_distinct_rows() {
    let collection = RegistrationCollection::new([1, 2]);
    let a_scope = collection.create_scope();
    let b_scope = collection.create_scope();
    let a = a_scope.writer().unwrap();
    let b = b_scope.writer().unwrap();
    let a_version = a.put(7, None, "a").unwrap().dispatch();
    let b_version = b.put(7, None, "b").unwrap().dispatch();
    assert_eq!(
        rows(&collection),
        [((a.scope_id(), 7), "a"), ((b.scope_id(), 7), "b")]
    );
    assert_eq!(
        b.put(7, Some(a_version), "stolen")
            .map(RegistrationDispatch::dispatch),
        Err(MutationError::Conflict {
            current: Some(b_version)
        })
    );
    assert_eq!(
        b.remove(8, None).map(RegistrationDispatch::dispatch),
        Ok(None)
    );
    assert_eq!(
        b.remove(8, Some(a_version))
            .map(RegistrationDispatch::dispatch),
        Err(MutationError::Conflict { current: None })
    );
    a_scope.close().dispatch();
    assert_eq!(rows(&collection), [((b.scope_id(), 7), "b")]);
}

#[test]
fn replacement_handles_share_cas_and_writer_drop_preserves_rows() {
    let collection = RegistrationCollection::new([1, 2]);
    let scope = collection.create_scope();
    let writer = scope.writer().unwrap();
    let first = writer.put(1, None, "one").unwrap().dispatch();
    drop(writer);
    assert_eq!(rows(&collection), [((scope.id(), 1), "one")]);
    let replacement = scope.writer().unwrap();
    let peer = scope.writer().unwrap();
    assert_eq!(replacement.version(&1), Some(first));
    assert_eq!(
        replacement
            .put(1, None, "blind")
            .map(RegistrationDispatch::dispatch),
        Err(MutationError::Conflict {
            current: Some(first)
        })
    );
    let second = replacement.put(1, Some(first), "two").unwrap().dispatch();
    assert!(second > first);
    assert_eq!(peer.version(&1), Some(second));
    assert_eq!(
        peer.remove(1, Some(first))
            .map(RegistrationDispatch::dispatch),
        Err(MutationError::Conflict {
            current: Some(second)
        })
    );
    let removed = peer.remove(1, Some(second)).unwrap().dispatch().unwrap();
    assert!(removed > second);
    assert_eq!(replacement.row_count(), 0);
    assert_eq!(
        replacement
            .remove(1, None)
            .map(RegistrationDispatch::dispatch),
        Ok(None)
    );
    let recreated = peer.put(1, None, "three").unwrap().dispatch();
    assert!(recreated > removed);
    assert_eq!(
        replacement
            .put(1, Some(second), "stale")
            .map(RegistrationDispatch::dispatch),
        Err(MutationError::Conflict {
            current: Some(recreated)
        })
    );
}

#[test]
fn concurrent_handles_serialize_cas_against_one_scope() {
    let collection = RegistrationCollection::new([1, 2]);
    let scope = collection.create_scope();
    let first = scope
        .writer()
        .unwrap()
        .put(1, None, 0)
        .map(RegistrationDispatch::dispatch)
        .unwrap();
    let barrier = std::sync::Barrier::new(8);
    let outcomes = std::thread::scope(|threads| {
        let joins = (1..=8)
            .map(|value| {
                let writer = scope.writer().unwrap();
                let barrier = &barrier;
                threads.spawn(move || {
                    barrier.wait();
                    (
                        value,
                        writer
                            .put(1, Some(first), value)
                            .map(RegistrationDispatch::dispatch),
                    )
                })
            })
            .collect::<Vec<_>>();
        joins
            .into_iter()
            .map(|join| join.join().unwrap())
            .collect::<Vec<_>>()
    });
    let winners = outcomes
        .iter()
        .filter_map(|(value, outcome)| outcome.as_ref().ok().map(|version| (*value, *version)))
        .collect::<Vec<_>>();
    assert_eq!(winners.len(), 1);
    let (value, version) = winners[0];
    for (_, outcome) in &outcomes {
        if outcome.is_err() {
            assert_eq!(
                *outcome,
                Err(MutationError::Conflict {
                    current: Some(version)
                })
            );
        }
    }
    assert_eq!(
        collection.snapshot()[&(scope.id(), 1)],
        Entry {
            version,
            value,
            evidence: RowEvidence::Authoritative,
            write_context: None
        }
    );
}

#[test]
fn scope_close_withdraws_only_its_rows_atomically_and_invalidates_handles() {
    let collection = RegistrationCollection::new([1, 2]);
    let a_scope = collection.create_scope();
    let b_scope = collection.create_scope();
    let a = a_scope.writer().unwrap();
    let peer = a_scope.writer().unwrap();
    let b = b_scope.writer().unwrap();
    let first = a.put(1, None, "a1").unwrap().dispatch();
    peer.put(2, None, "a2").unwrap().dispatch();
    b.put(1, None, "b1").unwrap().dispatch();
    let mut lease = collection.projection().acquire((), None).unwrap();
    let bootstrap = ready(lease.read(None, ReadLimits::default())).unwrap();
    drop(a);
    let unchanged = lease
        .read(Some(bootstrap.through.clone()), ReadLimits::default())
        .complete_timeout()
        .unwrap();
    assert_eq!(unchanged.through, bootstrap.through);
    a_scope.close().dispatch();
    let page = ready(lease.read(
        Some(bootstrap.through),
        ReadLimits {
            max_transactions: Some(1),
            max_changes: Some(1),
        },
    ))
    .unwrap();
    assert_eq!(page.transactions.len(), 1);
    assert_eq!(
        page.transactions[0]
            .changes
            .iter()
            .map(|change| match change {
                Change::Remove { key, .. } => *key,
                Change::Upsert { .. } => panic!("closure published an upsert"),
            })
            .collect::<Vec<_>>(),
        [(a_scope.id(), 1), (a_scope.id(), 2)]
    );
    assert_eq!(rows(&collection), [((b_scope.id(), 1), "b1")]);
    assert_eq!(
        peer.put(1, None, "late")
            .map(RegistrationDispatch::dispatch),
        Err(MutationError::Closed)
    );
    assert_eq!(
        peer.remove(1, Some(first))
            .map(RegistrationDispatch::dispatch),
        Err(MutationError::Closed)
    );
    assert_eq!(
        peer.clear().map(RegistrationDispatch::dispatch),
        Err(MutationError::Closed)
    );
    assert_eq!(a_scope.writer().unwrap_err(), MutationError::Closed);
    a_scope.close().dispatch();
    drop(a_scope);
    let unchanged = lease
        .read(Some(page.through.clone()), ReadLimits::default())
        .complete_timeout()
        .unwrap();
    assert_eq!(unchanged.through, page.through);
}

#[test]
fn owner_drop_closes_scope_even_while_writer_survives() {
    let collection = RegistrationCollection::new([1, 2]);
    let scope = collection.create_scope();
    let writer = scope.writer().unwrap();
    writer.put(1, None, "owned").unwrap().dispatch();
    drop(scope);
    assert!(rows(&collection).is_empty());
    assert_eq!(
        writer
            .put(2, None, "late")
            .map(RegistrationDispatch::dispatch),
        Err(MutationError::Closed)
    );
    assert_eq!(writer.version(&1), None);
    assert_eq!(writer.row_count(), 0);
}

#[test]
fn clear_is_atomic_scope_local_and_keeps_handles_usable() {
    let collection = RegistrationCollection::new([1, 2]);
    let scope = collection.create_scope();
    let other = collection.create_scope();
    let writer = scope.writer().unwrap();
    let peer = scope.writer().unwrap();
    let first = writer.put(1, None, "one").unwrap().dispatch();
    peer.put(2, None, "two").unwrap().dispatch();
    other
        .writer()
        .unwrap()
        .put(1, None, "kept")
        .map(RegistrationDispatch::dispatch)
        .unwrap();
    let mut lease = collection.projection().acquire((), None).unwrap();
    let bootstrap = ready(lease.read(None, ReadLimits::default())).unwrap();
    peer.clear().unwrap().dispatch();
    let page = ready(lease.read(
        Some(bootstrap.through),
        ReadLimits {
            max_transactions: Some(1),
            max_changes: Some(1),
        },
    ))
    .unwrap();
    assert_eq!(page.transactions.len(), 1);
    assert_eq!(page.transactions[0].changes.len(), 2);
    assert!(
        page.transactions[0]
            .changes
            .iter()
            .all(|change| matches!(change, Change::Remove { key, .. } if key.0 == scope.id()))
    );
    assert_eq!(rows(&collection), [((other.id(), 1), "kept")]);
    assert_eq!(writer.row_count(), 0);
    writer.clear().unwrap().dispatch();
    assert_eq!(
        lease
            .read(Some(page.through.clone()), ReadLimits::default())
            .complete_timeout()
            .unwrap()
            .through,
        page.through
    );
    let fresh = writer.put(1, None, "fresh").unwrap().dispatch();
    assert!(fresh > first);
    assert_eq!(
        peer.remove(1, Some(first))
            .map(RegistrationDispatch::dispatch),
        Err(MutationError::Conflict {
            current: Some(fresh)
        })
    );
}

#[test]
fn removal_and_scope_churn_retains_only_current_rows_and_monotonic_versions() {
    let collection = RegistrationCollection::new([1, 2]);
    let keeper_scope = collection.create_scope();
    let keeper = keeper_scope.writer().unwrap();
    let kept = keeper.put(0, None, "kept").unwrap().dispatch();
    let mut last = kept;
    for key in 0..512 {
        let scope = collection.create_scope();
        let writer = scope.writer().unwrap();
        let version = writer.put(key, None, "temporary").unwrap().dispatch();
        assert!(version > last);
        let removed = writer
            .remove(key, Some(version))
            .map(RegistrationDispatch::dispatch)
            .unwrap()
            .unwrap();
        assert!(removed > version);
        let fresh = writer.put(key, None, "again").unwrap().dispatch();
        assert!(fresh > removed);
        drop(scope);
        let inner = collection.lock();
        assert_eq!(inner.scopes.len(), 1);
        assert_eq!(inner.scopes[&keeper_scope.id()].rows.len(), 1);
        assert!(inner.version > fresh);
        last = inner.version;
    }
    assert_eq!(rows(&collection), [((keeper_scope.id(), 0), "kept")]);
    assert_eq!(keeper.version(&0), Some(kept));
}

#[test]
fn scope_identity_exhaustion_cannot_wrap() {
    let collection = RegistrationCollection::<u32, u32>::new([1, 2]);
    collection.lock().next_scope = u64::MAX - 1;
    let last = collection.create_scope();
    assert_eq!(last.id().get(), u64::MAX);
    drop(last);
    let error = std::panic::catch_unwind(|| collection.create_scope());
    assert!(error.is_err());
}

#[test]
fn physical_key_encoding_carries_scope_identity_to_projection() {
    let collection = RegistrationCollection::<u32, &str, String>::with_source(
        (),
        NonZeroUsize::new(4).unwrap(),
        [1, 2],
        |scope, key| format!("{}/{key}", scope.get()),
    );
    let scope = collection.create_scope();
    scope
        .writer()
        .unwrap()
        .put(5, None, "five")
        .map(RegistrationDispatch::dispatch)
        .unwrap();
    let mut lease = collection.projection().acquire((), None).unwrap();
    let page = ready(lease.read(None, ReadLimits::default())).unwrap();
    assert!(
        matches!(&page.transactions[0].changes[..], [Change::Upsert { key, value: "five", .. }] if *key == format!("{}/5", scope.id().get()))
    );
}

#[test]
fn staged_scopes_share_cas_and_commit_together_even_after_writer_drop() {
    let collection = RegistrationCollection::new([1, 2]);
    let live = collection.create_scope();
    live.writer()
        .unwrap()
        .put(9, None, "live")
        .map(RegistrationDispatch::dispatch)
        .unwrap();
    let mut lease = collection.projection().acquire((), None).unwrap();
    let bootstrap = ready(lease.read(None, ReadLimits::default())).unwrap();
    let a = collection.create_staged_scope();
    let b = collection.create_staged_scope();
    let writer = a.writer().unwrap();
    let first = writer.put(1, None, "a1").unwrap().dispatch();
    drop(writer);
    let peer = a.writer().unwrap();
    assert_eq!(
        peer.put(1, None, "blind")
            .map(RegistrationDispatch::dispatch),
        Err(MutationError::Conflict {
            current: Some(first)
        })
    );
    peer.put(1, Some(first), "a1'").unwrap().dispatch();
    peer.put(2, None, "a2").unwrap().dispatch();
    peer.remove(2, peer.version(&2)).unwrap().dispatch();
    b.writer().unwrap().put(1, None, "b1").unwrap().dispatch();
    assert_eq!(rows(&collection), [((live.id(), 9), "live")]);
    assert_eq!(
        lease
            .read(Some(bootstrap.through.clone()), ReadLimits::default())
            .complete_timeout()
            .unwrap()
            .through,
        bootstrap.through
    );
    collection.commit_scopes(&[&a, &b, &a]).dispatch();
    let page = ready(lease.read(
        Some(bootstrap.through),
        ReadLimits {
            max_transactions: Some(1),
            max_changes: Some(1),
        },
    ))
    .unwrap();
    assert_eq!(page.transactions.len(), 1);
    assert_eq!(
        page.transactions[0]
            .changes
            .iter()
            .map(|change| match change {
                Change::Upsert { key, value, .. } => (*key, *value),
                Change::Remove { .. } => panic!("commit published a removal"),
            })
            .collect::<Vec<_>>(),
        [((a.id(), 1), "a1'"), ((b.id(), 1), "b1")]
    );
    collection.commit_scopes(&[&a, &b]).dispatch();
    assert_eq!(
        lease
            .read(Some(page.through.clone()), ReadLimits::default())
            .complete_timeout()
            .unwrap()
            .through,
        page.through
    );
    peer.put(3, None, "a3").unwrap().dispatch();
    assert_eq!(
        rows(&collection),
        [
            ((live.id(), 9), "live"),
            ((a.id(), 1), "a1'"),
            ((a.id(), 3), "a3"),
            ((b.id(), 1), "b1")
        ]
    );
}

#[test]
fn staged_clear_and_close_never_publish_and_empty_commit_enables_future_writes() {
    let collection = RegistrationCollection::new([1, 2]);
    let mut lease = collection.projection().acquire((), None).unwrap();
    let bootstrap = ready(lease.read(None, ReadLimits::default())).unwrap();
    let doomed = collection.create_staged_scope();
    let writer = doomed.writer().unwrap();
    writer.put(1, None, "unseen").unwrap().dispatch();
    doomed.close().dispatch();
    collection.commit_scopes(&[&doomed]).dispatch();
    assert_eq!(
        writer.clear().map(RegistrationDispatch::dispatch),
        Err(MutationError::Closed)
    );
    let staged = collection.create_staged_scope();
    let writer = staged.writer().unwrap();
    writer.put(1, None, "cleared").unwrap().dispatch();
    writer.clear().unwrap().dispatch();
    collection.commit_scopes(&[&staged]).dispatch();
    assert!(rows(&collection).is_empty());
    assert_eq!(
        lease
            .read(Some(bootstrap.through.clone()), ReadLimits::default())
            .complete_timeout()
            .unwrap()
            .through,
        bootstrap.through
    );
    writer.put(2, None, "seen").unwrap().dispatch();
    let page = ready(lease.read(Some(bootstrap.through), ReadLimits::default())).unwrap();
    assert_eq!(page.transactions.len(), 1);
    assert!(
        matches!(&page.transactions[0].changes[..], [Change::Upsert { key, value: "seen", .. }] if *key == (staged.id(), 2))
    );
}

#[test]
fn batch_close_combines_live_scopes_and_discards_staged_scopes() {
    let collection = RegistrationCollection::new([1, 2]);
    let a = collection.create_scope();
    let b = collection.create_scope();
    let staged = collection.create_staged_scope();
    let kept = collection.create_scope();
    for scope in [&a, &b, &staged, &kept] {
        scope
            .writer()
            .unwrap()
            .put(1, None, "row")
            .map(RegistrationDispatch::dispatch)
            .unwrap();
    }
    let mut lease = collection.projection().acquire((), None).unwrap();
    let bootstrap = ready(lease.read(None, ReadLimits::default())).unwrap();
    collection.close_scopes(&[&a, &staged, &b, &a]).dispatch();
    let page = ready(lease.read(
        Some(bootstrap.through),
        ReadLimits {
            max_transactions: Some(1),
            max_changes: Some(1),
        },
    ))
    .unwrap();
    assert_eq!(page.transactions.len(), 1);
    assert_eq!(
        page.transactions[0]
            .changes
            .iter()
            .map(Change::key)
            .copied()
            .collect::<Vec<_>>(),
        [(a.id(), 1), (b.id(), 1)]
    );
    assert_eq!(rows(&collection), [((kept.id(), 1), "row")]);
    collection.commit_scopes(&[&staged]).dispatch();
    collection.close_scopes(&[&a, &b]).dispatch();
    assert_eq!(
        lease
            .read(Some(page.through.clone()), ReadLimits::default())
            .complete_timeout()
            .unwrap()
            .through,
        page.through
    );
}

#[test]
fn reset_revokes_old_scopes_and_late_cleanup_cannot_touch_new_scopes() {
    let collection = RegistrationCollection::new([1, 2]);
    let old = collection.create_scope();
    let staged = collection.create_staged_scope();
    let writer = old.writer().unwrap();
    writer.put(1, None, "old").unwrap().dispatch();
    staged
        .writer()
        .unwrap()
        .put(1, None, "unseen")
        .map(RegistrationDispatch::dispatch)
        .unwrap();
    collection.reset().dispatch();
    let fresh = collection.create_scope();
    assert_ne!(old.id(), fresh.id());
    fresh
        .writer()
        .unwrap()
        .put(1, None, "fresh")
        .map(RegistrationDispatch::dispatch)
        .unwrap();
    collection.commit_scopes(&[&staged]).dispatch();
    collection.close_scopes(&[&old, &staged]).dispatch();
    drop(old);
    assert_eq!(
        writer
            .put(1, None, "stale")
            .map(RegistrationDispatch::dispatch),
        Err(MutationError::Closed)
    );
    assert_eq!(rows(&collection), [((fresh.id(), 1), "fresh")]);
}

#[test]
fn batch_rejects_foreign_scope_before_changing_any_state() {
    let collection = RegistrationCollection::new([1, 2]);
    let other = RegistrationCollection::new([1, 2]);
    let local = collection.create_staged_scope();
    let foreign = other.create_staged_scope();
    local
        .writer()
        .unwrap()
        .put(1, None, "local")
        .map(RegistrationDispatch::dispatch)
        .unwrap();
    foreign
        .writer()
        .unwrap()
        .put(1, None, "foreign")
        .map(RegistrationDispatch::dispatch)
        .unwrap();
    assert!(
        std::panic::catch_unwind(|| collection.commit_scopes(&[&local, &foreign]).dispatch())
            .is_err()
    );
    assert!(
        std::panic::catch_unwind(|| collection.close_scopes(&[&local, &foreign]).dispatch())
            .is_err()
    );
    assert!(rows(&collection).is_empty());
    collection.commit_scopes(&[&local]).dispatch();
    other.commit_scopes(&[&foreign]).dispatch();
    assert_eq!(rows(&collection), [((local.id(), 1), "local")]);
    assert_eq!(rows(&other), [((foreign.id(), 1), "foreign")]);
}
