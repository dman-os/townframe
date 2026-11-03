use std::future::Future;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use super::*;
use crate::projection::ProjectionLease;
use crate::types::Cursor;
use crate::types::ReadLimits;

type Collection = RegistrationCollection<u32, &'static str>;

pub(super) fn ready<T>(future: impl Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    let Poll::Ready(value) = future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("expected a ready read");
    };
    value
}

pub(super) fn writer_id(name: &str) -> WriterId {
    WriterId::new(name)
}

/// One reader's view of a collection's feed: a lease plus its committed position.
pub(super) struct Feed<K, V> {
    lease: ProjectionLease<K, Stamped<V>>,
    through: Cursor,
}

impl<K: Ord + Clone, V: Clone> Feed<K, V> {
    /// Acquire a lease and consume its bootstrap page, returning its changes.
    pub(super) fn open(
        collection: &RegistrationCollection<K, V>,
    ) -> (Self, Vec<Change<K, Stamped<V>>>) {
        let mut lease = collection.projection().acquire((), None).unwrap();
        let page = ready(lease.read(None, ReadLimits::default())).unwrap();
        assert!(page.bootstrap_complete);
        let changes = page
            .transactions
            .into_iter()
            .flat_map(|transaction| transaction.changes)
            .collect();
        (
            Self {
                lease,
                through: page.through,
            },
            changes,
        )
    }

    /// The transactions published since the last call, one `Vec` of changes each.
    pub(super) fn transactions(&mut self) -> Vec<Vec<Change<K, Stamped<V>>>> {
        let page = ready(
            self.lease
                .read(Some(self.through.clone()), ReadLimits::default()),
        )
        .unwrap();
        self.through = page.through;
        page.transactions
            .into_iter()
            .map(|transaction| transaction.changes)
            .collect()
    }

    pub(super) fn assert_quiet(&mut self) {
        let page = self
            .lease
            .read(Some(self.through.clone()), ReadLimits::default())
            .complete_timeout()
            .unwrap();
        assert_eq!(page.through, self.through, "unexpected publication");
    }
}

/// The reader-visible winners as `(key, value)`.
fn values(collection: &Collection) -> Vec<(u32, &'static str)> {
    collection
        .snapshot()
        .into_iter()
        .map(|(key, row)| (key, row.value))
        .collect()
}

fn stamp(lamport: u64, writer: &str) -> Stamp {
    Stamp {
        lamport,
        writer: writer_id(writer),
    }
}

fn put(
    writer: &RegistrationWriter<u32, &'static str>,
    key: u32,
    basis: WriteBasis,
    value: &'static str,
) -> Stamp {
    writer.put(key, basis, value).unwrap().dispatch()
}

#[test]
fn concurrent_creation_ties_break_by_writer_id_in_any_arrival_order() {
    for low_first in [true, false] {
        let collection = Collection::new([1, 2]);
        let low_scope = collection.create_scope(writer_id("a"));
        let high_scope = collection.create_scope(writer_id("b"));
        let (low, high) = (low_scope.writer().unwrap(), high_scope.writer().unwrap());
        let (mut feed, _) = Feed::open(&collection);
        let (first, second) = if low_first {
            (&low, &high)
        } else {
            (&high, &low)
        };
        let first_stamp = put(first, 7, WriteBasis::Concurrent, "first");
        let second_stamp = put(second, 7, WriteBasis::Concurrent, "second");
        assert_eq!((first_stamp.lamport, second_stamp.lamport), (1, 1));
        assert_eq!(
            collection.get(&7),
            Some(Stamped {
                stamp: stamp(1, "b"),
                value: if low_first { "second" } else { "first" },
            })
        );
        // Low then high publishes twice; high then low leaves the winner untouched by
        // the second write, so publishes once.
        let published = feed.transactions();
        assert_eq!(published.len(), if low_first { 2 } else { 1 });
    }
}

#[test]
fn observed_write_supersedes_regardless_of_writer_order() {
    let collection = Collection::new([1, 2]);
    let high_scope = collection.create_scope(writer_id("z"));
    let low_scope = collection.create_scope(writer_id("a"));
    let seen = put(
        &high_scope.writer().unwrap(),
        1,
        WriteBasis::Concurrent,
        "z1",
    );
    let low = low_scope.writer().unwrap();
    let superseding = put(&low, 1, WriteBasis::Observed(seen), "a2");
    assert_eq!(superseding, stamp(2, "a"));
    assert_eq!(values(&collection), [(1, "a2")]);
}

#[test]
fn stale_observed_write_loses_to_a_higher_stamp() {
    let collection = Collection::new([1, 2]);
    let (z, a, m) = (
        collection.create_scope(writer_id("z")),
        collection.create_scope(writer_id("a")),
        collection.create_scope(writer_id("m")),
    );
    let (z, a, m) = (
        z.writer().unwrap(),
        a.writer().unwrap(),
        m.writer().unwrap(),
    );
    let base = put(&z, 1, WriteBasis::Concurrent, "base");
    let second = put(&a, 1, WriteBasis::Observed(base.clone()), "a2");
    let third = put(&a, 1, WriteBasis::Observed(second), "a3");
    assert_eq!(third.lamport, 3);
    let (mut feed, _) = Feed::open(&collection);
    // `m` still believes `base` is current: lamport 2 beats nothing.
    let stale = put(&m, 1, WriteBasis::Observed(base), "stale");
    assert_eq!(stale, stamp(2, "m"));
    assert_eq!(values(&collection), [(1, "a3")]);
    feed.assert_quiet();
}

#[test]
fn a_scope_replaces_its_own_candidate_and_its_stamps_never_repeat() {
    let collection = Collection::new([1, 2]);
    let scope = collection.create_scope(writer_id("a"));
    let writer = scope.writer().unwrap();
    let first = put(&writer, 1, WriteBasis::Concurrent, "one");
    let second = put(&writer, 1, WriteBasis::Concurrent, "two");
    let third = put(&writer, 1, WriteBasis::Observed(first.clone()), "three");
    assert!(first < second && second < third);
    assert_eq!(collection.lock().candidates[&1].len(), 1);
    assert_eq!(values(&collection), [(1, "three")]);
}

#[test]
fn compare_and_set_succeeds_only_against_the_exact_winner() {
    let collection = Collection::new([1, 2]);
    let (a_scope, b_scope) = (
        collection.create_scope(writer_id("a")),
        collection.create_scope(writer_id("b")),
    );
    let (a, b) = (a_scope.writer().unwrap(), b_scope.writer().unwrap());
    let conflict = |result: Result<RegistrationDispatch<u32, &'static str, (), Stamp>, _>| {
        result.map(RegistrationDispatch::dispatch)
    };

    // Absent: only a `None` expectation passes.
    assert_eq!(
        conflict(a.put(1, WriteBasis::CompareAndSet(Some(stamp(1, "a"))), "x")),
        Err(MutationError::Conflict { current: None })
    );
    let created = put(&a, 1, WriteBasis::CompareAndSet(None), "created");
    assert_eq!(created, stamp(1, "a"));

    // Present: `None` and stale stamps conflict, reporting the actual winner.
    for wrong in [None, Some(stamp(1, "b")), Some(stamp(2, "a"))] {
        assert_eq!(
            conflict(b.put(1, WriteBasis::CompareAndSet(wrong), "lost")),
            Err(MutationError::Conflict {
                current: Some(created.clone())
            })
        );
    }
    let swapped = put(
        &b,
        1,
        WriteBasis::CompareAndSet(Some(created.clone())),
        "b2",
    );
    assert_eq!(swapped, stamp(2, "b"));
    assert_eq!(
        conflict(a.delete(1, WriteBasis::CompareAndSet(Some(created)))),
        Err(MutationError::Conflict {
            current: Some(swapped.clone())
        })
    );

    // A winning tombstone reads as absent and is superseded, not merely tied.
    let tombstone = a
        .delete(1, WriteBasis::CompareAndSet(Some(swapped)))
        .unwrap()
        .dispatch();
    assert_eq!(collection.get(&1), None);
    let recreated = put(&b, 1, WriteBasis::CompareAndSet(None), "again");
    assert!(recreated > tombstone);
    assert_eq!(values(&collection), [(1, "again")]);
}

#[test]
fn tombstone_wins_until_its_scope_closes_then_the_older_value_resurfaces() {
    let collection = Collection::new([1, 2]);
    let (owner, deleter) = (
        collection.create_scope(writer_id("a")),
        collection.create_scope(writer_id("b")),
    );
    let (owner_writer, deleter_writer) = (owner.writer().unwrap(), deleter.writer().unwrap());
    let original = put(&owner_writer, 1, WriteBasis::Concurrent, "kept");
    let (mut feed, bootstrap) = Feed::open(&collection);
    assert!(matches!(&bootstrap[..], [Change::Upsert { value, .. }] if value.stamp == original));

    deleter_writer
        .delete(1, WriteBasis::Observed(original.clone()))
        .unwrap()
        .dispatch();
    assert_eq!(collection.get(&1), None);
    assert!(
        matches!(&feed.transactions()[..], [tx] if matches!(&tx[..], [Change::Remove { key: 1, .. }]))
    );

    deleter.close().dispatch();
    let published = feed.transactions();
    let [tx] = &published[..] else {
        panic!("expected one transaction")
    };
    let [
        Change::Upsert {
            value, row_version, ..
        },
    ] = &tx[..]
    else {
        panic!("expected resurfacing upsert")
    };
    // The winner is older than the tombstone it outlived, yet the feed position is new.
    assert_eq!(value.stamp, original);
    assert_eq!(value.value, "kept");
    assert!(*row_version > bootstrap[0].row_version());
}

#[test]
fn closing_a_scope_recomputes_only_the_keys_it_held() {
    let collection = Collection::new([1, 2]);
    let (stable, closing) = (
        collection.create_scope(writer_id("a")),
        collection.create_scope(writer_id("b")),
    );
    let (stable_writer, closing_writer) = (stable.writer().unwrap(), closing.writer().unwrap());
    // Key 1: `stable` outranks `closing`. Key 2: only `stable`. Key 3: only `closing`.
    let low = put(&closing_writer, 1, WriteBasis::Concurrent, "loser");
    put(&stable_writer, 1, WriteBasis::Observed(low), "winner");
    put(&stable_writer, 2, WriteBasis::Concurrent, "stable");
    put(&closing_writer, 3, WriteBasis::Concurrent, "gone");
    let (mut feed, _) = Feed::open(&collection);

    closing.close().dispatch();
    let published = feed.transactions();
    let [tx] = &published[..] else {
        panic!("expected one transaction")
    };
    assert!(matches!(&tx[..], [Change::Remove { key: 3, .. }]));
    assert_eq!(values(&collection), [(1, "winner"), (2, "stable")]);
    assert!(
        collection
            .lock()
            .candidates
            .values()
            .all(|holders| holders.len() == 1),
        "retraction must leave no candidate of the closed scope"
    );
}

#[test]
fn readers_get_the_stamp_to_pass_back_as_their_basis() {
    let collection = Collection::new([1, 2]);
    let (a, b) = (
        collection.create_scope(writer_id("b")),
        collection.create_scope(writer_id("a")),
    );
    let (a, b) = (a.writer().unwrap(), b.writer().unwrap());
    put(&a, 1, WriteBasis::Concurrent, "first");
    let (mut feed, bootstrap) = Feed::open(&collection);
    let Change::Upsert { value: seen, .. } = &bootstrap[0] else {
        panic!("expected upsert")
    };
    put(&b, 1, WriteBasis::Observed(seen.stamp.clone()), "second");
    let published = feed.transactions();
    let [tx] = &published[..] else { panic!() };
    let [Change::Upsert { value, .. }] = &tx[..] else {
        panic!()
    };
    assert_eq!(value.stamp, stamp(2, "a"));
    assert!(value.stamp > seen.stamp);
}

#[test]
fn writer_drop_preserves_candidates_and_owner_close_invalidates_every_writer() {
    let collection = Collection::new([1, 2]);
    let scope = collection.create_scope(writer_id("a"));
    let writer = scope.writer().unwrap();
    let peer = scope.writer().unwrap();
    let first = put(&writer, 1, WriteBasis::Concurrent, "one");
    drop(writer);
    assert_eq!(values(&collection), [(1, "one")]);
    // Writers of one scope share its candidate: the peer's write replaces it.
    let second = put(&peer, 1, WriteBasis::CompareAndSet(Some(first)), "two");
    assert_eq!(second.lamport, 2);

    let (mut feed, _) = Feed::open(&collection);
    scope.close().dispatch();
    assert!(
        matches!(&feed.transactions()[..], [tx] if matches!(&tx[..], [Change::Remove { key: 1, .. }]))
    );
    assert!(values(&collection).is_empty());
    assert_eq!(
        peer.put(1, WriteBasis::Concurrent, "late")
            .map(RegistrationDispatch::dispatch),
        Err(MutationError::Closed)
    );
    assert_eq!(
        peer.delete(1, WriteBasis::Concurrent)
            .map(RegistrationDispatch::dispatch),
        Err(MutationError::Closed)
    );
    assert_eq!(scope.writer().unwrap_err(), MutationError::Closed);
    scope.close().dispatch();
    drop(scope);
    feed.assert_quiet();
}

#[test]
fn owner_drop_closes_scope_even_while_writer_survives() {
    let collection = Collection::new([1, 2]);
    let scope = collection.create_scope(writer_id("a"));
    let writer = scope.writer().unwrap();
    put(&writer, 1, WriteBasis::Concurrent, "owned");
    drop(scope);
    assert!(values(&collection).is_empty());
    assert!(collection.lock().candidates.is_empty());
    assert_eq!(
        writer
            .put(2, WriteBasis::Concurrent, "late")
            .map(RegistrationDispatch::dispatch),
        Err(MutationError::Closed)
    );
}

#[test]
fn concurrent_compare_and_set_from_many_scopes_admits_exactly_one() {
    let collection = Collection::new([1, 2]);
    let seed_scope = collection.create_scope(writer_id("seed"));
    let first = put(
        &seed_scope.writer().unwrap(),
        1,
        WriteBasis::Concurrent,
        "seed",
    );
    let scopes: Vec<_> = (1..=8)
        .map(|index| collection.create_scope(writer_id(&format!("w{index}"))))
        .collect();
    let barrier = std::sync::Barrier::new(scopes.len());
    let outcomes = std::thread::scope(|threads| {
        let joins: Vec<_> = scopes
            .iter()
            .map(|scope| {
                let writer = scope.writer().unwrap();
                let (barrier, first) = (&barrier, &first);
                threads.spawn(move || {
                    barrier.wait();
                    writer
                        .put(
                            1,
                            WriteBasis::CompareAndSet(Some(first.clone())),
                            "contender",
                        )
                        .map(RegistrationDispatch::dispatch)
                })
            })
            .collect();
        joins
            .into_iter()
            .map(|join| join.join().unwrap())
            .collect::<Vec<_>>()
    });
    let winners: Vec<_> = outcomes
        .iter()
        .filter_map(|outcome| outcome.as_ref().ok())
        .collect();
    assert_eq!(winners.len(), 1);
    assert_eq!(winners[0].lamport, 2);
    for outcome in &outcomes {
        if let Err(error) = outcome {
            assert_eq!(
                error,
                &MutationError::Conflict {
                    current: Some(winners[0].clone())
                }
            );
        }
    }
    assert_eq!(collection.get(&1).unwrap().stamp, *winners[0]);
}

#[test]
fn staged_scopes_are_invisible_until_commit_and_commit_publishes_one_transaction() {
    let collection = Collection::new([1, 2]);
    let live = collection.create_scope(writer_id("live"));
    let base = put(&live.writer().unwrap(), 9, WriteBasis::Concurrent, "live");
    let (mut feed, _) = Feed::open(&collection);

    let (a, b) = (
        collection.create_staged_scope(writer_id("a")),
        collection.create_staged_scope(writer_id("b")),
    );
    let writer = a.writer().unwrap();
    let first = put(&writer, 1, WriteBasis::Concurrent, "a1");
    // The scope compares against its own staged candidate; replacing it publishes nothing.
    put(&writer, 1, WriteBasis::CompareAndSet(Some(first)), "a1'");
    writer
        .put(2, WriteBasis::Concurrent, "a2")
        .unwrap()
        .dispatch();
    writer.delete(2, WriteBasis::Concurrent).unwrap().dispatch();
    // A staged tombstone cannot hide a published value from anyone before commit.
    writer
        .delete(9, WriteBasis::Observed(base))
        .unwrap()
        .dispatch();
    put(&b.writer().unwrap(), 1, WriteBasis::Concurrent, "b1");
    assert_eq!(values(&collection), [(9, "live")]);
    feed.assert_quiet();

    // Another scope cannot see staged candidates when comparing.
    let outsider = collection.create_scope(writer_id("c"));
    assert!(
        outsider
            .writer()
            .unwrap()
            .put(1, WriteBasis::CompareAndSet(None), "c1")
            .is_ok()
    );
    drop(outsider);
    feed.transactions();

    collection.commit_scopes(&[&a, &b, &a]).dispatch();
    let published = feed.transactions();
    let [tx] = &published[..] else {
        panic!("expected one transaction")
    };
    assert_eq!(
        tx.iter()
            .map(|change| match change {
                Change::Upsert { key, value, .. } => (*key, Some(value.value)),
                Change::Remove { key, .. } => (*key, None),
            })
            .collect::<Vec<_>>(),
        [(1, Some("a1'")), (9, None)]
    );
    // Committing again is a no-op; the scope stays live and publishes directly.
    collection.commit_scopes(&[&a, &b]).dispatch();
    feed.assert_quiet();
    put(&writer, 3, WriteBasis::Concurrent, "a3");
    assert_eq!(values(&collection), [(1, "a1'"), (3, "a3")]);
}

#[test]
fn closing_a_staged_scope_discards_it_unseen() {
    let collection = Collection::new([1, 2]);
    let (mut feed, _) = Feed::open(&collection);
    let doomed = collection.create_staged_scope(writer_id("a"));
    let writer = doomed.writer().unwrap();
    put(&writer, 1, WriteBasis::Concurrent, "unseen");
    doomed.close().dispatch();
    collection.commit_scopes(&[&doomed]).dispatch();
    assert_eq!(
        writer
            .put(2, WriteBasis::Concurrent, "late")
            .map(RegistrationDispatch::dispatch),
        Err(MutationError::Closed)
    );
    assert!(collection.lock().candidates.is_empty());
    feed.assert_quiet();
}

#[test]
fn batch_close_combines_live_scopes_and_discards_staged_scopes() {
    let collection = Collection::new([1, 2]);
    let a = collection.create_scope(writer_id("a"));
    let b = collection.create_scope(writer_id("b"));
    let staged = collection.create_staged_scope(writer_id("s"));
    let kept = collection.create_scope(writer_id("k"));
    for scope in [&a, &b, &staged, &kept] {
        put(&scope.writer().unwrap(), 1, WriteBasis::Concurrent, "row");
    }
    let (mut feed, _) = Feed::open(&collection);
    // Among live scopes the highest writer id ("k") wins; the staged "s" is invisible.
    assert_eq!(collection.get(&1).unwrap().stamp.writer, writer_id("k"));
    collection.close_scopes(&[&a, &staged, &b, &a]).dispatch();
    feed.assert_quiet();
    collection.close_scopes(&[&kept]).dispatch();
    assert!(
        matches!(&feed.transactions()[..], [tx] if matches!(&tx[..], [Change::Remove { key: 1, .. }]))
    );
    collection.commit_scopes(&[&staged]).dispatch();
    feed.assert_quiet();
}

#[test]
fn reset_revokes_old_scopes_and_late_cleanup_cannot_touch_new_scopes() {
    let collection = Collection::new([1, 2]);
    let old = collection.create_scope(writer_id("a"));
    let staged = collection.create_staged_scope(writer_id("s"));
    let writer = old.writer().unwrap();
    put(&writer, 1, WriteBasis::Concurrent, "old");
    put(
        &staged.writer().unwrap(),
        1,
        WriteBasis::Concurrent,
        "unseen",
    );
    collection.reset().dispatch();
    let fresh = collection.create_scope(writer_id("f"));
    assert_ne!(old.id(), fresh.id());
    put(&fresh.writer().unwrap(), 1, WriteBasis::Concurrent, "fresh");
    collection.commit_scopes(&[&staged]).dispatch();
    collection.close_scopes(&[&old, &staged]).dispatch();
    drop(old);
    assert_eq!(
        writer
            .put(1, WriteBasis::Concurrent, "stale")
            .map(RegistrationDispatch::dispatch),
        Err(MutationError::Closed)
    );
    assert_eq!(values(&collection), [(1, "fresh")]);
}

#[test]
fn scope_churn_retains_only_live_candidates() {
    let collection = Collection::new([1, 2]);
    let keeper_scope = collection.create_scope(writer_id("keeper"));
    let keeper = keeper_scope.writer().unwrap();
    let kept = put(&keeper, 0, WriteBasis::Concurrent, "kept");
    for key in 0..512 {
        let scope = collection.create_scope(writer_id("churn"));
        let writer = scope.writer().unwrap();
        writer
            .put(key, WriteBasis::Concurrent, "temporary")
            .unwrap()
            .dispatch();
        writer
            .delete(key, WriteBasis::Concurrent)
            .unwrap()
            .dispatch();
        drop(scope);
        let inner = collection.lock();
        assert_eq!(inner.scopes.len(), 1);
        assert_eq!(inner.candidates.len(), 1);
    }
    assert_eq!(collection.get(&0).unwrap().stamp, kept);
}

#[test]
fn batch_rejects_foreign_scope_before_changing_any_state() {
    let collection = Collection::new([1, 2]);
    let other = Collection::new([1, 2]);
    let local = collection.create_staged_scope(writer_id("a"));
    let foreign = other.create_staged_scope(writer_id("b"));
    put(&local.writer().unwrap(), 1, WriteBasis::Concurrent, "local");
    put(
        &foreign.writer().unwrap(),
        1,
        WriteBasis::Concurrent,
        "foreign",
    );
    assert!(
        std::panic::catch_unwind(|| collection.commit_scopes(&[&local, &foreign]).dispatch())
            .is_err()
    );
    assert!(
        std::panic::catch_unwind(|| collection.close_scopes(&[&local, &foreign]).dispatch())
            .is_err()
    );
    assert!(values(&collection).is_empty());
    collection.commit_scopes(&[&local]).dispatch();
    other.commit_scopes(&[&foreign]).dispatch();
    assert_eq!(values(&collection), [(1, "local")]);
    assert_eq!(values(&other), [(1, "foreign")]);
}

#[test]
fn scope_identity_exhaustion_cannot_wrap() {
    let collection = RegistrationCollection::<u32, u32>::new([1, 2]);
    collection.lock().next_scope = u64::MAX - 1;
    let last = collection.create_scope(writer_id("a"));
    assert_eq!(last.id().get(), u64::MAX);
    drop(last);
    let error = std::panic::catch_unwind(|| collection.create_scope(writer_id("a")));
    assert!(error.is_err());
}
