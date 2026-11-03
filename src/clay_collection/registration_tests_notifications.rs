use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;

use super::semantics_tests::writer_id;
use super::*;
use crate::types::Cursor;
use crate::types::ReadError;
use crate::types::ReadLimits;

type Collection = RegistrationCollection<u32, u32>;
type Writer = RegistrationWriter<u32, u32>;

/// On wake: assert that no lock is held and that the published state is already
/// visible through both the authority snapshot and a fresh bootstrap read.
struct SnapshotOnWake {
    collection: Arc<Collection>,
    expected: Vec<(u32, u32)>,
    calls: AtomicUsize,
}

impl Wake for SnapshotOnWake {
    fn wake(self: Arc<Self>) {
        assert!(
            self.collection.inner.try_lock().is_ok(),
            "registration locked during wake"
        );
        assert_eq!(
            self.collection
                .snapshot()
                .into_iter()
                .map(|(key, row)| (key, row.value))
                .collect::<Vec<_>>(),
            self.expected
        );
        let projection = self.collection.projection();
        assert!(
            crate::projection::callback_tests::unlocked(&projection),
            "projection locked during wake"
        );
        let mut lease = projection.acquire((), None).unwrap();
        let mut read = lease.read(None, ReadLimits::default());
        let Poll::Ready(Ok(page)) =
            Pin::new(&mut read).poll(&mut Context::from_waker(Waker::noop()))
        else {
            panic!("expected ready snapshot during wake");
        };
        assert!(page.bootstrap_complete);
        assert_eq!(
            page.transactions
                .into_iter()
                .flat_map(|transaction| transaction.changes)
                .map(|change| match change {
                    Change::Upsert { key, value, .. } => (key, value.value),
                    Change::Remove { .. } => panic!("remove in bootstrap"),
                })
                .collect::<Vec<_>>(),
            self.expected
        );
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}

fn poll_ready<T>(future: &mut (impl Future<Output = T> + Unpin), waker: &Waker) -> Poll<T> {
    Pin::new(future).poll(&mut Context::from_waker(waker))
}

fn bootstrap_through(
    collection: &Collection,
) -> (crate::ProjectionLease<u32, Stamped<u32>>, Cursor) {
    let mut lease = collection.projection().acquire((), None).unwrap();
    let Poll::Ready(Ok(bootstrap)) =
        poll_ready(&mut lease.read(None, ReadLimits::default()), Waker::noop())
    else {
        panic!("expected ready bootstrap");
    };
    assert!(bootstrap.bootstrap_complete);
    (lease, bootstrap.through)
}

/// A wake observer that expects the published state to equal `expected`.
fn observer(collection: &Arc<Collection>, expected: Vec<(u32, u32)>) -> Arc<SnapshotOnWake> {
    Arc::new(SnapshotOnWake {
        collection: Arc::clone(collection),
        expected,
        calls: AtomicUsize::new(0),
    })
}

fn assert_reentrant_wake(
    mutate: impl FnOnce(&RegistrationScope<u32, u32>, &Writer, Stamp),
    expected: Vec<(u32, u32)>,
    changes: Option<usize>,
) {
    let collection = Arc::new(Collection::new([1, 2]));
    let scope = collection.create_scope(writer_id("a"));
    let writer = scope.writer().unwrap();
    let stamp = writer
        .put(1, WriteBasis::Concurrent, 10)
        .unwrap()
        .dispatch();
    writer
        .put(2, WriteBasis::Concurrent, 20)
        .unwrap()
        .dispatch();
    let (mut lease, through) = bootstrap_through(&collection);
    let observer = observer(&collection, expected);
    let waker = Waker::from(Arc::clone(&observer));
    let mut pending = lease.read(Some(through), ReadLimits::default());
    assert!(poll_ready(&mut pending, &waker).is_pending());
    mutate(&scope, &writer, stamp);
    assert_eq!(
        observer.calls.load(Ordering::SeqCst),
        1,
        "wake must reenter after registration mutex is released"
    );
    let result = poll_ready(&mut pending, &waker);
    if let Some(changes) = changes {
        let Poll::Ready(Ok(page)) = result else {
            panic!("expected published transaction");
        };
        assert_eq!(page.transactions.len(), 1);
        assert_eq!(page.transactions[0].changes.len(), changes);
    } else {
        assert_eq!(result, Poll::Ready(Err(ReadError::ResetRequired)));
    }
}

#[test]
fn deferred_mutations_commit_before_dispatch() {
    for dispatch in [false, true] {
        let collection = Arc::new(Collection::new([1, 2]));
        let scope = collection.create_scope(writer_id("a"));
        let writer = scope.writer().unwrap();
        let mut current = None;
        for value in [Some(10), None] {
            let (mut lease, through) = bootstrap_through(&collection);
            let observer = observer(
                &collection,
                value.into_iter().map(|value| (1, value)).collect(),
            );
            let waker = Waker::from(Arc::clone(&observer));
            let mut pending = lease.read(Some(through), ReadLimits::default());
            assert!(poll_ready(&mut pending, &waker).is_pending());

            let basis = WriteBasis::CompareAndSet(current.take());
            let notifications = match value {
                Some(value) => writer.put(1, basis, value),
                None => writer.delete(1, basis),
            }
            .unwrap();
            let stamp = notifications.output().clone();
            assert_eq!(observer.calls.load(Ordering::SeqCst), 0);
            assert_eq!(
                collection
                    .snapshot()
                    .into_iter()
                    .map(|(key, row)| (key, row.value))
                    .collect::<Vec<_>>(),
                observer.expected
            );
            let Poll::Ready(Ok(page)) = poll_ready(&mut pending, &waker) else {
                panic!("expected committed mutation before dispatch");
            };
            let [transaction] = &page.transactions[..] else {
                panic!("expected one transaction");
            };
            match (value, &transaction.changes[..]) {
                (
                    Some(value),
                    [
                        Change::Upsert {
                            key: 1, value: row, ..
                        },
                    ],
                ) => {
                    assert_eq!(
                        row,
                        &Stamped {
                            stamp: stamp.clone(),
                            value
                        }
                    );
                }
                (None, [Change::Remove { key: 1, .. }]) => {}
                other => panic!("unexpected publication {other:?}"),
            }
            assert_eq!(observer.calls.load(Ordering::SeqCst), 0);
            if dispatch {
                notifications.dispatch();
            } else {
                drop(notifications);
            }
            assert_eq!(observer.calls.load(Ordering::SeqCst), usize::from(dispatch));
            current = value.map(|_| stamp);
        }
    }
}

#[test]
fn writes_that_do_not_change_the_winner_and_conflicts_publish_and_wake_nothing() {
    let collection = Arc::new(Collection::new([1, 2]));
    let high_scope = collection.create_scope(writer_id("b"));
    let low_scope = collection.create_scope(writer_id("a"));
    let (high, low) = (high_scope.writer().unwrap(), low_scope.writer().unwrap());
    let winner = high.put(1, WriteBasis::Concurrent, 10).unwrap().dispatch();
    let (mut lease, through) = bootstrap_through(&collection);
    let observer = observer(&collection, vec![(1, 10)]);
    let waker = Waker::from(Arc::clone(&observer));
    let mut pending = lease.read(Some(through), ReadLimits::default());
    assert!(poll_ready(&mut pending, &waker).is_pending());

    // Loses the tie to the higher writer id; a lone tombstone on an absent key is no row.
    low.put(1, WriteBasis::Concurrent, 99).unwrap().dispatch();
    low.delete(2, WriteBasis::Concurrent).unwrap().dispatch();
    assert_eq!(
        low.put(1, WriteBasis::CompareAndSet(None), 5)
            .map(RegistrationDispatch::dispatch),
        Err(MutationError::Conflict {
            current: Some(winner.clone())
        })
    );
    assert_eq!(collection.get(&1).unwrap().stamp, winner);
    assert_eq!(observer.calls.load(Ordering::SeqCst), 0);
    assert!(poll_ready(&mut pending, &waker).is_pending());
}

#[test]
fn put_wakes_after_unlocking_registration() {
    assert_reentrant_wake(
        |_, writer, stamp| {
            writer
                .put(1, WriteBasis::Observed(stamp), 11)
                .unwrap()
                .dispatch();
        },
        vec![(1, 11), (2, 20)],
        Some(1),
    );
}

#[test]
fn delete_wakes_after_unlocking_registration() {
    assert_reentrant_wake(
        |_, writer, stamp| {
            writer
                .delete(1, WriteBasis::Observed(stamp))
                .unwrap()
                .dispatch();
        },
        vec![(2, 20)],
        Some(1),
    );
}

#[test]
fn scope_close_wakes_after_unlocking_registration() {
    assert_reentrant_wake(|scope, _, _| scope.close().dispatch(), Vec::new(), Some(2));
}

#[test]
fn reset_wakes_after_unlocking_registration() {
    assert_reentrant_wake(
        |scope, _, _| scope.collection.reset().dispatch(),
        Vec::new(),
        None,
    );
}

struct MutateOnWake {
    collection: Arc<Collection>,
    writer: Mutex<Writer>,
    calls: AtomicUsize,
}

impl Wake for MutateOnWake {
    fn wake(self: Arc<Self>) {
        assert!(
            self.collection.inner.try_lock().is_ok(),
            "registration locked during wake"
        );
        let projection = self.collection.projection();
        assert!(
            crate::projection::callback_tests::unlocked(&projection),
            "projection locked during wake"
        );
        let (mut lease, through) = bootstrap_through(&self.collection);
        let observer = observer(&self.collection, vec![(1, 11), (2, 20)]);
        let waker = Waker::from(Arc::clone(&observer));
        let mut pending = lease.read(Some(through), ReadLimits::default());
        assert!(poll_ready(&mut pending, &waker).is_pending());
        self.writer
            .lock()
            .unwrap()
            .put(2, WriteBasis::Concurrent, 20)
            .unwrap()
            .dispatch();
        assert_eq!(observer.calls.load(Ordering::SeqCst), 1);
        let Poll::Ready(Ok(page)) = poll_ready(&mut pending, &waker) else {
            panic!("expected reentrant mutation to notify parked reader");
        };
        assert!(matches!(
            &page.transactions[..],
            [crate::Transaction { changes }] if matches!(&changes[..], [Change::Upsert { key: 2, .. }])
        ));
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn notification_reentrant_mutation_preserves_publication_order_and_wakes() {
    let collection = Arc::new(Collection::new([1, 2]));
    let scope = collection.create_scope(writer_id("a"));
    let writer = scope.writer().unwrap();
    let stamp = writer
        .put(1, WriteBasis::Concurrent, 10)
        .unwrap()
        .dispatch();
    let observer = Arc::new(MutateOnWake {
        collection: Arc::clone(&collection),
        writer: Mutex::new(scope.writer().unwrap()),
        calls: AtomicUsize::new(0),
    });
    let (mut lease, through) = bootstrap_through(&collection);
    let waker = Waker::from(Arc::clone(&observer));
    let mut pending = lease.read(Some(through), ReadLimits::default());
    assert!(poll_ready(&mut pending, &waker).is_pending());
    writer
        .put(1, WriteBasis::Observed(stamp), 11)
        .unwrap()
        .dispatch();
    assert_eq!(observer.calls.load(Ordering::SeqCst), 1);
    let Poll::Ready(Ok(page)) = poll_ready(&mut pending, Waker::noop()) else {
        panic!("expected both ordered mutations");
    };
    assert_eq!(
        page.transactions
            .into_iter()
            .map(|transaction| transaction
                .changes
                .into_iter()
                .map(|change| match change {
                    Change::Upsert { key, value, .. } => (key, value.value),
                    Change::Remove { .. } => panic!("unexpected removal"),
                })
                .collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        vec![vec![(1, 11)], vec![(2, 20)]]
    );
}

#[test]
fn deferred_batch_commit_and_close_wake_only_after_dispatch() {
    let collection = Arc::new(Collection::new([1, 2]));
    let a = collection.create_staged_scope(writer_id("a"));
    let b = collection.create_staged_scope(writer_id("b"));
    a.writer()
        .unwrap()
        .put(1, WriteBasis::Concurrent, 10)
        .unwrap()
        .dispatch();
    b.writer()
        .unwrap()
        .put(2, WriteBasis::Concurrent, 20)
        .unwrap()
        .dispatch();
    for commit in [true, false] {
        let (mut lease, through) = bootstrap_through(&collection);
        let observer = observer(
            &collection,
            if commit {
                vec![(1, 10), (2, 20)]
            } else {
                Vec::new()
            },
        );
        let waker = Waker::from(Arc::clone(&observer));
        let mut pending = lease.read(
            Some(through),
            ReadLimits {
                max_transactions: Some(1),
                max_changes: Some(1),
            },
        );
        assert!(poll_ready(&mut pending, &waker).is_pending());
        let dispatch = if commit {
            collection.commit_scopes(&[&a, &b])
        } else {
            collection.close_scopes(&[&a, &b])
        };
        assert_eq!(observer.calls.load(Ordering::SeqCst), 0);
        let Poll::Ready(Ok(page)) = poll_ready(&mut pending, Waker::noop()) else {
            panic!("expected committed transaction")
        };
        assert_eq!(page.transactions.len(), 1);
        assert_eq!(page.transactions[0].changes.len(), 2);
        dispatch.dispatch();
        assert_eq!(observer.calls.load(Ordering::SeqCst), 1);
    }
}

#[derive(Clone)]
struct DropProbe(Arc<dyn Fn() + Send + Sync>);

impl Drop for DropProbe {
    fn drop(&mut self) {
        (self.0)();
    }
}

#[test]
fn retired_values_reenter_after_authority_projection_and_host_unlock() {
    for staged in [false, true] {
        for operation in 0..4 {
            let collection = RegistrationCollection::<u32, DropProbe>::new([1, 2]);
            let host_lock = Arc::new(Mutex::new(()));
            let events = Arc::new(Mutex::new(Vec::new()));
            let probe = DropProbe(Arc::new({
                let inner = Arc::downgrade(&collection.inner);
                let host_lock = Arc::clone(&host_lock);
                let events = Arc::clone(&events);
                move || {
                    let Some(inner) = inner.upgrade() else { return };
                    let collection = RegistrationCollection { inner };
                    let authority_unlocked = collection.inner.try_lock().is_ok();
                    let host_unlocked = host_lock.try_lock().is_ok();
                    let projection_unlocked = authority_unlocked
                        && crate::projection::callback_tests::unlocked(&collection.projection());
                    events.lock().unwrap().push((
                        authority_unlocked,
                        projection_unlocked,
                        host_unlocked,
                    ));
                    if authority_unlocked && projection_unlocked && host_unlocked {
                        let scope = collection.create_scope(writer_id("reentrant"));
                        scope.close().dispatch();
                    }
                }
            }));
            let scope = if staged {
                collection.create_staged_scope(writer_id("a"))
            } else {
                collection.create_scope(writer_id("a"))
            };
            let writer = scope.writer().unwrap();
            writer
                .put(1, WriteBasis::Concurrent, probe.clone())
                .unwrap()
                .dispatch();

            let guard = host_lock.lock().unwrap();
            let before = events.lock().unwrap().len();
            let deferred = match operation {
                0 => {
                    let dispatch = writer
                        .put(1, WriteBasis::Concurrent, probe.clone())
                        .unwrap();
                    Box::new(move || {
                        dispatch.dispatch();
                    }) as Box<dyn FnOnce()>
                }
                1 => {
                    let dispatch = writer.delete(1, WriteBasis::Concurrent).unwrap();
                    Box::new(move || {
                        dispatch.dispatch();
                    })
                }
                2 => {
                    let dispatch = scope.close();
                    Box::new(move || dispatch.dispatch())
                }
                3 => {
                    let dispatch = collection.reset();
                    Box::new(move || dispatch.dispatch())
                }
                _ => unreachable!(),
            };
            assert_eq!(
                events.lock().unwrap().len(),
                before,
                "destructor ran before dispatch: {operation}"
            );
            drop(guard);
            deferred();
            // Every operation retires the probe the scope held.
            assert!(
                events.lock().unwrap().len() > before,
                "retired value not dropped by dispatch: {operation}"
            );
            drop(scope);
            drop(writer);
            drop(probe);
            assert!(
                events
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|event| *event == (true, true, true))
            );
        }
    }
}

#[test]
fn scope_drop_and_projection_bootstrap_retirement_run_destructors_unlocked() {
    let collection = RegistrationCollection::<u32, DropProbe>::new([1, 2]);
    let scope = collection.create_scope(writer_id("a"));
    let events = Arc::new(Mutex::new(Vec::new()));
    let value = DropProbe(Arc::new({
        let inner = Arc::downgrade(&collection.inner);
        let events = Arc::clone(&events);
        move || {
            if let Some(inner) = inner.upgrade() {
                let collection = RegistrationCollection { inner };
                let authority_unlocked = collection.inner.try_lock().is_ok();
                let projection_unlocked = authority_unlocked
                    && crate::projection::callback_tests::unlocked(&collection.projection());
                events
                    .lock()
                    .unwrap()
                    .push((authority_unlocked, projection_unlocked));
            }
        }
    }));
    let writer = scope.writer().unwrap();
    writer
        .put(1, WriteBasis::Concurrent, value)
        .unwrap()
        .dispatch();
    let mut lease = collection.projection().acquire((), None).unwrap();
    let Poll::Ready(Ok(page)) =
        poll_ready(&mut lease.read(None, ReadLimits::default()), Waker::noop())
    else {
        panic!("expected bootstrap")
    };
    lease
        .read(Some(page.through.clone()), ReadLimits::default())
        .complete_timeout()
        .unwrap();
    drop(page);
    drop(scope);
    collection.reset().dispatch();
    assert!(!events.lock().unwrap().is_empty());
    assert!(
        events
            .lock()
            .unwrap()
            .iter()
            .all(|event| *event == (true, true))
    );
}
