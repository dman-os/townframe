use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;

use super::*;
use crate::types::ReadError;
use crate::types::ReadLimits;

struct SnapshotOnWake {
    collection: Arc<RegistrationCollection<u32, u32>>,
    expected: Vec<(u32, u32)>,
    calls: AtomicUsize,
}

impl Wake for SnapshotOnWake {
    fn wake(self: Arc<Self>) {
        if self.collection.inner.try_lock().is_err() {
            return;
        }
        let rows = self.collection.snapshot();
        assert_eq!(
            rows.iter()
                .map(|((_, key), entry)| (*key, entry.value))
                .collect::<Vec<_>>(),
            self.expected
        );
        let projection = self.collection.projection();
        if !crate::projection::callback_tests::unlocked(&projection) {
            return;
        }
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
                    Change::Upsert {
                        key: (_, key),
                        value,
                        ..
                    } => (key, value),
                    Change::Remove { .. } => panic!("remove in bootstrap"),
                })
                .collect::<Vec<_>>(),
            self.expected
        );
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}

fn assert_reentrant_wake(
    mutate: impl FnOnce(
        &RegistrationCollection<u32, u32>,
        &mut Option<RegistrationWriter<u32, u32>>,
        Version,
    ),
    expected: Vec<(u32, u32)>,
    changes: Option<usize>,
) {
    let collection = Arc::new(RegistrationCollection::new([1, 2]));
    let mut writer = collection.writer();
    let version = writer.put(1, None, 10).unwrap();
    writer.put(2, None, 20).unwrap();
    let mut writer = Some(writer);
    let mut lease = collection.projection().acquire((), None).unwrap();
    let Poll::Ready(Ok(bootstrap)) = Pin::new(&mut lease.read(None, ReadLimits::default()))
        .poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("expected ready bootstrap");
    };
    assert!(bootstrap.bootstrap_complete);
    let observer = Arc::new(SnapshotOnWake {
        collection: Arc::clone(&collection),
        expected,
        calls: AtomicUsize::new(0),
    });
    let waker = Waker::from(Arc::clone(&observer));
    let mut pending = lease.read(Some(bootstrap.through), ReadLimits::default());
    assert!(
        Pin::new(&mut pending)
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    mutate(&collection, &mut writer, version);
    assert_eq!(
        observer.calls.load(Ordering::SeqCst),
        1,
        "wake must reenter after registration mutex is released"
    );
    let result = Pin::new(&mut pending).poll(&mut Context::from_waker(&waker));
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
        let collection = Arc::new(RegistrationCollection::new([1, 2]));
        let mut writer = collection.writer();
        let mut current = None;
        for value in [Some(10), None] {
            let mut lease = collection.projection().acquire((), None).unwrap();
            let Poll::Ready(Ok(bootstrap)) = Pin::new(&mut lease.read(None, ReadLimits::default()))
                .poll(&mut Context::from_waker(Waker::noop()))
            else {
                panic!("expected ready bootstrap");
            };
            let observer = Arc::new(SnapshotOnWake {
                collection: Arc::clone(&collection),
                expected: value.into_iter().map(|value| (1, value)).collect(),
                calls: AtomicUsize::new(0),
            });
            let waker = Waker::from(Arc::clone(&observer));
            let mut pending = lease.read(Some(bootstrap.through), ReadLimits::default());
            assert!(
                Pin::new(&mut pending)
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            let (version, notifications) = match value {
                Some(value) => writer.put_deferred(1, current, value).unwrap(),
                None => {
                    let (version, notifications) = writer.remove_deferred(1, current).unwrap();
                    (version.unwrap(), notifications)
                }
            };
            assert_eq!(observer.calls.load(Ordering::SeqCst), 0);
            assert_eq!(writer.version(&1), value.map(|_| version));
            assert_eq!(writer.row_count(), usize::from(value.is_some()));
            assert_eq!(
                collection
                    .snapshot()
                    .into_iter()
                    .map(|((_, key), entry)| (key, entry.value))
                    .collect::<Vec<_>>(),
                observer.expected
            );
            let Poll::Ready(Ok(page)) =
                Pin::new(&mut pending).poll(&mut Context::from_waker(&waker))
            else {
                panic!("expected committed mutation before dispatch");
            };
            let expected = match value {
                Some(value) => Change::Upsert {
                    key: (writer.id(), 1),
                    row_version: version,
                    value,
                    evidence: crate::RowEvidence::Authoritative,
                    write_context: None,
                },
                None => Change::Remove {
                    key: (writer.id(), 1),
                    row_version: version,
                },
            };
            assert_eq!(
                page.transactions,
                vec![crate::Transaction {
                    changes: vec![expected]
                }]
            );
            assert_eq!(observer.calls.load(Ordering::SeqCst), 0);
            if dispatch {
                notifications.dispatch();
            } else {
                drop(notifications);
            }
            assert_eq!(observer.calls.load(Ordering::SeqCst), usize::from(dispatch));
            assert_eq!(writer.version(&1), value.map(|_| version));
            current = value.map(|_| version);
        }
    }
}

#[test]
fn deferred_noops_and_conflicts_leave_state_and_parked_reads_unchanged() {
    let collection = Arc::new(RegistrationCollection::new([1, 2]));
    let mut writer = collection.writer();
    let version = writer.put(1, None, 10).unwrap();
    let initial = collection.lock().replica.clone();
    let mut lease = collection.projection().acquire((), None).unwrap();
    let Poll::Ready(Ok(bootstrap)) = Pin::new(&mut lease.read(None, ReadLimits::default()))
        .poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("expected ready bootstrap");
    };
    let observer = Arc::new(SnapshotOnWake {
        collection: Arc::clone(&collection),
        expected: Vec::new(),
        calls: AtomicUsize::new(0),
    });
    let waker = Waker::from(Arc::clone(&observer));
    let mut pending = lease.read(Some(bootstrap.through), ReadLimits::default());
    assert!(
        Pin::new(&mut pending)
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let (absent, notifications) = writer.remove_deferred(2, None).unwrap();
    assert_eq!(absent, None);
    notifications.dispatch();
    crate::PendingNotifications::default().dispatch();
    assert_eq!(
        writer.put_deferred(1, None, 20).unwrap_err(),
        MutationError::Conflict {
            current: Some(version)
        }
    );
    assert_eq!(
        writer.remove_deferred(1, None).unwrap_err(),
        MutationError::Conflict {
            current: Some(version)
        }
    );
    assert_eq!(
        writer.remove_deferred(2, Some(version)).unwrap_err(),
        MutationError::Conflict { current: None }
    );
    assert_eq!(collection.lock().replica, initial);
    assert_eq!(writer.row_count(), 1);
    assert_eq!(observer.calls.load(Ordering::SeqCst), 0);
    assert!(
        Pin::new(&mut pending)
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    writer.remove(1, Some(version)).unwrap();
    assert_eq!(observer.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn put_wakes_after_unlocking_registration() {
    assert_reentrant_wake(
        |_, writer, version| {
            writer.as_mut().unwrap().put(1, Some(version), 11).unwrap();
        },
        vec![(1, 11), (2, 20)],
        Some(1),
    );
}

#[test]
fn remove_wakes_after_unlocking_registration() {
    assert_reentrant_wake(
        |_, writer, version| {
            writer.as_mut().unwrap().remove(1, Some(version)).unwrap();
        },
        vec![(2, 20)],
        Some(1),
    );
}

#[test]
fn writer_drop_wakes_after_unlocking_registration() {
    assert_reentrant_wake(|_, writer, _| drop(writer.take()), Vec::new(), Some(2));
}

#[test]
fn reset_wakes_after_unlocking_registration() {
    assert_reentrant_wake(|collection, _, _| collection.reset(), Vec::new(), None);
}

struct MutateOnWake {
    collection: Arc<RegistrationCollection<u32, u32>>,
    writer: Mutex<RegistrationWriter<u32, u32>>,
    calls: AtomicUsize,
}

impl Wake for MutateOnWake {
    fn wake(self: Arc<Self>) {
        if self.collection.inner.try_lock().is_err() {
            return;
        }
        let projection = self.collection.projection();
        if !crate::projection::callback_tests::unlocked(&projection) {
            return;
        }
        let mut lease = projection.acquire((), None).unwrap();
        let Poll::Ready(Ok(bootstrap)) = Pin::new(&mut lease.read(None, ReadLimits::default()))
            .poll(&mut Context::from_waker(Waker::noop()))
        else {
            panic!("expected snapshot before reentrant mutation");
        };
        assert_eq!(bootstrap.transactions.len(), 1);
        assert!(matches!(
            &bootstrap.transactions[0].changes[..],
            [Change::Upsert { value: 11, .. }]
        ));
        let observer = Arc::new(SnapshotOnWake {
            collection: Arc::clone(&self.collection),
            expected: vec![(1, 11), (2, 20)],
            calls: AtomicUsize::new(0),
        });
        let waker = Waker::from(Arc::clone(&observer));
        let mut pending = lease.read(Some(bootstrap.through), ReadLimits::default());
        assert!(
            Pin::new(&mut pending)
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        self.writer.lock().unwrap().put(2, None, 20).unwrap();
        assert_eq!(observer.calls.load(Ordering::SeqCst), 1);
        let Poll::Ready(Ok(page)) = Pin::new(&mut pending).poll(&mut Context::from_waker(&waker))
        else {
            panic!("expected reentrant mutation to notify parked reader");
        };
        assert_eq!(page.transactions.len(), 1);
        assert!(matches!(
            &page.transactions[0].changes[..],
            [Change::Upsert {
                key: (_, 2),
                value: 20,
                ..
            }]
        ));
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn notification_reentrant_mutation_preserves_publication_order_and_wakes() {
    let collection = Arc::new(RegistrationCollection::new([1, 2]));
    let mut writer = collection.writer();
    let version = writer.put(1, None, 10).unwrap();
    let observer = Arc::new(MutateOnWake {
        collection: Arc::clone(&collection),
        writer: Mutex::new(collection.writer()),
        calls: AtomicUsize::new(0),
    });
    let mut lease = collection.projection().acquire((), None).unwrap();
    let Poll::Ready(Ok(bootstrap)) = Pin::new(&mut lease.read(None, ReadLimits::default()))
        .poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("expected initial snapshot");
    };
    let waker = Waker::from(Arc::clone(&observer));
    let mut pending = lease.read(Some(bootstrap.through), ReadLimits::default());
    assert!(
        Pin::new(&mut pending)
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    writer.put(1, Some(version), 11).unwrap();
    assert_eq!(observer.calls.load(Ordering::SeqCst), 1);
    let Poll::Ready(Ok(page)) =
        Pin::new(&mut pending).poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("expected both ordered mutations");
    };
    assert_eq!(
        page.transactions
            .into_iter()
            .map(|transaction| transaction
                .changes
                .into_iter()
                .map(|change| match change {
                    Change::Upsert { key, value, .. } => (key, value),
                    Change::Remove { .. } => panic!("unexpected removal"),
                })
                .collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        vec![
            vec![((writer.id(), 1), 11)],
            vec![((observer.writer.lock().unwrap().id(), 2), 20)]
        ]
    );
}
