use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;

use super::*;
use crate::types::Change;
use crate::types::ReadError;
use crate::types::ReadLimits;

struct SnapshotOnWake {
    collection: Arc<RegistrationCollection<u32, u32>>,
    expected: Vec<(u32, u32)>,
    calls: AtomicUsize,
}

impl Wake for SnapshotOnWake {
    fn wake(self: Arc<Self>) {
        assert!(
            self.collection.inner.try_lock().is_ok(),
            "registration locked during wake"
        );
        let rows = self.collection.snapshot();
        assert_eq!(
            rows.iter()
                .map(|((_, key), entry)| (*key, entry.value))
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
        &RegistrationScope<u32, u32>,
        &mut Option<RegistrationWriter<u32, u32>>,
        Version,
    ),
    expected: Vec<(u32, u32)>,
    changes: Option<usize>,
) {
    let collection = Arc::new(RegistrationCollection::new([1, 2]));
    let scope = collection.create_scope();
    let writer = scope.writer().unwrap();
    let version = writer.put(1, None, 10).unwrap().dispatch();
    writer.put(2, None, 20).unwrap().dispatch();
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
    mutate(&scope, &mut writer, version);
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
        let scope = collection.create_scope();
        let writer = scope.writer().unwrap();
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
                Some(value) => {
                    let notifications = writer.put(1, current, value).unwrap();
                    (
                        *notifications.output(),
                        Box::new(|| {
                            notifications.dispatch();
                        }) as Box<dyn FnOnce()>,
                    )
                }
                None => {
                    let notifications = writer.remove(1, current).unwrap();
                    (
                        notifications.output().unwrap(),
                        Box::new(|| {
                            notifications.dispatch();
                        }) as Box<dyn FnOnce()>,
                    )
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
                    key: (writer.scope_id(), 1),
                    row_version: version,
                    value,
                    evidence: crate::RowEvidence::Authoritative,
                    write_context: None,
                },
                None => Change::Remove {
                    key: (writer.scope_id(), 1),
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
                notifications();
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
    let scope = collection.create_scope();
    let writer = scope.writer().unwrap();
    let version = writer.put(1, None, 10).unwrap().dispatch();
    let initial = collection.lock().scopes[&scope.id()].rows.clone();
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
    let absent = writer.remove(2, None).unwrap().dispatch();
    assert_eq!(absent, None);
    crate::PendingNotifications::default().dispatch();
    assert_eq!(
        writer.put(1, None, 20).unwrap_err(),
        MutationError::Conflict {
            current: Some(version)
        }
    );
    assert_eq!(
        writer.remove(1, None).unwrap_err(),
        MutationError::Conflict {
            current: Some(version)
        }
    );
    assert_eq!(
        writer.remove(2, Some(version)).unwrap_err(),
        MutationError::Conflict { current: None }
    );
    assert_eq!(collection.lock().scopes[&scope.id()].rows, initial);
    assert_eq!(writer.row_count(), 1);
    assert_eq!(observer.calls.load(Ordering::SeqCst), 0);
    assert!(
        Pin::new(&mut pending)
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    writer.remove(1, Some(version)).unwrap().dispatch();
    assert_eq!(observer.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn put_wakes_after_unlocking_registration() {
    assert_reentrant_wake(
        |_, writer, version| {
            writer
                .as_mut()
                .unwrap()
                .put(1, Some(version), 11)
                .unwrap()
                .dispatch();
        },
        vec![(1, 11), (2, 20)],
        Some(1),
    );
}

#[test]
fn remove_wakes_after_unlocking_registration() {
    assert_reentrant_wake(
        |_, writer, version| {
            writer
                .as_mut()
                .unwrap()
                .remove(1, Some(version))
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
fn clear_wakes_after_unlocking_registration() {
    assert_reentrant_wake(
        |_, writer, _| writer.as_ref().unwrap().clear().unwrap().dispatch(),
        Vec::new(),
        Some(2),
    );
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
    collection: Arc<RegistrationCollection<u32, u32>>,
    writer: Mutex<RegistrationWriter<u32, u32>>,
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
        self.writer
            .lock()
            .unwrap()
            .put(2, None, 20)
            .unwrap()
            .dispatch();
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
    let scope = collection.create_scope();
    let writer = scope.writer().unwrap();
    let version = writer.put(1, None, 10).unwrap().dispatch();
    let observer = Arc::new(MutateOnWake {
        collection: Arc::clone(&collection),
        writer: Mutex::new(scope.writer().unwrap()),
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
    writer.put(1, Some(version), 11).unwrap().dispatch();
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
            vec![((writer.scope_id(), 1), 11)],
            vec![((observer.writer.lock().unwrap().scope_id(), 2), 20)]
        ]
    );
}

#[test]
fn deferred_batch_commit_and_close_wake_only_after_dispatch() {
    let collection = Arc::new(RegistrationCollection::new([1, 2]));
    let a = collection.create_staged_scope();
    let b = collection.create_staged_scope();
    a.writer().unwrap().put(1, None, 10).unwrap().dispatch();
    b.writer().unwrap().put(2, None, 20).unwrap().dispatch();
    for commit in [true, false] {
        let mut lease = collection.projection().acquire((), None).unwrap();
        let Poll::Ready(Ok(bootstrap)) = Pin::new(&mut lease.read(None, ReadLimits::default()))
            .poll(&mut Context::from_waker(Waker::noop()))
        else {
            panic!("expected bootstrap")
        };
        let observer = Arc::new(SnapshotOnWake {
            collection: Arc::clone(&collection),
            expected: if commit {
                vec![(1, 10), (2, 20)]
            } else {
                Vec::new()
            },
            calls: AtomicUsize::new(0),
        });
        let waker = Waker::from(Arc::clone(&observer));
        let mut pending = lease.read(
            Some(bootstrap.through),
            ReadLimits {
                max_transactions: Some(1),
                max_changes: Some(1),
            },
        );
        assert!(
            Pin::new(&mut pending)
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        let dispatch = if commit {
            collection.commit_scopes(&[&a, &b])
        } else {
            collection.close_scopes(&[&a, &b])
        };
        assert_eq!(observer.calls.load(Ordering::SeqCst), 0);
        let Poll::Ready(Ok(page)) =
            Pin::new(&mut pending).poll(&mut Context::from_waker(Waker::noop()))
        else {
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
        let collection = RegistrationCollection::<u32, DropProbe>::with_source(
            (),
            NonZeroUsize::new(1).unwrap(),
            [1, 2],
            |scope, key| (scope, *key),
        );
        let scope = if staged {
            collection.create_staged_scope()
        } else {
            collection.create_scope()
        };
        let writer = scope.writer().unwrap();
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
                    let scope = collection.create_scope();
                    assert_eq!(scope.writer().unwrap().row_count(), 0);
                    scope.close().dispatch();
                }
            }
        }));
        writer.put(1, None, probe.clone()).unwrap().dispatch();
        for operation in 0..5 {
            let guard = host_lock.lock().unwrap();
            let before = events.lock().unwrap().len();
            let dispatch = match operation {
                0 => {
                    let dispatch = writer.put(1, writer.version(&1), probe.clone()).unwrap();
                    Box::new(move || {
                        dispatch.dispatch();
                    }) as Box<dyn FnOnce()>
                }
                1 => {
                    let dispatch = writer.remove(1, writer.version(&1)).unwrap();
                    Box::new(move || {
                        dispatch.dispatch();
                    })
                }
                2 => {
                    let dispatch = writer.clear().unwrap();
                    Box::new(move || dispatch.dispatch())
                }
                3 => {
                    let dispatch = scope.close();
                    Box::new(move || dispatch.dispatch())
                }
                4 => {
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
            dispatch();
            if operation != 4 || !staged {
                assert!(events.lock().unwrap().len() > before);
            }
            if (1..=2).contains(&operation) {
                writer.put(1, None, probe.clone()).unwrap().dispatch();
            }
        }
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

#[test]
fn scope_drop_and_projection_bootstrap_retirement_run_destructors_unlocked() {
    let collection = RegistrationCollection::new([1, 2]);
    let scope = collection.create_scope();
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
    writer.put(1, None, value).unwrap().dispatch();
    let mut lease = collection.projection().acquire((), None).unwrap();
    let Poll::Ready(Ok(page)) = Pin::new(&mut lease.read(None, ReadLimits::default()))
        .poll(&mut Context::from_waker(Waker::noop()))
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
