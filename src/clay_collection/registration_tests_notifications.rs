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
