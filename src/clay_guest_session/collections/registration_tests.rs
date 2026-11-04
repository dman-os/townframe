use super::*;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;

struct OnceOnWake(Mutex<Option<Box<dyn FnOnce() + Send>>>);

impl Wake for OnceOnWake {
    fn wake(self: Arc<Self>) {
        let action = self.0.lock().unwrap().take();
        if let Some(action) = action {
            action();
        }
    }
}

fn registrations() -> BytesRegistrations {
    bytes_registrations(NonZeroUsize::new(8).unwrap(), [3, 4])
}

fn session(limits: CollectionLimits) -> (CollectionSession, Host, Instance) {
    let host = Host::new();
    let instance = host.start("registrations");
    (
        CollectionSession::new(host.clone(), instance.clone(), limits),
        host,
        instance,
    )
}

#[test]
fn key_value_and_row_limits_refuse_before_the_collection_changes() {
    let (session, _, _) = session(CollectionLimits {
        key_bytes: 2,
        value_bytes: 3,
        rows_per_writer: 1,
        ..CollectionLimits::default()
    });
    let registrations = registrations();
    let writer = session
        .grant_writer(full_grant(&registrations, "w"))
        .unwrap();
    assert_eq!(
        writer.put(vec![0; 3], WriteBasis::Concurrent, vec![0]),
        Err(WriteFailure::KeyTooLarge)
    );
    assert_eq!(
        writer.put(vec![0], WriteBasis::Concurrent, vec![0; 4]),
        Err(WriteFailure::ValueTooLarge)
    );
    assert_eq!(
        writer.delete(vec![0; 3], WriteBasis::Concurrent),
        Err(WriteFailure::KeyTooLarge)
    );
    assert!(registrations.snapshot().is_empty());

    let stamp = writer
        .put(vec![1], WriteBasis::Concurrent, vec![1])
        .unwrap();
    assert_eq!(
        writer.put(vec![2], WriteBasis::Concurrent, vec![2]),
        Err(WriteFailure::RowLimit)
    );
    // A tombstone holds a row of the writer's budget like a value does.
    assert_eq!(
        writer.delete(vec![2], WriteBasis::Concurrent),
        Err(WriteFailure::RowLimit)
    );
    let replaced = writer
        .put(vec![1], WriteBasis::Observed(stamp), vec![3])
        .unwrap();
    assert_eq!(replaced.lamport, 2);
    assert_eq!(
        registrations
            .snapshot()
            .into_values()
            .map(|entry| (entry.stamp, entry.value.unwrap().body))
            .collect::<Vec<_>>(),
        [(replaced, vec![3])]
    );
}

#[test]
fn a_put_must_fit_the_projection_page_even_when_the_value_limit_is_larger() {
    let (session, _, _) = session(CollectionLimits {
        page_bytes: 128,
        key_bytes: 1,
        value_bytes: 1024,
        ..CollectionLimits::default()
    });
    let registrations = registrations();
    let writer = session
        .grant_writer(full_grant(&registrations, "w"))
        .unwrap();
    assert_eq!(
        writer.put(vec![1], WriteBasis::Concurrent, vec![1; 128]),
        Err(WriteFailure::ValueTooLarge)
    );
    assert!(registrations.snapshot().is_empty());
    writer
        .put(vec![1], WriteBasis::Concurrent, vec![1])
        .unwrap();
}

#[tokio::test]
async fn put_notification_can_close_the_writers_instance_synchronously() {
    let (session, host, instance) = session(CollectionLimits::default());
    let registrations = registrations();
    let writer = Arc::new(
        session
            .grant_writer(full_grant(&registrations, "w"))
            .unwrap(),
    );
    let mut lease = registrations
        .projection()
        .acquire(Vec::new(), None)
        .unwrap();
    let bootstrap = lease.read(None, ReadLimits::default()).await.unwrap();
    let observed = Arc::clone(&writer);
    let waker = Waker::from(Arc::new(OnceOnWake(Mutex::new(Some(Box::new(
        move || {
            host.close_instance(&instance).unwrap();
            assert_eq!(
                observed.put(vec![2], WriteBasis::Concurrent, vec![2]),
                Err(WriteFailure::Revoked)
            );
        },
    ))))));
    let pending = lease.read(Some(bootstrap.through), ReadLimits::default());
    let mut pending = std::pin::pin!(pending);
    assert!(
        pending
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let accepted = writer
        .put(vec![1], WriteBasis::Concurrent, vec![1])
        .unwrap();
    let Poll::Ready(Ok(page)) = pending
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("accepted put and withdrawal must be published");
    };
    assert_eq!(page.transactions.len(), 2);
    assert!(
        matches!(&page.transactions[0].changes[..], [Change::Upsert { value, .. }] if value.stamp == accepted)
    );
    assert!(matches!(
        &page.transactions[1].changes[..],
        [Change::Remove { .. }]
    ));
    assert!(registrations.snapshot().is_empty());
}

#[tokio::test]
async fn remove_notification_can_mutate_the_same_writer_synchronously() {
    let (session, _, _) = session(CollectionLimits::default());
    let registrations = registrations();
    let writer = Arc::new(
        session
            .grant_writer(full_grant(&registrations, "w"))
            .unwrap(),
    );
    let stamp = writer
        .put(vec![1], WriteBasis::Concurrent, vec![1])
        .unwrap();
    let mut lease = registrations
        .projection()
        .acquire(Vec::new(), None)
        .unwrap();
    let bootstrap = lease.read(None, ReadLimits::default()).await.unwrap();
    let observed = Arc::clone(&writer);
    let waker = Waker::from(Arc::new(OnceOnWake(Mutex::new(Some(Box::new(
        move || {
            observed
                .put(vec![1], WriteBasis::Concurrent, vec![2])
                .unwrap();
        },
    ))))));
    let pending = lease.read(Some(bootstrap.through), ReadLimits::default());
    let mut pending = std::pin::pin!(pending);
    assert!(
        pending
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let tombstone = writer.delete(vec![1], WriteBasis::Observed(stamp)).unwrap();
    assert_eq!(tombstone.lamport, 2);
    let Poll::Ready(Ok(page)) = pending
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("removal and reentrant put must be published");
    };
    assert_eq!(page.transactions.len(), 2);
    // The deletion replicates as a tombstone row carrying its stamp.
    assert!(matches!(
        &page.transactions[0].changes[..],
        [Change::Upsert { value, .. }] if value.stamp == tombstone && value.value.is_none()
    ));
    assert!(
        matches!(&page.transactions[1].changes[..], [Change::Upsert { value, .. }] if value.value.as_ref().unwrap().body == vec![2])
    );
    assert_eq!(
        registrations
            .snapshot()
            .into_values()
            .map(|row| row.value.unwrap().body)
            .collect::<Vec<_>>(),
        [vec![2]]
    );
}

#[test]
fn writer_budget_admits_up_to_the_limit_and_drops_release_it() {
    let (session, _, _) = session(CollectionLimits {
        writers: 2,
        ..CollectionLimits::default()
    });
    let registrations = registrations();
    let first = session
        .grant_writer(full_grant(&registrations, "w"))
        .unwrap();
    let _second = session
        .grant_writer(full_grant(&registrations, "w"))
        .unwrap();
    assert_eq!(
        session.grant_writer(full_grant(&registrations, "w")).err(),
        Some(AccessError::Denied)
    );
    assert_eq!(session.writer_count(), 2);
    first.put(vec![1], WriteBasis::Concurrent, vec![1]).unwrap();
    drop(first);
    assert_eq!(session.writer_count(), 1);
    // Dropping a writer releases authority only; its rows belong to the incarnation's scope.
    assert_eq!(registrations.snapshot().len(), 1);
    let _third = session
        .grant_writer(full_grant(&registrations, "w"))
        .unwrap();
    assert_eq!(session.writer_count(), 2);
}

#[tokio::test]
async fn instance_close_withdraws_its_writers_rows_in_one_transaction_and_wakes_readers() {
    let (session, host, instance) = session(CollectionLimits::default());
    let registrations = registrations();
    let other = CollectionSession::new(
        host.clone(),
        host.start("bystander"),
        CollectionLimits::default(),
    );
    let doomed = session
        .grant_writer(full_grant(&registrations, "doomed"))
        .unwrap();
    let bystander = other
        .grant_writer(full_grant(&registrations, "bystander"))
        .unwrap();
    // Both write key 1 at lamport 1; "doomed" outranks "bystander", so the bystander's
    // value is a hidden candidate until the doomed scope retracts.
    doomed
        .put(vec![1], WriteBasis::Concurrent, vec![1])
        .unwrap();
    doomed
        .put(vec![2], WriteBasis::Concurrent, vec![2])
        .unwrap();
    bystander
        .put(vec![1], WriteBasis::Concurrent, vec![9])
        .unwrap();

    let lease = other
        .grant(registrations.projection())
        .unwrap()
        .acquire(Vec::new(), None)
        .unwrap();
    let bootstrap = lease
        .read(None, ReadLimits::default(), std::future::pending())
        .await
        .unwrap();
    assert_eq!(bootstrap.transactions.len(), 2);
    let parked = lease.read(
        Some(bootstrap.through),
        ReadLimits::default(),
        std::future::pending(),
    );
    let mut parked = std::pin::pin!(parked);
    assert!(futures::poll!(parked.as_mut()).is_pending());

    host.close_instance(&instance).unwrap();
    let Poll::Ready(Ok(page)) = futures::poll!(parked.as_mut()) else {
        panic!("closing the writer's instance must wake the parked reader");
    };
    assert_eq!(page.transactions.len(), 1);
    // The doomed key 2 disappears and key 1 reveals the bystander's value, in one transaction.
    assert!(matches!(
        &page.transactions[0].changes[..],
        [
            Change::Upsert { key: revealed, value, .. },
            Change::Remove { key: removed, .. },
        ] if revealed == &vec![1] && value.value.as_ref().unwrap().body == vec![9] && removed == &vec![2]
    ));
    assert_eq!(
        registrations
            .snapshot()
            .into_values()
            .map(|entry| entry.value.unwrap().body)
            .collect::<Vec<_>>(),
        [vec![9]]
    );
    assert_eq!(
        doomed.put(vec![3], WriteBasis::Concurrent, vec![3]),
        Err(WriteFailure::Revoked)
    );
    assert_eq!(
        session
            .grant_writer(full_grant(&registrations, "doomed"))
            .err(),
        Some(AccessError::Revoked)
    );
    drop(doomed);
    assert_eq!(session.writer_count(), 0);
}
