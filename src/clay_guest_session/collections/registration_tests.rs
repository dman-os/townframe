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
fn physical_keys_are_deterministic_cbor_writer_and_byte_string_pairs() {
    assert_eq!(
        encode_physical_key(1, &b"x".to_vec()),
        [0x82, 0x01, 0x41, b'x']
    );
    let local = vec![7; 24];
    let mut expected = vec![0x82, 0x19, 0x01, 0x2c, 0x58, 24];
    expected.extend(&local);
    assert_eq!(encode_physical_key(300, &local), expected);
    assert_eq!(
        encode_physical_key(u64::MAX, &Vec::new()),
        [
            0x82, 0x1b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x40
        ]
    );
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
    let writer = session.grant_writer(&registrations).unwrap();
    assert_eq!(
        writer.put(vec![0; 3], None, vec![0]),
        Err(WriteFailure::KeyTooLarge)
    );
    assert_eq!(
        writer.put(vec![0], None, vec![0; 4]),
        Err(WriteFailure::ValueTooLarge)
    );
    assert_eq!(
        writer.remove(vec![0; 3], None),
        Err(WriteFailure::KeyTooLarge)
    );
    assert!(registrations.snapshot().is_empty());

    let version = writer.put(vec![1], None, vec![1]).unwrap();
    assert_eq!(
        writer.put(vec![2], None, vec![2]),
        Err(WriteFailure::RowLimit)
    );
    assert_eq!(
        writer.put(vec![2], Some(version), vec![2]),
        Err(WriteFailure::Conflict { current: None })
    );
    let replaced = writer.put(vec![1], Some(version), vec![3]).unwrap();
    assert_eq!(
        registrations
            .snapshot()
            .into_values()
            .map(|entry| (entry.version, entry.value.body))
            .collect::<Vec<_>>(),
        [(replaced, vec![3])]
    );
}

#[tokio::test]
async fn admitted_rows_fit_an_atomic_withdrawal_page() {
    let limits = CollectionLimits {
        key_bytes: 64,
        value_bytes: 16,
        page_bytes: 1024,
        rows_per_writer: usize::MAX,
        ..CollectionLimits::default()
    };
    let (session, host, instance) = session(limits);
    let registrations = registrations();
    let writer = session.grant_writer(&registrations).unwrap();
    let capacity = (limits.page_bytes - limits.cursor_bytes)
        / (limits.key_bytes + RegistrationWriter::MAX_PHYSICAL_KEY_OVERHEAD);
    for index in 0..capacity {
        writer.put(vec![index as u8; 64], None, vec![1]).unwrap();
    }
    assert_eq!(
        writer.put(vec![capacity as u8; 64], None, vec![1]),
        Err(WriteFailure::RowLimit)
    );
    // The reader lives on another incarnation: its own instance is about to close.
    let observer = CollectionSession::new(
        host.clone(),
        host.start("observer"),
        CollectionLimits::default(),
    );
    let reader = observer.grant(registrations.projection()).unwrap();
    let lease = reader.acquire(Vec::new(), None).unwrap();
    let read_limits = ReadLimits {
        max_transactions: Some(1),
        max_changes: Some(1),
    };
    let mut committed = None;
    loop {
        let page = lease
            .read(committed, read_limits, std::future::pending())
            .await
            .unwrap();
        committed = Some(page.through);
        if page.bootstrap_complete {
            break;
        }
    }
    // Closing the incarnation, not dropping the writer handle, withdraws the scope's rows.
    host.close_instance(&instance).unwrap();
    drop(writer);
    let withdrawn = lease
        .read(committed, read_limits, std::future::pending())
        .await
        .unwrap();
    assert_eq!(withdrawn.transactions.len(), 1);
    assert_eq!(withdrawn.transactions[0].changes.len(), capacity);
    assert!(
        withdrawn.transactions[0]
            .changes
            .iter()
            .all(|change| matches!(change, Change::Remove { .. }))
    );
    assert!(page_bytes(&withdrawn) <= limits.page_bytes);
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
    let writer = session.grant_writer(&registrations).unwrap();
    assert_eq!(
        writer.put(vec![1], None, vec![1; 128]),
        Err(WriteFailure::ValueTooLarge)
    );
    assert!(registrations.snapshot().is_empty());
    writer.put(vec![1], None, vec![1]).unwrap();
}

#[tokio::test]
async fn put_notification_can_close_the_writers_instance_synchronously() {
    let (session, host, instance) = session(CollectionLimits::default());
    let registrations = registrations();
    let writer = Arc::new(session.grant_writer(&registrations).unwrap());
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
                observed.put(vec![2], None, vec![2]),
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
    let accepted = writer.put(vec![1], None, vec![1]).unwrap();
    let Poll::Ready(Ok(page)) = pending
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("accepted put and withdrawal must be published");
    };
    assert_eq!(page.transactions.len(), 2);
    assert!(
        matches!(&page.transactions[0].changes[..], [Change::Upsert { row_version, .. }] if *row_version == accepted)
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
    let writer = Arc::new(session.grant_writer(&registrations).unwrap());
    let version = writer.put(vec![1], None, vec![1]).unwrap();
    let mut lease = registrations
        .projection()
        .acquire(Vec::new(), None)
        .unwrap();
    let bootstrap = lease.read(None, ReadLimits::default()).await.unwrap();
    let observed = Arc::clone(&writer);
    let waker = Waker::from(Arc::new(OnceOnWake(Mutex::new(Some(Box::new(
        move || {
            observed.put(vec![1], None, vec![2]).unwrap();
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
    let removed = writer.remove(vec![1], Some(version)).unwrap().unwrap();
    let Poll::Ready(Ok(page)) = pending
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("removal and reentrant put must be published");
    };
    assert_eq!(page.transactions.len(), 2);
    assert!(
        matches!(&page.transactions[0].changes[..], [Change::Remove { row_version, .. }] if *row_version == removed)
    );
    assert!(
        matches!(&page.transactions[1].changes[..], [Change::Upsert { value, .. }] if value.body == vec![2])
    );
    assert_eq!(
        registrations
            .snapshot()
            .into_values()
            .map(|row| row.value.body)
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
    let first = session.grant_writer(&registrations).unwrap();
    let _second = session.grant_writer(&registrations).unwrap();
    assert_eq!(
        session.grant_writer(&registrations).err(),
        Some(AccessError::Denied)
    );
    assert_eq!(session.writer_count(), 2);
    first.put(vec![1], None, vec![1]).unwrap();
    drop(first);
    assert_eq!(session.writer_count(), 1);
    // Dropping a writer releases authority only; its rows belong to the incarnation's scope.
    assert_eq!(registrations.snapshot().len(), 1);
    let _third = session.grant_writer(&registrations).unwrap();
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
    let doomed = session.grant_writer(&registrations).unwrap();
    let bystander = other.grant_writer(&registrations).unwrap();
    doomed.put(vec![1], None, vec![1]).unwrap();
    doomed.put(vec![2], None, vec![2]).unwrap();
    bystander.put(vec![1], None, vec![9]).unwrap();

    let lease = other
        .grant(registrations.projection())
        .unwrap()
        .acquire(Vec::new(), None)
        .unwrap();
    let bootstrap = lease
        .read(None, ReadLimits::default(), std::future::pending())
        .await
        .unwrap();
    assert_eq!(bootstrap.transactions.len(), 3);
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
    assert_eq!(
        page.transactions[0]
            .changes
            .iter()
            .filter(|change| matches!(change, Change::Remove { .. }))
            .count(),
        2
    );
    assert_eq!(
        registrations
            .snapshot()
            .into_values()
            .map(|entry| entry.value.body)
            .collect::<Vec<_>>(),
        [vec![9]]
    );
    assert_eq!(
        doomed.put(vec![3], None, vec![3]),
        Err(WriteFailure::Revoked)
    );
    assert_eq!(
        session.grant_writer(&registrations).err(),
        Some(AccessError::Revoked)
    );
    drop(doomed);
    assert_eq!(session.writer_count(), 0);
}
