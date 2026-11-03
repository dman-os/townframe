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
) -> Vec<((WriterId, u32), &'static str)> {
    collection
        .snapshot()
        .into_iter()
        .map(|(key, entry)| (key, entry.value))
        .collect()
}

#[test]
fn writers_sharing_a_local_key_hold_distinct_rows_and_cannot_touch_each_other() {
    let collection = RegistrationCollection::new([1, 2]);
    let mut a = collection.writer();
    let mut b = collection.writer();
    let a_version = a.put(7, None, "a").unwrap();
    let b_version = b.put(7, None, "b").unwrap();
    assert_eq!(rows(&collection), [((a.id(), 7), "a"), ((b.id(), 7), "b")]);

    assert_eq!(
        b.put(7, Some(a_version), "stolen"),
        Err(MutationError::Conflict {
            current: Some(b_version)
        })
    );
    assert_eq!(b.remove(8, None), Ok(None));
    assert_eq!(
        b.remove(8, Some(a_version)),
        Err(MutationError::Conflict { current: None })
    );
    assert_eq!(rows(&collection), [((a.id(), 7), "a"), ((b.id(), 7), "b")]);
    assert_eq!(a.version(&7), Some(a_version));
    assert_eq!(b.version(&8), None);
}

#[test]
fn compare_and_set_replaces_on_current_version_and_reports_the_actual_one_otherwise() {
    let collection = RegistrationCollection::new([1, 2]);
    let mut writer = collection.writer();
    let first = writer.put(1, None, "one").unwrap();
    assert_eq!(
        writer.put(1, None, "blind"),
        Err(MutationError::Conflict {
            current: Some(first)
        })
    );
    let second = writer.put(1, Some(first), "two").unwrap();
    assert!(second > first);
    assert_eq!(
        writer.remove(1, Some(first)),
        Err(MutationError::Conflict {
            current: Some(second)
        })
    );
    assert_eq!(rows(&collection), [((writer.id(), 1), "two")]);
    let removed = writer.remove(1, Some(second)).unwrap().unwrap();
    assert!(removed > second);
    assert_eq!(writer.row_count(), 0);
    assert_eq!(writer.remove(1, None), Ok(None));
}

#[test]
fn dropping_a_writer_withdraws_all_and_only_its_rows_in_one_transaction() {
    let collection = RegistrationCollection::new([1, 2]);
    let mut a = collection.writer();
    let mut b = collection.writer();
    a.put(1, None, "a1").unwrap();
    a.put(2, None, "a2").unwrap();
    b.put(1, None, "b1").unwrap();
    assert_eq!(a.row_count(), 2);
    let projection = collection.projection();
    let mut lease = projection.acquire((), None).unwrap();
    let bootstrap = ready(lease.read(None, ReadLimits::default())).unwrap();
    assert!(bootstrap.bootstrap_complete);

    let a_id = a.id();
    drop(a);
    let page = ready(lease.read(Some(bootstrap.through), ReadLimits::default())).unwrap();
    assert_eq!(page.transactions.len(), 1);
    let mut removed = page.transactions[0]
        .changes
        .iter()
        .map(|change| match change {
            Change::Remove { key, .. } => *key,
            Change::Upsert { .. } => panic!("closure published an upsert"),
        })
        .collect::<Vec<_>>();
    removed.sort();
    assert_eq!(removed, [(a_id, 1), (a_id, 2)]);
    assert_eq!(rows(&collection), [((b.id(), 1), "b1")]);
}

#[test]
fn removal_churn_retires_authority_tombstones_without_losing_sequence_or_replay() {
    let collection = RegistrationCollection::new([1, 2]);
    let mut writer = collection.writer();
    let mut expected = Replica::new(1);
    let mut last_removal = Version::new(1, 0);
    for key in 0..512 {
        let physical = (writer.id(), key);
        let version = writer.put(key, None, "value").unwrap();
        assert!(version > last_removal);
        assert_eq!(
            version,
            expected.plan_put(&physical, None, "value").row_version()
        );
        let removed = writer.remove(key, Some(version)).unwrap().unwrap();
        assert!(removed > version);
        assert_eq!(removed, expected.plan_remove(&physical).row_version());
        assert_eq!(collection.lock().replica, expected);
        assert_eq!(writer.row_count(), 0);
        last_removal = removed;
    }

    let recreated = writer.put(0, None, "recreated").unwrap();
    assert!(recreated > last_removal);
    let mut lease = collection.projection().acquire((), None).unwrap();
    let bootstrap = ready(lease.read(None, ReadLimits::default())).unwrap();
    let removed = writer.remove(0, Some(recreated)).unwrap().unwrap();
    assert!(removed > recreated);
    assert_eq!(collection.lock().replica.version(&(writer.id(), 0)), None);
    let page = ready(lease.read(Some(bootstrap.through), ReadLimits::default())).unwrap();
    assert_eq!(
        page.transactions,
        vec![crate::Transaction {
            changes: vec![Change::Remove {
                key: (writer.id(), 0),
                row_version: removed,
            }],
        }]
    );
    let newest = writer.put(0, None, "newest").unwrap();
    assert!(newest > removed);
    assert_eq!(
        writer.put(0, Some(recreated), "stale"),
        Err(MutationError::Conflict {
            current: Some(newest)
        })
    );
}

#[test]
fn writer_drop_churn_retires_authority_tombstones_and_preserves_other_rows_and_sequence() {
    let collection = RegistrationCollection::new([1, 2]);
    let mut keeper = collection.writer();
    let kept = keeper.put(0, None, "kept").unwrap();
    let mut expected = Replica::new(1);
    let change = expected.plan_put(&(keeper.id(), 0), None, "kept");
    assert_eq!(change.row_version(), kept);
    expected.apply(&[change]);
    let mut last_removal = kept;
    for _ in 0..64 {
        let mut writer = collection.writer();
        let id = writer.id();
        for key in 0..4 {
            let version = writer.put(key, None, "temporary").unwrap();
            assert!(version > last_removal);
            assert_eq!(
                version,
                expected
                    .plan_put(&(id, key), None, "temporary")
                    .row_version()
            );
        }
        for key in 0..4 {
            last_removal = expected.plan_remove(&(id, key)).row_version();
        }
        drop(writer);
        let inner = collection.lock();
        assert_eq!(inner.replica, expected);
        assert_eq!(inner.rows.len(), 1);
        assert!(!inner.rows.contains_key(&id));
        drop(inner);
        assert_eq!(rows(&collection), [((keeper.id(), 0), "kept")]);
        assert_eq!(keeper.version(&0), Some(kept));
    }
}

#[test]
#[should_panic(expected = "writer identity exhausted")]
fn writer_identity_exhaustion_cannot_wrap() {
    let collection = RegistrationCollection::<u32, u32>::new([1, 2]);
    collection.lock().next_writer = u64::MAX - 1;
    let last = collection.writer();
    assert_eq!(last.id(), WriterId(u64::MAX));
    drop(last);
    collection.writer();
}

#[test]
fn physical_key_encoding_carries_rows_to_the_projection() {
    let collection = RegistrationCollection::<u32, &str, String>::with_source(
        (),
        std::num::NonZeroUsize::new(4).unwrap(),
        [1, 2],
        |writer, key| format!("{}/{key}", writer.0),
    );
    let mut writer = collection.writer();
    writer.put(5, None, "five").unwrap();
    let mut lease = collection.projection().acquire((), None).unwrap();
    let page = ready(lease.read(None, ReadLimits::default())).unwrap();
    assert!(matches!(
        &page.transactions[0].changes[..],
        [Change::Upsert { key, value: "five", .. }] if *key == format!("{}/5", writer.id().0)
    ));
}

#[test]
fn staged_rows_stay_out_of_the_projection_until_one_commit_publishes_them_together() {
    let collection = RegistrationCollection::new([1, 2]);
    let mut live = collection.writer();
    live.put(9, None, "live").unwrap();
    let mut lease = collection.projection().acquire((), None).unwrap();
    let bootstrap = ready(lease.read(None, ReadLimits::default())).unwrap();
    assert!(bootstrap.bootstrap_complete);

    let mut a = collection.staged_writer();
    let mut b = collection.staged_writer();
    let first = a.put(1, None, "a1").unwrap();
    // Staged writers keep full compare-and-set semantics against real versions.
    assert_eq!(
        a.put(1, None, "blind"),
        Err(MutationError::Conflict {
            current: Some(first)
        })
    );
    a.put(1, Some(first), "a1'").unwrap();
    a.put(2, None, "a2").unwrap();
    let doomed = a.version(&2).unwrap();
    a.remove(2, Some(doomed)).unwrap();
    b.put(1, None, "b1").unwrap();
    assert_eq!(rows(&collection), [((live.id(), 9), "live")]);
    let quiet = lease
        .read(Some(bootstrap.through.clone()), ReadLimits::default())
        .complete_timeout()
        .unwrap();
    assert!(
        quiet.transactions.is_empty(),
        "staged rows leaked: {quiet:?}"
    );

    collection.commit_staged([a.id(), b.id()]);
    let page = ready(lease.read(Some(bootstrap.through), ReadLimits::default())).unwrap();
    assert_eq!(page.transactions.len(), 1, "commit must be one transaction");
    let mut published = page.transactions[0]
        .changes
        .iter()
        .map(|change| match change {
            Change::Upsert { key, value, .. } => (*key, *value),
            Change::Remove { .. } => panic!("commit published a removal"),
        })
        .collect::<Vec<_>>();
    published.sort();
    assert_eq!(published, [((a.id(), 1), "a1'"), ((b.id(), 1), "b1")]);

    // After commit the writer publishes directly.
    a.put(3, None, "a3").unwrap();
    assert_eq!(
        rows(&collection),
        [
            ((live.id(), 9), "live"),
            ((a.id(), 1), "a1'"),
            ((a.id(), 3), "a3"),
            ((b.id(), 1), "b1")
        ]
    );
    collection.commit_staged([a.id()]);
}

#[test]
fn dropping_a_staged_writer_discards_its_rows_without_a_projection_change() {
    let collection = RegistrationCollection::new([1, 2]);
    let mut lease = collection.projection().acquire((), None).unwrap();
    let bootstrap = ready(lease.read(None, ReadLimits::default())).unwrap();
    let mut staged = collection.staged_writer();
    let id = staged.id();
    staged.put(1, None, "never seen").unwrap();
    drop(staged);
    collection.commit_staged([id]);
    assert!(rows(&collection).is_empty());
    let mut live = collection.writer();
    live.put(2, None, "seen").unwrap();
    let page = ready(lease.read(Some(bootstrap.through), ReadLimits::default())).unwrap();
    assert_eq!(page.transactions.len(), 1);
    assert!(matches!(
        &page.transactions[0].changes[..],
        [Change::Upsert { key, value: "seen", .. }] if *key == (live.id(), 2)
    ));
    assert!(collection.lock().staged.is_empty());
}
