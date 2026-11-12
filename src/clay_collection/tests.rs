use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use crate::AcquireError;
use crate::Change;
use crate::Cursor;
use crate::Entry;
use crate::Mirror;
use crate::Page;
use crate::Projection;
use crate::ProjectionLease;
use crate::ReadError;
use crate::ReadLimits;
use crate::RowEvidence;
use crate::Source;
use crate::Transaction;
use crate::Version;

#[path = "tests_cursors.rs"]
mod cursors;
#[path = "tests_lifecycle.rs"]
mod lifecycle;
#[path = "tests_model.rs"]
mod model;
#[path = "tests_requests.rs"]
mod requests;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Group {
    X,
    Y,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Selector {
    X,
    All,
    Invalid,
}

#[derive(Clone, Debug, PartialEq)]
struct Row {
    group: Group,
    value: u64,
}

#[derive(Debug)]
struct Host(Arc<AtomicUsize>);

#[derive(Debug)]
struct Interest(Arc<AtomicUsize>);

impl Drop for Interest {
    fn drop(&mut self) {
        assert!(self.0.fetch_sub(1, Ordering::SeqCst) > 0);
    }
}

impl Source<u8, Row> for Host {
    type Selector = Selector;
    type Interest = Interest;

    fn acquire(&self, selector: Selector) -> Result<(Selector, Interest), AcquireError> {
        if selector == Selector::Invalid {
            return Err(AcquireError::InvalidSelector);
        }
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok((selector, Interest(Arc::clone(&self.0))))
    }

    fn matches(&self, selector: &Selector, _key: &u8, value: &Row) -> bool {
        match selector {
            Selector::X => value.group == Group::X,
            Selector::All => true,
            Selector::Invalid => unreachable!(),
        }
    }
}

type TestProjection = Projection<u8, Row, Host>;
type Lease = ProjectionLease<u8, Row, Host>;
fn projection(capacity: usize) -> (TestProjection, Arc<AtomicUsize>) {
    let interests = Arc::new(AtomicUsize::new(0));
    let source = Projection::with_source(
        Host(Arc::clone(&interests)),
        NonZeroUsize::new(capacity).unwrap(),
        [1, 2],
    );
    (source, interests)
}

fn upsert(key: u8, seq: u64, group: Group) -> Change<u8, Row> {
    Change::Upsert {
        key,
        row_version: Version::new(1, seq),
        value: Row { group, value: seq },
        evidence: if seq.is_multiple_of(2) {
            RowEvidence::Authoritative
        } else {
            RowEvidence::Pending
        },
        write_context: Some(vec![key, 255]),
    }
}

fn remove(key: u8, seq: u64) -> Change<u8, Row> {
    Change::Remove {
        key,
        row_version: Version::new(1, seq),
    }
}

fn one() -> ReadLimits {
    ReadLimits {
        max_transactions: Some(1),
        max_changes: Some(1),
    }
}

fn poll<F: Future + Unpin>(future: &mut F, waker: &Waker) -> Poll<F::Output> {
    Pin::new(future).poll(&mut Context::from_waker(waker))
}

fn read(
    lease: &mut Lease,
    committed: Option<Cursor>,
    limits: ReadLimits,
) -> Result<Page<u8, Row>, ReadError> {
    match poll(&mut lease.read(committed, limits), Waker::noop()) {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("expected ready projection progress"),
    }
}

fn apply(lease: &mut Lease, mirror: &mut Mirror<u8, Row>, limits: ReadLimits) -> Page<u8, Row> {
    let ticket = mirror.begin_read();
    let page = read(lease, ticket.committed().cloned(), limits).unwrap();
    assert!(mirror.apply(&ticket, &page));
    page
}

#[test]
fn overlapping_selections_swap_membership_atomically_and_advance_empty_progress() {
    let (source, _) = projection(8);
    source.publish(vec![upsert(0, 1, Group::X), upsert(1, 2, Group::Y)]);
    let mut narrow = source.acquire(Selector::X, None).unwrap();
    let mut broad = source.acquire(Selector::All, None).unwrap();
    let (mut x, mut all) = (Mirror::empty(), Mirror::empty());
    assert!(apply(&mut narrow, &mut x, one()).bootstrap_complete);
    assert!(apply(&mut broad, &mut all, ReadLimits::default()).bootstrap_complete);
    assert_eq!(x.len(), 1);
    assert_eq!(all.len(), 2);
    assert_eq!(
        x.row(&0),
        Some(&Entry {
            version: Version::new(1, 1),
            value: Row {
                group: Group::X,
                value: 1
            },
            evidence: RowEvidence::Pending,
            write_context: Some(vec![0, 255]),
        })
    );

    let swap = vec![upsert(0, 3, Group::Y), upsert(1, 4, Group::X)];
    source.publish(swap.clone());
    let selected = apply(&mut narrow, &mut x, one());
    assert_eq!(
        selected.transactions,
        vec![Transaction {
            changes: vec![remove(0, 3), upsert(1, 4, Group::X)],
        }]
    );
    assert!(x.row(&0).is_none());
    assert_eq!(
        x.rows(),
        vec![(
            1,
            Row {
                group: Group::X,
                value: 4
            }
        )]
    );
    assert_eq!(all.row(&0).unwrap().version, Version::new(1, 1));
    assert_eq!(
        apply(&mut broad, &mut all, one()).transactions,
        vec![Transaction { changes: swap }]
    );
    assert_eq!(all.len(), 2);

    let before = (x.committed().cloned(), x.rows());
    source.publish(vec![upsert(2, 5, Group::Y)]);
    let empty = apply(&mut narrow, &mut x, one());
    assert!(empty.transactions.is_empty());
    assert_ne!(x.committed(), before.0.as_ref());
    assert_eq!(x.rows(), before.1);
    assert!(empty.bootstrap_complete);
    assert_eq!(
        apply(&mut broad, &mut all, one()).transactions,
        vec![Transaction {
            changes: vec![upsert(2, 5, Group::Y)],
        }]
    );
}

#[test]
fn compacted_history_bootstraps_only_selected_current_rows() {
    let (source, _) = projection(1);
    source.publish(vec![upsert(0, 1, Group::X), upsert(1, 2, Group::Y)]);
    source.publish(vec![remove(0, 3), upsert(1, 4, Group::X)]);
    source.publish(vec![upsert(2, 5, Group::Y)]);
    let mut lease = source.acquire(Selector::X, None).unwrap();
    let mut mirror = Mirror::empty();
    let current = apply(&mut lease, &mut mirror, one());
    assert_eq!(
        current.transactions,
        vec![Transaction {
            changes: vec![upsert(1, 4, Group::X)]
        }]
    );
    assert!(current.bootstrap_complete);
    assert_eq!(
        mirror.rows(),
        vec![(
            1,
            Row {
                group: Group::X,
                value: 4
            }
        )]
    );
}

#[test]
fn interrupted_bootstrap_keeps_its_snapshot_then_applies_concurrent_live_transactions() {
    let (source, _) = projection(8);
    let opening = vec![
        upsert(0, 1, Group::X),
        upsert(1, 2, Group::X),
        upsert(2, 3, Group::X),
    ];
    source.publish(opening.clone());
    let mut lease = source.acquire(Selector::All, None).unwrap();
    let mut mirror = Mirror::empty();
    assert!(!apply(&mut lease, &mut mirror, one()).bootstrap_complete);
    let cancelled = mirror.begin_read();
    drop(lease.read(cancelled.committed().cloned(), one()));
    assert!(mirror.cancel_read(&cancelled));
    let live = vec![remove(0, 4), upsert(1, 5, Group::Y), upsert(2, 6, Group::X)];
    source.publish(live.clone());
    let second = apply(&mut lease, &mut mirror, one());
    assert_eq!(
        second.transactions,
        vec![Transaction {
            changes: vec![opening[1].clone()]
        }]
    );
    assert!(!second.bootstrap_complete);
    let last = apply(&mut lease, &mut mirror, one());
    assert_eq!(
        last.transactions,
        vec![Transaction {
            changes: vec![opening[2].clone()]
        }]
    );
    assert!(last.bootstrap_complete);
    assert_eq!(mirror.len(), 3);
    let caught_up = apply(&mut lease, &mut mirror, one());
    assert_eq!(caught_up.transactions, vec![Transaction { changes: live }]);
    assert!(caught_up.bootstrap_complete);
    assert_eq!(
        mirror.rows(),
        vec![
            (
                1,
                Row {
                    group: Group::Y,
                    value: 5
                }
            ),
            (
                2,
                Row {
                    group: Group::X,
                    value: 6
                }
            ),
        ]
    );
}
