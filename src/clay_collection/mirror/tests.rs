use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;

use super::*;
use crate::RowEvidence;
use crate::Transaction;
use crate::Version;

fn upsert(key: u64, incarnation: u64, seq: u64) -> Change<u64, u64> {
    Change::Upsert {
        key,
        row_version: Version::new(incarnation, seq),
        value: seq,
        evidence: RowEvidence::Pending,
        write_context: Some(vec![7, 9]),
    }
}

fn page(changes: Vec<Change<u64, u64>>, through: u8, complete: bool) -> Page<u64, u64> {
    Page {
        transactions: vec![Transaction { changes }],
        through: Cursor(vec![through]),
        bootstrap_complete: complete,
    }
}

#[test]
fn accepted_ticket_is_consumed_and_preserves_every_row_field() {
    let mut mirror = Mirror::default();
    let ticket = mirror.begin_read();
    let first = page(vec![upsert(0, 1, 3)], 1, true);
    assert!(mirror.apply(&ticket, &first));
    assert_eq!(
        mirror.row(&0),
        Some(&Entry {
            version: Version::new(1, 3),
            value: 3,
            evidence: RowEvidence::Pending,
            write_context: Some(vec![7, 9]),
        })
    );
    assert_eq!(mirror.rows(), vec![(0, 3)]);
    assert_eq!(mirror.cursor(), Some(&first.through));
    assert_eq!(mirror.committed(), Some(&first.through));
    assert!(mirror.bootstrap_complete());
    assert!(mirror.active.is_none());
    assert!(!mirror.apply(&ticket.clone(), &first));
    assert!(!mirror.cancel_read(&ticket));
    let next = mirror.begin_read();
    assert_eq!(next.base(), Some(&first.through));
    assert!(!mirror.apply(&ticket, &page(vec![upsert(0, 1, 9)], 2, false)));
    assert_eq!(mirror.row(&0).unwrap().value, 3);
    assert!(mirror.cancel_read(&next));
}

#[test]
fn foreign_tickets_cannot_apply_or_cancel_the_active_request() {
    let mut left = Mirror::empty();
    let mut right = Mirror::empty();
    let a = left.begin_read();
    let b = right.begin_read();
    assert_eq!(a.sequence, b.sequence);
    assert_eq!(a.base, b.base);
    assert_eq!(a.first, b.first);
    assert_eq!(a.lease_epoch, b.lease_epoch);
    assert!(!Arc::ptr_eq(&a.identity, &b.identity));
    let first = page(vec![upsert(0, 1, 1)], 1, true);
    assert!(!left.apply(&b, &first));
    assert!(!right.apply(&a, &first));
    assert!(!left.cancel_read(&b));
    assert!(!right.cancel_read(&a));
    assert!(left.is_empty());
    assert_eq!(left.cursor(), None);
    assert!(!left.bootstrap_complete());
    assert!(left.apply(&a, &first));
    assert!(right.apply(&b, &first));
}

#[test]
fn request_counter_exhaustion_cannot_roll_over_or_alias_an_old_ticket() {
    let mut mirror = Mirror::empty();
    mirror.sequence = u64::MAX - 1;
    let old = mirror.begin_read();
    assert_eq!(old.sequence, u64::MAX);
    assert!(mirror.cancel_read(&old));
    assert!(catch_unwind(AssertUnwindSafe(|| mirror.begin_read())).is_err());
    assert_eq!(mirror.sequence, u64::MAX);
    assert!(mirror.active.is_none());
    mirror.begin_lease(None);
    let fresh = mirror.begin_read();
    assert_eq!(fresh.sequence, 1);
    let first = page(vec![upsert(0, 1, 1)], 1, true);
    assert!(!mirror.apply(&old, &first));
    assert!(mirror.apply(&fresh, &first));
}

#[test]
#[should_panic(expected = "mirror already has an active read")]
fn only_one_read_can_be_active() {
    let mut mirror = Mirror::<u64, u64>::empty();
    let _ticket = mirror.begin_read();
    mirror.begin_read();
}

#[test]
fn incarnation_validation_precedes_every_mutation_and_reset_accepts_a_new_epoch() {
    let mut mirror = Mirror::empty();
    let ticket = mirror.begin_read();
    let mixed = page(vec![upsert(0, 1, 1), upsert(1, 2, 2)], 1, true);
    assert!(catch_unwind(AssertUnwindSafe(|| mirror.apply(&ticket, &mixed))).is_err());
    assert!(mirror.is_empty());
    assert_eq!(mirror.incarnation, None);
    assert_eq!(mirror.cursor(), None);
    assert!(!mirror.bootstrap_complete());
    assert!(mirror.apply(&ticket, &page(vec![upsert(0, 1, 1)], 1, true)));
    let next = mirror.begin_read();
    let mixed = Page {
        transactions: vec![
            Transaction {
                changes: vec![upsert(0, 1, 3)],
            },
            Transaction {
                changes: vec![upsert(1, 2, 4)],
            },
        ],
        through: Cursor(vec![2]),
        bootstrap_complete: true,
    };
    assert!(catch_unwind(AssertUnwindSafe(|| mirror.apply(&next, &mixed))).is_err());
    assert_eq!(mirror.rows(), vec![(0, 1)]);
    assert_eq!(mirror.cursor(), Some(&Cursor(vec![1])));
    assert_eq!(mirror.incarnation, Some(1));
    assert!(mirror.bootstrap_complete());
    mirror.clear();
    let fresh = mirror.begin_read();
    assert!(!mirror.apply(&next, &mixed));
    assert!(!mirror.cancel_read(&next));
    assert!(mirror.apply(&fresh, &page(vec![upsert(1, 2, 1)], 3, true)));
    assert_eq!(mirror.rows(), vec![(1, 1)]);
    assert_eq!(mirror.incarnation, Some(2));
}

#[test]
fn mismatched_resume_is_rejected_without_clearing_rows_or_cancelling_the_request() {
    let mut mirror = Mirror::empty();
    let ticket = mirror.begin_read();
    assert!(mirror.apply(&ticket, &page(vec![upsert(0, 1, 1)], 1, true)));
    let current = mirror.begin_read();
    assert!(
        catch_unwind(AssertUnwindSafe(
            || mirror.begin_lease(Some(&Cursor(vec![2])))
        ))
        .is_err()
    );
    assert_eq!(mirror.rows(), vec![(0, 1)]);
    assert_eq!(mirror.cursor(), Some(&Cursor(vec![1])));
    assert!(mirror.bootstrap_complete());
    assert!(mirror.cancel_read(&current));
}

#[test]
fn existing_row_versions_do_not_regress_and_completion_is_sticky_within_a_lease() {
    let mut mirror = Mirror::empty();
    let ticket = mirror.begin_read();
    assert!(mirror.apply(&ticket, &page(vec![upsert(0, 1, 4)], 1, true)));
    let next = mirror.begin_read();
    let stale = page(
        vec![
            upsert(0, 1, 3),
            Change::Remove {
                key: 0,
                row_version: Version::new(1, 2),
            },
        ],
        2,
        false,
    );
    assert!(mirror.apply(&next, &stale));
    assert_eq!(mirror.rows(), vec![(0, 4)]);
    assert_eq!(mirror.cursor(), Some(&stale.through));
    assert!(mirror.bootstrap_complete());
}

#[test]
fn late_narrow_response_cannot_resurrect_a_removed_key_or_roll_back_the_boundary() {
    for cancelled in [false, true] {
        let mut mirror = Mirror::empty();
        let initial = mirror.begin_read();
        let baseline = page(Vec::new(), 1, true);
        assert!(mirror.apply(&initial, &baseline));
        let narrow = mirror.begin_read();
        let late = page(vec![upsert(0, 1, 1)], 2, true);
        let wide = if cancelled {
            assert!(mirror.cancel_read(&narrow));
            mirror.begin_read()
        } else {
            narrow.clone()
        };
        assert_eq!(wide.base(), Some(&baseline.through));
        let accepted = Page {
            transactions: vec![
                late.transactions[0].clone(),
                Transaction {
                    changes: vec![Change::Remove {
                        key: 0,
                        row_version: Version::new(1, 2),
                    }],
                },
            ],
            through: Cursor(vec![3]),
            bootstrap_complete: true,
        };
        assert!(mirror.apply(&wide, &accepted));
        assert!(!mirror.apply(&narrow, &late));
        let next = mirror.begin_read();
        assert!(!mirror.apply(&narrow, &late));
        assert!(!mirror.cancel_read(&narrow));
        assert!(mirror.is_empty());
        assert_eq!(mirror.cursor(), Some(&accepted.through));
        assert_eq!(mirror.committed(), Some(&accepted.through));
        assert!(mirror.bootstrap_complete());
        assert!(mirror.cancel_read(&next));
    }
}

#[test]
fn churn_of_distinct_removed_keys_keeps_only_current_rows_and_one_request() {
    let mut mirror = Mirror::empty();
    for id in 0..10_000u64 {
        let key = Arc::new(id);
        let value = Arc::new(id);
        let retired_key = Arc::downgrade(&key);
        let retired_value = Arc::downgrade(&value);
        let changes = [
            vec![Change::Upsert {
                key: Arc::clone(&key),
                row_version: Version::new(1, id * 2),
                value,
                evidence: RowEvidence::Authoritative,
                write_context: Some(vec![7, 9]),
            }],
            vec![Change::Remove {
                key,
                row_version: Version::new(1, id * 2 + 1),
            }],
        ];
        for (offset, changes) in changes.into_iter().enumerate() {
            let ticket = mirror.begin_read();
            let page = Page {
                transactions: vec![Transaction { changes }],
                through: Cursor((id * 2 + offset as u64).to_be_bytes().to_vec()),
                bootstrap_complete: true,
            };
            assert!(mirror.apply(&ticket, &page));
            assert_eq!(mirror.len(), 1 - offset);
            assert!(mirror.active.is_none());
        }
        assert!(
            retired_key.upgrade().is_none(),
            "removed key retained at {id}"
        );
        assert!(
            retired_value.upgrade().is_none(),
            "historical value retained at {id}"
        );
    }
    assert_eq!(mirror.incarnation, Some(1));
    assert!(mirror.is_empty());
}

#[test]
fn write_basis_names_any_stamp_the_replica_holds_tombstones_included() {
    use crate::Stamp;
    use crate::WriterId;
    let stamp = |lamport| Stamp {
        lamport,
        writer: WriterId::from_encoded("w"),
    };
    let row = |key: u64, seq, lamport, value| Change::Upsert {
        key,
        row_version: Version::new(1, seq),
        value: Stamped {
            stamp: stamp(lamport),
            value,
        },
        evidence: RowEvidence::Authoritative,
        write_context: None,
    };
    let mut mirror = Mirror::default();
    let ticket = mirror.begin_read();
    let changes = vec![row(1, 1, 1, Some("live")), row(2, 2, 4, None)];
    assert!(mirror.apply(&ticket, &page_of(changes, 1)));
    assert_eq!(mirror.write_basis(&1), WriteBasis::Observed(stamp(1)));
    assert_eq!(mirror.write_basis(&2), WriteBasis::Observed(stamp(4)));
    assert_eq!(mirror.write_basis(&3), WriteBasis::Concurrent);
    assert_eq!(mirror.live(&1), Some(&"live"));
    assert_eq!(mirror.live(&2), None);
    assert_eq!(mirror.live_rows(), vec![(1, "live")]);
}

fn page_of<V>(changes: Vec<Change<u64, V>>, through: u8) -> Page<u64, V> {
    Page {
        transactions: vec![Transaction { changes }],
        through: Cursor(vec![through]),
        bootstrap_complete: true,
    }
}

mod ticket_lifecycle;
