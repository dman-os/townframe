use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;

use super::*;

fn empty_page(through: u8) -> Page<u64, u64> {
    Page {
        transactions: Vec::new(),
        through: Cursor(vec![through]),
        bootstrap_complete: true,
    }
}

#[test]
fn replacement_epoch_rejects_old_tickets_even_when_every_other_field_matches() {
    let mut mirror = Mirror::empty();
    let old = mirror.begin_read();
    mirror.begin_lease(None);
    let current = mirror.begin_read();
    assert!(Arc::ptr_eq(&old.identity, &current.identity));
    assert_ne!(old.lease_epoch, current.lease_epoch);
    assert_eq!(old.sequence, current.sequence);
    assert_eq!(old.base, current.base);
    assert_eq!(old.first, current.first);
    let late = empty_page(1);
    assert!(!mirror.apply(&old, &late));
    assert!(!mirror.cancel_read(&old));
    assert_eq!(mirror.cursor(), None);
    assert!(!mirror.bootstrap_complete());
    assert!(mirror.apply(&current, &late));
    assert!(!mirror.apply(&old, &late));
}

#[test]
fn lease_epoch_exhaustion_cannot_clear_state_or_invalidate_the_active_request() {
    let mut mirror = Mirror::empty();
    let first = mirror.begin_read();
    let baseline = empty_page(1);
    assert!(mirror.apply(&first, &baseline));
    mirror.lease_epoch = u64::MAX;
    let active = mirror.begin_read();
    assert!(catch_unwind(AssertUnwindSafe(|| mirror.begin_lease(None))).is_err());
    assert_eq!(mirror.lease_epoch, u64::MAX);
    assert_eq!(mirror.cursor(), Some(&baseline.through));
    assert_eq!(mirror.committed(), Some(&baseline.through));
    assert!(mirror.bootstrap_complete());
    assert!(mirror.apply(&active, &empty_page(2)));
}
