use super::*;
use std::cell::Cell;

fn route() -> documents::FacetRoute {
    documents::FacetRoute {
        document: "config".into(),
        branch: "main".into(),
        key: documents::FacetKey {
            tag: "counter".into(),
            key_id: "count".into(),
        },
    }
}

fn snapshot(
    watch_id: u64,
    revision: u64,
    value: documents::FacetValue,
) -> documents::DocumentSnapshot {
    documents::DocumentSnapshot {
        watch_id,
        revision,
        facets: vec![documents::FacetEntry {
            route: route(),
            value,
        }],
        pending: vec![],
        refreshing: false,
        revoked: false,
    }
}

fn retain(mirror: &DocumentMirror, id: u64) -> MirrorWatch {
    mirror.retain(
        None,
        route(),
        snapshot(id, 1, documents::FacetValue::Loading),
    )
}

fn receipt(
    id: u64,
    revision: u64,
    status: documents::OperationStatus,
) -> documents::OperationReceipt {
    documents::OperationReceipt {
        id,
        revision,
        status,
    }
}

#[test]
fn reads_are_local_and_preserve_availability_freshness_and_pending() {
    let mirror = DocumentMirror::default();
    let watch = retain(&mirror, 1);
    for _ in 0..10 {
        assert_eq!(
            watch.current().value::<u64>("counter", "count").unwrap(),
            FacetState::Loading
        );
    }
    assert!(mirror.changed(snapshot(1, 2, documents::FacetValue::Absent)));
    assert_eq!(
        watch.current().value::<u64>("counter", "count").unwrap(),
        FacetState::Absent
    );
    let mut incoming = snapshot(1, 3, documents::FacetValue::Present("5".into()));
    incoming.pending = vec![42];
    incoming.refreshing = true;
    assert!(mirror.changed(incoming));
    assert_eq!(
        watch.current(),
        MirrorSnapshot {
            revision: 3,
            facets: [(
                ("counter".into(), "count".into()),
                FacetState::Present("5".into())
            )]
            .into(),
            pending: vec![42],
            refreshing: true,
            status: MirrorStatus::Live,
        }
    );
    assert_eq!(
        watch.current().value::<u64>("counter", "count").unwrap(),
        FacetState::Present(5)
    );
    assert!(matches!(
        watch.current().value::<u64>("other", "count"),
        Err(documents::DocumentError::Denied)
    ));
}

#[test]
fn routing_is_monotonic_and_isolated_between_watches() {
    let mirror = DocumentMirror::default();
    let first = retain(&mirror, 1);
    let second = retain(&mirror, 2);
    let second_initial = second.current();
    assert!(mirror.changed(snapshot(1, 3, documents::FacetValue::Present("8".into()))));
    let current = first.current();
    for (id, revision) in [(1, 1), (1, 3), (99, 9)] {
        assert!(!mirror.changed(snapshot(id, revision, documents::FacetValue::Absent)));
    }
    assert_eq!(first.current(), current);
    assert_eq!(second.current(), second_initial);
    assert!(mirror.lagged(99).is_none());
}

#[test]
fn lag_requires_explicit_reset_and_never_fetches_during_reads() {
    let mirror = DocumentMirror::default();
    let watch = retain(&mirror, 1);
    mirror.changed(snapshot(1, 3, documents::FacetValue::Present("8".into())));
    let lagged = mirror.lagged(1).unwrap();
    for _ in 0..10 {
        assert_eq!(watch.current().status, MirrorStatus::Lagged);
        assert_eq!(
            watch.current().value::<u64>("counter", "count").unwrap(),
            FacetState::Present(8)
        );
    }
    assert!(mirror.changed(snapshot(1, 4, documents::FacetValue::Present("9".into()))));
    assert_eq!(watch.current().status, MirrorStatus::Lagged);
    let reads = Cell::new(0);
    assert!(matches!(
        lagged.resync_with(|| {
            reads.set(reads.get() + 1);
            Ok(snapshot(1, 3, documents::FacetValue::Absent))
        }),
        Err(documents::DocumentError::SubscriptionLagged)
    ));
    assert_eq!(watch.current().status, MirrorStatus::Lagged);
    assert!(
        lagged
            .resync_with(|| {
                reads.set(reads.get() + 1);
                Ok(snapshot(1, 4, documents::FacetValue::Present("9".into())))
            })
            .unwrap()
    );
    assert_eq!(reads.get(), 2);
    assert_eq!(watch.current().status, MirrorStatus::Live);
    assert_eq!(
        watch.current().value::<u64>("counter", "count").unwrap(),
        FacetState::Present(9)
    );
}

#[test]
fn release_and_revocation_are_terminal_for_every_clone() {
    for terminal in [MirrorStatus::Released, MirrorStatus::Revoked] {
        let mirror = DocumentMirror::default();
        let watch = retain(&mirror, 1);
        let clone = watch.clone();
        mirror.changed(snapshot(1, 2, documents::FacetValue::Present("7".into())));
        match terminal {
            MirrorStatus::Released => watch.release(),
            MirrorStatus::Revoked => {
                let mut revoked = snapshot(1, 3, documents::FacetValue::Present("99".into()));
                revoked.revoked = true;
                revoked.pending = vec![4];
                revoked.refreshing = true;
                assert!(mirror.changed(revoked));
            }
            MirrorStatus::Live | MirrorStatus::Lagged => unreachable!(),
        }
        assert_eq!(
            clone.current(),
            MirrorSnapshot {
                revision: if terminal == MirrorStatus::Released {
                    2
                } else {
                    3
                },
                facets: BTreeMap::new(),
                pending: vec![],
                refreshing: false,
                status: terminal,
            }
        );
        assert!(!mirror.changed(snapshot(1, 9, documents::FacetValue::Present("99".into()))));
        assert!(mirror.lagged(1).is_none());
        assert!(matches!(
            clone.resync_with(|| panic!("released watch performed IO")),
            Err(documents::DocumentError::Revoked)
        ));
        assert!(matches!(
            clone.current().value::<u64>("counter", "count"),
            Err(documents::DocumentError::Revoked)
        ));
        clone.release();
        assert_eq!(clone.current().status, terminal);
    }
}

#[test]
fn resync_authority_failure_withdraws_local_values() {
    let mirror = DocumentMirror::default();
    let watch = retain(&mirror, 1);
    mirror.changed(snapshot(1, 2, documents::FacetValue::Present("7".into())));
    assert!(matches!(
        watch.resync_with(|| Err(documents::DocumentError::Revoked)),
        Err(documents::DocumentError::Revoked)
    ));
    assert_eq!(watch.current().status, MirrorStatus::Revoked);
    assert!(watch.current().facets.is_empty());
    assert!(!mirror.changed(snapshot(1, 3, documents::FacetValue::Present("8".into()))));
}

#[test]
fn last_watch_drop_removes_routing_but_not_pending_operations() {
    let mirror = DocumentMirror::default();
    let watch = retain(&mirror, 1);
    let clone = watch.clone();
    let operation = mirror.track(
        None,
        receipt(42, 0, documents::OperationStatus::LocallyApplied),
    );
    drop(watch);
    assert!(mirror.changed(snapshot(1, 2, documents::FacetValue::Present("1".into()))));
    drop(clone);
    assert!(mirror.state.borrow().watches.is_empty());
    assert!(!mirror.changed(snapshot(1, 3, documents::FacetValue::Absent)));
    assert!(!mirror.completed(receipt(42, 0, documents::OperationStatus::Accepted)));
    assert!(mirror.completed(receipt(42, 1, documents::OperationStatus::Accepted)));
    assert!(matches!(
        operation.current().status,
        documents::OperationStatus::Accepted
    ));
    assert_eq!(operation.current().revision, 1);
    assert!(mirror.state.borrow().operations.is_empty());
    assert!(!mirror.completed(receipt(42, 2, documents::OperationStatus::LocallyApplied)));
    assert!(matches!(
        operation.current().status,
        documents::OperationStatus::Accepted
    ));
}

#[test]
fn rollback_details_survive_explicit_watch_release() {
    let mirror = DocumentMirror::default();
    let watch = retain(&mirror, 1);
    let operation = mirror.track(
        None,
        receipt(42, 0, documents::OperationStatus::LocallyApplied),
    );
    watch.release();
    assert!(mirror.completed(receipt(
        42,
        1,
        documents::OperationStatus::RolledBack(documents::OperationFailure::Transport(
            documents::TransportFailure {
                message: "disconnected".into(),
                outcome_unknown: true,
            },
        )),
    )));
    let current = operation.current();
    assert_eq!((current.id, current.revision), (42, 1));
    let documents::OperationStatus::RolledBack(documents::OperationFailure::Transport(failure)) =
        current.status
    else {
        panic!("transport receipt lost its failure details");
    };
    assert_eq!(
        (failure.message.as_str(), failure.outcome_unknown),
        ("disconnected", true)
    );
    assert_eq!(watch.current().status, MirrorStatus::Released);
    assert!(mirror.state.borrow().operations.is_empty());
}

#[test]
fn dropping_ui_operation_handles_does_not_discard_completion_tracking() {
    let mirror = DocumentMirror::default();
    drop(mirror.track(
        None,
        receipt(42, 0, documents::OperationStatus::LocallyApplied),
    ));
    assert_eq!(mirror.state.borrow().operations.len(), 1);
    assert!(mirror.completed(receipt(
        42,
        1,
        documents::OperationStatus::RolledBack(documents::OperationFailure::Transport(
            documents::TransportFailure {
                message: "disconnected".into(),
                outcome_unknown: true,
            }
        ),)
    )));
    assert!(mirror.state.borrow().operations.is_empty());
}

// ── Notifier contract ──

fn counting(mirror: &DocumentMirror) -> Rc<Cell<u32>> {
    let calls = Rc::new(Cell::new(0));
    let counted = calls.clone();
    mirror.set_notifier(move || counted.set(counted.get() + 1));
    calls
}

#[test]
fn notifier_runs_exactly_once_per_applied_change_and_never_for_a_non_change() {
    let mirror = DocumentMirror::default();
    let calls = counting(&mirror);
    let watch = retain(&mirror, 1);
    assert_eq!(calls.get(), 0);

    // An applied change notifies once; a stale, duplicate, or unrouted snapshot does not.
    assert!(mirror.changed(snapshot(1, 2, documents::FacetValue::Present("1".into()))));
    assert_eq!(calls.get(), 1);
    assert!(!mirror.changed(snapshot(1, 2, documents::FacetValue::Present("9".into()))));
    assert!(!mirror.changed(snapshot(1, 1, documents::FacetValue::Absent)));
    assert!(!mirror.changed(snapshot(99, 9, documents::FacetValue::Absent)));
    assert_eq!(calls.get(), 1);

    // A lag mark on a routed watch notifies; an unknown watch does not.
    assert!(mirror.lagged(1).is_some());
    assert_eq!(calls.get(), 2);
    assert!(mirror.lagged(99).is_none());
    assert_eq!(calls.get(), 2);

    // Tracking is silent; a completion that advances notifies once; an already-settled or
    // unknown operation does not.
    mirror.track(
        None,
        receipt(42, 0, documents::OperationStatus::LocallyApplied),
    );
    assert_eq!(calls.get(), 2);
    assert!(mirror.completed(receipt(42, 1, documents::OperationStatus::Accepted)));
    assert_eq!(calls.get(), 3);
    assert!(!mirror.completed(receipt(42, 2, documents::OperationStatus::Accepted)));
    assert!(!mirror.completed(receipt(99, 1, documents::OperationStatus::Accepted)));
    assert_eq!(calls.get(), 3);

    // Release notifies once and is terminal: a second release is a silent no-op.
    watch.release();
    assert_eq!(calls.get(), 4);
    watch.release();
    assert_eq!(calls.get(), 4);
    assert!(!mirror.changed(snapshot(1, 3, documents::FacetValue::Absent)));
    assert_eq!(calls.get(), 4);
}

#[test]
fn resync_notifies_whichever_way_the_read_went() {
    let mirror = DocumentMirror::default();
    let calls = counting(&mirror);
    let watch = retain(&mirror, 1);

    // An unchanged snapshot applies nothing, but the notifier still runs so observers can
    // settle against the mirror either way.
    assert!(
        !watch
            .resync_with(|| Ok(snapshot(1, 1, documents::FacetValue::Loading)))
            .unwrap()
    );
    assert_eq!(calls.get(), 1);

    // A transient failure leaves the mirror untouched and still notifies.
    assert!(matches!(
        watch.resync_with(|| Err(documents::DocumentError::InvalidValue("bad".into()))),
        Err(documents::DocumentError::InvalidValue(_))
    ));
    assert_eq!(calls.get(), 2);

    // An authority failure withdraws the watch and notifies.
    assert!(matches!(
        watch.resync_with(|| Err(documents::DocumentError::Denied)),
        Err(documents::DocumentError::Denied)
    ));
    assert_eq!(watch.current().status, MirrorStatus::Revoked);
    assert_eq!(calls.get(), 3);

    // A terminal watch reports revocation without reading, and still notifies.
    assert!(matches!(
        watch.resync_with(|| panic!("released watch performed IO")),
        Err(documents::DocumentError::Revoked)
    ));
    assert_eq!(calls.get(), 4);
}

#[test]
#[should_panic(expected = "document mirror notifier installed twice")]
fn installing_a_second_notifier_is_an_invariant_failure() {
    let mirror = DocumentMirror::default();
    mirror.set_notifier(|| {});
    mirror.set_notifier(|| {});
}

#[test]
#[should_panic(expected = "configuration facet violates its declared value shape")]
fn decoding_a_present_facet_at_the_wrong_shape_is_an_invariant_failure() {
    let mirror = DocumentMirror::default();
    let watch = retain(&mirror, 1);
    mirror.changed(snapshot(
        1,
        2,
        documents::FacetValue::Present("not a number".into()),
    ));
    let _ = watch.current().value::<u64>("counter", "count");
}
