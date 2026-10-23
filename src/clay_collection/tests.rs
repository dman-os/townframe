use std::cell::RefCell;
use std::rc::Rc;

use serde_json::json;

use crate::BaseDelta;
use crate::Batch;
use crate::DemandId;
use crate::DemandToken;
use crate::Downstream;
use crate::Event;
use crate::OperationId;
use crate::OverlayPolicy;
use crate::Query;
use crate::RowChange;
use crate::ScopeId;
use crate::Subscription;
use crate::Upstream;
use crate::Version;
use crate::View;
use crate::WriteOutcome;

type K = u32;
type V = &'static str;

fn upsert(key: K, seq: u64, value: V) -> BaseDelta<K, V> {
    BaseDelta::Upsert {
        key,
        version: Version::new(1, seq),
        value,
    }
}

fn removal(key: K, seq: u64) -> BaseDelta<K, V> {
    BaseDelta::Remove {
        key,
        version: Version::new(1, seq),
    }
}

fn base_batch(incarnation: u64, base: Vec<BaseDelta<K, V>>) -> Batch<K, V> {
    let mut batch = Batch::new(incarnation);
    batch.base = base;
    batch
}

fn token() -> DemandToken {
    DemandToken(json!({ "kind": "all" }))
}

#[derive(Default)]
struct Recording {
    demands: Vec<(DemandToken, DemandId)>,
    releases: Vec<DemandId>,
    writes: Vec<(OperationId, K, Option<V>, ScopeId)>,
}

/// An upstream that only records what it is asked to do, so a test can hold an echo back
/// until it decides the authority has answered.
#[derive(Clone)]
struct MockUpstream {
    recording: Rc<RefCell<Recording>>,
}

impl Upstream<K, V> for MockUpstream {
    fn attach(&self, _downstream: &Downstream<K, V>) {}

    fn demand(&self, token: DemandToken, id: DemandId) {
        self.recording.borrow_mut().demands.push((token, id));
    }

    fn release(&self, id: DemandId) {
        self.recording.borrow_mut().releases.push(id);
    }

    fn write(&self, id: OperationId, key: K, value: Option<V>, writer: ScopeId) {
        self.recording
            .borrow_mut()
            .writes
            .push((id, key, value, writer));
    }
}

/// A consumer hop whose upstream never answers on its own: the test feeds authority
/// batches in by hand and can read every write intent the chain forwarded.
struct Harness {
    view: View<K, V>,
    recording: Rc<RefCell<Recording>>,
    events: Rc<RefCell<Vec<Event<K, V>>>>,
    _subscription: Subscription<K, V>,
}

impl Harness {
    fn new(policy: OverlayPolicy) -> Self {
        let recording = Rc::new(RefCell::new(Recording::default()));
        let upstream = MockUpstream {
            recording: recording.clone(),
        };
        let view = View::<K, V>::consumer(policy, 1, Box::new(upstream));
        let events = Rc::new(RefCell::new(Vec::new()));
        let sink = events.clone();
        let subscription = view.subscribe(move |event| sink.borrow_mut().push(event.clone()));
        Self {
            view,
            recording,
            events,
            _subscription: subscription,
        }
    }

    fn events(&self) -> Vec<Event<K, V>> {
        self.events.borrow().clone()
    }

    fn writes(&self) -> Vec<(OperationId, K, Option<V>, ScopeId)> {
        self.recording.borrow().writes.clone()
    }
}

fn change(key: K, before: Option<V>, after: Option<V>) -> RowChange<K, V> {
    RowChange { key, before, after }
}

#[test]
fn origin_write_records_overlay_notifies_and_forwards_intent() {
    let h = Harness::new(OverlayPolicy::Replicated);
    let id = h.view.write(1, Some("pending"), ScopeId(7));

    assert_eq!(h.view.visible(&1), Some("pending"));
    assert_eq!(h.view.overlay_len(), 1);
    assert_eq!(h.view.base_len(), 0);
    assert_eq!(h.writes(), vec![(id, 1, Some("pending"), ScopeId(7))]);

    let events = h.events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].changes, vec![change(1, None, Some("pending"))]);
    assert_eq!(events[0].forward.overlays.len(), 1);
}

#[test]
fn local_policy_origin_still_records_and_notifies_its_own_subscribers() {
    let h = Harness::new(OverlayPolicy::Local);
    let id = h.view.write(1, Some("pending"), ScopeId(7));

    assert_eq!(h.view.visible(&1), Some("pending"));
    assert_eq!(h.view.overlay_len(), 1);
    assert_eq!(h.writes(), vec![(id, 1, Some("pending"), ScopeId(7))]);

    let events = h.events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].changes, vec![change(1, None, Some("pending"))]);
}

#[test]
fn rejected_or_unknown_write_reverts_to_base() {
    // Unknown is a transport failure: the write may have landed, but for visibility it is
    // treated exactly like a rejection — replication restores the value if it did land.
    for outcome in [WriteOutcome::Rejected, WriteOutcome::Unknown] {
        let h = Harness::new(OverlayPolicy::Replicated);
        h.view.apply(base_batch(1, vec![upsert(1, 1, "old")]));
        let id = h.view.write(1, Some("mine"), ScopeId(7));
        assert_eq!(h.view.visible(&1), Some("mine"));

        let mut batch = Batch::new(1);
        batch.settled = vec![(id, outcome)];
        h.view.apply(batch);

        assert_eq!(h.view.visible(&1), Some("old"));
        assert_eq!(h.view.overlay_len(), 0);

        let events = h.events();
        assert_eq!(events.len(), 3);
        assert_eq!(
            events[2].changes,
            vec![change(1, Some("mine"), Some("old"))]
        );
    }
}

#[test]
fn durable_settle_then_base_retires_without_flicker() {
    let h = Harness::new(OverlayPolicy::Replicated);
    h.view.apply(base_batch(1, vec![upsert(1, 1, "old")]));
    let id = h.view.write(1, Some("new"), ScopeId(7));
    let events_after_write = h.events().len();
    assert_eq!(events_after_write, 2);

    // The authority accepts; base has not arrived yet, so the overlay parks.
    let mut settled = Batch::new(1);
    settled.settled = vec![(
        id,
        WriteOutcome::Durable {
            version: Version::new(1, 2),
        },
    )];
    h.view.apply(settled);
    assert_eq!(h.view.overlay_len(), 1);
    assert_eq!(h.view.visible(&1), Some("new"));
    assert_eq!(h.events().len(), events_after_write);

    // Base arrives at the committed version with the committed value: the overlay
    // retires inside the same transition, so the row never reads as `old`.
    h.view.apply(base_batch(1, vec![upsert(1, 2, "new")]));
    assert_eq!(h.view.overlay_len(), 0);
    assert_eq!(h.view.visible(&1), Some("new"));
    assert_eq!(h.events().len(), events_after_write);
}

#[test]
fn settle_only_retires_when_base_is_already_ahead() {
    let h = Harness::new(OverlayPolicy::Replicated);
    h.view.apply(base_batch(1, vec![upsert(1, 5, "final")]));
    let id = h.view.write(1, Some("mine"), ScopeId(7));
    assert_eq!(h.view.visible(&1), Some("mine"));

    // The batch carries only the outcome, but base already outranks the parked version.
    let mut settled = Batch::new(1);
    settled.settled = vec![(
        id,
        WriteOutcome::Durable {
            version: Version::new(1, 4),
        },
    )];
    h.view.apply(settled);

    assert_eq!(h.view.overlay_len(), 0);
    assert_eq!(h.view.visible(&1), Some("final"));
    let events = h.events();
    assert_eq!(events.len(), 3);
    assert_eq!(
        events[2].changes,
        vec![change(1, Some("mine"), Some("final"))]
    );
}

#[test]
fn newest_pending_write_is_the_visible_one() {
    let h = Harness::new(OverlayPolicy::Replicated);
    h.view.apply(base_batch(1, vec![upsert(1, 1, "base")]));
    let first = h.view.write(1, Some("first"), ScopeId(7));
    let second = h.view.write(1, Some("second"), ScopeId(7));
    assert_eq!(h.view.visible(&1), Some("second"));

    // Rejecting the newest write falls back to the older pending one, not to base.
    let mut rejected = Batch::new(1);
    rejected.settled = vec![(second, WriteOutcome::Rejected)];
    h.view.apply(rejected);
    assert_eq!(h.view.overlay_len(), 1);
    assert_eq!(h.view.visible(&1), Some("first"));

    let mut rejected = Batch::new(1);
    rejected.settled = vec![(first, WriteOutcome::Rejected)];
    h.view.apply(rejected);
    assert_eq!(h.view.overlay_len(), 0);
    assert_eq!(h.view.visible(&1), Some("base"));
}

#[test]
fn optimistic_delete_hides_the_row_until_replication_confirms() {
    let h = Harness::new(OverlayPolicy::Replicated);
    h.view.apply(base_batch(1, vec![upsert(1, 1, "old")]));
    let id = h.view.write(1, None, ScopeId(7));

    assert_eq!(h.view.visible(&1), None);
    assert_eq!(h.view.base_len(), 1);
    assert!(h.view.query(&Query::new()).is_empty());

    let mut batch = Batch::new(1);
    batch.base = vec![removal(1, 2)];
    batch.settled = vec![(
        id,
        WriteOutcome::Durable {
            version: Version::new(1, 2),
        },
    )];
    h.view.apply(batch);

    assert_eq!(h.view.base_len(), 0);
    assert_eq!(h.view.overlay_len(), 0);
    assert_eq!(h.view.visible(&1), None);
    // seed + delete; the confirming batch is invisible because the row reads absent
    // both before and after.
    assert_eq!(h.events().len(), 2);
}

#[test]
fn authority_write_is_terminal_without_an_overlay() {
    let view = View::<K, V>::authority(OverlayPolicy::Replicated, 1);
    let events = Rc::new(RefCell::new(Vec::new()));
    let sink = events.clone();
    let _subscription = view.subscribe(move |event| sink.borrow_mut().push(event.clone()));

    let id = view.write(1, Some("row"), ScopeId(7));
    assert_eq!(view.base_len(), 1);
    assert_eq!(view.overlay_len(), 0);
    assert_eq!(view.visible(&1), Some("row"));
    // Terminal means the durable settlement rides the write's own transition: no pending
    // overlay ever exists for the authority's own row.
    assert_eq!(
        events.borrow()[0].forward.settled,
        vec![(
            id,
            WriteOutcome::Durable {
                version: Version::new(1, 1),
            }
        )]
    );

    view.write(1, None, ScopeId(7));
    assert_eq!(view.base_len(), 0);
    assert_eq!(view.visible(&1), None);
    assert_eq!(view.overlay_len(), 0);
}
#[test]
fn consumer_disposal_withdraws_overlays_and_reaches_downstream_but_keeps_base() {
    let h = Harness::new(OverlayPolicy::Replicated);
    let child = View::<K, V>::consumer(OverlayPolicy::Replicated, 1, Box::new(h.view.clone()));
    h.view.apply(base_batch(
        1,
        vec![upsert(1, 1, "base-one"), upsert(2, 2, "base-two")],
    ));
    h.view.write(1, Some("mine"), ScopeId(1));
    h.view.write(2, Some("theirs"), ScopeId(2));
    assert_eq!(child.visible(&1), Some("mine"));

    let events_before = h.events().len();
    let event = h.view.dispose_scope(ScopeId(1));

    assert_eq!(
        event.changes,
        vec![change(1, Some("mine"), Some("base-one"))]
    );
    assert_eq!(h.view.overlay_len(), 1);
    assert_eq!(h.view.visible(&1), Some("base-one"));
    assert_eq!(h.view.visible(&2), Some("theirs"));
    assert_eq!(h.view.base_len(), 2);
    assert_eq!(h.events().len(), events_before + 1);

    // The Clear rides the relay, so the child that saw the pending row reverts too, and
    // no base row is evicted anywhere.
    assert_eq!(child.visible(&1), Some("base-one"));
    assert_eq!(child.visible(&2), Some("theirs"));
    assert_eq!(child.base_len(), 2);
}

#[test]
fn authority_disposal_removes_every_owned_row_in_one_event() {
    let authority = View::<K, V>::authority(OverlayPolicy::Replicated, 1);
    authority.write(1, Some("a"), ScopeId(1));
    authority.write(2, Some("b"), ScopeId(1));
    authority.write(3, Some("c"), ScopeId(2));

    let events = Rc::new(RefCell::new(Vec::new()));
    let sink = events.clone();
    let _subscription = authority.subscribe(move |event| sink.borrow_mut().push(event.clone()));

    let event = authority.dispose_scope(ScopeId(1));

    assert_eq!(authority.base_len(), 1);
    assert_eq!(authority.visible(&1), None);
    assert_eq!(authority.visible(&2), None);
    assert_eq!(authority.visible(&3), Some("c"));
    assert_eq!(event.changes.len(), 2);
    assert_eq!(
        event.changes,
        vec![change(1, Some("a"), None), change(2, Some("b"), None)]
    );
    assert_eq!(events.borrow().len(), 1);
}

#[test]
fn query_sees_pending_rows_in_filter_order_and_limit() {
    let h = Harness::new(OverlayPolicy::Replicated);
    h.view.apply(base_batch(
        1,
        vec![upsert(1, 1, "apple"), upsert(2, 2, "banana")],
    ));
    h.view.write(3, Some("cherry"), ScopeId(7));
    h.view.write(2, Some("cranberry"), ScopeId(7));

    let cherry = Query::<V>::new().filter(|value| value.starts_with('c'));
    let mut rows = h.view.query(&cherry);
    rows.sort();
    assert_eq!(rows, vec![(2, "cranberry"), (3, "cherry")]);

    let descending = Query::<V>::new()
        .filter(|value| value.starts_with('c'))
        .order_by(|left, right| right.cmp(left));
    assert_eq!(
        h.view.query(&descending),
        vec![(2, "cranberry"), (3, "cherry")]
    );
    assert_eq!(h.view.query(&descending.limit(1)), vec![(2, "cranberry")]);

    let only_apple = Query::<V>::new().filter(|value| *value == "apple");
    assert_eq!(h.view.query(&only_apple), vec![(1, "apple")]);
}

#[test]
fn demand_coverage_is_released_without_evicting_rows() {
    let authority = View::<K, V>::authority(OverlayPolicy::Replicated, 1);
    authority.apply(base_batch(
        1,
        vec![upsert(1, 1, "one"), upsert(2, 2, "two")],
    ));
    let consumer =
        View::<K, V>::consumer(OverlayPolicy::Replicated, 1, Box::new(authority.clone()));

    let id = consumer.demand(token());
    assert!(consumer.is_covered(id));
    assert!(authority.is_covered(id));
    assert_eq!(consumer.base_len(), 2);
    assert_eq!(consumer.visible(&1), Some("one"));

    consumer.release(id);
    assert!(!consumer.is_covered(id));
    assert!(!authority.is_covered(id));
    assert_eq!(consumer.base_len(), 2);
    assert_eq!(consumer.visible(&1), Some("one"));
    assert_eq!(authority.base_len(), 2);
}

/// Coverage is the authority's word, never the consumer's own. A consumer whose upstream
/// has not answered stays uncovered over an empty replica — the state a consumer with no
/// upstream used to hide by bootstrapping from itself and reporting a satisfied lease.
#[test]
fn an_unanswered_demand_stays_uncovered_on_the_consumer() {
    let h = Harness::new(OverlayPolicy::Replicated);
    let id = h.view.demand(token());

    assert!(!h.view.is_covered(id));
    assert_eq!(h.view.base_len(), 0);
    assert_eq!(h.recording.borrow().demands, vec![(token(), id)]);

    // The authority answers: rows and coverage land in one transition.
    let mut answered = base_batch(1, vec![upsert(4, 1, "four")]);
    answered.covered = vec![id];
    h.view.apply(answered);
    assert!(h.view.is_covered(id));
    assert_eq!(h.view.visible(&4), Some("four"));

    // A second demand is its own lease; the first answer does not satisfy it.
    let second = h.view.demand(token());
    assert!(!h.view.is_covered(second));
    assert_eq!(h.recording.borrow().demands.len(), 2);
}

/// host sits between two sibling guests and reports write intents upward to a recording
/// upstream, so the test decides when — or whether — the authority echoes.
struct Chain {
    host: View<K, V>,
    guests: [View<K, V>; 2],
    recording: Rc<RefCell<Recording>>,
}

fn chain(policy: OverlayPolicy) -> Chain {
    let recording = Rc::new(RefCell::new(Recording::default()));
    let upstream = MockUpstream {
        recording: recording.clone(),
    };
    let host = View::<K, V>::consumer(policy, 1, Box::new(upstream));
    let guests = [
        View::<K, V>::consumer(policy, 1, Box::new(host.clone())),
        View::<K, V>::consumer(policy, 1, Box::new(host.clone())),
    ];
    Chain {
        host,
        guests,
        recording,
    }
}

#[test]
fn replicated_chain_shares_a_pending_write_with_the_sibling_guest() {
    let chain = chain(OverlayPolicy::Replicated);
    let writer = &chain.guests[0];
    let sibling = &chain.guests[1];

    let id = writer.write(9, Some("pending"), ScopeId(7));

    assert_eq!(writer.visible(&9), Some("pending"));
    assert_eq!(chain.host.visible(&9), Some("pending"));
    assert_eq!(sibling.visible(&9), Some("pending"));
    assert_eq!(
        chain.recording.borrow().writes,
        vec![(id, 9, Some("pending"), ScopeId(7))]
    );
    assert_eq!(writer.overlay_len(), 1);
    assert_eq!(chain.host.overlay_len(), 1);
    assert_eq!(sibling.overlay_len(), 1);

    // The authority answers: base and outcome land on the host and retire every copy in
    // one transition each.
    let mut accepted = base_batch(1, vec![upsert(9, 1, "pending")]);
    accepted.settled = vec![(
        id,
        WriteOutcome::Durable {
            version: Version::new(1, 1),
        },
    )];
    chain.host.apply(accepted);

    for view in [&chain.host, writer, sibling] {
        assert_eq!(view.overlay_len(), 0);
        assert_eq!(view.base_len(), 1);
        assert_eq!(view.visible(&9), Some("pending"));
    }
}

#[test]
fn local_chain_keeps_a_pending_write_to_the_writing_guest() {
    let chain = chain(OverlayPolicy::Local);
    let writer = &chain.guests[0];
    let sibling = &chain.guests[1];

    let id = writer.write(9, Some("pending"), ScopeId(7));

    assert_eq!(writer.visible(&9), Some("pending"));
    assert_eq!(writer.overlay_len(), 1);
    assert_eq!(sibling.visible(&9), None);
    assert_eq!(chain.host.visible(&9), None);
    assert_eq!(
        chain.recording.borrow().writes,
        vec![(id, 9, Some("pending"), ScopeId(7))]
    );

    // The echo still carries the committed row; the local hop retires on it.
    let mut accepted = base_batch(1, vec![upsert(9, 1, "pending")]);
    accepted.settled = vec![(
        id,
        WriteOutcome::Durable {
            version: Version::new(1, 1),
        },
    )];
    chain.host.apply(accepted);

    assert_eq!(writer.overlay_len(), 0);
    assert_eq!(sibling.overlay_len(), 0);
    assert_eq!(sibling.visible(&9), Some("pending"));
    assert_eq!(chain.host.visible(&9), Some("pending"));
}

#[test]
fn later_incarnation_resets_base_overlays_and_coverage_together() {
    let h = Harness::new(OverlayPolicy::Replicated);
    h.view.apply(base_batch(
        1,
        vec![upsert(1, 1, "one"), upsert(2, 2, "two")],
    ));
    let covered = h.view.demand(token());
    let mut covering = Batch::new(1);
    covering.covered = vec![covered];
    h.view.apply(covering);
    assert!(h.view.is_covered(covered));
    h.view.write(3, Some("three"), ScopeId(7));

    let events_before = h.events().len();
    let mut rebuilt = Batch::new(2);
    rebuilt.base = vec![BaseDelta::Upsert {
        key: 1,
        version: Version::new(2, 1),
        value: "uno",
    }];
    h.view.apply(rebuilt);

    assert_eq!(h.view.incarnation(), 2);
    assert_eq!(h.view.base_len(), 1);
    assert_eq!(h.view.overlay_len(), 0);
    assert!(!h.view.is_covered(covered));
    assert_eq!(h.view.visible(&1), Some("uno"));
    assert_eq!(h.view.visible(&2), None);
    assert_eq!(h.view.visible(&3), None);

    // One event carries the whole epoch break: every previously visible key reports its
    // transition alongside the new epoch's first row.
    let events = h.events();
    assert_eq!(events.len(), events_before + 1);
    let mut changes = events.last().unwrap().changes.clone();
    changes.sort_by_key(|change| change.key);
    assert_eq!(
        changes,
        vec![
            change(1, Some("one"), Some("uno")),
            change(2, Some("two"), None),
            change(3, Some("three"), None),
        ]
    );
}

#[test]
fn stale_incarnation_and_stale_version_batches_change_nothing() {
    let h = Harness::new(OverlayPolicy::Replicated);
    h.view.apply(base_batch(1, vec![upsert(1, 9, "high")]));
    let events_before = h.events().len();

    let stale = h.view.apply(base_batch(0, vec![upsert(1, 1, "ghost")]));
    assert!(stale.is_empty());
    assert_eq!(h.view.visible(&1), Some("high"));
    assert_eq!(h.events().len(), events_before);

    // Same incarnation, older seq: the replica's version guard keeps the newer row.
    let stale = h.view.apply(base_batch(1, vec![upsert(1, 5, "low")]));
    assert_eq!(stale.changes, Vec::<RowChange<K, V>>::new());
    assert_eq!(h.view.visible(&1), Some("high"));
    assert_eq!(h.events().len(), events_before);
}

#[test]
fn a_batch_is_one_transition_even_when_base_and_overlay_touch_the_same_row() {
    let h = Harness::new(OverlayPolicy::Replicated);
    h.view.apply(base_batch(1, vec![upsert(1, 1, "old")]));

    let observed: Rc<RefCell<Vec<Option<V>>>> = Rc::new(RefCell::new(Vec::new()));
    let sink = observed.clone();
    let view = h.view.clone();
    let _subscription = h.view.subscribe(move |_event| {
        sink.borrow_mut().push(view.visible(&1));
    });

    let mut batch = Batch::new(1);
    batch.base = vec![upsert(1, 2, "newbase")];
    batch.overlays = vec![crate::OverlayDelta::Put {
        id: OperationId(4242),
        key: 1,
        value: Some("pending"),
        writer: ScopeId(7),
    }];
    let event = h.view.apply(batch);

    // One listener invocation, and the intermediate base value is never what the row read
    // at any observable point.
    assert_eq!(observed.borrow().as_slice(), [Some("pending")]);
    assert_eq!(event.changes, vec![change(1, Some("old"), Some("pending"))]);
    assert_eq!(h.view.visible(&1), Some("pending"));
    assert_eq!(h.view.base_len(), 1);
}

#[test]
fn empty_epoch_reset_propagates_across_relays() {
    let chain = chain(OverlayPolicy::Replicated);
    chain.host.apply(base_batch(
        1,
        vec![upsert(1, 1, "one"), upsert(2, 2, "two")],
    ));
    chain.guests[0].write(3, Some("pending"), ScopeId(7));
    assert_eq!(chain.guests[1].visible(&1), Some("one"));
    assert_eq!(chain.guests[1].visible(&3), Some("pending"));

    chain.host.apply(Batch::new(2));

    for view in [&chain.host, &chain.guests[0], &chain.guests[1]] {
        assert_eq!(view.incarnation(), 2);
        assert_eq!(view.base_len(), 0);
        assert_eq!(view.overlay_len(), 0);
        assert_eq!(view.visible(&1), None);
        assert_eq!(view.visible(&3), None);
    }
}

#[test]
fn a_single_row_batch_does_not_clone_the_whole_replica() {
    use std::cell::Cell;
    #[derive(Debug)]
    struct Counted {
        value: u32,
        clones: Rc<Cell<usize>>,
    }
    impl Clone for Counted {
        fn clone(&self) -> Self {
            self.clones.set(self.clones.get() + 1);
            Self {
                value: self.value,
                clones: self.clones.clone(),
            }
        }
    }
    impl PartialEq for Counted {
        fn eq(&self, other: &Self) -> bool {
            self.value == other.value
        }
    }
    let clones = Rc::new(Cell::new(0));
    let view = View::<u32, Counted>::authority(OverlayPolicy::Local, 1);
    let mut batch = Batch::new(1);
    batch.base = (0..1000)
        .map(|key| BaseDelta::Upsert {
            key,
            version: Version::new(1, 1),
            value: Counted {
                value: key,
                clones: clones.clone(),
            },
        })
        .collect();
    view.apply(batch);
    clones.set(0);
    let mut batch = Batch::new(1);
    batch.base.push(BaseDelta::Upsert {
        key: 1,
        version: Version::new(1, 2),
        value: Counted {
            value: 2000,
            clones: clones.clone(),
        },
    });
    assert_eq!(view.apply(batch).changes.len(), 1);
    assert!(
        clones.get() < 10,
        "cloned {} values for one row",
        clones.get()
    );
}

#[test]
fn computed_upstream_interprets_the_token_and_can_answer_in_a_new_epoch() {
    struct Source(RefCell<Option<Downstream<K, V>>>);
    impl Upstream<K, V> for Source {
        fn attach(&self, downstream: &Downstream<K, V>) {
            *self.0.borrow_mut() = Some(downstream.clone());
        }
        fn demand(&self, token: DemandToken, id: DemandId) {
            assert_eq!(token, DemandToken(json!({"route": "chosen"})));
            let mut batch = Batch::new(2);
            batch.base.push(BaseDelta::Upsert {
                key: 7,
                version: Version::new(2, 1),
                value: "chosen",
            });
            batch.covered.push(id);
            self.0.borrow().as_ref().unwrap().apply(batch);
        }
        fn release(&self, _: DemandId) {}
        fn write(&self, _: OperationId, _: K, _: Option<V>, _: ScopeId) {
            unreachable!()
        }
    }
    let (view, _) = View::<K, V, crate::ReadOnly>::reader(
        OverlayPolicy::Local,
        1,
        Box::new(Source(RefCell::new(None))),
    );
    let query = Query::new().demand(DemandToken(json!({"route": "chosen"})));
    assert!(view.query(&query).is_empty());
    assert_eq!(view.incarnation(), 2);
    assert_eq!(view.query_coverage(&query), crate::Coverage::Complete);
    assert_eq!(view.query(&query), vec![(7, "chosen")]);
}

#[test]
fn query_mounts_one_opaque_demand_after_answering_from_local_rows() {
    let authority = View::<K, V>::authority(OverlayPolicy::Replicated, 1);
    authority.write(1, Some("remote"), ScopeId(1));
    let (reader, _) =
        View::<K, V, crate::ReadOnly>::reader(OverlayPolicy::Replicated, 1, Box::new(authority));
    let query = Query::new().demand(token());
    assert_eq!(reader.query_coverage(&query), crate::Coverage::Unknown);
    assert!(reader.query(&query).is_empty());
    assert_eq!(reader.query_coverage(&query), crate::Coverage::Complete);
    assert_eq!(reader.query(&query), vec![(1, "remote")]);

    let h = Harness::new(OverlayPolicy::Replicated);
    h.view.apply(base_batch(1, vec![upsert(1, 1, "cached")]));
    let opaque = DemandToken(json!({ "custom": { "path": [1, "two", null] } }));
    let query = Query::new()
        .demand(opaque.clone())
        .filter(|value| *value == "cached");
    for _ in 0..3 {
        assert_eq!(h.view.query(&query), vec![(1, "cached")]);
    }
    assert_eq!(h.recording.borrow().demands.len(), 1);
    let first = h.recording.borrow().demands[0].1;
    assert_eq!(h.recording.borrow().demands[0].0, opaque);
    assert_eq!(h.view.query_coverage(&query), crate::Coverage::Partial);
    let mut covered = Batch::new(1);
    covered.covered.push(first);
    h.view.apply(covered.clone());
    assert_eq!(h.view.query_coverage(&query), crate::Coverage::Complete);
    h.view.release_query(&query);
    h.view.apply(covered);
    assert!(!h.view.is_covered(first));
    assert_eq!(h.view.query_coverage(&query), crate::Coverage::Unknown);
    assert_eq!(h.view.visible(&1), Some("cached"));
    h.view.query(&query);
    let second = h.recording.borrow().demands[1].1;
    assert_ne!(first, second);
    h.view.apply(Batch::new(2));
    assert_eq!(h.view.query_coverage(&query), crate::Coverage::Unknown);
    h.view.query(&query);
    let third = h.recording.borrow().demands[2].1;
    assert_ne!(second, third);
    assert_eq!(h.recording.borrow().releases, vec![first, second]);
    h.view.close();
    assert_eq!(h.recording.borrow().releases, vec![first, second, third]);
}

#[test]
fn coverage_only_and_empty_reset_notify_but_sibling_and_late_coverage_do_not() {
    let chain = chain(OverlayPolicy::Replicated);
    let requesting = &chain.guests[0];
    let sibling = &chain.guests[1];
    let events = Rc::new(RefCell::new(Vec::new()));
    let sink = events.clone();
    let _listener = requesting.subscribe(move |event| sink.borrow_mut().push(event.clone()));
    let id = requesting.demand(token());
    let mut batch = Batch::new(1);
    batch.covered.push(id);
    chain.host.apply(batch.clone());
    assert_eq!(events.borrow().len(), 1);
    assert!(events.borrow()[0].changes.is_empty());
    assert!(requesting.is_covered(id));
    assert!(!sibling.is_covered(id));
    requesting.release(id);
    chain.host.apply(batch);
    assert!(!requesting.is_covered(id));
    assert_eq!(events.borrow().len(), 1);
    chain.host.apply(Batch::new(2));
    assert_eq!(events.borrow().len(), 2);
    assert!(events.borrow()[1].reset);
    assert!(!events.borrow()[1].is_empty());
}

#[test]
fn dropping_the_last_view_releases_demand_without_requiring_an_answer() {
    let recording = Rc::new(RefCell::new(Recording::default()));
    let view = View::<K, V>::consumer(
        OverlayPolicy::Local,
        1,
        Box::new(MockUpstream {
            recording: recording.clone(),
        }),
    );
    let id = view.demand(token());
    let retained = view.clone();
    drop(view);
    assert!(recording.borrow().releases.is_empty());
    drop(retained);
    assert_eq!(recording.borrow().releases, vec![id]);
}

#[test]
fn read_only_replicas_observe_shared_overlays_and_terminal_close() {
    let h = Harness::new(OverlayPolicy::Replicated);
    let (reader, input) = View::<K, V, crate::ReadOnly>::reader(
        OverlayPolicy::Replicated,
        1,
        Box::new(h.view.clone()),
    );
    h.view.write(1, Some("pending"), ScopeId(1));
    assert_eq!(reader.visible(&1), Some("pending"));
    assert_eq!(reader.overlay_len(), 1);
    h.view.close();
    assert!(reader.is_closed());
    assert_eq!(reader.incarnation(), 1);
    assert_eq!(reader.overlay_len(), 0);
    input.apply(base_batch(
        2,
        vec![BaseDelta::Upsert {
            key: 1,
            version: Version::new(2, 1),
            value: "late",
        }],
    ));
    assert_eq!(reader.visible(&1), None);
    assert_eq!(reader.incarnation(), 1);
}

#[test]
fn deleted_rows_reject_stale_resurrection_and_settle_after_removal() {
    let h = Harness::new(OverlayPolicy::Replicated);
    h.view.apply(base_batch(1, vec![upsert(1, 1, "old")]));
    let id = h.view.write(1, Some("pending"), ScopeId(1));
    h.view.apply(base_batch(1, vec![removal(1, 3)]));
    h.view.apply(base_batch(1, vec![upsert(1, 2, "stale")]));
    assert_eq!(h.view.base_len(), 0);
    let mut settled = Batch::new(1);
    settled.settled.push((
        id,
        WriteOutcome::Durable {
            version: Version::new(1, 2),
        },
    ));
    h.view.apply(settled);
    assert_eq!(h.view.overlay_len(), 0);
    assert_eq!(h.view.visible(&1), None);
    h.view.apply(base_batch(1, vec![upsert(1, 4, "fresh")]));
    assert_eq!(h.view.visible(&1), Some("fresh"));
}

#[test]
fn a_batch_can_introduce_and_settle_an_overlay_without_leaving_it_pending() {
    for outcome in [
        WriteOutcome::Rejected,
        WriteOutcome::Durable {
            version: Version::new(1, 1),
        },
    ] {
        let h = Harness::new(OverlayPolicy::Replicated);
        let mut batch = base_batch(1, vec![upsert(1, 1, "base")]);
        batch.overlays.push(crate::OverlayDelta::Put {
            id: OperationId(42),
            key: 1,
            value: Some("pending"),
            writer: ScopeId(1),
        });
        batch.settled.push((OperationId(42), outcome));
        h.view.apply(batch);
        assert_eq!(h.view.overlay_len(), 0);
        assert_eq!(h.view.visible(&1), Some("base"));
        assert_eq!(h.events().len(), 1);
        assert_eq!(h.events()[0].changes, vec![change(1, None, Some("base"))]);
    }
}

#[test]
fn reentrant_settlement_cannot_overtake_overlay_delivery_to_siblings() {
    for settle_at_host in [true, false] {
        let chain = chain(OverlayPolicy::Replicated);
        let host = chain.host.clone();
        let observer = if settle_at_host {
            &chain.host
        } else {
            &chain.guests[0]
        };
        let _settler = observer.subscribe(move |event| {
            for delta in &event.forward.overlays {
                if let crate::OverlayDelta::Put { id, .. } = delta {
                    let mut settled = Batch::new(1);
                    settled.settled.push((*id, WriteOutcome::Rejected));
                    host.apply(settled);
                }
            }
        });
        let events = Rc::new(RefCell::new(Vec::new()));
        let sink = events.clone();
        let _listener = chain.guests[1].subscribe(move |event| {
            sink.borrow_mut().push(event.changes.clone());
        });
        chain.host.write(1, Some("pending"), ScopeId(1));
        for view in [&chain.host, &chain.guests[0], &chain.guests[1]] {
            assert_eq!(view.overlay_len(), 0, "a rejected overlay was resurrected");
            assert_eq!(view.visible(&1), None);
        }
        assert_eq!(
            *events.borrow(),
            vec![
                vec![change(1, None, Some("pending"))],
                vec![change(1, Some("pending"), None)],
            ]
        );
    }
}

#[test]
fn reentrant_close_is_the_last_notification_on_every_hop() {
    for close_at_host in [true, false] {
        let chain = chain(OverlayPolicy::Replicated);
        let host = chain.host.clone();
        let observer = if close_at_host {
            &chain.host
        } else {
            &chain.guests[0]
        };
        let _closer = observer.subscribe(move |event| {
            if !event.closed {
                host.close();
            }
        });
        let mut observations = Vec::new();
        for view in [&chain.host, &chain.guests[0], &chain.guests[1]] {
            let events = Rc::new(RefCell::new(Vec::new()));
            let sink = events.clone();
            let listener = view.subscribe(move |event| {
                sink.borrow_mut()
                    .push((event.closed, event.changes.clone()));
            });
            observations.push((events, listener));
        }
        chain.host.apply(base_batch(1, vec![upsert(1, 1, "base")]));
        for view in [&chain.host, &chain.guests[0], &chain.guests[1]] {
            assert!(view.is_closed());
            assert_eq!(view.visible(&1), None);
        }
        for (events, _listener) in observations {
            assert_eq!(
                *events.borrow(),
                vec![
                    (false, vec![change(1, None, Some("base"))]),
                    (true, vec![change(1, Some("base"), None)]),
                ]
            );
        }
    }
}

// ── Close and lifecycle edges ──

#[test]
#[should_panic(expected = "write on a closed view")]
fn write_on_a_closed_view_panics() {
    let h = Harness::new(OverlayPolicy::Replicated);
    h.view.close();
    h.view.write(1, Some("late"), ScopeId(7));
}

#[test]
#[should_panic(expected = "write on a closed view")]
fn closing_an_upstream_closes_its_consumers_before_any_relayed_write() {
    // Close cascades synchronously to consumers: by the time the guest could relay a write
    // upstream, it is itself closed, so the write hits the guest's own closed guard first.
    let chain = chain(OverlayPolicy::Replicated);
    chain.host.close();
    assert!(chain.guests[0].is_closed());
    chain.guests[0].write(1, Some("late"), ScopeId(7));
}

#[test]
fn attaching_to_a_closed_upstream_closes_the_consumer_immediately() {
    // The registration handshake must not leave a consumer hanging on a dead upstream:
    // attach observes the closed hop and closes the registrant in the same call.
    let upstream = View::<K, V>::authority(OverlayPolicy::Replicated, 1);
    upstream.close();
    let consumer = View::<K, V>::consumer(OverlayPolicy::Replicated, 1, Box::new(upstream));
    assert!(consumer.is_closed());
    assert_eq!(consumer.base_len(), 0);
}

#[test]
fn closing_twice_notifies_a_single_closed_event() {
    let h = Harness::new(OverlayPolicy::Replicated);
    h.view.apply(base_batch(1, vec![upsert(1, 1, "one")]));
    h.view.close();
    h.view.close();

    let closed: Vec<_> = h
        .events()
        .into_iter()
        .filter(|event| event.closed)
        .collect();
    assert_eq!(closed.len(), 1);
    assert_eq!(closed[0].changes, vec![change(1, Some("one"), None)]);
    // A closed hop is inert: a late batch is not applied and not published.
    let late = h.view.apply(base_batch(2, vec![upsert(2, 1, "ghost")]));
    assert!(late.is_empty());
    assert_eq!(h.view.visible(&2), None);
    assert_eq!(h.view.incarnation(), 1);
}

#[test]
fn releasing_an_unknown_demand_is_a_noop() {
    let h = Harness::new(OverlayPolicy::Replicated);
    h.view.release(DemandId(9999));
    assert!(h.recording.borrow().releases.is_empty());
}
