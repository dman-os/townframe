use super::*;
use big_sync_core::rpc::{
    BUCKET_LIVE_FP_SEED, BucketObjPageEntry, BucketSummary, LeafBucketPage, LeafBucketRequest,
    LeafBucketsRequest, ListPartsError, PartEvent, PartPage, SubPartsRequest, SubscriptionTarget,
};
use big_sync_core::{Fingerprint, FingerprintSeed};
use keyhive_core::access::Access;
use tokio::time::{Duration, timeout};

#[cfg(test)]
use crate::test_support::ObservedStore;

use super::contract::grant_bucket_read;

#[async_trait]
pub trait HostPartStoreContractHarness {
    fn store(&self) -> &dyn HostPartStore;
}

fn test_part(seed: u8) -> PartKey {
    PartKey(ByteKey::new([seed; 32]))
}

fn test_obj(seed: u8) -> ObjKey {
    let mut bytes = [0; 32];
    bytes[0] = seed;
    ObjKey(ByteKey::new(bytes))
}

fn payload(tag: &'static str, idx: u64) -> ObjPayload {
    serde_json::json!({
        "tag": tag,
        "idx": idx,
    })
}

/// An object key that lands in `bucket_id` at `bucket_id`'s own level.
///
/// The index is a hash of the key (ADR 012 decision 1), so a key can no longer be
/// assembled to fall in a chosen bucket; it has to be searched for, which costs about
/// `ARITY^level` tries — ~16 at level 1, ~65k at the deepest level.
fn obj_in_bucket(bucket_id: BuckId, salt: u8) -> ObjKey {
    let mut matches: u8 = 0;
    let mut counter: u32 = 0;
    loop {
        let mut bytes = [0u8; 32];
        bytes[..4].copy_from_slice(&counter.to_be_bytes());
        let obj_id = ObjKey(ByteKey::new(bytes));
        if BuckId::from_obj_key(bucket_id.level(), &obj_id) == bucket_id {
            matches += 1;
            // Returning the `salt`-th match keeps keys for one bucket ordered by salt,
            // which is the obj_id order the leaf page is asserted against.
            if matches == salt {
                return obj_id;
            }
        }
        counter = counter
            .checked_add(1)
            .expect("some key must hash into the requested bucket");
    }
}

/// An object key of `key_len` bytes that lands in `bucket_id` at its own level.
///
/// [`obj_in_bucket`]'s wide twin: only the leading counter bytes are searched and the tail
/// is filler, so a key's *width* is a parameter while the bucket it lands in is still found
/// rather than assumed.
fn wide_obj_in_bucket(bucket_id: BuckId, key_len: usize, salt: u8) -> ObjKey {
    assert!(key_len > 4, "the counter needs room in front of the filler");
    let mut matches: u8 = 0;
    let mut counter: u32 = 0;
    loop {
        let mut bytes = vec![0u8; key_len];
        bytes[..4].copy_from_slice(&counter.to_be_bytes());
        let obj_id = ObjKey::new(bytes);
        if BuckId::from_obj_key(bucket_id.level(), &obj_id) == bucket_id {
            matches += 1;
            if matches == salt {
                return obj_id;
            }
        }
        counter = counter
            .checked_add(1)
            .expect("some key must hash into the requested bucket");
    }
}

/// A 32-byte key that lands in `bucket_id` and sorts strictly after `after`.
///
/// The keyset page resumes at `obj_id > after_id`, so pinning "the byte budget
/// stops the page" needs a row the budget rejected to be followed by a narrower
/// row that would fit; only a key ordered after the wide one makes skipping it
/// observable. The search walks counters upward, so the first match is the next
/// key in `obj_id` order.
fn narrow_obj_after(bucket_id: BuckId, after: &ObjKey) -> ObjKey {
    let mut counter: u32 = 0;
    loop {
        let mut bytes = [0u8; 32];
        bytes[..4].copy_from_slice(&counter.to_be_bytes());
        let obj_id = ObjKey::new(bytes);
        if BuckId::from_obj_key(bucket_id.level(), &obj_id) == bucket_id && obj_id > *after {
            return obj_id;
        }
        counter = counter.checked_add(1).expect("some key must sort after");
    }
}

async fn seed_live_obj<S>(
    store: &S,
    obj_id: ObjKey,
    payload: ObjPayload,
    parts: &[PartKey],
) -> Res<()>
where
    S: HostPartStore + Sync + ?Sized,
{
    store
        .set_obj_payload(obj_id.clone(), payload.clone())
        .await?;
    assert_eq!(
        store.obj_payload(obj_id.clone()).await?,
        Some(payload.clone())
    );
    if !parts.is_empty() {
        store.add_obj_to_parts(obj_id, parts.to_vec()).await?;
    }
    Ok(())
}

fn assert_member_touched(
    event: &PartEvent,
    cursor: CursorIndex,
    part_id: PartKey,
    obj_id: ObjKey,
    payload: ObjPayload,
) {
    let PartEvent::Changed(transition) = event else {
        panic!("expected a member write, which arrives as the part's touch");
    };
    assert_eq!(transition.cursor, cursor);
    assert_eq!(transition.part_ids, vec![part_id]);
    assert_eq!(transition.obj_id, obj_id);
    assert_eq!(transition.payload, payload);
}

/// A test view over a durable revision reader. Events are exposed one at a time,
/// while replay completion remains the reader's explicit phase boundary rather
/// than being fabricated as an event.
pub(crate) struct TestEventStream {
    reader: tokio::sync::Mutex<Box<dyn LocalPartRevisionReader>>,
    pending: tokio::sync::Mutex<std::collections::VecDeque<PartEvent>>,
    pending_revision: tokio::sync::Mutex<Option<FrontierRevision>>,
}

impl TestEventStream {
    fn new(reader: Box<dyn LocalPartRevisionReader>) -> Self {
        Self {
            reader: tokio::sync::Mutex::new(reader),
            pending: tokio::sync::Mutex::new(std::collections::VecDeque::new()),
            pending_revision: tokio::sync::Mutex::new(None),
        }
    }

    pub(crate) async fn next_read(&self) -> Res<RevisionRead<FrontierRevision, PartEvent>> {
        let mut pending = self.pending.lock().await;
        if !pending.is_empty() {
            let revision = self
                .pending_revision
                .lock()
                .await
                .take()
                .expect("pending events carry their revision");
            let entries = pending.drain(..).collect();
            return Ok(RevisionRead::Entries { revision, entries });
        }
        drop(pending);
        self.reader
            .lock()
            .await
            .next(RevisionReadLimits::default())
            .await
    }

    pub(crate) async fn next(&self) -> Res<PartEvent> {
        loop {
            match self.next_read().await? {
                RevisionRead::Entries {
                    revision,
                    mut entries,
                } => {
                    if !entries.is_empty() {
                        let event = entries.remove(0);
                        self.pending.lock().await.extend(entries);
                        *self.pending_revision.lock().await = Some(revision);
                        return Ok(event);
                    }
                }
                RevisionRead::ReplayComplete { .. } => continue,
            }
        }
    }
}

/// The event-stream view of the pull reader, for these tests only. Production has
/// no subscription: a peer-facing page is a read plus the responder's verdict, and
/// a local consumer drives the reader directly. The `subscriber` argument is kept
/// for the call sites' shape and deliberately not used — the reader is the store's
/// unfiltered seam, so a test about authorization asserts the answer the responder
/// asks for (`read_denied`, `permitted_parts`) instead of filtering a stream.
#[async_trait]
pub trait SingleTargetPageStore: HostPartStore {
    async fn replay_page_for_target(
        &self,
        target: SubscriptionTarget,
        limit: u32,
        subscriber: PeerKey,
        hold: Duration,
    ) -> Res<ReplayPage> {
        self.replay_page_round(
            ReplayPageRequest {
                session_id: big_sync_core::rpc::ReplaySessionId(0),
                request_id: big_sync_core::rpc::ReplayRequestId(0),
                supersede: None,
                targets: vec![target],
                limit,
                hold_ms: u32::try_from(hold.as_millis()).unwrap_or(u32::MAX),
            },
            subscriber,
            hold,
            CancellationToken::new(),
        )
        .await
    }
}

impl<S: HostPartStore + ?Sized> SingleTargetPageStore for S {}

#[async_trait]
pub(crate) trait PageEventStore: HostPartStore {
    async fn page_events(
        &self,
        reqs: SubPartsRequest,
        subscriber: PeerKey,
    ) -> Res<Result<TestEventStream, ListPartsError>>;

    async fn page_events_local(
        &self,
        reqs: SubPartsRequest,
    ) -> Res<Result<TestEventStream, ListPartsError>>;
}

#[async_trait]
impl<S: HostPartStore + ?Sized> PageEventStore for S {
    async fn page_events(
        &self,
        reqs: SubPartsRequest,
        _subscriber: PeerKey,
    ) -> Res<Result<TestEventStream, ListPartsError>> {
        Ok(self.open_page_reader(reqs).await?.map(TestEventStream::new))
    }

    async fn page_events_local(
        &self,
        reqs: SubPartsRequest,
    ) -> Res<Result<TestEventStream, ListPartsError>> {
        Ok(self
            .open_revision_reader(reqs)
            .await?
            .map(TestEventStream::new))
    }
}

async fn recv_sub_event(stream: &TestEventStream) -> Res<PartEvent> {
    timeout(Duration::from_secs(5), stream.next()).await?
}

pub(crate) async fn wait_replay_complete(stream: &TestEventStream) -> Res<()> {
    loop {
        match timeout(Duration::from_secs(5), stream.next_read()).await?? {
            RevisionRead::Entries { .. } => {}
            RevisionRead::ReplayComplete { .. } => return Ok(()),
        }
    }
}

fn events_page(page: &ReplayPage) -> PartPage {
    assert_eq!(page.targets.len(), 1, "test page names one target");
    let TargetVerdict::Events { resume, drained } = page.targets[0].1 else {
        panic!("expected an events verdict, got {:?}", page.targets[0].1);
    };
    PartPage {
        events: page.events.clone(),
        resume,
        drained,
    }
}

fn verdict(page: &ReplayPage) -> &TargetVerdict {
    assert_eq!(page.targets.len(), 1, "test page names one target");
    &page.targets[0].1
}

pub(crate) async fn collect_sub_events(stream: &TestEventStream) -> Res<Vec<PartEvent>> {
    let mut out = Vec::new();
    while let RevisionRead::Entries { entries, .. } =
        timeout(Duration::from_secs(5), stream.next_read()).await??
    {
        out.extend(entries);
    }
    Ok(out)
}

#[tokio::test]
async fn replay_page_rejects_duplicate_targets() -> Res<()> {
    let store = crate::part_store::memory::MemoryPartStore::new();
    let target = SubscriptionTarget::Object {
        obj_id: test_obj(1),
        cursor: 0,
    };
    let same_route = SubscriptionTarget::Object {
        obj_id: test_obj(1),
        cursor: 1,
    };
    let err = store
        .replay_page_round(
            ReplayPageRequest {
                session_id: big_sync_core::rpc::ReplaySessionId(0),
                request_id: big_sync_core::rpc::ReplayRequestId(0),
                supersede: None,
                targets: vec![target, same_route],
                limit: 1,
                hold_ms: 0,
            },
            PeerKey::new([0; 32]),
            Duration::ZERO,
            CancellationToken::new(),
        )
        .await
        .expect_err("duplicate targets are invalid request input");
    assert!(err.to_string().contains("logical route"));
    Ok(())
}

#[test]
fn merge_page_events_collapses_same_object_revision() {
    let obj = test_obj(7);
    let part_a = test_part(8);
    let part_b = test_part(9);
    let mut events = vec![PartEvent::Changed(ObjChanged {
        cursor: 4,
        part_ids: vec![part_a.clone()],
        obj_id: obj.clone(),
        payload: payload("merge", 4),
    })];
    assert!(!merge_part_event(
        &mut events,
        PartEvent::Changed(ObjChanged {
            cursor: 4,
            part_ids: vec![part_b.clone()],
            obj_id: obj,
            payload: payload("merge", 4),
        })
    ));
    assert!(matches!(
        events.as_slice(),
        [PartEvent::Changed(ObjChanged { part_ids, .. })]
            if part_ids == &vec![part_a, part_b]
    ));
}

#[tokio::test]
async fn replay_page_enforces_global_limit_and_merges_ordered_overlap() -> Res<()> {
    let store = crate::part_store::memory::MemoryPartStore::new();
    let peer = PeerKey::new([1; 32]);
    let part_a = test_part(2);
    let part_b = test_part(3);
    let first = test_obj(4);
    let second = test_obj(5);
    let shared = test_obj(6);
    for part in [&part_a, &part_b] {
        store.ensure_part(part.clone()).await?;
        store
            .set_part_members(part.clone(), HashMap::from([(peer.clone(), Access::Read)]))
            .await?;
    }
    for (obj, part, tag, index) in [
        (first.clone(), part_a.clone(), "first", 1),
        (second.clone(), part_b.clone(), "second", 2),
    ] {
        store
            .set_obj_payload(obj.clone(), payload(tag, index))
            .await?;
        store.add_obj_to_parts(obj, vec![part]).await?;
    }
    store
        .set_obj_payload(shared.clone(), payload("shared", 3))
        .await?;
    store
        .add_obj_to_parts(shared.clone(), vec![part_a.clone(), part_b.clone()])
        .await?;

    let target_a = SubscriptionTarget::Part {
        part_id: part_a.clone(),
        cursor: 0,
    };
    let target_b = SubscriptionTarget::Part {
        part_id: part_b.clone(),
        cursor: 0,
    };
    let limited = store
        .replay_page_round(
            ReplayPageRequest {
                session_id: big_sync_core::rpc::ReplaySessionId(0),
                request_id: big_sync_core::rpc::ReplayRequestId(0),
                supersede: None,
                targets: vec![target_b.clone(), target_a.clone()],
                limit: 1,
                hold_ms: 0,
            },
            peer.clone(),
            Duration::ZERO,
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(
        limited.events.len(),
        1,
        "the global page limit applies across targets"
    );

    let overlapping = store
        .replay_page_round(
            ReplayPageRequest {
                session_id: big_sync_core::rpc::ReplaySessionId(0),
                request_id: big_sync_core::rpc::ReplayRequestId(1),
                supersede: None,
                targets: vec![
                    target_b,
                    target_a,
                    SubscriptionTarget::Object {
                        obj_id: shared.clone(),
                        cursor: 0,
                    },
                ],
                limit: 32,
                hold_ms: 0,
            },
            peer,
            Duration::ZERO,
            CancellationToken::new(),
        )
        .await?;
    assert!(
        overlapping
            .events
            .windows(2)
            .all(|events| events[0].cursor() <= events[1].cursor()),
        "page events are globally ordered: {:?}",
        overlapping.events
    );
    assert!(
        overlapping.events.iter().any(|event| {
            matches!(event, PartEvent::Changed(changed) if changed.obj_id == first)
        }),
        "the first target contributes an event"
    );
    assert!(
        overlapping.events.iter().any(|event| {
            matches!(event, PartEvent::Changed(changed) if changed.obj_id == second)
        }),
        "the second target contributes an event"
    );
    let shared_events: Vec<_> = overlapping
        .events
        .iter()
        .filter_map(|event| match event {
            PartEvent::Changed(changed) if changed.obj_id == shared => Some(changed),
            _ => None,
        })
        .collect();
    assert_eq!(
        shared_events.len(),
        2,
        "different revisions remain distinct object events"
    );
    assert!(
        shared_events
            .iter()
            .any(|event| event.part_ids == vec![part_a.clone(), part_b.clone()]),
        "the overlapping part route contributes both projected parts"
    );
    Ok(())
}

#[tokio::test]
async fn replay_page_cancelled_before_read_returns_cursor_verdict() -> Res<()> {
    let store = crate::part_store::memory::MemoryPartStore::new();
    let part_id = test_part(1);
    let peer = PeerKey::new([0; 32]);
    store.ensure_part(part_id.clone()).await?;
    store
        .set_part_members(
            part_id.clone(),
            HashMap::from([(peer.clone(), Access::Read)]),
        )
        .await?;
    let target = SubscriptionTarget::Part {
        part_id,
        cursor: 42,
    };
    let cancel = CancellationToken::new();
    cancel.cancel();
    let page = store
        .replay_page_round(
            ReplayPageRequest {
                session_id: big_sync_core::rpc::ReplaySessionId(0),
                request_id: big_sync_core::rpc::ReplayRequestId(0),
                supersede: None,
                targets: vec![target.clone()],
                limit: 1,
                hold_ms: 0,
            },
            peer,
            Duration::ZERO,
            cancel,
        )
        .await?;
    assert!(page.events.is_empty());
    assert!(matches!(
        page.verdict(&target),
        Some(TargetVerdict::Events {
            resume: 42,
            drained: false
        })
    ));
    Ok(())
}

#[tokio::test]
async fn replay_page_byte_budget_stops_at_an_atomic_revision() -> Res<()> {
    let store = crate::part_store::memory::MemoryPartStore::new();
    let part = test_part(90);
    let obj = test_obj(91);
    let peer = PeerKey::new([92; 32]);
    store.ensure_part(part.clone()).await?;
    store
        .set_part_members(part.clone(), HashMap::from([(peer.clone(), Access::Read)]))
        .await?;
    store
        .set_obj_payload(obj.clone(), serde_json::json!({"data": "a".repeat(40_000)}))
        .await?;
    store
        .add_obj_to_parts(obj.clone(), vec![part.clone()])
        .await?;
    store
        .set_obj_payload(obj.clone(), serde_json::json!({"data": "b".repeat(40_000)}))
        .await?;

    let page = store
        .replay_page_for_target(
            SubscriptionTarget::Part {
                part_id: part,
                cursor: 0,
            },
            32,
            peer,
            Duration::ZERO,
        )
        .await?;
    assert_eq!(page.events.len(), 1, "the byte budget admits one revision");
    assert!(page.encoded_size().map_err(|error| eyre::eyre!(error))? <= ReplayPage::BYTE_BUDGET);
    Ok(())
}

pub async fn assert_host_part_store_contract<H>(harness: &H) -> Res<()>
where
    H: HostPartStoreContractHarness + Sync,
{
    assert_summarize_parts_contract(harness).await?;
    assert_payload_can_trail_membership_contract(harness).await?;
    assert_changed_buckets_contract(harness).await?;
    assert_leaf_buckets_contract(harness).await?;
    assert_list_events_contract(harness).await?;
    assert_subscribe_contract(harness).await?;
    assert_readable_subscribe_contract(harness).await?;
    assert_subscribe_replay_filtering_contract(harness).await?;
    assert_subscription_semantics_contract(harness).await?;
    assert_subscribe_live_filtering_contract(harness).await?;
    assert_subscribe_per_part_cursor_contract(harness).await?;
    assert_list_events_pagination_contract(harness).await?;
    assert_peer_cursor_monotonicity_contract(harness).await?;
    assert_obj_occupancy_contract(harness).await?;
    assert_remove_obj_advances_latest_cursor_contract(harness).await?;
    assert_list_events_page_verdict_contract(harness).await?;
    assert_local_revision_reader_contract(harness).await?;
    assert_local_revision_reader_all_contract(harness).await?;
    assert_latest_revision_is_a_read_contract(harness).await?;
    assert_repeated_payload_write_is_not_a_change_contract(harness).await?;
    assert_page_outcome_contract(harness).await?;
    assert_object_route_resumes_from_its_cursor(harness).await?;
    assert_zero_page_limit_carries_no_events(harness).await?;
    assert_bucket_limit_hints_agree_across_endpoints(harness).await?;
    assert_subscribing_allocates_no_revision_contract(harness).await?;
    assert_payload_survives_membership_removal_contract(harness).await?;
    assert_tombstone_added_at_contract(harness).await?;
    assert_subscribe_part_target_bounds_are_per_target_contract(harness).await?;
    assert_object_lane_carries_no_membership_contract(harness).await?;
    assert_mixed_part_and_object_subscription_cursors_are_independent(harness).await?;
    Ok(())
}

/// `latest_revision` is a read: it reports the newest allocated revision and never
/// allocates one of its own, so two consecutive reads agree. A store that allocated here
/// would advance the cursor space on reads alone, which no write can account for.
pub async fn assert_latest_revision_is_a_read_contract<H>(harness: &H) -> Res<()>
where
    H: HostPartStoreContractHarness + Sync,
{
    let store = harness.store();
    let first = store.latest_revision().await?;
    let second = store.latest_revision().await?;
    assert_eq!(
        first, second,
        "reading the latest revision must not allocate a revision"
    );
    Ok(())
}

/// Re-writing the payload an object already holds, with its membership already live and no
/// membership transition riding along, is not a change: no revision is allocated and no part
/// cursor moves.
///
/// This is the presence plane's steady state, not an edge case. Its write edge re-announces
/// every blob the node holds on boot, so a store that reserved a revision per repeat put every
/// held blob object back on the wire on every restart. `add_obj_to_parts` already refuses the
/// same repeat for a present membership; the payload write has to match it.
pub async fn assert_repeated_payload_write_is_not_a_change_contract<H>(harness: &H) -> Res<()>
where
    H: HostPartStoreContractHarness + Sync,
{
    let store = harness.store();
    let part = test_part(249);
    let obj = test_obj(250);
    let held = payload("repeat-is-not-a-change", 1);

    store.ensure_part(part.clone()).await?;
    seed_live_obj(
        store,
        obj.clone(),
        held.clone(),
        std::slice::from_ref(&part),
    )
    .await?;

    let revision_before = store.latest_revision().await?;
    let cursor_before = store
        .summarize_parts(HashSet::from([part.clone()]))
        .await??
        .get(&part)
        .expect(ERROR_IMPOSSIBLE)
        .latest_cursor;

    store.set_obj_payload(obj.clone(), held.clone()).await?;

    assert_eq!(
        store.latest_revision().await?,
        revision_before,
        "repeating an object's payload must not allocate a revision"
    );
    assert_eq!(
        store
            .summarize_parts(HashSet::from([part.clone()]))
            .await??
            .get(&part)
            .expect(ERROR_IMPOSSIBLE)
            .latest_cursor,
        cursor_before,
        "repeating an object's payload must not move a part cursor"
    );
    assert_eq!(
        store.obj_payload(obj.clone()).await?,
        Some(held.clone()),
        "the repeat must leave the payload in place"
    );
    assert_eq!(
        store.obj_parts(obj.clone()).await?,
        vec![part.clone()],
        "the repeat must leave membership in place"
    );

    // Control: a payload the object does not hold is a change and must allocate a revision.
    // Without this leg, the assertions above would also pass for a store that never allocates.
    store
        .set_obj_payload(obj.clone(), payload("repeat-is-not-a-change", 2))
        .await?;
    assert!(
        store.latest_revision().await? > revision_before,
        "a changed payload must allocate a revision"
    );
    Ok(())
}

/// A subscribe writes nothing and emits nothing to any other subscriber.
///
/// Subscribing is not a sync event: it consumes no revision, creates no frontier entry,
/// writes no derived membership row, and hands no event to a subscriber that is already
/// caught up. The object route is the interesting one, because it resolves an object's
/// containing parts at read time rather than deriving a part for it, so this subscribes to
/// the object while the object lives in a part the subscriber and the observer both hold.
pub async fn assert_subscribing_allocates_no_revision_contract<H>(harness: &H) -> Res<()>
where
    H: HostPartStoreContractHarness + Sync,
{
    let store = harness.store();
    let part = test_part(247);
    let obj_id = test_obj(248);
    let subscriber = PeerKey::new([0xd0u8; 32]);
    let observer = PeerKey::new([0xd1u8; 32]);
    store.ensure_part(part.clone()).await?;
    store
        .set_part_members(
            part.clone(),
            HashMap::from([
                (subscriber.clone(), Access::Read),
                (observer.clone(), Access::Read),
            ]),
        )
        .await?;
    seed_live_obj(
        store,
        obj_id.clone(),
        payload("subscribe-writes-nothing", 1),
        std::slice::from_ref(&part),
    )
    .await?;

    let before = store.latest_revision().await?;
    let parts_before = store.obj_parts(obj_id.clone()).await?;

    // An observer that is already caught up: anything this subscribe emitted would reach it.
    let observer_rx = store
        .page_events(
            SubPartsRequest {
                lower_bound: before,
                targets: HashSet::from([SubscriptionTarget::Part {
                    part_id: part.clone(),
                    cursor: before,
                }]),
            },
            observer,
        )
        .await??;
    wait_replay_complete(&observer_rx).await?;

    let rx = store
        .page_events(
            SubPartsRequest {
                lower_bound: 0,
                targets: HashSet::from([SubscriptionTarget::Object {
                    obj_id: obj_id.clone(),
                    cursor: 0,
                }]),
            },
            subscriber,
        )
        .await??;
    wait_replay_complete(&rx).await?;

    assert_eq!(
        store.latest_revision().await?,
        before,
        "subscribing to an object must not allocate a revision"
    );
    assert_eq!(
        store.obj_parts(obj_id.clone()).await?,
        parts_before,
        "subscribing must not write a derived membership row"
    );
    let mut reader = store.open_revision_reader_all(before).await??;
    while let RevisionRead::Entries { entries, .. } = reader
        .next(RevisionReadLimits {
            max_entries: std::num::NonZeroUsize::new(1).expect("literal is non-zero"),
        })
        .await?
    {
        assert!(
            entries.is_empty(),
            "subscribing must not create a frontier entry: {entries:?}"
        );
    }
    match timeout(Duration::from_millis(100), observer_rx.next()).await {
        Err(_) => {}
        Ok(Ok(event)) => panic!("a subscribe emitted {event:?} to another subscriber"),
        Ok(Err(err)) => panic!("the observer's subscription closed: {err}"),
    }
    Ok(())
}

/// A payload is never dropped implicitly: removing the object from its last part keeps the
/// payload and leaves no membership, and `remove_obj_payload` is what clears it. Both
/// directions are asserted, together with the janitorial view that tells the two apart.
pub async fn assert_payload_survives_membership_removal_contract<H>(harness: &H) -> Res<()>
where
    H: HostPartStoreContractHarness + Sync,
{
    let store = harness.store();
    let part = test_part(251);
    let obj_id = test_obj(252);
    store.ensure_part(part.clone()).await?;
    let payload = payload("payload-outlives-membership", 1);
    seed_live_obj(
        store,
        obj_id.clone(),
        payload.clone(),
        std::slice::from_ref(&part),
    )
    .await?;
    assert!(
        !store
            .partless_objects(u32::MAX, None)
            .await?
            .contains(&obj_id),
        "a live member is not a GC candidate"
    );

    let before = store.part_store_stats().await?;
    store
        .remove_obj_from_part(obj_id.clone(), part.clone())
        .await?;
    assert_eq!(
        store.obj_payload(obj_id.clone()).await?,
        Some(payload),
        "removing the object from its last part must not drop the payload"
    );
    assert_eq!(store.obj_parts(obj_id.clone()).await?, Vec::new());
    assert_eq!(store.member_count(part.clone()).await?, 0);
    assert!(
        store
            .partless_objects(u32::MAX, None)
            .await?
            .contains(&obj_id),
        "a payload with no live membership is a GC candidate"
    );
    let after = store.part_store_stats().await?;
    assert_eq!(after.payload_objects, before.payload_objects);
    assert_eq!(after.payload_bytes, before.payload_bytes);
    assert_eq!(after.partless_objects, before.partless_objects + 1);
    assert_eq!(after.live_rows, before.live_rows - 1);
    assert_eq!(after.dead_rows, before.dead_rows + 1);

    store.remove_obj_payload(obj_id.clone()).await?;
    assert_eq!(
        store.obj_payload(obj_id.clone()).await?,
        None,
        "remove_obj_payload is the path that clears content"
    );
    assert_eq!(store.obj_parts(obj_id.clone()).await?, Vec::new());
    assert!(
        !store
            .partless_objects(u32::MAX, None)
            .await?
            .contains(&obj_id),
        "an object without a payload is not a GC candidate"
    );
    let cleared = store.part_store_stats().await?;
    assert_eq!(cleared.payload_objects, before.payload_objects - 1);
    assert_eq!(cleared.partless_objects, before.partless_objects);
    assert_eq!(
        cleared.dead_rows, after.dead_rows,
        "clearing the content leaves the tombstone"
    );
    // Idempotent: an object with no payload and an unknown object are both no-ops.
    store.remove_obj_payload(obj_id.clone()).await?;
    store.remove_obj_payload(test_obj(253)).await?;
    assert_eq!(store.obj_payload(obj_id).await?, None);

    // Keyset pagination over whatever the scope holds: ordered, one key per page, and the
    // page after `after` is the next key.
    let all = store.partless_objects(u32::MAX, None).await?;
    assert!(
        all.windows(2).all(|pair| pair[0] < pair[1]),
        "partless objects come back ordered by obj_id"
    );
    let first = store.partless_objects(1, None).await?;
    assert_eq!(first, all.iter().take(1).cloned().collect::<Vec<_>>());
    assert_eq!(
        store.partless_objects(1, first.first().cloned()).await?,
        all.iter().skip(1).take(1).cloned().collect::<Vec<_>>()
    );
    Ok(())
}

/// A `Removed` at cursor `T` reaches a reader at cursor `c` exactly when
/// `added_at <= c < T`: a removal for a member the reader never saw as present carries no
/// information for it, and one whose add it did see has to arrive. Two subscribers at
/// different cursors pin both directions.
pub async fn assert_tombstone_added_at_contract<H>(harness: &H) -> Res<()>
where
    H: HostPartStoreContractHarness + Sync,
{
    let store = harness.store();
    let part = test_part(254);
    let obj_id = test_obj(255);
    let reader_before = PeerKey::new([0xd2u8; 32]);
    let reader_after = PeerKey::new([0xd3u8; 32]);
    store.ensure_part(part.clone()).await?;
    store
        .set_part_members(
            part.clone(),
            HashMap::from([
                (reader_before.clone(), Access::Read),
                (reader_after.clone(), Access::Read),
            ]),
        )
        .await?;
    seed_live_obj(
        store,
        obj_id.clone(),
        payload("added-at", 1),
        std::slice::from_ref(&part),
    )
    .await?;

    // The member's own transition is its add, so the cursor it is reported at is the stamp.
    let added_at = store
        .list_events(HashSet::from([part.clone()]), 0, 8)
        .await??
        .get(&part)
        .expect(ERROR_IMPOSSIBLE)
        .events
        .iter()
        .map(|event| match event {
            PartEvent::Changed(changed) => changed.cursor,
            PartEvent::Removed(removed) => removed.cursor,
        })
        .max()
        .expect(ERROR_IMPOSSIBLE);
    store
        .remove_obj_from_part(obj_id.clone(), part.clone())
        .await?;
    let removed_at = store
        .list_events(HashSet::from([part.clone()]), added_at, 8)
        .await??
        .get(&part)
        .expect(ERROR_IMPOSSIBLE)
        .events
        .iter()
        .map(|event| match event {
            PartEvent::Changed(changed) => changed.cursor,
            PartEvent::Removed(removed) => removed.cursor,
        })
        .max()
        .expect(ERROR_IMPOSSIBLE);
    assert!(
        removed_at > added_at,
        "the tombstone cursor is newer than the add it removes"
    );

    // The read applies the rule, so a page whose request started before the add is not
    // handed the removal — the same answer the target page below gives, because it is the
    // same read rather than two paths.
    let before = store
        .page_events(
            SubPartsRequest {
                lower_bound: 0,
                targets: HashSet::from([SubscriptionTarget::Part {
                    part_id: part.clone(),
                    cursor: 0,
                }]),
            },
            reader_before.clone(),
        )
        .await??;
    let before_events = collect_sub_events(&before).await?;
    assert!(
        !before_events.iter().any(|event| matches!(
            event,
            PartEvent::Removed(removed) if removed.cursor == removed_at
        )),
        "a request that started before the add is not handed the removal; got {before_events:?}"
    );

    // The peer-facing rule: a `Removed` reaches a peer only when the request started at
    // or after the add, and the rule uses the request's own cursor rather than the
    // reader's advancing position.
    let before_add = store
        .replay_page_for_target(
            SubscriptionTarget::Part {
                part_id: part.clone(),
                cursor: 0,
            },
            8,
            reader_before,
            Duration::from_millis(50),
        )
        .await?;
    let before_add = events_page(&before_add);
    assert!(
        before_add.events.is_empty(),
        "a peer whose request started before the add is not told about the removal; got {:?}",
        before_add.events
    );
    let at_add = store
        .replay_page_for_target(
            SubscriptionTarget::Part {
                part_id: part.clone(),
                cursor: added_at,
            },
            8,
            reader_after,
            Duration::from_millis(50),
        )
        .await?;
    let at_add = events_page(&at_add);
    assert!(
        at_add.events.iter().any(|event| matches!(
            event,
            PartEvent::Removed(removed) if removed.cursor == removed_at
        )),
        "a peer whose request started at the add is handed the removal; got {:?}",
        at_add.events
    );
    Ok(())
}

/// The object lane carries content only: a membership transition is a part-lane fact, so
/// removing the object from a part yields no event on the object lane — least of all a
/// payload-less `Changed`, which means *resolve the membership* and books as content.
pub async fn assert_object_lane_carries_no_membership_contract<H>(harness: &H) -> Res<()>
where
    H: HostPartStoreContractHarness + Sync,
{
    let store = harness.store();
    let part_a = test_part(0xe0);
    let part_b = test_part(0xe1);
    let obj_id = test_obj(0xe2);
    let peer = PeerKey::new([0xe3u8; 32]);
    for part in [part_a.clone(), part_b.clone()] {
        store.ensure_part(part.clone()).await?;
        store
            .set_part_members(part, HashMap::from([(peer.clone(), Access::Read)]))
            .await?;
    }
    seed_live_obj(
        store,
        obj_id.clone(),
        payload("object-lane", 0),
        &[part_a.clone(), part_b.clone()],
    )
    .await?;
    let rx = store
        .page_events(
            SubPartsRequest {
                lower_bound: 0,
                targets: HashSet::from([SubscriptionTarget::Object {
                    obj_id: obj_id.clone(),
                    cursor: 0,
                }]),
            },
            peer,
        )
        .await??;
    wait_replay_complete(&rx).await?;

    // Content does reach the object lane, so the silence below is about membership rather
    // than about a subscription that is not delivering anything at all. A store is free to
    // report the object's one change once or once per part it names, so this waits for the
    // payload rather than counting events.
    store
        .set_obj_payload(obj_id.clone(), payload("object-lane", 1))
        .await?;
    let mut saw_content = false;
    while !saw_content {
        match recv_sub_event(&rx).await? {
            PartEvent::Changed(changed) => {
                assert_eq!(changed.obj_id, obj_id);
                assert!(
                    changed.part_ids.is_empty(),
                    "an object page reports content, not membership: {:?}",
                    changed.part_ids
                );
                saw_content = changed.payload == payload("object-lane", 1);
            }
            PartEvent::Removed(removed) => {
                panic!("the object lane must not carry a membership removal, got {removed:?}")
            }
        }
    }

    for part in [part_a, part_b] {
        store
            .remove_obj_from_part(obj_id.clone(), part.clone())
            .await?;
        match timeout(Duration::from_millis(100), rx.next()).await {
            Err(_) => {}
            Ok(Ok(event)) => panic!(
                "removing {part} emitted {event:?} on the object lane, which carries content only"
            ),
            Ok(Err(err)) => panic!("the object subscription closed: {err}"),
        }
    }
    Ok(())
}

/// An object route must resume from the cursor the client sends.
///
/// The route has no part cursor of its own, so a store that ignores the position
/// it is handed replays the object's events from the start on every page: the
/// caller re-issues, reads the same first page again, and never reaches the
/// second. One object with two changes and a page limit of one is therefore two
/// pages — but only if the position travels with the route.
pub async fn assert_object_route_resumes_from_its_cursor<H>(harness: &H) -> Res<()>
where
    H: HostPartStoreContractHarness + Sync,
{
    let store = harness.store();
    let part = test_part(240);
    let obj = test_obj(241);
    let member = big_sync_core::PeerKey::new([242u8; 32]);

    store.ensure_part(part.clone()).await?;
    store
        .set_part_members(
            part.clone(),
            std::collections::HashMap::from([(member.clone(), Access::Read)]),
        )
        .await?;
    // Two changes to the same member object, so its events span two cursors with
    // distinguishable payloads.
    seed_live_obj(
        store,
        obj.clone(),
        payload("object-route", 0),
        std::slice::from_ref(&part),
    )
    .await?;
    store
        .set_obj_payload(obj.clone(), payload("object-route", 1))
        .await?;

    /// One page of an object route. The subscription a page drains is produced by
    /// a spawned task, so a page can come back empty before that task delivered
    /// anything; re-issuing from the returned cursor is how a caller resumes.
    async fn object_page(
        store: &dyn HostPartStore,
        obj: &ObjKey,
        cursor: CursorIndex,
        member: &big_sync_core::PeerKey,
    ) -> Res<big_sync_core::rpc::PartPage> {
        const PAGE_HOLD: Duration = Duration::from_millis(50);
        const ATTEMPTS: u8 = 8;
        for _ in 0..ATTEMPTS {
            let outcome = store
                .replay_page_for_target(
                    SubscriptionTarget::Object {
                        obj_id: obj.clone(),
                        cursor,
                    },
                    1,
                    member.clone(),
                    PAGE_HOLD,
                )
                .await?;
            let page = events_page(&outcome);
            if !page.events.is_empty() {
                return Ok(page);
            }
        }
        panic!("an object route with buffered events produced no page");
    }

    fn changed(event: &PartEvent) -> &big_sync_core::rpc::ObjChanged {
        let PartEvent::Changed(changed) = event else {
            panic!("an object's own events arrive as changes, got {event:?}");
        };
        changed
    }

    let first = object_page(store, &obj, 0, &member).await?;
    let first_cursor = changed(&first.events[0]).cursor;

    let second = object_page(store, &obj, first_cursor, &member).await?;
    let second_cursor = changed(&second.events[0]).cursor;

    // A store that ignores the cursor it is handed answers the same first page
    // again, so this is the assertion that separates resuming from repeating.
    // (The payload cannot carry it: a store may report the object's current
    // payload on every event.)
    assert!(
        second_cursor > first_cursor,
        "resuming at cursor {first_cursor} must read the object's next event, not the same page again, got {second_cursor}",
    );
    Ok(())
}

/// `limit` is an upper bound on the events a page may carry, so a zero limit
/// carries none — whatever happens to be waiting behind it. The bound is applied
/// before anything is emitted: comparing it after a push made a zero-limit page
/// hand back one event whenever one was buffered, which is a bound only by
/// accident.
///
/// A page that carried nothing must not claim caught-up either, since nothing was
/// drained: the caller's own resume point comes back, so a caller that asked for
/// no events learns that instead of learning that it is done.
pub async fn assert_zero_page_limit_carries_no_events<H>(harness: &H) -> Res<()>
where
    H: HostPartStoreContractHarness + Sync,
{
    let store = harness.store();
    let part = test_part(243);
    let obj = test_obj(244);
    let member = big_sync_core::PeerKey::new([245u8; 32]);

    store.ensure_part(part.clone()).await?;
    store
        .set_part_members(
            part.clone(),
            std::collections::HashMap::from([(member.clone(), Access::Read)]),
        )
        .await?;
    // Something for a store that ignores the bound to hand back.
    seed_live_obj(
        store,
        obj.clone(),
        payload("zero-limit", 0),
        std::slice::from_ref(&part),
    )
    .await?;
    store
        .set_obj_payload(obj.clone(), payload("zero-limit", 1))
        .await?;

    for attempt in 0..4 {
        let outcome = store
            .replay_page_for_target(
                SubscriptionTarget::Part {
                    part_id: part.clone(),
                    cursor: 0,
                },
                0,
                member.clone(),
                Duration::from_millis(200),
            )
            .await?;
        let page = events_page(&outcome);
        assert!(
            page.events.is_empty(),
            "a zero limit must carry no events, attempt {attempt} got {}",
            page.events.len()
        );
        assert!(
            !page.drained,
            "a page that carried nothing must not claim caught-up"
        );
        assert_eq!(
            page.resume, 0,
            "a page that carried nothing resumes from the caller's own cursor"
        );
    }

    // The bound is what suppressed them, not an empty part: a page of one does
    // hand an event back. A store's replay runs on a spawned subscription, so
    // re-issuing is how a caller waits for it to deliver.
    let mut delivered = 0;
    for _ in 0..8 {
        let outcome = store
            .replay_page_for_target(
                SubscriptionTarget::Part {
                    part_id: part.clone(),
                    cursor: 0,
                },
                1,
                member.clone(),
                Duration::from_millis(200),
            )
            .await?;
        let page = events_page(&outcome);
        if !page.events.is_empty() {
            delivered = page.events.len();
            break;
        }
    }
    assert_eq!(delivered, 1, "a page of one carries exactly one event");
    Ok(())
}

/// The two bucket endpoints read their `limit_hint` differently, and every store
/// must agree on which is which.
///
/// `GetChangedBucketsRequest::limit_hint` is documented as the response's page
/// bound, with `BuckId::ARITY` extra siblings allowed, so a zero hint is a
/// zero-bucket page. `LeafBucketsRequest::limit_hint` is documented as a hint
/// rather than a bound, so zero means no preference and the smallest useful leaf
/// page is one entry — an empty page there would tell a pager it is done while
/// entries remain, because `done` is computed from where the page ended.
pub async fn assert_bucket_limit_hints_agree_across_endpoints<H>(harness: &H) -> Res<()>
where
    H: HostPartStoreContractHarness + Sync,
{
    let store = harness.store();
    let part = test_part(246);
    let bucket = BuckId::new(1, 5);
    let obj_a = obj_in_bucket(bucket, 1);
    let obj_b = obj_in_bucket(bucket, 2);
    let seed = FingerprintSeed::new(0x1111_2222, 0x3333_4444);

    store.ensure_part(part.clone()).await?;
    let subscriber = grant_bucket_read(store, [part.clone()]).await?;
    seed_live_obj(
        store,
        obj_a,
        payload("hint-a", 1),
        std::slice::from_ref(&part),
    )
    .await?;
    seed_live_obj(
        store,
        obj_b,
        payload("hint-b", 2),
        std::slice::from_ref(&part),
    )
    .await?;

    let none = store
        .get_changed_buckets(
            GetChangedBucketsRequest {
                part_id: part.clone(),
                offset: BuckId::ROOT,
                to_level: bucket.level(),
                since: 0,
                limit_hint: 0,
            },
            subscriber.clone(),
        )
        .await?
        .expect("a granted part answers a bucket walk");
    assert!(
        none.is_empty(),
        "a zero page bound carries no buckets, got {}",
        none.len()
    );

    let page = store
        .leaf_buckets(
            LeafBucketsRequest {
                part_id: part.clone(),
                since: 0,
                buckets: vec![LeafBucketRequest {
                    buck_id: bucket,
                    after: None,
                }],
                seed,
                limit_hint: 0,
            },
            subscriber,
        )
        .await?
        .expect("a granted part answers a leaf walk");
    let page = page
        .bucks
        .get(&bucket)
        .expect("the requested bucket has a page");
    assert_eq!(
        page.entries.len(),
        1,
        "a zero leaf hint means no preference, so the smallest useful page is one entry"
    );
    Ok(())
}

/// The page outcomes are distinct answers, for every store: an unknown part is not a
/// denial, a denial is not an empty page, and a granted part with nothing to send is an
/// empty page. Collapsing the denial into the empty page tells a caller it is caught up
/// when it has in fact lost access.
///
/// Visibility follows *current* access rather than the time access was granted, so both
/// orders are pinned below — grant before the event, and grant after it. A page's filter
/// resolves the parts a subscriber may read now and keeps no record of when it could first
/// read them.
pub async fn assert_page_outcome_contract<H>(harness: &H) -> Res<()>
where
    H: HostPartStoreContractHarness + Sync,
{
    let store = harness.store();
    let part = test_part(230);
    let granted_empty = test_part(231);
    let unknown = test_part(232);
    let obj = test_obj(233);
    let member = big_sync_core::PeerKey::new([234u8; 32]);
    let outsider = big_sync_core::PeerKey::new([235u8; 32]);
    let late_grant = test_part(236);
    let late_obj = test_obj(237);

    /// A part's events, read through the documented resume protocol.
    ///
    /// The subscription a page drains is produced by a spawned task, so a first page can come
    /// back empty before that task has delivered anything. A zero hold removes the entire
    /// window in which that delivery could happen, which turns the assertions below into
    /// tests of scheduling rather than of filtering, so this asks for a hold and re-issues
    /// while nothing has arrived — re-issuing from the returned cursor is how a caller is
    /// meant to resume.
    async fn page_events_for(
        store: &dyn HostPartStore,
        part_id: PartKey,
        subscriber: PeerKey,
    ) -> Res<Vec<PartEvent>> {
        const PAGE_HOLD: Duration = Duration::from_millis(50);
        const ATTEMPTS: u8 = 8;
        for _ in 0..ATTEMPTS {
            let outcome = store
                .replay_page_for_target(
                    SubscriptionTarget::Part {
                        part_id: part_id.clone(),
                        cursor: 0,
                    },
                    8,
                    subscriber.clone(),
                    PAGE_HOLD,
                )
                .await?;
            let page = events_page(&outcome);
            if !page.events.is_empty() {
                return Ok(page.events);
            }
        }
        Ok(Vec::new())
    }

    store.ensure_part(part.clone()).await?;
    store.ensure_part(granted_empty.clone()).await?;
    store
        .set_part_members(
            part.clone(),
            std::collections::HashMap::from([(member.clone(), Access::Read)]),
        )
        .await?;
    store
        .set_part_members(
            granted_empty.clone(),
            std::collections::HashMap::from([(member.clone(), Access::Read)]),
        )
        .await?;
    seed_live_obj(
        store,
        obj.clone(),
        payload("page-outcome", 1),
        std::slice::from_ref(&part),
    )
    .await?;

    let unknown_outcome = store
        .replay_page_for_target(
            SubscriptionTarget::Part {
                part_id: unknown,
                cursor: 0,
            },
            8,
            member.clone(),
            Duration::from_millis(0),
        )
        .await?;
    assert!(
        matches!(verdict(&unknown_outcome), TargetVerdict::UnknownPart),
        "an unknown part is its own answer"
    );

    let denied = store
        .replay_page_for_target(
            SubscriptionTarget::Part {
                part_id: part.clone(),
                cursor: 0,
            },
            8,
            outsider,
            Duration::from_millis(0),
        )
        .await?;
    assert!(
        matches!(verdict(&denied), TargetVerdict::Unauthorized),
        "a subscriber with no access row is denied, not reported as caught up"
    );

    let events = page_events_for(store, part.clone(), member.clone()).await?;
    assert!(
        events.iter().any(|event| matches!(
            event,
            PartEvent::Changed(changed) if changed.obj_id == obj && changed.part_ids.contains(&part)
        )),
        "a granted member sees the part's events, got {events:?}"
    );

    // The same property, with the grant landing *after* the event it authorizes reading.
    // Both stores filter on current access rows, so this ordering is visible too; pinned so
    // that a later change to grant-time filtering has to be deliberate.
    store.ensure_part(late_grant.clone()).await?;
    seed_live_obj(
        store,
        late_obj.clone(),
        payload("page-outcome-late", 2),
        std::slice::from_ref(&late_grant),
    )
    .await?;
    store
        .set_part_members(
            late_grant.clone(),
            std::collections::HashMap::from([(member.clone(), Access::Read)]),
        )
        .await?;
    let late_events = page_events_for(store, late_grant.clone(), member.clone()).await?;
    assert!(
        late_events.iter().any(|event| matches!(
            event,
            PartEvent::Changed(changed)
                if changed.obj_id == late_obj && changed.part_ids.contains(&late_grant)
        )),
        "a member granted after the event was written still reads it, because the filter is \
         current access and not grant time; got {late_events:?}"
    );

    let empty = store
        .replay_page_for_target(
            SubscriptionTarget::Part {
                part_id: granted_empty.clone(),
                cursor: 0,
            },
            8,
            member.clone(),
            // The hold bounds how long the responder waits for something to arrive, so the
            // claim "nothing to send" is only meaningful after it has waited.
            Duration::from_millis(50),
        )
        .await?;
    let page = events_page(&empty);
    assert!(page.events.is_empty(), "this part has nothing to send");
    assert!(page.drained, "an exhausted replay is the caught-up answer");
    assert!(
        page.resume > 0,
        "a drained page's claim carries the boundary its read scanned in full, so the next \
         request starts past the range it already covered instead of re-scanning it (got {})",
        page.resume
    );
    // Asking again from the returned boundary has nothing to add: the claim was about the
    // range up to it, and re-asking re-derives from durable state rather than trusting it.
    let again = store
        .replay_page_for_target(
            SubscriptionTarget::Part {
                part_id: granted_empty,
                cursor: page.resume,
            },
            8,
            member.clone(),
            Duration::from_millis(50),
        )
        .await?;
    let again = events_page(&again);
    assert!(
        again.events.is_empty() && again.drained,
        "nothing new past the boundary a drained page reported"
    );
    assert!(
        again.resume >= page.resume,
        "a position the caller can ask from again never moves backwards"
    );
    Ok(())
}

/// The `All` local scope: the reader resolves the part set at read time,
/// so parts and objects created after the reader was opened are still
/// observed, and a non-zero `after` skips the replayed prefix.
pub async fn assert_local_revision_reader_all_contract<H>(harness: &H) -> Res<()>
where
    H: HostPartStoreContractHarness + Sync,
{
    let store = harness.store();
    let latest = store.latest_revision().await?;

    // Opened before any part or object exists.
    let mut reader = store.open_revision_reader_all(latest).await??;
    while let RevisionRead::Entries { entries, .. } = reader
        .next(RevisionReadLimits {
            max_entries: std::num::NonZeroUsize::new(1).expect("literal is non-zero"),
        })
        .await?
    {
        assert!(entries.is_empty(), "an empty scope replays nothing");
    }

    let part = test_part(220);
    let obj = test_obj(221);
    store.ensure_part(part.clone()).await?;
    seed_live_obj(
        store,
        obj.clone(),
        payload("revision-all", 1),
        std::slice::from_ref(&part),
    )
    .await?;
    store
        .set_obj_payload(obj.clone(), payload("revision-all", 2))
        .await?;

    let mut saw_membership_event = false;
    while let RevisionRead::Entries { revision, entries } =
        reader.next(RevisionReadLimits::default()).await?
    {
        assert!(revision > latest);
        saw_membership_event |= entries.iter().any(|entry| match entry {
            PartEvent::Changed(changed) => {
                changed.obj_id == obj && changed.part_ids.contains(&part)
            }
            _ => false,
        });
        if saw_membership_event {
            break;
        }
    }
    assert!(
        saw_membership_event,
        "events for a part created after the reader was opened must be observed"
    );

    // A reader opened after the writes replays nothing. The bound is read twice: a store
    // that advanced the cursor on the read would report two different bounds, and its
    // second read would sit beyond every event — which is how an allocating read kept
    // this assertion true for the wrong reason.
    let after = store.latest_revision().await?;
    assert_eq!(
        store.latest_revision().await?,
        after,
        "reading the latest revision must not allocate a revision"
    );
    let mut bounded = store.open_revision_reader_all(after).await??;
    while let RevisionRead::Entries { entries, .. } = bounded
        .next(RevisionReadLimits {
            max_entries: std::num::NonZeroUsize::new(1).expect("literal is non-zero"),
        })
        .await?
    {
        assert!(entries.is_empty(), "the after bound must skip the prefix");
    }
    Ok(())
}

pub async fn assert_local_revision_reader_contract<H>(harness: &H) -> Res<()>
where
    H: HostPartStoreContractHarness + Sync,
{
    let store = harness.store();
    let part_a = test_part(201);
    let part_b = test_part(202);
    let obj = test_obj(203);
    store.ensure_part(part_a.clone()).await?;
    store.ensure_part(part_b.clone()).await?;
    seed_live_obj(store, obj.clone(), payload("revision-1", 1), &[]).await?;
    store
        .add_obj_to_parts(obj.clone(), vec![part_a.clone(), part_b.clone()])
        .await?;
    store
        .set_obj_payload(obj.clone(), payload("revision-2", 2))
        .await?;

    let mut reader = store
        .open_revision_reader(SubPartsRequest {
            lower_bound: 0,
            targets: HashSet::from([
                big_sync_core::rpc::SubscriptionTarget::Object {
                    obj_id: obj.clone(),
                    cursor: 0,
                },
                big_sync_core::rpc::SubscriptionTarget::Part {
                    part_id: part_a.clone(),
                    cursor: 0,
                },
                big_sync_core::rpc::SubscriptionTarget::Part {
                    part_id: part_b.clone(),
                    cursor: 0,
                },
            ]),
        })
        .await??;
    let mut last_revision = 0;
    let mut grouped_revision = None;
    let replay_through = loop {
        match reader
            .next(RevisionReadLimits {
                max_entries: std::num::NonZeroUsize::new(1).expect("literal is non-zero"),
            })
            .await?
        {
            RevisionRead::Entries { revision, entries } => {
                assert!(
                    revision > last_revision,
                    "local revisions must strictly increase: previous={last_revision}, next={revision}"
                );
                last_revision = revision;
                if let Some(changed) = entries.iter().find_map(|entry| match entry {
                    PartEvent::Changed(changed)
                        if changed.obj_id == obj
                            && changed.part_ids == vec![part_a.clone(), part_b.clone()] =>
                    {
                        Some(changed)
                    }
                    _ => None,
                }) {
                    assert_eq!(entries.len(), 1, "same-revision changes must be grouped");
                    grouped_revision = Some(revision);
                    assert_eq!(changed.obj_id, obj);
                    assert_eq!(changed.part_ids, vec![part_a.clone(), part_b.clone()]);
                }
            }
            RevisionRead::ReplayComplete { through } => {
                assert!(through >= last_revision);
                break through;
            }
        }
    };
    let grouped_revision = grouped_revision.expect("replay must contain grouped change");

    store
        .remove_obj_from_part(obj.clone(), part_a.clone())
        .await?;
    // The pull reader is handed what the log holds: it replays the log rather than a peer's
    // replica, so this removal arrives even though its own cursor never saw the add.
    let removed_revision = match reader.next(RevisionReadLimits::default()).await? {
        RevisionRead::Entries { revision, entries } => {
            assert!(revision > replay_through);
            assert!(
                matches!(entries.as_slice(), [PartEvent::Removed(removed)] if removed.obj_id == obj && removed.part_id == part_a)
            );
            revision
        }
        other => panic!("expected tombstone revision, got {other:?}"),
    };
    // A page read applies ADR 012 decision 9 with the request's own cursor: the same
    // removal is not its business below the add, and is at or after it.
    let mut below_the_add = store
        .open_page_reader(SubPartsRequest {
            lower_bound: 0,
            targets: HashSet::from([big_sync_core::rpc::SubscriptionTarget::Part {
                part_id: part_a.clone(),
                cursor: 0,
            }]),
        })
        .await??;
    let mut saw_removal = false;
    while let RevisionRead::Entries { entries, .. } =
        below_the_add.next(RevisionReadLimits::default()).await?
    {
        saw_removal |= entries.iter().any(|entry| {
            matches!(
                entry,
                PartEvent::Removed(removed) if removed.obj_id == obj && removed.part_id == part_a
            )
        });
    }
    assert!(
        !saw_removal,
        "a page request below the add is not handed the removal"
    );
    let mut at_the_add = store
        .open_page_reader(SubPartsRequest {
            lower_bound: replay_through,
            targets: HashSet::from([big_sync_core::rpc::SubscriptionTarget::Part {
                part_id: part_a.clone(),
                cursor: replay_through,
            }]),
        })
        .await??;
    let mut saw_removal = false;
    while let RevisionRead::Entries { entries, .. } =
        at_the_add.next(RevisionReadLimits::default()).await?
    {
        saw_removal |= entries.iter().any(|entry| {
            matches!(
                entry,
                PartEvent::Removed(removed) if removed.obj_id == obj && removed.part_id == part_a
            )
        });
    }
    assert!(
        saw_removal,
        "a page request at the add is handed the removal"
    );

    let missing_obj = test_obj(204);
    let mut filtered = store
        .open_revision_reader(SubPartsRequest {
            lower_bound: replay_through,
            targets: HashSet::from([big_sync_core::rpc::SubscriptionTarget::Object {
                obj_id: missing_obj,
                cursor: 0,
            }]),
        })
        .await??;
    let filtered_through = match filtered
        .next(RevisionReadLimits {
            max_entries: std::num::NonZeroUsize::new(1).expect("literal is non-zero"),
        })
        .await?
    {
        RevisionRead::Entries { revision, entries } => {
            assert!(revision >= replay_through);
            assert!(entries.is_empty());
            revision
        }
        other => panic!("expected empty filtered progress, got {other:?}"),
    };
    assert!(matches!(
        filtered
            .next(RevisionReadLimits {
                max_entries: std::num::NonZeroUsize::new(1).expect("literal is non-zero"),
            })
            .await?,
        RevisionRead::ReplayComplete { through } if through == filtered_through
    ));

    let mut bounded = store
        .open_revision_reader(SubPartsRequest {
            lower_bound: replay_through,
            targets: HashSet::from([big_sync_core::rpc::SubscriptionTarget::Part {
                part_id: part_b,
                cursor: grouped_revision,
            }]),
        })
        .await??;
    let bounded_through = match bounded
        .next(RevisionReadLimits {
            max_entries: std::num::NonZeroUsize::new(1).expect("literal is non-zero"),
        })
        .await?
    {
        RevisionRead::Entries { revision, entries } => {
            assert!(revision >= replay_through);
            assert!(entries.is_empty());
            revision
        }
        other => panic!("expected empty bounded progress, got {other:?}"),
    };
    assert!(matches!(
        bounded
            .next(RevisionReadLimits {
                max_entries: std::num::NonZeroUsize::new(1).expect("literal is non-zero"),
            })
            .await?,
        RevisionRead::ReplayComplete { through } if through == bounded_through
    ));

    let unrelated_obj = test_obj(205);
    store
        .set_obj_payload(unrelated_obj, payload("revision-filtered", 5))
        .await?;
    assert!(matches!(
        filtered
            .next(RevisionReadLimits {
                max_entries: std::num::NonZeroUsize::new(1).expect("literal is non-zero"),
            })
            .await?,
        RevisionRead::Entries {
            revision,
            entries
        } if entries.is_empty() && revision > filtered_through
    ));

    store
        .set_obj_payload(obj.clone(), payload("revision-live", 6))
        .await?;
    match reader.next(RevisionReadLimits::default()).await? {
        RevisionRead::Entries { revision, entries } => {
            assert!(revision > removed_revision);
            assert!(
                matches!(entries.as_slice(), [PartEvent::Changed(changed)] if changed.obj_id == obj)
            );
        }
        other => panic!("expected live revision, got {other:?}"),
    }
    Ok(())
}

pub async fn assert_obj_occupancy_contract<H>(harness: &H) -> Res<()>
where
    H: HostPartStoreContractHarness + Sync,
{
    let store = harness.store();
    let part_id = test_part(14);
    let obj_id = test_obj(15);
    assert!(!store.obj_exists(obj_id.clone()).await?);

    let payload = payload("occupancy", 1);
    store.set_obj_payload(obj_id.clone(), payload).await?;
    assert!(store.obj_exists(obj_id.clone()).await?);

    store
        .add_obj_to_parts(obj_id.clone(), vec![part_id.clone()])
        .await?;
    assert!(store.obj_exists(obj_id.clone()).await?);

    store.remove_obj_from_part(obj_id.clone(), part_id).await?;
    assert!(store.obj_exists(obj_id).await?);
    Ok(())
}

pub async fn assert_summarize_parts_contract<H>(harness: &H) -> Res<()>
where
    H: HostPartStoreContractHarness + Sync,
{
    let store = harness.store();
    let part_a = test_part(11);
    let part_b = test_part(12);
    let unknown = test_part(13);
    let obj_a = test_obj(1);
    let obj_b = test_obj(2);

    store.ensure_part(part_a.clone()).await?;
    store.ensure_part(part_b.clone()).await?;

    assert_eq!(
        store.summarize_parts(HashSet::new()).await??,
        HashMap::new()
    );
    match store
        .summarize_parts(HashSet::from([unknown.clone()]))
        .await?
    {
        Err(ListPartsError::UnkownParts { unkown_parts }) => {
            assert_eq!(unkown_parts, vec![unknown.clone()]);
        }
        other => panic!("unexpected summarize_parts result: {other:?}"),
    }

    seed_live_obj(
        store,
        obj_a,
        payload("summarize-a", 1),
        std::slice::from_ref(&part_a),
    )
    .await?;
    seed_live_obj(
        store,
        obj_b,
        payload("summarize-b", 2),
        std::slice::from_ref(&part_b),
    )
    .await?;

    let summary = store
        .summarize_parts(HashSet::from([part_a.clone(), part_b.clone()]))
        .await??;
    assert_eq!(summary.len(), 2);
    assert_eq!(summary[&part_a].member_count, 1);
    assert_eq!(summary[&part_a].latest_cursor, 2);
    assert_eq!(summary[&part_b].member_count, 1);
    assert_eq!(summary[&part_b].latest_cursor, 4);

    match store
        .summarize_parts(HashSet::from([part_a, unknown.clone()]))
        .await?
    {
        Err(ListPartsError::UnkownParts { unkown_parts }) => {
            assert_eq!(unkown_parts, vec![unknown]);
        }
        other => panic!("unexpected summarize_parts result: {other:?}"),
    }
    Ok(())
}

pub async fn assert_changed_buckets_contract<H>(harness: &H) -> Res<()>
where
    H: HostPartStoreContractHarness + Sync,
{
    let store = harness.store();
    let part = test_part(21);
    let unknown = test_part(22);
    let bucket_a = BuckId::new(1, 0);
    let bucket_b = BuckId::new(1, 1);
    let bucket_c = BuckId::new(1, 2);
    let obj_a = obj_in_bucket(bucket_a, 1);
    let obj_b = obj_in_bucket(bucket_b, 2);
    let obj_c = obj_in_bucket(bucket_c, 3);

    store.ensure_part(part.clone()).await?;
    let subscriber = grant_bucket_read(store, [part.clone()]).await?;
    seed_live_obj(
        store,
        obj_a.clone(),
        payload("changed-a", 1),
        std::slice::from_ref(&part),
    )
    .await?;
    seed_live_obj(
        store,
        obj_b,
        payload("changed-b", 2),
        std::slice::from_ref(&part),
    )
    .await?;
    seed_live_obj(
        store,
        obj_c,
        payload("changed-c", 3),
        std::slice::from_ref(&part),
    )
    .await?;

    // An unknown part and an unreadable one answer alike: `unknown` is never
    // granted, so this is both the unknown case and the refusal shape.
    match store
        .get_changed_buckets(
            GetChangedBucketsRequest {
                part_id: unknown.clone(),
                offset: bucket_a,
                to_level: bucket_a.level(),
                since: 0,
                limit_hint: 16,
            },
            subscriber.clone(),
        )
        .await?
    {
        Err(ListPartsError::UnkownParts { unkown_parts }) => {
            assert_eq!(unkown_parts, vec![unknown]);
        }
        other => panic!("unexpected get_changed_buckets result: {other:?}"),
    }

    let changed = store
        .get_changed_buckets(
            GetChangedBucketsRequest {
                part_id: part.clone(),
                offset: bucket_a,
                to_level: bucket_a.level(),
                since: 0,
                limit_hint: 16,
            },
            subscriber.clone(),
        )
        .await??
        .into_iter()
        .collect::<Vec<_>>();
    assert_eq!(changed.len(), 3);
    assert!(changed.windows(2).all(|pair| pair[0].id < pair[1].id));
    assert!(
        changed
            .iter()
            .all(|buck| buck.id.level() == bucket_a.level())
    );
    for buck in &changed {
        assert_eq!(
            store.get_bucket_summary(part.clone(), buck.id).await?,
            *buck
        );
    }

    let changed_from_b = store
        .get_changed_buckets(
            GetChangedBucketsRequest {
                part_id: part.clone(),
                offset: bucket_b,
                to_level: bucket_b.level(),
                since: 0,
                limit_hint: 16,
            },
            subscriber.clone(),
        )
        .await??
        .into_iter()
        .collect::<Vec<_>>();
    assert_eq!(
        changed_from_b
            .iter()
            .map(|buck| buck.id)
            .collect::<Vec<_>>(),
        vec![bucket_b, bucket_c]
    );

    // ADR 012 decision 4 correction 3: one request returns every changed bucket from the
    // cursor down through `to_level`, so a walk reaches its working level in a single
    // scan instead of one exchange per level. The page is still in bucket order, and it
    // now contains levels the cursor's own level never covered.
    let mixed = store
        .get_changed_buckets(
            GetChangedBucketsRequest {
                part_id: part.clone(),
                offset: BuckId::ROOT,
                to_level: BuckId::MAX_LEVEL,
                since: 0,
                limit_hint: 64,
            },
            subscriber.clone(),
        )
        .await??
        .into_iter()
        .collect::<Vec<_>>();
    assert!(
        mixed.windows(2).all(|pair| pair[0].id < pair[1].id),
        "a mixed-level page is still ordered by bucket id"
    );
    for (buck, salt) in [(bucket_a, 1u8), (bucket_b, 2), (bucket_c, 3)] {
        assert!(
            mixed.iter().any(|page| page.id == buck),
            "the scan covers the cursor's own level"
        );
        // The same key that was seeded above, so its deeper ancestor exists.
        let deeper = BuckId::from_obj_key(buck.level() + 1, &obj_in_bucket(buck, salt));
        assert!(
            mixed.iter().any(|page| page.id == deeper),
            "and levels beneath it, in the same request"
        );
    }

    let cutoff = changed
        .iter()
        .map(|buck| buck.changed_at)
        .max()
        .expect(ERROR_IMPOSSIBLE);
    let nothing = store
        .get_changed_buckets(
            GetChangedBucketsRequest {
                part_id: part.clone(),
                offset: bucket_a,
                to_level: bucket_a.level(),
                since: cutoff,
                limit_hint: 16,
            },
            subscriber.clone(),
        )
        .await??;
    assert!(nothing.is_empty());

    seed_live_obj(
        store,
        obj_a,
        payload("changed-a-2", 4),
        std::slice::from_ref(&part),
    )
    .await?;
    let changed_after = store
        .get_changed_buckets(
            GetChangedBucketsRequest {
                part_id: part.clone(),
                offset: bucket_a,
                to_level: bucket_a.level(),
                since: cutoff,
                limit_hint: 16,
            },
            subscriber,
        )
        .await??
        .into_iter()
        .collect::<Vec<_>>();
    assert_eq!(changed_after.len(), 1);
    assert_eq!(changed_after[0].id, bucket_a);
    assert!(changed_after[0].changed_at > cutoff);
    assert_eq!(
        store.get_bucket_summary(part, bucket_a).await?,
        changed_after[0]
    );
    Ok(())
}

pub async fn assert_payload_can_trail_membership_contract<H>(harness: &H) -> Res<()>
where
    H: HostPartStoreContractHarness + Sync,
{
    let store = harness.store();
    let part = test_part(24);
    let bucket = BuckId::new(1, 6);
    let obj = obj_in_bucket(bucket, 9);
    let seed = FingerprintSeed::new(0x4444_5555, 0x6666_7777);

    store.ensure_part(part.clone()).await?;
    let subscriber = grant_bucket_read(store, [part.clone()]).await?;
    store
        .add_obj_to_parts(obj.clone(), vec![part.clone()])
        .await?;

    assert_eq!(store.obj_payload(obj.clone()).await?, None);
    assert_eq!(store.obj_parts(obj.clone()).await?, vec![part.clone()]);
    assert_eq!(store.member_count(part.clone()).await?, 0);

    let bucket_before = store.get_bucket_summary(part.clone(), bucket).await?;
    assert_eq!(
        bucket_before,
        BucketSummary {
            id: bucket,
            len: 0,
            live_count: 0,
            fp: (0, 0),
            changed_at: 0,
        }
    );

    let leaf_before = store
        .leaf_buckets(
            LeafBucketsRequest {
                part_id: part.clone(),
                since: 0,
                buckets: vec![LeafBucketRequest {
                    buck_id: bucket,
                    after: None,
                }],
                seed,
                limit_hint: 8,
            },
            subscriber.clone(),
        )
        .await??
        .bucks
        .remove(&bucket)
        .expect(ERROR_IMPOSSIBLE);
    assert_eq!(
        leaf_before,
        LeafBucketPage {
            entries: Vec::new(),
            next_after: None,
            done: true,
        }
    );

    let events_before = store
        .list_events(HashSet::from([part.clone()]), 0, 8)
        .await??;
    assert_eq!(
        events_before.get(&part).expect(ERROR_IMPOSSIBLE),
        &PartPage {
            events: Vec::new(),
            resume: 0,
            drained: true,
        }
    );
    store
        .set_obj_payload(obj.clone(), payload("late-payload", 99))
        .await?;

    let live_fp_after = Fingerprint::new(
        &BUCKET_LIVE_FP_SEED,
        &(
            "big-sync-bucket-live-v1",
            bucket,
            obj.clone(),
            payload("late-payload", 99),
        ),
    )
    .as_u64();
    let bucket_after = store.get_bucket_summary(part.clone(), bucket).await?;
    assert_eq!(bucket_after.id, bucket);
    assert_eq!(bucket_after.len, 1);
    assert_eq!(bucket_after.live_count, 1);
    assert_eq!(bucket_after.fp, (live_fp_after, 0));
    assert!(bucket_after.changed_at > bucket_before.changed_at);

    let changed = store
        .get_changed_buckets(
            GetChangedBucketsRequest {
                part_id: part.clone(),
                offset: bucket,
                to_level: bucket.level(),
                since: bucket_before.changed_at,
                limit_hint: 16,
            },
            subscriber.clone(),
        )
        .await??
        .into_iter()
        .collect::<Vec<_>>();
    assert_eq!(changed, vec![bucket_after]);

    let leaf_after = store
        .leaf_buckets(
            LeafBucketsRequest {
                part_id: part.clone(),
                since: bucket_before.changed_at,
                buckets: vec![LeafBucketRequest {
                    buck_id: bucket,
                    after: None,
                }],
                seed,
                limit_hint: 8,
            },
            subscriber,
        )
        .await??
        .bucks
        .remove(&bucket)
        .expect(ERROR_IMPOSSIBLE);
    assert_eq!(
        leaf_after,
        LeafBucketPage {
            entries: vec![BucketObjPageEntry {
                obj_id: obj.clone(),
                dead: false,
                fp: Fingerprint::new(
                    &seed,
                    &(
                        "big-sync-obj-fp-v1",
                        obj.clone(),
                        payload("late-payload", 99)
                    ),
                ),
            }],
            next_after: None,
            done: true,
        }
    );

    let events_after = store
        .list_events(HashSet::from([part.clone()]), 0, 8)
        .await??;
    let page_after = events_after.get(&part).expect(ERROR_IMPOSSIBLE);
    assert_eq!(page_after.events.len(), 1);
    let touched_cursor = match &page_after.events[0] {
        PartEvent::Changed(changed) => changed.cursor,
        other => {
            panic!("expected the member write to arrive as the part's touch, got {other:?}")
        }
    };
    assert_member_touched(
        &page_after.events[0],
        touched_cursor,
        part,
        obj,
        payload("late-payload", 99),
    );
    assert!(page_after.drained, "the page is short of its limit");
    assert_eq!(page_after.resume, touched_cursor);
    Ok(())
}

pub async fn assert_leaf_buckets_contract<H>(harness: &H) -> Res<()>
where
    H: HostPartStoreContractHarness + Sync,
{
    let store = harness.store();
    let part = test_part(31);
    let unknown = test_part(32);
    let bucket_a = BuckId::new(1, 3);
    let bucket_b = BuckId::new(1, 4);
    let a1 = obj_in_bucket(bucket_a, 1);
    let a2 = obj_in_bucket(bucket_a, 2);
    let a3 = obj_in_bucket(bucket_a, 3);
    let b1 = obj_in_bucket(bucket_b, 1);
    let seed = FingerprintSeed::new(0xaaaa_bbbb, 0xcccc_dddd);

    store.ensure_part(part.clone()).await?;
    let subscriber = grant_bucket_read(store, [part.clone()]).await?;
    seed_live_obj(
        store,
        a2.clone(),
        payload("leaf-a2", 2),
        std::slice::from_ref(&part),
    )
    .await?;
    seed_live_obj(
        store,
        a1.clone(),
        payload("leaf-a1", 1),
        std::slice::from_ref(&part),
    )
    .await?;
    seed_live_obj(
        store,
        a3.clone(),
        payload("leaf-a3", 3),
        std::slice::from_ref(&part),
    )
    .await?;
    store.remove_obj_from_part(a3.clone(), part.clone()).await?;
    seed_live_obj(
        store,
        b1.clone(),
        payload("leaf-b1", 4),
        std::slice::from_ref(&part),
    )
    .await?;

    // `unknown` is never granted, so it pins the unknown shape and the refusal
    // shape at once.
    match store
        .leaf_buckets(
            LeafBucketsRequest {
                part_id: unknown,
                since: 0,
                buckets: vec![LeafBucketRequest {
                    buck_id: bucket_a,
                    after: None,
                }],
                seed,
                limit_hint: 2,
            },
            subscriber.clone(),
        )
        .await?
    {
        Err(LeafBucketsError::UnkownPart) => {}
        other => panic!("unexpected leaf_buckets result: {other:?}"),
    }

    let page = store
        .leaf_buckets(
            LeafBucketsRequest {
                part_id: part.clone(),
                since: 0,
                buckets: vec![
                    LeafBucketRequest {
                        buck_id: bucket_a,
                        after: None,
                    },
                    LeafBucketRequest {
                        buck_id: bucket_b,
                        after: None,
                    },
                ],
                seed,
                limit_hint: 2,
            },
            subscriber.clone(),
        )
        .await??;
    assert_eq!(page.seed, seed);
    let page_a = page.bucks.get(&bucket_a).expect(ERROR_IMPOSSIBLE);
    assert_eq!(
        page_a,
        &LeafBucketPage {
            entries: vec![
                BucketObjPageEntry {
                    obj_id: a1.clone(),
                    dead: false,
                    fp: Fingerprint::new(&seed, &("big-sync-obj-fp-v1", a1, payload("leaf-a1", 1))),
                },
                BucketObjPageEntry {
                    obj_id: a2.clone(),
                    dead: false,
                    fp: Fingerprint::new(
                        &seed,
                        &("big-sync-obj-fp-v1", a2.clone(), payload("leaf-a2", 2))
                    ),
                },
            ],
            next_after: Some(a2.clone()),
            done: false,
        }
    );
    let page_b = page.bucks.get(&bucket_b).expect(ERROR_IMPOSSIBLE);
    assert_eq!(
        page_b,
        &LeafBucketPage {
            entries: vec![BucketObjPageEntry {
                obj_id: b1.clone(),
                dead: false,
                fp: Fingerprint::new(
                    &seed,
                    &("big-sync-obj-fp-v1", b1.clone(), payload("leaf-b1", 4))
                ),
            }],
            next_after: None,
            done: true,
        }
    );

    let since = store
        .get_bucket_summary(part.clone(), bucket_b)
        .await?
        .changed_at;
    store
        .set_obj_payload(b1.clone(), payload("leaf-b1-2", 5))
        .await?;

    let since_page = store
        .leaf_buckets(
            LeafBucketsRequest {
                part_id: part.clone(),
                since,
                buckets: vec![
                    LeafBucketRequest {
                        buck_id: bucket_a,
                        after: None,
                    },
                    LeafBucketRequest {
                        buck_id: bucket_b,
                        after: None,
                    },
                ],
                seed,
                limit_hint: 2,
            },
            subscriber.clone(),
        )
        .await??;
    assert_eq!(
        since_page.bucks.get(&bucket_a).expect(ERROR_IMPOSSIBLE),
        &LeafBucketPage {
            entries: vec![],
            next_after: None,
            done: true,
        }
    );
    assert_eq!(
        since_page.bucks.get(&bucket_b).expect(ERROR_IMPOSSIBLE),
        &LeafBucketPage {
            entries: vec![BucketObjPageEntry {
                obj_id: b1.clone(),
                dead: false,
                fp: Fingerprint::new(&seed, &("big-sync-obj-fp-v1", b1, payload("leaf-b1-2", 5)),),
            }],
            next_after: None,
            done: true,
        }
    );

    let page_a_tail = store
        .leaf_buckets(
            LeafBucketsRequest {
                part_id: part,
                since: 0,
                buckets: vec![LeafBucketRequest {
                    buck_id: bucket_a,
                    after: Some(a2),
                }],
                seed,
                limit_hint: 2,
            },
            subscriber,
        )
        .await??
        .bucks
        .remove(&bucket_a)
        .expect(ERROR_IMPOSSIBLE);
    assert_eq!(
        page_a_tail,
        LeafBucketPage {
            entries: vec![BucketObjPageEntry {
                obj_id: a3.clone(),
                dead: true,
                fp: Fingerprint::new(&seed, &("big-sync-obj-fp-v1", a3, serde_json::Value::Null),),
            }],
            next_after: None,
            done: true,
        }
    );
    Ok(())
}

/// The budget's arithmetic, pinned as the framing it claims to be.
///
/// [`leaf_entry_wire_bytes`] decides where a page stops, so its numbers are load-bearing: a
/// 32-byte key costs 42 bytes (one length group, the flag, and the fingerprint), and the
/// length prefix widens at 128 and 16384 bytes.
#[test]
fn leaf_entry_wire_bytes_matches_the_postcard_framing() {
    assert_eq!(
        leaf_entry_wire_bytes(0),
        10,
        "a length prefix is paid even for an empty key"
    );
    assert_eq!(leaf_entry_wire_bytes(32), 42);
    assert_eq!(leaf_entry_wire_bytes(127), 137);
    assert_eq!(
        leaf_entry_wire_bytes(128),
        139,
        "the length prefix widens at 128"
    );
    assert_eq!(leaf_entry_wire_bytes(16_384), 16_396, "and again at 16384");
    assert_eq!(
        LEAF_PAGE_BYTE_BUDGET / leaf_entry_wire_bytes(32),
        1560,
        "the entry hint of 1024 still binds for 32-byte keys"
    );
    assert!(
        LEAF_PAGE_BYTE_BUDGET / leaf_entry_wire_bytes(4096) < 1024,
        "the budget binds for wide keys"
    );
}

/// A leaf page stops at the byte budget before it stops at the entry hint.
///
/// `limit_hint` counts entries and an entry is its key's width, so a page of long keys is
/// unbounded work for the peer that receives it. This asks for far more entries than the
/// budget allows and asserts that the page stops at the budget, that stopping strands
/// nothing — paging with `next_after` still reaches every entry — and that a key wider than
/// the whole budget still yields one entry, because a page with no entries reads as `done`
/// while entries remain.
pub async fn assert_leaf_page_byte_budget_contract<S>(store: &S) -> Res<()>
where
    S: HostPartStore + Sync + ?Sized,
{
    use std::collections::BTreeSet;

    let part = test_part(0xed);
    let bucket = BuckId::new(1, 5);
    let seed = FingerprintSeed::new(0x5eed_5eed, 0xb0d6_0b0d);
    // Twenty 4 KiB keys against the budget: one page cannot hold the bucket, so a full walk
    // needs two pages and the cap has to resume rather than drop.
    const WIDE_KEY_LEN: usize = 4096;
    const WIDE_COUNT: u32 = 20;
    let per_page = LEAF_PAGE_BYTE_BUDGET / leaf_entry_wire_bytes(WIDE_KEY_LEN);
    assert!(
        per_page >= 2,
        "the fixture needs more than one entry per page"
    );
    assert!(
        per_page < WIDE_COUNT as usize,
        "the fixture needs the budget to bind below the hint"
    );

    store.ensure_part(part.clone()).await?;
    let subscriber = grant_bucket_read(store, [part.clone()]).await?;
    let mut expected = BTreeSet::new();
    for salt in 1..=u8::try_from(WIDE_COUNT).expect(ERROR_IMPOSSIBLE) {
        let obj_id = wide_obj_in_bucket(bucket, WIDE_KEY_LEN, salt);
        expected.insert(obj_id.clone());
        seed_live_obj(
            store,
            obj_id,
            payload("leaf-budget", u64::from(salt)),
            std::slice::from_ref(&part),
        )
        .await?;
    }

    let mut after = None;
    let mut seen = BTreeSet::new();
    let mut pages = 0u32;
    loop {
        pages += 1;
        assert!(pages <= WIDE_COUNT, "a walk must advance: {seen:?}");
        let page = store
            .leaf_buckets(
                LeafBucketsRequest {
                    part_id: part.clone(),
                    since: 0,
                    buckets: vec![LeafBucketRequest {
                        buck_id: bucket,
                        after: after.clone(),
                    }],
                    seed,
                    limit_hint: 1024,
                },
                subscriber.clone(),
            )
            .await??
            .bucks
            .remove(&bucket)
            .expect(ERROR_IMPOSSIBLE);
        let encoded: usize = page
            .entries
            .iter()
            .map(|entry| leaf_entry_wire_bytes(entry.obj_id.as_bytes().len()))
            .sum();
        assert!(
            encoded <= LEAF_PAGE_BYTE_BUDGET,
            "every entry here fits the budget, so the page must not exceed it: {encoded} over {} entries",
            page.entries.len()
        );
        if pages == 1 {
            assert_eq!(
                page.entries.len(),
                per_page,
                "the byte budget binds before a hint of 1024 entries"
            );
            assert!(!page.done, "the cap strands nothing");
            assert_eq!(
                page.next_after.clone(),
                Some(page.entries.last().expect(ERROR_IMPOSSIBLE).obj_id.clone()),
                "a full page resumes after its last entry"
            );
        }
        for entry in &page.entries {
            seen.insert(entry.obj_id.clone());
        }
        if page.done {
            break;
        }
        after = page.next_after.clone();
    }
    assert_eq!(
        seen, expected,
        "paging past the byte cap reaches every entry"
    );

    // A key wider than the whole budget: one entry is always sent, so the walk advances
    // instead of reporting `done` on a page it could not fill.
    let huge_bucket = BuckId::new(1, 6);
    let huge = wide_obj_in_bucket(huge_bucket, 80 * 1024, 1);
    let normal = obj_in_bucket(huge_bucket, 2);
    seed_live_obj(
        store,
        huge.clone(),
        payload("leaf-huge", 1),
        std::slice::from_ref(&part),
    )
    .await?;
    seed_live_obj(
        store,
        normal.clone(),
        payload("leaf-huge", 2),
        std::slice::from_ref(&part),
    )
    .await?;
    let first_page = store
        .leaf_buckets(
            LeafBucketsRequest {
                part_id: part.clone(),
                since: 0,
                buckets: vec![LeafBucketRequest {
                    buck_id: huge_bucket,
                    after: None,
                }],
                seed,
                limit_hint: 1024,
            },
            subscriber.clone(),
        )
        .await??
        .bucks
        .remove(&huge_bucket)
        .expect(ERROR_IMPOSSIBLE);
    assert_eq!(
        first_page.entries.len(),
        1,
        "one entry is always sent, even when it alone is over budget"
    );
    assert!(
        !first_page.done,
        "the entry that did not fit is not reported as done"
    );
    let second_page = store
        .leaf_buckets(
            LeafBucketsRequest {
                part_id: part.clone(),
                since: 0,
                buckets: vec![LeafBucketRequest {
                    buck_id: huge_bucket,
                    after: first_page.next_after.clone(),
                }],
                seed,
                limit_hint: 1024,
            },
            subscriber.clone(),
        )
        .await??
        .bucks
        .remove(&huge_bucket)
        .expect(ERROR_IMPOSSIBLE);
    assert_eq!(second_page.entries.len(), 1);
    assert!(second_page.done, "the second page has nothing left");
    let walked: BTreeSet<_> = first_page
        .entries
        .iter()
        .map(|entry| entry.obj_id.clone())
        .chain(second_page.entries.iter().map(|entry| entry.obj_id.clone()))
        .collect();
    assert_eq!(
        walked,
        BTreeSet::from([huge, normal]),
        "a key wider than the budget still costs one page each way"
    );

    // The budget must stop the page, not skip ahead. A row the budget rejects has to
    // be the first row of the next page: skipping it and taking a later, narrower key
    // would advance `next_after` past it, and the keyset walk could never reach it
    // again. A wide row that does not fit followed by a narrow row that does is the
    // shape that separates stopping from skipping — the fixture above uses one width
    // per bucket, where both behave the same.
    let stop_bucket = BuckId::new(1, 7);
    let stop_wide: Vec<ObjKey> = (1..=u8::try_from(per_page + 1).expect(ERROR_IMPOSSIBLE))
        .map(|salt| wide_obj_in_bucket(stop_bucket, WIDE_KEY_LEN, salt))
        .collect();
    let stop_narrow = narrow_obj_after(stop_bucket, stop_wide.last().expect(ERROR_IMPOSSIBLE));
    let mut stop_expected: BTreeSet<ObjKey> = BTreeSet::new();
    for (index, obj_id) in stop_wide.iter().enumerate() {
        stop_expected.insert(obj_id.clone());
        seed_live_obj(
            store,
            obj_id.clone(),
            payload("leaf-budget-stop", index as u64),
            std::slice::from_ref(&part),
        )
        .await?;
    }
    stop_expected.insert(stop_narrow.clone());
    seed_live_obj(
        store,
        stop_narrow,
        payload("leaf-budget-stop", 0),
        std::slice::from_ref(&part),
    )
    .await?;

    let mut after = None;
    let mut stop_seen = BTreeSet::new();
    loop {
        let page = store
            .leaf_buckets(
                LeafBucketsRequest {
                    part_id: part.clone(),
                    since: 0,
                    buckets: vec![LeafBucketRequest {
                        buck_id: stop_bucket,
                        after: after.clone(),
                    }],
                    seed,
                    limit_hint: 1024,
                },
                subscriber.clone(),
            )
            .await??
            .bucks
            .remove(&stop_bucket)
            .expect(ERROR_IMPOSSIBLE);
        for entry in &page.entries {
            stop_seen.insert(entry.obj_id.clone());
        }
        if page.done {
            break;
        }
        after = page.next_after.clone();
    }
    assert_eq!(
        stop_seen, stop_expected,
        "the byte budget must stop the page, not skip the row it rejected"
    );
    Ok(())
}

/// A payload that outlives its last part is not observable state.
///
/// Removing an object from a part keeps its payload by design, and the object route reads
/// membership to decide what it may show a peer, so a part-less payload is not part of the
/// store's observable contents. The memory store counted it once and the sqlite store did
/// not — the divergence this pins.
#[cfg(test)]
pub(crate) async fn assert_observed_snapshot_excludes_partless_payload(
    store: &dyn ObservedStore,
) -> Res<()> {
    let part = test_part(0xee);
    let obj_id = test_obj(0xef);

    store.ensure_part(part.clone()).await?;
    seed_live_obj(
        store,
        obj_id.clone(),
        payload("observed", 1),
        std::slice::from_ref(&part),
    )
    .await?;
    assert!(
        store.observed_snapshot().await?.objs.contains_key(&obj_id),
        "a live member is observable"
    );
    store
        .remove_obj_from_part(obj_id.clone(), part.clone())
        .await?;
    assert_eq!(
        store.obj_payload(obj_id.clone()).await?,
        Some(payload("observed", 1)),
        "the payload outlives its last part"
    );
    assert!(
        !store.observed_snapshot().await?.objs.contains_key(&obj_id),
        "an object in no part is not remotely deliverable, so it is not observable"
    );
    Ok(())
}

pub async fn assert_list_events_contract<H>(harness: &H) -> Res<()>
where
    H: HostPartStoreContractHarness + Sync,
{
    let store = harness.store();
    let part_a = test_part(41);
    let part_b = test_part(42);
    let unknown = test_part(43);
    let obj = test_obj(44);

    store.ensure_part(part_a.clone()).await?;
    store.ensure_part(part_b.clone()).await?;

    seed_live_obj(
        store,
        obj.clone(),
        payload("events-1", 1),
        std::slice::from_ref(&part_a),
    )
    .await?;
    store
        .add_obj_to_parts(obj.clone(), vec![part_b.clone()])
        .await?;
    store
        .set_obj_payload(obj.clone(), payload("events-2", 2))
        .await?;
    store
        .remove_obj_from_part(obj.clone(), part_a.clone())
        .await?;
    store
        .set_obj_payload(obj.clone(), payload("events-3", 3))
        .await?;

    match store
        .list_events(HashSet::from([unknown.clone()]), 0, 10)
        .await?
    {
        Err(ListPartsError::UnkownParts { unkown_parts }) => {
            assert_eq!(unkown_parts, vec![unknown]);
        }
        other => panic!("unexpected list_events result: {other:?}"),
    }

    // The object joined `part_a` at cursor 2 and left it at cursor 5, so a reader at cursor
    // 0 never saw it in the part and is not handed the removal (`added_at <= cursor < T`),
    // while a reader that did see the add is.
    let page_a_at_zero = store
        .list_events(HashSet::from([part_a.clone()]), 0, 10)
        .await??
        .remove(&part_a)
        .expect(ERROR_IMPOSSIBLE);
    assert!(
        page_a_at_zero.events.is_empty(),
        "a tombstone for a member this reader never saw is not delivered: {:?}",
        page_a_at_zero.events
    );
    assert!(
        page_a_at_zero.drained,
        "a page that carried nothing and had nothing waiting is caught up"
    );
    assert_eq!(page_a_at_zero.resume, 0);

    let page_b = store
        .list_events(HashSet::from([part_b.clone()]), 0, 10)
        .await??
        .remove(&part_b)
        .expect(ERROR_IMPOSSIBLE);
    let part_b_cursor = match &page_b.events[..] {
        [PartEvent::Changed(changed)] => {
            assert_eq!(changed.part_ids, vec![part_b]);
            assert_eq!(changed.obj_id, obj);
            assert_eq!(changed.payload, payload("events-3", 3));
            changed.cursor
        }
        other => panic!("unexpected latest part_b page: {other:?}"),
    };

    // The tombstone is delivered from exactly the cursor the member became present on, and no
    // reader before that boundary is handed it. The boundary is derived from the store rather
    // than hardcoded, then pinned against the `part_b` write that must follow the add.
    let latest = store.latest_revision().await?;
    let mut boundaries = Vec::new();
    for cursor in 0..=latest {
        let page = store
            .list_events(HashSet::from([part_a.clone()]), cursor, 10)
            .await??
            .remove(&part_a)
            .expect(ERROR_IMPOSSIBLE);
        match page.events[..] {
            [] => {}
            [PartEvent::Removed(ref removed)] => {
                assert_eq!(removed.part_id, part_a);
                assert_eq!(removed.obj_id, obj);
                boundaries.push((cursor, removed.cursor));
            }
            ref other => panic!("unexpected part_a page at cursor {cursor}: {other:?}"),
        }
    }
    assert!(
        !boundaries.is_empty(),
        "the tombstone is delivered to some reader: {boundaries:?}"
    );
    let (first_delivered, removed_cursor) = boundaries[0];
    let (last_delivered, _) = *boundaries.last().expect(ERROR_IMPOSSIBLE);
    assert_eq!(
        last_delivered + 1,
        removed_cursor,
        "the tombstone stops being delivered at its own cursor: {boundaries:?}"
    );
    assert_eq!(
        boundaries.len() as u64,
        removed_cursor - first_delivered,
        "every cursor from the add to the removal is delivered: {boundaries:?}"
    );
    assert!(
        first_delivered > 0,
        "a reader that never saw the add is not handed the removal"
    );
    assert!(
        first_delivered < part_b_cursor,
        "the boundary is the add, not a later touch: {first_delivered} vs {part_b_cursor}"
    );
    Ok(())
}

pub async fn assert_readable_subscribe_contract<H>(harness: &H) -> Res<()>
where
    H: HostPartStoreContractHarness + Sync,
{
    let store = harness.store();
    let part = test_part(54);
    let obj = test_obj(55);
    let reader = big_sync_core::PeerKey::new([56u8; 32]);

    store.ensure_part(part.clone()).await?;
    store
        .set_part_members(
            part.clone(),
            std::collections::HashMap::from([(reader.clone(), Access::Read)]),
        )
        .await?;
    let rx = store
        .page_events(
            SubPartsRequest {
                lower_bound: 0,
                targets: HashSet::from([big_sync_core::rpc::SubscriptionTarget::Part {
                    part_id: part.clone(),
                    cursor: 0,
                }]),
            },
            reader,
        )
        .await??;
    wait_replay_complete(&rx).await?;

    store
        .set_obj_payload(obj.clone(), payload("readable-subscribe", 1))
        .await?;
    store
        .add_obj_to_parts(obj.clone(), vec![part.clone()])
        .await?;

    loop {
        match recv_sub_event(&rx).await? {
            PartEvent::Changed(event) => {
                assert_eq!(event.obj_id, obj);
                assert!(event.part_ids.contains(&part), "the touched part is named");
                break;
            }
            PartEvent::Removed(_) => {}
        }
    }
    Ok(())
}

pub async fn assert_subscribe_contract<H>(harness: &H) -> Res<()>
where
    H: HostPartStoreContractHarness + Sync,
{
    let store = harness.store();
    let part_a = test_part(51);
    let part_b = test_part(52);
    let obj = test_obj(53);

    store.ensure_part(part_a.clone()).await?;
    store.ensure_part(part_b.clone()).await?;

    // Grant the subscriber Read access on every part the object is in, so the
    // filter passes events.
    let sub_peer = big_sync_core::PeerKey::new([0u8; 32]);
    for part in [part_a.clone(), part_b.clone()] {
        store
            .set_part_members(
                part,
                std::collections::HashMap::from([(sub_peer.clone(), Access::Read)]),
            )
            .await?;
    }

    seed_live_obj(
        store,
        obj.clone(),
        payload("sub-1", 1),
        std::slice::from_ref(&part_a),
    )
    .await?;
    store
        .add_obj_to_parts(obj.clone(), vec![part_b.clone()])
        .await?;
    store
        .set_obj_payload(obj.clone(), payload("sub-2", 2))
        .await?;
    store.remove_obj_from_part(obj.clone(), part_a).await?;
    store
        .set_obj_payload(obj.clone(), payload("sub-3", 3))
        .await?;

    let rx = store
        .page_events(
            SubPartsRequest {
                lower_bound: 0,
                targets: HashSet::from([big_sync_core::rpc::SubscriptionTarget::Part {
                    part_id: part_b.clone(),
                    cursor: 0,
                }]),
            },
            sub_peer,
        )
        .await??;
    let events = collect_sub_events(&rx).await?;
    let replay_cursor = match &events[..] {
        [PartEvent::Changed(changed)] => {
            assert_eq!(changed.part_ids, vec![part_b.clone()]);
            assert_eq!(changed.obj_id, obj);
            assert_eq!(changed.payload, payload("sub-3", 3));
            changed.cursor
        }
        other => panic!("unexpected replay events: {other:?}"),
    };

    store
        .set_obj_payload(obj.clone(), payload("sub-4", 4))
        .await?;
    let live_evt = recv_sub_event(&rx).await?;
    match live_evt {
        PartEvent::Changed(transition) => {
            assert_eq!(transition.part_ids, vec![part_b]);
            assert_eq!(transition.obj_id, obj);
            assert_eq!(transition.payload, payload("sub-4", 4));
            assert!(transition.cursor > replay_cursor);
        }
        other => panic!("unexpected live sub event: {other:?}"),
    }
    Ok(())
}

pub async fn assert_subscribe_replay_filtering_contract<H>(harness: &H) -> Res<()>
where
    H: HostPartStoreContractHarness + Sync,
{
    let store = harness.store();
    let part = test_part(61);
    let obj = test_obj(62);
    let auth_peer = big_sync_core::PeerKey::new([63u8; 32]);
    let denied_peer = big_sync_core::PeerKey::new([64u8; 32]);

    store.ensure_part(part.clone()).await?;

    // Seed the doc before any subscriptions.
    store
        .set_obj_payload(obj.clone(), payload("replay-filter", 1))
        .await?;
    store
        .add_obj_to_parts(obj.clone(), vec![part.clone()])
        .await?;

    // Set explicit membership: auth_peer has Read; denied_peer gets
    // an empty membership map (explicitly denied).
    store
        .set_part_members(
            part.clone(),
            std::collections::HashMap::from([(auth_peer.clone(), Access::Read)]),
        )
        .await?;

    // Subscribe the authorized peer.
    let auth_rx = store
        .page_events(
            SubPartsRequest {
                lower_bound: 0,
                targets: HashSet::from([big_sync_core::rpc::SubscriptionTarget::Part {
                    part_id: part.clone(),
                    cursor: 0,
                }]),
            },
            auth_peer.clone(),
        )
        .await??;
    let auth_events = collect_sub_events(&auth_rx).await?;
    assert!(
        auth_events.iter().any(|evt| match evt {
            PartEvent::Changed(changed) => {
                changed.obj_id == obj && changed.part_ids == vec![part.clone()]
            }
            _ => false,
        }),
        "authorized subscriber must receive the document event during replay; got {auth_events:?}"
    );

    // The denied peer's answer is the responder's verdict, not a filtered stream:
    // the reader is the store's unfiltered seam, so the denial is asserted where it
    // is enforced — the page refuses the target, and the document's event itself is
    // unreadable to that peer.
    let target = big_sync_core::rpc::SubscriptionTarget::Part {
        part_id: part.clone(),
        cursor: 0,
    };
    let denied_outcome = store
        .replay_page_for_target(target, 16, denied_peer.clone(), Duration::from_millis(50))
        .await?;
    assert!(
        matches!(verdict(&denied_outcome), TargetVerdict::Unauthorized),
        "a peer with no access must be refused the target; got {denied_outcome:?}"
    );
    let transition = PartEvent::Changed(ObjChanged {
        cursor: 0,
        part_ids: vec![part.clone()],
        obj_id: obj.clone(),
        payload: payload("replay-filter", 1),
    });
    assert!(
        !store
            .page_event_is_readable(&transition, denied_peer.clone())
            .await?,
        "the denied peer must not be able to read the document's event"
    );
    assert!(
        store
            .page_event_is_readable(&transition, auth_peer.clone())
            .await?,
        "the authorized peer reads the same event"
    );
    Ok(())
}

pub async fn assert_subscribe_live_filtering_contract<H>(harness: &H) -> Res<()>
where
    H: HostPartStoreContractHarness + Sync,
{
    let store = harness.store();
    let part = test_part(71);
    let overlapping_part = test_part(76);
    let obj = test_obj(72);
    let auth_peer = big_sync_core::PeerKey::new([73u8; 32]);
    let relay_peer = big_sync_core::PeerKey::new([74u8; 32]);
    let denied_peer = big_sync_core::PeerKey::new([75u8; 32]);

    store.ensure_part(part.clone()).await?;
    store.ensure_part(overlapping_part.clone()).await?;

    // Seed the doc before any subscriptions.
    store
        .set_obj_payload(obj.clone(), payload("live-filter", 1))
        .await?;
    store
        .add_obj_to_parts(obj.clone(), vec![part.clone(), overlapping_part.clone()])
        .await?;

    // Grant both parts the object is in: auth_peer has Read, relay_peer has
    // Relay, denied_peer has no row in either (explicitly denied).
    for part in [part.clone(), overlapping_part.clone()] {
        store
            .set_part_members(
                part,
                std::collections::HashMap::from([
                    (auth_peer.clone(), Access::Read),
                    (relay_peer.clone(), Access::Relay),
                ]),
            )
            .await?;
    }

    // Subscribe all three and drain through ReplayComplete so each
    // is registered for live events.
    // The async block moves the two part keys in, so each call needs its own
    // copies: the closure is invoked once per subscriber below.
    let sub = |peer| {
        let part = part.clone();
        let overlapping_part = overlapping_part.clone();
        async move {
            store
                .page_events(
                    SubPartsRequest {
                        lower_bound: 0,
                        targets: HashSet::from([
                            big_sync_core::rpc::SubscriptionTarget::Part {
                                part_id: part,
                                cursor: 0,
                            },
                            big_sync_core::rpc::SubscriptionTarget::Part {
                                part_id: overlapping_part,
                                cursor: 0,
                            },
                        ]),
                    },
                    peer,
                )
                .await?
                .map_err(eyre::Report::from)
        }
    };
    let auth_rx = sub(auth_peer).await?;
    let relay_rx = sub(relay_peer).await?;
    let denied_rx = sub(denied_peer.clone()).await?;

    collect_sub_events(&auth_rx).await?;
    collect_sub_events(&relay_rx).await?;
    collect_sub_events(&denied_rx).await?;

    // Now all three are subscribed for live events.  Mutate the doc.
    store
        .set_obj_payload(obj.clone(), payload("live-filter", 2))
        .await?;

    // Authorized (Read) must receive the live Changed event for each subscribed partition.
    // A change to one object is one logical event, even when it has
    // multiple subscribed part tags.
    let auth_live = recv_sub_event(&auth_rx).await?;
    let PartEvent::Changed(auth_changed) = auth_live else {
        panic!("authorized subscriber expected Changed, got {auth_live:?}");
    };
    assert_eq!(auth_changed.obj_id, obj);
    assert_eq!(auth_changed.payload, payload("live-filter", 2));
    assert_eq!(
        auth_changed.part_ids.into_iter().collect::<HashSet<_>>(),
        HashSet::from([part.clone(), overlapping_part.clone()]),
        "one live event must cover every subscribed part",
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), auth_rx.next())
            .await
            .is_err(),
        "multi-part change must not emit duplicate logical events",
    );

    let relay_live = recv_sub_event(&relay_rx).await?;
    let PartEvent::Changed(relay_changed) = relay_live else {
        panic!("relay subscriber expected Changed, got {relay_live:?}");
    };
    assert_eq!(relay_changed.obj_id, obj);
    assert_eq!(relay_changed.payload, payload("live-filter", 2));
    assert_eq!(
        relay_changed.part_ids.into_iter().collect::<HashSet<_>>(),
        HashSet::from([part.clone(), overlapping_part.clone()]),
        "one relay event must cover every subscribed part",
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), relay_rx.next())
            .await
            .is_err(),
        "relay multi-part change must not be duplicated",
    );

    // Denied subscriber must not receive any live document event. The denial is the
    // responder's refusal, not a stream filter: the reader is the store's unfiltered
    // seam, so the property is asserted where it is enforced — and on the live path
    // the refusal is asked again after the change, which is what "must not receive"
    // means once nothing is pushed to anyone.
    let denied_target = big_sync_core::rpc::SubscriptionTarget::Part {
        part_id: part.clone(),
        cursor: 0,
    };
    let refused = store
        .replay_page_for_target(
            denied_target,
            16,
            denied_peer.clone(),
            Duration::from_millis(50),
        )
        .await?;
    assert!(
        matches!(verdict(&refused), TargetVerdict::Unauthorized),
        "a denied peer must be refused the part's page; got {refused:?}"
    );
    assert!(
        !store
            .page_event_is_readable(
                &PartEvent::Changed(ObjChanged {
                    cursor: 0,
                    part_ids: vec![part.clone()],
                    obj_id: obj.clone(),
                    payload: payload("live-filter", 1),
                }),
                denied_peer.clone(),
            )
            .await?,
        "the denied peer must not be able to read the document's live event"
    );

    Ok(())
}

pub async fn assert_subscription_semantics_contract<H>(harness: &H) -> Res<()>
where
    H: HostPartStoreContractHarness + Sync,
{
    #[expect(clippy::too_many_arguments)]
    async fn run_case(
        store: &dyn HostPartStore,
        mode: u8,
        order: u8,
        obj: ObjKey,
        part_a: PartKey,
        part_b: PartKey,
        peer: PeerKey,
        live: bool,
    ) -> Res<Vec<PartEvent>> {
        store.ensure_part(part_a.clone()).await?;
        store.ensure_part(part_b.clone()).await?;
        for part in [part_a.clone(), part_b.clone()] {
            store
                .set_part_members(part, HashMap::from([(peer.clone(), Access::Read)]))
                .await?;
        }
        store
            .set_obj_payload(obj.clone(), payload("matrix", 0))
            .await?;
        store
            .add_obj_to_parts(obj.clone(), vec![part_a.clone(), part_b.clone()])
            .await?;
        let baseline_cursor = store
            .list_events(HashSet::from([part_a.clone(), part_b.clone()]), 0, u32::MAX)
            .await??
            .values()
            .flat_map(|page| page.events.iter())
            .map(|event| match event {
                PartEvent::Changed(inner) => inner.cursor,
                PartEvent::Removed(inner) => inner.cursor,
            })
            .max()
            .unwrap_or_default();
        let mut targets = HashSet::new();
        if mode != 1 {
            targets.insert(big_sync_core::rpc::SubscriptionTarget::Part {
                part_id: part_a.clone(),
                cursor: baseline_cursor,
            });
            targets.insert(big_sync_core::rpc::SubscriptionTarget::Part {
                part_id: part_b,
                cursor: baseline_cursor,
            });
        }
        if mode != 0 {
            targets.insert(big_sync_core::rpc::SubscriptionTarget::Object {
                obj_id: obj.clone(),
                cursor: 0,
            });
        }
        let request = SubPartsRequest {
            lower_bound: baseline_cursor,
            targets,
        };
        let operations: &[u8] = match order {
            0 => &[0],
            1 => &[1, 2],
            2 => &[0, 1],
            _ => unreachable!("unknown subscription mutation order"),
        };
        if !live {
            for operation in operations {
                match operation {
                    0 => {
                        store
                            .set_obj_payload(obj.clone(), payload("matrix", 1))
                            .await?
                    }
                    1 => {
                        store
                            .remove_obj_from_part(obj.clone(), part_a.clone())
                            .await?
                    }
                    2 => {
                        store
                            .set_obj_payload(obj.clone(), payload("matrix", 2))
                            .await?
                    }
                    _ => unreachable!("unknown subscription mutation"),
                }
            }
        }
        let rx = store.page_events(request, peer).await??;
        if !live {
            return collect_sub_events(&rx).await;
        }
        let mut events = collect_sub_events(&rx).await?;
        for operation in operations {
            match operation {
                0 => {
                    store
                        .set_obj_payload(obj.clone(), payload("matrix", 1))
                        .await?
                }
                1 => {
                    store
                        .remove_obj_from_part(obj.clone(), part_a.clone())
                        .await?
                }
                2 => {
                    store
                        .set_obj_payload(obj.clone(), payload("matrix", 2))
                        .await?
                }
                _ => unreachable!("unknown subscription mutation"),
            }
            events.push(recv_sub_event(&rx).await?);
        }
        while let Ok(Ok(event)) = tokio::time::timeout(Duration::from_millis(150), rx.next()).await
        {
            events.push(event);
        }
        Ok(events)
    }
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct CanonicalState {
        payload: Option<ObjPayload>,
        live_parts: BTreeSet<PartKey>,
    }

    struct EventLedger {
        mode: u8,
        obj: ObjKey,
        requested_parts: BTreeSet<PartKey>,
        state: CanonicalState,
        last_cursor: Option<CursorIndex>,
        changed_groups: HashMap<(CursorIndex, ObjKey), BTreeSet<PartKey>>,
        violations: Vec<String>,
    }

    impl EventLedger {
        fn new(mode: u8, obj: ObjKey, part_a: PartKey, part_b: PartKey) -> Self {
            Self {
                mode,
                obj,
                requested_parts: BTreeSet::from([part_a, part_b]),
                state: CanonicalState {
                    payload: None,
                    live_parts: BTreeSet::new(),
                },
                last_cursor: None,
                changed_groups: HashMap::new(),
                violations: Vec::new(),
            }
        }

        fn cursor(&mut self, cursor: CursorIndex) {
            if let Some(last) = self.last_cursor
                && cursor < last
            {
                self.violations
                    .push(format!("cursor regressed from {last} to {cursor}"));
            }
            self.last_cursor = Some(self.last_cursor.map_or(cursor, |last| last.max(cursor)));
        }

        fn check_part(&mut self, part_id: PartKey, event: &str) {
            if self.mode == 1 || !self.requested_parts.contains(&part_id) {
                self.violations.push(format!(
                    "{event} projected invalid part {part_id:?} for subscription mode {}",
                    self.mode
                ));
            }
        }

        fn check_changed_projection(&mut self, part_ids: &[PartKey]) {
            if self.mode == 1 && !part_ids.is_empty() {
                self.violations.push(format!(
                    "object-target Changed contained real parts: {part_ids:?}"
                ));
            }
            if part_ids
                .iter()
                .any(|part| !self.requested_parts.contains(part))
            {
                self.violations.push(format!(
                    "Changed contained an unsubscribed part: {part_ids:?}"
                ));
            }
        }

        fn observe(&mut self, event: PartEvent) {
            match event {
                PartEvent::Changed(inner) => {
                    self.cursor(inner.cursor);
                    if inner.obj_id != self.obj {
                        self.violations.push(format!(
                            "Changed targeted {:?}, expected {:?}",
                            inner.obj_id, self.obj
                        ));
                    }
                    self.check_changed_projection(&inner.part_ids);
                    self.state.payload = Some(inner.payload);
                    self.state.live_parts.extend(inner.part_ids.iter().cloned());
                    self.changed_groups
                        .entry((inner.cursor, inner.obj_id))
                        .or_default()
                        .extend(inner.part_ids);
                }
                PartEvent::Removed(inner) => {
                    self.cursor(inner.cursor);
                    if inner.obj_id != self.obj {
                        self.violations.push(format!(
                            "Removed targeted {:?}, expected {:?}",
                            inner.obj_id, self.obj
                        ));
                    }
                    self.check_part(inner.part_id.clone(), "Removed");
                    self.state.live_parts.remove(&inner.part_id);
                }
            }
        }

        fn finish(self, expected: CanonicalState) -> CanonicalState {
            let mut violations = self.violations;
            if self.state != expected {
                violations.push(format!(
                    "canonical state mismatch: observed {:?}, expected {:?}",
                    self.state, expected
                ));
            }
            assert!(
                violations.is_empty(),
                "unresolved subscription violations: {violations:?}"
            );
            self.state
        }
    }
    fn canonical_state(
        mode: u8,
        obj: ObjKey,
        part_a: PartKey,
        part_b: PartKey,
        events: Vec<PartEvent>,
        expected: CanonicalState,
    ) -> CanonicalState {
        let mut ledger = EventLedger::new(mode, obj, part_a, part_b);
        for event in events {
            ledger.observe(event);
        }
        ledger.finish(expected)
    }

    let store = harness.store();
    let peer = PeerKey::new([90u8; 32]);
    for (mode, order, seed) in [
        (0u8, 0u8, 91u8),
        (0, 1, 94),
        (0, 2, 97),
        (1, 0, 100),
        (2, 0, 103),
    ] {
        let part_a = test_part(seed);
        let part_b = test_part(seed + 1);
        let replay = run_case(
            store,
            mode,
            order,
            test_obj(seed + 2),
            part_a.clone(),
            part_b.clone(),
            peer.clone(),
            false,
        )
        .await?;
        let live = run_case(
            store,
            mode,
            order,
            test_obj(seed + 3),
            part_a.clone(),
            part_b.clone(),
            peer.clone(),
            true,
        )
        .await?;
        let expected = CanonicalState {
            payload: Some(payload(
                "matrix",
                match order {
                    0 | 2 => 1,
                    1 => 2,
                    _ => unreachable!("unknown mutation order"),
                },
            )),
            live_parts: if mode == 1 {
                BTreeSet::new()
            } else if order == 0 {
                BTreeSet::from([part_a.clone(), part_b.clone()])
            } else {
                BTreeSet::from([part_b.clone()])
            },
        };
        let replay_state = canonical_state(
            mode,
            test_obj(seed + 2),
            part_a.clone(),
            part_b.clone(),
            replay,
            expected.clone(),
        );
        let live_state = canonical_state(
            mode,
            test_obj(seed + 3),
            part_a,
            part_b,
            live,
            expected.clone(),
        );
        assert_eq!(
            replay_state, live_state,
            "replay and live canonical states diverged for subscription mode {mode}, order {order}",
        );
    }

    // Partless objects keep their lane locally, where filtering does not apply:
    // replay and live must still converge there.
    async fn run_zero_part_local_case(
        store: &dyn HostPartStore,
        obj: ObjKey,
        live: bool,
    ) -> Res<Vec<PartEvent>> {
        if !live {
            store
                .set_obj_payload(obj.clone(), payload("zero-part", 1))
                .await?;
        }
        let rx = store
            .page_events_local(SubPartsRequest {
                lower_bound: 0,
                targets: HashSet::from([big_sync_core::rpc::SubscriptionTarget::Object {
                    obj_id: obj.clone(),
                    cursor: 0,
                }]),
            })
            .await??;
        if live {
            let mut events = collect_sub_events(&rx).await?;
            store.set_obj_payload(obj, payload("zero-part", 1)).await?;
            events.push(recv_sub_event(&rx).await?);
            Ok(events)
        } else {
            collect_sub_events(&rx).await
        }
    }

    // Access is granted per part and a partless object is in no part, so it has no
    // remote authorization: the responder refuses it outright (fail-closed) rather
    // than falling back to an unauthenticated object lane, and the object's own event
    // is unreadable to that peer, so no part id can leak. The reader is the store's
    // unfiltered seam, so the property is asserted where it is enforced.
    async fn run_zero_part_remote_case(
        store: &dyn HostPartStore,
        obj: ObjKey,
        peer: PeerKey,
        live: bool,
    ) -> Res<()> {
        if !live {
            store
                .set_obj_payload(obj.clone(), payload("zero-part", 1))
                .await?;
        }
        let target = big_sync_core::rpc::SubscriptionTarget::Object {
            obj_id: obj.clone(),
            cursor: 0,
        };
        let refused = store
            .replay_page_for_target(target.clone(), 16, peer.clone(), Duration::from_millis(50))
            .await?;
        assert!(
            matches!(verdict(&refused), TargetVerdict::Unauthorized),
            "a partless object must not be delivered to a remote subscriber (live={live}); got {refused:?}"
        );
        assert!(
            !store
                .page_event_is_readable(
                    &PartEvent::Changed(ObjChanged {
                        cursor: 0,
                        part_ids: Vec::new(),
                        obj_id: obj.clone(),
                        payload: payload("zero-part", 1),
                    }),
                    peer.clone(),
                )
                .await?,
            "a partless object's event must not be readable by a remote peer (live={live})"
        );
        if live {
            store.set_obj_payload(obj, payload("zero-part", 1)).await?;
            let refused_again = store
                .replay_page_for_target(target, 16, peer, Duration::from_millis(50))
                .await?;
            assert!(
                matches!(verdict(&refused_again), TargetVerdict::Unauthorized),
                "a partless object stays refused on the live path (live={live}); got {refused_again:?}"
            );
        }
        Ok(())
    }

    // The local lane is unfiltered, so partless replay and live still converge.
    let zero_replay = run_zero_part_local_case(store, test_obj(180), false).await?;
    let zero_live = run_zero_part_local_case(store, test_obj(182), true).await?;
    let zero_expected = CanonicalState {
        payload: Some(payload("zero-part", 1)),
        live_parts: BTreeSet::new(),
    };
    let zero_replay_state = canonical_state(
        1,
        test_obj(180),
        test_part(0),
        test_part(1),
        zero_replay,
        zero_expected.clone(),
    );
    let zero_live_state = canonical_state(
        1,
        test_obj(182),
        test_part(0),
        test_part(1),
        zero_live,
        zero_expected,
    );
    assert_eq!(
        zero_replay_state, zero_live_state,
        "zero-real-part object replay and live LOCAL object subscriptions must converge",
    );

    // Partless objects have no remote authorization until virtual parts land: refused
    // on both the replay and the live path, and no part ids disclosed either way.
    for (live, obj_seed, peer_seed) in [(false, 184u8, 186u8), (true, 185, 187)] {
        run_zero_part_remote_case(
            store,
            test_obj(obj_seed),
            PeerKey::new([peer_seed; 32]),
            live,
        )
        .await?;
    }

    async fn run_zero_mixed_case(
        store: &dyn HostPartStore,
        obj: ObjKey,
        part: PartKey,
        peer: PeerKey,
        live: bool,
    ) -> Res<Vec<PartEvent>> {
        store.ensure_part(part.clone()).await?;
        store
            .set_part_members(part.clone(), HashMap::from([(peer.clone(), Access::Read)]))
            .await?;
        store
            .set_obj_payload(obj.clone(), payload("mixed", 0))
            .await?;
        store
            .add_obj_to_parts(obj.clone(), vec![part.clone()])
            .await?;
        let baseline = store
            .list_events(HashSet::from([part.clone()]), 0, u32::MAX)
            .await??
            .values()
            .flat_map(|page| page.events.iter())
            .map(|event| match event {
                PartEvent::Changed(inner) => inner.cursor,
                PartEvent::Removed(inner) => inner.cursor,
            })
            .max()
            .unwrap_or_default();
        if !live {
            store
                .set_obj_payload(obj.clone(), payload("mixed", 1))
                .await?;
        }
        let rx = store
            .page_events(
                SubPartsRequest {
                    lower_bound: baseline,
                    targets: HashSet::from([
                        big_sync_core::rpc::SubscriptionTarget::Part {
                            part_id: part,
                            cursor: baseline,
                        },
                        big_sync_core::rpc::SubscriptionTarget::Object {
                            obj_id: obj.clone(),
                            cursor: 0,
                        },
                    ]),
                },
                peer,
            )
            .await??;
        if live {
            let mut events = collect_sub_events(&rx).await?;
            store.set_obj_payload(obj, payload("mixed", 1)).await?;
            events.push(recv_sub_event(&rx).await?);
            while let Ok(Ok(event)) =
                tokio::time::timeout(Duration::from_millis(150), rx.next()).await
            {
                events.push(event);
            }
            Ok(events)
        } else {
            collect_sub_events(&rx).await
        }
    }

    async fn run_populated_object_case(
        store: &dyn HostPartStore,
        obj: ObjKey,
        part: PartKey,
        peer: PeerKey,
        live: bool,
    ) -> Res<Vec<PartEvent>> {
        store.ensure_part(part.clone()).await?;
        store
            .set_part_members(part.clone(), HashMap::from([(peer.clone(), Access::Read)]))
            .await?;
        store
            .set_obj_payload(obj.clone(), payload("object-only", 0))
            .await?;
        store
            .add_obj_to_parts(obj.clone(), vec![part.clone()])
            .await?;
        let baseline = store
            .list_events(HashSet::from([part]), 0, u32::MAX)
            .await??
            .values()
            .flat_map(|page| page.events.iter())
            .map(|event| match event {
                PartEvent::Changed(inner) => inner.cursor,
                PartEvent::Removed(inner) => inner.cursor,
            })
            .max()
            .unwrap_or_default();

        if !live {
            store
                .set_obj_payload(obj.clone(), payload("object-only", 1))
                .await?;
        }
        let rx = store
            .page_events(
                SubPartsRequest {
                    lower_bound: baseline,
                    targets: HashSet::from([big_sync_core::rpc::SubscriptionTarget::Object {
                        obj_id: obj.clone(),
                        cursor: 0,
                    }]),
                },
                peer,
            )
            .await??;
        if live {
            let mut events = collect_sub_events(&rx).await?;
            store
                .set_obj_payload(obj, payload("object-only", 1))
                .await?;
            events.push(recv_sub_event(&rx).await?);
            Ok(events)
        } else {
            collect_sub_events(&rx).await
        }
    }

    let populated_object_replay = run_populated_object_case(
        store,
        test_obj(190),
        test_part(191),
        PeerKey::new([192; 32]),
        false,
    )
    .await?;
    let populated_object_live = run_populated_object_case(
        store,
        test_obj(193),
        test_part(194),
        PeerKey::new([195; 32]),
        true,
    )
    .await?;
    let object_expected = CanonicalState {
        payload: Some(payload("object-only", 1)),
        live_parts: BTreeSet::new(),
    };
    let populated_object_replay_state = canonical_state(
        1,
        test_obj(190),
        test_part(191),
        test_part(191),
        populated_object_replay,
        object_expected.clone(),
    );
    let populated_object_live_state = canonical_state(
        1,
        test_obj(193),
        test_part(194),
        test_part(194),
        populated_object_live,
        object_expected,
    );
    assert_eq!(
        populated_object_replay_state, populated_object_live_state,
        "populated object replay and live subscriptions must converge",
    );

    let mixed_replay_part = test_part(185);
    let mixed_replay = run_zero_mixed_case(
        store,
        test_obj(184),
        mixed_replay_part.clone(),
        PeerKey::new([186; 32]),
        false,
    )
    .await?;
    let mixed_live_part = test_part(188);
    let mixed_live = run_zero_mixed_case(
        store,
        test_obj(187),
        mixed_live_part.clone(),
        PeerKey::new([189; 32]),
        true,
    )
    .await?;
    let mixed_replay_expected = CanonicalState {
        payload: Some(payload("mixed", 1)),
        live_parts: BTreeSet::from([mixed_replay_part.clone()]),
    };
    let mixed_replay_state = canonical_state(
        2,
        test_obj(184),
        mixed_replay_part.clone(),
        mixed_replay_part,
        mixed_replay,
        mixed_replay_expected,
    );
    let mixed_live_expected = CanonicalState {
        payload: Some(payload("mixed", 1)),
        live_parts: BTreeSet::from([mixed_live_part.clone()]),
    };
    let mixed_live_state = canonical_state(
        2,
        test_obj(187),
        mixed_live_part.clone(),
        mixed_live_part,
        mixed_live,
        mixed_live_expected,
    );
    assert_eq!(
        (
            &mixed_replay_state.payload,
            mixed_replay_state.live_parts.is_empty(),
            mixed_replay_state.live_parts.len(),
        ),
        (
            &mixed_live_state.payload,
            mixed_live_state.live_parts.is_empty(),
            mixed_live_state.live_parts.len(),
        ),
        "mixed object/part replay and live subscriptions must converge semantically",
    );

    Ok(())
}

pub async fn assert_subscribe_per_part_cursor_contract<H>(harness: &H) -> Res<()>
where
    H: HostPartStoreContractHarness + Sync,
{
    let store = harness.store();
    let part_a = test_part(81);
    let part_b = test_part(82);
    let obj = test_obj(83);
    let peer = big_sync_core::PeerKey::new([84u8; 32]);

    store.ensure_part(part_a.clone()).await?;
    store.ensure_part(part_b.clone()).await?;
    for part in [part_a.clone(), part_b.clone()] {
        store
            .set_part_members(
                part,
                HashMap::from([(peer.clone(), keyhive_core::access::Access::Read)]),
            )
            .await?;
    }
    // Build events: cursor 1-4 only for part_a, 5-6 involve part_b.
    // First set payload while obj has no parts (no event recorded).
    store
        .set_obj_payload(obj.clone(), payload("per-cursor", 1))
        .await?;
    store
        .add_obj_to_parts(obj.clone(), vec![part_a.clone()])
        .await?;
    // cursor=1: Added obj, part_a
    store
        .set_obj_payload(obj.clone(), payload("per-cursor", 2))
        .await?;
    // cursor=2: Changed [part_a]
    store
        .set_obj_payload(obj.clone(), payload("per-cursor", 3))
        .await?;
    // cursor=3: Changed [part_a]
    store
        .set_obj_payload(obj.clone(), payload("per-cursor", 4))
        .await?;
    // cursor=4: Changed [part_a]

    store
        .add_obj_to_parts(obj.clone(), vec![part_b.clone()])
        .await?;
    // cursor=5: Added obj, part_b
    store
        .set_obj_payload(obj.clone(), payload("per-cursor", 5))
        .await?;
    // cursor=6: Changed [part_a, part_b]

    // The request lower bound is shared by all targets. The latest-state
    // replay returns one Changed event for the payload mutation spanning
    // both parts, rather than replaying stale Added events.
    let rx = store
        .page_events(
            SubPartsRequest {
                lower_bound: 0,
                targets: HashSet::from([
                    big_sync_core::rpc::SubscriptionTarget::Part {
                        part_id: part_a.clone(),
                        cursor: 0,
                    },
                    big_sync_core::rpc::SubscriptionTarget::Part {
                        part_id: part_b.clone(),
                        cursor: 0,
                    },
                ]),
            },
            peer,
        )
        .await??;
    let events = collect_sub_events(&rx).await?;

    let changes: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            PartEvent::Changed(changed) if changed.obj_id == obj => Some(changed),
            _ => None,
        })
        .collect();
    assert_eq!(
        changes.len(),
        1,
        "one logical multi-part change must produce one replay message: {events:?}",
    );
    assert_eq!(
        changes[0].part_ids.iter().cloned().collect::<HashSet<_>>(),
        HashSet::from([part_a, part_b]),
    );
    Ok(())
}

/// A part target's cursor is resolved against the request's lower bound per target, not once
/// for every target in the request.
///
/// A reader that saw a membership start and asks from a lower bound below it is still handed
/// that member's tombstone, because the target's own cursor wins the `max` — and a sibling
/// part asking from that same lower bound is not dragged up to it. A store that resolves one
/// bound for the whole request loses the tombstone; a store that folds the targets together
/// loses the sibling's earlier row.
pub async fn assert_subscribe_part_target_bounds_are_per_target_contract<H>(harness: &H) -> Res<()>
where
    H: HostPartStoreContractHarness + Sync,
{
    /// The newest cursor a part reports from `from` on.
    async fn last_cursor(
        store: &dyn HostPartStore,
        part: &PartKey,
        from: CursorIndex,
    ) -> Res<CursorIndex> {
        let page = store
            .list_events(HashSet::from([part.clone()]), from, 8)
            .await??
            .remove(part)
            .expect(ERROR_IMPOSSIBLE);
        Ok(page
            .events
            .iter()
            .map(|event| match event {
                PartEvent::Changed(changed) => changed.cursor,
                PartEvent::Removed(removed) => removed.cursor,
            })
            .max()
            .expect(ERROR_IMPOSSIBLE))
    }

    let store = harness.store();
    let part_a = test_part(0xe4);
    let part_b = test_part(0xe5);
    let obj_a = test_obj(0xe6);
    let obj_b = test_obj(0xe7);
    let reader = PeerKey::new([0xe8u8; 32]);

    store.ensure_part(part_a.clone()).await?;
    store.ensure_part(part_b.clone()).await?;
    for part in [part_a.clone(), part_b.clone()] {
        store
            .set_part_members(part, HashMap::from([(reader.clone(), Access::Read)]))
            .await?;
    }

    // The sibling's row lands first, so a store that folded the targets onto one bound
    // would page it from `added_at` and skip it.
    seed_live_obj(
        store,
        obj_b.clone(),
        payload("per-target-b", 1),
        std::slice::from_ref(&part_b),
    )
    .await?;
    let sibling_cursor = last_cursor(store, &part_b, 0).await?;
    seed_live_obj(
        store,
        obj_a.clone(),
        payload("per-target-a", 1),
        std::slice::from_ref(&part_a),
    )
    .await?;
    let added_at = last_cursor(store, &part_a, 0).await?;
    store
        .remove_obj_from_part(obj_a.clone(), part_a.clone())
        .await?;
    let removed_at = last_cursor(store, &part_a, added_at).await?;
    assert!(
        removed_at > added_at,
        "the tombstone cursor is newer than the add it removes"
    );
    assert!(
        sibling_cursor < added_at,
        "the sibling's row must sit below the other target's cursor, or this case proves nothing"
    );

    let rx = store
        .page_events(
            SubPartsRequest {
                lower_bound: 0,
                targets: HashSet::from([
                    SubscriptionTarget::Part {
                        part_id: part_a.clone(),
                        cursor: added_at,
                    },
                    SubscriptionTarget::Part {
                        part_id: part_b.clone(),
                        cursor: 0,
                    },
                ]),
            },
            reader,
        )
        .await??;
    let events = collect_sub_events(&rx).await?;

    assert!(
        events.contains(&PartEvent::Removed(ObjRemovedFromPart {
            cursor: removed_at,
            part_id: part_a.clone(),
            obj_id: obj_a.clone(),
        })),
        "a target asking from its own cursor keeps the tombstone below the shared lower bound: {events:?}"
    );
    assert!(
        events.iter().any(|event| matches!(
            event,
            PartEvent::Changed(changed)
                if changed.obj_id == obj_b && changed.part_ids == vec![part_b.clone()]
        )),
        "a sibling part asking from the lower bound is not dragged up to another target's cursor: {events:?}"
    );
    Ok(())
}

/// In a mixed subscription the object route keeps its own position: part events do not
/// advance it.
///
/// A part target asking from far ahead must not cost the object route its backlog. The routes
/// have separate cursors, so resolving one bound for the request as a whole silently drops
/// the object's events that sit below the part's.
pub async fn assert_mixed_part_and_object_subscription_cursors_are_independent<H>(
    harness: &H,
) -> Res<()>
where
    H: HostPartStoreContractHarness + Sync,
{
    let store = harness.store();
    let part = test_part(0xe9);
    let obj_backlog = test_obj(0xea);
    let obj_other = test_obj(0xeb);
    let reader = PeerKey::new([0xecu8; 32]);

    store.ensure_part(part.clone()).await?;
    store
        .set_part_members(
            part.clone(),
            HashMap::from([(reader.clone(), Access::Read)]),
        )
        .await?;
    seed_live_obj(
        store,
        obj_backlog.clone(),
        payload("mixed", 0),
        std::slice::from_ref(&part),
    )
    .await?;
    let first = store
        .list_events(HashSet::from([part.clone()]), 0, 8)
        .await??
        .remove(&part)
        .expect(ERROR_IMPOSSIBLE)
        .events
        .iter()
        .map(|event| match event {
            PartEvent::Changed(changed) => changed.cursor,
            PartEvent::Removed(removed) => removed.cursor,
        })
        .max()
        .expect(ERROR_IMPOSSIBLE);
    store
        .set_obj_payload(obj_backlog.clone(), payload("mixed", 1))
        .await?;
    let second = store
        .list_events(HashSet::from([part.clone()]), 0, 8)
        .await??
        .remove(&part)
        .expect(ERROR_IMPOSSIBLE)
        .events
        .iter()
        .map(|event| match event {
            PartEvent::Changed(changed) => changed.cursor,
            PartEvent::Removed(removed) => removed.cursor,
        })
        .max()
        .expect(ERROR_IMPOSSIBLE);
    // A second object puts the part's own cursor above the whole backlog.
    seed_live_obj(
        store,
        obj_other.clone(),
        payload("mixed", 2),
        std::slice::from_ref(&part),
    )
    .await?;
    let part_cursor = store
        .list_events(HashSet::from([part.clone()]), 0, 8)
        .await??
        .remove(&part)
        .expect(ERROR_IMPOSSIBLE)
        .events
        .iter()
        .map(|event| match event {
            PartEvent::Changed(changed) => changed.cursor,
            PartEvent::Removed(removed) => removed.cursor,
        })
        .max()
        .expect(ERROR_IMPOSSIBLE);
    assert!(
        first < second && second < part_cursor,
        "the part's newest cursor must sit above the object's backlog: {first} {second} {part_cursor}"
    );

    let rx = store
        .page_events(
            SubPartsRequest {
                lower_bound: 0,
                targets: HashSet::from([
                    SubscriptionTarget::Part {
                        part_id: part.clone(),
                        cursor: part_cursor,
                    },
                    SubscriptionTarget::Object {
                        obj_id: obj_backlog.clone(),
                        cursor: 0,
                    },
                ]),
            },
            reader,
        )
        .await??;
    let events = collect_sub_events(&rx).await?;
    let backlog: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            PartEvent::Changed(changed) if changed.obj_id == obj_backlog => Some(changed.cursor),
            _ => None,
        })
        .collect();
    // Which of the object's own events a route reports is its projection's business — the
    // part-less write and the member write can both appear — but the route must not have been
    // dragged up to the part target's cursor, which would leave the backlog empty.
    assert_eq!(
        backlog.iter().max().copied(),
        Some(second),
        "the object route replays the object's own events below the part target's cursor: {events:?}"
    );
    assert!(
        backlog.iter().all(|cursor| *cursor < part_cursor),
        "no event above the part target's cursor is part of the backlog: {events:?}"
    );
    Ok(())
}

pub async fn assert_list_events_pagination_contract<H>(harness: &H) -> Res<()>
where
    H: HostPartStoreContractHarness + Sync,
{
    let store = harness.store();
    let part = test_part(151);
    let objs = [test_obj(152), test_obj(153), test_obj(154)];

    store.ensure_part(part.clone()).await?;

    // Create three distinct events on the same part, each at a distinct cursor.
    for (ii, obj_id) in objs.iter().enumerate() {
        store
            .set_obj_payload(obj_id.clone(), payload("pagination", ii as u64))
            .await?;
        store
            .add_obj_to_parts(obj_id.clone(), vec![part.clone()])
            .await?;
    }

    // Paginate with limit=1, following `resume` until the page reports drained.
    let mut cursor = 0;
    let mut collected: Vec<(CursorIndex, ObjKey)> = Vec::new();
    loop {
        let page = store
            .list_events(HashSet::from([part.clone()]), cursor, 1)
            .await??
            .remove(&part)
            .expect(ERROR_IMPOSSIBLE);
        for evt in &page.events {
            match evt {
                PartEvent::Changed(changed) => {
                    collected.push((changed.cursor, changed.obj_id.clone()));
                }
                PartEvent::Removed(_) => {
                    panic!("unexpected event type for single-object part");
                }
            }
        }
        cursor = page.resume;
        if page.drained {
            break;
        }
    }

    // Every event must be returned exactly once, in order.
    assert_eq!(
        collected.len(),
        objs.len(),
        "expected {} events via pagination, got {collected:?}",
        objs.len(),
    );
    for (ii, expected_obj_id) in objs.iter().enumerate() {
        let (retrieved_cursor, retrieved_obj_id) = collected[ii].clone();
        assert_eq!(
            retrieved_obj_id,
            expected_obj_id.clone(),
            "event {ii}: expected obj {expected_obj_id}, got {retrieved_obj_id}"
        );
        if ii > 0 {
            assert!(
                retrieved_cursor > collected[ii - 1].0,
                "event {ii} cursor {} not after previous cursor {}",
                retrieved_cursor,
                collected[ii - 1].0,
            );
        }
    }
    Ok(())
}

pub async fn assert_peer_cursor_monotonicity_contract<H>(harness: &H) -> Res<()>
where
    H: HostPartStoreContractHarness + Sync,
{
    let store = harness.store();
    let part = test_part(101);
    let peer = big_sync_core::PeerKey::new([102u8; 32]);

    store.ensure_part(part.clone()).await?;

    // Set to a higher cursor, then attempt regression.
    store
        .set_peer_part_cursor(peer.clone(), part.clone(), 42)
        .await?;
    assert_eq!(
        store
            .get_peer_part_cursor(peer.clone(), part.clone())
            .await?,
        42,
        "initial cursor should be 42"
    );

    // Attempt to regress: setting to 5 must be a no-op.
    store
        .set_peer_part_cursor(peer.clone(), part.clone(), 5)
        .await?;
    let cursor = store.get_peer_part_cursor(peer, part).await?;
    assert!(
        cursor >= 42,
        "peer part cursor regressed from 42 to {cursor}"
    );
    Ok(())
}

pub async fn assert_remove_obj_advances_latest_cursor_contract<H>(harness: &H) -> Res<()>
where
    H: HostPartStoreContractHarness + Sync,
{
    let store = harness.store();
    let part = test_part(111);
    let obj = test_obj(112);

    store.ensure_part(part.clone()).await?;
    store
        .set_obj_payload(obj.clone(), payload("cursor-adv", 1))
        .await?;
    store
        .add_obj_to_parts(obj.clone(), vec![part.clone()])
        .await?;

    let summaries_after_add = store
        .summarize_parts(HashSet::from([part.clone()]))
        .await??;
    let cursor_after_add = summaries_after_add
        .get(&part)
        .expect("summary must contain part")
        .latest_cursor;

    store
        .remove_obj_from_part(obj.clone(), part.clone())
        .await?;

    let summaries_after_remove = store
        .summarize_parts(HashSet::from([part.clone()]))
        .await??;
    let cursor_after_remove = summaries_after_remove
        .get(&part)
        .expect("summary must contain part")
        .latest_cursor;

    assert!(
        cursor_after_remove > cursor_after_add,
        "removing an object from a part must advance latest_cursor: initial={cursor_after_add}, post_remove={cursor_after_remove}"
    );

    let page = store
        .list_events(HashSet::from([part.clone()]), cursor_after_add, u32::MAX)
        .await??
        .remove(&part)
        .expect(ERROR_IMPOSSIBLE);

    assert!(
        page.events.iter().any(|evt| matches!(evt, PartEvent::Removed(rem) if rem.obj_id == obj && rem.cursor == cursor_after_remove)),
        "list_events must contain Removed event with advanced cursor {cursor_after_remove}: {:?}",
        page.events
    );
    Ok(())
}

pub async fn assert_list_events_page_verdict_contract<H>(harness: &H) -> Res<()>
where
    H: HostPartStoreContractHarness + Sync,
{
    let store = harness.store();
    let part = test_part(121);
    let obj1 = test_obj(122);
    let obj2 = test_obj(123);
    let obj3 = test_obj(124);

    store.ensure_part(part.clone()).await?;

    // Seed 2 objects
    store
        .set_obj_payload(obj1.clone(), payload("verdict", 1))
        .await?;
    store.add_obj_to_parts(obj1, vec![part.clone()]).await?;
    store
        .set_obj_payload(obj2.clone(), payload("verdict", 2))
        .await?;
    store.add_obj_to_parts(obj2, vec![part.clone()]).await?;

    // limit=2 matching exactly 2 events: the page fills its limit, so it is not
    // drained (a successor may still exist), and it resumes past its last event.
    let page_exact = store
        .list_events(HashSet::from([part.clone()]), 0, 2)
        .await??
        .remove(&part)
        .expect(ERROR_IMPOSSIBLE);

    assert_eq!(page_exact.events.len(), 2);
    assert!(
        !page_exact.drained,
        "a page that filled its limit may still have a successor"
    );
    let last_exact = match page_exact.events.last().expect(ERROR_IMPOSSIBLE) {
        PartEvent::Changed(changed) => changed.cursor,
        PartEvent::Removed(_) => panic!("a single-object part yields Changed"),
    };
    assert_eq!(
        page_exact.resume, last_exact,
        "a full page resumes past its last event"
    );

    // Asking again from that resume comes up short, and only there does the
    // verdict turn caught-up.
    let page_exact_tail = store
        .list_events(HashSet::from([part.clone()]), page_exact.resume, 2)
        .await??
        .remove(&part)
        .expect(ERROR_IMPOSSIBLE);
    assert!(
        page_exact_tail.events.is_empty(),
        "nothing waits beyond the exact page"
    );
    assert!(
        page_exact_tail.drained,
        "the short page is where caught-up is learned"
    );
    assert_eq!(page_exact_tail.resume, page_exact.resume);

    // Seed 3rd object
    store
        .set_obj_payload(obj3.clone(), payload("verdict", 3))
        .await?;
    store.add_obj_to_parts(obj3, vec![part.clone()]).await?;

    // limit=2 when 3 events exist: a full page that stops short of drained, and
    // resumes past the last event it carried.
    let page_more = store
        .list_events(HashSet::from([part.clone()]), 0, 2)
        .await??
        .remove(&part)
        .expect(ERROR_IMPOSSIBLE);

    assert_eq!(page_more.events.len(), 2);
    assert!(!page_more.drained, "more events wait beyond the page");

    // Fetching from `resume` gets the 3rd event and is short, so it is drained.
    let page_tail = store
        .list_events(HashSet::from([part.clone()]), page_more.resume, 2)
        .await??
        .remove(&part)
        .expect(ERROR_IMPOSSIBLE);

    assert_eq!(page_tail.events.len(), 1);
    assert!(page_tail.drained, "the tail page has nothing beyond it");
    // A zero limit is a legal request for "no events right now": it answers
    // nothing, and while anything is waiting it must not claim the log is caught
    // up — a caller reads `drained` as "nothing further is waiting" and would
    // strand the events it never received.
    let page_zero = store
        .list_events(HashSet::from([part.clone()]), 0, 0)
        .await??
        .remove(&part)
        .expect(ERROR_IMPOSSIBLE);

    assert!(
        page_zero.events.is_empty(),
        "a zero limit returns no events: {:?}",
        page_zero.events
    );
    assert!(
        !page_zero.drained,
        "a zero limit must not report caught-up while events are waiting"
    );
    assert_eq!(
        page_zero.resume, 0,
        "and it resumes from the caller's own cursor"
    );

    // The same request against a part with nothing waiting is caught up.
    let drained_part = test_part(125);
    store.ensure_part(drained_part.clone()).await?;
    let page_zero_empty = store
        .list_events(HashSet::from([drained_part.clone()]), 0, 0)
        .await??
        .remove(&drained_part)
        .expect(ERROR_IMPOSSIBLE);

    assert!(page_zero_empty.events.is_empty());
    assert!(
        page_zero_empty.drained,
        "a zero limit on a part with nothing waiting is caught up"
    );
    assert_eq!(page_zero_empty.resume, 0);
    Ok(())
}

/// A store whose log is empty and whose reader never answers again, so a page
/// can only end on its hold. A real reader's live half blocks until a commit
/// lands, which makes an expired hold untestable against a store with rows.
#[cfg(test)]
#[derive(Default)]
struct EmptyLogStore {}

/// The reader behind [`EmptyLogStore`]: its replay half is complete
/// the moment it opens, because the log holds no rows for the target — which
/// is the caught-up verdict a real store reports immediately for a target with
/// nothing to replay. Its live half then never answers, the way a real
/// reader's live half blocks until a commit lands.
#[cfg(test)]
struct SilentlyEmptyReader {
    replay_complete: bool,
}

#[cfg(test)]
#[async_trait]
impl LocalPartRevisionReader for SilentlyEmptyReader {
    async fn next(
        &mut self,
        _limits: RevisionReadLimits,
    ) -> Res<RevisionRead<FrontierRevision, PartEvent>> {
        if !self.replay_complete {
            self.replay_complete = true;
            return Ok(RevisionRead::ReplayComplete { through: 0 });
        }
        std::future::pending::<()>().await;
        unreachable!("the live half of an empty log never answers")
    }
}

#[cfg(test)]
#[async_trait]
impl HostPartStore for EmptyLogStore {
    async fn open_revision_reader(
        &self,
        _reqs: SubPartsRequest,
    ) -> Res<Result<Box<dyn LocalPartRevisionReader>, ListPartsError>> {
        Ok(Ok(Box::new(SilentlyEmptyReader {
            replay_complete: false,
        })))
    }

    async fn open_page_reader(
        &self,
        _reqs: SubPartsRequest,
    ) -> Res<Result<Box<dyn LocalPartRevisionReader>, ListPartsError>> {
        Ok(Ok(Box::new(SilentlyEmptyReader {
            replay_complete: false,
        })))
    }

    async fn summarize_parts(
        &self,
        _parts: HashSet<PartKey>,
    ) -> Res<Result<HashMap<PartKey, PartSummary>, ListPartsError>> {
        Ok(Ok(HashMap::new()))
    }

    async fn latest_revision(&self) -> Res<CursorIndex> {
        unreachable!("the page path does not read the latest revision")
    }
    async fn get_changed_buckets(
        &self,
        _req: GetChangedBucketsRequest,
        _subscriber: PeerKey,
    ) -> Res<Result<Vec<BucketSummary>, ListPartsError>> {
        unreachable!("the page path does not walk buckets")
    }
    async fn leaf_buckets(
        &self,
        _req: LeafBucketsRequest,
        _subscriber: PeerKey,
    ) -> Res<Result<LeafBucketResult, LeafBucketsError>> {
        unreachable!("the page path does not read leaf buckets")
    }
    async fn member_count(&self, _part_id: PartKey) -> Res<u64> {
        unreachable!("the page path does not count members")
    }
    async fn part_dirty_count(
        &self,
        _part_id: PartKey,
        _principal: Option<PeerKey>,
        _since: CursorIndex,
    ) -> Res<PartDirtyCount> {
        unreachable!("the page path does not count dirty rows")
    }
    async fn get_bucket_summary(&self, _part_id: PartKey, _id: BuckId) -> Res<BucketSummary> {
        unreachable!("the page path does not read bucket summaries")
    }
    async fn obj_parts(&self, _obj_id: ObjKey) -> Res<Vec<PartKey>> {
        unreachable!("the page path does not read object parts")
    }
    async fn obj_exists(&self, _obj_id: ObjKey) -> Res<bool> {
        unreachable!("the page path does not test object existence")
    }
    async fn set_obj_payload(&self, _obj_id: ObjKey, _payload: ObjPayload) -> Res<()> {
        unreachable!("the page path does not write payloads")
    }
    async fn obj_payload(&self, _obj_id: ObjKey) -> Res<Option<ObjPayload>> {
        unreachable!("the page path does not read payloads")
    }
    async fn add_obj_to_parts(&self, _obj_id: ObjKey, _parts: Vec<PartKey>) -> Res<()> {
        unreachable!("the page path does not write membership")
    }
    async fn remove_obj_from_part(&self, _obj_id: ObjKey, _part_id: PartKey) -> Res<()> {
        unreachable!("the page path does not write membership")
    }
    async fn set_peer_part_cursor(
        &self,
        _peer_id: PeerKey,
        _part_id: PartKey,
        _cursor: CursorIndex,
    ) -> Res<()> {
        unreachable!("the page path does not write peer cursors")
    }
    async fn get_peer_part_cursor(&self, _peer_id: PeerKey, _part_id: PartKey) -> Res<CursorIndex> {
        unreachable!("the page path does not read peer cursors")
    }
    async fn list_events(
        &self,
        _parts: HashSet<PartKey>,
        _cursor: CursorIndex,
        _limit: u32,
    ) -> Res<Result<HashMap<PartKey, PartPage>, ListPartsError>> {
        unreachable!("the page path does not list events")
    }
    async fn ensure_part(&self, _part_id: PartKey) -> Res<()> {
        unreachable!("the page path does not create parts")
    }
    async fn set_part_members(&self, _part: PartKey, _agents: HashMap<PeerKey, Access>) -> Res<()> {
        unreachable!("the page path does not set access")
    }
    async fn add_part_member(&self, _part: PartKey, _member: PeerKey, _access: Access) -> Res<()> {
        unreachable!("the page path does not set access")
    }
    async fn remove_part_member(&self, _part: PartKey, _member: PeerKey) -> Res<()> {
        unreachable!("the page path does not set access")
    }
    async fn remove_obj_payload(&self, _obj_id: ObjKey) -> Res<()> {
        unreachable!("the page path does not clear content")
    }
    async fn partless_objects(&self, _limit: u32, _after: Option<ObjKey>) -> Res<Vec<ObjKey>> {
        unreachable!("the page path does not enumerate GC candidates")
    }
    async fn part_store_stats(&self) -> Res<PartStoreStats> {
        unreachable!("the page path does not read store stats")
    }
}

/// A held page that delivers nothing is caught up, and the claim is the
/// reader's: under the pull reader a target with no rows reports its replay
/// complete the moment the reader opens, where the subscription parked
/// silently. The distinction that still carries weight is the other side of the
/// limit — a page that stops on its own limit reports `drained: false`, because
/// backlog remains and the caller asks again straight away (pinned in
/// `rpc::tests`, where a store with rows can exercise it).
#[tokio::test(flavor = "multi_thread")]
async fn a_held_page_that_delivers_nothing_is_caught_up_by_the_reader() -> Res<()> {
    let store = EmptyLogStore::default();
    let outcome = store
        .replay_page_for_target(
            SubscriptionTarget::Part {
                part_id: test_part(171),
                cursor: 7,
            },
            8,
            PeerKey::new([172; 32]),
            Duration::from_millis(20),
        )
        .await?;
    let page = events_page(&outcome);
    assert!(
        page.events.is_empty(),
        "the log has nothing for this target"
    );
    assert!(
        page.drained,
        "a target with no rows is caught up: the reader reported its replay complete"
    );
    assert_eq!(
        page.resume, 7,
        "nothing delivered means the position does not move"
    );
    Ok(())
}
