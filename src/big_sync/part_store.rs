use crate::interlude::*;

use big_sync_core::keyed_frontier::{FrontierRead, FrontierRevision, KeyedFrontierReader};
use big_sync_core::part_store::{CursorIndex, ObjPayload, PartDirtyCount};
use big_sync_core::revisioned_store::{RevisionRead, RevisionReadLimits};
use big_sync_core::rpc::{
    BucketSummary, GetChangedBucketsRequest, LeafBucketResult, LeafBucketsError,
    LeafBucketsRequest, ListPartsError, ObjChanged, ObjRemovedFromPart, PartEvent, PartPage,
    PartSummary, ReplayPage, ReplayPageRequest, SubPartsRequest, SubscriptionTarget, TargetVerdict,
};
use big_sync_core::{BuckId, ObjKey, PartKey, PeerKey};
// Only the test-support contract module uses this, so gate it the same way that
// module is gated; otherwise a plain lib build reports it as unused.
#[cfg(any(test, feature = "test-support"))]
use big_sync_core::ByteKey;

/// The logical object and part routes represented by the part-store frontier.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum PartFrontierKey {
    Object(ObjKey),
    Part { obj_id: ObjKey, part_id: PartKey },
}

pub(crate) use sqlite_frontier::SqlitePartFrontier;
pub(crate) use sqlite_read::SqlitePartSelector;

mod sqlite_frontier;
mod sqlite_read;
mod sqlite_write;

pub mod memory;
pub mod sqlite;
pub mod sqlite_core;

/// Local, already-authorized revision stream for part-store consumers.
#[async_trait]
pub trait LocalPartRevisionReader: Send {
    async fn next(
        &mut self,
        limits: RevisionReadLimits,
    ) -> Res<RevisionRead<FrontierRevision, PartEvent>>;
}

pub(crate) struct PartRevisionReader {
    inner: Box<dyn KeyedFrontierReader<PartFrontierKey, PartEvent>>,
    /// `true` selects every object and part (the `All` local scope).
    all: bool,
    objects: HashSet<ObjKey>,
    parts: HashSet<PartKey>,
    pending: std::collections::VecDeque<RevisionRead<FrontierRevision, PartEvent>>,
    pending_replay_complete: Option<FrontierRevision>,
    last_revision: FrontierRevision,
    replay_complete_seen: bool,
}

impl PartRevisionReader {
    pub(crate) fn new(
        inner: Box<dyn KeyedFrontierReader<PartFrontierKey, PartEvent>>,
        objects: HashSet<ObjKey>,
        parts: HashSet<PartKey>,
    ) -> Self {
        Self {
            inner,
            all: false,
            objects,
            parts,
            pending: std::collections::VecDeque::new(),
            pending_replay_complete: None,
            last_revision: 0,
            replay_complete_seen: false,
        }
    }

    /// An unfiltered reader: every object and part event in the scope is
    /// projected, including events for parts created after this reader was
    /// opened.
    pub(crate) fn new_all(inner: Box<dyn KeyedFrontierReader<PartFrontierKey, PartEvent>>) -> Self {
        Self {
            all: true,
            ..Self::new(inner, HashSet::new(), HashSet::new())
        }
    }

    fn project(
        &self,
        key: PartFrontierKey,
        value: Option<PartEvent>,
        revision: FrontierRevision,
    ) -> Option<PartEvent> {
        match (key, value) {
            (PartFrontierKey::Object(_), None) => None,
            (PartFrontierKey::Object(_), Some(PartEvent::Changed(mut event))) => {
                event.cursor = revision;
                Some(PartEvent::Changed(event))
            }
            (PartFrontierKey::Part { obj_id, part_id }, value)
                if self.selects_object(&obj_id) && !self.selects_part(&part_id) =>
            {
                // Content only: a touch carries the object's payload, and an object reader that
                // selected neither the part nor its own part lane has nothing else to book.
                let payload = match value {
                    Some(PartEvent::Changed(event)) => event.payload,
                    // A part-level deletion is a part-lane fact. Inventing a payload-less
                    // `Changed` here would mean *resolve the membership*, which the cursor
                    // machine books as a content delivery.
                    Some(PartEvent::Removed(_)) | None => return None,
                };
                Some(PartEvent::Changed(ObjChanged {
                    cursor: revision,
                    part_ids: Vec::new(),
                    obj_id,
                    payload,
                }))
            }
            (PartFrontierKey::Part { obj_id, part_id }, Some(PartEvent::Changed(mut event)))
                if self.selects_part(&part_id) =>
            {
                event.cursor = revision;
                event.obj_id = obj_id;
                event.part_ids = vec![part_id];
                Some(PartEvent::Changed(event))
            }
            (PartFrontierKey::Part { obj_id, part_id }, Some(PartEvent::Removed(_)) | None)
                if self.selects_part(&part_id) =>
            {
                Some(PartEvent::Removed(ObjRemovedFromPart {
                    cursor: revision,
                    part_id,
                    obj_id,
                }))
            }
            _ => None,
        }
    }

    fn selects_object(&self, obj_id: &ObjKey) -> bool {
        self.all || self.objects.contains(obj_id)
    }

    fn selects_part(&self, part_id: &PartKey) -> bool {
        self.all || self.parts.contains(part_id)
    }

    fn merge_changed(events: &mut Vec<PartEvent>, event: PartEvent) {
        merge_part_event(events, event);
    }
}

#[async_trait]
impl LocalPartRevisionReader for PartRevisionReader {
    async fn next(
        &mut self,
        limits: RevisionReadLimits,
    ) -> Res<RevisionRead<FrontierRevision, PartEvent>> {
        if let Some(read) = self.pending.pop_front() {
            return Ok(read);
        }
        if let Some(through) = self.pending_replay_complete.take() {
            return Ok(RevisionRead::ReplayComplete { through });
        }
        match self
            .inner
            .next(big_sync_core::keyed_frontier::FrontierReadLimits {
                max_entries: limits.max_entries,
            })
            .await
            .map_err(|error| ferr!("{error}"))?
        {
            FrontierRead::ReplayComplete { through } => {
                if self.replay_complete_seen {
                    return Err(ferr!("frontier emitted ReplayComplete twice"));
                }
                self.replay_complete_seen = true;
                if through > self.last_revision {
                    self.last_revision = through;
                    self.pending_replay_complete = Some(through);
                    return Ok(RevisionRead::Entries {
                        revision: through,
                        entries: Vec::new(),
                    });
                }
                Ok(RevisionRead::ReplayComplete { through })
            }
            FrontierRead::Entries { entries, through } => {
                self.last_revision = self.last_revision.max(through);
                let mut grouped = BTreeMap::<FrontierRevision, Vec<PartEvent>>::new();
                for entry in entries {
                    if let Some(event) = self.project(entry.key, entry.value, entry.revision) {
                        Self::merge_changed(grouped.entry(entry.revision).or_default(), event);
                    }
                }
                if grouped.is_empty() {
                    return Ok(RevisionRead::Entries {
                        revision: through,
                        entries: Vec::new(),
                    });
                }
                let last = *grouped.keys().next_back().expect(ERROR_IMPOSSIBLE);
                self.pending.extend(
                    grouped
                        .into_iter()
                        .map(|(revision, entries)| RevisionRead::Entries { revision, entries }),
                );
                if through > last {
                    self.pending.push_back(RevisionRead::Entries {
                        revision: through,
                        entries: Vec::new(),
                    });
                }
                Ok(self.pending.pop_front().expect(ERROR_IMPOSSIBLE))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct HostPartStoreConfig {
    /// Parts that remain physically present but are invisible to remote part access.
    pub hidden_parts: HashSet<PartKey>,
    pub debounce_quiet_window: std::time::Duration,
    pub debounce_max_latency: std::time::Duration,
}

/// Assemble a page's per-target verdicts in the order the request named its targets.
///
/// Every requested target is answered: a target this responder could not serve is answered
/// with why, so the caller never has to infer a denial from an empty list of events.
fn assemble_page(
    ordered: Vec<SubscriptionTarget>,
    verdicts: &mut HashMap<SubscriptionTarget, TargetVerdict>,
) -> Vec<(SubscriptionTarget, TargetVerdict)> {
    ordered
        .into_iter()
        .map(|target| {
            let verdict = verdicts
                .remove(&target)
                .expect("every requested target has a verdict");
            (target, verdict)
        })
        .collect()
}

/// Compute the encoded size of a candidate page before an atomic revision is committed.
fn replay_page_wire_size(
    events: Vec<PartEvent>,
    order: &[SubscriptionTarget],
    verdicts: &HashMap<SubscriptionTarget, TargetVerdict>,
) -> Res<usize> {
    let targets = order
        .iter()
        .map(|target| {
            let verdict = verdicts.get(target).cloned().unwrap_or_else(|| {
                let cursor = match target {
                    SubscriptionTarget::Part { cursor, .. }
                    | SubscriptionTarget::Object { cursor, .. } => *cursor,
                };
                TargetVerdict::Events {
                    resume: cursor,
                    drained: false,
                }
            });
            (target.clone(), verdict)
        })
        .collect();
    ReplayPage { events, targets }
        .encoded_size()
        .map_err(|error| eyre::eyre!(error))
}

/// Merge one projected event into a page, preserving the existing object projection rules.
///
/// Changed rows for one object and revision collapse into one event with the union of part ids;
/// identical removals are delivered only once. The boolean says whether a new page entry was
/// added, which lets a global page budget ignore overlap between targets.
fn merge_part_event(events: &mut Vec<PartEvent>, event: PartEvent) -> bool {
    if let PartEvent::Changed(changed) = &event
        && let Some(PartEvent::Changed(existing)) = events.iter_mut().find(|candidate| {
            matches!(candidate, PartEvent::Changed(candidate)
                if candidate.cursor == changed.cursor && candidate.obj_id == changed.obj_id)
        })
    {
        for part_id in &changed.part_ids {
            if !existing.part_ids.contains(part_id) {
                existing.part_ids.push(part_id.clone());
            }
        }
        existing.part_ids.sort_unstable();
        existing.payload = changed.payload.clone();
        false
    } else if events.iter().any(|candidate| candidate == &event) {
        false
    } else {
        events.push(event);
        true
    }
}

/// Validate the peer's target set before touching storage or authorization state.
///
/// Cursors are positions on a logical route, not part of its identity: naming one part or object
/// twice with different cursors would otherwise create two competing verdicts for one route.
pub(crate) fn validate_replay_targets(targets: &[SubscriptionTarget]) -> Res<()> {
    let mut seen_parts = HashSet::new();
    let mut seen_objects = HashSet::new();
    for target in targets {
        let duplicate = match target {
            SubscriptionTarget::Part { part_id, .. } => !seen_parts.insert(part_id),
            SubscriptionTarget::Object { obj_id, .. } => !seen_objects.insert(obj_id),
        };
        if duplicate {
            eyre::bail!("a replay page request cannot name a logical route more than once");
        }
    }
    Ok(())
}

impl Default for HostPartStoreConfig {
    fn default() -> Self {
        Self {
            hidden_parts: HashSet::new(),
            debounce_quiet_window: std::time::Duration::from_millis(50),
            debounce_max_latency: std::time::Duration::from_millis(500),
        }
    }
}

// pub type ObjStoreLease = u64;

// #[derive(Debug, Clone, Copy, PartialEq, Eq)]
// pub enum StoreMutationOutcome {
//     Applied,
//     Stale,
// }

/// Bytes-and-counts summary used by an embedder's janitorial loop: how much payload the scope is
/// holding, how many objects are candidates for collection, and how much of the membership state
/// is tombstones.
pub struct PartStoreStats {
    /// Objects holding a payload.
    pub payload_objects: u64,
    /// Bytes of stored payload, counted over the stored encoding.
    pub payload_bytes: u64,
    /// Objects holding a payload and in no part: the GC candidates.
    pub partless_objects: u64,
    /// Membership rows naming a part whose member is present.
    pub live_rows: u64,
    /// Membership rows naming a part whose member is removed (tombstones).
    pub dead_rows: u64,
}

#[async_trait]
pub trait HostPartStore: Send + Sync {
    async fn latest_revision(&self) -> Res<CursorIndex>;
    async fn summarize_parts(
        &self,
        parts: HashSet<PartKey>,
    ) -> Res<Result<HashMap<PartKey, PartSummary>, ListPartsError>>;
    /// One page of a part's changed bucket summaries, for `subscriber`.
    ///
    /// A part `subscriber` may not read answers as [`ListPartsError::UnkownParts`],
    /// exactly as a part this scope does not have: a refusal has to be
    /// indistinguishable from an unknown part, because an empty or partial page
    /// still confirms that the part exists.
    async fn get_changed_buckets(
        &self,
        req: GetChangedBucketsRequest,
        subscriber: PeerKey,
    ) -> Res<Result<Vec<BucketSummary>, ListPartsError>>;
    /// One page of the entries of each requested bucket, for `subscriber`.
    ///
    /// A part `subscriber` may not read answers as [`LeafBucketsError::UnkownPart`],
    /// exactly as a part this scope does not have does.
    async fn leaf_buckets(
        &self,
        req: LeafBucketsRequest,
        subscriber: PeerKey,
    ) -> Res<Result<LeafBucketResult, LeafBucketsError>>;
    async fn member_count(&self, part_id: PartKey) -> Res<u64>;
    /// The relevance `principal` is behind on `part_id` at `since`.
    ///
    /// `None` is the local principal, which access rows do not gate.
    async fn part_dirty_count(
        &self,
        part_id: PartKey,
        principal: Option<PeerKey>,
        since: CursorIndex,
    ) -> Res<PartDirtyCount>;
    async fn get_bucket_summary(&self, part_id: PartKey, id: BuckId) -> Res<BucketSummary>;

    async fn obj_parts(&self, obj_id: ObjKey) -> Res<Vec<PartKey>>;
    async fn obj_exists(&self, obj_id: ObjKey) -> Res<bool>;

    // NOTE: upsert_obj doesn't take/invalidate leases since
    // it doesn't affect part membership
    async fn set_obj_payload(&self, obj_id: ObjKey, payload: ObjPayload) -> Res<()>;

    async fn obj_payload(&self, obj_id: ObjKey) -> Res<Option<ObjPayload>>;

    // async fn get_obj_lease(&self, obj_id: ObjKey) -> Res<ObjStoreLease>;

    async fn add_obj_to_parts(&self, obj_id: ObjKey, parts: Vec<PartKey>) -> Res<()>;

    async fn remove_obj_from_part(&self, obj_id: ObjKey, part_id: PartKey) -> Res<()>;

    async fn set_peer_part_cursor(
        &self,
        peer_id: PeerKey,
        part_id: PartKey,
        cursor: CursorIndex,
    ) -> Res<()>;

    async fn get_peer_part_cursor(&self, peer_id: PeerKey, part_id: PartKey) -> Res<CursorIndex>;

    async fn list_events(
        &self,
        parts: HashSet<PartKey>,
        cursor: CursorIndex,
        limit: u32,
    ) -> Res<Result<HashMap<PartKey, PartPage>, ListPartsError>>;
    async fn list_events_with_policy(
        &self,
        parts: HashSet<PartKey>,
        cursor: CursorIndex,
        limit: u32,
        enforce_policy: bool,
    ) -> Res<Result<HashMap<PartKey, PartPage>, ListPartsError>> {
        if enforce_policy {
            return Err(ferr!("policy enforcement not supported on this store"));
        }
        self.list_events(parts, cursor, limit).await
    }

    /// Whether a peer-facing read of `target` must be refused to `subscriber`.
    ///
    /// The one authorization answer for the whole peer-facing read surface: a page,
    /// a bucket walk and a part summary all ask this, and all of them answer a
    /// refusal as if the part were unknown, because a refusal that looks different
    /// from one confirms that the part exists.
    ///
    /// `permitted_parts` decides it per part, so a store that filters its
    /// subscriptions per recipient is refused here too, and a store that hands every
    /// subscriber the same stream says so there rather than inheriting an answer
    /// here.
    async fn read_denied(&self, target: ReadTarget, subscriber: PeerKey) -> Res<bool> {
        let (scope, obj_id) = target.into_scope();
        Ok(self
            .permitted_parts(scope, obj_id, Some(subscriber))
            .await?
            .is_some_and(|readable| readable.is_empty()))
    }

    /// Whether a replayed event's parts are readable by `subscriber`: the
    /// remote filter for the page read, decided per event rather than inferred
    /// from an empty page. The scope is the event's own: content-only events
    /// (no part ids) ask the object route, a part event asks its part, and a
    /// collapsed touch asks every part it names. A store with no authorization
    /// model answers `true` through [`Self::permitted_parts`].
    async fn page_event_is_readable(&self, event: &PartEvent, subscriber: PeerKey) -> Res<bool> {
        let (scope, obj_id) = match event {
            PartEvent::Changed(inner) => (
                match inner.part_ids.as_slice() {
                    [] => PartScope::FromObject,
                    [part] => PartScope::Part(part.clone()),
                    parts => PartScope::AnyOf(parts.to_vec()),
                },
                inner.obj_id.clone(),
            ),
            PartEvent::Removed(inner) => {
                (PartScope::Part(inner.part_id.clone()), inner.obj_id.clone())
            }
        };
        Ok(!self
            .permitted_parts(scope, obj_id, Some(subscriber))
            .await?
            .is_some_and(|readable| readable.is_empty()))
    }

    /// One bounded, filtered page over a set of targets, held while there is nothing to send.
    ///
    /// This is the responder half of client-driven delivery, and it reads durable state
    /// directly: no subscription, no channel, no registration, so a cancelled request leaves
    /// nothing behind and a re-issue re-derives everything from the caller's own per-target
    /// bounds. Paging is the flow control, so the caller's processing rate is what decides
    /// how fast events arrive.
    ///
    /// Three properties are load-bearing:
    ///
    /// * **Verdicts are per target and never suppress each other.** An unknown or
    ///   unauthorized target is answered as such alongside the targets that are served: the
    ///   event filter drops unreadable parts silently, which would otherwise collapse
    ///   "nothing to send" and "you may not read this" into one answer for the whole page.
    /// * **The page's limit is sliced across the targets.** One read ordered by revision lets
    ///   a target with a large backlog starve a target with a small one — the small target's
    ///   newest row sits behind the whole backlog and is never reached — so each target is
    ///   read from its own bound with its own share of the page. A complete atomic revision
    ///   may exceed its share: the reader cannot split one revision without losing rows. The
    ///   live/bulk lane split is what makes that affordable: the request carrying a live doc's few targets is small,
    ///   while the request carrying a large set is latency-insensitive catch-up.
    /// * **Cancellation is cooperative and row-aware.** The read stays outside the select, so
    ///   a cancel never interrupts a page in flight, and a page that already holds rows ships
    ///   them anyway. A cancel therefore only ever ends a wait, and it is checked at exactly
    ///   two points: the top of the page loop before any query, and inside the wait.
    async fn replay_page_round_with_update(
        &self,
        req: ReplayPageRequest,
        subscriber: PeerKey,
        hold: Duration,
        cancel: CancellationToken,
        update: Option<Arc<tokio::sync::Notify>>,
    ) -> Res<ReplayPage> {
        let ReplayPageRequest {
            session_id: _,
            request_id,
            supersede: _,
            targets: requested,
            limit,
            hold_ms: _,
        } = req;
        validate_replay_targets(&requested)?;
        // Existence and authorization first, per target: a target that cannot be served is
        // answered as such and the rest of the page is still served.
        let order = requested.clone();
        let mut verdicts: HashMap<SubscriptionTarget, TargetVerdict> = HashMap::new();
        let mut live: Vec<(SubscriptionTarget, CursorIndex)> = Vec::new();
        for target in requested {
            // A target is either denied (with why) or live (with the position to read from):
            // the match borrows the target, so the decision is carried out of it rather than
            // made by moving the target inside an arm.
            let mut denial: Option<TargetVerdict> = None;
            let mut bound: Option<CursorIndex> = None;
            match &target {
                SubscriptionTarget::Part { part_id, cursor } => {
                    if let Err(ListPartsError::UnkownParts { .. }) = self
                        .summarize_parts(HashSet::from([part_id.clone()]))
                        .await?
                    {
                        denial = Some(TargetVerdict::UnknownPart);
                    } else if self
                        .read_denied(ReadTarget::from(&target), subscriber.clone())
                        .await?
                    {
                        denial = Some(TargetVerdict::Unauthorized);
                    } else {
                        bound = Some(*cursor);
                    }
                }
                // An object target carries no part cursor of its own: the client sends the
                // position its replay has reached, and the store materializes the object's
                // derived part while reading. Replaying from the start on every page would
                // re-read the object's first page forever.
                SubscriptionTarget::Object { cursor, .. } => {
                    if self
                        .read_denied(ReadTarget::from(&target), subscriber.clone())
                        .await?
                    {
                        denial = Some(TargetVerdict::Unauthorized);
                    } else {
                        bound = Some(*cursor);
                    }
                }
            }
            match (denial, bound) {
                (Some(verdict), _) => {
                    verdicts.insert(target, verdict);
                }
                (None, Some(cursor)) => live.push((target, cursor)),
                (None, None) => unreachable!("a requested target is either denied or live"),
            }
        }
        // `limit` bounds the events the whole page may carry, so a zero limit carries none.
        // Answering before anything is read is what makes the bound deterministic, and every
        // position stays the caller's own because nothing was read.
        if limit == 0 {
            for (target, cursor) in live {
                verdicts.insert(
                    target,
                    TargetVerdict::Events {
                        resume: cursor,
                        drained: false,
                    },
                );
            }
            return Ok(ReplayPage {
                events: Vec::new(),
                targets: assemble_page(order, &mut verdicts),
            });
        }
        let limit = usize::try_from(limit).expect(ERROR_IMPOSSIBLE);
        // The hold is pacing, not a deadline: when it expires the caller gets a normal empty
        // answer and re-issues immediately, so there is nothing for a timeout multiplier to
        // buy here. Scaling it multiplies live-delivery latency for every round
        // (`UTILS_RS_TIMEOUT_MULTIPLIER=3` in CI made each round cost 45s while the callers'
        // own budgets and nextest's process timeouts stayed unscaled).
        let hold_is_zero = hold.is_zero();
        let hold_duration_ms = hold.as_millis();
        let hold = tokio::time::sleep(hold);
        tokio::pin!(hold);
        let mut events: Vec<PartEvent> = Vec::new();
        loop {
            // Cancellation, first of the two places it is checked: the top of the page loop,
            // before any query. A page that already holds rows is shipped either way.
            if cancel.is_cancelled() {
                for (target, cursor) in &live {
                    verdicts.insert(
                        target.clone(),
                        TargetVerdict::Events {
                            resume: *cursor,
                            drained: false,
                        },
                    );
                }
                break;
            }
            // Fair drain: one read per target from that target's own bound, each bounded by
            // its fair share of the *remaining global* page budget.
            let mut remaining_page = limit;
            let mut all_drained = true;
            let mut positions = Vec::with_capacity(live.len());
            for (index, (target, bound)) in live.iter().enumerate() {
                let requested_cursor = *bound;
                let targets_left = live.len() - index;
                let quota = if remaining_page == 0 {
                    0
                } else {
                    remaining_page.div_ceil(targets_left)
                };
                let mut remaining = quota;
                let mut resume = requested_cursor;
                let mut delivered = requested_cursor;
                let mut drained = false;
                let mut filled = false;
                if quota == 0 {
                    all_drained = false;
                    positions.push((target.clone(), requested_cursor));
                    verdicts.insert(
                        target.clone(),
                        TargetVerdict::Events {
                            resume: requested_cursor,
                            drained: false,
                        },
                    );
                    continue;
                }
                let mut reader = match self
                    .open_page_reader(SubPartsRequest {
                        lower_bound: requested_cursor,
                        targets: HashSet::from([target.clone()]),
                    })
                    .await?
                {
                    Ok(reader) => reader,
                    Err(ListPartsError::UnkownParts { .. }) => {
                        verdicts.insert(target.clone(), TargetVerdict::UnknownPart);
                        all_drained = false;
                        positions.push((target.clone(), requested_cursor));
                        continue;
                    }
                };
                loop {
                    let read = reader
                        .next(RevisionReadLimits {
                            max_entries: std::num::NonZeroUsize::new(remaining)
                                .expect("a per-target page share is at least one event"),
                        })
                        .await?;
                    match read {
                        // The reader's own replay boundary: this target is caught up, and
                        // because its range was covered in full the position may move onto
                        // the boundary. That is what keeps the claim falsifiable by the
                        // caller and stops the next request re-scanning the range it already
                        // covered. The invariant this rests on: a page that dropped rows it
                        // should have delivered — its share filled — must never take this
                        // arm. Such a page is not drained.
                        RevisionRead::ReplayComplete { through } => {
                            drained = true;
                            resume = resume.max(through);
                            break;
                        }
                        RevisionRead::Entries { revision, entries } => {
                            let mut revision_events = Vec::new();
                            for event in entries {
                                // The tombstone rule (ADR 012 decision 9) is the read's: a
                                // reader is never handed a `Removed` whose `added_at` is after
                                // its own requested cursor, so there is nothing to look up or
                                // compare for this event here.
                                if !self
                                    .page_event_is_readable(&event, subscriber.clone())
                                    .await?
                                {
                                    continue;
                                }
                                revision_events.push(event);
                            }

                            if revision_events.is_empty() {
                                // A batch whose rows were all dropped still advances the
                                // durable read position, but emits no page work.
                                resume = resume.max(revision);
                                continue;
                            }

                            // Build and size the complete revision transactionally. If it does
                            // not fit, leave the durable cursor before this revision so the next
                            // page can retry it; an oversized first revision is admitted alone.
                            let mut candidate = events.clone();
                            let mut added_events = 0usize;
                            for event in revision_events {
                                if merge_part_event(&mut candidate, event) {
                                    added_events += 1;
                                }
                            }
                            let oversized =
                                replay_page_wire_size(candidate.clone(), &order, &verdicts)?
                                    > ReplayPage::BYTE_BUDGET;
                            if oversized && !events.is_empty() {
                                filled = true;
                                break;
                            }
                            events = candidate;
                            remaining_page = remaining_page.saturating_sub(added_events);
                            remaining = remaining.saturating_sub(added_events);
                            delivered = revision;
                            resume = resume.max(revision);

                            // `RevisionRead::Entries` is one complete atomic revision. Do not
                            // stop halfway through it: both event and byte limits are soft at
                            // this boundary. An oversized revision is sent alone.
                            if remaining == 0 || oversized {
                                filled = true;
                                break;
                            }
                        }
                    }
                }
                // A target whose share filled has rows still waiting: it is not drained, and
                // its position stays at the last row it delivered, so the next request
                // re-reads what was left rather than stepping over it.
                if filled {
                    all_drained = false;
                    drained = false;
                    resume = delivered;
                }
                positions.push((target.clone(), resume));
                verdicts.insert(target.clone(), TargetVerdict::Events { resume, drained });
            }
            live = positions;
            // The page answers as soon as it carries anything, or as soon as a target still
            // has rows waiting, or when the caller asked not to wait at all.
            if !events.is_empty() || !all_drained || hold_is_zero || live.is_empty() {
                break;
            }
            // Every live target is caught up and the page carried nothing: this is the second
            // place cancellation is checked, and the only place a request waits. One reader
            // over the whole live set is used as the wake — any target's row wakes it — and
            // the hold is the pacing bound, not a deadline.
            tracing::debug!(
                ?request_id,
                subscriber = %subscriber,
                target_count = live.len(),
                hold_ms = hold_duration_ms,
                "replay page entering long poll",
            );
            let mut wake = match self
                .open_page_reader(SubPartsRequest {
                    lower_bound: live
                        .iter()
                        .map(|(_, cursor)| *cursor)
                        .min()
                        .unwrap_or_default(),
                    targets: live.iter().map(|(target, _)| target.clone()).collect(),
                })
                .await?
            {
                Ok(reader) => reader,
                Err(ListPartsError::UnkownParts { .. }) => break,
            };
            let mut probed = false;
            let woke = loop {
                let read = tokio::select! {
                    biased;
                    read = wake.next(RevisionReadLimits {
                        max_entries: std::num::NonZeroUsize::new(1)
                            .expect("a wake read asks for one event"),
                    }) => read?,
                    () = &mut hold => {
                        tracing::debug!(?request_id, "replay page long poll hold expired");
                        break false;
                    }
                    () = cancel.cancelled() => {
                        tracing::debug!(?request_id, "replay page long poll cancelled");
                        break false;
                    }
                    () = async {
                        if let Some(update) = &update {
                            update.notified().await;
                        } else {
                            std::future::pending::<()>().await;
                        }
                    } => {
                        tracing::debug!(?request_id, "replay page long poll subscription updated");
                        break false;
                    },
                };
                match read {
                    // Rows exist again: go back to the fair drain so every target keeps its
                    // share rather than the woken one taking the page.
                    RevisionRead::Entries { entries, .. } if !entries.is_empty() => {
                        tracing::debug!(?request_id, "replay page long poll woke for new event");
                        break true;
                    }
                    // No rows (an empty page) or the reader's boundary: neither is a wake. A
                    // reader that answers with no rows every time must not turn this wait into a
                    // spin, so after one probe the pacing is the hold, which the caller re-issues
                    // from. This is also the arm that parks a blocking reader's live phase.
                    _ => {
                        if probed {
                            tokio::select! {
                                () = &mut hold => {
                                    tracing::debug!(?request_id, "replay page long poll hold expired after probe");
                                    break false;
                                },
                                () = cancel.cancelled() => {
                                    tracing::debug!(?request_id, "replay page long poll cancelled after probe");
                                    break false;
                                },
                                () = async {
                                    if let Some(update) = &update {
                                        update.notified().await;
                                    } else {
                                        std::future::pending::<()>().await;
                                    }
                                } => {
                                    tracing::debug!(?request_id, "replay page long poll subscription updated after probe");
                                    break false;
                                },
                            }
                        }
                        probed = true;
                    }
                }
            };
            if !woke {
                break;
            }
        }
        let drained_count = verdicts
            .values()
            .filter(|verdict| matches!(verdict, TargetVerdict::Events { drained: true, .. }))
            .count();
        tracing::debug!(
            ?request_id,
            subscriber = %subscriber,
            event_count = events.len(),
            target_count = verdicts.len(),
            drained_count,
            "replay page ready",
        );
        events.sort_by_key(PartEvent::cursor);
        Ok(ReplayPage {
            events,
            targets: assemble_page(order, &mut verdicts),
        })
    }

    async fn replay_page_round(
        &self,
        req: ReplayPageRequest,
        subscriber: PeerKey,
        hold: Duration,
        cancel: CancellationToken,
    ) -> Res<ReplayPage> {
        self.replay_page_round_with_update(req, subscriber, hold, cancel, None)
            .await
    }
    /// Open a durable revision reader over a set of targets, each carrying its
    /// own bound. This is the read seam for the whole part store: the responder
    /// drives it to answer one page (bounded by the caller's limit, held while
    /// there is nothing to send) and local pull consumers drive it directly, so
    /// replay and live delivery are the same read. It is a reader — not a
    /// channel — so the caller owns pacing and a dropped reader leaves nothing
    /// behind. Stores without a mirror over their own storage answer an error.
    /// This boundary intentionally has no remote authorization or hidden-part
    /// filtering: a store that serves peers filters the page at the responder.
    async fn open_revision_reader(
        &self,
        _reqs: SubPartsRequest,
    ) -> Res<Result<Box<dyn LocalPartRevisionReader>, ListPartsError>> {
        Err(ferr!("revision reader is not available"))
    }

    /// Open the read a page is drawn from: the same durable revision reader, with ADR 012
    /// decision 9's tombstone predicate applied to the requested cursors in `reqs`.
    ///
    /// A page's cursor is a claim about what the requester already has, so a removal whose add
    /// is newer than that cursor is not the requester's business and is not fetched at all.
    /// A pull consumer's bound is where it starts streaming and it then advances, so
    /// [`Self::open_revision_reader`] hands that reader the log whole — tombstones for adds it
    /// never saw included, which its own replica discards as no-ops.
    async fn open_page_reader(
        &self,
        _reqs: SubPartsRequest,
    ) -> Res<Result<Box<dyn LocalPartRevisionReader>, ListPartsError>> {
        Err(ferr!("page reader is not available"))
    }

    /// Open a durable revision reader over every part and object in the scope,
    /// including parts created after this call. This is the `All` worker scope:
    /// the part set is resolved by the store at read time, so no enumeration is
    /// frozen into the reader. `after` is the replay lower bound (a part-store
    /// frontier revision).
    /// This boundary intentionally has no remote authorization or
    /// hidden-part filtering.
    async fn open_revision_reader_all(
        &self,
        _after: CursorIndex,
    ) -> Res<Result<Box<dyn LocalPartRevisionReader>, ListPartsError>> {
        Err(ferr!("local revision reader is not available"))
    }
    async fn ensure_part(&self, part_id: PartKey) -> Res<()>;

    /// Replace the agents who have access to `part` and their [`Access`] level.
    /// The store's [`ObjAccessPolicy`] uses this to determine fetchability.
    async fn set_part_members(
        &self,
        part: PartKey,
        agents: HashMap<PeerKey, keyhive_core::access::Access>,
    ) -> Res<()>;

    /// Add a single member to `part` with the given [`Access`] level.
    async fn add_part_member(
        &self,
        part: PartKey,
        member: PeerKey,
        access: keyhive_core::access::Access,
    ) -> Res<()>;

    /// Remove a single member from `part`.
    async fn remove_part_member(&self, part: PartKey, member: PeerKey) -> Res<()>;

    /// Filter an outbound event down to the parts `principal` may read.
    ///
    /// `scope` is the event's *candidate* part set: the parts the event names, or
    /// [`PartScope::FromObject`] when nothing usable is named and the object's
    /// containing parts must be resolved from membership. Access is granted per
    /// part, so authorization and non-exposure are the same operation: a part id
    /// is disclosed only to a principal that may read it.
    ///
    /// - `principal == None` (trusted local subscriber): `Ok(None)`, unfiltered.
    ///   Delivery proceeds with the event's own part list untouched.
    /// - Otherwise `Ok(Some(readable))` with `readable ⊆ candidates`. Empty means
    ///   nothing about this event is the principal's business: drop it.
    async fn permitted_parts(
        &self,
        _scope: PartScope,
        _obj_id: ObjKey,
        _principal: Option<PeerKey>,
    ) -> Res<Option<Vec<PartKey>>> {
        Ok(None)
    }

    /// Drop an object's payload, leaving no membership behind: remove it from every part it is in
    /// (emitting the same events and frontier updates as `remove_obj_from_part` per part, in one
    /// transaction), then drop the payload. Idempotent: an unknown object or an object with no payload
    /// is a no-op. This is the only path that clears content.
    async fn remove_obj_payload(&self, obj_id: ObjKey) -> Res<()>;

    /// Objects with a payload and no live membership row, ordered by `obj_id`, keyset-paginated:
    /// pass the last returned key as `after` for the next page. GC candidates.
    async fn partless_objects(&self, limit: u32, after: Option<ObjKey>) -> Res<Vec<ObjKey>>;

    /// Scope-wide counters for the janitorial loop and for seeing a part's tombstone pressure.
    async fn part_store_stats(&self) -> Res<PartStoreStats>;
}

/// The candidate part set an outbound event is about, to be filtered down to the
/// parts the recipient may read.
///
/// `Part`/`AnyOf` carry the parts the event itself names; `FromObject` means the
/// event named no usable part (an object-scoped change, or an unreliable empty
/// list) and the object's live containing parts must be resolved from membership
/// instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartScope {
    /// The event names exactly one part.
    Part(PartKey),
    /// The event names several parts.
    AnyOf(Vec<PartKey>),
    /// Nothing usable is named: resolve the object's containing parts.
    FromObject,
}

/// What a peer-facing read is about.
///
/// A page names a part or an object; a bucket walk and a part summary name a part.
/// All of them reduce to the same question — may `subscriber` read it — which
/// [`HostPartStore::read_denied`] answers through `permitted_parts`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadTarget {
    Part(PartKey),
    Object(ObjKey),
}

impl ReadTarget {
    /// The `(scope, obj_id)` pair `permitted_parts` resolves this target through.
    fn into_scope(self) -> (PartScope, ObjKey) {
        match self {
            // `permitted_parts` reads the object only for a `FromObject` scope, so a
            // part is asked about directly.
            Self::Part(part_id) => (PartScope::Part(part_id), ObjKey::new([0u8; 32])),
            Self::Object(obj_id) => (PartScope::FromObject, obj_id),
        }
    }
}

impl From<&SubscriptionTarget> for ReadTarget {
    fn from(target: &SubscriptionTarget) -> Self {
        match target {
            SubscriptionTarget::Part { part_id, .. } => Self::Part(part_id.clone()),
            SubscriptionTarget::Object { obj_id, .. } => Self::Object(obj_id.clone()),
        }
    }
}

/// The range of stored bucket indices belonging to `bucket_id`.
///
/// ADR 012 decision 1: the index is a hash of the object key, so a bucket's members are a
/// contiguous range of that index and not of the key. A level-`L` bucket covers the deepest
/// indices whose top `L` nibbles equal its own, which is the level/truncation hierarchy
/// holding under a hash. `None` as the upper bound means "to the end of the index space",
/// i.e. a terminal bucket at its level, so a range never wraps.
#[must_use]
pub fn bucket_index_bounds(bucket_id: BuckId) -> (u16, Option<u16>) {
    debug_assert!(bucket_id.level() <= BuckId::MAX_LEVEL);
    let shift = u16::BITS - u32::from(bucket_id.level()) * u32::from(BuckId::BITS_PER_LEVEL);
    // Widened so the last index of a level still has a representable exclusive upper bound.
    let lower = u32::from(bucket_id.index()) << shift;
    debug_assert!(lower <= u32::from(u16::MAX));
    let upper = lower + (1u32 << shift);
    (
        lower as u16,
        (upper <= u32::from(u16::MAX)).then_some(upper as u16),
    )
}

/// The bytes one leaf-page entry adds to its bucket's page on the wire.
///
/// An entry is the key, the `dead` flag, and the keyed fingerprint's `u64`. The key is the
/// only variable-width part — ADR 012 decision 1 makes identity *is* the byte string, so any
/// length is a valid key — and IRPC encodes messages as postcard, which frames a byte string
/// as a varint length followed by the bytes. This is that arithmetic and nothing else: a
/// responder decides what a page costs from the key alone, because the fingerprint hashes the
/// payload without ever carrying it.
#[must_use]
pub fn leaf_entry_wire_bytes(key_len: usize) -> usize {
    varint_wire_bytes(key_len) + key_len + 1 + 8
}

/// The bytes postcard spends on a byte string's length prefix.
fn varint_wire_bytes(value: usize) -> usize {
    // Base-128 groups, and never fewer than one byte for the zero case.
    (usize::BITS - value.leading_zeros()).div_ceil(7).max(1) as usize
}

/// The encoded size one bucket's leaf page is bounded by, whatever `limit_hint` asks for.
///
/// `LeafBucketsRequest::limit_hint` bounds entries, and an entry is its key's width, so the rpc
/// layer's `MAX_BUCKET_LIMIT` (1024) entries of 32-byte keys is ~42 KiB and a page of longer
/// keys is more: trusting the hint alone leaves the per-page cost unbounded. At 64 KiB this
/// budget holds ~1560 of 32-byte keys, so the entry hint still binds first for ordinary keys
/// and this budget binds once 1024 entries average wider than ~64 bytes.
///
/// A page is never empty: one entry is sent even when it alone exceeds the budget, because a
/// page with no entries reads as `done` while entries remain, which would strand the tail.
pub const LEAF_PAGE_BYTE_BUDGET: usize = 64 * 1024;

#[cfg(any(test, feature = "test-support"))]
#[cfg_attr(not(test), allow(dead_code))]
pub mod contract;

#[cfg(any(test, feature = "test-support"))]
pub mod host_contract;
