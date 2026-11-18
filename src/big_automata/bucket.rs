/// FIXME: this machine is per part which is not great if we have
/// large overlapping parts from a peer? They'll share buckets anyways.
/// It should be per peer like CursorSyncMachine
/// - Use batched Leafing to minimize RPC
use crate::interlude::*;

use crate::fingerprint::Fingerprint;
use crate::fingerprint::FingerprintSeed;
use crate::part_store::PartStoreRead;
use crate::part_store::PayloadState;
use crate::rpc::bucket::LeafBucketPage as RawLeafBucketPage;
use crate::rpc::bucket::LeafBucketRequest;
use crate::store::CursorIndex;
use crate::store::ObjPayload;
use crate::store::bucket::BucketSummary;

structstruck::strike! {
    #[derive(Debug)]
    pub enum BucketMachineCommand {
        SyncObj {
            obj_key: ObjKey,
            remote_payload: Option<ObjPayload>,
            part_key: PartKey,
        },
        RemoveObjFromParts {
            obj_key: ObjKey,
            part_key: PartKey,
        },
        ListBuckets {
            part_key: PartKey,
            offset: BuckId,
            since: CursorIndex,
            working_level: BuckLevel,
        }
        LeafBuckets {
            part_key: PartKey,
            since: CursorIndex,
            buckets: Vec<LeafBucketRequest>,
        }
        UpgradeToCursor {
            part_key: PartKey,
        }
    }
}

structstruck::strike! {
    pub struct BucketMachine {
        part_key: PartKey,
        //remote_depth: BuckLevel,

        working_level: BuckLevel,
        next_page_offset: BuckId,

        done_listing: bool,
        list_dispatched: bool,

        pending_buckets: BTreeMap<BuckId, BucketSummary>,
        working_buckets: Map<BuckId, struct WorkingBucketState {
            #![derive(Debug, Clone)]
            summary: BucketSummary,
            leaf_after: Option<ObjKey>,
            leaf_seen: u64,
            leaf_inflight: bool,
            leaf_exhausted: bool,
            pending_objs: Vec<BucketObjEntry>,
            active_objs: Map<ObjKey, BucketObjEntry>,
        }>,

        // Bound the initial number of buckets we admit to the active
        // leafing set before low-watermark refill takes over.
        initial_working_set: u64,
        leaf_watermark: u64,

        active_obj_jobs: Map<ObjKey, BuckId>,

        last_cursor: u64,
    }
}

impl BucketMachine {
    const ACTIVE_SYNC_JOB_TARGET: u32 = 1024;
    /// The relevance a peer may be behind on before the bucket walk is worth
    /// entering.
    ///
    /// Still an UNMEASURED placeholder. `dirty.total()` is logged at the decision site
    /// next to the raw counter gap it replaced, so the number can be chosen from data
    /// rather than guessed. Two regimes have been measured end to end (the `band work
    /// measured` lines in `big_sync::test`) and both sit *above* this value:
    ///
    /// - dense dirt, 1k objects, dirty 1000: the bands move identical object work (1000
    ///   syncs each) and land within 20% on the clock, because a cold peer has nothing
    ///   to prune — this is the case decision 7 calls enumeration;
    /// - sparse dirt, 20k objects, 3 objects changed: the walk moves 3 object syncs
    ///   against the cursor's 20000, carrying 276 bucket summaries and 230 leaf entries
    ///   instead of the whole event gap.
    ///
    /// Neither pins a crossing point below the placeholder, where the walk pays a fixed
    /// summary round trip to discover very little. Choosing the value needs a dense-dirt
    /// sweep over roughly 8..=512 at part sizes either side of `ACTIVE_SYNC_JOB_TARGET *
    /// ARITY` (below which the whole part is one root bucket and there is nothing to
    /// prune). That sweep needs a way to force the walk below this cutoff: the per-part
    /// hint cannot, because this check overrides it.
    pub const BUCKET_DIRTY_THRESHOLD: u64 = 256;
    pub const GET_BUCKET_LIMIT_HINT: u32 = 8 * BuckId::ARITY as u32;
    pub const LEAF_BUCKET_LIMIT_HINT: u32 = Self::ACTIVE_SYNC_JOB_TARGET;

    pub fn new(
        part_key: PartKey,
        remote_depth: BuckLevel,
        remote_size: u64,
        last_cursor: CursorIndex,
    ) -> Self {
        let working_level = calc_working_level(remote_size, remote_depth);
        let bucket_width = remote_size / (BuckId::ARITY.pow(working_level as _) as u64);
        let initial_working_set = (Self::ACTIVE_SYNC_JOB_TARGET as u64)
            .div_ceil(bucket_width.max(1))
            .max(1) as _;
        let leaf_watermark = bucket_width.max(1);
        info!(
            %part_key, %working_level, %bucket_width, %leaf_watermark,
            %initial_working_set, %remote_size, %remote_size,
            "starting bucket sync machine"
        );
        Self {
            // remote_depth,
            active_obj_jobs: default(),
            done_listing: true,
            last_cursor,
            initial_working_set,
            leaf_watermark,
            next_page_offset: BuckId::ROOT,
            part_key,
            pending_buckets: default(),
            working_buckets: default(),
            working_level,
            list_dispatched: default(),
        }
    }

    #[tracing::instrument(skip_all)]
    pub fn on_bucket_page(
        &mut self,
        filtered_buckets: Vec<BucketSummary>,
        out: &mut Vec<BucketMachineCommand>,
    ) {
        // special signal when FilteredBuckets::Done is returned from
        // filtered_buckets
        self.done_listing = filtered_buckets.is_empty() || self.working_level == 0;
        self.list_dispatched = false;
        for buck in filtered_buckets {
            if buck.id < self.next_page_offset {
                unreachable!("remote RPC should return buckets in order");
            }
            if buck.id.level() != self.working_level {
                unreachable!("filtered_buckets must ensure that we get calc_working_level buckets");
            }
            self.next_page_offset = buck.id.increment();
            if buck.changed_at < self.last_cursor {
                unreachable!("curiousity trap: the RPC should have prevented this");
            }
            self.pending_buckets.insert(buck.id, buck);
        }
        self.schedule_leaf_requests(out);
    }

    #[tracing::instrument(skip_all)]
    pub fn on_obj_page(
        &mut self,
        pages: Map<BuckId, BucketObjLeafPage>,
        out: &mut Vec<BucketMachineCommand>,
    ) {
        for (buck_id, page) in pages {
            let Some(state) = self.working_buckets.get_mut(&buck_id) else {
                warn!("on_obj_page on an unexpected bucket: {buck_id:?}");
                continue;
            };
            state.leaf_after = page.next_after;
            state.leaf_seen += page.entries.len() as u64;
            state.leaf_inflight = false;
            state.leaf_exhausted = page.done || state.leaf_seen >= state.summary.len as u64;
            // state.pending_objs.extend(page.entries);
            // FIXME: the following code is uncessary and is too defensive
            // we should trust that the remote only returns items after the requested offset
            // basically, we can get the same page twice if a failing leaf job
            // is retried
            let mut seen = HashSet::new();
            seen.extend(state.pending_objs.iter().map(|obj| obj.obj_key.clone()));
            seen.extend(state.active_objs.keys().cloned());
            seen.extend(self.active_obj_jobs.keys().cloned());
            state
                .pending_objs
                .extend(page.entries.into_iter().filter(|obj| {
                    let allow = seen.insert(obj.obj_key.clone());
                    if !allow {
                        // warn!("dupe obj_key seen");
                    }
                    allow
                }));
        }
        self.queue_pending_objs(out);
        self.schedule_leaf_requests(out);
    }

    #[tracing::instrument(skip_all)]
    pub fn on_obj_sync_completed(&mut self, obj_key: &ObjKey, out: &mut Vec<BucketMachineCommand>) {
        let buck_id = BuckId::from_obj_key(self.working_level, obj_key);
        if let Some(state) = self.working_buckets.get_mut(&buck_id) {
            let had_active_obj = self.active_obj_jobs.remove(obj_key).is_some();
            tracing::debug!(
                buck_id = ?buck_id,
                obj_key = %obj_key,
                had_active_obj,
                "bucket completion active obj lookup"
            );
            if had_active_obj {
                let old = state.active_objs.remove(obj_key);
                if old.is_none() {
                    warn!(
                        buck_id = ?buck_id,
                        obj_key = %obj_key,
                        "bucket completion missing active bucket entry"
                    );
                }
            } else {
                warn!(
                    buck_id = ?buck_id,
                    obj_key = %obj_key,
                    active_obj_job_count = self.active_obj_jobs.len(),
                    working_bucket_count = self.working_buckets.len(),
                    pending_bucket_count = self.pending_buckets.len(),
                    "bucket completion for inactive object"
                );
            }
        } else if self.pending_buckets.contains_key(&buck_id) {
            warn!(
                buck_id = ?buck_id,
                obj_key = %obj_key,
                "bucket completion arrived while bucket still waiting on first page"
            );
        }
        tracing::debug!(
            buck_id = ?buck_id,
            active_obj_job_count = self.active_obj_jobs.len(),
            working_bucket_count = self.working_buckets.len(),
            pending_bucket_count = self.pending_buckets.len(),
            "bucket obj completion"
        );
        self.queue_pending_objs(out);
        self.schedule_leaf_requests(out);
    }

    #[tracing::instrument(skip_all)]
    fn schedule_leaf_requests(&mut self, out: &mut Vec<BucketMachineCommand>) {
        let budget =
            (Self::ACTIVE_SYNC_JOB_TARGET as usize).saturating_sub(self.active_obj_jobs.len());

        let mut selected = Vec::new();
        let mut estimated = 0usize;
        for (buck_id, buck) in self.working_buckets.extract_if(|_, buck| {
            buck.leaf_exhausted && buck.pending_objs.is_empty() && buck.active_objs.is_empty()
        }) {
            info!(?buck_id, len = %buck.summary.len, "done with buck");
        }

        let mut eligible_partials = self
            .working_buckets
            .iter()
            .map(|(&buck_id, state)| {
                (
                    buck_id,
                    state.summary.len.saturating_sub(state.leaf_seen as u32) as usize,
                )
            })
            .collect::<Vec<_>>();
        eligible_partials.sort_by_key(|(_, remaining)| *remaining);

        for (buck_id, remaining) in eligible_partials {
            if estimated >= budget {
                break;
            }
            let state = self
                .working_buckets
                .get_mut(&buck_id)
                .expect(ERROR_IMPOSSIBLE);
            if state.leaf_inflight || state.leaf_exhausted {
                continue;
            }
            state.leaf_inflight = true;
            selected.push((buck_id, state.leaf_after.clone()));
            estimated += remaining;
        }

        while estimated < budget && self.working_buckets.len() < self.initial_working_set as usize {
            if estimated > 0 && budget.saturating_sub(estimated) < self.leaf_watermark as usize {
                break;
            }
            let Some((buck_id, summary)) = self.pending_buckets.pop_first() else {
                break;
            };
            estimated += summary.len as usize;
            self.working_buckets.insert(
                buck_id,
                WorkingBucketState {
                    summary,
                    leaf_after: default(),
                    leaf_seen: default(),
                    leaf_inflight: true,
                    leaf_exhausted: default(),
                    pending_objs: default(),
                    active_objs: default(),
                },
            );
            selected.push((buck_id, None));
        }

        if !selected.is_empty() {
            let buckets = selected
                .into_iter()
                .map(|(buck_id, after)| LeafBucketRequest { buck_id, after })
                .collect();
            out.push(BucketMachineCommand::LeafBuckets {
                part_key: self.part_key.clone(),
                since: self.last_cursor,
                buckets,
            });
        }

        if self.pending_buckets.is_empty()
            && self.active_obj_jobs.is_empty()
            && self.working_buckets.is_empty()
            && self.done_listing
        {
            tracing::debug!(
                part_key = %self.part_key,
                "bucket machine upgrading to cursor"
            );
            out.push(BucketMachineCommand::UpgradeToCursor {
                part_key: self.part_key.clone(),
            });
        } else if !self.list_dispatched
            && !self.done_listing
            && (self.working_buckets.len() + self.pending_buckets.len())
                < self.initial_working_set as usize
        {
            let offset = self.next_page_offset;
            if offset.level() > self.working_level {
                self.done_listing = true;
            } else {
                self.done_listing = false;
                self.list_dispatched = true;
                out.push(BucketMachineCommand::ListBuckets {
                    since: self.last_cursor,
                    offset,
                    part_key: self.part_key.clone(),
                    working_level: self.working_level,
                });
            }
        }
    }

    fn queue_pending_objs(&mut self, out: &mut Vec<BucketMachineCommand>) {
        for (buck_id, buck) in &mut self.working_buckets {
            loop {
                if self.active_obj_jobs.len() >= Self::ACTIVE_SYNC_JOB_TARGET as usize {
                    return;
                }
                let Some(obj) = buck.pending_objs.pop() else {
                    break;
                };
                match obj.delta {
                    PartObjDelta::New | PartObjDelta::Change => {
                        out.push(BucketMachineCommand::SyncObj {
                            obj_key: obj.obj_key.clone(),
                            part_key: self.part_key.clone(),
                            remote_payload: None,
                        });
                        self.active_obj_jobs.insert(obj.obj_key.clone(), *buck_id);
                        let old = buck.active_objs.insert(obj.obj_key.clone(), obj);
                        assert!(old.is_none(), "fishy");
                    }
                    PartObjDelta::Delete => {
                        out.push(BucketMachineCommand::RemoveObjFromParts {
                            obj_key: obj.obj_key,
                            part_key: self.part_key.clone(),
                        });
                    }
                }
            }
            tracing::debug!(
                buck_id = ?buck_id,
                active_obj_count = buck.active_objs.len(),
                pending_obj_count = buck.pending_objs.len(),
                "bucket queue pending objs"
            );
        }
    }
}

pub fn calc_working_level(remote_size: u64, remote_depth: u8) -> u8 {
    let mut working_level = 0;
    let mut bucket_width = remote_size / (BuckId::ARITY as u64);
    while working_level < remote_depth
        && bucket_width > BucketMachine::ACTIVE_SYNC_JOB_TARGET as u64
    {
        working_level += 1;
        bucket_width = remote_size / (BuckId::ARITY.pow(working_level as _) as u64);
    }
    working_level
}

pub enum FilteredBuckets {
    /// This indicates that we've finished the working_lvl
    Done,
    Handoff(Vec<BucketSummary>),
    Relist(BuckId),
}

/// Whether a remote bucket summary describes the same set as the local one.
///
/// Fingerprints alone are not enough to prune on. Bucket fingerprints are modular
/// sums under the fixed `BUCKET_LIVE_FP_SEED`/`BUCKET_DEAD_FP_SEED` constants, which
/// the two sides compute independently and persist, so their basis is stable — but a
/// sum is still cancellable by a crafted pair of objects. Requiring the materialized
/// counts to agree as well means cancellation needs a count-preserving collision, and
/// it also catches summary-maintenance defects. Every field compared here is already
/// on the wire.
///
/// `changed_at` is deliberately NOT compared. It is a watermark for since-filtering,
/// not content, and dirt is view-relative: two sides holding identical sets routinely
/// carry different watermarks, so comparing it would make every bucket dirty forever.
fn summaries_agree(local: &BucketSummary, remote: &BucketSummary) -> bool {
    local.fp == remote.fp && local.len == remote.len && local.live_count == remote.live_count
}

/// Use the part store and the local bucket fingerprins to filter out
/// buckets that are identical to local.
///
/// The local summaries of every bucket the scan can reach (the prefix at or above the working
/// level) are read in one batch.
pub async fn filter_buckets<S: PartStoreRead>(
    part_key: PartKey,
    working_lvl: BuckLevel,
    buckets: Vec<BucketSummary>,
    part_store: &S,
) -> Res<FilteredBuckets> {
    if buckets.is_empty() {
        return Ok(FilteredBuckets::Done);
    }
    let reachable = buckets
        .iter()
        .take_while(|buck| buck.id.level() <= working_lvl)
        .map(|buck| buck.id)
        .collect::<Vec<_>>();
    let local_summaries = part_store.bucket_summaries(&part_key, &reachable).await?;
    let mut last_id = BuckId::ROOT;
    let in_len = buckets.len();

    let mut out = vec![];
    let mut clean_bucks = HashSet::new();
    let mut clean_ctr = 0;
    'b: for (buck, local_summary) in buckets.into_iter().zip(&local_summaries) {
        last_id = buck.id;
        for level in 0..buck.id.level() {
            let ancestor = buck.id.to_level(level);
            if clean_bucks.contains(&ancestor) {
                clean_ctr += 1;
                continue 'b;
            }
        }
        if summaries_agree(local_summary, &buck) {
            clean_bucks.insert(buck.id);
            clean_ctr += 1;
            continue;
        }
        if buck.id.level() == working_lvl {
            out.push(buck);
        }
    }
    Ok(if out.is_empty() {
        if working_lvl == 0 {
            // No dirty bucket at the working level: at level 0 a bucket is a leaf range and
            // there is nothing beneath it left to look at.
            FilteredBuckets::Done
        } else {
            // Every bucket in this page was either clean or dirty *above* the working level.
            // In both cases the buckets that settle the answer come later in the same
            // bucket-order scan, because the request carries `to_level` (ADR 012 decision 4,
            // correction 3): the cursor is the whole response and no dive is needed.
            let offset = last_id.increment();
            if offset.level() > working_lvl {
                assert_eq!(in_len, clean_ctr);
                FilteredBuckets::Done
            } else {
                FilteredBuckets::Relist(offset)
            }
        }
    } else {
        FilteredBuckets::Handoff(out)
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum PartObjDelta {
    New,
    Change,
    Delete,
}

#[derive(Debug, Clone)]
pub struct BucketObjEntry {
    obj_key: ObjKey,
    delta: PartObjDelta,
}

#[derive(Debug, Clone)]
pub struct BucketObjLeafPage {
    pub entries: Vec<BucketObjEntry>,
    pub next_after: Option<ObjKey>,
    pub done: bool,
}

// FIXME: this is too expensive
/// The local side of every requested bucket is read in two batches: the summaries of all the
/// buckets, then the states of every object listed in a bucket that is not empty locally.
pub async fn filter_objects<S: PartStoreRead>(
    part_key: PartKey,
    bucks: Map<BuckId, RawLeafBucketPage>,
    seed: FingerprintSeed,
    part_store: &S,
) -> Res<Map<BuckId, BucketObjLeafPage>> {
    let buck_ids = bucks.keys().copied().collect::<Vec<_>>();
    let summaries = part_store.bucket_summaries(&part_key, &buck_ids).await?;
    let lens = buck_ids
        .into_iter()
        .zip(summaries.iter().map(|summary| summary.len))
        .collect::<Map<_, _>>();
    let objs = bucks
        .iter()
        .filter(|(buck_id, _)| lens[buck_id] != 0)
        .flat_map(|(_, page)| page.entries.iter().map(|entry| entry.obj_key.clone()))
        .collect::<Vec<_>>();
    let states = part_store.obj_states(&objs).await?;
    let states = objs.into_iter().zip(states).collect::<Map<_, _>>();

    let mut out = Map::new();
    for (buck_id, page) in bucks {
        let mut out_objs = vec![];
        if lens[&buck_id] == 0 {
            out_objs.extend(page.entries.into_iter().filter_map(|ee| {
                if !ee.dead {
                    Some(BucketObjEntry {
                        obj_key: ee.obj_key,
                        delta: PartObjDelta::New,
                    })
                } else {
                    None
                }
            }));
            out.insert(
                buck_id,
                BucketObjLeafPage {
                    entries: out_objs,
                    next_after: page.next_after,
                    done: page.done,
                },
            );
            continue;
        }
        for obj in page.entries {
            match &states[&obj.obj_key] {
                Some(state) => {
                    if obj.dead {
                        out_objs.push(BucketObjEntry {
                            obj_key: obj.obj_key,
                            delta: PartObjDelta::Delete,
                        });
                        continue;
                    }
                    let changed = match &state.payload {
                        PayloadState::Payload(payload) => {
                            Fingerprint::new(
                                &seed,
                                &("big-sync-obj-fp-v1", obj.obj_key.clone(), payload.clone()),
                            ) != obj.fp
                        }
                        // The payload is not held here, so it can't match the remote one:
                        // fetch it.
                        PayloadState::Withheld(_) => true,
                    };
                    if changed {
                        out_objs.push(BucketObjEntry {
                            obj_key: obj.obj_key,
                            delta: PartObjDelta::Change,
                        });
                    } else if !state.parts.contains(&part_key) {
                        out_objs.push(BucketObjEntry {
                            obj_key: obj.obj_key,
                            delta: PartObjDelta::New,
                        });
                    }
                }
                None => {
                    if !obj.dead {
                        out_objs.push(BucketObjEntry {
                            obj_key: obj.obj_key,
                            delta: PartObjDelta::New,
                        });
                    }
                }
            }
        }
        out.insert(
            buck_id,
            BucketObjLeafPage {
                entries: out_objs,
                next_after: page.next_after,
                done: page.done,
            },
        );
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::part_store::ObjState;
    use crate::rev::Rev;
    use crate::store::bucket::BucketFingerprint;
    use std::collections::HashMap;

    fn part() -> PartKey {
        PartKey(ByteKey::new(&[1; 32]))
    }

    /// `calc_working_level` must never return a level exceeding `remote_depth`, even at
    /// extreme `remote_size`.
    #[test]
    fn calc_working_level_never_exceeds_remote_depth() {
        assert_eq!(calc_working_level(20_000, 0), 0);
        assert_eq!(calc_working_level(1_000_000, 0), 0);
        assert!(calc_working_level(10_000, 5) <= 5);
        assert_eq!(calc_working_level(0, 0), 0);
        assert_eq!(calc_working_level(0, 10), 0);
    }

    /// A part store that answers bucket summaries from a table. `filter_buckets` reads
    /// nothing else, so the remaining methods are unreachable rather than stubbed.
    struct SummaryStore {
        summaries: HashMap<BuckId, BucketSummary>,
    }

    #[async_trait]
    impl PartStoreRead for SummaryStore {
        async fn obj_states(&self, _objs: &[ObjKey]) -> Res<Vec<Option<ObjState>>> {
            unreachable!("filter_buckets does not read objects")
        }

        async fn bucket_summaries(
            &self,
            _part: &PartKey,
            ids: &[BuckId],
        ) -> Res<Vec<BucketSummary>> {
            Ok(ids
                .iter()
                .map(|id| {
                    self.summaries
                        .get(id)
                        .cloned()
                        .unwrap_or_else(|| panic!("no local summary for {id:?}"))
                })
                .collect())
        }

        async fn member_count(&self, _part: &PartKey) -> Res<u64> {
            unreachable!("filter_buckets does not count members")
        }

        async fn peer_part_cursors(
            &self,
            _peer: &PeerKey,
            _parts: &[PartKey],
        ) -> Res<Vec<Option<Rev>>> {
            unreachable!("filter_buckets does not read peer cursors")
        }

        async fn set_peer_part_cursors(&self, _cursors: &[(PeerKey, PartKey, Rev)]) -> Res<()> {
            unreachable!("filter_buckets does not write peer cursors")
        }
    }

    fn summary(id: BuckId, len: u32, live_count: u32, changed_at: CursorIndex) -> BucketSummary {
        BucketSummary {
            id,
            // only equality matters here, and both sides carry the same pair
            fp: (BucketFingerprint::default(), BucketFingerprint::default()),
            len,
            live_count,
            changed_at,
        }
    }

    /// Pruning must not rest on fingerprint equality alone. Both buckets below agree on their
    /// fingerprint pair, but the second disagrees on the materialized counts, so it is dirty
    /// and must be handed off. Fingerprint-only pruning called it clean, left `out` empty and
    /// returned `Relist`, silently dropping the difference.
    #[test]
    fn filter_buckets_equal_fp_different_counts_is_dirty() {
        let clean = BuckId::new(1, 0);
        let dirty = BuckId::new(1, 1);
        let store = SummaryStore {
            summaries: HashMap::from([
                // Counts agree; the watermark disagrees and must not matter.
                (clean, summary(clean, 5, 3, 99)),
                (dirty, summary(dirty, 4, 3, 7)),
            ]),
        };
        let remote = vec![summary(clean, 5, 3, 7), summary(dirty, 5, 3, 7)];

        match futures::executor::block_on(filter_buckets(part(), 1, remote, &store))
            .expect("the summary store reads infallibly")
        {
            FilteredBuckets::Handoff(out) => {
                assert_eq!(out.len(), 1, "only the count-mismatched bucket is dirty");
                assert_eq!(out[0].id, dirty);
            }
            FilteredBuckets::Done => panic!("count-mismatched bucket was pruned as clean"),
            FilteredBuckets::Relist(id) => {
                panic!("count-mismatched bucket was pruned as clean, relisting {id:?}")
            }
        }
    }

    /// The guard for the test above: equal counts still prune, even when the two sides
    /// disagree on `changed_at`.
    #[test]
    fn filter_buckets_equal_counts_different_watermark_is_clean() {
        let buck = BuckId::ROOT;
        let store = SummaryStore {
            summaries: HashMap::from([(buck, summary(buck, 5, 3, 99))]),
        };
        let remote = vec![summary(buck, 5, 3, 7)];

        match futures::executor::block_on(filter_buckets(part(), 0, remote, &store))
            .expect("the summary store reads infallibly")
        {
            FilteredBuckets::Done => {}
            FilteredBuckets::Handoff(out) => {
                panic!("identical bucket was not pruned, handed off {out:?}")
            }
            FilteredBuckets::Relist(id) => {
                panic!("identical bucket was not pruned, relisted {id:?}")
            }
        }
    }
}
