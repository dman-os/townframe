//! The bucket walk of one part: list the buckets that differ, leaf them, and turn each leaf
//! entry into the object delta that makes the local part match the remote one.
//!
//! The machine decides *what* to ask of the part; it does not track the work it asks for. The
//! per-object jobs a walk starts (syncs and removals alike) belong to `ObjectJobs`, which merges
//! them, runs them and settles each one (`Settlement::Bucket`). Flow control is therefore a
//! budget the caller passes to [`BucketMachine::pull`] (`ObjectJobs::room()`), not a mirror of
//! the jobs in flight, and the parent upgrades the part to the cursor when
//! [`BucketMachine::walk_done`] holds **and** `ObjectJobs` has nothing outstanding for the part.
//!
//! # Laws
//!
//! 1. **The walk emits the difference.** Driven to [`BucketMachine::walk_done`], the deltas
//!    emitted are exactly the set difference between the remote part's leaf entries and the
//!    local part, as the leaf pages the caller fed in describe it: a `SyncObj` for each `New` or
//!    `Change` entry and a `RemoveObjFromParts` for each `Delete` entry, and nothing else.
//! 2. **A pull never exceeds its budget.** [`BucketMachine::pull`] emits at most `budget` object
//!    deltas, and none at budget 0. Leaf and list requests are not object deltas and are not
//!    counted.
//! 3. **At most once per walk.** Each object is emitted at most once, even when a leaf page is
//!    delivered again: a page is accepted only as the answer to the bucket's outstanding leaf
//!    request, which it names by echoing the position it was requested from
//!    ([`BucketObjLeafPage::after`]). The remote's order inside a bucket is its own, so the
//!    machine compares positions for equality and never orders them.
//! 4. **Done means every delta went out.** `walk_done` holds only once listing is finished,
//!    every working bucket is exhausted, no leaf request is outstanding and no entry is buffered.
//!    A delta that was not pulled yet keeps it false.
//! 5. **Memory is bounded.** Buffered entries stay below `buffer_target + leaf_hint` and the
//!    working set never exceeds its bound, whatever budgets the caller passes, including none.
//! 6. **Leaf pages are requested on the low watermark.** New leaf requests go out only while the
//!    buffered plus expected entries are below the watermark, and then until they reach the
//!    target; so a caller that pulls keeps the buffer fed and a caller that does not stops the
//!    walk from reading ahead.
//! 7. **A page for an unknown bucket is ignored.** So is a page for a bucket with no outstanding
//!    leaf request.
//! 8. **No stall.** While the caller keeps pulling with a positive budget and answering the
//!    requests the machine emits, the walk reaches `walk_done`.

use crate::interlude::*;

use crate::fingerprint::FingerprintSeed;
use crate::part_store::ObjPayload;
use crate::part_store::ObjState;
use crate::part_store::PartStoreRead;
use crate::part_store::bucket_summary::BucketSummary;
use crate::rev::Rev;
use crate::sync::rpc::bucket::BucketObjPageEntry as RawLeafBucketEntry;
use crate::sync::rpc::bucket::LeafBucketPage as RawLeafBucketPage;
use crate::sync::rpc::bucket::LeafBucketRequest;
use std::collections::VecDeque;

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
            since: Rev,
            working_level: BuckLevel,
        }
        LeafBuckets {
            part_key: PartKey,
            since: Rev,
            buckets: Vec<LeafBucketRequest>,
        }
    }
}

structstruck::strike! {
    #[derive(Debug)]
    pub struct BucketMachine {
        part_key: PartKey,
        //remote_depth: BuckLevel,

        working_level: BuckLevel,
        next_page_offset: BuckId,

        done_listing: bool,
        list_dispatched: bool,

        pending_buckets: BTreeMap<BuckId, BucketSummary>,
        working_buckets: BTreeMap<BuckId, struct WorkingBucketState {
            #![derive(Debug, Clone)]
            summary: BucketSummary,
            leaf_after: Option<ObjKey>,
            leaf_seen: u64,
            leaf_inflight: bool,
            leaf_exhausted: bool,
            pending_objs: VecDeque<BucketObjEntry>,
        }>,
        /// Entries across every working bucket's `pending_objs`.
        buffered: usize,

        // Bound the number of buckets we admit to the working set: enough of them to
        // cover `buffer_target` entries.
        initial_working_set: u64,
        leaf_watermark: u64,
        /// Entries the walk reads ahead of the caller's pulls.
        buffer_target: usize,
        /// The page size the leaf requests ask for.
        leaf_hint: usize,

        last_cursor: u64,
    }
}

impl BucketMachine {
    /// Entries the machine buffers ahead of the caller's pulls, and so the width of a bucket at
    /// the working level (see [`calc_working_level`]).
    const BUFFER_TARGET: u32 = 1024;
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
    /// sweep over roughly 8..=512 at part sizes either side of `BUFFER_TARGET *
    /// ARITY` (below which the whole part is one root bucket and there is nothing to
    /// prune). That sweep needs a way to force the walk below this cutoff: the per-part
    /// hint cannot, because this check overrides it.
    pub const BUCKET_DIRTY_THRESHOLD: u64 = 256;
    pub const GET_BUCKET_LIMIT_HINT: u32 = 8 * BuckId::ARITY as u32;
    pub const LEAF_BUCKET_LIMIT_HINT: u32 = Self::BUFFER_TARGET;

    pub fn new(
        part_key: PartKey,
        remote_depth: BuckLevel,
        remote_size: u64,
        last_cursor: Rev,
    ) -> Self {
        let working_level = calc_working_level(remote_size, remote_depth);
        Self::with_limits(
            part_key,
            working_level,
            remote_size,
            last_cursor,
            Self::BUFFER_TARGET as usize,
            Self::LEAF_BUCKET_LIMIT_HINT as usize,
        )
    }

    /// The machine walking at `working_level`, reading ahead `buffer_target` entries in leaf
    /// pages of `leaf_hint`. `new` is this at the production limits; tests shrink them so the
    /// watermark and the working set bind on small parts.
    fn with_limits(
        part_key: PartKey,
        working_level: BuckLevel,
        remote_size: u64,
        last_cursor: Rev,
        buffer_target: usize,
        leaf_hint: usize,
    ) -> Self {
        let bucket_width = remote_size / u64::from(BuckId::ARITY).pow(u32::from(working_level));
        let initial_working_set = (buffer_target as u64).div_ceil(bucket_width.max(1)).max(1);
        let leaf_watermark = bucket_width.max(1);
        info!(
            %part_key, %working_level, %bucket_width, %leaf_watermark,
            %initial_working_set, %remote_size,
            "starting bucket sync machine"
        );
        Self {
            // remote_depth,
            // The first page, delivered by `on_bucket_page`, decides.
            done_listing: false,
            last_cursor,
            initial_working_set,
            leaf_watermark,
            buffer_target,
            leaf_hint,
            buffered: 0,
            next_page_offset: BuckId::ROOT,
            part_key,
            pending_buckets: default(),
            working_buckets: default(),
            working_level,
            list_dispatched: default(),
        }
    }

    /// Whether the walk has nothing left to ask for or emit: listing is finished, every
    /// working bucket is exhausted, no leaf request is outstanding and no entry is buffered.
    ///
    /// The part's jobs are not the machine's to count: the parent upgrades the part to the
    /// cursor when this holds and `ObjectJobs` has nothing outstanding for it.
    pub fn walk_done(&self) -> bool {
        self.done_listing
            && self.pending_buckets.is_empty()
            && self
                .working_buckets
                .values()
                .all(|buck| buck.leaf_exhausted && buck.pending_objs.is_empty())
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
            // A page answers the bucket's outstanding request, which it names by the position
            // it was requested from. A page delivered again (a retried leaf job whose first
            // attempt also landed) names a request that is no longer outstanding, and is
            // dropped before any of its entries is buffered a second time.
            if !state.leaf_inflight || page.after != state.leaf_after {
                debug!("on_obj_page dropped a stale page for {buck_id:?}");
                continue;
            }
            state.leaf_seen += page.entries.len() as u64;
            state.leaf_inflight = false;
            state.leaf_exhausted =
                page.next_after.is_none() || state.leaf_seen >= state.summary.len as u64;
            state.leaf_after = page.next_after;
            self.buffered += page.entries.len();
            state.pending_objs.extend(page.entries);
        }
        self.schedule_leaf_requests(out);
    }

    /// Emit at most `budget` object deltas from the buffered leaf entries, then read ahead if
    /// the buffer fell below the watermark. The caller passes what it can take now
    /// (`ObjectJobs::room()`); a budget of 0 only refills.
    #[tracing::instrument(skip_all)]
    pub fn pull(&mut self, mut budget: usize, out: &mut Vec<BucketMachineCommand>) {
        for buck in self.working_buckets.values_mut() {
            while budget > 0 {
                let Some(obj) = buck.pending_objs.pop_front() else {
                    break;
                };
                budget -= 1;
                self.buffered -= 1;
                out.push(match obj.delta {
                    PartObjDelta::New | PartObjDelta::Change => BucketMachineCommand::SyncObj {
                        obj_key: obj.obj_key,
                        part_key: self.part_key.clone(),
                        remote_payload: None,
                    },
                    PartObjDelta::Delete => BucketMachineCommand::RemoveObjFromParts {
                        obj_key: obj.obj_key,
                        part_key: self.part_key.clone(),
                    },
                });
            }
            if budget == 0 {
                break;
            }
        }
        self.schedule_leaf_requests(out);
    }

    #[tracing::instrument(skip_all)]
    fn schedule_leaf_requests(&mut self, out: &mut Vec<BucketMachineCommand>) {
        self.working_buckets.retain(|buck_id, buck| {
            let done = buck.leaf_exhausted && buck.pending_objs.is_empty();
            if done {
                info!(?buck_id, len = %buck.summary.len, "done with buck");
            }
            !done
        });

        // What a leaf request of the bucket brings: its next page, at most.
        let next_page = |buck: &WorkingBucketState| {
            (buck.summary.len as usize)
                .saturating_sub(buck.leaf_seen as usize)
                .min(self.leaf_hint)
        };
        // Entries the walk holds or has asked for.
        let mut expected = self.buffered
            + self
                .working_buckets
                .values()
                .filter(|buck| buck.leaf_inflight)
                .map(next_page)
                .sum::<usize>();

        let mut selected = Vec::new();
        // Read ahead only below the low watermark, then up to the target.
        if expected < self.buffer_target.div_ceil(2) {
            let mut eligible_partials = self
                .working_buckets
                .iter()
                .filter(|(_, buck)| !buck.leaf_inflight && !buck.leaf_exhausted)
                .map(|(&buck_id, buck)| {
                    (
                        buck_id,
                        buck.summary.len.saturating_sub(buck.leaf_seen as u32) as usize,
                    )
                })
                .collect::<Vec<_>>();
            eligible_partials.sort_by_key(|(_, remaining)| *remaining);

            for (buck_id, remaining) in eligible_partials {
                if expected >= self.buffer_target {
                    break;
                }
                let state = self
                    .working_buckets
                    .get_mut(&buck_id)
                    .expect(ERROR_IMPOSSIBLE);
                state.leaf_inflight = true;
                selected.push((buck_id, state.leaf_after.clone()));
                expected += remaining.min(self.leaf_hint);
            }

            while expected < self.buffer_target
                && self.working_buckets.len() < self.initial_working_set as usize
            {
                if expected > 0
                    && self.buffer_target.saturating_sub(expected) < self.leaf_watermark as usize
                {
                    break;
                }
                let Some((buck_id, summary)) = self.pending_buckets.pop_first() else {
                    break;
                };
                expected += (summary.len as usize).min(self.leaf_hint);
                self.working_buckets.insert(
                    buck_id,
                    WorkingBucketState {
                        summary,
                        leaf_after: default(),
                        leaf_seen: default(),
                        leaf_inflight: true,
                        leaf_exhausted: default(),
                        pending_objs: default(),
                    },
                );
                selected.push((buck_id, None));
            }
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

        if !self.list_dispatched
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
}

/// The level the walk leafs at: the shallowest whose *child* buckets each hold at most
/// `BUFFER_TARGET` members (`remote_size / ARITY^(level + 1)`), capped at
/// `remote_depth`. So a part below `BUFFER_TARGET * ARITY` members is walked as its one
/// root bucket, and every deeper level is reached in turn as the part grows by `ARITY`.
pub fn calc_working_level(remote_size: u64, remote_depth: u8) -> u8 {
    let arity = u64::from(BuckId::ARITY);
    let target = u64::from(BucketMachine::BUFFER_TARGET);
    let mut working_level = 0;
    while working_level < remote_depth
        && remote_size / arity.pow(u32::from(working_level) + 1) > target
    {
        working_level += 1;
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
pub(crate) enum PartObjDelta {
    New,
    Change,
    Delete,
}

#[derive(Debug, Clone)]
pub struct BucketObjEntry {
    pub(crate) obj_key: ObjKey,
    pub(crate) delta: PartObjDelta,
}

#[derive(Debug, Clone)]
pub struct BucketObjLeafPage {
    /// Where the request this page answers started: the `after` of its [`LeafBucketRequest`].
    pub after: Option<ObjKey>,
    pub entries: Vec<BucketObjEntry>,
    /// Where the bucket's next page starts; `None` when this page finished it.
    pub next_after: Option<ObjKey>,
}

// FIXME: this is too expensive
/// The local side of every requested bucket is read in two batches: the summaries of all the
/// buckets, then the states of the objects whose delta depends on them: every entry of a bucket
/// that is not empty locally, and the dead entries of one that is. A pending membership has no
/// bucket entry, so an empty local bucket can still hold one, and a peer's removal must cancel
/// it: the dead entry becomes a removal whenever the local object is a member of the part, live
/// or pending.
pub async fn filter_objects<S: PartStoreRead>(
    part_key: PartKey,
    bucks: Map<BuckId, RawLeafBucketPage>,
    seed: FingerprintSeed,
    part_store: &S,
) -> Res<Map<BuckId, BucketObjLeafPage>> {
    let buck_ids = bucks.keys().copied().collect::<Vec<_>>();
    let summaries = part_store.bucket_summaries(&part_key, &buck_ids).await?;
    let local_empty = buck_ids
        .into_iter()
        .zip(summaries.iter().map(|summary| summary.len == 0))
        .collect::<Map<_, _>>();
    let objs = bucks
        .iter()
        .flat_map(|(buck_id, page)| {
            let empty = local_empty[buck_id];
            page.entries
                .iter()
                .filter(move |entry| !empty || entry.fp.is_none())
                .map(|entry| entry.obj_key.clone())
        })
        .collect::<Vec<_>>();
    let states = part_store.obj_states(&objs).await?;
    let states = objs.into_iter().zip(states).collect::<Map<_, _>>();

    let mut out = Map::new();
    for (buck_id, page) in bucks {
        let empty = local_empty[&buck_id];
        let entries = page
            .entries
            .into_iter()
            .filter_map(|entry| {
                let delta = match (entry.fp, empty) {
                    (None, _) => states[&entry.obj_key]
                        .as_ref()
                        .is_some_and(|state| state.parts.contains(&part_key))
                        .then_some(PartObjDelta::Delete)?,
                    (Some(_), true) => PartObjDelta::New,
                    (Some(fp), false) => match &states[&entry.obj_key] {
                        Some(ObjState {
                            content: Some((_, digest)),
                            parts,
                        }) => {
                            if RawLeafBucketEntry::fingerprint(&seed, &entry.obj_key, *digest) != fp
                            {
                                PartObjDelta::Change
                            } else if !parts.contains(&part_key) {
                                PartObjDelta::New
                            } else {
                                return None;
                            }
                        }
                        // No content yet, or no state at all: the sync brings it.
                        Some(ObjState { content: None, .. }) | None => PartObjDelta::New,
                    },
                };
                Some(BucketObjEntry {
                    obj_key: entry.obj_key,
                    delta,
                })
            })
            .collect();
        out.insert(
            buck_id,
            BucketObjLeafPage {
                after: page.after,
                entries,
                next_after: page.next_after,
            },
        );
    }
    Ok(out)
}

#[cfg(test)]
mod tests;
