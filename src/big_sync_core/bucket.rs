/// FIXME: this machine is per part which is not great if we have
/// large overlapping parts from a peer? They'll share buckets anyways.
/// It should be per peer like CursorSyncMachine
/// - Use batched Leafing to minimize RPC
use crate::interlude::*;

use crate::fingerprint::{Fingerprint, FingerprintSeed};
use crate::part_store::{CursorIndex, ObjPayload, PartStoreReadOnly};
use crate::rpc::{
    BuckLevel, BucketSummary, LeafBucketPage as RawLeafBucketPage, LeafBucketRequest,
};
use crate::SyncJobEvt;

use std::collections::BTreeMap;

structstruck::strike! {
    #[derive(Debug)]
    pub enum BucketMachineCommand {
        SyncObj {
            obj_id: ObjId,
            remote_payload: Option<ObjPayload>,
            part_id: PartId,
        },
        RemoveObjFromPart {
            obj_id: ObjId,
            part_id: PartId,
        },
        ListBuckets {
            part_id: PartId,
            offset: BuckId,
            since: CursorIndex,
            working_level: BuckLevel,
        }
        LeafBuckets {
            part_id: PartId,
            since: CursorIndex,
            buckets: Vec<LeafBucketRequest>,
        }
        UpgradeToCursor {
            part_id: PartId,
        }
    }
}

structstruck::strike! {
    pub struct BucketMachine {
        part_id: PartId,
        //remote_depth: BuckLevel,

        done_listing: bool,
        working_level: BuckLevel,
        next_page_offset: BuckId,
        list_dispatched: bool,
        pending_buckets: BTreeMap<BuckId, BucketSummary>,
        working_buckets: Map<BuckId, struct WorkingBucketState {
            #![derive(Debug, Clone)]
            summary: BucketSummary,
            leaf_after: Option<ObjId>,
            leaf_seen: u64,
            leaf_inflight: bool,
            leaf_exhausted: bool,
            pending_objs: Vec<BucketObjEntry>,
            active_objs: Map<ObjId, BucketObjEntry>,
        }>,

        // Bound the initial number of buckets we admit to the active
        // leafing set before low-watermark refill takes over.
        initial_working_set: u64,
        leaf_watermark: u64,

        active_obj_jobs: Map<ObjId, BuckId>,

        last_cursor: u64,
    }
}

impl BucketMachine {
    const ACTIVE_SYNC_JOB_TARGET: u32 = 1024;
    pub const BUCKET_DIFF_THRESHOLD: u64 = 256;
    pub const GET_BUCKET_LIMIT_HINT: u32 = 8 * BuckId::ARITY as u32;
    pub const LEAF_BUCKET_LIMIT_HINT: u32 = Self::ACTIVE_SYNC_JOB_TARGET;

    pub fn new(
        part_id: PartId,
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
            %part_id, %working_level, %bucket_width, %leaf_watermark,
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
            part_id,
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
            seen.extend(state.pending_objs.iter().map(|obj| obj.obj_id));
            seen.extend(state.active_objs.keys().copied());
            seen.extend(self.active_obj_jobs.keys().copied());
            state
                .pending_objs
                .extend(page.entries.into_iter().filter(|obj| {
                    let allow = seen.insert(obj.obj_id);
                    if !allow {
                        // warn!("dupe obj_id seen");
                    }
                    allow
                }));
        }
        self.queue_pending_objs(out);
        self.schedule_leaf_requests(out);
    }

    #[tracing::instrument(skip_all)]
    pub fn on_obj_sync_completed(&mut self, evt: &SyncJobEvt, out: &mut Vec<BucketMachineCommand>) {
        let buck_id = BuckId::from_obj_id(self.working_level, &evt.obj_id);
        if let Some(state) = self.working_buckets.get_mut(&buck_id) {
            let had_active_obj = self.active_obj_jobs.remove(&evt.obj_id).is_some();
            tracing::debug!(
                buck_id = ?buck_id,
                obj_id = %evt.obj_id,
                had_active_obj,
                "bucket completion active obj lookup"
            );
            if had_active_obj {
                let old = state.active_objs.remove(&evt.obj_id);
                if old.is_none() {
                    warn!(
                        buck_id = ?buck_id,
                        obj_id = %evt.obj_id,
                        "bucket completion missing active bucket entry"
                    );
                }
            } else {
                warn!(
                    buck_id = ?buck_id,
                    obj_id = %evt.obj_id,
                    active_obj_job_count = self.active_obj_jobs.len(),
                    working_bucket_count = self.working_buckets.len(),
                    pending_bucket_count = self.pending_buckets.len(),
                    "bucket completion for inactive object"
                );
            }
        } else if self.pending_buckets.contains_key(&buck_id) {
            warn!(
                buck_id = ?buck_id,
                obj_id = %evt.obj_id,
                "bucket completion arrived while bucket still waiting on first page"
            );
        }
        tracing::debug!(
            buck_id = ?buck_id,
            cursor_count = evt.cursors.len(),
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
            selected.push((buck_id, state.leaf_after));
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
                part_id: self.part_id,
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
                part_id = %self.part_id,
                "bucket machine upgrading to cursor"
            );
            out.push(BucketMachineCommand::UpgradeToCursor {
                part_id: self.part_id,
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
                    part_id: self.part_id,
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
                            obj_id: obj.obj_id,
                            part_id: self.part_id,
                            remote_payload: None,
                        });
                        self.active_obj_jobs.insert(obj.obj_id, *buck_id);
                        let old = buck.active_objs.insert(obj.obj_id, obj);
                        assert!(old.is_none(), "fishy");
                    }
                    PartObjDelta::Delete => {
                        out.push(BucketMachineCommand::RemoveObjFromPart {
                            obj_id: obj.obj_id,
                            part_id: self.part_id,
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
    while working_level <= remote_depth
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

/// Use the part store and the local bucket fingerprins to filter out
/// buckets that are identical to local.
pub async fn filter_buckets<K: FutureForm, S: PartStoreReadOnly<K>>(
    part_id: PartId,
    working_lvl: BuckLevel,
    buckets: Vec<BucketSummary>,
    part_store: &S,
) -> FilteredBuckets {
    if buckets.is_empty() {
        return FilteredBuckets::Done;
    }
    let mut first_dirty = None;
    let mut last_id = BuckId::ROOT;
    let in_len = buckets.len();

    let mut out = vec![];
    let mut clean_bucks = HashSet::new();
    let mut clean_ctr = 0;
    'b: for buck in buckets {
        if buck.id.level() > working_lvl {
            break;
        }
        last_id = buck.id;
        for level in 0..buck.id.level() {
            let ancestor = buck.id.to_level(level);
            if clean_bucks.contains(&ancestor) {
                clean_ctr += 1;
                continue 'b;
            }
        }
        let local_summary = part_store.get_bucket_summary(part_id, buck.id).await;
        if local_summary.fp == buck.fp {
            clean_bucks.insert(buck.id);
            clean_ctr += 1;
            continue;
        }
        first_dirty.get_or_insert(buck.id);
        if buck.id.level() == working_lvl {
            out.push(buck);
        }
    }
    if out.is_empty() {
        if let Some(dirty) = first_dirty {
            // dirty buckets seen but none
            // on the working level: dive
            FilteredBuckets::Relist(dirty.to_level(dirty.level() + 1))
        } else if working_lvl == 0 {
            FilteredBuckets::Done
        } else {
            let offset = last_id.increment();
            if offset.level() > working_lvl {
                assert_eq!(in_len, clean_ctr);
                FilteredBuckets::Done
            } else {
                // all the buckets we saw were clean
                FilteredBuckets::Relist(offset)
            }
        }
    } else {
        FilteredBuckets::Handoff(out)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum PartObjDelta {
    New,
    Change,
    Delete,
}

#[derive(Debug, Clone)]
pub struct BucketObjEntry {
    obj_id: ObjId,
    delta: PartObjDelta,
}

#[derive(Debug, Clone)]
pub struct BucketObjLeafPage {
    pub entries: Vec<BucketObjEntry>,
    pub next_after: Option<ObjId>,
    pub done: bool,
}

// FIXME: this is too expensive
pub async fn filter_objects<K: FutureForm, S: PartStoreReadOnly<K>>(
    part_id: PartId,
    bucks: Map<BuckId, RawLeafBucketPage>,
    seed: FingerprintSeed,
    part_store: &S,
) -> Map<BuckId, BucketObjLeafPage> {
    let mut out = Map::new();
    for (buck_id, page) in bucks {
        let summary = part_store.get_bucket_summary(part_id, buck_id).await;
        let mut out_objs = vec![];
        if summary.len == 0 {
            out_objs.extend(page.entries.into_iter().filter_map(|ee| {
                if !ee.dead {
                    Some(BucketObjEntry {
                        obj_id: ee.obj_id,
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
            match part_store.obj_payload(obj.obj_id).await {
                Some(payload) => {
                    if !obj.dead {
                        let local_fp =
                            Fingerprint::new(&seed, &("big-sync-obj-fp-v1", obj.obj_id, payload));
                        if local_fp != obj.fp {
                            out_objs.push(BucketObjEntry {
                                obj_id: obj.obj_id,
                                delta: PartObjDelta::Change,
                            });
                        } else {
                            let local_parts = part_store.obj_parts(obj.obj_id).await;
                            if !local_parts.contains(&part_id) {
                                out_objs.push(BucketObjEntry {
                                    obj_id: obj.obj_id,
                                    delta: PartObjDelta::New,
                                });
                            }
                        }
                    } else {
                        out_objs.push(BucketObjEntry {
                            obj_id: obj.obj_id,
                            delta: PartObjDelta::Delete,
                        });
                    }
                }
                None => {
                    if !obj.dead {
                        out_objs.push(BucketObjEntry {
                            obj_id: obj.obj_id,
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
    out
}
