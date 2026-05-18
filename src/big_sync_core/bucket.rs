/// FIXME: this machine is per part which is not great if we have
/// large overlapping parts from a peer? They'll share buckets anyways.
/// It should be per peer like CursorSyncMachine
/// - Use batched Leafing to minimize RPC
use crate::interlude::*;

use crate::cursor::{CursorMachineCommand, CursorSyncMachine};
use crate::fingerprint::{Fingerprint, FingerprintSeed};
use crate::part_store::{CursorIndex, ObjPayload, PartStoreReadOnly};
use crate::rpc::{BuckLevel, BucketObjPageEntry, BucketSummary};
use crate::SyncJobEvt;

use std::collections::BTreeMap;

structstruck::strike! {
    pub enum BucketMachineCommand {
        SyncObj {
            obj_id: ObjId,
            cursors: Vec<CursorIndex>,
            remote_payload: Option<ObjPayload>,
            part_hints: Vec<PartId>,
        },
        SetPartCursor {
            part_id: PartId,
            cursor: CursorIndex
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
            buckets: Vec<BuckId>,
        }
        UpgradeToCursor {
            part_id: PartId,
            floor: CursorIndex,
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
        pending_buckets: BTreeMap<BuckId, BucketSummary>,

        // The number of buckets to be working on at
        // any time.
        max_working_set: u64,
        working_set: Map<BuckId, enum ActiveBucketState {
            WaitingOnPage {
                summary: BucketSummary,
            },
            Working {
                summary: BucketSummary,
                pending_objs: Vec<BucketObjEntry>,
                active_objs: Map<ObjId, BucketObjEntry>,
            }
        }>,

        active_obj_jobs: Map<ObjId, BuckId>,


        cursor_machine: CursorSyncMachine,
        cursors_cmd_buf: Vec<CursorMachineCommand>,

        latest_cursor: u64,
        last_cursor: u64,
    }
}

impl BucketMachine {
    const ACTIVE_SYNC_JOB_TARGET: u32 = 1024;
    pub const BUCKET_DIFF_THRESHOLD: u64 = 256;
    pub const GET_BUCKET_LIMIT_HINT: u32 = 8 * BuckId::ARITY as u32;

    pub fn new(
        part_id: PartId,
        remote_depth: BuckLevel,
        remote_size: u64,
        latest_cursor: CursorIndex,
        last_cursor: CursorIndex,
    ) -> Self {
        let working_level = calc_working_level(remote_size, remote_depth);
        let bucket_width = remote_size / (BuckId::ARITY.pow(working_level as _) as u64);
        let max_working_set = (Self::ACTIVE_SYNC_JOB_TARGET as u64)
            .div_ceil(bucket_width)
            .max(1) as _;
        Self {
            // remote_depth,
            active_obj_jobs: default(),
            cursor_machine: default(),
            cursors_cmd_buf: default(),
            done_listing: true,
            last_cursor,
            latest_cursor,
            max_working_set,
            next_page_offset: BuckId::ROOT,
            part_id,
            pending_buckets: default(),
            working_level,
            working_set: default(),
        }
    }

    pub fn on_bucket_page(
        &mut self,
        filtered_buckets: Vec<BucketSummary>,
        out: &mut Vec<BucketMachineCommand>,
    ) {
        // special signal when FilteredBuckets::Done is returned from
        // filtered_buckets
        self.done_listing = filtered_buckets.is_empty();
        for buck in filtered_buckets {
            if buck.id < self.next_page_offset {
                unreachable!("remote RPC should return buckets in order");
            }
            if buck.id.level() != self.working_level {
                unreachable!("filtered_buckets must ensure that we get calc_working_level buckets");
            }
            self.next_page_offset = buck.id;
            if buck.changed_at < self.last_cursor {
                unreachable!("curiousity trap: the RPC should have prevented this");
            }
            self.pending_buckets.insert(buck.id, buck);
        }
        self.queue_pending_bucks(out);
    }

    pub fn on_obj_page(
        &mut self,
        objs: Map<BuckId, Vec<BucketObjEntry>>,
        out: &mut Vec<BucketMachineCommand>,
    ) {
        for (buck_id, objs) in objs {
            let Some(old) = self.working_set.remove(&buck_id) else {
                warn!("on_obj_page on an unexpected bucket: {buck_id:?}");
                continue;
            };
            match old {
                ActiveBucketState::Working { .. } => {
                    warn!("on_obj_page on an already active bucket: {buck_id:?}");
                    continue;
                }
                ActiveBucketState::WaitingOnPage { summary } => {
                    self.working_set.insert(
                        buck_id,
                        ActiveBucketState::Working {
                            summary,
                            pending_objs: objs,
                            active_objs: default(),
                        },
                    );
                }
            }
        }
        self.queue_pending_objs(out);
    }

    pub fn on_obj_sync_completed(&mut self, evt: &SyncJobEvt, out: &mut Vec<BucketMachineCommand>) {
        if evt.cursors.is_empty() {
            let buck_id = BuckId::from_obj_id(self.working_level, &evt.obj_id);
            if let Some(ActiveBucketState::Working {
                active_objs,
                pending_objs,
                ..
            }) = self.working_set.get_mut(&buck_id)
            {
                if let Some(_obj) = active_objs.remove(&evt.obj_id) {
                    assert!(self.active_obj_jobs.remove(&evt.obj_id).is_some());
                    if pending_objs.is_empty() && active_objs.is_empty() {
                        self.working_set.remove(&buck_id);
                        self.queue_pending_bucks(out);
                    }
                }
            }
            self.queue_pending_objs(out);
        } else {
            self.cursor_machine
                .on_obj_sync_job_evt(evt, &mut self.cursors_cmd_buf);
            self.drain_cursor_cmd_buf(out);
        }
    }

    pub fn on_subscription_evt(
        &mut self,
        evt: crate::rpc::SubEvent,
        out: &mut Vec<BucketMachineCommand>,
    ) {
        self.cursor_machine
            .on_subscription_evt(evt, &mut self.cursors_cmd_buf);
        self.drain_cursor_cmd_buf(out);
    }

    fn queue_pending_bucks(&mut self, out: &mut Vec<BucketMachineCommand>) {
        let mut buckets_to_leaf = vec![];
        while self.working_set.len() < self.max_working_set as usize {
            let Some((buck_id, summary)) = self.pending_buckets.pop_first() else {
                break;
            };
            buckets_to_leaf.push(buck_id);
            self.working_set
                .insert(buck_id, ActiveBucketState::WaitingOnPage { summary });
        }
        if !buckets_to_leaf.is_empty() {
            out.push(BucketMachineCommand::LeafBuckets {
                part_id: self.part_id,
                since: self.last_cursor,
                buckets: buckets_to_leaf,
            });
        }
        // if we're out of pending_buckets to leaf
        if self.working_set.len() < self.max_working_set as usize {
            if self.next_page_offset.level() == BuckId::MAX_LEVEL {
                self.done_listing = true;
            } else {
                let offset = self.next_page_offset.increment();
                if offset.level() <= self.working_level {
                    self.done_listing = false;
                    out.push(BucketMachineCommand::ListBuckets {
                        since: self.last_cursor,
                        offset,
                        part_id: self.part_id,
                        working_level: self.working_level,
                    });
                } else {
                    self.done_listing = true;
                }
            }
        }
        if self.pending_buckets.is_empty()
            && self.active_obj_jobs.is_empty()
            && self.working_set.is_empty()
        {
            self.done_listing = true;
            out.push(BucketMachineCommand::UpgradeToCursor {
                floor: self.latest_cursor,
                part_id: self.part_id,
            });
        }
    }

    fn queue_pending_objs(&mut self, out: &mut Vec<BucketMachineCommand>) {
        for (buck_id, buck) in &mut self.working_set {
            let ActiveBucketState::Working {
                pending_objs,
                active_objs,
                ..
            } = buck
            else {
                continue;
            };
            loop {
                if self.active_obj_jobs.len() >= Self::ACTIVE_SYNC_JOB_TARGET as _ {
                    return;
                }
                let Some(obj) = pending_objs.pop() else {
                    break;
                };
                match obj.delta {
                    PartObjDelta::New | PartObjDelta::Change => {
                        out.push(BucketMachineCommand::SyncObj {
                            obj_id: obj.obj_id,
                            part_hints: vec![self.part_id],
                            cursors: vec![],
                            remote_payload: None,
                        });
                        self.active_obj_jobs.insert(obj.obj_id, *buck_id);
                        active_objs.insert(obj.obj_id, obj);
                    }
                    PartObjDelta::Delete => {
                        out.push(BucketMachineCommand::RemoveObjFromPart {
                            obj_id: obj.obj_id,
                            part_id: self.part_id,
                        });
                    }
                }
            }
        }
    }

    fn drain_cursor_cmd_buf(&mut self, out: &mut Vec<BucketMachineCommand>) {
        for cmd in self.cursors_cmd_buf.drain(..) {
            match cmd {
                CursorMachineCommand::SetPartCursor { cursor, .. } => {
                    self.latest_cursor = cursor;
                }
                CursorMachineCommand::SyncObj {
                    obj_id,
                    part_hints,
                    remote_payload,
                    cursors,
                } => out.push(BucketMachineCommand::SyncObj {
                    obj_id,
                    part_hints,
                    cursors,
                    remote_payload: Some(remote_payload),
                }),
                CursorMachineCommand::RemoveObjFromPart { obj_id, part_id } => {
                    out.push(BucketMachineCommand::RemoveObjFromPart { obj_id, part_id })
                }
            }
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
        } else {
            let offset = last_id.increment();
            if offset.level() > working_lvl {
                assert_eq!(in_len, clean_ctr);
                FilteredBuckets::Done
            }
            // all the buckets we saw were clean
            else if in_len == clean_ctr
                // the next offset is in another parent
                && offset.parent() != last_id.parent()
            {
                // climb another level
                // WARN: this trick will not work if the limit
                // is less than the arity
                assert!(BucketMachine::GET_BUCKET_LIMIT_HINT > BuckId::ARITY as _);
                FilteredBuckets::Relist(offset.parent())
            } else {
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

pub struct BucketObjEntry {
    obj_id: ObjId,
    delta: PartObjDelta,
}

// FIXME: this is too expensive
pub async fn filter_objects<K: FutureForm, S: PartStoreReadOnly<K>>(
    part_id: PartId,
    bucks: Map<BuckId, Vec<BucketObjPageEntry>>,
    seed: FingerprintSeed,
    part_store: &S,
) -> Map<BuckId, Vec<BucketObjEntry>> {
    let mut out = Map::new();
    for (buck_id, objs) in bucks {
        let summary = part_store.get_bucket_summary(part_id, buck_id).await;
        let mut out_objs = vec![];
        if summary.len == 0 {
            out_objs.extend(objs.into_iter().filter_map(|ee| {
                if !ee.dead {
                    Some(BucketObjEntry {
                        obj_id: ee.obj_id,
                        delta: PartObjDelta::New,
                    })
                } else {
                    None
                }
            }));
            out.insert(buck_id, out_objs);
            continue;
        }
        for obj in objs {
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
    }
    out
}
