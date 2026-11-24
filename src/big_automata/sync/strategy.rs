//! The per-part decision of the peer strategy task: which way to catch up on a part, given what
//! the peer summarized. Pure: the bucket walk's RPC loop stays with the task, which may still
//! settle a [`PeerPartStrat::BucketWalk`] on cursor replay when the walk finds nothing dirty.

use crate::AllowedStrat;
use crate::sync::bucket::BucketMachine;
use crate::sync::bucket::calc_working_level;
use crate::interlude::*;
use crate::part_store::PartSummary;
use crate::rev::Rev;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerPartStrat {
    /// Replay the peer's log from `from`, the cursor we hold for it, up to `latest`.
    Cursor { from: Rev, latest: Rev },
    /// Walk the buckets down to `working_level`, then hand over to cursor replay at `latest`,
    /// the summary's `latest_cursor`: the walk reads state no older than the summary, so
    /// replaying from there may repeat rows but never skips one. The walk lists changes since
    /// the cursor we hold, which the caller already has.
    BucketWalk {
        working_level: BuckLevel,
        remote_depth: BuckLevel,
        remote_len: u64,
        latest: Rev,
    },
    /// The peer's answer has no entry for the part: it is unknown to the peer or refused.
    /// Per part, so a refused part never takes a readable neighbor's decision down with it.
    Unknown,
}

/// Decide every part of `parts` from the peer's `summaries`.
///
/// - `cursors` is what we hold per part for this peer; a part without an entry has consumed
///   nothing, so replays from 0. The peer counted `dirty` against the cursor we advertised, so
///   the caller must pass the cursors it advertised.
/// - `modes` overrides `default_mode` per part. [`AllowedStrat::Cursor`] forces cursor replay.
pub fn decide_peer_strat(
    parts: &Set<PartKey>,
    summaries: &Map<PartKey, PartSummary>,
    cursors: &Map<PartKey, Rev>,
    modes: &Map<PartKey, AllowedStrat>,
    default_mode: AllowedStrat,
) -> Map<PartKey, PeerPartStrat> {
    parts
        .iter()
        .map(|part_key| {
            let Some(summary) = summaries.get(part_key) else {
                return (part_key.clone(), PeerPartStrat::Unknown);
            };
            let from = cursors.get(part_key).copied().unwrap_or(0);
            let cursor = PeerPartStrat::Cursor {
                from,
                latest: summary.latest_cursor,
            };
            let mode = modes.get(part_key).copied().unwrap_or(default_mode);
            // The threshold is on the count the peer published, not on the gap between
            // cursors: a large gap can hold one relevant change, and a small one hundreds.
            let dirty = summary
                .dirty
                .member_changes
                .saturating_add(summary.dirty.access_changes);
            // A peer that keeps no bucket levels offers no walk: like the original, whose
            // summary carried a bucket strat only for `deepest_bucket_level > 0`, a large dirty
            // count then degrades to the cursor.
            let strat = if matches!(mode, AllowedStrat::Cursor)
                || dirty <= BucketMachine::BUCKET_DIRTY_THRESHOLD
                || summary.deepest_bucket_level == 0
            {
                cursor
            } else {
                PeerPartStrat::BucketWalk {
                    working_level: calc_working_level(
                        summary.member_count,
                        summary.deepest_bucket_level,
                    ),
                    remote_depth: summary.deepest_bucket_level,
                    remote_len: summary.member_count,
                    latest: summary.latest_cursor,
                }
            };
            (part_key.clone(), strat)
        })
        .collect()
}

#[cfg(test)]
mod tests;
