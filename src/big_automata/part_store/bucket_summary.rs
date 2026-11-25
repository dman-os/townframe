use crate::interlude::*;

use crate::fingerprint::Fingerprint;
use crate::fingerprint::FingerprintSeed;
use crate::part_store::PayloadDigest;
use crate::rev::Rev;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketSummary {
    pub id: BuckId,
    pub len: u32,
    pub live_count: u32,
    // live_fp, dead_fp
    pub fp: (BucketFingerprint, BucketFingerprint),
    pub changed_at: Rev,
}

/// We use the same seeds on all nodes for bucket fingerprints to nable
/// cross-peer matching
pub const BUCKET_LIVE_FP_SEED: FingerprintSeed = FingerprintSeed::new(0x6c697665, 0x6275636b);
pub const BUCKET_DEAD_FP_SEED: FingerprintSeed = FingerprintSeed::new(0x64656164, 0x6275636b);

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BucketFingerprint(u64);

impl BucketFingerprint {
    fn add<T: core::hash::Hash>(&mut self, seed: &FingerprintSeed, value: &T) {
        self.0 = self.0.wrapping_add(Fingerprint::new(seed, value).as_u64());
    }

    fn remove<T: core::hash::Hash>(&mut self, seed: &FingerprintSeed, value: &T) {
        self.0 = self.0.wrapping_sub(Fingerprint::new(seed, value).as_u64());
    }

    fn as_u64(&self) -> u64 {
        self.0
    }
}

/// A member's place in its bucket: a live member counts by its payload digest (stored when the
/// payload was written, so a summary never rereads payloads), a removed member by its key
/// alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketMemberKind {
    Absent,
    Live(PayloadDigest),
    Dead,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BucketSummaryState {
    changed_at: Rev,
    live_count: u32,
    dead_count: u32,
    live_fp: BucketFingerprint,
    dead_fp: BucketFingerprint,
}

/// A bucket's counters as plain integers, for stores that persist [`BucketSummaryState`]: the
/// fingerprints are the wrapping sums the state keeps, so a store restores exactly what it saved.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BucketSummaryRaw {
    pub changed_at: Rev,
    pub live_count: u32,
    pub dead_count: u32,
    pub live_fp: u64,
    pub dead_fp: u64,
}

impl From<BucketSummaryRaw> for BucketSummaryState {
    fn from(raw: BucketSummaryRaw) -> Self {
        Self {
            changed_at: raw.changed_at,
            live_count: raw.live_count,
            dead_count: raw.dead_count,
            live_fp: BucketFingerprint(raw.live_fp),
            dead_fp: BucketFingerprint(raw.dead_fp),
        }
    }
}

impl BucketSummaryState {
    pub fn raw(&self) -> BucketSummaryRaw {
        BucketSummaryRaw {
            changed_at: self.changed_at,
            live_count: self.live_count,
            dead_count: self.dead_count,
            live_fp: self.live_fp.as_u64(),
            dead_fp: self.dead_fp.as_u64(),
        }
    }

    pub fn apply_transition(
        &mut self,
        buck_id: BuckId,
        obj_id: ObjKey,
        cursor: Rev,
        old: BucketMemberKind,
        new: BucketMemberKind,
    ) {
        self.changed_at = cursor;
        match old {
            BucketMemberKind::Absent => {}
            BucketMemberKind::Live(digest) => self.remove_live(buck_id, obj_id.clone(), digest),
            BucketMemberKind::Dead => self.remove_dead(buck_id, obj_id.clone()),
        }
        match new {
            BucketMemberKind::Absent => {}
            BucketMemberKind::Live(digest) => self.add_live(buck_id, obj_id, digest),
            BucketMemberKind::Dead => self.add_dead(buck_id, obj_id),
        }
    }

    pub fn summary(&self, id: BuckId) -> BucketSummary {
        BucketSummary {
            id,
            len: self.live_count + self.dead_count,
            live_count: self.live_count,
            fp: (self.live_fp, self.dead_fp),
            changed_at: self.changed_at,
        }
    }

    pub const fn changed_at(&self) -> Rev {
        self.changed_at
    }

    fn add_live(&mut self, buck_id: BuckId, obj_id: ObjKey, digest: PayloadDigest) {
        self.live_count = self.live_count.checked_add(1).expect(ERROR_IMPOSSIBLE);
        self.live_fp.add(
            &BUCKET_LIVE_FP_SEED,
            &("big-sync-bucket-live-v2", buck_id, obj_id, digest),
        );
    }

    fn remove_live(&mut self, buck_id: BuckId, obj_id: ObjKey, digest: PayloadDigest) {
        assert!(self.live_count > 0, "fishy");
        self.live_count -= 1;
        self.live_fp.remove(
            &BUCKET_LIVE_FP_SEED,
            &("big-sync-bucket-live-v2", buck_id, obj_id, digest),
        );
    }

    fn add_dead(&mut self, buck_id: BuckId, obj_id: ObjKey) {
        self.dead_count = self.dead_count.checked_add(1).expect(ERROR_IMPOSSIBLE);
        self.dead_fp.add(
            &BUCKET_DEAD_FP_SEED,
            &("big-sync-bucket-dead-v1", buck_id, obj_id),
        );
    }

    fn remove_dead(&mut self, buck_id: BuckId, obj_id: ObjKey) {
        assert!(self.dead_count > 0, "fishy");
        self.dead_count -= 1;
        self.dead_fp.remove(
            &BUCKET_DEAD_FP_SEED,
            &("big-sync-bucket-dead-v1", buck_id, obj_id),
        );
    }
}
