use crate::interlude::*;

use crate::fingerprint::{Fingerprint, FingerprintSeed};
use crate::store::ObjPayload;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketSummary {
    pub id: BuckId,
    pub len: u32,
    pub live_count: u32,
    // live_fp, dead_fp
    pub fp: (BucketFingerprint, BucketFingerprint),
    pub changed_at: CursorIndex,
}

/// We use the same seeds on all nodes for bucket fingerprints to nable
/// cross-peer matching
pub const BUCKET_LIVE_FP_SEED: FingerprintSeed = FingerprintSeed::new(0x6c697665, 0x6275636b);
pub const BUCKET_DEAD_FP_SEED: FingerprintSeed = FingerprintSeed::new(0x64656164, 0x6275636b);

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
struct BucketFingerprint(u64);

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketMemberKind<'a> {
    Absent,
    Live(&'a ObjPayload),
    Dead,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BucketSummaryState {
    changed_at: CursorIndex,
    live_count: u32,
    dead_count: u32,
    live_fp: BucketFingerprint,
    dead_fp: BucketFingerprint,
}

impl BucketSummaryState {
    pub fn apply_transition(
        &mut self,
        buck_id: BuckId,
        obj_id: ObjKey,
        cursor: CursorIndex,
        old: BucketMemberKind<'_>,
        new: BucketMemberKind<'_>,
    ) {
        self.changed_at = cursor;
        match old {
            BucketMemberKind::Absent => {}
            BucketMemberKind::Live(payload) => self.remove_live(buck_id, obj_id.clone(), payload),
            BucketMemberKind::Dead => self.remove_dead(buck_id, obj_id.clone()),
        }
        match new {
            BucketMemberKind::Absent => {}
            BucketMemberKind::Live(payload) => self.add_live(buck_id, obj_id, payload),
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

    pub const fn changed_at(&self) -> CursorIndex {
        self.changed_at
    }

    fn add_live(&mut self, buck_id: BuckId, obj_id: ObjKey, payload: &ObjPayload) {
        self.live_count = self.live_count.checked_add(1).expect(ERROR_IMPOSSIBLE);
        self.live_fp.add(
            &BUCKET_LIVE_FP_SEED,
            &("big-sync-bucket-live-v1", buck_id, obj_id, payload),
        );
    }

    fn remove_live(&mut self, buck_id: BuckId, obj_id: ObjKey, payload: &ObjPayload) {
        assert!(self.live_count > 0, "fishy");
        self.live_count -= 1;
        self.live_fp.remove(
            &BUCKET_LIVE_FP_SEED,
            &("big-sync-bucket-live-v1", buck_id, obj_id, payload),
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
