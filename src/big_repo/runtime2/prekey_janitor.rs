//! Prekey janitor: keeps the local published prekey pool healthy.
//!
//! Keyhive individuals publish a pool of prekeys so that peers can invite us
//! into document encryption trees while we are offline. The semantics (see
//! `keyhive_core::principal::individual`) are: when an invitation consumes a
//! prekey, the invitee should rotate it out of the public set so a used key
//! is never reused, and keep enough keys published that concurrent inviters
//! keep finding distinct slots.
//!
//! Upstream exposes `rotate_prekey`/`expand_prekeys` but ships no policy, so
//! the genesis pool would otherwise sit frozen at 7 keys. This janitor
//! implements the policy: watch CGKA Add operations (fired for both local
//! and remotely replayed ops) that consume one of our published prekeys,
//! rotate the consumed key exactly once, and refill the pool whenever it
//! drops below [`PREKEY_POOL_FLOOR`].

use crate::interlude::*;
use beekem::id::MemberId;
use keyhive_crypto::share_key::ShareKey;

/// Minimum number of published prekeys to keep available so that concurrent
/// inviters keep finding distinct slots for document invitations.
pub(crate) const PREKEY_POOL_FLOOR: usize = 8;

/// Interior-mutable janitor state. Cloneable (state shared through `Arc`) so
/// it can live on structs that derive [`Clone`]; the hub can call the janitor
/// from concurrently spawned futures without holding a lock across awaits.
#[derive(Debug, Clone, Default)]
pub(crate) struct PrekeyJanitor {
    /// Prekeys already marked for rotation. Sync replays and merges deliver
    /// the same `CgkaOperation::Add` repeatedly; this makes rotation
    /// idempotent for the lifetime of the process. Keys rotated out of the
    /// published set disappear from `prekeys()`, so a fresh process cannot
    /// double-rotate historical ops either.
    rotated: std::sync::Arc<std::sync::Mutex<std::collections::HashSet<ShareKey>>>,
    /// Whether a low-water refill has already been issued for the current
    /// exhaustion of the pool.
    refill_issued:
        std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl PrekeyJanitor {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Claim a rotation for `pk` (dedupes op replays). Returns `true` if this
    /// is the first sighting.
    fn claim_rotation(&self, pk: &ShareKey) -> bool {
        self.rotated
            .lock()
            .expect("prekey janitor state lock poisoned")
            .insert(*pk)
    }

    /// Un-claim after a failed rotation so a later sighting can retry.
    fn release_rotation(&self, pk: &ShareKey) {
        self.rotated
            .lock()
            .expect("prekey janitor state lock poisoned")
            .remove(pk);
    }

    /// Claim the right to refill the pool. Returns `true` if no refill is
    /// already in flight for the current low-water event.
    fn claim_refill(&self) -> bool {
        !self
            .refill_issued
            .swap(true, std::sync::atomic::Ordering::Relaxed)
    }

    fn release_refill(&self) {
        self.refill_issued
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Handle a CGKA Add operation naming our own prekey (an offline invitation
/// into some document's encryption tree). Rotates the consumed prekey (once
/// per key per process), then refills the pool when it dips below
/// [`PREKEY_POOL_FLOOR`].
///
/// Never fails the caller: rotation/refill failures are logged and can be
/// retried on a later sighting of the same operation.
pub(crate) async fn housekeep_after_add(
    janitor: &PrekeyJanitor,
    keyhive: &crate::keyhive::BigKeyhiveHandle,
    added_id: &MemberId,
    pk: &ShareKey,
) {
    let local_id = keyhive.local_individual_id().await;
    if added_id.0.as_bytes() != local_id.0.as_bytes() {
        return;
    }
    let prekeys = keyhive.prekeys().await;
    if !prekeys.contains(pk) {
        return;
    }
    if !janitor.claim_rotation(pk) {
        // Replayed op: already rotated this prekey.
        return;
    }
    if let Err(err) = keyhive.rotate_prekey(*pk).await {
        janitor.release_rotation(pk);
        tracing::warn!(
            ?pk,
            error = %err,
            "prekey janitor: failed rotating consumed prekey"
        );
        return;
    }
    tracing::debug!(?pk, "prekey janitor: rotated consumed prekey");

    let prekeys = keyhive.prekeys().await;
    if prekeys.len() >= PREKEY_POOL_FLOOR {
        janitor.release_refill();
        return;
    }
    if !janitor.claim_refill() {
        return;
    }
    if let Err(err) = keyhive.expand_prekeys().await {
        janitor.release_refill();
        tracing::warn!(
            pool = prekeys.len(),
            floor = PREKEY_POOL_FLOOR,
            error = %err,
            "prekey janitor: failed expanding prekey pool"
        );
        return;
    }
    tracing::debug!(
        pool = prekeys.len(),
        floor = PREKEY_POOL_FLOOR,
        "prekey janitor: refilled prekey pool below floor"
    );
}