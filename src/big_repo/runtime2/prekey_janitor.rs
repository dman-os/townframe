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
//! implements the policy: when a `CgkaOperation::Add` consuming one of our
//! published prekeys is observed in the durable admission log (see
//! [`super::prekey_janitor_worker`]), rotate the consumed key exactly once
//! and refill the pool whenever it drops below [`PREKEY_POOL_FLOOR`].
//!
//! Idempotence is structural, not tracked: the rotation guard is membership
//! in the *current published set*, and a rotated key is tombstoned out of
//! that set (see `BigKeyhiveHandle::prekeys`, which folds `{Add, Rotate}`
//! ops in two passes exactly like upstream `PrekeyState::build`). An
//! arbitrary replay of already-handled admissions therefore hits the
//! precheck's false branch and is a no-op; no in-memory dedupe state is
//! needed, so janitor behavior survives process restarts unchanged.

use crate::interlude::*;
use beekem::id::MemberId;
use keyhive_crypto::share_key::ShareKey;

/// Minimum number of published prekeys to keep available so that concurrent
/// inviters keep finding distinct slots for document invitations.
pub(crate) const PREKEY_POOL_FLOOR: usize = 8;

/// Handle a CGKA Add operation naming our own prekey (an offline invitation
/// into some document's encryption tree). Rotates the consumed prekey and
/// refills the pool while it sits below [`PREKEY_POOL_FLOOR`].
///
/// The rotation only fires for the *current* published set, so replays of
/// already-handled rows (the admission cursor is at-least-once) are
/// structural no-ops. Failures propagate to the caller: the worker must not
/// advance its durable cursor past a row whose housekeeping failed, so the
/// row is retried on the next poll.
pub(crate) async fn housekeep_after_add(
    keyhive: &crate::keyhive::BigKeyhiveHandle,
    added_id: &MemberId,
    pk: &ShareKey,
) -> Res<()> {
    let local_id = keyhive.local_individual_id().await;
    if added_id.0.as_bytes() != local_id.0.as_bytes() {
        tracing::debug!(
            ?added_id,
            local = ?local_id,
            "prekey janitor: Add names another agent"
        );
        return Ok(());
    }

    let prekeys = keyhive.prekeys().await;
    if !prekeys.contains(pk) {
        tracing::debug!(
            ?pk,
            pool = prekeys.len(),
            "prekey janitor: pk not in published set (replayed admission)"
        );
        return Ok(());
    }
    keyhive
        .rotate_prekey(*pk)
        .await
        .map_err(|err| ferr!("prekey janitor: rotating consumed prekey failed: {err}"))?;
    tracing::debug!(?pk, "prekey janitor: rotated consumed prekey");

    refill_to_floor(keyhive).await
}

/// Keep publishing prekeys until the pool reaches [`PREKEY_POOL_FLOOR`].
///
/// Idempotent: a pool already at the floor does nothing.
///
/// Termination is structural rather than argued. A published pool is never empty (see
/// `BigKeyhiveHandle::prekeys`), and a successful `expand_prekeys` inserts one freshly
/// generated `AddKeyOp` into the same op map that `prekeys` folds — a fresh key can never
/// be a rotation tombstone, because tombstone values come from keys already in the set, and
/// the tombstone pass only ever skips removals. So each iteration must grow the published
/// set by at least one key, and at most one expansion per missing slot is ever needed.
///
/// Both ways out of the loop are crashes, never "give up and continue": a published pool that
/// stops growing while the core keeps publishing is a broken invariant, not a race between
/// data planes, so proceeding below the floor would hand concurrent inviters a pool with no
/// distinct slot left. The growth check is the primary diagnostic; the expansion bound is the
/// backstop that keeps a weakened growth check from silently restoring an unbounded spin.
pub(crate) async fn refill_to_floor(keyhive: &crate::keyhive::BigKeyhiveHandle) -> Res<()> {
    let mut pool = keyhive.prekeys().await.len();
    let mut expansions = 0usize;
    while pool < PREKEY_POOL_FLOOR {
        if expansions == PREKEY_POOL_FLOOR {
            panic!(
                "prekey janitor: pool still below the floor after {expansions} expansions \
                 (pool={pool} floor={PREKEY_POOL_FLOOR}); every expansion is required to \
                 publish one fresh prekey, so the published prekey view and the keyhive \
                 core's prekey state have diverged and no further expansion can reach the \
                 floor"
            );
        }
        keyhive
            .expand_prekeys()
            .await
            .map_err(|err| ferr!("prekey janitor: growing pool below floor failed: {err}"))?;
        expansions += 1;
        let grown = keyhive.prekeys().await.len();
        if grown <= pool {
            panic!(
                "prekey janitor: publishing a prekey did not grow the pool \
                 (pool={pool} floor={PREKEY_POOL_FLOOR} expansions={expansions}); the \
                 published prekey view and the keyhive core's prekey state have diverged, \
                 so no further expansion can reach the floor"
            );
        }
        pool = grown;
    }
    if pool != PREKEY_POOL_FLOOR {
        tracing::debug!(
            pool,
            floor = PREKEY_POOL_FLOOR,
            "prekey janitor: pool refilled"
        );
    }
    Ok(())
}
