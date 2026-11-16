//! Host creation: what an off-actor spawn hands back to the actor, and how long
//! an unavailable host waits before it is asked again.
//!
//! Kept apart from the actor itself because host startup is the one place the
//! runtime accepts a decision from outside the authority, and its failure classes
//! are what the durable fault vocabulary is chosen from.

use super::authority;
use super::session;

pub(super) enum SpawnOutcome {
    Session(Box<dyn session::Session>),
    /// The host could not start the invocation; the class says whether a later
    /// attempt could succeed and carries the host's diagnosis.
    Failed(session::SpawnError),
}

/// Host creation completes off-actor and carries the complete lease so every
/// completion can be fenced against the durable authority state.
pub(super) struct Spawned {
    pub(super) key: String,
    pub(super) lease: authority::Lease,
    pub(super) outcome: SpawnOutcome,
}

/// The re-grant delay after `failures` consecutive unavailable spawn reports:
/// doubling from 100 ms and saturating at 30 s, so 100 ms, 200 ms, 400 ms, ….
///
/// A spawn failure used to re-grant immediately, so an engine or store that was
/// briefly unavailable burned the submission's whole restart budget in a
/// millisecond-scale loop and settled as `RestartsExhausted`, which named the
/// wrong class and kept no diagnosis. The floor is deliberately short: the
/// common case is a host that is ready almost at once, and the durable restart
/// budget — not this loop — is what bounds the total damage. The count is
/// actor-local and a daemon restart resets it, which is acceptable for the same
/// reason: the restart budget survives, so a pathological host still faults
/// instead of retrying forever. `Rejected` spawn failures never reach this path;
/// they settle immediately.
pub(super) fn regrant_backoff_ms(failures: u32) -> u64 {
    const MIN_MS: u64 = 100;
    const MAX_MS: u64 = 30_000;
    let doublings = failures.saturating_sub(1).min(31);
    MIN_MS.saturating_mul(1u64 << doublings).min(MAX_MS)
}
