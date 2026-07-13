//! test2 harness: topology builder, fixtures, Tier-0 invariants, diagnostics.
//!
//! See `play.big_repo.test2.md` for the full design. This module is the
//! foundation every tier builds on; ladder/tier files consume [`topo::Pair`],
//! [`fixtures`] helpers, and [`heads::tier0_invariants`].
//!
//! # Synchronous-by-design
//! Sync helpers in [`fixtures`] make exactly one `sync_*` call. There are no
//! retry loops: a missed post-condition after one sync surfaces as an error,
//! exposing runtime2 ordering bugs instead of masking them.

pub(crate) mod dump;
pub(crate) mod fixtures;
pub(crate) mod heads;
pub(crate) mod log_nickname;
pub(crate) mod topo;

pub(crate) use topo::Pair;
