//! Fresh regression tests for the BigRepo sync ladder.
//!
//! This suite is intentionally separate from `test.rs`: each rung has a small
//! fixture and the Tier-0 state assertions are kept close to the scenario.

mod access_matrix;
mod cgka;
mod harness;
mod ladder;
mod notifications;
mod revocation;
