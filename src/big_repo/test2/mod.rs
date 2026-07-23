//! Fresh regression tests for the BigRepo sync ladder.
//!
//! This suite is intentionally separate from `test.rs`: each rung has a small
//! fixture and the Tier-0 state assertions are kept close to the scenario.

mod access_matrix;
mod capability;
mod cgka;
mod convergence;
mod edge;
mod encryption;
mod ephemeral;
mod harness;
mod ladder;
mod notifications;
mod restart;
mod revocation;
mod topologies;
