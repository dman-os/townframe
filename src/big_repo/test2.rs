//! Fresh regression tests for the BigRepo sync ladder.
//!
//! This suite is intentionally separate from `test.rs`: each rung has a small
//! fixture and the Tier-0 state assertions are kept close to the scenario.

mod access_matrix;
mod capability;
mod cgka;
mod conn_lifecycle;
mod convergence;
mod edge;
mod encryption;
mod ephemeral;
mod fragmentation;
mod freeze;
mod harness;
mod keyhive_rpc;
mod ladder;
mod notifications;
mod restart;
mod revocation;
mod stress;
mod topologies;
