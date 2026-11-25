//! Port of `types.ts`, split by theme into submodules (the crate's module
//! item budget is 40; `types.ts` declares about 90 types). Each submodule
//! carries the upstream header with the full list of deviations.

pub mod conflict;
pub mod layout;
pub mod lines;
pub mod options;
pub mod patch;
pub mod view;

#[cfg(test)]
mod oracle_tests;
