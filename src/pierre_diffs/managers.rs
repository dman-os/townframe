//! Ports of `packages/diffs/src/managers/*.ts`, one Rust module per upstream
//! file (snake_case of the upstream name).

pub mod universal_rendering_manager;

#[cfg(test)]
mod universal_rendering_manager_test;
