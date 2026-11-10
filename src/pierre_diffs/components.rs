//! Ports of `packages/diffs/src/components/*.ts`, one Rust module per upstream
//! file (snake_case of the upstream name). The logic lives here against a host
//! trait; the browser wiring is in the `pierre_diffs_dioxus` crate.

pub mod file_diff;
pub mod virtualizer;

#[cfg(test)]
mod file_diff_oracle_tests;
#[cfg(test)]
mod virtualizer_oracle_tests;
#[cfg(test)]
mod virtualizer_reconcile_queue_test;
#[cfg(test)]
mod virtualizer_scroll_state_test;
