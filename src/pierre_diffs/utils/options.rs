//! Ports of the option helpers of `packages/diffs/src/utils/`, one Rust module
//! per upstream file. `areOptionsEqual` is in `predicates`.

pub mod get_diff_file_input;
pub mod get_diff_hunks_renderer_options;
pub mod should_use_token_transformer;

#[cfg(test)]
mod oracle_tests;
