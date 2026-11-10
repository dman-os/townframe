//! Ports of the option helpers of `packages/diffs/src/utils/`, one Rust module
//! per upstream file. `getDiffHunksRendererOptions` and `areOptionsEqual` map
//! `FileDiffOptions`, which ports with `FileDiff` (S12).

pub mod get_diff_file_input;
pub mod should_use_token_transformer;

#[cfg(test)]
mod oracle_tests;
