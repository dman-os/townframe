//! Ports of `packages/diffs/src/utils/*.ts`, one Rust module per upstream file
//! (snake_case of the upstream name).

pub mod are_diff_targets_equal;
pub mod clean_last_newline;
pub mod clone_file_diff_metadata;
pub mod compose_cache_key;
pub mod get_filetype_from_file_name;
pub mod get_hunk_side_boundaries;
pub mod get_line_ending_type;
pub mod get_singular_patch;
pub mod get_total_line_count_from_hunks;
pub mod parse_diff_from_file;
pub mod parse_line_type;
pub mod parse_patch_files;
pub mod parse_quoted_diff_file_name;
pub mod realign_change_content;
pub mod split_file_contents;

#[cfg(test)]
mod get_filetype_from_file_name_test;
#[cfg(test)]
mod oracle_tests;
#[cfg(test)]
mod parse_diff_from_file_test;
#[cfg(test)]
mod parse_patch_files_test;
#[cfg(test)]
mod parse_quoted_diff_file_name_test;
