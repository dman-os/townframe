//! Ports of `packages/diffs/src/utils/*.ts`, one Rust module per upstream file
//! (snake_case of the upstream name).

pub mod are_diff_targets_equal;
pub mod clean_last_newline;
pub mod clone_file_diff_metadata;
pub mod compose_cache_key;
pub mod compute_estimated_diff_heights;
pub mod compute_file_offsets;
pub mod compute_virtual_file_metrics;
pub mod create_window_from_scroll_position;
pub mod get_filetype_from_file_name;
pub mod get_hunk_side_boundaries;
pub mod get_line_ending_type;
pub mod get_singular_patch;
pub mod get_total_line_count_from_hunks;
pub mod hydrate_partial_diff;
pub mod is_diff_plain_text;
pub mod iterate_over_diff;
pub mod parse_diff_decorations;
pub mod parse_diff_from_file;
pub mod parse_line_type;
pub mod parse_patch_files;
pub mod parse_quoted_diff_file_name;
pub mod realign_change_content;
pub mod render;
pub mod split_file_contents;
pub mod trim_patch_context;
pub mod virtual_diff_layout;

#[cfg(test)]
mod compute_estimated_diff_heights_test;
#[cfg(test)]
mod compute_file_offsets_test;
#[cfg(test)]
mod get_filetype_from_file_name_test;
#[cfg(test)]
mod get_hunk_side_boundaries_test;
#[cfg(test)]
mod hydrate_partial_diff_test;
#[cfg(test)]
mod is_addition_line_renderable_test;
#[cfg(test)]
mod iterate_over_diff_test;
#[cfg(test)]
mod layout_oracle_tests;
#[cfg(test)]
mod oracle_tests;
#[cfg(test)]
mod parse_diff_from_file_test;
#[cfg(test)]
mod parse_patch_files_test;
#[cfg(test)]
mod parse_quoted_diff_file_name_test;
#[cfg(test)]
mod trim_patch_context_test;
#[cfg(test)]
mod virtual_diff_layout_test;
