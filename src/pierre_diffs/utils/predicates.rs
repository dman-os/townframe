//! Ports of the `are*Equal` / `is*` predicates of `packages/diffs/src/utils/`,
//! one Rust module per upstream file (snake_case of the upstream name).
//! Grouped under one module to stay inside the `utils` module item budget.

pub mod are_diff_line_annotations_equal;
pub mod are_diff_targets_equal;
pub mod are_files_equal;
pub mod are_hunk_data_equal;
pub mod are_objects_equal;
pub mod are_options_equal;
pub mod are_pre_properties_equal;
pub mod are_render_ranges_equal;
pub mod are_selection_points_equal;
pub mod are_selections_equal;
pub mod are_themes_equal;
pub mod are_virtual_window_specs_equal;
pub mod is_default_render_range;
pub mod is_diff_plain_text;

#[cfg(test)]
mod oracle_tests;
