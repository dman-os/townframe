// Slice S5: hast construction and rendering of highlighted lines (the
// utilities `DiffHunksRenderer` builds on).

pub mod create_annotation_element;
pub mod create_content_column;
pub mod create_empty_row_buffer;
pub mod create_file_header_element;
pub mod create_gutter_utility_element;
pub mod create_no_newline_element;
pub mod create_pre_element;
pub mod create_separator;
pub mod format_css_variable_prefix;
pub mod get_highlighter_theme_styles;
pub mod get_hunk_separator_slot_name;
pub mod get_icon_for_type;
pub mod get_line_annotation_name;
pub mod get_token_options;
pub mod hast_utils;
pub mod includes_file_annotations;
pub mod process_line;
pub mod render_diff_with_highlighter;
pub mod render_token_lines;
pub mod token_style;

#[cfg(test)]
mod create_file_header_element_test;
#[cfg(test)]
mod render_oracle_tests;
#[cfg(test)]
mod render_token_lines_test;
