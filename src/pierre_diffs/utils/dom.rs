//! Ports of the utils of `packages/diffs/src/utils/` that build or mutate DOM
//! nodes, one Rust module per upstream file. There is no DOM here: a node is a
//! hast [`crate::hast::Element`] (the description the Dioxus layer renders),
//! attribute and style writes are edits of its `Properties`, and what upstream
//! reads off the live DOM (platform sniffing, scrollbar probing) comes through
//! plain data or a host trait. Grouped under one module to stay inside the
//! `utils` module item budget.

pub mod create_annotation_wrapper_node;
pub mod create_gutter_utility_content_node;
pub mod create_unsafe_css_style_node;
pub mod css_wrappers;
pub mod get_or_create_code_node;
pub mod host_theme;
pub mod is_style_node;
pub mod platform;
pub mod scrollbar_gutter;
pub mod set_wrapper_node_props;

#[cfg(test)]
mod oracle_tests;
