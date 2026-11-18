//! The behaviour half of every kit component: state, ARIA, events and the structural layout that
//! makes the behaviour work, with no appearance (`KIT-010`).
//!
//! A headless component renders real elements with their roles, accessible names, `data-state`
//! and listeners, plus only the inline style it cannot work without (flex weights, a gesture
//! overlay's `position: fixed`). Everything a theme decides arrives through the component's
//! `*Skin` prop, a plain struct of class strings, so a second skin (the agentic IDE's, say) is a
//! second set of class strings over the same behaviour. The styled components at the crate root
//! are the Dayframe skin and the only callers in this crate.

pub mod app_bar;
pub mod drawer;
pub mod list_details;
pub mod list_row;
pub mod nav_button;
pub mod sections;
pub mod sheet;
