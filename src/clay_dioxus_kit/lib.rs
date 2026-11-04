//! The Dayframe/Clay component kit. See the README for the conventions every component follows.
//!
//! Each component is a headless part under [`headless`] (behaviour, ARIA, events) and a styled
//! part at the crate root (the Dayframe skin over the semantic design tokens).
pub mod headless;

mod list_details;
mod nav_button;
mod sections;
mod sheet;
mod width;

pub use headless::list_details::ListDetailsActions;
pub use headless::list_details::use_list_details;
pub use headless::nav_button::NavButtonMode;
pub use headless::sections::SectionPane;
pub use headless::sections::SizeRule;
pub use headless::sections::Snap;
pub use headless::sections::Split;
pub use list_details::ListDetails;
pub use nav_button::NavButton;
pub use sections::Sections;
pub use sheet::Sheet;
pub use width::WidthClass;

#[cfg(test)]
mod testing;
