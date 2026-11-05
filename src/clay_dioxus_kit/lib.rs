//! The Dayframe/Clay component kit. See the README for the conventions every component follows.
//!
//! Each component is a headless part under [`headless`] (behaviour, ARIA, events) and a styled
//! part at the crate root (the Dayframe skin over the semantic design tokens).
pub mod headless;

mod app_bar;
mod drawer;
mod list_details;
mod list_row;
mod nav_button;
mod sections;
mod sheet;
mod width;

pub use app_bar::AppBar;
pub use drawer::Drawer;
pub use headless::list_details::ListDetailsActions;
pub use headless::list_details::use_list_details;
pub use headless::nav_button::NavButtonMode;
pub use headless::sections::SectionPane;
pub use headless::sections::SizeRule;
pub use headless::sections::Snap;
pub use headless::sections::Split;
pub use list_details::ListDetails;
pub use list_row::ListRow;
pub use nav_button::NavButton;
pub use sections::Sections;
pub use sheet::Sheet;
pub use width::WidthClass;

#[cfg(test)]
mod testing;

#[cfg(test)]
mod conventions;
