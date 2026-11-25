//! The claytxt/Clay component kit. See the README for the conventions every component follows.
//!
//! Each component is a headless part under [`headless`] (behaviour, ARIA, events) and a skin. The
//! components of the styling contract (`docs/design-language/styling-contract.md`: `Button`,
//! `Tabs`, `Card`, `Separator`, `Alert`, `Chip`, `ScrubStrip`) are their headless part alone,
//! re-exported at the root, and their skin is a stylesheet (`styles/kit/<name>.css`) keyed on
//! part names and states. The older components still have a styled part at the crate root (the
//! claytxt skin as Tailwind classes) until the contract reaches them.
pub mod headless;

#[cfg(feature = "host")]
mod foundation;
mod width;

#[cfg(feature = "host")]
pub use foundation::FoundationStyles;
#[cfg(feature = "host")]
pub use foundation::ThemeMode;
#[cfg(feature = "host")]
pub use foundation::ThemeRoot;
#[cfg(feature = "host")]
pub use foundation::ThemeStyles;
pub use headless::Orientation;
pub use headless::Status;
pub use headless::alert::Alert;
pub use headless::app_bar::AppBar;
pub use headless::button::Button;
pub use headless::button::ButtonSize;
pub use headless::button::ButtonVariant;
pub use headless::card::Card;
pub use headless::card::CardContent;
pub use headless::card::CardFooter;
pub use headless::card::CardHeader;
pub use headless::card::CardVariant;
pub use headless::chip::Chip;
pub use headless::chip::ChipVariant;
pub use headless::drawer::Drawer;
pub use headless::list_details::ListDetails;
pub use headless::list_details::ListDetailsActions;
pub use headless::list_details::use_list_details;
pub use headless::list_row::ListRow;
pub use headless::nav_button::NavButton;
pub use headless::nav_button::NavButtonKind;
pub use headless::nav_button::NavRack;
pub use headless::panel_page::PanelPage;
pub use headless::scrub_strip::ScrubStrip;
pub use headless::sections::Renderer;
pub use headless::sections::SectionPane;
pub use headless::sections::Sections;
pub use headless::sections::SizeRule;
pub use headless::sections::Snap;
pub use headless::sections::Split;
pub use headless::separator::Separator;
pub use headless::sheet::Sheet;
pub use headless::tabs::TabItem;
pub use headless::tabs::TabPanel;
pub use headless::tabs::Tabs;
pub use width::WidthClass;

#[cfg(test)]
mod testing;

#[cfg(test)]
mod conventions;
