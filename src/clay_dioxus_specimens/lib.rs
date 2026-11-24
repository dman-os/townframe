//! The specimen pages of the styling contract (one per kit component, every variant and state)
//! and the foundation pages (typography, spacing, shape, surfaces, states, semantic colours),
//! shared by the claytxt showcase and the theme lab micro so a theme is judged against the same
//! pages in both. Every cell is a `clay_dioxus_kit` component or a live `--ct-*` token: nothing
//! here is a stylesheet.
//!
//! Context the pages read from the host:
//! - `Signal<BTreeMap<String, Rc<MountedData>>>`: `FoundationSection` registers its mounted
//!   handle under its id, and `jump_to` scrolls to one. The host provides the (empty) map and
//!   owns the index UI that calls `jump_to`.
//! - `clay_dioxus_kit::Renderer`: only by the pages that show `Tabs` or `ScrubStrip`.

mod foundations;
mod specimens;

#[cfg(test)]
mod tests;

pub use foundations::ColorFoundation;
pub use foundations::FoundationSection;
pub use foundations::TokenFoundations;
pub use specimens::jump_to;
pub use specimens::AlertSpecimens;
pub use specimens::AppBarSpecimens;
pub use specimens::ButtonSpecimens;
pub use specimens::CardSpecimens;
pub use specimens::ChipSpecimens;
pub use specimens::DrawerSpecimens;
pub use specimens::ListDetailsSpecimens;
pub use specimens::ListRowSpecimens;
pub use specimens::NavButtonSpecimens;
pub use specimens::PanelPageSpecimens;
pub use specimens::ScrubStripSpecimens;
pub use specimens::SectionsSpecimens;
pub use specimens::SeparatorSpecimens;
pub use specimens::SheetSpecimens;
pub use specimens::TabsSpecimens;
