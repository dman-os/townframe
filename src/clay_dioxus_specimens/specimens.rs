//! One specimen page per component of the styling contract (`KIT-204`), each showing every
//! variant and state the component has. A page is a set of `FoundationSection`s (so the lab's
//! index strip can jump between them) whose cells are the kit's own components; the only styling
//! here is layout glue (`flex`, `gap`) around them.
//!
//! The pages that hold a renderer-dependent kit component (`Tabs`, `ScrubStrip`) read the
//! `clay_dioxus_kit::Renderer` from context: the host provides it above the pages.
//!
//! The pages are what a theme is judged against: every cell is a stable vocabulary
//! (`docs/design-language/styling-contract.md`) that a `user`-layer stylesheet can restyle.
mod alert;
mod app_bar;
mod button;
mod card;
mod chip;
mod drawer;
mod list_details;
mod list_row;
mod nav_button;
mod nav_rack;
mod panel_page;
mod scrub_strip;
mod sections;
mod separator;
mod sheet;
mod tabs;

pub use alert::AlertSpecimens;
pub use app_bar::AppBarSpecimens;
pub use button::ButtonSpecimens;
pub use card::CardSpecimens;
pub use chip::ChipSpecimens;
pub use drawer::DrawerSpecimens;
pub use list_details::ListDetailsSpecimens;
pub use list_row::ListRowSpecimens;
pub use nav_button::NavButtonSpecimens;
pub use panel_page::PanelPageSpecimens;
pub use scrub_strip::ScrubStripSpecimens;
pub use sections::SectionsSpecimens;
pub use separator::SeparatorSpecimens;
pub use sheet::SheetSpecimens;
pub use tabs::TabsSpecimens;

use clay_dioxus_kit::WidthClass;
use dioxus::prelude::*;
use std::collections::BTreeMap;
use std::rc::Rc;

pub(crate) const COMPACT_PX: f64 = 390.0;
pub(crate) const MEDIUM_PX: f64 = 700.0;
pub(crate) const EXPANDED_PX: f64 = 960.0;
const FRAME_HEIGHT_PX: f64 = 260.0;

/// A host stand-in: a bordered box of known size that the component fills.
#[component]
pub(crate) fn Frame(title: String, width_px: f64, children: Element) -> Element {
    rsx! {
        figure {
            class: "m-0",
            figcaption {
                class: "mb-2 text-sm font-medium text-foreground-muted",
                "{title} — {width_px}px ({WidthClass::from_px(width_px):?})"
            }
            // The 1px `bg-border` padding is the frame's outline: Tailwind's `border` utilities drew
            // nothing in the Blitz showcase (observed; cause not investigated).
            div {
                class: "overflow-hidden rounded-lg bg-border p-px",
                style: "width:{width_px + 2.0}px;height:{FRAME_HEIGHT_PX + 2.0}px",
                div {
                    class: "relative h-full w-full overflow-hidden rounded-lg bg-surface-sunken",
                    {children}
                }
            }
        }
    }
}

/// A labelled row of cells.
#[component]
fn Row(label: String, children: Element) -> Element {
    rsx! {
        div { class: "flex flex-wrap items-center gap-3",
            span { class: "w-28 shrink-0 text-sm text-foreground-muted", "{label}" }
            {children}
        }
    }
}

/// Scrolls the section registered under `id` into view (the lab's sections register themselves,
/// see `FoundationSection`). A failure to scroll is a bug in the renderer integration.
pub fn jump_to(anchors: Signal<BTreeMap<String, Rc<MountedData>>>, id: String) {
    let target = anchors.peek().get(&id).cloned();
    spawn(async move {
        if let Some(target) = target {
            target
                .scroll_to(ScrollBehavior::Instant)
                .await
                .expect("the renderer must be able to scroll a section into view");
        }
    });
}
