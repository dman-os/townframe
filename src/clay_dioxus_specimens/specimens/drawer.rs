use crate::foundations::FoundationSection;
use crate::specimens::Frame;
use crate::specimens::COMPACT_PX;
use crate::specimens::EXPANDED_PX;
use clay_dioxus_kit::AppBar;
use clay_dioxus_kit::Button;
use clay_dioxus_kit::ButtonVariant;
use clay_dioxus_kit::Drawer;
use clay_dioxus_kit::WidthClass;
use dioxus::prelude::*;

/// `Drawer`: the compact host's sidebar. A wide host has room for the sidebar beside its content,
/// so its frame stays empty on purpose (`KIT-101`).
#[component]
pub fn DrawerSpecimens() -> Element {
    rsx! {
        FoundationSection {
            id: "drawer-widths".to_string(),
            title: "Compact and wide".to_string(),
            description: "data-state open, data-placement leading; data-part scrim, panel. Only a compact host shows a drawer.".to_string(),
            div { class: "flex flex-wrap items-start gap-8",
                Frame { title: "Drawer".to_string(), width_px: COMPACT_PX,
                    DrawerSpecimen { width_px: COMPACT_PX }
                }
                Frame { title: "Drawer (never shown at this width)".to_string(), width_px: EXPANDED_PX,
                    DrawerSpecimen { width_px: EXPANDED_PX }
                }
            }
        }
    }
}

#[component]
fn DrawerSpecimen(width_px: f64) -> Element {
    let width = WidthClass::from_px(width_px);
    let mut open = use_signal(|| true);
    rsx! {
        div { class: "p-4",
            Button { variant: ButtonVariant::Outline, onclick: move |()| open.set(true), "Open drawer" }
        }
        Drawer { width, name: "Sidebar", open: open(), on_dismiss: move |()| open.set(false),
            nav { class: "flex flex-col gap-1 p-2", "aria-label": "Specimen sidebar",
                AppBar { title: "claytxt".to_string() }
                for title in ["Home", "Settings", "Layout"] {
                    Button { key: "{title}", variant: ButtonVariant::Ghost, onclick: |()| {}, "{title}" }
                }
            }
        }
    }
}
