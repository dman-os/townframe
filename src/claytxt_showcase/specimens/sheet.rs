use super::Frame;
use super::COMPACT_PX;
use super::EXPANDED_PX;
use super::MEDIUM_PX;
use crate::foundations::FoundationSection;
use clay_dioxus_kit::Button;
use clay_dioxus_kit::ButtonVariant;
use clay_dioxus_kit::Sheet;
use clay_dioxus_kit::WidthClass;
use dioxus::prelude::*;

/// `Sheet`: a bottom sheet on a compact host and a side sheet on a wider one, sized by the width
/// class; it fills its frame, as it fills the panel that owns it.
#[component]
pub(crate) fn SheetSpecimens() -> Element {
    rsx! {
        FoundationSection {
            id: "sheet-widths".to_string(),
            title: "Width classes".to_string(),
            description: "data-placement bottom|side and data-width compact|medium|expanded; data-part scrim, panel. Dismiss by the scrim or Escape.".to_string(),
            div { class: "flex flex-wrap items-start gap-8",
                for width_px in [COMPACT_PX, MEDIUM_PX, EXPANDED_PX] {
                    Frame { key: "{width_px}", title: "Sheet".to_string(), width_px,
                        SheetSpecimen { width_px }
                    }
                }
            }
        }
    }
}

#[component]
fn SheetSpecimen(width_px: f64) -> Element {
    let width = WidthClass::from_px(width_px);
    let mut open = use_signal(|| true);
    rsx! {
        div { class: "p-4",
            Button { variant: ButtonVariant::Outline, onclick: move |()| open.set(true), "Open sheet" }
        }
        if open() {
            Sheet { width, name: "Choose a theme", on_dismiss: move |()| open.set(false),
                div { class: "p-4",
                    h3 { class: "m-0 mb-2 text-lg font-semibold", "Choose a theme" }
                    p { class: "text-text-muted", "A sheet belongs to its panel and is dismissed by its scrim or Escape." }
                }
            }
        }
    }
}
