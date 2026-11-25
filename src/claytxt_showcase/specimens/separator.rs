use crate::foundations::FoundationSection;
use clay_dioxus_kit::Button;
use clay_dioxus_kit::ButtonVariant;
use clay_dioxus_kit::Orientation;
use clay_dioxus_kit::Separator;
use dioxus::prelude::*;

/// `Separator`: horizontal between blocks, vertical between inline items, decorative or announced.
#[component]
pub(crate) fn SeparatorSpecimens() -> Element {
    rsx! {
        FoundationSection {
            id: "separator-orientations".to_string(),
            title: "Orientations".to_string(),
            description: "data-orientation horizontal|vertical.".to_string(),
            p { "Above the rule." }
            Separator {}
            p { "Below the rule." }
            div { class: "flex h-12 items-center",
                Button { variant: ButtonVariant::Ghost, onclick: |()| {}, "Left" }
                Separator { orientation: Orientation::Vertical }
                Button { variant: ButtonVariant::Ghost, onclick: |()| {}, "Right" }
            }
        }
        FoundationSection {
            id: "separator-roles".to_string(),
            title: "Decorative or announced".to_string(),
            description: "role=separator with aria-orientation, or role=none when it only decorates.".to_string(),
            p { "A separator that divides content a reader should hear about:" }
            Separator {}
            p { "And one that only decorates, which assistive technology skips:" }
            Separator { decorative: true }
        }
    }
}
