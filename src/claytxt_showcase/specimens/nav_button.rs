use super::Row;
use crate::foundations::FoundationSection;
use clay_dioxus_kit::NavButton;
use clay_dioxus_kit::NavButtonMode;
use dioxus::prelude::*;

/// `NavButton`: the one control with two meanings, back and open-sidebar.
#[component]
pub(crate) fn NavButtonSpecimens() -> Element {
    let mut presses = use_signal(|| 0u32);
    rsx! {
        FoundationSection {
            id: "nav-button-modes".to_string(),
            title: "Modes".to_string(),
            description: "data-state back|sidebar decides the glyph, and the accessible name is the mode's label; data-part icon holds a glyph or three bars. Always a 48px target.".to_string(),
            Row { label: "back".to_string(),
                NavButton { mode: NavButtonMode::Back, onclick: move |()| presses += 1 }
            }
            Row { label: "sidebar".to_string(),
                NavButton { mode: NavButtonMode::Sidebar, onclick: move |()| presses += 1 }
            }
            p { class: "text-sm text-foreground-muted", "Pressed {presses} times." }
        }
    }
}
