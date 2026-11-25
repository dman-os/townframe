use crate::foundations::FoundationSection;
use crate::specimens::Row;
use clay_dioxus_kit::NavButton;
use clay_dioxus_kit::NavButtonKind;
use dioxus::prelude::*;

/// `NavButton`: the sidebar toggle, back and forward. The rack that groups them follows.
#[component]
pub fn NavButtonSpecimens() -> Element {
    let mut presses = use_signal(|| 0u32);
    rsx! {
        FoundationSection {
            id: "nav-button-modes".to_string(),
            title: "Kinds".to_string(),
            description: "data-kind sidebar|back|forward decides the glyph, and the accessible name is the kind's label; data-part icon holds a glyph or three bars. Always a 48px target. A disabled button keeps its place and ignores its click.".to_string(),
            Row { label: "sidebar".to_string(),
                NavButton { kind: NavButtonKind::Sidebar, onclick: move |()| presses += 1 }
                NavButton { kind: NavButtonKind::HideSidebar, onclick: move |()| presses += 1 }
            }
            Row { label: "hide, with the app mark (bars on hover)".to_string(),
                NavButton { kind: NavButtonKind::HideSidebar, mark: "M", onclick: move |()| presses += 1 }
            }
            Row { label: "back".to_string(),
                NavButton { kind: NavButtonKind::Back, onclick: move |()| presses += 1 }
            }
            Row { label: "forward, disabled".to_string(),
                NavButton { kind: NavButtonKind::Forward, disabled: true, onclick: move |()| presses += 1 }
            }
            p { class: "text-sm text-foreground-muted", "Pressed {presses} times." }
        }
        super::nav_rack::NavRackSpecimens {}
    }
}
