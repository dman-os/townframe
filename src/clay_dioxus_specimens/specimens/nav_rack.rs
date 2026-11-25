use crate::foundations::FoundationSection;
use crate::specimens::Row;
use clay_dioxus_kit::NavButton;
use clay_dioxus_kit::NavButtonKind;
use clay_dioxus_kit::NavRack;
use dioxus::prelude::*;

/// `NavRack`: the group of nav buttons a host places together, as it holds them on a wide host
/// (everything, back and forward disabled with nowhere to go) and on a compact main one.
#[component]
pub fn NavRackSpecimens() -> Element {
    rsx! {
        FoundationSection {
            id: "nav-rack".to_string(),
            title: "Rack".to_string(),
            description: "role group named Navigation; it lays its buttons in a row and takes no position of its own.".to_string(),
            Row { label: "wide".to_string(),
                NavRack {
                    NavButton { kind: NavButtonKind::Sidebar, onclick: |()| {} }
                    NavButton { kind: NavButtonKind::Back, disabled: true, onclick: |()| {} }
                    NavButton { kind: NavButtonKind::Forward, disabled: true, onclick: |()| {} }
                }
            }
            Row { label: "compact main".to_string(),
                NavRack {
                    NavButton { kind: NavButtonKind::Sidebar, onclick: |()| {} }
                    NavButton { kind: NavButtonKind::Back, onclick: |()| {} }
                }
            }
        }
    }
}
