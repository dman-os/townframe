use crate::foundations::FoundationSection;
use crate::specimens::Frame;
use crate::specimens::COMPACT_PX;
use crate::specimens::EXPANDED_PX;
use clay_dioxus_kit::AppBar;
use clay_dioxus_kit::NavButton;
use clay_dioxus_kit::NavButtonKind;
use clay_dioxus_kit::NavRack;
use clay_dioxus_kit::PanelPage;
use dioxus::prelude::*;

/// `PanelPage`: the bar stays where it is while a body taller than the frame scrolls under it.
#[component]
pub fn PanelPageSpecimens() -> Element {
    rsx! {
        FoundationSection {
            id: "panel-page-scroll".to_string(),
            title: "Pinned bar, floating rack, scrolling body".to_string(),
            description: "data-part rack, bar, body. The page fills its mount, the bar is pinned and the body takes the height the bar leaves; the rack floats over the top-left corner and the title starts after it.".to_string(),
            div { class: "flex flex-wrap items-start gap-8",
                for width_px in [COMPACT_PX, EXPANDED_PX] {
                    Frame { key: "{width_px}", title: "PanelPage".to_string(), width_px,
                        PanelPage {
                            rack: rsx! {
                                NavRack {
                                    NavButton { kind: NavButtonKind::Sidebar, onclick: |_| {} }
                                    NavButton { kind: NavButtonKind::Back, onclick: |_| {} }
                                }
                            },
                            app_bar: rsx! { AppBar { title: "Activity".to_string() } },
                            ul { class: "m-0 list-none p-4",
                                for index in 1..=14 {
                                    li { key: "{index}", class: "py-2 text-foreground-muted", "Entry {index}" }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
