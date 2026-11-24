use crate::foundations::FoundationSection;
use crate::specimens::Frame;
use crate::specimens::COMPACT_PX;
use crate::specimens::EXPANDED_PX;
use clay_dioxus_kit::AppBar;
use clay_dioxus_kit::NavButton;
use clay_dioxus_kit::NavButtonMode;
use clay_dioxus_kit::PanelPage;
use dioxus::prelude::*;

/// `PanelPage`: the bar stays where it is while a body taller than the frame scrolls under it.
#[component]
pub fn PanelPageSpecimens() -> Element {
    rsx! {
        FoundationSection {
            id: "panel-page-scroll".to_string(),
            title: "Pinned bar, scrolling body".to_string(),
            description: "data-part bar, body. The page fills its mount, the bar is pinned and the body takes the height the bar leaves.".to_string(),
            div { class: "flex flex-wrap items-start gap-8",
                for width_px in [COMPACT_PX, EXPANDED_PX] {
                    Frame { key: "{width_px}", title: "PanelPage".to_string(), width_px,
                        PanelPage {
                            app_bar: rsx! {
                                AppBar {
                                    title: "Activity".to_string(),
                                    leading: rsx! { NavButton { mode: NavButtonMode::Back, onclick: |_| {} } },
                                }
                            },
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
