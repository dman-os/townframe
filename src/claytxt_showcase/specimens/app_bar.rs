use super::Frame;
use super::COMPACT_PX;
use super::EXPANDED_PX;
use crate::foundations::FoundationSection;
use clay_dioxus_kit::AppBar;
use clay_dioxus_kit::Button;
use clay_dioxus_kit::ButtonVariant;
use clay_dioxus_kit::NavButton;
use clay_dioxus_kit::NavButtonMode;
use dioxus::prelude::*;

/// `AppBar`: the panel's top strip, with every slot, with none, and with a title too long to fit.
#[component]
pub(crate) fn AppBarSpecimens() -> Element {
    rsx! {
        FoundationSection {
            id: "app-bar-slots".to_string(),
            title: "Slots".to_string(),
            description: "data-state leading|bare; data-part leading, title, trailing. A bare bar insets its title to the page gutter.".to_string(),
            div { class: "flex flex-wrap items-start gap-8",
                Frame { title: "AppBar".to_string(), width_px: COMPACT_PX,
                    AppBar {
                        title: "Settings".to_string(),
                        leading: rsx! { NavButton { mode: NavButtonMode::Sidebar, onclick: |_| {} } },
                        trailing: rsx! {
                            Button { variant: ButtonVariant::Ghost, onclick: |()| {}, "Search" }
                        },
                    }
                }
                Frame { title: "AppBar".to_string(), width_px: EXPANDED_PX,
                    AppBar {
                        title: "Settings".to_string(),
                        leading: rsx! { NavButton { mode: NavButtonMode::Back, onclick: |_| {} } },
                        trailing: rsx! {
                            Button { variant: ButtonVariant::Ghost, onclick: |()| {}, "Search" }
                            Button { variant: ButtonVariant::Ghost, onclick: |()| {}, "More" }
                        },
                    }
                }
                Frame { title: "AppBar, bare".to_string(), width_px: COMPACT_PX,
                    AppBar { title: "daytxt".to_string() }
                }
                Frame { title: "AppBar, title too long".to_string(), width_px: COMPACT_PX,
                    AppBar {
                        title: "A title far too long to fit in the strip it is given, which ends in an ellipsis".to_string(),
                        leading: rsx! { NavButton { mode: NavButtonMode::Back, onclick: |_| {} } },
                    }
                }
            }
        }
    }
}
