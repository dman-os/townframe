use super::Frame;
use super::COMPACT_PX;
use super::EXPANDED_PX;
use crate::foundations::FoundationSection;
use clay_dioxus_kit::use_list_details;
use clay_dioxus_kit::AppBar;
use clay_dioxus_kit::ListDetails;
use clay_dioxus_kit::ListRow;
use clay_dioxus_kit::NavButton;
use clay_dioxus_kit::NavButtonMode;
use clay_dioxus_kit::SizeRule;
use clay_dioxus_kit::Split;
use clay_dioxus_kit::WidthClass;
use dioxus::prelude::*;

/// `ListDetails`: one pane at a time on a compact host (the details cover the list), both side by
/// side otherwise.
#[component]
pub(crate) fn ListDetailsSpecimens() -> Element {
    rsx! {
        FoundationSection {
            id: "list-details-widths".to_string(),
            title: "Width classes".to_string(),
            description: "data-state stacked|split on the root, covered|idle on a stacked list; data-part list, details.".to_string(),
            div { class: "flex flex-wrap items-start gap-8",
                Frame { title: "ListDetails, list".to_string(), width_px: COMPACT_PX,
                    ListDetailsSpecimen { width_px: COMPACT_PX, initially_open: None }
                }
                Frame { title: "ListDetails, details over list".to_string(), width_px: COMPACT_PX,
                    ListDetailsSpecimen { width_px: COMPACT_PX, initially_open: Some("Appearance") }
                }
                Frame { title: "ListDetails".to_string(), width_px: EXPANDED_PX,
                    ListDetailsSpecimen { width_px: EXPANDED_PX, initially_open: Some("Appearance") }
                }
            }
        }
    }
}

const SETTINGS: [&str; 4] = ["Appearance", "Sync", "Micros", "About"];

#[component]
fn SettingsRow(title: String) -> Element {
    rsx! {
        ListRow { id: title.clone(), name: "Open {title}", "{title}" }
    }
}

#[component]
fn SettingsDetails(title: String, with_back: bool) -> Element {
    let actions = use_list_details();
    rsx! {
        AppBar {
            title: title.clone(),
            leading: with_back.then(|| rsx! {
                NavButton { mode: NavButtonMode::Back, onclick: move |_| actions.back() }
            }),
        }
        p { class: "p-4 text-foreground-muted", "Settings for {title}." }
    }
}

#[component]
fn ListDetailsSpecimen(width_px: f64, initially_open: Option<&'static str>) -> Element {
    let width = WidthClass::from_px(width_px);
    let mut open = use_signal(|| initially_open.map(str::to_owned));
    let mut weights = use_signal(|| vec![1.0, 2.0]);
    let list = rsx! {
        for title in SETTINGS {
            SettingsRow { key: "{title}", title }
        }
    };
    let current = open();
    let details = match open() {
        Some(title) => {
            Some(rsx! { SettingsDetails { title, with_back: !width.fits_side_by_side() } })
        }
        // Side by side, the details socket is never empty.
        None if width.fits_side_by_side() => Some(rsx! {
            p { class: "p-4 text-foreground-muted", "Select a section." }
        }),
        None => None,
    };
    rsx! {
        ListDetails {
            width,
            list_name: "Settings",
            list,
            details_name: "Section",
            details,
            current,
            split: Split::new(weights(), width_px).rule(0, SizeRule { min: 160.0, ..SizeRule::default() }),
            on_resize: move |next| weights.set(next),
            on_open: move |key| open.set(Some(key)),
            on_back: move |()| open.set(None),
        }
    }
}
