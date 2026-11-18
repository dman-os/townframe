//! Specimens of the `clay_dioxus_kit` components, each at a compact and an expanded host width.
//!
//! The kit never measures, so a specimen is a fixed-size frame whose width is both its CSS width
//! and the `WidthClass` / split extent it hands the component: the same contract a real host
//! (the shell) follows with its window.
use crate::specimens::Frame;
use crate::specimens::COMPACT_PX;
use crate::specimens::EXPANDED_PX;
use clay_dioxus_kit::use_list_details;
use clay_dioxus_kit::AppBar;
use clay_dioxus_kit::ListDetails;
use clay_dioxus_kit::ListRow;
use clay_dioxus_kit::NavButton;
use clay_dioxus_kit::NavButtonMode;
use clay_dioxus_kit::SectionPane;
use clay_dioxus_kit::Sections;
use clay_dioxus_kit::SizeRule;
use clay_dioxus_kit::Snap;
use clay_dioxus_kit::Split;
use clay_dioxus_kit::WidthClass;
use dioxus::prelude::*;

/// Which specimen groups to show: all of them, or the one a screenshot run names through
/// `DAYTXT_SHOWCASE_KIT` (`sections`, `list`). The native window cannot scroll
/// under automation, so a group per run is how every specimen gets captured.
#[component]
pub fn KitSpecimens(only: Option<String>) -> Element {
    let shows = |group: &str| only.as_deref().is_none_or(|only| only == group);
    rsx! {
        div {
            class: "flex flex-col gap-8",
            if shows("sections") {
                div {
                    class: "flex flex-wrap items-start gap-8",
                    Frame { title: "Sections", width_px: COMPACT_PX,
                        SectionsSpecimen { width_px: COMPACT_PX, three: false }
                    }
                    Frame { title: "Sections", width_px: EXPANDED_PX,
                        SectionsSpecimen { width_px: EXPANDED_PX, three: true }
                    }
                }
            }
            if shows("list") {
                div {
                    class: "flex flex-wrap items-start gap-8",
                    Frame { title: "ListDetails, list", width_px: COMPACT_PX,
                        ListDetailsSpecimen { width_px: COMPACT_PX, initially_open: None }
                    }
                    Frame { title: "ListDetails, details over list", width_px: COMPACT_PX,
                        ListDetailsSpecimen { width_px: COMPACT_PX, initially_open: Some("Appearance") }
                    }
                    Frame { title: "ListDetails", width_px: EXPANDED_PX,
                        ListDetailsSpecimen { width_px: EXPANDED_PX, initially_open: Some("Appearance") }
                    }
                }
            }
        }
    }
}

#[component]
fn PaneBody(title: String, size_px: f64) -> Element {
    rsx! {
        div {
            class: "p-3",
            p { class: "font-medium", "{title}" }
            p { class: "text-sm text-foreground-muted", "{size_px:.0}px" }
        }
    }
}

#[component]
fn SectionsSpecimen(width_px: f64, three: bool) -> Element {
    // The Kotlin sidebar: an 80px rail, or at least 240px, never in between.
    let rail = SizeRule {
        min: 80.0,
        max: f64::INFINITY,
        snap: Some(Snap {
            below: 160.0,
            to: 80.0,
        }),
        collapse_past: None,
    };
    let mut weights = use_signal(|| {
        if three {
            vec![240.0, 300.0, 420.0]
        } else {
            vec![80.0, 310.0]
        }
    });
    let mut split = Split::new(weights(), width_px).rule(0, rail);
    for index in 1..weights.read().len() {
        split = split.rule(
            index,
            SizeRule {
                min: 120.0,
                ..SizeRule::default()
            },
        );
    }
    let sizes = split.sizes();
    let names = ["Sidebar", "List", "Details"];
    let panes = sizes
        .iter()
        .zip(if three {
            &names[..]
        } else {
            &["Sidebar", "Content"][..]
        })
        .map(|(size, name)| SectionPane {
            name: (*name).to_owned(),
            content: rsx! { PaneBody { title: *name, size_px: *size } },
        })
        .collect();
    rsx! {
        Sections {
            label: "Specimen sections",
            panes,
            split,
            on_resize: move |next| weights.set(next),
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
