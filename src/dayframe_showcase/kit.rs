//! Specimens of the `clay_dioxus_kit` components, each at a compact and an expanded host width.
//!
//! The kit never measures, so a specimen is a fixed-size frame whose width is both its CSS width
//! and the `WidthClass` / split extent it hands the component: the same contract a real host
//! (the shell) follows with its window.
use clay_dioxus_kit::AppBar;
use clay_dioxus_kit::Drawer;
use clay_dioxus_kit::ListDetails;
use clay_dioxus_kit::ListRow;
use clay_dioxus_kit::NavButton;
use clay_dioxus_kit::NavButtonMode;
use clay_dioxus_kit::SectionPane;
use clay_dioxus_kit::Sections;
use clay_dioxus_kit::Sheet;
use clay_dioxus_kit::SizeRule;
use clay_dioxus_kit::Snap;
use clay_dioxus_kit::Split;
use clay_dioxus_kit::WidthClass;
use clay_dioxus_kit::use_list_details;
use dioxus::prelude::*;

const COMPACT_PX: f64 = 390.0;
const EXPANDED_PX: f64 = 960.0;
const FRAME_HEIGHT_PX: f64 = 260.0;

/// A host stand-in: a bordered box of known size that the component fills.
#[component]
fn Frame(title: String, width_px: f64, children: Element) -> Element {
    rsx! {
        figure {
            class: "m-0",
            figcaption {
                class: "mb-2 text-sm font-medium text-foreground-muted",
                "{title} — {width_px}px ({WidthClass::from_px(width_px):?})"
            }
            // The 1px `bg-border` padding is the frame's outline: Tailwind's `border` utilities drew
            // nothing in the Blitz showcase (observed; cause not investigated).
            div {
                class: "overflow-hidden rounded-lg bg-border p-px",
                style: "width:{width_px + 2.0}px;height:{FRAME_HEIGHT_PX + 2.0}px",
                div {
                    class: "relative h-full w-full overflow-hidden rounded-lg bg-surface-sunken",
                    {children}
                }
            }
        }
    }
}

/// Which specimen groups to show: all of them, or the one a screenshot run names through
/// `DAYFRAME_SHOWCASE_KIT` (`nav`, `appbar`, `sections`, `list`, `sheet`, `drawer`). The native window cannot scroll
/// under automation, so a group per run is how every specimen gets captured.
#[component]
pub fn KitSpecimens(only: Option<String>) -> Element {
    let shows = |group: &str| only.as_deref().is_none_or(|only| only == group);
    rsx! {
        div {
            class: "flex flex-col gap-8",
            if shows("nav") {
                div {
                    class: "flex items-center gap-4",
                    NavButton { mode: NavButtonMode::Back, onclick: |_| {} }
                    NavButton { mode: NavButtonMode::Sidebar, onclick: |_| {} }
                    span { class: "text-sm text-foreground-muted", "NavButton: back, then sidebar" }
                }
            }
            if shows("appbar") {
                div {
                    class: "flex flex-wrap items-start gap-8",
                    Frame { title: "AppBar", width_px: COMPACT_PX,
                        AppBarSpecimen {}
                    }
                    Frame { title: "AppBar", width_px: EXPANDED_PX,
                        AppBarSpecimen {}
                    }
                }
            }
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
            if shows("sheet") {
                div {
                    class: "flex flex-wrap items-start gap-8",
                    Frame { title: "Sheet", width_px: COMPACT_PX,
                        SheetSpecimen { width_px: COMPACT_PX }
                    }
                    Frame { title: "Sheet", width_px: EXPANDED_PX,
                        SheetSpecimen { width_px: EXPANDED_PX }
                    }
                }
            }
            if shows("drawer") {
                div {
                    class: "flex flex-wrap items-start gap-8",
                    Frame { title: "Drawer", width_px: COMPACT_PX,
                        DrawerSpecimen { width_px: COMPACT_PX }
                    }
                    // Wide hosts show the sidebar beside the content, never a drawer: this frame
                    // stays empty on purpose (`KIT-101`).
                    Frame { title: "Drawer (never shown at this width)", width_px: EXPANDED_PX,
                        DrawerSpecimen { width_px: EXPANDED_PX }
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

#[component]
fn SheetSpecimen(width_px: f64) -> Element {
    let width = WidthClass::from_px(width_px);
    let mut open = use_signal(|| true);
    rsx! {
        div {
            class: "p-4",
            button {
                class: "cursor-pointer rounded-md border border-border bg-card px-4 py-2 text-foreground",
                onclick: move |_| open.set(true),
                "Open sheet"
            }
        }
        if open() {
            Sheet { width, name: "Choose a theme", on_dismiss: move |()| open.set(false),
                div {
                    class: "p-4",
                    h3 { class: "m-0 mb-2 text-lg font-semibold", "Choose a theme" }
                    p { class: "text-text-muted", "A sheet belongs to its panel and is dismissed by its scrim or Escape." }
                }
            }
        }
    }
}

#[component]
fn AppBarSpecimen() -> Element {
    rsx! {
        AppBar {
            title: "Settings",
            leading: rsx! { NavButton { mode: NavButtonMode::Sidebar, onclick: |_| {} } },
            trailing: rsx! {
                button {
                    class: "min-h-12 cursor-pointer rounded-md px-3 text-text hover:bg-surface-sunken",
                    "Search"
                }
            },
        }
    }
}

#[component]
fn DrawerSpecimen(width_px: f64) -> Element {
    let width = WidthClass::from_px(width_px);
    let mut open = use_signal(|| true);
    rsx! {
        div {
            class: "p-4",
            button {
                class: "cursor-pointer rounded-md border border-border bg-card px-4 py-2 text-foreground",
                onclick: move |_| open.set(true),
                "Open drawer"
            }
        }
        Drawer { width, name: "Sidebar", open: open(), on_dismiss: move |()| open.set(false),
            nav {
                class: "flex flex-col gap-1 p-2",
                "aria-label": "Specimen sidebar",
                AppBar { title: "Dayframe" }
                for title in ["Home", "Settings", "Layout"] {
                    button {
                        key: "{title}",
                        class: "block min-h-12 w-full cursor-pointer rounded-md px-4 text-left text-text hover:bg-surface-sunken",
                        "{title}"
                    }
                }
            }
        }
    }
}
