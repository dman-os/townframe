use crate::foundations::FoundationSection;
use crate::specimens::Frame;
use crate::specimens::COMPACT_PX;
use crate::specimens::EXPANDED_PX;
use clay_dioxus_kit::SectionPane;
use clay_dioxus_kit::Sections;
use clay_dioxus_kit::SizeRule;
use clay_dioxus_kit::Snap;
use clay_dioxus_kit::Split;
use dioxus::prelude::*;

/// `Sections`: a resizable run of panes, two at a compact width and three at an expanded one. The
/// first pane has the Kotlin sidebar's rule (an 80px rail, or at least 240px, never in between).
#[component]
pub fn SectionsSpecimens() -> Element {
    rsx! {
        FoundationSection {
            id: "sections-widths".to_string(),
            title: "Resizable panes".to_string(),
            description: "part sections-pane, sections-handle (with a sections-grip) and sections-gesture; --ct-sections-weight, -min and -max arrive as custom properties on a pane. Drag a handle, or focus it and use the arrow keys.".to_string(),
            div { class: "flex flex-wrap items-start gap-8",
                Frame { title: "Sections".to_string(), width_px: COMPACT_PX,
                    SectionsSpecimen { width_px: COMPACT_PX, three: false }
                }
                Frame { title: "Sections".to_string(), width_px: EXPANDED_PX,
                    SectionsSpecimen { width_px: EXPANDED_PX, three: true }
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
