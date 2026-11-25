//! The Tokens and Colors tabs: one section per design-language foundation (typography, spacing,
//! shape, surfaces, states, colour), each rendered from the live tokens.

use clay_dioxus_kit::Card;
use clay_dioxus_kit::CardContent;
use clay_dioxus_kit::CardHeader;
use dioxus::prelude::*;
use std::collections::BTreeMap;
use std::rc::Rc;

/// One design-language foundation, shown as one section of live tokens.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Foundation {
    Colour,
    Typography,
    Spacing,
    Shape,
    Surfaces,
    States,
}

impl Foundation {
    pub const ALL: [Foundation; 6] = [
        Self::Colour,
        Self::Typography,
        Self::Spacing,
        Self::Shape,
        Self::Surfaces,
        Self::States,
    ];

    pub fn title(self) -> &'static str {
        match self {
            Self::Colour => "Colour roles",
            Self::Typography => "Typography",
            Self::Spacing => "Spacing grid",
            Self::Shape => "Shape & borders",
            Self::Surfaces => "Surfaces & elevation",
            Self::States => "Interaction states",
        }
    }

    fn id(self) -> &'static str {
        match self {
            Self::Colour => "colour",
            Self::Typography => "typography",
            Self::Spacing => "spacing",
            Self::Shape => "shape",
            Self::Surfaces => "surfaces",
            Self::States => "states",
        }
    }

    fn description(self) -> &'static str {
        match self {
            Self::Colour => {
                "Every semantic colour role, painted from its live custom property: a sheet that \
                 sets a role changes its swatch here."
            }
            Self::Typography => {
                "Type tokens establish hierarchy, rhythm, and readable long-form content before \
                 components add decoration."
            }
            Self::Spacing => {
                "Spacing uses Tailwind's native utilities, driven by the single claytxt spacing unit."
            }
            Self::Shape => {
                "Radius and border tokens provide the shared geometry for controls, cards, surfaces, \
                 and focused states."
            }
            Self::Surfaces => {
                "Surface roles and shadows describe how content separates from the canvas without \
                 relying on arbitrary colors."
            }
            Self::States => {
                "States are shown as deliberate specimens so themes can be evaluated for focus, \
                 selection, feedback, and disabled legibility."
            }
        }
    }
}

/// One foundation's section.
#[component]
pub fn FoundationPage(foundation: Foundation) -> Element {
    rsx! {
        FoundationSection {
            id: foundation.id().to_owned(),
            title: foundation.title().to_owned(),
            description: foundation.description().to_owned(),
            match foundation {
                Foundation::Colour => rsx! { ColorFoundation {} },
                Foundation::Typography => rsx! { TypographyFoundation {} },
                Foundation::Spacing => rsx! { SpacingFoundation {} },
                Foundation::Shape => rsx! { ShapeFoundation {} },
                Foundation::Surfaces => rsx! { SurfaceFoundation {} },
                Foundation::States => rsx! { StateFoundation {} },
            }
        }
    }
}

/// Every foundation but colour, in order: the showcase's Tokens tab (its Colors tab shows the
/// Material scheme instead).
#[component]
pub fn TokenFoundations() -> Element {
    rsx! {
        for foundation in Foundation::ALL.into_iter().filter(|foundation| *foundation != Foundation::Colour) {
            FoundationPage { key: "{foundation.id()}", foundation }
        }
    }
}

#[component]
pub fn FoundationSection(
    id: String,
    title: String,
    description: String,
    children: Element,
) -> Element {
    let mut anchors = use_context::<Signal<BTreeMap<String, Rc<MountedData>>>>();
    let mount_id = id.clone();
    rsx! {
        section {
            id: "{id}",
            onmounted: move |event| {
                anchors.write().insert(mount_id.clone(), event.data());
            },
            class: "scroll-mt-18 py-8",
            h2 {
                class: "mb-2 text-2xl font-bold leading-tight tracking-tight",
                "{title}"
            }
            p {
                class: "mb-6 max-w-3xl text-foreground-muted leading-normal",
                "{description}"
            }
            {children}
        }
    }
}

#[component]
fn TypographyFoundation() -> Element {
    let specimens = [
        ("Display", "text-4xl font-bold leading-tight tracking-tight"),
        ("Title", "text-2xl font-bold leading-tight tracking-tight"),
        ("Large body", "text-lg font-normal leading-snug"),
        ("Body", "text-base font-normal leading-normal"),
        (
            "Small label",
            "text-sm font-medium leading-snug tracking-wide",
        ),
    ];

    rsx! {
        div {
            class: "grid grid-cols-[repeat(auto-fit,minmax(15rem,1fr))] gap-3",
                    for (name, utility) in specimens {
                Card {
                    CardHeader { strong { "{name}" } }
                    CardContent {
                        p {
                            class: "{utility}",
                            "Aa — claytxt foundations"
                        }
                        code {
                            class: "mt-3 block text-sm text-foreground-muted",
                            "tw: {utility}"
                        }
                    }
                }
            }
        }
        div {
            class: "mt-4",
            Card {
            CardHeader { strong { "Readable paragraph" } }
            CardContent {
                p {
                    class: "max-w-prose leading-relaxed",
                    "A theme is successful when the hierarchy remains clear before color is applied. This paragraph intentionally demonstrates a comfortable reading measure, relaxed line height, and muted supporting text."
                }
            }
        }
        }
    }
}

#[component]
fn SpacingFoundation() -> Element {
    let spaces = [
        ("space-1", "0.25rem", "p-1"),
        ("space-2", "0.5rem", "p-2"),
        ("space-3", "0.75rem", "p-3"),
        ("space-4", "1rem", "p-4"),
        ("space-5", "1.25rem", "p-5"),
        ("space-6", "1.5rem", "p-6"),
        ("space-8", "2rem", "p-8"),
        ("space-9", "2.25rem", "p-9"),
        ("space-12", "3rem", "p-12"),
    ];

    rsx! {
        Card {
            CardHeader { strong { "Tailwind spacing utilities" } }
            CardContent {
                div {
                class: "grid gap-3",
                    for (_, value, utility) in spaces {
                        div {
                            class: "grid grid-cols-[6rem_1fr_10rem] items-center gap-3",
                            code { "{utility}" }
                            div {
                                class: "h-2 max-w-full rounded-full bg-primary",
                                style: "width:{value};",
                            }
                            small { class: "text-foreground-muted", "{value} · {utility}" }
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn ShapeFoundation() -> Element {
    let radii = [
        ("sm", "var(--ct-radius-sm)", "rounded-sm"),
        ("md", "var(--ct-radius-md)", "rounded-md"),
        ("lg", "var(--ct-radius-lg)", "rounded-lg"),
        ("xl", "var(--ct-radius-xl)", "rounded-xl"),
        ("pill", "var(--ct-radius-pill)", "rounded-full"),
    ];
    let borders = [
        (
            "hairline",
            "var(--ct-border-width-hairline)",
            "border-hairline",
        ),
        ("thin", "var(--ct-border-width-thin)", "border-thin"),
        ("thick", "var(--ct-border-width-thick)", "border-thick"),
    ];

    rsx! {
        div {
            class: "grid grid-cols-[repeat(auto-fit,minmax(14rem,1fr))] gap-3",
            Card {
                CardHeader { strong { "Radius" } }
                CardContent {
                    div { class: "flex flex-wrap gap-3",
                        for (name, _radius, utility) in radii {
                            div {
                                class: "grid size-24 min-h-18 place-items-center rounded border border-border bg-card p-3 text-center text-sm {utility}",
                                "{name} · {utility}"
                            }
                        }
                    }
                }
            }
            Card {
                CardHeader { strong { "Border widths" } }
                CardContent {
                    div { class: "grid gap-3",
                        for (name, width, utility) in borders {
                            div {
                                class: "rounded-sm p-3",
                                style: "border:{width} solid var(--ct-color-primary);",
                                "{name} — {width} · {utility}"
                            }
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn SurfaceFoundation() -> Element {
    let surfaces = [
        ("canvas", "var(--ct-color-canvas)", "none", "bg-background"),
        ("surface", "var(--ct-color-surface)", "none", "bg-card"),
        (
            "sunken",
            "var(--ct-color-surface-sunken)",
            "none",
            "bg-surface-sunken",
        ),
    ];
    let shadows = [
        ("none", "none", "in the flow: nothing casts"),
        (
            "float",
            "var(--ct-elevation-float)",
            "popover, menu, scrub bubble",
        ),
        ("anchored", "var(--ct-elevation-anchored)", "sheet, drawer"),
    ];

    rsx! {
        div {
            class: "grid grid-cols-[repeat(auto-fit,minmax(14rem,1fr))] gap-3",
            Card {
                CardHeader { strong { "Surface roles" } }
                CardContent {
                    div { class: "grid gap-3",
                        for (name, background, shadow, utility) in surfaces {
                            div {
                                class: "flex min-h-16 items-end rounded-md p-3",
                                style: "background:{background}; box-shadow:{shadow};",
                                "{name} · {utility}"
                            }
                        }
                    }
                }
            }
            Card {
                CardHeader { strong { "Elevation" } }
                CardContent {
                    div { class: "grid gap-4 rounded-md bg-surface-sunken p-3",
                        for (name, shadow, utility) in shadows {
                            div {
                                class: "rounded-md bg-card p-4",
                                style: "box-shadow:{shadow};",
                                "{name} · {utility}"
                            }
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn StateFoundation() -> Element {
    rsx! {
        div {
            class: "grid grid-cols-[repeat(auto-fit,minmax(14rem,1fr))] gap-3",
            Card {
                CardHeader { strong { "State surfaces" } }
                CardContent {
                    div { class: "grid gap-2",
                        StateRow { name: "Default", detail: "Resting surface", decoration: "background:var(--ct-color-surface);" }
                        StateRow { name: "Hover", detail: "Tonal response", decoration: "background:var(--ct-color-surface-sunken);" }
                        StateRow { name: "Focus", detail: "Visible keyboard ring", decoration: "background:var(--ct-color-surface); outline:var(--ct-border-width-thick) solid var(--ct-color-focus); outline-offset:2px;" }
                        StateRow { name: "Selected", detail: "Primary emphasis", decoration: "background:var(--ct-color-primary); color:var(--ct-color-on-primary);" }
                        StateRow { name: "Disabled", detail: "Reduced emphasis", decoration: "background:var(--ct-color-surface); opacity:0.38;" }
                        StateRow { name: "Error", detail: "Actionable feedback", decoration: "border-color:var(--ct-color-danger); background:var(--ct-color-surface);" }
                    }
                }
            }
        }
    }
}

#[component]
fn StateRow(name: &'static str, detail: &'static str, decoration: &'static str) -> Element {
    rsx! {
        div {
            class: "flex items-center justify-between gap-3 rounded-md border border-border p-3",
            style: "{decoration}",
            strong { "{name}" }
            small { style: "opacity:0.8;", "{detail}" }
        }
    }
}

/// The colour roles, in the order of the `roles` table in `x/design-tokens.ts` that derives them
/// from the palette. A role named `on-X` is the content colour drawn over `X`.
const SEMANTIC_COLORS: [&str; 35] = [
    "canvas",
    "surface-sunken",
    "surface",
    "surface-raised",
    "text",
    "text-muted",
    "text-subtle",
    "border",
    "border-strong",
    "focus",
    "primary",
    "on-primary",
    "primary-container",
    "on-primary-container",
    "accent",
    "on-accent",
    "accent-container",
    "on-accent-container",
    "danger",
    "on-danger",
    "danger-container",
    "on-danger-container",
    "success",
    "on-success",
    "success-container",
    "on-success-container",
    "warning",
    "on-warning",
    "warning-container",
    "on-warning-container",
    "info",
    "on-info",
    "info-container",
    "on-info-container",
    "scrim",
];

/// One swatch per semantic colour token, painted with the live `var(--ct-color-*)` so a theme
/// stylesheet changes what it shows. A swatch's text is its `on-` pair when the token has one,
/// the paired base when the token is itself an `on-` token, and `--ct-color-text` otherwise.
#[component]
pub fn ColorFoundation() -> Element {
    rsx! {
        div {
            class: "grid grid-cols-[repeat(auto-fill,minmax(11.25rem,1fr))] gap-3",
            for name in SEMANTIC_COLORS {
                ColorSwatch { key: "{name}", name }
            }
        }
    }
}

/// One colour role: a chip painted with the role, and its name and property beneath on the
/// surface, so every role reads the same way whatever its colour. A role with an `on-` pair shows
/// the pair as an "Aa" sample on the chip, which is how the two are meant to be used.
#[component]
fn ColorSwatch(name: &'static str) -> Element {
    let on = format!("on-{name}");
    let paired = SEMANTIC_COLORS.contains(&on.as_str());
    rsx! {
        figure { class: "m-0 flex flex-col gap-2",
            // Geometry inline with the colour: these utilities are not all in the kit's generated
            // Tailwind, and the chip must keep its size whether or not it carries a sample.
            div {
                style: format!(
                    "box-sizing:border-box; display:flex; align-items:center; justify-content:center; \
                     height:4rem; border:var(--ct-dimension-border-width-thin) solid var(--ct-color-border); \
                     border-radius:var(--ct-dimension-radius-lg); font-size:var(--ct-dimension-font-size-lg); \
                     background:var(--ct-color-{name});{}",
                    if paired { format!(" color:var(--ct-color-{on});") } else { String::new() },
                ),
                if paired {
                    "Aa"
                }
            }
            figcaption {
                div { class: "font-medium text-foreground", "{name}" }
                code { class: "block text-sm text-foreground-muted", "--ct-color-{name}" }
            }
        }
    }
}
