//! The Tokens and Colors tabs: one section per design-language foundation (typography, spacing,
//! shape, surfaces, states, colour), each rendered from the live tokens.

use super::*;
use clay_dioxus_kit::Card;
use clay_dioxus_kit::CardContent;
use clay_dioxus_kit::CardHeader;

#[component]
pub(super) fn TokenFoundations() -> Element {
    rsx! {
                FoundationSection {
                    id: "typography".to_string(),
                    title: "Typography".to_string(),
                    description: "Type tokens establish hierarchy, rhythm, and readable long-form content before components add decoration.".to_string(),
                    TypographyFoundation {}
                }
                FoundationSection {
                    id: "spacing".to_string(),
                    title: "Spacing grid".to_string(),
                    description: "Spacing uses Tailwind's native utilities, driven by the single claytxt spacing unit.",
                    SpacingFoundation {}
                }
                FoundationSection {
                    id: "shape".to_string(),
                    title: "Shape & borders".to_string(),
                    description: "Radius and border tokens provide the shared geometry for controls, cards, surfaces, and focused states.".to_string(),
                    ShapeFoundation {}
                }
                FoundationSection {
                    id: "surfaces".to_string(),
                    title: "Surfaces & elevation".to_string(),
                    description: "Surface roles and shadows describe how content separates from the canvas without relying on arbitrary colors.".to_string(),
                    SurfaceFoundation {}
                }
                FoundationSection {
                    id: "states".to_string(),
                    title: "Interaction states".to_string(),
                    description: "States are shown as deliberate specimens so themes can be evaluated for focus, selection, feedback, and disabled legibility.".to_string(),
                    StateFoundation {}
                }
    }
}

#[component]
pub(super) fn FoundationSection(
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
        (
            "canvas",
            "var(--ct-color-canvas)",
            "var(--ct-shadow-none)",
            "bg-background",
        ),
        (
            "surface",
            "var(--ct-color-surface)",
            "var(--ct-shadow-none)",
            "bg-card",
        ),
        (
            "sunken",
            "var(--ct-color-surface-sunken)",
            "var(--ct-shadow-none)",
            "bg-surface-sunken",
        ),
    ];
    let shadows = [
        ("none", "var(--ct-shadow-none)", "shadow-none"),
        ("sm", "var(--ct-shadow-sm)", "shadow-sm"),
        ("md", "var(--ct-shadow-md)", "shadow-md"),
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
                                "shadow-{name} · {utility}"
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

#[component]
pub(super) fn ColorFoundation(theme: MaterialTheme) -> Element {
    rsx! {
        h3 {
            class: "mb-3 text-lg",
            "Palettes"
        }
        for (palette_name, colors) in theme.palettes {
            section {
                key: "palette-{palette_name}",
                class: "mb-8",
                h4 {
                    class: "mb-3 text-base font-medium",
                    "{palette_name}"
                }
                div {
                    class: "grid grid-cols-[repeat(auto-fill,minmax(11.25rem,1fr))] gap-2",
                    for (name, value) in colors {
                        ColorSwatch {
                            key: "{palette_name}-{name}",
                            name,
                            value,
                            rounded: false,
                        }
                    }
                }
            }
        }
        h3 {
            class: "mt-12 mb-6 text-lg",
            "Schemes"
        }
        for (scheme_name, colors) in theme.schemes {
            section {
                key: "scheme-{scheme_name}",
                class: "mb-8",
                h4 {
                    class: "mb-3 text-base font-medium",
                    "{scheme_name}"
                }
                div {
                class: "grid grid-cols-[repeat(auto-fill,minmax(11.25rem,1fr))] gap-3",
                    for (name, value) in colors {
                        ColorSwatch {
                            key: "{scheme_name}-{name}",
                            name,
                            value,
                            rounded: true,
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn ColorSwatch(name: String, value: String, rounded: bool) -> Element {
    let foreground = readable_text_color(&value);
    let radius_class = if rounded { "rounded-lg" } else { "rounded-sm" };

    rsx! {
        div {
            class: "min-h-18 box-border rounded-md p-3 {radius_class}",
            style: "background:{value}; color:{foreground};",
            div { class: "font-medium", "{name}" }
            div { class: "mt-1 text-sm opacity-80", "{value}" }
        }
    }
}

fn readable_text_color(value: &str) -> &'static str {
    let hex = value.strip_prefix('#').unwrap_or(value);
    if hex.len() != 6 {
        return "#111827";
    }

    let red = u8::from_str_radix(&hex[0..2], 16).expect("Material colors must be hex values");
    let green = u8::from_str_radix(&hex[2..4], 16).expect("Material colors must be hex values");
    let blue = u8::from_str_radix(&hex[4..6], 16).expect("Material colors must be hex values");
    let luminance =
        (0.299 * f64::from(red) + 0.587 * f64::from(green) + 0.114 * f64::from(blue)) / 255.0;

    if luminance > 0.58 {
        "#111827"
    } else {
        "#ffffff"
    }
}
