//! The Colors tab: the Material baseline palettes and schemes as hex values, from
//! `material-theme.json`. A showcase-only view; the library's `ColorFoundation` shows the live
//! `--ct-color-*` tokens instead.

use super::MaterialTheme;
use dioxus::prelude::*;

#[component]
pub(crate) fn ColorFoundation(theme: MaterialTheme) -> Element {
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
