use crate::foundations::FoundationSection;
use crate::specimens::Row;
use clay_dioxus_kit::Button;
use clay_dioxus_kit::ButtonSize;
use clay_dioxus_kit::ButtonVariant;
use dioxus::prelude::*;

/// `Button`: every variant at every size, the states a static page can show, and a toggle.
/// Hover, press and focus are pseudo-classes: point at the cells and press Tab.
#[component]
pub fn ButtonSpecimens() -> Element {
    let mut bold = use_signal(|| false);
    let mut italic = use_signal(|| true);
    rsx! {
        FoundationSection {
            id: "button-variants".to_string(),
            title: "Variants and sizes".to_string(),
            description: "data-variant and data-size. Hover, press and Tab onto a cell to see :hover, :active and :focus-visible.".to_string(),
            div { class: "grid gap-3",
                for variant in ButtonVariant::ALL {
                    Row { key: "{variant.as_str()}", label: variant.as_str().to_string(),
                        for size in ButtonSize::ALL {
                            Button {
                                key: "{size.as_str()}",
                                variant,
                                size,
                                onclick: |()| {},
                                "{variant.as_str()} {size.as_str()}"
                            }
                        }
                    }
                }
            }
        }
        FoundationSection {
            id: "button-states".to_string(),
            title: "States".to_string(),
            description: "data-disabled, and an accessible name for a button whose content is an icon.".to_string(),
            div { class: "grid gap-3",
                Row { label: "disabled".to_string(),
                    for variant in ButtonVariant::ALL {
                        Button { key: "{variant.as_str()}", variant, disabled: true, onclick: |()| {}, "{variant.as_str()}" }
                    }
                }
                Row { label: "icon only".to_string(),
                    Button {
                        variant: ButtonVariant::Ghost,
                        label: Some("Add".to_string()),
                        onclick: |()| {},
                        "+"
                    }
                    Button {
                        variant: ButtonVariant::Outline,
                        label: Some("Add".to_string()),
                        onclick: |()| {},
                        "+"
                    }
                }
            }
        }
        FoundationSection {
            id: "button-label-styles".to_string(),
            title: "Label styles".to_string(),
            description: "Emphasis inside a label: strong and em (a toggle's own state is data-state, not the label's weight).".to_string(),
            div { class: "grid gap-3",
                Row { label: "emphasis".to_string(),
                    for variant in [ButtonVariant::Primary, ButtonVariant::Outline] {
                        Button { key: "{variant.as_str()}", variant, onclick: |()| {},
                            strong { "Bold" }
                            " "
                            em { "italic" }
                            " "
                            strong { em { "both" } }
                        }
                    }
                }
            }
        }
        FoundationSection {
            id: "button-toggle".to_string(),
            title: "Toggle".to_string(),
            description: "pressed makes a button a toggle: aria-pressed and data-state (on|off). The caller owns the state.".to_string(),
            div { class: "grid gap-3",
                Row { label: "toggle".to_string(),
                    Button {
                        variant: ButtonVariant::Outline,
                        pressed: Some(bold()),
                        onclick: move |()| bold.set(!bold()),
                        "Bold"
                    }
                    Button {
                        variant: ButtonVariant::Outline,
                        pressed: Some(italic()),
                        onclick: move |()| italic.set(!italic()),
                        "Italic"
                    }
                }
            }
        }
    }
}
