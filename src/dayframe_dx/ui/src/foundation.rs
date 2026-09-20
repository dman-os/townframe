use dioxus::prelude::*;

const TAILWIND_CSS: Asset = asset!("/assets/tailwind.css");

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ThemeMode {
    Light,
    Dark,
}

impl ThemeMode {
    pub fn class_name(self) -> &'static str {
        match self {
            Self::Light => "light",
            Self::Dark => "dark",
        }
    }
}

/// Loads the generated Material variables and Dayframe's semantic aliases.
#[component]
pub fn FoundationStyles() -> Element {
    rsx! {
        document::Link { rel: "stylesheet", href: TAILWIND_CSS }
    }
}

/// Injects a user or subtree theme stylesheet without changing the token contract.
#[component]
pub fn ThemeStyles(css: String) -> Element {
    rsx! {
        document::Style { "{css}" }
    }
}

/// Establishes the inheritance boundary for a theme family and mode.
#[component]
pub fn ThemeRoot(family: String, mode: ThemeMode, children: Element) -> Element {
    rsx! {
        FoundationStyles {}
        div {
            class: "df-theme theme-{family} {mode.class_name()}",
            {children}
        }
    }
}
