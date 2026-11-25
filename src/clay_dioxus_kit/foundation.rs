use dioxus::prelude::*;

/// Not minified: the Dioxus CLI minifies a bundled stylesheet with lightningcss at the `defaults`
/// browserslist, which lowers every `light-dark()` to the `var(--lightningcss-light, a)
/// var(--lightningcss-dark, b)` toggle pair. The toggles are decided once, on `:root`, by
/// `prefers-color-scheme`, so the palette arrives in every subtree already resolved and an element's
/// own `color-scheme` (the theme lab canvas's Scheme toggle, ADR 055 section 4.2) cannot flip it.
/// The browsers the kit targets (WebKitGTK, Android WebView, evergreen web) have `light-dark()`.
const TAILWIND_CSS: Asset = asset!(
    "/assets/tailwind.css",
    AssetOptions::css().with_minify(false)
);

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

/// Loads the generated Material variables and claytxt's semantic aliases.
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
            class: "ct-theme theme-{family} {mode.class_name()}",
            {children}
        }
    }
}
