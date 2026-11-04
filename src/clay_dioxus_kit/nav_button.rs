//! The Dayframe skin of [`headless::nav_button::NavButton`].
use crate::headless::nav_button::NavButtonMode;
use crate::headless::nav_button::{self};
use dioxus::prelude::*;

const CLASS: &str = "inline-flex size-11 cursor-pointer items-center justify-center rounded-pill text-text hover:bg-surface-sunken focus-visible:outline-2 focus-visible:outline-focus";

/// The back / open-sidebar button a micro mounts in its app bar (FDR 006).
#[component]
pub fn NavButton(mode: NavButtonMode, onclick: EventHandler<()>) -> Element {
    // A chevron for back, a three-line menu for the sidebar. Decorative: the name is on the
    // button.
    let path = match mode {
        NavButtonMode::Back => "M15 18l-6-6 6-6",
        NavButtonMode::Sidebar => "M4 6h16M4 12h16M4 18h16",
    };
    rsx! {
        nav_button::NavButton { mode, onclick, class: CLASS,
            svg {
                class: "size-6",
                view_box: "0 0 24 24",
                fill: "none",
                stroke: "currentColor",
                stroke_width: "2",
                stroke_linecap: "round",
                stroke_linejoin: "round",
                "aria-hidden": "true",
                path { d: path }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::Page;

    fn app(mode: NavButtonMode) -> Element {
        rsx! { NavButton { mode, onclick: |_| {} } }
    }

    #[test]
    fn the_icon_is_decorative_and_the_name_is_the_mode() {
        for mode in [NavButtonMode::Back, NavButtonMode::Sidebar] {
            let page = Page::with_props(app, mode);
            let button = page.by_role("button", mode.label());
            // The glyph must not add an accessible name of its own.
            assert_eq!(page.text(button), "");
        }
    }
}
