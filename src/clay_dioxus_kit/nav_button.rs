//! The Dayframe skin of [`headless::nav_button::NavButton`].
use crate::headless::nav_button::NavButtonMode;
use crate::headless::nav_button::{self};
use dioxus::prelude::*;

const CLASS: &str = "inline-flex size-11 cursor-pointer items-center justify-center rounded-pill text-text hover:bg-surface-sunken focus-visible:outline-2 focus-visible:outline-focus";

/// The back / open-sidebar button a micro mounts in its app bar (FDR 006).
#[component]
pub fn NavButton(mode: NavButtonMode, onclick: EventHandler<()>) -> Element {
    // The glyph is drawn from boxes in `currentColor`, which are plain elements in every
    // renderer; inline `svg` is in the guest vocabulary but was not tried on Blitz. Decorative:
    // the name is on the button.
    rsx! {
        nav_button::NavButton { mode, onclick, class: CLASS,
            span { class: "flex size-6 items-center justify-center", "aria-hidden": "true",
                match mode {
                    // A chevron: two borders of a square turned 45 degrees.
                    NavButtonMode::Back => rsx! {
                        span { class: "size-3 rotate-45 border-b-2 border-l-2 border-current" }
                    },
                    // The menu glyph: three bars.
                    NavButtonMode::Sidebar => rsx! {
                        span { class: "flex flex-col gap-1",
                            span { class: "h-0.5 w-5 bg-current" }
                            span { class: "h-0.5 w-5 bg-current" }
                            span { class: "h-0.5 w-5 bg-current" }
                        }
                    },
                }
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
