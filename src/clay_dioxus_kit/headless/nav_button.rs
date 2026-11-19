//! The sidebar/back button (`ct-nav-button`, FDR 006): one control with two meanings.
//!
//! Parts: `icon` (decorative, `aria-hidden`), which holds the `glyph` of the back mode or the
//! three `bar`s of the sidebar mode. State: `data-state` (`back`|`sidebar`), the mode. The glyphs
//! are drawn by the stylesheet from boxes in `currentColor`; the accessible name is the mode's
//! label, so the icon adds none.
//!
//! # Renderer limits
//!
//! * Blitz has no keyboard focus today, so the button is reachable by pointer only there. It is a
//!   native `<button>` and sets no `tabindex`, which would panic Blitz (`KIT-052`); a renderer
//!   that has focus makes it keyboard-activatable with no change here.
use dioxus::prelude::*;

/// What the button does, which decides its accessible name.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum NavButtonMode {
    /// There is somewhere to go back to in the current context.
    Back,
    /// There is nothing to go back to: the button opens the sidebar.
    Sidebar,
}

impl NavButtonMode {
    /// The accessible name. Fixed per mode so every micro that mounts the button reads the same
    /// to assistive technology and to tests (`KIT-050`).
    pub fn label(self) -> &'static str {
        match self {
            Self::Back => "Back",
            Self::Sidebar => "Open sidebar",
        }
    }

    fn state(self) -> &'static str {
        match self {
            Self::Back => "back",
            Self::Sidebar => "sidebar",
        }
    }
}

/// A native `<button>`: focusable and activatable by keyboard wherever the renderer has focus,
/// and clickable everywhere (`KIT-052`; Blitz has no keyboard focus today).
#[component]
pub fn NavButton(mode: NavButtonMode, onclick: EventHandler<()>) -> Element {
    rsx! {
        button {
            class: "ct-nav-button",
            r#type: "button",
            "aria-label": mode.label(),
            "data-state": mode.state(),
            onclick: move |_| onclick.call(()),
            span { "data-part": "icon", "aria-hidden": "true",
                match mode {
                    NavButtonMode::Back => rsx! {
                        span { "data-part": "glyph" }
                    },
                    NavButtonMode::Sidebar => rsx! {
                        for index in 0..3 {
                            span { key: "{index}", "data-part": "bar" }
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
    use dioxus_storm::Storm;
    use std::cell::RefCell;
    use std::rc::Rc;

    #[derive(Clone)]
    struct Probe {
        mode: NavButtonMode,
        clicks: Rc<RefCell<u32>>,
    }

    fn app(probe: Probe) -> Element {
        let clicks = probe.clicks;
        rsx! {
            NavButton {
                mode: probe.mode,
                onclick: move |()| *clicks.borrow_mut() += 1,
            }
        }
    }

    fn render(mode: NavButtonMode) -> (Page, Rc<RefCell<u32>>) {
        let clicks = Rc::new(RefCell::new(0));
        let probe = Probe {
            mode,
            clicks: Rc::clone(&clicks),
        };
        (Page::with_props(app, probe), clicks)
    }

    #[test]
    fn each_mode_is_named_for_what_it_does() {
        let (page, _) = render(NavButtonMode::Back);
        assert!(page.has_role("button", "Back"));
        assert!(!page.has_role("button", "Open sidebar"));
        let (page, _) = render(NavButtonMode::Sidebar);
        assert!(page.has_role("button", "Open sidebar"));
    }

    #[test]
    fn the_button_names_its_vocabulary_and_draws_its_glyph_from_parts() {
        for (mode, state, glyph) in [
            (NavButtonMode::Back, "back", vec!["glyph"]),
            (NavButtonMode::Sidebar, "sidebar", vec!["bar", "bar", "bar"]),
        ] {
            let (page, _) = render(mode);
            let button = page.by_class("ct-nav-button");
            assert_eq!(page.attr(button, "type").as_deref(), Some("button"));
            assert_eq!(page.attr(button, "data-state").as_deref(), Some(state));
            assert_eq!(page.parts(button), ["icon"]);
            let icon = page.children(button)[0];
            assert_eq!(page.attr(icon, "aria-hidden").as_deref(), Some("true"));
            assert_eq!(page.parts(icon), glyph);
            // The glyph adds no accessible name of its own.
            assert_eq!(page.text(button), "");
        }
    }

    #[test]
    fn click_reports_once_per_press() {
        let (mut page, clicks) = render(NavButtonMode::Sidebar);
        let button = page.by_role("button", "Open sidebar");
        page.click(button);
        page.click(button);
        assert_eq!(*clicks.borrow(), 2);
    }

    fn owner() -> Element {
        let mut mode = use_signal(|| NavButtonMode::Sidebar);
        rsx! {
            NavButton {
                mode: mode(),
                onclick: move |()| {
                    mode.set(match mode() {
                        NavButtonMode::Back => NavButtonMode::Sidebar,
                        NavButtonMode::Sidebar => NavButtonMode::Back,
                    });
                },
            }
        }
    }

    #[test]
    fn a_nav_button_survives_any_order_of_events() {
        Storm::of(owner).run_seeds(0..16);
    }
}
