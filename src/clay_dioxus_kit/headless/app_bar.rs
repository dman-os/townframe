//! AppBar (`ct-app-bar`): the strip across the top of a panel (FDR 006 "a micro controls its own
//! app bar").
//!
//! Parts: `leading` (a button that belongs in the bar itself; the shell's navigation rack floats over the page instead, see `PanelPage`), `title` and `trailing`
//! (actions); a slot that is absent renders nothing. State: `data-state` is `leading` when the bar
//! has a leading slot and `bare` when it does not, so a stylesheet can inset a title that has no
//! button before it. The bar owns no behaviour beyond arranging them; it exists so panels stop
//! hand-rolling the same flex row around the button, and so every panel's title is the same
//! heading level.
//!
//! The skin puts the bar on the `app-bar` layer (`--ct-layer-app-bar`), so content scrolling
//! beneath it passes under it, and makes it a stacking context of its own.
//!
//! The title is an `h2`: a panel is a region of its own, and its app bar names it. A bar nested
//! in details beside a list therefore reads as a second top-level section, which is what the
//! arrangement is.
use dioxus::prelude::*;

#[component]
pub fn AppBar(title: String, leading: Option<Element>, trailing: Option<Element>) -> Element {
    let state = if leading.is_some() { "leading" } else { "bare" };
    rsx! {
        header { class: "ct-app-bar", "data-state": state,
            if let Some(leading) = leading {
                div { "data-part": "leading", {leading} }
            }
            h2 { "data-part": "title", "{title}" }
            if let Some(trailing) = trailing {
                div { "data-part": "trailing", {trailing} }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::Page;
    use dioxus_storm::Storm;

    fn app(with_slots: bool) -> Element {
        rsx! {
            AppBar {
                title: "Settings",
                leading: with_slots.then(|| rsx! { button { "Back" } }),
                trailing: with_slots.then(|| rsx! { button { "Search" } }),
            }
        }
    }

    #[test]
    fn the_title_is_the_panels_heading_between_its_slots() {
        let page = Page::with_props(app, true);
        assert!(page.has_role("heading", "Settings"));
        assert!(page.has_role("button", "Back"));
        assert!(page.has_role("button", "Search"));
        let outline = page.outline();
        let at = |text: &str| outline.find(text).unwrap_or_else(|| panic!("{outline}"));
        assert!(at("Back") < at("Settings") && at("Settings") < at("Search"));
    }

    #[test]
    fn the_bar_names_its_parts_in_order_and_says_whether_it_has_a_leading_slot() {
        for (with_slots, state, parts) in [
            (true, "leading", vec!["leading", "title", "trailing"]),
            (false, "bare", vec!["title"]),
        ] {
            let page = Page::with_props(app, with_slots);
            let bar = page.by_class("ct-app-bar");
            assert_eq!(page.attr(bar, "data-state").as_deref(), Some(state));
            assert_eq!(page.parts(bar), parts);
            assert_eq!(page.parent(page.by_role("heading", "Settings")), bar);
        }
    }

    #[test]
    fn absent_slots_render_nothing() {
        let page = Page::with_props(app, false);
        assert!(page.has_role("heading", "Settings"));
        assert!(!page.has_role("button", "Back"));
    }

    fn bar() -> Element {
        let mut presses = use_signal(|| 0u32);
        rsx! {
            AppBar {
                title: "Settings",
                leading: Some(rsx! { button { onclick: move |_| presses += 1, "Menu {presses}" } }),
                trailing: Some(rsx! { button { onclick: move |_| presses.set(0), "More" } }),
            }
        }
    }

    #[test]
    fn an_app_bar_with_actions_survives_any_order_of_events() {
        Storm::of(bar).run_seeds(0..16);
    }
}
