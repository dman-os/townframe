//! AppBar: the strip across the top of a panel (FDR 006 "a micro controls its own app bar").
//!
//! A leading slot (conventionally the shell's sidebar/back button), the panel's title and
//! trailing actions. The bar owns no behaviour beyond arranging them; it exists so panels stop
//! hand-rolling the same flex row around the button, and so every panel's title is the same
//! heading level.
//!
//! The bar sits on [`Layer::AppBar`] so content scrolling beneath it passes under it, and is a
//! stacking context of its own. `data-state` is `leading` when it has a leading slot and `bare`
//! when it does not, so a skin can inset a title that has no button before it.
//!
//! The title is an `h2`: a panel is a region of its own, and its app bar names it. A bar nested
//! in details beside a list therefore reads as a second top-level section, which is what the
//! arrangement is.
use crate::Layer;
use dioxus::prelude::*;

/// The classes a skin gives each part of [`AppBar`].
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct AppBarSkin {
    pub root: &'static str,
    pub leading: &'static str,
    pub title: &'static str,
    pub trailing: &'static str,
}

#[component]
pub fn AppBar(
    title: String,
    leading: Option<Element>,
    trailing: Option<Element>,
    #[props(default)] skin: AppBarSkin,
) -> Element {
    let state = if leading.is_some() { "leading" } else { "bare" };
    let style = format!(
        "display:flex;align-items:center;min-width:0;position:relative;z-index:{}",
        Layer::AppBar.css()
    );
    rsx! {
        header { class: skin.root, "data-state": state, style,
            if let Some(leading) = leading {
                div { class: skin.leading, style: "flex:none", {leading} }
            }
            h2 { class: skin.title, style: "flex:1 1 0;min-width:0", "{title}" }
            if let Some(trailing) = trailing {
                div { class: skin.trailing, style: "flex:none", {trailing} }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::Page;

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
    fn the_bar_is_on_the_app_bar_layer_and_says_whether_it_has_a_leading_slot() {
        for (with_slots, state) in [(true, "leading"), (false, "bare")] {
            let page = Page::with_props(app, with_slots);
            let heading = page.by_role("heading", "Settings");
            let bar = page.parent(heading);
            assert_eq!(page.attr(bar, "data-state").as_deref(), Some(state));
            assert_eq!(
                page.style(bar, "z-index").as_deref(),
                Some(Layer::AppBar.css())
            );
        }
    }

    #[test]
    fn absent_slots_render_nothing() {
        let page = Page::with_props(app, false);
        assert!(page.has_role("heading", "Settings"));
        assert!(!page.has_role("button", "Back"));
    }
}
