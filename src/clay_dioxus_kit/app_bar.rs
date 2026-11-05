//! The Dayframe skin of [`headless::app_bar::AppBar`].
use crate::headless::app_bar::AppBarSkin;
use crate::headless::app_bar::{self};
use dioxus::prelude::*;

// Metrics (`KIT-105`): a 56 px bar holding a 48 px nav button, 8 px from the edge. The title follows
// the leading slot at 8 px; with no leading slot it is inset to 16 px, the page's own gutter.
// It is one line and ends in an ellipsis rather than growing the bar.
const SKIN: AppBarSkin = AppBarSkin {
    root: "h-14 shrink-0 gap-2 px-2 text-text data-[state=bare]:pl-4",
    leading: "flex items-center",
    title: "m-0 truncate text-lg font-semibold",
    trailing: "flex items-center gap-1",
};

/// The panel's top strip: leading slot (the sidebar/back button), title, trailing actions.
#[component]
pub fn AppBar(title: String, leading: Option<Element>, trailing: Option<Element>) -> Element {
    rsx! {
        app_bar::AppBar { title, leading, trailing, skin: SKIN }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::Page;

    fn app(title: &'static str) -> Element {
        rsx! { AppBar { title } }
    }

    #[test]
    fn the_skin_fixes_the_bar_height_and_truncates_the_title() {
        let page = Page::with_props(app, "Home");
        let heading = page.by_role("heading", "Home");
        let bar = page.attr(page.parent(heading), "class").unwrap();
        assert!(bar.split(' ').any(|class| class == "h-14"), "{bar}");
        assert!(page.attr(heading, "class").unwrap().contains("truncate"));
    }

    #[test]
    fn the_skin_styles_the_title() {
        let page = Page::with_props(app, "Home");
        let heading = page.by_role("heading", "Home");
        assert!(
            page.attr(heading, "class")
                .unwrap()
                .contains("font-semibold")
        );
    }
}
