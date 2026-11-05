//! The Dayframe skin of [`headless::app_bar::AppBar`].
use crate::headless::app_bar::AppBarSkin;
use crate::headless::app_bar::{self};
use dioxus::prelude::*;

const SKIN: AppBarSkin = AppBarSkin {
    root: "min-h-11 gap-2 p-2 text-text",
    leading: "flex items-center",
    title: "m-0 text-lg font-semibold",
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
    fn the_skin_styles_the_title() {
        let page = Page::with_props(app, "Home");
        let heading = page.by_role("heading", "Home");
        assert!(page.attr(heading, "class").unwrap().contains("font-semibold"));
    }
}
