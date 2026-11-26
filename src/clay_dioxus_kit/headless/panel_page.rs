//! PanelPage (`ct-panel-page`): a panel's page, its app bar pinned over a body that scrolls.
//!
//! Parts: `bar` (holds the caller's app bar) and `body`. A micro's presentation root is exactly
//! its pane's size and clips what overflows it (the Clay mount contract), so a body taller than
//! the panel scrolls here or nowhere: the stylesheet makes the page fill the root, keeps the bar
//! out of the scroll and gives the body the height the bar leaves.
use dioxus::prelude::*;

/// The page of a panel micro: `app_bar` pinned at the top and `children` in a body that scrolls
/// within the panel.
#[component]
pub fn PanelPage(app_bar: Element, children: Element) -> Element {
    rsx! {
        div { class: "ct-panel-page",
            div { "part": "panel-page-bar", {app_bar} }
            div { "part": "panel-page-body", {children} }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::Page;

    fn app(_: ()) -> Element {
        rsx! {
            PanelPage { app_bar: rsx! { h1 { "Title" } },
                p { role: "status", "Body" }
            }
        }
    }

    #[test]
    fn the_bar_sits_beside_the_scrolling_body_not_inside_it() {
        let page = Page::with_props(app, ());
        let root = page.by_class("ct-panel-page");
        assert_eq!(page.parts(root), ["panel-page-bar", "panel-page-body"]);
        let [bar, body] = page.children(root)[..] else {
            panic!("the page is its bar and its body");
        };
        assert_eq!(page.parent(page.by_role("heading", "Title")), bar);
        assert_eq!(page.parent(page.by_role("status", "Body")), body);
    }
}
