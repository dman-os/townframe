//! PanelPage (`ct-panel-page`): a panel's page, its app bar pinned over a body that scrolls, and
//! the navigation rack floating over its top-left corner.
//!
//! Parts: `rack` (when the pane has one), `bar` (holds the caller's app bar) and `body`. A
//! micro's presentation root is exactly its pane's size and clips what overflows it (the Clay
//! mount contract), so a body taller than the panel scrolls here or nowhere: the stylesheet makes
//! the page fill the root, keeps the bar out of the scroll and gives the body the height the bar
//! leaves. The rack is `position: absolute` in the page's top-left, so it stays put while the body
//! scrolls; the page publishes the corner it covers as `--ct-pane-rack-inset` for the app bar's
//! title to start after. `data-rack` says whether there is one.
//!
//! Where the rack comes from: a micro provides the shell's rack element once at its root with
//! [`use_pane_rack`], and every `PanelPage` under it reads that. The `rack` prop overrides it for
//! one page. The kit cannot name the shell's capability, so it holds only an element.
use dioxus::prelude::*;

/// The rack element a pane's pages float over their corner, provided by [`use_pane_rack`].
#[derive(Clone)]
struct PaneRack(Element);

/// Provides `rack` to every [`PanelPage`] below the calling component. Call it once at a micro's
/// root, before any early return.
pub fn use_pane_rack(rack: Element) {
    use_context_provider(|| PaneRack(rack));
}

/// The page of a panel micro: `app_bar` pinned at the top and `children` in a body that scrolls
/// within the panel, with the pane's rack (or `rack`, when given) floating over the corner.
#[component]
pub fn PanelPage(app_bar: Element, rack: Option<Element>, children: Element) -> Element {
    let rack = rack.or_else(|| try_use_context::<PaneRack>().map(|provided| provided.0));
    rsx! {
        div { class: "ct-panel-page", "data-rack": rack.is_some(),
            if let Some(rack) = rack.as_ref() {
                div { "data-part": "rack", {rack} }
            }
            div { "data-part": "bar", {app_bar} }
            div { "data-part": "body", {children} }
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

    fn provided(_: ()) -> Element {
        use_pane_rack(rsx! { button { "Rack" } });
        rsx! {
            PanelPage { app_bar: rsx! { h1 { "Title" } }, p { "Body" } }
            PanelPage { app_bar: rsx! { h1 { "Other" } }, rack: rsx! { button { "Own" } }, p { "Body" } }
        }
    }

    #[test]
    fn a_page_with_no_rack_has_no_rack_part() {
        let page = Page::with_props(app, ());
        assert_eq!(page.parts(page.by_class("ct-panel-page")), ["bar", "body"]);
    }

    #[test]
    fn the_provided_rack_floats_first_and_the_prop_overrides_it() {
        let page = Page::with_props(provided, ());
        let first = page.by_role("button", "Rack");
        let second = page.by_role("button", "Own");
        assert_eq!(
            page.parts(page.parent(page.parent(first))),
            ["rack", "bar", "body"]
        );
        assert_eq!(
            page.parts(page.parent(page.parent(second))),
            ["rack", "bar", "body"]
        );
    }

    #[test]
    fn the_bar_sits_beside_the_scrolling_body_not_inside_it() {
        let page = Page::with_props(app, ());
        let root = page.by_class("ct-panel-page");
        assert_eq!(page.parts(root), ["bar", "body"]);
        let [bar, body] = page.children(root)[..] else {
            panic!("the page is its bar and its body");
        };
        assert_eq!(page.parent(page.by_role("heading", "Title")), bar);
        assert_eq!(page.parent(page.by_role("status", "Body")), body);
    }
}
