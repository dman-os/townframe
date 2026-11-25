//! The navigation rack (`ct-nav-rack`; ADR 058): the group a host places its
//! [`NavButton`](crate::NavButton)s in, a `role="group"` named "Navigation" that lays them out in
//! a row. It takes no position of its own: where the rack sits is the caller's business.
use dioxus::prelude::*;

#[component]
pub fn NavRack(children: Element) -> Element {
    rsx! {
        div { class: "ct-nav-rack", role: "group", "aria-label": "Navigation", {children} }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NavButton;
    use crate::NavButtonKind;
    use crate::testing::Page;

    fn app(_: ()) -> Element {
        rsx! {
            NavRack {
                NavButton { kind: NavButtonKind::Sidebar, onclick: |_| {} }
                NavButton { kind: NavButtonKind::Back, onclick: |_| {} }
            }
        }
    }

    #[test]
    fn the_rack_is_a_named_group_of_its_buttons() {
        let page = Page::with_props(app, ());
        let group = page.by_role("group", "Navigation");
        assert_eq!(page.children(group).len(), 2);
        for label in ["Open sidebar", "Back"] {
            assert_eq!(page.parent(page.by_role("button", label)), group);
        }
    }
}
