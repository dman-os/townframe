//! ListDetails: the list/details scaffold.
//!
//! Compact hosts show one pane at a time: the details come on top of the list, which stays
//! mounted underneath (its scroll position survives) but is hidden from assistive technology
//! while covered. Wider hosts show both side by side inside [`Sections`]. With no details the
//! list fills the space either way.
//!
//! The scaffold owns no navigation state. Whether details are open is the `details` prop;
//! opening and going back are requests made through [`use_list_details`] by whatever the
//! scaffold renders, and forwarded to `on_open` / `on_back`, so the owner of the navigation
//! stack hears of every push through one path.
use super::sections::SectionPane;
use super::sections::Sections;
use super::sections::SectionsSkin;
use super::sections::Split;
use crate::WidthClass;
use dioxus::prelude::*;

/// What the list and the details can ask of the scaffold; obtained with [`use_list_details`].
#[derive(Clone, Copy, PartialEq)]
pub struct ListDetailsActions {
    open: Callback<String>,
    back: Callback<()>,
}

impl ListDetailsActions {
    /// Ask for the row named `key` to be opened as the details.
    pub fn open(&self, key: impl Into<String>) {
        self.open.call(key.into());
    }

    /// Ask to leave the details.
    pub fn back(&self) {
        self.back.call(());
    }
}

/// The actions of the nearest enclosing [`ListDetails`]. Panics outside one: a list row that
/// cannot open anything is a wiring bug.
pub fn use_list_details() -> ListDetailsActions {
    use_context::<ListDetailsActions>()
}

/// The classes a skin gives each part of [`ListDetails`].
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct ListDetailsSkin {
    pub root: &'static str,
    pub list: &'static str,
    pub details: &'static str,
    /// Used for the side-by-side arrangement.
    pub sections: SectionsSkin,
}

#[component]
pub fn ListDetails(
    width: WidthClass,
    list_name: String,
    list: Element,
    details_name: String,
    details: Option<Element>,
    split: Split,
    on_resize: EventHandler<Vec<f64>>,
    on_open: EventHandler<String>,
    on_back: EventHandler<()>,
    #[props(default)] skin: ListDetailsSkin,
) -> Element {
    // Callbacks made with `use_callback` track the latest props, so the context value, which is
    // created once, never calls a stale `on_open`.
    let open = use_callback(move |key: String| on_open.call(key));
    let back = use_callback(move |()| on_back.call(()));
    use_context_provider(|| ListDetailsActions { open, back });

    let Some(details) = details else {
        return rsx! {
            div {
                class: skin.root,
                role: "region",
                "aria-label": "{list_name}",
                style: "width:100%;height:100%;min-width:0;min-height:0",
                div { class: skin.list, style: "width:100%;height:100%", {list} }
            }
        };
    };
    if width.fits_side_by_side() {
        return rsx! {
            Sections {
                label: "{list_name} and {details_name}",
                panes: vec![
                    SectionPane {
                        name: list_name,
                        content: rsx! { div { class: skin.list, style: "width:100%;height:100%", {list} } },
                    },
                    SectionPane {
                        name: details_name,
                        content: rsx! { div { class: skin.details, style: "width:100%;height:100%", {details} } },
                    },
                ],
                split,
                on_resize,
                skin: skin.sections,
            }
        };
    }
    rsx! {
        div {
            class: skin.root,
            style: "position:relative;width:100%;height:100%;min-width:0;min-height:0",
            div {
                class: skin.list,
                role: "region",
                "aria-label": "{list_name}",
                "aria-hidden": "true",
                style: "width:100%;height:100%",
                {list}
            }
            div {
                class: skin.details,
                role: "region",
                "aria-label": "{details_name}",
                style: "position:absolute;top:0;right:0;bottom:0;left:0;z-index:20",
                {details}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::Page;
    use std::cell::RefCell;
    use std::rc::Rc;

    #[derive(Clone)]
    struct Probe {
        width: WidthClass,
        has_details: bool,
        calls: Rc<RefCell<Vec<String>>>,
    }

    #[component]
    fn Row() -> Element {
        let actions = use_list_details();
        rsx! { button { onclick: move |_| actions.open("inbox"), "Inbox" } }
    }

    #[component]
    fn Close() -> Element {
        let actions = use_list_details();
        rsx! { button { onclick: move |_| actions.back(), "Close" } }
    }

    fn app(probe: Probe) -> Element {
        let (open_calls, back_calls) = (Rc::clone(&probe.calls), probe.calls);
        rsx! {
            ListDetails {
                width: probe.width,
                list_name: "Settings",
                list: rsx! { Row {} },
                details_name: "Inbox settings",
                details: probe.has_details.then(|| rsx! { Close {} }),
                split: Split::new(vec![1.0, 2.0], 900.0),
                on_resize: |_| {},
                on_open: move |key| open_calls.borrow_mut().push(format!("open {key}")),
                on_back: move |()| back_calls.borrow_mut().push("back".into()),
            }
        }
    }

    fn render(width: WidthClass, has_details: bool) -> (Page, Rc<RefCell<Vec<String>>>) {
        let calls = Rc::new(RefCell::new(vec![]));
        let probe = Probe {
            width,
            has_details,
            calls: Rc::clone(&calls),
        };
        (Page::with_props(app, probe), calls)
    }

    #[test]
    fn compact_with_details_shows_only_the_details_to_assistive_technology() {
        let (page, _) = render(WidthClass::Compact, true);
        assert!(page.has_role("region", "Inbox settings"));
        assert!(!page.has_role("region", "Settings"), "{}", page.outline());
        assert!(!page.has_role("button", "Inbox"));
    }

    #[test]
    fn compact_without_details_shows_the_list() {
        let (page, _) = render(WidthClass::Compact, false);
        assert!(page.has_role("region", "Settings"));
        assert!(page.has_role("button", "Inbox"));
        assert!(!page.has_role("region", "Inbox settings"));
    }

    #[test]
    fn wide_with_details_shows_both_and_a_handle_between_them() {
        for width in [WidthClass::Medium, WidthClass::Expanded] {
            let (page, _) = render(width, true);
            assert!(page.has_role("region", "Settings"));
            assert!(page.has_role("region", "Inbox settings"));
            assert!(page.has_role("separator", "Resize Settings and Inbox settings"));
        }
    }

    #[test]
    fn opening_and_going_back_reach_the_owner_of_the_stack() {
        let (mut page, calls) = render(WidthClass::Expanded, true);
        let row = page.by_role("button", "Inbox");
        page.click(row);
        let close = page.by_role("button", "Close");
        page.click(close);
        assert_eq!(*calls.borrow(), ["open inbox", "back"]);
    }
}
