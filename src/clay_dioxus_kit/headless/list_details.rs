//! ListDetails: the list/details scaffold.
//!
//! Compact hosts show one pane at a time: the details come on top of the list, which stays
//! mounted underneath (its scroll position and state survive) but is hidden from assistive
//! technology while covered. Wider hosts show both side by side inside [`Sections`]. With no
//! details the list fills the space either way. The list stays mounted as details open and
//! close (see the comment in [`ListDetails`]).
//!
//! The scaffold owns no navigation state. Whether details are open is the `details` prop;
//! opening and going back are requests made through [`use_list_details`] by whatever the
//! scaffold renders, and forwarded to `on_open` / `on_back`, so the owner of the navigation
//! stack hears of every push through one path. Which row is open is the owner's too (`current`);
//! rows read it through the same context to mark themselves (see [`ListRow`](super::list_row)).
use super::sections::SectionPane;
use super::sections::Sections;
use super::sections::SectionsSkin;
use super::sections::Split;
use crate::Layer;
use crate::WidthClass;
use dioxus::prelude::*;

/// What the list and the details can ask of the scaffold; obtained with [`use_list_details`].
#[derive(Clone, Copy, PartialEq)]
pub struct ListDetailsActions {
    open: Callback<String>,
    back: Callback<()>,
    current: Signal<Option<String>>,
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

    /// Whether the row named `key` is the one open. Reading it subscribes the calling component,
    /// so a row re-renders when the owner opens another.
    pub fn is_current(&self, key: &str) -> bool {
        self.current.read().as_deref() == Some(key)
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
    /// The key of the row that is open in the details, if the owner knows one. Independent of
    /// `details` being present: a wide host shows a placeholder as details while no row is open.
    current: Option<String>,
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
    // The context value is created once, so what changes lives behind a signal that is written
    // when the prop differs (a plain write in render: this component never reads it).
    let mut open_row = use_signal(|| current.clone());
    if *open_row.peek() != current {
        open_row.set(current);
    }
    use_context_provider(|| ListDetailsActions {
        open,
        back,
        current: open_row,
    });

    // The list is the same element in every arrangement (the first pane of a `Sections` on a
    // wide host, the first child of one container on a compact host), keyed so opening and
    // closing the details never remounts it: a list that owns state or a mounted presentation
    // (a provider that would otherwise be released and started again) keeps both. Only a change
    // of arrangement, which is a different host width, rebuilds it.
    if width.fits_side_by_side() {
        let single = Split {
            weights: split.weights[..1].to_vec(),
            rules: split.rules[..1].to_vec(),
            ..split.clone()
        };
        let mut panes = vec![SectionPane {
            name: list_name.clone(),
            content: rsx! { div { class: skin.list, style: "width:100%;height:100%", {list} } },
        }];
        let (label, split) = match details {
            Some(details) => {
                panes.push(SectionPane {
                    name: details_name.clone(),
                    content: rsx! { div { class: skin.details, style: "width:100%;height:100%", {details} } },
                });
                (format!("{list_name} and {details_name}"), split)
            }
            None => (list_name, single),
        };
        return rsx! {
            Sections { label, panes, split, on_resize, skin: skin.sections }
        };
    }
    let covered = details.is_some();
    let raised = Layer::Raised.css();
    let content = Layer::Content.css();
    let list_events = if covered { "none" } else { "auto" };
    rsx! {
        div {
            class: skin.root,
            style: "position:relative;width:100%;height:100%;min-width:0;min-height:0",
            div {
                class: skin.list,
                role: "region",
                "aria-label": "{list_name}",
                // Covered by the details, the list is out of reach of assistive technology.
                "aria-hidden": covered.then_some("true"),
                // A layer of its own makes the list one stacking context on `Content`, below the
                // details on `Raised`. Without it the list's app bar (a positioned `AppBar`-layer
                // element) took part in the root's stacking order, painted over the details and
                // intercepted their pointer events.
                // `pointer-events:none` while covered takes the list out of hit-testing outright
                // (`inert` is not in the guest wire vocabulary), so nothing in it can take a
                // click meant for the details even if it paints above them.
                style: "position:relative;width:100%;height:100%;z-index:{content};pointer-events:{list_events}",
                {list}
            }
            if let Some(details) = details {
                div {
                    class: skin.details,
                    role: "region",
                    "aria-label": "{details_name}",
                    style: "position:absolute;top:0;right:0;bottom:0;left:0;z-index:{raised}",
                    {details}
                }
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
                current: None,
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
    fn compact_details_cover_the_list_on_the_raised_layer() {
        let (page, _) = render(WidthClass::Compact, true);
        let details = page.by_role("region", "Inbox settings");
        assert_eq!(
            page.style(details, "z-index").as_deref(),
            Some(Layer::Raised.css())
        );
    }

    #[test]
    fn compact_covered_list_is_inert_and_on_a_layer_below_the_details() {
        let (page, _) = render(WidthClass::Compact, true);
        let details = page.by_role("region", "Inbox settings");
        let list = page.children(page.parent(details))[0];
        assert_eq!(page.style(list, "pointer-events").as_deref(), Some("none"));
        assert_eq!(page.attr(list, "aria-hidden").as_deref(), Some("true"));
        assert_eq!(page.style(list, "position").as_deref(), Some("relative"));
        assert_eq!(
            page.style(list, "z-index").as_deref(),
            Some(Layer::Content.css())
        );
        assert!(Layer::Content < Layer::Raised);
        let (page, _) = render(WidthClass::Compact, false);
        let list = page.by_role("region", "Settings");
        assert_eq!(page.style(list, "pointer-events").as_deref(), Some("auto"));
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

    #[component]
    fn Counter() -> Element {
        let mut count = use_signal(|| 0);
        rsx! { button { onclick: move |_| count += 1, "count {count}" } }
    }

    fn stateful(width: WidthClass) -> Element {
        let mut open = use_signal(|| false);
        rsx! {
            ListDetails {
                width,
                list_name: "Settings",
                list: rsx! {
                    Counter {}
                    button { onclick: move |_| open.set(true), "Open" }
                },
                details_name: "Inbox settings",
                details: open().then(|| rsx! { button { onclick: move |_| open.set(false), "Close" } }),
                current: None,
                split: Split::new(vec![1.0, 2.0], 900.0),
                on_resize: |_| {},
                on_open: |_| {},
                on_back: |_| {},
            }
        }
    }

    #[test]
    fn opening_and_closing_the_details_never_remounts_the_list() {
        for width in [WidthClass::Compact, WidthClass::Expanded] {
            let mut page = Page::with_props(stateful, width);
            let count = page.by_role("button", "count 0");
            page.click(count);
            let open = page.by_role("button", "Open");
            page.click(open);
            let close = page.by_role("button", "Close");
            page.click(close);
            assert!(
                page.has_role("button", "count 1"),
                "{width:?}: the list lost its state\n{}",
                page.outline()
            );
        }
    }
}
