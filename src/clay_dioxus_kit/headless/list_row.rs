//! ListRow: one openable row of a list that sits inside a [`ListDetails`](super::list_details).
//!
//! The row opens itself through the scaffold's action channel (`use_list_details`) and marks
//! itself with `aria-current` while the scaffold's owner says it is the open one, so a list does
//! not carry its own copy of the selection and the highlight cannot disagree with the stack after
//! back or forward.
//!
//! A row exists only inside a scaffold in the same Dioxus tree: a list in a Wasm guest renders in
//! the guest's own tree, where there is no scaffold context, and has to ask the shell to navigate
//! and cannot mark the open row until the shell can report the open row reactively.
use super::list_details::use_list_details;
use dioxus::prelude::*;

/// The classes a skin gives [`ListRow`].
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct ListRowSkin {
    pub root: &'static str,
}

#[component]
pub fn ListRow(
    /// The key the scaffold's `on_open` receives and `current` is compared with.
    id: String,
    /// The accessible name (`KIT-050`).
    name: String,
    #[props(default)] skin: ListRowSkin,
    children: Element,
) -> Element {
    let actions = use_list_details();
    let current = actions.is_current(&id);
    rsx! {
        button {
            class: skin.root,
            r#type: "button",
            "aria-label": "{name}",
            "aria-current": current.then_some("true"),
            "data-state": if current { "current" } else { "idle" },
            onclick: move |_| actions.open(id.clone()),
            {children}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WidthClass;
    use crate::headless::list_details::ListDetails;
    use crate::headless::sections::Split;
    use crate::testing::Page;
    use std::cell::RefCell;
    use std::rc::Rc;

    #[derive(Clone)]
    struct Probe {
        opened: Rc<RefCell<Vec<String>>>,
    }

    fn app(probe: Probe) -> Element {
        // The owner follows `on_open`, as the shell follows its stack.
        let mut current = use_signal(|| None::<String>);
        let opened = probe.opened;
        rsx! {
            ListDetails {
                width: WidthClass::Expanded,
                list_name: "Settings",
                list: rsx! {
                    ListRow { id: "sync", name: "Open Sync", "Sync" }
                    ListRow { id: "about", name: "Open About", "About" }
                },
                details_name: "Details",
                details: Some(rsx! { "details" }),
                current: current(),
                split: Split::new(vec![1.0, 2.0], 900.0),
                on_resize: |_| {},
                on_open: move |key: String| {
                    opened.borrow_mut().push(key.clone());
                    current.set(Some(key));
                },
                on_back: |_| {},
            }
        }
    }

    #[test]
    fn a_press_opens_the_row_and_only_the_open_row_is_current() {
        let opened = Rc::new(RefCell::new(vec![]));
        let mut page = Page::with_props(
            app,
            Probe {
                opened: Rc::clone(&opened),
            },
        );
        let sync = page.by_role("button", "Open Sync");
        let about = page.by_role("button", "Open About");
        assert_eq!(page.attr(sync, "aria-current"), None);
        assert_eq!(page.attr(about, "aria-current"), None);

        page.click(sync);
        assert_eq!(*opened.borrow(), ["sync"]);
        assert_eq!(page.attr(sync, "aria-current").as_deref(), Some("true"));
        assert_eq!(page.attr(sync, "data-state").as_deref(), Some("current"));
        assert_eq!(page.attr(about, "aria-current"), None);

        // Another row replaces it.
        page.click(about);
        assert_eq!(page.attr(sync, "aria-current"), None);
        assert_eq!(page.attr(about, "aria-current").as_deref(), Some("true"));
    }
}
