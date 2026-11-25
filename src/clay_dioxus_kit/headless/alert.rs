//! A message about the state of something (`ct-alert`).
//!
//! Parts: `icon`, `content`, `title`, `body`, `dismiss`. Axis: `data-status` (`info`, `success`,
//! `warning`, `danger`). The role follows the urgency of the status: `danger` and `warning` are
//! `role="alert"` (announced at once, interrupting), `info` and `success` are `role="status"`
//! (announced politely). The icon is decorative and drawn by the stylesheet. `dismiss` exists
//! only when the caller can act on it.
use super::Status;
use dioxus::prelude::*;

#[component]
pub fn Alert(
    #[props(default)] status: Status,
    #[props(default)] title: Option<String>,
    /// Shows a dismiss button that calls this; the caller removes the alert.
    #[props(default)]
    on_dismiss: Option<EventHandler<()>>,
    children: Element,
) -> Element {
    let role = match status {
        Status::Danger | Status::Warning => "alert",
        Status::Info | Status::Success => "status",
    };
    rsx! {
        div { class: "ct-alert", role, "data-status": status.as_str(),
            span { "data-part": "icon", "aria-hidden": "true" }
            div { "data-part": "content",
                if let Some(title) = title {
                    p { "data-part": "title", "{title}" }
                }
                div { "data-part": "body", {children} }
            }
            if let Some(on_dismiss) = on_dismiss {
                button {
                    r#type: "button",
                    "data-part": "dismiss",
                    "aria-label": "Dismiss",
                    onclick: move |_| on_dismiss.call(()),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::Page;
    use dioxus_storm::Storm;
    use std::cell::RefCell;
    use std::rc::Rc;

    #[derive(Clone)]
    struct Probe {
        status: Status,
        dismissals: Option<Rc<RefCell<u32>>>,
    }

    fn app(probe: Probe) -> Element {
        let dismissals = probe.dismissals;
        rsx! {
            Alert {
                status: probe.status,
                title: "Heads up".to_string(),
                on_dismiss: dismissals.map(|count| {
                    EventHandler::new(move |()| *count.borrow_mut() += 1)
                }),
                "Disk is nearly full"
            }
        }
    }

    const NAME: &str = "Heads upDisk is nearly full";

    #[test]
    fn urgent_statuses_interrupt_and_calm_ones_do_not() {
        for (status, role) in [
            (Status::Danger, "alert"),
            (Status::Warning, "alert"),
            (Status::Info, "status"),
            (Status::Success, "status"),
        ] {
            let page = Page::with_props(
                app,
                Probe {
                    status,
                    dismissals: None,
                },
            );
            let alert = page.by_role(role, NAME);
            assert_eq!(page.attr(alert, "class").as_deref(), Some("ct-alert"));
            assert_eq!(
                page.attr(alert, "data-status").as_deref(),
                Some(status.as_str())
            );
        }
    }

    #[test]
    fn the_parts_are_the_icon_the_content_and_nothing_else_without_a_dismiss() {
        let page = Page::with_props(
            app,
            Probe {
                status: Status::Info,
                dismissals: None,
            },
        );
        let alert = page.by_role("status", NAME);
        let parts: Vec<_> = page
            .children(alert)
            .into_iter()
            .map(|part| page.attr(part, "data-part").expect("every child is a part"))
            .collect();
        assert_eq!(parts, ["icon", "content"]);
        let icon = page.children(alert)[0];
        assert_eq!(page.attr(icon, "aria-hidden").as_deref(), Some("true"));
        assert!(!page.has_role("button", "Dismiss"));
    }

    #[test]
    fn a_dismissable_alert_reports_its_dismissal() {
        let dismissals = Rc::new(RefCell::new(0));
        let mut page = Page::with_props(
            app,
            Probe {
                status: Status::Success,
                dismissals: Some(Rc::clone(&dismissals)),
            },
        );
        let dismiss = page.by_role("button", "Dismiss");
        assert_eq!(page.attr(dismiss, "data-part").as_deref(), Some("dismiss"));
        page.click(dismiss);
        assert_eq!(*dismissals.borrow(), 1);
    }

    fn owner() -> Element {
        let mut open = use_signal(|| true);
        rsx! {
            if open() {
                Alert {
                    status: Status::Danger,
                    on_dismiss: move |()| open.set(false),
                    "Gone"
                }
            }
        }
    }

    #[test]
    fn an_alert_survives_any_order_of_events() {
        Storm::of(owner).run_seeds(0..16);
    }
}
