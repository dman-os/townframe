//! A compact label for an attribute, a filter or a selection (`ct-chip`).
//!
//! Parts: `label`, and `remove` on a removable chip. Axes: `data-variant` (`filled`, `outline`),
//! `data-status` (`info`, `success`, `warning`, `danger`; absent is neutral). Three shapes, chosen
//! by the props:
//!
//! * static: a `<span>` that only labels;
//! * toggle (`selected` and `on_toggle`): a `<button>` with `aria-pressed` and `data-state`
//!   (`on`, `off`), for a filter;
//! * removable (`on_remove`): a `<span>` with a `remove` button named `Remove <label>`.
//!
//! A chip that both toggles and removes would nest a button in a button, so it is refused.
use super::Status;
use dioxus::prelude::*;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum ChipVariant {
    #[default]
    Filled,
    Outline,
}

impl ChipVariant {
    pub const ALL: [Self; 2] = [Self::Filled, Self::Outline];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Filled => "filled",
            Self::Outline => "outline",
        }
    }
}

/// Refuses props that break an invariant (`KIT-023`).
fn validate(has_selected: bool, has_toggle: bool, has_remove: bool) {
    assert!(
        has_selected == has_toggle,
        "Chip: `selected` and `on_toggle` go together"
    );
    assert!(
        !(has_toggle && has_remove),
        "Chip: a chip toggles or is removable, not both"
    );
}

#[component]
pub fn Chip(
    label: String,
    #[props(default)] variant: ChipVariant,
    #[props(default)] status: Option<Status>,
    /// The state of a toggle chip, which the caller owns; goes with `on_toggle`.
    #[props(default)]
    selected: Option<bool>,
    /// Called with the state the chip asks to become.
    #[props(default)]
    on_toggle: Option<EventHandler<bool>>,
    #[props(default)] on_remove: Option<EventHandler<()>>,
) -> Element {
    validate(selected.is_some(), on_toggle.is_some(), on_remove.is_some());
    let variant = variant.as_str();
    let status = status.map(Status::as_str);
    if let (Some(selected), Some(on_toggle)) = (selected, on_toggle) {
        return rsx! {
            button {
                class: "ct-chip",
                r#type: "button",
                "aria-pressed": if selected { "true" } else { "false" },
                "data-variant": variant,
                "data-status": status,
                "data-state": if selected { "on" } else { "off" },
                onclick: move |_| on_toggle.call(!selected),
                span { "part": "chip-label", "{label}" }
            }
        };
    }
    rsx! {
        span { class: "ct-chip", "data-variant": variant, "data-status": status,
            span { "part": "chip-label", "{label}" }
            if let Some(on_remove) = on_remove {
                button {
                    r#type: "button",
                    "part": "chip-remove",
                    "aria-label": "Remove {label}",
                    onclick: move |_| on_remove.call(()),
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

    fn static_chip((variant, status): (ChipVariant, Option<Status>)) -> Element {
        rsx! { Chip { label: "Draft".to_string(), variant, status } }
    }

    #[test]
    fn a_static_chip_only_labels_and_carries_its_axes() {
        for variant in ChipVariant::ALL {
            for status in [None, Some(Status::Success), Some(Status::Danger)] {
                let page = Page::with_props(static_chip, (variant, status));
                assert!(
                    !page.has_role("button", "Draft"),
                    "a static chip is not a control"
                );
                let chip = page.by_class("ct-chip");
                assert_eq!(
                    page.attr(chip, "data-variant").as_deref(),
                    Some(variant.as_str())
                );
                assert_eq!(
                    page.attr(chip, "data-status").as_deref(),
                    status.map(Status::as_str)
                );
                assert_eq!(page.parts(chip), ["chip-label"]);
                assert_eq!(page.text(chip), "Draft");
            }
        }
    }

    #[derive(Clone)]
    struct Toggle {
        selected: bool,
        toggles: Rc<RefCell<Vec<bool>>>,
    }

    fn toggle_chip(probe: Toggle) -> Element {
        let toggles = probe.toggles;
        rsx! {
            Chip {
                label: "Unread".to_string(),
                selected: Some(probe.selected),
                on_toggle: move |next| toggles.borrow_mut().push(next),
            }
        }
    }

    #[test]
    fn a_toggle_chip_is_a_pressed_button_and_asks_for_the_opposite_state() {
        for selected in [false, true] {
            let toggles = Rc::new(RefCell::new(Vec::new()));
            let mut page = Page::with_props(
                toggle_chip,
                Toggle {
                    selected,
                    toggles: Rc::clone(&toggles),
                },
            );
            let chip = page.by_role("button", "Unread");
            assert_eq!(page.attr(chip, "class").as_deref(), Some("ct-chip"));
            assert_eq!(
                page.attr(chip, "aria-pressed").as_deref(),
                Some(if selected { "true" } else { "false" })
            );
            assert_eq!(
                page.attr(chip, "data-state").as_deref(),
                Some(if selected { "on" } else { "off" })
            );
            page.click(chip);
            assert_eq!(*toggles.borrow(), [!selected]);
        }
    }

    fn removable_chip(removals: Rc<RefCell<u32>>) -> Element {
        rsx! {
            Chip {
                label: "rust".to_string(),
                on_remove: move |()| *removals.borrow_mut() += 1,
            }
        }
    }

    #[test]
    fn a_removable_chip_has_a_remove_button_named_for_what_it_removes() {
        let removals = Rc::new(RefCell::new(0));
        let mut page = Page::with_props(removable_chip, Rc::clone(&removals));
        let chip = page.by_class("ct-chip");
        assert_eq!(page.parts(chip), ["chip-label", "chip-remove"]);
        let remove = page.by_role("button", "Remove rust");
        page.click(remove);
        assert_eq!(*removals.borrow(), 1);
    }

    #[test]
    #[should_panic(expected = "go together")]
    fn selected_without_a_toggle_handler_is_a_programming_error() {
        validate(true, false, false);
    }

    #[test]
    #[should_panic(expected = "not both")]
    fn a_chip_that_toggles_and_removes_is_refused() {
        validate(true, true, true);
    }

    fn owner() -> Element {
        let mut selected = use_signal(|| false);
        let mut present = use_signal(|| true);
        rsx! {
            Chip {
                label: "Unread".to_string(),
                selected: Some(selected()),
                on_toggle: move |next| selected.set(next),
            }
            if present() {
                Chip {
                    label: "rust".to_string(),
                    status: Some(Status::Info),
                    on_remove: move |()| present.set(false),
                }
            }
        }
    }

    #[test]
    fn chips_survive_any_order_of_events() {
        Storm::of(owner).run_seeds(0..16);
    }
}
