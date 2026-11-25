//! A button (`ct-button`): one action, pressed by pointer, touch or keyboard.
//!
//! Parts: `label`. Axes: `data-variant` (`primary`, `secondary`, `outline`, `ghost`,
//! `destructive`), `data-size` (`sm`, `md`, `lg`). States: `data-disabled`; with `pressed` set it
//! is a toggle and carries `aria-pressed` and `data-state` (`on`, `off`); with `current` set it is
//! the entry of a run of buttons that is open now (a sidebar's destination) and carries
//! `aria-current` and `data-current`. It is a native
//! `<button type="button">`, so Enter and Space activate it wherever the renderer has focus; a
//! disabled one ignores its click whatever the renderer does.
use dioxus::prelude::*;

/// The weight of a button among its neighbours.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum ButtonVariant {
    /// The one main action of a surface.
    #[default]
    Primary,
    Secondary,
    Outline,
    Ghost,
    /// An action that destroys something.
    Destructive,
}

impl ButtonVariant {
    pub const ALL: [Self; 5] = [
        Self::Primary,
        Self::Secondary,
        Self::Outline,
        Self::Ghost,
        Self::Destructive,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Primary => "primary",
            Self::Secondary => "secondary",
            Self::Outline => "outline",
            Self::Ghost => "ghost",
            Self::Destructive => "destructive",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum ButtonSize {
    Sm,
    #[default]
    Md,
    Lg,
}

impl ButtonSize {
    pub const ALL: [Self; 3] = [Self::Sm, Self::Md, Self::Lg];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sm => "sm",
            Self::Md => "md",
            Self::Lg => "lg",
        }
    }
}

#[component]
pub fn Button(
    #[props(default)] variant: ButtonVariant,
    #[props(default)] size: ButtonSize,
    #[props(default)] disabled: bool,
    /// `Some` makes this a toggle button in that state; the caller owns the state and flips it in
    /// `onclick`.
    #[props(default)]
    pressed: Option<bool>,
    /// The button is the open entry of a run of navigation buttons.
    #[props(default)]
    current: bool,
    /// The accessible name, for a button whose children are an icon.
    #[props(default)]
    label: Option<String>,
    onclick: EventHandler<()>,
    children: Element,
) -> Element {
    rsx! {
        button {
            class: "ct-button",
            r#type: "button",
            disabled,
            "aria-label": label,
            "aria-pressed": pressed.map(|pressed| if pressed { "true" } else { "false" }),
            "data-variant": variant.as_str(),
            "data-size": size.as_str(),
            "data-state": pressed.map(|pressed| if pressed { "on" } else { "off" }),
            "data-disabled": if disabled { Some("") } else { None },
            "aria-current": if current { Some("true") } else { None },
            "data-current": if current { Some("") } else { None },
            onclick: move |_| {
                if !disabled {
                    onclick.call(());
                }
            },
            span { "part": "button-label", {children} }
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
        variant: ButtonVariant,
        size: ButtonSize,
        disabled: bool,
        pressed: Option<bool>,
        current: bool,
        clicks: Rc<RefCell<u32>>,
    }

    fn app(probe: Probe) -> Element {
        let clicks = probe.clicks;
        rsx! {
            Button {
                variant: probe.variant,
                size: probe.size,
                disabled: probe.disabled,
                pressed: probe.pressed,
                current: probe.current,
                onclick: move |()| *clicks.borrow_mut() += 1,
                "Save"
            }
        }
    }

    fn render(configure: impl FnOnce(&mut Probe)) -> (Page, Rc<RefCell<u32>>) {
        let clicks = Rc::new(RefCell::new(0));
        let mut probe = Probe {
            variant: ButtonVariant::default(),
            size: ButtonSize::default(),
            disabled: false,
            pressed: None,
            current: false,
            clicks: Rc::clone(&clicks),
        };
        configure(&mut probe);
        (Page::with_props(app, probe), clicks)
    }

    #[test]
    fn a_button_is_a_named_native_button_with_a_label_part() {
        let (page, _) = render(|_| {});
        let button = page.by_role("button", "Save");
        assert_eq!(page.attr(button, "class").as_deref(), Some("ct-button"));
        assert_eq!(page.attr(button, "type").as_deref(), Some("button"));
        let label = page.children(button)[0];
        assert_eq!(page.attr(label, "part").as_deref(), Some("button-label"));
        // Nothing but vocabulary: no state a plain button does not have.
        assert_eq!(page.attr(button, "aria-pressed"), None);
        assert_eq!(page.attr(button, "data-state"), None);
        assert_eq!(page.attr(button, "data-disabled"), None);
        assert_eq!(page.attr(button, "aria-current"), None);
        assert_eq!(page.attr(button, "data-current"), None);
    }

    #[test]
    fn the_open_entry_of_a_run_says_it_is_current() {
        let (page, _) = render(|probe| probe.current = true);
        let button = page.by_role("button", "Save");
        assert_eq!(page.attr(button, "aria-current").as_deref(), Some("true"));
        assert_eq!(page.attr(button, "data-current").as_deref(), Some(""));
    }

    #[test]
    fn every_variant_and_size_names_itself_for_the_stylesheet() {
        for variant in ButtonVariant::ALL {
            for size in ButtonSize::ALL {
                let (page, _) = render(|probe| {
                    probe.variant = variant;
                    probe.size = size;
                });
                let button = page.by_role("button", "Save");
                assert_eq!(
                    page.attr(button, "data-variant").as_deref(),
                    Some(variant.as_str())
                );
                assert_eq!(
                    page.attr(button, "data-size").as_deref(),
                    Some(size.as_str())
                );
            }
        }
    }

    #[test]
    fn a_click_reports_once_per_press() {
        let (mut page, clicks) = render(|_| {});
        let button = page.by_role("button", "Save");
        page.click(button);
        page.click(button);
        assert_eq!(*clicks.borrow(), 2);
    }

    #[test]
    fn a_disabled_button_says_so_and_ignores_its_click() {
        let (mut page, clicks) = render(|probe| probe.disabled = true);
        let button = page.by_role("button", "Save");
        assert_eq!(page.attr(button, "disabled").as_deref(), Some("true"));
        assert_eq!(page.attr(button, "data-disabled").as_deref(), Some(""));
        page.click(button);
        assert_eq!(*clicks.borrow(), 0);
    }

    #[test]
    fn a_toggle_carries_its_state_in_aria_and_in_data_state() {
        for (pressed, aria, state) in [(true, "true", "on"), (false, "false", "off")] {
            let (page, _) = render(|probe| probe.pressed = Some(pressed));
            let button = page.by_role("button", "Save");
            assert_eq!(page.attr(button, "aria-pressed").as_deref(), Some(aria));
            assert_eq!(page.attr(button, "data-state").as_deref(), Some(state));
        }
    }

    fn owner() -> Element {
        let mut pressed = use_signal(|| false);
        rsx! {
            Button {
                pressed: Some(pressed()),
                onclick: move |()| pressed.set(!pressed()),
                "Save"
            }
        }
    }

    #[test]
    fn a_button_survives_any_order_of_events() {
        Storm::of(owner).run_seeds(0..16);
    }
}
